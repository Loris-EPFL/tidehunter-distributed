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
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod live_inline;
#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
mod live_inline_tests;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod live_lane;
mod read_trace;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod sealed;

/// One member's admission facts, computed once per request or batch: pending
/// bytes to charge (including its aligned NVMe-oF buffer) and, when a sealed
/// reader serves it, that reader and the member's aligned range.
#[derive(Clone, Copy)]
struct MemberPlan {
    charge: usize,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    sealed: Option<sealed::SealedMember>,
}
impl MemberPlan {
    /// Index of the serving sealed reader in the routing snapshot.
    fn route(&self) -> Option<usize> {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
            self.sealed.map(|member| member.route)
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            None
        }
    }
}
/// Plans made against one reader-set snapshot. Valid only while that
/// snapshot is current.
#[derive(Clone, Copy)]
struct PlannedMembers<'a> {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    generation: u64,
    members: &'a [MemberPlan],
}
/// One member's plan with the reader-set generation it was made against.
struct CachedPlan {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    generation: u64,
    member: [MemberPlan; 1],
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type RoutingView = arc_swap::Guard<Arc<sealed::NvmeRouting>>;
#[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
struct NoRouting;
#[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
type RoutingView = NoRouting;
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

    fn find(
        &self,
        node: usize,
        reference: &ValueRecordReference,
    ) -> Option<(Arc<ReadFlight>, usize)> {
        let mut index = self.buckets[self.bucket(node, reference)];
        while index != NO_INDEX {
            let entry = self.entries[index].as_ref().unwrap();
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

    fn clear_slot(&mut self, slot: usize) {
        let mut index = std::mem::replace(&mut self.slot_heads[slot], NO_INDEX);
        while index != NO_INDEX {
            let entry = self.entries[index].take().unwrap();
            let bucket = self.bucket(entry.node, &entry.reference);
            let mut current = self.buckets[bucket];
            let mut previous = NO_INDEX;
            while current != index {
                previous = current;
                current = self.entries[current].as_ref().unwrap().bucket_next;
            }
            if previous == NO_INDEX {
                self.buckets[bucket] = entry.bucket_next;
            } else {
                self.entries[previous].as_mut().unwrap().bucket_next = entry.bucket_next;
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
    deadline: Instant,
    pin: Option<IndexReadPin>,
    trace: Option<ReadTraceChild>,
}

/// A caller-bounded collection, submitted as the largest fitting shard groups.
/// Only its admitted requests own transport/result credits; completed values
/// move into the caller's collection before another internal wave is admitted.
pub(super) struct ReadBatch {
    pub(super) values: Vec<Option<Mutation>>,
    pub(super) reads: VecDeque<(Vec<usize>, ReadRequest)>,
    pub(super) deadline: Instant,
    class: ReadClass,
    pub(super) pin: Option<IndexReadPin>,
    groups: VecDeque<(usize, VecDeque<PlannedMember>)>,
    /// Reader-set generation the member plans in `groups` were made against.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    planned_generation: u64,
    owned_lanes: Vec<OwnedLane>,
    state: Arc<State>,
    signal: Option<Arc<BatchSignal>>,
    epoch: u64,
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
        // Plan every member once: one authority lookup and one alignment each.
        // Dispatch and every admission retry reuse these plans.
        let routing = service.routing();
        let mut planned = VecDeque::with_capacity(groups.len());
        for (node, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut members = VecDeque::with_capacity(group.len());
            for (slot, reference) in group {
                let plan = service.plan_member(&routing, &reference)?;
                // Reject an impossible member before issuing any of this batch's RPCs.
                if plan.charge > service.max_bytes.saturating_sub(512) {
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
            deadline: Instant::now() + service.timeout,
            owned_lanes: Vec::new(),
            state: service.state.clone(),
            signal: None,
            epoch: service.epoch,
            groups: planned,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            planned_generation: routing.generation,
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

    fn dispatch_traced(
        &mut self,
        service: &ReadService,
        trace: Option<&ReadTraceCallGuard>,
        offset: usize,
    ) -> Result<(), LocalDbError> {
        // One snapshot for planning checks and route lookups: plan indexes
        // are valid only for the snapshot they were made against.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let routing = service.routing();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if routing.generation != self.planned_generation {
            // The reader set changed since planning: plan again once, not
            // once per admission retry.
            for (_, group) in &mut self.groups {
                for member in group.iter_mut() {
                    member.plan = service.plan_member(&routing, &member.reference)?;
                }
            }
            self.planned_generation = routing.generation;
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let planned_generation = self.planned_generation;
        let mut skipped = 0;
        while skipped < self.groups.len() {
            let (node, group) = self.groups.front_mut().expect("nonempty groups");
            let retained = *service.state.retained.lock();
            let available = service.max_bytes - retained.1;
            let mut bytes = 512usize;
            let mut count = 0;
            let first_route = group.front().and_then(|member| member.plan.route());
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            let mut nvme_bytes = 0usize;
            for member in group.iter().take(256) {
                if member.plan.route() != first_route {
                    break;
                }
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                if let Some(sealed) = member.plan.sealed {
                    let catalog = &routing.routes[sealed.route].catalog;
                    let member_bytes = sealed.range.charge();
                    let (max_bytes, max_members) =
                        (catalog.max_request_bytes(), catalog.max_request_members());
                    if member_bytes > max_bytes || max_members == 0 {
                        return Err(LocalDbError::new("record exceeds NVMe-oF reader capacity"));
                    }
                    if count >= max_members || member_bytes > max_bytes.saturating_sub(nvme_bytes) {
                        break;
                    }
                    nvme_bytes += member_bytes;
                }
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
            let mut request = match service.submit_until_owned(
                *node,
                &references,
                0,
                false,
                ReadContext {
                    deadline: self.deadline,
                    pin: self.pin.clone(),
                    trace: child.clone(),
                },
                (self.class == ReadClass::Point).then_some(&mut self.owned_lanes),
                Some(PlannedMembers {
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    generation: planned_generation,
                    members: &plans,
                }),
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

    pub(super) fn finish_one(&mut self) -> Result<(), LocalDbError> {
        let state = &self
            .reads
            .front()
            .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?
            .1
            .credit
            .state;
        let signal = self.signal.as_ref().expect("admitted batch has a signal");
        // Check only readiness atomics while holding this batch's predicate
        // mutex; producers take the same mutex before notifying.
        let mut empty_sweeps = 0u8;
        let ready = loop {
            // Only the caller can release its reservation. A joined request
            // asks it to hand progress to the actor without stealing the guard.
            self.owned_lanes
                .retain(|owned| !owned.lane.scheduling.lock().grouped_handoff_requested);
            let mut progressed = false;
            for owned in &self.owned_lanes {
                if let Some(mut driver) = owned.lane.driver.try_lock() {
                    progressed |= progress_read_lane(&self.state, &mut driver, self.epoch);
                }
            }
            if progressed {
                empty_sweeps = 0;
            } else if !self.owned_lanes.is_empty() {
                empty_sweeps += 1;
                if empty_sweeps == 64 {
                    // Accepted work stays in the shared drivers; dropping the
                    // reservations wakes the actor without releasing its credits.
                    self.owned_lanes.clear();
                }
            }
            self.owned_lanes
                .retain(|owned| owned.lane.outstanding.load(Ordering::Acquire) != 0);
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
            if Instant::now() >= self.deadline {
                drop(wait_guard);
                state.poison();
                return Err(LocalDbError::new(
                    "remote read deadline expired or worker stopped",
                ));
            }
            if self.owned_lanes.is_empty() {
                #[cfg(test)]
                signal.waiters.fetch_add(1, Ordering::Release);
                signal.changed.wait_until(&mut wait_guard, self.deadline);
                #[cfg(test)]
                signal.waiters.fetch_sub(1, Ordering::Release);
            } else {
                drop(wait_guard);
                thread::yield_now();
            }
        };
        self.owned_lanes
            .retain(|owned| owned.lane.outstanding.load(Ordering::Acquire) != 0);
        let (slots, request) = self.reads.remove(ready).expect("ready request exists");
        if self.reads.is_empty() {
            // The removed request still owns its subscription and credit until
            // finish returns; no idle batch retains an uncharged signal.
            self.signal = None;
        }
        let (values, _) = request.finish()?;
        if slots.len() != values.len() {
            return Err(LocalDbError::new("remote read count mismatch"));
        }
        for (slot, value) in slots.into_iter().zip(values) {
            self.values[slot] = Some(value);
        }
        if self.reads.is_empty() {
            // A joined request may still occupy this lane and its credits.
            // This batch can now wait for admission of a later wave, so the
            // actor must own progress for those unrelated accepted requests.
            self.owned_lanes.clear();
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
                set("read_profile_server_available", u64::from(
                    self.server_response_recheck_samples.load(Ordering::Relaxed) != 0));
                set("read_profile_transport_progress_available", u64::from(
                    self.transport_sweeps.load(Ordering::Relaxed) != 0));
                set("read_profile_server_executor_available", u64::from(self.server_executor_requests.load(Ordering::Relaxed) != 0));
                set("read_profile_server_planning_available", u64::from(self.server_planning_count.load(Ordering::Relaxed) != 0));
                set("read_profile_server_nowait_available", u64::from(self.server_nowait_count.load(Ordering::Relaxed) != 0));
                set("read_profile_server_async_available", u64::from(self.server_async_count.load(Ordering::Relaxed) != 0));
                set("read_profile_server_response_ready_available", u64::from(self.server_response_ready_count.load(Ordering::Relaxed) != 0));
                $(if !matches!(stringify!($name),
                    "transport_tx_all_slots_pending_polls"
                    | "transport_rx_no_posted_slots_polls"
                    | "transport_rx_unread_slots_polls"
                    | "transport_send_post_would_block"
                    | "transport_receive_post_would_block")
                    || self.transport_cq_timed_calls.load(Ordering::Relaxed) != 0
                {
                    set(concat!("read_profile_", stringify!($name)),
                        self.$name.load(Ordering::Relaxed));
                })+
            }
        }
    };
}
read_diagnostics!(
    selected,
    completed,
    errors,
    failed_owner_profile_unavailable,
    failed_owner_profile_not_applicable,
    failed_owner_profile_retained,
    returned,
    wire_requests,
    inline_completed,
    grouped_inline_completed,
    inline_ns,
    actor_completed,
    nvme_completed,
    nvme_flight_ns,
    nvme_flight_count,
    nvme_flight_max_ns,
    actor_timed_success_count,
    actor_queue_ns,
    actor_service_ns,
    actor_return_ns,
    request_encode_ns,
    request_encode_count,
    exchange_count,
    response_decode_count,
    record_verify_count,
    exchange_ns,
    response_decode_ns,
    record_verify_ns,
    server_handler_ns,
    server_authority_lookup_ns,
    server_record_read_ns,
    server_response_recheck_wait_ns,
    server_response_recheck_ns,
    server_response_recheck_samples,
    server_executor_requests,
    server_sync_reader_requests,
    server_planning_ns,
    server_planning_count,
    server_planning_max_ns,
    server_executor_residence_ns,
    server_executor_residence_count,
    server_executor_residence_max_ns,
    server_nowait_ns,
    server_nowait_count,
    server_nowait_max_ns,
    server_nowait_errors,
    server_nowait_partial,
    server_nowait_deferred,
    server_async_ns,
    server_async_count,
    server_async_max_ns,
    server_async_errors,
    server_async_partial,
    server_response_ready_ns,
    server_response_ready_count,
    server_response_ready_max_ns,
    transport_sweeps,
    transport_output_queue_ns,
    transport_output_queue_samples,
    transport_output_submit_ns,
    transport_output_submit_samples,
    transport_output_retire_ns,
    transport_output_retire_samples,
    transport_frame_gate_stalls,
    transport_retirement_stalls,
    transport_cq_polls,
    transport_cq_empty_polls,
    transport_cq_completions,
    transport_cq_call_ns,
    transport_cq_timed_calls,
    transport_tx_all_slots_pending_polls,
    transport_rx_no_posted_slots_polls,
    transport_rx_unread_slots_polls,
    transport_send_post_would_block,
    transport_receive_post_would_block,
    transport_send_size_le_4k,
    transport_send_size_le_16k,
    transport_send_size_le_64k,
    transport_send_size_gt_64k,
    transport_send_post_eagain,
    transport_send_post_retries,
    transport_cq_idle_waits,
    // Why admission returned backpressure; credit shortage is real load, the
    // others are structural or a reader-set change.
    admission_retry_credit,
    admission_retry_flight,
    admission_retry_lane,
    admission_retry_paused,
    admission_retry_config,
    admission_retry_mixed,
    // Live lane inbox pressure: inbox-full events, deepest inbox, deepest
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
    fn add_wal(&self, wal: super::super::tcp::WalReadProfile) {
        macro_rules! add {
            ($(($counter:ident, $field:ident)),* $(,)?) => {
                $(self.$counter.fetch_add(wal.$field, Ordering::Relaxed);)*
            };
        }
        add!(
            (server_executor_requests, executor_requests),
            (server_sync_reader_requests, sync_reader_requests),
            (server_planning_ns, planning_ns),
            (server_planning_count, planning_count),
            (server_executor_residence_ns, executor_residence_ns),
            (server_executor_residence_count, executor_residence_count),
            (server_nowait_ns, nowait_ns),
            (server_nowait_count, nowait_count),
            (server_nowait_errors, nowait_errors),
            (server_nowait_partial, nowait_partial),
            (server_nowait_deferred, nowait_deferred),
            (server_async_ns, async_ns),
            (server_async_count, async_count),
            (server_async_errors, async_errors),
            (server_async_partial, async_partial),
            (server_response_ready_ns, response_ready_ns),
            (server_response_ready_count, response_ready_count),
        );
        self.server_planning_max_ns
            .fetch_max(wal.planning_max_ns, Ordering::Relaxed);
        self.server_executor_residence_max_ns
            .fetch_max(wal.executor_residence_max_ns, Ordering::Relaxed);
        self.server_nowait_max_ns
            .fetch_max(wal.nowait_max_ns, Ordering::Relaxed);
        self.server_async_max_ns
            .fetch_max(wal.async_max_ns, Ordering::Relaxed);
        self.server_response_ready_max_ns
            .fetch_max(wal.response_ready_max_ns, Ordering::Relaxed);
    }
    fn inline_complete(
        &self,
        elapsed_ns: u64,
        profile: Option<super::super::tcp::TcpReadProfile>,
        failed: bool,
    ) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.inline_completed.fetch_add(1, Ordering::Relaxed);
        self.inline_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        if let Some(tcp) = profile {
            self.add_wal(tcp.server_wal);
            self.wire_requests
                .fetch_add(tcp.requests, Ordering::Relaxed);
            macro_rules! add {
                ($($field:ident),+ $(,)?) => {
                    $(self.$field.fetch_add(tcp.$field, Ordering::Relaxed);)+
                };
            }
            add!(
                request_encode_ns,
                request_encode_count,
                exchange_count,
                response_decode_count,
                record_verify_count,
                exchange_ns,
                response_decode_ns,
                record_verify_ns,
                server_handler_ns,
                server_authority_lookup_ns,
                server_record_read_ns,
                server_response_recheck_wait_ns,
                server_response_recheck_ns,
                server_response_recheck_samples
            );
        }
        if failed {
            self.errors.fetch_add(1, Ordering::Relaxed);
            if profile.is_some_and(|profile| profile.server_handler_available) {
                self.failed_owner_profile_retained
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.failed_owner_profile_unavailable
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    // Connection-local work during sweeps with sampled requests outstanding.
    // Includes interleaved unsampled reads; not attributable per RPC, not RTT.
    // Frames crossing the sample boundary may lack one or more stage samples.
    fn transport_delta(
        &self,
        before: super::super::tcp::TransportProgressStats,
        after: super::super::tcp::TransportProgressStats,
    ) {
        self.transport_sweeps.fetch_add(1, Ordering::Relaxed);
        macro_rules! add {
            ($(($counter:ident, $field:ident)),+ $(,)?) => {
                $(self.$counter.fetch_add(after.$field.saturating_sub(before.$field),
                    Ordering::Relaxed);)+
            };
        }
        add!(
            (transport_output_queue_ns, output_queue_ns),
            (transport_output_queue_samples, output_queue_samples),
            (transport_output_submit_ns, output_submit_ns),
            (transport_output_submit_samples, output_submit_samples),
            (transport_output_retire_ns, output_retire_ns),
            (transport_output_retire_samples, output_retire_samples),
            (transport_frame_gate_stalls, frame_gate_stalls),
            (transport_retirement_stalls, retirement_stalls),
            (transport_cq_polls, cq_polls),
            (transport_cq_empty_polls, cq_empty_polls),
            (transport_cq_completions, cq_completions),
            (transport_cq_call_ns, cq_call_ns),
            (transport_cq_timed_calls, cq_timed_calls),
            (
                transport_tx_all_slots_pending_polls,
                tx_all_slots_pending_polls
            ),
            (transport_rx_no_posted_slots_polls, rx_no_posted_slots_polls),
            (transport_rx_unread_slots_polls, rx_unread_slots_polls),
            (transport_send_post_would_block, send_post_would_block),
            (transport_receive_post_would_block, receive_post_would_block),
            (transport_send_size_le_4k, send_size_le_4k),
            (transport_send_size_le_16k, send_size_le_16k),
            (transport_send_size_le_64k, send_size_le_64k),
            (transport_send_size_gt_64k, send_size_gt_64k),
            (transport_send_post_eagain, send_post_eagain),
            (transport_send_post_retries, send_post_retries),
            (transport_cq_idle_waits, cq_idle_waits),
        );
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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

    fn complete(
        &self,
        result: &ReadCompletion,
        failed_profile: Option<super::super::tcp::TcpReadProfile>,
    ) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.actor_completed.fetch_add(1, Ordering::Relaxed);
        match result {
            Ok((_, Some(worker))) => {
                self.actor_timed_success_count
                    .fetch_add(1, Ordering::Relaxed);
                self.actor_queue_ns
                    .fetch_add(worker.queue_ns, Ordering::Relaxed);
                self.actor_service_ns
                    .fetch_add(worker.service_ns, Ordering::Relaxed);
                let tcp = &worker.tcp;
                self.add_wal(tcp.server_wal);
                self.wire_requests
                    .fetch_add(tcp.requests, Ordering::Relaxed);
                macro_rules! add {
                    ($($field:ident),+ $(,)?) => {
                        $(self.$field.fetch_add(tcp.$field, Ordering::Relaxed);)+
                    };
                }
                add!(
                    request_encode_ns,
                    request_encode_count,
                    exchange_count,
                    response_decode_count,
                    record_verify_count,
                    exchange_ns,
                    response_decode_ns,
                    record_verify_ns,
                    server_handler_ns,
                    server_authority_lookup_ns,
                    server_record_read_ns,
                    server_response_recheck_wait_ns,
                    server_response_recheck_ns,
                    server_response_recheck_samples,
                );
            }
            _ => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                if let Some(profile) = failed_profile {
                    self.add_wal(profile.server_wal);
                    self.wire_requests
                        .fetch_add(profile.requests, Ordering::Relaxed);
                    self.request_encode_ns
                        .fetch_add(profile.request_encode_ns, Ordering::Relaxed);
                    self.request_encode_count
                        .fetch_add(profile.request_encode_count, Ordering::Relaxed);
                    self.exchange_count
                        .fetch_add(profile.exchange_count, Ordering::Relaxed);
                    self.response_decode_count
                        .fetch_add(profile.response_decode_count, Ordering::Relaxed);
                    self.record_verify_count
                        .fetch_add(profile.record_verify_count, Ordering::Relaxed);
                    self.exchange_ns
                        .fetch_add(profile.exchange_ns, Ordering::Relaxed);
                    self.response_decode_ns
                        .fetch_add(profile.response_decode_ns, Ordering::Relaxed);
                    self.record_verify_ns
                        .fetch_add(profile.record_verify_ns, Ordering::Relaxed);
                    self.server_handler_ns
                        .fetch_add(profile.server_handler_ns, Ordering::Relaxed);
                    self.server_authority_lookup_ns
                        .fetch_add(profile.server_authority_lookup_ns, Ordering::Relaxed);
                    self.server_record_read_ns
                        .fetch_add(profile.server_record_read_ns, Ordering::Relaxed);
                    self.server_response_recheck_wait_ns
                        .fetch_add(profile.server_response_recheck_wait_ns, Ordering::Relaxed);
                    self.server_response_recheck_ns
                        .fetch_add(profile.server_response_recheck_ns, Ordering::Relaxed);
                    self.server_response_recheck_samples
                        .fetch_add(profile.server_response_recheck_samples, Ordering::Relaxed);
                }
                if failed_profile.is_some_and(|profile| profile.server_handler_available) {
                    self.failed_owner_profile_retained
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.failed_owner_profile_unavailable
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
/// Lock-free local snapshot handle; keeps no connection or database owner alive.
#[derive(Clone)]
pub struct ReadProfileMonitor {
    state: Arc<State>,
    fabric: bool,
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
        set(
            "read_profile_transport_cq_available",
            u64::from(
                self.fabric
                    && self
                        .state
                        .diagnostics
                        .transport_sweeps
                        .load(Ordering::Relaxed)
                        != 0,
            ),
        );
    }
    /// Sequential, lock-free snapshots of configured partitions; not a global cut.
    pub fn visit_partitions(&self, set: &mut impl FnMut(u32, &'static str, u64)) {
        for (partition, diagnostics) in self.state.owner_diagnostics.iter() {
            diagnostics.visit(&mut |name, value| {
                if name.starts_with("read_profile_server_")
                    || name.starts_with("read_profile_live_")
                    || matches!(
                        name,
                        "read_profile_completed"
                            | "read_profile_inline_completed"
                            | "read_profile_grouped_inline_completed"
                            | "read_profile_actor_completed"
                            | "read_profile_errors"
                            | "read_profile_failed_owner_profile_not_applicable"
                            | "read_profile_failed_owner_profile_retained"
                            | "read_profile_failed_owner_profile_unavailable"
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type UnreleasedLiveGrant = (Arc<Mutex<Client>>, u64, Vec<[u8; 16]>);

struct State {
    failed: AtomicBool,
    first_failure: Arc<std::sync::OnceLock<String>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_reads: bool,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_inline_reads: bool,
    /// Drain commands accepted by live workers and not yet completed.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_drains: AtomicUsize,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_unreleased: Mutex<Vec<UnreleasedLiveGrant>>,
    stopped: AtomicBool,
    retained: Mutex<(usize, usize)>,
    admission_paused: AtomicBool,
    admission_generation: AtomicU64,
    admission_changed: parking_lot::Condvar,
    wake_pending: Box<[AtomicBool]>,
    // Fixed flight signals, weak slots and exact-reference index are reserved from pending bytes.
    flight_ready: Box<[FlightSignal]>,
    read_rpcs: AtomicU64,
    diagnostics: ReadDiagnostics,
    trace: Arc<ReadTrace>,
    coordinator_diagnostics: CoordinatorResolutionDiagnostics,
    owner_diagnostics: Box<[(u32, ReadDiagnostics)]>,
    flights: Mutex<FlightRegistry>,
    coalesced_members: AtomicU64,
    flight_metadata_bytes: usize,
    /// Installed sealed readers. Replaced only by install and release, never
    /// by credit changes; each reader has its own mutex (see `sealed`).
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvme_routing: arc_swap::ArcSwap<sealed::NvmeRouting>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_metrics: Mutex<Vec<super::super::nvmeof::NvmeofMetrics>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_enabled: AtomicBool,
    /// Endpoints whose reactor has drained and closed its readers.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvme_shutdown_ready: AtomicUsize,
    #[cfg(test)]
    peak_connection_reads: AtomicUsize,
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    inline_mutex_fallbacks: AtomicUsize,
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    inline_result_copies: AtomicUsize,
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    inline_retirement_pause: ReactorPause,
    #[cfg(test)]
    inline_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    inline_published_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    completion_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    reactor_pause: Box<[ReactorPause]>,
    #[cfg(test)]
    reactor_enqueue_entered: Box<[Mutex<Option<mpsc::SyncSender<()>>>]>,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
use sealed::NvmeDispatch;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct NvmeEpochPin {
    client: Arc<Mutex<Client>>,
    epoch: u64,
    token: [u8; 16],
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
/// Closes the sealed readers of one endpoint when its reactor has drained. A
/// reader serves only its own endpoint's requests, so no other reactor's
/// state matters here.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct NvmeActorBarrier<'a> {
    state: &'a State,
    node: usize,
    ready: bool,
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl NvmeActorBarrier<'_> {
    fn arrive(&mut self) {
        self.state.begin_nvme_shutdown(self.node);
        self.ready = true;
        self.state
            .nvme_shutdown_ready
            .fetch_add(1, Ordering::Relaxed);
    }
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl Drop for NvmeActorBarrier<'_> {
    fn drop(&mut self) {
        if !self.ready && thread::panicking() {
            self.state.begin_nvme_shutdown(self.node);
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
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) struct AdmissionPause(Arc<State>);
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    // A grouped owner drives the shared reactor; a scalar owner uses the
    // synchronous client and must remain exclusive of queued requests.
    grouped_reserved: bool,
    grouped_handoff_requested: bool,
}
struct InlineLaneReservation(Arc<Mutex<LaneScheduling>>);
impl Drop for InlineLaneReservation {
    fn drop(&mut self) {
        let mut scheduling = self.0.lock();
        scheduling.inline_reserved = false;
        scheduling.grouped_reserved = false;
        scheduling.grouped_handoff_requested = false;
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
    deadline: Instant,
    submitted: Option<Instant>,
    sampled: bool,
    trace: Option<ReadTraceChild>,
    flight: Arc<ReadFlight>,
    _credit: Arc<Credit>,
    /// Sealed reader SQE layout built once at admission; `None` for owner RPC.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    sealed: Option<Box<super::super::nvmeof::ReadPlan>>,
    _lane_credit: LaneCredit,
}
enum ReadCommand {
    Request(Request),
    Wake,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    DrainLive(mpsc::SyncSender<Result<(), String>>),
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
enum LiveLaneCommand {
    Request(Request),
    Drain(mpsc::SyncSender<Result<(), String>>),
}

struct OwnedLane {
    lane: Lane,
    reservation: Option<InlineLaneReservation>,
    sender: mpsc::SyncSender<ReadCommand>,
    state: Arc<State>,
    node: usize,
}
impl Drop for OwnedLane {
    fn drop(&mut self) {
        drop(self.reservation.take());
        // At most one control wake occupies the channel independently of
        // request credits. Accepted work remains in the shared driver.
        if !self.state.wake_pending[self.node].swap(true, Ordering::AcqRel)
            && self.sender.try_send(ReadCommand::Wake).is_err()
        {
            self.state.wake_pending[self.node].store(false, Ordering::Release);
        }
    }
}

impl Request {
    fn complete(self, result: Result<(Vec<Mutation>, Option<WorkerReadProfile>), LocalDbError>) {
        self.complete_with_profile(result, None);
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn complete_nvme(
        self,
        result: Result<(Vec<Mutation>, Option<WorkerReadProfile>), LocalDbError>,
    ) {
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
        self.complete_with_profile_inner(result, None, false);
    }
    fn complete_with_profile(
        self,
        result: Result<(Vec<Mutation>, Option<WorkerReadProfile>), LocalDbError>,
        failed_profile: Option<super::super::tcp::TcpReadProfile>,
    ) {
        self.complete_with_profile_inner(result, failed_profile, true);
    }
    fn complete_with_profile_inner(
        self,
        result: Result<(Vec<Mutation>, Option<WorkerReadProfile>), LocalDbError>,
        failed_profile: Option<super::super::tcp::TcpReadProfile>,
        record_profile: bool,
    ) {
        kv_capture::routes(
            &self.flight.references,
            !record_profile,
            self.flight.slot as u64,
        );
        if let Some(trace) = &self.trace {
            trace.event(if result.is_err() {
                "flight_error"
            } else {
                "flight_completed"
            });
        }
        if self.sampled && record_profile {
            let diagnostics = &self._credit.state.diagnostics;
            let owner = &self._credit.state.owner_diagnostics[self.flight.node].1;
            // This class records admission origin, not the executing thread:
            // a grouped caller can hand its accepted flight to the actor.
            if self.flight.grouped_inline.load(Ordering::Acquire) {
                let profile = result
                    .as_ref()
                    .ok()
                    .and_then(|(_, worker)| worker.as_ref().map(|worker| worker.tcp))
                    .or(failed_profile);
                let duration = self.submitted.map_or(0, elapsed);
                diagnostics.inline_complete(duration, profile, result.is_err());
                owner.inline_complete(duration, profile, result.is_err());
                diagnostics
                    .grouped_inline_completed
                    .fetch_add(1, Ordering::Relaxed);
                owner
                    .grouped_inline_completed
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                diagnostics.complete(&result, failed_profile);
                owner.complete(&result, failed_profile);
            }
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
    grouped_inline: AtomicBool,
    completion: FlightCompletion,
    _pin: Option<IndexReadPin>,
    // Keep retained-result credit while any caller can consume the flight.
    _credit: Arc<Credit>,
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

fn copied_worker_profile(profile: &WorkerReadProfile) -> WorkerReadProfile {
    WorkerReadProfile {
        queue_ns: profile.queue_ns,
        service_ns: profile.service_ns,
        finished: profile.finished,
        tcp: profile.tcp,
    }
}
#[derive(Clone)]
struct Lane {
    index: usize,
    client: Arc<Mutex<Client>>,
    outstanding: Arc<AtomicUsize>,
    scheduling: Arc<Mutex<LaneScheduling>>,
    driver: Arc<Mutex<ReactorLane>>,
}
struct InflightRead {
    request: Request,
    started: Option<Instant>,
}
// Live processing takes `driver` before `client`. Callers try both locks;
// the live endpoint actor uses neither, so one inline I/O cannot stall it.
struct ReactorLane {
    client: Arc<Mutex<Client>>,
    /// Endpoint whose reactor drives this lane. It selects the only sealed
    /// readers this lane may submit to: those of the same endpoint.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    node: usize,
    queued: VecDeque<Request>,
    active: HashMap<u64, InflightRead>,
    profiling: bool,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_cache: Option<super::super::tcp::live_read::LiveReadCache>,
}
/// Harvest completions of `node`'s sealed readers. Each reader has its own
/// mutex, held only across its ring submit and CQ harvest; record verification
/// and request completion run with no lock held.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn progress_nvmeof(state: &Arc<State>, node: usize) -> bool {
    let mut ready = Vec::new();
    {
        let routing = state.nvme_routing.load();
        for route in routing.routes.iter().filter(|route| route.node == node) {
            // Only this reactor raises the count, so zero means no CQE is owed.
            if route.cell.in_flight.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let mut guard = route.lock();
            let Some(dispatch) = guard.dispatch() else {
                continue;
            };
            let completions = match dispatch.reader.progress() {
                Ok(completions) => completions,
                Err(error) => {
                    super::record_first_failure(
                        &state.first_failure,
                        Some(dispatch.reader.manifest().partition),
                        None,
                        "nvme_progress",
                        &error,
                    );
                    // Retain requests, credits and pins while the queue owns DMA.
                    state.poison();
                    continue;
                }
            };
            for completion in completions {
                if let Some(request) = dispatch.active.remove(&completion.id) {
                    route.cell.in_flight.fetch_sub(1, Ordering::Relaxed);
                    if let Some(trace) = &request.trace {
                        trace.event_io("sealed_nvme_completion_observed", 0, completion.id);
                    }
                    ready.push((
                        route.catalog.clone(),
                        dispatch.shape.clone(),
                        request,
                        completion.result,
                    ));
                } else {
                    state.poison();
                }
            }
        }
    }
    let progressed = !ready.is_empty();
    for (catalog, shape, request, result) in ready {
        if let Some(trace) = &request.trace {
            trace.event("sealed_nvme_validation_started");
        }
        let verifying = result
            .as_ref()
            .is_ok_and(|members| members.len() == request.flight.references.len());
        let verification_started = Instant::now();
        let result = result.map_err(LocalDbError::component).and_then(|members| {
            if members.len() != request.flight.references.len() {
                return Err(LocalDbError::new("NVMe-oF completion count mismatch"));
            }
            let values = request
                .flight
                .references
                .iter()
                .zip(members)
                .map(|(reference, bytes)| {
                    CompleteBatch::decode_record(&shape, reference, bytes)
                        .map_err(LocalDbError::component)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if Instant::now() >= request.deadline {
                return Err(LocalDbError::new("remote read operation deadline expired"));
            }
            Ok((values, None))
        });
        if let Some(trace) = &request.trace {
            trace.event("sealed_nvme_validation_finished");
        }
        if verifying {
            catalog.record_verification(verification_started.elapsed());
        }
        request.complete_nvme(result);
    }
    progressed
}

// Returns whether the head belongs to NVMe-oF and whether it made progress.
// Admission already chose the reader and built the SQE layout, so this takes
// only that reader's mutex and repeats no alignment or eligibility work.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn submit_nvmeof(state: &Arc<State>, lane: &mut ReactorLane) -> (bool, bool) {
    let Some(request) = lane.queued.front() else {
        return (false, false);
    };
    let Some(plan) = request.sealed.as_deref() else {
        return (false, false);
    };
    let routing = state.nvme_routing.load();
    let Some(route) = routing
        .routes
        .iter()
        .find(|route| route.node == lane.node && route.catalog.id() == plan.reader_id())
    else {
        // Admission holds the request credit, which blocks any reader-set
        // change, so a missing reader is an internal error.
        if let Some(request) = lane.queued.pop_front() {
            request.complete(Err(LocalDbError::new("sealed read plan has no reader")));
        }
        return (true, true);
    };
    let mut guard = route.lock();
    let Some(dispatch) = guard.dispatch() else {
        drop(guard);
        if let Some(request) = lane.queued.pop_front() {
            request.complete(Err(LocalDbError::new("sealed reader was released")));
        }
        return (true, true);
    };
    let id = dispatch.next;
    dispatch.next = dispatch.next.wrapping_add(1);
    match dispatch.reader.submit_planned(id, plan, request.deadline) {
        Ok(super::super::nvmeof::SubmitOutcome::Submitted) => {
            if let Some(request) = lane.queued.pop_front() {
                if let Some(trace) = &request.trace {
                    trace.event_io("sealed_nvme_request_submitted", 0, id);
                }
                dispatch.active.insert(id, request);
                route.cell.in_flight.fetch_add(1, Ordering::Relaxed);
            }
            (true, true)
        }
        Ok(super::super::nvmeof::SubmitOutcome::Full) => (true, false),
        Err(error) => {
            drop(guard);
            if let Some(request) = lane.queued.pop_front() {
                request.complete(Err(LocalDbError::component(error)));
            }
            (true, true)
        }
    }
}

fn progress_read_lane(state: &Arc<State>, lane: &mut ReactorLane, epoch: u64) -> bool {
    let mut progressed = false;
    let now = Instant::now();
    if lane.queued.iter().any(|request| now >= request.deadline)
        || lane
            .active
            .values()
            .any(|read| now >= read.request.deadline)
    {
        state.poison();
    }
    if state.failed.load(Ordering::Acquire) {
        for request in lane.queued.drain(..) {
            request.complete(Err(LocalDbError::new("read authority is poisoned")));
            progressed = true;
        }
    }
    if lane.queued.is_empty() && lane.active.is_empty() && !lane.profiling {
        return progressed;
    }
    // Diagnostics and the deterministic test hook may own a mutex.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let nvme_head = if state.nvmeof_enabled.load(Ordering::Acquire) {
        loop {
            let (handled, made_progress) = submit_nvmeof(state, lane);
            progressed |= made_progress;
            if !handled || !made_progress {
                break handled;
            }
        }
    } else {
        false
    };
    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
    let nvme_head = false;
    if nvme_head && lane.active.is_empty() && !lane.profiling {
        return progressed;
    }
    // Never let one such connection stall another connection's reads.
    let Some(mut client) = lane.client.try_lock() else {
        return progressed;
    };
    let diagnostics = &state.diagnostics;
    let selected = diagnostics.selected.load(Ordering::Relaxed);
    let sampling = lane.profiling
        || (selected != 0 && selected > diagnostics.completed.load(Ordering::Relaxed));
    let profiling = sampling
        && (lane.queued.iter().any(|request| request.sampled)
            || lane.active.values().any(|read| read.request.sampled));
    if profiling != lane.profiling || profiling {
        client.profile_read_transport(profiling);
        lane.profiling = profiling;
    }
    if lane.queued.is_empty() && lane.active.is_empty() {
        return progressed;
    }
    let transport_before = profiling.then(|| client.transport_progress_stats());
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let mut fallback = None;
    while let Some(request) = lane.queued.front() {
        if nvme_head {
            break;
        }
        if state.failed.load(Ordering::Acquire) {
            break;
        }
        match client.read_admission(&request.references, request.submitted.is_some()) {
            Ok(false) => break,
            Err(error) => {
                lane.queued
                    .pop_front()
                    .unwrap()
                    .complete(Err(LocalDbError::component(error)));
                progressed = true;
                break;
            }
            Ok(true) => {}
        }
        let mut request = lane.queued.pop_front().unwrap();
        let started = request.submitted.map(|_| Instant::now());
        let references = std::mem::take(&mut request.references);
        let message_requests = client.message_read_requests();
        match client.submit_read(
            epoch,
            references,
            request.deadline,
            request.submitted.is_some(),
        ) {
            Ok(Some(id)) => {
                if let Some(trace) = &request.trace {
                    trace.event_io("transport_request_submitted", 0, id);
                }
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                if state.nvmeof_enabled.load(Ordering::Acquire) {
                    fallback = Some((
                        request.flight.references[0].batch_address.partition,
                        request.flight.references.len(),
                    ));
                }
                if client.message_read_requests() != message_requests {
                    state.read_rpcs.fetch_add(1, Ordering::Relaxed);
                }
                lane.active.insert(id, InflightRead { request, started });
                #[cfg(test)]
                state
                    .peak_connection_reads
                    .fetch_max(lane.active.len(), Ordering::Relaxed);
            }
            Ok(None) => {
                request.complete(Err(LocalDbError::new(
                    "read admission changed under exclusive ownership",
                )));
                break;
            }
            Err(error) => {
                request.complete(Err(LocalDbError::component(error)));
                break;
            }
        }
        progressed = true;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if state.nvmeof_enabled.load(Ordering::Acquire) {
            break;
        }
    }
    // Even after global poisoning, accepted transport work is driven to
    // completion/error before releasing its application-buffer credits.
    let completions = client.progress_reads();
    progressed |= client.read_progress_made();
    if let Some(before) = transport_before {
        state
            .diagnostics
            .transport_delta(before, client.transport_progress_stats());
        let active_samples = lane
            .active
            .values()
            .filter(|read| read.request.sampled)
            .count();
        let completed_samples = match &completions {
            Ok(ready) => ready
                .iter()
                .filter(|completion| {
                    lane.active
                        .get(&completion.id)
                        .is_some_and(|read| read.request.sampled)
                })
                .count(),
            Err(_) => active_samples,
        };
        if active_samples == completed_samples && !lane.queued.iter().any(|request| request.sampled)
        {
            // Disable before releasing the client or retiring actor
            // ownership; pooled lanes must not retain sample clocks.
            client.profile_read_transport(false);
            lane.profiling = false;
        }
    }
    drop(client);
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    if let Some((partition, count)) = fallback
        && let Some(route) = state
            .nvme_routing
            .load()
            .routes
            .iter()
            .find(|route| route.catalog.manifest().partition == partition)
    {
        route.catalog.record_ineligible_fallbacks(count);
    }
    match completions {
        Ok(completions) => {
            progressed |= !completions.is_empty();
            for completion in completions {
                let Some(inflight) = lane.active.remove(&completion.id) else {
                    state.poison();
                    continue;
                };
                let finished = Instant::now();
                let failed_profile = completion
                    .result
                    .as_ref()
                    .err()
                    .and_then(|error| error.read_profile.as_deref().copied());
                let result =
                    completion
                        .result
                        .map_err(LocalDbError::component)
                        .and_then(|(values, tcp)| {
                            if finished >= inflight.request.deadline {
                                return Err(LocalDbError::new(
                                    "remote read operation deadline expired",
                                ));
                            }
                            let profile = inflight.request.submitted.map(|submitted| {
                                let started = inflight.started.unwrap();
                                WorkerReadProfile {
                                    queue_ns: started
                                        .duration_since(submitted)
                                        .as_nanos()
                                        .min(u64::MAX as u128)
                                        as u64,
                                    service_ns: finished
                                        .duration_since(started)
                                        .as_nanos()
                                        .min(u64::MAX as u128)
                                        as u64,
                                    finished,
                                    tcp,
                                }
                            });
                            Ok((values, profile))
                        });
                inflight
                    .request
                    .complete_with_profile(result, failed_profile);
            }
        }
        Err(error) => {
            super::record_first_failure(
                &state.first_failure,
                None,
                None,
                "read_transport_progress",
                &error,
            );
            state.poison();
            // Client's fail-stop path relinquishes every request buffer
            // before returning; endpoint-owned DMA slots are separate.
            let message = error.to_string();
            for (_, inflight) in lane.active.drain() {
                inflight.request.complete(Err(LocalDbError::new(&message)));
            }
            progressed = true;
        }
    }
    progressed
}

// Live control and CQ waits must not occupy the shared multi-lane reactor.
// Global flight credits bound all channel entries, including these lane queues.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    /// request holds a global flight credit. Depth counts inbox entries.
    struct LiveLane {
        sender: mpsc::SyncSender<LiveLaneCommand>,
        depth: Arc<AtomicUsize>,
        waker: live_lane::LaneWaker,
        pending: VecDeque<(Instant, Request)>,
    }
    /// Hand `request` to the lane without blocking; false if the inbox is full.
    fn offer(lane: &mut LiveLane, request: Request, state: &State, node: usize) -> Option<Request> {
        // Count before sending so the worker's decrement cannot underflow.
        let depth = lane.depth.fetch_add(1, Ordering::Relaxed) + 1;
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
                if Instant::now() >= request.deadline {
                    // Never reached a worker: fail it like an expired admission.
                    state.poison();
                    request.complete_nvme(Err(LocalDbError::new(
                        "remote read admission deadline expired",
                    )));
                    continue;
                }
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

#[allow(clippy::too_many_arguments)] // The actor takes explicit ownership, admission and polling inputs once.
fn drive_reads(
    state: Arc<State>,
    receiver: mpsc::Receiver<ReadCommand>,
    lanes: Vec<Lane>,
    node: usize,
    base: usize,
    epoch: u64,
    max_requests: usize,
    fabric: bool,
    busy: bool,
) {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    if state.live_reads {
        drive_live_reads(state, receiver, lanes, node, base, epoch, max_requests);
        return;
    }
    let _poison = PoisonOnPanic(&state);
    let mut disconnected = false;
    let mut first_lane = 0;
    let mut empty_polls = 0u8;
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let mut nvme_barrier = NvmeActorBarrier {
        state: &state,
        node,
        ready: false,
    };
    loop {
        let mut progressed = false;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if state.nvmeof_enabled.load(Ordering::Acquire) {
            progressed |= progress_nvmeof(&state, node);
        }
        #[cfg(test)]
        if let Some((entered, resume)) = state.reactor_pause[node].lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        // Credits bound the sum of this queue, lane scheduling state, transport
        // requests and undelivered completions, not merely the channel length.
        for _ in 0..max_requests {
            match receiver.try_recv() {
                Ok(ReadCommand::Request(request)) => {
                    #[cfg(test)]
                    if let Some(entered) = state.reactor_enqueue_entered[node].lock().take() {
                        entered.send(()).unwrap();
                    }
                    lanes[request.lane - base]
                        .driver
                        .lock()
                        .queued
                        .push_back(request);
                    progressed = true;
                }
                Ok(ReadCommand::Wake) => {
                    state.wake_pending[node].store(false, Ordering::Release);
                    progressed = true;
                }
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                Ok(ReadCommand::DrainLive(reply)) => {
                    let _ = reply.send(Err("not a live read worker".into()));
                    progressed = true;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        for offset in 0..lanes.len() {
            let index = (first_lane + offset) % lanes.len();
            let lane = &lanes[index];
            let scheduling = lane.scheduling.lock();
            if scheduling.inline_reserved {
                continue;
            }
            if let Some(mut driver) = lane.driver.try_lock() {
                drop(scheduling);
                progressed |= progress_read_lane(&state, &mut driver, epoch);
            }
        }
        // Every owner must drain its channel and submit or fail queued reads
        // before the last owner closes shared NVMe-oF reader admission.
        // Terminal CQEs remain driven after begin_shutdown.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if disconnected
            && !nvme_barrier.ready
            && lanes.iter().all(|lane| {
                !lane.scheduling.lock().inline_reserved
                    && lane
                        .driver
                        .try_lock()
                        .is_some_and(|driver| driver.queued.is_empty())
            })
        {
            nvme_barrier.arrive();
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let may_exit = nvme_barrier.ready;
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        let may_exit = true;
        if disconnected
            && may_exit
            && lanes.iter().all(|lane| {
                !lane.scheduling.lock().inline_reserved
                    && lane.outstanding.load(Ordering::Acquire) == 0
            })
        {
            break;
        }
        if !lanes.is_empty() {
            first_lane = (first_lane + 1) % lanes.len();
        }
        if !progressed {
            if busy {
                std::hint::spin_loop();
            } else if lanes
                .iter()
                .any(|lane| lane.outstanding.load(Ordering::Acquire) != 0)
            {
                // EFA's manual progress stays on its owner for a bounded burst;
                // TCP keeps scheduler-driven waits. Neither sleeps through I/O.
                if fabric && empty_polls < 64 {
                    empty_polls += 1;
                    std::hint::spin_loop();
                } else {
                    empty_polls = 0;
                    thread::yield_now();
                }
            } else {
                empty_polls = 0;
                // No accepted I/O needs polling. Wake on admission rather than
                // periodically sleeping through the next scalar request.
                match receiver.recv_timeout(Duration::from_millis(50)) {
                    Ok(ReadCommand::Request(request)) => {
                        #[cfg(test)]
                        if let Some(entered) = state.reactor_enqueue_entered[node].lock().take() {
                            entered.send(()).unwrap();
                        }
                        lanes[request.lane - base]
                            .driver
                            .lock()
                            .queued
                            .push_back(request)
                    }
                    Ok(ReadCommand::Wake) => {
                        state.wake_pending[node].store(false, Ordering::Release);
                    }
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    Ok(ReadCommand::DrainLive(reply)) => {
                        let _ = reply.send(Err("not a live read worker".into()));
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => disconnected = true,
                }
            }
        } else {
            empty_polls = 0;
        }
    }
}

pub(super) struct ReadService {
    state: Arc<State>,
    lanes: RwLock<Vec<Vec<Lane>>>,
    sender: Mutex<Vec<mpsc::SyncSender<ReadCommand>>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_maintenance: Mutex<()>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_epoch_pins: Mutex<Vec<NvmeEpochPin>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_pin_uncertain: AtomicBool,
    thread: Mutex<Vec<JoinHandle<()>>>,
    next: Vec<AtomicUsize>,
    max_requests: usize,
    max_bytes: usize,
    timeout: Duration,
    epoch: u64,
}
impl ReadService {
    pub(super) fn resolution_monitor(&self) -> Option<ReadProfileMonitor> {
        self.state
            .coordinator_diagnostics
            .enabled()
            .then(|| ReadProfileMonitor {
                state: self.state.clone(),
                fabric: false,
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
    deadline: Instant,
    sampled: bool,
    trace: Option<ReadTraceChild>,
    profile: Option<Arc<ReadFlight>>,
    subscription: Option<BatchSubscription>,
}
impl ReadRequest {
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
    pub(super) fn try_result(&self) -> Result<Option<ReadOutput>, LocalDbError> {
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
            let Some(Ok((members, _))) = result.as_ref() else {
                unreachable!("a completed flight cannot change");
            };
            let Some(member) = members.get(*slot) else {
                return Err(self.failure("remote read count mismatch"));
            };
            values.push(member.clone());
        }
        let profile = self.profile.as_ref().and_then(|flight| {
            let result = flight.completion.result.lock();
            match result.as_ref() {
                Some(Ok((_, Some(profile)))) => Some(copied_worker_profile(profile)),
                _ => None,
            }
        });
        Ok(Some((values, profile)))
    }

    pub(super) fn finish(self) -> ReadCompletion {
        for (flight, _) in &self.parts {
            if !flight._credit.state.flight_ready[flight.slot].wait_until(self.deadline) {
                return Err(self.failure("remote read deadline expired or worker stopped"));
            }
        }
        let result = self
            .try_result()?
            .ok_or_else(|| self.failure("remote read did not complete"))?;
        if let Some(trace) = &self.trace {
            trace.event("request_result_delivered");
        }
        if self.sampled
            && !self
                .profile
                .as_ref()
                .is_some_and(|flight| flight.grouped_inline.load(Ordering::Acquire))
            && let Some(worker) = &result.1
        {
            let diagnostics = &self.credit.state.diagnostics;
            diagnostics.returned.fetch_add(1, Ordering::Relaxed);
            diagnostics
                .actor_return_ns
                .fetch_add(elapsed(worker.finished), Ordering::Relaxed);
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
    fn nvmeof_enabled(&self) -> bool {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
            self.state.nvmeof_enabled.load(Ordering::Acquire)
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            false
        }
    }

    fn routing(&self) -> RoutingView {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
            self.state.nvme_routing.load()
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            NoRouting
        }
    }

    /// Charge and, for a sealed member, the aligned range of one member. One
    /// authority lookup and one alignment computation; callers keep the result.
    fn plan_member(
        &self,
        routing: &RoutingView,
        reference: &ValueRecordReference,
    ) -> Result<MemberPlan, LocalDbError> {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
            self.plan_sealed_member(routing, reference)
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            let NoRouting = routing;
            Ok(MemberPlan {
                charge: member_charge(reference)?,
            })
        }
    }

    #[cfg(test)]
    fn member_charge(&self, reference: &ValueRecordReference) -> Result<usize, LocalDbError> {
        self.plan_member(&self.routing(), reference)
            .map(|plan| plan.charge)
    }

    /// Backpressure error that records why admission was refused.
    fn backpressure(&self, cause: &AtomicU64) -> LocalDbError {
        cause.fetch_add(1, Ordering::Relaxed);
        LocalDbError::new(BACKPRESSURE)
    }

    /// Plan of one member kept across admission retries of a blocking caller.
    /// It is recomputed only when the reader set changed, never because
    /// credit was released.
    fn cached_plan<'a>(
        &self,
        cached: &'a mut Option<CachedPlan>,
        reference: &ValueRecordReference,
    ) -> Result<PlannedMembers<'a>, LocalDbError> {
        let routing = self.routing();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let generation = routing.generation;
        let plan = match cached.take() {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Some(plan) if plan.generation == generation => plan,
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            Some(plan) => plan,
            _ => CachedPlan {
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                generation,
                member: [self.plan_member(&routing, reference)?],
            },
        };
        let plan = cached.insert(plan);
        Ok(PlannedMembers {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            generation: plan.generation,
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

    pub(super) fn from_clients(
        config: &RemoteDatabaseConfig,
        epoch: u64,
        clients: Vec<Client>,
        recovery_stats: &mut RecoveryStats,
        first_failure: Arc<std::sync::OnceLock<String>>,
    ) -> Result<Self, LocalDbError> {
        // Resolve immutable node policy before handing clients to the reactor;
        // startup must not contend with an otherwise idle scalar caller.
        let busy = clients
            .iter()
            .any(|client| client.polling_policy() == super::super::tcp::PollingPolicy::Busy);
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live_reads = clients.iter().any(Client::live_nvme_configured);
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if live_reads && clients.iter().any(|client| !client.live_nvme_configured()) {
            return Err(LocalDbError::new("mixed live and message read lanes"));
        }
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
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_reads,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_inline_reads: config
                .live_nvme
                .as_ref()
                .is_none_or(|live| live.inline_reads),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_drains: AtomicUsize::new(0),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_unreleased: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
            retained: Mutex::new((0, 0)),
            admission_paused: AtomicBool::new(false),
            admission_generation: AtomicU64::new(0),
            admission_changed: parking_lot::Condvar::new(),
            wake_pending: (0..config.endpoints.len())
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            flight_ready: (0..flight_slots)
                .map(|_| FlightSignal::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            read_rpcs: AtomicU64::new(0),
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
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvme_routing: arc_swap::ArcSwap::from_pointee(sealed::NvmeRouting::empty()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_metrics: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_enabled: AtomicBool::new(false),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvme_shutdown_ready: AtomicUsize::new(0),
            #[cfg(test)]
            peak_connection_reads: AtomicUsize::new(0),
            #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
            inline_mutex_fallbacks: AtomicUsize::new(0),
            #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
            inline_result_copies: AtomicUsize::new(0),
            #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
            inline_retirement_pause: Mutex::new(None),
            #[cfg(test)]
            inline_pause: Mutex::new(None),
            #[cfg(test)]
            inline_published_pause: Mutex::new(None),
            #[cfg(test)]
            completion_pause: Mutex::new(None),
            #[cfg(test)]
            reactor_pause: (0..config.endpoints.len())
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            #[cfg(test)]
            reactor_enqueue_entered: (0..config.endpoints.len())
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
        let mut service = Self {
            state,
            lanes: RwLock::new(Vec::new()),
            sender: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_maintenance: Mutex::new(()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_epoch_pins: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_pin_uncertain: AtomicBool::new(false),
            thread: Mutex::new(Vec::new()),
            next: (0..config.endpoints.len())
                .map(|_| AtomicUsize::new(0))
                .collect(),
            max_requests: config.max_pending_reads,
            max_bytes: config
                .max_pending_read_bytes
                .saturating_sub(flight_metadata_bytes),
            timeout: config.timeout,
            epoch,
        };
        let mut clients = clients.into_iter();
        for node in 0..config.endpoints.len() {
            let base = node * config.read_lanes;
            let mut lanes = Vec::with_capacity(config.read_lanes);
            for _ in 0..config.read_lanes {
                let client = clients.next().unwrap();
                recovery_stats.add_connection(client.connect_profile());
                let client = Arc::new(Mutex::new(client));
                let index = base + lanes.len();
                let scheduling = Arc::new(Mutex::new(LaneScheduling {
                    inline_reserved: false,
                    grouped_reserved: false,
                    grouped_handoff_requested: false,
                }));
                let driver = Arc::new(Mutex::new(ReactorLane {
                    client: client.clone(),
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    node,
                    queued: VecDeque::new(),
                    active: HashMap::new(),
                    profiling: false,
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    live_cache: live_reads
                        .then(super::super::tcp::live_read::LiveReadCache::default),
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
            let fabric = config.tls.transport.is_fabric();
            let thread = thread::Builder::new()
                .name(format!("wal-read-reactor-{node}"))
                .spawn(move || {
                    drive_reads(
                        state,
                        receiver,
                        lanes,
                        node,
                        base,
                        epoch,
                        flight_slots,
                        fabric,
                        busy,
                    )
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn pause_live_for_retirement(&self) -> Result<Option<AdmissionPause>, LocalDbError> {
        if !self.state.live_reads {
            return Ok(None);
        }
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
        let deadline = Instant::now() + self.timeout;
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
        Ok(Some(pause))
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn clear_nvmeof_reader(&self) -> Result<(), LocalDbError> {
        let _maintenance = self.nvmeof_maintenance.lock();
        let _pause = self.pause_idle_nvmeof(true)?;
        self.release_nvmeof_readers()
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn acquire_nvmeof_epoch_pins(&self) -> Result<bool, LocalDbError> {
        if !self.nvmeof_epoch_pins.lock().is_empty() {
            return Ok(false);
        }
        let clients: Vec<_> = self
            .lanes
            .read()
            .iter()
            .map(|lanes| {
                lanes
                    .first()
                    .map(|lane| lane.client.clone())
                    .ok_or_else(|| LocalDbError::new("owner has no read lane"))
            })
            .collect::<Result<_, _>>()?;
        let mut acquired = Vec::with_capacity(clients.len());
        for client in clients {
            let result = client.lock().register_nvmeof_epoch_pin(self.epoch);
            match result {
                Ok(token) => acquired.push(NvmeEpochPin {
                    client,
                    epoch: self.epoch,
                    token,
                }),
                Err(error) => {
                    // The owner may have persisted a pin before its reply was
                    // lost. Preserve all known pins as well as the unknown one.
                    *self.nvmeof_epoch_pins.lock() = acquired;
                    self.nvmeof_pin_uncertain.store(true, Ordering::Release);
                    self.state.poison();
                    return Err(LocalDbError::component(error));
                }
            }
        }
        *self.nvmeof_epoch_pins.lock() = acquired;
        Ok(true)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn release_nvmeof_epoch_pins(&self) -> Result<(), LocalDbError> {
        if self.nvmeof_pin_uncertain.load(Ordering::Acquire) {
            return Err(LocalDbError::new(
                "uncertain NVMe-oF owner pins remain quarantined",
            ));
        }
        let pins = std::mem::take(&mut *self.nvmeof_epoch_pins.lock());
        let mut pins = pins.into_iter();
        while let Some(pin) = pins.next() {
            let result = pin
                .client
                .lock()
                .release_nvmeof_epoch_pin(pin.epoch, pin.token);
            if let Err(error) = result {
                let mut remaining = self.nvmeof_epoch_pins.lock();
                remaining.push(pin);
                remaining.extend(pins);
                self.nvmeof_pin_uncertain.store(true, Ordering::Release);
                self.state.poison();
                return Err(LocalDbError::component(error));
            }
        }
        Ok(())
    }

    // The caller has stopped admissions and drained the reactor. Close every
    // namespace FD before acknowledging any owner lease release.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn release_nvmeof_readers(&self) -> Result<(), LocalDbError> {
        let routing = self.state.nvme_routing.load_full();
        // Cold path: lock every reader in routing order, then check that none
        // has an accepted request before any of them is closed.
        let mut guards: Vec<_> = routing.routes.iter().map(sealed::NvmeRoute::lock).collect();
        if guards.iter_mut().any(|guard| {
            guard
                .dispatch()
                .is_some_and(|dispatch| !dispatch.active.is_empty())
        }) {
            return Err(LocalDbError::new("NVMe-oF requests have not drained"));
        }
        self.state.nvmeof_enabled.store(false, Ordering::Release);
        if !routing.routes.is_empty() {
            self.publish_routing(Vec::new());
        }
        let readers: Vec<_> = guards.iter_mut().filter_map(|guard| guard.take()).collect();
        drop(guards);
        let mut releases = Vec::with_capacity(readers.len());
        for dispatch in readers {
            let NvmeDispatch {
                reader,
                lease_client,
                lease_epoch,
                lease_token,
                ..
            } = dispatch;
            drop(reader);
            releases.push((lease_client, lease_epoch, lease_token));
        }
        let mut error = None;
        for (client, epoch, token) in releases {
            if let Err(cause) = client.lock().release_nvmeof_export(epoch, token) {
                self.nvmeof_pin_uncertain.store(true, Ordering::Release);
                self.state.poison();
                if error.is_none() {
                    error = Some(LocalDbError::component(cause));
                }
            }
        }
        match error {
            Some(error) => Err(error),
            None => self.release_nvmeof_epoch_pins(),
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn nvmeof_stats(&self) -> Vec<super::super::nvmeof::NvmeofStats> {
        self.state
            .nvme_routing
            .load()
            .routes
            .iter()
            .map(|route| route.catalog.snapshot())
            .collect()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn register_nvmeof_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), LocalDbError> {
        // Serialize with install and release so no reader-set change is missed.
        let _maintenance = self.nvmeof_maintenance.lock();
        let metrics = super::super::nvmeof::NvmeofMetrics::register(registry)
            .map_err(LocalDbError::component)?;
        metrics.replace_sources(
            self.state
                .nvme_routing
                .load()
                .routes
                .iter()
                .map(|route| route.catalog.stats().clone())
                .collect(),
        );
        self.state.nvmeof_metrics.lock().push(metrics);
        Ok(())
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn install_nvmeof_reader(
        &self,
        config: super::super::nvmeof::NvmeofReaderConfig,
        database: &RemoteDatabaseConfig,
        shape: &KeyShape,
    ) -> Result<(), LocalDbError> {
        self.install_nvmeof_with(
            config,
            database,
            shape,
            super::super::nvmeof::NvmeofReader::open_verified,
        )
    }

    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn install_nvmeof_file_fixture(
        &self,
        config: super::super::nvmeof::NvmeofReaderConfig,
        database: &RemoteDatabaseConfig,
        shape: &KeyShape,
        path: &std::path::Path,
    ) -> Result<(), LocalDbError> {
        self.install_nvmeof_with(config, database, shape, |config| {
            super::super::nvmeof::NvmeofReader::open_verified_file(config, path)
        })
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn install_nvmeof_with(
        &self,
        config: super::super::nvmeof::NvmeofReaderConfig,
        database: &RemoteDatabaseConfig,
        shape: &KeyShape,
        open: impl FnOnce(
            super::super::nvmeof::NvmeofReaderConfig,
        ) -> Result<
            super::super::nvmeof::NvmeofReader,
            super::super::nvmeof::NvmeofError,
        >,
    ) -> Result<(), LocalDbError> {
        let _maintenance = self.nvmeof_maintenance.lock();
        let _pause = self.pause_idle_nvmeof(false)?;
        let manifest = &config.manifest;
        let metadata_budget = config
            .max_inflight_members
            .checked_mul(2048)
            .and_then(|bytes| {
                config
                    .max_inflight_requests
                    .checked_mul(1024)
                    .and_then(|requests| bytes.checked_add(requests))
            })
            .and_then(|bytes| bytes.checked_add(config.max_catalog_bytes))
            .ok_or_else(|| LocalDbError::new("NVMe-oF metadata budget overflow"))?;
        let allowance = database
            .read_memory_allowance_bytes
            .saturating_sub(database.read_cache_bytes)
            .saturating_sub(database.max_pending_read_bytes);
        let reserved = self
            .state
            .nvme_routing
            .load()
            .routes
            .iter()
            .try_fold(0usize, |total, route| {
                total.checked_add(route.metadata_budget)
            })
            .ok_or_else(|| LocalDbError::new("NVMe-oF metadata budget overflow"))?;
        if metadata_budget > allowance.saturating_sub(reserved) {
            return Err(LocalDbError::new(
                "NVMe-oF metadata exceeds read memory allowance",
            ));
        }
        if manifest.database_uuid != database.storage.database
            || manifest.epoch > self.epoch
            || manifest.wal_file_bytes != database.storage.wal_file_bytes
            || manifest.catalog.is_empty()
            || manifest
                .catalog
                .len()
                .checked_mul(4096)
                .and_then(|bytes| bytes.checked_add(manifest.nqn.len()))
                .is_none_or(|bytes| bytes > config.max_catalog_bytes)
        {
            return Err(LocalDbError::new("NVMe-oF manifest authority mismatch"));
        }
        for entry in &manifest.catalog {
            for existing in &self.state.nvme_routing.load().routes {
                match existing
                    .catalog
                    .eligibility(&entry.reference)
                    .map_err(LocalDbError::component)?
                {
                    super::super::nvmeof::Eligibility::Eligible => {
                        return Err(LocalDbError::new("overlapping NVMe-oF catalogs"));
                    }
                    super::super::nvmeof::Eligibility::Ineligible => {}
                }
            }
        }
        let node = database
            .endpoints
            .iter()
            .position(|endpoint| endpoint.partition == manifest.partition)
            .ok_or_else(|| LocalDbError::new("NVMe-oF partition is not registered"))?;
        let client = {
            let lanes = self.lanes.read();
            lanes[node][0].client.clone()
        };
        for entries in manifest.catalog.chunks(1) {
            let references: Vec<_> = entries.iter().map(|entry| entry.reference).collect();
            let locations = client
                .lock()
                .validate_record_references(self.epoch, &references)
                .map_err(LocalDbError::component)?;
            if locations.len() != entries.len() {
                return Err(LocalDbError::new("NVMe-oF owner validation count mismatch"));
            }
            for (entry, location) in entries.iter().zip(locations) {
                if location.reference != entry.reference
                    || location.wal_file_bytes != manifest.wal_file_bytes
                    || location.source_file_index != manifest.source_file_index
                    || location.source_device != manifest.source_device
                    || location.source_inode != manifest.source_inode
                    || location.source_file_len != manifest.image_len
                    || location.durable_frontier != manifest.durable_frontier
                    || location.offset != entry.offset
                    || location.len != entry.len as u64
                    || entry.reference.batch_address.partition != manifest.partition
                    || entry.reference.batch_address.segment_generation != manifest.generation
                    || entry.reference.batch_version.epoch != manifest.epoch
                {
                    return Err(LocalDbError::new(
                        "NVMe-oF catalog differs from owner authority",
                    ));
                }
            }
        }
        // Direct reads require the entire coordinator epoch, including owners
        // without an export, to remain pinned for the reader lifetime.
        let first_reader = self.acquire_nvmeof_epoch_pins()?;
        // Admission is serialized with the owner's authority transitions. A
        // lost reply is uncertain and therefore poisons this coordinator.
        let mut binding = super::super::nvmeof::lease::ExportBinding::from(manifest);
        binding.target.clone_from(&config.binding.target);
        let token = client
            .lock()
            .register_nvmeof_export(self.epoch, binding, manifest.catalog[0].reference)
            .map_err(|error| {
                self.nvmeof_pin_uncertain.store(true, Ordering::Release);
                self.state.poison();
                LocalDbError::component(error)
            })?;
        let opened = open(config)
            .map_err(LocalDbError::component)
            .and_then(|reader| {
                reader
                    .validate_authority(
                        database.storage.database,
                        self.epoch,
                        &database.storage.partitions,
                    )
                    .map_err(LocalDbError::component)?;
                Ok(reader)
            });
        let reader = match opened {
            Ok(reader) => reader,
            Err(error) => {
                if client
                    .lock()
                    .release_nvmeof_export(self.epoch, token)
                    .is_err()
                {
                    self.nvmeof_pin_uncertain.store(true, Ordering::Release);
                    self.state.poison();
                } else if first_reader {
                    let _ = self.release_nvmeof_epoch_pins();
                }
                return Err(error);
            }
        };
        let route = sealed::NvmeRoute {
            catalog: reader.catalog().clone(),
            node,
            metadata_budget,
            cell: Arc::new(sealed::NvmeDispatchCell::new(NvmeDispatch {
                reader,
                lease_client: client,
                lease_epoch: self.epoch,
                lease_token: token,
                shape: Arc::new(shape.clone()),
                next: 0,
                active: HashMap::new(),
            })),
        };
        let mut routes = self.state.nvme_routing.load().routes.clone();
        routes.push(route);
        self.publish_routing(routes);
        self.state.nvmeof_enabled.store(true, Ordering::Release);
        Ok(())
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
    pub(super) fn stop_with_clients(&self) -> Vec<transports::SharedClient> {
        self.stop_actor();
        self.lanes
            .write()
            .drain(..)
            .flatten()
            .map(|lane| lane.client)
            .collect()
    }
    fn stop_actor(&self) {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let _maintenance = self.nvmeof_maintenance.lock();
        self.state.stopped.store(true, Ordering::Release);
        self.state.notify_batches();
        self.notify_admission();
        self.sender.lock().clear();
        // Serialize all shutdown callers through join and lease release. A
        // second caller must not release an owner token while the first one is
        // still waiting for the reactor's terminal completions.
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.release_nvmeof_readers().is_err() {
            self.poison();
        }
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

    pub(super) fn wait_admission(
        &self,
        observed: u64,
        deadline: Instant,
    ) -> Result<(), LocalDbError> {
        let mut retained = self.state.retained.lock();
        // The absolute deadline bounds retries even when other callers keep
        // releasing credits before this caller can reserve them.
        loop {
            if self.failed() || self.state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is poisoned or closed"));
            }
            if Instant::now() >= deadline {
                return Err(LocalDbError::new("remote read admission deadline expired"));
            }
            if self.admission_generation() != observed {
                return Ok(());
            }
            let _ = self
                .state
                .admission_changed
                .wait_until(&mut retained, deadline);
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
        profile: Option<&mut super::super::tcp::TcpReadProfile>,
        pin: Option<IndexReadPin>,
    ) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        self.read_inline_traced(node, epoch, reference, extra_bytes, profile, pin, None)
    }

    // Keep routing, pin ownership and the optional borrowed trace explicit.
    #[allow(clippy::too_many_arguments)]
    fn read_inline_traced(
        &self,
        node: usize,
        epoch: u64,
        reference: &ValueRecordReference,
        extra_bytes: usize,
        profile: Option<&mut super::super::tcp::TcpReadProfile>,
        pin: Option<IndexReadPin>,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        if self.failed() || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if self.nvmeof_enabled() {
            return Ok(None);
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.state.live_reads {
            if !self.state.live_inline_reads {
                return Ok(None);
            }
            return self.read_live_inline(node, epoch, reference, extra_bytes, pin, trace);
        }
        let bytes = member_charge(reference)?
            .checked_add(extra_bytes)
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| LocalDbError::new("read budget overflow"))?;
        if bytes > self.max_bytes {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let mut flights = self.state.flights.lock();
        if flights.find(node, reference).is_some() {
            return Ok(None);
        }
        let flight_slot = flights
            .available_slot(&self.state.flight_ready)
            .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?;
        if !flights.reserve_entries(1, &self.state.flight_ready) {
            return Err(LocalDbError::new(BACKPRESSURE));
        }
        let (lane, reservation) = {
            let mut retained = self.state.retained.lock();
            if self.failed() {
                return Err(LocalDbError::new("read authority is poisoned"));
            }
            if self.state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is closed"));
            }
            if self.state.admission_paused.load(Ordering::Acquire) {
                return Err(LocalDbError::new(BACKPRESSURE));
            }
            if self.nvmeof_enabled() {
                return Ok(None);
            }
            if retained.0 == self.max_requests || bytes > self.max_bytes - retained.1 {
                return Ok(None);
            }
            let lane_guard = self.lanes.read();
            let Some(lanes) = lane_guard.get(node) else {
                return Err(LocalDbError::new("read reactor stopped"));
            };
            let next = self.next[node].fetch_add(1, Ordering::Relaxed);
            let mut selected = None;
            for offset in 0..lanes.len() {
                let lane = &lanes[next.wrapping_add(offset) % lanes.len()];
                let mut scheduling = lane.scheduling.lock();
                if !scheduling.inline_reserved && lane.outstanding.load(Ordering::Acquire) == 0 {
                    scheduling.inline_reserved = true;
                    lane.outstanding.fetch_add(1, Ordering::AcqRel);
                    selected = Some((lane.clone(), InlineLaneReservation(lane.scheduling.clone())));
                    break;
                }
            }
            let Some((lane, reservation)) = selected else {
                return Ok(None);
            };
            retained.0 += 1;
            retained.1 += bytes;
            (lane, reservation)
        };
        let credit = Arc::new(Credit {
            state: self.state.clone(),
            bytes,
            requests: 1,
        });
        let _reservation = reservation;
        let _lane_credit = LaneCredit {
            outstanding: lane.outstanding.clone(),
        };
        let Some(mut client) = lane.client.try_lock() else {
            return Ok(None);
        };
        if let Some(trace) = trace {
            trace.classify_members(std::iter::empty());
            trace.lane(lane.index, flight_slot);
            trace.event("inline_lane_acquired");
        }
        self.state.flight_ready[flight_slot].begin();
        let flight = Arc::new(ReadFlight {
            node,
            references: vec![*reference],
            slot: flight_slot,
            grouped_inline: AtomicBool::new(false),
            completion: FlightCompletion::new(),
            _pin: pin,
            _credit: credit,
        });
        flights.insert(&flight);
        drop(flights);
        #[cfg(test)]
        if let Some((entered, resume)) = self.state.inline_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        let _poison = PoisonOnPanic(&self.state);
        let sampled = self.state.diagnostics.select();
        let started = sampled.then(Instant::now);
        let transport_before = sampled.then(|| {
            client.profile_read_transport(true);
            client.transport_progress_stats()
        });
        let message_requests = client.message_read_requests();
        let result = if sampled || profile.is_some() {
            let result = client.read_records_profiled(epoch, std::slice::from_ref(reference));
            if sampled {
                let elapsed_ns = elapsed(started.unwrap());
                let measured = result.as_ref().ok().map(|(_, tcp)| *tcp).or_else(|| {
                    result
                        .as_ref()
                        .err()
                        .and_then(|error| error.read_profile.as_deref().copied())
                });
                self.state
                    .diagnostics
                    .inline_complete(elapsed_ns, measured, result.is_err());
                self.state.owner_diagnostics[node].1.inline_complete(
                    elapsed_ns,
                    measured,
                    result.is_err(),
                );
            }
            result.map(|(values, measured)| {
                if let Some(profile) = profile {
                    profile.add(measured);
                }
                values
            })
        } else {
            client.read_records(epoch, std::slice::from_ref(reference))
        };
        kv_capture::reference(15, reference, None, 1, flight_slot as u64);
        if client.message_read_requests() != message_requests {
            self.state.read_rpcs.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(before) = transport_before {
            self.state
                .diagnostics
                .transport_delta(before, client.transport_progress_stats());
            client.profile_read_transport(false);
        }
        let result = result.map_err(|error| {
            self.state.poison();
            LocalDbError::component(error)
        });
        *flight.completion.result.lock() = Some(match &result {
            Ok(values) => Ok((values.clone(), None)),
            Err(error) => Err(copied_read_error(error)),
        });
        // The transport has completed and the scalar result is now owned by
        // this caller. Release producer ownership before waking coalesced
        // consumers; their flight references retain the completion and credit.
        drop(client);
        drop(_lane_credit);
        drop(_reservation);
        drop(flight);
        self.state.flight_ready[flight_slot].retire();
        self.state.notify_admission();
        #[cfg(test)]
        if let Some((entered, resume)) = self.state.inline_published_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        result.map(Some)
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
            ReadContext {
                deadline: Instant::now() + self.timeout,
                pin,
                trace: None,
            },
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
        self.submit_until_owned(node, references, extra_bytes, profiled, context, None, None)
    }
    // Admission takes its context, optional lane ownership and optional
    // precomputed member plans explicitly; each belongs to a distinct concern.
    #[allow(clippy::too_many_arguments)]
    fn submit_until_owned(
        &self,
        node: usize,
        references: &[ValueRecordReference],
        extra_bytes: usize,
        profiled: bool,
        context: ReadContext,
        mut owned_lanes: Option<&mut Vec<OwnedLane>>,
        planned: Option<PlannedMembers<'_>>,
    ) -> Result<ReadRequest, LocalDbError> {
        let ReadContext {
            deadline,
            pin,
            trace,
        } = context;
        if let Some(trace) = &trace {
            trace.event("admission_started");
        }
        if self.failed() || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if references.is_empty() || references.len() > 256 {
            return Err(LocalDbError::new("read request count exceeds bound"));
        }
        let routing = self.routing();
        // Each member's authority lookup, alignment and charge are computed
        // once here, or by the batch that owns the plans, and reused by the
        // charge sum, the reader plan and every retry of this request.
        let mut computed = SmallVec::<[MemberPlan; 32]>::new();
        let plans: &[MemberPlan] = if let Some(planned) = planned {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if planned.generation != routing.generation {
                return Err(self.backpressure(&self.state.diagnostics.admission_retry_config));
            }
            planned.members
        } else {
            for reference in references {
                computed.push(self.plan_member(&routing, reference)?);
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
        if bytes > self.max_bytes {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let mut flights = self.state.flights.lock();
        let mut missing = Vec::new();
        let mut missing_plans = SmallVec::<[MemberPlan; 32]>::new();
        let mut parts: SmallVec<[(Option<Arc<ReadFlight>>, usize); 1]> =
            SmallVec::with_capacity(references.len());
        let mut missing_index = (references.len() > 1).then(HashMap::new);
        for (position, reference) in references.iter().enumerate() {
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
                        missing_plans.push(plans[position]);
                        missing.len() - 1
                    })
                } else {
                    missing.push(*reference);
                    missing_plans.push(plans[position]);
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
        // One request goes to one reader or to none. A mixed request is
        // split by the batch dispatcher, so this is a routing change, not load.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let sealed = {
            let route = missing_plans
                .first()
                .and_then(|plan| plan.sealed)
                .map(|member| member.route);
            if missing_plans
                .iter()
                .any(|plan| plan.sealed.map(|member| member.route) != route)
            {
                return Err(self.backpressure(&self.state.diagnostics.admission_retry_mixed));
            }
            let members: SmallVec<[sealed::SealedMember; 32]> = missing_plans
                .iter()
                .filter_map(|plan| plan.sealed)
                .collect();
            routing.sealed_plan(&members)?
        };
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
        let mut reserved = false;
        let (credit, lane) = {
            let mut retained = self.state.retained.lock();
            // The reader set may change only while admission is paused, so a
            // changed snapshot means the plans above are stale. Credit
            // releases never advance this generation.
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if self.state.nvme_routing.load().generation != routing.generation {
                return Err(self.backpressure(&self.state.diagnostics.admission_retry_config));
            }
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
                || bytes > self.max_bytes - retained.1
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
                                let mut scheduling = lane.scheduling.lock();
                                if (scheduling.inline_reserved
                                    && (idle_only || !scheduling.grouped_reserved))
                                    || (idle_only && lane.outstanding.load(Ordering::Acquire) != 0)
                                {
                                    return None;
                                }
                                if idle_only && owned_lanes.is_some() && !self.nvmeof_enabled() && {
                                    #[cfg(all(
                                        feature = "experimental-nvmeof",
                                        target_os = "linux"
                                    ))]
                                    {
                                        !self.state.live_reads
                                    }
                                    #[cfg(not(all(
                                        feature = "experimental-nvmeof",
                                        target_os = "linux"
                                    )))]
                                    {
                                        true
                                    }
                                } {
                                    scheduling.inline_reserved = true;
                                    scheduling.grouped_reserved = true;
                                    reserved = true;
                                } else if scheduling.grouped_reserved {
                                    scheduling.grouped_handoff_requested = true;
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
                trace.lane(lane.index, flight_slot.unwrap());
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
                grouped_inline: AtomicBool::new(false),
                completion: FlightCompletion::new(),
                _pin: pin,
                _credit: credit.clone(),
            });
            flights.insert(&flight);
            let request = Request {
                lane: lane.index,
                references: missing,
                deadline,
                submitted: (profiled || sampled).then(Instant::now),
                sampled,
                trace: trace.clone(),
                flight: flight.clone(),
                _credit: credit.clone(),
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                sealed,
                _lane_credit: LaneCredit {
                    outstanding: lane.outstanding.clone(),
                },
            };
            let sender_guard = self.sender.lock();
            let Some(sender) = sender_guard.get(node) else {
                if reserved {
                    drop(InlineLaneReservation(lane.scheduling));
                }
                drop(request);
                drop(sender_guard);
                self.state.flight_ready[slot].retire();
                return Err(LocalDbError::new("read reactor stopped"));
            };
            let sender = sender.clone();
            drop(sender_guard);
            if reserved {
                let reservation = InlineLaneReservation(lane.scheduling.clone());
                if lane.client.try_lock().is_some()
                    && let Some(mut driver) = lane.driver.try_lock()
                {
                    flight.grouped_inline.store(true, Ordering::Release);
                    driver.queued.push_back(request);
                    drop(driver);
                    owned_lanes
                        .as_mut()
                        .expect("reserved for batch")
                        .push(OwnedLane {
                            lane: lane.clone(),
                            reservation: Some(reservation),
                            sender,
                            state: self.state.clone(),
                            node,
                        });
                    drop(flights);
                    self.state
                        .coalesced_members
                        .fetch_add(coalesced_members as u64, Ordering::Relaxed);
                    let parts = parts
                        .into_iter()
                        .map(|(existing, slot)| (existing.unwrap_or_else(|| flight.clone()), slot))
                        .collect();
                    return Ok(ReadRequest {
                        parts,
                        credit,
                        deadline,
                        sampled,
                        trace,
                        profile: Some(flight),
                        subscription: None,
                    });
                }
                drop(reservation);
            }
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
                    existing.unwrap_or_else(|| flight.as_ref().unwrap().clone()),
                    slot,
                )
            })
            .collect();
        drop(flights);
        Ok(ReadRequest {
            parts,
            credit,
            deadline,
            sampled,
            trace,
            profile: flight,
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
    pub fn wait(self) -> CompletedRecord {
        let started = Instant::now();
        let result = match self.ready {
            Some(value) => Ok(value),
            None => self
                .read
                .expect("pending record owns read")
                .finish()
                .and_then(|(mut values, _)| {
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

type ReadOutput = (Vec<Mutation>, Option<WorkerReadProfile>);
type ReadCompletion = Result<ReadOutput, LocalDbError>;

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
        self.reads
            .state
            .diagnostics
            .limit
            .store(limit, Ordering::Relaxed);
        self.reads
            .state
            .diagnostics
            .remaining
            .store(limit, Ordering::Relaxed);
        self.reads.state.coordinator_diagnostics.arm(limit);
    }
    /// Cumulative first-N sampled RPC counters, never per-request means.
    /// Server spans nest inside exchange/service. Failures without a profiled
    /// owner reply are explicitly counted as unavailable.
    pub fn read_profile_monitor(&self) -> ReadProfileMonitor {
        ReadProfileMonitor {
            state: self.reads.state.clone(),
            fabric: self.config.tls.transport.is_fabric(),
        }
    }
    pub fn read_rpc_count(&self) -> u64 {
        self.reads.state.read_rpcs.load(Ordering::Relaxed)
    }
    pub fn submit_record(
        &mut self,
        reference: ValueRecordReference,
    ) -> Result<PendingRecord, LocalDbError> {
        self.ensure_healthy()?;
        let pin = self
            .index
            .pin_cut(IndexReadCut::At(MutationVersion {
                batch: reference.batch_version,
                operation_index: reference.descriptor.operation_index,
            }))
            .map_err(LocalDbError::component)?;
        let ready = self.read_cache.get_scan(&reference);
        let read = if ready.is_none() {
            let node = *self
                .partition_indexes
                .get(&reference.batch_address.partition)
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
            Some(self.reads.submit_until(
                node,
                std::slice::from_ref(&reference),
                0,
                false,
                ReadContext {
                    deadline: Instant::now() + self.config.timeout,
                    pin: Some(pin),
                    trace: None,
                },
            )?)
        } else {
            None
        };
        Ok(PendingRecord {
            owner: self.reads.state.clone(),
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
        self.stats.value_read_ns += completed.wait_ns;
        if !Arc::ptr_eq(&self.reads.state, &completed.owner) {
            return Err(LocalDbError::new("record belongs to another authority"));
        }
        self.ensure_healthy()?;
        let mutation = completed.result.inspect_err(|_| self.healthy = false)?;
        if matches!(mutation, Mutation::Delete { .. })
            || mutation.keyspace() != ks
            || completed.reference.descriptor.keyspace != ks.0
            || self.shape.ks(ks).reduce_key(mutation.key()).as_ref() != reduced_key
        {
            self.healthy = false;
            return Err(LocalDbError::new(
                "scan member does not match its live index reference",
            ));
        }
        Ok(mutation)
    }

    pub fn finish_ready_get(
        &mut self,
        completed: CompletedGet,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.stats.value_read_ns += completed.wait_ns;
        self.check_handle(&completed.pending)?;
        match completed.pending.state {
            GetState::Ready(value) => Ok(value),
            GetState::Resolved { reference, ks, key } => {
                let (values, _) = completed
                    .result
                    .expect("resolved read has completion")
                    .inspect_err(|_| self.healthy = false)?;
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
    pub(super) fn prepare_get(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
        mut profile: Option<&mut RemoteReadProfile>,
        inline: bool,
    ) -> Result<PendingGet, LocalDbError> {
        let mut sample = self.reads.state.coordinator_diagnostics.select(false, 1);
        self.ensure_healthy()?;
        let index_started = Instant::now();
        let (reference, pin, cached) = {
            // Resolve the cut once; the lookup and a miss's pin must see the
            // same cut. No lock or guard is held past each call.
            let cut = self.index.resolve_cut(cut);
            let clock = ResolutionSample::start(&sample);
            let reference = self
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
                let cached = self.read_cache.get_point(&reference, ks, key);
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::CacheLookup, clock);
                }
                cached
            } else {
                None
            };
            let pin = if reference.is_some() && cached.is_none() {
                let clock = ResolutionSample::start(&sample);
                let pin = self.index.pin_cut(cut).map_err(LocalDbError::component);
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
        self.stats.read_resolve_ns += index_ns;
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
                    self.reads
                        .read_inline(
                            node,
                            self.epoch,
                            &reference,
                            key.len(),
                            profile.as_deref_mut().map(|profile| &mut profile.tcp),
                            pin.clone(),
                        )
                        .inspect_err(|error| {
                            if !admission_error(error) {
                                self.healthy = false;
                            }
                        })?
                } else {
                    None
                };
                if let Some(values) = immediate {
                    let elapsed = elapsed(inline_started.unwrap());
                    self.stats.value_read_ns += elapsed;
                    if let Some(profile) = profile {
                        profile.inline_read_ns += elapsed;
                    }
                    GetState::Ready(self.complete_get(reference, ks, key, values)?)
                } else {
                    let read = self.reads.submit(
                        node,
                        vec![reference],
                        key.len(),
                        profile.is_some(),
                        pin,
                    )?;
                    if let Some(profile) = profile {
                        profile.dispatch_ns += elapsed(dispatch_started.unwrap());
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
            owner: self.reads.state.clone(),
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
        }
    }
    fn check_handle(&mut self, pending: &PendingGet) -> Result<(), LocalDbError> {
        if !Arc::ptr_eq(&self.reads.state, &pending.owner) {
            return Err(LocalDbError::new(
                "read handle belongs to another authority/epoch",
            ));
        }
        if self.reads.failed() {
            self.healthy = false;
        }
        self.ensure_healthy()
    }
    fn complete_get(
        &mut self,
        reference: ValueRecordReference,
        ks: KeySpace,
        key: &[u8],
        mut values: Vec<Mutation>,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.ensure_healthy()?;
        if values.len() != 1 {
            self.healthy = false;
            return Err(LocalDbError::new("point read count mismatch"));
        }
        let mutation = values.pop().unwrap();
        if matches!(mutation, Mutation::Delete { .. }) {
            self.healthy = false;
            return Err(LocalDbError::new(
                "live index reference resolves to tombstone",
            ));
        }
        let prepared = self.read_cache.prepare_verified(reference, &mutation);
        self.read_cache.insert_prepared(prepared);
        Self::point_value(ks, key, mutation)
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
                Ok(None) if Instant::now() < read.deadline => return Ok(None),
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
        self.stats.value_read_ns += elapsed(read.deadline - self.config.timeout);
        let (values, _) = result?;
        self.complete_get(reference, ks, &key, values).map(Some)
    }
    pub fn finish_get(&mut self, pending: PendingGet) -> Result<Option<Bytes>, LocalDbError> {
        self.finish_get_profiled(pending, None)
    }
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
                let value_started = read.deadline - self.config.timeout;
                let wait_started = profile.as_ref().map(|_| Instant::now());
                let result = read.finish();
                self.stats.value_read_ns += elapsed(value_started);
                let (values, worker) = result.inspect_err(|_| {
                    self.healthy = false;
                })?;
                if let (Some(profile), Some(worker)) = (profile, worker) {
                    profile.wait_ns += elapsed(wait_started.unwrap());
                    profile.actor_return_ns += elapsed(worker.finished);
                    profile.actor_queue_ns += worker.queue_ns;
                    profile.actor_service_ns += worker.service_ns;
                    add_tcp_profile(&mut profile.tcp, worker.tcp);
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
mod locality_tests;

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
mod live_worker_tests {
    use super::*;
    use crate::distributed::remote_db::pipeline_tests::Fixture;

    #[test]
    fn live_grant_barrier_waits_for_lane_owner() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = 1;
        let mut db = fixture.open();
        let lanes = db.reads.lanes.read()[0].clone();
        let client = lanes[0].client.clone();
        let base = lanes[0].index;
        let (sender, receiver) = mpsc::sync_channel(db.reads.max_requests + 1);
        let state = db.reads.state.clone();
        let epoch = db.reads.epoch;
        let max_requests = db.reads.max_requests;
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
        assert!(!db.reads.failed());
        db.shutdown().unwrap();
    }

    #[test]
    fn retirement_pause_preserves_outstanding_read_and_can_retry() {
        let mut fixture = Fixture::new();
        fixture.config.read_cache_bytes = 0;
        fixture.config.read_lanes = 1;
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 0), vec![super::tests::put(0, 7)])
            .unwrap();
        let reference = db
            .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        let node = db.partition_indexes[&reference.batch_address.partition];
        let lane = db.reads.lane_client(node, 0);
        let held = lane.lock();
        let request = db
            .reads
            .submit(node, vec![reference], 0, false, None)
            .unwrap();
        assert!(db.reads.pause_idle_nvmeof(false).is_err());
        assert!(!db.reads.failed());
        drop(held);
        let (values, _) = request.finish().unwrap();
        assert_eq!(values, vec![super::tests::put(0, 7)]);
        drop(db.reads.pause_idle_nvmeof(false).unwrap());
        db.shutdown().unwrap();
    }

    #[test]
    fn live_worker_propagates_real_client_configuration_failure_and_retires_credit() {
        // A normal authenticated TCP connection is deliberately not configured
        // as live. This exercises the real worker failure, not a fake device.
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = 1;
        fixture.config.read_cache_bytes = 0;
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 0), vec![super::tests::put(0, 7)])
            .unwrap();
        let reference = db
            .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        let node = db.partition_indexes[&reference.batch_address.partition];
        let lanes = db.reads.lanes.read()[node].clone();
        let base = lanes[0].index;
        let (sender, receiver) = mpsc::sync_channel(db.reads.max_requests + 1);
        let state = db.reads.state.clone();
        let epoch = db.reads.epoch;
        let max_requests = db.reads.max_requests;
        let worker = thread::spawn(move || {
            drive_live_reads(state, receiver, lanes, node, base, epoch, max_requests)
        });
        db.reads.sender.lock()[node] = sender;
        let request = db
            .reads
            .submit(node, vec![reference], 0, false, None)
            .unwrap();
        let error = match request.finish() {
            Err(error) => error,
            Ok(_) => panic!("unconfigured live worker unexpectedly succeeded"),
        };
        assert!(error.to_string().contains("live NVMe is not configured"));
        assert!(db.reads.failed());
        assert_eq!(*db.reads.state.retained.lock(), (0, 0));
        db.reads.sender.lock().clear();
        worker.join().unwrap();
    }

    #[test]
    fn missing_and_before_all_cut_do_not_send_live_payload_work() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = 1;
        fixture.config.read_cache_bytes = 0;
        let mut original = fixture.open();
        original
            .write_batch(fixture.operation(0, 0), vec![super::tests::put(0, 7)])
            .unwrap();
        let db = ConcurrentRemoteDatabase::new(original).unwrap();
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
        // The workers have real but intentionally unconfigured clients: any
        // accidentally submitted payload read would fail rather than succeed.
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
        assert_eq!(*db.reads.state.retained.lock(), (0, 0));
        db.reads.sender.lock().clear();
        for worker in workers {
            worker.join().unwrap();
        }
        db.shutdown().unwrap();
    }

    #[test]
    fn full_lane_inbox_never_blocks_the_actor() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = 1;
        fixture.config.read_cache_bytes = 0;
        let mut db = fixture.open();
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
        let node = db.partition_indexes[&references[0].batch_address.partition];
        let same_owner: Vec<_> = references
            .into_iter()
            .filter(|reference| db.partition_indexes[&reference.batch_address.partition] == node)
            .take(6)
            .collect();
        assert!(same_owner.len() >= 5, "need distinct flights on one lane");
        let lanes = db.reads.lanes.read()[node].clone();
        let base = lanes[0].index;
        let client = lanes[0].client.clone();
        let (sender, receiver) = mpsc::sync_channel(db.reads.max_requests + 1);
        let state = db.reads.state.clone();
        let epoch = db.reads.epoch;
        // A lane inbox of two entries, and a worker stuck behind the client lock.
        let held = client.lock();
        let actor =
            thread::spawn(move || drive_live_reads(state, receiver, lanes, node, base, epoch, 2));
        db.reads.sender.lock()[node] = sender.clone();
        let pending: Vec<_> = same_owner
            .iter()
            .map(|reference| {
                db.reads
                    .submit(node, vec![*reference], 0, false, None)
                    .unwrap()
            })
            .collect();
        let diagnostics = &db.reads.state.diagnostics;
        let deadline = Instant::now() + Duration::from_secs(5);
        while diagnostics.live_lane_full.load(Ordering::Relaxed) == 0 {
            assert!(Instant::now() < deadline, "the lane inbox never filled");
            thread::yield_now();
        }
        // The actor still serves its own channel while one lane is full.
        db.reads.state.wake_pending[node].store(true, Ordering::Release);
        assert!(sender.try_send(ReadCommand::Wake).is_ok());
        while db.reads.state.wake_pending[node].load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "the actor blocked on a full lane"
            );
            thread::yield_now();
        }
        assert!(diagnostics.live_lane_pending_max.load(Ordering::Relaxed) >= 1);
        assert!(diagnostics.live_lane_depth_max.load(Ordering::Relaxed) <= 2);
        drop(held);
        // Unconfigured live clients fail every request; none may be lost.
        for request in pending {
            assert!(request.finish().is_err());
        }
        assert_eq!(*db.reads.state.retained.lock(), (0, 0));
        drop(sender);
        db.reads.sender.lock().clear();
        actor.join().unwrap();
        drop(db);
    }
}
