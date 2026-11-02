//! Bounded coordinator-owned reads, driven by one nonblocking connection owner.
//! Exact-reference fill sharing adapts native DecompressCache::FillCell and
//! CacheLib's fill-context waiters, not its mutable-key tombstone protocol:
//! <https://github.com/facebook/CacheLib/blob/a24370092f0bd6fa7145b2f01c3e50e63edd6a6e/cachelib/allocator/nvmcache/NvmCache.h#L767-L848>.
//! A fixed weak registry is reserved from the pending byte allowance. Every
//! caller owns bounded result/slot credits; only new storage work owns a request
//! credit. Pins and authenticated completion checks survive delivery cancellation.
use super::*;
use crate::distributed::native_index::IndexReadPin;
use smallvec::SmallVec;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

const BACKPRESSURE: &str = "remote read admission is full";
const BYTE_BUDGET: &str = "request exceeds pending read byte budget";

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
    Ok(wire * 3 + 3 * std::mem::size_of::<ValueRecordReference>() + 64)
}

pub(super) fn admission_error(error: &LocalDbError) -> bool {
    matches!(error.to_string().as_str(), BACKPRESSURE | BYTE_BUDGET)
}

struct ReadContext {
    deadline: Instant,
    pin: Option<IndexReadPin>,
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
    groups: VecDeque<(usize, VecDeque<(usize, ValueRecordReference)>)>,
    owned_lanes: Vec<OwnedLane>,
    state: Arc<State>,
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
        // Reject an impossible member before issuing any of this batch's RPCs.
        for (_, reference) in groups.iter().flatten() {
            if service.member_charge(reference)? > service.max_bytes.saturating_sub(512) {
                return Err(LocalDbError::new(BYTE_BUDGET));
            }
        }
        Ok(Self {
            values,
            class: ReadClass::Point,
            pin: None,
            reads: VecDeque::new(),
            deadline: Instant::now() + service.timeout,
            owned_lanes: Vec::new(),
            state: service.state.clone(),
            epoch: service.epoch,
            groups: groups
                .into_iter()
                .enumerate()
                .filter(|(_, group)| !group.is_empty())
                .map(|(node, group)| (node, group.into()))
                .collect(),
        })
    }

    pub(super) fn has_unsubmitted(&self) -> bool {
        !self.groups.is_empty()
    }

    pub(super) fn complete(&self) -> bool {
        self.groups.is_empty() && self.reads.is_empty()
    }

    pub(super) fn dispatch(&mut self, service: &ReadService) -> Result<(), LocalDbError> {
        let mut skipped = 0;
        while skipped < self.groups.len() {
            let (node, group) = self.groups.front_mut().expect("nonempty groups");
            let retained = *service.state.retained.lock();
            let available = service.max_bytes - retained.1;
            let mut bytes = 512usize;
            let mut count = 0;
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            let first_eligible = service.nvmeof_eligible(&group.front().unwrap().1)?;
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            let mut nvme_bytes = 0usize;
            for (_, reference) in group.iter().take(256) {
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                if service.nvmeof_eligible(reference)? != first_eligible {
                    break;
                }
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                if let Some(index) = first_eligible {
                    let (member_bytes, max_bytes, max_members) =
                        service.nvmeof_limits(index, reference)?;
                    if member_bytes > max_bytes || max_members == 0 {
                        return Err(LocalDbError::new("record exceeds NVMe-oF reader capacity"));
                    }
                    if count >= max_members || member_bytes > max_bytes.saturating_sub(nvme_bytes) {
                        break;
                    }
                    nvme_bytes += member_bytes;
                }
                let charge = service.member_charge(reference)?;
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
            let (slots, references) = group.iter().take(count).copied().unzip();
            let request = match service.submit_until_owned(
                *node,
                references,
                0,
                false,
                ReadContext {
                    deadline: self.deadline,
                    pin: self.pin.clone(),
                },
                (self.class == ReadClass::Point).then_some(&mut self.owned_lanes),
            ) {
                Ok(request) => request,
                Err(error) if RemoteDatabase::read_backpressured(&error) => {
                    self.groups.rotate_left(1);
                    skipped += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
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
        let state = self
            .reads
            .front()
            .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?
            .1
            .credit
            .state
            .clone();
        // The same generation/condition variable used for admission also
        // advances after flight readiness is published. Hold its mutex while
        // checking all flights to avoid sleeping through a completion.
        let mut empty_sweeps = 0u8;
        let ready = loop {
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
            let mut retained = state.retained.lock();
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
                drop(retained);
                state.poison();
                return Err(LocalDbError::new(
                    "remote read deadline expired or worker stopped",
                ));
            }
            if self.owned_lanes.is_empty() {
                state
                    .admission_changed
                    .wait_until(&mut retained, self.deadline);
            } else {
                drop(retained);
                thread::yield_now();
            }
        };
        self.owned_lanes
            .retain(|owned| owned.lane.outstanding.load(Ordering::Acquire) != 0);
        let (slots, request) = self.reads.remove(ready).expect("ready request exists");
        let (values, _) = request.finish()?;
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
                set("read_profile_server_available", u64::from(
                    self.server_response_recheck_samples.load(Ordering::Relaxed) != 0));
                set("read_profile_transport_progress_available", u64::from(
                    self.transport_sweeps.load(Ordering::Relaxed) != 0));
                set("read_profile_server_executor_available", u64::from(self.server_executor_requests.load(Ordering::Relaxed) != 0));
                set("read_profile_server_planning_available", u64::from(self.server_planning_count.load(Ordering::Relaxed) != 0));
                set("read_profile_server_nowait_available", u64::from(self.server_nowait_count.load(Ordering::Relaxed) != 0));
                set("read_profile_server_async_available", u64::from(self.server_async_count.load(Ordering::Relaxed) != 0));
                set("read_profile_server_response_ready_available", u64::from(self.server_response_ready_count.load(Ordering::Relaxed) != 0));
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
struct FlightSignal {
    ready: AtomicBool,
    wait_lock: Mutex<()>,
    changed: parking_lot::Condvar,
    #[cfg(test)]
    waiters: AtomicUsize,
}
impl FlightSignal {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(true),
            wait_lock: Mutex::new(()),
            changed: parking_lot::Condvar::new(),
            #[cfg(test)]
            waiters: AtomicUsize::new(0),
        }
    }
    fn begin(&self) {
        self.ready.store(false, Ordering::Release);
    }
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    fn retire(&self) {
        self.ready.store(true, Ordering::Release);
        let _wait_lock = self.wait_lock.lock();
        self.changed.notify_all();
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
}

struct State {
    failed: AtomicBool,
    stopped: AtomicBool,
    retained: Mutex<(usize, usize)>,
    admission_paused: AtomicBool,
    admission_generation: AtomicU64,
    admission_changed: parking_lot::Condvar,
    wake_pending: AtomicBool,
    // Fixed flight signals and weak slots are reserved from pending bytes.
    flight_ready: Box<[FlightSignal]>,
    read_rpcs: AtomicU64,
    diagnostics: ReadDiagnostics,
    coordinator_diagnostics: CoordinatorResolutionDiagnostics,
    owner_diagnostics: Box<[(u32, ReadDiagnostics)]>,
    flights: Mutex<Box<[Weak<ReadFlight>]>>,
    coalesced_members: AtomicU64,
    flight_metadata_bytes: usize,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof: Mutex<Vec<NvmeDispatch>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_metrics: Mutex<Vec<super::super::nvmeof::NvmeofMetrics>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_enabled: AtomicBool,
    #[cfg(test)]
    peak_connection_reads: AtomicUsize,
    #[cfg(test)]
    inline_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    inline_published_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    completion_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    reactor_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct NvmeDispatch {
    reader: super::super::nvmeof::NvmeofReader,
    lease_client: Arc<Mutex<Client>>,
    lease_epoch: u64,
    lease_token: [u8; 16],
    shape: Arc<KeyShape>,
    metadata_budget: usize,
    next: u64,
    active: HashMap<u64, Request>,
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct NvmeEpochPin {
    client: Arc<Mutex<Client>>,
    epoch: u64,
    token: [u8; 16],
}

impl State {
    fn poison(&self) {
        self.failed.store(true, Ordering::Release);
        self.notify_admission();
    }
    fn notify_admission(&self) {
        let _retained = self.retained.lock();
        self.admission_generation.fetch_add(1, Ordering::Release);
        self.admission_changed.notify_all();
    }
}
struct PoisonOnPanic(Arc<State>);
impl Drop for PoisonOnPanic {
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
    deadline: Instant,
    submitted: Option<Instant>,
    sampled: bool,
    flight: Arc<ReadFlight>,
    _credit: Arc<Credit>,
    _lane_credit: LaneCredit,
}
enum ReadCommand {
    Request(Request),
    Wake,
}

struct OwnedLane {
    lane: Lane,
    reservation: Option<InlineLaneReservation>,
    sender: mpsc::SyncSender<ReadCommand>,
    state: Arc<State>,
}
impl Drop for OwnedLane {
    fn drop(&mut self) {
        drop(self.reservation.take());
        // At most one control wake occupies the channel independently of
        // request credits. Accepted work remains in the shared driver.
        if !self.state.wake_pending.swap(true, Ordering::AcqRel)
            && self.sender.try_send(ReadCommand::Wake).is_err()
        {
            self.state.wake_pending.store(false, Ordering::Release);
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
        if self.sampled && record_profile {
            let diagnostics = &self._credit.state.diagnostics;
            let owner = &self._credit.state.owner_diagnostics[self.flight.node].1;
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
        if result.is_err() {
            self._credit.state.poison();
        }
        // Transport has relinquished this request's application buffers. The
        // endpoint separately owns any fixed registered slots still in DMA.
        let state = self._credit.state.clone();
        let flight_slot = self.flight.slot;
        let credit = self._credit.clone();
        let completion = self.flight.completion.clone();
        *completion.result.lock() = Some(result);
        drop(completion);
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
    completion: Arc<FlightCompletion>,
    _pin: Option<IndexReadPin>,
    // The flight, rather than a notifier's temporary completion Arc, owns the
    // retained-result credit for as long as any caller can consume its result.
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
struct ReactorLane {
    client: Arc<Mutex<Client>>,
    queued: VecDeque<Request>,
    active: HashMap<u64, InflightRead>,
    profiling: bool,
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn progress_nvmeof(state: &Arc<State>) -> bool {
    let mut guard = state.nvmeof.lock();
    let mut ready = Vec::new();
    for (index, dispatch) in guard.iter_mut().enumerate() {
        if dispatch.active.is_empty() {
            continue;
        }
        let completions = match dispatch.reader.progress() {
            Ok(completions) => completions,
            Err(_) => {
                // Retain requests, credits and pins while the queue owns DMA.
                state.poison();
                continue;
            }
        };
        for completion in completions {
            if let Some(request) = dispatch.active.remove(&completion.id) {
                ready.push((index, dispatch.shape.clone(), request, completion.result));
            } else {
                state.poison();
            }
        }
    }
    drop(guard);
    let progressed = !ready.is_empty();
    for (index, shape, request, result) in ready {
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
        if verifying && let Some(dispatch) = state.nvmeof.lock().get(index) {
            dispatch
                .reader
                .record_verification(verification_started.elapsed());
        }
        request.complete_nvme(result);
    }
    progressed
}

// Returns whether the head belongs to NVMe-oF and whether it made progress.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn submit_nvmeof(state: &Arc<State>, lane: &mut ReactorLane) -> (bool, bool) {
    let mut guard = state.nvmeof.lock();
    let Some(request) = lane.queued.front() else {
        return (false, false);
    };
    let mut selected = None;
    for reference in &request.references {
        let mut member = None;
        for (index, dispatch) in guard.iter().enumerate() {
            match dispatch.reader.eligibility(reference) {
                Ok(super::super::nvmeof::Eligibility::Eligible) => member = Some(index),
                Ok(super::super::nvmeof::Eligibility::Ineligible) => {}
                Err(error) => {
                    let request = lane.queued.pop_front().unwrap();
                    drop(guard);
                    request.complete(Err(LocalDbError::component(error)));
                    return (true, true);
                }
            }
        }
        if let Some(previous) = selected {
            if previous != member {
                let request = lane.queued.pop_front().unwrap();
                drop(guard);
                request.complete(Err(LocalDbError::new(
                    "mixed NVMe-oF request was not partitioned",
                )));
                return (true, true);
            }
        } else {
            selected = Some(member);
        }
    }
    let Some(Some(index)) = selected else {
        return (false, false);
    };
    let dispatch = &mut guard[index];
    let id = dispatch.next;
    dispatch.next = dispatch.next.wrapping_add(1);
    match dispatch
        .reader
        .submit(id, request.references.clone(), request.deadline)
    {
        Ok(super::super::nvmeof::SubmitOutcome::Submitted) => {
            let request = lane.queued.pop_front().unwrap();
            dispatch.active.insert(id, request);
            (true, true)
        }
        Ok(super::super::nvmeof::SubmitOutcome::Full) => (true, false),
        Err(error) => {
            let request = lane.queued.pop_front().unwrap();
            drop(guard);
            request.complete(Err(LocalDbError::component(error)));
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
    let nvme_head = loop {
        let (handled, made_progress) = submit_nvmeof(state, lane);
        progressed |= made_progress;
        if !handled || !made_progress {
            break handled;
        }
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
        && let Some(dispatch) = state
            .nvmeof
            .lock()
            .iter()
            .find(|dispatch| dispatch.reader.manifest().partition == partition)
    {
        dispatch.reader.record_ineligible_fallbacks(count);
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

#[allow(clippy::too_many_arguments)] // The actor takes explicit ownership, admission and polling inputs once.
fn drive_reads(
    state: Arc<State>,
    receiver: mpsc::Receiver<ReadCommand>,
    lanes: Vec<Lane>,
    epoch: u64,
    max_requests: usize,
    fabric: bool,
    busy: bool,
) {
    let _poison = PoisonOnPanic(state.clone());
    let mut disconnected = false;
    let mut first_lane = 0;
    let mut empty_polls = 0u8;
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let mut nvme_shutdown_started = false;
    loop {
        let mut progressed = false;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
            progressed |= progress_nvmeof(&state);
        }
        #[cfg(test)]
        if let Some((entered, resume)) = state.reactor_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        // Credits bound the sum of this queue, lane scheduling state, transport
        // requests and undelivered completions, not merely the channel length.
        for _ in 0..max_requests {
            match receiver.try_recv() {
                Ok(ReadCommand::Request(request)) => {
                    lanes[request.lane].driver.lock().queued.push_back(request);
                    progressed = true;
                }
                Ok(ReadCommand::Wake) => {
                    state.wake_pending.store(false, Ordering::Release);
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
        // Sender disconnection proves no producer can enqueue another accepted
        // request. Preserve existing drain semantics by submitting every queued
        // request before closing reader admission, then keep polling terminal CQEs.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if disconnected
            && !nvme_shutdown_started
            && lanes.iter().all(|lane| {
                !lane.scheduling.lock().inline_reserved
                    && lane
                        .driver
                        .try_lock()
                        .is_some_and(|driver| driver.queued.is_empty())
            })
        {
            let mut readers = state.nvmeof.lock();
            for dispatch in readers.iter_mut() {
                dispatch.reader.begin_shutdown();
            }
            nvme_shutdown_started = true;
        }
        if disconnected
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
                        lanes[request.lane].driver.lock().queued.push_back(request)
                    }
                    Ok(ReadCommand::Wake) => {
                        state.wake_pending.store(false, Ordering::Release);
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
    sender: Mutex<Option<mpsc::SyncSender<ReadCommand>>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_maintenance: Mutex<()>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_epoch_pins: Mutex<Vec<NvmeEpochPin>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    nvmeof_pin_uncertain: AtomicBool,
    thread: Mutex<Option<JoinHandle<()>>>,
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
pub(super) struct ReadRequest {
    parts: SmallVec<[(Arc<ReadFlight>, usize); 1]>,
    credit: Arc<Credit>,
    deadline: Instant,
    sampled: bool,
    profile: Option<Arc<ReadFlight>>,
}
impl ReadRequest {
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

    fn member_charge(&self, reference: &ValueRecordReference) -> Result<usize, LocalDbError> {
        let charge = member_charge(reference)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        for dispatch in self.state.nvmeof.lock().iter() {
            if dispatch.reader.eligibility(reference).map_err(|error| {
                self.state.poison();
                LocalDbError::component(error)
            })? == super::super::nvmeof::Eligibility::Eligible
            {
                let aligned = dispatch
                    .reader
                    .aligned_charge(reference)
                    .map_err(LocalDbError::component)?;
                if aligned > dispatch.reader.max_request_bytes()
                    || dispatch.reader.max_request_members() == 0
                {
                    return Err(LocalDbError::new("record exceeds NVMe-oF reader capacity"));
                }
                return charge
                    .checked_add(aligned)
                    .ok_or_else(|| LocalDbError::new("read budget overflow"));
            }
        }
        Ok(charge)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn nvmeof_eligible(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<Option<usize>, LocalDbError> {
        let mut selected = None;
        for (index, dispatch) in self.state.nvmeof.lock().iter().enumerate() {
            if dispatch.reader.eligibility(reference).map_err(|error| {
                self.state.poison();
                LocalDbError::component(error)
            })? == super::super::nvmeof::Eligibility::Eligible
            {
                selected = Some(index);
            }
        }
        Ok(selected)
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn nvmeof_limits(
        &self,
        index: usize,
        reference: &ValueRecordReference,
    ) -> Result<(usize, usize, usize), LocalDbError> {
        let guard = self.state.nvmeof.lock();
        let reader = &guard
            .get(index)
            .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?
            .reader;
        Ok((
            reader
                .aligned_charge(reference)
                .map_err(LocalDbError::component)?,
            reader.max_request_bytes(),
            reader.max_request_members(),
        ))
    }

    const FLIGHT_SLOT_BYTES: usize = std::mem::size_of::<Weak<ReadFlight>>()
        + std::mem::size_of::<ReadFlight>()
        + std::mem::size_of::<FlightCompletion>()
        + std::mem::size_of::<FlightSignal>()
        + 2 * std::mem::size_of::<usize>();
    pub(super) const MIN_FLIGHT_BYTES: usize = Self::FLIGHT_SLOT_BYTES + 512;

    pub(super) fn from_clients(
        config: &RemoteDatabaseConfig,
        epoch: u64,
        clients: Vec<Client>,
        recovery_stats: &mut RecoveryStats,
    ) -> Result<Self, LocalDbError> {
        // Resolve immutable node policy before handing clients to the reactor;
        // startup must not contend with an otherwise idle scalar caller.
        let busy = clients
            .iter()
            .any(|client| client.polling_policy() == super::super::tcp::PollingPolicy::Busy);
        let flight_slots = config
            .max_pending_reads
            .min(config.max_pending_read_bytes / Self::MIN_FLIGHT_BYTES);
        let flight_metadata_bytes = flight_slots * Self::FLIGHT_SLOT_BYTES;
        let state = Arc::new(State {
            failed: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            retained: Mutex::new((0, 0)),
            admission_paused: AtomicBool::new(false),
            admission_generation: AtomicU64::new(0),
            admission_changed: parking_lot::Condvar::new(),
            wake_pending: AtomicBool::new(false),
            flight_ready: (0..flight_slots)
                .map(|_| FlightSignal::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            read_rpcs: AtomicU64::new(0),
            diagnostics: ReadDiagnostics::default(),
            coordinator_diagnostics: CoordinatorResolutionDiagnostics::default(),
            owner_diagnostics: config
                .endpoints
                .iter()
                .map(|endpoint| (endpoint.partition, ReadDiagnostics::default()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            flights: Mutex::new((0..flight_slots).map(|_| Weak::new()).collect()),
            coalesced_members: AtomicU64::new(0),
            flight_metadata_bytes,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_metrics: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_enabled: AtomicBool::new(false),
            #[cfg(test)]
            peak_connection_reads: AtomicUsize::new(0),
            #[cfg(test)]
            inline_pause: Mutex::new(None),
            #[cfg(test)]
            inline_published_pause: Mutex::new(None),
            #[cfg(test)]
            completion_pause: Mutex::new(None),
            #[cfg(test)]
            reactor_pause: Mutex::new(None),
        });
        let mut service = Self {
            state,
            lanes: RwLock::new(Vec::new()),
            sender: Mutex::new(None),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_maintenance: Mutex::new(()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_epoch_pins: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_pin_uncertain: AtomicBool::new(false),
            thread: Mutex::new(None),
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
        let mut reactor_lanes = Vec::new();
        let mut clients = clients.into_iter();
        for _ in &config.endpoints {
            let mut lanes = Vec::with_capacity(config.read_lanes);
            for _ in 0..config.read_lanes {
                let client = clients.next().unwrap();
                recovery_stats.add_connection(client.connect_profile());
                let client = Arc::new(Mutex::new(client));
                let index = reactor_lanes.len();
                let scheduling = Arc::new(Mutex::new(LaneScheduling {
                    inline_reserved: false,
                }));
                let driver = Arc::new(Mutex::new(ReactorLane {
                    client: client.clone(),
                    queued: VecDeque::new(),
                    active: HashMap::new(),
                    profiling: false,
                }));
                let lane = Lane {
                    index,
                    client,
                    outstanding: Arc::new(AtomicUsize::new(0)),
                    scheduling,
                    driver,
                };
                reactor_lanes.push(lane.clone());
                lanes.push(lane);
            }
            service.lanes.get_mut().push(lanes);
        }
        // Up to flight_slots charged requests plus one coalesced control wake.
        let (sender, receiver) = mpsc::sync_channel(flight_slots + 1);
        let state = service.state.clone();
        let fabric = config.tls.transport.is_fabric();
        *service.thread.get_mut() = Some(
            thread::Builder::new()
                .name("wal-read-reactor".to_owned())
                .spawn(move || {
                    drive_reads(
                        state,
                        receiver,
                        reactor_lanes,
                        epoch,
                        flight_slots,
                        fabric,
                        busy,
                    )
                })
                .map_err(LocalDbError::component)?,
        );
        *service.sender.get_mut() = Some(sender);
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
        let mut guard = self.state.nvmeof.lock();
        if guard.iter().any(|dispatch| !dispatch.active.is_empty()) {
            return Err(LocalDbError::new("NVMe-oF requests have not drained"));
        }
        self.state.nvmeof_enabled.store(false, Ordering::Release);
        let readers = std::mem::take(&mut *guard);
        for metrics in self.state.nvmeof_metrics.lock().iter() {
            metrics.replace_sources(&[]);
        }
        drop(guard);
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
            .nvmeof
            .lock()
            .iter()
            .map(|dispatch| dispatch.reader.snapshot())
            .collect()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn register_nvmeof_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), LocalDbError> {
        let guard = self.state.nvmeof.lock();
        let metrics = super::super::nvmeof::NvmeofMetrics::register(registry)
            .map_err(LocalDbError::component)?;
        let readers: Vec<_> = guard.iter().map(|dispatch| &dispatch.reader).collect();
        metrics.replace_sources(&readers);
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
            .nvmeof
            .lock()
            .iter()
            .try_fold(0usize, |total, reader| {
                total.checked_add(reader.metadata_budget)
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
        {
            let readers = self.state.nvmeof.lock();
            for entry in &manifest.catalog {
                for existing in readers.iter() {
                    match existing
                        .reader
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
        let binding = super::super::nvmeof::lease::ExportBinding::from(manifest);
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
        let mut readers = self.state.nvmeof.lock();
        readers.push(NvmeDispatch {
            reader,
            lease_client: client,
            lease_epoch: self.epoch,
            lease_token: token,
            shape: Arc::new(shape.clone()),
            next: 0,
            active: HashMap::new(),
            metadata_budget,
        });
        let sources: Vec<_> = readers.iter().map(|dispatch| &dispatch.reader).collect();
        for metrics in self.state.nvmeof_metrics.lock().iter() {
            metrics.replace_sources(&sources);
        }
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
        self.notify_admission();
        self.sender.lock().take();
        // Serialize all shutdown callers through join and lease release. A
        // second caller must not release an owner token while the first one is
        // still waiting for the reactor's terminal completions.
        let mut thread_guard = self.thread.lock();
        if let Some(thread) = thread_guard.take()
            && thread.join().is_err()
        {
            self.poison();
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
        if self.failed() || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if self.nvmeof_enabled() {
            return Ok(None);
        }
        let bytes = member_charge(reference)?
            .checked_add(extra_bytes)
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| LocalDbError::new("read budget overflow"))?;
        if bytes > self.max_bytes {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let mut flights = self.state.flights.lock();
        if flights
            .iter()
            .filter_map(Weak::upgrade)
            .any(|flight| flight.node == node && flight.references.contains(reference))
        {
            return Ok(None);
        }
        let flight_slot = flights
            .iter()
            .enumerate()
            .position(|(slot, flight)| {
                flight.strong_count() == 0 && self.state.flight_ready[slot].is_ready()
            })
            .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?;
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
        self.state.flight_ready[flight_slot].begin();
        let flight = Arc::new(ReadFlight {
            node,
            references: vec![*reference],
            slot: flight_slot,
            grouped_inline: AtomicBool::new(false),
            completion: Arc::new(FlightCompletion::new()),
            _pin: pin,
            _credit: credit,
        });
        flights[flight_slot] = Arc::downgrade(&flight);
        drop(flights);
        #[cfg(test)]
        if let Some((entered, resume)) = self.state.inline_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        let _poison = PoisonOnPanic(self.state.clone());
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
            references,
            extra_bytes,
            profiled,
            ReadContext {
                deadline: Instant::now() + self.timeout,
                pin,
            },
        )
    }
    fn submit_until(
        &self,
        node: usize,
        references: Vec<ValueRecordReference>,
        extra_bytes: usize,
        profiled: bool,
        context: ReadContext,
    ) -> Result<ReadRequest, LocalDbError> {
        self.submit_until_owned(node, references, extra_bytes, profiled, context, None)
    }
    fn submit_until_owned(
        &self,
        node: usize,
        references: Vec<ValueRecordReference>,
        extra_bytes: usize,
        profiled: bool,
        context: ReadContext,
        mut owned_lanes: Option<&mut Vec<OwnedLane>>,
    ) -> Result<ReadRequest, LocalDbError> {
        let ReadContext { deadline, pin } = context;
        if self.failed() || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if references.is_empty() || references.len() > 256 {
            return Err(LocalDbError::new("read request count exceeds bound"));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let generation = self.state.admission_generation.load(Ordering::Acquire);
        let bytes = references
            .iter()
            .try_fold(extra_bytes + 512, |bytes, reference| {
                bytes
                    .checked_add(self.member_charge(reference)?)
                    .ok_or_else(|| LocalDbError::new("read budget overflow"))
            })?;
        if bytes > self.max_bytes {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let mut flights = self.state.flights.lock();
        let mut missing = Vec::new();
        let mut parts: SmallVec<[(Option<Arc<ReadFlight>>, usize); 1]> =
            SmallVec::with_capacity(references.len());
        for reference in &references {
            let existing = flights.iter().filter_map(Weak::upgrade).find_map(|flight| {
                if flight.node != node {
                    return None;
                }
                flight
                    .references
                    .iter()
                    .position(|member| member == reference)
                    .map(|slot| (flight, slot))
            });
            if let Some((flight, slot)) = existing {
                parts.push((Some(flight), slot));
            } else {
                let slot = match missing.iter().position(|member| member == reference) {
                    Some(slot) => slot,
                    None => {
                        missing.push(*reference);
                        missing.len() - 1
                    }
                };
                parts.push((None, slot));
            }
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(first) = missing.first() {
            let eligible = self.nvmeof_eligible(first)?;
            for reference in &missing[1..] {
                if self.nvmeof_eligible(reference)? != eligible {
                    return Err(LocalDbError::new(BACKPRESSURE));
                }
            }
        }
        let coalesced_members = references.len() - missing.len();
        let new_request = usize::from(!missing.is_empty());
        let flight_slot = if new_request != 0 {
            Some(
                flights
                    .iter()
                    .enumerate()
                    .position(|(slot, flight)| {
                        flight.strong_count() == 0 && self.state.flight_ready[slot].is_ready()
                    })
                    .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?,
            )
        } else {
            None
        };
        let mut reserved = false;
        let (credit, lane) = {
            let mut retained = self.state.retained.lock();
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if self.state.admission_generation.load(Ordering::Acquire) != generation {
                return Err(LocalDbError::new(BACKPRESSURE));
            }
            if self.failed() {
                return Err(LocalDbError::new("read authority is poisoned"));
            }
            if self.state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is closed"));
            }
            if self.state.admission_paused.load(Ordering::Acquire) {
                return Err(LocalDbError::new(BACKPRESSURE));
            }
            if (new_request != 0 && retained.0 == self.max_requests)
                || bytes > self.max_bytes - retained.1
            {
                return Err(LocalDbError::new(BACKPRESSURE));
            }
            let lane_guard = self.lanes.read();
            let lanes = lane_guard
                .get(node)
                .ok_or_else(|| LocalDbError::new("read reactor stopped"))?;
            // Prefer an idle lane so work can progress independently instead
            // of queueing behind an unrelated slow or stalled request. If all
            // lanes are occupied, fall back to a non-inline-reserved lane.
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
                                if scheduling.inline_reserved
                                    || (idle_only && lane.outstanding.load(Ordering::Acquire) != 0)
                                {
                                    return None;
                                }
                                if idle_only && owned_lanes.is_some() && !self.nvmeof_enabled() {
                                    scheduling.inline_reserved = true;
                                    reserved = true;
                                }
                                lane.outstanding.fetch_add(1, Ordering::AcqRel);
                                Some(lane.clone())
                            })
                        })
                        .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?,
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
        let sampled = new_request != 0 && self.state.diagnostics.select();
        let flight = if let Some(lane) = lane {
            let slot = flight_slot.expect("a new request reserves a flight slot");
            self.state.flight_ready[slot].begin();
            let flight = Arc::new(ReadFlight {
                node,
                references: missing.clone(),
                slot,
                grouped_inline: AtomicBool::new(false),
                completion: Arc::new(FlightCompletion::new()),
                _pin: pin,
                _credit: credit.clone(),
            });
            flights[slot] = Arc::downgrade(&flight);
            let request = Request {
                lane: lane.index,
                references: missing,
                deadline,
                submitted: (profiled || sampled).then(Instant::now),
                sampled,
                flight: flight.clone(),
                _credit: credit.clone(),
                _lane_credit: LaneCredit {
                    outstanding: lane.outstanding.clone(),
                },
            };
            let sender_guard = self.sender.lock();
            let Some(sender) = sender_guard.as_ref() else {
                if reserved {
                    drop(InlineLaneReservation(lane.scheduling.clone()));
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
                        profile: Some(flight),
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
                        (LocalDbError::new(BACKPRESSURE), false)
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
            profile: flight,
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
            .read()
            .pin_cut(IndexReadCut::At(MutationVersion {
                batch: reference.batch_version,
                operation_index: reference.descriptor.operation_index,
            }))
            .map_err(LocalDbError::component)?;
        let ready = self.read_cache.lock().get_scan(&reference);
        let read = if ready.is_none() {
            let node = *self
                .partition_indexes
                .get(&reference.batch_address.partition)
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
            Some(self.reads.submit_until(
                node,
                vec![reference],
                0,
                false,
                ReadContext {
                    deadline: Instant::now() + self.config.timeout,
                    pin: Some(pin),
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
        error.to_string() == BACKPRESSURE
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
            let clock = ResolutionSample::start(&sample);
            let index = self.index.read();
            if let Some(sample) = &sample {
                sample.stage(ResolutionStage::IndexLockWait, clock);
            }
            let clock = ResolutionSample::start(&sample);
            let reference = index
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
                let mut cache = self.read_cache.lock();
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::CacheLockWait, clock);
                }
                let clock = ResolutionSample::start(&sample);
                let cached = cache.get_point(&reference, ks, key);
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::CacheLookup, clock);
                }
                cached
            } else {
                None
            };
            let pin = if reference.is_some() && cached.is_none() {
                let clock = ResolutionSample::start(&sample);
                let pin = index.pin_cut(cut).map_err(LocalDbError::component);
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
        let prepared = ReadCache::prepare_verified(
            &self.read_cache,
            self.config.read_cache_bytes,
            reference,
            &mutation,
        );
        self.read_cache.lock().insert_prepared(prepared);
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
