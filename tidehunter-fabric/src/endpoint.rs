//! Rust ownership and completion state over OFI's generated libfabric bindings.

mod efa_direct;
mod resources;
use efa_direct::{ControlWindow, SendWindow, StreamReceiver};
use ofi_libfabric_sys::bindgen as ffi;
use resources::{ADDRESS, Resources, error, invalid};
use std::{
    io::{self, IoSlice, Write},
    mem,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

const CHUNK: usize = 65_536;
const SLOTS: usize = 4;
const PROGRESS_LIMIT: usize = 10_000_000;
pub(crate) const ENDPOINTS: usize = 1024;
static LIVE: AtomicUsize = AtomicUsize::new(0);

/// Bounded lifetime CQ counters. Poll-gap timing is opt-in and measures the
/// longest interval between provider polls while a local send is outstanding,
/// not network latency, remote processing time, or time between completions.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProgressStats {
    pub cq_polls: u64,
    pub cq_empty_polls: u64,
    pub cq_completions: u64,
    pub cq_max_poll_gap_ns: u64,
    /// Opt-in synchronous work; post time is not completion residence or RTT.
    pub send_copy_ns: u64,
    pub send_copy_calls: u64,
    pub send_copy_bytes: u64,
    pub receive_copy_ns: u64,
    pub receive_copy_calls: u64,
    pub receive_copy_bytes: u64,
    pub send_post_ns: u64,
    pub send_post_calls: u64,
    pub send_post_bytes: u64,
    pub send_post_would_block: u64,
    pub send_post_errors: u64,
    pub receive_post_ns: u64,
    pub receive_post_calls: u64,
    /// Successfully posted receive capacity, not delivered bytes.
    pub receive_post_bytes: u64,
    pub receive_post_would_block: u64,
    pub receive_post_errors: u64,
    /// `fi_cq_read` only, separate from validation/ownership dispatch below.
    pub cq_call_ns: u64,
    pub cq_timed_calls: u64,
    pub cq_error_call_ns: u64,
    pub cq_error_calls: u64,
    pub cq_dispatch_ns: u64,
    pub cq_dispatch_calls: u64,
    pub cq_dispatch_errors: u64,
    /// All inter-poll gaps, including receive-only/idle intervals, not arrivals.
    pub cq_poll_gap_ns: u64,
    pub cq_poll_gap_samples: u64,
    pub cq_poll_gap_max_ns: u64,
}

/// Successful endpoint setup stages, not an estimate of hardware latency.
/// Queue sizes are negotiated packet/operation capacities, not application slots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpenProfile {
    pub allocation_ns: u64,
    pub discovery_ns: u64,
    pub fabric_ns: u64,
    pub domain_ns: u64,
    pub cq_ns: u64,
    pub av_ns: u64,
    pub endpoint_ns: u64,
    pub bind_enable_ns: u64,
    pub register_ns: u64,
    pub address_ns: u64,
    pub peer_setup_ns: u64,
    /// First provider CQ read, including any deferred RX pool initialization.
    /// Zero until progress first runs; not reset with diagnostic CQ counters.
    pub first_progress_ns: u64,
    pub requested_tx: usize,
    pub requested_rx: usize,
    pub selected_tx: usize,
    pub selected_rx: usize,
    pub mtu: usize,
    pub provider_version: u32,
}

/// One exclusively operated `FI_EP_RDM` endpoint with four 64 KiB slots each
/// for sends and receives (512 KiB total).
/// Buffers and provider contexts have stable allocations across thread moves.
pub struct Endpoint {
    resources: Option<Box<Resources>>,
    direct: Option<Box<DirectState>>,
    failed: bool,
}

struct DirectState {
    mtu: usize,
    send: SendWindow,
    receive: StreamReceiver,
    control: ControlWindow,
    pending_control: Option<u64>,
}

impl DirectState {
    fn new(mtu: usize) -> io::Result<Self> {
        Ok(Self {
            mtu,
            send: SendWindow::default(),
            receive: StreamReceiver::new(mtu)?,
            control: ControlWindow::default(),
            pending_control: None,
        })
    }
}

// SAFETY: FI_THREAD_DOMAIN permits serialized operations from different threads.
// Every operation requires &mut self. The exclusively owned domain, endpoint,
// buffers and contexts have no aliases exposed to Rust callers and never move.
unsafe impl Send for Endpoint {}

impl Endpoint {
    /// Opens exactly the selected libfabric provider, without software fallback.
    /// # Errors
    /// Reports invalid names, absent providers/devices or exhausted admission.
    pub fn open(provider: &str, domain: Option<&str>) -> io::Result<Self> {
        Self::open_inner(provider, domain, false)
    }

    /// Opens only the `efa-direct` fabric of the EFA provider. No regular-EFA
    /// or software fallback is attempted.
    /// # Errors
    /// Returns a hardware-unavailable error when the direct fabric is absent.
    pub fn open_efa_direct(domain: Option<&str>) -> io::Result<Self> {
        Self::open_inner("efa", domain, true)
    }

    /// Test-only packet-protocol exercise over libfabric's software provider.
    /// The ordinary provider remains honestly reported and is never a runtime
    /// fallback for `open_efa_direct`.
    #[cfg(test)]
    fn open_software_packet_test(domain: Option<&str>) -> io::Result<Self> {
        let mut endpoint = Self::open_inner("tcp;ofi_rxm", domain, false)?;
        endpoint.inner().enable_test_packet_adapter();
        endpoint.direct = Some(Box::new(DirectState::new(4096)?));
        Ok(endpoint)
    }
    fn open_inner(provider: &str, domain: Option<&str>, direct: bool) -> io::Result<Self> {
        LIVE.fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
            (live < ENDPOINTS).then_some(live + 1)
        })
        .map_err(|_| io::Error::other("fabric endpoint admission exhausted"))?;
        let allocation = Instant::now();
        let mut endpoint = Self {
            resources: Some(Box::default()),
            direct: None,
            failed: false,
        };
        endpoint.inner().open_profile.allocation_ns = resources::elapsed_ns(allocation);
        if direct {
            endpoint.inner().open_mode(provider, domain, true)?;
        } else {
            endpoint.inner().open(provider, domain)?;
        }
        if direct {
            endpoint.direct = Some(Box::new(DirectState::new(endpoint.open_profile().mtu)?));
        }
        Ok(endpoint)
    }

    /// Returns setup timings and negotiated capacities without provider calls.
    /// # Panics
    /// Panics if the internal resource-ownership invariant is broken.
    #[must_use]
    pub fn open_profile(&self) -> OpenProfile {
        self.resources
            .as_deref()
            .expect("resources exist until Endpoint::drop")
            .open_profile
    }

    /// Returns the bounded address for the authenticated TLS bootstrap.
    /// # Errors
    /// Reports invalid provider address data.
    pub fn address(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.inner().address().to_vec())
    }

    /// Installs one authenticated peer and posts the initial receive buffer.
    /// # Errors
    /// Rejects invalid addresses, rebinding or provider failures.
    pub fn set_peer(&mut self, address: &[u8]) -> io::Result<()> {
        if self.failed || self.inner().has_peer() || address.is_empty() || address.len() > ADDRESS {
            return Err(invalid("invalid fabric peer setup"));
        }
        let result = self.inner().set_peer(address);
        self.failed = result.is_err();
        result
    }

    /// Copies and attempts to post at most 64 KiB without waiting. Accepted bytes
    /// belong to this endpoint until their local CQ completion or successful close,
    /// so the caller may immediately change or drop its input.
    /// # Errors
    /// Returns `WouldBlock` without poisoning when no slot is free or the provider
    /// returns EAGAIN. Other progress/post errors poison the endpoint.
    pub fn try_send(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.try_send_vectored(&[IoSlice::new(bytes)])
    }

    /// Coalesces at most 64 KiB into one stable owned slot. Reads at most one CQ
    /// batch and attempts exactly one send when a slot is available; never waits,
    /// retries, sleeps or retains the caller's slices.
    /// # Errors
    /// `WouldBlock` accepts no bytes and leaves the endpoint usable. A copied but
    /// unposted slot is immediately reusable. Other progress/post errors poison.
    pub fn try_send_vectored(&mut self, bytes: &[IoSlice<'_>]) -> io::Result<usize> {
        self.try_send_vectored_inner(bytes, true)
    }

    /// One send attempt using only already-observed completions, with no CQ read.
    /// The owner must call `try_progress` once per bounded reactor sweep, including
    /// after EAGAIN; this operation alone cannot release provider-owned slots.
    /// # Errors
    /// Same ownership, `WouldBlock` and poisoning rules as `try_send_vectored`.
    pub fn try_send_vectored_no_poll(&mut self, bytes: &[IoSlice<'_>]) -> io::Result<usize> {
        self.try_send_vectored_inner(bytes, false)
    }

    fn try_send_vectored_inner(&mut self, bytes: &[IoSlice<'_>], poll: bool) -> io::Result<usize> {
        self.ready()?;
        if bytes.iter().all(|slice| slice.is_empty()) {
            return Ok(0);
        }
        let result = if self.direct.is_some() {
            self.direct_send_vectored(bytes, poll)
        } else {
            (|| {
                let state = self.inner();
                if poll {
                    state.poll()?;
                }
                let slot = state.free_send_slot().ok_or(io::ErrorKind::WouldBlock)?;
                let len = state.copy_send(slot, bytes);
                if !state.post_send(slot, len)? {
                    return Err(io::ErrorKind::WouldBlock.into());
                }
                Ok(len)
            })()
        };
        self.finish_try(result)
    }

    fn direct_send_vectored(&mut self, slices: &[IoSlice<'_>], poll: bool) -> io::Result<usize> {
        if poll {
            self.direct_progress()?;
        }
        let direct = self
            .direct
            .as_mut()
            .ok_or_else(|| invalid("missing EFA-direct state"))?;
        if slices.iter().all(|slice| slice.is_empty()) {
            return Ok(0);
        }
        let ordinal = direct.send.next_ordinal()?;
        let credit = direct.receive.consumed();
        let mtu = direct.mtu;
        let state = self
            .resources
            .as_deref_mut()
            .ok_or_else(|| invalid("missing fabric resources"))?;
        let slot = state.free_send_slot().ok_or(io::ErrorKind::WouldBlock)?;
        let packet_len = state.prepare_direct_send(slot, |output| {
            efa_direct::encode_data_vectored(output, ordinal, credit, slices, mtu)
        })?;
        if !state.post_send(slot, packet_len)? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        direct.send.posted(ordinal)?;
        Ok(packet_len - efa_direct::HEADER_LEN)
    }

    fn direct_receive(&mut self, output: &mut [u8], poll: bool) -> io::Result<usize> {
        if poll {
            self.direct_progress()?;
        }
        let result = self
            .direct
            .as_mut()
            .ok_or_else(|| invalid("missing EFA-direct state"))?
            .receive
            .read(output);
        match result {
            Ok((len, Some(_credit))) => {
                // Application consumption earns peer credit. EAGAIN while
                // posting the control packet is retried by later progress.
                let _ = self.queue_direct_control()?;
                Ok(len)
            }
            Ok((len, None)) => Ok(len),
            Err(error) => Err(error),
        }
    }

    fn queue_direct_control(&mut self) -> io::Result<bool> {
        let direct = self
            .direct
            .as_mut()
            .ok_or_else(|| invalid("missing EFA-direct state"))?;
        let state = self
            .resources
            .as_deref_mut()
            .ok_or_else(|| invalid("missing fabric resources"))?;
        if let Some(ordinal) = direct.pending_control {
            if state.free_control_slot().is_none() {
                return Ok(false);
            }
            direct.control.complete(ordinal)?;
            direct.pending_control = None;
        }
        let consumed = direct.receive.consumed();
        let Some(ordinal) = direct.control.prepare(consumed)? else {
            return Ok(false);
        };
        let Some(slot) = state.free_control_slot() else {
            return Ok(false);
        };
        let len = state.prepare_direct_send(slot, |output| {
            efa_direct::encode_credit(output, ordinal, consumed)
        })?;
        if !state.post_send(slot, len)? {
            return Ok(false);
        }
        direct.control.posted(ordinal, consumed)?;
        direct.pending_control = Some(ordinal);
        Ok(true)
    }

    fn direct_progress(&mut self) -> io::Result<bool> {
        let direct = self
            .direct
            .as_mut()
            .ok_or_else(|| invalid("missing EFA-direct state"))?;
        let state = self
            .resources
            .as_deref_mut()
            .ok_or_else(|| invalid("missing fabric resources"))?;
        let mut progressed = state.poll()?;
        loop {
            let packet = state.process_direct_packet(|bytes| {
                match efa_direct::decode(bytes, direct.mtu)? {
                    efa_direct::Packet::Data {
                        ordinal,
                        credit,
                        payload,
                    } => {
                        direct.send.credit(credit)?;
                        direct.receive.accept_data(ordinal, payload)
                    }
                    efa_direct::Packet::Credit { consumed, .. } => {
                        // Cumulative control is idempotent; an older reordered
                        // credit cannot release more than its stated frontier.
                        direct.send.credit(consumed)
                    }
                }
            })?;
            if !packet {
                break;
            }
            progressed = true;
        }
        if let Some(ordinal) = direct.pending_control
            && state.free_control_slot().is_some()
        {
            direct.control.complete(ordinal)?;
            direct.pending_control = None;
            progressed = true;
        }
        match state.try_post_receive() {
            Ok(posted) => progressed |= posted,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        if self.queue_direct_control()? {
            progressed = true;
        }
        Ok(progressed)
    }
    /// Copies bytes from the next completed receive in stream order. Reads at
    /// most one CQ batch and attempts at most one receive repost, without waiting.
    /// Consumed slots whose repost gets EAGAIN are retried by later receive or
    /// progress calls; bytes already copied are still returned successfully.
    /// # Errors
    /// Returns `WouldBlock` without poisoning if no bytes are ready, never zero
    /// for a nonempty output. Other progress/post errors poison the endpoint.
    pub fn try_receive(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.try_receive_inner(bytes, true)
    }

    /// Consumes one already-completed receive and attempts one repost, without
    /// reading the CQ. Pair with `try_progress` once per bounded reactor sweep.
    /// # Errors
    /// Same ownership, `WouldBlock` and poisoning rules as `try_receive`.
    pub fn try_receive_no_poll(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.try_receive_inner(bytes, false)
    }

    fn try_receive_inner(&mut self, bytes: &mut [u8], poll: bool) -> io::Result<usize> {
        self.ready()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        let result = if self.direct.is_some() {
            self.direct_receive(bytes, poll)
        } else {
            (|| {
                let state = self.inner();
                if poll {
                    state.poll()?;
                }
                let len = state.copy_receive(bytes);
                if let Err(error) = state.try_post_receive()
                    && error.kind() != io::ErrorKind::WouldBlock
                {
                    return Err(error);
                }
                len.ok_or_else(|| io::ErrorKind::WouldBlock.into())
            })()
        };
        self.finish_try(result)
    }

    /// Reads one bounded CQ batch and attempts at most one receive repost.
    /// Returns whether a completion or successful repost made progress; false
    /// means neither was available. Never waits for a send completion.
    /// # Errors
    /// A receive-post EAGAIN returns nonpoisoning `WouldBlock` (a CQ batch may
    /// already have progressed). Other progress/post errors poison the endpoint.
    pub fn try_progress(&mut self) -> io::Result<bool> {
        self.ready()?;
        let result = if self.direct.is_some() {
            self.direct_progress()
        } else {
            let state = self.inner();
            state
                .poll()
                .and_then(|completed| state.try_post_receive().map(|posted| completed || posted))
        };
        self.finish_try(result)
    }

    /// Whether a successful send post still owns any local TX slot. Application
    /// completion or cancellation does not release these provider-owned bytes.
    /// # Panics
    /// Panics if the internal ownership invariant is broken. A live endpoint
    /// always retains its resources until `Drop`.
    #[must_use]
    pub fn sends_pending(&self) -> bool {
        self.resources
            .as_deref()
            .expect("resources exist until Endpoint::drop")
            .sends_pending()
    }

    /// Last successfully posted local send ordinal. Zero precedes all sends.
    /// Snapshot only after all ciphertext for a frame has been accepted. Fences
    /// are endpoint-local and never prove remote receipt or durable storage.
    /// # Errors
    /// Fails on an unbound or poisoned endpoint; never polls or waits.
    pub fn accepted_send_fence(&mut self) -> io::Result<u64> {
        self.ready()?;
        Ok(self.inner().send_fences().0)
    }

    /// Contiguous locally completed send frontier. A fence at or below this
    /// ordinal has no provider-owned send buffers, even with out-of-order CQEs.
    /// Cancellation does not advance this frontier; close quiesces or quarantines
    /// live buffers instead. Fixed slot metadata bounds tracking storage.
    /// # Errors
    /// Fails on an unbound or poisoned endpoint; never polls or waits.
    pub fn completed_send_fence(&mut self) -> io::Result<u64> {
        self.ready()?;
        Ok(self.inner().send_fences().1)
    }

    /// Returns diagnostic counters without driving CQ progress.
    /// # Panics
    /// Panics if the internal resource-ownership invariant is broken.
    #[must_use]
    pub fn progress_stats(&self) -> ProgressStats {
        self.resources
            .as_deref()
            .expect("resources exist until Endpoint::drop")
            .progress_stats()
    }

    /// Enables/disables work and gap clocks; legacy CQ counts remain always on.
    /// Reset only clears diagnostics, never ownership or send fence ordinals.
    pub fn configure_progress_stats(&mut self, enabled: bool, reset: bool) {
        self.inner().configure_progress_stats(enabled, reset);
    }

    fn finish_try<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if let Err(error) = &result
            && error.kind() != io::ErrorKind::WouldBlock
        {
            self.failed = true;
        }
        result
    }

    /// Sends a bounded chunk and waits for local buffer completion. This is not
    /// a peer acknowledgement or storage durability guarantee.
    /// # Errors
    /// A progress failure poisons the endpoint. Expired deadlines submit no work.
    pub fn send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<usize> {
        let len = self.queue_send(bytes, deadline)?;
        if len != 0
            && let Err(error) = self.flush(deadline)
        {
            // queue_send already submitted bytes. Even if the deadline expires
            // before flush starts, this synchronous send has uncertain progress.
            self.failed = true;
            return Err(error);
        }
        Ok(len)
    }

    /// Copies a chunk into an owned slot and posts it without waiting for its
    /// completion. Up to four sends can be in flight. The caller must continue
    /// progress with sends, receives or flush before waiting outside the fabric.
    /// # Errors
    /// A progress failure poisons the endpoint. Expired deadlines submit no work.
    pub fn queue_send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<usize> {
        self.queue_send_vectored(&[IoSlice::new(bytes)], deadline)
    }

    /// Coalesces at most 64 KiB of stream slices directly into a free owned slot.
    /// TLS headers and records can share a message without a temporary allocation.
    /// # Errors
    /// A progress failure poisons the endpoint. Expired deadlines submit no work.
    pub fn queue_send_vectored(
        &mut self,
        bytes: &[IoSlice<'_>],
        deadline: Instant,
    ) -> io::Result<usize> {
        self.ready()?;
        if bytes.iter().all(|slice| slice.is_empty()) {
            return Ok(0);
        }
        remaining(deadline)?;
        let result = self.queue_send_inner(bytes, deadline);
        self.failed = result.is_err();
        result
    }

    fn queue_send_inner(&mut self, bytes: &[IoSlice<'_>], deadline: Instant) -> io::Result<usize> {
        if self.direct.is_some() {
            for _ in 0..PROGRESS_LIMIT {
                remaining(deadline)?;
                match self.direct_send_vectored(bytes, false) {
                    Ok(len) => return Ok(len),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        self.direct_progress()?;
                    }
                    Err(error) => return Err(error),
                }
            }
            return Err(error("efa-direct send progress limit", ffi::FI_ETIMEDOUT));
        }
        let state = self.inner();
        let mut copied = None;
        let mut progress = Progress::new(false);
        for _ in 0..PROGRESS_LIMIT {
            remaining(deadline)?;
            if copied.is_none()
                && let Some(slot) = state.free_send_slot()
            {
                copied = Some((slot, state.copy_send(slot, bytes)));
            }
            if let Some((slot, len)) = copied
                && state.post_send(slot, len)?
            {
                return Ok(len);
            }
            progress.drive(state, deadline)?;
        }
        Err(error("fabric send progress limit", ffi::FI_ETIMEDOUT))
    }

    /// Progresses one completion batch without waiting for pending sends.
    /// Posted bytes remain in owned, immutable slots until their validated
    /// completions. Callers must continue provider progress through subsequent
    /// I/O, or explicitly flush before waiting outside the endpoint.
    /// # Errors
    /// Reports expired deadlines and failed endpoints. A CQ error poisons it.
    pub fn progress(&mut self, deadline: Instant) -> io::Result<()> {
        self.ready()?;
        remaining(deadline)?;
        let result = if self.direct.is_some() {
            self.direct_progress().map(|_| ())
        } else {
            self.inner().poll().map(|_| ())
        };
        self.failed = result.is_err();
        result
    }

    /// Drives all queued sends to local completion. This says nothing about
    /// application receipt or durable storage acknowledgement.
    /// # Errors
    /// A progress failure poisons the endpoint. An expired deadline leaves queued
    /// buffers owned and unchanged; closing the endpoint safely cancels them.
    pub fn flush(&mut self, deadline: Instant) -> io::Result<()> {
        self.ready()?;
        remaining(deadline)?;
        let result = self.flush_inner(deadline);
        self.failed = result.is_err();
        result
    }

    fn flush_inner(&mut self, deadline: Instant) -> io::Result<()> {
        if self.direct.is_some() {
            for _ in 0..PROGRESS_LIMIT {
                remaining(deadline)?;
                self.direct_progress()?;
                let sends_pending = self.inner().sends_pending();
                let control_pending = self
                    .direct
                    .as_ref()
                    .is_some_and(|direct| direct.control.needs_send(direct.receive.consumed()));
                if !sends_pending && !control_pending {
                    return Ok(());
                }
            }
            return Err(error("efa-direct flush progress limit", ffi::FI_ETIMEDOUT));
        }
        let state = self.inner();
        let mut progress = Progress::new(false);
        for _ in 0..PROGRESS_LIMIT {
            if !state.sends_pending() {
                return Ok(());
            }
            progress.drive(state, deadline)?;
        }
        Err(error("fabric flush progress limit", ffi::FI_ETIMEDOUT))
    }

    /// Reads stream bytes while driving both completion directions. Use this
    /// while awaiting a response to an active request: it has no timed sleeps.
    /// # Errors
    /// A progress failure poisons the endpoint. Expired deadlines submit no work.
    pub fn receive(&mut self, bytes: &mut [u8], deadline: Instant) -> io::Result<usize> {
        self.receive_with_policy(bytes, deadline, false)
    }

    /// Reads while waiting for an unsolicited request. A short polling window
    /// follows each completion; idle connections then use bounded sleep backoff.
    /// Outstanding sends always use active progress, even on an idle receiver.
    /// # Errors
    /// A progress failure poisons the endpoint. Expired deadlines submit no work.
    pub fn receive_idle(&mut self, bytes: &mut [u8], deadline: Instant) -> io::Result<usize> {
        self.receive_with_policy(bytes, deadline, true)
    }

    /// Active receive with a bounded peer-liveness check every 1024 polls.
    /// The callback must not block. Failure poisons this endpoint and submits no
    /// retry elsewhere; already submitted requests may have an unknown outcome.
    /// # Errors
    /// Returns provider, deadline or caller cancellation errors.
    pub fn receive_checked(
        &mut self,
        bytes: &mut [u8],
        deadline: Instant,
        mut check_peer: impl FnMut() -> io::Result<()>,
    ) -> io::Result<usize> {
        self.ready()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        remaining(deadline)?;
        let result = self.receive_inner(bytes, deadline, false, &mut check_peer);
        self.failed = result.is_err();
        result
    }

    fn receive_with_policy(
        &mut self,
        bytes: &mut [u8],
        deadline: Instant,
        idle: bool,
    ) -> io::Result<usize> {
        self.ready()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        remaining(deadline)?;
        let result = self.receive_inner(bytes, deadline, idle, &mut || Ok(()));
        self.failed = result.is_err();
        result
    }

    fn receive_inner(
        &mut self,
        bytes: &mut [u8],
        deadline: Instant,
        idle: bool,
        check_peer: &mut impl FnMut() -> io::Result<()>,
    ) -> io::Result<usize> {
        if self.direct.is_some() {
            for poll in 0..PROGRESS_LIMIT {
                remaining(deadline)?;
                if poll % 1024 == 0 {
                    check_peer()?;
                }
                match self.direct_receive(bytes, false) {
                    Ok(len) => return Ok(len),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        self.direct_progress()?;
                        if idle && poll >= 64 {
                            std::thread::sleep(Duration::from_micros(50));
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            return Err(error(
                "efa-direct receive progress limit",
                ffi::FI_ETIMEDOUT,
            ));
        }
        let state = self.inner();
        let mut progress = Progress::new(idle);
        for poll in 0..PROGRESS_LIMIT {
            remaining(deadline)?;
            if poll % 1024 == 0 {
                check_peer()?;
            }
            state.fill_receives()?;
            if let Some(len) = state.copy_receive(bytes) {
                state.fill_receives()?;
                return Ok(len);
            }
            progress.drive(state, deadline)?;
        }
        Err(error("fabric receive progress limit", ffi::FI_ETIMEDOUT))
    }

    pub(crate) fn ready(&mut self) -> io::Result<()> {
        if self.failed || !self.inner().has_peer() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "fabric endpoint is not usable",
            ));
        }
        Ok(())
    }

    fn inner(&mut self) -> &mut Resources {
        self.resources
            .as_deref_mut()
            .expect("resources exist until Endpoint::drop")
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        if let Some(mut resources) = self.resources.take() {
            if let Err(error) = resources.close() {
                // A provider may still access registered memory after failed
                // close. Retain its buffers, contexts and admission permit.
                // At most ENDPOINTS such allocations can exist process-wide.
                mem::forget(resources);
                // A stderr error must not turn failed provider teardown into a
                // panic. Memory is already retained before best-effort logging.
                let _ = writeln!(
                    io::stderr(),
                    "fabric teardown failed ({error}); bounded resources quarantined"
                );
            } else {
                LIVE.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

fn remaining(deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        return Err(error("fabric deadline", ffi::FI_ETIMEDOUT));
    }
    Ok(())
}

/// Diagnostic synchronous progress policy. Production reactors use the bounded
/// `try_*` operations above instead of parking inside an endpoint. EFA 2.4 uses
/// manual progress without wait objects, so diagnostic active waits spin/yield,
/// and idle waits alone use a bounded sleep backoff.
struct Progress {
    idle: bool,
    last_completion: Instant,
    empty_polls: usize,
}

impl Progress {
    fn new(idle: bool) -> Self {
        Self {
            idle,
            last_completion: Instant::now(),
            empty_polls: 0,
        }
    }

    fn drive(&mut self, state: &mut Resources, deadline: Instant) -> io::Result<()> {
        remaining(deadline)?;
        if state.poll()? {
            self.empty_polls = 0;
            self.last_completion = Instant::now();
            return Ok(());
        }
        self.empty_polls += 1;
        let now = Instant::now();
        if self.should_sleep(state.sends_pending(), now) {
            // No remote request is outstanding on this server receive path.
            // Do not apply this timer to active sends or client response waits.
            let micros = 1_u64 << self.empty_polls.saturating_sub(64).min(6);
            std::thread::sleep(
                Duration::from_micros(micros).min(deadline.saturating_duration_since(now)),
            );
        } else if self.empty_polls > 64 {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
        Ok(())
    }

    fn should_sleep(&self, sends_pending: bool, now: Instant) -> bool {
        self.idle
            && !sends_pending
            && now.saturating_duration_since(self.last_completion) >= Duration::from_micros(250)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_deadline_does_not_submit() {
        assert_eq!(
            remaining(Instant::now()).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[test]
    fn active_progress_never_sleeps_and_idle_backoff_requires_no_sends() {
        let active = Progress::new(false);
        let idle = Progress::new(true);
        let later = Instant::now() + Duration::from_secs(1);
        assert!(!active.should_sleep(false, later));
        assert!(!active.should_sleep(true, later));
        assert!(!idle.should_sleep(false, idle.last_completion));
        assert!(!idle.should_sleep(true, later));
        assert!(idle.should_sleep(false, later));
    }

    #[test]
    fn provider_name_cannot_contain_nul() {
        assert!(Endpoint::open("efa\0tcp", None).is_err());
    }
}
