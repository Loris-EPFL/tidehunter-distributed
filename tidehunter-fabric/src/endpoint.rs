//! Rust ownership and completion state over OFI's generated libfabric bindings.

mod resources;
use crate::{FabricBackend, FabricConfig};
use ofi_libfabric_sys::bindgen as ffi;
use resources::{ADDRESS, Resources, error, invalid};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::{
    io::{self, IoSlice, Write},
    mem,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

const CHUNK: usize = 65_536;
/// Default per-endpoint slot count; a `FabricConfig` selects the actual count.
const SLOTS: usize = crate::DEFAULT_SLOTS;
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
    /// Diagnostic poll samples with all application TX slots provider-owned.
    pub tx_all_slots_pending_polls: u64,
    /// Diagnostic poll samples with no application RX slot recorded as posted.
    /// Pending receives may already have an unobserved CQ completion.
    pub rx_no_posted_slots_polls: u64,
    /// Diagnostic poll samples with at least one completed, unread RX slot.
    pub rx_unread_slots_polls: u64,
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
    /// Successful `fi_send` posts by posted message length. Always on: plain
    /// counter increments, no clock reads. Every normal post is at most 64 KiB,
    /// so the last bucket stays zero except for a future larger chunk size.
    pub send_size_le_4k: u64,
    pub send_size_le_16k: u64,
    pub send_size_le_64k: u64,
    pub send_size_gt_64k: u64,
    /// Every `fi_send` that returned EAGAIN (always on, unlike `send_post_would_block`).
    pub send_post_eagain: u64,
    /// Posts that reused the filled slot of an earlier EAGAIN without copying again.
    pub send_post_retries: u64,
    /// Idle sweeps that parked on the provider CQ wait object (not busy polls).
    pub cq_idle_waits: u64,
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

/// Observed provider setup and the provenance of its advertised capabilities.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderInfo {
    pub backend: FabricBackend,
    pub provider: String,
    pub fabric: Option<String>,
    pub domain: Option<String>,
    pub source_address: Option<IpAddr>,
    pub device: Option<String>,
    pub driver: Option<String>,
    pub bus_address: Option<String>,
    pub capability_evidence: String,
    /// Effective provider and geometry facts read once during open.
    #[serde(default)]
    pub geometry: ProviderGeometry,
}

/// One libfabric environment parameter owned by the selected provider.
/// `value` is the configured override (`None` when the environment does not set
/// it). `configured_or_default` is that override, else the default libfabric
/// states in the parameter's help text (parsed, so a heuristic). Neither is the
/// value the provider finally uses: `RxM` may clamp or derive limits internally
/// and libfabric does not expose them, so the actual eager and SAR thresholds
/// are unverified.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderParameter {
    pub name: String,
    pub value: Option<String>,
    pub configured_or_default: Option<String>,
    pub help: String,
}

/// Provider/domain/endpoint attributes captured from the selected `fi_info`.
/// These are negotiated capacities, not hardware queue depths or latencies.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGeometry {
    /// `fi_version()` of the loaded libfabric, formatted `major.minor`.
    pub libfabric_api_version: String,
    /// API version passed to `fi_getinfo`, formatted `major.minor`.
    pub requested_api_version: String,
    /// `fi_fabric_attr::prov_version`, formatted `major.minor`.
    pub provider_version: String,
    pub protocol: u32,
    pub protocol_version: u32,
    /// `tx_attr.inject_size` as reported by `fi_getinfo`. Not the `RxM` eager
    /// threshold: on the software provider it is 64 while the `RxM` buffer size
    /// default is 16384.
    pub tx_inject_size: usize,
    pub tx_size: usize,
    pub rx_size: usize,
    pub tx_iov_limit: usize,
    pub rx_iov_limit: usize,
    pub max_msg_size: usize,
    pub mr_mode: i32,
    pub mr_mode_flags: Vec<String>,
    pub data_progress: String,
    pub control_progress: String,
    pub threading: String,
    /// Application TX/RX slot count (each 64 KiB) selected for this endpoint.
    pub slots: usize,
    /// Completion-queue wait object actually opened: `none`, `fd` or `unspec`,
    /// plus why waiting is unavailable when the provider refused it.
    pub cq_wait: String,
    /// All `FI_OFI_RXM_*` parameters (buffer, SAR, eager, inject, queue sizes)
    /// when the selected provider is RxM-based; empty otherwise.
    pub rxm_parameters: Vec<ProviderParameter>,
}

/// One exclusively operated `FI_EP_RDM` endpoint with `slots` 64 KiB slots each
/// for sends and receives (default four, 512 KiB total; at most 64 each).
/// Buffers and provider contexts have stable allocations across thread moves.
pub struct Endpoint {
    resources: Option<Box<Resources>>,
    failed: bool,
}

// SAFETY: Provider operations require &mut self and use a domain negotiated as
// FI_THREAD_DOMAIN or FI_THREAD_SAFE, so moving the sole endpoint owner preserves
// serialized access. Provider handles, buffers and contexts keep stable addresses
// across thread moves.
unsafe impl Send for Endpoint {}

impl Endpoint {
    /// Opens exactly the configured libfabric provider, without software fallback.
    ///
    /// # Errors
    /// Reports invalid configuration, unavailable providers, unsupported
    /// capabilities, exhausted admission or native resource-setup failures.
    pub fn open(config: &FabricConfig) -> io::Result<Self> {
        Self::open_checked(config, false)
    }

    /// Like [`Endpoint::open`], for an owner that parks while idle: `verbs`
    /// additionally requests a CQ wait fd (software already parks in a bounded
    /// `fi_cq_sread`). Whether the provider granted a wait object is reported in
    /// `ProviderInfo::geometry.cq_wait`; a refusal keeps busy polling.
    ///
    /// # Errors
    /// Same as [`Endpoint::open`].
    pub fn open_idle_parking(config: &FabricConfig) -> io::Result<Self> {
        Self::open_checked(config, true)
    }

    fn open_checked(config: &FabricConfig, park_idle: bool) -> io::Result<Self> {
        config.validate()?;
        LIVE.fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
            (live < ENDPOINTS).then_some(live + 1)
        })
        .map_err(|_| io::Error::other("fabric endpoint admission exhausted"))?;
        let allocation = Instant::now();
        let mut endpoint = Self {
            resources: Some(Box::default()),
            failed: false,
        };
        endpoint.inner().open_profile.allocation_ns = resources::elapsed_ns(allocation);
        endpoint.inner().open_mode(config, park_idle)?;
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
    /// Returns actual provider/domain/device setup evidence without discovery calls.
    /// # Panics
    /// Panics if the internal ownership invariant is broken: successful open
    /// retains the provider metadata until this endpoint is dropped.
    #[must_use]
    pub fn provider_info(&self) -> &ProviderInfo {
        self.resources
            .as_deref()
            .and_then(Resources::provider_info)
            .expect("provider info exists after successful endpoint open")
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
    /// unposted slot owns no ordinal and is immediately reusable. If the next call
    /// presents the same bytes, the endpoint posts that filled slot without
    /// copying again; different bytes are copied afresh. Other errors poison.
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
        let result = (|| {
            let state = self.inner();
            if poll {
                state.poll()?;
            }
            state.send_copied(bytes)
        })();
        self.finish_try(result)
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
    /// reading the CQ. Sweep callers pair this with
    /// `try_progress_for_sweep`.
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
        let result = (|| {
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
        })();
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
        let state = self.inner();
        let result = state
            .poll()
            .and_then(|completed| state.try_post_receive().map(|posted| completed || posted));
        self.finish_try(result)
    }

    /// One CQ acquisition for a reactor sweep. An idle deadline permits a
    /// provider-managed bounded wait only when the message endpoint is fully
    /// quiescent.
    /// Returns `(made_progress, used_bounded_wait)`.
    /// # Errors
    /// Returns nonpoisoning `WouldBlock` for receive repost backpressure; other
    /// provider or packet errors poison the endpoint.
    pub fn try_progress_for_sweep(
        &mut self,
        idle_deadline: Option<Instant>,
    ) -> io::Result<(bool, bool)> {
        self.ready()?;
        let state = self.inner();
        let result = state
            .poll_for_sweep(idle_deadline)
            .and_then(|(completed, waited)| {
                state
                    .try_post_receive()
                    .map(|posted| (completed || posted, waited))
            });
        self.finish_try(result)
    }

    /// Whether setup qualified bounded CQ waiting for this software endpoint.
    /// This is a capability query only; transient sweep eligibility is stricter.
    #[must_use]
    pub fn supports_idle_wait(&self) -> bool {
        !self.failed
            && self
                .resources
                .as_deref()
                .is_some_and(Resources::supports_idle_wait)
    }

    /// Whether setup granted a CQ wait fd (`verbs` opened with
    /// [`Endpoint::open_idle_parking`] and accepted by the provider). A
    /// capability query only; [`Endpoint::arm_wait_fd`] checks quiescence.
    #[must_use]
    pub fn has_wait_fd(&self) -> bool {
        !self.failed
            && self
                .resources
                .as_deref()
                .is_some_and(Resources::has_wait_fd)
    }

    /// Arms the CQ wait fd for one caller-owned readiness wait that can also
    /// include other descriptors. `Ok(Some(fd))`: `fi_trywait` succeeded on a
    /// fully quiescent message endpoint; the caller may block in `poll(2)` until
    /// `fd` is readable and must then progress normally. `Ok(None)`: work is
    /// queued or the endpoint is not quiescent; progress without blocking. The fd
    /// stays owned by the provider and valid while this endpoint lives.
    /// # Errors
    /// `Unsupported` without a wait fd; a fatal `fi_trywait` code poisons the
    /// endpoint.
    pub fn arm_wait_fd(&mut self) -> io::Result<Option<std::os::fd::RawFd>> {
        self.ready()?;
        if !self.has_wait_fd() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "fabric endpoint has no completion queue wait fd",
            ));
        }
        let result = self.inner().arm_wait_fd();
        self.finish_try(result)
    }

    /// Derived ordered-stream readiness; never polls, copies or posts.
    /// # Errors
    /// Fails if the endpoint is unbound or poisoned.
    pub fn receive_ready(&mut self) -> io::Result<bool> {
        self.ready()?;
        Ok(self.inner().receive_ready())
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
    /// completion. Up to the configured slot count of sends can be in flight. The caller must continue
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
        let result = self.inner().poll().map(|_| ());
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
/// `try_*` operations above instead of parking inside an endpoint. Active waits
/// spin/yield; idle waits alone use bounded sleep backoff.
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
    fn domain_selector_cannot_contain_nul() {
        let config = FabricConfig {
            backend: FabricBackend::Verbs,
            domain: Some("verbs\0device".to_owned()),
            source_address: None,
            slots: None,
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
