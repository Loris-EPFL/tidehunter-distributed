//! Rust ownership and completion state over OFI's generated libfabric bindings.

mod efa_direct;
mod resources;
use crate::{FabricBackend, FabricConfig};
use efa_direct::{ControlWindow, SendWindow, StreamReceiver};
use ofi_libfabric_sys::bindgen as ffi;
use resources::{ADDRESS, Resources, error, invalid};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::{
    any::Any,
    io::{self, IoSlice, Write},
    mem,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const CHUNK: usize = 65_536;
const SLOTS: usize = 4;
const PROGRESS_LIMIT: usize = 10_000_000;
pub(crate) const ENDPOINTS: usize = 1024;
pub(super) const RMA_CANCELLED: u64 = 1 << 63;
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
    /// Successful EFA-direct DATA packets, excluding EAGAIN attempts.
    pub efa_direct_data_posts: u64,
    /// Successful standalone EFA-direct CREDIT packets, excluding DATA-carried credit.
    pub efa_direct_credit_posts: u64,
    /// Successful DATA posts that advanced the advertised cumulative credit frontier.
    pub efa_direct_credit_piggyback_advances: u64,
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
/// Fixed per-endpoint bounds for registered one-sided reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RmaConfig {
    pub source_bytes: usize,
    pub source_region_slots: usize,
    pub destination_bytes: usize,
    pub destination_slots: usize,
    pub max_read_bytes: usize,
    pub require_device_read: bool,
}

/// The provider's selected memory addressing and read implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RmaCapabilities {
    pub address_mode: RmaAddressMode,
    pub read_mode: RmaReadMode,
    pub max_read_bytes: usize,
    pub max_iov: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RmaAddressMode {
    Virtual,
    Offset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RmaReadMode {
    Native,
    Emulated,
    Unknown,
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
    pub rma_capabilities: Option<RmaCapabilities>,
    pub capability_evidence: String,
}

/// Wire-safe remote address; contains no process-local MR descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteReadRegion {
    pub region_id: u64,
    pub generation: u64,
    /// Peer-defined window base: a virtual address or an MR-relative offset.
    pub remote_address: u64,
    pub key: u64,
    pub len: u64,
}

/// Opaque attestation that a peer/session grant has been drained.
/// Its constructor is unsafe because remote-DMA quiescence cannot be inferred
/// from local CQ, timeout, endpoint close, or MR close state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerDrainToken {
    session_id: u64,
    grant_generation: u64,
}

impl PeerDrainToken {
    /// Creates an attestation after the exact peer's grant has been drained.
    ///
    /// # Safety
    /// The caller must have closed admission for this session and grant, and
    /// validated an acknowledgement on that exact peer session proving every accepted
    /// `FI_READ` for this grant reached terminal completion. A timeout,
    /// local endpoint/MR close, failed operation, unknown CQ context, or assumed
    /// idle interval is not sufficient.
    #[must_use]
    pub const unsafe fn after_ack(session_id: u64, grant_generation: u64) -> Self {
        Self {
            session_id,
            grant_generation,
        }
    }
}

/// Endpoint-local handle for a reserved or registered immutable source region.
#[derive(Debug, Eq, PartialEq)]
pub struct RegisteredReadRegion {
    owner_id: u64,
    region_id: u64,
    generation: u64,
    session_id: u64,
    grant_generation: u64,
}

impl RegisteredReadRegion {
    /// Stable identity to include in the immutable member envelope.
    #[must_use]
    pub const fn region_id(&self) -> u64 {
        self.region_id
    }

    /// Generation paired with `region_id` in the immutable member envelope.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Identifier for one accepted `FI_READ`. Dropping it never cancels provider
/// work or releases its registered destination.
pub struct ReadOperation {
    slot: usize,
    generation: u64,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicU64>,
    finished: bool,
}

impl Drop for ReadOperation {
    fn drop(&mut self) {
        if !self.finished {
            self.cancelled.fetch_or(RMA_CANCELLED, Ordering::Release);
        }
    }
}

#[derive(Debug)]
struct ReadBufferStorage {
    bytes: Box<[std::cell::UnsafeCell<u8>]>,
    len: AtomicUsize,
    charged_bytes: usize,
    charged_slots: usize,
}

// SAFETY: Resources returns a ReadBuffer only for a terminal successful CQE.
// It does not reuse the backing slot while any ReadBuffer Arc remains alive.
unsafe impl Sync for ReadBufferStorage {}

impl Drop for ReadBufferStorage {
    fn drop(&mut self) {
        resources::release_destination_charge(self.charged_bytes, self.charged_slots);
    }
}

/// Stable owned bytes whose backing DMA slot remains unavailable until drop.
#[derive(Clone, Debug)]
pub struct ReadBuffer {
    storage: std::sync::Arc<ReadBufferStorage>,
}

impl AsRef<[u8]> for ReadBufferStorage {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: Only a completed DMA slot is exposed as an owner, and Arc
        // ownership prevents endpoint reuse while this immutable borrow exists.
        unsafe {
            std::slice::from_raw_parts(
                self.bytes.as_ptr().cast::<u8>(),
                self.len.load(Ordering::Acquire),
            )
        }
    }
}

impl minibytes::BytesOwner for ReadBufferStorage {}

impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self.storage.as_ref().as_ref()
    }
}

impl ReadBuffer {
    /// Transfers the preallocated destination-slot lease into a zero-copy owner.
    #[must_use]
    pub fn into_minibytes_bytes(self) -> minibytes::Bytes {
        minibytes::Bytes::from_shared_owner(self.storage)
    }
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
    /// Opens exactly the configured libfabric provider, without software fallback.
    ///
    /// # Errors
    /// Reports invalid configuration, unavailable providers, unsupported
    /// capabilities, exhausted admission or native resource-setup failures.
    pub fn open(config: &FabricConfig, rma: Option<RmaConfig>) -> io::Result<Self> {
        config.validate()?;
        if config.backend == FabricBackend::Efa && rma.is_some() {
            return Err(invalid(
                "regular EFA RMA is unsupported; select the explicit efa-direct backend",
            ));
        }
        Self::open_inner(config, rma)
    }

    /// Test-only packet-protocol exercise over the explicitly selected software provider.
    #[cfg(test)]
    fn open_software_packet_test(domain: Option<&str>) -> io::Result<Self> {
        let config = FabricConfig {
            backend: FabricBackend::SoftwareTcp,
            domain: domain.map(str::to_owned),
            source_address: None,
        };
        let mut endpoint = Self::open(&config, None)?;
        endpoint.inner().enable_test_packet_adapter();
        endpoint.direct = Some(Box::new(DirectState::new(4096)?));
        Ok(endpoint)
    }

    fn open_inner(config: &FabricConfig, rma: Option<RmaConfig>) -> io::Result<Self> {
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
        endpoint.inner().open_mode(config, rma)?;
        if config.backend.is_direct() {
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

    /// Returns negotiated RMA capabilities, or `None` for message-only opens.
    #[must_use]
    pub fn rma_capabilities(&self) -> Option<RmaCapabilities> {
        self.resources
            .as_deref()
            .and_then(Resources::rma_capabilities)
    }
    /// Counts currently reusable destination slots; this is advisory only.
    /// The subsequent post is authoritative and can still return `WouldBlock`.
    /// # Errors
    /// Reports a failed endpoint or unavailable RMA state.
    pub fn available_read_slots(&mut self) -> io::Result<usize> {
        self.ready()?;
        self.inner().available_read_slots()
    }

    /// Reserves an immutable source-region identity before encoding its envelope.
    ///
    /// The reservation consumes one bounded source-region slot but does not
    /// allocate or register bytes. Finalize the envelope using its identity,
    /// then call `register_read_region`.
    /// # Errors
    /// Rejects invalid identities, failed endpoints and exhausted source slots.
    pub fn reserve_read_region(
        &mut self,
        session_id: u64,
        grant_generation: u64,
    ) -> io::Result<RegisteredReadRegion> {
        self.ready()?;
        self.inner()
            .reserve_read_region(session_id, grant_generation)
    }

    /// Registers the finalized immutable envelope for its prior reservation.
    /// # Errors
    /// Rejects foreign or inactive reservations and exhausted byte budgets.
    /// Provider registration errors retain any uncertain native ownership.
    pub fn register_read_region(
        &mut self,
        region: &RegisteredReadRegion,
        bytes: Vec<u8>,
    ) -> io::Result<RemoteReadRegion> {
        self.ready()?;
        let result = self.inner().register_read_region(region, bytes);
        if result.as_ref().is_err_and(|error| {
            !matches!(
                error.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::WouldBlock
            )
        }) {
            self.failed = true;
        }
        result
    }

    /// Registers a finalized immutable envelope and retains its original source
    /// owner for exactly as long as the provider-owned region is retained.
    ///
    /// Use this for buffers whose native authority or backing storage must stay
    /// pinned while remote DMA may still address the registration. The owner is
    /// retained on uncertain registration teardown and endpoint quarantine, and
    /// released after exact peer drain and successful MR retirement.
    /// # Errors
    /// Rejects foreign or inactive reservations and exhausted source budgets.
    /// Provider registration errors retain the owner whenever native ownership
    /// is uncertain.
    pub fn register_read_region_with_owner(
        &mut self,
        region: &RegisteredReadRegion,
        bytes: Vec<u8>,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> io::Result<RemoteReadRegion> {
        self.ready()?;
        let result = self
            .inner()
            .register_read_region_with_owner(region, bytes, Some(owner));
        if result.as_ref().is_err_and(|error| {
            !matches!(
                error.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::WouldBlock
            )
        }) {
            self.failed = true;
        }
        result
    }

    /// Retires a source region only after the matching peer drain acknowledgement.
    /// # Errors
    /// Rejects foreign regions, inactive identities and mismatched drain tokens.
    /// Native close failures quarantine the registered source.
    pub fn retire_read_region(
        &mut self,
        region: RegisteredReadRegion,
        token: PeerDrainToken,
    ) -> io::Result<()> {
        self.ready()?;
        let result = self.inner().retire_read_region(region, token);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::InvalidInput)
        {
            self.failed = true;
        }
        result
    }
    /// Reclaims a source registered for a grant that was never published.
    ///
    /// # Errors
    /// Reports invalid region identity, failed endpoints or native close failure.
    /// Uncertain ownership remains quarantined.
    ///
    /// # Safety
    /// The caller must prove that neither this region's descriptor nor any
    /// equivalent address/key/length was ever exposed to the remote peer.
    pub unsafe fn retire_unpublished_read_region(
        &mut self,
        region: RegisteredReadRegion,
    ) -> io::Result<()> {
        self.ready()?;
        // SAFETY: The caller upholds the documented unpublished-region proof.
        let result = unsafe { self.inner().retire_unpublished_read_region(region) };
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::InvalidInput)
        {
            self.failed = true;
        }
        result
    }

    /// Posts one actual `FI_READ` from a previously registered peer region.
    /// `WouldBlock` means no operation was accepted and ownership was not taken.
    /// # Errors
    /// Rejects invalid ranges and failed endpoints. Exhausted destinations or
    /// provider backpressure return `WouldBlock` without accepting a read.
    pub fn try_post_read(
        &mut self,
        remote: &RemoteReadRegion,
        offset: usize,
        len: usize,
    ) -> io::Result<ReadOperation> {
        self.ready()?;
        let result = self.inner().try_post_read(remote, offset, len);
        match result {
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => Err(error),
            result => self.finish_try(result),
        }
    }

    /// Polls the shared CQ once and returns the exact operation's terminal bytes.
    /// The destination remains unavailable until every returned `ReadBuffer`
    /// clone is dropped. Dropping the operation before completion never cancels DMA.
    /// # Errors
    /// The outer result reports foreign operations or endpoint/progress errors.
    /// The inner result reports the accepted operation's terminal provider error.
    pub fn try_read_completion(
        &mut self,
        operation: &mut ReadOperation,
    ) -> io::Result<Option<io::Result<ReadBuffer>>> {
        self.ready()?;
        self.inner().validate_read_operation(operation)?;
        let result = self
            .inner()
            .poll()
            .and_then(|_| self.inner().take_read_completion(operation));
        self.finish_try(result)
    }

    /// Takes a previously progressed operation without polling the shared CQ.
    ///
    /// Batch owners can call `try_progress` once and then harvest each operation.
    /// Destination ownership and generation checks match `try_read_completion`.
    /// # Errors
    /// The outer result reports foreign operations or a failed endpoint.
    /// The inner result reports the accepted operation's terminal provider error.
    pub fn take_read_completion(
        &mut self,
        operation: &mut ReadOperation,
    ) -> io::Result<Option<io::Result<ReadBuffer>>> {
        self.ready()?;
        let result = self.inner().take_read_completion(operation);
        match result {
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => Err(error),
            result => self.finish_try(result),
        }
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
            self.direct_progress(true)?;
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
        let direct = self
            .direct
            .as_mut()
            .ok_or_else(|| invalid("missing EFA-direct state"))?;
        direct.send.posted(ordinal)?;
        let piggyback_advanced = direct.control.advertised_by_data(credit);
        state.record_direct_data_post(piggyback_advanced);
        Ok(packet_len - efa_direct::HEADER_LEN)
    }

    fn direct_receive(
        &mut self,
        output: &mut [u8],
        poll: bool,
        defer_credit: bool,
    ) -> io::Result<usize> {
        if poll {
            self.direct_progress(true)?;
        }
        let result = self
            .direct
            .as_mut()
            .ok_or_else(|| invalid("missing EFA-direct state"))?
            .receive
            .read(output);
        match result {
            Ok((len, Some(_credit))) => {
                // The reactor's sweep adapter defers CREDIT so queued DATA can
                // piggyback its cumulative frontier. Blocking helpers retain
                // eager control progress before returning consumed bytes.
                if !defer_credit {
                    let _ = self.queue_direct_control()?;
                }
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
        state.record_direct_credit_post();
        Ok(true)
    }

    fn direct_progress(&mut self, flush_credit: bool) -> io::Result<bool> {
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
        if flush_credit && self.queue_direct_control()? {
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
    /// reading the CQ. Sweep callers pair this with
    /// `try_progress_for_sweep`; standalone `try_progress` keeps eager control progress.
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
            self.direct_receive(bytes, poll, !poll)
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
            self.direct_progress(true)
        } else {
            let state = self.inner();
            state
                .poll()
                .and_then(|completed| state.try_post_receive().map(|posted| completed || posted))
        };
        self.finish_try(result)
    }

    /// One CQ acquisition for a reactor sweep. An idle deadline permits a
    /// provider-managed bounded wait only when the message endpoint is fully
    /// quiescent; it never changes direct or RMA progress.
    /// Returns `(made_progress, used_bounded_wait)`.
    /// # Errors
    /// Returns nonpoisoning `WouldBlock` for receive repost backpressure; other
    /// provider or packet errors poison the endpoint.
    pub fn try_progress_for_sweep(
        &mut self,
        idle_deadline: Option<Instant>,
    ) -> io::Result<(bool, bool)> {
        self.ready()?;
        let result = if self.direct.is_some() {
            self.direct_progress(false)
                .map(|progress| (progress, false))
        } else {
            let state = self.inner();
            state
                .poll_for_sweep(idle_deadline)
                .and_then(|(completed, waited)| {
                    state
                        .try_post_receive()
                        .map(|posted| (completed || posted, waited))
                })
        };
        self.finish_try(result)
    }

    /// Whether setup qualified bounded CQ waiting for this software endpoint.
    /// This is a capability query only; transient sweep eligibility is stricter.
    #[must_use]
    pub fn supports_idle_wait(&self) -> bool {
        !self.failed
            && self.direct.is_none()
            && self
                .resources
                .as_deref()
                .is_some_and(Resources::supports_idle_wait)
    }

    /// Sends standalone EFA-direct credit debt without acquiring the CQ.
    /// # Errors
    /// Returns provider/packet errors; a nonfatal unavailable slot or EAGAIN
    /// leaves credit debt pending for the next sweep.
    pub fn finish_sweep_no_poll(&mut self) -> io::Result<bool> {
        self.ready()?;
        let result = if self.direct.is_some() {
            self.queue_direct_control()
        } else {
            Ok(false)
        };
        self.finish_try(result)
    }

    /// Derived ordered-stream readiness; never polls, copies or posts.
    /// # Errors
    /// Fails if the endpoint is unbound or poisoned.
    pub fn receive_ready(&mut self) -> io::Result<bool> {
        self.ready()?;
        if let Some(direct) = &self.direct {
            Ok(direct.receive.receive_ready())
        } else {
            Ok(self.inner().receive_ready())
        }
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
                        self.direct_progress(true)?;
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
            self.direct_progress(true).map(|_| ())
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
                self.direct_progress(true)?;
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
                match self.direct_receive(bytes, false, false) {
                    Ok(len) => return Ok(len),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        self.direct_progress(true)?;
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
            if resources.has_live_read_regions() {
                // A successful local endpoint close does not prove that remote
                // initiators have stopped reading a published registered region.
                // Retain the entire domain, MR, bytes and admission permit.
                mem::forget(resources);
                let _ = writeln!(
                    io::stderr(),
                    "fabric source region not peer-drained; bounded resources quarantined"
                );
                return;
            }
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
    fn domain_selector_cannot_contain_nul() {
        let config = FabricConfig {
            backend: FabricBackend::Verbs,
            domain: Some("verbs\0device".to_owned()),
            source_address: None,
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
