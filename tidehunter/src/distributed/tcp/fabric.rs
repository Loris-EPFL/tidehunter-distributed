//! Optional OFI message transport beneath the existing storage protocol.
//! No physical WAL address or registration key is exported. Fabric completion
//! releases a send buffer; only the existing storage receipt proves durability.

use super::TcpError;
use serde::{Deserialize, Serialize};
#[cfg(feature = "experimental-fabric")]
use std::io::{self, Read, Write};
pub use tidehunter_fabric::{FabricBackend, FabricConfig};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FabricSecurity {
    #[default]
    MutualTls,
    Trusted,
}

/// Connection-local bounded progress evidence, including control traffic.
/// Counters saturate; occupancy gauges describe owned encoded output or reserved
/// reply credits, not total RSS, TLS/provider pools, or remote durability.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct TransportProgressStats {
    /// Progress sweeps with a later queued frame held behind plaintext/TLS output
    /// not yet accepted by transport. CQ retirement alone never gates submission.
    pub frame_gate_stalls: u64,
    /// Sweeps with submitted frames still held behind their local send fence.
    pub retirement_stalls: u64,
    pub frames_submitted: u64,
    pub frames_retired: u64,
    /// Current/peak frame count, including accepted frames awaiting local CQEs.
    pub output_frames: u64,
    pub peak_output_frames: u64,
    /// Current/peak sum of retained encoded frame allocation capacities.
    pub output_bytes: u64,
    pub peak_output_bytes: u64,
    pub awaiting_retirement_frames: u64,
    /// Server queued replies' request-credit reservations, including metadata and
    /// payload allowance. Not actual payload size; released only after the fence.
    pub retained_reply_credit_bytes: u64,
    pub peak_retained_reply_credit_bytes: u64,
    /// Actual local provider CQ calls/empty results/validated completion entries.
    /// Zero on TCP, whose writes transfer ownership to the socket synchronously.
    pub cq_polls: u64,
    pub cq_empty_polls: u64,
    pub cq_completions: u64,
    /// Opt-in maximum inter-poll interval while a local send is outstanding.
    /// Scheduler/manual-progress gap, not CQ completion latency or network RTT.
    pub cq_max_poll_gap_ns: u64,
    /// Opt-in poll-weighted application slot samples, not hardware WQE counts.
    pub tx_all_slots_pending_polls: u64,
    pub rx_no_posted_slots_polls: u64,
    pub rx_unread_slots_polls: u64,
    /// Actual TCP health probes on fabric lanes, gated independently of CQ/data
    /// polls at one millisecond (forced once before idle-lane reuse). Zero on TCP.
    pub control_peer_checks: u64,
    /// Opt-in encoded-frame enqueue to first successful plaintext write.
    pub output_queue_ns: u64,
    pub output_queue_samples: u64,
    /// First plaintext write to all ciphertext accepted by socket/provider.
    /// Includes TLS work, backpressure and progress-loop residence, not peer receipt.
    pub output_submit_ns: u64,
    pub output_submit_samples: u64,
    /// TCP retires at acceptance (zero); OFI includes scheduling/CQ polling.
    pub output_retire_ns: u64,
    pub output_retire_samples: u64,
    /// Opt-in frame encoding (size pass/allocation included); bytes include header.
    pub frame_encode: super::diagnostics::WorkProfile,
    /// Decode bytes exclude the four-byte framing header.
    pub frame_decode: super::diagnostics::WorkProfile,
    /// Rustls writer includes record encryption; reader copies decrypted bytes.
    pub plaintext_write: super::diagnostics::WorkProfile,
    pub plaintext_read: super::diagnostics::WorkProfile,
    /// Allocation/zero-initialization resize call; bytes are logical buffer length.
    pub plaintext_buffer_resize: super::diagnostics::WorkProfile,
    /// These calls enclose SocketProfile I/O; do not add the nested times.
    pub tls_write: super::diagnostics::WorkProfile,
    pub tls_read: super::diagnostics::WorkProfile,
    /// Packet authentication/decryption, without the read_tls transport call.
    /// Bytes are ciphertext supplied to this successful packet-processing call,
    /// not a count of unique plaintext (records may span multiple reads).
    pub tls_process: super::diagnostics::WorkProfile,
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
    /// Posted receive capacity, not delivered payload.
    pub receive_post_bytes: u64,
    pub receive_post_would_block: u64,
    pub receive_post_errors: u64,
    pub cq_call_ns: u64,
    pub cq_timed_calls: u64,
    pub cq_error_call_ns: u64,
    pub cq_error_calls: u64,
    pub cq_dispatch_ns: u64,
    pub cq_dispatch_calls: u64,
    pub cq_dispatch_errors: u64,
    /// All poll gaps, including receive-only/idle; not packet arrival delay.
    pub cq_poll_gap_ns: u64,
    pub cq_poll_gap_samples: u64,
    pub cq_poll_gap_max_ns: u64,
    /// Successful provider sends by posted message length (always on, no clock):
    /// at most 4 KiB, 16 KiB, 64 KiB, and above. Together with the provider
    /// geometry in `ProviderInfo` these locate eager/SAR/rendezvous boundaries.
    pub send_size_le_4k: u64,
    pub send_size_le_16k: u64,
    pub send_size_le_64k: u64,
    pub send_size_gt_64k: u64,
    /// Provider sends that returned EAGAIN, and posts that reused the filled
    /// slot of an earlier EAGAIN without copying the bytes again.
    pub send_post_eagain: u64,
    pub send_post_retries: u64,
    /// Idle sweeps that parked on the provider CQ wait object instead of
    /// busy polling. Zero when no wait object exists (see `ProviderInfo`).
    pub cq_idle_waits: u64,
}

impl TransportProgressStats {
    pub(crate) fn add(&mut self, other: Self) {
        macro_rules! sum {
            ($($field:ident),* $(,)?) => {$(
                self.$field = self.$field.saturating_add(other.$field);
            )*};
        }
        sum!(
            frame_gate_stalls,
            retirement_stalls,
            frames_submitted,
            frames_retired,
            output_frames,
            output_bytes,
            awaiting_retirement_frames,
            retained_reply_credit_bytes,
            cq_polls,
            cq_empty_polls,
            cq_completions,
            tx_all_slots_pending_polls,
            rx_no_posted_slots_polls,
            rx_unread_slots_polls,
            control_peer_checks,
            output_queue_ns,
            output_queue_samples,
            output_submit_ns,
            output_submit_samples,
            output_retire_ns,
            output_retire_samples,
            send_copy_ns,
            send_copy_calls,
            send_copy_bytes,
            receive_copy_ns,
            receive_copy_calls,
            receive_copy_bytes,
            send_post_ns,
            send_post_calls,
            send_post_bytes,
            send_post_would_block,
            send_post_errors,
            receive_post_ns,
            receive_post_calls,
            receive_post_bytes,
            receive_post_would_block,
            receive_post_errors,
            cq_call_ns,
            cq_timed_calls,
            cq_error_call_ns,
            cq_error_calls,
            cq_dispatch_ns,
            cq_dispatch_calls,
            cq_dispatch_errors,
            cq_poll_gap_ns,
            cq_poll_gap_samples,
            send_size_le_4k,
            send_size_le_16k,
            send_size_le_64k,
            send_size_gt_64k,
            send_post_eagain,
            send_post_retries,
            cq_idle_waits,
        );
        // Aggregated peaks are maxima of individual connections, not a sampled
        // simultaneous cluster occupancy. Current gauges above are summed.
        self.peak_output_frames = self.peak_output_frames.max(other.peak_output_frames);
        self.peak_output_bytes = self.peak_output_bytes.max(other.peak_output_bytes);
        self.peak_retained_reply_credit_bytes = self
            .peak_retained_reply_credit_bytes
            .max(other.peak_retained_reply_credit_bytes);
        self.cq_max_poll_gap_ns = self.cq_max_poll_gap_ns.max(other.cq_max_poll_gap_ns);
        self.cq_poll_gap_max_ns = self.cq_poll_gap_max_ns.max(other.cq_poll_gap_max_ns);
        self.frame_encode.add(other.frame_encode);
        self.frame_decode.add(other.frame_decode);
        self.plaintext_write.add(other.plaintext_write);
        self.plaintext_read.add(other.plaintext_read);
        self.plaintext_buffer_resize
            .add(other.plaintext_buffer_resize);
        self.tls_write.add(other.tls_write);
        self.tls_read.add(other.tls_read);
        self.tls_process.add(other.tls_process);
    }
    #[cfg(feature = "experimental-fabric")]
    pub(super) fn set_provider(&mut self, provider: tidehunter_fabric::ProgressStats) {
        macro_rules! copy {
            ($($field:ident),* $(,)?) => {$(self.$field = provider.$field;)*};
        }
        copy!(
            cq_polls,
            cq_empty_polls,
            cq_completions,
            cq_max_poll_gap_ns,
            tx_all_slots_pending_polls,
            rx_no_posted_slots_polls,
            rx_unread_slots_polls,
            send_copy_ns,
            send_copy_calls,
            send_copy_bytes,
            receive_copy_ns,
            receive_copy_calls,
            receive_copy_bytes,
            send_post_ns,
            send_post_calls,
            send_post_bytes,
            send_post_would_block,
            send_post_errors,
            receive_post_ns,
            receive_post_calls,
            receive_post_bytes,
            receive_post_would_block,
            receive_post_errors,
            cq_call_ns,
            cq_timed_calls,
            cq_error_call_ns,
            cq_error_calls,
            cq_dispatch_ns,
            cq_dispatch_calls,
            cq_dispatch_errors,
            cq_poll_gap_ns,
            cq_poll_gap_samples,
            cq_poll_gap_max_ns,
            send_size_le_4k,
            send_size_le_16k,
            send_size_le_64k,
            send_size_gt_64k,
            send_post_eagain,
            send_post_retries,
            cq_idle_waits,
        );
    }
}
/// Transport selection is explicit: a selected fabric is never replaced with TCP.
///
/// Security is part of the selection and is never inferred or downgraded:
/// `Tcp` and `Ofi { security: MutualTls }` authenticate and encrypt with mutual
/// TLS; `TcpTrusted` and `Ofi { security: Trusted }` are the explicit
/// trusted-network/no-TLS modes. A no-TLS mode has no confidentiality or
/// cryptographic peer authentication and is only for an isolated network;
/// WAL integrity, fencing and durability checks remain.
///
/// Fair-comparison partners: `Tcp` against `Ofi { MutualTls }` (both mutual
/// TLS, `security_label()` = `mutual_tls`), and `TcpTrusted` against
/// `Ofi { Trusted }` (both `trusted_network_no_tls`). Never compare across the
/// two security classes as an isolated network effect.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Transport {
    Tcp,
    /// Explicit opt-in: plain TCP with the trust semantics of trusted OFI. It
    /// requires empty TLS credential paths and is never a default or fallback.
    TcpTrusted,
    Ofi {
        fabric: FabricConfig,
        #[serde(default)]
        security: FabricSecurity,
    },
}

impl Default for Transport {
    fn default() -> Self {
        Self::Tcp
    }
}

impl Transport {
    /// Selects an OFI provider without a local domain or source-address constraint.
    #[must_use]
    pub fn ofi(backend: FabricBackend, security: FabricSecurity) -> Self {
        Self::Ofi {
            fabric: FabricConfig {
                backend,
                domain: None,
                source_address: None,
                slots: None,
            },
            security,
        }
    }

    pub fn validate(&self) -> Result<(), TcpError> {
        if let Self::Ofi { fabric, .. } = self {
            fabric.validate().map_err(TcpError::component)?;
            if !cfg!(feature = "experimental-fabric") {
                return Err(TcpError::new("OFI transport requires experimental-fabric"));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn is_fabric(&self) -> bool {
        matches!(self, Self::Ofi { .. })
    }

    /// True for the explicit trusted-network/no-TLS modes (`TcpTrusted` and
    /// `Ofi { Trusted }`): no TLS configuration, certificate or ALPN exists.
    #[must_use]
    pub fn is_native(&self) -> bool {
        matches!(
            self,
            Self::TcpTrusted
                | Self::Ofi {
                    security: FabricSecurity::Trusted,
                    ..
                }
        )
    }

    /// Receipt label for the transport's security class: `mutual_tls` or
    /// `trusted_network_no_tls`. Experiments must record it beside `label()`.
    #[must_use]
    pub fn security_label(&self) -> &'static str {
        if self.is_native() {
            "trusted_network_no_tls"
        } else {
            "mutual_tls"
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::TcpTrusted => "tcp-trusted",
            Self::Ofi { fabric, security } => match (&fabric.backend, security) {
                (FabricBackend::Verbs, FabricSecurity::MutualTls) => "ofi-verbs-mutual-tls",
                (FabricBackend::Verbs, FabricSecurity::Trusted) => "ofi-verbs-trusted",
                (FabricBackend::SoftwareTcp, FabricSecurity::MutualTls) => {
                    "ofi-software-tcp-mutual-tls"
                }
                (FabricBackend::SoftwareTcp, FabricSecurity::Trusted) => "ofi-software-tcp-trusted",
            },
        }
    }

    #[must_use]
    pub fn same_protocol(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tcp, Self::Tcp) | (Self::TcpTrusted, Self::TcpTrusted) => true,
            (
                Self::Ofi {
                    fabric: left,
                    security: left_security,
                },
                Self::Ofi {
                    fabric: right,
                    security: right_security,
                },
            ) => left.backend == right.backend && left_security == right_security,
            _ => false,
        }
    }

    pub(super) fn alpn(&self) -> Vec<u8> {
        format!("tidehunter-{}-v{}", self.label(), super::PROTOCOL).into_bytes()
    }

    pub(super) fn server_alpn(&self) -> Vec<Vec<u8>> {
        vec![self.alpn()]
    }

    #[cfg(feature = "experimental-fabric")]
    fn open(&self, park_idle: bool) -> io::Result<tidehunter_fabric::Endpoint> {
        let Self::Ofi { fabric, .. } = self else {
            return Err(io::Error::other("TCP needs no fabric endpoint"));
        };
        if park_idle {
            tidehunter_fabric::Endpoint::open_idle_parking(fabric)
        } else {
            tidehunter_fabric::Endpoint::open(fabric)
        }
    }
}

/// Owner-side provider facts, once per accepted session. The client reads its own
/// through `Client::provider_info`; an owner has no response field for them, so
/// they use the local-observer line protocol on stderr (never a wire field).
/// Observer IO must not fail a session.
#[cfg(feature = "experimental-fabric")]
pub(super) fn report_owner_provider(endpoint: &tidehunter_fabric::Endpoint) {
    let record = serde_json::json!({
        "schema": 1,
        "role": "owner",
        "provider": endpoint.provider_info(),
    });
    let _ = writeln!(
        io::stderr().lock(),
        "TIDEHUNTER_FABRIC_PROVIDER_FACTS {record}"
    );
}

// Exchange bounded endpoint addresses over the selected control stream; only
// endpoint setup is provider-specific.
#[cfg(feature = "experimental-fabric")]
pub(super) fn bootstrap(
    stream: &mut (impl Read + Write),
    transport: &Transport,
) -> Result<tidehunter_fabric::Endpoint, TcpError> {
    bootstrap_with(stream, transport, false)
}

/// Owner-side bootstrap: the endpoint may park on the provider CQ wait object
/// while no request is pending, instead of spinning (`verbs` gets a wait fd).
#[cfg(feature = "experimental-fabric")]
pub(super) fn bootstrap_idle_parking(
    stream: &mut (impl Read + Write),
    transport: &Transport,
) -> Result<tidehunter_fabric::Endpoint, TcpError> {
    let endpoint = bootstrap_with(stream, transport, true)?;
    report_owner_provider(&endpoint);
    Ok(endpoint)
}

/// Coordinator durable-progress lane bootstrap: the endpoint requests a CQ wait
/// fd like an owner (`verbs`), and must have one. Without it the lane would have
/// to spin, so setup fails with a named error before the Status handshake. The
/// owner endpoint is qualified by the same rule when it parks the lane.
#[cfg(feature = "experimental-fabric")]
pub(super) fn bootstrap_progress(
    stream: &mut (impl Read + Write),
    transport: &Transport,
) -> Result<tidehunter_fabric::Endpoint, TcpError> {
    let endpoint = bootstrap_with(stream, transport, true)?;
    if !endpoint.has_wait_fd() {
        return Err(TcpError::wait_unavailable(format_args!(
            "progress lane transport {} has no completion queue wait fd (provider {}: {}); \
             use a TCP transport for this deployment",
            transport.label(),
            endpoint.provider_info().provider,
            endpoint.provider_info().geometry.cq_wait
        )));
    }
    Ok(endpoint)
}

#[cfg(feature = "experimental-fabric")]
fn bootstrap_with(
    stream: &mut (impl Read + Write),
    transport: &Transport,
    park_idle: bool,
) -> Result<tidehunter_fabric::Endpoint, TcpError> {
    let mut endpoint = transport.open(park_idle)?;
    let address = endpoint.address()?;
    let len = u16::try_from(address.len()).map_err(TcpError::component)?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&address)?;
    stream.flush()?;
    let mut header = [0; 2];
    stream.read_exact(&mut header)?;
    let len = usize::from(u16::from_be_bytes(header));
    if len == 0 || len > 512 {
        return Err(TcpError::new("invalid fabric address length"));
    }
    let mut peer = [0; 512];
    stream.read_exact(&mut peer[..len])?;
    endpoint.set_peer(&peer[..len])?;
    stream.write_all(&[1])?;
    stream.flush()?;
    let mut ready = [0];
    stream.read_exact(&mut ready)?;
    if ready != [1] {
        return Err(TcpError::new("invalid fabric ready marker"));
    }
    stream.flush()?;
    Ok(endpoint)
}
