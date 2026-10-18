//! Optional EFA message transport beneath the existing authenticated TLS stream.
//! No physical WAL address or registration key is exported. Fabric completion
//! releases a send buffer; only the existing storage receipt proves durability.

use super::TcpError;
use serde::{Deserialize, Serialize};
#[cfg(feature = "experimental-fabric")]
use std::io::{self, Read, Write};

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
    /// Ciphertext acceptance to observing its contiguous local completion frontier.
    /// TCP retires at acceptance (zero); EFA includes scheduling/CQ polling.
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
        );
    }
}
/// Explicit selection prevents a missing EFA device from producing a TCP result
/// labelled RDMA. TCP remains the default and works without native libraries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Transport {
    #[default]
    Tcp,
    Efa,
    /// Software libfabric provider for correctness tests, never RDMA evidence.
    FabricTcp,
    /// Hardware direct fabric with explicit bounded stream ordering and credits.
    EfaDirect,
}

impl Transport {
    /// # Errors
    /// Rejects a fabric selection when the native feature is absent.
    pub fn validate(self) -> Result<(), TcpError> {
        if self != Self::Tcp && !cfg!(feature = "experimental-fabric") {
            return Err(TcpError::new(
                "fabric transport requires experimental-fabric",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Efa => "efa",
            Self::EfaDirect => "efa-direct",
            Self::FabricTcp => "fabric-tcp",
        }
    }

    pub(super) fn alpn(self) -> Vec<u8> {
        format!("tidehunter-{}-v{}", self.label(), super::PROTOCOL).into_bytes()
    }

    pub(super) fn server_alpn(self) -> Vec<Vec<u8>> {
        if self == Self::Tcp {
            vec![self.alpn()]
        } else {
            vec![self.alpn(), Self::Tcp.alpn()]
        }
    }

    #[cfg(feature = "experimental-fabric")]
    fn open(self) -> io::Result<tidehunter_fabric::Endpoint> {
        match self {
            Self::Efa => tidehunter_fabric::Endpoint::open("efa", None),
            Self::EfaDirect => tidehunter_fabric::Endpoint::open_efa_direct(None),
            Self::FabricTcp => tidehunter_fabric::Endpoint::open("tcp;ofi_rxm", Some("lo")),
            Self::Tcp => Err(io::Error::other("TCP needs no fabric endpoint")),
        }
    }
}

// Both peers use the authenticated TCP TLS session to exchange bounded native
// addresses and a ready marker. TLS continues across the new ordered stream;
// authentication, confidentiality and record sequence numbers remain intact.
#[cfg(feature = "experimental-fabric")]
pub(super) fn bootstrap(
    stream: &mut (impl Read + Write),
    transport: Transport,
) -> Result<tidehunter_fabric::Endpoint, TcpError> {
    let mut endpoint = transport.open()?;
    let address = endpoint.address()?;
    let len = u16::try_from(address.len()).map_err(TcpError::component)?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&address)?;
    stream.flush()?;
    let mut header = [0; 2];
    stream.read_exact(&mut header)?;
    let len = usize::from(u16::from_be_bytes(header));
    if len == 0 || len > 512 {
        return Err(TcpError::new("invalid authenticated fabric address length"));
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
