//! Opt-in monotonic clocks. Protocol v9 retains interleaved requests: socket clocks are
//! connection-wide (including control traffic); they are not assigned to a read.
//! Handler spans overlap. A completed read first awaits its authority recheck,
//! then response encoding, then response-send (output queue/TLS/local retirement).
//! Transport output queue/submit/retire use their own transition clocks, not an
//! additive request budget or peer-receipt time. Summed node clocks are not cluster
//! wall time. Recheck samples count completed checks; a disconnected request has
//! no exported terminal span.
use serde::{Deserialize, Serialize};

/// Local synchronous call wall time, not CPU time. Errors include WouldBlock;
/// bytes count successfully handled bytes in the named scope, not unique wire
/// payload. Nested scopes must not be added.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct WorkProfile {
    pub ns: u64,
    pub calls: u64,
    pub bytes: u64,
    pub errors: u64,
}

impl WorkProfile {
    pub(crate) fn add(&mut self, other: Self) {
        self.ns = self.ns.saturating_add(other.ns);
        self.calls = self.calls.saturating_add(other.calls);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.errors = self.errors.saturating_add(other.errors);
    }

    pub(super) fn record<E>(
        &mut self,
        began: Option<std::time::Instant>,
        result: &Result<usize, E>,
    ) {
        if let Some(began) = began {
            self.add(Self {
                ns: super::elapsed_ns(began),
                calls: 1,
                bytes: result.as_ref().copied().unwrap_or(0) as u64,
                errors: u64::from(result.is_err()),
            });
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct SocketProfile {
    /// Local TX completion drain, separate from queue submission. No durability implication.
    pub fabric_drain_ns: u64,
    pub fabric_drains: u64,
    pub read_ns: u64,
    pub write_ns: u64,
    pub timeout_setup_ns: u64,
    pub read_calls: u64,
    pub write_calls: u64,
    pub timeout_sets: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

impl SocketProfile {
    pub(crate) fn add(&mut self, other: Self) {
        self.fabric_drain_ns += other.fabric_drain_ns;
        self.fabric_drains += other.fabric_drains;
        self.read_ns += other.read_ns;
        self.write_ns += other.write_ns;
        self.timeout_setup_ns += other.timeout_setup_ns;
        self.read_calls += other.read_calls;
        self.write_calls += other.write_calls;
        self.timeout_sets += other.timeout_sets;
        self.read_bytes += other.read_bytes;
        self.write_bytes += other.write_bytes;
    }
}

/// Opt-in server network-owner evidence, scoped/reset with ConnectionProfile.
/// Counts include diagnostic control traffic; durations are not network RTT.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct PollingProfile {
    pub progress_sweeps: u64,
    /// Readiness waits by drained TCP owners, not periodic sleeps.
    pub idle_wait_calls: u64,
    /// Actual readiness-wait residence, including scheduler delay.
    pub idle_wait_ns: u64,
    pub scheduler_yields: u64,
    pub busy_spin_hints: u64,
    /// Sum/max of loop-start gaps for sweeps first observing a frame's bytes.
    /// This cannot measure arrival-to-poll delay: arrival time is not known.
    pub request_first_poll_gap_ns: u64,
    pub request_first_poll_gap_max_ns: u64,
    pub request_first_poll_samples: u64,
}

impl PollingProfile {
    fn add(&mut self, other: Self) {
        self.progress_sweeps += other.progress_sweeps;
        self.idle_wait_calls += other.idle_wait_calls;
        self.idle_wait_ns += other.idle_wait_ns;
        self.scheduler_yields += other.scheduler_yields;
        self.busy_spin_hints += other.busy_spin_hints;
        self.request_first_poll_gap_ns += other.request_first_poll_gap_ns;
        self.request_first_poll_samples += other.request_first_poll_samples;
        self.request_first_poll_gap_max_ns = self
            .request_first_poll_gap_max_ns
            .max(other.request_first_poll_gap_max_ns);
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ConnectionProfile {
    /// Filled on the client after the control response. Its exchange clocks
    /// include server work and overlap other nodes; never sum as wall time.
    pub client: super::TcpReadProfile,
    /// Client transport captured before the profile-control exchange.
    pub client_transport: super::TransportProgressStats,
    pub requests: u64,
    /// Not attributable under v2 multiplexing; retained as zero. Partial-frame
    /// receive and idle intervals are not presented as execution time.
    pub receive_frame_including_idle_ns: u64,
    pub request_decode_ns: u64,
    pub handler_ns: u64,
    /// Encoding time only; excludes the subsequent output queue.
    pub response_encode_ns: u64,
    /// Queue residence through local TLS/provider completion, not peer receipt.
    pub response_send_ns: u64,
    /// Completed read response to its short reader-authority recheck, including
    /// scheduling delay but never waiting for append or persistence ownership.
    pub read_response_recheck_wait_ns: u64,
    /// Authority recheck elapsed time; excludes response encoding.
    pub read_response_recheck_ns: u64,
    /// Measured read responses rechecked, including read errors and fenced replies.
    pub read_response_recheck_samples: u64,
    /// Retained for protocol compatibility; reader-authority rechecks do not defer.
    pub read_response_recheck_deferrals: u64,
    /// Rechecks rejecting unhealthy, recovering or superseded authority.
    pub read_response_recheck_errors: u64,
    pub append_inventory_wait_ns: u64,
    /// Actual inventory-guard residence during append admission; excludes
    /// off-lock preparation and persistence.
    pub append_inventory_residence_ns: u64,
    pub append_decode_ns: u64,
    pub append_group_build_ns: u64,
    pub append_wal_ns: u64,
    pub append_persist_ns: u64,
    pub storage_write: crate::distributed::storage::StorageWriteProfile,
    /// Per-request control/pool, flusher and barrier queue waits, summed once.
    pub control_queue_ns: u64,
    pub control_queue_samples: u64,
    /// Appender and persistence service, nested in handler_ns; excludes queue waits.
    pub control_execution_ns: u64,
    pub control_execution_samples: u64,
    pub socket: SocketProfile,
    /// Server transport snapshot; client-side transport is queried separately.
    pub transport: super::TransportProgressStats,
    pub polling: PollingProfile,
}

impl ConnectionProfile {
    pub(crate) fn add(&mut self, other: Self) {
        self.client.add(other.client);
        self.client_transport.add(other.client_transport);
        self.requests += other.requests;
        self.receive_frame_including_idle_ns += other.receive_frame_including_idle_ns;
        self.request_decode_ns += other.request_decode_ns;
        self.handler_ns += other.handler_ns;
        self.response_encode_ns += other.response_encode_ns;
        self.response_send_ns += other.response_send_ns;
        self.read_response_recheck_wait_ns += other.read_response_recheck_wait_ns;
        self.read_response_recheck_ns += other.read_response_recheck_ns;
        self.read_response_recheck_samples += other.read_response_recheck_samples;
        self.read_response_recheck_deferrals += other.read_response_recheck_deferrals;
        self.read_response_recheck_errors += other.read_response_recheck_errors;
        self.append_inventory_wait_ns += other.append_inventory_wait_ns;
        self.append_inventory_residence_ns += other.append_inventory_residence_ns;
        self.append_decode_ns += other.append_decode_ns;
        self.append_group_build_ns += other.append_group_build_ns;
        self.append_wal_ns += other.append_wal_ns;
        self.append_persist_ns += other.append_persist_ns;
        self.storage_write.add(other.storage_write);
        self.control_queue_ns += other.control_queue_ns;
        self.control_queue_samples += other.control_queue_samples;
        self.control_execution_ns += other.control_execution_ns;
        self.control_execution_samples += other.control_execution_samples;
        self.socket.add(other.socket);
        self.transport.add(other.transport);
        self.polling.add(other.polling);
    }
}
