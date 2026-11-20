//! Owner file-maintenance counters: seal, retirement and the live grant paths
//! that must never wait for them.

/// Owner file-maintenance counters of this process (R1 seal and retirement,
/// and the live grant paths they must never delay). Changed only under the
/// inventory mutex.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct FileMaintenanceStats {
    /// Files this process sealed (durable `FileSeal` installed).
    pub sealed_files: u64,
    /// Files whose retirement finished unlink, directory sync and cleanup.
    pub retired_files: u64,
    /// Retirement requests that made no durable decision.
    pub busy_retirements: u64,
    /// Finish passes that unlinked nothing although cleanups were pending:
    /// a victim was still the writer's open fragment map (`WouldBlock` from
    /// the mapper). The next pass repeats the work once the next fragment's
    /// first allocation finalizes that map.
    pub deferred_retirements: u64,
    /// Grants answered from an already published binding: no lifecycle gate.
    pub ready_binding_grants: u64,
    /// Grants that took the lifecycle gate although their binding was
    /// published. Must stay zero: a prepared grant never waits for maintenance.
    pub prepared_gate_waits: u64,
    pub prepared_gate_wait_ns: u64,
    /// Grants without a published binding that waited for the gate (the
    /// unprepared residual, as native waits for map preparation).
    pub unprepared_gate_waits: u64,
    pub unprepared_gate_wait_ns: u64,
}
