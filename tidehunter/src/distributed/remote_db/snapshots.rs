//! Snapshot types reported by one shutdown and by one read.

#[derive(Clone, Copy, Debug, Default)]
pub struct ShutdownStats {
    pub drain_ns: u64,
    pub checkpoint_ns: u64,
    pub seal_ns: u64,
    pub join_ns: u64,
}

/// Diagnostic clocks only. Stages are nested and must not be summed with
/// dispatch/wait into a purported non-overlapping latency breakdown.
#[derive(Clone, Copy, Debug, Default)]
pub struct RemoteReadProfile {
    pub index_ns: u64,
    pub dispatch_ns: u64,
    pub wait_ns: u64,
    /// Caller-owned scalar live read, with no read-actor queue/return handoff.
    pub inline_read_ns: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}
