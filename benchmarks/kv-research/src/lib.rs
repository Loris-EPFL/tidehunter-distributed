//! Local research workloads and explicit distributed-KV comparison contracts.
//! Embedded, application-integrated and service-level results remain distinct.
// This unpublished CLI-support library documents its shared BackendResult and
// failure/unknown-mutation contract once in backend.rs and the comparison protocol.
#![allow(clippy::missing_errors_doc)]

pub mod backend;
pub mod backends;
pub mod capacity_r0;
pub mod config;
pub mod report;
pub mod runner;
pub mod workload;

pub(crate) fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
pub mod mainnet_replay;
pub mod mainnet_trace;
