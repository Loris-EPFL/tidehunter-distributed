//! Local research workloads and explicit distributed-KV comparison contracts.
//! Embedded, application-integrated and service-level results remain distinct.
// This unpublished CLI-support library documents its shared BackendResult and
// failure/unknown-mutation contract once in backend.rs and the comparison protocol.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::undocumented_unsafe_blocks,
    clippy::multiple_unsafe_ops_per_block,
    clippy::mutex_atomic,
    clippy::await_holding_lock,
    clippy::await_holding_refcell_ref,
    clippy::arc_with_non_send_sync,
    unsafe_op_in_unsafe_fn
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the shared BackendResult and failure/unknown-mutation contract is documented once in backend.rs and the comparison protocol"
)]

pub mod backend;
pub mod backends;
pub mod config;
pub mod report;
pub mod runner;
pub mod workload;

pub(crate) fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
pub(crate) mod leased_fixture;
pub mod mainnet_replay;
pub mod mainnet_trace;
pub mod op_replay;
