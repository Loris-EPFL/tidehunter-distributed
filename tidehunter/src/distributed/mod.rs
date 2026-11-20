//! Experimental building blocks for a physically sharded, authoritative Value WAL.
//!
//! This module is opt-in and does not replace [`crate::db::Db`]. Physical addresses
//! are deliberately separate from logical mutation versions. See
//! `docs/distributed/ARCHITECTURE.md` for the leased NVMe-oF design: the
//! recovery and publication contract and the supported subset. No wire format
//! or disk-format stability is promised yet.

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

pub mod capacity;
pub mod codec;
pub mod durability;
pub mod group;
pub mod index_reference;
pub mod native_index;
pub mod native_publication;
pub mod nvmeof;
pub mod owner_events;
pub mod r1_events;
pub mod reference_directory;
pub mod remote_db;
pub mod sequence;
pub mod shard_lock;
pub mod storage;
pub mod sui;
pub(crate) mod sys;
pub mod tcp;
pub mod types;
pub mod write_timeline;

#[cfg(test)]
mod group_model;
#[cfg(test)]
mod model;
#[cfg(test)]
pub(crate) mod test_bounds;
