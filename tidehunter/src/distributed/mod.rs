//! Experimental building blocks for a physically sharded, authoritative Value WAL.
//!
//! This module is opt-in and does not replace [`crate::db::Db`]. Physical addresses
//! are deliberately separate from logical mutation versions. See
//! `docs/distributed/ARCHITECTURE.md` for the leased NVMe-oF design: the
//! recovery and publication contract and the supported subset. No wire format
//! or disk-format stability is promised yet.

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
