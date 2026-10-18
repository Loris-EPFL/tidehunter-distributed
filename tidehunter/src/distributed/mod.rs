//! Experimental building blocks for a physically sharded, authoritative Value WAL.
//!
//! This module is opt-in and does not replace [`crate::db::Db`]. Physical addresses
//! are deliberately separate from logical mutation versions. See
//! `docs/distributed/distributed_tidehunter_architecture_decision_2026-10-06.md`
//! for the recovery and publication contract, and the implementation record for
//! the supported subset. No wire format or disk-format stability is promised yet.

pub mod codec;
pub mod completion;
pub mod group;
pub mod index_reference;
pub mod local_db;
pub mod local_inventory;
pub mod native_index;
pub mod native_publication;
pub mod publication;
pub mod reference_directory;
pub mod remote_db;
pub mod sequence;
pub mod storage;
pub mod sui;
pub mod tcp;
pub mod types;
pub mod worker;

#[cfg(test)]
mod group_model;
#[cfg(test)]
mod model;
