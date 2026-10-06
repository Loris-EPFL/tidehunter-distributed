//! Experimental building blocks for a physically sharded, authoritative Value WAL.
//!
//! This module is opt-in and does not replace [`crate::db::Db`]. Physical addresses
//! are deliberately separate from logical mutation versions. See
//! `docs/distributed/distributed_tidehunter_architecture_decision_2026-10-06.md`
//! for the recovery and publication contract, and the implementation record for
//! the supported subset. No wire format or disk-format stability is promised yet.

pub mod codec;
pub mod sequence;
pub mod storage;
pub mod types;

#[cfg(test)]
mod model;
