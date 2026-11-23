//! Coordinator-resident native index for the hybrid design.
//!
//! The hybrid keeps the remote, multi-owner value WAL (leased NVMe-oF owner
//! frames) and runs the native single-node index stack in the coordinator:
//! `LargeTable`, `IndexFlusher`, the coordinator's local index WAL, and the
//! native control region and snapshots. This module owns the pieces that are
//! new because the index is remote:
//!
//! * [`position`] — the 48-byte version-carrying [`position::VersionPosition`]
//!   and its ordering/flat codec, the instantiation that makes the native
//!   index types generic without changing native bytes.
//! * [`loader`] — the cut fronts (durable, index-applied, published,
//!   control-region floor, held latches), the per-stream allocation
//!   watermarks, the monotone value-byte counter, and the hybrid
//!   [`crate::large_table::Loader`] over the coordinator's index WAL.
//! * [`region`] — control-region V4 (`ControlRegion<BatchVersion>` plus the
//!   per-stream replay table and per-cell byte positions) and the pure
//!   byte-cadence arithmetic.
//! * [`config`] — the native index configuration with Sui's values.
//!
//! `table` and `read` assemble these into the serving path in later slices
//! and are declared by their slices; `replay` and `snapshot` (S5) are declared
//! here because S4 owns the module root.

pub(crate) mod config;
pub(crate) mod loader;
pub(crate) mod position;
pub(crate) mod read;
pub(crate) mod region;
pub(crate) mod table;

pub use config::HybridIndexConfig;

pub(crate) mod replay;
mod snapshot;

pub(crate) use snapshot::{SnapshotConfig, StoredSnapshot, spawn_snapshot_thread};
// The crash-injection boundary is read only by test modules outside `hybrid`.
#[cfg(test)]
pub(crate) use snapshot::{SnapshotBoundary, inject_snapshot_fault};

#[cfg(test)]
mod replay_tests;
#[cfg(test)]
mod snapshot_tests;
