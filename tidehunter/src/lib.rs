#[cfg(feature = "experimental-distributed")]
mod background_health;
pub mod batch;
mod cell;
pub mod checkpoint;
#[doc(hidden)] // Used by tools/tideconsole for WAL compression estimation
pub mod compressed_batch;
pub mod config;
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub mod container;
mod context;
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub mod control;
#[doc(hidden)] // Used by benchmarks and tools for WAL writes
pub mod crc;
pub mod db;
#[cfg(feature = "experimental-distributed")]
pub mod distributed;
#[cfg(test)]
mod failpoints;
pub mod file_reader;
mod flusher;
pub mod index;
mod index_blob_cache;
pub mod iterators;
pub mod key_shape;
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub mod large_table;
mod lock;
pub mod lookup;
mod math;
pub mod metrics;
#[cfg(feature = "experimental-distributed")]
pub mod parallelism;
mod position_cache;
mod primitives;
// The distributed owner protocol carries the evidence structs in every build.
#[cfg(any(
    feature = "experimental-distributed",
    all(target_os = "linux", feature = "test-utils")
))]
pub mod read_probe;
mod relocation;
mod replay_buffer;
mod runtime;
mod state_snapshot;
mod thread_util;
mod wal_replay;

pub(crate) mod wal;

mod latch;

// todo remove re-export
pub use minibytes;

pub use index::index_table::TaggedWalPosition as IndexWalPosition;
pub use relocation::{Decision, RelocationStrategy, compute_target_position_from_ratio};
#[cfg(feature = "test_methods")]
pub use relocation::{
    RelocationCallback, RelocationGate, RelocationObservation, RelocationStreamId,
    RelocationTestError, RelocationWorkerWitness, StreamObservation,
};

#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub use cell::CellId;

// WAL re-exports
#[doc(hidden)] // Used by tools and benchmarks
pub use wal::layout::{WalKind, WalLayout};
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub use wal::position::LastProcessed;
pub use wal::position::WalPosition;
#[doc(hidden)] // Used by benchmarks
pub use wal::{PreparedWalWrite, Wal, WalWriter};

#[doc(hidden)] // Used by benchmarks
pub mod wal_allocator {
    pub use crate::wal::allocator::WalAllocator;
}

#[cfg(feature = "test-utils")]
pub mod test_utils {
    pub use crate::db::WalEntry;
    pub use crate::index::index_format::IndexFormat;
    pub use crate::index::index_table::IndexTable;
    pub use crate::metrics::Metrics;
    pub use crate::wal::files::list_wal_files_with_sizes;
    pub use crate::wal::layout::WalLayout;
    pub use crate::wal::{Wal, WalError, WalIterator};
}
