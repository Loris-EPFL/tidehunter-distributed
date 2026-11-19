//! Coordinator configuration, statistics and error types shared by the remote
//! database and its Sui adapter.

use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::native_index::IndexReadPin;
use crate::distributed::types::{BatchVersion, MutationVersion};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Static placement and bounded resident authority windows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalInventoryConfig {
    pub database: [u8; 16],
    pub partitions: Vec<u32>,
    pub batch_limits: BatchLimits,
    pub group_limits: GroupLimits,
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
    /// Per-log resident receipt/descriptor window, never a lifetime batch ceiling.
    pub max_batches_per_partition: usize,
    /// Includes cold-authority cache and metadata pinned through read completion.
    pub max_reference_metadata_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalDbStats {
    pub admitted_batches: u64,
    pub published_batches: u64,
    pub validation_ns: u64,
    pub dispatch_ns: u64,
    /// Caller wait, not a device-service or fsync measurement.
    pub storage_wait_ns: u64,
    pub publication_ns: u64,
    pub read_resolve_ns: u64,
    /// True only when the cumulative resolution timer was enabled for this sample.
    pub read_resolve_ns_available: bool,
    pub value_read_ns: u64,
}

#[derive(Clone, Debug)]
pub struct LocalCheckpoint {
    pub(crate) database: [u8; 16],
    pub(crate) cut: Option<MutationVersion>,
    /// Holds the index cut pinned for the checkpoint's lifetime.
    pub(crate) _pin: Option<IndexReadPin>,
}

impl LocalCheckpoint {
    pub fn cut(&self) -> Option<MutationVersion> {
        self.cut
    }
}

#[derive(Debug)]
pub struct LocalDbError {
    message: String,
    pub uncertain_version: Option<BatchVersion>,
}

impl LocalDbError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            uncertain_version: None,
        }
    }
    pub(crate) fn message(&self) -> &str {
        &self.message
    }
    pub(crate) fn component(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }
    pub(crate) fn unknown(version: BatchVersion, error: impl fmt::Display) -> Self {
        Self {
            message: format!("outcome requires recovery for {version:?}: {error}"),
            uncertain_version: Some(version),
        }
    }
}

impl fmt::Display for LocalDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for LocalDbError {}
