//! Coordinator configuration, statistics and error types shared by the remote
//! database and its Sui adapter.

use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::types::BatchVersion;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io;

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

#[derive(Debug)]
pub struct LocalDbError {
    message: String,
    kind: io::ErrorKind,
    pub uncertain_version: Option<BatchVersion>,
    /// Set only by [`LocalDbError::copies_not_durable`]: a typed marker, so a
    /// caller never classifies the deferral by its kind or message prefix.
    copies_not_durable: bool,
}

impl LocalDbError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self::with_kind(io::ErrorKind::Other, message)
    }

    pub(crate) fn with_kind(kind: io::ErrorKind, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind,
            uncertain_version: None,
            copies_not_durable: false,
        }
    }
    pub(crate) fn kind(&self) -> io::ErrorKind {
        self.kind
    }
    pub(crate) fn message(&self) -> &str {
        &self.message
    }
    pub(crate) fn component(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }

    /// A value read failed twice: the member's file was retired between the
    /// index lookup and the fetch, and the one re-resolve did not find a live
    /// address either. Both causes stay in the message; the kind is fixed so a
    /// caller can tell this apart from corruption without matching prose.
    pub(crate) fn evicted_during_read(
        version: BatchVersion,
        first: impl fmt::Display,
        second: impl fmt::Display,
    ) -> Self {
        // `Other`, not `NotFound`: `NotFound` is the value path's "retry this
        // once" marker, and a typed eviction is terminal.
        Self::with_kind(
            io::ErrorKind::Other,
            format!("value evicted during read: {version:?}: {first}; re-resolve: {second}"),
        )
    }

    /// True for [`Self::evicted_during_read`].
    #[cfg(test)]
    pub(crate) fn is_evicted_during_read(&self) -> bool {
        self.message.starts_with("value evicted during read:")
    }

    /// A relocation pass copied survivors but the durable cut did not cover the
    /// copies in time, so the pass applied nothing. Not a failure of the
    /// database: the copies stay unreferenced and the next pass retries.
    pub(crate) fn copies_not_durable(next: u64, cause: impl fmt::Display) -> Self {
        Self {
            copies_not_durable: true,
            ..Self::with_kind(
                io::ErrorKind::TimedOut,
                format!("relocation copies not durable through sequence {next}: {cause}"),
            )
        }
    }

    /// True for [`Self::copies_not_durable`].
    pub(crate) fn is_copies_not_durable(&self) -> bool {
        self.copies_not_durable
    }

    pub(crate) fn unknown(version: BatchVersion, error: impl fmt::Display) -> Self {
        Self {
            message: format!("outcome requires recovery for {version:?}: {error}"),
            kind: io::ErrorKind::Other,
            uncertain_version: Some(version),
            copies_not_durable: false,
        }
    }
}

impl fmt::Display for LocalDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for LocalDbError {}

impl From<io::Error> for LocalDbError {
    fn from(error: io::Error) -> Self {
        Self::with_kind(error.kind(), error.to_string())
    }
}
