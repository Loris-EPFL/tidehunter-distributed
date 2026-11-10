//! Write-durability tiers of the distributed engine.
//!
//! Two tiers, one mechanism. [`frontier`] holds the only frontier tracker and the only
//! syncer. [`native`] and [`synchronous`] hold the acknowledgement conditions. The default
//! tier is [`DurabilityTier::Native`], the contract of native Tidehunter. The contracts, the
//! failure matrix, and the matched benchmark cohorts are in
//! `docs/distributed/DURABILITY_TIERS.md`.
//!
//! No wire or disk format depends on this module. [`ReceiptDurability`] is an in-memory
//! label.

pub mod frontier;
pub mod native;
pub mod synchronous;

#[cfg(test)]
mod tests;

use frontier::{Frontier, FrontierConfig, FrontierError, SyncPolicy};
use std::str::FromStr;
use std::time::Instant;

/// Write-acknowledgement tier. Comparisons never mix tiers.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityTier {
    /// Acknowledge when the batch and all earlier batches are appended (page cache).
    /// Background fsync; explicit flush. Same contract as native Tidehunter.
    #[default]
    Native,
    /// Acknowledge when the covering fsync is complete.
    Synchronous,
}

impl DurabilityTier {
    /// Stable label for configuration, receipts, and reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Synchronous => "synchronous",
        }
    }

    /// The matched benchmark cohort. Comparison across cohorts is invalid.
    #[must_use]
    pub const fn comparison_cohort(self) -> &'static str {
        match self {
            Self::Native => "native_pagecache",
            Self::Synchronous => "value_wal_barrier_and_valkey_appendfsync_always",
        }
    }

    /// What a receipt of this tier proves about the acknowledged batch.
    #[must_use]
    pub const fn receipt_durability(self) -> ReceiptDurability {
        match self {
            Self::Native => ReceiptDurability::Appended,
            Self::Synchronous => ReceiptDurability::Synced,
        }
    }

    /// Whether the owner may reply as soon as the batch is appended.
    #[must_use]
    pub const fn ack_after_append(self) -> bool {
        match self {
            Self::Native => native::ack_after_append(),
            Self::Synchronous => synchronous::ack_after_append(),
        }
    }

    /// Wait for this tier's acknowledgement condition on a batch ending at `end`.
    pub fn acknowledge(
        self,
        frontier: &Frontier,
        end: u64,
        deadline: Option<Instant>,
    ) -> Result<ReceiptDurability, FrontierError> {
        match self {
            Self::Native => native::acknowledge(frontier, end, deadline),
            Self::Synchronous => synchronous::acknowledge(frontier, end, deadline),
        }
    }
}

impl std::fmt::Display for DurabilityTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The rejected tier name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownTier(pub String);

impl std::fmt::Display for UnknownTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unknown durability tier {:?}; expected \"native\" or \"synchronous\"",
            self.0
        )
    }
}

impl std::error::Error for UnknownTier {}

impl FromStr for DurabilityTier {
    type Err = UnknownTier;

    fn from_str(name: &str) -> Result<Self, UnknownTier> {
        match name {
            "native" => Ok(Self::Native),
            "synchronous" => Ok(Self::Synchronous),
            other => Err(UnknownTier(other.to_owned())),
        }
    }
}

/// What a receipt says about the acknowledged batch. In-memory label only.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReceiptDurability {
    /// In the owner page cache, with every earlier batch. Survives an owner process crash.
    Appended,
    /// Covered by a completed fsync. Survives an owner kernel crash.
    Synced,
}

impl ReceiptDurability {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Appended => "appended",
            Self::Synced => "synced",
        }
    }
}

/// Engine configuration of the durability layer. Every field has the native default.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DurabilityConfig {
    pub tier: DurabilityTier,
    /// Opt-in: also sync once the oldest unsynced byte is this old (milliseconds). `None`
    /// (default) is native Tidehunter: sync only when a WAL fragment is finalized, or on
    /// demand (flush, Synchronous acknowledgement, checkpoint capture).
    pub max_unsynced_age_ms: Option<u64>,
    /// Capacity of each owner's out-of-order completion buffer.
    pub max_pending_completions: usize,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            tier: DurabilityTier::Native,
            max_unsynced_age_ms: None,
            max_pending_completions: FrontierConfig::DEFAULT_MAX_PENDING,
        }
    }
}

impl DurabilityConfig {
    #[must_use]
    pub fn with_tier(tier: DurabilityTier) -> Self {
        Self {
            tier,
            ..Self::default()
        }
    }

    /// Frontier parameters for one owner WAL whose durable end is `start` and whose
    /// fragment size is `fragment_bytes` (the owner storage configuration).
    #[must_use]
    pub fn frontier_config(&self, start: u64, fragment_bytes: u64) -> FrontierConfig {
        FrontierConfig {
            start,
            max_pending: self.max_pending_completions,
            policy: SyncPolicy {
                fragment_bytes,
                max_unsynced_age_ms: self.max_unsynced_age_ms,
            },
        }
    }
}
