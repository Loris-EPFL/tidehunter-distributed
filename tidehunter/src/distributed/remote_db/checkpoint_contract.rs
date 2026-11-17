//! Option C contract, authority side (checkpoint option C spec §§6.3, 7.1,
//! 9.2, 10.2, 10.3): the checkpoint mode, the fields of checkpoint metadata
//! format 4 and of the format-5 authority-manifest commit, and the
//! generation-tagged credit token that returns replay-tail and identity
//! credits exactly once.
//!
//! `Exact` is the only active mode. `PerCell` is rejected by
//! [`RemoteDatabaseConfig::validate`] until the per-cell stages pass their
//! joint gate, and a format-5 manifest never opens in `Exact` mode. No code
//! here writes or reads a file.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "option C contract, wired by the per-cell stages")
)]

use super::write_core::core::CheckpointCredits;
use super::*;
use crate::distributed::native_index::{CellRootHeader, PrefixCut};

/// The one checkpoint protocol of a database (spec §10.3). There is no
/// dual-write and no fallback to `Exact` after a per-cell root was adopted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointMode {
    /// Global exact checkpoints: index root 4, checkpoint metadata 2 or 3,
    /// authority manifest 2 to 4.
    #[default]
    Exact,
    /// Per-cell flushes with metadata-only roots: index root 5, checkpoint
    /// metadata 4, authority manifest 5. Inactive in this revision.
    PerCell,
}

impl CheckpointMode {
    pub(super) fn is_exact(&self) -> bool {
        *self == Self::Exact
    }
}

/// Authority manifest format that selects `PerCell`. Formats 2 to 4 are
/// `Exact`; older binaries accept only 2 to 4 and so reject format 5.
pub(super) const PER_CELL_MANIFEST_FORMAT: u32 = 5;
/// Checkpoint metadata (wrapper) format of `PerCell`. Formats 2 (receipt
/// history) and 3 (WAL replay) stay the exact wrappers.
pub(super) const PER_CELL_CHECKPOINT_FORMAT: u32 = 4;

/// Validate the checkpoint tags of a manifest and return its mode. Old and
/// new tags never mix: format 5 carries only the per-cell commit, while a
/// format 2 to 4 manifest carries none of the per-cell fields. During
/// migration the obsolete commit of a format-5 manifest can still be the
/// last exact commit, which remains retained until GC.
pub(super) fn manifest_checkpoint_mode(
    manifest: &AuthorityManifest,
) -> Result<CheckpointMode, LocalDbError> {
    let per_cell = manifest.format == PER_CELL_MANIFEST_FORMAT;
    let consistent = if per_cell {
        manifest.checkpoint_mode == CheckpointMode::PerCell && manifest.checkpoint.is_none()
    } else {
        manifest.checkpoint_mode.is_exact()
            && manifest.cell_checkpoint.is_none()
            && manifest.cell_checkpoint_obsolete.is_none()
    };
    if !consistent {
        return Err(LocalDbError::new(format!(
            "authority manifest format {} has inconsistent checkpoint mode tags",
            manifest.format
        )));
    }
    Ok(manifest.checkpoint_mode)
}

/// The adopted per-cell commit of a format-5 manifest (spec §7.1). It names
/// the wrapper and root by generation and binds both by length and digest.
/// The root file table, not this commit, lists the referenced index files.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct CellCheckpointCommit {
    pub epoch: u64,
    pub generation: u64,
    pub metadata_bytes: u64,
    pub metadata_digest: [u8; 32],
    pub root_bytes: u64,
    pub root_digest: [u8; 32],
}

impl CellCheckpointCommit {
    /// Same output names as the exact checkpoint staging reservation.
    pub(super) fn metadata_name(&self) -> String {
        format!("checkpoint-{}-{}.metadata", self.epoch, self.generation)
    }
    pub(super) fn root_name(&self) -> String {
        format!("index-{}-{}.snapshot", self.epoch, self.generation)
    }
}

/// Recovery protocol tag of a format-4 wrapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum RecoveryKind {
    PerCellCoverageV1,
}

/// Checkpoint metadata format 4 (spec §7.1), bincode with fixed-width
/// integers like formats 2 and 3, bounded by `MAX_CHECKPOINT_METADATA_BYTES`.
/// Stream and retry state are through `F`; owner proofs cover `U`.
#[derive(Serialize, Deserialize)]
pub(super) struct CheckpointImageV4 {
    pub format: u32,
    pub recovery_kind: RecoveryKind,
    pub history_persistence: HistoryPersistence,
    pub database: [u8; 16],
    pub shape_yaml: String,
    pub authority_epoch: u64,
    pub generation: u64,
    pub root_name: String,
    pub root_bytes: u64,
    pub root_digest: [u8; 32],
    /// `T`, `F`, `U` and `H`, equal to the bound root's header.
    pub directory_target: PrefixCut,
    pub replay_floor: PrefixCut,
    pub max_frame_cut: PrefixCut,
    pub history_floor: Option<MutationVersion>,
    /// Next sequence of `authority_epoch` after `F` (0 if `F` is in an older
    /// epoch): the complete-prefix state at `F`, not at `U`.
    pub published_next: u64,
    /// Every retained `(partition, owner epoch)` stream, sorted, unique.
    pub streams: Vec<CellCheckpointStream>,
    /// WAL-replay mode only: the bounded recent retry window ending at `F`.
    pub recent: Vec<RecentReceipt>,
    /// Receipt-history mode only: durable exact-retry history through `F`.
    pub history_root: Option<HistoryPointer>,
    /// The replay bounds recorded for this generation.
    pub replay_limits: ReplayLimits,
    /// Retirement of `(old F, F]` owned by this generation.
    pub retirement: RetirementToken,
}

/// One retained owner stream of a format-4 wrapper.
#[derive(Serialize, Deserialize)]
pub(super) struct CellCheckpointStream {
    pub partition: u32,
    pub epoch: u64,
    /// Fragment boundary at or before the first batch above `F`.
    pub replay_from: u64,
    /// Offset of the last complete batch at or below `F`. Replay skips
    /// through it; a proof end through `U` is never the skip position.
    pub last_offset: Option<u64>,
    /// Owner-proved durable end. It covers every address through `U`.
    pub durable_end: u64,
    /// The sealed history holds no batch above `F`; not merely fully synced.
    pub complete: bool,
    /// Receipt-history mode only: receipt prefix proof and prefix state
    /// through `F`. The proof prefix is also the receipt suffix cursor.
    pub receipt_prefix: Option<CheckpointStream>,
}

impl CheckpointImageV4 {
    /// Structural rules of spec §7.1 for a wrapper bound to `commit` and the
    /// already checked root `header`. Owner proofs and stream histories are
    /// validated against the fenced owners by the loader.
    pub(super) fn check(
        &self,
        commit: &CellCheckpointCommit,
        header: &CellRootHeader,
        config: &RemoteDatabaseConfig,
        shape_yaml: &str,
    ) -> Result<(), LocalDbError> {
        let receipts = config.history_persistence != HistoryPersistence::WalReplay;
        let floor = self.replay_floor;
        if self.format != PER_CELL_CHECKPOINT_FORMAT
            || self.recovery_kind != RecoveryKind::PerCellCoverageV1
            || self.history_persistence != config.history_persistence
            || self.database != config.storage.database
            || self.shape_yaml != shape_yaml
            || self.authority_epoch != commit.epoch
            || self.generation != commit.generation
            || self.root_name != commit.root_name()
            || (self.root_bytes, self.root_digest) != (commit.root_bytes, commit.root_digest)
        {
            return Err(LocalDbError::new(
                "checkpoint wrapper domain, generation or root binding differs",
            ));
        }
        if header.database != self.database
            || header.authority_epoch != self.authority_epoch
            || header.snapshot_generation != self.generation
            || header.directory_target != self.directory_target
            || header.replay_floor != floor
            || header.max_frame_cut != self.max_frame_cut
            || header.history_floor != self.history_floor
        {
            return Err(LocalDbError::new(
                "checkpoint wrapper cuts differ from root",
            ));
        }
        let next = match floor {
            PrefixCut::Through(batch) if batch.epoch == self.authority_epoch => {
                batch.sequence.saturating_add(1)
            }
            _ => 0,
        };
        if self.published_next != next
            || matches!(floor, PrefixCut::Through(batch) if batch.epoch > self.authority_epoch)
        {
            return Err(LocalDbError::new(
                "checkpoint wrapper prefix state is not at F",
            ));
        }
        if self
            .streams
            .windows(2)
            .any(|pair| (pair[0].partition, pair[0].epoch) >= (pair[1].partition, pair[1].epoch))
            || self.streams.iter().any(|stream| {
                !config.storage.partitions.contains(&stream.partition)
                    || stream.epoch > self.authority_epoch
                    || stream.replay_from % config.storage.fragment_bytes != 0
                    || stream.last_offset.is_some_and(|offset| {
                        offset < stream.replay_from || offset >= stream.durable_end
                    })
                    || stream.receipt_prefix.as_ref().map_or(receipts, |prefix| {
                        !receipts
                            || prefix.proof.database != self.database
                            || prefix.proof.partition != stream.partition
                            || prefix.proof.epoch != stream.epoch
                    })
            })
        {
            return Err(LocalDbError::new("invalid checkpoint wrapper stream"));
        }
        if (receipts && !self.recent.is_empty())
            || (!receipts && self.history_root.is_some())
            || self.recent.len() > WAL_REPLAY_WINDOW_BATCHES
            || self
                .recent
                .windows(2)
                .any(|pair| pair[0].version >= pair[1].version)
            || self.recent.iter().any(|receipt| {
                receipt.id.database != self.database || !floor.contains(receipt.version)
            })
        {
            return Err(LocalDbError::new("invalid checkpoint wrapper retry state"));
        }
        self.replay_limits.validate().map_err(LocalDbError::new)?;
        self.retirement.check()?;
        if self.retirement.generation != self.generation || self.retirement.through != floor {
            return Err(LocalDbError::new(
                "checkpoint wrapper retirement is not owned by this generation",
            ));
        }
        Ok(())
    }
}

/// Exact retirement of the ledger prefix `(from, through]` by one adopted
/// generation (spec §6.3, §9.2). Only an adopted root apply produces it;
/// a cell flush never returns replay-tail or identity credit. The format-4
/// wrapper records it, so the manifest commit digest protects it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct RetirementToken {
    pub generation: u64,
    /// The previously adopted `F`, exclusive.
    pub from: PrefixCut,
    /// The adopted `F`, inclusive.
    pub through: PrefixCut,
    /// Real batches in the prefix; aborted sequence gaps hold no WAL batch.
    pub batches: u64,
    /// Validated encoded batch lengths, not index frame bytes.
    pub encoded_bytes: u64,
    /// Charged identities that the token certifies for removal.
    pub identities: u64,
}

impl RetirementToken {
    fn check(&self) -> Result<(), LocalDbError> {
        let unchanged = self.from == self.through;
        if self.from > self.through
            || (unchanged && (self.batches, self.encoded_bytes, self.identities) != (0, 0, 0))
            || ((self.batches == 0) != (self.encoded_bytes == 0))
        {
            return Err(LocalDbError::new(format!(
                "invalid retirement token of generation {}",
                self.generation
            )));
        }
        Ok(())
    }
}

/// Applied retirement state. It makes credit return exactly once per
/// generation and contiguous in `F`. After a restart it starts at the adopted
/// generation and `F`: counters are rebuilt from the root and replay, never
/// from a partly applied token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RetirementLedger {
    pub generation: u64,
    pub floor: PrefixCut,
}

impl RetirementLedger {
    /// Credits to return to Admission, after all serving guards are dropped.
    /// `None`: this generation was already applied; nothing is returned
    /// again. An unchanged `F` returns zero credits.
    pub(super) fn apply(
        &mut self,
        token: &RetirementToken,
    ) -> Result<Option<CheckpointCredits>, LocalDbError> {
        token.check()?;
        if token.generation == self.generation && token.through == self.floor {
            return Ok(None);
        }
        if token.generation <= self.generation || token.from != self.floor {
            return Err(LocalDbError::new(format!(
                "retirement token of generation {} does not follow generation {}",
                token.generation, self.generation
            )));
        }
        let count = |value: u64| {
            usize::try_from(value).map_err(|_| LocalDbError::new("retirement credit overflow"))
        };
        let credits = CheckpointCredits {
            identities: count(token.identities)?,
            replay_batches: count(token.batches)?,
            replay_bytes: count(token.encoded_bytes)?,
        };
        self.generation = token.generation;
        self.floor = token.through;
        Ok(Some(credits))
    }
}

#[cfg(test)]
#[path = "checkpoint_contract_tests.rs"]
mod contract_tests;
