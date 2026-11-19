//! Metadata for direct writes to owner-allocated native WAL frames.
//!
//! These messages are not authorization or durability proofs. The owner must
//! validate them under the partition lock before allocating any bytes.
//!
//! Leased writes acknowledge page-cache completion in the native tier; an owner
//! kernel or power failure can lose its unsynced suffix. Synchronous leased
//! writes acknowledge a successful Data/FUA completion on the final WAL inode,
//! after the lease made its preallocation, name and inventory witness durable.
//! Neither completion implies that the owner's frontier root has caught up.
//! Adoption publishes only APPENDED authority and requests no per-frame sync
//! in either tier. A synchronous owner read beyond the durable prefix syncs the
//! WAL on demand. Extent close requests the fragment-boundary sync; the live
//! root is installed there and at explicit barriers (flush, seal, fence,
//! retirement, shutdown).
//! Synchronous cold record grants require a covering owner barrier on the pinned
//! inode; its scoped proof never advances the adopted or durable prefix.
//! Checkpoint proofs still require the contiguous owner barrier. Recovery scans
//! the complete valid prefix beyond the root: it cannot
//! distinguish corruption of acknowledged data there from a torn tail. A live
//! coordinator must fail stop on a receipt mismatch or adopted-prefix shortfall.
//! Alignment is not a power-fail atomicity guarantee.

use crate::distributed::group::{CompleteGroup, GroupError};
use crate::distributed::storage::AppendedBatch;
use crate::distributed::types::{BatchId, BatchVersion, PayloadDigest};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// WAL files per permanent export subsystem: one subsystem per owner covers
/// 1024 files of an epoch, one namespace each.
pub const FILES_PER_SUBSYSTEM: u64 = 1024;

/// Deterministic active-epoch subsystem and namespace identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalExportName {
    pub subsystem: u64,
    pub subsystem_id: [u8; 16],
    pub nqn: String,
    pub nsid: u32,
    pub uuid: [u8; 16],
}

pub fn wal_export_name(
    files_per_subsystem: u64,
    database: [u8; 16],
    partition: u32,
    epoch: u64,
    file_index: u64,
) -> WalExportName {
    let subsystem = file_index / files_per_subsystem;
    let digest = |domain: &[u8], index: u64| {
        let mut hash = blake2b_simd::Params::new().hash_length(16).to_state();
        hash.update(domain);
        hash.update(&database);
        hash.update(&partition.to_be_bytes());
        hash.update(&epoch.to_be_bytes());
        hash.update(&index.to_be_bytes());
        let mut id = [0; 16];
        id.copy_from_slice(hash.finalize().as_bytes());
        id
    };
    let subsystem_id = digest(b"tidehunter-live-sub-v1", subsystem);
    let id = subsystem_id;
    let text = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        id[0],
        id[1],
        id[2],
        id[3],
        id[4],
        id[5],
        id[6],
        id[7],
        id[8],
        id[9],
        id[10],
        id[11],
        id[12],
        id[13],
        id[14],
        id[15]
    );
    WalExportName {
        subsystem,
        subsystem_id,
        nqn: format!("nqn.2026-01.local.tidehunter:sub-{text}"),
        nsid: (file_index % files_per_subsystem) as u32 + 1,
        uuid: digest(b"tidehunter-live-wal-v1", file_index),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveBatchSummary {
    pub id: BatchId,
    pub version: BatchVersion,
    pub digest: PayloadDigest,
    pub encoded_len: u32,
    pub descriptor_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveGroupDescriptor {
    /// Complete native outer CRC frame size, including canonical padding.
    pub frame_len: u32,
    /// Outer frame CRC over the padded group payload.
    pub frame_crc: u32,
    pub batches: Vec<LiveBatchSummary>,
}

impl LiveGroupDescriptor {
    /// Construct metadata and exact bytes together so their CRC cannot diverge.
    pub fn from_group(group: &CompleteGroup) -> Result<(Self, Vec<u8>), GroupError> {
        let frame = group.live_frame()?;
        let batches = group
            .batches()
            .iter()
            .map(|batch| {
                Ok(LiveBatchSummary {
                    id: batch.id(),
                    version: batch.version(),
                    digest: batch.digest(),
                    encoded_len: u32::try_from(batch.encoded().len())
                        .map_err(|_| GroupError::TooLarge)?,
                    descriptor_count: u32::try_from(batch.record_descriptors().len())
                        .map_err(|_| GroupError::TooLarge)?,
                })
            })
            .collect::<Result<Vec<_>, GroupError>>()?;
        let descriptor = Self {
            frame_len: u32::try_from(frame.len()).map_err(|_| GroupError::TooLarge)?,
            frame_crc: u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]),
            batches,
        };
        Ok((descriptor, frame))
    }
}

/// Exact owner-derived WAL file identity. The file must remain pinned while
/// any target namespace or in-flight I/O can refer to it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveWalSegment {
    pub database: [u8; 16],
    pub partition: u32,
    pub epoch: u64,
    pub generation: u64,
    pub file_index: u64,
    pub path: PathBuf,
    pub bytes: u64,
    pub device: u64,
    pub inode: u64,
    pub block_size: u32,
}

/// One live namespace exposing one owner WAL file. Returned by the owner for
/// leases, record resolution and replay; also persisted in the live target
/// registry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTargetBinding {
    pub nqn: String,
    pub uuid: [u8; 16],
    pub nsid: u32,
    pub bytes: u64,
    pub block_size: u32,
    pub address: String,
    pub service: u16,
    pub transport: super::NvmeofTransport,
    pub readonly: bool,
    #[serde(default)]
    pub permanent: bool,
}

/// Owner-local authority and inode pin. Keep it until namespace and read
/// completions retire; it is not a transport message.
pub struct LiveRecordLease {
    pub segment: LiveWalSegment,
    pub offset: u64,
    pub len: u32,
    pub(crate) _plan: crate::distributed::storage::RecordReadPlan,
}

impl Drop for LiveRecordLease {
    fn drop(&mut self) {
        self._plan._log.release_live_read_pin();
    }
}

/// One fragment remainder granted to a single writer. Offsets are global WAL
/// offsets; the final inode remains pinned until close or fenced recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveWriteExtent {
    pub token: [u8; 16],
    pub segment: LiveWalSegment,
    pub start: u64,
    pub end: u64,
}

/// Cumulative adoption proof for one extent, not a persisted-root proof.
/// Summaries include opportunistically adopted frames. Compare the cumulative
/// count and digest at `adopted_end`, not merely at the requested lower bound.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveAdoption {
    pub token: [u8; 16],
    pub adopted_end: u64,
    pub receipt_count: u64,
    pub receipt_digest: [u8; 32],
    pub closed: bool,
}

/// Extend the bounded I5 proof in physical receipt order. The fixed-width
/// fields below are exactly AppendedBatch's little-endian bincode encoding.
pub fn extend_live_receipt_digest(previous: [u8; 32], receipt: AppendedBatch) -> [u8; 32] {
    use blake2::{Blake2b, Digest, digest::consts::U32};
    let mut hash = Blake2b::<U32>::new();
    hash.update(b"tidehunter-live-extent-receipt-v1");
    hash.update(previous);
    hash.update(receipt.id().database);
    hash.update(receipt.id().operation);
    hash.update(receipt.version().epoch.to_le_bytes());
    hash.update(receipt.version().sequence.to_le_bytes());
    hash.update(receipt.digest().0);
    hash.update(receipt.address().partition.to_le_bytes());
    hash.update(receipt.address().segment_generation.to_le_bytes());
    hash.update(receipt.address().offset.to_le_bytes());
    hash.update(receipt.address().len.to_le_bytes());
    hash.finalize().into()
}

#[cfg(test)]
mod export_name_tests {
    use super::*;

    #[test]
    fn wal_export_names_are_stable_and_cover_subsystem_boundaries() {
        for (file, subsystem, nsid) in [(0, 0, 1), (1, 0, 2), (1023, 0, 1024), (1024, 1, 1)] {
            let name = wal_export_name(1024, [1; 16], 2, 3, file);
            assert_eq!((name.subsystem, name.nsid), (subsystem, nsid));
            assert_eq!(name, wal_export_name(1024, [1; 16], 2, 3, file));
            let one = wal_export_name(1, [1; 16], 2, 3, file);
            assert_eq!((one.subsystem, one.nsid), (file, 1));
        }
        let base = wal_export_name(1024, [1; 16], 2, 3, 0);
        assert_eq!(base.nqn, wal_export_name(1024, [1; 16], 2, 3, 1).nqn);
        for changed in [
            wal_export_name(1024, [2; 16], 2, 3, 0),
            wal_export_name(1024, [1; 16], 4, 3, 0),
            wal_export_name(1024, [1; 16], 2, 4, 0),
            wal_export_name(1024, [1; 16], 2, 3, 1),
        ] {
            assert_ne!(base.uuid, changed.uuid);
        }
        assert_ne!(base.nqn, wal_export_name(1024, [1; 16], 2, 3, 1024).nqn);
    }
}
