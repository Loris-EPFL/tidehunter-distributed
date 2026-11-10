//! Metadata for direct writes to owner-allocated native WAL frames.
//!
//! These messages are not authorization or durability proofs. The owner must
//! validate them under the partition lock before allocating any bytes.

use crate::distributed::group::{CompleteGroup, GroupError};
use crate::distributed::storage::AppendedBatch;
use crate::distributed::types::{BatchId, BatchVersion, PayloadDigest};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LiveWriteReservation {
    pub token: [u8; 16],
    pub segment: LiveWalSegment,
    /// Global native WAL offset; subtract file_index * segment.bytes for I/O.
    pub frame_offset: u64,
    pub frame_len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LiveReserveResult {
    Existing(Vec<AppendedBatch>),
    Reserved(LiveWriteReservation),
}
