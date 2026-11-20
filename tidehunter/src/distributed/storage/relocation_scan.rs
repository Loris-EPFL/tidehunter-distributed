//! R1 relocation scan (CAPACITY-R0-R1.md §5): one bounded metadata page of
//! original WAL batches of one stream, active or sealed, in file order.
//!
//! The page names every complete batch that starts in `[from, end)`,
//! including batches without a put (they still order Stop against other
//! streams), and every put member with its exact index position and reduced
//! key. Groups are read whole, so the cursor can pass `end`. No payload is
//! copied into the page: the coordinator reads a callback's value at its
//! position through the ordinary position read. Recovery replay stays
//! sealed-only.
//!
//! The position is built by the one constructor publication uses,
//! [`VersionPosition::member`], from the batch address and the member
//! descriptor. Its `payload_crc` here is the CRC-32 of the member payload the
//! owner just read (after the member's `[len u32][checksum u32]` header): it
//! only verifies that transfer. It is never stored. The copy ladder re-points
//! positions taken from the index view and reuses their `payload_crc`, so a
//! corrupt survivor can never launder a matching CRC into the index.
//!
//! The scan reads only bytes the owner already adopted (`end` at or below the
//! adopted end), so it never races a leased write; a corrupt group below that
//! end is an error, never an end of history.
use super::wal_replay::FRAME_BOUNDARY;
use super::*;
use crate::distributed::codec::MEMBER_HEADER_LEN;
use crate::distributed::hybrid::position::VersionPosition;
use minibytes::Bytes;

/// One put member of a scanned batch: its keyspace, index position and reduced
/// key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelocationPut {
    pub keyspace: u8,
    pub position: VersionPosition,
    pub reduced_key: Bytes,
}

/// One original batch in stream order. `puts` keeps member order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelocationBatch {
    pub version: BatchVersion,
    pub address: PhysicalValueAddress,
    pub puts: Vec<RelocationPut>,
}

/// One page of the relocation scan.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelocationPage {
    /// First offset at or after the requested start that is not inside a
    /// retired interval: the stream's oldest unretired position at `from`.
    pub start: u64,
    /// Adopted end of the stream when the page was taken. Below the requested
    /// end the page is empty and the coordinator retries later.
    pub available_end: u64,
    pub batches: Vec<RelocationBatch>,
    /// Next cursor, or `None` once `end` is reached.
    pub next: Option<u64>,
}

impl PartitionLog {
    /// Metadata of the complete batches in `[from, end)`. `end` must not
    /// exceed the adopted end; otherwise the page is empty with the current
    /// `available_end`, a deferral and not an error. Takes `state` only to
    /// copy the root and adopted end; reads and decoding run without it.
    pub fn relocation_page(
        &self,
        from: u64,
        end: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<RelocationPage, StorageError> {
        let (available_end, root) = {
            let state = self.state.lock();
            if state.poison.is_some() {
                return Err(StorageError::Poisoned);
            }
            (state.appended_end, state.wal_root.clone())
        };
        let mut start = from;
        while start < end {
            let file_start = start / self.config().wal_file_bytes * self.config().wal_file_bytes;
            // The WAL root is the only retirement record; no root, none retired.
            let skip = root
                .as_deref()
                .and_then(|root| root.retired.skip_end(file_start));
            match skip {
                Some(skip) => start = skip.min(end),
                None => break,
            }
        }
        if end > available_end {
            return Ok(RelocationPage {
                start,
                available_end,
                batches: Vec::new(),
                next: Some(from),
            });
        }
        // `end` is a coverage requirement, not a frame bound: a receipt end
        // can lie inside its group frame. Read every group that starts below
        // `end` whole, up to the adopted end, and name only its batches that
        // start below `end`.
        let page = self.replay_groups(
            from..end,
            available_end,
            root.as_deref(),
            None,
            limit,
            max_bytes,
        )?;
        let mut batches = Vec::with_capacity(page.groups.iter().map(|g| g.receipts.len()).sum());
        for decoded in &page.groups {
            for (batch, receipt) in decoded.group.batches().iter().zip(&decoded.receipts) {
                if receipt.address().offset >= end {
                    break;
                }
                let mut puts = Vec::new();
                for (index, mutation) in batch.mutations().iter().enumerate() {
                    let Mutation::Put { keyspace, key, .. } = mutation else {
                        continue;
                    };
                    let descriptor = batch
                        .record_descriptors()
                        .get(index)
                        .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
                    // The member is `[len u32][checksum u32]` plus its native
                    // entry; the entry starts after that member header. The
                    // batch's own outer `CrcFrame` header precedes the member
                    // area and is already included in `descriptor.offset`.
                    // The position's CRC here verifies this transfer only: the
                    // copy ladder reuses the indexed `payload_crc`, so no CRC
                    // read from owner bytes ever reaches the index.
                    let member_start = usize::try_from(descriptor.offset)
                        .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
                    let member_end = member_start
                        .checked_add(
                            usize::try_from(descriptor.len)
                                .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?,
                        )
                        .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
                    let payload = batch
                        .encoded()
                        .get(member_start + MEMBER_HEADER_LEN..member_end)
                        .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
                    let position = VersionPosition::member(
                        batch.version(),
                        receipt.address(),
                        descriptor.operation_index,
                        *descriptor,
                        crc32fast::hash(payload),
                    );
                    puts.push(RelocationPut {
                        keyspace: keyspace.0,
                        position,
                        reduced_key: Bytes::from(
                            self.shape.ks(*keyspace).reduce_key(key).into_owned(),
                        ),
                    });
                }
                batches.push(RelocationBatch {
                    version: batch.version(),
                    address: receipt.address(),
                    puts,
                });
            }
        }
        Ok(RelocationPage {
            start,
            available_end,
            batches,
            next: page.next,
        })
    }
}
