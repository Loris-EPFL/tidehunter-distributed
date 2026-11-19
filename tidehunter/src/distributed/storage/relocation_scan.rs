//! R1 relocation scan (CAPACITY-R0-R1.md §5): one bounded metadata page of
//! original WAL batches of one stream, active or sealed, in file order.
//!
//! The page names every complete batch that starts in `[from, end)`,
//! including batches without a put (they still order Stop against other
//! streams), and every put member with its exact reference and reduced key.
//! Groups are read whole, so the cursor can pass `end`. No payload leaves the
//! owner on this path: the coordinator reads a callback's value through its
//! ordinary pinned member read. Recovery replay stays sealed-only.
//!
//! The scan reads only bytes the owner already adopted (`end` at or below the
//! adopted end), so it never races a leased write; a corrupt group below that
//! end is an error, never an end of history.
use super::wal_replay::FRAME_BOUNDARY;
use super::*;

/// One put member of a scanned batch: its exact reference and reduced key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelocationPut {
    pub reference: super::super::types::ValueRecordReference,
    pub reduced_key: Vec<u8>,
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
                    let reference = batch
                        .record_reference(receipt.address(), index)
                        .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
                    puts.push(RelocationPut {
                        reference,
                        reduced_key: self.shape.ks(*keyspace).reduce_key(key).into_owned(),
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
