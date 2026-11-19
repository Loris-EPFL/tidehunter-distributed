//! Coordinator side of the read-only R0 `FileInventoryPage` request.
use super::*;
use crate::distributed::capacity::{FileInventoryReply, MAX_FILE_INVENTORY_PAGE};

impl Client {
    /// One sorted owner WAL file page of `source_generation`, strictly after
    /// `after`. Read-only: same authentication as retirement, no side effects.
    pub(crate) fn file_inventory_page(
        &mut self,
        authority_generation: u64,
        source_generation: u64,
        after: Option<u64>,
        limit: u16,
    ) -> Result<FileInventoryReply, TcpError> {
        if limit == 0 || limit > MAX_FILE_INVENTORY_PAGE || source_generation > authority_generation
        {
            return Err(TcpError::new("invalid file inventory page request"));
        }
        let bytes = self.expected.wal_file_bytes;
        let partition = self.expected.partition;
        match self.request(Request::FileInventoryPage {
            authority_generation,
            source_generation,
            after,
            limit,
        })? {
            Response::FileInventoryPage(reply)
                if reply.partition == partition
                    && reply.source_generation == source_generation
                    && reply.rows.len() <= usize::from(limit)
                    && reply
                        .rows
                        .windows(2)
                        .all(|pair| pair[0].file_start < pair[1].file_start)
                    && reply.rows.iter().all(|row| {
                        row.file_start % bytes == 0
                            && after.is_none_or(|after| row.file_start > after)
                            && row.logical_bytes <= bytes
                            && row.file_start.checked_add(bytes).is_some_and(|end| {
                                row.validated_through.is_none_or(|through| through <= end)
                                    && row.seal.is_none_or(|seal| {
                                        seal.start == row.file_start
                                            && seal.end == end
                                            && seal.device == row.device
                                            && seal.inode == row.inode
                                            && reply
                                                .wal_synced_end
                                                .is_some_and(|synced| end <= synced)
                                    })
                            })
                    })
                    && reply
                        .wal_scan_from
                        .zip(reply.wal_synced_end)
                        .is_none_or(|(scan, synced)| scan <= synced) =>
            {
                Ok(*reply)
            }
            response => self.wrong_variant(
                "FileInventoryPage",
                &response,
                "FileInventoryPage for the requested partition and generation, within the page \
                 limit, with strictly increasing file-aligned starts after the cursor",
            ),
        }
    }
}
