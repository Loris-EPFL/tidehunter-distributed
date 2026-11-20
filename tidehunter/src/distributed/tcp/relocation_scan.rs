//! Coordinator side of the R1 `RelocationPage` request.
use super::*;
use crate::distributed::storage::RelocationPage;

impl Client {
    /// Metadata of the complete batches of one stream in `[from, end)`. An
    /// empty page with `available_end < end` means the owner has not adopted
    /// the requested bytes yet. The reply is checked against the request: one
    /// partition and generation, ascending addresses and versions inside the
    /// interval, and every put reference bound to its batch.
    pub(crate) fn relocation_page(
        &mut self,
        authority_generation: u64,
        source_generation: u64,
        from: u64,
        end: u64,
        limit: u16,
    ) -> Result<RelocationPage, TcpError> {
        if limit == 0 || from > end || source_generation > authority_generation {
            return Err(TcpError::new("invalid relocation page request"));
        }
        let partition = self.expected.partition;
        match self.request(Request::RelocationPage {
            authority_generation,
            source_generation,
            from,
            end,
            limit,
        })? {
            Response::RelocationPage(page)
                if valid_page(&page, partition, source_generation, from, end) =>
            {
                Ok(*page)
            }
            response => self.wrong_variant(
                "RelocationPage",
                &response,
                "RelocationPage of the requested stream with ascending batches inside the \
                 interval and every put bound to its batch",
            ),
        }
    }
}

/// The owner returns whole groups, so a page can exceed the requested batch
/// count by one group; the frame byte limit bounds it instead. `end` is a
/// coverage bound: a batch starts below it, and a returned cursor is below
/// it (the owner returns none once its cursor reaches `end`).
fn valid_page(page: &RelocationPage, partition: u32, generation: u64, from: u64, end: u64) -> bool {
    let behind = page.available_end < end;
    let mut cursor = from;
    let mut last = None;
    let batches_valid = page.batches.iter().all(|batch| {
        let address = batch.address;
        let Some(batch_end) = address.offset.checked_add(u64::from(address.len)) else {
            return false;
        };
        let ordered = last.is_none_or(|last| last < batch.version);
        last = Some(batch.version);
        let inside = address.offset >= cursor && address.offset < end;
        cursor = batch_end;
        ordered
            && inside
            && address.partition == partition
            && address.segment_generation == generation
            && batch.version.epoch == generation
            && batch.puts.iter().all(|put| {
                let member = put.position.address;
                put.position.batch == batch.version
                    && member.partition == partition
                    && member.segment_generation == generation
                    && member.offset >= address.offset
                    && member
                        .offset
                        .checked_add(u64::from(member.len))
                        .is_some_and(|member_end| member_end <= batch_end)
            })
    });
    batches_valid
        && page.start >= from
        && (!behind || (page.batches.is_empty() && page.next == Some(from)))
        && page
            .next
            .is_none_or(|next| next <= end && (behind || next > from) && next >= cursor)
}
