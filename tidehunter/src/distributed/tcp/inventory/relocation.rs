//! Stream lookup for the R1 relocation scan (CAPACITY-R0-R1.md §5).
use super::*;
use crate::distributed::storage::RelocationPage;

/// Batches per relocation page; one page per owner is the coordinator's
/// whole k-way merge state.
pub(in crate::distributed::tcp) const MAX_RELOCATION_PAGE: u16 = 256;

impl NodeInventory {
    /// Maintenance-lane handler: the inventory mutex only copies the stream
    /// handle; the scan reads and decodes with no owner lock held.
    pub(in crate::distributed::tcp) fn relocation_page(
        inventory: &Mutex<NodeInventory>,
        authority_generation: u64,
        source_generation: u64,
        from: u64,
        end: u64,
        limit: u16,
        limits: TcpLimits,
    ) -> Result<RelocationPage, TcpError> {
        if limit == 0 || limit > MAX_RELOCATION_PAGE || from > end {
            return Err(TcpError::new("relocation page request exceeds bound"));
        }
        if source_generation > authority_generation {
            return Err(TcpError::new("relocation scan of a future generation"));
        }
        let log = inventory
            .lock()
            .relocation_log(authority_generation, source_generation)?;
        match log {
            Some(log) => log
                .relocation_page(
                    from,
                    end,
                    usize::from(limit),
                    limits.max_frame_bytes.saturating_sub(4096),
                )
                .map_err(TcpError::component),
            // An abandoned generation adopted no batch.
            None if end == 0 => Ok(RelocationPage::default()),
            None => Err(TcpError::new(
                "relocation scan beyond an abandoned generation",
            )),
        }
    }

    /// The stream a relocation scan of `epoch` reads under the current
    /// authority: the active stream itself, or an older sealed one. `None`
    /// for an abandoned generation, which holds no adopted batch.
    pub(in crate::distributed::tcp) fn relocation_log(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<Option<Arc<PartitionLog>>, TcpError> {
        let active = self.active(authority_epoch)?;
        if epoch == authority_epoch {
            return Ok(Some(active));
        }
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("relocation scan names an absent fencing generation"))?;
        match record.state {
            EpochState::Abandoned => Ok(None),
            EpochState::Sealed => self
                .logs
                .get(&epoch)
                .cloned()
                .map(Some)
                .ok_or_else(|| TcpError::new("relocation scan stream absent")),
            _ => Err(TcpError::new(
                "relocation scan of an older generation requires a sealed stream",
            )),
        }
    }
}
