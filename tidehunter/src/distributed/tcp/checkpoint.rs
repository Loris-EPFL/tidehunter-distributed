//! WAL position proofs and retained-file retirement over the authenticated
//! transport. A position proof is not a lease or permission to reclaim WAL
//! segments.

use super::*;

/// Synced WAL position bound to one owner stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPositionProof {
    pub database: [u8; 16],
    pub partition: u32,
    pub epoch: u64,
    pub durable_end: u64,
}

/// A busy controller defers maintenance, but is a refusal for a direct caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetirementResult {
    Completed,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    Busy,
}

impl Client {
    pub(crate) fn position_deadline(&self) -> Instant {
        Instant::now() + self.timeout
    }

    pub fn checkpoint_position(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        through: u64,
    ) -> Result<CheckpointPositionProof, TcpError> {
        let response = self.exchange_position(&Request::CheckpointPosition {
            authority_epoch,
            epoch,
            through,
        })?;
        self.validate_checkpoint_position_response(response, epoch, through)
    }

    pub(crate) fn submit_checkpoint_position(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        through: u64,
        deadline: Instant,
    ) -> Result<bool, TcpError> {
        self.submit_position(
            &Request::CheckpointPosition {
                authority_epoch,
                epoch,
                through,
            },
            deadline,
        )
    }

    pub(crate) fn take_checkpoint_position(
        &mut self,
        epoch: u64,
        through: u64,
    ) -> Option<Result<CheckpointPositionProof, TcpError>> {
        self.take_position()
            .map(|response| self.validate_checkpoint_position_response(response?, epoch, through))
    }

    fn validate_checkpoint_position_response(
        &mut self,
        response: Response,
        epoch: u64,
        through: u64,
    ) -> Result<CheckpointPositionProof, TcpError> {
        match response {
            Response::CheckpointPosition {
                database,
                partition,
                epoch: source,
                durable_end,
            } if database == self.expected.inventory.database
                && partition == self.expected.partition
                && source == epoch
                && durable_end >= through =>
            {
                Ok(CheckpointPositionProof {
                    database,
                    partition,
                    epoch,
                    durable_end,
                })
            }
            response => self.wrong_variant("CheckpointPosition", &response, "CheckpointPosition with matching database, partition and epoch, and durable_end >= through"),
        }
    }

    pub fn retirement_files(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<u64>, TcpError> {
        if limit == 0 || limit > 256 || epoch >= authority_epoch {
            return Err(TcpError::new("invalid retained WAL file page"));
        }
        match self.request(Request::RetirementFiles {
            authority_epoch,
            epoch,
            after,
            limit,
        })? {
            Response::RetirementFiles(files)
                if files.len() <= limit
                    && files.windows(2).all(|pair| pair[0] < pair[1])
                    && files.iter().all(|offset| {
                        offset % self.expected.wal_file_bytes == 0
                            && after.is_none_or(|after| *offset > after)
                    }) =>
            {
                Ok(files)
            }
            response => self.wrong_variant("RetirementFiles", &response, "RetirementFiles within the page limit with strictly increasing, file-aligned offsets after the cursor"),
        }
    }

    pub fn retire_wal_files(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        files: Vec<u64>,
    ) -> Result<(), TcpError> {
        match self.retire_files_if_idle(authority_epoch, epoch, files)? {
            RetirementResult::Completed => Ok(()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            RetirementResult::Busy => Err(TcpError::new("retirement controller is in use")),
        }
    }

    pub(crate) fn retire_files_if_idle(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        files: Vec<u64>,
    ) -> Result<RetirementResult, TcpError> {
        if files.len() > 256
            || epoch >= authority_epoch
            || files.windows(2).any(|pair| pair[0] >= pair[1])
            || files
                .iter()
                .any(|offset| offset % self.expected.wal_file_bytes != 0)
        {
            return Err(TcpError::new("invalid retained WAL retirement decision"));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(pool) = &self.live_pool
            && pool
                .evict_files(
                    self.expected.partition,
                    epoch,
                    self.expected.wal_file_bytes,
                    &files,
                    Instant::now() + self.timeout,
                )
                .map_err(TcpError::component)?
                == RetirementResult::Busy
        {
            return Ok(RetirementResult::Busy);
        }
        let request = Request::RetireWalFiles {
            authority_epoch,
            epoch,
            files,
        };
        match self.request(request)? {
            Response::Retired => Ok(RetirementResult::Completed),
            response => self.wrong_variant("RetireWalFiles", &response, "Retired"),
        }
    }
}
