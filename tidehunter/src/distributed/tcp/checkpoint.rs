//! WAL position proofs and retained-file retirement over the authenticated
//! transport. A position proof is not a lease or permission to reclaim WAL
//! segments.

use super::*;
use crate::distributed::storage::{RetireProof, RetirementOutcome, RetirementWitness};

/// Synced WAL position bound to one owner stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPositionProof {
    pub database: [u8; 16],
    pub partition: u32,
    pub epoch: u64,
    pub durable_end: u64,
}

/// A busy controller or owner defers maintenance, but is a refusal for a
/// direct caller. `Busy` means no durable retirement decision was made.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetirementResult {
    Completed,
    Busy,
}

impl Client {
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
        if self.evict_retirement_files(epoch, &files)? == RetirementResult::Busy {
            return Ok(RetirementResult::Busy);
        }
        let request = Request::RetireWalFiles {
            authority_epoch,
            epoch,
            files: files
                .into_iter()
                .map(|file_start| RetireProof {
                    file_start,
                    inode: None,
                    seal_generation: None,
                })
                .collect(),
            witness: None,
        };
        self.retirement_reply(request)
    }

    /// Retire durably sealed files of the serving stream (`epoch ==
    /// authority_epoch`). Every proof names the exact inode and seal; the
    /// witness comes from one adopted Exact root. The coordinator proved the
    /// files unreferenced by every retained root and view; a coordinator read
    /// holds its index pin until its terminal completion, so no read of a
    /// proved file is in flight. The owner drains its own target I/O.
    pub(crate) fn retire_sealed_files(
        &mut self,
        authority_epoch: u64,
        proofs: Vec<RetireProof>,
        witness: RetirementWitness,
    ) -> Result<RetirementResult, TcpError> {
        let bytes = self.expected.wal_file_bytes;
        if proofs.is_empty()
            || proofs.len() > 256
            || proofs
                .windows(2)
                .any(|pair| pair[0].file_start >= pair[1].file_start)
            || proofs.iter().any(|proof| {
                proof.file_start % bytes != 0
                    || proof.inode.is_none()
                    || proof.seal_generation.is_none()
            })
        {
            return Err(TcpError::new("invalid sealed WAL retirement decision"));
        }
        let files: Vec<u64> = proofs.iter().map(|proof| proof.file_start).collect();
        if self.evict_retirement_files(authority_epoch, &files)? == RetirementResult::Busy {
            return Ok(RetirementResult::Busy);
        }
        self.retirement_reply(Request::RetireWalFiles {
            authority_epoch,
            epoch: authority_epoch,
            files: proofs,
            witness: Some(witness),
        })
    }

    /// Disconnect only pinned controllers of the exact files; busy ones defer.
    fn evict_retirement_files(
        &self,
        epoch: u64,
        files: &[u64],
    ) -> Result<RetirementResult, TcpError> {
        let Some(pool) = &self.live_pool else {
            return Ok(RetirementResult::Completed);
        };
        pool.evict_files(
            self.expected.partition,
            epoch,
            self.expected.wal_file_bytes,
            files,
            Instant::now() + self.timeout,
        )
        .map_err(TcpError::component)
    }

    fn retirement_reply(&mut self, request: Request) -> Result<RetirementResult, TcpError> {
        match self.request(request)? {
            Response::Retired(RetirementOutcome::Completed) => Ok(RetirementResult::Completed),
            Response::Retired(RetirementOutcome::Busy) => Ok(RetirementResult::Busy),
            response => self.wrong_variant("RetireWalFiles", &response, "Retired"),
        }
    }
}
