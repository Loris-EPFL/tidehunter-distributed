//! Exact retained receipt-prefix evidence over the authenticated transport.
//! A digest is not a lease or permission to reclaim WAL segments.

use super::*;
use crate::distributed::storage::AppendedBatch;

fn metadata_hasher() -> blake2b_simd::State {
    blake2b_simd::Params::new().hash_length(32).to_state()
}

fn finish_metadata_hash(digest: blake2b_simd::State) -> [u8; 32] {
    let hash = digest.finalize();
    let mut bytes = [0; 32];
    bytes.copy_from_slice(hash.as_bytes());
    bytes
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointProof {
    pub database: [u8; 16],
    pub partition: u32,
    pub epoch: u64,
    pub prefix: usize,
    pub retained: usize,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct CheckpointReceipt {
    pub id: BatchId,
    pub version: BatchVersion,
    pub digest: PayloadDigest,
    pub address: PhysicalValueAddress,
}

impl From<AppendedBatch> for CheckpointReceipt {
    fn from(receipt: AppendedBatch) -> Self {
        Self {
            id: receipt.id(),
            version: receipt.version(),
            digest: receipt.digest(),
            address: receipt.address(),
        }
    }
}

impl CheckpointReceipt {
    /// Caller must first compare the entire ordered prefix to a fresh proof
    /// from the fenced, authenticated owning shard. Local metadata alone is not
    /// a durability receipt.
    pub(crate) fn authenticated(self) -> AppendedBatch {
        DurableBatch::from_authenticated_parts(self.id, self.version, self.digest, self.address)
            .appended()
    }
}

pub(crate) fn prefix_digest(
    database: [u8; 16],
    partition: u32,
    epoch: u64,
    receipts: impl ExactSizeIterator<Item = CheckpointReceipt>,
) -> [u8; 32] {
    let mut digest = PrefixDigest::new(database, partition, epoch, 0);
    for receipt in receipts {
        digest.push(receipt);
    }
    digest.finish()
}

/// Incrementally extensible exact-prefix evidence (transport protocol 8).
/// Rooted page states permit O(1) old-prefix proofs without scanning or keeping
/// completed receipt history resident. The final digest also binds the count.
pub(crate) struct PrefixDigest {
    state: [u8; 32],
    count: usize,
}

impl PrefixDigest {
    pub(crate) fn new(database: [u8; 16], partition: u32, epoch: u64, count: usize) -> Self {
        let mut digest = metadata_hasher();
        digest.update(b"tidehunter-checkpoint-genesis-v2");
        digest.update(&database);
        digest.update(&partition.to_be_bytes());
        digest.update(&epoch.to_be_bytes());
        Self {
            state: finish_metadata_hash(digest),
            count,
        }
    }

    pub(crate) fn push(&mut self, receipt: CheckpointReceipt) {
        let mut digest = metadata_hasher();
        digest.update(b"tidehunter-checkpoint-chain-v2");
        digest.update(&self.state);
        digest.update(&receipt.id.database);
        digest.update(&receipt.id.operation);
        digest.update(&receipt.version.epoch.to_be_bytes());
        digest.update(&receipt.version.sequence.to_be_bytes());
        digest.update(&receipt.digest.0);
        digest.update(&receipt.address.partition.to_be_bytes());
        digest.update(&receipt.address.segment_generation.to_be_bytes());
        digest.update(&receipt.address.offset.to_be_bytes());
        digest.update(&receipt.address.len.to_be_bytes());
        self.state = finish_metadata_hash(digest);
        // Receipt owners check count admission/exhaustion before appending.
        self.count += 1;
    }

    pub(crate) fn from_state(count: usize, state: [u8; 32]) -> Self {
        Self { count, state }
    }

    pub(crate) fn state(&self) -> [u8; 32] {
        self.state
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        let mut digest = metadata_hasher();
        digest.update(b"tidehunter-checkpoint-prefix-v2");
        digest.update(&(self.count as u64).to_be_bytes());
        digest.update(&self.state);
        finish_metadata_hash(digest)
    }
}

impl Client {
    pub fn checkpoint(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
    ) -> Result<CheckpointProof, TcpError> {
        match self.request(Request::Checkpoint {
            authority_epoch,
            epoch,
            prefix,
        })? {
            Response::Checkpoint(proof)
                if proof.database == self.expected.inventory.database
                    && proof.partition == self.expected.partition
                    && proof.epoch == epoch
                    && proof.prefix == prefix
                    && proof.retained >= prefix =>
            {
                Ok(proof)
            }
            _ => self.wrong_response(),
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
            _ => self.wrong_response(),
        }
    }

    pub fn retire_files(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
        files: Vec<u64>,
    ) -> Result<(), TcpError> {
        if files.len() > 256
            || epoch >= authority_epoch
            || files.windows(2).any(|pair| pair[0] >= pair[1])
            || files
                .iter()
                .any(|offset| offset % self.expected.wal_file_bytes != 0)
        {
            return Err(TcpError::new("invalid retained WAL retirement decision"));
        }
        match self.request(Request::RetireFiles {
            authority_epoch,
            epoch,
            prefix,
            files,
        })? {
            Response::Retired => Ok(()),
            _ => self.wrong_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(sequence: u64) -> CheckpointReceipt {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&sequence.to_be_bytes());
        CheckpointReceipt {
            id: BatchId {
                database: [1; 16],
                operation,
            },
            version: BatchVersion { epoch: 7, sequence },
            digest: PayloadDigest([sequence as u8; 32]),
            address: PhysicalValueAddress {
                partition: 2,
                segment_generation: 7,
                offset: sequence * 256,
                len: 128,
            },
        }
    }

    #[test]
    fn rooted_hash_chain_proves_old_exact_prefix_during_later_appends() {
        let mut first = PrefixDigest::new([1; 16], 2, 7, 0);
        first.push(receipt(0));
        let first_state = first.state();
        let first_digest = first.finish();
        let mut resumed = PrefixDigest::from_state(1, first_state);
        resumed.push(receipt(1));
        assert_eq!(
            resumed.finish(),
            prefix_digest([1; 16], 2, 7, [receipt(0), receipt(1)].into_iter()),
        );
        assert_eq!(
            PrefixDigest::from_state(1, first_state).finish(),
            first_digest
        );
        assert_ne!(
            PrefixDigest::from_state(2, first_state).finish(),
            first_digest
        );
        assert_ne!(
            PrefixDigest::new([1; 16], 3, 7, 0).finish(),
            PrefixDigest::new([1; 16], 2, 7, 0).finish(),
        );
    }
}
