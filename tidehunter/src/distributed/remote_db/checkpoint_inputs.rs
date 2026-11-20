//! Bounded readers and validators for the checkpoint inputs of recovery.

use super::{LocalDbError, MAX_CHECKPOINT_METADATA_BYTES, RemoteEndpoint};
use bincode::Options;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub(super) fn read_checkpoint<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<T, LocalDbError> {
    let file = File::open(path).map_err(LocalDbError::component)?;
    if file.metadata().map_err(LocalDbError::component)?.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Err(LocalDbError::new(
            "checkpoint receipt metadata exceeds bound",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CHECKPOINT_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(LocalDbError::component)?;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_CHECKPOINT_METADATA_BYTES)
        .reject_trailing_bytes()
        .deserialize(&bytes)
        .map_err(LocalDbError::component)
}

/// Whether a checkpointed WAL position names a configured partition that an
/// owner endpoint serves with the position's epoch in its history. A configured
/// partition without an endpoint is rejected rather than indexed past the
/// endpoint list.
pub(super) fn position_has_owner_history(
    storage_partitions: &[u32],
    endpoints: &[RemoteEndpoint],
    histories: &[Vec<u64>],
    partition: u32,
    epoch: u64,
) -> bool {
    storage_partitions.contains(&partition)
        && endpoints
            .iter()
            .position(|endpoint| endpoint.partition == partition)
            .is_some_and(|index| histories[index].contains(&epoch))
}

#[cfg(test)]
mod tests;
