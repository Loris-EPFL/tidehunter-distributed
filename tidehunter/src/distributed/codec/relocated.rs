//! Format 5: a batch of relocated copies.
//!
//! Native Tidehunter writes a value it moves out of a reclaimed WAL file as a
//! *relocated record* (WAL entry tag 5) and replay skips it: the index already
//! names the key, and a copy must never be inserted at the version of the copy.
//! The remote value WAL has the same hazard. A relocation copy is a durable
//! owner batch with a fresh allocator version `vR` that is never published; a
//! replay that inserted its members at `vR` would override a user write of the
//! same key with `vu < vR` by the stale copied value. Format 5 marks the *batch*,
//! so replay (`hybrid::replay`) can skip it without looking at a member.
//!
//! The marker is the header's format `u16` and nothing else. A member of a format
//! 5 batch is encoded exactly like a member of a format 3 batch: its payload (the
//! native `Record` entry after the 8-byte member header) is byte-identical, so
//! the `payload_crc` the index stores for the original member still verifies the
//! copy. Only `Put` members exist: a relocation moves bytes, it does not write
//! tombstones.
//!
//! No separate format gate: an authority whose manifest can name a relocated batch
//! carries the hybrid manifest format, which an older binary refuses at open, so
//! an old decoder never meets a format 5 frame.

use super::{
    BatchId, BatchLimits, BatchVersion, CodecError, CompleteBatch, DROP_CELLS_FORMAT_VERSION,
    FORMAT_VERSION, HEADER_LEN, MEMBER_HEADER_LEN, Mutation, ValidatedMutations,
};
use crate::crc::IntoBytesFixed;
use crate::key_shape::KeyShape;

/// Format of a batch of relocated copies; members are `Put` only.
pub(super) const RELOCATED_FORMAT_VERSION: u16 = 5;

/// Every header format this decoder accepts.
pub(super) const fn known_format(format: u16) -> bool {
    matches!(
        format,
        FORMAT_VERSION | DROP_CELLS_FORMAT_VERSION | RELOCATED_FORMAT_VERSION
    )
}

/// A relocated batch holds `Put` members only; any other member is a forgery or
/// a bug, never a batch to skip silently.
pub(super) fn check_members(format: u16, mutations: &[Mutation]) -> Result<(), CodecError> {
    if format == RELOCATED_FORMAT_VERSION
        && !mutations
            .iter()
            .all(|mutation| matches!(mutation, Mutation::Put { .. }))
    {
        return Err(CodecError::InvalidMutation);
    }
    Ok(())
}

impl CompleteBatch {
    /// Encodes `mutations` (all `Put`) as a relocated-copy batch. Same
    /// validation and limits as [`CompleteBatch::new`].
    ///
    /// # Errors
    /// Any `new` error, or `InvalidMutation` for a non-`Put` member.
    pub(crate) fn new_relocated(
        shape: &KeyShape,
        id: BatchId,
        version: BatchVersion,
        mutations: Vec<Mutation>,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        check_members(RELOCATED_FORMAT_VERSION, &mutations)?;
        let size = Self::encoded_len_for_mutations(shape, &mutations, limits)?;
        Self::encode_validated(id, version, mutations, size, RELOCATED_FORMAT_VERSION)
    }

    /// Validates `mutations` (all `Put`) for a relocated-copy batch without
    /// encoding: the write path encodes under its own batch id and version, as it
    /// does for any batch, and the header marks the batch as relocated.
    ///
    /// # Errors
    /// Any `validated_mutations` error, or `InvalidMutation` for a non-`Put` member.
    pub(crate) fn validated_relocated<'a>(
        shape: &KeyShape,
        mutations: &'a [Mutation],
        limits: BatchLimits,
    ) -> Result<ValidatedMutations<'a>, CodecError> {
        check_members(RELOCATED_FORMAT_VERSION, mutations)?;
        let mut validated = Self::validated_mutations(shape, mutations, limits)?;
        validated.format = RELOCATED_FORMAT_VERSION;
        Ok(validated)
    }

    /// The longest prefix of `mutations` that fits one batch under `limits`
    /// (operation count and encoded bytes). Zero when even the first member
    /// cannot fit, which the caller reports instead of looping.
    pub(crate) fn fitting_prefix_len(mutations: &[Mutation], limits: BatchLimits) -> usize {
        let mut size = HEADER_LEN;
        for (count, mutation) in mutations.iter().enumerate() {
            let next = size
                .checked_add(MEMBER_HEADER_LEN)
                .and_then(|size| size.checked_add(mutation.native_entry().len()));
            match next {
                Some(next)
                    if next <= limits.max_encoded_bytes
                        && u32::try_from(count + 1)
                            .is_ok_and(|count| count <= limits.max_operations) =>
                {
                    size = next;
                }
                _ => return count,
            }
        }
        mutations.len()
    }

    /// True for a batch of relocated copies (format 5). Read from the canonical
    /// header, so it survives encode, decode and the owner page path unchanged.
    pub(crate) fn is_relocated(&self) -> bool {
        u16::from_be_bytes([self.encoded[8], self.encoded[9]]) == RELOCATED_FORMAT_VERSION
    }
}

#[cfg(test)]
mod tests;
