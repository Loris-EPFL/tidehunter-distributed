//! A bounded persistence group containing whole application batches.
//!
//! Only a complete outer native CRC frame plus successful group/member decoding
//! makes any member authority-eligible. Inner CRC frames support existing native
//! random reads; finding a valid inner frame alone NEVER authorizes recovery.
//! The caller must authenticate/fence all members before emitting this group.
//!
//! Storage must fail closed on uncertain I/O and discover recovery frames only
//! at registered fragment boundaries.
//! Each group occupies one native allocation within one fragment; splitting a
//! native-sized batch across groups is deliberately unsupported, not silently
//! substituted for atomicity. Native `Db`'s accepted batch API remains unchanged.
//!
//! Tidehunter §§3.1/3.3 and `wal_replay.rs` supply the complete atomic replay rule.
//! TiKV Raft Engine's caller grouping motivates barrier amortization only:
//! https://github.com/tikv/raft-engine/blob/master/src/write_barrier.rs

use super::codec::{BatchLimits, CodecError, CompleteBatch};
use crate::crc::{CrcFrame, IntoBytesFixed};
use crate::key_shape::KeyShape;
use bytes::{BufMut, BytesMut};
use minibytes::Bytes;
use std::collections::BTreeSet;
use std::fmt;

#[cfg(test)]
mod tests;

const MAGIC: &[u8; 8] = b"THGROUP2";
const FORMAT_VERSION: u16 = 2;
const LIVE_FORMAT_VERSION: u16 = 3;
pub const LIVE_BLOCK_SIZE: usize = 4096;
const LIVE_BLOCK_SHIFT: u16 = 12;
/// Magic, format version, zero reserved field and member count.
pub const GROUP_HEADER_LEN: usize = 16;
const FRAME_HEADER_LEN: usize = CrcFrame::CRC_HEADER_LENGTH;
const NATIVE_FRAME_MAX_EXCLUSIVE: usize = 1 << 30;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupLimits {
    pub max_batches: u32,
    /// Group payload, excluding its outer native CRC header. Each contained
    /// batch contributes its native CRC header and 0..7 alignment bytes.
    /// Live v3's bounded physical tail is excluded from this logical limit.
    pub max_encoded_bytes: usize,
}

impl GroupLimits {
    pub fn validate(self) -> Result<(), GroupError> {
        if self.max_batches == 0
            || self.max_batches > 65_536
            || self.max_encoded_bytes < GROUP_HEADER_LEN + 16
            || self.max_encoded_bytes >= NATIVE_FRAME_MAX_EXCLUSIVE - FRAME_HEADER_LEN
        {
            return Err(GroupError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CompleteGroup {
    batches: Vec<CompleteBatch>,
    batch_offsets: Vec<u32>,
    encoded: Bytes,
}

/// Validated live members without an intermediate encoded payload.
pub(crate) struct LiveFrame {
    batches: Vec<CompleteBatch>,
    len: usize,
}

impl LiveFrame {
    pub(crate) fn new(
        shape: &KeyShape,
        batches: Vec<CompleteBatch>,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        limits.validate()?;
        batch_limits.validate()?;
        check_count(batches.len(), limits)?;
        if CompleteGroup::encoded_len_for_batches(&batches)? > limits.max_encoded_bytes {
            return Err(GroupError::TooLarge);
        }
        for batch in &batches {
            batch.validate_for(shape, batch_limits)?;
        }
        check_members(&batches)?;
        let len = Self::frame_len_for_batches(&batches)?;
        Ok(Self { batches, len })
    }

    /// Exact physical length, including the outer header and v3 zero tail.
    pub(crate) fn frame_len_for_batches(batches: &[CompleteBatch]) -> Result<usize, GroupError> {
        let len = CompleteGroup::encoded_len_for_batches(batches)?
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|n| n.checked_add(LIVE_BLOCK_SIZE - 1))
            .map(|n| n & !(LIVE_BLOCK_SIZE - 1))
            .ok_or(GroupError::TooLarge)?;
        if len >= NATIVE_FRAME_MAX_EXCLUSIVE {
            return Err(GroupError::TooLarge);
        }
        Ok(len)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn batches(&self) -> &[CompleteBatch] {
        &self.batches
    }

    pub(crate) fn into_batches(self) -> Vec<CompleteBatch> {
        self.batches
    }

    /// Same payload-relative offsets as CompleteGroup, without an offset Vec.
    pub(crate) fn batch_offsets(&self) -> impl Iterator<Item = u32> + '_ {
        self.batches.iter().scan(GROUP_HEADER_LEN, |offset, batch| {
            let current = *offset as u32;
            // Construction checked all lengths and their sum.
            *offset += (batch.encoded().len() + FRAME_HEADER_LEN + 7) & !7;
            Some(current)
        })
    }

    /// Fully initializes the destination, including both kinds of padding.
    /// Each immutable batch encoding is copied directly to its final position.
    pub(crate) fn encode_into(&self, frame: &mut [u8]) -> Result<(), GroupError> {
        if frame.len() != self.len {
            return Err(GroupError::InvalidFormat);
        }
        let payload = &mut frame[FRAME_HEADER_LEN..];
        payload[..8].copy_from_slice(MAGIC);
        payload[8..10].copy_from_slice(&LIVE_FORMAT_VERSION.to_be_bytes());
        payload[10..12].copy_from_slice(&LIVE_BLOCK_SHIFT.to_be_bytes());
        payload[12..16].copy_from_slice(&(self.batches.len() as u32).to_be_bytes());
        let mut end = GROUP_HEADER_LEN;
        for (batch, offset) in self.batches.iter().zip(self.batch_offsets()) {
            let offset = offset as usize;
            let encoded = batch.encoded();
            payload[offset..offset + 4].copy_from_slice(&(encoded.len() as u32).to_be_bytes());
            payload[offset + 4..offset + 8]
                .copy_from_slice(&crc32fast::hash(encoded).to_be_bytes());
            payload[offset + FRAME_HEADER_LEN..offset + FRAME_HEADER_LEN + encoded.len()]
                .copy_from_slice(encoded);
            end = offset + FRAME_HEADER_LEN + encoded.len();
            let padded = (end + 7) & !7;
            payload[end..padded].fill(0);
            end = padded;
        }
        payload[end..].fill(0);
        let crc = crc32fast::hash(payload);
        frame[..4].copy_from_slice(&((self.len - FRAME_HEADER_LEN) as u32).to_be_bytes());
        frame[4..8].copy_from_slice(&crc.to_be_bytes());
        Ok(())
    }
}

impl CompleteGroup {
    /// Recheck the storage schema/limits for an immutable validated group.
    /// External bytes must pass `decode` before reaching this private fast path.
    /// Identity uniqueness, epoch agreement, CRCs and canonical group layout
    /// were already checked by the only constructors and cannot change here.
    pub(crate) fn validate_for(
        &self,
        shape: &KeyShape,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<(), GroupError> {
        limits.validate()?;
        batch_limits.validate()?;
        check_count(self.batches.len(), limits)?;
        if self.encoded.len() > limits.max_encoded_bytes
            && (self.encoded[9] != LIVE_FORMAT_VERSION as u8
                || Self::encoded_len_for_batches(&self.batches)? > limits.max_encoded_bytes)
        {
            return Err(GroupError::TooLarge);
        }
        for batch in &self.batches {
            batch.validate_for(shape, batch_limits)?;
        }
        Ok(())
    }

    /// Validates every member against the storage schema before producing an
    /// emit-ready group. A batch constructed against another schema is rejected.
    pub fn new(
        shape: &KeyShape,
        batches: Vec<CompleteBatch>,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        limits.validate()?;
        batch_limits.validate()?;
        check_count(batches.len(), limits)?;
        let size = Self::encoded_len_for_batches(&batches)?;
        if size > limits.max_encoded_bytes {
            return Err(GroupError::TooLarge);
        }
        // Validate the supplied schema, not only the schema used by a caller's
        // CompleteBatch constructor. Private immutable batches were already
        // integrity-checked; do not hash every value again just for grouping.
        for batch in &batches {
            batch.validate_for(shape, batch_limits)?;
        }
        check_members(&batches)?;
        let mut encoded = BytesMut::with_capacity(size);
        encoded.put_slice(MAGIC);
        encoded.put_u16(FORMAT_VERSION);
        encoded.put_u16(0);
        encoded.put_u32(batches.len() as u32);
        let mut batch_offsets = Vec::with_capacity(batches.len());
        for batch in &batches {
            batch_offsets.push(encoded.len() as u32);
            // Same native frame as CrcFrame::new(batch), directly in the group.
            // Canonical batches are nonempty and already bounded below 1 GiB;
            // avoid a per-member temporary allocation and second payload copy.
            encoded.put_u32(batch.encoded().len() as u32);
            encoded.put_u32(crc32fast::hash(batch.encoded()));
            encoded.put_slice(batch.encoded());
            encoded.resize(align(encoded.len())?, 0);
        }
        debug_assert_eq!(encoded.len(), size);
        Ok(Self {
            batches,
            batch_offsets,
            encoded: encoded.freeze().into(),
        })
    }

    /// Construct a distributed live frame whose outer CRC frame is block aligned.
    /// Padding is part of the CRC payload, so the native iterator needs no
    /// special treatment and cannot interpret padding as another frame.
    pub fn new_live(
        shape: &KeyShape,
        batches: Vec<CompleteBatch>,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        let mut group = Self::new(shape, batches, batch_limits, limits)?;
        let frame_len = group
            .encoded
            .len()
            .checked_add(FRAME_HEADER_LEN)
            .and_then(|n| n.checked_add(LIVE_BLOCK_SIZE - 1))
            .map(|n| n & !(LIVE_BLOCK_SIZE - 1))
            .ok_or(GroupError::TooLarge)?;
        let payload_len = frame_len - FRAME_HEADER_LEN;
        if frame_len >= NATIVE_FRAME_MAX_EXCLUSIVE {
            return Err(GroupError::TooLarge);
        }
        let mut encoded = BytesMut::with_capacity(payload_len);
        encoded.put_slice(&group.encoded);
        encoded[8..10].copy_from_slice(&LIVE_FORMAT_VERSION.to_be_bytes());
        encoded[10..12].copy_from_slice(&LIVE_BLOCK_SHIFT.to_be_bytes());
        encoded.resize(payload_len, 0);
        group.encoded = encoded.freeze().into();
        Ok(group)
    }

    /// Return the exact native outer CRC frame for a live group.
    pub fn live_frame(&self) -> Result<Vec<u8>, GroupError> {
        if self.encoded.len() < GROUP_HEADER_LEN
            || u16::from_be_bytes([self.encoded[8], self.encoded[9]]) != LIVE_FORMAT_VERSION
        {
            return Err(GroupError::InvalidFormat);
        }
        Ok(CrcFrame::new(self).as_ref().to_vec())
    }

    /// Decode an outer-CRC-validated group payload. This method checks shape,
    /// length, count, inner CRCs, batch digests, padding and identity uniqueness.
    /// Storage must verify the OUTER CRC before treating any result as authority.
    pub fn decode(
        shape: &KeyShape,
        encoded: Bytes,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        Self::decode_slice(shape, &encoded, batch_limits, limits)
    }

    /// Detach a borrowed payload once before validating its stable bytes.
    pub(crate) fn decode_slice(
        shape: &KeyShape,
        encoded: &[u8],
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        limits.validate()?;
        batch_limits.validate()?;
        if encoded.len() > limits.max_encoded_bytes.saturating_add(LIVE_BLOCK_SIZE - 1) {
            return Err(GroupError::TooLarge);
        }
        if encoded.len() < GROUP_HEADER_LEN {
            return Err(GroupError::Truncated);
        }
        Self::decode_owned(shape, Bytes::copy_from_slice(encoded), batch_limits, limits)
    }

    /// Decode stable, owned bytes without a second whole-group copy.
    fn decode_owned(
        shape: &KeyShape,
        encoded: Bytes,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        let version = u16::from_be_bytes(encoded[8..10].try_into().unwrap());
        let flags = u16::from_be_bytes(encoded[10..12].try_into().unwrap());
        if &encoded[..8] != MAGIC
            || !matches!(
                (version, flags),
                (FORMAT_VERSION, 0) | (LIVE_FORMAT_VERSION, LIVE_BLOCK_SHIFT)
            )
        {
            return Err(GroupError::InvalidFormat);
        }
        let count = u32::from_be_bytes(encoded[12..16].try_into().unwrap()) as usize;
        check_count(count, limits)?;
        // At least an inner CRC and one payload byte rounded to alignment.
        // Reject impossible peer-provided counts before count-sized allocation.
        if count > (encoded.len() - GROUP_HEADER_LEN) / 16 {
            return Err(GroupError::Truncated);
        }
        let mut offset = GROUP_HEADER_LEN;
        let mut batches = Vec::with_capacity(count);
        let mut batch_offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let batch_bytes = CrcFrame::read_from_bytes(&encoded, offset)
                .map_err(|_| GroupError::InvalidMemberFrame)?;
            let end = offset
                .checked_add(FRAME_HEADER_LEN)
                .and_then(|n| n.checked_add(batch_bytes.len()))
                .ok_or(GroupError::TooLarge)?;
            let next = align(end)?;
            let padding = encoded.get(end..next).ok_or(GroupError::Truncated)?;
            if padding.iter().any(|b| *b != 0) {
                return Err(GroupError::InvalidPadding);
            }
            batch_offsets.push(offset as u32);
            batches.push(CompleteBatch::decode(shape, batch_bytes, batch_limits)?);
            offset = next;
        }
        if offset > limits.max_encoded_bytes {
            return Err(GroupError::TooLarge);
        }
        if version == FORMAT_VERSION {
            if offset != encoded.len() {
                return Err(GroupError::TrailingBytes);
            }
        } else {
            let canonical_len = offset
                .checked_add(FRAME_HEADER_LEN)
                .and_then(|n| n.checked_add(LIVE_BLOCK_SIZE - 1))
                .map(|n| (n & !(LIVE_BLOCK_SIZE - 1)) - FRAME_HEADER_LEN)
                .ok_or(GroupError::TooLarge)?;
            if encoded.len() != canonical_len {
                return Err(GroupError::TrailingBytes);
            }
            if encoded[offset..].iter().any(|byte| *byte != 0) {
                return Err(GroupError::InvalidPadding);
            }
        }
        check_members(&batches)?;
        Ok(Self {
            batches,
            batch_offsets,
            encoded,
        })
    }

    /// Exact group payload size, including inner CRC frames and zero padding.
    pub fn encoded_len_for_batches(batches: &[CompleteBatch]) -> Result<usize, GroupError> {
        if batches.is_empty() {
            return Err(GroupError::InvalidBatchCount);
        }
        let mut size = GROUP_HEADER_LEN;
        for batch in batches {
            size = size
                .checked_add(Self::encoded_member_len(batch.encoded().len())?)
                .ok_or(GroupError::TooLarge)?;
        }
        if size >= NATIVE_FRAME_MAX_EXCLUSIVE - FRAME_HEADER_LEN {
            return Err(GroupError::TooLarge);
        }
        Ok(size)
    }

    /// Bytes contributed by one batch. Useful for bounded queue/group admission
    /// without first allocating a group or cloning all pending batches.
    pub fn encoded_member_len(batch_payload_len: usize) -> Result<usize, GroupError> {
        align(
            batch_payload_len
                .checked_add(FRAME_HEADER_LEN)
                .ok_or(GroupError::TooLarge)?,
        )
    }

    pub fn batches(&self) -> &[CompleteBatch] {
        &self.batches
    }

    /// Offsets relative to the GROUP PAYLOAD, excluding its outer CRC header.
    /// Add `group_frame.offset() + CrcFrame::CRC_HEADER_LENGTH` to obtain an
    /// embedded batch frame's native physical position. Frame length is that
    /// batch's `encoded().len() + 8`; alignment padding is not part of its frame.
    pub fn batch_offsets(&self) -> &[u32] {
        &self.batch_offsets
    }

    pub fn encoded(&self) -> &Bytes {
        &self.encoded
    }
}

impl IntoBytesFixed for CompleteGroup {
    fn len(&self) -> usize {
        self.encoded.len()
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.encoded);
    }
}

fn align(n: usize) -> Result<usize, GroupError> {
    n.checked_add(7).map(|n| n & !7).ok_or(GroupError::TooLarge)
}

fn check_count(count: usize, limits: GroupLimits) -> Result<(), GroupError> {
    if count == 0 || count > limits.max_batches as usize {
        return Err(GroupError::InvalidBatchCount);
    }
    Ok(())
}

fn check_members(batches: &[CompleteBatch]) -> Result<(), GroupError> {
    // A singleton has no cross-member identity or authority conflict.
    if batches.len() == 1 {
        return Ok(());
    }
    let first = &batches[0]; // caller has already checked a nonzero bounded count
    let mut identities = BTreeSet::new();
    let mut versions = BTreeSet::new();
    for batch in batches {
        if batch.id().database != first.id().database
            || batch.version().epoch != first.version().epoch
        {
            return Err(GroupError::MixedAuthority);
        }
        if !identities.insert(batch.id()) {
            return Err(GroupError::DuplicateIdentity);
        }
        if !versions.insert(batch.version()) {
            return Err(GroupError::DuplicateVersion);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupError {
    InvalidLimits,
    InvalidBatchCount,
    TooLarge,
    Truncated,
    InvalidFormat,
    InvalidMemberFrame,
    InvalidPadding,
    TrailingBytes,
    MixedAuthority,
    DuplicateIdentity,
    DuplicateVersion,
    Batch(CodecError),
}

impl From<CodecError> for GroupError {
    fn from(error: CodecError) -> Self {
        Self::Batch(error)
    }
}

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid complete persistence group: {self:?}")
    }
}

impl std::error::Error for GroupError {}
