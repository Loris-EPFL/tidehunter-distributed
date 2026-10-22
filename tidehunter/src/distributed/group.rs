//! A bounded persistence group containing whole application batches.
//!
//! Only a complete outer native CRC frame plus successful group/member decoding
//! makes any member authority-eligible. Inner CRC frames support existing native
//! random reads; finding a valid inner frame alone NEVER authorizes recovery.
//! The caller must authenticate/fence all members before emitting this group.
//!
//! The independent `group_model` passed before implementing this format. The
//! storage refinement must still permit only one unpersisted group per stream,
//! fail-stop on uncertain I/O and discover only registered fragment boundaries.
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

const MAGIC: &[u8; 8] = b"THGROUP2";
const FORMAT_VERSION: u16 = 2;
/// Magic, format version, zero reserved field and member count.
pub const GROUP_HEADER_LEN: usize = 16;
const FRAME_HEADER_LEN: usize = CrcFrame::CRC_HEADER_LENGTH;
const NATIVE_FRAME_MAX_EXCLUSIVE: usize = 1 << 30;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GroupLimits {
    pub max_batches: u32,
    /// Group payload, excluding its outer native CRC header. Each contained
    /// batch contributes its native CRC header and 0..7 alignment bytes.
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
        if self.encoded.len() > limits.max_encoded_bytes {
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

    /// Decode an outer-CRC-validated group payload. This method checks shape,
    /// length, count, inner CRCs, batch digests, padding and identity uniqueness.
    /// Storage must verify the OUTER CRC before treating any result as authority.
    pub fn decode(
        shape: &KeyShape,
        encoded: Bytes,
        batch_limits: BatchLimits,
        limits: GroupLimits,
    ) -> Result<Self, GroupError> {
        limits.validate()?;
        batch_limits.validate()?;
        if encoded.len() > limits.max_encoded_bytes {
            return Err(GroupError::TooLarge);
        }
        if encoded.len() < GROUP_HEADER_LEN {
            return Err(GroupError::Truncated);
        }
        // CRC/member validation below must describe bytes that cannot change
        // after decode. Arbitrary input may be a mutable file-backed mapping.
        let encoded: Bytes = encoded.as_ref().to_vec().into_boxed_slice().into();
        if &encoded[..8] != MAGIC
            || u16::from_be_bytes(encoded[8..10].try_into().unwrap()) != FORMAT_VERSION
            || encoded[10..12] != [0, 0]
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
        if offset != encoded.len() {
            return Err(GroupError::TrailingBytes);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::Mutation;
    use crate::distributed::types::{BatchId, BatchVersion};
    use crate::key_shape::KeyType;

    fn shape() -> KeyShape {
        KeyShape::new_single(32, 1, KeyType::uniform(1))
    }

    fn limits() -> (BatchLimits, GroupLimits) {
        (
            BatchLimits {
                max_operations: 16,
                max_encoded_bytes: 4096,
            },
            GroupLimits {
                max_batches: 8,
                max_encoded_bytes: 32768,
            },
        )
    }

    fn batch(id: u8, sequence: u64) -> CompleteBatch {
        let shape = shape();
        let keyspace = shape.iter_ks().next().unwrap().id();
        CompleteBatch::new(
            &shape,
            BatchId {
                database: [1; 16],
                operation: [id; 16],
            },
            BatchVersion { epoch: 2, sequence },
            vec![
                Mutation::Put {
                    keyspace,
                    key: Bytes::from(vec![id; 32]),
                    value: Bytes::from(vec![id; 17]),
                },
                Mutation::Delete {
                    keyspace,
                    key: Bytes::from(vec![id.wrapping_add(1); 32]),
                },
            ],
            limits().0,
        )
        .unwrap()
    }

    #[test]
    fn complete_group_roundtrips_and_exposes_native_inner_frames() {
        let (batch_limits, group_limits) = limits();
        // Physical grouping does not reorder logical versions or duplicate keys.
        let batches = vec![batch(3, 2), batch(4, 1)];
        let expected_size = CompleteGroup::encoded_len_for_batches(&batches).unwrap();
        let group = CompleteGroup::new(&shape(), batches, batch_limits, group_limits).unwrap();
        assert_eq!(group.encoded().len(), expected_size);
        let outer = CrcFrame::new(&group);
        let payload = CrcFrame::read_from_slice(outer.as_ref(), 0).unwrap();
        let recovered = CompleteGroup::decode(
            &shape(),
            Bytes::from(payload.to_vec()),
            batch_limits,
            group_limits,
        )
        .unwrap();
        for (i, offset) in recovered.batch_offsets().iter().enumerate() {
            assert_eq!(offset % 8, 0);
            let inner =
                CrcFrame::read_from_slice(outer.as_ref(), *offset as usize + FRAME_HEADER_LEN)
                    .unwrap();
            assert_eq!(inner, recovered.batches()[i].encoded().as_ref());
            assert_eq!(recovered.batches()[i].digest(), group.batches()[i].digest());
            assert_eq!(
                recovered.batches()[i].mutations(),
                group.batches()[i].mutations()
            );
        }
    }

    #[test]
    fn direct_member_encoding_is_byte_identical_to_native_crc_frames() {
        let (batch_limits, group_limits) = limits();
        let keyspace = shape().iter_ks().next().unwrap().id();
        let batches: Vec<_> = [0, 1, 7, 8, 127, 128, 1024]
            .into_iter()
            .enumerate()
            .map(|(n, len)| {
                CompleteBatch::new(
                    &shape(),
                    BatchId {
                        database: [1; 16],
                        operation: [n as u8; 16],
                    },
                    BatchVersion {
                        epoch: 2,
                        sequence: n as u64,
                    },
                    vec![Mutation::Put {
                        keyspace,
                        key: vec![n as u8; 32].into(),
                        value: vec![n as u8; len].into(),
                    }],
                    batch_limits,
                )
                .unwrap()
            })
            .collect();
        let group = CompleteGroup::new(&shape(), batches, batch_limits, group_limits).unwrap();
        let mut expected = BytesMut::new();
        expected.put_slice(MAGIC);
        expected.put_u16(FORMAT_VERSION);
        expected.put_u16(0);
        expected.put_u32(group.batches().len() as u32);
        for batch in group.batches() {
            expected.put_slice(CrcFrame::new(batch).as_ref());
            expected.resize(align(expected.len()).unwrap(), 0);
        }
        assert_eq!(group.encoded().as_ref(), expected.as_ref());
        let recovered = CompleteGroup::decode(
            &shape(),
            group.encoded().clone(),
            batch_limits,
            group_limits,
        )
        .unwrap();
        for (actual, expected) in recovered.batches().iter().zip(group.batches()) {
            assert_eq!(actual.mutations(), expected.mutations());
            assert_eq!(actual.digest(), expected.digest());
        }
    }

    #[test]
    fn decoded_group_detaches_arbitrary_mapped_backing_before_validation() {
        use std::io::{Seek, SeekFrom, Write};
        let (bl, gl) = limits();
        let original = CompleteGroup::new(&shape(), vec![batch(3, 1)], bl, gl).unwrap();
        let temp = tempdir::TempDir::new("group-owned-decode").unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(temp.path().join("input"))
            .unwrap();
        file.write_all(original.encoded()).unwrap();
        file.sync_all().unwrap();
        // SAFETY: the mapping's backing length is fixed for this whole test.
        let map = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
        let mapped = Bytes::from_owner(map);
        let decoded = CompleteGroup::decode(&shape(), mapped.clone(), bl, gl).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        assert_eq!(mapped[0], b'X');
        assert_eq!(decoded.encoded(), original.encoded());
        assert_eq!(
            decoded.batches()[0].mutations(),
            original.batches()[0].mutations()
        );
        assert!(CompleteGroup::decode(&shape(), mapped, bl, gl).is_err());
    }

    #[test]
    fn every_outer_and_inner_truncation_rejects_partial_group() {
        let (bl, gl) = limits();
        let group = CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap();
        let mut old_group = group.encoded().to_vec();
        old_group[..8].copy_from_slice(b"THGROUP1");
        assert_eq!(
            CompleteGroup::decode(&shape(), old_group.into(), bl, gl).unwrap_err(),
            GroupError::InvalidFormat
        );
        let outer = CrcFrame::new(&group);
        for cut in 0..outer.as_ref().len() {
            assert!(
                CrcFrame::read_from_slice(&outer.as_ref()[..cut], 0).is_err(),
                "outer cut {cut}"
            );
        }
        for cut in 0..group.encoded().len() {
            assert!(
                CompleteGroup::decode(&shape(), group.encoded().slice(..cut), bl, gl).is_err(),
                "inner cut {cut}"
            );
        }
    }

    #[test]
    fn identity_version_epoch_and_capacity_validation_happens_before_emit() {
        let (bl, gl) = limits();
        let b = batch(3, 1);
        assert_eq!(
            CompleteGroup::new(&shape(), vec![b.clone(), b.clone()], bl, gl).unwrap_err(),
            GroupError::DuplicateIdentity
        );
        assert_eq!(
            CompleteGroup::new(&shape(), vec![b.clone(), batch(4, 1)], bl, gl).unwrap_err(),
            GroupError::DuplicateVersion
        );
        let tiny = GroupLimits {
            max_encoded_bytes: CompleteGroup::encoded_len_for_batches(std::slice::from_ref(&b))
                .unwrap()
                - 1,
            ..gl
        };
        assert_eq!(
            CompleteGroup::new(&shape(), vec![b.clone()], bl, tiny).unwrap_err(),
            GroupError::TooLarge
        );
        assert_eq!(
            CompleteGroup::new(&shape(), vec![], bl, gl).unwrap_err(),
            GroupError::InvalidBatchCount
        );
        assert_eq!(
            CompleteGroup::new(
                &shape(),
                vec![b.clone(), batch(4, 2)],
                bl,
                GroupLimits {
                    max_batches: 1,
                    ..gl
                }
            )
            .unwrap_err(),
            GroupError::InvalidBatchCount
        );
        let wrong = CompleteBatch::new(
            &shape(),
            BatchId {
                database: [2; 16],
                operation: [4; 16],
            },
            BatchVersion {
                epoch: 2,
                sequence: 2,
            },
            b.mutations().to_vec(),
            bl,
        )
        .unwrap();
        assert_eq!(
            CompleteGroup::new(&shape(), vec![b, wrong], bl, gl).unwrap_err(),
            GroupError::MixedAuthority
        );
        assert!(CompleteGroup::encoded_member_len(usize::MAX).is_err());
    }

    #[test]
    fn immutable_members_are_revalidated_against_storage_schema_and_limits() {
        let (bl, gl) = limits();
        let b = batch(3, 1);
        let wrong_shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
        assert!(matches!(
            CompleteGroup::new(&wrong_shape, vec![b.clone()], bl, gl),
            Err(GroupError::Batch(CodecError::InvalidKeyLength { .. }))
        ));
        let fewer = BatchLimits {
            max_operations: 1,
            ..bl
        };
        assert_eq!(
            CompleteGroup::new(&shape(), vec![b.clone()], fewer, gl).unwrap_err(),
            GroupError::Batch(CodecError::InvalidOperationCount)
        );
        let smaller = BatchLimits {
            max_encoded_bytes: b.encoded().len() - 1,
            ..bl
        };
        assert_eq!(
            CompleteGroup::new(&shape(), vec![b], smaller, gl).unwrap_err(),
            GroupError::Batch(CodecError::TooLarge)
        );

        let group = CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap();
        group.validate_for(&shape(), bl, gl).unwrap();
        assert!(matches!(
            group.validate_for(&wrong_shape, bl, gl),
            Err(GroupError::Batch(CodecError::InvalidKeyLength { .. }))
        ));
        assert_eq!(
            group.validate_for(
                &shape(),
                bl,
                GroupLimits {
                    max_batches: 1,
                    ..gl
                }
            ),
            Err(GroupError::InvalidBatchCount)
        );
        assert_eq!(
            group.validate_for(
                &shape(),
                bl,
                GroupLimits {
                    max_encoded_bytes: group.encoded().len() - 1,
                    ..gl
                }
            ),
            Err(GroupError::TooLarge)
        );
        assert_eq!(
            group.validate_for(&shape(), fewer, gl),
            Err(GroupError::Batch(CodecError::InvalidOperationCount))
        );
    }

    #[test]
    fn corruption_rejected_even_with_recomputed_outer_crc() {
        let (bl, gl) = limits();
        let group = CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap();
        // Corruption of every byte must fail either group structure, member CRC,
        // digest or canonical padding even if a caller recomputes the outer CRC.
        for index in 0..group.encoded().len() {
            let mut bytes = group.encoded().to_vec();
            bytes[index] ^= 0x80;
            assert!(
                CompleteGroup::decode(&shape(), Bytes::from(bytes), bl, gl).is_err(),
                "byte {index}"
            );
        }
        let mut trailing = group.encoded().to_vec();
        trailing.extend_from_slice(&[0; 8]);
        assert_eq!(
            CompleteGroup::decode(&shape(), Bytes::from(trailing), bl, gl).unwrap_err(),
            GroupError::TrailingBytes
        );
    }

    #[test]
    fn immutable_groups_can_be_decoded_concurrently() {
        let (bl, gl) = limits();
        let group = std::sync::Arc::new(
            CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap(),
        );
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let group = group.clone();
                scope.spawn(move || {
                    for _ in 0..16 {
                        let decoded =
                            CompleteGroup::decode(&shape(), group.encoded().clone(), bl, gl)
                                .unwrap();
                        assert_eq!(decoded.batches()[1].digest(), group.batches()[1].digest());
                    }
                });
            }
        });
    }
}
