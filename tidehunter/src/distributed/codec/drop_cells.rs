//! The non-value `DropCells` member of a singleton complete batch.
//!
//! The member bytes are native `WalEntry::DropCells`: `6 | keyspace | first |
//! last`, where a `CellId` is `0 + be_u64` (integer cell) or `1 + be_u16 byte
//! count + canonical prefix bytes`. Encoding reuses the native encoder.
//! Decoding is checked over a bounded cursor: native `WalEntry::from_bytes` and
//! `CellId::from_bytes` panic on malformed input and never see these bytes.
//!
//! The member has no value and no record descriptor. Its CRC uses a dedicated
//! domain and binds the batch database, identity, version, ordinal 0, member
//! offset and length, keyspace and native bytes.

use super::{CodecError, Cursor, MEMBER_HEADER_LEN, Mutation, RangeMemberDescriptor};
use crate::cell::CellId;
use crate::crc::IntoBytesFixed;
use crate::db::WalEntry;
use crate::distributed::types::{BatchId, BatchVersion, MemberChecksum};
use crate::key_shape::{CELL_PREFIX_LENGTH, KeyShape, KeySpace, KeyType};
use bytes::{BufMut, BytesMut};

const TAG: u8 = 6;
const CHECKSUM_DOMAIN: &[u8] = b"tidehunter-drop-cells-member-v1\0";

/// Validate an inclusive cell range against the storage shape: known live
/// keyspace with fixed-length index keys, cells of that keyspace's cell type
/// in canonical form, and `first <= last`.
pub(super) fn validate(
    shape: &KeyShape,
    keyspace: KeySpace,
    first: &CellId,
    last: &CellId,
) -> Result<(), CodecError> {
    let Some(descriptor) = shape
        .iter_ks()
        .find(|descriptor| descriptor.id() == keyspace)
    else {
        return Err(CodecError::UnknownKeySpace(keyspace.0));
    };
    if descriptor.destroyed() {
        return Err(CodecError::DestroyedKeySpace(keyspace.0));
    }
    let invalid = CodecError::InvalidDropCells {
        keyspace: keyspace.0,
    };
    // Native cell boundary math needs fixed-length reduced keys that cover the
    // cell prefix (four bytes for uniform cells).
    let prefix_len = match descriptor.first_cell() {
        CellId::Bytes(prefix) => prefix.len(),
        CellId::Integer(_) => CELL_PREFIX_LENGTH,
    };
    if descriptor
        .index_key_size()
        .is_none_or(|key_size| prefix_len == 0 || key_size < prefix_len)
    {
        return Err(invalid);
    }
    let canonical = |cell: &CellId| match (descriptor.key_type(), cell) {
        (KeyType::Uniform(config), CellId::Integer(id)) => config
            .cells_per_mutex()
            .checked_mul(descriptor.num_mutexes())
            .and_then(|count| u32::try_from(count).ok())
            .is_some_and(|count| (*id as u128) < u128::from(count)),
        (KeyType::PrefixedUniform(_), CellId::Bytes(prefix)) => {
            !prefix.is_empty()
                && matches!(descriptor.first_cell(), CellId::Bytes(first) if first.len() == prefix.len())
                && descriptor.cell_id(prefix) == *cell
        }
        _ => false,
    };
    // Both cells have the keyspace's cell type here, so `>` cannot panic.
    if !canonical(first) || !canonical(last) || first > last {
        return Err(invalid);
    }
    Ok(())
}

/// Heap bytes owned by spilled cell prefixes (inline up to 16 bytes).
pub(super) fn spilled_bytes(first: &CellId, last: &CellId) -> usize {
    [first, last]
        .into_iter()
        .map(|cell| match cell {
            CellId::Bytes(prefix) if prefix.spilled() => prefix.capacity(),
            _ => 0,
        })
        .sum()
}

/// Append the framed member and return its descriptor with the final CRC.
pub(super) fn encode_member(
    id: BatchId,
    version: BatchVersion,
    keyspace: KeySpace,
    first: &CellId,
    last: &CellId,
    encoded: &mut BytesMut,
) -> Result<RangeMemberDescriptor, CodecError> {
    let entry = WalEntry::DropCells(keyspace, first.clone(), last.clone());
    let native_len = entry.len();
    let offset = encoded.len();
    let descriptor = RangeMemberDescriptor {
        offset: u32::try_from(offset).map_err(|_| CodecError::TooLarge)?,
        len: u32::try_from(MEMBER_HEADER_LEN + native_len).map_err(|_| CodecError::TooLarge)?,
        checksum: MemberChecksum(0),
    };
    encoded.put_u32(native_len as u32);
    encoded.put_u32(0);
    entry.write_into_bytes(encoded);
    let checksum = member_checksum(
        id,
        version,
        &descriptor,
        keyspace,
        &encoded[offset + MEMBER_HEADER_LEN..],
    );
    encoded[offset + 4..offset + MEMBER_HEADER_LEN].copy_from_slice(&checksum.0.to_be_bytes());
    Ok(RangeMemberDescriptor {
        checksum,
        ..descriptor
    })
}

/// Checked decode of native member bytes; validates against the shape.
pub(super) fn decode_native(shape: &KeyShape, native: &[u8]) -> Result<Mutation, CodecError> {
    let mut cursor = Cursor::new(native);
    if cursor.u8()? != TAG {
        return Err(CodecError::InvalidMutation);
    }
    let keyspace = KeySpace(cursor.u8()?);
    let first_cell = decode_cell(&mut cursor, keyspace)?;
    let last_cell = decode_cell(&mut cursor, keyspace)?;
    if cursor.position != native.len() {
        return Err(CodecError::TrailingBytes);
    }
    validate(shape, keyspace, &first_cell, &last_cell)?;
    Ok(Mutation::DropCells {
        keyspace,
        first_cell,
        last_cell,
    })
}

fn decode_cell(cursor: &mut Cursor<'_>, keyspace: KeySpace) -> Result<CellId, CodecError> {
    match cursor.u8()? {
        0 => usize::try_from(cursor.u64()?)
            .map(CellId::Integer)
            .map_err(|_| CodecError::InvalidDropCells {
                keyspace: keyspace.0,
            }),
        1 => {
            let len = usize::from(cursor.u16()?);
            Ok(CellId::Bytes(cursor.take(len)?.into()))
        }
        _ => Err(CodecError::InvalidDropCells {
            keyspace: keyspace.0,
        }),
    }
}

pub(super) fn member_checksum(
    id: BatchId,
    version: BatchVersion,
    descriptor: &RangeMemberDescriptor,
    keyspace: KeySpace,
    native: &[u8],
) -> MemberChecksum {
    let mut crc = crc32fast::Hasher::new();
    crc.update(CHECKSUM_DOMAIN);
    crc.update(&id.database);
    crc.update(&id.operation);
    crc.update(&version.epoch.to_be_bytes());
    crc.update(&version.sequence.to_be_bytes());
    crc.update(&0u32.to_be_bytes());
    crc.update(&descriptor.offset.to_be_bytes());
    crc.update(&descriptor.len.to_be_bytes());
    crc.update(&[keyspace.0]);
    crc.update(native);
    MemberChecksum(crc.finalize())
}

#[cfg(test)]
mod tests;
