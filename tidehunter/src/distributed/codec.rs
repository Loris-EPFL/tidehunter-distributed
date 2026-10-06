//! Complete authority record using native `WalEntry` record/tombstone encoding.
//!
//! Tidehunter's atomic-batch write flow requires all-or-none replay; unlike native BatchStart's
//! following frames, this experimental path wraps the whole batch in one native
//! CRC frame. The October 6 design §7.1 requires semantic validation *before*
//! that frame can exist. CORFU's multi-object entry motivates the authority
//! boundary, but does not supply Tidehunter's index or relocation semantics.

use super::types::{BatchId, BatchVersion, MutationVersion, PayloadDigest};
use crate::crc::IntoBytesFixed;
use crate::db::{MAX_KEY_LEN, WalEntry};
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace};
use blake2::{Blake2b, Digest, digest::consts::U32};
use bytes::{BufMut, BytesMut};
use minibytes::Bytes;
use std::fmt;

const MAGIC: &[u8; 8] = b"THBATCH1";
const FORMAT_VERSION: u16 = 1;
// magic, format, reserved, stable identity, original version, count, digest.
const DIGEST_OFFSET: usize = 8 + 2 + 2 + 32 + 16 + 4;
const HEADER_LEN: usize = DIGEST_OFFSET + 32;
const NATIVE_FRAME_MAX_EXCLUSIVE: usize = 1 << 30;
const NATIVE_CRC_HEADER_LEN: usize = 8;
const NATIVE_BATCH_MAX_EXCLUSIVE: u32 = 1_000_000;

/// Explicit admission bounds for the experimental record API only.
/// Native `Db` and its accepted batch sizes are unaffected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BatchLimits {
    pub max_operations: u32,
    /// Complete encoded payload bytes, excluding the outer native CRC header.
    pub max_encoded_bytes: usize,
}

impl BatchLimits {
    pub fn validate(self) -> Result<(), CodecError> {
        if self.max_operations == 0
            || self.max_operations >= NATIVE_BATCH_MAX_EXCLUSIVE
            || self.max_encoded_bytes < HEADER_LEN + 6
            || self.max_encoded_bytes >= NATIVE_FRAME_MAX_EXCLUSIVE - NATIVE_CRC_HEADER_LEN
        {
            return Err(CodecError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put {
        keyspace: KeySpace,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        keyspace: KeySpace,
        key: Bytes,
    },
}

impl Mutation {
    pub fn keyspace(&self) -> KeySpace {
        match self {
            Self::Put { keyspace, .. } | Self::Delete { keyspace, .. } => *keyspace,
        }
    }

    pub fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    fn native_entry(&self) -> WalEntry {
        match self {
            Self::Put {
                keyspace,
                key,
                value,
            } => WalEntry::Record(*keyspace, key.clone(), value.clone(), false),
            Self::Delete { keyspace, key } => WalEntry::Remove(*keyspace, key.clone()),
        }
    }
}

/// A validated, immutable canonical record. Construction does not append or
/// establish authority: storage must still authenticate, fence and admit it.
#[derive(Clone, Debug)]
pub struct CompleteBatch {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    mutations: Vec<Mutation>,
    encoded: Bytes,
}

impl CompleteBatch {
    pub fn new(
        shape: &KeyShape,
        id: BatchId,
        version: BatchVersion,
        mutations: Vec<Mutation>,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        limits.validate()?;
        check_count(mutations.len(), limits)?;
        let mut size = HEADER_LEN;
        // Validate every operation and total size before allocating encoded bytes.
        for mutation in &mutations {
            validate_mutation(shape, mutation)?;
            size = size
                .checked_add(4)
                .and_then(|s| s.checked_add(mutation.native_entry().len()))
                .ok_or(CodecError::TooLarge)?;
            if size > limits.max_encoded_bytes {
                return Err(CodecError::TooLarge);
            }
        }
        let mut encoded = BytesMut::with_capacity(size);
        encoded.put_slice(MAGIC);
        encoded.put_u16(FORMAT_VERSION);
        encoded.put_u16(0); // reserved, must remain zero for canonical encoding
        encoded.put_slice(&id.database);
        encoded.put_slice(&id.operation);
        encoded.put_u64(version.epoch);
        encoded.put_u64(version.sequence);
        encoded.put_u32(mutations.len() as u32);
        encoded.put_slice(&[0; 32]);
        for mutation in &mutations {
            let entry = mutation.native_entry();
            encoded.put_u32(entry.len() as u32);
            entry.write_into_bytes(&mut encoded);
        }
        let digest = record_digest(&encoded);
        encoded[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
        Ok(Self {
            id,
            version,
            digest,
            mutations,
            encoded: encoded.freeze().into(),
        })
    }

    /// Fallible, bounded parser for disk/network bytes. Does not call native
    /// `WalEntry::from_bytes`, whose trusted-local parser can panic on bad input.
    /// Each mutation shares the immutable input buffer; values are not copied.
    pub fn decode(
        shape: &KeyShape,
        encoded: Bytes,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        limits.validate()?;
        if encoded.len() > limits.max_encoded_bytes {
            return Err(CodecError::TooLarge);
        }
        if encoded.len() < HEADER_LEN {
            return Err(CodecError::Truncated);
        }
        let mut cursor = Cursor::new(&encoded);
        if cursor.take(8)? != MAGIC {
            return Err(CodecError::InvalidFormat);
        }
        if cursor.u16()? != FORMAT_VERSION || cursor.u16()? != 0 {
            return Err(CodecError::InvalidFormat);
        }
        let id = BatchId {
            database: cursor.take(16)?.try_into().expect("checked 16-byte slice"),
            operation: cursor.take(16)?.try_into().expect("checked 16-byte slice"),
        };
        let version = BatchVersion {
            epoch: cursor.u64()?,
            sequence: cursor.u64()?,
        };
        let count = cursor.u32()? as usize;
        check_count(count, limits)?;
        // Every entry needs a length plus at least its native tag and keyspace.
        // Check this before allocating a count-sized vector supplied by a peer.
        if count > (encoded.len() - HEADER_LEN) / 6 {
            return Err(CodecError::Truncated);
        }
        let digest = PayloadDigest(cursor.take(32)?.try_into().expect("checked 32-byte slice"));
        if digest != record_digest(&encoded) {
            return Err(CodecError::DigestMismatch);
        }
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            let len = cursor.u32()? as usize;
            let start = cursor.position;
            let raw = cursor.take(len)?;
            let mut native = Cursor::new(raw);
            let tag = native.u8()?;
            let keyspace = KeySpace(native.u8()?);
            let mutation = match tag {
                1 => {
                    let key_len = native.u16()? as usize;
                    native.take(key_len)?;
                    Mutation::Put {
                        keyspace,
                        key: encoded.slice(start + 4..start + 4 + key_len),
                        value: encoded.slice(start + 4 + key_len..start + len),
                    }
                }
                3 => Mutation::Delete {
                    keyspace,
                    key: encoded.slice(start + 2..start + len),
                },
                // Relocation, control/index entries and nested batches cannot
                // become newly committed user mutations through this interface.
                _ => return Err(CodecError::InvalidMutation),
            };
            validate_mutation(shape, &mutation)?;
            mutations.push(mutation);
        }
        if cursor.position != encoded.len() {
            return Err(CodecError::TrailingBytes);
        }
        Ok(Self {
            id,
            version,
            digest,
            mutations,
            encoded,
        })
    }

    pub fn id(&self) -> BatchId {
        self.id
    }
    pub fn version(&self) -> BatchVersion {
        self.version
    }
    pub fn digest(&self) -> PayloadDigest {
        self.digest
    }
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }
    pub fn encoded(&self) -> &Bytes {
        &self.encoded
    }

    pub fn mutation_version(&self, operation_index: usize) -> Option<MutationVersion> {
        (operation_index < self.mutations.len()).then_some(MutationVersion {
            batch: self.version,
            operation_index: operation_index as u32,
        })
    }
}

impl IntoBytesFixed for CompleteBatch {
    fn len(&self) -> usize {
        self.encoded.len()
    }
    fn write_into_bytes(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.encoded);
    }
}

fn check_count(count: usize, limits: BatchLimits) -> Result<(), CodecError> {
    if count == 0 || count > limits.max_operations as usize {
        Err(CodecError::InvalidOperationCount)
    } else {
        Ok(())
    }
}

fn validate_mutation(shape: &KeyShape, mutation: &Mutation) -> Result<(), CodecError> {
    let keyspace = mutation.keyspace();
    let Some(descriptor) = shape
        .iter_ks()
        .find(|descriptor| descriptor.id() == keyspace)
    else {
        return Err(CodecError::UnknownKeySpace(keyspace.0));
    };
    if descriptor.destroyed() {
        return Err(CodecError::DestroyedKeySpace(keyspace.0));
    }
    let len = mutation.key().len();
    let valid = len <= MAX_KEY_LEN
        && match descriptor.key_indexing() {
            KeyIndexing::Fixed(expected) | KeyIndexing::Reduction(expected, _) => len == *expected,
            KeyIndexing::Hash | KeyIndexing::VariableLength => true,
        };
    if !valid {
        return Err(CodecError::InvalidKeyLength {
            keyspace: keyspace.0,
            actual: len,
        });
    }
    Ok(())
}

fn record_digest(encoded: &[u8]) -> PayloadDigest {
    let mut hash = Blake2b::<U32>::new();
    hash.update(b"tidehunter-complete-batch-v1\0");
    hash.update(&encoded[..DIGEST_OFFSET]);
    hash.update(&encoded[HEADER_LEN..]);
    PayloadDigest(hash.finalize().into())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    InvalidLimits,
    InvalidOperationCount,
    TooLarge,
    Truncated,
    InvalidFormat,
    InvalidMutation,
    DigestMismatch,
    TrailingBytes,
    UnknownKeySpace(u8),
    DestroyedKeySpace(u8),
    InvalidKeyLength { keyspace: u8, actual: usize },
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid complete batch: {self:?}")
    }
}
impl std::error::Error for CodecError {}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(CodecError::Truncated)?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecError::Truncated)?;
        self.position = end;
        Ok(result)
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("checked slice"),
        ))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("checked slice"),
        ))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("checked slice"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::CrcFrame;
    use crate::key_shape::{KeySpaceConfig, KeyType};

    fn shape() -> KeyShape {
        KeyShape::new_single(4, 1, KeyType::uniform(1))
    }

    fn limits() -> BatchLimits {
        BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 4096,
        }
    }

    fn identity() -> BatchId {
        BatchId {
            database: [1; 16],
            operation: [2; 16],
        }
    }

    fn version() -> BatchVersion {
        BatchVersion {
            epoch: 7,
            sequence: 11,
        }
    }

    fn put(value: &'static [u8]) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: value.into(),
        }
    }

    fn batch(mutations: Vec<Mutation>) -> CompleteBatch {
        CompleteBatch::new(&shape(), identity(), version(), mutations, limits()).unwrap()
    }

    fn with_valid_digest(mut bytes: Vec<u8>) -> Bytes {
        let digest = record_digest(&bytes);
        bytes[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
        bytes.into()
    }

    #[test]
    fn native_frame_roundtrip_preserves_complete_batch_and_duplicate_key_order() {
        let original = batch(vec![
            put(b"first"),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
            },
            put(b"last"),
        ]);
        let framed = CrcFrame::new(&original);
        let payload = CrcFrame::read_from_slice(framed.as_ref(), 0).unwrap();
        let decoded = CompleteBatch::decode(&shape(), payload.to_vec().into(), limits()).unwrap();
        assert_eq!(original.mutations(), decoded.mutations());
        assert_eq!(original.encoded(), decoded.encoded());
        assert_eq!(original.digest(), decoded.digest());
        assert!(decoded.mutation_version(0) < decoded.mutation_version(2));
        assert_eq!(decoded.mutation_version(3), None);
        // The envelope retains native record/tombstone bytes, rather than a
        // second serialization with subtly different key/value behavior.
        let mut cursor = Cursor::new(&payload[HEADER_LEN..]);
        for mutation in original.mutations() {
            let len = cursor.u32().unwrap() as usize;
            let native = WalEntry::from_bytes(cursor.take(len).unwrap().to_vec().into());
            match (native, mutation) {
                (
                    WalEntry::Record(ks, key, value, false),
                    Mutation::Put {
                        keyspace,
                        key: k,
                        value: v,
                    },
                ) => {
                    assert_eq!((ks, key, value), (*keyspace, k.clone(), v.clone()));
                }
                (WalEntry::Remove(ks, key), Mutation::Delete { keyspace, key: k }) => {
                    assert_eq!((ks, key), (*keyspace, k.clone()));
                }
                other => panic!("unexpected native record: {other:?}"),
            }
        }
    }

    #[test]
    fn retry_digest_binds_identity_original_version_and_order() {
        let first = batch(vec![put(b"a"), put(b"b")]);
        let retry = batch(vec![put(b"a"), put(b"b")]);
        assert_eq!(first.digest(), retry.digest());
        let mut id = identity();
        id.operation[0] ^= 1;
        let other_id = CompleteBatch::new(
            &shape(),
            id,
            version(),
            vec![put(b"a"), put(b"b")],
            limits(),
        )
        .unwrap();
        assert_ne!(first.digest(), other_id.digest());
        let other_version = CompleteBatch::new(
            &shape(),
            identity(),
            BatchVersion {
                epoch: 8,
                sequence: 11,
            },
            vec![put(b"a"), put(b"b")],
            limits(),
        )
        .unwrap();
        assert_ne!(first.digest(), other_version.digest());
        assert_ne!(first.digest(), batch(vec![put(b"b"), put(b"a")]).digest());
    }

    #[test]
    fn malformed_or_oversized_input_never_reaches_native_trusted_parser() {
        let original = batch(vec![put(b"some value")]);
        for len in 0..original.encoded().len() {
            assert!(
                CompleteBatch::decode(&shape(), original.encoded().slice(..len), limits()).is_err()
            );
        }
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::Truncated)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes[DIGEST_OFFSET - 4..DIGEST_OFFSET].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), bytes.into(), limits()),
            Err(CodecError::InvalidOperationCount)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN + 4] = 5; // Native relocation record is not a user write.
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::InvalidMutation)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN + 6..HEADER_LEN + 8].copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::Truncated)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes.push(0);
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::TrailingBytes)
        ));
    }

    #[test]
    fn semantics_are_validated_even_when_digest_and_native_crc_are_valid() {
        let original = batch(vec![put(b"value")]);
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN + 5] = 255;
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::UnknownKeySpace(255))
        ));
        let incompatible_shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        assert!(matches!(
            CompleteBatch::decode(&incompatible_shape, original.encoded().clone(), limits()),
            Err(CodecError::InvalidKeyLength { .. })
        ));
        let destroyed_shape = shape();
        destroyed_shape.iter_ks().next().unwrap().mark_destroyed();
        assert!(matches!(
            CompleteBatch::decode(&destroyed_shape, original.encoded().clone(), limits()),
            Err(CodecError::DestroyedKeySpace(0))
        ));
    }

    #[test]
    fn variable_keys_and_empty_values_preserve_native_semantics() {
        let variable_shape = KeyShape::new_single_config_indexing(
            KeyIndexing::VariableLength,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::default(),
        );
        let mutations = vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: Bytes::new(),
            value: Bytes::new(),
        }];
        let batch = CompleteBatch::new(
            &variable_shape,
            identity(),
            version(),
            mutations.clone(),
            limits(),
        )
        .unwrap();
        assert_eq!(
            CompleteBatch::decode(&variable_shape, batch.encoded().clone(), limits())
                .unwrap()
                .mutations(),
            &mutations
        );
    }

    #[test]
    fn limits_reject_before_encoded_record_can_exist() {
        assert!(matches!(
            CompleteBatch::new(&shape(), identity(), version(), vec![], limits()),
            Err(CodecError::InvalidOperationCount)
        ));
        let mut limit = limits();
        limit.max_operations = 1;
        assert!(matches!(
            CompleteBatch::new(
                &shape(),
                identity(),
                version(),
                vec![put(b"a"), put(b"b")],
                limit
            ),
            Err(CodecError::InvalidOperationCount)
        ));
        limit = limits();
        limit.max_encoded_bytes = HEADER_LEN + 6;
        assert!(matches!(
            CompleteBatch::new(&shape(), identity(), version(), vec![put(b"a")], limit),
            Err(CodecError::TooLarge)
        ));
        let invalid = Mutation::Put {
            keyspace: KeySpace(0),
            key: b"wrong length"[..].into(),
            value: Bytes::new(),
        };
        assert!(matches!(
            CompleteBatch::new(&shape(), identity(), version(), vec![invalid], limits()),
            Err(CodecError::InvalidKeyLength { .. })
        ));
    }
}
