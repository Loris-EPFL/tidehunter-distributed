//! Serde's byte-buffer path, preserving bincode 1 fixed-int sequence bytes.
//!
//! `Vec<u8>` otherwise invokes the serializer/deserializer for each byte. The
//! existing minibytes serde implementation uses serialize_bytes too; receive
//! buffers stay owned Vecs here for CompleteBatch::decode_owned.
//! https://serde.rs/impl-serialize.html#other-special-cases
use minibytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Clone)]
pub(super) enum WireBytes {
    Owned(Vec<u8>),
    Shared(Bytes),
}
impl WireBytes {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes.as_ref(),
        }
    }
    pub(super) fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes.to_vec(),
        }
    }
    pub(super) fn into_bytes(self) -> Bytes {
        match self {
            Self::Owned(bytes) => bytes.into(),
            Self::Shared(bytes) => bytes,
        }
    }
}
impl From<Vec<u8>> for WireBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}
impl From<Bytes> for WireBytes {
    fn from(bytes: Bytes) -> Self {
        Self::Shared(bytes)
    }
}
impl Serialize for WireBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.as_slice())
    }
}
impl<'de> Deserialize<'de> for WireBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ByteVisitor;
        impl serde::de::Visitor<'_> for ByteVisitor {
            type Value = WireBytes;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("owned byte buffer")
            }
            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<WireBytes, E> {
                Ok(WireBytes::Owned(bytes))
            }
            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<WireBytes, E> {
                Ok(WireBytes::Owned(bytes.to_vec()))
            }
        }
        // bincode charges the declared byte length against the decoder's limit
        // and available input before obtaining this owned buffer.
        deserializer.deserialize_byte_buf(ByteVisitor)
    }
}
