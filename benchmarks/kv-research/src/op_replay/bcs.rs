//! The BCS subset used by the operation trace payloads (OPS-FORMAT.md):
//! fixed-width little-endian integers, `bool`, ULEB128 lengths and enum tags,
//! byte vectors, UTF-8 strings, options and fixed 32-byte hashes.
//! Decoding is strict: non-canonical ULEB128, invalid booleans, invalid UTF-8,
//! truncation and trailing bytes are errors, as in the reference `bcs` crate.

use crate::backend::BackendResult;

/// BCS bounds sequence lengths and enum tags to `u32` values below 2^31.
const MAX_ULEB: u64 = (1 << 31) - 1;

pub(super) struct Decoder<'a> {
    bytes: &'a [u8],
}

impl<'a> Decoder<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(super) fn finish(self) -> BackendResult<()> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(format!("{} trailing BCS payload bytes", self.bytes.len()).into())
        }
    }

    fn take(&mut self, len: usize) -> BackendResult<&'a [u8]> {
        if self.bytes.len() < len {
            return Err(format!(
                "truncated BCS payload: need {len} bytes, have {}",
                self.bytes.len()
            )
            .into());
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    pub(super) fn u8(&mut self) -> BackendResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn bool(&mut self) -> BackendResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(format!("invalid BCS bool {other}").into()),
        }
    }

    pub(super) fn u32(&mut self) -> BackendResult<u32> {
        let mut raw = [0; 4];
        raw.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(raw))
    }

    pub(super) fn u64(&mut self) -> BackendResult<u64> {
        let mut raw = [0; 8];
        raw.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(raw))
    }

    pub(super) fn hash(&mut self) -> BackendResult<[u8; 32]> {
        let mut raw = [0; 32];
        raw.copy_from_slice(self.take(32)?);
        Ok(raw)
    }

    /// Canonical ULEB128 below 2^31: a sequence length or an enum tag.
    pub(super) fn uleb(&mut self) -> BackendResult<usize> {
        let mut value = 0u64;
        // Five 7-bit groups cover 2^31; a sixth byte can never be canonical.
        for shift in (0..35).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                if shift != 0 && byte == 0 {
                    return Err("non-canonical BCS ULEB128".into());
                }
                if value > MAX_ULEB {
                    return Err(format!("BCS ULEB128 {value} exceeds 2^31 - 1").into());
                }
                return usize::try_from(value).map_err(|_| "BCS ULEB128 exceeds usize".into());
            }
        }
        Err("BCS ULEB128 longer than five bytes".into())
    }

    /// A sequence length, bounded by the remaining payload so a corrupt length
    /// cannot reserve more memory than the frame holds (each element is at
    /// least one byte for every sequence type in the trace).
    pub(super) fn len(&mut self) -> BackendResult<usize> {
        let len = self.uleb()?;
        if len > self.bytes.len() {
            return Err(format!(
                "BCS sequence length {len} exceeds the {} remaining payload bytes",
                self.bytes.len()
            )
            .into());
        }
        Ok(len)
    }

    pub(super) fn bytes(&mut self) -> BackendResult<Vec<u8>> {
        let len = self.len()?;
        Ok(self.take(len)?.to_vec())
    }

    pub(super) fn string(&mut self) -> BackendResult<String> {
        let len = self.len()?;
        let raw = self.take(len)?;
        Ok(std::str::from_utf8(raw)
            .map_err(|error| format!("invalid UTF-8 in BCS string: {error}"))?
            .to_owned())
    }

    pub(super) fn option<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> BackendResult<T>,
    ) -> BackendResult<Option<T>> {
        match self.u8()? {
            0 => Ok(None),
            1 => item(self).map(Some),
            other => Err(format!("invalid BCS option tag {other}").into()),
        }
    }

    pub(super) fn vec<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> BackendResult<T>,
    ) -> BackendResult<Vec<T>> {
        let len = self.len()?;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(item(self)?);
        }
        Ok(items)
    }
}

#[cfg(test)]
mod tests;
