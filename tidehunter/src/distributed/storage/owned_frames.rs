//! Owned-buffer frame reader for the on-disk-history recovery scan.
//!
//! The restart durability guard writes back frames above the durable point from
//! the bytes recovery validated. Those bytes must be a private snapshot: a frame
//! checked through the retained mapping and copied later could change if a clean
//! page is evicted in between, and the copy would then get a fresh valid CRC.
//! Writing a file while a retained mapping of it exists is also forbidden (see
//! `create_retained_map`). This reader therefore reads each frame with `pread`
//! into an owned buffer, checks it with the same `CrcFrame` rules as
//! `WalIterator`, and hands back that buffer; recovery decodes, registers and
//! rewrites the same bytes. It holds no mapping.

use super::*;
use crate::crc::CrcReadError;

/// Reads the frames of one fragment the way `WalIterator::next` does, from owned
/// buffers. Errors keep the iterator's classes: `EndOfWal` for a missing
/// fragment, `Crc` for a short or invalid frame, `Io` for an operating error.
pub(super) struct OwnedFrames {
    file: File,
    #[cfg(test)]
    path: PathBuf,
    layout: WalLayout,
    position: u64,
    fragment_start: u64,
    fragment_end: u64,
    /// Existing bytes of this fragment in its file (the iterator maps no more).
    fragment_len: u64,
    wal_file_bytes: u64,
}

impl OwnedFrames {
    pub(super) fn new(
        path: &Path,
        config: &PartitionConfig,
        position: u64,
    ) -> Result<Self, WalError> {
        let layout = config.layout();
        let wal_file_bytes = config.wal_file_bytes;
        let fragment_start = position / config.fragment_bytes * config.fragment_bytes;
        let name = layout.wal_file_name(
            path,
            crate::wal::position::WalFileId::from_u64(position / wal_file_bytes),
        );
        let file = match File::open(&name) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(WalError::EndOfWal);
            }
            Err(error) => return Err(WalError::Io(error)),
        };
        let fragment_len = file
            .metadata()?
            .len()
            .saturating_sub(fragment_start % wal_file_bytes)
            .min(config.fragment_bytes);
        if fragment_len == 0 {
            return Err(WalError::EndOfWal);
        }
        Ok(Self {
            file,
            #[cfg(test)]
            path: name,
            layout,
            position,
            fragment_start,
            fragment_end: fragment_start + config.fragment_bytes,
            fragment_len,
            wal_file_bytes,
        })
    }

    /// The next frame: its position, its validated payload and the whole
    /// validated frame (header included), both views of one owned buffer. A skip
    /// marker moves to the next fragment; the returned position then lies at the
    /// fragment end, where the caller stops this fragment.
    pub(super) fn next(
        &mut self,
    ) -> Result<(WalPosition, minibytes::Bytes, minibytes::Bytes), WalError> {
        let header_len = CrcFrame::CRC_HEADER_LENGTH as u64;
        let in_fragment = self.position - self.fragment_start;
        if in_fragment + header_len > self.fragment_len {
            return Err(CrcReadError::OutOfBoundsHeader.into());
        }
        let mut header = [0u8; CrcFrame::CRC_HEADER_LENGTH];
        self.file
            .read_exact_at(&mut header, self.position % self.wal_file_bytes)?;
        if header == [0xff; CrcFrame::CRC_HEADER_LENGTH] {
            self.position = self.fragment_end;
            let empty = minibytes::Bytes::new();
            return Ok((WalPosition::new(self.fragment_end, 0), empty.clone(), empty));
        }
        let len = u64::from(u32::from_be_bytes([
            header[0], header[1], header[2], header[3],
        ]));
        if in_fragment + header_len + len > self.fragment_len {
            return Err(CrcReadError::OutOfBoundsBody(len as usize).into());
        }
        let frame_len = usize::try_from(header_len + len)
            .map_err(|_| WalError::Io(io::Error::other("WAL frame length exceeds memory")))?;
        let mut frame = vec![0u8; frame_len];
        self.file
            .read_exact_at(&mut frame, self.position % self.wal_file_bytes)?;
        let frame: minibytes::Bytes = frame.into();
        let payload = CrcFrame::read_from_bytes(&frame, 0)?;
        #[cfg(test)]
        wal_replay::suffix_rewrite_hook::after_validation(
            &self.path,
            self.position % self.wal_file_bytes,
            frame.len(),
        )?;
        let position = WalPosition::new(self.position, frame_len as u32);
        self.position += self.layout.align(frame_len as u64);
        Ok((position, payload, frame))
    }
}

/// The frame reader of one fragment in the on-disk recovery scan: the retained
/// mapping below the durable point, owned buffers where the guard may rewrite.
pub(super) enum FrameSource {
    Mapped(crate::wal::WalIterator),
    Owned(OwnedFrames),
}

impl FrameSource {
    /// Position and payload of the next frame, plus the whole validated frame
    /// when it comes from an owned buffer.
    pub(super) fn next(
        &mut self,
    ) -> Result<(WalPosition, minibytes::Bytes, Option<minibytes::Bytes>), WalError> {
        match self {
            Self::Mapped(scan) => scan
                .next()
                .map(|(position, payload)| (position, payload, None)),
            Self::Owned(frames) => frames
                .next()
                .map(|(position, payload, frame)| (position, payload, Some(frame))),
        }
    }
}
