//! Restart durability guard for the recovered unsynced WAL suffix.
//!
//! After a writeback error the kernel can leave page-cache pages clean although
//! their content never reached the device. A process that restarts on the same
//! kernel reads those frames from the cache, and a later fsync succeeds because
//! nothing is dirty: it would certify bytes the device does not hold. The
//! recovery scan therefore writes every frame and skip marker it accepts above
//! the persisted root position back from the bytes it validated (the write
//! dirties the pages again), and syncs each written file before any root, seal
//! or barrier certifies the stream. No byte is read again, so a page evicted
//! after validation cannot change what becomes durable. A frame is written as
//! `CrcFrame::new(payload)`: its header is the length and checksum the scan
//! checked, so the bytes equal the validated frame. A skip marker is its 8-byte
//! header followed by the zeros the scan checked. Alignment padding between
//! frames carries no data and is not written. The cost is bounded by the
//! uncheckpointed suffix and is paid once per recovery, never on the write path.
//! Any write or sync error fails the recovery: the owner stops instead of
//! reporting progress it cannot prove.

use super::*;

/// Test hook: records every rewrite step on this thread and can inject one
/// fault. Recovery runs on the calling thread, so a thread-local hook is
/// deterministic.
#[cfg(test)]
pub(crate) mod hook;

#[cfg(test)]
mod tests;

/// Write-back state of one recovery scan: the WAL file written last.
#[derive(Default)]
pub(super) struct SuffixRewrite {
    /// Start offset and writable handle of the file written last; synced when
    /// the scan moves to another file and at the end.
    file: Option<(u64, File)>,
    #[cfg(test)]
    elapsed: Duration,
}

impl PartitionLog {
    /// Write back the validated group frame whose payload is `payload` at global
    /// WAL offset `offset`.
    pub(super) fn rewrite_validated_frame(
        &self,
        rewrite: &mut SuffixRewrite,
        offset: u64,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        let frame = CrcFrame::new(&payload.to_vec());
        self.rewrite_at(rewrite, offset, frame.as_ref())
    }

    /// Write back the validated skip marker at `offset` and its zero suffix up to
    /// `end`.
    pub(super) fn rewrite_skip_marker(
        &self,
        rewrite: &mut SuffixRewrite,
        offset: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        let len = usize::try_from(end - offset).map_err(|_| StorageError::Capacity)?;
        let mut bytes = vec![0; len];
        let marker = CrcFrame::skip_marker();
        let header = bytes
            .get_mut(..marker.as_ref().len())
            .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
        header.copy_from_slice(marker.as_ref());
        self.rewrite_at(rewrite, offset, &bytes)
    }

    /// Sync the last written file. Called before any root or seal install.
    pub(super) fn finish_suffix_rewrite(
        &self,
        mut rewrite: SuffixRewrite,
    ) -> Result<(), StorageError> {
        #[cfg(test)]
        let started = Instant::now();
        Self::sync_rewritten(&mut rewrite)?;
        #[cfg(test)]
        hook::finish(rewrite.elapsed + started.elapsed());
        Ok(())
    }

    fn sync_rewritten(rewrite: &mut SuffixRewrite) -> Result<(), StorageError> {
        if let Some((file_start, file)) = rewrite.file.take() {
            #[cfg(test)]
            hook::sync(file_start)?;
            #[cfg(not(test))]
            let _ = file_start;
            file.sync_data()?;
        }
        Ok(())
    }

    fn rewrite_at(
        &self,
        rewrite: &mut SuffixRewrite,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        #[cfg(test)]
        let started = Instant::now();
        let file_bytes = self.config().wal_file_bytes;
        let file_start = offset / file_bytes * file_bytes;
        if rewrite
            .file
            .as_ref()
            .is_none_or(|(start, _)| *start != file_start)
        {
            Self::sync_rewritten(rewrite)?;
            let name = self.config().layout().wal_file_name(
                &self.path,
                crate::wal::position::WalFileId::from_u64(file_start / file_bytes),
            );
            let file = OpenOptions::new().write(true).open(name)?;
            rewrite.file = Some((file_start, file));
        }
        let (_, file) = rewrite
            .file
            .as_ref()
            .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
        #[cfg(test)]
        hook::write(file, offset - file_start, offset, bytes)?;
        file.write_all_at(bytes, offset - file_start)?;
        #[cfg(test)]
        {
            rewrite.elapsed += started.elapsed();
        }
        Ok(())
    }
}
