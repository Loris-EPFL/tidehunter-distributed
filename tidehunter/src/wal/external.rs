//! Preparation shared by initial, lookahead and inline external WAL grants.
use super::position::WalFileId;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const ZERO_BYTES: usize = 1024 * 1024;
const ALIGNMENT: usize = 4096;

pub(super) struct ExternalPreparation {
    pub(super) prewrite: bool,
    pub(super) ready: AtomicU64,
    bytes: AtomicU64,
    ns: AtomicU64,
    files: AtomicU64,
    #[cfg(test)]
    fail_before_ready: std::sync::atomic::AtomicBool,
}

impl ExternalPreparation {
    pub(super) fn new(prewrite: bool) -> Self {
        Self {
            prewrite,
            ready: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            ns: AtomicU64::new(0),
            files: AtomicU64::new(0),
            #[cfg(test)]
            fail_before_ready: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) fn stats(&self) -> (u64, u64, u64) {
        // Relaxed: diagnostic counters do not authorize grants or file access.
        (
            self.bytes.load(Ordering::Relaxed),
            self.ns.load(Ordering::Relaxed),
            self.files.load(Ordering::Relaxed),
        )
    }

    /// The initial opener, then the sole mapper, owns preparation. No file may
    /// be granted or written before this succeeds. The marker is memory-only:
    /// reopening an ungranted file repeats zeroing instead of trusting its size.
    pub(super) fn prepare(
        &self,
        file: &File,
        id: WalFileId,
        len: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        // Acquire pairs with publication below; ready generations only increase.
        if self.ready.load(Ordering::Acquire) > id.0 {
            return Ok(());
        }
        if len == 0 || len > i64::MAX as u64 || file.metadata()?.len() > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid external WAL file size",
            ));
        }
        #[cfg(target_os = "linux")]
        {
            // SAFETY: file retains its descriptor and the checked nonnegative
            // range fits off_t. Only the opener or sole mapper grows this file.
            let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len as libc::off_t) };
            if result != 0 {
                return Err(io::Error::from_raw_os_error(result));
            }
        }
        #[cfg(not(target_os = "linux"))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "live NVMe-oF requires Linux",
        ));
        #[cfg(target_os = "linux")]
        {
            if self.prewrite {
                self.zero_and_sync(file, len, scratch)?;
            }
            #[cfg(test)]
            // Relaxed: deterministic fault injection, not an I/O ordering edge.
            if self.fail_before_ready.swap(false, Ordering::Relaxed) {
                return Err(io::Error::other("injected crash before prepared marker"));
            }
            // Release publishes successful preallocation, and (when enabled)
            // every zero write and its data barrier, before any readiness check.
            self.ready.fetch_max(id.0 + 1, Ordering::Release);
            Ok(())
        }
    }

    fn zero_and_sync(&self, file: &File, len: u64, scratch: &mut Vec<u8>) -> io::Result<()> {
        let started = Instant::now();
        // One reusable 1 MiB, 4096-aligned slice, without an unsafe allocator.
        if scratch.is_empty() {
            scratch.resize(ZERO_BYTES + ALIGNMENT - 1, 0);
        }
        let start = scratch.as_ptr().align_offset(ALIGNMENT);
        let zeros = &scratch[start..start + ZERO_BYTES];
        // Exactly ceil(file_bytes / 1 MiB) positional writes; never dirty an
        // already-granted file. The sole preparer publishes ready only below.
        for offset in (0..len).step_by(ZERO_BYTES) {
            let count = (len - offset).min(ZERO_BYTES as u64) as usize;
            file.write_all_at(&zeros[..count], offset)?;
            // Relaxed: bytes count successful work, including interrupted prepares.
            self.bytes.fetch_add(count as u64, Ordering::Relaxed);
        }
        file.sync_data()?;
        // Relaxed: elapsed preparation work is diagnostic, not a readiness proof.
        self.ns.fetch_add(
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        // Relaxed: completed zero-fill/data-sync operations are diagnostic only.
        self.files.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn file() -> (tempdir::TempDir, File) {
        let directory = tempdir::TempDir::new("wal-prewrite").unwrap();
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(directory.path().join("wal"))
            .unwrap();
        (directory, file)
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct Extent {
        logical: u64,
        physical: u64,
        length: u64,
        reserved64: [u64; 2],
        flags: u32,
        reserved: [u32; 3],
    }

    #[repr(C)]
    struct Fiemap {
        start: u64,
        length: u64,
        flags: u32,
        mapped: u32,
        count: u32,
        reserved: u32,
        extents: [Extent; 256],
    }

    fn assert_written_extents(file: &File, len: u64) {
        const FS_IOC_FIEMAP: libc::c_ulong = 0xc020_660b;
        const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x800;
        let mut map = Fiemap {
            start: 0,
            length: len,
            flags: 1,
            mapped: 0,
            count: 256,
            reserved: 0,
            extents: [Extent::default(); 256],
        };
        // SAFETY: repr(C) header and its contiguous extent array match the Linux
        // fiemap ABI; count is the writable array capacity. file stays open.
        let status = unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FIEMAP, &mut map) };
        assert_eq!(
            status,
            0,
            "test filesystem must support FIEMAP: {}",
            io::Error::last_os_error()
        );
        assert!(
            map.mapped > 0 && map.mapped < map.count,
            "bounded FIEMAP buffer exhausted or empty"
        );
        let mut covered = 0;
        for extent in &map.extents[..map.mapped as usize] {
            assert_eq!(
                extent.flags & FIEMAP_EXTENT_UNWRITTEN,
                0,
                "prewritten extent is still unwritten"
            );
            assert_eq!(extent.logical, covered, "hole in prewritten file");
            covered += extent.length;
        }
        assert!(covered >= len);
    }

    #[test]
    fn prewrite_zeroes_and_initializes_every_extent() {
        let (_directory, file) = file();
        let len = 2 * ZERO_BYTES as u64 + ALIGNMENT as u64;
        let preparation = ExternalPreparation::new(true);
        let mut scratch = Vec::new();
        preparation
            .prepare(&file, WalFileId(0), len, &mut scratch)
            .unwrap();
        assert_eq!(preparation.ready.load(Ordering::Acquire), 1);
        let mut bytes = vec![1; len as usize];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 0));
        assert_written_extents(&file, len);
        assert_eq!(preparation.stats().0, len);
        assert!(preparation.stats().1 > 0);
        preparation
            .prepare(&file, WalFileId(0), len, &mut scratch)
            .unwrap();
        assert_eq!(
            preparation.stats().0,
            len,
            "ready files must never be zeroed again"
        );
    }

    #[test]
    fn disabled_prewrite_preserves_existing_file_bytes() {
        let (_directory, file) = file();
        file.write_all_at(&[7; 4096], 0).unwrap();
        let preparation = ExternalPreparation::new(false);
        let mut scratch = Vec::new();
        preparation
            .prepare(&file, WalFileId(0), 8192, &mut scratch)
            .unwrap();
        let mut bytes = [0; 4096];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [7; 4096]);
        assert!(scratch.is_empty());
        assert_eq!(preparation.stats(), (0, 0, 0));
    }

    #[test]
    fn crash_before_ready_repeats_prewrite_on_restart() {
        let (_directory, file) = file();
        let preparation = ExternalPreparation::new(true);
        // Relaxed: only this test thread arms the injected crash.
        preparation.fail_before_ready.store(true, Ordering::Relaxed);
        assert!(
            preparation
                .prepare(&file, WalFileId(0), 8192, &mut Vec::new())
                .is_err()
        );
        assert_eq!(preparation.ready.load(Ordering::Acquire), 0);
        assert_eq!(preparation.stats().0, 8192);
        // An ungranted file's surviving size/content is not a prepared marker.
        file.write_all_at(&[9; 4096], 0).unwrap();
        let restarted = ExternalPreparation::new(true);
        assert_eq!(restarted.ready.load(Ordering::Acquire), 0);
        restarted
            .prepare(&file, WalFileId(0), 8192, &mut Vec::new())
            .unwrap();
        let mut bytes = [1; 8192];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [0; 8192]);
        assert_eq!(restarted.stats().0, 8192);
        assert_eq!(restarted.ready.load(Ordering::Acquire), 1);
    }
}
