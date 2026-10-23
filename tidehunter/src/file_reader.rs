use minibytes::{Bytes, BytesOwner};
use std::alloc::{self, Layout};
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::ptr::NonNull;
use std::sync::Arc;

pub struct FileReader {
    file: Arc<File>,
    direct_io: bool,
}

impl FileReader {
    pub fn new(file: Arc<File>, direct_io: bool) -> Self {
        Self { file, direct_io }
    }

    pub fn read_exact_at(&self, pos: u64, len: usize) -> io::Result<Bytes> {
        let len_u64 = u64::try_from(len).map_err(|_| invalid_range())?;
        let end = pos.checked_add(len_u64).ok_or_else(invalid_range)?;
        if len == 0 {
            return Ok(Bytes::new());
        }

        if self.direct_io {
            let (read_range, map_range) = align_range(pos..end)?;
            let read_len =
                usize::try_from(read_range.end - read_range.start).map_err(|_| invalid_range())?;
            let mut buffer = AlignedBuffer::new(read_len)?;
            let mut bytes_read =
                read_direct_at(self.file.as_ref(), buffer.as_mut_slice(), read_range.start)?;
            if bytes_read < map_range.end && bytes_read != 0 {
                // A short read may stop before the requested bytes even when
                // the file has more data. Resume from an aligned overlapping
                // sector into aligned storage, merging only newly covered bytes.
                let mut scratch = AlignedBuffer::new(read_len)?;
                while bytes_read < map_range.end {
                    let overlap_start = align_down(bytes_read as u64, DIRECT_IO_ALIGNMENT) as usize;
                    let overlap = bytes_read - overlap_start;
                    let offset = read_range
                        .start
                        .checked_add(overlap_start as u64)
                        .ok_or_else(invalid_range)?;
                    let remaining = read_len - overlap_start;
                    let next = read_direct_at(
                        self.file.as_ref(),
                        &mut scratch.as_mut_slice()[..remaining],
                        offset,
                    )?;
                    if next <= overlap {
                        return Err(unexpected_eof());
                    }
                    let next_end = overlap_start + next;
                    buffer.as_mut_slice()[bytes_read..next_end]
                        .copy_from_slice(&scratch.as_ref()[overlap..next]);
                    bytes_read = next_end;
                }
            }
            if bytes_read < map_range.end {
                return Err(unexpected_eof());
            }
            Ok(Bytes::from_owner(buffer).slice(map_range))
        } else {
            let mut buffer = vec![0; len];
            self.file.read_exact_at(&mut buffer, pos)?;
            Ok(Bytes::from(buffer))
        }
    }
}

const DIRECT_BUFFER_ALIGNMENT: usize = 4 * 1024;

/// Owns a zero-initialized direct-I/O allocation with the exact layout needed
/// to release it. Once returned as immutable bytes, no mutable access remains.
struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(len: usize) -> io::Result<Self> {
        let layout =
            Layout::from_size_align(len, DIRECT_BUFFER_ALIGNMENT).map_err(|_| invalid_range())?;
        // SAFETY: this private constructor receives a nonzero read length and
        // the checked layout is valid. Initialize the allocation before exposing
        // it through FileExt's initialized byte-slice API.
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        // SAFETY: null was handled above and `layout` is nonzero because this
        // buffer is only allocated for a non-empty aligned read.
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Ok(Self { ptr, layout })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: allocation is live, initialized, uniquely borrowed here, and
        // `layout.size()` is the exact initialized byte length.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

// The allocation is uniquely mutable before publication and immutable after
// it becomes a Bytes owner; sharing the owner therefore cannot race mutation.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AsRef<[u8]> for AlignedBuffer {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the allocation remains live and fully initialized for its
        // lifetime, and consumers only receive immutable access.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl BytesOwner for AlignedBuffer {}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: this pointer was allocated with this exact non-zero layout
        // and has not been deallocated or transferred elsewhere.
        unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

fn invalid_range() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "file read range is too large")
}

fn read_direct_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    loop {
        match file.read_at(buffer, offset) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn unexpected_eof() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "failed to read requested file range",
    )
}

/// Aligns an arbitrary range for direct I/O.
///
/// Returns the aligned read range and the range that selects the requested
/// bytes from that read.
fn align_range(range: Range<u64>) -> io::Result<(Range<u64>, Range<usize>)> {
    let read_start = align_down(range.start, DIRECT_IO_ALIGNMENT);
    let read_end = checked_align_up(range.end, DIRECT_IO_ALIGNMENT).ok_or_else(invalid_range)?;
    let map_start = usize::try_from(range.start - read_start).map_err(|_| invalid_range())?;
    let range_len = usize::try_from(range.end - range.start).map_err(|_| invalid_range())?;
    let map_end = map_start.checked_add(range_len).ok_or_else(invalid_range)?;
    Ok((read_start..read_end, map_start..map_end))
}

const DEFAULT_ALIGNMENT: u64 = 8;
const DIRECT_IO_ALIGNMENT: u64 = 512;

pub(crate) const fn align_size(v: u64, direct_io: bool) -> u64 {
    if direct_io {
        align_up(v, DIRECT_IO_ALIGNMENT)
    } else {
        align_up(v, DEFAULT_ALIGNMENT)
    }
}

const fn align_down(v: u64, align: u64) -> u64 {
    v / align * align
}

const fn align_up(v: u64, align: u64) -> u64 {
    align_down(v + align - 1, align)
}

const fn checked_align_up(v: u64, align: u64) -> Option<u64> {
    match v.checked_add(align - 1) {
        Some(v) => Some(align_down(v, align)),
        None => None,
    }
}

#[doc(hidden)]
pub fn set_direct_options(options: &mut OpenOptions, direct_io: bool) {
    if direct_io {
        set_o_direct(options)
    }
}

#[cfg(all(unix, any(target_arch = "x86_64", target_arch = "aarch64")))]
fn set_o_direct(options: &mut OpenOptions) {
    {
        // O_DIRECT values are architecture-specific on Linux
        #[cfg(target_arch = "x86_64")]
        const O_DIRECT: i32 = 0x4000;

        #[cfg(target_arch = "aarch64")]
        const O_DIRECT: i32 = 0x10000;

        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(O_DIRECT);
    }
}

#[cfg(not(unix))]
fn set_o_direct(_options: &mut OpenOptions) {
    unimplemented!("set_o_direct not implemented non-unix systems");
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::fs::OpenOptions;
    use std::path::Path;

    #[test]
    fn test_align_range() {
        #[track_caller]
        fn assert_align(range: Range<u64>, expected_read: Range<u64>, expected_map: Range<usize>) {
            let (actual_read, actual_map) = align_range(range).unwrap();
            assert_eq!(actual_read, expected_read, "Read range mismatch");
            assert_eq!(actual_map, expected_map, "Map range mismatch");
        }

        assert_align(0..10, 0..512, 0..10);
        assert_align(0..512, 0..512, 0..512);
        assert_align(5..512, 0..512, 5..512);
        assert_align(5..20, 0..512, 5..20);

        assert_align(1024..1034, 1024..(1024 + 512), 0..10);
        assert_align(1024..(1024 + 512), 1024..(1024 + 512), 0..512);
        assert_align(1034..(1024 + 512), 1024..(1024 + 512), 10..512);
        assert_align(1034..1044, 1024..(1024 + 512), 10..20);

        assert!(align_range(u64::MAX - 10..u64::MAX).is_err());
    }

    #[test]
    fn aligned_bytes_owner_is_initialized_aligned_and_drops_with_its_layout() {
        let bytes = Bytes::from_owner(AlignedBuffer::new(512).unwrap());
        let owner = bytes.downcast_ref::<AlignedBuffer>().unwrap();

        assert_eq!(owner.as_ref(), &[0; 512]);
        assert_eq!(owner.ptr.as_ptr() as usize % DIRECT_BUFFER_ALIGNMENT, 0);
        assert_eq!(owner.layout.size(), 512);
        assert_eq!(owner.layout.align(), DIRECT_BUFFER_ALIGNMENT);
    }

    #[test]
    fn test_align_value() {
        const A: u64 = 512;
        assert_eq!(align_down(0, A), 0);
        assert_eq!(align_down(1, A), 0);
        assert_eq!(align_down(10, A), 0);
        assert_eq!(align_down(511, A), 0);
        assert_eq!(align_down(512, A), 512);
        assert_eq!(align_down(513, A), 512);
        assert_eq!(align_down(1023, A), 512);
        assert_eq!(align_down(1024, A), 1024);
        assert_eq!(align_down(1025, A), 1024);

        assert_eq!(align_up(0, A), 0);
        assert_eq!(align_up(1, A), 512);
        assert_eq!(align_up(10, A), 512);
        assert_eq!(align_up(511, A), 512);
        assert_eq!(align_up(512, A), 512);
        assert_eq!(align_up(513, A), 1024);
        assert_eq!(align_up(1023, A), 1024);
        assert_eq!(align_up(1024, A), 1024);
        assert_eq!(align_up(1025, A), 1024 + 512);
    }

    #[test]
    fn test_file_reader() {
        let dir = tempdir::TempDir::new("test_file_reader").unwrap();
        let path = dir.path().join("file");
        let mut buf = BytesMut::zeroed(12 * 1024);
        let mut writer = &mut buf[..];
        for i in 0..3 * 1024 {
            writer.put_u32(i);
        }
        std::fs::write(&path, &buf).unwrap();
        test_file_reader_impl(&path, false);
        test_file_reader_impl(&path, true);
    }

    #[test]
    fn read_at_non_aligned_eof_accepts_only_fully_present_ranges() {
        let dir = tempdir::TempDir::new("test_file_reader_partial_eof").unwrap();
        let path = dir.path().join("file");
        let expected: Vec<u8> = (0..513).map(|i| i as u8).collect();
        std::fs::write(&path, &expected).unwrap();

        for direct_io in [false, true] {
            let mut options = OpenOptions::new();
            options.read(true);
            set_direct_options(&mut options, direct_io);
            let file = options.open(&path).unwrap();
            let reader = FileReader::new(Arc::new(file), direct_io);

            assert_eq!(
                reader.read_exact_at(512, 1).unwrap().as_ref(),
                &[expected[512]]
            );
            assert_eq!(
                reader.read_exact_at(512, 2).unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
    }

    fn test_file_reader_impl(path: &Path, direct_io: bool) {
        println!("test_file_reader_impl direct_io: {direct_io}");
        let mut options = OpenOptions::new();
        options.read(true);
        set_direct_options(&mut options, direct_io);
        let file = options.open(path).unwrap();
        let reader = FileReader::new(Arc::new(file), direct_io);
        #[track_caller]
        fn test_read(reader: &FileReader, v: u32) {
            let pos = (v as u64) * 4;
            let buf = reader.read_exact_at(pos, 4).unwrap();
            let mut sbuf = [0u8; 4];
            sbuf.copy_from_slice(&buf);
            assert_eq!(u32::from_be_bytes(sbuf), v);
        }

        let expected = std::fs::read(path).unwrap();
        for (pos, len) in [(0, 0), (511, 2), (4095, 3), (512, 512), (12 * 1024, 0)] {
            let actual = reader.read_exact_at(pos, len).unwrap();
            assert_eq!(actual.as_ref(), &expected[pos as usize..pos as usize + len]);
        }

        assert!(reader.read_exact_at(12 * 1024 - 1, 2).is_err());
        assert_eq!(
            reader.read_exact_at(u64::MAX, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            reader.read_exact_at(12 * 1024 - 4, 4).unwrap().as_ref(),
            &expected[12 * 1024 - 4..]
        );

        let retained = reader.read_exact_at(4095, 3).unwrap();
        test_read(&reader, 0);
        test_read(&reader, 1);
        test_read(&reader, 15);
        test_read(&reader, 1024);
        test_read(&reader, 1025);
        test_read(&reader, 2048);
        test_read(&reader, 2047);
        test_read(&reader, 2049);
        test_read(&reader, 3 * 1024 - 1);
        drop(reader);
        assert_eq!(retained.as_ref(), &expected[4095..4098]);
    }
}
