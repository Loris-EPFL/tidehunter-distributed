//! Linux-only benchmark evidence. Never clears the machine-wide page cache.
//! Advisory eviction is not proof of cold I/O: callers must record residency
//! and physical read-byte deltas for the measured interval.
use serde::{Deserialize, Serialize};
use std::os::fd::AsRawFd;
use std::{fs, io};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ReadEvidence {
    pub pid: u32,
    pub start_ticks: u64,
    pub process_boot_id: Option<[u8; 16]>,
    pub read_bytes: u64,
    pub minor_faults: u64,
    pub major_faults: u64,
    pub file_bytes: u64,
    pub resident_bytes: u64,
}

/// Bounded process-wide snapshots for opt-in benchmark diagnostics only.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ProcessResources {
    pub pid: u32,
    pub process_start_ticks: Option<u64>,
    pub rss_bytes: u64,
    pub peak_rss_bytes: u64,
    pub user_cpu_ns: u64,
    pub system_cpu_ns: u64,
    /// Linux process-accounted bytes; `None` means the observation was unavailable.
    pub accounted_process_read_bytes: Option<u64>,
    /// Linux process-accounted bytes, not completed device writes.
    pub accounted_process_write_bytes: Option<u64>,
}

impl ProcessResources {
    /// # Errors
    /// Rejects missing, oversized or malformed Linux process accounting.
    pub fn process() -> io::Result<Self> {
        use std::io::Read;
        const LIMIT: u64 = 64 * 1024;
        let mut status = String::new();
        fs::File::open("/proc/self/status")?
            .take(LIMIT + 1)
            .read_to_string(&mut status)?;
        if status.len() as u64 > LIMIT {
            return Err(io::Error::other("process status exceeds diagnostic bound"));
        }
        // SAFETY: rusage is an output-only C structure and getrusage writes
        // within it. RUSAGE_SELF includes every thread, not only the caller.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let cpu_ns = |time: libc::timeval| -> io::Result<u64> {
            let seconds = u64::try_from(time.tv_sec).map_err(io::Error::other)?;
            let micros = u64::try_from(time.tv_usec).map_err(io::Error::other)?;
            seconds
                .checked_mul(1_000_000_000)
                .and_then(|n| n.checked_add(micros.checked_mul(1000)?))
                .ok_or_else(|| io::Error::other("process CPU counter overflow"))
        };
        Ok(Self {
            pid: std::process::id(),
            process_start_ticks: process_start_ticks().ok(),
            rss_bytes: status_bytes(&status, "VmRSS:")?,
            peak_rss_bytes: status_bytes(&status, "VmHWM:")?,
            user_cpu_ns: cpu_ns(usage.ru_utime)?,
            system_cpu_ns: cpu_ns(usage.ru_stime)?,
            accounted_process_read_bytes: process_io_counter("read_bytes:"),
            accounted_process_write_bytes: process_io_counter("write_bytes:"),
        })
    }
}
fn process_start_ticks() -> io::Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let (_, tail) = stat
        .rsplit_once(") ")
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    tail.split_whitespace()
        .nth(19)
        .ok_or_else(|| io::Error::other("missing process start time"))?
        .parse()
        .map_err(io::Error::other)
}

fn process_io_counter(name: &str) -> Option<u64> {
    let io = fs::read_to_string("/proc/self/io").ok()?;
    parse_process_io_counter(&io, name)
}

fn parse_process_io_counter(io: &str, name: &str) -> Option<u64> {
    io.lines()
        .find_map(|line| line.strip_prefix(name))?
        .trim()
        .parse()
        .ok()
}
fn process_boot_id() -> Option<[u8; 16]> {
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    parse_process_boot_id(&boot_id)
}

fn parse_process_boot_id(value: &str) -> Option<[u8; 16]> {
    let mut bytes = [0; 16];
    let mut nibble = None;
    let mut index = 0;
    for byte in value
        .bytes()
        .filter(|byte| *byte != b'-' && !byte.is_ascii_whitespace())
    {
        let value = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        if let Some(high) = nibble.take() {
            let slot = bytes.get_mut(index)?;
            *slot = high << 4 | value;
            index += 1;
        } else {
            nibble = Some(value);
        }
    }
    (index == bytes.len() && nibble.is_none()).then_some(bytes)
}

fn status_bytes(status: &str, name: &str) -> io::Result<u64> {
    let line = status
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .ok_or_else(|| io::Error::other("missing process memory counter"))?;
    let mut fields = line.split_whitespace();
    let kib = fields
        .next()
        .ok_or_else(|| io::Error::other("missing process memory value"))?
        .parse::<u64>()
        .map_err(io::Error::other)?;
    if fields.next() != Some("kB") || fields.next().is_some() {
        return Err(io::Error::other("invalid process memory unit"));
    }
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::other("process memory counter overflow"))
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NativeReadProfile {
    pub index_ns: u64,
    /// Native WAL read includes its CRC validation and record decoding. This
    /// clock is deliberately not labelled pure disk service time.
    pub wal_and_decode_ns: u64,
    pub value_cache_hit: bool,
}

impl ReadEvidence {
    /// # Errors
    /// Returns an error if Linux process identity or I/O counters cannot be read.
    pub fn process() -> io::Result<Self> {
        let stat = fs::read_to_string("/proc/self/stat")?;
        let (_, tail) = stat
            .rsplit_once(") ")
            .ok_or_else(|| io::Error::other("invalid process stat"))?;
        let fields: Vec<_> = tail.split_whitespace().collect();
        let field = |index: usize| -> io::Result<u64> {
            fields
                .get(index)
                .ok_or_else(|| io::Error::other("missing process stat field"))?
                .parse::<u64>()
                .map_err(io::Error::other)
        };
        let io = fs::read_to_string("/proc/self/io")?;
        let bytes = io
            .lines()
            .find_map(|l| l.strip_prefix("read_bytes: "))
            .ok_or_else(|| io::Error::other("physical read accounting unavailable"))?
            .trim()
            .parse()
            .map_err(io::Error::other)?;
        Ok(Self {
            pid: std::process::id(),
            start_ticks: field(19)?,
            process_boot_id: process_boot_id(),
            read_bytes: bytes,
            minor_faults: field(7)?,
            major_faults: field(9)?,
            ..Self::default()
        })
    }

    /// # Errors
    /// Rejects a changed process identity or a decreasing cumulative counter.
    pub fn delta(self, earlier: Self) -> io::Result<Self> {
        if (self.pid, self.start_ticks) != (earlier.pid, earlier.start_ticks)
            || matches!(
                (self.process_boot_id, earlier.process_boot_id),
                (Some(current), Some(previous)) if current != previous
            )
        {
            return Err(io::Error::other(
                "read evidence process or boot identity changed",
            ));
        }
        let sub = |a: u64, b: u64| {
            a.checked_sub(b)
                .ok_or_else(|| io::Error::other("read counter decreased"))
        };
        Ok(Self {
            read_bytes: sub(self.read_bytes, earlier.read_bytes)?,
            minor_faults: sub(self.minor_faults, earlier.minor_faults)?,
            major_faults: sub(self.major_faults, earlier.major_faults)?,
            ..self
        })
    }
}

/// Inspect page-cache residency without touching file contents. Mapping metadata
/// alone does not fault file pages in. Holes count as nonresident too, so this is
/// a whole-file bound, not a per-value cache-hit measurement.
pub(crate) fn file_residency(file: &fs::File, evict: bool) -> io::Result<(u64, u64)> {
    if evict {
        file.sync_all()?;
        // SAFETY: the descriptor belongs to this live File; zero length means
        // the entire file. Advice changes caching only, never the file bytes.
        let status =
            unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
    }
    let len = file.metadata()?.len();
    // SAFETY: sysconf does not access caller memory.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = usize::try_from(page)
        .ok()
        .filter(|n| n.is_power_of_two())
        .ok_or_else(|| io::Error::other("invalid OS page size"))?;
    let mut resident = 0;
    // Bound scratch and mappings even for large benchmark files.
    let chunk = (1024 * 1024).max(page);
    for offset in (0..len).step_by(chunk) {
        let bytes = usize::try_from((len - offset).min(chunk as u64)).map_err(io::Error::other)?;
        // SAFETY: mapping is held until mincore completes. We do not read or
        // expose mapped bytes, including when the WAL writer changes a page.
        let map = unsafe {
            memmap2::MmapOptions::new()
                .offset(offset)
                .len(bytes)
                .map(file)?
        };
        let mut pages = vec![0u8; bytes.div_ceil(page)];
        // SAFETY: mmap returns a page-aligned live range; pages has exactly one
        // writable byte per covered OS page. mincore does not change file data.
        let result =
            unsafe { libc::mincore(map.as_ptr().cast_mut().cast(), bytes, pages.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        resident += pages.iter().filter(|&&b| b & 1 != 0).count() as u64 * page as u64;
    }
    Ok((len, resident.min(len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn process_resources_require_real_bounded_memory_units() {
        assert_eq!(
            status_bytes("VmRSS:\t42 kB\n", "VmRSS:").unwrap(),
            42 * 1024
        );
        for status in [
            "",
            "VmRSS: 42 MB",
            "VmRSS: -1 kB",
            "VmRSS: 18446744073709551615 kB",
        ] {
            assert!(status_bytes(status, "VmRSS:").is_err());
        }
        let resources = ProcessResources::process().unwrap();
        assert_eq!(resources.pid, std::process::id());
        assert!(resources.rss_bytes > 0);
        assert!(resources.peak_rss_bytes >= resources.rss_bytes);
        assert_eq!(
            resources.accounted_process_write_bytes,
            process_io_counter("write_bytes:")
        );
    }

    #[test]
    fn process_io_counter_preserves_missing_or_malformed_as_unavailable() {
        assert_eq!(
            parse_process_io_counter("read_bytes: 12\nwrite_bytes: 34\n", "write_bytes:"),
            Some(34)
        );
        assert_eq!(
            parse_process_io_counter("read_bytes: 12\n", "write_bytes:"),
            None
        );
        assert_eq!(
            parse_process_io_counter("write_bytes: unknown\n", "write_bytes:"),
            None
        );
    }

    #[test]
    fn process_boot_id_parser_preserves_valid_identity_and_rejects_invalid_values() {
        assert_eq!(
            parse_process_boot_id("00112233-4455-6677-8899-aabbccddeeff\n"),
            Some([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff
            ])
        );
        assert_eq!(parse_process_boot_id("invalid"), None);
    }

    #[test]
    fn evidence_rejects_changed_boot_identity() {
        let first = ReadEvidence {
            pid: 1,
            start_ticks: 2,
            process_boot_id: Some([1; 16]),
            read_bytes: 4096,
            ..ReadEvidence::default()
        };
        assert!(
            ReadEvidence {
                process_boot_id: Some([2; 16]),
                ..first
            }
            .delta(first)
            .is_err()
        );
    }
    #[test]
    fn evidence_rejects_changed_process_or_decreased_counters() {
        let first = ReadEvidence {
            pid: 1,
            start_ticks: 2,
            read_bytes: 4096,
            ..ReadEvidence::default()
        };
        assert!(ReadEvidence { pid: 2, ..first }.delta(first).is_err());
        assert!(
            ReadEvidence {
                read_bytes: 0,
                ..first
            }
            .delta(first)
            .is_err()
        );
        assert_eq!(
            ReadEvidence {
                read_bytes: 8192,
                ..first
            }
            .delta(first)
            .unwrap()
            .read_bytes,
            4096
        );
        assert_eq!(ReadEvidence::process().unwrap().pid, std::process::id());
    }
}
