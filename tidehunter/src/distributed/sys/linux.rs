//! Linux implementation of the live NVMe-oF system interface.
use super::Sqe;
use io_uring::{IoUring, opcode, types};
use smallvec::SmallVec;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

/// `None`: Linux runs the leased live NVMe-oF route.
pub(crate) fn route_unavailable() -> Option<String> {
    None
}

/// The `O_DIRECT` open flag.
pub(crate) fn direct_io_flag() -> io::Result<libc::c_int> {
    Ok(libc::O_DIRECT)
}

/// Largest CPU index plus one that [`set_thread_affinity`] accepts.
pub(crate) fn affinity_cpu_limit() -> usize {
    libc::CPU_SETSIZE as usize
}

/// A new nonblocking, close-on-exec eventfd with counter 0.
pub(crate) fn eventfd() -> io::Result<OwnedFd> {
    // SAFETY: eventfd has plain value arguments. Check its result before taking ownership.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is a new, valid descriptor and has no other owner.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Pin only the calling thread to `cpu`, which must be below
/// [`affinity_cpu_limit`]. Failure leaves its existing affinity unchanged.
pub(crate) fn set_thread_affinity(cpu: usize) -> io::Result<()> {
    if cpu >= affinity_cpu_limit() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CPU outside affinity mask",
        ));
    }
    // SAFETY: `cpu_set_t` is a plain integer bitset, so the all-zero value is
    // valid and initialized; it is the empty CPU set, so no `CPU_ZERO` is
    // needed.
    let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: cpu is inside cpu_set_t's bounds (checked above), so `CPU_SET`
    // only flips one bit in the initialized mask.
    unsafe {
        libc::CPU_SET(cpu, &mut mask);
    }
    // SAFETY: the mask is initialized and lives for the whole syscall; pid 0
    // changes only the calling thread's affinity.
    let result = unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The `major:minor` form sysfs uses for a device number.
pub(crate) fn device_number(rdev: u64) -> String {
    format!("{}:{}", libc::major(rdev), libc::minor(rdev))
}

/// Allocate `len` bytes from offset 0, which must fit `off_t`.
pub(crate) fn allocate(file: &File, len: u64) -> io::Result<()> {
    let len = libc::off_t::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "allocation exceeds off_t"))?;
    // SAFETY: file retains its descriptor for the call and len is nonnegative.
    let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

/// One io_uring owned by a single queue thread, without SQPOLL.
pub(crate) struct Ring(IoUring);

impl Ring {
    /// `polled` requests IOPOLL; the ring never falls back to interrupt mode.
    pub(crate) fn new(entries: u32, polled: bool) -> io::Result<Self> {
        let mut builder = IoUring::builder();
        builder.dontfork();
        if polled {
            builder.setup_iopoll();
        }
        builder.build(entries).map(Self)
    }

    pub(crate) fn register_eventfd(&self, fd: RawFd) -> io::Result<()> {
        self.0.submitter().register_eventfd(fd)
    }

    /// Descriptor that polls readable while the completion queue holds an entry.
    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }

    pub(crate) fn completion_ready(&mut self) -> bool {
        !self.0.completion().is_empty()
    }

    /// Queue every entry, or none (`false`: the submission queue is full).
    ///
    /// # Safety
    /// Every buffer and descriptor an entry names must stay valid until that
    /// entry's completion is harvested by [`Ring::next_completion`].
    pub(crate) unsafe fn push(&mut self, entries: &[(Sqe, u64)]) -> bool {
        let entries: SmallVec<[io_uring::squeue::Entry; 1]> = entries
            .iter()
            .map(|&(sqe, user_data)| {
                match sqe {
                    Sqe::Read {
                        fd,
                        buf,
                        len,
                        offset,
                    } => opcode::Read::new(types::Fd(fd), buf, len)
                        .offset(offset)
                        .build(),
                    Sqe::Write {
                        fd,
                        buf,
                        len,
                        offset,
                        data_sync,
                    } => opcode::Write::new(types::Fd(fd), buf, len)
                        .offset(offset)
                        .rw_flags(if data_sync { libc::RWF_DSYNC } else { 0 })
                        .build(),
                    Sqe::DataSync { fd } => opcode::Fsync::new(types::Fd(fd))
                        .flags(types::FsyncFlags::DATASYNC)
                        .build(),
                }
                .user_data(user_data)
            })
            .collect();
        // SAFETY: the caller keeps every named buffer and descriptor valid until
        // its completion. No SQPOLL observes SQEs before submit.
        // push_multiple is all-or-nothing.
        unsafe { self.0.submission().push_multiple(&entries) }.is_ok()
    }

    /// Entries queued but not yet consumed by the kernel.
    pub(crate) fn queued(&mut self) -> usize {
        self.0.submission().len()
    }

    /// Submit queued entries and wait up to `timeout` for one completion.
    pub(crate) fn submit_and_wait_timeout(&mut self, timeout: Duration) -> io::Result<usize> {
        let timespec = types::Timespec::from(timeout);
        self.0
            .submitter()
            .submit_with_args(1, &types::SubmitArgs::new().timespec(&timespec))
    }

    /// Submit queued entries; `reap` also enters the kernel with nothing
    /// queued, as an IOPOLL ring must to make progress.
    pub(crate) fn submit(&mut self, reap: bool) -> io::Result<usize> {
        if reap {
            self.0.submit_and_wait(0)
        } else {
            self.0.submit()
        }
    }

    /// The next completion as `(user_data, result)`.
    pub(crate) fn next_completion(&mut self) -> Option<(u64, i32)> {
        self.0
            .completion()
            .next()
            .map(|cqe| (cqe.user_data(), cqe.result()))
    }
}
