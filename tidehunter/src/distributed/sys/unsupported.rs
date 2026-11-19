//! Stand-in for targets without the Linux live NVMe-oF interfaces. Every
//! operation fails; remote and owner opens are refused before reaching it.
use super::Sqe;
use std::fs::File;
use std::io;
use std::os::fd::{OwnedFd, RawFd};
use std::time::Duration;

/// The named error of every remote or owner open on this target.
pub(crate) fn route_unavailable() -> Option<String> {
    Some(format!(
        "distributed route requires Linux (leased NVMe-oF): {}",
        std::env::consts::OS
    ))
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "distributed route requires Linux (leased NVMe-oF)",
    )
}

pub(crate) fn direct_io_flag() -> io::Result<libc::c_int> {
    Err(unsupported())
}

/// No CPU can be pinned.
pub(crate) fn affinity_cpu_limit() -> usize {
    0
}

pub(crate) fn eventfd() -> io::Result<OwnedFd> {
    Err(unsupported())
}

pub(crate) fn set_thread_affinity(_cpu: usize) -> io::Result<()> {
    Err(unsupported())
}

/// Empty: never equal to a sysfs `major:minor` value.
pub(crate) fn device_number(_rdev: u64) -> String {
    String::new()
}

pub(crate) fn allocate(_file: &File, _len: u64) -> io::Result<()> {
    Err(unsupported())
}

/// Uninhabited: [`Ring::new`] always fails, so no other method can run.
pub(crate) enum Ring {}

impl Ring {
    pub(crate) fn new(_entries: u32, _polled: bool) -> io::Result<Self> {
        Err(unsupported())
    }

    pub(crate) fn register_eventfd(&self, _fd: RawFd) -> io::Result<()> {
        match *self {}
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        match *self {}
    }

    pub(crate) fn completion_ready(&mut self) -> bool {
        match *self {}
    }

    /// # Safety
    /// Same contract as the Linux ring; this method cannot run.
    pub(crate) unsafe fn push(&mut self, _entries: &[(Sqe, u64)]) -> bool {
        match *self {}
    }

    pub(crate) fn queued(&mut self) -> usize {
        match *self {}
    }

    pub(crate) fn submit_and_wait_timeout(&mut self, _timeout: Duration) -> io::Result<usize> {
        match *self {}
    }

    pub(crate) fn submit(&mut self, _reap: bool) -> io::Result<usize> {
        match *self {}
    }

    pub(crate) fn next_completion(&mut self) -> Option<(u64, i32)> {
        match *self {}
    }
}
