//! Linux system interface of the leased live NVMe-oF route: io_uring, eventfd,
//! CPU affinity, block-device numbers, `O_DIRECT` and file preallocation.
//!
//! This is the only target-specific boundary of the distributed build. Other
//! targets compile an unsupported stand-in whose operations fail. Every remote
//! open and every owner node open is refused first with [`route_unavailable`],
//! so the stand-in is never reached in a supported call sequence.

#[cfg_attr(target_os = "linux", path = "linux.rs")]
#[cfg_attr(not(target_os = "linux"), path = "unsupported.rs")]
mod imp;

pub(crate) use imp::*;

/// One submission of the live data plane. Raw buffer pointers must stay valid
/// until the operation's completion (see [`Ring::push`]).
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    not(target_os = "linux"),
    expect(dead_code, reason = "only the Linux ring reads submissions")
)]
pub(crate) enum Sqe {
    Read {
        fd: std::os::fd::RawFd,
        buf: *mut u8,
        len: u32,
        offset: u64,
    },
    /// `data_sync` requests `RWF_DSYNC` (FUA on a block namespace).
    Write {
        fd: std::os::fd::RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
        data_sync: bool,
    },
    DataSync {
        fd: std::os::fd::RawFd,
    },
}
