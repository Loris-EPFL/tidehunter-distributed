//! The connect budget and the typed errors of a live read grant.
//!
//! Split out of `live_read.rs` to hold that file under its length ratchet; the
//! items stay private to that module through `pub(super)`.

use super::TcpError;
use std::io;
use std::time::{Duration, Instant};

/// A grant setup that did not produce a grant.
pub(super) enum ConnectError {
    /// The controller pool has no free slot; may clear soon.
    PoolFull(io::Error),
    Failed(TcpError),
}

pub(super) fn failed(error: impl std::fmt::Display) -> ConnectError {
    ConnectError::Failed(TcpError::component(error))
}

/// A setup that hit its deadline is transient, not a broken ring. Keep the
/// class so a slow connect does not poison the read authority.
pub(super) fn failed_io(error: io::Error) -> ConnectError {
    if error.kind() == io::ErrorKind::TimedOut {
        ConnectError::Failed(TcpError::with_kind(error.kind(), error.to_string()))
    } else {
        failed(error)
    }
}

/// The connect budget is spent with the pool still full: a deadline, not a
/// broken ring, so the caller may retry the read.
pub(super) fn pool_deadline(error: io::Error) -> TcpError {
    TcpError::with_kind(io::ErrorKind::TimedOut, error.to_string())
}

/// The one connect deadline of a request, counted from its first attempt: the
/// call deadline or the connect budget, whichever is earlier. The elapsed
/// connect time is charged once, by `since`, and never subtracted from the call
/// deadline again.
pub(super) fn connect_budget(call: Instant, since: Instant, connect_timeout: Duration) -> Instant {
    call.min(since + connect_timeout)
}

#[cfg(test)]
#[path = "connect_tests.rs"]
mod tests;
