//! Absolute-deadline socket adapter for the blocking transport phases.
//!
//! One absolute deadline bounds the TCP connect, the transport-mode byte, the
//! TLS handshake and the setup probe. Each blocking I/O derives its socket
//! timeout from the remaining budget. The timeout rounds that budget down to
//! whole milliseconds, so an expiry that arrives before the deadline is
//! retried; once the deadline is spent the stage reports
//! `io::ErrorKind::TimedOut`. No step can renew the budget, and the phase
//! cannot outlive the deadline by more than one socket tick.

use super::SocketProfile;
use super::elapsed_ns;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// Absolute deadline checked for every underlying TLS socket operation, so a
/// trickling peer cannot renew the timeout for each byte of one frame.
pub(super) struct DeadlineIo {
    #[cfg(feature = "experimental-fabric")]
    pub(super) fabric: Option<tidehunter_fabric::Endpoint>,
    pub(super) socket: TcpStream,
    /// The absolute deadline of the blocking phase that owns this adapter.
    pub(super) deadline: Instant,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    pub(super) profile: Option<SocketProfile>,
}

impl DeadlineIo {
    /// A deadline relative to now, for a stage whose whole budget starts here.
    pub(super) fn new(socket: TcpStream, timeout: Duration) -> io::Result<Self> {
        Self::new_until(socket, Instant::now() + timeout)
    }

    /// One absolute deadline shared by every stage of one setup.
    pub(super) fn new_until(socket: TcpStream, deadline: Instant) -> io::Result<Self> {
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            #[cfg(feature = "experimental-fabric")]
            fabric: None,
            deadline,
            read_timeout: None,
            write_timeout: None,
            profile: None,
        })
    }

    fn expired() -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, "TLS frame deadline expired")
    }

    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(Self::expired)
    }

    fn prepare(&mut self, read: bool) -> io::Result<()> {
        let began = self.profile.map(|_| Instant::now());
        let remaining = self.remaining()?;
        // A sub-microsecond timeout serializes as a zero `timeval`, which a
        // socket treats as no timeout at all. The deadline is spent for this
        // stage; report it instead of blocking without a bound.
        if remaining < Duration::from_micros(1) {
            return Err(Self::expired());
        }
        #[cfg(feature = "experimental-fabric")]
        if self.fabric.is_some() {
            return Err(io::Error::other(
                "blocking I/O forbidden after fabric switch",
            ));
        }
        // Never round a timeout upwards: keep the absolute deadline and use
        // sub-millisecond precision for its final fraction. Reusing the same
        // millisecond avoids a setsockopt for every small TLS read/write.
        let timeout = if remaining >= Duration::from_millis(1) {
            Duration::from_millis(remaining.as_millis() as u64)
        } else {
            remaining
        };
        let cached = if read {
            &mut self.read_timeout
        } else {
            &mut self.write_timeout
        };
        let changed = *cached != Some(timeout);
        if changed {
            if read {
                self.socket.set_read_timeout(Some(timeout))?;
            } else {
                self.socket.set_write_timeout(Some(timeout))?;
            }
            *cached = Some(timeout);
        }
        if let Some(p) = self.profile.as_mut() {
            p.timeout_setup_ns += began.map_or(0, elapsed_ns);
            p.timeout_sets += u64::from(changed);
        }
        Ok(())
    }

    /// One blocking socket operation under the absolute deadline. A blocking
    /// socket reports `WouldBlock` only when its timeout expired; the rounded
    /// timeout can fire before the deadline, so that case retries and the
    /// spent deadline ends the loop with the typed class. Blocking mode is
    /// required: a nonblocking socket would spin here.
    fn blocking<T>(
        &mut self,
        read: bool,
        mut operation: impl FnMut(&mut TcpStream) -> io::Result<T>,
    ) -> io::Result<T> {
        loop {
            self.prepare(read)?;
            match operation(&mut self.socket) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.remaining()?;
                }
                result => return result,
            }
        }
    }
}
impl Read for DeadlineIo {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let began = self.profile.map(|_| Instant::now());
        let result = self.blocking(true, |socket| socket.read(&mut bytes[..]));
        if let Some(p) = self.profile.as_mut() {
            p.read_ns += began.map_or(0, elapsed_ns);
            p.read_calls += 1;
            p.read_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
}
impl Write for DeadlineIo {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let began = self.profile.map(|_| Instant::now());
        let result = self.blocking(false, |socket| socket.write(bytes));
        if let Some(p) = self.profile.as_mut() {
            p.write_ns += began.map_or(0, elapsed_ns);
            p.write_calls += 1;
            p.write_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        // rustls offers multiple encrypted records together. The default Write
        // implementation only uses the first slice and defeats that batching.
        let began = self.profile.map(|_| Instant::now());
        let result = self.blocking(false, |socket| socket.write_vectored(buffers));
        if let Some(p) = self.profile.as_mut() {
            p.write_ns += began.map_or(0, elapsed_ns);
            p.write_calls += 1;
            p.write_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
}

#[cfg(test)]
#[path = "deadline_io_tests.rs"]
mod tests;
