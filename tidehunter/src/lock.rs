use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

static OPEN_DBS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

fn open_dbs() -> &'static Mutex<HashSet<PathBuf>> {
    OPEN_DBS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// An exclusive lock on a database directory, held for the lifetime of the value.
///
/// Uses two layers of protection to prevent concurrent access:
///
/// 1. **In-process registry**: a global `HashSet` of canonical paths tracks all currently-open
///    databases within the process. This is necessary because POSIX `fcntl` locks are
///    per-process, not per-fd — a second `F_SETLK` call from the same process would silently
///    succeed and replace the first lock rather than blocking.
///
/// 2. **`fcntl(F_SETLK)` advisory lock** on a `LOCK` file in the database directory: catches
///    concurrent access from other processes. The OS releases this lock automatically on process
///    exit or fd close, so no cleanup is needed after a crash.
///
/// The registry check must happen before opening any fd. Opening then closing a competing fd
/// would silently release all `fcntl` locks held by this process on that inode — including the
/// one the first opener thinks it still holds (see [RocksDB issue #1780]).
///
/// [RocksDB issue #1780]: https://github.com/facebook/rocksdb/issues/1780
#[derive(Debug)]
pub struct DbLock {
    // Taken and closed under the registry guard before ownership is released.
    file: Option<File>,
    canonical_path: PathBuf,
    #[cfg(test)]
    after_unregister: Option<(
        std::sync::mpsc::SyncSender<()>,
        Mutex<std::sync::mpsc::Receiver<()>>,
    )>,
}

impl DbLock {
    /// Default timeout used when no config is available (e.g. in `Db::drop_db`).
    /// Acquires an exclusive lock on `db_path`.
    ///
    /// Retries for up to `retry_timeout` awhen the in-process registry shows the database is
    /// held — background threads may briefly extend the lifetime of a dropped `Arc<Db>`,
    /// and this gives them time to finish before the open is considered an error.
    ///
    /// Returns `ErrorKind::AlreadyExists` if the database is still open after the timeout,
    /// or if it is locked by another process.
    pub fn acquire(db_path: &Path, retry_timeout: Duration) -> io::Result<Self> {
        let canonical = db_path.canonicalize()?;

        // Insert into the in-process registry before opening any fd.
        // Must happen before open() to avoid the close()-releases-fcntl-locks hazard:
        // closing a competing fd on the same inode would silently drop the first
        // opener's fcntl lock (POSIX fcntl locks are per-process, not per-fd).
        let start = std::time::Instant::now();
        let deadline = start + retry_timeout;
        let mut last_warning = None::<std::time::Instant>;
        loop {
            {
                let mut open = open_dbs().lock();
                if !open.contains(&canonical) {
                    open.insert(canonical.clone());
                    break;
                }
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "Database at {} is already open in this process",
                        db_path.display()
                    ),
                ));
            }
            let should_warn =
                last_warning.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1));
            if should_warn {
                eprintln!(
                    "WARNING: waiting for database at {} to close (waited {:.1}s)",
                    db_path.display(),
                    now.duration_since(start).as_secs_f32()
                );
                last_warning = Some(now);
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let lock_path = canonical.join("LOCK");
        let file = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(f) => f,
            Err(e) => {
                open_dbs().lock().remove(&canonical);
                return Err(e);
            }
        };

        #[allow(clippy::unnecessary_cast)] // l_type is i16 on some platforms, i32 on others
        let mut flock = libc::flock {
            l_type: libc::F_WRLCK as i16, // Exclusive write lock
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 0, // Lock the entire file
            l_pid: 0,
        };
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &mut flock) };
        if result == -1 {
            let err = io::Error::last_os_error();
            // Close even an unsuccessful acquisition's fd before a competing
            // same-process opener can register/acquire a replacement lock.
            let mut open = open_dbs().lock();
            drop(file);
            open.remove(&canonical);
            drop(open);
            if err.raw_os_error() == Some(libc::EWOULDBLOCK)
                || err.raw_os_error() == Some(libc::EAGAIN)
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "Database at {} is already locked by another process",
                        db_path.display()
                    ),
                ));
            }
            return Err(err);
        }

        Ok(DbLock {
            file: Some(file),
            canonical_path: canonical,
            #[cfg(test)]
            after_unregister: None,
        })
    }
}

impl Drop for DbLock {
    fn drop(&mut self) {
        // POSIX record locks belong to the process: closing ANY fd for this
        // inode releases them all. Keep the registry guard until the old fd is
        // closed so a successor cannot acquire a lock that this Drop removes.
        // https://man7.org/linux/man-pages/man2/fcntl_locking.2.html
        let mut open = open_dbs().lock();
        drop(self.file.take());
        open.remove(&self.canonical_path);
        drop(open);
        #[cfg(test)]
        if let Some((entered, resume)) = self.after_unregister.take() {
            let _ = entered.send(());
            let _ = resume.into_inner().recv();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use tempdir::TempDir;

    #[test]
    fn test_lock_stale_lock_file() {
        // A LOCK file left behind by a crashed process must not prevent reopening.
        // fcntl locks are released automatically by the OS on process exit, so the
        // file's presence alone is not an error.
        let dir = TempDir::new("test-lock-stale").unwrap();
        std::fs::write(dir.path().join("LOCK"), b"").unwrap();
        let _lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
    }

    #[test]
    fn test_lock_same_process() {
        let dir = TempDir::new("test-lock").unwrap();
        let canonical = dir.path().canonicalize().unwrap();
        let lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();

        // Verify the registry shows the DB as held without triggering the retry timeout.
        assert!(open_dbs().lock().contains(&canonical));

        drop(lock);
        // Lock released — should succeed now
        let _lock2 = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
    }

    #[test]
    fn test_lock_registry_cleaned_up_on_drop() {
        let dir = TempDir::new("test-lock-cleanup").unwrap();
        let canonical = dir.path().canonicalize().unwrap();

        assert!(!open_dbs().lock().contains(&canonical));
        let lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
        assert!(open_dbs().lock().contains(&canonical));
        drop(lock);
        assert!(!open_dbs().lock().contains(&canonical));
    }

    #[test]
    fn _subprocess_lock_probe() {
        let Ok(path) = std::env::var("TIDEHUNTER_TEST_PROBE_LOCK") else {
            return;
        };
        let result = DbLock::acquire(Path::new(&path), Duration::ZERO);
        match std::env::var("TIDEHUNTER_TEST_PROBE_EXPECT")
            .unwrap()
            .as_str()
        {
            "blocked" => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists),
            "free" => drop(result.unwrap()),
            other => panic!("invalid lock probe expectation: {other}"),
        }
    }

    fn probe_from_other_process(path: &Path, expected: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lock::tests::_subprocess_lock_probe",
                "--nocapture",
            ])
            .env("TIDEHUNTER_TEST_PROBE_LOCK", path)
            .env("TIDEHUNTER_TEST_PROBE_EXPECT", expected)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child probe failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    fn test_lock_handoff_cannot_release_successors_process_lock() {
        let dir = TempDir::new("test-lock-handoff").unwrap();
        let mut first = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
        let (entered, paused) = std::sync::mpsc::sync_channel(1);
        let (resume, resumed) = std::sync::mpsc::sync_channel(1);
        first.after_unregister = Some((entered, Mutex::new(resumed)));
        let dropping = std::thread::spawn(move || drop(first));
        paused.recv_timeout(Duration::from_secs(5)).unwrap();

        // The predecessor's Drop has released its registry entry but has not
        // returned to automatic field destruction. The previous implementation
        // still held its File here: closing it AFTER this same-process acquire
        // silently unlocked the successor. No timing loop is needed to force it.
        let second = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
        resume.send(()).unwrap();
        dropping.join().unwrap();
        probe_from_other_process(dir.path(), "blocked");
        drop(second);
        probe_from_other_process(dir.path(), "free");
    }

    // This "test" is actually a subprocess entrypoint used by test_lock_cross_process.
    // When TIDEHUNTER_TEST_HOLD_LOCK is set, it acquires the lock at that path, creates
    // a "{path}/.ready" file to signal the parent, then waits for stdin to close.
    #[test]
    fn _subprocess_lock_holder() {
        let Ok(path) = std::env::var("TIDEHUNTER_TEST_HOLD_LOCK") else {
            return;
        };
        let path = PathBuf::from(path);
        let _lock =
            DbLock::acquire(&path, Duration::ZERO).expect("subprocess failed to acquire lock");
        // Signal parent via file — avoids libtest stdout capture interference
        std::fs::write(path.join(".ready"), b"").unwrap();
        // Block until parent closes stdin
        let mut buf = [0u8; 1];
        let _ = std::io::stdin().read(&mut buf);
    }

    #[test]
    fn test_lock_cross_process() {
        let dir = TempDir::new("test-lock-cross-process").unwrap();
        let exe = std::env::current_exe().unwrap();
        let ready = dir.path().join(".ready");

        let mut child = Command::new(&exe)
            .arg("_subprocess_lock_holder")
            .env("TIDEHUNTER_TEST_HOLD_LOCK", dir.path())
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();

        // Wait for subprocess to signal it holds the lock (up to 5s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready.exists() {
            assert!(std::time::Instant::now() < deadline, "subprocess timed out");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Another process holds the lock — must fail
        let err = DbLock::acquire(dir.path(), Duration::ZERO).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        // Signal subprocess to exit by closing its stdin
        drop(child.stdin.take());
        child.wait().unwrap();

        // Lock released — must succeed now
        let _lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
    }
}
