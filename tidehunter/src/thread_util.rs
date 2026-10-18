use std::thread::JoinHandle;
#[cfg(not(any(test, feature = "experimental-distributed")))]
use std::time::Duration;

/// Joins owned workers before their owner releases its directory lock.
///
/// The experimental distributed path (and tests) waits for actual completion.
/// Returning after a timeout could leave a mapper/sync/unlink worker accessing
/// files after the old owner releases its lock and a replacement opens them.
/// This deliberately does not promise bounded shutdown: a stalled worker keeps
/// ownership rather than permitting unsafe directory reuse. Default native
/// production builds retain their historical timeout-and-warning behavior.
///
/// # Arguments
/// * `jh` - The thread's JoinHandle to wait for
/// * `thread_name` - Name of the thread for error messages
/// * `timeout_secs` - Timeout in seconds
#[cfg(any(test, feature = "experimental-distributed"))]
pub fn join_thread_with_timeout(jh: JoinHandle<()>, thread_name: &str, _timeout_secs: u64) {
    if let Err(error) = jh.join() {
        panic!("{thread_name} thread panicked: {error:?}");
    }
}

#[cfg(not(any(test, feature = "experimental-distributed")))]
pub fn join_thread_with_timeout(jh: JoinHandle<()>, thread_name: &str, timeout_secs: u64) {
    // Create a channel to signal join completion
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = jh.join();
        tx.send(result).ok();
    });

    // Wait up to timeout for the thread to complete
    match rx.recv_timeout(Duration::from_secs(timeout_secs)) {
        Ok(Ok(_result)) => {
            // Thread completed successfully
        }
        Ok(Err(e)) => {
            panic!("{thread_name} thread panicked: {e:?}");
        }
        Err(_timeout) => {
            let msg = format!(
                "{thread_name} thread did not stop in {timeout_secs} seconds (this should not happen)"
            );

            eprintln!("WARNING: {msg}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock::DbLock;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::time::Duration;

    #[test]
    fn strict_join_keeps_directory_locked_until_worker_finishes() {
        let dir = tempdir::TempDir::new("strict-worker-shutdown").unwrap();
        let lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
        let (release, wait_for_release) = mpsc::channel();
        let worker_exited = Arc::new(AtomicBool::new(false));
        let exited = worker_exited.clone();
        let worker = std::thread::spawn(move || {
            wait_for_release.recv().unwrap();
            exited.store(true, Ordering::Release);
        });
        let (started, wait_for_start) = mpsc::channel();
        let (done, wait_for_done) = mpsc::channel();
        let owner = std::thread::spawn(move || {
            started.send(()).unwrap();
            // A zero timeout distinguishes strict ownership from the former
            // detached joiner's immediate timeout path without a long sleep.
            join_thread_with_timeout(worker, "test-owned-worker", 0);
            assert!(worker_exited.load(Ordering::Acquire));
            drop(lock);
            done.send(()).unwrap();
        });
        wait_for_start.recv().unwrap();
        let premature = wait_for_done.recv_timeout(Duration::from_millis(30));
        let held = DbLock::acquire(dir.path(), Duration::ZERO);
        release.send(()).unwrap();
        owner.join().unwrap();
        assert_eq!(premature, Err(mpsc::RecvTimeoutError::Timeout));
        assert_eq!(held.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
        wait_for_done.recv().unwrap();
        let _replacement = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
    }

    #[test]
    #[should_panic(expected = "test-panicking-worker thread panicked")]
    fn strict_join_reports_worker_panic_after_thread_exit() {
        let worker = std::thread::spawn(|| panic!("injected worker failure"));
        join_thread_with_timeout(worker, "test-panicking-worker", 0);
    }
}
