//! The one thread per database that retires a write queue a resume detached.
//!
//! `LiveIo::shutdown` has no bound: a command lost with the controller can stay
//! outstanding until the kernel keep-alive fires, and `Drop for LiveIo` aborts
//! the process when DMA is still owned. A resume therefore moves the dead queue
//! here instead of draining it on the worker thread, exactly as the fail-stop
//! path already moves it outside the client mutex.

use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::LiveWriteQueue;

/// The reaper polls this cadence for its stop flag. The database's `Drop` joins
/// it while the clients that still hold senders outlive that `Drop`, so a
/// channel close alone would never end it; the wait only bounds shutdown.
const STOP_POLL: Duration = Duration::from_millis(100);

/// One detached-queue retirer. Capacity is one queue per owner worker; a full
/// channel makes the resume fail instead of growing memory.
pub(crate) struct ResumeReaper {
    sender: Option<SyncSender<LiveWriteQueue>>,
    thread: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl ResumeReaper {
    pub(crate) fn start(capacity: usize) -> std::io::Result<Self> {
        let (sender, receiver): (SyncSender<LiveWriteQueue>, Receiver<LiveWriteQueue>) =
            sync_channel(capacity.max(1));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("live-resume-reaper".to_owned())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(STOP_POLL) {
                        Ok(mut queue) => {
                            // A failed drain keeps the queue's own fail-stop
                            // `Drop` responsible for its DMA; report the cause.
                            if let Err(error) = queue.drain_failed() {
                                eprintln!("detached live write queue drain failed: {error}");
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            if thread_stop.load(Ordering::Acquire) {
                                return;
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            stop,
        })
    }

    /// The handle a write worker's client hands a detached queue to. `None`
    /// after the reaper stopped accepting work.
    pub(crate) fn sender(&self) -> Option<SyncSender<LiveWriteQueue>> {
        self.sender.clone()
    }

    /// Stop after the queue being drained, then join. Idempotent.
    pub(crate) fn finish(&mut self) {
        self.sender.take();
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ResumeReaper {
    fn drop(&mut self) {
        self.finish();
    }
}
