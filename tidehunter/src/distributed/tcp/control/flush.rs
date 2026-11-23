//! The owner's single flusher thread and its ordered commands.
//!
//! Moved from `control.rs` to stay under that file's length baseline; only the
//! visibility differs. A barrier answers after every earlier command and after
//! every log's pending bytes are durable.

use super::*;

pub(super) enum FlushCommand {
    /// A frontier flush ordered after every earlier command.
    Barrier(mpsc::SyncSender<()>),
    Stop,
}

pub(super) fn flush_run(shared: &Shared, receiver: &mpsc::Receiver<FlushCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            FlushCommand::Barrier(done) => {
                // Every command before this one is replied; now make anything
                // acknowledged under the native tier durable too.
                if shared.flush_logs().is_err() {
                    drop(done);
                    shared.context.poison_readers();
                    {
                        let mut queue = shared.queue.lock();
                        queue.flush_failed = true;
                        queue.stopped = true;
                    }
                    fail_queued_flush(receiver);
                    return;
                }
                let _ = done.send(());
            }
            FlushCommand::Stop => return,
        }
    }
}

/// Drop every queued barrier after a persistence failure: its waiter sees the
/// disconnect and fails its control.
pub(super) fn fail_queued_flush(receiver: &mpsc::Receiver<FlushCommand>) {
    while let Ok(FlushCommand::Barrier(done)) = receiver.try_recv() {
        drop(done);
    }
}
