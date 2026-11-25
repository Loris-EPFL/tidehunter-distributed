use super::super::{Command, LocalDbError};
use super::owner_queue::OwnerQueue;
use parking_lot::{Condvar, Mutex};
use std::sync::{Arc, OnceLock, mpsc};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

pub(in crate::distributed::remote_db) struct OrderedCommand {
    pub before_owner_seq: u64,
    pub command: Command,
}

#[derive(Default)]
struct CapacityState {
    generation: u64,
    closed: bool,
}

#[derive(Default)]
struct Capacity {
    state: Mutex<CapacityState>,
    changed: Condvar,
    #[cfg(test)]
    full_park: Mutex<Option<mpsc::SyncSender<()>>>,
}

/// One owner's worker receives this bounded control channel. Full controls wait
/// without holding owner.order, up to the configured deadline; wake hints may
/// coalesce on Full. Dropping the receiver wakes all senders as stopped.
/// A successful send under owner.order is the control's linearization point.
/// The capacity mutex is never held with owner.order or any write-core lock.
/// Send lock order: owner.order → sender mutex, never the reverse. Neither is
/// held while waiting for capacity or joining. READY data never takes this
/// mutex; only its coalesced wake and cold controls use the sender.
pub(in crate::distributed::remote_db) struct OwnerSender {
    sender: Mutex<Option<mpsc::SyncSender<OrderedCommand>>>,
    queue: Arc<OwnerQueue>,
    capacity: Arc<Capacity>,
    timeout: Duration,
    worker: OnceLock<Thread>,
    #[cfg(test)]
    waiting: Mutex<Option<mpsc::SyncSender<()>>>,
}

pub(in crate::distributed::remote_db) struct OwnerReceiver {
    receiver: mpsc::Receiver<OrderedCommand>,
    capacity: Arc<Capacity>,
}

impl OwnerSender {
    pub fn channel(
        capacity: usize,
        queue: Arc<OwnerQueue>,
        timeout: Duration,
    ) -> (Self, OwnerReceiver) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let capacity = Arc::new(Capacity::default());
        (
            Self {
                sender: Mutex::new(Some(sender)),
                queue,
                capacity: Arc::clone(&capacity),
                timeout,
                worker: OnceLock::new(),
                #[cfg(test)]
                waiting: Mutex::new(None),
            },
            OwnerReceiver { receiver, capacity },
        )
    }

    pub fn set_worker(&self, worker: Thread) {
        let _ = self.worker.set(worker);
    }

    pub fn close(&self) {
        let sender = self.sender.lock().take();
        drop(sender);
        {
            let mut state = self.capacity.state.lock();
            state.closed = true;
            self.capacity.changed.notify_all();
        }
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
    }

    #[cfg(test)]
    pub fn observe_full_park(&self, entered: mpsc::SyncSender<()>) {
        *self.capacity.full_park.lock() = Some(entered);
    }

    pub fn try_send(&self, command: Command) -> Result<(), mpsc::TrySendError<Command>> {
        let result = self.queue.capture_order(|before_owner_seq| {
            let sender = self.sender.lock();
            let Some(sender) = sender.as_ref() else {
                return Err(mpsc::TrySendError::Disconnected(command));
            };
            sender
                .try_send(OrderedCommand {
                    before_owner_seq,
                    command,
                })
                .map_err(|error| match error {
                    mpsc::TrySendError::Full(ordered) => mpsc::TrySendError::Full(ordered.command),
                    mpsc::TrySendError::Disconnected(ordered) => {
                        mpsc::TrySendError::Disconnected(ordered.command)
                    }
                })
        });
        // Full local control metadata stops channel receives. Consequently a
        // READY hint can find the channel full; it must still wake this token.
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
        result
    }

    pub fn send(&self, command: Command) -> Result<(), LocalDbError> {
        self.send_until(command, Instant::now() + self.timeout)
    }

    pub fn send_until(&self, mut command: Command, deadline: Instant) -> Result<(), LocalDbError> {
        // This is a deadline-bounded capacity wait, not an operation retry:
        // failed try_send has no effect and releases owner.order before waiting.
        loop {
            let generation = self.capacity.state.lock().generation;
            if Instant::now() >= deadline {
                return Err(LocalDbError::new(
                    "owner control deadline expired before admission",
                ));
            }
            match self.try_send(command) {
                Ok(()) => return Ok(()),
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(LocalDbError::new("TCP worker stopped"));
                }
                Err(mpsc::TrySendError::Full(returned)) => command = returned,
            }
            #[cfg(test)]
            if let Some(waiting) = self.waiting.lock().take() {
                let _ = waiting.try_send(());
            }
            let mut state = self.capacity.state.lock();
            if state.closed {
                return Err(LocalDbError::new("TCP worker stopped"));
            }
            // A receive between the failed send and this lock must not strand
            // the waiter. The receiver increments under this same mutex.
            if state.generation == generation {
                self.capacity.changed.wait_until(&mut state, deadline);
            }
        }
    }
}

impl Drop for OwnerSender {
    fn drop(&mut self) {
        self.close();
    }
}

impl OwnerReceiver {
    /// The caller has completed the SeqCst owner-queue parking handshake.
    /// Local metadata is full, so stop receiving: the bounded channel fills and
    /// control senders back off on Capacity.changed. READY independently wakes
    /// the Thread token even when its QueueReady hint cannot enter that channel.
    /// Only for a worker with no adoption or flight progress pending.
    pub fn park_full(&self) {
        self.note_full_park();
        thread::park();
    }

    /// `park_full` for a worker that must still drive a pending leased
    /// adoption and its deadline: wake after `timeout` at the latest.
    pub fn park_full_timeout(&self, timeout: Duration) {
        self.note_full_park();
        thread::park_timeout(timeout);
    }

    fn note_full_park(&self) {
        #[cfg(test)]
        if let Some(entered) = self.capacity.full_park.lock().take() {
            let _ = entered.try_send(());
        }
    }

    fn notify_capacity(&self) {
        let mut state = self.capacity.state.lock();
        state.generation = state.generation.wrapping_add(1);
        self.capacity.changed.notify_all();
    }

    /// Capture controls and the allocation bound in one owner-order section.
    /// Notify capacity only after releasing owner.order.
    pub fn poll(
        &self,
        queue: &OwnerQueue,
        budget: usize,
        mut accept: impl FnMut(OrderedCommand),
    ) -> (u64, bool) {
        let mut received = 0;
        let mut disconnected = false;
        let bound = queue.capture_order(|next| {
            let mut bound = queue.head();
            for _ in 0..budget {
                match self.receiver.try_recv() {
                    Ok(command) => {
                        received += 1;
                        bound = command.before_owner_seq;
                        accept(command);
                    }
                    Err(mpsc::TryRecvError::Empty) => return next,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        return next;
                    }
                }
            }
            bound
        });
        for _ in 0..received {
            self.notify_capacity();
        }
        (bound, disconnected)
    }

    pub fn recv(&self) -> Result<OrderedCommand, mpsc::RecvError> {
        let result = self.receiver.recv();
        if result.is_ok() {
            self.notify_capacity();
        }
        result
    }

    /// Only the leased worker loop waits with a deadline on the control channel.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<OrderedCommand, mpsc::RecvTimeoutError> {
        let result = self.receiver.recv_timeout(timeout);
        if result.is_ok() {
            self.notify_capacity();
        }
        result
    }
}

impl Drop for OwnerReceiver {
    fn drop(&mut self) {
        // Under the guard `wait_until` checks `closed` under: a parking_lot
        // notify outside it can fall between the check and the park and be
        // dropped, which costs the sender one full wait before it sees the
        // close.
        let mut state = self.capacity.state.lock();
        state.closed = true;
        self.capacity.changed.notify_all();
    }
}

#[cfg(test)]
mod tests;
