use crate::distributed::codec::CompleteBatch;
use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const ALLOCATED: u64 = 1;
const READY: u64 = 2;
const ABORTED: u64 = 3;

/// Owner-queue head state after a drain that rejected no batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeadStop {
    /// The head is at or past the ordered control or snapshot bound.
    Fence,
    /// The head entry is not allocated yet.
    Empty,
    /// The head entry is allocated and not READY yet.
    Allocated,
}

struct OwnerEntry {
    state: AtomicU64,
    batch: Mutex<Option<(u64, CompleteBatch)>>,
}

/// One worker consumes this bounded ring; callers reserve while holding the
/// identity/key locks, then encode without a lock. Live owner entries are a
/// subset of admitted sequencer slots, so admission also bounds this queue.
#[repr(align(64))]
pub(crate) struct OwnerQueue {
    order: Mutex<u64>,
    entries: Box<[OwnerEntry]>,
    head: AtomicU64,
    parked: AtomicBool,
}

pub(super) struct LockedOwnerOrders<'a> {
    owners: Vec<(usize, &'a OwnerQueue, MutexGuard<'a, u64>)>,
}

impl LockedOwnerOrders<'_> {
    pub(super) fn reserve(
        &mut self,
        owner: usize,
        allocate: impl FnOnce() -> u64,
    ) -> Option<(u64, u64)> {
        let position = self.owners.partition_point(|entry| entry.0 < owner);
        let entry = self
            .owners
            .get_mut(position)
            .filter(|entry| entry.0 == owner)?;
        Some(entry.1.reserve_ordered(&mut entry.2, allocate))
    }
}

impl OwnerQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity != 0, "validated nonzero pending capacity");
        Self {
            order: Mutex::new(1),
            entries: (0..capacity)
                .map(|_| OwnerEntry {
                    state: AtomicU64::new(0),
                    batch: Mutex::new(None),
                })
                .collect(),
            head: AtomicU64::new(1),
            parked: AtomicBool::new(false),
        }
    }

    /// Window allocation follows identity shards and key shards, each ascending.
    /// Freeze all selected owner orders last, also ascending and deduplicated,
    /// so preflight can fail without allocating any member of the window.
    pub(super) fn with_owners<T>(
        owners: &[(usize, &Self)],
        allocate: impl FnOnce(&mut LockedOwnerOrders<'_>) -> T,
    ) -> T {
        let mut owners = owners.to_vec();
        owners.sort_unstable_by_key(|entry| entry.0);
        owners.dedup_by_key(|entry| entry.0);
        let owners = owners
            .into_iter()
            .map(|(index, queue)| (index, queue, queue.order.lock()))
            .collect();
        allocate(&mut LockedOwnerOrders { owners })
    }

    #[cfg(test)]
    pub(crate) fn reserve(&self, allocate: impl FnOnce() -> u64) -> (u64, u64) {
        let mut order = self.order.lock();
        self.reserve_ordered(&mut order, allocate)
    }

    fn reserve_ordered(&self, order: &mut u64, allocate: impl FnOnce() -> u64) -> (u64, u64) {
        let owner_sequence = *order;
        assert!(owner_sequence < u64::MAX >> 2, "owner generation exhausted");
        // Global allocation inside this owner lock makes each physical stream's
        // versions strictly increasing even when independent callers encode.
        let sequence = allocate();
        let entry = self.entry(owner_sequence);
        *entry.batch.lock() = None;
        // Release announces the initialized generation. The worker stops at
        // ALLOCATED; no incomplete frame can precede an acknowledged later one.
        entry
            .state
            .store((owner_sequence << 2) | ALLOCATED, Ordering::Release);
        *order += 1;
        (owner_sequence, sequence)
    }

    /// Control enqueue and worker snapshots share the allocation order lock.
    /// No caller may wait for channel capacity while holding this lock.
    pub(crate) fn capture_order<T>(&self, capture: impl FnOnce(u64) -> T) -> T {
        capture(*self.order.lock())
    }

    pub(crate) fn head(&self) -> u64 {
        // Acquire observes the worker's ordered consumption frontier.
        self.head.load(Ordering::Acquire)
    }

    fn entry(&self, sequence: u64) -> &OwnerEntry {
        &self.entries[sequence as usize % self.entries.len()]
    }

    pub(crate) fn ready(&self, owner_sequence: u64, sequence: u64, batch: CompleteBatch) -> bool {
        let entry = self.entry(owner_sequence);
        *entry.batch.lock() = Some((sequence, batch));
        // Batch handoff: the batch mutex orders the payload, and the SeqCst
        // READY store followed by parked.load is the producer's Dekker half.
        entry
            .state
            .store((owner_sequence << 2) | READY, Ordering::SeqCst);
        self.take_wake()
    }

    pub(crate) fn abort(&self, owner_sequence: u64) -> bool {
        // Abortion also wakes a worker stopped at an ALLOCATED head. It has no
        // payload or remote effect and is skipped in physical owner order.
        self.entry(owner_sequence)
            .state
            .store((owner_sequence << 2) | ABORTED, Ordering::SeqCst);
        self.take_wake()
    }

    fn take_wake(&self) -> bool {
        // SeqCst pairs with worker parked.store followed by head state reload:
        // at least one side observes the other, so a wake cannot be lost.
        self.parked.load(Ordering::SeqCst) && self.parked.swap(false, Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn take_ready(
        &self,
        fits: impl FnMut(&CompleteBatch) -> bool,
    ) -> Option<(u64, CompleteBatch)> {
        self.take_ready_before(u64::MAX, fits)
    }

    pub(crate) fn take_ready_before(
        &self,
        before_owner_seq: u64,
        mut fits: impl FnMut(&CompleteBatch) -> bool,
    ) -> Option<(u64, CompleteBatch)> {
        for _ in 0..self.entries.len() {
            // Only this owner worker changes head. Acquire also supports park's
            // independent diagnostic/readiness observation without stale data.
            let head = self.head.load(Ordering::Acquire);
            let entry = self.entry(head);
            if head >= before_owner_seq {
                return None;
            }
            let state = entry.state.load(Ordering::Acquire);
            if state >> 2 != head || state & 3 == ALLOCATED {
                return None;
            }
            if state & 3 == ABORTED {
                self.head.store(head + 1, Ordering::Release);
                continue;
            }
            let mut batch = entry.batch.lock();
            if !batch.as_ref().is_some_and(|(_, batch)| fits(batch)) {
                return None;
            }
            let batch = batch.take();
            self.head.store(head + 1, Ordering::Release);
            return batch;
        }
        None
    }

    /// Diagnostic classification of the head after a drain stopped without a
    /// rejected batch. A concurrent READY store can race with this read; a
    /// READY or ABORTED head here was not READY when the drain looked.
    pub(crate) fn head_stop(&self, before_owner_seq: u64) -> HeadStop {
        // Only this owner worker changes head; Acquire matches take_ready_before.
        let head = self.head.load(Ordering::Acquire);
        if head >= before_owner_seq {
            return HeadStop::Fence;
        }
        let state = self.entry(head).state.load(Ordering::Acquire);
        if state >> 2 == head {
            HeadStop::Allocated
        } else {
            HeadStop::Empty
        }
    }

    /// Consecutive READY entries at the head before `before_owner_seq`, at most
    /// `limit`. Packing evidence only: callers may make more entries READY.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(crate) fn ready_run(&self, before_owner_seq: u64, limit: usize) -> usize {
        // Only this owner worker changes head; Acquire matches take_ready_before.
        let head = self.head.load(Ordering::Acquire);
        let mut run = 0;
        for sequence in head..before_owner_seq.min(head.saturating_add(limit as u64)) {
            if self.entry(sequence).state.load(Ordering::Acquire) != (sequence << 2) | READY {
                break;
            }
            run += 1;
        }
        run
    }

    /// True means the worker may block on its existing bounded command/wake
    /// route. A producer ignores a full wake channel: a command is already ready
    /// and the worker must check this queue before its next blocking wait.
    pub(crate) fn park(&self) -> bool {
        // Worker half of Dekker: SeqCst parked store, then head state reload.
        self.parked.store(true, Ordering::SeqCst);
        let head = self.head.load(Ordering::Acquire);
        let state = self.entry(head).state.load(Ordering::SeqCst);
        if state >> 2 == head && matches!(state & 3, READY | ABORTED) {
            self.unpark();
            false
        } else {
            true
        }
    }

    pub(crate) fn unpark(&self) {
        // SeqCst preserves the same parking/wakeup order on every wake route.
        self.parked.store(false, Ordering::SeqCst);
    }

    /// Stable test observation while the worker's client mutex prevents drain.
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    pub(crate) fn snapshot(&self) -> (u64, u64, Vec<(u64, u64, usize)>) {
        let order = self.order.lock();
        let next = *order;
        let head = self.head.load(Ordering::Acquire);
        assert!(next >= head && next - head <= self.entries.len() as u64);
        let mut ready = Vec::with_capacity((next - head) as usize);
        for owner_sequence in (head..next).take(self.entries.len()) {
            let entry = self.entry(owner_sequence);
            let state = entry.state.load(Ordering::Acquire);
            if state == ((owner_sequence << 2) | READY) {
                let batch = entry.batch.lock();
                let (sequence, batch) = batch.as_ref().expect("READY owns a complete batch");
                ready.push((owner_sequence, *sequence, batch.mutations().len()));
            }
        }
        (head, next, ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::{BatchLimits, Mutation};
    use crate::distributed::types::{BatchId, BatchVersion};
    use crate::key_shape::{KeyShape, KeySpace, KeyType};
    use minibytes::Bytes;
    use std::sync::{Barrier, mpsc};
    use std::thread;
    use std::time::Duration;

    fn batch() -> CompleteBatch {
        CompleteBatch::new(
            &KeyShape::new_single(8, 1, KeyType::uniform(1)),
            BatchId {
                database: [1; 16],
                operation: [2; 16],
            },
            BatchVersion {
                epoch: 1,
                sequence: 1,
            },
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: Bytes::from_static(b"12345678"),
                value: Bytes::from_static(b"value"),
            }],
            BatchLimits {
                max_operations: 8,
                max_encoded_bytes: 4096,
            },
        )
        .unwrap()
    }

    #[test]
    fn owner_queue_wake_never_lost() {
        const ENTRIES: usize = 100_000;
        let queue = OwnerQueue::new(ENTRIES);
        let next = AtomicU64::new(1);
        let template = batch();
        let start = Barrier::new(9);
        let (wake, pending) = mpsc::sync_channel(1);
        thread::scope(|scope| {
            for _ in 0..8 {
                let wake = &wake;
                let queue = &queue;
                let next = &next;
                let template = &template;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..ENTRIES / 8 {
                        let (owner_sequence, sequence) =
                            queue.reserve(|| next.fetch_add(1, Ordering::AcqRel));
                        assert_eq!(owner_sequence, sequence);
                        if queue.ready(owner_sequence, sequence, template.clone()) {
                            match wake.try_send(()) {
                                Ok(()) | Err(mpsc::TrySendError::Full(())) => {}
                                Err(mpsc::TrySendError::Disconnected(())) => {
                                    panic!("worker stopped")
                                }
                            }
                        }
                    }
                });
            }
            start.wait();
            for expected in 1..=ENTRIES as u64 {
                for attempt in 0..ENTRIES * 2 {
                    if let Some((sequence, _)) = queue.take_ready(|_| true) {
                        assert_eq!(sequence, expected);
                        break;
                    }
                    assert!(attempt + 1 < ENTRIES * 2, "queue drain exceeded its bound");
                    if queue.park() {
                        pending.recv_timeout(Duration::from_secs(5)).unwrap();
                        queue.unpark();
                    }
                }
            }
        });
        assert!(queue.take_ready(|_| true).is_none());
    }

    #[test]
    fn allocated_head_and_group_budget_block_later_entries() {
        let queue = OwnerQueue::new(2);
        let (first, _) = queue.reserve(|| 1);
        let (second, _) = queue.reserve(|| 2);
        queue.ready(second, 2, batch());
        assert!(queue.take_ready(|_| true).is_none());
        assert!(queue.park());
        assert!(queue.abort(first));
        assert!(queue.take_ready(|_| false).is_none());
        assert_eq!(queue.take_ready(|_| true).unwrap().0, 2);
        assert!(queue.take_ready(|_| true).is_none());
    }

    #[test]
    fn ordered_control_fence_stops_ready_and_aborted_entries() {
        let queue = OwnerQueue::new(4);
        let (first, _) = queue.reserve(|| 1);
        let fence = queue.capture_order(|next| next);
        let (second, _) = queue.reserve(|| 2);
        let (third, _) = queue.reserve(|| 3);
        queue.ready(first, 1, batch());
        queue.ready(second, 2, batch());
        queue.abort(third);
        assert_eq!(queue.take_ready_before(fence, |_| true).unwrap().0, 1);
        assert!(queue.take_ready_before(fence, |_| true).is_none());
        assert_eq!(queue.head(), fence);
        assert_eq!(queue.take_ready_before(third, |_| true).unwrap().0, 2);
        assert!(queue.take_ready_before(third, |_| true).is_none());
        assert_eq!(queue.head(), third);
        assert!(queue.take_ready(|_| true).is_none());
        assert_eq!(queue.head(), third + 1);
    }
}
