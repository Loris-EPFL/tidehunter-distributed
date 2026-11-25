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
