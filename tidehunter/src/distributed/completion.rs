//! Mutex-free completion handoff to a single database publication owner.
//!
//! The owner polls only outstanding slots. Completed slots may be retired after
//! their outcomes have another durable authority owner. Each producer ticket
//! owns its immutable result through `OnceLock`; uncertainty is monotonic.
//! This is a handoff component, not fencing, a publication engine or a proof of
//! end-to-end lock freedom. Tidehunter paper §§3.1/3.3 and `wal::tracker` require
//! a completed prefix: a missing result must never be synthesized into a hole.

use super::codec::CompleteBatch;
use super::storage::DurableBatch;
use super::types::{BatchId, BatchVersion, PayloadDigest};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
struct Slot {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    durable: OnceLock<DurableBatch>,
    unknown: AtomicBool,
    producer_active: AtomicBool,
}

struct OwnerEntry {
    slot: Arc<Slot>,
    unknown_delivered: bool,
    durable_delivered: bool,
}

/// Owner-only bounded history plus concurrently writable completion slots.
///
/// Register consecutive already-validated/admitted batches before dispatch.
/// The enclosing authority must reserve both its publication and completion
/// capacity before allocating a logical version; a registration failure must
/// never be skipped to dispatch a later sequence. Registration itself has no
/// storage effects and establishes no remote authority.
///
/// Retiring completed slots preserves the absolute sequence frontier. It cannot
/// discard unresolved results or reset an old identity to a new operation; the
/// enclosing database must consult its durable retry authority before admission.
pub struct CompletionInbox {
    database: [u8; 16],
    epoch: u64,
    capacity: usize,
    entries: VecDeque<OwnerEntry>,
    retained_start: usize,
    identities: HashMap<[u8; 16], usize>,
    outstanding: VecDeque<usize>,
}

/// Exactly one producer owns a ticket for a given identity at a time.
///
/// Dropping an incomplete ticket reports an unknown outcome. It does not abort
/// a write, free retained history or permit a different identity retry. A retry
/// requires the inbox owner to issue a new ticket for the same binding.
#[derive(Debug)]
#[must_use = "dropping an incomplete completion ticket reports an unknown outcome"]
pub struct CompletionTicket {
    slot: Arc<Slot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionEvent {
    Unknown { id: BatchId, version: BatchVersion },
    Durable(DurableBatch),
}

impl CompletionInbox {
    pub fn new(database: [u8; 16], epoch: u64, capacity: usize) -> Result<Self, CompletionError> {
        if capacity == 0 || capacity > 1_000_000 {
            return Err(CompletionError::InvalidCapacity);
        }
        Ok(Self {
            database,
            epoch,
            capacity,
            entries: VecDeque::new(),
            retained_start: 0,
            identities: HashMap::new(),
            outstanding: VecDeque::new(),
        })
    }

    pub fn register(&mut self, batch: &CompleteBatch) -> Result<CompletionTicket, CompletionError> {
        if batch.id().database != self.database {
            return Err(CompletionError::WrongDatabase);
        }
        if batch.version().epoch != self.epoch {
            return Err(CompletionError::WrongEpoch);
        }
        if let Some(&index) = self.identities.get(&batch.id().operation) {
            let slot = &self.entries[index - self.retained_start].slot;
            return Err(
                if slot.version == batch.version() && slot.digest == batch.digest() {
                    CompletionError::AlreadyRegistered
                } else {
                    CompletionError::ConflictingBinding
                },
            );
        }
        let next = self.retained_start + self.entries.len();
        if batch.version().sequence < next as u64 {
            return Err(CompletionError::ConflictingBinding);
        }
        if self.entries.len() == self.capacity {
            return Err(CompletionError::Capacity);
        }
        if batch.version().sequence != next as u64 {
            return Err(CompletionError::OutOfOrder {
                expected: next as u64,
                actual: batch.version().sequence,
            });
        }
        let slot = Arc::new(Slot {
            id: batch.id(),
            version: batch.version(),
            digest: batch.digest(),
            durable: OnceLock::new(),
            unknown: AtomicBool::new(false),
            producer_active: AtomicBool::new(true),
        });
        let index = next;
        self.entries.push_back(OwnerEntry {
            slot: slot.clone(),
            unknown_delivered: false,
            durable_delivered: false,
        });
        self.identities.insert(batch.id().operation, index);
        self.outstanding.push_back(index);
        Ok(CompletionTicket { slot })
    }

    /// Reissue the original binding only after its previous producer dropped.
    /// A timeout alone is insufficient while that ticket remains active.
    pub fn resume_ticket(&mut self, sequence: u64) -> Result<CompletionTicket, CompletionError> {
        let entry = usize::try_from(sequence)
            .ok()
            .and_then(|index| index.checked_sub(self.retained_start))
            .and_then(|index| self.entries.get(index))
            .ok_or(CompletionError::UnknownSequence)?;
        let slot = &entry.slot;
        if slot.durable.get().is_some() {
            return Err(CompletionError::AlreadyCompleted);
        }
        slot.producer_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| CompletionError::ProducerActive)?;
        // A producer could have completed between the first get and its Drop
        // release above. Acquiring its flag forbids reinitializing that result.
        if slot.durable.get().is_some() {
            slot.producer_active.store(false, Ordering::Release);
            return Err(CompletionError::AlreadyCompleted);
        }
        Ok(CompletionTicket { slot: slot.clone() })
    }

    /// Inspect at most `max_scanned` distinct outstanding slots without waiting.
    ///
    /// Each unknown notification is emitted at most once; a later durable
    /// receipt is emitted exactly once when polled. Notification order is not
    /// logical publication order: feed receipts to `PublicationTracker`.
    /// Calling with zero does no work. The owner must poll again while work is
    /// outstanding; producer tickets do not supply a wakeup primitive.
    pub fn poll(&mut self, max_scanned: usize) -> Vec<CompletionEvent> {
        let mut events = Vec::new();
        let scans = max_scanned.min(self.outstanding.len());
        for _ in 0..scans {
            let index = self
                .outstanding
                .pop_front()
                .expect("initial outstanding scan bound");
            let entry = &mut self.entries[index - self.retained_start];
            if let Some(&durable) = entry.slot.durable.get() {
                entry.durable_delivered = true;
                events.push(CompletionEvent::Durable(durable));
            } else {
                if !entry.unknown_delivered && entry.slot.unknown.load(Ordering::Acquire) {
                    events.push(CompletionEvent::Unknown {
                        id: entry.slot.id,
                        version: entry.slot.version,
                    });
                    entry.unknown_delivered = true;
                }
                self.outstanding.push_back(index);
            }
        }
        events
    }

    pub fn outstanding_batches(&self) -> usize {
        self.outstanding.len()
    }

    pub fn retained_batches(&self) -> usize {
        self.entries.len()
    }

    /// Drop only a contiguous prefix whose durable result was delivered and
    /// whose producer has finished. Durable retry authority must already exist.
    pub fn retire_completed(&mut self) -> usize {
        let before = self.retained_start;
        while self.entries.front().is_some_and(|entry| {
            entry.durable_delivered && !entry.slot.producer_active.load(Ordering::Acquire)
        }) {
            if let Some(entry) = self.entries.pop_front() {
                self.identities.remove(&entry.slot.id.operation);
                self.retained_start += 1;
            }
        }
        self.retained_start - before
    }
}

impl CompletionTicket {
    pub fn id(&self) -> BatchId {
        self.slot.id
    }

    pub fn version(&self) -> BatchVersion {
        self.slot.version
    }

    /// Report timeout/lost response without terminating a possible late result.
    pub fn report_unknown(&self) {
        self.slot.unknown.store(true, Ordering::Release);
    }

    /// Install the exact storage receipt. Consuming the sole ticket keeps
    /// `OnceLock` initialization uncontended. A mismatched receipt is rejected;
    /// Drop retains uncertainty about the intended operation in that case.
    pub fn complete(self, durable: DurableBatch) -> Result<(), CompletionError> {
        let appended = durable.appended();
        if appended.id() != self.slot.id
            || appended.version() != self.slot.version
            || appended.digest() != self.slot.digest
        {
            return Err(CompletionError::ConflictingBinding);
        }
        self.slot
            .durable
            .set(durable)
            .map_err(|_| CompletionError::AlreadyCompleted)
    }
}

impl Drop for CompletionTicket {
    fn drop(&mut self) {
        if self.slot.durable.get().is_none() {
            self.slot.unknown.store(true, Ordering::Release);
        }
        // New tickets cannot be issued until all accesses by this producer,
        // including the immutable result or unknown flag, precede this release.
        self.slot.producer_active.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionError {
    InvalidCapacity,
    WrongDatabase,
    WrongEpoch,
    Capacity,
    OutOfOrder { expected: u64, actual: u64 },
    ConflictingBinding,
    AlreadyRegistered,
    UnknownSequence,
    ProducerActive,
    AlreadyCompleted,
}

impl fmt::Display for CompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "distributed completion: {self:?}")
    }
}

impl std::error::Error for CompletionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::{BatchLimits, Mutation};
    use crate::distributed::group::{CompleteGroup, GroupLimits};
    use crate::distributed::publication::{PublicationState, PublicationTracker};
    use crate::distributed::storage::{PartitionConfig, PartitionLog, StaticInventory};
    use crate::distributed::types::RequestAuthority;
    use crate::key_shape::{KeyShape, KeySpace, KeyType};
    use crate::metrics::Metrics;

    const DATABASE: [u8; 16] = [8; 16];
    const EPOCH: u64 = 3;

    fn shape() -> KeyShape {
        KeyShape::new_single(4, 1, KeyType::uniform(1))
    }

    fn batch(sequence: u64, value: u8) -> CompleteBatch {
        CompleteBatch::new(
            &shape(),
            BatchId {
                database: DATABASE,
                operation: (sequence as u128).to_le_bytes(),
            },
            BatchVersion {
                epoch: EPOCH,
                sequence,
            },
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![value; 32].into(),
            }],
            BatchLimits {
                max_operations: 8,
                max_encoded_bytes: 2048,
            },
        )
        .unwrap()
    }

    fn receipts(batches: &[CompleteBatch]) -> Vec<DurableBatch> {
        let tmp = tempdir::TempDir::new("completion-receipts").unwrap();
        let config = PartitionConfig {
            inventory: StaticInventory {
                database: DATABASE,
                epoch: EPOCH,
                segment_generation: 1,
                partitions: vec![0],
            },
            partition: 0,
            batch_limits: BatchLimits {
                max_operations: 8,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: 8,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches: batches.len().max(1),
            max_reference_metadata_bytes: 4 * 1024 * 1024,
        };
        let log = PartitionLog::create(
            &tmp.path().join("partition"),
            config,
            &shape(),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap();
        batches
            .chunks(8)
            .flat_map(|batches| {
                let group = CompleteGroup::new(
                    &shape(),
                    batches.to_vec(),
                    log.config().batch_limits,
                    log.config().group_limits,
                )
                .unwrap();
                log.append_group(RequestAuthority { epoch: EPOCH }, &group)
                    .unwrap()
                    .into_iter()
                    .map(|appended| log.persist_through(appended).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn out_of_order_tickets_feed_contiguous_publication_without_losing_unknown_gap() {
        let batches = [batch(0, 1), batch(1, 2), batch(2, 3)];
        let receipts = receipts(&batches);
        let mut tracker = PublicationTracker::new(DATABASE, EPOCH, 3).unwrap();
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 3).unwrap();
        let mut tickets = Vec::new();
        for batch in &batches {
            tracker.admit(batch).unwrap();
            tracker.mark_in_flight(batch.id()).unwrap();
            tickets.push(inbox.register(batch).unwrap());
        }
        tickets.pop().unwrap().complete(receipts[2]).unwrap();
        tickets.pop().unwrap().complete(receipts[1]).unwrap();
        tickets[0].report_unknown();
        for event in inbox.poll(3) {
            match event {
                CompletionEvent::Unknown { id, .. } => {
                    tracker.mark_unknown(id).unwrap();
                }
                CompletionEvent::Durable(receipt) => {
                    tracker.record_durable(receipt).unwrap();
                }
            }
        }
        assert_eq!(tracker.durable_prefix().next_sequence(), 0);
        assert_eq!(
            tracker.state(batches[0].id()),
            Some(PublicationState::Unknown)
        );
        assert_eq!(inbox.outstanding_batches(), 1);
        assert!(inbox.poll(3).is_empty());
        tickets.pop().unwrap().complete(receipts[0]).unwrap();
        assert_eq!(inbox.poll(3), vec![CompletionEvent::Durable(receipts[0])]);
        tracker.record_durable(receipts[0]).unwrap();
        let mut order = Vec::new();
        tracker
            .publish_ready::<()>(|receipt| {
                order.push(receipt.appended().version().sequence);
                Ok(())
            })
            .unwrap();
        assert_eq!(order, vec![0, 1, 2]);
        assert_eq!(inbox.retained_batches(), 3);
        assert_eq!(inbox.outstanding_batches(), 0);
        assert!(inbox.poll(usize::MAX).is_empty());
    }

    #[test]
    fn dropped_or_unwinding_ticket_requires_same_identity_retry() {
        let batch = batch(0, 1);
        let receipt = receipts(std::slice::from_ref(&batch))[0];
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 1).unwrap();
        let ticket = inbox.register(&batch).unwrap();
        assert_eq!(
            inbox.resume_ticket(0).unwrap_err(),
            CompletionError::ProducerActive
        );
        let result = std::panic::catch_unwind(move || {
            let _ticket = ticket;
            panic!("producer disappeared after possible storage effects");
        });
        assert!(result.is_err());
        assert_eq!(
            inbox.poll(1),
            vec![CompletionEvent::Unknown {
                id: batch.id(),
                version: batch.version()
            }]
        );
        let retry = inbox.resume_ticket(0).unwrap();
        assert_eq!(retry.id(), batch.id());
        assert_eq!(retry.version(), batch.version());
        assert_eq!(
            inbox.resume_ticket(0).unwrap_err(),
            CompletionError::ProducerActive
        );
        retry.report_unknown();
        assert!(inbox.poll(1).is_empty());
        retry.complete(receipt).unwrap();
        assert_eq!(inbox.poll(1), vec![CompletionEvent::Durable(receipt)]);
        assert_eq!(
            inbox.resume_ticket(0).unwrap_err(),
            CompletionError::AlreadyCompleted
        );
    }

    #[test]
    fn completion_racing_resume_never_issues_competing_producer() {
        let batch = batch(0, 1);
        let receipt = receipts(std::slice::from_ref(&batch))[0];
        for _ in 0..64 {
            let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 1).unwrap();
            let ticket = inbox.register(&batch).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let producer_barrier = barrier.clone();
            let producer = std::thread::spawn(move || {
                producer_barrier.wait();
                ticket.complete(receipt).unwrap();
            });
            barrier.wait();
            assert!(matches!(
                inbox.resume_ticket(0),
                Err(CompletionError::ProducerActive | CompletionError::AlreadyCompleted)
            ));
            producer.join().unwrap();
            assert_eq!(inbox.poll(1), vec![CompletionEvent::Durable(receipt)]);
            assert_eq!(
                inbox.resume_ticket(0).unwrap_err(),
                CompletionError::AlreadyCompleted
            );
        }
    }

    #[test]
    fn wrong_receipt_fails_closed_and_full_history_cannot_be_reused() {
        let original = batch(0, 1);
        let changed = batch(0, 2);
        let wrong = receipts(&[changed])[0];
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 1).unwrap();
        let ticket = inbox.register(&original).unwrap();
        assert_eq!(
            ticket.complete(wrong),
            Err(CompletionError::ConflictingBinding)
        );
        assert_eq!(
            inbox.poll(1),
            vec![CompletionEvent::Unknown {
                id: original.id(),
                version: original.version()
            }]
        );
        assert_eq!(
            inbox.register(&original).unwrap_err(),
            CompletionError::AlreadyRegistered
        );
        assert_eq!(
            inbox.register(&batch(1, 3)).unwrap_err(),
            CompletionError::Capacity
        );
        assert_eq!(inbox.retained_batches(), 1);
        assert_eq!(
            inbox.resume_ticket(u64::MAX).unwrap_err(),
            CompletionError::UnknownSequence
        );
    }

    #[test]
    fn poll_budget_rotates_only_outstanding_slots() {
        let batches: Vec<_> = (0..8).map(|sequence| batch(sequence, 1)).collect();
        let receipts = receipts(&batches);
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 8).unwrap();
        let mut tickets: Vec<_> = batches
            .iter()
            .map(|batch| inbox.register(batch).unwrap())
            .collect();
        tickets.pop().unwrap().complete(receipts[7]).unwrap();
        assert!(inbox.poll(0).is_empty());
        for _ in 0..7 {
            assert!(inbox.poll(1).is_empty());
        }
        assert_eq!(inbox.poll(1), vec![CompletionEvent::Durable(receipts[7])]);
        assert_eq!(inbox.outstanding_batches(), 7);
        assert_eq!(inbox.retained_batches(), 8);
        assert!(!inbox.outstanding.contains(&7));
        drop(tickets);
        let events: Vec<_> = (0..7).flat_map(|_| inbox.poll(1)).collect();
        assert_eq!(events.len(), 7);
        assert!(
            events
                .iter()
                .all(|event| matches!(event, CompletionEvent::Unknown { .. }))
        );
        assert!(inbox.poll(usize::MAX).is_empty());

        // Rotate past the beginning, then mix ready and unknown slots. Each
        // slot must be visited once even when removal changes the worklist.
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 8).unwrap();
        let mut tickets: Vec<_> = batches
            .iter()
            .map(|batch| inbox.register(batch).unwrap())
            .collect();
        assert!(inbox.poll(3).is_empty());
        tickets.remove(0).complete(receipts[0]).unwrap();
        drop(tickets);
        let events = inbox.poll(8);
        assert_eq!(events.len(), 8);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CompletionEvent::Durable(_)))
                .count(),
            1
        );
        assert_eq!(inbox.outstanding_batches(), 7);
        assert!(inbox.poll(8).is_empty());
    }

    #[test]
    fn registration_rejects_sparse_sequence_and_wrong_domain_before_retention() {
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 2).unwrap();
        assert_eq!(
            inbox.register(&batch(u64::MAX, 1)).unwrap_err(),
            CompletionError::OutOfOrder {
                expected: 0,
                actual: u64::MAX
            }
        );
        assert_eq!(inbox.retained_batches(), 0);
        assert_eq!(inbox.outstanding_batches(), 0);
        let mut wrong_db = CompletionInbox::new([9; 16], EPOCH, 2).unwrap();
        assert_eq!(
            wrong_db.register(&batch(0, 1)).unwrap_err(),
            CompletionError::WrongDatabase
        );
        let mut wrong_epoch = CompletionInbox::new(DATABASE, EPOCH + 1, 2).unwrap();
        assert_eq!(
            wrong_epoch.register(&batch(0, 1)).unwrap_err(),
            CompletionError::WrongEpoch
        );
        assert!(matches!(
            CompletionInbox::new(DATABASE, EPOCH, 0),
            Err(CompletionError::InvalidCapacity)
        ));
        assert!(matches!(
            CompletionInbox::new(DATABASE, EPOCH, 1_000_001),
            Err(CompletionError::InvalidCapacity)
        ));
    }

    #[test]
    fn completed_slot_retirement_preserves_absolute_sequence_beyond_capacity() {
        let batches: Vec<_> = (0..8)
            .map(|sequence| batch(sequence, sequence as u8))
            .collect();
        let durable = receipts(&batches);
        let mut inbox = CompletionInbox::new(DATABASE, EPOCH, 1).unwrap();
        for (batch, durable) in batches.iter().zip(durable) {
            let ticket = inbox.register(batch).unwrap();
            assert_eq!(inbox.retire_completed(), 0);
            ticket.complete(durable).unwrap();
            assert_eq!(
                inbox.retire_completed(),
                0,
                "undelivered result must remain owned"
            );
            assert_eq!(inbox.poll(1), vec![CompletionEvent::Durable(durable)]);
            assert_eq!(inbox.retire_completed(), 1);
            assert_eq!(inbox.retained_batches(), 0);
            assert_eq!(inbox.outstanding_batches(), 0);
            assert_eq!(
                inbox.resume_ticket(batch.version().sequence).unwrap_err(),
                CompletionError::UnknownSequence
            );
        }
        assert_eq!(
            inbox.register(&batches[0]).unwrap_err(),
            CompletionError::ConflictingBinding
        );
    }
}
