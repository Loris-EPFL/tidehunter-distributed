//! Bounded logical progress for one externally fenced database authority.
//!
//! Paper §§3.1/3.3 and native `index::pending_table::Transaction` distinguish
//! whole-batch visibility from append completion. The caller supplies the actual
//! atomic native-index callback; this module is not a serving database. See
//! `docs/distributed/NATIVE_INTEGRATION_MAP_2026-10-06.md` for that integration gate.

use super::codec::CompleteBatch;
use super::storage::DurableBatch;
use super::types::{BatchId, BatchVersion, MutationVersion, PayloadDigest};
use std::collections::HashMap;
use std::fmt;

/// Whole batches acknowledged durable in a contiguous prefix, independent of
/// whether the native index has published them. `next_sequence` is exclusive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableBatchPrefix {
    database: [u8; 16],
    epoch: u64,
    next_sequence: u64,
}

impl DurableBatchPrefix {
    pub fn database(self) -> [u8; 16] {
        self.database
    }

    pub fn epoch(self) -> u64 {
        self.epoch
    }

    pub fn next_sequence(self) -> u64 {
        self.next_sequence
    }
}

/// Whole batches whose ordered publication callbacks returned success.
///
/// This is a logical cut, never a byte address or proof of native index contents
/// without a correctly wired callback. All mutations of an included batch are
/// covered, including repeated-key mutations distinguished by operation index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedBatchPrefix {
    database: [u8; 16],
    epoch: u64,
    next_sequence: u64,
}

impl PublishedBatchPrefix {
    pub fn database(self) -> [u8; 16] {
        self.database
    }

    pub fn epoch(self) -> u64 {
        self.epoch
    }

    pub fn next_sequence(self) -> u64 {
        self.next_sequence
    }

    /// The caller must already be operating in this prefix's database domain.
    pub fn contains(self, mutation: MutationVersion) -> bool {
        mutation.batch.epoch == self.epoch && mutation.batch.sequence < self.next_sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationState {
    Admitted,
    InFlight,
    /// The request may have taken effect. A timeout never proves a hole.
    Unknown,
    Durable,
    Published,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    version: BatchVersion,
    digest: PayloadDigest,
    state: PublicationState,
    durable: Option<DurableBatch>,
}

/// Volatile state machine for a single database epoch, beginning at sequence 0.
///
/// Admission is serialized with version allocation by the enclosing authority:
/// validated batches must be admitted in consecutive sequence order before any
/// storage request. Their storage work and durable responses may complete in any
/// order. Only in-flight ordering state belongs here. The enclosing authority
/// retains exact completed retry outcomes in its WAL-backed/pageable history
/// before calling `retire_published`; retirement never resolves an unknown hole.
///
/// Construction establishes no lease or fencing. The caller must first register
/// all streams, establish exclusive authority, and finish old-epoch recovery.
/// This component cannot be reconstructed in the same live epoch after a crash.
/// There is no live `mark_hole` operation: recovery must obtain sealed inventory
/// evidence and rebuild native state before creating the next epoch's tracker.
pub struct PublicationTracker {
    database: [u8; 16],
    epoch: u64,
    capacity: usize,
    next_admission: u64,
    durable_prefix: u64,
    published_prefix: u64,
    // Admission is [retained_start, next_admission). Retired complete prefixes
    // remain represented by the monotonic durable/publication counters.
    retained_start: u64,
    entries: std::collections::VecDeque<Entry>,
    // The database is fixed by this tracker and checked before every lookup.
    // Randomized standard hashing retains collision resistance for caller IDs;
    // hash equality never substitutes for full 128-bit operation equality.
    identities: HashMap<[u8; 16], u64>,
    recovery_required: bool,
}

impl PublicationTracker {
    pub fn new(database: [u8; 16], epoch: u64, capacity: usize) -> Result<Self, PublicationError> {
        if capacity == 0 || capacity > 1_000_000 {
            return Err(PublicationError::InvalidCapacity);
        }
        Ok(Self {
            database,
            epoch,
            capacity,
            next_admission: 0,
            durable_prefix: 0,
            published_prefix: 0,
            retained_start: 0,
            entries: std::collections::VecDeque::new(),
            identities: HashMap::new(),
            recovery_required: false,
        })
    }

    /// Admit an already validated immutable authority record, before append.
    /// An identical retry resolves to the retained state even at full capacity.
    pub fn admit(&mut self, batch: &CompleteBatch) -> Result<PublicationState, PublicationError> {
        self.ensure_publication_healthy()?;
        self.check_domain(batch.id(), batch.version())?;
        if let Some(&slot) = self.identities.get(&batch.id().operation) {
            let existing = &self.entries[(slot - self.retained_start) as usize];
            if existing.version != batch.version() || existing.digest != batch.digest() {
                return Err(PublicationError::ConflictingIdentity);
            }
            return Ok(existing.state);
        }
        if batch.version().sequence < self.next_admission {
            return Err(PublicationError::ConflictingVersion);
        }
        if self.entries.len() == self.capacity {
            return Err(PublicationError::Capacity);
        }
        if batch.version().sequence != self.next_admission {
            return Err(PublicationError::OutOfOrderAdmission {
                expected: self.next_admission,
                actual: batch.version().sequence,
            });
        }
        let next = self
            .next_admission
            .checked_add(1)
            .ok_or(PublicationError::SequenceExhausted)?;
        let entry = Entry {
            version: batch.version(),
            digest: batch.digest(),
            state: PublicationState::Admitted,
            durable: None,
        };
        let slot = self.next_admission;
        self.entries.push_back(entry);
        self.identities.insert(batch.id().operation, slot);
        self.next_admission = next;
        Ok(PublicationState::Admitted)
    }

    /// Call before a storage request can have effects. Retrying an unknown
    /// request retains Unknown; an attempted send does not resolve its outcome.
    pub fn mark_in_flight(&mut self, id: BatchId) -> Result<PublicationState, PublicationError> {
        self.ensure_publication_healthy()?;
        let entry = self.entry_mut(id)?;
        if entry.state == PublicationState::Admitted {
            entry.state = PublicationState::InFlight;
        }
        Ok(entry.state)
    }

    /// Record response loss/uncertain I/O without turning it into rejection.
    /// A late timeout cannot undo a durable receipt or successful publication.
    pub fn mark_unknown(&mut self, id: BatchId) -> Result<PublicationState, PublicationError> {
        let entry = self.entry_mut(id)?;
        match entry.state {
            PublicationState::Admitted => return Err(PublicationError::NotInFlight),
            PublicationState::InFlight => entry.state = PublicationState::Unknown,
            _ => {}
        }
        Ok(entry.state)
    }

    /// Accept a storage-verified receipt only for the exact admitted record.
    /// This does not publish it. Late receipts remain recordable after a failed
    /// publication callback so recovery diagnostics retain the known history.
    pub fn record_durable(
        &mut self,
        durable: DurableBatch,
    ) -> Result<PublicationState, PublicationError> {
        let appended = durable.appended();
        self.check_domain(appended.id(), appended.version())?;
        let entry = self.entry_mut(appended.id())?;
        if entry.version != appended.version() || entry.digest != appended.digest() {
            return Err(PublicationError::ConflictingIdentity);
        }
        if let Some(existing) = entry.durable {
            if existing != durable {
                return Err(PublicationError::ConflictingReceipt);
            }
            return Ok(entry.state);
        }
        if entry.state == PublicationState::Admitted {
            return Err(PublicationError::NotInFlight);
        }
        entry.durable = Some(durable);
        entry.state = PublicationState::Durable;
        // Prefixes cannot exceed the configured usize capacity: every advance
        // consumes an admitted dense slot. External sequences are never cast.
        while self
            .entries
            .get((self.durable_prefix - self.retained_start) as usize)
            .is_some_and(|entry| entry.durable.is_some())
        {
            // Every admitted sequence was checked to have a successor.
            self.durable_prefix += 1;
        }
        Ok(PublicationState::Durable)
    }

    /// Invoke the actual atomic publication callback for each contiguous durable
    /// batch. A callback must install all native pending entries and commit their
    /// shared Transaction before returning success. It must never acknowledge an
    /// application from inside the callback.
    ///
    /// Failure (including unwinding panic) leaves the tracker recovery-required:
    /// the callback may already have made effects visible. The enclosing DB must
    /// gate reads, cached reads and writes on `ensure_publication_healthy` and
    /// rebuild from authoritative storage before serving again. No retry/reset
    /// method here can guess whether the callback committed before failing.
    pub fn publish_ready<E>(
        &mut self,
        mut publish: impl FnMut(DurableBatch) -> Result<(), E>,
    ) -> Result<PublicationProgress, PublishError<E>> {
        self.ensure_publication_healthy()
            .map_err(PublishError::State)?;
        let before = self.published_prefix;
        while let Some(durable) = self
            .entries
            .get((self.published_prefix - self.retained_start) as usize)
            .and_then(|entry| entry.durable)
        {
            // Set before user code: an unwinding callback must also fail closed.
            self.recovery_required = true;
            publish(durable).map_err(PublishError::Callback)?;
            self.entries
                .get_mut((self.published_prefix - self.retained_start) as usize)
                .expect("published entry remains admitted")
                .state = PublicationState::Published;
            self.published_prefix += 1;
            self.recovery_required = false;
        }
        Ok(PublicationProgress {
            callbacks_completed: self.published_prefix - before,
            published: self.published_prefix(),
            blocked_on: (self.published_prefix < self.next_admission).then_some(BatchVersion {
                epoch: self.epoch,
                sequence: self.published_prefix,
            }),
        })
    }

    pub fn state(&self, id: BatchId) -> Option<PublicationState> {
        if id.database != self.database {
            return None;
        }
        self.identities
            .get(&id.operation)
            .map(|&slot| self.entries[(slot - self.retained_start) as usize].state)
    }

    pub fn retained_batches(&self) -> usize {
        self.entries.len()
    }

    /// Charged collection storage, excluding allocator overhead.
    pub fn resident_metadata_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Entry>()
            + self.identities.capacity() * std::mem::size_of::<([u8; 16], u64)>()
    }

    /// Release a completely published prefix only after its exact identity,
    /// version and digest have another retained authority owner. This changes no
    /// logical counter and can never release an unresolved/in-flight outcome.
    pub fn retire_published(&mut self) -> Result<(), PublicationError> {
        self.ensure_publication_healthy()?;
        let before = self.retained_start;
        let through = self.published_prefix;
        for _ in before..through {
            let entry = self
                .entries
                .pop_front()
                .ok_or(PublicationError::UnknownBatch)?;
            let receipt = entry.durable.ok_or(PublicationError::UnknownBatch)?;
            self.identities.remove(&receipt.appended().id().operation);
        }
        self.retained_start = through;
        Ok(())
    }

    pub fn durable_prefix(&self) -> DurableBatchPrefix {
        DurableBatchPrefix {
            database: self.database,
            epoch: self.epoch,
            next_sequence: self.durable_prefix,
        }
    }

    pub fn published_prefix(&self) -> PublishedBatchPrefix {
        PublishedBatchPrefix {
            database: self.database,
            epoch: self.epoch,
            next_sequence: self.published_prefix,
        }
    }

    pub fn ensure_publication_healthy(&self) -> Result<(), PublicationError> {
        if self.recovery_required {
            Err(PublicationError::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    fn entry_mut(&mut self, id: BatchId) -> Result<&mut Entry, PublicationError> {
        if id.database != self.database {
            return Err(PublicationError::UnknownBatch);
        }
        let &slot = self
            .identities
            .get(&id.operation)
            .ok_or(PublicationError::UnknownBatch)?;
        Ok(self
            .entries
            .get_mut((slot - self.retained_start) as usize)
            .expect("identity map and dense admission slots change together"))
    }

    fn check_domain(&self, id: BatchId, version: BatchVersion) -> Result<(), PublicationError> {
        if id.database != self.database {
            return Err(PublicationError::WrongDatabase);
        }
        if version.epoch != self.epoch {
            return Err(PublicationError::WrongEpoch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationProgress {
    pub callbacks_completed: u64,
    pub published: PublishedBatchPrefix,
    pub blocked_on: Option<BatchVersion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationError {
    InvalidCapacity,
    Capacity,
    SequenceExhausted,
    WrongDatabase,
    WrongEpoch,
    ConflictingIdentity,
    ConflictingVersion,
    ConflictingReceipt,
    OutOfOrderAdmission { expected: u64, actual: u64 },
    UnknownBatch,
    NotInFlight,
    RecoveryRequired,
}

impl fmt::Display for PublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "logical publication: {self:?}")
    }
}

impl std::error::Error for PublicationError {}

#[derive(Debug, Eq, PartialEq)]
pub enum PublishError<E> {
    State(PublicationError),
    Callback(E),
}

impl<E: fmt::Display> fmt::Display for PublishError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::State(error) => error.fmt(f),
            Self::Callback(error) => write!(f, "publication callback requires recovery: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PublishError<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::{BatchLimits, Mutation};
    use crate::distributed::group::{CompleteGroup, GroupLimits};
    use crate::distributed::storage::{PartitionConfig, PartitionLog, StaticInventory};
    use crate::distributed::types::RequestAuthority;
    use crate::key_shape::{KeyShape, KeySpace, KeyType};
    use crate::metrics::Metrics;

    const DATABASE: [u8; 16] = [4; 16];
    const EPOCH: u64 = 8;

    fn shape() -> KeyShape {
        KeyShape::new_single(4, 1, KeyType::uniform(1))
    }

    fn batch(sequence: u64, identity: u8, value: u8) -> CompleteBatch {
        batch_with_operation(sequence, [identity; 16], value)
    }

    fn batch_with_operation(sequence: u64, operation: [u8; 16], value: u8) -> CompleteBatch {
        CompleteBatch::new(
            &shape(),
            BatchId {
                database: DATABASE,
                operation,
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

    fn durable_batches(batches: &[CompleteBatch]) -> Vec<DurableBatch> {
        let tmp = tempdir::TempDir::new("publication-receipts").unwrap();
        let config = PartitionConfig {
            inventory: StaticInventory {
                database: DATABASE,
                epoch: EPOCH,
                segment_generation: 3,
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
            .flat_map(|group| {
                let group = CompleteGroup::new(
                    &shape(),
                    group.to_vec(),
                    log.config().batch_limits,
                    log.config().group_limits,
                )
                .unwrap();
                let appended = log
                    .append_group(RequestAuthority { epoch: EPOCH }, &group)
                    .unwrap();
                appended
                    .into_iter()
                    .map(|appended| log.persist_through(appended).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn admitted(batches: &[CompleteBatch]) -> PublicationTracker {
        let mut tracker = PublicationTracker::new(DATABASE, EPOCH, batches.len()).unwrap();
        for batch in batches {
            tracker.admit(batch).unwrap();
            tracker.mark_in_flight(batch.id()).unwrap();
        }
        tracker
    }

    #[test]
    fn bounded_admission_preserves_retry_identity_and_version() {
        let mut tracker = PublicationTracker::new(DATABASE, EPOCH, 2).unwrap();
        let first = batch(0, 1, 1);
        tracker.admit(&first).unwrap();
        assert_eq!(
            tracker.admit(&batch(0, 1, 2)),
            Err(PublicationError::ConflictingIdentity)
        );
        assert_eq!(
            tracker.admit(&batch(1, 1, 1)),
            Err(PublicationError::ConflictingIdentity)
        );
        assert_eq!(
            tracker.admit(&batch(0, 2, 1)),
            Err(PublicationError::ConflictingVersion)
        );
        assert_eq!(
            tracker.admit(&batch(2, 2, 1)),
            Err(PublicationError::OutOfOrderAdmission {
                expected: 1,
                actual: 2
            })
        );
        tracker.admit(&batch(1, 2, 1)).unwrap();
        assert_eq!(tracker.admit(&first), Ok(PublicationState::Admitted));
        assert_eq!(
            tracker.admit(&batch(2, 3, 1)),
            Err(PublicationError::Capacity)
        );
        assert_eq!(tracker.retained_batches(), 2);
        assert_eq!(tracker.durable_prefix().next_sequence(), 0);
        assert!(matches!(
            PublicationTracker::new(DATABASE, EPOCH, 0),
            Err(PublicationError::InvalidCapacity)
        ));
    }

    #[test]
    fn reverse_durable_responses_publish_only_in_logical_order() {
        let batches = [batch(0, 1, 1), batch(1, 2, 2), batch(2, 3, 3)];
        let receipts = durable_batches(&batches);
        let mut tracker = admitted(&batches);
        tracker.record_durable(receipts[2]).unwrap();
        tracker.record_durable(receipts[1]).unwrap();
        assert_eq!(tracker.durable_prefix().next_sequence(), 0);
        let blocked = tracker
            .publish_ready::<()>(|_| panic!("gap skipped"))
            .unwrap();
        assert_eq!(blocked.callbacks_completed, 0);
        assert_eq!(blocked.blocked_on, Some(batches[0].version()));
        tracker.record_durable(receipts[0]).unwrap();
        assert_eq!(tracker.durable_prefix().next_sequence(), 3);
        assert_eq!(tracker.published_prefix().next_sequence(), 0);
        let mut observed = vec![];
        let progress = tracker
            .publish_ready::<()>(|receipt| {
                observed.push(receipt.appended().version().sequence);
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, vec![0, 1, 2]);
        assert_eq!(progress.callbacks_completed, 3);
        assert_eq!(progress.blocked_on, None);
        assert_eq!(tracker.retained_batches(), 3);
        assert_eq!(tracker.admit(&batches[0]), Ok(PublicationState::Published));
        assert_eq!(
            tracker.record_durable(receipts[0]),
            Ok(PublicationState::Published)
        );
        assert_eq!(
            tracker
                .publish_ready::<()>(|_| panic!("republished"))
                .unwrap()
                .callbacks_completed,
            0
        );
        assert!(progress.published.contains(MutationVersion {
            batch: batches[2].version(),
            operation_index: u32::MAX,
        }));
        assert!(!progress.published.contains(MutationVersion {
            batch: BatchVersion {
                epoch: EPOCH + 1,
                sequence: 0
            },
            operation_index: 0,
        }));
    }

    #[test]
    fn timeout_preserves_gap_until_matching_durable_receipt_arrives() {
        let batches = [batch(0, 1, 1), batch(1, 2, 2)];
        let receipts = durable_batches(&batches);
        let mut tracker = admitted(&batches);
        tracker.mark_unknown(batches[0].id()).unwrap();
        tracker.record_durable(receipts[1]).unwrap();
        assert_eq!(
            tracker.mark_in_flight(batches[0].id()),
            Ok(PublicationState::Unknown)
        );
        for _ in 0..100 {
            tracker.mark_unknown(batches[0].id()).unwrap();
            tracker
                .publish_ready::<()>(|_| panic!("timeout became hole"))
                .unwrap();
        }
        assert_eq!(tracker.published_prefix().next_sequence(), 0);
        tracker.record_durable(receipts[0]).unwrap();
        tracker.publish_ready::<()>(|_| Ok(())).unwrap();
        assert_eq!(tracker.published_prefix().next_sequence(), 2);
        assert_eq!(
            tracker.mark_unknown(batches[0].id()),
            Ok(PublicationState::Published)
        );
    }

    #[test]
    fn receipt_must_match_admitted_identity_payload_epoch_and_send_state() {
        let original = batch(0, 1, 1);
        let changed = batch(0, 1, 2);
        let receipts = durable_batches(&[changed]);
        let mut tracker = admitted(&[original]);
        assert_eq!(
            tracker.record_durable(receipts[0]),
            Err(PublicationError::ConflictingIdentity)
        );
        assert_eq!(tracker.durable_prefix().next_sequence(), 0);
        let mut wrong_epoch = PublicationTracker::new(DATABASE, EPOCH + 1, 1).unwrap();
        assert_eq!(
            wrong_epoch.record_durable(receipts[0]),
            Err(PublicationError::WrongEpoch)
        );
        let mut wrong_db = PublicationTracker::new([9; 16], EPOCH, 1).unwrap();
        assert_eq!(
            wrong_db.record_durable(receipts[0]),
            Err(PublicationError::WrongDatabase)
        );
        let mut unsent = PublicationTracker::new(DATABASE, EPOCH, 1).unwrap();
        unsent.admit(&batch(0, 1, 2)).unwrap();
        assert_eq!(
            unsent.mark_unknown(receipts[0].appended().id()),
            Err(PublicationError::NotInFlight)
        );
        assert_eq!(
            unsent.record_durable(receipts[0]),
            Err(PublicationError::NotInFlight)
        );
    }

    #[test]
    fn callback_failure_retains_durable_future_and_requires_authority_recovery() {
        let batches = [batch(0, 1, 1), batch(1, 2, 2), batch(2, 3, 3)];
        let receipts = durable_batches(&batches);
        let mut tracker = admitted(&batches);
        tracker.record_durable(receipts[0]).unwrap();
        tracker.record_durable(receipts[1]).unwrap();
        let result = tracker.publish_ready(|receipt| {
            if receipt.appended().version().sequence == 1 {
                Err("index callback result uncertain")
            } else {
                Ok(())
            }
        });
        assert_eq!(
            result,
            Err(PublishError::Callback("index callback result uncertain"))
        );
        assert_eq!(tracker.published_prefix().next_sequence(), 1);
        assert_eq!(
            tracker.state(batches[1].id()),
            Some(PublicationState::Durable)
        );
        tracker.record_durable(receipts[2]).unwrap();
        assert_eq!(tracker.durable_prefix().next_sequence(), 3);
        assert_eq!(tracker.retained_batches(), 3);
        assert_eq!(
            tracker.ensure_publication_healthy(),
            Err(PublicationError::RecoveryRequired)
        );
        assert_eq!(
            tracker.admit(&batches[0]),
            Err(PublicationError::RecoveryRequired)
        );
        assert_eq!(
            tracker.publish_ready::<()>(|_| panic!("retried uncertain callback")),
            Err(PublishError::State(PublicationError::RecoveryRequired))
        );
    }

    #[test]
    fn callback_panic_also_prevents_resume_and_false_prefix_advance() {
        let batches = [batch(0, 1, 1)];
        let receipt = durable_batches(&batches)[0];
        let mut tracker = admitted(&batches);
        tracker.record_durable(receipt).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = tracker.publish_ready::<()>(|_| panic!("simulated native callback panic"));
        }));
        assert!(result.is_err());
        assert_eq!(tracker.published_prefix().next_sequence(), 0);
        assert_eq!(
            tracker.state(batches[0].id()),
            Some(PublicationState::Durable)
        );
        assert_eq!(
            tracker.ensure_publication_healthy(),
            Err(PublicationError::RecoveryRequired)
        );
    }

    #[test]
    fn large_requested_sequence_never_allocates_sparse_slots() {
        let mut tracker = PublicationTracker::new(DATABASE, EPOCH, 1_000_000).unwrap();
        assert_eq!(tracker.entries.capacity(), 0);
        assert_eq!(tracker.identities.capacity(), 0);
        for sequence in [u32::MAX as u64, u64::MAX - 1, u64::MAX] {
            assert_eq!(
                tracker.admit(&batch(sequence, 1, 1)),
                Err(PublicationError::OutOfOrderAdmission {
                    expected: 0,
                    actual: sequence,
                })
            );
            assert_eq!(tracker.entries.capacity(), 0);
            assert_eq!(tracker.identities.capacity(), 0);
        }
        tracker.admit(&batch(0, 1, 1)).unwrap();
        assert_eq!(tracker.retained_batches(), 1);
        assert_eq!(tracker.entries[0].version.sequence, 0);
        assert!(matches!(
            PublicationTracker::new(DATABASE, EPOCH, 1_000_001),
            Err(PublicationError::InvalidCapacity)
        ));
    }

    #[test]
    fn operation_lookup_preserves_database_and_all_identity_bits() {
        let original = batch(0, 1, 1);
        let mut tracker = PublicationTracker::new(DATABASE, EPOCH, 2).unwrap();
        tracker.admit(&original).unwrap();
        let wrong_database = BatchId {
            database: [5; 16],
            operation: original.id().operation,
        };
        assert_eq!(tracker.state(wrong_database), None);
        assert_eq!(
            tracker.mark_in_flight(wrong_database),
            Err(PublicationError::UnknownBatch)
        );
        assert_eq!(
            tracker.mark_unknown(wrong_database),
            Err(PublicationError::UnknownBatch)
        );
        let mut second_operation = original.id().operation;
        second_operation[15] ^= 0x80;
        let second = batch_with_operation(1, second_operation, 1);
        tracker.admit(&second).unwrap();
        tracker.mark_in_flight(second.id()).unwrap();
        assert_eq!(
            tracker.state(original.id()),
            Some(PublicationState::Admitted)
        );
        assert_eq!(tracker.state(second.id()), Some(PublicationState::InFlight));
        assert_eq!(tracker.retained_batches(), 2);
    }

    #[test]
    fn long_unknown_gap_retains_every_receipt_and_publishes_once_when_resolved() {
        const COUNT: usize = 1025;
        let batches: Vec<_> = (0..COUNT)
            .map(|sequence| {
                batch_with_operation(sequence as u64, (sequence as u128).to_le_bytes(), 1)
            })
            .collect();
        let receipts = durable_batches(&batches);
        let mut tracker = admitted(&batches);
        tracker.mark_unknown(batches[0].id()).unwrap();
        for sequence in (1..COUNT).rev() {
            tracker.record_durable(receipts[sequence]).unwrap();
        }
        assert_eq!(tracker.durable_prefix().next_sequence(), 0);
        assert_eq!(tracker.retained_batches(), COUNT);
        assert_eq!(
            tracker
                .publish_ready::<()>(|_| panic!("unresolved gap skipped"))
                .unwrap()
                .callbacks_completed,
            0
        );
        for sequence in [1, 63, 64, 255, 256, 1023, 1024] {
            assert_eq!(
                tracker.admit(&batches[sequence]),
                Ok(PublicationState::Durable)
            );
        }
        tracker.record_durable(receipts[0]).unwrap();
        assert_eq!(tracker.durable_prefix().next_sequence(), COUNT as u64);
        let mut next = 0;
        let progress = tracker
            .publish_ready::<()>(|receipt| {
                assert_eq!(receipt.appended().version().sequence, next);
                next += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(progress.callbacks_completed, COUNT as u64);
        assert_eq!(progress.blocked_on, None);
        assert_eq!(next, COUNT as u64);
        assert_eq!(tracker.retained_batches(), COUNT);
        assert_eq!(
            tracker.admit(&batch_with_operation(
                COUNT as u64,
                (COUNT as u128).to_le_bytes(),
                1
            )),
            Err(PublicationError::Capacity)
        );
        assert_eq!(tracker.admit(&batches[0]), Ok(PublicationState::Published));
        assert_eq!(
            tracker
                .publish_ready::<()>(|_| panic!("published twice"))
                .unwrap()
                .callbacks_completed,
            0
        );
    }

    #[test]
    fn published_retirement_preserves_global_counters_and_never_releases_unknown_gaps() {
        let batches: Vec<_> = (0..128u64)
            .map(|sequence| batch_with_operation(sequence, (sequence as u128).to_le_bytes(), 1))
            .collect();
        let receipts = durable_batches(&batches);
        let mut tracker = PublicationTracker::new(DATABASE, EPOCH, 2).unwrap();
        for batch in &batches[..2] {
            tracker.admit(batch).unwrap();
            tracker.mark_in_flight(batch.id()).unwrap();
        }
        tracker.mark_unknown(batches[0].id()).unwrap();
        tracker.record_durable(receipts[1]).unwrap();
        tracker.retire_published().unwrap();
        assert_eq!(tracker.retained_batches(), 2);
        assert_eq!(tracker.published_prefix().next_sequence(), 0);
        tracker.record_durable(receipts[0]).unwrap();
        tracker.publish_ready::<()>(|_| Ok(())).unwrap();
        tracker.retire_published().unwrap();
        assert_eq!(tracker.retained_batches(), 0);
        for sequence in 2..batches.len() {
            tracker.admit(&batches[sequence]).unwrap();
            tracker.mark_in_flight(batches[sequence].id()).unwrap();
            tracker.record_durable(receipts[sequence]).unwrap();
            tracker.publish_ready::<()>(|_| Ok(())).unwrap();
            tracker.retire_published().unwrap();
            assert_eq!(tracker.retained_batches(), 0);
            assert_eq!(
                tracker.durable_prefix().next_sequence(),
                sequence as u64 + 1
            );
            assert_eq!(
                tracker.published_prefix().next_sequence(),
                sequence as u64 + 1
            );
        }
        assert_eq!(
            tracker.admit(&batches[0]),
            Err(PublicationError::ConflictingVersion)
        );
    }
}
