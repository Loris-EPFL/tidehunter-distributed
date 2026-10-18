//! Bridge from an authoritative durable batch to native pending transactions.
//!
//! Tidehunter paper §3.1 and `batch::PendingOp` keep complete application batches
//! together. This bridge reuses the actual native pending table and transaction
//! flag, with explicit distributed value references. It performs no value copy
//! into a second storage engine and no I/O while installing pending entries.
//!
//! This is not `LargeTable` integration: native cache generations, promotion,
//! persisted index formats, checkpoint cuts and replay still require adaptation.
//! Call this inside `PublicationTracker::publish_ready`; any failure after durable
//! authority requires recovery, even if dropping the transaction hides all staged
//! entries. It must never become a definite abort of that durable batch.

use super::codec::{BatchLimits, CodecError, CompleteBatch, Mutation};
use super::storage::{DurableBatch, PartitionLog, StorageError};
use super::types::{BatchId, BatchVersion, PayloadDigest, ValueRecordReference};
use crate::batch::PendingOp;
use crate::cell::CellId;
use crate::index::pending_table::{PendingTable, Transaction};
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace};
use std::convert::Infallible;
use std::fmt;

/// A native cell owner supplies exclusive access only for short pending updates.
///
/// Implementations must select the specified keyspace/cell, invoke `apply`
/// exactly once synchronously and release the cell before returning. Returning
/// an error after invoking it is permitted: the enclosing transaction reverts
/// pending visibility. Do not promote entries, update an external value cache or
/// acknowledge the application independently of the shared transaction.
///
/// The concrete table parameter prevents a sink from substituting another KV
/// store for native publication. `FnOnce` allows a future native row guard to own
/// table access without exposing a reference after that guard is released.
pub trait NativePendingSink {
    type Error;

    fn with_pending_table(
        &mut self,
        keyspace: KeySpace,
        cell: &CellId,
        apply: impl FnOnce(&mut PendingTable<ValueRecordReference>),
    ) -> Result<(), Self::Error>;
}

/// All authority membership and native key routing prepared before staging.
///
/// The full record descriptor is retained while pending. The compact logical
/// codec remains a candidate for the later persistent index; this bridge does
/// not select a permanently resident indirection map or its retention policy.
pub struct PreparedNativePublication {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    operations: Vec<(PendingOp, ValueRecordReference)>,
}

/// Routing retained from local preflight through durable publication. Values
/// still belong to the canonical batch; this token never establishes durability.
pub(crate) struct NativePublicationRouting {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    operations: Vec<PendingOp>,
}

impl NativePublicationRouting {
    /// Only authority-local batches already constructed/decoded with this shape.
    /// Untrusted bytes must pass CompleteBatch::decode before reaching here.
    pub(crate) fn prepare(
        shape: &KeyShape,
        batch: &CompleteBatch,
    ) -> Result<Self, NativePublicationError> {
        let mut operations = Vec::with_capacity(batch.mutations().len());
        for mutation in batch.mutations() {
            operations.push(route_mutation(shape, mutation)?);
        }
        Ok(Self {
            id: batch.id(),
            version: batch.version(),
            digest: batch.digest(),
            operations,
        })
    }

    pub(crate) fn operations(&self) -> &[PendingOp] {
        &self.operations
    }

    pub(crate) fn owned_bytes(&self, shape: &KeyShape) -> Option<usize> {
        self.operations.iter().try_fold(
            self.operations
                .capacity()
                .checked_mul(std::mem::size_of::<PendingOp>())?,
            |bytes, operation| {
                let extra = match operation.cell_id() {
                    CellId::Bytes(prefix) if prefix.spilled() => prefix.capacity(),
                    _ => 0,
                };
                let bytes = bytes.checked_add(extra)?;
                if matches!(shape.ks(operation.ks()).key_indexing(), KeyIndexing::Hash) {
                    bytes
                        .checked_add(operation.reduced_key().len())?
                        .checked_add(super::codec::ROUTING_HASH_OWNER_BYTES)
                } else {
                    Some(bytes)
                }
            },
        )
    }

    pub(crate) fn publish_authenticated<S: NativePendingSink>(
        self,
        batch: &CompleteBatch,
        durable: DurableBatch,
        sink: &mut S,
    ) -> Result<(), NativePublicationError<S::Error>> {
        let receipt = durable.appended();
        if receipt.id() != self.id
            || receipt.version() != self.version
            || receipt.digest() != self.digest
            || batch.id() != self.id
            || batch.version() != self.version
            || batch.digest() != self.digest
            || self.operations.len() != batch.mutations().len()
        {
            return Err(NativePublicationError::ReceiptMismatch);
        }
        let transaction = Transaction::default();
        for (index, pending) in self.operations.into_iter().enumerate() {
            let reference = batch
                .record_reference(receipt.address(), index)
                .map_err(NativePublicationError::Codec)?;
            let keyspace = pending.ks();
            let cell = pending.cell_id().clone();
            let mut installed = false;
            sink.with_pending_table(keyspace, &cell, |table| {
                match pending {
                    PendingOp::Insert {
                        reduced_key,
                        lru_update,
                        ..
                    } => table.insert(reduced_key, reference, lru_update, &transaction),
                    PendingOp::Remove { reduced_key, .. } => {
                        table.remove(reduced_key, reference, &transaction)
                    }
                }
                installed = true;
            })
            .map_err(NativePublicationError::Install)?;
            if !installed {
                return Err(NativePublicationError::SkippedOperation);
            }
        }
        transaction.commit();
        Ok(())
    }
}

fn route_mutation(
    shape: &KeyShape,
    mutation: &Mutation,
) -> Result<PendingOp, NativePublicationError> {
    let keyspace = mutation.keyspace();
    let descriptor = shape.ks(keyspace);
    if let KeyIndexing::Reduction(_, range) = descriptor.key_indexing()
        && (range.start > range.end || range.end > mutation.key().len())
    {
        return Err(NativePublicationError::InvalidRouting);
    }
    let reduced_key = descriptor.reduced_key_bytes(mutation.key().clone());
    if let CellId::Bytes(prefix) = descriptor.first_cell()
        && reduced_key.len() < prefix.len()
    {
        return Err(NativePublicationError::InvalidRouting);
    }
    let cell_id = descriptor.cell_id(&reduced_key);
    Ok(match mutation {
        Mutation::Put { key, value, .. } => PendingOp::Insert {
            ks: keyspace,
            reduced_key,
            cell_id,
            lru_update: descriptor
                .value_cache_size()
                .map(|_| (key.clone(), value.clone())),
        },
        Mutation::Delete { .. } => PendingOp::Remove {
            ks: keyspace,
            reduced_key,
            cell_id,
        },
    })
}

impl PreparedNativePublication {
    pub fn prepare(
        log: &PartitionLog,
        batch: &CompleteBatch,
        durable: DurableBatch,
    ) -> Result<Self, NativePublicationError> {
        // Local persistence evidence is tied to this exact open log instance.
        log.validate_durable_receipt(durable)
            .map_err(NativePublicationError::Storage)?;
        Self::prepare_members(
            log.key_shape(),
            log.config().batch_limits,
            batch,
            durable,
            |index| {
                log.record_reference(durable.appended(), index as u32)
                    .map_err(NativePublicationError::Storage)
            },
        )
    }

    /// The TCP client has authenticated the storage peer and bound its durable
    /// acknowledgement to this exact immutable batch and registered placement.
    /// Derive member references locally, without one metadata RPC per mutation.
    /// Unauthenticated wire fields must never enter this path.
    pub(crate) fn prepare_authenticated(
        shape: &KeyShape,
        limits: BatchLimits,
        batch: &CompleteBatch,
        durable: DurableBatch,
    ) -> Result<Self, NativePublicationError> {
        Self::prepare_members(shape, limits, batch, durable, |index| {
            batch
                .record_reference(durable.appended().address(), index)
                .map_err(NativePublicationError::Codec)
        })
    }

    fn prepare_members(
        shape: &KeyShape,
        limits: BatchLimits,
        batch: &CompleteBatch,
        durable: DurableBatch,
        mut member: impl FnMut(usize) -> Result<ValueRecordReference, NativePublicationError>,
    ) -> Result<Self, NativePublicationError> {
        let appended = durable.appended();
        if appended.id() != batch.id()
            || appended.version() != batch.version()
            || appended.digest() != batch.digest()
        {
            return Err(NativePublicationError::ReceiptMismatch);
        }
        batch
            .validate_for(shape, limits)
            .map_err(NativePublicationError::Codec)?;
        let mut operations = Vec::with_capacity(batch.mutations().len());
        for (index, mutation) in batch.mutations().iter().enumerate() {
            let reference = member(index)?;
            let expected = batch
                .record_reference(appended.address(), index)
                .map_err(NativePublicationError::Codec)?;
            if reference != expected {
                return Err(NativePublicationError::ReceiptMismatch);
            }
            operations.push((route_mutation(shape, mutation)?, reference));
        }
        Ok(Self {
            id: batch.id(),
            version: batch.version(),
            digest: batch.digest(),
            operations,
        })
    }

    pub fn id(&self) -> BatchId {
        self.id
    }

    pub fn version(&self) -> BatchVersion {
        self.version
    }

    pub fn digest(&self) -> PayloadDigest {
        self.digest
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }

    /// Install every entry before committing the one shared native transaction.
    /// A returned error or unwinding panic drops that transaction, reverting
    /// staged visibility. Durable authority still exists and must be replayed;
    /// the enclosing publication tracker must remain recovery-required.
    pub fn publish<S: NativePendingSink>(
        self,
        sink: &mut S,
    ) -> Result<(), NativePublicationError<S::Error>> {
        let transaction = Transaction::default();
        for (pending, reference) in self.operations {
            let keyspace = pending.ks();
            let cell = pending.cell_id().clone();
            let mut installed = false;
            sink.with_pending_table(keyspace, &cell, |table| {
                match pending {
                    PendingOp::Insert {
                        reduced_key,
                        lru_update,
                        ..
                    } => table.insert(reduced_key, reference, lru_update, &transaction),
                    PendingOp::Remove { reduced_key, .. } => {
                        table.remove(reduced_key, reference, &transaction)
                    }
                }
                installed = true;
            })
            .map_err(NativePublicationError::Install)?;
            if !installed {
                return Err(NativePublicationError::SkippedOperation);
            }
        }
        transaction.commit();
        Ok(())
    }
}

#[derive(Debug)]
pub enum NativePublicationError<E = Infallible> {
    Storage(StorageError),
    Codec(CodecError),
    ReceiptMismatch,
    InvalidRouting,
    Install(E),
    SkippedOperation,
}

impl<E: fmt::Display> fmt::Display for NativePublicationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "native publication storage validation: {error}"),
            Self::Codec(error) => write!(f, "native publication batch validation: {error}"),
            Self::ReceiptMismatch => f.write_str("durable receipt does not match complete batch"),
            Self::InvalidRouting => f.write_str("mutation cannot use the native cell routing"),
            Self::Install(error) => write!(f, "native pending installation failed: {error}"),
            Self::SkippedOperation => {
                f.write_str("native pending sink did not install the operation")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NativePublicationError<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::BatchLimits;
    use crate::distributed::group::{CompleteGroup, GroupLimits};
    use crate::distributed::publication::PublicationTracker;
    use crate::distributed::storage::{PartitionConfig, StaticInventory};
    use crate::distributed::types::RequestAuthority;
    use crate::index::pending_table::CommittedChange;
    use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpaceConfig, KeyType};
    use crate::metrics::Metrics;
    use minibytes::Bytes;
    use std::collections::HashMap;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    const EPOCH: u64 = 6;

    fn shape() -> KeyShape {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("plain", 4, 1, KeyType::uniform(2));
        builder.add_key_space_config_indexing(
            "reduced",
            KeyIndexing::key_reduction(8, 4..8),
            1,
            KeyType::uniform(2),
            KeySpaceConfig::new().with_value_cache_size(16),
        );
        builder.add_key_space_config_indexing(
            "prefix",
            KeyIndexing::variable_length(),
            1,
            KeyType::prefix_uniform(1, 0),
            KeySpaceConfig::new(),
        );
        builder.build()
    }

    fn config() -> PartitionConfig {
        PartitionConfig {
            inventory: StaticInventory {
                database: [7; 16],
                epoch: EPOCH,
                segment_generation: 9,
                partitions: vec![0],
            },
            partition: 0,
            batch_limits: BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: 4,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches: 16,
            max_reference_metadata_bytes: 1024 * 1024,
        }
    }

    fn batch(sequence: u64) -> CompleteBatch {
        let put = |space, key: &[u8], value: &[u8]| Mutation::Put {
            keyspace: KeySpace(space),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };
        CompleteBatch::new(
            &shape(),
            BatchId {
                database: [7; 16],
                operation: (sequence as u128).to_be_bytes(),
            },
            BatchVersion {
                epoch: EPOCH,
                sequence,
            },
            vec![
                put(0, &[0, 0, 0, 1], b"first"),
                put(0, &[255, 0, 0, 2], b"other cell"),
                put(1, &[1, 2, 3, 4, 128, 0, 0, 3], b"cached"),
                put(2, b"prefix-key", b"variable"),
                Mutation::Delete {
                    keyspace: KeySpace(0),
                    key: vec![0, 0, 0, 1].into(),
                },
                put(0, &[0, 0, 0, 1], b"last"),
            ],
            config().batch_limits,
        )
        .unwrap()
    }

    fn open(path: &std::path::Path) -> PartitionLog {
        PartitionLog::create(
            path,
            config(),
            &shape(),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap()
    }

    fn durable(log: &PartitionLog, batches: &[CompleteBatch]) -> Vec<DurableBatch> {
        let group = CompleteGroup::new(
            &shape(),
            batches.to_vec(),
            config().batch_limits,
            config().group_limits,
        )
        .unwrap();
        log.append_group(RequestAuthority { epoch: EPOCH }, &group)
            .unwrap()
            .into_iter()
            .map(|receipt| log.persist_through(receipt).unwrap())
            .collect()
    }

    #[derive(Default)]
    struct Sink {
        // Routing fixtures containing the actual native pending tables, not a
        // serving key/value engine or a replacement for native LargeTable.
        tables: HashMap<(KeySpace, CellId), PendingTable<ValueRecordReference>>,
        calls: usize,
        fail_at: Option<usize>,
        panic_at: Option<usize>,
        skip_at: Option<usize>,
    }

    impl NativePendingSink for Sink {
        type Error = &'static str;

        fn with_pending_table(
            &mut self,
            keyspace: KeySpace,
            cell: &CellId,
            apply: impl FnOnce(&mut PendingTable<ValueRecordReference>),
        ) -> Result<(), Self::Error> {
            // All earlier staged operations remain invisible until the final
            // shared commit, including when the next operation targets a new cell.
            for table in self.tables.values_mut() {
                assert!(table.take_committed().0.is_empty());
            }
            let call = self.calls;
            self.calls += 1;
            if self.panic_at == Some(call) {
                panic!("injected native install panic");
            }
            if self.skip_at == Some(call) {
                return Ok(());
            }
            apply(self.tables.entry((keyspace, cell.clone())).or_default());
            if self.fail_at == Some(call) {
                return Err("injected native install failure");
            }
            Ok(())
        }
    }

    fn collect(sink: &mut Sink) -> Vec<(KeySpace, CellId, CommittedChange<ValueRecordReference>)> {
        sink.tables
            .iter_mut()
            .flat_map(|((ks, cell), table)| {
                table
                    .take_committed()
                    .0
                    .into_iter()
                    .map(|change| (*ks, cell.clone(), change))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn durable_group_installs_native_multicell_batch_and_resolves_values() {
        let temp = tempdir::TempDir::new("native-publication").unwrap();
        let log = open(&temp.path().join("log"));
        let batches = [batch(0), batch(1)];
        let receipts = durable(&log, &batches);
        assert_eq!(log.stats().persistence_barriers, 1);
        let prepared = PreparedNativePublication::prepare(&log, &batches[0], receipts[0]).unwrap();
        assert_eq!(prepared.operation_count(), 6);
        let mut sink = Sink::default();
        prepared.publish(&mut sink).unwrap();
        assert_eq!(sink.tables.len(), 4);
        let mut changes = collect(&mut sink);
        changes.sort_by_key(|(_, _, change)| change.value.descriptor.operation_index);
        assert_eq!(changes.len(), 6);
        for (index, (keyspace, cell, change)) in changes.iter().enumerate() {
            let mutation = &batches[0].mutations()[index];
            let descriptor = shape().ks(*keyspace).clone();
            let reduced = descriptor.reduced_key_bytes(mutation.key().clone());
            assert_eq!(change.key, reduced);
            assert_eq!(*cell, descriptor.cell_id(&reduced));
            assert_eq!(*keyspace, mutation.keyspace());
            assert_eq!(log.read_record(&change.value).unwrap(), *mutation);
            assert_eq!(change.is_modified, matches!(mutation, Mutation::Put { .. }));
        }
        assert_eq!(changes[0].2.lru_update, None);
        assert_eq!(changes[2].2.key.as_ref(), &[128, 0, 0, 3]);
        assert_eq!(
            changes[2].2.lru_update,
            Some((
                batches[0].mutations()[2].key().clone(),
                b"cached"[..].into()
            ))
        );
        assert_eq!(changes[4].2.lru_update, None);
        assert_eq!(changes[0].2.value.mutation_version().operation_index, 0);
        assert_eq!(changes[4].2.value.mutation_version().operation_index, 4);
        assert_eq!(changes[5].2.value.mutation_version().operation_index, 5);
    }

    #[test]
    fn partial_install_error_panic_and_skipped_callback_hide_pending_but_require_recovery() {
        let temp = tempdir::TempDir::new("native-publication-failure").unwrap();
        let log = open(&temp.path().join("log"));
        let batch = batch(0);
        let receipt = durable(&log, std::slice::from_ref(&batch))[0];
        for kind in 0..3 {
            let mut sink = Sink::default();
            match kind {
                0 => sink.fail_at = Some(1),
                1 => sink.panic_at = Some(1),
                _ => sink.skip_at = Some(1),
            }
            let mut tracker = PublicationTracker::new(batch.id().database, EPOCH, 8).unwrap();
            tracker.admit(&batch).unwrap();
            tracker.mark_in_flight(batch.id()).unwrap();
            tracker.record_durable(receipt).unwrap();
            let prepared = PreparedNativePublication::prepare(&log, &batch, receipt).unwrap();
            let mut prepared = Some(prepared);
            let result = catch_unwind(AssertUnwindSafe(|| {
                tracker.publish_ready(|_| prepared.take().unwrap().publish(&mut sink))
            }));
            if kind == 1 {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert!(tracker.ensure_publication_healthy().is_err());
            assert_eq!(tracker.published_prefix().next_sequence(), 0);
            assert!(collect(&mut sink).is_empty());
            assert!(sink.tables.values().all(|table| table.len() == 0));
            // Native transaction cleanup does not abort the durable authority.
            assert_eq!(
                log.read_batch(receipt.appended()).unwrap().digest(),
                batch.digest()
            );
        }
    }

    #[test]
    fn foreign_receipt_and_identical_unpersisted_copy_cannot_publish() {
        let temp = tempdir::TempDir::new("native-publication-receipt").unwrap();
        let first = open(&temp.path().join("first"));
        let second = open(&temp.path().join("second"));
        let batch = batch(0);
        let receipt = durable(&first, std::slice::from_ref(&batch))[0];
        assert!(PreparedNativePublication::prepare(&second, &batch, receipt).is_err());
        let second_appended = second
            .append_complete_batch(RequestAuthority { epoch: EPOCH }, &batch)
            .unwrap();
        assert_eq!(second_appended, receipt.appended());
        assert!(PreparedNativePublication::prepare(&second, &batch, receipt).is_err());
        let second_receipt = second.persist_through(second_appended).unwrap();
        assert!(PreparedNativePublication::prepare(&second, &batch, second_receipt).is_ok());
        assert!(matches!(
            PreparedNativePublication::prepare(&first, &super::tests::batch(1), receipt),
            Err(NativePublicationError::ReceiptMismatch)
        ));
    }

    #[test]
    fn preflight_routing_binds_exact_receipt_and_keeps_atomic_visibility() {
        let temp = tempdir::TempDir::new("native-routing-reuse").unwrap();
        let log = open(&temp.path().join("log"));
        let batches = [
            batch(0).into_canonical_owned(),
            batch(1).into_canonical_owned(),
        ];
        let receipts = durable(&log, &batches);
        let routing = NativePublicationRouting::prepare(&shape(), &batches[0]).unwrap();
        assert_eq!(routing.operations().len(), 6);
        assert!(routing.owned_bytes(&shape()).unwrap() >= 6 * std::mem::size_of::<PendingOp>());
        let mut sink = Sink::default();
        assert!(matches!(
            routing.publish_authenticated(&batches[0], receipts[1], &mut sink),
            Err(NativePublicationError::ReceiptMismatch)
        ));
        assert_eq!(sink.calls, 0);
        let routing = NativePublicationRouting::prepare(&shape(), &batches[0]).unwrap();
        routing
            .publish_authenticated(&batches[0], receipts[0], &mut sink)
            .unwrap();
        let changes = collect(&mut sink);
        assert_eq!(changes.len(), 6);
        for (_, _, change) in changes {
            let index = change.value.descriptor.operation_index as usize;
            assert_eq!(
                log.read_record(&change.value).unwrap(),
                batches[0].mutations()[index]
            );
        }
        let routing = NativePublicationRouting::prepare(&shape(), &batches[1]).unwrap();
        let mut failing = Sink {
            fail_at: Some(2),
            ..Sink::default()
        };
        assert!(
            routing
                .publish_authenticated(&batches[1], receipts[1], &mut failing)
                .is_err()
        );
        assert!(collect(&mut failing).is_empty());
    }

    #[test]
    fn hash_routing_reserves_derived_key_owners_for_short_inputs() {
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::Hash,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::new(),
        );
        let mutations = (0..32)
            .map(|byte| Mutation::Put {
                keyspace: KeySpace(0),
                key: vec![byte].into(),
                value: Bytes::new(),
            })
            .collect::<Vec<_>>();
        let limits = BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 8192,
        };
        let validated = CompleteBatch::validated_mutations(&shape, &mutations, limits).unwrap();
        let batch = validated
            .encode(
                BatchId {
                    database: [7; 16],
                    operation: [3; 16],
                },
                BatchVersion {
                    epoch: EPOCH,
                    sequence: 0,
                },
            )
            .unwrap()
            .into_canonical_owned();
        let routing = NativePublicationRouting::prepare(&shape, &batch).unwrap();
        // Hashing creates a separate Vec and minibytes Arc owner per operation;
        // unlike fixed/reduced keys, none aliases the canonical batch backing.
        let owner = std::mem::size_of::<Vec<u8>>() + 2 * std::mem::size_of::<usize>();
        let derived_bytes: usize = routing
            .operations()
            .iter()
            .map(|op| op.reduced_key().len() + owner)
            .sum();
        assert!(validated.routing_heap_bound() >= derived_bytes);
        assert!(
            routing.owned_bytes(&shape).unwrap()
                >= routing.operations.capacity() * std::mem::size_of::<PendingOp>() + derived_bytes
        );
    }
}
