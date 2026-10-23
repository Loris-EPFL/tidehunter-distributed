//! Bounded completed/pending authority and cold immutable descriptor pages.
//!
//! Readers capture a small immutable root plus owned metadata files, then do
//! cold I/O without an authority guard. Metadata credits survive final read/CQ
//! owners, not just cache entries. A sole state owner freezes/persists/adopts
//! completed prefixes; it never roots the one unpersisted recovery group.
use super::storage::history::{History, HistoryReader, HistoryRoot};
use super::storage::{AppendedBatch, PartitionConfig, StorageError};
use super::types::{
    BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RecordDescriptor,
    ValueRecordReference,
};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const MAX_RESIDENT_BATCHES: usize = 1024;
const AUTHORITY_CACHE_ENTRIES: usize = 64;
const MAX_AUTHORITY_CACHE_ENTRIES: usize = 1024;
// The base allowance covers the 64-entry hash table and LRU sentinels. Charge
// extra capacity conservatively for hashbrown's bucket/control-byte rounding.
const CACHE_TABLE_BYTES_PER_EXTRA_ENTRY: usize = 64;
const CACHE_TABLE_HEADROOM_DIVISOR: usize = 16;
// lru 0.12's fixed KeyRef/pointer table and sentinels remain allocated on eviction.
const CACHE_TABLE_BYTES: usize = 4096;
const CACHE_ENTRY_BYTES: usize = 128;

#[derive(Debug)]
pub(crate) struct RegisteredBatch {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    address: PhysicalValueAddress,
    descriptors: Box<[RecordDescriptor]>,
    group_end: u64,
    credit: Option<MetadataCredit>,
}
impl RegisteredBatch {
    pub(crate) fn new(
        id: BatchId,
        version: BatchVersion,
        digest: PayloadDigest,
        address: PhysicalValueAddress,
        descriptors: Box<[RecordDescriptor]>,
        group_end: u64,
    ) -> Self {
        Self {
            id,
            version,
            digest,
            address,
            descriptors,
            group_end,
            credit: None,
        }
    }
    pub(crate) fn with_credit(mut self, credit: MetadataCredit) -> Self {
        self.credit = Some(credit);
        self
    }
    #[cfg(test)]
    pub(crate) fn authority_bytes(&self) -> usize {
        self.credit.as_ref().map_or(0, |credit| credit.bytes)
    }
    pub(crate) fn descriptors(&self) -> &[RecordDescriptor] {
        &self.descriptors
    }
    pub(crate) fn receipt(&self) -> AppendedBatch {
        AppendedBatch::from_parts(self.id, self.version, self.digest, self.address)
    }
    pub(crate) fn matches(
        &self,
        id: BatchId,
        version: BatchVersion,
        digest: PayloadDigest,
        address: PhysicalValueAddress,
    ) -> bool {
        self.id == id && self.version == version && self.digest == digest && self.address == address
    }
    pub(crate) fn group_end(&self) -> u64 {
        self.group_end
    }
    pub(crate) fn validate_reference(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<(), StorageError> {
        if self.reference(reference.descriptor.operation_index)? != *reference {
            return Err(StorageError::Invalid("foreign record reference"));
        }
        Ok(())
    }
    pub(crate) fn reference(
        &self,
        operation_index: u32,
    ) -> Result<ValueRecordReference, StorageError> {
        let descriptor = *self
            .descriptors
            .get(operation_index as usize)
            .ok_or(StorageError::Invalid("unknown operation reference"))?;
        Ok(ValueRecordReference {
            batch_id: self.id,
            batch_version: self.version,
            batch_digest: self.digest,
            batch_address: self.address,
            descriptor,
        })
    }
}

struct Authority {
    history: HistoryReader,
    pending: Vec<Arc<RegisteredBatch>>,
    last_end: u64,
}
struct AuthorityCache {
    entries: LruCache<PhysicalValueAddress, Arc<RegisteredBatch>>,
    bytes: usize,
}
impl AuthorityCache {
    fn charge(batch: &RegisteredBatch) -> usize {
        batch_charge() + batch.descriptors.len() * size_of::<RecordDescriptor>() + CACHE_ENTRY_BYTES
    }
    fn evict(&mut self) -> bool {
        let Some((_, batch)) = self.entries.pop_lru() else {
            return false;
        };
        self.bytes -= Self::charge(&batch);
        true
    }
    fn trim(&mut self, budget: usize) {
        while self.bytes > budget {
            if !self.evict() {
                break;
            }
        }
    }
}
fn batch_charge() -> usize {
    size_of::<RegisteredBatch>() + size_of::<Arc<RegisteredBatch>>() + 2 * size_of::<usize>()
}
fn root_charge() -> usize {
    size_of::<ReferenceDirectory>()
        + size_of::<History>()
        + size_of::<MetadataBudget>()
        + 2 * size_of::<usize>()
        + CACHE_TABLE_BYTES
        // Preallocated pending window plus the immutable spill capture. Charge
        // both conservatively even while empty, so retained capacity is bounded.
        + 2 * MAX_RESIDENT_BATCHES * size_of::<Arc<RegisteredBatch>>()
}

fn authority_cache_capacity(metadata_limit: usize) -> usize {
    let table_headroom =
        metadata_limit.saturating_sub(root_charge()) / CACHE_TABLE_HEADROOM_DIVISOR;
    AUTHORITY_CACHE_ENTRIES
        + (table_headroom / CACHE_TABLE_BYTES_PER_EXTRA_ENTRY)
            .min(MAX_AUTHORITY_CACHE_ENTRIES - AUTHORITY_CACHE_ENTRIES)
}

fn extra_cache_table_bytes(metadata_limit: usize) -> usize {
    (authority_cache_capacity(metadata_limit) - AUTHORITY_CACHE_ENTRIES)
        * CACHE_TABLE_BYTES_PER_EXTRA_ENTRY
}

/// Authority planning is separate from value residency and physical device I/O.
/// Event counters include attempted lookups; live snapshots are not atomic
/// across all counters. History loads can themselves hit the OS page cache.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AuthorityCacheStats {
    pub resident_hits: u64,
    pub resident_misses: u64,
    pub contention: u64,
    pub history_loads: u64,
    pub entries: usize,
    pub capacity: usize,
    pub table_bytes: usize,
}

#[derive(Default)]
struct AuthorityReadCounters {
    resident_hits: AtomicU64,
    resident_misses: AtomicU64,
    contention: AtomicU64,
    history_loads: AtomicU64,
}
#[derive(Debug)]
struct MetadataBudget {
    limit: usize,
    used: AtomicUsize,
    descriptors: AtomicUsize,
}
#[derive(Debug)]
pub(crate) struct MetadataCredit {
    budget: Arc<MetadataBudget>,
    bytes: usize,
    descriptors: usize,
}
impl MetadataBudget {
    fn reserve(self: &Arc<Self>, bytes: usize, descriptors: usize) -> Option<MetadataCredit> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .ok()?;
        self.descriptors.fetch_add(descriptors, Ordering::Relaxed);
        Some(MetadataCredit {
            budget: self.clone(),
            bytes,
            descriptors,
        })
    }
}
impl MetadataCredit {
    /// Split a pre-WAL group reservation into inline per-batch credits, without
    /// another allocation or retaining other completed members behind one pin.
    pub(crate) fn take_batch(&mut self, descriptors: usize) -> Result<Self, StorageError> {
        let bytes = descriptors
            .checked_mul(size_of::<RecordDescriptor>())
            .and_then(|bytes| bytes.checked_add(batch_charge()))
            .ok_or(StorageError::Capacity)?;
        let remaining_bytes = self.bytes.checked_sub(bytes).ok_or(StorageError::Invalid(
            "batch authority exceeds reserved credit",
        ))?;
        let remaining_descriptors =
            self.descriptors
                .checked_sub(descriptors)
                .ok_or(StorageError::Invalid(
                    "descriptor authority exceeds reserved credit",
                ))?;
        self.bytes = remaining_bytes;
        self.descriptors = remaining_descriptors;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
            descriptors,
        })
    }
}
impl Drop for MetadataCredit {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
        if self.descriptors != 0 {
            self.budget
                .descriptors
                .fetch_sub(self.descriptors, Ordering::Relaxed);
        }
    }
}

pub(crate) struct ReferenceDirectory {
    partition: u32,
    generation: u64,
    max_batches: usize,
    max_metadata_bytes: usize,
    authority: RwLock<Authority>,
    cache: Mutex<AuthorityCache>,
    budget: Arc<MetadataBudget>,
    reads: AuthorityReadCounters,
}
pub(crate) struct ReferenceDirectoryWriter {
    directory: Arc<ReferenceDirectory>,
    history: History,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectoryStats {
    pub retained_descriptors: usize,
    pub charged_metadata_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectoryError {
    InvalidLimits,
    Capacity,
    InvalidMetadata,
}
impl std::fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reference directory: {self:?}")
    }
}
impl std::error::Error for DirectoryError {}

impl ReferenceDirectory {
    pub(crate) fn check_fresh_total_capacity(
        max_batches: usize,
        max_metadata_bytes: usize,
        batches: usize,
        descriptors: usize,
    ) -> Result<(), DirectoryError> {
        if max_batches == 0 || max_batches > 1_000_000 || max_metadata_bytes < root_charge() {
            return Err(DirectoryError::InvalidLimits);
        }
        if batches > max_batches.min(MAX_RESIDENT_BATCHES)
            || Self::metadata_bytes(batches, descriptors)?
                .checked_add(extra_cache_table_bytes(max_metadata_bytes))
                .is_none_or(|bytes| bytes > max_metadata_bytes)
        {
            return Err(DirectoryError::Capacity);
        }
        Ok(())
    }
    pub(crate) fn metadata_bytes(
        batches: usize,
        descriptors: usize,
    ) -> Result<usize, DirectoryError> {
        batches
            .checked_mul(batch_charge())
            .and_then(|n| {
                descriptors
                    .checked_mul(size_of::<RecordDescriptor>())
                    .and_then(|d| n.checked_add(d))
            })
            .and_then(|n| n.checked_add(root_charge()))
            .ok_or(DirectoryError::Capacity)
    }
    pub(crate) fn open(
        path: &Path,
        config: &PartitionConfig,
        create: bool,
    ) -> Result<(Arc<Self>, ReferenceDirectoryWriter), StorageError> {
        Self::check_fresh_total_capacity(
            config.max_batches,
            config.max_reference_metadata_bytes,
            0,
            0,
        )?;
        let history = if create {
            History::create(path, config)?
        } else {
            History::open(path, config)?
        };
        let reader = history.reader();
        let last_end = reader.root().end;
        let directory = Arc::new(Self {
            partition: config.partition,
            generation: config.inventory.segment_generation,
            max_batches: config.max_batches.min(MAX_RESIDENT_BATCHES),
            max_metadata_bytes: config.max_reference_metadata_bytes,
            authority: RwLock::new(Authority {
                history: reader,
                pending: Vec::with_capacity(config.max_batches.min(MAX_RESIDENT_BATCHES)),
                last_end,
            }),
            cache: Mutex::new(AuthorityCache {
                entries: LruCache::new(
                    NonZeroUsize::new(authority_cache_capacity(
                        config.max_reference_metadata_bytes,
                    ))
                    .ok_or(DirectoryError::InvalidLimits)?,
                ),
                bytes: 0,
            }),
            reads: AuthorityReadCounters::default(),
            budget: Arc::new(MetadataBudget {
                limit: config.max_reference_metadata_bytes - root_charge(),
                used: AtomicUsize::new(extra_cache_table_bytes(
                    config.max_reference_metadata_bytes,
                )),
                descriptors: AtomicUsize::new(0),
            }),
        });
        Ok((
            directory.clone(),
            ReferenceDirectoryWriter { directory, history },
        ))
    }
    pub(crate) fn root(&self) -> HistoryRoot {
        *self.authority.read().history.root()
    }
    pub(crate) fn len(&self) -> usize {
        let authority = self.authority.read();
        authority.history.root().count + authority.pending.len()
    }
    pub(crate) fn lookup_resident(
        &self,
        address: PhysicalValueAddress,
    ) -> Option<Arc<RegisteredBatch>> {
        if address.partition != self.partition || address.segment_generation != self.generation {
            return None;
        }
        if let Some(mut cache) = self.cache.try_lock() {
            if let Some(batch) = cache.entries.get(&address) {
                self.reads.resident_hits.fetch_add(1, Ordering::Relaxed);
                return Some(batch.clone());
            }
        } else {
            self.reads.contention.fetch_add(1, Ordering::Relaxed);
        }
        let Some(authority) = self.authority.try_read() else {
            self.reads.contention.fetch_add(1, Ordering::Relaxed);
            self.reads.resident_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let batch = authority
            .pending
            .iter()
            .find(|batch| batch.address == address)
            .cloned();
        if batch.is_some() {
            self.reads.resident_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.reads.resident_misses.fetch_add(1, Ordering::Relaxed);
        }
        batch
    }
    pub(crate) fn read_stats(&self) -> AuthorityCacheStats {
        let cache = self.cache.lock();
        AuthorityCacheStats {
            resident_hits: self.reads.resident_hits.load(Ordering::Relaxed),
            resident_misses: self.reads.resident_misses.load(Ordering::Relaxed),
            contention: self.reads.contention.load(Ordering::Relaxed),
            history_loads: self.reads.history_loads.load(Ordering::Relaxed),
            entries: cache.entries.len(),
            capacity: cache.entries.cap().get(),
            table_bytes: CACHE_TABLE_BYTES
                + (cache.entries.cap().get() - AUTHORITY_CACHE_ENTRIES)
                    * CACHE_TABLE_BYTES_PER_EXTRA_ENTRY,
        }
    }
    fn reserve_with_cache(
        &self,
        cache: &mut AuthorityCache,
        bytes: usize,
        descriptors: usize,
    ) -> Result<MetadataCredit, StorageError> {
        for _ in 0..=cache.entries.len() {
            if let Some(credit) = self.budget.reserve(bytes, descriptors) {
                return Ok(credit);
            }
            if !cache.evict() {
                break;
            }
        }
        Err(StorageError::Capacity)
    }
    pub(crate) fn lookup(
        &self,
        address: PhysicalValueAddress,
    ) -> Result<Option<Arc<RegisteredBatch>>, StorageError> {
        if address.partition != self.partition || address.segment_generation != self.generation {
            return Ok(None);
        }
        if let Some(batch) = self.cache.lock().entries.get(&address).cloned() {
            return Ok(Some(batch));
        }
        let reader = {
            let authority = self.authority.read();
            if let Some(batch) = authority
                .pending
                .iter()
                .find(|batch| batch.address == address)
            {
                return Ok(Some(batch.clone()));
            }
            authority.history.clone()
        };
        self.reads.history_loads.fetch_add(1, Ordering::Relaxed);
        let Some(page) = reader.lookup_address_page(address)? else {
            return Ok(None);
        };
        let count = page.descriptor_count();
        let core = Self::metadata_bytes(1, count)?
            .checked_sub(root_charge())
            .ok_or(StorageError::Capacity)?;
        let charge = core
            .checked_add(CACHE_ENTRY_BYTES)
            .ok_or(StorageError::Capacity)?;
        let (credit, cacheable) = {
            let mut cache = self.cache.lock();
            if let Some(batch) = cache.entries.get(&address).cloned() {
                return Ok(Some(batch));
            }
            match self.reserve_with_cache(&mut cache, charge, count) {
                Ok(credit) => (credit, true),
                Err(StorageError::Capacity) => {
                    (self.reserve_with_cache(&mut cache, core, count)?, false)
                }
                Err(error) => return Err(error),
            }
        };
        // The complete decoded heap is reserved BEFORE the first descriptor is
        // allocated. Streaming fixed slots need only a bounded stack buffer.
        let batch = Arc::new(reader.read_registered(&page)?.with_credit(credit));
        if cacheable {
            let mut cache = self.cache.lock();
            if let Some(existing) = cache.entries.get(&address).cloned() {
                return Ok(Some(existing));
            }
            if cache.entries.len() == cache.entries.cap().get() {
                cache.evict();
            }
            cache.bytes += charge;
            cache.entries.put(address, batch.clone());
        }
        Ok(Some(batch))
    }
    pub(crate) fn lookup_id(&self, id: BatchId) -> Result<Option<AppendedBatch>, StorageError> {
        let reader = {
            let authority = self.authority.read();
            if let Some(batch) = authority.pending.iter().find(|batch| batch.id == id) {
                return Ok(Some(batch.receipt()));
            }
            authority.history.clone()
        };
        Ok(reader.lookup(id)?.map(|record| record.receipt()))
    }
    pub(crate) fn lookup_version(
        &self,
        version: BatchVersion,
    ) -> Result<Option<AppendedBatch>, StorageError> {
        let reader = {
            let authority = self.authority.read();
            if let Some(batch) = authority
                .pending
                .iter()
                .find(|batch| batch.version == version)
            {
                return Ok(Some(batch.receipt()));
            }
            authority.history.clone()
        };
        Ok(reader
            .lookup_version(version)?
            .map(|record| record.receipt()))
    }
    pub(crate) fn receipt_page(
        &self,
        start: usize,
        limit: usize,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        if limit == 0 || limit > 256 {
            return Err(StorageError::Invalid("receipt page limit must be 1..=256"));
        }
        let (reader, count, end, tail) = {
            let authority = self.authority.read();
            let count = authority.history.root().count;
            let total = count + authority.pending.len();
            if start > total {
                return Err(StorageError::Invalid(
                    "receipt page starts beyond authority",
                ));
            }
            let end = start.saturating_add(limit).min(total);
            let mut tail = smallvec::SmallVec::<[AppendedBatch; 256]>::new();
            if end > count {
                tail.extend(
                    authority.pending[start.max(count) - count..end - count]
                        .iter()
                        .map(|batch| batch.receipt()),
                );
            }
            (authority.history.clone(), count, end, tail)
        };
        let mut receipts = Vec::with_capacity(end - start);
        for ordinal in start..end.min(count) {
            receipts.push(reader.at(ordinal)?.receipt());
        }
        receipts.extend(tail);
        Ok(receipts)
    }
    pub(crate) fn checkpoint_prefix(
        &self,
        prefix: usize,
        database: [u8; 16],
        epoch: u64,
    ) -> Result<[u8; 32], StorageError> {
        use super::tcp::checkpoint::{CheckpointReceipt, PrefixDigest};
        let reader = {
            let authority = self.authority.read();
            let root = authority.history.root();
            if prefix > root.count + authority.pending.len() {
                return Err(StorageError::Invalid(
                    "checkpoint prefix exceeds receipt authority",
                ));
            }
            if prefix == 0 {
                return Ok(PrefixDigest::new(database, self.partition, epoch, 0).finish());
            }
            if prefix == root.count {
                return Ok(PrefixDigest::from_state(
                    prefix,
                    root.prefix_state
                        .ok_or(StorageError::Invalid("missing prefix state"))?,
                )
                .finish());
            }
            if prefix > root.count {
                let mut digest = if let Some(state) = root.prefix_state {
                    PrefixDigest::from_state(root.count, state)
                } else {
                    PrefixDigest::new(database, self.partition, epoch, 0)
                };
                for batch in authority.pending.iter().take(prefix - root.count) {
                    digest.push(CheckpointReceipt::from(batch.receipt()));
                }
                return Ok(digest.finish());
            }
            authority.history.clone()
        };
        Ok(PrefixDigest::from_state(prefix, reader.prefix_state(prefix - 1)?).finish())
    }
    pub(crate) fn is_retired(&self, offset: u64) -> Result<bool, StorageError> {
        let reader = self.authority.read().history.clone();
        reader.is_retired(offset)
    }
    #[cfg(test)]
    pub(crate) fn freeze_history(&self) -> HistoryReader {
        self.authority.read().history.clone()
    }
    #[cfg(test)]
    pub(crate) fn evict_cache(&self) {
        let mut cache = self.cache.lock();
        cache.entries.clear();
        cache.bytes = 0;
    }
}

impl ReferenceDirectoryWriter {
    pub(crate) fn receipt_group_end(&self, receipt: AppendedBatch) -> Result<u64, StorageError> {
        let reader = {
            let authority = self.directory.authority.read();
            if let Some(batch) = authority
                .pending
                .iter()
                .find(|batch| batch.receipt() == receipt)
            {
                return Ok(batch.group_end);
            }
            authority.history.clone()
        };
        let record = reader
            .lookup(receipt.id())?
            .filter(|record| record.receipt() == receipt)
            .ok_or(StorageError::Invalid("receipt not from this log"))?;
        Ok(record.group_end)
    }
    pub(crate) fn stats(&self) -> DirectoryStats {
        DirectoryStats {
            retained_descriptors: self.directory.budget.descriptors.load(Ordering::Relaxed),
            charged_metadata_bytes: root_charge()
                + self.directory.budget.used.load(Ordering::Acquire),
        }
    }
    pub(crate) fn reserve_group(
        &self,
        batches: usize,
        descriptors: usize,
    ) -> Result<MetadataCredit, StorageError> {
        let bytes = ReferenceDirectory::metadata_bytes(batches, descriptors)?
            .checked_sub(root_charge())
            .ok_or(StorageError::Capacity)?;
        self.directory
            .reserve_with_cache(&mut self.directory.cache.lock(), bytes, descriptors)
    }
    pub(crate) fn check_capacity(
        &self,
        batches: usize,
        descriptors: usize,
    ) -> Result<(), DirectoryError> {
        let authority = self.directory.authority.read();
        let pending_descriptors: usize = authority
            .pending
            .iter()
            .map(|batch| batch.descriptors.len())
            .sum();
        ReferenceDirectory::check_fresh_total_capacity(
            self.directory.max_batches,
            self.directory.max_metadata_bytes,
            authority
                .pending
                .len()
                .checked_add(batches)
                .ok_or(DirectoryError::Capacity)?,
            pending_descriptors
                .checked_add(descriptors)
                .ok_or(DirectoryError::Capacity)?,
        )
    }
    pub(crate) fn publish_group(
        &mut self,
        mut batches: Vec<RegisteredBatch>,
    ) -> Result<(), StorageError> {
        let descriptors = batches.iter().map(|batch| batch.descriptors.len()).sum();
        self.check_capacity(batches.len(), descriptors)?;
        if batches.is_empty() {
            return Err(StorageError::Invalid("empty authority group"));
        }
        if batches.iter().any(|batch| batch.credit.is_none()) {
            let mut credit = self.reserve_group(batches.len(), descriptors)?;
            for batch in &mut batches {
                batch.credit = Some(credit.take_batch(batch.descriptors.len())?);
            }
        }
        let mut authority = self.directory.authority.write();
        let group_end = batches[0].group_end;
        let database = batches[0].id.database;
        let epoch = batches[0].version.epoch;
        let mut previous_end = authority.last_end;
        for batch in &batches {
            let address = batch.address;
            let end = address
                .offset
                .checked_add(u64::from(address.len))
                .ok_or(DirectoryError::InvalidMetadata)?;
            if address.partition != self.directory.partition
                || address.segment_generation != self.directory.generation
                || address.len <= 8
                || address.offset < previous_end
                || end > group_end
                || batch.group_end != group_end
                || batch.id.database != database
                || batch.version.epoch != epoch
            {
                return Err(DirectoryError::InvalidMetadata.into());
            }
            let mut descriptor_end = 0;
            for (index, descriptor) in batch.descriptors.iter().enumerate() {
                let end = descriptor
                    .offset
                    .checked_add(descriptor.len)
                    .ok_or(DirectoryError::InvalidMetadata)?;
                if descriptor.operation_index as usize != index
                    || descriptor.offset < descriptor_end
                    || end > address.len - 8
                    || descriptor.len == 0
                    || descriptor.value_offset.checked_add(descriptor.value_len)
                        != Some(descriptor.len)
                {
                    return Err(DirectoryError::InvalidMetadata.into());
                }
                descriptor_end = end;
            }
            previous_end = end;
        }
        let descriptors = authority
            .pending
            .iter()
            .map(|batch| batch.descriptors.len())
            .sum::<usize>()
            + descriptors;
        let bytes = ReferenceDirectory::metadata_bytes(
            authority.pending.len() + batches.len(),
            descriptors,
        )?;
        self.directory.cache.lock().trim(
            self.directory
                .max_metadata_bytes
                .saturating_sub(bytes)
                .saturating_sub(extra_cache_table_bytes(self.directory.max_metadata_bytes)),
        );
        authority.last_end = group_end;
        authority.pending.extend(batches.into_iter().map(Arc::new));
        Ok(())
    }
    pub(crate) fn persist_pending(&mut self, file_count: u64) -> Result<(), StorageError> {
        self.persist_prefix(u64::MAX, file_count, true).map(|_| ())
    }
    pub(crate) fn persist_completed(
        &mut self,
        durable_end: u64,
        file_count: u64,
    ) -> Result<bool, StorageError> {
        self.persist_prefix(durable_end, file_count, false)
    }
    fn persist_prefix(
        &mut self,
        durable_end: u64,
        file_count: u64,
        allow_empty: bool,
    ) -> Result<bool, StorageError> {
        let captured: Vec<_> = {
            let authority = self.directory.authority.read();
            let completed = authority
                .pending
                .partition_point(|batch| batch.group_end <= durable_end);
            if completed == 0 && !allow_empty {
                return Ok(false);
            }
            authority.pending[..completed].to_vec()
        };
        // Existing state ownership serializes metadata writers, while readers
        // retain the previous immutable root throughout file I/O and fsync.
        self.history.append(&captured, file_count)?;
        let mut authority = self.directory.authority.write();
        authority.history = self.history.reader();
        // Keep the prepaid window allocation across spills; only an actually
        // unpersisted suffix moves, rather than reallocating each append group.
        drop(authority.pending.drain(..captured.len()));
        Ok(!captured.is_empty())
    }
    pub(crate) fn retire(&mut self, offsets: &[u64]) -> Result<(), StorageError> {
        self.history.retire(offsets)?;
        self.directory.authority.write().history = self.history.reader();
        Ok(())
    }
}
