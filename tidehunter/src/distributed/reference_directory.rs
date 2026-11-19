//! Bounded pending authority, a recent receipt window and a descriptor cache.
//!
//! WAL replay is the only history: published groups move straight into the
//! in-memory receipt window. Metadata credits survive final read/CQ owners,
//! not just cache entries.
use super::storage::history::{History, HistoryReader, HistoryRoot};
use super::storage::{AppendedBatch, PartitionConfig, StorageError};
use super::types::{
    BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RecordDescriptor,
    ValueRecordReference,
};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::hash::BuildHasher;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const MAX_RESIDENT_BATCHES: usize = 1024;
const AUTHORITY_CACHE_ENTRIES: usize = 64;
// The separately charged table and descriptor credits, not an artificial
// 1,024-batch ceiling, bound useful authority residency for larger budgets.
const MAX_AUTHORITY_CACHE_ENTRIES: usize = 8192;
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
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
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
    // Hash positions over `pending`, kept in step with it under this lock.
    index: PendingIndex,
    last_end: u64,
}

/// Slots per pending position table: a power of two, at least twice the
/// largest pending window, so linear probing keeps its load factor <= 1/2.
const INDEX_SLOTS: usize = 2 * MAX_RESIDENT_BATCHES;
const _: () = assert!(INDEX_SLOTS.is_power_of_two() && INDEX_SLOTS <= u16::MAX as usize);

/// Mixes a 128-bit key with a per-directory random seed. Clients choose batch
/// identities, so an unseeded hash would let them force long probe chains.
fn mix(seed: u64, key: u128) -> u64 {
    const K: u64 = 0xD6E8_FEB8_6659_FD93;
    let mut h = (key as u64 ^ seed).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 32;
    h = (h ^ (key >> 64) as u64).wrapping_mul(K);
    h ^= h >> 32;
    h = h.wrapping_mul(K);
    h ^ (h >> 32)
}

/// Work done by one conflict lookup, for opt-in write diagnostics.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LookupWork {
    /// Pending-table slots examined; replaces the former linear pending scan.
    pub(crate) pending_probes: u64,
    /// Reads of the receipt window.
    pub(crate) history_lookups: u64,
}

/// Result of an exact identity-then-version lookup for one batch.
pub(crate) enum ConflictLookup {
    Free,
    /// The identity is already registered; the caller compares version/digest.
    Existing(AppendedBatch),
    /// A different batch owns this version.
    VersionTaken,
}

/// Open-addressed tables of `position + 1` (0 is empty) into `Authority::pending`,
/// one keyed by identity and one by version. Probe loops are bounded by the table
/// size. Rebuilt in full when the pending prefix drains, which happens once per
/// history spill, so the cost is amortized over the spilled batches.
struct PendingIndex {
    seed: u64,
    ids: Box<[u16]>,
    versions: Box<[u16]>,
}
impl PendingIndex {
    fn new() -> Self {
        Self {
            seed: std::collections::hash_map::RandomState::new().hash_one(0u8),
            ids: vec![0; INDEX_SLOTS].into_boxed_slice(),
            versions: vec![0; INDEX_SLOTS].into_boxed_slice(),
        }
    }
    fn id_hash(&self, id: BatchId) -> u64 {
        mix(self.seed, u128::from_be_bytes(id.operation))
    }
    fn version_hash(&self, version: BatchVersion) -> u64 {
        mix(self.seed, u128::from(version.sequence))
    }
    /// Keeps the first position for a repeated key, so lookups find the oldest.
    fn insert(table: &mut [u16], hash: u64, position: usize, same: impl Fn(usize) -> bool) -> bool {
        let Ok(value) = u16::try_from(position + 1) else {
            return false;
        };
        let mask = table.len() - 1;
        let mut slot = hash as usize & mask;
        for _ in 0..table.len() {
            match usize::from(table[slot]).checked_sub(1) {
                None => {
                    table[slot] = value;
                    return true;
                }
                Some(existing) if same(existing) => return true,
                Some(_) => slot = (slot + 1) & mask,
            }
        }
        false
    }
    /// Index `pending[position]`; false when a table cannot hold it.
    fn push(&mut self, pending: &[Arc<RegisteredBatch>], position: usize) -> bool {
        let batch = &pending[position];
        let (id, version) = (batch.id, batch.version);
        let id_hash = self.id_hash(id);
        let version_hash = self.version_hash(version);
        Self::insert(&mut self.ids, id_hash, position, |other| {
            pending[other].id == id
        }) && Self::insert(&mut self.versions, version_hash, position, |other| {
            pending[other].version == version
        })
    }
    fn rebuild(&mut self, pending: &[Arc<RegisteredBatch>]) -> bool {
        self.ids.fill(0);
        self.versions.fill(0);
        (0..pending.len()).all(|position| self.push(pending, position))
    }
    fn find(
        table: &[u16],
        hash: u64,
        pending: &[Arc<RegisteredBatch>],
        work: &mut LookupWork,
        matches: impl Fn(&RegisteredBatch) -> bool,
    ) -> Option<usize> {
        let mask = table.len() - 1;
        let mut slot = hash as usize & mask;
        for _ in 0..table.len() {
            work.pending_probes += 1;
            let position = usize::from(table[slot]).checked_sub(1)?;
            if pending.get(position).is_some_and(|batch| matches(batch)) {
                return Some(position);
            }
            slot = (slot + 1) & mask;
        }
        None
    }
}

impl Authority {
    fn pending_id(
        &self,
        id: BatchId,
        hash: u64,
        work: &mut LookupWork,
    ) -> Option<&Arc<RegisteredBatch>> {
        PendingIndex::find(&self.index.ids, hash, &self.pending, work, |batch| {
            batch.id == id
        })
        .map(|position| &self.pending[position])
    }
    fn pending_version(&self, version: BatchVersion, hash: u64, work: &mut LookupWork) -> bool {
        PendingIndex::find(&self.index.versions, hash, &self.pending, work, |batch| {
            batch.version == version
        })
        .is_some()
    }
    /// Pending offsets strictly increase (checked at publication).
    fn pending_address(&self, address: PhysicalValueAddress) -> Option<&Arc<RegisteredBatch>> {
        let position = self
            .pending
            .partition_point(|batch| batch.address.offset < address.offset);
        self.pending
            .get(position)
            .filter(|batch| batch.address == address)
    }
    /// False only when the receipt window is empty.
    fn history_may_hold(&self) -> bool {
        self.history.root().count != 0
    }
    fn drain_prefix(&mut self, count: usize) -> Result<(), StorageError> {
        drop(self.pending.drain(..count));
        if self.index.rebuild(&self.pending) {
            Ok(())
        } else {
            Err(DirectoryError::Capacity.into())
        }
    }
}
struct CachedAuthority {
    batch: Arc<RegisteredBatch>,
    _entry_credit: MetadataCredit,
}
struct AuthorityCache {
    entries: LruCache<PhysicalValueAddress, CachedAuthority>,
    bytes: usize,
}
impl AuthorityCache {
    fn charge(batch: &RegisteredBatch) -> usize {
        batch_charge() + batch.descriptors.len() * size_of::<RecordDescriptor>() + CACHE_ENTRY_BYTES
    }
    fn evict(&mut self) -> bool {
        let Some((_, entry)) = self.entries.pop_lru() else {
            return false;
        };
        self.bytes -= Self::charge(&entry.batch);
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
        + size_of::<ReferenceDirectoryWriter>()
        + size_of::<MetadataBudget>()
        + size_of::<Mutex<History>>()
        + 6 * size_of::<usize>()
        + CACHE_TABLE_BYTES
        // Preallocated pending window plus its `persist_pending` capture. Charge
        // both conservatively even while empty, so retained capacity is bounded.
        + 2 * MAX_RESIDENT_BATCHES * size_of::<Arc<RegisteredBatch>>()
        // The identity and version position tables over the pending window.
        + 2 * INDEX_SLOTS * size_of::<u16>()
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

/// Fixed charge taken from the budget at open, before any batch.
fn extra_fixed_bytes(metadata_limit: usize) -> usize {
    extra_cache_table_bytes(metadata_limit)
}

/// Authority cache occupancy and its fixed table charge.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AuthorityCacheStats {
    pub entries: usize,
    pub capacity: usize,
    pub table_bytes: usize,
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
}
pub(crate) struct ReferenceDirectoryWriter {
    directory: Arc<ReferenceDirectory>,
    history: Arc<Mutex<History>>,
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
                .checked_add(extra_fixed_bytes(max_metadata_bytes))
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
        config: &PartitionConfig,
    ) -> Result<(Arc<Self>, ReferenceDirectoryWriter), StorageError> {
        Self::check_fresh_total_capacity(
            config.max_batches,
            config.max_reference_metadata_bytes,
            0,
            0,
        )?;
        let history = History::new();
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
                index: PendingIndex::new(),
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
            budget: Arc::new(MetadataBudget {
                limit: config.max_reference_metadata_bytes - root_charge(),
                used: AtomicUsize::new(extra_fixed_bytes(config.max_reference_metadata_bytes)),
                descriptors: AtomicUsize::new(0),
            }),
        });
        Ok((
            directory.clone(),
            ReferenceDirectoryWriter {
                directory,
                history: Arc::new(Mutex::new(history)),
            },
        ))
    }

    pub(crate) fn cache_registered(
        &self,
        batch: RegisteredBatch,
    ) -> Result<Arc<RegisteredBatch>, StorageError> {
        let count = batch.descriptors().len();
        let core = Self::metadata_bytes(1, count)?
            .checked_sub(root_charge())
            .ok_or(StorageError::Capacity)?;
        let mut cache = self.cache.lock();
        if let Some(entry) = cache.entries.get(&batch.address) {
            return Ok(entry.batch.clone());
        }
        let (core_credit, entry_credit) =
            match self.reserve_with_cache(&mut cache, core + CACHE_ENTRY_BYTES, count) {
                Ok(mut credit) => (credit.take_batch(count)?, Some(credit)),
                Err(StorageError::Capacity) => {
                    (self.reserve_with_cache(&mut cache, core, count)?, None)
                }
                Err(error) => return Err(error),
            };
        let batch = Arc::new(batch.with_credit(core_credit));
        if let Some(entry_credit) = entry_credit {
            if cache.entries.len() == cache.entries.cap().get() {
                cache.evict();
            }
            cache.bytes += AuthorityCache::charge(&batch);
            cache.entries.put(
                batch.address,
                CachedAuthority {
                    batch: batch.clone(),
                    _entry_credit: entry_credit,
                },
            );
        }
        Ok(batch)
    }

    pub(crate) fn root(&self) -> HistoryRoot {
        *self.authority.read().history.root()
    }
    pub(crate) fn len(&self) -> usize {
        let authority = self.authority.read();
        authority.history.root().count + authority.pending.len()
    }
    pub(crate) fn read_stats(&self) -> AuthorityCacheStats {
        let cache = self.cache.lock();
        AuthorityCacheStats {
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
    /// Carry already charged descriptors across root adoption without making
    /// optional cache residency part of the durable publication boundary.
    fn retain_adopted(&self, captured: impl IntoIterator<Item = Arc<RegisteredBatch>>) {
        let mut captured = captured.into_iter();
        while let Some(first) = captured.next() {
            let mut cache = self.cache.lock();
            for batch in std::iter::once(first).chain(captured.by_ref().take(31)) {
                if cache.entries.contains(&batch.address) {
                    continue;
                }
                if cache.entries.len() == cache.entries.cap().get() {
                    cache.evict();
                }
                let Some(entry_credit) = self.budget.reserve(CACHE_ENTRY_BYTES, 0) else {
                    continue;
                };
                cache.bytes += AuthorityCache::charge(&batch);
                cache.entries.put(
                    batch.address,
                    CachedAuthority {
                        batch,
                        _entry_credit: entry_credit,
                    },
                );
            }
        }
    }
    pub(crate) fn lookup(
        &self,
        address: PhysicalValueAddress,
    ) -> Result<Option<Arc<RegisteredBatch>>, StorageError> {
        if address.partition != self.partition || address.segment_generation != self.generation {
            return Ok(None);
        }
        if let Some(entry) = self.cache.lock().entries.get(&address) {
            return Ok(Some(entry.batch.clone()));
        }
        // The receipt window holds no descriptors: the WAL frame is the authority.
        Ok(self.authority.read().pending_address(address).cloned())
    }
    pub(crate) fn lookup_id(&self, id: BatchId) -> Result<Option<AppendedBatch>, StorageError> {
        let mut work = LookupWork::default();
        let reader = {
            let authority = self.authority.read();
            let hash = authority.index.id_hash(id);
            if let Some(batch) = authority.pending_id(id, hash, &mut work) {
                return Ok(Some(batch.receipt()));
            }
            if !authority.history_may_hold() {
                return Ok(None);
            }
            authority.history.clone()
        };
        Ok(reader.lookup(id)?.map(|record| record.receipt()))
    }
    pub(crate) fn lookup_version(
        &self,
        version: BatchVersion,
    ) -> Result<Option<AppendedBatch>, StorageError> {
        let mut work = LookupWork::default();
        let reader = {
            let authority = self.authority.read();
            let hash = authority.index.version_hash(version);
            if let Some(position) = PendingIndex::find(
                &authority.index.versions,
                hash,
                &authority.pending,
                &mut work,
                |batch| batch.version == version,
            ) {
                return Ok(Some(authority.pending[position].receipt()));
            }
            if !authority.history_may_hold() {
                return Ok(None);
            }
            authority.history.clone()
        };
        Ok(reader
            .lookup_version(version)?
            .map(|record| record.receipt()))
    }
    /// Exact identity-then-version admission lookup under one authority read
    /// guard. Same precedence as `lookup_id` followed by `lookup_version`: a
    /// rooted identity match wins over a pending version match.
    pub(crate) fn lookup_conflict(
        &self,
        id: BatchId,
        version: BatchVersion,
        work: &mut LookupWork,
    ) -> Result<ConflictLookup, StorageError> {
        let (version_pending, history_id, history_version, reader) = {
            let authority = self.authority.read();
            let id_hash = authority.index.id_hash(id);
            if let Some(batch) = authority.pending_id(id, id_hash, work) {
                return Ok(ConflictLookup::Existing(batch.receipt()));
            }
            let version_hash = authority.index.version_hash(version);
            let version_pending = authority.pending_version(version, version_hash, work);
            let history_id = authority.history_may_hold();
            let history_version = !version_pending && authority.history_may_hold();
            let reader = (history_id || history_version).then(|| authority.history.clone());
            (version_pending, history_id, history_version, reader)
        };
        if let Some(reader) = reader {
            if history_id {
                work.history_lookups += 1;
                if let Some(record) = reader.lookup(id)? {
                    return Ok(ConflictLookup::Existing(record.receipt()));
                }
            }
            if !version_pending && history_version {
                work.history_lookups += 1;
                if reader.lookup_version(version)?.is_some() {
                    return Ok(ConflictLookup::VersionTaken);
                }
            }
        }
        Ok(if version_pending {
            ConflictLookup::VersionTaken
        } else {
            ConflictLookup::Free
        })
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
            if start < authority.history.base() {
                return Err(StorageError::Invalid(
                    "receipt ordinal outside the replay window",
                ));
            }
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
            let hash = authority.index.id_hash(receipt.id());
            if let Some(batch) = authority
                .pending_id(receipt.id(), hash, &mut LookupWork::default())
                .filter(|batch| batch.receipt() == receipt)
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
        // Take the history lock before authority, as persist_pending does, and
        // publish the receipt window last.
        let mut history = self.history.lock();
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
                .saturating_sub(extra_fixed_bytes(self.directory.max_metadata_bytes)),
        );
        authority.last_end = group_end;
        authority.pending.extend(batches.into_iter().map(Arc::new));
        // The receipt window consumes each group in O(group size).
        history.append(&authority.pending, 1)?;
        authority.history = history.reader();
        self.directory.retain_adopted(authority.pending.drain(..));
        Ok(())
    }
    pub(crate) fn persist_pending(&mut self, file_count: u64) -> Result<(), StorageError> {
        let captured = self.directory.authority.read().pending.to_vec();
        let mut history = self.history.lock();
        history.append(&captured, file_count)?;
        {
            let mut authority = self.directory.authority.write();
            authority.history = history.reader();
            authority.drain_prefix(captured.len())?;
        }
        drop(history);
        self.directory.retain_adopted(captured);
        Ok(())
    }
}
