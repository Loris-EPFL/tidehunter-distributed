//! Bounded completed/pending authority and cold immutable descriptor pages.
//!
//! Readers capture a small immutable root plus owned metadata files, then do
//! cold I/O without an authority guard. Metadata credits survive final read/CQ
//! owners, not just cache entries. A sole state owner freezes/persists/adopts
//! completed prefixes; it never roots the one unpersisted recovery group.
use super::storage::history::{History, HistoryPersistence, HistoryReader, HistoryRoot};
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
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;

pub(crate) type HistoryWorkerHandle = Arc<Mutex<Option<JoinHandle<()>>>>;

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
    // Hash positions over `pending`, kept in step with it under this lock.
    index: PendingIndex,
    // `None` when the rooted history already held receipts at open: its keys
    // are unknown, so every history lookup reads the on-disk index.
    seen: Option<SeenKeys>,
    last_end: u64,
}

/// Slots per pending position table: a power of two, at least twice the
/// largest pending window, so linear probing keeps its load factor <= 1/2.
const INDEX_SLOTS: usize = 2 * MAX_RESIDENT_BATCHES;
const _: () = assert!(INDEX_SLOTS.is_power_of_two() && INDEX_SLOTS <= u16::MAX as usize);
const FILTER_HASHES: u64 = 3;

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
    /// Reads of the rooted on-disk identity/version index.
    pub(crate) history_lookups: u64,
    /// Rooted-index reads avoided because the filter proved the key absent.
    pub(crate) filter_skips: u64,
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

/// Bloom filter over every key ever published to this directory, pending or
/// rooted. It never forgets a key, so "absent" is exact; "present" only means
/// the rooted index must be read. Once more than a quarter of its bits could be
/// set it stops filtering (every answer is "maybe"), keeping the false-positive
/// rate bounded instead of silently degrading.
struct KeyFilter {
    words: Box<[u64]>,
    inserted: usize,
}
impl KeyFilter {
    fn new(bytes: usize) -> Self {
        Self {
            words: vec![0; bytes / size_of::<u64>()].into_boxed_slice(),
            inserted: 0,
        }
    }
    fn bit_mask(&self) -> u64 {
        (self.words.len() as u64 * 64) - 1
    }
    fn insert(&mut self, hash: u64) {
        let (step, mask) = (hash.rotate_left(32) | 1, self.bit_mask());
        for round in 0..FILTER_HASHES {
            let bit = hash.wrapping_add(round.wrapping_mul(step)) & mask;
            self.words[(bit / 64) as usize] |= 1 << (bit % 64);
        }
        self.inserted = self.inserted.saturating_add(1);
    }
    fn may_contain(&self, hash: u64) -> bool {
        let (step, mask) = (hash.rotate_left(32) | 1, self.bit_mask());
        if (self.inserted as u64).saturating_mul(FILTER_HASHES + 1) > mask + 1 {
            return true;
        }
        (0..FILTER_HASHES).all(|round| {
            let bit = hash.wrapping_add(round.wrapping_mul(step)) & mask;
            self.words[(bit / 64) as usize] & (1 << (bit % 64)) != 0
        })
    }
}
struct SeenKeys {
    ids: KeyFilter,
    versions: KeyFilter,
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
    /// False only when the rooted history provably lacks the key.
    fn history_may_hold(
        &self,
        filter: impl Fn(&SeenKeys) -> &KeyFilter,
        hash: u64,
        work: &mut LookupWork,
    ) -> bool {
        if self.history.root().count == 0 {
            return false;
        }
        if self.history.windowed() {
            return true;
        }
        if let Some(seen) = &self.seen
            && !filter(seen).may_contain(hash)
        {
            work.filter_skips += 1;
            return false;
        }
        true
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
        + size_of::<Mutex<Option<JoinHandle<()>>>>()
        + 6 * size_of::<usize>()
        // One bounded weak-registry notifier and its Arc allocation.
        + size_of::<[usize; 8]>()
        + 2 * size_of::<usize>()
        + CACHE_TABLE_BYTES
        // Preallocated pending window plus the immutable spill capture. Charge
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

// Each of the two history key filters gets 1/128 of the budget headroom (the
// largest power of two not above it), capped at 256 KiB. A budget too small for
// 2 KiB per filter has no filter and always reads the rooted index.
const MIN_FILTER_BYTES: usize = 2048;
const MAX_FILTER_BYTES: usize = 256 * 1024;
fn filter_bytes(metadata_limit: usize) -> usize {
    let target = (metadata_limit.saturating_sub(root_charge()) / 128).min(MAX_FILTER_BYTES);
    if target < MIN_FILTER_BYTES {
        0
    } else {
        1 << target.ilog2()
    }
}

/// Fixed charge taken from the budget at open, before any batch.
fn extra_fixed_bytes(metadata_limit: usize) -> usize {
    extra_cache_table_bytes(metadata_limit) + 2 * filter_bytes(metadata_limit)
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

/// Opt-in observations of authority history maintenance for one open handle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct HistoryMaintenanceStats {
    pub spills: u64,
    pub batches: u64,
    pub max_batch_size: u64,
    pub failures: u64,
    pub waits: u64,
    pub wait_ns: u64,
    pub persist_ns: u64,
    pub max_wait_ns: u64,
    pub max_persist_ns: u64,
    pub adoption_ns: u64,
}

#[derive(Default)]
struct HistoryMaintenance {
    enabled: AtomicBool,
    spills: AtomicU64,
    batches: AtomicU64,
    max_batch_size: AtomicU64,
    failures: AtomicU64,
    waits: AtomicU64,
    wait_ns: AtomicU64,
    persist_ns: AtomicU64,
    max_wait_ns: AtomicU64,
    max_persist_ns: AtomicU64,
    adoption_ns: AtomicU64,
}

impl HistoryMaintenance {
    fn snapshot(&self) -> HistoryMaintenanceStats {
        HistoryMaintenanceStats {
            spills: self.spills.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            max_batch_size: self.max_batch_size.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            wait_ns: self.wait_ns.load(Ordering::Relaxed),
            persist_ns: self.persist_ns.load(Ordering::Relaxed),
            max_wait_ns: self.max_wait_ns.load(Ordering::Relaxed),
            max_persist_ns: self.max_persist_ns.load(Ordering::Relaxed),
            adoption_ns: self.adoption_ns.load(Ordering::Relaxed),
        }
    }
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
    failed: Arc<AtomicBool>,
    failure_notifier: OnceLock<Arc<dyn Fn() + Send + Sync>>,
    failure_notified: AtomicBool,
    maintenance: HistoryMaintenance,
    #[cfg(test)]
    pause_history: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    #[cfg(test)]
    fail_history_after_root: AtomicBool,
}
pub(crate) struct ReferenceDirectoryWriter {
    directory: Arc<ReferenceDirectory>,
    history: Arc<Mutex<History>>,
    background: HistoryWorkerHandle,
    failed: Arc<AtomicBool>,
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
        path: &Path,
        config: &PartitionConfig,
        create: bool,
        mode: HistoryPersistence,
    ) -> Result<(Arc<Self>, ReferenceDirectoryWriter), StorageError> {
        Self::check_fresh_total_capacity(
            config.max_batches,
            config.max_reference_metadata_bytes,
            0,
            0,
        )?;
        let history = if mode != HistoryPersistence::OnDisk {
            History::windowed(path, config, mode)
        } else if create {
            History::create(path, config)?
        } else {
            History::open(path, config)?
        };
        let reader = history.reader();
        let last_end = reader.root().end;
        let failed = Arc::new(AtomicBool::new(false));
        let directory = Arc::new(Self {
            partition: config.partition,
            generation: config.inventory.segment_generation,
            max_batches: config.max_batches.min(MAX_RESIDENT_BATCHES),
            max_metadata_bytes: config.max_reference_metadata_bytes,
            authority: RwLock::new(Authority {
                seen: (reader.root().count == 0
                    && filter_bytes(config.max_reference_metadata_bytes) != 0)
                    .then(|| {
                        let bytes = filter_bytes(config.max_reference_metadata_bytes);
                        SeenKeys {
                            ids: KeyFilter::new(bytes),
                            versions: KeyFilter::new(bytes),
                        }
                    }),
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
            reads: AuthorityReadCounters::default(),
            failed: failed.clone(),
            failure_notifier: OnceLock::new(),
            failure_notified: AtomicBool::new(false),
            maintenance: HistoryMaintenance::default(),
            #[cfg(test)]
            pause_history: Mutex::new(None),
            #[cfg(test)]
            fail_history_after_root: AtomicBool::new(false),
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
                background: Arc::new(Mutex::new(None)),
                failed,
            },
        ))
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
    pub(crate) fn set_history_diagnostics(&self, enabled: bool) {
        self.maintenance.enabled.store(enabled, Ordering::Release);
    }

    pub(crate) fn history_stats(&self) -> HistoryMaintenanceStats {
        self.maintenance.snapshot()
    }

    pub(crate) fn wait_started(&self) -> Option<std::time::Instant> {
        self.maintenance
            .enabled
            .load(Ordering::Acquire)
            .then(std::time::Instant::now)
    }

    pub(crate) fn wait_finished(&self, started: Option<std::time::Instant>) {
        if let Some(started) = started {
            self.maintenance.waits.fetch_add(1, Ordering::Relaxed);
            let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            self.maintenance
                .wait_ns
                .fetch_add(elapsed, Ordering::Relaxed);
            self.maintenance
                .max_wait_ns
                .fetch_max(elapsed, Ordering::Relaxed);
        }
    }
    #[cfg(test)]
    pub(crate) fn pause_next_history(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        *self.pause_history.lock() = Some((entered, released));
        (started, resume)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_history_after_root(&self) {
        self.fail_history_after_root.store(true, Ordering::Release);
    }
    /// Register one non-panicking failure action. It must not acquire the
    /// partition state or inventory locks; a caller can hold either at failure.
    pub(crate) fn set_history_failure_notifier(&self, notifier: Arc<dyn Fn() + Send + Sync>) {
        let _ = self.failure_notifier.set(notifier);
        if self.failed() {
            self.notify_failure();
        }
    }

    fn notify_failure(&self) {
        if let Some(notifier) = self.failure_notifier.get()
            && !self.failure_notified.swap(true, Ordering::AcqRel)
        {
            notifier();
        }
    }

    pub(crate) fn mark_failed(&self) {
        self.failed.store(true, Ordering::Release);
        self.notify_failure();
    }

    fn check_health(&self) -> Result<(), StorageError> {
        if self.failed() {
            Err(StorageError::Poisoned)
        } else {
            Ok(())
        }
    }

    pub(crate) fn history_persistence(&self) -> HistoryPersistence {
        self.authority.read().history.persistence()
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
    pub(crate) fn lookup_resident(
        &self,
        address: PhysicalValueAddress,
    ) -> Option<Arc<RegisteredBatch>> {
        if address.partition != self.partition || address.segment_generation != self.generation {
            return None;
        }
        if let Some(mut cache) = self.cache.try_lock() {
            if let Some(entry) = cache.entries.get(&address) {
                self.reads.resident_hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.batch.clone());
            }
        } else {
            self.reads.contention.fetch_add(1, Ordering::Relaxed);
        }
        let Some(authority) = self.authority.try_read() else {
            self.reads.contention.fetch_add(1, Ordering::Relaxed);
            self.reads.resident_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let batch = authority.pending_address(address).cloned();
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
        self.check_health()?;
        if address.partition != self.partition || address.segment_generation != self.generation {
            return Ok(None);
        }
        if let Some(entry) = self.cache.lock().entries.get(&address) {
            return Ok(Some(entry.batch.clone()));
        }
        let reader = {
            let authority = self.authority.read();
            if let Some(batch) = authority.pending_address(address) {
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
        let (credit, entry_credit) = {
            let mut cache = self.cache.lock();
            if let Some(entry) = cache.entries.get(&address) {
                return Ok(Some(entry.batch.clone()));
            }
            match self.reserve_with_cache(&mut cache, charge, count) {
                Ok(mut credit) => {
                    let core_credit = credit.take_batch(count)?;
                    (core_credit, Some(credit))
                }
                Err(StorageError::Capacity) => {
                    (self.reserve_with_cache(&mut cache, core, count)?, None)
                }
                Err(error) => return Err(error),
            }
        };
        // The complete decoded heap is reserved BEFORE the first descriptor is
        // allocated. Streaming fixed slots need only a bounded stack buffer.
        let batch = Arc::new(reader.read_registered(&page)?.with_credit(credit));
        if let Some(entry_credit) = entry_credit {
            let mut cache = self.cache.lock();
            if let Some(existing) = cache.entries.get(&address) {
                return Ok(Some(existing.batch.clone()));
            }
            if cache.entries.len() == cache.entries.cap().get() {
                cache.evict();
            }
            cache.bytes += charge;
            cache.entries.put(
                address,
                CachedAuthority {
                    batch: batch.clone(),
                    _entry_credit: entry_credit,
                },
            );
        }
        Ok(Some(batch))
    }
    pub(crate) fn lookup_id(&self, id: BatchId) -> Result<Option<AppendedBatch>, StorageError> {
        self.check_health()?;
        let mut work = LookupWork::default();
        let reader = {
            let authority = self.authority.read();
            let hash = authority.index.id_hash(id);
            if let Some(batch) = authority.pending_id(id, hash, &mut work) {
                return Ok(Some(batch.receipt()));
            }
            if !authority.history_may_hold(|seen| &seen.ids, hash, &mut work) {
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
        self.check_health()?;
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
            if !authority.history_may_hold(|seen| &seen.versions, hash, &mut work) {
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
        self.check_health()?;
        let (version_pending, history_id, history_version, reader) = {
            let authority = self.authority.read();
            let id_hash = authority.index.id_hash(id);
            if let Some(batch) = authority.pending_id(id, id_hash, work) {
                return Ok(ConflictLookup::Existing(batch.receipt()));
            }
            let version_hash = authority.index.version_hash(version);
            let version_pending = authority.pending_version(version, version_hash, work);
            let history_id = authority.history_may_hold(|seen| &seen.ids, id_hash, work);
            let history_version = !version_pending
                && authority.history_may_hold(|seen| &seen.versions, version_hash, work);
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
        self.check_health()?;
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
    pub(crate) fn checkpoint_prefix(
        &self,
        prefix: usize,
        database: [u8; 16],
        epoch: u64,
    ) -> Result<[u8; 32], StorageError> {
        self.check_health()?;
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
            if prefix - 1 < authority.history.base() {
                return Err(StorageError::Invalid(
                    "receipt ordinal outside the replay window",
                ));
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
        self.check_health()?;
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
    pub(crate) fn set_history_persistence(
        &mut self,
        mode: HistoryPersistence,
    ) -> Result<(), StorageError> {
        let mut history = self.history.lock();
        history.set_persistence(mode)?;
        self.directory.authority.write().history = history.reader();
        Ok(())
    }

    pub(crate) fn windowed(&self) -> bool {
        self.directory.authority.read().history.windowed()
    }

    pub(crate) fn wait_background(&self) -> Result<(), StorageError> {
        let mut started = None;
        let mut background = match self.background.try_lock() {
            Some(slot) => slot,
            None => {
                started = self.directory.wait_started();
                self.background.lock()
            }
        };
        if started.is_none() && background.is_some() {
            started = self.directory.wait_started();
        }
        if let Some(handle) = background.take()
            && handle.join().is_err()
        {
            self.directory.mark_failed();
        }
        self.directory.wait_finished(started);
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::Poisoned);
        }
        Ok(())
    }

    pub(crate) fn background_handle(&self) -> (HistoryWorkerHandle, Arc<AtomicBool>) {
        (self.background.clone(), self.failed.clone())
    }

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
        // WAL replay has no history worker. Take the existing history lock before
        // authority, as persist_pending does, and publish the receipt window last.
        let mut history = (self.directory.history_persistence() == HistoryPersistence::WalReplay)
            .then(|| self.history.lock());
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
        let first = authority.pending.len();
        authority.pending.extend(batches.into_iter().map(Arc::new));
        if let Some(history) = &mut history {
            // No pending radix rebuild or spill is needed for a replay window.
            // Its preallocated ring/maps consume each group in O(group size).
            history.append(&authority.pending, 1)?;
            authority.history = history.reader();
            self.directory.retain_adopted(authority.pending.drain(..));
            return Ok(());
        }
        let Authority {
            pending,
            index,
            seen,
            ..
        } = &mut *authority;
        for position in first..pending.len() {
            if !index.push(pending, position) {
                return Err(DirectoryError::Capacity.into());
            }
            if let Some(seen) = seen {
                seen.ids.insert(index.id_hash(pending[position].id));
                seen.versions
                    .insert(index.version_hash(pending[position].version));
            }
        }
        Ok(())
    }
    /// Start one durable-prefix rewrite. The prepaid root charge covers the
    /// captured Arc vector; the batches themselves remain charged until all
    /// read owners release them. The pending prefix stays visible until adopt.
    /// Valkey 9.0.7 `src/aof.c` rotates its incremental log before rewriting
    /// and publishes the manifest only after the new base succeeds. Here the
    /// old immutable root and live WAL suffix remain authoritative until our
    /// separately synced root is installed; WAL fsync policy is unchanged.
    pub(crate) fn start_background(
        &mut self,
        durable_end: u64,
        file_count: u64,
        path: &Path,
    ) -> Result<bool, StorageError> {
        if self.windowed() {
            return Ok(false);
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::Poisoned);
        }
        let Some(mut slot) = self.background.try_lock() else {
            return Ok(false);
        };
        if slot.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return Ok(false);
        }
        if let Some(handle) = slot.take()
            && (handle.join().is_err() || self.failed.load(Ordering::Acquire))
        {
            self.directory.mark_failed();
            return Err(StorageError::Poisoned);
        }
        let captured = {
            let authority = self.directory.authority.read();
            let completed = authority
                .pending
                .partition_point(|batch| batch.group_end <= durable_end);
            if completed == 0 {
                return Ok(false);
            }
            authority.pending[..completed].to_vec()
        };
        let directory = self.directory.clone();
        let history = self.history.clone();
        let path = path.to_path_buf();
        let worker = std::thread::Builder::new()
            .name("authority-history".into())
            .spawn(move || {
                let measured = directory.maintenance.enabled.load(Ordering::Acquire);
                let started = measured.then(std::time::Instant::now);
                if measured {
                    directory.maintenance.spills.fetch_add(1, Ordering::Relaxed);
                    directory
                        .maintenance
                        .batches
                        .fetch_add(captured.len() as u64, Ordering::Relaxed);
                    directory
                        .maintenance
                        .max_batch_size
                        .fetch_max(captured.len() as u64, Ordering::Relaxed);
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || -> Result<(), StorageError> {
                        #[cfg(test)]
                        if let Some((entered, resume)) = directory.pause_history.lock().take() {
                            let _ = entered.send(());
                            let _ = resume.recv();
                        }
                        let mut history = history.lock();
                        history.append(&captured, file_count)?;
                        #[cfg(test)]
                        if directory
                            .fail_history_after_root
                            .swap(false, Ordering::AcqRel)
                        {
                            return Err(StorageError::Invalid(
                                "injected failure after durable history root",
                            ));
                        }
                        if let Some(started) = started {
                            let elapsed =
                                started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                            directory
                                .maintenance
                                .persist_ns
                                .fetch_add(elapsed, Ordering::Relaxed);
                            directory
                                .maintenance
                                .max_persist_ns
                                .fetch_max(elapsed, Ordering::Relaxed);
                        }
                        let adopting = measured.then(std::time::Instant::now);
                        let root = history.reader();
                        {
                            let mut authority = directory.authority.write();
                            if authority.pending.len() < captured.len()
                                || !authority
                                    .pending
                                    .iter()
                                    .zip(&captured)
                                    .all(|(a, b)| Arc::ptr_eq(a, b))
                            {
                                return Err(StorageError::Invalid(
                                    "history prefix changed before adoption",
                                ));
                            }
                            authority.history = root;
                            authority.drain_prefix(captured.len())?;
                        }
                        drop(history);
                        directory.retain_adopted(captured);
                        if let Some(adopting) = adopting {
                            directory.maintenance.adoption_ns.fetch_add(
                                adopting.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                                Ordering::Relaxed,
                            );
                        }
                        super::storage::file_inventory::remove_covered(&path, file_count)?;
                        Ok(())
                    },
                ));
                if !matches!(result, Ok(Ok(()))) {
                    if measured {
                        directory
                            .maintenance
                            .failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    directory.mark_failed();
                }
            });
        match worker {
            Ok(handle) => *slot = Some(handle),
            Err(error) => {
                if self.directory.maintenance.enabled.load(Ordering::Acquire) {
                    self.directory
                        .maintenance
                        .failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.directory.mark_failed();
                return Err(error.into());
            }
        }
        Ok(true)
    }

    pub(crate) fn background_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(crate) fn background_active(&self) -> bool {
        self.background.try_lock().is_none_or(|slot| slot.is_some())
    }

    pub(crate) fn should_start_background(&self, durable_end: u64) -> bool {
        let authority = self.directory.authority.read();
        if authority.history.windowed() {
            return false;
        }
        let used = root_charge().saturating_add(self.directory.budget.used.load(Ordering::Acquire));
        authority.pending.len() >= self.directory.max_batches.div_ceil(2)
            || used >= self.directory.max_metadata_bytes / 2
            || durable_end.saturating_sub(authority.history.root().end) >= 32 * 1024 * 1024
    }

    pub(crate) fn persist_pending(&mut self, file_count: u64) -> Result<(), StorageError> {
        self.wait_background()?;
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
    pub(crate) fn retire(&mut self, offsets: &[u64]) -> Result<(), StorageError> {
        self.wait_background()?;
        let mut history = self.history.lock();
        history.retire(offsets)?;
        self.directory.authority.write().history = history.reader();
        Ok(())
    }
}

impl Drop for ReferenceDirectoryWriter {
    fn drop(&mut self) {
        let _ = self.wait_background();
    }
}
