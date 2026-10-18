//! Bounded, single-owner local database over independently persisted Value WALs.
//!
//! This is an explicitly selected experimental backend. It uses native pending
//! transactions and typed native index cells; it never writes a second copy of
//! user values into a local Db. Paper §§3.1–3.4 require complete WAL authority,
//! logical publication and replay to agree. CRC-rooted index/receipt history
//! permits only whole-unreferenced sealed-file retirement. Local file locks
//! remain local authority, not remote leases.

use super::codec::{CompleteBatch, Mutation};
use super::completion::{CompletionEvent, CompletionInbox, CompletionTicket};
use super::local_inventory::{LocalInventory, LocalInventoryConfig};
use super::native_index::{IndexReadCut, IndexReadPin, NativeIndex};
use super::native_publication::PreparedNativePublication;
use super::publication::PublicationTracker;
use super::sequence::LogicalClock;
use super::storage::{PartitionLog, PartitionStats, RecordReadProfile};
use super::types::{
    BatchId, BatchVersion, MutationVersion, RequestAuthority, ValueRecordReference,
};
use super::worker::{GroupCommitWorker, PendingBatch, WorkerConfig, WorkerStats};
use crate::key_shape::{KeyShape, KeySpace};
use crate::metrics::Metrics;
use minibytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

mod checkpoint;
pub const BACKEND_ID: &str = "experimental-native-cell-sharded-value-wal-local-rf1";

#[derive(Clone, Debug)]
pub struct LocalDatabaseConfig {
    pub storage: LocalInventoryConfig,
    pub worker: WorkerConfig,
    /// Maximum admitted requests retained in the current publication window.
    pub max_admitted_batches: usize,
    /// Typed index metadata plus outstanding publication reservations.
    pub max_index_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalDbStats {
    pub admitted_batches: u64,
    pub published_batches: u64,
    pub validation_ns: u64,
    pub dispatch_ns: u64,
    /// Caller wait, not a device-service or fsync measurement.
    pub storage_wait_ns: u64,
    pub publication_ns: u64,
    pub read_resolve_ns: u64,
    pub value_read_ns: u64,
}

#[derive(Clone, Debug)]
pub struct LocalCheckpoint {
    pub(crate) database: [u8; 16],
    pub(crate) cut: Option<MutationVersion>,
    pub(crate) pin: Option<IndexReadPin>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalReadProfile {
    pub index_ns: u64,
    pub record: RecordReadProfile,
}

impl LocalCheckpoint {
    pub fn cut(&self) -> Option<MutationVersion> {
        self.cut
    }
}

struct Submission {
    partition: usize,
    batch: Option<CompleteBatch>,
    pending: Option<PendingBatch>,
    completion: Option<CompletionTicket>,
    metadata_bytes: usize,
}

/// One mutable authority, with pipelined independent storage workers.
///
/// `submit` returns an admission/version token, never a commit acknowledgement.
/// `drain` publishes the entire submitted prefix or fails closed; `write_batch`
/// combines both. Reads observe only published history. A checkpoint drains
/// first and retains a native index read pin. Durable roots retain pin-clamped
/// history before sealed, wholly unreachable payload files can be retired.
pub struct LocalDatabase {
    // Workers shut down before this owner is released; logs also retain the root
    // inventory lock through their authority anchor if a worker outlives a field.
    inventory: LocalInventory,
    workers: Vec<GroupCommitWorker>,
    config: LocalDatabaseConfig,
    path: PathBuf,
    index: NativeIndex,
    logs: HashMap<(u64, u32), Arc<PartitionLog>>,
    active_identities: HashMap<BatchId, usize>,
    clock: LogicalClock,
    publication: PublicationTracker,
    completions: CompletionInbox,
    submissions: Vec<Submission>,
    submission_base: u64,
    next_dispatch: usize,
    next_completion: usize,
    queued_requests: Vec<usize>,
    queued_bytes: Vec<usize>,
    reserved_index_bytes: usize,
    latest: Option<MutationVersion>,
    snapshot_generation: u64,
    healthy: bool,
    stopped: bool,
    stats: LocalDbStats,
}

impl LocalDatabase {
    pub fn open(
        path: &Path,
        shape: &KeyShape,
        config: LocalDatabaseConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, LocalDbError> {
        if config.max_admitted_batches == 0 || config.max_admitted_batches > 1_000_000 {
            return Err(LocalDbError::new("invalid database admission capacity"));
        }
        // Validate the index configuration before filesystem effects.
        let mut index = NativeIndex::new(shape, config.storage.database, config.max_index_bytes)
            .map_err(LocalDbError::component)?;
        let mut loaded = None;
        let inventory = LocalInventory::open_or_create_checkpoint(
            path,
            config.storage.clone(),
            shape,
            metrics,
            |canonical| {
                loaded =
                    checkpoint::LocalRoot::load(canonical, shape, &config).map_err(|error| {
                        super::local_inventory::LocalInventoryError::Io(std::io::Error::other(
                            error,
                        ))
                    })?;
                Ok(loaded.is_some())
            },
        )
        .map_err(LocalDbError::component)?;
        let mut latest = None;
        let root = if let Some((root, recovered_index)) = loaded {
            index = recovered_index;
            latest = root.latest;
            Some(root)
        } else {
            None
        };
        let mut logs = HashMap::new();
        let mut replay = Vec::with_capacity(inventory.recovered_partitions().len());
        for log in inventory.recovered_partitions() {
            let epoch = log.config().inventory.epoch;
            logs.insert((epoch, log.config().partition), log.clone());
            let retained = root.as_ref().and_then(|root| {
                root.prefixes.iter().find(|prefix| {
                    prefix.epoch == epoch && prefix.partition == log.config().partition
                })
            });
            let start = if let Some(prefix) = retained {
                if prefix.count > log.receipt_count()
                    || log
                        .checkpoint_prefix(prefix.count)
                        .map_err(LocalDbError::component)?
                        != prefix.digest
                {
                    return Err(LocalDbError::new(
                        "local index recovery prefix differs from storage authority",
                    ));
                }
                prefix.count
            } else {
                0
            };
            let next = log
                .receipt_page(start, 1)
                .map_err(LocalDbError::component)?
                .first()
                .copied();
            replay.push((log.clone(), start, next));
        }
        if let Some(root) = &root {
            for prefix in &root.prefixes {
                if !logs.contains_key(&(prefix.epoch, prefix.partition)) {
                    return Err(LocalDbError::new(
                        "local recovery root names missing storage authority",
                    ));
                }
            }
        }
        let mut last_version = latest.map(|cut| cut.batch);
        while let Some(slot) = replay
            .iter()
            .enumerate()
            .filter_map(|(slot, (_, _, receipt))| receipt.map(|receipt| (slot, receipt.version())))
            .min_by_key(|(_, version)| *version)
            .map(|(slot, _)| slot)
        {
            let (log, ordinal, next) = &mut replay[slot];
            let receipt = next
                .take()
                .ok_or_else(|| LocalDbError::new("missing replay cursor"))?;
            if last_version.is_some_and(|previous| previous >= receipt.version()) {
                return Err(LocalDbError::new(
                    "duplicate or unordered recovered logical version",
                ));
            }
            for other in logs.values() {
                if (other.config().inventory.epoch, other.config().partition)
                    != (log.config().inventory.epoch, log.config().partition)
                    && other
                        .lookup(receipt.id())
                        .map_err(LocalDbError::component)?
                        .is_some()
                {
                    return Err(LocalDbError::new(
                        "duplicate recovered stable operation identity",
                    ));
                }
            }
            let batch = log.read_batch(receipt).map_err(LocalDbError::component)?;
            let bytes = index
                .estimated_batch_bytes(&batch)
                .map_err(LocalDbError::component)?;
            if index.check_capacity(bytes).is_err() {
                let name = inventory.path().join(format!(
                    "index-replay-{}-{}-{}.snapshot",
                    inventory.active_inventory().epoch,
                    receipt.version().epoch,
                    receipt.version().sequence
                ));
                index
                    .flush_and_unload(&name, index.latest_cut())
                    .map_err(LocalDbError::component)?;
            }
            index
                .check_capacity(bytes)
                .map_err(LocalDbError::component)?;
            let durable = log
                .persist_through(receipt)
                .map_err(LocalDbError::component)?;
            PreparedNativePublication::prepare(log, &batch, durable)
                .map_err(LocalDbError::component)?
                .publish(&mut index)
                .map_err(LocalDbError::component)?;
            index.promote_committed().map_err(LocalDbError::component)?;
            last_version = Some(receipt.version());
            latest = batch.mutation_version(batch.mutations().len() - 1);
            *ordinal += 1;
            *next = log
                .receipt_page(*ordinal, 1)
                .map_err(LocalDbError::component)?
                .first()
                .copied();
        }
        let epoch = inventory.active_inventory().epoch;
        let database = config.storage.database;
        let mut workers = Vec::new();
        for log in inventory.active_partitions() {
            logs.insert((epoch, log.config().partition), log.clone());
            workers.push(
                GroupCommitWorker::start(log.clone(), shape, config.worker)
                    .map_err(LocalDbError::component)?,
            );
        }
        let count = workers.len();
        let canonical_path = inventory.path().to_path_buf();
        Ok(Self {
            inventory,
            workers,
            path: canonical_path,
            index,
            logs,
            active_identities: HashMap::new(),
            clock: LogicalClock::new_fenced_epoch(epoch),
            publication: PublicationTracker::new(database, epoch, config.max_admitted_batches)
                .map_err(LocalDbError::component)?,
            completions: CompletionInbox::new(database, epoch, config.max_admitted_batches)
                .map_err(LocalDbError::component)?,
            config,
            submissions: Vec::new(),
            submission_base: 0,
            next_dispatch: 0,
            next_completion: 0,
            queued_requests: vec![0; count],
            queued_bytes: vec![0; count],
            reserved_index_bytes: 0,
            latest,
            snapshot_generation: 0,
            healthy: true,
            stopped: false,
            stats: LocalDbStats::default(),
        })
    }

    pub fn backend_id(&self) -> &'static str {
        BACKEND_ID
    }

    fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        if self.stopped {
            return Err(LocalDbError::new("database is closed"));
        }
        if !self.healthy {
            return Err(LocalDbError::new("database requires fenced recovery"));
        }
        self.publication
            .ensure_publication_healthy()
            .map_err(LocalDbError::component)
    }

    /// Validate and reserve bounded state before allocating a logical position.
    /// The caller owns worker submission; no other producer can consume credits
    /// between preflight and dispatch. Worker failure after allocation poisons
    /// this authority rather than fabricating a hole and continuing.
    pub fn submit(
        &mut self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.ensure_healthy()?;
        let began = Instant::now();
        let id = BatchId {
            database: self.config.storage.database,
            operation,
        };
        let mut previous = None;
        for log in self.logs.values() {
            if let Some(receipt) = log.lookup(id).map_err(LocalDbError::component)?
                && previous.replace(receipt).is_some()
            {
                return Err(LocalDbError::new("duplicate retained operation identity"));
            }
        }
        if let Some(receipt) = previous {
            let retried = CompleteBatch::new(
                self.inventory.shape(),
                id,
                receipt.version(),
                mutations,
                self.config.storage.batch_limits,
            )
            .map_err(LocalDbError::component)?;
            if retried.digest() != receipt.digest() {
                return Err(LocalDbError::new("retry changed the original batch"));
            }
            return Ok(receipt.version());
        }
        // An in-flight retry must retain the original version and payload too.
        if let Some(&slot) = self.active_identities.get(&id) {
            let original = self.submissions[slot].batch.as_ref().unwrap();
            let retried = CompleteBatch::new(
                self.inventory.shape(),
                id,
                original.version(),
                mutations,
                self.config.storage.batch_limits,
            )
            .map_err(LocalDbError::component)?;
            if retried.digest() != original.digest() {
                return Err(LocalDbError::new(
                    "in-flight retry changed the original batch",
                ));
            }
            return Ok(original.version());
        }
        if self.submissions.len() == self.config.max_admitted_batches {
            return Err(LocalDbError::new("publication window capacity reached"));
        }
        let version = BatchVersion {
            epoch: self.inventory.active_inventory().epoch,
            sequence: self
                .submission_base
                .checked_add(self.submissions.len() as u64)
                .ok_or_else(|| LocalDbError::new("logical sequence exhausted"))?,
        };
        let batch = CompleteBatch::new(
            self.inventory.shape(),
            id,
            version,
            mutations,
            self.config.storage.batch_limits,
        )
        .map_err(LocalDbError::component)?
        .into_canonical_owned();
        let partition = self
            .inventory
            .active_inventory()
            .partition_for(id)
            .map_err(LocalDbError::component)?;
        let partition_index = self
            .inventory
            .active_partitions()
            .iter()
            .position(|log| log.config().partition == partition)
            .ok_or_else(|| LocalDbError::new("partition absent from active inventory"))?;
        let metadata_bytes = self
            .index
            .estimated_batch_bytes(&batch)
            .map_err(LocalDbError::component)?;
        let reserved = self
            .reserved_index_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
        if self.index.check_capacity(reserved).is_err() && self.submissions.is_empty() {
            self.persist_index_root(self.index.retention_floor())?;
        }
        self.index
            .check_capacity(reserved)
            .map_err(LocalDbError::component)?;
        self.inventory.active_partitions()[partition_index]
            .check_reference_admission(1, batch.mutations().len())
            .map_err(LocalDbError::component)?;
        let worker_bytes = self.workers[partition_index]
            .check_admission_reserved(
                &batch,
                self.queued_requests[partition_index],
                self.queued_bytes[partition_index],
            )
            .map_err(LocalDbError::component)?;
        self.stats.validation_ns += elapsed_ns(began);

        // All normal rejection checks above occur before version allocation and
        // before any authoritative bytes. From this point fail closed on unwind.
        self.healthy = false;
        if self.clock.allocate() != Some(version) {
            return Err(LocalDbError::unknown(
                version,
                "logical allocation disagrees with admission",
            ));
        }
        self.publication
            .admit(&batch)
            .map_err(|e| LocalDbError::unknown(version, e))?;
        let completion = self
            .completions
            .register(&batch)
            .map_err(|e| LocalDbError::unknown(version, e))?;
        self.publication
            .mark_in_flight(id)
            .map_err(|e| LocalDbError::unknown(version, e))?;
        // submit is admission only. Keep the caller's explicit window together
        // until drain, rather than waking a worker before sibling batches arrive.
        self.queued_requests[partition_index] += 1;
        self.queued_bytes[partition_index] += worker_bytes;
        self.reserved_index_bytes = reserved;
        self.active_identities.insert(id, self.submissions.len());
        self.submissions.push(Submission {
            partition: partition_index,
            batch: Some(batch),
            pending: None,
            completion: Some(completion),
            metadata_bytes,
        });
        self.stats.admitted_batches += 1;
        self.healthy = true;
        Ok(version)
    }

    /// Complete and atomically publish all admitted batches in logical order.
    /// Storage workers execute concurrently. Waiting here is caller latency;
    /// it must not be presented as an isolated SSD or network service time.
    pub fn drain(&mut self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        self.healthy = false;
        let dispatch = Instant::now();
        let mut windows = vec![Vec::new(); self.workers.len()];
        for slot in self.next_dispatch..self.submissions.len() {
            windows[self.submissions[slot].partition].push(slot);
        }
        for (partition, slots) in windows.into_iter().enumerate() {
            if slots.is_empty() {
                continue;
            }
            let version = self.submissions[slots[0]].batch.as_ref().unwrap().version();
            let batches = slots
                .iter()
                .map(|i| self.submissions[*i].batch.as_ref().unwrap().clone())
                .collect();
            let pending = self.workers[partition]
                .try_submit_many(batches)
                .map_err(|e| LocalDbError::unknown(version, e))?;
            for (slot, pending) in slots.into_iter().zip(pending) {
                self.submissions[slot].pending = Some(pending);
            }
            self.queued_requests[partition] = 0;
            self.queued_bytes[partition] = 0;
        }
        self.next_dispatch = self.submissions.len();
        self.stats.dispatch_ns += elapsed_ns(dispatch);
        while self.next_completion < self.submissions.len() {
            let slot = self.next_completion;
            let version = self.submissions[slot].batch.as_ref().unwrap().version();
            let wait = Instant::now();
            let durable = self.submissions[slot]
                .pending
                .take()
                .unwrap()
                .wait()
                .map_err(|e| LocalDbError::unknown(version, e))?;
            self.stats.storage_wait_ns += elapsed_ns(wait);
            self.submissions[slot]
                .completion
                .take()
                .unwrap()
                .complete(durable)
                .map_err(|e| LocalDbError::unknown(version, e))?;
            self.next_completion += 1;
        }
        for event in self.completions.poll(self.submissions.len()) {
            match event {
                CompletionEvent::Durable(durable) => {
                    self.publication
                        .record_durable(durable)
                        .map_err(LocalDbError::component)?;
                }
                CompletionEvent::Unknown { id, .. } => {
                    self.publication
                        .mark_unknown(id)
                        .map_err(LocalDbError::component)?;
                }
            }
        }
        let started = Instant::now();
        let index = &mut self.index;
        let submissions = &mut self.submissions;
        let active_identities = &mut self.active_identities;
        let latest = &mut self.latest;
        let reserved = &mut self.reserved_index_bytes;
        let partitions = self.inventory.active_partitions();
        let submission_base = self.submission_base;
        let progress = self
            .publication
            .publish_ready(|durable| {
                let receipt = durable.appended();
                let entry =
                    &mut submissions[(receipt.version().sequence - submission_base) as usize];
                let batch = entry.batch.as_ref().unwrap();
                PreparedNativePublication::prepare(&partitions[entry.partition], batch, durable)
                    .map_err(LocalDbError::component)?
                    .publish(index)
                    .map_err(LocalDbError::component)?;
                index.promote_committed().map_err(LocalDbError::component)?;
                *latest = batch.mutation_version(batch.mutations().len() - 1);
                active_identities.remove(&receipt.id());
                *reserved -= entry.metadata_bytes;
                entry.batch = None;
                Ok::<_, LocalDbError>(())
            })
            .map_err(LocalDbError::component)?;
        self.stats.publication_ns += elapsed_ns(started);
        self.stats.published_batches += progress.callbacks_completed;
        if progress.published.next_sequence()
            != self.submission_base + self.submissions.len() as u64
        {
            return Err(LocalDbError::new(
                "unresolved publication prefix requires recovery",
            ));
        }
        self.submission_base = progress.published.next_sequence();
        self.publication
            .retire_published()
            .map_err(LocalDbError::component)?;
        self.completions.retire_completed();
        self.submissions.clear();
        self.next_dispatch = 0;
        self.next_completion = 0;
        self.healthy = true;
        Ok(())
    }

    pub fn write_batch(
        &mut self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        let version = self.submit(operation, mutations)?;
        self.drain()?;
        Ok(version)
    }

    pub fn checkpoint(&mut self) -> Result<LocalCheckpoint, LocalDbError> {
        self.drain()?;
        Ok(LocalCheckpoint {
            database: self.config.storage.database,
            cut: self.latest,
            pin: Some(
                self.index
                    .pin_cut(
                        self.latest
                            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
                    )
                    .map_err(LocalDbError::component)?,
            ),
        })
    }

    fn resolve(&mut self, reference: ValueRecordReference) -> Result<Mutation, LocalDbError> {
        let started = Instant::now();
        let log = self
            .logs
            .get(&(
                reference.batch_version.epoch,
                reference.batch_address.partition,
            ))
            .ok_or_else(|| LocalDbError::new("value reference names missing epoch/partition"))?;
        let result = log.read_record(&reference).map_err(LocalDbError::component);
        self.stats.value_read_ns += elapsed_ns(started);
        result
    }

    fn get_cut(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.ensure_healthy()?;
        let began = Instant::now();
        let reference = self
            .index
            .get(ks, key, cut)
            .map_err(LocalDbError::component)?;
        self.stats.read_resolve_ns += elapsed_ns(began);
        let Some(reference) = reference else {
            return Ok(None);
        };
        match self.resolve(reference)? {
            Mutation::Put {
                keyspace,
                key: actual,
                value,
            } if keyspace == ks && actual.as_ref() == key => Ok(Some(value)),
            Mutation::Put { .. } => Ok(None), // Native reduced-key collision check.
            Mutation::Delete { .. } => Err(LocalDbError::new(
                "index returned a deletion as a live value",
            )),
        }
    }

    pub fn get(&mut self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>, LocalDbError> {
        self.get_cut(ks, key, IndexReadCut::Latest)
    }

    pub fn get_many(
        &mut self,
        ks: KeySpace,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Bytes>>, LocalDbError> {
        if keys.len() > 256 {
            return Err(LocalDbError::new("read batch exceeds 256 keys"));
        }
        keys.iter().map(|key| self.get(ks, key)).collect()
    }

    /// Separate diagnostic pass; its extra clocks are absent from ordinary gets.
    pub fn get_profiled(
        &mut self,
        ks: KeySpace,
        key: &[u8],
    ) -> Result<(Option<Bytes>, LocalReadProfile), LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let reference = self
            .index
            .get(ks, key, IndexReadCut::Latest)
            .map_err(LocalDbError::component)?;
        let mut profile = LocalReadProfile {
            index_ns: elapsed_ns(started),
            ..Default::default()
        };
        let Some(reference) = reference else {
            return Ok((None, profile));
        };
        let log = self
            .logs
            .get(&(
                reference.batch_version.epoch,
                reference.batch_address.partition,
            ))
            .ok_or_else(|| LocalDbError::new("value reference names missing epoch/partition"))?;
        let (mutation, record) = log
            .read_record_profiled(&reference)
            .map_err(LocalDbError::component)?;
        profile.record = record;
        match mutation {
            Mutation::Put {
                keyspace,
                key: actual,
                value,
            } if keyspace == ks && actual.as_ref() == key => Ok((Some(value), profile)),
            Mutation::Put { .. } => Ok((None, profile)),
            Mutation::Delete { .. } => Err(LocalDbError::new(
                "index returned a deletion as a live value",
            )),
        }
    }
    pub fn exists(&mut self, ks: KeySpace, key: &[u8]) -> Result<bool, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let reference = self
            .index
            .get(ks, key, IndexReadCut::Latest)
            .map_err(LocalDbError::component)?;
        self.stats.read_resolve_ns += elapsed_ns(started);
        let Some(reference) = reference else {
            return Ok(false);
        };
        // Preserve native Db::exists: full-key index hits need no value read.
        // Reduced keys still require the actual WAL key to reject collisions.
        if !self.inventory.shape().ks(ks).need_check_index_key() {
            return Ok(true);
        }
        Ok(
            matches!(self.resolve(reference)?, Mutation::Put { keyspace, key: actual, .. }
            if keyspace == ks && actual.as_ref() == key),
        )
    }
    pub fn get_at(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        checkpoint: &LocalCheckpoint,
    ) -> Result<Option<Bytes>, LocalDbError> {
        if checkpoint.database != self.config.storage.database {
            return Err(LocalDbError::new("checkpoint belongs to another database"));
        }
        let cut = checkpoint
            .cut
            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        if checkpoint.pin.as_ref().is_some_and(|pin| pin.cut() != cut) {
            return Err(LocalDbError::new(
                "checkpoint cut differs from its retained pin",
            ));
        }
        self.get_cut(
            ks,
            key,
            checkpoint
                .cut
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
        )
    }
    /// Ordered by the configured native reduced index keys, like native iteration.
    pub fn scan(&mut self, ks: KeySpace) -> Result<Vec<(Bytes, Bytes)>, LocalDbError> {
        self.ensure_healthy()?;
        let mut output = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .index
                .scan(
                    ks,
                    after.as_ref().map(Bytes::as_ref),
                    false,
                    256,
                    IndexReadCut::Latest,
                )
                .map_err(LocalDbError::component)?;
            if page.is_empty() {
                break;
            }
            for (reduced, reference) in page {
                after = Some(reduced);
                match self.resolve(reference)? {
                    Mutation::Put {
                        keyspace,
                        key,
                        value,
                    } if keyspace == ks => output.push((key, value)),
                    _ => {
                        return Err(LocalDbError::new(
                            "index iteration reference is not a live keyspace value",
                        ));
                    }
                }
            }
        }
        Ok(output)
    }

    /// Adopt a durable pageable index root before releasing resident metadata.
    pub fn flush_index(&mut self) -> Result<(), LocalDbError> {
        self.drain()?;
        self.persist_index_root(self.index.retention_floor())
    }

    /// Advance supported history, clamped to every live checkpoint pin, then
    /// retire only sealed files unreferenced by the newly installed root.
    pub fn compact_history(&mut self, floor: IndexReadCut) -> Result<(), LocalDbError> {
        self.drain()?;
        self.persist_index_root(floor)
    }
    pub fn unload_index(&mut self) -> Result<(), LocalDbError> {
        self.flush_index()
    }
    pub fn stats(&self) -> LocalDbStats {
        self.stats
    }

    pub fn worker_stats(&self) -> WorkerStats {
        let mut total = WorkerStats::default();
        for worker in &self.workers {
            let stats = worker.stats();
            total.pending_requests += stats.pending_requests;
            total.pending_bytes += stats.pending_bytes;
            total.groups_persisted += stats.groups_persisted;
            total.batches_persisted += stats.batches_persisted;
            total.coalescing_waits += stats.coalescing_waits;
            total.submitted_windows += stats.submitted_windows;
            total.max_window_batches = total.max_window_batches.max(stats.max_window_batches);
            total.admission_validation_ns += stats.admission_validation_ns;
            total.ownership_ns += stats.ownership_ns;
            total.queue_lock_wait_ns += stats.queue_lock_wait_ns;
            total.queue_wait_ns += stats.queue_wait_ns;
            total.group_build_ns += stats.group_build_ns;
            total.append_ns += stats.append_ns;
            total.persist_ns += stats.persist_ns;
        }
        total
    }
    pub fn storage_stats(&self) -> PartitionStats {
        let mut result = PartitionStats::default();
        for log in self.inventory.active_partitions() {
            let stats = log.stats();
            result.appended_groups += stats.appended_groups;
            result.persistence_barriers += stats.persistence_barriers;
            result.namespace_syncs += stats.namespace_syncs;
            result.inventory_witnesses += stats.inventory_witnesses;
            result.registered_records += stats.registered_records;
            result.reference_metadata_bytes += stats.reference_metadata_bytes;
        }
        result
    }
    pub fn shutdown(&mut self) -> Result<(), LocalDbError> {
        if self.stopped {
            return Ok(());
        }
        let mut result = self.drain();
        for worker in &mut self.workers {
            if let Err(error) = worker.shutdown()
                && result.is_ok()
            {
                result = Err(LocalDbError::component(error));
            }
        }
        for log in self.inventory.active_partitions() {
            if let Err(error) = log.seal_epoch(RequestAuthority {
                epoch: self.inventory.active_inventory().epoch,
            }) && result.is_ok()
            {
                result = Err(LocalDbError::component(error));
            }
        }
        self.stopped = true;
        result
    }
}

impl Drop for LocalDatabase {
    fn drop(&mut self) {
        // Shutdown may complete admitted storage despite a lost caller response.
        // Reopen replays every eligible complete batch, including unpublished ones.
        let _ = self.shutdown();
    }
}

#[derive(Debug)]
pub struct LocalDbError {
    message: String,
    pub uncertain_version: Option<BatchVersion>,
}
impl LocalDbError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            uncertain_version: None,
        }
    }
    pub(crate) fn component(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }
    pub(crate) fn unknown(version: BatchVersion, error: impl fmt::Display) -> Self {
        Self {
            message: format!("outcome requires recovery for {version:?}: {error}"),
            uncertain_version: Some(version),
        }
    }
}
impl fmt::Display for LocalDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for LocalDbError {}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests;
