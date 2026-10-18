//! Bounded local caller grouping for the distributed Value WAL.
//!
//! NEXT_STEPS M2 and `group_model` require one complete, validated group to pass
//! its persistence barrier before another group can be emitted. Tidehunter's
//! paper §§3.1/3.3 preserves whole-batch replay; this worker does not publish the
//! native index or establish a remote fencing lease.
//!
//! TiKV Raft Engine's write barrier motivates immediate service for an isolated
//! caller and grouping callers that arrive behind active I/O:
//! https://github.com/tikv/raft-engine/blob/master/src/write_barrier.rs
//! This implementation uses owned bounded jobs and a joined worker instead of
//! that implementation's unsafe intrusive caller list. Do not interleave direct
//! append/persist calls with this scheduler; storage still fails closed if they
//! conflict. The scheduler claim prevents duplicate workers, not remote writers.

use super::codec::{CodecError, CompleteBatch};
use super::group::{CompleteGroup, GROUP_HEADER_LEN, GroupError};
use super::reference_directory::ReferenceDirectory;
use super::storage::{DurableBatch, PartitionLog, StorageError};
use super::types::{BatchId, BatchVersion, PayloadDigest, RequestAuthority};
use crate::key_shape::KeyShape;
use parking_lot::{Condvar, Mutex};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct WorkerConfig {
    /// Includes the active group and queued requests, including duplicate retries.
    pub max_pending_requests: usize,
    /// Encoded payloads, mutation/descriptors and bounded queue metadata; active
    /// requests retain credits until completion. The active group additionally
    /// allocates at most the partition's group limit while encoding its container.
    pub max_pending_bytes: usize,
    /// Coalescing delay only when a backlog already has at least two requests.
    /// Isolated callers proceed immediately. Must be at most 10 milliseconds.
    pub max_group_wait: Duration,
}

#[derive(Clone, Debug)]
pub enum WorkerError {
    InvalidConfig(&'static str),
    /// Rejected before admission; no storage effects from this submission.
    AdmissionFull,
    /// No append was attempted for this submission.
    StoppedBeforeEffects,
    BatchRejected(Arc<CodecError>),
    GroupRejected(Arc<GroupError>),
    StorageRejected(Arc<StorageError>),
    /// Storage may contain this identity. Recovery must resolve the original ID.
    OutcomeUnknown(Arc<StorageError>),
    /// The worker stopped without delivering a result for an active request.
    /// This is not permission to retry using a different identity.
    WorkerLostOutcome,
    ThreadStart(Arc<std::io::Error>),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "distributed persistence worker: {self:?}")
    }
}
impl std::error::Error for WorkerError {}

pub struct PendingBatch {
    result: mpsc::Receiver<Result<DurableBatch, WorkerError>>,
}
impl PendingBatch {
    /// Dropping this handle does not cancel an admitted write.
    pub fn wait(self) -> Result<DurableBatch, WorkerError> {
        self.result
            .recv()
            .unwrap_or(Err(WorkerError::WorkerLostOutcome))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerStats {
    pub pending_requests: usize,
    pub pending_bytes: usize,
    pub groups_persisted: u64,
    pub batches_persisted: u64,
    pub coalescing_waits: u64,
    pub submitted_windows: u64,
    pub max_window_batches: usize,
    /// Sum of successful submission validation, ownership and queue-lock stages.
    pub admission_validation_ns: u64,
    pub ownership_ns: u64,
    pub queue_lock_wait_ns: u64,
    /// Sum of per-request time from enqueue to extraction; not elapsed wall time.
    pub queue_wait_ns: u64,
    pub group_build_ns: u64,
    pub append_ns: u64,
    pub persist_ns: u64,
}

struct Job {
    // Move the validated canonical object into its group without rehashing. None
    // while the group's storage work owns it; credit still includes all metadata.
    batch: Option<CompleteBatch>,
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    group_bytes: usize,
    credit: usize,
    enqueued: Instant,
    window_ready: bool,
    result: mpsc::SyncSender<Result<DurableBatch, WorkerError>>,
}
struct Queue {
    jobs: VecDeque<Job>,
    stopped: bool,
    terminal_error: Option<WorkerError>,
    stats: WorkerStats,
}
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    config: WorkerConfig,
    log: Arc<PartitionLog>,
    shape: KeyShape,
    #[cfg(test)]
    before_process: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    panic_after_append: std::sync::atomic::AtomicBool,
}

/// One explicitly owned persistence worker for a partition. `Drop` drains and
/// joins, so no worker retaining the storage directory outlives this handle.
pub struct GroupCommitWorker {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}
impl GroupCommitWorker {
    pub fn start(
        log: Arc<PartitionLog>,
        shape: &KeyShape,
        config: WorkerConfig,
    ) -> Result<Self, WorkerError> {
        if config.max_pending_requests == 0
            || config.max_pending_requests > 1_000_000
            || config.max_pending_bytes == 0
            || config.max_group_wait > Duration::from_millis(10)
        {
            return Err(WorkerError::InvalidConfig(
                "invalid queue capacity or coalescing delay",
            ));
        }
        let shape = shape
            .to_yaml()
            .and_then(|yaml| KeyShape::from_yaml(&yaml))
            .map_err(|_| WorkerError::InvalidConfig("invalid key shape"))?;
        log.claim_worker()
            .map_err(|error| WorkerError::StorageRejected(Arc::new(error)))?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                jobs: VecDeque::new(),
                stopped: false,
                terminal_error: None,
                stats: WorkerStats::default(),
            }),
            ready: Condvar::new(),
            config,
            log: log.clone(),
            shape,
            #[cfg(test)]
            before_process: Mutex::new(None),
            #[cfg(test)]
            panic_after_append: std::sync::atomic::AtomicBool::new(false),
        });
        let thread_shared = shared.clone();
        let thread = match thread::Builder::new()
            .name(format!("value-wal-{}", log.config().partition))
            .spawn(move || {
                let _claim = Claim(thread_shared.log.clone());
                // A panic drops active senders (unknown outcomes) and drains all
                // untouched queued jobs with an explicit before-effects result.
                let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&thread_shared, &thread_shared.shape)
                }))
                .is_err();
                if panicked {
                    thread_shared.log.poison_writer();
                    thread_shared.queue.lock().terminal_error =
                        Some(WorkerError::WorkerLostOutcome);
                }
                stop_pending(&thread_shared);
                if panicked {
                    // Unwinding dropped active jobs and their buffers/senders.
                    // Their outcomes are unknown; their credits are no longer held.
                    let mut queue = thread_shared.queue.lock();
                    queue.stats.pending_requests = 0;
                    queue.stats.pending_bytes = 0;
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                log.release_worker();
                return Err(WorkerError::ThreadStart(Arc::new(error)));
            }
        };
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    /// Nonblocking admission. Limits count both waiting and currently active
    /// requests; no unbounded internal queue of payload-owning blocked callers.
    pub fn try_submit(&self, batch: CompleteBatch) -> Result<PendingBatch, WorkerError> {
        Ok(self
            .submit_window(vec![batch], false)?
            .pop()
            .expect("one submission"))
    }

    /// Atomically admit a complete submitted window in input order, then wake
    /// once. No member is queued if validation or aggregate admission fails.
    /// The worker splits at its existing group limits without splitting a batch.
    /// An empty window is a no-op. A window does not create a coalescing delay.
    pub fn try_submit_many(
        &self,
        batches: Vec<CompleteBatch>,
    ) -> Result<Vec<PendingBatch>, WorkerError> {
        self.submit_window(batches, true)
    }

    fn submit_window(
        &self,
        batches: Vec<CompleteBatch>,
        window_ready: bool,
    ) -> Result<Vec<PendingBatch>, WorkerError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        if batches.len() > self.shared.config.max_pending_requests {
            return Err(WorkerError::AdmissionFull);
        }
        let started = Instant::now();
        let mut sizes = Vec::with_capacity(batches.len());
        let mut total_credit = 0usize;
        for batch in &batches {
            let (group_bytes, credit) = self.submission_sizes(batch)?;
            total_credit = total_credit
                .checked_add(credit)
                .ok_or(WorkerError::AdmissionFull)?;
            sizes.push((group_bytes, credit));
        }
        let validation_ns = elapsed_ns(started);
        let started = Instant::now();
        let mut queue = self.shared.queue.lock();
        let lock_wait_ns = elapsed_ns(started);
        self.check_totals(&queue, batches.len(), total_credit)?;
        let started = Instant::now();
        let mut jobs = Vec::with_capacity(batches.len());
        let mut pending = Vec::with_capacity(batches.len());
        for (batch, (group_bytes, credit)) in batches.into_iter().zip(sizes) {
            let batch = batch.into_canonical_owned();
            let (result, receiver) = mpsc::sync_channel(1);
            jobs.push(Job {
                id: batch.id(),
                version: batch.version(),
                digest: batch.digest(),
                batch: Some(batch),
                group_bytes,
                credit,
                enqueued: started,
                window_ready,
                result,
            });
            pending.push(PendingBatch { result: receiver });
        }
        queue.jobs.reserve(jobs.len());
        let ownership_ns = elapsed_ns(started);
        let enqueued = Instant::now();
        for job in &mut jobs {
            job.enqueued = enqueued;
        }
        let count = jobs.len();
        queue.jobs.extend(jobs);
        queue.stats.pending_requests += count;
        queue.stats.pending_bytes += total_credit;
        queue.stats.submitted_windows += 1;
        queue.stats.max_window_batches = queue.stats.max_window_batches.max(count);
        queue.stats.admission_validation_ns += validation_ns;
        queue.stats.ownership_ns += ownership_ns;
        queue.stats.queue_lock_wait_ns += lock_wait_ns;
        self.shared.ready.notify_one();
        Ok(pending)
    }

    /// Owner-only preflight including its admitted-but-not-yet-dispatched window.
    /// Returns this batch's byte reservation; it does not reserve worker credits.
    /// A sole owner calls this before logical allocation and still fails closed
    /// if shutdown or another submitter races its subsequent dispatch.
    pub(crate) fn check_admission_reserved(
        &self,
        batch: &CompleteBatch,
        reserved_requests: usize,
        reserved_bytes: usize,
    ) -> Result<usize, WorkerError> {
        let (_, credit) = self.submission_sizes(batch)?;
        let queue = self.shared.queue.lock();
        self.check_totals(
            &queue,
            reserved_requests
                .checked_add(1)
                .ok_or(WorkerError::AdmissionFull)?,
            reserved_bytes
                .checked_add(credit)
                .ok_or(WorkerError::AdmissionFull)?,
        )?;
        Ok(credit)
    }

    fn check_totals(
        &self,
        queue: &Queue,
        requests: usize,
        bytes: usize,
    ) -> Result<(), WorkerError> {
        if queue.stopped {
            return Err(WorkerError::StoppedBeforeEffects);
        }
        if queue
            .stats
            .pending_requests
            .checked_add(requests)
            .is_none_or(|n| n > self.shared.config.max_pending_requests)
            || queue
                .stats
                .pending_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > self.shared.config.max_pending_bytes)
        {
            return Err(WorkerError::AdmissionFull);
        }
        Ok(())
    }

    fn submission_sizes(&self, batch: &CompleteBatch) -> Result<(usize, usize), WorkerError> {
        let config = self.shared.log.config();
        batch
            .validate_for(&self.shared.shape, config.batch_limits)
            .map_err(|error| WorkerError::BatchRejected(Arc::new(error)))?;
        let group_bytes = CompleteGroup::encoded_member_len(batch.encoded().len())
            .map_err(|e| WorkerError::GroupRejected(Arc::new(e)))?;
        if batch.encoded().len() > config.batch_limits.max_encoded_bytes
            || batch.mutations().len() > config.batch_limits.max_operations as usize
            || group_bytes
                .checked_add(GROUP_HEADER_LEN)
                .is_none_or(|n| n > config.group_limits.max_encoded_bytes)
        {
            return Err(WorkerError::GroupRejected(Arc::new(GroupError::TooLarge)));
        }
        ReferenceDirectory::check_fresh_total_capacity(
            config.max_batches.min(1024),
            config.max_reference_metadata_bytes,
            1,
            batch.mutations().len(),
        )
        .map_err(|error| WorkerError::StorageRejected(Arc::new(StorageError::from(error))))?;
        if batch.id().database != config.inventory.database {
            return Err(WorkerError::StorageRejected(Arc::new(
                StorageError::WrongDatabase,
            )));
        }
        if batch.version().epoch != config.inventory.epoch {
            return Err(WorkerError::StorageRejected(Arc::new(StorageError::Fenced)));
        }
        if config
            .inventory
            .partition_for(batch.id())
            .map_err(|e| WorkerError::StorageRejected(Arc::new(e)))?
            != config.partition
        {
            return Err(WorkerError::StorageRejected(Arc::new(
                StorageError::WrongPartition,
            )));
        }
        // Canonicalization drops arbitrary input owners and oversized vectors.
        // Include the typed mutation/descriptor metadata retained during I/O.
        let credit = batch
            .canonical_heap_bytes()
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Job>()))
            .ok_or(WorkerError::AdmissionFull)?;
        Ok((group_bytes, credit))
    }

    pub fn commit(&self, batch: CompleteBatch) -> Result<DurableBatch, WorkerError> {
        self.try_submit(batch)?.wait()
    }
    pub fn stats(&self) -> WorkerStats {
        self.shared.queue.lock().stats
    }
    pub fn shutdown(&mut self) -> Result<(), WorkerError> {
        self.shared.queue.lock().stopped = true;
        self.shared.ready.notify_all();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            self.shared.log.poison_writer();
            self.shared.queue.lock().terminal_error = Some(WorkerError::WorkerLostOutcome);
        }
        match self.shared.queue.lock().terminal_error.clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
impl Drop for GroupCommitWorker {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
struct Claim(Arc<PartitionLog>);
impl Drop for Claim {
    fn drop(&mut self) {
        self.0.release_worker();
    }
}

fn run(shared: &Shared, shape: &KeyShape) {
    loop {
        let jobs = {
            let mut queue = shared.queue.lock();
            while queue.jobs.is_empty() && !queue.stopped {
                shared.ready.wait(&mut queue);
            }
            if queue.jobs.is_empty() {
                return;
            }
            let config = shared.log.config();
            let limits = config.group_limits;
            let max_batches = (limits.max_batches as usize).min(config.max_batches.min(1024));
            if queue.jobs.len() >= 2
                && !queue.stopped
                && !shared.config.max_group_wait.is_zero()
                && !queue
                    .jobs
                    .iter()
                    .take(max_batches)
                    .any(|job| job.window_ready)
            {
                let deadline = Instant::now() + shared.config.max_group_wait;
                loop {
                    let count = queue.jobs.len().min(max_batches);
                    let (bytes, descriptors) = queue.jobs.iter().take(count).fold(
                        (0usize, 0usize),
                        |(bytes, descriptors), job| {
                            (
                                bytes.saturating_add(job.group_bytes),
                                descriptors.saturating_add(
                                    job.batch
                                        .as_ref()
                                        .expect("queued canonical batch")
                                        .mutations()
                                        .len(),
                                ),
                            )
                        },
                    );
                    if count == max_batches
                        || ReferenceDirectory::check_fresh_total_capacity(
                            max_batches,
                            config.max_reference_metadata_bytes,
                            count,
                            descriptors,
                        )
                        .is_err()
                        || bytes.saturating_add(GROUP_HEADER_LEN) >= limits.max_encoded_bytes
                        || queue.stopped
                        || queue.jobs.iter().take(count).any(|job| job.window_ready)
                    {
                        break;
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    queue.stats.coalescing_waits += 1;
                    shared.ready.wait_for(&mut queue, deadline - now);
                }
            }
            let mut jobs = Vec::new();
            let mut bytes = GROUP_HEADER_LEN;
            let mut descriptors = 0usize;
            while jobs.len() < max_batches {
                let Some(front) = queue.jobs.front() else {
                    break;
                };
                let Some(next_bytes) = bytes.checked_add(front.group_bytes) else {
                    break;
                };
                let next_descriptors = descriptors.saturating_add(
                    front
                        .batch
                        .as_ref()
                        .expect("queued canonical batch")
                        .mutations()
                        .len(),
                );
                if next_bytes > limits.max_encoded_bytes
                    || ReferenceDirectory::check_fresh_total_capacity(
                        max_batches,
                        config.max_reference_metadata_bytes,
                        jobs.len() + 1,
                        next_descriptors,
                    )
                    .is_err()
                {
                    break;
                }
                descriptors = next_descriptors;
                bytes = next_bytes;
                let job = queue.jobs.pop_front().unwrap();
                queue.stats.queue_wait_ns += elapsed_ns(job.enqueued);
                jobs.push(job);
            }
            jobs
        };
        #[cfg(test)]
        if let Some((entered, resume)) = shared.before_process.lock().take() {
            let _ = entered.send(());
            let _ = resume.recv();
        }
        let (results, fatal) = process(shared, shape, jobs);
        if fatal {
            shared.queue.lock().terminal_error = results
                .iter()
                .find_map(|(_, result)| match result {
                    Err(WorkerError::OutcomeUnknown(error)) => {
                        Some(WorkerError::OutcomeUnknown(error.clone()))
                    }
                    Err(WorkerError::StorageRejected(error))
                        if matches!(**error, StorageError::Poisoned) =>
                    {
                        Some(WorkerError::StorageRejected(error.clone()))
                    }
                    _ => None,
                })
                .or(Some(WorkerError::WorkerLostOutcome));
        }
        for (job, result) in results {
            finish(shared, job, result);
        }
        if fatal {
            return;
        }
    }
}

fn finish(shared: &Shared, job: Job, outcome: Result<DurableBatch, WorkerError>) {
    let Job {
        batch,
        credit,
        result,
        ..
    } = job;
    drop(batch);
    {
        let mut queue = shared.queue.lock();
        queue.stats.pending_requests -= 1;
        queue.stats.pending_bytes -= credit;
    }
    // Capacity one means completion never waits for the caller to receive.
    let _ = result.send(outcome);
}
fn stop_pending(shared: &Shared) {
    let jobs = {
        let mut queue = shared.queue.lock();
        queue.stopped = true;
        queue.jobs.drain(..).collect::<Vec<_>>()
    };
    for job in jobs {
        finish(shared, job, Err(WorkerError::StoppedBeforeEffects));
    }
    shared.ready.notify_all();
}
fn storage_error(error: StorageError, may_have_effects: bool) -> (WorkerError, bool) {
    let fatal = matches!(error, StorageError::Unknown(_) | StorageError::Poisoned);
    let unknown = matches!(error, StorageError::Unknown(_))
        || (may_have_effects && matches!(error, StorageError::Poisoned));
    let error = Arc::new(error);
    if unknown {
        (WorkerError::OutcomeUnknown(error), true)
    } else {
        (WorkerError::StorageRejected(error), fatal)
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

type JobResults = Vec<(Job, Result<DurableBatch, WorkerError>)>;
fn process(shared: &Shared, shape: &KeyShape, jobs: Vec<Job>) -> (JobResults, bool) {
    let config = shared.log.config();
    let authority = RequestAuthority {
        epoch: config.inventory.epoch,
    };
    let mut results = Vec::new();
    let mut batches = Vec::new();
    let mut listeners: Vec<Vec<Job>> = Vec::new();
    let mut identities: BTreeMap<BatchId, usize> = BTreeMap::new();
    let mut versions = BTreeSet::new();
    let mut fatal = false;
    let mut retry_persist_ns = 0u64;
    for mut job in jobs {
        if fatal {
            results.push((job, Err(WorkerError::StoppedBeforeEffects)));
            continue;
        }
        let old = match shared.log.lookup(job.id) {
            Ok(old) => old,
            Err(error) => {
                // A failed authority lookup cannot prove this is a new ID.
                // Stop rather than append a possible duplicate occurrence.
                shared.log.poison_writer();
                fatal = true;
                results.push((job, Err(WorkerError::StorageRejected(Arc::new(error)))));
                continue;
            }
        };
        if let Some(old) = old {
            if old.version() != job.version || old.digest() != job.digest {
                results.push((
                    job,
                    Err(WorkerError::StorageRejected(Arc::new(
                        StorageError::IdentityConflict,
                    ))),
                ));
            } else {
                let started = Instant::now();
                let outcome = shared.log.persist_through(old).map_err(|error| {
                    let (error, stop) = storage_error(error, true);
                    fatal = stop;
                    error
                });
                retry_persist_ns += elapsed_ns(started);
                results.push((job, outcome));
            }
            continue;
        }
        let old_version = match shared.log.lookup_version(job.version) {
            Ok(old) => old,
            Err(error) => {
                shared.log.poison_writer();
                fatal = true;
                results.push((job, Err(WorkerError::StorageRejected(Arc::new(error)))));
                continue;
            }
        };
        if old_version.is_some() {
            results.push((
                job,
                Err(WorkerError::StorageRejected(Arc::new(
                    StorageError::VersionConflict,
                ))),
            ));
            continue;
        }
        if let Some(&index) = identities.get(&job.id) {
            let batch: &CompleteBatch = &batches[index];
            if batch.version() == job.version && batch.digest() == job.digest {
                listeners[index].push(job);
            } else {
                results.push((
                    job,
                    Err(WorkerError::StorageRejected(Arc::new(
                        StorageError::IdentityConflict,
                    ))),
                ));
            }
            continue;
        }
        if versions.contains(&job.version) {
            results.push((
                job,
                Err(WorkerError::StorageRejected(Arc::new(
                    StorageError::VersionConflict,
                ))),
            ));
            continue;
        }
        // Only immutable constructor/decode results can enter this owned job.
        // Schema was checked at admission; CompleteGroup rechecks before emit.
        // External bytes still require full decode/integrity validation first.
        let batch = job.batch.take().expect("queued canonical batch");
        versions.insert(job.version);
        identities.insert(job.id, batches.len());
        batches.push(batch);
        listeners.push(vec![job]);
    }
    if retry_persist_ns != 0 {
        shared.queue.lock().stats.persist_ns += retry_persist_ns;
    }
    if batches.is_empty() {
        return (results, fatal);
    }
    if fatal {
        for job in listeners.into_iter().flatten() {
            results.push((job, Err(WorkerError::StoppedBeforeEffects)));
        }
        return (results, true);
    }
    let count = batches.len();
    let started = Instant::now();
    let group = CompleteGroup::new(shape, batches, config.batch_limits, config.group_limits);
    let group_build_ns = elapsed_ns(started);
    let mut append_ns = 0;
    let mut persist_ns = 0;
    let outcome = group
        .map_err(|e| WorkerError::GroupRejected(Arc::new(e)))
        .and_then(|group| {
            let started = Instant::now();
            let appended = shared.log.append_group(authority, &group).map_err(|e| {
                let (error, stop) = storage_error(e, false);
                fatal = stop;
                error
            });
            append_ns = elapsed_ns(started);
            appended
        })
        .and_then(|receipts| {
            #[cfg(test)]
            if shared
                .panic_after_append
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                panic!("injected panic after group append before persistence");
            }
            // Exact receipts share one complete group's persistence proof.
            // Validate them under one lock instead of reacquiring per member.
            let started = Instant::now();
            let durable = shared.log.persist_many(&receipts).map_err(|e| {
                let (error, stop) = storage_error(e, true);
                fatal = stop;
                error
            });
            persist_ns += elapsed_ns(started);
            durable
        });
    {
        let mut queue = shared.queue.lock();
        queue.stats.group_build_ns += group_build_ns;
        queue.stats.append_ns += append_ns;
        queue.stats.persist_ns += persist_ns;
    }
    match outcome {
        Ok(receipts) => {
            {
                let mut queue = shared.queue.lock();
                queue.stats.groups_persisted += 1;
                queue.stats.batches_persisted += count as u64;
            }
            for (jobs, receipt) in listeners.into_iter().zip(receipts) {
                for job in jobs {
                    results.push((job, Ok(receipt)));
                }
            }
        }
        Err(error) => {
            for job in listeners.into_iter().flatten() {
                results.push((job, Err(error.clone())));
            }
        }
    }
    (results, fatal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::{BatchLimits, Mutation};
    use crate::distributed::group::GroupLimits;
    use crate::distributed::storage::{PartitionConfig, StaticInventory};
    use crate::key_shape::{KeySpace, KeyType};
    use crate::metrics::Metrics;

    fn shape() -> KeyShape {
        KeyShape::new_single(4, 1, KeyType::uniform(1))
    }
    fn config() -> PartitionConfig {
        PartitionConfig {
            inventory: StaticInventory {
                database: [1; 16],
                epoch: 5,
                segment_generation: 1,
                partitions: vec![3],
            },
            partition: 3,
            batch_limits: BatchLimits {
                max_operations: 16,
                max_encoded_bytes: 1024,
            },
            group_limits: GroupLimits {
                max_batches: 8,
                max_encoded_bytes: 4096,
            },
            fragment_bytes: 8192,
            wal_file_bytes: 16384,
            max_batches: 100,
            max_reference_metadata_bytes: 1024 * 1024,
        }
    }
    fn worker_config() -> WorkerConfig {
        WorkerConfig {
            max_pending_requests: 32,
            max_pending_bytes: 64 * 1024,
            max_group_wait: Duration::from_micros(100),
        }
    }
    fn batch(id: u8, sequence: u64) -> CompleteBatch {
        CompleteBatch::new(
            &shape(),
            BatchId {
                database: [1; 16],
                operation: [id; 16],
            },
            BatchVersion { epoch: 5, sequence },
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![id; 32].into(),
            }],
            config().batch_limits,
        )
        .unwrap()
    }
    fn fixture() -> (tempdir::TempDir, Arc<PartitionLog>) {
        let dir = tempdir::TempDir::new("group-worker").unwrap();
        let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
        let log = Arc::new(
            PartitionLog::create(&dir.path().join("stream"), config(), &shape(), metrics).unwrap(),
        );
        (dir, log)
    }
    struct Gate {
        entered: mpsc::Receiver<()>,
        release: Option<mpsc::SyncSender<()>>,
    }
    impl Gate {
        fn install(worker: &GroupCommitWorker) -> Self {
            let (entered_tx, entered) = mpsc::sync_channel(1);
            let (release, receiver) = mpsc::sync_channel(1);
            *worker.shared.before_process.lock() = Some((entered_tx, receiver));
            Self {
                entered,
                release: Some(release),
            }
        }
        fn entered(&self) {
            self.entered.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        fn release(mut self) {
            let _ = self.release.take().unwrap().send(());
        }
    }
    impl Drop for Gate {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.try_send(());
            }
        }
    }

    #[test]
    fn submitted_window_is_atomic_against_schema_and_aggregate_admission() {
        let (_dir, log) = fixture();
        let mut config = worker_config();
        config.max_pending_requests = 3;
        config.max_group_wait = Duration::from_millis(10);
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), config).unwrap();
        let gate = Gate::install(&worker);
        let first = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let foreign = CompleteBatch::new(
            &KeyShape::new_single(8, 1, KeyType::uniform(1)),
            batch(9, 9).id(),
            batch(9, 9).version(),
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: b"long-key"[..].into(),
                value: b"value"[..].into(),
            }],
            self::config().batch_limits,
        )
        .unwrap();
        assert!(matches!(
            worker.try_submit_many(vec![batch(1, 1), foreign]),
            Err(WorkerError::BatchRejected(_))
        ));
        assert!(matches!(
            worker.try_submit_many(vec![batch(1, 1), batch(2, 2), batch(3, 3)]),
            Err(WorkerError::AdmissionFull)
        ));
        assert_eq!(worker.stats().pending_requests, 1);
        assert!(worker.shared.queue.lock().jobs.is_empty());
        let credit = worker.check_admission_reserved(&batch(1, 1), 0, 0).unwrap();
        assert!(
            worker
                .check_admission_reserved(&batch(2, 2), 1, credit)
                .is_ok()
        );
        assert!(matches!(
            worker.check_admission_reserved(&batch(3, 3), 2, credit * 2),
            Err(WorkerError::AdmissionFull)
        ));
        let pending = worker
            .try_submit_many(vec![batch(1, 1), batch(2, 2)])
            .unwrap();
        assert_eq!(worker.stats().pending_requests, 3);
        gate.release();
        first.wait().unwrap();
        for pending in pending {
            pending.wait().unwrap();
        }
        worker.shutdown().unwrap();
        let stats = worker.stats();
        assert_eq!(stats.coalescing_waits, 0);
        assert_eq!((stats.groups_persisted, stats.batches_persisted), (2, 3));
        assert_eq!((stats.submitted_windows, stats.max_window_batches), (2, 2));
        assert_eq!((stats.pending_requests, stats.pending_bytes), (0, 0));
        assert!(log.lookup(batch(9, 9).id()).unwrap().is_none());
    }

    #[test]
    fn submitted_window_splits_at_group_limit_without_premature_wake_or_delay() {
        let (_dir, log) = fixture();
        let mut config = worker_config();
        config.max_group_wait = Duration::from_millis(10);
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), config).unwrap();
        let gate = Gate::install(&worker);
        let pending = worker
            .try_submit_many((0..10).map(|n| batch(n, n as u64)).collect())
            .unwrap();
        gate.entered();
        // The worker sees the complete window even when it was idle at enqueue.
        // Eight form its first group; two remain queued with their credits held.
        assert_eq!(worker.stats().pending_requests, 10);
        assert_eq!(worker.shared.queue.lock().jobs.len(), 2);
        gate.release();
        for (n, pending) in pending.into_iter().enumerate() {
            assert_eq!(
                pending.wait().unwrap().appended().id(),
                batch(n as u8, n as u64).id()
            );
        }
        worker.shutdown().unwrap();
        let stats = worker.stats();
        assert_eq!((stats.groups_persisted, stats.batches_persisted), (2, 10));
        assert_eq!((stats.submitted_windows, stats.max_window_batches), (1, 10));
        assert_eq!(stats.coalescing_waits, 0);
        assert!(
            stats.admission_validation_ns > 0
                && stats.group_build_ns > 0
                && stats.append_ns > 0
                && stats.persist_ns > 0
        );
        assert_eq!(log.stats().persistence_barriers, 2);
    }

    #[test]
    fn submitted_window_retries_coalesce_and_uncertain_group_stops_queued_tail() {
        let (_dir, log) = fixture();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        let mut pending = worker
            .try_submit_many(vec![
                batch(1, 1),
                batch(1, 1),
                batch(1, 2),
                batch(2, 1),
                batch(3, 3),
            ])
            .unwrap()
            .into_iter();
        assert_eq!(
            pending.next().unwrap().wait().unwrap(),
            pending.next().unwrap().wait().unwrap()
        );
        assert!(
            matches!(pending.next().unwrap().wait(), Err(WorkerError::StorageRejected(e)) if matches!(*e, StorageError::IdentityConflict))
        );
        assert!(
            matches!(pending.next().unwrap().wait(), Err(WorkerError::StorageRejected(e)) if matches!(*e, StorageError::VersionConflict))
        );
        pending.next().unwrap().wait().unwrap();
        assert_eq!(log.stats().persistence_barriers, 1);
        log.fail_next_persist();
        let pending = worker
            .try_submit_many((10..20).map(|n| batch(n, n as u64)).collect())
            .unwrap();
        for (n, pending) in pending.into_iter().enumerate() {
            if n < 8 {
                assert!(matches!(
                    pending.wait(),
                    Err(WorkerError::OutcomeUnknown(_))
                ));
            } else {
                assert!(matches!(
                    pending.wait(),
                    Err(WorkerError::StoppedBeforeEffects)
                ));
            }
        }
        assert!(matches!(
            worker.shutdown(),
            Err(WorkerError::OutcomeUnknown(_))
        ));
        assert_eq!(
            (
                worker.stats().pending_requests,
                worker.stats().pending_bytes
            ),
            (0, 0)
        );
        assert!(log.lookup(batch(17, 17).id()).unwrap().is_some());
        assert!(log.lookup(batch(18, 18).id()).unwrap().is_none());
    }

    #[test]
    fn backlog_groups_amortize_barriers_without_sleeping_isolated_caller() {
        let (_dir, log) = fixture();
        let mut config = worker_config();
        config.max_group_wait = Duration::from_millis(10);
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), config).unwrap();
        let gate = Gate::install(&worker);
        let first = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        assert_eq!(worker.stats().coalescing_waits, 0);
        let rest: Vec<_> = (1..=8)
            .map(|n| worker.try_submit(batch(n, n as u64)).unwrap())
            .collect();
        assert_eq!(worker.stats().pending_requests, 9);
        gate.release();
        first.wait().unwrap();
        for result in rest {
            result.wait().unwrap();
        }
        worker.shutdown().unwrap();
        let stats = worker.stats();
        assert_eq!((stats.groups_persisted, stats.batches_persisted), (2, 9));
        assert_eq!((stats.pending_requests, stats.pending_bytes), (0, 0));
        assert_eq!(log.receipt_count(), 9);
        let storage = log.stats();
        assert_eq!(storage.appended_groups, 2);
        assert_eq!(storage.persistence_barriers, 2);
    }

    #[test]
    fn count_and_byte_admission_include_active_group() {
        let (_dir, log) = fixture();
        let mut limits = worker_config();
        limits.max_pending_requests = 2;
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), limits).unwrap();
        let gate = Gate::install(&worker);
        let first = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let second = worker.try_submit(batch(1, 1)).unwrap();
        assert!(matches!(
            worker.try_submit(batch(2, 2)),
            Err(WorkerError::AdmissionFull)
        ));
        assert!(log.lookup(batch(2, 2).id()).unwrap().is_none());
        gate.release();
        first.wait().unwrap();
        second.wait().unwrap();
        worker.shutdown().unwrap();
        let mut limits = worker_config();
        limits.max_pending_bytes =
            batch(3, 3).canonical_heap_bytes().unwrap() + std::mem::size_of::<Job>();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), limits).unwrap();
        assert!(matches!(
            worker.try_submit_many(vec![batch(3, 3), batch(4, 4)]),
            Err(WorkerError::AdmissionFull)
        ));
        assert_eq!(worker.stats().pending_requests, 0);
        assert!(log.lookup(batch(3, 3).id()).unwrap().is_none());
        let gate = Gate::install(&worker);
        let first = worker.try_submit(batch(3, 3)).unwrap();
        gate.entered();
        assert!(matches!(
            worker.try_submit(batch(4, 4)),
            Err(WorkerError::AdmissionFull)
        ));
        gate.release();
        first.wait().unwrap();
        worker.shutdown().unwrap();
    }

    #[test]
    fn retries_coalesce_conflicts_reject_individually_and_other_writes_succeed() {
        let (_dir, log) = fixture();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        let gate = Gate::install(&worker);
        let first = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let a = worker.try_submit(batch(1, 1)).unwrap();
        let duplicate = worker.try_submit(batch(1, 1)).unwrap();
        let identity_conflict = worker.try_submit(batch(1, 2)).unwrap();
        let version_conflict = worker.try_submit(batch(2, 1)).unwrap();
        let valid = worker.try_submit(batch(3, 3)).unwrap();
        gate.release();
        first.wait().unwrap();
        assert_eq!(a.wait().unwrap(), duplicate.wait().unwrap());
        assert!(
            matches!(identity_conflict.wait(), Err(WorkerError::StorageRejected(e)) if matches!(*e, StorageError::IdentityConflict))
        );
        assert!(
            matches!(version_conflict.wait(), Err(WorkerError::StorageRejected(e)) if matches!(*e, StorageError::VersionConflict))
        );
        let durable = valid.wait().unwrap();
        assert_eq!(worker.commit(batch(3, 3)).unwrap(), durable);
        worker.shutdown().unwrap();
        assert_eq!(log.receipt_count(), 3);
    }

    #[test]
    fn shutdown_drains_accepted_and_dropped_handles_then_releases_worker_claim() {
        let (_dir, log) = fixture();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        assert!(matches!(
            GroupCommitWorker::start(log.clone(), &shape(), worker_config()),
            Err(WorkerError::StorageRejected(_))
        ));
        let jobs: Vec<_> = (0..16)
            .map(|n| worker.try_submit(batch(n, n as u64)).unwrap())
            .collect();
        drop(jobs); // Response receivers are not required for draining persistence.
        worker.shutdown().unwrap();
        assert_eq!(log.receipt_count(), 16);
        assert!(matches!(
            worker.try_submit(batch(20, 20)),
            Err(WorkerError::StoppedBeforeEffects)
        ));
        let mut replacement = GroupCommitWorker::start(log, &shape(), worker_config()).unwrap();
        replacement.commit(batch(21, 21)).unwrap();
        replacement.shutdown().unwrap();
    }

    #[test]
    fn sealed_before_processing_rejects_admitted_requests_without_effects_or_deadlock() {
        let (_dir, log) = fixture();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        let gate = Gate::install(&worker);
        let first = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let second = worker.try_submit(batch(1, 1)).unwrap();
        log.seal_epoch(RequestAuthority { epoch: 5 }).unwrap();
        gate.release();
        for job in [first, second] {
            assert!(
                matches!(job.wait(), Err(WorkerError::StorageRejected(e)) if matches!(*e, StorageError::Sealed))
            );
        }
        worker.shutdown().unwrap();
        assert_eq!(log.receipt_count(), 0);
        assert_eq!(worker.stats().pending_requests, 0);
    }

    #[test]
    fn panic_after_append_marks_active_unknown_drains_untouched_and_requires_recovery() {
        let (dir, log) = fixture();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        worker
            .shared
            .panic_after_append
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let gate = Gate::install(&worker);
        let active = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let untouched = worker.try_submit(batch(1, 1)).unwrap();
        gate.release();
        assert!(matches!(active.wait(), Err(WorkerError::WorkerLostOutcome)));
        assert!(matches!(
            untouched.wait(),
            Err(WorkerError::StoppedBeforeEffects)
        ));
        assert!(matches!(
            worker.shutdown(),
            Err(WorkerError::WorkerLostOutcome)
        ));
        assert_eq!(
            (
                worker.stats().pending_requests,
                worker.stats().pending_bytes
            ),
            (0, 0)
        );
        assert!(log.lookup(batch(0, 0).id()).unwrap().is_some());
        assert!(log.lookup(batch(1, 1).id()).unwrap().is_none());
        assert!(
            matches!(GroupCommitWorker::start(log.clone(), &shape(), worker_config()),
            Err(WorkerError::StorageRejected(error)) if matches!(*error, StorageError::Poisoned))
        );
        drop(worker);
        drop(log);
        let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
        let recovered =
            PartitionLog::recover_sealed(&dir.path().join("stream"), &config(), metrics).unwrap();
        assert!(recovered.lookup(batch(0, 0).id()).unwrap().is_some());
        assert!(recovered.lookup(batch(1, 1).id()).unwrap().is_none());
    }

    #[test]
    fn uncertain_persistence_fails_active_group_and_never_emits_queued_group() {
        let (_dir, log) = fixture();
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        log.fail_next_persist();
        let gate = Gate::install(&worker);
        let active = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let untouched = worker.try_submit(batch(1, 1)).unwrap();
        gate.release();
        assert!(
            matches!(active.wait(), Err(WorkerError::OutcomeUnknown(error)) if matches!(*error, StorageError::Unknown(_)))
        );
        assert!(matches!(
            untouched.wait(),
            Err(WorkerError::StoppedBeforeEffects)
        ));
        assert!(matches!(
            worker.shutdown(),
            Err(WorkerError::OutcomeUnknown(_))
        ));
        assert_eq!(
            (
                worker.stats().pending_requests,
                worker.stats().pending_bytes
            ),
            (0, 0)
        );
        assert_eq!(log.stats().appended_groups, 1);
        assert_eq!(log.stats().persistence_barriers, 0);
        assert!(log.lookup(batch(0, 0).id()).unwrap().is_some());
        assert!(log.lookup(batch(1, 1).id()).unwrap().is_none());
        assert!(
            matches!(GroupCommitWorker::start(log, &shape(), worker_config()),
            Err(WorkerError::StorageRejected(error)) if matches!(*error, StorageError::Poisoned))
        );
    }

    #[test]
    fn unreadable_identity_history_stops_before_emitting_another_group() {
        let dir = tempdir::TempDir::new("worker-cold-identity").unwrap();
        let mut config = config();
        config.max_batches = 1;
        config.group_limits.max_batches = 1;
        let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
        let log = Arc::new(
            PartitionLog::create(&dir.path().join("stream"), config, &shape(), metrics).unwrap(),
        );
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        worker.commit(batch(0, 0)).unwrap();
        // The one-batch resident window spills the first completed authority.
        worker.commit(batch(1, 1)).unwrap();
        let gate = Gate::install(&worker);
        let active = worker.try_submit(batch(0, 0)).unwrap();
        gate.entered();
        let untouched = worker.try_submit(batch(2, 2)).unwrap();
        // Damage real cold authority, not the WAL payload or an injected result.
        // A failed lookup must not be interpreted as a never-before-seen ID.
        std::fs::write(
            dir.path().join("stream/storage-history-v1/receipts.pages"),
            b"corrupt",
        )
        .unwrap();
        gate.release();
        assert!(matches!(
            active.wait(),
            Err(WorkerError::StorageRejected(_))
        ));
        assert!(matches!(
            untouched.wait(),
            Err(WorkerError::StoppedBeforeEffects)
        ));
        assert!(worker.shutdown().is_err());
        assert_eq!(log.stats().appended_groups, 2);
        assert_eq!(log.stats().persistence_barriers, 2);
        assert_eq!(worker.stats().pending_requests, 0);
        assert_eq!(worker.stats().pending_bytes, 0);
    }

    #[test]
    fn submitted_windows_split_at_fresh_resident_count_and_metadata_limits() {
        for metadata_bound in [false, true] {
            let dir = tempdir::TempDir::new("worker-resident-group-bound").unwrap();
            let mut config = config();
            config.group_limits.max_batches = 4;
            if metadata_bound {
                config.max_reference_metadata_bytes =
                    ReferenceDirectory::metadata_bytes(1, 1).unwrap();
            } else {
                config.max_batches = 1;
            }
            let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
            let log = Arc::new(
                PartitionLog::create(&dir.path().join("stream"), config, &shape(), metrics)
                    .unwrap(),
            );
            let mut worker =
                GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
            let pending = worker
                .try_submit_many(vec![batch(0, 0), batch(1, 1), batch(2, 2)])
                .unwrap();
            for (sequence, pending) in pending.into_iter().enumerate() {
                let durable = pending.wait().unwrap();
                assert_eq!(
                    durable.appended().id(),
                    batch(sequence as u8, sequence as u64).id()
                );
            }
            worker.shutdown().unwrap();
            assert_eq!(log.receipt_count(), 3);
            assert_eq!(log.stats().appended_groups, 3);
            assert_eq!(log.stats().persistence_barriers, 3);
            assert_eq!(worker.stats().coalescing_waits, 0);
        }
    }

    #[test]
    fn singleton_metadata_overflow_rejects_before_worker_admission() {
        let dir = tempdir::TempDir::new("worker-singleton-metadata-bound").unwrap();
        let mut config = config();
        config.max_reference_metadata_bytes = ReferenceDirectory::metadata_bytes(1, 1).unwrap();
        let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
        let log = Arc::new(
            PartitionLog::create(
                &dir.path().join("stream"),
                config.clone(),
                &shape(),
                metrics,
            )
            .unwrap(),
        );
        let mut worker = GroupCommitWorker::start(log.clone(), &shape(), worker_config()).unwrap();
        let too_large = CompleteBatch::new(
            &shape(),
            batch(0, 0).id(),
            batch(0, 0).version(),
            vec![
                Mutation::Put {
                    keyspace: KeySpace(0),
                    key: b"key1"[..].into(),
                    value: b"one"[..].into(),
                },
                Mutation::Put {
                    keyspace: KeySpace(0),
                    key: b"key2"[..].into(),
                    value: b"two"[..].into(),
                },
            ],
            config.batch_limits,
        )
        .unwrap();
        assert!(matches!(
            worker.try_submit(too_large),
            Err(WorkerError::StorageRejected(error)) if matches!(*error, StorageError::Capacity)
        ));
        assert_eq!(worker.stats().pending_requests, 0);
        assert_eq!(log.stats().appended_groups, 0);
        worker.shutdown().unwrap();
    }
}
