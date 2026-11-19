use super::super::{
    CheckpointPressure, CompleteBatch, DependencyKey, LocalDbError, Mutation, WriteTimelineSample,
    WriteTimelineStage, WriteWait, elapsed,
};
use super::Dependency;
use super::core::{RangeBarrier, WriteCore};
use super::identity::{IdentityClass, IdentityEntry};
use super::owner_queue::OwnerQueue;
use super::publisher::AbortedAllocation;
use super::sequencer::{Resolution, SlotPublication};
use crate::distributed::codec::ValidatedMutations;
use crate::distributed::native_publication::UnboundNativePublicationRouting;
use crate::distributed::reference_directory::ReferenceDirectory;
use crate::distributed::remote_db::concurrent::{WriteCredit, WriteGrant};
use crate::distributed::types::{BatchId, BatchVersion};
use crate::distributed::write_timeline::WriteBlockStage;
use smallvec::SmallVec;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

type Input = ([u8; 16], Vec<Mutation>);

struct Allocation {
    sequence: u64,
    owner_sequence: u64,
}

/// Consecutive plans whose per-partition totals fit one fresh reference
/// directory, with the request count and bytes of the inputs naming them.
struct Segment {
    plans: std::ops::Range<usize>,
    requests: usize,
    bytes: usize,
}

struct Planned<'a> {
    operation: [u8; 16],
    validated: ValidatedMutations<'a>,
    routing: Option<UnboundNativePublicationRouting<'a>>,
    owner: usize,
    metadata_charge: usize,
    bytes: usize,
    keys: Vec<Dependency>,
    identity: Option<IdentityEntry>,
    allocation: Option<Allocation>,
    publication: Option<SlotPublication>,
}

/// Caller ownership, not a reply channel. Drop releases only generations this
/// invocation allocated. Retry aliases never touch another caller's slot.
struct QueuedWindow<'a> {
    core: &'a WriteCore,
    versions: SmallVec<[BatchVersion; 1]>,
    owned: SmallVec<[u64; 1]>,
    _credit: WriteCredit,
}

impl Drop for QueuedWindow<'_> {
    fn drop(&mut self) {
        for &sequence in &self.owned {
            // release_sequence fail-stops the core on an accounting violation.
            let _ = self.core.release_sequence(sequence);
        }
    }
}

enum AllocationResult {
    Allocated,
    Replan,
    /// Another fresh `DropCells` slot still holds the publication barrier.
    RangeBusy,
    Closed(LocalDbError),
}

impl WriteCore {
    pub(in crate::distributed::remote_db) fn write(
        self: &Arc<Self>,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<BatchVersion, LocalDbError> {
        let inputs = [(operation, mutations)];
        let queued = self.enqueue_window(&inputs, true, sample, false)?;
        // No queued routing/value handle retains caller backing after bind.
        // Release even arbitrarily large source allocations before waiting.
        drop(inputs);
        let version = *queued
            .versions
            .first()
            .ok_or_else(|| LocalDbError::new("write allocated no version"))?;
        let started = Instant::now();
        let deadline = started + self.config.timeout;
        let result = if queued.owned.contains(&version.sequence) && version.epoch == self.epoch {
            self.sequencer.wait(version, deadline)
        } else {
            self.wait_retry(version, deadline)
        };
        #[cfg(test)]
        {
            // One-shot: the outcome is resolved but this caller has not yet
            // released its slot. A dropped resume sender also unblocks.
            let pause = self.test_hooks.lock().before_release.take();
            if let Some((entered, resume)) = pause {
                let _ = entered.try_send(());
                let _ = resume.recv();
            }
        }
        self.stats.lock().database.storage_wait_ns += elapsed(started);
        result.and_then(|version| {
            self.ensure_healthy()
                .map(|()| version)
                .map_err(|error| LocalDbError::unknown(version, error))
        })
    }

    /// Submit one singleton `DropCells` batch and wait for its publication.
    /// The slot holds the global publication barrier from allocation until
    /// it is published or aborted before READY; WAL admission and the shared
    /// write gate stay open. Point windows can never carry this member.
    pub(in crate::distributed::remote_db) fn write_drop_cells(
        self: &Arc<Self>,
        operation: [u8; 16],
        mutation: Mutation,
    ) -> Result<BatchVersion, LocalDbError> {
        if !matches!(mutation, Mutation::DropCells { .. }) {
            return Err(LocalDbError::new(
                "write_drop_cells requires a DropCells member",
            ));
        }
        let inputs = [(operation, vec![mutation])];
        let queued = self.enqueue_window(&inputs, true, None, true)?;
        let version = *queued
            .versions
            .first()
            .ok_or_else(|| LocalDbError::new("write allocated no version"))?;
        let deadline = Instant::now() + self.config.timeout;
        let result = if queued.owned.contains(&version.sequence) && version.epoch == self.epoch {
            self.sequencer.wait(version, deadline)
        } else {
            self.wait_retry(version, deadline)
        };
        result.and_then(|version| {
            self.ensure_healthy()
                .map(|()| version)
                .map_err(|error| LocalDbError::unknown(version, error))
        })
    }

    pub(in crate::distributed::remote_db) fn submit(
        self: &Arc<Self>,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        let inputs = [(operation, mutations)];
        let queued = self.enqueue_window(&inputs, false, None, false)?;
        queued
            .versions
            .first()
            .copied()
            .ok_or_else(|| LocalDbError::new("write allocated no version"))
    }

    /// Admits the window in input order without acknowledging durability.
    /// Validation and retry conflicts reject the whole window before any
    /// allocation. A window whose batches exceed one partition's fresh
    /// reference capacity is admitted in consecutive segments; each later
    /// segment waits for the earlier segments' publications. An error in a
    /// later segment leaves the earlier segments admitted: it carries the
    /// window's first allocated version as `uncertain_version` (an uncertain
    /// prefix, as admission pressure did); retry the whole window.
    pub(in crate::distributed::remote_db) fn submit_many(
        self: &Arc<Self>,
        inputs: &[Input],
    ) -> Result<Vec<BatchVersion>, LocalDbError> {
        self.ensure_writable()?;
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let mut queued = self.enqueue_window(inputs, false, None, false)?;
        Ok(std::mem::take(&mut queued.versions).into_vec())
    }

    fn enqueue_window<'a>(
        self: &'a Arc<Self>,
        inputs: &[Input],
        block: bool,
        sample: Option<WriteTimelineSample>,
        drop_cells: bool,
    ) -> Result<QueuedWindow<'a>, LocalDbError> {
        let first_allocated = Cell::new(None);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.enqueue_window_inner(inputs, block, sample, drop_cells, &first_allocated)
        })) {
            Ok(result) => result,
            Err(_) => {
                let error = match first_allocated.get() {
                    Some(version) => {
                        LocalDbError::unknown(version, "write caller panicked; recovery required")
                    }
                    None => LocalDbError::new("write caller panicked; recovery required"),
                };
                self.fail(copy_error(&error));
                Err(error)
            }
        }
    }

    fn enqueue_window_inner<'a>(
        self: &'a Arc<Self>,
        inputs: &[Input],
        block: bool,
        sample: Option<WriteTimelineSample>,
        drop_cells: bool,
        first_allocated: &Cell<Option<BatchVersion>>,
    ) -> Result<QueuedWindow<'a>, LocalDbError> {
        self.ensure_writable()?;
        if inputs.len() > self.config.max_pending_batches {
            return Err(LocalDbError::new("write exceeds admission request budget"));
        }
        // A DropCells member needs the publication barrier: only the
        // dedicated entry point admits it, alone in its window.
        if inputs.iter().any(|(_, mutations)| {
            mutations
                .iter()
                .any(|mutation| matches!(mutation, Mutation::DropCells { .. }))
        }) != drop_cells
            || (drop_cells && inputs.len() != 1)
        {
            return Err(LocalDbError::new(
                "DropCells must be the only member in a batch",
            ));
        }
        if let Some(sample) = sample {
            self.write_timeline
                .event(sample, WriteTimelineStage::ValidationStart);
        }
        let started = Instant::now();
        let mut plans = SmallVec::<[Planned<'_>; 1]>::with_capacity(inputs.len());
        let mut aliases = SmallVec::<[usize; 1]>::with_capacity(inputs.len());
        // The scalar path needs no duplicate-id hash table or heap allocation.
        let mut identities = (inputs.len() > 1)
            .then(|| HashMap::<[u8; 16], (usize, usize)>::with_capacity(inputs.len()));
        for (input_index, (operation, mutations)) in inputs.iter().enumerate() {
            let validated = CompleteBatch::validated_mutations(
                &self.shape,
                mutations,
                self.config.storage.batch_limits,
            )
            .map_err(LocalDbError::component)?;
            if let Some(&(plan_index, first_input)) =
                identities.as_ref().and_then(|ids| ids.get(operation))
            {
                if inputs[first_input].1 != *mutations {
                    return Err(LocalDbError::new("retry changed original batch"));
                }
                aliases.push(plan_index);
                continue;
            }
            let id = BatchId {
                database: self.config.storage.database,
                operation: *operation,
            };
            let owner = self
                .inventory
                .partition_for_validated(id)
                .map_err(LocalDbError::component)?;
            let owner = *self
                .partition_indexes
                .get(&owner)
                .ok_or_else(|| LocalDbError::new("write names an unregistered owner"))?;
            let routing = UnboundNativePublicationRouting::prepare(&self.shape, &validated)
                .map_err(LocalDbError::component)?;
            let metadata_charge = self
                .index
                .estimated_routed_bytes(routing.operations())
                .map_err(LocalDbError::component)?;
            let dependency_bytes = routing
                .operations()
                .iter()
                .try_fold(0usize, |bytes, operation| {
                    bytes.checked_add(if operation.reduced_key().len() > 32 {
                        operation.reduced_key().len().checked_mul(2)?
                    } else {
                        0
                    })
                })
                .ok_or_else(|| LocalDbError::new("dependency key reservation overflow"))?;
            // Two complete-batch metadata vectors (slot and owner), one routing
            // vector, key/registry bucket slack, and bounded predecessor storage.
            // Values have only the caller input and one canonical encoding.
            let credit = validated
                .encoded_len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(validated.routing_heap_bound()))
                .and_then(|n| n.checked_add(routing.owned_bytes(&self.shape)?))
                .and_then(|n| {
                    n.checked_add(mutations.len().checked_mul(
                        2 * (std::mem::size_of::<Mutation>()
                            + std::mem::size_of::<crate::distributed::types::RecordDescriptor>())
                            + 4 * std::mem::size_of::<(DependencyKey, u64)>()
                            + 2 * std::mem::size_of::<u64>(),
                    )?)
                })
                .and_then(|n| n.checked_add(dependency_bytes))
                .and_then(|n| n.checked_add(2048))
                .ok_or_else(|| LocalDbError::new("write admission byte overflow"))?;
            let keys = routing
                .operations()
                .iter()
                .map(|operation| {
                    (
                        operation.ks(),
                        DependencyKey::from_slice(operation.reduced_key().as_ref()),
                    )
                })
                .collect();
            let plan_index = plans.len();
            if let Some(ids) = &mut identities {
                ids.insert(*operation, (plan_index, input_index));
            }
            aliases.push(plan_index);
            plans.push(Planned {
                operation: *operation,
                validated,
                routing: Some(routing),
                owner,
                metadata_charge,
                bytes: credit,
                keys,
                identity: None,
                allocation: None,
                publication: None,
            });
        }
        let preparation_ns = elapsed(started);
        {
            let mut stats = self.stats.lock();
            stats.write.authority_windows += 1;
            stats.write.authority_batches += inputs.len() as u64;
            stats.write.authority_prepare_ns += preparation_ns;
        }
        let segments = self.partition_segments(&plans, &aliases)?;
        if segments.len() > 1 {
            // Retry conflicts are deterministic input errors: reject them before
            // the first segment allocates. Each segment rechecks under its grant.
            let deadline = Instant::now() + self.config.timeout;
            for plan in &plans {
                let Some(identity) = self.wait_identity_ready(plan.operation, deadline)? else {
                    continue;
                };
                let retry = plan
                    .validated
                    .encode(
                        BatchId {
                            database: self.config.storage.database,
                            operation: plan.operation,
                        },
                        identity.version,
                    )
                    .map_err(LocalDbError::component)?;
                if Some(retry.digest()) != identity.digest {
                    return Err(LocalDbError::new("retry changed original batch"));
                }
            }
        }
        let mut queued = None;
        let mut preparation_ns = Some(preparation_ns);
        for (index, segment) in segments.into_iter().enumerate() {
            // A finished segment releases only its own generations; publication
            // returns its partition charges independently of this caller.
            drop(queued.take());
            // Later segments wait for earlier segments' partition charges.
            let prefix = first_allocated.get();
            let result = self.enqueue_segment(
                &mut plans[segment.plans],
                segment.requests,
                segment.bytes,
                block || index > 0,
                sample,
                first_allocated,
                preparation_ns.take().unwrap_or(0),
                drop_cells,
            );
            queued = Some(match (result, prefix) {
                (Ok(segment), _) => segment,
                // Earlier segments are admitted: never report a no-effect error.
                (Err(error), Some(version)) if error.uncertain_version.is_none() => {
                    return Err(LocalDbError::unknown(version, error));
                }
                (Err(error), _) => return Err(error),
            });
        }
        let mut queued = queued.ok_or_else(|| LocalDbError::new("write window has no segment"))?;
        queued.versions.reserve(aliases.len());
        for &alias in &aliases {
            let plan = &plans[alias];
            let version = match (&plan.allocation, plan.identity) {
                (Some(allocation), _) => BatchVersion {
                    epoch: self.epoch,
                    sequence: allocation.sequence,
                },
                (None, Some(identity)) => identity.version,
                _ => return Err(LocalDbError::new("write window lost an allocated identity")),
            };
            queued.versions.push(version);
        }
        #[cfg(test)]
        self.report_progress();
        Ok(queued)
    }

    /// Splits the planned window, in input order, into consecutive segments
    /// whose per-partition batch and record totals each fit one fresh
    /// reference directory. Retry plans are counted conservatively.
    fn partition_segments(
        &self,
        plans: &[Planned<'_>],
        aliases: &[usize],
    ) -> Result<SmallVec<[Segment; 1]>, LocalDbError> {
        let max_batches = self.config.storage.max_batches_per_partition.min(1024);
        let max_bytes = self.config.storage.max_reference_metadata_bytes;
        let mut segments = SmallVec::<[Segment; 1]>::new();
        // (partition, batches, records) of the open segment.
        let mut totals = SmallVec::<[(usize, usize, usize); 4]>::new();
        let mut start = 0;
        for (index, plan) in plans.iter().enumerate() {
            let records = plan.validated.mutations().len();
            let (batches, total) = totals
                .iter()
                .find(|entry| entry.0 == plan.owner)
                .map_or((1, records), |entry| {
                    (entry.1.saturating_add(1), entry.2.saturating_add(records))
                });
            if index > start
                && ReferenceDirectory::check_fresh_total_capacity(
                    max_batches,
                    max_bytes,
                    batches,
                    total,
                )
                .is_err()
            {
                segments.push(Segment {
                    plans: start..index,
                    requests: 0,
                    bytes: 0,
                });
                start = index;
                totals.clear();
            }
            match totals.iter_mut().find(|entry| entry.0 == plan.owner) {
                Some(entry) => {
                    entry.1 += 1;
                    entry.2 += records;
                }
                None => totals.push((plan.owner, 1, records)),
            }
        }
        segments.push(Segment {
            plans: start..plans.len(),
            requests: 0,
            bytes: 0,
        });
        for &alias in aliases {
            // Segments are ordered and contiguous over plan indices.
            let index = segments.partition_point(|segment| segment.plans.end <= alias);
            let segment = &mut segments[index];
            segment.requests += 1;
            segment.bytes = segment
                .bytes
                .checked_add(plans[alias].bytes)
                .ok_or_else(|| LocalDbError::new("write admission byte overflow"))?;
        }
        Ok(segments)
    }

    /// Grant, allocate, encode and mark READY one segment. Versions are
    /// resolved by the window after its last segment.
    #[allow(clippy::too_many_arguments)]
    fn enqueue_segment<'a>(
        self: &'a Arc<Self>,
        plans: &mut [Planned<'_>],
        requests: usize,
        bytes: usize,
        block: bool,
        sample: Option<WriteTimelineSample>,
        first_allocated: &Cell<Option<BatchVersion>>,
        preparation_ns: u64,
        drop_cells: bool,
    ) -> Result<QueuedWindow<'a>, LocalDbError> {
        let deadline = Instant::now() + self.config.timeout;
        // Only no-effect identity races replan. The absolute deadline also
        // bounds repeated aborts of the same operation id by other callers.
        loop {
            self.ensure_writable()?;
            if Instant::now() >= deadline {
                return Err(LocalDbError::new(
                    "write identity preflight deadline expired",
                ));
            }
            let mut grant = WriteGrant {
                requests,
                bytes,
                ..WriteGrant::default()
            };
            for plan in plans.iter_mut() {
                plan.identity = self.lookup_identity(plan.operation)?;
                if plan.identity.is_none() {
                    grant.slots += 1;
                    grant.identities += 1;
                    grant.index_bytes = grant
                        .index_bytes
                        .checked_add(plan.metadata_charge)
                        .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
                    grant.replay_batches += 1;
                    grant.replay_bytes = grant
                        .replay_bytes
                        .checked_add(plan.validated.encoded_len())
                        .ok_or_else(|| LocalDbError::new("checkpoint tail reservation overflow"))?;
                    grant.add_partition(plan.owner, plan.validated.mutations().len())?;
                }
            }
            if let Some(sample) = sample {
                self.write_timeline
                    .event(sample, WriteTimelineStage::AdmissionWaitStarted);
            }
            let block_started = self.write_timeline.block_started();
            let credit = self.admission.reserve_grant(
                grant,
                block,
                || self.ensure_writable(),
                |pressure| self.admission_pressure(pressure),
            )?;
            self.write_timeline.block_finished(
                WriteBlockStage::AdmissionCredit,
                block_started,
                requests,
            );
            if let Some(sample) = sample {
                self.write_timeline
                    .event(sample, WriteTimelineStage::AdmissionAcquired);
            }
            let mut queued = QueuedWindow {
                core: self,
                versions: SmallVec::with_capacity(requests),
                owned: SmallVec::with_capacity(plans.len()),
                _credit: credit,
            };
            let mut replan = false;
            // Fresh plans need no work here: the sparse-view budget is a
            // standing reservation held by the publisher since open.
            for plan in plans.iter_mut().filter(|plan| plan.identity.is_some()) {
                let Some(identity) = self.wait_identity_ready(plan.operation, deadline)? else {
                    replan = true;
                    break;
                };
                let retry = plan
                    .validated
                    .encode(
                        BatchId {
                            database: self.config.storage.database,
                            operation: plan.operation,
                        },
                        identity.version,
                    )
                    .map_err(LocalDbError::component)?;
                if Some(retry.digest()) != identity.digest {
                    return Err(LocalDbError::new("retry changed original batch"));
                }
                plan.identity = Some(identity);
            }
            if replan {
                drop(queued);
                continue;
            }
            #[cfg(test)]
            {
                let pause = self.test_hooks.lock().after_grant.take();
                if let Some((entered, resume)) = pause {
                    let _ = entered.try_send(());
                    resume.recv_timeout(self.config.timeout).map_err(|_| {
                        LocalDbError::new("write allocation test pause disconnected")
                    })?;
                }
            }
            let operations: SmallVec<[[u8; 16]; 1]> =
                plans.iter().map(|plan| plan.operation).collect();
            let shards = self
                .dependencies
                .shard_indices(plans.iter().flat_map(|plan| plan.keys.iter()));
            let mut owners: SmallVec<[(usize, &OwnerQueue); 2]> = plans
                .iter()
                .filter(|plan| plan.identity.is_none())
                .map(|plan| (plan.owner, self.clients[plan.owner].queue.as_ref()))
                .collect();
            if owners.is_empty() {
                owners.push((0, self.clients[0].queue.as_ref()));
            }
            // Option D: the owner-ordering stage is the caller's own allocation
            // in the identity → key → owner-order critical section.
            if let Some(sample) = sample {
                self.write_timeline
                    .event(sample, WriteTimelineStage::OwnerEnqueueStarted);
            }
            let allocate_started = Instant::now();
            let block_started = self.write_timeline.block_started();
            // A DropCells allocation takes the publisher mutex outside its short
            // allocate transaction and records the barrier before release, so
            // no higher sequence can publish ahead of it. Order: publisher →
            // identity shards → key shards → owner order → slot; it never
            // takes the index writer or Admission while holding publisher.
            let mut range_publisher = drop_cells.then(|| self.publisher.lock());
            let allocated = self.identities.with_operations(&operations, |identities| {
                self.dependencies.with_shards(shards, |keys| {
                    OwnerQueue::with_owners(&owners, |owners| {
                        if let Err(error) = self.ensure_writable() {
                            return Ok(AllocationResult::Closed(error));
                        }
                        if range_publisher
                            .as_ref()
                            .is_some_and(|publisher| publisher.range_barrier.is_some())
                        {
                            return Ok(AllocationResult::RangeBusy);
                        }
                        // Freeze every identity before allocating any member.
                        // A newly inserted or changed identity must be encoded
                        // and compared outside these locks, never admitted late.
                        // Replan repeats the lookup outside these locks.
                        for plan in plans.iter() {
                            match (plan.identity, identities.get(plan.operation)) {
                                (None, None) | (Some(_), None) => {}
                                (Some(expected), Some(actual))
                                    if expected.version == actual.version
                                        && expected.digest == actual.digest => {}
                                _ => return Ok(AllocationResult::Replan),
                            }
                        }
                        queued._credit.mark_allocated();
                        for plan in plans.iter_mut() {
                            if plan.identity.is_some() {
                                continue;
                            }
                            let id = BatchId {
                                database: self.config.storage.database,
                                operation: plan.operation,
                            };
                            let (owner_sequence, sequence) = owners
                                .reserve(plan.owner, || self.sequencer.allocate(id))
                                .ok_or_else(|| {
                                    LocalDbError::new("allocation lost its locked owner")
                                })?;
                            let version = BatchVersion {
                                epoch: self.epoch,
                                sequence,
                            };
                            if first_allocated.get().is_none() {
                                first_allocated.set(Some(version));
                            }
                            queued.owned.push(sequence);
                            let slot = self.sequencer.slot(sequence);
                            let mut data = slot.data.lock();
                            keys.register(&plan.keys, sequence, &mut data.predecessors, |prior| {
                                self.sequencer.resolved(prior)
                            });
                            data.keys.append(&mut plan.keys);
                            identities.insert(
                                plan.operation,
                                IdentityEntry {
                                    version,
                                    digest: None,
                                    seq: sequence,
                                    class: IdentityClass::Active,
                                },
                            );
                            plan.allocation = Some(Allocation {
                                sequence,
                                owner_sequence,
                            });
                            if let Some(publisher) = range_publisher.as_mut() {
                                publisher.range_barrier = Some(RangeBarrier {
                                    sequence,
                                    deferred_ready: Vec::new(),
                                });
                            }
                        }
                        Ok(AllocationResult::Allocated)
                    })
                })
            });
            drop(range_publisher);
            self.write_timeline.block_finished(
                WriteBlockStage::Allocate,
                block_started,
                plans.len(),
            );
            self.stats.lock().write.authority_admit_ns += elapsed(allocate_started);
            match allocated {
                Ok(AllocationResult::Replan) => {
                    drop(queued);
                    continue;
                }
                Ok(AllocationResult::RangeBusy) => {
                    drop(queued);
                    self.wait_range_barrier(deadline)?;
                    continue;
                }
                Ok(AllocationResult::Allocated) => {
                    if let Some(sample) = sample {
                        self.write_timeline
                            .event(sample, WriteTimelineStage::OwnerDequeued);
                    }
                }
                Ok(AllocationResult::Closed(error)) => return Err(error),
                Err(error) => {
                    self.fail(copy_error(&error));
                    return Err(match first_allocated.get() {
                        Some(version) => LocalDbError::unknown(version, error),
                        None => error,
                    });
                }
            }
            {
                let mut stats = self.stats.lock();
                stats.database.admitted_batches += queued.owned.len() as u64;
                stats.pipeline.concurrent_admitted += requests as u64;
            }
            self.write_timeline
                .wait_enter(WriteWait::AdmittedToAppendEnqueue, queued.owned.len());
            #[cfg(test)]
            self.report_progress();
            #[cfg(test)]
            {
                let (pause, panic_after) = {
                    let mut hooks = self.test_hooks.lock();
                    (
                        hooks.before_preparation.take(),
                        hooks.panic_after_concurrent_admissions,
                    )
                };
                if let Some((entered, resume)) = pause {
                    let _ = entered.try_send(());
                    resume
                        .recv_timeout(self.config.timeout)
                        .map_err(|_| LocalDbError::new("write encoding test pause disconnected"))?;
                }
                if panic_after
                    .is_some_and(|limit| self.stats.lock().pipeline.concurrent_admitted >= limit)
                {
                    panic!("injected caller allocation panic");
                }
            }
            // DropCells preflight: after the lower prefix resolves and before
            // READY, collect the occupied target cells and reserve their mask
            // charge. Any failure here is a pre-effect abort of the slot.
            let mut drop_targets = None;
            if drop_cells
                && let Some(plan) = plans.first_mut()
                && let Some(allocation) = &plan.allocation
            {
                let sequence = allocation.sequence;
                match self.prepare_drop_cells(
                    plan.validated.mutations(),
                    sequence,
                    plan.metadata_charge,
                    deadline,
                ) {
                    Ok((targets, metadata_charge)) => {
                        plan.metadata_charge = metadata_charge;
                        drop_targets = Some(targets);
                    }
                    Err(error) => {
                        self.write_timeline
                            .wait_leave(WriteWait::AdmittedToAppendEnqueue, queued.owned.len());
                        self.abort_window(plans, &error)?;
                        return Err(error);
                    }
                }
            }
            let encoding_started = Instant::now();
            let block_started = self.write_timeline.block_started();
            let encoding = (|| {
                // Encode the whole validated window before any owner READY.
                for plan in plans.iter_mut() {
                    let Some(allocation) = &plan.allocation else {
                        continue;
                    };
                    // One-shot on match, so a retry of the same id succeeds.
                    #[cfg(test)]
                    if self
                        .test_hooks
                        .lock()
                        .fail_prepared_operation
                        .take_if(|operation| *operation == plan.operation)
                        .is_some()
                    {
                        return Err(LocalDbError::new("injected caller encoding failure"));
                    }
                    let version = BatchVersion {
                        epoch: self.epoch,
                        sequence: allocation.sequence,
                    };
                    self.write_timeline.batch(plan.operation, version);
                    let batch = plan
                        .validated
                        .encode(
                            BatchId {
                                database: self.config.storage.database,
                                operation: plan.operation,
                            },
                            version,
                        )
                        .map_err(LocalDbError::component)?
                        .into_canonical_owned();
                    self.write_timeline.batch_event(
                        version,
                        WriteTimelineStage::Prepared,
                        None,
                        None,
                    );
                    let mut routing = plan
                        .routing
                        .take()
                        .ok_or_else(|| LocalDbError::new("allocation lost its routing token"))?
                        .bind(&batch)
                        .map_err(LocalDbError::component)?;
                    if let Some(targets) = drop_targets.take() {
                        routing
                            .set_drop_targets(targets)
                            .map_err(LocalDbError::component)?;
                    }
                    let last_mutation = batch
                        .mutation_version(batch.mutations().len() - 1)
                        .ok_or_else(|| LocalDbError::new("empty admitted batch"))?;
                    plan.publication = Some(SlotPublication {
                        batch,
                        routing,
                        last_mutation,
                        metadata_charge: plan.metadata_charge,
                    });
                }
                Ok::<(), LocalDbError>(())
            })();
            self.write_timeline.block_finished(
                WriteBlockStage::Encode,
                block_started,
                queued.owned.len(),
            );
            {
                let encoded_ns = elapsed(encoding_started);
                let mut stats = self.stats.lock();
                stats.database.validation_ns += preparation_ns + encoded_ns;
                stats.write.authority_prepare_ns += encoded_ns;
                stats.pipeline.prepare_calls += 1;
                stats.pipeline.prepare_wall_ns += encoded_ns;
                stats.pipeline.prepare_task_ns += encoded_ns;
            }
            if let Err(error) = encoding {
                self.write_timeline
                    .wait_leave(WriteWait::AdmittedToAppendEnqueue, queued.owned.len());
                self.abort_window(plans, &error)?;
                return Err(error);
            }
            let block_started = self.write_timeline.block_started();
            let enqueue = (|| {
                for plan in plans.iter_mut() {
                    let Some(allocation) = &plan.allocation else {
                        continue;
                    };
                    self.ensure_healthy()?;
                    let publication = plan.publication.take().ok_or_else(|| {
                        LocalDbError::new("encoded allocation lost its publication payload")
                    })?;
                    let batch = publication.batch.clone();
                    self.identities.with_operation(plan.operation, |entries| {
                        let entry = entries
                            .get_mut(&plan.operation)
                            .filter(|entry| entry.seq == allocation.sequence)
                            .ok_or_else(|| {
                                LocalDbError::new("encoded allocation lost its identity")
                            })?;
                        entry.digest = Some(batch.digest());
                        Ok::<(), LocalDbError>(())
                    })?;
                    let slot = self.sequencer.slot(allocation.sequence);
                    {
                        let mut data = slot.data.lock();
                        data.digest = Some(batch.digest());
                        data.last_mutation = Some(publication.last_mutation);
                        data.publication = Some(publication);
                        // Slot READY and its publication payload precede owner READY.
                        // CAS cannot overwrite a concurrent fail-stop UNKNOWN state.
                        self.sequencer.transition(
                            allocation.sequence,
                            Resolution::Allocated,
                            Resolution::Ready,
                        )?;
                        slot.done.notify_all();
                    }
                    self.write_timeline.batch_event(
                        batch.version(),
                        WriteTimelineStage::Admitted,
                        None,
                        None,
                    );
                    // The owner-queue READY handoff replaces the authority's append enqueue.
                    self.write_timeline.batch_event(
                        batch.version(),
                        WriteTimelineStage::AppendEnqueueStarted,
                        Some(self.config.endpoints[plan.owner].partition),
                        None,
                    );
                    // Enter before READY: the worker's dequeue leave cannot precede it.
                    self.write_timeline
                        .wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
                    // One W1 wake attempt per READY handoff. It is coalesced when no
                    // new hint was sent: the worker was not parked (it rechecks the
                    // queue before parking) or its bounded hint channel was full.
                    let coalesced = if self.clients[plan.owner].queue.ready(
                        allocation.owner_sequence,
                        allocation.sequence,
                        batch,
                    ) {
                        self.wake_owner(plan.owner)?
                    } else {
                        true
                    };
                    self.write_timeline
                        .wait_wake(WriteWait::OwnerEnqueueToDequeue, coalesced);
                }
                Ok::<(), LocalDbError>(())
            })();
            self.write_timeline
                .wait_leave(WriteWait::AdmittedToAppendEnqueue, queued.owned.len());
            if let Err(error) = enqueue {
                self.fail(copy_error(&error));
                return Err(match first_allocated.get() {
                    Some(version) => LocalDbError::unknown(version, error),
                    None => error,
                });
            }
            self.write_timeline.block_finished(
                WriteBlockStage::OwnerQueue,
                block_started,
                queued.owned.len(),
            );
            return Ok(queued);
        }
    }

    /// Called by a parked caller with no Admission or foreground lock held.
    fn admission_pressure(
        self: &Arc<Self>,
        pressure: CheckpointPressure,
    ) -> Result<(), LocalDbError> {
        if self.write_timeline.enabled() {
            let generation = {
                let publisher = self.publisher.lock();
                publisher
                    .background_checkpoint
                    .as_ref()
                    .map(|_| publisher.snapshot_generation)
            };
            self.write_timeline
                .checkpoint_pressure(self.epoch, generation, pressure);
        }
        self.request_checkpoint()
    }

    fn abort_window(
        self: &Arc<Self>,
        plans: &[Planned<'_>],
        error: &LocalDbError,
    ) -> Result<(), LocalDbError> {
        // Any encoded publication of an aborted plan is dropped with the plan.
        for plan in plans {
            let Some(allocation) = &plan.allocation else {
                continue;
            };
            self.abort_allocated(
                self.clients[plan.owner].queue.as_ref(),
                AbortedAllocation {
                    sequence: allocation.sequence,
                    owner_sequence: allocation.owner_sequence,
                    owner: plan.owner,
                    metadata_charge: plan.metadata_charge,
                    records: plan.validated.mutations().len(),
                    encoded_bytes: plan.validated.encoded_len(),
                },
                copy_error(error),
            )?;
        }
        Ok(())
    }

    fn wait_identity_ready(
        &self,
        operation: [u8; 16],
        deadline: Instant,
    ) -> Result<Option<IdentityEntry>, LocalDbError> {
        loop {
            self.ensure_writable()?;
            let Some(current) = self.lookup_identity(operation)? else {
                return Ok(None);
            };
            if current.class != IdentityClass::Active || current.version.epoch != self.epoch {
                return Ok(Some(current));
            }
            let slot = self.sequencer.slot(current.seq);
            let mut data = slot.data.lock();
            if self
                .sequencer
                .generation(current.seq)
                .is_some_and(|generation| generation > current.seq)
            {
                drop(data);
                return self.lookup_identity(operation);
            }
            match self.sequencer.resolution(current.seq) {
                Some(Resolution::Ready | Resolution::Durable | Resolution::Published) => {
                    let digest = data
                        .digest
                        .ok_or_else(|| LocalDbError::new("ready identity lost its digest"))?;
                    return Ok(Some(IdentityEntry {
                        digest: Some(digest),
                        ..current
                    }));
                }
                Some(Resolution::Aborted) => {
                    drop(data);
                    continue;
                }
                Some(Resolution::Unknown) => {
                    return Err(LocalDbError::unknown(
                        current.version,
                        "retry outcome requires recovery",
                    ));
                }
                Some(Resolution::Allocated) => {}
                None => {
                    drop(data);
                    let error = LocalDbError::new("retry identity lost its slot generation");
                    self.fail(copy_error(&error));
                    return Err(error);
                }
            }
            if Instant::now() >= deadline {
                return Err(LocalDbError::unknown(
                    current.version,
                    "retry encoding not resolved before deadline",
                ));
            }
            slot.done.wait_until(&mut data, deadline);
        }
    }

    fn wait_retry(
        &self,
        version: BatchVersion,
        deadline: Instant,
    ) -> Result<BatchVersion, LocalDbError> {
        if version.epoch != self.epoch {
            self.ensure_healthy()?;
            return Ok(version);
        }
        let slot = self.sequencer.slot(version.sequence);
        let mut data = slot.data.lock();
        loop {
            self.ensure_healthy()?;
            if self
                .sequencer
                .generation(version.sequence)
                .is_some_and(|generation| generation > version.sequence)
            {
                return Ok(version);
            }
            if let Some(result) = &data.outcome {
                return result.as_ref().copied().map_err(copy_error);
            }
            if Instant::now() >= deadline {
                return Err(LocalDbError::unknown(
                    version,
                    "write outcome not resolved before deadline",
                ));
            }
            slot.done.wait_until(&mut data, deadline);
        }
    }
}

/// Preserve the exact message and uncertain version of a fail-stop cause.
pub(in crate::distributed::remote_db) fn copy_error(error: &LocalDbError) -> LocalDbError {
    let mut copy = LocalDbError::new(error.message());
    copy.uncertain_version = error.uncertain_version;
    copy
}
