use super::super::{
    CompleteBatch, DependencyKey, LocalDbError, Mutation, WriteTimelineSample, WriteTimelineStage,
    WriteWait, elapsed,
};
use super::Dependency;
use super::core::{RangeBarrier, WriteCore};
use super::inflight::InFlightEntry;
use super::owner_queue::OwnerQueue;
use super::publisher::AbortedAllocation;
use super::sequencer::{Resolution, SlotPublication};
use crate::distributed::codec::ValidatedMutations;
use crate::distributed::native_publication::UnboundNativePublicationRouting;
use crate::distributed::reference_directory::ReferenceDirectory;
use crate::distributed::remote_db::RecentReceipt;
use crate::distributed::remote_db::concurrent::{WriteCredit, WriteGrant};
use crate::distributed::types::{BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress};
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
    bytes: usize,
    keys: Vec<Dependency>,
    /// A receipt already in the retry window: the window answers with the
    /// original version and no owner byte is appended.
    retry: Option<RecentReceipt>,
    /// The generation that currently owns this operation id, when neither the
    /// window nor this plan can answer. A retry waits on that generation's
    /// outcome instead of allocating a second version.
    in_flight: Option<InFlightEntry>,
    allocation: Option<Allocation>,
    publication: Option<SlotPublication>,
}

impl Planned<'_> {
    /// An already-owned operation id: the retry window or an in-flight
    /// generation answers it with the recorded version, so this plan allocates
    /// nothing, takes no owner order and charges no slot (`WriteGrant`).
    /// Evaluated before the allocation pass sets `in_flight` on a fresh plan.
    fn is_owned(&self) -> bool {
        self.retry.is_some() || self.in_flight.is_some()
    }
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
    /// Another generation claimed an observed operation id between the lookup
    /// and the freeze: re-look up outside the locks and retry the segment.
    Replan,
    /// Another fresh `DropCells` slot still holds the publication barrier.
    RangeBusy,
    Closed(LocalDbError),
}

/// What one admitted window carries. A `DropCells` member needs the publication
/// barrier and a relocated-copy batch needs the format-5 header; both are
/// admitted alone by their dedicated entry points.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowKind {
    Point,
    DropCells,
    Relocated,
}

impl WriteCore {
    pub(in crate::distributed::remote_db) fn write(
        self: &Arc<Self>,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<BatchVersion, LocalDbError> {
        let inputs = [(operation, mutations)];
        let started = Instant::now();
        let deadline = started + self.config.timeout;
        #[cfg(test)]
        let deadline = if std::mem::take(&mut self.test_hooks.lock().expire_next_write_deadline) {
            // Already expired: the resolution wait below fails at its first
            // predicate check, which is how a real deadline behaves once it
            // passes. Tests use this to time a caller out without a delay.
            started
        } else {
            deadline
        };
        // One re-application is allowed: a retry whose observed generation
        // aborted before publishing must behave as a fresh write, which is what
        // the original caller asked for either way.
        let mut reapplied = false;
        let result = loop {
            let queued = self.enqueue_window(&inputs, true, sample, WindowKind::Point)?;
            let version = *queued
                .versions
                .first()
                .ok_or_else(|| LocalDbError::new("write allocated no version"))?;
            let result = if queued.owned.contains(&version.sequence) && version.epoch == self.epoch
            {
                self.sequencer.wait(version, deadline)
            } else {
                match self.wait_retry(operation, version, deadline)? {
                    Retried::Published(version) => Ok(version),
                    Retried::Reapply if !reapplied => {
                        reapplied = true;
                        drop(queued);
                        continue;
                    }
                    Retried::Reapply => Err(LocalDbError::new(
                        "retried write did not publish after re-applying",
                    )),
                }
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
            break result;
        };
        // The slot and credit are released (`queued` dropped with the loop
        // body): an acknowledged write is readable by its writer, so wait for
        // the published prefix to cover it (`wait_readable`).
        let result = result.and_then(|version| self.wait_readable(version, deadline));
        // No queued routing/value handle retains caller backing after bind.
        // Release even arbitrarily large source allocations before returning.
        drop(inputs);
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
        let queued = self.enqueue_window(&inputs, true, None, WindowKind::DropCells)?;
        let version = *queued
            .versions
            .first()
            .ok_or_else(|| LocalDbError::new("write allocated no version"))?;
        let deadline = Instant::now() + self.config.timeout;
        let result = if queued.owned.contains(&version.sequence) && version.epoch == self.epoch {
            self.sequencer.wait(version, deadline)
        } else {
            match self.wait_retry(operation, version, deadline)? {
                Retried::Published(version) => Ok(version),
                // The range never published; a fresh submission gets the
                // barrier and a new version, which is what the caller wants.
                Retried::Reapply => Err(LocalDbError::new(
                    "drop-cells retry observed an unpublished generation; submit again",
                )),
            }
        };
        result.and_then(|version| {
            self.ensure_healthy()
                .map(|()| version)
                .map_err(|error| LocalDbError::unknown(version, error))
        })
    }

    /// Submits one batch of relocated copies and waits for its publication,
    /// returning its version and the owner address of its batch frame.
    ///
    /// The batch takes an allocator version and publishes in order like any
    /// other, so the cut cells advance over it, but it inserts nothing into the
    /// index (`NativePublicationRouting::publish`) and replay skips it
    /// (`CompleteBatch::is_relocated`). `operation` must be a fresh id: a retry
    /// of a relocation pass re-reads the index and copies again.
    pub(in crate::distributed::remote_db) fn write_relocated(
        self: &Arc<Self>,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<(BatchVersion, PhysicalValueAddress), LocalDbError> {
        let inputs = [(operation, mutations)];
        let queued = self.enqueue_window(&inputs, true, None, WindowKind::Relocated)?;
        let version = *queued
            .versions
            .first()
            .ok_or_else(|| LocalDbError::new("write allocated no version"))?;
        if !queued.owned.contains(&version.sequence) || version.epoch != self.epoch {
            return Err(LocalDbError::new(
                "relocation copy was answered by an earlier operation id",
            ));
        }
        let version = self
            .sequencer
            .wait(version, Instant::now() + self.config.timeout)?;
        // The slot is owned until `queued` drops, so its durable receipt is
        // still the one this batch published under.
        let address = self
            .durable_receipt(version.sequence)
            .map(|receipt| receipt.appended().address())
            .ok_or_else(|| LocalDbError::unknown(version, "relocation copy lost its receipt"))?;
        self.ensure_healthy()
            .map_err(|error| LocalDbError::unknown(version, error))?;
        Ok((version, address))
    }

    pub(in crate::distributed::remote_db) fn submit(
        self: &Arc<Self>,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        let inputs = [(operation, mutations)];
        let queued = self.enqueue_window(&inputs, false, None, WindowKind::Point)?;
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
        let mut queued = self.enqueue_window(inputs, false, None, WindowKind::Point)?;
        Ok(std::mem::take(&mut queued.versions).into_vec())
    }

    fn enqueue_window<'a>(
        self: &'a Arc<Self>,
        inputs: &[Input],
        block: bool,
        sample: Option<WriteTimelineSample>,
        kind: WindowKind,
    ) -> Result<QueuedWindow<'a>, LocalDbError> {
        let first_allocated = Cell::new(None);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.enqueue_window_inner(inputs, block, sample, kind, &first_allocated)
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
        kind: WindowKind,
        first_allocated: &Cell<Option<BatchVersion>>,
    ) -> Result<QueuedWindow<'a>, LocalDbError> {
        let drop_cells = kind == WindowKind::DropCells;
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
        let mut duplicate_inputs = (inputs.len() > 1)
            .then(|| HashMap::<[u8; 16], (usize, usize)>::with_capacity(inputs.len()));
        for (input_index, (operation, mutations)) in inputs.iter().enumerate() {
            let validated = if kind == WindowKind::Relocated {
                CompleteBatch::validated_relocated(
                    &self.shape,
                    mutations,
                    self.config.storage.batch_limits,
                )
            } else {
                CompleteBatch::validated_mutations(
                    &self.shape,
                    mutations,
                    self.config.storage.batch_limits,
                )
            }
            .map_err(LocalDbError::component)?;
            if let Some(&(plan_index, first_input)) =
                duplicate_inputs.as_ref().and_then(|ids| ids.get(operation))
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
            let dependency_bytes = routing
                .keys()
                .try_fold(0usize, |bytes, (_, reduced_key)| {
                    bytes.checked_add(if reduced_key.len() > 32 {
                        reduced_key.len().checked_mul(2)?
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
                .keys()
                .map(|(keyspace, reduced_key)| (keyspace, DependencyKey::from_slice(reduced_key)))
                .collect();
            let plan_index = plans.len();
            if let Some(ids) = &mut duplicate_inputs {
                ids.insert(*operation, (plan_index, input_index));
            }
            aliases.push(plan_index);
            plans.push(Planned {
                operation: *operation,
                validated,
                routing: Some(routing),
                owner,
                bytes: credit,
                keys,
                retry: None,
                in_flight: None,
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
            // the first segment allocates. Each segment rechecks under its grant,
            // which is what makes the answer authoritative.
            for plan in &mut plans {
                let in_flight = self.in_flight.lookup(plan.operation);
                if plan.retry.is_none() {
                    plan.retry = self.lookup_receipt(plan.operation)?;
                }
                if plan.retry.is_none() && plan.in_flight.is_none() {
                    plan.in_flight = in_flight;
                }
                // A stale observation is cleared here; the segment grant looks
                // the operation up again under the freeze.
                let _ = self.validate_retry(plan)?;
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
            let version = match (&plan.allocation, plan.retry, plan.in_flight) {
                (Some(allocation), ..) => BatchVersion {
                    epoch: self.epoch,
                    sequence: allocation.sequence,
                },
                (None, Some(receipt), _) => receipt.version,
                (None, None, Some(entry)) => entry.version,
                _ => return Err(LocalDbError::new("write window lost its version")),
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
        // The absolute deadline bounds repeated contention with another fresh
        // range. No retry race can restart this segment: a receipt that appears
        // between preflight and grant just turns that plan into a retry, which
        // re-returns the published version without an owner byte.
        loop {
            self.ensure_writable()?;
            if Instant::now() >= deadline {
                return Err(LocalDbError::new("write admission deadline expired"));
            }
            let mut grant = WriteGrant {
                requests,
                bytes,
                ..WriteGrant::default()
            };
            for plan in plans.iter_mut() {
                // The in-flight table must be probed before the retry window,
                // and the window wins when both answer. Publication pushes the
                // receipt before it retires the in-flight entry, so any probe
                // that misses the window (read second) still sees the entry;
                // the reverse order leaves one instant where both are gone.
                let in_flight = self.in_flight.lookup(plan.operation);
                plan.retry = self.lookup_receipt(plan.operation)?;
                plan.in_flight = if plan.retry.is_some() {
                    None
                } else {
                    in_flight
                };
                if !plan.is_owned() {
                    grant.slots += 1;
                    grant.add_partition(plan.owner, plan.validated.mutations().len())?;
                }
            }
            if let Some(sample) = sample {
                self.write_timeline
                    .event(sample, WriteTimelineStage::AdmissionWaitStarted);
            }
            let block_started = self.write_timeline.block_started();
            let credit = self
                .admission
                .reserve_grant(grant, block, || self.ensure_writable())?;
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
            // A retry must be validated against its owner before any allocation;
            // a vanished in-flight owner restarts the segment so the operation
            // is looked up (and, if unowned, allocated) afresh.
            let mut replan = false;
            for plan in plans.iter_mut() {
                if !self.validate_retry(plan)? {
                    replan = true;
                }
            }
            if replan {
                drop(queued);
                continue;
            }
            // Fresh plans need no work here: the sparse-view budget is a
            // standing reservation held by the publisher since open.
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
            let shards = self
                .dependencies
                .shard_indices(plans.iter().flat_map(|plan| plan.keys.iter()));
            let mut owners: SmallVec<[(usize, &OwnerQueue); 2]> = plans
                .iter()
                .filter(|plan| !plan.is_owned())
                .map(|plan| (plan.owner, self.clients[plan.owner].queue.as_ref()))
                .collect();
            if owners.is_empty() {
                owners.push((0, self.clients[0].queue.as_ref()));
            }
            // Option D: the owner-ordering stage is the caller's own allocation
            // in the key → owner-order critical section.
            if let Some(sample) = sample {
                self.write_timeline
                    .event(sample, WriteTimelineStage::OwnerEnqueueStarted);
            }
            let allocate_started = Instant::now();
            let block_started = self.write_timeline.block_started();
            // A DropCells allocation takes the publisher mutex outside its short
            // allocate transaction and records the barrier before release, so
            // no higher sequence can publish ahead of it. Order: publisher →
            // key shards → owner order → slot; it never takes Admission while
            // holding publisher.
            let mut range_publisher = drop_cells.then(|| self.publisher.lock());
            let owned_operations: SmallVec<[[u8; 16]; 1]> = plans
                .iter()
                .filter(|plan| plan.retry.is_none())
                .map(|plan| plan.operation)
                .collect();
            let allocated = self
                .in_flight
                .with_operations(&owned_operations, |in_flight| {
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
                            // Freeze every observed owner before allocating any
                            // member: a generation that appeared since the
                            // lookup must be waited on, not duplicated.
                            for plan in plans.iter() {
                                if plan.retry.is_some() {
                                    continue;
                                }
                                if in_flight.get(plan.operation) != plan.in_flight {
                                    return Ok(AllocationResult::Replan);
                                }
                            }
                            // A window probe made before the grant can be stale:
                            // a same-id generation may have published — and
                            // retired its in-flight entry — while this caller
                            // waited for admission. This section holds the
                            // in-flight shards of these operations, which is what
                            // excludes a concurrent allocation for the same id;
                            // a receipt visible here is therefore authoritative and
                            // the grant is replanned instead of allocating a second
                            // version that would overwrite a newer value.
                            for plan in plans.iter() {
                                if plan.retry.is_none()
                                    && plan.in_flight.is_none()
                                    && self.receipts.get(&plan.operation).is_some()
                                {
                                    return Ok(AllocationResult::Replan);
                                }
                            }
                            queued._credit.mark_allocated();
                            for plan in plans.iter_mut() {
                                if plan.is_owned() {
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
                                keys.register(
                                    &plan.keys,
                                    sequence,
                                    &mut data.predecessors,
                                    |prior| self.sequencer.resolved(prior),
                                );
                                data.keys.append(&mut plan.keys);
                                in_flight.insert(
                                    plan.operation,
                                    InFlightEntry {
                                        version,
                                        seq: sequence,
                                    },
                                );
                                plan.in_flight = Some(InFlightEntry {
                                    version,
                                    seq: sequence,
                                });
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
            // DropCells preflight: the slot waits for the contiguous lower
            // prefix before READY, so no earlier publication can create a cell
            // in the range. The range itself is applied by the native index at
            // its version; nothing is enumerated here.
            if drop_cells
                && let Some(plan) = plans.first()
                && let Some(allocation) = &plan.allocation
            {
                let sequence = allocation.sequence;
                if let Err(error) = self.wait_drop_cells_prefix(sequence, deadline) {
                    self.write_timeline
                        .wait_leave(WriteWait::AdmittedToAppendEnqueue, queued.owned.len());
                    self.abort_window(plans, &error)?;
                    return Err(error);
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
                    let routing = plan
                        .routing
                        .take()
                        .ok_or_else(|| LocalDbError::new("allocation lost its routing token"))?
                        .bind(&batch)
                        .map_err(LocalDbError::component)?;
                    let last_mutation = batch
                        .mutation_version(batch.mutations().len() - 1)
                        .ok_or_else(|| LocalDbError::new("empty admitted batch"))?;
                    plan.publication = Some(SlotPublication {
                        batch,
                        routing,
                        last_mutation,
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
                    operation: plan.operation,
                    records: plan.validated.mutations().len(),
                },
                copy_error(error),
            )?;
        }
        Ok(())
    }

    /// A plan whose operation is already owned — by the receipt window or by an
    /// in-flight generation — must carry the same payload as that owner. The
    /// version is never the caller's to choose: the window's receipt names it,
    /// and an in-flight generation names it through its slot.
    ///
    /// Returns `false` when the observed in-flight owner disappeared (aborted or
    /// its slot was reused): the plan's `in_flight` is then cleared, and the
    /// caller must re-look up and retry the segment instead of waiting on a
    /// generation that no longer owns this operation.
    fn validate_retry(&self, plan: &mut Planned<'_>) -> Result<bool, LocalDbError> {
        let Some(version) = self.owned_version(plan) else {
            return Ok(true);
        };
        let retry = plan
            .validated
            .encode(
                BatchId {
                    database: self.config.storage.database,
                    operation: plan.operation,
                },
                version,
            )
            .map_err(LocalDbError::component)?;
        let expected = match (&plan.retry, &plan.in_flight) {
            (Some(receipt), _) => Some(receipt.digest),
            (None, Some(entry)) => {
                match self.in_flight_digest(*entry, Instant::now() + self.config.timeout)? {
                    Some(digest) => Some(digest),
                    None => {
                        plan.in_flight = None;
                        return Ok(false);
                    }
                }
            }
            (None, None) => None,
        };
        if expected.is_some_and(|digest| digest != retry.digest()) {
            return Err(LocalDbError::new("retry changed original batch"));
        }
        Ok(true)
    }

    /// The digest of the generation that owns an in-flight operation, or `None`
    /// when that generation was already aborted, reused or retired (the caller
    /// then re-validates from the window or allocates afresh). Waits for the
    /// generation to reach READY, so an in-flight retry is compared against the
    /// original's encoded digest instead of silently accepted.
    fn in_flight_digest(
        &self,
        entry: InFlightEntry,
        deadline: Instant,
    ) -> Result<Option<PayloadDigest>, LocalDbError> {
        let slot = self.sequencer.slot(entry.seq);
        let mut data = slot.data.lock();
        loop {
            self.ensure_writable()?;
            if self.sequencer.generation(entry.seq) != Some(entry.seq) {
                return Ok(None);
            }
            match self.sequencer.resolution(entry.seq) {
                Some(Resolution::Ready | Resolution::Durable | Resolution::Published) => {
                    return Ok(data.digest);
                }
                Some(Resolution::Aborted) => return Ok(None),
                Some(Resolution::Unknown) => {
                    return Err(LocalDbError::unknown(
                        entry.version,
                        "retry outcome requires recovery",
                    ));
                }
                Some(Resolution::Allocated) => {}
                None => {
                    // A live in-flight entry names a generation this core no
                    // longer knows: the ownership bookkeeping is inconsistent,
                    // so continue only after recovery.
                    let error = LocalDbError::new("retry identity lost its slot generation");
                    self.fail(copy_error(&error));
                    return Err(error);
                }
            }
            if Instant::now() >= deadline {
                return Err(LocalDbError::unknown(
                    entry.version,
                    "retry encoding not resolved before deadline",
                ));
            }
            slot.done.wait_until(&mut data, deadline);
        }
    }

    /// The version an already-owned operation must return: the window's receipt
    /// version, else the in-flight generation's version.
    fn owned_version(&self, plan: &Planned<'_>) -> Option<BatchVersion> {
        match (&plan.retry, &plan.in_flight) {
            (Some(receipt), _) => Some(receipt.version),
            (None, Some(entry)) => Some(entry.version),
            (None, None) => None,
        }
    }

    /// Wait for the outcome of a generation another caller owns.
    ///
    /// A generation that is reused before this retry observes it has no outcome
    /// left to read, so the answer comes from what outlived it: the retry window.
    /// `Published` means the operation's receipt names exactly this version; an
    /// aborted original retires its in-flight ownership before the slot is
    /// reused and publishes no receipt, so `Reapply` is the only honest answer —
    /// never a success for a write that did not land.
    pub(super) fn wait_retry(
        &self,
        operation: [u8; 16],
        version: BatchVersion,
        deadline: Instant,
    ) -> Result<Retried, LocalDbError> {
        if version.epoch != self.epoch {
            self.ensure_healthy()?;
            return Ok(Retried::Published(version));
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
                // A reused slot means the original is at or beyond the window
                // edge, so re-applying is the documented out-of-window
                // behaviour, not a new one.
                let published = self
                    .receipts
                    .get(&operation)
                    .is_some_and(|receipt| receipt.version == version);
                return Ok(if published {
                    Retried::Published(version)
                } else {
                    Retried::Reapply
                });
            }
            if let Some(result) = &data.outcome {
                return match result {
                    Ok(version) => Ok(Retried::Published(*version)),
                    // An uncertain outcome is never silently re-applied: the
                    // original may have landed. Fail-stop is already caught by
                    // the health check above, so a definite error here is an
                    // abort before READY, and the caller re-applies as a fresh
                    // write (the abort had no effect).
                    Err(error) if error.uncertain_version.is_some() => Err(copy_error(error)),
                    Err(_) => Ok(Retried::Reapply),
                };
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

/// What a retry learned from the generation that owned its operation id.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Retried {
    /// The generation published this exact version.
    Published(BatchVersion),
    /// The generation ended without publishing: the caller re-applies as a
    /// fresh write instead of reporting a success that never happened.
    Reapply,
}

/// Preserve the exact message and uncertain version of a fail-stop cause.
pub(in crate::distributed::remote_db) fn copy_error(error: &LocalDbError) -> LocalDbError {
    let mut copy = LocalDbError::new(error.message());
    copy.uncertain_version = error.uncertain_version;
    copy
}
