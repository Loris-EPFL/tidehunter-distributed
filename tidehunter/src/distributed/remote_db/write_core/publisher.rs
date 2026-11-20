use super::super::stream_positions;
use super::super::{CompleteBatch, DurableBatch, LocalDbError, Mutation, WriteTimelineStage};
use super::core::WriteCore;
use super::sequencer::{Resolution, SlotPublication};
use crate::distributed::types::{BatchVersion, MutationVersion, PhysicalValueAddress};
use smallvec::SmallVec;
use std::sync::Arc;

struct Published {
    sequence: u64,
    batch: CompleteBatch,
    address: PhysicalValueAddress,
}

/// Min-heap of the durable generations one publication call still has to
/// resolve. A generation enters only as a receipt, or as a dependent of the
/// generation being published or aborted. Key-shard registration gives every
/// predecessor a lower sequence than its dependent, so each push exceeds the
/// current pop and pops strictly increase: every lower predecessor in the heap
/// resolves before its dependents, and a generation is popped at most once.
#[derive(Default)]
struct PublicationWork(SmallVec<[u64; 16]>);

impl PublicationWork {
    fn push(&mut self, sequence: u64) {
        let heap = &mut self.0;
        heap.push(sequence);
        let mut child = heap.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if heap[parent] <= heap[child] {
                break;
            }
            heap.swap(parent, child);
            child = parent;
        }
    }

    fn pop(&mut self) -> Option<u64> {
        let heap = &mut self.0;
        let last = heap.pop()?;
        let Some(first) = heap.first_mut() else {
            return Some(last);
        };
        let minimum = std::mem::replace(first, last);
        let mut parent = 0;
        loop {
            let left = 2 * parent + 1;
            if left >= heap.len() {
                break;
            }
            let right = left + 1;
            let child = if right < heap.len() && heap[right] < heap[left] {
                right
            } else {
                left
            };
            if heap[parent] <= heap[child] {
                break;
            }
            heap.swap(parent, child);
            parent = child;
        }
        Some(minimum)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Grant-time dimensions for an allocation that never became owner READY.
pub(super) struct AbortedAllocation {
    pub(super) sequence: u64,
    pub(super) owner_sequence: u64,
    pub(super) owner: usize,
    /// The operation id whose in-flight ownership this abort releases.
    pub(super) operation: [u8; 16],
    pub(super) records: usize,
}

impl WriteCore {
    pub(super) fn abort_allocated(
        self: &Arc<Self>,
        queue: &super::owner_queue::OwnerQueue,
        allocation: AbortedAllocation,
        error: LocalDbError,
    ) -> Result<(), LocalDbError> {
        let result = (|| {
            let sequence = allocation.sequence;
            if self.sequencer.resolution(sequence) != Some(Resolution::Allocated) {
                return Err(LocalDbError::new("abort has no allocated generation"));
            }
            // Remove ownership before ABORTED becomes visible to the contiguous
            // floor, so a racing retry cannot be left waiting on a dead slot: it
            // re-looks up outside this lock and allocates a fresh generation.
            self.in_flight.retire(allocation.operation, sequence);
            // D6: the generation becomes ABORTED before writer→publisher can
            // expose it to the contiguous floor, so no later publication pass
            // waits on it and no retry observes it as in flight.
            if queue.abort(allocation.owner_sequence) {
                self.wake_owner(allocation.owner)?;
            }
            self.publish_work(PublicationWork::default(), Some((allocation, error)))
        })();
        if let Err(error) = &result {
            self.fail(LocalDbError::new(error.message()));
        }
        result
    }

    pub(super) fn on_append_failed(&self, owner: u32, sequences: &[u64], error: LocalDbError) {
        let version = error.uncertain_version.or_else(|| {
            sequences.first().map(|&sequence| BatchVersion {
                epoch: self.epoch,
                sequence,
            })
        });
        super::super::record_first_failure(
            &self.first_failure,
            Some(owner),
            version,
            "owner_append",
            &error,
        );
        // An uncertain append can already have changed WAL bytes. It is never
        // an abort: fail finalization marks every unresolved generation UNKNOWN,
        // poisons value delivery and closes/wakes all admissions.
        self.fail(match version {
            Some(version) => LocalDbError::unknown(version, error),
            None => error,
        });
    }

    /// Runs on the owner worker, after the transport has released its contiguous
    /// completed flight prefix (including FUA/covering flush where required).
    pub(super) fn on_durable(self: &Arc<Self>, owner: u32, receipts: Vec<(u64, DurableBatch)>) {
        #[cfg(test)]
        {
            // One-shot pause on the worker before receipt validation and
            // publication. A dropped resume sender also unblocks.
            let pause = self.test_hooks.lock().before_publish.take();
            if let Some((entered, resume)) = pause {
                let _ = entered.try_send(());
                let _ = resume.recv();
            }
        }
        // The leased worker's ordered receipt release entered W3 for these
        // receipts (`release_leased_replies`); publication leaves it here.
        self.write_timeline
            .wait_leave(super::super::WriteWait::HarvestToDurable, receipts.len());
        if let Some(cut) = self.durable_cut.get()
            && let Some(&index) = self.partition_indexes.get(&owner)
            && let Some(last) = receipts.iter().map(|(sequence, _)| *sequence).max()
        {
            // Before the publisher lock; see `DurableCut::published`.
            cut.receipted(index, last.saturating_add(1));
        }
        let result = self.publish_durable(owner, receipts);
        if let Err(error) = result {
            self.fail(error);
        }
    }

    fn publish_durable(
        self: &Arc<Self>,
        owner: u32,
        receipts: Vec<(u64, DurableBatch)>,
    ) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        if !self.partition_indexes.contains_key(&owner) {
            return Err(LocalDbError::new("receipt names an unregistered owner"));
        }
        let mut work = PublicationWork::default();
        for (sequence, durable) in receipts {
            let receipt = durable.appended();
            let version = receipt.version();
            if version
                != (BatchVersion {
                    epoch: self.epoch,
                    sequence,
                })
                || receipt.address().partition != owner
                || sequence >= self.sequencer.next()
                || sequence < self.sequencer.reuse_next()
            {
                return Err(LocalDbError::unknown(
                    version,
                    "receipt outside active admission window",
                ));
            }
            let slot = self.sequencer.slot(sequence);
            let mut data = slot.data.lock();
            if self.sequencer.generation(sequence) != Some(sequence)
                || data.id != receipt.id()
                || data.digest != Some(receipt.digest())
                || data.publication.is_none()
                || data.receipt.is_some()
            {
                return Err(LocalDbError::unknown(
                    version,
                    "receipt differs from its admitted generation",
                ));
            }
            self.sequencer
                .transition(sequence, Resolution::Ready, Resolution::Durable)?;
            data.receipt = Some(durable);
            // The timeline session mutex is a leaf lock.
            self.write_timeline.batch_event(
                version,
                WriteTimelineStage::DurableObserved,
                None,
                None,
            );
            work.push(sequence);
        }
        self.publish_work(work, None)
    }

    fn publish_work(
        self: &Arc<Self>,
        mut work: PublicationWork,
        aborted: Option<(AbortedAllocation, LocalDbError)>,
    ) -> Result<(), LocalDbError> {
        let mut published = SmallVec::<[Published; 4]>::new();
        let mut publisher = self.publisher.lock();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.ensure_healthy()?;
            if let Some((allocation, error)) = &aborted {
                let sequence = allocation.sequence;
                let mut data = self.sequencer.slot(sequence).data.lock();
                if data.receipt.is_some() || data.publication.is_some() || data.outcome.is_some() {
                    return Err(LocalDbError::new(
                        "allocated abort already has write effects",
                    ));
                }
                self.sequencer
                    .transition(sequence, Resolution::Allocated, Resolution::Aborted)?;
                data.outcome = Some(Err(LocalDbError::new(error.message())));
                debug_assert!(
                    data.outcome.as_ref().is_some_and(|outcome| outcome
                        .as_ref()
                        .is_err_and(|error| error.uncertain_version.is_none())),
                    "an abort must install a definite outcome with no uncertain version",
                );
                if let Some(dependents) = publisher.dependents.remove(&sequence) {
                    for dependent in dependents {
                        work.push(dependent);
                    }
                }
                // A DropCells slot aborted before READY has no WAL effect:
                // release its barrier and requeue the deferred suffix.
                if publisher
                    .range_barrier
                    .as_ref()
                    .is_some_and(|barrier| barrier.sequence == sequence)
                    && let Some(barrier) = publisher.range_barrier.take()
                {
                    for deferred in barrier.deferred_ready {
                        work.push(deferred);
                    }
                }
            }
            // Pass 1 publishes below any range barrier, pass 2 the range once
            // its prefix is contiguous, pass 3 the suffix it deferred. Without
            // a barrier only pass 1 runs. Within a pass, pops strictly
            // increase (PublicationWork) and every popped generation is
            // durable, unresolved and inside the admitted window
            // [reuse_next, next), whose length is at most the ring capacity.
            // The bound therefore covers all work; leftover work or a
            // non-increasing pop is an invariant violation and fail-stops.
            for _pass in 0..3 {
                let mut previous = None;
                for _ in 0..self.sequencer.capacity() {
                    let Some(sequence) = work.pop() else { break };
                    if previous.is_some_and(|previous| sequence <= previous) {
                        return Err(LocalDbError::new(
                            "publication work did not increase in sequence order",
                        ));
                    }
                    previous = Some(sequence);
                    if self.sequencer.resolved(sequence) {
                        continue;
                    }
                    if self.sequencer.resolution(sequence) != Some(Resolution::Durable) {
                        return Err(LocalDbError::new(
                            "publication work has no durable generation",
                        ));
                    }
                    let published_next = publisher.published_next;
                    if let Some(barrier) = &mut publisher.range_barrier {
                        if sequence > barrier.sequence {
                            // Durable, unpublished: requeued when the range resolves.
                            barrier.deferred_ready.push(sequence);
                            continue;
                        }
                        if sequence == barrier.sequence && published_next != sequence {
                            // Rearmed below once the lower prefix is contiguous.
                            continue;
                        }
                    }
                    let slot = self.sequencer.slot(sequence);
                    let mut data = slot.data.lock();
                    if let Some(predecessor) = data
                        .predecessors
                        .iter()
                        .copied()
                        .find(|predecessor| !self.sequencer.resolved(*predecessor))
                    {
                        publisher
                            .dependents
                            .entry(predecessor)
                            .or_default()
                            .push(sequence);
                        continue;
                    }
                    let durable = data
                        .receipt
                        .ok_or_else(|| LocalDbError::new("durable slot has no receipt"))?;
                    let SlotPublication {
                        batch,
                        routing,
                        last_mutation,
                    } = data.publication.take().ok_or_else(|| {
                        LocalDbError::new("durable slot has no publication payload")
                    })?;
                    if data.last_mutation != Some(last_mutation) {
                        return Err(LocalDbError::new(
                            "publication mutation frontier differs from its slot",
                        ));
                    }
                    // The position key is the address's (partition, segment
                    // generation). It must be a retained fencing generation of that
                    // owner, or the entry this publication creates is one no scan
                    // reads and the stream's files could be retired unseen: fail the
                    // database before anything publishes.
                    {
                        let address = durable.appended().address();
                        stream_positions::admit_stream(
                            &publisher.positions,
                            &publisher.retained_epochs,
                            &self.partition_indexes,
                            address.partition,
                            address.segment_generation,
                        )?;
                    }
                    #[cfg(test)]
                    crate::failpoints::write_model::boundary("publication_before");
                    // Test-only leaf mutex: injects a native publication callback
                    // error for one generation, under writer→publisher as in production.
                    #[cfg(test)]
                    if self.test_hooks.lock().fail_publication == Some(sequence) {
                        return Err(LocalDbError::new("injected publication callback failure"));
                    }
                    routing
                        .publish(&batch, durable, &self.table)
                        .map_err(LocalDbError::component)?;
                    #[cfg(test)]
                    crate::failpoints::write_model::boundary("publication_after");
                    self.sequencer.transition(
                        sequence,
                        Resolution::Durable,
                        Resolution::Published,
                    )?;
                    // The retry window answers a repeated operation id with this
                    // receipt instead of appending the batch again.
                    publisher.recent.push(super::super::RecentReceipt {
                        id: batch.id(),
                        version: batch.version(),
                        digest: batch.digest(),
                    });
                    publisher.upper = Some(
                        publisher
                            .upper
                            .map_or(last_mutation, |upper| upper.max(last_mutation)),
                    );
                    {
                        let state = publisher.checkpoint_state();
                        *state.live_latest = Some(
                            state
                                .live_latest
                                .map_or(last_mutation, |previous| previous.max(last_mutation)),
                        );
                        *state.checkpoint_tail_batches += 1;
                        *state.checkpoint_tail_bytes += batch.encoded().len();
                        // The relocation scan and file retirement read these: the
                        // stream's durable end only grows. Written here and at open
                        // only, always under the publisher lock; the stream was
                        // admitted before the batch published.
                        let address = durable.appended().address();
                        stream_positions::record_extent(
                            state.positions,
                            address.partition,
                            address.segment_generation,
                            address.offset,
                            address.len,
                        );
                    }
                    published.push(Published {
                        sequence,
                        batch,
                        address: durable.appended().address(),
                    });
                    if let Some(dependents) = publisher.dependents.remove(&sequence) {
                        for dependent in dependents {
                            work.push(dependent);
                        }
                    }
                }
                if !work.is_empty() {
                    return Err(LocalDbError::new(
                        "publication dependency work exceeded the admitted window",
                    ));
                }
                for _ in 0..self.sequencer.capacity() {
                    let sequence = publisher.published_next;
                    match self.sequencer.resolution(sequence) {
                        Some(Resolution::Published) => {
                            let data = self.sequencer.slot(sequence).data.lock();
                            let last = data.last_mutation.ok_or_else(|| {
                                LocalDbError::new("published slot lost its last mutation")
                            })?;
                            *publisher.checkpoint_state().latest = Some(last);
                        }
                        Some(Resolution::Aborted) => {}
                        _ => break,
                    }
                    publisher.published_next = sequence + 1;
                }
                match publisher
                    .range_barrier
                    .as_ref()
                    .map(|barrier| barrier.sequence)
                {
                    // The range published: release the barrier, requeue its suffix.
                    Some(range) if publisher.published_next > range => {
                        if let Some(barrier) = publisher.range_barrier.take() {
                            for deferred in barrier.deferred_ready {
                                work.push(deferred);
                            }
                        }
                    }
                    // The lower prefix became contiguous with the range durable.
                    Some(range)
                        if publisher.published_next == range
                            && self.sequencer.resolution(range) == Some(Resolution::Durable) =>
                    {
                        work.push(range);
                    }
                    _ => {}
                }
                if work.is_empty() {
                    break;
                }
            }
            if !work.is_empty() {
                return Err(LocalDbError::new(
                    "publication barrier work exceeded its passes",
                ));
            }
            for _ in 0..self.sequencer.capacity() {
                let sequence = publisher.published_next;
                match self.sequencer.resolution(sequence) {
                    Some(Resolution::Published) => {
                        let data = self.sequencer.slot(sequence).data.lock();
                        let last = data.last_mutation.ok_or_else(|| {
                            LocalDbError::new("published slot lost its last mutation")
                        })?;
                        *publisher.checkpoint_state().latest = Some(last);
                    }
                    Some(Resolution::Aborted) => {}
                    _ => break,
                }
                publisher.published_next = sequence + 1;
            }
            if let Some(cut) = self.durable_cut.get() {
                cut.published(publisher.published_next);
            }
            // Every member of every batch below `published_next` is inserted
            // into the native index; the aborted generations in between have no
            // effect. This is the loader's contiguous applied cut and the
            // exclusive flush bound (plan I2).
            self.table.advance_applied(BatchVersion {
                epoch: self.epoch,
                sequence: publisher.published_next,
            });
            self.floor_changed.notify_all();
            Ok(())
        }))
        .unwrap_or_else(|_| Err(LocalDbError::new("native publication callback panicked")));
        if let Err(error) = &result {
            self.begin_failure(error);
        }
        drop(publisher);
        if self.write_timeline.enabled() {
            for entry in &published {
                self.write_timeline.batch_event(
                    entry.batch.version(),
                    WriteTimelineStage::Published,
                    None,
                    None,
                );
            }
        }
        if result.is_err() {
            self.finish_failure();
        }
        let mut slots = 0usize;
        let mut partitions = SmallVec::<[(usize, usize); 4]>::new();
        partitions.resize(self.partition_indexes.len(), (0, 0));
        if let Some((allocation, _)) = &aborted {
            partitions[allocation.owner].0 += 1;
            partitions[allocation.owner].1 += allocation.records;
            slots += self.finalize_slot(allocation.sequence);
        }
        let published_count = published.len() as u64;
        // Counted before `finalize_published` installs any caller outcome, so
        // an acknowledged write always observes its own publication (as the
        // suffix counter above).
        self.stats.lock().database.published_batches += published_count;
        for entry in published {
            let partition = *self
                .partition_indexes
                .get(&entry.address.partition)
                .ok_or_else(|| LocalDbError::new("published batch lost its owner"))?;
            partitions[partition].0 += 1;
            partitions[partition].1 += entry.batch.mutations().len();
            slots += self.finalize_published(entry)?;
        }
        self.admission.release_publication(slots, &partitions)?;
        result
    }

    fn finalize_published(&self, entry: Published) -> Result<usize, LocalDbError> {
        let Published {
            sequence,
            batch,
            address,
        } = entry;
        if self.config.read_cache_bytes != 0 {
            if let Some((keyspace, _, _)) = batch.drop_cells() {
                // No warming for DropCells. The index drop hides the covered rows
                // from reads; the keyspace's older cached values are evicted
                // before the caller completes (conservative: a hit is never
                // authoritative). Writer and publisher are released; one cache
                // shard at a time.
                self.read_cache.evict_dropped(keyspace, batch.version());
            } else if !batch.is_relocated() {
                // A relocated copy is not indexed, so nothing would read it by
                // its own identity; warming would only evict live entries.
                for (operation, mutation) in batch.mutations().iter().enumerate() {
                    let Mutation::Put {
                        keyspace, value, ..
                    } = mutation
                    else {
                        continue;
                    };
                    let descriptor = batch
                        .record_reference(address, operation)
                        .map_err(LocalDbError::component)?
                        .descriptor;
                    let identity = MutationVersion {
                        batch: batch.version(),
                        operation_index: descriptor.operation_index,
                    };
                    let prepared = self
                        .read_cache
                        .prepare_post_publish(identity, keyspace.0, value);
                    self.read_cache.insert_prepared(prepared);
                }
            }
        }
        // The receipt is already in the window (pushed under publisher), so the
        // in-flight entry can go: a retry now answers from the window, and until
        // this point both existed. The in-flight-first lookup sees one of them at
        // every instant, which is what makes retiring after the push safe.
        self.in_flight.retire(batch.id().operation, sequence);
        let slot = self.sequencer.slot(sequence);
        let mut data = slot.data.lock();
        if data.outcome.is_none() {
            data.outcome = Some(
                self.ensure_healthy()
                    .map(|()| batch.version())
                    .map_err(|error| LocalDbError::unknown(batch.version(), error)),
            );
            debug_assert!(
                data.outcome.as_ref().is_some_and(|outcome| outcome.is_ok()
                    || outcome
                        .as_ref()
                        .is_err_and(|error| error.uncertain_version.is_some())),
                "a published outcome is Ok or carries an uncertain version",
            );
        }
        drop(data);
        let slots = self.finalize_slot(sequence);
        // finalize_slot notified the caller: the old reply-queued point.
        self.write_timeline.batch_event(
            batch.version(),
            WriteTimelineStage::CallerReplyQueued,
            None,
            None,
        );
        Ok(slots)
    }

    fn finalize_slot(&self, sequence: u64) -> usize {
        let slot = self.sequencer.slot(sequence);
        let mut data = slot.data.lock();
        slot.done.notify_all();
        let mut keys = std::mem::take(&mut data.keys);
        drop(data);
        self.dependencies.remove(&keys, sequence);
        keys.clear();
        slot.data.lock().keys = keys;
        // FINALIZED follows outcome, notification and dependency cleanup. The
        // caller may already have released, but cannot reuse this generation.
        self.sequencer.complete_resolve(sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::PublicationWork;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    /// Interleaved pushes above the current pop, as dependents arrive while
    /// their predecessor publishes, always pop in strictly increasing order.
    #[test]
    fn publication_work_pops_in_increasing_sequence_order() {
        let mut rng = StdRng::seed_from_u64(62);
        for _ in 0..256 {
            let mut work = PublicationWork::default();
            let mut expected: Vec<u64> = Vec::new();
            let mut next = 0u64;
            for _ in 0..rng.gen_range(1..32) {
                next += rng.gen_range(1..4);
                expected.push(next);
            }
            let (initial, mut later) = expected.split_at(rng.gen_range(1..=expected.len()));
            let mut shuffled = initial.to_vec();
            shuffled.shuffle(&mut rng);
            for &sequence in &shuffled {
                work.push(sequence);
            }
            let mut popped = Vec::with_capacity(expected.len());
            while let Some(sequence) = work.pop() {
                popped.push(sequence);
                let take = rng.gen_range(0..=later.len().min(3));
                let (dependents, rest) = later.split_at(take);
                for &dependent in dependents.iter().rev() {
                    work.push(dependent);
                }
                later = rest;
            }
            // The heap drained before every later sequence was pushed.
            for &sequence in later {
                work.push(sequence);
            }
            while let Some(sequence) = work.pop() {
                popped.push(sequence);
            }
            assert_eq!(popped, expected);
        }
    }
}
