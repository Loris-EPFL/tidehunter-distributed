use super::super::checkpoint_attribution::{TailCount, diagnostics_clock_ns};
use super::super::{CompleteBatch, DurableBatch, IndexReadCut, LocalDbError, WriteTimelineStage};
use super::core::WriteCore;
use super::identity::IdentityClass;
use super::sequencer::{Resolution, SlotPublication};
use crate::distributed::types::{BatchVersion, PhysicalValueAddress};
use smallvec::SmallVec;
use std::sync::Arc;

struct Published {
    sequence: u64,
    batch: CompleteBatch,
    address: PhysicalValueAddress,
    metadata_charge: usize,
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
    pub(super) metadata_charge: usize,
    pub(super) records: usize,
    pub(super) encoded_bytes: usize,
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
            let id = {
                let data = self.sequencer.slot(sequence).data.lock();
                if self.sequencer.resolution(sequence) != Some(Resolution::Allocated) {
                    return Err(LocalDbError::new("abort has no allocated generation"));
                }
                data.id
            };
            let removed = self.identities.with_operation(id.operation, |entries| {
                if entries.get(&id.operation).is_some_and(|entry| {
                    entry.seq == sequence && entry.class == IdentityClass::Active
                }) {
                    entries.remove(&id.operation);
                    true
                } else {
                    false
                }
            });
            if !removed {
                return Err(LocalDbError::new("abort lost its active identity"));
            }
            // D6: remove the identity and return its sole identity credit before
            // writer→publisher can expose ABORTED to the contiguous floor.
            // A checkpoint covering this sequence therefore cannot retire that
            // identity again. A racing retry is a fresh, effect-free allocation.
            self.release_checkpoint_credits(super::core::CheckpointCredits {
                identities: 1,
                ..super::core::CheckpointCredits::default()
            })?;
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
        let mut writer = self.index.writer();
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
                        metadata_charge,
                        last_mutation,
                    } = data.publication.take().ok_or_else(|| {
                        LocalDbError::new("durable slot has no publication payload")
                    })?;
                    if data.last_mutation != Some(last_mutation) {
                        return Err(LocalDbError::new(
                            "publication mutation frontier differs from its slot",
                        ));
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
                        .publish_authenticated(&batch, durable, &mut writer)
                        .map_err(LocalDbError::component)?;
                    #[cfg(test)]
                    crate::failpoints::write_model::boundary("publication_after");
                    self.sequencer.transition(
                        sequence,
                        Resolution::Durable,
                        Resolution::Published,
                    )?;
                    publisher.upper = Some(
                        publisher
                            .upper
                            .map_or(last_mutation, |upper| upper.max(last_mutation)),
                    );
                    {
                        let state = publisher.checkpoint_state();
                        if state.known.insert(data.id, durable.appended()).is_some() {
                            return Err(LocalDbError::new(
                                "publication repeated a retained identity",
                            ));
                        }
                        *state.live_latest = Some(
                            state
                                .live_latest
                                .map_or(last_mutation, |previous| previous.max(last_mutation)),
                        );
                        *state.checkpoint_tail_batches += 1;
                        *state.checkpoint_tail_bytes += batch.encoded().len();
                        // Record each publication exactly where it charges the suffix.
                        if let Some(suffix) = state.index_suffix.as_mut() {
                            suffix.on_publish(
                                diagnostics_clock_ns(),
                                TailCount::new(
                                    *state.checkpoint_tail_batches,
                                    *state.checkpoint_tail_bytes,
                                ),
                                self.index.charged_metadata_bytes(),
                            );
                        }
                    }
                    published.push(Published {
                        sequence,
                        batch,
                        address: durable.appended().address(),
                        metadata_charge,
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
                writer
                    .promote_committed()
                    .map_err(LocalDbError::component)?;
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
            writer
                .promote_committed()
                .map_err(LocalDbError::component)?;
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
            if let Some(upper) = publisher
                .upper
                .filter(|upper| upper.batch.epoch == self.epoch)
            {
                let mut excluded = Vec::new();
                for sequence in (publisher.published_next..=upper.batch.sequence)
                    .take(self.sequencer.capacity())
                {
                    if self.sequencer.resolution(sequence) != Some(Resolution::Published) {
                        if excluded.capacity() == 0 {
                            // Allocate only for a real sparse view, and never
                            // let geometric Vec growth exceed its reservation.
                            excluded = Vec::with_capacity(self.sequencer.capacity() - 1);
                        }
                        excluded.push(BatchVersion {
                            epoch: self.epoch,
                            sequence,
                        });
                    }
                }
                if excluded.is_empty() {
                    writer.publish_latest();
                } else if !published.is_empty() || aborted.is_some() {
                    let floor = publisher
                        .checkpoint_state()
                        .latest
                        .map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
                    // The standing budget already holds this view's quota at
                    // its worst-case size: no capacity failure after the WAL
                    // write. Only invalid-view programming errors can fail.
                    writer
                        .install_standing_live_view(upper, floor, excluded, &publisher.view_budget)
                        .map_err(LocalDbError::component)?;
                }
            }
            self.floor_changed.notify_all();
            Ok(())
        }))
        .unwrap_or_else(|_| Err(LocalDbError::new("native publication callback panicked")));
        if let Err(error) = &result {
            self.begin_failure(error);
        }
        // Sampled under publisher: batches published while a background
        // checkpoint is in flight form its acknowledged suffix.
        let during_checkpoint = publisher.background_checkpoint.is_some();
        drop(publisher);
        drop(writer);
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
        if during_checkpoint && !published.is_empty() {
            // Counted before any caller outcome is installed below, so an
            // acknowledged write always observes its own suffix contribution.
            self.stats.lock().write.background_checkpoint_suffix_batches += published.len() as u64;
        }
        if result.is_err() {
            self.finish_failure();
        }
        let mut index_bytes = 0usize;
        let mut slots = 0usize;
        let mut partitions = SmallVec::<[(usize, usize); 4]>::new();
        partitions.resize(self.partition_indexes.len(), (0, 0));
        let mut aborted_replay = (0, 0);
        if let Some((allocation, _)) = &aborted {
            index_bytes += allocation.metadata_charge;
            partitions[allocation.owner].0 += 1;
            partitions[allocation.owner].1 += allocation.records;
            aborted_replay = (1, allocation.encoded_bytes);
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
            index_bytes += entry.metadata_charge;
            partitions[partition].0 += 1;
            partitions[partition].1 += entry.batch.mutations().len();
            slots += self.finalize_published(entry)?;
        }
        self.admission
            .release_publication(index_bytes, slots, &partitions, aborted_replay)?;
        result?;
        self.request_checkpoint()
    }

    fn finalize_published(&self, entry: Published) -> Result<usize, LocalDbError> {
        let Published {
            sequence,
            batch,
            address,
            metadata_charge: _,
        } = entry;
        if self.config.read_cache_bytes != 0 {
            if let Some((keyspace, first, last)) = batch.drop_cells() {
                // No warming for DropCells. Masks already hide the covered
                // references; evict them before the caller completes. Writer
                // and publisher are released; one cache shard at a time.
                let descriptor = self.shape.ks(keyspace);
                let key_size = descriptor.index_key_size();
                self.read_cache
                    .evict_dropped(keyspace, batch.version(), |key: &[u8]| {
                        let reduced = descriptor.reduce_key(key);
                        if Some(reduced.len()) != key_size {
                            return false;
                        }
                        let cell = descriptor.cell_id(&reduced);
                        *first <= cell && cell <= *last
                    });
            } else {
                for (operation, mutation) in batch.mutations().iter().enumerate() {
                    let reference = batch
                        .record_reference(address, operation)
                        .map_err(LocalDbError::component)?;
                    let prepared = self.read_cache.prepare_post_publish(reference, mutation);
                    self.read_cache.insert_prepared(prepared);
                }
            }
        }
        self.identities
            .with_operation(batch.id().operation, |entries| {
                if let Some(identity) = entries.get_mut(&batch.id().operation)
                    && identity.seq == sequence
                    && identity.class == IdentityClass::Active
                {
                    // Checkpoint may already have moved this identity to Recent;
                    // never restore its retired identity charge by moving it back.
                    identity.class = IdentityClass::Suffix;
                }
            });
        let slot = self.sequencer.slot(sequence);
        let mut data = slot.data.lock();
        if data.outcome.is_none() {
            data.outcome = Some(
                self.ensure_healthy()
                    .map(|()| batch.version())
                    .map_err(|error| LocalDbError::unknown(batch.version(), error)),
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
