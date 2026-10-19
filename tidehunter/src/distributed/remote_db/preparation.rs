//! Bounded CPU preparation outside the ordered admission/publication step.
//!
//! TiKV Raft Engine `Engine::write` prepares each log batch before entering its
//! write barrier. DistributedANN §2.3 motivates parallel independent computation
//! around small orchestration; neither source supplies our commit protocol.
//! Tidehunter §§3.1/3.3 remain the authority: allocation is not completion, whole
//! batches stay intact, and no failed preparation is allowed to create a hole.

use super::*;
use rayon::prelude::*;

impl RemoteDatabase {
    /// Prepare a bounded borrowed window in parallel, then admit in input order.
    /// Caller-owned inputs are borrowed only until this synchronous call returns.
    /// Every retained result owns canonical bytes, never the caller's backing.
    ///
    /// Validation, retry conflicts and capacity errors discovered by preflight
    /// admit none of this window. Storage failures during admission can leave an
    /// accepted/uncertain prefix: they are not a definite abort. `drain` remains
    /// the durable acknowledgement; this call never acknowledges persistence.
    pub fn submit_many(
        &mut self,
        batches: &[([u8; 16], Vec<Mutation>)],
    ) -> Result<Vec<BatchVersion>, LocalDbError> {
        let started = Instant::now();
        let before = self.authority_write_stats();
        let result = self.submit_many_inner(batches);
        self.record_admission_span(started, before);
        result
    }

    fn submit_many_inner(
        &mut self,
        batches: &[([u8; 16], Vec<Mutation>)],
    ) -> Result<Vec<BatchVersion>, LocalDbError> {
        self.ensure_healthy()?;
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        if batches.len() > self.config.max_pending_batches {
            return Err(LocalDbError::new("preparation window exceeds batch budget"));
        }
        if self.config.pipelined_writes && self.inflight.iter().any(Option::is_some) {
            self.progress_pipeline(false)?;
        }
        let validation_started = Instant::now();
        let validation_before = self.stats;
        let mut plans = Vec::with_capacity(batches.len());
        let mut planned = HashMap::new();
        let mut next = self.submission_next();
        let mut reserved = self.pending_bytes;
        for (operation, mutations) in batches {
            let id = BatchId {
                database: self.config.storage.database,
                operation: *operation,
            };
            let version = if let Some((version, _)) = self.existing_identity(id)? {
                version
            } else if let Some(version) = planned.get(&id) {
                *version
            } else {
                let version = BatchVersion {
                    epoch: self.epoch,
                    sequence: next,
                };
                next = next
                    .checked_add(1)
                    .ok_or_else(|| LocalDbError::new("version overflow"))?;
                planned.insert(id, version);
                version
            };
            let validated = CompleteBatch::validated_mutations(
                &self.shape,
                mutations,
                self.config.storage.batch_limits,
            )
            .map_err(LocalDbError::component)?;
            let encoded = validated.encoded_len();
            if planned.contains_key(&id) && encoded > CHECKPOINT_MAX_TAIL_BYTES {
                return Err(LocalDbError::new(
                    "batch exceeds checkpoint tail byte bound",
                ));
            }
            // Covers the canonical result, temporary mutation/descriptor vectors,
            // actor metadata copy, plan/result slots and Vec/Bytes owner slack.
            // Existing borrowed application buffers are not newly retained here.
            let credit = mutations
                .len()
                .checked_mul(
                    2 * (std::mem::size_of::<Mutation>()
                        + std::mem::size_of::<super::super::types::RecordDescriptor>())
                        + std::mem::size_of::<crate::batch::PendingOp>(),
                )
                // Canonical bytes plus worst-case spilled cell-prefix backing.
                .and_then(|n| n.checked_add(encoded))
                .and_then(|n| n.checked_add(validated.routing_heap_bound()))
                .and_then(|n| n.checked_add(2 * std::mem::size_of::<CompleteBatch>() + 640))
                .ok_or_else(|| LocalDbError::new("preparation byte overflow"))?;
            reserved = reserved
                .checked_add(credit)
                .filter(|n| *n <= self.config.max_pending_bytes)
                .ok_or_else(|| LocalDbError::new("preparation window exceeds byte budget"))?;
            plans.push((id, version, validated));
        }
        let published = self.publication.published_prefix().next_sequence();
        if next - published > self.config.max_admitted_batches as u64
            || next - published > self.config.max_pending_batches as u64
        {
            return Err(LocalDbError::new(
                "preparation window exceeds admission capacity",
            ));
        }
        self.pipeline_stats.prepare_reserved_bytes_peak = self
            .pipeline_stats
            .prepare_reserved_bytes_peak
            .max(reserved);
        type PlannedInput<'a> = &'a (
            BatchId,
            BatchVersion,
            super::super::codec::ValidatedMutations<'a>,
        );
        let prepare = |(id, version, validated): PlannedInput<'_>| {
            let began = Instant::now();
            let batch = validated
                .encode(*id, *version)
                .map_err(LocalDbError::component)?
                .into_canonical_owned();
            Ok::<_, LocalDbError>((batch, elapsed(began)))
        };
        let began = Instant::now();
        #[cfg(test)]
        let gate = self.before_preparation.take();
        let prepared: Vec<_> = if let Some(pool) = self
            .preparation_pool
            .as_ref()
            .and_then(|owner| owner.pool.clone())
        {
            let build = || {
                #[cfg(test)]
                if let Some((entered, resume)) = gate {
                    let _ = entered.send(());
                    let _ = resume.recv();
                }
                plans.par_iter().map(prepare).collect::<Result<Vec<_>, _>>()
            };
            if self.config.pipelined_writes && !self.submissions.is_empty() {
                // TiKV-style stage separation: preparation of this window must
                // not prevent already-admitted complete groups from progressing.
                // Scoped work borrows the caller only until this call returns;
                // no new logical slot is allocated until ALL preflight succeeds.
                let (sender, receiver) = mpsc::sync_channel(1);
                let wake = self.preparation_wake.clone();
                let before = self.pipeline_stats.completions_before_drain;
                let result = pool.in_place_scope(|scope| {
                    scope.spawn(move |_| {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(build))
                            .unwrap_or_else(|_| {
                                Err(LocalDbError::new("batch preparation panicked"))
                            });
                        let _ = sender.send(result);
                        // Same bounded hint channel as storage completions.
                        // Send AFTER the result; coalescing cannot lose authority.
                        let _ = wake.try_send(());
                    });
                    loop {
                        self.pipeline_stats.prepare_progress_passes += 1;
                        self.progress_pipeline(false)?;
                        match receiver.try_recv() {
                            Ok(result) => break result,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                break Err(LocalDbError::new("batch preparation lost result"));
                            }
                            Err(mpsc::TryRecvError::Empty) => {}
                        }
                        // Wait on either preparation or storage. Never spin or
                        // infer an abort from timeout; recheck authoritative replies.
                        let _ = self
                            .completion_wake
                            .recv_timeout(Duration::from_millis(100));
                    }
                });
                self.pipeline_stats.prepare_progress_completions +=
                    self.pipeline_stats.completions_before_drain - before;
                result?
            } else {
                pool.install(build)?
            }
        } else {
            plans.iter().map(prepare).collect::<Result<Vec<_>, _>>()?
        };
        self.pipeline_stats.prepare_calls += 1;
        self.pipeline_stats.prepare_wall_ns += elapsed(began);
        self.pipeline_stats.prepare_task_ns += prepared.iter().map(|(_, ns)| ns).sum::<u64>();
        if self.preparation_pool.is_some() && batches.len() > 1 {
            self.pipeline_stats.parallel_prepared_batches += batches.len() as u64;
        }
        let prepared = prepared
            .into_iter()
            .map(|(batch, _)| self.prepare_admission(batch))
            .collect::<Result<Vec<_>, _>>()?;

        // Check ALL conflicts and reservations before allocating any logical slot.
        // Distinct equal retries share their original version, including within
        // this window. Distinct contents under one identity are always rejected.
        let mut seen = HashMap::new();
        let mut index_bytes = self.reserved_index_bytes;
        let mut pending_bytes = self.pending_bytes;
        for prepared in &prepared {
            let batch = &prepared.batch;
            let id = batch.id();
            let old = self
                .existing_identity(id)?
                .map(|(_, digest)| digest)
                .or_else(|| seen.get(&id).copied());
            if let Some(digest) = old {
                if digest != batch.digest() {
                    return Err(LocalDbError::new("retry changed original batch"));
                }
                continue;
            }
            seen.insert(id, batch.digest());
            pending_bytes = pending_bytes
                .checked_add(prepared.credit)
                .filter(|bytes| *bytes <= self.config.max_pending_bytes)
                .ok_or_else(|| LocalDbError::new("pending byte budget reached"))?;
            // A window may span successive spillable groups on one node. Check
            // every indivisible batch now; actual group construction enforces
            // the combined fresh-window count/descriptor budget before I/O.
            self.check_storage_group_capacity(1, batch.mutations().len())?;
            index_bytes = index_bytes
                .checked_add(prepared.metadata_bytes)
                .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
        }
        let needs_checkpoint = self.index.read().check_capacity(index_bytes).is_err();
        if needs_checkpoint {
            let previously_reserved = self.reserved_index_bytes;
            self.force_admission_checkpoint()?;
            index_bytes -= previously_reserved;
        }
        self.index
            .read()
            .check_capacity(index_bytes)
            .map_err(LocalDbError::component)?;
        self.check_checkpoint_capacity(seen.len())?;
        // Progress may publish older work while preparation is running.
        let nested = (self.stats.storage_wait_ns - validation_before.storage_wait_ns)
            .saturating_add(self.stats.publication_ns - validation_before.publication_ns)
            .saturating_add(self.stats.dispatch_ns - validation_before.dispatch_ns);
        self.stats.validation_ns += elapsed(validation_started).saturating_sub(nested);
        // Move results one by one; do not keep extra owners after publication
        // refunds credit. Input order is retained regardless of worker schedule.
        prepared
            .into_iter()
            .map(|prepared| self.admit_prepared(prepared))
            .collect()
    }
}
