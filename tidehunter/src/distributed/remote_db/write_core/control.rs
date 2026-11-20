use super::super::{
    AuthorityWriteStats, CheckpointDiagnosticSummary, LocalDbError, LocalDbStats, PipelineStats,
    WriteProfileSnapshot, elapsed, failure_context, receive,
};
use super::completion::DrainStop;
use super::core::WriteCore;
use crate::distributed::hybrid::StoredSnapshot;
use crate::distributed::types::BatchVersion;
use std::collections::VecDeque;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

/// Lags of waiting a relocation copy's durability is allowed; see
/// [`WriteCore::await_copies_durable`].
const COPY_DURABLE_LAG_MULTIPLE: u32 = 8;
/// Hard cap of the wait: it runs while a relocation cell latch is held, so a
/// large `lag_ms` must not turn it into an hours-long stall.
const COPY_DURABLE_WAIT_CAP: Duration = Duration::from_secs(120);

impl WriteCore {
    pub(in crate::distributed::remote_db) fn drain(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
            .map_err(|error| self.with_first_failure_version(error))?;
        let next = self.sequencer.next();
        #[cfg(test)]
        if self.publisher.lock().published_next < next {
            let entered = self.test_hooks.lock().drain_wait_entered.take();
            if let Some(entered) = entered {
                let _ = entered.try_send(());
            }
        }
        self.wait_published_before(next)
    }

    /// Native `flush`: waits for the published prefix, then runs one native
    /// snapshot pass. The pass stores at `min(durable, applied, held latch)`, so a
    /// native-tier flush does not claim more than the owners made durable (A25).
    /// A failed pass is fail-stop, as native's snapshot thread is.
    pub(in crate::distributed::remote_db) fn flush_index(
        self: &Arc<Self>,
    ) -> Result<StoredSnapshot, LocalDbError> {
        let next = self.sequencer.next();
        self.wait_published_before(next)?;
        let result = self.table.snapshot();
        if let Err(error) = &result {
            self.fail(LocalDbError::new(error.message()));
        }
        result
    }

    pub(in crate::distributed::remote_db) fn flush_durable(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        let next = self.sequencer.next();
        self.wait_published_before(next)?;
        let mut owners: Vec<_> = self
            .clients
            .iter()
            .map(|worker| worker.control_handle())
            .collect();
        super::super::endpoint_map(&mut owners, |_, owner| {
            owner.control(|client| client.status().map(|_| ()))
        })?;
        // Every sequence below `next` is now published and synced, or aborted:
        // the explicit flush survives recovery only once the record says so.
        if let Some(cut) = self.durable_cut.get() {
            cut.certify(next)?;
        }
        self.ensure_healthy()
    }

    /// Blocks until the owners made every relocation copy up to `last` durable
    /// and the durable cut says so; the relocation pass calls it before it
    /// re-points the index at those copies.
    ///
    /// A re-point keeps the entry's original version, so no cut check on the
    /// index ever looks at the copy's version: the invariant "an index entry
    /// never names a position that is not durable-proven" has to be enforced
    /// here. On the native tier an owner acknowledges at its page cache, so the
    /// copy can be lost by a crash that the old bytes survive; re-pointing first
    /// and retiring the old file would then lose the value. A synchronous owner
    /// (no durable cut) acknowledged at fsync: nothing to wait for.
    ///
    /// The wait is active, not passive: it asks every owner to sync now
    /// ([`DurableCut::wait_through`]), because an idle native tier would
    /// otherwise sync only when the cut lag reached `lag_ms`/`lag_bytes`. It
    /// targets the copy's own version, so later writes cannot extend it.
    ///
    /// # Errors
    /// [`LocalDbError::copies_not_durable`] when the cut did not cover the copy
    /// within the bound. The caller applies nothing; the copies stay unreferenced
    /// and the next pass retries.
    pub(in crate::distributed::remote_db) fn await_copies_durable(
        &self,
        last: BatchVersion,
    ) -> Result<(), LocalDbError> {
        let Some(cut) = self.durable_cut.get() else {
            return Ok(());
        };
        let next = (last.epoch == self.epoch)
            .then(|| last.sequence.checked_add(1))
            .flatten()
            .ok_or_else(|| LocalDbError::new("relocation copy version is outside this epoch"))?;
        let lag = Duration::from_millis(self.config.durable_cut.lag_ms);
        // The cut reaches a nudged owner's sync through three stages, each at
        // most a quarter of the lag (poller pickup, parked progress poll, actor
        // tick), plus one failed-poll retry of one lag; the owner's own sync is
        // bounded by the request timeout. Eight lags cover the stages with
        // margin, so the bound is configuration-derived, not a constant.
        let timeout = lag
            .saturating_mul(COPY_DURABLE_LAG_MULTIPLE)
            .saturating_add(self.config.timeout)
            .min(COPY_DURABLE_WAIT_CAP);
        #[cfg(test)]
        let timeout = self
            .test_hooks
            .lock()
            .copy_durable_timeout
            .unwrap_or(timeout);
        cut.wait_through(next, timeout, lag)
            .map_err(|error| LocalDbError::copies_not_durable(next, error))
    }

    pub(in crate::distributed::remote_db) fn durable_cut_stats(
        &self,
    ) -> super::super::DurableCutStats {
        self.durable_cut
            .get()
            .map_or_else(super::super::DurableCutStats::default, |cut| cut.stats())
    }

    pub(in crate::distributed::remote_db) fn database_stats(&self) -> LocalDbStats {
        let mut stats = self.stats.lock().database;
        // Relaxed counters carry diagnostic time only, not publication authority.
        stats.read_resolve_ns += self.read_resolve_ns.load(Ordering::Relaxed);
        stats.value_read_ns += self.value_read_ns.load(Ordering::Relaxed);
        stats.read_resolve_ns_available = stats.read_resolve_ns != 0;
        stats
    }

    pub(in crate::distributed::remote_db) fn authority_write_stats(&self) -> AuthorityWriteStats {
        let stats = *self.stats.lock();
        AuthorityWriteStats {
            storage_wait_ns: stats.database.storage_wait_ns,
            publication_ns: stats.database.publication_ns,
            dispatch_ns: stats.database.dispatch_ns,
            ..stats.write
        }
    }

    pub(in crate::distributed::remote_db) fn pipeline_stats(&self) -> PipelineStats {
        let mut stats = self.stats.lock().pipeline;
        let (requests, bytes) = self.admission.pipeline_peaks();
        stats.concurrent_pending_peak = requests;
        stats.concurrent_queue_bytes_peak = bytes;
        stats.concurrent_queued_peak = requests;
        // These are weakly consistent diagnostic counters. No ownership or
        // payload depends on observing the same instant across owner workers.
        stats.append_requests = self
            .clients
            .iter()
            .map(|worker| worker.accounting.formed.load(Ordering::Relaxed))
            .sum();
        for worker in &self.clients {
            let accounting = &worker.accounting;
            let stop =
                |reason: DrainStop| accounting.drain_stops[reason as usize].load(Ordering::Relaxed);
            stats.owner_drains += accounting.drains.load(Ordering::Relaxed);
            stats.owner_drain_batches += accounting.drain_batches.load(Ordering::Relaxed);
            stats.owner_drain_stop_empty += stop(DrainStop::Empty);
            stats.owner_drain_stop_allocated += stop(DrainStop::Allocated);
            stats.owner_drain_stop_fence += stop(DrainStop::Fence);
            stats.owner_drain_stop_limit += stop(DrainStop::Limit);
            stats.owner_drain_stop_credit += stop(DrainStop::Credit);
            // Allocations this owner could not queue at the cap. Nonzero is not a
            // loss (the dropped floor keeps the replay start at or below them), but
            // sustained growth means the index is a whole snapshot interval behind.
            stats.allocation_cap_hits += accounting.cap_hits.load(Ordering::Relaxed);
        }
        stats
    }

    #[cfg(test)]
    pub(super) fn report_progress(&self) {
        let sender = self.test_hooks.lock().concurrent_progress.clone();
        if let Some(sender) = sender {
            let _ = sender.try_send(self.pipeline_stats());
        }
    }

    pub(in crate::distributed::remote_db) fn arm_write_profile(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<(), LocalDbError> {
        if limit > 65_536 {
            return Err(LocalDbError::new("write profile limit exceeds 65536"));
        }
        self.drain()?;
        {
            let mut publisher = self.publisher.lock();
            // Armed index diagnostics are gone with the Exact lifecycle: a profile
            // window owns the ring and its counters.
            if limit == 0 {
                publisher.checkpoint_diagnostics = None;
            } else if let Some(records) = &mut publisher.checkpoint_diagnostics {
                records.clear();
            } else {
                publisher.checkpoint_diagnostics = Some(VecDeque::with_capacity(128));
            }
            publisher.checkpoint_diagnostic_summary = CheckpointDiagnosticSummary {
                available: limit != 0,
                ..CheckpointDiagnosticSummary::default()
            };
            publisher.dropped_checkpoint_diagnostics = 0;
            publisher.last_checkpoint_diagnostic = None;
        }
        *self.write_profile.lock() = WriteProfileSnapshot {
            limit_per_connection: limit,
            expected_connections: self.clients.len(),
            ..WriteProfileSnapshot::default()
        };
        for worker in &self.clients {
            worker.arm_profile(limit)?;
        }
        Ok(())
    }

    pub(in crate::distributed::remote_db) fn finish_write_profile(
        &self,
    ) -> Result<(), LocalDbError> {
        self.drain()?;
        if self.write_profile.lock().limit_per_connection == 0 {
            return Ok(());
        }
        let mut result = Ok(());
        for worker in &self.clients {
            if let Err(error) = worker.finish_profile() {
                combine_error(&mut result, error);
            }
        }
        result
    }

    pub(in crate::distributed::remote_db) fn shutdown(
        self: &Arc<Self>,
    ) -> Result<(), LocalDbError> {
        self.shutdown_with(|_| Ok(()))
    }

    pub(in crate::distributed::remote_db) fn shutdown_live_target_final(
        self: &Arc<Self>,
    ) -> Result<Vec<(u32, crate::distributed::nvmeof::LiveTargetStats)>, LocalDbError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new(
                "final live shutdown requires an open live database",
            ));
        }
        let mut targets = Vec::with_capacity(self.clients.len());
        self.shutdown_with(|core| {
            for (endpoint, worker) in core.config.endpoints.iter().zip(&core.clients) {
                let target = worker
                    .client
                    .lock()
                    .shutdown_live_target(core.epoch)
                    .map_err(LocalDbError::component)?;
                targets.push((endpoint.partition, target));
            }
            Ok(())
        })?;
        if targets.len() != self.clients.len() {
            return Err(LocalDbError::new(
                "another shutdown already owns final target teardown",
            ));
        }
        Ok(targets)
    }

    fn shutdown_result(&self) -> Result<(), LocalDbError> {
        if self.concurrent_failure.load(Ordering::Acquire) || self.reads.failed() {
            Err(LocalDbError::new(failure_context(
                &self.first_failure,
                "write core requires recovery",
            )))
        } else {
            Ok(())
        }
    }

    pub(in crate::distributed::remote_db) fn shutdown_with(
        self: &Arc<Self>,
        after_seal: impl FnOnce(&Self) -> Result<(), LocalDbError>,
    ) -> Result<(), LocalDbError> {
        // A repeated shutdown returns Ok after a successful one, exactly as the
        // pre-cutover authority did. After a failed one it reports the failure
        // again: the old engine returned early without stopping, so its retry
        // failed too. Every failed teardown fail-stops the core (or poisoned
        // reads), which shutdown_result reports.
        if self.stopped.load(Ordering::Acquire) {
            return self.shutdown_result();
        }
        let Some(next) = self.close_allocations() else {
            self.admission.wait_until(
                Instant::now() + self.config.timeout,
                || self.stopped.load(Ordering::Acquire),
                || Ok(()),
            )?;
            return self.shutdown_result();
        };
        let started = Instant::now();
        let mut result = self.admission.wait_until(
            Instant::now() + self.config.timeout,
            || self.sequencer.reuse_next() == next,
            || self.ensure_healthy(),
        );
        self.stats.lock().shutdown.drain_ns = elapsed(started);
        let started = Instant::now();
        #[cfg(test)]
        let skip_checkpoint = std::mem::take(&mut self.test_hooks.lock().skip_shutdown_checkpoint);
        #[cfg(not(test))]
        let skip_checkpoint = false;
        if result.is_ok() && !skip_checkpoint {
            // Test hooks at the shutdown snapshot: park here (so a test can
            // observe the closed allocation state), or fail before it stores
            // anything (a crash between the drain and the region).
            #[cfg(test)]
            let (pause, interruption) = {
                let mut hooks = self.test_hooks.lock();
                (
                    hooks.checkpoint_pause.take(),
                    hooks.checkpoint_interruption.take(),
                )
            };
            #[cfg(test)]
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.recv();
            }
            #[cfg(test)]
            let interrupted = interruption.is_some();
            #[cfg(not(test))]
            let interrupted = false;
            result = if interrupted {
                Err(LocalDbError::new("injected shutdown snapshot interruption"))
            } else {
                self.table.snapshot().map(|_| ())
            };
        }
        self.stats.lock().shutdown.checkpoint_ns = elapsed(started);
        // After the checkpoint: its owner syncs end every parked poll, and the
        // checkpoint then covers everything the cut covered.
        if let Some(cut) = self.durable_cut.get() {
            cut.stop();
        }
        let started = Instant::now();
        self.reads.stop();
        self.stats.lock().shutdown.join_ns = elapsed(started);
        if self.reads.failed() && result.is_ok() {
            result = Err(LocalDbError::new(
                "asynchronous read failed before shutdown",
            ));
        }
        let mut drain_failed = false;
        if self.live_pool.is_some() {
            for worker in &self.clients {
                let (closed, failed_writer) = {
                    let mut client = worker.client.lock();
                    if self.concurrent_failure.load(Ordering::Acquire) {
                        client.record_live_write_error();
                    }
                    let closed = client.close_live_writer(&mut Vec::new(), None);
                    let failed_writer = if closed.is_err() {
                        client.take_failed_live_writer()
                    } else {
                        None
                    };
                    (closed.map_err(LocalDbError::component), failed_writer)
                };
                // Publish poison before the potentially slow DMA retirement.
                if let Err(error) = &closed {
                    self.begin_failure(error);
                }
                // Ownership is detached under the client mutex; DMA retirement
                // runs outside it, exactly as in the existing fail-stop path.
                let closed = match failed_writer.map(|mut writer| writer.drain_failed()) {
                    Some(Err(error)) => Err(match closed {
                        Ok(()) => LocalDbError::component(error),
                        Err(primary) => LocalDbError::new(format!(
                            "{primary}; detached live writer drain failed: {error}"
                        )),
                    }),
                    _ => closed,
                };
                if let Err(error) = closed {
                    drain_failed = true;
                    self.begin_failure(&error);
                    let previous = std::mem::replace(&mut result, Ok(()));
                    result = Err(match previous {
                        Ok(()) => error,
                        Err(primary) => LocalDbError::new(format!(
                            "{primary}; live writer drain failed: {error}"
                        )),
                    });
                }
            }
        }
        // A failed close or drain retains the poisoned clients' controllers in
        // the pool for recovery; a failed retirement stays owned by its queue's
        // fail-stop Drop, so the pool is not shut down underneath it.
        if !drain_failed
            && let Some(pool) = &self.live_pool
            && let Err(error) = pool.shutdown(Instant::now() + self.config.timeout)
        {
            combine_error(&mut result, LocalDbError::component(error));
        }
        let started = Instant::now();
        if result.is_ok() {
            let seals: Vec<_> = self
                .clients
                .iter()
                .map(|worker| worker.seal(self.epoch))
                .collect();
            for seal in seals {
                if let Err(error) = seal.and_then(receive) {
                    combine_error(&mut result, error);
                }
            }
        }
        self.stats.lock().shutdown.seal_ns = elapsed(started);
        if result.is_ok() {
            result = after_seal(self);
        }
        if let Err(error) = &result {
            self.fail(LocalDbError::new(error.message()));
        }
        let started = Instant::now();
        for worker in &self.clients {
            if let Err(error) = worker.stop() {
                combine_error(&mut result, error);
            }
        }
        self.stats.lock().shutdown.join_ns += elapsed(started);
        // Release publishes completed joins before another shutdown returns.
        self.stopped.store(true, Ordering::Release);
        self.admission.notify_lifecycle();
        result
    }
}

fn combine_error(result: &mut Result<(), LocalDbError>, error: LocalDbError) {
    let previous = std::mem::replace(result, Ok(()));
    *result = Err(match previous {
        Ok(()) => error,
        Err(primary) => LocalDbError::new(format!("{primary}; shutdown also failed: {error}")),
    });
}
