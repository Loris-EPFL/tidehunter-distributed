use super::super::{
    AuthorityWriteStats, CheckpointDiagnosticSummary, LocalDbError, LocalDbStats, PipelineStats,
    WriteProfileSnapshot, elapsed, failure_context, receive,
};
use super::completion::DrainStop;
use super::core::WriteCore;
use std::collections::VecDeque;
use std::sync::{Arc, atomic::Ordering};
use std::time::Instant;

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

    pub(in crate::distributed::remote_db) fn flush_index(
        self: &Arc<Self>,
    ) -> Result<(), LocalDbError> {
        let next = self.sequencer.next();
        self.wait_published_before(next)?;
        // Finish any older capture before freezing one that covers this cut.
        // Writes allocated after the snapshot do not extend the caller's wait.
        self.progress_checkpoint(true)?;
        self.start_checkpoint()?;
        self.progress_checkpoint(true)
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
        if let Some(cut) = &self.durable_cut {
            cut.certify(next)?;
        }
        self.ensure_healthy()
    }

    pub(in crate::distributed::remote_db) fn durable_cut_stats(
        &self,
    ) -> super::super::DurableCutStats {
        self.durable_cut
            .as_ref()
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
        self.progress_checkpoint(true)?;
        {
            let mut publisher = self.publisher.lock();
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

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            result = self.commit_checkpoint();
        }
        self.stats.lock().shutdown.checkpoint_ns = elapsed(started);
        // After the checkpoint: its owner syncs end every parked poll, and the
        // checkpoint then covers everything the cut covered.
        if let Some(cut) = &self.durable_cut {
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let mut drain_failed = false;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
        // A checkpoint may own worker senders: take its handle under publisher,
        // then join outside that mutex while controls can still be serviced.
        let checkpoint = self.publisher.lock().background_checkpoint.take();
        drop(checkpoint);
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
