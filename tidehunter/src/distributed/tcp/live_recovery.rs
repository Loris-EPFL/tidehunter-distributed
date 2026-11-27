//! Metadata-only recovery with native inner frames read from the live namespace.

use super::live_control::LiveRecoveryPage;
use super::live_read::{LiveReadTiming, record_read_io_stats};
use super::{Client, LiveClientConfig, Receipt, RecoveryPage, TcpError};
use crate::crc::CrcFrame;
use crate::distributed::codec::CompleteBatch;
use crate::distributed::nvmeof::live_connect::{ControllerAdoption, LiveConnectTarget};
use crate::distributed::nvmeof::live_io::{LiveIo, LiveIoError, LiveIoResult, LiveSubmitOutcome};
use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
use crate::distributed::nvmeof::live_metrics::{LiveNamespaceIdentity, LiveRouteEvidence};
use crate::distributed::nvmeof::live_namespace::{LiveNamespaceAccess, retryable_lost_read};
use crate::distributed::nvmeof::live_pool::{
    LiveControllerLease, LiveControllerPool, LivePoolAcquisition,
};
use crate::distributed::storage::DurableBatch;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Page reads per recovery request: the first, plus one after a controller
/// the sysfs probe proved lost was retired.
const RECOVERY_PAGE_ATTEMPTS: u32 = 2;

/// One failed page read. `retryable` marks the only class a second read may
/// replace; `failed_lease` is the exact lease the attempt used, so the retry
/// retires that object instead of letting the pool reuse it.
struct RecoveryAttemptError {
    error: TcpError,
    retryable: bool,
    failed_lease: Option<Arc<LiveControllerLease>>,
}

impl From<TcpError> for RecoveryAttemptError {
    fn from(error: TcpError) -> Self {
        Self::fatal(error)
    }
}

impl RecoveryAttemptError {
    fn fatal(error: TcpError) -> Self {
        Self {
            error,
            retryable: false,
            failed_lease: None,
        }
    }

    /// Carry the lease this attempt used, so the retry retires exactly it.
    fn with_lease(mut self, lease: &Arc<LiveControllerLease>) -> Self {
        self.failed_lease = Some(lease.clone());
        self
    }

    /// Classify a local page-read failure. The probe runs only here, on the
    /// error path, and never replaces the controller of a live lease.
    fn io(error: io::Error, pool: &LiveControllerPool, nqn: &str) -> Self {
        let lost = crate::distributed::nvmeof::live_namespace::controller_lost(&error)
            || pool.entry_lost(nqn);
        let retry = retryable_lost_read(&error, lost);
        Self {
            error: TcpError::component(error),
            retryable: retry,
            failed_lease: None,
        }
    }

    /// Classify a live queue failure. Only its typed `Io` cause can name a
    /// lost controller; other queue verdicts stay fatal.
    fn live_io(error: LiveIoError, pool: &LiveControllerPool, nqn: &str) -> Self {
        match error {
            LiveIoError::Io(error) => Self::io(error, pool, nqn),
            other => Self::fatal(TcpError::component(other)),
        }
    }
}

impl Client {
    /// Replay whole WAL groups. `next` is a WAL byte offset, not a receipt ordinal.
    /// Runs during bounded open/recovery, never on the shared read reactor.
    ///
    /// A page read that fails because the controller it used is verifiably not
    /// live is retried once on a fresh connection: the controller a killed
    /// coordinator left behind can still pin the subsystem an owner restart has
    /// replaced. The page is retried, never the database. The owner's verdicts
    /// (epoch, receipt, frame and sealed-end checks) are never retried.
    pub fn replay_live_wal_page(
        &mut self,
        authority_epoch: u64,
        source_epoch: u64,
        from: u64,
        limit: usize,
        require_end: u64,
    ) -> Result<RecoveryPage, TcpError> {
        let cursor = usize::try_from(from)
            .map_err(|_| TcpError::new("replay offset exceeds address space"))?;
        if limit == 0 || limit > 1024 {
            return Err(TcpError::new("invalid recovery page limit"));
        }
        let config = self
            .live_config
            .clone()
            .ok_or_else(|| TcpError::new("live NVMe is not configured"))?;
        let pool = self
            .live_pool
            .clone()
            .ok_or_else(|| TcpError::new("live controller pool unavailable"))?;
        // Bound the retained decoded page independently of the number of tiny
        // batches a peer might return. A single large batch remains admissible.
        let per_batch = self
            .expected
            .batch_limits
            .max_encoded_bytes
            .saturating_add(8);
        let page_limit = (config.max_inflight_bytes / per_batch.max(1))
            .max(1)
            .min(limit)
            .min(self.page_limit)
            .min(256);
        // The owner incarnation this reopen sees. The first page attempt may
        // adopt a leftover controller only when its durable intent was created
        // against this same owner process.
        let owner_incarnation = Some(self.connected_status().incarnation);
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            let adoption = if attempts == 1 {
                ControllerAdoption::Recovery { owner_incarnation }
            } else {
                // The retry follows a verified loss: always connect fresh.
                ControllerAdoption::Fresh { owner_incarnation }
            };
            match self.replay_live_wal_page_attempt(
                authority_epoch,
                source_epoch,
                from,
                require_end,
                page_limit,
                per_batch,
                cursor,
                &config,
                &pool,
                adoption,
            ) {
                Ok(page) => break Ok(page),
                Err(RecoveryAttemptError {
                    error: _,
                    retryable: true,
                    failed_lease,
                }) if attempts < RECOVERY_PAGE_ATTEMPTS => {
                    // The failed attempt released its epoch pin before this
                    // point. Retire the exact lease it used: a liveness probe
                    // can read live again and veto a generic eviction, after
                    // which the pool would reuse the failed lease. Retiring a
                    // different lease another lane cached is forbidden.
                    if let Some(lease) = failed_lease {
                        let teardown =
                            super::controller_teardown_deadline(config.connect_timeout_ms);
                        if let Err(error) = pool.evict_lease(&lease, teardown) {
                            break Err(TcpError::with_kind(
                                io::ErrorKind::ConnectionAborted,
                                format!(
                                    "live recovery could not retire its failed controller: {error}"
                                ),
                            ));
                        }
                    }
                }
                Err(RecoveryAttemptError { error, .. }) => break Err(error),
            }
        };
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                self.broken = true;
                if let Some(counters) = &self.live_counters {
                    counters.record_error();
                }
                Err(error)
            }
        }
    }

    /// One page request and its read. Every epoch pin this attempt takes is
    /// released before it returns, so a retry never overlaps an old pin.
    #[allow(clippy::too_many_arguments)]
    fn replay_live_wal_page_attempt(
        &mut self,
        authority_epoch: u64,
        source_epoch: u64,
        from: u64,
        require_end: u64,
        page_limit: usize,
        per_batch: usize,
        cursor: usize,
        config: &LiveClientConfig,
        pool: &LiveControllerPool,
        adoption: ControllerAdoption,
    ) -> Result<RecoveryPage, RecoveryAttemptError> {
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
        }
        let page = {
            let _control = LiveReadTiming::start(&self.live_counters, LiveTimingStage::Control);
            let response = self
                .request(super::Request::ReplayLiveWal {
                    authority_epoch,
                    source_epoch,
                    from,
                    limit: page_limit,
                    require_end,
                })
                .map_err(RecoveryAttemptError::fatal)?;
            self.validate_live_recovery_page(
                response,
                authority_epoch,
                source_epoch,
                cursor,
                page_limit,
            )
            .map_err(RecoveryAttemptError::fatal)?
        };
        let token = page.epoch_pin_token;
        let read =
            self.read_live_recovery_records(&page, config, pool, per_batch, source_epoch, adoption);
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
        }
        let release = {
            let _control = LiveReadTiming::start(&self.live_counters, LiveTimingStage::Control);
            self.release_live_epoch_pin(authority_epoch, token)
        };
        match (read, release) {
            (Ok(batches), Ok(())) => Ok(RecoveryPage {
                batches,
                next: page.next,
            }),
            (Err(error), Ok(())) => Err(error),
            (Err(mut error), Err(cleanup)) => {
                // An unreleased pin forbids the retry: the next page could
                // conflict with the owner's drain of the old pin.
                error.error = TcpError::new(format!(
                    "{}; live recovery pin release failed: {cleanup}",
                    error.error
                ));
                error.retryable = false;
                error.failed_lease = None;
                Err(error)
            }
            (Ok(_), Err(error)) => Err(RecoveryAttemptError::fatal(error)),
        }
    }

    /// Read every binding of one validated page. Each binding opens through the
    /// attempt's recovery connection and releases its lease before the caller's
    /// page pin.
    fn read_live_recovery_records(
        &mut self,
        page: &LiveRecoveryPage,
        config: &LiveClientConfig,
        pool: &LiveControllerPool,
        per_batch: usize,
        source_epoch: u64,
        adoption: ControllerAdoption,
    ) -> Result<Vec<(CompleteBatch, DurableBatch)>, RecoveryAttemptError> {
        let mut batches = Vec::with_capacity(page.entries.len());
        let mut index = 0;
        while index < page.entries.len() {
            let first = &page.entries[index];
            let binding = &first.binding;
            let target = LiveConnectTarget {
                nqn: binding.nqn.clone(),
                uuid: binding.uuid,
                nsid: binding.nsid,
                bytes: binding.bytes,
                block_size: binding.block_size,
                address: binding.address.clone(),
                service: binding.service,
                transport: binding.transport,
                geometry: config.queue_geometry,
            };
            let access = if binding.readonly {
                LiveNamespaceAccess::ReadOnly
            } else {
                LiveNamespaceAccess::ReadWrite
            };
            #[cfg(any(test, feature = "test-utils"))]
            if self.live_write_local_fixture {
                // Use the real namespace I/O and CRC checks without a kernel target.
                pool.cache_local_shared_namespace(
                    &target,
                    &config.host_nqn,
                    config.host_id,
                    &first.segment.path,
                )
                .map_err(|error| RecoveryAttemptError::io(error, pool, &target.nqn))?;
            }
            let setup = LiveReadTiming::start(&self.live_counters, LiveTimingStage::NamespaceSetup);
            let deadline = Instant::now() + Duration::from_millis(config.connect_timeout_ms);
            let (lease, acquisition) = pool
                .acquire_file(
                    &target,
                    &config.host_nqn,
                    config.host_id,
                    access,
                    // The first attempt adopts only a controller its intent
                    // records against this owner incarnation; a retry always
                    // retires and connects fresh.
                    adoption,
                    deadline,
                    (
                        first.segment.partition,
                        first.segment.epoch,
                        first.segment.file_index,
                    ),
                )
                .map(|(lease, acquisition, _)| (lease, acquisition))
                .map_err(|error| RecoveryAttemptError::io(error, pool, &target.nqn))?;
            let (namespace, observed) = lease
                .namespace(target.nsid, target.uuid, access, deadline)
                .map_err(|error| {
                    RecoveryAttemptError::io(error, pool, &target.nqn).with_lease(&lease)
                })?;
            drop(setup);
            if let Some(counters) = &self.live_counters
                && matches!(acquisition, LivePoolAcquisition::Connected)
            {
                counters.record_namespace_connect();
            }
            if let Some(metrics) = &self.live_metrics {
                metrics.record_route(LiveRouteEvidence {
                    transport: observed.transport,
                    address: binding.address.clone(),
                    service: binding.service,
                    host_nqn: config.host_nqn.clone(),
                    host_id: config.host_id,
                    writer: None,
                    namespace_identity: LiveNamespaceIdentity {
                        namespace_path: observed.namespace_path.clone(),
                        controller_instance: lease.controller_instance(),
                        nqn: observed.nqn.clone(),
                        uuid: observed.uuid,
                        nsid: observed.nsid,
                        bytes: observed.bytes,
                        block_size: observed.block_size,
                        controller: lease.evidence(),
                    },
                });
            }
            let mut io = LiveIo::new(
                namespace,
                config.max_inflight_requests,
                config.max_inflight_bytes,
                false,
            )
            .map_err(TcpError::component)
            .map_err(RecoveryAttemptError::fatal)?;
            while index < page.entries.len() && page.entries[index].binding == *binding {
                let start = index;
                let mut ranges = Vec::new();
                let mut charge = 0usize;
                let validation =
                    LiveReadTiming::start(&self.live_counters, LiveTimingStage::Validation);
                while index < page.entries.len()
                    && page.entries[index].binding == *binding
                    && ranges.len() < config.max_inflight_requests
                {
                    let entry = &page.entries[index];
                    let len = usize::try_from(entry.len).map_err(TcpError::component)?;
                    if len > per_batch {
                        return Err(RecoveryAttemptError::fatal(TcpError::new(
                            "live recovery frame exceeds batch limit",
                        )));
                    }
                    let block = binding.block_size as usize;
                    let offset_in_block = (entry.offset % u64::from(binding.block_size)) as usize;
                    let aligned = offset_in_block
                        .checked_add(len)
                        .and_then(|n| n.checked_add(block - 1))
                        .map(|n| n / block * block)
                        .ok_or_else(|| TcpError::new("live recovery range overflow"))?;
                    let next_charge = charge
                        .checked_add(aligned)
                        .and_then(|n| n.checked_add(block.max(4096) - 1))
                        .and_then(|n| n.checked_add(len))
                        .ok_or_else(|| TcpError::new("live recovery charge overflow"))?;
                    if next_charge > config.max_inflight_bytes {
                        if ranges.is_empty() {
                            return Err(RecoveryAttemptError::fatal(TcpError::new(
                                "live recovery frame exceeds queue byte budget",
                            )));
                        }
                        break;
                    }
                    charge = next_charge;
                    ranges.push((entry.offset, len));
                    index += 1;
                }
                drop(validation);
                let before = io.stats();
                let deadline = Instant::now() + Duration::from_millis(config.io_timeout_ms);
                match io.submit_reads(1, &ranges, deadline).map_err(|error| {
                    RecoveryAttemptError::live_io(error, pool, &target.nqn).with_lease(&lease)
                })? {
                    LiveSubmitOutcome::Submitted => {}
                    LiveSubmitOutcome::Full => {
                        return Err(RecoveryAttemptError::fatal(TcpError::new(
                            "live recovery admission is full",
                        )));
                    }
                }
                let read_io = LiveReadTiming::start(&self.live_counters, LiveTimingStage::ReadIo);
                let records = loop {
                    let completions = match io.progress() {
                        Ok(completions) => completions,
                        Err(error) => {
                            let _ = io.shutdown();
                            if let Some(counters) = &self.live_counters {
                                record_read_io_stats(counters, &before, &io.stats(), true);
                            }
                            return Err(RecoveryAttemptError::live_io(error, pool, &target.nqn)
                                .with_lease(&lease));
                        }
                    };
                    if let Some(completion) = completions.into_iter().next() {
                        if let Some(counters) = &self.live_counters {
                            record_read_io_stats(counters, &before, &io.stats(), true);
                        }
                        if completion.id != 1 {
                            return Err(RecoveryAttemptError::fatal(TcpError::new(
                                "unexpected live recovery completion",
                            )));
                        }
                        break match completion.result {
                            Ok(LiveIoResult::Reads(records)) => records,
                            Ok(LiveIoResult::Written | LiveIoResult::Flushed) => {
                                return Err(RecoveryAttemptError::fatal(TcpError::new(
                                    "unexpected live recovery write",
                                )));
                            }
                            Err(LiveIoError::Io(error)) => {
                                return Err(RecoveryAttemptError::io(error, pool, &target.nqn)
                                    .with_lease(&lease));
                            }
                            Err(error) => {
                                return Err(RecoveryAttemptError::fatal(TcpError::component(
                                    error,
                                )));
                            }
                        };
                    }
                    std::thread::yield_now();
                };
                drop(read_io);
                // The queue stats are cumulative, so use a new queue per wave.
                drop(io);
                if records.len() != index - start {
                    return Err(RecoveryAttemptError::fatal(TcpError::new(
                        "live recovery result count mismatch",
                    )));
                }
                let validation =
                    LiveReadTiming::start(&self.live_counters, LiveTimingStage::Validation);
                for (entry, frame) in page.entries[start..index].iter().zip(records) {
                    let payload = exact_frame_payload(&frame).map_err(TcpError::component)?;
                    let batch = CompleteBatch::decode_owned(
                        &self.shape,
                        payload.to_vec(),
                        self.expected.batch_limits,
                    )
                    .map_err(TcpError::component)?;
                    if batch.version().epoch != source_epoch {
                        return Err(RecoveryAttemptError::fatal(TcpError::new(
                            "live recovery source epoch mismatch",
                        )));
                    }
                    let durable =
                        self.check_receipt(&batch, Receipt::from_durable(entry.receipt))?;
                    batches.push((batch, durable));
                }
                drop(validation);
                if index < page.entries.len() && page.entries[index].binding == *binding {
                    let setup =
                        LiveReadTiming::start(&self.live_counters, LiveTimingStage::NamespaceSetup);
                    io = LiveIo::new(
                        lease
                            .namespace(target.nsid, target.uuid, access, deadline)
                            .map_err(|error| {
                                RecoveryAttemptError::io(error, pool, &target.nqn)
                                    .with_lease(&lease)
                            })?
                            .0,
                        config.max_inflight_requests,
                        config.max_inflight_bytes,
                        false,
                    )
                    .map_err(TcpError::component)
                    .map_err(RecoveryAttemptError::fatal)?;
                    drop(setup);
                } else {
                    break;
                }
            }
            // Each queue and its namespace FD have retired before the page pin.
            drop(lease);
        }
        Ok(batches)
    }
}

fn exact_frame_payload(frame: &[u8]) -> Result<&[u8], TcpError> {
    let payload = CrcFrame::read_from_slice(frame, 0)
        .map_err(|_| TcpError::new("invalid live recovery CRC frame"))?;
    if payload.len().checked_add(8) != Some(frame.len()) {
        return Err(TcpError::new("live recovery frame length mismatch"));
    }
    Ok(payload)
}

#[cfg(test)]
#[path = "live_recovery_tests.rs"]
mod tests;
