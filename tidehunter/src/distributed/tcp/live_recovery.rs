//! Metadata-only recovery with native inner frames read from the live namespace.

use super::live_read::{LiveReadTiming, record_read_io_stats};
use super::{Client, Receipt, RecoveryPage, TcpError};
use crate::crc::CrcFrame;
use crate::distributed::codec::CompleteBatch;
use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
use crate::distributed::nvmeof::live_io::{LiveIo, LiveIoResult, LiveSubmitOutcome};
use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
use crate::distributed::nvmeof::live_metrics::{LiveNamespaceIdentity, LiveRouteEvidence};
use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
use crate::distributed::nvmeof::live_pool::LivePoolAcquisition;
use std::time::{Duration, Instant};

impl Client {
    /// Replay whole WAL groups. `next` is a WAL byte offset, not a receipt ordinal.
    /// Runs during bounded open/recovery, never on the shared read reactor.
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
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
        }
        let page = {
            let _control = LiveReadTiming::start(&self.live_counters, LiveTimingStage::Control);
            let response = self.request(super::Request::ReplayLiveWal {
                authority_epoch,
                source_epoch,
                from,
                limit: page_limit,
                require_end,
            })?;
            self.validate_live_recovery_page(
                response,
                authority_epoch,
                source_epoch,
                cursor,
                page_limit,
            )?
        };
        let token = page.epoch_pin_token;
        let result = (|| {
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
                    pool.cache_local_namespace(
                        &target,
                        &config.host_nqn,
                        config.host_id,
                        access,
                        &first.segment.path,
                    )
                    .map_err(TcpError::component)?;
                }
                let setup =
                    LiveReadTiming::start(&self.live_counters, LiveTimingStage::NamespaceSetup);
                let deadline = Instant::now() + Duration::from_millis(config.connect_timeout_ms);
                let (lease, acquisition) = pool
                    .acquire_file(
                        &target,
                        &config.host_nqn,
                        config.host_id,
                        access,
                        deadline,
                        (
                            first.segment.partition,
                            first.segment.epoch,
                            first.segment.file_index,
                        ),
                    )
                    .map(|(lease, acquisition, _)| (lease, acquisition))
                    .map_err(TcpError::component)?;
                let namespace = lease.open_namespace().map_err(TcpError::component)?;
                drop(setup);
                if let Some(counters) = &self.live_counters
                    && matches!(acquisition, LivePoolAcquisition::Connected)
                {
                    counters.record_namespace_connect();
                }
                if let Some(metrics) = &self.live_metrics {
                    let observed = lease.binding();
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
                .map_err(TcpError::component)?;
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
                            return Err(TcpError::new("live recovery frame exceeds batch limit"));
                        }
                        let block = binding.block_size as usize;
                        let offset_in_block =
                            (entry.offset % u64::from(binding.block_size)) as usize;
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
                                return Err(TcpError::new(
                                    "live recovery frame exceeds queue byte budget",
                                ));
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
                    match io
                        .submit_reads(1, &ranges, deadline)
                        .map_err(TcpError::component)?
                    {
                        LiveSubmitOutcome::Submitted => {}
                        LiveSubmitOutcome::Full => {
                            return Err(TcpError::new("live recovery admission is full"));
                        }
                    }
                    let read_io =
                        LiveReadTiming::start(&self.live_counters, LiveTimingStage::ReadIo);
                    let records = loop {
                        let completions = match io.progress() {
                            Ok(completions) => completions,
                            Err(error) => {
                                let _ = io.shutdown();
                                if let Some(counters) = &self.live_counters {
                                    record_read_io_stats(counters, &before, &io.stats(), true);
                                }
                                return Err(TcpError::component(error));
                            }
                        };
                        if let Some(completion) = completions.into_iter().next() {
                            if let Some(counters) = &self.live_counters {
                                record_read_io_stats(counters, &before, &io.stats(), true);
                            }
                            if completion.id != 1 {
                                return Err(TcpError::new("unexpected live recovery completion"));
                            }
                            break match completion.result.map_err(TcpError::component)? {
                                LiveIoResult::Reads(records) => records,
                                LiveIoResult::Written | LiveIoResult::Flushed => {
                                    return Err(TcpError::new("unexpected live recovery write"));
                                }
                            };
                        }
                        std::thread::yield_now();
                    };
                    drop(read_io);
                    // The queue stats are cumulative, so use a new queue per wave.
                    drop(io);
                    if records.len() != index - start {
                        return Err(TcpError::new("live recovery result count mismatch"));
                    }
                    let validation =
                        LiveReadTiming::start(&self.live_counters, LiveTimingStage::Validation);
                    for (entry, frame) in page.entries[start..index].iter().zip(records) {
                        let payload = exact_frame_payload(&frame)?;
                        let batch = CompleteBatch::decode_owned(
                            &self.shape,
                            payload.to_vec(),
                            self.expected.batch_limits,
                        )
                        .map_err(TcpError::component)?;
                        if batch.version().epoch != source_epoch {
                            return Err(TcpError::new("live recovery source epoch mismatch"));
                        }
                        let durable =
                            self.check_receipt(&batch, Receipt::from_durable(entry.receipt))?;
                        batches.push((batch, durable));
                    }
                    drop(validation);
                    if index < page.entries.len() && page.entries[index].binding == *binding {
                        let setup = LiveReadTiming::start(
                            &self.live_counters,
                            LiveTimingStage::NamespaceSetup,
                        );
                        io = LiveIo::new(
                            lease.open_namespace().map_err(TcpError::component)?,
                            config.max_inflight_requests,
                            config.max_inflight_bytes,
                            false,
                        )
                        .map_err(TcpError::component)?;
                        drop(setup);
                    } else {
                        break;
                    }
                }
                // Each queue and its namespace FD have retired before the page pin.
                drop(lease);
            }
            Ok(RecoveryPage {
                batches,
                next: page.next,
            })
        })();
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
        }
        let release = {
            let _control = LiveReadTiming::start(&self.live_counters, LiveTimingStage::Control);
            self.release_live_epoch_pin(authority_epoch, token)
        };
        match (result, release) {
            (Err(error), release) => {
                self.broken = true;
                if let Some(counters) = &self.live_counters {
                    counters.record_error();
                }
                match release {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(TcpError::new(format!(
                        "{error}; live recovery pin release failed: {cleanup}"
                    ))),
                }
            }
            (Ok(_), Err(error)) => Err(error),
            (Ok(page), Ok(())) => Ok(page),
        }
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
