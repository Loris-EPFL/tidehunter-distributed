//! Initiator configuration for the live owner-WAL NVMe-oF data plane.

use serde::{Deserialize, Serialize};

/// Bounded initiator resources. Namespace identity and geometry are supplied by
/// the authenticated owner for each WAL segment, not inferred from device names.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveClientConfig {
    pub host_nqn: String,
    /// Stable initiator UUID; must remain the same across authority reopen.
    pub host_id: [u8; 16],
    pub max_inflight_requests: usize,
    pub max_inflight_bytes: usize,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
}

impl LiveClientConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.host_id == [0; 16]
            || !self.host_nqn.starts_with("nqn.")
            || self.host_nqn.len() > 223
            || !self
                .host_nqn
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b',' && b != b'=')
            || !(1..=32768).contains(&self.max_inflight_requests)
            || self.max_inflight_bytes == 0
            || !(1..=300_000).contains(&self.connect_timeout_ms)
            || !(1..=300_000).contains(&self.io_timeout_ms)
        {
            return Err("invalid live NVMe-oF initiator configuration");
        }
        Ok(())
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct WriteTiming<'a> {
    counters: &'a crate::distributed::nvmeof::live_metrics::LiveNvmeCounters,
    stage: crate::distributed::nvmeof::live_metrics::LiveTimingStage,
    started: std::time::Instant,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl Drop for WriteTiming<'_> {
    fn drop(&mut self) {
        self.counters.record_timing(
            self.stage,
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            1,
        );
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct UnknownWriteOnError<'a> {
    counters: Option<&'a crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>,
    armed: bool,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl Drop for UnknownWriteOnError<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Some(counters) = &self.counters
        {
            counters.record_unknown_write();
        }
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) struct LiveWriteQueue {
    target: crate::distributed::nvmeof::live_connect::LiveConnectTarget,
    io: crate::distributed::nvmeof::live_io::LiveIo,
    _lease: std::sync::Arc<crate::distributed::nvmeof::live_pool::LiveControllerLease>,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl LiveWriteQueue {
    fn matches(
        &self,
        binding: &crate::distributed::nvmeof::live_target::LiveTargetBinding,
    ) -> bool {
        let target = &self.target;
        !binding.readonly
            && target.nqn == binding.nqn
            && target.uuid == binding.uuid
            && target.nsid == binding.nsid
            && target.bytes == binding.bytes
            && target.block_size == binding.block_size
            && target.address == binding.address
            && target.service == binding.service
            && target.transport == binding.transport
            && target.spdk == binding.spdk
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn record_write_io(
    counters: &crate::distributed::nvmeof::live_metrics::LiveNvmeCounters,
    before: &crate::distributed::nvmeof::live_io::LiveIoStats,
    after: &crate::distributed::nvmeof::live_io::LiveIoStats,
) {
    use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
    let ns = |n: u128| n.min(u64::MAX as u128) as u64;
    counters.record_write(
        after
            .completed_write_ops
            .saturating_sub(before.completed_write_ops),
        after
            .completed_write_bytes
            .saturating_sub(before.completed_write_bytes),
    );
    let submissions = after.submissions.saturating_sub(before.submissions);
    if submissions != 0 {
        counters.record_timing(
            LiveTimingStage::Queue,
            ns(after.queue_ns.saturating_sub(before.queue_ns)),
            submissions,
        );
        counters.record_timing(
            LiveTimingStage::Submit,
            ns(after.submit_ns.saturating_sub(before.submit_ns)),
            submissions,
        );
    }
    let completions = after.completions.saturating_sub(before.completions);
    if completions != 0 {
        counters.record_timing(
            LiveTimingStage::Completion,
            ns(after.submit_to_cq_ns.saturating_sub(before.submit_to_cq_ns)),
            completions,
        );
    }
}

/// How many queued append groups one live reservation can carry. One frame has
/// one reservation, one NVMe write, one completion RPC and one persistence
/// sequence, so merging queued groups divides all four by the merge factor.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveGroupBudget {
    max_batches: usize,
    max_payload: usize,
    max_frame: usize,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl LiveGroupBudget {
    /// Size of an empty group: header only.
    pub(crate) const EMPTY_PAYLOAD: usize = crate::distributed::group::GROUP_HEADER_LEN;

    /// Counters of a group after adding `extra`, or `None` if it would exceed the
    /// owner's reservation limits (batch count, payload, aligned frame size).
    pub(crate) fn extend(
        &self,
        count: usize,
        payload: usize,
        extra: &[super::CompleteBatch],
    ) -> Option<(usize, usize)> {
        use crate::distributed::group::CompleteGroup;
        let count = count.checked_add(extra.len())?;
        let mut payload = payload;
        for batch in extra {
            payload = payload
                .checked_add(CompleteGroup::encoded_member_len(batch.encoded().len()).ok()?)?;
        }
        let frame = payload.checked_add(8 + 4095)? & !4095;
        (count <= self.max_batches && payload <= self.max_payload && frame <= self.max_frame)
            .then_some((count, payload))
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl super::Client {
    pub(crate) fn live_group_budget(&self) -> LiveGroupBudget {
        LiveGroupBudget {
            max_batches: self.expected.group_limits.max_batches as usize,
            max_payload: self.expected.group_limits.max_encoded_bytes,
            max_frame: (self.expected.fragment_bytes as usize).min(64 * 1024 * 1024),
        }
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl super::Client {
    pub(crate) fn set_live_pool(
        &mut self,
        pool: std::sync::Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>,
    ) {
        self.live_pool = Some(pool);
    }

    pub(crate) fn set_live_metrics(
        &mut self,
        metrics: std::sync::Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>,
    ) -> Result<(), super::TcpError> {
        self.live_counters = Some(metrics.register().map_err(super::TcpError::component)?);
        self.live_metrics = Some(metrics);
        Ok(())
    }

    /// Drain accepted DMA before dropping the cached queue and controller lease.
    /// An error retains the queue so its buffers and namespace remain owned.
    pub(crate) fn drain_live_writer(&mut self) -> Result<(), super::TcpError> {
        let Some(writer) = self.live_writer.as_mut() else {
            return Ok(());
        };
        let completions = writer.io.shutdown().map_err(super::TcpError::component)?;
        self.live_writer.take();
        if !completions.is_empty() {
            return Err(super::TcpError::new(
                "unexpected outstanding live write during drain",
            ));
        }
        Ok(())
    }

    /// Stop admission under the same client mutex used by the owner worker.
    /// A queued append cannot recreate a queue after shutdown drains this one.
    pub(crate) fn close_live_writer(&mut self) -> Result<(), super::TcpError> {
        self.live_writer_closing = true;
        self.drain_live_writer()
    }

    pub(crate) fn configure_live_nvme(
        &mut self,
        config: &LiveClientConfig,
    ) -> Result<(), super::TcpError> {
        config.validate().map_err(super::TcpError::new)?;
        if self.live_writer_closing {
            return Err(super::TcpError::new("live writer admission is closed"));
        }
        if !self.connected_status().live_nvmeof || self.has_pending_io() {
            return Err(super::TcpError::new(
                "live NVMe client is not ready for configuration",
            ));
        }
        if self.live_config.as_ref() != Some(config) {
            self.drain_live_writer()?;
            self.live_config = Some(config.clone());
        }
        Ok(())
    }

    pub(crate) fn live_nvme_configured(&self) -> bool {
        self.live_config.is_some()
    }

    pub(crate) fn record_live_write_error(&self) {
        if let Some(counters) = &self.live_counters {
            counters.record_error();
        }
    }

    /// Runs on the dedicated owner worker, never the coordinator or a read reactor.
    /// The reservation remains unresolved on any failure and the caller poisons
    /// the authority; a successful CQE alone is not a durable receipt.
    pub(crate) fn write_live_group(
        &mut self,
        epoch: u64,
        batches: &[super::CompleteBatch],
        timeline: &crate::distributed::write_timeline::WriteTimeline,
        owner: u32,
    ) -> Result<Vec<super::DurableBatch>, super::TcpError> {
        use super::live_control::LiveReserveResponse;
        use crate::distributed::group::CompleteGroup;
        use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
        use crate::distributed::nvmeof::live_io::{LiveIo, LiveIoResult, LiveSubmitOutcome};
        use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
        use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
        use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
        use crate::distributed::write_timeline::WriteTimelineStage as Stage;
        use std::time::{Duration, Instant};
        if self.live_writer_closing {
            return Err(super::TcpError::new("live writer admission is closed"));
        }

        let config = self
            .live_config
            .as_ref()
            .ok_or_else(|| super::TcpError::new("live NVMe is not configured"))?;
        let (host_id, max_inflight_requests, max_inflight_bytes, connect_timeout_ms, io_timeout_ms) = (
            config.host_id,
            config.max_inflight_requests,
            config.max_inflight_bytes,
            config.connect_timeout_ms,
            config.io_timeout_ms,
        );
        let pool = self
            .live_pool
            .clone()
            .ok_or_else(|| super::TcpError::new("live controller pool unavailable"))?;
        let counters = self.live_counters.clone();
        let timed = |stage| {
            counters.as_deref().map(|counters| WriteTiming {
                counters,
                stage,
                started: Instant::now(),
            })
        };
        let mut unknown = UnknownWriteOnError {
            counters: counters.as_deref(),
            armed: false,
        };
        let sample = timeline.live_group(owner, batches.iter().map(|batch| batch.version()));
        timeline.live_group_event(sample, Stage::LiveGroupBuildStarted);
        let (group, descriptor, frame) = {
            let _timing = timed(LiveTimingStage::Allocation);
            let group = CompleteGroup::new_live(
                &self.shape,
                batches.to_vec(),
                self.expected.batch_limits,
                self.expected.group_limits,
            )
            .map_err(super::TcpError::component)?;
            let (descriptor, frame) =
                LiveGroupDescriptor::from_group(&group).map_err(super::TcpError::component)?;
            (group, descriptor, frame)
        };
        timeline.live_group_event(sample, Stage::LiveGroupBuilt);
        if frame
            .len()
            .checked_add(4095)
            .is_none_or(|n| n > max_inflight_bytes)
        {
            return Err(super::TcpError::new("live frame exceeds queue byte budget"));
        }
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
        }
        timeline.live_group_event(sample, Stage::LiveReserveStarted);
        let reserve = {
            let _timing = timed(LiveTimingStage::Control);
            self.reserve_live_group(epoch, descriptor)
                .map_err(|error| super::TcpError::new(format!("live reserve RPC: {error}")))?
        };
        timeline.live_group_event(sample, Stage::LiveReserveCompleted);
        let (receipts, frame_offset) = match reserve {
            LiveReserveResponse::Existing(receipts) => (receipts, None),
            LiveReserveResponse::Reserved(reserved) => {
                timeline.live_group_token(sample, reserved.reservation.token);
                if reserved.binding.readonly {
                    return Err(super::TcpError::new(
                        "owner returned a read-only write namespace",
                    ));
                }
                let binding = &reserved.binding;
                if !self
                    .live_writer
                    .as_ref()
                    .is_some_and(|writer| writer.matches(binding))
                {
                    timeline.live_group_event(sample, Stage::LiveNamespaceSetupStarted);
                    self.drain_live_writer()?;
                    let target = LiveConnectTarget {
                        nqn: binding.nqn.clone(),
                        uuid: binding.uuid,
                        nsid: binding.nsid,
                        bytes: binding.bytes,
                        block_size: binding.block_size,
                        address: binding.address.clone(),
                        service: binding.service,
                        transport: binding.transport,
                        spdk: binding.spdk.clone(),
                    };
                    let host_nqn = &self
                        .live_config
                        .as_ref()
                        .ok_or_else(|| super::TcpError::new("live NVMe is not configured"))?
                        .host_nqn;
                    let setup_timing = timed(LiveTimingStage::NamespaceSetup);
                    let (lease, acquisition) = pool
                        .get_or_connect(
                            &target,
                            host_nqn,
                            host_id,
                            LiveNamespaceAccess::ReadWrite,
                            Instant::now() + Duration::from_millis(connect_timeout_ms),
                        )
                        .map_err(super::TcpError::component)?;
                    let namespace = lease.open_namespace().map_err(super::TcpError::component)?;
                    let io = LiveIo::new(namespace, max_inflight_requests, max_inflight_bytes)
                        .map_err(super::TcpError::component)?;
                    drop(setup_timing);
                    timeline.live_group_event(sample, Stage::LiveNamespaceSetupCompleted);
                    if let Some(counters) = &self.live_counters
                        && matches!(
                            acquisition,
                            crate::distributed::nvmeof::live_pool::LivePoolAcquisition::Connected
                        )
                    {
                        counters.record_namespace_connect();
                    }
                    if let Some(metrics) = &self.live_metrics {
                        let observed = lease.binding();
                        metrics.record_route(crate::distributed::nvmeof::live_metrics::LiveRouteEvidence {
                            transport: observed.transport,
                            address: binding.address.clone(),
                            service: binding.service,
                            host_nqn: host_nqn.clone(),
                            host_id,
                            target_implementation: crate::distributed::nvmeof::live_metrics::target_implementation(&observed.spdk),
                            spdk: observed.spdk.clone(),
                            writer: Some(crate::distributed::nvmeof::live_metrics::LiveWriterRoute {
                                epoch: reserved.reservation.segment.epoch,
                                file_index: reserved.reservation.segment.file_index,
                                file_identity: Some(crate::distributed::nvmeof::live_target::LiveFileIdentity {
                                    path: reserved.reservation.segment.path.clone(),
                                    device: reserved.reservation.segment.device,
                                    inode: reserved.reservation.segment.inode,
                                    bytes: reserved.reservation.segment.bytes,
                                    block_size: reserved.reservation.segment.block_size,
                                }),
                            }),
                            namespace_identity: crate::distributed::nvmeof::live_metrics::LiveNamespaceIdentity {
                                namespace_path: observed.namespace_path.clone(),
                                controller_instance: lease.controller_instance(),
                                nqn: observed.nqn.clone(),
                                uuid: observed.uuid,
                                nsid: observed.nsid,
                                bytes: observed.bytes,
                                block_size: observed.block_size,
                                target_implementation: crate::distributed::nvmeof::live_metrics::target_implementation(&observed.spdk),
                            },
                        });
                    }
                    self.live_writer = Some(LiveWriteQueue {
                        target,
                        io,
                        _lease: lease,
                    });
                }
                let io = &mut self
                    .live_writer
                    .as_mut()
                    .ok_or_else(|| super::TcpError::new("live write queue missing"))?
                    .io;
                let before = io.stats();
                let offset = reserved.reservation.frame_offset % reserved.reservation.segment.bytes;
                let deadline = Instant::now() + Duration::from_millis(io_timeout_ms);
                timeline.live_group_event(sample, Stage::LiveIoStarted);
                match io
                    .submit_write(1, offset, &frame, deadline)
                    .map_err(|error| {
                        super::TcpError::new(format!("live write submit offset={offset}: {error}"))
                    })? {
                    LiveSubmitOutcome::Submitted => {
                        unknown.armed = true;
                    }
                    LiveSubmitOutcome::Full => {
                        return Err(super::TcpError::new("live write admission is full"));
                    }
                }
                loop {
                    let completions = match io.progress() {
                        Ok(completions) => completions,
                        Err(error) => {
                            let drain = io.shutdown();
                            if let Some(counters) = &counters {
                                record_write_io(counters, &before, &io.stats());
                            }
                            return match drain {
                                Ok(_) => Err(super::TcpError::new(format!(
                                    "live write progress offset={offset}: {error}"
                                ))),
                                Err(drain_error) => Err(super::TcpError::new(format!(
                                    "live write progress failed: {error}; drain failed: {drain_error}"
                                ))),
                            };
                        }
                    };
                    if let Some(completion) = completions.into_iter().next() {
                        if let Some(counters) = &counters {
                            record_write_io(counters, &before, &io.stats());
                        }
                        if completion.id != 1 {
                            return Err(super::TcpError::new("unexpected live write completion"));
                        }
                        match completion.result.map_err(|error| {
                            super::TcpError::new(format!("live write CQE offset={offset}: {error}"))
                        })? {
                            LiveIoResult::Written => break,
                            LiveIoResult::Reads(_) => {
                                return Err(super::TcpError::new("unexpected live write result"));
                            }
                        }
                    }
                    std::thread::yield_now();
                }
                timeline.live_group_event(sample, Stage::LiveIoCompleted);
                if let Some(counters) = &self.live_counters {
                    counters.record_metadata_request();
                }
                timeline.live_group_event(sample, Stage::LiveCompleteStarted);
                let completion = {
                    let _timing = timed(LiveTimingStage::Control);
                    self.complete_live_group_profiled(epoch, reserved.reservation.token)
                        .map_err(|error| {
                            super::TcpError::new(format!(
                                "live completion RPC offset={offset}: {error}"
                            ))
                        })?
                };
                timeline.live_group_event(sample, Stage::LiveCompleteCompleted);
                timeline.live_group_owner(
                    sample,
                    (completion.validation_samples != 0).then_some(completion.validation_ns),
                    completion.persistence,
                );
                if let Some(counters) = &counters {
                    let profile = &completion.persistence;
                    // Persistence contains the WAL and frontier barrier spans;
                    // these nested observations must not be added together.
                    for (stage, ns, samples) in [
                        (
                            LiveTimingStage::OwnerCompletion,
                            completion.validation_ns,
                            completion.validation_samples,
                        ),
                        (
                            LiveTimingStage::Persistence,
                            profile.persist_total_ns,
                            profile.persist_calls,
                        ),
                        (
                            LiveTimingStage::WalPersistence,
                            profile.persist_wal_file_ns,
                            profile.file_persist_calls,
                        ),
                        (
                            LiveTimingStage::WalNamespace,
                            profile.persist_namespace_ns,
                            profile.namespace_persist_calls,
                        ),
                        (
                            LiveTimingStage::FrontierFile,
                            profile.live_frontier_file_ns,
                            profile.live_frontier_file_calls,
                        ),
                        (
                            LiveTimingStage::FrontierNamespace,
                            profile.live_frontier_namespace_ns,
                            profile.live_frontier_namespace_calls,
                        ),
                    ] {
                        if samples != 0 {
                            counters.record_timing(stage, ns, samples);
                        }
                    }
                }
                (completion.receipts, Some(reserved.reservation.frame_offset))
            }
        };
        let _validation_timing = timed(LiveTimingStage::Validation);
        if receipts.len() != batches.len() {
            return Err(super::TcpError::new(
                "live receipt count differs from submitted group",
            ));
        }
        let validated = batches
            .iter()
            .zip(receipts)
            .zip(group.batch_offsets())
            .map(|((batch, receipt), offset)| {
                if let Some(frame_offset) = frame_offset {
                    let expected = frame_offset
                        .checked_add(8)
                        .and_then(|n| n.checked_add(u64::from(*offset)));
                    if expected != Some(receipt.appended().address().offset) {
                        return Err(super::TcpError::new(
                            "live receipt is outside the reserved frame",
                        ));
                    }
                }
                self.check_receipt(batch, super::Receipt::from_durable(receipt))
            })
            .collect::<Result<Vec<_>, _>>()?;
        timeline.live_group_event(sample, Stage::LiveReceiptsValidated);
        unknown.armed = false;
        Ok(validated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn unknown_write_counts_only_accepted_unresolved_writes() {
        use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
        let metrics = LiveNvmeMetrics::new();
        let counters = metrics.register().unwrap();
        drop(UnknownWriteOnError {
            counters: Some(&counters),
            armed: false,
        });
        assert_eq!(metrics.snapshot().counts.unknown_writes, 0);
        drop(UnknownWriteOnError {
            counters: Some(&counters),
            armed: true,
        });
        assert_eq!(metrics.snapshot().counts.unknown_writes, 1);
        let mut resolved = UnknownWriteOnError {
            counters: Some(&counters),
            armed: true,
        };
        resolved.armed = false;
        drop(resolved);
        assert_eq!(metrics.snapshot().counts.unknown_writes, 1);
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn io_timings_use_retired_cq_and_submission_counts() {
        use crate::distributed::nvmeof::{live_io::LiveIoStats, live_metrics::LiveNvmeMetrics};
        let metrics = LiveNvmeMetrics::new();
        let counters = metrics.register().unwrap();
        let stats = LiveIoStats {
            completed_write_ops: 1,
            completed_write_bytes: 4096,
            submissions: 1,
            completions: 1,
            queue_ns: 11,
            submit_ns: 13,
            submit_to_cq_ns: 17,
            ..LiveIoStats::default()
        };
        record_write_io(&counters, &LiveIoStats::default(), &stats);
        let second = LiveIoStats {
            completed_write_ops: 2,
            completed_write_bytes: 8192,
            submissions: 2,
            completions: 2,
            queue_ns: 22,
            submit_ns: 26,
            submit_to_cq_ns: 34,
            ..LiveIoStats::default()
        };
        record_write_io(&counters, &stats, &second);
        let counts = metrics.snapshot().counts;
        assert_eq!(
            (counts.completed_write_ops, counts.completed_write_bytes),
            (2, 8192)
        );
        assert_eq!((counts.queue_ns, counts.queue_samples), (22, 2));
        assert_eq!((counts.submit_ns, counts.submit_samples), (26, 2));
        assert_eq!((counts.cq_ns, counts.cq_samples), (34, 2));
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn local_file_reuses_write_queue_with_per_group_stats() {
        use crate::distributed::nvmeof::{
            live_io::{LiveIo, LiveIoResult, LiveSubmitOutcome},
            live_metrics::LiveNvmeMetrics,
            live_namespace::{LiveNamespace, LiveNamespaceAccess},
        };
        use std::os::unix::fs::FileExt;
        use std::time::{Duration, Instant};
        let dir = tempdir::TempDir::new("live-writer-reuse").unwrap();
        let path = dir.path().join("wal");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(8192).unwrap();
        drop(file);
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadWrite)
                .unwrap();
        let mut io = LiveIo::new(namespace, 1, 8191).unwrap();
        let metrics = LiveNvmeMetrics::new();
        let counters = metrics.register().unwrap();
        for index in 0..2 {
            let before = io.stats();
            let frame = vec![index as u8 + 1; 4096];
            let deadline = Instant::now() + Duration::from_secs(10);
            assert_eq!(
                io.submit_write(1, index * 4096, &frame, deadline).unwrap(),
                LiveSubmitOutcome::Submitted
            );
            loop {
                let mut completions = io.progress().unwrap();
                if let Some(completion) = completions.pop() {
                    assert!(matches!(completion.result, Ok(LiveIoResult::Written)));
                    break;
                }
                assert!(Instant::now() < deadline, "local write did not retire");
                std::thread::yield_now();
            }
            record_write_io(&counters, &before, &io.stats());
        }
        let counts = metrics.snapshot().counts;
        assert_eq!(
            (counts.completed_write_ops, counts.completed_write_bytes),
            (2, 8192)
        );
        assert_eq!((counts.queue_samples, counts.cq_samples), (2, 2));
        assert!(io.shutdown().unwrap().is_empty());
        let file = std::fs::File::open(path).unwrap();
        let mut bytes = [0u8; 8192];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert!(bytes[..4096].iter().all(|&b| b == 1));
        assert!(bytes[4096..].iter().all(|&b| b == 2));
    }

    #[test]
    fn rejects_invalid_initiator_identity_and_queue_limits() {
        let mut config = LiveClientConfig {
            host_nqn: "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-1111-1111-111111111111".into(),
            host_id: [1; 16],
            max_inflight_requests: 32,
            max_inflight_bytes: 4096,
            connect_timeout_ms: 1000,
            io_timeout_ms: 1000,
        };
        assert!(config.validate().is_ok());
        config.host_id = [0; 16];
        assert!(config.validate().is_err());
        config.host_id = [1; 16];
        config.host_nqn.push(',');
        assert!(config.validate().is_err());
        config.host_nqn.pop();
        config.max_inflight_requests = 0;
        assert!(config.validate().is_err());
        config.max_inflight_requests = 32769;
        assert!(config.validate().is_err());
        config.max_inflight_requests = 1;
        config.io_timeout_ms = 0;
        assert!(config.validate().is_err());
    }
}
