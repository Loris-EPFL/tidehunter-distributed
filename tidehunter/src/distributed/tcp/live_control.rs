//! Authenticated, metadata-only control for direct live WAL access.

use super::*;
use crate::distributed::nvmeof::live_storage::{LiveTargetBinding, LiveWalSegment};

/// Immutable authenticated connection recipe shared by cold pool operations.
pub(crate) struct LivePinRecipe {
    pub(crate) key: (String, [u8; 32], u32),
    tls: TlsFiles,
    config: ClientTlsConfig,
    timeout: Duration,
    expected: PartitionConfig,
    shape: KeyShape,
}

impl LivePinRecipe {
    pub(crate) fn new(
        endpoint: &str,
        tls: &TlsFiles,
        config: &ClientTlsConfig,
        timeout: Duration,
        expected: &PartitionConfig,
        shape: &KeyShape,
    ) -> Self {
        Self {
            key: (endpoint.to_owned(), config.identity, expected.partition),
            tls: tls.clone(),
            config: config.clone(),
            timeout,
            expected: expected.clone(),
            shape: shape.clone(),
        }
    }
}

pub(crate) struct LiveExportPinRequest<'a> {
    pub(crate) recipe: &'a Arc<LivePinRecipe>,
    pub(crate) epoch: u64,
    pub(crate) segment: &'a LiveWalSegment,
    pub(crate) binding: &'a LiveTargetBinding,
}

/// One metadata session per owner, never a caller's data/control session.
/// This per-owner lock is cold-path only; cache hits never acquire it.
pub(crate) struct LiveExportPinControl {
    #[cfg(test)]
    lose_next_reply: std::sync::atomic::AtomicBool,
    recipe: Arc<LivePinRecipe>,
    client: Mutex<Option<Client>>,
}

#[derive(Debug)]
pub(crate) struct LivePinAcquireError {
    pub(crate) error: io::Error,
    pub(crate) outcome_unknown: bool,
}

impl LiveExportPinControl {
    pub(crate) fn new(recipe: Arc<LivePinRecipe>) -> Self {
        Self {
            recipe,
            client: Mutex::new(None),
            #[cfg(test)]
            lose_next_reply: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn lose_next_reply(&self) {
        // Independent test fault flag, read only after a real owner reply.
        self.lose_next_reply.store(true, Ordering::Relaxed);
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        request: &LiveExportPinRequest<'_>,
    ) -> Result<LiveExportPin, LivePinAcquireError> {
        let mut slot = self.client.lock();
        // Only a new acquisition may replace a failed session. Release must
        // retain an uncertain token for the next authority fence, not reconnect.
        if slot.as_ref().is_none_or(|client| client.broken) {
            *slot = Some(
                Client::connect_shared(
                    &self.recipe.key.0,
                    &self.recipe.tls,
                    self.recipe.config.clone(),
                    self.recipe.timeout,
                    self.recipe.expected.clone(),
                    &self.recipe.shape,
                )
                .map_err(|error| LivePinAcquireError {
                    error: io::Error::other(error),
                    outcome_unknown: false,
                })?,
            );
        }
        let client = slot.as_mut().ok_or_else(|| LivePinAcquireError {
            error: io::Error::other("export pin session unavailable"),
            outcome_unknown: false,
        })?;
        let response = client
            .request(Request::AcquireLiveExportPin {
                epoch: request.epoch,
                segment: request.segment.clone(),
                binding: request.binding.clone(),
            })
            .map_err(|error| LivePinAcquireError {
                error: io::Error::other(error),
                outcome_unknown: client.broken,
            })?;
        #[cfg(test)]
        // Independent test fault flag; the owner has already executed the RPC.
        if self.lose_next_reply.swap(false, Ordering::Relaxed) {
            client.broken = true;
            client
                .stream
                .sock
                .socket
                .shutdown(std::net::Shutdown::Both)
                .map_err(|error| LivePinAcquireError {
                    error,
                    outcome_unknown: true,
                })?;
            return Err(LivePinAcquireError {
                error: io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "injected lost export pin reply",
                ),
                outcome_unknown: true,
            });
        }
        match response {
            Response::NvmeofEpochPinned(token) if token != [0; 16] => Ok(LiveExportPin {
                control: self.clone(),
                epoch: request.epoch,
                token,
            }),
            _ => {
                client.broken = true;
                Err(LivePinAcquireError {
                    error: io::Error::other("invalid live export pin response"),
                    outcome_unknown: true,
                })
            }
        }
    }
}

pub(crate) struct LiveExportPin {
    control: Arc<LiveExportPinControl>,
    epoch: u64,
    token: [u8; 16],
}

impl LiveExportPin {
    /// Caller has confirmed that the controller is gone. An error retains the
    /// token; it may only be discarded after a subsequent authority fence.
    pub(crate) fn release(&mut self) -> io::Result<()> {
        let mut slot = self.control.client.lock();
        let client = slot
            .as_mut()
            .ok_or_else(|| io::Error::other("export pin release session unavailable"))?;
        match client
            .request(Request::ReleaseLiveExportPin {
                epoch: self.epoch,
                token: self.token,
            })
            .map_err(io::Error::other)?
        {
            Response::NvmeofEpochReleased => Ok(()),
            _ => {
                client.broken = true;
                Err(io::Error::other("invalid live export pin release response"))
            }
        }
    }
}

pub struct LiveRecoveryEntry {
    pub receipt: DurableBatch,
    pub segment: LiveWalSegment,
    pub binding: LiveTargetBinding,
    pub offset: u64,
    pub len: u64,
}

pub struct LiveRecoveryPage {
    pub entries: Vec<LiveRecoveryEntry>,
    pub next: Option<usize>,
    pub epoch_pin_token: [u8; 16],
}

impl Client {
    /// Runs on the live lane owner after its CQEs and grant leases retire.
    pub(crate) fn drain_poisoned_live_pool(&self) -> Result<(), TcpError> {
        if let Some(pool) = &self.live_pool {
            pool.drain_idle(Instant::now() + self.timeout)
                .map_err(TcpError::component)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn live_config_for_test(&self) -> Option<LiveClientConfig> {
        self.live_config.clone()
    }

    #[cfg(test)]
    pub(crate) fn export_pin_request<'a>(
        &'a self,
        epoch: u64,
        segment: &'a LiveWalSegment,
        binding: &'a LiveTargetBinding,
    ) -> LiveExportPinRequest<'a> {
        LiveExportPinRequest {
            recipe: &self.live_pin_recipe,
            epoch,
            segment,
            binding,
        }
    }

    pub(crate) fn open_live_exports(&mut self, authority_epoch: u64) -> Result<(), TcpError> {
        use super::super::nvmeof::live_metrics::LiveTimingStage;
        let started = Instant::now();
        if let Some(counters) = &self.live_counters {
            counters.record_metadata_request();
        }
        let response = self.request(Request::OpenLiveExports { authority_epoch });
        if let Some(counters) = &self.live_counters {
            counters.record_timing(
                LiveTimingStage::Control,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                1,
            );
        }
        let response = response?;
        let Response::LiveExports(info) = response else {
            return self.wrong_variant("OpenLiveExports", &response, "LiveExports");
        };
        if self.live_config.is_none() {
            self.broken = true;
            return Err(TcpError::new("live NVMe is not configured"));
        }
        if info.wal_file_bytes != self.expected.wal_file_bytes {
            self.broken = true;
            return Err(TcpError::new(
                "live WAL file size differs between owner and coordinator",
            ));
        }
        if info.block_size != 4096
            || info.service == 0
            || info.address.parse::<std::net::IpAddr>().is_err()
        {
            return self.wrong_response("OpenLiveExports", format_args!("block_size={} (expected 4096), service={} (expected nonzero), address={} (expected IP address)", info.block_size, info.service, info.address));
        }
        let count = info.exported_files;
        self.live_exports = Some((authority_epoch, info, count));
        Ok(())
    }

    /// Validate extent authority and geometry. The caller must compare `tier`
    /// with its required write policy before issuing any namespace write.
    pub fn lease_live_extent(&mut self, epoch: u64) -> Result<LiveLeased, TcpError> {
        match self.request(Request::LeaseLiveExtent { epoch })? {
            Response::LiveLeased(leased) if self.valid_live_extent(&leased, epoch) => Ok(leased),
            response => self.wrong_variant(
                "LeaseLiveExtent",
                &response,
                "LiveLeased with valid extent authority and geometry",
            ),
        }
    }

    /// The deadline follows `Request::timeout`, as the owner's does: a
    /// prelease may create the next file's export, a lifecycle operation.
    pub(crate) fn submit_prelease_live_extent(
        &mut self,
        epoch: u64,
        after: [u8; 16],
    ) -> Result<bool, TcpError> {
        let request = Request::PreleaseLiveExtent { epoch, after };
        self.submit_control(&request, Instant::now() + request.timeout(self.timeout))
    }

    pub(crate) fn take_prelease_live_extent(
        &mut self,
        epoch: u64,
    ) -> Option<Result<LiveLeased, TcpError>> {
        self.take_control().map(|response| match response? {
            Response::LiveLeased(leased) if self.valid_live_extent(&leased, epoch) => Ok(leased),
            response => self.wrong_variant(
                "PreleaseLiveExtent",
                &response,
                "LiveLeased with valid extent authority and geometry",
            ),
        })
    }

    pub(super) fn valid_live_extent(&self, leased: &LiveLeased, epoch: u64) -> bool {
        let extent = &leased.extent;
        let fragment = self.expected.fragment_bytes;
        self.valid_live_segment(&extent.segment, &leased.binding, epoch, extent.start)
            && (self.live_config.is_none() || {
                let name = super::super::nvmeof::live_storage::wal_export_name(
                    super::super::nvmeof::live_storage::FILES_PER_SUBSYSTEM,
                    extent.segment.database,
                    extent.segment.partition,
                    epoch,
                    extent.segment.file_index,
                );
                leased.binding.permanent
                    && leased.binding.nqn == name.nqn
                    && leased.binding.nsid == name.nsid
                    && leased.binding.uuid == name.uuid
            })
            && extent.token != [0; 16]
            && !leased.binding.readonly
            && extent.segment.block_size == 4096
            && extent.start.is_multiple_of(4096)
            && extent.end.is_multiple_of(4096)
            && extent.end > extent.start
            && extent.end - extent.start < 1 << 30
            && fragment != 0
            && extent.end - extent.start == fragment - extent.start % fragment
            && (extent.start % extent.segment.bytes)
                .checked_add(extent.end - extent.start)
                .is_some_and(|end| end <= extent.segment.bytes)
    }

    /// Return the cumulative adoption proof and, when requested, the next lease
    /// with its frozen tier. Policy comparison belongs to the coordinator.
    pub fn adopt_live_extent(
        &mut self,
        epoch: u64,
        token: [u8; 16],
        through: u64,
        close: bool,
        lease_next: bool,
    ) -> Result<LiveAdopted, TcpError> {
        let response = self.request(Request::AdoptLiveExtent {
            epoch,
            token,
            through,
            close,
            lease_next,
        })?;
        self.validate_live_adoption(response, epoch, token, through, close, lease_next)
    }

    pub(crate) fn submit_adopt_live_extent(
        &mut self,
        epoch: u64,
        token: [u8; 16],
        through: u64,
        close: bool,
        lease_next: bool,
    ) -> Result<bool, TcpError> {
        let request = Request::AdoptLiveExtent {
            epoch,
            token,
            through,
            close,
            lease_next,
        };
        self.submit_control(&request, Instant::now() + request.timeout(self.timeout))
    }

    pub(crate) fn take_adopt_live_extent(
        &mut self,
        epoch: u64,
        token: [u8; 16],
        through: u64,
        close: bool,
        lease_next: bool,
    ) -> Option<Result<LiveAdopted, TcpError>> {
        self.take_control().map(|response| {
            self.validate_live_adoption(response?, epoch, token, through, close, lease_next)
        })
    }

    fn validate_live_adoption(
        &mut self,
        response: Response,
        epoch: u64,
        token: [u8; 16],
        through: u64,
        close: bool,
        lease_next: bool,
    ) -> Result<LiveAdopted, TcpError> {
        match response {
            Response::LiveAdopted(reply)
                if reply.adoption.token == token
                    && reply.adoption.adopted_end >= through
                    && reply.adoption.adopted_end.is_multiple_of(4096)
                    && (!close || reply.adoption.adopted_end == through)
                    && reply.adoption.closed == close
                    && reply.next.is_some() == lease_next
                    && (!lease_next || close)
                    && reply
                        .next
                        .as_ref()
                        .is_none_or(|next| self.valid_live_extent(next, epoch)) =>
            {
                Ok(reply)
            }
            response => self.wrong_variant("AdoptLiveExtent", &response, "LiveAdopted with matching token, aligned adopted frontier, close state and next lease"),
        }
    }

    /// Release a live grant only after the caller has retired all associated I/O.
    pub fn release_live_epoch_pin(&mut self, epoch: u64, token: [u8; 16]) -> Result<(), TcpError> {
        match self.request(Request::ReleaseLiveEpochPin { epoch, token })? {
            Response::NvmeofEpochReleased => Ok(()),
            response => self.wrong_variant("ReleaseLiveEpochPin", &response, "NvmeofEpochReleased"),
        }
    }

    fn invalid_live_metadata<T>(
        &mut self,
        epoch: u64,
        token: Option<[u8; 16]>,
    ) -> Result<T, TcpError> {
        let cleanup = match token {
            Some(token) if token != [0; 16] => self.release_live_epoch_pin(epoch, token),
            _ => Err(TcpError::new(
                "live grant token unavailable; owner may retain a quarantined pin",
            )),
        };
        self.broken = true;
        match cleanup {
            Ok(()) => Err(TcpError::new(
                "invalid authenticated live metadata response; grant released",
            )),
            Err(error) => Err(TcpError::new(format!(
                "invalid authenticated live metadata response; grant cleanup failed or outcome unknown: {error}"
            ))),
        }
    }

    fn invalid_live_response<T>(&mut self, epoch: u64, response: Response) -> Result<T, TcpError> {
        let token = match response {
            Response::LiveRecords(locations) => Some(locations.epoch_pin_token),
            Response::LiveRecovery(page) => Some(page.epoch_pin_token),
            _ => None,
        };
        self.invalid_live_metadata(epoch, token)
    }

    pub(super) fn valid_live_segment(
        &self,
        segment: &LiveWalSegment,
        binding: &LiveTargetBinding,
        epoch: u64,
        global_offset: u64,
    ) -> bool {
        segment.database == self.expected.inventory.database
            && segment.partition == self.expected.partition
            && segment.epoch == epoch
            && segment.generation == epoch
            && segment.bytes == self.expected.wal_file_bytes
            && segment.bytes != 0
            && segment.file_index == global_offset / segment.bytes
            && segment.block_size == binding.block_size
            && segment.bytes == binding.bytes
            && segment.block_size >= 512
            && segment.block_size <= 4096
            && segment.block_size.is_power_of_two()
            && segment.bytes.is_multiple_of(u64::from(segment.block_size))
    }

    /// Validate member geometry against a retained file grant without copying it.
    pub(super) fn valid_live_record_geometry(
        &self,
        segment: &LiveWalSegment,
        binding: &LiveTargetBinding,
        reference: &ValueRecordReference,
        offset: u64,
        len: u64,
    ) -> bool {
        self.valid_live_segment(
            segment,
            binding,
            reference.batch_version.epoch,
            reference.batch_address.offset,
        ) && self.valid_record_range(segment.bytes, reference, offset, len)
    }

    pub(super) fn valid_record_range(
        &self,
        file_bytes: u64,
        reference: &ValueRecordReference,
        offset: u64,
        len: u64,
    ) -> bool {
        reference.batch_id.database == self.expected.inventory.database
            && self
                .expected
                .inventory
                .partition_for(reference.batch_id)
                .ok()
                == Some(self.expected.partition)
            && reference.batch_address.partition == self.expected.partition
            && reference.batch_address.segment_generation == reference.batch_version.epoch
            && reference
                .batch_address
                .len
                .checked_sub(8)
                .is_some_and(|len| {
                    reference
                        .descriptor
                        .offset
                        .checked_add(reference.descriptor.len)
                        .is_some_and(|end| end <= len)
                })
            && len == u64::from(reference.descriptor.len)
            && (reference.batch_address.offset % file_bytes)
                .checked_add(8)
                .and_then(|offset| offset.checked_add(u64::from(reference.descriptor.offset)))
                == Some(offset)
            && offset.checked_add(len).is_some_and(|end| end <= file_bytes)
    }

    pub fn resolve_live_records(
        &mut self,
        authority_epoch: u64,
        references: &[ValueRecordReference],
    ) -> Result<LiveRecordLocations, TcpError> {
        if references.is_empty() || references.len() > self.max_reads.min(256) {
            return Err(TcpError::new("live record count exceeds negotiated bound"));
        }
        match self.request(Request::ResolveLiveRecords {
            authority_epoch,
            references: references.to_vec(),
        })? {
            Response::LiveRecords(locations) => {
                if locations.epoch_pin_token == [0; 16]
                    || locations.entries.len() != references.len()
                    || !locations
                        .entries
                        .iter()
                        .zip(references)
                        .all(|(entry, reference)| {
                            entry.reference == *reference
                                && self.valid_live_record_geometry(
                                    &entry.segment,
                                    &entry.binding,
                                    reference,
                                    entry.offset,
                                    entry.len,
                                )
                        })
                {
                    return self
                        .invalid_live_metadata(authority_epoch, Some(locations.epoch_pin_token));
                }
                Ok(locations)
            }
            response => self.invalid_live_response(authority_epoch, response),
        }
    }

    /// Validate one `ReplayLiveWal` page: `next` is a WAL byte offset beyond
    /// `from`, and a page may exceed `limit` by at most one whole group.
    pub(super) fn validate_live_recovery_page(
        &mut self,
        response: Response,
        authority_epoch: u64,
        source_epoch: u64,
        from: usize,
        limit: usize,
    ) -> Result<LiveRecoveryPage, TcpError> {
        match response {
            Response::LiveRecovery(page) => {
                if page.epoch_pin_token == [0; 16]
                    || page.entries.len()
                        > limit.max(self.expected.group_limits.max_batches as usize)
                    || !page
                        .next
                        .is_none_or(|next| !page.entries.is_empty() && next > from)
                {
                    return self.invalid_live_metadata(authority_epoch, Some(page.epoch_pin_token));
                }
                let mut entries = Vec::with_capacity(page.entries.len());
                for entry in page.entries {
                    if entry.receipt.version.epoch != source_epoch
                        || entry.receipt.id.database != self.expected.inventory.database
                        || self.expected.inventory.partition_for(entry.receipt.id).ok()
                            != Some(self.expected.partition)
                        || entry.receipt.address.partition != self.expected.partition
                        || entry.receipt.address.segment_generation != source_epoch
                        || !self.valid_live_segment(
                            &entry.segment,
                            &entry.binding,
                            source_epoch,
                            entry.receipt.address.offset,
                        )
                        || entry.offset != entry.receipt.address.offset % entry.segment.bytes
                        || entry.len != u64::from(entry.receipt.address.len)
                        || entry
                            .offset
                            .checked_add(entry.len)
                            .is_none_or(|end| end > entry.segment.bytes)
                    {
                        return self
                            .invalid_live_metadata(authority_epoch, Some(page.epoch_pin_token));
                    }
                    entries.push(LiveRecoveryEntry {
                        receipt: entry.receipt.durable(),
                        segment: entry.segment,
                        binding: entry.binding,
                        offset: entry.offset,
                        len: entry.len,
                    });
                }
                Ok(LiveRecoveryPage {
                    entries,
                    next: page.next,
                    epoch_pin_token: page.epoch_pin_token,
                })
            }
            response => self.invalid_live_response(authority_epoch, response),
        }
    }
    /// Snapshot of actual owner target lifecycle operations.
    pub fn live_target_stats(
        &mut self,
    ) -> Result<crate::distributed::nvmeof::LiveTargetStats, TcpError> {
        match self.request(Request::LiveTargetStats)? {
            Response::LiveTargetStats(stats) => Ok(stats),
            Response::LiveRecords(_) | Response::LiveRecovery(_) => {
                self.broken = true;
                Err(TcpError::new(
                    "unexpected live grant without authority epoch; owner pin may be quarantined",
                ))
            }
            response => self.wrong_variant("LiveTargetStats", &response, "LiveTargetStats"),
        }
    }
    /// Shut down a sealed owner target after all initiator I/O and pins drain.
    pub fn shutdown_live_target(
        &mut self,
        epoch: u64,
    ) -> Result<crate::distributed::nvmeof::LiveTargetStats, TcpError> {
        match self.request(Request::ShutdownLiveTarget { epoch })? {
            Response::LiveTargetStats(stats) if stats.owned_namespace_count == 0 => Ok(stats),
            response => self.invalid_live_response(epoch, response),
        }
    }
}
