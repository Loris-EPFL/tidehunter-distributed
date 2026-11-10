//! Authenticated, metadata-only control for direct live WAL access.

use super::*;
use crate::distributed::nvmeof::live_storage::{
    LiveGroupDescriptor, LiveWalSegment, LiveWriteReservation,
};
use crate::distributed::nvmeof::live_target::LiveTargetBinding;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveReserved {
    pub reservation: LiveWriteReservation,
    pub binding: LiveTargetBinding,
}

pub enum LiveReserveResponse {
    Existing(Vec<DurableBatch>),
    Reserved(LiveReserved),
}

#[derive(Serialize, Deserialize)]
pub(super) enum LiveReserveWire {
    Existing(Vec<Receipt>),
    Reserved(LiveReserved),
}
/// Owner-side validation and persistence observations, separate from RPC RTT.
pub struct LiveCompletion {
    pub receipts: Vec<DurableBatch>,
    pub validation_ns: u64,
    pub validation_samples: u64,
    pub persistence: crate::distributed::storage::StorageWriteProfile,
}

#[derive(Serialize, Deserialize)]
pub(super) struct LiveCompletionWire {
    pub(super) receipts: Vec<Receipt>,
    pub(super) validation_ns: u64,
    pub(super) validation_samples: u64,
    pub(super) persistence: crate::distributed::storage::StorageWriteProfile,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveRecordLocation {
    pub reference: ValueRecordReference,
    pub segment: LiveWalSegment,
    pub binding: LiveTargetBinding,
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveRecordLocations {
    pub entries: Vec<LiveRecordLocation>,
    pub epoch_pin_token: [u8; 16],
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

#[derive(Serialize, Deserialize)]
pub(super) struct LiveRecoveryEntryWire {
    pub(super) receipt: Receipt,
    pub(super) segment: LiveWalSegment,
    pub(super) binding: LiveTargetBinding,
    pub(super) offset: u64,
    pub(super) len: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct LiveRecoveryWire {
    pub(super) entries: Vec<LiveRecoveryEntryWire>,
    pub(super) next: Option<usize>,
    pub(super) epoch_pin_token: [u8; 16],
}

impl Client {
    /// Release a live grant only after the caller has retired all associated I/O.
    pub fn release_live_epoch_pin(&mut self, epoch: u64, token: [u8; 16]) -> Result<(), TcpError> {
        match self.request(Request::ReleaseLiveEpochPin { epoch, token })? {
            Response::NvmeofEpochReleased => Ok(()),
            _ => self.wrong_response(),
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

    pub(super) fn valid_live_record(
        &self,
        entry: &LiveRecordLocation,
        reference: &ValueRecordReference,
    ) -> bool {
        entry.reference == *reference
            && self.valid_live_segment(
                &entry.segment,
                &entry.binding,
                reference.batch_version.epoch,
                reference.batch_address.offset,
            )
            && reference.batch_id.database == self.expected.inventory.database
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
            && entry.len == u64::from(reference.descriptor.len)
            && (reference.batch_address.offset % entry.segment.bytes)
                .checked_add(8)
                .and_then(|offset| offset.checked_add(u64::from(reference.descriptor.offset)))
                == Some(entry.offset)
            && entry
                .offset
                .checked_add(entry.len)
                .is_some_and(|end| end <= entry.segment.bytes)
    }

    pub fn reserve_live_group(
        &mut self,
        epoch: u64,
        descriptor: LiveGroupDescriptor,
    ) -> Result<LiveReserveResponse, TcpError> {
        let expected = descriptor.clone();
        match self.request(Request::ReserveLiveGroup { epoch, descriptor })? {
            Response::LiveReserved(LiveReserveWire::Reserved(reserved)) => {
                let reservation = &reserved.reservation;
                if !self.valid_live_segment(
                    &reservation.segment,
                    &reserved.binding,
                    epoch,
                    reservation.frame_offset,
                ) || reservation.token == [0; 16]
                    || reservation.frame_len != expected.frame_len
                    || reservation.frame_offset % u64::from(reservation.segment.block_size) != 0
                    || (reservation.frame_offset % reservation.segment.bytes)
                        .checked_add(u64::from(reservation.frame_len))
                        .is_none_or(|end| end > reservation.segment.bytes)
                {
                    return self.wrong_response();
                }
                Ok(LiveReserveResponse::Reserved(reserved))
            }
            Response::LiveReserved(LiveReserveWire::Existing(receipts)) => {
                if receipts.len() != expected.batches.len() {
                    return self.wrong_response();
                }
                let mut durable = Vec::with_capacity(receipts.len());
                for (receipt, summary) in receipts.into_iter().zip(expected.batches) {
                    if receipt.id != summary.id
                        || receipt.version != summary.version
                        || receipt.digest != summary.digest
                        || receipt.version.epoch != epoch
                        || receipt.id.database != self.expected.inventory.database
                        || self
                            .expected
                            .inventory
                            .partition_for(receipt.id)
                            .map_err(TcpError::component)?
                            != self.expected.partition
                        || receipt.address.partition != self.expected.partition
                        || receipt.address.segment_generation != epoch
                        || receipt.address.len != summary.encoded_len.saturating_add(8)
                    {
                        return self.wrong_response();
                    }
                    durable.push(receipt.durable());
                }
                Ok(LiveReserveResponse::Existing(durable))
            }
            response => self.invalid_live_response(epoch, response),
        }
    }

    pub fn complete_live_group(
        &mut self,
        epoch: u64,
        token: [u8; 16],
    ) -> Result<Vec<DurableBatch>, TcpError> {
        self.complete_live_group_profiled(epoch, token)
            .map(|completion| completion.receipts)
    }

    /// Returns owner validation and actual persistence timings, not wire RTT.
    pub fn complete_live_group_profiled(
        &mut self,
        epoch: u64,
        token: [u8; 16],
    ) -> Result<LiveCompletion, TcpError> {
        match self.request(Request::CompleteLiveGroup { epoch, token })? {
            Response::LiveCompleted(completion)
                if !completion.receipts.is_empty()
                    && completion.receipts.len()
                        <= self.expected.group_limits.max_batches as usize
                    && completion.validation_samples <= 1
                    && (completion.validation_samples != 0 || completion.validation_ns == 0) =>
            {
                let mut durable = Vec::with_capacity(completion.receipts.len());
                for receipt in completion.receipts {
                    if receipt.version.epoch != epoch
                        || receipt.id.database != self.expected.inventory.database
                        || self
                            .expected
                            .inventory
                            .partition_for(receipt.id)
                            .map_err(TcpError::component)?
                            != self.expected.partition
                        || receipt.address.partition != self.expected.partition
                        || receipt.address.segment_generation != epoch
                        || receipt
                            .address
                            .offset
                            .checked_add(u64::from(receipt.address.len))
                            .is_none()
                    {
                        return self.wrong_response();
                    }
                    durable.push(receipt.durable());
                }
                Ok(LiveCompletion {
                    receipts: durable,
                    validation_ns: completion.validation_ns,
                    validation_samples: completion.validation_samples,
                    persistence: completion.persistence,
                })
            }
            response => self.invalid_live_response(epoch, response),
        }
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
                        .all(|(entry, reference)| self.valid_live_record(entry, reference))
                {
                    return self
                        .invalid_live_metadata(authority_epoch, Some(locations.epoch_pin_token));
                }
                Ok(locations)
            }
            response => self.invalid_live_response(authority_epoch, response),
        }
    }

    pub fn recover_live_metadata(
        &mut self,
        authority_epoch: u64,
        source_epoch: u64,
        cursor: usize,
        limit: usize,
    ) -> Result<LiveRecoveryPage, TcpError> {
        if limit == 0 || limit > self.page_limit {
            return Err(TcpError::new("invalid live recovery page limit"));
        }
        match self.request(Request::RecoverLiveMetadata {
            authority_epoch,
            source_epoch,
            cursor,
            limit: limit.min(256),
        })? {
            Response::LiveRecovery(page) => {
                if page.epoch_pin_token == [0; 16]
                    || page.entries.len() > limit.min(256)
                    || !page.next.is_none_or(|next| {
                        !page.entries.is_empty()
                            && cursor.checked_add(page.entries.len()) == Some(next)
                    })
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
            _ => self.wrong_response(),
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
