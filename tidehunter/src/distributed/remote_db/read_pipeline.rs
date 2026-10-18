//! Bounded lookahead for exact ordered Value-WAL reads.
//!
//! DiskANN (NeurIPS 2019), §3.3, reduces sequential I/O rounds with a small
//! frontier and warns against an oversized frontier. Here the index already
//! knows every exact reference: at most two pages overlap on bounded persistent
//! read lanes. No ANN approximation, skipped result or new wire protocol is
//! inherited. See DISKANN_SOURCE_REVIEW_2026-10-06.md.
//!
//! Tidehunter §3.2 still governs the read: resolve the authoritative index, then
//! verify the referenced immutable WAL member. One captured published cut covers
//! the entire scan, and no read prefetch advances publication or refunds writes.

use super::{
    Bytes, ConcurrentRemoteDatabase, IndexReadCut, KeySpace, LocalDbError, Mutation,
    RemoteDatabase, ValueRecordReference, elapsed,
};
use crate::distributed::native_index::IndexReadPin;
use crate::distributed::native_index::NativeIndexScan;
use std::collections::VecDeque;
use std::mem::size_of;
use std::time::Instant;

const PAGE_RECORDS: usize = 256;
const PAGE_BYTES: usize = 4 * 1024 * 1024;

struct ScanPage {
    rows: Vec<(Bytes, ValueRecordReference)>,
    charged_bytes: usize,
}

pub(crate) struct PendingScanPage {
    page: ScanPage,
    batch: super::pending_read::ReadBatch,
}

impl PendingScanPage {
    pub(crate) fn has_unsubmitted(&self) -> bool {
        self.batch.has_unsubmitted()
    }
    pub(crate) fn oversized(&self) -> bool {
        self.page.oversized()
    }
}

impl ScanPage {
    /// A normal page has bounded payload and metadata. An individually legal
    /// larger record is returned alone and cannot overlap another page. This
    /// preserves legal large values without making lookahead memory unbounded.
    fn next(
        cursor: &mut NativeIndexScan,
        index: &crate::distributed::native_index::NativeIndex,
    ) -> Result<Option<Self>, LocalDbError> {
        let mut page = Self {
            rows: Vec::with_capacity(PAGE_RECORDS),
            charged_bytes: 0,
        };
        while let Some((key, reference)) = cursor.peek() {
            let Some(charge) = page.next_charge(key, reference)? else {
                break;
            };
            page.charged_bytes = charge;
            page.rows.push(cursor.next(index).expect("peeked scan row"));
            if page.rows.len() == PAGE_RECORDS || page.oversized() {
                break;
            }
        }
        Ok((!page.rows.is_empty()).then_some(page))
    }

    fn next_charge(
        &self,
        key: &Bytes,
        reference: &ValueRecordReference,
    ) -> Result<Option<usize>, LocalDbError> {
        let next = record_charge(key, reference)?;
        if !self.rows.is_empty()
            && (self.rows.len() == PAGE_RECORDS
                || next > PAGE_BYTES.saturating_sub(self.charged_bytes))
        {
            return Ok(None);
        }
        self.charged_bytes
            .checked_add(next)
            .map(Some)
            .ok_or_else(|| LocalDbError::new("scan page size overflow"))
    }

    fn oversized(&self) -> bool {
        self.charged_bytes > PAGE_BYTES
    }
}

impl ConcurrentRemoteDatabase {
    pub(crate) fn scan_prefetch_pages(&self) -> usize {
        self.config.scan_prefetch_pages
    }
    pub(crate) fn scan_page_oversized(
        &self,
        rows: &[(Bytes, ValueRecordReference)],
    ) -> Result<bool, LocalDbError> {
        let bytes = rows.iter().try_fold(0usize, |total, (key, reference)| {
            total
                .checked_add(record_charge(key, reference)?)
                .ok_or_else(|| LocalDbError::new("scan page size overflow"))
        })?;
        Ok(bytes > PAGE_BYTES)
    }

    /// Detaches bounded page ownership from the iterator. Dropping the handle
    /// cancels delivery; the read service retains all submitted I/O credits.
    pub(crate) fn submit_scan_page(
        &self,
        rows: Vec<(Bytes, ValueRecordReference)>,
        pin: IndexReadPin,
    ) -> Result<PendingScanPage, LocalDbError> {
        self.ensure_healthy()?;
        if rows.len() > PAGE_RECORDS {
            return Err(LocalDbError::new("scan page exceeds reference bound"));
        }
        let mut charged_bytes = 0usize;
        for (key, reference) in &rows {
            charged_bytes = charged_bytes
                .checked_add(record_charge(key, reference)?)
                .ok_or_else(|| LocalDbError::new("scan page size overflow"))?;
        }
        let mut values = vec![None; rows.len()];
        let mut groups = vec![Vec::new(); self.partition_indexes.len()];
        for (slot, (_, reference)) in rows.iter().enumerate() {
            if let Some(value) = self.cache.lock().get_scan(reference) {
                values[slot] = Some(value);
            } else {
                let node = *self
                    .partition_indexes
                    .get(&reference.batch_address.partition)
                    .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
                groups[node].push((slot, *reference));
            }
        }
        let mut batch = super::pending_read::ReadBatch::new_scan(&self.reads, values, groups)?;
        batch.pin = Some(pin);
        batch.dispatch(&self.reads)?;
        Ok(PendingScanPage {
            page: ScanPage {
                rows,
                charged_bytes,
            },
            batch,
        })
    }

    pub(crate) fn finish_scan_page(
        &self,
        ks: KeySpace,
        mut pending: PendingScanPage,
    ) -> Result<Vec<(Bytes, Bytes)>, LocalDbError> {
        while !pending.batch.complete() {
            self.ensure_healthy()?;
            let generation = self.read_admission_generation();
            pending.batch.dispatch(&self.reads)?;
            if pending.batch.reads.is_empty() {
                self.wait_read_admission(generation, pending.batch.deadline)?;
            } else {
                pending.batch.finish_one().inspect_err(|_| self.poison())?;
            }
        }
        self.ensure_healthy()?;
        let rows = pending
            .page
            .rows
            .into_iter()
            .zip(pending.batch.values)
            .map(|((index_key, reference), mutation)| {
                let mutation =
                    mutation.ok_or_else(|| LocalDbError::new("missing remote scan result"))?;
                if reference.descriptor.keyspace != ks.0 {
                    self.poison();
                    return Err(LocalDbError::new("scan reference has wrong keyspace"));
                }
                checked_row(&self.shape, ks, &index_key, mutation).inspect_err(|_| self.poison())
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.ensure_healthy()?;
        Ok(rows)
    }
    pub(crate) fn bound_scan_page(
        &self,
        rows: Vec<(Bytes, ValueRecordReference)>,
        limit: usize,
    ) -> Result<Vec<(Bytes, ValueRecordReference)>, LocalDbError> {
        let mut page = ScanPage {
            rows,
            charged_bytes: 0,
        };
        let mut selected = 0;
        for (key, reference) in &page.rows {
            let charge = record_charge(key, reference)?;
            if selected != 0
                && (selected == limit.min(PAGE_RECORDS)
                    || charge > PAGE_BYTES.saturating_sub(page.charged_bytes))
            {
                break;
            }
            page.charged_bytes = page
                .charged_bytes
                .checked_add(charge)
                .ok_or_else(|| LocalDbError::new("scan page size overflow"))?;
            selected += 1;
            if page.charged_bytes > PAGE_BYTES {
                break;
            }
        }
        page.rows.truncate(selected);
        Ok(page.rows)
    }
}

fn record_charge(key: &Bytes, reference: &ValueRecordReference) -> Result<usize, LocalDbError> {
    // Bound the record before queueing it, matching Client aggregate read limits.
    // Transport also checks the peer's negotiated frame limit before sending.
    let wire = (reference.descriptor.len as usize)
        .checked_add(8)
        .map(|bytes| bytes.max(256))
        .filter(|bytes| *bytes <= crate::distributed::tcp::HARD_FRAME_LIMIT - 32)
        .ok_or_else(|| LocalDbError::new("one remote record exceeds read transport budget"))?;
    wire.checked_add(key.len())
        // One page row, one per-node request reference, result/slot metadata.
        .and_then(|bytes| bytes.checked_add(size_of::<(Bytes, ValueRecordReference)>() * 2))
        .and_then(|bytes| bytes.checked_add(size_of::<Option<Mutation>>() + size_of::<usize>()))
        .ok_or_else(|| LocalDbError::new("scan record size overflow"))
}

impl RemoteDatabase {
    /// Exact ordered scan at the published cut visible when the call starts.
    ///
    /// The next bounded page can progress while this page is assembled. Each
    /// read lane owns one authenticated sequential stream, separate from writes.
    /// The returned collection itself is caller-sized, as in the previous API;
    /// only additional prefetch storage has the two-page bound.
    pub fn scan(&mut self, ks: KeySpace) -> Result<Vec<(Bytes, Bytes)>, LocalDbError> {
        self.ensure_healthy()?;
        let cut = self
            .latest
            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        // Reject unsupported/invalid keyspace scans before issuing any RPC or
        // poisoning a serving authority for a caller's invalid request.
        let resolve_started = Instant::now();
        let (mut cursor, pin) = {
            let index = self.index.read();
            let pin = index.pin_cut(cut).map_err(LocalDbError::component)?;
            let cursor = index
                .scan_cursor(ks, None, false, pin.cut())
                .map_err(LocalDbError::component)?;
            (cursor, pin)
        };
        self.stats.read_resolve_ns += elapsed(resolve_started);
        let first = self.next_scan_page(&mut cursor)?;
        let started = Instant::now();
        let result = self.scan_prefetched(ks, &mut cursor, first, pin);
        self.stats.value_read_ns += elapsed(started);
        if result
            .as_ref()
            .is_err_and(|error| !super::pending_read::admission_error(error))
        {
            // A failed/fenced response cannot be hidden by subsequent cached
            // reads. Already queued immutable reads may finish; no writes occur.
            self.healthy = false;
        }
        result
    }

    fn next_scan_page(
        &mut self,
        cursor: &mut NativeIndexScan,
    ) -> Result<Option<ScanPage>, LocalDbError> {
        let started = Instant::now();
        let page = ScanPage::next(cursor, &self.index.read());
        self.stats.read_resolve_ns += elapsed(started);
        page
    }

    fn scan_prefetched(
        &mut self,
        ks: KeySpace,
        cursor: &mut NativeIndexScan,
        first: Option<ScanPage>,
        pin: IndexReadPin,
    ) -> Result<Vec<(Bytes, Bytes)>, LocalDbError> {
        let mut ready = first;
        let mut pending: VecDeque<PendingScanPage> = VecDeque::new();
        let mut exhausted = ready.is_none();
        let mut output = Vec::new();
        loop {
            while pending.len() < self.config.scan_prefetch_pages && !exhausted {
                // An incompletely admitted page must finish its own waves
                // before later pages compete for credits it still needs.
                if pending
                    .back()
                    .is_some_and(|page| page.batch.has_unsubmitted())
                {
                    break;
                }
                // Keep an oversized singleton alone, including while its result
                // is pending. Do not resolve or dispatch a second page yet.
                if pending.front().is_some_and(|page| page.page.oversized()) {
                    break;
                }
                let page = match ready.take() {
                    Some(page) => page,
                    None => match self.next_scan_page(cursor)? {
                        Some(page) => page,
                        None => {
                            exhausted = true;
                            break;
                        }
                    },
                };
                if page.oversized() && !pending.is_empty() {
                    ready = Some(page);
                    break;
                }
                pending.push_back(self.dispatch_scan_page(page, pin.clone())?);
            }
            let Some(page) = pending.pop_front() else {
                break;
            };
            // FIFO emission preserves index order even if later nodes/pages
            // finish first. Every missing/error response fails the whole scan.
            self.finish_scan_page(ks, page, &mut output)?;
        }
        Ok(output)
    }

    fn dispatch_scan_page(
        &mut self,
        page: ScanPage,
        pin: IndexReadPin,
    ) -> Result<PendingScanPage, LocalDbError> {
        let mut values = vec![None; page.rows.len()];
        let mut groups = vec![Vec::new(); self.clients.len()];
        for (slot, (_, reference)) in page.rows.iter().enumerate() {
            if let Some(value) = self.read_cache.lock().get_scan(reference) {
                values[slot] = Some(value);
            } else {
                let node = *self
                    .partition_indexes
                    .get(&reference.batch_address.partition)
                    .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
                groups[node].push((slot, *reference));
            }
        }
        let mut batch = super::pending_read::ReadBatch::new_scan(&self.reads, values, groups)?;
        batch.pin = Some(pin);
        batch.dispatch(&self.reads)?;
        Ok(PendingScanPage { page, batch })
    }

    fn finish_scan_page(
        &mut self,
        ks: KeySpace,
        mut pending: PendingScanPage,
        output: &mut Vec<(Bytes, Bytes)>,
    ) -> Result<(), LocalDbError> {
        while !pending.batch.complete() {
            pending.batch.dispatch(&self.reads)?;
            pending.batch.finish_one()?;
        }
        for ((index_key, _), mutation) in pending.page.rows.into_iter().zip(pending.batch.values) {
            let mutation =
                mutation.ok_or_else(|| LocalDbError::new("missing remote scan result"))?;
            output.push(checked_row(&self.shape, ks, &index_key, mutation)?);
        }
        self.ensure_healthy()?;
        // Deliberately no cache fill: a one-pass scan must not evict point reads.
        Ok(())
    }
}

fn checked_row(
    shape: &crate::key_shape::KeyShape,
    ks: KeySpace,
    index_key: &[u8],
    mutation: Mutation,
) -> Result<(Bytes, Bytes), LocalDbError> {
    match mutation {
        Mutation::Put {
            keyspace,
            key,
            value,
        } if keyspace == ks && shape.ks(ks).reduce_key(&key).as_ref() == index_key => {
            Ok((key, value))
        }
        _ => Err(LocalDbError::new("invalid live ordered reference")),
    }
}

#[cfg(test)]
mod tests;
