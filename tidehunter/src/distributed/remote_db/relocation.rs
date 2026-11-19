//! R1 logical pruning pass (CAPACITY-R0-R1.md §5): native `WalBased`
//! relocation semantics on leased owner WALs, without a logical write and
//! without the exclusive Sui write gate.
//!
//! One pass:
//! 1. Under `index writer -> publisher`, freeze a contiguous stable cut `Q`
//!    and, per retained stream `(partition, fencing generation)`, the scan
//!    interval `[callback start, required end)`. The required end covers
//!    every batch at or below `Q` (published receipts and adopted positions),
//!    so a lagging owner cannot hide an earlier Stop. A pin at `Q` holds the
//!    retention floor.
//! 2. Page each stream's original batches from its owner (`RelocationPage`,
//!    metadata only) and merge them in global `(fencing generation, batch
//!    sequence, member)` order with one page and one head per stream. Every
//!    authenticated put of a filtered keyspace is read through the ordinary
//!    pinned member read and passed to the Sui filter once, including puts
//!    the index no longer holds. The first Stop ends all callbacks, including
//!    later members of its batch.
//! 3. Per stream: terminal = the Stop batch (Stop stream), the first batch
//!    after Stop (other streams), the first batch above `Q`, or the clean
//!    tail rule at the processed upper. The physical target is the native
//!    rule `round_down_to_file(min(terminal, lo + budget))` with the per-owner
//!    `relocation_max_reclaim_pct` budget. Without one complete file before a
//!    target the pass is `NoWork`: no plan, floor, start or retirement.
//! 4. `DidWork`: each observed `Remove` before the Stop batch that is still
//!    the exact head at `Q` becomes a prune candidate; the plan, the raised
//!    callback starts and the floor `min(Q, before Stop)` go to the next
//!    Exact capture as one transaction. Then bounded physical retirement.
//!
//! Index membership gates only the application of a removal, never a
//! callback. The callback scan is not percentage-capped.

use super::checkpoint_lifecycle::{core_progress_checkpoint, core_start_relocation_checkpoint};
use super::*;
use crate::distributed::capacity::{OwnerSpan, span_budget_end};
use crate::distributed::native_index::{IndexReadPin, PruneCandidate, PrunePlan};
use crate::distributed::storage::RelocationBatch;
use std::sync::atomic::{AtomicBool, Ordering};

/// Batches requested per owner page; one page per stream is the merge state.
const PAGE_BATCHES: u16 = 256;
/// Pause between polls of an owner whose adoption is behind the required end.
const BEHIND_POLL: Duration = Duration::from_millis(2);

/// Sent `Ok` once the capture is adopted, live-installed and cleaned up.
pub(super) type RelocationReply = mpsc::SyncSender<Result<(), LocalDbError>>;

/// One relocation's contribution to the next Exact capture.
pub(super) struct RelocationCapture {
    pub plan: PrunePlan,
    /// Proposed starts at whole-file targets; merged monotonically at freeze.
    pub callback_starts: BTreeMap<(u32, u64), u64>,
    pub reply: RelocationReply,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RelocationGate {
    NoWork,
    DidWork,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallbackDecision {
    Keep,
    Remove,
    Stop,
}

/// One callback, recorded only for an observed (test) pass.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(feature = "test_methods"),
    allow(dead_code, reason = "read only by the observed test pass")
)]
pub(crate) struct CallbackRecord {
    pub keyspace: KeySpace,
    pub key: Bytes,
    pub value: Bytes,
    /// Stream-local offset of the member's batch frame.
    pub wal_position: u64,
    pub decision: CallbackDecision,
}

/// Per scanned stream bounds, sorted by `(partition, fencing_generation)`.
/// Offsets of different streams are never comparable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamReport {
    pub partition: u32,
    pub fencing_generation: u64,
    /// Oldest unretired position at or after the callback start.
    pub lo: u64,
    pub processed_upper: u64,
    pub terminal_position: u64,
    pub rounded_target: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(
    not(feature = "test_methods"),
    allow(dead_code, reason = "production passes report through fail-stop only")
)]
pub(crate) struct RelocationReport {
    pub gate: RelocationGate,
    pub streams: Vec<StreamReport>,
    pub callbacks: Vec<CallbackRecord>,
}

/// Raise the adopted starts by the proposals of one relocation capture.
/// Every proposal names a retained stream and is file-aligned; a proposal
/// below the adopted start leaves the adopted one (starts never move back).
pub(super) fn merge_callback_starts(
    adopted: &BTreeMap<(u32, u64), u64>,
    proposed: &BTreeMap<(u32, u64), u64>,
    retained_epochs: &[Vec<u64>],
    partition_indexes: &HashMap<u32, usize>,
    wal_file_bytes: u64,
) -> Result<BTreeMap<(u32, u64), u64>, LocalDbError> {
    let mut merged = adopted.clone();
    for (&(partition, generation), &from) in proposed {
        let retained = partition_indexes
            .get(&partition)
            .and_then(|node| retained_epochs.get(*node))
            .is_some_and(|epochs| epochs.contains(&generation));
        if !retained || from % wal_file_bytes != 0 {
            return Err(LocalDbError::new(
                "relocation callback start names no retained stream or is not file-aligned",
            ));
        }
        let start = merged.entry((partition, generation)).or_insert(0);
        *start = (*start).max(from);
    }
    Ok(merged)
}

/// One retained stream frozen with `Q`.
struct StreamScan {
    node: usize,
    partition: u32,
    generation: u64,
    from: u64,
    required_end: u64,
}

/// Merge cursor of one stream: at most one owner page of batches.
struct Cursor {
    scan: StreamScan,
    next: Option<u64>,
    buffer: VecDeque<RelocationBatch>,
    /// First unretired offset at or after `from`, from the first page.
    start: Option<u64>,
    /// Set once nothing more will be fetched.
    upper: Option<u64>,
}

struct Removal {
    keyspace: KeySpace,
    key: Bytes,
    reference: ValueRecordReference,
}

fn cancelled() -> LocalDbError {
    LocalDbError::new("relocation pass cancelled by shutdown")
}

impl ConcurrentRemoteDatabase {
    /// One production pass. Native panics its relocator on a pass error and
    /// its background health then fails every later operation; the leased
    /// pass fail-stops the authority the same way. A shutdown cancellation is
    /// not a failure.
    pub(crate) fn background_relocation_pass(&self, cancel: &AtomicBool) {
        if let Err(error) = self.relocation_pass(false, cancel) {
            // Relaxed: the flag only selects the outcome; no data is handed off.
            if !cancel.load(Ordering::Relaxed) {
                self.poison(&error);
            }
        }
    }

    /// Run one relocation pass. `observe` records every callback with its key
    /// and value (test observation only; production keeps none). `cancel` is
    /// checked between merged batches and owner polls, before adoption.
    pub(crate) fn relocation_pass(
        &self,
        observe: bool,
        cancel: &AtomicBool,
    ) -> Result<RelocationReport, LocalDbError> {
        self.ensure_healthy()?;
        let core = self.write_core();
        let (cut, streams) = self.freeze_relocation_scan()?;
        let file = core.config.storage.wal_file_bytes;
        let empty = |streams: Vec<StreamReport>| RelocationReport {
            gate: RelocationGate::NoWork,
            streams,
            callbacks: Vec::new(),
        };
        let Some(q) = cut else {
            return Ok(empty(Vec::new()));
        };
        let q_cut = IndexReadCut::At(q);
        // Holds the retention floor at or below Q for the scan's member reads.
        let pin = self.pin_cut(q_cut.clone())?;
        let mut cursors: Vec<Cursor> = streams
            .into_iter()
            .map(|scan| Cursor {
                next: Some(scan.from),
                scan,
                buffer: VecDeque::new(),
                start: None,
                upper: None,
            })
            .collect();
        let mut clients = self.relocation_clients()?;
        let mut callbacks = Vec::new();
        // At most one candidate per key: only the exact head at Q passes, and
        // a key has one head. The plan is therefore bounded by the keys of
        // filtered keyspaces that the resident index already holds at Q, not
        // by the scanned history.
        let mut candidates = Vec::new();
        // Removals of the current batch only (at most one batch's members):
        // a Stop later in the same batch discards all of them.
        let mut batch_removals: Vec<Removal> = Vec::new();
        let mut stop: Option<(usize, BatchVersion, u64)> = None;
        'merge: loop {
            // Relaxed: a cancellation hint; shutdown joins the worker.
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            for cursor in &mut cursors {
                self.fill_cursor(&mut clients[cursor.scan.node], cursor, q, cancel)?;
            }
            let Some(index) = (0..cursors.len())
                .filter(|&index| !cursors[index].buffer.is_empty())
                .min_by_key(|&index| cursors[index].buffer[0].version)
            else {
                break;
            };
            let batch = cursors[index]
                .buffer
                .pop_front()
                .ok_or_else(|| LocalDbError::new("relocation merge head vanished"))?;
            batch_removals.clear();
            for put in &batch.puts {
                let keyspace = KeySpace(put.reference.descriptor.keyspace);
                let descriptor = self.shape.ks(keyspace);
                if descriptor.destroyed() {
                    continue;
                }
                let Some(filter) = descriptor.relocation_filter() else {
                    continue;
                };
                let Mutation::Put { key, value, .. } =
                    self.relocation_member(keyspace, &put.reduced_key, put.reference, &pin)?
                else {
                    return Err(LocalDbError::new("relocation member is not a put"));
                };
                let decision = match filter(&key, &value) {
                    crate::Decision::Keep => CallbackDecision::Keep,
                    crate::Decision::Remove => CallbackDecision::Remove,
                    crate::Decision::StopRelocation => CallbackDecision::Stop,
                };
                if observe {
                    callbacks.push(CallbackRecord {
                        keyspace,
                        key: key.clone(),
                        value,
                        wal_position: batch.address.offset,
                        decision,
                    });
                }
                match decision {
                    // The whole Stop batch stays: its removals are dropped.
                    CallbackDecision::Stop => {
                        stop = Some((index, batch.version, batch.address.offset));
                        break 'merge;
                    }
                    CallbackDecision::Remove => batch_removals.push(Removal {
                        keyspace,
                        key,
                        reference: put.reference,
                    }),
                    CallbackDecision::Keep => {}
                }
            }
            for removal in batch_removals.drain(..) {
                let Some(head) = self
                    .index
                    .relocation_head(removal.keyspace, &removal.key, &q_cut)
                    .map_err(LocalDbError::component)?
                else {
                    continue;
                };
                if head.head == Some(removal.reference) {
                    candidates.push(PruneCandidate {
                        keyspace: removal.keyspace,
                        cell: head.cell,
                        birth: head.birth,
                        key: head.reduced_key,
                        expected: removal.reference,
                    });
                }
            }
        }
        let reports = stream_reports(&cursors, stop, file, core.config.relocation_max_reclaim_pct)?;
        let gate = if reports
            .iter()
            .any(|stream| stream.rounded_target >= stream.lo.saturating_add(file))
        {
            RelocationGate::DidWork
        } else {
            RelocationGate::NoWork
        };
        if gate == RelocationGate::NoWork {
            // Native: the filters were called, nothing else happens.
            return Ok(RelocationReport {
                callbacks,
                ..empty(reports)
            });
        }
        // Only whole files below a target move a callback start.
        let callback_starts = reports
            .iter()
            .filter(|stream| stream.rounded_target >= stream.lo.saturating_add(file))
            .map(|stream| {
                (
                    (stream.partition, stream.fencing_generation),
                    stream.rounded_target,
                )
            })
            .collect();
        let stop_version = stop.map(|(_, version, _)| version);
        candidates.sort_by(|left, right| {
            (left.keyspace.0, &left.cell, &left.key).cmp(&(
                right.keyspace.0,
                &right.cell,
                &right.key,
            ))
        });
        let floor = match stop_version {
            Some(stop) => before_batch(q, stop),
            None => q_cut,
        };
        // Release the scan pin before asking to advance the floor.
        drop(pin);
        self.adopt_relocation(PrunePlan { candidates }, callback_starts, floor.clone())?;
        // Bounded physical retirement under the adopted starts and floor.
        core.retire_history_before(floor)?;
        Ok(RelocationReport {
            gate,
            streams: reports,
            callbacks,
        })
    }

    /// `Q` and the retained streams' scan intervals, frozen together under
    /// `index writer -> publisher` (the checkpoint capture lock order).
    fn freeze_relocation_scan(
        &self,
    ) -> Result<(Option<MutationVersion>, Vec<StreamScan>), LocalDbError> {
        let core = self.write_core();
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        let published_next = publisher.published_next;
        let state = publisher.checkpoint_state();
        let q = *state.latest;
        if q.is_some_and(|q| q.batch.epoch == core.epoch && q.batch.sequence >= published_next) {
            return Err(LocalDbError::new(
                "relocation stable cut exceeds published floor",
            ));
        }
        let mut ends: BTreeMap<(u32, u64), u64> = BTreeMap::new();
        for receipt in state.known.values() {
            if q.is_none_or(|q| receipt.version() > q.batch) {
                continue;
            }
            let address = receipt.address();
            let end = address
                .offset
                .checked_add(u64::from(address.len))
                .ok_or_else(|| LocalDbError::new("relocation receipt bound overflow"))?;
            let entry = ends
                .entry((address.partition, receipt.version().epoch))
                .or_insert(0);
            *entry = (*entry).max(end);
        }
        let mut streams = Vec::new();
        for (node, generations) in state.retained_epochs.iter().enumerate() {
            let partition = core.config.endpoints[node].partition;
            for &generation in generations {
                let key = (partition, generation);
                let required_end = ends.get(&key).copied().unwrap_or(0).max(
                    state
                        .positions
                        .get(&key)
                        .map_or(0, |position| position.durable_end),
                );
                streams.push(StreamScan {
                    node,
                    partition,
                    generation,
                    from: state.callback_starts.get(&key).copied().unwrap_or(0),
                    required_end,
                });
            }
        }
        streams.sort_by_key(|stream| (stream.partition, stream.generation));
        Ok((q, streams))
    }

    /// One dedicated authenticated session per owner: relocation pages never
    /// use the owner worker's control lane.
    fn relocation_clients(&self) -> Result<Vec<Client>, LocalDbError> {
        let config = &self.write_core().config;
        let tls_config = config
            .tls
            .client_config_with_identity()
            .map_err(LocalDbError::component)?;
        config
            .endpoints
            .iter()
            .map(|endpoint| {
                let mut tls = config.tls.clone();
                tls.server_name = endpoint.server_name.clone();
                Client::connect_shared(
                    &endpoint.address,
                    &tls,
                    tls_config.clone(),
                    config.timeout,
                    config.partition_config(1, endpoint.partition),
                    &self.shape,
                )
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "relocation session to partition {} failed: {error}",
                        endpoint.partition
                    ))
                })
            })
            .collect()
    }

    /// Give `cursor` a head batch or mark it ended. Batches above `Q` end
    /// the stream at their offset. Waits, bounded by the configured timeout,
    /// while the owner has not adopted the required end.
    fn fill_cursor(
        &self,
        client: &mut Client,
        cursor: &mut Cursor,
        q: MutationVersion,
        cancel: &AtomicBool,
    ) -> Result<(), LocalDbError> {
        let deadline = Instant::now() + self.config.timeout;
        while cursor.buffer.is_empty() && cursor.upper.is_none() {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            let Some(from) = cursor.next else {
                cursor.upper = Some(cursor.scan.required_end);
                break;
            };
            let page = client
                .relocation_page(
                    self.epoch,
                    cursor.scan.generation,
                    from,
                    cursor.scan.required_end,
                    PAGE_BATCHES,
                )
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "relocation page of partition {} generation {} failed: {error}",
                        cursor.scan.partition, cursor.scan.generation
                    ))
                })?;
            cursor.start.get_or_insert(page.start);
            if page.available_end < cursor.scan.required_end {
                if Instant::now() >= deadline {
                    return Err(LocalDbError::new(
                        "relocation scan: owner adoption stays below the published cut",
                    ));
                }
                std::thread::sleep(BEHIND_POLL);
                continue;
            }
            for batch in page.batches {
                if batch.version > q.batch {
                    cursor.upper = Some(batch.address.offset);
                    break;
                }
                cursor.buffer.push_back(batch);
            }
            cursor.next = page.next;
        }
        Ok(())
    }

    /// The put of `reference` through the ordinary pinned member read; the
    /// owner revalidates membership, the coordinator the key reduction.
    fn relocation_member(
        &self,
        keyspace: KeySpace,
        reduced_key: &[u8],
        reference: ValueRecordReference,
        pin: &IndexReadPin,
    ) -> Result<Mutation, LocalDbError> {
        let deadline = Instant::now() + self.config.timeout;
        let pending = loop {
            let generation = self.read_admission_generation();
            match self.submit_record_at(reference, pin.clone()) {
                Ok(pending) => break pending,
                Err(error) if RemoteDatabase::read_backpressured(&error) => {
                    self.wait_read_admission(generation, Some(deadline))?;
                }
                Err(error) => return Err(error),
            }
        };
        self.finish_record(pending.wait(), keyspace, reduced_key)
    }

    /// Hand the plan, starts and floor to the next Exact capture and drive
    /// checkpoint progress until that capture replies.
    fn adopt_relocation(
        &self,
        plan: PrunePlan,
        callback_starts: BTreeMap<(u32, u64), u64>,
        floor: IndexReadCut,
    ) -> Result<(), LocalDbError> {
        let core = self.write_core();
        let (reply, adopted) = mpsc::sync_channel(1);
        core_start_relocation_checkpoint(
            core,
            floor,
            RelocationCapture {
                plan,
                callback_starts,
                reply,
            },
        )?;
        let deadline = Instant::now() + core.config.timeout;
        loop {
            match adopted.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Disconnected) => {
                    core.ensure_healthy()?;
                    return Err(LocalDbError::new(
                        "relocation capture was dropped before adoption",
                    ));
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if Instant::now() >= deadline {
                return Err(LocalDbError::new("relocation adoption deadline expired"));
            }
            // Launches the successor that carries the plan, and waits for it.
            core_progress_checkpoint(core, true)?;
        }
    }
}

/// Per-stream lo, terminal and native whole-file target. `stop` names the
/// cursor, version and offset of the first Stop batch.
fn stream_reports(
    cursors: &[Cursor],
    stop: Option<(usize, BatchVersion, u64)>,
    file: u64,
    pct: u8,
) -> Result<Vec<StreamReport>, LocalDbError> {
    let mut reports: Vec<StreamReport> = cursors
        .iter()
        .enumerate()
        .map(|(index, cursor)| {
            let start = cursor
                .start
                .unwrap_or(cursor.scan.from)
                .max(cursor.scan.from);
            let lo = start - start % file;
            let processed_upper = cursor.upper.unwrap_or(cursor.scan.required_end);
            // A stream that reached its processed upper without a Stop ends
            // there, as fixed native's clean tail does: native takes the
            // processed frontier as terminal when its scan stops exactly
            // there on end-of-WAL or a zero header. Leased scans only
            // complete adopted groups, so no torn frame lies below the upper.
            let ended = || processed_upper;
            let terminal_position = match stop {
                Some((stream, _, offset)) if stream == index => offset,
                // Every unconsumed head is after the Stop in logical order.
                Some(_) => cursor
                    .buffer
                    .front()
                    .map_or_else(ended, |head| head.address.offset),
                None => ended(),
            };
            StreamReport {
                partition: cursor.scan.partition,
                fencing_generation: cursor.scan.generation,
                lo,
                processed_upper,
                terminal_position,
                rounded_target: 0,
            }
        })
        .collect();
    // Per owner: budget over its streams, oldest generation first.
    let mut partitions: Vec<u32> = reports.iter().map(|stream| stream.partition).collect();
    partitions.dedup();
    for partition in partitions {
        let spans: Vec<OwnerSpan> = reports
            .iter()
            .filter(|stream| stream.partition == partition)
            .map(|stream| OwnerSpan {
                generation: stream.fencing_generation,
                lo: stream.lo,
                hi: stream.processed_upper.max(stream.lo),
            })
            .collect();
        let ends = span_budget_end(&spans, file, pct);
        for stream in reports
            .iter_mut()
            .filter(|stream| stream.partition == partition)
        {
            let budget_end = ends
                .get(&stream.fencing_generation)
                .copied()
                .ok_or_else(|| LocalDbError::new("relocation budget lost a stream"))?;
            let target = stream.terminal_position.min(budget_end);
            stream.rounded_target = target - target % file;
        }
    }
    Ok(reports)
}

/// The scalar cut immediately before every member of `stop`, clamped to `q`.
fn before_batch(q: MutationVersion, stop: BatchVersion) -> IndexReadCut {
    let before = if stop.sequence != 0 {
        BatchVersion {
            sequence: stop.sequence - 1,
            ..stop
        }
    } else if stop.epoch != 0 {
        BatchVersion {
            epoch: stop.epoch - 1,
            sequence: u64::MAX,
        }
    } else {
        return IndexReadCut::BeforeAll;
    };
    IndexReadCut::At(q.min(MutationVersion {
        batch: before,
        operation_index: u32::MAX,
    }))
}
