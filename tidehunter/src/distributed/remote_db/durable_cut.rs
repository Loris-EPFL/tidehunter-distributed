//! Durable cut of Native-tier writes (durable-prefix recovery).
//!
//! A Native acknowledgement waits for the owner page cache, not for an fsync
//! (paper §3.1). One native WAL loses at most a suffix after a power loss. With
//! one WAL per owner, each owner can lose a different suffix, so plain recovery
//! can keep a later batch and lose an earlier one. The coordinator therefore
//! persists a cut: every batch of the epoch with a sequence below `next` is
//! synced on its owner or was aborted. Recovery extends the cut over the run of
//! surviving sequences that starts at it and discards every batch above that
//! run, which keeps an exact prefix of the coordinator's version order.
//!
//! Owner continuity: an owner is continuous when its incarnation at the
//! recovery fence equals the incarnation recorded for the epoch and its recovery
//! found no hole and it saw no persistence failure (`NodeStatus::recovery_clean`).
//! The incarnation is random per owner open, so every owner process restart
//! changes it: a failed fsync can drop dirty pages without a reboot, and the
//! owner then stops. A continuous owner is the same process that appended, with
//! no failed sync, so it lost nothing it appended. The continuity bound is the minimum
//! recorded synced end over the other owners (unbounded when every owner is
//! continuous). A sequence missing below the bound was never written (aborted):
//! the run skips it. A sequence missing at or above it may have been lost: the run
//! stops there.
//!
//! Live cut (exclusive) = min(published floor, synced end of every owner that
//! holds a receipted batch its last report does not cover). An idle owner does
//! not pin the cut. The minimum is O(owners) at report cadence, never on the
//! write path; the write path stores one atomic per receipt group and one per
//! publication call.
//!
//! Threads: one poller per owner (`value-wal-durable-{partition}`) on its own
//! plain connection, and one actor (`value-wal-durable-cut`). The actor owns the
//! tracker and is the only writer of the record while the database serves.
//! Explicit flush certifies its covered prefix through the actor, so a flush
//! returns only after the record covers it.

use super::super::durability::DurableCutConfig;
use super::super::storage::{
    LocalDbError, StorageError, encode_metadata, install_metadata, read_metadata,
};
use super::super::tcp::{
    Client, DURABLE_PROGRESS_MAX_WAIT, DurableProgressReport, TcpError, WakeFd,
};
use super::super::types::BatchVersion;
use crate::distributed::hybrid::loader::CutSources;
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Record file in the authority directory. Written by open before the epoch is
/// activated, then by the actor only. The name stays fixed across formats, so an
/// older record fails to load instead of reading as absent.
pub(super) const RECORD: &str = "durable-cut.v1";
/// 2: owner marks hold the owner incarnation instead of the kernel boot id.
const FORMAT: u32 = 2;
/// Lag samples kept by the tracker. When full, the newest sample absorbs new
/// floors and keeps its older time: the age can only be overestimated.
const MAX_SAMPLES: usize = 256;

/// Every batch of `epoch` with a sequence below `next` is covered.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EpochCut {
    pub epoch: u64,
    pub next: u64,
}

/// Continuity evidence of one owner in the tracked epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OwnerMark {
    pub partition: u32,
    /// The owner's incarnation at the epoch's fence. Zero once a report showed
    /// another incarnation: such an owner is never continuous.
    pub incarnation: [u8; 16],
    /// One above the highest sequence the owner reported synced.
    pub synced_next: u64,
}

/// What the recovery fence reported about one owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OwnerFence {
    pub partition: u32,
    pub incarnation: [u8; 16],
    pub recovery_clean: bool,
}

/// The persisted cut record (CRC-framed YAML, like the authority manifest).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CutRecord {
    format: u32,
    database: [u8; 16],
    /// The epoch that runs under the durable-prefix contract and its persisted
    /// cut. `None` when the current epoch is not tracked.
    live: Option<EpochCut>,
    /// One mark per owner of the `live` epoch, in endpoint order; empty when
    /// `live` is `None`. Written with the cut, so a mark can lag its owner's
    /// reports; a lagging mark only lowers the continuity bound.
    owners: Vec<OwnerMark>,
    /// One entry per tracked epoch, written by the first recovery after it:
    /// that recovery kept only the batches of `epoch` below `next`. Every later
    /// recovery honours it, so a discarded batch never returns. Epochs ascend;
    /// at most one entry per retained fencing generation: an offline open drops
    /// the entry of a trimmed generation (`forget_retired_generation`).
    truncations: Vec<EpochCut>,
}

impl CutRecord {
    /// Read and validate the record; `None` only when the file does not exist.
    /// Any other error (permission, I/O, a directory in its place) fails the open:
    /// an unreadable record must never read as "no truncations".
    pub(super) fn load(
        path: &Path,
        database: [u8; 16],
        current_epoch: u64,
    ) -> Result<Option<Self>, LocalDbError> {
        let file = path.join(RECORD);
        let record: Self = match read_metadata(&file) {
            Ok(record) => record,
            Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => {
                return Err(LocalDbError::new(format!(
                    "durable cut record {}: {error}",
                    file.display()
                )));
            }
        };
        let ascending = record
            .truncations
            .windows(2)
            .all(|pair| pair[0].epoch < pair[1].epoch);
        let last = record.truncations.last().map(|cut| cut.epoch);
        if record.format != FORMAT
            || record.database != database
            || !ascending
            || last.is_some_and(|epoch| epoch >= current_epoch)
            || record.live.is_some_and(|live| {
                live.epoch >= current_epoch || last.is_some_and(|e| e >= live.epoch)
            })
            || (record.live.is_none() && !record.owners.is_empty())
        {
            return Err(LocalDbError::new(format!(
                "durable cut record {} differs from the authority",
                file.display()
            )));
        }
        Ok(Some(record))
    }

    fn persist(&self, path: &Path) -> Result<(), LocalDbError> {
        let bytes = encode_metadata(self).map_err(LocalDbError::component)?;
        install_metadata(path, RECORD, &bytes).map_err(|error| {
            LocalDbError::new(format!(
                "durable cut record {}: {error}",
                path.join(RECORD).display()
            ))
        })?;
        // Coordinator device attribution: not Exact output.
        super::coordinator_io::record_installed(
            path,
            RECORD,
            bytes.len(),
            super::coordinator_io::CoordinatorIoCategory::DurableCut,
        );
        Ok(())
    }

    /// The tracked generation, if any.
    pub(super) fn live_generation(&self) -> Option<u64> {
        self.live.map(|live| live.epoch)
    }

    /// Drop the truncation of a trimmed fencing generation. Offline open only:
    /// after every owner acknowledged the trim and before a durable-cut actor
    /// starts. Never touches `live`. Returns whether the record changed.
    pub(super) fn forget_retired_generation(
        &mut self,
        generation: u64,
    ) -> Result<bool, LocalDbError> {
        if self.live.is_some_and(|live| live.epoch == generation) {
            return Err(LocalDbError::new(format!(
                "durable cut still tracks fencing generation {generation}"
            )));
        }
        let before = self.truncations.len();
        self.truncations.retain(|cut| cut.epoch != generation);
        Ok(self.truncations.len() != before)
    }
}

/// The bound recovery applied to the tracked epoch: it kept only the batches of
/// `epoch` with a sequence below `next`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveredCut {
    pub epoch: u64,
    pub next: u64,
}

/// The epoch whose bound this recovery decides.
struct OpenTruncation {
    epoch: u64,
    next: u64,
    /// No lost sequence seen yet.
    extending: bool,
    /// Continuity bound (exclusive): a missing sequence below it was never written.
    bound: u64,
}

/// Replay filter built at open from the record, before tail replay.
pub(super) struct RecoveryPlan {
    database: [u8; 16],
    existed: bool,
    truncations: Vec<EpochCut>,
    recorded: BTreeMap<u64, u64>,
    open: Option<OpenTruncation>,
    pub discarded: u64,
    pub extended: u64,
    /// Missing sequences the extension skipped as never written.
    pub skipped: u64,
    /// The continuity bound of an Extend truncation; `u64::MAX` when every owner
    /// was continuous.
    pub continuity_bound: Option<u64>,
}

/// Minimum recorded synced end over the owners that are not continuous;
/// `u64::MAX` when all are. No evidence for an owner (no mark) gives 0.
fn continuity_bound(marks: &[OwnerMark], fenced: &[OwnerFence]) -> u64 {
    if marks.is_empty() {
        return 0;
    }
    let unmarked = fenced
        .iter()
        .any(|owner| !marks.iter().any(|mark| mark.partition == owner.partition));
    if unmarked {
        return 0;
    }
    marks
        .iter()
        .map(|mark| {
            let continuous = fenced.iter().any(|owner| {
                owner.partition == mark.partition
                    && owner.incarnation == mark.incarnation
                    && owner.recovery_clean
            });
            if continuous {
                u64::MAX
            } else {
                mark.synced_next
            }
        })
        .min()
        .unwrap_or(0)
}

impl RecoveryPlan {
    /// `checkpoint` is the last batch of the committed checkpoint. Every batch
    /// it covers was synced before the checkpoint committed. `fenced` holds the
    /// fence status of every owner, for the continuity rule.
    pub(super) fn new(
        record: Option<CutRecord>,
        database: [u8; 16],
        checkpoint: Option<BatchVersion>,
        fenced: &[OwnerFence],
    ) -> Self {
        let existed = record.is_some();
        let (live, marks, truncations) = record.map_or((None, Vec::new(), Vec::new()), |record| {
            (record.live, record.owners, record.truncations)
        });
        let recorded: BTreeMap<_, _> = truncations
            .iter()
            .map(|cut| (cut.epoch, cut.next))
            .collect();
        let open = live
            .filter(|live| !recorded.contains_key(&live.epoch))
            .map(|live| OpenTruncation {
                epoch: live.epoch,
                next: live.next.max(
                    checkpoint
                        .filter(|version| version.epoch == live.epoch)
                        .map_or(0, |version| version.sequence.saturating_add(1)),
                ),
                extending: true,
                bound: continuity_bound(&marks, fenced),
            });
        let continuity_bound = open.as_ref().map(|open| open.bound);
        Self {
            database,
            existed,
            truncations,
            recorded,
            open,
            discarded: 0,
            extended: 0,
            skipped: 0,
            continuity_bound,
        }
    }

    /// Whether replay keeps this batch. Replay visits each epoch in ascending
    /// version order, which makes the extension a single pass.
    pub(super) fn keep(&mut self, version: BatchVersion) -> bool {
        let keep = if let Some(&next) = self.recorded.get(&version.epoch) {
            version.sequence < next
        } else {
            match &mut self.open {
                Some(open) if open.epoch == version.epoch => {
                    if version.sequence < open.next {
                        true
                    } else if open.extending
                        && (version.sequence == open.next || version.sequence <= open.bound)
                    {
                        // Every missing sequence in [next, sequence) is below
                        // the continuity bound: it was never written.
                        self.skipped += version.sequence - open.next;
                        open.next = version.sequence.saturating_add(1);
                        self.extended += 1;
                        true
                    } else {
                        open.extending = false;
                        false
                    }
                }
                _ => true,
            }
        };
        self.discarded += u64::from(!keep);
        keep
    }

    /// The record to persist before `epoch` activates, and the bound this
    /// recovery decided. No record is written when none existed and `epoch` is
    /// not tracked (a Synchronous owner). A tracked
    /// epoch starts with one mark per fenced owner (its incarnation, nothing synced).
    pub(super) fn finish(
        mut self,
        epoch: u64,
        tracked: bool,
        fenced: &[OwnerFence],
    ) -> (Option<CutRecord>, Option<RecoveredCut>) {
        let decided = self.open.take().map(|open| RecoveredCut {
            epoch: open.epoch,
            next: open.next,
        });
        if let Some(cut) = decided {
            self.truncations.push(EpochCut {
                epoch: cut.epoch,
                next: cut.next,
            });
        }
        let record = (self.existed || tracked).then(|| CutRecord {
            format: FORMAT,
            database: self.database,
            live: tracked.then_some(EpochCut { epoch, next: 0 }),
            owners: if tracked {
                fenced
                    .iter()
                    .map(|owner| OwnerMark {
                        partition: owner.partition,
                        incarnation: owner.incarnation,
                        synced_next: 0,
                    })
                    .collect()
            } else {
                Vec::new()
            },
            truncations: self.truncations,
        });
        (record, decided)
    }
}

/// Persist the open-time record. A crash after this and before activation is
/// safe: the next open finds the same truncation and an empty tracked epoch.
pub(super) fn persist_open_record(path: &Path, record: &CutRecord) -> Result<(), LocalDbError> {
    record.persist(path)
}

/// Test hook: edit the persisted owner marks (for example, to model an owner restart).
#[cfg(test)]
pub(super) fn rewrite_marks_for_test(path: &Path, edit: impl FnOnce(&mut [OwnerMark])) {
    let mut record: CutRecord = read_metadata(&path.join(RECORD)).unwrap();
    edit(&mut record.owners);
    record.persist(path).unwrap();
}

/// Test hook: persisted truncation entries (zero when no record exists).
#[cfg(test)]
pub(super) fn truncation_count_for_test(path: &Path) -> usize {
    read_metadata::<CutRecord>(&path.join(RECORD)).map_or(0, |record| record.truncations.len())
}

#[derive(Clone, Copy, Default)]
struct Progress {
    synced_next: u64,
    appended_end: u64,
    /// When the tracker last asked this owner to sync. A nudge on a live stream
    /// installs a strict root; while a lagging peer pins the cut, the lag triggers
    /// stay true, so without this bound an appending owner would install one root
    /// per poll round trip.
    last_nudge: Option<Instant>,
}

impl Progress {
    /// The first nudge of the epoch is immediate; later ones wait `lag` after the
    /// previous nudge. A report does not re-arm it: the reply to a nudge advances
    /// the report itself.
    fn may_nudge(&self, now: Instant, lag: Duration) -> bool {
        self.last_nudge
            .is_none_or(|at| now.saturating_duration_since(at) >= lag)
    }
}

#[derive(Clone, Copy)]
struct Sample {
    at: Instant,
    published_next: u64,
    appended: u64,
}

/// Pure cut and lag state of one epoch. Owned by the actor.
pub(super) struct Tracker {
    lag: Duration,
    lag_bytes: u64,
    owners: Vec<Progress>,
    /// Highest cut known valid: computed or certified. Never decreases.
    target: u64,
    samples: VecDeque<Sample>,
    /// Summed appended ends when the cut last covered the published floor.
    covered: u64,
}

impl Tracker {
    pub(super) fn new(owners: usize, config: &DurableCutConfig) -> Self {
        Self {
            lag: Duration::from_millis(config.lag_ms),
            lag_bytes: config.lag_bytes,
            owners: vec![Progress::default(); owners],
            target: 0,
            samples: VecDeque::with_capacity(MAX_SAMPLES),
            covered: 0,
        }
    }

    pub(super) fn report(&mut self, owner: usize, synced_next: u64, appended_end: u64) {
        if let Some(progress) = self.owners.get_mut(owner) {
            progress.synced_next = progress.synced_next.max(synced_next);
            progress.appended_end = progress.appended_end.max(appended_end);
        }
    }

    /// Explicit flush: every batch below `next` is synced or aborted.
    pub(super) fn certify(&mut self, next: u64) {
        self.target = self.target.max(next);
    }

    /// Recompute the cut from the published floor and the receipted ends.
    /// `receipted[o]` is one above the highest sequence receipted by owner `o`;
    /// it must be read after `published_next` (see [`Shared::published`]).
    /// Pushes every lagging owner to `nudge` once the cut lag reaches a bound, at
    /// most once per `lag` per owner.
    pub(super) fn evaluate(
        &mut self,
        now: Instant,
        published_next: u64,
        receipted: &[u64],
        nudge: &mut Vec<usize>,
    ) -> u64 {
        let pinned = |progress: &Progress, receipted: u64| receipted > progress.synced_next;
        let mut cut = published_next;
        for (progress, &receipted) in self.owners.iter().zip(receipted) {
            if pinned(progress, receipted) {
                cut = cut.min(progress.synced_next);
            }
        }
        self.target = self.target.max(cut);
        let appended = self.owners.iter().fold(0u64, |sum, progress| {
            sum.saturating_add(progress.appended_end)
        });
        while let Some(front) = self.samples.front().copied() {
            if front.published_next > self.target {
                break;
            }
            self.covered = front.appended;
            self.samples.pop_front();
        }
        if published_next <= self.target {
            self.covered = appended;
        } else if self
            .samples
            .back()
            .is_none_or(|back| back.published_next < published_next)
        {
            if self.samples.len() == MAX_SAMPLES {
                if let Some(back) = self.samples.back_mut() {
                    back.published_next = published_next;
                    back.appended = appended;
                }
            } else {
                self.samples.push_back(Sample {
                    at: now,
                    published_next,
                    appended,
                });
            }
        }
        let age = self.samples.front().map_or(Duration::ZERO, |front| {
            now.saturating_duration_since(front.at)
        });
        if age >= self.lag || appended.saturating_sub(self.covered) >= self.lag_bytes {
            for (owner, (progress, &receipted)) in self.owners.iter_mut().zip(receipted).enumerate()
            {
                if pinned(progress, receipted)
                    && progress.synced_next < published_next
                    && progress.may_nudge(now, self.lag)
                {
                    progress.last_nudge = Some(now);
                    nudge.push(owner);
                }
            }
        }
        self.target
    }
}

/// Per-owner atomics, padded so that owner workers do not share a cache line.
#[repr(align(64))]
#[derive(Default)]
struct OwnerShared {
    /// One above the highest receipted sequence. Written by the owner worker.
    receipted: AtomicU64,
    /// One above the reported synced sequence. Written by the poller.
    synced: AtomicU64,
    /// Set by the actor, consumed by the poller's next poll.
    nudge: AtomicBool,
}

#[derive(Default)]
struct Counters {
    persisted_next: AtomicU64,
    reports: AtomicU64,
    nudges: AtomicU64,
    persists: AtomicU64,
    persist_failures: AtomicU64,
    poll_failures: AtomicU64,
    certifications: AtomicU64,
    /// Syncs requested by `wait_through` callers (relocation copies).
    targeted_syncs: AtomicU64,
}

struct Shared {
    epoch: u64,
    /// The publisher's exclusive contiguous floor. Written under the publisher lock.
    published: AtomicU64,
    /// Version B's consistent prefix (exclusive): every batch of `epoch` below
    /// it is synced on its owner or aborted. Written by the actor after it read
    /// the published floor and every owner's receipted end, so it never exceeds
    /// the floor; monotone. Read by the index loader as the durable term of its
    /// flush frontier (plan I2). Zero means "no batch covered yet".
    consistent: AtomicU64,
    owners: Box<[OwnerShared]>,
    /// Control wake of each owner poller, one bounded (capacity 1) channel per
    /// owner. Sender: [`Shared`], from the nudge paths and [`DurableCut::stop`];
    /// receiver: that owner's poller. A nudge and a stop both use it, and the
    /// poller re-reads its stop and nudge flags on every wake, so a queued wake
    /// stands for a pending one of either. Full queue: a wake is already
    /// pending, so `try_send` drops the new one and no sender blocks or
    /// allocates. Shutdown: `stopping` is set (Release) before the wakes and the
    /// poller exits on it, so a wake that races the stop is still observed as a
    /// stop; the senders live in `Shared`, the receivers in the pollers it
    /// joins.
    poller_wakes: Box<[mpsc::SyncSender<()>]>,
    /// Set by [`DurableCut::stop`] before its wakes: the only reason a wake ends
    /// a poller.
    stopping: AtomicBool,
    /// Wakes `wait_through`: notified by the actor after each cut advance.
    advanced: Mutex<()>,
    advanced_cv: Condvar,
    counters: Counters,
    /// Test hook: every persisted cut, sent with `try_send` (capacity 4096).
    #[cfg(test)]
    persisted_events: Mutex<Option<mpsc::SyncSender<u64>>>,
    /// Test hook: one count per completed owner-poller park, in endpoint order.
    #[cfg(test)]
    park_samples: Box<[AtomicU64]>,
}

impl Shared {
    fn bump(counter: &AtomicU64) {
        // Relaxed: independent diagnostic counter.
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Non-blocking wake of one owner poller. A full queue already holds a
    /// wake for it, and the poller re-reads every flag on each wake.
    fn wake_poller(&self, owner: usize) {
        if let Some(wake) = self.poller_wakes.get(owner) {
            let _ = wake.try_send(());
        }
    }

    /// Whether a stop is in progress. Acquire pairs with the Release store in
    /// `DurableCut::stop`, before its wakes.
    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    /// Park until a control wake, `wait`, or a stop. True when stopping. A
    /// wake that is not a stop is a nudge: the caller re-reads its flags.
    fn park(&self, control: &mpsc::Receiver<()>, wait: Duration) -> bool {
        let _ = control.recv_timeout(wait);
        self.stopping()
    }
}

/// Diagnostic snapshot of the durable cut. Counters are weakly consistent.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurableCutStats {
    /// False when this epoch is not tracked (a Synchronous owner).
    pub enabled: bool,
    pub epoch: u64,
    /// Every batch of `epoch` below this sequence is synced or aborted, and the
    /// record on disk says so.
    pub persisted_next: u64,
    pub reports: u64,
    pub nudges: u64,
    pub persists: u64,
    pub persist_failures: u64,
    pub poll_failures: u64,
    pub certifications: u64,
    /// Targeted syncs requested by relocation copies (`wait_through`).
    pub targeted_syncs: u64,
}

enum Message {
    Report {
        owner: usize,
        synced_next: u64,
        appended_end: u64,
        incarnation: [u8; 16],
    },
    /// Reply capacity 1; the requester waits for it.
    Certify {
        next: u64,
        reply: mpsc::SyncSender<Result<(), LocalDbError>>,
    },
    Stop,
}

struct Running {
    /// Owner: the actor (sole receiver). Capacity: owners + 8. Senders: one
    /// report per poller turn and one certification per flush caller. Full
    /// queue: senders wait (blocking `send`); none of them holds a lock. Stop:
    /// sent after every poller joined; the actor exits on it or on disconnect.
    messages: mpsc::SyncSender<Message>,
    /// One stop wake and handle per poller. The wake interrupts a parked
    /// progress wait; it is never drained. A nudge reuses the poller's control
    /// wake ([`Shared::poller_wakes`]), not this one.
    pollers: Vec<(Arc<WakeFd>, JoinHandle<()>)>,
    actor: JoinHandle<()>,
}

/// Coordinator side of the durable cut for one serving epoch.
pub(in crate::distributed::remote_db) struct DurableCut {
    shared: Arc<Shared>,
    // Cold: taken by certify (per flush) and stop only.
    running: Mutex<Option<Running>>,
}

/// One owner endpoint as the poller needs it. `connect` opens the dedicated
/// progress lane (`Client::connect_progress_shared`) with the poller's stop wake.
pub(super) struct PollTarget {
    pub partition: u32,
    pub connect: Box<dyn Fn(Arc<WakeFd>) -> Result<Client, TcpError> + Send>,
}

impl DurableCut {
    /// Start the pollers and the actor of `epoch`. `record` is the record that
    /// open persisted; the actor rewrites only its `live` cut.
    pub(super) fn start(
        path: PathBuf,
        epoch: u64,
        config: DurableCutConfig,
        record: CutRecord,
        targets: Vec<PollTarget>,
        cuts: CutSources,
    ) -> Result<Self, LocalDbError> {
        // Open every first session now, before open returns: the owner's Status
        // handshake flushes its logs, and here they hold nothing of this epoch,
        // so no handshake syncs a write ahead of the cut's nudge.
        let mut targets = targets
            .into_iter()
            .map(|target| match WakeFd::for_progress("poller stop wake") {
                Ok(stop) => Ok((target, Arc::new(stop))),
                Err(error) => Err(LocalDbError::new(format!(
                    "durable cut session to partition {}: {error}",
                    target.partition
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let clients = super::endpoint_map(&mut targets, |_, (target, stop)| {
            (target.connect)(stop.clone()).map_err(|error| {
                LocalDbError::new(format!(
                    "durable cut session to partition {}: {error}",
                    target.partition
                ))
            })
        })?;
        let owners = targets.len();
        let (poller_wakes, poller_controls): (Vec<_>, Vec<_>) =
            (0..owners).map(|_| mpsc::sync_channel(1)).unzip();
        let shared = Arc::new(Shared {
            epoch,
            published: AtomicU64::new(0),
            consistent: AtomicU64::new(0),
            owners: (0..owners).map(|_| OwnerShared::default()).collect(),
            poller_wakes: poller_wakes.into_boxed_slice(),
            stopping: AtomicBool::new(false),
            advanced: Mutex::new(()),
            advanced_cv: Condvar::new(),
            counters: Counters::default(),
            #[cfg(test)]
            persisted_events: Mutex::new(None),
            #[cfg(test)]
            park_samples: (0..owners).map(|_| AtomicU64::new(0)).collect(),
        });
        let (messages, inbox) = mpsc::sync_channel(owners + 8);
        let wait = Duration::from_millis(config.lag_ms);
        let actor_shared = shared.clone();
        let actor = thread::Builder::new()
            .name("value-wal-durable-cut".to_owned())
            .spawn(move || {
                run_actor(
                    &inbox,
                    &actor_shared,
                    Tracker::new(owners, &config),
                    record,
                    &path,
                    (wait / 4).max(Duration::from_millis(1)),
                    &cuts,
                );
            })
            .map_err(LocalDbError::component)?;
        let cut = Self {
            shared: shared.clone(),
            running: Mutex::new(Some(Running {
                messages: messages.clone(),
                pollers: Vec::with_capacity(owners),
                actor,
            })),
        };
        for (owner, (((target, wake), control), client)) in targets
            .into_iter()
            .zip(poller_controls)
            .zip(clients)
            .enumerate()
        {
            let shared = shared.clone();
            let messages = messages.clone();
            let poller_wake = wake.clone();
            let handle = thread::Builder::new()
                .name(format!("value-wal-durable-{}", target.partition))
                .spawn(move || {
                    poll_owner(
                        owner,
                        &target,
                        client,
                        &shared,
                        &messages,
                        (&control, &poller_wake),
                        wait,
                    );
                })
                .map_err(LocalDbError::component)?;
            if let Some(running) = cut.running.lock().as_mut() {
                running.pollers.push((wake, handle));
            }
        }
        Ok(cut)
    }

    /// Owner worker, before publication: `next` is one above the highest
    /// sequence of this receipt group.
    pub(super) fn receipted(&self, owner: usize, next: u64) {
        if let Some(state) = self.shared.owners.get(owner) {
            // Release, before the worker takes the publisher lock: a cut reader
            // that observes a floor covering this receipt also observes it.
            state.receipted.fetch_max(next, Ordering::Release);
        }
    }

    /// Publisher, under the publisher lock, after the floor moved.
    pub(super) fn published(&self, next: u64) {
        // Release pairs with the actor's Acquire load. The receipt stores of
        // every batch below `next` happen before this store: each was made
        // before its worker took the publisher lock, and this store is made
        // while the lock is held after them. The actor loads this first, then
        // the receipted ends, so it never sees a floor without its receipts.
        self.shared.published.fetch_max(next, Ordering::Release);
    }

    /// Explicit flush: every batch below `next` is synced or aborted. Returns
    /// after the record covers `next`.
    pub(super) fn certify(&self, next: u64) -> Result<(), LocalDbError> {
        let messages = self
            .running
            .lock()
            .as_ref()
            .map(|running| running.messages.clone())
            .ok_or_else(|| LocalDbError::new("durable cut stopped"))?;
        let (reply, result) = mpsc::sync_channel(1);
        messages
            .send(Message::Certify { next, reply })
            .map_err(|_| LocalDbError::new("durable cut actor stopped"))?;
        result
            .recv()
            .map_err(|_| LocalDbError::new("durable cut actor stopped"))?
    }

    /// Active, bounded wait for the persisted cut to cover `next`: every batch
    /// of this epoch below it is synced on its owner (or aborted) and the record
    /// names it. Used by relocation before it re-points the index at a copy: an
    /// index entry must never name a position that is not durable-proven.
    ///
    /// The wait first raises every owner's immediate-nudge flag so each poller
    /// asks its owner to sync now instead of waiting for the cut lag to reach
    /// `lag_ms`/`lag_bytes`, then parks on the actor's advance notification.
    /// While the cut has not covered `next` the flag is raised again once per
    /// `renudge` (a failed poll re-arms itself, but a poll that raced the first
    /// request could have consumed it before the copy was receipted).
    ///
    /// The persisted cut is stored, and this wait woken, before the actor
    /// advances the table's durable cut (`CutSet::set_durable`). The table's
    /// durable cut can therefore lag the return of this call; the re-point
    /// safety rests on the persisted record, which this wait observes. Tests
    /// that need the table's cut wait for the persist event, which the actor
    /// sends after the advance.
    ///
    /// # Errors
    /// `timeout` elapsed first. Nothing is rolled back; the caller skips its
    /// re-points and the next pass retries.
    pub(super) fn wait_through(
        &self,
        next: u64,
        timeout: Duration,
        renudge: Duration,
    ) -> Result<(), LocalDbError> {
        let shared = &self.shared;
        let deadline = Instant::now() + timeout;
        let mut guard = shared.advanced.lock();
        let mut nudge_at = Instant::now();
        loop {
            // Acquire pairs with the actor's Release store of the persisted cut.
            if shared.consistent.load(Ordering::Acquire) >= next {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(LocalDbError::new(format!(
                    "durable cut did not cover sequence {next} within {timeout:?}"
                )));
            }
            if now >= nudge_at {
                for (owner, state) in shared.owners.iter().enumerate() {
                    // AcqRel pairs with the poller's swap. Counted only when the
                    // flag was clear, as the actor counts its own nudges.
                    if !state.nudge.swap(true, Ordering::AcqRel) {
                        Shared::bump(&shared.counters.nudges);
                    }
                    // Wake an idle poller: the flag alone is only read when it
                    // next runs, and an idle owner parks for a quarter lag.
                    shared.wake_poller(owner);
                }
                Shared::bump(&shared.counters.targeted_syncs);
                nudge_at = now + renudge;
            }
            let wake = deadline.min(nudge_at);
            let _ = shared.advanced_cv.wait_until(&mut guard, wake);
        }
    }

    /// Test hook: receive every cut the actor persists from now on.
    #[cfg(test)]
    pub(super) fn persisted_events(&self) -> mpsc::Receiver<u64> {
        let (sender, receiver) = mpsc::sync_channel(4096);
        *self.shared.persisted_events.lock() = Some(sender);
        receiver
    }

    /// Test hook: how many times the poller of `owner` has entered its idle
    /// park. Relaxed: the caller polls it until it moves, and the park is what
    /// it waits for, not the value.
    #[cfg(test)]
    pub(super) fn park_samples(&self, owner: usize) -> u64 {
        self.shared
            .park_samples
            .get(owner)
            .map_or(0, |samples| samples.load(Ordering::Relaxed))
    }

    pub(super) fn stats(&self) -> DurableCutStats {
        let counters = &self.shared.counters;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        DurableCutStats {
            enabled: true,
            epoch: self.shared.epoch,
            persisted_next: load(&counters.persisted_next),
            reports: load(&counters.reports),
            nudges: load(&counters.nudges),
            persists: load(&counters.persists),
            persist_failures: load(&counters.persist_failures),
            poll_failures: load(&counters.poll_failures),
            certifications: load(&counters.certifications),
            targeted_syncs: load(&counters.targeted_syncs),
        }
    }

    /// Stop and join every thread. Idempotent. The stop flag is set before the
    /// wakes, so a poller woken by a nudge sees the stop; the stop wake ends a
    /// parked progress wait at once, and the control wake ends an idle park.
    pub(super) fn stop(&self) {
        let Some(running) = self.running.lock().take() else {
            return;
        };
        self.shared.stopping.store(true, Ordering::Release);
        for owner in 0..running.pollers.len() {
            self.shared.wake_poller(owner);
        }
        for (wake, poller) in running.pollers {
            // Cannot fail (a full eventfd counter is already readable); a lost
            // wake would only delay this stop to the owner's poll deadline.
            let _ = wake.signal();
            if poller.join().is_err() {
                Shared::bump(&self.shared.counters.poll_failures);
            }
        }
        // Err: the actor already exited (panic); joined below.
        let _ = running.messages.send(Message::Stop);
        drop(running.messages);
        if running.actor.join().is_err() {
            Shared::bump(&self.shared.counters.persist_failures);
        }
    }
}

impl Drop for DurableCut {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Service loop: runs until stop. Each turn either parks on the control wake
/// (idle owner, failed connection) or completes one bounded long poll. A nudge
/// wakes the park at once through the same control wake as the stop, so an idle
/// owner never delays a `wait_through` caller by its lag. The stop wake (second
/// part of `stop`) ends a parked poll at once; that exit takes no failed-poll
/// path and certifies nothing. A route that cannot wait without spinning
/// (`TcpError::is_wait_unavailable`) ends this poller: reconnecting would repeat
/// the owner's Status sync without fixing the route, and the cut of this owner
/// then stays at its last report, so certification reports it.
fn poll_owner(
    owner: usize,
    target: &PollTarget,
    client: Client,
    shared: &Shared,
    messages: &mpsc::SyncSender<Message>,
    (control, wake): (&mpsc::Receiver<()>, &Arc<WakeFd>),
    wait: Duration,
) {
    // A quarter of the lag bound: an owner with nothing outstanding re-checks
    // its flags this often. A nudge wakes it at once through `control`
    // (see [`Shared::poller_wakes`]), so the park is never a nudge latency.
    let idle_wait = (wait / 4).max(Duration::from_millis(1));
    let max_wait = idle_wait.min(DURABLE_PROGRESS_MAX_WAIT);
    let epoch = shared.epoch;
    let Some(state) = shared.owners.get(owner) else {
        return;
    };
    let unavailable = |error: &TcpError| {
        Shared::bump(&shared.counters.poll_failures);
        // Observer output only; the stopped poller is the authority signal.
        eprintln!(
            "durable cut poller for partition {} stopped: {error}",
            target.partition
        );
    };
    let mut client = Some(client);
    let mut generation = 0;
    loop {
        if shared.stopping() {
            return;
        }
        // Acquire pairs with the Release stores of the owner worker, of this
        // thread (synced) and of the actor (nudge).
        let idle = state.receipted.load(Ordering::Acquire) <= state.synced.load(Ordering::Acquire);
        if idle && !state.nudge.load(Ordering::Acquire) {
            #[cfg(test)]
            Shared::bump(&shared.park_samples[owner]);
            if shared.park(control, idle_wait) {
                return;
            }
            continue;
        }
        if shared.stopping() {
            return;
        }
        if client.is_none() {
            match (target.connect)(wake.clone()) {
                Ok(connection) => client = Some(connection),
                Err(error) if error.is_wait_unavailable() => {
                    unavailable(&error);
                    return;
                }
                Err(_) => {
                    Shared::bump(&shared.counters.poll_failures);
                    if shared.park(control, wait) {
                        return;
                    }
                    continue;
                }
            }
        }
        let Some(connection) = client.as_mut() else {
            continue;
        };
        // AcqRel: consume the actor's request exactly once.
        let sync = state.nudge.swap(false, Ordering::AcqRel);
        let report = match connection.durable_progress(epoch, epoch, generation, max_wait, sync) {
            Ok(report) => Some(report).filter(|report| valid_report(report, epoch)),
            // The stop wake ended the wait; not a failed poll.
            Err(_) if shared.stopping() => return,
            Err(error) if error.is_wait_unavailable() => {
                unavailable(&error);
                return;
            }
            Err(_) => None,
        };
        let Some(report) = report else {
            client = None;
            generation = 0;
            if sync {
                state.nudge.store(true, Ordering::Release);
            }
            Shared::bump(&shared.counters.poll_failures);
            if shared.park(control, wait) {
                return;
            }
            continue;
        };
        generation = report.generation;
        let synced_next = report
            .synced
            .map_or(0, |version| version.sequence.saturating_add(1));
        // Release: the next idle check of this thread and the actor's reads.
        state.synced.fetch_max(synced_next, Ordering::Release);
        Shared::bump(&shared.counters.reports);
        if messages
            .send(Message::Report {
                owner,
                synced_next,
                appended_end: report.appended_end,
                incarnation: report.incarnation,
            })
            .is_err()
        {
            return;
        }
    }
}

fn valid_report(report: &DurableProgressReport, epoch: u64) -> bool {
    report.epoch == epoch
        && report.authority_epoch == epoch
        && report.synced.is_none_or(|version| version.epoch == epoch)
}

/// Service loop: runs until `Stop` or until every sender is gone.
fn run_actor(
    inbox: &mpsc::Receiver<Message>,
    shared: &Shared,
    mut tracker: Tracker,
    mut record: CutRecord,
    path: &Path,
    tick: Duration,
    cuts: &CutSources,
) {
    let drain = shared.owners.len() + 8;
    let mut persisted = 0u64;
    let mut replies = Vec::new();
    let mut nudge = Vec::with_capacity(shared.owners.len());
    let mut receipted = vec![0u64; shared.owners.len()];
    let mut stopping = false;
    loop {
        let mut next = match inbox.recv_timeout(tick) {
            Ok(message) => Some(message),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        };
        // Batch at most `drain` messages per evaluation. The last iteration
        // fetches nothing, so no received message is ever dropped.
        for taken in 1..=drain {
            match next.take() {
                Some(Message::Report {
                    owner,
                    synced_next,
                    appended_end,
                    incarnation,
                }) => {
                    tracker.report(owner, synced_next, appended_end);
                    // Persisted with the next cut. Pollers run in endpoint
                    // order, which is the order of the marks.
                    if let Some(mark) = record.owners.get_mut(owner) {
                        if mark.incarnation != incarnation {
                            // Another owner incarnation inside the epoch: the owner
                            // may have lost unsynced appends, so it is never continuous.
                            mark.incarnation = [0; 16];
                        }
                        mark.synced_next = mark.synced_next.max(synced_next);
                    }
                }
                Some(Message::Certify { next, reply }) => {
                    Shared::bump(&shared.counters.certifications);
                    tracker.certify(next);
                    replies.push((next, reply));
                }
                Some(Message::Stop) => stopping = true,
                None => {}
            }
            if stopping || taken == drain {
                break;
            }
            match inbox.try_recv() {
                Ok(message) => next = Some(message),
                Err(_) => break,
            }
        }
        // Floor first, then receipts: see `DurableCut::published`.
        let published = shared.published.load(Ordering::Acquire);
        for (slot, state) in receipted.iter_mut().zip(shared.owners.iter()) {
            *slot = state.receipted.load(Ordering::Acquire);
        }
        nudge.clear();
        let target = tracker.evaluate(Instant::now(), published, &receipted, &mut nudge);
        for &owner in &nudge {
            // Count before the flag is visible: the poller may act on it at
            // once. Only this thread sets a cleared flag; the poller clears
            // it, or sets it again after a failed poll (not a new nudge).
            if let Some(state) = shared.owners.get(owner)
                && !state.nudge.load(Ordering::Acquire)
            {
                Shared::bump(&shared.counters.nudges);
                // Release pairs with the poller's swap.
                state.nudge.store(true, Ordering::Release);
            }
            // Wake an idle poller at once; a full wake queue already holds one.
            shared.wake_poller(owner);
        }
        let mut failure = None;
        if target > persisted {
            record.live = Some(EpochCut {
                epoch: shared.epoch,
                next: target,
            });
            match record.persist(path) {
                Ok(()) => {
                    persisted = target;
                    shared
                        .counters
                        .persisted_next
                        .store(persisted, Ordering::Relaxed);
                    // Version B's frontier for the index loader, published only
                    // once the record names it: a crash-cut that truncates the
                    // record must also lower this cut, or the index would flush
                    // at a `replay_from` recovery no longer covers. Every batch
                    // below it is owner-synced; a batch replayed from a record
                    // that lags by a later persist is re-applied idempotently by
                    // version. Monotone, and Release pairs with `cut`'s Acquire.
                    shared.consistent.store(persisted, Ordering::Release);
                    // Wake `wait_through` callers: the store above happens before
                    // the lock is taken, so a waiter that checked the cut under
                    // the lock either saw it or is parked when this notify runs.
                    drop(shared.advanced.lock());
                    shared.advanced_cv.notify_all();
                    // The index loader's durable front (A25), fed only from the
                    // persisted prefix and never from the tracker's live target:
                    // a native-tier persist still in flight must not let a
                    // snapshot name a batch the record does not carry. Cold path,
                    // one monotone advance per persist.
                    cuts.set_durable(BatchVersion {
                        epoch: shared.epoch,
                        sequence: persisted,
                    });
                    Shared::bump(&shared.counters.persists);
                    #[cfg(test)]
                    if let Some(events) = shared.persisted_events.lock().as_ref() {
                        let _ = events.try_send(persisted);
                    }
                }
                Err(error) => {
                    // The record lags; recovery stays conservative. Retried at
                    // the next evaluation.
                    Shared::bump(&shared.counters.persist_failures);
                    failure = Some(error);
                }
            }
        }
        for (next, reply) in replies.drain(..) {
            let result = if next <= persisted {
                Ok(())
            } else {
                Err(LocalDbError::new(format!(
                    "durable cut not persisted through sequence {next}: {}",
                    failure
                        .as_ref()
                        .map_or("unknown failure", |error| error.message())
                )))
            };
            let _ = reply.try_send(result);
        }
        if stopping {
            return;
        }
    }
}

#[cfg(test)]
mod tests;
