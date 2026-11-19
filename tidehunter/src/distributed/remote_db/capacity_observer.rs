//! Opt-in R0 capacity observer (CAPACITY-R0-R1.md §2): shadow measurement only.
//!
//! One observer thread per database owns all accounting state (ledger, roll
//! log, counters, sink). The write path only touches it through try-send hooks:
//! the prune path posts an event after Exact adoption and live install, and the
//! live writer stores two atomics per file crossing. In shadow mode the prune
//! path skips physical retirement; logical pruning, checkpoints and the floor
//! are unchanged. The thread holds a `Weak` core reference, never the database.
//!
//! A sample walks the immutable installed Exact image between installs (a
//! seqlock around image installation), then pages owner inventories over one
//! dedicated control session per owner, never the owner worker's lane (a
//! worker control settles the open leased extent). No index writer, publisher
//! or shard lock is held across a walk step, an RPC or a sink write.

use super::write_core::core::WriteCore;
use super::{Client, IndexReadCut, MutationVersion};
use crate::distributed::capacity::{
    CapacityObserverConfig, DurableCutWitness, ExactKey, FileId, FileInputs, FileInventoryReply,
    FileRetained, FileUseState, GrowthLedger, LedgerFile, LogicalCut, MAX_FILE_INVENTORY_PAGE,
    OBSERVER_QUEUE, ObserverEvent, OwnerRpcTiming, OwnerSpan, PositionFacts, R0Error, R0File,
    R0Geometry, R0Sample, R0Sink, R0Summary, RetainedAccumulator, RollCursor, RolledFile,
    SampleEvent, StreamFacts, WalFileObservation, evaluate, hex, span_budget_end,
};
use arc_swap::ArcSwapOption;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Owner inventory pages per stream and sample; a stream has far fewer files.
const MAX_PAGES_PER_STREAM: usize = 1 << 16;
/// Bounded wait for an in-progress image installation before sampling.
const INSTALL_WAIT_TRIES: u32 = 1000;

/// Core-resident R0 state. Inert (two atomics per checkpoint) without a hook.
#[derive(Default)]
pub(in crate::distributed::remote_db) struct CapacitySlot {
    /// Seqlock over installed-image changes: odd while cells or the manifest
    /// are being replaced. AcqRel increments; observers load with Acquire.
    /// Shared with roll cursors, which stamp each crossing with it.
    install_seq: Arc<AtomicU64>,
    /// Cut of the last image installed by this process; `None` before one.
    /// Cold: written once per checkpoint between the seqlock increments.
    installed_cut: Mutex<Option<Option<MutationVersion>>>,
    hook: ArcSwapOption<CapacityHook>,
    /// One observer per database: set by `start`, cleared by `uninstall`.
    enabled: AtomicBool,
}

pub(in crate::distributed::remote_db) struct CapacityHook {
    events: mpsc::SyncSender<ObserverEvent>,
    missed_prunes: AtomicU64,
    suppressed_retirements: AtomicU64,
}

impl CapacitySlot {
    pub(in crate::distributed::remote_db) fn begin_install(&self) {
        // AcqRel: the odd value is ordered before every cell replacement; the
        // shard locks then order it before any observer read of a new cell.
        self.install_seq.fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::distributed::remote_db) fn end_install(&self, cut: Option<MutationVersion>) {
        *self.installed_cut.lock() = Some(cut);
        // AcqRel: an observer that acquires this even value sees every cell,
        // the manifest and the cut written above.
        self.install_seq.fetch_add(1, Ordering::AcqRel);
    }

    /// With an observer installed: count the skipped retirement pass, post a
    /// prune event without waiting, and return true (skip retirement).
    pub(in crate::distributed::remote_db) fn shadow_prune(&self, core: &WriteCore) -> bool {
        let Some(hook) = self.hook.load_full() else {
            return false;
        };
        // Relaxed: independent counters read once by the observer at the end.
        hook.suppressed_retirements.fetch_add(1, Ordering::Relaxed);
        let exact = exact_key(core);
        if hook
            .events
            .try_send(ObserverEvent::Prune { exact })
            .is_err()
        {
            hook.missed_prunes.fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    fn install_seq(&self) -> u64 {
        self.install_seq.load(Ordering::Acquire)
    }
}

fn exact_key(core: &WriteCore) -> Option<ExactKey> {
    let mut publisher = core.publisher.lock();
    publisher
        .checkpoint_state()
        .manifest
        .checkpoint
        .as_ref()
        .map(|commit| ExactKey {
            fencing_generation: commit.epoch,
            checkpoint_generation: commit.generation,
            metadata_digest: hex(&commit.metadata_digest),
        })
}

fn logical(version: MutationVersion) -> LogicalCut {
    LogicalCut {
        fencing_generation: version.batch.epoch,
        sequence: version.batch.sequence,
        operation_index: version.operation_index,
    }
}

fn component(error: impl std::fmt::Display) -> R0Error {
    R0Error::new(error.to_string())
}

/// Handle owned by the benchmark. `finish` must run before the database
/// closes; dropping the handle uninstalls every hook and joins the thread.
pub struct CapacityObserver {
    events: mpsc::SyncSender<ObserverEvent>,
    thread: Option<JoinHandle<()>>,
    core: Weak<WriteCore>,
    geometry: R0Geometry,
}

impl CapacityObserver {
    pub(in crate::distributed::remote_db) fn start(
        core: &Arc<WriteCore>,
        config: CapacityObserverConfig,
        sink: Box<dyn R0Sink>,
    ) -> Result<Self, R0Error> {
        if config.relocation_max_reclaim_pct > 100 {
            return Err(R0Error::new("relocation_max_reclaim_pct exceeds 100"));
        }
        // AcqRel: exactly one concurrent start installs hooks; `uninstall`
        // clears the flag with Release after removing them.
        if core
            .capacity
            .enabled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(R0Error::new("a capacity observer is already enabled"));
        }
        let (events, inbox) = mpsc::sync_channel(OBSERVER_QUEUE);
        // Open one dedicated session per owner before any hook is installed.
        // Its Status handshake syncs the owner once, here, before `begin`.
        let tls_config = match core.config.tls.client_config_with_identity() {
            Ok(tls_config) => tls_config,
            Err(error) => {
                core.capacity.enabled.store(false, Ordering::Release);
                return Err(component(error));
            }
        };
        let mut sessions = Vec::with_capacity(core.config.endpoints.len());
        for endpoint in &core.config.endpoints {
            let mut tls = core.config.tls.clone();
            tls.server_name = endpoint.server_name.clone();
            match Client::connect_shared(
                &endpoint.address,
                &tls,
                tls_config.clone(),
                core.config.timeout,
                core.config.partition_config(1, endpoint.partition),
                &core.shape,
            ) {
                Ok(client) => sessions.push(client),
                Err(error) => {
                    // Release: no hook was installed; a later start may retry.
                    core.capacity.enabled.store(false, Ordering::Release);
                    return Err(component(error));
                }
            }
        }
        let mut clients = sessions.into_iter();
        let mut cursors = Vec::with_capacity(core.clients.len());
        for (node, endpoint) in core.config.endpoints.iter().enumerate() {
            let cursor = Arc::new(RollCursor::new(
                node,
                events.clone(),
                Arc::clone(&core.capacity.install_seq),
            ));
            if let Some(worker) = core.clients.get(node) {
                let mut client = worker.client.lock();
                // Read under the client lock: every batch submitted before the
                // cursor was installed was sequenced before this read.
                client.set_capacity_roll(Some(Arc::clone(&cursor)), core.next_sequence());
            }
            cursors.push(Cursor {
                node,
                partition: endpoint.partition,
                cursor,
                last_file: None,
                client: clients
                    .next()
                    .ok_or_else(|| R0Error::new("owner session missing"))?,
            });
        }
        let hook = Arc::new(CapacityHook {
            events: events.clone(),
            missed_prunes: AtomicU64::new(0),
            suppressed_retirements: AtomicU64::new(0),
        });
        core.capacity.hook.store(Some(Arc::clone(&hook)));
        let mut owners: Vec<u32> = core.config.endpoints.iter().map(|e| e.partition).collect();
        owners.sort_unstable();
        let geometry = R0Geometry {
            database: hex(&core.config.storage.database),
            owners,
            wal_file_bytes: core.config.storage.wal_file_bytes,
            fragment_bytes: core.config.storage.fragment_bytes,
            relocation_max_reclaim_pct: config.relocation_max_reclaim_pct,
        };
        let mut observer = Observer {
            core: Arc::downgrade(core),
            hook,
            cursors,
            config,
            sink,
            ledger: GrowthLedger::default(),
            begun: None,
            next_sample: 0,
            prune_events: 0,
            roll_events: 0,
            missed: 0,
            unlinks_begin: BTreeMap::new(),
            unlinks_last: BTreeMap::new(),
            crossings: BTreeMap::new(),
            dropped_at_begin: 0,
            sample_bounds: BTreeMap::new(),
            reasons: Vec::new(),
        };
        let thread = std::thread::Builder::new()
            .name("r0-capacity-observer".into())
            .spawn(move || observer.run(&inbox));
        let thread = match thread {
            Ok(thread) => thread,
            Err(error) => {
                uninstall(core);
                return Err(component(error));
            }
        };
        Ok(Self {
            events,
            thread: Some(thread),
            core: Arc::downgrade(core),
            geometry,
        })
    }

    pub fn geometry(&self) -> &R0Geometry {
        &self.geometry
    }

    /// Take the complete initial inventory. Call before timed mutations.
    pub fn begin(&self) -> Result<(), R0Error> {
        let (reply, result) = mpsc::sync_channel(1);
        self.events
            .send(ObserverEvent::Begin(reply))
            .map_err(|_| R0Error::new("capacity observer stopped"))?;
        result
            .recv()
            .map_err(|_| R0Error::new("capacity observer stopped"))?
    }

    /// Take the end inventory, uninstall the hooks, join, and return totals.
    pub fn finish(mut self) -> Result<R0Summary, R0Error> {
        self.stop()
            .unwrap_or_else(|| Err(R0Error::new("capacity observer stopped")))
    }

    fn stop(&mut self) -> Option<Result<R0Summary, R0Error>> {
        let thread = self.thread.take()?;
        let (reply, result) = mpsc::sync_channel(1);
        let summary = match self.events.send(ObserverEvent::End(reply)) {
            Ok(()) => result.recv().ok(),
            Err(_) => None,
        };
        if let Some(core) = self.core.upgrade() {
            uninstall(&core);
        }
        let joined = thread.join();
        match (summary, joined) {
            (Some(summary), Ok(())) => Some(summary),
            (_, Err(_)) => Some(Err(R0Error::new("capacity observer panicked"))),
            (None, Ok(())) => None,
        }
    }
}

impl Drop for CapacityObserver {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn uninstall(core: &WriteCore) {
    core.capacity.hook.store(None);
    for worker in &core.clients {
        worker.client.lock().set_capacity_roll(None, 0);
    }
    // Release: a later `start` sees every hook above removed.
    core.capacity.enabled.store(false, Ordering::Release);
}

struct Cursor {
    node: usize,
    partition: u32,
    cursor: Arc<RollCursor>,
    /// File the writer had moved to at the last consumed crossing.
    last_file: Option<u64>,
    /// Dedicated authenticated control session to this owner, owned by the
    /// observer thread; it carries only read-only inventory requests.
    client: Client,
}

/// Identity a sample must still observe for its event to be qualified.
enum Expected {
    Nothing,
    /// The Exact root installed when the prune posted its event.
    Exact(Option<ExactKey>),
    /// Installed-image seqlock stamps of the crossings the roll sample reports.
    Images(Vec<u64>),
}

/// Observer-thread state; nothing here is shared.
struct Observer {
    core: Weak<WriteCore>,
    hook: Arc<CapacityHook>,
    cursors: Vec<Cursor>,
    config: CapacityObserverConfig,
    sink: Box<dyn R0Sink>,
    ledger: GrowthLedger,
    begun: Option<Instant>,
    next_sample: u64,
    prune_events: u64,
    roll_events: u64,
    missed: u64,
    unlinks_begin: BTreeMap<u32, u64>,
    unlinks_last: BTreeMap<u32, u64>,
    /// Per partition: file moved to → sequence bound of that crossing.
    crossings: BTreeMap<u32, BTreeMap<u64, u64>>,
    /// Crossing events dropped on a full queue before `begin`.
    dropped_at_begin: u64,
    /// Looser bounds read after an inventory that showed the allocator past.
    sample_bounds: BTreeMap<(u32, u64), u64>,
    reasons: Vec<String>,
}

/// One owner stream's pages in a sample.
struct StreamObservation {
    partition: u32,
    generation: u64,
    header: Option<FileInventoryReply>,
    rows: Vec<WalFileObservation>,
    stable: bool,
    timing: OwnerRpcTiming,
}

/// Publisher state copied once per sample.
struct Snapshot {
    exact: Option<ExactKey>,
    obsolete_root: bool,
    positions: BTreeMap<(u32, u64), PositionFacts>,
    complete: BTreeSet<(u32, u64)>,
    retained: Vec<Vec<u64>>,
}

impl Snapshot {
    fn take(core: &WriteCore) -> Self {
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        Self {
            exact: state.manifest.checkpoint.as_ref().map(|commit| ExactKey {
                fencing_generation: commit.epoch,
                checkpoint_generation: commit.generation,
                metadata_digest: hex(&commit.metadata_digest),
            }),
            obsolete_root: state.manifest.checkpoint_obsolete.is_some(),
            positions: state
                .positions
                .iter()
                .map(|(key, position)| {
                    (
                        *key,
                        PositionFacts {
                            replay_from: position.replay_from,
                            last_offset: position.last_offset,
                            durable_end: position.durable_end,
                        },
                    )
                })
                .collect(),
            complete: state.committed_complete.clone(),
            retained: state.retained_epochs.clone(),
        }
    }
}

impl Observer {
    fn run(&mut self, inbox: &mpsc::Receiver<ObserverEvent>) {
        // Ends on `End` or when every sender (hook, cursors, handle) is gone.
        while let Ok(event) = inbox.recv() {
            match event {
                ObserverEvent::Begin(reply) => {
                    let result = self.begin();
                    let _ = reply.try_send(result);
                }
                ObserverEvent::Prune { exact } => {
                    if self.begun.is_some() {
                        self.prune_events += 1;
                        self.observe(SampleEvent::Prune, Vec::new(), Expected::Exact(exact));
                    }
                }
                ObserverEvent::Roll(crossing) => {
                    if let Some(rolled) = self.record_crossing(crossing)
                        && self.begun.is_some()
                    {
                        // Qualified only on the image installed at this roll.
                        let images = Expected::Images(vec![crossing.image]);
                        self.observe(SampleEvent::Roll, vec![rolled], images);
                    }
                }
                ObserverEvent::End(reply) => {
                    let result = self.end();
                    let _ = reply.try_send(result);
                    return;
                }
            }
        }
    }

    fn now_ns(&self) -> u64 {
        self.begun.map_or(0, |begun| {
            u64::try_from(begun.elapsed().as_nanos()).unwrap_or(u64::MAX)
        })
    }

    fn begin(&mut self) -> Result<(), R0Error> {
        if self.begun.is_some() {
            return Err(R0Error::new("capacity observation already began"));
        }
        // Crossings before `begin` are history: keep their bounds only.
        self.absorb_latest();
        self.dropped_at_begin = self.dropped();
        self.begun = Some(Instant::now());
        let sample = self.sample(SampleEvent::Begin, Vec::new(), Expected::Nothing)?;
        let complete = sample.complete;
        self.emit(&sample);
        if complete {
            Ok(())
        } else {
            Err(R0Error::new(format!(
                "initial owner inventory incomplete: {}",
                sample.reason.unwrap_or_default()
            )))
        }
    }

    fn end(&mut self) -> Result<R0Summary, R0Error> {
        if self.begun.is_none() {
            return Err(R0Error::new("capacity observation never began"));
        }
        // Dropped crossings got no roll sample of their own.
        self.absorb_latest();
        let dropped = self.dropped().saturating_sub(self.dropped_at_begin);
        self.roll_events += dropped;
        self.missed += dropped;
        let end = self.sample(SampleEvent::End, Vec::new(), Expected::Nothing);
        match &end {
            Ok(sample) => {
                if !sample.complete {
                    self.reasons.push(format!(
                        "end inventory incomplete: {}",
                        sample.reason.clone().unwrap_or_default()
                    ));
                }
                self.emit(sample);
            }
            Err(error) => self.reasons.push(format!("end inventory failed: {error}")),
        }
        let elapsed_ns = self.now_ns();
        // Relaxed: the prune path stopped posting before `End` was processed
        // or its late increments are reported as observed here.
        let missed_prunes = self.hook.missed_prunes.load(Ordering::Relaxed);
        let unlinks = if self.unlinks_begin.keys().eq(self.unlinks_last.keys())
            && !self.unlinks_begin.is_empty()
        {
            Some(
                self.unlinks_last
                    .iter()
                    .map(|(partition, last)| {
                        last.saturating_sub(self.unlinks_begin.get(partition).copied().unwrap_or(0))
                    })
                    .sum::<u64>(),
            )
        } else {
            None
        };
        match unlinks {
            Some(0) => {}
            Some(count) => self
                .reasons
                .push(format!("{count} Value WAL unlinks during a shadow run")),
            None => self
                .reasons
                .push("owner unlink counters not observed at begin and end".into()),
        }
        let complete = self.reasons.is_empty() && self.ledger.errors().is_empty();
        let (per_owner, worst_owner) = self.ledger.finish(elapsed_ns, complete);
        let mut reasons = self.reasons.clone();
        reasons.extend(self.ledger.errors().iter().cloned());
        Ok(R0Summary {
            elapsed_ns,
            sample_count: self.next_sample,
            prune_events: self.prune_events + missed_prunes,
            file_roll_events: self.roll_events,
            missed_events: self.missed + missed_prunes,
            value_wal_unlinks: unlinks,
            suppressed_retirement_passes: self.hook.suppressed_retirements.load(Ordering::Relaxed),
            complete,
            reasons,
            per_owner,
            worst_owner,
        })
    }

    /// Record one crossing event in arrival order. Each owner's events arrive
    /// in roll order; a stale one (behind a later file) only adds its bound.
    fn record_crossing(
        &mut self,
        crossing: crate::distributed::capacity::Crossing,
    ) -> Option<RolledFile> {
        let core = self.core.upgrade()?;
        let cursor = self.cursors.get_mut(crossing.owner)?;
        self.crossings
            .entry(cursor.partition)
            .or_default()
            .entry(crossing.file)
            .and_modify(|bound| *bound = (*bound).min(crossing.sequence_bound))
            .or_insert(crossing.sequence_bound);
        if cursor.last_file.is_some_and(|last| last >= crossing.file) {
            return None;
        }
        cursor.last_file = Some(crossing.file);
        self.begun?;
        self.roll_events += 1;
        Some(RolledFile {
            partition: cursor.partition,
            fencing_generation: core.epoch,
            file_index: crossing.file,
        })
    }

    /// Add the latest file/bound of each cursor (covers dropped events). The
    /// bound is at least as new as its file's, so it stays an upper bound.
    fn absorb_latest(&mut self) {
        for cursor in &mut self.cursors {
            if let Some((file, bound)) = cursor.cursor.latest() {
                self.crossings
                    .entry(cursor.partition)
                    .or_default()
                    .entry(file)
                    .or_insert(bound);
                if cursor.last_file.is_none_or(|last| last < file) {
                    cursor.last_file = Some(file);
                }
            }
        }
    }

    fn dropped(&self) -> u64 {
        self.cursors
            .iter()
            .map(|cursor| cursor.cursor.dropped())
            .sum()
    }

    fn observe(&mut self, event: SampleEvent, rolled: Vec<RolledFile>, expected: Expected) {
        match self.sample(event, rolled, expected) {
            Ok(sample) => self.emit(&sample),
            Err(error) => {
                self.missed += 1;
                self.reasons
                    .push(format!("{event:?} sample failed: {error}"));
            }
        }
    }

    fn emit(&mut self, sample: &R0Sample) {
        if let Err(error) = self.sink.sample(sample) {
            self.reasons.push(format!("r0 sink failed: {error}"));
        }
    }

    fn sample(
        &mut self,
        event: SampleEvent,
        rolled_files: Vec<RolledFile>,
        expected: Expected,
    ) -> Result<R0Sample, R0Error> {
        let core = self
            .core
            .upgrade()
            .ok_or_else(|| R0Error::new("database closed before the sample"))?;
        let sample_id = self.next_sample;
        self.next_sample += 1;
        let observed_start_ns = self.now_ns();
        let mut reasons = Vec::new();
        let mut seq = core.capacity.install_seq();
        for _ in 0..INSTALL_WAIT_TRIES {
            if seq % 2 == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
            seq = core.capacity.install_seq();
        }
        let installed_cut = *core.capacity.installed_cut.lock();
        let snapshot = Snapshot::take(&core);
        // A prune or roll sample is qualified only on the image its event saw;
        // a later cut is never attributed to it (design §2).
        match &expected {
            Expected::Exact(exact) if *exact != snapshot.exact => {
                self.missed += 1;
                reasons.push("Exact root replaced before the prune sample".to_owned());
            }
            Expected::Images(images) if images.iter().any(|image| *image != seq) => {
                self.missed += 1;
                reasons.push("Exact image changed between the file roll and its sample".to_owned());
            }
            _ => {}
        }
        let wal_file_bytes = core.config.storage.wal_file_bytes;
        let (retained, batch_error) = self.walk(&core, wal_file_bytes, &mut reasons)?;
        let mut volatile = BTreeSet::new();
        core.index.visit_volatile_references(|reference| {
            if reference.descriptor.kind == crate::distributed::types::RecordKind::Put {
                let address = reference.batch_address;
                let end = address.offset.saturating_add(u64::from(address.len));
                let mut start = address.offset - address.offset % wal_file_bytes;
                while start < end {
                    volatile.insert((address.partition, address.segment_generation, start));
                    start = start.saturating_add(wal_file_bytes);
                }
            }
        });
        let changed = seq % 2 == 1 || core.capacity.install_seq() != seq;
        if changed {
            reasons.push("Exact image changed during the walk".to_owned());
        }
        if let Some(error) = &batch_error {
            reasons.push(format!("distinct batch count failed: {error}"));
        }
        let streams = self.inventory(&core, &snapshot, event == SampleEvent::Begin, &mut reasons);
        // Read after every inventory: any batch in a file the allocator had
        // passed was sequenced before this read.
        let sequence_after = core.next_sequence();
        let durable_cut = core
            .durable_cut
            .get()
            .map(|cut| cut.stats())
            .filter(|stats| stats.enabled)
            .map(|stats| DurableCutWitness {
                fencing_generation: stats.epoch,
                persisted_next: stats.persisted_next,
            });
        let floor = core.index.retention_floor();
        // Recheck after the owner RPCs and the floor read: an image installed
        // meanwhile would pair the old Exact key with a new floor/cut, so the
        // sample must not be complete or credit any file.
        let installed_during_inventory = core.capacity.install_seq() != seq;
        if installed_during_inventory && !changed {
            reasons.push("Exact image changed during the owner inventory".to_owned());
        }
        let changed = changed || installed_during_inventory;
        let complete_inputs = reasons.is_empty();
        let files = self.rows(
            &core,
            &snapshot,
            &streams,
            &retained,
            batch_error.is_none(),
            &volatile,
            changed,
            sequence_after,
            durable_cut,
        );
        let complete = complete_inputs && streams.iter().all(|stream| stream.stable);
        if complete_inputs && !complete {
            reasons.push("owner inventory changed or failed during the sample".to_owned());
        }
        let (files, ledger) = files;
        let owners: Vec<u32> = core.config.endpoints.iter().map(|e| e.partition).collect();
        let reclaimed =
            self.ledger
                .observe(&owners, &ledger.0, complete, event == SampleEvent::End);
        let mut files = files;
        for (index, bytes) in ledger.1.into_iter().zip(reclaimed) {
            files[index].newly_shadow_reclaimed_bytes = bytes;
        }
        let owner_rpcs = streams.iter().map(|stream| stream.timing).collect();
        Ok(R0Sample {
            sample_id,
            event,
            observed_start_ns,
            observed_end_ns: self.now_ns(),
            complete,
            reason: (!reasons.is_empty()).then(|| reasons.join("; ")),
            rolled_files,
            exact_key: snapshot.exact,
            cut: installed_cut.flatten().map(logical),
            floor: match &floor {
                IndexReadCut::At(version) => Some(logical(*version)),
                _ => None,
            },
            floor_before_all: matches!(floor, IndexReadCut::BeforeAll),
            durable_cut,
            owner_rpcs,
            files,
        })
    }

    fn walk(
        &self,
        core: &WriteCore,
        wal_file_bytes: u64,
        reasons: &mut Vec<String>,
    ) -> Result<(BTreeMap<FileId, FileRetained>, Option<R0Error>), R0Error> {
        let mut accumulator = RetainedAccumulator::new(
            wal_file_bytes,
            self.config.max_batch_records,
            self.config.scratch_dir.clone(),
        )?;
        let mut failure = None;
        let walked = core
            .index
            .visit_installed_references(|reference| match reference {
                Some(reference) if failure.is_none() => {
                    if let Err(error) = accumulator.add_put(reference) {
                        failure = Some(error);
                    }
                }
                Some(_) => {}
                None => accumulator.end_cell(),
            });
        if let Err(error) = walked {
            reasons.push(format!("installed image walk failed: {error}"));
        }
        if let Some(error) = failure {
            reasons.push(format!("retained attribution failed: {error}"));
        }
        Ok(accumulator.finish())
    }

    fn inventory(
        &mut self,
        core: &Arc<WriteCore>,
        snapshot: &Snapshot,
        begin: bool,
        reasons: &mut Vec<String>,
    ) -> Vec<StreamObservation> {
        let mut streams = Vec::new();
        for index in 0..self.cursors.len() {
            let (node, partition) = (self.cursors[index].node, self.cursors[index].partition);
            let mut generations: Vec<u64> =
                snapshot.retained.get(node).cloned().unwrap_or_default();
            generations.push(core.epoch);
            generations.sort_unstable();
            generations.dedup();
            for generation in generations {
                let started = self.now_ns();
                let observation = page_stream(
                    &mut self.cursors[index].client,
                    core.epoch,
                    partition,
                    generation,
                    started,
                );
                match observation {
                    Ok(mut observation) => {
                        observation.timing.observed_end_ns = self.now_ns();
                        if let Some(header) = &observation.header {
                            self.unlinks_last
                                .insert(partition, header.value_wal_unlinks);
                            if begin {
                                self.unlinks_begin
                                    .insert(partition, header.value_wal_unlinks);
                            }
                        }
                        streams.push(observation);
                    }
                    Err(error) => {
                        reasons.push(format!(
                            "partition {} generation {generation} inventory failed: {error}",
                            partition
                        ));
                        streams.push(StreamObservation {
                            partition,
                            generation,
                            header: None,
                            rows: Vec::new(),
                            stable: false,
                            timing: OwnerRpcTiming {
                                partition,
                                fencing_generation: generation,
                                observed_start_ns: self.now_ns(),
                                observed_end_ns: self.now_ns(),
                                pages: 0,
                                rows: 0,
                                stable: false,
                            },
                        });
                    }
                }
            }
        }
        streams
    }

    /// Build sorted file rows and the ledger inputs `(files, row index)`.
    #[allow(clippy::too_many_arguments, reason = "one sample's independent inputs")]
    fn rows(
        &mut self,
        core: &WriteCore,
        snapshot: &Snapshot,
        streams: &[StreamObservation],
        retained: &BTreeMap<FileId, FileRetained>,
        batches_valid: bool,
        volatile: &BTreeSet<(u32, u64, u64)>,
        changed: bool,
        sequence_after: u64,
        durable_cut: Option<DurableCutWitness>,
    ) -> (Vec<R0File>, (Vec<LedgerFile>, Vec<usize>)) {
        let wal_file_bytes = core.config.storage.wal_file_bytes;
        let pct = self.config.relocation_max_reclaim_pct;
        let mut facts: BTreeMap<(u32, u64), StreamFacts> = BTreeMap::new();
        let mut spans: BTreeMap<u32, Vec<OwnerSpan>> = BTreeMap::new();
        let mut keys: BTreeSet<(u32, u64, u64)> = BTreeSet::new();
        let mut observed: BTreeMap<(u32, u64, u64), &WalFileObservation> = BTreeMap::new();
        for stream in streams {
            let key = (stream.partition, stream.generation);
            let same_generation = stream.generation == core.epoch;
            facts.insert(
                key,
                StreamFacts {
                    same_generation,
                    complete_stream: snapshot.complete.contains(&key),
                    position: snapshot.positions.get(&key).copied(),
                    owner_root_scan_from: stream.header.as_ref().and_then(|h| h.wal_scan_from),
                    owner_root_synced_end: stream.header.as_ref().and_then(|h| h.wal_synced_end),
                    processed_end: stream.header.as_ref().map_or(0, |h| h.processed_end),
                    page_stable: stream.stable,
                    persisted_cut_next: durable_cut
                        .filter(|cut| {
                            same_generation && cut.fencing_generation == stream.generation
                        })
                        .map(|cut| cut.persisted_next),
                },
            );
            let used = stream
                .rows
                .iter()
                .filter(|row| matches!(row.state, FileUseState::Used | FileUseState::Leased));
            if let (Some(lo), Some(header)) = (used.map(|row| row.file_start).min(), &stream.header)
            {
                spans.entry(stream.partition).or_default().push(OwnerSpan {
                    generation: stream.generation,
                    lo,
                    hi: header.processed_end,
                });
            }
            for row in &stream.rows {
                if same_generation && row.allocator_past {
                    self.sample_bounds
                        .entry((stream.partition, row.file_start / wal_file_bytes))
                        .or_insert(sequence_after);
                }
                let file = (stream.partition, stream.generation, row.file_start);
                keys.insert(file);
                observed.insert(file, row);
            }
        }
        keys.extend(
            retained
                .keys()
                .map(|id| (id.partition, id.generation, id.start)),
        );
        keys.extend(volatile.iter().copied());
        let budgets: BTreeMap<u32, BTreeMap<u64, u64>> = spans
            .iter()
            .map(|(partition, spans)| (*partition, span_budget_end(spans, wal_file_bytes, pct)))
            .collect();
        let missing = StreamFacts {
            same_generation: false,
            complete_stream: false,
            position: None,
            owner_root_scan_from: None,
            owner_root_synced_end: None,
            processed_end: 0,
            page_stable: false,
            persisted_cut_next: None,
        };
        let mut files = Vec::with_capacity(keys.len());
        let mut ledger = (Vec::new(), Vec::new());
        for (partition, generation, file_start) in keys {
            let file_end = file_start.saturating_add(wal_file_bytes);
            let stream = facts.get(&(partition, generation)).unwrap_or(&missing);
            let row = observed.get(&(partition, generation, file_start)).copied();
            let counters = retained
                .get(&FileId {
                    partition,
                    generation,
                    start: file_start,
                })
                .copied()
                .unwrap_or_default();
            let index = file_start / wal_file_bytes;
            let sequence_bound = (generation == core.epoch)
                .then(|| {
                    let crossing = self
                        .crossings
                        .get(&partition)
                        .and_then(|log| log.range(index + 1..).next().map(|(_, bound)| *bound));
                    let sampled = self.sample_bounds.get(&(partition, index)).copied();
                    match (crossing, sampled) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    }
                })
                .flatten();
            let within_budget = budgets
                .get(&partition)
                .and_then(|ends| ends.get(&generation))
                .is_some_and(|end| file_end <= *end);
            let blockers = evaluate(&FileInputs {
                file_start,
                file_end,
                observation: row,
                stream,
                adopted_root: snapshot.exact.is_some(),
                obsolete_root_pending: snapshot.obsolete_root,
                retained_puts: counters.put_members,
                volatile_reference: volatile.contains(&(partition, generation, file_start)),
                sequence_bound,
                within_budget,
                changed,
            });
            let eligible = blockers.is_empty();
            if let Some(row) = row {
                ledger.0.push(LedgerFile {
                    partition,
                    generation,
                    start: file_start,
                    device: row.device,
                    inode: row.inode,
                    allocated: row.allocated_bytes,
                    eligible,
                    retained_puts: counters.put_members,
                });
                ledger.1.push(files.len());
            }
            files.push(R0File {
                partition,
                fencing_generation: generation,
                file_start,
                file_end,
                device: row.map(|row| row.device),
                inode: row.map(|row| row.inode),
                logical_bytes: row.map(|row| row.logical_bytes),
                allocated_bytes: row.map(|row| row.allocated_bytes),
                state: row.map_or(FileUseState::Unknown, |row| row.state),
                retained_put_members: counters.put_members,
                retained_member_bytes: counters.member_bytes,
                retained_value_bytes: counters.value_bytes,
                retained_batches: batches_valid.then_some(counters.batches),
                retained_batch_bytes: batches_valid.then_some(counters.batch_bytes),
                affected_cells: counters.affected_cells,
                owner_root_generation: streams
                    .iter()
                    .find(|s| (s.partition, s.generation) == (partition, generation))
                    .and_then(|s| s.header.as_ref())
                    .and_then(|h| h.owner_root_generation),
                wal_scan_from: stream.owner_root_scan_from,
                wal_synced_end: stream.owner_root_synced_end,
                replay_from: stream.position.map(|p| p.replay_from),
                last_offset: stream.position.and_then(|p| p.last_offset),
                durable_end: stream.position.map(|p| p.durable_end),
                complete_stream: facts
                    .contains_key(&(partition, generation))
                    .then_some(stream.complete_stream),
                validated_through: row.and_then(|row| row.validated_through),
                last_batch_sequence: None,
                batch_sequence_bound: sequence_bound,
                read_pins: row.and_then(|row| row.read_pins),
                export_pins: row.and_then(|row| row.export_pins),
                blockers,
                shadow_eligible: eligible,
                newly_shadow_reclaimed_bytes: 0,
            });
        }
        (files, ledger)
    }
}

/// Page one owner stream to its end over the observer's own control session.
/// Never the owner worker's control lane: a worker control settles the open
/// leased extent (skip marker), which would create the growth R0 measures.
fn page_stream(
    client: &mut Client,
    authority: u64,
    partition: u32,
    generation: u64,
    started_ns: u64,
) -> Result<StreamObservation, R0Error> {
    let mut after = None;
    let mut rows = Vec::new();
    let mut header: Option<FileInventoryReply> = None;
    let mut stable = true;
    for page in 0..MAX_PAGES_PER_STREAM {
        let reply = client
            .file_inventory_page(authority, generation, after, MAX_FILE_INVENTORY_PAGE)
            .map_err(component)?;
        stable &= reply.stable;
        if let Some(first) = &header
            && (first.owner_incarnation != reply.owner_incarnation
                || first.owner_root_generation != reply.owner_root_generation)
        {
            stable = false;
        }
        let full = reply.rows.len() == usize::from(MAX_FILE_INVENTORY_PAGE);
        after = reply.rows.last().map(|row| row.file_start);
        let mut reply = reply;
        rows.append(&mut reply.rows);
        header = Some(reply);
        if !full {
            let rows_seen = u64::try_from(rows.len()).unwrap_or(u64::MAX);
            return Ok(StreamObservation {
                partition,
                generation,
                header,
                rows,
                stable,
                timing: OwnerRpcTiming {
                    partition,
                    fencing_generation: generation,
                    observed_start_ns: started_ns,
                    observed_end_ns: started_ns,
                    pages: u64::try_from(page + 1).unwrap_or(u64::MAX),
                    rows: rows_seen,
                    stable,
                },
            });
        }
    }
    Err(R0Error::new("owner inventory exceeds its page bound"))
}
