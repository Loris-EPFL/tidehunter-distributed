//! Independent committed reads and one bounded, pipelined write owner.
//!
//! eRPC §§3.1–3.2 and `ReFlex` §§3–4 separate admission from completion; the
//! owner never waits for a single append before accepting another batch. Like
//! `FoundationDB` §§2.3–2.6, read cuts and durable ordered publication remain
//! separate from scheduling. This RF1 path still requires synced WAL receipts.
use super::super::hybrid::read::ValueReader;
use super::super::hybrid::table::HybridTable;
use super::super::hybrid::{SnapshotConfig, StoredSnapshot, spawn_snapshot_thread};
use super::super::native_publication::member_positions;
#[allow(clippy::wildcard_imports)]
// Private implementation shares its parent's internal types.
use super::*;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

mod hard_wait;
mod reads;
pub use hard_wait::{HardWaitSnapshot, WaitReasonTotals};

#[derive(Default)]
struct CheckpointBudget {
    identities: usize,
    partition_batches: Vec<usize>,
    partition_records: Vec<usize>,
}

#[derive(Default)]
struct WriteBudget {
    requests: usize,
    bytes: usize,
    slots: usize,
    index_bytes: usize,
    peak_requests: usize,
    peak_bytes: usize,
    closed: bool,
    waiters: VecDeque<Arc<AdmissionWaiter>>,
    live_waiters: usize,
    checkpoint: CheckpointBudget,
    hard_wait: hard_wait::HardWaitTracker,
    /// Test-only lower identity bound, applied inside the grant predicate.
    #[cfg(test)]
    identity_limit: Option<usize>,
}
/// One FIFO grant covers the complete invocation window. Partitions are
/// ascending and unique; each entry is (endpoint index, batches, records).
#[derive(Default)]
pub(in crate::distributed::remote_db) struct WriteGrant {
    pub requests: usize,
    pub bytes: usize,
    pub slots: usize,
    pub identities: usize,
    pub index_bytes: usize,
    pub partitions: SmallVec<[(usize, usize, usize); 4]>,
}

impl WriteGrant {
    #[cfg(test)]
    fn request(bytes: usize) -> Self {
        Self {
            requests: 1,
            bytes,
            ..Self::default()
        }
    }

    pub fn add_partition(&mut self, partition: usize, records: usize) -> Result<(), LocalDbError> {
        match self
            .partitions
            .binary_search_by_key(&partition, |entry| entry.0)
        {
            Ok(position) => {
                let entry = &mut self.partitions[position];
                entry.1 = entry
                    .1
                    .checked_add(1)
                    .ok_or_else(|| LocalDbError::new("partition reservation overflow"))?;
                entry.2 = entry
                    .2
                    .checked_add(records)
                    .ok_or_else(|| LocalDbError::new("reference reservation overflow"))?;
            }
            Err(position) => self.partitions.insert(position, (partition, 1, records)),
        }
        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::distributed::remote_db) struct AdmissionCharges {
    pub requests: usize,
    pub bytes: usize,
    pub slots: usize,
    pub index_bytes: usize,
    pub identities: usize,
    pub partition_batches: usize,
    pub partition_records: usize,
}

pub(super) struct Admission {
    budget: Mutex<WriteBudget>,
    // Cold shutdown/diagnostic wait paired with budget. Reuse completers notify
    // under this existing mutex, so shutdown needs no new hot-path lock.
    drained: parking_lot::Condvar,
    max_requests: usize,
    max_bytes: usize,
    max_identities: usize,
    max_index_bytes: usize,
    partition_batches: usize,
    reference_bytes: usize,
    timeout: Duration,
    first_failure: Arc<std::sync::OnceLock<FirstFailure>>,
}
pub(in crate::distributed::remote_db) struct WriteCredit {
    admission: Arc<Admission>,
    charge: CreditCharge,
    committed: bool,
}

enum CreditCharge {
    Direct(WriteGrant),
    Queued(Arc<AdmissionWaiter>),
}

impl CreditCharge {
    fn grant(&self) -> &WriteGrant {
        match self {
            Self::Direct(grant) => grant,
            Self::Queued(waiter) => &waiter.grant,
        }
    }
}

impl WriteCredit {
    /// Called under the allocation locks before the first fetch_add. A panic
    /// after allocation must retain persistent credits, not reuse unknown slots.
    pub(in crate::distributed::remote_db) fn mark_allocated(&mut self) {
        self.committed = true;
    }
}

impl Drop for WriteCredit {
    fn drop(&mut self) {
        self.admission.release(self.charge.grant(), self.committed);
    }
}

const WAITER_QUEUED: u8 = 0;
const WAITER_GRANTED: u8 = 1;
const WAITER_CLOSED: u8 = 2;

struct AdmissionWaiter {
    grant: WriteGrant,
    thread: thread::Thread,
    state: AtomicU8,
}

/// One node per live blocked call. There is no independent producer of nodes:
/// queue length is bounded by live blocked callers, not by admitted batch count.
/// This guard also returns a grant if the blocked call unwinds before claiming it.
struct WaitingReservation<'a> {
    admission: &'a Admission,
    waiter: Arc<AdmissionWaiter>,
    claimed: bool,
    /// Blocking reasons, ended under the budget mutex at claim or drop.
    wait: hard_wait::WaiterWait,
}

impl Drop for WaitingReservation<'_> {
    fn drop(&mut self) {
        if self.claimed {
            return;
        }
        let mut budget = self.admission.budget.lock();
        budget
            .hard_wait
            .set(&mut self.wait, 0, diagnostics_clock_ns());
        if let Some(position) = budget
            .waiters
            .iter()
            .position(|waiter| Arc::ptr_eq(waiter, &self.waiter))
        {
            budget.waiters.remove(position);
        } else if self.waiter.state.load(Ordering::Relaxed) == WAITER_GRANTED {
            // The mutex transfers ownership of the whole grant exactly once.
            Admission::uncharge(&mut budget, &self.waiter.grant, false);
        }
        budget.live_waiters -= 1;
        let granted = self.admission.grant_waiters(&mut budget);
        drop(budget);
        wake_granted(granted);
    }
}

fn wake_granted(granted: SmallVec<[Arc<AdmissionWaiter>; 4]>) {
    for waiter in granted {
        waiter.thread.unpark();
    }
}

impl Admission {
    pub(super) fn for_open(database: &super::write_core::core::OpenState) -> Arc<Self> {
        // This is the exact fixed-width checkpoint_metadata_fits inequality,
        // solved once at open rather than recomputed on every FIFO grant. The
        // streams are the actual retained (owner, generation) pairs; open-time
        // trims only shrink them, and no generation is added while serving.
        let streams: usize = database
            .publisher
            .retained_epochs
            .iter()
            .map(Vec::len)
            .sum();
        let metadata_identities = MAX_CHECKPOINT_METADATA_BYTES
            .checked_sub(1024 * 1024)
            .map(|bytes| bytes / 128)
            .and_then(|entries| entries.checked_sub(u64::try_from(streams).ok()?))
            .and_then(|entries| usize::try_from(entries).ok())
            .unwrap_or(0);
        let max_identities = if metadata_identities >= WAL_REPLAY_WINDOW_BATCHES {
            database.config.max_admitted_batches
        } else {
            database
                .config
                .max_admitted_batches
                .min(metadata_identities)
        };
        Arc::new(Self {
            budget: Mutex::new(WriteBudget {
                checkpoint: CheckpointBudget {
                    // A hybrid write carries no identity charge (S7): the retry
                    // window is the receipt window, bounded by itself.
                    identities: 0,
                    partition_batches: vec![0; database.clients.len()],
                    partition_records: vec![0; database.clients.len()],
                },
                ..WriteBudget::default()
            }),
            drained: parking_lot::Condvar::new(),
            max_requests: database.config.max_pending_batches,
            max_bytes: database.config.max_pending_bytes,
            max_identities,
            max_index_bytes: database.config.max_index_bytes,
            partition_batches: database.config.storage.max_batches_per_partition.min(1024),
            reference_bytes: database.config.storage.max_reference_metadata_bytes,
            timeout: database.config.timeout,
            first_failure: Arc::clone(&database.first_failure),
        })
    }

    /// Peak caller-held request credits and bytes. A caller holds its credit
    /// from grant until its outcome returns, so the request peak is both the
    /// admission queue peak and the concurrently pending write peak.
    pub(in crate::distributed::remote_db) fn pipeline_peaks(&self) -> (usize, usize) {
        let budget = self.budget.lock();
        (budget.peak_requests, budget.peak_bytes)
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn queued_waiters(&self) -> usize {
        self.budget.lock().waiters.len()
    }

    /// Every charged dimension, copied under the budget mutex.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn charges(&self) -> AdmissionCharges {
        Self::charges_of(&self.budget.lock())
    }

    #[cfg(test)]
    fn charges_of(budget: &WriteBudget) -> AdmissionCharges {
        AdmissionCharges {
            requests: budget.requests,
            bytes: budget.bytes,
            slots: budget.slots,
            index_bytes: budget.index_bytes,
            identities: budget.checkpoint.identities,
            partition_batches: budget.checkpoint.partition_batches.iter().sum(),
            partition_records: budget.checkpoint.partition_records.iter().sum(),
        }
    }

    /// Waits on the existing drained condvar until `done` holds. Every release
    /// path notifies it through grant_waiters once no request credit remains,
    /// so callers must not hold an unreleased request credit while waiting.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn wait_charges(
        &self,
        deadline: Instant,
        done: impl Fn(AdmissionCharges) -> bool,
    ) -> bool {
        let mut budget = self.budget.lock();
        loop {
            if done(Self::charges_of(&budget)) {
                return true;
            }
            if self.drained.wait_until(&mut budget, deadline).timed_out() {
                return done(Self::charges_of(&budget));
            }
        }
    }

    pub(super) fn hard_wait_snapshot(&self) -> HardWaitSnapshot {
        let now = diagnostics_clock_ns();
        self.budget.lock().hard_wait.snapshot(now)
    }

    /// Return every batch/record/slot dimension in one transaction, after all
    /// publisher, slot, identity and dependency guards have been dropped.
    /// `_replay` is the old replay-tail dimension: no grant charges it any more
    /// (the hybrid has no replay limit), so it is ignored. The parameter keeps
    /// the three-argument call sites of the write path unchanged; the cleanup
    /// that removes it lands with the Exact deletion.
    pub(super) fn release_publication(
        &self,
        slots: usize,
        partitions: &[(usize, usize)],
        _replay: (usize, usize),
    ) -> Result<(), LocalDbError> {
        let mut budget = self.budget.lock();
        if slots > budget.slots
            || partitions.len() != budget.checkpoint.partition_batches.len()
            || partitions.len() != budget.checkpoint.partition_records.len()
            || partitions
                .iter()
                .enumerate()
                .any(|(partition, &(batches, records))| {
                    batches > budget.checkpoint.partition_batches[partition]
                        || records > budget.checkpoint.partition_records[partition]
                })
        {
            return Err(LocalDbError::new(
                "publication credit release exceeds admission charge",
            ));
        }
        budget.slots -= slots;
        for (partition, &(batches, records)) in partitions.iter().enumerate() {
            budget.checkpoint.partition_batches[partition] -= batches;
            budget.checkpoint.partition_records[partition] -= records;
        }
        let granted = self.grant_waiters(&mut budget);
        drop(budget);
        wake_granted(granted);
        Ok(())
    }

    pub(super) fn return_slots(&self, count: usize) -> Result<(), LocalDbError> {
        if count == 0 {
            return Ok(());
        }
        let mut budget = self.budget.lock();
        if count > budget.slots {
            return Err(LocalDbError::new("slot release exceeds admission charge"));
        }
        budget.slots -= count;
        let granted = self.grant_waiters(&mut budget);
        drop(budget);
        wake_granted(granted);
        Ok(())
    }

    pub(super) fn regrant(&self) {
        let granted = self.grant_waiters(&mut self.budget.lock());
        wake_granted(granted);
    }

    fn fits(&self, budget: &WriteBudget, grant: &WriteGrant) -> bool {
        budget
            .requests
            .checked_add(grant.requests)
            .is_some_and(|n| n <= self.max_requests)
            && budget
                .bytes
                .checked_add(grant.bytes)
                .is_some_and(|n| n <= self.max_bytes)
            && budget
                .slots
                .checked_add(grant.slots)
                .is_some_and(|n| n <= self.max_requests)
            && self.checkpoint_fits(budget, grant)
            && grant
                .partitions
                .iter()
                .all(|&(partition, batches, records)| {
                    let Some(batches) = budget
                        .checkpoint
                        .partition_batches
                        .get(partition)
                        .and_then(|current| current.checked_add(batches))
                    else {
                        return false;
                    };
                    let Some(records) = budget
                        .checkpoint
                        .partition_records
                        .get(partition)
                        .and_then(|current| current.checked_add(records))
                    else {
                        return false;
                    };
                    ReferenceDirectory::check_fresh_total_capacity(
                        self.partition_batches,
                        self.reference_bytes,
                        batches,
                        records,
                    )
                    .is_ok()
                })
    }

    fn checkpoint_fits(&self, budget: &WriteBudget, grant: &WriteGrant) -> bool {
        self.checkpoint_pressure_for(budget, grant).is_none()
    }

    /// The first checkpoint-bound dimension that blocks this grant, if any.
    /// Only checkpoint apply releases these, so a blocked caller requests one.
    fn checkpoint_pressure_for(
        &self,
        budget: &WriteBudget,
        grant: &WriteGrant,
    ) -> Option<CheckpointPressure> {
        // A hybrid write carries no replay-tail charge (WAL replay has no hard
        // bound), so only the identity and index dimensions can name a reason.
        let reason = self.checkpoint_block(budget, grant, false).0?;
        Some(CheckpointPressure {
            reason,
            batches: 0,
            encoded_bytes: 0,
            max_batches: usize::MAX,
            max_encoded_bytes: usize::MAX,
            incoming_encoded_bytes: None,
        })
    }

    /// The first blocking checkpoint dimension, in priority order, and the
    /// wait bits of the dimensions evaluated. `all` evaluates every dimension
    /// (waiting path); otherwise evaluation stops at the first block.
    fn checkpoint_block(
        &self,
        budget: &WriteBudget,
        grant: &WriteGrant,
        all: bool,
    ) -> (Option<&'static str>, u8) {
        let tail = &budget.checkpoint;
        #[cfg(test)]
        let max_identities = budget
            .identity_limit
            .map_or(self.max_identities, |limit| limit.min(self.max_identities));
        #[cfg(not(test))]
        let max_identities = self.max_identities;
        // WAL replay deliberately has no hard tail bound.
        let identity = tail
            .identities
            .checked_add(grant.identities)
            .is_none_or(|n| n > max_identities);
        if identity && !all {
            return (Some("identity"), hard_wait::WAIT_IDENTITY);
        }
        let index = budget
            .index_bytes
            .checked_add(grant.index_bytes)
            .is_none_or(|n| n > self.max_index_bytes);
        let first = if identity {
            Some("identity")
        } else {
            index.then_some("index")
        };
        let bits = (if identity {
            hard_wait::WAIT_IDENTITY
        } else {
            0
        }) | (if index { hard_wait::WAIT_INDEX } else { 0 });
        (first, bits)
    }

    /// Wait bits of a queued grant: its checkpoint dimensions, or `other`.
    fn wait_bits(&self, budget: &WriteBudget, grant: &WriteGrant) -> u8 {
        match self.checkpoint_block(budget, grant, true).1 {
            0 => hard_wait::WAIT_OTHER,
            bits => bits,
        }
    }

    /// Lower the identity (checkpoint receipt metadata) bound for a test,
    /// under the budget mutex so every later grant predicate observes it.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn limit_identities_for_test(&self, limit: usize) {
        self.budget.lock().identity_limit = Some(limit);
    }

    fn charge_grant(budget: &mut WriteBudget, grant: &WriteGrant) {
        // The complete predicate was checked under this same budget mutex.
        budget.requests += grant.requests;
        budget.bytes += grant.bytes;
        budget.slots += grant.slots;
        budget.index_bytes += grant.index_bytes;
        budget.checkpoint.identities += grant.identities;
        for &(partition, batches, records) in &grant.partitions {
            budget.checkpoint.partition_batches[partition] += batches;
            budget.checkpoint.partition_records[partition] += records;
        }
        budget.peak_requests = budget.peak_requests.max(budget.requests);
        budget.peak_bytes = budget.peak_bytes.max(budget.bytes);
    }

    fn resources_owned(budget: &WriteBudget, grant: &WriteGrant) -> bool {
        grant.slots <= budget.slots
            && grant.index_bytes <= budget.index_bytes
            && grant.identities <= budget.checkpoint.identities
            && grant
                .partitions
                .iter()
                .all(|&(partition, batches, records)| {
                    budget
                        .checkpoint
                        .partition_batches
                        .get(partition)
                        .is_some_and(|n| batches <= *n)
                        && budget
                            .checkpoint
                            .partition_records
                            .get(partition)
                            .is_some_and(|n| records <= *n)
                })
    }

    fn uncharge_resources(budget: &mut WriteBudget, grant: &WriteGrant) {
        budget.slots -= grant.slots;
        budget.index_bytes -= grant.index_bytes;
        budget.checkpoint.identities -= grant.identities;
        for &(partition, batches, records) in &grant.partitions {
            budget.checkpoint.partition_batches[partition] -= batches;
            budget.checkpoint.partition_records[partition] -= records;
        }
    }

    fn uncharge(budget: &mut WriteBudget, grant: &WriteGrant, committed: bool) {
        // A move-only credit or an unclaimed FIFO node owns this grant exactly
        // once. Violating ownership is a programmer error, not backpressure.
        assert!(grant.requests <= budget.requests && grant.bytes <= budget.bytes);
        budget.requests -= grant.requests;
        budget.bytes -= grant.bytes;
        if !committed {
            assert!(
                Self::resources_owned(budget, grant),
                "cancelled grant owns its resources"
            );
            Self::uncharge_resources(budget, grant);
        }
    }

    #[cfg(test)]
    fn charge(budget: &mut WriteBudget, bytes: usize) {
        Self::charge_grant(budget, &WriteGrant::request(bytes));
    }

    fn grant_waiters(&self, budget: &mut WriteBudget) -> SmallVec<[Arc<AdmissionWaiter>; 4]> {
        let mut granted = SmallVec::new();
        // Each grant consumes a request slot, so at most max_requests can fit.
        for _ in 0..self.max_requests {
            if budget.closed
                || budget
                    .waiters
                    .front()
                    .is_none_or(|waiter| !self.fits(budget, &waiter.grant))
            {
                break;
            }
            let Some(waiter) = budget.waiters.pop_front() else {
                break;
            };
            Self::charge_grant(budget, &waiter.grant);
            // Release publishes the exact reservation before unpark. The caller
            // checks with Acquire, including when unpark precedes its first park.
            waiter.state.store(WAITER_GRANTED, Ordering::Release);
            granted.push(waiter);
        }
        debug_assert!(budget.waiters.len() <= budget.live_waiters);
        if budget.closed || budget.requests == 0 {
            self.drained.notify_all();
        }
        granted
    }

    fn enqueue_grant<'a>(
        &'a self,
        budget: &mut WriteBudget,
        grant: WriteGrant,
    ) -> WaitingReservation<'a> {
        let waiter = Arc::new(AdmissionWaiter {
            grant,
            thread: thread::current(),
            state: AtomicU8::new(WAITER_QUEUED),
        });
        budget.live_waiters += 1;
        budget.waiters.push_back(waiter.clone());
        debug_assert!(budget.waiters.len() <= budget.live_waiters);
        let mut reservation = WaitingReservation {
            admission: self,
            waiter,
            claimed: false,
            wait: hard_wait::WaiterWait::default(),
        };
        let bits = self.wait_bits(budget, &reservation.waiter.grant);
        budget
            .hard_wait
            .set(&mut reservation.wait, bits, diagnostics_clock_ns());
        reservation
    }

    #[cfg(test)]
    fn enqueue<'a>(&'a self, budget: &mut WriteBudget, bytes: usize) -> WaitingReservation<'a> {
        self.enqueue_grant(budget, WriteGrant::request(bytes))
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        healthy: impl Fn() -> Result<(), LocalDbError>,
    ) -> Result<WriteCredit, LocalDbError> {
        self.reserve_grant(WriteGrant::request(bytes), true, healthy, |_| Ok(()))
    }

    pub(in crate::distributed::remote_db) fn reserve_grant(
        self: &Arc<Self>,
        grant: WriteGrant,
        block: bool,
        healthy: impl Fn() -> Result<(), LocalDbError>,
        checkpoint: impl Fn(CheckpointPressure) -> Result<(), LocalDbError>,
    ) -> Result<WriteCredit, LocalDbError> {
        if grant.bytes > self.max_bytes {
            return Err(LocalDbError::new("write exceeds admission byte budget"));
        }
        if grant.requests == 0
            || grant.requests > self.max_requests
            || grant.slots > self.max_requests
            || grant.identities > self.max_identities
            || grant.index_bytes > self.max_index_bytes
        {
            return Err(LocalDbError::new("write exceeds admission window capacity"));
        }
        let partition_count = self.budget.lock().checkpoint.partition_batches.len();
        let mut previous = None;
        for &(partition, batches, records) in &grant.partitions {
            if partition >= partition_count || previous.is_some_and(|old| old >= partition) {
                return Err(LocalDbError::new("invalid admission partition charges"));
            }
            ReferenceDirectory::check_fresh_total_capacity(
                self.partition_batches,
                self.reference_bytes,
                batches,
                records,
            )
            .map_err(LocalDbError::component)?;
            previous = Some(partition);
        }
        let deadline = Instant::now() + self.timeout;
        let mut budget = self.budget.lock();
        healthy()?;
        if budget.closed {
            return Err(self.closed_error());
        }
        if budget.waiters.is_empty() && self.fits(&budget, &grant) {
            Self::charge_grant(&mut budget, &grant);
            return Ok(WriteCredit {
                admission: self.clone(),
                charge: CreditCharge::Direct(grant),
                committed: false,
            });
        }
        if !block {
            return Err(LocalDbError::new(
                "TCP authority admission capacity reached",
            ));
        }
        let mut waiting = self.enqueue_grant(&mut budget, grant);
        drop(budget);
        // Deadline bounds this predicate loop, including spurious unparks.
        loop {
            match waiting.waiter.state.load(Ordering::Acquire) {
                WAITER_GRANTED => {
                    healthy()?;
                    let mut budget = self.budget.lock();
                    budget
                        .hard_wait
                        .set(&mut waiting.wait, 0, diagnostics_clock_ns());
                    if budget.closed {
                        return Err(self.closed_error());
                    }
                    budget.live_waiters -= 1;
                    waiting.claimed = true;
                    return Ok(WriteCredit {
                        admission: self.clone(),
                        charge: CreditCharge::Queued(Arc::clone(&waiting.waiter)),
                        committed: false,
                    });
                }
                WAITER_CLOSED => return Err(self.closed_error()),
                _ => {
                    // Health is checked before enqueue and again at claim. A
                    // failure or shutdown while queued always reaches this
                    // waiter through close() (WAITER_CLOSED); rechecking here
                    // would cancel a queued grant before it can be claimed.
                    if Instant::now() >= deadline {
                        return Err(LocalDbError::new("write admission deadline expired"));
                    }
                    let pressure = {
                        let mut budget = self.budget.lock();
                        let bits = self.wait_bits(&budget, &waiting.waiter.grant);
                        budget
                            .hard_wait
                            .set(&mut waiting.wait, bits, diagnostics_clock_ns());
                        self.checkpoint_pressure_for(&budget, &waiting.waiter.grant)
                    };
                    if let Some(pressure) = pressure {
                        // No Admission/identity/key/owner/publication lock
                        // survives into checkpoint request or the capacity park.
                        checkpoint(pressure)?;
                    }
                    thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
            }
        }
    }

    pub(in crate::distributed::remote_db) fn wait_until(
        &self,
        deadline: Instant,
        done: impl Fn() -> bool,
        healthy: impl Fn() -> Result<(), LocalDbError>,
    ) -> Result<(), LocalDbError> {
        let mut budget = self.budget.lock();
        loop {
            if done() {
                return Ok(());
            }
            healthy()?;
            if Instant::now() >= deadline {
                return Err(LocalDbError::new("write shutdown deadline expired"));
            }
            self.drained.wait_until(&mut budget, deadline);
        }
    }

    pub(in crate::distributed::remote_db) fn notify_lifecycle(&self) {
        let _budget = self.budget.lock();
        self.drained.notify_all();
    }

    fn closed_error(&self) -> LocalDbError {
        LocalDbError::new(failure_context(
            &self.first_failure,
            "write authority closed",
        ))
    }

    fn release(&self, grant: &WriteGrant, committed: bool) {
        let mut budget = self.budget.lock();
        Self::uncharge(&mut budget, grant, committed);
        let granted = self.grant_waiters(&mut budget);
        drop(budget);
        wake_granted(granted);
    }

    pub(super) fn close(&self) {
        let mut budget = self.budget.lock();
        budget.closed = true;
        self.drained.notify_all();
        let waiters = std::mem::take(&mut budget.waiters);
        drop(budget);
        for waiter in waiters {
            // Release/Acquire also covers shutdown before the caller parks.
            waiter.state.store(WAITER_CLOSED, Ordering::Release);
            waiter.thread.unpark();
        }
    }

    /// A finished checkpoint may release too little for the FIFO head, which
    /// then stays parked with no publication or grant left to wake it. Unpark
    /// only the head (FIFO: later waiters cannot be granted before it); it
    /// rechecks its pressure and requests the next checkpoint with no lock held.
    pub(super) fn wake_checkpoint_waiter(&self) {
        let head = self.budget.lock().waiters.front().cloned();
        if let Some(waiter) = head {
            waiter.thread.unpark();
        }
    }
}

/// Setup evidence from one owner-monitor connection.
#[cfg(feature = "experimental-fabric")]
pub struct FabricProviderEvidence {
    pub partition: u32,
    pub provider: Option<tidehunter_fabric::ProviderInfo>,
    pub open_profile: Option<tidehunter_fabric::OpenProfile>,
}

type LiveTargetReports = Vec<(u32, super::super::nvmeof::LiveTargetStats)>;

/// Evidence collected only after the final sealed authority and initiator drain.
pub struct LiveFinalShutdownEvidence {
    pub metrics: super::super::nvmeof::live_metrics::LiveNvmeStats,
    pub targets: LiveTargetReports,
}

/// Shared read facade over the caller-allocated, owner-published write engine.
/// A write acknowledgement includes the configured durability contract and
/// atomic publication after every native-index predecessor.
pub struct ConcurrentRemoteDatabase {
    database: Arc<RemoteDatabase>,
    admission: Arc<Admission>,
    pub(super) table: Arc<HybridTable>,
    pub(super) reader: Arc<ValueReader>,
    pub(super) reads: Arc<ReadService>,
    pub(super) cache: Arc<ReadCache>,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    live_metrics: Option<Arc<super::super::nvmeof::live_metrics::LiveNvmeMetrics>>,
    pub(crate) write_timeline: Arc<WriteTimeline>,
    pub(super) shape: KeyShape,
    pub(super) config: RemoteDatabaseConfig,
    pub(super) epoch: u64,
    pub(super) partition_indexes: HashMap<u32, usize>,
    pub(super) read_resolve_ns: AtomicU64,
    pub(super) value_read_ns: AtomicU64,
    io_clients: Vec<(u32, ControlHandle)>,
}

fn owner_timeline_error(
    owner: u32,
    phase: &'static str,
    error: LocalDbError,
) -> super::super::write_timeline::OwnerTimelineError {
    let mut message = error.to_string();
    let truncated = message.len() > 128;
    if truncated {
        let mut end = 128;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    super::super::write_timeline::OwnerTimelineError {
        owner,
        phase,
        message,
        truncated,
    }
}

impl ConcurrentRemoteDatabase {
    /// Owner persistence counters summed over owners, for benchmark boundaries.
    pub fn storage_stats(
        &self,
    ) -> Result<crate::distributed::storage::PartitionStats, LocalDbError> {
        self.database.storage_stats()
    }

    /// The provisioned database identity of the wrapped authority.
    pub fn database_id(&self) -> [u8; 16] {
        self.database.database_id()
    }

    /// Owner persistence counters of each owner, in endpoint order.
    pub fn storage_stats_by_partition(
        &self,
    ) -> Result<Vec<(u32, crate::distributed::storage::PartitionStats)>, LocalDbError> {
        self.database.storage_stats_by_partition()
    }

    /// Upper mutation version of the published cut (the logical frontier);
    /// `None` before the first publication. Benchmark observation only.
    pub fn published_frontier(
        &self,
    ) -> Result<Option<crate::distributed::types::MutationVersion>, LocalDbError> {
        self.ensure_healthy()?;
        Ok(self.database.core.publisher.lock().latest)
    }

    pub fn live_nvmeof_stats(&self) -> Option<super::super::nvmeof::live_metrics::LiveNvmeStats> {
        self.live_metrics.as_ref().map(|metrics| metrics.snapshot())
    }

    pub(crate) fn arm_write_timeline(
        &self,
        limit: usize,
        sample_every: usize,
    ) -> Result<(), LocalDbError> {
        self.write_timeline
            .arm(limit, sample_every)
            .map_err(LocalDbError::new)?;
        if limit == 0 {
            return Ok(());
        }
        for (_, client) in &self.io_clients {
            if let Err(error) = client.owner_timeline_arm(limit) {
                let mut failures = Vec::new();
                for (partition, client) in &self.io_clients {
                    if let Err(cleanup) = client.control(|client| client.owner_timeline_arm(0)) {
                        failures.push(format!("owner {partition}: {cleanup}"));
                    }
                }
                let _ = self.write_timeline.finish();
                return Err(LocalDbError::new(format!(
                    "owner timeline arm failed: {error}; disarm failures: {}",
                    failures.join(", ")
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn finish_write_timeline(
        &self,
    ) -> Result<super::super::write_timeline::WriteTimelineExport, LocalDbError> {
        if !self.write_timeline.enabled() {
            return Err(LocalDbError::new("write timeline is not armed"));
        }
        let mut records = Vec::new();
        let mut dropped = 0u64;
        let mut incomplete = 0u64;
        let mut captured = 0;
        let mut errors = Vec::new();
        for (partition, client) in &self.io_clients {
            match client.control(|client| Ok(client.owner_timeline_fetch_partial())) {
                Ok((mut page, owner_dropped, owner_incomplete, fetch_error)) => {
                    for record in &mut page {
                        record.owner = *partition;
                    }
                    records.extend(page);
                    dropped = dropped.saturating_add(owner_dropped);
                    incomplete = incomplete.saturating_add(owner_incomplete);
                    if let Some(error) = fetch_error {
                        errors.push(owner_timeline_error(
                            *partition,
                            "fetch",
                            LocalDbError::component(error),
                        ));
                    } else {
                        captured += 1;
                    }
                }
                Err(error) => errors.push(owner_timeline_error(*partition, "fetch", error)),
            }
        }
        for (partition, client) in &self.io_clients {
            if let Err(error) = client.control(|client| client.owner_timeline_arm(0)) {
                errors.push(owner_timeline_error(*partition, "disarm", error));
            }
        }
        self.write_timeline.set_owner_records(
            records,
            dropped,
            incomplete,
            self.io_clients.len(),
            captured,
            errors,
        );
        self.write_timeline.finish().map_err(LocalDbError::new)
    }
    pub(crate) fn finish_write_timeline_until(
        &self,
        deadline: Instant,
    ) -> Result<super::super::write_timeline::WriteTimelineExport, LocalDbError> {
        if !self.write_timeline.enabled() {
            return Err(LocalDbError::new("write timeline is not armed"));
        }
        let mut records = Vec::new();
        let mut dropped = 0u64;
        let mut incomplete = 0u64;
        let mut captured = 0;
        let mut errors = Vec::new();
        for (partition, client) in &self.io_clients {
            let mut offset = 0usize;
            let mut owner_dropped = 0u64;
            let mut owner_incomplete = 0u64;
            loop {
                match client.control_until(deadline, move |client| {
                    client.owner_timeline_page_until(offset, deadline)
                }) {
                    Ok((mut page, page_dropped, page_incomplete, more)) => {
                        for record in &mut page {
                            record.owner = *partition;
                        }
                        offset += page.len();
                        records.extend(page);
                        owner_dropped = page_dropped;
                        owner_incomplete = page_incomplete;
                        if !more {
                            captured += 1;
                            break;
                        }
                    }
                    Err(error) => {
                        errors.push(owner_timeline_error(*partition, "fetch", error));
                        break;
                    }
                }
            }
            dropped = dropped.saturating_add(owner_dropped);
            incomplete = incomplete.saturating_add(owner_incomplete);
        }
        for (partition, client) in &self.io_clients {
            if let Err(error) = client.control_until(deadline, move |client| {
                client.owner_timeline_arm_until(0, deadline)
            }) {
                errors.push(owner_timeline_error(*partition, "disarm", error));
            }
        }
        self.write_timeline
            .finish_with_owner_until(
                deadline,
                super::super::write_timeline::OwnerTimelineCollection {
                    records,
                    dropped,
                    incomplete,
                    expected: self.io_clients.len(),
                    captured,
                    errors,
                },
            )
            .map_err(LocalDbError::new)
    }
    /// Setup-time evidence for the existing owner-monitor connections only.
    #[cfg(feature = "experimental-fabric")]
    pub fn fabric_provider_evidence(&self) -> Vec<FabricProviderEvidence> {
        self.io_clients
            .iter()
            .map(|(partition, client)| {
                let client = client.client.lock();
                FabricProviderEvidence {
                    partition: *partition,
                    provider: client.provider_info().cloned(),
                    open_profile: client.connect_profile().fabric_open,
                }
            })
            .collect()
    }

    pub fn register_nvmeof_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), LocalDbError> {
        if let Some(metrics) = &self.live_metrics {
            super::super::nvmeof::live_prometheus::register(registry, metrics.clone())
                .map_err(LocalDbError::component)?;
        }
        Ok(())
    }

    /// Share the existing engine after resolving the caller's earlier submissions.
    pub fn new(db: RemoteDatabase) -> Result<Self, LocalDbError> {
        db.ensure_healthy()?;
        db.drain()?;
        let database = Arc::new(db);
        let core = &database.core;
        let this = Self {
            admission: Arc::clone(&core.admission),
            table: Arc::clone(&core.table),
            reader: Arc::clone(&database.reader),
            reads: Arc::clone(&core.reads),
            cache: Arc::clone(&core.read_cache),
            write_profile: Arc::clone(&core.write_profile),
            live_metrics: core.live_metrics.clone(),
            write_timeline: Arc::clone(&core.write_timeline),
            shape: core.shape.clone(),
            config: core.config.clone(),
            epoch: core.epoch,
            partition_indexes: core.partition_indexes.clone(),
            read_resolve_ns: AtomicU64::new(0),
            value_read_ns: AtomicU64::new(0),
            io_clients: core
                .config
                .endpoints
                .iter()
                .zip(&core.clients)
                .map(|(endpoint, worker)| (endpoint.partition, worker.control_handle()))
                .collect(),
            database: Arc::clone(&database),
        };
        // Native cadence: a root per `snapshot_written_bytes` of published value
        // bytes or per hour, never on a put or a get. The thread holds a `Weak`
        // and ends with the database or at the first failed pass, which poisons
        // the engine (native's fail-stop `expect`); dropping the handle detaches
        // it.
        drop(
            spawn_snapshot_thread(
                Arc::downgrade(&database),
                SnapshotConfig::native(this.table.config().snapshot_written_bytes),
            )
            .map_err(|error| LocalDbError::new(format!("spawn snapshot thread: {error}")))?,
        );
        Ok(this)
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        self.database.core.ensure_healthy()
    }

    /// The shared write core, for maintenance passes in sibling modules.
    pub(super) fn write_core(&self) -> &Arc<super::write_core::core::WriteCore> {
        &self.database.core
    }

    pub(super) fn poison(&self, cause: &LocalDbError) {
        // One fail-stop path: records the exact cause, closes Admission,
        // resolves every unresolved slot as unknown and wakes owner workers.
        self.database.poison(cause);
    }

    /// # Errors
    /// Rejects reads after an observed authority, transport, or shutdown failure.
    pub fn validate_cached_read(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
    }

    /// The coordinator's native index, for the iterator and snapshot paths.
    pub(in crate::distributed) fn hybrid_table(&self) -> &HybridTable {
        &self.table
    }

    /// The value path: a position names an owner member frame; the reader
    /// fetches it, verifies the native CRC and decodes the entry.
    pub(crate) fn value_reader(&self) -> &ValueReader {
        &self.reader
    }

    /// The publication watermark: every batch below it is visible.
    ///
    /// # Errors
    /// Rejects reads after an observed authority, transport, or shutdown failure.
    pub fn published_cut(&self) -> Result<BatchVersion, LocalDbError> {
        self.ensure_healthy()?;
        let cut = self.table.current();
        self.ensure_healthy()?;
        Ok(cut)
    }

    pub fn metadata_history_bytes(&self) -> usize {
        self.database.core.metadata_history_bytes()
    }

    /// Submit one complete batch and wait for synced, ordered publication.
    ///
    /// # Errors
    /// Returns validation, admission-budget, storage, publication, or owner errors.
    /// An uncertain version requires recovery, not an assumed failed append.
    pub fn write_batch(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.write_batch_inner(operation, mutations, None)
    }

    pub(crate) fn write_batch_traced(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.write_batch_inner(operation, mutations, sample)
    }

    fn write_batch_inner(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.database.core.write(operation, mutations, sample)
    }

    /// Commit one singleton `DropCells` batch over the inclusive reduced-cell
    /// range and wait for its ordered publication. One WAL batch whatever the
    /// number of keys or cells; the caller validates the endpoints first.
    ///
    /// # Errors
    /// Returns validation, capacity, storage or publication errors. After an
    /// unknown owner completion the error carries the uncertain version.
    pub(crate) fn drop_cells(
        &self,
        operation: [u8; 16],
        keyspace: crate::key_shape::KeySpace,
        first_cell: crate::cell::CellId,
        last_cell: crate::cell::CellId,
    ) -> Result<BatchVersion, LocalDbError> {
        self.database.core.write_drop_cells(
            operation,
            Mutation::DropCells {
                keyspace,
                first_cell,
                last_cell,
            },
        )
    }

    /// Native `relocation_batch_max_bytes`, bounded by the admission byte budget
    /// so one chunk of copies never exceeds what the coordinator may hold.
    pub(crate) fn relocation_batch_max_bytes(&self) -> usize {
        self.table
            .config()
            .to_native_config()
            .relocation_batch_max_bytes()
            .min(self.config.max_pending_bytes)
    }

    /// Writes one chunk of relocation copies to the serving generation and
    /// returns where each landed, in input order.
    ///
    /// Each copy is a `Put` of the source member's own bytes (the key, the value
    /// and the native record encoding are unchanged, so the payload CRC the
    /// index stores still verifies the copy). The chunk is split into as many
    /// format-5 batches as the batch limits require. A relocated batch takes an
    /// allocator version and publishes in order but inserts nothing into the
    /// index and is skipped by replay, so a copy can never override a later
    /// write of its key. The caller must wait for the owners to make the copies
    /// durable ([`WriteCore::await_copies_durable`]) before it re-points the
    /// index at them: a native-tier acknowledgement is only the owner's page
    /// cache.
    ///
    /// # Errors
    /// A copy that cannot fit one batch, or any storage, publication or owner
    /// error; the copies already written stay unreferenced in the log.
    pub(crate) fn write_relocated_batch(
        &self,
        keyspace: KeySpace,
        cell: &crate::cell::CellId,
        copies: &[(Bytes, Bytes, BatchVersion)],
    ) -> Result<RelocatedCopies, LocalDbError> {
        let limits = self.config.storage.batch_limits;
        let mutations: Vec<Mutation> = copies
            .iter()
            .map(|(key, value, _)| Mutation::Put {
                keyspace,
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let mut members = Vec::with_capacity(mutations.len());
        let mut last_version: Option<BatchVersion> = None;
        let mut start = 0;
        while start < mutations.len() {
            let take = CompleteBatch::fitting_prefix_len(&mutations[start..], limits);
            if take == 0 {
                return Err(LocalDbError::new(format!(
                    "a relocation copy in cell {cell:?} exceeds the batch limits"
                )));
            }
            let (written, version) =
                self.write_relocated_chunk(mutations[start..start + take].to_vec())?;
            members.extend(written);
            last_version =
                Some(last_version.map_or(version, |last: BatchVersion| last.max(version)));
            start += take;
        }
        let last_version = last_version
            .ok_or_else(|| LocalDbError::new("a relocation copy chunk wrote no batch"))?;
        Ok(RelocatedCopies {
            members,
            last_version,
        })
    }

    fn write_relocated_chunk(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<(Vec<RelocatedMember>, BatchVersion), LocalDbError> {
        let operation: [u8; 16] = rand::random();
        let (version, address) = self
            .database
            .core
            .write_relocated(operation, mutations.clone())?;
        // The batch is deterministic in (id, version, members): encoding it again
        // yields the frame the owner stored, from which every member address and
        // payload CRC follows exactly as publication derives them.
        let id = BatchId {
            database: self.config.storage.database,
            operation,
        };
        let batch = CompleteBatch::new_relocated(
            &self.shape,
            id,
            version,
            mutations,
            self.config.storage.batch_limits,
        )
        .map_err(LocalDbError::component)?;
        let members = member_positions(&batch, address)
            .map_err(LocalDbError::component)?
            .into_iter()
            .map(|position| RelocatedMember {
                address: position.address,
                payload_crc: position.payload_crc,
            })
            .collect();
        Ok((members, version))
    }

    /// One native snapshot pass: native `rebuild_control_region`. Flushes the
    /// cells that are stale by value-byte distance, stores one region when the
    /// flush frontier advanced and is a no-op otherwise (`stored == false`).
    ///
    /// # Errors
    /// Returns an observed authority failure, an owner drain failure or an
    /// index/control-region persistence error.
    pub(crate) fn snapshot_index(&self) -> Result<StoredSnapshot, LocalDbError> {
        self.ensure_healthy()?;
        self.table.snapshot()
    }

    /// Snapshot passes that were a no-op only because a reader latch held the
    /// flush frontier below the stored head. Diagnostic, exported as the
    /// `snapshot_latch_deferrals` gauge.
    pub(crate) fn snapshot_latch_deferrals(&self) -> u64 {
        self.table.snapshot_latch_deferrals()
    }

    /// Named index rebuild: native `force_rebuild_control_region`. Force-flushes
    /// every dirty cell at the current flush frontier and stores one region, even
    /// when the head did not move. Takes no write gate; later writes proceed.
    ///
    /// # Errors
    /// As [`Self::snapshot_index`].
    pub(crate) fn force_rebuild_index(&self) -> Result<StoredSnapshot, LocalDbError> {
        self.ensure_healthy()?;
        let frontier = self.table.last_processed();
        self.table.snapshot_for_relocation(frontier)
    }

    pub fn read_cache_stats(&self) -> ReadCacheStats {
        self.cache.stats()
    }
    pub fn read_cache_shard_stats(&self) -> super::ReadCacheShardStats {
        self.cache.shard_stats()
    }
    pub fn per_keyspace_read_cache_stats(
        &self,
    ) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.cache.per_keyspace_stats()
    }
    pub fn stats(&self) -> LocalDbStats {
        let mut stats = self.database.core.database_stats();
        stats.read_resolve_ns += self.read_resolve_ns.load(Ordering::Relaxed);
        stats.read_resolve_ns_available = self.read_profile_enabled();
        stats.value_read_ns += self.value_read_ns.load(Ordering::Relaxed);
        stats
    }
    pub fn pipeline_stats(&self) -> PipelineStats {
        self.database.core.pipeline_stats()
    }

    /// Visit one local-only snapshot of write owner, checkpoint, and cache metrics.
    /// This performs no diagnostic RPC and does not enable sampled profiling.
    pub fn visit_write_metrics(&self, visit: impl FnOnce(WriteMetricsSnapshot)) {
        let checkpoint_diagnostics = self
            .database
            .core
            .publisher
            .lock()
            .checkpoint_diagnostic_summary;
        let write_profile = *self.write_profile.lock();
        visit(WriteMetricsSnapshot {
            authority: self.database.core.authority_write_stats(),
            pipeline: self.pipeline_stats(),
            read_cache: self.cache.stats(),
            write_profile,
            checkpoint_diagnostics,
        });
    }
    /// Bounded metrics snapshot; lock contention makes the whole snapshot unavailable.
    pub fn try_visit_write_metrics(&self, visit: impl FnOnce(WriteMetricsSnapshot)) -> Option<()> {
        // Copy each domain separately; metrics must not introduce a reverse
        // stats/Admission→publisher lock order.
        let stored = *self.database.core.stats.try_lock()?;
        let diagnostics = self
            .database
            .core
            .publisher
            .try_lock()?
            .checkpoint_diagnostic_summary;
        let (requests, bytes) = {
            let budget = self.admission.budget.try_lock()?;
            (budget.peak_requests, budget.peak_bytes)
        };
        let profile = *self.write_profile.try_lock()?;
        let mut pipeline = stored.pipeline;
        pipeline.concurrent_pending_peak = requests;
        pipeline.concurrent_queue_bytes_peak = bytes;
        pipeline.concurrent_queued_peak = requests;
        pipeline.append_requests = self
            .database
            .core
            .clients
            .iter()
            .map(|worker| worker.accounting.formed.load(Ordering::Relaxed))
            .sum();
        let snapshot = WriteMetricsSnapshot {
            authority: AuthorityWriteStats {
                storage_wait_ns: stored.database.storage_wait_ns,
                publication_ns: stored.database.publication_ns,
                dispatch_ns: stored.database.dispatch_ns,
                ..stored.write
            },
            pipeline,
            read_cache: self.cache.stats(),
            write_profile: profile,
            checkpoint_diagnostics: diagnostics,
        };
        visit(snapshot);
        Some(())
    }
    pub fn try_metadata_history_bytes(&self) -> Option<usize> {
        self.database.core.try_metadata_history_bytes()
    }
    pub fn try_read_cache_stats(&self) -> Option<ReadCacheStats> {
        Some(self.cache.stats())
    }
    pub fn try_stats(&self) -> Option<LocalDbStats> {
        let core = &self.database.core;
        let mut stats = core.stats.try_lock()?.database;
        // Relaxed diagnostic counters; see WriteCore::database_stats.
        stats.read_resolve_ns += core.read_resolve_ns.load(Ordering::Relaxed)
            + self.read_resolve_ns.load(Ordering::Relaxed);
        stats.read_resolve_ns_available = self.read_profile_enabled();
        stats.value_read_ns +=
            core.value_read_ns.load(Ordering::Relaxed) + self.value_read_ns.load(Ordering::Relaxed);
        Some(stats)
    }
    pub fn try_per_keyspace_read_cache_stats(
        &self,
    ) -> Option<[PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT]> {
        Some(self.cache.per_keyspace_stats())
    }
    /// Explicit flush: snapshot the next sequence, wait until every earlier
    /// allocation is published (or fail-stop), then run each owner's control
    /// barrier, which flushes every log's frontier before replying. Owners run
    /// in parallel, bounded by available parallelism. A `Synchronous` owner has
    /// nothing unsynced, so the barrier costs one round trip.
    ///
    /// # Errors
    /// Returns the first owner failure; owners that already flushed stay flushed.
    pub fn flush_durable(&self) -> Result<(), LocalDbError> {
        self.database.core.flush_durable()
    }

    /// Engine recovery evidence of this open.
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.database.recovery_stats()
    }
    /// Durable-cut progress of this epoch; `enabled` is false when untracked.
    pub fn durable_cut_stats(&self) -> super::DurableCutStats {
        self.database.core.durable_cut_stats()
    }
    /// Request advisory mapped-WAL eviction on every owner at a quiescent boundary.
    /// The owner control handler visits all retained source epochs, not just the
    /// current writer epoch. Callers must exclude concurrent reads and writes.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub fn evict_mapped_wal_for_benchmark(
        &self,
    ) -> Result<Vec<(u32, crate::read_probe::ReadEvidence)>, LocalDbError> {
        self.ensure_healthy()?;
        self.reads.ensure_idle()?;
        // The reply can wake its caller just before the owner drops its credit.
        // Wait for that bounded retirement rather than spuriously failing.
        let deadline = Instant::now() + self.config.timeout;
        let mut budget = self.admission.budget.lock();
        while budget.requests != 0 {
            let now = Instant::now();
            if now >= deadline {
                return Err(LocalDbError::new(
                    "writes did not drain before mapped-WAL eviction",
                ));
            }
            self.admission.drained.wait_for(&mut budget, deadline - now);
        }
        drop(budget);
        if self.config.read_cache_bytes != 0 {
            return Err(LocalDbError::new(
                "mapped-WAL eviction requires disabled coordinator read cache",
            ));
        }
        let epoch = self.epoch;
        let result = self
            .io_clients
            .iter()
            .map(|(partition, client)| {
                client
                    .control(move |client| client.read_evidence_for_benchmark(epoch, true))
                    .map(|evidence| (*partition, evidence))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.ensure_healthy()?;
        Ok(result)
    }
    pub fn read_admission_generation(&self) -> u64 {
        self.reads.admission_generation()
    }
    /// Wait for read credit to change after `observed`. Ordinary reads pass
    /// `None`: like a native read, load alone never fails them (see
    /// `ReadService::wait_admission`).
    ///
    /// # Errors
    /// Returns an expired explicit deadline, failure, or closed-owner error.
    pub fn wait_read_admission(
        &self,
        observed: u64,
        deadline: Option<Instant>,
    ) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        self.reads.wait_admission(observed, deadline)?;
        self.ensure_healthy()
    }
    /// # Errors
    /// Returns write-owner, transport, or shutdown failures after joining progress.
    pub fn shutdown(mut self) -> Result<(), LocalDbError> {
        self.stop()
    }

    /// Final campaign teardown, distinct from ordinary close/reopen.
    /// Returns only owner-reported target evidence after successful drain and seal.
    pub fn shutdown_live_target_final(mut self) -> Result<LiveFinalShutdownEvidence, LocalDbError> {
        let metrics = self
            .live_metrics
            .take()
            .ok_or_else(|| LocalDbError::new("live NVMe is not configured"))?;
        let targets = self.database.core.shutdown_live_target_final()?;
        Ok(LiveFinalShutdownEvidence {
            metrics: metrics.snapshot(),
            targets,
        })
    }
    /// Arm a fresh bounded profile after warm-up has drained.
    pub fn arm_write_profile(&self, limit: usize) -> Result<(), LocalDbError> {
        self.database.core.arm_write_profile(limit)
    }
    /// Drain admitted writes and finalize any partial opt-in storage write profiles.
    /// This is the only call apart from the configured sample boundary that may
    /// perform profile-control RPCs; the metrics visitor remains local-only.
    pub fn finish_write_profile(&self) -> Result<(), LocalDbError> {
        self.database.core.finish_write_profile()
    }
    fn stop(&mut self) -> Result<(), LocalDbError> {
        self.database.core.shutdown()
    }
}
impl Drop for ConcurrentRemoteDatabase {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests;

/// Where one relocation copy landed: the member frame address and the CRC-32 of
/// its payload, which the pass compares with the indexed CRC of the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RelocatedMember {
    pub(crate) address: super::super::types::PhysicalValueAddress,
    pub(crate) payload_crc: u32,
}

/// The copies of one chunk, as written: where each landed (input order) and
/// the highest version a copy batch took. A relocated batch inserts nothing
/// into the index, so that version is what the durable cut has to cover before
/// the caller re-points at the copies.
#[derive(Clone, Debug)]
pub(crate) struct RelocatedCopies {
    pub(crate) members: Vec<RelocatedMember>,
    pub(crate) last_version: BatchVersion,
}
