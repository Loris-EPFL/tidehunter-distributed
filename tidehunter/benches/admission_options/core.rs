//! M2 ordering-only prototypes; no durability or engine throughput claim.
//! Run the auditable matrix with `cargo bench -p tidehunter --bench admission_options`.
//! The same core backs the standalone `admission_options` example; see --help.
//! CLI parameters select matrix cells and CSV/JSONL outputs. Criterion is opt-in.
//! Enable experimental-distributed,test-utils for a separate real codec control.
use criterion::{Criterion, black_box};
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fs::{self, File};
use std::future::{Future, poll_fn};
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Barrier, Notify, mpsc};
#[path = "cli.rs"]
mod cli;

const KEYS: u32 = 1_000_000;
const SHARDS: usize = 256;
const TRACE: usize = 65_536;
// Independent caller bounds: dividing a global quota by caller count biases
// heterogeneous/oversubscribed CPUs and can truncate a fast caller's window.
const MAX_SAMPLES: usize = 8_000_000;
const WAIT_LIMIT: usize = 10_000_000;
type Owners = SmallVec<[(usize, u64); 16]>;
const SPIN_LIMIT: usize = 64;

#[derive(Debug, Default)]
struct SchedulingStats {
    spin_checks: u64,
    parks: u64,
    wakeups: u64,
    spurious_wakeups: u64,
}

/// Every option uses bounded spin then task parking. Register BEFORE the final
/// predicate check, so completion cannot lose a wakeup. Parking releases the
/// worker to another ready logical request; no per-put task is spawned.
async fn wait_until(
    changed: &Notify,
    stats: &mut SchedulingStats,
    mut ready: impl FnMut() -> bool,
) -> bool {
    if ready() {
        return true;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    for attempt in 0..WAIT_LIMIT {
        if attempt < SPIN_LIMIT {
            stats.spin_checks += 1;
            std::hint::spin_loop();
            if ready() {
                return true;
            }
            continue;
        }
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if ready() {
            return true;
        }
        let mut parked = false;
        let parked_wait = poll_fn(|cx| {
            let state = notified.as_mut().poll(cx);
            if state.is_pending() && !parked {
                stats.parks += 1;
                parked = true;
            }
            state
        });
        if tokio::time::timeout_at(deadline, parked_wait)
            .await
            .is_err()
        {
            return false;
        }
        stats.wakeups += 1;
        if ready() {
            return true;
        }
        stats.spurious_wakeups += 1;
    }
    false
}

struct Completion {
    identity: AtomicU64,
    changed: Notify,
}
impl Completion {
    fn new() -> Self {
        Self {
            identity: AtomicU64::new(u64::MAX),
            changed: Notify::new(),
        }
    }
    fn complete(&self, identity: u64) {
        self.identity.store(identity, Ordering::Release);
        self.changed.notify_waiters();
    }
    async fn wait(&self, identity: u64, stats: &mut SchedulingStats) {
        if !wait_until(&self.changed, stats, || {
            self.identity.load(Ordering::Acquire) == identity
        })
        .await
        {
            eprintln!("completion deadline: identity={identity}");
            std::process::exit(1);
        }
    }
}

struct ClientSignals {
    published: Completion,
    decision: Completion,
    active: AtomicU64,
}
impl ClientSignals {
    fn new() -> Self {
        Self {
            published: Completion::new(),
            decision: Completion::new(),
            active: AtomicU64::new(u64::MAX),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OptionKind {
    A,
    B,
    C,
    D,
}
impl OptionKind {
    fn name(self) -> &'static str {
        match self {
            Self::A => "A-model",
            Self::B => "B",
            Self::C => "C",
            Self::D => "D",
        }
    }
}
#[derive(Clone)]
struct Batch {
    keys: SmallVec<[u32; 64]>,
    shards: SmallVec<[usize; 64]>,
    participants: SmallVec<[usize; 16]>,
    key_shard_positions: SmallVec<[u8; 64]>,
    key_owner_positions: SmallVec<[u8; 64]>,
    owner: usize,
}
#[derive(Clone, Copy)]
struct Case {
    option: OptionKind,
    callers: usize,
    owners: usize,
    batch: usize,
    zipf: bool,
    fraction: u32,
}
impl Case {
    fn name(self) -> String {
        format!(
            "{}/c{}/o{}/k{}/{}/f{}",
            self.option.name(),
            self.callers,
            self.owners,
            self.batch,
            if self.zipf { "zipf099" } else { "uniform" },
            self.fraction
        )
    }
}
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
fn owner(key: u32, owners: usize) -> usize {
    mix(u64::from(key)) as usize & (owners - 1)
}
fn shard(key: u32) -> usize {
    (mix(u64::from(key)) >> 16) as usize & (SHARDS - 1)
}
struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / ((1u64 << 53) as f64)
    }
}
fn trace(case: Case, cdf: &[f64]) -> Vec<Batch> {
    let mut rng = Random(0x4d32_2026);
    (0..TRACE)
        .map(|_| {
            let first = if case.zipf {
                let target = rng.unit();
                cdf.partition_point(|p| *p < target) as u32
            } else {
                (rng.next() % u64::from(KEYS)) as u32
            };
            // Draw shape independently of keys. Conditional sampling preserves the named
            // distribution within each owner, but not the unconstrained iid batch law.
            let home = owner(first, case.owners);
            let multi = case.batch > 1
                && case.owners > 1
                && mix(u64::from(first) + rng.next()) % 100 < u64::from(case.fraction);
            let mut keys = SmallVec::new();
            keys.push(first);
            for index in 1..case.batch {
                let mut found = None;
                for _ in 0..100_000 {
                    let key = if case.zipf {
                        let target = rng.unit();
                        cdf.partition_point(|p| *p < target) as u32
                    } else {
                        (rng.next() % u64::from(KEYS)) as u32
                    };
                    let other = owner(key, case.owners) != home;
                    if ((multi && (index != 1 || other)) || (!multi && !other))
                        && !keys.contains(&key)
                    {
                        found = Some(key);
                        break;
                    }
                }
                keys.push(found.expect("bounded conditional key sampling"));
            }
            // Match M4's distinct-key batches; draws reject within-batch duplicates.
            keys.sort_unstable();
            let mut shards: SmallVec<[usize; 64]> = keys.iter().map(|k| shard(*k)).collect();
            shards.sort_unstable();
            shards.dedup();
            let mut participants: SmallVec<[usize; 16]> =
                keys.iter().map(|k| owner(*k, case.owners)).collect();
            participants.sort_unstable();
            participants.dedup();
            // Equivalent to engine PreparedAdmission routing: keep key hashing
            // and lock-set searches outside the ordering-core measurement.
            let key_shard_positions = keys
                .iter()
                .map(|key| shards.binary_search(&shard(*key)).expect("key shard") as u8)
                .collect();
            let key_owner_positions = keys
                .iter()
                .map(|key| {
                    participants
                        .binary_search(&owner(*key, case.owners))
                        .expect("key owner") as u8
                })
                .collect();
            Batch {
                keys,
                shards,
                participants,
                key_shard_positions,
                key_owner_positions,
                owner: (rng.next() as usize) & (case.owners - 1),
            }
        })
        .collect()
}

/// A bounded generation-tagged ring. Credits cover allocated AND reserved slots.
/// Full policy: bounded spin then park the logical caller until prefix advance.
/// Only resolved success and known-before-send abortion are modeled.
#[repr(align(64))]
struct Tracker {
    next: AtomicU64,
    prefix: AtomicU64,
    credits: AtomicUsize,
    slots: Box<[AtomicU64]>,
    waiting_on: Box<[AtomicU64]>,
    request: Box<[AtomicU64]>,
    credits_changed: Notify,
    slot_changed: Box<[Notify]>,
    frontier_gated: bool,
}
impl Tracker {
    fn new(capacity: usize, frontier_gated: bool) -> Self {
        Self {
            next: AtomicU64::new(1),
            prefix: AtomicU64::new(0),
            credits: AtomicUsize::new(capacity),
            slots: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
            waiting_on: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
            request: (0..capacity).map(|_| AtomicU64::new(u64::MAX)).collect(),
            credits_changed: Notify::new(),
            slot_changed: (0..capacity).map(|_| Notify::new()).collect(),
            frontier_gated,
        }
    }
    fn try_reserve(&self) -> bool {
        // Reuse handoff: old slot completion/metadata writes -> Release credit
        // return -> Acquire reservation -> metadata reset/new slot generation.
        // A failed reservation observes availability only and orders no payload.
        self.credits
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
    }
    fn reserve(&self) {
        assert!(self.try_reserve(), "self-check exceeded ring capacity");
    }
    async fn reserve_async(&self, stats: &mut SchedulingStats) {
        if !wait_until(&self.credits_changed, stats, || self.try_reserve()).await {
            eprintln!("admission credit deadline");
            std::process::exit(1);
        }
    }
    fn allocate(&self, identity: u64) -> u64 {
        // Capacity 2: H reserves early; G retires seq1; X reserves its returned
        // credit and allocates seq2; H then allocates seq3 and reuses seq1's slot.
        // G slot resolution/reset -> credit return (Release) -> X reservation
        // (Acquire) -> X fetch_add (Release) -> H fetch_add (Acquire) -> H reuse.
        // Thus allocation relays retirement even for an old held reservation.
        // Locks order key registration; slot Release publishes new metadata.
        let seq = self.next.fetch_add(1, Ordering::AcqRel);
        assert!(seq < u64::MAX >> 2);
        // Slot Release publishes the matching diagnostic identity as well.
        self.request[seq as usize % self.slots.len()].store(identity, Ordering::Release);
        // Reset edge metadata before the pending-generation Release publication;
        // a diagnostic must never attribute a previous ring generation's edge.
        self.waiting_on[seq as usize % self.slots.len()].store(0, Ordering::Release);
        self.slots[seq as usize % self.slots.len()].store(seq << 2, Ordering::Release);
        seq
    }
    fn complete(&self, seq: u64, aborted: bool) {
        assert!(!self.covers(seq), "prefix passed an unresolved slot");
        assert_eq!(
            self.slots[seq as usize % self.slots.len()].load(Ordering::Acquire),
            seq << 2,
            "completion must own a pending generation"
        );
        // Dekker-style invariant: this completer either sees a later slot done,
        // or its completer sees our advanced prefix. Both store->load pairs
        // participate in the same SeqCst order; Release/Acquire permits both
        // sides to miss and permanently strand a fully resolved prefix.
        self.slots[seq as usize % self.slots.len()]
            .store((seq << 2) | if aborted { 3 } else { 2 }, Ordering::SeqCst);
        let advanced = self.advance();
        self.slot_changed[seq as usize % self.slots.len()].notify_waiters();
        if advanced {
            self.credits_changed.notify_waiters();
        }
    }
    fn advance(&self) -> bool {
        let mut advanced = false;
        for _ in 0..self.slots.len() {
            let prior = self.prefix.load(Ordering::SeqCst);
            let seq = prior + 1;
            let state = self.slots[seq as usize % self.slots.len()].load(Ordering::SeqCst);
            if state >> 2 != seq || state & 2 == 0 {
                return advanced;
            }
            // Only a resolved slot may move the prefix. Generation prevents ABA
            // on ring wrap; the winning CAS returns precisely one credit.
            if self
                .prefix
                .compare_exchange(prior, seq, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Publish retirement of this old generation before reuse.
                self.credits.fetch_add(1, Ordering::Release);
                advanced = true;
                if self.frontier_gated {
                    // B waiters need frontier entry, not merely local completion.
                    self.slot_changed[seq as usize % self.slots.len()].notify_waiters();
                }
            }
        }
        advanced
    }
    fn covers(&self, seq: u64) -> bool {
        self.prefix.load(Ordering::Acquire) >= seq
    }
    fn published(&self, seq: u64) -> bool {
        if self.covers(seq) {
            return true;
        }
        let state = self.slots[seq as usize % self.slots.len()].load(Ordering::Acquire);
        // A newer generation proves this old slot was resolved before reuse.
        state >> 2 > seq || (state >> 2 == seq && state & 2 != 0)
    }
    fn check_drained(&self) {
        assert_eq!(
            self.prefix.load(Ordering::Acquire) + 1,
            self.next.load(Ordering::Relaxed)
        );
        assert_eq!(self.credits.load(Ordering::Relaxed), self.slots.len());
    }
}
/// Identical inline map/dependency value in every option. Publication is queried
/// through generation-tagged tracker slots, never per-key Arc refcount traffic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Version {
    stamp: u64,
    owner: usize,
    sequence: u64,
}
#[derive(Default)]
struct Shard {
    last: HashMap<u32, Version>,
}
#[derive(Default)]
struct OwnerStage {
    sequence: u64,
    timestamp: u64,
    last: HashMap<u32, Version>,
    identities: HashMap<u64, Version>,
}
struct Command {
    identity: u64,
    index: usize,
    client: usize,
}
struct Decision {
    identity: u64,
    client: usize,
}
#[derive(Default)]
struct PutStats {
    dependency_ns: u64,
    cross_frontier_ns: u64,
    cross_frontier_waits: u64,
    hol_ns: u64,
    lags: SmallVec<[(usize, u64); 16]>,
    scheduling: SchedulingStats,
}

struct Admission {
    versions: Owners,
    dependencies: SmallVec<[Version; 64]>,
}

/// Caller/owner locks are always acquired in ascending index; owner stage locks
/// precede key locks. No lock is held while waiting for dependencies or decisions.
struct Parallel {
    option: OptionKind,
    shards: Vec<Mutex<Shard>>,
    identities: Vec<Mutex<HashMap<u64, Version>>>,
    owners: Vec<Mutex<OwnerStage>>,
    trackers: Vec<Tracker>,
    decision: Option<mpsc::Sender<Decision>>,
    case: Case,
    signals: Arc<Vec<ClientSignals>>,
    diagnosed: AtomicBool,
}
impl Parallel {
    fn new(
        case: Case,
        decision: Option<mpsc::Sender<Decision>>,
        signals: Arc<Vec<ClientSignals>>,
    ) -> Self {
        let capacity = (case.callers * 2).max(64).next_power_of_two();
        let core = Self {
            option: case.option,
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        last: HashMap::with_capacity(KEYS as usize / SHARDS),
                    })
                })
                .collect(),
            identities: (0..SHARDS)
                .map(|_| Mutex::new(HashMap::with_capacity(case.callers.max(4))))
                .collect(),
            owners: (0..case.owners)
                .map(|_| {
                    Mutex::new(OwnerStage {
                        last: HashMap::with_capacity(if case.option == OptionKind::C {
                            KEYS as usize / case.owners
                        } else {
                            0
                        }),
                        identities: HashMap::with_capacity(case.callers),
                        ..OwnerStage::default()
                    })
                })
                .collect(),
            trackers: (0..if case.option == OptionKind::D {
                1
            } else {
                case.owners
            })
                .map(|_| Tracker::new(capacity, case.option == OptionKind::B))
                .collect(),
            decision,
            case,
            signals,
            diagnosed: AtomicBool::new(false),
        };
        if case.option == OptionKind::D {
            // Exercise a held slot, out-of-order completion and a known abort on
            // this case's actual ring. This fault prelude is outside measurement.
            let ring = &core.trackers[0];
            ring.reserve();
            let held = ring.allocate(u64::MAX);
            ring.reserve();
            let later = ring.allocate(u64::MAX);
            ring.complete(later, false);
            assert!(!ring.covers(held), "D passed an unresolved slot");
            ring.complete(held, true);
            assert!(ring.covers(later), "D abort did not resolve prefix");
            ring.check_drained();
        }
        core
    }
    // Synchronous registration owns every guard; none can cross an await point.
    fn register(&self, batch: &Batch, identity: u64, participants: &[usize]) -> Admission {
        let mut stages: SmallVec<[_; 16]> = if self.option == OptionKind::D {
            SmallVec::new()
        } else {
            participants
                .iter()
                .map(|o| self.owners[*o].lock())
                .collect()
        };
        let mut guards: SmallVec<[_; 64]> = if self.option == OptionKind::C {
            SmallVec::new()
        } else {
            batch
                .shards
                .iter()
                .map(|s| self.shards[*s].lock())
                .collect()
        };
        let mut dependencies: SmallVec<[Version; 64]> = SmallVec::new();
        for (index, key) in batch.keys.iter().enumerate() {
            let writers = if self.option == OptionKind::C {
                let position = usize::from(batch.key_owner_positions[index]);
                &stages[position].last
            } else {
                let position = usize::from(batch.key_shard_positions[index]);
                &guards[position].last
            };
            if let Some(&prior) = writers.get(key)
                && !dependencies.contains(&prior)
            {
                dependencies.push(prior);
            }
        }
        let stamp = match self.option {
            OptionKind::D => self.trackers[0].allocate(identity),
            OptionKind::B => {
                1 + dependencies
                    .iter()
                    .map(|p| p.stamp)
                    .chain(std::iter::once(stages[0].timestamp))
                    .max()
                    .unwrap_or(0)
            }
            OptionKind::C => 0,
            OptionKind::A => unreachable!(),
        };
        let mut versions = Owners::new();
        if self.option == OptionKind::D {
            versions.push((0, stamp));
        } else {
            for (index, &o) in participants.iter().enumerate() {
                let seq = self.trackers[o].allocate(identity);
                stages[index].sequence += 1;
                assert_eq!(seq, stages[index].sequence, "dense owner append order");
                stages[index].timestamp = stamp;
                versions.push((o, seq));
            }
        }
        let primary = Version {
            stamp,
            owner: versions[0].0,
            sequence: versions[0].1,
        };
        if self.option == OptionKind::C {
            for (stage, &(o, sequence)) in stages.iter_mut().zip(&versions) {
                assert!(
                    stage
                        .identities
                        .insert(
                            identity,
                            Version {
                                stamp,
                                owner: o,
                                sequence
                            }
                        )
                        .is_none()
                );
            }
        } else {
            assert!(
                self.identities[identity as usize & (SHARDS - 1)]
                    .lock()
                    .insert(identity, primary)
                    .is_none()
            );
        }
        for (index, key) in batch.keys.iter().enumerate() {
            let writers = if self.option == OptionKind::C {
                let position = usize::from(batch.key_owner_positions[index]);
                &mut stages[position].last
            } else {
                let position = usize::from(batch.key_shard_positions[index]);
                &mut guards[position].last
            };
            let version = if self.option == OptionKind::C {
                let position = usize::from(batch.key_owner_positions[index]);
                let o = versions[position].0;
                Version {
                    stamp,
                    owner: o,
                    sequence: versions[position].1,
                }
            } else {
                primary
            };
            if let Some(prior) = writers.insert(*key, version) {
                if self.option == OptionKind::C {
                    assert_eq!(prior.owner, version.owner);
                    assert!(
                        prior.sequence < version.sequence,
                        "C per-key registration order"
                    );
                } else {
                    assert!(prior.stamp < version.stamp, "increasing conflict order");
                }
            }
        }
        Admission {
            versions,
            dependencies,
        }
    }
    async fn put(&self, batch: &Batch, identity: u64, client: usize) -> PutStats {
        let single_owner = if self.option == OptionKind::D {
            0
        } else {
            batch.owner
        };
        let participants = match self.option {
            OptionKind::B | OptionKind::D => std::slice::from_ref(&single_owner),
            OptionKind::C => batch.participants.as_slice(),
            OptionKind::A => unreachable!(),
        };
        let mut stats = PutStats::default();
        for &o in participants {
            self.trackers[o].reserve_async(&mut stats.scheduling).await;
        }
        let Admission {
            versions,
            dependencies,
        } = self.register(batch, identity, participants);
        if self.option == OptionKind::C && participants.len() > 1 {
            // Null participants have prepared immediately. This synchronous extra
            // actor hop is a memory-only decision, NOT a durable log or network RTT.
            self.decision
                .as_ref()
                .expect("C decision actor")
                // One outstanding request per logical client and capacity N:
                // full is impossible under the admission bound, and fails hard.
                .try_send(Decision { identity, client })
                .expect("bounded decision channel has capacity");
            self.signals[client]
                .decision
                .wait(identity, &mut stats.scheduling)
                .await;
        }
        let wait_start = Instant::now();
        for dependency in &dependencies {
            let edge = (dependency.sequence << 4) | dependency.owner as u64;
            for &(owner, sequence) in &versions {
                self.trackers[owner].waiting_on
                    [sequence as usize % self.trackers[owner].slots.len()]
                .store(edge, Ordering::Release);
            }
            let dependency_started = Instant::now();
            let mut checks = 0usize;
            let cross = self.option == OptionKind::B && dependency.owner != batch.owner;
            let cross_start = (cross
                && !self.trackers[dependency.owner].covers(dependency.sequence))
            .then(Instant::now);
            let mut hol_start = None;
            let resolved = wait_until(
                &self.trackers[dependency.owner].slot_changed
                    [dependency.sequence as usize % self.trackers[dependency.owner].slots.len()],
                &mut stats.scheduling,
                || {
                    let resolved = if self.option == OptionKind::B {
                        // Exactly pred.owner_seq <= V[pred.owner], never a
                        // published bit beyond an unresolved owner-local gap.
                        self.trackers[dependency.owner].covers(dependency.sequence)
                    } else {
                        self.trackers[dependency.owner].published(dependency.sequence)
                    };
                    if !resolved
                        && cross
                        && hol_start.is_none()
                        && self.trackers[dependency.owner].published(dependency.sequence)
                    {
                        hol_start = Some(Instant::now());
                    }
                    checks += 1;
                    // One-bit log-volume counter, never a data publication flag.
                    if !resolved
                        && checks & 1023 == 0
                        && dependency_started.elapsed() >= Duration::from_millis(50)
                        && !self.diagnosed.swap(true, Ordering::Relaxed)
                    {
                        self.wait_diagnostic(identity, &versions, *dependency, false);
                    }
                    resolved
                },
            )
            .await;
            if !resolved {
                self.wait_diagnostic(identity, &versions, *dependency, true);
            }
            for &(owner, sequence) in &versions {
                self.trackers[owner].waiting_on
                    [sequence as usize % self.trackers[owner].slots.len()]
                .store(0, Ordering::Release);
            }
            if let Some(since) = cross_start {
                stats.cross_frontier_ns += since.elapsed().as_nanos() as u64;
                stats.cross_frontier_waits += 1;
            }
            if let Some(since) = hol_start {
                stats.hol_ns += since.elapsed().as_nanos() as u64;
            }
        }
        if self.option == OptionKind::B {
            assert!(
                dependencies
                    .iter()
                    .all(|p| self.trackers[p.owner].covers(p.sequence)),
                "B frontier must be dependency-closed"
            );
        }
        stats.dependency_ns = wait_start.elapsed().as_nanos() as u64;
        // Release slot completion publishes this null-owner batch to successors.
        for &(o, seq) in &versions {
            self.trackers[o].complete(seq, false);
        }
        for &(o, _) in &versions {
            let tracker = &self.trackers[o];
            // Observational lag, not a readiness predicate or atomic snapshot.
            let lag = tracker
                .next
                .load(Ordering::Relaxed)
                .saturating_sub(1 + tracker.prefix.load(Ordering::Acquire));
            stats.lags.push((o, lag));
        }
        if self.option == OptionKind::C {
            for &o in participants {
                assert!(self.owners[o].lock().identities.remove(&identity).is_some());
            }
        } else {
            assert!(
                self.identities[identity as usize & (SHARDS - 1)]
                    .lock()
                    .remove(&identity)
                    .is_some()
            );
        }
        stats
    }
    fn wait_diagnostic(&self, identity: u64, versions: &Owners, dependency: Version, fatal: bool) {
        fn file(path: String) -> serde_json::Value {
            match fs::read_to_string(&path) {
                Ok(value) => serde_json::json!({"value": value}),
                Err(error) => serde_json::json!({"path": path, "error": error.to_string()}),
            }
        }
        let mut snapshot = Vec::new();
        let mut before = Vec::new();
        let mut after = Vec::new();
        for _ in 0..32 {
            before = self
                .trackers
                .iter()
                .map(|t| t.prefix.load(Ordering::Acquire))
                .collect::<Vec<_>>();
            snapshot = self.trackers.iter().enumerate().map(|(owner, tracker)| {
                let head = before[owner] + 1;
                let index = head as usize % tracker.slots.len();
                let state = tracker.slots[index].load(Ordering::Acquire);
                let edge = tracker.waiting_on[index].load(Ordering::Acquire);
                let request = tracker.request[index].load(Ordering::Acquire);
                let client = (state >> 2 == head && request != u64::MAX).then(|| ((request & !(1u64 << 63)) % self.case.callers as u64) as usize);
                let active = client.map(|c| self.signals[c].active.load(Ordering::Acquire).to_string());
                serde_json::json!({
                    "owner": owner, "prefix": before[owner], "next": tracker.next.load(Ordering::Acquire),
                    "credits": tracker.credits.load(Ordering::Relaxed), "head_sequence": head,
                    "head_slot_raw": state, "head_generation": state >> 2, "head_resolved": state & 2 != 0,
                    "waiting_on": (edge != 0).then(|| serde_json::json!({"owner": edge & 15, "sequence": edge >> 4})),
                    "request_identity": (request != u64::MAX).then(|| request.to_string()), "client": client,
                    "client_active_identity": active
                })
            }).collect();
            after = self
                .trackers
                .iter()
                .map(|t| t.prefix.load(Ordering::Acquire))
                .collect();
            if before == after {
                break;
            }
        }
        let mut cycle = Vec::new();
        if self.option == OptionKind::B && before == after {
            for root in 0..snapshot.len() {
                let mut path = Vec::new();
                let mut node = root;
                loop {
                    if let Some(position) = path.iter().position(|&prior| prior == node) {
                        cycle.extend_from_slice(&path[position..]);
                        cycle.push(node);
                        break;
                    }
                    let head = &snapshot[node];
                    if head["head_resolved"] != false
                        || head["head_generation"] != head["head_sequence"]
                    {
                        break;
                    }
                    let edge = &head["waiting_on"];
                    let (Some(owner), Some(sequence)) =
                        (edge["owner"].as_u64(), edge["sequence"].as_u64())
                    else {
                        break;
                    };
                    if before[owner as usize] >= sequence {
                        break;
                    }
                    path.push(node);
                    node = owner as usize;
                }
                if !cycle.is_empty() {
                    break;
                }
            }
        }
        // A mixed diagnostic snapshot is not a proof of a cycle. Reconfirm the
        // same pending generations, identities, edges and uncovered frontiers.
        let cycle_confirmed = !cycle.is_empty()
            && cycle.windows(2).all(|pair| {
                let owner = pair[0];
                let target = pair[1];
                let tracker = &self.trackers[owner];
                let index = (before[owner] + 1) as usize % tracker.slots.len();
                let edge = tracker.waiting_on[index].load(Ordering::Acquire);
                let expected_request = snapshot[owner]["request_identity"]
                    .as_str()
                    .and_then(|value| value.parse::<u64>().ok());
                tracker.prefix.load(Ordering::Acquire) == before[owner]
                    && snapshot[owner]["head_slot_raw"].as_u64()
                        == Some(tracker.slots[index].load(Ordering::Acquire))
                    && expected_request == Some(tracker.request[index].load(Ordering::Acquire))
                    && edge & 15 == target as u64
                    && snapshot[owner]["waiting_on"]["sequence"].as_u64() == Some(edge >> 4)
                    && self.trackers[target].prefix.load(Ordering::Acquire) < edge >> 4
            });
        let threads = fs::read_dir("/proc/self/task")
            .expect("diagnostic thread inventory")
            .map(|entry| {
                let root = entry
                    .expect("diagnostic thread entry")
                    .path()
                    .display()
                    .to_string();
                serde_json::json!({
                    "thread_stat": file(format!("{root}/stat")),
                    "thread_wchan": file(format!("{root}/wchan")),
                    "thread_schedstat": file(format!("{root}/schedstat"))
                })
            })
            .collect::<Vec<_>>();
        eprintln!(
            "WAIT_DIAGNOSTIC {}",
            serde_json::json!({
                "type": if fatal { "fatal_dependency_wait" } else { "slow_dependency_wait" },
                "case": self.case.name(), "phase": if identity >> 63 != 0 { "warmup" } else { "timed" },
                "waiter_identity": identity.to_string(), "waiter_versions": versions.as_slice(),
                "dependency": {"owner": dependency.owner, "sequence": dependency.sequence, "stamp": dependency.stamp},
                "prefix_before": before, "prefix_after": after, "snapshot": snapshot,
                "cycle_owners": cycle, "cycle_confirmed": cycle_confirmed, "threads": threads,
                "runtime": "logical clients migrate across a fixed worker pool"
            })
        );
        // A fatal benchmark error must stop every waiter, not strand a pending
        // slot after one thread unwinds and cascade into hour-long join hangs.
        if fatal || cycle_confirmed {
            std::process::exit(1);
        }
    }
    fn check(&self) {
        for tracker in &self.trackers {
            tracker.check_drained();
        }
        assert!(self.identities.iter().all(|map| map.lock().is_empty()));
        assert!(
            self.owners
                .iter()
                .all(|stage| stage.lock().identities.is_empty())
        );
    }
}

/// Capacity = logical caller count, with at most one command per caller. A full
/// try_send is a violated admission bound. Final sender drop drains and joins.
fn serial(
    mut receiver: mpsc::Receiver<Command>,
    trace: Arc<Vec<Batch>>,
    signals: Arc<Vec<ClientSignals>>,
) {
    pin_cpu(0);
    let mut identities = HashMap::new();
    let mut last: HashMap<u32, Version> = HashMap::with_capacity(KEYS as usize);
    let mut next = 0u64;
    let mut prefix = 0u64;
    while let Some(command) = receiver.blocking_recv() {
        next += 1;
        let version = Version {
            stamp: next,
            owner: 0,
            sequence: next,
        };
        assert!(identities.insert(command.identity, version).is_none());
        let mut predecessors: SmallVec<[Version; 64]> = SmallVec::new();
        for key in &trace[command.index].keys {
            if let Some(&previous) = last.get(key)
                && !predecessors.contains(&previous)
            {
                predecessors.push(previous);
            }
        }
        for key in &trace[command.index].keys {
            if let Some(previous) = last.insert(*key, version) {
                assert!(previous.stamp < next, "A registration order");
            }
        }
        assert!(predecessors.iter().all(|p| p.sequence <= prefix));
        // The null owner completes inline; the dense tracker has one resolved head.
        assert_eq!(prefix + 1, next);
        prefix = next;
        assert!(identities.remove(&command.identity).is_some());
        signals[command.client].published.complete(command.identity);
    }
}
fn cpu_ticks() -> u64 {
    let stat = fs::read_to_string("/proc/self/stat").expect("Linux process CPU accounting");
    let fields: Vec<_> = stat[stat.rfind(')').expect("proc comm terminator") + 2..]
        .split_whitespace()
        .collect();
    fields[11].parse::<u64>().expect("utime") + fields[12].parse::<u64>().expect("stime")
}
fn ticks_per_second() -> f64 {
    let out = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .expect("getconf CLK_TCK");
    assert!(out.status.success());
    String::from_utf8(out.stdout)
        .expect("clock output")
        .trim()
        .parse()
        .expect("clock ticks")
}
#[derive(Default)]
struct WaitTotals {
    dependency_ns: u64,
    cross_frontier_ns: u64,
    cross_frontier_waits: u64,
    hol_ns: u64,
    lag_sum: [u64; 16],
    lag_max: [u64; 16],
    lag_samples: [u64; 16],
    scheduling: SchedulingStats,
}
impl WaitTotals {
    fn add(&mut self, stats: PutStats) {
        self.dependency_ns += stats.dependency_ns;
        self.cross_frontier_ns += stats.cross_frontier_ns;
        self.cross_frontier_waits += stats.cross_frontier_waits;
        self.hol_ns += stats.hol_ns;
        self.scheduling.spin_checks += stats.scheduling.spin_checks;
        self.scheduling.parks += stats.scheduling.parks;
        self.scheduling.wakeups += stats.scheduling.wakeups;
        self.scheduling.spurious_wakeups += stats.scheduling.spurious_wakeups;
        for (o, lag) in stats.lags {
            self.lag_sum[o] += lag;
            self.lag_max[o] = self.lag_max[o].max(lag);
            self.lag_samples[o] += 1;
        }
    }
    fn merge(&mut self, other: Self) {
        self.dependency_ns += other.dependency_ns;
        self.cross_frontier_ns += other.cross_frontier_ns;
        self.cross_frontier_waits += other.cross_frontier_waits;
        self.hol_ns += other.hol_ns;
        self.scheduling.spin_checks += other.scheduling.spin_checks;
        self.scheduling.parks += other.scheduling.parks;
        self.scheduling.wakeups += other.scheduling.wakeups;
        self.scheduling.spurious_wakeups += other.scheduling.spurious_wakeups;
        for o in 0..16 {
            self.lag_sum[o] += other.lag_sum[o];
            self.lag_max[o] = self.lag_max[o].max(other.lag_max[o]);
            self.lag_samples[o] += other.lag_samples[o];
        }
    }
    fn lags(&self) -> String {
        (0..16)
            .filter(|o| self.lag_samples[*o] > 0)
            .map(|o| {
                format!(
                    "{o}:{:.3}:{}",
                    self.lag_sum[o] as f64 / self.lag_samples[o] as f64,
                    self.lag_max[o]
                )
            })
            .collect::<Vec<_>>()
            .join("|")
    }
}
struct Samples {
    latencies: Vec<u64>,
    buckets: Vec<u64>,
    total: u64,
    mutations: u64,
    waits: WaitTotals,
    window_api_ns: u128,
}
struct ResultRow {
    rate: f64,
    p50: u64,
    p99: u64,
    p999: u64,
    cpu: f64,
    inflight: f64,
    count: usize,
    total: u64,
    mutations: f64,
    elapsed: Duration,
    multi: f64,
    waits: WaitTotals,
    host_start: HostSample,
    host_end: HostSample,
    external_cpu_s_per_s: Option<f64>,
    started_unix_s: f64,
    process_cpu_ticks: u64,
    api_ns_sum: u128,
    window_api_ns_sum: u128,
    workers: usize,
    worker_parks: u64,
    harness_wall: Duration,
}
#[derive(Clone, Copy, serde::Serialize)]
struct HostSample {
    load: [f64; 3],
    busy_ticks: u64,
}
fn host_sample() -> HostSample {
    let load = fs::read_to_string("/proc/loadavg").expect("host load averages");
    let mut fields = load.split_whitespace();
    let load = std::array::from_fn(|_| {
        fields
            .next()
            .expect("load field")
            .parse()
            .expect("load number")
    });
    let stat = fs::read_to_string("/proc/stat").expect("host CPU accounting");
    // USER_HZ: user, nice, system, irq, softirq and steal are busy. Exclude idle,
    // iowait and the guest counters already included in user/nice.
    let busy_ticks = stat
        .lines()
        .next()
        .expect("aggregate CPU row")
        .split_whitespace()
        .skip(1)
        .enumerate()
        .filter(|(i, _)| matches!(i, 0 | 1 | 2 | 5 | 6 | 7))
        .map(|(_, n)| n.parse::<u64>().expect("CPU ticks"))
        .sum();
    HostSample { load, busy_ticks }
}
struct Caller {
    id: usize,
    trace: Arc<Vec<Batch>>,
    signals: Arc<Vec<ClientSignals>>,
    command: Option<mpsc::Sender<Command>>,
    parallel: Option<Arc<Parallel>>,
}
impl Caller {
    async fn submit(&self, index: usize, identity: u64) -> PutStats {
        let signals = &self.signals[self.id];
        signals.active.store(identity, Ordering::Release);
        let mut stats = if let Some(core) = &self.parallel {
            let stats = core.put(&self.trace[index], identity, self.id).await;
            signals.published.complete(identity);
            stats
        } else {
            self.command
                .as_ref()
                .expect("A authority")
                .try_send(Command {
                    identity,
                    index,
                    client: self.id,
                })
                .expect("one outstanding command per client fits capacity N");
            PutStats::default()
        };
        signals
            .published
            .wait(identity, &mut stats.scheduling)
            .await;
        signals.active.store(u64::MAX, Ordering::Release);
        stats
    }
}

fn measure(
    case: Case,
    trace: Arc<Vec<Batch>>,
    seconds: usize,
    hz: f64,
    sample_limit: usize,
) -> ResultRow {
    let harness_started = Instant::now();
    let workers = case
        .callers
        .min(cpu_map().len().saturating_sub(1).max(1))
        .min(thread::available_parallelism().expect("worker bound").get());
    let worker_index = AtomicUsize::new(0);
    let park_counter = Arc::new(AtomicU64::new(0));
    let park_events = Arc::clone(&park_counter);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("m2-admission")
        .enable_time()
        .on_thread_start(move || pin_cpu(worker_index.fetch_add(1, Ordering::Relaxed) + 1))
        .on_thread_park(move || {
            park_events.fetch_add(1, Ordering::Relaxed);
        })
        .build()
        .expect("bounded worker runtime");
    let signals: Arc<Vec<_>> = Arc::new((0..case.callers).map(|_| ClientSignals::new()).collect());
    let barrier = Arc::new(Barrier::new(case.callers + 1));
    let start = Arc::new(OnceLock::<Instant>::new());
    let (command, actor) = if case.option == OptionKind::A {
        let (sender, receiver) = mpsc::channel(case.callers);
        let t = Arc::clone(&trace);
        let acknowledgements = Arc::clone(&signals);
        (
            Some(sender),
            Some(thread::spawn(move || serial(receiver, t, acknowledgements))),
        )
    } else {
        (None, None)
    };
    // Capacity N and one decision per logical request bound queue memory.
    // Full try_send rejects the run; closing the sender drains and joins.
    let (decision, decision_actor) = if case.option == OptionKind::C {
        let (sender, mut receiver) = mpsc::channel::<Decision>(case.callers);
        let acknowledgements = Arc::clone(&signals);
        (
            Some(sender),
            Some(thread::spawn(move || {
                pin_cpu(0);
                let mut decisions = HashMap::with_capacity(case.callers);
                while let Some(request) = receiver.blocking_recv() {
                    assert!(decisions.insert(request.identity, true).is_none());
                    assert!(decisions.remove(&request.identity).is_some());
                    acknowledgements[request.client]
                        .decision
                        .complete(request.identity);
                }
            })),
        )
    } else {
        (None, None)
    };
    let parallel = (case.option != OptionKind::A)
        .then(|| Arc::new(Parallel::new(case, decision, Arc::clone(&signals))));
    let mut tasks = Vec::with_capacity(case.callers);
    for caller in 0..case.callers {
        let barrier = Arc::clone(&barrier);
        let start = Arc::clone(&start);
        let client = Caller {
            id: caller,
            trace: Arc::clone(&trace),
            signals: Arc::clone(&signals),
            command: command.clone(),
            parallel: parallel.clone(),
        };
        tasks.push(runtime.spawn(async move {
            let limit = sample_limit;
            let mut samples = Samples {
                latencies: Vec::with_capacity(limit),
                buckets: vec![0; seconds],
                total: 0,
                mutations: 0,
                waits: WaitTotals::default(),
                window_api_ns: 0,
            };
            // Warm the same live maps and actors, without charging CPU/latency.
            // Every caller drains its last warm request before the two barriers.
            let warm_end = Instant::now() + Duration::from_millis(200);
            for operation in 0..limit {
                if Instant::now() >= warm_end {
                    break;
                }
                tokio::task::consume_budget().await;
                let index = (operation * case.callers + caller) % client.trace.len();
                let identity =
                    (operation as u64 * case.callers as u64 + caller as u64) | (1u64 << 63);
                black_box(client.submit(index, identity).await);
            }
            assert!(Instant::now() >= warm_end, "warmup sample bound reached");
            barrier.wait().await;
            barrier.wait().await;
            let beginning = *start.get().expect("measurement start set");
            let deadline = beginning + Duration::from_secs(seconds as u64);
            // A logical arrival exists BEFORE yielding the worker. The first N
            // requests arrive at the common gate; later arrivals follow the
            // preceding completion. Runtime ready-queue delay is API latency,
            // not silently excluded when a synchronous option exhausts budget.
            let mut dispatched = beginning;
            for operation in 0..limit {
                if dispatched >= deadline {
                    break;
                }
                tokio::task::consume_budget().await;
                let index = (operation * case.callers + caller) % client.trace.len();
                let identity = operation as u64 * case.callers as u64 + caller as u64;
                let stats = client.submit(index, identity).await;
                let completed = Instant::now();
                samples.total += 1;
                // Keep every timed arrival, including completions after the
                // deadline. Only rate buckets and occupancy clip to the window.
                samples
                    .latencies
                    .push(completed.duration_since(dispatched).as_nanos() as u64);
                samples.window_api_ns += completed
                    .min(deadline)
                    .duration_since(dispatched)
                    .as_nanos();
                samples.waits.add(stats);
                let bucket = completed.duration_since(beginning).as_secs() as usize;
                if bucket < seconds {
                    samples.buckets[bucket] += 1;
                    samples.mutations += client.trace[index].keys.len() as u64;
                }
                dispatched = Instant::now();
            }
            assert!(
                Instant::now() >= deadline,
                "sample bound reached: increase --sample-limit or reduce --seconds"
            );
            samples
        }));
    }
    runtime.block_on(barrier.wait());
    let host_start = host_sample();
    let started_unix_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Unix wall clock")
        .as_secs_f64();
    let cpu_start = cpu_ticks();
    let parks_start = park_counter.load(Ordering::Relaxed);
    start.set(Instant::now()).expect("single measurement start");
    let sample_sets = runtime.block_on(async {
        barrier.wait().await;
        let mut results = Vec::with_capacity(case.callers);
        for task in tasks {
            match task.await {
                Ok(samples) => results.push(samples),
                Err(error) => {
                    eprintln!("logical caller failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        results
    });
    let elapsed = start.get().expect("start").elapsed();
    let cpu = cpu_ticks() - cpu_start;
    let host_end = host_sample();
    let worker_parks = park_counter.load(Ordering::Relaxed) - parks_start;
    // Do not manufacture a negative CPU component when tick-boundary skew wins.
    let external_cpu_s_per_s = host_end
        .busy_ticks
        .checked_sub(host_start.busy_ticks)
        .and_then(|host| host.checked_sub(cpu))
        .map(|outside| outside as f64 / hz / elapsed.as_secs_f64());
    let sample_count: usize = sample_sets
        .iter()
        .map(|samples| samples.latencies.len())
        .sum();
    let mut latencies = Vec::with_capacity(sample_count);
    let mut buckets = vec![0u64; seconds];
    let mut total = 0;
    let mut mutations = 0;
    let mut waits = WaitTotals::default();
    let mut window_api_ns = 0;
    for samples in sample_sets {
        latencies.extend(samples.latencies);
        for (all, count) in buckets.iter_mut().zip(samples.buckets) {
            *all += count;
        }
        total += samples.total;
        mutations += samples.mutations;
        waits.merge(samples.waits);
        window_api_ns += samples.window_api_ns;
    }
    if let Some(p) = &parallel {
        p.check();
    }
    drop(command);
    drop(parallel);
    if let Some(actor) = actor {
        actor.join().expect("authority correctness checks");
    }
    if let Some(actor) = decision_actor {
        actor.join().expect("decision correctness checks");
    }
    drop(runtime);
    assert!(!latencies.is_empty());
    assert_eq!(
        latencies.len() as u64,
        total,
        "every timed success retains its latency"
    );
    let sum: u128 = latencies.iter().map(|n| u128::from(*n)).sum();
    latencies.sort_unstable();
    let quantile = |numerator: usize, denominator: usize| {
        latencies[(latencies.len() * numerator).div_ceil(denominator) - 1]
    };
    let multi =
        trace.iter().filter(|b| b.participants.len() > 1).count() as f64 / trace.len() as f64;
    ResultRow {
        rate: buckets.iter().sum::<u64>() as f64 / seconds as f64,
        p50: quantile(50, 100),
        p99: quantile(99, 100),
        p999: quantile(999, 1000),
        cpu: cpu as f64 / hz * 1e9 / total as f64,
        inflight: window_api_ns as f64 / (seconds as f64 * 1e9),
        count: buckets.iter().sum::<u64>() as usize,
        total,
        mutations: mutations as f64 / seconds as f64,
        elapsed,
        multi,
        waits,
        host_start,
        host_end,
        external_cpu_s_per_s,
        started_unix_s,
        process_cpu_ticks: cpu,
        api_ns_sum: sum,
        window_api_ns_sum: window_api_ns,
        workers,
        worker_parks,
        harness_wall: harness_started.elapsed(),
    }
}
/// Two simultaneously released completers exercise the store-buffering window,
/// including ring wrap and known aborts. The producer checks only after BOTH
/// callbacks returned, so a stranded resolved head is not scheduler starvation.
fn prefix_stress(rounds: usize) {
    let tracker = Tracker::new(2, false);
    let epoch = AtomicUsize::new(0);
    let finished = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let failure = thread::scope(|scope| {
        for worker in 0..2 {
            let tracker = &tracker;
            let epoch = &epoch;
            let finished = &finished;
            let stop = &stop;
            scope.spawn(move || {
                pin_cpu(worker + 1);
                for round in 0..rounds {
                    while epoch.load(Ordering::Acquire) != round + 1 {
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        std::hint::spin_loop();
                    }
                    let sequence = (round * 2 + worker + 1) as u64;
                    tracker.complete(sequence, sequence.is_multiple_of(7));
                    finished.fetch_or(1 << worker, Ordering::Release);
                }
            });
        }
        for round in 0..rounds {
            for index in 0..2 {
                tracker.reserve();
                assert_eq!(tracker.allocate(u64::MAX), (round * 2 + index + 1) as u64);
            }
            finished.store(0, Ordering::Release);
            epoch.store(round + 1, Ordering::Release);
            while finished.load(Ordering::Acquire) != 3 {
                std::hint::spin_loop();
            }
            let prefix = tracker.prefix.load(Ordering::Acquire);
            let expected = (round * 2 + 2) as u64;
            if prefix != expected {
                let diagnostic = serde_json::json!({
                    "round": round, "prefix": prefix, "expected": expected,
                    "next": tracker.next.load(Ordering::Relaxed),
                    "credits": tracker.credits.load(Ordering::Relaxed),
                    "slots": tracker.slots.iter().map(|s| s.load(Ordering::Acquire)).collect::<Vec<_>>(),
                    "all_completers_returned": true
                });
                stop.store(true, Ordering::Release);
                return Some(diagnostic);
            }
            tracker.check_drained();
        }
        None
    });
    assert!(failure.is_none(), "PREFIX_STRESS_FAILURE {failure:?}");
    tracker.check_drained();
}

#[test]
fn concurrent_prefix_completion() {
    prefix_stress(100_000);
    let tracker = Tracker::new(2, false);
    tracker.reserve(); // H retains this initial credit across many wraps.
    thread::scope(|scope| {
        let target = &tracker;
        let producer = scope.spawn(move || {
            pin_cpu(1);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("reuse scenario runtime");
            runtime.block_on(async {
                let mut stats = SchedulingStats::default();
                for operation in 0..100_000 {
                    target.reserve_async(&mut stats).await;
                    let seq = target.allocate(operation);
                    target.complete(seq, seq.is_multiple_of(7));
                }
            });
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        // Scheduling counter only, deliberately NOT a payload handoff. H must
        // acquire retirement through allocate(), not through a test barrier.
        while tracker.next.load(Ordering::Relaxed) < 20_001 {
            if Instant::now() >= deadline {
                eprintln!("EARLY_RESERVATION_STRESS_DEADLINE");
                std::process::exit(1);
            }
            thread::yield_now();
        }
        let held = tracker.allocate(u64::MAX);
        assert!(held >= 20_001, "early credit must survive many ring wraps");
        tracker.complete(held, false);
        // Join AFTER H's reuse, so it cannot hide the missing allocator edge.
        producer.join().expect("generation-wrap producer");
    });
    tracker.check_drained();
}

#[test]
fn parking_and_credit_cancellation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        // Completion between the final check and first poll must not be lost.
        let changed = Notify::new();
        let mut checks = 0;
        let mut stats = SchedulingStats::default();
        assert!(
            wait_until(&changed, &mut stats, || {
                checks += 1;
                if checks == SPIN_LIMIT + 2 {
                    changed.notify_waiters();
                    false
                } else {
                    checks > SPIN_LIMIT + 2
                }
            })
            .await
        );
        assert_eq!(stats.spin_checks, SPIN_LIMIT as u64);
        assert_eq!(stats.parks, 0, "already-notified is not an actual park");
        assert_eq!(stats.wakeups, 1);
        assert_eq!(stats.spurious_wakeups, 0);

        let tracker = Arc::new(Tracker::new(1, false));
        for cancel in [true, false] {
            tracker.reserve();
            let held = tracker.allocate(u64::MAX);
            let target = Arc::clone(&tracker);
            let entered = Arc::new(Notify::new());
            let started = Arc::clone(&entered);
            let waiter = tokio::spawn(async move {
                started.notify_one();
                let mut stats = SchedulingStats::default();
                target.reserve_async(&mut stats).await;
                stats
            });
            // On one worker, the child cannot return control until its full-ring
            // wait actually parks. No sleep or scheduler-yield guess is needed.
            entered.notified().await;
            assert!(!waiter.is_finished());
            if cancel {
                waiter.abort();
                assert!(waiter.await.expect_err("cancelled waiter").is_cancelled());
                tracker.complete(held, true);
            } else {
                tracker.complete(held, false);
                let stats = waiter.await.expect("woken waiter");
                assert_eq!(stats.spin_checks, SPIN_LIMIT as u64);
                assert_eq!(stats.parks, 1);
                assert_eq!(stats.wakeups, 1);
                assert_eq!(stats.spurious_wakeups, 0);
                let next = tracker.allocate(u64::MAX);
                tracker.complete(next, false);
            }
            tracker.check_drained();
        }
    });
}

#[test]
fn targeted_slot_and_frontier_notifications() {
    struct WakeCount(AtomicU64);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let tracker = Tracker::new(3, true);
    let sequences: Vec<_> = (0..3)
        .map(|_| {
            tracker.reserve();
            tracker.allocate(u64::MAX)
        })
        .collect();
    let counter = Arc::new(WakeCount(AtomicU64::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&counter));
    let mut context = std::task::Context::from_waker(&waker);
    let mut waiting = std::pin::pin!(tracker.slot_changed[0].notified());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    tracker.complete(sequences[1], false);
    assert_eq!(
        counter.0.load(Ordering::Relaxed),
        0,
        "unrelated slot woke waiter"
    );
    tracker.complete(sequences[2], false);
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert!(waiting.as_mut().poll(&mut context).is_ready());
    assert!(!tracker.covers(sequences[2]), "held head must gate B");
    let mut frontier = std::pin::pin!(tracker.slot_changed[0].notified());
    assert!(frontier.as_mut().poll(&mut context).is_pending());
    tracker.complete(sequences[0], false);
    assert_eq!(
        counter.0.load(Ordering::Relaxed),
        2,
        "frontier crossing must wake B"
    );
    assert!(frontier.as_mut().poll(&mut context).is_ready());
    assert!(tracker.covers(sequences[2]));
    tracker.check_drained();
}

fn correctness() {
    prefix_stress(4096);
    let tracker = Tracker::new(4, false);
    tracker.reserve();
    let one = tracker.allocate(u64::MAX);
    tracker.reserve();
    let two = tracker.allocate(u64::MAX);
    tracker.reserve();
    let three = tracker.allocate(u64::MAX);
    tracker.reserve();
    let four = tracker.allocate(u64::MAX);
    assert!(!tracker.try_reserve(), "full ring must not allocate");
    tracker.complete(four, false);
    tracker.complete(three, false);
    tracker.complete(two, false);
    assert!(!tracker.covers(one), "D must not pass an unresolved slot");
    tracker.complete(one, true);
    assert!(tracker.covers(two), "known abort resolves a gap");
    for _ in 0..100 {
        tracker.reserve();
        let seq = tracker.allocate(u64::MAX);
        tracker.complete(seq, seq.is_multiple_of(3));
    }
    tracker.check_drained();
    // Explicitly reproduce why a published bit alone cannot close B's vector.
    let a = Tracker::new(4, true);
    let b = Tracker::new(4, true);
    a.reserve();
    let a1 = a.allocate(u64::MAX);
    a.reserve();
    let a2 = a.allocate(u64::MAX);
    a.complete(a2, false);
    b.reserve();
    let b1 = b.allocate(u64::MAX);
    assert!(!a.covers(a2));
    assert!(!b.covers(b1));
    a.complete(a1, false);
    assert!(a.covers(a2));
    b.complete(b1, false);
    a.check_drained();
    b.check_drained();
}
#[cfg(all(feature = "experimental-distributed", feature = "test-utils"))]
fn codec_control(hz: f64, json: &mut impl Write, label: &str) {
    use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
    use tidehunter::distributed::types::{BatchId, BatchVersion};
    use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
    let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    for bytes in [64, 1024] {
        let mutation = Mutation::Put {
            keyspace: KeySpace::new(0),
            key: 0u32.to_be_bytes().to_vec().into(),
            value: vec![1; bytes].into(),
        };
        let host_start = host_sample();
        let cpu = cpu_ticks();
        let start = Instant::now();
        let mut n = 0u64;
        while start.elapsed() < Duration::from_secs(1) {
            black_box(
                CompleteBatch::new(
                    &shape,
                    BatchId {
                        database: [0; 16],
                        operation: u128::from(n).to_be_bytes(),
                    },
                    BatchVersion {
                        epoch: 1,
                        sequence: n,
                    },
                    vec![mutation.clone()],
                    BatchLimits {
                        max_operations: 1,
                        max_encoded_bytes: 4096,
                    },
                )
                .expect("codec control"),
            );
            n += 1;
        }
        let elapsed = start.elapsed();
        let cpu = cpu_ticks() - cpu;
        let host_end = host_sample();
        let external = host_end
            .busy_ticks
            .checked_sub(host_start.busy_ticks)
            .and_then(|host| host.checked_sub(cpu))
            .map(|outside| outside as f64 / hz / elapsed.as_secs_f64());
        let wall_ns = elapsed.as_nanos() as f64 / n as f64;
        let cpu_ns = cpu as f64 / hz * 1e9 / n as f64;
        writeln!(
            json,
            "{}",
            serde_json::json!({
                "type": "codec", "label": label, "value_bytes": bytes, "n": n,
                "wall_ns_per_batch": wall_ns, "cpu_ns_per_batch": cpu_ns,
                "method": "CompleteBatch::new, not whole authority J2",
                "host_start": host_start, "host_end": host_end, "process_cpu_ticks": cpu,
                "case_elapsed_s": elapsed.as_secs_f64(), "external_cpu_s_per_s": external,
                "contaminated": external.is_none_or(|outside| outside > 1.0)
            })
        )
        .expect("codec JSON");
        json.flush().expect("codec JSON flush");
        eprintln!(
            "J2_CODEC,value_bytes={bytes},n={n},wall_ns_per_batch={wall_ns:.2},cpu_ns_per_batch={cpu_ns:.2}"
        );
    }
}
/// Use the existing taskset executable to avoid adding an unsafe affinity edge.
/// Reserve the actor's physical core; use one logical CPU per other core first.
fn cpu_map() -> &'static Vec<usize> {
    static CPUS: std::sync::LazyLock<Vec<usize>> = std::sync::LazyLock::new(|| {
        let status = fs::read_to_string("/proc/self/status").expect("allowed CPU inventory");
        let allowed = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            .expect("CPU list")
            .trim();
        let mut ids = Vec::new();
        for part in allowed.split(',') {
            if let Some((first, last)) = part.split_once('-') {
                ids.extend(
                    first.parse::<usize>().expect("first CPU")
                        ..=last.parse::<usize>().expect("last CPU"),
                );
            } else {
                ids.push(part.parse().expect("CPU number"));
            }
        }
        assert!(!ids.is_empty());
        let topology = |cpu| {
            let root = format!("/sys/devices/system/cpu/cpu{cpu}/topology");
            let package =
                fs::read_to_string(format!("{root}/physical_package_id")).expect("CPU package");
            let core = fs::read_to_string(format!("{root}/core_id")).expect("CPU core");
            (package, core)
        };
        let actor_core = topology(ids[0]);
        let mut seen = vec![actor_core.clone()];
        let mut primary = vec![ids[0]];
        let mut siblings = Vec::new();
        for &cpu in &ids[1..] {
            let core = topology(cpu);
            if core == actor_core {
                continue;
            }
            if seen.contains(&core) {
                siblings.push(cpu);
            } else {
                seen.push(core);
                primary.push(cpu);
            }
        }
        primary.extend(siblings);
        let selected = if primary.len() == 1 { ids } else { primary };
        eprintln!("M2_CPU_MAP={selected:?}");
        selected
    });
    &CPUS
}
fn pin_cpu(slot: usize) {
    let cpus = cpu_map();
    let index = if slot == 0 || cpus.len() == 1 {
        0
    } else {
        1 + (slot - 1) % (cpus.len() - 1)
    };
    let tid = fs::read_link("/proc/thread-self")
        .expect("current Linux TID")
        .file_name()
        .expect("TID component")
        .to_owned();
    let result = std::process::Command::new("taskset")
        .args(["-pc", &cpus[index].to_string()])
        .arg(tid)
        .output()
        .expect("taskset executable");
    assert!(
        result.status.success(),
        "affinity failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

pub fn run() {
    let config = cli::Config::parse();
    correctness();
    let hz = ticks_per_second();
    let mut csv = BufWriter::new(File::create(&config.csv).expect("create matrix CSV"));
    let sink: Box<dyn Write> = if config.json == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(File::create(&config.json).expect("create JSONL output"))
    };
    let mut json = BufWriter::new(sink);
    let cpu_info = fs::read_to_string("/proc/cpuinfo").expect("CPU inventory");
    let model = cpu_info
        .lines()
        .find_map(|line| line.strip_prefix("model name"))
        .and_then(|line| line.split_once(':'))
        .map(|(_, name)| name.trim())
        .unwrap_or("unreported");
    writeln!(json, "{}", serde_json::json!({
        "type": "metadata", "schema": 2, "label": config.label,
        "cpu_model": model, "logical_cpus": std::thread::available_parallelism().expect("CPU count").get(),
        "pin_map_actor_then_callers": cpu_map(), "clock_ticks_per_second": hz,
        "kernel": fs::read_to_string("/proc/sys/kernel/osrelease").expect("kernel version").trim(),
        "keys": KEYS, "trace_batches": TRACE, "seed": 0x4d32_2026u64,
        "sample_limit_per_caller": config.sample_limit,
        "callers": config.callers, "owners": config.owners, "batches": config.batches,
        "fractions_pct": config.fractions, "distributions": config.distributions.iter().map(|z| if *z { "zipf099" } else { "uniform" }).collect::<Vec<_>>(),
        "options": config.options.iter().map(|o| o.name()).collect::<Vec<_>>(),
        "seconds": config.seconds, "repeats": config.repeats,
        "acknowledgement": "instant in-process null owner, no storage/network I/O",
        "engine_path": false, "external_cpu_flag_threshold_s_per_s": 1.0,
        "latency_population": "all successful timed arrivals, including drained tail completions",
        "rate_population": "complete one-second buckets",
        "mean_inflight_population": "API occupancy integral clipped to the timed window",
        "stage_wait_population": "all successful timed arrivals",
        "execution_model": "bounded pinned worker runtime, one task and at most one outstanding request per logical caller",
        "arrival_boundary": "first wave at common gate; subsequent arrivals before cooperative worker yield",
        "wait_policy": "64 failed spin checks then Notify task parking with predecessor wakeup",
        "wait_attempt_limit": WAIT_LIMIT, "wait_deadline_s": 30,
        "logical_park_count": "one count when a registered waiter first polls Pending",
        "runtime_worker_parks": "runtime on_thread_park callbacks during timed arrivals and drain"
    })).expect("metadata JSON");
    json.flush().expect("metadata flush");
    if config.codec {
        #[cfg(all(feature = "experimental-distributed", feature = "test-utils"))]
        codec_control(hz, &mut json, &config.label);
        #[cfg(not(all(feature = "experimental-distributed", feature = "test-utils")))]
        writeln!(json, "{}", serde_json::json!({
            "type": "codec_unavailable", "label": config.label,
            "reason": "enable experimental-distributed,test-utils for CompleteBatch codec control"
        })).expect("codec availability JSON");
    }
    let quoted_label = format!("\"{}\"", config.label.replace('"', "\"\""));
    writeln!(csv, "case,repetition,option,callers,owners,distribution,batch_keys,requested_multi_pct,trace_multi_fraction,seconds,successful_complete_buckets,offered,successful,rejected,batches_s,mutations_s,p50_ns,p99_ns,p999_ns,cpu_ns_per_batch,mean_inflight,case_elapsed_s,dependency_wait_ns_per_batch,cross_frontier_wait_ns_per_batch,cross_frontier_waits,cross_owner_hol_ns_per_batch,owner_frontier_lag_mean_max,started_unix_s,load1_start,load5_start,load15_start,load1_end,load5_end,load15_end,host_busy_ticks_start,host_busy_ticks_end,process_cpu_ticks,external_cpu_s_per_s,contaminated,label,latency_samples,whole_case_batches_s,whole_case_mean_inflight,api_ns_sum,window_api_ns_sum,runtime_workers,spin_checks,logical_parks,runtime_worker_parks,harness_wall_s,wakeups,spurious_wakeups").expect("CSV header");
    let mut cdf = Vec::with_capacity(KEYS as usize);
    let mut sum = 0.0;
    for key in 1..=KEYS {
        sum += f64::from(key).powf(-0.99);
        cdf.push(sum);
    }
    for p in &mut cdf {
        *p /= sum;
    }
    let mut criterion = config.criterion.then(|| {
        Criterion::default()
            .sample_size(10)
            .warm_up_time(Duration::from_millis(10))
            .measurement_time(Duration::from_secs(1))
    });
    for &owners in &config.owners {
        for &batch in &config.batches {
            for &zipf in &config.distributions {
                let fractions = if owners == 1 || batch == 1 {
                    &[0][..]
                } else {
                    &config.fractions
                };
                for &fraction in fractions {
                    let base = Case {
                        option: OptionKind::A,
                        callers: 1,
                        owners,
                        batch,
                        zipf,
                        fraction: fraction as u32,
                    };
                    let workload = Arc::new(trace(base, &cdf));
                    for &callers in &config.callers {
                        for repetition in 0..config.repeats {
                            let mut options =
                                [OptionKind::A, OptionKind::B, OptionKind::C, OptionKind::D];
                            options.rotate_left(
                                (repetition + callers.trailing_zeros() as usize + batch + owners)
                                    % 4,
                            );
                            if repetition % 2 == 1 {
                                options.reverse();
                            }
                            for option in options {
                                if !config.options.contains(&option) {
                                    continue;
                                }
                                let case = Case {
                                    option,
                                    callers,
                                    ..base
                                };
                                let row = measure(
                                    case,
                                    Arc::clone(&workload),
                                    config.seconds,
                                    hz,
                                    config.sample_limit,
                                );
                                let contaminated =
                                    row.external_cpu_s_per_s.is_none_or(|outside| outside > 1.0);
                                write!(csv, "{},{},{},{},{},{},{},{},{:.8},{},{},{},{},0,{:.3},{:.3},{},{},{},{:.3},{:.6},{:.6},{:.3},{:.3},{},{:.3},{}", case.name(), repetition + 1, option.name(), callers, owners, if zipf { "zipf099" } else { "uniform" }, batch, fraction, row.multi, config.seconds, row.count, row.total, row.total, row.rate, row.mutations, row.p50, row.p99, row.p999, row.cpu, row.inflight, row.elapsed.as_secs_f64(), row.waits.dependency_ns as f64 / row.total as f64, row.waits.cross_frontier_ns as f64 / row.total as f64, row.waits.cross_frontier_waits, row.waits.hol_ns as f64 / row.total as f64, row.waits.lags()).expect("CSV metrics");
                                let external = row
                                    .external_cpu_s_per_s
                                    .map(|v| format!("{v:.6}"))
                                    .unwrap_or_default();
                                writeln!(
                                    csv,
                                    ",{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{},{},{},{},{},{:.3},{:.6},{},{},{},{},{},{},{:.6},{},{}",
                                    row.started_unix_s,
                                    row.host_start.load[0],
                                    row.host_start.load[1],
                                    row.host_start.load[2],
                                    row.host_end.load[0],
                                    row.host_end.load[1],
                                    row.host_end.load[2],
                                    row.host_start.busy_ticks,
                                    row.host_end.busy_ticks,
                                    row.process_cpu_ticks,
                                    external,
                                    u8::from(contaminated),
                                    quoted_label,
                                    row.total,
                                    row.total as f64 / row.elapsed.as_secs_f64(),
                                    row.api_ns_sum as f64 / (row.elapsed.as_secs_f64() * 1e9),
                                    row.api_ns_sum,
                                    row.window_api_ns_sum,
                                    row.workers,
                                    row.waits.scheduling.spin_checks,
                                    row.waits.scheduling.parks,
                                    row.worker_parks,
                                    row.harness_wall.as_secs_f64(),
                                    row.waits.scheduling.wakeups,
                                    row.waits.scheduling.spurious_wakeups
                                )
                                .expect("CSV provenance");
                                let frontier: Vec<_> = (0..owners).filter(|o| row.waits.lag_samples[*o] > 0).map(|o| serde_json::json!({
                                    "owner": o, "sum": row.waits.lag_sum[o], "samples": row.waits.lag_samples[o],
                                    "mean": row.waits.lag_sum[o] as f64 / row.waits.lag_samples[o] as f64,
                                    "sampled_max": row.waits.lag_max[o]
                                })).collect();
                                writeln!(json, "{}", serde_json::json!({
                                    "type": "case", "label": config.label, "case": case.name(), "repetition": repetition + 1,
                                    "option": option.name(), "callers": callers, "owners": owners, "batch_keys": batch,
                                    "distribution": if zipf { "zipf099" } else { "uniform" },
                                    "requested_multi_pct": fraction, "trace_multi_fraction": row.multi,
                                    "seconds": config.seconds, "successful_complete_buckets": row.count,
                                    "offered": row.total, "successful": row.total, "rejected": 0,
                                    "latency_samples": row.total,
                                    "whole_case_batches_s": row.total as f64 / row.elapsed.as_secs_f64(),
                                    "whole_case_mean_inflight": row.api_ns_sum as f64 / (row.elapsed.as_secs_f64() * 1e9),
                                    "api_ns_sum": row.api_ns_sum.to_string(), "window_api_ns_sum": row.window_api_ns_sum.to_string(),
                                    "batches_s": row.rate, "mutations_s": row.mutations,
                                    "p50_ns": row.p50, "p99_ns": row.p99, "p999_ns": row.p999,
                                    "cpu_ns_per_batch": row.cpu, "mean_inflight": row.inflight,
                                    "case_elapsed_s": row.elapsed.as_secs_f64(), "started_unix_s": row.started_unix_s,
                                    "dependency_wait_ns_per_batch": row.waits.dependency_ns as f64 / row.total as f64,
                                    "cross_frontier_wait_ns_per_batch": row.waits.cross_frontier_ns as f64 / row.total as f64,
                                    "cross_frontier_waits": row.waits.cross_frontier_waits,
                                    "cross_owner_hol_ns_per_batch": row.waits.hol_ns as f64 / row.total as f64,
                                    "per_owner_frontier_lag": frontier, "host_start": row.host_start, "host_end": row.host_end,
                                    "process_cpu_ticks": row.process_cpu_ticks,
                                    "external_cpu_s_per_s": row.external_cpu_s_per_s, "contaminated": contaminated,
                                    "runtime_workers": row.workers, "spin_checks": row.waits.scheduling.spin_checks,
                                    "logical_parks": row.waits.scheduling.parks, "runtime_worker_parks": row.worker_parks,
                                    "harness_wall_s": row.harness_wall.as_secs_f64(),
                                    "wakeups": row.waits.scheduling.wakeups,
                                    "spurious_wakeups": row.waits.scheduling.spurious_wakeups
                                })).expect("case JSON");
                                csv.flush().expect("persist CSV case");
                                json.flush().expect("persist JSON case");
                                if config.json != "-" {
                                    println!(
                                        "{} r{} {:.0} batch/s p50={}ns p99={}ns external_cpu={external} contaminated={contaminated} [{}]",
                                        case.name(),
                                        repetition + 1,
                                        row.rate,
                                        row.p50,
                                        row.p99,
                                        config.label
                                    );
                                }
                                if let Some(c) = &mut criterion {
                                    c.bench_function(&case.name(), |b| {
                                        b.iter(|| {
                                            black_box(
                                                measure(
                                                    case,
                                                    Arc::clone(&workload),
                                                    config.seconds,
                                                    hz,
                                                    config.sample_limit,
                                                )
                                                .rate,
                                            )
                                        })
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(c) = criterion {
        c.final_summary();
    }
    if config.json != "-" {
        println!("M2_CSV={} M2_JSON={}", config.csv, config.json);
    }
}

#[test]
fn concurrent_slot_reuse() {
    let tracker = Tracker::new(64, false);
    thread::scope(|scope| {
        for worker in 0..4 {
            let tracker = &tracker;
            scope.spawn(move || {
                pin_cpu(worker + 1);
                // No round barrier: credit publication is the reuse handoff.
                for operation in 0..100_000 {
                    let started = Instant::now();
                    let mut reserved = false;
                    for attempt in 0..WAIT_LIMIT {
                        if tracker.try_reserve() {
                            reserved = true;
                            break;
                        }
                        if attempt & 1023 == 0 && started.elapsed() >= Duration::from_secs(30) {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    if !reserved {
                        eprintln!(
                            "SLOT_REUSE_STRESS_DEADLINE prefix={} next={} credits={}",
                            tracker.prefix.load(Ordering::Acquire),
                            tracker.next.load(Ordering::Relaxed),
                            tracker.credits.load(Ordering::Relaxed)
                        );
                        std::process::exit(1);
                    }
                    if operation % 127 == 0 {
                        // Exercise a reservation held across other allocations.
                        thread::yield_now();
                    }
                    let seq = tracker.allocate((operation * 4 + worker) as u64);
                    tracker.complete(seq, seq.is_multiple_of(7));
                }
            });
        }
    });
    tracker.check_drained();
}
