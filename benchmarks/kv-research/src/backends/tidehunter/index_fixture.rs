//! Roadmap Phase 6 A1 index fixture on the shared leased ordinary-file route
//! (`checkpoint-work/research/design/INDEX-A0-A1.md`, "A1"), and the shared
//! stage machinery of the Q1 staged two-owner index runner ([`q1`]).
//!
//! One A1 case directory (`backend.options.path`, absolute) holds, across
//! three processes:
//! - `build`: a fresh owner child and coordinator; exactly `N` scalar puts at
//!   IDs `0..N`; untimed verification of every row; one explicit adopted
//!   checkpoint (`harness_drain`); orderly close of both ends; then the build
//!   receipt (written only after both ends closed cleanly);
//! - `overwrite`: receipt validation before any write; reopen; timed uniform
//!   overwrites; natural drain; the stop record (suffix, timed byte counters,
//!   acknowledged count, touched bitmap); a crash-style coordinator stop (no
//!   shutdown root) and a graceful owner stop;
//! - cold reopen ([`cold`]): eviction with a residency check, timed open to
//!   the first correct read, recovery reconciliation and exact final rows.
//!
//! PerCell arms are rejected at open: this revision has no active PerCell
//! scheduler (`exp/option-c` test branch only).

pub(super) mod cold;
pub(super) mod q1;
#[cfg(test)]
mod tests;

use super::{Options, TidehunterBackend};
use crate::backend::BackendResult;
use crate::config::RunConfig;
use crate::index_a1::{FixtureFinish, FixtureRun, IndexFixtureStage, TouchedBitmap, ValueDomain};
use crate::index_checkpoint::{OpenKind, OpenRecord};
use crate::leased_fixture::{BenchmarkGeometry, FixtureSpec, OwnerChild, fresh_owners};
use crate::workload::key_bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tidehunter::distributed::remote_db::{
    IndexCheckpointSnapshot, ReplayLimits, diagnostics_clock_ns,
};
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::Transport;
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};

/// Keys per untimed verification read.
const VERIFY_CHUNK: u64 = 1024;
/// Mismatching IDs kept for the report; the total is always counted.
const MISMATCH_SAMPLE: usize = 16;
const IDLE_TIMEOUT: Duration = Duration::from_secs(600);
const RECEIPT: &str = "fixture-receipt.json";
const STOP_STATE: &str = "overwrite-stop.json";
const TOUCHED: &str = "touched.bin";
const STAGE_OUTPUT: &str = "index-a1-stage.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum IndexArm {
    Exact,
    PerCell,
}

/// Native-style PerCell triggers (dirty keys per cell, snapshot cadence and
/// stale age on the owner WAL data-frame byte clock).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PerCellThresholds {
    dirty_keys: u64,
    snapshot_written_bytes: u64,
    stale_wal_bytes: u64,
}

/// `backend.options.index_fixture`; benchmark-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IndexFixtureOptions {
    stage: IndexFixtureStage,
    arm: IndexArm,
    max_admitted_batches: usize,
    max_index_bytes: usize,
    fragment_bytes: u64,
    wal_file_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    per_cell: Option<PerCellThresholds>,
}

/// Fixture geometry rule: admitted batches 1..=1M, positive index bytes, 4 KiB
/// fragments of at least one 2 MiB group frame plus its header page, dividing
/// the WAL file.
fn valid_geometry(
    max_admitted_batches: usize,
    max_index_bytes: usize,
    fragment_bytes: u64,
    wal_file_bytes: u64,
) -> bool {
    (1..=1_000_000).contains(&max_admitted_batches)
        && max_index_bytes > 0
        && fragment_bytes.is_multiple_of(4096)
        && fragment_bytes >= 2 * 1024 * 1024 + 4096
        && wal_file_bytes > 0
        && wal_file_bytes.is_multiple_of(fragment_bytes)
}

impl IndexFixtureOptions {
    pub(super) fn validate(&self) -> BackendResult<()> {
        if !valid_geometry(
            self.max_admitted_batches,
            self.max_index_bytes,
            self.fragment_bytes,
            self.wal_file_bytes,
        ) {
            return Err("index_fixture geometry: admitted batches 1..=1000000, positive index bytes, 4 KiB fragments of at least 2 MiB + 4 KiB dividing the WAL file".into());
        }
        match (self.arm, &self.per_cell) {
            (IndexArm::Exact, None) | (IndexArm::PerCell, Some(_)) => Ok(()),
            _ => Err(
                "index_fixture.per_cell is required for arm=per_cell and forbidden for arm=exact"
                    .into(),
            ),
        }
    }

    /// Everything but the stage must equal the build receipt.
    fn same_case(&self, other: &Self) -> bool {
        self.arm == other.arm
            && self.max_admitted_batches == other.max_admitted_batches
            && self.max_index_bytes == other.max_index_bytes
            && self.fragment_bytes == other.fragment_bytes
            && self.wal_file_bytes == other.wal_file_bytes
            && self.per_cell == other.per_cell
    }

    fn geometry(&self, replay_limits: ReplayLimits) -> BenchmarkGeometry {
        BenchmarkGeometry {
            fragment_bytes: self.fragment_bytes,
            wal_file_bytes: self.wal_file_bytes,
            max_admitted_batches: self.max_admitted_batches,
            max_index_bytes: self.max_index_bytes,
            replay_limits: Some(replay_limits),
        }
    }
}

/// Adapter options the fixture route admits: one owner on trusted loopback
/// TCP, no coordinator value cache, explicit replay envelope, nothing external.
fn validate_route(options: &Options) -> BackendResult<ReplayLimits> {
    if options.owners != 1
        || options.transport != Transport::TcpTrusted
        || options.coordinator_read_cache_bytes.unwrap_or(0) != 0
        || options.external_remote_config.is_some()
        || !options.external_owner_durability_receipts.is_empty()
        || options.batch_limits.is_some()
        || options.owner_append_occupancy.is_some()
        || !options.path.is_absolute()
    {
        return Err("index_fixture requires owners=1, tcp-trusted, coordinator_read_cache_bytes=0, an absolute path and no external, reader, batch or occupancy overrides".into());
    }
    options
        .replay_limits
        .ok_or_else(|| "index_fixture requires explicit replay_limits".into())
}

/// Dataset identity a receipt binds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetIdentity {
    key_namespace_hex: String,
    seed: u64,
    keys: u64,
    value_size: usize,
    entropy: f64,
}

impl DatasetIdentity {
    fn from_config(config: &RunConfig) -> BackendResult<Self> {
        Ok(Self {
            key_namespace_hex: config
                .key_namespace_hex
                .clone()
                .ok_or("index_fixture requires key_namespace_hex")?,
            seed: config.seed,
            keys: config.dataset.keys,
            value_size: config.dataset.value_size,
            entropy: config.dataset.entropy,
        })
    }

    fn namespace(&self) -> BackendResult<[u8; 16]> {
        let mut namespace = [0; 16];
        let digits = self.key_namespace_hex.as_bytes();
        if digits.len() != 32 {
            return Err("key_namespace_hex must have 32 hex digits".into());
        }
        for (byte, pair) in namespace.iter_mut().zip(digits.chunks_exact(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        }
        Ok(namespace)
    }

    fn value(&self, domain: ValueDomain, id: u64) -> BackendResult<tidehunter::minibytes::Bytes> {
        Ok(domain.value(
            &self.namespace()?,
            self.seed,
            id,
            self.value_size,
            self.entropy,
        ))
    }
}

/// Case files under `backend.options.path`.
#[derive(Clone, Debug)]
struct CasePaths {
    root: PathBuf,
    coordinator: PathBuf,
    fixture: PathBuf,
    evidence: PathBuf,
}

impl CasePaths {
    fn new(options: &Options) -> Self {
        Self {
            root: options.path.clone(),
            coordinator: options.path.join("coordinator"),
            fixture: options.path.join("fixture"),
            evidence: options
                .evidence
                .clone()
                .unwrap_or_else(|| options.path.join("evidence")),
        }
    }

    fn file(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

/// Written only after a verified build and a clean close of both ends.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildReceipt {
    schema_version: u32,
    path: PathBuf,
    dataset: DatasetIdentity,
    shape_yaml: String,
    fixture: IndexFixtureOptions,
    replay_limits: ReplayLimits,
    spec: FixtureSpec,
    build_case_id: Option<String>,
    verification: Value,
}

impl BuildReceipt {
    /// Fails before any owner start or write on a mismatched case.
    fn check(
        &self,
        paths: &CasePaths,
        dataset: &DatasetIdentity,
        shape_yaml: &str,
        fixture: &IndexFixtureOptions,
        replay_limits: ReplayLimits,
        tier: tidehunter::distributed::durability::DurabilityTier,
    ) -> BackendResult<()> {
        let mut mismatches = Vec::new();
        if self.schema_version != 1 {
            mismatches.push("schema_version");
        }
        if self.path != paths.root || self.spec.root != paths.fixture {
            mismatches.push("path");
        }
        if &self.dataset != dataset {
            mismatches.push("dataset (namespace, seed, keys, value size, entropy)");
        }
        if self.shape_yaml != shape_yaml {
            mismatches.push("key shape");
        }
        if !self.fixture.same_case(fixture) {
            mismatches.push("arm or fixture geometry");
        }
        if self.replay_limits != replay_limits {
            mismatches.push("replay_limits");
        }
        if self.spec.tier != tier {
            mismatches.push("durability tier");
        }
        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(format!("build receipt mismatch: {}", mismatches.join(", ")).into())
        }
    }
}

/// Timed-window boundary counters from `set_measurement_window`.
#[derive(Clone, Debug, Serialize)]
struct Boundary {
    at_ns: u64,
    index: Option<IndexCheckpointSnapshot>,
    /// Sum of every coordinator file category (process-wide producer counters).
    coordinator_file_bytes: Option<u64>,
    /// Leased data-frame bytes including padding, charged once per group at
    /// successful completion release.
    leased_bytes: Option<u64>,
    leased_frames: Option<u64>,
    leased_skip_markers: Option<u64>,
    published_batches: Option<u64>,
    admitted_batches: Option<u64>,
}

impl Boundary {
    fn capture(db: &sui::Db) -> Self {
        let index = db.index_checkpoint_snapshot();
        let live = db.live_nvmeof_stats();
        let counters = db.remote_counters();
        Self {
            at_ns: diagnostics_clock_ns(),
            coordinator_file_bytes: index.map(|snapshot| snapshot.coordinator_io.total().bytes),
            index,
            leased_bytes: live.as_ref().map(|stats| stats.counts.leased_bytes),
            leased_frames: live.as_ref().map(|stats| stats.counts.leased_frames),
            leased_skip_markers: live.as_ref().map(|stats| stats.counts.leased_skip_markers),
            published_batches: counters
                .as_ref()
                .map(|(_, stats, _)| stats.published_batches),
            admitted_batches: counters
                .as_ref()
                .map(|(_, stats, _)| stats.admitted_batches),
        }
    }
}

fn delta(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    end?.checked_sub(start?)
}

#[derive(Default)]
struct Boundaries {
    start: Option<Boundary>,
    end: Option<Boundary>,
}

/// Fixture state the adapter owns for one stage: runner hooks, timed-window
/// boundaries, the crash-style stop flag, supervised owners and the
/// route-specific stage end.
pub(super) struct FixtureState {
    pub(super) run: FixtureRun,
    boundaries: Mutex<Boundaries>,
    /// Set by a stage end that stops crash-style: drop the coordinator
    /// without close.
    crash_stop: AtomicBool,
    /// Supervised owner children of the local fixture route, stopped at
    /// close; empty when owners are supervised outside this process.
    owners: Mutex<Vec<OwnerChild>>,
    stage: StageEnd,
}

/// The stage end and close of each route.
#[allow(
    clippy::large_enum_variant,
    reason = "one stage per benchmark process; keep its receipt inline"
)]
enum StageEnd {
    A1(A1Stage),
    Q1(q1::Q1Stage),
}

struct A1Stage {
    paths: CasePaths,
    options: IndexFixtureOptions,
    replay_limits: ReplayLimits,
    spec: FixtureSpec,
    dataset: DatasetIdentity,
    shape_yaml: String,
    pending_receipt: Mutex<Option<BuildReceipt>>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // Boundary bookkeeping only: a panicked holder leaves plain values.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Durable small file: temporary, file sync, rename, directory sync.
fn publish(path: &Path, bytes: &[u8]) -> BackendResult<()> {
    let pending = path.with_extension("pending");
    let mut file = fs::File::create(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&pending, path)?;
    let parent = path.parent().ok_or("published file has no parent")?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn directory_is_empty(path: &Path) -> BackendResult<bool> {
    match fs::read_dir(path) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
    }
}

/// Open the fixture adapter for one `build` or `overwrite` stage.
// One admission sequence: receipt checks precede every owner start and write.
#[allow(clippy::too_many_lines)]
pub(super) fn open(config: &RunConfig) -> BackendResult<TidehunterBackend> {
    let mut options = super::parse_options(&config.backend.options)?;
    let fixture = options
        .index_fixture
        .take()
        .ok_or("index_fixture is missing")?;
    fixture.validate()?;
    let replay_limits = validate_route(&options)?;
    if fixture.arm == IndexArm::PerCell {
        return Err("index_fixture.arm=per_cell is unavailable: this revision rejects inactive PerCell checkpoints; the scheduler exists only on the exp/option-c test branch".into());
    }
    let dataset = DatasetIdentity::from_config(config)?;
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let shape_yaml = serde_yaml::to_string(&shape)?;
    let paths = CasePaths::new(&options);
    let tier = options.durability_tier;
    let (spec, label, kind) = match fixture.stage {
        IndexFixtureStage::Build => {
            if paths.fixture.exists()
                || !directory_is_empty(&paths.coordinator)?
                || paths.file(RECEIPT).exists()
            {
                return Err(
                    "index_fixture build needs a fresh case path; never reuse a prepared database"
                        .into(),
                );
            }
            let spec =
                fixture
                    .geometry(replay_limits)
                    .spec(paths.fixture.clone(), tier, fresh_owners(1)?);
            (spec, "owner-build", OpenKind::Create)
        }
        IndexFixtureStage::Overwrite => {
            let receipt: BuildReceipt =
                serde_json::from_slice(&fs::read(paths.file(RECEIPT)).map_err(|error| {
                    format!("index_fixture overwrite needs a build receipt: {error}")
                })?)?;
            receipt.check(&paths, &dataset, &shape_yaml, &fixture, replay_limits, tier)?;
            if paths.file(STOP_STATE).exists() {
                return Err("overwrite already ran on this prepared case; use a new prepared case for each repeat, pilot and attribution run".into());
            }
            (receipt.spec, "owner-overwrite", OpenKind::Reopen)
        }
    };
    fs::create_dir_all(&paths.coordinator)?;
    fs::create_dir_all(&paths.evidence)?;
    let owner = OwnerChild::spawn(&spec, 0, &paths.evidence, label)?;
    let remote_config = spec.remote_config()?;
    let owner_address = owner.address.to_string();
    let keyspace = shape
        .iter_ks()
        .next()
        .ok_or("empty fixture key shape")?
        .id();
    let open_start_ns = diagnostics_clock_ns();
    let db =
        crate::leased_fixture::open_coordinator(&paths.coordinator, shape, remote_config.clone())?;
    let open_record = OpenRecord {
        kind,
        open_start_ns,
        open_return_ns: diagnostics_clock_ns(),
        first_get_ns: Arc::default(),
        recovery: db.remote_recovery_stats(),
    };
    let soft = |value: usize| value / 4;
    let effective_configuration = json!({
        "backend": "tidehunter_leased_fixture",
        "route": "rootless leased ordinary-file fixture: loopback tcp-trusted control, real L2 OwnerHost, fake configfs trees, PerOwner export, buffered file backing, WAL-replay history",
        "durability_tier": tier.as_str(),
        "index_fixture": fixture,
        "fixture_spec": spec,
        "remote_config": remote_config,
        "database": db.remote_database_id().map(|id| tidehunter::distributed::tcp::host::identity::database_hex(&id)),
        "shape": "KeyShape::new_single(32, 16, KeyType::uniform(64))",
        "shape_yaml": shape_yaml,
        "owner_process": {"pid": owner.pid, "address": owner_address, "supervision": "child process; PR_SET_PDEATHSIG"},
        "replay_soft_triggers_derived": {
            "batches": soft(replay_limits.max_batches),
            "bytes": soft(replay_limits.max_encoded_bytes),
            "source": "one quarter of the configured envelope (remote_db/replay_limits.rs); not read back from the engine"
        },
        "features": ["test-utils"],
        "binary": std::env::current_exe().ok(),
        "owner_count": 1,
        "replication_factor": 1,
        "coordinator_path": paths.coordinator,
    });
    let state = FixtureState {
        run: FixtureRun::new(fixture.stage, dataset.keys)?,
        boundaries: Mutex::default(),
        crash_stop: AtomicBool::new(false),
        owners: Mutex::new(vec![owner]),
        stage: StageEnd::A1(A1Stage {
            paths: paths.clone(),
            options: fixture,
            replay_limits,
            spec,
            dataset,
            shape_yaml,
            pending_receipt: Mutex::new(None),
        }),
    };
    Ok(TidehunterBackend {
        owner_sample_gate: super::OwnerSampleGate::default(),
        db: Some(db),
        keyspace,
        endpoints: vec![(0, owner_address)],
        owners: 1,
        backend_name: "tidehunter_leased_fixture",
        durability_tier: tier,
        evidence_path: paths.evidence,
        effective_configuration,
        read_evidence_written: false,
        coordinator_interval_baseline: Mutex::new(None),
        coordinator_path: paths.coordinator,
        open_record,
        live_write_profile: Mutex::new(None),
        index_fixture: Some(state),
        local_owners: Vec::new(),
    })
}

/// Classification of one dataset row against its acceptable payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowCheck {
    /// The single expected payload.
    Correct,
    /// One of several acceptable payloads of an ID with an uncertain put.
    Ambiguous,
    Missing,
    Wrong,
}

/// Counts of one verification pass.
#[derive(Default)]
struct RowTally {
    correct: u64,
    ambiguous: u64,
    missing: u64,
    wrong: u64,
}

impl RowTally {
    fn mismatched(&self) -> u64 {
        self.missing + self.wrong
    }
}

/// One verification pass over `0..keys`: `check(id, value)` classifies each
/// row. Returns the tally and its evidence.
fn verify_rows(
    db: &sui::Db,
    keyspace: KeySpace,
    namespace: &[u8; 16],
    dataset: &DatasetIdentity,
    check: impl Fn(u64, Option<&[u8]>) -> BackendResult<RowCheck>,
) -> BackendResult<(RowTally, Value)> {
    let started = Instant::now();
    let mut tally = RowTally::default();
    let mut sample = Vec::new();
    let mut id = 0;
    while id < dataset.keys {
        let end = (id + VERIFY_CHUNK).min(dataset.keys);
        let keys: Vec<_> = (id..end)
            .map(|id| key_bytes(namespace, dataset.seed, id))
            .collect();
        for (row, result) in (id..end).zip(db.multi_get(keyspace, &keys)) {
            let value = result.map_err(|error| format!("verification read {row}: {error:?}"))?;
            let class = check(row, value.as_deref())?;
            match class {
                RowCheck::Correct => tally.correct += 1,
                RowCheck::Ambiguous => tally.ambiguous += 1,
                RowCheck::Missing => tally.missing += 1,
                RowCheck::Wrong => tally.wrong += 1,
            }
            if matches!(class, RowCheck::Missing | RowCheck::Wrong)
                && sample.len() < MISMATCH_SAMPLE
            {
                sample.push(json!({"id": row, "present": value.is_some()}));
            }
        }
        id = end;
    }
    let evidence = json!({
        "rows": dataset.keys,
        "correct": tally.correct,
        "ambiguous_accepted": tally.ambiguous,
        "missing": tally.missing,
        "wrong": tally.wrong,
        "mismatched": tally.mismatched(),
        "mismatch_sample": sample,
        "elapsed_ns": crate::duration_ns(started.elapsed()),
    });
    Ok((tally, evidence))
}

/// Exact comparison against one expected payload.
fn exact_row(expected: &[u8], value: Option<&[u8]>) -> RowCheck {
    match value {
        Some(value) if value == expected => RowCheck::Correct,
        Some(_) => RowCheck::Wrong,
        None => RowCheck::Missing,
    }
}

/// Rows the dataset keyspace iterates (the runner's qualification rows are
/// deleted again, so a complete run iterates exactly the dataset).
fn iterated_rows(db: &Arc<sui::Db>, keyspace: KeySpace) -> BackendResult<u64> {
    db.iterator(keyspace).try_fold(0_u64, |rows, row| {
        row.map(|_| rows + 1)
            .map_err(|error| format!("keyspace iteration: {error:?}").into())
    })
}

/// Wait (bounded) until no checkpoint worker runs; natural work only.
fn idle_snapshot(db: &sui::Db) -> BackendResult<IndexCheckpointSnapshot> {
    db.drain_index_checkpoints(false)
        .map_err(|error| format!("natural drain: {error:?}"))?;
    let deadline = Instant::now() + IDLE_TIMEOUT;
    loop {
        let snapshot = db
            .index_checkpoint_snapshot()
            .ok_or("index checkpoint snapshot unavailable")?;
        if !snapshot.active_worker {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err("checkpoint worker stayed active past the stop bound".into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl FixtureState {
    pub(super) fn record_boundary(&self, db: &sui::Db, open: bool) {
        let boundary = Boundary::capture(db);
        let mut boundaries = lock(&self.boundaries);
        if open {
            boundaries.start = Some(boundary);
        } else {
            boundaries.end = Some(boundary);
        }
    }

    pub(super) fn crash_stop(&self) -> bool {
        self.crash_stop.load(Ordering::Acquire)
    }

    fn set_crash_stop(&self) {
        // Release pairs with the Acquire in `crash_stop` at drop.
        self.crash_stop.store(true, Ordering::Release);
    }

    fn timed(&self) -> Value {
        let boundaries = lock(&self.boundaries);
        let (Some(start), Some(end)) = (&boundaries.start, &boundaries.end) else {
            return json!({"complete": false, "reason": "timed window boundaries missing"});
        };
        let coordinator = delta(start.coordinator_file_bytes, end.coordinator_file_bytes);
        let wal = delta(start.leased_bytes, end.leased_bytes);
        let published = delta(start.published_batches, end.published_batches);
        let successful = self.run.successful_puts();
        #[allow(clippy::cast_precision_loss)] // Ratio evidence; byte counts stay exact.
        let ratio = match (coordinator, wal) {
            (Some(coordinator), Some(wal)) if wal > 0 => Some(coordinator as f64 / wal as f64),
            _ => None,
        };
        json!({
            "complete": coordinator.is_some() && wal.is_some() && published.is_some(),
            "start": start,
            "end": end,
            "duration_ns": end.at_ns.saturating_sub(start.at_ns),
            "coordinator_file_bytes": coordinator,
            "owner_wal_data_frame_bytes": wal,
            "leased_frames": delta(start.leased_frames, end.leased_frames),
            "leased_skip_markers": delta(start.leased_skip_markers, end.leased_skip_markers),
            "published_batches": published,
            "admitted_batches": delta(start.admitted_batches, end.admitted_batches),
            "successful_puts": successful,
            "ack_matches_publication": published == Some(successful),
            "coordinator_bytes_per_owner_wal_byte": ratio,
            "numerator": "all coordinator file categories written in the timed window (producer counters), never a du delta",
            "denominator": "leased data-frame bytes including padding at successful completion release; skip markers and preallocation excluded",
        })
    }

    /// Stage end; runs on a blocking worker before shutdown.
    pub(super) fn finish(
        &self,
        db: &Arc<sui::Db>,
        keyspace: KeySpace,
        finish: &FixtureFinish,
    ) -> BackendResult<Value> {
        match &self.stage {
            StageEnd::A1(stage) => stage.finish(self, db, keyspace, finish),
            StageEnd::Q1(stage) => stage.finish(self, db, keyspace, finish),
        }
    }

    /// After the coordinator is closed or abandoned: stop the owners, then
    /// complete the route's close.
    pub(super) fn close(&self, coordinator_clean: bool) -> BackendResult<()> {
        let owners = std::mem::take(&mut *lock(&self.owners));
        let mut stopped = Ok(());
        for owner in owners {
            let result = owner.stop();
            if stopped.is_ok() {
                stopped = result;
            }
        }
        match &self.stage {
            StageEnd::A1(stage) => stage.close(coordinator_clean, stopped),
            StageEnd::Q1(stage) => stage.close(coordinator_clean, stopped),
        }
    }
}

impl A1Stage {
    fn finish(
        &self,
        common: &FixtureState,
        db: &Arc<sui::Db>,
        keyspace: KeySpace,
        finish: &FixtureFinish,
    ) -> BackendResult<Value> {
        let result = match common.run.stage {
            IndexFixtureStage::Build => self.finish_build(common, db, keyspace, finish),
            IndexFixtureStage::Overwrite => self.finish_overwrite(common, db, finish),
        };
        let evidence = match &result {
            Ok(value) => value.clone(),
            Err(error) => {
                json!({"status": "failed", "stage": common.run.stage, "error": error.to_string()})
            }
        };
        fs::write(
            finish.output.join(STAGE_OUTPUT),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        result
    }

    fn finish_build(
        &self,
        common: &FixtureState,
        db: &Arc<sui::Db>,
        keyspace: KeySpace,
        finish: &FixtureFinish,
    ) -> BackendResult<Value> {
        if !finish.measurement_ok {
            return Err("build measurement did not drain cleanly; no receipt".into());
        }
        let namespace = self.dataset.namespace()?;
        if namespace != finish.namespace {
            return Err("run namespace differs from the fixture dataset".into());
        }
        let (tally, verification) =
            verify_rows(db, keyspace, &namespace, &self.dataset, |id, value| {
                Ok(exact_row(
                    &self.dataset.value(ValueDomain::Build, id)?,
                    value,
                ))
            })?;
        let iterated_rows = iterated_rows(db, keyspace)?;
        let successful = common.run.successful_puts();
        if tally.mismatched() != 0
            || successful != self.dataset.keys
            || iterated_rows < self.dataset.keys
        {
            return Err(format!(
                "build verification failed: {successful} successful puts, {} mismatched rows, {iterated_rows} iterated rows for {} keys",
                tally.mismatched(),
                self.dataset.keys
            )
            .into());
        }
        // Benchmark-only explicit engine barrier: one adopted root, marked
        // `harness_drain`; never typed-store `flush`.
        db.drain_index_checkpoints(true)
            .map_err(|error| format!("explicit build checkpoint: {error:?}"))?;
        let checkpoint = db.index_checkpoint_snapshot();
        let owner = db
            .owner_storage_stats()
            .map_err(|error| format!("owner progress: {error:?}"))?;
        let verification = json!({
            "dataset_rows": verification,
            "iterated_rows": iterated_rows,
            "other_rows": iterated_rows - self.dataset.keys,
            "other_rows_semantics": "qualification keys of the runner, outside the dataset namespace",
            "successful_puts": successful,
            "checkpoint_after_explicit_root": checkpoint,
            "owner_progress": owner,
        });
        *lock(&self.pending_receipt) = Some(BuildReceipt {
            schema_version: 1,
            path: self.paths.root.clone(),
            dataset: self.dataset.clone(),
            shape_yaml: self.shape_yaml.clone(),
            fixture: self.options.clone(),
            replay_limits: self.replay_limits,
            spec: self.spec.clone(),
            build_case_id: finish.case_id.clone(),
            verification: verification.clone(),
        });
        Ok(json!({
            "status": "verified",
            "stage": "build",
            "receipt": self.paths.file(RECEIPT),
            "receipt_written": "after a clean close of coordinator and owner",
            "verification": verification,
        }))
    }

    fn finish_overwrite(
        &self,
        common: &FixtureState,
        db: &Arc<sui::Db>,
        finish: &FixtureFinish,
    ) -> BackendResult<Value> {
        // Natural work only: no forced root before the crash-style stop.
        let snapshot = idle_snapshot(db)?;
        let counters = db.remote_counters().map(|(_, stats, _)| stats);
        let owner = db
            .owner_storage_stats()
            .map_err(|error| format!("owner progress: {error:?}"))?;
        let touched = common.run.touched();
        publish(&self.paths.file(TOUCHED), &touched.to_bytes())?;
        let timed = common.timed();
        let stop = json!({
            "schema_version": 1,
            "stage": "overwrite",
            "case_id": finish.case_id,
            "measurement_ok": finish.measurement_ok,
            "recorded_at_ns": diagnostics_clock_ns(),
            "dataset": self.dataset,
            "stopped": {
                "fencing_generation": snapshot.fencing_generation,
                "snapshot_generation": snapshot.snapshot_generation,
                "suffix_batches": snapshot.suffix.batches,
                "suffix_bytes": snapshot.suffix.bytes,
                "suffix_oldest_age_ns": snapshot.suffix.oldest_age_ns,
                "published_batches_this_open": counters.as_ref().map(|stats| stats.published_batches),
                "admitted_batches_this_open": counters.as_ref().map(|stats| stats.admitted_batches),
                "semantics": "suffix = contiguous published batches and canonical encoded bytes after the adopted root; the cold reopen must replay exactly this tail",
            },
            "snapshot": snapshot,
            "owner_progress_before_stop": owner,
            "timed": timed,
            "touched": {
                "count": touched.count(),
                "first_read_id": touched.first(),
                "file": TOUCHED,
            },
            "coordinator_stop": "crash-style: no shutdown root; the handle is abandoned until process exit",
        });
        publish(
            &self.paths.file(STOP_STATE),
            &serde_json::to_vec_pretty(&stop)?,
        )?;
        common.set_crash_stop();
        if !finish.measurement_ok {
            return Err("overwrite measurement did not drain cleanly; stop record kept".into());
        }
        Ok(stop)
    }

    /// Publish a pending build receipt if everything closed cleanly.
    fn close(&self, coordinator_clean: bool, owners: BackendResult<()>) -> BackendResult<()> {
        let receipt = lock(&self.pending_receipt).take();
        owners?;
        if let Some(receipt) = receipt {
            if !coordinator_clean {
                return Err("coordinator close failed; build receipt withheld".into());
            }
            publish(
                &self.paths.file(RECEIPT),
                &serde_json::to_vec_pretty(&receipt)?,
            )?;
        }
        Ok(())
    }
}

/// Expected final payload of `id` after an A1 overwrite stage.
fn final_value(
    dataset: &DatasetIdentity,
    touched: &TouchedBitmap,
    id: u64,
) -> BackendResult<tidehunter::minibytes::Bytes> {
    let domain = if touched.is_set(id)? {
        ValueDomain::Timed
    } else {
        ValueDomain::Build
    };
    dataset.value(domain, id)
}
