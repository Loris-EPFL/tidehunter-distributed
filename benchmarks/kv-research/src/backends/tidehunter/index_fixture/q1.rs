//! Q1 staged two-OwnerHost index runner (producer of Q1 index evidence):
//!
//! `kv-research --q1-index-stage STAGE.json --phase <build|pilot|overwrite|cold-reopen|verify> --output FRESH`
//! and, on an owner guest, `--phase owner-observe --partition N`.
//!
//! One coordinator (this process) and exactly two L2 OwnerHosts, partitions
//! 0 and 1. Fleet mode: owners are separately supervised processes
//! (`--experimental-storage-node` host launch) reached over TCP/mTLS control
//! and kernel NVMe-oF `per_owner` data; this process never starts them. Local
//! proof mode: two supervised ordinary-file fixture owner children; its
//! receipts name the mode and never qualify a fleet cell.
//!
//! Phases, in order, on one retained database (no inter-phase reset):
//! - `build`: the engine provisions the database; exactly `keys` puts at IDs
//!   `0..keys`; untimed multi-get of every row and an iterator count; one
//!   labeled forced root (`harness_drain`, never counted as a normal
//!   install); clean close of both ends.
//! - `pilot`: closed-loop pilot overwrites (pilot value domain); the qualified
//!   rate is the median complete-second success rate; open-loop confirmation
//!   at floor(0.70 x rate), and only if that is unstable one confirmation at
//!   floor(0.56 x rate) (the prebudgeted 0.8 fallback). Pilot outputs never
//!   count toward qualification.
//! - `overwrite`: timed open-loop overwrites at the selected rate (timed value
//!   domain); admissions close, in-flight calls finish, natural checkpoint
//!   work drains; the stop record names the published frontier, the replay
//!   suffix and per-owner progress; then a crash-style coordinator stop with
//!   no forcing root.
//! - `cold-reopen`: coordinator and owner pages evicted and measured on their
//!   own guests; fresh processes; stopwatch from coordinator open (owners
//!   ready) to the first correct read of the lowest timed-acknowledged ID;
//!   recovery must replay exactly the stopped suffix to the stopped frontier.
//! - `verify`: every dataset row against its final value domain (timed, then
//!   pilot, then build; uncertain puts admit their prior value), no extra
//!   rows, and the owner shutdown/target inventory receipts.
//!
//! Every phase writes `<phase>-receipt.json` (schema version 1) to its
//! output and to `<case_root>/receipts/`, binding the stage input SHA-256,
//! the case, the executable SHA-256, every predecessor receipt SHA-256, the
//! engine-provisioned database identity, the owner identities, the terminal
//! status and every artifact's length and SHA-256.

mod evidence;
mod input;
mod recovery;
#[cfg(test)]
mod tests;

use self::input::{RunKind, StageInput, StageMode};
use self::recovery::{cold_reopen, owner_observe, verify};
use super::super::TidehunterBackend;
use super::{
    DatasetIdentity, FixtureState, StageEnd, idle_snapshot, iterated_rows, lock, publish,
    verify_rows,
};
use crate::backend::BackendResult;
use crate::index_a1::{FixtureFinish, FixtureRun, IndexFixtureStage, TouchedBitmap, ValueDomain};
use crate::index_checkpoint::{OpenKind, OpenRecord};
use crate::leased_fixture::{FixtureSpec, OwnerChild, fresh_host_id};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tidehunter::distributed::remote_db::{RemoteDatabaseConfig, diagnostics_clock_ns};
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::host::identity::database_hex;
use tidehunter::distributed::types::MutationVersion;
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};

const RECEIPTS: &str = "receipts";
const OWNERS: &str = "owners";
const EVIDENCE: &str = "evidence";
const FIXTURE_SPEC: &str = "spec.json";
const PILOT_TOUCHED: &str = "pilot-touched.bin";
const PILOT_UNCERTAIN: &str = "pilot-uncertain.bin";
const TIMED_TOUCHED: &str = "timed-touched.bin";
const TIMED_UNCERTAIN: &str = "timed-uncertain.bin";
const OVERWRITE_STOP: &str = "overwrite-stop.json";
const WRITE_DIAGNOSTICS: &str = "write-diagnostics.json";
const OWNER_OBSERVE: &str = "owner-observe-receipt.json";
/// Pilot rate fractions in percent: primary, then the one prebudgeted
/// fallback (0.8 x 0.70).
const PILOT_PRIMARY_PERCENT: u64 = 70;
const PILOT_FALLBACK_PERCENT: u64 = 56;

/// One phase of the staged runner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Build,
    Pilot,
    Overwrite,
    ColdReopen,
    Verify,
    /// Owner guest only: allocation, database inventory, eviction and
    /// residency of one owner host root.
    OwnerObserve,
}

impl Phase {
    pub(super) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "build" => Self::Build,
            "pilot" => Self::Pilot,
            "overwrite" => Self::Overwrite,
            "cold-reopen" => Self::ColdReopen,
            "verify" => Self::Verify,
            "owner-observe" => Self::OwnerObserve,
            _ => return None,
        })
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Pilot => "pilot",
            Self::Overwrite => "overwrite",
            Self::ColdReopen => "cold-reopen",
            Self::Verify => "verify",
            Self::OwnerObserve => "owner-observe",
        }
    }

    /// The coordinator phase that must have completed first.
    pub(super) const fn predecessor(self) -> Option<Self> {
        match self {
            Self::Build | Self::OwnerObserve => None,
            Self::Pilot => Some(Self::Build),
            Self::Overwrite => Some(Self::Pilot),
            Self::ColdReopen => Some(Self::Overwrite),
            Self::Verify => Some(Self::ColdReopen),
        }
    }

    fn receipt_file(self) -> String {
        format!("{}-receipt.json", self.name())
    }
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn file_sha256(path: &Path) -> BackendResult<(u64, String)> {
    let bytes = fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok((u64::try_from(bytes.len())?, sha256_hex(&bytes)))
}

fn frontier_json(frontier: Option<MutationVersion>) -> Value {
    frontier.map_or(Value::Null, |version| {
        json!({
            "fencing_generation": version.batch.epoch,
            "sequence": version.batch.sequence,
            "operation_index": version.operation_index,
        })
    })
}

fn parse_database(hex: &str) -> BackendResult<[u8; 16]> {
    let digits = hex.as_bytes();
    if digits.len() != 32 {
        return Err("database identity must have 32 hex digits".into());
    }
    let mut database = [0; 16];
    for (byte, pair) in database.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(database)
}

/// The sealed stage, its byte digest and the executable digest.
struct Context {
    stage: StageInput,
    stage_sha256: String,
    executable_sha256: String,
    phase: Phase,
    output: PathBuf,
}

impl Context {
    fn registry(&self) -> PathBuf {
        self.stage.case_root.join(RECEIPTS)
    }

    fn registered(&self, name: &str) -> PathBuf {
        self.registry().join(name)
    }

    /// A completed predecessor receipt and its SHA-256.
    fn receipt(&self, phase: Phase) -> BackendResult<(Value, String)> {
        let path = self.registered(&phase.receipt_file());
        let (_, sha) = file_sha256(&path)?;
        let receipt = evidence::read_json(&path)?;
        if receipt["status"] != "complete"
            || receipt["input_sha256"] != self.stage_sha256.as_str()
            || receipt["case_id"] != self.stage.case_id.as_str()
            || receipt["phase"] != phase.name()
        {
            return Err(format!(
                "{} receipt is not a complete receipt of this stage input",
                phase.name()
            )
            .into());
        }
        Ok((receipt, sha))
    }

    fn database(&self) -> BackendResult<[u8; 16]> {
        let (build, _) = self.receipt(Phase::Build)?;
        parse_database(
            build["database_id"]
                .as_str()
                .ok_or("build receipt names no database")?,
        )
    }

    /// The registered bitmap `name` of the dataset.
    fn bitmap(&self, name: &str) -> BackendResult<TouchedBitmap> {
        TouchedBitmap::from_bytes(self.stage.index.keys, &fs::read(self.registered(name))?)
    }

    /// The local proof fixture written by `build`, checked against the stage.
    fn local_spec(&self) -> BackendResult<FixtureSpec> {
        let spec: FixtureSpec =
            serde_json::from_slice(&fs::read(self.stage.fixture_root().join(FIXTURE_SPEC))?)?;
        if spec != self.stage.local_spec(spec.host_id)? {
            return Err("local fixture spec differs from the stage input".into());
        }
        Ok(spec)
    }

    fn evidence_dir(&self, label: &str) -> PathBuf {
        self.stage
            .case_root
            .join(EVIDENCE)
            .join(format!("{}-{label}", self.phase.name()))
    }
}

/// `kv-research --q1-index-stage STAGE --phase PHASE [--partition N] --output FRESH`.
pub fn run(stage: &Path, phase: &str, partition: Option<u32>, output: &Path) -> BackendResult<()> {
    let phase = Phase::parse(phase)
        .ok_or("--phase must be build, pilot, overwrite, cold-reopen, verify or owner-observe")?;
    if (phase == Phase::OwnerObserve) != partition.is_some() {
        return Err("--partition is required by owner-observe and taken by no other phase".into());
    }
    let bytes = fs::read(stage).map_err(|error| format!("{}: {error}", stage.display()))?;
    let input: StageInput = serde_json::from_slice(&bytes)?;
    input.validate()?;
    fs::create_dir_all(output)?;
    if fs::read_dir(output)?.next().is_some() {
        return Err("--output must be a fresh directory".into());
    }
    let context = Context {
        stage_sha256: sha256_hex(&bytes),
        executable_sha256: file_sha256(&std::env::current_exe()?)?.1,
        stage: input,
        phase,
        output: output.to_path_buf(),
    };
    if let Some(partition) = partition {
        return owner_observe(&context, partition);
    }
    begin(&context)?;
    let outcome = match phase {
        Phase::Build => build(&context),
        Phase::Pilot => pilot(&context),
        Phase::Overwrite => overwrite(&context),
        Phase::ColdReopen => cold_reopen(&context),
        Phase::Verify => verify(&context),
        Phase::OwnerObserve => Err("owner-observe takes --partition".into()),
    };
    let report = outcome.unwrap_or_else(|error| PhaseReport::failed(error.to_string()));
    let complete = report.status == Status::Complete;
    let error = report.error.clone();
    finish(&context, report)?;
    if complete {
        Ok(())
    } else {
        Err(format!(
            "{} phase did not complete: {}",
            phase.name(),
            error.as_deref().unwrap_or("see the receipt")
        )
        .into())
    }
}

/// Refuse a phase whose predecessor did not complete, that ran before, or
/// whose case was already started; then mark it started (durably), so a
/// crashed phase can never run again on changed data.
fn begin(context: &Context) -> BackendResult<()> {
    if let Some(predecessor) = context.phase.predecessor() {
        context.receipt(predecessor)?;
    }
    fs::create_dir_all(context.registry())?;
    let marker = context.registered(&format!("{}.started", context.phase.name()));
    if marker.exists() || context.registered(&context.phase.receipt_file()).exists() {
        return Err(format!(
            "{} already started on this case; a phase never reruns on retained data",
            context.phase.name()
        )
        .into());
    }
    publish(
        &marker,
        serde_json::to_vec(&json!({"phase": context.phase.name(),
            "input_sha256": context.stage_sha256, "started_unix_ns": unix_ns()}))?
        .as_slice(),
    )
}

fn unix_ns() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Complete,
    Failed,
}

/// What a phase produced; the common receipt envelope is added by `finish`.
struct PhaseReport {
    status: Status,
    error: Option<String>,
    database: Option<[u8; 16]>,
    owners: Value,
    predecessors: Vec<Value>,
    terminal: Value,
    /// `(name, bytes)` written to the output and the registry.
    artifacts: Vec<(String, Vec<u8>)>,
    /// Files already in the output, listed with length and SHA-256.
    output_files: Vec<PathBuf>,
}

impl PhaseReport {
    fn failed(error: String) -> Self {
        Self {
            status: Status::Failed,
            error: Some(error),
            database: None,
            owners: Value::Null,
            predecessors: Vec::new(),
            terminal: Value::Null,
            artifacts: Vec::new(),
            output_files: Vec::new(),
        }
    }
}

/// Write artifacts and the receipt to the output and the registry.
fn finish(context: &Context, report: PhaseReport) -> BackendResult<()> {
    let mut listed = Vec::with_capacity(report.artifacts.len() + report.output_files.len());
    for (name, bytes) in &report.artifacts {
        publish(&context.output.join(name), bytes)?;
        publish(&context.registered(name), bytes)?;
        listed.push(json!({"file": name, "bytes": bytes.len(), "sha256": sha256_hex(bytes)}));
    }
    for path in &report.output_files {
        let (bytes, sha) = file_sha256(path)?;
        let name = path.strip_prefix(&context.output).unwrap_or(path);
        listed.push(json!({"file": name, "bytes": bytes, "sha256": sha}));
    }
    let receipt = json!({
        "schema_version": 1,
        "kind": "q1_index_stage_receipt",
        "phase": context.phase.name(),
        "mode": context.stage.mode,
        "case_id": context.stage.case_id,
        "input_sha256": context.stage_sha256,
        "executable_sha256": context.executable_sha256,
        "predecessors": report.predecessors,
        "database_id": report.database.as_ref().map(database_hex),
        "owner_roots": context.stage.index.owner_roots,
        "owners": report.owners,
        "status": match report.status { Status::Complete => "complete", Status::Failed => "failed" },
        "error": report.error,
        "terminal": report.terminal,
        "artifacts": listed,
        "recorded_unix_ns": unix_ns(),
    });
    let bytes = serde_json::to_vec_pretty(&receipt)?;
    let name = context.phase.receipt_file();
    publish(&context.output.join(&name), &bytes)?;
    if context.phase != Phase::OwnerObserve {
        publish(&context.registered(&name), &bytes)?;
    }
    Ok(())
}

fn predecessor_entry(phase: Phase, sha: &str) -> Value {
    json!({"phase": phase.name(), "file": phase.receipt_file(), "sha256": sha})
}

/// Predecessor receipts of a phase: every completed coordinator phase before it.
fn predecessors(context: &Context) -> BackendResult<Vec<Value>> {
    let mut entries = Vec::new();
    let mut phase = context.phase.predecessor();
    while let Some(current) = phase {
        let (_, sha) = context.receipt(current)?;
        entries.push(predecessor_entry(current, &sha));
        phase = current.predecessor();
    }
    entries.reverse();
    Ok(entries)
}

// --- Owners -----------------------------------------------------------------

/// Owner processes of one open: supervised fixture children (local proof)
/// or the receipts of the externally supervised fleet owner hosts.
struct Owners {
    children: Vec<OwnerChild>,
    evidence: Value,
}

impl Owners {
    fn start(context: &Context, spec: Option<&FixtureSpec>, label: &str) -> BackendResult<Self> {
        match (context.stage.mode, spec) {
            (StageMode::LocalFixtureProof, Some(spec)) => {
                let evidence_dir = context.evidence_dir(label);
                let mut children = Vec::with_capacity(spec.owners.len());
                let mut evidence = Vec::with_capacity(spec.owners.len());
                for owner in &spec.owners {
                    let child = OwnerChild::spawn(
                        spec,
                        owner.partition,
                        &evidence_dir,
                        &format!("owner-p{}", owner.partition),
                    )?;
                    let (bytes, sha) = file_sha256(&child.receipt_file)?;
                    evidence.push(json!({
                        "partition": owner.partition,
                        "address": child.address.to_string(),
                        "root": spec.owner_root(owner.partition),
                        "route": "local ordinary-file fixture owner child",
                        "start_receipt": evidence::read_json(&child.receipt_file)?,
                        "start_receipt_file": child.receipt_file,
                        "start_receipt_bytes": bytes,
                        "start_receipt_sha256": sha,
                    }));
                    children.push(child);
                }
                Ok(Self {
                    children,
                    evidence: Value::Array(evidence),
                })
            }
            (StageMode::Fleet, None) => Ok(Self {
                children: Vec::new(),
                evidence: fleet_owner_receipts(context)?,
            }),
            _ => Err("owner route does not match the stage mode".into()),
        }
    }

    fn stop(self) -> BackendResult<()> {
        let mut result = Ok(());
        for child in self.children {
            let stopped = child.stop();
            if result.is_ok() {
                result = stopped;
            }
        }
        result
    }
}

/// The current start receipt of each fleet owner host
/// (`<case_root>/owners/p<N>.receipt.json`, published by the owner before
/// readiness and copied by the controller): it must name its partition, the
/// stage address and the stage tier.
fn fleet_owner_receipts(context: &Context) -> BackendResult<Value> {
    let mut owners = Vec::with_capacity(context.stage.owners.len());
    for owner in &context.stage.owners {
        let path = context
            .stage
            .case_root
            .join(OWNERS)
            .join(format!("p{}.receipt.json", owner.partition));
        let receipt = super::super::tcp_cluster::read_owner_receipt(&path)
            .map_err(|error| error.to_string())?;
        if receipt.partition != owner.partition
            || receipt.address != owner.address
            || receipt.launch_nonce.is_empty()
            || receipt.durability.tier != context.stage.index.tier
        {
            return Err(format!(
                "owner {} start receipt does not name its partition, stage address and tier",
                owner.partition
            )
            .into());
        }
        let (bytes, sha) = file_sha256(&path)?;
        owners.push(json!({
            "partition": owner.partition,
            "address": owner.address,
            "root": owner.root,
            "route": "external L2 OwnerHost (host launch)",
            "start_receipt": receipt,
            "start_receipt_file": path,
            "start_receipt_bytes": bytes,
            "start_receipt_sha256": sha,
        }));
    }
    Ok(Value::Array(owners))
}

/// The coordinator configuration of this route.
fn coordinator_config(
    context: &Context,
    spec: Option<&FixtureSpec>,
) -> BackendResult<RemoteDatabaseConfig> {
    match (context.stage.mode, spec, &context.stage.remote) {
        (StageMode::LocalFixtureProof, Some(spec), None) => spec.remote_config(),
        (StageMode::Fleet, None, Some(remote)) => Ok(remote.clone()),
        _ => Err("coordinator route does not match the stage mode".into()),
    }
}

fn shape() -> KeyShape {
    KeyShape::new_single(32, 16, KeyType::uniform(64))
}

fn open_coordinator(
    context: &Context,
    config: RemoteDatabaseConfig,
) -> BackendResult<Arc<sui::Db>> {
    let path = context.stage.coordinator_path();
    fs::create_dir_all(&path)?;
    match context.stage.mode {
        StageMode::LocalFixtureProof => {
            crate::leased_fixture::open_coordinator(&path, shape(), config)
        }
        StageMode::Fleet => sui::Db::open_remote(&path, shape(), config)
            .map_err(|error| format!("{error:?}").into()),
    }
}

/// The opened database must be the one `build` provisioned.
fn check_database(db: &sui::Db, expected: Option<[u8; 16]>) -> BackendResult<[u8; 16]> {
    let database = db
        .remote_database_id()
        .ok_or("coordinator has no provisioned database")?;
    if expected.is_some_and(|expected| expected != database) {
        return Err(format!(
            "opened database {} is not the build database",
            database_hex(&database)
        )
        .into());
    }
    Ok(database)
}

fn keyspace() -> BackendResult<KeySpace> {
    Ok(shape().iter_ks().next().ok_or("empty Q1 key shape")?.id())
}

fn owner_stats(db: &sui::Db) -> Value {
    match db.owner_storage_stats_by_partition() {
        Ok(Some(stats)) => json!(
            stats
                .into_iter()
                .map(|(partition, stats)| json!({"partition": partition, "stats": stats}))
                .collect::<Vec<_>>()
        ),
        Ok(None) => Value::Null,
        Err(error) => json!({"error": format!("{error:?}")}),
    }
}

// --- Stage end inside the runner ---------------------------------------------

/// What the stage end and close of one runner invocation observed.
#[derive(Default)]
pub(super) struct StageOutcome {
    finish: Option<Result<Value, String>>,
    touched: Vec<u8>,
    uncertain: Vec<u8>,
    successful_puts: u64,
    close: Option<Result<(), String>>,
    coordinator_clean: bool,
}

/// The Q1 stage end of one runner invocation.
pub(super) struct Q1Stage {
    run: RunKind,
    dataset: DatasetIdentity,
    outcome: Arc<Mutex<StageOutcome>>,
}

impl Q1Stage {
    pub(super) fn finish(
        &self,
        common: &FixtureState,
        db: &Arc<sui::Db>,
        keyspace: KeySpace,
        finish: &FixtureFinish,
    ) -> BackendResult<Value> {
        let namespace = self.dataset.namespace();
        let result = if !finish.measurement_ok {
            Err("measurement did not drain cleanly".into())
        } else if namespace.as_ref().ok() != Some(&finish.namespace) {
            Err("run namespace differs from the stage dataset".into())
        } else {
            match self.run {
                RunKind::Build => self.finish_build(common, db, keyspace),
                RunKind::PilotClosed | RunKind::PilotConfirm(_) => {
                    idle_snapshot(db).map(|snapshot| {
                        json!({"snapshot": snapshot, "timed": common.timed(),
                            "owner_progress": owner_stats(db)})
                    })
                }
                RunKind::Overwrite(_) => self.finish_overwrite(common, db),
            }
        };
        let mut outcome = lock(&self.outcome);
        outcome.touched = common.run.touched().to_bytes();
        outcome.uncertain = common.run.uncertain().to_bytes();
        outcome.successful_puts = common.run.successful_puts();
        outcome.finish = Some(
            result
                .as_ref()
                .map(Clone::clone)
                .map_err(ToString::to_string),
        );
        result
    }

    fn finish_build(
        &self,
        common: &FixtureState,
        db: &Arc<sui::Db>,
        keyspace: KeySpace,
    ) -> BackendResult<Value> {
        let namespace = self.dataset.namespace()?;
        let (tally, rows) = verify_rows(db, keyspace, &namespace, &self.dataset, |id, value| {
            Ok(super::exact_row(
                &self.dataset.value(ValueDomain::Build, id)?,
                value,
            ))
        })?;
        let iterated = iterated_rows(db, keyspace)?;
        let frontier = db
            .published_frontier()
            .map_err(|error| format!("published frontier: {error:?}"))?;
        // Benchmark-forced root, labeled `harness_drain`; never a normal install.
        db.drain_index_checkpoints(true)
            .map_err(|error| format!("explicit build checkpoint: {error:?}"))?;
        let snapshot = db
            .index_checkpoint_snapshot()
            .ok_or("index checkpoint snapshot unavailable")?;
        Ok(json!({
            "rows": rows,
            "distinct_rows": tally.correct,
            "iterated_rows": iterated,
            "successful": common.run.successful_puts(),
            "frontier_before_forced_root": frontier_json(frontier),
            "forced_root": {"trigger": "harness_drain", "counts_as_normal_install": false,
                "suffix_batches_after": snapshot.suffix.batches},
            "snapshot_after_forced_root": snapshot,
            "owner_progress": owner_stats(db),
        }))
    }

    fn finish_overwrite(&self, common: &FixtureState, db: &Arc<sui::Db>) -> BackendResult<Value> {
        // Admissions closed and in-flight calls finished (the runner drained
        // the phase); natural checkpoint work only, no forcing root.
        let snapshot = idle_snapshot(db)?;
        let frontier = db
            .published_frontier()
            .map_err(|error| format!("published frontier: {error:?}"))?;
        let counters = db.remote_counters().map(|(_, stats, _)| stats);
        let touched = common.run.touched();
        let stop = json!({
            "frontier": frontier_json(frontier),
            "suffix_batches": snapshot.suffix.batches,
            "suffix_bytes": snapshot.suffix.bytes,
            "suffix_oldest_age_ns": snapshot.suffix.oldest_age_ns,
            "touched_key": touched.first(),
            "touched_count": touched.count(),
            "forced_root": false,
            "fencing_generation": snapshot.fencing_generation,
            "snapshot_generation": snapshot.snapshot_generation,
            "published_batches_this_open": counters.as_ref().map(|stats| stats.published_batches),
            "admitted_batches_this_open": counters.as_ref().map(|stats| stats.admitted_batches),
            "snapshot": snapshot,
            "owner_progress": owner_stats(db),
            "timed": common.timed(),
            "recorded_at_ns": diagnostics_clock_ns(),
            "coordinator_stop": "crash-style: no shutdown root; the handle is abandoned until process exit",
            "semantics": "suffix = contiguous published batches and canonical encoded bytes after the adopted Exact root; the cold reopen must replay exactly this tail to this frontier",
        });
        common.set_crash_stop();
        Ok(stop)
    }

    pub(super) fn close(
        &self,
        coordinator_clean: bool,
        owners: BackendResult<()>,
    ) -> BackendResult<()> {
        let mut outcome = lock(&self.outcome);
        outcome.coordinator_clean = coordinator_clean;
        outcome.close = Some(owners.as_ref().copied().map_err(ToString::to_string));
        owners
    }
}

/// Open the Q1 adapter for one runner invocation.
fn open_backend(
    context: &Context,
    spec: Option<&FixtureSpec>,
    run: RunKind,
    expected: Option<[u8; 16]>,
    label: &str,
    outcome: Arc<Mutex<StageOutcome>>,
) -> BackendResult<(TidehunterBackend, Value, [u8; 16])> {
    let stage = &context.stage;
    let evidence_path = context.evidence_dir(label);
    fs::create_dir_all(&evidence_path)?;
    let owners = Owners::start(context, spec, label)?;
    let config = coordinator_config(context, spec)?;
    let kind = if run == RunKind::Build {
        OpenKind::Create
    } else {
        OpenKind::Reopen
    };
    let open_start_ns = diagnostics_clock_ns();
    let db = open_coordinator(context, config.clone())?;
    let database = check_database(&db, expected)?;
    let open_record = OpenRecord {
        kind,
        open_start_ns,
        open_return_ns: diagnostics_clock_ns(),
        first_get_ns: Arc::default(),
        recovery: db.remote_recovery_stats(),
    };
    let (fixture_stage, domain) = match run {
        RunKind::Build => (IndexFixtureStage::Build, ValueDomain::Build),
        RunKind::PilotClosed | RunKind::PilotConfirm(_) => {
            (IndexFixtureStage::Overwrite, ValueDomain::Pilot)
        }
        RunKind::Overwrite(_) => (IndexFixtureStage::Overwrite, ValueDomain::Timed),
    };
    let tier = stage.index.tier;
    let effective_configuration = json!({
        "backend": "tidehunter_q1_index_stage",
        "mode": stage.mode,
        "route": match stage.mode {
            StageMode::Fleet => "two external L2 OwnerHosts: TCP/mTLS control, kernel NVMe-oF per_owner leased data, WAL-replay history",
            StageMode::LocalFixtureProof => "local proof: two ordinary-file fixture OwnerHost children, loopback tcp-trusted control, PerOwner export, buffered file backing; never a fleet route",
        },
        "durability_tier": tier.as_str(),
        "run": run,
        "value_domain": domain,
        "remote_config": config,
        "database": database_hex(&database),
        "owners": owners.evidence,
        "shape": "KeyShape::new_single(32, 16, KeyType::uniform(64))",
        "owner_count": 2,
        "replication_factor": 1,
        "coordinator_path": stage.coordinator_path(),
    });
    let owner_evidence = owners.evidence.clone();
    let state = FixtureState {
        run: FixtureRun::with_domain(fixture_stage, domain, stage.index.keys)?,
        boundaries: Mutex::default(),
        crash_stop: AtomicBool::new(false),
        owners: Mutex::new(owners.children),
        stage: StageEnd::Q1(Q1Stage {
            run,
            dataset: stage.dataset_identity(),
            outcome,
        }),
    };
    let backend = TidehunterBackend {
        owner_sample_gate: super::super::OwnerSampleGate::default(),
        db: Some(db),
        keyspace: keyspace()?,
        endpoints: config
            .endpoints
            .iter()
            .map(|endpoint| (endpoint.partition, endpoint.address.clone()))
            .collect(),
        owners: 2,
        backend_name: "tidehunter_q1_index_stage",
        durability_tier: tier,
        evidence_path,
        effective_configuration,
        read_evidence_written: false,
        coordinator_interval_baseline: Mutex::new(None),
        coordinator_path: stage.coordinator_path(),
        open_record,
        live_write_profile: Mutex::new(None),
        index_fixture: Some(state),
        local_owners: Vec::new(),
    };
    Ok((backend, owner_evidence, database))
}

/// One runner invocation and what it left behind.
struct Invocation {
    run_dir: PathBuf,
    runner_error: Option<String>,
    outcome: StageOutcome,
    owners: Value,
    /// The database the coordinator opened (provisioned by `build`).
    database: [u8; 16],
    memory_start: Value,
    memory_end: Value,
}

impl Invocation {
    fn finish_value(&self) -> Option<&Value> {
        self.outcome
            .finish
            .as_ref()
            .and_then(|result| result.as_ref().ok())
    }

    /// Runner, stage end, owner stop and coordinator close all succeeded.
    fn clean(&self) -> bool {
        self.runner_error.is_none()
            && self.finish_value().is_some()
            && matches!(self.outcome.close, Some(Ok(())))
            && self.outcome.coordinator_clean
    }

    fn json(&self) -> Value {
        json!({
            "run_dir": self.run_dir,
            "runner_error": self.runner_error,
            "stage_end": match &self.outcome.finish {
                Some(Ok(value)) => value.clone(),
                Some(Err(error)) => json!({"error": error}),
                None => Value::Null,
            },
            "successful_puts": self.outcome.successful_puts,
            "owner_close": match &self.outcome.close {
                Some(Ok(())) => json!("ok"),
                Some(Err(error)) => json!({"error": error}),
                None => Value::Null,
            },
            "coordinator_clean": self.outcome.coordinator_clean,
            "memory_start": self.memory_start,
            "memory_end": self.memory_end,
        })
    }
}

fn invoke(
    context: &Context,
    runtime: &tokio::runtime::Runtime,
    spec: Option<&FixtureSpec>,
    run: RunKind,
    expected: Option<[u8; 16]>,
    label: &str,
) -> BackendResult<Invocation> {
    let config = context.stage.run_config(run)?;
    let run_dir = context.output.join(format!("run-{label}"));
    let outcome = Arc::new(Mutex::new(StageOutcome::default()));
    let memory_start = evidence::memory();
    let (backend, owners, database) =
        open_backend(context, spec, run, expected, label, Arc::clone(&outcome))?;
    let result = runtime.block_on(crate::runner::run(
        backend,
        config,
        &run_dir,
        prometheus::Registry::new(),
    ));
    let memory_end = evidence::memory();
    let outcome = std::mem::take(&mut *lock(&outcome));
    Ok(Invocation {
        run_dir,
        runner_error: result.err().map(|error| error.to_string()),
        outcome,
        owners,
        database,
        memory_start,
        memory_end,
    })
}

fn runtime() -> BackendResult<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

/// Local proof fixture spec of a phase after build; `None` in fleet mode.
fn spec_after_build(context: &Context) -> BackendResult<Option<FixtureSpec>> {
    match context.stage.mode {
        StageMode::LocalFixtureProof => Ok(Some(context.local_spec()?)),
        StageMode::Fleet => Ok(None),
    }
}

/// Per-owner allocated bytes of the local owner roots; `null` in fleet mode
/// (measured on the owner guests by `owner-observe`).
fn local_owner_allocation(context: &Context) -> BackendResult<Value> {
    if context.stage.mode == StageMode::Fleet {
        return Ok(Value::Null);
    }
    let peaks = context
        .stage
        .owners
        .iter()
        .map(|owner| evidence::tree_allocation(&owner.root).map(|total| total.allocated_bytes))
        .collect::<BackendResult<Vec<_>>>()?;
    Ok(json!(peaks))
}

// --- Phases -------------------------------------------------------------------

fn build(context: &Context) -> BackendResult<PhaseReport> {
    let stage = &context.stage;
    if !super::directory_is_empty(&stage.coordinator_path())? {
        return Err("build needs a fresh case root; never reuse a prepared database".into());
    }
    let spec = match stage.mode {
        StageMode::LocalFixtureProof => {
            if stage.fixture_root().exists() {
                return Err("build needs a fresh fixture root".into());
            }
            let spec = stage.local_spec(fresh_host_id())?;
            fs::create_dir_all(stage.fixture_root())?;
            publish(
                &stage.fixture_root().join(FIXTURE_SPEC),
                &serde_json::to_vec_pretty(&spec)?,
            )?;
            Some(spec)
        }
        StageMode::Fleet => None,
    };
    let runtime = runtime()?;
    let invocation = invoke(
        context,
        &runtime,
        spec.as_ref(),
        RunKind::Build,
        None,
        "build",
    )?;
    let details = evidence::checkpoint_details(&invocation.run_dir);
    let (generations, collector) = details
        .as_ref()
        .map_or((Vec::new(), Value::Null), evidence::generations);
    let normal = generations
        .iter()
        .filter(|row| row["kind"] == "normal")
        .count();
    let stage_end = invocation.finish_value();
    let distinct_rows = stage_end.and_then(|value| value["distinct_rows"].as_u64());
    let successful = invocation.outcome.successful_puts;
    let missing_or_wrong = stage_end
        .map(|value| &value["rows"])
        .and_then(|rows| Some(rows["missing"].as_u64()? + rows["wrong"].as_u64()?));
    let iterated = stage_end.and_then(|value| value["iterated_rows"].as_u64());
    // The forced root covers every published batch (suffix 0 after it), and
    // an Exact root adopts only owner-durable positions of every partition.
    let persistence_covered = stage_end
        .and_then(|value| value["forced_root"]["suffix_batches_after"].as_u64())
        .map(|suffix| suffix == 0);
    let database = invocation.database;
    let clean_close = invocation.clean();
    let verified = distinct_rows == Some(stage.index.keys)
        && successful == stage.index.keys
        && missing_or_wrong == Some(0)
        && iterated == Some(stage.index.keys);
    let owner_databases = owner_databases(context)?;
    // Local proof: both owner roots hold exactly the provisioned database.
    // Fleet: the owner-observe receipts prove it on the owner guests.
    let owners_hold_database = owner_databases.as_array().map(|owners| {
        owners
            .iter()
            .all(|owner| owner["databases"] == json!([database_hex(&database)]))
    });
    let complete = clean_close
        && verified
        && normal > 0
        && persistence_covered == Some(true)
        && owners_hold_database != Some(false);
    Ok(PhaseReport {
        status: if complete { Status::Complete } else { Status::Failed },
        error: (!complete).then(|| {
            format!(
                "build: clean_close={clean_close}, verified={verified}, normal_generations={normal}, persistence_covered={persistence_covered:?}"
            )
        }),
        database: Some(database),
        owners: invocation.owners.clone(),
        predecessors: Vec::new(),
        terminal: json!({
            "database_id": database_hex(&database),
            "owners_hold_database": owners_hold_database,
            "owner_roots": stage.index.owner_roots,
            "distinct_rows": distinct_rows,
            "successful": successful,
            "normally_adopted": normal > 0,
            "normal_generations": normal,
            "persistence_covered": persistence_covered,
            "persistence_covered_rule": "the labeled forced root left a zero published suffix; Exact adopts only owner-durable positions of every partition",
            "clean_close": clean_close,
            "iterated_rows": iterated,
            "owner_databases": owner_databases,
            "generations": generations,
            "collector": collector,
            "invocation": invocation.json(),
        }),
        artifacts: Vec::new(),
        output_files: run_files(&invocation.run_dir),
    })
}

/// Database identities each local owner root holds; `null` in fleet mode.
fn owner_databases(context: &Context) -> BackendResult<Value> {
    if context.stage.mode == StageMode::Fleet {
        return Ok(Value::Null);
    }
    let databases = context
        .stage
        .owners
        .iter()
        .map(|owner| {
            evidence::host_databases(&owner.root)
                .map(|databases| json!({"partition": owner.partition, "databases": databases}))
        })
        .collect::<BackendResult<Vec<_>>>()?;
    Ok(json!(databases))
}

/// Runner artifacts listed in the receipt.
fn run_files(run_dir: &Path) -> Vec<PathBuf> {
    [
        "summary.json",
        "manifest.json",
        WRITE_DIAGNOSTICS,
        "samples.csv",
    ]
    .iter()
    .map(|name| run_dir.join(name))
    .filter(|path| path.is_file())
    .collect()
}

/// Merge one invocation's pilot-domain bitmaps into the phase bitmaps.
fn merge_bitmaps(
    keys: u64,
    invocation: &Invocation,
    touched: &TouchedBitmap,
    uncertain: &TouchedBitmap,
) -> BackendResult<()> {
    touched.merge(&TouchedBitmap::from_bytes(
        keys,
        &invocation.outcome.touched,
    )?)?;
    uncertain.merge(&TouchedBitmap::from_bytes(
        keys,
        &invocation.outcome.uncertain,
    )?)
}

fn pilot(context: &Context) -> BackendResult<PhaseReport> {
    let stage = &context.stage;
    let predecessors = predecessors(context)?;
    let database = context.database()?;
    let spec = spec_after_build(context)?;
    let keys = stage.index.keys;
    let touched = TouchedBitmap::new(keys)?;
    let uncertain = TouchedBitmap::new(keys)?;
    let runtime = runtime()?;
    let pilot = &stage.workload.pilot;
    let closed = invoke(
        context,
        &runtime,
        spec.as_ref(),
        RunKind::PilotClosed,
        Some(database),
        "pilot-closed",
    )?;
    merge_bitmaps(keys, &closed, &touched, &uncertain)?;
    let mut output_files = run_files(&closed.run_dir);
    let closed_measurement = evidence::measurement(&closed.run_dir, pilot.skip_seconds)?;
    #[allow(clippy::cast_precision_loss)] // Half of the caller count.
    let occupancy_floor = stage.workload.clients as f64 / 2.0;
    let complete_seconds = u64::try_from(closed_measurement.complete_seconds.len())?;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    // floor of a finite positive rate
    let qualified_rate = (closed.clean()
        && closed_measurement.non_success() == 0
        && complete_seconds >= pilot.min_complete_seconds
        && closed_measurement
            .mean_in_flight
            .is_some_and(|occupancy| occupancy >= occupancy_floor))
    .then_some(closed_measurement.median_puts_per_s)
    .flatten()
    .map(|rate| rate.floor() as u64)
    .filter(|rate| *rate > 0);
    let mut confirmations = Vec::new();
    let mut selected = None;
    if let Some(rate) = qualified_rate {
        for (attempt, percent) in [
            ("primary", PILOT_PRIMARY_PERCENT),
            ("fallback_0_8", PILOT_FALLBACK_PERCENT),
        ] {
            let attempt_rate = rate * percent / 100;
            if attempt_rate == 0 {
                break;
            }
            if !pilot.qualify {
                // CI correctness route: select the primary rate without the
                // open-loop confirmations, so no rate or suffix-debt claim
                // is made and no shed offer can fail the phase.
                selected = Some((attempt, attempt_rate));
                break;
            }
            let label = format!("pilot-confirm-{attempt}");
            let confirm = invoke(
                context,
                &runtime,
                spec.as_ref(),
                RunKind::PilotConfirm(attempt_rate),
                Some(database),
                &label,
            )?;
            merge_bitmaps(keys, &confirm, &touched, &uncertain)?;
            output_files.extend(run_files(&confirm.run_dir));
            let measurement = evidence::measurement(&confirm.run_dir, 0)?;
            let seconds = evidence::checkpoint_details(&confirm.run_dir)
                .map(|details| evidence::timed_seconds(&details))
                .unwrap_or(Value::Null);
            let debt = evidence::debt(&seconds, stage.index.exact.soft_bytes());
            let stable = confirm.clean()
                && measurement.non_success() == 0
                && Some(measurement.offered) == attempt_rate.checked_mul(pilot.confirm_seconds)
                && debt.as_ref().is_some_and(|debt| debt["growing"] == false);
            confirmations.push(json!({
                "attempt": attempt,
                "percent_of_qualified_rate": percent,
                "rate": attempt_rate,
                "stable": stable,
                "stability_rule": "clean run, zero non-success outcomes, every offered call accepted, suffix debt not growing over thirds",
                "measurement": measurement,
                "debt": debt,
                "seconds": seconds,
                "invocation": confirm.json(),
            }));
            if stable {
                selected = Some((attempt, attempt_rate));
                break;
            }
        }
    }
    let pilot_touched = touched.to_bytes();
    let pilot_uncertain = uncertain.to_bytes();
    let complete = selected.is_some();
    Ok(PhaseReport {
        status: if complete {
            Status::Complete
        } else {
            Status::Failed
        },
        error: (!complete).then(|| "pilot: no stable rate (unqualified)".to_owned()),
        database: Some(database),
        owners: closed.owners.clone(),
        predecessors,
        terminal: json!({
            "database_id": database_hex(&database),
            "counts_toward_qualification": false,
            "closed_loop": {
                "measurement": closed_measurement,
                "complete_seconds_required": pilot.min_complete_seconds,
                "occupancy_floor": occupancy_floor,
                "qualified_rate": qualified_rate,
                "qualify": pilot.qualify,
                "rule": if pilot.qualify {
                    "clean run, zero non-success outcomes, enough complete seconds after the skip, mean API occupancy at least half the callers; rate = floor(median complete-second successful puts)"
                } else {
                    "closed-loop median only: the open-loop confirmations and the suffix-debt rule are skipped, so no rate or debt claim is made"
                },
                "invocation": closed.json(),
            },
            "confirmations": confirmations,
            "selected_rate": selected.map(|(_, rate)| rate),
            "selected_attempt": selected.map(|(attempt, _)| attempt),
            "fallback_used": selected.is_some_and(|(attempt, _)| attempt != "primary"),
            "pilot_touched": {"file": PILOT_TOUCHED, "count": touched.count()},
            "pilot_uncertain": {"file": PILOT_UNCERTAIN, "count": uncertain.count()},
        }),
        artifacts: vec![
            (PILOT_TOUCHED.to_owned(), pilot_touched),
            (PILOT_UNCERTAIN.to_owned(), pilot_uncertain),
        ],
        output_files,
    })
}

fn overwrite(context: &Context) -> BackendResult<PhaseReport> {
    let stage = &context.stage;
    let predecessors = predecessors(context)?;
    let database = context.database()?;
    let (pilot, _) = context.receipt(Phase::Pilot)?;
    let rate = pilot["terminal"]["selected_rate"]
        .as_u64()
        .ok_or("pilot selected no rate")?;
    let spec = spec_after_build(context)?;
    let runtime = runtime()?;
    let invocation = invoke(
        context,
        &runtime,
        spec.as_ref(),
        RunKind::Overwrite(rate),
        Some(database),
        "overwrite",
    )?;
    let measurement = evidence::measurement(&invocation.run_dir, 0)?;
    let details = evidence::checkpoint_details(&invocation.run_dir);
    let (generations, collector) = details
        .as_ref()
        .map_or((Vec::new(), Value::Null), evidence::generations);
    let seconds = details
        .as_ref()
        .map_or(Value::Null, evidence::timed_seconds);
    let debt = evidence::debt(&seconds, stage.index.exact.soft_bytes());
    let (start, end) = (seconds["start_ns"].as_u64(), seconds["end_ns"].as_u64());
    let normal_in_window = start
        .zip(end)
        .map(|(start, end)| evidence::normal_generations_within(&generations, start, end));
    let stop = invocation.finish_value().cloned();
    let owner_allocation = local_owner_allocation(context)?;
    let memory_end = &invocation.memory_end;
    let rss_peak = memory_end["vm_hwm_bytes"].as_u64();
    let cgroup_peak = memory_end["memory_peak_bytes"].as_u64();
    let cap_events = evidence::cap_events(&invocation.memory_start, memory_end);
    let touched = invocation.outcome.touched.clone();
    let uncertain = invocation.outcome.uncertain.clone();
    let mut artifacts = vec![
        (TIMED_TOUCHED.to_owned(), touched),
        (TIMED_UNCERTAIN.to_owned(), uncertain),
    ];
    if let Some(stop) = &stop {
        artifacts.push((OVERWRITE_STOP.to_owned(), serde_json::to_vec_pretty(stop)?));
    }
    let diagnostics_path = invocation.run_dir.join(WRITE_DIAGNOSTICS);
    if diagnostics_path.is_file() {
        artifacts.push((WRITE_DIAGNOSTICS.to_owned(), fs::read(&diagnostics_path)?));
    }
    // The coordinator stops crash-style, so only the runner, the stage end
    // and the owner stop must be clean.
    let complete = invocation.runner_error.is_none()
        && stop.is_some()
        && matches!(invocation.outcome.close, Some(Ok(())));
    let lost = collector["lost_generations"].clone();
    Ok(PhaseReport {
        status: if complete {
            Status::Complete
        } else {
            Status::Failed
        },
        error: (!complete).then(|| {
            format!(
                "overwrite: runner_error={:?}, stop_record={}",
                invocation.runner_error,
                stop.is_some()
            )
        }),
        database: Some(database),
        owners: invocation.owners.clone(),
        predecessors,
        terminal: json!({
            "database_id": database_hex(&database),
            "rate": rate,
            "start_ns": start,
            "end_ns": end,
            "offered": measurement.offered,
            "accepted": measurement.accepted,
            "rejected": measurement.rejected,
            "unknown": measurement.unknown,
            "wrong": measurement.wrong,
            "errors": measurement.errors,
            "lost_generations": lost,
            "generations": generations,
            "normal_generations_in_window": normal_in_window,
            "rss_peak": rss_peak,
            "cgroup_peak": cgroup_peak,
            "cap_events": cap_events,
            "owner_allocation_peaks": owner_allocation,
            "owner_allocation_source": match stage.mode {
                StageMode::Fleet => "owner-observe receipts on the owner guests",
                StageMode::LocalFixtureProof => "local owner roots after the owner stop (st_blocks)",
            },
            "seconds": seconds["seconds"],
            "missing_seconds": seconds["missing_seconds"],
            "sampled_rss_peak_bytes": seconds["sampled_rss_peak_bytes"],
            "debt": debt,
            "measurement": measurement,
            "collector": collector,
            "stop": stop,
            "invocation": invocation.json(),
        }),
        artifacts,
        output_files: run_files(&invocation.run_dir),
    })
}
