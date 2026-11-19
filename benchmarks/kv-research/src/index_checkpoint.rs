//! Exact checkpoint attribution collector (roadmap Phase 6 A0).
//!
//! One actor thread owns all collection state. It polls each coordinator's
//! bounded checkpoint ring every 250 ms and at window boundaries; the engine
//! copies at most 128 scalar rows per database per poll under its publisher
//! lock, and this thread serializes them to a spool file outside every engine
//! lock. Collection is opt-in (`diagnostics.index_checkpoint`) and arms no
//! request profile, timeline or owner profiling.
//!
//! Windows are the intervals between runner boundaries (`startup` from arming,
//! `timed`, `gate`, `drain`, closed by `end`). File I/O is assigned to a window
//! by the engine's cumulative producer counters at its two boundaries, never
//! by directory size deltas.

use crate::backend::BackendResult;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;
use tidehunter::distributed::remote_db::{
    self as engine, CheckpointDiagnostic, CheckpointTriggerFlags, CoordinatorIoSnapshot, FileIo,
    RecoveryStats,
};
use tidehunter::distributed::sui;

#[cfg(test)]
mod tests;

const POLL: Duration = Duration::from_millis(250);
/// Spooled generation rows per run; later rows are counted as overflow.
const MAX_ROWS: u64 = 65_536;
/// One-second samples per run (24 h); later samples are counted as overflow.
const MAX_SAMPLES: usize = 86_400;
const SAMPLE_INTERVAL_NS: u64 = 1_000_000_000;
const SPOOL_NAME: &str = "index-checkpoint-generations.jsonl.tmp";
/// Generations a benchmark requested; never compared as steady-state cadence.
const REQUESTED: [CheckpointTriggerFlags; 4] = [
    CheckpointTriggerFlags::EXPLICIT,
    CheckpointTriggerFlags::RETENTION_FLOOR,
    CheckpointTriggerFlags::RECOVERY,
    CheckpointTriggerFlags::HARNESS_DRAIN,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenKind {
    /// The coordinator directory was empty before open.
    Create,
    Reopen,
}

/// External open timing and the engine recovery evidence of one open.
#[derive(Clone)]
pub struct OpenRecord {
    pub kind: OpenKind,
    pub open_start_ns: u64,
    pub open_return_ns: u64,
    /// First successful point get after open; zero until one completes.
    pub first_get_ns: Arc<AtomicU64>,
    pub recovery: Option<RecoveryStats>,
}

impl OpenRecord {
    /// Record the first successful get once; later calls cost one load.
    pub fn observe_get(&self) {
        // Relaxed: an independent diagnostic timestamp, set at most once.
        if self.first_get_ns.load(Ordering::Relaxed) == 0 {
            let _ = self.first_get_ns.compare_exchange(
                0,
                engine::diagnostics_clock_ns().max(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }
}

/// One coordinator database of the run.
pub struct IndexCheckpointTarget {
    pub label: String,
    pub db: Arc<sui::Db>,
    pub coordinator: PathBuf,
    pub open: OpenRecord,
}

/// `details` is the authoritative `write-diagnostics.json:index_checkpoint`;
/// `summary` holds the same scalar rows and windows without per-second samples.
pub struct IndexCheckpointExport {
    pub details: Value,
    pub summary: Value,
}

enum Command {
    Mark(&'static str, mpsc::SyncSender<()>),
    Finish(mpsc::SyncSender<IndexCheckpointExport>),
}

/// Handle to the collector actor. Commands use a bounded channel of capacity
/// one; the runner sends one command and waits for its reply, so a full queue
/// only makes the sender wait. Dropping the handle closes the channel, the
/// actor exits and its database handles are released before the join returns.
pub struct IndexCheckpointCollector {
    commands: Option<mpsc::SyncSender<Command>>,
    actor: Option<JoinHandle<()>>,
}

impl IndexCheckpointCollector {
    /// Arm every target and start the actor; the `startup` window opens now.
    pub fn start(targets: Vec<IndexCheckpointTarget>, output: &Path) -> BackendResult<Self> {
        if targets.is_empty() {
            return Err("index checkpoint diagnostics need at least one coordinator".into());
        }
        for target in &targets {
            target
                .db
                .arm_index_checkpoint_diagnostics()
                .map_err(|error| format!("{}: {error:?}", target.label))?;
        }
        engine::set_coordinator_allocation_accounting(true);
        let spool_path = output.join(SPOOL_NAME);
        let spool = BufWriter::new(File::create(&spool_path)?);
        let actor = Actor::new(targets, spool_path, spool);
        let (commands, receiver) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("index-checkpoint-collector".to_owned())
            .spawn(move || actor.run(&receiver))?;
        Ok(Self {
            commands: Some(commands),
            actor: Some(thread),
        })
    }

    /// Start the window `name` at a quiescent runner boundary.
    pub async fn mark(&self, name: &'static str) {
        let Some(commands) = self.commands.clone() else {
            return;
        };
        let _ = tokio::task::spawn_blocking(move || {
            let (reply, done) = mpsc::sync_channel(1);
            if commands.send(Command::Mark(name, reply)).is_ok() {
                let _ = done.recv();
            }
        })
        .await;
    }

    /// Drain natural checkpoint work, close the last window, sync the
    /// coordinator file systems outside the timed windows and export.
    pub async fn finish(mut self) -> BackendResult<IndexCheckpointExport> {
        let commands = self
            .commands
            .take()
            .ok_or("index checkpoint collector already finished")?;
        let export = tokio::task::spawn_blocking(move || {
            let (reply, done) = mpsc::sync_channel(1);
            commands
                .send(Command::Finish(reply))
                .map_err(|_| "index checkpoint collector stopped")?;
            done.recv()
                .map_err(|_| "index checkpoint collector exited without export")
        })
        .await??;
        if let Some(actor) = self.actor.take() {
            actor
                .join()
                .map_err(|_| "index checkpoint collector panicked")?;
        }
        Ok(export)
    }
}

impl Drop for IndexCheckpointCollector {
    fn drop(&mut self) {
        drop(self.commands.take());
        if let Some(actor) = self.actor.take() {
            let _ = actor.join();
        }
    }
}

struct Boundary {
    name: &'static str,
    at_ns: u64,
    snapshots: Vec<Value>,
    io: CoordinatorIoSnapshot,
    device: Vec<Value>,
}

struct Actor {
    targets: Vec<IndexCheckpointTarget>,
    devices: Vec<Device>,
    cursors: Vec<u64>,
    lost_unread: Vec<u64>,
    spool_path: PathBuf,
    spool: Option<BufWriter<File>>,
    rows: u64,
    row_overflow: u64,
    boundaries: Vec<Boundary>,
    samples: Vec<Value>,
    sample_overflow: u64,
    next_sample_ns: u64,
    errors: Vec<String>,
}

impl Actor {
    fn new(
        targets: Vec<IndexCheckpointTarget>,
        spool_path: PathBuf,
        spool: BufWriter<File>,
    ) -> Self {
        let mut devices: Vec<Device> = Vec::new();
        for target in &targets {
            let device = Device::resolve(&target.coordinator);
            if !devices.iter().any(|known| known.same(&device)) {
                devices.push(device);
            }
        }
        let count = targets.len();
        Self {
            targets,
            devices,
            cursors: vec![0; count],
            lost_unread: vec![0; count],
            spool_path,
            spool: Some(spool),
            rows: 0,
            row_overflow: 0,
            boundaries: Vec::new(),
            samples: Vec::new(),
            sample_overflow: 0,
            next_sample_ns: 0,
            errors: Vec::new(),
        }
    }

    fn run(mut self, commands: &mpsc::Receiver<Command>) {
        self.boundary("startup");
        loop {
            match commands.recv_timeout(POLL) {
                Ok(Command::Mark(name, reply)) => {
                    self.boundary(name);
                    let _ = reply.send(());
                }
                Ok(Command::Finish(reply)) => {
                    let export = self.finish();
                    let _ = reply.send(export);
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => self.poll(),
                // The handle was dropped without finishing: release the targets.
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn poll(&mut self) {
        for index in 0..self.targets.len() {
            let Some(snapshot) = self.targets[index]
                .db
                .index_checkpoint_diagnostics_since(self.cursors[index])
            else {
                continue;
            };
            let expected = self.cursors[index];
            let first = snapshot
                .records
                .first()
                .map_or(snapshot.next_cursor, |record| record.cursor);
            // Rows overwritten in the ring before they were read.
            self.lost_unread[index] += first.saturating_sub(expected);
            for record in &snapshot.records {
                self.spool_row(index, record);
            }
            self.cursors[index] = snapshot.next_cursor.max(expected);
        }
        let now = engine::diagnostics_clock_ns();
        if now >= self.next_sample_ns {
            self.next_sample_ns = now.saturating_add(SAMPLE_INTERVAL_NS);
            if self.samples.len() < MAX_SAMPLES {
                let sample = self.sample(now);
                self.samples.push(sample);
            } else {
                self.sample_overflow += 1;
            }
        }
    }

    fn spool_row(&mut self, index: usize, record: &CheckpointDiagnostic) {
        if self.rows >= MAX_ROWS {
            self.row_overflow += 1;
            return;
        }
        let Some(spool) = self.spool.as_mut() else {
            return;
        };
        let line = generation_row(&self.targets[index].label, record)
            .and_then(|row| serde_json::to_vec(&row).map_err(Into::into));
        let written = line.and_then(|mut line| {
            line.push(b'\n');
            spool.write_all(&line).map_err(Into::into)
        });
        match written {
            Ok(()) => self.rows += 1,
            Err(error) => {
                self.errors.push(format!("spool write failed: {error}"));
                self.spool = None;
            }
        }
    }

    fn sample(&self, now: u64) -> Value {
        let databases: Vec<Value> = self
            .targets
            .iter()
            .filter_map(|target| {
                let snapshot = target.db.index_checkpoint_snapshot()?;
                Some(json!({
                    "db": target.label,
                    "suffix_batches": snapshot.suffix.batches,
                    "suffix_bytes": snapshot.suffix.bytes,
                    "oldest_age_ns": snapshot.suffix.oldest_age_ns,
                    "age_quality": snapshot.suffix.age_quality,
                    "charged_index_bytes": snapshot.charged_index_bytes,
                    "active_worker": snapshot.active_worker,
                }))
            })
            .collect();
        json!({"at_ns": now, "rss_bytes": process_rss_bytes(), "databases": databases})
    }

    fn boundary(&mut self, name: &'static str) {
        self.poll();
        let snapshots = self
            .targets
            .iter()
            .map(|target| {
                json!({
                    "db": target.label,
                    "snapshot": target.db.index_checkpoint_snapshot(),
                })
            })
            .collect();
        let device = self.devices.iter().map(Device::sample).collect();
        self.boundaries.push(Boundary {
            name,
            at_ns: engine::diagnostics_clock_ns(),
            snapshots,
            io: engine::coordinator_io_snapshot(),
            device,
        });
    }

    fn finish(&mut self) -> IndexCheckpointExport {
        let mut drain_errors = Vec::new();
        for target in &self.targets {
            if let Err(error) = target.db.drain_index_checkpoints(false) {
                drain_errors.push(format!("{}: {error:?}", target.label));
            }
        }
        self.boundary("end");
        let sync = self.sync_file_systems();
        self.boundary("end_synced");
        let generations = self.read_spool();
        let windows = windows(&self.boundaries, &generations);
        let run_totals = generation_totals(&generations);
        let databases: Vec<Value> = self
            .targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let snapshot = target.db.index_checkpoint_snapshot();
                json!({
                    "db": target.label,
                    "coordinator_path": target.coordinator,
                    "ring_next_cursor": self.cursors[index],
                    "lost_unread_rows": self.lost_unread[index],
                    "final": snapshot,
                })
            })
            .collect();
        let reopens: Vec<Value> = self.targets.iter().map(open_json).collect();
        let lost: u64 = self.lost_unread.iter().sum();
        let final_io = self
            .boundaries
            .iter()
            .rev()
            .find(|boundary| boundary.name == "end")
            .map(|boundary| boundary.io);
        let eligible = lost == 0 && self.row_overflow == 0 && self.errors.is_empty();
        let sync_ok = !sync.is_empty() && sync.iter().all(|entry| entry["ok"] == true);
        let device_gate = device_gate(
            &self.boundaries,
            &[
                (eligible, "ring loss, spool overflow or collector error"),
                (drain_errors.is_empty(), "drain error"),
                (sync_ok, "syncfs failed or no coordinator path"),
                (self.sample_overflow == 0, "sample overflow"),
            ],
        );
        let mut details = json!({
            "schema_version": 1,
            "status": if eligible && drain_errors.is_empty() { "complete" } else { "incomplete" },
            "scope": "coordinator Exact checkpoint generations; coordinator file counters are process-wide over every coordinator database of this process",
            "clock": "tidehunter remote_db::diagnostics_clock_ns, one process monotonic origin; owner clocks are never used",
            "windows_semantics": "a window runs from its boundary to the next; startup starts at arming, after open; file I/O is the producer counter delta between boundaries",
            "databases": databases,
            "windows": windows,
            "generations": generations,
            "reopens": reopens,
            "run_totals": run_totals,
            "failed_opens": "reported by the run failure: a failed open creates no backend",
            "samples_1s": self.samples,
            "sample_overflow": self.sample_overflow,
            "file_io": final_io,
            "detailed_totals_eligible": eligible,
            "device_attribution_eligible": device_gate["eligible"],
            "device_reconciliation": device_gate,
            "ring": {"lost_unread_rows": lost, "capacity_per_database": 128},
            "spool": {"rows": self.rows, "overflow_rows": self.row_overflow, "cap": MAX_ROWS},
            "drain": {"forced_root": false, "errors": drain_errors},
            "sync": sync,
            "errors": self.errors,
        });
        let mut summary = details.clone();
        if let Some(object) = summary.as_object_mut() {
            object.remove("samples_1s");
            object.insert(
                "details_artifact".to_owned(),
                Value::String("write-diagnostics.json".to_owned()),
            );
        }
        if let Some(object) = details.as_object_mut() {
            object.insert(
                "boundaries".to_owned(),
                Value::Array(self.boundaries.iter().map(boundary_json).collect()),
            );
        }
        IndexCheckpointExport { details, summary }
    }

    /// `sync -f` on each coordinator file system so final writeback lands in
    /// the accounting window; outside every timed window.
    fn sync_file_systems(&self) -> Vec<Value> {
        let mut paths: Vec<&Path> = Vec::new();
        for target in &self.targets {
            if !paths.contains(&target.coordinator.as_path()) {
                paths.push(&target.coordinator);
            }
        }
        paths
            .into_iter()
            .map(|path| {
                let status = std::process::Command::new("sync")
                    .arg("-f")
                    .arg(path)
                    .status();
                json!({
                    "path": path,
                    "ok": status.as_ref().is_ok_and(std::process::ExitStatus::success),
                    "error": status.err().map(|error| error.to_string()),
                })
            })
            .collect()
    }

    fn read_spool(&mut self) -> Vec<Value> {
        let Some(mut spool) = self.spool.take() else {
            return Vec::new();
        };
        if let Err(error) = spool.flush() {
            self.errors.push(format!("spool flush failed: {error}"));
            return Vec::new();
        }
        drop(spool);
        let rows = File::open(&self.spool_path).map(|file| {
            BufReader::new(file)
                .lines()
                .map(|line| {
                    line.map_err(|error| error.to_string())
                        .and_then(|line| serde_json::from_str(&line).map_err(|e| e.to_string()))
                })
                .collect::<Result<Vec<Value>, String>>()
        });
        match rows {
            Ok(Ok(rows)) => {
                let _ = fs::remove_file(&self.spool_path);
                rows
            }
            Ok(Err(error)) => {
                self.errors.push(format!("spool read failed: {error}"));
                Vec::new()
            }
            Err(error) => {
                self.errors.push(format!("spool open failed: {error}"));
                Vec::new()
            }
        }
    }
}

/// Requested generations (explicit, retention, recovery, harness drain) are
/// never compared as automatic steady-state cadence.
fn automatic(flags: CheckpointTriggerFlags) -> bool {
    !REQUESTED.iter().any(|flag| flags.contains(*flag))
}

/// Completion time: cleanup end, or for a failed row its last observed boundary.
fn completed_at(row: &engine::CheckpointAttribution) -> Option<u64> {
    row.cleanup_end_at_ns.or_else(|| {
        [
            row.credits_returned_at_ns,
            row.live_install_end_at_ns,
            row.live_install_start_at_ns,
            row.adopt_end_at_ns,
            row.adopt_start_at_ns,
            row.capture_end_at_ns,
            row.capture_start_at_ns,
            row.worker_started_at_ns,
        ]
        .into_iter()
        .flatten()
        .max()
    })
}

fn generation_row(label: &str, record: &CheckpointDiagnostic) -> BackendResult<Value> {
    let mut row = serde_json::to_value(record)?;
    let object = row
        .as_object_mut()
        .ok_or("checkpoint row must be an object")?;
    object.insert("db".to_owned(), Value::String(label.to_owned()));
    if let Some(attribution) = &record.attribution {
        object.insert(
            "trigger_names".to_owned(),
            json!(attribution.trigger_flags.names().collect::<Vec<_>>()),
        );
        object.insert(
            "automatic".to_owned(),
            json!(automatic(attribution.trigger_flags)),
        );
        object.insert(
            "completed_at_ns".to_owned(),
            json!(completed_at(attribution)),
        );
    }
    Ok(row)
}

fn io_delta(start: &CoordinatorIoSnapshot, end: &CoordinatorIoSnapshot) -> (Value, FileIo) {
    let delta = |a: FileIo, b: FileIo| FileIo {
        bytes: b.bytes.saturating_sub(a.bytes),
        file_syncs: b.file_syncs.saturating_sub(a.file_syncs),
        dir_syncs: b.dir_syncs.saturating_sub(a.dir_syncs),
        allocated_bytes: b.allocated_bytes.saturating_sub(a.allocated_bytes),
    };
    let categories = [
        ("cells", start.cells, end.cells),
        ("root", start.root, end.root),
        ("wrapper", start.wrapper, end.wrapper),
        ("manifest_stage", start.manifest_stage, end.manifest_stage),
        ("manifest_adopt", start.manifest_adopt, end.manifest_adopt),
        (
            "manifest_cleanup",
            start.manifest_cleanup,
            end.manifest_cleanup,
        ),
        ("cleanup_names", start.cleanup_names, end.cleanup_names),
        ("open", start.open, end.open),
        ("durable_cut", start.durable_cut, end.durable_cut),
    ];
    let mut object = serde_json::Map::new();
    let mut total = FileIo::default();
    for (name, before, after) in categories {
        let value = delta(before, after);
        total.add(value);
        object.insert(name.to_owned(), json!(value));
    }
    object.insert("total".to_owned(), json!(total));
    object.insert(
        "allocation_errors".to_owned(),
        json!(
            end.allocation_errors
                .saturating_sub(start.allocation_errors)
        ),
    );
    (Value::Object(object), total)
}

/// Device write bytes between two boundary samples, summed over distinct
/// coordinator devices; `None` when any device lacks a counter.
fn device_write_delta(start: &[Value], end: &[Value]) -> Option<u64> {
    if start.len() != end.len() || start.is_empty() {
        return None;
    }
    start
        .iter()
        .zip(end)
        .try_fold(0u64, |sum, (before, after)| {
            let before = before["write_sectors"].as_u64()?;
            let after = after["write_sectors"].as_u64()?;
            Some(sum.saturating_add(after.saturating_sub(before).saturating_mul(512)))
        })
}

/// `A` = new synced coordinator inode allocation, `D` = device write delta.
fn reconcile(allocated: u64, device: Option<u64>) -> Value {
    match device {
        Some(device) if device > 0 => {
            let residual = i128::from(device) - i128::from(allocated);
            #[allow(clippy::cast_precision_loss, reason = "a diagnostic ratio")]
            let ratio = residual.unsigned_abs() as f64 / device as f64;
            json!({"a_bytes": allocated, "d_bytes": device, "d_minus_a_bytes": residual.to_string(),
                "abs_residual_ratio": ratio, "within_0_10": ratio <= 0.10})
        }
        device => json!({"a_bytes": allocated, "d_bytes": device,
            "unavailable": "no device counter or no device writes in this window"}),
    }
}

/// V6 aggregate gate over the measured span, from the `timed` boundary
/// (`startup` when no measurement ran) to `end_synced`, so `D` includes the
/// final `syncfs` writeback. Fails closed: any listed check, a missing device
/// counter or allocation, no device writes, or a residual above 10% makes the
/// run ineligible; every failed reason is listed. Per-window rows are pre-sync.
fn device_gate(boundaries: &[Boundary], checks: &[(bool, &'static str)]) -> Value {
    let mut reasons: Vec<&'static str> = checks
        .iter()
        .filter(|(ok, _)| !ok)
        .map(|(_, reason)| *reason)
        .collect();
    let start = boundaries
        .iter()
        .find(|boundary| boundary.name == "timed")
        .or_else(|| boundaries.first());
    let synced = boundaries
        .iter()
        .find(|boundary| boundary.name == "end_synced");
    let (Some(start), Some(synced)) = (start, synced) else {
        reasons.push("missing span boundary");
        return json!({"eligible": false, "reasons": reasons});
    };
    let (io, total) = io_delta(&start.io, &synced.io);
    if !synced.io.allocation_accounting || synced.io.allocation_errors != 0 {
        reasons.push("allocation accounting off or incomplete");
    }
    let device = device_write_delta(&start.device, &synced.device);
    if device.is_none() {
        reasons.push("device write counter unavailable");
    }
    let reconciliation = reconcile(total.allocated_bytes, device);
    match reconciliation["within_0_10"].as_bool() {
        Some(true) => {}
        Some(false) => reasons.push("abs(D-A)/D above 0.10"),
        None => reasons.push("no device writes in span"),
    }
    json!({
        "eligible": reasons.is_empty(),
        "reasons": reasons,
        "span": {"from": start.name, "to": synced.name, "start_ns": start.at_ns, "end_ns": synced.at_ns},
        "coordinator_io": io,
        "reconciliation": reconciliation,
        "scope": "host block writes of the coordinator partition, not SSD NAND; idle-gate and unrelated-traffic rejection are controller duties",
    })
}

fn windows(boundaries: &[Boundary], generations: &[Value]) -> Vec<Value> {
    let last = boundaries
        .iter()
        .position(|boundary| boundary.name == "end")
        .unwrap_or_else(|| boundaries.len().saturating_sub(1));
    let Some(closed) = boundaries.get(..=last) else {
        return Vec::new();
    };
    closed
        .windows(2)
        .map(|pair| {
            let (start, stop) = (&pair[0], &pair[1]);
            let inside = |at: u64| at >= start.at_ns && at < stop.at_ns;
            let completed: Vec<&Value> = generations
                .iter()
                .filter(|row| row["completed_at_ns"].as_u64().is_some_and(&inside))
                .collect();
            let status = |row: &Value, status: &str| row["attribution"]["status"] == status;
            let contained = completed
                .iter()
                .filter(|row| {
                    row["automatic"] == true
                        && status(row, "Cleaned")
                        && row["attribution"]["requested_at_ns"]
                            .as_u64()
                            .is_some_and(|at| at >= start.at_ns)
                })
                .count();
            let (io, total) = io_delta(&start.io, &stop.io);
            let device = device_write_delta(&start.device, &stop.device);
            json!({
                "name": start.name,
                "start_ns": start.at_ns,
                "end_ns": stop.at_ns,
                "generations_completed": completed.len(),
                "generations_cleaned": completed.iter().filter(|row| status(row, "Cleaned")).count(),
                "generations_failed": completed.iter().filter(|row| status(row, "Failed")).count(),
                "automatic_generations_contained": contained,
                "coordinator_io": io,
                "device_write_bytes": device,
                "device_reconciliation": reconcile(total.allocated_bytes, device),
            })
        })
        .collect()
}

fn boundary_json(boundary: &Boundary) -> Value {
    json!({
        "name": boundary.name,
        "at_ns": boundary.at_ns,
        "databases": boundary.snapshots,
        "coordinator_io": boundary.io,
        "devices": boundary.device,
    })
}

fn recovery_json(stats: &RecoveryStats) -> Value {
    json!({
        "open_id": stats.open_id,
        "checkpoint_loaded": stats.checkpoint_loaded,
        "tail_batches": stats.tail_batches,
        "tail_bytes": stats.tail_bytes,
        "tail_replay_ns": stats.tail_replay_ns,
        "checkpoint_load_ns": stats.checkpoint_load_ns,
        // WAL replay validates no receipt-prefix proof; zero is not a timing.
        "proof_validation_ns": Value::Null,
        "checkpoint_commit_ns": stats.checkpoint_commit_ns,
        "connection_setup_ns": stats.connection_setup_ns,
        "fencing_ns": stats.fencing_ns,
        "tls_config_ns": stats.tls_config_ns,
        "cut": {
            "discarded_batches": stats.cut_discarded_batches,
            "extended_batches": stats.cut_extended_batches,
            "skipped_versions": stats.cut_skipped_versions,
            "continuity_bound": stats.cut_continuity_bound,
        },
        "semantics": "engine timing fields may nest; tail counts are applied batches only",
    })
}

fn open_json(target: &IndexCheckpointTarget) -> Value {
    let open = &target.open;
    let kind = match open.kind {
        OpenKind::Create => "create",
        OpenKind::Reopen => "reopen",
    };
    // Relaxed: the timestamp is read once, after the timed windows.
    let first_get = match open.first_get_ns.load(Ordering::Relaxed) {
        0 => None,
        at => Some(at),
    };
    json!({
        "db": target.label,
        "kind": kind,
        "status": "opened",
        "open_start_ns": open.open_start_ns,
        "open_return_ns": open.open_return_ns,
        "open_elapsed_ns": open.open_return_ns.saturating_sub(open.open_start_ns),
        "first_successful_get_ns": first_get,
        "open_start_to_first_get_ns": first_get.map(|at| at.saturating_sub(open.open_start_ns)),
        "recovery": open.recovery.as_ref().map(recovery_json),
    })
}

fn process_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib.saturating_mul(1024))
}

/// Linux `dev_t` to (major, minor), as glibc `gnu_dev_major/minor`.
fn split_dev(dev: u64) -> (u64, u64) {
    let major = ((dev >> 32) & 0xffff_f000) | ((dev >> 8) & 0x0000_0fff);
    let minor = ((dev >> 12) & 0xffff_ff00) | (dev & 0x0000_00ff);
    (major, minor)
}

/// The mount of `path` with this device: `(mount point, fs type, options)`.
fn find_mount(mountinfo: &str, path: &Path, id: (u64, u64)) -> Option<Value> {
    let wanted = format!("{}:{}", id.0, id.1);
    mountinfo
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let separator = fields.iter().position(|field| *field == "-")?;
            (fields.get(2) == Some(&wanted.as_str())).then_some(())?;
            let point = Path::new(fields.get(4)?);
            path.starts_with(point).then(|| {
                (
                    point.as_os_str().len(),
                    json!({
                        "mount_point": point,
                        "fs_type": fields.get(separator + 1),
                        "mount_options": fields.get(5),
                        "super_options": fields.get(separator + 3),
                    }),
                )
            })
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, mount)| mount)
}

/// Sectors written from a `/sys/block/.../stat` line (field 7).
fn parse_write_sectors(stat: &str) -> Option<u64> {
    stat.split_whitespace().nth(6)?.parse().ok()
}

/// `wbytes` of one device from a cgroup v2 `io.stat`.
fn parse_cgroup_wbytes(io_stat: &str, id: (u64, u64)) -> Option<u64> {
    let prefix = format!("{}:{} ", id.0, id.1);
    let line = io_stat.lines().find(|line| line.starts_with(&prefix))?;
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("wbytes="))?
        .parse()
        .ok()
}

fn meminfo_bytes(meminfo: &str, key: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with(key))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib.saturating_mul(1024))
}

/// The block device that holds one coordinator directory. Rootless: sysfs,
/// procfs and the process's own cgroup only.
struct Device {
    path: PathBuf,
    id: Option<(u64, u64)>,
    mount: Option<Value>,
    cgroup_io_stat: Option<PathBuf>,
    error: Option<String>,
}

impl Device {
    fn resolve(path: &Path) -> Self {
        use std::os::unix::fs::MetadataExt;
        let (id, error) = match fs::metadata(path) {
            Ok(metadata) => (Some(split_dev(metadata.dev())), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let absolute = fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
        let mount = id.and_then(|id| {
            find_mount(
                &fs::read_to_string("/proc/self/mountinfo").ok()?,
                &absolute,
                id,
            )
        });
        let cgroup_io_stat = fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|cgroup| {
                let relative = cgroup.lines().find_map(|line| line.strip_prefix("0::"))?;
                Some(
                    Path::new("/sys/fs/cgroup")
                        .join(relative.trim_start_matches('/'))
                        .join("io.stat"),
                )
            });
        Self {
            path: absolute,
            id,
            mount,
            cgroup_io_stat,
            error,
        }
    }

    fn same(&self, other: &Self) -> bool {
        match (self.id, other.id) {
            (Some(left), Some(right)) => left == right,
            _ => self.path == other.path,
        }
    }

    fn sample(&self) -> Value {
        let Some(id) = self.id else {
            return json!({"path": self.path, "error": self.error});
        };
        let stat_path = format!("/sys/dev/block/{}:{}/stat", id.0, id.1);
        let stat = fs::read_to_string(&stat_path);
        let meminfo = fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let cgroup_wbytes = self
            .cgroup_io_stat
            .as_ref()
            .and_then(|path| fs::read_to_string(path).ok())
            .and_then(|io_stat| parse_cgroup_wbytes(&io_stat, id));
        json!({
            "path": self.path,
            "major": id.0,
            "minor": id.1,
            "mount": self.mount,
            "write_sectors": stat.as_deref().ok().and_then(parse_write_sectors),
            "stat_error": stat.err().map(|error| format!("{stat_path}: {error}")),
            "dirty_bytes": meminfo_bytes(&meminfo, "Dirty:"),
            "writeback_bytes": meminfo_bytes(&meminfo, "Writeback:"),
            "cgroup_wbytes": cgroup_wbytes,
            "cgroup_wbytes_unavailable": cgroup_wbytes.is_none(),
        })
    }
}

/// Per-database run totals of the spooled rows.
pub fn generation_totals(generations: &[Value]) -> BTreeMap<String, Value> {
    let mut totals: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
    for row in generations {
        let label = row["db"].as_str().unwrap_or_default().to_owned();
        let entry = totals.entry(label).or_default();
        match row["attribution"]["status"].as_str() {
            Some("Cleaned") => entry.0 += 1,
            Some("Failed") => entry.1 += 1,
            _ => {}
        }
        entry.2 += row["captured_bytes"].as_u64().unwrap_or_default();
    }
    totals
        .into_iter()
        .map(|(label, (cleaned, failed, captured_bytes))| {
            (
                label,
                json!({"cleaned": cleaned, "failed": failed, "captured_bytes": captured_bytes}),
            )
        })
        .collect()
}
