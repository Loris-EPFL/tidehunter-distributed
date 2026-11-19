//! Producer evidence of one runner invocation, read back from its own
//! artifacts: `samples.csv` (outcomes, exact nearest-rank latency,
//! complete-second rates, API occupancy), `write-diagnostics.json` (Exact
//! generations and per-second suffix debt in the timed window) and the
//! process/cgroup memory counters. Every value is an observation; absent
//! evidence is `null`, never a synthesized pass.

use crate::backend::BackendResult;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

const NS: u64 = 1_000_000_000;
/// Directory depth of an owner host root (`db-<hex>/<wal files>` and target
/// registries) with margin.
const MAX_DEPTH: usize = 8;

/// Streaming RFC 4180 records of the runner's `samples.csv`: quoted fields
/// may hold commas, doubled quotes and line breaks.
struct CsvRecords<R> {
    reader: R,
    line: Vec<u8>,
}

impl<R: BufRead> CsvRecords<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            line: Vec::new(),
        }
    }

    /// The next record into `fields`; `false` at end of input.
    fn next_record(&mut self, fields: &mut Vec<String>) -> BackendResult<bool> {
        fields.clear();
        let mut field = Vec::new();
        let mut quoted = false;
        let mut started = false;
        loop {
            self.line.clear();
            if self.reader.read_until(b'\n', &mut self.line)? == 0 {
                if quoted {
                    return Err("samples.csv ends inside a quoted field".into());
                }
                if started {
                    fields.push(String::from_utf8(std::mem::take(&mut field))?);
                    return Ok(true);
                }
                return Ok(false);
            }
            started = true;
            let mut bytes = self.line.iter().copied().peekable();
            while let Some(byte) = bytes.next() {
                match (quoted, byte) {
                    (true, b'"') if bytes.peek() == Some(&b'"') => {
                        field.push(b'"');
                        bytes.next();
                    }
                    (true, b'"') => quoted = false,
                    (true, byte) => field.push(byte),
                    (false, b'"') => quoted = true,
                    (false, b',') => fields.push(String::from_utf8(std::mem::take(&mut field))?),
                    (false, b'\n') => {
                        if field.last() == Some(&b'\r') {
                            field.pop();
                        }
                        fields.push(String::from_utf8(std::mem::take(&mut field))?);
                        return Ok(true);
                    }
                    (false, byte) => field.push(byte),
                }
            }
        }
    }
}

/// Exact nearest-rank quantile of a sorted population.
fn nearest_rank(sorted: &[u64], quantile: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )] // Rank of a bounded population; quantile in (0, 1].
    let rank = ((quantile * sorted.len() as f64).ceil() as usize).max(1);
    sorted.get(rank - 1).copied()
}

fn median(sorted: &[u64]) -> Option<f64> {
    let count = sorted.len();
    if count == 0 {
        return None;
    }
    #[allow(clippy::cast_precision_loss)] // Rate evidence.
    let value = if count % 2 == 1 {
        sorted[count / 2] as f64
    } else {
        (sorted[count / 2 - 1] as f64 + sorted[count / 2] as f64) / 2.0
    };
    Some(value)
}

/// Measurement-phase evidence of one runner invocation.
#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct Measurement {
    /// Every measurement arrival (one row each, rejected included).
    pub offered: u64,
    pub outcomes: BTreeMap<String, u64>,
    pub accepted: u64,
    pub rejected: u64,
    /// `timeout` and `unknown_mutation`.
    pub unknown: u64,
    /// `wrong_value` and `unexpected_present`.
    pub wrong: u64,
    pub errors: u64,
    /// Successful put API latency, exact nearest rank.
    pub success_api_ns: BTreeMap<&'static str, Option<u64>>,
    /// Successful put scheduling delay, exact nearest rank.
    pub success_scheduling_ns: BTreeMap<&'static str, Option<u64>>,
    /// Non-success API latency (dispatched calls), exact nearest rank.
    pub failure_api_ns: BTreeMap<&'static str, Option<u64>>,
    pub first_scheduled_ns: Option<u64>,
    pub first_dispatched_ns: Option<u64>,
    pub last_completed_ns: u64,
    /// Sum of dispatched API time over the dispatch window.
    pub mean_in_flight: Option<f64>,
    pub achieved_success_per_s: Option<f64>,
    /// Successful puts of each complete second after the skip.
    pub complete_seconds: Vec<u64>,
    pub median_puts_per_s: Option<f64>,
}

impl Measurement {
    pub(super) fn non_success(&self) -> u64 {
        self.offered - self.accepted
    }
}

fn column(header: &[String], name: &str) -> BackendResult<usize> {
    header
        .iter()
        .position(|field| field == name)
        .ok_or_else(|| format!("samples.csv has no {name} column").into())
}

/// One streaming pass over the measurement rows of `samples.csv`.
pub(super) fn measurement(run_dir: &Path, skip_seconds: u64) -> BackendResult<Measurement> {
    let file = File::open(run_dir.join("samples.csv"))
        .map_err(|error| format!("{}/samples.csv: {error}", run_dir.display()))?;
    let mut records = CsvRecords::new(BufReader::new(file));
    let mut header = Vec::new();
    if !records.next_record(&mut header)? {
        return Err("samples.csv is empty".into());
    }
    let index = |name| column(&header, name);
    let (phase, operation, outcome) = (index("phase")?, index("operation")?, index("outcome")?);
    let (scheduled, dispatched, observed) = (
        index("scheduled_ns")?,
        index("dispatched_ns")?,
        index("dispatch_observed")?,
    );
    let (completed, api, scheduling) = (
        index("completed_ns")?,
        index("api_ns")?,
        index("scheduling_ns")?,
    );
    let mut evidence = Measurement::default();
    let mut success_api = Vec::new();
    let mut success_scheduling = Vec::new();
    let mut failure_api = Vec::new();
    let mut buckets: BTreeMap<u64, u64> = BTreeMap::new();
    let mut api_total: u128 = 0;
    let mut fields = Vec::new();
    while records.next_record(&mut fields)? {
        if fields.len() != header.len() {
            return Err("samples.csv row width differs from its header".into());
        }
        if fields[phase] != "measurement" {
            continue;
        }
        let number = |column: usize| -> BackendResult<u64> { Ok(fields[column].parse()?) };
        let result = fields[outcome].as_str();
        evidence.offered += 1;
        *evidence.outcomes.entry(result.to_owned()).or_default() += 1;
        match result {
            "success" => evidence.accepted += 1,
            "rejected" => evidence.rejected += 1,
            "timeout" | "unknown_mutation" => evidence.unknown += 1,
            "wrong_value" | "unexpected_present" => evidence.wrong += 1,
            _ => evidence.errors += 1,
        }
        let scheduled_ns = number(scheduled)?;
        let completed_ns = number(completed)?;
        evidence.first_scheduled_ns = Some(
            evidence
                .first_scheduled_ns
                .map_or(scheduled_ns, |first| first.min(scheduled_ns)),
        );
        evidence.last_completed_ns = evidence.last_completed_ns.max(completed_ns);
        if fields[observed] == "true" {
            let dispatched_ns = number(dispatched)?;
            evidence.first_dispatched_ns = Some(
                evidence
                    .first_dispatched_ns
                    .map_or(dispatched_ns, |first| first.min(dispatched_ns)),
            );
            let api_ns = number(api)?;
            api_total += u128::from(api_ns);
            if result == "success" {
                success_api.push(api_ns);
                success_scheduling.push(number(scheduling)?);
            } else {
                failure_api.push(api_ns);
            }
        }
        if result == "success" && fields[operation] == "put" {
            *buckets.entry(completed_ns / NS).or_default() += 1;
        }
    }
    let quantiles = |values: &mut Vec<u64>| {
        values.sort_unstable();
        [("p50", 0.50), ("p99", 0.99), ("p999", 0.999)]
            .into_iter()
            .map(|(name, quantile)| (name, nearest_rank(values, quantile)))
            .collect::<BTreeMap<_, _>>()
    };
    evidence.success_api_ns = quantiles(&mut success_api);
    evidence.success_scheduling_ns = quantiles(&mut success_scheduling);
    evidence.failure_api_ns = quantiles(&mut failure_api);
    if let Some(first) = evidence.first_dispatched_ns {
        let window = evidence.last_completed_ns.saturating_sub(first);
        if window > 0 {
            #[allow(clippy::cast_precision_loss)] // Occupancy and rate evidence.
            {
                evidence.mean_in_flight = Some(api_total as f64 / window as f64);
                evidence.achieved_success_per_s =
                    Some(evidence.accepted as f64 * NS as f64 / window as f64);
            }
        }
    }
    // Complete seconds only: the last (partial) second is excluded.
    let start = evidence.first_scheduled_ns.unwrap_or(0) / NS + skip_seconds;
    let last = evidence.last_completed_ns / NS;
    evidence.complete_seconds = (start..last)
        .map(|second| buckets.get(&second).copied().unwrap_or(0))
        .collect();
    let mut sorted = evidence.complete_seconds.clone();
    sorted.sort_unstable();
    evidence.median_puts_per_s = median(&sorted);
    Ok(evidence)
}

pub(super) fn read_json(path: &Path) -> BackendResult<Value> {
    Ok(serde_json::from_slice(&fs::read(path).map_err(
        |error| format!("{}: {error}", path.display()),
    )?)?)
}

/// The `index_checkpoint` details of a run's `write-diagnostics.json`.
pub(super) fn checkpoint_details(run_dir: &Path) -> BackendResult<Value> {
    let mut diagnostics = read_json(&run_dir.join("write-diagnostics.json"))?;
    match diagnostics.get_mut("index_checkpoint") {
        Some(details) => Ok(details.take()),
        None => Err("write-diagnostics.json has no index_checkpoint details".into()),
    }
}

/// The named window's `(start_ns, end_ns)`.
fn window(details: &Value, name: &str) -> Option<(u64, u64)> {
    details["windows"]
        .as_array()?
        .iter()
        .find(|window| window["name"] == name)
        .and_then(|window| Some((window["start_ns"].as_u64()?, window["end_ns"].as_u64()?)))
}

fn database_hex(value: &Value) -> Option<String> {
    let bytes = value.as_array()?;
    if bytes.len() != 16 {
        return None;
    }
    bytes
        .iter()
        .map(|byte| {
            byte.as_u64()
                .filter(|byte| *byte <= 0xff)
                .map(|byte| format!("{byte:02x}"))
        })
        .collect()
}

/// One Exact generation row as Python Q1 consumes it. `kind` is `normal`
/// (automatic and cleaned), `requested` (explicit, retention, recovery or
/// harness drain) or `failed`.
fn generation_row(row: &Value) -> Option<Value> {
    let attribution = &row["attribution"];
    if !attribution.is_object() {
        return None;
    }
    let kind = if attribution["status"] != "Cleaned" {
        "failed"
    } else if row["automatic"] == true {
        "normal"
    } else {
        "requested"
    };
    Some(json!({
        "database_id": database_hex(&attribution["database"]),
        "fencing_generation": attribution["fencing_generation"],
        "generation": row["checkpoint_generation"].as_u64().or_else(|| row["generation"].as_u64()),
        "kind": kind,
        "trigger_names": row["trigger_names"],
        "adopted_ns": attribution["adopt_end_at_ns"],
        "installed_ns": attribution["live_install_end_at_ns"],
        "credited_ns": attribution["credits_returned_at_ns"],
    }))
}

/// Generations of a run plus the counts of rows the collector lost.
pub(super) fn generations(details: &Value) -> (Vec<Value>, Value) {
    let rows: Vec<Value> = details["generations"]
        .as_array()
        .map(|rows| rows.iter().filter_map(generation_row).collect())
        .unwrap_or_default();
    let lost = details["ring"]["lost_unread_rows"]
        .as_u64()
        .zip(details["spool"]["overflow_rows"].as_u64());
    (
        rows,
        json!({
            "lost_generations": lost.map(|(ring, spool)| ring + spool),
            "ring_lost_unread_rows": details["ring"]["lost_unread_rows"],
            "spool_overflow_rows": details["spool"]["overflow_rows"],
            "sample_overflow": details["sample_overflow"],
            "collector_status": details["status"],
        }),
    )
}

/// Normal generations whose adopt, live install and credit return all lie
/// inside `[start, end]`, in order.
pub(super) fn normal_generations_within(rows: &[Value], start: u64, end: u64) -> usize {
    rows.iter()
        .filter(|row| {
            row["kind"] == "normal"
                && [
                    &row["adopted_ns"],
                    &row["installed_ns"],
                    &row["credited_ns"],
                ]
                .map(Value::as_u64)
                .iter()
                .try_fold(start, |previous, at| at.filter(|at| *at >= previous))
                .is_some_and(|last| last <= end)
        })
        .count()
}

/// Per complete second of the timed window: the last coordinator suffix
/// sample in that second. A second without a sample is absent; the receipt
/// counts it.
pub(super) fn timed_seconds(details: &Value) -> Value {
    let Some((start, end)) = window(details, "timed") else {
        return json!({"available": false, "reason": "no timed window"});
    };
    let count = (end - start) / NS;
    let mut seconds = Vec::new();
    let mut missing = Vec::new();
    let samples = details["samples_1s"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    for second in 0..count {
        let (from, to) = (start + second * NS, start + (second + 1) * NS);
        let sample = samples
            .iter()
            .filter(|sample| {
                sample["at_ns"]
                    .as_u64()
                    .is_some_and(|at| at >= from && at < to)
            })
            .filter_map(|sample| {
                sample["databases"]
                    .as_array()?
                    .iter()
                    .find(|database| database["db"] == "coordinator")
            })
            .next_back();
        match sample {
            Some(database) => seconds.push(json!({
                "second": second,
                "age_ns": database["oldest_age_ns"],
                "bytes": database["suffix_bytes"],
                "batches": database["suffix_batches"],
            })),
            None => missing.push(second),
        }
    }
    let rss_peak = samples
        .iter()
        .filter(|sample| {
            sample["at_ns"]
                .as_u64()
                .is_some_and(|at| at >= start && at < end)
        })
        .filter_map(|sample| sample["rss_bytes"].as_u64())
        .max();
    json!({
        "available": true,
        "start_ns": start,
        "end_ns": end,
        "complete_seconds": count,
        "seconds": seconds,
        "missing_seconds": missing,
        "sampled_rss_peak_bytes": rss_peak,
    })
}

fn median_f64(values: &mut [u64]) -> Option<f64> {
    values.sort_unstable();
    median(values)
}

/// Suffix debt over thirds of the timed seconds, the Python rule: debt grows
/// when the age median rises strictly over the thirds together with bytes or
/// batches, or when the bytes median of the last third exceeds the first by
/// at least the Exact soft byte trigger. `None` without eight complete
/// seconds: a shorter confirmation samples the checkpoint sawtooth fewer
/// than three times per third and its verdict flips on phase, not on rate.
pub(super) fn debt(seconds: &Value, soft_bytes: u64) -> Option<Value> {
    let rows = seconds["seconds"].as_array()?;
    let count = rows.len();
    if count < 8 {
        return None;
    }
    let mut medians: BTreeMap<&str, [f64; 3]> = BTreeMap::new();
    for name in ["age_ns", "bytes", "batches"] {
        let mut thirds = [0.0; 3];
        for (third, slot) in thirds.iter_mut().enumerate() {
            let mut values: Vec<u64> = rows[third * count / 3..(third + 1) * count / 3]
                .iter()
                .map(|row| row[name].as_u64())
                .collect::<Option<_>>()?;
            *slot = median_f64(&mut values)?;
        }
        medians.insert(name, thirds);
    }
    let rising =
        |name: &str| medians[name][0] < medians[name][1] && medians[name][1] < medians[name][2];
    #[allow(clippy::cast_precision_loss)] // Trigger comparison in bytes.
    let bytes_growth_below_soft = medians["bytes"][2] - medians["bytes"][0] < soft_bytes as f64;
    let growing =
        (rising("age_ns") && (rising("bytes") || rising("batches"))) || !bytes_growth_below_soft;
    Some(json!({
        "medians_by_third": medians,
        "soft_bytes": soft_bytes,
        "growing": growing,
    }))
}

/// Process memory: `VmRSS`/`VmHWM` of this process and the cgroup v2
/// counters of its cgroup (`memory.current`, `memory.peak`, `memory.max`,
/// `memory.events`). Unavailable counters are `null`.
pub(super) fn memory() -> Value {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.trim().strip_suffix("kB"))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(|kib| kib * 1024)
    };
    let cgroup = fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("0::").map(str::to_owned))
        })
        .map(|path| Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')));
    let read = |name: &str| {
        cgroup
            .as_ref()
            .and_then(|dir| fs::read_to_string(dir.join(name)).ok())
    };
    let number = |name: &str| read(name).and_then(|text| text.trim().parse::<u64>().ok());
    let events: BTreeMap<String, u64> = read("memory.events")
        .map(|text| {
            text.lines()
                .filter_map(|line| {
                    let (key, value) = line.split_once(' ')?;
                    Some((key.to_owned(), value.trim().parse().ok()?))
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "vm_rss_bytes": field("VmRSS:"),
        "vm_hwm_bytes": field("VmHWM:"),
        "cgroup": cgroup,
        "memory_current_bytes": number("memory.current"),
        "memory_peak_bytes": number("memory.peak"),
        "memory_max": read("memory.max").map(|text| text.trim().to_owned()),
        "memory_events": if events.is_empty() { Value::Null } else { json!(events) },
    })
}

/// Memory-cap events between two [`memory`] observations: the increase of
/// `max`, `oom` and `oom_kill`.
pub(super) fn cap_events(start: &Value, end: &Value) -> Option<u64> {
    ["max", "oom", "oom_kill"]
        .iter()
        .map(|name| {
            let before = start["memory_events"][name].as_u64().unwrap_or(0);
            end["memory_events"][name]
                .as_u64()
                .map(|after| after.saturating_sub(before))
        })
        .sum()
}

/// Allocation of every regular file below `root` (bounded depth; symlinks
/// are not followed). Metadata only: no page is read or evicted.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(super) struct TreeAllocation {
    pub files: u64,
    pub bytes: u64,
    pub allocated_bytes: u64,
}

pub(super) fn tree_allocation(root: &Path) -> BackendResult<TreeAllocation> {
    fn walk(path: &Path, depth: usize, total: &mut TreeAllocation) -> BackendResult<()> {
        if depth > MAX_DEPTH {
            return Err(format!("{}: tree deeper than {MAX_DEPTH}", path.display()).into());
        }
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                walk(&entry.path(), depth + 1, total)?;
            } else if kind.is_file() {
                let metadata = entry.metadata()?;
                total.files += 1;
                total.bytes += metadata.len();
                total.allocated_bytes += metadata.blocks() * 512;
            }
        }
        Ok(())
    }
    let mut total = TreeAllocation::default();
    walk(root, 0, &mut total)?;
    Ok(total)
}

/// Database identities an owner host root holds (`db-<32 hex>` entries).
pub(super) fn host_databases(root: &Path) -> BackendResult<Vec<String>> {
    let mut databases: Vec<String> = fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("db-"))
                .filter(|hex| hex.len() == 32 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
                .map(str::to_owned)
        })
        .collect();
    databases.sort_unstable();
    Ok(databases)
}
