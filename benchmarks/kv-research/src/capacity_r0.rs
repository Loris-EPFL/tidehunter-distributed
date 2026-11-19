//! `r0.json` writer for R0 shadow capacity measurement
//! (`checkpoint-work/research/design/CAPACITY-R0-R1.md` §2, `format_version` 1).
//!
//! The engine observer streams typed samples to [`R0Writer`] on its own thread;
//! the run summary is appended after the last sample, and the file becomes
//! `r0.json` only after its final sync and rename. Leased databases only.

use crate::backend::BackendResult;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tidehunter::distributed::capacity::{
    CapacityObserverConfig, R0Geometry, R0Sample, R0Sink, R0Summary,
};
use tidehunter::distributed::remote_db::CapacityObserver;
use tidehunter::distributed::sui;

/// Replay configuration block `capacity_r0`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct R0Request {
    pub workload: R0Workload,
    /// Native `relocation_max_reclaim_pct` for the shadow span budget (Sui: 100).
    #[serde(default = "default_reclaim_pct")]
    pub relocation_max_reclaim_pct: u8,
    /// Distinct-batch records in memory before a metadata-only spill.
    #[serde(default)]
    pub max_batch_records: Option<usize>,
}

fn default_reclaim_pct() -> u8 {
    100
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum R0Workload {
    SuiMainnetRetention,
    SyntheticChurn,
}

/// Run fields the benchmark knows; the engine adds the geometry.
#[derive(Clone, Debug, Serialize)]
pub struct R0Header {
    pub run_id: String,
    pub engine_revision: serde_json::Value,
    pub workload: R0Workload,
    pub input_digest: Option<String>,
    pub durability_tier: serde_json::Value,
    pub route: serde_json::Value,
    pub fixture: serde_json::Value,
    /// Owner WAL prewrite is an owner setting the coordinator cannot observe.
    pub prewrite: Option<bool>,
}

struct WriterState {
    file: Option<BufWriter<File>>,
    samples: u64,
    error: Option<String>,
    /// Every top-level key written so far: the object must have no duplicate
    /// key, whose meaning would depend on the parser.
    keys: BTreeSet<String>,
}

/// Streams one JSON object: header fields, `samples`, then the summary.
#[derive(Clone)]
pub struct R0Writer {
    state: Arc<Mutex<WriterState>>,
    partial: PathBuf,
    path: PathBuf,
}

impl R0Writer {
    /// Open `<path>.partial` and write the header fields and `"samples":[`.
    pub fn create(path: &Path, header: &R0Header) -> BackendResult<Self> {
        let partial = path.with_extension("json.partial");
        let mut file = BufWriter::new(File::create_new(&partial)?);
        let mut object = serde_json::Map::new();
        object.insert("format_version".into(), serde_json::json!(1));
        if let serde_json::Value::Object(fields) = serde_json::to_value(header)? {
            object.extend(fields);
        }
        file.write_all(b"{")?;
        let mut keys = BTreeSet::new();
        for (key, value) in &object {
            keys.insert(key.clone());
            serde_json::to_writer(&mut file, key)?;
            file.write_all(b":")?;
            serde_json::to_writer(&mut file, value)?;
            file.write_all(b",")?;
        }
        if !keys.insert("samples".to_owned()) {
            return Err("r0.json header already has a `samples` key".into());
        }
        file.write_all(b"\"samples\":[")?;
        Ok(Self {
            state: Arc::new(Mutex::new(WriterState {
                file: Some(file),
                samples: 0,
                error: None,
                keys,
            })),
            partial,
            path: path.to_owned(),
        })
    }

    /// Append the engine geometry and run summary after the samples, sync,
    /// rename to the final name and sync the directory.
    pub fn finish(&self, geometry: &R0Geometry, summary: &R0Summary) -> BackendResult<()> {
        let mut state = self.state.lock().map_err(|_| "r0 writer lock poisoned")?;
        if let Some(error) = &state.error {
            return Err(format!("r0 sample stream failed: {error}").into());
        }
        if state.file.is_none() {
            return Err("r0 writer already finished".into());
        }
        let mut fields = Vec::new();
        for value in [
            serde_json::to_value(geometry)?,
            serde_json::to_value(summary)?,
        ] {
            let serde_json::Value::Object(object) = value else {
                return Err("r0 geometry and summary must serialize as objects".into());
            };
            fields.extend(object);
        }
        // Check every key before writing anything after the samples.
        let mut seen = state.keys.clone();
        for (key, _) in &fields {
            if !seen.insert(key.clone()) {
                return Err(format!("duplicate r0.json key `{key}`").into());
            }
        }
        state.keys = seen;
        let mut file = state.file.take().ok_or("r0 writer already finished")?;
        file.write_all(b"]")?;
        for (key, value) in &fields {
            file.write_all(b",")?;
            serde_json::to_writer(&mut file, key)?;
            file.write_all(b":")?;
            serde_json::to_writer(&mut file, value)?;
        }
        file.write_all(b"}\n")?;
        let file = file.into_inner().map_err(|error| error.into_error())?;
        file.sync_all()?;
        std::fs::rename(&self.partial, &self.path)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

impl R0Sink for R0Writer {
    fn sample(&mut self, sample: &R0Sample) -> std::io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("r0 writer lock poisoned"))?;
        let first = state.samples == 0;
        let result = match state.file.as_mut() {
            Some(file) => (|| {
                if !first {
                    file.write_all(b",")?;
                }
                serde_json::to_writer(&mut *file, sample).map_err(std::io::Error::other)
            })(),
            None => Err(std::io::Error::other("r0 writer finished")),
        };
        match &result {
            Ok(()) => state.samples += 1,
            Err(error) => state.error = Some(error.to_string()),
        }
        result
    }
}

/// One database's observer and its report file.
pub struct R0Session {
    observer: CapacityObserver,
    writer: R0Writer,
    path: PathBuf,
    workload: R0Workload,
}

impl R0Session {
    /// Install the observer (suppressing physical retirement) and open the
    /// report. Call before preload or measurement mutations, then `begin`.
    pub fn start(
        db: &sui::Db,
        request: &R0Request,
        header: &R0Header,
        path: &Path,
    ) -> BackendResult<Self> {
        let scratch = path.with_extension("scratch");
        let mut config = CapacityObserverConfig {
            relocation_max_reclaim_pct: request.relocation_max_reclaim_pct,
            scratch_dir: Some(scratch),
            ..CapacityObserverConfig::default()
        };
        if let Some(records) = request.max_batch_records {
            config.max_batch_records = records;
        }
        // One shared stream: the observer thread appends samples through its
        // clone; `finish` appends the summary after the observer joined.
        let writer = R0Writer::create(path, header)?;
        let observer = db
            .start_capacity_observer(config, Box::new(writer.clone()))
            .map_err(|error| format!("R0 observer start: {error:?}"))?;
        Ok(Self {
            observer,
            writer,
            path: path.to_owned(),
            workload: request.workload,
        })
    }

    /// Complete initial owner inventory.
    pub fn begin(&self) -> BackendResult<()> {
        self.observer
            .begin()
            .map_err(|error| format!("R0 begin: {error}").into())
    }

    /// End inventory, observer join, then the final `r0.json`.
    pub fn finish(self) -> BackendResult<R0Summary> {
        let geometry = self.observer.geometry().clone();
        let summary = self
            .observer
            .finish()
            .map_err(|error| format!("R0 finish: {error}"))?;
        let summary = representative(self.workload, summary);
        self.writer.finish(&geometry, &summary)?;
        Ok(summary)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A Sui retention run without a prune event in the window is not a capacity
/// slope (design §2): report every slope `null` with the reason.
fn representative(workload: R0Workload, mut summary: R0Summary) -> R0Summary {
    if workload != R0Workload::SuiMainnetRetention || summary.prune_events != 0 {
        return summary;
    }
    let reason = "no Sui retention prune event in the measurement window".to_owned();
    for owner in &mut summary.per_owner {
        owner.net_allocated_growth_bytes_per_day = None;
        owner.reason = Some(reason.clone());
    }
    summary.worst_owner.partition = None;
    summary.worst_owner.net_allocated_growth_bytes_per_day = None;
    summary.worst_owner.reason = Some(reason.clone());
    summary.reasons.push(reason);
    summary
}

/// Report path for one node database: `r0.json`, or `r0-db<id>.json` when a
/// replay spans several node databases.
pub fn report_path(output: &Path, database: u16, databases: usize) -> PathBuf {
    if databases == 1 {
        output.join("r0.json")
    } else {
        output.join(format!("r0-db{database}.json"))
    }
}

#[cfg(test)]
mod tests;
