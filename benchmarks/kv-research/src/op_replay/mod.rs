//! Native-vs-leased replay of recorded Tidehunter operation streams
//! (`checkpoint-work/golden-sui/OPS-FORMAT.md`, versions 1 and 2).
//!
//! Each `ops-NNNN.bin` stream is replayed into a native `sui::Db` and into a
//! leased `sui::Db` (one in-process owner, rootless ordinary-file live I/O),
//! both opened with the recorded key shape and native configuration. Both
//! arms implement kv-research's [`ReplayBackend`](crate::backends::replay::ReplayBackend):
//! recorded point reads, put-only batches and the background-health boundary
//! go through that contract; the other trace operations stay on the arm.
//! Every recorded result is checked on each arm at its recorded point. At each
//! relocation fence both arms run one complete pass with the recorded filter
//! decisions, and every recorded-removed key must be absent. Recorded close
//! digests are the oracle for each reopen, including the closing reopen after
//! the stream. Version 2 relocation evidence is decoded and reported; its
//! exact comparison waits for the R1 observation API. The objects-compactor
//! qualification is a separate deterministic test ([`compactor_parity`]).
//!
//! The leased fixture needs the `test-utils` feature on Linux.
#![cfg(all(feature = "test-utils", target_os = "linux"))]

mod arm;
mod bcs;
mod compactor;
mod stream;
mod trace;

use crate::backend::BackendResult;
use serde::Serialize;
use std::path::{Path, PathBuf};

pub use compactor::{
    CompactorArm, CompactorReport, LeasedForce, compactor_parity, compactor_parity_with,
};
pub use stream::{Divergence, Gap, StreamReport, StreamStatus};

/// Incremental SHA-256 over trace rows: `u64 BE key length || key ||
/// u64 BE value length || value` per row (OPS-FORMAT.md).
pub(crate) struct RowHasher(ring::digest::Context);

impl RowHasher {
    pub(crate) fn new() -> Self {
        Self(ring::digest::Context::new(&ring::digest::SHA256))
    }

    pub(crate) fn row(&mut self, key: &[u8], value: &[u8]) {
        self.0.update(&(key.len() as u64).to_be_bytes());
        self.0.update(key);
        self.0.update(&(value.len() as u64).to_be_bytes());
        self.0.update(value);
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        digest_bytes(&self.0.finish())
    }
}

fn digest_bytes(digest: &ring::digest::Digest) -> [u8; 32] {
    let mut out = [0; 32];
    // SHA-256 digests are exactly 32 bytes.
    out.copy_from_slice(digest.as_ref());
    out
}

pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    digest_bytes(&ring::digest::digest(&ring::digest::SHA256, data))
}

/// SHA-256 of one row, as recorded by `IteratorStep`.
pub(crate) fn row_hash(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hasher = RowHasher::new();
    hasher.row(key, value);
    hasher.finish()
}

#[derive(Debug, Serialize)]
pub struct TraceReport {
    pub trace: PathBuf,
    pub passed: bool,
    pub streams: Vec<StreamReport>,
}

/// Replay every stream of the trace at `trace` into fresh arms under `work`,
/// which must not exist. Writes `op-replay.json` into `work` and returns the
/// same report. Streams are independent: one stream's failure does not stop
/// the others. Errors only for an unusable trace index or work directory.
pub fn replay_trace(trace: &Path, work: &Path) -> BackendResult<TraceReport> {
    replay_trace_inner(trace, work, false)
}

fn replay_trace_inner(
    trace: &Path,
    work: &Path,
    discard_databases: bool,
) -> BackendResult<TraceReport> {
    let index = trace::Index::load(trace)?;
    // The exact trace requires the objects compactor off; compaction parity is
    // qualified separately by `compactor_parity`.
    if index.objects_compactor_enabled {
        return Err("exact replay requires a trace recorded with the objects compactor off".into());
    }
    for (name, entry) in &index.databases {
        let perpetual = Path::new(name).ends_with("perpetual");
        let resolved_off = entry.objects_compactor_enabled == Some(false);
        if (perpetual && !resolved_off)
            || (!perpetual && entry.objects_compactor_enabled == Some(true))
        {
            return Err(format!("{name}: missing or enabled objects compactor setting").into());
        }
    }
    std::fs::create_dir(work).map_err(|error| format!("{}: {error}", work.display()))?;
    let version = index.format_version;
    let mut streams = Vec::with_capacity(index.databases.len());
    for (i, (name, entry)) in index.databases.into_iter().enumerate() {
        let dir = work.join(format!("db-{i:04}"));
        std::fs::create_dir(&dir)?;
        let path = trace.join(&entry.file);
        let replay = stream::StreamReplay::new(name, entry.file, entry.frames, version, &dir);
        streams.push(replay.run(&path));
        if discard_databases {
            std::fs::remove_dir_all(&dir)?;
        }
    }
    let report = TraceReport {
        trace: trace.to_path_buf(),
        passed: streams
            .iter()
            .all(|stream| stream.status == StreamStatus::Passed),
        streams,
    };
    std::fs::write(
        work.join("op-replay.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(report)
}

#[cfg(test)]
mod tests;
