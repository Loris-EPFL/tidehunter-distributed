//! Test-only trace writer: the exact inverse of the reader, used to build
//! synthetic streams in the recorder's format.

use super::{DATABASE, FilterDecision, HEADER_LEN, Member, Op, OpenSpec, Outcome, magic};
use crate::backend::BackendResult;
use std::path::Path;

fn uleb(out: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 0x7f).to_le_bytes()[0];
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    uleb(out, value.len());
    out.extend_from_slice(value);
}

fn option(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            bytes(out, value);
        }
    }
}

fn outcome<T>(out: &mut Vec<u8>, value: &Outcome<T>, ok: impl FnOnce(&mut Vec<u8>, &T)) {
    match value {
        Outcome::Ok(value) => {
            uleb(out, 0);
            ok(out, value);
        }
        Outcome::Err(error) => {
            uleb(out, 1);
            bytes(out, error.as_bytes());
        }
    }
}

fn status(out: &mut Vec<u8>, value: &Outcome<()>) {
    outcome(out, value, |_, ()| ());
}

fn open_spec(out: &mut Vec<u8>, spec: &OpenSpec) {
    bytes(out, spec.shape.as_bytes());
    bytes(out, spec.config.as_bytes());
    out.push(u8::from(spec.durable));
    uleb(out, spec.filters.len());
    for filter in &spec.filters {
        bytes(out, filter.as_bytes());
    }
}

/// Wire kind and BCS payload of one operation.
pub(crate) fn payload(op: &Op) -> (u8, Vec<u8>) {
    let mut out = Vec::new();
    let kind = match op {
        Op::Create(spec) => {
            open_spec(&mut out, spec);
            0
        }
        Op::Open(spec) => {
            open_spec(&mut out, spec);
            1
        }
        Op::Drop { result } => {
            status(&mut out, result);
            2
        }
        Op::Put { key, value, result } => {
            bytes(&mut out, key);
            bytes(&mut out, value);
            status(&mut out, result);
            3
        }
        Op::Delete { key, result } => {
            bytes(&mut out, key);
            status(&mut out, result);
            4
        }
        Op::Batch { members, result } => {
            uleb(&mut out, members.len());
            for member in members {
                match member {
                    Member::Put(ks, key, value) => {
                        uleb(&mut out, 0);
                        out.extend_from_slice(&ks.to_le_bytes());
                        bytes(&mut out, key);
                        bytes(&mut out, value);
                    }
                    Member::Delete(ks, key) => {
                        uleb(&mut out, 1);
                        out.extend_from_slice(&ks.to_le_bytes());
                        bytes(&mut out, key);
                    }
                }
            }
            status(&mut out, result);
            5
        }
        Op::Get { key, result } => {
            bytes(&mut out, key);
            outcome(&mut out, result, |out, value| option(out, value.as_deref()));
            6
        }
        Op::Exists { key, result } => {
            bytes(&mut out, key);
            outcome(&mut out, result, |out, value| out.push(u8::from(*value)));
            7
        }
        Op::MultiGet { keys, result } => {
            uleb(&mut out, keys.len());
            keys.iter().for_each(|key| bytes(&mut out, key));
            uleb(&mut out, result.len());
            for item in result {
                outcome(&mut out, item, |out, value| option(out, value.as_deref()));
            }
            8
        }
        Op::MultiExists { keys, result } => {
            uleb(&mut out, keys.len());
            keys.iter().for_each(|key| bytes(&mut out, key));
            outcome(&mut out, result, |out, values| {
                uleb(out, values.len());
                out.extend(values.iter().map(|value| u8::from(*value)));
            });
            9
        }
        Op::DropCells { from, to, result } => {
            bytes(&mut out, from);
            bytes(&mut out, to);
            status(&mut out, result);
            10
        }
        Op::Flush { result } => {
            status(&mut out, result);
            11
        }
        Op::Checkpoint { result } => {
            status(&mut out, result);
            12
        }
        Op::IteratorBegin {
            id,
            lower,
            upper,
            reverse,
            snapshot,
        } => {
            out.extend_from_slice(&id.to_le_bytes());
            option(&mut out, lower.as_deref());
            option(&mut out, upper.as_deref());
            out.push(u8::from(*reverse));
            out.push(u8::from(*snapshot));
            13
        }
        Op::IteratorStep { id, result } => {
            out.extend_from_slice(&id.to_le_bytes());
            outcome(&mut out, result, |out, value| match value {
                None => out.push(0),
                Some(hash) => {
                    out.push(1);
                    out.extend_from_slice(hash);
                }
            });
            14
        }
        Op::IteratorEnd { id, count, sha256 } => {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
            out.extend_from_slice(sha256);
            15
        }
        Op::RelocationStart => 16,
        Op::RelocationComplete {
            decisions,
            removed,
            evidence,
        } => {
            if let Some(evidence) = evidence {
                uleb(&mut out, usize::from(evidence.gate));
                uleb(&mut out, evidence.streams.len());
                for stream in &evidence.streams {
                    out.extend_from_slice(&stream.partition.to_le_bytes());
                    for value in [
                        stream.fencing_generation,
                        stream.processed_upper,
                        stream.terminal_position,
                        stream.rounded_target,
                    ] {
                        out.extend_from_slice(&value.to_le_bytes());
                    }
                }
                out.extend_from_slice(&evidence.copied_survivors.to_le_bytes());
                uleb(&mut out, usize::from(evidence.witness));
            }
            uleb(&mut out, decisions.len());
            for FilterDecision {
                keyspace,
                key,
                value_sha256,
                wal_position,
                decision,
            } in decisions
            {
                bytes(&mut out, keyspace.as_bytes());
                bytes(&mut out, key);
                out.extend_from_slice(value_sha256);
                if evidence.is_some() {
                    out.extend_from_slice(&wal_position.unwrap_or(0).to_le_bytes());
                }
                out.push(*decision);
            }
            uleb(&mut out, removed.len());
            for (keyspace, key) in removed {
                out.extend_from_slice(&keyspace.to_le_bytes());
                bytes(&mut out, key);
            }
            if evidence.is_some() { 20 } else { 17 }
        }
        Op::RebuildControl { result } => {
            status(&mut out, result);
            18
        }
        Op::Close => 19,
    };
    (kind, out)
}

/// Encode one frame: length, body (`seq`, `thread`, `kind`, `keyspace`, payload), CRC.
pub(crate) fn frame(seq: u64, keyspace: u32, kind: u8, payload: &[u8]) -> BackendResult<Vec<u8>> {
    let mut body = Vec::with_capacity(HEADER_LEN + payload.len());
    body.extend_from_slice(&seq.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.push(kind);
    body.extend_from_slice(&keyspace.to_le_bytes());
    body.extend_from_slice(payload);
    let mut out = Vec::with_capacity(body.len() + 8);
    out.extend_from_slice(&u32::try_from(body.len())?.to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    Ok(out)
}

/// Builds one `ops-NNNN.bin` stream in recording order. An encoding error is
/// kept and returned by `write_trace`, so builders stay infallible.
pub(crate) struct StreamWriter {
    bytes: Vec<u8>,
    pub frames: u64,
    pub version: u8,
    error: Option<String>,
}

impl StreamWriter {
    pub(crate) fn new() -> Self {
        Self::with_version(1)
    }

    pub(crate) fn with_version(version: u8) -> Self {
        Self {
            bytes: magic(version).to_vec(),
            frames: 0,
            version,
            error: None,
        }
    }

    /// Append `op`; returns its sequence number (an iterator's ID is its begin sequence).
    pub(crate) fn push(&mut self, keyspace: u32, op: &Op) -> u64 {
        let (kind, payload) = payload(op);
        let seq = self.frames;
        match frame(seq, keyspace, kind, &payload) {
            Ok(frame) => self.bytes.extend(frame),
            Err(error) => {
                self.error
                    .get_or_insert_with(|| format!("frame {seq}: {error}"));
            }
        }
        self.frames += 1;
        seq
    }

    pub(crate) fn database(&mut self, op: &Op) -> u64 {
        self.push(DATABASE, op)
    }

    /// The stream bytes, or the first encoding error.
    pub(crate) fn finish(&self) -> BackendResult<&[u8]> {
        match &self.error {
            Some(error) => Err(error.clone().into()),
            None => Ok(&self.bytes),
        }
    }
}

/// Write a complete trace directory: streams plus `index.json`. All streams
/// share the first stream's format version.
pub(crate) fn write_trace(dir: &Path, streams: &[(&str, &StreamWriter)]) -> BackendResult<()> {
    std::fs::create_dir(dir)?;
    let mut databases = serde_json::Map::new();
    for (i, (name, stream)) in streams.iter().enumerate() {
        let file = format!("ops-{i:04}.bin");
        std::fs::write(dir.join(&file), stream.finish()?)?;
        databases.insert(
            (*name).to_owned(),
            serde_json::json!({"file": file, "frames": stream.frames, "objects_compactor_enabled": null}),
        );
    }
    let version = streams.first().map_or(1, |(_, stream)| stream.version);
    let index = serde_json::json!({
        "format_version": version, "objects_compactor_enabled": false, "databases": databases,
    });
    std::fs::write(dir.join("index.json"), serde_json::to_vec_pretty(&index)?)?;
    Ok(())
}
