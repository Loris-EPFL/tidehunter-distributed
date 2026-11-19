//! Reader for the Tidehunter operation trace, versions 1 and 2
//! (`checkpoint-work/golden-sui/OPS-FORMAT.md`). Truncated frames, bad CRCs,
//! unknown kinds, sequence gaps and frame-count mismatches are errors. The two
//! versions differ only in the relocation completion frame: tag 17 (v1) or
//! tag 20 with observed evidence and positioned callbacks (v2).

use super::bcs::Decoder;
use crate::backend::BackendResult;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path};

/// Stream header: `THOPS\0`, the format version, `\0`.
pub(super) const fn magic(version: u8) -> [u8; 8] {
    [b'T', b'H', b'O', b'P', b'S', 0, version, 0]
}
/// `seq` (8), `thread` (4), `kind` (1) and `keyspace` (4).
pub(super) const HEADER_LEN: usize = 17;
pub(super) const MAX_FRAME: usize = 512 * 1024 * 1024;
/// Keyspace field of database-wide calls.
pub(super) const DATABASE: u32 = u32::MAX;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Index {
    pub format_version: u32,
    pub objects_compactor_enabled: bool,
    pub databases: BTreeMap<String, Entry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub file: String,
    pub frames: u64,
    pub objects_compactor_enabled: Option<bool>,
}

impl Index {
    /// Load and validate `index.json`. A trace without it did not finish.
    pub(super) fn load(dir: &Path) -> BackendResult<Self> {
        let path = dir.join("index.json");
        let file = File::open(&path).map_err(|error| {
            format!(
                "{}: {error}; a trace without index.json did not finish",
                path.display()
            )
        })?;
        let index: Self = serde_json::from_reader(BufReader::new(file))?;
        if !matches!(index.format_version, 1 | 2) {
            return Err(format!(
                "unsupported operation trace index version {}",
                index.format_version
            )
            .into());
        }
        if index.databases.is_empty() {
            return Err("operation trace index lists no databases".into());
        }
        for (name, entry) in &index.databases {
            let file = Path::new(&entry.file);
            if file.components().count() != 1
                || !matches!(file.components().next(), Some(Component::Normal(_)))
            {
                return Err(format!("{name}: invalid stream file name {}", entry.file).into());
            }
        }
        Ok(index)
    }
}

/// Engine call result as recorded: errors are diagnostic text, so replay
/// compares success/failure and the value on success only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Outcome<T> {
    Ok(T),
    Err(String),
}

pub(super) type Status = Outcome<()>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Member {
    Put(u32, Vec<u8>, Vec<u8>),
    Delete(u32, Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FilterDecision {
    pub keyspace: String,
    pub key: Vec<u8>,
    pub value_sha256: [u8; 32],
    /// Native physical WAL offset of the examined record (v2 only).
    pub wal_position: Option<u64>,
    pub decision: u8,
}

/// Native relocation evidence recorded by trace version 2.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RelocationEvidence {
    /// `0 = NoWork`, `1 = DidWork`.
    pub gate: u8,
    pub streams: Vec<StreamObservation>,
    pub copied_survivors: u64,
    /// `0 = Completed`, the only defined witness.
    pub witness: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StreamObservation {
    pub partition: u32,
    pub fencing_generation: u64,
    pub processed_upper: u64,
    pub terminal_position: u64,
    pub rounded_target: u64,
}

/// Kinds in wire-tag order (OPS-FORMAT.md table).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Op {
    Create(OpenSpec),
    Open(OpenSpec),
    Drop {
        result: Status,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        result: Status,
    },
    Delete {
        key: Vec<u8>,
        result: Status,
    },
    Batch {
        members: Vec<Member>,
        result: Status,
    },
    Get {
        key: Vec<u8>,
        result: Outcome<Option<Vec<u8>>>,
    },
    Exists {
        key: Vec<u8>,
        result: Outcome<bool>,
    },
    MultiGet {
        keys: Vec<Vec<u8>>,
        result: Vec<Outcome<Option<Vec<u8>>>>,
    },
    MultiExists {
        keys: Vec<Vec<u8>>,
        result: Outcome<Vec<bool>>,
    },
    DropCells {
        from: Vec<u8>,
        to: Vec<u8>,
        result: Status,
    },
    Flush {
        result: Status,
    },
    Checkpoint {
        result: Status,
    },
    IteratorBegin {
        id: u64,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        reverse: bool,
        snapshot: bool,
    },
    IteratorStep {
        id: u64,
        result: Outcome<Option<[u8; 32]>>,
    },
    IteratorEnd {
        id: u64,
        count: u64,
        sha256: [u8; 32],
    },
    RelocationStart,
    RelocationComplete {
        decisions: Vec<FilterDecision>,
        removed: Vec<(u32, Vec<u8>)>,
        /// Present exactly for version 2 (tag 20).
        evidence: Option<RelocationEvidence>,
    },
    RebuildControl {
        result: Status,
    },
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OpenSpec {
    /// Canonical engine shape YAML; closures (filters, compactors) are not serialized.
    pub shape: String,
    /// Native `Config` YAML.
    pub config: String,
    pub durable: bool,
    /// Keyspace names whose relocation filter was active.
    pub filters: Vec<String>,
}

impl Op {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Create(_) => "create",
            Self::Open(_) => "open",
            Self::Drop { .. } => "drop",
            Self::Put { .. } => "put",
            Self::Delete { .. } => "delete",
            Self::Batch { .. } => "batch",
            Self::Get { .. } => "get",
            Self::Exists { .. } => "exists",
            Self::MultiGet { .. } => "multi_get",
            Self::MultiExists { .. } => "multi_exists",
            Self::DropCells { .. } => "drop_cells",
            Self::Flush { .. } => "flush",
            Self::Checkpoint { .. } => "checkpoint",
            Self::IteratorBegin { .. } => "iterator_begin",
            Self::IteratorStep { .. } => "iterator_step",
            Self::IteratorEnd { .. } => "iterator_end",
            Self::RelocationStart => "relocation_start",
            Self::RelocationComplete { .. } => "relocation_complete",
            Self::RebuildControl { .. } => "rebuild_control",
            Self::Close => "close",
        }
    }

    /// Database-wide calls carry the `DATABASE` keyspace field.
    pub(super) fn database_wide(&self) -> bool {
        matches!(
            self,
            Self::Create(_)
                | Self::Open(_)
                | Self::Drop { .. }
                | Self::Flush { .. }
                | Self::Checkpoint { .. }
                | Self::RebuildControl { .. }
                | Self::RelocationStart
                | Self::RelocationComplete { .. }
                | Self::Close
        )
    }

    fn decode(kind: u8, version: u8, payload: &[u8]) -> BackendResult<Self> {
        let mut d = Decoder::new(payload);
        let op = match kind {
            0 => Self::Create(open_spec(&mut d)?),
            1 => Self::Open(open_spec(&mut d)?),
            2 => Self::Drop {
                result: status(&mut d)?,
            },
            3 => Self::Put {
                key: d.bytes()?,
                value: d.bytes()?,
                result: status(&mut d)?,
            },
            4 => Self::Delete {
                key: d.bytes()?,
                result: status(&mut d)?,
            },
            5 => Self::Batch {
                members: d.vec(member)?,
                result: status(&mut d)?,
            },
            6 => Self::Get {
                key: d.bytes()?,
                result: outcome(&mut d, |d| d.option(Decoder::bytes))?,
            },
            7 => Self::Exists {
                key: d.bytes()?,
                result: outcome(&mut d, Decoder::bool)?,
            },
            8 => Self::MultiGet {
                keys: d.vec(Decoder::bytes)?,
                result: d.vec(|d| outcome(d, |d| d.option(Decoder::bytes)))?,
            },
            9 => Self::MultiExists {
                keys: d.vec(Decoder::bytes)?,
                result: outcome(&mut d, |d| d.vec(Decoder::bool))?,
            },
            10 => Self::DropCells {
                from: d.bytes()?,
                to: d.bytes()?,
                result: status(&mut d)?,
            },
            11 => Self::Flush {
                result: status(&mut d)?,
            },
            12 => Self::Checkpoint {
                result: status(&mut d)?,
            },
            13 => Self::IteratorBegin {
                id: d.u64()?,
                lower: d.option(Decoder::bytes)?,
                upper: d.option(Decoder::bytes)?,
                reverse: d.bool()?,
                snapshot: d.bool()?,
            },
            14 => Self::IteratorStep {
                id: d.u64()?,
                result: outcome(&mut d, |d| d.option(Decoder::hash))?,
            },
            15 => Self::IteratorEnd {
                id: d.u64()?,
                count: d.u64()?,
                sha256: d.hash()?,
            },
            16 => Self::RelocationStart,
            17 if version == 1 => Self::RelocationComplete {
                decisions: d.vec(|d| {
                    Ok(FilterDecision {
                        keyspace: d.string()?,
                        key: d.bytes()?,
                        value_sha256: d.hash()?,
                        wal_position: None,
                        decision: d.u8()?,
                    })
                })?,
                removed: d.vec(|d| Ok((d.u32()?, d.bytes()?)))?,
                evidence: None,
            },
            20 if version == 2 => {
                let evidence = relocation_evidence(&mut d)?;
                Self::RelocationComplete {
                    decisions: d.vec(|d| {
                        Ok(FilterDecision {
                            keyspace: d.string()?,
                            key: d.bytes()?,
                            value_sha256: d.hash()?,
                            wal_position: Some(d.u64()?),
                            decision: d.u8()?,
                        })
                    })?,
                    removed: d.vec(|d| Ok((d.u32()?, d.bytes()?)))?,
                    evidence: Some(evidence),
                }
            }
            18 => Self::RebuildControl {
                result: status(&mut d)?,
            },
            19 => Self::Close,
            other => {
                return Err(
                    format!("operation trace version {version} has no kind {other}").into(),
                );
            }
        };
        d.finish()?;
        Ok(op)
    }
}

fn relocation_evidence(d: &mut Decoder<'_>) -> BackendResult<RelocationEvidence> {
    let gate = d.uleb()?;
    if gate > 1 {
        return Err(format!("invalid relocation gate {gate}").into());
    }
    let streams = d.vec(|d| {
        Ok(StreamObservation {
            partition: d.u32()?,
            fencing_generation: d.u64()?,
            processed_upper: d.u64()?,
            terminal_position: d.u64()?,
            rounded_target: d.u64()?,
        })
    })?;
    let copied_survivors = d.u64()?;
    let witness = d.uleb()?;
    if witness != 0 {
        return Err(format!("invalid relocation witness {witness}").into());
    }
    Ok(RelocationEvidence {
        gate: u8::try_from(gate)?,
        streams,
        copied_survivors,
        witness: 0,
    })
}

fn open_spec(d: &mut Decoder<'_>) -> BackendResult<OpenSpec> {
    Ok(OpenSpec {
        shape: d.string()?,
        config: d.string()?,
        durable: d.bool()?,
        filters: d.vec(Decoder::string)?,
    })
}

fn outcome<'a, T>(
    d: &mut Decoder<'a>,
    ok: impl FnOnce(&mut Decoder<'a>) -> BackendResult<T>,
) -> BackendResult<Outcome<T>> {
    match d.uleb()? {
        0 => ok(d).map(Outcome::Ok),
        1 => d.string().map(Outcome::Err),
        other => Err(format!("invalid Outcome tag {other}").into()),
    }
}

fn status(d: &mut Decoder<'_>) -> BackendResult<Status> {
    outcome(d, |_| Ok(()))
}

fn member(d: &mut Decoder<'_>) -> BackendResult<Member> {
    match d.uleb()? {
        0 => Ok(Member::Put(d.u32()?, d.bytes()?, d.bytes()?)),
        1 => Ok(Member::Delete(d.u32()?, d.bytes()?)),
        other => Err(format!("invalid batch member tag {other}").into()),
    }
}

#[derive(Debug)]
pub(super) struct Frame {
    pub seq: u64,
    pub keyspace: u32,
    pub op: Op,
}

/// Sequential frame reader. One body buffer is reused for every frame.
pub(super) struct Reader {
    reader: BufReader<File>,
    body: Vec<u8>,
    next: u64,
    version: u8,
}

impl Reader {
    /// Open a stream whose header must carry the index's format `version`.
    pub(super) fn open(path: &Path, version: u32) -> BackendResult<Self> {
        let version = u8::try_from(version)?;
        let mut reader = BufReader::new(
            File::open(path).map_err(|error| format!("{}: {error}", path.display()))?,
        );
        let mut header = [0; 8];
        reader
            .read_exact(&mut header)
            .map_err(|error| format!("{}: missing trace header: {error}", path.display()))?;
        if header != magic(version) {
            return Err(format!(
                "{}: not an operation trace version {version}",
                path.display()
            )
            .into());
        }
        Ok(Self {
            reader,
            body: Vec::new(),
            next: 0,
            version,
        })
    }

    /// The next frame, or `None` at a clean end of file.
    pub(super) fn next_frame(&mut self) -> BackendResult<Option<Frame>> {
        let mut len = [0; 4];
        if self.reader.read(&mut len[..1])? == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut len[1..])
            .map_err(|error| format!("frame {}: truncated length: {error}", self.next))?;
        let len = usize::try_from(u32::from_le_bytes(len))?;
        if !(HEADER_LEN..=MAX_FRAME).contains(&len) {
            return Err(format!("frame {}: invalid body length {len}", self.next).into());
        }
        self.body.resize(len, 0);
        let mut crc = [0; 4];
        self.reader
            .read_exact(&mut self.body)
            .and_then(|()| self.reader.read_exact(&mut crc))
            .map_err(|error| format!("frame {}: truncated body: {error}", self.next))?;
        if u32::from_le_bytes(crc) != crc32fast::hash(&self.body) {
            return Err(format!("frame {}: CRC mismatch", self.next).into());
        }
        let (header, payload) = self.body.split_at(HEADER_LEN);
        let mut d = Decoder::new(header);
        let seq = d.u64()?;
        let _thread = d.u32()?;
        let kind = d.u8()?;
        let keyspace = d.u32()?;
        if seq != self.next {
            return Err(format!("frame sequence {seq} != expected {}", self.next).into());
        }
        let op = Op::decode(kind, self.version, payload)
            .map_err(|error| format!("frame {seq}: {error}"))?;
        self.next += 1;
        Ok(Some(Frame { seq, keyspace, op }))
    }
}

#[cfg(test)]
pub(super) mod encode;
#[cfg(test)]
mod tests;
