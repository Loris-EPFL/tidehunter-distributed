//! Bounded v1 archive-derived node KV trace admission. All byte views retain their file owner.
use crate::backend::BackendResult;
use blake2::{Blake2b512, Digest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tidehunter::minibytes::Bytes;

pub const SCOPE: &str = "archive-derivable node KV subset";
pub const DOMAIN: &[u8] = b"tidehunter-sui-mainnet-repeat-v1";
pub const OBJECTS: u16 = 13;
pub const CHECKPOINT_ROW: u32 = 65535;
const MAX_FILE: usize = 5_000_000;
const PHASE_DBS: [u16; 9] = [1, 1, 0, 1, 1, 2, 0, 0, 1];
const SOURCE_SHA: &str = "20c4b1839e5fd44353bc58c0f5a9c458a6a07160ea30b617f5e2648b7302effa";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub id: u16,
    pub db_id: u16,
    pub name: String,
    pub node_name: String,
    pub key_bytes: usize,
    pub key_shape: String,
    pub control: bool,
}

pub fn tables() -> Vec<Table> {
    [
        ("certified_checkpoints", 1, 8, "sequence_prefix_48"),
        ("checkpoint_by_digest", 1, 32, "digest_uniform"),
        ("checkpoint_content", 1, 32, "digest_uniform"),
        (
            "checkpoint_sequence_by_contents_digest",
            1,
            32,
            "digest_uniform",
        ),
        ("effects", 0, 32, "digest_reduction_0_16"),
        (
            "epoch_executed_transactions_to_checkpoint",
            2,
            32,
            "digest_reduction_0_16",
        ),
        ("events_2", 0, 32, "digest_uniform"),
        ("executed_effects", 0, 32, "digest_reduction_0_16"),
        (
            "executed_transaction_digests",
            0,
            48,
            "epoch_digest_prefix_140",
        ),
        (
            "executed_transactions_to_checkpoint",
            0,
            32,
            "digest_uniform",
        ),
        ("full_checkpoint_content_v2", 1, 8, "sequence_prefix_48"),
        ("highest_committed_checkpoint", 0, 0, "singleton"),
        (
            "object_per_epoch_marker_table_v2",
            0,
            56,
            "epoch_marker_prefix_76",
        ),
        ("objects", 0, 40, "object_fixed_40"),
        ("transactions", 0, 32, "digest_reduction_0_16"),
        ("unchanged_loaded_runtime_objects", 0, 32, "digest_uniform"),
        ("watermarks", 1, 4, "control"),
    ]
    .into_iter()
    .zip(0u16..)
    .map(|((name, db_id, key_bytes, shape), id)| Table {
        id,
        db_id,
        name: name.into(),
        node_name: if id == 5 {
            "executed_transactions_to_checkpoint"
        } else {
            name
        }
        .into(),
        key_bytes,
        key_shape: shape.into(),
        control: matches!(id, 11 | 16),
    })
    .collect()
}

#[derive(Clone, Debug)]
pub struct Row {
    pub table: u16,
    pub tx: u32,
    pub key: Bytes,
    /// Cold backend encoding; logical identity and byte accounting always use `key`.
    pub backend_key: Bytes,
    pub backend_owner: usize,
    pub value: Bytes,
}
#[derive(Clone, Debug)]
pub struct Read {
    pub row: Row,
    pub producer: Option<usize>,
}
#[derive(Clone, Debug)]
pub struct Batch {
    pub db: u16,
    pub phase: u16,
    pub entries: Vec<Row>,
}
#[derive(Debug)]
pub struct Trace {
    pub manifest: serde_json::Value,
    pub tables: Vec<Table>,
    pub sequence: u64,
    pub epoch: u64,
    pub transactions: usize,
    pub preload: Vec<Row>,
    pub batches: Vec<Batch>,
    pub reads: Vec<Vec<Read>>,
    pub dependencies: Vec<Vec<usize>>,
    selected_databases: Option<Vec<u8>>,
}
impl Trace {
    pub fn select_node_databases(&mut self, selected: &[u8]) -> BackendResult<()> {
        let declared = self.manifest["node_databases"]
            .as_array()
            .ok_or("missing node database declarations")?;
        require(
            !selected.is_empty()
                && selected.windows(2).all(|pair| pair[0] < pair[1])
                && selected
                    .iter()
                    .all(|id| declared.iter().any(|node| node["id"] == *id)),
            "node_databases must be a non-empty strictly increasing subset of manifest ids",
        )?;
        let tables = &self.tables;
        let includes = |row: &Row| selected.contains(&(tables[usize::from(row.table)].db_id as u8));
        self.preload.retain(includes);
        self.batches
            .retain(|batch| selected.contains(&(batch.db as u8)));
        for reads in &mut self.reads {
            reads.retain(|read| includes(&read.row));
        }
        for (dependencies, reads) in self.dependencies.iter_mut().zip(&self.reads) {
            dependencies
                .retain(|producer| reads.iter().any(|read| read.producer == Some(*producer)));
        }
        self.selected_databases = Some(selected.to_vec());
        Ok(())
    }

    pub fn scope(&self) -> String {
        let Some(selected) = &self.selected_databases else {
            return SCOPE.to_owned();
        };
        let ids: BTreeSet<_> = selected.iter().copied().map(u64::from).collect();
        let names = self.manifest["node_databases"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|node| node["id"].as_u64().is_some_and(|id| ids.contains(&id)))
            .filter_map(|node| node["name"].as_str())
            .collect::<Vec<_>>();
        format!("{SCOPE}, node databases: {}", names.join(","))
    }
}
#[derive(Debug)]
enum Record {
    Begin(u64, u64, u32),
    Batch(u16, u16, usize),
    Put(Row),
    Read(Row, bool),
    End,
}
fn require(ok: bool, message: &str) -> BackendResult<()> {
    if ok {
        Ok(())
    } else {
        Err(format!("mainnet trace: {message}").into())
    }
}
fn number<const N: usize>(bytes: &[u8], offset: usize) -> BackendResult<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or("mainnet trace offset overflow")?;
    bytes
        .get(offset..end)
        .ok_or("mainnet trace truncated number")?
        .try_into()
        .map_err(Into::into)
}
fn u16_at(b: &[u8], o: usize) -> BackendResult<u16> {
    Ok(u16::from_le_bytes(number(b, o)?))
}
fn u32_at(b: &[u8], o: usize) -> BackendResult<u32> {
    Ok(u32::from_le_bytes(number(b, o)?))
}
fn u64_at(b: &[u8], o: usize) -> BackendResult<u64> {
    Ok(u64::from_le_bytes(number(b, o)?))
}

fn decode(bytes: Bytes, preload: bool, tables: &[Table]) -> BackendResult<Vec<Record>> {
    require(
        (64..=MAX_FILE).contains(&bytes.len()),
        "file size outside bounds",
    )?;
    require(
        &bytes[..8] == if preload { b"THMNPRE1" } else { b"THMNTRC1" },
        "wrong magic",
    )?;
    require(
        u32_at(&bytes, 8)? == 1
            && u32_at(&bytes, 12)? == 64
            && u32_at(&bytes, 16)? == u32::from(!preload)
            && u32_at(&bytes, 20)? == 17
            && bytes[40..64] == [0; 24],
        "invalid format/header",
    )?;
    let count = usize::try_from(u64_at(&bytes, 24)?)?;
    require(
        count <= 100_000 && u64_at(&bytes, 32)? == (bytes.len() - 64) as u64,
        "header length/count mismatch",
    )?;
    let mut records = Vec::with_capacity(count);
    let mut offset = 64usize;
    while offset < bytes.len() {
        require(records.len() < count, "record count mismatch")?;
        let length = usize::try_from(u32_at(&bytes, offset)?)?;
        let end = offset.checked_add(length).ok_or("record offset overflow")?;
        require(length >= 8 && end <= bytes.len(), "invalid record length")?;
        require(
            bytes.get(offset + 5..offset + 8) == Some(&[0, 0, 0]),
            "nonzero reserved flags",
        )?;
        let b = &bytes[offset + 8..end];
        let kind = bytes[offset + 4];
        let record = match kind {
            1 => {
                require(!preload && b.len() == 20, "invalid checkpoint record")?;
                Record::Begin(u64_at(b, 0)?, u64_at(b, 8)?, u32_at(b, 16)?)
            }
            2 => {
                require(!preload && b.len() == 8, "invalid batch record")?;
                Record::Batch(
                    u16_at(b, 0)?,
                    u16_at(b, 2)?,
                    usize::try_from(u32_at(b, 4)?)?,
                )
            }
            3 | 4 => {
                let read = kind == 4;
                let start = if read { 16 } else { 12 };
                require(
                    b.len() >= start && !(read && preload),
                    "truncated/unexpected row",
                )?;
                let table = u16_at(b, if read { 4 } else { 0 })?;
                let tx = if read {
                    u32_at(b, 0)?
                } else {
                    u32::from(u16_at(b, 2)?)
                };
                let table_decl = tables.get(usize::from(table)).ok_or("unknown table")?;
                let kl = usize::try_from(u32_at(b, if read { 8 } else { 4 })?)?;
                let vl = usize::try_from(u32_at(b, if read { 12 } else { 8 })?)?;
                let payload = kl
                    .checked_add(vl)
                    .and_then(|n| n.checked_add(start))
                    .ok_or("row length overflow")?;
                require(
                    kl == table_decl.key_bytes && payload == b.len(),
                    "key/value length mismatch",
                )?;
                if read {
                    require(
                        table == OBJECTS && b[6] <= 1 && b[7] == 0,
                        "invalid read flags/table",
                    )?;
                }
                let key_start = offset.checked_add(8 + start).ok_or("key offset overflow")?;
                let value_start = key_start.checked_add(kl).ok_or("value offset overflow")?;
                let row = Row {
                    table,
                    tx,
                    key: bytes.slice(key_start..value_start),
                    backend_key: bytes.slice(key_start..value_start),
                    backend_owner: 0,
                    value: bytes.slice(value_start..end),
                };
                if read {
                    Record::Read(row, b[6] == 1)
                } else {
                    Record::Put(row)
                }
            }
            5 => {
                require(!preload && b.is_empty(), "invalid checkpoint end")?;
                Record::End
            }
            _ => return Err("mainnet trace unknown record kind".into()),
        };
        records.push(record);
        offset = end;
    }
    require(records.len() == count, "record count mismatch")?;
    Ok(records)
}

/// SHA-256 is a cold-path file identity check, using the same system utility as build provenance.
pub fn sha256(bytes: &[u8]) -> BackendResult<String> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("sha256sum stdin unavailable")?
        .write_all(bytes)?;
    let output = child.wait_with_output()?;
    require(output.status.success(), "sha256sum failed")?;
    let text = String::from_utf8(output.stdout)?;
    Ok(text
        .split_whitespace()
        .next()
        .ok_or("missing SHA-256")?
        .into())
}
fn regular(path: &Path) -> BackendResult<Bytes> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    // Pin the opened inode; reject final-component symlinks and avoid a raced
    // FIFO blocking admission. The read cap also covers growth after metadata().
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file() && metadata.len() <= MAX_FILE as u64,
        "fixture must be bounded regular file",
    )?;
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len())?);
    file.take(MAX_FILE as u64 + 1).read_to_end(&mut bytes)?;
    require(bytes.len() <= MAX_FILE, "fixture grew beyond file size cap")?;
    Ok(Bytes::from(bytes))
}
impl Trace {
    pub fn load(path: &Path, expected_sha: &str) -> BackendResult<Self> {
        let raw = regular(path)?;
        require(sha256(&raw)? == expected_sha, "manifest hash mismatch")?;
        let manifest: serde_json::Value = serde_json::from_slice(&raw)?;
        let mut canonical = serde_json::to_vec(&manifest)?;
        canonical.push(b'\n');
        require(
            canonical.as_slice() == &*raw,
            "manifest must be canonical JSON",
        )?;
        require(
            manifest["schema_version"] == 1
                && manifest["format_version"] == 1
                && manifest["network"] == "mainnet"
                && manifest["scope"] == SCOPE
                && manifest["checkpoint_window"] == 1,
            "wrong network/format/scope/window",
        )?;
        let declarations: Vec<Table> = serde_json::from_value(manifest["tables"].clone())?;
        require(
            declarations == tables(),
            "table declarations differ from v1",
        )?;
        require(
            manifest["source"]["sequence"] == 345_683_547u64
                && manifest["source"]["epoch"] == 1313
                && manifest["source"]["sha256"] == SOURCE_SHA,
            "wrong source identity",
        )?;
        let root = path.parent().ok_or("manifest parent unavailable")?;
        let files = manifest["files"]
            .as_object()
            .ok_or("missing fixture file map")?;
        let names: BTreeSet<_> = files.keys().map(String::as_str).collect();
        require(
            names
                == BTreeSet::from([
                    "trace.bin",
                    "preload.bin",
                    "profile.json",
                    "345683547.binpb.zst",
                ]),
            "unexpected fixture files",
        )?;
        let mut trace = None;
        let mut preload = None;
        for (name, identity) in files {
            let bytes = regular(&root.join(name))?;
            require(
                identity["bytes"].as_u64() == Some(bytes.len() as u64)
                    && identity["sha256"].as_str() == Some(sha256(&bytes)?.as_str()),
                "fixture hash/length mismatch",
            )?;
            match name.as_str() {
                "trace.bin" => trace = Some(bytes),
                "preload.bin" => preload = Some(bytes),
                _ => {}
            }
        }
        require(
            files["345683547.binpb.zst"]["sha256"] == SOURCE_SHA,
            "wrong retained source",
        )?;
        let mut result = Self::parse(
            trace.ok_or("missing trace")?,
            preload.ok_or("missing preload")?,
        )?;
        result.check_manifest(&manifest)?;
        result.manifest = manifest;
        Ok(result)
    }

    pub fn parse(trace: Bytes, preload: Bytes) -> BackendResult<Self> {
        let tables = tables();
        let records = decode(trace, false, &tables)?;
        let initial = decode(preload, true, &tables)?;
        let Some(Record::Begin(sequence, epoch, txs)) = records.first() else {
            return Err("missing checkpoint begin".into());
        };
        require(
            *sequence == 345_683_547 && *epoch == 1313 && (1..CHECKPOINT_ROW).contains(txs),
            "wrong checkpoint/transaction count",
        )?;
        require(
            matches!(records.last(), Some(Record::End)),
            "missing checkpoint end",
        )?;
        let transactions = usize::try_from(*txs)?;
        let mut baseline = BTreeMap::new();
        let mut preload = Vec::with_capacity(initial.len());
        for record in initial {
            let Record::Put(row) = record else {
                return Err("invalid preload record".into());
            };
            require(
                row.table == OBJECTS && row.tx == CHECKPOINT_ROW,
                "invalid preload row",
            )?;
            require(
                baseline
                    .insert(row.key.clone(), row.value.clone())
                    .is_none(),
                "duplicate preload key",
            )?;
            preload.push(row);
        }
        let mut batches = Vec::with_capacity(9);
        let mut demands = Vec::new();
        let mut i = 1usize;
        while i < records.len() - 1 {
            match &records[i] {
                Record::Read(row, overlay) => {
                    require(
                        batches.len() == 5 && row.tx < *txs,
                        "read outside execution stage/invalid ordinal",
                    )?;
                    demands.push((row.clone(), *overlay));
                    i += 1;
                }
                Record::Batch(db, phase, members) => {
                    require(
                        usize::from(*phase) == batches.len()
                            && PHASE_DBS.get(usize::from(*phase)) == Some(db)
                            && *members > 0,
                        "batch phase/database mismatch",
                    )?;
                    let end = i
                        .checked_add(1)
                        .and_then(|n| n.checked_add(*members))
                        .ok_or("batch count overflow")?;
                    let rows = records.get(i + 1..end).ok_or("truncated batch")?;
                    let mut entries = Vec::with_capacity(*members);
                    for record in rows {
                        let Record::Put(row) = record else {
                            return Err("batch member mismatch".into());
                        };
                        require(
                            tables[usize::from(row.table)].db_id == *db,
                            "cross-DB batch",
                        )?;
                        require(
                            row.tx == CHECKPOINT_ROW || row.tx < *txs,
                            "wrong put tx ordinal",
                        )?;
                        entries.push(row.clone());
                    }
                    batches.push(Batch {
                        db: *db,
                        phase: *phase,
                        entries,
                    });
                    i = end;
                }
                _ => return Err("unexpected record/checkpoint boundary".into()),
            }
        }
        require(batches.len() == 9, "missing batch phase")?;
        let mut outputs = BTreeMap::new();
        for row in &batches[7].entries {
            if row.table == OBJECTS {
                require(
                    row.tx < *txs
                        && outputs
                            .insert(row.key.clone(), (row.tx, row.value.clone()))
                            .is_none(),
                    "invalid/duplicate output ObjectKey",
                )?;
                require(
                    !baseline.contains_key(&row.key),
                    "preloaded in-window output",
                )?;
            }
        }
        let mut reads = vec![Vec::new(); transactions];
        let mut dependencies = vec![Vec::new(); transactions];
        let mut used = BTreeSet::new();
        let mut seen = BTreeSet::new();
        let mut previous = 0;
        for (row, overlay) in demands {
            require(row.tx >= previous, "read ordinal order")?;
            previous = row.tx;
            require(
                seen.insert((row.tx, row.key.clone())),
                "duplicate input demand",
            )?;
            let producer = if overlay {
                let (producer, value) = outputs.get(&row.key).ok_or("overlay missing producer")?;
                require(*producer < row.tx, "forward/cyclic dependency")?;
                require(*value == row.value, "conflicting ObjectKey bytes")?;
                Some(usize::try_from(*producer)?)
            } else {
                require(
                    baseline.get(&row.key) == Some(&row.value),
                    "missing preload reference/conflicting bytes",
                )?;
                used.insert(row.key.clone());
                None
            };
            let tx = usize::try_from(row.tx)?;
            if let Some(p) = producer
                && !dependencies[tx].contains(&p)
            {
                dependencies[tx].push(p);
            }
            reads[tx].push(Read { row, producer });
        }
        require(used.len() == baseline.len(), "unused preload/missing read")?;
        require(
            reads.iter().all(|r| !r.is_empty()),
            "missing transaction reads",
        )?;
        let watermark = batches[7].entries.last().ok_or("empty output batch")?;
        require(
            watermark.table == 11
                && watermark.key.is_empty()
                && watermark.value.as_ref() == sequence.to_le_bytes(),
            "missing commit watermark",
        )?;
        Ok(Self {
            manifest: serde_json::Value::Null,
            tables,
            sequence: *sequence,
            epoch: *epoch,
            transactions,
            preload,
            batches,
            reads,
            dependencies,
            selected_databases: None,
        })
    }
    fn check_manifest(&self, m: &serde_json::Value) -> BackendResult<()> {
        let store = self
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_none())
            .count();
        let overlay = self.reads.iter().flatten().count() - store;
        require(
            m["transactions"].as_u64() == Some(self.transactions as u64)
                && m["store_reads"].as_u64() == Some(store as u64)
                && m["overlay_demands"].as_u64() == Some(overlay as u64)
                && m["write_calls"] == 9
                && m["put_members"].as_u64()
                    == Some(self.batches.iter().map(|b| b.entries.len() as u64).sum())
                && m["preload"]["objects"].as_u64() == Some(self.preload.len() as u64),
            "manifest count mismatch",
        )?;
        require(
            m["preload"]["key_bytes"].as_u64()
                == Some(self.preload.iter().map(|r| r.key.len() as u64).sum())
                && m["preload"]["value_bytes"].as_u64()
                    == Some(self.preload.iter().map(|r| r.value.len() as u64).sum()),
            "preload byte maxima mismatch",
        )?;
        let declared = m["batches"].as_array().ok_or("missing batch limits")?;
        require(declared.len() == 9, "batch limit count mismatch")?;
        for (b, d) in self.batches.iter().zip(declared) {
            let keys: u64 = b.entries.iter().map(|r| r.key.len() as u64).sum();
            let values: u64 = b.entries.iter().map(|r| r.value.len() as u64).sum();
            let encoded = 96 + 32 * b.entries.len() as u64 + keys + values;
            require(
                d["db_id"] == b.db
                    && d["phase"] == b.phase
                    && d["members"].as_u64() == Some(b.entries.len() as u64)
                    && d["key_bytes"].as_u64() == Some(keys)
                    && d["value_bytes"].as_u64() == Some(values)
                    && d["owner_encoded_bytes_upper_bound"].as_u64() == Some(encoded),
                "over-limit/mismatched checkpoint batch",
            )?;
        }
        require(
            m["repetition"]["domain"].as_str() == std::str::from_utf8(DOMAIN).ok()
                && m["repetition"]["hash"] == "blake2b-512"
                && m["repetition"]["unchanged_control_table_ids"] == serde_json::json!([11, 16]),
            "wrong repetition rules",
        )
    }
}

pub fn expand_key(table: &Table, repeat: u64, key: &Bytes) -> BackendResult<Bytes> {
    if table.control || key.is_empty() {
        return Ok(key.clone());
    }
    let len = u32::try_from(key.len())?;
    let mut expanded = Vec::with_capacity(key.len());
    for block in 0..len.div_ceil(64) {
        let mut hash = Blake2b512::new();
        hash.update(DOMAIN);
        hash.update(table.db_id.to_le_bytes());
        hash.update(table.id.to_le_bytes());
        hash.update(repeat.to_le_bytes());
        hash.update(len.to_le_bytes());
        hash.update(key);
        hash.update(block.to_le_bytes());
        let digest = hash.finalize();
        expanded.extend_from_slice(&digest[..(key.len() - expanded.len()).min(64)]);
    }
    Ok(Bytes::from(expanded))
}

pub type ExpandedKeys = BTreeMap<(u16, Bytes), Bytes>;
/// Precompute every finite repetition key and reject collisions before backend opening.
pub fn expanded_keys(trace: &Trace, repetitions: u64) -> BackendResult<Vec<ExpandedKeys>> {
    let source: BTreeSet<_> = trace
        .preload
        .iter()
        .chain(trace.batches.iter().flat_map(|b| &b.entries))
        .chain(trace.reads.iter().flatten().map(|r| &r.row))
        .map(|r| (r.table, r.key.clone()))
        .collect();
    let mut seen: BTreeSet<(u16, Bytes)> = source
        .iter()
        .filter(|(id, _)| !trace.tables[usize::from(*id)].control)
        .cloned()
        .collect();
    let mut result = Vec::with_capacity(usize::try_from(repetitions)?);
    for repetition in 0..repetitions {
        let mut keys = BTreeMap::new();
        for (id, key) in &source {
            let table = &trace.tables[usize::from(*id)];
            let expanded = expand_key(table, repetition, key)?;
            if !table.control {
                require(
                    seen.insert((*id, expanded.clone())),
                    "repetition-key collision",
                )?;
            }
            keys.insert((*id, key.clone()), expanded);
        }
        result.push(keys);
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
