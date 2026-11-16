//! Admitted RESP replay only: standalone MULTI/EXEC or source-ordered primary runs.
//! Synthetic `KvBackend::put_batch` remains unsupported for these stores.
use super::replay::{
    FinalRows, ReplayBackend, ReplayCommitCapability, UnknownReplayMutation, physical_key,
    verify_rows,
};
use crate::backend::{BackendInfo, BackendResult, KvBackend};
use crate::mainnet_replay::Prepared;
use crate::mainnet_trace::{Row, Table, tables};
use redis::aio::MultiplexedConnection;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tidehunter::minibytes::Bytes;
use tokio::time::timeout;

// Explicit harness bounds on encoded requests and replies, not synthetic member limits.
const MAX_REQUEST: usize = 1_048_576;
const MAX_VALUE: usize = 536_870_912;
const MAX_MEMBERS: usize = 65_536;

fn bulk_size(length: usize) -> BackendResult<usize> {
    let digits = if length == 0 {
        1
    } else {
        length.ilog10() as usize + 1
    };
    length
        .checked_add(digits + 5)
        .ok_or_else(|| "RESP bulk size overflow".into())
}
fn set_size(row: &Row) -> BackendResult<usize> {
    4usize
        .checked_add(bulk_size(3)?)
        .and_then(|n| n.checked_add(bulk_size(row.backend_key.len()).ok()?))
        .and_then(|n| n.checked_add(bulk_size(row.value.len()).ok()?))
        .ok_or_else(|| "RESP SET size overflow".into())
}
fn check_value_size(length: usize) -> BackendResult<()> {
    if length > MAX_VALUE {
        return Err("RESP string value exceeds server bulk limit".into());
    }
    Ok(())
}
fn reply_size(members: usize) -> BackendResult<usize> {
    let digits = if members == 0 {
        1
    } else {
        members.ilog10() as usize + 1
    };
    members
        .checked_mul(14)
        .and_then(|n| n.checked_add(digits + 8))
        .ok_or_else(|| "RESP reply size overflow".into())
}
fn run_length(rows: &[Row]) -> usize {
    rows.iter()
        .take_while(|row| row.backend_owner == rows[0].backend_owner)
        .count()
}
fn check_rows(rows: &[Row], declarations: &[Table], db: Option<u16>) -> BackendResult<usize> {
    if rows.len() > MAX_MEMBERS {
        return Err("RESP replay reply/member bound exceeded".into());
    }
    // MULTI and EXEC add 29 encoded bytes. Reserve this even for cluster pipelines.
    let mut encoded = 29usize;
    for row in rows {
        let table = declarations
            .get(usize::from(row.table))
            .ok_or("unknown replay table")?;
        if table.key_bytes != row.key.len() || db.is_some_and(|db| db != table.db_id) {
            return Err("invalid key length or cross-DB replay batch".into());
        }
        check_value_size(row.value.len())?;
        encoded = encoded
            .checked_add(set_size(row)?)
            .ok_or("RESP request size overflow")?;
        if encoded > MAX_REQUEST {
            return Err("RESP replay encoded request exceeds 1 MiB bound".into());
        }
    }
    if reply_size(rows.len())? > MAX_REQUEST {
        return Err("RESP replay reply buffer exceeds 1 MiB bound".into());
    }
    Ok(encoded)
}

pub fn admit(prepared: &[Prepared], store: &str) -> BackendResult<Value> {
    let declarations = tables();
    let mut largest = 0;
    let mut physical = BTreeMap::new();
    for p in prepared {
        for chunk in p.preload.chunks(256) {
            largest = largest.max(check_rows(chunk, &declarations, Some(0))?);
        }
        for batch in &p.batches {
            largest = largest.max(check_rows(&batch.entries, &declarations, Some(batch.db))?);
        }
        for row in p
            .preload
            .iter()
            .chain(p.batches.iter().flat_map(|b| &b.entries))
            .chain(p.reads.iter().flatten().map(|r| &r.row))
        {
            let cluster = row.backend_key.starts_with(b"{}");
            if row.backend_key != physical_key(row.table, &row.key, cluster) {
                return Err("noninjective or incorrect physical table key".into());
            }
            let identity = (row.table, row.key.clone());
            if physical
                .insert(row.backend_key.clone(), identity.clone())
                .is_some_and(|previous| previous != identity)
            {
                return Err("cross-table physical key collision".into());
            }
        }
    }
    Ok(
        json!({"store":store,"maximum_encoded_request_bytes_including_transaction":largest,
        "request_buffer_limit":MAX_REQUEST,"reply_buffer_limit":MAX_REQUEST,"client_value_safety_limit":MAX_VALUE,
        "pinned_default_server_bulk_value_limit":if store=="garnet" {None}else{Some(MAX_VALUE)},
        "garnet_default_receive_ceiling":if store=="garnet" {Some(MAX_REQUEST)}else{None},
        "logical_commits_split":false,"physical_key_rows":physical.len()}),
    )
}

enum Reader {
    #[cfg(feature = "valkey-tcp")]
    Valkey(super::valkey_tcp::ValkeyTcpBackend),
    #[cfg(any(feature = "garnet", feature = "kvrocks"))]
    Industry(super::industry::IndustryBackend),
}
impl Reader {
    async fn open(kind: &str, options: &Value) -> BackendResult<Self> {
        match kind {
            #[cfg(feature = "valkey-tcp")]
            "valkey_tcp" => Ok(Self::Valkey(
                super::valkey_tcp::ValkeyTcpBackend::open(options, &prometheus::Registry::new())
                    .await?,
            )),
            #[cfg(feature = "garnet")]
            "garnet" => Ok(Self::Industry(
                super::industry::IndustryBackend::open(options, super::industry::Store::Garnet)
                    .await?,
            )),
            #[cfg(feature = "kvrocks")]
            "kvrocks" => Ok(Self::Industry(
                super::industry::IndustryBackend::open(options, super::industry::Store::Kvrocks)
                    .await?,
            )),
            _ => Err(format!("replay store feature unavailable: {kind}").into()),
        }
    }
    fn info(&self) -> BackendInfo {
        match self {
            #[cfg(feature = "valkey-tcp")]
            Self::Valkey(backend) => backend.info(),
            #[cfg(any(feature = "garnet", feature = "kvrocks"))]
            Self::Industry(backend) => backend.info(),
        }
    }
    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        match self {
            #[cfg(feature = "valkey-tcp")]
            Self::Valkey(backend) => backend.get(key).await,
            #[cfg(any(feature = "garnet", feature = "kvrocks"))]
            Self::Industry(backend) => backend.get(key).await,
        }
    }
}

/// The replay has W=1. This checkout cell transfers exclusive ownership of the
/// persistent writer connections; no guard spans serialization or I/O. Reads use
/// separate multiplexed clients and never take this lock. Busy checkout rejects.
/// Any failed/unknown mutation permanently poisons the writer, with no reconnect/retry.
pub struct RespReplayBackend {
    reader: Reader,
    writers: Mutex<Option<Vec<MultiplexedConnection>>>,
    runtime: tokio::runtime::Handle,
    timeout: Duration,
    cluster: bool,
    declarations: Vec<Table>,
    slot_owner: Vec<usize>,
    info: BackendInfo,
    // Relaxed counters are independent diagnostics, never publication/fencing state.
    sets: AtomicU64,
    runs: AtomicU64,
    transactions: AtomicU64,
    commands: AtomicU64,
    request_bytes: AtomicU64,
    completed_sets: AtomicU64,
}

fn require_empty_scan(cursor: u64, keys: &[Vec<u8>]) -> BackendResult<()> {
    // SCAN may return an empty page before finishing a nonempty keyspace.
    if cursor != 0 || !keys.is_empty() {
        return Err("RESP replay requires fresh empty owned databases".into());
    }
    Ok(())
}

// Physical cluster keys always begin with {}, so the whole binary key is hashed.
fn slot(key: &[u8]) -> usize {
    let mut crc = 0u16;
    for &byte in key {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 == 0 {
                crc << 1
            } else {
                (crc << 1) ^ 0x1021
            };
        }
    }
    usize::from(crc % 16384)
}
fn local_slots(nodes: &str) -> BackendResult<Vec<usize>> {
    let columns: Vec<_> = nodes
        .lines()
        .find(|line| {
            line.split_whitespace()
                .nth(2)
                .is_some_and(|flags| flags.split(',').any(|f| f == "myself"))
        })
        .ok_or("CLUSTER NODES lacks local primary")?
        .split_whitespace()
        .collect();
    let mut slots = Vec::new();
    for range in columns.get(8..).ok_or("invalid primary slot view")? {
        let (a, b) = range.split_once('-').unwrap_or((range, range));
        let a: usize = a.parse()?;
        let b: usize = b.parse()?;
        if a > b || b >= 16384 {
            return Err("invalid primary slot range".into());
        }
        slots.extend(a..=b);
    }
    Ok(slots)
}
fn okay(value: &redis::Value) -> bool {
    matches!(value, redis::Value::Okay)
        || matches!(value, redis::Value::SimpleString(s) if s == "OK")
}
fn check_replies(replies: &[redis::Value], members: usize, transaction: bool) -> BackendResult<()> {
    if !transaction {
        if replies.len() != members || !replies.iter().all(okay) {
            return Err("SET pipeline has failed, partial or missing replies".into());
        }
        return Ok(());
    }
    if replies.len() != members + 2 || !okay(&replies[0]) {
        return Err("MULTI/EXEC reply count or MULTI acknowledgement mismatch".into());
    }
    if !replies[1..members + 1]
        .iter()
        .all(|v| matches!(v, redis::Value::SimpleString(s) if s == "QUEUED"))
    {
        return Err("MULTI SET was not QUEUED".into());
    }
    match &replies[members + 1] {
        redis::Value::Array(values) if values.len() == members && values.iter().all(okay) => Ok(()),
        _ => Err("EXEC failed or returned an incomplete/error member array; execution errors are not rolled back".into()),
    }
}

impl RespReplayBackend {
    pub async fn open(kind: &str, options: &Value) -> BackendResult<Self> {
        let reader = Reader::open(kind, options).await?;
        let mut info = reader.info();
        let duration = Duration::from_millis(
            options["request_timeout_ms"]
                .as_u64()
                .ok_or("missing RESP timeout")?,
        );
        let cluster = options["cluster"] == true;
        let nodes = options["nodes"].as_array().ok_or("missing RESP nodes")?;
        let mut writers = Vec::with_capacity(nodes.len());
        let mut slot_owner = vec![usize::MAX; 16384];
        let mut limits = Vec::with_capacity(nodes.len());
        let mut initial_population = Vec::with_capacity(nodes.len());
        for (owner, node) in nodes.iter().enumerate() {
            let client =
                redis::Client::open(format!("redis://{}/", node.as_str().ok_or("invalid node")?))?;
            let mut conn = timeout(duration, client.get_multiplexed_async_connection()).await??;
            if cluster {
                let view: String = timeout(
                    duration,
                    redis::cmd("CLUSTER").arg("NODES").query_async(&mut conn),
                )
                .await??;
                for index in local_slots(&view)? {
                    if slot_owner[index] != usize::MAX {
                        return Err("duplicate replay slot ownership".into());
                    }
                    slot_owner[index] = owner;
                }
            }
            let mut observed_limits = BTreeMap::new();
            let keys: &[&str] = match kind {
                "valkey_tcp" => &["proto-max-bulk-len", "client-query-buffer-limit"],
                "kvrocks" => &["proto-max-bulk-len"],
                _ => &[],
            };
            for key in keys {
                let values: Vec<String> = timeout(
                    duration,
                    redis::cmd("CONFIG")
                        .arg("GET")
                        .arg(key)
                        .query_async(&mut conn),
                )
                .await??;
                if values.len() != 2
                    || values[0] != *key
                    || values[1].parse::<usize>()? < MAX_REQUEST
                {
                    return Err(
                        format!("RESP replay requires observed {key} >= {MAX_REQUEST}").into(),
                    );
                }
                observed_limits.insert((*key).to_owned(), values[1].clone());
            }
            limits.push(observed_limits);
            // Kvrocks DBSIZE can be a stale cached estimate. Require a terminal,
            // empty real scan; a nonterminal empty page does not prove freshness.
            let (cursor, keys): (u64, Vec<Vec<u8>>) = timeout(
                duration,
                redis::cmd("SCAN")
                    .arg(0)
                    .arg("COUNT")
                    .arg(1)
                    .query_async(&mut conn),
            )
            .await??;
            require_empty_scan(cursor, &keys)?;
            initial_population
                .push(json!({"node":node,"cursor":cursor,"returned_keys":keys.len()}));
            writers.push(conn);
        }
        if cluster && slot_owner.contains(&usize::MAX) {
            return Err("incomplete replay slot map".into());
        }
        if !cluster {
            slot_owner.fill(0);
        }
        info.topology["replay_transport"] = json!("TCP");
        info.topology["physical_hosts"] = json!("unverified; local lifecycle receipt required");
        info.topology["independent_ssds"] = Value::Null;
        info.topology["callers_per_ssd"] = Value::Null;
        info.configuration["replay_mutation_retries"] = json!(0);
        info.configuration["replay_write_routing"] = json!(
            "admitted fixed primary slot map; redirect/error stops case; no mutation retries"
        );
        info.configuration["replay_protocol_limits"] = json!(limits);
        info.configuration["replay_initial_population"] = json!(initial_population);
        Ok(Self {
            reader,
            writers: Mutex::new(Some(writers)),
            runtime: tokio::runtime::Handle::current(),
            timeout: duration,
            cluster,
            declarations: tables(),
            slot_owner,
            info,
            sets: AtomicU64::new(0),
            runs: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            commands: AtomicU64::new(0),
            request_bytes: AtomicU64::new(0),
            completed_sets: AtomicU64::new(0),
        })
    }
    pub fn info(&self) -> BackendInfo {
        self.info.clone()
    }
    fn checkout(&self) -> BackendResult<Vec<MultiplexedConnection>> {
        self.writers
            .lock()
            .map_err(|_| "RESP writer checkout poisoned")?
            .take()
            .ok_or_else(|| "RESP writer busy or poisoned by a prior unknown outcome".into())
    }
    fn restore(&self, writers: Vec<MultiplexedConnection>) -> BackendResult<()> {
        *self
            .writers
            .lock()
            .map_err(|_| "RESP writer checkout poisoned")? = Some(writers);
        Ok(())
    }
    async fn send(
        &self,
        conn: &mut MultiplexedConnection,
        rows: &[Row],
        transaction: bool,
    ) -> BackendResult<()> {
        let mut pipeline = redis::pipe();
        if transaction {
            pipeline.cmd("MULTI");
        }
        for row in rows {
            pipeline
                .cmd("SET")
                .arg(row.backend_key.as_ref())
                .arg(row.value.as_ref());
        }
        if transaction {
            pipeline.cmd("EXEC");
        }
        let count = rows.len() + if transaction { 2 } else { 0 };
        self.sets.fetch_add(rows.len() as u64, Ordering::Relaxed);
        self.commands.fetch_add(count as u64, Ordering::Relaxed);
        let encoded = rows.iter().try_fold(
            if transaction { 29usize } else { 0 },
            |bytes, row| -> BackendResult<usize> {
                bytes
                    .checked_add(set_size(row)?)
                    .ok_or_else(|| "RESP encoded size overflow".into())
            },
        )?;
        self.request_bytes
            .fetch_add(encoded as u64, Ordering::Relaxed);
        if transaction {
            self.transactions.fetch_add(1, Ordering::Relaxed);
        } else {
            self.runs.fetch_add(1, Ordering::Relaxed);
        }
        let replies = conn.send_packed_commands(&pipeline, 0, count).await?;
        check_replies(&replies, rows.len(), transaction)?;
        self.completed_sets
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}
impl ReplayBackend for RespReplayBackend {
    fn prepare_case(&self, prepared: &mut [Prepared]) -> BackendResult<Value> {
        let mut unique: Vec<BTreeSet<Bytes>> =
            vec![BTreeSet::new(); self.slot_owner.iter().copied().max().unwrap_or(0) + 1];
        let mut members = vec![0u64; unique.len()];
        let mut bytes = vec![0u64; unique.len()];
        let mut occupied = BTreeSet::new();
        let mut preload = vec![0u64; unique.len()];
        let mut preload_bytes = vec![0u64; unique.len()];
        let mut gets = vec![0u64; unique.len()];
        for p in prepared {
            for row in p
                .preload
                .iter_mut()
                .chain(p.batches.iter_mut().flat_map(|b| &mut b.entries))
                .chain(p.reads.iter_mut().flatten().map(|r| &mut r.row))
            {
                row.backend_owner = self.slot_owner[slot(&row.backend_key)];
            }
            for row in &p.preload {
                preload[row.backend_owner] += 1;
                preload_bytes[row.backend_owner] +=
                    (row.backend_key.len() + row.value.len()) as u64;
                unique[row.backend_owner].insert(row.backend_key.clone());
                occupied.insert(slot(&row.backend_key));
            }
            for read in p.reads.iter().flatten().filter(|r| r.producer.is_none()) {
                gets[read.row.backend_owner] += 1;
            }
            for row in p.batches.iter().flat_map(|b| &b.entries) {
                let owner = row.backend_owner;
                members[owner] += 1;
                bytes[owner] += (row.backend_key.len() + row.value.len()) as u64;
                unique[owner].insert(row.backend_key.clone());
                occupied.insert(slot(&row.backend_key));
            }
        }
        Ok(
            json!({"per_primary_unique_keys":unique.iter().map(BTreeSet::len).collect::<Vec<_>>(),"per_primary_put_members":members,"per_primary_client_key_value_bytes":bytes,"per_primary_preload_members":preload,"per_primary_preload_client_bytes":preload_bytes,"per_primary_measured_gets":gets,"occupied_slots":if self.cluster {Some(occupied.len())}else{None},"placement":"fully prepared binary keys; source-ordered contiguous primary runs"}),
        )
    }
    fn check_background_health(&self) -> BackendResult<()> {
        Ok(())
    }
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let prefix = if self.cluster { 2 } else { 0 };
        if (self.cluster && !key.starts_with(b"{}"))
            || key.get(prefix..prefix + 2) != Some(table.to_be_bytes().as_slice())
        {
            return Err("table-aware RESP get physical prefix mismatch".into());
        }
        self.runtime
            .block_on(async { timeout(self.timeout, self.reader.get(key)).await? })
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        check_rows(entries, &self.declarations, Some(db))?;
        if entries.is_empty() {
            return Ok(());
        }
        let mut writers = self.checkout()?;
        let result: BackendResult<()> = self.runtime.block_on(async {
            timeout(self.timeout, async {
                if !self.cluster {
                    return self.send(&mut writers[0], entries, true).await;
                }
                let mut start = 0;
                while start < entries.len() {
                    let owner = entries[start].backend_owner;
                    let length = run_length(&entries[start..]);
                    let conn = writers.get_mut(owner).ok_or("unadmitted primary owner")?;
                    // No later owner run is sent until every reply of this run is checked.
                    self.send(conn, &entries[start..start + length], false)
                        .await?;
                    start += length;
                }
                Ok(())
            })
            .await?
        });
        match result {
            Ok(()) => self.restore(writers),
            Err(error) => Err(Box::new(UnknownReplayMutation(error.to_string()))),
        }
    }
    fn commit_capability(&self) -> ReplayCommitCapability {
        ReplayCommitCapability {
            commit_mapping: if self.cluster {
                "ordered_set_pipeline"
            } else {
                "redis_multi_exec"
            },
            atomic_scope: if self.cluster {
                "single_key"
            } else {
                "node_db"
            },
            batch_visibility: if self.cluster {
                "partial_possible"
            } else {
                "atomic"
            },
            failure_atomicity: "none",
            member_order: "submitted_source_order",
            apply_order: "strict_source_order_including_cross_primary_runs",
            conflict_order: "source_order",
            key_encoding: if self.cluster {
                "empty_resp_tag_table_u16_be_logical_suffix"
            } else {
                "table_u16_be_logical_suffix"
            },
            value_layout: "raw_bcs",
            runtime_error_caveat: "EXEC execution errors do not roll back; pipeline/transport errors may leave partial application; unknown outcomes stop the case",
        }
    }
    fn physical_counts(&self) -> Value {
        json!({"sets_submitted":self.sets.load(Ordering::Relaxed),"sets_acknowledged":self.completed_sets.load(Ordering::Relaxed),"commands_submitted":self.commands.load(Ordering::Relaxed),"primary_pipeline_runs":self.runs.load(Ordering::Relaxed),"multi_exec_transactions":self.transactions.load(Ordering::Relaxed),"encoded_request_bytes":self.request_bytes.load(Ordering::Relaxed),"mutation_retries":0,"physical_clear_count":0,"chunk_count":0,"metadata_bytes":0,"internal_engine_key_overhead":"unmeasured (client prefix is not on-disk overhead)"})
    }
    fn verify_final(&self, expected: &FinalRows, deadline: Instant) -> BackendResult<Value> {
        let mut receipt = verify_rows(self, expected, deadline)?;
        let desired: BTreeSet<_> = expected
            .values()
            .map(|row| row.backend_key.clone())
            .collect();
        let mut found = BTreeSet::new();
        let mut writers = self.checkout()?;
        self.runtime.block_on(async {
            for conn in &mut writers {
                let mut cursor = 0u64;
                // A bounded fixture owns at most this many unique rows; SCAN may repeat keys.
                for _ in 0..=desired.len() + 1 {
                    let remaining = deadline
                        .checked_duration_since(Instant::now())
                        .ok_or("final SCAN deadline expired")?;
                    let (next, keys): (u64, Vec<Vec<u8>>) = timeout(
                        remaining.min(self.timeout),
                        redis::cmd("SCAN")
                            .arg(cursor)
                            .arg("COUNT")
                            .arg(256)
                            .query_async(conn),
                    )
                    .await??;
                    for key in keys {
                        let key = Bytes::from(key);
                        if !desired.contains(&key) {
                            return Err("unexpected row/table in final RESP database".into());
                        }
                        found.insert(key);
                    }
                    cursor = next;
                    if cursor == 0 {
                        break;
                    }
                }
                if cursor != 0 {
                    return Err("final SCAN iteration bound exceeded".into());
                }
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        self.restore(writers)?;
        if found != desired {
            return Err("final RESP row population differs from all 17 logical tables".into());
        }
        receipt["enumerated_physical_rows"] = json!(found.len());
        receipt["empty_marker_table_verified"] =
            json!(receipt["table_rows"][12].as_u64() == Some(0));
        receipt["fresh_empty_database_admitted"] = json!(
            self.info.configuration["replay_initial_population"]
                .as_array()
                .is_some_and(|probes| !probes.is_empty()
                    && probes
                        .iter()
                        .all(|probe| probe["cursor"].as_u64() == Some(0)
                            && probe["returned_keys"].as_u64() == Some(0)))
        );
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
