//! RawKV replay deliberately does not implement atomic synthetic batching.
use super::*;
#[cfg(test)]
use crate::backends::replay::physical_key;
use crate::backends::replay::{
    FinalRows, ReplayBackend, ReplayCommitCapability, UnknownReplayMutation, verify_rows,
};
use crate::mainnet_replay::Prepared;
use crate::mainnet_trace::{Row, tables};
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::time::timeout;

static TABLES: std::sync::LazyLock<Vec<crate::mainnet_trace::Table>> =
    std::sync::LazyLock::new(tables);

const PAYLOAD_LIMIT: usize = 1_048_576;
fn varint(mut n: usize) -> usize {
    let mut bytes = 1;
    while n >= 128 {
        bytes += 1;
        n >>= 7;
    }
    bytes
}
fn encoded_pairs(rows: &[Row], db: u16) -> BackendResult<usize> {
    let declarations = &*TABLES;
    let mut size = if rows.is_empty() {
        0
    } else {
        1 + varint(rows.len()) + rows.len()
    };
    for row in rows {
        let table = declarations
            .get(usize::from(row.table))
            .ok_or("unknown TiKV table")?;
        if table.db_id != db
            || row.key.len() != table.key_bytes
            || row.backend_key.len() != row.key.len() + 2
            || row.backend_key.get(..2) != Some(row.table.to_be_bytes().as_slice())
            || row.backend_key.get(2..) != Some(row.key.as_ref())
        {
            return Err("invalid TiKV table/key or cross-DB batch".into());
        }
        // protobuf KvPair: length-delimited key and value (empty value omitted),
        // then the enclosing pairs field. The repeated zero TTLs are packed.
        let pair = 1
            + varint(row.backend_key.len())
            + row.backend_key.len()
            + if row.value.is_empty() {
                0
            } else {
                1 + varint(row.value.len()) + row.value.len()
            };
        size = size
            .checked_add(1 + varint(pair) + pair)
            .ok_or("TiKV request size overflow")?;
        if size > PAYLOAD_LIMIT {
            return Err("TiKV replay encoded pairs exceed 1 MiB safety cap".into());
        }
    }
    Ok(size)
}
pub fn admit(prepared: &[Prepared]) -> BackendResult<JsonValue> {
    let mut maximum = 0;
    for p in prepared {
        for rows in p.preload.chunks(256) {
            maximum = maximum.max(encoded_pairs(rows, 0)?);
        }
        for batch in &p.batches {
            maximum = maximum.max(encoded_pairs(&batch.entries, batch.db)?);
        }
    }
    Ok(
        json!({"variant":"raw_nonatomic","maximum_encoded_pairs_and_ttls_bytes":maximum,
        "payload_safety_cap":PAYLOAD_LIMIT,"routing_context_reservation_bytes":65536,
        "request_bound_bytes":PAYLOAD_LIMIT+65536,"logical_commits_split":false,
        "rpc_fanout_and_retries":"not observable through RawClient; internal bounded client retry plan retained"}),
    )
}
fn readable_bytes(value: &JsonValue) -> BackendResult<u64> {
    if let Some(bytes) = value.as_u64() {
        return Ok(bytes);
    }
    let text = value.as_str().ok_or("missing effective TiKV byte cap")?;
    for (suffix, multiplier) in [
        ("GiB", 1u64 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ] {
        if let Some(number) = text.strip_suffix(suffix) {
            return number
                .parse::<u64>()?
                .checked_mul(multiplier)
                .ok_or_else(|| "TiKV byte cap overflow".into());
        }
    }
    Err("unrecognized effective TiKV byte cap unit".into())
}
pub struct TikvReplayBackend {
    backend: TikvBackend,
    runtime: tokio::runtime::Handle,
    duration: Duration,
    info: BackendInfo,
    // Relaxed diagnostic counters do not publish data. W=1 mutation failure poisons subsequent writes.
    poisoned: AtomicBool,
    batches: AtomicU64,
    sets: AtomicU64,
    acknowledged: AtomicU64,
    bytes: AtomicU64,
}
impl TikvReplayBackend {
    pub async fn open(options: &JsonValue) -> BackendResult<Self> {
        let parsed = parse_options(options)?;
        let backend = TikvBackend::open(options, &Registry::new()).await?;
        let duration = Duration::from_millis(parsed.request_timeout_ms);
        let mut info = backend.info();
        let http = HttpClient::builder().timeout(duration).build()?;
        let stores = info.topology["store_metadata"]["stores"]
            .as_array()
            .ok_or("missing TiKV stores")?;
        let mut configs = Vec::with_capacity(stores.len());
        for store in stores {
            if store["store"]["version"] != "8.5.4" {
                return Err(
                    "TiKV replay receive-limit source admission requires pinned 8.5.4".into(),
                );
            }
            let address = store["store"]["status_address"]
                .as_str()
                .ok_or("missing TiKV status address")?;
            let config: JsonValue = http
                .get(format!("http://{address}/config"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let raft = readable_bytes(&config["raftstore"]["raft-entry-max-size"])?;
            if raft < (PAYLOAD_LIMIT + 65536) as u64 {
                return Err("TiKV effective Raft cap below admitted request bound".into());
            }
            configs.push(config);
        }
        if !timeout(duration, backend.client.scan_keys(Vec::<u8>::new().., 1))
            .await??
            .is_empty()
        {
            return Err("TiKV replay requires fresh empty owned database".into());
        }
        info.configuration["replay_effective_configs"] = json!(configs);
        info.configuration["replay_server_grpc_receive_limit"] = json!({
            "bytes": -1, "meaning": "unlimited by pinned server channel; harness request safety cap remains bounded",
            "source": "https://github.com/tikv/tikv/blob/v8.5.4/src/server/server.rs",
            "mechanism": "BuilderFactory::create_builder max_receive_message_len(-1)",
            "admission": "all observed store versions must be 8.5.4; not an independently queried runtime option"
        });
        info.configuration["replay_variant"] = json!("raw_nonatomic");
        info.configuration["ack_contract"] = json!("server_unverified");
        info.configuration["replay_retry_policy"] = json!(
            "tikv-client 0.4.0 default multi-region bounded retries; one aggregate replay deadline; no harness mutation retry"
        );
        info.configuration["replay_initial_population"] = json!({"scan_keys":0});
        Ok(Self {
            backend,
            runtime: tokio::runtime::Handle::current(),
            duration,
            info,
            poisoned: AtomicBool::new(false),
            batches: AtomicU64::new(0),
            sets: AtomicU64::new(0),
            acknowledged: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    }
    pub fn info(&self) -> BackendInfo {
        self.info.clone()
    }
}
impl ReplayBackend for TikvReplayBackend {
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        if key.get(..2) != Some(table.to_be_bytes().as_slice()) {
            return Err("TiKV physical table prefix mismatch".into());
        }
        self.runtime
            .block_on(async { timeout(self.duration, self.backend.get(key)).await? })
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        let bytes = encoded_pairs(entries, db)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err("TiKV mutation poisoned by unknown outcome".into());
        }
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.sets.fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let result: BackendResult<()> = self.runtime.block_on(async {
            timeout(
                self.duration,
                self.backend.client.batch_put(
                    entries
                        .iter()
                        .map(|r| (r.backend_key.to_vec(), r.value.to_vec())),
                ),
            )
            .await??;
            Ok(())
        });
        match result {
            Ok(()) => {
                self.acknowledged
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.poisoned.store(true, Ordering::Relaxed);
                Err(Box::new(UnknownReplayMutation(error.to_string())))
            }
        }
    }
    fn check_background_health(&self) -> BackendResult<()> {
        Ok(())
    }
    fn commit_capability(&self) -> ReplayCommitCapability {
        ReplayCommitCapability {
            commit_mapping: "raw_batch_put",
            atomic_scope: "single_key",
            batch_visibility: "partial_possible",
            failure_atomicity: "none",
            member_order: "submitted_source_order",
            apply_order: "backend_unordered",
            conflict_order: "backend_unordered",
            key_encoding: "table_u16_be_logical_suffix",
            value_layout: "raw_bcs",
            runtime_error_caveat: "raw_nonatomic; region-split partial application possible; errors stop case",
        }
    }
    fn physical_counts(&self) -> JsonValue {
        json!({"batch_put_submissions":self.batches.load(Ordering::Relaxed),"sets_submitted":self.sets.load(Ordering::Relaxed),
            "sets_acknowledged":self.acknowledged.load(Ordering::Relaxed),"encoded_pairs_and_ttls_bytes":self.bytes.load(Ordering::Relaxed),
            "physical_rpc_count":null,"internal_retry_count":null,"harness_mutation_retries":0,"physical_clear_count":0,"chunk_count":0,"metadata_bytes":0})
    }
    fn verify_final(&self, expected: &FinalRows, deadline: Instant) -> BackendResult<JsonValue> {
        let mut receipt = verify_rows(self, expected, deadline)?;
        let desired: BTreeSet<Vec<u8>> =
            expected.values().map(|r| r.backend_key.to_vec()).collect();
        let mut found = BTreeSet::new();
        self.runtime.block_on(async {
            let mut start = Vec::new();
            for _ in 0..=desired.len() / 256 + 1 {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or("TiKV verification deadline")?;
                let keys = timeout(
                    remaining.min(self.duration),
                    self.backend.client.scan_keys(start.clone().., 256),
                )
                .await??;
                if keys.is_empty() {
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                }
                let count = keys.len();
                for key in keys {
                    let key: Vec<u8> = key.into();
                    if !desired.contains(&key) {
                        return Err("unexpected final TiKV row".into());
                    }
                    start = key.clone();
                    found.insert(key);
                }
                start.push(0);
                if count < 256 {
                    return Ok(());
                }
            }
            Err("TiKV scan bound exceeded".into())
        })?;
        if found != desired {
            return Err("TiKV final population mismatch".into());
        }
        receipt["enumerated_physical_rows"] = json!(found.len());
        receipt["empty_marker_table_verified"] = json!(receipt["table_rows"][12] == 0);
        receipt["fresh_empty_database_admitted"] = json!(true);
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
