//! Source node-DB replay requests, distinct from atomic synthetic `put_batch`.
use crate::backend::{BackendInfo, BackendResult};
use crate::mainnet_trace::Row;
use blake2::{Blake2b512, Digest};
use serde::Serialize;
use std::collections::BTreeMap;
use tidehunter::minibytes::Bytes;

#[derive(Debug)]
pub struct UnsupportedReplayBackend(pub String);
impl std::fmt::Display for UnsupportedReplayBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported replay backend: {}", self.0)
    }
}
impl std::error::Error for UnsupportedReplayBackend {}

/// A failed mutation can have applied some or all members. Never retry the source batch.
#[derive(Debug)]
pub struct UnknownReplayMutation(pub String);
impl std::fmt::Display for UnknownReplayMutation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown/partial replay mutation: {}", self.0)
    }
}
impl std::error::Error for UnknownReplayMutation {}

#[derive(Clone, Debug, Serialize)]
pub struct ReplayCommitCapability {
    pub commit_mapping: &'static str,
    pub atomic_scope: &'static str,
    pub batch_visibility: &'static str,
    pub failure_atomicity: &'static str,
    pub member_order: &'static str,
    pub apply_order: &'static str,
    pub conflict_order: &'static str,
    pub key_encoding: &'static str,
    pub value_layout: &'static str,
    pub runtime_error_caveat: &'static str,
}
impl ReplayCommitCapability {
    pub const fn node_db() -> Self {
        Self {
            commit_mapping: "native_batch",
            atomic_scope: "node_db",
            batch_visibility: "atomic",
            failure_atomicity: "transaction",
            member_order: "submitted_source_order",
            apply_order: "atomic_publication",
            conflict_order: "source_order",
            key_encoding: "named_tables_unprefixed",
            value_layout: "raw_bcs",
            runtime_error_caveat: "timeout is not rollback; an unacknowledged commit may have applied",
        }
    }
    pub fn labels(&self, info: &BackendInfo) -> BackendResult<serde_json::Value> {
        let mut labels = serde_json::to_value(self)?;
        labels["ack_class"] = info
            .configuration
            .get("ack_contract")
            .cloned()
            .unwrap_or_else(|| {
                let native = info.configuration["durability"].as_str();
                let distributed = info.configuration["databases"].as_array().and_then(|dbs| {
                    let tier = dbs.first()?["configuration"]["durability_tier"].as_str()?;
                    dbs.iter()
                        .all(|db| db["configuration"]["durability_tier"] == tier)
                        .then_some(tier)
                });
                serde_json::json!(match native.or(distributed) {
                    Some("value_wal_barrier" | "synced") => "synced",
                    Some("page_cache") => "page_cache",
                    _ => "server_unverified",
                })
            });
        labels["acknowledgement"] = serde_json::json!(info.acknowledgement);
        labels["topology"] = info.topology.clone();
        labels["replication_factor"] = serde_json::json!(info.replication_factor);
        labels["replica_reads"] = serde_json::json!(false);
        labels["cache_read_tier"] = serde_json::json!(info.measurement_boundary);
        Ok(labels)
    }
}

pub type FinalRows = BTreeMap<(u16, Bytes), Row>;

/// One source node-DB batch request, with the declared backend semantics.
/// Calls run on bounded plain caller threads, not Tokio executor threads.
pub trait ReplayBackend: Send + Sync + 'static {
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>>;
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()>;
    /// Populate cold routing descriptors after topology admission, before timing.
    fn prepare_case(
        &self,
        _prepared: &mut [crate::mainnet_replay::Prepared],
    ) -> BackendResult<serde_json::Value> {
        Ok(serde_json::json!({"routing":"named_table"}))
    }
    fn check_background_health(&self) -> BackendResult<()>;
    fn commit_capability(&self) -> ReplayCommitCapability {
        ReplayCommitCapability::node_db()
    }
    /// Full real-store reads outside measurement. Network stores also enumerate all rows.
    fn verify_final(
        &self,
        expected: &FinalRows,
        deadline: std::time::Instant,
    ) -> BackendResult<serde_json::Value> {
        verify_rows(self, expected, deadline)
    }
    fn physical_counts(&self) -> serde_json::Value {
        serde_json::json!({"available":false,"reason":"backend does not expose physical command counters"})
    }
}

pub fn verify_rows<B: ReplayBackend + ?Sized>(
    backend: &B,
    expected: &FinalRows,
    deadline: std::time::Instant,
) -> BackendResult<serde_json::Value> {
    let mut tables = [0u64; 17];
    let mut hash = Blake2b512::new();
    for ((table, _), row) in expected {
        if std::time::Instant::now() >= deadline {
            return Err("final-row verification deadline expired".into());
        }
        let actual = backend.get(*table, &row.backend_key)?;
        if actual.as_ref() != Some(&row.value) {
            return Err(format!("final-row verification mismatch in table {table}").into());
        }
        hash.update(table.to_be_bytes());
        hash.update((row.key.len() as u64).to_be_bytes());
        hash.update(&row.key);
        hash.update((row.value.len() as u64).to_be_bytes());
        hash.update(actual.as_ref().ok_or("verified row unexpectedly missing")?);
        tables[usize::from(*table)] += 1;
    }
    Ok(
        serde_json::json!({"verified_rows":expected.len(),"table_rows":tables,"full_values":true,"content_blake2b512":format!("{:x}",hash.finalize()),"outside_measurement":true}),
    )
}

/// Cold injective table encoding. Leading empty RESP tag forces whole-key hashing.
pub fn physical_key(table: u16, logical: &Bytes, cluster_resp: bool) -> Bytes {
    let mut bytes = Vec::with_capacity(logical.len() + if cluster_resp { 4 } else { 2 });
    if cluster_resp {
        bytes.extend_from_slice(b"{}");
    }
    bytes.extend_from_slice(&table.to_be_bytes());
    bytes.extend_from_slice(logical);
    Bytes::from(bytes)
}

#[cfg(test)]
mod tests;
