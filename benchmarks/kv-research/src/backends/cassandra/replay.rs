//! Seventeen real CQL tables; one logged batch per source node-DB batch.
use super::*;
use crate::backends::replay::{
    FinalRows, ReplayBackend, ReplayCommitCapability, UnknownReplayMutation, verify_rows,
};
use crate::mainnet_replay::Prepared;
use crate::mainnet_trace::{Row, tables};
use scylla::value::CqlValue;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::time::timeout;

static TABLES: std::sync::LazyLock<Vec<crate::mainnet_trace::Table>> =
    std::sync::LazyLock::new(tables);

enum ReplayKey<'a> {
    Singleton,
    Blob(&'a [u8]),
}
impl scylla::serialize::value::SerializeValue for ReplayKey<'_> {
    fn serialize<'b>(
        &self,
        typ: &scylla::frame::response::result::ColumnType,
        writer: scylla::serialize::writers::CellWriter<'b>,
    ) -> Result<
        scylla::serialize::writers::WrittenCellProof<'b>,
        scylla::serialize::SerializationError,
    > {
        match self {
            Self::Singleton => 0i8.serialize(typ, writer),
            Self::Blob(key) => key.serialize(typ, writer),
        }
    }
}

const LIMIT: usize = 1_048_576;
const TIMESTAMP_BASE: u64 = 1_700_000_000_000_000;
fn sizes(rows: &[Row], db: u16) -> BackendResult<(usize, usize)> {
    if rows.len() > u16::MAX as usize {
        return Err("CQL batch member limit exceeded".into());
    }
    let declarations = &*TABLES;
    // Native protocol v4: 9-byte header, type/count, consistency/flags.
    // Each member: prepared kind + short 16-byte ID + value count,
    // three length-prefixed values (key, value, explicit i64 timestamp).
    let mut encoded = 15usize;
    let mut final_members = BTreeMap::new();
    for row in rows {
        let table = declarations
            .get(usize::from(row.table))
            .ok_or("unknown CQL table")?;
        if table.db_id != db
            || row.key.len() != table.key_bytes
            || (row.table == 11 && row.backend_key.as_ref() != [0])
            || (row.table != 11 && row.backend_key != row.key)
        {
            return Err("invalid CQL key/table or cross-DB batch".into());
        }
        let key_bytes = if row.table == 11 { 1 } else { row.key.len() };
        encoded = encoded
            .checked_add(41 + key_bytes)
            .and_then(|n| n.checked_add(row.value.len()))
            .ok_or("CQL encoded size overflow")?;
        final_members.insert((row.table, row.key.clone()), row.value.len());
    }
    // Cassandra 5.0.9 BatchStatement.verifyBatchSize uses IMutation.dataSize,
    // not native frame bytes. For this exact non-TTL one-cell INSERT schema:
    // BTreeRow = liveness 8 + row deletion 13 + cell (8+4+8+value).
    // Empty static row contributes 21; partition deletion contributes 8.
    // Repeated partition/table members reconcile to the final timestamped value.
    // Source: apache/cassandra cassandra-5.0.9 {BatchStatement,PartitionUpdate,
    // BTreeRow,AbstractCell,LivenessInfo,Row.Deletion,MutableDeletionInfo}.
    let mutation = final_members
        .values()
        .try_fold(0usize, |n, len| n.checked_add(70 + len))
        .ok_or("CQL mutation size overflow")?;
    if encoded > LIMIT || mutation > LIMIT {
        return Err("CQL replay encoded batch or mutation exceeds 1 MiB guardrail".into());
    }
    Ok((encoded, mutation))
}
pub fn admit(prepared: &[Prepared]) -> BackendResult<JsonValue> {
    let mut maximum = (0, 0);
    for p in prepared {
        for rows in p.preload.chunks(256) {
            let s = sizes(rows, 0)?;
            maximum = (maximum.0.max(s.0), maximum.1.max(s.1));
        }
        for batch in &p.batches {
            let s = sizes(&batch.entries, batch.db)?;
            maximum = (maximum.0.max(s.0), maximum.1.max(s.1));
        }
    }
    Ok(
        json!({"maximum_encoded_native_batch_bytes":maximum.0,"maximum_cassandra_mutation_data_size":maximum.1,
        "batch_size_fail_threshold_bytes":LIMIT,"native_frame_safety_cap_bytes":LIMIT,
        "guardrail_accounting":"Cassandra 5.0.9 IMutation.dataSize; reconciled final one-cell INSERT per partition/table; value+70",
        "logical_commits_split":false,"singleton_table":11,"singleton_physical_key_bytes":1}),
    )
}
pub fn prepare_keys(prepared: &mut [Prepared]) {
    for p in prepared {
        for row in p
            .preload
            .iter_mut()
            .chain(p.batches.iter_mut().flat_map(|b| &mut b.entries))
            .chain(p.reads.iter_mut().flatten().map(|r| &mut r.row))
        {
            if row.table == 11 {
                row.backend_key = Bytes::from_static(&[0]);
            }
        }
    }
}
fn validate_receipt(value: &JsonValue, options: &Options) -> BackendResult<()> {
    if value["backend"] != "cassandra"
        || value["server_version"] != "5.0.9"
        || value["nodes"] != json!(options.nodes)
        || value["batch_size_fail_threshold_bytes"] != LIMIT
        || value["batch_size_warn_threshold_bytes"] != 5120
        || value["effective_yaml"].as_array().is_none_or(|nodes| {
            nodes.len() != options.nodes.len()
                || nodes.iter().any(|text| {
                    text.as_str()
                        .is_none_or(|s| !s.lines().any(|l| l == "batch_size_fail_threshold: 1MiB"))
                })
        })
        || value["effective_settings"].as_array().is_none_or(|nodes| {
            nodes.len() != options.nodes.len()
                || nodes.iter().zip(&options.nodes).any(|(node, endpoint)| {
                    node["node"].as_str() != Some(endpoint.as_str())
                        || node["settings"]["batch_size_fail_threshold"] != "1MiB"
                        || node["settings"]["batch_size_warn_threshold"] != "5KiB"
                        || node["settings"]["commitlog_sync"] != "periodic"
                        || node["settings"]["commitlog_sync_period"].as_str().is_none()
                })
        })
    {
        return Err("CQL replay requires captured Cassandra 5.0.9 effective 1 MiB guardrail receipt for every contact node".into());
    }
    Ok(())
}
pub struct CassandraReplayBackend {
    backend: CassandraBackend,
    runtime: tokio::runtime::Handle,
    gets: Vec<PreparedStatement>,
    puts: Vec<PreparedStatement>,
    names: Vec<String>,
    info: BackendInfo,
    // Relaxed: W=1 sequence and independent diagnostics, not data publication.
    sequence: AtomicU64,
    poisoned: AtomicBool,
    batches: AtomicU64,
    sets: AtomicU64,
    acknowledged: AtomicU64,
    encoded_bytes: AtomicU64,
}
impl CassandraReplayBackend {
    pub async fn open(options: &JsonValue) -> BackendResult<Self> {
        let parsed = parse_options(options)?;
        let path = parsed
            .replay_server_config_receipt
            .as_ref()
            .ok_or("CQL replay requires replay_server_config_receipt")?;
        if std::fs::metadata(path)?.len() > 1_048_576 {
            return Err("CQL receipt exceeds 1 MiB bound".into());
        }
        let receipt: JsonValue = serde_json::from_slice(&std::fs::read(path)?)?;
        validate_receipt(&receipt, &parsed)?;
        let backend = CassandraBackend::open(options, &Registry::new()).await?;
        if backend.info.server_version != "5.0.9" || backend.info.name == "scylla_cql" {
            return Err("CQL replay guardrail accounting requires admitted Cassandra 5.0.9, not an unverified compatible vendor".into());
        }
        let discovered: BTreeSet<String> = backend
            .session
            .get_cluster_state()
            .get_nodes_info()
            .iter()
            .map(|node| node.address.to_string())
            .collect();
        if discovered != parsed.nodes.iter().cloned().collect() {
            return Err(
                "CQL replay receipt must cover every discovered coordinator by its socket address"
                    .into(),
            );
        }
        // The receipt preserves the lifecycle config; query the running server's
        // virtual settings table too, so a stale YAML receipt is not admission.
        let settings = backend
            .session
            .query_unpaged("SELECT name, value FROM system_views.settings", &[])
            .await?
            .into_rows_result()?;
        let settings: BTreeMap<String, Option<String>> = settings
            .rows::<(String, Option<String>)>()?
            .collect::<Result<_, _>>()?;
        if settings
            .get("batch_size_fail_threshold")
            .and_then(Option::as_deref)
            != Some("1MiB")
            || settings
                .get("batch_size_warn_threshold")
                .and_then(Option::as_deref)
                != Some("5KiB")
        {
            return Err(
                "CQL running guardrail differs from captured 1 MiB fail / 5 KiB warn receipt"
                    .into(),
            );
        }
        let existing = backend
            .session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                (parsed.keyspace.as_str(),),
            )
            .await?
            .into_rows_result()?;
        let existing = existing
            .rows::<(String,)>()?
            .collect::<Result<Vec<_>, _>>()?;
        let scaffold = backend
            .session
            .query_unpaged(
                format!("SELECT k FROM {}.kv_research LIMIT 1", parsed.keyspace),
                &[],
            )
            .await?
            .into_rows_result()?;
        if existing != [("kv_research".to_owned(),)] || scaffold.rows_num() != 0 {
            return Err("CQL replay requires fresh owned keyspace, with only its empty admission scaffold table".into());
        }
        let mut gets = Vec::with_capacity(17);
        let mut puts = Vec::with_capacity(17);
        let mut names = Vec::with_capacity(17);
        for table in tables() {
            let name = format!("{}.{}", parsed.keyspace, table.name);
            let (key, schema) = if table.id == 11 {
                ("singleton", "singleton tinyint PRIMARY KEY, v blob")
            } else {
                ("k", "k blob PRIMARY KEY, v blob")
            };
            backend
                .session
                .query_unpaged(format!("CREATE TABLE {name} ({schema})"), &[])
                .await?;
            let count = backend
                .session
                .query_unpaged(format!("SELECT {key} FROM {name} LIMIT 1"), &[])
                .await?
                .into_rows_result()?;
            if count.rows_num() != 0 {
                return Err("CQL replay requires fresh empty owned tables".into());
            }
            let mut get = backend
                .session
                .prepare(format!("SELECT v FROM {name} WHERE {key} = ?"))
                .await?;
            let mut put = backend
                .session
                .prepare(format!(
                    "INSERT INTO {name} ({key}, v) VALUES (?, ?) USING TIMESTAMP ?"
                ))
                .await?;
            if put.get_id().len() != 16 {
                return Err("CQL prepared ID does not match exact encoded admission".into());
            }
            for statement in [&mut get, &mut put] {
                statement.set_consistency(backend.consistency);
                statement.set_request_timeout(Some(backend.request_timeout));
                // No driver mutation replay after an unknown acknowledgement.
                statement.set_is_idempotent(false);
            }
            gets.push(get);
            puts.push(put);
            names.push(name);
        }
        let mut info = backend.info();
        info.name = "cassandra_cql".into();
        info.topology["server_product"] = json!(
            "Apache Cassandra (effective local lifecycle config and virtual settings admitted)"
        );
        info.configuration["replay_server_config_receipt"] = receipt;
        info.configuration["replay_observed_settings"] = json!(settings);
        info.configuration["ack_contract"] = json!("periodic_sync");
        info.configuration["replay_tables"] = json!(names);
        info.configuration["replay_timestamp_base_microseconds"] = json!(TIMESTAMP_BASE);
        info.configuration["replay_initial_population"] =
            json!({"workload_tables_preexisting":0,"admission_scaffold_rows":0});
        info.acknowledgement="Cassandra LOGGED BATCH at admitted consistency; durable_writes=true; observed periodic commitlog sync (not sync at acknowledgement); device persistence unverified".into();
        if info.configuration["replay_observed_settings"]["commitlog_sync"] != "periodic" {
            return Err("CQL receipt expects periodic commitlog sync".into());
        }
        Ok(Self {
            backend,
            runtime: tokio::runtime::Handle::current(),
            gets,
            puts,
            names,
            info,
            sequence: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            batches: AtomicU64::new(0),
            sets: AtomicU64::new(0),
            acknowledged: AtomicU64::new(0),
            encoded_bytes: AtomicU64::new(0),
        })
    }
    pub fn info(&self) -> BackendInfo {
        self.info.clone()
    }
}
impl ReplayBackend for CassandraReplayBackend {
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let declaration = TABLES.get(usize::from(table)).ok_or("unknown CQL table")?;
        if (table == 11 && key != [0]) || (table != 11 && key.len() != declaration.key_bytes) {
            return Err("CQL table-aware key length or singleton mismatch".into());
        }
        let statement = self
            .gets
            .get(usize::from(table))
            .ok_or("unknown CQL table")?;
        self.runtime.block_on(async {
            timeout(self.backend.request_timeout, async {
                let result = if table == 11 {
                    self.backend
                        .session
                        .execute_unpaged(statement, (0i8,))
                        .await?
                } else {
                    self.backend
                        .session
                        .execute_unpaged(statement, (key,))
                        .await?
                };
                let rows = result.into_rows_result()?;
                let mut rows = rows.rows::<(Vec<u8>,)>()?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    rows.next().transpose()?.map(|(v,)| Bytes::from(v)),
                )
            })
            .await?
        })
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        let (encoded, _) = sizes(entries, db)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err("CQL mutation poisoned by unknown outcome".into());
        }
        let start = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(entries.len() as u64)
                    .filter(|n| *n < i64::MAX as u64 - TIMESTAMP_BASE)
            })
            .map_err(|_| "CQL timestamp sequence overflow")?;
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_consistency(self.backend.consistency);
        batch.set_serial_consistency(None);
        batch.set_request_timeout(Some(self.backend.request_timeout));
        batch.set_is_idempotent(false);
        let mut values = Vec::with_capacity(entries.len());
        for (index, row) in entries.iter().enumerate() {
            batch.append_statement(self.puts[usize::from(row.table)].clone());
            let key = if row.table == 11 {
                ReplayKey::Singleton
            } else {
                ReplayKey::Blob(&row.key)
            };
            values.push((
                key,
                row.value.as_ref(),
                (TIMESTAMP_BASE + start + index as u64) as i64,
            ));
        }
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.sets.fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.encoded_bytes
            .fetch_add(encoded as u64, Ordering::Relaxed);
        let result: BackendResult<()> = self.runtime.block_on(async {
            timeout(
                self.backend.request_timeout,
                self.backend.session.batch(&batch, values),
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
            commit_mapping: "cql_logged_batch",
            atomic_scope: "partition",
            batch_visibility: "partial_possible",
            failure_atomicity: "logged_eventual_completion",
            member_order: "submitted_source_order",
            apply_order: "backend_unordered",
            conflict_order: "explicit_source_timestamp",
            key_encoding: "real_cql_tables_singleton_tinyint_zero",
            value_layout: "raw_bcs",
            runtime_error_caveat: "batchlog eventual completion is not cross-partition isolation; unknown acknowledgement stops case",
        }
    }
    fn physical_counts(&self) -> JsonValue {
        json!({"logged_batch_submissions":self.batches.load(Ordering::Relaxed),"sets_submitted":self.sets.load(Ordering::Relaxed),
            "sets_acknowledged":self.acknowledged.load(Ordering::Relaxed),"encoded_request_bytes":self.encoded_bytes.load(Ordering::Relaxed),
            "timestamp_members_assigned":self.sequence.load(Ordering::Relaxed),
            "harness_mutation_retries":0,"physical_clear_count":0,"chunk_count":0,"metadata_bytes":0})
    }
    fn verify_final(&self, expected: &FinalRows, deadline: Instant) -> BackendResult<JsonValue> {
        let mut receipt = verify_rows(self, expected, deadline)?;
        let mut found = BTreeSet::new();
        self.runtime.block_on(async {
            for (id, name) in self.names.iter().enumerate() {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or("CQL final verification deadline")?;
                let key = if id == 11 { "singleton" } else { "k" };
                // LIMIT bounds unexpected-store memory as well as the iteration.
                let result = timeout(
                    remaining.min(self.backend.request_timeout),
                    self.backend.session.query_unpaged(
                        format!("SELECT {key}, v FROM {name} LIMIT {}", expected.len() + 1),
                        &[],
                    ),
                )
                .await??
                .into_rows_result()?;
                for row in result.rows::<scylla::value::Row>()? {
                    let row = row?;
                    let key = if id == 11 {
                        if row.columns[0] != Some(CqlValue::TinyInt(0)) {
                            return Err("invalid CQL singleton sentinel".into());
                        }
                        Bytes::new()
                    } else {
                        Bytes::copy_from_slice(
                            row.columns[0]
                                .as_ref()
                                .and_then(CqlValue::as_blob)
                                .ok_or("CQL final key missing")?,
                        )
                    };
                    let identity = (id as u16, key);
                    let desired = expected.get(&identity).ok_or("unexpected final CQL row")?;
                    if row.columns[1]
                        .as_ref()
                        .and_then(CqlValue::as_blob)
                        .map(Vec::as_slice)
                        != Some(desired.value.as_ref())
                    {
                        return Err("CQL enumerated value mismatch".into());
                    }
                    found.insert(identity);
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        if found.len() != expected.len() {
            return Err("CQL final population mismatch".into());
        }
        receipt["enumerated_physical_rows"] = json!(found.len());
        receipt["empty_marker_table_verified"] = json!(receipt["table_rows"][12] == 0);
        receipt["fresh_empty_database_admitted"] = json!(true);
        Ok(receipt)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn row(table: u16, len: usize) -> Row {
        let key = Bytes::from(vec![1; tables()[usize::from(table)].key_bytes]);
        Row {
            table,
            tx: 0,
            backend_key: if table == 11 {
                Bytes::from_static(&[0])
            } else {
                key.clone()
            },
            key,
            backend_owner: 0,
            value: Bytes::from(vec![0; len]),
        }
    }
    #[test]
    fn exact_encoding_singleton_empty_and_repeated_members() {
        assert_eq!(sizes(&[row(11, 0)], 0).unwrap(), (57, 70));
        assert_eq!(sizes(&[row(13, 10), row(13, 20)], 0).unwrap(), (207, 90));
        assert!(sizes(&[row(13, LIMIT)], 0).is_err());
        assert!(sizes(&[row(13, 1)], 1).is_err());
        let mut invalid = row(11, 1);
        invalid.key = Bytes::from(vec![0]);
        assert!(sizes(&[invalid], 0).is_err());
    }
    #[test]
    fn rejects_absent_or_wrong_guardrail_receipt() {
        let options=parse_options(&json!({"nodes":["127.0.0.11:9042"],"keyspace":"kv_comparison","required_replication_factor":1})).unwrap();
        assert!(validate_receipt(&json!({}), &options).is_err());
        let mut receipt = json!({"backend":"cassandra","server_version":"5.0.9","nodes":options.nodes,"batch_size_fail_threshold_bytes":LIMIT,"batch_size_warn_threshold_bytes":5120,"effective_yaml":["batch_size_fail_threshold: 1MiB\n"],"effective_settings":[{"node":"127.0.0.11:9042","settings":{"batch_size_fail_threshold":"1MiB","batch_size_warn_threshold":"5KiB","commitlog_sync":"periodic","commitlog_sync_period":"10000ms"}}]});
        validate_receipt(&receipt, &options).unwrap();
        receipt["batch_size_fail_threshold_bytes"] = json!(51200);
        assert!(validate_receipt(&receipt, &options).is_err());
        receipt["batch_size_fail_threshold_bytes"] = json!(LIMIT);
        receipt["effective_settings"][0]["settings"]["batch_size_fail_threshold"] = json!("50KiB");
        assert!(validate_receipt(&receipt, &options).is_err());
        receipt["effective_settings"] = json!([]);
        assert!(validate_receipt(&receipt, &options).is_err());
    }
}

#[cfg(test)]
#[test]
fn real_fixture_admits_full_large_row_and_guardrail_override() {
    let trace = crate::mainnet_trace::Trace::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap();
    let mut prepared = crate::mainnet_replay::prepare(&trace, 2).unwrap();
    prepare_keys(&mut prepared);
    let receipt = admit(&prepared).unwrap();
    assert!(
        receipt["maximum_encoded_native_batch_bytes"]
            .as_u64()
            .unwrap()
            < LIMIT as u64
    );
    assert!(
        receipt["maximum_cassandra_mutation_data_size"]
            .as_u64()
            .unwrap()
            > 51200
    );
    assert!(
        prepared
            .iter()
            .flat_map(|p| p.batches.iter().flat_map(|b| &b.entries))
            .any(|r| r.value.len() == 134995)
    );
    assert_eq!(TABLES.len(), 17);
}
