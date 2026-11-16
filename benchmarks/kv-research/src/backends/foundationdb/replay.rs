//! Atomic node-DB transactions with the admitted fdb_chunked_values_v1 layout.
use super::{FoundationDbBackend, normalize_fdb_error};
use crate::backend::{BackendInfo, BackendResult, KvBackend};
use crate::backends::replay::{
    FinalRows, ReplayBackend, ReplayCommitCapability, UnknownReplayMutation, physical_key,
    verify_rows,
};
use crate::mainnet_replay::Prepared;
use crate::mainnet_trace::{Row, tables};
use foundationdb::{KeySelector, RangeOption};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tidehunter::minibytes::Bytes;

const CHUNK: usize = 100_000;
const MAX_VALUE: usize = 1_048_576;
const MAX_TRANSACTION: usize = 10_000_000;
const DESCRIPTOR: usize = 13;
static TABLES: LazyLock<Vec<crate::mainnet_trace::Table>> = LazyLock::new(tables);

pub fn key(table: u16, logical: &Bytes) -> BackendResult<Bytes> {
    let declarations = &*TABLES;
    let declaration = declarations
        .get(usize::from(table))
        .ok_or("unknown FDB table")?;
    if declaration.key_bytes != logical.len() {
        return Err("invalid FDB logical key length".into());
    }
    Ok(Bytes::from(FoundationDbBackend::key(&physical_key(
        table, logical, false,
    ))?))
}
fn suffix(base: &[u8], kind: u8, index: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(base.len() + 5);
    key.extend_from_slice(base);
    key.push(kind);
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn descriptor(length: usize) -> [u8; DESCRIPTOR] {
    let mut bytes = [0; DESCRIPTOR];
    bytes[0] = 1;
    bytes[1..9].copy_from_slice(&(length as u64).to_be_bytes());
    bytes[9..].copy_from_slice(&(length.div_ceil(CHUNK) as u32).to_be_bytes());
    bytes
}
fn decode(bytes: &[u8]) -> BackendResult<(usize, usize)> {
    if bytes.len() != DESCRIPTOR || bytes[0] != 1 {
        return Err("invalid FDB chunk descriptor".into());
    }
    let length = usize::try_from(u64::from_be_bytes(bytes[1..9].try_into()?))?;
    let count = u32::from_be_bytes(bytes[9..13].try_into()?) as usize;
    if length <= CHUNK || length > MAX_VALUE || count != length.div_ceil(CHUNK) {
        return Err("invalid FDB chunk length/count".into());
    }
    Ok((length, count))
}
fn check(rows: &[Row], db: u16) -> BackendResult<usize> {
    let declarations = &*TABLES;
    let mut affected = 0usize;
    for row in rows {
        let table = declarations
            .get(usize::from(row.table))
            .ok_or("unknown FDB table")?;
        if table.db_id != db
            || row.key.len() != table.key_bytes
            || row.backend_key.len() != table.key_bytes + 3
            || row.backend_key.get(..3)
                != Some([0, (row.table >> 8) as u8, row.table as u8].as_slice())
            || row.backend_key.get(3..) != Some(row.key.as_ref())
        {
            return Err("cross-DB or invalid physical FDB key".into());
        }
        if row.value.len() > MAX_VALUE {
            return Err("FDB replay logical value exceeds admitted maximum".into());
        }
        let n = if row.value.len() > CHUNK {
            row.value.len().div_ceil(CHUNK)
        } else {
            0
        };
        // Three range endpoints clear base and every stale suffix, independent of old length.
        affected = affected
            .checked_add(
                row.backend_key.len() * 3
                    + 10
                    + row.value.len()
                    + if n == 0 {
                        row.backend_key.len()
                    } else {
                        (n + 1) * (row.backend_key.len() + 5) + DESCRIPTOR
                    },
            )
            .ok_or("FDB transaction size overflow")?;
        if affected >= MAX_TRANSACTION {
            return Err("FDB replay transaction reaches hard affected-data limit".into());
        }
    }
    Ok(affected)
}
pub fn admit(prepared: &mut [Prepared]) -> BackendResult<Value> {
    let mut maximum = 0;
    for p in prepared {
        for row in p
            .preload
            .iter_mut()
            .chain(p.batches.iter_mut().flat_map(|b| &mut b.entries))
            .chain(p.reads.iter_mut().flatten().map(|r| &mut r.row))
        {
            row.backend_key = key(row.table, &row.key)?;
        }
        for rows in p.preload.chunks(256) {
            maximum = maximum.max(check(rows, 0)?);
        }
        for batch in &p.batches {
            maximum = maximum.max(check(&batch.entries, batch.db)?);
        }
    }
    Ok(
        json!({"value_layout":"fdb_chunked_values_v1","chunk_bytes":CHUNK,"logical_value_max_bytes":MAX_VALUE,"physical_value_max_bytes":CHUNK,"transaction_affected_data_limit":MAX_TRANSACTION,"maximum_admitted_affected_bytes":maximum,"descriptor_bytes":DESCRIPTOR,"logical_commits_split":false}),
    )
}

pub struct FoundationDbReplayBackend {
    backend: FoundationDbBackend,
    runtime: tokio::runtime::Handle,
    failed: AtomicBool,
    // Relaxed counters are diagnostics only; failed uses release/acquire to stop admission.
    sets: AtomicU64,
    attempted_sets: AtomicU64,
    clears: AtomicU64,
    chunks: AtomicU64,
    chunk_bytes: AtomicU64,
    metadata: AtomicU64,
    key_bytes: AtomicU64,
    value_bytes: AtomicU64,
    transactions: AtomicU64,
    attempts: AtomicU64,
    maybe_committed: AtomicU64,
}
impl FoundationDbReplayBackend {
    pub async fn open(options: &Value) -> BackendResult<Self> {
        let backend = FoundationDbBackend::open(options, &prometheus::Registry::new()).await?;
        let result = Self {
            backend,
            runtime: tokio::runtime::Handle::current(),
            failed: AtomicBool::new(false),
            sets: AtomicU64::new(0),
            attempted_sets: AtomicU64::new(0),
            clears: AtomicU64::new(0),
            chunks: AtomicU64::new(0),
            chunk_bytes: AtomicU64::new(0),
            metadata: AtomicU64::new(0),
            key_bytes: AtomicU64::new(0),
            value_bytes: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            maybe_committed: AtomicU64::new(0),
        };
        if !result
            .scan(1, Instant::now() + result.backend.timeout)
            .await?
            .is_empty()
        {
            return Err("FDB replay requires fresh empty owned database".into());
        }
        Ok(result)
    }
    pub fn info(&self) -> BackendInfo {
        let mut info = self.backend.info();
        info.configuration["replay_value_layout"] = json!("fdb_chunked_values_v1");
        info.configuration["ack_contract"] = json!("server_unverified");
        info.configuration["replay_initial_population"] = json!({"physical_rows":0});
        info.configuration["replay_key_encoding"] = json!({
            "ordinary_key_overhead_bytes":3,"suffix_bytes":5,
            "suffix":"fixed table-key length, kind u8, index u32 big endian",
            "metadata_kind":1,"chunk_kind":2,"chunk_first_index":0,
            "descriptor":"version u8=1; logical length u64 big endian; chunk count u32 big endian",
            "read_transaction":"ordinary row, descriptor and all declared chunks in one serializable transaction",
            "overwrite":"one base clear and one suffix-range clear before replacement in the same transaction"
        });
        info
    }
    async fn scan(&self, limit: usize, deadline: Instant) -> BackendResult<BTreeSet<Vec<u8>>> {
        let database = &self.backend.database;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            database.run(|tr, _| async move {
                let mut option = RangeOption::from((
                    KeySelector::first_greater_or_equal(b""),
                    KeySelector::first_greater_or_equal(b"\xff"),
                ));
                option.limit = Some(limit);
                let mut found = BTreeSet::new();
                for iteration in 1..=limit + 1 {
                    let rows = tr.get_range(&option, iteration, false).await?;
                    found.extend(rows.iter().map(|r| r.key().to_vec()));
                    let Some(next) = option.next_range(&rows) else {
                        return Ok(found);
                    };
                    option = next;
                }
                Err(foundationdb::FdbBindingError::CustomError(
                    "FDB scan iteration bound exceeded".into(),
                ))
            }),
        )
        .await?
        .map_err(normalize_fdb_error)
    }
    async fn read(&self, base: &[u8]) -> BackendResult<Option<Bytes>> {
        let metadata = suffix(base, 1, 0);
        tokio::time::timeout(
            self.backend.timeout,
            self.backend.database.run(|tr, _| {
                let metadata = &metadata;
                async move {
                    let raw = tr.get(base, false).await?;
                    let meta = tr.get(metadata, false).await?;
                    if raw.is_some() && meta.is_some() {
                        return Ok(Err(
                            "FDB row has both raw and chunked representations".into()
                        ));
                    }
                    let Some(meta) = meta else {
                        return Ok(Ok(raw.map(|v| Bytes::copy_from_slice(v.as_ref()))));
                    };
                    let (length, count) = match decode(meta.as_ref()) {
                        Ok(v) => v,
                        Err(e) => return Ok(Err(e)),
                    };
                    let mut value = Vec::with_capacity(length);
                    for i in 0..count {
                        let Some(chunk) = tr.get(&suffix(base, 2, i as u32), false).await? else {
                            return Ok(Err("FDB chunk is missing".into()));
                        };
                        if chunk.len() != (length - i * CHUNK).min(CHUNK) {
                            return Ok(Err("FDB chunk length mismatch".into()));
                        }
                        value.extend_from_slice(chunk.as_ref());
                    }
                    Ok(Ok(Some(Bytes::from(value))))
                }
            }),
        )
        .await?
        .map_err(normalize_fdb_error)?
    }
}
impl ReplayBackend for FoundationDbReplayBackend {
    fn get(&self, table: u16, base: &[u8]) -> BackendResult<Option<Bytes>> {
        let declarations = &*TABLES;
        let declaration = declarations
            .get(usize::from(table))
            .ok_or("unknown FDB table")?;
        if base.len() != declaration.key_bytes + 3
            || base.get(..3) != Some([0, (table >> 8) as u8, table as u8].as_slice())
        {
            return Err("table-aware FDB key mismatch".into());
        }
        self.runtime.block_on(self.read(base))
    }
    fn commit(&self, db: u16, rows: &[Row]) -> BackendResult<()> {
        check(rows, db)?;
        self.check_background_health()?;
        let physical_sets: u64 = rows
            .iter()
            .map(|row| {
                if row.value.len() > CHUNK {
                    row.value.len().div_ceil(CHUNK) as u64 + 1
                } else {
                    1
                }
            })
            .sum();
        let result = self.runtime.block_on(async {
            tokio::time::timeout(
                self.backend.timeout,
                self.backend.database.run(|tr, maybe| async move {
                    self.attempts.fetch_add(1, Ordering::Relaxed);
                    self.attempted_sets
                        .fetch_add(physical_sets, Ordering::Relaxed);
                    if bool::from(maybe) {
                        self.maybe_committed.fetch_add(1, Ordering::Relaxed);
                    }
                    for row in rows {
                        let base = row.backend_key.as_ref();
                        tr.clear(base);
                        tr.clear_range(&suffix(base, 1, 0), &suffix(base, 3, 0));
                        if row.value.len() <= CHUNK {
                            tr.set(base, &row.value);
                        } else {
                            tr.set(&suffix(base, 1, 0), &descriptor(row.value.len()));
                            for (i, chunk) in row.value.chunks(CHUNK).enumerate() {
                                tr.set(&suffix(base, 2, i as u32), chunk);
                            }
                        }
                    }
                    Ok(())
                }),
            )
            .await
        });
        match result {
            Ok(Ok(())) => {}
            other => {
                self.failed.store(true, Ordering::Release);
                return Err(Box::new(UnknownReplayMutation(format!(
                    "FDB transaction failed: {other:?}"
                ))));
            }
        }
        self.transactions.fetch_add(1, Ordering::Relaxed);
        for row in rows {
            let n = if row.value.len() > CHUNK {
                row.value.len().div_ceil(CHUNK)
            } else {
                0
            };
            self.sets
                .fetch_add(if n == 0 { 1 } else { n as u64 + 1 }, Ordering::Relaxed);
            if n != 0 {
                self.chunk_bytes
                    .fetch_add(row.value.len() as u64, Ordering::Relaxed);
            }
            self.clears.fetch_add(2, Ordering::Relaxed);
            self.chunks.fetch_add(n as u64, Ordering::Relaxed);
            self.metadata.fetch_add(
                if n == 0 { 0 } else { DESCRIPTOR as u64 },
                Ordering::Relaxed,
            );
            self.key_bytes.fetch_add(
                if n == 0 {
                    row.backend_key.len()
                } else {
                    (n + 1) * (row.backend_key.len() + 5)
                } as u64,
                Ordering::Relaxed,
            );
            self.value_bytes.fetch_add(
                (row.value.len() + if n == 0 { 0 } else { DESCRIPTOR }) as u64,
                Ordering::Relaxed,
            );
        }
        Ok(())
    }
    fn check_background_health(&self) -> BackendResult<()> {
        if self.failed.load(Ordering::Acquire) {
            Err("FDB replay stopped after unknown mutation".into())
        } else {
            Ok(())
        }
    }
    fn commit_capability(&self) -> ReplayCommitCapability {
        ReplayCommitCapability {
            commit_mapping: "fdb_transaction",
            key_encoding: "bounded_leading_byte_table_u16_be_fixed_length_kind_u32_suffix",
            value_layout: "fdb_chunked_values_v1",
            ..ReplayCommitCapability::node_db()
        }
    }
    fn physical_counts(&self) -> Value {
        json!({"physical_sets":self.sets.load(Ordering::Relaxed),"physical_sets_submitted_including_retries":self.attempted_sets.load(Ordering::Relaxed),"physical_clear_operations":self.clears.load(Ordering::Relaxed),"clear_scope":"acknowledged transactions: one base clear and one suffix range clear per logical set; deleted row count unobserved","chunk_count":self.chunks.load(Ordering::Relaxed),"chunk_bytes":self.chunk_bytes.load(Ordering::Relaxed),"metadata_bytes":self.metadata.load(Ordering::Relaxed),"physical_set_key_bytes":self.key_bytes.load(Ordering::Relaxed),"physical_set_value_bytes":self.value_bytes.load(Ordering::Relaxed),"transactions_acknowledged":self.transactions.load(Ordering::Relaxed),"transaction_attempts":self.attempts.load(Ordering::Relaxed),"maybe_committed_attempts":self.maybe_committed.load(Ordering::Relaxed),"counter_scope":"acknowledged mutations excluding retries except submitted sets and transaction attempts"})
    }
    fn verify_final(&self, expected: &FinalRows, deadline: Instant) -> BackendResult<Value> {
        let mut receipt = verify_rows(self, expected, deadline)?;
        let mut desired = BTreeSet::new();
        for row in expected.values() {
            if row.value.len() <= CHUNK {
                desired.insert(row.backend_key.to_vec());
            } else {
                desired.insert(suffix(&row.backend_key, 1, 0));
                for i in 0..row.value.len().div_ceil(CHUNK) {
                    desired.insert(suffix(&row.backend_key, 2, i as u32));
                }
            }
        }
        let actual = self
            .runtime
            .block_on(self.scan(desired.len() + 1, deadline))?;
        if actual != desired {
            return Err("FDB final physical row population mismatch".into());
        }
        receipt["enumerated_physical_rows"] = json!(actual.len());
        receipt["expected_physical_rows"] = json!(desired.len());
        receipt["fresh_empty_database_admitted"] = json!(true);
        receipt["empty_marker_table_verified"] = json!(receipt["table_rows"][12] == 0);
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn descriptor_limits_and_exact_large_value() {
        assert_eq!(decode(&descriptor(134_995)).unwrap(), (134_995, 2));
        for length in [0, CHUNK, MAX_VALUE + 1] {
            assert!(decode(&descriptor(length)).is_err());
        }
        assert!(decode(b"arbitrary BCS").is_err());
        let value = vec![7u8; 134_995];
        assert_eq!(
            value.chunks(CHUNK).map(<[u8]>::len).collect::<Vec<_>>(),
            [100_000, 34_995]
        );
    }
    #[test]
    fn fixture_admission_and_strict_limits() {
        let trace = crate::mainnet_trace::Trace::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../experiments/sui-mainnet/fixture/manifest.json"),
            "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
        )
        .unwrap();
        let mut prepared = crate::mainnet_replay::prepare(&trace, 2).unwrap();
        admit(&mut prepared).unwrap();
        let mut row = prepared[0]
            .batches
            .iter()
            .flat_map(|b| &b.entries)
            .find(|r| r.value.len() == 134_995)
            .unwrap()
            .clone();
        let db = tables()[usize::from(row.table)].db_id;
        assert!(check(&[row.clone()], db).is_ok());
        assert_ne!(
            suffix(&row.backend_key, 1, 0),
            suffix(&row.backend_key, 2, 0)
        );
        row.value = Bytes::from(vec![0; MAX_VALUE + 1]);
        assert!(check(&[row.clone()], db).is_err());
        row.value = Bytes::from(vec![0; CHUNK]);
        assert!(check(&vec![row.clone(); 101], db).is_err());
        assert!(check(&[row], db + 1).is_err());
        assert!(key(11, &Bytes::from_static(b"")).is_ok());
        assert!(key(11, &Bytes::from_static(b"x")).is_err());
    }
    /// Run only against the empty exact-owned FDB lifecycle provided by the qualifier.
    #[test]
    #[ignore = "requires KV_REPLAY_FDB_CLUSTER_FILE for an empty owned server"]
    fn owned_server_overwrite_and_strict_rejection() {
        let cluster = std::env::var("KV_REPLAY_FDB_CLUSTER_FILE").unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let backend = runtime
            .block_on(FoundationDbReplayBackend::open(&json!({
                "cluster_file":cluster,"required_replication_factor":1,"transaction_timeout_ms":5000
            })))
            .unwrap();
        let logical = Bytes::from(vec![7; 8]);
        let mut row = Row {
            table: 10,
            tx: 0,
            key: logical.clone(),
            backend_key: key(10, &logical).unwrap(),
            backend_owner: 0,
            value: Bytes::from(vec![9; 134_995]),
        };
        for length in [100_000, 134_995, 300_001, 134_995, 0, 134_995] {
            row.value = Bytes::from(vec![9; length]);
            backend.commit(tables()[10].db_id, &[row.clone()]).unwrap();
            assert_eq!(
                backend.get(10, &row.backend_key).unwrap(),
                Some(row.value.clone())
            );
            let expected = BTreeMap::from([((10, logical.clone()), row.clone())]);
            backend
                .verify_final(
                    &expected,
                    Instant::now() + std::time::Duration::from_secs(10),
                )
                .unwrap();
        }
        // The raw adapter must reject the real oversized value, never truncate/fallback.
        runtime.block_on(async {
            assert!(
                backend
                    .backend
                    .put(Bytes::from_static(b"strict"), row.value.clone())
                    .await
                    .is_err()
            );
            assert_eq!(backend.backend.get(b"strict").await.unwrap(), None);
        });
        row.value = Bytes::from(vec![0; MAX_VALUE + 1]);
        assert!(backend.commit(tables()[10].db_id, &[row.clone()]).is_err());
        assert_eq!(
            backend.get(10, &row.backend_key).unwrap().unwrap().len(),
            134_995
        );
        runtime
            .block_on(backend.backend.database.run(|tr, _| async move {
                tr.clear_range(b"", b"\xff");
                Ok(())
            }))
            .unwrap();
    }
}
