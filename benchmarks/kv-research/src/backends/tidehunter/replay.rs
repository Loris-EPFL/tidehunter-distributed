//! Three independent node DBs, named table keyspaces and one complete remote batch per commit.
use super::{Options, TidehunterBackend, open_with_shape, parse_options};
use crate::backend::{BackendInfo, BackendResult, native_error};
use crate::backends::native::replay::table_mutexes;
use crate::backends::replay::ReplayBackend;
use crate::mainnet_trace::{Row, Table, Trace};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use tidehunter::distributed::types::{BatchId, BatchVersion};
use tidehunter::key_shape::{
    KeyIndexing, KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType,
};
use tidehunter::minibytes::Bytes;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayOptions {
    databases: Vec<NodeOptions>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeOptions {
    db_id: u16,
    options: serde_json::Value,
}

fn options(value: &serde_json::Value) -> BackendResult<Vec<(u16, serde_json::Value, Options)>> {
    let parsed = ReplayOptions::deserialize(value)?.databases;
    if parsed.is_empty()
        || parsed.iter().any(|node| node.db_id > 2)
        || !parsed.windows(2).all(|pair| pair[0].db_id < pair[1].db_id)
    {
        return Err("distributed replay requires strictly increasing node DB ids in 0..=2".into());
    }
    let mut identities = BTreeSet::new();
    let mut paths = Vec::<std::path::PathBuf>::new();
    let mut result = Vec::with_capacity(3);
    for node in parsed {
        let opts = parse_options(&node.options)?;
        if let Some(remote) = &opts.external_remote_config {
            remote.validate()?;
            if opts
                .database
                .is_some_and(|db| db != remote.storage.database)
                || opts
                    .batch_limits
                    .is_some_and(|limits| limits != remote.storage.batch_limits)
            {
                return Err("conflicting external replay database identity or batch limits".into());
            }
        }
        let database = opts
            .external_remote_config
            .as_ref()
            .map(|c| c.storage.database)
            .or(opts.database)
            .ok_or("distributed replay requires an explicit database identity per node DB")?;
        if !identities.insert(database) {
            return Err("distributed replay node DB identities must be distinct".into());
        }
        if !opts.path.is_absolute()
            || opts
                .path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(
                "distributed replay paths must be absolute without parent components".into(),
            );
        }
        let resolved = resolve_path(&opts.path)?;
        if paths
            .iter()
            .any(|p| p.starts_with(&resolved) || resolved.starts_with(p))
        {
            return Err("distributed replay node DB paths must not overlap".into());
        }
        paths.push(resolved);
        if !opts.nvmeof_readers.is_empty() {
            return Err(
                "checkpoint replay requires owner RPC or live NVMe-oF, not sealed-WAL readers"
                    .into(),
            );
        }
        batch_limits(&opts)?.validate()?;
        result.push((node.db_id, node.options, opts));
    }
    let first = &result[0].2;
    for (_, _, other) in &result[1..] {
        if !first.transport.same_protocol(&other.transport)
            || first.durability_tier != other.durability_tier
            || first
                .external_remote_config
                .as_ref()
                .and_then(|c| c.live_nvme.as_ref())
                .map(|c| c.write_mode)
                != other
                    .external_remote_config
                    .as_ref()
                    .and_then(|c| c.live_nvme.as_ref())
                    .map(|c| c.write_mode)
        {
            return Err("distributed replay node DBs must use the same transport, live write mode and durability tier".into());
        }
    }
    Ok(result)
}
fn resolve_path(path: &std::path::Path) -> BackendResult<std::path::PathBuf> {
    let mut ancestor = path;
    let mut tail = Vec::new();
    while !ancestor.exists() {
        tail.push(ancestor.file_name().ok_or("invalid replay path")?);
        ancestor = ancestor.parent().ok_or("invalid replay path ancestor")?;
    }
    let mut resolved = ancestor.canonicalize()?;
    for part in tail.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}
fn batch_limits(opts: &Options) -> BackendResult<BatchLimits> {
    opts.external_remote_config
        .as_ref()
        .map(|c| c.storage.batch_limits)
        .or(opts.batch_limits)
        .ok_or_else(|| "distributed replay requires explicit complete-batch limits".into())
}
pub(super) fn shape(tables: &[Table], db: u16) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    for table in tables.iter().filter(|t| t.db_id == db) {
        // Match the pinned production Sui declarations used by native replay,
        // including digest reduction; no synthetic root or namespace prefix.
        let indexing = if table.key_shape == "digest_reduction_0_16" {
            KeyIndexing::key_reduction(32, 0..16)
        } else {
            KeyIndexing::fixed(table.key_bytes)
        };
        let key_type = match table.key_shape.as_str() {
            "sequence_prefix_48" => KeyType::from_prefix_bits(48),
            "epoch_digest_prefix_140" => KeyType::from_prefix_bits(140),
            "epoch_marker_prefix_76" => KeyType::from_prefix_bits(76),
            _ => KeyType::uniform(1),
        };
        builder.add_key_space_config_indexing(
            &table.node_name,
            indexing,
            table_mutexes(table),
            key_type,
            KeySpaceConfig::default(),
        );
    }
    builder.build()
}

pub struct TidehunterReplayBackend {
    databases: BTreeMap<u16, TidehunterBackend>,
    tables: Vec<Option<(u16, KeySpace)>>,
}
impl TidehunterReplayBackend {
    pub fn validate(value: &serde_json::Value, trace: &Trace) -> BackendResult<()> {
        let parsed = options(value)?;
        let expected: BTreeSet<_> = trace.batches.iter().map(|batch| batch.db).collect();
        if parsed.iter().map(|(id, _, _)| *id).collect::<BTreeSet<_>>() != expected {
            return Err(
                "distributed replay databases must match workload.node_databases exactly".into(),
            );
        }
        for (id, _, opts) in &parsed {
            let shape = shape(&trace.tables, *id);
            let limits = batch_limits(opts)?;
            // Use the actual codec on the immutable source rows before opening owners.
            // Expanded keys have identical lengths. Never split the 519-member output batch.
            for entries in trace
                .batches
                .iter()
                .filter(|b| b.db == *id)
                .map(|b| b.entries.as_slice())
                .chain(trace.preload.chunks(256).filter(|_| *id == 0))
            {
                let mutations = entries
                    .iter()
                    .map(|r| Mutation::Put {
                        keyspace: shape
                            .iter_ks()
                            .find(|ks| ks.name() == trace.tables[usize::from(r.table)].node_name)
                            .expect("declared table")
                            .id(),
                        key: r.key.clone(),
                        value: r.value.clone(),
                    })
                    .collect();
                CompleteBatch::new(
                    &shape,
                    BatchId {
                        database: [0; 16],
                        operation: [0; 16],
                    },
                    BatchVersion {
                        epoch: 1,
                        sequence: 0,
                    },
                    mutations,
                    limits,
                )
                .map_err(|e| format!("distributed replay DB {id} complete-batch admission: {e}"))?;
            }
        }
        Ok(())
    }
    pub(crate) fn coordinator_reservation(
        value: &serde_json::Value,
        trace: &Trace,
        repetitions: u64,
    ) -> BackendResult<serde_json::Value> {
        resources::reservation(value, trace, repetitions)
    }
    pub fn geometry(value: &serde_json::Value) -> BackendResult<serde_json::Value> {
        let opts = options(value)?;
        let mut wal = 0;
        for (_, _, node) in &opts {
            let bytes = node
                .external_remote_config
                .as_ref()
                .map_or(node.owner_wal_geometry.wal_file_bytes(), |c| {
                    c.storage.wal_file_bytes
                });
            wal = wal.max(
                bytes
                    .checked_mul(node.owners as u64)
                    .ok_or("replay owner WAL reservation overflow")?,
            );
        }
        Ok(serde_json::json!({
            "wal_file_size":wal, "admission_path":opts[0].2.path,
            "databases":value["databases"], "node_databases":opts.len(),
            "owner_disk_admission":"local owner roots share their per-DB coordinator filesystem; external owner capacity is not observable here",
            "atomicity":"complete batch within one node DB; no cross-DB atomicity",
            "scope":crate::mainnet_trace::SCOPE
        }))
    }
    pub fn validate_disk_paths(value: &serde_json::Value, reservation: u64) -> BackendResult<()> {
        for (_, _, node) in options(value)? {
            let path = resolve_path(&node.path)?;
            let mut ancestor = path.as_path();
            while !ancestor.exists() {
                ancestor = ancestor.parent().ok_or("replay disk path absent")?;
            }
            let output = std::process::Command::new("df")
                .args(["-B1", "--output=avail", "--"])
                .arg(ancestor)
                .output()?;
            if !output.status.success() {
                return Err("distributed replay disk admission failed".into());
            }
            let available = std::str::from_utf8(&output.stdout)?
                .lines()
                .last()
                .ok_or("missing disk space")?
                .trim()
                .parse::<u64>()?;
            if available < reservation {
                return Err(format!(
                    "insufficient distributed replay disk space at {}",
                    ancestor.display()
                )
                .into());
            }
        }
        Ok(())
    }
    pub async fn open(value: &serde_json::Value, trace: &Trace) -> BackendResult<Self> {
        Self::validate(value, trace)?;
        let mut databases = BTreeMap::new();
        for (id, value, _) in options(value)? {
            databases.insert(
                id,
                open_with_shape(
                    &value,
                    &prometheus::Registry::new(),
                    shape(&trace.tables, id),
                )
                .await?,
            );
        }
        let tables = trace
            .tables
            .iter()
            .map(|t| {
                databases
                    .get(&t.db_id)
                    .map(|backend| {
                        Ok((
                            t.db_id,
                            backend
                                .db
                                .as_ref()
                                .ok_or("replay DB closed")?
                                .try_ks(&t.node_name)
                                .ok_or("replay table not declared")?,
                        ))
                    })
                    .transpose()
            })
            .collect::<BackendResult<_>>()?;
        Ok(Self { databases, tables })
    }
    pub fn info(&self) -> BackendInfo {
        let first = self
            .databases
            .first_key_value()
            .expect("validated node DBs")
            .1;
        let mut info = first.info();
        if let Some(ack) =
            first.effective_configuration["live_write"]["acknowledgement_contract"].as_str()
        {
            info.acknowledgement = ack.to_owned();
        }
        info.topology = serde_json::json!({"node_databases":self.databases.len(), "databases": self.databases.iter().map(|(id, b)| serde_json::json!({"db_id":id,"topology":b.info().topology})).collect::<Vec<_>>()});
        if self.databases.len() != 1 {
            info.configuration = serde_json::json!({"databases":self.databases.iter().map(|(id,b)| serde_json::json!({"db_id":id,"configuration":b.effective_configuration})).collect::<Vec<_>>(), "atomicity":"one complete batch per node DB; no cross-DB atomicity"});
            // Each node DB confirmed its tier from its own owner receipts; a common tier is
            // published only when every node DB confirmed the same one.
            let tier = &first.effective_configuration["durability_tier"];
            if self
                .databases
                .values()
                .all(|b| b.effective_configuration["durability_tier"] == *tier)
            {
                info.configuration["durability_tier"] = tier.clone();
            }
        }
        info
    }
}
impl ReplayBackend for TidehunterReplayBackend {
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let &(db, ks) = self
            .tables
            .get(usize::from(table))
            .and_then(Option::as_ref)
            .ok_or("unknown replay table")?;
        self.databases[&db]
            .db
            .as_ref()
            .ok_or("replay DB closed")?
            .get(ks, key)
            .map_err(native_error)
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        let database = self
            .databases
            .get(&db)
            .ok_or("unknown replay DB")?
            .db
            .as_ref()
            .ok_or("replay DB closed")?;
        for row in entries {
            let &(row_db, _) = self
                .tables
                .get(usize::from(row.table))
                .and_then(Option::as_ref)
                .ok_or("unknown replay table")?;
            if row_db != db {
                return Err("cross-DB replay batch".into());
            }
        }
        let mut batch = database.write_batch();
        for row in entries {
            batch.write(
                self.tables[usize::from(row.table)]
                    .ok_or("unknown replay table")?
                    .1,
                row.key.clone(),
                row.value.clone(),
            );
        }
        // sui::WriteBatch::commit -> commit_remote -> ConcurrentRemoteDatabase::write_batch_traced.
        // codec::CompleteBatch is one CRC-framed all-or-none recovery record, allocated
        // wholly to one owner. Publication follows the complete owner receipt. This is
        // crash-recovery batch atomicity, not a serializable multi-key read transaction.
        // Owner RPC acknowledgements use the confirmed durability tier; leased live
        // writes acknowledge their configured device completion before owner adoption.
        batch.commit().map_err(native_error)
    }
    fn physical_counts(&self) -> serde_json::Value {
        use crate::backend::KvBackend;
        let observations = self
            .databases
            .iter()
            .map(|(id, backend)| {
                let observation = backend.read_observation().unwrap_or_else(
                    |error| serde_json::json!({"available":false,"error":error.to_string()}),
                );
                (*id, observation)
            })
            .collect::<BTreeMap<_, _>>();
        if observations.len() == 1 {
            observations
                .into_values()
                .next()
                .unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::json!({"databases": observations})
        }
    }
    fn check_background_health(&self) -> BackendResult<()> {
        for (id, backend) in &self.databases {
            // A poisoned authority/publication/read worker fails even on cache hits.
            backend
                .db
                .as_ref()
                .ok_or("replay DB closed")?
                .validate_cached_read()
                .map_err(|e| format!("distributed replay node DB {id} health: {e:?}"))?;
            drop(
                backend
                    .cluster
                    .lock()
                    .map_err(|_| "distributed replay owner cluster lock poisoned")?,
            );
        }
        Ok(())
    }
}

mod resources;
#[cfg(test)]
mod tests;
