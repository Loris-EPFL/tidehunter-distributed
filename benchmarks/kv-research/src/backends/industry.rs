//! Redis-cluster protocol clients for Garnet and Apache Kvrocks, with store-specific admission.

use crate::backend::{
    BackendInfo, BackendResult, BackendTransportError, BatchAtomicity, KvBackend, unsupported,
};
use redis::{aio::MultiplexedConnection, cluster::ClusterClient, cluster_async::ClusterConnection};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    io,
    time::Duration,
};
use tidehunter::minibytes::Bytes;
use tokio::time::timeout;

#[derive(Clone, Copy)]
pub enum Store {
    Garnet,
    Kvrocks,
}
impl Store {
    fn name(self) -> &'static str {
        match self {
            Self::Garnet => "garnet",
            Self::Kvrocks => "kvrocks",
        }
    }
    fn version(self) -> &'static str {
        match self {
            Self::Garnet => "2.2.5",
            Self::Kvrocks => "2.17.0",
        }
    }
    fn version_field(self) -> &'static str {
        match self {
            Self::Garnet => "garnet_version",
            Self::Kvrocks => "kvrocks_version",
        }
    }
    fn config_keys(self) -> &'static [&'static str] {
        match self {
            Self::Garnet => &[
                "appendonly",
                "aof-commit-freq",
                "aof-commit-wait",
                "aof-null-device",
                "logdir",
                "aof-size-limit",
                "checkpoint-freq",
                "compaction-type",
            ],
            Self::Kvrocks => &[
                "rocksdb.write_options.sync",
                "rocksdb.write_options.disable_wal",
                "rocksdb.block_cache_size",
                "rocksdb.subkey_block_cache_size",
                "rocksdb.metadata_block_cache_size",
                "rocksdb.share_metadata_and_subkey_block_cache",
                "rocksdb.disable_auto_compactions",
                "cluster-enabled",
            ],
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    nodes: Vec<String>,
    cluster: bool,
    required_replication_factor: usize,
    required_primary_nodes: usize,
    request_timeout_ms: u64,
}
impl Options {
    fn parse(value: &Value) -> BackendResult<Self> {
        let o: Self = serde_json::from_value(value.clone())?;
        if !o.cluster
            || o.nodes.len() != 5
            || o.required_primary_nodes != 5
            || o.required_replication_factor != 1
            || o.request_timeout_ms == 0
        {
            return Err(invalid(
                "requires cluster=true, five primary endpoints, RF1, and a nonzero timeout",
            ));
        }
        let mut unique = HashSet::new();
        for node in &o.nodes {
            let (host, port) = node
                .rsplit_once(':')
                .ok_or_else(|| invalid("nodes must be host:port"))?;
            if host.is_empty()
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
                || !matches!(port.parse::<u16>(), Ok(1..=65535))
                || !unique.insert(node)
            {
                return Err(invalid(
                    "nodes must be unique unauthenticated TCP host:port endpoints",
                ));
            }
        }
        Ok(o)
    }
}
pub fn validate_options(value: &Value) -> BackendResult<()> {
    Options::parse(value).map(|_| ())
}
fn invalid(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_owned()).into()
}
fn redis_error(error: redis::RedisError) -> Box<dyn std::error::Error + Send + Sync> {
    if error.is_timeout() {
        io::Error::new(io::ErrorKind::TimedOut, error).into()
    } else if error.is_io_error() {
        Box::new(BackendTransportError::new(Box::new(error)))
    } else {
        Box::new(error)
    }
}
async fn direct<T: redis::FromRedisValue>(
    conn: &mut MultiplexedConnection,
    duration: Duration,
    cmd: &mut redis::Cmd,
) -> BackendResult<T> {
    timeout(duration, cmd.query_async(conn))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "admission request timed out"))?
        .map_err(redis_error)
}
async fn execute<T: redis::FromRedisValue>(
    conn: &ClusterConnection,
    duration: Duration,
    cmd: &mut redis::Cmd,
) -> BackendResult<T> {
    let mut conn = conn.clone();
    timeout(duration, cmd.query_async(&mut conn))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cluster request timed out"))?
        .map_err(redis_error)
}
fn fields(info: &str) -> BTreeMap<&str, &str> {
    info.lines()
        .filter_map(|line| line.trim().split_once(':'))
        .collect()
}
fn check_config(store: Store, config: &BTreeMap<String, String>) -> BackendResult<()> {
    let required: &[(&str, &str)] = match store {
        Store::Garnet => &[
            ("appendonly", "yes"),
            ("aof-commit-freq", "0"),
            ("aof-commit-wait", "yes"),
            ("aof-null-device", "no"),
        ],
        Store::Kvrocks => &[
            ("rocksdb.write_options.sync", "yes"),
            ("rocksdb.write_options.disable_wal", "no"),
            ("rocksdb.disable_auto_compactions", "no"),
            ("cluster-enabled", "yes"),
        ],
    };
    for &(key, expected) in required {
        if config.get(key).map(String::as_str) != Some(expected) {
            return Err(invalid(&format!(
                "{} requires CONFIG {key}={expected}; observed {:?}",
                store.name(),
                config.get(key)
            )));
        }
    }
    Ok(())
}

type ClusterView = BTreeMap<String, (String, Vec<(usize, usize)>)>;

// Compare complete per-node views, including each slot's owner, rather than just counts.
fn topology(nodes: &str) -> BackendResult<(ClusterView, String)> {
    let mut result = BTreeMap::new();
    let mut covered = vec![false; 16384];
    let mut myself = None;
    for line in nodes.lines() {
        let columns: Vec<_> = line.split_whitespace().collect();
        if columns.len() < 8 || columns[3] != "-" || columns[7] != "connected" {
            return Err(invalid("cluster node is not a connected primary"));
        }
        let flags: Vec<_> = columns[2].split(',').collect();
        if !flags.contains(&"master")
            || flags
                .iter()
                .any(|flag| matches!(*flag, "slave" | "fail" | "fail?" | "handshake" | "noaddr"))
        {
            return Err(invalid("cluster contains a replica or unhealthy node"));
        }
        if flags.contains(&"myself") && myself.replace(columns[0].to_owned()).is_some() {
            return Err(invalid("multiple local node identities"));
        }
        let mut ranges = Vec::new();
        for slot in &columns[8..] {
            let (a, b) = slot.split_once('-').unwrap_or((slot, slot));
            let start: usize = a.parse().map_err(|_| invalid("invalid slot range"))?;
            let end: usize = b.parse().map_err(|_| invalid("invalid slot range"))?;
            if start > end || end >= covered.len() {
                return Err(invalid("slot range out of bounds"));
            }
            for seen in &mut covered[start..=end] {
                if *seen {
                    return Err(invalid("duplicate slot ownership"));
                }
                *seen = true;
            }
            ranges.push((start, end));
        }
        if ranges.is_empty() {
            return Err(invalid("primary owns no slots"));
        }
        ranges.sort_unstable();
        if result
            .insert(
                columns[0].to_owned(),
                (
                    columns[1]
                        .split('@')
                        .next()
                        .unwrap_or(columns[1])
                        .to_owned(),
                    ranges,
                ),
            )
            .is_some()
        {
            return Err(invalid("duplicate cluster node id"));
        }
    }
    if result.len() != 5 || covered.iter().any(|v| !v) {
        return Err(invalid(
            "cluster requires five primaries covering all 16384 slots",
        ));
    }
    Ok((
        result,
        myself.ok_or_else(|| invalid("cluster view lacks local node identity"))?,
    ))
}

pub struct IndustryBackend {
    connection: ClusterConnection,
    timeout: Duration,
    info: BackendInfo,
}
impl IndustryBackend {
    pub async fn open(value: &Value, store: Store) -> BackendResult<Self> {
        let options = Options::parse(value)?;
        let duration = Duration::from_millis(options.request_timeout_ms);
        let mut node_info = Vec::new();
        let mut node_config = Vec::new();
        let mut views = Vec::new();
        let mut local_ids = HashSet::new();
        for node in &options.nodes {
            let client = redis::Client::open(format!("redis://{node}/"))?;
            let mut conn = timeout(duration, client.get_multiplexed_async_connection())
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "admission connection timed out")
                })?
                .map_err(redis_error)?;
            let server: String =
                direct(&mut conn, duration, redis::cmd("INFO").arg("server")).await?;
            let server_fields = fields(&server);
            if server_fields.get(store.version_field()).copied() != Some(store.version()) {
                return Err(invalid(&format!(
                    "{} requires {}={}",
                    store.name(),
                    store.version_field(),
                    store.version()
                )));
            }
            let replication: String =
                direct(&mut conn, duration, redis::cmd("INFO").arg("replication")).await?;
            let memory: String =
                direct(&mut conn, duration, redis::cmd("INFO").arg("memory")).await?;
            let repl = fields(&replication);
            if repl.get("role").copied() != Some("master")
                || repl.get("connected_slaves").copied() != Some("0")
            {
                return Err(invalid(
                    "every endpoint must be a primary with zero replicas",
                ));
            }
            let cluster: String =
                direct(&mut conn, duration, redis::cmd("INFO").arg("cluster")).await?;
            if fields(&cluster).get("cluster_enabled").copied() != Some("1") {
                return Err(invalid("cluster mode is not enabled"));
            }
            let persistence: String =
                direct(&mut conn, duration, redis::cmd("INFO").arg("persistence")).await?;
            let storage: String = if matches!(store, Store::Garnet) {
                direct(&mut conn, duration, redis::cmd("INFO").arg("store")).await?
            } else {
                String::new()
            };
            let mut config = BTreeMap::new();
            for key in store.config_keys() {
                let values: Vec<String> = direct(
                    &mut conn,
                    duration,
                    redis::cmd("CONFIG").arg("GET").arg(key),
                )
                .await?;
                if values.len() == 2 && values[0] == *key {
                    config.insert(values[0].clone(), values[1].clone());
                } else if !values.is_empty() {
                    return Err(invalid("CONFIG GET returned an invalid response"));
                }
            }
            check_config(store, &config)?;
            let cluster_info: String =
                direct(&mut conn, duration, redis::cmd("CLUSTER").arg("INFO")).await?;
            let state = fields(&cluster_info);
            if state.get("cluster_state").copied() != Some("ok")
                || state.get("cluster_slots_assigned").copied() != Some("16384")
            {
                return Err(invalid("cluster state is not ok or slots are incomplete"));
            }
            let nodes: String =
                direct(&mut conn, duration, redis::cmd("CLUSTER").arg("NODES")).await?;
            let (view, self_id) = topology(&nodes)?;
            if !local_ids.insert(self_id) {
                return Err(invalid(
                    "listed endpoints do not identify five distinct primaries",
                ));
            }
            if views.first().is_some_and(|previous| previous != &view) {
                return Err(invalid("primary cluster views disagree"));
            }
            views.push(view);
            node_info.push(json!({"node":node, "server":server, "replication":replication, "cluster":cluster, "cluster_info":cluster_info, "persistence":persistence, "store":storage, "memory":memory}));
            node_config.push(config);
        }
        let urls = options
            .nodes
            .iter()
            .map(|node| format!("redis://{node}/"))
            .collect::<Vec<_>>();
        let client = ClusterClient::builder(urls)
            .retries(2)
            .min_retry_wait(1)
            .max_retry_wait(25)
            .connection_timeout(duration)
            .response_timeout(duration)
            .build()
            .map_err(redis_error)?;
        let connection = timeout(duration, client.get_async_connection())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cluster connection timed out"))?
            .map_err(redis_error)?;
        let primaries: Vec<_> = views[0].iter().map(|(id, (address, ranges))| json!({"node_id":id,"address":address,"slots":ranges.iter().map(|(a,b)| b-a+1).sum::<usize>(),"slot_ranges":ranges})).collect();
        let acknowledgement = match store {
            Store::Garnet => {
                "SET/DEL reply after AOF commit; CONFIG appendonly=yes, aof-commit-freq=0, aof-commit-wait=yes, aof-null-device=no observed on every primary"
            }
            Store::Kvrocks => {
                "SET/DEL reply with RocksDB WAL enabled and write_options.sync=yes observed on every primary"
            }
        };
        let tier = match store {
            Store::Garnet => {
                "not exposed by CONFIG/INFO; requires separate lifecycle live container image, argv and mount receipt"
            }
            Store::Kvrocks => "RocksDB on disk; data mount requires separate lifecycle receipt",
        };
        let info = BackendInfo {
            name: store.name().to_owned(),
            server_version: store.version().to_owned(),
            client_version: "redis-rs 1.8.0 (BSD-3-Clause)".into(),
            acknowledgement: acknowledgement.into(),
            consistency: "primary-routed reads/writes; RF1, no replica reads".into(),
            replication_factor: 1,
            measurement_boundary: "async TCP commands through fully materialized responses; OS cache eviction does not guarantee cold-device reads".into(),
            get_many_strategy: "native MGET through redis-rs cross-slot splitter and ordered response combiner; preserves duplicates and nil misses; not atomic".into(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({"cluster":true,"primary_count":5,"slot_count":16384,"primaries":primaries,"routing":"redis-rs cluster slot routing and MOVED/ASK"}),
            configuration: json!({"transport":"plain TCP", "request_timeout_ms":options.request_timeout_ms,"required_replication_factor":1,"node_config":node_config,"node_info":node_info,"storage_tier_evidence":tier,"replica_reads":false,"client_retries":2,"mutation_uncertainty":"transport failures or timeouts can leave mutation outcome unknown; retries may replay SET/DEL"}),
        };
        Ok(Self {
            connection,
            timeout: duration,
            info,
        })
    }
}
impl KvBackend for IndustryBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }
    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let value: Option<Vec<u8>> =
            execute(&self.connection, self.timeout, redis::cmd("GET").arg(key)).await?;
        Ok(value.map(Bytes::from))
    }
    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let result: String = execute(
            &self.connection,
            self.timeout,
            redis::cmd("SET").arg(key.as_ref()).arg(value.as_ref()),
        )
        .await?;
        if result != "OK" {
            return Err(invalid("SET did not return OK"));
        }
        Ok(())
    }
    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        let _: i64 = execute(
            &self.connection,
            self.timeout,
            redis::cmd("DEL").arg(key.as_ref()),
        )
        .await?;
        Ok(())
    }
    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut command = redis::cmd("MGET");
        for key in keys {
            command.arg(key.as_ref());
        }
        let values: Vec<Option<Vec<u8>>> =
            execute(&self.connection, self.timeout, &mut command).await?;
        if values.len() != keys.len() {
            return Err(invalid("MGET result count differs from input key count"));
        }
        Ok(values
            .into_iter()
            .map(|value| value.map(Bytes::from))
            .collect())
    }
    async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        Err(unsupported("cross-slot atomic put_batch"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durability_requires_actual_settings() {
        let mut c = BTreeMap::from([
            ("rocksdb.write_options.sync".into(), "yes".into()),
            ("rocksdb.write_options.disable_wal".into(), "no".into()),
            ("rocksdb.disable_auto_compactions".into(), "no".into()),
            ("cluster-enabled".into(), "yes".into()),
        ]);
        assert!(check_config(Store::Kvrocks, &c).is_ok());
        c.insert("rocksdb.write_options.disable_wal".into(), "yes".into());
        assert!(check_config(Store::Kvrocks, &c).is_err());
        let mut c = BTreeMap::from([
            ("appendonly".into(), "yes".into()),
            ("aof-commit-freq".into(), "0".into()),
            ("aof-commit-wait".into(), "yes".into()),
            ("aof-null-device".into(), "no".into()),
        ]);
        assert!(check_config(Store::Garnet, &c).is_ok());
        c.insert("aof-commit-wait".into(), "no".into());
        assert!(check_config(Store::Garnet, &c).is_err());
    }
    #[test]
    fn topology_rejects_holes_overlap_and_replicas() {
        let good = "a a:1@2 myself,master - 0 0 0 connected 0-3276\nb b:1@2 master - 0 0 0 connected 3277-6553\nc c:1@2 master - 0 0 0 connected 6554-9830\nd d:1@2 master - 0 0 0 connected 9831-13107\ne e:1@2 master - 0 0 0 connected 13108-16383\n";
        assert!(topology(good).is_ok());
        assert!(topology(&good.replace("13108-16383", "13109-16383")).is_err());
        assert!(topology(&good.replace("13108-16383", "13107-16383")).is_err());
        assert!(
            topology(&good.replace(
                "master - 0 0 0 connected 3277",
                "slave - 0 0 0 connected 3277"
            ))
            .is_err()
        );
    }
}
