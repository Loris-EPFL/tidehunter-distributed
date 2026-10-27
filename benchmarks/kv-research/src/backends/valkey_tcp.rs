//! Valkey 9 TCP comparison adapter. Reads remain RAM-resident; AOF policy is admitted explicitly.

use std::{collections::BTreeMap, io, time::Duration};

use redis::{aio::MultiplexedConnection, cluster::ClusterClient};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tidehunter::minibytes::Bytes;
use tokio::time::timeout;

use crate::backend::{
    BackendInfo, BackendResult, BackendTransportError, BatchAtomicity, KvBackend, unsupported,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    nodes: Vec<String>,
    cluster: bool,
    required_replication_factor: usize,
    required_primary_nodes: usize,
    request_timeout_ms: u64,
}

impl Options {
    fn parse(value: &JsonValue) -> BackendResult<Self> {
        let options: Self = serde_json::from_value(value.clone())?;
        if options.nodes.is_empty() {
            return Err(invalid("nodes must contain at least one host:port"));
        }
        if options.request_timeout_ms == 0 {
            return Err(invalid("request_timeout_ms must be nonzero"));
        }
        if options.required_replication_factor != 1 {
            return Err(invalid(
                "Valkey comparison requires required_replication_factor=1",
            ));
        }
        if options.required_primary_nodes == 0
            || (options.cluster && options.required_primary_nodes != 5)
            || (!options.cluster && options.required_primary_nodes != 1)
            || (!options.cluster && options.nodes.len() != 1)
            || (options.cluster && options.nodes.len() != 5)
        {
            return Err(invalid(
                "standalone requires one node and required_primary_nodes=1; cluster requires exactly five listed primaries",
            ));
        }
        let mut unique = std::collections::HashSet::new();
        for node in &options.nodes {
            let (host, port) = node
                .rsplit_once(':')
                .ok_or_else(|| invalid("nodes entries must be host:port (TCP only)"))?;
            if host.is_empty()
                || !host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
                || port.parse::<u16>().is_err()
                || port.parse::<u16>().ok() == Some(0)
                || !unique.insert(node)
            {
                return Err(invalid(
                    "nodes entries must be unique, unauthenticated host:port TCP endpoints",
                ));
            }
        }
        Ok(options)
    }

    fn urls(&self) -> Vec<String> {
        self.nodes
            .iter()
            .map(|node| format!("redis://{node}/"))
            .collect()
    }
}

pub fn validate_options(value: &JsonValue) -> BackendResult<()> {
    Options::parse(value).map(|_| ())
}

#[derive(Clone)]
enum Connection {
    Standalone(MultiplexedConnection),
    Cluster(redis::cluster_async::ClusterConnection),
}

pub struct ValkeyTcpBackend {
    connection: Connection,
    timeout: Duration,
    info: BackendInfo,
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

async fn query<T: redis::FromRedisValue>(
    connection: &mut Connection,
    command: &mut redis::Cmd,
) -> redis::RedisResult<T> {
    match connection {
        Connection::Standalone(connection) => command.query_async(connection).await,
        Connection::Cluster(connection) => command.query_async(connection).await,
    }
}

async fn execute<T: redis::FromRedisValue>(
    connection: &Connection,
    request_timeout: Duration,
    command: &mut redis::Cmd,
) -> BackendResult<T> {
    let mut connection = connection.clone();
    timeout(request_timeout, query(&mut connection, command))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey request timed out"))?
        .map_err(redis_error)
}

async fn direct_info(
    node: &str,
    timeout_duration: Duration,
) -> BackendResult<(String, String, String, String, Vec<String>)> {
    let client = redis::Client::open(format!("redis://{node}/"))?;
    let mut connection = timeout(timeout_duration, client.get_multiplexed_async_connection())
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Valkey admission connection timed out",
            )
        })?
        .map_err(redis_error)?;
    let server: String = timeout(
        timeout_duration,
        redis::cmd("INFO")
            .arg("server")
            .query_async(&mut connection),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey INFO server timed out"))?
    .map_err(redis_error)?;
    let replication: String = timeout(
        timeout_duration,
        redis::cmd("INFO")
            .arg("replication")
            .query_async(&mut connection),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey INFO replication timed out"))?
    .map_err(redis_error)?;
    let persistence: String = timeout(
        timeout_duration,
        redis::cmd("INFO")
            .arg("persistence")
            .query_async(&mut connection),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey INFO persistence timed out"))?
    .map_err(redis_error)?;
    let cluster: String = timeout(
        timeout_duration,
        redis::cmd("INFO")
            .arg("cluster")
            .query_async(&mut connection),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey INFO cluster timed out"))?
    .map_err(redis_error)?;
    let config: Vec<String> = timeout(
        timeout_duration,
        redis::cmd("CONFIG")
            .arg("GET")
            .arg("appendonly")
            .arg("appendfsync")
            .arg("no-appendfsync-on-rewrite")
            .arg("maxmemory-policy")
            .query_async(&mut connection),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey CONFIG GET timed out"))?
    .map_err(redis_error)?;
    Ok((server, replication, persistence, cluster, config))
}

fn fields(info: &str) -> BTreeMap<&str, &str> {
    info.lines()
        .filter_map(|line| {
            let line = line.trim();
            let (key, value) = line.split_once(':')?;
            Some((key, value))
        })
        .collect()
}

fn parse_config(values: &[String]) -> BackendResult<BTreeMap<String, String>> {
    if !values.len().is_multiple_of(2) {
        return Err(invalid(
            "Valkey CONFIG GET returned an incomplete key/value list",
        ));
    }
    let mut config = BTreeMap::new();
    for pair in values.chunks_exact(2) {
        config.insert(pair[0].clone(), pair[1].clone());
    }
    for (key, expected) in [
        ("appendonly", "yes"),
        ("appendfsync", "always"),
        ("no-appendfsync-on-rewrite", "no"),
        ("maxmemory-policy", "noeviction"),
    ] {
        if config.get(key).map(String::as_str) != Some(expected) {
            return Err(invalid(&format!(
                "Valkey requires CONFIG {key}={expected}; observed {:?}",
                config.get(key)
            )));
        }
    }
    Ok(config)
}

fn parse_topology(nodes: &str, expected_primaries: usize) -> BackendResult<JsonValue> {
    let mut primaries = Vec::new();
    let mut slots = vec![false; 16_384];
    for line in nodes.lines() {
        let columns: Vec<_> = line.split_whitespace().collect();
        if columns.len() < 8 || columns[3] != "-" || columns[7] != "connected" {
            return Err(invalid("CLUSTER NODES requires connected primary records"));
        }
        let flags: Vec<_> = columns[2].split(',').collect();
        if flags
            .iter()
            .any(|flag| matches!(*flag, "fail" | "fail?" | "handshake" | "noaddr"))
        {
            return Err(invalid("cluster contains a failed or unready node"));
        }
        if flags.contains(&"slave") {
            return Err(invalid("Valkey Cluster must have zero replica nodes"));
        }
        if !flags.contains(&"master") {
            return Err(invalid(
                "CLUSTER NODES returned a node without a primary role",
            ));
        }
        let mut node_slots = 0usize;
        for &slot in columns.iter().skip(8) {
            if slot.starts_with('[') {
                return Err(invalid("Valkey cluster slot migration is not admitted"));
            }
            let (start, end) = slot.split_once('-').map_or((slot, slot), |(a, b)| (a, b));
            let start: usize = start
                .parse()
                .map_err(|_| invalid("invalid CLUSTER NODES slot"))?;
            let end: usize = end
                .parse()
                .map_err(|_| invalid("invalid CLUSTER NODES slot range"))?;
            if start > end || end >= slots.len() {
                return Err(invalid("CLUSTER NODES slot outside 0..16383"));
            }
            for covered in &mut slots[start..=end] {
                if *covered {
                    return Err(invalid("duplicate CLUSTER NODES slot ownership"));
                }
                *covered = true;
                node_slots += 1;
            }
        }
        primaries.push(json!({"node_id": columns[0], "address": columns[1], "slots": node_slots}));
    }
    if primaries.len() != expected_primaries || slots.iter().any(|slot| !slot) {
        return Err(invalid(&format!(
            "cluster requires {expected_primaries} healthy primaries owning all 16384 slots; observed {} primaries and {} uncovered slots",
            primaries.len(),
            slots.iter().filter(|slot| !**slot).count()
        )));
    }
    Ok(json!({"primary_count": primaries.len(), "slot_count": 16384, "primaries": primaries}))
}

impl ValkeyTcpBackend {
    pub async fn open(
        options: &JsonValue,
        _registry: &prometheus::Registry,
    ) -> BackendResult<Self> {
        let options = Options::parse(options)?;
        let request_timeout = Duration::from_millis(options.request_timeout_ms);
        let mut observed = Vec::with_capacity(options.nodes.len());
        for node in &options.nodes {
            observed.push(direct_info(node, request_timeout).await?);
        }
        let version = fields(&observed[0].0)
            .get("valkey_version")
            .copied()
            .ok_or_else(|| invalid("server did not identify valkey_version"))?
            .to_owned();
        if version != "9.0.7" {
            return Err(invalid(&format!(
                "unsupported Valkey version {version}; require 9.0.7"
            )));
        }
        let mut configs = Vec::with_capacity(observed.len());
        let mut persistence = Vec::with_capacity(observed.len());
        let mut cluster_modes = Vec::with_capacity(observed.len());
        for (server, replication, persistence_info, cluster_info, config) in &observed {
            let actual_version = fields(server)
                .get("valkey_version")
                .copied()
                .ok_or_else(|| invalid("Valkey version missing on node"))?;
            if actual_version != version {
                return Err(invalid("Valkey nodes report inconsistent server versions"));
            }
            let replication = fields(replication);
            if replication.get("role").copied() != Some("master")
                || replication.get("connected_slaves").copied() != Some("0")
            {
                return Err(invalid(
                    "every admitted Valkey node must be a primary with zero replicas",
                ));
            }
            let cluster_enabled = fields(cluster_info)
                .get("cluster_enabled")
                .copied()
                .ok_or_else(|| invalid("Valkey INFO cluster lacks cluster_enabled"))?;
            let expected_cluster_enabled = if options.cluster { "1" } else { "0" };
            if cluster_enabled != expected_cluster_enabled {
                return Err(invalid(
                    "Valkey cluster_enabled does not match the requested standalone/cluster layout",
                ));
            }
            cluster_modes.push(cluster_enabled.to_owned());
            configs.push(parse_config(config)?);
            persistence.push(
                fields(persistence_info)
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect::<BTreeMap<_, _>>(),
            );
        }
        let topology = if options.cluster {
            let client = redis::Client::open(format!("redis://{}/", options.nodes[0]))?;
            let mut conn = timeout(request_timeout, client.get_multiplexed_async_connection())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "cluster topology connection timed out",
                    )
                })?
                .map_err(redis_error)?;
            let cluster_info: String = timeout(
                request_timeout,
                redis::cmd("CLUSTER").arg("INFO").query_async(&mut conn),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "CLUSTER INFO timed out"))?
            .map_err(redis_error)?;
            let state = fields(&cluster_info);
            if state.get("cluster_state").copied() != Some("ok")
                || state.get("cluster_slots_assigned").copied() != Some("16384")
            {
                return Err(invalid(
                    "Valkey Cluster must report cluster_state=ok and all 16384 slots assigned",
                ));
            }
            let nodes: String = timeout(
                request_timeout,
                redis::cmd("CLUSTER").arg("NODES").query_async(&mut conn),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "CLUSTER NODES timed out"))?
            .map_err(redis_error)?;
            let mut topology = parse_topology(&nodes, options.required_primary_nodes)?;
            topology["cluster_state"] = json!("ok");
            topology["cluster_slots_assigned"] = json!(16384);
            topology
        } else {
            json!({"primary_count": 1, "slot_count": null, "primaries": [{"address": options.nodes[0]}]})
        };
        let connection = if options.cluster {
            let builder = ClusterClient::builder(options.urls())
                .retries(2)
                .min_retry_wait(1)
                .max_retry_wait(25)
                .connection_timeout(request_timeout)
                .response_timeout(request_timeout);
            let client = builder.build().map_err(redis_error)?;
            let connection = timeout(request_timeout, client.get_async_connection())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Valkey cluster connection timed out",
                    )
                })?
                .map_err(redis_error)?;
            Connection::Cluster(connection)
        } else {
            let client = redis::Client::open(format!("redis://{}/", options.nodes[0]))?;
            let connection = timeout(request_timeout, client.get_multiplexed_async_connection())
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "Valkey connection timed out")
                })?
                .map_err(redis_error)?;
            Connection::Standalone(connection)
        };
        let info = BackendInfo {
            name: "valkey_tcp".to_owned(),
            server_version: version,
            client_version: "redis-rs 1.8.0 (BSD-3-Clause)".to_owned(),
            acknowledgement: "SET success reply; primary appendonly=yes, appendfsync=always, no-appendfsync-on-rewrite=no observed at admission; Valkey AOF fsyncs before replying; no replica acknowledgement".to_owned(),
            consistency: "primary-routed reads/writes; RF1, no replica reads".to_owned(),
            replication_factor: 1,
            measurement_boundary: "redis-rs async command through fully materialized response; Valkey GET reads RAM-resident values (not NVMe cold reads)".to_owned(),
            get_many_strategy: if options.cluster {
                "one MGET through redis-rs; client splits by slot and reassembles input order, duplicates and nil misses"
            } else {
                "one native standalone MGET, preserving input order, duplicates and nil misses"
            }.to_owned(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({"cluster": options.cluster, "required_primary_nodes": options.required_primary_nodes, "observed_primary_nodes": topology["primary_count"], "slot_count": topology["slot_count"], "cluster_state": topology.get("cluster_state"), "primaries": topology["primaries"], "routing": if options.cluster { "redis-rs CLUSTER SLOTS slot map; MOVED/ASK redirect handling" } else { "direct standalone TCP connection" }}),
            configuration: json!({
                "transport": "plain TCP",
                "request_timeout_ms": options.request_timeout_ms,
                "cluster_enabled": cluster_modes,
                "required_replication_factor": options.required_replication_factor,
                "observed_replication_factor": 1,
                "node_config": configs,
                "persistence": persistence,
                "persistence_query": "INFO persistence sampled once per configured node at admission; no per-operation probes",
                "eviction": "noeviction",
                "authentication": "disabled",
                "tls": "disabled",
                "replica_reads": false,
                "client_retries": if options.cluster { 2 } else { 0 },
                "mutation_uncertainty": "Cluster MOVED/ASK routing is distinct from its bounded two-retry transport policy; standalone commands are not retried. SET/DEL may be replayed in cluster mode. Final transport failures and actual timeouts are unknown_mutation; timeout/cancellation is not rollback"
            }),
        };
        Ok(Self {
            connection,
            timeout: request_timeout,
            info,
        })
    }
}

impl KvBackend for ValkeyTcpBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let mut cmd = redis::cmd("GET");
        cmd.arg(key);
        let value: Option<Vec<u8>> = execute(&self.connection, self.timeout, &mut cmd).await?;
        Ok(value.map(Bytes::from))
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(key.as_ref()).arg(value.as_ref());
        let reply: String = execute(&self.connection, self.timeout, &mut cmd).await?;
        if reply != "OK" {
            return Err(invalid("Valkey SET did not return OK"));
        }
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        let mut cmd = redis::cmd("DEL");
        cmd.arg(key.as_ref());
        let _: i64 = execute(&self.connection, self.timeout, &mut cmd).await?;
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut cmd = redis::cmd("MGET");
        for key in keys {
            cmd.arg(key.as_ref());
        }
        let values: Vec<Option<Vec<u8>>> =
            execute(&self.connection, self.timeout, &mut cmd).await?;
        if values.len() != keys.len() {
            return Err(invalid(
                "Valkey MGET result count differs from input key count",
            ));
        }
        Ok(values
            .into_iter()
            .map(|value| value.map(Bytes::from))
            .collect())
    }

    async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        Err(unsupported("Valkey cross-slot atomic put_batch"))
    }
}
