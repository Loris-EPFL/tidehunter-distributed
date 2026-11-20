//! Pinned upstream libvalkey RDMA, with a deliberately static Cluster slot map.
//! The upstream cluster client hardcodes TCP; never use it for this transport.

use crate::backend::{
    AckContract, BackendInfo, BackendResult, BackendTransportError, BatchAtomicity, KvBackend,
    unsupported,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::{CStr, CString, c_char, c_int, c_void},
    fmt, io,
    net::SocketAddr,
    ptr::NonNull,
    sync::{
        Arc, LazyLock, Mutex as StdMutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tidehunter::minibytes::Bytes;
use tokio::{sync::Mutex, time::timeout};

mod connection;
mod diagnostics;

#[cfg(test)]
mod tests;

use connection::*;
use diagnostics::*;

pub mod replay;

const POOL_SIZE: usize = 4;
const COMMIT: &str = "fe8d406c0b5a64ec897f6455e4de684629883764";
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    nodes: Vec<String>,
    cluster: bool,
    required_replication_factor: usize,
    required_primary_nodes: usize,
    request_timeout_ms: u64,
    client: Option<String>,
    #[serde(default)]
    ack_contract: AckContract,
}
fn invalid(s: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, s.into()).into()
}
impl Options {
    fn parse(v: &Value) -> BackendResult<Self> {
        let o: Self = serde_json::from_value(v.clone())?;
        o.ack_contract.validate_for("valkey_rdma")?;
        let n = if o.cluster { 5 } else { 1 };
        if o.required_replication_factor != 1
            || o.required_primary_nodes != n
            || o.nodes.len() != n
            || o.request_timeout_ms == 0
            || o.request_timeout_ms > 60_000
        {
            return Err(invalid(
                "Valkey RDMA requires RF1, one standalone or five cluster primaries, and timeout 1..=60000 ms",
            ));
        }
        let mut unique = HashSet::new();
        for node in &o.nodes {
            let addr: SocketAddr = node.parse().map_err(|_| {
                invalid("RDMA nodes must be numeric IP:port endpoints (IPv6 in brackets)")
            })?;
            if addr.port() == 0 || !unique.insert(addr) {
                return Err(invalid(
                    "RDMA endpoints must be unique and use nonzero ports",
                ));
            }
        }
        Ok(o)
    }
}
pub fn validate_options(v: &Value) -> BackendResult<()> {
    let o = Options::parse(v)?;
    if o.client.is_some() {
        return Err(invalid("client option is not valid for valkey_rdma"));
    }
    Ok(())
}
pub fn validate_tcp_options(v: &Value) -> BackendResult<()> {
    let o = Options::parse(v)?;
    if o.client.as_deref() != Some("libvalkey") {
        return Err(invalid("paired TCP requires client=libvalkey"));
    }
    Ok(())
}

fn command(c: &mut Connection, args: &[&str]) -> BackendResult<Response> {
    let deadline = Instant::now() + Duration::from_millis(c.timeout_ms);
    c.command(
        &args
            .iter()
            .map(|x| x.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        deadline,
    )
}
fn text(r: Response) -> BackendResult<String> {
    match r {
        Response::Bytes(b) => Ok(String::from_utf8(b)?),
        _ => Err(invalid("expected text reply")),
    }
}
fn fields(s: &str) -> BTreeMap<&str, &str> {
    s.lines().filter_map(|l| l.trim().split_once(':')).collect()
}
fn config(r: Response, ack_contract: AckContract) -> BackendResult<BTreeMap<String, String>> {
    let Response::Array(a) = r else {
        return Err(invalid("expected CONFIG GET array"));
    };
    if a.len() % 2 != 0 {
        return Err(invalid("incomplete CONFIG GET response"));
    }
    let mut map = BTreeMap::new();
    let mut it = a.into_iter();
    while let Some(k) = it.next() {
        map.insert(
            text(k)?,
            text(it.next().ok_or_else(|| invalid("incomplete config"))?)?,
        );
    }
    for (k, v) in [
        ("appendonly", "yes"),
        ("appendfsync", ack_contract.valkey_appendfsync()?),
        ("no-appendfsync-on-rewrite", "no"),
        ("maxmemory-policy", "noeviction"),
    ] {
        if map.get(k).map(String::as_str) != Some(v) {
            return Err(invalid(format!(
                "ack_contract {} requires CONFIG {k}={v}; observed {:?}",
                ack_contract.as_str(),
                map.get(k)
            )));
        }
    }
    Ok(map)
}
// Cluster hash tags and CRC16/XMODEM per the upstream cluster specification.
fn slot(key: &[u8]) -> usize {
    let data = if let Some(a) = key.iter().position(|b| *b == b'{') {
        if let Some(b) = key[a + 1..].iter().position(|b| *b == b'}') {
            if b > 0 { &key[a + 1..a + 1 + b] } else { key }
        } else {
            key
        }
    } else {
        key
    };
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    (crc as usize) & 16383
}
fn topology(s: &str, ids: &HashMap<String, usize>) -> BackendResult<Vec<usize>> {
    let mut slots = vec![usize::MAX; 16384];
    let mut seen = HashSet::new();
    for line in s.lines() {
        let c: Vec<_> = line.split_whitespace().collect();
        if c.len() < 8 || c[3] != "-" || c[7] != "connected" {
            return Err(invalid("unhealthy cluster primary"));
        }
        let flags: Vec<_> = c[2].split(',').collect();
        if !flags.contains(&"master")
            || flags
                .iter()
                .any(|f| matches!(*f, "slave" | "fail" | "fail?" | "handshake" | "noaddr"))
        {
            return Err(invalid("cluster must contain only healthy primaries"));
        }
        let owner = *ids
            .get(c[0])
            .ok_or_else(|| invalid("cluster node ID missing from RDMA endpoints"))?;
        if !seen.insert(owner) {
            return Err(invalid("duplicate cluster node ID"));
        }
        let mut count = 0;
        for range in &c[8..] {
            if range.starts_with('[') {
                return Err(invalid("slot migration is unsupported"));
            }
            let (a, b) = range.split_once('-').unwrap_or((range, range));
            let a: usize = a.parse()?;
            let b: usize = b.parse()?;
            if a > b || b >= slots.len() {
                return Err(invalid("invalid cluster slot range"));
            }
            for s in &mut slots[a..=b] {
                if *s != usize::MAX {
                    return Err(invalid("duplicate slot ownership"));
                }
                *s = owner;
                count += 1;
            }
        }
        if count == 0 {
            return Err(invalid("every primary must own slots"));
        }
    }
    if seen.len() != ids.len() || slots.contains(&usize::MAX) {
        return Err(invalid(
            "cluster must cover all slots with exactly five RDMA-connected primaries",
        ));
    }
    Ok(slots)
}
fn slot_response(response: Response, ids: &HashMap<String, usize>) -> BackendResult<Vec<usize>> {
    let Response::Array(ranges) = response else {
        return Err(invalid("CLUSTER SLOTS must be an array"));
    };
    let mut slots = vec![usize::MAX; 16384];
    for range in ranges {
        let Response::Array(mut fields) = range else {
            return Err(invalid("invalid CLUSTER SLOTS range"));
        };
        if fields.len() != 3 {
            return Err(invalid(
                "CLUSTER SLOTS must list exactly one primary per range",
            ));
        }
        let primary = fields
            .pop()
            .ok_or_else(|| invalid("missing slot primary"))?;
        let end = fields.pop().ok_or_else(|| invalid("missing slot end"))?;
        let start = fields.pop().ok_or_else(|| invalid("missing slot start"))?;
        let (Response::Integer(start), Response::Integer(end), Response::Array(primary)) =
            (start, end, primary)
        else {
            return Err(invalid("invalid CLUSTER SLOTS fields"));
        };
        if start < 0 || end < start || end >= 16384 || primary.len() < 3 {
            return Err(invalid("invalid CLUSTER SLOTS owner or range"));
        }
        let Response::Bytes(id) = &primary[2] else {
            return Err(invalid("CLUSTER SLOTS primary lacks node ID"));
        };
        let id = std::str::from_utf8(id)?;
        let owner = *ids
            .get(id)
            .ok_or_else(|| invalid("CLUSTER SLOTS identifies an unknown primary"))?;
        for slot in &mut slots[start as usize..=end as usize] {
            if *slot != usize::MAX {
                return Err(invalid("overlapping CLUSTER SLOTS ranges"));
            }
            *slot = owner;
        }
    }
    if slots.contains(&usize::MAX) {
        return Err(invalid("CLUSTER SLOTS does not cover all slots"));
    }
    Ok(slots)
}
struct Node {
    connections: Vec<Arc<Mutex<Connection>>>,
    next: AtomicUsize,
    ctx: Arc<NodeCtx>,
}
pub struct ValkeyRdmaBackend {
    nodes: Vec<Node>,
    slots: Vec<usize>,
    cluster: bool,
    timeout: Duration,
    rdma: bool,
    profile: ClientProfile,
    info: BackendInfo,
}
impl ValkeyRdmaBackend {
    pub async fn open(value: &Value, _: &prometheus::Registry) -> BackendResult<Self> {
        validate_options(value)?;
        let o = Options::parse(value)?;
        tokio::task::spawn_blocking(move || Self::open_blocking(o, true))
            .await
            .map_err(|e| invalid(format!("libvalkey admission task failed: {e}")))?
    }
    pub async fn open_tcp(value: &Value, _: &prometheus::Registry) -> BackendResult<Self> {
        validate_tcp_options(value)?;
        let o = Options::parse(value)?;
        tokio::task::spawn_blocking(move || Self::open_blocking(o, false))
            .await
            .map_err(|e| invalid(format!("libvalkey admission task failed: {e}")))?
    }
    fn open_blocking(o: Options, rdma: bool) -> BackendResult<Self> {
        let mut initial = Vec::new();
        let mut configs = Vec::new();
        let mut persistence = Vec::new();
        let mut server_builds = Vec::new();
        let mut ids = HashMap::new();
        let mut node_ids = Vec::new();
        let mut maps = Vec::new();
        let mut slot_views = Vec::new();
        for (i, node) in o.nodes.iter().enumerate() {
            let mut c = Connection::connect(node, o.request_timeout_ms, rdma)?;
            let server = text(command(&mut c, &["INFO", "server"])?)?;
            if fields(&server).get("valkey_version") != Some(&"9.0.7") {
                return Err(invalid("RDMA server must report Valkey 9.0.7"));
            }
            server_builds.push(
                fields(&server)
                    .get("valkey_build_id")
                    .copied()
                    .unwrap_or("unreported")
                    .to_owned(),
            );
            let repl = text(command(&mut c, &["INFO", "replication"])?)?;
            let r = fields(&repl);
            if r.get("role") != Some(&"master") || r.get("connected_slaves") != Some(&"0") {
                return Err(invalid("RDMA node must be RF1 primary"));
            }
            let cluster = text(command(&mut c, &["INFO", "cluster"])?)?;
            if fields(&cluster).get("cluster_enabled") != Some(&(if o.cluster { "1" } else { "0" }))
            {
                return Err(invalid("cluster mode mismatch"));
            }
            configs.push(config(
                command(
                    &mut c,
                    &[
                        "CONFIG",
                        "GET",
                        "appendonly",
                        "appendfsync",
                        "no-appendfsync-on-rewrite",
                        "maxmemory-policy",
                    ],
                )?,
                o.ack_contract,
            )?);
            let p = text(command(&mut c, &["INFO", "persistence"])?)?;
            persistence.push(
                fields(&p)
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect::<BTreeMap<_, _>>(),
            );
            if o.cluster {
                let id = text(command(&mut c, &["CLUSTER", "MYID"])?)?;
                if ids.insert(id.clone(), i).is_some() {
                    return Err(invalid("RDMA endpoints identify the same cluster primary"));
                }
                node_ids.push(id);
                let info = text(command(&mut c, &["CLUSTER", "INFO"])?)?;
                let f = fields(&info);
                if f.get("cluster_state") != Some(&"ok")
                    || f.get("cluster_slots_assigned") != Some(&"16384")
                {
                    return Err(invalid("cluster must be healthy with all slots assigned"));
                }
                maps.push(text(command(&mut c, &["CLUSTER", "NODES"])?)?);
                slot_views.push(command(&mut c, &["CLUSTER", "SLOTS"])?);
            }
            initial.push(c);
        }
        let slots = if o.cluster {
            let first = topology(&maps[0], &ids)?;
            for m in &maps[1..] {
                if topology(m, &ids)? != first {
                    return Err(invalid("cluster nodes disagree about slot ownership"));
                }
            }
            for view in slot_views {
                if slot_response(view, &ids)? != first {
                    return Err(invalid("CLUSTER SLOTS disagrees with CLUSTER NODES"));
                }
            }
            first
        } else {
            vec![0; 16384]
        };
        let mut nodes = Vec::new();
        for (node, first) in o.nodes.iter().zip(initial) {
            let mut connections = vec![Arc::new(Mutex::new(first))];
            for _ in 1..POOL_SIZE {
                connections.push(Arc::new(Mutex::new(Connection::connect(
                    node,
                    o.request_timeout_ms,
                    rdma,
                )?)));
            }
            nodes.push(Node {
                connections,
                next: AtomicUsize::new(0),
                ctx: Arc::new(NodeCtx {
                    endpoint: node.clone(),
                    timeout_ms: o.request_timeout_ms,
                    rdma,
                    health: PoolHealth::default(),
                }),
            });
        }
        let info = BackendInfo {
            name: if rdma { "valkey_rdma" } else { "valkey_tcp" }.into(),
            server_version: "9.0.7".into(),
            client_version: format!("bundled libvalkey 0.2.1, source checkout {COMMIT}; linked artifacts operator-built"),
            acknowledgement: o.ack_contract.acknowledgement().unwrap_or("SET success; appendonly=yes, appendfsync=always, no-appendfsync-on-rewrite=no observed; RF1, no replica acknowledgement").into(),
            consistency: "primary-routed static slot map; topology redirects fail, no retries".into(),
            replication_factor: 1,
            measurement_boundary: "upstream libvalkey blocking command via tokio blocking pool through materialized reply; Valkey GET reads RAM, not NVMe WAL".into(),
            get_many_strategy: if o.cluster { "MGET split by hash slot, pipelined per primary concurrently; input order, duplicate keys and misses preserved" } else { "native MGET preserving input order, duplicate keys and misses" }.into(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({"cluster":o.cluster,"primary_count":o.nodes.len(),"endpoints":o.nodes,"node_ids":node_ids,"slot_count":if o.cluster {Some(16384)} else {None},"cluster_nodes_views":maps,"routing":"static CLUSTER NODES slot map linked by CLUSTER MYID; MOVED/ASK fail"}),
            configuration: json!({"ack_contract":o.ack_contract,"transport":if rdma {"upstream libvalkey VALKEY_CONN_RDMA, RDMA-CM RC"} else {"upstream libvalkey VALKEY_CONN_TCP"},"source_commit":COMMIT,"source_attestation":"client checkout commit checked at build; linked artifacts and server source provenance operator asserted","server_build_ids":server_builds,"request_timeout_ms":o.request_timeout_ms,"persistent_connections_per_primary":POOL_SIZE,"progress":"tokio spawn_blocking, per-connection mutex","pool_repair":{"policy":"a connection poisoned by an incomplete pipeline or in sticky error is replaced before its next request","attempts_per_request":REPLACEMENT_ATTEMPTS,"no_poison_before_first_byte":true,"counters":"client_pool in read observations and manifest run.client_pool_final"},"node_config":configs,"persistence":persistence,"replica_reads":false,"client_retries":0,"hardware_path_attested":false,"mutation_uncertainty":"timeout or transport failure does not imply rollback"}),
        };
        Ok(Self {
            nodes,
            slots,
            cluster: o.cluster,
            timeout: Duration::from_millis(o.request_timeout_ms),
            rdma,
            profile: ClientProfile::default(),
            info,
        })
    }
    pub fn arm_client_profile(&self, limit: usize) -> BackendResult<()> {
        if limit > 65_536 {
            return Err(invalid("Valkey client profile limit exceeds 65536"));
        }
        let mut samples = self
            .profile
            .samples
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.profile.remaining.store(0, Ordering::Release);
        samples.clear();
        self.profile.remaining.store(limit, Ordering::Release);
        Ok(())
    }

    pub fn finish_client_profile(&self, _deadline: Instant) -> BackendResult<Value> {
        let samples = self
            .profile
            .samples
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Zeroed under the samples lock: a sampler holds the same lock from its non-zero check
        // through its decrement, so none can decrement zero or add a sample after this point.
        self.profile.remaining.store(0, Ordering::Release);
        let records: Vec<_> = samples
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).clone())
            .collect();
        Ok(json!({
            "available": true,
            "source_commit": COMMIT,
            "rdma_source_sha256": env!("VALKEY_RDMA_SOURCE_SHA256"),
            "rdma_archive_sha256": env!("VALKEY_RDMA_ARCHIVE_SHA256"),
            "client_archive_sha256": env!("VALKEY_CLIENT_ARCHIVE_SHA256"),
            "snapshot": "nonblocking snapshot; unfinished worker or cancelled caller remains explicit",
            "clock": "client monotonic; durations only, no cross-host subtraction",
            "cq_available": self.rdma && cfg!(valkey_cq_hook),
            "cq_unavailable_reason": if !self.rdma { Some("TCP has no RDMA CQ") } else if !cfg!(valkey_cq_hook) { Some("client built without pinned CQ hook") } else { None },
            "poll_wait_scope": "poll(CQ and CM fds) elapsed, includes kernel blocking and scheduling; not isolated OS wakeup latency",
            "wake_to_cq_scope": "userspace poll return to first ibv_poll_cq after CQ-ready notification",
            "transport_scope": "libvalkey buffer I/O and reply parser; includes CQ and network",
            "records": records,
        }))
    }

    fn begin_client_sample(
        &self,
        id: Option<u64>,
        operation: &'static str,
        owner: usize,
        connection: usize,
    ) -> Option<SharedSample> {
        let id = id?;
        if self.profile.remaining.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut samples = self
            .profile
            .samples
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.profile.remaining.load(Ordering::Relaxed) == 0 {
            return None;
        }
        self.profile.remaining.fetch_sub(1, Ordering::Relaxed);
        let sample = Arc::new(StdMutex::new(ClientSample {
            caller_sample_id: id,
            operation,
            owner,
            connection,
            ..Default::default()
        }));
        samples.push(sample.clone());
        Some(sample)
    }

    fn transport(&self) -> &'static str {
        transport_name(self.rdma)
    }

    fn scalar_timeout(
        &self,
        owner: usize,
        command: ScalarCommand,
        progress: &RequestProgress,
    ) -> io::Error {
        scalar_timeout_error(
            self.transport(),
            command,
            self.timeout,
            owner,
            &self.nodes[owner].ctx.endpoint,
            progress,
        )
    }

    /// Adds this request's context to a timeout its worker raised before the outer deadline
    /// fired; other errors pass unchanged.
    fn scalar_worker_timeout(
        &self,
        owner: usize,
        command: ScalarCommand,
        progress: &RequestProgress,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        with_worker_timeout_context(error, || {
            scalar_worker_context(
                self.transport(),
                command,
                self.timeout,
                owner,
                &self.nodes[owner].ctx.endpoint,
                progress,
            )
        })
    }

    async fn execute_traced(
        &self,
        owner: usize,
        command: ScalarCommand,
        operands: &[&[u8]],
        id: u64,
    ) -> BackendResult<Response> {
        let args = command.args(operands);
        let progress = Arc::new(RequestProgress::new());
        let deadline = Instant::now() + self.timeout;
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        progress.assign(index);
        let conn = node.connections[index].clone();
        let ctx = Arc::clone(&node.ctx);
        let sample = self.begin_client_sample(Some(id), command.sample_operation(), owner, index);
        let _outcome = SampleOutcome(sample.clone());
        let run_sample = sample.clone();
        let run_progress = Arc::clone(&progress);
        let run = async move {
            let lock_start = run_sample.as_ref().map(|_| Instant::now());
            let mut guard = conn.lock_owned().await;
            run_progress.enter(RequestStage::BlockingQueue);
            if let (Some(start), Some(sample)) = (lock_start, run_sample.as_ref()) {
                update_sample(sample, |s| s.mutex_wait_ns = Some(elapsed_ns(start)));
            }
            let worker_sample = run_sample.clone();
            let enqueued = run_sample.as_ref().map(|_| Instant::now());
            let (result, finished) = tokio::task::spawn_blocking(move || {
                let started = worker_sample.as_ref().map(|_| Instant::now());
                let result = ctx.tracked_pipeline(
                    &mut guard,
                    &[&args],
                    deadline,
                    worker_sample.as_ref(),
                    enqueued,
                    started,
                    Some(&*run_progress),
                );
                drop(guard);
                if let Some(sample) = worker_sample.as_ref() {
                    update_sample(sample, |s| s.worker_finished = true);
                }
                let finished = worker_sample.as_ref().map(|_| Instant::now());
                (result, finished)
            })
            .await
            .map_err(|e| invalid(format!("libvalkey command task failed: {e}")))?;
            if let (Some(sample), Some(finished)) = (run_sample.as_ref(), finished) {
                update_sample(sample, |s| {
                    s.blocking_return_to_wake_ns = Some(elapsed_ns(finished))
                });
            }
            result?
                .into_iter()
                .next()
                .ok_or_else(|| invalid("missing libvalkey reply"))
        };
        let result = timeout(self.timeout, run)
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                self.scalar_timeout(owner, command, &progress).into()
            })
            .and_then(|r| {
                r.map_err(|error| self.scalar_worker_timeout(owner, command, &progress, error))
            });
        if let Some(sample) = sample {
            update_sample(&sample, |s| {
                s.pipeline_outcome = Some(if result.is_ok() { "ok" } else { "error" })
            });
        }
        result
    }
    async fn execute(
        &self,
        owner: usize,
        command: ScalarCommand,
        operands: &[&[u8]],
    ) -> BackendResult<Response> {
        let args = command.args(operands);
        let progress = Arc::new(RequestProgress::new());
        let deadline = Instant::now() + self.timeout;
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        progress.assign(index);
        let conn = node.connections[index].clone();
        let ctx = Arc::clone(&node.ctx);
        let run_progress = Arc::clone(&progress);
        let run = async move {
            let guard = conn.lock_owned().await;
            run_progress.enter(RequestStage::BlockingQueue);
            tokio::task::spawn_blocking(move || {
                let mut guard = guard;
                ctx.tracked_pipeline(
                    &mut guard,
                    &[&args],
                    deadline,
                    None,
                    None,
                    None,
                    Some(&*run_progress),
                )
                .and_then(|replies| {
                    replies
                        .into_iter()
                        .next()
                        .ok_or_else(|| invalid("missing libvalkey reply"))
                })
            })
            .await
            .map_err(|e| invalid(format!("RDMA command task failed: {e}")))?
        };
        timeout(self.timeout, run)
            .await
            .map_err(|_| self.scalar_timeout(owner, command, &progress))?
            .map_err(|error| self.scalar_worker_timeout(owner, command, &progress, error))
    }
    async fn pipeline_group(
        &self,
        owner: usize,
        groups: Vec<SlotGroup>,
        deadline: Instant,
        id: Option<u64>,
        progress: &Arc<MgetProgress>,
    ) -> BackendResult<Vec<(Vec<usize>, Response)>> {
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        progress[owner].assign(index);
        let conn = node.connections[index].clone();
        let ctx = Arc::clone(&node.ctx);
        let sample = self.begin_client_sample(id, "get_many", owner, index);
        let _outcome = SampleOutcome(sample.clone());
        let lock_start = sample.as_ref().map(|_| Instant::now());
        let guard = conn.lock_owned().await;
        progress[owner].enter(RequestStage::BlockingQueue);
        if let (Some(start), Some(sample)) = (lock_start, sample.as_ref()) {
            update_sample(sample, |s| s.mutex_wait_ns = Some(elapsed_ns(start)));
        }
        let worker_sample = sample.clone();
        let enqueued = sample.as_ref().map(|_| Instant::now());
        let worker_progress = Arc::clone(progress);
        let joined = tokio::task::spawn_blocking(move || {
            let started = worker_sample.as_ref().map(|_| Instant::now());
            let (commands, positions): (Vec<_>, Vec<_>) = groups.into_iter().unzip();
            let mut guard = guard;
            let refs: Vec<_> = commands.iter().map(Vec::as_slice).collect();
            let replies = ctx
                .tracked_pipeline(
                    &mut guard,
                    &refs,
                    deadline,
                    worker_sample.as_ref(),
                    enqueued,
                    started,
                    Some(&worker_progress[owner]),
                )
                .map_err(|error| {
                    // Read here, before the caller can see the error: this owner's slot and
                    // failure stage, plus its command and key counts.
                    with_worker_timeout_context(error, || {
                        mget_worker_context(
                            transport_name(ctx.rdma),
                            Duration::from_millis(ctx.timeout_ms),
                            &MgetOwner {
                                owner,
                                endpoint: &ctx.endpoint,
                                commands: commands.len(),
                                keys: positions.iter().map(Vec::len).sum(),
                                progress: &worker_progress[owner],
                            },
                        )
                    })
                });
            let result = replies.and_then(|replies| {
                if replies.len() != positions.len() {
                    return Err(invalid("pipeline response length mismatch"));
                }
                Ok(positions.into_iter().zip(replies).collect())
            });
            drop(guard);
            if let Some(sample) = worker_sample.as_ref() {
                update_sample(sample, |s| s.worker_finished = true);
            }
            let finished = worker_sample.as_ref().map(|_| Instant::now());
            (result, finished)
        })
        .await
        .map_err(|e| invalid(format!("libvalkey pipeline task failed: {e}")));
        progress[owner].enter(RequestStage::Completed);
        let result = match joined {
            Ok((result, finished)) => {
                if let (Some(sample), Some(finished)) = (sample.as_ref(), finished) {
                    update_sample(sample, |s| {
                        s.blocking_return_to_wake_ns = Some(elapsed_ns(finished))
                    });
                }
                result
            }
            Err(error) => Err(error),
        };
        if let Some(sample) = sample {
            update_sample(&sample, |s| {
                s.pipeline_outcome = Some(if result.is_ok() { "ok" } else { "error" })
            });
        }
        result
    }
    /// Key positions per hash slot, in slot order; standalone mode puts every key in slot 0.
    fn slot_groups(&self, keys: &[Bytes]) -> BTreeMap<usize, Vec<usize>> {
        let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (i, key) in keys.iter().enumerate() {
            let s = if self.cluster { slot(key) } else { 0 };
            groups.entry(s).or_default().push(i);
        }
        groups
    }

    fn mget_timeout(&self, keys: &[Bytes], progress: &MgetProgress) -> io::Error {
        // (MGET commands, keys) per owner, recomputed only on this error path.
        let mut load = [(0_usize, 0_usize); 5];
        for (s, indices) in self.slot_groups(keys) {
            let owner = &mut load[self.slots[s]];
            owner.0 += 1;
            owner.1 += indices.len();
        }
        mget_timeout_error(
            self.transport(),
            self.timeout,
            keys.len(),
            load.iter()
                .enumerate()
                .filter(|(_, (commands, _))| *commands > 0)
                .map(move |(owner, &(commands, owner_keys))| MgetOwner {
                    owner,
                    endpoint: &self.nodes[owner].ctx.endpoint,
                    commands,
                    keys: owner_keys,
                    progress: &progress[owner],
                }),
        )
    }

    async fn get_many_inner(
        &self,
        keys: &[Bytes],
        id: Option<u64>,
    ) -> BackendResult<Vec<Option<Bytes>>> {
        let progress: Arc<MgetProgress> = Arc::new(std::array::from_fn(|_| RequestProgress::new()));
        let deadline = Instant::now() + self.timeout;
        timeout(self.timeout, async {
            let mut owners: [Vec<SlotGroup>; 5] = std::array::from_fn(|_| Vec::new());
            for (s, indices) in self.slot_groups(keys) {
                let mut args = vec![b"MGET".to_vec()];
                args.extend(indices.iter().map(|&i| keys[i].to_vec()));
                owners[self.slots[s]].push((args, indices));
            }
            let [a, b, c, d, e] = owners;
            let (a, b, c, d, e) = tokio::try_join!(
                self.pipeline_group(0, a, deadline, id, &progress),
                self.pipeline_group(1, b, deadline, id, &progress),
                self.pipeline_group(2, c, deadline, id, &progress),
                self.pipeline_group(3, d, deadline, id, &progress),
                self.pipeline_group(4, e, deadline, id, &progress)
            )?;
            let mut output = vec![None; keys.len()];
            for (indices, response) in a.into_iter().chain(b).chain(c).chain(d).chain(e) {
                let Response::Array(values) = response else {
                    return Err(invalid("invalid MGET response"));
                };
                if values.len() != indices.len() {
                    return Err(invalid("MGET response length mismatch"));
                }
                for (i, v) in indices.into_iter().zip(values) {
                    output[i] = match v {
                        Response::Nil => None,
                        Response::Bytes(b) => Some(Bytes::from(b)),
                        _ => return Err(invalid("invalid MGET element")),
                    };
                }
            }
            Ok(output)
        })
        .await
        .map_err(|_| self.mget_timeout(keys, &progress))?
    }
    pub async fn traced_get(&self, key: &[u8], id: u64) -> BackendResult<Option<Bytes>> {
        match self
            .execute_traced(self.slots[slot(key)], ScalarCommand::Get, &[key], id)
            .await?
        {
            Response::Nil => Ok(None),
            Response::Bytes(b) => Ok(Some(Bytes::from(b))),
            _ => Err(invalid("invalid GET response")),
        }
    }

    pub async fn traced_put(&self, key: Bytes, value: Bytes, id: u64) -> BackendResult<()> {
        match self
            .execute_traced(
                self.slots[slot(&key)],
                ScalarCommand::Set,
                &[&key[..], &value[..]],
                id,
            )
            .await?
        {
            Response::Bytes(b) if b == b"OK" => Ok(()),
            _ => Err(invalid("invalid SET response")),
        }
    }

    pub async fn traced_delete(&self, key: Bytes, id: u64) -> BackendResult<()> {
        match self
            .execute_traced(self.slots[slot(&key)], ScalarCommand::Del, &[&key[..]], id)
            .await?
        {
            Response::Integer(n) if n == 0 || n == 1 => Ok(()),
            _ => Err(invalid("invalid DEL response")),
        }
    }

    pub async fn traced_get_many(
        &self,
        keys: &[Bytes],
        id: u64,
    ) -> BackendResult<Vec<Option<Bytes>>> {
        self.get_many_inner(keys, Some(id)).await
    }
}
impl KvBackend for ValkeyRdmaBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }
    fn read_observation(&self) -> BackendResult<Value> {
        // Valkey serves reads from RAM and exposes no storage read counters; only client pool
        // health is observed, so the generic read evidence stays unavailable.
        let sum = |counter: fn(&PoolHealth) -> &AtomicU64| -> u64 {
            self.nodes
                .iter()
                .map(|node| counter(&node.ctx.health).load(Ordering::Relaxed))
                .fold(0, u64::saturating_add)
        };
        Ok(json!({
            "available": false,
            "error": "Valkey exposes no storage read counters",
            "client_pool": {
                "connections_poisoned": sum(|h| &h.connections_poisoned),
                "connections_replaced": sum(|h| &h.connections_replaced),
                "replacement_attempts_failed": sum(|h| &h.replacement_attempts_failed),
                "timeouts_before_send": sum(|h| &h.timeouts_before_send),
                "pool_size_per_owner": POOL_SIZE,
            }
        }))
    }
    fn arm_client_profile(&self, limit: usize) -> BackendResult<()> {
        Self::arm_client_profile(self, limit)
    }
    fn finish_client_profile(&self, deadline: Instant) -> BackendResult<serde_json::Value> {
        Self::finish_client_profile(self, deadline)
    }
    async fn traced_get(&self, key: &[u8], id: u64) -> BackendResult<Option<Bytes>> {
        Self::traced_get(self, key, id).await
    }
    async fn traced_get_many(&self, keys: &[Bytes], id: u64) -> BackendResult<Vec<Option<Bytes>>> {
        Self::traced_get_many(self, keys, id).await
    }
    async fn traced_put(&self, key: Bytes, value: Bytes, id: u64) -> BackendResult<()> {
        Self::traced_put(self, key, value, id).await
    }
    async fn traced_delete(&self, key: Bytes, id: u64) -> BackendResult<()> {
        Self::traced_delete(self, key, id).await
    }
    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        match self
            .execute(self.slots[slot(key)], ScalarCommand::Get, &[key])
            .await?
        {
            Response::Nil => Ok(None),
            Response::Bytes(b) => Ok(Some(Bytes::from(b))),
            _ => Err(invalid("invalid GET response")),
        }
    }
    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        match self
            .execute(
                self.slots[slot(&key)],
                ScalarCommand::Set,
                &[&key[..], &value[..]],
            )
            .await?
        {
            Response::Bytes(b) if b == b"OK" => Ok(()),
            _ => Err(invalid("invalid SET response")),
        }
    }
    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        match self
            .execute(self.slots[slot(&key)], ScalarCommand::Del, &[&key[..]])
            .await?
        {
            Response::Integer(n) if n == 0 || n == 1 => Ok(()),
            _ => Err(invalid("invalid DEL response")),
        }
    }
    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        self.get_many_inner(keys, None).await
    }
    async fn put_batch(&self, _: &[(Bytes, Bytes)]) -> BackendResult<()> {
        Err(unsupported("Valkey cross-slot atomic put_batch"))
    }
    async fn shutdown(self) -> BackendResult<()> {
        // A cancelled spawn_blocking task still holds its guard. Wait for every
        // such task to release its context before reporting teardown complete.
        for node in &self.nodes {
            for connection in &node.connections {
                drop(connection.lock().await);
            }
        }
        Ok(())
    }
}
