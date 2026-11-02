//! Pinned upstream libvalkey RDMA, with a deliberately static Cluster slot map.
//! The upstream cluster client hardcodes TCP; never use it for this transport.

use crate::backend::{
    BackendInfo, BackendResult, BackendTransportError, BatchAtomicity, KvBackend, unsupported,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::{CStr, CString, c_char, c_int, c_void},
    io,
    net::SocketAddr,
    ptr::NonNull,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tidehunter::minibytes::Bytes;
use tokio::{sync::Mutex, time::timeout};

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
}
fn invalid(s: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, s.into()).into()
}
impl Options {
    fn parse(v: &Value) -> BackendResult<Self> {
        let o: Self = serde_json::from_value(v.clone())?;
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

unsafe extern "C" {
    fn kv_rdma_init() -> c_int;
    fn kv_rdma_connect(
        host: *const c_char,
        port: c_int,
        ms: u64,
        rdma: c_int,
        timed_out: *mut c_int,
    ) -> *mut c_void;
    fn kv_context_error(c: *const c_void) -> c_int;
    fn kv_context_error_string(c: *const c_void) -> *const c_char;
    fn kv_context_free(c: *mut c_void);
    fn kv_append(
        c: *mut c_void,
        n: c_int,
        args: *const *const c_char,
        lengths: *const usize,
        rdma: c_int,
        timed_out: *mut c_int,
    ) -> c_int;
    fn kv_get_reply(
        c: *mut c_void,
        rdma: c_int,
        remaining_ms: u64,
        timed_out: *mut c_int,
    ) -> *mut c_void;
    fn kv_set_timeout(c: *mut c_void, timeout_ms: u64) -> c_int;
    fn kv_reply_free(r: *mut c_void);
    fn kv_reply_type(r: *const c_void) -> c_int;
    fn kv_reply_data(r: *const c_void) -> *const c_char;
    fn kv_reply_len(r: *const c_void) -> usize;
    fn kv_reply_integer(r: *const c_void) -> i64;
    fn kv_reply_count(r: *const c_void) -> usize;
    fn kv_reply_element(r: *const c_void, i: usize) -> *const c_void;
    fn kv_reply_string() -> c_int;
    fn kv_reply_array() -> c_int;
    fn kv_reply_integer_type() -> c_int;
    fn kv_reply_nil() -> c_int;
    fn kv_reply_status() -> c_int;
    fn kv_reply_error() -> c_int;
}
struct Connection {
    ptr: NonNull<c_void>,
    rdma: bool,
    timed_out: bool,
    poisoned: bool,
    timeout_ms: u64,
}
// SAFETY: The context has exclusive ownership and is only accessed while its
// per-connection mutex is held. libvalkey contexts can be moved between threads.
unsafe impl Send for Connection {}
impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: ptr is the uniquely owned live context returned by libvalkey.
        unsafe { kv_context_free(self.ptr.as_ptr()) }
    }
}
struct Reply(NonNull<c_void>);
impl Drop for Reply {
    fn drop(&mut self) {
        // SAFETY: ptr is the uniquely owned root reply returned by libvalkey.
        unsafe { kv_reply_free(self.0.as_ptr()) }
    }
}
#[derive(Debug)]
enum Response {
    Bytes(Vec<u8>),
    Nil,
    Integer(i64),
    Array(Vec<Response>),
}
type SlotGroup = (Vec<Vec<u8>>, Vec<usize>);
// The caller retains the root Reply for the whole traversal. Child pointers and
// string buffers are borrowed from that reply and never escape this function.
unsafe fn decode(p: *const c_void) -> BackendResult<Response> {
    if p.is_null() {
        return Err(invalid("null libvalkey reply element"));
    }
    // SAFETY: p is a live root or child valkeyReply from that retained reply;
    // libvalkey guarantees each non-null string buffer has its reported length.
    unsafe {
        let t = kv_reply_type(p);
        if t == kv_reply_nil() {
            return Ok(Response::Nil);
        }
        if t == kv_reply_integer_type() {
            return Ok(Response::Integer(kv_reply_integer(p)));
        }
        if t == kv_reply_array() {
            let count = kv_reply_count(p);
            let mut v = Vec::with_capacity(count);
            for i in 0..count {
                v.push(decode(kv_reply_element(p, i))?);
            }
            return Ok(Response::Array(v));
        }
        if t == kv_reply_string() || t == kv_reply_status() || t == kv_reply_error() {
            let len = kv_reply_len(p);
            let data = kv_reply_data(p);
            let bytes = if len == 0 {
                Vec::new()
            } else if data.is_null() {
                return Err(invalid("null libvalkey reply data"));
            } else {
                std::slice::from_raw_parts(data.cast::<u8>(), len).to_vec()
            };
            if t == kv_reply_error() {
                return Err(invalid(format!(
                    "Valkey error (no redirect or mutation retry): {}",
                    String::from_utf8_lossy(&bytes)
                )));
            }
            return Ok(Response::Bytes(bytes));
        }
        Err(invalid("unexpected libvalkey reply type"))
    }
}
impl Connection {
    fn connect(node: &str, ms: u64, rdma: bool) -> BackendResult<Self> {
        // SAFETY: LazyLock runs the upstream global registration once before
        // any connection is created, avoiding concurrent registry writes.
        static INIT: LazyLock<c_int> = LazyLock::new(|| unsafe { kv_rdma_init() });
        if *INIT != 0 {
            return Err(invalid("libvalkey RDMA initialization failed"));
        }
        let addr: SocketAddr = node.parse()?;
        let host = CString::new(addr.ip().to_string())?;
        let mut timed_out = 0;
        // SAFETY: host is NUL-terminated and alive for the call; timed_out
        // points to writable storage. Returned context is owned by Connection.
        let raw = unsafe {
            kv_rdma_connect(
                host.as_ptr(),
                i32::from(addr.port()),
                ms,
                i32::from(rdma),
                &mut timed_out,
            )
        };
        let ptr = NonNull::new(raw).ok_or_else(|| invalid("libvalkey allocation failed"))?;
        let c = Self {
            ptr,
            rdma,
            timed_out: timed_out != 0,
            poisoned: false,
            timeout_ms: ms,
        };
        // SAFETY: ptr is a live context and is exclusively owned by c.
        if unsafe { kv_context_error(raw) } != 0 {
            return Err(c.error());
        }
        Ok(c)
    }
    fn error(&self) -> Box<dyn std::error::Error + Send + Sync> {
        // SAFETY: ptr is a live context; its NUL-terminated error buffer remains
        // valid throughout this synchronous borrow and is copied immediately.
        let text = unsafe {
            let p = kv_context_error_string(self.ptr.as_ptr());
            if p.is_null() {
                "unknown libvalkey failure".into()
            } else {
                CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        };
        if self.timed_out {
            io::Error::new(io::ErrorKind::TimedOut, text).into()
        } else {
            Box::new(BackendTransportError::new(Box::new(io::Error::other(text))))
        }
    }
    fn command(&mut self, args: &[Vec<u8>], deadline: Instant) -> BackendResult<Response> {
        self.pipeline(&[args], deadline)?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("missing libvalkey reply"))
    }
    fn pipeline(
        &mut self,
        commands: &[&[Vec<u8>]],
        deadline: Instant,
    ) -> BackendResult<Vec<Response>> {
        if self.poisoned {
            return Err(invalid(
                "libvalkey connection is unusable after incomplete pipeline",
            ));
        }
        // SAFETY: this method has exclusive access to the live context.
        if unsafe { kv_context_error(self.ptr.as_ptr()) } != 0 {
            return Err(self.error());
        }
        // Until every reply is consumed, any error leaves this connection unusable.
        self.poisoned = true;
        for args in commands {
            if Instant::now() >= deadline {
                return Err(
                    io::Error::new(io::ErrorKind::TimedOut, "Valkey pipeline timed out").into(),
                );
            }
            let n = i32::try_from(args.len()).map_err(|_| invalid("too many command arguments"))?;
            let pointers: Vec<_> = args.iter().map(|a| a.as_ptr().cast::<c_char>()).collect();
            let lengths: Vec<_> = args.iter().map(Vec::len).collect();
            let mut timed_out = 0;
            // SAFETY: the exclusive context and all argument buffers remain
            // valid throughout this synchronous append; timed_out is writable.
            let result = unsafe {
                kv_append(
                    self.ptr.as_ptr(),
                    n,
                    pointers.as_ptr(),
                    lengths.as_ptr(),
                    i32::from(self.rdma),
                    &mut timed_out,
                )
            };
            self.timed_out = timed_out != 0;
            if result != 0 {
                return Err(self.error());
            }
        }
        let mut replies = Vec::with_capacity(commands.len());
        for _ in commands {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::TimedOut, "Valkey pipeline timed out")
                })?;
            let remaining_ms =
                u64::try_from(remaining.as_millis().saturating_add(1)).unwrap_or(u64::MAX);
            let mut timed_out = 0;
            // SAFETY: this thread exclusively owns the live context and the
            // shim returns a uniquely owned reply or null on failure.
            let p = unsafe {
                kv_get_reply(
                    self.ptr.as_ptr(),
                    i32::from(self.rdma),
                    remaining_ms,
                    &mut timed_out,
                )
            };
            self.timed_out = timed_out != 0;
            let reply = Reply(NonNull::new(p).ok_or_else(|| self.error())?);
            // SAFETY: reply owns the live root for the duration of decoding.
            replies.push(unsafe { decode(reply.0.as_ptr()) }?);
        }
        // SAFETY: the context is still exclusively held; restoring the original
        // timeout prevents this pipeline's remaining budget leaking to later calls.
        if unsafe { kv_set_timeout(self.ptr.as_ptr(), self.timeout_ms) } != 0 {
            return Err(self.error());
        }
        self.poisoned = false;
        Ok(replies)
    }
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
fn config(r: Response) -> BackendResult<BTreeMap<String, String>> {
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
        ("appendfsync", "always"),
        ("no-appendfsync-on-rewrite", "no"),
        ("maxmemory-policy", "noeviction"),
    ] {
        if map.get(k).map(String::as_str) != Some(v) {
            return Err(invalid(format!("CONFIG {k} must be {v}")));
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
}
pub struct ValkeyRdmaBackend {
    nodes: Vec<Node>,
    slots: Vec<usize>,
    cluster: bool,
    timeout: Duration,
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
            configs.push(config(command(
                &mut c,
                &[
                    "CONFIG",
                    "GET",
                    "appendonly",
                    "appendfsync",
                    "no-appendfsync-on-rewrite",
                    "maxmemory-policy",
                ],
            )?)?);
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
            });
        }
        let info = BackendInfo {
            name: if rdma { "valkey_rdma" } else { "valkey_tcp" }.into(),
            server_version: "9.0.7".into(),
            client_version: format!("bundled libvalkey 0.2.1, source checkout {COMMIT}; linked artifacts operator-built"),
            acknowledgement: "SET success; appendonly=yes, appendfsync=always, no-appendfsync-on-rewrite=no observed; RF1, no replica acknowledgement".into(),
            consistency: "primary-routed static slot map; topology redirects fail, no retries".into(),
            replication_factor: 1,
            measurement_boundary: "upstream libvalkey blocking command via tokio blocking pool through materialized reply; Valkey GET reads RAM, not NVMe WAL".into(),
            get_many_strategy: if o.cluster { "MGET split by hash slot, pipelined per primary concurrently; input order, duplicate keys and misses preserved" } else { "native MGET preserving input order, duplicate keys and misses" }.into(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({"cluster":o.cluster,"primary_count":o.nodes.len(),"endpoints":o.nodes,"node_ids":node_ids,"slot_count":if o.cluster {Some(16384)} else {None},"cluster_nodes_views":maps,"routing":"static CLUSTER NODES slot map linked by CLUSTER MYID; MOVED/ASK fail"}),
            configuration: json!({"transport":if rdma {"upstream libvalkey VALKEY_CONN_RDMA, RDMA-CM RC"} else {"upstream libvalkey VALKEY_CONN_TCP"},"source_commit":COMMIT,"source_attestation":"client checkout commit checked at build; linked artifacts and server source provenance operator asserted","server_build_ids":server_builds,"request_timeout_ms":o.request_timeout_ms,"persistent_connections_per_primary":POOL_SIZE,"progress":"tokio spawn_blocking, per-connection mutex","node_config":configs,"persistence":persistence,"replica_reads":false,"client_retries":0,"hardware_path_attested":false,"mutation_uncertainty":"timeout or transport failure does not imply rollback"})
        };
        Ok(Self {
            nodes,
            slots,
            cluster: o.cluster,
            timeout: Duration::from_millis(o.request_timeout_ms),
            info,
        })
    }
    async fn execute(&self, owner: usize, args: Vec<Vec<u8>>) -> BackendResult<Response> {
        let deadline = Instant::now() + self.timeout;
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        let conn = node.connections[index].clone();
        let run = async move {
            let guard = conn.lock_owned().await;
            tokio::task::spawn_blocking(move || {
                let mut guard = guard;
                guard.command(&args, deadline)
            })
            .await
            .map_err(|e| invalid(format!("RDMA command task failed: {e}")))?
        };
        timeout(self.timeout, run).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Valkey RDMA request timed out; mutation outcome may be unknown",
            )
        })?
    }
    async fn pipeline_group(
        &self,
        owner: usize,
        groups: Vec<SlotGroup>,
        deadline: Instant,
    ) -> BackendResult<Vec<(Vec<usize>, Response)>> {
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        let conn = node.connections[index].clone();
        let guard = conn.lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let (commands, positions): (Vec<_>, Vec<_>) = groups.into_iter().unzip();
            let mut guard = guard;
            let refs: Vec<_> = commands.iter().map(Vec::as_slice).collect();
            let replies = guard.pipeline(&refs, deadline)?;
            if replies.len() != positions.len() {
                return Err(invalid("pipeline response length mismatch"));
            }
            Ok(positions.into_iter().zip(replies).collect())
        })
        .await
        .map_err(|e| invalid(format!("libvalkey pipeline task failed: {e}")))?
    }
}
impl KvBackend for ValkeyRdmaBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }
    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        match self
            .execute(self.slots[slot(key)], vec![b"GET".to_vec(), key.to_vec()])
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
                vec![b"SET".to_vec(), key.to_vec(), value.to_vec()],
            )
            .await?
        {
            Response::Bytes(b) if b == b"OK" => Ok(()),
            _ => Err(invalid("invalid SET response")),
        }
    }
    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        match self
            .execute(self.slots[slot(&key)], vec![b"DEL".to_vec(), key.to_vec()])
            .await?
        {
            Response::Integer(n) if n == 0 || n == 1 => Ok(()),
            _ => Err(invalid("invalid DEL response")),
        }
    }
    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        let deadline = Instant::now() + self.timeout;
        timeout(self.timeout, async {
            let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for (i, key) in keys.iter().enumerate() {
                let s = if self.cluster { slot(key) } else { 0 };
                groups.entry(s).or_default().push(i);
            }
            let mut owners: [Vec<SlotGroup>; 5] = std::array::from_fn(|_| Vec::new());
            for (s, indices) in groups {
                let mut args = vec![b"MGET".to_vec()];
                args.extend(indices.iter().map(|&i| keys[i].to_vec()));
                owners[self.slots[s]].push((args, indices));
            }
            let [a, b, c, d, e] = owners;
            let (a, b, c, d, e) = tokio::try_join!(
                self.pipeline_group(0, a, deadline),
                self.pipeline_group(1, b, deadline),
                self.pipeline_group(2, c, deadline),
                self.pipeline_group(3, d, deadline),
                self.pipeline_group(4, e, deadline)
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
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Valkey MGET timed out"))?
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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_tags_and_binary() {
        assert_eq!(slot(b"123456789"), 12739);
        assert_eq!(slot(b"a{same}x"), slot(b"b{same}y"));
        assert_ne!(slot(b"a{}"), slot(b"b{}"));
        assert_eq!(slot(b"a{\0x}y"), slot(b"{\0x}"));
    }
    #[test]
    fn static_topology_rejects_gaps_duplicates_and_migration() {
        let ids = HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]);
        let good = "a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\nb 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383";
        let slots = topology(good, &ids).unwrap();
        assert_eq!(slots[8191], 0);
        assert_eq!(slots[8192], 1);
        assert!(topology(&good.replace("8192-16383", "8193-16383"), &ids).is_err());
        assert!(topology(&good.replace("8192-16383", "8191-16383"), &ids).is_err());
        assert!(topology(&good.replace("8192-16383", "8192-16383 [1->-b]"), &ids).is_err());
    }
    #[test]
    fn options_require_numeric_distinct_endpoints_and_rf1() {
        let mut v = json!({"nodes":["127.0.0.1:16379"],"cluster":false,"required_replication_factor":1,"required_primary_nodes":1,"request_timeout_ms":1000});
        assert!(validate_options(&v).is_ok());
        v["nodes"] = json!(["localhost:16379"]);
        assert!(validate_options(&v).is_err());
        v["nodes"] = json!(["127.0.0.1:16379"]);
        v["required_replication_factor"] = json!(2);
        assert!(validate_options(&v).is_err());
    }
}
