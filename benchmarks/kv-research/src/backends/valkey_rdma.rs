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
    io,
    net::SocketAddr,
    ptr::NonNull,
    sync::{
        Arc, LazyLock, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tidehunter::minibytes::Bytes;
use tokio::{sync::Mutex, time::timeout};

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

#[repr(C)]
#[derive(Default, Clone, Serialize)]
struct CqSample {
    poll_wait_ns: u64,
    poll_calls: u64,
    cq_poll_ns: u64,
    cq_poll_calls: u64,
    wake_to_cq_ns: u64,
    wake_to_cq_samples: u64,
    completions: u64,
    clock_failures: u64,
}

#[derive(Default, Clone, Serialize)]
struct ClientSample {
    caller_sample_id: u64,
    operation: &'static str,
    owner: usize,
    connection: usize,
    mutex_wait_ns: Option<u64>,
    blocking_enqueue_to_start_ns: Option<u64>,
    blocking_return_to_wake_ns: Option<u64>,
    command_encode_ns: Option<u64>,
    transport_and_parser_ns: Option<u64>,
    reply_decode_ns: Option<u64>,
    cq: Option<CqSample>,
    worker_finished: bool,
    pipeline_outcome: Option<&'static str>,
}

type SharedSample = Arc<StdMutex<ClientSample>>;

#[derive(Default)]
struct ClientProfile {
    remaining: AtomicUsize,
    samples: StdMutex<Vec<SharedSample>>,
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn update_sample(sample: &SharedSample, update: impl FnOnce(&mut ClientSample)) {
    update(&mut sample.lock().unwrap_or_else(|e| e.into_inner()));
}
struct SampleOutcome(Option<SharedSample>);
impl Drop for SampleOutcome {
    fn drop(&mut self) {
        if let Some(sample) = &self.0 {
            update_sample(sample, |s| {
                if s.pipeline_outcome.is_none() {
                    s.pipeline_outcome = Some("cancelled");
                }
            });
        }
    }
}
struct CqScope;
impl Drop for CqScope {
    fn drop(&mut self) {
        // SAFETY: only installed on the current blocking thread; the sample
        // remains in the enclosing stack frame until this guard is dropped.
        unsafe { kv_cq_sample_end() };
    }
}

fn measured_pipeline(
    connection: &mut Connection,
    commands: &[&[Vec<u8>]],
    deadline: Instant,
    sample: Option<&SharedSample>,
    enqueued: Option<Instant>,
    started: Option<Instant>,
) -> BackendResult<Vec<Response>> {
    let Some(sample) = sample else {
        return connection.pipeline(commands, deadline, None);
    };
    if let (Some(enqueued), Some(started)) = (enqueued, started) {
        update_sample(sample, |s| {
            s.blocking_enqueue_to_start_ns =
                Some(u64::try_from(started.duration_since(enqueued).as_nanos()).unwrap_or(u64::MAX))
        });
    }
    let mut phases = ClientSample::default();
    let mut cq = CqSample::default();
    let cq_enabled = connection.rdma && cfg!(valkey_cq_hook);
    let scope = if cq_enabled {
        // SAFETY: cq stays at this address for the lifetime of the guard and
        // is touched only by hooks on this same blocking thread.
        unsafe { kv_cq_sample_begin(&mut cq) };
        Some(CqScope)
    } else {
        None
    };
    let result = connection.pipeline(commands, deadline, Some(&mut phases));
    drop(scope);
    update_sample(sample, |s| {
        s.command_encode_ns = phases.command_encode_ns;
        s.transport_and_parser_ns = phases.transport_and_parser_ns;
        s.reply_decode_ns = phases.reply_decode_ns;
        s.cq = cq_enabled.then_some(cq);
    });
    result
}

unsafe extern "C" {
    fn kv_cq_sample_begin(sample: *mut CqSample);
    fn kv_cq_sample_end();
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
// SAFETY: The context has one owner and moves between threads without concurrent
// access. Once pooled, each access holds the per-connection mutex.
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
        Self::connect_within(node, ms, rdma, ms)
    }
    /// Connects within `connect_ms`, then restores the normal per-command timeout `ms`.
    fn connect_within(node: &str, ms: u64, rdma: bool, connect_ms: u64) -> BackendResult<Self> {
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
                connect_ms,
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
        if connect_ms != ms {
            // SAFETY: ptr is a live context exclusively owned by c.
            if unsafe { kv_set_timeout(raw, ms) } != 0 {
                return Err(c.error());
            }
        }
        Ok(c)
    }
    /// The context cannot serve another command: an earlier pipeline left unread replies
    /// (`poisoned`) or libvalkey recorded a sticky context error.
    fn is_broken(&self) -> bool {
        // SAFETY: ptr is a live context exclusively borrowed through &self by its owner.
        self.poisoned || unsafe { kv_context_error(self.ptr.as_ptr()) } != 0
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
        self.pipeline(&[args], deadline, None)?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("missing libvalkey reply"))
    }
    fn pipeline(
        &mut self,
        commands: &[&[Vec<u8>]],
        deadline: Instant,
        mut sample: Option<&mut ClientSample>,
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
        // Poison starts at the first byte appended (below), not here: a request that already
        // expired before sending anything leaves the connection clean.
        for args in commands {
            let encode_start = sample.as_ref().map(|_| Instant::now());
            if Instant::now() >= deadline {
                return Err(
                    io::Error::new(io::ErrorKind::TimedOut, "Valkey pipeline timed out").into(),
                );
            }
            let n = i32::try_from(args.len()).map_err(|_| invalid("too many command arguments"))?;
            let pointers: Vec<_> = args.iter().map(|a| a.as_ptr().cast::<c_char>()).collect();
            let lengths: Vec<_> = args.iter().map(Vec::len).collect();
            let mut timed_out = 0;
            // From here bytes may reach the peer; until every reply is consumed, any error
            // leaves this connection unusable.
            self.poisoned = true;
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
            if let (Some(start), Some(sample)) = (encode_start, sample.as_deref_mut()) {
                *sample.command_encode_ns.get_or_insert(0) += elapsed_ns(start);
            }
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
            let transport_start = sample.as_ref().map(|_| Instant::now());
            // SAFETY: this worker has exclusive access to the live context. The shim
            // returns an owned reply or null; timed_out is writable stack storage.
            let p = unsafe {
                kv_get_reply(
                    self.ptr.as_ptr(),
                    i32::from(self.rdma),
                    remaining_ms,
                    &mut timed_out,
                )
            };
            if let (Some(start), Some(sample)) = (transport_start, sample.as_deref_mut()) {
                *sample.transport_and_parser_ns.get_or_insert(0) += elapsed_ns(start);
            }
            self.timed_out = timed_out != 0;
            let reply = Reply(NonNull::new(p).ok_or_else(|| self.error())?);
            let decode_start = sample.as_ref().map(|_| Instant::now());
            // SAFETY: reply retains the root and its borrowed children until decode returns.
            let decoded = unsafe { decode(reply.0.as_ptr()) };
            if let (Some(start), Some(sample)) = (decode_start, sample.as_deref_mut()) {
                *sample.reply_decode_ns.get_or_insert(0) += elapsed_ns(start);
            }
            replies.push(decoded?);
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

/// Attempts to replace one broken connection per request; a failing endpoint is retried by
/// later requests instead of looping here.
const REPLACEMENT_ATTEMPTS: usize = 2;

/// Milliseconds one connection attempt may take: the remaining request time rounded up, capped at
/// the connection timeout. `None` once the deadline has passed.
fn attempt_budget_ms(timeout_ms: u64, deadline: Instant) -> Option<u64> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    let remaining_ms = u64::try_from(remaining.as_millis().saturating_add(1)).unwrap_or(u64::MAX);
    Some(remaining_ms.min(timeout_ms))
}

fn is_timed_out(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
}

/// Lifetime counters per owner endpoint; atomics, no lock on the request path.
#[derive(Default)]
struct PoolHealth {
    connections_poisoned: AtomicU64,
    connections_replaced: AtomicU64,
    replacement_attempts_failed: AtomicU64,
    timeouts_before_send: AtomicU64,
}

/// Per-owner connection factory and health, shared by every pooled connection of that owner.
struct NodeCtx {
    endpoint: String,
    timeout_ms: u64,
    rdma: bool,
    health: PoolHealth,
}

impl NodeCtx {
    /// Run a pipeline on a pooled connection, replacing the connection first when it is broken.
    fn pipeline(
        &self,
        connection: &mut Connection,
        commands: &[&[Vec<u8>]],
        deadline: Instant,
        sample: Option<&SharedSample>,
        enqueued: Option<Instant>,
        started: Option<Instant>,
    ) -> BackendResult<Vec<Response>> {
        if let Err(error) = self.repair(connection, deadline) {
            if is_timed_out(error.as_ref()) {
                self.health
                    .timeouts_before_send
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(error);
        }
        let result = measured_pipeline(connection, commands, deadline, sample, enqueued, started);
        if let Err(error) = &result {
            // The connection was usable on entry, so poison here is new and bytes may have been sent.
            let counter = if connection.poisoned {
                Some(&self.health.connections_poisoned)
            } else if is_timed_out(error.as_ref()) {
                Some(&self.health.timeouts_before_send)
            } else {
                None
            };
            if let Some(counter) = counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    /// Replace a broken connection with a fresh one. Each attempt is bounded by the time the
    /// request still has, never more than the connection timeout; nothing is attempted once the
    /// request deadline has passed.
    fn repair(&self, connection: &mut Connection, deadline: Instant) -> BackendResult<()> {
        if !connection.is_broken() {
            return Ok(());
        }
        let mut last = None;
        for _ in 0..REPLACEMENT_ATTEMPTS {
            let Some(budget_ms) = attempt_budget_ms(self.timeout_ms, deadline) else {
                break;
            };
            match Connection::connect_within(&self.endpoint, self.timeout_ms, self.rdma, budget_ms)
            {
                Ok(fresh) => {
                    // Dropping the old context frees it; the slot never keeps a dead connection.
                    *connection = fresh;
                    self.health
                        .connections_replaced
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error) => {
                    self.health
                        .replacement_attempts_failed
                        .fetch_add(1, Ordering::Relaxed);
                    last = Some(error);
                }
            }
        }
        Err(last.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Valkey connection replacement skipped: request deadline expired",
            )
            .into()
        }))
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

    async fn execute_traced(
        &self,
        owner: usize,
        args: Vec<Vec<u8>>,
        id: u64,
        operation: &'static str,
    ) -> BackendResult<Response> {
        let deadline = Instant::now() + self.timeout;
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        let conn = node.connections[index].clone();
        let ctx = Arc::clone(&node.ctx);
        let sample = self.begin_client_sample(Some(id), operation, owner, index);
        let _outcome = SampleOutcome(sample.clone());
        let run_sample = sample.clone();
        let run = async move {
            let lock_start = run_sample.as_ref().map(|_| Instant::now());
            let mut guard = conn.lock_owned().await;
            if let (Some(start), Some(sample)) = (lock_start, run_sample.as_ref()) {
                update_sample(sample, |s| s.mutex_wait_ns = Some(elapsed_ns(start)));
            }
            let worker_sample = run_sample.clone();
            let enqueued = run_sample.as_ref().map(|_| Instant::now());
            let (result, finished) = tokio::task::spawn_blocking(move || {
                let started = worker_sample.as_ref().map(|_| Instant::now());
                let result = ctx.pipeline(
                    &mut guard,
                    &[&args],
                    deadline,
                    worker_sample.as_ref(),
                    enqueued,
                    started,
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
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Valkey RDMA request timed out; mutation outcome may be unknown",
                )
                .into()
            })
            .and_then(|r| r);
        if let Some(sample) = sample {
            update_sample(&sample, |s| {
                s.pipeline_outcome = Some(if result.is_ok() { "ok" } else { "error" })
            });
        }
        result
    }
    async fn execute(&self, owner: usize, args: Vec<Vec<u8>>) -> BackendResult<Response> {
        let deadline = Instant::now() + self.timeout;
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        let conn = node.connections[index].clone();
        let ctx = Arc::clone(&node.ctx);
        let run = async move {
            let guard = conn.lock_owned().await;
            tokio::task::spawn_blocking(move || {
                let mut guard = guard;
                ctx.pipeline(&mut guard, &[&args], deadline, None, None, None)
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
        id: Option<u64>,
    ) -> BackendResult<Vec<(Vec<usize>, Response)>> {
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let node = &self.nodes[owner];
        let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
        let conn = node.connections[index].clone();
        let ctx = Arc::clone(&node.ctx);
        let sample = self.begin_client_sample(id, "get_many", owner, index);
        let _outcome = SampleOutcome(sample.clone());
        let lock_start = sample.as_ref().map(|_| Instant::now());
        let guard = conn.lock_owned().await;
        if let (Some(start), Some(sample)) = (lock_start, sample.as_ref()) {
            update_sample(sample, |s| s.mutex_wait_ns = Some(elapsed_ns(start)));
        }
        let worker_sample = sample.clone();
        let enqueued = sample.as_ref().map(|_| Instant::now());
        let joined = tokio::task::spawn_blocking(move || {
            let started = worker_sample.as_ref().map(|_| Instant::now());
            let (commands, positions): (Vec<_>, Vec<_>) = groups.into_iter().unzip();
            let mut guard = guard;
            let refs: Vec<_> = commands.iter().map(Vec::as_slice).collect();
            let replies = ctx.pipeline(
                &mut guard,
                &refs,
                deadline,
                worker_sample.as_ref(),
                enqueued,
                started,
            );
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
    async fn get_many_inner(
        &self,
        keys: &[Bytes],
        id: Option<u64>,
    ) -> BackendResult<Vec<Option<Bytes>>> {
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
                self.pipeline_group(0, a, deadline, id),
                self.pipeline_group(1, b, deadline, id),
                self.pipeline_group(2, c, deadline, id),
                self.pipeline_group(3, d, deadline, id),
                self.pipeline_group(4, e, deadline, id)
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
    pub async fn traced_get(&self, key: &[u8], id: u64) -> BackendResult<Option<Bytes>> {
        match self
            .execute_traced(
                self.slots[slot(key)],
                vec![b"GET".to_vec(), key.to_vec()],
                id,
                "get",
            )
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
                vec![b"SET".to_vec(), key.to_vec(), value.to_vec()],
                id,
                "put",
            )
            .await?
        {
            Response::Bytes(b) if b == b"OK" => Ok(()),
            _ => Err(invalid("invalid SET response")),
        }
    }

    pub async fn traced_delete(&self, key: Bytes, id: u64) -> BackendResult<()> {
        match self
            .execute_traced(
                self.slots[slot(&key)],
                vec![b"DEL".to_vec(), key.to_vec()],
                id,
                "delete",
            )
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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn client_profile_is_bounded_and_preserves_absence() {
        let backend = ValkeyRdmaBackend {
            nodes: Vec::new(),
            slots: Vec::new(),
            cluster: false,
            timeout: Duration::from_secs(1),
            rdma: false,
            profile: ClientProfile::default(),
            info: BackendInfo {
                name: String::new(),
                server_version: String::new(),
                client_version: String::new(),
                acknowledgement: String::new(),
                consistency: String::new(),
                replication_factor: 1,
                measurement_boundary: String::new(),
                get_many_strategy: String::new(),
                batch_atomicity: BatchAtomicity::Unsupported,
                topology: json!({}),
                configuration: json!({}),
            },
        };
        assert!(backend.arm_client_profile(65_537).is_err());
        backend.arm_client_profile(1).unwrap();
        let first = backend.begin_client_sample(Some(42), "get", 0, 1).unwrap();
        update_sample(&first, |s| s.mutex_wait_ns = Some(0));
        assert!(backend.begin_client_sample(Some(43), "put", 0, 0).is_none());
        let result = backend.finish_client_profile(Instant::now()).unwrap();
        assert_eq!(result["records"].as_array().unwrap().len(), 1);
        assert_eq!(result["records"][0]["caller_sample_id"], 42);
        assert_eq!(result["records"][0]["mutex_wait_ns"], 0);
        assert!(result["records"][0]["cq"].is_null());
        assert_eq!(result["cq_available"], false);
        assert!(backend.begin_client_sample(Some(44), "get", 0, 0).is_none());
    }
    #[test]
    fn disconnected_tcp_pipeline_stays_poisoned() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            drop(socket);
        });
        let mut connection = Connection::connect(&address.to_string(), 1000, false).unwrap();
        peer.join().unwrap();
        let command = vec![b"GET".to_vec(), b"key".to_vec()];
        let deadline = Instant::now() + Duration::from_secs(1);
        assert!(connection.command(&command, deadline).is_err());
        assert!(connection.poisoned);
        let error = connection.command(&command, deadline).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unusable after incomplete pipeline")
        );
    }
    #[test]
    fn replacement_attempts_use_the_remaining_request_time_not_a_full_timeout() {
        let far = Instant::now() + Duration::from_secs(60);
        assert_eq!(attempt_budget_ms(1000, far), Some(1000));
        let near = attempt_budget_ms(1000, Instant::now() + Duration::from_millis(40)).unwrap();
        assert!((1..=41).contains(&near), "{near}");
        assert_eq!(attempt_budget_ms(1000, Instant::now()), None);
        assert_eq!(
            attempt_budget_ms(
                1000,
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .unwrap()
            ),
            None
        );
    }

    #[test]
    fn profile_sampling_stops_exactly_at_finish_under_concurrent_samplers() {
        let backend = Arc::new(ValkeyRdmaBackend {
            nodes: Vec::new(),
            slots: Vec::new(),
            cluster: false,
            timeout: Duration::from_secs(1),
            rdma: false,
            profile: ClientProfile::default(),
            info: BackendInfo {
                name: String::new(),
                server_version: String::new(),
                client_version: String::new(),
                acknowledgement: String::new(),
                consistency: String::new(),
                replication_factor: 1,
                measurement_boundary: String::new(),
                get_many_strategy: String::new(),
                batch_atomicity: BatchAtomicity::Unsupported,
                topology: json!({}),
                configuration: json!({}),
            },
        });
        for round in 0..50 {
            backend.arm_client_profile(3).unwrap();
            let samplers: Vec<_> = (0..4)
                .map(|thread| {
                    let backend = Arc::clone(&backend);
                    std::thread::spawn(move || {
                        for id in 0..200 {
                            let _ =
                                backend.begin_client_sample(Some(thread * 1000 + id), "get", 0, 0);
                        }
                    })
                })
                .collect();
            let finished = backend.finish_client_profile(Instant::now()).unwrap();
            for sampler in samplers {
                sampler.join().unwrap();
            }
            let recorded = finished["records"].as_array().unwrap().len();
            assert!(
                recorded <= 3,
                "round {round}: {recorded} records over the limit"
            );
            // The counter must stay at zero (a decrement of zero would wrap to usize::MAX) and
            // nothing may be added after the snapshot.
            assert_eq!(
                backend.profile.remaining.load(Ordering::Acquire),
                0,
                "round {round}"
            );
            assert_eq!(
                backend.profile.samples.lock().unwrap().len(),
                recorded,
                "round {round}"
            );
        }
    }

    fn tcp_ctx(endpoint: &str) -> NodeCtx {
        NodeCtx {
            endpoint: endpoint.to_owned(),
            timeout_ms: 1000,
            rdma: false,
            health: PoolHealth::default(),
        }
    }

    fn count(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    #[test]
    fn request_expired_before_any_byte_is_sent_does_not_poison_the_connection() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let ctx = tcp_ctx(&address);
        // The kernel completes the handshake from the backlog; the peer never has to accept.
        let mut connection = Connection::connect(&address, 1000, false).unwrap();
        let command = vec![b"GET".to_vec(), b"key".to_vec()];
        let expired = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        let error = ctx
            .pipeline(&mut connection, &[&command], expired, None, None, None)
            .unwrap_err();
        assert!(is_timed_out(error.as_ref()));
        assert!(!connection.poisoned);
        assert!(!connection.is_broken());
        assert_eq!(count(&ctx.health.timeouts_before_send), 1);
        assert_eq!(count(&ctx.health.connections_poisoned), 0);
        assert_eq!(count(&ctx.health.connections_replaced), 0);
    }

    #[test]
    fn poisoned_connection_is_replaced_before_its_next_request() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let ctx = tcp_ctx(&address);
        let first = listener.try_clone().unwrap();
        let peer = std::thread::spawn(move || drop(first.accept().unwrap().0));
        let mut connection = Connection::connect(&address, 1000, false).unwrap();
        peer.join().unwrap();
        let command = vec![b"GET".to_vec(), b"key".to_vec()];
        let deadline = Instant::now() + Duration::from_secs(1);
        assert!(
            ctx.pipeline(&mut connection, &[&command], deadline, None, None, None)
                .is_err()
        );
        assert!(connection.poisoned);
        assert_eq!(count(&ctx.health.connections_poisoned), 1);
        // The replacement handshake is again served by the backlog; no byte is exchanged.
        ctx.repair(&mut connection, deadline).unwrap();
        assert!(!connection.poisoned);
        assert!(!connection.is_broken());
        assert_eq!(count(&ctx.health.connections_replaced), 1);
    }

    #[test]
    fn failed_replacement_is_bounded_counted_and_keeps_the_slot_broken() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let ctx = tcp_ctx(&address);
        let mut connection = Connection::connect(&address, 1000, false).unwrap();
        connection.poisoned = true;
        // Nothing listens any more, so every connect attempt is refused.
        drop(listener);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(ctx.repair(&mut connection, deadline).is_err());
        assert_eq!(
            count(&ctx.health.replacement_attempts_failed),
            REPLACEMENT_ATTEMPTS as u64
        );
        assert_eq!(count(&ctx.health.connections_replaced), 0);
        assert!(connection.is_broken());
    }

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
    fn live_config(fsync: &str) -> Response {
        Response::Array(
            [
                "appendonly",
                "yes",
                "appendfsync",
                fsync,
                "no-appendfsync-on-rewrite",
                "no",
                "maxmemory-policy",
                "noeviction",
            ]
            .into_iter()
            .map(|value| Response::Bytes(value.as_bytes().to_vec()))
            .collect(),
        )
    }

    #[test]
    fn contracts_parse_for_rdma_and_paired_tcp_and_check_live_settings() {
        let mut input = json!({"nodes":["127.0.0.1:16379"],"cluster":false,
            "required_replication_factor":1,"required_primary_nodes":1,"request_timeout_ms":1000});
        assert_eq!(
            Options::parse(&input).unwrap().ack_contract,
            AckContract::Synced
        );
        assert!(config(live_config("everysec"), AckContract::default()).is_err());
        for (contract, fsync) in [
            (AckContract::Synced, "always"),
            (AckContract::OsBuffered, "no"),
            (AckContract::PeriodicSync, "everysec"),
        ] {
            input["ack_contract"] = json!(contract);
            assert!(validate_options(&input).is_ok());
            input["client"] = json!("libvalkey");
            assert!(validate_tcp_options(&input).is_ok());
            input.as_object_mut().unwrap().remove("client");
            for actual in ["always", "no", "everysec"] {
                let result = config(live_config(actual), contract);
                assert_eq!(result.is_ok(), actual == fsync);
                if let Err(error) = result {
                    assert!(error.to_string().contains(contract.as_str()));
                }
            }
            for index in [1, 5, 7] {
                let Response::Array(mut fields) = live_config(fsync) else {
                    unreachable!()
                };
                fields[index] = Response::Bytes(b"invalid".to_vec());
                assert!(config(Response::Array(fields), contract).is_err());
            }
        }
        for invalid_contract in ["page_cache", "invalid"] {
            input["ack_contract"] = json!(invalid_contract);
            assert!(validate_options(&input).is_err());
            input["client"] = json!("libvalkey");
            assert!(validate_tcp_options(&input).is_err());
            input.as_object_mut().unwrap().remove("client");
        }
    }
}
