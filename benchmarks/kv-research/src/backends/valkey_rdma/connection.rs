use super::*;

unsafe extern "C" {
    pub(super) fn kv_cq_sample_begin(sample: *mut CqSample);
    pub(super) fn kv_cq_sample_end();
    pub(super) fn kv_rdma_init() -> c_int;
    pub(super) fn kv_rdma_connect(
        host: *const c_char,
        port: c_int,
        ms: u64,
        rdma: c_int,
        timed_out: *mut c_int,
    ) -> *mut c_void;
    pub(super) fn kv_context_error(c: *const c_void) -> c_int;
    pub(super) fn kv_context_error_string(c: *const c_void) -> *const c_char;
    pub(super) fn kv_context_free(c: *mut c_void);
    pub(super) fn kv_append(
        c: *mut c_void,
        n: c_int,
        args: *const *const c_char,
        lengths: *const usize,
        rdma: c_int,
        timed_out: *mut c_int,
    ) -> c_int;
    pub(super) fn kv_get_reply(
        c: *mut c_void,
        rdma: c_int,
        remaining_ms: u64,
        timed_out: *mut c_int,
    ) -> *mut c_void;
    pub(super) fn kv_set_timeout(c: *mut c_void, timeout_ms: u64) -> c_int;
    pub(super) fn kv_reply_free(r: *mut c_void);
    pub(super) fn kv_reply_type(r: *const c_void) -> c_int;
    pub(super) fn kv_reply_data(r: *const c_void) -> *const c_char;
    pub(super) fn kv_reply_len(r: *const c_void) -> usize;
    pub(super) fn kv_reply_integer(r: *const c_void) -> i64;
    pub(super) fn kv_reply_count(r: *const c_void) -> usize;
    pub(super) fn kv_reply_element(r: *const c_void, i: usize) -> *const c_void;
    // Constants of the shim: no argument, no state, no precondition.
    pub(super) safe fn kv_reply_string() -> c_int;
    pub(super) safe fn kv_reply_array() -> c_int;
    pub(super) safe fn kv_reply_integer_type() -> c_int;
    pub(super) safe fn kv_reply_nil() -> c_int;
    pub(super) safe fn kv_reply_status() -> c_int;
    pub(super) safe fn kv_reply_error() -> c_int;
}
pub(super) struct Connection {
    pub(super) ptr: NonNull<c_void>,
    pub(super) rdma: bool,
    pub(super) timed_out: bool,
    pub(super) poisoned: bool,
    pub(super) timeout_ms: u64,
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
pub(super) struct Reply(NonNull<c_void>);
impl Drop for Reply {
    fn drop(&mut self) {
        // SAFETY: ptr is the uniquely owned root reply returned by libvalkey.
        unsafe { kv_reply_free(self.0.as_ptr()) }
    }
}
#[derive(Debug)]
pub(super) enum Response {
    Bytes(Vec<u8>),
    Nil,
    Integer(i64),
    Array(Vec<Response>),
}
pub(super) type SlotGroup = (Vec<Vec<u8>>, Vec<usize>);
/// Decodes one libvalkey reply tree into owned data.
///
/// # Safety
/// `p` must be null or a live libvalkey reply, and the caller must retain the
/// root reply for the whole call. Child pointers and string buffers are
/// borrowed from that reply and never escape this function.
unsafe fn decode(p: *const c_void) -> BackendResult<Response> {
    if p.is_null() {
        return Err(invalid("null libvalkey reply element"));
    }
    // SAFETY: p is non-null and a live reply by the function contract; the
    // shim only reads the type field.
    let t = unsafe { kv_reply_type(p) };
    if t == kv_reply_nil() {
        return Ok(Response::Nil);
    }
    if t == kv_reply_integer_type() {
        // SAFETY: p is a live integer reply by the type check; the shim only
        // reads the integer field.
        return Ok(Response::Integer(unsafe { kv_reply_integer(p) }));
    }
    if t == kv_reply_array() {
        // SAFETY: p is a live array reply by the type check; the shim only
        // reads the element count.
        let count = unsafe { kv_reply_count(p) };
        let mut v = Vec::with_capacity(count);
        for i in 0..count {
            // SAFETY: p is a live array reply and the shim returns null for
            // an index at or past the element count.
            let child = unsafe { kv_reply_element(p, i) };
            // SAFETY: child is null, which decode rejects, or a live child of
            // the root reply the caller retains.
            v.push(unsafe { decode(child) }?);
        }
        return Ok(Response::Array(v));
    }
    if t == kv_reply_string() || t == kv_reply_status() || t == kv_reply_error() {
        // SAFETY: p is a live string, status or error reply by the type
        // check; the shim only reads the length field.
        let len = unsafe { kv_reply_len(p) };
        // SAFETY: same reply; the shim only reads the string pointer field.
        let data = unsafe { kv_reply_data(p) };
        let bytes = if len == 0 {
            Vec::new()
        } else if data.is_null() {
            return Err(invalid("null libvalkey reply data"));
        } else {
            // SAFETY: data is non-null and libvalkey guarantees its buffer
            // holds len bytes while the root reply is retained. len is the
            // size of one libvalkey allocation, so len <= isize::MAX. u8 needs
            // no alignment and to_vec copies before this function returns.
            unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }.to_vec()
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
impl Connection {
    pub(super) fn connect(node: &str, ms: u64, rdma: bool) -> BackendResult<Self> {
        Self::connect_within(node, ms, rdma, ms)
    }
    /// Connects within `connect_ms`, then restores the normal per-command timeout `ms`.
    pub(super) fn connect_within(
        node: &str,
        ms: u64,
        rdma: bool,
        connect_ms: u64,
    ) -> BackendResult<Self> {
        static INIT: LazyLock<c_int> = LazyLock::new(|| {
            // SAFETY: LazyLock runs the upstream global registration once
            // before any connection is created, avoiding concurrent registry
            // writes.
            unsafe { kv_rdma_init() }
        });
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
    pub(super) fn is_broken(&self) -> bool {
        // SAFETY: ptr is a live context exclusively borrowed through &self by its owner.
        self.poisoned || unsafe { kv_context_error(self.ptr.as_ptr()) } != 0
    }
    pub(super) fn error(&self) -> Box<dyn std::error::Error + Send + Sync> {
        // SAFETY: ptr is a live context, so the shim returns its non-null,
        // NUL-terminated error buffer.
        let p = unsafe { kv_context_error_string(self.ptr.as_ptr()) };
        let text = if p.is_null() {
            "unknown libvalkey failure".into()
        } else {
            // SAFETY: p is non-null and NUL-terminated, and the context keeps
            // the buffer valid for this synchronous borrow; it is copied now.
            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
        };
        if self.timed_out {
            io::Error::new(io::ErrorKind::TimedOut, text).into()
        } else {
            Box::new(BackendTransportError::new(Box::new(io::Error::other(text))))
        }
    }
    pub(super) fn command(
        &mut self,
        args: &[Vec<u8>],
        deadline: Instant,
    ) -> BackendResult<Response> {
        self.pipeline(&[args], deadline, None, None)?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("missing libvalkey reply"))
    }
    pub(super) fn pipeline(
        &mut self,
        commands: &[&[Vec<u8>]],
        deadline: Instant,
        mut sample: Option<&mut ClientSample>,
        progress: Option<&RequestProgress>,
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
            if let Some(progress) = progress {
                progress.enter(RequestStage::Send);
            }
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
        if let Some(progress) = progress {
            progress.enter(RequestStage::Reply);
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
pub(super) const REPLACEMENT_ATTEMPTS: usize = 2;

/// Milliseconds one connection attempt may take: the remaining request time rounded up, capped at
/// the connection timeout. `None` once the deadline has passed.
pub(super) fn attempt_budget_ms(timeout_ms: u64, deadline: Instant) -> Option<u64> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    let remaining_ms = u64::try_from(remaining.as_millis().saturating_add(1)).unwrap_or(u64::MAX);
    Some(remaining_ms.min(timeout_ms))
}

pub(super) fn is_timed_out(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
}

/// Lifetime counters per owner endpoint; atomics, no lock on the request path.
#[derive(Default)]
pub(super) struct PoolHealth {
    pub(super) connections_poisoned: AtomicU64,
    pub(super) connections_replaced: AtomicU64,
    pub(super) replacement_attempts_failed: AtomicU64,
    pub(super) timeouts_before_send: AtomicU64,
}

/// Per-owner connection factory and health, shared by every pooled connection of that owner.
pub(super) struct NodeCtx {
    pub(super) endpoint: String,
    pub(super) timeout_ms: u64,
    pub(super) rdma: bool,
    pub(super) health: PoolHealth,
}

impl NodeCtx {
    /// Run a pipeline on a pooled connection, replacing the connection first when it is broken.
    pub(super) fn pipeline(
        &self,
        connection: &mut Connection,
        commands: &[&[Vec<u8>]],
        deadline: Instant,
        sample: Option<&SharedSample>,
        enqueued: Option<Instant>,
        started: Option<Instant>,
    ) -> BackendResult<Vec<Response>> {
        self.tracked_pipeline(
            connection, commands, deadline, sample, enqueued, started, None,
        )
    }

    /// [`Self::pipeline`] that also records the request's worker stages in `progress`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn tracked_pipeline(
        &self,
        connection: &mut Connection,
        commands: &[&[Vec<u8>]],
        deadline: Instant,
        sample: Option<&SharedSample>,
        enqueued: Option<Instant>,
        started: Option<Instant>,
        progress: Option<&RequestProgress>,
    ) -> BackendResult<Vec<Response>> {
        if let Some(progress) = progress {
            progress.enter(RequestStage::ConnectionCheck);
        }
        let result = match self.repair(connection, deadline) {
            Err(error) => {
                if is_timed_out(error.as_ref()) {
                    self.health
                        .timeouts_before_send
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(error)
            }
            Ok(()) => {
                let result = measured_pipeline(
                    connection, commands, deadline, sample, enqueued, started, progress,
                );
                if let Err(error) = &result {
                    // The connection was usable on entry, so poison here is new and bytes may
                    // have been sent.
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
        };
        if let Some(progress) = progress.filter(|_| result.is_ok()) {
            progress.enter(RequestStage::WorkerFinished);
        }
        result
    }

    /// Replace a broken connection with a fresh one. Each attempt is bounded by the time the
    /// request still has, never more than the connection timeout; nothing is attempted once the
    /// request deadline has passed.
    pub(super) fn repair(
        &self,
        connection: &mut Connection,
        deadline: Instant,
    ) -> BackendResult<()> {
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
