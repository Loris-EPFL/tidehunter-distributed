use super::*;

#[repr(C)]
#[derive(Default, Clone, Serialize)]
pub(super) struct CqSample {
    pub(super) poll_wait_ns: u64,
    pub(super) poll_calls: u64,
    pub(super) cq_poll_ns: u64,
    pub(super) cq_poll_calls: u64,
    pub(super) wake_to_cq_ns: u64,
    pub(super) wake_to_cq_samples: u64,
    pub(super) completions: u64,
    pub(super) clock_failures: u64,
}

#[derive(Default, Clone, Serialize)]
pub(super) struct ClientSample {
    pub(super) caller_sample_id: u64,
    pub(super) operation: &'static str,
    pub(super) owner: usize,
    pub(super) connection: usize,
    pub(super) mutex_wait_ns: Option<u64>,
    pub(super) blocking_enqueue_to_start_ns: Option<u64>,
    pub(super) blocking_return_to_wake_ns: Option<u64>,
    pub(super) command_encode_ns: Option<u64>,
    pub(super) transport_and_parser_ns: Option<u64>,
    pub(super) reply_decode_ns: Option<u64>,
    pub(super) cq: Option<CqSample>,
    pub(super) worker_finished: bool,
    pub(super) pipeline_outcome: Option<&'static str>,
}

pub(super) type SharedSample = Arc<StdMutex<ClientSample>>;

#[derive(Default)]
pub(super) struct ClientProfile {
    pub(super) remaining: AtomicUsize,
    pub(super) samples: StdMutex<Vec<SharedSample>>,
}

pub(super) fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn update_sample(sample: &SharedSample, update: impl FnOnce(&mut ClientSample)) {
    update(&mut sample.lock().unwrap_or_else(|e| e.into_inner()));
}
pub(super) struct SampleOutcome(pub(super) Option<SharedSample>);
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
pub(super) struct CqScope;
impl Drop for CqScope {
    fn drop(&mut self) {
        // SAFETY: only installed on the current blocking thread; the sample
        // remains in the enclosing stack frame until this guard is dropped.
        unsafe { kv_cq_sample_end() };
    }
}

/// Scalar command issued through the pooled connections. Its name is the only command text a
/// timeout diagnostic reports; key and value bytes never appear in an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScalarCommand {
    Get,
    Set,
    Del,
}

impl ScalarCommand {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Set => "SET",
            Self::Del => "DEL",
        }
    }

    /// Operation label of the client profile sample.
    pub(super) fn sample_operation(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Set => "put",
            Self::Del => "delete",
        }
    }

    pub(super) fn mutates(self) -> bool {
        !matches!(self, Self::Get)
    }

    /// Wire arguments: the command name, then the operands unchanged.
    pub(super) fn args(self, operands: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut args = Vec::with_capacity(operands.len() + 1);
        args.push(self.name().as_bytes().to_vec());
        args.extend(operands.iter().map(|operand| operand.to_vec()));
        args
    }
}

/// Where one pooled request was when its outer deadline fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum RequestStage {
    /// Waiting for the pooled connection's lock: no worker exists and nothing was sent.
    ConnectionLock,
    /// Lock held; waiting for a blocking-pool thread.
    BlockingQueue,
    /// Worker running: checking the pooled connection and replacing it when broken.
    ConnectionCheck,
    /// Appending command bytes; some may already have reached the server.
    Send,
    /// Every command appended; waiting for or decoding replies.
    Reply,
    /// The worker returned successfully; the caller had not yet received its result. A worker
    /// that failed keeps the stage it failed in, so a worker timeout names that stage.
    WorkerFinished,
    /// The caller received this connection's result (one owner of a multi-owner MGET).
    Completed,
}

impl RequestStage {
    pub(super) const ALL: [Self; 7] = [
        Self::ConnectionLock,
        Self::BlockingQueue,
        Self::ConnectionCheck,
        Self::Send,
        Self::Reply,
        Self::WorkerFinished,
        Self::Completed,
    ];

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionLock => "connection_lock",
            Self::BlockingQueue => "blocking_queue",
            Self::ConnectionCheck => "connection_check",
            Self::Send => "send",
            Self::Reply => "reply",
            Self::WorkerFinished => "worker_finished",
            Self::Completed => "completed",
        }
    }
}

/// Progress of one pooled request, kept only for its timeout diagnostic. The request's future
/// and its blocking worker write it; the future's timeout handler reads it.
///
/// All accesses are `Relaxed`: the values only describe the request in an error message and
/// publish no other data. The one conclusion drawn from a stage, that a mutation still in
/// `ConnectionLock` was never sent, needs no cross-thread ordering: only the request's own
/// future leaves that stage, on the same task and before it spawns the worker.
pub(super) struct RequestProgress {
    pub(super) connection: AtomicUsize,
    pub(super) stage: AtomicU8,
}

impl RequestProgress {
    const UNASSIGNED: usize = usize::MAX;

    pub(super) fn new() -> Self {
        Self {
            connection: AtomicUsize::new(Self::UNASSIGNED),
            stage: AtomicU8::new(RequestStage::ConnectionLock as u8),
        }
    }

    pub(super) fn assign(&self, connection: usize) {
        self.connection.store(connection, Ordering::Relaxed);
    }

    pub(super) fn enter(&self, stage: RequestStage) {
        self.stage.store(stage as u8, Ordering::Relaxed);
    }

    pub(super) fn stage(&self) -> RequestStage {
        // Only `new` and `enter` store, always a discriminant of `RequestStage::ALL`.
        RequestStage::ALL
            .get(usize::from(self.stage.load(Ordering::Relaxed)))
            .copied()
            .unwrap_or(RequestStage::Completed)
    }

    /// Pool slot and stage, for a timeout message. The stage is read once and also returned, so
    /// the text and any conclusion drawn from the stage agree.
    pub(super) fn describe(&self) -> (String, RequestStage) {
        let stage = self.stage();
        let text = match self.connection.load(Ordering::Relaxed) {
            Self::UNASSIGNED => format!("connection unassigned, stage {}", stage.as_str()),
            connection => format!("connection {connection}, stage {}", stage.as_str()),
        };
        (text, stage)
    }
}

/// Per-owner progress of one MGET, indexed by owner.
pub(super) type MgetProgress = [RequestProgress; 5];

pub(super) const READ_ONLY_OUTCOME: &str = "read-only command; no mutation";

/// What a timeout means for the command's effect.
pub(super) fn timeout_outcome(command: ScalarCommand, stage: RequestStage) -> &'static str {
    match (command.mutates(), stage) {
        (false, _) => READ_ONLY_OUTCOME,
        (true, RequestStage::ConnectionLock) => {
            "mutation not sent: the request never acquired its connection"
        }
        (true, _) => "mutation outcome may be unknown",
    }
}

pub(super) fn transport_name(rdma: bool) -> &'static str {
    if rdma { "RDMA" } else { "TCP" }
}

/// Owner, pool slot, stage and effect of a timed-out scalar command; never key or value bytes.
pub(super) fn scalar_timeout_detail(
    command: ScalarCommand,
    owner: usize,
    endpoint: &str,
    progress: &RequestProgress,
) -> String {
    let (location, stage) = progress.describe();
    format!(
        "owner {owner} ({endpoint}), {location}; {}",
        timeout_outcome(command, stage)
    )
}

/// Timeout of one scalar command at its outer request deadline.
pub(super) fn scalar_timeout_error(
    transport: &str,
    command: ScalarCommand,
    after: Duration,
    owner: usize,
    endpoint: &str,
    progress: &RequestProgress,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "Valkey {transport} {} timed out after {} ms: {}",
            command.name(),
            after.as_millis(),
            scalar_timeout_detail(command, owner, endpoint, progress),
        ),
    )
}

/// Context of a timeout that the blocking worker of a scalar command raised itself.
pub(super) fn scalar_worker_context(
    transport: &str,
    command: ScalarCommand,
    after: Duration,
    owner: usize,
    endpoint: &str,
    progress: &RequestProgress,
) -> String {
    format!(
        "Valkey {transport} {} worker timed out (request deadline {} ms): {}",
        command.name(),
        after.as_millis(),
        scalar_timeout_detail(command, owner, endpoint, progress),
    )
}

/// One owner's share of an MGET, for its timeout message.
pub(super) struct MgetOwner<'a> {
    pub(super) owner: usize,
    pub(super) endpoint: &'a str,
    /// Slot-grouped MGET commands pipelined to this owner.
    pub(super) commands: usize,
    pub(super) keys: usize,
    pub(super) progress: &'a RequestProgress,
}

impl MgetOwner<'_> {
    /// Owner, pool slot, stage and counts; never key bytes.
    pub(super) fn detail(&self) -> String {
        format!(
            "owner {} ({}), {}, {} commands, {} keys",
            self.owner,
            self.endpoint,
            self.progress.describe().0,
            self.commands,
            self.keys,
        )
    }
}

/// Timeout of an MGET at its outer request deadline, naming the key and command counts and,
/// per owner, its pool slot and stage but no key bytes.
pub(super) fn mget_timeout_error<'a>(
    transport: &str,
    after: Duration,
    keys: usize,
    owners: impl IntoIterator<Item = MgetOwner<'a>>,
) -> io::Error {
    let mut commands = 0;
    let mut detail = String::new();
    for owner in owners {
        commands += owner.commands;
        detail.push_str("; ");
        detail.push_str(&owner.detail());
    }
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "Valkey {transport} MGET timed out after {} ms: {keys} keys, {commands} slot commands{detail}; {}",
            after.as_millis(),
            READ_ONLY_OUTCOME,
        ),
    )
}

/// Context of a timeout that one owner's blocking worker raised itself during an MGET.
pub(super) fn mget_worker_context(
    transport: &str,
    after: Duration,
    owner: &MgetOwner<'_>,
) -> String {
    format!(
        "Valkey {transport} MGET worker timed out (request deadline {} ms): {}; {}",
        after.as_millis(),
        owner.detail(),
        READ_ONLY_OUTCOME,
    )
}

/// A `TimedOut` error raised inside the blocking worker, carrying the request context the
/// worker lacks. Display keeps the worker's own text; `source` is the worker's error.
#[derive(Debug)]
pub(super) struct WorkerTimeout {
    pub(super) context: String,
    pub(super) source: Box<dyn std::error::Error + Send + Sync>,
}

impl fmt::Display for WorkerTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}; worker error: {}", self.context, self.source)
    }
}

impl std::error::Error for WorkerTimeout {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Adds `context` to a `TimedOut` worker error, keeping that kind and the worker error as text
/// and source. Any other error is returned unchanged.
pub(super) fn with_worker_timeout_context(
    error: Box<dyn std::error::Error + Send + Sync>,
    context: impl FnOnce() -> String,
) -> Box<dyn std::error::Error + Send + Sync> {
    if !is_timed_out(error.as_ref()) {
        return error;
    }
    io::Error::new(
        io::ErrorKind::TimedOut,
        WorkerTimeout {
            context: context(),
            source: error,
        },
    )
    .into()
}

pub(super) fn measured_pipeline(
    connection: &mut Connection,
    commands: &[&[Vec<u8>]],
    deadline: Instant,
    sample: Option<&SharedSample>,
    enqueued: Option<Instant>,
    started: Option<Instant>,
    progress: Option<&RequestProgress>,
) -> BackendResult<Vec<Response>> {
    let Some(sample) = sample else {
        return connection.pipeline(commands, deadline, None, progress);
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
    let result = connection.pipeline(commands, deadline, Some(&mut phases), progress);
    drop(scope);
    update_sample(sample, |s| {
        s.command_encode_ns = phases.command_encode_ns;
        s.transport_and_parser_ns = phases.transport_and_parser_ns;
        s.reply_decode_ns = phases.reply_decode_ns;
        s.cq = cq_enabled.then_some(cq);
    });
    result
}
