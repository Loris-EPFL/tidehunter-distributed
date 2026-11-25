//! Seeded histories against real loopback owners, not an admission-core mock.
//! Every case writes through the leased live route (the only write route) over
//! the ordinary-file LiveIo fixture.
//!
//! Run `write_model_crash_soak --ignored --nocapture` with TH_MODEL_SEED and
//! TH_MODEL_ITERATIONS for the long sweep. To replay one cut, also set
//! TH_MODEL_OWNERS, TH_MODEL_HOT, TH_MODEL_LEASED and TH_MODEL_BOUNDARY. Crash
//! gates follow a seed-selected acknowledged prefix, never a count of physical
//! writes/flushes. Every failure retains its exact history, gate and boundary
//! trace.
use super::pipeline_tests::{Fixture, open_live_fixture_host, owner_limits};
use super::*;
use crate::distributed::test_bounds::CHILD_RUN_CAP;
use crate::failpoints::write_model;
use crate::key_shape::KeyType;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::sync::Barrier;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "model_check_concurrent_tests.rs"]
mod concurrent_model;
#[path = "drop_cells_crash_tests.rs"]
mod drop_cells_crash;
#[path = "model_check_leased_tests.rs"]
mod leased;
const KEYS: usize = 64;
const WAVES: usize = 4;
const CHILD: &str = "distributed::remote_db::model_check_tests::write_model_child";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Case {
    seed: u64,
    owners: usize,
    hot: bool,
    boundary: String,
    gate_waves: usize,
    leased: LeasedCase,
    #[serde(default)]
    concurrent: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct LeasedCase {
    synchronous: bool,
    per_frame_fua: bool,
    window_one: bool,
}

impl Case {
    fn new(seed: u64, owners: usize, hot: bool, leased: LeasedCase) -> Self {
        Self {
            seed,
            owners,
            hot,
            boundary: String::new(),
            gate_waves: 2 + (seed as usize & 1),
            leased,
            concurrent: false,
        }
    }
    fn value_bytes(&self) -> usize {
        4096
    }
    fn flush_probe(&self) -> bool {
        self.leased.synchronous && !self.leased.per_frame_fua && !self.leased.window_one
    }
    fn flush_cut(&self) -> bool {
        matches!(
            self.boundary.as_str(),
            "flush_submitted_to_kernel" | "leased_flush_completed"
        )
    }
    fn gate_acks(&self) -> usize {
        1 + self.gate_waves * if self.concurrent { 8 } else { 3 }
            + 2 * usize::from(self.flush_cut())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Edit {
    key: usize,
    value: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Action {
    Write { operation: usize, edits: Vec<Edit> },
    Snapshot,
}

#[derive(Debug, Serialize, Deserialize)]
enum Event {
    Begin {
        call: usize,
        action: Action,
    },
    Ack {
        call: usize,
        receipt: String,
    },
    Values {
        call: usize,
        values: Vec<Option<u64>>,
    },
}

/// One recorder owns the append-only observation file. Its cold test-only lock
/// is never held over an engine call; no shared engine state lives behind it.
struct Recorder(Mutex<(fs::File, usize)>);
impl Recorder {
    fn open(path: &Path) -> Self {
        Self(Mutex::new((
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
            0,
        )))
    }
    fn event(file: &mut fs::File, event: &Event) {
        let mut bytes = serde_json::to_vec(event).unwrap();
        bytes.push(b'\n');
        file.write_all(&bytes).unwrap();
        // A completed syscall survives process exit. This is a process-crash
        // test, not a power-cut simulator; no simulated disk durability is used.
    }
    fn begin(&self, action: Action) -> usize {
        let mut state = self.0.lock();
        let call = state.1;
        state.1 += 1;
        Self::event(&mut state.0, &Event::Begin { call, action });
        call
    }
    fn ack(&self, call: usize, receipt: String) {
        Self::event(&mut self.0.lock().0, &Event::Ack { call, receipt });
        write_model::acknowledged();
    }
    fn values(&self, call: usize, values: Vec<Option<u64>>) {
        Self::event(&mut self.0.lock().0, &Event::Values { call, values });
    }
}

#[derive(Serialize, Deserialize)]
struct Restart {
    root: PathBuf,
    shape: KeyShape,
    config: RemoteDatabaseConfig,
    database: [u8; 16],
    owners: Vec<(super::super::tcp::OwnerHostConfig, TlsFiles)>,
    operations: Vec<[u8; 16]>,
}

impl Restart {
    fn capture(fixture: &Fixture, operations: Vec<[u8; 16]>) -> Self {
        Self {
            root: fixture.temp.path().to_path_buf(),
            shape: fixture.shape.clone(),
            config: fixture.config.clone(),
            database: fixture.config.storage.database,
            owners: fixture
                .host_configs
                .iter()
                .zip(&fixture.startups)
                .map(|(host, (_, tls, _))| (host.clone(), tls.clone()))
                .collect(),
            operations,
        }
    }

    fn serve_owners(
        &self,
        shutdown: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Vec<JoinHandle<Result<(), super::super::tcp::TcpError>>> {
        self.owners
            .iter()
            .zip(&self.config.endpoints)
            .map(|((config, tls), endpoint)| {
                let host = open_live_fixture_host(config.clone()).unwrap();
                assert!(host.database(&self.database).is_some());
                let listener = std::net::TcpListener::bind(&endpoint.address).unwrap();
                let tls = tls.clone();
                let stop = Arc::clone(shutdown);
                thread::spawn(move || host.serve(listener, tls, owner_limits(), stop))
            })
            .collect()
    }
}

/// The owner tier of a case is the tier of its leased profile.
fn synchronous(case: &Case) -> bool {
    case.leased.synchronous
}

/// Version B tracks Native owners with WAL replay under the default config.
fn durable_prefix(case: &Case) -> bool {
    !synchronous(case)
}

fn key_bytes(key: usize) -> [u8; 4] {
    // Spread the small domain across native cells and index shards.
    ((key as u32) << 26).to_be_bytes()
}

fn mutations(edits: &[Edit], value_bytes: usize) -> Vec<Mutation> {
    edits
        .iter()
        .map(|edit| {
            let key = key_bytes(edit.key).to_vec().into();
            match edit.value {
                Some(value) => Mutation::Put {
                    keyspace: KeySpace(0),
                    key,
                    value: value.to_le_bytes().repeat(value_bytes / 8).into(),
                },
                None => Mutation::Delete {
                    keyspace: KeySpace(0),
                    key,
                },
            }
        })
        .collect()
}

fn workload(case: &Case) -> Vec<Vec<Edit>> {
    let mut rng = StdRng::seed_from_u64(case.seed);
    let weights: Vec<_> = (1..=KEYS).map(|rank| (rank as f64).powf(-0.99)).collect();
    let total: f64 = weights.iter().sum();
    (0..WAVES * 2)
        .map(|operation| {
            let size = if case.concurrent {
                [1, 8, 32][operation % 3]
            } else {
                [1, 2, 22, 57][operation % 4]
            };
            let mut keys = Vec::with_capacity(size);
            // Sampling without replacement preserves the requested batch size,
            // while small batches retain the selected key distribution.
            for _ in 0..KEYS * 1024 {
                if keys.len() == size {
                    break;
                }
                let key = if case.hot {
                    let draw = rng.gen_range(0.0..total);
                    let mut sum = 0.0;
                    weights
                        .iter()
                        .position(|weight| {
                            sum += weight;
                            draw < sum
                        })
                        .unwrap()
                } else {
                    rng.gen_range(0..KEYS)
                };
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
            assert_eq!(
                keys.len(),
                size,
                "seed={} sampling budget exhausted",
                case.seed
            );
            keys.into_iter()
                .enumerate()
                .map(|(member, key)| Edit {
                    key,
                    value: if operation == 0 || (operation != 4 && rng.gen_ratio(3, 4)) {
                        Some(((operation as u64 + 1) << 32) | member as u64)
                    } else {
                        None
                    },
                })
                .collect()
        })
        .collect()
}

/// Capture one protected cut, then compare all point reads AND the entire index
/// walk to it. Independent scalar latest reads cannot prove batch atomicity.
fn snapshot(db: &ConcurrentRemoteDatabase) -> Vec<Option<u64>> {
    let table = db.hybrid_table();
    let (cut, _pin) = table.pin_checkpoint();
    let page: Vec<_> = pipeline_tests::index_entries(table, KeySpace(0), cut)
        .into_iter()
        .filter(|(_, position)| position.is_valid())
        .collect();
    let mut values = Vec::with_capacity(KEYS);
    for key in 0..KEYS {
        let bytes = key_bytes(key);
        let indexed = table.get(KeySpace(0), &bytes, cut).unwrap();
        let value = pipeline_tests::read_at_cut(db, KeySpace(0), &bytes, cut).unwrap();
        assert_eq!(indexed.is_some(), value.is_some(), "index/read key={key}");
        assert_eq!(
            page.iter()
                .filter(|(entry, _)| entry.as_ref() == bytes)
                .count(),
            usize::from(indexed.is_some()),
            "index page key={key}"
        );
        values.push(value.map(|value| {
            let token: [u8; 8] = value[..8].try_into().unwrap();
            assert_eq!(value.len() % 8, 0);
            assert!(value.chunks_exact(8).all(|chunk| chunk == token));
            u64::from_le_bytes(token)
        }));
    }
    assert_eq!(
        page.len(),
        values.iter().flatten().count(),
        "unexpected visible index key"
    );
    values
}

fn observe(db: &ConcurrentRemoteDatabase, recorder: &Recorder) {
    let call = recorder.begin(Action::Snapshot);
    recorder.values(call, snapshot(db));
}

fn write_call(
    db: &ConcurrentRemoteDatabase,
    recorder: &Recorder,
    case: &Case,
    operations: &[[u8; 16]],
    plan: &[Vec<Edit>],
    id: usize,
) {
    let call = recorder.begin(Action::Write {
        operation: id,
        edits: plan[id].clone(),
    });
    let receipt = db
        .write_batch(operations[id], mutations(&plan[id], case.value_bytes()))
        .unwrap();
    recorder.ack(call, format!("{receipt:?}"));
}

fn write_child(directory: &Path, case: &Case) {
    let temp = tempdir::TempDir::new_in(directory, "cluster").unwrap();
    let fixture = Fixture::new_with_root(
        temp,
        case.owners,
        128,
        KeyShape::new_single(4, 8, KeyType::uniform(8)),
        2,
        |config| {
            config.storage.batch_limits.max_operations = 57;
            config.read_cache_bytes = 0;
            leased::configure(config, case.leased);
        },
        |_| {},
    );
    let fixture = if synchronous(case) {
        fixture.synchronous()
    } else {
        fixture
    };
    let mut plan = workload(case);
    for size in [57, 22, 16, 16] {
        let operation = plan.len();
        plan.push(
            (0..size)
                .map(|key| Edit {
                    key,
                    value: Some(((operation as u64 + 1) << 32) | key as u64),
                })
                .collect(),
        );
    }
    let baseline = plan.len();
    plan.push(vec![Edit {
        key: KEYS - 1,
        value: Some(u64::MAX),
    }]);
    let operations: Vec<_> = (0..plan.len())
        .map(|id| {
            let owner = if case.concurrent && id < WAVES * 2 {
                (case.seed as usize + id) % case.owners
            } else if id < WAVES * 2 {
                id % case.owners
            } else {
                3 % case.owners
            };
            fixture.operation(owner as u32, id as u64 + 1)
        })
        .collect();
    let restart = Restart::capture(&fixture, operations.clone());
    fs::write(
        directory.join("restart.json"),
        serde_json::to_vec(&restart).unwrap(),
    )
    .unwrap();
    let original = fixture.open();
    let clients: Vec<_> = original
        .core
        .clients
        .iter()
        .map(|worker| worker.client.clone())
        .collect();
    let live_metrics = original.core.live_metrics.clone();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let recorder = Recorder::open(&directory.join("history.jsonl"));
    write_call(&db, &recorder, case, &operations, &plan, baseline);
    db.force_rebuild_index().unwrap();
    write_model::arm(
        &directory.join("boundaries.txt"),
        case.boundary.clone(),
        case.gate_acks(),
    );
    for wave in 0..case.gate_waves {
        if case.concurrent {
            concurrent_model::wave(&db, &recorder, case, &operations, &plan, wave);
        } else {
            let mut rng = StdRng::seed_from_u64(case.seed ^ wave as u64);
            let retry = if wave == 0 {
                0
            } else {
                rng.gen_range(0..wave * 2)
            };
            let mut ids = [wave * 2, wave * 2 + 1, retry];
            for index in (1..ids.len()).rev() {
                ids.swap(index, rng.gen_range(0..=index));
            }
            let barrier = Barrier::new(4);
            thread::scope(|scope| {
                for id in ids {
                    let (db, recorder, operations, plan, barrier) =
                        (&db, &recorder, &operations, &plan, &barrier);
                    scope.spawn(move || {
                        let call = recorder.begin(Action::Write {
                            operation: id,
                            edits: plan[id].clone(),
                        });
                        barrier.wait();
                        let receipt = db
                            .write_batch(operations[id], mutations(&plan[id], case.value_bytes()))
                            .unwrap();
                        recorder.ack(call, format!("{receipt:?}"));
                    });
                }
                barrier.wait();
                observe(&db, &recorder);
            });
        }
        observe(&db, &recorder);
        // Deliberately NO checkpoint here: the gate must have acknowledged large
        // batches recoverable only from the WAL tail behind the baseline image.
    }
    db.flush_durable().unwrap();
    if case.flush_cut() {
        // A 57-key frame leaves less than a 22-key frame in this extent.
        // The 22-key frame switches extent; two 16-key frames then fit beside
        // it and exceed the unchanged 65536-byte GroupFlush threshold.
        for id in WAVES * 2..WAVES * 2 + 2 {
            write_call(&db, &recorder, case, &operations, &plan, id);
        }
        db.flush_durable().unwrap();
    }
    leased::quiesce(&clients);
    write_model::open_gate();
    if !case.flush_cut() {
        for id in WAVES * 2..WAVES * 2 + 2 {
            write_call(&db, &recorder, case, &operations, &plan, id);
        }
    }
    if case.flush_probe() {
        leased::flush_probe(
            &db,
            &recorder,
            case,
            &operations,
            &plan,
            &clients[3 % case.owners],
        );
    }
    if !case.flush_probe() {
        for id in WAVES * 2 + 2..WAVES * 2 + 4 {
            write_call(&db, &recorder, case, &operations, &plan, id);
        }
    }
    db.flush_durable().unwrap();
    db.force_rebuild_index().unwrap();
    observe(&db, &recorder);
    leased::verify_counts(case.leased, &live_metrics.unwrap(), directory);
    std::process::exit(0);
}

fn recover_child(directory: &Path) {
    let restart: Restart =
        serde_json::from_slice(&fs::read(directory.join("restart.json")).unwrap()).unwrap();
    let case: Case =
        serde_json::from_slice(&fs::read(directory.join("case.json")).unwrap()).unwrap();
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let servers = restart.serve_owners(&shutdown);
    // Recovery and every later read of the recovered files go through the
    // ordinary-file live fixture.
    let db = RemoteDatabase::open_local_live_fixture(
        &restart.root.join("authority"),
        &restart.shape,
        restart.config.clone(),
    )
    .unwrap();
    assert_eq!(db.database_id(), restart.database);
    // Durable-prefix recovery must run exactly for Native owners with WAL
    // replay, and may discard only batches that no caller saw acknowledged
    // (checked against the history by the parent).
    let recovery = db.recovery_stats();
    assert_eq!(
        recovery.recovered_cut.is_some(),
        durable_prefix(&case),
        "durable-prefix recovery selection {case:?}"
    );
    // Owner continuity: when every owner process kept running without a failure
    // and recovered without a hole, a missing version was never written, so
    // Extend discards nothing.
    if recovery.cut_continuity_bound == Some(u64::MAX) {
        assert_eq!(
            recovery.cut_discarded_batches, 0,
            "continuous owners lost no batch {case:?}"
        );
    }
    fs::write(
        directory.join("cut-discarded.json"),
        serde_json::to_vec(&recovery.cut_discarded_batches).unwrap(),
    )
    .unwrap();
    let discard = recovery.recovered_cut.map(|cut| Discard {
        epoch: cut.epoch,
        next: cut.next,
        bound: recovery.cut_continuity_bound,
    });
    fs::write(
        directory.join("recovered-cut.json"),
        serde_json::to_vec(&discard).unwrap(),
    )
    .unwrap();
    let epoch = db.epoch();
    let db = ConcurrentRemoteDatabase::new(db).unwrap();
    let values = snapshot(&db);
    // OWNER decision (BOARD 2026-11-20, decision 2): the retry window is empty
    // after a restart, so a retry after the reopen is a new write. Retry every
    // externally acknowledged identity after a real owner restart: each retry
    // takes a version in the new epoch (never the original receipt) and
    // re-applies its edits, so the expected state is the recovered state with
    // the retried edits applied in retry order. A second retry of the same
    // identity inside this epoch returns the first retry's receipt and applies
    // nothing. Pending identities are deliberately not retried before the
    // oracle. Neither are acknowledged identities the durable cut discarded:
    // recovery removed them, so a retry would be a new write (the parent
    // checks that the cut allowed each discard).
    let calls = history(directory, values.clone());
    let mut retried = HashSet::new();
    let mut expected = values.clone();
    for call in &calls {
        if let Action::Write { operation, edits } = &call.action
            && let Some(receipt) = &call.receipt
            && !discard.is_some_and(|discard| discard.covers(receipt))
            && retried.insert(*operation)
        {
            let new_receipt = db
                .write_batch(
                    restart.operations[*operation],
                    mutations(edits, case.value_bytes()),
                )
                .unwrap();
            assert_eq!(
                new_receipt.epoch, epoch,
                "a retry after recovery is a new write in the new epoch operation={operation}"
            );
            assert_ne!(
                &format!("{new_receipt:?}"),
                receipt,
                "retry returned the pre-restart receipt operation={operation}"
            );
            for edit in edits {
                expected[edit.key] = edit.value;
            }
            let again = db
                .write_batch(
                    restart.operations[*operation],
                    mutations(edits, case.value_bytes()),
                )
                .unwrap();
            assert_eq!(
                again, new_receipt,
                "in-epoch retry changed receipt operation={operation}"
            );
        }
    }
    assert_eq!(
        snapshot(&db),
        expected,
        "a retry after recovery must apply exactly its edits"
    );
    fs::write(
        directory.join("recovered.json"),
        serde_json::to_vec(&values).unwrap(),
    )
    .unwrap();
    db.shutdown().unwrap();
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    for server in servers {
        server.join().unwrap().unwrap();
    }
}

#[test]
#[ignore = "subprocess entry point; invoked by write_model_seeded_crashes"]
fn write_model_child() {
    let directory = PathBuf::from(
        std::env::var_os("TH_MODEL_CHILD_ROOT").expect("only run through parent test"),
    );
    let case: Case =
        serde_json::from_slice(&fs::read(directory.join("case.json")).unwrap()).unwrap();
    match std::env::var("TH_MODEL_CHILD_STAGE").unwrap().as_str() {
        "write" => write_child(&directory, &case),
        "recover" => recover_child(&directory),
        other => panic!("unknown model stage {other}"),
    }
}

/// The bound of a durable-prefix recovery: it discarded every batch of `epoch`
/// at or above `next`. `bound` is its continuity bound (Extend only).
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
struct Discard {
    epoch: u64,
    next: u64,
    bound: Option<u64>,
}

impl Discard {
    /// Whether recovery discarded the batch acknowledged with `receipt`.
    fn covers(&self, receipt: &str) -> bool {
        let version = receipt_version(receipt);
        version.epoch == self.epoch && version.sequence >= self.next
    }
}

/// Receipts are recorded as `BatchVersion` debug text.
fn receipt_version(receipt: &str) -> BatchVersion {
    let mut numbers = receipt
        .split(|c: char| !c.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .map(|part| part.parse().unwrap());
    let version = BatchVersion {
        epoch: numbers.next().unwrap(),
        sequence: numbers.next().unwrap(),
    };
    assert_eq!(format!("{version:?}"), receipt, "receipt is not a version");
    version
}

#[derive(Clone, Debug)]
struct Call {
    action: Action,
    begin: usize,
    end: Option<usize>,
    values: Option<Vec<Option<u64>>>,
    receipt: Option<String>,
}

fn history(directory: &Path, recovered: Vec<Option<u64>>) -> Vec<Call> {
    let bytes = fs::read(directory.join("history.jsonl")).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    let mut calls = Vec::<Call>::new();
    let mut receipts = HashMap::new();
    let mut clock = 0;
    for line in text.split_inclusive('\n') {
        // exit can cut an in-progress recorder syscall. Only a complete external
        // observation counts as received; a partial last record is not an ack.
        if !line.ends_with('\n') {
            break;
        }
        let event: Event = serde_json::from_str(line).unwrap();
        match event {
            Event::Begin { call, action } => {
                assert_eq!(call, calls.len());
                calls.push(Call {
                    action,
                    begin: clock,
                    end: None,
                    values: None,
                    receipt: None,
                });
            }
            Event::Ack { call, receipt } => {
                let Action::Write { operation, .. } = calls[call].action else {
                    panic!("snapshot ack")
                };
                if let Some(previous) = receipts.insert(operation, receipt.clone()) {
                    assert_eq!(
                        previous, receipt,
                        "retry changed receipt operation={operation}"
                    );
                }
                assert!(calls[call].end.replace(clock).is_none());
                calls[call].receipt = Some(receipt);
            }
            Event::Values { call, values } => {
                assert!(matches!(calls[call].action, Action::Snapshot));
                assert!(calls[call].end.replace(clock).is_none());
                calls[call].values = Some(values);
            }
        }
        clock += 1;
    }
    calls.push(Call {
        action: Action::Snapshot,
        begin: clock,
        end: Some(clock + 1),
        values: Some(recovered),
        receipt: None,
    });
    assert!(
        calls.len() < 32,
        "bounded history no longer fits the search mask"
    );
    calls
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Model {
    values: Vec<Option<u64>>,
    applied: u16,
}

/// Independent, bounded linearizability search. No engine version, physical WAL
/// address, receipt order, or recovery implementation supplies the oracle.
/// Completed calls must occur once; a pending write may occur once or not at
/// all. Retry identity is idempotent. Each batch transitions all keys at once.
fn linearizable(calls: &[Call]) -> bool {
    fn search(
        calls: &[Call],
        required: u32,
        done: u32,
        model: Model,
        visited: &mut HashSet<(u32, Model)>,
    ) -> bool {
        if done & required == required {
            return true;
        }
        if !visited.insert((done, model.clone())) {
            return false;
        }
        assert!(
            visited.len() <= 100_000,
            "linearizability search budget exhausted"
        );
        for (index, call) in calls.iter().enumerate() {
            let bit = 1 << index;
            if done & bit != 0 {
                continue;
            }
            // A response preceding this invocation imposes real-time order.
            if calls.iter().enumerate().any(|(prior, other)| {
                done & (1 << prior) == 0 && other.end.is_some_and(|end| end < call.begin)
            }) {
                continue;
            }
            let mut next = model.clone();
            match &call.action {
                Action::Write { operation, edits } => {
                    let identity = 1 << operation;
                    if next.applied & identity == 0 {
                        for edit in edits {
                            next.values[edit.key] = edit.value;
                        }
                        next.applied |= identity;
                    }
                }
                Action::Snapshot => {
                    // Incomplete reads impose no observation and can be dropped.
                    let Some(values) = &call.values else { continue };
                    if values != &next.values {
                        continue;
                    }
                }
            }
            if search(calls, required, done | bit, next, visited) {
                return true;
            }
        }
        false
    }
    let required = calls.iter().enumerate().fold(0, |mask, (index, call)| {
        mask | if call.end.is_some() { 1 << index } else { 0 }
    });
    search(
        calls,
        required,
        0,
        Model {
            values: vec![None; KEYS],
            applied: 0,
        },
        &mut HashSet::new(),
    )
}

fn child(directory: &Path, stage: &str) -> Output {
    // File-backed stdout avoids a pipe filling while the parent waits. A fixed
    // deadline bounds child hangs; every spawned process is waited or killed.
    let stdout = fs::File::create(directory.join(format!("{stage}.stdout"))).unwrap();
    let stderr = fs::File::create(directory.join(format!("{stage}.stderr"))).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            CHILD,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TH_MODEL_CHILD_ROOT", directory)
        .env("TH_MODEL_CHILD_STAGE", stage)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    // Whole test-binary child run; try_wait is the progress signal below.
    let deadline = Instant::now() + CHILD_RUN_CAP;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "model child timed out stage={stage} artifacts={}",
                directory.display()
            );
        }
        thread::sleep(Duration::from_millis(10));
    };
    Output {
        status,
        stdout: fs::read(directory.join(format!("{stage}.stdout"))).unwrap(),
        stderr: fs::read(directory.join(format!("{stage}.stderr"))).unwrap(),
    }
}

/// Root of the retained write-model crash artifacts for this process. A unique
/// path per process keeps concurrent test processes from deleting each other's
/// trees; every failing case keeps its directory and the eprintln names it.
fn artifacts_root() -> &'static std::path::Path {
    static ROOT: std::sync::LazyLock<std::path::PathBuf> = std::sync::LazyLock::new(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let root = std::env::temp_dir().join(format!(
            "tidehunter-write-model-artifacts-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create the write-model artifact root");
        root
    });
    &ROOT
}

fn run_case(case: &Case) {
    let temp = tempdir::TempDir::new_in(artifacts_root(), "case").unwrap();
    // Retain on *any* failure (including a checker assertion), delete on success.
    let directory = temp.into_path();
    fs::write(
        directory.join("case.json"),
        serde_json::to_vec(case).unwrap(),
    )
    .unwrap();
    eprintln!("write model {case:?} artifacts={}", directory.display());
    let write = child(&directory, "write");
    let expected = if case.boundary.is_empty() {
        0
    } else {
        write_model::CRASH_EXIT
    };
    assert_eq!(
        write.status.code(),
        Some(expected),
        "write stage {case:?} artifacts={}\n{}\n{}",
        directory.display(),
        String::from_utf8_lossy(&write.stdout),
        String::from_utf8_lossy(&write.stderr)
    );
    let trace: Vec<String> = fs::read_to_string(directory.join("boundaries.txt"))
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(
        trace.first(),
        Some(&format!("gate_after_acks={}", case.gate_acks()))
    );
    if !case.boundary.is_empty() {
        assert_eq!(trace.last(), Some(&case.boundary), "wrong crash cut");
        assert_eq!(
            trace.iter().filter(|name| **name == case.boundary).count(),
            1
        );
    }
    let recover = child(&directory, "recover");
    assert!(
        recover.status.success(),
        "recovery stage {case:?} artifacts={}\n{}\n{}",
        directory.display(),
        String::from_utf8_lossy(&recover.stdout),
        String::from_utf8_lossy(&recover.stderr)
    );
    let values: Vec<Option<u64>> =
        serde_json::from_slice(&fs::read(directory.join("recovered.json")).unwrap()).unwrap();
    assert!(values.iter().any(Option::is_some), "vacuous empty recovery");
    let mut calls = history(&directory, values);
    let acknowledged: Vec<_> = calls
        .iter()
        .filter(|call| call.end.is_some() && matches!(call.action, Action::Write { .. }))
        .collect();
    assert!(
        acknowledged.len() >= case.gate_acks(),
        "gate has no acknowledged WAL tail"
    );
    // Prefix oracle. A discarded tail batch is pending, or acknowledged and at or
    // above the continuity bound of an owner that restarted or failed: only
    // there can a missing version hide a lost batch, so the run stops and the
    // acknowledged batches after it go with the discarded suffix (Native
    // acknowledges before the sync). No acknowledged batch below the bound may
    // be discarded, and none at all when every owner was continuous.
    let discarded: u64 =
        serde_json::from_slice(&fs::read(directory.join("cut-discarded.json")).unwrap()).unwrap();
    let discard: Option<Discard> =
        serde_json::from_slice(&fs::read(directory.join("recovered-cut.json")).unwrap()).unwrap();
    let pending = calls
        .iter()
        .filter(|call| call.end.is_none() && matches!(call.action, Action::Write { .. }))
        .count() as u64;
    let lost: HashSet<usize> = calls
        .iter()
        .enumerate()
        .filter(|(_, call)| {
            call.receipt
                .as_ref()
                .is_some_and(|receipt| discard.is_some_and(|discard| discard.covers(receipt)))
        })
        .map(|(index, _)| index)
        .collect();
    for &index in &lost {
        let sequence = receipt_version(calls[index].receipt.as_ref().unwrap()).sequence;
        assert!(
            discard
                .and_then(|discard| discard.bound)
                .is_some_and(|bound| bound != u64::MAX && sequence >= bound),
            "durable cut discarded acknowledged sequence {sequence} below its bound \
             {discard:?} {case:?} artifacts={}",
            directory.display()
        );
    }
    assert!(
        discarded <= pending + lost.len() as u64,
        "durable cut discarded {discarded} batches but only {pending} writes were pending \
         and {} acknowledged ones were allowed {case:?} artifacts={}",
        lost.len(),
        directory.display()
    );
    let sizes: &[usize] = if case.concurrent {
        &[1, 8, 32]
    } else {
        &[22, 57]
    };
    for size in sizes {
        assert!(
            acknowledged.iter().any(
                |call| matches!(&call.action, Action::Write { edits, .. } if edits.len() == *size)
            ),
            "no acknowledged {size}-key batch before the crash"
        );
    }
    // Linearizability without exactly the discarded acknowledged writes: each
    // must be absent (no other call explains its values), every other
    // acknowledged write must be present.
    let mut index = 0;
    calls.retain(|_| {
        index += 1;
        !lost.contains(&(index - 1))
    });
    assert!(
        linearizable(&calls),
        "non-linearizable history {case:?} artifacts={}\n{calls:#?}",
        directory.display()
    );
    fs::remove_dir_all(directory).unwrap();
}

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

#[test]
#[ignore = "long seeded crash sweep; TH_MODEL_SEED=23063 TH_MODEL_ITERATIONS=32"]
fn write_model_crash_soak() {
    let seed = env_number("TH_MODEL_SEED", 0x5a17);
    let iterations = env_number("TH_MODEL_ITERATIONS", 32);
    assert!((1..=10_000).contains(&iterations));
    for iteration in 0..iterations {
        // Unfiltered, iterations rotate through all eight leased profiles.
        let profile = env_number("TH_MODEL_LEASED", iteration % 8);
        assert!(profile < 8, "TH_MODEL_LEASED must be 0..8");
        let mut case = Case::new(
            seed.wrapping_add(iteration),
            env_number("TH_MODEL_OWNERS", [1, 2, 4][iteration as usize % 3]) as usize,
            env_number("TH_MODEL_HOT", iteration % 2) != 0,
            leased::profile(profile as usize),
        );
        case.gate_waves = env_number("TH_MODEL_GATE_WAVES", case.gate_waves as u64) as usize;
        assert!([2, 3].contains(&case.gate_waves));
        assert!([1, 2, 4].contains(&case.owners));
        let boundaries = leased::boundaries(&case);
        if let Ok(boundary) = std::env::var("TH_MODEL_BOUNDARY") {
            assert!(boundaries.contains(&boundary.as_str()));
            run_case(&Case { boundary, ..case });
        } else {
            run_case(&case);
            for boundary in boundaries.iter().copied() {
                run_case(&Case {
                    boundary: boundary.into(),
                    ..case.clone()
                });
            }
        }
    }
}

#[test]
fn write_model_oracle_rejects_lost_ack_partial_batch_and_reordered_real_time() {
    let edits = vec![
        Edit {
            key: 0,
            value: Some(1),
        },
        Edit {
            key: 1,
            value: Some(1),
        },
    ];
    let write = Call {
        action: Action::Write {
            operation: 0,
            edits,
        },
        begin: 0,
        end: Some(1),
        values: None,
        receipt: None,
    };
    let mut values = vec![None; KEYS];
    let read = |values| Call {
        action: Action::Snapshot,
        begin: 2,
        end: Some(3),
        values: Some(values),
        receipt: None,
    };
    assert!(
        !linearizable(&[write.clone(), read(values.clone())]),
        "lost acknowledged write"
    );
    values[0] = Some(1);
    assert!(
        !linearizable(&[
            Call {
                end: None,
                ..write.clone()
            },
            read(values.clone())
        ]),
        "unacknowledged partial batch"
    );
    values[1] = Some(1);
    assert!(linearizable(&[write.clone(), read(values.clone())]));
    assert!(
        linearizable(&[
            Call {
                end: None,
                ..write.clone()
            },
            read(values)
        ]),
        "uncertain full batch may survive"
    );
    let newer = Call {
        action: Action::Write {
            operation: 1,
            edits: vec![Edit {
                key: 0,
                value: Some(2),
            }],
        },
        begin: 2,
        end: Some(3),
        values: None,
        receipt: None,
    };
    let mut stale = vec![None; KEYS];
    stale[0] = Some(1);
    stale[1] = Some(1);
    assert!(!linearizable(&[
        write,
        newer,
        Call {
            begin: 4,
            end: Some(5),
            ..read(stale)
        }
    ]));
}
