//! Seeded histories against real loopback owners, not an admission-core mock.
//!
//! Run `write_model_crash_soak --ignored --nocapture` with TH_MODEL_SEED and
//! TH_MODEL_ITERATIONS for the long sweep. To replay one cut, also set
//! TH_MODEL_OWNERS, TH_MODEL_HOT and TH_MODEL_BOUNDARY. Crash gates follow a
//! seed-selected acknowledged prefix, never a count of physical writes/flushes.
//! Every failure retains its exact history, gate and boundary trace.
use super::pipeline_tests::Fixture;
use super::tests::recovery_metrics;
use super::*;
use crate::failpoints::write_model;
use crate::key_shape::KeyType;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashSet;
use std::io::Write;
use std::process::{Command, Output, Stdio};
use std::sync::Barrier;

#[path = "model_check_concurrent.rs"]
mod concurrent_model;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[path = "model_check_leased.rs"]
mod leased;
const KEYS: usize = 64;
const WAVES: usize = 4;
const CHILD: &str = "distributed::remote_db::model_check_tests::write_model_child";
const BOUNDARIES: &[&str] = &[
    "append_before",
    "append_after",
    "file_sync_before",
    "file_sync_after",
    "namespace_sync_before",
    "namespace_sync_after",
    "publication_before",
    "publication_after",
    "checkpoint_staging_before",
    "checkpoint_staging_after",
    "checkpoint_image_before",
    "checkpoint_image_after",
    "checkpoint_metadata_before",
    "checkpoint_metadata_after",
    "checkpoint_root_before",
    "checkpoint_root_after",
    "checkpoint_install_before",
    "checkpoint_install_after",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Case {
    seed: u64,
    owners: usize,
    hot: bool,
    boundary: String,
    gate_waves: usize,
    on_disk_history: bool,
    leased: Option<LeasedCase>,
    #[serde(default)]
    concurrent: bool,
    #[serde(default)]
    native: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct LeasedCase {
    synchronous: bool,
    per_frame_fua: bool,
    window_one: bool,
}

impl Case {
    fn rpc(seed: u64, owners: usize, hot: bool) -> Self {
        Self {
            seed,
            owners,
            hot,
            boundary: String::new(),
            gate_waves: 2 + (seed as usize & 1),
            on_disk_history: false,
            leased: None,
            concurrent: false,
            native: false,
        }
    }
    fn value_bytes(&self) -> usize {
        if self.leased.is_some() { 4096 } else { 8 }
    }
    fn flush_probe(&self) -> bool {
        self.leased.is_some_and(|profile| {
            profile.synchronous && !profile.per_frame_fua && !profile.window_one
        })
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
    nodes: Vec<(super::super::tcp::NodeConfig, TlsFiles)>,
    operations: Vec<[u8; 16]>,
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type PassiveReadProbe = (Arc<super::super::tcp::StorageNode>, Arc<Mutex<Client>>);

/// Product reads always use the captured cut. Separate owner-RPC readers exist
/// only for explicit read-route comparison or passive failure diagnosis.
struct Readers {
    clients: Option<BTreeMap<u32, Mutex<Client>>>,
    epoch: u64,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    probes: Option<Vec<PassiveReadProbe>>,
}

impl Readers {
    fn new(config: &RemoteDatabaseConfig, shape: &KeyShape, epoch: u64) -> Self {
        let clients = config
            .live_nvme
            .as_ref()
            .filter(|_| {
                std::env::var_os("TH_MODEL_COMPARE_READS").is_some()
                    || std::env::var_os("TH_MODEL_PASSIVE_READS").is_some()
            })
            .map(|_| {
                config
                    .endpoints
                    .iter()
                    .map(|endpoint| {
                        let mut tls = config.tls.clone();
                        tls.server_name = endpoint.server_name.clone();
                        (
                            endpoint.partition,
                            Mutex::new(
                                Client::connect(
                                    &endpoint.address,
                                    &tls,
                                    config.timeout,
                                    config.partition_config(1, endpoint.partition),
                                    shape,
                                )
                                .unwrap(),
                            ),
                        )
                    })
                    .collect()
            });
        Self {
            clients,
            epoch,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            probes: None,
        }
    }
}

/// Capture one protected cut, then compare all point reads AND the entire index
/// page to it. Independent scalar latest reads cannot prove batch atomicity.
fn snapshot(db: &ConcurrentRemoteDatabase, readers: &Readers) -> Vec<Option<u64>> {
    let (cut, _pin) = db.protected_published_cut().unwrap();
    let page = db
        .index_page(KeySpace(0), None, false, cut.clone())
        .unwrap();
    let mut values = Vec::with_capacity(KEYS);
    for key in 0..KEYS {
        let bytes = key_bytes(key);
        let reference = db
            .index_reference(KeySpace(0), &bytes, cut.clone())
            .unwrap();
        let value = if let Some(clients) = &readers.clients {
            reference.map(|reference| {
                let product = std::env::var_os("TH_MODEL_COMPARE_READS").map(|_| {
                    db.submit_get_cut(KeySpace(0), &bytes, cut.clone())
                        .and_then(|pending| db.finish_ready_get(pending.wait()))
                });
                let rpc = clients
                    .get(&reference.batch_address.partition)
                    .unwrap()
                    .lock()
                    .read_record(readers.epoch, &reference);
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                if rpc.is_err()
                    && let Some(probes) = &readers.probes
                {
                    let (node, writer) = &probes[reference.batch_address.partition as usize];
                    let log = node.log_for_test(reference.batch_version.epoch);
                    let appended = log.appended_frontier();
                    let synced = log.synced_frontier();
                    let root = log.position_root_for_test();
                    let end = reference.batch_address.offset + u64::from(reference.batch_address.len);
                    let writer = writer.lock();
                    eprintln!(
                        "passive_read key={key} cut={cut:?} reference={reference:?} appended={appended} synced={synced} root={root:?} beyond_appended={} beyond_synced={} beyond_root={} completed_released_durable={:?} current_written_pending={:?} closing_next={:?} flights={} rpc={rpc:?}",
                        end > appended,
                        end > synced,
                        end > root.2,
                        writer.leased_test_completion_prefixes(),
                        writer.leased_test_frontier(),
                        writer.leased_test_ahead_state(),
                        writer.leased_flights_in_flight(),
                    );
                }
                if let Some(product) = product {
                    eprintln!(
                        "compare_read key={key} cut={cut:?} reference={reference:?} product={:?} rpc={:?}",
                        product
                            .as_ref()
                            .map(|value| value.as_ref().map(|value| (&value[..8], value.len()))),
                        rpc.as_ref().map(|mutation| match mutation {
                            Mutation::Put { value, .. } => Some((&value[..8], value.len())),
                            Mutation::Delete { .. } => None,
                        }),
                    );
                }
                match rpc.unwrap() {
                    Mutation::Put {
                        keyspace,
                        key,
                        value,
                    } => {
                        assert_eq!(keyspace, KeySpace(0));
                        assert_eq!(key.as_ref(), bytes);
                        value
                    }
                    Mutation::Delete { .. } => panic!("visible reference points to delete"),
                }
            })
        } else {
            let pending = db.submit_get_cut(KeySpace(0), &bytes, cut.clone()).unwrap();
            db.finish_ready_get(pending.wait()).unwrap()
        };
        assert_eq!(reference.is_some(), value.is_some(), "index/read key={key}");
        assert_eq!(
            page.iter()
                .filter(|(indexed, _)| indexed.as_ref() == bytes)
                .count(),
            usize::from(reference.is_some()),
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

fn observe(db: &ConcurrentRemoteDatabase, readers: &Readers, recorder: &Recorder) {
    let call = recorder.begin(Action::Snapshot);
    recorder.values(call, snapshot(db, readers));
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
            // Small RPC files force a namespace boundary after the gate.
            config.storage.batch_limits.max_encoded_bytes = 1900;
            config.storage.group_limits.max_encoded_bytes = 2040;
            config.storage.fragment_bytes = 2048;
            config.storage.wal_file_bytes = 2048;
            config.max_index_bytes = 16 * 1024 * 1024;
            config.read_cache_bytes = 0;
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if let Some(profile) = case.leased {
                leased::configure(config, profile);
            }
        },
        |config| {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if case.leased.is_some() {
                return leased::open_node(config);
            }
            super::super::tcp::StorageNode::open(config, recovery_metrics()).unwrap()
        },
    )
    .history(if case.on_disk_history {
        HistoryPersistence::OnDisk
    } else {
        HistoryPersistence::WalReplay
    });
    let synchronous = !case.native && case.leased.is_none_or(|profile| profile.synchronous);
    let fixture = if synchronous {
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
    let restart = Restart {
        root: fixture.temp.path().to_path_buf(),
        shape: fixture.shape.clone(),
        config: fixture.config.clone(),
        operations: operations.clone(),
        nodes: fixture
            .startups
            .iter()
            .map(|(config, tls, _)| (config.clone(), tls.clone()))
            .collect(),
    };
    fs::write(
        directory.join("restart.json"),
        serde_json::to_vec(&restart).unwrap(),
    )
    .unwrap();
    let original = fixture.open();
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let original = {
        let mut original = original;
        if case.leased.is_some() {
            leased::prepare_database(&mut original);
        }
        original
    };
    let readers = Readers::new(&fixture.config, &fixture.shape, original.epoch());
    let clients: Vec<_> = original
        .core
        .clients
        .iter()
        .map(|worker| worker.client.clone())
        .collect();
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let readers = {
        let mut readers = readers;
        if std::env::var_os("TH_MODEL_PASSIVE_READS").is_some() && case.leased.is_some() {
            readers.probes = Some(
                fixture
                    .nodes
                    .iter()
                    .cloned()
                    .zip(clients.iter().cloned())
                    .collect(),
            );
        }
        readers
    };
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let live_metrics = original.core.live_metrics.clone();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let recorder = Recorder::open(&directory.join("history.jsonl"));
    write_call(&db, &recorder, case, &operations, &plan, baseline);
    db.flush_index().unwrap();
    write_model::arm(
        &directory.join("boundaries.txt"),
        case.boundary.clone(),
        case.gate_acks(),
    );
    for wave in 0..case.gate_waves {
        if case.concurrent {
            concurrent_model::wave(&db, &readers, &recorder, case, &operations, &plan, wave);
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
                observe(&db, &readers, &recorder);
            });
        }
        observe(&db, &readers, &recorder);
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    if case.leased.is_some() {
        leased::quiesce(&clients);
    }
    write_model::open_gate();
    if !case.flush_cut() {
        for id in WAVES * 2..WAVES * 2 + 2 {
            write_call(&db, &recorder, case, &operations, &plan, id);
        }
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    db.flush_index().unwrap();
    observe(&db, &readers, &recorder);
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    if let Some(profile) = case.leased {
        leased::verify_counts(profile, &live_metrics.unwrap(), directory);
    }
    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
    let _ = clients;
    std::process::exit(0);
}

fn recover_child(directory: &Path) {
    let restart: Restart =
        serde_json::from_slice(&fs::read(directory.join("restart.json")).unwrap()).unwrap();
    let case: Case =
        serde_json::from_slice(&fs::read(directory.join("case.json")).unwrap()).unwrap();
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut servers = Vec::new();
    let mut nodes = Vec::new();
    for ((config, tls), endpoint) in restart.nodes.iter().zip(&restart.config.endpoints) {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let node = if case.leased.is_some() {
            leased::open_node(config.clone())
        } else {
            super::super::tcp::StorageNode::open(config.clone(), recovery_metrics()).unwrap()
        };
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        let node =
            super::super::tcp::StorageNode::open(config.clone(), recovery_metrics()).unwrap();
        node.set_history_persistence(restart.config.history_persistence)
            .unwrap();
        node.set_durability(super::super::durability::DurabilityConfig::with_tier(
            if !case.native && case.leased.is_none_or(|profile| profile.synchronous) {
                super::super::durability::DurabilityTier::Synchronous
            } else {
                super::super::durability::DurabilityTier::Native
            },
        ))
        .unwrap();
        let listener = std::net::TcpListener::bind(&endpoint.address).unwrap();
        let serving = node.clone();
        let tls = tls.clone();
        let stop = shutdown.clone();
        servers.push(thread::spawn(move || {
            serving.serve(listener, tls, super::super::tcp::TcpLimits::default(), stop)
        }));
        nodes.push(node);
    }
    let db = RemoteDatabase::open(
        &restart.root.join("authority"),
        &restart.shape,
        restart.config.clone(),
        recovery_metrics(),
    )
    .unwrap();
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let (db, recovery_pins) = {
        let mut db = db;
        let pins = if case.leased.is_some() {
            leased::prepare_database(&mut db);
            leased::prepare_recovery_reads(&db)
        } else {
            Vec::new()
        };
        (db, pins)
    };
    let readers = Readers::new(&restart.config, &restart.shape, db.epoch());
    let db = ConcurrentRemoteDatabase::new(db).unwrap();
    let values = snapshot(&db, &readers);
    // Retry every externally acknowledged identity after a real owner restart.
    // Such retries must return their original receipt without overwriting newer
    // values. Pending identities are deliberately not retried before the oracle.
    let calls = history(directory, values.clone());
    let mut retried = HashSet::new();
    for call in &calls {
        if let Action::Write { operation, edits } = &call.action
            && call.end.is_some()
            && retried.insert(*operation)
        {
            let receipt = db
                .write_batch(
                    restart.operations[*operation],
                    mutations(edits, case.value_bytes()),
                )
                .unwrap();
            assert_eq!(
                &format!("{receipt:?}"),
                call.receipt.as_ref().unwrap(),
                "retry changed receipt after recovery operation={operation}"
            );
        }
    }
    assert_eq!(
        snapshot(&db, &readers),
        values,
        "recovered retry changed visible state"
    );
    fs::write(
        directory.join("recovered.json"),
        serde_json::to_vec(&values).unwrap(),
    )
    .unwrap();
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    for (client, token) in recovery_pins {
        client
            .lock()
            .release_live_epoch_pin(readers.epoch, token)
            .unwrap();
    }
    db.shutdown().unwrap();
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    for server in servers {
        server.join().unwrap().unwrap();
    }
    drop(nodes);
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
    let deadline = Instant::now() + Duration::from_secs(60);
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

fn run_case(case: &Case) {
    let temp = tempdir::TempDir::new("write-model-case").unwrap();
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
    let calls = history(&directory, values);
    let acknowledged: Vec<_> = calls
        .iter()
        .filter(|call| call.end.is_some() && matches!(call.action, Action::Write { .. }))
        .collect();
    assert!(
        acknowledged.len() >= case.gate_acks(),
        "gate has no acknowledged WAL tail"
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
    assert!(
        linearizable(&calls),
        "non-linearizable history {case:?} artifacts={}\n{calls:#?}",
        directory.display()
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn write_model_seeded_crashes() {
    // Six topology/distribution controls, then every named boundary after a
    // seed-selected acknowledged WAL tail containing 22- and 57-key batches.
    for owners in [1, 2, 4] {
        for hot in [false, true] {
            run_case(&Case::rpc(0x5a17 + owners as u64, owners, hot));
        }
    }
    for (index, boundary) in BOUNDARIES.iter().enumerate() {
        run_case(&Case {
            boundary: (*boundary).into(),
            on_disk_history: index % 2 != 0,
            ..Case::rpc(0x5a17 + index as u64, [1, 2, 4][index % 3], index % 2 == 0)
        });
    }
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
        let mut case = Case::rpc(
            seed.wrapping_add(iteration),
            env_number("TH_MODEL_OWNERS", [1, 2, 4][iteration as usize % 3]) as usize,
            env_number("TH_MODEL_HOT", iteration % 2) != 0,
        );
        case.gate_waves = env_number("TH_MODEL_GATE_WAVES", case.gate_waves as u64) as usize;
        case.on_disk_history = env_number("TH_MODEL_ON_DISK_HISTORY", iteration % 2) != 0;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Ok(index) = std::env::var("TH_MODEL_LEASED") {
            case.leased = Some(leased::profile(index.parse().unwrap()));
        }
        assert!([2, 3].contains(&case.gate_waves));
        assert!([1, 2, 4].contains(&case.owners));
        if let Ok(boundary) = std::env::var("TH_MODEL_BOUNDARY") {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            let allowed = leased::boundaries(&case);
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            let allowed = BOUNDARIES;
            assert!(allowed.contains(&boundary.as_str()));
            run_case(&Case { boundary, ..case });
        } else {
            run_case(&case);
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            let boundaries = leased::boundaries(&case);
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            let boundaries = BOUNDARIES;
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
