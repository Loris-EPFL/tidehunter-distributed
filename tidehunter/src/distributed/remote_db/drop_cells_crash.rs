//! Process-crash table for one `DropCells` range against real loopback
//! owners on the leased ordinary-file fixture, in both ACK tiers and both
//! checkpoint orderings.
//!
//! The shape gives each of the 64 model keys its own cell, so dropping cells
//! 16..=47 is exactly deleting keys 16..=47 and the shared linearizability
//! oracle checks it unchanged. The crash cut is the first named boundary
//! after a quiescent acknowledged prefix (see `write_model`). Recovery
//! replays the WAL, retries every acknowledged identity (the range through
//! `drop_cells`) and must keep the original receipts. This is a process-crash
//! test with owner page cache intact, like the rest of the write model.

use super::*;
use crate::cell::CellId;

const DROP_CHILD: &str =
    "distributed::remote_db::model_check_tests::drop_cells_crash::drop_cells_crash_child";
const FIRST_CELL: usize = 16;
const LAST_CELL: usize = 47;
/// Plan index of the range operation.
const DROP: usize = 6;
/// Acknowledged writes before the gate: two baseline batches and four
/// overwrites, plus the recorder's initial count of one.
const GATE_ACKS: usize = 7;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum DropOrder {
    /// Drop, newer points, then a checkpoint that covers the drop.
    DropThenCheckpoint,
    /// A checkpoint, then the drop and newer points stay in the WAL tail.
    CheckpointThenDrop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DropCase {
    owners: usize,
    order: DropOrder,
    boundary: String,
    leased: LeasedCase,
}

impl DropCase {
    fn value_bytes(&self) -> usize {
        4096
    }
}

fn plan() -> Vec<Vec<Edit>> {
    let put = |keys: &[usize], tag: u64| {
        keys.iter()
            .map(|&key| Edit {
                key,
                value: Some((tag << 32) | key as u64),
            })
            .collect::<Vec<_>>()
    };
    let low: Vec<usize> = (0..KEYS / 2).collect();
    let high: Vec<usize> = (KEYS / 2..KEYS).collect();
    vec![
        put(&low, 1),
        put(&high, 1),
        put(&[3], 2),
        put(&[21], 2),
        put(&[40], 2),
        put(&[60], 2),
        // DROP: exactly the keys of cells 16..=47.
        (FIRST_CELL..=LAST_CELL)
            .map(|key| Edit { key, value: None })
            .collect(),
        put(&[20, 50], 3),
        put(&[5], 4),
        put(&[30], 5),
        // Large rewrites after the range: a 57-key batch of 4 KiB values
        // nearly fills a 256 KiB leased fragment, so these switch extents
        // after the gate, which the extent boundaries need.
        put(&(0..57).collect::<Vec<_>>(), 6),
        put(&(30..52).collect::<Vec<_>>(), 7),
        put(&(0..57).collect::<Vec<_>>(), 8),
        put(&(0..57).collect::<Vec<_>>(), 9),
        put(&(0..57).collect::<Vec<_>>(), 10),
        put(&(0..57).collect::<Vec<_>>(), 11),
    ]
}

fn fixture(directory: &Path, case: &DropCase) -> Fixture {
    let temp = tempdir::TempDir::new_in(directory, "cluster").unwrap();
    let fixture = Fixture::new_with_root(
        temp,
        case.owners,
        128,
        KeyShape::new_single(4, 8, KeyType::uniform(8)),
        2,
        |config| {
            config.storage.batch_limits.max_operations = 57;
            config.max_index_bytes = 16 * 1024 * 1024;
            config.read_cache_bytes = 64 * 1024;
            leased::configure(config, case.leased);
        },
        |_| {},
    );
    if case.leased.synchronous {
        fixture.synchronous()
    } else {
        fixture
    }
}

fn drop_range(db: &ConcurrentRemoteDatabase, operation: [u8; 16]) -> BatchVersion {
    db.drop_cells(
        operation,
        KeySpace(0),
        CellId::Integer(FIRST_CELL),
        CellId::Integer(LAST_CELL),
    )
    .unwrap()
}

fn call(
    db: &ConcurrentRemoteDatabase,
    recorder: &Recorder,
    case: &DropCase,
    operations: &[[u8; 16]],
    plan: &[Vec<Edit>],
    id: usize,
) {
    let call = recorder.begin(Action::Write {
        operation: id,
        edits: plan[id].clone(),
    });
    let receipt = if id == DROP {
        drop_range(db, operations[id])
    } else {
        db.write_batch(operations[id], mutations(&plan[id], case.value_bytes()))
            .unwrap()
    };
    recorder.ack(call, format!("{receipt:?}"));
}

fn write_stage(directory: &Path, case: &DropCase) {
    let fixture = fixture(directory, case);
    let plan = plan();
    let operations: Vec<_> = (0..plan.len())
        .map(|id| fixture.operation((id % case.owners) as u32, id as u64 + 1))
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
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let recorder = Recorder::open(&directory.join("history.jsonl"));
    // Acknowledgements count toward the gate only once it is armed.
    write_model::arm(
        &directory.join("boundaries.txt"),
        case.boundary.clone(),
        GATE_ACKS,
    );
    for id in 0..DROP {
        call(&db, &recorder, case, &operations, &plan, id);
    }
    db.flush_index().unwrap();
    observe(&db, &recorder);
    db.flush_durable().unwrap();
    leased::quiesce(&clients);
    write_model::open_gate();
    match case.order {
        DropOrder::DropThenCheckpoint => {
            call(&db, &recorder, case, &operations, &plan, DROP);
            call(&db, &recorder, case, &operations, &plan, DROP + 1);
            observe(&db, &recorder);
            db.flush_durable().unwrap();
            db.flush_index().unwrap();
            call(&db, &recorder, case, &operations, &plan, DROP + 2);
            call(&db, &recorder, case, &operations, &plan, DROP + 3);
        }
        DropOrder::CheckpointThenDrop => {
            call(&db, &recorder, case, &operations, &plan, DROP + 2);
            db.flush_index().unwrap();
            call(&db, &recorder, case, &operations, &plan, DROP);
            call(&db, &recorder, case, &operations, &plan, DROP + 1);
            observe(&db, &recorder);
            call(&db, &recorder, case, &operations, &plan, DROP + 3);
        }
    }
    for id in DROP + 4..plan.len() {
        call(&db, &recorder, case, &operations, &plan, id);
    }
    db.flush_durable().unwrap();
    db.flush_index().unwrap();
    observe(&db, &recorder);
    std::process::exit(0);
}

fn recover_stage(directory: &Path) {
    let restart: Restart =
        serde_json::from_slice(&fs::read(directory.join("restart.json")).unwrap()).unwrap();
    let case: DropCase =
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
    let recovery = db.recovery_stats();
    assert_eq!(
        recovery.recovered_cut.is_some(),
        !case.leased.synchronous,
        "durable-prefix recovery selection {case:?}"
    );
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
    let db = ConcurrentRemoteDatabase::new(db).unwrap();
    let values = snapshot(&db);
    let calls = history(directory, values.clone());
    let mut retried = HashSet::new();
    for call in &calls {
        if let Action::Write { operation, edits } = &call.action
            && let Some(receipt) = &call.receipt
            && !discard.is_some_and(|discard| discard.covers(receipt))
            && retried.insert(*operation)
        {
            let retry = if *operation == DROP {
                drop_range(&db, restart.operations[*operation])
            } else {
                db.write_batch(
                    restart.operations[*operation],
                    mutations(edits, case.value_bytes()),
                )
                .unwrap()
            };
            assert_eq!(
                &format!("{retry:?}"),
                receipt,
                "retry changed receipt after recovery operation={operation}"
            );
        }
    }
    assert_eq!(
        snapshot(&db),
        values,
        "recovered retry changed visible state"
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
#[ignore = "subprocess entry point; invoked by the drop_cells crash tests"]
fn drop_cells_crash_child() {
    let directory = PathBuf::from(
        std::env::var_os("TH_MODEL_CHILD_ROOT").expect("only run through parent test"),
    );
    let case: DropCase =
        serde_json::from_slice(&fs::read(directory.join("case.json")).unwrap()).unwrap();
    match std::env::var("TH_MODEL_CHILD_STAGE").unwrap().as_str() {
        "write" => write_stage(&directory, &case),
        "recover" => recover_stage(&directory),
        other => panic!("unknown drop model stage {other}"),
    }
}

fn spawn(directory: &Path, stage: &str) -> Output {
    let stdout = fs::File::create(directory.join(format!("{stage}.stdout"))).unwrap();
    let stderr = fs::File::create(directory.join(format!("{stage}.stderr"))).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            DROP_CHILD,
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
                "drop model child timed out stage={stage} artifacts={}",
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

fn run(case: &DropCase) {
    let temp = tempdir::TempDir::new("drop-cells-model-case").unwrap();
    // Retain on any failure, delete on success.
    let directory = temp.into_path();
    fs::write(
        directory.join("case.json"),
        serde_json::to_vec(case).unwrap(),
    )
    .unwrap();
    eprintln!("drop model {case:?} artifacts={}", directory.display());
    let write = spawn(&directory, "write");
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
    assert_eq!(trace.first(), Some(&format!("gate_after_acks={GATE_ACKS}")));
    if !case.boundary.is_empty() {
        assert_eq!(trace.last(), Some(&case.boundary), "wrong crash cut");
    }
    let recover = spawn(&directory, "recover");
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
    let discard: Option<Discard> =
        serde_json::from_slice(&fs::read(directory.join("recovered-cut.json")).unwrap()).unwrap();
    // Version B may discard acknowledged batches only at or above the
    // continuity bound of an owner that restarted (all do here).
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
    let mut index = 0;
    calls.retain(|_| {
        index += 1;
        !lost.contains(&(index - 1))
    });
    assert!(
        linearizable(&calls),
        "non-linearizable drop history {case:?} artifacts={}\n{calls:#?}",
        directory.display()
    );
    fs::remove_dir_all(directory).unwrap();
}

fn leased_case(profile: LeasedCase, order: DropOrder, boundary: &str) -> DropCase {
    DropCase {
        owners: 2,
        order,
        boundary: boundary.into(),
        leased: profile,
    }
}

/// Leased write-model boundaries without the GroupFlush probe cuts, which
/// need the probe's frame schedule.
fn leased_boundaries(profile: LeasedCase) -> Vec<&'static str> {
    leased::boundaries(&Case::new(0, 2, false, profile))
        .into_iter()
        .filter(|boundary| !boundary.contains("flush"))
        .collect()
}

/// The full table: every leased boundary in both tiers and both orderings.
#[test]
#[ignore = "full drop crash table; run in the 1,000-schedule sweep window"]
fn drop_cells_crash_table() {
    for profile in [leased::profile(0), leased::profile(4)] {
        for order in [DropOrder::DropThenCheckpoint, DropOrder::CheckpointThenDrop] {
            let case = leased_case(profile, order, "");
            run(&case);
            for boundary in leased_boundaries(profile) {
                run(&DropCase {
                    boundary: boundary.into(),
                    ..case.clone()
                });
            }
        }
    }
}

/// The leased route end to end: an owner grants and adopts the zero-
/// descriptor range frame, and recovery replays it in both tiers.
#[test]
fn drop_cells_leased_crash_cuts() {
    for profile in [leased::profile(0), leased::profile(4)] {
        for (order, boundary) in [
            (DropOrder::DropThenCheckpoint, ""),
            (DropOrder::CheckpointThenDrop, ""),
            (DropOrder::DropThenCheckpoint, "leased_frame_completed"),
            (DropOrder::DropThenCheckpoint, "leased_adoption_completed"),
            (DropOrder::DropThenCheckpoint, "publication_after"),
            (DropOrder::DropThenCheckpoint, "checkpoint_root_after"),
            (DropOrder::CheckpointThenDrop, "publication_before"),
        ] {
            run(&leased_case(profile, order, boundary));
        }
    }
}
