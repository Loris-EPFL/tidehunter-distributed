//! Ordered R1 evidence across a retirement cut by a crash before the unlink.
//!
//! The coordinator and the owner each write one stream per incarnation.
//! Every stream is read back with the duplicate-key-rejecting parser:
//! - the pre-crash owner stream is `.partial`, without a terminal line, and
//!   holds `file_rolled` → `seal_durable` → `target_io_drained` →
//!   `target_removed` → `retirement_root_durable` for file 0, each linked by
//!   `after` to the previous event of that file, and no unlink;
//! - the pre-crash coordinator stream claims the same exact file (inode and
//!   seal root generation) and records the read-pin basis, with no reply
//!   (the request failed at the cut);
//! - the restarted owner (a new incarnation) publishes the unlink its
//!   recovery repeated, then the directory sync, and finishes complete.
use super::*;
use crate::distributed::r1_events::R1EventConfig;
use crate::distributed::r1_events::strict_json::{Stream, read_stream};
use serde_json::Value;

fn events_config(case: &Path) -> R1EventConfig {
    R1EventConfig {
        case_output: case.to_path_buf(),
        run_id: "r1-events-test".into(),
        case_id: "crash-before-unlink".into(),
        tier: "native".into(),
        policy: "r1".into(),
        retention_sha256: "0".repeat(64),
        max_events: 1 << 16,
        max_payload_bytes: 4096,
        queue: 1 << 14,
    }
}

/// Every stream file of the single role directory starting with `prefix`.
fn stream_files(case: &Path, prefix: &str) -> Vec<PathBuf> {
    let root = case.join("capacity/r1-events");
    let roles: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
        .map(|entry| entry.path())
        .collect();
    assert_eq!(roles.len(), 1, "{prefix}: {roles:?}");
    let mut files: Vec<PathBuf> = fs::read_dir(&roles[0])
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .collect();
    files.sort();
    files
}

fn is_partial(path: &Path) -> bool {
    path.to_string_lossy().ends_with(".jsonl.partial")
}

fn kind(event: &Value) -> &str {
    event["kind"].as_str().unwrap()
}

fn seq(event: &Value) -> u64 {
    event["seq"].as_u64().unwrap()
}

/// Events of the exact file `start` of `generation`, in stream order,
/// including the directory syncs that list it.
fn of_file(stream: &Stream, generation: u64, start: u64) -> Vec<Value> {
    stream
        .events
        .iter()
        .filter(|event| {
            event["fencing_generation"].as_u64() == Some(generation)
                && (event["file_start"].as_u64() == Some(start)
                    || (kind(event) == "wal_directory_synced"
                        && event["payload"]["files"]
                            .as_array()
                            .unwrap()
                            .contains(&Value::from(start))))
        })
        .cloned()
        .collect()
}

/// `event` names `previous` (same stream) among its predecessors; only a
/// directory sync (one per batch of unlinks) has more than one.
fn follows(event: &Value, previous: &Value, header: &Value) {
    let after = event["after"].as_array().unwrap();
    if kind(event) != "wal_directory_synced" {
        assert_eq!(after.len(), 1, "{event}");
    }
    let link = after
        .iter()
        .find(|id| id["seq"].as_u64() == Some(seq(previous)))
        .unwrap_or_else(|| panic!("{event} does not follow {previous}"));
    assert_eq!(link["role"], header["producer_role"], "{event}");
    assert_eq!(link["incarnation"], header["incarnation"], "{event}");
}

#[test]
fn r1_events_order_a_retirement_across_a_crash_before_unlink() {
    let mut fixture = Fixture::new(DurabilityTier::Native);
    // Outside the frozen crash state: a crash never rewrites evidence.
    let case = fixture.root.path().join("case");
    let config = events_config(&case);
    let db = fixture.open();
    let generation = db.epoch();
    db.core.r1_events.enable(&db.core, &config).unwrap();
    fixture.node().enable_r1_events(&config).unwrap();
    churn(&db, 0, INITIAL_WRITES);
    let mut written = INITIAL_WRITES;
    fixture
        .node()
        .source_log_for_test(generation, generation)
        .arm_crash_cut(CrashCut::BeforeUnlink);
    let mut fired = false;
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        if let Err(error) = retire(&db) {
            assert!(
                error
                    .message()
                    .contains("injected seal or retirement crash"),
                "{error:?}"
            );
            fired = true;
            break;
        }
    }
    assert!(fired, "no retirement reached the cut");
    // Both pre-crash handles are dropped without `finish`.
    let db = fixture.crash(db);
    fixture.node().enable_r1_events(&config).unwrap();
    assert_eq!(logical_state(&db, fixture.node()), model(written));
    assert!(!fixture.wal_files(generation).contains(&0));
    fixture.node().finish_r1_events().unwrap();
    db.shutdown().unwrap();

    // Owner, before the crash: incomplete, ordered, linked, no unlink.
    let owner_files = stream_files(&case, "owner_O");
    assert_eq!(owner_files.len(), 2, "{owner_files:?}");
    let (before, after): (Vec<_>, Vec<_>) = owner_files
        .iter()
        .cloned()
        .partition(|path| is_partial(path));
    assert_eq!((before.len(), after.len()), (1, 1), "{owner_files:?}");
    let old = read_stream(&before[0]);
    assert!(old.terminal.is_none());
    assert_eq!(old.header["schema_version"], 1);
    assert_eq!(old.header["mono_clock"], "monotonic_ns");
    // Every event of file 0 follows the previous one of that file. Pins
    // (if any read held one) interleave; an owner `Busy` after the revoke
    // repeats the target pair before the root.
    let file0 = of_file(&old, generation, 0);
    for pair in file0.windows(2) {
        follows(&pair[1], &pair[0], &old.header);
    }
    let steps: Vec<&Value> = file0
        .iter()
        .filter(|event| !kind(event).starts_with("pin_"))
        .collect();
    let kinds: Vec<&str> = steps.iter().map(|event| kind(event)).collect();
    let middle = &kinds[2.min(kinds.len())..kinds.len().saturating_sub(1)];
    assert!(
        kinds.len() >= 5
            && kinds[..2] == ["file_rolled", "seal_durable"]
            && kinds.last() == Some(&"retirement_root_durable")
            && middle
                .chunks(2)
                .all(|pair| pair == ["target_io_drained", "target_removed"]),
        "{kinds:?}"
    );
    let seal = steps[1];
    let root = steps[steps.len() - 1];
    let seal_generation = seal["payload"]["root_generation"].as_u64().unwrap();
    assert_eq!(
        root["payload"]["seal_generation"].as_u64(),
        Some(seal_generation)
    );
    assert!(root["payload"]["root_generation"].as_u64() > Some(seal_generation));
    for event in &steps[1..] {
        assert_eq!(event["inode"], seal["inode"], "{event}");
        assert_eq!(event["device"], seal["device"], "{event}");
    }

    // Coordinator, before the crash: the same exact file and seal, then the
    // pin basis; the failed request has no reply.
    let coordinator_files = stream_files(&case, "coordinator");
    assert_eq!(coordinator_files.len(), 1);
    assert!(is_partial(&coordinator_files[0]));
    let coordinator = read_stream(&coordinator_files[0]);
    assert!(coordinator.terminal.is_none());
    // Each attempt claims, then records the pin basis. Earlier attempts (if
    // any) were answered `busy`; the last one failed at the cut, unanswered.
    let claims = of_file(&coordinator, generation, 0);
    assert!(
        !claims.is_empty() && claims.len().is_multiple_of(2),
        "{claims:?}"
    );
    for (index, pair) in claims.chunks(2).enumerate() {
        let (claim, drained) = (&pair[0], &pair[1]);
        assert_eq!(kind(claim), "victim_claimed", "{claim}");
        assert_eq!(kind(drained), "initiator_cq_drained", "{drained}");
        follows(drained, claim, &coordinator.header);
        assert_eq!(
            drained["payload"]["basis"],
            "no_reference_index_pins_to_terminal_cqe"
        );
        assert_eq!(claim["attempt_id"], drained["attempt_id"]);
        assert_eq!(claim["inode"], seal["inode"]);
        assert_eq!(claim["device"], seal["device"]);
        assert_eq!(
            claim["payload"]["seal_generation"].as_u64(),
            Some(seal_generation)
        );
        let replies: Vec<&Value> = coordinator
            .events
            .iter()
            .filter(|event| {
                kind(event) == "retirement_reply" && event["attempt_id"] == claim["attempt_id"]
            })
            .collect();
        if index + 1 == claims.len() / 2 {
            assert!(replies.is_empty(), "{replies:?}");
        } else {
            assert_eq!(replies.len(), 1, "{replies:?}");
            assert_eq!(replies[0]["payload"]["outcome"], "busy");
        }
    }

    // Owner after restart: a new incarnation publishes the recovery unlink
    // of the same inode, then the directory sync, and ends complete.
    let new = read_stream(&after[0]);
    assert_ne!(new.header["incarnation"], old.header["incarnation"]);
    // `read_stream` returns the object under the line's `terminal` key.
    let terminal = new.terminal.as_ref().unwrap();
    assert_eq!(terminal["complete"], true);
    assert_eq!(terminal["lost"], 0);
    assert_eq!(terminal["events"].as_u64(), Some(new.events.len() as u64));
    let unlink = new
        .events
        .iter()
        .find(|event| {
            kind(event) == "wal_unlinked"
                && event["fencing_generation"].as_u64() == Some(generation)
                && event["file_start"].as_u64() == Some(0)
        })
        .unwrap_or_else(|| panic!("no recovery unlink: {:?}", new.events));
    assert_eq!(unlink["inode"], seal["inode"]);
    assert_eq!(unlink["payload"]["phase"], "recovery");
    assert_eq!(
        unlink["payload"]["seal_generation"].as_u64(),
        Some(seal_generation)
    );
    let synced = new
        .events
        .iter()
        .find(|event| kind(event) == "wal_directory_synced" && seq(event) > seq(unlink))
        .unwrap();
    assert!(
        synced["after"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id["seq"].as_u64() == Some(seq(unlink)))
    );
    assert!(
        synced["payload"]["files"]
            .as_array()
            .unwrap()
            .contains(&Value::from(0u64))
    );
}

/// A victim whose live binding is already gone (evicted, or revoked by an
/// earlier attempt) gets no `target_io_drained`/`target_removed`: the owner
/// certifies only the bindings its retirement call actually revoked. The
/// other steps of the file still appear, in order and linked.
#[test]
fn r1_events_skip_target_steps_for_a_victim_without_a_binding() {
    let fixture = Fixture::new(DurabilityTier::Native);
    let case = fixture.root.path().join("case");
    let config = events_config(&case);
    let db = fixture.open();
    let generation = db.epoch();
    db.core.r1_events.enable(&db.core, &config).unwrap();
    fixture.node().enable_r1_events(&config).unwrap();
    churn(&db, 0, INITIAL_WRITES);
    let mut written = INITIAL_WRITES;
    let mut revoked = 0;
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        // Drop file 0's binding just before each pass, so the pass that
        // retires it finds no binding to revoke.
        if fixture.wal_files(generation).contains(&0) {
            revoked += fixture
                .node()
                .revoke_file_binding_for_test(generation, 0)
                .unwrap();
        }
        retire(&db).unwrap();
        if !fixture.wal_files(generation).contains(&0) {
            break;
        }
    }
    assert!(revoked >= 1, "file 0 never had a live binding to drop");
    assert!(
        !fixture.wal_files(generation).contains(&0),
        "file 0 not retired"
    );
    fixture.node().finish_r1_events().unwrap();
    db.core.r1_events.finish().unwrap();
    db.shutdown().unwrap();

    let owner_files = stream_files(&case, "owner_O");
    assert_eq!(owner_files.len(), 1, "{owner_files:?}");
    let owner = read_stream(&owner_files[0]);
    assert_eq!(owner.terminal.as_ref().unwrap()["lost"], 0);
    let file0 = of_file(&owner, generation, 0);
    for pair in file0.windows(2) {
        follows(&pair[1], &pair[0], &owner.header);
    }
    let kinds: Vec<&str> = file0
        .iter()
        .map(kind)
        .filter(|kind| !kind.starts_with("pin_"))
        .collect();
    assert_eq!(
        kinds,
        [
            "file_rolled",
            "seal_durable",
            "retirement_root_durable",
            "wal_unlinked",
            "wal_directory_synced",
            "target_tombstone_synced",
            "cleanup_root_durable",
        ],
        "{file0:?}"
    );
    let coordinator = read_stream(&stream_files(&case, "coordinator")[0]);
    assert_eq!(coordinator.terminal.as_ref().unwrap()["lost"], 0);
    let claims = of_file(&coordinator, generation, 0);
    assert!(
        claims.iter().any(|event| kind(event) == "victim_claimed"),
        "{claims:?}"
    );
}
