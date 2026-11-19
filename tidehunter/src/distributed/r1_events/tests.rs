//! Stream format, causal IDs, loss accounting and the terminal boundary.
use super::strict_json::{Strict, read_stream};
use super::*;

fn config(root: &Path, max_events: u64) -> R1EventConfig {
    R1EventConfig {
        case_output: root.to_owned(),
        run_id: "run".into(),
        case_id: "case".into(),
        tier: "native".into(),
        policy: "gc".into(),
        retention_sha256: "00".repeat(32),
        max_events,
        max_payload_bytes: 256,
        queue: 64,
    }
}

fn stream_path(root: &Path, role: &str, incarnation: &str, partial: bool) -> PathBuf {
    let suffix = if partial { ".jsonl.partial" } else { ".jsonl" };
    root.join("capacity/r1-events")
        .join(role)
        .join(format!("{incarnation}{suffix}"))
}

#[test]
fn strict_parser_rejects_duplicate_keys_at_any_depth() {
    assert!(serde_json::from_str::<Strict>(r#"{"a":1,"a":2}"#).is_err());
    assert!(serde_json::from_str::<Strict>(r#"{"a":{"b":1,"b":2}}"#).is_err());
    assert!(serde_json::from_str::<Strict>(r#"[{"b":1},{"b":2}]"#).is_ok());
}

#[test]
fn finished_stream_has_header_contiguous_events_and_a_terminal_line() {
    let temp = tempdir::TempDir::new("r1-events-finish").unwrap();
    let log = R1EventLog::open(&config(temp.path(), 16), "owner_O0", "inc0", [7; 16]).unwrap();
    let file = FileRef::located(0, 1, 0, 4096).with_inode(5, 9);
    let first = log
        .emit(
            "seal_durable",
            file,
            None,
            &[],
            serde_json::json!({"root_generation": 3}),
        )
        .unwrap();
    let second = log
        .emit(
            "retirement_root_durable",
            file,
            Some(1),
            std::slice::from_ref(&first),
            serde_json::json!({"root_generation": 4}),
        )
        .unwrap();
    assert_eq!((first.seq, second.seq), (0, 1));
    assert!(stream_path(temp.path(), "owner_O0", "inc0", true).exists());
    log.finish().unwrap();
    assert!(!stream_path(temp.path(), "owner_O0", "inc0", true).exists());
    let stream = read_stream(&stream_path(temp.path(), "owner_O0", "inc0", false));
    assert_eq!(stream.header["schema_version"], 1);
    assert_eq!(stream.header["producer_role"], "owner_O0");
    assert_eq!(stream.header["incarnation"], "inc0");
    assert_eq!(stream.header["database_id"], "07".repeat(16));
    assert_eq!(stream.header["mono_clock"], "monotonic_ns");
    assert_eq!(stream.events.len(), 2);
    let event = &stream.events[1];
    assert_eq!(event["kind"], "retirement_root_durable");
    assert_eq!(event["inode"], 9);
    assert_eq!(event["attempt_id"], 1);
    assert_eq!(
        event["after"],
        serde_json::json!([{"role": "owner_O0", "incarnation": "inc0", "seq": 0}])
    );
    assert!(
        stream.events[0]["monotonic_ns"].as_u64() <= event["monotonic_ns"].as_u64(),
        "monotonic within one stream"
    );
    // Unknown fields are explicit nulls, never absent.
    let unknown = &stream.events[0];
    assert_eq!(unknown["attempt_id"], serde_json::Value::Null);
    assert_eq!(
        stream.terminal,
        Some(serde_json::json!({"complete": true, "events": 2, "lost": 0}))
    );
    // Emits after the terminal boundary are ignored, and a second finish errors.
    assert!(
        log.emit("late", FileRef::default(), None, &[], serde_json::json!({}))
            .is_none()
    );
    assert!(log.finish().is_err());
}

#[test]
fn losses_are_counted_and_reported_and_never_leave_seq_gaps() {
    let temp = tempdir::TempDir::new("r1-events-loss").unwrap();
    let log = R1EventLog::open(&config(temp.path(), 2), "coordinator", "inc1", [0; 16]).unwrap();
    let small = serde_json::json!({});
    let large = serde_json::json!({"blob": "x".repeat(512)});
    assert!(
        log.emit("a", FileRef::default(), None, &[], small.clone())
            .is_some()
    );
    // Over the payload cap: lost, and the next event keeps seq contiguous.
    assert!(
        log.emit("b", FileRef::default(), None, &[], large)
            .is_none()
    );
    assert!(
        log.emit("c", FileRef::default(), None, &[], small.clone())
            .is_some()
    );
    // Over the event cap: lost.
    assert!(
        log.emit("d", FileRef::default(), None, &[], small)
            .is_none()
    );
    assert_eq!(log.lost(), 2);
    log.finish().unwrap();
    let stream = read_stream(&stream_path(temp.path(), "coordinator", "inc1", false));
    let kinds: Vec<_> = stream
        .events
        .iter()
        .map(|event| event["kind"].clone())
        .collect();
    assert_eq!(kinds, [serde_json::json!("a"), serde_json::json!("c")]);
    assert_eq!(
        stream.terminal,
        Some(serde_json::json!({"complete": true, "events": 2, "lost": 2}))
    );
}

#[test]
fn a_dropped_stream_stays_partial_without_a_terminal_line() {
    let temp = tempdir::TempDir::new("r1-events-drop").unwrap();
    let log = R1EventLog::open(&config(temp.path(), 16), "owner_O1", "inc2", [1; 16]).unwrap();
    log.emit(
        "file_rolled",
        FileRef::default(),
        None,
        &[],
        serde_json::json!({}),
    )
    .unwrap();
    drop(log);
    // The writer flushes on channel close; wait for its exit by polling the
    // file contents (bounded).
    let path = stream_path(temp.path(), "owner_O1", "inc2", true);
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let text = std::fs::read_to_string(&path).unwrap();
        if text.ends_with('\n') && text.lines().count() == 2 {
            let stream = read_stream(&path);
            assert_eq!(stream.events.len(), 1);
            assert!(stream.terminal.is_none());
            break;
        }
        assert!(Instant::now() < deadline, "writer never flushed");
        std::thread::yield_now();
    }
    assert!(!stream_path(temp.path(), "owner_O1", "inc2", false).exists());
}
