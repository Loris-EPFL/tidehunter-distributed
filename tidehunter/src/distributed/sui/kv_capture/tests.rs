use super::*;
#[test]
fn record_has_fixed_columns_and_stable_key_hash() {
    assert_eq!(hash(b"hello"), 0xa430d84680aabd0b);
    let mut output = Vec::new();
    Record {
        event: 3,
        key: hash(b"hello"),
        ..Record::default()
    }
    .write(&mut output)
    .unwrap();
    let text = String::from_utf8(output).unwrap();
    assert_eq!(
        text.trim_end().split('\t').count(),
        HEADER.split('\t').count()
    );
    assert_eq!(text.trim_end().split('\t').next_back().unwrap().len(), 64);
}
#[test]
fn bounded_channel_accounts_for_overflow_and_disconnect() {
    let (sender, receiver) = mpsc::sync_channel(1);
    let owner = Recorder {
        sender,
        start: Instant::now(),
        next: AtomicU64::new(0),
        accepted: AtomicU64::new(0),
        dropped_full: AtomicU64::new(0),
        dropped_closed: AtomicU64::new(0),
        active: AtomicU64::new(0),
        send_gate: AtomicU64::new(0),
        admitting: AtomicBool::new(true),
        writer: std::sync::Mutex::new(None),
        directory: PathBuf::new(),
    };
    owner.send(Record::default());
    owner.send(Record::default());
    assert_eq!(owner.accepted.load(Ordering::Relaxed), 1);
    assert_eq!(owner.dropped_full.load(Ordering::Relaxed), 1);
    drop(receiver);
    owner.send(Record::default());
    assert_eq!(owner.dropped_closed.load(Ordering::Relaxed), 1);
}
#[test]
fn stop_drains_and_reports_exact_record_counts() {
    let directory = tempdir::TempDir::new("kv-capture-drain").unwrap();
    let (sender, receiver) = mpsc::sync_channel(4);
    let owner = Recorder {
        sender,
        start: Instant::now(),
        next: AtomicU64::new(0),
        accepted: AtomicU64::new(0),
        dropped_full: AtomicU64::new(0),
        dropped_closed: AtomicU64::new(0),
        active: AtomicU64::new(0),
        send_gate: AtomicU64::new(0),
        admitting: AtomicBool::new(true),
        writer: std::sync::Mutex::new(None),
        directory: directory.path().into(),
    };
    owner.send(Record::default());
    owner.send(Record::default());
    fs::write(directory.path().join("stop"), "").unwrap();
    let mut output = Vec::new();
    writer(&owner, receiver, &mut output).unwrap();
    let status = fs::read_to_string(directory.path().join("final.status")).unwrap();
    assert!(status.contains("accepted\t2\nwritten\t2\ndropped_full\t0\ndropped_closed\t0\n"));
    assert_eq!(String::from_utf8(output).unwrap().lines().count(), 2);
}

#[test]
fn native_capture_records_calls_batches_and_bounds() {
    let root = tempdir::TempDir::new("sui-kv-capture-api").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "distributed::sui::kv_capture::tests::capture_child",
            "--ignored",
            "--nocapture",
        ])
        .env("SUI_TH_KV_CAPTURE_DIR", root.path().join("trace"))
        .env("SUI_CAPTURE_TEST_ROOT", root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events = fs::read_to_string(root.path().join("trace/events.tsv")).unwrap();
    let records: Vec<Vec<&str>> = events
        .lines()
        .skip(1)
        .map(|line| line.split('\t').collect())
        .collect();
    assert!(records.iter().any(|row| row[0] == "4" && row[14] == "0"));
    assert!(
        records
            .iter()
            .any(|row| row[0] == "5" && row[14] == "2" && row[15] == "1")
    );
    assert!(records.iter().any(|row| row[0] == "7" && row[9] == "4"));
    assert!(records.iter().any(|row| row[0] == "8"));
    let status = fs::read_to_string(root.path().join("trace/final.status")).unwrap();
    assert!(status.contains("dropped_full\t0\ndropped_closed\t0\nactive\t0\n"));
}

#[test]
#[ignore = "subprocess fixture used by native_capture_records_calls_batches_and_bounds"]
fn capture_child() {
    let root = PathBuf::from(std::env::var_os("SUI_CAPTURE_TEST_ROOT").unwrap());
    fs::create_dir(root.join("db")).unwrap();
    let shape = KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1));
    let mut config = crate::config::Config::small();
    config.snapshot_written_bytes = u64::MAX;
    let db = super::super::Db::open_native(
        &root.join("db"),
        shape,
        Arc::new(config),
        crate::metrics::Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
    .unwrap();
    let ks = db.ks("root");
    db.insert(ks, 1u32.to_be_bytes().to_vec(), vec![1; 8])
        .unwrap();
    assert_eq!(db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().len(), 8);
    assert!(!db.exists(ks, &2u32.to_be_bytes()).unwrap());
    let mut batch = db.write_batch();
    batch.write(ks, 2u32.to_be_bytes().to_vec(), vec![2; 16]);
    batch.delete(ks, 1u32.to_be_bytes().to_vec());
    batch.commit().unwrap();
    let mut iterator = db.checkpoint().iterator(ks);
    iterator.set_lower_bound(0u32.to_be_bytes().to_vec());
    assert_eq!(iterator.next().unwrap().unwrap().1.len(), 16);
    assert!(iterator.next().is_none());
    drop(iterator);
    finish_for_test();
    drop(db);
}
