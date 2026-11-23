use super::pipeline_tests::Fixture;
use super::*;
use std::fs;

pub(super) fn checkpoint_put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

pub(super) fn recovery_metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

pub(super) fn fail_stop_without_checkpoint(db: RemoteDatabase) {
    // A worker may durably adopt its captured root during drop. No explicit
    // checkpoint is requested here; recovery must accept either valid root.
    db.core
        .begin_failure(&LocalDbError::new("injected fail-stop before reopen"));
    drop(db);
}

pub(super) fn reopen_result(fixture: &Fixture) -> Result<RemoteDatabase, LocalDbError> {
    fixture.try_open_with(fixture.config.clone())
}

fn manifest(db: &RemoteDatabase) -> AuthorityManifest {
    db.core.publisher.lock().manifest.clone()
}

/// Waits until the persisted durable cut covers `version` and the index
/// table's durable cut has advanced past it.
///
/// Native owners acknowledge before sync, so a native flush stores only the
/// durable prefix (A25). The wait is the production relocation wait
/// (`DurableCut::wait_through`): it nudges the owners and parks on the cut
/// actor's advance notification, so it follows observed progress and never a
/// sleep. The actor stores the persisted cut and wakes that wait before it
/// advances the table's durable cut, so the wait alone does not order the two.
/// The actor sends its persist event after the table advance; the event
/// channel, armed before the wait, is the signal that closes the gap.
fn await_durable(db: &RemoteDatabase, version: BatchVersion) {
    let cut = db
        .core
        .durable_cut
        .get()
        .expect("a tracked native epoch has a durable cut");
    let persisted = cut.persisted_events();
    let next = version.sequence + 1;
    cut.wait_through(next, Duration::from_secs(30), Duration::from_millis(20))
        .expect("the owner syncs the acknowledged batch");
    let covered = BatchVersion {
        epoch: version.epoch,
        sequence: next,
    };
    while db.core.table.cuts().durable() < covered {
        persisted
            .recv_timeout(Duration::from_secs(30))
            .expect("the cut actor publishes the durable cut it persisted");
    }
}

/// Config and named error of the off-Linux open tests. Those tests compile and
/// run only off Linux; on this Linux workstation they are checked by
/// `cargo check --tests --target x86_64-apple-darwin` and never executed.
#[cfg(not(target_os = "linux"))]
fn off_linux_open() -> (RemoteDatabaseConfig, KeyShape, String) {
    let config = super::pipeline_tests::coordinator_config(
        1,
        16,
        2,
        TlsFiles {
            transport: Default::default(),
            ca: std::path::PathBuf::new(),
            cert: std::path::PathBuf::new(),
            key: std::path::PathBuf::new(),
            server_name: String::new(),
        },
    );
    let shape = KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1));
    let expected = format!(
        "distributed route requires Linux (leased NVMe-oF): {}",
        std::env::consts::OS
    );
    (config, shape, expected)
}

#[cfg(not(target_os = "linux"))]
#[test]
fn remote_open_off_linux_fails_with_named_error() {
    let temp = tempdir::TempDir::new("remote-open-off-linux").unwrap();
    let path = temp.path().join("coordinator");
    let (config, shape, expected) = off_linux_open();
    let Err(error) = RemoteDatabase::open(&path, &shape, config, recovery_metrics()) else {
        panic!("remote open succeeded off Linux");
    };
    assert!(error.to_string().contains(&expected), "{error}");
    assert!(
        !path.exists(),
        "the refused open must not create its directory"
    );
}

/// The Sui entry point refuses before API capture: with capture enabled, the
/// error is still the named one and no capture file is created. Capture state
/// is process-wide, so the open runs in a child test process.
#[cfg(not(target_os = "linux"))]
#[test]
fn sui_open_remote_off_linux_fails_before_capture() {
    let root = tempdir::TempDir::new("sui-open-off-linux").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "distributed::remote_db::tests::sui_open_remote_off_linux_child",
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
    assert!(!root.path().join("trace").exists());
    assert!(!root.path().join("coordinator").exists());
}

#[cfg(not(target_os = "linux"))]
#[test]
#[ignore = "subprocess fixture used by sui_open_remote_off_linux_fails_before_capture"]
fn sui_open_remote_off_linux_child() {
    let root = std::path::PathBuf::from(std::env::var_os("SUI_CAPTURE_TEST_ROOT").unwrap());
    let (config, shape, expected) = off_linux_open();
    let Err(error) =
        crate::distributed::sui::Db::open_remote(&root.join("coordinator"), shape, config)
    else {
        panic!("Sui remote open succeeded off Linux");
    };
    let error = format!("{error:?}");
    assert!(error.contains(&expected), "{error}");
}

#[test]
fn read_lanes_default_to_parallelism_and_validate_bounds() {
    let fixture = Fixture::new();
    let mut serialized = serde_json::to_value(&fixture.config).unwrap();
    serialized.as_object_mut().unwrap().remove("read_lanes");
    let mut config: RemoteDatabaseConfig = serde_json::from_value(serialized).unwrap();
    assert_eq!(config.read_lanes, None);
    assert_eq!(
        config.effective_read_lanes(),
        crate::parallelism::available().min(RemoteDatabaseConfig::MAX_AUTO_READ_LANES)
    );
    assert!(
        serde_json::to_value(&config)
            .unwrap()
            .get("read_lanes")
            .is_none()
    );
    for lanes in [1, 1024] {
        config.read_lanes = Some(lanes);
        config.validate().unwrap();
    }
    for lanes in [0, 1025] {
        config.read_lanes = Some(lanes);
        assert!(config.validate().is_err());
    }
}

/// Open keeps the caller's key shape: relocation filters and compactors are
/// closures that the shape YAML cannot carry, and coordinator code that reads
/// them (relocation, retirement guards) must see the caller's.
#[test]
fn open_keeps_the_callers_relocation_filters_and_compactors() {
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = KeyShapeBuilder::new();
    let counted = Arc::clone(&calls);
    builder.add_key_space_config(
        "filtered",
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new()
            .with_relocation_filter(move |_, _| {
                counted.fetch_add(1, Ordering::Relaxed);
                crate::Decision::StopRelocation
            })
            .with_compactor(Box::new(|keys| {
                keys.next_back().cloned().into_iter().collect()
            })),
    );
    builder.add_key_space_config("plain", 4, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let fixture = Fixture::new_with_shape(128, builder.build());
    // First open, then a reopen of the existing authority.
    for open in 1..=2 {
        let db = fixture.open();
        let keyspace = |name: &str| {
            db.core
                .shape
                .iter_ks()
                .find(|keyspace| keyspace.name() == name)
                .unwrap()
        };
        let filter = keyspace("filtered").relocation_filter().unwrap();
        // The caller's closure itself: its decision and its side effect.
        assert!(matches!(filter(&[], &[]), crate::Decision::StopRelocation));
        assert_eq!(calls.load(Ordering::Relaxed), open);
        assert!(keyspace("filtered").compactor().is_some());
        assert!(keyspace("plain").relocation_filter().is_none());
        assert!(keyspace("plain").compactor().is_none());
        drop(db);
    }
}

#[test]
fn recorded_live_identity_cannot_reopen_as_tcp() {
    let fixture = Fixture::new();
    let db = fixture.open();
    fail_stop_without_checkpoint(db);
    let root = fixture.temp.path().join("authority");
    let mut manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    manifest.live_nvme_identity = Some((
        "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-1111-1111-111111111111".into(),
        [1; 16],
    ));
    persist_manifest(&root, &manifest, CoordinatorIoCategory::Open).unwrap();

    let error = reopen_result(&fixture).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("authority configuration differs")
    );
    let after: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(after.generations, manifest.generations);
    assert_eq!(after.next_generation, manifest.next_generation);
}

#[test]
fn empty_owner_identity_map_preserves_manifest_bytes() {
    // Exact serialized form of the current manifest without either optional
    // per-owner map: the flattened mode keys stay where the encoder puts them.
    #[derive(Serialize, Deserialize)]
    struct ManifestWithoutOwnerMap {
        format: u32,
        storage: LocalInventoryConfig,
        #[serde(flatten)]
        mode: AuthorityMode,
        endpoints: Vec<RemoteEndpoint>,
        shape_yaml: String,
        live_nvme_identity: Option<(String, [u8; 16])>,
        next_generation: u64,
        generations: Vec<AuthorityGeneration>,
        trim_pending: Option<u64>,
        lifecycle: Option<AuthorityLifecycle>,
        owner_control: Option<OwnerControlConfig>,
    }
    let f = Fixture::new();
    let db = f.open();
    let manifest = &manifest(&db);
    let without_map = ManifestWithoutOwnerMap {
        format: manifest.format,
        storage: manifest.storage.clone(),
        mode: manifest.mode.clone(),
        endpoints: manifest.endpoints.clone(),
        shape_yaml: manifest.shape_yaml.clone(),
        live_nvme_identity: manifest.live_nvme_identity.clone(),
        next_generation: manifest.next_generation,
        generations: manifest.generations.clone(),
        trim_pending: manifest.trim_pending,
        lifecycle: manifest.lifecycle,
        owner_control: manifest.owner_control.clone(),
    };
    assert_eq!(
        encode_metadata(manifest).unwrap(),
        encode_metadata(&without_map).unwrap()
    );
    let decoded: AuthorityManifest =
        serde_yaml::from_str(&serde_yaml::to_string(&without_map).unwrap()).unwrap();
    assert!(decoded.live_nvme_owner_identities.is_empty());
    drop(db);
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[test]
fn benchmark_owner_lookup_uses_authoritative_reference_and_reports_absence() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let key = 11u32.to_be_bytes();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(11, 1)])
        .unwrap();
    db.drain().unwrap();
    let partition = db
        .partition_for_key_for_benchmark(KeySpace(0), &key)
        .unwrap()
        .unwrap();
    assert!(fixture.config.storage.partitions.contains(&partition));
    assert_eq!(
        db.partition_for_key_for_benchmark(KeySpace(0), &12u32.to_be_bytes())
            .unwrap(),
        None
    );

    db.write_batch(
        fixture.operation(1, 2),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: key.to_vec().into(),
        }],
    )
    .unwrap();
    db.drain().unwrap();
    assert_eq!(
        db.partition_for_key_for_benchmark(KeySpace(0), &key)
            .unwrap(),
        None
    );
    db.shutdown().unwrap();
}

#[test]
fn checkpoint_restores_overwrite_delete_tail_and_original_retry_receipts() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let operation = fixture.operation(0, 1);
    let original = checkpoint_put(1, 1);
    let version = db
        .write_batch(operation, vec![original.clone(), checkpoint_put(2, 2)])
        .unwrap();
    let stored = db.core.flush_index().unwrap();
    assert!(stored.stored, "the first flush stores a control region");
    // A cadence pass heads the region at the lowest cell frontier: the batch
    // this cell has not flushed stays outside the region and is replayed.
    assert!(
        stored.cut <= version,
        "the stored region must not claim an unflushed batch: cut {:?}, version {:?}",
        stored.cut,
        version
    );
    let again = db.core.flush_index().unwrap();
    assert!(!again.stored, "an unchanged flush stores nothing");
    // Inside one epoch the retry window answers the exact retry and rejects a
    // changed payload for the same operation id.
    assert_eq!(
        db.write_batch(operation, vec![original.clone(), checkpoint_put(2, 2)])
            .unwrap(),
        version
    );
    assert!(
        db.write_batch(operation, vec![checkpoint_put(1, 9)])
            .is_err()
    );
    db.write_batch(
        fixture.operation(1, 2),
        vec![
            checkpoint_put(1, 3),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: 2u32.to_be_bytes().to_vec().into(),
            },
        ],
    )
    .unwrap();
    fail_stop_without_checkpoint(db);

    let mut db = fixture.open();
    assert!(db.recovery_stats().checkpoint_loaded);
    assert_eq!(
        db.recovery_stats().tail_batches,
        2,
        "the region names the cell frontier below the first batch, so recovery replays both"
    );
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[3; 32]
    );
    assert!(db.get(KeySpace(0), &2u32.to_be_bytes()).unwrap().is_none());
    db.shutdown().unwrap();
    drop(db);
    let mut db = fixture.open();
    assert!(db.recovery_stats().checkpoint_loaded);
    // Shutdown runs one ordinary cadence pass (`WriteCore::shutdown` calls
    // `snapshot_table`, never the forced pass): it flushes only the cells that
    // are stale by value-byte distance, so the stored region still names the
    // cell frontier below the first batch and recovery replays the same tail
    // (idempotently, by version; paper 3.3-3.4). The native `Db` drop runs no
    // pass at all; whether the hybrid should match that is open with Main.
    assert_eq!(
        db.recovery_stats().tail_batches,
        2,
        "a clean shutdown stores no region above the unflushed cell frontier"
    );
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[3; 32]
    );
    assert!(db.get(KeySpace(0), &2u32.to_be_bytes()).unwrap().is_none());
}

#[test]
fn checkpoint_is_not_adopted_until_every_required_shard_is_fenced() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(4, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    assert!(
        RemoteDatabase::open_fail_after_fences(
            &fixture.temp.path().join("authority"),
            &fixture.shape,
            fixture.config.clone(),
            recovery_metrics(),
            2,
        )
        .is_err()
    );
    let manifest: AuthorityManifest =
        read_metadata(&fixture.temp.path().join("authority").join(MANIFEST)).unwrap();
    assert_eq!(
        manifest.generations.last().copied(),
        Some(AuthorityGeneration {
            generation: 2,
            state: GenerationState::Reserved,
        })
    );
    let mut db = fixture.open();
    assert_eq!(db.epoch(), 3);
    assert!(db.recovery_stats().checkpoint_loaded);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
}

#[test]
fn a_damaged_referenced_value_is_never_served_as_absence() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    let cut = pipeline_tests::newest_cut();
    let position = db
        .core
        .table
        .get(KeySpace(0), &1u32.to_be_bytes(), cut)
        .unwrap()
        .expect("the written key is indexed");
    db.shutdown().unwrap();
    drop(db);
    // The member frame ends with the value, so its last byte is a value byte.
    let address = position.address;
    let file = fs::OpenOptions::new()
        .write(true)
        .open(
            fixture.startups[0]
                .0
                .root
                .join("epoch_0000000000000001")
                .join(format!(
                    "wal_{:016x}",
                    address.offset / fixture.config.storage.wal_file_bytes
                )),
        )
        .unwrap();
    file.write_all_at(
        &[99],
        address.offset % fixture.config.storage.wal_file_bytes + u64::from(address.len) - 1,
    )
    .unwrap();
    file.sync_all().unwrap();
    // History is WAL replay only: reopen re-reads the retained frame window
    // (retry identities) and rejects the damaged frame instead of serving.
    // Either way the value never becomes absence.
    let error = fixture
        .try_open_with(fixture.config.clone())
        .err()
        .expect("a damaged retained frame must not reopen as absence");
    assert!(
        error.to_string().contains("frame"),
        "unexpected reopen error: {error}"
    );
}

#[test]
fn submit_many_answers_retries_and_deduplicates_the_window() {
    // The hybrid admission has no receipt-identity cap: it charges each
    // segment's batch and byte budget and splits a window by partition
    // capacity (`WriteCore::partition_segments`), so a window of fresh
    // identities is admitted instead of rejected as a whole.
    let fixture = Fixture::new();
    let db = fixture.open();
    let known_id = fixture.operation(0, 1);
    let active_id = fixture.operation(1, 2);
    let new_id = fixture.operation(2, 3);
    let known = checkpoint_put(1, 1);
    let active = checkpoint_put(2, 2);
    let new = checkpoint_put(3, 3);
    let known_version = db.write_batch(known_id, vec![known.clone()]).unwrap();
    // Hold the active batch's owner so its identity stays Active (allocated,
    // unpublished); callers no longer wait for publication on submit.
    let active_connection = Arc::clone(&db.core.clients[1].client);
    let active_held = active_connection.lock();
    let active_version = db.submit(active_id, vec![active.clone()]).unwrap();
    let fresh = db
        .submit_many(&[
            (new_id, vec![new.clone()]),
            (fixture.operation(3, 4), vec![checkpoint_put(4, 4)]),
        ])
        .unwrap();
    assert_eq!(fresh[0].sequence, 2, "the window allocates in input order");
    assert_eq!(fresh[1].sequence, 3, "the window allocates in input order");
    assert_eq!(db.core.next_sequence(), 4);
    assert!(
        db.core.active_identities() >= 1,
        "the held batch keeps its allocated identity"
    );

    // One published retry, one in-flight retry, one fresh identity and its
    // duplicate: each retry answers with its original version and the
    // duplicate consumes no second slot.
    let versions = db
        .submit_many(&[
            (known_id, vec![known.clone()]),
            (active_id, vec![active.clone()]),
            (new_id, vec![new.clone()]),
            (new_id, vec![new.clone()]),
        ])
        .unwrap();
    assert_eq!(versions[0], known_version);
    assert_eq!(versions[1], active_version);
    assert_eq!(versions[2], fresh[0]);
    assert_eq!(versions[3], fresh[0]);
    assert_eq!(db.core.next_sequence(), 4);
    drop(active_held);
    db.drain().unwrap();
    assert_eq!(db.core.publisher.lock().recent.len(), 4);
    assert_eq!(
        db.submit_many(&[
            (known_id, vec![known]),
            (active_id, vec![active]),
            (new_id, vec![new]),
        ])
        .unwrap(),
        [known_version, active_version, fresh[0]],
    );
    assert_eq!(
        db.core.next_sequence(),
        4,
        "retries allocate no additional slot"
    );
}

#[test]
fn unchanged_checkpoint_is_a_noop_and_shutdown_clocks_are_retained() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let operation = fixture.operation(0, 200);
    let mutation = checkpoint_put(7, 42);
    let version = db.write_batch(operation, vec![mutation.clone()]).unwrap();
    let first = db.core.flush_index().unwrap();
    assert!(first.stored, "the first flush stores a control region");
    let region = db.core.table.region().lock().path().to_path_buf();
    let stored_bytes = fs::read(&region).unwrap();
    let barriers = db.storage_stats().unwrap().persistence_barriers;
    let again = db.core.flush_index().unwrap();
    assert!(!again.stored, "an unchanged flush stores no root");
    assert_eq!(
        fs::read(&region).unwrap(),
        stored_bytes,
        "an unchanged flush leaves the control region byte-identical"
    );
    assert_eq!(db.storage_stats().unwrap().persistence_barriers, barriers);
    assert_eq!(db.write_batch(operation, vec![mutation]).unwrap(), version);
    db.shutdown().unwrap();
    let closed = db.shutdown_stats();
    assert!(closed.seal_ns > 0);
    assert!(closed.join_ns > 0);
    assert!(db.core.stopped.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        db.core
            .clients
            .iter()
            .all(|worker| worker.thread.lock().is_none()),
        "shutdown joins every owner worker"
    );
    assert_eq!(
        fs::read(&region).unwrap(),
        stored_bytes,
        "shutdown of an unchanged database stores no root"
    );
    db.shutdown().unwrap();
    assert_eq!(db.shutdown_stats().join_ns, closed.join_ns);
}

#[test]
fn failed_parallel_connect_does_not_reserve_or_escape_an_authority() {
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.endpoints[2].server_name = "wrong-peer.invalid".to_owned();
    let root = fixture.authority();
    let mut manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    // Match the recorded endpoint so open reaches the parallel mTLS handshakes
    // rather than refusing changed geometry or a supplied ID for a new path.
    manifest.endpoints[2].server_name = config.endpoints[2].server_name.clone();
    persist_manifest(&root, &manifest, CoordinatorIoCategory::Open).unwrap();
    let before = fs::read(root.join(MANIFEST)).unwrap();
    let error = fixture.try_open_with(config.clone()).err().unwrap();
    assert!(error.to_string().contains("wrong-peer.invalid"), "{error}");
    assert!(manifest.generations.is_empty());
    assert_eq!(manifest.next_generation, 1);
    assert_eq!(fs::read(root.join(MANIFEST)).unwrap(), before);
    // The failed attempt's scoped connection workers are all gone before lock
    // acquisition can succeed. Keep the recorded immutable endpoint inventory.
    let lock = DbLock::acquire(&root, Duration::ZERO).unwrap();
    drop(lock);
    let retry = fixture.try_open_with(config).err().unwrap();
    assert!(retry.to_string().contains("wrong-peer.invalid"), "{retry}");
    assert_eq!(fs::read(root.join(MANIFEST)).unwrap(), before);
}

#[test]
fn failed_setup_joins_owned_workers_and_next_open_reserves_a_new_epoch() {
    let fixture = Fixture::new();
    let root = fixture.temp.path().join("authority");
    assert!(
        RemoteDatabase::open_inner_local_fixture(
            &root,
            &fixture.shape,
            fixture.config.clone(),
            Some(OpenInterruption::AfterReadSetup)
        )
        .is_err()
    );
    let manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(
        manifest.generations,
        vec![AuthorityGeneration {
            generation: 1,
            state: GenerationState::Reserved,
        }]
    );
    let db = fixture.open();
    assert_eq!(db.epoch(), 2);
    assert_eq!(db.recovery_stats().fabric_worker_ns, None);
    db.write_batch(fixture.operation(4, 201), vec![checkpoint_put(8, 9)])
        .unwrap();
    db.shutdown().unwrap();
}

#[test]
fn endpoint_fanout_is_bounded_ordered_and_joins_even_failed_waves() {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let parallelism = crate::parallelism::available();
    let barrier = Barrier::new(parallelism);
    let completed = AtomicUsize::new(0);
    let mut inputs = vec![0usize; parallelism * 2];
    let result: Result<Vec<usize>, _> = endpoint_map(&mut inputs, |index, value| {
        assert!(
            index < parallelism,
            "a failed wave must prevent later waves"
        );
        barrier.wait();
        *value = index;
        completed.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            Err(LocalDbError::new("injected endpoint failure"))
        } else {
            Ok(index)
        }
    });
    assert!(result.is_err());
    assert_eq!(completed.load(Ordering::SeqCst), parallelism);
    assert_eq!(inputs[parallelism - 1], parallelism - 1);
    assert_eq!(inputs[parallelism], 0);
    let ordered = endpoint_map(&mut inputs, |index, _| Ok(index)).unwrap();
    assert_eq!(ordered, (0..parallelism * 2).collect::<Vec<_>>());
    let result: Result<Vec<usize>, _> = endpoint_map(&mut inputs[..parallelism], |index, _| {
        barrier.wait();
        if index == 0 {
            panic!("injected endpoint panic");
        }
        completed.fetch_add(1, Ordering::SeqCst);
        Ok(index)
    });
    assert!(result.is_err());
    assert_eq!(completed.load(Ordering::SeqCst), parallelism * 2 - 1);
}

#[test]
fn explicit_cache_host_slice_accepts_large_budgets_and_rejects_overflow() {
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.read_cache_bytes = 300 * 1024 * 1024;
    assert!(config.validate().is_ok());
    assert!(config.read_cache_metadata_bytes() > 0);
    assert!(config.read_cache_metadata_bytes() * 100 <= config.read_cache_bytes * 8);
    config.read_memory_allowance_bytes = config.read_cache_bytes;
    assert!(config.validate().is_err());
    config.read_memory_allowance_bytes = usize::MAX;
    config.read_cache_bytes = usize::MAX;
    assert!(config.validate().is_err());
    config.read_cache_bytes = 0;
    config.read_memory_allowance_bytes = 0;
    assert!(config.validate().is_err());
}

#[test]
fn live_frame_can_fill_one_fragment() {
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.storage.fragment_bytes = 4096;
    config.storage.wal_file_bytes = 8192;
    config.storage.group_limits.max_encoded_bytes = 4088;
    config.live_nvme = super::super::tcp::LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        host_nqn: "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-1111-1111-111111111111".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        leased_write_window: Some(4),
        leased_sync: Default::default(),
        leased_flush_min_frame_bytes: 0,
        max_inflight_requests: 8,
        max_inflight_bytes: 64 * 1024,
        connect_timeout_ms: 1000,
        io_timeout_ms: 1000,
        leased_durability: super::super::durability::DurabilityTier::Native,
        inline_reads: true,
        queue_geometry: Default::default(),
    };
    assert!(config.validate().is_ok());
    assert_eq!(
        config.effective_owner_append_occupancy(),
        config.owner_append_occupancy
    );
    config.owner_append_occupancy = 8;
    let live = &mut config.live_nvme;
    live.max_inflight_requests = 32;
    live.leased_write_window = None;
    assert!(config.validate().is_ok());
    assert_eq!(config.effective_owner_append_occupancy(), 31);
    config.live_nvme.leased_write_window = Some(9);
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("leased write window exceeds owner append occupancy")
    );
    config.live_nvme.leased_write_window = Some(8);
    assert!(config.validate().is_ok());
    assert_eq!(config.effective_owner_append_occupancy(), 8);
}

#[test]
fn unusable_read_capacity_is_rejected_before_authority_mutation() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.max_pending_read_bytes = ReadService::MIN_FLIGHT_BYTES + 4096;
    fixture.config.read_cache_bytes = 0;
    for existing in [false, true] {
        if existing {
            let db = fixture.open();
            db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(0, 7)])
                .unwrap();
            db.shutdown().unwrap();
        }
        let root = if existing {
            fixture.authority()
        } else {
            fixture.temp.path().join("unusable-before-provision")
        };
        let before = existing.then(|| fs::read(root.join(MANIFEST)).unwrap());
        for (requests, bytes) in [
            (0, fixture.config.max_pending_read_bytes),
            (1, ReadService::MIN_FLIGHT_BYTES - 1),
        ] {
            let mut config = fixture.config.clone();
            config.max_pending_reads = requests;
            config.max_pending_read_bytes = bytes;
            if !existing {
                config.storage.database = [0; 16];
            }
            assert!(
                RemoteDatabase::open(&root, &fixture.shape, config, recovery_metrics()).is_err(),
                "accepted unusable read capacity: requests={requests}, bytes={bytes}"
            );
            assert_eq!(root.exists(), existing);
            if let Some(before) = &before {
                assert_eq!(fs::read(root.join(MANIFEST)).unwrap(), *before);
            }
        }
    }
    let mut db = fixture.open();
    assert_eq!(db.epoch(), 2);
    assert_eq!(
        db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(),
        Some(vec![7; 32].into())
    );
    assert_eq!(db.core.reads.live_read_submissions_for_test(), 1);
    db.shutdown().unwrap();
}

/// A read view pins the table of the open that took it. After a reopen its cut
/// names old-epoch versions and its latch pins nothing, so `get_at` refuses it
/// with a typed error (native `DbCheckpoint` cannot outlive its `Db`).
#[test]
fn a_read_checkpoint_from_an_earlier_open_is_refused() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    let old = db.checkpoint().unwrap();
    // Counter-control: inside its own open the view answers.
    assert_eq!(
        db.get_at(KeySpace(0), &1u32.to_be_bytes(), &old)
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    let error = reopened
        .get_at(KeySpace(0), &1u32.to_be_bytes(), &old)
        .unwrap_err();
    assert!(
        error.message().contains("earlier open"),
        "unexpected refusal: {error}"
    );
    let fresh = reopened.checkpoint().unwrap();
    assert!(
        reopened
            .get_at(KeySpace(0), &1u32.to_be_bytes(), &fresh)
            .unwrap()
            .is_some()
    );
    reopened.shutdown().unwrap();
}

#[test]
fn parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries() {
    let fixture = Fixture::new_with_capacity(1024);
    let mut db = fixture.open();
    let first = db
        .write_batch(fixture.operation(0, 900), vec![checkpoint_put(1, 7)])
        .unwrap();
    let old_cut = db.checkpoint().unwrap();
    db.flush_index().unwrap();
    // A cadence pass heads the region at the cell frontier below the first
    // batch, so recovery replays the whole stream; the tail below is large
    // enough that the replay merge refills several pages per owner. No
    // snapshot runs before the crash: the byte cadence is far away.
    assert!(
        db.core.table.stored_cut() <= first,
        "the stored region must not claim an unflushed batch"
    );
    let mut acknowledged = Vec::new();
    for round in 0..70u64 {
        for node in 0..5 {
            let operation = fixture.operation(node, 1000 + round * 5 + node as u64);
            let mutation = checkpoint_put(1, (round * 5 + node as u64) as u8);
            let version = db.write_batch(operation, vec![mutation.clone()]).unwrap();
            acknowledged.push((operation, mutation, version));
        }
    }
    // The read view pinned before the tail still answers with the value of its
    // cut while the later writes are in the index.
    assert_eq!(
        db.get_at(KeySpace(0), &1u32.to_be_bytes(), &old_cut)
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    // Inside one epoch every retry is answered from the retry window with its
    // recorded version and allocates nothing: the admission counter, which
    // counts every batch written in this open, does not move.
    let admitted = db.stats().admitted_batches;
    assert_eq!(
        admitted,
        acknowledged.len() as u64 + 1,
        "the first batch and every acknowledged round were admitted once"
    );
    for (operation, mutation, version) in &acknowledged {
        assert_eq!(
            db.write_batch(*operation, vec![mutation.clone()]).unwrap(),
            *version
        );
    }
    assert_eq!(
        db.stats().admitted_batches,
        admitted,
        "every in-epoch retry is answered from the window, none is rewritten"
    );
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    assert_eq!(
        db.recovery_stats().tail_batches,
        351,
        "the region names the epoch-start frontier, so the first batch and the tail are replayed"
    );
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[349u16 as u8; 32]
    );
    // A read view is a pin inside one open: it names a cut, not a durable
    // image, so it does not survive the crash (it was checked before it).
}

#[test]
fn legacy_manifest_modes_fail_open_before_any_mutation() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    let root = fixture.temp.path().join("authority");
    let current: serde_yaml::Value = read_metadata(&root.join(MANIFEST)).unwrap();
    // (case, key, value; None removes the key, expected mode name).
    let mut legacy: Vec<(String, &str, Option<serde_yaml::Value>, &str)> = Vec::new();
    for key in [
        "checkpoint_mode",
        "cell_checkpoint",
        "cell_checkpoint_obsolete",
    ] {
        // Explicit null is a present key and must fail like any other value.
        for value in [serde_yaml::Value::Null, "per_cell".into(), 1u32.into()] {
            legacy.push((format!("{key}={value:?}"), key, Some(value), "per_cell"));
        }
    }
    legacy.push(("format 5".into(), "format", Some(5u32.into()), "per_cell"));
    legacy.push((
        "absent history".into(),
        "history_persistence",
        None,
        "on_disk",
    ));
    legacy.push((
        "memory_only".into(),
        "history_persistence",
        Some("memory_only".into()),
        "memory_only",
    ));
    for (case, key, value, mode) in legacy {
        let mut old = current.clone();
        let fields = old.as_mapping_mut().unwrap();
        match value {
            Some(value) => {
                fields.insert(key.into(), value);
            }
            None => {
                fields.remove(serde_yaml::Value::from(key));
            }
        }
        install_metadata(&root, MANIFEST, &encode_metadata(&old).unwrap()).unwrap();
        let before = fs::read(root.join(MANIFEST)).unwrap();
        let mut entries: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        entries.sort();
        let error = reopen_result(&fixture).err().unwrap();
        assert_eq!(
            error.to_string(),
            format!("unsupported legacy distributed mode {mode}: use archive/multi-design"),
            "{case}"
        );
        assert_eq!(fs::read(root.join(MANIFEST)).unwrap(), before, "{case}");
        let mut after: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        after.sort();
        assert_eq!(after, entries, "{case}");
    }
    for (index, node) in fixture.nodes.iter().enumerate() {
        // Fencing generation 1 only: no reopen reached an owner.
        assert_eq!(
            node.status().unwrap().highest_fenced_generation,
            1,
            "owner {index} was fenced by a legacy open"
        );
    }
    install_metadata(&root, MANIFEST, &encode_metadata(&current).unwrap()).unwrap();
    let mut db = fixture.open();
    assert_eq!(db.epoch(), 2);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    db.shutdown().unwrap();
}

#[test]
fn pre_generation_authority_manifest_is_rejected_without_rewrite() {
    let fixture = Fixture::new();
    fail_stop_without_checkpoint(fixture.open());
    let root = fixture.temp.path().join("authority");
    let mut old: serde_yaml::Value = read_metadata(&root.join(MANIFEST)).unwrap();
    let fields = old.as_mapping_mut().unwrap();
    fields.insert("format".into(), 2u32.into());
    install_metadata(&root, MANIFEST, &encode_metadata(&old).unwrap()).unwrap();
    let before = fs::read(root.join(MANIFEST)).unwrap();
    let error = reopen_result(&fixture).err().unwrap();
    assert_eq!(
        error.to_string(),
        format!(
            "unsupported distributed authority manifest format 2 (expected {})",
            fencing_generations::AUTHORITY_FORMAT
        )
    );
    assert_eq!(fs::read(root.join(MANIFEST)).unwrap(), before);
}

#[test]
fn native_checkpoint_commits_no_image_while_an_owner_sync_is_stalled() {
    // Native owners acknowledge before sync. A stored control region is headed
    // at the lowest cell frontier and never names a cut above the durable cut
    // (A25), so it cannot cover a batch whose owner has not synced.
    let fixture = Fixture::new();
    let db = fixture.open();
    let epoch = db.epoch();
    let version = db
        .write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    let log = fixture.nodes[0].log_for_test(epoch);
    // A leased owner learns frame positions at adoption, not at write; the
    // acknowledged prefix ends at the acknowledged frame.
    let acknowledged_end = {
        let address = db
            .core
            .durable_receipt(version.sequence)
            .unwrap()
            .appended()
            .address();
        address.offset + u64::from(address.len)
    };
    assert!(acknowledged_end > 0);
    let (entered, release) = fixture.nodes[0].pause_next_persist(epoch).unwrap();
    let observer_log = Arc::clone(&log);
    let core = Arc::clone(&db.core);
    let observer = std::thread::spawn(move || {
        // The owner's sync for the checkpoint proof is now held.
        entered.recv_timeout(Duration::from_secs(30)).unwrap();
        // A cut is exclusive: a region whose cut is at or below the batch
        // does not cover it, so no stored image names the unsynced batch.
        let held = (
            core.table.stored_cut() <= version,
            observer_log.durable_progress().synced_end < acknowledged_end,
        );
        release.send(()).unwrap();
        held
    });
    let stored = db.core.flush_index().unwrap();
    let (no_image_while_stalled, still_unsynced) = observer.join().unwrap();
    assert!(stored.stored, "the pass stores a control region");
    assert!(
        no_image_while_stalled,
        "the stored region covered the batch before its owner synced"
    );
    assert!(still_unsynced, "the owner had already synced the batch");
    // After the owner synced, the persisted durable cut covers the batch and
    // the stored region never names a cut above it.
    await_durable(&db, version);
    assert!(
        db.core.table.cuts().durable() > version,
        "the durable cut must cover the synced batch"
    );
    assert!(
        db.core.table.stored_cut() <= db.core.table.cuts().durable(),
        "the stored region must not name a cut above the durable cut"
    );
    // The owner reports its synced prefix through the strict live root: the
    // durable cut can cover the batch only after the root covers it.
    assert!(
        log.durable_progress().synced_end >= acknowledged_end,
        "the owner's synced prefix must cover the synced batch"
    );
    db.shutdown().unwrap();
}
