use super::pipeline_tests::Fixture;
use super::*;

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

fn snapshot_generation(db: &RemoteDatabase) -> u64 {
    db.core.publisher.lock().snapshot_generation
}

fn checkpoint_tail(db: &RemoteDatabase) -> (usize, usize) {
    let publisher = db.core.publisher.lock();
    (
        publisher.checkpoint_tail_batches,
        publisher.checkpoint_tail_bytes,
    )
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
fn empty_owner_identity_map_preserves_legacy_manifest_bytes() {
    // Exact field order and representations of the pre-owner-map manifest.
    #[derive(Serialize, Deserialize)]
    struct LegacyManifest {
        format: u32,
        storage: LocalInventoryConfig,
        history_persistence: String,
        endpoints: Vec<RemoteEndpoint>,
        shape_yaml: String,
        replay_limits: ReplayLimits,
        live_nvme_identity: Option<(String, [u8; 16])>,
        next_generation: u64,
        generations: Vec<AuthorityGeneration>,
        trim_pending: Option<u64>,
        checkpoint: Option<CheckpointCommit>,
        checkpoint_staging: Option<CheckpointFiles>,
        checkpoint_obsolete: Option<CheckpointCommit>,
    }
    let f = Fixture::new();
    let db = f.open();
    let manifest = &manifest(&db);
    let legacy = LegacyManifest {
        format: manifest.format,
        storage: manifest.storage.clone(),
        history_persistence: "wal_replay".into(),
        endpoints: manifest.endpoints.clone(),
        shape_yaml: manifest.shape_yaml.clone(),
        replay_limits: manifest.replay_limits,
        live_nvme_identity: manifest.live_nvme_identity.clone(),
        next_generation: manifest.next_generation,
        generations: manifest.generations.clone(),
        trim_pending: manifest.trim_pending,
        checkpoint: manifest.checkpoint.clone(),
        checkpoint_staging: manifest.checkpoint_staging.clone(),
        checkpoint_obsolete: manifest.checkpoint_obsolete.clone(),
    };
    assert_eq!(
        encode_metadata(manifest).unwrap(),
        encode_metadata(&legacy).unwrap()
    );
    let old: AuthorityManifest =
        serde_yaml::from_str(&serde_yaml::to_string(&legacy).unwrap()).unwrap();
    assert!(old.live_nvme_owner_identities.is_empty());
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
    db.flush_index().unwrap();
    let generation = snapshot_generation(&db);
    db.flush_index().unwrap();
    assert_eq!(
        snapshot_generation(&db),
        generation,
        "unchanged flush must not duplicate snapshots"
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
    assert_eq!(db.recovery_stats().tail_batches, 1);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[3; 32]
    );
    assert!(db.get(KeySpace(0), &2u32.to_be_bytes()).unwrap().is_none());
    assert_eq!(
        db.write_batch(operation, vec![original, checkpoint_put(2, 2)])
            .unwrap(),
        version
    );
    assert!(
        db.write_batch(operation, vec![checkpoint_put(1, 9)])
            .is_err()
    );
    assert!(db.get(KeySpace(0), &2u32.to_be_bytes()).unwrap().is_none());
    db.shutdown().unwrap();
    drop(db);
    let db = fixture.open();
    assert!(db.recovery_stats().checkpoint_loaded);
    assert_eq!(db.recovery_stats().tail_batches, 0);
}

#[test]
fn interrupted_checkpoint_images_and_metadata_never_supersede_committed_pointer() {
    for stage in [1, 2] {
        let fixture = Fixture::new();
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.flush_index().unwrap();
        let committed = manifest(&db).checkpoint.as_ref().unwrap().metadata_name();
        db.write_batch(fixture.operation(1, 2), vec![checkpoint_put(1, 2)])
            .unwrap();
        db.core.test_hooks.lock().checkpoint_interruption = Some(stage);
        assert!(db.flush_index().is_err());
        let persisted: AuthorityManifest = read_metadata(&db.core.path.join(MANIFEST)).unwrap();
        assert_eq!(persisted.checkpoint.unwrap().metadata_name(), committed);
        let staged = persisted.checkpoint_staging.unwrap();
        drop(db);
        let mut db = fixture.open();
        assert!(db.recovery_stats().checkpoint_loaded);
        assert_eq!(db.recovery_stats().tail_batches, 1);
        assert_eq!(
            db.get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[2; 32]
        );
        for name in staged.names() {
            assert!(
                !db.core.path.join(name).exists(),
                "reserved interrupted output must be retired"
            );
        }
        assert!(manifest(&db).checkpoint_staging.is_none());
        assert!(manifest(&db).checkpoint_obsolete.is_none());
    }
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

fn replace_checkpoint_metadata(root: &Path, change: impl FnOnce(&mut CheckpointImageV4)) {
    let mut manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    let commit = manifest.checkpoint.as_mut().unwrap();
    let name = commit.metadata_name();
    let mut image = read_checkpoint(&root.join(&name)).unwrap();
    change(&mut image);
    let encoded = encode_checkpoint(&image).unwrap();
    commit.metadata_bytes = encoded.len() as u64;
    commit.metadata_digest = Blake2b::<U32>::digest(&encoded).into();
    install_metadata(root, &name, &encoded).unwrap();
    persist_manifest(root, &manifest, CoordinatorIoCategory::Open).unwrap();
}

#[test]
fn committed_checkpoint_corruption_and_missing_files_fail_closed() {
    for damage in 0..6 {
        let fixture = Fixture::new();
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.shutdown().unwrap();
        let root = db.core.path.clone();
        let commit = manifest(&db).checkpoint.clone().unwrap();
        drop(db);
        match damage {
            0 => fs::remove_file(root.join(commit.index_name())).unwrap(),
            1 => fs::write(root.join(commit.index_name()), b"corrupt image").unwrap(),
            2 => fs::remove_file(root.join(commit.metadata_name())).unwrap(),
            3 => fs::write(root.join(commit.metadata_name()), b"corrupt metadata").unwrap(),
            4 => replace_checkpoint_metadata(&root, |image| image.database[0] ^= 1),
            5 => replace_checkpoint_metadata(&root, |image| {
                // An unaligned replay start is never a valid WAL position.
                image.positions[0].replay_from += 1;
            }),
            _ => unreachable!(),
        }
        let error = reopen_result(&fixture)
            .err()
            .expect("corrupt checkpoint must not fall back");
        if damage == 5 {
            assert!(
                error
                    .to_string()
                    .contains("invalid checkpoint WAL position")
            );
        }
    }
}

#[test]
fn automatic_checkpoint_admission_keeps_crash_replay_bounded() {
    let mut fixture = Fixture::new_with_capacity(40);
    fixture.config.replay_limits = ReplayLimits {
        max_batches: 32,
        max_encoded_bytes: 1024 * 1024,
    };
    fixture.config.max_admitted_batches = 1024;
    let limits = fixture.config.replay_limits;
    // Eight retained generations per owner, the old fixture's lifetime quota.
    let streams = fixture.config.endpoints.len() * 8;
    assert!(checkpoint_metadata_fits(streams, limits.max_batches + 8));
    assert!(!checkpoint_metadata_fits(streams, usize::MAX));
    assert!(!checkpoint_metadata_fits(
        streams,
        MAX_CHECKPOINT_METADATA_BYTES as usize / 128
    ));
    let db = fixture.open();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    let mut acknowledged = Vec::new();
    let count = 4 * limits.max_batches;
    for n in 0..count {
        let operation = fixture.operation((n % 5) as u32, n as u64);
        let value = n as u8;
        let version = db
            .write_batch(operation, vec![checkpoint_put(1, value)])
            .unwrap();
        acknowledged.push((operation, value, version));
    }
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(checkpoint_tail(&db).0, count);
    assert!(!db.core.path.join("receipt-history-v2").exists());
    let retained_tail = checkpoint_tail(&db).0;
    let clocks = db.authority_write_stats();
    assert!(clocks.checkpoint_count > 0);
    drop(resume);
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    assert_eq!(db.recovery_stats().tail_batches, count as u64);
    assert!(db.recovery_stats().tail_batches <= retained_tail as u64);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[acknowledged.last().unwrap().1; 32]
    );
    for (operation, value, version) in acknowledged {
        assert_eq!(
            db.write_batch(operation, vec![checkpoint_put(1, value)])
                .unwrap(),
            version
        );
    }
}

#[test]
fn checkpoint_does_not_turn_damaged_referenced_value_into_absence() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    let reference = db
        .core
        .index
        .get(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    let address = reference.batch_address;
    let file = fs::OpenOptions::new()
        .write(true)
        .open(
            fixture
                .temp
                .path()
                .join("node-0")
                .join("epoch_0000000000000001")
                .join(format!(
                    "wal_{:016x}",
                    address.offset / fixture.config.storage.wal_file_bytes
                )),
        )
        .unwrap();
    file.write_all_at(
        &[99],
        address.offset % fixture.config.storage.wal_file_bytes
            + 8
            + u64::from(reference.descriptor.offset)
            + u64::from(reference.descriptor.value_offset),
    )
    .unwrap();
    file.sync_all().unwrap();
    // History is WAL replay only: reopen re-reads the retained frame window
    // (retry identities) and rejects the damaged frame instead of serving.
    // The removed on-disk receipt history let the checkpoint load and failed
    // the read later; either way the value never becomes absence.
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
fn checkpoint_retirement_bounds_files_and_preserves_pinned_immutable_index() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let first = manifest(&db).checkpoint.clone().unwrap();
    let pinned = NativeIndex::open_snapshot(
        &db.core.path.join(first.index_name()),
        &fixture.shape,
        fixture.config.storage.database,
        fixture.config.max_index_bytes,
    )
    .unwrap();
    let reference = pinned
        .get(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap();
    for n in 2..6 {
        let previous = manifest(&db).checkpoint.clone().unwrap();
        db.write_batch(fixture.operation(1, n), vec![checkpoint_put(1, n as u8)])
            .unwrap();
        db.flush_index().unwrap();
        assert!(!db.core.path.join(previous.index_name()).exists());
        assert!(!db.core.path.join(previous.metadata_name()).exists());
        assert!(
            db.core
                .path
                .join(manifest(&db).checkpoint.as_ref().unwrap().index_name())
                .is_file()
        );
        assert!(manifest(&db).checkpoint_obsolete.is_none());
    }
    assert_eq!(
        pinned
            .get(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap(),
        reference
    );
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[5; 32]
    );
}

#[test]
fn committed_checkpoint_resumes_interrupted_retirement_without_rolling_back() {
    for partially_unlinked in [false, true] {
        let fixture = Fixture::new();
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.flush_index().unwrap();
        let previous = manifest(&db).checkpoint.clone().unwrap();
        db.write_batch(fixture.operation(1, 2), vec![checkpoint_put(1, 2)])
            .unwrap();
        db.core.test_hooks.lock().checkpoint_interruption = Some(3);
        let error = db.flush_index().unwrap_err();
        assert!(error.to_string().contains("checkpoint committed"));
        let persisted: AuthorityManifest = read_metadata(&db.core.path.join(MANIFEST)).unwrap();
        assert_eq!(
            persisted.checkpoint.unwrap().generation,
            previous.generation + 1
        );
        assert_eq!(
            persisted.checkpoint_obsolete.unwrap().generation,
            previous.generation
        );
        if partially_unlinked {
            fs::remove_file(db.core.path.join(previous.index_name())).unwrap();
        }
        drop(db);
        let mut db = fixture.open();
        assert!(db.recovery_stats().checkpoint_loaded);
        assert_eq!(db.recovery_stats().tail_batches, 0);
        assert!(!db.core.path.join(previous.index_name()).exists());
        assert!(!db.core.path.join(previous.metadata_name()).exists());
        assert!(manifest(&db).checkpoint_obsolete.is_none());
        assert_eq!(
            db.get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[2; 32]
        );
    }
}

#[test]
fn checkpoint_cleanup_error_reports_committed_root_and_preserves_recovery_intent() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let previous = manifest(&db).checkpoint.clone().unwrap();
    let metadata = db.core.path.join(previous.metadata_name());
    let saved = db.core.path.join("saved-old-metadata");
    fs::rename(&metadata, &saved).unwrap();
    fs::create_dir(&metadata).unwrap(); // Inject an actual non-regular cleanup target.
    db.write_batch(fixture.operation(1, 2), vec![checkpoint_put(1, 2)])
        .unwrap();
    let error = db.flush_index().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checkpoint committed; artifact cleanup incomplete")
    );
    assert!(db.core.ensure_healthy().is_err());
    let persisted: AuthorityManifest = read_metadata(&db.core.path.join(MANIFEST)).unwrap();
    assert_eq!(
        persisted.checkpoint.unwrap().generation,
        previous.generation + 1
    );
    assert!(persisted.checkpoint_obsolete.is_some());
    assert!(
        db.core.path.join(previous.index_name()).exists(),
        "validate all cleanup targets before unlink"
    );
    fs::remove_dir(&metadata).unwrap();
    fs::rename(saved, &metadata).unwrap();
    drop(db);
    let mut db = fixture.open();
    assert_eq!(db.recovery_stats().tail_batches, 0);
    assert!(!metadata.exists());
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
}

#[test]
fn checkpoint_never_claims_or_cleans_preexisting_untracked_outputs() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let unrelated = db.core.path.join(format!(
        "index-{}-{}.snapshot",
        db.epoch(),
        snapshot_generation(&db) + 1
    ));
    fs::write(&unrelated, b"not checkpoint-owned").unwrap();
    db.write_batch(fixture.operation(1, 2), vec![checkpoint_put(1, 2)])
        .unwrap();
    assert!(
        db.flush_index()
            .unwrap_err()
            .to_string()
            .contains("output path already exists")
    );
    assert!(manifest(&db).checkpoint_staging.is_none());
    drop(db);
    let mut db = fixture.open();
    assert_eq!(fs::read(unrelated).unwrap(), b"not checkpoint-owned");
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
}

#[test]
fn checkpoint_capacity_preflight_rejects_entire_window_and_deduplicates_retries() {
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
    // Exactly one distinct new receipt fits the identity (receipt metadata) bound.
    db.core.admission.limit_identities_for_test(3);
    // Owner stats would need the held client; the worker-formed group count
    // is the local proof that no rejected member reached a transport append.
    let before_groups = db.pipeline_stats().append_requests;
    let error = db
        .submit_many(&[
            (new_id, vec![new.clone()]),
            (fixture.operation(3, 4), vec![checkpoint_put(4, 4)]),
        ])
        .unwrap_err();
    assert!(error.to_string().contains("admission capacity"), "{error}");
    assert_eq!(
        db.core.next_sequence(),
        2,
        "no member of rejected window may allocate a slot"
    );
    assert_eq!(db.core.active_identities(), 1);
    assert_eq!(db.core.publisher.lock().known.len(), 1);
    assert_eq!(db.pipeline_stats().append_requests, before_groups);
    assert!(db.core.ensure_healthy().is_ok());

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
    assert_eq!(versions[2], versions[3]);
    assert_eq!(db.core.next_sequence(), 3);
    drop(active_held);
    db.drain().unwrap();
    assert_eq!(db.core.publisher.lock().known.len(), 3);
    assert_eq!(
        db.submit_many(&[
            (known_id, vec![known]),
            (active_id, vec![active]),
            (new_id, vec![new]),
        ])
        .unwrap(),
        versions[..3]
    );
    assert_eq!(
        db.core.next_sequence(),
        3,
        "retries consume no additional checkpoint capacity"
    );
}

#[test]
fn unchanged_checkpoint_is_a_noop_and_shutdown_clocks_are_retained() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let operation = fixture.operation(0, 200);
    let mutation = checkpoint_put(7, 42);
    let version = db.write_batch(operation, vec![mutation.clone()]).unwrap();
    db.flush_index().unwrap();
    let before = db.authority_write_stats();
    let generation = snapshot_generation(&db);
    let barriers = db.storage_stats().unwrap().persistence_barriers;
    db.flush_index().unwrap();
    assert_eq!(snapshot_generation(&db), generation);
    assert_eq!(
        db.authority_write_stats().checkpoint_count,
        before.checkpoint_count
    );
    assert_eq!(
        db.authority_write_stats().checkpoint_ns,
        before.checkpoint_ns
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
        db.authority_write_stats().checkpoint_count,
        before.checkpoint_count
    );
    db.shutdown().unwrap();
    assert_eq!(db.shutdown_stats().join_ns, closed.join_ns);
}

#[test]
fn failed_parallel_connect_does_not_reserve_or_escape_an_authority() {
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.endpoints[2].server_name = "wrong-peer.invalid".to_owned();
    let root = fixture.temp.path().join("failed-connect");
    assert!(
        RemoteDatabase::open(&root, &fixture.shape, config.clone(), recovery_metrics()).is_err()
    );
    let manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert!(manifest.generations.is_empty());
    assert_eq!(manifest.next_generation, 1);
    // The failed attempt's scoped connection workers are all gone before lock
    // acquisition can succeed. Keep the recorded immutable endpoint inventory.
    let lock = DbLock::acquire(&root, Duration::ZERO).unwrap();
    drop(lock);
    assert!(RemoteDatabase::open(&root, &fixture.shape, config, recovery_metrics()).is_err());
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
    let root = fixture.temp.path().join("authority");
    for existing in [false, true] {
        if existing {
            let db = fixture.open();
            db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(0, 7)])
                .unwrap();
            db.shutdown().unwrap();
        }
        let before = existing.then(|| fs::read(root.join(MANIFEST)).unwrap());
        for (requests, bytes) in [
            (0, fixture.config.max_pending_read_bytes),
            (1, ReadService::MIN_FLIGHT_BYTES - 1),
        ] {
            let mut config = fixture.config.clone();
            config.max_pending_reads = requests;
            config.max_pending_read_bytes = bytes;
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
    assert_eq!(db.read_rpc_count(), 1);
    db.shutdown().unwrap();
}

#[test]
fn parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries() {
    let mut fixture = Fixture::new_with_capacity(1024);
    // Freeze an explicit bound whose low watermark lies inside this replay
    // cohort; the test exercises refill/retry semantics, not an implicit default.
    fixture.config.replay_limits = ReplayLimits::legacy();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 900), vec![checkpoint_put(1, 7)])
        .unwrap();
    let old_cut = db.checkpoint().unwrap();
    db.flush_index().unwrap();
    // Keep the previously adopted root fixed while building a tail large enough
    // to exercise replay refills. Cancel the pending adoption before dropping.
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_adoption_pause = Some((entered, gate));
    let mut acknowledged = Vec::new();
    for round in 0..70u64 {
        for node in 0..5 {
            let operation = fixture.operation(node, 1000 + round * 5 + node as u64);
            let mutation = checkpoint_put(1, (round * 5 + node as u64) as u8);
            let version = db.write_batch(operation, vec![mutation.clone()]).unwrap();
            acknowledged.push((operation, mutation, version));
        }
    }
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(resume);
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    assert_eq!(db.recovery_stats().tail_batches, 350);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[349u16 as u8; 32]
    );
    assert_eq!(
        db.get_at(KeySpace(0), &1u32.to_be_bytes(), &old_cut)
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    for (operation, mutation, version) in acknowledged {
        assert_eq!(db.write_batch(operation, vec![mutation]).unwrap(), version);
    }
    assert_eq!(checkpoint_tail(&db).0, 0);
}

#[test]
fn sui_adapter_pages_reverse_bounds_snapshot_and_reopen() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let ks = db.ks("root");
    for first in (0..260u32).step_by(4) {
        let mut batch = db.write_batch();
        for key in first..first + 4 {
            batch.write(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32]);
        }
        batch.commit().unwrap();
    }
    let snapshot = db.checkpoint();
    db.remove(ks, 255u32.to_be_bytes().to_vec()).unwrap();
    db.insert(ks, 256u32.to_be_bytes().to_vec(), vec![99; 32])
        .unwrap();
    let mut forward = db.iterator(ks);
    forward.set_lower_bound(253u32.to_be_bytes().to_vec());
    forward.set_upper_bound(259u32.to_be_bytes().to_vec());
    let rows = forward.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|(k, _)| u32::from_be_bytes(k.as_ref().try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![253, 254, 256, 257, 258]
    );
    let mut reverse = snapshot.iterator(ks);
    reverse.set_lower_bound(253u32.to_be_bytes().to_vec());
    reverse.set_upper_bound(259u32.to_be_bytes().to_vec());
    reverse.reverse();
    let rows = reverse.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|(k, _)| u32::from_be_bytes(k.as_ref().try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![258, 257, 256, 255, 254, 253]
    );
    assert_eq!(rows[2].1.as_ref(), &[0; 32]);
    let before_scan_rpcs = db.remote_counters().unwrap().2;
    let full_rows = db.iterator(ks).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(full_rows.len(), 259);
    let scan_rpcs = db.remote_counters().unwrap().2 - before_scan_rpcs;
    assert!(
        scan_rpcs < 259,
        "iterator did not group scan reads: {scan_rpcs}"
    );
    assert_eq!(db.remote_counters().unwrap().0.inserts, 0);
    let before_short_scan = db.remote_counters().unwrap().2;
    let mut short = db.iterator(ks);
    assert!(short.next().unwrap().is_ok());
    drop(short);
    let short_scan_rpcs = db.remote_counters().unwrap().2 - before_short_scan;
    assert!(
        (1..=9).contains(&short_scan_rpcs),
        "short iterator exceeded its two-page grouped lookahead (4+8 references, five shards): {short_scan_rpcs}"
    );
    let mut timing = std::collections::BTreeMap::new();
    db.visit_timing_metrics(|name, value| {
        timing.insert(name, value);
    });
    assert!(timing.contains_key("write_durable_receipt_publication_blocked_batches"));
    assert!(timing.contains_key("write_durable_receipt_publication_waiting_batches"));
    assert!(timing.contains_key("write_durable_receipt_publication_waiting_peak"));
    assert!(timing.contains_key("write_index_checkpoint_cleaned"));
    assert!(timing.contains_key("coordinator_io_durable_cut_bytes"));
    assert!(!timing.contains_key("write_foreground_checkpoint_count"));
    assert!(!timing.contains_key("write_concurrent_checkpoint_wait_ns"));
    assert!(timing.contains_key("cache_inserts"));
    assert!(timing.contains_key("index_reader_held_view_bytes"));
    assert!(!timing.contains_key("read_cache_hits"));
    assert!(timing.contains_key("write_profile_expected_connections"));
    assert!(timing.contains_key("write_profile_captured_connections"));
    assert!(timing.contains_key("write_profile_sampled_requests"));
    assert!(!timing.contains_key("write_profile_storage_append_calls"));
    db.flush().unwrap();

    drop(db);
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    assert!(db.get(ks, &255u32.to_be_bytes()).unwrap().is_none());
    assert_eq!(
        db.get(ks, &256u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[99; 32]
    );
}

#[test]
fn sui_iterator_rechecks_authority_health_before_yielding_prefetched_rows() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in 0..4u32 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32])
            .unwrap();
    }

    let mut iterator = db.iterator(ks);
    assert!(iterator.next().unwrap().is_ok());
    db.remote_for_test().reads.poison();
    assert!(iterator.next().unwrap().is_err());
    assert!(iterator.next().is_none());
}

#[test]
fn sui_scan_filters_reduced_boundary_candidates_by_verified_full_key() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(4, 0..2),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let mut fixture = Fixture::new_with_shape(128, shape);
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    db.insert(ks, vec![0x12, 0x34, 0x50, 0], vec![0x50; 32])
        .unwrap();
    db.insert(ks, vec![0x12, 0x35, 0, 0], vec![0x35; 32])
        .unwrap();

    let mut forward = db.iterator(ks);
    forward.set_lower_bound(vec![0x12, 0x34, 0x60, 0]);
    let rows = forward.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_ref(), &[0x12, 0x35, 0, 0]);

    let mut reverse = db.iterator(ks);
    reverse.set_upper_bound(vec![0x12, 0x34, 0x40, 0]);
    reverse.reverse();
    assert!(reverse.next().is_none());
}
#[test]
fn sui_grouped_scan_keeps_order_while_one_storage_lane_is_stalled() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    let epoch = db.remote_for_test().epoch;
    let partition_for_sequence = |sequence: u64| {
        let mut operation = [0u8; 16];
        operation[..8].copy_from_slice(&epoch.to_le_bytes());
        operation[8..].copy_from_slice(&sequence.to_le_bytes());
        fixture
            .config
            .inventory(epoch)
            .partition_for(BatchId {
                database: fixture.config.storage.database,
                operation,
            })
            .unwrap()
    };
    db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![0; 32])
        .unwrap();
    let first_partition = partition_for_sequence(0);
    let other_sequence = (1..32)
        .find(|sequence| partition_for_sequence(*sequence) != first_partition)
        .unwrap();
    for sequence in 1..=other_sequence {
        let key = if sequence == other_sequence {
            1
        } else {
            100 + sequence as u32
        };
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32])
            .unwrap();
    }
    for key in [2u32, 3] {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32])
            .unwrap();
    }
    db.flush().unwrap();
    db.wait_for_background_threads_to_finish().unwrap();
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let cut = db.remote_for_test().published_cut().unwrap();
    let first_page = db
        .remote_for_test()
        .index_page(ks, None, false, cut)
        .unwrap();
    assert!(first_page.len() >= 4);
    let stalled_partition = first_page[0].1.batch_address.partition;
    let stalled_node = db.remote_for_test().partition_indexes[&stalled_partition];
    assert!(
        first_page[..4]
            .iter()
            .any(|(_, reference)| reference.batch_address.partition != stalled_partition)
    );
    let lane = db.remote_for_test().reads.lane_client(stalled_node, 0);
    let held = lane.lock();
    let (started, start_rx) = std::sync::mpsc::sync_channel(1);
    let (finished, finish_rx) = std::sync::mpsc::sync_channel(1);
    let reader = db.clone();
    let scan_rpcs_before = db.remote_for_test().read_rpc_count();
    let scan = std::thread::spawn(move || {
        started.send(()).unwrap();
        let rows = reader.iterator(ks).collect::<Result<Vec<_>, _>>();
        finished.send(rows).unwrap();
    });
    start_rx.recv().unwrap();
    let dispatch_deadline = Instant::now() + Duration::from_secs(5);
    while db.remote_for_test().read_rpc_count() == scan_rpcs_before
        && Instant::now() < dispatch_deadline
    {
        std::thread::yield_now();
    }
    assert!(db.remote_for_test().read_rpc_count() > scan_rpcs_before);
    let independent = first_page[..4]
        .iter()
        .find(|(_, reference)| reference.batch_address.partition != stalled_partition)
        .unwrap()
        .0
        .clone();
    assert_eq!(
        db.get(ks, &independent).unwrap().unwrap().as_ref(),
        &[independent[3]; 32]
    );
    assert!(matches!(
        finish_rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    drop(held);
    let rows = finish_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    scan.join().unwrap();
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

#[test]
fn sui_adapter_concurrent_reads_with_one_credit_and_atomic_batch_failure() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    let mut batch = db.write_batch();
    batch.write(ks, 1u32.to_be_bytes().to_vec(), vec![7; 32]);
    batch.write(ks, 2u32.to_be_bytes().to_vec(), vec![8; 32]);
    batch.commit().unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let db = &db;
            scope.spawn(move || {
                assert_eq!(
                    db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().as_ref(),
                    &[7; 32]
                );
                assert_eq!(
                    db.get(ks, &2u32.to_be_bytes()).unwrap().unwrap().as_ref(),
                    &[8; 32]
                );
            });
        }
    });
    let mut oversized = db.write_batch();
    for key in 1..=5u32 {
        oversized.write(ks, key.to_be_bytes().to_vec(), vec![9; 32]);
    }
    assert!(oversized.commit().is_err());
    assert_eq!(
        db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[7; 32]
    );
    assert!(db.get(ks, &5u32.to_be_bytes()).unwrap().is_none());
}

#[test]
fn sui_adapter_hash_enumeration_and_reduced_key_collisions() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    for indexing in [KeyIndexing::Hash, KeyIndexing::key_reduction(4, 0..2)] {
        let shape = KeyShape::new_single_config_indexing(
            indexing.clone(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let fixture = Fixture::new_with_shape(128, shape);
        let db = Db::open_remote_local_fixture(
            &fixture.temp.path().join("authority"),
            fixture.shape.clone(),
            fixture.config.clone(),
        )
        .unwrap();
        let ks = db.ks("root");
        let a = vec![1, 2, 3, 4];
        let b = vec![1, 2, 9, 9];
        db.insert(ks, a.clone(), vec![7]).unwrap();
        db.insert(ks, b.clone(), vec![8]).unwrap();
        let rows = db.iterator(ks).collect::<Result<Vec<_>, _>>().unwrap();
        if matches!(indexing, KeyIndexing::Hash) {
            assert_eq!(rows.len(), 2);
            assert_eq!(db.get(ks, &a).unwrap().unwrap().as_ref(), &[7]);
        } else {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0.as_ref(), b.as_slice());
            assert!(db.get(ks, &a).unwrap().is_none());
            assert!(!db.exists(ks, &a).unwrap());
        }
        assert_eq!(db.get(ks, &b).unwrap().unwrap().as_ref(), &[8]);
        db.remove(ks, b.clone()).unwrap();
        assert!(!db.exists(ks, &b).unwrap());
    }
}

#[test]
fn sui_adapter_variable_prefix_bounds_and_bulk_order() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::VariableLength,
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let mut fixture = Fixture::new_with_shape(128, shape);
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in [vec![], vec![1], vec![1, 0], vec![1, 255], vec![2]] {
        db.insert(ks, key.clone(), key).unwrap();
    }
    let mut iter = db.iterator(ks);
    iter.set_lower_bound(vec![1]);
    iter.set_upper_bound(vec![2]);
    iter.reverse();
    let keys = iter.map(|row| row.unwrap().0.to_vec()).collect::<Vec<_>>();
    assert_eq!(keys, vec![vec![1, 255], vec![1, 0], vec![1]]);
    let queries = vec![vec![2], vec![3], vec![1, 0], vec![], vec![2]];
    let values = db
        .multi_get(ks, &queries)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        values
            .into_iter()
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>(),
        vec![
            Some(vec![2]),
            None,
            Some(vec![1, 0]),
            Some(vec![]),
            Some(vec![2])
        ]
    );
}

#[test]
fn sui_cell_pruning_drops_more_keys_than_one_batch_admits() {
    // One DropCells batch replaces the bounded enumerate-and-delete batch, so
    // five keys drop although a batch admits four operations.
    use crate::distributed::sui::Db;
    let fixture = Fixture::new();
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in 0..5u32 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![7]).unwrap();
    }
    assert!(5 > fixture.config.storage.batch_limits.max_operations);
    db.drop_cells_in_range(ks, &[0; 4], &[255; 4]).unwrap();
    assert!(db.iterator(ks).next().is_none());
    for key in 0..5u32 {
        assert!(!db.exists(ks, &key.to_be_bytes()).unwrap());
    }
}

#[test]
fn sui_cell_pruning_uses_reduced_full_cell_range_and_survives_reopen() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let fixture = Fixture::new_with_shape(128, shape);
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let ks = db.ks("root");
    db.insert(ks, vec![0; 8], vec![7]).unwrap();
    db.drop_cells_in_range(ks, &[0, 0, 0, 0, 255, 255, 255, 255], &[255; 8])
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    drop(db);
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
}

#[test]
fn sui_native_cell_pruning_propagates_durability_barrier_failure() {
    use crate::distributed::sui::Db;
    let temp = tempdir::TempDir::new("sui-native-drop-barrier").unwrap();
    let path = temp.path().join("db");
    std::fs::create_dir(&path).unwrap();
    let moved = temp.path().join("moved");
    let mut config = crate::config::Config::small();
    config.snapshot_written_bytes = u64::MAX;
    let db = Db::open_native(
        &path,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        Arc::new(config),
        recovery_metrics(),
    )
    .unwrap();
    db.set_native_durable(true);
    let ks = db.ks("root");
    db.insert(ks, vec![0; 4], vec![7]).unwrap();
    // Registered WAL handles remain writable, but syncing their original
    // namespace must fail. A deletion may not acknowledge that failed barrier.
    std::fs::rename(&path, &moved).unwrap();
    let result = db.drop_cells_in_range(ks, &[0; 4], &[255; 4]);
    std::fs::rename(&moved, &path).unwrap();
    assert!(
        result.is_err(),
        "durable deletion must propagate the WAL namespace barrier failure"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn sui_native_cell_pruning_durable_deletion_survives_reopen() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let temp = tempdir::TempDir::new("sui-native-drop-reopen").unwrap();
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let mut config = crate::config::Config::small();
    config.snapshot_written_bytes = u64::MAX;
    let config = Arc::new(config);
    let db = Db::open_native(
        temp.path(),
        shape.clone(),
        config.clone(),
        recovery_metrics(),
    )
    .unwrap();
    db.set_native_durable(true);
    let ks = db.ks("root");
    db.insert(ks, vec![0; 8], vec![7]).unwrap();
    db.drop_cells_in_range(ks, &[0, 0, 0, 0, 255, 255, 255, 255], &[255; 8])
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    db.wait_for_background_threads_to_finish().unwrap();
    let db = Db::open_native(temp.path(), shape, config, recovery_metrics()).unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    db.wait_for_background_threads_to_finish().unwrap();
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
    // Native owners acknowledge before sync. The position root, not the
    // separately adopted receipt frontier, must cover a committed checkpoint.
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
    let authority = fixture.temp.path().join("authority");
    let observer_log = Arc::clone(&log);
    let observer = std::thread::spawn(move || {
        // The owner's sync for the checkpoint proof is now held.
        entered.recv_timeout(Duration::from_secs(30)).unwrap();
        let manifest: AuthorityManifest = read_metadata(&authority.join(MANIFEST)).unwrap();
        let held = (
            manifest.checkpoint.is_none(),
            observer_log.position_frontier_for_test() < acknowledged_end,
        );
        release.send(()).unwrap();
        held
    });
    db.flush_index().unwrap();
    let (no_image_while_stalled, still_unsynced) = observer.join().unwrap();
    assert!(
        no_image_while_stalled,
        "an image was committed before the sync"
    );
    assert!(still_unsynced, "the owner had already synced the batch");
    // After the sync completed the image exists and the owner covers its prefix.
    assert!(log.position_frontier_for_test() >= acknowledged_end);
    let manifest: AuthorityManifest =
        read_metadata(&fixture.temp.path().join("authority").join(MANIFEST)).unwrap();
    assert!(manifest.checkpoint.is_some());
    db.shutdown().unwrap();
}
