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

pub(super) fn fail_stop_without_checkpoint(mut db: RemoteDatabase) {
    // A worker may durably adopt its captured root during drop. No explicit
    // checkpoint is requested here; recovery must accept either valid root.
    db.healthy = false;
    drop(db);
}

pub(super) fn reopen_result(fixture: &Fixture) -> Result<RemoteDatabase, LocalDbError> {
    RemoteDatabase::open(
        &fixture.temp.path().join("authority"),
        &fixture.shape,
        fixture.config.clone(),
        recovery_metrics(),
    )
}
#[test]
fn read_lanes_default_to_parallelism_and_validate_bounds() {
    let fixture = Fixture::new();
    let mut serialized = serde_json::to_value(&fixture.config).unwrap();
    serialized.as_object_mut().unwrap().remove("read_lanes");
    let mut config: RemoteDatabaseConfig = serde_json::from_value(serialized).unwrap();
    assert_eq!(config.read_lanes, crate::parallelism::available());
    for lanes in [1, 1024] {
        config.read_lanes = lanes;
        config.validate().unwrap();
    }
    for lanes in [0, 1025] {
        config.read_lanes = lanes;
        assert!(config.validate().is_err());
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
    persist_manifest(&root, &manifest).unwrap();

    let error = reopen_result(&fixture).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("authority configuration/history differs")
    );
    let after: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(after.epochs.len(), manifest.epochs.len());
}

#[test]
fn empty_owner_identity_map_preserves_legacy_manifest_bytes() {
    // Exact field order and representations of the pre-owner-map manifest.
    #[derive(Serialize, Deserialize)]
    struct LegacyManifest {
        format: u32,
        storage: LocalInventoryConfig,
        history_persistence: HistoryPersistence,
        endpoints: Vec<RemoteEndpoint>,
        shape_yaml: String,
        replay_limits: ReplayLimits,
        live_nvme_identity: Option<(String, [u8; 16])>,
        epochs: Vec<AuthorityEpoch>,
        checkpoint: Option<CheckpointCommit>,
        checkpoint_staging: Option<CheckpointFiles>,
        checkpoint_obsolete: Option<CheckpointCommit>,
    }
    let f = Fixture::new();
    let db = f.open();
    let manifest = &db.manifest;
    let legacy = LegacyManifest {
        format: manifest.format,
        storage: manifest.storage.clone(),
        history_persistence: manifest.history_persistence,
        endpoints: manifest.endpoints.clone(),
        shape_yaml: manifest.shape_yaml.clone(),
        replay_limits: manifest.replay_limits,
        live_nvme_identity: manifest.live_nvme_identity.clone(),
        epochs: manifest.epochs.clone(),
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

#[test]
fn sui_read_evidence_samples_configured_owners_at_boundaries() {
    use crate::distributed::sui::Db;

    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    let key = 11u32.to_be_bytes();
    db.insert(ks, key.to_vec(), vec![42; 32]).unwrap();
    let monitor = db.read_evidence_monitor().unwrap();
    let before = monitor.sample_owner_read_io().unwrap();
    assert_eq!(
        before
            .iter()
            .map(|(partition, _)| *partition)
            .collect::<Vec<_>>(),
        fixture
            .config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.partition)
            .collect::<Vec<_>>()
    );
    assert_eq!(db.get(ks, &key).unwrap().unwrap().as_ref(), &[42; 32]);
    let after = monitor.sample_owner_read_io().unwrap();
    assert_eq!(
        after
            .iter()
            .map(|(partition, _)| *partition)
            .collect::<Vec<_>>(),
        before
            .iter()
            .map(|(partition, _)| *partition)
            .collect::<Vec<_>>()
    );
    assert!(
        after
            .iter()
            .map(|(_, stats)| stats.completed_reads)
            .sum::<u64>()
            > before
                .iter()
                .map(|(_, stats)| stats.completed_reads)
                .sum::<u64>()
    );
    drop(monitor);
    db.wait_for_background_threads_to_finish().unwrap();
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[test]
fn benchmark_owner_lookup_uses_authoritative_reference_and_reports_absence() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
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
    checkpoint_restores_overwrite_delete_tail_and_original_retry_receipts_mode(
        HistoryPersistence::OnDisk,
    );
}
#[test]
fn wal_replay_checkpoint_restores_overwrite_delete_tail_and_original_retry_receipts() {
    checkpoint_restores_overwrite_delete_tail_and_original_retry_receipts_mode(
        HistoryPersistence::WalReplay,
    );
}
fn checkpoint_restores_overwrite_delete_tail_and_original_retry_receipts_mode(
    mode: HistoryPersistence,
) {
    let fixture = Fixture::new().history(mode);
    let mut db = fixture.open();
    let operation = fixture.operation(0, 1);
    let original = checkpoint_put(1, 1);
    let version = db
        .write_batch(operation, vec![original.clone(), checkpoint_put(2, 2)])
        .unwrap();
    db.flush_index().unwrap();
    let generation = db.snapshot_generation;
    db.flush_index().unwrap();
    assert_eq!(
        db.snapshot_generation, generation,
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
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.flush_index().unwrap();
        let committed = db.manifest.checkpoint.as_ref().unwrap().metadata_name();
        db.write_batch(fixture.operation(1, 2), vec![checkpoint_put(1, 2)])
            .unwrap();
        db.checkpoint_interruption = Some(stage);
        assert!(db.flush_index().is_err());
        let persisted: AuthorityManifest = read_metadata(&db.path.join(MANIFEST)).unwrap();
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
                !db.path.join(name).exists(),
                "reserved interrupted output must be retired"
            );
        }
        assert!(db.manifest.checkpoint_staging.is_none());
        assert!(db.manifest.checkpoint_obsolete.is_none());
    }
}

#[test]
fn checkpoint_is_not_adopted_until_every_required_shard_is_fenced() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
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
    assert_eq!(manifest.epochs.last().unwrap().epoch, 2);
    assert!(!manifest.epochs.last().unwrap().active);
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

fn replace_checkpoint_metadata(root: &Path, change: impl FnOnce(&mut CheckpointImage)) {
    let mut manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    let commit = manifest.checkpoint.as_mut().unwrap();
    let name = commit.metadata_name();
    let mut image = read_checkpoint(&root.join(&name)).unwrap();
    change(&mut image);
    let encoded = encode_checkpoint(&image).unwrap();
    commit.metadata_bytes = encoded.len() as u64;
    commit.metadata_digest = Blake2b::<U32>::digest(&encoded).into();
    install_metadata(root, &name, &encoded).unwrap();
    persist_manifest(root, &manifest).unwrap();
}

#[test]
fn committed_checkpoint_corruption_missing_files_and_wrong_prefixes_fail_closed() {
    for damage in 0..7 {
        let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.shutdown().unwrap();
        let root = db.path.clone();
        let commit = db.manifest.checkpoint.clone().unwrap();
        drop(db);
        match damage {
            0 => fs::remove_file(root.join(commit.index_name())).unwrap(),
            1 => fs::write(root.join(commit.index_name()), b"corrupt image").unwrap(),
            2 => fs::remove_file(root.join(commit.metadata_name())).unwrap(),
            3 => fs::write(root.join(commit.metadata_name()), b"corrupt metadata").unwrap(),
            4 => replace_checkpoint_metadata(&root, |image| image.database[0] ^= 1),
            5 => replace_checkpoint_metadata(&root, |image| {
                image.streams.pop();
            }),
            6 => replace_checkpoint_metadata(&root, |image| {
                let stream = image
                    .streams
                    .iter_mut()
                    .find(|stream| stream.proof.prefix != 0)
                    .unwrap();
                let mut history =
                    ReceiptHistory::open(&root, fixture.config.storage.database).unwrap();
                let mut receipt = history
                    .receipt(stream.proof.partition, stream.proof.epoch, 0)
                    .unwrap();
                receipt.address.offset += 8;
                // Forge mutually consistent local cold authority and its digest.
                // The independent authenticated shard must still reject it.
                history.replace_single_receipt_for_test(receipt).unwrap();
                image.history_root = history.root;
                stream.state = history
                    .state(stream.proof.partition, stream.proof.epoch)
                    .unwrap();
                stream.proof.digest = history
                    .prefix(stream.proof.partition, stream.proof.epoch, 1)
                    .unwrap();
            }),
            _ => unreachable!(),
        }
        let error = reopen_result(&fixture)
            .err()
            .expect("corrupt checkpoint must not fall back");
        if damage == 6 {
            assert!(
                error
                    .to_string()
                    .contains("fenced shard checkpoint proof differs")
            );
        }
    }
}

#[test]
fn automatic_checkpoint_admission_keeps_crash_replay_bounded() {
    automatic_checkpoint_admission_keeps_crash_replay_bounded_mode(HistoryPersistence::OnDisk);
}
#[test]
fn wal_replay_automatic_checkpoint_admission_keeps_crash_replay_bounded() {
    automatic_checkpoint_admission_keeps_crash_replay_bounded_mode(HistoryPersistence::WalReplay);
}
fn automatic_checkpoint_admission_keeps_crash_replay_bounded_mode(mode: HistoryPersistence) {
    let mut fixture = Fixture::new_with_capacity(40).history(mode);
    fixture.config.replay_limits = ReplayLimits {
        max_batches: 32,
        max_encoded_bytes: 1024 * 1024,
    };
    let replay = mode == HistoryPersistence::WalReplay;
    if replay {
        fixture.config.max_admitted_batches = 1024;
    }
    let limits = fixture.config.replay_limits;
    assert!(checkpoint_metadata_fits(
        &fixture.config,
        limits.max_batches + 8
    ));
    assert!(!checkpoint_metadata_fits(&fixture.config, usize::MAX));
    assert!(!checkpoint_metadata_fits(
        &fixture.config,
        MAX_CHECKPOINT_METADATA_BYTES as usize / 128
    ));
    let mut db = fixture.open();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    if replay {
        db.checkpoint_pause = Some((entered, gate));
    }
    let mut acknowledged = Vec::new();
    let count = if replay {
        4 * limits.max_batches
    } else {
        limits.max_batches + 3
    };
    for n in 0..count {
        let operation = fixture.operation((n % 5) as u32, n as u64);
        let value = n as u8;
        let version = db
            .write_batch(operation, vec![checkpoint_put(1, value)])
            .unwrap();
        acknowledged.push((operation, value, version));
        if !replay {
            assert!(db.checkpoint_tail_batches <= limits.max_batches);
            assert!(db.checkpoint_tail_bytes <= limits.max_encoded_bytes);
        }
    }
    if replay {
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(db.authority_write_stats().foreground_checkpoint_count, 0);
        assert_eq!(db.checkpoint_tail_batches, count);
        assert!(!db.path.join("receipt-history-v2").exists());
    } else {
        assert!(db.manifest.checkpoint.is_some());
    }
    let retained_tail = db.checkpoint_tail_batches;
    let clocks = db.authority_write_stats();
    assert!(clocks.checkpoint_count > 0);
    assert!(clocks.foreground_checkpoint_count <= clocks.checkpoint_count);
    if clocks.foreground_checkpoint_count == 0 {
        assert_eq!(clocks.foreground_checkpoint_ns, 0);
    } else {
        assert!(clocks.foreground_checkpoint_ns > 0);
    }
    drop(resume);
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    if replay {
        assert_eq!(db.recovery_stats().tail_batches, count as u64);
    }
    assert!(db.recovery_stats().tail_batches <= retained_tail as u64);
    if !replay {
        assert!(db.recovery_stats().tail_batches <= limits.max_batches as u64);
        assert!(db.recovery_stats().tail_bytes <= limits.max_encoded_bytes as u64);
    }
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
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    let reference = db
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
    // The live node's receipt directory still authenticates metadata. Unlike
    // full replay, checkpoint adoption does not scrub old value bytes eagerly.
    let mut db = fixture.open();
    assert!(db.recovery_stats().checkpoint_loaded);
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert!(
        db.get(KeySpace(0), &9u32.to_be_bytes()).is_err(),
        "failed authority must not report absence"
    );
}

#[test]
fn checkpoint_retirement_bounds_files_and_preserves_pinned_immutable_index() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let first = db.manifest.checkpoint.clone().unwrap();
    let pinned = NativeIndex::open_snapshot(
        &db.path.join(first.index_name()),
        &fixture.shape,
        fixture.config.storage.database,
        fixture.config.max_index_bytes,
    )
    .unwrap();
    let reference = pinned
        .get(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap();
    for n in 2..6 {
        let previous = db.manifest.checkpoint.clone().unwrap();
        db.write_batch(fixture.operation(1, n), vec![checkpoint_put(1, n as u8)])
            .unwrap();
        db.flush_index().unwrap();
        assert!(!db.path.join(previous.index_name()).exists());
        assert!(!db.path.join(previous.metadata_name()).exists());
        assert!(
            db.path
                .join(db.manifest.checkpoint.as_ref().unwrap().index_name())
                .is_file()
        );
        assert!(db.manifest.checkpoint_obsolete.is_none());
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
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.flush_index().unwrap();
        let previous = db.manifest.checkpoint.clone().unwrap();
        db.write_batch(fixture.operation(1, 2), vec![checkpoint_put(1, 2)])
            .unwrap();
        db.checkpoint_interruption = Some(3);
        let error = db.flush_index().unwrap_err();
        assert!(error.to_string().contains("checkpoint committed"));
        let persisted: AuthorityManifest = read_metadata(&db.path.join(MANIFEST)).unwrap();
        assert_eq!(
            persisted.checkpoint.unwrap().generation,
            previous.generation + 1
        );
        assert_eq!(
            persisted.checkpoint_obsolete.unwrap().generation,
            previous.generation
        );
        if partially_unlinked {
            fs::remove_file(db.path.join(previous.index_name())).unwrap();
        }
        drop(db);
        let mut db = fixture.open();
        assert!(db.recovery_stats().checkpoint_loaded);
        assert_eq!(db.recovery_stats().tail_batches, 0);
        assert!(!db.path.join(previous.index_name()).exists());
        assert!(!db.path.join(previous.metadata_name()).exists());
        assert!(db.manifest.checkpoint_obsolete.is_none());
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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let previous = db.manifest.checkpoint.clone().unwrap();
    let metadata = db.path.join(previous.metadata_name());
    let saved = db.path.join("saved-old-metadata");
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
    assert!(!db.healthy);
    let persisted: AuthorityManifest = read_metadata(&db.path.join(MANIFEST)).unwrap();
    assert_eq!(
        persisted.checkpoint.unwrap().generation,
        previous.generation + 1
    );
    assert!(persisted.checkpoint_obsolete.is_some());
    assert!(
        db.path.join(previous.index_name()).exists(),
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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let unrelated = db.path.join(format!(
        "index-{}-{}.snapshot",
        db.epoch,
        db.snapshot_generation + 1
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
    assert!(db.manifest.checkpoint_staging.is_none());
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
    let mut db = fixture.open();
    db.config.pipelined_writes = false;
    let known_id = fixture.operation(0, 1);
    let active_id = fixture.operation(1, 2);
    let new_id = fixture.operation(2, 3);
    let known = checkpoint_put(1, 1);
    let active = checkpoint_put(2, 2);
    let new = checkpoint_put(3, 3);
    let known_version = db.write_batch(known_id, vec![known.clone()]).unwrap();
    let active_version = db.submit(active_id, vec![active.clone()]).unwrap();
    db.checkpoint_receipt_limit = Some(3); // Exactly one distinct new receipt fits.
    let before_groups = db.storage_stats().unwrap().appended_groups;
    let error = db
        .submit_many(&[
            (new_id, vec![new.clone()]),
            (fixture.operation(3, 4), vec![checkpoint_put(4, 4)]),
        ])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checkpoint receipt metadata admission capacity")
    );
    assert_eq!(
        db.submission_next(),
        2,
        "no member of rejected window may allocate a slot"
    );
    assert_eq!(db.active_identities.len(), 1);
    assert_eq!(db.known.len(), 1);
    assert_eq!(db.storage_stats().unwrap().appended_groups, before_groups);
    assert!(db.healthy);

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
    assert_eq!(db.submission_next(), 3);
    db.drain().unwrap();
    assert_eq!(db.known.len(), 3);
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
        db.submission_next(),
        3,
        "retries consume no additional checkpoint capacity"
    );
}

#[test]
fn unchanged_checkpoint_is_a_noop_and_shutdown_clocks_are_retained() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let operation = fixture.operation(0, 200);
    let mutation = checkpoint_put(7, 42);
    let version = db.write_batch(operation, vec![mutation.clone()]).unwrap();
    db.flush_index().unwrap();
    let before = db.authority_write_stats();
    let generation = db.snapshot_generation;
    let barriers = db.storage_stats().unwrap().persistence_barriers;
    db.flush_index().unwrap();
    assert_eq!(db.snapshot_generation, generation);
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
    assert!(db.clients.is_empty());
    assert!(db.preparation_pool.is_none());
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
    assert!(manifest.epochs.is_empty());
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
        RemoteDatabase::open_inner(
            &root,
            &fixture.shape,
            fixture.config.clone(),
            None,
            Some(OpenInterruption::AfterReadSetup)
        )
        .is_err()
    );
    let manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(manifest.epochs.len(), 1);
    assert!(!manifest.epochs[0].active);
    let mut db = fixture.open();
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn live_frame_can_fill_one_fragment() {
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.storage.fragment_bytes = 4096;
    config.storage.wal_file_bytes = 8192;
    config.storage.group_limits.max_encoded_bytes = 4088;
    config.live_nvme = Some(super::super::tcp::LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        export_mode: Default::default(),
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
        write_mode: super::super::tcp::LiveWriteMode::Reserved,
        leased_durability: None,
        inline_reads: true,
        queue_geometry: Default::default(),
    });
    assert!(config.validate().is_ok());
    assert_eq!(
        config.effective_owner_append_occupancy(),
        config.owner_append_occupancy
    );
    config.owner_append_occupancy = 8;
    let live = config.live_nvme.as_mut().unwrap();
    live.write_mode = super::super::tcp::LiveWriteMode::Leased;
    live.leased_durability = Some(super::super::durability::DurabilityTier::Native);
    live.max_inflight_requests = 32;
    live.leased_write_window = None;
    assert!(config.validate().is_ok());
    assert_eq!(config.effective_owner_append_occupancy(), 31);
    config.live_nvme.as_mut().unwrap().leased_write_window = Some(9);
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("leased write window exceeds owner append occupancy")
    );
    config.live_nvme.as_mut().unwrap().leased_write_window = Some(8);
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
            let mut db = fixture.open();
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
fn preparation_pool_drop_joins_scheduled_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let completed = Arc::new(AtomicUsize::new(0));
    let pool = PreparationPool::build(2).unwrap();
    let done = completed.clone();
    pool.pool.as_ref().unwrap().spawn(move || {
        done.fetch_add(1, Ordering::SeqCst);
    });
    drop(pool);
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[test]
fn parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries() {
    parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries_mode(
        HistoryPersistence::OnDisk,
    );
}
#[test]
fn wal_replay_parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries() {
    parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries_mode(
        HistoryPersistence::WalReplay,
    );
}
fn parallel_replay_refills_keep_one_bounded_ordered_merge_and_exact_retries_mode(
    mode: HistoryPersistence,
) {
    let mut fixture = Fixture::new_with_capacity(1024).history(mode);
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
    db.checkpoint_adoption_pause = Some((entered, gate));
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
    assert_eq!(db.checkpoint_tail_batches, 0);
}

#[test]
fn sui_adapter_pages_reverse_bounds_snapshot_and_reopen() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
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
    assert!(timing.contains_key("write_foreground_checkpoint_count"));
    assert!(timing.contains_key("cache_inserts"));
    assert!(!timing.contains_key("read_cache_hits"));
    assert!(timing.contains_key("write_profile_expected_connections"));
    assert!(timing.contains_key("write_profile_captured_connections"));
    assert!(timing.contains_key("write_profile_sampled_requests"));
    assert!(!timing.contains_key("write_profile_storage_append_calls"));
    db.flush().unwrap();

    drop(db);
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
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
    let db = Db::open_remote(
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
    let db = Db::open_remote(
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
    fixture.config.read_lanes = 1;
    let db = Db::open_remote(
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
    let db = Db::open_remote(
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
    let db = Db::open_remote(
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
        let db = Db::open_remote(
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
    let db = Db::open_remote(
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
fn sui_cell_pruning_rejects_oversized_atomic_range_without_partial_deletion() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new();
    let db = Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in 0..5u32 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![7]).unwrap();
    }
    assert!(db.drop_cells_in_range(ks, &[0; 4], &[255; 4]).is_err());
    for key in 0..5u32 {
        assert!(db.exists(ks, &key.to_be_bytes()).unwrap());
    }
    db.remove(ks, 4u32.to_be_bytes().to_vec()).unwrap();
    db.drop_cells_in_range(ks, &[0; 4], &[255; 4]).unwrap();
    assert!(db.iterator(ks).next().is_none());
}

fn sui_pruning_shape() -> KeyShape {
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        "transactions",
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|_, _| crate::Decision::Remove),
    );
    builder.add_key_space_config(
        "effects",
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|_, value: &[u8]| match value[0] {
            0 => crate::Decision::Remove,
            1 => crate::Decision::StopRelocation,
            _ => crate::Decision::Keep,
        }),
    );
    builder.build()
}

#[test]
fn sui_relocation_respects_operation_and_encoded_byte_limits() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
    for compact in [false, true] {
        for byte_limited in [false, true] {
            let key_size = if byte_limited { 1024 } else { 4 };
            let key_bytes = |number: u32| {
                let mut key = vec![0; key_size];
                key[..4].copy_from_slice(&number.to_be_bytes());
                key
            };
            let mut builder = KeyShapeBuilder::new();
            let mut config =
                KeySpaceConfig::new().with_relocation_filter(|_, _| crate::Decision::Remove);
            if compact {
                config = config.with_compactor(Box::new(|keys| {
                    keys.next_back().cloned().into_iter().collect()
                }));
            }
            builder.add_key_space_config("transactions", key_size, 1, KeyType::uniform(1), config);
            builder.add_key_space_config(
                "effects",
                4,
                1,
                KeyType::uniform(1),
                KeySpaceConfig::new()
                    .with_relocation_filter(|_, _| crate::Decision::StopRelocation),
            );
            let fixture = Fixture::new_with_shape(128, builder.build());
            let tx = fixture
                .shape
                .iter_ks()
                .find(|ks| ks.name() == "transactions")
                .unwrap()
                .id();
            let db = Db::open_remote(
                &fixture.temp.path().join("authority"),
                fixture.shape.clone(),
                fixture.config.clone(),
            )
            .unwrap();
            for key in 0..5u32 {
                db.insert(tx, key_bytes(key), vec![0]).unwrap();
            }
            db.insert(db.ks("effects"), vec![0; 4], vec![1]).unwrap();
            db.insert(tx, key_bytes(99), vec![0]).unwrap();
            db.start_relocation().unwrap();
            for key in 0..5u32 {
                assert!(!db.exists(tx, &key_bytes(key)).unwrap());
            }
            assert!(db.exists(tx, &key_bytes(99)).unwrap());
            assert!(db.exists(db.ks("effects"), &[0; 4]).unwrap());
        }
    }
}

#[test]
fn sui_relocation_protects_unconditional_remove_after_other_keyspace_stop() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new_with_shape(128, sui_pruning_shape());
    let db = Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    db.insert(db.ks("effects"), vec![0; 4], vec![1]).unwrap();
    db.insert(db.ks("transactions"), vec![0; 4], vec![0])
        .unwrap();
    db.start_relocation().unwrap();
    assert!(db.exists(db.ks("transactions"), &[0; 4]).unwrap());
    assert!(db.exists(db.ks("effects"), &[0; 4]).unwrap());
}

#[test]
fn sui_relocation_protects_entire_atomic_batch_at_stop() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new_with_shape(128, sui_pruning_shape());
    let db = Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let mut batch = db.write_batch();
    batch.write(db.ks("transactions"), vec![0; 4], vec![0]);
    batch.write(db.ks("effects"), vec![0; 4], vec![1]);
    batch.commit().unwrap();
    db.start_relocation().unwrap();
    assert!(db.exists(db.ks("transactions"), &[0; 4]).unwrap());
    assert!(db.exists(db.ks("effects"), &[0; 4]).unwrap());
}

#[test]
fn sui_relocation_deletes_eligible_rows_but_keeps_rows_across_keyspaces() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new_with_shape(128, sui_pruning_shape());
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
    let tx = db.ks("transactions");
    let effects = db.ks("effects");
    db.insert(tx, vec![0; 4], vec![0]).unwrap();
    db.insert(effects, vec![0; 4], vec![0]).unwrap();
    db.insert(effects, vec![1; 4], vec![2]).unwrap();
    db.insert(effects, vec![2; 4], vec![1]).unwrap();
    db.insert(tx, vec![1; 4], vec![0]).unwrap();
    db.start_relocation().unwrap();
    assert!(!db.exists(tx, &[0; 4]).unwrap());
    assert!(!db.exists(effects, &[0; 4]).unwrap());
    assert!(db.exists(effects, &[1; 4]).unwrap());
    assert!(db.exists(effects, &[2; 4]).unwrap());
    assert!(db.exists(tx, &[1; 4]).unwrap());
    drop(db);
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
    assert!(!db.exists(tx, &[0; 4]).unwrap());
    assert!(!db.exists(effects, &[0; 4]).unwrap());
    assert!(db.exists(tx, &[1; 4]).unwrap());
}

#[test]
fn sui_relocation_consults_overwritten_and_deleted_stop_history_after_reopen() {
    use crate::distributed::sui::Db;
    for delete in [false, true] {
        let fixture = Fixture::new_with_shape(128, sui_pruning_shape());
        let path = fixture.temp.path().join("authority");
        let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
        let tx = db.ks("transactions");
        let effects = db.ks("effects");
        db.insert(tx, vec![0; 4], vec![0]).unwrap();
        db.insert(effects, vec![0; 4], vec![1]).unwrap();
        db.insert(tx, vec![1; 4], vec![0]).unwrap();
        if delete {
            db.remove(effects, vec![0; 4]).unwrap();
        } else {
            db.insert(effects, vec![0; 4], vec![0]).unwrap();
        }
        db.flush().unwrap();
        drop(db);
        let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
        db.start_relocation().unwrap();
        assert!(!db.exists(tx, &[0; 4]).unwrap());
        assert!(
            db.exists(tx, &[1; 4]).unwrap(),
            "historical Stop must survive overwrite/delete and checkpoint reload"
        );
        assert_eq!(db.exists(effects, &[0; 4]).unwrap(), !delete);
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
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
    let ks = db.ks("root");
    db.insert(ks, vec![0; 8], vec![7]).unwrap();
    db.drop_cells_in_range(ks, &[0, 0, 0, 0, 255, 255, 255, 255], &[255; 8])
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    drop(db);
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
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
    assert!(db.read_evidence_monitor().is_none());
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
fn sui_relocation_compactor_keeps_cell_semantics_and_historical_stop() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        "effects",
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new()
            .with_relocation_filter(|_, _| crate::Decision::StopRelocation)
            .with_compactor(Box::new(|keys| {
                keys.next_back().cloned().into_iter().collect()
            })),
    );
    builder.add_key_space_config(
        "transactions",
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|_, _| crate::Decision::Remove),
    );
    let fixture = Fixture::new_with_shape(128, builder.build());
    let db = Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let effects = db.ks("effects");
    let tx = db.ks("transactions");
    db.insert(effects, vec![0; 4], vec![1]).unwrap();
    db.insert(tx, vec![0; 4], vec![0]).unwrap();
    db.insert(effects, vec![1; 4], vec![1]).unwrap();
    // The earliest stop is in a mapped block; the later versions are overlay.
    db.flush().unwrap();
    db.insert(effects, vec![2; 4], vec![1]).unwrap();
    db.start_relocation().unwrap();
    assert!(!db.exists(effects, &[0; 4]).unwrap());
    assert!(!db.exists(effects, &[1; 4]).unwrap());
    assert!(db.exists(effects, &[2; 4]).unwrap());
    assert!(db.exists(tx, &[0; 4]).unwrap());
    // Compaction's tombstones must not hide the old stop on the next pass.
    db.start_relocation().unwrap();
    assert!(db.exists(tx, &[0; 4]).unwrap());
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
fn sui_relocation_retains_stop_overwritten_within_atomic_batch() {
    use crate::distributed::sui::Db;
    for checkpoint in [false, true] {
        let fixture = Fixture::new_with_shape(128, sui_pruning_shape());
        let path = fixture.temp.path().join("authority");
        let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
        let tx = db.ks("transactions");
        let effects = db.ks("effects");
        let mut batch = db.write_batch();
        batch.write(tx, vec![0; 4], vec![0]);
        batch.write(effects, vec![0; 4], vec![1]);
        batch.write(effects, vec![0; 4], vec![0]);
        batch.commit().unwrap();
        let db = if checkpoint {
            db.flush().unwrap();
            drop(db);
            Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap()
        } else {
            db
        };
        db.start_relocation().unwrap();
        assert!(db.exists(tx, &[0; 4]).unwrap());
        assert_eq!(db.get(effects, &[0; 4]).unwrap().unwrap().as_ref(), &[0]);
    }
}

#[test]
fn recorded_replay_limits_reject_shrink_before_fencing_and_allow_recovered_growth() {
    let mut fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    fixture.config.replay_limits = ReplayLimits {
        max_batches: 32,
        max_encoded_bytes: 1024 * 1024,
    };
    let mut db = fixture.open();
    let operation = fixture.operation(0, 80);
    let version = db
        .write_batch(operation, vec![checkpoint_put(1, 7)])
        .unwrap();
    assert!(db.manifest.checkpoint.is_none());
    fail_stop_without_checkpoint(db);
    let root = fixture.temp.path().join("authority");
    let before: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    for limits in [
        ReplayLimits {
            max_batches: 31,
            max_encoded_bytes: 1024 * 1024,
        },
        ReplayLimits {
            max_batches: 32,
            max_encoded_bytes: 1024 * 1024 - 1,
        },
    ] {
        fixture.config.replay_limits = limits;
        let error = reopen_result(&fixture).err().unwrap();
        assert!(error.to_string().contains("cannot shrink"));
        let after: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
        assert_eq!(after.epochs.len(), before.epochs.len());
        assert_eq!(after.replay_limits, before.replay_limits);
    }
    fixture.config.replay_limits = ReplayLimits {
        max_batches: 64,
        max_encoded_bytes: 2 * 1024 * 1024,
    };
    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 1);
    assert_eq!(
        reopened
            .write_batch(operation, vec![checkpoint_put(1, 7)])
            .unwrap(),
        version
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    let activated: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(activated.replay_limits, fixture.config.replay_limits);
    assert!(activated.epochs.last().unwrap().active);
}

#[test]
fn increased_replay_configuration_cannot_relax_the_recorded_recovery_check() {
    let mut fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    fixture.config.replay_limits = ReplayLimits {
        max_batches: 32,
        max_encoded_bytes: 1024 * 1024,
    };
    let mut db = fixture.open();
    for n in 0..3 {
        db.write_batch(
            fixture.operation(n, 90 + n as u64),
            vec![checkpoint_put(1, n as u8)],
        )
        .unwrap();
    }
    assert!(db.manifest.checkpoint.is_none());
    fail_stop_without_checkpoint(db);
    let root = fixture.temp.path().join("authority");
    let mut recorded: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    // Simulate a validly framed but inconsistent durable bound. The larger
    // requested configuration must not legitimize an impossible old suffix.
    recorded.replay_limits.max_batches = 1;
    persist_manifest(&root, &recorded).unwrap();
    fixture.config.replay_limits.max_batches = 64;
    let error = reopen_result(&fixture).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("tail exceeds committed admission bound")
    );
    let after: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(after.replay_limits, recorded.replay_limits);
    assert!(!after.epochs.last().unwrap().active);
}

#[test]
fn legacy_authority_without_explicit_limits_replays_before_format_upgrade() {
    let mut fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    fixture.config.replay_limits = ReplayLimits::legacy();
    let mut db = fixture.open();
    let operation = fixture.operation(2, 100);
    let version = db
        .write_batch(operation, vec![checkpoint_put(1, 9)])
        .unwrap();
    fail_stop_without_checkpoint(db);
    let root = fixture.temp.path().join("authority");
    let mut old: serde_yaml::Value = read_metadata(&root.join(MANIFEST)).unwrap();
    let fields = old.as_mapping_mut().unwrap();
    fields.insert("format".into(), 2u32.into());
    fields.remove(serde_yaml::Value::from("replay_limits"));
    fields.remove(serde_yaml::Value::from("live_nvme_identity"));
    fields.remove(serde_yaml::Value::from("history_persistence"));
    install_metadata(&root, MANIFEST, &encode_metadata(&old).unwrap()).unwrap();
    let restored: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(restored.replay_limits, ReplayLimits::legacy());
    assert!(restored.live_nvme_identity.is_none());
    fixture.config.replay_limits = ReplayLimits::default();
    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 1);
    assert_eq!(
        reopened
            .write_batch(operation, vec![checkpoint_put(1, 9)])
            .unwrap(),
        version
    );
    let upgraded: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(upgraded.format, 4);
    assert_eq!(upgraded.replay_limits, fixture.config.replay_limits);
}

#[test]
fn native_checkpoint_commits_no_image_while_an_owner_sync_is_stalled() {
    // Native owners acknowledge before sync. The position root, not the
    // separately adopted receipt frontier, must cover a committed checkpoint.
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let epoch = db.epoch();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    let log = fixture.nodes[0].log_for_test(epoch);
    let acknowledged_end = log.appended_frontier();
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
