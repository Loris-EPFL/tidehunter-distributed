//! Exact -> per-cell migration over real five-node authenticated WAL storage
//! (spec §10.3, §11.3): the adopted first root, crash cuts before and after
//! the first authority-5 adoption, rejected downgrade, obsolete-exact
//! retirement and legacy container GC. Both history modes.

use super::super::checkpoint_contract::{CheckpointImageV4, PER_CELL_MANIFEST_FORMAT};
use super::super::pipeline_tests::Fixture;
use super::*;
use crate::distributed::native_index::{IndexImageMetadata, read_cell_root};
use crate::key_shape::KeyType;

const KEYS: u32 = 24;

fn shape() -> KeyShape {
    // 16 cells: batches span several cells and legacy containers.
    KeyShape::new_single(4, 4, KeyType::uniform(4))
}

fn key(key: u32) -> Vec<u8> {
    // Uniform cells route by the leading key bits: spread the keys over the
    // whole u32 range so batches touch several cells.
    ((u64::from(key) << 32) / u64::from(KEYS)).to_be_bytes()[4..].to_vec()
}

/// Sequential per-key oracle of acknowledged writes, plus every batch by
/// seed for retry checks.
#[derive(Clone, Default)]
struct Oracle {
    values: BTreeMap<u32, Option<Vec<u8>>>,
    batches: BTreeMap<u64, Batch>,
}

/// One acknowledged batch: its operations and version.
type Batch = (Vec<(u32, Option<u8>)>, BatchVersion);

fn mutations(operations: &[(u32, Option<u8>)]) -> Vec<Mutation> {
    operations
        .iter()
        .map(|(k, value)| match value {
            Some(byte) => Mutation::Put {
                keyspace: KeySpace(0),
                key: key(*k).into(),
                value: vec![*byte; 32].into(),
            },
            None => Mutation::Delete {
                keyspace: KeySpace(0),
                key: key(*k).into(),
            },
        })
        .collect()
}

impl Oracle {
    /// One batch: `Some(byte)` puts a 32-byte value, `None` deletes.
    fn write(
        &mut self,
        fixture: &Fixture,
        db: &RemoteDatabase,
        seed: u64,
        operations: &[(u32, Option<u8>)],
    ) {
        let version = db
            .write_batch(
                fixture.operation((seed % 5) as u32, seed),
                mutations(operations),
            )
            .unwrap();
        for (k, value) in operations {
            self.values.insert(*k, value.map(|byte| vec![byte; 32]));
        }
        self.batches.insert(seed, (operations.to_vec(), version));
    }

    fn check(&self, db: &mut RemoteDatabase) {
        for k in 0..KEYS {
            let expected = self.values.get(&k).cloned().flatten();
            let actual = db.get(KeySpace(0), &key(k)).unwrap().map(|v| v.to_vec());
            assert_eq!(actual, expected, "key {k}");
        }
    }

    /// The retry authority survives migration: an identical retry of an
    /// acknowledged batch returns its version without a new write, and a
    /// different payload under the same id is refused.
    fn check_retries(&self, fixture: &Fixture, db: &mut RemoteDatabase, seeds: &[u64]) {
        for seed in seeds {
            let (operations, version) = &self.batches[seed];
            let operation = fixture.operation((seed % 5) as u32, *seed);
            assert_eq!(
                db.write_batch(operation, mutations(operations)).unwrap(),
                *version,
                "retry of batch {seed}"
            );
            assert!(
                db.write_batch(operation, mutations(&[(KEYS - 1, Some(0xEE))]))
                    .is_err(),
                "conflicting retry of batch {seed}"
            );
        }
        self.check(db);
    }
}

fn manifest(fixture: &Fixture) -> AuthorityManifest {
    read_metadata(&authority(fixture).join(MANIFEST)).unwrap()
}

fn authority(fixture: &Fixture) -> PathBuf {
    fixture
        .temp
        .path()
        .join("authority")
        .canonicalize()
        .unwrap()
}

fn names(fixture: &Fixture) -> BTreeSet<String> {
    fs::read_dir(authority(fixture))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

/// An exact database with two exact generations (the second reuses the
/// first generation's container for untouched cells) and a suffix above the
/// adopted cut: updates, deletes and keys in new cells. The last open closes
/// without a checkpoint, so the suffix stays in the owner WALs only.
fn exact_with_suffix(history: HistoryPersistence) -> (Fixture, Oracle) {
    let fixture = Fixture::new_with_shape(128, shape()).history(history);
    let mut oracle = Oracle::default();
    let db = fixture.open();
    for seed in 1..=4u64 {
        let base = (seed as u32 - 1) * 3;
        oracle.write(
            &fixture,
            &db,
            seed,
            &[
                (base, Some(seed as u8)),
                (base + 1, Some(seed as u8)),
                (base + 2, Some(1)),
            ],
        );
    }
    db.core.commit_checkpoint().unwrap();
    oracle.write(&fixture, &db, 5, &[(0, Some(50)), (1, None)]);
    oracle.write(&fixture, &db, 6, &[(12, Some(60)), (13, Some(61))]);
    db.core.commit_checkpoint().unwrap();
    // Suffix above the adopted exact cut.
    oracle.write(
        &fixture,
        &db,
        7,
        &[(0, Some(70)), (3, None), (16, Some(71))],
    );
    oracle.write(
        &fixture,
        &db,
        8,
        &[(17, Some(80)), (18, Some(81)), (12, None)],
    );
    oracle.write(&fixture, &db, 9, &[(20, Some(90)), (1, Some(91))]);
    db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
    db.shutdown().unwrap();
    drop(db);
    (fixture, oracle)
}

fn per_cell(fixture: &mut Fixture) {
    fixture.config.checkpoint_mode = CheckpointMode::PerCell;
}

fn exact(fixture: &mut Fixture) {
    fixture.config.checkpoint_mode = CheckpointMode::Exact;
}

fn try_open(fixture: &Fixture) -> Result<RemoteDatabase, LocalDbError> {
    RemoteDatabase::open(
        &fixture.temp.path().join("authority"),
        &fixture.shape,
        fixture.config.clone(),
        Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
}

fn set_hook(hook: impl FnMut(MigrationStep) -> Result<(), LocalDbError> + 'static) {
    MIGRATION_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn clear_hook() {
    MIGRATION_HOOK.with(|slot| *slot.borrow_mut() = None);
}

/// A downgrade to `Exact` is rejected before any change, and old binaries
/// accept only manifest formats 2 to 4.
fn assert_downgrade_rejected(fixture: &mut Fixture) {
    let before = fs::read(authority(fixture).join(MANIFEST)).unwrap();
    exact(fixture);
    let error = try_open(fixture).err().expect("downgrade must be rejected");
    assert!(
        error.message().contains("checkpoint mode differs"),
        "{}",
        error.message()
    );
    assert_eq!(fs::read(authority(fixture).join(MANIFEST)).unwrap(), before);
    assert_eq!(manifest(fixture).format, PER_CELL_MANIFEST_FORMAT);
    per_cell(fixture);
}

/// After a completed (re)open in `PerCell`: format 5, the obsolete exact
/// root and wrapper retired, no staging output left, and every per-cell root
/// on disk is the adopted or retained one.
fn assert_settled_per_cell(fixture: &Fixture, exact_commit: &CheckpointCommit) {
    let manifest = manifest(fixture);
    assert_eq!(manifest.format, PER_CELL_MANIFEST_FORMAT);
    assert_eq!(manifest.checkpoint_mode, CheckpointMode::PerCell);
    assert!(manifest.checkpoint.is_none());
    assert!(manifest.checkpoint_obsolete.is_none());
    assert!(manifest.checkpoint_staging.is_none());
    let names = names(fixture);
    assert!(!names.contains(&exact_commit.index_name()));
    assert!(!names.contains(&exact_commit.metadata_name()));
    let commits: Vec<_> = [
        &manifest.cell_checkpoint,
        &manifest.cell_checkpoint_obsolete,
    ]
    .into_iter()
    .flatten()
    .collect();
    for name in &names {
        assert!(!name.ends_with(".pending"), "staging file {name} left");
        if name.ends_with(".cellroot") {
            assert!(
                commits.iter().any(|commit| commit.root_name() == *name),
                "unadopted root {name} left"
            );
        }
    }
}

fn exact_commit(fixture: &Fixture) -> CheckpointCommit {
    let manifest = manifest(fixture);
    assert!(matches!(manifest.format, 2..=4));
    manifest.checkpoint.expect("exact commit")
}

fn exact_cut_of(fixture: &Fixture, commit: &CheckpointCommit) -> PrefixCut {
    let path = authority(fixture).join(commit.metadata_name());
    let cut = if fixture.config.history_persistence == HistoryPersistence::WalReplay {
        read_checkpoint::<CheckpointImageV3>(&path).unwrap().cut
    } else {
        read_checkpoint::<CheckpointImage>(&path).unwrap().cut
    };
    prefix_of(cut)
}

#[test]
fn migration_adopts_the_exact_cut_wal_replay() {
    migration_adopts_the_exact_cut_as_cell_coverage(HistoryPersistence::WalReplay);
}

#[test]
fn migration_adopts_the_exact_cut_receipt_history() {
    migration_adopts_the_exact_cut_as_cell_coverage(HistoryPersistence::OnDisk);
}

fn migration_adopts_the_exact_cut_as_cell_coverage(history: HistoryPersistence) {
    {
        let (mut fixture, mut oracle) = exact_with_suffix(history);
        let path = authority(&fixture);
        let commit = exact_commit(&fixture);
        let target = exact_cut_of(&fixture, &commit);
        assert!(matches!(target, PrefixCut::Through(_)));
        let containers: BTreeMap<String, Vec<u8>> = commit
            .index_files
            .iter()
            .map(|name| (name.clone(), fs::read(path.join(name)).unwrap()))
            .collect();
        assert!(
            containers.len() >= 2,
            "two exact generations reuse containers"
        );
        let (exact_recent, exact_history) = if history == HistoryPersistence::WalReplay {
            let image: CheckpointImageV3 =
                read_checkpoint(&path.join(commit.metadata_name())).unwrap();
            (image.recent, None)
        } else {
            let image: CheckpointImage =
                read_checkpoint(&path.join(commit.metadata_name())).unwrap();
            (Vec::new(), image.history_root)
        };
        let recorded_limits = manifest(&fixture).replay_limits;
        let inspected = Arc::new(Mutex::new(false));
        let seen = Arc::clone(&inspected);
        let expected = commit.clone();
        let shape = fixture.shape.clone();
        let max_index_bytes = fixture.config.max_index_bytes;
        let database = fixture.config.storage.database;
        let directory = path.clone();
        set_hook(move |step| {
            if step != MigrationStep::Applied {
                return Ok(());
            }
            // The first adopted per-cell generation, before any later root.
            let adopted: AuthorityManifest = read_metadata(&directory.join(MANIFEST)).unwrap();
            assert_eq!(adopted.format, PER_CELL_MANIFEST_FORMAT);
            assert_eq!(adopted.checkpoint_mode, CheckpointMode::PerCell);
            assert!(adopted.checkpoint.is_none());
            let obsolete = adopted.checkpoint_obsolete.as_ref().unwrap();
            assert_eq!(
                (obsolete.epoch, obsolete.generation),
                (expected.epoch, expected.generation)
            );
            assert_eq!(adopted.replay_limits, recorded_limits);
            let cell = adopted.cell_checkpoint.clone().unwrap();
            let root = read_cell_root(
                &directory.join(cell.root_name()),
                IndexImageMetadata {
                    bytes: cell.root_bytes,
                    digest: cell.root_digest,
                },
                &shape,
                database,
                max_index_bytes,
            )
            .unwrap();
            let header = &root.header;
            assert_eq!(header.directory_target, target);
            assert_eq!(header.replay_floor, target);
            assert_eq!(header.max_frame_cut, target);
            assert!(root.cells.iter().all(|cell| cell.coverage == target));
            assert!(root.cells.iter().any(|cell| cell.base.is_some()));
            // Legacy frames are referenced unchanged under their ids.
            let files: Vec<_> = root.files.iter().map(|file| file.name.clone()).collect();
            assert_eq!(files, expected.index_files);
            assert!(
                root.files
                    .iter()
                    .all(|file| matches!(file.id, IndexStoreFileId::LegacyContainer { .. }))
            );
            let wrapper: CheckpointImageV4 =
                read_checkpoint(&directory.join(cell.metadata_name())).unwrap();
            assert_eq!(wrapper.replay_floor, target);
            assert_eq!(wrapper.published_next, 0);
            assert_eq!(wrapper.retirement.from, target);
            assert_eq!(wrapper.retirement.through, target);
            assert_eq!(
                (
                    wrapper.retirement.batches,
                    wrapper.retirement.encoded_bytes,
                    wrapper.retirement.identities
                ),
                (0, 0, 0)
            );
            assert_eq!(wrapper.history_root, exact_history);
            assert_eq!(
                wrapper.recent.iter().map(|r| r.version).collect::<Vec<_>>(),
                exact_recent.iter().map(|r| r.version).collect::<Vec<_>>()
            );
            // Every partition lists a stream of the adopting epoch.
            for partition in 0..5 {
                assert!(
                    wrapper
                        .streams
                        .iter()
                        .any(|stream| stream.partition == partition
                            && stream.epoch == wrapper.authority_epoch
                            && !stream.complete
                            && stream.last_offset.is_none())
                );
            }
            assert!(wrapper.streams.iter().all(|stream| {
                stream.receipt_prefix.is_some()
                    == (wrapper.history_persistence != HistoryPersistence::WalReplay)
            }));
            *seen.lock() = true;
            Ok(())
        });
        per_cell(&mut fixture);
        let mut db = fixture.open();
        clear_hook();
        assert!(*inspected.lock(), "migration did not run");
        // Batch 1 is below `T` (old retry authority), 8 is in the suffix.
        oracle.check_retries(&fixture, &mut db, &[1, 8]);
        // Every legacy container still referenced is byte-identical, and the
        // live bases keep every container alive for legacy GC.
        let adopted = db.core.publisher.lock().manifest.clone();
        assert_settled_per_cell(&fixture, &commit);
        // Every container a live base names exists.
        for name in db.core.index.reachable_files() {
            assert!(path.join(&name).exists(), "live container {name} deleted");
        }
        for (name, bytes) in &containers {
            if let Ok(now) = fs::read(path.join(name)) {
                assert_eq!(&now, bytes, "legacy container {name} changed");
            }
        }
        assert_eq!(adopted.format, PER_CELL_MANIFEST_FORMAT);
        // Writes after migration recover through the per-cell protocol.
        oracle.write(&fixture, &db, 10, &[(21, Some(100)), (0, None)]);
        oracle.write(&fixture, &db, 11, &[(2, Some(110)), (22, Some(111))]);
        db.shutdown().unwrap();
        drop(db);
        let mut db = fixture.open();
        oracle.check_retries(&fixture, &mut db, &[1, 8, 10]);
        db.shutdown().unwrap();
        drop(db);
        assert_settled_per_cell(&fixture, &commit);
        assert_downgrade_rejected(&mut fixture);
    }
}

/// How an injected crash leaves the durable manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Cut {
    /// Crash right after this step.
    After(MigrationStep),
    /// The format-5 rename happened but its directory entry was lost: the
    /// old manifest is the durable outcome (model of a rename before the
    /// directory sync).
    RenameLost,
}

#[test]
fn migration_crash_cuts_wal_replay() {
    migration_crash_cuts_before_and_after_the_first_authority_5_adoption(
        HistoryPersistence::WalReplay,
    );
}

#[test]
fn migration_crash_cuts_receipt_history() {
    migration_crash_cuts_before_and_after_the_first_authority_5_adoption(
        HistoryPersistence::OnDisk,
    );
}

fn migration_crash_cuts_before_and_after_the_first_authority_5_adoption(
    history: HistoryPersistence,
) {
    let cuts = [
        Cut::After(MigrationStep::Captured),
        Cut::After(MigrationStep::Commit(CommitStep::StagingReserved)),
        Cut::After(MigrationStep::Commit(CommitStep::RootPersisted)),
        Cut::After(MigrationStep::Commit(CommitStep::WrapperPersisted)),
        Cut::RenameLost,
        Cut::After(MigrationStep::Commit(CommitStep::ManifestPersisted)),
        Cut::After(MigrationStep::Applied),
    ];
    {
        for cut in cuts {
            let (mut fixture, mut oracle) = exact_with_suffix(history);
            let commit = exact_commit(&fixture);
            let path = authority(&fixture);
            let directory = path.clone();
            let mut old_manifest = None;
            set_hook(move |step| {
                if step == MigrationStep::Commit(CommitStep::WrapperPersisted) {
                    old_manifest = Some(fs::read(directory.join(MANIFEST)).unwrap());
                }
                match cut {
                    Cut::After(at) if at == step => Err(LocalDbError::new(format!(
                        "injected migration crash after {step:?}"
                    ))),
                    Cut::RenameLost
                        if step == MigrationStep::Commit(CommitStep::ManifestPersisted) =>
                    {
                        fs::write(directory.join(MANIFEST), old_manifest.take().unwrap()).unwrap();
                        Err(LocalDbError::new("injected migration crash: rename lost"))
                    }
                    _ => Ok(()),
                }
            });
            per_cell(&mut fixture);
            let error = try_open(&fixture).err().expect("injected crash");
            clear_hook();
            assert!(
                error.message().contains("injected"),
                "{cut:?}: {}",
                error.message()
            );
            let adopted = matches!(
                cut,
                Cut::After(MigrationStep::Commit(CommitStep::ManifestPersisted))
                    | Cut::After(MigrationStep::Applied)
            );
            let durable = manifest(&fixture);
            if adopted {
                // After the first authority-5 adoption: no fallback to the
                // old exact root, and the downgrade is rejected.
                assert_eq!(durable.format, PER_CELL_MANIFEST_FORMAT, "{cut:?}");
                assert!(durable.checkpoint.is_none());
                let obsolete = durable.checkpoint_obsolete.clone().unwrap();
                assert_eq!(obsolete.generation, commit.generation);
                if cut == Cut::After(MigrationStep::Applied) {
                    // A crash in the middle of the obsolete-exact unlinks:
                    // one target is gone, the retirement repeats safely.
                    fs::remove_file(path.join(commit.metadata_name())).unwrap();
                    let cell = durable.cell_checkpoint.clone().unwrap();
                    let root = read_cell_root(
                        &path.join(cell.root_name()),
                        IndexImageMetadata {
                            bytes: cell.root_bytes,
                            digest: cell.root_digest,
                        },
                        &fixture.shape,
                        fixture.config.storage.database,
                        fixture.config.max_index_bytes,
                    )
                    .unwrap();
                    let mut retiring = durable.clone();
                    retire_obsolete_exact(&path, &mut retiring, &root.files).unwrap();
                    assert!(retiring.checkpoint_obsolete.is_none());
                    assert!(!path.join(commit.index_name()).exists());
                    for file in &root.files {
                        assert!(
                            path.join(&file.name).exists(),
                            "referenced {} deleted",
                            file.name
                        );
                    }
                    // Idempotent once complete.
                    retire_obsolete_exact(&path, &mut retiring, &root.files).unwrap();
                    assert!(manifest(&fixture).checkpoint_obsolete.is_none());
                }
                assert_downgrade_rejected(&mut fixture);
            } else {
                // Before adoption the exact manifest, root and suffix stay
                // authoritative: an Exact reopen recovers every write and its
                // cleanup removes every staged per-cell output.
                assert!(matches!(durable.format, 2..=4), "{cut:?}");
                assert_eq!(
                    durable.checkpoint.as_ref().unwrap().generation,
                    commit.generation
                );
                exact(&mut fixture);
                let mut db = fixture.open();
                oracle.check(&mut db);
                db.shutdown().unwrap();
                drop(db);
                assert!(matches!(manifest(&fixture).format, 2..=4));
                for name in names(&fixture) {
                    assert!(
                        !name.ends_with(".cellroot") && !name.ends_with(".pending"),
                        "{cut:?}: per-cell output {name} survived exact cleanup"
                    );
                }
                per_cell(&mut fixture);
            }
            // The next per-cell open finishes or repeats the migration.
            let commit = if adopted {
                commit
            } else {
                exact_commit(&fixture)
            };
            let mut db = fixture.open();
            oracle.check(&mut db);
            oracle.write(&fixture, &db, 20, &[(23, Some(200)), (16, None)]);
            db.shutdown().unwrap();
            drop(db);
            let mut db = fixture.open();
            oracle.check_retries(&fixture, &mut db, &[1, 9, 20]);
            db.shutdown().unwrap();
            drop(db);
            assert_settled_per_cell(&fixture, &commit);
            assert_downgrade_rejected(&mut fixture);
        }
    }
}

#[test]
fn obsolete_exact_retirement_and_legacy_gc_keep_referenced_files() {
    let (mut fixture, _oracle) = exact_with_suffix(HistoryPersistence::OnDisk);
    let path = authority(&fixture);
    let commit = exact_commit(&fixture);
    let mut format4 = manifest(&fixture);
    // An exact manifest never retires through the per-cell rule.
    format4.checkpoint_obsolete = Some(commit.clone());
    assert!(retire_obsolete_exact(&path, &mut format4, &[]).is_err());
    assert!(path.join(commit.metadata_name()).exists());
    // Legacy GC: only unreferenced canonical legacy containers go.
    let index = NativeIndex::with_shards(
        &fixture.shape,
        fixture.config.storage.database,
        fixture.config.max_index_bytes,
        fixture.config.index_shards,
    )
    .unwrap();
    let keep = commit.index_files[0].clone();
    let keep_id = IndexStoreFileId::parse_canonical(&keep).unwrap();
    let stray = IndexStoreFileId::LegacyContainer {
        authority_epoch: 7,
        generation: 9,
    };
    fs::write(path.join(stray.canonical_name()), b"stray").unwrap();
    let segment = IndexStoreFileId::Segment {
        authority_epoch: 1,
        stream_id: 0,
        segment_seq: 0,
    };
    fs::write(path.join(segment.canonical_name()), b"segment").unwrap();
    fs::write(path.join("index-07-9.snapshot.cells"), b"not canonical").unwrap();
    let roots = [RootFile {
        id: keep_id,
        name: keep.clone(),
        sealed_len: fs::metadata(path.join(&keep)).unwrap().len(),
    }];
    let deleted = collect_legacy_containers(&path, &[&roots], &index).unwrap();
    assert!(deleted.contains(&stray));
    assert!(!deleted.contains(&keep_id));
    assert!(path.join(&keep).exists());
    assert!(path.join(segment.canonical_name()).exists());
    assert!(path.join("index-07-9.snapshot.cells").exists());
    // Every other exact container was unreferenced in this call.
    for name in &commit.index_files[1..] {
        assert!(!path.join(name).exists());
    }
    // A second sweep deletes nothing.
    assert!(
        collect_legacy_containers(&path, &[&roots], &index)
            .unwrap()
            .is_empty()
    );
    // The fixture's database is now corrupt by construction; do not reopen.
    exact(&mut fixture);
}

/// Open with age-driven cell flushes disabled, so only explicit drains move
/// cell coverage.
fn open_without_age_flushes(fixture: &Fixture) -> RemoteDatabase {
    use super::super::cell_checkpoint::TEST_FLUSH_POLICY;
    TEST_FLUSH_POLICY.with(|policy| {
        policy.set(Some(super::super::cell_flush::FlushPolicy {
            age: Duration::from_secs(3600),
            ..Default::default()
        }))
    });
    let db = try_open(fixture);
    TEST_FLUSH_POLICY.with(|policy| policy.set(None));
    db.unwrap()
}

/// Receipt history after migration: a later per-cell root whose frames cover
/// a batch above `F` proves `(F, U]` with an extended owner receipt prefix,
/// and a forged extension fails the commit without adopting it.
#[test]
fn receipt_history_commit_after_migration_proves_frames_above_f() {
    use super::super::cell_checkpoint::CellStep;
    let (mut fixture, mut oracle) = exact_with_suffix(HistoryPersistence::OnDisk);
    per_cell(&mut fixture);
    for forged in [false, true] {
        let mut db = open_without_age_flushes(&fixture);
        oracle.check(&mut db);
        let seed = if forged { 40 } else { 30 };
        oracle.write(&fixture, &db, seed, &[(5, Some(seed as u8))]);
        db.drain().unwrap();
        let before = manifest(&fixture).cell_checkpoint.unwrap();
        let (entered, drained) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.core.test_hooks.lock().cell_pause = Some((CellStep::Drained, entered, gate));
        db.core.test_hooks.lock().corrupt_checkpoint_proof = forged;
        let core = Arc::clone(&db.core);
        let flush = thread::spawn(move || core.flush_index());
        drained.recv_timeout(Duration::from_secs(60)).unwrap();
        // Published after the drain and before the root capture: its cell
        // stays dirty (F below it) while the root target covers it (U).
        oracle.write(&fixture, &db, seed + 1, &[(5, Some(seed as u8 + 1))]);
        db.drain().unwrap();
        resume.send(()).unwrap();
        let result = flush.join().unwrap();
        let late = oracle.batches[&(seed + 1)].1;
        if forged {
            assert!(
                result.is_err(),
                "a forged prefix extension must fail the commit"
            );
            assert_eq!(manifest(&fixture).cell_checkpoint, Some(before));
            db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
            let _ = db.shutdown();
            drop(db);
        } else {
            result.unwrap();
            let adopted = manifest(&fixture).cell_checkpoint.unwrap();
            assert!(adopted.generation > before.generation);
            let image: CheckpointImageV4 =
                read_checkpoint(&authority(&fixture).join(adopted.metadata_name())).unwrap();
            assert_eq!(image.max_frame_cut, PrefixCut::Through(late));
            assert!(!image.replay_floor.contains(late));
            assert!(image.history_root.is_some());
            assert!(
                image
                    .streams
                    .iter()
                    .all(|stream| stream.receipt_prefix.is_some())
            );
            oracle.check_retries(&fixture, &mut db, &[1, 8, seed]);
            db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
            db.shutdown().unwrap();
            drop(db);
        }
    }
    // The forged attempt adopted nothing: the last good root and the suffix
    // recover every acknowledged write.
    let mut db = fixture.open();
    oracle.check(&mut db);
    db.shutdown().unwrap();
}
