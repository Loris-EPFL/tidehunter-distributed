//! Per-cell checkpoint commit, retirement, recovery and crash cuts on the
//! five-owner mTLS fixture with real owner WALs, real index-store segments and
//! real format-5 roots. Every crash cut fails the core at a persistence step,
//! drops it without further disk effects (fail-stop) and reopens: the adopted
//! root plus the retained WAL suffix must reproduce every acknowledged write
//! and identity, in both durability tiers.

use super::super::pipeline_tests::Fixture;
use super::*;
use crate::key_shape::KeyType;
use std::collections::BTreeSet;

const ALL_STEPS: [CellStep; 14] = [
    CellStep::Drained,
    CellStep::Capture,
    CellStep::OwnerProof,
    CellStep::Staging,
    CellStep::Root,
    CellStep::Wrapper,
    CellStep::Manifest,
    CellStep::ApplyHistory,
    CellStep::ApplyAuthority,
    CellStep::ApplyLedger,
    CellStep::IdentityRemove,
    CellStep::CreditReturn,
    CellStep::GcUnlink,
    CellStep::GcSweep,
];

fn per_cell(synchronous: bool) -> Fixture {
    let fixture = Fixture::new_with_config(
        256,
        KeyShape::new_single(4, 1, KeyType::uniform(16)),
        2,
        |config| {
            config.checkpoint_mode = CheckpointMode::PerCell;
            config.history_persistence = HistoryPersistence::WalReplay;
        },
    );
    if synchronous {
        fixture.synchronous()
    } else {
        fixture
    }
}

/// Keys spread over the uniform cells of keyspace 0.
fn key(n: u32) -> Vec<u8> {
    (n << 27).to_be_bytes().to_vec()
}

fn put(n: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key(n).into(),
        value: vec![value; 32].into(),
    }
}

fn delete(n: u32) -> Mutation {
    Mutation::Delete {
        keyspace: KeySpace(0),
        key: key(n).into(),
    }
}

/// Acknowledged writes, their identities and the expected final values.
#[derive(Default)]
struct Oracle {
    values: BTreeMap<u32, Option<u8>>,
    writes: Vec<([u8; 16], Vec<Mutation>, BatchVersion)>,
}

impl Oracle {
    fn write(&mut self, db: &RemoteDatabase, fixture: &Fixture, mutations: Vec<Mutation>) {
        let seed = self.writes.len() as u64 + 1;
        let operation = fixture.operation((seed % 5) as u32, seed);
        let version = db.write_batch(operation, mutations.clone()).unwrap();
        for mutation in &mutations {
            match mutation {
                Mutation::Put { key, value, .. } => {
                    let n = u32::from_be_bytes(key[..4].try_into().unwrap()) >> 27;
                    self.values.insert(n, Some(value[0]));
                }
                Mutation::Delete { key, .. } => {
                    let n = u32::from_be_bytes(key[..4].try_into().unwrap()) >> 27;
                    self.values.insert(n, None);
                }
            }
        }
        self.writes.push((operation, mutations, version));
    }

    /// Round 1: one put per key. Round 2 overwrites, deletes and adds keys.
    fn round(&mut self, db: &RemoteDatabase, fixture: &Fixture, round: u8) {
        if round == 1 {
            for n in 0..8 {
                self.write(db, fixture, vec![put(n, n as u8 + 1)]);
            }
        } else {
            for n in 0..4 {
                self.write(db, fixture, vec![put(n, 100 + n as u8 + round)]);
            }
            self.write(
                db,
                fixture,
                vec![delete(5), put(8 + u32::from(round), round)],
            );
            self.write(db, fixture, vec![put(6, round), put(7, round)]);
        }
    }

    fn check(&self, db: &mut RemoteDatabase, context: &str) {
        for (n, expected) in &self.values {
            let actual = db.get(KeySpace(0), &key(*n)).unwrap().map(|value| value[0]);
            assert_eq!(actual, *expected, "key {n}: {context}");
        }
        // Every acknowledged identity returns its original version.
        for (operation, mutations, version) in &self.writes {
            assert_eq!(
                db.write_batch(*operation, mutations.clone()).unwrap(),
                *version,
                "retry of {version:?}: {context}"
            );
        }
    }
}

fn authority(fixture: &Fixture) -> PathBuf {
    fixture
        .temp
        .path()
        .join("authority")
        .canonicalize()
        .unwrap()
}

fn manifest(fixture: &Fixture) -> AuthorityManifest {
    read_metadata(&authority(fixture).join(MANIFEST)).unwrap()
}

/// Canonical segment, root and wrapper names, and any staging files.
fn outputs(fixture: &Fixture) -> (BTreeSet<IndexStoreFileId>, BTreeSet<String>) {
    let mut segments = BTreeSet::new();
    let mut metadata = BTreeSet::new();
    for entry in fs::read_dir(authority(fixture)).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if let Some(id) = IndexStoreFileId::parse_canonical(&name) {
            segments.insert(id);
        } else if name.ends_with(".pending")
            || name.ends_with(".cellroot")
            || name.starts_with("checkpoint-")
        {
            metadata.insert(name);
        }
    }
    (segments, metadata)
}

/// Exactly the adopted root and wrapper exist and every segment on disk is
/// referenced by the adopted root.
fn assert_only_adopted_outputs(fixture: &Fixture, context: &str) {
    let manifest = manifest(fixture);
    let (segments, metadata) = outputs(fixture);
    let Some(commit) = manifest.cell_checkpoint.clone() else {
        assert!(metadata.is_empty(), "{context}: {metadata:?}");
        assert!(segments.is_empty(), "{context}: {segments:?}");
        return;
    };
    assert_eq!(
        metadata,
        BTreeSet::from([commit.root_name(), commit.metadata_name()]),
        "{context}"
    );
    let root = read_cell_root(
        &authority(fixture).join(commit.root_name()),
        IndexImageMetadata {
            bytes: commit.root_bytes,
            digest: commit.root_digest,
        },
        &fixture.shape,
        fixture.config.storage.database,
        fixture.config.max_index_bytes,
    )
    .unwrap();
    let referenced: BTreeSet<_> = root.files.iter().map(|file| file.id).collect();
    assert_eq!(segments, referenced, "{context}");
}

fn wrapper(fixture: &Fixture) -> CheckpointImageV4 {
    let commit = manifest(fixture).cell_checkpoint.unwrap();
    read_checkpoint(&authority(fixture).join(commit.metadata_name())).unwrap()
}

fn runtime(db: &RemoteDatabase) -> &CellRuntime {
    db.core.cell.get().unwrap()
}

/// Launch one checkpoint generation even with an empty tail (the ordinary
/// start skips that case before launching) and wait for it.
fn launch(db: &RemoteDatabase) {
    {
        let mut publisher = db.core.publisher.lock();
        super::super::checkpoint_lifecycle::core_launch_checkpoint(
            &db.core,
            publisher.checkpoint_state(),
            None,
        )
        .unwrap();
    }
    db.core.progress_checkpoint(true).unwrap();
}

#[test]
fn per_cell_root_adopts_metadata_only_and_returns_exact_credits() {
    for synchronous in [false, true] {
        let fixture = per_cell(synchronous);
        let mut db = fixture.open();
        let mut oracle = Oracle::default();
        oracle.round(&db, &fixture, 1);
        oracle.round(&db, &fixture, 2);
        db.drain().unwrap();
        let batches = oracle.writes.len();
        let granted = db.core.admission.charges();
        assert_eq!(
            (granted.replay_batches, granted.identities),
            (batches, batches)
        );
        let tail_bytes = db.core.publisher.lock().checkpoint_tail_bytes;
        db.flush_index().unwrap();

        let adopted = manifest(&fixture);
        assert_eq!(adopted.format, PER_CELL_MANIFEST_FORMAT);
        assert_eq!(adopted.checkpoint_mode, CheckpointMode::PerCell);
        assert!(adopted.checkpoint.is_none() && adopted.checkpoint_staging.is_none());
        assert!(adopted.cell_checkpoint_obsolete.is_none());
        let commit = adopted.cell_checkpoint.clone().unwrap();
        assert_eq!((commit.epoch, commit.generation), (db.epoch(), 1));
        // The open (new store epoch, fence) is one lifecycle event.
        let root = read_cell_root(
            &authority(&fixture).join(commit.root_name()),
            IndexImageMetadata {
                bytes: commit.root_bytes,
                digest: commit.root_digest,
            },
            &fixture.shape,
            fixture.config.storage.database,
            fixture.config.max_index_bytes,
        )
        .unwrap();
        assert_eq!(root.header.lifecycle_generation, 1);
        let image = wrapper(&fixture);
        let last = oracle.writes.last().unwrap().2;
        assert_eq!(image.replay_floor, PrefixCut::Through(last));
        // Through-F retirement: every batch, its exact encoded bytes and
        // identity, from the receipt ledger (not index frame bytes).
        assert_eq!(
            image.retirement,
            RetirementToken {
                generation: 1,
                from: PrefixCut::BeforeAll,
                through: PrefixCut::Through(last),
                batches: batches as u64,
                encoded_bytes: tail_bytes as u64,
                identities: batches as u64,
            }
        );
        assert!(image.recent.len() == batches && image.streams.iter().all(|s| !s.complete));
        // Owner proofs cover U; the skip position is through F.
        for stream in &image.streams {
            assert!(
                stream
                    .last_offset
                    .is_none_or(|last| last < stream.durable_end)
            );
        }
        let remaining = db.core.admission.charges();
        assert_eq!(
            (
                remaining.replay_batches,
                remaining.replay_bytes,
                remaining.identities
            ),
            (0, 0, 0)
        );
        {
            let publisher = db.core.publisher.lock();
            assert!(publisher.known.is_empty());
            assert_eq!(
                (
                    publisher.checkpoint_tail_batches,
                    publisher.checkpoint_tail_bytes
                ),
                (0, 0)
            );
        }
        let (captured, committed, applied, skipped) = runtime(&db).last_root();
        assert!(captured.cells > 0 && captured.files > 0 && committed.root_bytes > 0);
        assert!(committed.owner_proofs > 0);
        assert_eq!(skipped, 0);
        assert_eq!(
            (
                applied.returned_batches,
                applied.returned_encoded_bytes,
                applied.returned_identities,
                applied.f_advanced
            ),
            (batches, tail_bytes, batches, true)
        );
        assert_eq!(runtime(&db).adopted().floor, PrefixCut::Through(last));
        assert_only_adopted_outputs(&fixture, "after generation 1");
        oracle.check(&mut db, "after generation 1");

        // A second generation retires only its own suffix and removes the
        // obsolete root and wrapper after adoption.
        let first = oracle.writes.len();
        oracle.round(&db, &fixture, 3);
        db.drain().unwrap();
        db.flush_index().unwrap();
        let image = wrapper(&fixture);
        assert_eq!(image.generation, 2);
        assert_eq!(image.retirement.from, PrefixCut::Through(last));
        assert_eq!(
            image.retirement.batches,
            (oracle.writes.len() - first) as u64
        );
        assert_only_adopted_outputs(&fixture, "after generation 2");
        assert_eq!(db.core.admission.charges().identities, 0);
        oracle.check(&mut db, "after generation 2");
        db.shutdown().unwrap();
        drop(db);

        // Recovery from the adopted root: nothing above F, every value and
        // identity (WAL-replay window) preserved.
        let mut db = fixture.open();
        let recovery = db.recovery_stats();
        assert!(recovery.checkpoint_loaded);
        assert_eq!(recovery.tail_batches, 0);
        oracle.check(&mut db, "after reopen");
        db.shutdown().unwrap();
    }
}

#[test]
fn unchanged_floor_root_is_skipped_and_a_forced_one_returns_zero_credit() {
    let fixture = per_cell(false);
    let mut db = fixture.open();
    let mut oracle = Oracle::default();
    oracle.round(&db, &fixture, 1);
    db.drain().unwrap();
    db.flush_index().unwrap();
    let floor = runtime(&db).adopted().floor;
    // A start with no tail and an adopted root does not even launch.
    db.flush_index().unwrap();
    assert_eq!(manifest(&fixture).cell_checkpoint.unwrap().generation, 1);
    // A launched generation whose F and H are unchanged is skipped.
    launch(&db);
    assert_eq!(runtime(&db).last_root().3, 1);
    assert_eq!(manifest(&fixture).cell_checkpoint.unwrap().generation, 1);
    // Forced: adopted with a zero token; no tail or identity credit moves.
    let before = db.core.admission.charges();
    db.core.test_hooks.lock().force_cell_root = true;
    launch(&db);
    let image = wrapper(&fixture);
    assert_eq!(image.generation, 3);
    assert_eq!(
        image.retirement,
        RetirementToken {
            generation: 3,
            from: floor,
            through: floor,
            batches: 0,
            encoded_bytes: 0,
            identities: 0,
        }
    );
    let after = db.core.admission.charges();
    assert_eq!(
        (after.replay_batches, after.replay_bytes, after.identities),
        (
            before.replay_batches,
            before.replay_bytes,
            before.identities
        )
    );
    let applied = runtime(&db).last_root().2;
    assert_eq!(
        (
            applied.returned_batches,
            applied.returned_identities,
            applied.f_advanced
        ),
        (0, 0, false)
    );
    // The ledger applied generation 3 once: a second apply returns nothing.
    let mut ledger = runtime(&db).ledger();
    assert_eq!(ledger.generation, 3);
    assert!(ledger.apply(&image.retirement).unwrap().is_none());
    assert_only_adopted_outputs(&fixture, "after forced root");
    oracle.check(&mut db, "after forced root");
    db.shutdown().unwrap();
}

/// Reopen after the injected crash and verify values, identities, the
/// adopted generation and that cleanup left only adopted outputs.
fn recover_after(fixture: &Fixture, oracle: &mut Oracle, adopted_round: usize, context: &str) {
    let mut db = fixture.open();
    let recovery = db.recovery_stats();
    assert!(recovery.checkpoint_loaded, "{context}");
    assert_eq!(
        recovery.tail_batches,
        (oracle.writes.len() - adopted_round) as u64,
        "{context}"
    );
    oracle.check(&mut db, context);
    assert_only_adopted_outputs(fixture, context);
    // The recovered database keeps committing and recovering.
    oracle.round(&db, fixture, 9);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut db = fixture.open();
    assert_eq!(db.recovery_stats().tail_batches, 0, "{context}");
    oracle.check(&mut db, context);
    assert_only_adopted_outputs(fixture, context);
    db.shutdown().unwrap();
}

#[test]
fn crash_cuts_at_every_commit_step_recover_in_both_tiers() {
    for synchronous in [false, true] {
        for step in ALL_STEPS {
            let context = format!("step {step:?}, synchronous {synchronous}");
            let fixture = per_cell(synchronous);
            let db = fixture.open();
            let mut oracle = Oracle::default();
            oracle.round(&db, &fixture, 1);
            db.flush_index().unwrap();
            let first = oracle.writes.len();
            oracle.round(&db, &fixture, 2);
            db.drain().unwrap();
            db.core.test_hooks.lock().cell_interruption = Some(step);
            assert!(db.flush_index().is_err(), "{context}");
            assert!(db.core.ensure_healthy().is_err(), "{context}");
            let adopted = manifest(&fixture).cell_checkpoint.unwrap().generation;
            let new_root = !matches!(
                step,
                CellStep::Drained
                    | CellStep::Capture
                    | CellStep::OwnerProof
                    | CellStep::Staging
                    | CellStep::Root
                    | CellStep::Wrapper
            );
            assert_eq!(adopted, if new_root { 2 } else { 1 }, "{context}");
            drop(db);
            let replayed_from = if new_root { oracle.writes.len() } else { first };
            recover_after(&fixture, &mut oracle, replayed_from, &context);
        }
    }
}

#[test]
fn manifest_rename_without_directory_sync_recovers_either_outcome() {
    for synchronous in [false, true] {
        // Old directory entry: the staging manifest, which names the
        // unadopted root and wrapper for cleanup.
        let fixture = per_cell(synchronous);
        let db = fixture.open();
        let mut oracle = Oracle::default();
        oracle.round(&db, &fixture, 1);
        db.flush_index().unwrap();
        let first = oracle.writes.len();
        oracle.round(&db, &fixture, 2);
        db.drain().unwrap();
        let (entered, staged) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        {
            let mut hooks = db.core.test_hooks.lock();
            hooks.cell_pause = Some((CellStep::Staging, entered, gate));
            hooks.cell_interruption = Some(CellStep::Manifest);
        }
        let core = Arc::clone(&db.core);
        let flush = thread::spawn(move || core.flush_index());
        staged.recv_timeout(Duration::from_secs(60)).unwrap();
        let path = authority(&fixture).join(MANIFEST);
        let staging_bytes = fs::read(&path).unwrap();
        resume.send(()).unwrap();
        assert!(flush.join().unwrap().is_err());
        drop(db);
        assert_eq!(manifest(&fixture).cell_checkpoint.unwrap().generation, 2);
        fs::write(&path, &staging_bytes).unwrap();
        let restored = manifest(&fixture);
        assert!(restored.checkpoint_staging.is_some());
        assert_eq!(restored.cell_checkpoint.unwrap().generation, 1);
        // Torn leftovers of an older interrupted root also go.
        let epoch = restored.epochs.len() as u64;
        let stray = cell_root_name(epoch, 99);
        fs::write(authority(&fixture).join(&stray), b"torn").unwrap();
        fs::write(
            authority(&fixture).join(cell_root_staging_name(&stray)),
            b"torn",
        )
        .unwrap();
        recover_after(&fixture, &mut oracle, first, "old manifest entry");

        // New directory entry: the adopted generation 2 (the normal case).
        let fixture = per_cell(synchronous);
        let db = fixture.open();
        let mut oracle = Oracle::default();
        oracle.round(&db, &fixture, 1);
        db.flush_index().unwrap();
        oracle.round(&db, &fixture, 2);
        db.drain().unwrap();
        db.core.test_hooks.lock().cell_interruption = Some(CellStep::Manifest);
        assert!(db.flush_index().is_err());
        drop(db);
        let adopted = oracle.writes.len();
        recover_after(&fixture, &mut oracle, adopted, "new manifest entry");
    }
}

#[test]
fn history_is_installed_before_identities_retire() {
    let fixture = per_cell(false);
    let db = fixture.open();
    let mut oracle = Oracle::default();
    oracle.round(&db, &fixture, 1);
    db.drain().unwrap();
    let (entered, installed) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().cell_pause = Some((CellStep::ApplyAuthority, entered, gate));
    let core = Arc::clone(&db.core);
    let flush = thread::spawn(move || core.flush_index());
    installed.recv_timeout(Duration::from_secs(60)).unwrap();
    // The retry window is installed; the identity entries are still live.
    let (operation, mutations, version) = oracle.writes[0].clone();
    assert_eq!(
        db.write_batch(operation, mutations.clone()).unwrap(),
        version
    );
    assert!(db.core.admission.charges().identities >= oracle.writes.len());
    resume.send(()).unwrap();
    flush.join().unwrap().unwrap();
    // Identities retired: the window answers the retry; a different payload
    // under a retired id is still a conflict, never a new write.
    assert_eq!(db.core.admission.charges().identities, 0);
    assert!(db.core.recent_identity(operation));
    assert_eq!(db.write_batch(operation, mutations).unwrap(), version);
    assert!(db.write_batch(operation, vec![put(0, 250)]).is_err());
    drop(db);
}

/// Open a per-cell fixture whose flushers never select a cell by age, so a
/// batch published after the checkpoint drain stays dirty in the root.
fn open_without_age_flushes(fixture: &Fixture) -> RemoteDatabase {
    TEST_FLUSH_POLICY.with(|policy| {
        policy.set(Some(FlushPolicy {
            age: Duration::from_secs(3600),
            ..FlushPolicy::default()
        }))
    });
    let db = fixture.open();
    TEST_FLUSH_POLICY.with(|policy| policy.set(None));
    db
}

#[test]
fn wal_streams_skip_through_f_and_prove_through_u() {
    for synchronous in [false, true] {
        let fixture = per_cell(synchronous);
        let mut db = open_without_age_flushes(&fixture);
        let mut oracle = Oracle::default();
        oracle.round(&db, &fixture, 1);
        db.flush_index().unwrap();
        // The drain flushes this cell, so its coverage reaches this batch;
        // the late batch below then keeps the same cell (and F) just below it.
        oracle.write(&db, &fixture, vec![put(3, 77)]);
        db.drain().unwrap();
        let (entered, drained) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.core.test_hooks.lock().cell_pause = Some((CellStep::Drained, entered, gate));
        let core = Arc::clone(&db.core);
        let flush = thread::spawn(move || core.flush_index());
        drained.recv_timeout(Duration::from_secs(60)).unwrap();
        // Published after the drain, before the root capture: its cell stays
        // dirty, so F stays below it while U = T covers it.
        oracle.write(&db, &fixture, vec![put(3, 78)]);
        db.drain().unwrap();
        resume.send(()).unwrap();
        flush.join().unwrap().unwrap();
        let (operation, _, late) = oracle.writes.last().unwrap().clone();
        let image = wrapper(&fixture);
        assert_eq!(image.max_frame_cut, PrefixCut::Through(late));
        assert!(!image.replay_floor.contains(late));
        assert_eq!(image.retirement.through, image.replay_floor);
        let address = db
            .core
            .publisher
            .lock()
            .known
            .get(&BatchId {
                database: fixture.config.storage.database,
                operation,
            })
            .expect("the batch above F stays in the ledger")
            .address();
        let stream = image
            .streams
            .iter()
            .find(|stream| (stream.partition, stream.epoch) == (address.partition, late.epoch))
            .unwrap();
        // Skip through F only; prove durability through U.
        assert!(stream.last_offset.is_none_or(|last| last < address.offset));
        assert!(stream.durable_end >= address.offset + u64::from(address.len));
        let granted = db.core.admission.charges();
        assert_eq!((granted.replay_batches, granted.identities), (1, 1));
        oracle.check(&mut db, "after the U proof");
        db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
        db.shutdown().unwrap();
        drop(db);
        let mut db = fixture.open();
        assert_eq!(db.recovery_stats().tail_batches, 1);
        oracle.check(&mut db, "after reopen above F");
        db.shutdown().unwrap();
    }
}

/// Spec §7.2 step 4, §9.2 step 1: per-cell retirement raises H through the
/// root-floor lease, clamped by an older reader pin, never above F; the
/// adopted root records it and a reopen keeps it. Both tiers, in WAL-replay
/// and in receipt-history mode.
#[test]
fn per_cell_history_floor_advances_with_pin_clamp_and_survives_reopen() {
    for history in [HistoryPersistence::WalReplay, HistoryPersistence::OnDisk] {
        for synchronous in [false, true] {
            history_floor_advances(per_cell(synchronous).history(history));
        }
    }
}

fn history_floor_advances(fixture: Fixture) {
    let mut db = fixture.open();
    let mut oracle = Oracle::default();
    oracle.round(&db, &fixture, 1);
    db.drain().unwrap();
    db.flush_index().unwrap();
    assert_eq!(db.core.index.retention_floor(), IndexReadCut::BeforeAll);

    // An older pin clamps the requested floor; it stays readable.
    let pinned = MutationVersion {
        batch: oracle.writes[1].2,
        operation_index: 0,
    };
    let older = IndexReadCut::At(pinned);
    let pin = db.core.index.pin_cut(older.clone()).unwrap();
    db.core.retire_history_before(IndexReadCut::Latest).unwrap();
    db.core.ensure_healthy().unwrap();
    assert_eq!(db.core.index.retention_floor(), older);
    let image = wrapper(&fixture);
    assert_eq!(image.history_floor, Some(pinned));
    assert!(image.replay_floor.holds_floor(image.history_floor));
    assert_eq!(runtime(&db).adopted().history_floor, image.history_floor);
    drop(pin);

    // Without the pin, H reaches the requested latest version, <= F.
    oracle.round(&db, &fixture, 2);
    db.drain().unwrap();
    let latest = db.core.publisher.lock().checkpoint_state().latest.unwrap();
    db.core.retire_history_before(IndexReadCut::Latest).unwrap();
    db.core.ensure_healthy().unwrap();
    assert_eq!(db.core.index.retention_floor(), IndexReadCut::At(latest));
    let image = wrapper(&fixture);
    assert_eq!(image.history_floor, Some(latest));
    assert!(image.replay_floor.holds_floor(image.history_floor));
    assert!(matches!(
        db.core.index.pin_cut(older.clone()),
        Err(NativeIndexError::ExpiredCut)
    ));
    oracle.check(&mut db, "after H advance");
    db.shutdown().unwrap();
    drop(db);

    let mut db = fixture.open();
    assert_eq!(db.core.index.retention_floor(), IndexReadCut::At(latest));
    assert_eq!(runtime(&db).adopted().history_floor, Some(latest));
    assert!(matches!(
        db.core.index.pin_cut(older),
        Err(NativeIndexError::ExpiredCut)
    ));
    oracle.check(&mut db, "after reopen with advanced H");
    db.shutdown().unwrap();
}
