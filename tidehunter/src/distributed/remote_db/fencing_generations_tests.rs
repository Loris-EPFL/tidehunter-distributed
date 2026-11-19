//! L1 fencing generations: monotone allocation, bounded retained state and
//! crash cuts at every persisted step. Two owners, so partial fences are real.
//! An injected coordinator crash returns an error at a durable boundary and
//! suppresses the graceful shutdown checkpoint (see `injected`); every earlier
//! step was installed with a synced rename, so the next open sees exactly the
//! durable state at that boundary. Owner crash points restart the owners.

use super::super::pipeline_tests::Fixture;
use super::super::tests::fail_stop_without_checkpoint;
use super::*;
use crate::distributed::codec::Mutation;
use crate::distributed::tcp::OwnerTrimCrash;
use crate::key_shape::KeyType;

struct Harness {
    fixture: Fixture,
    root: PathBuf,
}

impl Harness {
    fn new(synchronous: bool) -> Self {
        let fixture = Fixture::new_with_root(
            tempdir::TempDir::new("remote-fencing-generations").unwrap(),
            2,
            128,
            KeyShape::new_single(4, 1, KeyType::uniform(1)),
            2,
            |_| {},
            |_| {},
        );
        let fixture = if synchronous {
            fixture.synchronous()
        } else {
            fixture
        };
        let root = fixture.temp.path().join("authority");
        Self { fixture, root }
    }

    fn open(&self) -> Result<RemoteDatabase, LocalDbError> {
        self.fixture.try_open_with(self.fixture.config.clone())
    }

    fn open_with(&self, interruption: OpenInterruption) -> Result<RemoteDatabase, LocalDbError> {
        RemoteDatabase::open_inner_local_fixture(
            &self.root,
            &self.fixture.shape,
            self.fixture.config.clone(),
            Some(interruption),
        )
    }

    fn fail_after_one_fence(&self) {
        assert!(self.open_with(OpenInterruption::AfterFences(1)).is_err());
    }

    fn manifest(&self) -> AuthorityManifest {
        let manifest = read_metadata(&self.root.join(MANIFEST)).unwrap();
        validate(&manifest).unwrap();
        manifest
    }

    fn manifest_bytes(&self) -> u64 {
        fs::metadata(self.root.join(MANIFEST)).unwrap().len()
    }

    fn put(&self, db: &RemoteDatabase, key: u32, value: u8) {
        db.write_batch(
            self.fixture
                .operation(key % 2, u64::from(key) + 1_000 * u64::from(value)),
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key.to_be_bytes().to_vec().into(),
                value: vec![value; 32].into(),
            }],
        )
        .unwrap();
    }

    /// Owner generation directories, per owner.
    fn owner_generations(&self) -> Vec<Vec<u64>> {
        (0..2)
            .map(|owner| {
                let mut generations: Vec<u64> =
                    fs::read_dir(self.fixture.owner_database_root(owner))
                        .unwrap()
                        .filter_map(|entry| {
                            let name = entry.unwrap().file_name();
                            let name = name.to_str()?.strip_prefix("epoch_")?.to_owned();
                            u64::from_str_radix(&name, 16).ok()
                        })
                        .collect();
                generations.sort_unstable();
                generations
            })
            .collect()
    }

    fn high_waters(&self) -> Vec<u64> {
        self.fixture
            .nodes
            .iter()
            .map(|node| node.status().unwrap().highest_fenced_generation)
            .collect()
    }

    /// Open, check the seeded values, close cleanly.
    fn cycle(&self) -> u64 {
        let mut db = self.open().unwrap();
        assert_eq!(value(&mut db, 1), Some(vec![7; 32]));
        assert_eq!(value(&mut db, 2), Some(vec![9; 32]));
        let generation = db.epoch();
        db.shutdown().unwrap();
        generation
    }

    /// Generation 1 holds data that is never overwritten; generation 2 is an
    /// empty served generation, the trim candidate of the next open.
    fn seeded(synchronous: bool) -> Self {
        Self::seeded_with(synchronous, Seed::RootByOne)
    }

    fn seeded_with(synchronous: bool, seed: Seed) -> Self {
        let harness = Self::new(synchronous);
        let db = harness.open().unwrap();
        harness.put(&db, 1, 7);
        harness.put(&db, 2, 9);
        match seed {
            Seed::RootByOne => {
                db.shutdown().unwrap();
                drop(db);
            }
            Seed::RootByTwo => fail_stop_without_checkpoint(db),
        }
        assert_eq!(harness.cycle(), 2);
        let root = harness.manifest().checkpoint.unwrap();
        let expected = match seed {
            Seed::RootByOne => 1,
            Seed::RootByTwo => 2,
        };
        assert_eq!(root.epoch, expected);
        harness
    }
}

/// The committed root when generation 2 becomes the trim candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Seed {
    /// Generation 1 produced the root, which cannot name generation 2: the
    /// trim is a plain manifest replacement.
    RootByOne,
    /// Generation 1 stopped without a checkpoint, so the open of generation 2
    /// committed the recovery baseline: the root is produced by generation 2
    /// and the trim adopts a replacement root produced by generation 3.
    RootByTwo,
}

fn value(db: &mut RemoteDatabase, key: u32) -> Option<Vec<u8>> {
    db.get(KeySpace(0), &key.to_be_bytes())
        .unwrap()
        .map(|value| value.to_vec())
}

fn reserved(manifest: &AuthorityManifest) -> Vec<u64> {
    manifest
        .generations
        .iter()
        .filter(|entry| entry.state == GenerationState::Reserved)
        .map(|entry| entry.generation)
        .collect()
}

fn ids(manifest: &AuthorityManifest) -> Vec<u64> {
    manifest
        .generations
        .iter()
        .map(|entry| entry.generation)
        .collect()
}

fn sample_manifest() -> AuthorityManifest {
    let harness = Harness::new(false);
    drop(harness.open().unwrap());
    harness.manifest()
}

#[test]
fn reservation_folds_the_previous_reservation_and_never_reuses_a_generation() {
    let mut manifest = sample_manifest();
    assert_eq!(manifest.next_generation, 2);
    assert_eq!(reserve(&mut manifest, [1; 16]).unwrap(), 2);
    assert_eq!(reserve(&mut manifest, [1; 16]).unwrap(), 3);
    assert_eq!(
        manifest.generations,
        vec![
            AuthorityGeneration {
                generation: 1,
                state: GenerationState::Served,
            },
            AuthorityGeneration {
                generation: 2,
                state: GenerationState::Aborted,
            },
            AuthorityGeneration {
                generation: 3,
                state: GenerationState::Reserved,
            },
        ]
    );
    assert_eq!(manifest.next_generation, 4);
    validate(&manifest).unwrap();
    assert!(mark_served(&mut manifest, 2).is_err());
    mark_served(&mut manifest, 3).unwrap();
    assert_eq!(
        retained_below(&manifest, 4),
        vec![(1, false), (2, true), (3, false)]
    );

    let mut two_reserved = manifest.clone();
    two_reserved.generations[1].state = GenerationState::Reserved;
    assert!(validate(&two_reserved).is_err());
    let mut pending_retained = manifest.clone();
    pending_retained.trim_pending = Some(2);
    assert!(validate(&pending_retained).is_err());
    let mut unsorted = manifest.clone();
    unsorted.generations.swap(0, 1);
    assert!(validate(&unsorted).is_err());

    let mut exhausted = manifest.clone();
    exhausted.next_generation = u64::MAX;
    let before = exhausted.clone();
    assert!(
        reserve(&mut exhausted, [0xab; 16])
            .unwrap_err()
            .to_string()
            .starts_with("fencing generation exhausted for database abab")
    );
    assert_eq!(exhausted.generations, before.generations);
    assert_eq!(exhausted.next_generation, u64::MAX);
}

#[test]
fn adoption_trim_excludes_the_generation_only_at_root_adoption() {
    let mut manifest = sample_manifest();
    reserve(&mut manifest, [1; 16]).unwrap();
    mark_served(&mut manifest, 2).unwrap();
    manifest.adopt_trim = Some(1);
    // The staging install never carries the in-memory intent.
    let staged: AuthorityManifest =
        serde_yaml::from_str(&serde_yaml::to_string(&manifest).unwrap()).unwrap();
    assert_eq!(staged.adopt_trim, None);
    assert_eq!(ids(&staged), vec![1, 2]);
    apply_adoption_trim(&mut manifest);
    assert_eq!(ids(&manifest), vec![2]);
    assert_eq!(manifest.trim_pending, Some(1));
    assert_eq!(manifest.adopt_trim, None);
    validate(&manifest).unwrap();
}

fn cycles(synchronous: bool, cycles: usize, failed: usize) {
    // Every open trims its predecessor through a replacement root: the root
    // committed by the previous open is produced by the trim candidate.
    let harness = Harness::seeded_with(synchronous, Seed::RootByTwo);
    let base_bytes = harness.manifest_bytes();
    let mut next = harness.manifest().next_generation;
    let mut high_waters = harness.high_waters();
    let every = (cycles / failed.max(1)).max(1);
    let mut failures = 0;
    for cycle in 0..cycles {
        if failures < failed && cycle % every == 0 {
            harness.fail_after_one_fence();
            failures += 1;
            let manifest = harness.manifest();
            assert_eq!(manifest.next_generation, next + 1);
            assert_eq!(reserved(&manifest), vec![next]);
            let after = harness.high_waters();
            assert_eq!(after, vec![next, high_waters[1]]);
            next += 1;
            high_waters = after;
        }
        assert_eq!(harness.cycle(), next);
        let manifest = harness.manifest();
        assert_eq!(manifest.next_generation, next + 1);
        assert!(reserved(&manifest).is_empty());
        assert_eq!(manifest.trim_pending, None);
        // Generation 1 stays referenced; every older empty or aborted
        // generation was trimmed by this open.
        assert_eq!(ids(&manifest), vec![1, next]);
        let after = harness.high_waters();
        assert!(after.iter().zip(&high_waters).all(|(new, old)| new > old));
        assert_eq!(after, vec![next, next]);
        high_waters = after;
        assert_eq!(harness.owner_generations(), vec![vec![1, next]; 2]);
        assert!(durable_cut::truncation_count_for_test(&harness.root) <= 2);
        assert!(
            harness.manifest_bytes() <= base_bytes + 256,
            "manifest grew from {base_bytes} to {} bytes",
            harness.manifest_bytes()
        );
        next += 1;
    }
    assert_eq!(failures, failed);
}

#[test]
fn open_close_cycles_and_failed_opens_keep_retained_state_bounded_native() {
    cycles(false, 48, 8);
}

#[test]
fn open_close_cycles_and_failed_opens_keep_retained_state_bounded_synchronous() {
    cycles(true, 24, 4);
}

#[test]
#[ignore = "L1 acceptance: 5,000 open/close cycles and 100 failed opens; run explicitly"]
fn five_thousand_cycles_and_one_hundred_failed_opens_keep_retained_state_bounded() {
    cycles(false, 5_000, 100);
}

/// After the reopen, the authority and both owners hold exactly generation 1
/// and the new served generation, with no pending intent.
fn assert_recovered(harness: &Harness) {
    let generation = harness.cycle();
    let manifest = harness.manifest();
    assert_eq!(ids(&manifest), vec![1, generation]);
    assert_eq!(manifest.trim_pending, None);
    assert_eq!(harness.owner_generations(), vec![vec![1, generation]; 2]);
    // A second reopen sees the same durable state.
    let again = harness.cycle();
    assert_eq!(again, generation + 1);
    assert_eq!(ids(&harness.manifest()), vec![1, again]);
}

#[test]
fn coordinator_crash_cuts_before_and_during_reservation_and_fencing() {
    for synchronous in [false, true] {
        // After the reservation is durable, before any fence.
        let harness = Harness::seeded(synchronous);
        assert!(
            harness
                .open_with(OpenInterruption::AfterReservation)
                .is_err()
        );
        let manifest = harness.manifest();
        assert_eq!(reserved(&manifest), vec![3]);
        assert_eq!(harness.high_waters(), vec![2, 2]);
        assert_recovered(&harness);

        // Two consecutive partial fences: the first reservation is folded into
        // Aborted by the second, so no durable manifest holds two reservations
        // and no owner receives the same generation twice.
        let harness = Harness::seeded(synchronous);
        harness.fail_after_one_fence();
        assert_eq!(harness.high_waters(), vec![3, 2]);
        harness.fail_after_one_fence();
        let manifest = harness.manifest();
        assert_eq!(reserved(&manifest), vec![4]);
        assert!(manifest.generations.contains(&AuthorityGeneration {
            generation: 3,
            state: GenerationState::Aborted,
        }));
        assert_eq!(harness.high_waters(), vec![4, 2]);
        assert_recovered(&harness);

        // After the open-time cut record, before Served(g).
        let harness = Harness::seeded(synchronous);
        assert!(harness.open_with(OpenInterruption::AfterCutRecord).is_err());
        assert_eq!(reserved(&harness.manifest()), vec![3]);
        assert_eq!(harness.high_waters(), vec![3, 3]);
        assert_recovered(&harness);

        // After Served(g), before any trim: g is never aborted or reused.
        let harness = Harness::seeded(synchronous);
        assert!(harness.open_with(OpenInterruption::AfterServed).is_err());
        let manifest = harness.manifest();
        assert_eq!(ids(&manifest), vec![1, 2, 3]);
        assert!(is_served(&manifest, 3));
        assert_recovered(&harness);
    }
}

#[test]
fn coordinator_crash_cuts_inside_the_trim_transaction() {
    let points = [
        TrimCrash::BeforeCommit,
        TrimCrash::AfterCommit,
        TrimCrash::AfterOwnerAcks(1),
        TrimCrash::AfterOwnerAcks(2),
        TrimCrash::BeforeClear,
    ];
    for (synchronous, seed) in [false, true]
        .into_iter()
        .flat_map(|synchronous| [Seed::RootByOne, Seed::RootByTwo].map(|seed| (synchronous, seed)))
    {
        for point in points {
            let harness = Harness::seeded_with(synchronous, seed);
            assert!(harness.open_with(OpenInterruption::Trim(point)).is_err());
            let manifest = harness.manifest();
            let owners = harness.owner_generations();
            match point {
                TrimCrash::BeforeCommit => {
                    assert_eq!(ids(&manifest), vec![1, 2, 3]);
                    assert_eq!(manifest.trim_pending, None);
                    assert_eq!(owners, vec![vec![1, 2, 3]; 2]);
                }
                TrimCrash::AfterCommit => {
                    // The exclusion, the pending trim and (when the root named
                    // generation 2) the replacement root are durable together;
                    // generation 2 is never planned for default replay again.
                    assert_eq!(ids(&manifest), vec![1, 3]);
                    assert_eq!(manifest.trim_pending, Some(2));
                    let root = manifest.checkpoint.as_ref().unwrap().epoch;
                    match seed {
                        Seed::RootByOne => assert_eq!(root, 1),
                        Seed::RootByTwo => assert_eq!(root, 3),
                    }
                    assert_eq!(owners, vec![vec![1, 2, 3]; 2]);
                }
                TrimCrash::AfterOwnerAcks(1) => {
                    assert_eq!(manifest.trim_pending, Some(2));
                    assert_eq!(owners, vec![vec![1, 3], vec![1, 2, 3]]);
                }
                TrimCrash::AfterOwnerAcks(_) | TrimCrash::BeforeClear => {
                    assert_eq!(manifest.trim_pending, Some(2));
                    assert_eq!(owners, vec![vec![1, 3]; 2]);
                }
            }
            assert_recovered(&harness);
        }
    }
}

#[test]
fn owner_crash_inside_trim_resumes_at_owner_restart() {
    let points = [OwnerTrimCrash::AfterLedger, OwnerTrimCrash::AfterUnlink];
    for (synchronous, point) in [false, true]
        .into_iter()
        .flat_map(|synchronous| points.map(|point| (synchronous, point)))
    {
        let mut harness = Harness::seeded_with(synchronous, Seed::RootByTwo);
        harness.fixture.nodes[0].fail_next_generation_trim(point);
        assert!(harness.open().is_err());
        let manifest = harness.manifest();
        assert_eq!(manifest.trim_pending, Some(2));
        harness.fixture.stop_owners();
        harness.fixture.restart_owners().unwrap();
        if synchronous {
            harness.fixture = harness.fixture.synchronous();
        }
        // Owner restart finished the named deletion before serving.
        assert_eq!(harness.owner_generations()[0], vec![1, 3]);
        assert_recovered(&harness);
    }
}

#[test]
fn retained_generation_unknown_to_the_authority_fails_closed() {
    let harness = Harness::seeded(false);
    let mut manifest = harness.manifest();
    manifest.generations.retain(|entry| entry.generation != 1);
    persist_manifest(&harness.root, &manifest, CoordinatorIoCategory::Open).unwrap();
    let error = harness.open().err().unwrap();
    assert!(
        error
            .to_string()
            .contains("fencing inventory disagrees: unknown generation 1"),
        "{error}"
    );
}

#[test]
fn exhausted_generation_counter_mutates_nothing() {
    let harness = Harness::seeded(false);
    let mut manifest = harness.manifest();
    manifest.next_generation = u64::MAX;
    persist_manifest(&harness.root, &manifest, CoordinatorIoCategory::Open).unwrap();
    let before = fs::read(harness.root.join(MANIFEST)).unwrap();
    let error = harness.open().err().unwrap();
    assert!(
        error
            .to_string()
            .starts_with("fencing generation exhausted for database"),
        "{error}"
    );
    assert_eq!(fs::read(harness.root.join(MANIFEST)).unwrap(), before);
    assert_eq!(harness.high_waters(), vec![2, 2]);
}

#[test]
fn aborted_generation_with_accepted_data_fails_closed() {
    let harness = Harness::new(true);
    let db = harness.open().unwrap();
    harness.put(&db, 1, 7);
    db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
    drop(db);
    let mut manifest = harness.manifest();
    assert!(manifest.checkpoint.is_none());
    manifest.generations[0].state = GenerationState::Aborted;
    persist_manifest(&harness.root, &manifest, CoordinatorIoCategory::Open).unwrap();
    let error = harness.open().err().unwrap();
    assert_eq!(
        error.to_string(),
        "aborted fencing generation 1 contains accepted WAL data"
    );
}

#[test]
fn repeated_failed_opens_leave_aborted_reservations_that_the_next_open_trims() {
    for synchronous in [false, true] {
        let harness = Harness::seeded(synchronous);
        // Two reservations that no owner saw, then one partial fence: owner 0
        // activated generation 5, owner 1 never heard of 3, 4 or 5.
        for _ in 0..2 {
            assert!(
                harness
                    .open_with(OpenInterruption::AfterReservation)
                    .is_err()
            );
        }
        harness.fail_after_one_fence();
        let manifest = harness.manifest();
        assert_eq!(
            manifest.generations,
            vec![
                AuthorityGeneration {
                    generation: 1,
                    state: GenerationState::Served,
                },
                AuthorityGeneration {
                    generation: 2,
                    state: GenerationState::Served,
                },
                AuthorityGeneration {
                    generation: 3,
                    state: GenerationState::Aborted,
                },
                AuthorityGeneration {
                    generation: 4,
                    state: GenerationState::Aborted,
                },
                AuthorityGeneration {
                    generation: 5,
                    state: GenerationState::Reserved,
                },
            ]
        );
        assert_eq!(manifest.next_generation, 6);
        assert_eq!(harness.high_waters(), vec![5, 2]);
        assert_eq!(harness.owner_generations(), vec![vec![1, 2, 5], vec![1, 2]]);
        // The next open folds 5 into Aborted, serves 6 and trims 2 to 5: the
        // never-fenced reservations are absent on both owners, and the
        // partially fenced one is an empty sealed stream on owner 0.
        assert_eq!(harness.cycle(), 6);
        let manifest = harness.manifest();
        assert_eq!(ids(&manifest), vec![1, 6]);
        assert_eq!(manifest.trim_pending, None);
        assert_eq!(harness.high_waters(), vec![6, 6]);
        assert_eq!(harness.owner_generations(), vec![vec![1, 6]; 2]);
        assert_recovered(&harness);
    }
}

#[test]
fn unreferenced_generation_with_data_stays_retained_until_its_files_retire() {
    for synchronous in [false, true] {
        let harness = Harness::seeded(synchronous);
        // Generation 3 overwrites every key of generation 1, so no index value
        // names generation 1 any more; its WAL files still exist on the owners.
        let mut db = harness.open().unwrap();
        assert_eq!(db.epoch(), 3);
        harness.put(&db, 1, 8);
        harness.put(&db, 2, 10);
        assert_eq!(value(&mut db, 1), Some(vec![8; 32]));
        db.shutdown().unwrap();
        drop(db);
        assert_eq!(ids(&harness.manifest()), vec![1, 3]);
        for expected in [4, 5] {
            let mut db = harness.open().unwrap();
            assert_eq!(db.epoch(), expected);
            assert_eq!(value(&mut db, 1), Some(vec![8; 32]));
            assert_eq!(value(&mut db, 2), Some(vec![10; 32]));
            db.shutdown().unwrap();
            drop(db);
            // Generation 1 is never trimmed while its files are present (the
            // owner reports it Blocked); generation 3 holds the live values;
            // the empty generation before this open is gone.
            let manifest = harness.manifest();
            assert_eq!(ids(&manifest), vec![1, 3, expected]);
            assert_eq!(manifest.trim_pending, None);
            assert_eq!(harness.owner_generations(), vec![vec![1, 3, expected]; 2]);
        }
    }
}

#[test]
fn adopted_callback_start_keeps_a_generation_with_a_remaining_suffix() {
    let harness = Harness::new(true);
    let db = harness.open().unwrap();
    let generation = db.epoch();
    let key = (0, generation);
    let end = harness.fixture.config.storage.wal_file_bytes;
    {
        // Isolate the eligibility check: a complete stream, no retained
        // references or receipts, and one file in the adopted callback suffix.
        let mut state = db.core.publisher.lock();
        let state = state.checkpoint_state();
        state.positions.insert(
            key,
            StreamPosition {
                partition: 0,
                epoch: generation,
                replay_from: end,
                last_offset: None,
                durable_end: end,
                complete: true,
            },
        );
        state.callback_starts.insert(key, 0);
    }
    assert!(!coordinator_eligible(&db.core, None, generation));
    db.core.publisher.lock().callback_starts.insert(key, end);
    assert!(coordinator_eligible(&db.core, None, generation));
    fail_stop_without_checkpoint(db);
}

#[test]
fn empty_generation_the_root_does_not_name_is_trimmed_without_a_replacement_root() {
    for synchronous in [false, true] {
        let harness = Harness::new(synchronous);
        // The root is produced by generation 1. Generation 2 starts after it,
        // writes nothing and stops without a checkpoint, so no root names it.
        let db = harness.open().unwrap();
        harness.put(&db, 1, 7);
        db.flush_index().unwrap();
        fail_stop_without_checkpoint(db);
        let db = harness.open().unwrap();
        assert_eq!(db.epoch(), 2);
        fail_stop_without_checkpoint(db);
        let root = harness.manifest().checkpoint.unwrap();
        assert_eq!(root.epoch, 1);

        let mut db = harness.open().unwrap();
        assert_eq!(db.epoch(), 3);
        assert_eq!(value(&mut db, 1), Some(vec![7; 32]));
        // A plain manifest replacement trimmed generation 2: the root is the
        // same committed image, and no replay of generation 2 is planned.
        let manifest = harness.manifest();
        assert_eq!(ids(&manifest), vec![1, 3]);
        assert_eq!(manifest.trim_pending, None);
        let after = manifest.checkpoint.unwrap();
        assert_eq!(
            (after.epoch, after.generation, after.metadata_digest),
            (root.epoch, root.generation, root.metadata_digest)
        );
        assert_eq!(harness.owner_generations(), vec![vec![1, 3]; 2]);
        db.shutdown().unwrap();
        drop(db);
        let mut db = harness.open().unwrap();
        assert_eq!(db.epoch(), 4);
        assert_eq!(value(&mut db, 1), Some(vec![7; 32]));
        db.shutdown().unwrap();
        assert_eq!(ids(&harness.manifest()), vec![1, 4]);
    }
}
