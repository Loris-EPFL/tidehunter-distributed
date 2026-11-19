use super::*;

const DB: [u8; 16] = [7; 16];

fn config(cut: CutRecovery) -> DurableCutConfig {
    DurableCutConfig {
        cut,
        ..DurableCutConfig::default()
    }
}

fn record(live: Option<EpochCut>, truncations: Vec<EpochCut>) -> CutRecord {
    CutRecord {
        format: FORMAT,
        database: DB,
        live,
        owners: Vec::new(),
        truncations,
    }
}

fn version(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

/// Replay of epoch 2 with sequences 0..=7 except the lost 5.
fn replay(plan: &mut RecoveryPlan) -> Vec<u64> {
    [0, 1, 2, 3, 4, 6, 7]
        .into_iter()
        .filter(|&sequence| plan.keep(version(2, sequence)))
        .collect()
}

/// Only a missing record means "no record". A directory in its place (EISDIR on
/// read) or a file in place of the authority directory (ENOTDIR) fails the load.
#[test]
fn unreadable_record_fails_instead_of_reading_as_absent() {
    let directory = tempdir::TempDir::new("durable-cut-record-errors").unwrap();
    assert_eq!(CutRecord::load(directory.path(), DB, 5).unwrap(), None);
    std::fs::create_dir(directory.path().join(RECORD)).unwrap();
    let error = CutRecord::load(directory.path(), DB, 5).unwrap_err();
    assert!(error.message().contains(RECORD), "{error}");
    let not_directory = directory.path().join("authority-file");
    std::fs::write(&not_directory, b"x").unwrap();
    assert!(CutRecord::load(&not_directory, DB, 5).is_err());
}

#[test]
fn record_round_trips_and_rejects_foreign_or_future_state() {
    let directory = tempdir::TempDir::new("durable-cut-record").unwrap();
    assert_eq!(CutRecord::load(directory.path(), DB, 5).unwrap(), None);
    let stored = record(
        Some(EpochCut { epoch: 4, next: 9 }),
        vec![EpochCut { epoch: 2, next: 3 }],
    );
    stored.persist(directory.path()).unwrap();
    assert_eq!(
        CutRecord::load(directory.path(), DB, 5).unwrap(),
        Some(stored.clone())
    );
    assert!(CutRecord::load(directory.path(), [8; 16], 5).is_err());
    // A record naming an epoch the authority never reserved is rejected.
    assert!(CutRecord::load(directory.path(), DB, 4).is_err());
    record(
        None,
        vec![
            EpochCut { epoch: 3, next: 0 },
            EpochCut { epoch: 2, next: 0 },
        ],
    )
    .persist(directory.path())
    .unwrap();
    assert!(CutRecord::load(directory.path(), DB, 5).is_err());
    record(
        Some(EpochCut { epoch: 2, next: 0 }),
        vec![EpochCut { epoch: 2, next: 0 }],
    )
    .persist(directory.path())
    .unwrap();
    assert!(CutRecord::load(directory.path(), DB, 5).is_err());
}

#[test]
fn strict_keeps_only_the_persisted_cut() {
    let live = Some(EpochCut { epoch: 2, next: 3 });
    let mut plan = RecoveryPlan::new(
        Some(record(live, Vec::new())),
        DB,
        &config(CutRecovery::Strict),
        None,
        &[],
    );
    assert_eq!(replay(&mut plan), [0, 1, 2]);
    assert_eq!((plan.discarded, plan.extended), (4, 0));
    let (record, cut) = plan.finish(3, true, &[]);
    assert_eq!(cut, Some(RecoveredCut { epoch: 2, next: 3 }));
    let record = record.unwrap();
    assert_eq!(record.truncations, [EpochCut { epoch: 2, next: 3 }]);
    assert_eq!(record.live, Some(EpochCut { epoch: 3, next: 0 }));
}

#[test]
fn extend_stops_at_the_first_missing_version() {
    let live = Some(EpochCut { epoch: 2, next: 3 });
    let mut plan = RecoveryPlan::new(
        Some(record(live, Vec::new())),
        DB,
        &config(CutRecovery::Extend),
        None,
        &[],
    );
    assert_eq!(replay(&mut plan), [0, 1, 2, 3, 4]);
    assert_eq!((plan.discarded, plan.extended), (2, 2));
    let (record, cut) = plan.finish(3, false, &[]);
    assert_eq!(cut, Some(RecoveredCut { epoch: 2, next: 5 }));
    let record = record.unwrap();
    assert_eq!(record.truncations, [EpochCut { epoch: 2, next: 5 }]);
    assert_eq!(record.live, None);
}

#[test]
fn extend_from_nothing_requires_sequence_zero() {
    let live = Some(EpochCut { epoch: 2, next: 0 });
    let mut plan = RecoveryPlan::new(
        Some(record(live, Vec::new())),
        DB,
        &config(CutRecovery::Extend),
        None,
        &[],
    );
    // Sequence 0 was aborted: nothing of the epoch survives.
    assert!(!plan.keep(version(2, 1)));
    assert!(!plan.keep(version(2, 2)));
    assert_eq!(
        plan.finish(3, true, &[]).1,
        Some(RecoveredCut { epoch: 2, next: 0 })
    );
}

#[test]
fn checkpoint_raises_the_start_of_the_run() {
    let live = Some(EpochCut { epoch: 2, next: 1 });
    let mut plan = RecoveryPlan::new(
        Some(record(live, Vec::new())),
        DB,
        &config(CutRecovery::Strict),
        Some(version(2, 4)),
        &[],
    );
    // The checkpoint covered 0..=4; replay resumes after it.
    assert!(!plan.keep(version(2, 6)));
    assert_eq!(
        plan.finish(3, true, &[]).1,
        Some(RecoveredCut { epoch: 2, next: 5 })
    );
    // A checkpoint of an older epoch does not raise the bound.
    let plan = RecoveryPlan::new(
        Some(record(live, Vec::new())),
        DB,
        &config(CutRecovery::Strict),
        Some(version(1, 9)),
        &[],
    );
    assert_eq!(
        plan.finish(3, true, &[]).1,
        Some(RecoveredCut { epoch: 2, next: 1 })
    );
}

#[test]
fn recorded_truncations_are_always_honoured() {
    let truncations = vec![EpochCut { epoch: 2, next: 3 }];
    for recovery in [NativeRecovery::DurablePrefix, NativeRecovery::PerOwner] {
        let mut plan = RecoveryPlan::new(
            Some(record(None, truncations.clone())),
            DB,
            &DurableCutConfig {
                recovery,
                ..DurableCutConfig::default()
            },
            None,
            &[],
        );
        assert_eq!(replay(&mut plan), [0, 1, 2]);
        // Untracked epochs are unbounded.
        assert!(plan.keep(version(3, 40)));
        let (record, cut) = plan.finish(4, false, &[]);
        assert_eq!(cut, None);
        assert_eq!(record.unwrap().truncations, truncations);
    }
}

#[test]
fn per_owner_open_keeps_the_tracked_epoch_and_clears_it() {
    let live = Some(EpochCut { epoch: 2, next: 0 });
    let mut plan = RecoveryPlan::new(
        Some(record(live, Vec::new())),
        DB,
        &DurableCutConfig {
            recovery: NativeRecovery::PerOwner,
            ..DurableCutConfig::default()
        },
        None,
        &[],
    );
    assert_eq!(replay(&mut plan), [0, 1, 2, 3, 4, 6, 7]);
    let (record, cut) = plan.finish(3, false, &[]);
    assert_eq!(cut, None);
    let record = record.unwrap();
    assert_eq!((record.live, record.truncations), (None, Vec::new()));
}

#[test]
fn no_record_and_no_tracking_writes_nothing() {
    let mut plan = RecoveryPlan::new(None, DB, &DurableCutConfig::default(), None, &[]);
    assert_eq!(replay(&mut plan), [0, 1, 2, 3, 4, 6, 7]);
    assert_eq!(plan.finish(3, false, &[]), (None, None));
    let plan = RecoveryPlan::new(None, DB, &DurableCutConfig::default(), None, &[]);
    let (record, cut) = plan.finish(3, true, &[]);
    assert_eq!(cut, None);
    assert_eq!(
        record,
        Some(self::record(
            Some(EpochCut { epoch: 3, next: 0 }),
            Vec::new()
        ))
    );
}

const INCARNATION: [u8; 16] = [1; 16];
const OTHER_INCARNATION: [u8; 16] = [2; 16];

fn mark(partition: u32, synced_next: u64) -> OwnerMark {
    OwnerMark {
        partition,
        incarnation: INCARNATION,
        synced_next,
    }
}

fn fence(partition: u32, incarnation: [u8; 16], recovery_clean: bool) -> OwnerFence {
    OwnerFence {
        partition,
        incarnation,
        recovery_clean,
    }
}

/// Epoch 2, persisted cut 3, owner 0 synced through 3, owner 1 through `synced`.
fn continuity_plan(cut: CutRecovery, owner1_synced: u64, fenced: &[OwnerFence]) -> RecoveryPlan {
    let mut stored = record(Some(EpochCut { epoch: 2, next: 3 }), Vec::new());
    stored.owners = vec![mark(0, 4), mark(1, owner1_synced)];
    RecoveryPlan::new(Some(stored), DB, &config(cut), None, fenced)
}

/// Coordinator-only crash: every owner process kept running, so the missing 5
/// was never written and the acknowledged 6 and 7 survive.
#[test]
fn continuous_owners_skip_a_never_written_version() {
    let fenced = [fence(0, INCARNATION, true), fence(1, INCARNATION, true)];
    let mut plan = continuity_plan(CutRecovery::Extend, 1, &fenced);
    assert_eq!(plan.continuity_bound, Some(u64::MAX));
    assert_eq!(replay(&mut plan), [0, 1, 2, 3, 4, 6, 7]);
    assert_eq!((plan.discarded, plan.extended, plan.skipped), (0, 4, 1));
    assert_eq!(
        plan.finish(3, true, &fenced).1,
        Some(RecoveredCut { epoch: 2, next: 8 })
    );
}

/// A restarted owner (new incarnation) or one whose recovery found a hole or
/// that saw a persistence failure bounds the skip at its recorded synced end:
/// below it nothing was lost, at or above it the run stops.
#[test]
fn a_discontinuous_owner_bounds_the_skip_at_its_recorded_sync() {
    for discontinuity in [
        fence(1, OTHER_INCARNATION, true),
        fence(1, INCARNATION, false),
        fence(1, OTHER_INCARNATION, false),
    ] {
        let fenced = [fence(0, INCARNATION, true), discontinuity];
        for (synced, kept) in [(6, &[0, 1, 2, 3, 4, 6, 7][..]), (5, &[0, 1, 2, 3, 4])] {
            let mut plan = continuity_plan(CutRecovery::Extend, synced, &fenced);
            assert_eq!(plan.continuity_bound, Some(synced));
            assert_eq!(replay(&mut plan), kept, "{discontinuity:?} synced={synced}");
        }
    }
}

/// Without evidence for an owner the bound is 0: Extend stops at the first gap.
#[test]
fn missing_continuity_evidence_stops_at_the_first_gap() {
    let unmarked = [
        fence(0, INCARNATION, true),
        fence(1, INCARNATION, true),
        fence(2, INCARNATION, true),
    ];
    let mut plan = continuity_plan(CutRecovery::Extend, 9, &unmarked);
    assert_eq!(plan.continuity_bound, Some(0));
    assert_eq!(replay(&mut plan), [0, 1, 2, 3, 4]);
    let mut plan = RecoveryPlan::new(
        Some(record(Some(EpochCut { epoch: 2, next: 3 }), Vec::new())),
        DB,
        &config(CutRecovery::Extend),
        None,
        &unmarked,
    );
    assert_eq!(replay(&mut plan), [0, 1, 2, 3, 4]);
}

#[test]
fn strict_ignores_continuity() {
    let fenced = [fence(0, INCARNATION, true), fence(1, INCARNATION, true)];
    let mut plan = continuity_plan(CutRecovery::Strict, 9, &fenced);
    assert_eq!(plan.continuity_bound, None);
    assert_eq!(replay(&mut plan), [0, 1, 2]);
}

/// A tracked epoch starts with every fenced owner's incarnation and nothing synced; an
/// untracked one has no marks, and a record with marks but no tracked epoch is
/// rejected.
#[test]
fn finish_marks_every_fenced_owner_of_a_tracked_epoch() {
    let fenced = [
        fence(4, INCARNATION, true),
        fence(9, OTHER_INCARNATION, false),
    ];
    let plan = RecoveryPlan::new(None, DB, &config(CutRecovery::Extend), None, &fenced);
    let record = plan.finish(3, true, &fenced).0.unwrap();
    assert_eq!(
        record.owners,
        [
            OwnerMark {
                partition: 4,
                incarnation: INCARNATION,
                synced_next: 0
            },
            OwnerMark {
                partition: 9,
                incarnation: OTHER_INCARNATION,
                synced_next: 0
            }
        ]
    );
    let directory = tempdir::TempDir::new("durable-cut-marks").unwrap();
    record.persist(directory.path()).unwrap();
    assert_eq!(
        CutRecord::load(directory.path(), DB, 4).unwrap(),
        Some(record.clone())
    );
    let untracked = RecoveryPlan::new(
        Some(record),
        DB,
        &config(CutRecovery::Extend),
        None,
        &fenced,
    )
    .finish(4, false, &fenced)
    .0
    .unwrap();
    assert!(untracked.owners.is_empty());
    let mut orphan = untracked;
    orphan.owners = vec![mark(4, 0)];
    orphan.persist(directory.path()).unwrap();
    assert!(CutRecord::load(directory.path(), DB, 5).is_err());
}

/// A format 1 record (marks with the kernel boot id) fails the open: it never
/// reads as format 2 and never reads as absent.
#[test]
fn format_1_record_fails_to_load() {
    #[derive(Serialize)]
    struct BootMark {
        partition: u32,
        boot_id: [u8; 16],
        synced_next: u64,
    }
    #[derive(Serialize)]
    struct FormatOne {
        format: u32,
        database: [u8; 16],
        live: Option<EpochCut>,
        owners: Vec<BootMark>,
        truncations: Vec<EpochCut>,
    }
    let directory = tempdir::TempDir::new("durable-cut-format-1").unwrap();
    let live = Some(EpochCut { epoch: 2, next: 1 });
    let old = FormatOne {
        format: 1,
        database: DB,
        live,
        owners: vec![BootMark {
            partition: 0,
            boot_id: INCARNATION,
            synced_next: 1,
        }],
        truncations: Vec::new(),
    };
    install_metadata(directory.path(), RECORD, &encode_metadata(&old).unwrap()).unwrap();
    assert!(CutRecord::load(directory.path(), DB, 3).is_err());
    let mut renamed = record(live, Vec::new());
    renamed.owners = vec![mark(0, 1)];
    renamed.format = 1;
    renamed.persist(directory.path()).unwrap();
    assert!(CutRecord::load(directory.path(), DB, 3).is_err());
}

fn evaluate(
    tracker: &mut Tracker,
    at: Instant,
    published: u64,
    receipted: &[u64],
) -> (u64, Vec<usize>) {
    let mut nudge = Vec::new();
    let cut = tracker.evaluate(at, published, receipted, &mut nudge);
    (cut, nudge)
}

#[test]
fn cut_is_the_minimum_over_pinning_owners_and_the_floor() {
    let mut tracker = Tracker::new(2, &DurableCutConfig::default());
    let now = Instant::now();
    // {x=1,y=1} is v0 on owner 0 (synced); {x=2} is v1 on owner 1 (unsynced).
    tracker.report(0, 1, 100);
    assert_eq!(evaluate(&mut tracker, now, 2, &[1, 2]).0, 0);
    tracker.report(1, 0, 100);
    // Owner 1 pins the cut at its synced end; v1 stays above the cut.
    assert_eq!(evaluate(&mut tracker, now, 2, &[1, 2]).0, 0);
    tracker.report(1, 2, 120);
    assert_eq!(evaluate(&mut tracker, now, 2, &[1, 2]).0, 2);
    // The published floor bounds the cut even when every owner is synced.
    tracker.report(0, 9, 140);
    assert_eq!(evaluate(&mut tracker, now, 3, &[9, 2]).0, 3);
}

#[test]
fn idle_owner_does_not_pin_the_cut() {
    let mut tracker = Tracker::new(2, &DurableCutConfig::default());
    let now = Instant::now();
    // Owner 1 never received a batch: its synced end stays 0 but it is idle.
    tracker.report(0, 6, 300);
    assert_eq!(evaluate(&mut tracker, now, 6, &[6, 0]).0, 6);
    // A receipt makes owner 1 non-idle until a report covers it.
    assert_eq!(evaluate(&mut tracker, now, 8, &[6, 8]).0, 6);
    tracker.report(1, 8, 50);
    assert_eq!(evaluate(&mut tracker, now, 8, &[6, 8]).0, 8);
}

#[test]
fn cut_never_moves_back_and_certification_raises_it() {
    let mut tracker = Tracker::new(1, &DurableCutConfig::default());
    let now = Instant::now();
    tracker.report(0, 4, 10);
    assert_eq!(evaluate(&mut tracker, now, 4, &[4]).0, 4);
    tracker.certify(7);
    assert_eq!(evaluate(&mut tracker, now, 7, &[9]).0, 7);
    // A stale report cannot lower the owner's synced end or the cut.
    tracker.report(0, 2, 5);
    assert_eq!(evaluate(&mut tracker, now, 9, &[9]).0, 7);
}

#[test]
fn lagging_owners_are_nudged_after_the_age_bound() {
    let mut tracker = Tracker::new(3, &DurableCutConfig::default());
    let start = Instant::now();
    tracker.report(0, 2, 10);
    tracker.report(1, 1, 10);
    tracker.report(2, 5, 10);
    // Owners 0 and 1 hold receipted batches above their synced ends; owner 2 is idle.
    let receipted = [4, 3, 5];
    assert_eq!(
        evaluate(&mut tracker, start, 5, &receipted),
        (1, Vec::new())
    );
    let later = start + Duration::from_millis(999);
    assert_eq!(
        evaluate(&mut tracker, later, 5, &receipted).1,
        Vec::<usize>::new()
    );
    let late = start + Duration::from_millis(1000);
    assert_eq!(evaluate(&mut tracker, late, 5, &receipted).1, [0, 1]);
    // Once the cut covers the oldest floor, the age restarts.
    tracker.report(0, 5, 10);
    tracker.report(1, 5, 10);
    assert_eq!(evaluate(&mut tracker, late, 5, &receipted), (5, Vec::new()));
}

#[test]
fn lagging_owners_are_nudged_after_the_byte_bound() {
    let mut tracker = Tracker::new(2, &DurableCutConfig::default());
    let now = Instant::now();
    assert_eq!(evaluate(&mut tracker, now, 0, &[0, 0]).0, 0);
    tracker.report(0, 0, 32 << 20);
    tracker.report(1, 2, 31 << 20);
    assert_eq!(
        evaluate(&mut tracker, now, 2, &[2, 2]).1,
        Vec::<usize>::new()
    );
    tracker.report(1, 2, (32 << 20) + 1);
    assert_eq!(evaluate(&mut tracker, now, 2, &[2, 2]), (0, vec![0]));
}

#[test]
fn full_sample_queue_keeps_the_oldest_age() {
    let mut tracker = Tracker::new(1, &DurableCutConfig::default());
    let start = Instant::now();
    tracker.report(0, 0, 0);
    for floor in 1..=(MAX_SAMPLES as u64 * 2) {
        let at = start + Duration::from_micros(floor);
        assert!(evaluate(&mut tracker, at, floor, &[floor]).1.is_empty());
    }
    assert_eq!(tracker.samples.len(), MAX_SAMPLES);
    let late = start + Duration::from_millis(1001);
    assert_eq!(
        evaluate(
            &mut tracker,
            late,
            MAX_SAMPLES as u64 * 2,
            &[MAX_SAMPLES as u64 * 2]
        )
        .1,
        [0]
    );
}

/// The inbox is pre-filled past several drain passes (owners + 8 messages
/// each). A loop that received one message beyond the bound would drop the
/// message at position `drain + 1` of the first pass and `2 * (drain + 1)` of
/// the second; this layout puts a report (the only report of its owner) and a
/// certification exactly there. Every report pins its own owner at the same
/// floor, and certifications ask only for 1, so a lost report leaves the cut
/// at 1 and a lost certification fails its reply.
#[test]
fn saturated_inbox_handles_every_report_and_certification() {
    const OWNERS: usize = 4;
    const FLOOR: u64 = 50;
    let directory = tempdir::TempDir::new("durable-cut-inbox").unwrap();
    let shared = Shared {
        epoch: 1,
        published: AtomicU64::new(FLOOR),
        owners: (0..OWNERS).map(|_| OwnerShared::default()).collect(),
        counters: Counters::default(),
        persisted_events: Mutex::new(None),
    };
    for owner in shared.owners.iter() {
        owner.receipted.store(FLOOR, Ordering::Relaxed);
    }
    let drain = OWNERS + 8;
    let (messages, inbox) = mpsc::sync_channel(64);
    let mut replies = Vec::new();
    let mut certify = |messages: &mpsc::SyncSender<Message>| {
        let (reply, result) = mpsc::sync_channel(1);
        messages.send(Message::Certify { next: 1, reply }).unwrap();
        replies.push(result);
    };
    // Positions 1..=drain: certifications; drain + 1: owner 0's report.
    for _ in 0..drain {
        certify(&messages);
    }
    for owner in 0..OWNERS {
        messages
            .send(Message::Report {
                owner,
                synced_next: FLOOR,
                appended_end: 100,
                // The last owner reports an incarnation other than its epoch mark.
                incarnation: if owner + 1 == OWNERS {
                    OTHER_INCARNATION
                } else {
                    INCARNATION
                },
            })
            .unwrap();
    }
    // Certifications through position 2 * (drain + 1), then Stop.
    for _ in drain + 1 + OWNERS..=2 * (drain + 1) {
        certify(&messages);
    }
    messages.send(Message::Stop).unwrap();
    // A lost Stop then ends the actor by disconnect instead of a hang.
    drop(messages);
    let mut stored = record(Some(EpochCut { epoch: 1, next: 0 }), Vec::new());
    stored.owners = (0..OWNERS as u32)
        .map(|partition| mark(partition, 0))
        .collect();
    run_actor(
        &inbox,
        &shared,
        Tracker::new(OWNERS, &DurableCutConfig::default()),
        stored,
        directory.path(),
        Duration::from_millis(1),
    );
    let certified = replies.len() as u64;
    for result in replies {
        result.try_recv().unwrap().unwrap();
    }
    assert_eq!(
        shared.counters.certifications.load(Ordering::Relaxed),
        certified
    );
    assert_eq!(
        shared.counters.persisted_next.load(Ordering::Relaxed),
        FLOOR
    );
    let persisted = CutRecord::load(directory.path(), DB, 2).unwrap().unwrap();
    assert_eq!(
        persisted.live,
        Some(EpochCut {
            epoch: 1,
            next: FLOOR
        })
    );
    // Marks travel with the cut: synced ends advance, a changed incarnation is zeroed.
    let marks: Vec<_> = persisted
        .owners
        .iter()
        .map(|mark| (mark.synced_next, mark.incarnation))
        .collect();
    assert_eq!(
        marks,
        [
            (FLOOR, INCARNATION),
            (FLOOR, INCARNATION),
            (FLOOR, INCARNATION),
            (FLOOR, [0; 16])
        ]
    );
}
