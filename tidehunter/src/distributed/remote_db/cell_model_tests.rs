//! Seeded schedules of the option C event model (spec §11.3). Every failure
//! prints its seed, configuration, crash cut, crash outcome and first
//! divergent event; `cell_model_replay_one` replays it from the environment.
use super::*;

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a number"))
    })
}

/// Every crash cut that applies to a history mode, then a no-cut control.
fn cuts(on_disk_history: bool) -> Vec<Option<Transition>> {
    Transition::RUN
        .iter()
        .chain(Transition::RESTART)
        .copied()
        .filter(|transition| transition.applies(on_disk_history))
        .map(Some)
        .chain([None])
        .collect()
}

fn run(config: ModelConfig) -> Report {
    run_schedule(config).unwrap_or_else(|failure| panic!("{failure}"))
}

#[test]
fn model_filesystem_keeps_old_or_new_entry_for_a_rename_before_dirsync() {
    let mut fs = Fs::default();
    let manifest = |epoch| {
        Content::Manifest(Arc::new(Manifest {
            epoch,
            commit: None,
        }))
    };
    let old = fs.link(MANIFEST, manifest(1), OBJECT_BYTES);
    fs.inodes[old].len = OBJECT_BYTES;
    fs.fsync(old);
    fs.dirsync();
    let temp = fs.create(&staging(MANIFEST), manifest(2), OBJECT_BYTES);
    fs.inodes[temp].len = OBJECT_BYTES;
    let epoch = |fs: &Fs| match fs.object(MANIFEST) {
        Some(Content::Manifest(manifest)) => Some(manifest.epoch),
        _ => None,
    };
    let power = |applied| FsOutcome::PowerLoss {
        applied,
        unlinks: false,
    };
    // Unsynced temp renamed: a power loss can expose an empty new entry.
    fs.rename(&staging(MANIFEST), MANIFEST).unwrap();
    assert_eq!(epoch(&fs.crash(FsOutcome::Process)), Some(2));
    assert_eq!(epoch(&fs.crash(power(0))), Some(1));
    assert_eq!(epoch(&fs.crash(power(1))), Some(1));
    assert_eq!(epoch(&fs.crash(power(2))), None);
    // Synced before the rename: the old or the new complete manifest.
    fs.fsync(temp);
    assert_eq!(epoch(&fs.crash(power(0))), Some(1));
    assert_eq!(epoch(&fs.crash(power(2))), Some(2));
    fs.dirsync();
    assert_eq!(epoch(&fs.crash(power(0))), Some(2));
    // A later unlink can persist before an earlier rename.
    let doomed = fs.link("doomed", Content::HistoryLog(Vec::new()), 0);
    fs.fsync(doomed);
    fs.dirsync();
    let next = fs.create(&staging(MANIFEST), manifest(3), OBJECT_BYTES);
    fs.inodes[next].len = OBJECT_BYTES;
    fs.fsync(next);
    fs.rename(&staging(MANIFEST), MANIFEST).unwrap();
    fs.unlink("doomed");
    let reordered = fs.crash(FsOutcome::PowerLoss {
        applied: 0,
        unlinks: true,
    });
    assert_eq!(epoch(&reordered), Some(2));
    assert!(reordered.inode("doomed").is_none());
    assert!(fs.crash(power(0)).inode("doomed").is_some());
    fs.dirsync();
    // An unsynced log suffix is truncated, not hidden.
    let log = fs.link(HISTORY_LOG, Content::HistoryLog(Vec::new()), 0);
    if let Content::HistoryLog(records) = &mut fs.inodes[log].content {
        records.extend(
            [(
                1,
                BatchVersion {
                    epoch: 1,
                    sequence: 1,
                },
            ); 3],
        );
    }
    fs.inodes[log].len = 3 * HISTORY_RECORD;
    fs.inodes[log].synced = HISTORY_RECORD;
    fs.dirsync();
    let crashed = fs.crash(power(0));
    match &crashed.inode(HISTORY_LOG).unwrap().content {
        Content::HistoryLog(records) => assert_eq!(records.len(), 1),
        other => panic!("{other:?}"),
    }
}

#[test]
fn compaction_keeps_the_anchor_and_every_newer_version() {
    let version = |sequence, operation_index| MutationVersion {
        batch: BatchVersion { epoch: 1, sequence },
        operation_index,
    };
    let reference = |key, sequence, value| Ref {
        key,
        version: version(sequence, 0),
        value,
    };
    let refs = [
        reference(1, 1, Some(1)),
        reference(1, 2, None),
        reference(1, 4, Some(4)),
        reference(2, 1, Some(9)),
    ];
    assert_eq!(compact(refs, None), refs.to_vec());
    assert_eq!(
        compact(refs, Some(version(3, u32::MAX))),
        vec![refs[1], refs[2], refs[3]],
        "the tombstone at 2 is the anchor of key 1"
    );
}

/// The default gate: every crash cut per tier and history mode, owner counts
/// 1/2/5/16 and both key distributions rotating, the real orphan scan
/// cross-checked on every fourth schedule. A cut that a schedule never
/// reaches (for example no offline flush happened) takes the next seed of a
/// fixed list of eight; every schedule run must pass, and each cut must be
/// reached within its list.
#[test]
fn cell_model_bounded_crash_matrix() {
    let started = std::time::Instant::now();
    let mut total = Report::default();
    let mut schedules = 0;
    for native in [false, true] {
        for on_disk_history in [false, true] {
            for (index, cut) in cuts(on_disk_history).into_iter().enumerate() {
                let mut reached = false;
                for attempt in 0..8_u64 {
                    let seed = 0xce11_0000
                        + (index as u64) * 7
                        + u64::from(native) * 1009
                        + u64::from(on_disk_history) * 2003
                        + attempt * 65_537;
                    let mut config = ModelConfig::new(
                        seed,
                        [1, 2, 5, 16][(index + attempt as usize) % 4],
                        (index + attempt as usize) % 2 == 1,
                        native,
                        on_disk_history,
                    );
                    config.cut = cut.map(|at| CrashCut {
                        at,
                        occurrence: 1,
                        again: at == Transition::OrphanScan,
                    });
                    config.real_store = (index + attempt as usize).is_multiple_of(4);
                    let report = run(config);
                    schedules += 1;
                    total.merge(&report);
                    if cut.is_none() || report.cut_reached {
                        reached = true;
                        break;
                    }
                }
                assert!(
                    reached,
                    "cut {cut:?} not reached in eight seeds (native={native} on_disk_history={on_disk_history})"
                );
            }
        }
    }
    eprintln!(
        "cell model bounded matrix: {schedules} schedules in {:?}: {total:#?}",
        started.elapsed()
    );
    for (counter, value) in [
        ("rename_old", total.rename_old),
        ("rename_new", total.rename_new),
        ("split_batches", total.split_batches),
        ("multi_epoch_split", total.multi_epoch_split),
        ("fully_covered", total.fully_covered),
        ("coverage_behind_frame", total.coverage_behind_frame),
        ("offline_flushes", total.offline_flushes),
        ("orphans_deleted", total.orphans_deleted),
        ("restart_crashes", total.restart_crashes),
        ("real_store_checks", total.real_store_checks),
        ("owner_power_loss", total.owner_power_loss),
        ("retries", total.retries),
        ("aborted", total.aborted),
    ] {
        assert!(value > 0, "{counter} never exercised: {total:#?}");
    }
}

/// Spec §11.3 matrix: owners 1..=16, uniform and Zipf 0.99, both tiers and
/// both history modes, 1000 seeded schedules each, rotating every cut plus a
/// no-cut control. TH_MODEL_{OWNERS,HOT,NATIVE,ON_DISK_HISTORY} filter
/// (OWNERS=0 or the others=2 mean all); TH_MODEL_SEED, TH_MODEL_START and
/// TH_MODEL_ITERATIONS select a subrange.
#[test]
#[ignore = "128 configurations x 1000 seeded schedules; run with --release"]
fn cell_model_thousand_schedules() {
    let owner_filter = env_number("TH_MODEL_OWNERS", 0);
    let hot_filter = env_number("TH_MODEL_HOT", 2);
    let native_filter = env_number("TH_MODEL_NATIVE", 2);
    let history_filter = env_number("TH_MODEL_ON_DISK_HISTORY", 2);
    let seed = env_number("TH_MODEL_SEED", 0xce11);
    let start = env_number("TH_MODEL_START", 0);
    let count = env_number("TH_MODEL_ITERATIONS", 1000);
    assert!(owner_filter <= 16 && hot_filter <= 2 && native_filter <= 2 && history_filter <= 2);
    assert!(start < 1000 && (1..=1000 - start).contains(&count));
    let started = std::time::Instant::now();
    let mut configurations = 0;
    let mut total = Report::default();
    let mut unreached = 0;
    for owners in 1..=16 {
        if owner_filter != 0 && owner_filter != owners {
            continue;
        }
        for zipf in [false, true] {
            if hot_filter != 2 && hot_filter != u64::from(zipf) {
                continue;
            }
            for native in [false, true] {
                if native_filter != 2 && native_filter != u64::from(native) {
                    continue;
                }
                for on_disk_history in [false, true] {
                    if history_filter != 2 && history_filter != u64::from(on_disk_history) {
                        continue;
                    }
                    configurations += 1;
                    let cuts = cuts(on_disk_history);
                    for schedule in start..start + count {
                        let cut = cuts[schedule as usize % cuts.len()];
                        let mut config = ModelConfig::new(
                            seed.wrapping_add(schedule),
                            owners as usize,
                            zipf,
                            native,
                            on_disk_history,
                        );
                        config.cut = cut.map(|at| CrashCut {
                            at,
                            occurrence: 1 + (schedule as usize / cuts.len()) % 3,
                            again: schedule.is_multiple_of(5),
                        });
                        let report = run(config);
                        unreached += usize::from(cut.is_some() && !report.cut_reached);
                        total.merge(&report);
                    }
                }
            }
        }
    }
    eprintln!(
        "cell model: {configurations} configurations x {count} schedules in {:?}; cuts not reached (occurrence beyond the schedule): {unreached}; {total:#?}",
        started.elapsed()
    );
    assert!(configurations > 0);
}

/// Replay one schedule: TH_MODEL_SEED, TH_MODEL_OWNERS, TH_MODEL_HOT,
/// TH_MODEL_NATIVE, TH_MODEL_ON_DISK_HISTORY, optional TH_MODEL_CUT (a
/// transition name) and TH_MODEL_OCCURRENCE, as printed by a failure.
#[test]
#[ignore = "replays the schedule given in the environment"]
fn cell_model_replay_one() {
    let mut config = ModelConfig::new(
        env_number("TH_MODEL_SEED", 0xce11),
        env_number("TH_MODEL_OWNERS", 2) as usize,
        env_number("TH_MODEL_HOT", 0) != 0,
        env_number("TH_MODEL_NATIVE", 0) != 0,
        env_number("TH_MODEL_ON_DISK_HISTORY", 0) != 0,
    );
    config.cut = std::env::var("TH_MODEL_CUT").ok().map(|name| CrashCut {
        at: Transition::parse(&name).unwrap_or_else(|| panic!("unknown cut {name}")),
        occurrence: env_number("TH_MODEL_OCCURRENCE", 1) as usize,
        again: env_number("TH_MODEL_AGAIN", 0) != 0,
    });
    config.real_store = true;
    eprintln!("{:#?}", run(config));
}

/// Each seeded protocol defect is rejected within a bounded seed range, with
/// a message carrying the seed and first divergent event.
#[test]
fn cell_model_rejects_seeded_protocol_defects() {
    const LOST: &[&str] = &[
        "per-key history differs (partial batch or lost member)",
        "lost acknowledged write",
        "recovered batch without its identity",
        "base reference above F without its suffix batch",
    ];
    for (bug, kinds) in [
        (
            Bug::SkipSegmentSync,
            &["adopted frame missing", "root segment missing or short"][..],
        ),
        // The real `CellRoot::check` rejects F above the minimum coverage.
        (
            Bug::MaxCoverageFloor,
            &["CellRoot::check rejected a built root"][..],
        ),
        (Bug::SkipCoveredBatch, LOST),
        (
            Bug::IdentityBeforeAuthority,
            &["identity-free retry gap"][..],
        ),
        (
            Bug::GcBeforeManifestSync,
            // The old manifest survives while its root, wrapper or segments
            // were already unlinked.
            &[
                "root segment missing or short",
                "adopted frame missing",
                "adopted root missing or torn",
                "adopted wrapper missing or torn",
            ][..],
        ),
        (Bug::CreditBeforeAdoption, &["replay tail above bound"][..]),
        (Bug::NoOrphanCleanup, &["orphan survived reopen"][..]),
    ] {
        let mut found = None;
        for schedule in 0..64_u64 {
            let on_disk_history = schedule % 2 == 1;
            let cuts = cuts(on_disk_history);
            let mut config = ModelConfig::new(
                0xbad0 + schedule,
                1 + (schedule as usize % 4),
                schedule.is_multiple_of(3),
                schedule % 4 >= 2,
                on_disk_history,
            );
            config.cut = cuts[(schedule as usize * 5) % cuts.len()].map(|at| CrashCut {
                at,
                occurrence: 1,
                again: false,
            });
            config.bug = Some(bug);
            if let Err(failure) = run_schedule(config) {
                assert!(
                    kinds.contains(&failure.divergence.kind),
                    "{bug:?} produced an unexpected divergence: {failure}"
                );
                found = Some(failure);
                break;
            }
        }
        let failure = found.unwrap_or_else(|| panic!("{bug:?} was not detected in 64 seeds"));
        let message = failure.to_string();
        assert!(message.contains("seed=") && message.contains("first_divergent_event="));
        eprintln!("{bug:?} rejected: {message}");
    }
}

/// Seed 52769 of the 1000-schedule sweep: a cell created after a root's
/// directory boundary keeps the previous F, so a later undrained root
/// computes F below the adopted F. Like C5a's commit (spec §7.3 step 5),
/// the capture is rejected before any output; the schedule stays correct.
#[test]
fn cell_model_regressed_root_capture_is_abandoned() {
    let mut config = ModelConfig::new(52769, 3, true, false, true);
    config.cut = Some(CrashCut {
        at: Transition::RootTemp,
        occurrence: 1,
        again: false,
    });
    let report = run(config);
    assert!(report.abandoned_roots > 0, "{report:#?}");
}
