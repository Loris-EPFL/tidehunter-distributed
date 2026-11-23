//! Whole-file retirement on live and sealed streams: retirement under a
//! serving stream, the crash-cut matrix, prepared grants and reopen.

use super::*;

/// The bound for the prepared-grant wait. It only ends a failed wait.
const GRANT_WAIT: Duration = Duration::from_secs(5);
/// The bound for a put that must complete while a seal sync is parked.
const PUT_WAIT: Duration = Duration::from_secs(30);

/// Fragment size equals file size, so every extent fills exactly one file and
/// `scan_from` (the fragment start of the root's proven end) always sits at a
/// file start. Four files are filled and sealed (each ends below the
/// allocator), then the next extent is leased and the maintenance threads are
/// quiesced: every file can be detached.
fn whole_file_stream() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let temp = tempdir::TempDir::new("r1-whole-files").unwrap();
    let mut config = live_config();
    config.fragment_bytes = FILE;
    config.wal_file_bytes = FILE;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    fill(&log, &config, 0, 4);
    log.persist_wal_through(log.appended_frontier()).unwrap();
    assert_eq!(log.seal_ready_files().unwrap(), 4);
    let start = lease_and_resolve(&log, &config);
    assert!(start >= 4 * FILE, "the lease starts in file 4");
    log.quiesce_wal_maps();
    (temp, config, log)
}

/// The recovered end of the whole-file stream killed after `retire` was applied
/// to its seals (`None`: the no-GC control). Three reopens; prints each.
fn whole_file_ends(retire: Option<u64>) -> (Vec<u64>, u64, u64, PartitionConfig, tempdir::TempDir) {
    let (temp, config, log) = whole_file_stream();
    if let Some(file) = retire {
        let victims: Vec<_> = proofs(&log)
            .into_iter()
            .filter(|proof| proof.file_start == file * FILE)
            .collect();
        assert_eq!(victims.len(), 1, "one seal per file");
        assert_eq!(
            log.retire_proved_files(&victims, Some(&witness())).unwrap(),
            RetirementOutcome::Completed
        );
        assert!(
            log.file_retirement_state().1.is_empty(),
            "the unlink completed"
        );
    }
    drop(log);
    let path = temp.path().join("stream");
    let mut ends = Vec::new();
    let mut first_scan_from = 0;
    let mut first_synced_end = 0;
    for open in 1..=3 {
        let recovered = recover(&path, &config);
        let (_, scan_from, synced_end) = recovered.position_root_for_test();
        if open == 1 {
            first_scan_from = scan_from;
            first_synced_end = synced_end;
        }
        eprintln!(
            "retired {retire:?}, open {open}: end {} scan_from {scan_from} synced_end {synced_end}",
            recovered.appended_frontier()
        );
        ends.push(recovered.appended_frontier());
        drop(recovered);
    }
    (ends, first_scan_from, first_synced_end, config, temp)
}

/// A killed stream whose TAIL file was retired behind a live file: `scan_from`
/// sits exactly at the start of the retired tail file, and the file before it
/// is NOT retired. The floor condition tests the file of the floored end
/// (`strict_end - 1`), which is retired; the file of the scanned end
/// (`physical_end - 1`) is the live one above it. The end is floored, recorded
/// in the SEAL, and identical on the second and third open.
///
/// The recovery-hole probe (`recovery_found_hole`, mutants m09a/m09b) is NOT
/// reachable from this fixture: a retired interval is skipped whole
/// (`WalRoot::retired`), so the scan never reports the `gap` the probe needs.
/// The hole stays asserted by `storage/crash_tests.rs:347` and
/// `storage/tests/live_extent/leased_declared_invalid_frames_cases.rs:299`;
/// part A reports m09a and m09b NOT-CAUGHT.
#[test]
fn a_tail_file_retired_behind_a_live_hole_recovers_the_floored_end_on_every_open() {
    let (control, _, _, _, _keep) = whole_file_ends(None);
    let before = control[0];
    let (ends, scan_from, _, config, temp) = whole_file_ends(Some(3));
    assert_eq!(
        scan_from,
        3 * FILE,
        "premise: scan_from is at the retired file's start"
    );
    for (index, end) in ends.iter().enumerate() {
        assert!(
            *end >= before,
            "open {}: end {end} below the control {before}",
            index + 1
        );
        assert_eq!(
            *end,
            ends[0],
            "open {}: the end moved between opens",
            index + 1
        );
    }
    let path = temp.path().join("stream");
    let recovered = recover(&path, &config);
    assert!(
        recovered.is_retired(3 * FILE).unwrap(),
        "premise: the tail file is retired"
    );
    assert!(
        !recovered.is_retired(2 * FILE).unwrap(),
        "premise: the live hole is not"
    );
    // The coordinator's `require_end` is its replay front, the control end.
    recovered
        .replay_page(0, 3, usize::MAX, before)
        .expect("the recovered stream satisfies the required end");
}

/// The floored end's file is NOT retired (the head file is): the floor must not
/// be taken. The scanned end stays exact and the SEAL written by the first
/// recovery matches the surviving frames at the second and third open (no
/// "sealed inventory no longer matches surviving frames"), and the end equals
/// the no-GC control.
///
/// Negative control (mutation run): floor whenever no SEAL exists, without the
/// retired-file condition: the SEAL records the floored end and the second open
/// refuses it, or the end differs from the control.
#[test]
fn a_floored_end_in_a_live_file_is_never_taken_and_every_open_matches_the_control() {
    let (control, _, _, _, _keep) = whole_file_ends(None);
    let (ends, _, synced_end, _, _temp) = whole_file_ends(Some(0));
    // The premise that makes the floor condition matter: the root certifies
    // more than the scan finds (`strict_end > physical_end`: the last frame is
    // followed by a skip marker, not by the end of its file). Without it the
    // unconditional-floor mutation would change nothing.
    assert!(
        synced_end > ends[0],
        "premise: strict_end {synced_end} is not above the scanned end {}",
        ends[0]
    );
    for (index, end) in ends.iter().enumerate() {
        assert_eq!(
            *end,
            control[0],
            "open {}: end differs from the control",
            index + 1
        );
    }
}

#[test]
fn retired_ranges_stay_constant_under_turnover_and_keep_a_live_hole() {
    let mut retired = RetiredFiles::default();
    let size = |retired: &RetiredFiles| bincode::serialize(retired).unwrap().len();
    let empty = size(&retired);
    for file in 0..1000 {
        retired
            .insert_authorized_batch(&[file * FILE], FILE)
            .unwrap();
        assert_eq!(retired.interval_count(), 1);
        assert_eq!(size(&retired), empty);
    }
    // Retire 1000, 1001 and 1003: file 1002 stays a live hole.
    retired
        .insert_authorized_batch(&[1000 * FILE, 1001 * FILE, 1003 * FILE], FILE)
        .unwrap();
    assert!(!retired.contains(1002 * FILE));
    assert_eq!(retired.skip_end(500 * FILE), Some(1002 * FILE));
    assert_eq!(retired.skip_end(1003 * FILE), Some(1004 * FILE));
    assert_eq!(retired.skip_end(1002 * FILE), None);
    retired.validate(FILE).unwrap();
    let round_trip: RetiredFiles =
        bincode::deserialize(&bincode::serialize(&retired).unwrap()).unwrap();
    assert_eq!(round_trip, retired);
    // Filling the hole coalesces the range into the prefix.
    retired
        .insert_authorized_batch(&[1002 * FILE], FILE)
        .unwrap();
    assert_eq!(retired.interval_count(), 1);
    assert_eq!(retired.end(), 1004 * FILE);
    // Corrupt or repeated input is refused.
    assert!(retired.insert_authorized_batch(&[5 * FILE], FILE).is_err());
    assert!(
        retired
            .insert_authorized_batch(&[2000 * FILE, 1999 * FILE], FILE)
            .is_err()
    );
    assert!(
        retired
            .insert_authorized_batch(&[2000 * FILE + 1], FILE)
            .is_err()
    );
}

#[test]
fn noncanonical_retired_ranges_are_rejected() {
    let mut adjacent = RetiredFiles::default();
    adjacent
        .insert_authorized_batch(&[2 * FILE, 4 * FILE], FILE)
        .unwrap();
    let bytes = bincode::serialize(&adjacent).unwrap();
    // prefix_end = 0, ranges [2,3) and [4,5): make the second start at 3.
    let mut touching = bytes.clone();
    let second_start = touching.len() - 16;
    touching[second_start..second_start + 8].copy_from_slice(&(3 * FILE).to_le_bytes());
    let decoded: RetiredFiles = bincode::deserialize(&touching).unwrap();
    assert!(decoded.validate(FILE).is_err());
    let mut misaligned = bytes;
    let second_start = misaligned.len() - 16;
    misaligned[second_start..second_start + 8].copy_from_slice(&(4 * FILE + 1).to_le_bytes());
    let decoded: RetiredFiles = bincode::deserialize(&misaligned).unwrap();
    assert!(decoded.validate(FILE).is_err());
}

#[test]
fn seal_is_lease_logical_and_denies_any_stale_grant() {
    let (_temp, config, log) = sealed_stream();
    let (seals, pending) = log.file_retirement_state();
    assert!(pending.is_empty());
    assert_eq!(
        seals.iter().map(|seal| seal.start).collect::<Vec<_>>(),
        [0, FILE, 2 * FILE]
    );
    // Two groups per file, sequences 0..6 in order.
    assert_eq!(seals[0].last_batch_sequence, Some(1));
    assert_eq!(seals[1].last_batch_sequence, Some(3));
    assert_eq!(seals[2].last_batch_sequence, Some(5));
    // No further file is past the allocator.
    assert_eq!(log.seal_ready_files().unwrap(), 0);
    // A sealed file stays readable through its unchanged inode.
    let receipt = log.receipt_page(0, 1).unwrap()[0];
    assert_eq!(
        log.read_batch(receipt).unwrap().id(),
        batch(&config, 0).id()
    );
    // A grant can never move back into a sealed file.
    let mut root = WalRoot::clone(log.state.lock().wal_root.as_ref().unwrap());
    root.sealed_files.push(FileSeal {
        start: 3 * FILE,
        end: 4 * FILE,
        device: 0,
        inode: 0,
        root_generation: root.generation,
        last_batch_sequence: None,
    });
    log.state.lock().wal_root = Some(Arc::new(root));
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Invalid(
            "live grant inside a sealed or retired WAL file"
        ))
    ));
}

#[test]
fn seal_root_crash_cuts_leave_every_file_and_recover() {
    for phase in 1..=4 {
        let (temp, config, log) = fixture();
        fill(&log, &config, 0, 6);
        log.persist_wal_through(log.appended_frontier()).unwrap();
        if phase <= 3 {
            log.fail_wal_root.store(phase, Ordering::Release);
        } else {
            log.arm_crash_cut(CrashCut::SealAfterRoot);
        }
        assert!(log.seal_ready_files().is_err());
        drop(log);
        let path = temp.path().join("stream");
        let recovered = recover(&path, &config);
        for file in 0..3 {
            assert!(wal_exists(&path, file), "phase {phase} lost file {file}");
        }
        assert!(recovered.file_retirement_state().1.is_empty());
    }
}

/// The live frame of `batch(config, sequence)`, position independent.
fn frame_of(config: &PartitionConfig, sequence: u64) -> Vec<u8> {
    let group = CompleteGroup::new_live(
        &shape(),
        vec![batch(config, sequence)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    LiveGroupDescriptor::from_group(&group).unwrap().1.to_vec()
}

/// Design §3 step 3: sequences strictly ascend for the stream. A valid frame
/// replaying an earlier sequence below the durable position is corruption,
/// both inside one file and across the boundary with the previous seal.
#[test]
fn seal_requires_strictly_ascending_batch_sequences() {
    // (stream offset of the replaced group, replayed sequence, seals kept)
    for (offset, replayed, kept) in [(16 * 1024, 0, 0), (FILE, 1, 1)] {
        let (temp, config, log) = fixture();
        fill(&log, &config, 0, 6);
        log.persist_wal_through(log.appended_frontier()).unwrap();
        let path = temp.path().join("stream");
        let frame = frame_of(&config, replayed);
        assert_eq!(frame.len(), 4096);
        write_at(
            &path.join(format!("wal_{:016x}", offset / FILE)),
            offset % FILE,
            &frame,
        );
        assert!(matches!(
            log.seal_ready_files(),
            Err(StorageError::Invalid(message)) if message.contains("ascend")
        ));
        let (seals, pending) = log.file_retirement_state();
        assert_eq!(seals.len(), kept, "offset {offset}");
        assert!(pending.is_empty());
        assert!(seals.iter().all(|seal| seal.last_batch_sequence == Some(1)));
        assert!(log.state.lock().poison.is_some(), "offset {offset}");
    }
}

#[test]
fn retirement_crash_cuts_match_the_no_gc_control() {
    let control = {
        let (temp, config, log) = sealed_stream();
        drop(log);
        let recovered = recover(&temp.path().join("stream"), &config);
        (
            recovered.receipt_page(0, 256).unwrap(),
            recovered.appended_frontier(),
        )
    };
    let cuts: [Option<CrashCut>; 6] = [
        None,
        Some(CrashCut::RetireAfterRoot),
        Some(CrashCut::BeforeDetach),
        Some(CrashCut::BeforeUnlink),
        Some(CrashCut::AfterFirstUnlink),
        Some(CrashCut::AfterDirectorySync),
    ];
    for phase in 0..=3u8 {
        for cut in cuts {
            if (phase == 0) == cut.is_none() {
                continue;
            }
            let (temp, config, log) = sealed_stream();
            let victims = proofs(&log);
            assert_eq!(victims.len(), 3);
            if phase != 0 {
                log.fail_wal_root.store(phase, Ordering::Release);
            }
            if let Some(cut) = cut {
                log.arm_crash_cut(cut);
            }
            assert!(log.retire_proved_files(&victims, Some(&witness())).is_err());
            drop(log);
            let path = temp.path().join("stream");
            let recovered = recover(&path, &config);
            let retired = recovered.is_retired(0).unwrap();
            // Before a durable root nothing is gone; after it recovery has
            // repeated every unlink from the exact pending identities.
            for file in 0..3 {
                assert_eq!(wal_exists(&path, file), !retired, "{phase} {cut:?}");
                assert_eq!(recovered.is_retired(file * FILE).unwrap(), retired);
            }
            assert_eq!(
                (
                    recovered.receipt_page(0, 256).unwrap(),
                    recovered.appended_frontier()
                ),
                control,
                "{phase} {cut:?}"
            );
            // The next retirement (idempotent repeat) clears the pending set.
            recovered
                .retire_wal_files(if retired { &[0, FILE, 2 * FILE] } else { &[] })
                .unwrap();
            assert!(recovered.file_retirement_state().1.is_empty());
        }
    }
}

#[test]
fn retirement_completes_while_the_stream_serves_and_the_root_stays_bounded() {
    let (_temp, config, log) = fixture();
    let mut next = 0;
    let mut sizes = Vec::new();
    for _ in 0..12 {
        let allocator_end = fill(&log, &config, next, 4);
        next += 4;
        log.persist_wal_through(log.appended_frontier()).unwrap();
        log.seal_ready_files().unwrap();
        // The file that ends exactly at the allocator keeps a writable last
        // fragment map until the next fragment is allocated, so the mapper
        // refuses to detach it (`WouldBlock`) and refuses its whole batch:
        // it is left out here (the test below pins that refusal). Finalization
        // of every other file is asynchronous, so wait for the maintenance
        // threads first: the pass then sees the final map state.
        log.quiesce_wal_maps();
        let mut victims = proofs(&log);
        victims.retain(|proof| proof.file_start + FILE != allocator_end);
        if !victims.is_empty() {
            assert_eq!(
                log.retire_proved_files(&victims, Some(&witness())).unwrap(),
                RetirementOutcome::Completed
            );
        }
        let (seals, pending) = log.file_retirement_state();
        assert!(
            pending.is_empty(),
            "every retired victim must be finished: {pending:?}"
        );
        assert!(seals.len() <= 2, "seals of unretired files only: {seals:?}");
        sizes.push(log.wal_root_encoded_len());
    }
    // Contiguous turnover: one retired prefix, a bounded seal set.
    assert!(sizes[4..].iter().all(|size| *size <= sizes[3]), "{sizes:?}");
    // The serving stream keeps granting after many retirements.
    fill(&log, &config, next, 1);
}

/// The deferral IS the mechanism: a batch that holds the file ending exactly at
/// the allocator is refused whole by the mapper (the writer's last fragment map
/// stays writable until the next fragment is allocated). The retirement is
/// durable (the pass returns `Completed`), nothing is unlinked, every file stays
/// in the cleanup set, and the next allocation plus a repeat unlinks all of
/// them. No sleeps: `quiesce_wal_maps` waits for the maintenance threads.
#[test]
fn retiring_the_file_that_ends_at_the_allocator_is_deferred_until_the_next_lease() {
    let (temp, config, log) = sealed_stream_at_the_allocator();
    let path = temp.path().join("stream");
    let victims = proofs(&log);
    assert_eq!(victims.len(), 3);
    log.quiesce_wal_maps();
    assert_eq!(
        log.retire_proved_files(&victims, Some(&witness())).unwrap(),
        RetirementOutcome::Completed
    );
    let (_, pending) = log.file_retirement_state();
    assert_eq!(pending.len(), 3, "the whole batch is deferred: {pending:?}");
    for file in 0..3 {
        assert!(wal_exists(&path, file), "file {file} was unlinked early");
    }
    // The next lease allocates a new fragment: the last map is finalized.
    lease_and_resolve(&log, &config);
    log.quiesce_wal_maps();
    // The repeat of the same proofs (the serving stream takes the watermark
    // witness) unlinks what the first pass deferred.
    assert_eq!(
        log.retire_proved_files(&victims, Some(&witness())).unwrap(),
        RetirementOutcome::Completed
    );
    assert!(
        log.file_retirement_state().1.is_empty(),
        "the repeat clears the cleanup set"
    );
    for file in 0..3 {
        assert!(!wal_exists(&path, file), "file {file} was not unlinked");
    }
}

/// A sealed-stream reservation checks every victim before closing any of
/// them: a record lease on the LAST victim leaves the earlier victims open.
/// Dropping an uncommitted reservation reopens all newly closed files.
///
/// Negative controls (mutation runs): ignore pins in the reservation check
/// (`pinned || closed` becomes `closed`): the first reservation succeeds;
/// remove the rollback in `SealedRetirement::drop`: a victim stays closed.
#[test]
fn a_sealed_reservation_is_atomic_and_rolls_back_before_the_root() {
    use std::sync::atomic::Ordering;

    let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
    let receipt = log.receipt_page(5, 1).unwrap()[0];
    let reference = batch(&config, 5)
        .record_reference(receipt.address(), 0)
        .unwrap();
    drop(log);
    let log = Arc::new(recover(&temp.path().join("stream"), &config));
    let original =
        std::fs::read(log.wal_file_path(0)).expect("read victim bytes before reservation");
    assert!(!original.is_empty(), "premise: the victim contains data");
    let victims: Vec<_> = (0..3)
        .map(|file| RetireProof {
            file_start: file * FILE,
            inode: None,
            seal_generation: None,
        })
        .collect();
    let counters = log
        .wal
        .file_read_pin_counters(victims.iter().map(|proof| proof.file_start));
    assert_eq!(counters.len(), victims.len());
    assert!(counters.iter().all(|(_, counter)| counter.is_some()));
    let lease = log
        .resolve_live_record(&reference)
        .expect("premise: record can be leased");
    assert_eq!(
        lease.segment.file_index, 2,
        "premise: the lease pins the last victim"
    );
    assert!(log.reserve_sealed_retirement(&victims).unwrap().is_none());
    let (events, dropped) = log.take_busy_events();
    assert_eq!(dropped, 0, "no Busy cause was lost");
    assert_eq!(
        events,
        vec![crate::distributed::storage::BusyEvent {
            epoch: config.inventory.epoch,
            file: 2 * FILE,
            cause: crate::distributed::storage::BusyCause::RecordPin,
        }],
        "the refusal came from the record lease on the last victim and nothing else"
    );
    for (_, counter) in &counters {
        assert_eq!(
            counter.as_ref().unwrap().load(Ordering::Acquire) & crate::wal::Wal::READ_PIN_CLOSED,
            0,
            "a pinned victim must leave every victim open"
        );
    }
    drop(lease);
    let reservation = log
        .reserve_sealed_retirement(&victims)
        .unwrap()
        .expect("unpinned victims can be reserved");
    for (_, counter) in &counters {
        assert_ne!(
            counter.as_ref().unwrap().load(Ordering::Acquire) & crate::wal::Wal::READ_PIN_CLOSED,
            0,
            "every victim is closed before root persistence"
        );
    }
    assert!(
        log.resolve_live_record(&reference).is_err(),
        "new pins are refused after close"
    );
    drop(reservation);
    for (_, counter) in &counters {
        assert_eq!(
            counter.as_ref().unwrap().load(Ordering::Acquire) & crate::wal::Wal::READ_PIN_CLOSED,
            0,
            "dropping an uncommitted reservation reopens the victim"
        );
    }
    assert_eq!(
        std::fs::read(log.wal_file_path(0)).expect("read victim bytes after rollback"),
        original,
        "rollback preserves the victim bytes"
    );
    assert!(
        log.resolve_live_record(&reference).is_ok(),
        "the record is readable after rollback"
    );
}

/// A failed root install is ambiguous at every phase the fault hook offers
/// (1: before the temporary file is written, 2: after it is synced, 3: after
/// the rename): the process cannot tell which root is on disk, so it must not
/// reopen the reserved files and poisons the owner with a fatal error;
/// recovery then resolves the boundary from disk. After phases 1 and 2 the old
/// root is still the durable one (nothing retired, every file present, the
/// record readable again); after phase 3 the new root survives (file 0
/// retired, pending victims unlinked by recovery).
///
/// Negative control (mutation run `m05`): do not poison after the failed
/// install; the poison assertion fails at every phase.
#[test]
fn an_ambiguous_sealed_root_failure_poison_closes_until_recovery() {
    use std::sync::atomic::Ordering;

    for phase in 1..=3u8 {
        let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
        let receipt = log.receipt_page(0, 1).unwrap()[0];
        let reference = batch(&config, 0)
            .record_reference(receipt.address(), 0)
            .unwrap();
        drop(log);
        let path = temp.path().join("stream");
        let log = Arc::new(recover(&path, &config));
        let victims: Vec<_> = (0..3)
            .map(|file| RetireProof {
                file_start: file * FILE,
                inode: None,
                seal_generation: None,
            })
            .collect();
        let counters = log
            .wal
            .file_read_pin_counters(victims.iter().map(|proof| proof.file_start));
        assert!(counters.iter().all(|(_, counter)| counter.is_some()));
        assert!(
            log.resolve_live_record(&reference).is_ok(),
            "phase {phase}: premise: readable"
        );
        let reservation = log
            .reserve_sealed_retirement(&victims)
            .unwrap()
            .expect("premise: the unpinned victims are reserved");
        log.fail_next_position_persist(phase);
        let error = log.finish_sealed_retirement(reservation).unwrap_err();
        let message = error.to_string();
        let pending = path.join(".partition-wal-root-v1.pending");
        match phase {
            1 => {
                assert!(
                    message.contains("injected crash before WAL root write"),
                    "phase 1: the early root fault fired: {message}"
                );
                assert!(!pending.exists(), "phase 1 writes no temporary root");
            }
            2 => {
                assert!(
                    message.contains("injected crash during WAL root rename"),
                    "phase 2: the rename-phase fault fired: {message}"
                );
                assert!(
                    pending.exists(),
                    "phase 2 leaves its synced temporary root behind"
                );
            }
            _ => {
                assert!(
                    message.contains("injected crash during WAL root rename"),
                    "phase 3: the rename-phase fault fired: {message}"
                );
                assert!(!pending.exists(), "phase 3 renamed the temporary root away");
            }
        }
        assert!(
            matches!(log.state.lock().poison, Some(WriterPoison::Fatal)),
            "phase {phase}: an ambiguous root install poisons the owner"
        );
        for (_, counter) in &counters {
            assert_ne!(
                counter.as_ref().unwrap().load(Ordering::Acquire)
                    & crate::wal::Wal::READ_PIN_CLOSED,
                0,
                "phase {phase}: an ambiguous root must leave every victim closed"
            );
        }
        assert!(
            log.resolve_live_record(&reference).is_err(),
            "phase {phase}: poison refuses reads"
        );
        drop(log);
        let recovered = Arc::new(recover(&path, &config));
        if phase == 3 {
            assert!(
                recovered.is_retired(0).unwrap(),
                "the renamed root survives recovery"
            );
            assert!(
                recovered.resolve_live_record(&reference).is_err(),
                "recovery refuses the retired record"
            );
            for file in 0..3 {
                assert!(
                    !wal_exists(&path, file),
                    "recovery removed pending victim {file}"
                );
            }
        } else {
            assert!(
                !recovered.is_retired(0).unwrap(),
                "phase {phase}: the old root stays"
            );
            for file in 0..3 {
                assert!(
                    wal_exists(&path, file),
                    "phase {phase}: victim {file} must remain"
                );
            }
            assert!(
                recovered.resolve_live_record(&reference).is_ok(),
                "phase {phase}: the record is readable after recovery"
            );
        }
    }
}

/// A pinned member blocks detachment of the whole pending batch. No victim
/// closes until all pins have drained; after closure, no new lease is granted.
/// Negative control: close the other victim counters before checking all pins.
#[test]
fn a_pinned_pending_file_keeps_every_victim_open_until_the_lease_drops() {
    use std::sync::atomic::Ordering;

    let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
    let log = Arc::new(log);
    let path = temp.path().join("stream");
    let receipt = log.receipt_page(0, 1).unwrap()[0];
    let reference = batch(&config, 0)
        .record_reference(receipt.address(), 0)
        .unwrap();
    let lease = log.resolve_live_record(&reference).unwrap();
    let victims = proofs(&log);
    assert_eq!(victims.len(), 3);
    let counters = log
        .wal
        .file_read_pin_counters(victims.iter().map(|proof| proof.file_start));
    assert_eq!(counters.len(), 3);
    assert!(
        counters.iter().all(|(_, counter)| counter.is_some()),
        "premise: registered victims"
    );
    assert_eq!(
        log.authorize_file_retirement(&victims, &witness()).unwrap(),
        RetirementOutcome::Completed
    );
    assert_eq!(log.file_retirement_state().1.len(), 3);
    assert!(
        log.finish_file_retirement().unwrap().is_empty(),
        "the pinned batch is deferred"
    );
    for (file, (_, counter)) in counters.iter().enumerate() {
        let state = counter.as_ref().unwrap().load(Ordering::Acquire);
        assert_eq!(
            state & crate::wal::Wal::READ_PIN_CLOSED,
            0,
            "victim {file} closed early"
        );
        assert!(
            wal_exists(&path, file as u64),
            "victim {file} unlinked early"
        );
    }
    drop(lease);
    let finished = log.finish_file_retirement().unwrap();
    assert_eq!(
        finished.len(),
        3,
        "all victims detach after the lease drops"
    );
    for (file, (_, counter)) in counters.iter().enumerate() {
        let state = counter.as_ref().unwrap().load(Ordering::Acquire);
        assert_ne!(
            state & crate::wal::Wal::READ_PIN_CLOSED,
            0,
            "victim {file} must close"
        );
        assert!(!wal_exists(&path, file as u64), "victim {file} remains");
    }
    assert!(
        log.resolve_live_record(&reference).is_err(),
        "a retired file must refuse new leases"
    );
}

/// A read that has captured a victim counter holds admission until its pin is
/// installed. Retirement cannot close the batch in that interval.
/// Negative control: release read admission before incrementing the pin.
#[test]
fn a_read_in_flight_pins_before_pending_retirement_can_close() {
    let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
    let log = Arc::new(log);
    let receipt = log.receipt_page(0, 1).unwrap()[0];
    let reference = batch(&config, 0)
        .record_reference(receipt.address(), 0)
        .unwrap();
    let victims = proofs(&log);
    let (entered, resume) =
        log.pause_live(crate::distributed::storage::LivePausePoint::ReadPinBeforeIncrement);
    let reader = {
        let log = Arc::clone(&log);
        std::thread::spawn(move || log.resolve_live_record(&reference))
    };
    entered
        .recv_timeout(HOOK_WAIT)
        .expect("read reached pin acquisition");
    assert!(
        log.read_admission.try_write().is_none(),
        "the read holds admission"
    );
    assert_eq!(
        log.authorize_file_retirement(&victims, &witness()).unwrap(),
        RetirementOutcome::Completed
    );
    resume.send(()).unwrap();
    let lease = reader.join().unwrap().expect("read acquired its pin");
    assert!(
        log.finish_file_retirement().unwrap().is_empty(),
        "the read blocks detach"
    );
    assert!(wal_exists(&temp.path().join("stream"), 0));
    drop(lease);
    assert_eq!(log.finish_file_retirement().unwrap().len(), victims.len());
    assert!(!wal_exists(&temp.path().join("stream"), 0));
}

/// A detach failure leaves CLOSED counters and a durable pending root. The
/// same process can finish the pending work, and a crash/reopen also refuses
/// the retired member before serving any read.
/// Negative control: omit counter closure before the detach attempt.
#[test]
fn a_failed_detach_keeps_retired_files_closed_through_retry_and_reopen() {
    use std::sync::atomic::Ordering;

    for reopen in [false, true] {
        let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
        let log = Arc::new(log);
        let path = temp.path().join("stream");
        let receipt = log.receipt_page(0, 1).unwrap()[0];
        let reference = batch(&config, 0)
            .record_reference(receipt.address(), 0)
            .unwrap();
        let victims = proofs(&log);
        let counters = log
            .wal
            .file_read_pin_counters(victims.iter().map(|proof| proof.file_start));
        log.arm_crash_cut(CrashCut::BeforeDetach);
        assert!(log.retire_proved_files(&victims, Some(&witness())).is_err());
        assert_eq!(log.file_retirement_state().1.len(), victims.len());
        for (file, (_, counter)) in counters.iter().enumerate() {
            let state = counter
                .as_ref()
                .expect("registered victim")
                .load(Ordering::Acquire);
            assert_ne!(
                state & crate::wal::Wal::READ_PIN_CLOSED,
                0,
                "victim {file} must close"
            );
            assert!(wal_exists(&path, file as u64), "detach did not run");
        }
        assert!(
            log.resolve_live_record(&reference).is_err(),
            "retired root refuses a lease"
        );
        if reopen {
            drop(log);
            let recovered = Arc::new(recover(&path, &config));
            assert!(recovered.is_retired(0).unwrap());
            assert!(
                recovered.resolve_live_record(&reference).is_err(),
                "recovery refuses a lease"
            );
            for file in 0..3 {
                assert!(
                    !wal_exists(&path, file),
                    "recovery removed pending victim {file}"
                );
            }
        } else {
            assert_eq!(log.finish_file_retirement().unwrap().len(), victims.len());
            for file in 0..3 {
                assert!(!wal_exists(&path, file), "retry removed victim {file}");
            }
        }
    }
}

#[test]
fn retirement_rejects_wrong_inode_seal_and_uncovered_witness() {
    let (_temp, _config, log) = sealed_stream();
    let mut victims = proofs(&log);
    victims[0].inode = Some(WalInode {
        device: 1,
        inode: 1,
    });
    // The pre-revoke check refuses it with no durable change and no seal
    // consumed; the node revokes nothing for a refused request.
    let before = log.file_retirement_state();
    assert!(log.precheck_file_retirement(&victims, &witness()).is_err());
    assert_eq!(log.file_retirement_state(), before);
    assert_eq!(
        log.precheck_file_retirement(&proofs(&log), &witness())
            .unwrap(),
        RetirementOutcome::Completed
    );
    assert_eq!(log.file_retirement_state(), before);
    assert!(log.retire_proved_files(&victims, Some(&witness())).is_err());
    let mut victims = proofs(&log);
    victims[0].seal_generation = victims[0].seal_generation.map(|generation| generation + 1);
    assert!(log.retire_proved_files(&victims, Some(&witness())).is_err());
    let victims = proofs(&log);
    // Each watermark term is independent: either one below the file end
    // refuses the request, with no durable change.
    let mut uncovered = witness();
    uncovered.callback_from = FILE / 2;
    assert!(log.retire_proved_files(&victims, Some(&uncovered)).is_err());
    let mut uncovered = witness();
    uncovered.replay_from = FILE / 2;
    assert!(log.retire_proved_files(&victims, Some(&uncovered)).is_err());
    let mut cut = witness();
    cut.persisted_cut_next = Some(1);
    assert!(log.retire_proved_files(&victims, Some(&cut)).is_err());
    assert!(log.retire_proved_files(&victims, None).is_err());
    // Nothing was retired by the refused requests.
    assert!(!log.is_retired(0).unwrap());
    assert_eq!(
        log.retire_proved_files(&victims, Some(&witness())).unwrap(),
        RetirementOutcome::Completed
    );
    assert!(log.is_retired(0).unwrap() && log.is_retired(FILE).unwrap());
    assert!(log.is_retired(2 * FILE).unwrap());
}

/// A prepared grant takes only `state`: it completes while maintenance holds
/// the metadata-writer gate across a seal or retirement root sync.
#[test]
fn prepared_grant_completes_while_maintenance_holds_persistence() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    fill(&log, &config, 0, 1);
    let current = log.lease_live_extent(authority(&config)).unwrap().0;
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    let paused = log.persistence.lock();
    let (done, finished) = std::sync::mpsc::sync_channel(1);
    let owner = Arc::clone(&log);
    let config_copy = config.clone();
    let worker = std::thread::spawn(move || {
        let next = owner.prelease_live_extent(authority(&config_copy), current.token);
        done.send(next.map(|(extent, _)| extent.segment.file_index))
            .expect("grant completion receiver");
    });
    let granted = finished
        .recv_timeout(GRANT_WAIT)
        .expect("prepared grant waited for maintenance");
    drop(paused);
    worker.join().unwrap();
    assert_eq!(granted.unwrap(), 1);
}

/// The root names a seal only together with `synced_end >= end`, and only after
/// the file was synced. Sealing with no earlier position proof (the Native
/// tier, where nothing else advances `synced_end`) used to install a seal over
/// `synced_end == 0`, which `validate_root_sets` refuses at the next open.
#[test]
fn a_seal_installed_without_a_prior_position_proof_reopens() {
    let (temp, config, log) = fixture();
    fill(&log, &config, 0, 6);
    assert_eq!(synced_end(&log), 0, "no position proof was taken");
    assert_eq!(log.seal_ready_files().unwrap(), 3);
    assert!(synced_end(&log) >= 3 * FILE);
    drop(log);
    let recovered = recover(&temp.path().join("stream"), &config);
    assert_eq!(recovered.file_retirement_state().0.len(), 3);
    assert!(synced_end(&recovered) >= 3 * FILE);
}

/// Crash at each boundary of the seal: before the sync, after the sync before
/// the root, and after the root before it is published. Every cut reopens with
/// a consistent root, either without the seal or with the seal and its
/// `synced_end`; never a seal over a smaller `synced_end`.
#[test]
fn seal_sync_and_root_crash_cuts_reopen_with_or_without_the_seal() {
    for (cut, sealed) in [
        (CrashCut::SealBeforeSync, false),
        (CrashCut::SealAfterSync, false),
        (CrashCut::SealAfterRoot, true),
    ] {
        let (temp, config, log) = fixture();
        fill(&log, &config, 0, 6);
        log.arm_crash_cut(cut);
        assert!(log.seal_ready_files().is_err(), "{cut:?}");
        drop(log);
        let path = temp.path().join("stream");
        let recovered = recover(&path, &config);
        let (seals, pending) = recovered.file_retirement_state();
        assert!(pending.is_empty(), "{cut:?}");
        if sealed {
            assert_eq!(seals.len(), 1, "{cut:?}");
            assert!(synced_end(&recovered) >= seals[0].end, "{cut:?}");
        } else {
            assert!(seals.is_empty(), "{cut:?}");
        }
        for file in 0..3 {
            assert!(wal_exists(&path, file), "{cut:?} lost file {file}");
        }
    }
}

fn root_bytes(log: &PartitionLog) -> Vec<u8> {
    log.state
        .lock()
        .wal_root
        .as_ref()
        .expect("a live stream has a root")
        .encode()
        .expect("encode the root")
}

/// After a failed `fsync` the kernel may drop the dirty state, so a retried
/// sync could report success over bytes that never reached the disk. The seal
/// path poisons the stream, installs nothing and is never retried; the owner
/// that restarts finds the pre-seal root and opens (a recovered epoch is an
/// immutable sealed stream, so its files retire without per-file seals).
///
/// The poison refusal is `seal_allowed`'s `state.poison.is_none()` clause: the
/// injected failure is one-shot (the cut disarms itself), so a second kick that
/// still sealed nothing can only have been refused by the poison.
#[test]
fn a_seal_sync_failure_poisons_and_is_never_retried() {
    let (temp, config, log) = fixture();
    fill(&log, &config, 0, 6);
    let before = root_bytes(&log);
    log.arm_crash_cut(CrashCut::SealSyncFails);
    let error = log.seal_ready_files().unwrap_err();
    assert!(
        matches!(error, StorageError::Unknown(_)),
        "a failed seal sync is the fatal-sync variant (readers poison, no retry): {error:?}"
    );
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
    assert!(log.file_retirement_state().0.is_empty());
    assert_eq!(synced_end(&log), 0);
    assert_eq!(
        root_bytes(&log),
        before,
        "a failed seal leaves the root untouched"
    );
    assert_eq!(
        log.seal_ready_files().unwrap(),
        0,
        "no retry on a poisoned stream"
    );
    assert_eq!(root_bytes(&log), before);
    drop(log);

    // The owner restarts: the root on disk is the pre-seal root and the open
    // succeeds. A recovered epoch is an immutable sealed stream
    // (`recover_sealed_impl` installs `sealed: true`), so no seal kick runs on
    // it; its unsealed files retire through the sealed-stream path.
    let path = temp.path().join("stream");
    let recovered = recover(&path, &config);
    assert!(recovered.file_retirement_state().0.is_empty());
    assert!(
        recovered.state.lock().poison.is_none(),
        "poison is per process"
    );
    assert_eq!(synced_end(&recovered), 0);
    assert_eq!(
        recovered.seal_ready_files().unwrap(),
        0,
        "a recovered epoch seals nothing"
    );
    for file in 0..3 {
        assert!(
            wal_exists(&path, file),
            "file {file} exists before retirement"
        );
    }
    recovered
        .retire_wal_files(&[0, FILE, 2 * FILE])
        .expect("the unsealed files of the old epoch retire");
    for file in 0..3 {
        assert!(!wal_exists(&path, file), "file {file}");
    }
    drop(recovered);
    let reopened = recover(&path, &config);
    assert!(reopened.is_retired(0).unwrap());
}

/// Partial progress: the first file seals (clean sync, root with seal and
/// `synced_end`), the second file's sync fails. The stream is poisoned, the
/// durable root holds exactly the first seal, a reopen is consistent (the
/// recovered epoch is sealed whole, so its files retire without seals).
#[test]
fn a_seal_sync_failure_after_progress_keeps_the_earlier_seal_and_the_old_epoch_retires() {
    let (temp, config, log) = fixture();
    // Three fragments: the writer has passed file 0 only (two fragments per
    // file), so exactly one file is ready.
    fill(&log, &config, 0, 3);
    assert_eq!(log.seal_ready_files().unwrap(), 1);
    let first = log.file_retirement_state().0[0];
    assert!(synced_end(&log) >= first.end);
    // Three more fragments: files 1 and 2 are now past the allocator.
    fill(&log, &config, 3, 3);
    log.arm_crash_cut(CrashCut::SealSyncFails);
    assert!(log.seal_ready_files().is_err());
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
    let (seals, _) = log.file_retirement_state();
    assert_eq!(seals, vec![first], "only the cleanly synced file is sealed");
    assert_eq!(log.seal_ready_files().unwrap(), 0, "poisoned: no retry");
    drop(log);

    let path = temp.path().join("stream");
    let recovered = recover(&path, &config);
    let (seals, pending) = recovered.file_retirement_state();
    assert_eq!(
        seals,
        vec![first],
        "the reopened root names the first seal only"
    );
    assert!(pending.is_empty());
    assert!(synced_end(&recovered) >= first.end);
    // A recovered epoch is an immutable sealed stream (`recover_sealed_impl`
    // installs `sealed: true`): it is never sealed file by file again, and its
    // files retire without per-file seals.
    assert_eq!(
        recovered.seal_ready_files().unwrap(),
        0,
        "a recovered epoch seals nothing"
    );
    for file in 0..3 {
        assert!(
            wal_exists(&path, file),
            "file {file} exists before retirement"
        );
    }
    recovered
        .retire_wal_files(&[0, FILE, 2 * FILE])
        .expect("the unsealed files of the old epoch retire");
    for file in 0..3 {
        assert!(!wal_exists(&path, file), "file {file}");
        assert!(recovered.is_retired(file * FILE).unwrap());
    }
}

/// The sync runs with no lock held: a put (lease, write, adopt) completes while
/// a seal is parked before its `fdatasync`, and nothing is sealed until the
/// sync returns. The put runs on its own thread and its completion is awaited
/// with a bound, so a regression that holds `state` or `persistence` across the
/// sync fails the test instead of hanging it.
#[test]
fn a_put_completes_while_a_seal_sync_is_blocked() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    fill(&log, &config, 0, 6);
    let (entered, resume) = log.pause_before_seal_sync();
    let sealer = Arc::clone(&log);
    let worker = std::thread::spawn(move || sealer.seal_ready_files());
    entered
        .recv_timeout(HOOK_WAIT)
        .expect("the seal reached its file sync");
    assert!(log.file_retirement_state().0.is_empty());
    assert_eq!(synced_end(&log), 0);
    let (done, finished) = std::sync::mpsc::sync_channel(1);
    let putter = {
        let log = Arc::clone(&log);
        let config = config.clone();
        std::thread::spawn(move || {
            fill(&log, &config, 6, 1);
            done.send(()).expect("put completion receiver");
        })
    };
    let completed = finished.recv_timeout(PUT_WAIT);
    // Release the parked sync before any assertion so no thread outlives the test.
    resume.send(()).unwrap();
    let sealed = worker.join().unwrap();
    putter.join().unwrap();
    completed.expect("a put completes while a seal sync is parked");
    assert_eq!(sealed.unwrap(), 3);
}
