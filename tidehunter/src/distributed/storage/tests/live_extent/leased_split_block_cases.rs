use super::*;

#[test]
fn leased_split_block_reads_keep_a_high_water_buffer_and_exact_receipts() {
    let (_temp, config, log) = split_block_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = extent.start;
    let mut high_water = 0;
    let mut digest = [0; 32];
    let mut receipts = Vec::new();
    for (sequence, size) in [5000, 64, 14000, 64, 5000].into_iter().enumerate() {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence as u64, size)]);
        let receipt = expected(&config, &group, end, frame.len())[0];
        write(&extent, end, &frame);
        end += frame.len() as u64;
        high_water = high_water.max(frame.len());
        digest = extend_live_receipt_digest(digest, receipt);
        receipts.push(receipt);
        let adopted = log
            .adopt_live_extent(authority(&config), extent.token, end, false)
            .unwrap()
            .0;
        assert_eq!(adopted.receipt_count, sequence as u64 + 1);
        assert_eq!(adopted.receipt_digest, digest);
        assert_eq!(adopted.adopted_end, end);
        let gate = {
            let state = log.state.lock();
            Arc::clone(&state.open_live_extent.as_ref().unwrap().round)
        };
        assert_eq!(gate.lock().buffers[0].len(), high_water);
    }
    assert_eq!(log.receipt_page(0, receipts.len()).unwrap(), receipts);
}

#[test]
fn leased_split_block_reads_reject_short_declarations_and_corrupt_suffixes() {
    for defect in ["short", "torn", "value", "padding"] {
        let (_temp, config, log) = split_block_fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (first, prefix) = encoded(&config, vec![sized_batch(&config, 0, 64)]);
        write(&extent, extent.start, &prefix);
        let (_, mut suffix) = encoded(&config, vec![sized_batch(&config, 1, 5000)]);
        assert!(suffix.len() > 4096);
        let end = prefix.len() as u64 + suffix.len() as u64;
        match defect {
            "value" => suffix[4096] ^= 1,
            "padding" => *suffix.last_mut().unwrap() ^= 1,
            _ => {}
        }
        write(
            &extent,
            prefix.len() as u64,
            if defect == "torn" {
                &suffix[..4096]
            } else {
                &suffix
            },
        );
        let through = if defect == "short" { end - 4096 } else { end };
        assert!(
            matches!(
                log.adopt_live_extent(authority(&config), extent.token, through, false),
                Err(StorageError::IncompleteLiveFrame),
            ),
            "{defect}"
        );
        assert_eq!(log.appended_frontier(), prefix.len() as u64, "{defect}");
        assert_eq!(
            log.receipt_page(0, 1).unwrap(),
            expected(&config, &first, extent.start, prefix.len()),
            "{defect}",
        );
        assert_eq!(
            log.state
                .lock()
                .open_live_extent
                .as_ref()
                .unwrap()
                .adoption
                .receipt_count,
            1,
        );
    }
}

#[test]
fn prelease_requires_open_extent_and_is_single() {
    let (_temp, config, log) = fixture();
    assert!(matches!(
        log.prelease_live_extent(authority(&config), [1; 16]),
        Err(StorageError::Invalid(
            "prelease requires the open live extent"
        ))
    ));
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    assert!(matches!(
        log.prelease_live_extent(authority(&config), [1; 16]),
        Err(StorageError::Invalid(
            "prelease requires the open live extent"
        ))
    ));
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    assert_eq!(next.start, first.end);
    assert!(matches!(
        log.prelease_live_extent(authority(&config), first.token),
        Err(StorageError::Invalid("live extent already preleased"))
    ));
    assert!(
        !log.can_release_live_segment(next.segment.file_index)
            .unwrap()
    );
}

#[test]
fn close_promotes_preleased_extent() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    skip(&first, first.start);
    log.adopt_live_extent(authority(&config), first.token, first.start, true)
        .unwrap();
    let state = log.state.lock();
    assert_eq!(
        state.open_live_extent.as_ref().unwrap().extent.token,
        next.token
    );
    assert!(state.next_live_extent.is_none());
}

#[test]
fn leased_recovery_with_empty_preleased_extent_keeps_all_frames() {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&first, first.start, &frame);
    log.persist_appended().unwrap();
    log.prelease_live_extent(authority(&config), first.token)
        .unwrap();
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
    assert_eq!(recovered.appended_frontier(), 4096);
    let receipt = recovered.receipt_page(0, 1).unwrap()[0];
    assert_eq!(
        recovered.read_batch(receipt).unwrap().mutations(),
        batch(&config, 0).mutations()
    );
}

#[test]
fn leased_recovery_after_crash_between_skip_marker_and_first_frame_of_next_extent() {
    for next_written in [false, true] {
        let (temp, config, log) = fixture();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        let next = log
            .prelease_live_extent(authority(&config), first.token)
            .unwrap()
            .0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&first, first.start, &frame);
        log.persist_appended().unwrap();
        skip(&first, first.start + 4096);
        if next_written {
            let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
            write(&next, next.start, &frame);
        }
        // No close metadata is needed: these are also the bytes at a crash
        // during the background close, before it publishes the next extent.
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.receipt_count(), if next_written { 2 } else { 1 });
        for (sequence, receipt) in recovered
            .receipt_page(0, 2)
            .unwrap()
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                recovered.read_batch(receipt).unwrap().mutations(),
                batch(&config, sequence as u64).mutations()
            );
        }
    }
}

#[test]
fn seal_with_open_and_preleased_extents_adopts_across_the_skip_marker() {
    let (temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&first, first.start, &frame);
    skip(&first, first.start + 4096);
    let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
    write(&next, next.start, &frame);
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    assert_eq!(log.receipt_count(), 2);
    assert_eq!(log.synced_frontier(), next.start + 4096);
    assert!(log.state.lock().next_live_extent.is_none());
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 2);
}

/// A seal retires the syncer before it drains frames that were written but
/// whose adoption was never queued (the crashed-coordinator shape). The seal
/// must adopt them, and its own persist must be the covering barrier: the
/// retired frontier must not reject the frame.
#[test]
fn seal_after_a_retired_syncer_adopts_a_written_unadopted_frame() {
    let (temp, config, log) = fixture();
    let frontier = log
        .attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, extent.start, &frame);
    // inventory::seal_epoch retires the syncer before seal_live_epoch, so the
    // frontier is already stopping when the seal drains the unadopted frame.
    frontier.stop();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    let end = extent.start + frame.len() as u64;
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(
        log.synced_frontier(),
        end,
        "the seal persist covers the frame it adopted"
    );
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
    let receipt = recovered.receipt_page(0, 1).unwrap().remove(0);
    assert_eq!(
        recovered.read_batch(receipt).unwrap().mutations(),
        batch(&config, 0).mutations(),
        "the adopted frame survives the seal and reopen"
    );
}

/// A seal whose persist cannot cover the frame it adopted must fail instead of
/// reporting success. Recovery then replays the frame.
#[test]
fn seal_fails_when_its_persist_cannot_cover_an_adopted_frame() {
    let (temp, config, log) = fixture();
    let frontier = log
        .attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, extent.start, &frame);
    frontier.stop();
    log.fail_next_appended_persist();
    let result = log.seal_live_epoch(authority(&config), || Ok(()));
    assert!(
        matches!(result, Err(StorageError::Unknown(WalError::Io(_)))),
        "the seal must report Err(Unknown(Io)) but returned {result:?}"
    );
    assert!(
        log.appended_persist_failure_consumed(),
        "the injected appended persist failure fired"
    );
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(
        recovered.receipt_count(),
        1,
        "recovery replays the frame the failed seal adopted"
    );
}

#[test]
fn seal_rejects_live_frames_beyond_a_torn_extent_tail() {
    let (_temp, config, log) = fixture();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&first, first.start, &frame);
    // No valid skip bridges the unwritten old tail into the next frame.
    let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
    write(&next, next.start, &frame);
    assert!(matches!(
        log.seal_live_epoch(authority(&config), || Ok(())),
        Err(StorageError::Invalid(
            "live frames beyond a torn extent tail"
        ))
    ));
    assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
}

#[test]
fn owner_read_of_preleased_extent_frame_while_old_extent_closes() {
    {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let (_temp, config, log) = fixture();
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let log = Arc::new(log);
            let first = log.lease_live_extent(authority(&config)).unwrap().0;
            let next = log
                .prelease_live_extent(authority(&config), first.token)
                .unwrap()
                .0;
            let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
            write(&first, first.start, &frame);
            skip(&first, first.start + frame.len() as u64);
            let (group, frame) = encoded(&config, vec![batch(&config, 1)]);
            write(&next, next.start, &frame);
            let receipt = expected(&config, &group, next.start, frame.len())[0];
            let reference = group.batches()[0]
                .record_reference(receipt.address(), 0)
                .unwrap();
            let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
            let closer = log.clone();
            let auth = authority(&config);
            let close = std::thread::spawn(move || {
                closer.adopt_live_extent(auth, first.token, first.start + 4096, true)
            });
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(log.state.lock().open_live_extent.as_ref().unwrap().closing);
            if tier == DurabilityTier::Native {
                let plan = log.plan_record_read(&reference).unwrap();
                let mut bytes = vec![0; plan.member.len];
                plan.member
                    .file
                    .read_exact_at(&mut bytes, plan.member.offset)
                    .unwrap();
                assert_eq!(
                    CompleteBatch::decode_record(&shape(), &reference, bytes.into()).unwrap(),
                    group.batches()[0].mutations()[0]
                );
                drop(plan);
                assert!(log.resolve_live_record(&reference).is_ok());
            } else {
                assert!(matches!(
                    log.plan_record_read(&reference),
                    Err(StorageError::NeedsPersistence)
                ));
                assert!(log.resolve_live_record(&reference).is_ok());
            }
            let mut invalid = reference;
            invalid.batch_address.len += 8;
            assert!(matches!(
                log.plan_record_read(&invalid),
                Err(StorageError::Invalid(
                    "WAL member differs from the reference"
                ))
            ));
            resume.send(()).unwrap();
            close.join().unwrap().unwrap();
            log.adopt_live_extent(authority(&config), next.token, next.start + 4096, false)
                .unwrap();
            log.persist_appended().unwrap();
            assert_eq!(
                log.read_record(&reference).unwrap(),
                group.batches()[0].mutations()[0]
            );
        }
    }
}

#[test]
fn position_crash_cuts_with_interleaved_adoption_recover_in_both_tiers() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        // Root write, root file sync, rename, before data, after data, after root.
        for phase in 1..=6 {
            let (temp, config, log) = fixture();
            let log = Arc::new(log);
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let (first, bytes) = encoded(&config, vec![batch(&config, 0)]);
            write(&extent, 0, &bytes);
            log.self_adopt_live_extent().unwrap();
            let (second, bytes) = encoded(&config, vec![batch(&config, 1)]);
            write(&extent, 4096, &bytes);
            if tier == DurabilityTier::Synchronous {
                // The initiator's data-sync completion is independent of proof metadata.
                OpenOptions::new()
                    .write(true)
                    .open(&extent.segment.path)
                    .unwrap()
                    .sync_data()
                    .unwrap();
            }
            let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
            let proof_log = log.clone();
            let proof = std::thread::spawn(move || proof_log.persist_wal_through(4096));
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            // Adoption moves beyond the captured cut while the proof has no lock.
            let adopted = log.self_adopt_live_extent();
            log.fail_wal_root.store(phase, Ordering::Release);
            release.send(()).unwrap();
            adopted.unwrap();
            assert!(proof.join().unwrap().is_err(), "phase {phase}, tier {tier}");
            drop(log);
            let recovered = PartitionLog::recover_live_sealed(
                &temp.path().join("stream"),
                &config,
                metrics(),
                // This direct-file fixture has no exported writable target.
                || Ok(()),
            )
            .unwrap();
            for (n, group, offset) in [(0, first, 0), (1, second, 4096)] {
                let receipt = expected(&config, &group, offset, 4096)[0];
                let reference = group.batches()[0]
                    .record_reference(receipt.address(), 0)
                    .unwrap();
                assert_eq!(
                    recovered.read_record(&reference).unwrap(),
                    batch(&config, n).mutations()[0]
                );
            }
        }
    }
}

#[test]
fn position_proof_covers_completed_leased_bytes_before_owner_adoption() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        for read_kind in 0..3 {
            let (_temp, config, log) = fixture();
            let log = Arc::new(log);
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let (group, bytes) = encoded(&config, vec![batch(&config, 0)]);
            write(&extent, 0, &bytes);
            let through = bytes.len() as u64;
            if tier == DurabilityTier::Synchronous {
                OpenOptions::new()
                    .write(true)
                    .open(&extent.segment.path)
                    .unwrap()
                    .sync_data()
                    .unwrap();
            }
            assert!(
                through > log.appended_frontier(),
                "the requested leased cut must exceed the owner's adopted frontier"
            );
            assert!(log.persist_wal_through(through).unwrap() >= through);
            assert_eq!(log.position_frontier_for_test(), through);
            assert!(
                log.appended_frontier() < through,
                "proof must not require adoption"
            );
            assert!(log.synced_frontier() < through);
            assert_eq!(log.receipt_count(), 0);
            let receipt = expected(&config, &group, extent.start, bytes.len())[0];
            let reference = group.batches()[0]
                .record_reference(receipt.address(), 0)
                .unwrap();
            match read_kind {
                0 => assert_eq!(
                    log.read_record(&reference).unwrap(),
                    batch(&config, 0).mutations()[0]
                ),
                1 => assert_eq!(
                    log.read_batch(receipt).unwrap().mutations(),
                    batch(&config, 0).mutations()
                ),
                _ => assert_eq!(
                    log.resolve_live_batch(receipt).unwrap().len,
                    receipt.address().len
                ),
            }
            // Existing lookup still discovers and validates/adopts the frame;
            // the durable position root is not a substitute for that authority.
            assert_eq!(log.receipt_count(), 1);
            if tier == DurabilityTier::Synchronous {
                assert!(
                    log.synced_frontier() >= through,
                    "the read must obtain its own receipt barrier"
                );
            }
        }
    }
}

#[test]
fn position_root_does_not_authorize_an_above_cut_read() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, first) = encoded(&config, vec![batch(&config, 0)]);
        write(&extent, 0, &first);
        let cut = first.len() as u64;
        assert_eq!(log.persist_wal_through(cut).unwrap(), cut);
        let (second, bytes) = encoded(&config, vec![batch(&config, 1)]);
        write(&extent, cut, &bytes);
        let receipt = expected(&config, &second, cut, bytes.len())[0];
        assert!(receipt.address().offset + u64::from(receipt.address().len) > cut);
        let reference = second.batches()[0]
            .record_reference(receipt.address(), 0)
            .unwrap();
        let record_read = log.read_record(&reference);
        let profiled_read = log.read_record_profiled(&reference);
        let batch_read = log.read_batch(receipt);
        let lease_read = log.resolve_live_batch(receipt);
        if tier == DurabilityTier::Synchronous {
            assert!(matches!(batch_read, Err(StorageError::NeedsPersistence)));
            assert!(matches!(lease_read, Err(StorageError::NeedsPersistence)));
            assert!(matches!(record_read, Err(StorageError::NeedsPersistence)));
            assert!(matches!(profiled_read, Err(StorageError::NeedsPersistence)));
        } else {
            // Native already permits complete, validated page-cache reads.
            assert_eq!(
                batch_read.unwrap().mutations(),
                batch(&config, 1).mutations()
            );
            assert_eq!(lease_read.unwrap().len, receipt.address().len);
            assert_eq!(record_read.unwrap(), batch(&config, 1).mutations()[0]);
            assert_eq!(profiled_read.unwrap().0, batch(&config, 1).mutations()[0]);
        }
        assert_eq!(
            log.receipt_count(),
            2,
            "existing lookup must adopt/validate"
        );
        assert_eq!(log.position_frontier_for_test(), cut);
        assert_eq!(log.synced_frontier(), 0);
    }
}

#[test]
fn leased_prelease_completes_and_opportunistic_scan_waits_during_validation() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![sized_batch(&config, 0, 600 * 1024)]);
    write(&extent, 0, &frame);
    write(
        &extent,
        frame.len() as u64,
        &encoded(&config, vec![sized_batch(&config, 1, 600 * 1024)]).1,
    );
    let through = 2 * frame.len() as u64;
    let (entered, resume) = log.pause_live(LivePausePoint::ValidateUnlocked);
    let adopter = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, through, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Capacity one, one result per worker; dropping the receiver ends the send.
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let grant_log = log.clone();
    let grant = std::thread::spawn(move || {
        tx.send(grant_log.prelease_live_extent(auth, token))
            .unwrap();
    });
    let granted = rx.recv_timeout(Duration::from_secs(5));
    if granted.is_err() {
        resume.send(()).unwrap();
        grant.join().unwrap();
        adopt.join().unwrap().unwrap();
        panic!("prelease waited for unlocked adoption validation");
    }
    assert_eq!(granted.unwrap().unwrap().0.start, extent.end);
    grant.join().unwrap();
    let (snapshot, resume_snapshot) = log.pause_live(LivePausePoint::ScanSnapshot);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let scanner = log.clone();
    let scan = std::thread::spawn(move || {
        let result = scanner.self_adopt_live_extent();
        tx.send((result, scanner.receipt_count())).unwrap();
    });
    snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_snapshot.send(()).unwrap();
    assert!(matches!(
        rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    resume.send(()).unwrap();
    let (result, count) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    result.unwrap();
    assert_eq!(count, 2);
    scan.join().unwrap();
    assert_eq!(adopt.join().unwrap().unwrap().0.receipt_count, 2);
}

#[test]
fn leased_parallel_publication_matches_frame_by_frame_authority() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let other_temp = tempdir::TempDir::new("live-serial-adoption").unwrap();
    let serial = PartitionLog::create_live(
        &other_temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let other = serial.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    let mut receipts = Vec::new();
    let mut serial_summary = None;
    for sequence in 0..12 {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 64 * 1024)]);
        receipts.extend(expected(&config, &group, end, frame.len()));
        write(&extent, end, &frame);
        write(&other, end, &frame);
        end += frame.len() as u64;
        serial_summary = Some(
            serial
                .adopt_live_extent(authority(&config), other.token, end, false)
                .unwrap()
                .0,
        );
    }
    let adopted = log
        .adopt_live_extent(authority(&config), extent.token, end, false)
        .unwrap()
        .0;
    let serial_summary = serial_summary.unwrap();
    assert_eq!(adopted.adopted_end, serial_summary.adopted_end);
    assert_eq!(adopted.receipt_count, serial_summary.receipt_count);
    assert_eq!(adopted.receipt_digest, serial_summary.receipt_digest);
    assert_eq!(log.receipt_page(0, 16).unwrap(), receipts);
    assert_eq!(serial.receipt_page(0, 16).unwrap(), receipts);
    log.persist_many(&receipts).unwrap();
    serial.persist_many(&receipts).unwrap();
    for receipt in receipts {
        assert_eq!(
            log.read_batch(receipt).unwrap().mutations(),
            serial.read_batch(receipt).unwrap().mutations()
        );
        assert_eq!(
            log.record_reference(receipt, 0).unwrap(),
            serial.record_reference(receipt, 0).unwrap()
        );
    }
    let diagnostics = log.live_test_diagnostics().adoption;
    assert_eq!(diagnostics.frames, 12);
    assert_eq!(diagnostics.rounds, 1);
    assert_eq!(diagnostics.parallel_rounds, 1);
    assert_eq!(diagnostics.bytes, end);
    assert_eq!(serial.live_test_diagnostics().adoption.parallel_rounds, 0);
}

#[test]
fn leased_parallel_validation_publishes_only_the_prefix_before_corruption() {
    for digest_defect in [false, true] {
        for opportunistic in [false, true] {
            let (_temp, config, log) = parallel_adoption_fixture();
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let mut end = 0;
            let mut corrupt_start = 0;
            let mut prefix = Vec::new();
            // Equal frame sizes split after frame 5. Frame 7 fails in the
            // second worker's half, after all of the first half is valid.
            for sequence in 0..12 {
                let (group, mut frame) =
                    encoded(&config, vec![sized_batch(&config, sequence, 64 * 1024)]);
                if sequence < 7 {
                    prefix.extend(expected(&config, &group, end, frame.len()));
                }
                if sequence == 7 {
                    corrupt_start = end;
                    if digest_defect {
                        let member = 8 + group.batch_offsets()[0] as usize;
                        let len = u32::from_be_bytes(frame[member..member + 4].try_into().unwrap())
                            as usize;
                        // Change only the stored batch digest, then repair both
                        // enclosing CRCs so failure must reach digest validation.
                        frame[member + 8 + crate::distributed::codec::HEADER_LEN - 1] ^= 1;
                        let crc = crc32fast::hash(&frame[member + 8..member + 8 + len]);
                        frame[member + 4..member + 8].copy_from_slice(&crc.to_be_bytes());
                        let crc = crc32fast::hash(&frame[8..]);
                        frame[4..8].copy_from_slice(&crc.to_be_bytes());
                    } else {
                        frame[4] ^= 1;
                    }
                }
                write(&extent, end, &frame);
                end += frame.len() as u64;
            }
            let result = if opportunistic {
                log.self_adopt_live_extent()
            } else {
                log.adopt_live_extent(authority(&config), extent.token, end, false)
                    .map(|_| ())
            };
            if opportunistic {
                result.unwrap();
                assert!(log.state.lock().poison.is_none());
            } else {
                assert!(matches!(result, Err(StorageError::IncompleteLiveFrame)));
                assert!(matches!(
                    log.state.lock().poison,
                    Some(WriterPoison::IncompleteLiveFrame)
                ));
            }
            assert_eq!(log.receipt_count(), 7);
            assert_eq!(log.appended_frontier(), corrupt_start);
            assert_eq!(
                log.state
                    .lock()
                    .open_live_extent
                    .as_ref()
                    .unwrap()
                    .adoption
                    .adopted_end,
                corrupt_start
            );
            assert_eq!(log.receipt_page(0, 16).unwrap(), prefix);
            let digest = prefix
                .iter()
                .copied()
                .fold([0; 32], extend_live_receipt_digest);
            assert_eq!(
                log.state
                    .lock()
                    .open_live_extent
                    .as_ref()
                    .unwrap()
                    .adoption
                    .receipt_digest,
                digest,
            );
            for sequence in 7..12 {
                assert!(log.lookup(batch(&config, sequence).id()).unwrap().is_none());
            }
            let diagnostics = log.live_test_diagnostics().adoption;
            assert_eq!(diagnostics.rounds, 1);
            assert_eq!(diagnostics.parallel_rounds, 1);
        }
    }
}

#[test]
fn leased_concurrent_declared_adopters_share_one_round_and_publish_once() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    for sequence in 0..4 {
        let (_, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    let gate = Arc::clone(&log.state.lock().open_live_extent.as_ref().unwrap().round);
    let active = Arc::clone(&gate.lock().active);
    let maximum = Arc::clone(&gate.lock().maximum_active);
    let (entered, resume) = log.pause_live(LivePausePoint::ValidateUnlocked);
    let auth = authority(&config);
    let token = extent.token;
    let first_log = log.clone();
    let first = std::thread::spawn(move || first_log.adopt_live_extent(auth, token, end, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(active.load(Ordering::Relaxed), 1);
    assert!(gate.try_lock().is_none());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let second_barrier = barrier.clone();
    let second_log = log.clone();
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second_log.adopt_live_extent(auth, token, end, false)
    });
    barrier.wait();
    resume.send(()).unwrap();
    assert_eq!(
        first.join().unwrap().unwrap().0,
        second.join().unwrap().unwrap().0
    );
    assert_eq!(maximum.load(Ordering::Relaxed), 1);
    assert_eq!(active.load(Ordering::Relaxed), 0);
    assert_eq!(log.receipt_count(), 4);
    assert_eq!(log.live_test_diagnostics().adoption.frames, 4);
}

#[test]
fn leased_close_during_validation_discards_the_stale_round() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    skip(&extent, 4096);
    log.adopt_live_extent(authority(&config), extent.token, 4096, false)
        .unwrap();
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 8192, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (closing, close_resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let closer = log.clone();
    let close = std::thread::spawn(move || {
        closer.close_live_extent(
            token,
            4096,
            &mut crate::distributed::tcp::AdoptProfile::default(),
        )
    });
    closing.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        log.lease_live_extent(auth),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.prelease_live_extent(auth, token),
        Err(StorageError::Invalid(_))
    ));
    close_resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().closed);
    assert_eq!(log.lease_live_extent(auth).unwrap().0.start, extent.end);
    resume.send(()).unwrap();
    assert!(matches!(
        adopt.join().unwrap(),
        Err(StorageError::Invalid("closed live extent"))
    ));
    assert_eq!(log.receipt_count(), 1);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_seal_during_validation_discards_the_stale_round() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for sequence in 0..2 {
        write(
            &extent,
            sequence * 4096,
            &encoded(&config, vec![batch(&config, sequence)]).1,
        );
    }
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 8192, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (sealing, resume_seal) = log.pause_live(LivePausePoint::ScanSnapshot);
    let sealer = log.clone();
    let seal = std::thread::spawn(move || sealer.seal_live_epoch(auth, || Ok(())));
    sealing.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(log.state.lock().is_sealing());
    resume.send(()).unwrap();
    assert!(matches!(
        adopt.join().unwrap(),
        Err(StorageError::Invalid("live stream is sealing"))
    ));
    // The invalidated declared round publishes nothing. Seal then takes the
    // gate itself and adopts the complete immutable bytes under its old rules.
    assert_eq!(log.receipt_count(), 0);
    assert_eq!(log.live_test_diagnostics().adoption.frames, 0);
    resume_seal.send(()).unwrap();
    seal.join().unwrap().unwrap();
    assert_eq!(log.receipt_count(), 2);
    assert!(log.state.lock().sealed);
}

#[test]
fn leased_crash_after_validation_before_publication_recovers_acknowledged_bytes() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (temp, config, log) = parallel_adoption_fixture();
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut end = 0;
        let mut receipts = Vec::new();
        for sequence in 0..3 {
            let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
            receipts.extend(expected(&config, &group, end, frame.len()));
            write(&extent, end, &frame);
            end += frame.len() as u64;
        }
        if tier == DurabilityTier::Synchronous {
            OpenOptions::new()
                .write(true)
                .open(&extent.segment.path)
                .unwrap()
                .sync_data()
                .unwrap();
        }
        let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
        let adopter = log.clone();
        let token = extent.token;
        let auth = authority(&config);
        let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, end, false));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(log.receipt_count(), 0);
        let image = temp.path().join("crash-image");
        fs::create_dir(&image).unwrap();
        for entry in fs::read_dir(temp.path().join("stream")).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), image.join(entry.file_name())).unwrap();
            }
        }
        resume.send(()).unwrap();
        adopt.join().unwrap().unwrap();
        let recovered =
            PartitionLog::recover_live_sealed(&image, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.receipt_page(0, 16).unwrap(), receipts);
        assert_eq!(recovered.appended_frontier(), end);
        for (sequence, receipt) in receipts.into_iter().enumerate() {
            assert_eq!(
                recovered.read_batch(receipt).unwrap().mutations(),
                sized_batch(&config, sequence as u64, 600 * 1024).mutations()
            );
        }
    }
}
