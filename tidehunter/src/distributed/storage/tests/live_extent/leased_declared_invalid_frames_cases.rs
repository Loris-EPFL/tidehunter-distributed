use super::*;

#[test]
fn leased_declared_invalid_frames_poison_only_the_unvalidated_suffix() {
    for defect in ["middle", "crc", "epoch", "database", "identity", "version"] {
        let (_temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&extent, 0, &frame);
        let mut item = batch(&config, 1);
        if ["epoch", "database", "identity", "version"].contains(&defect) {
            let mut id = item.id();
            let mut version = item.version();
            match defect {
                "epoch" => version.epoch += 1,
                "database" => id.database = [99; 16],
                "identity" => id = batch(&config, 0).id(),
                "version" => version = batch(&config, 0).version(),
                _ => unreachable!(),
            }
            item = CompleteBatch::new(
                &shape(),
                id,
                version,
                item.mutations().to_vec(),
                config.batch_limits,
            )
            .unwrap();
        }
        let (_, mut frame) = encoded(&config, vec![item]);
        if defect == "crc" {
            frame[100] ^= 1;
        }
        write(&extent, 4096, &frame);
        let through = if defect == "middle" { 6144 } else { 8192 };
        assert!(
            matches!(
                log.adopt_live_extent(authority(&config), extent.token, through, false),
                Err(StorageError::IncompleteLiveFrame)
            ),
            "{defect}"
        );
        assert_eq!(log.receipt_count(), 1, "{defect}");
        assert_eq!(log.appended_frontier(), 4096);
        assert!(matches!(
            log.state.lock().poison,
            Some(WriterPoison::IncompleteLiveFrame)
        ));
        assert_eq!(log.persist_appended().unwrap(), 4096);
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Poisoned)
        ));
    }
}

#[test]
fn leased_close_validates_skip_and_suffix_and_releases_tracker() {
    for full in [false, true] {
        let (_temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let count = if full { 4 } else { 1 };
        for n in 0..count {
            let (_, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, n * 4096, &frame);
        }
        let through = count * 4096;
        if !full {
            skip(&extent, through);
        }
        let (closed, profile) = log
            .adopt_live_extent(authority(&config), extent.token, through, true)
            .unwrap();
        assert_eq!(profile.frames, count);
        assert_eq!(profile.close_scan_bytes, extent.end - through);
        assert!(profile.adopt_ns >= profile.close_scan_ns);
        assert!(closed.closed);
        assert_eq!(closed.adopted_end, through);
        assert_eq!(
            log.adopt_live_extent(authority(&config), extent.token, through, true)
                .unwrap()
                .0,
            closed
        );
        let next = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_eq!(next.start, extent.end);
        assert_eq!(next.end, extent.end + config.fragment_bytes);
        let (_, frame) = encoded(&config, vec![batch(&config, 10)]);
        write(&next, next.start, &frame);
        skip(&next, next.start + 4096);
        log.adopt_live_extent(authority(&config), next.token, next.start + 4096, true)
            .unwrap();
        // A third extent enters the next file. This cannot finish if the held
        // external guard did not unblock the native fragment tracker.
        let third = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_eq!(third.start, 2 * config.fragment_bytes);
        assert_eq!(third.segment.file_index, 1);
    }
}

#[test]
fn leased_close_rejects_missing_marker_and_nonzero_suffix() {
    for nonzero in [false, true] {
        let (_temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&extent, 0, &frame);
        if nonzero {
            skip(&extent, 4096);
            write(&extent, 12288, &[1]);
        }
        assert!(matches!(
            log.adopt_live_extent(authority(&config), extent.token, 4096, true),
            Err(StorageError::IncompleteLiveFrame)
        ));
        assert_eq!(log.receipt_count(), 1);
        assert_eq!(log.persist_appended().unwrap(), 4096);
    }
}

#[test]
fn leased_self_adoption_stops_at_torn_frame_and_flush_discovers_complete_frames() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        let frontier = log
            .attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
        let receipt = expected(&config, &group, 0, frame.len())[0];
        let reference = group.batches()[0]
            .record_reference(receipt.address(), 0)
            .unwrap();
        write(&extent, 0, &frame[..100]);
        log.self_adopt_live_extent().unwrap();
        assert_eq!(log.receipt_count(), 0);
        assert!(log.state.lock().poison.is_none());
        write(&extent, 0, &frame);
        if tier == DurabilityTier::Synchronous {
            assert!(matches!(
                log.read_record(&reference),
                Err(StorageError::NeedsPersistence)
            ));
        } else {
            assert_eq!(
                log.read_record(&reference).unwrap(),
                batch(&config, 0).mutations()[0]
            );
        }
        assert_eq!(frontier.appended(), 4096);
        assert_eq!(log.synced_frontier(), 0);
        let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
        write(&extent, 4096, &frame);
        // Flush discovers complete frames which no explicit adoption reported.
        assert_eq!(log.persist_appended().unwrap(), 8192);
        assert_eq!(log.receipt_count(), 2);
        assert_eq!(
            log.read_record(&reference).unwrap(),
            batch(&config, 0).mutations()[0]
        );
    }
}

#[test]
fn leased_mode_excludes_rpc_append() {
    let (_temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (group, _) = encoded(&config, vec![batch(&config, 0)]);
    assert!(matches!(
        log.append_group(authority(&config), &group),
        Err(StorageError::Invalid(_))
    ));
    skip(&extent, 0);
    log.adopt_live_extent(authority(&config), extent.token, 0, true)
        .unwrap();
    assert!(matches!(
        log.append_group(authority(&config), &group),
        Err(StorageError::Invalid(_))
    ));
}

#[test]
fn leased_sealed_recovery_accepts_gap_free_frames_and_stops_at_first_torn_frame() {
    for torn in [None, Some(0u64), Some(1), Some(2)] {
        let (temp, config, log) = fixture();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        for n in 0..3 {
            let (_, mut frame) = encoded(&config, vec![batch(&config, n)]);
            if torn == Some(n) {
                frame[100..].fill(0);
            }
            write(&extent, n * 4096, &frame);
        }
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        let count = torn.unwrap_or(3) as usize;
        assert_eq!(recovered.receipt_count(), count);
        for n in 0..count {
            assert_eq!(
                recovered.receipt_page(n, 1).unwrap()[0].version().sequence,
                n as u64
            );
        }
    }
}

#[test]
fn leased_recovery_crosses_skip_and_retains_fatal_root_corruption() {
    for rooted_defect in [false, true] {
        let (temp, config, log) = fixture();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        for n in 0..2 {
            let (_, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&first, n * 4096, &frame);
        }
        skip(&first, 8192);
        log.adopt_live_extent(authority(&config), first.token, 8192, true)
            .unwrap();
        let second = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 2)]);
        write(&second, second.start, &frame);
        if rooted_defect {
            log.persist_appended().unwrap();
            write(&first, 100, &[99]);
        }
        drop(log);
        let result = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        );
        if rooted_defect {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().receipt_count(), 3);
        }
    }
}

/// Owner continuity on the live path: a scan stopped by a damaged frame reports a
/// hole when a later fragment still starts with a valid frame.
#[test]
fn leased_recovery_reports_a_hole_only_below_a_later_valid_frame() {
    for damaged in [false, true] {
        let (temp, config, log) = fixture();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        for n in 0..2 {
            let (_, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&first, n * 4096, &frame);
        }
        skip(&first, 8192);
        log.adopt_live_extent(authority(&config), first.token, 8192, true)
            .unwrap();
        let second = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 2)]);
        write(&second, second.start, &frame);
        if damaged {
            // Unsynced (no persistence barrier ran), so recovery tolerates it.
            write(&first, 4096 + 100, &[0x5a; 16]);
        }
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.recovery_found_hole(), damaged);
        assert_eq!(recovered.receipt_count(), if damaged { 1 } else { 3 });
    }
}

/// An I/O error while probing a later fragment leaves continuity unknown: recovery
/// fails instead of reporting no hole. The hook replaces the result of each probe
/// read (open, header, body) with EIO, so the production classifier decides; without
/// the fault the same bytes recover with the hole.
#[test]
fn leased_recovery_probe_io_error_fails_instead_of_reading_as_clean() {
    use crate::distributed::storage::wal_replay::{RecoveryRead, inject_read_fault};
    for read in [
        RecoveryRead::ProbeOpen,
        RecoveryRead::ProbeHeader,
        RecoveryRead::ProbeBody,
    ] {
        let (temp, config, probe) = two_fragment_stream(true);
        let path = temp.path().join("stream");
        inject_read_fault(read, probe);
        assert_recovery_fails_with_eio(&path, &config, &format!("{read:?}"));
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert!(recovered.recovery_found_hole(), "{read:?}");
        assert_eq!(recovered.receipt_count(), 1);
    }
}

/// An I/O error on the main scan's header read at a skip marker fails recovery. It
/// must not fall through to a decode that reads the marker as an invalid frame
/// and reports the end of the WAL.
#[test]
fn leased_recovery_scan_header_io_error_at_a_skip_marker_fails() {
    use crate::distributed::storage::wal_replay::{RecoveryRead, inject_read_fault};
    let (temp, config, _) = two_fragment_stream(false);
    let path = temp.path().join("stream");
    inject_read_fault(RecoveryRead::ScanHeader, 8192);
    assert_recovery_fails_with_eio(&path, &config, "ScanHeader at the skip marker");
    let recovered =
        PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
    assert!(!recovered.recovery_found_hole());
    assert_eq!(recovered.receipt_count(), 3);
}

#[test]
fn leased_seal_adopts_and_persists_complete_frames_after_revocation() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.synced_frontier(), 4096);
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
}

#[test]
fn leased_metadata_pressure_finishes_declared_range() {
    let temp = tempdir::TempDir::new("live-lease-pressure").unwrap();
    let mut config = live_config();
    config.max_batches = 2;
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for n in 0..4 {
        let (_, frame) = encoded(&config, vec![batch(&config, n)]);
        write(&extent, n * 4096, &frame);
    }
    let synced_before = log.synced_frontier();
    let adopted = log
        .adopt_live_extent(authority(&config), extent.token, extent.end, false)
        .unwrap()
        .0;
    assert_eq!(adopted.receipt_count, 4);
    assert_eq!(adopted.adopted_end, extent.end);
    // OnDisk history persisted a pressure spill (synced ≥ 8192); WAL-replay
    // history (the only mode) adopts the whole range without a spill.
    assert_eq!(log.synced_frontier(), synced_before);
    assert_eq!(log.receipt_count(), 4);
}

#[test]
fn leased_namespace_and_witness_are_durable_before_export_authority() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    // Inspection is the power-loss ordering oracle: a process crash alone
    // cannot simulate loss of an unsynced directory entry or file witness.
    assert_eq!(
        log.state.lock().namespace_synced_file.unwrap().as_u64(),
        extent.segment.file_index
    );
    assert_eq!(log.stats().namespace_syncs, 1);
    assert_eq!(log.stats().inventory_witnesses, 1);
    assert_eq!(log.synced_frontier(), 0);
    assert_eq!(log.receipt_count(), 0);
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    let file = OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap();
    file.write_all_at(&frame, extent.start % extent.segment.bytes)
        .unwrap();
    file.sync_data().unwrap();
    drop(file);
    drop(log);
    let path = temp.path().join("stream");
    let recovered =
        PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
    assert_eq!(recovered.receipt_count(), 1);
    assert!(matches!(
        recovered.lease_live_extent(authority(&config)),
        Err(StorageError::Sealed)
    ));
    assert!(PartitionLog::create_live(&path, config, &shape(), metrics()).is_err());
}

#[test]
fn leased_namespace_or_witness_failure_never_grants_an_extent() {
    for witness in [false, true] {
        let (_temp, config, log) = fixture();
        if witness {
            log.fail_next_inventory_witness(false);
        } else {
            log.fail_next_namespace_persist();
        }
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Unknown(_))
        ));
        assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
        assert_eq!(log.receipt_count(), 0);
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Poisoned)
        ));
    }
}

#[test]
fn leased_recovery_refuses_uncertain_revocation_before_opening_wal() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, extent.start, &frame);
    drop(log);
    let result =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Err(StorageError::Invalid("uncertain target drain"))
        });
    assert!(matches!(
        result,
        Err(StorageError::Invalid("uncertain target drain"))
    ));
    // A failed revocation did not adopt or seal any bytes.
    assert!(!temp.path().join("stream").join(SEAL).exists());
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_count(), 1);
}

#[test]
fn leased_namespace_persistence_exposes_no_transient_poison_and_preserves_terminal_poison() {
    for terminal in [false, true] {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        let (entered, resume) = log.pause_next_persist();
        let worker = log.clone();
        let lease = std::thread::spawn(move || worker.lease_live_extent(authority(&config)));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        // The syncer validates state before acquiring persistence ownership.
        // A temporarily marked Fatal here would permanently fail its frontier.
        assert!(log.state.lock().poison.is_none());
        assert_eq!(log.persist_appended().unwrap(), 0);
        if terminal {
            log.poison_writer();
        }
        resume.send(()).unwrap();
        let result = lease.join().unwrap();
        if terminal {
            assert!(matches!(result, Err(StorageError::Poisoned)));
            assert!(matches!(log.state.lock().poison, Some(WriterPoison::Fatal)));
        } else {
            assert!(result.is_ok());
            assert!(log.state.lock().poison.is_none());
        }
    }
}

#[test]
fn leased_large_fragment_skip_does_not_charge_an_unadoptable_gap_as_metadata_pressure() {
    {
        let temp = tempdir::TempDir::new("live-lease-large-skip").unwrap();
        let mut config = live_config();
        config.fragment_bytes = 128 * 1024 * 1024;
        config.wal_file_bytes = 2 * config.fragment_bytes;
        let path = temp.path().join("stream");
        let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&first, first.start, &frame);
        skip(&first, first.start + 4096);
        log.adopt_live_extent(authority(&config), first.token, first.start + 4096, false)
            .unwrap();
        log.adopt_live_extent(authority(&config), first.token, first.start + 4096, true)
            .unwrap();
        let next = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_eq!(next.start, first.end);
        assert_eq!(next.start, 128 * 1024 * 1024);
        for n in 1..=2 {
            let (_, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&next, next.start + (n - 1) * 4096, &frame);
            let adoption = log
                .adopt_live_extent(authority(&config), next.token, next.start + n * 4096, false)
                .unwrap()
                .0;
            assert_eq!(adoption.adopted_end, next.start + n * 4096);
            assert_eq!(adoption.receipt_count, n);
            assert!(log.state.lock().poison.is_none());
        }
        // Once the bridging frame exists, subsequent pressure can root it.
        assert_eq!(log.receipt_count(), 3);
        log.persist_appended().unwrap();
        drop(log);
        let recovered =
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(recovered.receipt_count(), 3);
    }
}

#[test]
fn leased_header_probe_changes_never_publish_a_different_tail_crc() {
    for declared in [false, true] {
        let (temp, config, log) = fixture();
        let log = Arc::new(log);
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        let mut unfinished = frame.clone();
        unfinished[4] ^= 1;
        write(&extent, 0, &unfinished);
        let (entered, resume) = log.pause_live(LivePausePoint::FrameProbe);
        let worker = log.clone();
        let token = extent.token;
        let auth = authority(&config);
        let scan = std::thread::spawn(move || {
            if declared {
                worker
                    .adopt_live_extent(auth, token, 4096, false)
                    .map(|_| ())
            } else {
                worker.self_adopt_live_extent()
            }
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        write(&extent, 0, &frame);
        resume.send(()).unwrap();
        let result = scan.join().unwrap();
        assert_eq!(log.receipt_count(), 0);
        assert_eq!(log.appended_frontier(), 0);
        if declared {
            assert!(matches!(result, Err(StorageError::IncompleteLiveFrame)));
        } else {
            result.unwrap();
            assert!(log.state.lock().poison.is_none());
            log.self_adopt_live_extent().unwrap();
            log.persist_appended().unwrap();
            let tail = log.state.lock().live_tail.unwrap();
            assert_eq!(
                tail.crc,
                u32::from_be_bytes(frame[4..8].try_into().unwrap())
            );
        }
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.receipt_count(), 1);
    }
}

#[test]
fn leased_stale_opportunistic_scan_tolerates_close_and_two_rollovers() {
    for rollovers in 0..=2 {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        let frontier = log.attach_frontier(&DurabilityConfig::default()).unwrap();
        let first = log.lease_live_extent(authority(&config)).unwrap().0;
        let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
        write(&first, 0, &frame);
        log.adopt_live_extent(authority(&config), first.token, 4096, false)
            .unwrap();
        skip(&first, 4096);
        let (entered, resume) = log.pause_live(LivePausePoint::ScanSnapshot);
        let worker = log.clone();
        let scan = std::thread::spawn(move || worker.sync_for_frontier());
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        log.adopt_live_extent(authority(&config), first.token, 4096, true)
            .unwrap();
        for _ in 0..rollovers {
            let next = log.lease_live_extent(authority(&config)).unwrap().0;
            skip(&next, next.start);
            log.adopt_live_extent(authority(&config), next.token, next.start, true)
                .unwrap();
        }
        if rollovers != 0 {
            log.lease_live_extent(authority(&config)).unwrap();
        }
        resume.send(()).unwrap();
        scan.join().unwrap().unwrap();
        assert!(frontier.request_sync().is_ok());
        assert!(log.state.lock().poison.is_none());
        assert_eq!(log.synced_frontier(), 4096);
    }
}

#[test]
fn leased_whole_batch_reads_keep_the_synchronous_owner_barrier() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
        let receipt = expected(&config, &group, 0, frame.len())[0];
        write(&extent, 0, &frame);
        if tier == DurabilityTier::Synchronous {
            assert!(matches!(
                log.read_batch(receipt),
                Err(StorageError::NeedsPersistence)
            ));
            assert_eq!(log.synced_frontier(), 0);
            assert_eq!(log.receipt_count(), 1);
            assert!(matches!(
                log.read_batch(receipt),
                Err(StorageError::NeedsPersistence)
            ));
        } else {
            assert_eq!(log.read_batch(receipt).unwrap().id(), receipt.id());
        }
        log.persist_appended().unwrap();
        assert_eq!(log.read_batch(receipt).unwrap().id(), receipt.id());
    }
}

#[test]
fn leased_large_close_releases_state_and_excludes_other_extent_mutations() {
    let temp = tempdir::TempDir::new("live-close-unlocked").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 128 * 1024 * 1024;
    config.wal_file_bytes = 2 * config.fragment_bytes;
    let log = Arc::new(
        PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    skip(&extent, 4096);
    log.adopt_live_extent(authority(&config), extent.token, 4096, false)
        .unwrap();
    let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let worker = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let close = std::thread::spawn(move || worker.adopt_live_extent(auth, token, 4096, true));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Capacity one; the stats reader owns the send and closes it on exit.
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let reader = log.clone();
    let stats = std::thread::spawn(move || sent.send(reader.stats().appended_groups).unwrap());
    assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    stats.join().unwrap();
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.adopt_live_extent(authority(&config), token, 8192, false),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.adopt_live_extent(authority(&config), token, 4096, true),
        Err(StorageError::Invalid(_))
    ));
    log.self_adopt_live_extent().unwrap();
    assert!(log.state.lock().poison.is_none());
    resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().0.closed);
    assert_eq!(
        log.lease_live_extent(authority(&config)).unwrap().0.start,
        extent.end
    );
}

#[test]
fn leased_close_rejects_a_valid_unadopted_frame_at_the_marker_cursor() {
    let (_temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for n in 0..2 {
        let (_, frame) = encoded(&config, vec![batch(&config, n)]);
        write(&extent, n * 4096, &frame);
    }
    assert!(matches!(
        log.adopt_live_extent(authority(&config), extent.token, 4096, true),
        Err(StorageError::IncompleteLiveFrame)
    ));
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.persist_appended().unwrap(), 4096);
}

#[test]
fn leased_crash_after_reference_publication_recovers_the_same_complete_prefix() {
    let (temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (group, frame) = encoded(&config, vec![batch(&config, 0)]);
    let receipts = expected(&config, &group, 0, frame.len());
    write(&extent, 0, &frame);
    // Release publishes the one-shot crash injection to the adopting thread;
    // its AcqRel swap consumes this flag once before frontier publication.
    log.fail_after_live_publish.store(true, Ordering::Release);
    assert!(matches!(
        log.adopt_live_extent(authority(&config), extent.token, 4096, false),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.appended_frontier(), 0);
    drop(log);
    let recovered =
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || {
            Ok(())
        })
        .unwrap();
    assert_eq!(recovered.receipt_page(0, 1).unwrap(), receipts);
}

#[test]
fn leased_concurrent_reader_and_control_match_independent_canonical_receipt_digest() {
    use bincode::Options;
    use blake2::{Blake2b, Digest, digest::consts::U32};
    for _ in 0..8 {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut receipts = Vec::new();
        let mut reference = None;
        for n in 0..3 {
            let (group, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, n * 4096, &frame);
            let receipt = expected(&config, &group, n * 4096, frame.len())[0];
            reference = Some(
                group.batches()[0]
                    .record_reference(receipt.address(), 0)
                    .unwrap(),
            );
            receipts.push(receipt);
        }
        let mut independent = [0; 32];
        for receipt in &receipts {
            // Independent serializer, not the production digest field arithmetic.
            let encoding = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_little_endian()
                .serialize(receipt)
                .unwrap();
            let mut hash = Blake2b::<U32>::new();
            hash.update(b"tidehunter-live-extent-receipt-v1");
            hash.update(independent);
            hash.update(encoding);
            independent = hash.finalize().into();
        }
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let read = scope.spawn(|| {
                barrier.wait();
                log.read_record(&reference.unwrap())
            });
            let adopt = scope.spawn(|| {
                barrier.wait();
                log.adopt_live_extent(authority(&config), extent.token, 12288, false)
            });
            barrier.wait();
            read.join().unwrap().unwrap();
            let adoption = adopt.join().unwrap().unwrap().0;
            assert_eq!(adoption.receipt_count, 3);
            assert_eq!(adoption.receipt_digest, independent);
        });
        assert_eq!(log.receipt_count(), 3);
        assert_eq!(log.receipt_page(0, 3).unwrap(), receipts);
    }
}

#[test]
fn leased_seal_and_flush_scan_past_a_concurrent_adopter() {
    for operation in 0..2 {
        let (temp, config, log) = fixture();
        let log = Arc::new(log);
        log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
            .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut reference = None;
        for n in 0..2 {
            let (group, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, n * 4096, &frame);
            let receipt = expected(&config, &group, n * 4096, frame.len())[0];
            reference = Some(
                group.batches()[0]
                    .record_reference(receipt.address(), 0)
                    .unwrap(),
            );
        }
        let (snapshot, resume_snapshot) = log.pause_live(LivePausePoint::ScanSnapshot);
        let target = log.clone();
        let auth = authority(&config);
        let complete = std::thread::spawn(move || match operation {
            0 => target.seal_live_epoch(auth, || Ok(())),
            _ => target.persist_appended().map(|_| ()),
        });
        snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
        let (between, resume_reader) = log.pause_live(LivePausePoint::AfterFrame);
        let reader = log.clone();
        let read = std::thread::spawn(move || reader.read_record(&reference.unwrap()));
        between.recv_timeout(Duration::from_secs(5)).unwrap();
        // The hook runs after the completed round releases its gate.
        assert_eq!(log.appended_frontier(), 8192);
        resume_snapshot.send(()).unwrap();
        let result = complete.join().unwrap();
        result.unwrap();
        assert_eq!(log.receipt_count(), 2);
        assert_eq!(log.appended_frontier(), 8192);
        assert_eq!(log.synced_frontier(), 8192);
        resume_reader.send(()).unwrap();
        read.join().unwrap().unwrap();
        drop(log);
        let recovered = PartitionLog::recover_live_sealed(
            &temp.path().join("stream"),
            &config,
            metrics(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(recovered.receipt_count(), 2);
    }
}

#[test]
fn leased_seal_refuses_an_unlocked_close_without_poisoning_it() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    skip(&extent, 4096);
    let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
    let closer = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let close = std::thread::spawn(move || closer.adopt_live_extent(auth, token, 4096, true));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        log.seal_live_epoch(authority(&config), || {
            panic!("revocation must not run while close owns the extent")
        }),
        Err(StorageError::Invalid("live extent is closing"))
    ));
    assert!(log.state.lock().poison.is_none());
    assert!(!log.state.lock().is_sealing());
    resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().0.closed);
    log.persist_appended().unwrap();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
}
