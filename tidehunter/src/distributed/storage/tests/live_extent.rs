//! Real final-inode leased frames: prefix authority, close, and crash replay.
use super::*;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use crate::distributed::nvmeof::live_storage::{
    LiveGroupDescriptor, LiveWriteExtent, extend_live_receipt_digest,
};

fn fixture() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let temp = tempdir::TempDir::new("live-leased-extent").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    (temp, config, log)
}

fn encoded(config: &PartitionConfig, items: Vec<CompleteBatch>) -> (CompleteGroup, Vec<u8>) {
    let group =
        CompleteGroup::new_live(&shape(), items, config.batch_limits, config.group_limits).unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    (group, frame)
}

fn write(extent: &LiveWriteExtent, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap()
        .write_all_at(bytes, offset % extent.segment.bytes)
        .unwrap();
}

fn skip(extent: &LiveWriteExtent, offset: u64) {
    let mut block = [0; 4096];
    block[..8].copy_from_slice(CrcFrame::skip_marker().as_ref());
    write(extent, offset, &block);
}

fn expected(
    config: &PartitionConfig,
    group: &CompleteGroup,
    offset: u64,
    len: usize,
) -> Vec<AppendedBatch> {
    group
        .batches()
        .iter()
        .zip(group.batch_offsets())
        .map(|(batch, member)| {
            receipt_for(
                config,
                batch,
                member_position(WalPosition::new(offset, len as u32), *member, batch),
            )
        })
        .collect()
}

#[test]
fn leased_two_step_adoption_matches_reserved_receipts_and_frontier_rules() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let (_temp, config, log) = fixture();
        log.attach_frontier(&DurabilityConfig::with_tier(tier))
            .unwrap();
        let (extent, lease_profile) = log.lease_live_extent(authority(&config)).unwrap();
        assert!(lease_profile.reserve_ns > 0);
        assert!(lease_profile.namespace_sync_ns > 0);
        assert_eq!(lease_profile.publish_ns, 0);
        assert!(matches!(
            log.lease_live_extent(authority(&config)),
            Err(StorageError::Invalid("live extent already open"))
        ));
        assert!(log.state.lock().poison.is_none());
        let reserved_temp = tempdir::TempDir::new("live-reserved-parity").unwrap();
        let reserved = PartitionLog::create_live(
            &reserved_temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap();
        let mut receipts = Vec::new();
        for n in 0..3 {
            let (group, frame) = encoded(&config, vec![batch(&config, n)]);
            write(&extent, extent.start + n * 4096, &frame);
            let actual = append_live_fixture(&reserved, &config, n);
            assert_eq!(
                expected(&config, &group, extent.start + n * 4096, frame.len()),
                vec![actual]
            );
            receipts.push(actual);
        }
        let (first, profile) = log
            .adopt_live_extent(authority(&config), extent.token, 4096, false)
            .unwrap();
        assert_eq!(profile.frames, 1);
        assert!(profile.adopt_ns > 0);
        assert_eq!((profile.close_scan_ns, profile.close_scan_bytes), (0, 0));
        assert_eq!(first.receipt_count, 1);
        assert_eq!(
            first.receipt_digest,
            extend_live_receipt_digest([0; 32], receipts[0])
        );
        let (second, profile) = log
            .adopt_live_extent(authority(&config), extent.token, 12288, false)
            .unwrap();
        assert_eq!(profile.frames, 2);
        let digest = receipts.iter().fold([0; 32], |digest, receipt| {
            extend_live_receipt_digest(digest, *receipt)
        });
        assert_eq!(second.receipt_count, 3);
        assert_eq!(second.receipt_digest, digest);
        assert_eq!(log.receipt_page(0, 3).unwrap(), receipts);
        assert_eq!(
            log.adopt_live_extent(authority(&config), extent.token, 12288, false)
                .unwrap()
                .0,
            second
        );
        assert_eq!(
            log.adopt_live_extent(authority(&config), extent.token, 4096, false)
                .unwrap()
                .0,
            second
        );
        for receipt in &receipts {
            let item = batch(&config, receipt.version().sequence);
            let reference = item.record_reference(receipt.address(), 0).unwrap();
            if tier == DurabilityTier::Synchronous {
                assert!(matches!(
                    log.read_record(&reference),
                    Err(StorageError::NeedsPersistence)
                ));
            } else {
                assert_eq!(log.read_record(&reference).unwrap(), item.mutations()[0]);
            }
        }
        log.persist_appended().unwrap();
        for receipt in receipts {
            let item = batch(&config, receipt.version().sequence);
            let reference = item.record_reference(receipt.address(), 0).unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), item.mutations()[0]);
        }
    }
}

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
fn leased_self_adoption_stops_at_torn_frame_and_keeps_sync_checkpoint_barrier() {
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
        assert!(matches!(
            log.checkpoint_prefix(1),
            Err(StorageError::NeedsPersistence)
        ));
        let (_, frame) = encoded(&config, vec![batch(&config, 1)]);
        write(&extent, 4096, &frame);
        // Flush discovers complete frames which no explicit adoption reported.
        assert_eq!(log.persist_appended().unwrap(), 8192);
        assert_eq!(log.receipt_count(), 2);
        log.checkpoint_prefix(2).unwrap();
        assert_eq!(
            log.read_record(&reference).unwrap(),
            batch(&config, 0).mutations()[0]
        );
    }
}

#[test]
fn leased_mode_excludes_reservations_and_rpc_append() {
    let (_temp, config, log) = fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (group, _) = encoded(&config, vec![batch(&config, 0)]);
    let (descriptor, _) = LiveGroupDescriptor::from_group(&group).unwrap();
    assert!(matches!(
        log.reserve_live_group(authority(&config), descriptor.clone()),
        Err(StorageError::Invalid(_))
    ));
    assert!(matches!(
        log.append_group(authority(&config), &group),
        Err(StorageError::Invalid(_))
    ));
    skip(&extent, 0);
    log.adopt_live_extent(authority(&config), extent.token, 0, true)
        .unwrap();
    log.reserve_live_group(authority(&config), descriptor)
        .unwrap();
    assert!(matches!(
        log.lease_live_extent(authority(&config)),
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
fn leased_metadata_pressure_persists_spills_and_finishes_declared_range() {
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
    log.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    for n in 0..4 {
        let (_, frame) = encoded(&config, vec![batch(&config, n)]);
        write(&extent, n * 4096, &frame);
    }
    let adopted = log
        .adopt_live_extent(authority(&config), extent.token, extent.end, false)
        .unwrap()
        .0;
    assert_eq!(adopted.receipt_count, 4);
    assert_eq!(adopted.adopted_end, extent.end);
    assert!(log.synced_frontier() >= 8192);
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
    for rooted in [false, true] {
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
        if rooted {
            log.persist_appended().unwrap();
            log.state
                .lock()
                .references
                .start_background(4096, 1, &path)
                .unwrap();
            log.wait_history().unwrap();
            assert_eq!(log.references.root().end, 4096);
        }
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
fn leased_seal_flush_and_checkpoint_scan_past_a_concurrent_adopter() {
    for operation in 0..3 {
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
            1 => target.persist_appended().map(|_| ()),
            _ => target.checkpoint_prefix(2).map(|_| ()),
        });
        snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
        let (between, resume_reader) = log.pause_live(LivePausePoint::AfterFrame);
        let reader = log.clone();
        let read = std::thread::spawn(move || reader.read_record(&reference.unwrap()));
        between.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(log.appended_frontier(), 4096);
        resume_snapshot.send(()).unwrap();
        let result = complete.join().unwrap();
        if operation == 2 {
            assert!(matches!(result, Err(StorageError::NeedsPersistence)));
        } else {
            result.unwrap();
        }
        assert_eq!(log.receipt_count(), 2);
        assert_eq!(log.appended_frontier(), 8192);
        if operation == 2 {
            log.persist_appended().unwrap();
        }
        assert_eq!(log.synced_frontier(), 8192);
        log.checkpoint_prefix(2).unwrap();
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
    assert!(!log.state.lock().sealing);
    resume.send(()).unwrap();
    assert!(close.join().unwrap().unwrap().0.closed);
    log.persist_appended().unwrap();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
}

#[test]
fn leased_pressure_rechecks_capacity_after_an_independent_history_drain() {
    let temp = tempdir::TempDir::new("live-lease-history-race").unwrap();
    let mut config = live_config();
    config.max_batches = 1;
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = Arc::new(
        PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    log.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, first) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &first);
    log.adopt_live_extent(authority(&config), extent.token, 4096, false)
        .unwrap();
    let (history_entered, history_resume) = log.references.pause_next_history();
    log.persist_appended().unwrap();
    history_entered
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let (background, _) = log.state.lock().references.background_handle();
    let (_, second) = encoded(&config, vec![batch(&config, 1)]);
    write(&extent, 4096, &second);
    let (pressure, pressure_resume) = log.pause_live(LivePausePoint::PressureObserved);
    let adopter = log.clone();
    let auth = authority(&config);
    let token = extent.token;
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 8192, false));
    pressure.recv_timeout(Duration::from_secs(5)).unwrap();
    history_resume.send(()).unwrap();
    background.lock().take().unwrap().join().unwrap();
    assert_eq!(log.references.root().end, 4096);
    pressure_resume.send(()).unwrap();
    let reply = adopt.join().unwrap().unwrap().0;
    assert_eq!(reply.receipt_count, 2);
    assert_eq!(reply.adopted_end, 8192);
    assert!(log.state.lock().poison.is_none());
}

fn split_block_fixture() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let temp = tempdir::TempDir::new("live-leased-split-block").unwrap();
    let mut config = live_config();
    config.batch_limits.max_encoded_bytes = 32 * 1024;
    config.group_limits.max_encoded_bytes = 64 * 1024 - 8;
    config.fragment_bytes = 64 * 1024;
    config.wal_file_bytes = 128 * 1024;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    (temp, config, log)
}

fn sized_batch(config: &PartitionConfig, sequence: u64, size: usize) -> CompleteBatch {
    CompleteBatch::new(
        &shape(),
        id_for(config, sequence),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence,
        },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: vec![sequence as u8; size].into(),
        }],
        config.batch_limits,
    )
    .unwrap()
}

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
        assert_eq!(
            log.state
                .lock()
                .open_live_extent
                .as_ref()
                .unwrap()
                .buffer
                .len(),
            high_water,
        );
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
    for mode in [HistoryPersistence::OnDisk, HistoryPersistence::WalReplay] {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let (_temp, config, log) = fixture();
            log.set_history_persistence(mode).unwrap();
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
                assert!(matches!(
                    log.resolve_live_record(&reference),
                    Err(StorageError::NeedsPersistence)
                ));
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
