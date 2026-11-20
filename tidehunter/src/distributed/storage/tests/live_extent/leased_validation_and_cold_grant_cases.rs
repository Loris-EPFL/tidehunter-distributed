use super::*;

#[test]
fn leased_round_byte_cap_and_oversized_frame_bound_validation_work() {
    for (sizes, rounds, parallel_rounds) in [
        ([3 * 1024 * 1024, 3 * 1024 * 1024], 1, 1),
        ([6 * 1024 * 1024, 6 * 1024 * 1024], 2, 0),
        ([10 * 1024 * 1024, 600 * 1024], 2, 0),
    ] {
        let temp = tempdir::TempDir::new("live-round-byte-bound").unwrap();
        let mut config = live_config();
        config.batch_limits.max_encoded_bytes = 12 * 1024 * 1024;
        config.group_limits.max_encoded_bytes = 16 * 1024 * 1024 - 8;
        config.fragment_bytes = 32 * 1024 * 1024;
        config.wal_file_bytes = 2 * config.fragment_bytes;
        let log = PartitionLog::create_live(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap();
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let mut end = 0;
        for (sequence, size) in sizes.into_iter().enumerate() {
            let (_, frame) = encoded(&config, vec![sized_batch(&config, sequence as u64, size)]);
            write(&extent, end, &frame);
            end += frame.len() as u64;
        }
        let adoption = log
            .adopt_live_extent(authority(&config), extent.token, end, false)
            .unwrap()
            .0;
        assert_eq!(adoption.receipt_count, 2);
        assert_eq!(adoption.adopted_end, end);
        let diagnostics = log.live_test_diagnostics().adoption;
        assert_eq!(diagnostics.frames, 2);
        assert_eq!(diagnostics.rounds, rounds);
        assert_eq!(diagnostics.parallel_rounds, parallel_rounds);
    }
}

#[test]
fn leased_validation_keeps_abandoned_reservation_recovery_exclusive() {
    let (temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (_, frame) = encoded(&config, vec![batch(&config, 0)]);
    write(&extent, 0, &frame);
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, 4096, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        PartitionLog::recover_live_sealed(&temp.path().join("stream"), &config, metrics(), || Ok(
            ()
        ),)
        .is_err()
    );
    assert_eq!(log.receipt_count(), 0);
    resume.send(()).unwrap();
    assert_eq!(adopt.join().unwrap().unwrap().0.receipt_count, 1);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_read_waits_for_validated_prefix_publication() {
    let (_temp, config, log) = parallel_adoption_fixture();
    log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let mut end = 0;
    let mut reference = None;
    for sequence in 0..2 {
        let (group, frame) = encoded(&config, vec![sized_batch(&config, sequence, 600 * 1024)]);
        let receipt = expected(&config, &group, end, frame.len())[0];
        reference = Some(
            group.batches()[0]
                .record_reference(receipt.address(), 0)
                .unwrap(),
        );
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    let (entered, resume) = log.pause_live(LivePausePoint::ValidatedBeforePublish);
    let adopter = log.clone();
    let token = extent.token;
    let auth = authority(&config);
    let adopt = std::thread::spawn(move || adopter.adopt_live_extent(auth, token, end, false));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Each snapshot confirms its caller reached self-adoption before publish.
    let (read_snapshot, resume_read) = log.pause_live(LivePausePoint::ScanSnapshot);
    let reader = log.clone();
    let read = std::thread::spawn(move || reader.read_record(&reference.unwrap()));
    read_snapshot.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_read.send(()).unwrap();
    resume.send(()).unwrap();
    assert_eq!(adopt.join().unwrap().unwrap().0.receipt_count, 2);
    assert_eq!(
        read.join().unwrap().unwrap(),
        sized_batch(&config, 1, 600 * 1024).mutations()[0]
    );
    assert_eq!(log.receipt_count(), 2);
    assert_eq!(log.appended_frontier(), end);
    log.persist_appended().unwrap();
}

#[test]
fn leased_seal_self_adoption_adopts_and_syncs_every_frame() {
    let temp = tempdir::TempDir::new("live-seal-adoption-pressure").unwrap();
    let mut config = live_config();
    config.max_batches = 1;
    config.fragment_bytes = 16 * 1024;
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
    for sequence in 0..4 {
        write(
            &extent,
            sequence * 4096,
            &encoded(&config, vec![batch(&config, sequence)]).1,
        );
    }
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    assert_eq!(log.receipt_count(), 4);
    assert_eq!(log.appended_frontier(), extent.end);
    assert_eq!(log.synced_frontier(), extent.end);
    assert!(log.state.lock().sealed);
    assert!(log.state.lock().poison.is_none());
}

#[test]
fn leased_many_small_frames_respect_the_round_frame_budget() {
    let (_temp, config, log) = parallel_adoption_fixture();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let count = ADOPTION_ROUND_FRAMES + 8;
    let mut end = 0;
    let mut receipts = Vec::new();
    for sequence in 0..count {
        let (group, frame) = encoded(
            &config,
            vec![sized_batch(&config, sequence as u64, 64 * 1024)],
        );
        receipts.extend(expected(&config, &group, end, frame.len()));
        write(&extent, end, &frame);
        end += frame.len() as u64;
    }
    assert!(end < ADOPTION_ROUND_BYTES);
    let adoption = log
        .adopt_live_extent(authority(&config), extent.token, end, false)
        .unwrap()
        .0;
    assert_eq!(adoption.adopted_end, end);
    assert_eq!(adoption.receipt_count, count as u64);
    assert_eq!(
        adoption.receipt_digest,
        receipts
            .iter()
            .copied()
            .fold([0; 32], extend_live_receipt_digest)
    );
    assert_eq!(log.receipt_page(0, count).unwrap(), receipts);
    let diagnostics = log.live_test_diagnostics().adoption;
    assert_eq!(diagnostics.frames, count as u64);
    assert_eq!(
        diagnostics.rounds,
        count.div_ceil(ADOPTION_ROUND_FRAMES) as u64
    );
    assert_eq!(diagnostics.parallel_rounds, 2);
}

#[test]
fn prewritten_live_files_are_ready_before_inline_and_background_grants() {
    let temp = tempdir::TempDir::new("live-prewritten-files").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = 32 * 1024;
    let log = PartitionLog::create_live_with_prewrite(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
        true,
    )
    .unwrap();
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let file = File::open(&first.segment.path).unwrap();
    let mut bytes = vec![1; config.wal_file_bytes as usize];
    file.read_exact_at(&mut bytes, 0).unwrap();
    assert!(bytes.iter().all(|byte| *byte == 0));
    close_for_preparation(&log, &config, &first, 0);
    let mut written = vec![0; config.wal_file_bytes as usize];
    file.read_exact_at(&mut written, 0).unwrap();
    assert!(written.iter().any(|byte| *byte != 0));
    let second = log.lease_live_extent(authority(&config)).unwrap().0;
    assert_eq!(second.segment.file_index, 0);
    file.read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(
        bytes, written,
        "a later grant must not zero an earlier write"
    );
    assert_eq!(log.prepare_next_live_file().unwrap(), Some(1));
    let prepared = log.live_segment(1).unwrap();
    File::open(&prepared.path)
        .unwrap()
        .read_exact_at(&mut bytes, 0)
        .unwrap();
    assert!(bytes.iter().all(|byte| *byte == 0));
    let stats = log.live_file_preparation_stats();
    assert!(stats.live_file_prewrite_bytes >= 2 * config.wal_file_bytes);
    assert!(stats.live_file_prewrite_ns > 0);
}

#[test]
fn default_live_preparation_does_not_prewrite() {
    let (_temp, config, log) = fixture();
    log.lease_live_extent(authority(&config)).unwrap();
    log.prepare_next_live_file().unwrap();
    let stats = log.live_file_preparation_stats();
    assert_eq!(stats.live_file_prewrite_bytes, 0);
    assert_eq!(stats.live_file_prewrite_ns, 0);
}

#[test]
fn cold_grant_bypasses_close_and_metadata_gate_without_promoting_a_prefix() {
    for next_file in [false, true] {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let (temp, config, log) = fixture();
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let log = Arc::new(log);
            if next_file {
                let initial = log.lease_live_extent(authority(&config)).unwrap().0;
                close_for_preparation(&log, &config, &initial, 0);
            }
            let current = log.lease_live_extent(authority(&config)).unwrap().0;
            let next = log
                .prelease_live_extent(authority(&config), current.token)
                .unwrap()
                .0;
            assert_eq!(
                current.segment.file_index != next.segment.file_index,
                next_file
            );
            let (_, frame) = cold_record(&config, &current, 1);
            skip(&current, current.start + frame.len() as u64);
            let (reference, _) = cold_record(&config, &next, 2);
            let (entered, resume) = log.pause_live(LivePausePoint::CloseSuffix);
            let closer = log.clone();
            let auth = authority(&config);
            let close = std::thread::spawn(move || {
                closer.adopt_live_extent(
                    auth,
                    current.token,
                    current.start + frame.len() as u64,
                    true,
                )
            });
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let before = cold_frontiers(&log);
            assert!(
                before.1 < reference.batch_address.offset + u64::from(reference.batch_address.len)
            );
            // Holding both unrelated gates proves the cold read does not join
            // adoption or durable metadata installation, even for Synchronous.
            let round = log
                .state
                .lock()
                .open_live_extent
                .as_ref()
                .unwrap()
                .round
                .clone();
            let round_guard = round.lock();
            let persistence = log.persistence.lock();
            let lease = log.resolve_live_record(&reference).unwrap();
            assert_eq!(cold_frontiers(&log), before);
            assert!(lease._plan.authority_bytes() > 0);
            log.evict_authority_cache();
            let mut member = vec![0; lease.len as usize];
            lease
                ._plan
                .member
                .file
                .read_exact_at(&mut member, lease.offset)
                .unwrap();
            assert_eq!(
                CompleteBatch::decode_record(&shape(), &reference, member.into()).unwrap(),
                batch(&config, 2).mutations()[0],
            );
            assert_eq!(log.live_read_pins.load(Ordering::Acquire), 1);
            drop(lease);
            assert_eq!(log.live_read_pins.load(Ordering::Acquire), 0);
            drop(persistence);
            drop(round_guard);
            // Process-crash image while predecessor close is unfinished.
            // Native does not claim an owner-power-loss guarantee.
            let image = temp.path().join("cold-crash-image");
            fs::create_dir(&image).unwrap();
            for entry in fs::read_dir(temp.path().join("stream")).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    fs::copy(entry.path(), image.join(entry.file_name())).unwrap();
                }
            }
            resume.send(()).unwrap();
            close.join().unwrap().unwrap();
            let recovered =
                PartitionLog::recover_live_sealed(&image, &config, metrics(), || Ok(())).unwrap();
            assert_eq!(recovered.receipt_count(), if next_file { 3 } else { 2 });
            assert_eq!(
                recovered.read_record(&reference).unwrap(),
                batch(&config, 2).mutations()[0]
            );
        }
    }
}

#[test]
fn cold_grant_rejects_foreign_exact_membership_and_torn_native_frames() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (reference, frame) = cold_record(&config, &extent, 0);
    let mut invalid = Vec::new();
    let mut changed = reference;
    changed.batch_digest.0[0] ^= 1;
    let mut member = vec![0; reference.descriptor.len as usize];
    File::open(&extent.segment.path)
        .unwrap()
        .read_exact_at(
            &mut member,
            reference.batch_address.offset + 8 + u64::from(reference.descriptor.offset),
        )
        .unwrap();
    // Scalar checksums do not authenticate the complete-batch digest.
    assert!(CompleteBatch::decode_record(&shape(), &changed, member.into()).is_ok());
    invalid.push(changed);
    let mut changed = reference;
    changed.batch_id.operation[0] ^= 1;
    invalid.push(changed);
    let mut changed = reference;
    changed.batch_version.sequence += 1;
    invalid.push(changed);
    let mut changed = reference;
    changed.batch_version.epoch += 1;
    invalid.push(changed);
    let mut changed = reference;
    changed.descriptor.offset += 1;
    invalid.push(changed);
    let mut changed = reference;
    changed.batch_address.segment_generation += 1;
    invalid.push(changed);
    let mut changed = reference;
    changed.batch_address.offset = extent.end;
    invalid.push(changed);
    let mut changed = reference;
    changed.batch_address.len = u32::MAX;
    invalid.push(changed);
    for changed in invalid {
        assert!(log.resolve_live_record(&changed).is_err());
        assert_eq!(log.live_read_pins.load(Ordering::Acquire), 0);
    }
    // Preserve the scalar bytes/checksum but tear a different part of the full
    // native batch: self-consistent scalar verification is not membership.
    let scalar_start = reference.batch_address.offset + 8 + u64::from(reference.descriptor.offset);
    assert!(scalar_start > reference.batch_address.offset + 8);
    write(&extent, reference.batch_address.offset + 8, &[0xff]);
    log.evict_authority_cache();
    assert!(log.resolve_live_record(&reference).is_err());
    write(&extent, extent.start, &frame);
    // Tear the inner native frame, not the outer live group's zero padding.
    let half = reference.batch_address.len / 2;
    write(
        &extent,
        reference.batch_address.offset + u64::from(half),
        &vec![0; (reference.batch_address.len - half) as usize],
    );
    assert!(log.resolve_live_record(&reference).is_err());
    log.poison_writer();
    assert!(matches!(
        log.resolve_live_record(&reference),
        Err(StorageError::Poisoned)
    ));
}

#[test]
fn cold_grant_failure_cuts_discard_authority_and_pins() {
    for point in [
        LivePausePoint::ColdReadBeforeSync,
        LivePausePoint::ColdReadAfterSync,
        LivePausePoint::ColdReadBeforePin,
        LivePausePoint::ColdReadAfterPin,
    ] {
        for sealing in [false, true] {
            let (_temp, config, log) = fixture();
            let log = Arc::new(log);
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let (reference, _) = cold_record(&config, &extent, 0);
            let before = cold_frontiers(&log);
            let (entered, resume) = log.pause_live(point);
            let reader = log.clone();
            let read = std::thread::spawn(move || reader.resolve_live_record(&reference));
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            if sealing {
                log.state.lock().sealing = true;
            } else {
                log.poison_writer();
            }
            resume.send(()).unwrap();
            assert!(read.join().unwrap().is_err());
            assert_eq!(log.live_read_pins.load(Ordering::Acquire), 0);
            assert_eq!(cold_frontiers(&log), before);
            log.state.lock().sealing = false;
        }
    }
}

#[test]
fn cold_grant_rechecks_promotion_and_retains_pin_through_retirement() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    let first = log.lease_live_extent(authority(&config)).unwrap().0;
    let next = log
        .prelease_live_extent(authority(&config), first.token)
        .unwrap()
        .0;
    let (_, bytes) = cold_record(&config, &first, 0);
    skip(&first, first.start + bytes.len() as u64);
    let (reference, _) = cold_record(&config, &next, 1);
    let (entered, resume) = log.pause_live(LivePausePoint::ColdReadAfterSync);
    let reader = log.clone();
    let read = std::thread::spawn(move || reader.resolve_live_record(&reference));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    log.adopt_live_extent(
        authority(&config),
        first.token,
        first.start + bytes.len() as u64,
        true,
    )
    .unwrap();
    log.adopt_live_extent(authority(&config), next.token, next.start + 4096, false)
        .unwrap();
    resume.send(()).unwrap();
    let lease = read.join().unwrap().unwrap();
    log.evict_authority_cache();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    assert!(log.retire_wal_files(&[0]).is_err());
    let mut bytes = vec![0; lease.len as usize];
    lease
        ._plan
        .member
        .file
        .read_exact_at(&mut bytes, lease.offset)
        .unwrap();
    assert_eq!(
        CompleteBatch::decode_record(&shape(), &reference, bytes.into()).unwrap(),
        batch(&config, 1).mutations()[0]
    );
    drop(lease);
    log.retire_wal_files(&[0]).unwrap();
    assert!(log.resolve_live_record(&reference).is_err());
}

#[test]
fn cold_grant_uses_existing_owner_coverage_and_native_never_syncs() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        for covered in [false, true] {
            let (_temp, config, log) = fixture();
            log.attach_frontier(&DurabilityConfig::with_tier(tier))
                .unwrap();
            let log = Arc::new(log);
            let extent = log.lease_live_extent(authority(&config)).unwrap().0;
            let (reference, frame) = cold_record(&config, &extent, 0);
            if covered {
                log.persist_wal_through(frame.len() as u64).unwrap();
            }
            let before = cold_frontiers(&log);
            let lease = log.resolve_live_record(&reference).unwrap();
            assert_eq!(cold_frontiers(&log), before);
            assert_eq!(
                log.state.lock().live_test_diagnostics.cold_read_syncs,
                u64::from(tier == DurabilityTier::Synchronous && !covered),
            );
            assert_eq!(log.receipt_count(), 0);
            drop(lease);
        }
    }
}

#[test]
fn cold_grant_sync_failure_is_fail_stop_without_frontier_or_pin_publication() {
    let (_temp, config, log) = fixture();
    let log = Arc::new(log);
    log.attach_frontier(&DurabilityConfig::with_tier(DurabilityTier::Synchronous))
        .unwrap();
    let extent = log.lease_live_extent(authority(&config)).unwrap().0;
    let (reference, _) = cold_record(&config, &extent, 0);
    let before = cold_frontiers(&log);
    log.fail_next_persist();
    assert!(matches!(
        log.resolve_live_record(&reference),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(cold_frontiers(&log), before);
    assert_eq!(log.live_read_pins.load(Ordering::Acquire), 0);
    assert!(log.frontier().unwrap().failure().is_some());
    assert!(matches!(
        log.resolve_live_record(&reference),
        Err(StorageError::Poisoned)
    ));
}

#[test]
fn cold_grant_reloads_normal_authority_after_healthy_extent_close() {
    for point in [
        LivePausePoint::ColdReadAfterSync,
        LivePausePoint::ColdReadAfterPin,
    ] {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (reference, frame) = cold_record(&config, &extent, 0);
        skip(&extent, extent.start + frame.len() as u64);
        let (entered, resume) = log.pause_live(point);
        let reader = log.clone();
        let read = std::thread::spawn(move || reader.resolve_live_record(&reference));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        log.adopt_live_extent(
            authority(&config),
            extent.token,
            extent.start + frame.len() as u64,
            true,
        )
        .unwrap();
        let next = log.lease_live_extent(authority(&config)).unwrap().0;
        assert_ne!(next.token, extent.token);
        resume.send(()).unwrap();
        let lease = read.join().unwrap().unwrap();
        assert_eq!(log.live_read_pins.load(Ordering::Acquire), 1);
        let mut bytes = vec![0; lease.len as usize];
        lease
            ._plan
            .member
            .file
            .read_exact_at(&mut bytes, lease.offset)
            .unwrap();
        assert_eq!(
            CompleteBatch::decode_record(&shape(), &reference, bytes.into()).unwrap(),
            batch(&config, 0).mutations()[0],
        );
        drop(lease);
        assert_eq!(log.live_read_pins.load(Ordering::Acquire), 0);
    }
}

#[test]
fn cold_grant_rejects_completed_seal_instead_of_reloading_authority() {
    for point in [
        LivePausePoint::ColdReadAfterSync,
        LivePausePoint::ColdReadAfterPin,
    ] {
        let (_temp, config, log) = fixture();
        let log = Arc::new(log);
        let extent = log.lease_live_extent(authority(&config)).unwrap().0;
        let (reference, _) = cold_record(&config, &extent, 0);
        let (entered, resume) = log.pause_live(point);
        let reader = log.clone();
        let read = std::thread::spawn(move || reader.resolve_live_record(&reference));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
        resume.send(()).unwrap();
        assert!(matches!(read.join().unwrap(), Err(StorageError::Sealed)));
        assert_eq!(log.live_read_pins.load(Ordering::Acquire), 0);
    }
}
