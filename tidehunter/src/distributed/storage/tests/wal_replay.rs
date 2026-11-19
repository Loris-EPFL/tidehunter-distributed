use super::*;

fn append(log: &PartitionLog, config: &PartitionConfig, n: u64) -> AppendedBatch {
    log.append_complete_batch(authority(config), &batch(config, n))
        .unwrap()
}

fn window_config() -> PartitionConfig {
    let mut config = config(vec![3], 3);
    config.fragment_bytes = 64 * 1024;
    config.wal_file_bytes = 1024 * 1024;
    config
}

fn no_history(path: &Path) {
    assert!(path.join("partition-wal-root-v1").is_file());
    assert!(!path.join("storage-history-v2").exists());
    assert!(!path.join("storage-history-root-v2").exists());
}

#[test]
fn wal_replay_window_evicts_oldest_and_keeps_recent_retries() {
    let temp = tempdir::TempDir::new("wal-window").unwrap();
    let config = window_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    for n in 0..=4096 {
        append(&log, &config, n);
    }
    assert!(matches!(
        log.references
            .lookup_conflict(
                id_for(&config, 0),
                batch(&config, 0).version(),
                &mut LookupWork::default()
            )
            .unwrap(),
        ConflictLookup::Free
    ));
    for n in 1..=4096 {
        assert!(matches!(
            log.references
                .lookup_conflict(
                    id_for(&config, n),
                    batch(&config, n).version(),
                    &mut LookupWork::default()
                )
                .unwrap(),
            ConflictLookup::Existing(_)
        ));
    }
    assert_eq!(log.receipt_count(), 4097);
    assert!(matches!(
        log.receipt_page(0, 1),
        Err(StorageError::Invalid(
            "receipt ordinal outside the replay window"
        ))
    ));
    assert_eq!(log.receipt_page(1, 1).unwrap()[0].id(), id_for(&config, 1));
    no_history(&path);
}

#[test]
fn wal_replay_restart_rebuilds_window_and_end_from_root_position() {
    let temp = tempdir::TempDir::new("wal-position-restart").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    for n in 0..12 {
        append(&log, &config, n);
    }
    let cut = log.state.lock().writer.as_ref().unwrap().position();
    assert_eq!(log.persist_wal_through(cut).unwrap(), cut);
    let scan_from = log.state.lock().wal_root.as_ref().unwrap().scan_from;
    assert!(scan_from > 0);
    let tail = (12..20)
        .map(|n| append(&log, &config, n))
        .collect::<Vec<_>>();
    let end = log.state.lock().writer.as_ref().unwrap().position();
    log.wal.persist_registered_files().unwrap();
    drop(log);
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(log.state.lock().appended_end, end);
    for receipt in tail {
        assert_eq!(log.lookup(receipt.id()).unwrap(), Some(receipt));
    }
    assert_eq!(
        log.state.lock().wal_root.as_ref().unwrap().scan_from,
        scan_from
    );
    no_history(&path);
}

#[test]
fn wal_replay_restart_stops_at_torn_tail_beyond_position_and_fails_below_it() {
    for corrupt_synced in [false, true] {
        let temp = tempdir::TempDir::new("wal-torn-tail").unwrap();
        let config = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        for n in 0..12 {
            append(&log, &config, n);
        }
        let through = log.state.lock().writer.as_ref().unwrap().position();
        log.persist_wal_through(through).unwrap();
        let synced_start = log.state.lock().wal_root.as_ref().unwrap().scan_from;
        let last = append(&log, &config, 12);
        let tail_start = last.address().offset - (8 + GROUP_HEADER_LEN) as u64;
        let corrupt = if corrupt_synced {
            synced_start
        } else {
            tail_start
        };
        let file = OpenOptions::new()
            .write(true)
            .open(path.join(format!("wal_{:016x}", corrupt / config.wal_file_bytes)))
            .unwrap();
        file.write_all_at(&[0xff], corrupt % config.wal_file_bytes + 8)
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        drop(log);
        let recovered = PartitionLog::recover_sealed(&path, &config, metrics());
        if corrupt_synced {
            assert!(recovered.is_err());
        } else {
            let recovered = recovered.unwrap();
            assert_eq!(recovered.lookup(last.id()).unwrap(), None);
            assert!(recovered.state.lock().appended_end <= tail_start);
        }
    }
}

#[test]
fn wal_replay_missing_nonretired_file_below_scan_position_fails() {
    let temp = tempdir::TempDir::new("wal-missing-prefix").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    for n in 0..40 {
        append(&log, &config, n);
    }
    let through = log.state.lock().writer.as_ref().unwrap().position();
    log.persist_wal_through(through).unwrap();
    assert!(log.state.lock().wal_root.as_ref().unwrap().scan_from > config.wal_file_bytes);
    drop(log);
    fs::remove_file(path.join("wal_0000000000000001")).unwrap();
    assert!(matches!(
        PartitionLog::recover_sealed(&path, &config, metrics()),
        Err(StorageError::Invalid("unexplained missing WAL file"))
    ));
}

#[test]
fn wal_replay_read_authority_decodes_member_from_wal_after_eviction() {
    let temp = tempdir::TempDir::new("wal-member-authority").unwrap();
    let config = window_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let first = append(&log, &config, 0);
    let reference = log.record_reference(first, 0).unwrap();
    for n in 1..5000 {
        append(&log, &config, n);
    }
    log.persist_appended().unwrap();
    log.evict_authority_cache();
    assert_eq!(log.lookup(first.id()).unwrap(), None);
    assert_eq!(
        log.read_record(&reference).unwrap(),
        batch(&config, 0).mutations()[0]
    );
    let mut forged = reference;
    forged.batch_address.len += 8;
    assert!(matches!(
        log.read_record(&forged),
        Err(StorageError::Invalid(
            "WAL member differs from the reference"
        ))
    ));
    assert!(matches!(
        log.persist_through(first),
        Err(StorageError::Invalid("receipt outside the replay window"))
    ));
    no_history(&path);
}

#[test]
fn replay_page_walks_groups_skip_markers_and_fragments_in_order() {
    for live in [false, true] {
        #[cfg(not(feature = "experimental-nvmeof"))]
        if live {
            continue;
        }
        let temp = tempdir::TempDir::new("wal-replay-pages").unwrap();
        let mut config = config(vec![3], 3);
        config.fragment_bytes = 4096;
        config.wal_file_bytes = 8192;
        if live {
            config.fragment_bytes = 16384;
            config.wal_file_bytes = 32768;
            config.group_limits.max_encoded_bytes = 4088;
        }
        let path = temp.path().join("stream");
        #[cfg(feature = "experimental-nvmeof")]
        let log = if live {
            PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap()
        } else {
            PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap()
        };
        #[cfg(not(feature = "experimental-nvmeof"))]
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        let mut receipts = Vec::new();
        #[cfg(feature = "experimental-nvmeof")]
        if live {
            use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
            for n in 0..10 {
                let extent = log.lease_live_extent(authority(&config)).unwrap().0;
                let group = CompleteGroup::new_live(
                    &shape(),
                    vec![batch(&config, n)],
                    config.batch_limits,
                    config.group_limits,
                )
                .unwrap();
                let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
                let file = OpenOptions::new()
                    .write(true)
                    .open(&extent.segment.path)
                    .unwrap();
                file.write_all_at(&frame, extent.start % config.wal_file_bytes)
                    .unwrap();
                file.write_all_at(
                    CrcFrame::skip_marker().as_ref(),
                    (extent.start + frame.len() as u64) % config.wal_file_bytes,
                )
                .unwrap();
                log.adopt_live_extent(
                    authority(&config),
                    extent.token,
                    extent.start + frame.len() as u64,
                    true,
                )
                .unwrap();
                receipts.push(log.lookup(id_for(&config, n)).unwrap().unwrap());
            }
            log.persist_appended().unwrap();
            log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
        }
        if !live {
            for n in (0..40).step_by(2) {
                let group = CompleteGroup::new(
                    &shape(),
                    vec![batch(&config, n), batch(&config, n + 1)],
                    config.batch_limits,
                    config.group_limits,
                )
                .unwrap();
                receipts.extend(log.append_group(authority(&config), &group).unwrap());
            }
            log.seal_epoch(authority(&config)).unwrap();
        }
        if !live {
            let mut groups_per_fragment = std::collections::BTreeMap::new();
            for pair in receipts.chunks(2) {
                *groups_per_fragment
                    .entry(pair[0].address().offset / config.fragment_bytes)
                    .or_insert(0usize) += 1;
            }
            assert!(groups_per_fragment.values().copied().max().unwrap() >= 4);
            // Lose an ordinary stream's skip marker while retaining the next
            // fragment, as unordered kernel writeback can do at a process exit.
            let mut erased = false;
            for pair in receipts.chunks(2).collect::<Vec<_>>().windows(2) {
                let start = pair[0][0].address().offset - (8 + GROUP_HEADER_LEN) as u64;
                let next = pair[1][0].address().offset - (8 + GROUP_HEADER_LEN) as u64;
                let end = (log.decode_group_at(start).unwrap().frame_end + 7) & !7;
                if end / config.fragment_bytes != next / config.fragment_bytes {
                    let file = OpenOptions::new()
                        .write(true)
                        .open(path.join(format!("wal_{:016x}", end / config.wal_file_bytes)))
                        .unwrap();
                    file.write_all_at(&[0; 8], end % config.wal_file_bytes)
                        .unwrap();
                    file.sync_all().unwrap();
                    erased = true;
                    break;
                }
            }
            assert!(erased);
        }
        drop(log);
        #[cfg(feature = "experimental-nvmeof")]
        let log = if live {
            PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap()
        } else {
            PartitionLog::recover_sealed(&path, &config, metrics()).unwrap()
        };
        #[cfg(not(feature = "experimental-nvmeof"))]
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        let mut cursor = 0;
        let mut replayed = Vec::new();
        let before_decodes = log.replay_decodes.load(Ordering::Relaxed);
        let mut pages = 0;
        let mut groups = 0;
        loop {
            let page = log.replay_page(cursor, 3, usize::MAX, 0).unwrap();
            pages += 1;
            groups += page.groups.len() as u64;
            replayed.extend(page.groups.into_iter().flat_map(|group| group.receipts));
            let Some(next) = page.next else {
                break;
            };
            assert!(next > cursor);
            cursor = next;
        }
        assert_eq!(replayed, receipts);
        assert!(log.replay_decodes.load(Ordering::Relaxed) - before_decodes <= groups + pages);
        let oversized = log.replay_page(0, 1, usize::MAX, 0).unwrap();
        assert_eq!(oversized.groups.len(), 1);
        assert_eq!(oversized.groups[0].receipts.len(), if live { 1 } else { 2 });
    }
}

#[test]
fn replay_page_byte_budget_keeps_large_batch_paging_linear() {
    let temp = tempdir::TempDir::new("wal-replay-byte-pages").unwrap();
    let mut config = config(vec![3], 3);
    config.fragment_bytes = 1024 * 1024;
    config.wal_file_bytes = 2 * config.fragment_bytes;
    config.batch_limits.max_encoded_bytes = 70 * 1024;
    config.group_limits.max_encoded_bytes = 72 * 1024;
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let mut expected = Vec::new();
    for n in 0..12 {
        let batch = CompleteBatch::new(
            &shape(),
            id_for(&config, n),
            BatchVersion {
                epoch: config.inventory.epoch,
                sequence: n,
            },
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![n as u8; 64 * 1024].into(),
            }],
            config.batch_limits,
        )
        .unwrap();
        expected.push(
            log.append_complete_batch(authority(&config), &batch)
                .unwrap(),
        );
    }
    assert!(expected.last().unwrap().address().offset < config.fragment_bytes);
    log.seal_epoch(authority(&config)).unwrap();
    let before_decodes = log.replay_decodes.load(Ordering::Relaxed);
    let mut pages = 0;
    let mut actual = Vec::new();
    let mut cursor = 0;
    loop {
        let page = log.replay_page(cursor, 256, 70 * 1024, 0).unwrap();
        assert_eq!(page.groups.len(), 1);
        assert_eq!(
            log.replay_next.load(Ordering::Relaxed),
            page.next.unwrap_or(u64::MAX)
        );
        pages += 1;
        actual.extend(page.groups.into_iter().flat_map(|group| group.receipts));
        let Some(next) = page.next else { break };
        assert!(next > cursor);
        cursor = next;
    }
    assert_eq!(actual, expected);
    assert_eq!(pages, expected.len() as u64);
    assert!(
        log.replay_decodes.load(Ordering::Relaxed) - before_decodes
            <= expected.len() as u64 + pages
    );
    // A page never splits a group, even when the first one exceeds its byte bound.
    assert_eq!(
        log.replay_page(0, 256, 0, 0).unwrap().groups[0].receipts,
        expected[..1]
    );
}

#[test]
fn replay_page_rejects_mid_frame_position_and_short_sealed_end() {
    let temp = tempdir::TempDir::new("wal-replay-boundaries").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    append(&log, &config, 0);
    assert!(matches!(
        log.replay_page(0, 3, usize::MAX, 0),
        Err(StorageError::Invalid(
            "replay requires sealed immutable epoch"
        ))
    ));
    log.seal_epoch(authority(&config)).unwrap();
    let end = log.state.lock().appended_end;
    assert!(matches!(
        log.replay_page(8, 3, usize::MAX, 0),
        Err(StorageError::Invalid(
            "replay position is not a frame boundary"
        ))
    ));
    assert!(matches!(
        log.replay_page(1, 3, usize::MAX, 0),
        Err(StorageError::Invalid(
            "replay position is not a frame boundary"
        ))
    ));
    assert!(matches!(
        log.replay_page(0, 3, usize::MAX, end + 1),
        Err(StorageError::Invalid(
            "sealed WAL ends below the required position"
        ))
    ));
    assert!(
        log.replay_page(end, 3, usize::MAX, end)
            .unwrap()
            .next
            .is_none()
    );
}

#[test]
fn retire_wal_files_records_root_before_unlink_and_survives_restart() {
    for cut in [false, true] {
        let temp = tempdir::TempDir::new("wal-root-retirement").unwrap();
        let config = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        for n in 0..20 {
            append(&log, &config, n);
        }
        log.seal_epoch(authority(&config)).unwrap();
        log.fail_wal_retire.store(cut, Ordering::Release);
        assert_eq!(log.retire_wal_files(&[0]).is_err(), cut);
        assert_eq!(path.join("wal_0000000000000000").exists(), cut);
        drop(log);
        let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert!(!path.join("wal_0000000000000000").exists());
        assert!(recovered.is_retired(0).unwrap());
        recovered.retire_wal_files(&[0]).unwrap();
        no_history(&path);
    }
}

#[test]
fn wal_root_barrier_and_rename_crash_cuts_recover_the_synced_suffix() {
    for phase in 1..=3 {
        let temp = tempdir::TempDir::new("wal-root-crash-cut").unwrap();
        let config = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        for n in 0..12 {
            append(&log, &config, n);
        }
        let through = log.state.lock().writer.as_ref().unwrap().position();
        log.fail_wal_root.store(phase, Ordering::Release);
        assert!(log.persist_wal_through(through).is_err());
        drop(log);
        let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(recovered.state.lock().appended_end, through);
        assert_eq!(
            recovered
                .lookup(id_for(&config, 11))
                .unwrap()
                .unwrap()
                .version()
                .sequence,
            11
        );
    }
}

#[test]
fn wal_replay_rejects_failed_publication_without_window_authority() {
    let temp = tempdir::TempDir::new("wal-window-failed-publication").unwrap();
    let config = window_config();
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = append(&log, &config, 0);
    let conflict = CompleteBatch::new(
        &shape(),
        id_for(&config, 1),
        first.version(),
        batch(&config, 1).mutations().to_vec(),
        config.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &conflict),
        Err(StorageError::VersionConflict)
    ));
    assert_eq!(log.lookup(conflict.id()).unwrap(), None);
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.synced_frontier(), 0);
    log.persist_through(first).unwrap();
    assert!(log.synced_frontier() > 0);
}

#[test]
fn wal_replay_persists_synced_end_without_advancing_scan_fragment() {
    let temp = tempdir::TempDir::new("wal-exact-sync-position").unwrap();
    let config = window_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    append(&log, &config, 0);
    let first = log.state.lock().writer.as_ref().unwrap().position();
    assert_eq!(log.persist_wal_through(first).unwrap(), first);
    let generation = log.state.lock().wal_root.as_ref().unwrap().generation;
    append(&log, &config, 1);
    let second = log.state.lock().writer.as_ref().unwrap().position();
    assert_eq!(log.persist_wal_through(second).unwrap(), second);
    assert_eq!(log.persist_wal_through(first).unwrap(), second);
    {
        let state = log.state.lock();
        let root = state.wal_root.as_ref().unwrap();
        assert_eq!(root.scan_from, 0);
        assert_eq!(root.synced_end, second);
        assert_eq!(root.generation, generation + 1);
    }
    assert!(matches!(
        log.persist_wal_through(second + 8),
        Err(StorageError::Invalid(
            "checkpoint position beyond allocated WAL"
        ))
    ));
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(
        recovered.state.lock().wal_root.as_ref().unwrap().synced_end,
        second
    );
}

#[cfg(feature = "experimental-nvmeof")]
#[test]
fn wal_replay_live_scan_stops_before_a_later_valid_fragment() {
    let temp = tempdir::TempDir::new("wal-live-stop").unwrap();
    let config = live_config();
    let path = temp.path().join("stream");
    let log = PartitionLog::create_live(&path, config.clone(), &shape(), metrics()).unwrap();
    let receipts = (0..3)
        .map(|n| append_live_fixture(&log, &config, n))
        .collect::<Vec<_>>();
    let file = OpenOptions::new()
        .write(true)
        .open(path.join("wal_0000000000000000"))
        .unwrap();
    file.write_all_at(&[0; 4096], 4096).unwrap();
    file.sync_all().unwrap();
    drop(file);
    drop(log);
    for _ in 0..2 {
        let log = PartitionLog::recover_live_sealed(&path, &config, metrics(), || Ok(())).unwrap();
        assert_eq!(log.state.lock().appended_end, 4096);
        let page = log.replay_page(0, 3, usize::MAX, 0).unwrap();
        assert_eq!(
            page.groups
                .into_iter()
                .flat_map(|group| group.receipts)
                .collect::<Vec<_>>(),
            receipts[..1]
        );
        assert!(page.next.is_none());
    }
}

#[cfg(feature = "experimental-nvmeof")]
#[test]
fn position_proof_rejects_allocated_but_uncompleted_owner_rpc_bytes() {
    let temp = tempdir::TempDir::new("position-owner-frontier").unwrap();
    let config = window_config();
    let log =
        PartitionLog::create(&temp.path().join("stream"), config, &shape(), metrics()).unwrap();
    let _allocation = {
        let state = log.state.lock();
        state
            .writer
            .as_ref()
            .unwrap()
            .reserve_external(4096)
            .unwrap()
    };
    assert!(matches!(
        log.persist_wal_through(64),
        Err(StorageError::Invalid(
            "checkpoint position beyond completed owner writes"
        ))
    ));
}

#[test]
fn position_root_can_advance_without_another_file_barrier() {
    let temp = tempdir::TempDir::new("position-root-only").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    for n in 0..32 {
        append(&log, &config, n);
    }
    let through = log.state.lock().appended_end;
    log.persist_wal_through(through).unwrap();
    let mut root = log.state.lock().wal_root.clone().unwrap();
    assert!(root.scan_from > 0);
    root.scan_from = 0; // A valid conservative replay root, with the same durable end.
    root.install(&log.path).unwrap();
    let generation = root.generation;
    log.state.lock().wal_root = Some(root);
    let mut profile = StorageWriteProfile::default();
    log.persist_wal_through_profiled(through, Some(&mut profile))
        .unwrap();
    assert_eq!(
        (profile.file_persist_calls, profile.namespace_persist_calls),
        (0, 0)
    );
    let root = log.state.lock().wal_root.clone().unwrap();
    assert!(root.scan_from > 0);
    assert_eq!(root.generation, generation + 1);
}

#[cfg(feature = "experimental-nvmeof")]
#[test]
fn position_recheck_rejects_a_direct_lifecycle_change_during_data_sync() {
    let temp = tempdir::TempDir::new("position-lifecycle-recheck").unwrap();
    let config = window_config();
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let receipt = append(&log, &config, 0);
    let through = receipt.address().offset + u64::from(receipt.address().len);
    let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeRoot);
    let owner = log.clone();
    let proof = std::thread::spawn(move || owner.persist_wal_through(through));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // A storage caller outside the node scheduler deliberately breaks the
    // lifecycle exclusion. The proof must fail, not silently install its cut.
    let sealed = log.seal_epoch(authority(&config));
    release.send(()).unwrap();
    sealed.unwrap();
    assert!(matches!(
        proof.join().unwrap(),
        Err(StorageError::Invalid(
            "position proof lifecycle changed during sync"
        ))
    ));
}
