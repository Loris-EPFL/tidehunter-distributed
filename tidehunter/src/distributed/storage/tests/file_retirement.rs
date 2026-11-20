//! R1 owner retirement: compressed retired history, lease-logical seals,
//! retirement crash cuts against a no-GC control, and the prepared grant
//! staying independent of maintenance.
use super::*;
use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
use crate::distributed::storage::file_retirement::CrashCut;
use crate::distributed::storage::retired_files::RetiredFiles;

const FILE: u64 = 32 * 1024;

fn fixture() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let temp = tempdir::TempDir::new("r1-retirement").unwrap();
    let mut config = live_config();
    config.fragment_bytes = 16 * 1024;
    config.wal_file_bytes = FILE;
    let log = PartitionLog::create_live(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    (temp, config, log)
}

fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .write_all_at(bytes, offset)
        .unwrap();
}

/// Fill `extents` whole fragments (one group plus a skip marker each),
/// starting with batch sequence `first`; returns the allocator end.
fn fill(log: &PartitionLog, config: &PartitionConfig, first: u64, extents: u64) -> u64 {
    let mut end = 0;
    for n in first..first + extents {
        let extent = log.lease_live_extent(authority(config)).unwrap().0;
        let group = CompleteGroup::new_live(
            &shape(),
            vec![batch(config, n)],
            config.batch_limits,
            config.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        let bytes = extent.segment.bytes;
        write_at(&extent.segment.path, extent.start % bytes, &frame);
        let mut skip = [0; 4096];
        skip[..8].copy_from_slice(CrcFrame::skip_marker().as_ref());
        let marker = extent.start + frame.len() as u64;
        write_at(&extent.segment.path, marker % bytes, &skip);
        log.adopt_live_extent(authority(config), extent.token, marker, true)
            .unwrap();
        end = extent.end;
    }
    end
}

fn witness() -> RetirementWitness {
    RetirementWitness {
        replay_from: u64::MAX,
        callback_from: u64::MAX,
        persisted_cut_next: None,
    }
}

/// Proofs for every durably sealed file. Eligibility is the coordinator's
/// watermark (the `witness()` below covers everything), never an owner scan
/// origin: the owner keeps no position root and no reference information.
fn proofs(log: &PartitionLog) -> Vec<RetireProof> {
    log.file_retirement_state()
        .0
        .iter()
        .map(|seal| RetireProof {
            file_start: seal.start,
            inode: Some(WalInode {
                device: seal.device,
                inode: seal.inode,
            }),
            seal_generation: Some(seal.root_generation),
        })
        .collect()
}

/// Six fragments (three files), all past the allocator: every whole file seals.
/// The seal proves completeness from the allocator position and the extents,
/// never from an owner position root (native has none); durability is the
/// Version B term of the retirement witness.
fn sealed_stream() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let (temp, config, log) = fixture();
    fill(&log, &config, 0, 6);
    log.persist_wal_through(log.appended_frontier()).unwrap();
    assert_eq!(log.seal_ready_files().unwrap(), 3);
    (temp, config, log)
}

fn recover(path: &Path, config: &PartitionConfig) -> PartitionLog {
    PartitionLog::recover_live_sealed(path, config, metrics(), || Ok(())).unwrap()
}

fn wal_exists(path: &Path, file: u64) -> bool {
    path.join(format!("wal_{file:016x}")).exists()
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
        fill(&log, &config, next, 4);
        next += 4;
        log.persist_wal_through(log.appended_frontier()).unwrap();
        log.seal_ready_files().unwrap();
        let victims = proofs(&log);
        if !victims.is_empty() {
            assert_eq!(
                log.retire_proved_files(&victims, Some(&witness())).unwrap(),
                RetirementOutcome::Completed
            );
        }
        let (seals, pending) = log.file_retirement_state();
        assert!(pending.is_empty());
        assert!(seals.len() <= 2, "seals of unretired files only: {seals:?}");
        sizes.push(log.wal_root_encoded_len());
    }
    // Contiguous turnover: one retired prefix, a bounded seal set.
    assert!(sizes[4..].iter().all(|size| *size <= sizes[3]), "{sizes:?}");
    // The serving stream keeps granting after many retirements.
    fill(&log, &config, next, 1);
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
        let _ = done.send(next.map(|(extent, _)| extent.segment.file_index));
    });
    let granted = finished
        .recv_timeout(Duration::from_secs(5))
        .expect("prepared grant waited for maintenance");
    drop(paused);
    worker.join().unwrap();
    assert_eq!(granted.unwrap(), 1);
}
