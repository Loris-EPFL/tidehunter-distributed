//! R1 owner retirement: compressed retired history, lease-logical seals,
//! retirement crash cuts against a no-GC control, and the prepared grant
//! staying independent of maintenance.
use super::*;
use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
use crate::distributed::storage::file_retirement::CrashCut;
use crate::distributed::storage::retired_files::RetiredFiles;
use crate::distributed::storage::wal_replay::FRAME_BOUNDARY;

mod whole_file;

const FILE: u64 = 32 * 1024;

/// The bound a test waits for a hook or a maintenance thread. It only ends a
/// failed wait; no premise rests on this duration.
const HOOK_WAIT: Duration = Duration::from_secs(10);

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

/// Leases the next extent and resolves it at once with no frame in it: a skip
/// marker at its start and an adoption there. `quiesce_wal_maps` waits for every
/// allocated position to be processed, so an extent left open would hang it.
/// The fragment allocation still finalizes the map of the file that ended at the
/// old allocator. Returns the start of the new extent.
fn lease_and_resolve(log: &PartitionLog, config: &PartitionConfig) -> u64 {
    let extent = log.lease_live_extent(authority(config)).unwrap().0;
    let bytes = extent.segment.bytes;
    let mut skip = [0; 4096];
    skip[..8].copy_from_slice(CrcFrame::skip_marker().as_ref());
    write_at(&extent.segment.path, extent.start % bytes, &skip);
    log.adopt_live_extent(authority(config), extent.token, extent.start, true)
        .unwrap();
    extent.start
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

fn recover(path: &Path, config: &PartitionConfig) -> PartitionLog {
    PartitionLog::recover_live_sealed(path, config, metrics(), || Ok(())).unwrap()
}

fn try_recover(path: &Path, config: &PartitionConfig) -> Result<PartitionLog, StorageError> {
    PartitionLog::recover_live_sealed(path, config, metrics(), || Ok(()))
}

fn wal_exists(path: &Path, file: u64) -> bool {
    path.join(format!("wal_{file:016x}")).exists()
}

fn synced_end(log: &PartitionLog) -> u64 {
    log.state
        .lock()
        .wal_root
        .as_ref()
        .map_or(0, |root| root.synced_end)
}

/// Six fragments (three files), all past the allocator: every whole file seals.
/// The seal proves completeness from the allocator position and the extents,
/// never from an owner position root (native has none); durability is the
/// Version B term of the retirement witness.
fn sealed_stream() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let (temp, config, log) = fixture();
    fill(&log, &config, 0, 6);
    // One more extent: the writer has moved into file 3, which stays unsealed
    // and holds the tail batch, so the three sealed files are all behind it
    // (the no-GC control must keep the tail).
    fill(&log, &config, 6, 1);
    log.persist_wal_through(log.appended_frontier()).unwrap();
    assert_eq!(log.seal_ready_files().unwrap(), 3);
    (temp, config, log)
}

/// The exact-boundary scenario: six fragments, three files, all past the
/// allocator and sealed, and NO fragment after them, so the newest sealed file
/// ends exactly at the allocator and holds the tail batch.
fn sealed_stream_at_the_allocator() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let (temp, config, log) = fixture();
    fill(&log, &config, 0, 6);
    log.persist_wal_through(log.appended_frontier()).unwrap();
    assert_eq!(log.seal_ready_files().unwrap(), 3);
    (temp, config, log)
}

/// `sealed_stream_at_the_allocator`, then the writer leases the next extent
/// (nothing is written into it): the allocation moves the allocator into the
/// next file, which finalizes the fragment map of the file that ended at the old
/// allocator, so every sealed file can now be detached. The stream is the one
/// an owner leaves when it is killed right after retiring its tail data file.
fn sealed_stream_then_the_next_extent_is_leased()
-> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let (temp, config, log) = sealed_stream_at_the_allocator();
    let start = lease_and_resolve(&log, &config);
    assert!(start >= 3 * FILE, "the lease starts in file 3");
    log.quiesce_wal_maps();
    (temp, config, log)
}

/// Safety and liveness of a stream KILLED after it retired its tail file (no
/// SEAL record exists): whatever the crash leaves, the recovered allocator
/// frontier is never below the no-GC control and never a fragment or more above
/// it. A frontier inside a retired range would let the owner re-issue offsets
/// of acknowledged batches; a frontier below the coordinator's `require_end`
/// would make it refuse the stream ("sealed WAL ends below the required
/// position"). The difference below one fragment is the accepted residual: the
/// floor is `strict_end` (the root-certified end), which can lie above the true
/// data end by the skipped suffix of the last fragment; that generation is not
/// trim-eligible (the owner reports an end above the coordinator's front).
///
/// Fix A'2: when no SEAL record exists and the tail file is retired, the
/// recovered end is `max(scanned end, strict_end)` and the SEAL records it, so
/// the second and third recovery read back exactly the same end. All four cuts
/// are collected and checked after the loop, so one failing cut does not hide
/// the others, and every recovered end is printed.
///
/// Negative controls (mutation runs): (i) remove the floor: the cuts after the
/// root install fail the lower bound; (ii) keep the floor but record the
/// scanned end in the SEAL: the second open differs from the first.
#[test]
fn retiring_the_file_that_ends_at_the_allocator_never_moves_the_recovered_allocator_back() {
    // The no-GC control: the frontier a reopen of the untouched stream recovers.
    let (before, fragment_bytes) = {
        let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
        assert!(
            synced_end(&log) >= log.appended_frontier(),
            "the seal raised the root's synced end to the allocator"
        );
        drop(log);
        (
            recover(&temp.path().join("stream"), &config).appended_frontier(),
            config.fragment_bytes,
        )
    };
    let mut failures = Vec::new();
    for cut in [
        CrashCut::RetireAfterRoot,
        CrashCut::BeforeUnlink,
        CrashCut::AfterFirstUnlink,
        CrashCut::AfterDirectorySync,
    ] {
        let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
        let victims = proofs(&log);
        assert_eq!(victims.len(), 3);
        log.arm_crash_cut(cut);
        assert!(log.retire_proved_files(&victims, Some(&witness())).is_err());
        drop(log);
        let path = temp.path().join("stream");
        // Three opens of the crash image. The coordinator's `require_end` is its
        // replay front, here the control end: the recovered stream must satisfy
        // it on this and on every later open (the SEAL written by the first
        // recovery records the floored end). A failing open is recorded, not
        // unwrapped, so every cut reaches the final assertion.
        let mut first_end = None;
        for open in 1..=3 {
            let log = match try_recover(&path, &config) {
                Ok(log) => log,
                Err(error) => {
                    failures.push(format!("{cut:?}: open {open} failed: {error:?}"));
                    break;
                }
            };
            let end = log.appended_frontier();
            eprintln!("{cut:?}: open {open} end {end}, no-GC control {before}");
            match first_end {
                None => {
                    first_end = Some(end);
                    if end < before || end - before >= fragment_bytes {
                        failures.push(format!(
                            "{cut:?}: recovered end {end} vs control {before} \
                             (fragment {fragment_bytes})"
                        ));
                    }
                }
                Some(first) if end != first => {
                    failures.push(format!(
                        "{cut:?}: open {open} end {end} != first end {first}"
                    ));
                }
                Some(_) => {}
            }
            if let Err(error) = log.replay_page(0, 3, usize::MAX, before) {
                failures.push(format!(
                    "{cut:?}: open {open} refuses require_end {before}: {error:?}"
                ));
            }
            drop(log);
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Prints what `replay_page` and the root say about a recovered stream: the
/// root's synced end and scan origin, the recovered frontier, the length of
/// every WAL file and 16 bytes at the frontier. Diagnostics only: a failing
/// contract below is readable from this output.
fn print_replay_probe(label: &str, log: &PartitionLog, path: &Path) {
    let (generation, scan_from, synced_end) = log.position_root_for_test();
    let frontier = log.appended_frontier();
    let mut lengths = Vec::new();
    for file in 0..8 {
        if let Ok(metadata) = std::fs::metadata(path.join(format!("wal_{file:016x}"))) {
            lengths.push((file, metadata.len()));
        }
    }
    let at_frontier = std::fs::File::open(path.join(format!("wal_{:016x}", frontier / FILE)))
        .ok()
        .and_then(|file| {
            let mut bytes = [0u8; 16];
            file.read_exact_at(&mut bytes, frontier % FILE).ok()?;
            Some(bytes)
        });
    eprintln!(
        "{label}: root generation {generation} scan_from {scan_from} synced_end {synced_end}, \
         recovered frontier {frontier}, wal file lengths {lengths:?}, 16 bytes at the frontier \
         {at_frontier:02x?}"
    );
}

/// The contract of `replay_page` on a recovered sealed stream of six
/// one-batch groups, one per 16 KiB fragment (group `n` starts at `n * 16384`):
/// from any frame boundary at or below the frontier it returns every
/// acknowledged frame at or above that boundary (retired files contribute
/// none, and are skipped, never an error); a position that is not a frame
/// boundary and a required end above the frontier are typed
/// `StorageError::Invalid` errors.
fn assert_replay_contract(
    log: &PartitionLog,
    path: &Path,
    retired_below: u64,
    from_frontier: FrontierReplay,
) {
    assert_replay_contract_for(log, path, retired_below, 6, from_frontier);
}

/// What `replay_page(frontier)` answers, named by each caller from the
/// mechanism and never from the observed frontier. `replay_groups` accepts a
/// start only after it has decoded the frames of the start's fragment from the
/// fragment's first byte (`boundary` loop). From the end of the stream:
/// - `Empty`: the fragment's first byte is at or before the frontier and its
///   file is readable (the frontier is a fragment start, or the file is live):
///   an empty final page;
/// - `RefusedInRetiredFile`: the frontier lies inside a fragment of a RETIRED
///   file, so the boundary proof cannot read the fragment and the answer is
///   the typed `Invalid` error. The coordinator never asks for it: its replay
///   start is aligned down to a fragment.
#[derive(Clone, Copy, Debug)]
enum FrontierReplay {
    Empty,
    RefusedInRetiredFile,
}

/// [`assert_replay_contract`] for a stream of `frames` one-batch groups.
fn assert_replay_contract_for(
    log: &PartitionLog,
    path: &Path,
    retired_below: u64,
    frames: u64,
    from_frontier: FrontierReplay,
) {
    print_replay_probe("replay contract", log, path);
    let frontier = log.appended_frontier();
    let fragment = 16 * 1024;
    for first in 0..frames {
        let from = first * fragment;
        let mut cursor = from;
        let mut batches = Vec::new();
        loop {
            let page = log
                .replay_page(cursor, 3, usize::MAX, 0)
                .unwrap_or_else(|error| panic!("replay from boundary {from}: {error:?}"));
            batches.extend(page.groups.iter().flat_map(|group| {
                group
                    .receipts
                    .iter()
                    .map(|receipt| receipt.address().offset)
            }));
            let Some(next) = page.next else { break };
            assert!(next > cursor, "paging must advance");
            cursor = next;
        }
        // Frames of retired files are skipped; every other frame at or above
        // the boundary is returned, in order.
        let expected: Vec<u64> = (first..frames)
            .map(|n| n * fragment)
            .filter(|start| *start >= retired_below)
            .collect();
        assert_eq!(
            batches.len(),
            expected.len(),
            "replay from {from} returned {batches:?}, frames expected at {expected:?}"
        );
        assert!(
            batches.windows(2).all(|pair| pair[0] < pair[1]),
            "{batches:?}"
        );
    }
    // A position inside a frame is not a boundary.
    assert!(matches!(
        log.replay_page(fragment + 8, 3, usize::MAX, 0),
        Err(StorageError::Invalid(_))
    ));
    // A required end above the recovered frontier is the typed short-stream error.
    assert!(matches!(
        log.replay_page(0, 3, usize::MAX, frontier + 1),
        Err(StorageError::Invalid(
            "sealed WAL ends below the required position"
        ))
    ));
    // A start above the frontier is refused, never read.
    assert!(matches!(
        log.replay_page(frontier + 8, 3, usize::MAX, 0),
        Err(StorageError::Invalid(_))
    ));
    // From the frontier: the caller names the answer, and the premise that
    // makes it the mechanism's answer is asserted first (position of the
    // frontier in its fragment and the retirement of the file holding it).
    let frontier_file = frontier / FILE * FILE;
    let in_retired_fragment =
        !frontier.is_multiple_of(fragment) && log.is_retired(frontier_file).unwrap();
    let from_frontier_page = log.replay_page(frontier, 3, usize::MAX, 0);
    eprintln!(
        "replay from the frontier {frontier} ({from_frontier:?}, inside a retired fragment: \
         {in_retired_fragment}): {:?}",
        from_frontier_page.as_ref().map(|page| page.groups.len())
    );
    match from_frontier {
        FrontierReplay::Empty => {
            assert!(
                !in_retired_fragment,
                "premise: the boundary proof can read the fragment"
            );
            let page = from_frontier_page
                .unwrap_or_else(|error| panic!("replay from the frontier {frontier}: {error:?}"));
            assert!(
                page.groups.is_empty() && page.next.is_none(),
                "the frontier {frontier} replays nothing"
            );
        }
        FrontierReplay::RefusedInRetiredFile => {
            assert!(
                in_retired_fragment,
                "premise: the frontier is inside a retired fragment"
            );
            assert!(
                matches!(
                    from_frontier_page,
                    Err(StorageError::Invalid(FRAME_BOUNDARY))
                ),
                "the boundary proof cannot read a retired fragment: {from_frontier_page:?}"
            );
        }
    }
    // One whole frame above the frontier is above the stream: typed refusal.
    assert!(matches!(
        log.replay_page(frontier + fragment, 3, usize::MAX, 0),
        Err(StorageError::Invalid(_))
    ));
}

#[test]
fn replay_page_serves_every_acknowledged_frame_from_any_boundary_of_the_control_stream() {
    let (temp, config, log) = sealed_stream_at_the_allocator();
    drop(log);
    let path = temp.path().join("stream");
    let recovered = recover(&path, &config);
    assert_replay_contract(&recovered, &path, 0, FrontierReplay::Empty);
}

/// The same contract after every file retired and the owner restarted: the
/// recovered frontier covers the stream, retired files replay as skipped
/// ranges, and nothing errors. Before fix A' the in-memory recovered end was
/// the scanned end only, and `replay_page` refused the coordinator's required
/// end.
///
/// Negative control (mutation run): remove the post-SEAL floor
/// (`physical_end.max(strict_end).max(retired.end())`) in `recover_wal_replay`.
#[test]
fn replay_page_over_a_fully_retired_stream_returns_nothing_and_never_errors() {
    let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
    let victims = proofs(&log);
    assert_eq!(victims.len(), 3);
    // The lease moved the allocator into file 3 and finalized the fragment map
    // of the file that ended at the old allocator: every sealed file detaches.
    assert_eq!(
        log.retire_proved_files(&victims, Some(&witness())).unwrap(),
        RetirementOutcome::Completed,
        "premise: the whole stream retires"
    );
    drop(log);
    let path = temp.path().join("stream");
    for file in 0..3 {
        assert!(!wal_exists(&path, file), "premise: file {file} is unlinked");
    }
    let recovered = recover(&path, &config);
    assert!(
        recovered.is_retired(0).unwrap(),
        "premise: the retirement is durable"
    );
    let frontier = recovered.appended_frontier();
    assert_eq!(
        frontier, 98304,
        "premise: the recovered frontier is the old allocator end"
    );
    // Probe offsets of the failing owner image: 90112 lies inside the last
    // fragment, past its only frame; it is not a frame boundary.
    let inside = recovered.replay_page(90112, 3, usize::MAX, 0);
    eprintln!(
        "replay from 90112: {:?}",
        inside.as_ref().map(|page| page.groups.len())
    );
    assert!(
        matches!(inside, Err(StorageError::Invalid(_))),
        "90112 is not a frame boundary"
    );
    assert_replay_contract(&recovered, &path, 3 * FILE, FrontierReplay::Empty);
}

/// A GRACEFULLY sealed stream (`seal_live_epoch` writes the SEAL record) whose
/// files were all retired through the SEALED-stream path (`retire_wal_files`,
/// no file seals; the last file is retired with `start + size`, which can lie
/// above the true end) must reopen with exactly the no-GC end: a recovered end
/// raised to the retired end would be reported as the owner's durable end, and
/// the coordinator's `replay_from >= durable_end` check
/// (`generation_unused` in `fencing_generations.rs`) would keep the generation
/// non-trimmable for ever. With the SEAL present the end is the scanned end,
/// unchanged by A'2, and the replay contract holds.
#[test]
fn a_gracefully_sealed_stream_retired_through_the_sealed_path_reopens_at_the_control_end() {
    // Five fragments: files 0 and 1 are full and file 2 holds one fragment, so
    // the stream ends INSIDE its last file and the last file's end
    // (`start + size`) lies above the true end.
    let (temp, config, log) = fixture();
    fill(&log, &config, 0, 5);
    log.persist_appended().unwrap();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    drop(log);
    let path = temp.path().join("stream");
    let control = recover(&path, &config);
    let control_end = control.appended_frontier();
    let control_synced = control.synced_frontier();
    let last_file_end = (control_end / FILE + 1) * FILE;
    eprintln!(
        "control: end {control_end} synced frontier {control_synced} last file ends {last_file_end}"
    );
    assert!(
        control_end < last_file_end,
        "premise: the stream ends inside its last file, so a retired-end floor would differ"
    );
    control.retire_wal_files(&[0, FILE, 2 * FILE]).unwrap();
    drop(control);
    let recovered = recover(&path, &config);
    print_replay_probe("graceful seal, sealed-path retirement", &recovered, &path);
    assert_eq!(
        recovered.appended_frontier(),
        control_end,
        "the reported end is the no-GC end, so `replay_from >= durable_end` holds"
    );
    assert_eq!(recovered.synced_frontier(), control_synced);
    assert!(
        recovered.is_retired(0).unwrap(),
        "premise: the retirement is durable"
    );
    assert!(
        recovered.is_retired(2 * FILE).unwrap(),
        "premise: the last file retired too"
    );
    // Frames 0..3 lie in the retired files 0 and 1 and file 2 (frame 4): all gone.
    assert_replay_contract_for(
        &recovered,
        &path,
        3 * FILE,
        5,
        FrontierReplay::RefusedInRetiredFile,
    );
}

/// A graceful seal whose root `synced_end` lies above the data end, with the
/// tail file NOT retired, reopens: the SEAL records the scanned end and the
/// A'2 floor never applies (a SEAL record exists), so "sealed inventory no
/// longer matches surviving frames" cannot fire.
///
/// Negative control (mutation run): floor the recovered end before the SEAL
/// comparison (`physical_end` initial value) and this reopen fails.
#[test]
fn a_graceful_seal_with_a_synced_end_above_the_data_end_and_a_live_tail_reopens() {
    let (temp, config, log) = sealed_stream();
    assert!(
        synced_end(&log) > log.appended_frontier() - config.fragment_bytes,
        "premise: the three file seals raised synced_end to the sealed files' end"
    );
    let end = log.appended_frontier();
    log.persist_appended().unwrap();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    drop(log);
    let path = temp.path().join("stream");
    for open in 1..=2 {
        let recovered = recover(&path, &config);
        eprintln!("open {open}: end {}", recovered.appended_frontier());
        assert_eq!(recovered.appended_frontier(), end, "open {open}");
    }
}

/// The end a graceful shutdown (`persist_appended`, then the SEAL) of the
/// leased stream records and a reopen recovers when nothing was retired.
fn graceful_control_end() -> u64 {
    let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
    log.persist_appended().unwrap();
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    drop(log);
    recover(&temp.path().join("stream"), &config).appended_frontier()
}

/// An owner that retired every sealed frame file while it was serving: the
/// three files are unlinked and named by the WAL root, the log ends in file 2
/// (`appended_end` is the end of the last frame, inside file 2), and `state.durable_end`
/// still lags. `durable_end` moves only in `persist_scope_inner`, the graceful
/// seal and recovery; a file seal or a position proof moves `synced_end` of
/// the root and not `durable_end` (the premise is asserted before and after).
fn live_retired_stream() -> (tempdir::TempDir, PartitionConfig, PartitionLog) {
    let (temp, config, log) = sealed_stream_then_the_next_extent_is_leased();
    {
        let state = log.state.lock();
        assert!(
            state.appended_end > 2 * FILE && state.appended_end < 3 * FILE,
            "premise: the log ends inside file 2, {}",
            state.appended_end
        );
        assert_eq!(
            state.durable_end, 0,
            "premise: no persist moved the log durable end"
        );
    }
    let victims = proofs(&log);
    assert_eq!(victims.len(), 3);
    assert_eq!(
        log.retire_proved_files(&victims, Some(&witness())).unwrap(),
        RetirementOutcome::Completed,
        "premise: the live retirement completed"
    );
    let path = temp.path().join("stream");
    for file in 0..3 {
        assert!(
            log.is_retired(file * FILE).unwrap(),
            "premise: file {file} is retired"
        );
        assert!(!wal_exists(&path, file), "premise: file {file} is unlinked");
    }
    assert_eq!(
        log.state.lock().durable_end,
        0,
        "premise: the retirement left durable_end"
    );
    (temp, config, log)
}

/// The inventory witness file of WAL file `file` (the prefix is private to
/// `file_inventory`).
fn witness_path(path: &Path, file: u64) -> std::path::PathBuf {
    path.join(format!("partition-required-wal-v1-{file:016x}"))
}

/// A'7 (a): the graceful shutdown of an owner whose sealed frame files were
/// all retired live. `persist_scope_inner` starts at `state.durable_end`, so
/// it reaches the retired files; it must skip them (a retired file was
/// durable when it retired and its path is gone), (d) leave
/// `durable_end == appended_end`, poison nothing, and let the SEAL be
/// written. The reopen recovers exactly the end of the no-retirement control:
/// the SEAL records the in-memory end, which retirement does not change.
///
/// Negative control (mutation run): remove the `root.retired.contains` skip in
/// `persist_scope_inner`: `persist_appended` fails with the NotFound of
/// "completed WAL frame file disappeared" and poisons the stream (Fatal).
#[test]
fn a_graceful_shutdown_after_every_sealed_frame_file_retired_writes_the_seal_and_reopens() {
    let control_end = graceful_control_end();
    let (temp, config, log) = live_retired_stream();
    let syncs = log.state.lock().stats.wal_file_syncs;
    let durable = log
        .persist_appended()
        .unwrap_or_else(|error| panic!("the persist reached a retired file: {error:?}"));
    {
        let state = log.state.lock();
        assert!(state.poison.is_none(), "the persist poisoned the stream");
        // (d)
        assert_eq!(state.durable_end, state.appended_end);
        assert_eq!(durable, state.appended_end);
        // Files 0..=2 hold the whole log and are retired: no file sync at all.
        assert_eq!(
            state.stats.wal_file_syncs, syncs,
            "no retired file is synced"
        );
    }
    log.seal_live_epoch(authority(&config), || Ok(()))
        .unwrap_or_else(|error| panic!("the seal after the persist: {error:?}"));
    assert!(log.state.lock().poison.is_none());
    drop(log);
    let path = temp.path().join("stream");
    assert!(
        path.join(crate::distributed::storage::SEAL).exists(),
        "the SEAL is written"
    );
    let recovered = recover(&path, &config);
    print_replay_probe("seal after a live retirement", &recovered, &path);
    assert_eq!(
        recovered.appended_frontier(),
        control_end,
        "the control end"
    );
    assert!(
        recovered.is_retired(0).unwrap(),
        "premise: the retirement is durable"
    );
    assert_replay_contract(
        &recovered,
        &path,
        3 * FILE,
        FrontierReplay::RefusedInRetiredFile,
    );
}

/// A'7 (b): the same shutdown with `namespace_synced_file == None`, the state
/// a log has after its owner restarted (`recover_wal_replay` starts it at
/// `None`). The namespace sync and the inventory witnesses then run for files
/// `0..=last_file`, retired ones included. A witness is a file of its own, so
/// a missing witness is recreated (counted in `inventory_witnesses`), while
/// the others are validated and kept. The persist and reopen succeed.
///
/// Negative control: skip witness creation in the namespace persist.
#[test]
fn the_persist_past_retired_files_recreates_a_missing_witness() {
    let (temp, config, log) = live_retired_stream();
    let path = temp.path().join("stream");
    for file in 0..3 {
        assert!(
            witness_path(&path, file).exists(),
            "premise: witness {file} exists"
        );
    }
    std::fs::remove_file(witness_path(&path, 1)).unwrap();
    assert!(
        !witness_path(&path, 1).exists(),
        "premise: witness 1 is missing"
    );
    log.state.lock().namespace_synced_file = None;
    let witnesses = log.state.lock().stats.inventory_witnesses;
    log.persist_appended()
        .unwrap_or_else(|error| panic!("the persist reached a retired file: {error:?}"));
    for file in 0..3 {
        assert!(
            witness_path(&path, file).exists(),
            "the witness of file {file}"
        );
    }
    let written = log.state.lock().stats.inventory_witnesses - witnesses;
    assert_eq!(written, 1, "exactly the missing witness was written");
    assert_eq!(
        log.state
            .lock()
            .namespace_synced_file
            .map(|file| file.as_u64()),
        Some(2),
        "the namespace sync ran for the last file of the log"
    );
    log.seal_live_epoch(authority(&config), || Ok(())).unwrap();
    drop(log);
    let recovered = recover(&path, &config);
    assert!(
        recovered.is_retired(0).unwrap(),
        "premise: the retirement is durable"
    );
}

/// A'7 (c): a file that is retired in the root but still present (its unlink
/// is deferred: the mapper refuses the file that ends at the allocator, so
/// the pass is `Completed`, `cleanup_pending` holds all three files and none
/// is unlinked). The persist skips them by the root, not by their absence:
/// no file sync is issued for a file whose retirement is durable.
///
/// Negative control (mutation run): remove the skip: three file syncs.
#[test]
fn a_retired_file_that_is_still_present_is_not_synced_by_the_appended_persist() {
    let (temp, _config, log) = sealed_stream_at_the_allocator();
    let path = temp.path().join("stream");
    log.quiesce_wal_maps();
    let victims = proofs(&log);
    assert_eq!(victims.len(), 3);
    assert_eq!(
        log.retire_proved_files(&victims, Some(&witness())).unwrap(),
        RetirementOutcome::Completed
    );
    assert_eq!(
        log.file_retirement_state().1.len(),
        3,
        "premise: all three await their unlink"
    );
    for file in 0..3 {
        assert!(
            log.is_retired(file * FILE).unwrap(),
            "premise: file {file} is retired"
        );
        assert!(
            wal_exists(&path, file),
            "premise: file {file} is still present"
        );
    }
    {
        let state = log.state.lock();
        assert_eq!(state.durable_end, 0, "premise: durable_end lags");
        assert!(
            state.appended_end > 2 * FILE && state.appended_end < 3 * FILE,
            "premise: files 0..=2 hold the whole log, {}",
            state.appended_end
        );
    }
    let syncs = log.state.lock().stats.wal_file_syncs;
    log.persist_appended().unwrap();
    let state = log.state.lock();
    assert_eq!(state.durable_end, state.appended_end);
    assert_eq!(
        state.stats.wal_file_syncs, syncs,
        "a retired file is skipped, present or not"
    );
}

/// A'7 (e): a retirement request that arrives while the appended persist is
/// parked (after it captured the root, before it syncs) waits for the
/// `persistence` lock. The persist captured a root in which nothing is
/// retired, so it still syncs the three files, and they must still exist: the
/// retirement unlinks only after the persist returns. The persist and retirement
/// each rendezvous while holding the same lock, without a timed negative check.
///
/// Negative control: remove the retirement's `persistence` lock.
#[test]
fn a_retirement_requested_while_the_appended_persist_is_parked_waits_for_it() {
    let (_temp, _config, log) = sealed_stream_then_the_next_extent_is_leased();
    let log = Arc::new(log);
    let victims = proofs(&log);
    assert_eq!(victims.len(), 3);
    let syncs = log.state.lock().stats.wal_file_syncs;
    let (entered, resume) = log.pause_next_persist();
    let persister = {
        let log = Arc::clone(&log);
        std::thread::spawn(move || log.persist_appended())
    };
    entered
        .recv_timeout(HOOK_WAIT)
        .expect("the persist reached its pause");
    assert!(
        log.persistence.try_lock().is_none(),
        "the parked persist holds `persistence`"
    );
    let (before_lock, continue_retirement) =
        log.pause_live(crate::distributed::storage::LivePausePoint::RetirementBeforePersistence);
    let retirer = {
        let log = Arc::clone(&log);
        std::thread::spawn(move || log.retire_proved_files(&victims, Some(&witness())))
    };
    before_lock
        .recv_timeout(HOOK_WAIT)
        .expect("retirement reached the persistence lock");
    let (retirement_entered, retirement_resume) =
        log.pause_live(crate::distributed::storage::LivePausePoint::RetirementPersistenceLocked);
    continue_retirement.send(()).unwrap();
    resume.send(()).unwrap();
    let persisted = persister.join().unwrap();
    retirement_entered
        .recv_timeout(HOOK_WAIT)
        .expect("retirement reached its locked rendezvous");
    let locked = log.persistence.try_lock().is_none();
    retirement_resume.send(()).unwrap();
    let retired = retirer.join().unwrap();
    assert!(locked, "retirement must hold the persistence lock");
    persisted.expect("the persist synced files that still existed");
    assert_eq!(retired.unwrap(), RetirementOutcome::Completed);
    assert_eq!(
        log.state.lock().stats.wal_file_syncs - syncs,
        3,
        "the persist synced the three files its captured root had not retired"
    );
}
