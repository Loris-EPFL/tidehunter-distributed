//! The production trim path (`trim_generation_online`: prepare under both
//! gates, delete with no gate, finish under both gates). The restart reads only
//! the durable state, which is exactly the state at the crash boundary: every
//! ledger install, unlink and directory removal syncs before it returns.
//! The live-target tests use the journal-only target fixture: they are not
//! evidence of kernel namespace I/O drain.

use super::super::super::generation_control::{
    CheckTrimRequest, OwnerTrimCrash, TrimReadiness, TrimRequest,
};
use super::*;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use crate::distributed::nvmeof::live_storage::LiveWalSegment;
use crate::distributed::tcp::tests::{config, metrics};

fn trim_request(authority_generation: u64, generation: u64) -> Request {
    Request::TrimGeneration(TrimRequest {
        authority_generation,
        generation,
    })
}

fn absent(node: &StorageNode, authority_generation: u64, generation: u64) -> bool {
    let reply = node
        .handle(
            Request::CheckTrimGeneration(CheckTrimRequest {
                authority_generation,
                generation,
            }),
            TcpLimits::default(),
        )
        .unwrap();
    matches!(&reply, Response::CheckTrimGeneration(reply) if reply.result == TrimReadiness::Absent)
}

#[test]
fn online_trim_recovers_from_each_crash_point_and_repeats() {
    for point in [OwnerTrimCrash::AfterLedger, OwnerTrimCrash::AfterUnlink] {
        let temporary = tempdir::TempDir::new("owner-online-trim-crash").unwrap();
        let root = temporary.path().join("node");
        let node = StorageNode::open(config(&root), metrics()).unwrap();
        for epoch in 1..=3 {
            node.handle(Request::Fence { epoch }, TcpLimits::default())
                .unwrap();
        }
        node.fail_next_generation_trim(point);
        let error = node
            .handle(trim_request(3, 2), TcpLimits::default())
            .err()
            .expect("the injected crash must fail the trim");
        assert!(
            error.to_string().contains("injected owner crash"),
            "{point:?}: {error}"
        );
        // The intent is durable at both points; only the first one still has
        // the directory, because the deletion had not begun.
        assert_eq!(
            node.inventory.lock().manifest.trim_pending,
            Some(2),
            "{point:?}"
        );
        assert_eq!(
            epoch_path(&root, 2).exists(),
            point == OwnerTrimCrash::AfterLedger,
            "{point:?}"
        );
        drop(node);

        let node = StorageNode::open(config(&root), metrics()).unwrap();
        assert!(
            !epoch_path(&root, 2).exists(),
            "{point:?}: the open did not finish the deletion"
        );
        assert_eq!(
            node.inventory.lock().manifest.trim_pending,
            None,
            "{point:?}"
        );
        // The coordinator resends the same trim under its next fence.
        node.handle(Request::Fence { epoch: 4 }, TcpLimits::default())
            .unwrap();
        assert!(
            matches!(
                node.handle(trim_request(4, 2), TcpLimits::default()),
                Ok(Response::TrimGeneration(reply)) if reply.generation == 2
            ),
            "{point:?}: the repeated trim failed"
        );
        assert!(absent(&node, 4, 2), "{point:?}");
    }
}

/// An owner with the journal-only live target, reopened from disk on demand.
struct LiveFixture {
    config: NodeConfig,
    configfs: PathBuf,
}

impl LiveFixture {
    fn new(root: &Path) -> Self {
        use crate::distributed::nvmeof::{LiveTargetConfig, NvmeofTransport};
        let configfs = root.join("configfs");
        fs::create_dir(&configfs).unwrap();
        for name in ["hosts", "ports", "subsystems"] {
            fs::create_dir(configfs.join(name)).unwrap();
        }
        let mut config = config(&root.join("node"));
        config.partition.fragment_bytes = 16 * 1024;
        config.partition.wal_file_bytes = 32 * 1024;
        config.live_target = Some(LiveTargetConfig {
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            host_nqn: crate::distributed::tcp::host_identity::effective_host_nqn(
                &config.partition.inventory.database,
                config.partition.partition,
            ),
            port_id: 42,
            state_dir: root.join("target"),
            commissioning_port_id: 43,
            commissioning_service: 4421,
        });
        fs::create_dir(&config.live_target.as_ref().unwrap().state_dir).unwrap();
        Self { config, configfs }
    }

    fn open(&self) -> Arc<StorageNode> {
        StorageNode::bench_open_live_fixture(self.config.clone(), metrics(), self.configfs.clone())
            .unwrap()
    }
}

fn fence(node: &StorageNode, epoch: u64) {
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, epoch, false, |_| Ok(()))
    })
    .unwrap();
}

fn marker_present(node: &StorageNode, segment: &LiveWalSegment) -> bool {
    node.with_live_target(|target| target.sealed_file(&segment.path).unwrap().is_some())
}

/// Generation 1 with one leased WAL file that has a sealed marker, then
/// `lookahead_files` unmarked files after it, then the fence of generation 2.
/// Nothing was written, so the sealed end is zero and every file is lookahead.
fn trimmable_generation(node: &StorageNode, lookahead_files: u64) -> LiveWalSegment {
    fence(node, 1);
    let log = node.inventory.lock().logs.get(&1).unwrap().clone();
    let segment = log
        .lease_live_extent(RequestAuthority { epoch: 1 })
        .unwrap()
        .0
        .segment;
    node.with_live_target(|target| {
        let name = crate::distributed::nvmeof::live_storage::wal_export_name(
            crate::distributed::nvmeof::live_storage::FILES_PER_SUBSYSTEM,
            segment.database,
            segment.partition,
            segment.epoch,
            segment.file_index,
        );
        let binding = target
            .export_permanent(&name, live_file(&segment), false)
            .unwrap();
        target.revoke(&binding).unwrap();
    });
    drop(log);
    fence(node, 2);
    let directory = epoch_path(&node.inventory.lock().config.root, 1);
    for id in present_wal_files(&directory).unwrap() {
        let path = wal_name(&directory, id);
        if path != segment.path {
            fs::remove_file(path).unwrap();
        }
    }
    for offset in 1..=lookahead_files {
        File::create(wal_name(&directory, segment.file_index + offset)).unwrap();
    }
    File::open(&directory).unwrap().sync_all().unwrap();
    assert!(segment.path.exists());
    assert!(marker_present(node, &segment));
    segment
}

#[test]
fn online_trim_recovers_from_each_cut_with_a_live_target_and_real_wal_files() {
    for point in [
        OwnerTrimCrash::AfterLedger,
        OwnerTrimCrash::AfterLookaheadMarkerUnlink,
        OwnerTrimCrash::AfterLookaheadMarker,
        OwnerTrimCrash::AfterLookaheadFileUnlink,
        OwnerTrimCrash::AfterLookaheadUnlink,
        OwnerTrimCrash::AfterUnlink,
    ] {
        let temporary = tempdir::TempDir::new("owner-online-trim-live-cut").unwrap();
        let fixture = LiveFixture::new(temporary.path());
        let node = fixture.open();
        let segment = trimmable_generation(&node, 0);
        node.fail_next_generation_trim(point);
        let error = node
            .handle(trim_request(2, 1), TcpLimits::default())
            .err()
            .expect("the injected crash must fail the trim");
        // The marker release reports its own text; every other cut is owner-side.
        let text = if point == OwnerTrimCrash::AfterLookaheadMarkerUnlink {
            "injected crash before marker directory sync"
        } else {
            "injected owner crash"
        };
        assert!(error.to_string().contains(text), "{point:?}: {error}");
        assert_eq!(
            node.inventory.lock().durable_trim_pending().unwrap(),
            Some(1),
            "{point:?}"
        );
        assert_eq!(
            marker_present(&node, &segment),
            point == OwnerTrimCrash::AfterLedger,
            "marker at {point:?}"
        );
        assert_eq!(
            segment.path.exists(),
            matches!(
                point,
                OwnerTrimCrash::AfterLedger
                    | OwnerTrimCrash::AfterLookaheadMarkerUnlink
                    | OwnerTrimCrash::AfterLookaheadMarker
            ),
            "WAL at {point:?}"
        );
        drop(node);

        let reopened = fixture.open();
        assert!(!segment.path.exists(), "{point:?}: WAL after restart");
        assert!(!marker_present(&reopened, &segment), "{point:?}: marker");
        assert_eq!(
            reopened.inventory.lock().durable_trim_pending().unwrap(),
            None,
            "{point:?}"
        );
        assert_eq!(
            reopened.with_live_target(|target| target.stats().unwrap().namespaces_exposed),
            0,
            "{point:?}"
        );
        assert!(!reopened.inventory.lock().logs.contains_key(&1));
        // The coordinator resends the same trim under its next fence.
        fence(&reopened, 3);
        assert!(
            matches!(
                reopened.handle(trim_request(3, 1), TcpLimits::default()),
                Ok(Response::TrimGeneration(reply)) if reply.generation == 1
            ),
            "{point:?}: the repeated trim failed"
        );
        assert!(absent(&reopened, 3, 1), "{point:?}");
        drop(reopened);

        let again = fixture.open();
        assert!(
            !segment.path.exists(),
            "{point:?}: WAL after second restart"
        );
        assert!(!marker_present(&again, &segment), "{point:?}: marker");
        assert_eq!(
            again.with_live_target(|target| target.stats().unwrap().namespaces_exposed),
            0,
            "{point:?}"
        );
        assert!(!again.inventory.lock().logs.contains_key(&1));
    }
}

/// The deletion holds no gate between two file steps (S6h option C). The trim
/// runs on its own thread and pauses after it deleted the highest of two WAL
/// files. The two gates and the inventory are then free, the intent is on
/// disk, the lower file is still present, and a first grant of the current
/// epoch, which takes the lifecycle gate, completes. The `try_lock` checks state
/// the premise; the grant and its gate-path counter show the effect. A handler
/// that holds a gate through the deletion fails the premise checks. The test
/// calls the control handlers directly: it proves gate release, not queue
/// release. A queued lease renewal still waits behind the exclusive slot
/// (`control_active`).
#[test]
fn grant_completes_between_two_file_steps_of_an_online_trim() {
    use crate::distributed::tcp::inventory::InlineExport;
    use crate::distributed::test_bounds::PROGRESS_CAP;

    let temporary = tempdir::TempDir::new("owner-online-trim-interleave").unwrap();
    let fixture = LiveFixture::new(temporary.path());
    let node = fixture.open();
    let tier = DurabilityTier::Native;
    node.set_durability(DurabilityConfig::with_tier(tier))
        .unwrap();
    let segment = trimmable_generation(&node, 1);
    let upper = wal_name(
        &epoch_path(&node.inventory.lock().config.root, 1),
        segment.file_index + 1,
    );
    assert!(upper.exists(), "the second WAL file is missing");
    let (entered, resume) = node.inventory.lock().pause_trim_after_file_step();
    let trimmer = {
        let node = node.clone();
        std::thread::spawn(move || node.handle(trim_request(2, 1), TcpLimits::default()))
    };
    // The pause sender stays in the inventory when the trim fails early, so
    // the wait is bounded and names that case.
    entered
        .recv_timeout(PROGRESS_CAP)
        .expect("the trim did not reach its first file step");
    assert!(
        !upper.exists(),
        "the first step did not delete the upper file"
    );
    assert!(
        segment.path.exists(),
        "the lower file went with the first step"
    );
    assert_eq!(
        node.inventory.lock().durable_trim_pending().unwrap(),
        Some(1),
        "the intent is not durable at the pause"
    );
    let context = &node.control_context;
    assert!(
        context.live_control_gate.try_lock().is_some(),
        "the live-control gate is held between two file steps"
    );
    assert!(
        context.lifecycle.try_lock().is_some(),
        "the lifecycle gate is held between two file steps"
    );
    assert!(
        node.inventory.try_lock().is_some(),
        "the inventory is held between two file steps"
    );
    let before = node.file_maintenance_stats();
    let (done, finished) = std::sync::mpsc::sync_channel(1);
    let lessee = {
        let node = node.clone();
        std::thread::spawn(move || {
            let leased =
                node.control_context
                    .lease_live_published(2, tier, None, InlineExport::Lease);
            let _ = done.send(leased.is_ok());
        })
    };
    assert_eq!(
        finished.recv_timeout(PROGRESS_CAP),
        Ok(true),
        "a grant waited for the trim deletion or failed"
    );
    let after = node.file_maintenance_stats();
    assert_eq!(
        after.prepared_gate_waits + after.unprepared_gate_waits,
        before.prepared_gate_waits + before.unprepared_gate_waits + 1,
        "the grant did not take the lifecycle gate path: {after:?}"
    );
    lessee.join().unwrap();
    resume.send(()).unwrap();
    let reply = trimmer.join().unwrap();
    assert!(
        matches!(&reply, Ok(Response::TrimGeneration(reply)) if reply.generation == 1),
        "the paused trim did not finish"
    );
    assert!(!segment.path.exists(), "the lower file survived the trim");
    assert!(
        !marker_present(&node, &segment),
        "the marker survived the trim"
    );
    assert!(absent(&node, 2, 1));
    shutdown(&node, 2);
}

/// Seal the epoch and shut the journal-only target down, as the control tests do.
fn shutdown(node: &StorageNode, epoch: u64) {
    for request in [
        Request::Seal { epoch },
        Request::ShutdownLiveTarget { epoch },
    ] {
        let response = node.handle(request, TcpLimits::default()).unwrap();
        if let Response::LiveTargetStats(stats) = response {
            assert!(stats.fixture_only);
            assert_eq!(stats.owned_namespace_count, 0);
        } else {
            assert!(matches!(response, Response::Sealed));
        }
    }
}

/// The sealed branch of `delete_one_file` and `probe_retire` with sealed
/// markers, run on the `Gated` access of the online deletion. No readiness
/// check lets a generation reach the deletion with a data file below its
/// sealed end (the check blocks it as "durability proof"), so this test calls
/// `delete_generation` directly on a crafted state: the SEAL names a nonzero
/// end, which makes the leased file a sealed file, and a marker without its WAL
/// stands for a crash after an unlink. No `OwnerTrimCrash` cut exists for this
/// branch: the four lookahead cuts and `AfterUnlink` sit in the lookahead branch
/// and after the directory removal.
#[test]
fn gated_deletion_retires_sealed_files_and_the_marker_above_the_present_set() {
    use crate::distributed::nvmeof::live_target::LiveFileIdentity;
    use std::os::unix::fs::MetadataExt;

    #[derive(serde::Serialize)]
    struct Seal {
        format: u32,
        epoch: u64,
        generation: u64,
        frame_count: usize,
        physical_end: u64,
    }

    let temporary = tempdir::TempDir::new("owner-gated-sealed-delete").unwrap();
    let fixture = LiveFixture::new(temporary.path());
    let node = fixture.open();
    let segment = trimmable_generation(&node, 0);
    let root = node.inventory.lock().config.root.clone();
    let directory = epoch_path(&root, 1);
    // A marker without its WAL file: the file above the present set.
    let stray = wal_name(&directory, segment.file_index + 1);
    let file = File::create(&stray).unwrap();
    // The export refuses a sparse file: write every block.
    std::io::Write::write_all(&mut &file, &vec![0; segment.bytes as usize]).unwrap();
    file.sync_all().unwrap();
    let metadata = file.metadata().unwrap();
    let identity = LiveFileIdentity {
        path: stray.clone(),
        device: metadata.dev(),
        inode: metadata.ino(),
        bytes: segment.bytes,
        block_size: segment.block_size,
    };
    node.with_live_target(|target| {
        let name = crate::distributed::nvmeof::live_storage::wal_export_name(
            crate::distributed::nvmeof::live_storage::FILES_PER_SUBSYSTEM,
            segment.database,
            segment.partition,
            segment.epoch,
            segment.file_index + 1,
        );
        let binding = target.export_permanent(&name, identity, false).unwrap();
        target.revoke(&binding).unwrap();
    });
    drop(file);
    fs::remove_file(&stray).unwrap();
    // One byte of sealed data puts file 0 below the first lookahead file.
    let seal = Seal {
        format: 5,
        epoch: 1,
        generation: 1,
        frame_count: 0,
        physical_end: 1,
    };
    install_metadata(
        &directory,
        "partition-seal-v5",
        &encode_metadata(&seal).unwrap(),
    )
    .unwrap();
    assert!(marker_present(&node, &segment));
    let stray_marker = |node: &StorageNode| {
        node.with_live_target(|target| target.sealed_file(&stray).unwrap().is_some())
    };
    assert!(stray_marker(&node), "the crashed unlink left no marker");
    let mut access = TrimTargetAccess::Gated {
        inventory: &node.inventory,
        lifecycle: &node.control_context.lifecycle,
    };
    delete_generation(
        &root,
        1,
        fixture.config.partition.wal_file_bytes,
        TrimCrashCuts::default(),
        &mut access,
    )
    .unwrap();
    assert!(!directory.exists(), "the generation directory survived");
    assert!(
        !marker_present(&node, &segment),
        "the sealed file marker survived"
    );
    assert!(
        !stray_marker(&node),
        "the marker above the present set survived"
    );
    // The cumulative `namespaces_exposed` counts the two exports above.
    assert_eq!(
        node.with_live_target(|target| target.stats().unwrap().owned_namespace_count),
        0
    );
}
