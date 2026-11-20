//! Local-file LiveIo exercises the production leased worker, not a kernel target.
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
use crate::distributed::tcp::{LeasedSync, LiveClientConfig};
use crate::distributed::test_bounds::PROGRESS_CAP;

const LEASED_BOUNDARIES: &[&str] = &[
    "write_submitted_to_kernel",
    "leased_frame_completed",
    "leased_receipt_released",
    "leased_adoption_submitted",
    "leased_adoption_completed",
    "leased_extent_switched",
    "publication_before",
    "publication_after",
    "checkpoint_staging_before",
    "checkpoint_staging_after",
    "checkpoint_image_before",
    "checkpoint_image_after",
    "checkpoint_metadata_before",
    "checkpoint_metadata_after",
    "checkpoint_root_before",
    "checkpoint_root_after",
    "checkpoint_install_before",
    "checkpoint_install_after",
];
const FLUSH_BOUNDARIES: &[&str] = &["flush_submitted_to_kernel", "leased_flush_completed"];

pub(super) fn profile(index: usize) -> LeasedCase {
    assert!(index < 8);
    LeasedCase {
        synchronous: index & 4 != 0,
        per_frame_fua: index & 2 != 0,
        window_one: index & 1 != 0,
    }
}

pub(super) fn boundaries(case: &Case) -> Vec<&'static str> {
    let mut boundaries = LEASED_BOUNDARIES.to_vec();
    if case.flush_probe() {
        boundaries.extend_from_slice(FLUSH_BOUNDARIES);
    }
    boundaries
}

pub(super) fn configure(config: &mut RemoteDatabaseConfig, profile: LeasedCase) {
    config.storage.batch_limits.max_encoded_bytes = 256 * 1024 - 4096;
    config.storage.group_limits.max_encoded_bytes = 256 * 1024 - 8;
    config.storage.group_limits.max_batches = 1;
    config.storage.fragment_bytes = 256 * 1024;
    config.storage.wal_file_bytes = 8 * 1024 * 1024;
    // Recovery pages need one bounded batch per owner (`recovery_page_limit`):
    // 4 MiB holds up to 15 owners at this batch size; 16 owners need more.
    config.max_pending_bytes = (4 * 1024 * 1024).max(config.storage.partitions.len() * 320 * 1024);
    config.owner_append_occupancy = 4;
    config.live_nvme = LiveClientConfig {
        host_nqn: "nqn.2026-01.test:write-model".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        max_inflight_requests: 8,
        leased_write_window: profile.window_one.then_some(1),
        leased_sync: if profile.per_frame_fua {
            LeasedSync::PerFrameFua
        } else {
            LeasedSync::default()
        },
        leased_flush_min_frame_bytes: LiveClientConfig::default_leased_flush_min_frame_bytes(),
        max_inflight_bytes: 4 * 1024 * 1024,
        connect_timeout_ms: 5000,
        io_timeout_ms: 5000,
        leased_durability: if profile.synchronous {
            DurabilityTier::Synchronous
        } else {
            DurabilityTier::Native
        },
        lease_ahead: true,
        polled_writes: false,
        inline_reads: false,
        queue_geometry: Default::default(),
    };
}

pub(super) fn quiesce(clients: &[Arc<Mutex<Client>>]) {
    for client in clients {
        let mut released = Vec::new();
        client
            .lock()
            .settle_leased_extent(&mut released, None)
            .unwrap();
        assert!(released.is_empty(), "gate has unreplied writes");
    }
}

/// Keep the first large frame pending until the second is admitted. This makes
/// GroupFlush coverage independent of kernel completion speed and RPC grouping.
pub(super) fn flush_probe(
    db: &ConcurrentRemoteDatabase,
    recorder: &Recorder,
    case: &Case,
    operations: &[[u8; 16]],
    plan: &[Vec<Edit>],
    client: &Arc<Mutex<Client>>,
) {
    client.lock().leased_test_hold_next_completion();
    thread::scope(|scope| {
        scope.spawn(|| write_call(db, recorder, case, operations, plan, WAVES * 2 + 2));
        wait_flights(client, 1);
        scope.spawn(|| write_call(db, recorder, case, operations, plan, WAVES * 2 + 3));
        wait_flights(client, 2);
        client.lock().leased_test_release_held();
    });
}

fn wait_flights(client: &Arc<Mutex<Client>>, count: usize) {
    // Progress wait on the in-flight counter.
    let deadline = Instant::now() + PROGRESS_CAP;
    while client.lock().leased_flights_in_flight() < count {
        assert!(
            Instant::now() < deadline,
            "flush probe never admitted {count} frames"
        );
        thread::yield_now();
    }
}

pub(super) fn verify_counts(profile: LeasedCase, metrics: &LiveNvmeMetrics, directory: &Path) {
    let counts = metrics.snapshot().counts;
    fs::write(
        directory.join("live-counts.json"),
        serde_json::to_vec(&counts).unwrap(),
    )
    .unwrap();
    assert!(counts.leased_frames > 0);
    // Includes every LiveIo namespace barrier from process start, even before
    // the crash gate opens, rather than only the GroupFlush policy counter.
    let flushes = write_model::flush_submissions();
    fs::write(directory.join("flush-submissions.txt"), flushes.to_string()).unwrap();
    if profile.synchronous && !profile.per_frame_fua && !profile.window_one {
        assert!(flushes > 0, "GroupFlush submitted no initiator flush");
        assert!(
            counts.leased_flushes > 0,
            "GroupFlush control did not issue a flush"
        );
        assert!(
            counts.leased_flush_samples > 0,
            "GroupFlush control did not finish a flush"
        );
    } else {
        assert_eq!(
            flushes, 0,
            "native/FUA/window-1 submitted an initiator flush"
        );
        assert_eq!(
            counts.leased_flushes, 0,
            "native/FUA/window-1 issued a flush"
        );
    }
}

#[test]
fn write_model_leased_seeded_crashes() {
    for index in 0..8 {
        let case = Case::new(
            0x5a17 + index as u64,
            [1, 2, 4][index % 3],
            index % 2 != 0,
            profile(index),
        );
        run_case(&case);
        for boundary in boundaries(&case) {
            run_case(&Case {
                boundary: boundary.into(),
                ..case.clone()
            });
        }
    }
}

#[test]
fn write_model_leased_reopen_rejects_missing_registry() {
    let mut fixture = Fixture::new_with_root(
        tempdir::TempDir::new("write-model-missing-registry").unwrap(),
        1,
        128,
        KeyShape::new_single(4, 8, KeyType::uniform(8)),
        2,
        |config| configure(config, profile(0)),
        |_| {},
    );
    let root = fixture.startups[0].0.root.clone();
    fixture.stop_owners();
    let registry = root.join("live-target/live-target.json");
    assert!(registry.is_file());
    fs::remove_file(&registry).unwrap();
    let error = fixture.restart_owners().unwrap_err();
    assert!(
        error.to_string().contains("open target state registry"),
        "{error}"
    );
    assert!(
        !registry.exists(),
        "reopen recreated a missing commissioned registry"
    );
}
