use super::*;
use tidehunter::distributed::remote_db::{CheckpointAttribution, GenerationStatus};

fn io(bytes: u64, allocated_bytes: u64) -> FileIo {
    FileIo {
        bytes,
        file_syncs: 1,
        dir_syncs: 1,
        allocated_bytes,
    }
}

fn boundary(name: &'static str, at_ns: u64, root: FileIo, sectors: u64) -> Boundary {
    Boundary {
        name,
        at_ns,
        snapshots: Vec::new(),
        io: CoordinatorIoSnapshot {
            root,
            allocation_accounting: true,
            ..CoordinatorIoSnapshot::default()
        },
        device: vec![json!({"write_sectors": sectors})],
    }
}

fn row(
    requested_at_ns: u64,
    cleanup_end_at_ns: Option<u64>,
    trigger: CheckpointTriggerFlags,
    status: GenerationStatus,
) -> Value {
    let record = CheckpointDiagnostic {
        captured_bytes: Some(10),
        attribution: Some(CheckpointAttribution {
            trigger_flags: trigger,
            requested_at_ns,
            worker_started_at_ns: Some(requested_at_ns + 1),
            capture_start_at_ns: Some(requested_at_ns + 2),
            cleanup_end_at_ns,
            status,
            ..CheckpointAttribution::default()
        }),
        ..CheckpointDiagnostic::default()
    };
    generation_row("db0", &record).unwrap()
}

#[test]
fn dev_t_splits_like_glibc() {
    // nvme0n1p3 on the workstation: 259:10.
    assert_eq!(split_dev((259u64 << 8) | 10), (259, 10));
    // glibc makedev for a major and minor wider than the legacy 8/12 bits.
    let (major, minor) = (0x12345u64, 0xabcdeu64);
    let dev = ((major & 0xffff_f000) << 32)
        | ((major & 0x0fff) << 8)
        | ((minor & 0xffff_ff00) << 12)
        | (minor & 0x00ff);
    assert_eq!(split_dev(dev), (major, minor));
}

#[test]
fn mount_lookup_picks_the_longest_matching_mount_point() {
    let mountinfo = "\
22 1 259:10 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p3 rw,errors=remount-ro
30 22 0:40 / /dev/shm rw,nosuid shared:5 - tmpfs tmpfs rw,size=16G
31 22 259:10 /home /home rw,noatime shared:2 - ext4 /dev/nvme0n1p3 rw
";
    let mount = find_mount(mountinfo, Path::new("/home/user/db/coordinator"), (259, 10)).unwrap();
    assert_eq!(mount["mount_point"], "/home");
    assert_eq!(mount["fs_type"], "ext4");
    assert_eq!(mount["mount_options"], "rw,noatime");
    assert!(
        find_mount(mountinfo, Path::new("/dev/shm/x"), (259, 10))
            .is_some_and(|mount| mount["mount_point"] == "/")
    );
    assert!(find_mount(mountinfo, Path::new("/dev/shm/x"), (8, 1)).is_none());
}

#[test]
fn counter_parsers_read_the_named_fields_only() {
    let stat = "  100 2 300 4 5000 6 70000 8 0 9 10 0 0 0 0 0 0";
    assert_eq!(parse_write_sectors(stat), Some(70_000));
    assert_eq!(parse_write_sectors("1 2 3"), None);
    let io_stat = "8:0 rbytes=1 wbytes=2 rios=3\n259:10 rbytes=4 wbytes=4096 rios=5\n";
    assert_eq!(parse_cgroup_wbytes(io_stat, (259, 10)), Some(4096));
    assert_eq!(parse_cgroup_wbytes(io_stat, (259, 1)), None);
    let meminfo = "MemTotal: 10 kB\nDirty:  3 kB\nWriteback:  0 kB\n";
    assert_eq!(meminfo_bytes(meminfo, "Dirty:"), Some(3072));
    assert_eq!(meminfo_bytes(meminfo, "Writeback:"), Some(0));
}

#[test]
fn device_reconciliation_reports_signed_residual() {
    let start = [json!({"write_sectors": 10}), json!({"write_sectors": 0})];
    let end = [json!({"write_sectors": 30}), json!({"write_sectors": 2})];
    assert_eq!(device_write_delta(&start, &end), Some(22 * 512));
    assert_eq!(device_write_delta(&start, &[json!({}), json!({})]), None);
    let within = reconcile(10_500, Some(11_000));
    assert_eq!(within["within_0_10"], true);
    assert_eq!(within["d_minus_a_bytes"], "500");
    let over = reconcile(12_500, Some(11_000));
    assert_eq!(over["within_0_10"], false);
    assert_eq!(over["d_minus_a_bytes"], "-1500");
    assert!(reconcile(1, Some(0))["unavailable"].is_string());
}

#[test]
fn windows_assign_generations_by_completion_and_io_by_producer_counters() {
    let boundaries = [
        boundary("startup", 0, io(0, 0), 0),
        boundary("timed", 100, io(10, 4096), 8),
        boundary("gate", 200, io(30, 12_288), 32),
        boundary("end", 300, io(30, 12_288), 32),
        boundary("end_synced", 310, io(30, 12_288), 40),
    ];
    let generations = [
        // Started in startup, completed in timed: completed, not contained.
        row(
            50,
            Some(150),
            CheckpointTriggerFlags::PUBLISHED_BATCHES_SOFT,
            GenerationStatus::Cleaned,
        ),
        // Fully inside timed and automatic: contained.
        row(
            120,
            Some(180),
            CheckpointTriggerFlags::GRANTED_BYTES_SOFT,
            GenerationStatus::Cleaned,
        ),
        // Explicit: never counted as automatic cadence.
        row(
            130,
            Some(190),
            CheckpointTriggerFlags::EXPLICIT,
            GenerationStatus::Cleaned,
        ),
        // Failed: assigned by its last boundary (capture start 212).
        row(
            210,
            None,
            CheckpointTriggerFlags::INDEX_SOFT,
            GenerationStatus::Failed,
        ),
    ];
    let windows = windows(&boundaries, &generations);
    assert_eq!(windows.len(), 3);
    let names: Vec<_> = windows
        .iter()
        .map(|window| window["name"].clone())
        .collect();
    assert_eq!(names, ["startup", "timed", "gate"]);
    let timed = &windows[1];
    assert_eq!(timed["generations_completed"], 3);
    assert_eq!(timed["generations_cleaned"], 3);
    assert_eq!(timed["automatic_generations_contained"], 1);
    assert_eq!(timed["coordinator_io"]["root"]["bytes"], 20);
    assert_eq!(timed["coordinator_io"]["total"]["allocated_bytes"], 8192);
    assert_eq!(timed["device_write_bytes"], 24 * 512);
    assert_eq!(timed["device_reconciliation"]["a_bytes"], 8192);
    let gate = &windows[2];
    assert_eq!(gate["generations_failed"], 1);
    assert_eq!(gate["device_write_bytes"], 0);
}

#[test]
fn generation_rows_carry_labels_trigger_names_and_totals() {
    let explicit = row(
        1,
        Some(9),
        CheckpointTriggerFlags::EXPLICIT,
        GenerationStatus::Cleaned,
    );
    assert_eq!(explicit["db"], "db0");
    assert_eq!(explicit["trigger_names"], json!(["explicit"]));
    assert_eq!(explicit["automatic"], false);
    assert_eq!(explicit["completed_at_ns"], 9);
    let soft = row(
        1,
        None,
        CheckpointTriggerFlags::INDEX_SOFT,
        GenerationStatus::Failed,
    );
    assert_eq!(soft["automatic"], true);
    assert_eq!(soft["completed_at_ns"], 3);
    let totals = generation_totals(&[explicit, soft]);
    assert_eq!(
        totals["db0"],
        json!({"cleaned": 1, "failed": 1, "captured_bytes": 20})
    );
}

#[test]
fn device_gate_uses_post_sync_writes_and_fails_closed() {
    let ok = [(true, "collector")];
    // A = 10 KiB allocated; D before sync is 8 KiB, after sync 10.5 KiB.
    let boundaries = [
        boundary("startup", 0, io(0, 0), 0),
        boundary("timed", 100, io(0, 0), 0),
        boundary("end", 300, io(30, 10_240), 16),
        boundary("end_synced", 310, io(30, 10_240), 21),
    ];
    let gate = device_gate(&boundaries, &ok);
    assert_eq!(gate["eligible"], true, "{gate}");
    assert_eq!(gate["span"]["to"], "end_synced");
    assert_eq!(gate["reconciliation"]["d_bytes"], 21 * 512);
    // The same run with a failed sync or another failed check is ineligible.
    let failed = device_gate(&boundaries, &[(false, "syncfs failed")]);
    assert_eq!(failed["eligible"], false);
    assert_eq!(failed["reasons"], json!(["syncfs failed"]));
    // Residual above 10%.
    let mut far = boundaries;
    far[3] = boundary("end_synced", 310, io(30, 10_240), 40);
    let far = device_gate(&far, &ok);
    assert_eq!(far["reasons"], json!(["abs(D-A)/D above 0.10"]));
    // Missing device counter and missing allocation accounting.
    let mut blind = [
        boundary("timed", 100, io(0, 0), 0),
        boundary("end_synced", 310, io(30, 10_240), 21),
    ];
    blind[1].device = vec![json!({"stat_error": "absent"})];
    blind[1].io.allocation_accounting = false;
    let blind = device_gate(&blind, &ok);
    assert_eq!(blind["eligible"], false);
    let reasons = blind["reasons"].as_array().unwrap();
    assert!(reasons.contains(&json!("device write counter unavailable")));
    assert!(reasons.contains(&json!("allocation accounting off or incomplete")));
    assert!(reasons.contains(&json!("no device writes in span")));
}
