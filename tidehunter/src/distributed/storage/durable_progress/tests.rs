use super::*;

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 3, sequence }
}

#[test]
fn synced_tail_moves_only_on_publication_and_never_backwards() {
    let mut tail = ProgressTail::default();
    tail.appended(version(1));
    let first = tail.capture();
    tail.appended(version(4));
    assert_eq!(tail.snapshot(10).synced, None);
    tail.synced(first, 10);
    let report = tail.snapshot(20);
    assert_eq!(
        (report.synced, report.appended, report.synced_end),
        (Some(version(1)), Some(version(4)), 10)
    );
    assert_eq!(report.generation, 1);
    assert!(!report.idle());
    tail.synced(Some(version(0)), 10);
    assert_eq!(tail.snapshot(20).synced, Some(version(1)));
    assert_eq!(tail.snapshot(20).generation, 1);
    let second = tail.capture();
    tail.synced(second, 20);
    let report = tail.snapshot(20);
    assert_eq!(report.synced, Some(version(4)));
    assert!(report.idle());
    assert_eq!(report.generation, 2);
}

#[test]
fn recovered_tail_reports_its_end_without_versions() {
    let tail = ProgressTail::recovered(4096);
    let report = tail.snapshot(4096);
    assert_eq!(
        (report.appended, report.synced, report.synced_end),
        (None, None, 4096)
    );
    assert!(report.idle());
    assert_eq!(report.generation, 0);
}
