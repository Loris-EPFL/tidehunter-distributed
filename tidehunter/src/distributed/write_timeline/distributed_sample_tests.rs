use super::*;

#[test]
fn stride_retains_calls_across_window_and_stays_bounded() {
    let timeline = WriteTimeline::default();
    assert!(timeline.arm(3, 0).is_err());
    timeline.arm(3, 3).unwrap();
    for id in 0..10 {
        if let Some(sample) = timeline.begin(id) {
            timeline.complete(sample, None);
        }
    }
    let export = timeline.finish().unwrap();
    assert_eq!(
        export
            .records
            .iter()
            .map(|record| record.caller_sample_id)
            .collect::<Vec<_>>(),
        [0, 3, 6]
    );
    assert_eq!(export.seen_calls, 10);
    assert_eq!(export.skipped_calls, 6);
    assert_eq!(export.dropped_calls, 1);
}

#[test]
fn owner_diagnostics_are_window_matched_and_disabled_by_default() {
    let timeline = WriteTimeline::with_owners([2, 7].into_iter());
    timeline.owner_frame(2, 1, || panic!("disabled diagnostics evaluated puts"), true);
    timeline.arm(1, 1).unwrap();
    timeline.owner_frame(2, 2, || 3, true);
    timeline.owner_frame(7, 1, || 1, false);
    timeline.owner_queue(2, WriteOwnerQueue::LeasedFlights, 3);
    let export = timeline.finish().unwrap();
    assert_eq!(export.owner_diagnostics[0].submitted_frames, 1);
    assert_eq!(export.owner_diagnostics[0].submitted_fua_frames, 1);
    assert_eq!(export.owner_diagnostics[0].submitted_batches, 2);
    assert_eq!(export.owner_diagnostics[0].submitted_puts, 3);
    assert_eq!(export.owner_diagnostics[0].queues[0].max_depth, 3);
    assert_eq!(export.owner_diagnostics[1].submitted_frames, 1);
    assert_eq!(export.owner_diagnostics[1].submitted_fua_frames, 0);
    timeline.arm(1, 1).unwrap();
    let export = timeline.finish().unwrap();
    assert_eq!(export.owner_diagnostics[0].submitted_frames, 0);
    assert!(export.owner_diagnostics[0].queues.is_empty());
}
