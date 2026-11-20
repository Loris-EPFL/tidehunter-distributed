use super::*;

#[test]
fn wait_counters_are_untouched_when_off() {
    let timeline = WriteTimeline::default();
    let window = &timeline.waits.windows[0];
    window.depth.store(9, Ordering::Relaxed);
    window.observations.store(7, Ordering::Relaxed);
    window.depth_sum.store(11, Ordering::Relaxed);
    window.depth_max.store(4, Ordering::Relaxed);
    window.wake_attempts.store(3, Ordering::Relaxed);
    window.wake_coalesced.store(2, Ordering::Relaxed);
    timeline.arm(0, 1).unwrap();
    timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 2);
    timeline.wait_leave(WriteWait::OwnerEnqueueToDequeue, 1);
    timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
    assert_eq!(window.depth.load(Ordering::Relaxed), 9);
    let snapshot = timeline.waits.snapshot();
    let row = &snapshot.windows[0];
    assert_eq!(row.observations, 7);
    assert_eq!(row.depth_sum, 11);
    assert_eq!(row.depth_max, 4);
    assert_eq!(row.wake_attempts, 3);
    assert_eq!(row.wake_coalesced, 2);
    assert!(!snapshot.saturated);
    assert!(timeline.finish().is_err());
}

#[test]
fn wait_counts_follow_entries_exits_and_actual_coalesced_hints() {
    let timeline = WriteTimeline::default();
    timeline.arm(1, 1).unwrap();
    for wait in [
        WriteWait::OwnerEnqueueToDequeue,
        WriteWait::AdmittedToAppendEnqueue,
        WriteWait::HarvestToDurable,
    ] {
        timeline.wait_enter(wait, 2);
        timeline.wait_leave(wait, 1);
        timeline.wait_enter(wait, 2);
        timeline.wait_leave(wait, 3);
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    for wait in [
        WriteWait::OwnerEnqueueToDequeue,
        WriteWait::HarvestToDurable,
    ] {
        for _ in 0..2 {
            let result = sender.try_send(());
            timeline.wait_wake(
                wait,
                matches!(result, Err(std::sync::mpsc::TrySendError::Full(()))),
            );
        }
        receiver.try_recv().unwrap();
    }
    let export = timeline.finish().unwrap();
    assert!(export.write_wait_metrics.enabled);
    assert!(!export.write_wait_metrics.saturated);
    for (index, row) in export.write_wait_metrics.windows.iter().enumerate() {
        assert_eq!(row.observations, 4);
        assert_eq!(row.depth_sum, 8);
        assert_eq!(row.depth_max, 3);
        assert_eq!(row.wake_attempts, if index == 1 { 0 } else { 2 });
        assert_eq!(row.wake_coalesced, if index == 1 { 0 } else { 1 });
        assert_eq!(
            timeline.waits.windows[index].depth.load(Ordering::Relaxed),
            0
        );
    }
}

#[test]
fn wait_counters_saturate_and_flag_the_cap() {
    let timeline = WriteTimeline::default();
    timeline.arm(1, 1).unwrap();
    let window = &timeline.waits.windows[0];
    for counter in [
        &window.depth,
        &window.observations,
        &window.depth_sum,
        &window.wake_attempts,
        &window.wake_coalesced,
    ] {
        counter.store(u64::MAX - 1, Ordering::Relaxed);
    }
    timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
    assert!(timeline.waits.snapshot().saturated);
    timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
    timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
    timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
    let export = timeline.finish().unwrap();
    let row = &export.write_wait_metrics.windows[0];
    assert!(export.write_wait_metrics.saturated);
    assert_eq!(row.observations, u64::MAX);
    assert_eq!(row.depth_sum, u64::MAX);
    assert_eq!(row.depth_max, u64::MAX);
    assert_eq!(row.wake_attempts, u64::MAX);
    assert_eq!(row.wake_coalesced, u64::MAX);
    assert_eq!(window.depth.load(Ordering::Relaxed), u64::MAX);
}

#[test]
fn wait_counters_are_monotone_until_finish_and_reset_on_rearm() {
    let timeline = WriteTimeline::default();
    timeline.arm(1, 1).unwrap();
    timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
    let first = timeline.waits.snapshot();
    timeline.wait_leave(WriteWait::OwnerEnqueueToDequeue, 1);
    timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
    timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, false);
    let export = timeline.finish().unwrap();
    let before = &first.windows[0];
    let after = &export.write_wait_metrics.windows[0];
    assert!(after.observations >= before.observations);
    assert!(after.depth_sum >= before.depth_sum);
    assert!(after.depth_max >= before.depth_max);
    assert!(after.wake_attempts >= before.wake_attempts);
    assert!(after.wake_coalesced >= before.wake_coalesced);
    timeline.wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
    timeline.wait_wake(WriteWait::OwnerEnqueueToDequeue, true);
    assert_eq!(timeline.waits.snapshot().windows[0].observations, 2);
    assert_eq!(timeline.waits.snapshot().windows[0].wake_attempts, 1);
    timeline.arm(1, 1).unwrap();
    let next = timeline.finish().unwrap();
    assert_eq!(next.write_wait_metrics.windows[0].observations, 0);
    assert_eq!(next.write_wait_metrics.windows[0].wake_attempts, 0);
}

#[test]
fn live_group_links_coalesced_calls_and_preserves_failed_control() {
    let timeline = WriteTimeline::default();
    let versions = [
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        BatchVersion {
            epoch: 1,
            sequence: 2,
        },
    ];
    assert!(timeline.live_group(3, versions.into_iter()).is_none());
    timeline.arm(2, 1).unwrap();
    let a = timeline.begin(11).unwrap();
    let b = timeline.begin(12).unwrap();
    timeline.operation(a, [1; 16]);
    timeline.operation(b, [2; 16]);
    timeline.batch([1; 16], versions[0]);
    timeline.batch([2; 16], versions[1]);
    let group = timeline.live_group(3, versions.into_iter()).unwrap();
    timeline.live_group_token(Some(group), [9; 16]);
    timeline.live_group_event(Some(group), WriteTimelineStage::LiveIoStarted);
    timeline.complete(a, Some("control failed".into()));
    timeline.complete(b, Some("control failed".into()));
    let export = timeline.finish().unwrap();
    assert_eq!(export.live_groups[0].batches, versions);
    assert_eq!(export.live_groups[0].reservation_token, Some([9; 16]));
    for record in &export.records {
        assert_eq!(record.error.as_deref(), Some("control failed"));
        assert!(
            record
                .events
                .iter()
                .any(|e| e.stage == WriteTimelineStage::LiveIoStarted)
        );
        assert!(
            !record
                .events
                .iter()
                .any(|e| e.stage == WriteTimelineStage::LiveIoCompleted)
        );
        assert!(
            record
                .events
                .windows(2)
                .all(|e| e[0].elapsed_ns <= e[1].elapsed_ns)
        );
    }
}

#[test]
fn live_group_limit_and_stale_generation_are_explicit() {
    let timeline = WriteTimeline::default();
    let version = BatchVersion {
        epoch: 1,
        sequence: 1,
    };
    timeline.arm(1, 1).unwrap();
    let sample = timeline.begin(1).unwrap();
    timeline.operation(sample, [1; 16]);
    timeline.batch([1; 16], version);
    let stale = timeline.live_group(1, [version].into_iter()).unwrap();
    assert!(timeline.live_group(1, [version].into_iter()).is_none());
    let first = timeline.finish().unwrap();
    assert_eq!(first.dropped_live_groups, 1);
    timeline.arm(1, 1).unwrap();
    let sample = timeline.begin(2).unwrap();
    timeline.operation(sample, [2; 16]);
    timeline.batch([2; 16], version);
    timeline.live_group(1, [version].into_iter()).unwrap();
    timeline.live_group_token(Some(stale), [7; 16]);
    timeline.live_group_event(Some(stale), WriteTimelineStage::LiveIoCompleted);
    let second = timeline.finish().unwrap();
    assert_eq!(second.live_groups[0].reservation_token, None);
    assert_eq!(second.records[0].events.len(), 1);
}

#[test]
fn control_events_are_bounded_and_keep_request_identity() {
    let timeline = WriteTimeline::default();
    let target = ControlTimelineTarget {
        kind: "position",
        epoch: 1,
        extent: None,
        through: Some(1234),
        prefix: None,
    };
    timeline.control_event("submit", 2, 7, target, None);
    timeline.arm(1, 1).unwrap();
    for id in 0..9 {
        timeline.control_event("submit", 2, id, target, None);
    }
    let export = timeline.finish().unwrap();
    assert_eq!(export.control_events.len(), 8);
    assert_eq!(export.dropped_control_events, 1);
    assert_eq!(export.control_events[7].envelope_id, 7);
    assert_eq!(export.control_events[7].target.through, Some(1234));
}

#[test]
fn bounded_events_and_incomplete_calls_are_explicit() {
    let timeline = WriteTimeline::default();
    timeline.arm(1, 1).unwrap();
    let sample = timeline.begin(7).unwrap();
    for _ in 0..MAX_EVENTS {
        timeline.event(sample, WriteTimelineStage::GateAcquired);
    }
    assert!(timeline.begin(8).is_none());
    let export = timeline.finish().unwrap();
    assert_eq!(export.dropped_calls, 1);
    assert_eq!(export.dropped_events, 1);
    assert_eq!(export.incomplete_calls, 1);
    assert!(!export.records[0].completed);
    assert!(export.records[0].batch.is_none());
}

#[test]
fn stale_sample_cannot_mutate_rearmed_session() {
    let timeline = WriteTimeline::default();
    timeline.arm(1, 1).unwrap();
    let stale = timeline.begin(1).unwrap();
    let first = timeline.finish().unwrap();
    assert_eq!(first.incomplete_calls, 1);
    timeline.arm(1, 1).unwrap();
    let current = timeline.begin(2).unwrap();
    timeline.operation(stale, [7; 16]);
    timeline.event(stale, WriteTimelineStage::GateAcquired);
    timeline.complete(stale, Some("stale".into()));
    timeline.operation(current, [8; 16]);
    timeline.complete(current, None);
    let next = timeline.finish().unwrap();
    assert_eq!(next.records[0].caller_sample_id, 2);
    assert_eq!(next.records[0].operation, Some([8; 16]));
    assert!(next.records[0].error.is_none());
    assert_eq!(next.records[0].events.len(), 2);
    assert_eq!(next.incomplete_calls, 0);
}

#[test]
fn expired_finish_retains_collected_owner_page_as_incomplete() {
    let timeline = WriteTimeline::default();
    timeline.arm(1, 1).unwrap();
    let sample = timeline.begin(19).unwrap();
    timeline.complete(sample, None);
    let record = OwnerTimelineRecord {
        owner: 1,
        envelope_id: 7,
        epoch: Some(1),
        success: true,
        error: None,
        error_truncated: false,
        control_queue_ns: Some(2),
        session_admission_wait_ns: Some(1),
        appender_queue_ns: Some(1),
        flusher_queue_ns: None,
        append_barrier_wait_ns: None,
        append_wal_ns: None,
        append_validation_ns: None,
        append_basic_validation_ns: None,
        append_conflict_lookup_ns: None,
        append_geometry_capacity_ns: None,
        append_metadata_reservation_ns: None,
        append_descriptor_preparation_ns: None,
        append_errors: 0,
        append_retries: 0,
        append_persist_ns: None,
        wal_file_persist_ns: None,
        handler_ns: None,
        response_ready_ns: None,
        namespace_persist_ns: None,
        file_persist_calls: 0,
        namespace_persist_calls: 0,
        appended_groups: 0,
        batch_count: Some(1),
        encoded_bytes: Some(4),
        persistence_capture: None,
        ready_append_prefix: None,
    };
    let export = timeline
        .finish_with_owner_until(
            Instant::now(),
            OwnerTimelineCollection {
                records: vec![record],
                dropped: 0,
                incomplete: 0,
                expected: 2,
                captured: 0,
                errors: Vec::new(),
            },
        )
        .unwrap();
    assert!(export.deadline_exceeded);
    assert!(!export.owner_storage_durations_available);
    assert_eq!(export.records[0].caller_sample_id, 19);
    assert_eq!(export.owner_records[0].envelope_id, 7);
    assert_eq!(export.owner_captured_connections, 0);
}
