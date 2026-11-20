//! Read-trace assertions over the leased live fixture. Every record a test
//! asserts can be shed by the best-effort trace, so each test accounts for
//! missing records with the counted sheds.
use super::super::read_trace::ReadTraceGroup;
use super::*;
use super::{put, reopen_cold, value};
use crate::distributed::remote_db::pipeline_tests::Fixture;

/// Tracing is best effort: a record (call status, group, or event) that meets
/// a held trace lock is shed and counted once in `contention_drops`
/// (read_trace.rs `update`). Owner threads record events while the test
/// thread records the call status and groups, so any record can be shed.
/// Each `(name, count)` is a record kind the test asserts and how many are
/// missing; the total must be covered by counted sheds. A shed may also hit
/// a record the test does not assert, so the bound is `missing <= sheds`, not
/// equality; with no sheds every asserted record must be present.
/// Callers must have `begin` succeed before any owner thread can record,
/// because `begin` is shed on contention too and its `unwrap` would panic.
fn assert_losses_counted(sheds: u64, records: &[(&str, usize)]) {
    let missing: usize = records.iter().map(|(_, count)| count).sum();
    assert!(
        missing as u64 <= sheds,
        "records missing {records:?} with {sheds} counted sheds"
    );
}

fn has_event(group: &ReadTraceGroup, name: &str) -> bool {
    group.events.iter().any(|event| event.name == name)
}

#[test]
fn traced_bulk_call_preserves_owner_and_member_identity() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    original
        .write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let original = reopen_cold(&fixture, original);
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    db.set_read_profile_limit(1);
    let trace = db.begin_read_trace(123, 3).unwrap();
    // Sheds of this call alone (`set_read_profile_limit` armed the trace).
    let sheds_before = db.read_trace_snapshot().contention_drops;
    let keys = [0u32.to_be_bytes(), 1u32.to_be_bytes(), 0u32.to_be_bytes()];
    let values = db
        .traced_get_many_cut(KeySpace(0), &keys, IndexReadCut::Latest, Some(&trace), 0)
        .unwrap();
    assert!(values.iter().all(|value| value.as_ref().unwrap().is_some()));
    trace.finish(true);
    let snapshot = db.read_trace_snapshot();
    let sheds = snapshot.contention_drops - sheds_before;
    assert_eq!(snapshot.calls.len(), 1);
    let call = &snapshot.calls[0];
    assert_eq!((call.caller_id, call.members), (123, 3));
    assert_eq!(call.dropped_children, 0);
    // A shed `finish` leaves the call "incomplete"; nothing else may.
    assert!(
        matches!(call.status, "complete" | "incomplete"),
        "{}",
        call.status
    );
    let shared_group = call.groups.iter().find(|group| group.members == [0, 2]);
    let single_group = call.groups.iter().find(|group| group.members == [1]);
    let groups_lost = u64::from(shared_group.is_none()) + u64::from(single_group.is_none());
    assert_eq!(call.groups.len() as u64 + groups_lost, 2);
    if let (Some(first), Some(second)) = (shared_group, single_group) {
        assert_ne!(first.owner, second.owner);
    }
    let deliveries_lost = call
        .groups
        .iter()
        .filter(|group| !has_event(group, "request_result_delivered"))
        .count();
    assert_losses_counted(
        sheds,
        &[
            ("status", usize::from(call.status != "complete")),
            ("shared group", usize::from(shared_group.is_none())),
            ("single group", usize::from(single_group.is_none())),
            ("request_result_delivered", deliveries_lost),
        ],
    );
    db.shutdown().unwrap();
}

#[test]
fn cancelled_batch_keeps_accepted_flight_until_completion() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&reference.batch_address.partition];
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[node].extend([(0, reference), (1, reference)]);
    let mut batch = ReadBatch::new(&db.core.reads, vec![None; 2], groups).unwrap();
    db.core.reads.state.trace.arm(2);
    let trace = db.core.reads.state.trace.begin(91, 2).unwrap();
    batch
        .dispatch_traced(&db.core.reads, Some(&trace), 0)
        .unwrap();
    let slot = batch.reads[0].1.parts[0].0.slot;
    drop(batch);
    drop(trace);
    assert_eq!(db.core.reads.state.retained.lock().0, 1);
    drop(held);
    assert!(
        db.core.reads.state.flight_ready[slot].wait_until(Instant::now() + Duration::from_secs(5))
    );
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    let snapshot = db.core.reads.state.trace.snapshot();
    assert_eq!(snapshot.calls.len(), 1);
    let call = &snapshot.calls[0];
    assert_eq!(call.caller_id, 91);
    assert_eq!(call.dropped_children, 0);
    // A shed guard drop leaves the call "incomplete"; nothing else may.
    let cancelled = call.status == "cancelled_or_incomplete";
    assert!(cancelled || call.status == "incomplete", "{}", call.status);
    let group = call.groups.iter().find(|group| group.members == [0, 1]);
    assert_eq!(call.groups.len(), usize::from(group.is_some()));
    // `arm` reset the shed counter before this call began.
    assert_losses_counted(
        snapshot.contention_drops,
        &[
            ("status", usize::from(!cancelled)),
            ("group", usize::from(group.is_none())),
            (
                "flight_completed",
                group.map_or(0, |g| usize::from(!has_event(g, "flight_completed"))),
            ),
        ],
    );
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    db.shutdown().unwrap();
}

#[test]
fn traced_fenced_read_records_failure_and_retires_transport() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&reference.batch_address.partition];
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();
    db.core.reads.state.trace.arm(1);
    let trace = db.core.reads.state.trace.begin(92, 1).unwrap();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[node].push((0, reference));
    let mut batch = ReadBatch::new(&db.core.reads, vec![None], groups).unwrap();
    batch
        .dispatch_traced(&db.core.reads, Some(&trace), 0)
        .unwrap();
    let slot = batch.reads[0].1.parts[0].0.slot;
    fixture.fence_owner(node, db.epoch() + 1);
    drop(held);
    assert!(
        db.core.reads.state.flight_ready[slot].wait_until(Instant::now() + Duration::from_secs(5))
    );
    assert!(batch.finish_one().is_err());
    drop(batch);
    trace.finish(false);
    db.core.reads.stop();
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    let snapshot = db.core.reads.state.trace.snapshot();
    assert_eq!(snapshot.calls.len(), 1);
    let call = &snapshot.calls[0];
    assert_eq!(call.caller_id, 92);
    assert_eq!(call.dropped_children, 0);
    // A shed `finish` leaves the call "incomplete"; nothing else may.
    assert!(
        matches!(call.status, "error" | "incomplete"),
        "{}",
        call.status
    );
    let group = call.groups.first();
    assert_eq!(call.groups.len(), usize::from(group.is_some()));
    // `arm` reset the shed counter before this call began.
    assert_losses_counted(
        snapshot.contention_drops,
        &[
            ("status", usize::from(call.status != "error")),
            ("group", usize::from(group.is_none())),
            (
                "flight_error",
                group.map_or(0, |g| usize::from(!has_event(g, "flight_error"))),
            ),
        ],
    );
    assert!(db.shutdown().is_err());
}
