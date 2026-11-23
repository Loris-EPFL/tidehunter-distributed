//! Call-level read-trace assertions on the hybrid bulk path
//! (`ConcurrentRemoteDatabase::traced_get_many`, the entry the Sui adapter
//! uses). The trace is best effort: a record that meets a held trace lock is
//! shed and counted once in `contention_drops` (`read_trace.rs` `update`), so
//! every record the test asserts is accounted for by counted sheds.
use super::super::pending_read::ReadTraceSnapshot;
use super::super::pipeline_tests::Fixture;
use super::*;

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

/// Each `(name, count)` is a record kind the test asserts and how many of them
/// are missing; the total must be covered by counted sheds. A shed may also hit
/// a record the test does not assert, so the bound is `missing <= sheds`, not
/// equality; with no sheds every asserted record must be present. Callers
/// must have `begin` succeed first, because `begin` is shed on contention too.
fn assert_losses_counted(sheds: u64, records: &[(&str, usize)]) {
    let missing: usize = records.iter().map(|(_, count)| count).sum();
    assert!(
        missing as u64 <= sheds,
        "records missing {records:?} with {sheds} counted sheds"
    );
}

/// Names of the call events, in recorded order.
fn event_names(snapshot: &ReadTraceSnapshot) -> Vec<&'static str> {
    snapshot.calls[0]
        .events
        .iter()
        .map(|event| event.name)
        .collect()
}

/// One traced bulk call over keys that live in two owners' value WALs, with a
/// duplicate key: the call keeps its caller identity and member count, every
/// member gets the value of its own key (so a duplicate is answered twice and
/// members do not swap), and each trace record is either present or counted
/// as shed.
#[test]
fn traced_bulk_call_preserves_caller_and_member_identity() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    original
        .write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    db.set_read_profile_limit(1);
    let trace = db.begin_read_trace(123, 3).unwrap();
    // Sheds of this call alone (`set_read_profile_limit` armed the trace).
    let sheds_before = db.read_trace_snapshot().contention_drops;
    let submissions_before = db.live_read_submission_count();
    let keys = [0u32.to_be_bytes(), 1u32.to_be_bytes(), 0u32.to_be_bytes()];
    let values = db
        .traced_get_many(KeySpace(0), &keys, Some(&trace))
        .unwrap();
    let values: Vec<_> = values
        .into_iter()
        .map(|value| value.unwrap().map(|bytes| bytes.to_vec()))
        .collect();
    assert_eq!(
        values,
        vec![Some(vec![7; 32]), Some(vec![8; 32]), Some(vec![7; 32])],
        "every member answers with the value of its own key"
    );
    assert!(
        db.live_read_submission_count() > submissions_before,
        "with no value cache the call fetched its values from the owners"
    );
    trace.finish(true);
    let snapshot = db.read_trace_snapshot();
    let sheds = snapshot.contention_drops - sheds_before;
    assert_eq!(snapshot.calls.len(), 1, "one call, one record");
    let call = &snapshot.calls[0];
    assert_eq!((call.caller_id, call.members), (123, 3));
    assert_eq!(call.dropped_children, 0);
    assert_eq!(call.dropped_events, 0);
    // A shed `finish` leaves the call "incomplete"; nothing else may.
    assert!(
        matches!(call.status, "complete" | "incomplete"),
        "{}",
        call.status
    );
    // The reader exposes no per-owner child trace: the bulk call records its
    // own stage events, in the order the stages run.
    assert!(call.groups.is_empty(), "{:?}", call.groups.len());
    let names = event_names(&snapshot);
    let expected = [
        "api_started",
        "index_resolution_started",
        "value_fetch_started",
    ];
    let mut lost = 0;
    let mut cursor = 0;
    for name in expected {
        match names[cursor..].iter().position(|seen| *seen == name) {
            Some(offset) => cursor += offset + 1,
            None => lost += 1,
        }
    }
    assert_eq!(
        names.len() + lost,
        expected.len(),
        "the call records its stage events only: {names:?}"
    );
    assert_losses_counted(
        sheds,
        &[
            ("status", usize::from(call.status != "complete")),
            ("stage events", lost),
        ],
    );
    db.shutdown().unwrap();
}

/// A bulk call that fails marks its trace `error`, not `complete`, and keeps
/// the stage events recorded before the failure.
#[test]
fn traced_bulk_call_failure_marks_the_call_as_error() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    db.set_read_profile_limit(1);
    let trace = db.begin_read_trace(124, 1).unwrap();
    let sheds_before = db.read_trace_snapshot().contention_drops;
    // A poisoned read authority is the terminal fail-stop state: the bulk
    // call fails before any index lookup.
    db.reads.poison();
    let result = db.traced_get_many(KeySpace(0), &[0u32.to_be_bytes()], Some(&trace));
    assert!(result.is_err(), "a poisoned read authority fails the call");
    trace.finish(result.is_ok());
    let snapshot = db.read_trace_snapshot();
    let sheds = snapshot.contention_drops - sheds_before;
    let call = &snapshot.calls[0];
    assert_eq!(call.caller_id, 124);
    // A shed `finish` leaves the call "incomplete"; nothing else may.
    assert!(
        matches!(call.status, "error" | "incomplete"),
        "{}",
        call.status
    );
    assert_losses_counted(sheds, &[("status", usize::from(call.status != "error"))]);
    assert!(
        !event_names(&snapshot).contains(&"value_fetch_started"),
        "no value was fetched after the failure"
    );
    assert!(db.shutdown().is_err());
}
