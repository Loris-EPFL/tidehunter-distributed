//! Stage notes of an accepted replay page.

use super::*;

/// One session entry of a replay page: kind 37, no response yet.
fn page_entry(now: Instant) -> ServerPending {
    ServerPending {
        partition: 2,
        deadline: now + Duration::from_secs(1),
        kind: 37,
        timeout: Duration::from_secs(1),
        charge: 0,
        response_queued: false,
        ready_response: None,
        measured: false,
        output_started: None,
        control_queued: None,
        is_append: false,
        owner_trace: false,
    }
}

#[test]
fn describe_names_each_page_stage() {
    let now = Instant::now();
    let mut stages = ReplayStages::new();
    let mut entry = page_entry(now);
    stages.accept(7);
    let note = stages.describe(7, &entry, now);
    assert!(
        note.contains("did not pass it"),
        "the session stage is named: {note}"
    );
    stages.dispatch(7);
    let note = stages.describe(7, &entry, now);
    assert!(
        note.contains("admitted it") && note.contains("produced no reply"),
        "the owner stage is named: {note}"
    );
    entry.ready_response = Some(Response::Sealed);
    let note = stages.describe(7, &entry, now);
    assert!(
        note.contains("was ready") && note.contains("not queued"),
        "the ready stage is named: {note}"
    );
    entry.ready_response = None;
    entry.response_queued = true;
    let note = stages.describe(7, &entry, now);
    assert!(
        note.contains("was queued") && note.contains("in transit"),
        "the queued stage is named: {note}"
    );
    // The entry lives until the reply retires; the empty table is a no-op.
    stages.complete(7);
    assert_eq!(stages.describe(7, &entry, now), "");
    stages.complete(8);
}
