//! W1 write-resume tests against a local-file leased owner.
//!
//! Every wait is on an observable signal: a reply the fixture queue delivers, a
//! CQE the test harvests, or a counter read after the operation returned. No
//! test sleeps or assumes elapsed time, and every temp tree is unique.

use super::super::leased_tests::{Fixture, owner_read};
use super::super::{Client, TcpError};
use super::LeasedSubmit;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::write_timeline::WriteTimeline;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Instant;

/// One leased write client of `fixture` with the resume enabled.
///
/// The I/O deadline is the resume's own budget, so it is set far above any real
/// wait here: these tests drive the resume logic and must not depend on how
/// long a debug build takes to schedule a thread.
fn resumed_writer(fixture: &Fixture) -> Client {
    let mut client = fixture.writer();
    client.leased_test_resume(true);
    client
        .live_config
        .as_mut()
        .expect("a configured writer")
        .io_timeout_ms = 60_000;
    client
}

fn submit(client: &mut Client, fixture: &Fixture, sequence: u64) -> u64 {
    match client
        .submit_leased_group(
            1,
            vec![fixture.batch(sequence)],
            &WriteTimeline::default(),
            2,
            true,
        )
        .expect("leased submission")
    {
        LeasedSubmit::Submitted(id) => id,
        LeasedSubmit::Deferred(_) => panic!("frame {sequence} was deferred with spare capacity"),
    }
}

/// The owner file's bytes in `[start, end)`, read through the page cache.
fn owned_bytes(client: &Client, start: u64, end: u64) -> Vec<u8> {
    let extent = client.live_leased.current.as_ref().expect("an open extent");
    let path = extent.lease.extent.segment.path.clone();
    let mut bytes = vec![0u8; usize::try_from(end - start).expect("bounded extent")];
    std::fs::File::open(path)
        .expect("owner WAL file")
        .read_exact_at(&mut bytes, start)
        .expect("owner WAL region");
    bytes
}

/// Overwrite `bytes` at `offset` of the owner file, as a lost or torn write does.
fn write_bytes(client: &Client, offset: u64, bytes: &[u8]) {
    let path = client
        .live_leased
        .current
        .as_ref()
        .expect("an open extent")
        .lease
        .extent
        .segment
        .path
        .clone();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("owner WAL file")
        .write_all_at(bytes, offset)
        .expect("tear the owner WAL region");
}

fn counters(fixture: &Fixture) -> crate::distributed::nvmeof::live_metrics::LiveNvmeCounts {
    fixture.metrics.snapshot().counts
}

fn assert_reads_back(
    fixture: &Fixture,
    batch: &crate::distributed::tcp::CompleteBatch,
    receipt: &crate::distributed::tcp::DurableBatch,
) {
    let reference = batch
        .record_reference(receipt.appended().address(), 0)
        .expect("record reference");
    assert_eq!(
        owner_read(fixture, &reference),
        batch.mutations(),
        "batch {} does not read back after the resume",
        batch.version().sequence
    );
}

/// A control-session reset with frames in flight resumes on the same
/// incarnation, rewrites nothing, and every acknowledged batch still reads
/// back.
#[test]
fn session_reset_mid_flight_resumes_without_a_rewrite() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    for sequence in 0..4 {
        submit(&mut client, &fixture, sequence);
    }
    assert_eq!(
        client.leased_flights_in_flight(),
        4,
        "four frames are in flight before the reset"
    );
    // The owner process keeps running and its incarnation does not change.
    client.fail_connection_for_test();
    client
        .progress_session()
        .expect("the session break resumes instead of failing the writer");
    client
        .drain_leased_flights(&mut released, None)
        .expect("the queued frames still complete");
    assert_eq!(released.len(), 4, "every in-flight frame was released");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "exactly one resume attempt");
    assert_eq!(
        counts.resume_rewrites, 0,
        "a session break rewrites nothing"
    );
    assert_eq!(counts.resume_refused, 0, "the resume was not refused");
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// A lost data queue rewrites exactly the frames the owner could not adopt, at
/// their recorded offsets, and restores the identical byte image.
///
/// The tear is what makes the premise deterministic: the owner's opportunistic
/// scan before every control request stops quietly at the first invalid frame,
/// so its frontier is a real adopted prefix below the coordinator's cursor, and
/// the resume must heal the rest.
#[test]
fn data_break_rewrites_only_the_unadopted_suffix() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    // Four frames are submitted, released and adopted by the owner.
    for sequence in 0..4 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the first four frames complete");
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the released prefix");
    let (start, _, written_end, adopted_end) =
        client.leased_test_extent_ends().expect("an open extent");
    assert_eq!(
        written_end, adopted_end,
        "the first four frames are written and adopted"
    );
    // Two more frames are written and released but the owner never adopts them.
    for sequence in 4..6 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the two later frames complete");
    assert_eq!(
        client.leased_test_retained().0,
        2,
        "the two unadopted frames are retained"
    );
    let (_, cursor, _, _) = client.leased_test_extent_ends().expect("an open extent");
    let clean = owned_bytes(&client, start, cursor);
    let retained_layout: Vec<(u64, usize)> = client
        .live_leased
        .retained
        .iter()
        .map(|frame| (frame.start, frame.frame.len()))
        .collect();
    // Tear the first unadopted frame: the owner can prove only the prefix below
    // it, so the resume has to rewrite both unadopted frames.
    let torn = owned_bytes(&client, adopted_end, cursor);
    write_bytes(&client, adopted_end, &vec![0u8; torn.len()]);
    // The next submission loses its queue before any SQE.
    client.leased_test_fail_submit(libc::ENODEV);
    submit(&mut client, &fixture, 6);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the resumed submission completes");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "one resume attempt");
    assert_eq!(counts.resume_rewrites, 1, "one rewrite episode");
    assert_eq!(
        counts.resume_frames, 2,
        "only the frames below the owner's frontier are rewritten"
    );
    assert_eq!(
        client.leased_test_retained().0,
        1,
        "the retried frame is retained until the owner adopts it"
    );
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the recovered suffix");
    assert_eq!(
        client.leased_test_retained(),
        (0, 0),
        "the declaration adopted every rewritten frame"
    );
    assert_eq!(released.len(), 7, "every frame was released once");
    // The rewrite landed at the recorded offsets, in ascending order, and the
    // retried frame followed it.
    let submissions = client.leased_test_write_submissions();
    assert_eq!(
        submissions.len(),
        3,
        "the new queue saw two rewrites and the retried frame"
    );
    let segment = client
        .live_leased
        .current
        .as_ref()
        .expect("an open extent")
        .lease
        .extent
        .segment
        .bytes;
    let expected: Vec<(u64, usize)> = retained_layout
        .iter()
        .map(|(frame_start, len)| (frame_start % segment, *len))
        .collect();
    assert_eq!(
        submissions[..2],
        expected[..],
        "the rewrite reuses the recorded offsets in ascending order"
    );
    assert_eq!(
        submissions[2].0,
        cursor % segment,
        "the retried frame follows the rewrite"
    );
    // Identical bytes: the healed region equals the image before the tear.
    assert_eq!(
        owned_bytes(&client, start, cursor),
        clean,
        "the rewrite is byte-identical at identical offsets"
    );
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// A data break settles the flights the owner had already adopted even when
/// their CQEs were never harvested and no declaration exchange is needed. The
/// old queue is gone, so nothing else can ever complete them, and the release
/// still waits for its own durability gate.
#[test]
fn data_break_settles_owner_adopted_flights_without_their_cqes() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    for sequence in 0..4 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the first four frames complete");
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the released prefix");
    let (start, _, _, adopted_end) = client.leased_test_extent_ends().expect("an open extent");
    // Two frames reach the device, and their CQEs are withheld: the queue
    // reaps them, so the owner can read the bytes, but no flight completes.
    let mut ids = Vec::new();
    for sequence in 4..6 {
        ids.push(submit(&mut client, &fixture, sequence));
    }
    client.leased_test_hold_completions(&ids);
    let settled = released.len();
    // Progress until both CQEs are reaped, which is when their bytes are on
    // disk: each pass submits and harvests, and the held ids are withheld.
    let retire_by = Instant::now() + std::time::Duration::from_secs(30);
    loop {
        client
            .wait_leased_flights(false)
            .expect("the queue progresses");
        client
            .progress_leased_flights(&mut released, None)
            .expect("the CQEs are reaped and withheld");
        let outstanding = client
            .live_writer
            .as_ref()
            .map_or(0, |writer| writer.io.outstanding());
        if outstanding == 0 {
            break;
        }
        assert!(
            Instant::now() < retire_by,
            "the withheld writes did not retire"
        );
    }
    assert_eq!(
        released.len(),
        settled,
        "a withheld completion releases no receipt"
    );
    assert_eq!(
        client.leased_flights_in_flight(),
        2,
        "both frames wait for a completion the coordinator has not received"
    );
    let (_, cursor, _, _) = client.leased_test_extent_ends().expect("an open extent");
    let clean = owned_bytes(&client, start, cursor);
    assert!(
        clean
            .chunks(4096)
            .skip(4)
            .all(|frame| frame.iter().any(|&byte| byte != 0)),
        "both withheld frames are on disk before the probe"
    );
    // The owner adopts the withheld window, and the coordinator deliberately
    // ignores the reply: this is a declaration whose answer was lost with the
    // session, so the owner's frontier is ahead of what the coordinator knows.
    let token = client
        .live_leased
        .current
        .as_ref()
        .expect("an open extent")
        .lease
        .extent
        .token;
    let lost = client
        .adopt_live_extent(1, token, cursor, false, false)
        .expect("the owner adopts the written withheld window");
    assert_eq!(
        lost.adoption.adopted_end, cursor,
        "the owner is ahead of the coordinator's adopted end of {adopted_end}"
    );
    // The queue is lost on the next submission: nothing to rewrite, and the
    // recovered frontier already covers the prefix, so no declaration is sent.
    client.leased_test_fail_submit(libc::ENODEV);
    submit(&mut client, &fixture, 6);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the adopted flights settle and the retried frame completes");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "one resume attempt");
    assert_eq!(counts.resume_rewrites, 0, "nothing needed a rewrite");
    assert_eq!(counts.resume_frames, 0, "no frame was rewritten");
    assert_eq!(
        released.len(),
        7,
        "every frame was released once, in receipt order"
    );
    assert_eq!(
        client.leased_test_retained(),
        (1, 4096),
        "only the retried frame stays retained until the owner adopts it"
    );
    assert_eq!(
        owned_bytes(&client, start, cursor),
        clean,
        "settling an adopted window changes no byte"
    );
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// A different owner incarnation proves the in-memory adoption prefix is gone,
/// so the resume refuses and the database fail-stops.
#[test]
fn new_incarnation_is_refused() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    submit(&mut client, &fixture, 0);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the first frame completes");
    // The owner restarted: a new incarnation, so its adoption state is gone.
    client
        .connected_status
        .as_mut()
        .expect("a connected status")
        .incarnation[0] ^= 0xff;
    client.leased_test_fail_submit(libc::ENODEV);
    let refused = client.submit_leased_group(
        1,
        vec![fixture.batch(1)],
        &WriteTimeline::default(),
        2,
        true,
    );
    let error = match refused {
        Err(error) => error,
        Ok(_) => panic!("a restarted owner cannot be resumed"),
    };
    assert!(
        error.to_string().contains("restarted; reopen required"),
        "unexpected refusal: {error}"
    );
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied,
        "a refused identity gate is not retried"
    );
    let counts = counters(&fixture);
    assert_eq!(
        counts.resume_attempts, 1,
        "the identity gate stops at one attempt"
    );
    assert_eq!(counts.resume_rewrites, 0, "nothing was rewritten");
    assert_eq!(counts.resume_refused, 1, "the refusal is counted");
}

/// A lapsed coordinator lease withdraws the right to write, so the resume
/// refuses with the Phase 2 lapse message and rewrites nothing.
#[test]
fn lapsed_lease_is_refused() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    client.set_lease_validity(Arc::new(
        crate::distributed::remote_db::coordinator_lease::LeaseValidity::expired_for_test(),
    ));
    client.fail_connection_for_test();
    let error = client
        .progress_session()
        .expect_err("a lapsed lease refuses the resume");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::ConnectionAborted,
        "the lapse keeps the Phase 2 error shape"
    );
    assert_eq!(
        error.to_string(),
        "coordinator lease lapsed on owner partition 2",
        "the lapse names the partition"
    );
    let counts = counters(&fixture);
    assert_eq!(counts.resume_rewrites, 0, "a lapse rewrites nothing");
    assert_eq!(counts.resume_refused, 1, "the refusal is counted");
}

/// Retention is a bound, not a queue: above it a submission is deferred, and an
/// adoption releases the budget again.
#[test]
fn retention_backpressure_defers_instead_of_growing() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    submit(&mut client, &fixture, 0);
    let (retained, bytes) = client.leased_test_retained();
    assert_eq!(
        (retained, bytes),
        (1, 4096),
        "one aligned frame is retained"
    );
    // One frame fills the budget, so the next one waits for an adoption.
    client.leased_test_set_resume_retention(bytes);
    let deferred = client
        .submit_leased_group(
            1,
            vec![fixture.batch(1)],
            &WriteTimeline::default(),
            2,
            true,
        )
        .expect("the deferred submission is not an error");
    let LeasedSubmit::Deferred(batches) = deferred else {
        panic!("the retained budget must defer the second frame");
    };
    assert_eq!(
        client.leased_test_retained(),
        (1, bytes),
        "the deferred frame did not grow the retained set"
    );
    client
        .drain_leased_flights(&mut released, None)
        .expect("the retained frame completes");
    client
        .settle_leased_extent(&mut released, None)
        .expect("adoption prunes the retained frame");
    assert_eq!(
        client.leased_test_retained(),
        (0, 0),
        "adoption released the whole retained budget"
    );
    match client
        .submit_leased_group(1, batches, &WriteTimeline::default(), 2, true)
        .expect("the deferred frame is admitted again")
    {
        LeasedSubmit::Submitted(_) => {}
        LeasedSubmit::Deferred(_) => panic!("an adopted budget admits the frame"),
    }
}

/// The submission-time lease gate refuses before any frame is encoded, so no
/// resume episode is spent on a lease that is already withdrawn.
#[test]
fn lapsed_lease_refuses_before_a_resume_episode() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    client.set_lease_validity(Arc::new(
        crate::distributed::remote_db::coordinator_lease::LeaseValidity::expired_for_test(),
    ));
    let result = client.submit_leased_group(
        1,
        vec![fixture.batch(0)],
        &WriteTimeline::default(),
        2,
        true,
    );
    let error: TcpError = match result {
        Err(error) => error,
        Ok(_) => panic!("a lapsed lease refuses the frame before it is encoded"),
    };
    assert_eq!(
        error.to_string(),
        "coordinator lease lapsed on owner partition 2",
        "the submission gate names the lapse"
    );
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 0, "no resume episode ran");
    assert_eq!(counts.resume_rewrites, 0, "nothing was rewritten");
}

/// The retained set is per grant: a frame the owner adopted is dropped even
/// when a later frame of the same extent is still retained.
#[test]
fn retention_is_pruned_by_the_owner_frontier() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    for sequence in 0..2 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("both frames complete");
    assert_eq!(
        client.leased_test_retained().0,
        2,
        "both frames are retained"
    );
    submit(&mut client, &fixture, 2);
    assert_eq!(
        client.leased_test_retained().0,
        3,
        "the third frame is retained with the first two"
    );
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the written prefix");
    assert_eq!(
        client.leased_test_retained(),
        (0, 0),
        "everything the owner adopted left the retained set"
    );
}
