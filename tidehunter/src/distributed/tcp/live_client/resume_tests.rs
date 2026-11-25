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

/// The bytes of the owner file at `path` in `[start, end)`, read through the
/// page cache.
fn file_bytes(path: &std::path::Path, start: u64, end: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; usize::try_from(end - start).expect("bounded extent")];
    std::fs::File::open(path)
        .expect("owner WAL file")
        .read_exact_at(&mut bytes, start)
        .expect("owner WAL region");
    bytes
}

fn owned_bytes(client: &Client, start: u64, end: u64) -> Vec<u8> {
    let extent = client.live_leased.current.as_ref().expect("an open extent");
    file_bytes(&extent.lease.extent.segment.path, start, end)
}

/// Overwrite `bytes` at `offset` of the owner file at `path`.
fn write_file_bytes(path: &std::path::Path, offset: u64, bytes: &[u8]) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("owner WAL file")
        .write_all_at(bytes, offset)
        .expect("tear the owner WAL region");
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
    write_file_bytes(&path, offset, bytes);
}

/// The bytes of `path` in the absolute extent range `[start, end)`. A file
/// holds whole extents, so the file offset is the absolute start modulo the
/// segment length.
fn extent_bytes(path: &std::path::Path, start: u64, end: u64, segment_bytes: u64) -> Vec<u8> {
    let begin = start % segment_bytes;
    file_bytes(path, begin, begin + (end - start))
}

/// Overwrite `bytes` at the absolute extent start `start` of `path`.
fn write_extent_bytes(path: &std::path::Path, start: u64, segment_bytes: u64, bytes: &[u8]) {
    write_file_bytes(path, start % segment_bytes, bytes);
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

/// The episode budget bounds every step, not only the reconnect: a spent
/// deadline refuses a probe, a declaration and a queue open, and a capped
/// exchange restores the session timeout it borrowed.
#[test]
fn resume_budget_bounds_every_step_and_is_restored() {
    use std::time::Duration;
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let session_timeout = client.timeout;
    let connect_timeout = client
        .live_config
        .as_ref()
        .expect("a configured writer")
        .connect_timeout_ms;
    let spent = Instant::now() - Duration::from_millis(1);
    // A spent deadline refuses before the wire, and restores what it touched.
    let error = client
        .remaining_resume(spent)
        .expect_err("a spent budget refuses the next step");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "a spent budget is a typed timeout"
    );
    assert_eq!(
        error.to_string(),
        "live write resume budget expired",
        "the refusal names the budget"
    );
    assert!(
        client
            .within_resume_budget(spent, |client| client.progress_session())
            .is_err(),
        "a spent budget never reaches an exchange"
    );
    assert_eq!(client.timeout, session_timeout, "the timeout is restored");
    let leased = client
        .live_leased
        .current
        .as_ref()
        .map(|extent| extent.lease.clone());
    assert!(
        leased.is_none(),
        "no extent exists before a lease, so the open guard is the only step"
    );
    // A live deadline caps the exchange and restores the session timeout.
    let budget = Instant::now() + Duration::from_millis(100);
    let capped = client
        .within_resume_budget(budget, |client| Ok(client.timeout))
        .expect("a live budget admits the step");
    assert!(
        capped <= Duration::from_millis(100),
        "the exchange borrows at most the remaining budget, saw {capped:?}"
    );
    assert_eq!(client.timeout, session_timeout, "the timeout is restored");
    let wide = Instant::now() + Duration::from_secs(60);
    let unchanged = client
        .within_resume_budget(wide, |client| Ok(client.timeout))
        .expect("a wide budget admits the step");
    assert_eq!(
        unchanged, session_timeout,
        "a budget wider than the session timeout changes nothing"
    );
    assert_eq!(
        client
            .live_config
            .as_ref()
            .expect("a configured writer")
            .connect_timeout_ms,
        connect_timeout,
        "no open ran, so the connect timeout is untouched"
    );
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

/// A writer whose frames are not per-frame FUA, so the group flush is the
/// acknowledgement point for every frame after the first.
///
/// The `native` tier is always durable at completion (its checkpoint is the
/// owner's page cache), so only the `synchronous` tier can leave a frame
/// waiting for the covering flush.
fn flush_writer(fixture: &Fixture) -> Client {
    let mut client = resumed_writer(fixture);
    client.leased_test_group_flush();
    client
        .live_config
        .as_mut()
        .expect("a configured writer")
        .leased_flush_min_frame_bytes = 0;
    client
}

/// An fsync data error is the filesystem's verdict, not a lost link: the flush
/// CQE carries `EIO`, the resume never runs, and the writer fail-stops.
#[test]
fn failed_flush_eio_is_not_resumed() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 64 * 1024);
    let mut client = flush_writer(&fixture);
    let mut released = Vec::new();
    submit(&mut client, &fixture, 0);
    submit(&mut client, &fixture, 1);
    client.leased_test_fail_flush(-libc::EIO);
    let error = client
        .drain_leased_flights(&mut released, None)
        .expect_err("a failed fsync is sticky");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::Other,
        "a media error is not a transient break: {error}"
    );
    assert!(
        error.to_string().contains("errno 5"),
        "the refusal names the flush errno: {error}"
    );
    assert!(
        client.live_write_failed,
        "the writer fail-stops after a failed flush"
    );
    let counts = counters(&fixture);
    assert_eq!(
        counts.resume_attempts, 0,
        "a flush data error is never retried"
    );
    assert_eq!(counts.resume_rewrites, 0, "nothing was rewritten");
}

/// A session break while a group flush is in flight keeps the queue, so
/// `flushed_through_seq` is not reset and the flush CQE is still harvested.
#[test]
fn pending_flush_survives_a_session_break() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 64 * 1024);
    let mut client = flush_writer(&fixture);
    let mut released = Vec::new();
    submit(&mut client, &fixture, 0);
    submit(&mut client, &fixture, 1);
    let retire_by = Instant::now() + std::time::Duration::from_secs(30);
    while client.leased_test_flush_state().1.is_none() {
        assert!(
            Instant::now() < retire_by,
            "the group flush was never submitted"
        );
        client
            .wait_leased_flights(false)
            .expect("the queue progresses");
        client
            .progress_leased_flights(&mut released, None)
            .expect("the completions are harvested");
    }
    let (flushed, flush_id) = client.leased_test_flush_state();
    assert_eq!(
        flushed, 0,
        "no frame is proven flushed before the flush CQE"
    );
    client.fail_connection_for_test();
    client
        .progress_session()
        .expect("the session break resumes on the same incarnation");
    let (after, id) = client.leased_test_flush_state();
    assert_eq!(
        after, 0,
        "a kept queue keeps its flush coverage across a session break"
    );
    assert_eq!(id, flush_id, "the same flush is still awaited");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "one resume episode");
    assert_eq!(
        counts.resume_rewrites, 0,
        "a session break rewrites nothing"
    );
    client
        .drain_leased_flights(&mut released, None)
        .expect("the pending flush CQE still releases its frame");
    assert!(
        client.leased_test_flush_state().0 > 0,
        "the kept flush completed and covered the frame"
    );
    assert_eq!(released.len(), 2, "both frames were released once");
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// A deadline that expires after a probe completed no frame is not probed a
/// second time: the queue is detached and the retained suffix rewritten
/// (design 7.3). The new queue proves the detach, because it reaped nothing.
#[test]
fn timeout_probe_escalates_to_the_data_path_when_the_queue_stalls() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    // Both expiries this test reports are deadlines it sets: the flight's own
    // below, and the probe's retimed window here. The window is therefore
    // empty, so the escalation is observed on the next pass rather than by
    // waiting it out, and the resume keeps the suite's wide hang guard instead
    // of a short budget a parallel debug run can outrun.
    client.leased_test_set_retime_window(std::time::Duration::ZERO);
    // The frame reaches the device, and its CQE is reaped but never delivered:
    // the queue stalls with a frame outstanding and nothing to harvest.
    client.leased_test_hold_next_completion();
    submit(&mut client, &fixture, 0);
    let retire_by = Instant::now() + std::time::Duration::from_secs(30);
    while client.leased_test_reaped_writes() == 0 {
        assert!(
            Instant::now() < retire_by,
            "the withheld write did not reach the completion queue"
        );
        let writer = client.live_writer.as_mut().expect("a bound queue");
        let mut completions = Vec::new();
        writer
            .io
            .progress_into(&mut completions)
            .expect("the queue reaps the write");
    }
    assert_eq!(
        client.leased_flights_in_flight(),
        1,
        "the withheld completion leaves the flight outstanding"
    );
    // Expire the flight outright: the first pass probes, and the probe's
    // retimed window is already spent, which is the escalation.
    client
        .live_leased
        .flights
        .front_mut()
        .expect("one outstanding flight")
        .deadline = Instant::now();
    let mut released = Vec::new();
    client
        .drain_leased_flights(&mut released, None)
        .expect("the escalation detaches the stalled queue and settles the frame");
    let counts = counters(&fixture);
    assert_eq!(
        counts.resume_attempts, 2,
        "one timeout probe, then one data resume"
    );
    // Whether the data resume had to resend the frame depends on how far the
    // owner's own scan got: the probe may already have proved it written (the
    // design's `ControlOnly` outcome) or the rewrite may still owe it. Both are
    // correct; what the second resume must not do is probe a third time.
    assert!(
        counts.resume_rewrites <= 1,
        "at most one rewrite episode: {}",
        counts.resume_rewrites
    );
    assert_eq!(released.len(), 1, "the frame was released once");
    assert_reads_back(&fixture, &fixture.batch(0), &released[0].1[0]);
}

/// A rewritten synchronous frame carries the FUA its original submission
/// recorded, so a resent receipt still implies the barrier (design 8).
#[test]
fn fua_frame_stays_fua_on_resend() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    // The fixture requests per-frame FUA, so a synchronous frame's
    // acknowledgement point is its own barrier rather than a group flush.
    let mut released = Vec::new();
    for sequence in 0..2 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the two FUA frames complete");
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the FUA prefix");
    assert_eq!(
        client.leased_test_write_syncs(),
        vec![true, true],
        "both original submissions carried FUA"
    );
    let (_, cursor, _, adopted_end) = client.leased_test_extent_ends().expect("an open extent");
    assert_eq!(adopted_end, cursor, "the FUA prefix is written and adopted");
    for sequence in 2..4 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the two later FUA frames complete");
    assert_eq!(
        client.leased_test_retained().0,
        2,
        "the two unadopted frames are retained for a resend"
    );
    let (_, cursor, _, _) = client.leased_test_extent_ends().expect("an open extent");
    // Tearing the first unadopted frame keeps the owner's frontier below it, so
    // the resume must resend both frames instead of declaring them adopted.
    let torn = owned_bytes(&client, adopted_end, cursor);
    write_bytes(&client, adopted_end, &vec![0u8; torn.len()]);
    client.leased_test_fail_submit(libc::ENODEV);
    submit(&mut client, &fixture, 4);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the resumed submission completes");
    assert_eq!(
        client.leased_test_write_syncs(),
        vec![true, true, true],
        "the two resends and the retried frame all carry FUA"
    );
    let counts = counters(&fixture);
    assert_eq!(counts.resume_rewrites, 1, "one rewrite episode");
    assert_eq!(counts.resume_frames, 2, "both unadopted frames were resent");
    assert_eq!(released.len(), 5, "every frame was released once");
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// An owner that refuses adoption because its own scan hit a torn frame is
/// terminal for this epoch: the resume refuses once and never retries.
#[test]
fn poisoned_owner_is_refused_without_retry() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    for sequence in 0..2 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the first two frames complete");
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the first prefix");
    for sequence in 2..4 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the two later frames complete");
    let (_, cursor, _, adopted_end) = client.leased_test_extent_ends().expect("an open extent");
    // The explicit adoption below scans this window; tear its second frame so
    // the owner's own scan stops on an invalid frame and it poisons itself.
    write_bytes(&client, adopted_end + 4096, &vec![0u8; 4096]);
    let token = client
        .live_leased
        .current
        .as_ref()
        .expect("an open extent")
        .lease
        .extent
        .token;
    let poisoned = client
        .adopt_live_extent(1, token, cursor, false, false)
        .expect_err("a torn frame in an explicit adoption poisons the owner");
    assert!(
        poisoned.to_string().contains("incomplete live frame"),
        "the owner reported the invalid frame, not a transport error: {poisoned}"
    );
    // The queue is lost next; the rebuilt session's handshake sees the owner is
    // no longer recovery-clean and the resume refuses without a retry.
    client.leased_test_fail_submit(libc::ENODEV);
    let refused = client.submit_leased_group(
        1,
        vec![fixture.batch(4)],
        &WriteTimeline::default(),
        2,
        true,
    );
    let error = match refused {
        Err(error) => error,
        Ok(_) => panic!("a poisoned owner cannot be resumed"),
    };
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied,
        "the recovery-clean gate is terminal: {error}"
    );
    assert!(
        error.to_string().contains("reopen required"),
        "the refusal names the repair: {error}"
    );
    let counts = counters(&fixture);
    assert_eq!(
        counts.resume_attempts, 1,
        "the recovery-clean gate stops at one attempt"
    );
    assert_eq!(counts.resume_refused, 1, "the refusal is counted");
    assert_eq!(counts.resume_rewrites, 0, "nothing was rewritten");
}

/// A link that flaps without carrying a frame cannot live forever: three
/// resumes with no adopted frame between are the budget, and the fourth
/// refuses and fail-stops.
#[test]
fn second_break_exhausts_the_budget() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    for expected in 1..=2 {
        client.fail_connection_for_test();
        client
            .progress_session()
            .expect("the break resumes while the budget lasts");
        assert_eq!(
            counters(&fixture).resume_attempts,
            expected,
            "each break is one episode"
        );
    }
    client.fail_connection_for_test();
    client
        .progress_session()
        .expect("the third consecutive resume is still inside the budget");
    assert_eq!(
        counters(&fixture).resume_attempts,
        3,
        "three resumes without an adopted frame"
    );
    client.fail_connection_for_test();
    let error = client
        .progress_session()
        .expect_err("the fourth consecutive resume is refused");
    assert!(
        error.to_string().contains("3 consecutive resumes"),
        "the refusal names the budget: {error}"
    );
    assert_eq!(
        counters(&fixture).resume_attempts,
        3,
        "a refused resume is not an attempt"
    );
    assert_eq!(
        counters(&fixture).resume_refused,
        1,
        "the refusal is counted"
    );
}

/// A reaper that cannot take the detached queue is a capacity condition, not a
/// break: the resume fails once, the queue waits for the shutdown drain, and no
/// later detach can replace it.
#[test]
fn reaper_full_is_refused_without_a_retry() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    // A rendezvous channel never accepts a send without a waiting receiver, so
    // every `try_send` reports a full reaper.
    let (sender, _receiver) = std::sync::mpsc::sync_channel(0);
    client.set_resume_writes(true, Some(sender));
    client.leased_test_fail_submit(libc::ENODEV);
    let refused = client.submit_leased_group(
        1,
        vec![fixture.batch(0)],
        &WriteTimeline::default(),
        2,
        true,
    );
    let error = match refused {
        Err(error) => error,
        Ok(_) => panic!("a full reaper fails the resume"),
    };
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::Other,
        "reaper saturation is not a transient break: {error}"
    );
    assert!(
        error.to_string().contains("reaper cannot retire"),
        "the refusal names the reaper: {error}"
    );
    assert!(
        client.live_write_failed,
        "the writer fail-stops on a parked queue"
    );
    assert_eq!(
        counters(&fixture).resume_attempts,
        1,
        "a full reaper is not retried"
    );
    // A second detach must not replace the parked queue: the resume refuses
    // before it takes anything, so nothing is dropped on this thread.
    let again = client
        .resume_after_break(super::resume::BreakKind::Data)
        .expect_err("a parked queue refuses another detach");
    assert!(
        again.to_string().contains("already parked"),
        "the second detach is refused: {again}"
    );
    assert!(
        client.live_leased.undrained.is_some(),
        "the first detached queue is still parked"
    );
    let mut parked = client
        .take_failed_live_writer()
        .expect("the parked queue is handed to the shutdown drain");
    assert!(
        client.live_leased.undrained.is_none(),
        "taking the parked queue clears the slot"
    );
    parked
        .drain_failed()
        .expect("the fixture queue's bounded drain completes inline");
}

/// One extent of the owner WAL, as the tests name it.
struct ExtentSnapshot {
    token: [u8; 16],
    start: u64,
    end: u64,
    cursor: u64,
    path: std::path::PathBuf,
    segment_bytes: u64,
}

fn snapshot(extent: &super::ExtentState) -> ExtentSnapshot {
    ExtentSnapshot {
        token: extent.lease.extent.token,
        start: extent.lease.extent.start,
        end: extent.lease.extent.end,
        cursor: extent.cursor,
        path: extent.lease.extent.segment.path.clone(),
        segment_bytes: extent.lease.extent.segment.bytes,
    }
}

fn closing_snapshot(client: &Client) -> ExtentSnapshot {
    snapshot(
        client
            .live_leased
            .closing
            .as_ref()
            .expect("an extent is closing"),
    )
}

fn current_snapshot(client: &Client) -> ExtentSnapshot {
    snapshot(client.live_leased.current.as_ref().expect("an open extent"))
}

/// Fill the first file through two extents, switch to the second file, and
/// write `extra` frames into the new current extent. Returns the closing extent
/// (four written frames, not closed at the owner) and the current one.
fn rollover_with_current_frames(
    fixture: &Fixture,
    client: &mut Client,
    released: &mut Vec<(u64, Vec<crate::distributed::tcp::DurableBatch>)>,
    extra: u64,
) -> (ExtentSnapshot, ExtentSnapshot) {
    // Four 4 KiB frames fill one 16 KiB extent, so eight fill the first 32 KiB
    // file through two extents.
    for sequence in 0..8 {
        submit(client, fixture, sequence);
        client
            .drain_leased_flights(released, None)
            .expect("each first-file frame completes");
    }
    // The ninth frame does not fit, so its submission switches extents: the
    // second extent of the first file becomes closing and the second file
    // becomes current.
    submit(client, fixture, 8);
    client
        .drain_leased_flights(released, None)
        .expect("the first frame of the second file completes");
    for sequence in 9..9 + extra {
        submit(client, fixture, sequence);
        client
            .drain_leased_flights(released, None)
            .expect("each second-file frame completes");
    }
    (closing_snapshot(client), current_snapshot(client))
}

/// A break while one extent is closing resumes the closing extent first, then
/// the current one: the closing grant is probed and closed before the extent
/// that replaced it, every resend is byte-identical at its recorded offset, and
/// both extents reach the frontier their cursor proves.
#[test]
fn closing_extent_is_resumed_then_current() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    let (closing, current) = rollover_with_current_frames(&fixture, &mut client, &mut released, 2);
    assert_eq!(
        current.start, closing.end,
        "the new extent starts where the closing one ends"
    );
    assert_eq!(
        closing.cursor, closing.end,
        "the closing extent holds four written frames"
    );
    assert_eq!(
        client.leased_test_retained().0,
        7,
        "the four closing frames and the three current frames wait for an adoption"
    );
    // The pre-break image of both extents, then a tear of both unadopted tails,
    // so only a byte-identical resend can restore what the coordinator
    // acknowledged.
    let clean_closing = extent_bytes(
        &closing.path,
        closing.start,
        closing.cursor,
        closing.segment_bytes,
    );
    let clean_current = extent_bytes(
        &current.path,
        current.start,
        current.cursor,
        current.segment_bytes,
    );
    write_extent_bytes(
        &closing.path,
        closing.start,
        closing.segment_bytes,
        &vec![0u8; usize::try_from(closing.cursor - closing.start).expect("bounded extent")],
    );
    write_extent_bytes(
        &current.path,
        current.start,
        current.segment_bytes,
        &vec![0u8; usize::try_from(current.cursor - current.start).expect("bounded extent")],
    );
    client.leased_test_fail_submit(libc::ENODEV);
    submit(&mut client, &fixture, 11);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the resumed submission completes");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "one resume attempt");
    assert_eq!(counts.resume_rewrites, 1, "one rewrite episode");
    assert_eq!(
        counts.resume_frames, 7,
        "the four closing frames and the three current frames are resent"
    );
    assert_eq!(
        client.leased_test_resume_targets(),
        vec!["closing", "current"],
        "the resume probes the closing extent before the current one"
    );
    assert!(
        client.live_leased.closing.is_none(),
        "the resume closed the closing extent and retired it"
    );
    let rewrites = client.leased_test_rewrites();
    assert_eq!(rewrites.len(), 7, "every unadopted frame was resent once");
    let (closing_rewrites, current_rewrites) = rewrites.split_at(4);
    let mut expected = closing.start;
    for (token, start, len, _) in closing_rewrites {
        assert_eq!(
            *token, closing.token,
            "the first resends belong to the closing extent"
        );
        assert_eq!(
            *start, expected,
            "the closing resends ascend from its start"
        );
        assert_eq!(*len, 4096, "each resent frame is one aligned block");
        expected += u64::try_from(*len).expect("bounded frame");
    }
    assert_eq!(
        expected, closing.cursor,
        "the closing extent's resends cover its whole written prefix"
    );
    let mut expected = current.start;
    for (token, start, len, _) in current_rewrites {
        assert_ne!(
            *token, closing.token,
            "the later resends belong to the current extent"
        );
        assert_eq!(
            *start, expected,
            "the current resends ascend from its start"
        );
        assert_eq!(*len, 4096, "each resent frame is one aligned block");
        expected += u64::try_from(*len).expect("bounded frame");
    }
    assert_eq!(
        expected, current.cursor,
        "the current extent's resends cover its whole written prefix"
    );
    // Byte-identical: the tear is healed over the acknowledged image.
    assert_eq!(
        extent_bytes(
            &closing.path,
            closing.start,
            closing.cursor,
            closing.segment_bytes
        ),
        clean_closing,
        "the closing extent is resent byte-identically at its own offsets"
    );
    assert_eq!(
        extent_bytes(
            &current.path,
            current.start,
            current.cursor,
            current.segment_bytes
        ),
        clean_current,
        "the current extent is resent byte-identically at its own offsets"
    );
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the current extent's prefix");
    let (_, current_cursor, _, current_adopted) =
        client.leased_test_extent_ends().expect("an open extent");
    assert_eq!(
        current_adopted, current_cursor,
        "the current extent is adopted through its cursor"
    );
    assert_eq!(
        client.leased_test_retained(),
        (0, 0),
        "nothing is left to resend"
    );
    let owner = fixture.node.log_for_test(1);
    let adopted = owner.receipt_page(0, 32).expect("the owner's receipt page");
    assert_eq!(adopted.len(), 12, "twelve frames, one receipt each");
    for (index, receipt) in adopted.iter().enumerate() {
        assert_eq!(
            receipt.version().sequence,
            index as u64,
            "receipt {index} is the {index}th batch: no gap and no duplicate"
        );
    }
    assert_eq!(released.len(), 12, "every frame was released once");
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// The same resume when the owner applied the closing extent's close and its
/// reply died with the old session. The closing probe must accept the closed
/// summary, retire the extent, and rewrite only the current extent's unadopted
/// frames.
#[test]
fn closing_extent_with_a_lost_close_reply_is_resumed_then_current() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    let (closing, current) = rollover_with_current_frames(&fixture, &mut client, &mut released, 2);
    let clean_closing = extent_bytes(
        &closing.path,
        closing.start,
        closing.cursor,
        closing.segment_bytes,
    );
    let clean_current = extent_bytes(
        &current.path,
        current.start,
        current.cursor,
        current.segment_bytes,
    );
    // The owner applies the close of the closing extent; the reply is received
    // here and dropped, as a session break drops the answer it produced.
    let lost = client
        .adopt_live_extent(1, closing.token, closing.cursor, true, false)
        .expect("the owner adopts and closes the closing extent");
    assert_eq!(
        lost.adoption.adopted_end, closing.cursor,
        "the owner closed the whole written prefix"
    );
    assert!(lost.adoption.closed, "the owner reports the extent closed");
    assert_eq!(
        client.leased_test_retained().0,
        7,
        "the coordinator still holds every frame of both extents"
    );
    // Tear only the current extent: its frames are the ones that must be
    // rewritten. The closed extent's bytes are not needed again.
    write_extent_bytes(
        &current.path,
        current.start,
        current.segment_bytes,
        &vec![0u8; usize::try_from(current.cursor - current.start).expect("bounded extent")],
    );
    client.leased_test_fail_submit(libc::ENODEV);
    submit(&mut client, &fixture, 11);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the resumed submission completes");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "one resume attempt");
    assert_eq!(counts.resume_rewrites, 1, "one rewrite episode");
    assert_eq!(
        counts.resume_frames, 3,
        "only the current extent's three unadopted frames are resent"
    );
    assert_eq!(
        client.leased_test_resume_targets(),
        vec!["closing", "current"],
        "the resume probes the closing extent before the current one"
    );
    assert!(
        client.live_leased.closing.is_none(),
        "the closed summary retired the closing extent"
    );
    let rewrites = client.leased_test_rewrites();
    assert_eq!(rewrites.len(), 3, "only the current extent was resent");
    let mut expected = current.start;
    for (token, start, len, _) in &rewrites {
        assert_ne!(
            *token, closing.token,
            "no frame of the closed extent is resent"
        );
        assert_eq!(
            *start, expected,
            "the current resends ascend from its start"
        );
        assert_eq!(*len, 4096, "each resent frame is one aligned block");
        expected += u64::try_from(*len).expect("bounded frame");
    }
    assert_eq!(
        extent_bytes(
            &current.path,
            current.start,
            current.cursor,
            current.segment_bytes
        ),
        clean_current,
        "the current extent is resent byte-identically at its own offsets"
    );
    assert_eq!(
        extent_bytes(
            &closing.path,
            closing.start,
            closing.cursor,
            closing.segment_bytes
        ),
        clean_closing,
        "the closed extent's bytes are untouched"
    );
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the current extent's prefix");
    assert_eq!(
        client.leased_test_retained(),
        (0, 0),
        "nothing is left to resend"
    );
    let owner = fixture.node.log_for_test(1);
    let adopted = owner.receipt_page(0, 32).expect("the owner's receipt page");
    assert_eq!(adopted.len(), 12, "twelve frames, one receipt each");
    for (index, receipt) in adopted.iter().enumerate() {
        assert_eq!(
            receipt.version().sequence,
            index as u64,
            "receipt {index} is the {index}th batch: no gap and no duplicate"
        );
    }
    assert_eq!(released.len(), 12, "every frame was released once");
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}

/// The owner answers a no-op probe of the extent it preleased for the switch,
/// because that extent has adopted nothing yet. A probe past that extent's start
/// stays refused: only the open extent's scan may adopt frames.
#[test]
fn probe_of_a_preleased_extent_past_its_start_is_refused() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    let (closing, current) = rollover_with_current_frames(&fixture, &mut client, &mut released, 1);
    assert!(
        client.live_leased.closing.is_some(),
        "the first extent of the switch is still closing at the owner"
    );
    // The preleased extent's real fresh state: nothing adopted, nothing closed.
    let fresh = client
        .adopt_live_extent(1, current.token, current.start, false, false)
        .expect("the preleased extent answers its own no-op probe");
    assert_eq!(
        fresh.adoption.adopted_end, current.start,
        "the preleased extent adopted nothing"
    );
    assert_eq!(fresh.adoption.receipt_count, 0, "no receipt is adopted");
    assert!(!fresh.adoption.closed, "the preleased extent is not closed");
    // One block past its start would claim adoption the owner never scanned.
    let refused = client
        .adopt_live_extent(1, current.token, current.start + 4096, false, false)
        .expect_err("only the open extent may adopt frames");
    assert!(
        refused.to_string().contains("foreign live extent"),
        "the refusal names the foreign extent: {refused}"
    );
    assert_eq!(
        current.start, closing.cursor,
        "the current extent is still the preleased successor"
    );
}

/// A closed summary whose frontier differs from what the coordinator wrote is
/// an identity mismatch, not a completed rollover: retiring the extent would
/// drop frames that still owe a resend, so the resume fails the database.
#[test]
fn closed_reply_with_a_different_adopted_end_fail_stops() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    let (closing, _current) = rollover_with_current_frames(&fixture, &mut client, &mut released, 2);
    client
        .adopt_live_extent(1, closing.token, closing.cursor, true, false)
        .expect("the owner adopts and closes the closing extent");
    // The coordinator's recorded written end is now below the frontier the
    // owner closed, as a client that holds fewer frames than the owner adopted.
    client
        .live_leased
        .closing
        .as_mut()
        .expect("an extent is closing")
        .written_end -= 4096;
    client.leased_test_fail_submit(libc::ENODEV);
    let error = match client.submit_leased_group(
        1,
        vec![fixture.batch(11)],
        &WriteTimeline::default(),
        2,
        true,
    ) {
        Err(error) => error,
        Ok(_) => panic!("a closed frontier the coordinator did not write is refused"),
    };
    assert!(
        error.to_string().contains("identity or frontier mismatch"),
        "the refusal names the mismatch: {error}"
    );
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "the refusal is not retried");
    assert_eq!(counts.resume_refused, 1, "the refusal is counted");
    assert_eq!(counts.resume_rewrites, 0, "nothing is rewritten");
    assert_eq!(
        client.leased_test_retained().0,
        7,
        "the retained frames are not dropped by a refused rollover"
    );
}

/// A resume resend allocates a completion id the stream never used. Reusing
/// one would let a late CQE of the original command settle the wrong frame, so
/// every resend id of several breaks must be new and appear exactly once.
#[test]
fn rewrite_ids_do_not_reuse_group_ids() {
    let fixture = Fixture::new(DurabilityTier::Native, 64 * 1024);
    let mut client = resumed_writer(&fixture);
    let mut released = Vec::new();
    // Four adopted frames put four ids into the stream.
    for sequence in 0..4 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the first four frames complete");
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the first prefix");
    assert_eq!(
        client.leased_test_submitted_ids().len(),
        4,
        "four commands used four ids"
    );
    let mut seen = 0usize;
    for episode in 0..3u64 {
        let base = 4 + episode * 3;
        // Two unadopted frames, then a tear from the owner's frontier: the
        // resume must resend both.
        for sequence in base..base + 2 {
            submit(&mut client, &fixture, sequence);
        }
        client
            .drain_leased_flights(&mut released, None)
            .expect("the two unadopted frames complete");
        let (_, cursor, _, adopted_end) = client.leased_test_extent_ends().expect("an open extent");
        let torn = owned_bytes(&client, adopted_end, cursor);
        write_bytes(&client, adopted_end, &vec![0u8; torn.len()]);
        let entry = client.live_leased.next_flight_id;
        client.leased_test_fail_submit(libc::ENODEV);
        submit(&mut client, &fixture, base + 2);
        client
            .drain_leased_flights(&mut released, None)
            .expect("the resumed submission completes");
        let rewrites = client.leased_test_rewrites();
        assert_eq!(
            rewrites.len() - seen,
            2,
            "episode {episode} resends exactly the two unadopted frames"
        );
        let used = client.leased_test_submitted_ids();
        let mut expected = adopted_end;
        for (_, start, len, id) in &rewrites[seen..] {
            assert_eq!(
                *start, expected,
                "episode {episode}: the resend keeps the recorded offset"
            );
            assert_eq!(*len, 4096, "episode {episode}: one aligned block");
            assert!(
                *id > entry,
                "episode {episode}: resend id {id} must be above the {entry} ids in use at entry"
            );
            assert_eq!(
                used.iter().filter(|candidate| *candidate == id).count(),
                1,
                "episode {episode}: resend id {id} is used exactly once"
            );
            expected += *len as u64;
        }
        assert_eq!(
            expected, cursor,
            "episode {episode}: the resends cover the unadopted suffix"
        );
        seen = rewrites.len();
        client
            .settle_leased_extent(&mut released, None)
            .expect("the owner adopts the recovered suffix");
    }
    // Across the whole stream no completion id was ever used twice.
    let ids = client.leased_test_submitted_ids();
    for (position, id) in ids.iter().enumerate() {
        assert_eq!(
            ids[..position]
                .iter()
                .filter(|candidate| *candidate == id)
                .count(),
            0,
            "id {id} is reused at position {position}"
        );
    }
    assert_eq!(
        counters(&fixture).resume_rewrites,
        3,
        "the stream ran three rewrite episodes"
    );
}

/// The owner applied an adoption but its reply was lost, so the coordinator's
/// adopted end is behind the owner's frontier. The resume's adopted-end query
/// proves that frontier: it resends nothing, duplicates no receipt and leaves
/// no gap, and every acknowledged batch reads back.
#[test]
fn lost_adoption_reply_is_idempotent() {
    let fixture = Fixture::single_partition(DurabilityTier::Native, 64 * 1024);
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
        .expect("the owner adopts the first prefix");
    for sequence in 4..6 {
        submit(&mut client, &fixture, sequence);
    }
    client
        .drain_leased_flights(&mut released, None)
        .expect("the two later frames complete");
    let (start, cursor, _, adopted_end) = client.leased_test_extent_ends().expect("an open extent");
    assert_eq!(
        client.leased_test_retained().0,
        2,
        "the two later frames wait for an adoption"
    );
    let clean = owned_bytes(&client, start, cursor);
    // The owner applies the adoption of the whole written window; the reply is
    // received here and dropped, as a session break drops the answer that the
    // coordinator's own request produced.
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
        .expect("the owner adopts the written window");
    assert_eq!(
        lost.adoption.adopted_end, cursor,
        "the owner is ahead of the coordinator's adopted end of {adopted_end}"
    );
    assert_eq!(
        client.leased_test_retained().0,
        2,
        "the lost reply leaves the coordinator's retained set unchanged"
    );
    // The queue is lost before the next submission, so only the resume's probe
    // can prove the owner's frontier.
    client.leased_test_fail_submit(libc::ENODEV);
    submit(&mut client, &fixture, 6);
    client
        .drain_leased_flights(&mut released, None)
        .expect("the resumed submission completes");
    let counts = counters(&fixture);
    assert_eq!(counts.resume_attempts, 1, "one resume attempt");
    assert_eq!(
        counts.resume_rewrites, 0,
        "the adopted-end query answers the frontier, so nothing is resent"
    );
    assert_eq!(counts.resume_frames, 0, "no frame was resent");
    assert_eq!(
        client.leased_test_retained().0,
        1,
        "only the retried frame stays retained until the owner adopts it"
    );
    client
        .settle_leased_extent(&mut released, None)
        .expect("the owner adopts the retried frame");
    assert_eq!(
        client.leased_test_retained(),
        (0, 0),
        "the declaration adopted every frame"
    );
    assert_eq!(
        owned_bytes(&client, start, cursor),
        clean,
        "an answered adoption changes no byte"
    );
    // No duplicate and no gap: one receipt per submitted frame, in order.
    let owner = fixture.node.log_for_test(1);
    assert_eq!(owner.receipt_count(), 7, "seven frames, seven receipts");
    let adopted = owner.receipt_page(0, 16).expect("the owner's receipt page");
    assert_eq!(adopted.len(), 7, "the page holds every receipt");
    for (index, receipt) in adopted.iter().enumerate() {
        assert_eq!(
            receipt.version().sequence,
            index as u64,
            "receipt {index} is the {index}th batch: no gap and no duplicate"
        );
    }
    assert_eq!(released.len(), 7, "every frame was released once");
    for (sequence, (_, receipts)) in released.iter().enumerate() {
        assert_reads_back(&fixture, &fixture.batch(sequence as u64), &receipts[0]);
    }
}
