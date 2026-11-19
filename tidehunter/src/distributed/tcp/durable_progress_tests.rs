use super::super::tests::{batch, certificates, config, metrics};
use super::*;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use std::collections::HashMap;

fn submit(
    node: &StorageNode,
    id: u64,
    request: Request,
    completed: &mpsc::SyncSender<reactor::WorkCompletion>,
) {
    let admitted = node.control.try_submit(control::ControlWork {
        id,
        request,
        limits: TcpLimits::default(),
        measured: false,
        queued: None,
        submitted: None,
        charge: 1,
        completed: completed.clone(),
        progress: None,
    });
    assert!(admitted.is_ok(), "control queue rejected request {id}");
}

/// Appends one batch to stream 1 with its frontier and syncer attached, as an
/// adopted leased frame does (the owner-RPC append was removed with the
/// payload route). Native tier: nothing waits for a sync.
fn append(node: &StorageNode, config: &NodeConfig, sequence: u64) {
    node.stream_log_for_test(1)
        .append_complete_batch(
            crate::distributed::types::RequestAuthority { epoch: 1 },
            &batch(config, 1, sequence),
        )
        .unwrap();
}

fn poll(authority_epoch: u64, epoch: u64, after: u64, max_wait_ms: u32, sync: bool) -> Request {
    Request::DurableProgress {
        authority_epoch,
        epoch,
        after,
        max_wait_ms,
        sync,
    }
}

/// With the test cap of `native_node`, a parked poll ends only by its condition, a
/// fence or shutdown: no reply depends on how fast the host runs.
const WAIT_MS: u32 = u32::MAX;
const LONG_CAP: Duration = Duration::from_secs(3600);
/// Only a hang reaches this bound.
const RECEIVE: Duration = Duration::from_secs(120);

fn receive(
    completions: &mpsc::Receiver<reactor::WorkCompletion>,
    id: u64,
) -> Result<Response, TcpError> {
    let completion = completions.recv_timeout(RECEIVE).unwrap();
    assert_eq!(completion.id, id);
    completion.response
}

fn report(response: Result<Response, TcpError>) -> DurableProgressReport {
    match response.unwrap() {
        Response::DurableProgress(report) => report,
        response => panic!("expected DurableProgress, received {}", response.as_ref()),
    }
}

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
}

/// A native-tier node with active epoch 1. Native fragments are large, so a sync below
/// a fragment happens only on demand.
fn native_node(temp: &tempdir::TempDir) -> (NodeConfig, Arc<StorageNode>) {
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.set_durable_progress_cap_for_test(LONG_CAP);
    assert_eq!(node.durability().tier, DurabilityTier::Native);
    node.inventory.lock().fence_to(1).unwrap();
    (config, node)
}

#[test]
fn nudge_reports_the_captured_tail_only_after_its_sync() {
    let temp = tempdir::TempDir::new("progress-nudge").unwrap();
    let (config, node) = native_node(&temp);
    let log = node.log_for_test(1);
    let (completed, completions) = mpsc::sync_channel(8);
    append(&node, &config, 0);
    let before = node.durable_progress_for_test(1, 1).unwrap();
    assert_eq!(
        (
            before.appended,
            before.synced,
            before.generation,
            before.idle
        ),
        (Some(version(0)), None, 0, false)
    );

    let (entered, release) = log.pause_next_persist();
    submit(&node, 2, poll(1, 1, 0, WAIT_MS, true), &completed);
    // The syncer captured the tail at version 0 and holds before its file sync.
    entered.recv_timeout(RECEIVE).unwrap();
    append(&node, &config, 1);
    assert!(
        completions.try_recv().is_err(),
        "nudge replied before its sync"
    );
    assert_eq!(log.durable_progress().synced, None);
    release.send(()).unwrap();
    let after_sync = report(receive(&completions, 2));
    assert_eq!(
        (
            after_sync.database,
            after_sync.partition,
            after_sync.authority_epoch,
            after_sync.epoch
        ),
        (
            config.partition.inventory.database,
            config.partition.partition,
            1,
            1
        )
    );
    // The synced tail is the captured one, not the appended tail at reply time.
    assert_eq!(after_sync.synced, Some(version(0)));
    assert_eq!(after_sync.appended, Some(version(1)));
    assert!(after_sync.synced_end < after_sync.appended_end);
    assert!(!after_sync.idle);
    assert_eq!(after_sync.generation, 1);

    submit(&node, 4, poll(1, 1, 1, WAIT_MS, true), &completed);
    let caught_up = report(receive(&completions, 4));
    assert_eq!(
        (caught_up.synced, caught_up.appended, caught_up.generation),
        (Some(version(1)), Some(version(1)), 2)
    );
    assert!(caught_up.idle);
    assert_eq!(caught_up.synced_end, log.synced_frontier());
    assert_eq!(node.control.pending_requests_for_test(), 0);
}

#[test]
fn failed_sync_never_advances_the_synced_tail() {
    for failure in 0..3 {
        let temp = tempdir::TempDir::new("progress-failed-sync").unwrap();
        let (config, node) = native_node(&temp);
        let log = node.log_for_test(1);
        let (completed, completions) = mpsc::sync_channel(4);
        append(&node, &config, 0);
        match failure {
            0 => log.fail_next_persist(),
            1 => log.fail_next_namespace_persist(),
            _ => log.fail_next_inventory_witness(false),
        }
        submit(&node, 2, poll(1, 1, 0, WAIT_MS, true), &completed);
        assert!(receive(&completions, 2).is_err(), "failure {failure}");
        let progress = log.durable_progress();
        assert_eq!((progress.synced, progress.generation), (None, 0));
        assert_eq!(progress.appended, Some(version(0)));
    }
}

#[test]
fn idle_owner_reports_at_once_without_fixing_its_tier() {
    let temp = tempdir::TempDir::new("progress-idle").unwrap();
    let (_config, node) = native_node(&temp);
    let (completed, completions) = mpsc::sync_channel(4);
    submit(&node, 1, poll(1, 1, 0, 0, false), &completed);
    let idle = report(receive(&completions, 1));
    assert!(idle.idle);
    assert_eq!(
        (idle.appended, idle.synced, idle.generation),
        (None, None, 0)
    );
    assert_eq!(idle.synced_end, idle.appended_end);
    // A nudge with nothing to sync answers at once and starts no frontier.
    submit(&node, 2, poll(1, 1, 0, WAIT_MS, true), &completed);
    assert!(report(receive(&completions, 2)).idle);
    node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Synchronous))
        .unwrap();
}

#[test]
fn stale_authority_and_unknown_epoch_fail_and_a_sealed_stream_reports_its_seal() {
    let temp = tempdir::TempDir::new("progress-authority").unwrap();
    let (config, node) = native_node(&temp);
    let (completed, completions) = mpsc::sync_channel(8);
    submit(&node, 1, poll(2, 1, 0, 0, false), &completed);
    assert!(receive(&completions, 1).is_err());
    submit(&node, 2, poll(1, 7, 0, 0, false), &completed);
    assert!(receive(&completions, 2).is_err());
    append(&node, &config, 0);
    submit(&node, 4, Request::Fence { epoch: 2 }, &completed);
    assert!(matches!(receive(&completions, 4), Ok(Response::Status(_))));
    submit(&node, 5, poll(1, 1, 0, 0, false), &completed);
    assert!(receive(&completions, 5).is_err());
    submit(&node, 6, poll(2, 1, 0, 0, false), &completed);
    let sealed = report(receive(&completions, 6));
    assert_eq!(
        (sealed.synced, sealed.appended, sealed.idle),
        (Some(version(0)), Some(version(0)), true)
    );
}

#[test]
fn parked_poll_wakes_on_a_sync_and_a_fence_answers_parked_polls() {
    let temp = tempdir::TempDir::new("progress-long-poll").unwrap();
    let (config, node) = native_node(&temp);
    let (completed, completions) = mpsc::sync_channel(8);
    append(&node, &config, 0);
    submit(&node, 2, poll(1, 1, 0, WAIT_MS, false), &completed);
    submit(&node, 3, poll(1, 1, 0, WAIT_MS, true), &completed);
    let mut replies = HashMap::new();
    for _ in 0..2 {
        let completion = completions.recv_timeout(RECEIVE).unwrap();
        replies.insert(completion.id, report(completion.response));
    }
    // The poll waits without a deadline, so only the sync could answer it.
    assert_eq!(replies[&2].generation, 1);
    assert_eq!(replies[&2].synced, Some(version(0)));
    assert_eq!(replies[&3].synced, Some(version(0)));

    submit(&node, 4, poll(1, 1, 1, WAIT_MS, false), &completed);
    submit(&node, 5, Request::Fence { epoch: 2 }, &completed);
    let mut replies = HashMap::new();
    for _ in 0..2 {
        let completion = completions.recv_timeout(RECEIVE).unwrap();
        replies.insert(completion.id, completion.response);
    }
    assert!(replies[&4].is_err());
    assert!(matches!(replies[&5], Ok(Response::Status(_))));
}

#[test]
fn hub_answers_polls_beyond_its_waiter_bound_at_once() {
    let temp = tempdir::TempDir::new("progress-waiter-bound").unwrap();
    let (_config, node) = native_node(&temp);
    let polls = MAX_WAITERS as u64 + 1;
    let (completed, completions) = mpsc::sync_channel(2 * MAX_WAITERS + 4);
    for id in 1..=polls {
        submit(&node, id, poll(1, 1, 0, WAIT_MS, false), &completed);
    }
    // Generation 0 never exceeds `after`, so only the poll beyond the bound replies.
    let first = completions.recv_timeout(RECEIVE).unwrap();
    assert_eq!(report(first.response).generation, 0);
    submit(&node, polls + 1, Request::Fence { epoch: 2 }, &completed);
    let mut failed = 0;
    for _ in 0..MAX_WAITERS + 1 {
        let completion = completions.recv_timeout(RECEIVE).unwrap();
        if completion.id == polls + 1 {
            assert!(matches!(completion.response, Ok(Response::Status(_))));
        } else {
            assert!(completion.response.is_err());
            failed += 1;
        }
    }
    assert_eq!(failed, MAX_WAITERS);
    assert_eq!(node.control.pending_requests_for_test(), 0);
}

#[test]
fn status_and_fence_report_the_owner_durability_tier() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let temp = tempdir::TempDir::new("progress-tier").unwrap();
        let config = config(&temp.path().join("node"));
        let node = StorageNode::open(config, metrics()).unwrap();
        node.set_durability(DurabilityConfig::with_tier(tier))
            .unwrap();
        let (completed, completions) = mpsc::sync_channel(2);
        submit(&node, 1, Request::Status { protocol: PROTOCOL }, &completed);
        submit(&node, 2, Request::Fence { epoch: 1 }, &completed);
        for id in 1..=2 {
            let Ok(Response::Status(status)) = receive(&completions, id) else {
                panic!("expected a status reply")
            };
            assert_eq!(status.durability_tier, tier);
            assert_eq!(status.incarnation, node.inventory.lock().incarnation());
            assert!(status.recovery_clean);
        }
    }
}

/// Every owner open is a new incarnation, so an owner process restart changes it on
/// the same boot. One incarnation reports one value.
#[test]
fn every_owner_open_is_a_new_incarnation() {
    let temp = tempdir::TempDir::new("progress-incarnation").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    let first = node.inventory.lock().incarnation();
    assert_ne!(first, [0; 16]);
    assert_eq!(
        node.inventory.lock().fence_to(1).unwrap().incarnation,
        first
    );
    assert_eq!(node.status().unwrap().incarnation, first);
    drop(node);
    let reopened = StorageNode::open(config, metrics()).unwrap();
    let second = reopened.status().unwrap().incarnation;
    assert_ne!(second, [0; 16]);
    assert_ne!(second, first);
}

/// A failed syncer fsync can drop acknowledged dirty pages without a hole and without a
/// reboot. The incarnation never reports a clean recovery again, its next control
/// barrier fails and stops it (no in-process fence), and only a restarted owner (a new
/// incarnation) is clean.
#[test]
fn failed_sync_is_unclean_until_the_owner_restarts() {
    let temp = tempdir::TempDir::new("progress-unclean").unwrap();
    let (config, node) = native_node(&temp);
    let incarnation = node.status().unwrap().incarnation;
    let log = node.log_for_test(1);
    let (completed, completions) = mpsc::sync_channel(4);
    append(&node, &config, 0);
    assert!(node.status().unwrap().recovery_clean);
    log.fail_next_persist();
    submit(&node, 2, poll(1, 1, 0, WAIT_MS, true), &completed);
    assert!(receive(&completions, 2).is_err());
    drop(log);
    let status = node.status().unwrap();
    assert_eq!(status.incarnation, incarnation);
    assert!(!status.recovery_clean);
    // The barrier before the status meets the failed frontier: the owner stops.
    submit(&node, 3, Request::Status { protocol: PROTOCOL }, &completed);
    assert!(receive(&completions, 3).is_err());
    assert!(
        node.control_context
            .persistence_failed
            .load(Ordering::Acquire)
    );
    drop(node);
    let reopened = StorageNode::open(config, metrics()).unwrap();
    let status = reopened.status().unwrap();
    assert_ne!(status.incarnation, incarnation);
    assert!(status.recovery_clean);
}

#[test]
fn client_polls_over_a_dedicated_connection() {
    let temp = tempdir::TempDir::new("progress-client").unwrap();
    let tls = certificates(temp.path());
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));
    let server = node.clone();
    let stop = shutdown.clone();
    let server_tls = tls.clone();
    // A data timeout shorter than the parked poll below: the long-poll's own wait
    // must extend both the client and the server session deadline.
    let limits = TcpLimits {
        io_timeout: Duration::from_secs(2),
        ..TcpLimits::default()
    };
    let timeout = limits.io_timeout;
    let server_thread =
        thread::spawn(move || server.serve(listener, server_tls, limits, stop).unwrap());
    let shape = KeyShape::from_yaml(&config.shape_yaml).unwrap();
    let mut writer =
        Client::connect(&endpoint, &tls, timeout, config.partition.clone(), &shape).unwrap();
    let status = writer.fence_to(1).unwrap();
    assert_eq!(status.durability_tier, DurabilityTier::Native);
    append(&node, &config, 0);
    // The Status handshake of this session flushes the owner's logs, so the nudge
    // finds nothing to sync and answers at once.
    let mut progress =
        Client::connect(&endpoint, &tls, timeout, config.partition.clone(), &shape).unwrap();
    let synced = progress
        .durable_progress(1, 1, 0, DURABLE_PROGRESS_MAX_WAIT, true)
        .unwrap();
    assert_eq!(
        (synced.synced, synced.appended, synced.idle),
        (Some(version(0)), Some(version(0)), true)
    );
    assert_eq!(synced.incarnation, status.incarnation);
    // Parks past the data timeout, then replies at its deadline with the current report.
    let parked = progress
        .durable_progress(1, 1, synced.generation, Duration::from_secs(3), false)
        .unwrap();
    assert_eq!(parked, synced);
    assert!(
        progress
            .durable_progress(2, 1, 0, Duration::ZERO, false)
            .is_err()
    );
    drop(progress);
    drop(writer);
    shutdown.store(true, Ordering::Release);
    server_thread.join().unwrap();
}

#[test]
fn leased_owner_reports_the_adopted_and_synced_prefix() {
    use crate::distributed::write_timeline::WriteTimeline;
    let fixture = live_client::leased_tests::Fixture::new(DurabilityTier::Native, 16384);
    let mut writer = fixture.writer_without_ahead();
    for sequence in 0..2 {
        writer
            .submit_and_drain_leased_group(
                1,
                vec![fixture.batch(sequence)],
                &WriteTimeline::default(),
                2,
            )
            .unwrap();
    }
    // A plain connection on a live NVMe-oF owner answers a poll.
    let mut progress = fixture.client();
    let first = progress
        .durable_progress(1, 1, 0, Duration::ZERO, false)
        .unwrap();
    assert_eq!((first.authority_epoch, first.epoch), (1, 1));
    drop(progress);
    // The nudge self-adopts any frames the writer has not had adopted yet, then
    // syncs them and installs the strict root over them; with the long cap no
    // deadline can answer it first.
    fixture.node.set_durable_progress_cap_for_test(LONG_CAP);
    let (completed, completions) = mpsc::sync_channel(1);
    submit(&fixture.node, 1, poll(1, 1, 0, WAIT_MS, true), &completed);
    let synced = report(receive(&completions, 1));
    assert_eq!(
        (synced.appended, synced.synced, synced.idle),
        (Some(version(1)), Some(version(1)), true)
    );
    // A later group: the bounded syncer may defer its root within the fragment,
    // and only a root install reports it. A nudge without its own barrier would
    // park until the long cap unless the syncer happened to cross a fragment.
    writer
        .submit_and_drain_leased_group(1, vec![fixture.batch(2)], &WriteTimeline::default(), 2)
        .unwrap();
    submit(
        &fixture.node,
        2,
        poll(1, 1, synced.generation, WAIT_MS, true),
        &completed,
    );
    let rooted = report(receive(&completions, 2));
    assert_eq!(
        (rooted.appended, rooted.synced, rooted.idle),
        (Some(version(2)), Some(version(2)), true)
    );
    assert!(rooted.generation > synced.generation);
    writer.close_leased_extent(&mut Vec::new(), None).unwrap();
}
