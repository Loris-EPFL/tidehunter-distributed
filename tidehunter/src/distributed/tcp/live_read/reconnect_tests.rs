//! Read-lane session replacement after a control-session failure.
//!
//! The lane aborts its session when a control exchange fails. A later read must
//! replace it before its first control RPC, inside its own deadline, and a
//! failed replacement must stay typed and must not quarantine the subsystem.

use super::*;
use crate::distributed::nvmeof::NvmeofTransport;
use crate::distributed::nvmeof::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
use crate::distributed::nvmeof::live_pool::LiveControllerPool;
use crate::distributed::nvmeof::live_storage::{FILES_PER_SUBSYSTEM, wal_export_name};
use crate::distributed::tcp::LiveExportInfo;
use std::io::ErrorKind;
use std::sync::Arc;

/// A pool with both epoch-1 permanent namespaces of the subsystem cached, so
/// the read's positive path needs no controller work. The owner's export list
/// is cached too: the control session is then used by the replacement alone.
fn cached_permanent_pool(lane: &Lane) -> (Arc<LiveControllerPool>, LiveExportInfo) {
    let config = lane
        .client
        .live_config
        .as_ref()
        .expect("the lane client is configured");
    let registry = LiveConnectionRegistry::open(
        &lane._dir.path().join("registry"),
        crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
    )
    .unwrap();
    let pool = Arc::new(LiveControllerPool::new(registry, 8, 4).unwrap());
    let mut first = None;
    for file in 0..2 {
        let name = wal_export_name(
            FILES_PER_SUBSYSTEM,
            lane.client.expected.inventory.database,
            lane.client.expected.partition,
            1,
            file,
        );
        let target = LiveConnectTarget {
            nqn: name.nqn.clone(),
            uuid: name.uuid,
            nsid: name.nsid,
            bytes: 8192,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            geometry: config.queue_geometry,
        };
        pool.cache_local_shared_namespace(
            &target,
            &config.host_nqn,
            config.host_id,
            &lane._dir.path().join(format!("wal-{file}")),
        )
        .unwrap();
        // A controller-loss retry evicts the cached entry and asks the pool to
        // connect again: keep that reconnect on the local fixture, not the
        // host's fabrics control device.
        pool.register_local_target(
            &target,
            crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess::ReadShared,
            lane._dir.path().join(format!("wal-{file}")),
        )
        .unwrap();
        first.get_or_insert(target);
    }
    let first = first.expect("file 0 is cached");
    let info = LiveExportInfo {
        address: first.address,
        service: first.service,
        transport: first.transport,
        wal_file_bytes: 8192,
        block_size: 4096,
        exported_files: 2,
    };
    (pool, info)
}

/// The lane replaces the session the forced failure left unusable, and the
/// read is served after that one connection setup.
#[test]
fn a_read_after_a_forced_connection_failure_replaces_the_session() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    lane.client.fail_connection_for_test();
    assert_eq!(
        lane.client.session_present(),
        None,
        "the forced failure leaves no usable session"
    );
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the read replaces the aborted session and serves the member"),
        &vec![frame],
        "the read returns the member frame"
    );
    assert_eq!(
        lane.client.session_present(),
        Some(true),
        "the read installed a usable session"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A replacement that starts at or past the read deadline is a typed timeout,
/// installs nothing, and caches no subsystem. The lane's own spent budget is
/// typed the same way and reaches no control RPC.
#[test]
fn a_spent_read_deadline_stops_the_replacement_and_caches_nothing() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    lane.client.fail_connection_for_test();
    let error = lane
        .client
        .replace_read_session(Instant::now())
        .expect_err("a spent read deadline cannot replace the session");
    assert_eq!(
        error.kind(),
        ErrorKind::TimedOut,
        "the replacement keeps the timeout class: {error}"
    );
    assert_eq!(
        lane.client.session_present(),
        None,
        "a spent deadline installs no session"
    );
    // The lane's own budget is checked before any connect attempt.
    let request = LiveReadRequest {
        positions: vec![lane.a],
        deadline: Instant::now(),
        trace: None,
    };
    assert!(
        lane.cache.submit(&mut lane.client, 1, request).is_ok(),
        "the lane stores the request and fails it with the typed deadline"
    );
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("a spent read deadline cannot serve the member");
    assert_eq!(
        error.kind(),
        ErrorKind::TimedOut,
        "the read keeps the timeout class: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a deadline caches no subsystem as failed"
    );
}

/// A refused replacement keeps the transport's typed kind, is not cached as a
/// failed subsystem, and the next read on the same lane replaces the session
/// once the endpoint answers again.
#[test]
fn a_refused_replacement_keeps_its_kind_and_the_next_read_recovers() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let endpoint = lane._running.endpoint().to_owned();
    let refused = {
        // A port bound and released answers with a refused connection.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address.to_string()
    };
    lane.client.rebind_endpoint_for_test(&refused);
    lane.client.fail_connection_for_test();
    let a = lane.a;
    lane.submit(vec![a]);
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("a refused replacement cannot serve the member");
    assert_eq!(
        error.kind(),
        ErrorKind::ConnectionRefused,
        "the transport kind survives the replacement: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a refused replacement caches no subsystem as failed"
    );
    assert_eq!(
        lane.client.session_present(),
        None,
        "a refused replacement installs no session"
    );
    lane.client.rebind_endpoint_for_test(&endpoint);
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the owner answers again, so the next read replaces the session"),
        &vec![frame],
        "the recovered read returns the member frame"
    );
    assert_eq!(
        lane.client.session_present(),
        Some(true),
        "the recovered read installed a usable session"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A read whose grant's controller was verifiably deleted is an idempotent
/// retry: the lane evicts the dead lease, reconnects through the pool and
/// returns the same member bytes. The subsystem is not quarantined.
#[test]
fn a_lost_controller_read_reconnects_and_returns_the_same_member() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    assert_eq!(
        lane.collect(1)[0]
            .result
            .as_ref()
            .expect("the first read serves from the pool"),
        &vec![frame.clone()],
        "the pooled controller serves the member"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    let nqn = lane.cache.grants[index]
        .lease
        .as_ref()
        .expect("the pooled grant holds its lease")
        .binding()
        .nqn
        .clone();
    let cached = pool
        .cached_lease(&nqn)
        .expect("the read cached its controller");
    // A local-file lease has no kernel controller to probe: the test reports
    // the deletion the kernel would report.
    cached.force_controller_lost(true);
    // The kernel cancelled the member with EIO, and the pinned controller is
    // not live: a stopped controller, not a media verdict.
    lane.cache.grants[index].controller_lost = Some(true);
    lane.cache.grants[index]
        .io
        .override_next_completion_result(-libc::EIO);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the lost controller is resumed"),
        &vec![frame],
        "the retry returns the identical member bytes"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a lost controller must not quarantine the subsystem"
    );
    let reopened = pool
        .cached_lease(&nqn)
        .expect("the retry connected a new controller");
    assert!(
        !Arc::ptr_eq(&cached, &reopened),
        "the retry evicted the dead controller before it reopened"
    );
    // The shutdown contract requires every externally held lease released.
    drop(cached);
    drop(reopened);
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// The same EIO with a controller that is still live is the device's verdict:
/// the subsystem is quarantined and the read fails.
#[test]
fn a_live_controller_read_eio_stays_fatal() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    lane.submit(vec![a]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the pooled controller serves the first member"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    lane.cache.grants[index].controller_lost = Some(false);
    lane.cache.grants[index]
        .io
        .override_next_completion_result(-libc::EIO);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("a live controller's EIO is fatal");
    assert_eq!(
        error.kind(),
        ErrorKind::Other,
        "a media EIO keeps its fatal class: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.contains(&(1, 0)),
        "a media EIO still quarantines the subsystem"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// The inline route never sets up a controller. A lost controller evicts the
/// dead lease and declines the member with a transient class, so the caller
/// submits it through the lane and the lane reconnects.
#[test]
fn a_lost_controller_inline_read_declines_without_quarantining() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the first read caches the grant"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    let nqn = lane.cache.grants[index]
        .lease
        .as_ref()
        .expect("the pooled grant holds its lease")
        .binding()
        .nqn
        .clone();
    lane.cache.grants[index].controller_lost = Some(true);
    lane.cache.grants[index]
        .io
        .override_next_completion_result(-libc::EIO);
    let error = lane
        .cache
        .read_inline_cached(
            &lane.client,
            &a,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .expect_err("the inline route cannot set up a controller");
    assert_eq!(
        error.kind(),
        ErrorKind::ConnectionAborted,
        "the caller recognizes the decline: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "the lane must stay free to reconnect"
    );
    let grant = lane
        .cache
        .grants
        .iter()
        .find(|grant| grant.lease.is_some())
        .expect("the pooled grant remains for retirement");
    assert!(
        grant.broken && grant.lost,
        "the dead grant is broken and marked lost"
    );
    assert!(
        pool.cached_lease(&nqn).is_some(),
        "the fixture lease reports live, so the decline never depended on its eviction"
    );
    // The lane now reconnects and serves the member.
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the lane reconnects the lost controller"),
        &vec![frame],
        "the lane returns the identical member bytes"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A namespace open that reports the deleting controller is the first evidence
/// of a real controller loss and arrives before any EIO. The read must requeue
/// its member and reconnect instead of failing the subsystem.
#[test]
fn a_verify_report_of_a_dead_controller_requeues_the_read() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.cache.fail_next_open_lost_controller();
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0].result.as_ref().expect("the lost open is retried"),
        &vec![frame],
        "the retry returns the identical member bytes"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a lost open must not quarantine the subsystem"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// One controller loss spends one reconnect for the whole request, not one per
/// flight: two flights of the same request on the same dead lease both requeue.
#[test]
fn one_controller_loss_requeues_every_flight_of_the_request() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    lane.client
        .live_config
        .as_mut()
        .expect("the lane client is configured")
        .max_inflight_requests = 8;
    let (a, b) = (lane.a, lane.b);
    lane.submit(vec![a, b]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the warm read caches one subsystem grant for both files"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the warm read cached its grant");
    lane.cache.grants[index].controller_lost = Some(true);
    let first = lane.cache.next_flight;
    lane.cache.grants[index]
        .io
        .override_completion_result(first, -libc::EIO);
    lane.cache.grants[index]
        .io
        .override_completion_result(first + 1, -libc::EIO);
    let (fa, fb) = (
        member_frame(&lane.first, &a),
        member_frame(&lane.second, &b),
    );
    lane.submit(vec![a, a, a, b, b, b]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("both lost flights of the request are requeued"),
        &vec![fa.clone(), fa.clone(), fa, fb.clone(), fb.clone(), fb],
        "every member of the request is served once"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "one loss never quarantines the subsystem"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// The first read of another file of a subsystem reuses the cached grant. When
/// that grant's controller is verifiably gone, the branch must retire it before
/// the connect instead of handing the fresh grant the same dead lease.
#[test]
fn a_dead_stale_grant_is_dropped_before_the_connect() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    lane.submit(vec![a]);
    assert!(lane.collect(1)[0].result.is_ok(), "the first file reads");
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    assert!(
        lane.cache.grants[index].scope.slot(&lane.b).is_none(),
        "the cached grant has no slot for the second file"
    );
    lane.cache.grants[index].controller_lost = Some(true);
    let b = lane.b;
    let frame = member_frame(&lane.second, &b);
    lane.submit(vec![b]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the stale grant does not fail the read"),
        &vec![frame],
        "the second file reads through a fresh grant"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a dead stale grant is not a subsystem failure"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// The first connect the pool makes for a read grant can fail while it verifies
/// the controller it created. The read requeues and reconnects instead of
/// failing the subsystem.
#[test]
fn a_lost_acquire_requeues_the_read() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    pool.fail_next_connect_lost();
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the lost acquire is retried"),
        &vec![frame],
        "the retry returns the identical member bytes"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a lost acquire must not quarantine the subsystem"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A read CQE that completes short is the same resumable class as `EIO` when
/// the error-path probe proves the pinned controller is gone: the kernel
/// reports a vanished controller as a short read, not only as `EIO`. The lane
/// requeues the members, evicts the dead lease, reconnects and returns the
/// identical member bytes; the subsystem is not quarantined and the resume is
/// counted.
#[test]
fn a_short_read_on_a_lost_controller_reconnects_and_returns_the_same_member() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    assert_eq!(
        lane.collect(1)[0]
            .result
            .as_ref()
            .expect("the first read serves from the pool"),
        &vec![frame.clone()],
        "the pooled controller serves the member"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    let nqn = lane.cache.grants[index]
        .lease
        .as_ref()
        .expect("the pooled grant holds its lease")
        .binding()
        .nqn
        .clone();
    let cached = pool
        .cached_lease(&nqn)
        .expect("the read cached its controller");
    // A local-file lease has no kernel controller to probe: the test reports
    // the deletion the kernel would report.
    cached.force_controller_lost(true);
    lane.cache.grants[index].controller_lost = Some(true);
    // A positive count below the buffer length is the short namespace read a
    // vanished controller leaves on the completion queue.
    lane.cache.grants[index]
        .io
        .override_next_completion_result(0);
    let before = lane.metrics.snapshot().counts.read_controller_resumes;
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the short read of a lost controller is resumed"),
        &vec![frame],
        "the retry returns the identical member bytes"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a lost controller must not quarantine the subsystem"
    );
    let reopened = pool
        .cached_lease(&nqn)
        .expect("the retry connected a new controller");
    assert!(
        !Arc::ptr_eq(&cached, &reopened),
        "the retry evicted the dead controller before it reopened"
    );
    assert_eq!(
        lane.metrics.snapshot().counts.read_controller_resumes - before,
        1,
        "one resumed read is counted once"
    );
    // The shutdown contract requires every externally held lease released.
    drop(cached);
    drop(reopened);
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// The same short completion on a controller the probe reports live is a media
/// or namespace verdict: it stays fatal, quarantines the subsystem and is never
/// counted as a resume. This is the poison contract a read with no verdict
/// keeps.
#[test]
fn a_short_read_on_a_live_controller_stays_fatal() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    lane.submit(vec![a]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the pooled controller serves the first member"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    lane.cache.grants[index].controller_lost = Some(false);
    lane.cache.grants[index]
        .io
        .override_next_completion_result(0);
    let before = lane.metrics.snapshot().counts.read_controller_resumes;
    lane.submit(vec![a]);
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("an unexplained short read is fatal");
    assert_eq!(
        error.kind(),
        ErrorKind::Other,
        "the short read keeps its fatal class: {error}"
    );
    assert!(
        error.to_string().contains("short namespace read"),
        "the fatal verdict names the short read: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.contains(&(1, 0)),
        "an unexplained short read still quarantines the subsystem"
    );
    assert_eq!(
        lane.metrics.snapshot().counts.read_controller_resumes,
        before,
        "an unverified short read is never counted as a resume"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// One request spends at most one reconnect. A short read that resumes and a
/// reconnect that then fails leave the read a transient error, with exactly
/// one counted resume and no second connect.
#[test]
fn a_second_loss_in_one_request_is_not_retried() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    lane.submit(vec![a]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the pooled controller serves the first member"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    let cached = pool
        .cached_lease(
            &lane.cache.grants[index]
                .lease
                .as_ref()
                .expect("the pooled grant holds its lease")
                .binding()
                .nqn,
        )
        .expect("the read cached its controller");
    cached.force_controller_lost(true);
    lane.cache.grants[index].controller_lost = Some(true);
    lane.cache.grants[index]
        .io
        .override_next_completion_result(0);
    // The one reconnect the request is allowed fails on its own connect.
    pool.fail_next_connect_lost();
    let attempts = pool.connect_attempts();
    let before = lane.metrics.snapshot().counts.read_controller_resumes;
    lane.submit(vec![a]);
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("a second loss inside one request is not retried");
    assert_eq!(
        error.kind(),
        ErrorKind::ConnectionAborted,
        "the spent budget is a transient class, not a media verdict: {error}"
    );
    assert_eq!(
        lane.metrics.snapshot().counts.read_controller_resumes - before,
        1,
        "exactly one resume is counted"
    );
    assert_eq!(
        pool.connect_attempts() - attempts,
        1,
        "the request made exactly one reconnect attempt"
    );
    assert_eq!(
        pool.connect_losses_remaining(),
        0,
        "the armed connect loss was consumed by the one reconnect"
    );
    drop(cached);
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// The inline route never sets up a controller. A short read on a verifiably
/// lost controller declines the member with a transient class, so the caller
/// submits it through the lane, which reconnects and serves it.
#[test]
fn a_short_read_inline_declines_without_quarantining() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the first read caches the grant"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    let nqn = lane.cache.grants[index]
        .lease
        .as_ref()
        .expect("the pooled grant holds its lease")
        .binding()
        .nqn
        .clone();
    if let Some(cached) = pool.cached_lease(&nqn) {
        cached.force_controller_lost(true);
    }
    lane.cache.grants[index].controller_lost = Some(true);
    lane.cache.grants[index]
        .io
        .override_next_completion_result(0);
    let before = lane.metrics.snapshot().counts.read_controller_resumes;
    let error = lane
        .cache
        .read_inline_cached(
            &lane.client,
            &a,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .expect_err("the inline route cannot set up a controller");
    assert_eq!(
        error.kind(),
        ErrorKind::ConnectionAborted,
        "the caller recognizes the decline: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "the lane must stay free to reconnect"
    );
    assert_eq!(
        lane.metrics.snapshot().counts.read_controller_resumes - before,
        1,
        "the declined inline read is counted as a resume"
    );
    let grant = lane
        .cache
        .grants
        .iter()
        .find(|grant| grant.lease.is_some())
        .expect("the pooled grant remains for retirement");
    assert!(
        grant.broken && grant.lost,
        "the dead grant is broken and marked lost"
    );
    // The lane now reconnects and serves the member.
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the lane reconnects the lost controller"),
        &vec![frame],
        "the lane returns the identical member bytes"
    );
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A read submission that fails with `EIO` on a verifiably lost controller is
/// classified before `TcpError::component` flattens the errno: the lane marks
/// the grant lost, evicts it and requeues within its one-reconnect budget.
#[test]
fn a_lost_controller_read_submission_requeues_the_read() {
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    let (pool, info) = cached_permanent_pool(&lane);
    lane.client.live_exports = Some((1, info, 2));
    lane.client.live_pool = Some(pool.clone());
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    assert!(
        lane.collect(1)[0].result.is_ok(),
        "the first read caches the grant"
    );
    let index = lane
        .cache
        .find_grant(&a)
        .expect("the first read cached its grant");
    let nqn = lane.cache.grants[index]
        .lease
        .as_ref()
        .expect("the pooled grant holds its lease")
        .binding()
        .nqn
        .clone();
    let cached = pool
        .cached_lease(&nqn)
        .expect("the read cached its controller");
    cached.force_controller_lost(true);
    lane.cache.grants[index].controller_lost = Some(true);
    lane.cache.grants[index]
        .io
        .override_read_submit_result(libc::EIO);
    let before = lane.metrics.snapshot().counts.read_controller_resumes;
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0]
            .result
            .as_ref()
            .expect("the submit-time loss is requeued and reconnected"),
        &vec![frame],
        "the retry returns the identical member bytes"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a submit-time controller loss must not quarantine the subsystem"
    );
    assert_eq!(
        lane.metrics.snapshot().counts.read_controller_resumes - before,
        1,
        "one resumed read is counted once"
    );
    let reopened = pool
        .cached_lease(&nqn)
        .expect("the retry connected a new controller");
    assert!(
        !Arc::ptr_eq(&cached, &reopened),
        "the retry evicted the dead controller before it reopened"
    );
    drop(cached);
    drop(reopened);
    lane.cache.close(&mut lane.client, 1).unwrap();
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}
