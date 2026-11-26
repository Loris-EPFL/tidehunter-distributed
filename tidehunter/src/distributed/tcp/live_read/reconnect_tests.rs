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
        pool.cached_lease(&nqn).is_none(),
        "the dead lease was evicted"
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
