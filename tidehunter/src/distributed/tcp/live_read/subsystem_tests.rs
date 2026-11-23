//! Subsystem grant setup: a spent connect budget stays transient.

use super::*;

/// A connect budget that expires is a transient setup failure, not a broken
/// subsystem. The pool is real and empty and the connect budget is zero, so
/// `pool.acquire` reports the spent deadline before any controller work, with
/// no load and no sleep. The failure must keep the timeout class and must not
/// cache the subsystem as failed: the same lanes serve the member once the
/// fixture grants are back.
#[test]
fn a_spent_connect_budget_does_not_quarantine_the_subsystem() {
    use crate::distributed::nvmeof::live_connect::LiveConnectionRegistry;
    use crate::distributed::nvmeof::live_pool::LiveControllerPool;
    let mut lane = subsystem_lane();
    let grants = std::mem::take(&mut lane.cache.grants);
    let pool = Arc::new(
        LiveControllerPool::new(
            LiveConnectionRegistry::open(&lane._dir.path().join("registry")).unwrap(),
            8,
            4,
        )
        .unwrap(),
    );
    lane.client.set_live_pool(pool.clone());
    lane.client.live_exports = Some((
        1,
        crate::distributed::tcp::LiveExportInfo {
            address: "127.0.0.1".into(),
            service: 4420,
            transport: crate::distributed::nvmeof::NvmeofTransport::Tcp,
            wal_file_bytes: 8192,
            block_size: 4096,
            exported_files: 2,
        },
        2,
    ));
    lane.client
        .live_config
        .as_mut()
        .expect("the lane client is configured")
        .connect_timeout_ms = 0;
    let a = lane.a;
    lane.submit(vec![a]);
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("a spent connect budget cannot serve the member");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "the connect budget keeps its class: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a deadline must not cache the subsystem as failed"
    );
    // Put the fixture grants back: the same lane serves the member, so the
    // deadline did not poison the read authority.
    lane.cache.grants = grants;
    lane.client
        .live_config
        .as_mut()
        .expect("the lane client is configured")
        .connect_timeout_ms = 1000;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0].result.as_ref().unwrap(),
        &vec![frame],
        "the member is served after the connect deadline"
    );
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A namespace attach that hits its connect deadline is transient: the grant
/// keeps its other namespaces and the subsystem is not cached as failed. The
/// pool caches member 0's namespace only, so member 1's attach misses and
/// `resolve_namespace` reports the spent budget before any controller work.
#[test]
fn a_namespace_attach_deadline_keeps_the_grant() {
    use crate::distributed::nvmeof::NvmeofTransport;
    use crate::distributed::nvmeof::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
    use crate::distributed::nvmeof::live_pool::LiveControllerPool;
    let mut lane = subsystem_lane();
    let grants = std::mem::take(&mut lane.cache.grants);
    let config = lane
        .client
        .live_config
        .clone()
        .expect("the lane client is configured");
    let database = lane.client.expected.inventory.database;
    let pool = Arc::new(
        LiveControllerPool::new(
            LiveConnectionRegistry::open(&lane._dir.path().join("registry")).unwrap(),
            8,
            4,
        )
        .unwrap(),
    );
    let name = wal_export_name(1024, database, 2, 1, 0);
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
        &lane._dir.path().join("wal-0"),
    )
    .unwrap();
    lane.client.set_live_pool(pool.clone());
    lane.client.live_exports = Some((
        1,
        crate::distributed::tcp::LiveExportInfo {
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            wal_file_bytes: 8192,
            block_size: 4096,
            exported_files: 2,
        },
        2,
    ));
    // A normal read takes the pooled controller and attaches member 0.
    let a = lane.a;
    let first_frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0].result.as_ref().unwrap(),
        &vec![first_frame],
        "the pooled controller opens the grant for member 0"
    );
    assert_eq!(lane.cache.grants.len(), 1, "one subsystem grant");
    // Member 1's namespace is not attached yet, and the connect budget is
    // spent when its attach resolves.
    lane.client
        .live_config
        .as_mut()
        .expect("the lane client is configured")
        .connect_timeout_ms = 0;
    let b = lane.b;
    lane.submit(vec![b]);
    let done = lane.collect(1);
    let error = done[0]
        .result
        .as_ref()
        .expect_err("a spent attach budget cannot serve the member");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "the attach deadline keeps its class: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a deadline must not cache the subsystem as failed"
    );
    assert!(
        !lane.cache.grants[0].broken,
        "a deadline must not break the grant"
    );
    // The grant still serves member 0: the attach deadline left it usable.
    lane.client
        .live_config
        .as_mut()
        .expect("the lane client is configured")
        .connect_timeout_ms = 1000;
    let a = lane.a;
    let again_frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0].result.as_ref().unwrap(),
        &vec![again_frame],
        "the grant still serves its first member"
    );
    lane.cache.grants = grants;
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

/// A control exchange that hits its deadline is a timeout, not a broken
/// subsystem. The exchange budget is capped at zero, so the export control
/// reports the deadline before the owner's reply is observed; the owner is
/// never consulted and no wall clock decides the outcome. The failure must
/// keep the timeout class and must not cache the subsystem as failed.
#[test]
fn an_open_live_exports_deadline_does_not_quarantine_the_subsystem() {
    let mut lane = subsystem_lane();
    let grants = std::mem::take(&mut lane.cache.grants);
    lane.client.live_exports = None;
    lane.client.reactor.control_cap = Some(Duration::ZERO);
    let position = lane.a;
    let mut state = InFlightRequest {
        positions: vec![position],
        deadline: Instant::now() + Duration::from_secs(10),
        retries: 0,
        io_deadline: None,
        trace: None,
        slots: vec![Slot::Pending],
        output: vec![None],
        pending: 1,
        first_pending: 0,
        flights: 0,
        error: None,
        submitted_at: None,
        last_cqe: None,
        retry_at: None,
        connect_since: None,
    };
    let error = match lane.cache.open_permanent(
        &mut lane.client,
        1,
        &mut state,
        0,
        Duration::from_millis(1000),
    ) {
        Ok(_) => panic!("a spent control budget cannot open the exports"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "the control deadline keeps its class: {error}"
    );
    assert!(
        lane.cache.failed_subsystems.is_empty(),
        "a deadline must not cache the subsystem as failed"
    );
    // The deadline poisoned nothing: the fixture grants still serve the member.
    lane.client.reactor.control_cap = None;
    lane.cache.grants = grants;
    let a = lane.a;
    let frame = member_frame(&lane.first, &a);
    lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(
        done[0].result.as_ref().unwrap(),
        &vec![frame],
        "the fixture grants still serve the member"
    );
}
