use super::super::pipeline_tests::Fixture;
use super::*;

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn open(
    fixture: &Fixture,
    pool: &Arc<RemoteTransportPool>,
) -> Result<RemoteDatabase, LocalDbError> {
    RemoteDatabase::open_with_transport_pool(
        &fixture.temp.path().join("authority"),
        &fixture.shape,
        fixture.config.clone(),
        metrics(),
        pool.clone(),
    )
}

fn put(value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: 7u32.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

#[test]
fn clean_reuse_recovers_retry_identity_and_rejects_old_read_handles() {
    let fixture = Fixture::new();
    let pool = Arc::new(RemoteTransportPool::new());
    let mut db = open(&fixture, &pool).unwrap();
    let operation = fixture.operation(0, 1);
    let first = db.write_batch(operation, vec![put(9)]).unwrap();
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![9; 32].into())
    );
    let old = db.submit_get(KeySpace(0), &7u32.to_be_bytes()).unwrap();
    db.shutdown().unwrap();
    drop(db);

    let mut db = open(&fixture, &pool).unwrap();
    assert_eq!(db.epoch(), 2);
    assert_eq!(db.recovery_stats().reused_connections, 15);
    assert!(db.finish_get(old).is_err());
    assert_eq!(db.write_batch(operation, vec![put(9)]).unwrap(), first);
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![9; 32].into())
    );
    db.write_batch(fixture.operation(4, 2), vec![put(11)])
        .unwrap();
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![11; 32].into())
    );
    db.shutdown().unwrap();
    drop(db);

    let mut db = open(&fixture, &pool).unwrap();
    assert_eq!(db.epoch(), 3);
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![11; 32].into())
    );
    db.shutdown().unwrap();
    drop(db);
    drop(pool);
}

#[test]
fn reused_transport_refreshes_inventory_before_reserving_an_epoch() {
    let fixture = Fixture::new();
    let pool = Arc::new(RemoteTransportPool::new());
    let mut db = open(&fixture, &pool).unwrap();
    db.write_batch(fixture.operation(0, 1), vec![put(9)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    // The live peer changed after clean shutdown. Cached connect-time status
    // would incorrectly certify an incomplete authority manifest here.
    {
        let idle = pool.idle.lock();
        idle.as_ref().unwrap().controls[0]
            .lock()
            .fence_to(2)
            .unwrap();
    }
    assert!(open(&fixture, &pool).is_err());
    let manifest: AuthorityManifest =
        read_metadata(&fixture.temp.path().join("authority").join(MANIFEST)).unwrap();
    assert_eq!(manifest.epochs.len(), 1);
    assert!(pool.idle.lock().is_none());
}

#[test]
fn changed_trust_anchor_cannot_reuse_an_authenticated_old_connection() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let pool = Arc::new(RemoteTransportPool::new());
    let mut db = open(&fixture, &pool).unwrap();
    db.write_batch(fixture.operation(0, 1), vec![put(9)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    // Change the bytes at the same configured path, not just the path itself.
    fs::write(
        &fixture.config.tls.ca,
        fs::read(&other.config.tls.ca).unwrap(),
    )
    .unwrap();
    assert!(open(&fixture, &pool).is_err());
    assert!(pool.idle.lock().is_none());
    let manifest: AuthorityManifest =
        read_metadata(&fixture.temp.path().join("authority").join(MANIFEST)).unwrap();
    assert_eq!(manifest.epochs.len(), 1);
}

#[test]
fn failed_checkpoint_shutdown_discards_transports_and_recovers_durable_tail() {
    let fixture = Fixture::new();
    let pool = Arc::new(RemoteTransportPool::new());
    let mut db = open(&fixture, &pool).unwrap();
    db.write_batch(fixture.operation(0, 1), vec![put(9)])
        .unwrap();
    db.checkpoint_interruption = Some(1);
    assert!(db.shutdown().is_err());
    drop(db);
    assert!(pool.idle.lock().is_none());
    let mut db = open(&fixture, &pool).unwrap();
    assert_eq!(db.recovery_stats().reused_connections, 0);
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![9; 32].into())
    );
    db.shutdown().unwrap();
    drop(db);
    pool.clear();
}
