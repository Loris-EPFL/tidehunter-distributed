//! Reopen regressions over the leased live fixture. Every open connects fresh
//! owner sessions: the shared transport pool was removed with R3.

use super::super::pipeline_tests::Fixture;
use super::*;

fn put(value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: 7u32.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

#[test]
fn clean_reopen_recovers_retry_identity_and_rejects_old_read_handles() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let operation = fixture.operation(0, 1);
    let first = db.write_batch(operation, vec![put(9)]).unwrap();
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![9; 32].into())
    );
    let old = db.submit_get(KeySpace(0), &7u32.to_be_bytes()).unwrap();
    db.shutdown().unwrap();
    drop(db);

    let mut db = fixture.open();
    assert_eq!(db.epoch(), 2);
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

    let mut db = fixture.open();
    assert_eq!(db.epoch(), 3);
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![11; 32].into())
    );
    db.shutdown().unwrap();
}

#[test]
fn failed_checkpoint_shutdown_recovers_durable_tail() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(9)])
        .unwrap();
    db.core.test_hooks.lock().checkpoint_interruption = Some(1);
    assert!(db.shutdown().is_err());
    drop(db);
    let mut db = fixture.open();
    assert_eq!(
        db.get(KeySpace(0), &7u32.to_be_bytes()).unwrap(),
        Some(vec![9; 32].into())
    );
    db.shutdown().unwrap();
}
