use super::*;

#[test]
fn preparation_triggers_coalesce_when_the_worker_is_busy() {
    use crate::distributed::storage::LivePausePoint;
    use crate::distributed::tcp::leased_tests::Fixture;
    let fixture = Fixture::new(DurabilityTier::Native, 16 * 1024);
    let _client = fixture.writer_without_ahead();
    let log = fixture.node.log_for_test(1);
    log.lease_live_extent(RequestAuthority { epoch: 1 })
        .unwrap();
    let (entered, release) = log.pause_live(LivePausePoint::PrepareBeforePublish);
    let shared = &fixture.node.control.shared;
    shared.trigger_file_preparation(&log);
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..128 {
        shared.trigger_file_preparation(&log);
    }
    {
        let slots = shared.preparers.lock();
        assert_eq!(slots.len(), 1);
        assert!(matches!(
            slots[0].trigger.try_send(()),
            Err(mpsc::TrySendError::Full(()))
        ));
    }
    release.send(()).unwrap();
    shared.retire_syncer(&log).unwrap();
    assert!(shared.preparers.lock().is_empty());
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
}
