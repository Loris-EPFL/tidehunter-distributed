use super::*;
#[test]
fn io_timings_use_retired_cq_and_submission_counts() {
    use crate::distributed::nvmeof::{live_io::LiveIoStats, live_metrics::LiveNvmeMetrics};
    let metrics = LiveNvmeMetrics::new();
    let counters = metrics.register().unwrap();
    let stats = LiveIoStats {
        completed_write_ops: 1,
        completed_write_bytes: 4096,
        submissions: 1,
        completions: 1,
        queue_ns: 11,
        submit_ns: 13,
        submit_to_cq_ns: 17,
        ..LiveIoStats::default()
    };
    record_write_io(&counters, &LiveIoStats::default(), &stats);
    let second = LiveIoStats {
        completed_write_ops: 2,
        completed_write_bytes: 8192,
        submissions: 2,
        completions: 2,
        queue_ns: 22,
        submit_ns: 26,
        submit_to_cq_ns: 34,
        ..LiveIoStats::default()
    };
    record_write_io(&counters, &stats, &second);
    let counts = metrics.snapshot().counts;
    assert_eq!(
        (counts.completed_write_ops, counts.completed_write_bytes),
        (2, 8192)
    );
    assert_eq!((counts.queue_ns, counts.queue_samples), (22, 2));
    assert_eq!((counts.submit_ns, counts.submit_samples), (26, 2));
    assert_eq!((counts.cq_ns, counts.cq_samples), (34, 2));
}

#[test]
fn local_file_reuses_write_queue_with_per_group_stats() {
    use crate::distributed::nvmeof::{
        live_io::{LiveIo, LiveIoResult, LiveSubmitOutcome},
        live_metrics::LiveNvmeMetrics,
        live_namespace::{LiveNamespace, LiveNamespaceAccess},
    };
    use std::os::unix::fs::FileExt;
    use std::time::{Duration, Instant};
    let dir = tempdir::TempDir::new("live-writer-reuse").unwrap();
    let path = dir.path().join("wal");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(8192).unwrap();
    drop(file);
    let namespace =
        LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadWrite).unwrap();
    let mut io = LiveIo::new(namespace, 1, 8191, false).unwrap();
    let metrics = LiveNvmeMetrics::new();
    let counters = metrics.register().unwrap();
    for index in 0..2 {
        let before = io.stats();
        let frame = vec![index as u8 + 1; 4096];
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_write(
                1,
                index * 4096,
                &frame,
                crate::distributed::nvmeof::live_io::LiveWriteSync::None,
                deadline,
            )
            .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        loop {
            let mut completions = io.progress().unwrap();
            if let Some(completion) = completions.pop() {
                assert!(matches!(completion.result, Ok(LiveIoResult::Written)));
                break;
            }
            assert!(Instant::now() < deadline, "local write did not retire");
            std::thread::yield_now();
        }
        record_write_io(&counters, &before, &io.stats());
    }
    let counts = metrics.snapshot().counts;
    assert_eq!(
        (counts.completed_write_ops, counts.completed_write_bytes),
        (2, 8192)
    );
    assert_eq!((counts.queue_samples, counts.cq_samples), (2, 2));
    assert!(io.shutdown().unwrap().is_empty());
    let file = std::fs::File::open(path).unwrap();
    let mut bytes = [0u8; 8192];
    file.read_exact_at(&mut bytes, 0).unwrap();
    assert!(bytes[..4096].iter().all(|&b| b == 1));
    assert!(bytes[4096..].iter().all(|&b| b == 2));
}

#[test]
fn rejects_invalid_initiator_identity_and_queue_limits() {
    let mut config = LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        host_nqn: "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-1111-1111-111111111111".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        leased_write_window: Some(4),
        leased_sync: Default::default(),
        leased_flush_min_frame_bytes: 0,
        max_inflight_requests: 32,
        max_inflight_bytes: 4096,
        connect_timeout_ms: 1000,
        io_timeout_ms: 1000,
        leased_durability: crate::distributed::durability::DurabilityTier::Native,
        inline_reads: true,
        queue_geometry: Default::default(),
    };
    assert!(config.validate().is_ok());
    let mut old = serde_json::to_value(&config).unwrap();
    old.as_object_mut().unwrap().remove("queue_geometry");
    let mut removed = serde_json::to_value(&config).unwrap();
    removed["export_mode"] = serde_json::json!("per_owner");
    assert!(serde_json::from_value::<LiveClientConfig>(removed).is_err());
    old.as_object_mut().unwrap().remove("leased_write_window");
    old.as_object_mut().unwrap().remove("leased_sync");
    old.as_object_mut()
        .unwrap()
        .remove("leased_flush_min_frame_bytes");
    assert!(!old.as_object().unwrap().contains_key("owner_identities"));
    let restored: LiveClientConfig = serde_json::from_value(old).unwrap();
    assert_eq!(restored.queue_geometry, LiveQueueGeometry::default());
    assert_eq!(restored.leased_write_window, None);
    assert_eq!(restored.leased_sync, LeasedSync::GroupFlush);
    assert_eq!(restored.leased_flush_min_frame_bytes, 65536);
    config.leased_flush_min_frame_bytes = 8192;
    let restored: LiveClientConfig =
        serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
    assert_eq!(restored.leased_flush_min_frame_bytes, 8192);
    assert_eq!(
        config.for_owner(1).unwrap().leased_flush_min_frame_bytes,
        8192
    );
    for (mode, name) in [
        (LeasedSync::PerFrameFua, "per_frame_fua"),
        (LeasedSync::GroupFlush, "group_flush"),
    ] {
        assert_eq!(serde_json::to_value(mode).unwrap(), name);
        assert_eq!(
            serde_json::from_value::<LeasedSync>(serde_json::json!(name)).unwrap(),
            mode
        );
    }
    assert!(restored.owner_identities.is_empty());
    config.leased_write_window = None;
    assert!(config.validate().is_ok());
    config.leased_write_window = Some(0);
    assert!(config.validate().is_err());
    config.leased_write_window = Some(config.max_inflight_requests + 1);
    assert!(config.validate().is_err());
    config.leased_write_window = Some(config.max_inflight_requests);
    assert!(config.validate().is_ok());
    config.leased_write_window = Some(4);
    config.queue_geometry.poll_queues = 1;
    assert!(config.validate().is_ok());
    let mut polled = config.clone();
    polled.polled_writes = true;
    polled.leased_durability = crate::distributed::durability::DurabilityTier::Synchronous;
    polled.leased_sync = LeasedSync::GroupFlush;
    assert!(polled.validate().is_ok());
    config.inline_reads = false;
    assert_eq!(
        config.validate(),
        Err("polled live reads require inline reads")
    );
    config.inline_reads = true;
    config.queue_geometry.io_queues = 0;
    assert_eq!(config.validate(), Err("invalid live queue geometry"));
    config.queue_geometry = LiveQueueGeometry::default();
    config.host_id = [0; 16];
    assert!(config.validate().is_err());
    config.host_id = [1; 16];
    config.host_nqn.push(',');
    assert!(config.validate().is_err());
    config.host_nqn.pop();
    config.max_inflight_requests = 0;
    assert!(config.validate().is_err());
    config.max_inflight_requests = 32769;
    assert!(config.validate().is_err());
    config.max_inflight_requests = 1;
    config.leased_write_window = Some(1);
    config.io_timeout_ms = 0;
    assert!(config.validate().is_err());
    let first = LiveInitiatorIdentity {
        host_nqn: "nqn.2026-01.test:owner-0".into(),
        host_id: [2; 16],
    };
    let second = LiveInitiatorIdentity {
        host_nqn: "nqn.2026-01.test:owner-1".into(),
        host_id: [3; 16],
    };
    config.io_timeout_ms = 1000;
    config.owner_identities = [(0, first.clone()), (1, second.clone())].into();
    assert!(config.validate().is_ok());
    assert_eq!(config.for_owner(0).unwrap().host_nqn, first.host_nqn);
    assert_eq!(config.for_owner(1).unwrap().host_id, second.host_id);
    assert!(config.for_owner(2).is_err());
    config.owner_identities.get_mut(&1).unwrap().host_nqn = first.host_nqn;
    assert!(config.validate().is_err());
    config.owner_identities.get_mut(&1).unwrap().host_nqn = second.host_nqn;
    config.owner_identities.get_mut(&1).unwrap().host_id = first.host_id;
    assert!(config.validate().is_err());
}
