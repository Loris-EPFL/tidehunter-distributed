use super::*;
use crate::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use crate::distributed::group::GroupLimits;
use crate::distributed::storage::{PartitionConfig, StaticInventory, WalMemberReadPolicy};
use crate::distributed::types::{BatchId, BatchVersion, RequestAuthority};
use crate::key_shape::{KeyShape, KeySpace, KeyType};
use crate::metrics::Metrics;
use std::os::unix::fs::FileExt;
use std::time::{Duration, Instant};

struct Fixture {
    _temp: tempdir::TempDir,
    shape: KeyShape,
    log: Arc<PartitionLog>,
    references: Vec<ValueRecordReference>,
}
impl Fixture {
    fn new(count: usize) -> Self {
        let temp = tempdir::TempDir::new("uring-member-test").unwrap();
        let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
        let max_encoded_bytes = 64 + 512;
        let max_group_bytes = max_encoded_bytes + 512;
        let fragment_bytes = ((max_group_bytes as u64 + 15) / 8) * 8;
        let config = PartitionConfig {
            inventory: StaticInventory {
                database: [1; 16],
                epoch: 5,
                segment_generation: 27,
                partitions: vec![3],
            },
            partition: 3,
            batch_limits: BatchLimits {
                max_operations: 100,
                max_encoded_bytes,
            },
            group_limits: GroupLimits {
                max_batches: 4,
                max_encoded_bytes: max_group_bytes,
            },
            fragment_bytes,
            wal_file_bytes: fragment_bytes * 4,
            max_batches: 64,
            max_reference_metadata_bytes: 1024 * 1024,
        };
        let log = Arc::new(
            PartitionLog::create(
                &temp.path().join("stream"),
                config.clone(),
                &shape,
                Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            )
            .unwrap(),
        );
        let mut references = Vec::new();
        for n in 0..count {
            let mut operation = [0; 16];
            operation[..8].copy_from_slice(&(n as u64).to_le_bytes());
            let batch = CompleteBatch::new(
                &shape,
                BatchId {
                    database: [1; 16],
                    operation,
                },
                BatchVersion {
                    epoch: 5,
                    sequence: n as u64,
                },
                vec![Self::mutation(n)],
                config.batch_limits,
            )
            .unwrap();
            let receipt = log
                .append_complete_batch(RequestAuthority { epoch: 5 }, &batch)
                .unwrap();
            log.persist_through(receipt).unwrap();
            references.push(log.record_reference(receipt, 0).unwrap());
        }
        log.seal_epoch(RequestAuthority { epoch: 5 }).unwrap();
        Self {
            _temp: temp,
            shape,
            log,
            references,
        }
    }
    fn mutation(n: usize) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: vec![n as u8; 64].into(),
        }
    }
    fn member(&self, n: usize) -> Vec<(Arc<PartitionLog>, ValueRecordReference)> {
        vec![(self.log.clone(), self.references[n])]
    }
    fn assert_value(&self, n: usize, bytes: Bytes) {
        assert_eq!(
            CompleteBatch::decode_record(&self.shape, &self.references[n], bytes).unwrap(),
            Self::mutation(n)
        );
    }
}
fn executor(config: ReadExecutorConfig) -> (Arc<ReadAdmission>, ReadExecutor) {
    let admission = Arc::new(ReadAdmission::new(config).unwrap());
    // Deliberately fail, not silently skip, if the tested Linux host cannot run
    // io_uring. An unsupported kernel does not prove genuine asynchronous I/O.
    let executor = ReadExecutor::new(config, admission.clone()).unwrap();
    (admission, executor)
}
fn finish(executor: &mut ReadExecutor, count: usize) -> Vec<ReadCompletion> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut completed = Vec::new();
    while completed.len() < count {
        completed.extend(executor.poll_completed().unwrap());
        assert!(
            Instant::now() < deadline,
            "asynchronous completions timed out"
        );
        std::thread::yield_now();
    }
    completed
}
fn read(executor: &mut ReadExecutor, fixture: &Fixture, n: usize, id: u64) -> Bytes {
    executor.begin_sweep();
    match executor
        .submit(id, fixture.member(n), ReadClass::Point)
        .unwrap()
    {
        SubmitOutcome::Ready {
            mut records,
            lease: _lease,
        } => records.remove(0),
        SubmitOutcome::Pending => finish(executor, 1).remove(0).result.unwrap().remove(0),
        SubmitOutcome::Full(_) => panic!("unexpected admission pressure"),
    }
}

#[test]
fn independent_misses_are_inflight_together_and_members_keep_wire_order() {
    let fixture = Fixture::new(3);
    let (admission, mut executor) = executor(ReadExecutorConfig::default());
    assert!(matches!(
        executor
            .submit(11, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    let members = vec![
        (fixture.log.clone(), fixture.references[2]),
        (fixture.log.clone(), fixture.references[1]),
    ];
    assert!(matches!(
        executor.submit(22, members, ReadClass::Point).unwrap(),
        SubmitOutcome::Pending
    ));
    let stats = admission.stats();
    assert_eq!(stats.inflight_requests, 2);
    assert_eq!(stats.inflight_reads, 3);
    assert_eq!(stats.wal_reads, 3);
    assert_eq!(stats.peak_device_reads, 3);
    let mut completed = finish(&mut executor, 2);
    completed.sort_by_key(|c| c.request_id);
    fixture.assert_value(0, completed.remove(0).result.unwrap().remove(0));
    let mut second = completed.remove(0).result.unwrap();
    fixture.assert_value(2, second.remove(0));
    fixture.assert_value(1, second.remove(0));
    assert_eq!(admission.stats().inflight_bytes, 0);
    assert_eq!(admission.stats().completed_reads, 3);
}

#[test]
fn disabled_cache_repeated_reads_consult_wal_and_sync_async_is_rejected() {
    let fixture = Fixture::new(1);
    let (admission, mut executor) = executor(ReadExecutorConfig::default());
    for id in 0..3 {
        fixture.assert_value(0, read(&mut executor, &fixture, 0, id));
    }
    let stats = admission.stats();
    assert_eq!(stats.cache_budget, 0);
    assert_eq!(stats.cache_entries, 0);
    assert_eq!(stats.cache_bytes, 0);
    assert_eq!(stats.cache_hits, 0);
    assert_eq!(stats.cache_misses, 3);
    assert_eq!(stats.wal_reads, 3);
    assert!(
        fixture
            .log
            .read_record_encoded_with_policy(&fixture.references[0], WalMemberReadPolicy::Async)
            .is_err()
    );
    for policy in [WalMemberReadPolicy::Mapped, WalMemberReadPolicy::Positional] {
        fixture.assert_value(
            0,
            fixture
                .log
                .read_record_encoded_with_policy(&fixture.references[0], policy)
                .unwrap(),
        );
    }
}

#[test]
fn resident_cache_is_node_bounded_evictable_and_hit_does_not_queue_disk() {
    let fixture = Fixture::new(12);
    let config = ReadExecutorConfig {
        cache_bytes: 8192,
        max_requests: 2,
        max_device_reads: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut first) = executor(config);
    let mut second = ReadExecutor::new(config, admission.clone()).unwrap();
    let survivor = read(&mut first, &fixture, 0, 1);
    let wal_reads = admission.stats().wal_reads;
    assert!(matches!(
        second
            .submit(2, fixture.member(1), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    // Even while the sole device credit is occupied, hits stay immediate.
    assert!(matches!(
        first
            .submit(3, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Ready { .. }
    ));
    assert_eq!(admission.stats().wal_reads, wal_reads + 1);
    finish(&mut second, 1);
    for n in 2..12 {
        fixture.assert_value(n, read(&mut first, &fixture, n, n as u64 + 10));
        let stats = admission.stats();
        assert!(stats.cache_bytes <= config.cache_bytes);
        assert!(stats.cache_metadata_bytes > 0);
    }
    assert!(admission.stats().cache_evictions > 0);
    assert!(admission.stats().cache_entries < fixture.references.len());
    // Responses own their bytes independently of subsequent cache eviction.
    fixture.assert_value(0, survivor);
    admission.clear_cache();
    let cleared = admission.stats();
    assert_eq!(cleared.cache_entries, 0);
    assert_eq!(cleared.cache_bytes, cleared.cache_metadata_bytes);
    fixture.assert_value(0, read(&mut second, &fixture, 0, 100));
    assert_eq!(admission.stats().wal_reads, cleared.wal_reads + 1);
}

#[test]
fn oversized_entries_bypass_without_exceeding_cache_budget() {
    let fixture = Fixture::new(1);
    let mut stats = ReadExecutorStats::default();
    let mut cache = cache::Cache::new(8192, &mut stats);
    let huge = Bytes::from(vec![9; 16384].into_boxed_slice());
    cache.insert(fixture.references[0], &huge, &mut stats);
    assert_eq!(stats.cache_entries, 0);
    assert_eq!(stats.cache_oversized, 1);
    assert!(stats.cache_bytes <= stats.cache_budget);
    assert!(cache.get(&fixture.references[0]).is_none());
    // A nonzero budget smaller than table metadata is a bypass too, not growth.
    let (admission, mut executor) = executor(ReadExecutorConfig {
        cache_bytes: 1,
        ..ReadExecutorConfig::default()
    });
    for id in 0..2 {
        fixture.assert_value(0, read(&mut executor, &fixture, 0, id));
    }
    assert_eq!(admission.stats().wal_reads, 2);
    assert_eq!(admission.stats().cache_bytes, 0);
    assert_eq!(admission.stats().cache_oversized, 2);
}

#[test]
fn cache_hits_cannot_bypass_full_reference_membership_validation() {
    let fixture = Fixture::new(1);
    let (admission, mut executor) = executor(ReadExecutorConfig {
        cache_bytes: 8192,
        ..ReadExecutorConfig::default()
    });
    read(&mut executor, &fixture, 0, 1);
    let before = admission.stats();
    let mut forged = fixture.references[0];
    forged.batch_digest.0[0] ^= 1;
    assert!(
        executor
            .submit(2, vec![(fixture.log.clone(), forged)], ReadClass::Point)
            .is_err()
    );
    assert_eq!(admission.stats(), before);
}

#[test]
fn clear_prevents_older_inflight_reads_refilling_resident_cache() {
    let fixture = Fixture::new(1);
    let (admission, mut executor) = executor(ReadExecutorConfig {
        cache_bytes: 8192,
        ..ReadExecutorConfig::default()
    });
    assert!(matches!(
        executor
            .submit(1, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    admission.clear_cache();
    finish(&mut executor, 1);
    assert_eq!(admission.stats().cache_entries, 0);
    read(&mut executor, &fixture, 0, 2);
    assert_eq!(admission.stats().wal_reads, 2);
    assert_eq!(admission.stats().cache_entries, 1);
}

#[test]
fn aggregate_request_and_device_limits_cover_independent_connection_rings() {
    let fixture = Fixture::new(3);
    for config in [
        ReadExecutorConfig {
            max_requests: 1,
            ..ReadExecutorConfig::default()
        },
        ReadExecutorConfig {
            max_device_reads: 1,
            ..ReadExecutorConfig::default()
        },
    ] {
        let (admission, mut first) = executor(config);
        let mut second = ReadExecutor::new(config, admission.clone()).unwrap();
        assert!(matches!(
            first
                .submit(1, fixture.member(0), ReadClass::Point)
                .unwrap(),
            SubmitOutcome::Pending
        ));
        assert!(matches!(
            second
                .submit(2, fixture.member(1), ReadClass::Point)
                .unwrap(),
            SubmitOutcome::Full(_)
        ));
        assert_eq!(admission.stats().wal_reads, 1);
        assert_eq!(admission.stats().cache_misses, 1);
        finish(&mut first, 1);
        assert!(matches!(
            second
                .submit(2, fixture.member(1), ReadClass::Point)
                .unwrap(),
            SubmitOutcome::Pending
        ));
        finish(&mut second, 1);
        assert_eq!(admission.stats().inflight_requests, 0);
        assert!(admission.stats().peak_device_reads <= config.max_device_reads);
    }
}

#[test]
fn aggregate_byte_budget_and_per_request_oversize_are_distinct() {
    let fixture = Fixture::new(2);
    let (sizing_admission, mut sizing) = executor(ReadExecutorConfig::default());
    sizing
        .submit(1, fixture.member(0), ReadClass::Point)
        .unwrap();
    let charge = sizing_admission.stats().inflight_bytes;
    finish(&mut sizing, 1);
    let config = ReadExecutorConfig {
        max_bytes: charge,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut first) = executor(config);
    let mut second = ReadExecutor::new(config, admission.clone()).unwrap();
    assert!(matches!(
        first
            .submit(1, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    assert!(matches!(
        second
            .submit(2, fixture.member(1), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Full(_)
    ));
    assert_eq!(admission.stats().inflight_bytes, charge);
    finish(&mut first, 1);
    let too_large = vec![
        (fixture.log.clone(), fixture.references[0]),
        (fixture.log.clone(), fixture.references[1]),
    ];
    assert!(matches!(
        second.submit(3, too_large, ReadClass::Point),
        Err(StorageError::Capacity)
    ));
    assert_eq!(admission.stats().inflight_bytes, 0);
}

#[test]
fn short_read_is_reported_and_never_cached_even_after_cancellation() {
    for cancel in [false, true] {
        let fixture = Fixture::new(1);
        let plan = fixture
            .log
            .plan_record_read(&fixture.references[0])
            .unwrap();
        plan.member
            .file
            .set_len(plan.member.offset + plan.member.len as u64 - 1)
            .unwrap();
        let (admission, mut executor) = executor(ReadExecutorConfig {
            cache_bytes: 8192,
            ..ReadExecutorConfig::default()
        });
        assert!(matches!(
            executor
                .submit(1, fixture.member(0), ReadClass::Point)
                .unwrap(),
            SubmitOutcome::Pending
        ));
        if cancel {
            executor.cancel(1);
        }
        let completion = finish(&mut executor, 1).remove(0);
        assert!(
            matches!(&completion.result, Err(StorageError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
        );
        assert_eq!(admission.stats().errors, 1);
        assert_eq!(admission.stats().cache_entries, 0);
        drop(completion);
        assert_eq!(admission.stats().inflight_bytes, 0);
    }
}

#[test]
fn cancellation_and_drop_keep_file_segment_and_authority_until_drain() {
    let fixture = Fixture::new(1);
    let plan = fixture
        .log
        .plan_record_read(&fixture.references[0])
        .unwrap();
    let file = Arc::downgrade(&plan.member.file);
    let log = Arc::downgrade(&fixture.log);
    drop(plan);
    let (admission, mut executor) = executor(ReadExecutorConfig::default());
    executor
        .submit(1, fixture.member(0), ReadClass::Point)
        .unwrap();
    let before = admission.stats().inflight_bytes;
    executor.cancel(1);
    drop(fixture.log);
    assert!(file.upgrade().is_some());
    assert!(log.upgrade().is_some());
    assert_eq!(admission.stats().inflight_bytes, before);
    executor.shutdown().unwrap();
    assert_eq!(admission.stats().inflight_bytes, 0);
    assert_eq!(admission.stats().cancellations, 1);
    assert!(file.upgrade().is_none());
    assert!(log.upgrade().is_none());
    assert!(executor.poll_completed().unwrap().is_empty());

    let fixture = Fixture::new(1);
    let log = Arc::downgrade(&fixture.log);
    let (admission, mut executor) = self::executor(ReadExecutorConfig::default());
    executor
        .submit(2, fixture.member(0), ReadClass::Point)
        .unwrap();
    drop(fixture.log);
    drop(executor); // Drop itself drains; no thread or kernel-pointer leak.
    assert!(log.upgrade().is_none());
    assert_eq!(admission.stats().inflight_bytes, 0);
}

#[test]
fn shutdown_retries_submission_pressure_before_releasing_operation_owners() {
    let fixture = Fixture::new(1);
    let plan = fixture
        .log
        .plan_record_read(&fixture.references[0])
        .unwrap();
    let file = Arc::downgrade(&plan.member.file);
    let log = Arc::downgrade(&fixture.log);
    drop(plan);
    let (admission, mut executor) = executor(ReadExecutorConfig::default());
    executor
        .submit_errors
        .extend([libc::EAGAIN, libc::EBUSY, libc::EINTR]);
    assert!(matches!(
        executor
            .submit(1, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    drop(fixture.log);
    assert!(file.upgrade().is_some());
    assert!(log.upgrade().is_some());
    assert_eq!(admission.stats().inflight_reads, 1);
    assert!(admission.stats().inflight_bytes > 0);
    executor.shutdown().unwrap();
    let stats = admission.stats();
    assert_eq!(stats.wal_reads, 1);
    assert_eq!(stats.completed_reads, 1);
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.inflight_reads, 0);
    assert_eq!(stats.inflight_requests, 0);
    assert_eq!(stats.inflight_bytes, 0);
    assert!(file.upgrade().is_none());
    assert!(log.upgrade().is_none());
}

#[test]
fn response_lease_retains_request_and_byte_credit_after_device_retirement() {
    let fixture = Fixture::new(2);
    let config = ReadExecutorConfig {
        cache_bytes: 8192,
        max_requests: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut executor) = executor(config);
    executor
        .submit(1, fixture.member(0), ReadClass::Point)
        .unwrap();
    let charged = admission.stats().inflight_bytes;
    let mut completion = finish(&mut executor, 1).remove(0);
    assert_eq!(admission.stats().inflight_reads, 0);
    assert_eq!(admission.stats().inflight_requests, 1);
    assert_eq!(admission.stats().inflight_bytes, charged);
    assert!(matches!(
        executor
            .submit(2, fixture.member(1), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Full(_)
    ));
    drop(completion.lease.take());
    assert_eq!(admission.stats().inflight_bytes, 0);
    let SubmitOutcome::Ready { records, lease } = executor
        .submit(3, fixture.member(0), ReadClass::Point)
        .unwrap()
    else {
        panic!("resident reply must be immediate");
    };
    assert_eq!(admission.stats().wal_reads, 1);
    assert_eq!(admission.stats().inflight_requests, 1);
    assert!(admission.stats().inflight_bytes > 0);
    fixture.assert_value(0, records.into_iter().next().unwrap());
    drop(lease);
    assert_eq!(admission.stats().inflight_requests, 0);
}

#[test]
fn consuming_boundary_detects_corruption_on_wal_miss_and_resident_hit() {
    let fixture = Fixture::new(1);
    let plan = fixture
        .log
        .plan_record_read(&fixture.references[0])
        .unwrap();
    plan.member
        .file
        .write_all_at(&[0xff], plan.member.offset + plan.member.len as u64 - 1)
        .unwrap();
    let (admission, mut executor) = executor(ReadExecutorConfig {
        cache_bytes: 8192,
        ..ReadExecutorConfig::default()
    });
    for id in 0..2 {
        let encoded = read(&mut executor, &fixture, 0, id);
        assert!(
            CompleteBatch::decode_record(&fixture.shape, &fixture.references[0], encoded).is_err()
        );
    }
    assert_eq!(admission.stats().wal_reads, 1);
    assert_eq!(admission.stats().cache_hits, 1);
    // The encoded cache never claims integrity verification or suppresses the
    // client's check; it saves I/O, not the authoritative consuming boundary.
}

#[test]
fn diagnostic_reply_credit_uses_same_global_request_and_byte_budget() {
    let admission = Arc::new(
        ReadAdmission::new(ReadExecutorConfig {
            max_requests: 2,
            max_bytes: 1024,
            ..ReadExecutorConfig::default()
        })
        .unwrap(),
    );
    let first = admission.reserve_reply(700).unwrap().unwrap();
    assert!(admission.reserve_reply(400).unwrap().is_none());
    assert!(matches!(
        admission.reserve_reply(1025),
        Err(StorageError::Capacity)
    ));
    let second = admission.reserve_reply(324).unwrap().unwrap();
    assert_eq!(admission.stats().inflight_bytes, 1024);
    assert_eq!(admission.stats().inflight_requests, 2);
    assert!(admission.reserve_reply(1).unwrap().is_none());
    admission.record_sync_read(100, true);
    admission.record_sync_read(20, false);
    assert_eq!(admission.stats().wal_reads, 2);
    assert_eq!(admission.stats().wal_read_bytes, 120);
    assert_eq!(admission.stats().completed_reads, 1);
    assert_eq!(admission.stats().errors, 1);
    drop((first, second));
    assert_eq!(admission.stats().inflight_bytes, 0);
    assert_eq!(admission.stats().inflight_requests, 0);
}

#[test]
fn invalid_admission_configuration_is_rejected() {
    for config in [
        ReadExecutorConfig {
            max_requests: 0,
            ..ReadExecutorConfig::default()
        },
        ReadExecutorConfig {
            max_bytes: 0,
            ..ReadExecutorConfig::default()
        },
        ReadExecutorConfig {
            max_device_reads: 4097,
            ..ReadExecutorConfig::default()
        },
        ReadExecutorConfig {
            cache_bytes: usize::MAX,
            ..ReadExecutorConfig::default()
        },
        ReadExecutorConfig {
            memory_allowance_bytes: 0,
            ..ReadExecutorConfig::default()
        },
        ReadExecutorConfig {
            memory_allowance_bytes: 1024,
            ..ReadExecutorConfig::default()
        },
    ] {
        assert!(ReadAdmission::new(config).is_err());
    }
}

#[test]
fn large_cache_budget_requires_explicit_checked_host_allowance() {
    let cache_bytes = 2 * 1024 * 1024 * 1024;
    let config = ReadExecutorConfig {
        cache_bytes,
        memory_allowance_bytes: cache_bytes + 64 * 1024 * 1024,
        ..ReadExecutorConfig::default()
    };
    assert!(config.validate().is_ok());
    assert!(config.cache_metadata_bytes() > 0);
    assert!(config.cache_metadata_bytes() < cache_bytes);
    assert!(
        ReadExecutorConfig {
            memory_allowance_bytes: config.memory_allowance_bytes - 1,
            ..config
        }
        .validate()
        .is_err()
    );
    assert!(
        ReadExecutorConfig {
            cache_bytes: usize::MAX,
            memory_allowance_bytes: usize::MAX,
            ..config
        }
        .validate()
        .is_err()
    );
}

#[test]
fn scans_consult_hot_records_without_filling_or_evicting_point_cache() {
    let fixture = Fixture::new(12);
    let (admission, mut executor) = executor(ReadExecutorConfig {
        cache_bytes: 8192,
        ..ReadExecutorConfig::default()
    });
    let held = read(&mut executor, &fixture, 0, 1);
    let before = admission.stats();
    for n in 0..12 {
        let encoded = match executor
            .submit(n as u64 + 2, fixture.member(n), ReadClass::ScanNoFill)
            .unwrap()
        {
            SubmitOutcome::Ready {
                mut records,
                lease: _lease,
            } => records.remove(0),
            SubmitOutcome::Pending => finish(&mut executor, 1).remove(0).result.unwrap().remove(0),
            SubmitOutcome::Full(_) => panic!("sequential scan unexpectedly full"),
        };
        fixture.assert_value(n, encoded);
    }
    let after = admission.stats();
    assert_eq!(after.cache_inserts, before.cache_inserts);
    assert_eq!(after.cache_evictions, before.cache_evictions);
    assert_eq!(after.cache_bytes, before.cache_bytes);
    assert_eq!(after.cache_bypass_scan - before.cache_bypass_scan, 11);
    assert_eq!(after.cache_hits - before.cache_hits, 1);
    assert!(matches!(
        executor
            .submit(99, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Ready { .. }
    ));
    fixture.assert_value(0, held);
    let mut forged = fixture.references[0];
    forged.batch_digest.0[0] ^= 1;
    assert!(
        executor
            .submit(
                100,
                vec![(fixture.log.clone(), forged)],
                ReadClass::ScanNoFill
            )
            .is_err()
    );
}

#[test]
fn cache_lock_does_not_block_reply_credit_retirement() {
    let admission = Arc::new(ReadAdmission::new(ReadExecutorConfig::default()).unwrap());
    let lease = admission.reserve_reply(1024).unwrap().unwrap();
    let cache_guard = admission.cache.lock();
    // This same-thread release would deadlock if credits still used the cache lock.
    drop(lease);
    assert_eq!(admission.state.lock().stats.inflight_bytes, 0);
    drop(cache_guard);
}

#[test]
fn shard_cache_pressure_bypasses_instead_of_sweeping_large_table() {
    let fixture = Fixture::new(1);
    let budget = 1024 * 1024;
    let mut stats = ReadExecutorStats::default();
    let mut cache = cache::Cache::new(budget, &mut stats);
    let bytes = Bytes::from(vec![1; 1024].into_boxed_slice());
    for sequence in 0..4096 {
        let mut reference = fixture.references[0];
        reference.batch_version.sequence = sequence;
        cache.insert(reference, &bytes, &mut stats);
        assert!(cache.get(&reference).is_some());
        if stats.cache_entries >= 128 {
            break;
        }
    }
    assert!(stats.cache_entries >= 128);
    let before = stats;
    let large = Bytes::from(
        vec![2; budget - stats.cache_metadata_bytes - cache::OWNER_BYTES].into_boxed_slice(),
    );
    let mut reference = fixture.references[0];
    reference.batch_version.sequence = 10000;
    cache.insert(reference, &large, &mut stats);
    assert_eq!(
        stats.cache_bypass_eviction_limit,
        before.cache_bypass_eviction_limit + 1
    );
    assert!(stats.cache_evictions - before.cache_evictions <= 65);
    assert!(stats.cache_bytes <= budget);
    assert!(cache.get(&reference).is_none());
}

#[test]
fn repeated_async_misses_reuse_exact_storage_after_last_clone_only() {
    let fixture = Fixture::new(3);
    let (admission, mut executor) = executor(ReadExecutorConfig::default());
    let first = read(&mut executor, &fixture, 0, 1);
    let pointer = first.as_ptr();
    let retained = first.clone();
    drop(first);
    let second = read(&mut executor, &fixture, 1, 2);
    assert_ne!(second.as_ptr(), pointer);
    fixture.assert_value(0, retained.clone());
    assert_eq!(admission.stats().buffer_reuses, 0);
    drop(retained);
    assert_eq!(admission.stats().idle_buffers, 1);
    let third = read(&mut executor, &fixture, 2, 3);
    assert_eq!(third.as_ptr(), pointer);
    fixture.assert_value(1, second);
    fixture.assert_value(2, third);
    let stats = admission.stats();
    assert_eq!(stats.buffer_allocations, 2);
    assert_eq!(stats.buffer_reuses, 1);
    assert_eq!(stats.wal_reads, 3);
    assert!(stats.idle_buffer_bytes + stats.inflight_bytes <= admission.config.max_bytes);
}

#[test]
fn cancelled_buffers_stay_owned_until_cq_then_reuse_across_connection_rings() {
    let fixture = Fixture::new(2);
    let (admission, mut first) = executor(ReadExecutorConfig::default());
    let mut second = ReadExecutor::new(admission.config, admission.clone()).unwrap();
    first
        .submit(1, fixture.member(0), ReadClass::Point)
        .unwrap();
    first.cancel(1);
    assert_eq!(admission.stats().idle_buffers, 0);
    assert_eq!(admission.stats().inflight_reads, 1);
    let live = read(&mut second, &fixture, 1, 2);
    assert_eq!(admission.stats().buffer_allocations, 2);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !first.operations.is_empty() {
        assert!(first.poll_completed().unwrap().is_empty());
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(admission.stats().idle_buffers, 1);
    fixture.assert_value(0, read(&mut second, &fixture, 0, 3));
    fixture.assert_value(1, live);
    assert_eq!(admission.stats().buffer_reuses, 1);
    assert_eq!(admission.stats().inflight_bytes, 0);
}

#[test]
fn buffer_pool_uses_exact_sizes_and_existing_node_budget() {
    let config = ReadExecutorConfig {
        max_bytes: 4096,
        max_device_reads: 2,
        ..ReadExecutorConfig::default()
    };
    let admission = Arc::new(ReadAdmission::new(config).unwrap());
    admission.return_buffer(Box::<[u8]>::new_uninit_slice(512));
    admission.return_buffer(Box::<[u8]>::new_uninit_slice(1024));
    admission.return_buffer(Box::<[u8]>::new_uninit_slice(2048));
    assert_eq!(admission.stats().idle_buffers, 2);
    let exact = admission.take_buffer(512);
    assert_eq!(exact.len(), 512);
    assert_eq!(admission.stats().buffer_reuses, 1);
    let different = admission.take_buffer(513);
    assert_eq!(different.len(), 513);
    assert_eq!(admission.stats().buffer_allocations, 1);
    drop((exact, different));
    let lease = admission.reserve_reply(4096).unwrap().unwrap();
    assert_eq!(admission.stats().idle_buffer_bytes, 0);
    admission.return_buffer(Box::<[u8]>::new_uninit_slice(512));
    assert_eq!(admission.stats().idle_buffers, 0);
    drop(lease);
    admission.return_buffer(Box::<[u8]>::new_uninit_slice(512));
    assert_eq!(admission.stats().idle_buffer_bytes, 512);
}

#[test]
fn all_resident_and_cache_hits_bypass_full_async_device_capacity() {
    let fixture = Fixture::new(2);
    let uncached_config = ReadExecutorConfig {
        resident_first: true,
        cache_bytes: 0,
        max_requests: 4,
        max_device_reads: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut blocker) = executor(uncached_config);
    let mut resident = ReadExecutor::new(uncached_config, admission.clone()).unwrap();
    blocker.begin_sweep();
    blocker
        .resident_test_results
        .push_back(ResidentTestResult::Unsupported);
    assert!(matches!(
        blocker
            .submit(1, fixture.member(1), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    resident.begin_sweep();
    resident
        .resident_test_results
        .push_back(ResidentTestResult::Complete);
    let SubmitOutcome::Ready { records, lease } = resident
        .submit(2, fixture.member(0), ReadClass::Point)
        .unwrap()
    else {
        panic!("an exact resident read must bypass full async device capacity")
    };
    fixture.assert_value(0, records.into_iter().next().unwrap());
    drop(lease);
    assert!(resident.operations.is_empty());
    assert_eq!(admission.stats().async_read_submissions, 1);
    drop(finish(&mut blocker, 1));

    let cached_config = ReadExecutorConfig {
        cache_bytes: 1024 * 1024,
        ..uncached_config
    };
    let (admission, mut blocker) = executor(cached_config);
    let mut cache_reader = ReadExecutor::new(cached_config, admission.clone()).unwrap();
    blocker
        .resident_test_results
        .push_back(ResidentTestResult::Complete);
    let SubmitOutcome::Ready { records, lease } = blocker
        .submit(10, fixture.member(0), ReadClass::Point)
        .unwrap()
    else {
        panic!("the injected resident read should seed the shard cache")
    };
    fixture.assert_value(0, records.into_iter().next().unwrap());
    drop(lease);
    blocker.begin_sweep();
    blocker
        .resident_test_results
        .push_back(ResidentTestResult::Unsupported);
    assert!(matches!(
        blocker
            .submit(11, fixture.member(1), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    let SubmitOutcome::Ready { records, lease } = cache_reader
        .submit(12, fixture.member(0), ReadClass::Point)
        .unwrap()
    else {
        panic!("a warmed cache hit must bypass full async device capacity")
    };
    fixture.assert_value(0, records.into_iter().next().unwrap());
    drop(lease);
    assert!(cache_reader.operations.is_empty());
    assert_eq!(admission.stats().async_read_submissions, 1);
    assert_eq!(admission.stats().cache_hits, 1);
    drop(finish(&mut blocker, 1));
}

#[test]
fn cache_hit_and_generation_survive_repeated_full_retries_without_refill() {
    let fixture = Fixture::new(3);
    let config = ReadExecutorConfig {
        resident_first: true,
        cache_bytes: 1024 * 1024,
        max_requests: 4,
        max_device_reads: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut blocker) = executor(config);
    let mut reader = ReadExecutor::new(config, admission.clone()).unwrap();

    blocker
        .resident_test_results
        .push_back(ResidentTestResult::Complete);
    let SubmitOutcome::Ready { records, lease } = blocker
        .submit(10, fixture.member(0), ReadClass::Point)
        .unwrap()
    else {
        panic!("injected resident read should populate the cache synchronously")
    };
    fixture.assert_value(0, records[0].clone());
    drop(records);
    drop(lease);
    assert_eq!(admission.stats().cache_entries, 1);

    blocker.begin_sweep();
    blocker
        .resident_test_results
        .push_back(ResidentTestResult::Unsupported);
    assert!(matches!(
        blocker
            .submit(11, fixture.member(2), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));

    reader.begin_sweep();
    reader
        .resident_test_results
        .push_back(ResidentTestResult::Partial(7));
    let plans = [0, 1]
        .into_iter()
        .map(|index| {
            let reference = fixture.references[index];
            (reference, fixture.log.plan_record_read(&reference).unwrap())
        })
        .collect();
    let mut lease = None;
    let SubmitOutcome::Full(plans) = reader
        .submit_planned(12, plans, ReadClass::Point, &mut lease)
        .unwrap()
    else {
        panic!("the occupied device slot should retain the mixed hit/miss preparation")
    };
    let first_full = admission.stats();
    assert_eq!(first_full.resident_read_attempts, 3);
    assert_eq!(first_full.cache_hits, 0);
    assert_eq!(first_full.cache_misses, 2);

    let completion = finish(&mut blocker, 1).remove(0);
    assert_eq!(completion.request_id, 11);
    drop(completion);
    admission.clear_cache();
    assert_eq!(admission.stats().cache_entries, 0);

    blocker.begin_sweep();
    blocker
        .resident_test_results
        .push_back(ResidentTestResult::Unsupported);
    assert!(matches!(
        blocker
            .submit(13, fixture.member(2), ReadClass::ScanNoFill)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    reader.begin_sweep();
    let SubmitOutcome::Full(plans) = reader
        .submit_planned(12, plans, ReadClass::Point, &mut lease)
        .unwrap()
    else {
        panic!("the second blocker should keep this request Full")
    };
    let second_full = admission.stats();
    assert_eq!(second_full.cache_hits, first_full.cache_hits);
    assert_eq!(second_full.cache_misses, first_full.cache_misses + 1);
    assert_eq!(
        second_full.resident_read_attempts,
        first_full.resident_read_attempts + 1
    );

    let completion = finish(&mut blocker, 1).remove(0);
    assert_eq!(completion.request_id, 13);
    drop(completion);
    reader.begin_sweep();
    assert!(matches!(
        reader
            .submit_planned(12, plans, ReadClass::Point, &mut lease)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    assert!(lease.is_none());
    assert_eq!(
        admission.stats().resident_read_attempts,
        second_full.resident_read_attempts
    );

    let mut completion = finish(&mut reader, 1).remove(0);
    assert_eq!(completion.request_id, 12);
    let records = completion.result.as_mut().unwrap();
    fixture.assert_value(0, records.remove(0));
    fixture.assert_value(1, records.remove(0));
    drop(completion);

    let stats = admission.stats();
    assert_eq!(stats.cache_hits, first_full.cache_hits + 1);
    assert_eq!(stats.cache_misses, first_full.cache_misses + 2);
    assert_eq!(
        stats.resident_read_attempts,
        second_full.resident_read_attempts
    );
    assert_eq!(stats.resident_read_fallbacks, 3);
    assert_eq!(stats.async_read_submissions, 3);
    assert_eq!(stats.cache_entries, 0);
    assert_eq!(stats.cache_bypass_generation, 1);
}

#[test]
fn full_retry_retains_partial_prefix_and_completed_resident_member() {
    let fixture = Fixture::new(3);
    let config = ReadExecutorConfig {
        resident_first: true,
        cache_bytes: 0,
        max_requests: 4,
        max_device_reads: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut first) = executor(config);
    let mut second = ReadExecutor::new(config, admission.clone()).unwrap();
    first
        .resident_test_results
        .push_back(ResidentTestResult::Unsupported);
    assert!(matches!(
        first
            .submit(1, fixture.member(2), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));

    second.begin_sweep();
    second
        .resident_test_results
        .push_back(ResidentTestResult::Partial(7));
    second
        .resident_test_results
        .push_back(ResidentTestResult::Complete);
    let plans = (0..2)
        .map(|n| {
            let reference = fixture.references[n];
            (reference, fixture.log.plan_record_read(&reference).unwrap())
        })
        .collect();
    let mut lease = None;
    let SubmitOutcome::Full(plans) = second
        .submit_planned(2, plans, ReadClass::Point, &mut lease)
        .unwrap()
    else {
        panic!("occupied device slot should defer the async suffix")
    };
    assert!(lease.as_ref().unwrap().prepared.is_some());
    assert_eq!(admission.stats().resident_read_attempts, 3);
    assert_eq!(admission.stats().resident_read_successes, 1);
    assert_eq!(admission.stats().resident_read_partial, 0);

    let first_completion = finish(&mut first, 1).remove(0);
    assert_eq!(first_completion.request_id, 1);
    drop(first_completion);
    second.begin_sweep();
    assert!(matches!(
        second
            .submit_planned(2, plans, ReadClass::Point, &mut lease)
            .unwrap(),
        SubmitOutcome::Pending
    ));
    assert!(lease.is_none());
    assert_eq!(second.operations.len(), 1);
    assert_eq!(second.operations.values().next().unwrap().filled, 7);

    let mut completion = finish(&mut second, 1).remove(0);
    assert_eq!(completion.request_id, 2);
    let records = completion.result.as_mut().unwrap();
    assert_eq!(records.len(), 2);
    for n in 0..2 {
        fixture.assert_value(n, records.remove(0));
    }
    drop(completion);
    let stats = admission.stats();
    assert_eq!(stats.resident_read_attempts, 3);
    assert_eq!(stats.resident_read_successes, 1);
    assert_eq!(stats.resident_read_fallbacks, 2);
    assert_eq!(stats.resident_read_partial, 1);
    assert_eq!(stats.async_read_submissions, 2);
    assert_eq!(stats.wal_reads, 3);
    assert_eq!(stats.inflight_requests, 0);
    assert_eq!(stats.inflight_bytes, 0);
}

#[test]
fn dropping_full_preparation_releases_prefix_and_reply_credit() {
    let fixture = Fixture::new(2);
    let config = ReadExecutorConfig {
        resident_first: true,
        cache_bytes: 0,
        max_requests: 3,
        max_device_reads: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut first) = executor(config);
    let mut second = ReadExecutor::new(config, admission.clone()).unwrap();
    first
        .resident_test_results
        .push_back(ResidentTestResult::Unsupported);
    assert!(matches!(
        first
            .submit(1, fixture.member(1), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending
    ));

    second.begin_sweep();
    second
        .resident_test_results
        .push_back(ResidentTestResult::Partial(7));
    let reference = fixture.references[0];
    let plans = vec![(reference, fixture.log.plan_record_read(&reference).unwrap())];
    let mut lease = None;
    assert!(matches!(
        second
            .submit_planned(2, plans, ReadClass::Point, &mut lease)
            .unwrap(),
        SubmitOutcome::Full(_)
    ));
    assert!(lease.as_ref().unwrap().prepared.is_some());
    let second_charge = lease.as_ref().unwrap().charge;
    let charged_before_drop = admission.stats().inflight_bytes;
    drop(lease.take());
    assert_eq!(
        admission.stats().inflight_bytes,
        charged_before_drop - second_charge
    );
    assert!(admission.stats().idle_buffer_bytes > 0);
    assert_eq!(admission.stats().cancellations, 1);
    drop(finish(&mut first, 1));
    assert_eq!(admission.stats().inflight_requests, 0);
}

#[test]
fn warm_resident_read_completes_without_sq_or_uses_one_async_fallback() {
    let fixture = Fixture::new(1);
    let plan = fixture
        .log
        .plan_record_read(&fixture.references[0])
        .unwrap();
    let mut warm = vec![0; plan.member.len];
    plan.member
        .file
        .read_exact_at(&mut warm, plan.member.offset)
        .unwrap();
    let (admission, mut executor) = executor(ReadExecutorConfig {
        resident_first: true,
        ..ReadExecutorConfig::default()
    });
    fixture.assert_value(0, read(&mut executor, &fixture, 0, 1));
    let stats = admission.stats();
    assert_eq!(stats.resident_read_attempts, 1);
    assert_eq!(stats.wal_reads, 1);
    assert_eq!(stats.wal_read_bytes, plan.member.len as u64);
    assert_eq!(stats.completed_reads, 1);
    assert_eq!(stats.errors, 0);
    assert_eq!(
        stats.resident_read_successes + stats.resident_read_fallbacks,
        1
    );
    assert_eq!(stats.async_read_submissions, stats.resident_read_fallbacks);
    assert!(stats.resident_read_unsupported <= stats.resident_read_fallbacks);
}

#[test]
fn unsupported_and_partial_nowait_results_keep_suffix_owners_until_cq() {
    for (result, expected_prefix, unsupported) in [
        (ResidentTestResult::Unsupported, 0, true),
        (ResidentTestResult::Partial(7), 7, false),
    ] {
        let fixture = Fixture::new(1);
        let (admission, mut executor) = executor(ReadExecutorConfig {
            resident_first: true,
            ..ReadExecutorConfig::default()
        });

        executor.begin_sweep();
        executor.resident_test_results.push_back(result);
        assert!(matches!(
            executor
                .submit(1, fixture.member(0), ReadClass::Point)
                .unwrap(),
            SubmitOutcome::Pending
        ));
        let operation = executor.operations.values().next().unwrap();
        assert_eq!(operation.filled, expected_prefix);
        let buffer_ptr = operation.buffer.as_ptr();
        let file = Arc::downgrade(&operation.plan.member.file);
        let log = Arc::downgrade(&operation.plan._log);
        let charged = admission.stats().inflight_bytes;
        assert!(charged > 0);
        assert!(file.upgrade().is_some());
        assert!(log.upgrade().is_some());

        let mut completion = finish(&mut executor, 1).remove(0);
        let encoded = completion.result.as_mut().unwrap().remove(0);
        assert_eq!(encoded.as_ptr(), buffer_ptr.cast());
        fixture.assert_value(0, encoded);
        assert_eq!(admission.stats().inflight_reads, 0);
        assert_eq!(admission.stats().inflight_bytes, charged);
        let stats = admission.stats();
        assert_eq!(stats.resident_read_attempts, 1);
        assert_eq!(stats.resident_read_fallbacks, 1);
        assert_eq!(
            stats.resident_read_unsupported,
            usize::from(unsupported) as u64
        );
        assert_eq!(stats.resident_read_partial, u64::from(!unsupported));
        assert_eq!(stats.async_read_submissions, 1);
        assert_eq!(stats.completed_reads, 1);
        drop(completion);
        assert_eq!(admission.stats().inflight_bytes, 0);
        assert_eq!(admission.stats().inflight_requests, 0);
    }
}

#[test]
fn resident_attempt_budget_defers_larger_request_tail_and_keeps_order() {
    let fixture = Fixture::new(5);
    let (admission, mut executor) = executor(ReadExecutorConfig {
        resident_first: true,
        max_device_reads: 8,
        ..ReadExecutorConfig::default()
    });
    let mut plans = (0..5)
        .map(|n| {
            (
                fixture.references[n],
                fixture
                    .log
                    .plan_record_read(&fixture.references[n])
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let second_plans = plans.split_off(2);
    executor.begin_sweep();
    let first = executor
        .submit_planned(1, plans, ReadClass::Point, &mut None)
        .unwrap();
    let mut records = match first {
        SubmitOutcome::Ready {
            records,
            lease: _lease,
        } => records,
        SubmitOutcome::Pending => finish(&mut executor, 1).remove(0).result.unwrap(),
        SubmitOutcome::Full(_) => panic!("first bounded request unexpectedly full"),
    };
    let second = executor
        .submit_planned(2, second_plans, ReadClass::Point, &mut None)
        .unwrap();
    records.extend(match second {
        SubmitOutcome::Ready {
            records,
            lease: _lease,
        } => records,
        SubmitOutcome::Pending => finish(&mut executor, 1).remove(0).result.unwrap(),
        SubmitOutcome::Full(_) => panic!("second bounded request unexpectedly full"),
    });
    for (n, bytes) in records.into_iter().enumerate() {
        fixture.assert_value(n, bytes);
    }
    let stats = admission.stats();
    assert_eq!(stats.resident_read_attempts, 4);
    assert_eq!(stats.resident_read_deferred, 1);
    assert_eq!(stats.completed_reads, 5);
    assert_eq!(stats.inflight_bytes, 0);
    assert_eq!(stats.inflight_reads, 0);
}

#[test]
fn successful_nowait_reply_keeps_credit_until_response_lease_retires() {
    let fixture = Fixture::new(1);
    let (admission, mut executor) = executor(ReadExecutorConfig {
        resident_first: true,
        ..ReadExecutorConfig::default()
    });
    executor.begin_sweep();
    let outcome = executor
        .submit(1, fixture.member(0), ReadClass::Point)
        .unwrap();
    let SubmitOutcome::Ready { records, lease } = outcome else {
        // Filesystems that reject NOWAIT still exercise the asynchronous
        // owner path in other executor tests.
        return;
    };
    assert_eq!(admission.stats().inflight_requests, 1);
    assert!(admission.stats().inflight_bytes > 0);
    fixture.assert_value(0, records[0].clone());
    drop(records);
    assert_eq!(admission.stats().inflight_requests, 1);
    drop(lease);
    assert_eq!(admission.stats().inflight_requests, 0);
    assert_eq!(admission.stats().inflight_bytes, 0);
}

#[test]
fn preclaimed_planning_credit_survives_full_and_transfers_only_on_admission() {
    let fixture = Fixture::new(2);
    let config = ReadExecutorConfig {
        max_requests: 2,
        max_device_reads: 1,
        ..ReadExecutorConfig::default()
    };
    let (admission, mut first) = executor(config);
    let mut second = ReadExecutor::new(config, admission.clone()).unwrap();
    assert!(matches!(
        first
            .submit(1, fixture.member(0), ReadClass::Point)
            .unwrap(),
        SubmitOutcome::Pending,
    ));
    let reference = fixture.references[1];
    let plan = fixture.log.plan_record_read(&reference).unwrap();
    let reservation = REQUEST_METADATA_BYTES + MEMBER_METADATA_BYTES + plan.member.len;
    let plans = vec![(reference, plan)];
    let mut lease = admission.reserve_reply(reservation).unwrap();
    assert!(lease.is_some());
    let charged = admission.stats().inflight_bytes;
    let SubmitOutcome::Full(plans) = second
        .submit_planned(2, plans, ReadClass::Point, &mut lease)
        .unwrap()
    else {
        panic!("device credit should still belong to the first read")
    };
    assert!(
        lease.is_some(),
        "Full must not refund the admitted cold-planning owner"
    );
    assert_eq!(admission.stats().inflight_requests, 2);
    assert_eq!(admission.stats().inflight_bytes, charged);
    drop(finish(&mut first, 1));
    assert!(matches!(
        second
            .submit_planned(2, plans, ReadClass::Point, &mut lease)
            .unwrap(),
        SubmitOutcome::Pending,
    ));
    assert!(
        lease.is_none(),
        "the CQ request now owns the same reservation"
    );
    let completed = finish(&mut second, 1);
    assert_eq!(admission.stats().inflight_requests, 1);
    assert_eq!(admission.stats().inflight_bytes, reservation);
    drop(completed);
    assert_eq!(admission.stats().inflight_requests, 0);
    assert_eq!(admission.stats().inflight_bytes, 0);
}

#[test]
fn invalid_preclaimed_plan_charge_keeps_reply_credit_for_the_error_response() {
    let fixture = Fixture::new(1);
    let (admission, mut executor) = executor(ReadExecutorConfig::default());
    let mut lease = admission.reserve_reply(1).unwrap();
    let reference = fixture.references[0];
    let plans = vec![(reference, fixture.log.plan_record_read(&reference).unwrap())];
    assert!(
        executor
            .submit_planned(1, plans, ReadClass::Point, &mut lease)
            .is_err()
    );
    assert!(lease.is_some());
    assert_eq!(admission.stats().inflight_requests, 1);
    assert_eq!(admission.stats().wal_reads, 0);
    drop(lease);
    assert_eq!(admission.stats().inflight_requests, 0);
    assert_eq!(admission.stats().inflight_bytes, 0);
}
