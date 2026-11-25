#[test]
fn shared_sweep_reserves_exact_coverage_across_clients_and_wraps() {
    let cursor = AtomicU64::new(0);
    let mut clients = (0..3)
        .map(|shard| {
            WorkloadGenerator::new(
                shard,
                17,
                OperationMix {
                    read: 1,
                    get_many: 1,
                    ..OperationMix::default()
                },
                3,
                KeyDistribution::Uniform,
                PutKeyPolicy::Overwrite,
                Arc::new(AtomicU64::new(0)),
            )
        })
        .collect::<Vec<_>>();
    let mut visited = Vec::new();
    for index in 0..31 {
        let item = clients[index % 3].next_shared_sweep_item(&cursor).unwrap();
        visited.extend(item.keys);
    }
    assert_eq!(cursor.load(Ordering::Relaxed), visited.len() as u64);
    for (index, key) in visited.into_iter().enumerate() {
        assert_eq!(key, index as u64 % 17);
    }
}

use super::*;

#[test]
fn new_put_keys_share_one_exact_range_across_threads_and_phases() {
    for distribution in [
        KeyDistribution::Uniform,
        KeyDistribution::Zipf { exponent: 1.2 },
    ] {
        let counter = Arc::new(AtomicU64::new(0));
        let mut keys = Vec::new();
        for _ in 0..2 {
            let barrier = std::sync::Barrier::new(4);
            std::thread::scope(|scope| {
                let mut workers = Vec::new();
                for worker in 0..4 {
                    let mut generator = WorkloadGenerator::new(
                        worker,
                        100,
                        OperationMix {
                            put: u32::from(worker % 2 == 0),
                            put_batch: u32::from(worker % 2 != 0),
                            ..OperationMix::default()
                        },
                        4,
                        distribution.clone(),
                        PutKeyPolicy::New,
                        Arc::clone(&counter),
                    );
                    let barrier = &barrier;
                    workers.push(scope.spawn(move || {
                        barrier.wait();
                        (0..64)
                            .flat_map(|_| generator.next_item().unwrap().keys)
                            .collect::<Vec<_>>()
                    }));
                }
                for worker in workers {
                    keys.extend(worker.join().unwrap());
                }
            });
        }
        keys.sort_unstable();
        assert_eq!(keys, (100..1380).collect::<Vec<_>>());
    }
}

#[test]
fn new_put_key_overflow_is_an_error_and_never_wraps() {
    for batch in [false, true] {
        let mut generator = WorkloadGenerator::new(
            7,
            u64::MAX - 1,
            OperationMix {
                put: u32::from(!batch),
                put_batch: u32::from(batch),
                ..OperationMix::default()
            },
            2,
            KeyDistribution::Uniform,
            PutKeyPolicy::New,
            Arc::new(AtomicU64::new(0)),
        );
        if batch {
            assert_eq!(
                generator.next_item().unwrap().keys,
                [u64::MAX - 1, u64::MAX]
            );
        } else {
            assert_eq!(generator.next_item().unwrap().keys, [u64::MAX - 1]);
            assert_eq!(generator.next_item().unwrap().keys, [u64::MAX]);
        }
        for _ in 0..2 {
            assert_eq!(
                generator.next_item().unwrap_err().to_string(),
                "new put key range overflow"
            );
        }
    }
}

#[test]
fn overwrite_preserves_recorded_seeded_operation_and_key_sequence() {
    // Recorded from the old selection path: rand 0.8 StdRng, seed 7, six equal
    // weights, 100 prepared keys and three members per grouped operation.
    let expected = [
        (OperationKind::Read, vec![30]),
        (OperationKind::MissingRead, vec![127]),
        (OperationKind::PutBatch, vec![17, 25, 12]),
        (OperationKind::GetMany, vec![2, 8, 37]),
        (OperationKind::PutBatch, vec![56, 50, 64]),
        (OperationKind::Delete, vec![78]),
        (OperationKind::Put, vec![92]),
        (OperationKind::Delete, vec![46]),
        (OperationKind::PutBatch, vec![56, 1, 67]),
        (OperationKind::MissingRead, vec![128]),
        (OperationKind::Put, vec![66]),
        (OperationKind::Put, vec![40]),
    ];
    let mut generator = WorkloadGenerator::new(
        7,
        100,
        OperationMix {
            read: 1,
            put: 1,
            delete: 1,
            missing_read: 1,
            get_many: 1,
            put_batch: 1,
        },
        3,
        KeyDistribution::Uniform,
        PutKeyPolicy::Overwrite,
        Arc::new(AtomicU64::new(0)),
    );
    for (operation, keys) in expected {
        let item = generator.next_item().unwrap();
        assert_eq!(item.operation, operation);
        assert_eq!(item.keys, keys);
    }
}

#[test]
fn interference_keys_are_disjoint_and_put_keys_are_new() {
    for distribution in [
        KeyDistribution::Uniform,
        KeyDistribution::Zipf { exponent: 1.2 },
    ] {
        let mut generator = WorkloadGenerator::new(
            7,
            8,
            OperationMix {
                read: 1,
                ..OperationMix::default()
            },
            2,
            distribution,
            PutKeyPolicy::Overwrite,
            Arc::new(AtomicU64::new(0)),
        );
        for index in 0..1000 {
            let read = generator.interference_item(None).unwrap();
            let put = generator.interference_item(Some(index)).unwrap();
            assert_eq!(read.operation, OperationKind::Read);
            assert!(read.keys[0] < 8);
            assert_eq!(put.operation, OperationKind::Put);
            assert_eq!(put.keys, [8 + index]);
            assert_ne!(
                key_bytes(&[0; 16], 7, read.keys[0]),
                key_bytes(&[0; 16], 7, put.keys[0])
            );
        }
        assert!(generator.interference_item(Some(u64::MAX)).is_err());
    }
}

#[test]
fn deterministic_data_is_namespaced_and_uniform_key_width() {
    let ns = [9; 16];
    assert_eq!(key_bytes(&ns, 7, 42), key_bytes(&ns, 7, 42));
    assert_ne!(key_bytes(&ns, 7, 42), key_bytes(&ns, 7, 43));
    assert_ne!(key_bytes(&ns, 7, 42), key_bytes(&[8; 16], 7, 42));
    assert_eq!(key_bytes(&ns, 7, 42).len(), KEY_BYTES);
}

#[test]
fn generated_values_match_entropy_contract() {
    let ns = [1; 16];
    let compressible = value_bytes(&ns, 3, 4, 256, 0.0);
    assert!(compressible[16..].iter().all(|byte| *byte == 0));
    assert_ne!(compressible, value_bytes(&ns, 3, 5, 256, 0.0));
    let a = value_bytes(&ns, 3, 4, 256, 1.0);
    assert_eq!(a, value_bytes(&ns, 3, 4, 256, 1.0));
    assert_ne!(a, value_bytes(&ns, 3, 5, 256, 1.0));
}

#[test]
fn zipf_sampler_is_bounded_and_skewed() {
    let mut rng = StdRng::seed_from_u64(10);
    let mut counts = [0_u64; 8];
    for _ in 0..20_000 {
        counts[usize::try_from(zipf_rejection_inversion(&mut rng, 8, 1.0).unwrap()).unwrap()] += 1;
    }
    assert!(counts.iter().all(|count| *count > 0));
    assert!(counts[0] > counts[7] * 2);
}

#[test]
fn mix_is_reproducible_and_batches_keep_ordered_key_vector() {
    let mix = OperationMix {
        get_many: 1,
        ..OperationMix::default()
    };
    let mut a = WorkloadGenerator::new(
        4,
        100,
        mix.clone(),
        32,
        KeyDistribution::Uniform,
        PutKeyPolicy::Overwrite,
        Arc::new(AtomicU64::new(0)),
    );
    let mut b = WorkloadGenerator::new(
        4,
        100,
        mix,
        32,
        KeyDistribution::Uniform,
        PutKeyPolicy::Overwrite,
        Arc::new(AtomicU64::new(0)),
    );
    let left = a.next_item().unwrap();
    let right = b.next_item().unwrap();
    assert_eq!(left.operation, OperationKind::GetMany);
    assert_eq!(left.keys.len(), 32);
    assert_eq!(left.keys, right.keys);
}

#[test]
fn bounded_measurement_sweep_has_distinct_full_batch_members() {
    let mix = OperationMix {
        get_many: 1,
        ..OperationMix::default()
    };
    let mut generator = WorkloadGenerator::new(
        7,
        8,
        mix,
        2,
        KeyDistribution::Uniform,
        PutKeyPolicy::Overwrite,
        Arc::new(AtomicU64::new(0)),
    );
    let mut cursor = 0;
    let keys: Vec<_> = (0..4)
        .flat_map(|_| generator.next_sweep_item(&mut cursor).unwrap().keys)
        .collect();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());
}

#[test]
fn warmup_sweep_covers_tail_keys_without_changing_measurement_rng() {
    let mix = OperationMix {
        get_many: 1,
        ..OperationMix::default()
    };
    let mut sweep = WorkloadGenerator::new(
        4,
        7,
        mix.clone(),
        3,
        KeyDistribution::Uniform,
        PutKeyPolicy::Overwrite,
        Arc::new(AtomicU64::new(0)),
    );
    let mut random = WorkloadGenerator::new(
        4,
        7,
        mix,
        3,
        KeyDistribution::Uniform,
        PutKeyPolicy::Overwrite,
        Arc::new(AtomicU64::new(0)),
    );
    let mut cursor = 0;
    let mut keys = Vec::new();
    for _ in 0..3 {
        keys.extend(sweep.next_sweep_item(&mut cursor).unwrap().keys);
        random.next_item().unwrap();
    }
    assert_eq!(keys, vec![0, 1, 2, 3, 4, 5, 6, 0, 1]);
    assert_eq!(
        sweep.next_item().unwrap().keys,
        random.next_item().unwrap().keys
    );
}
