use super::*;
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShapeBuilder, KeyType};

const SINGLE_SET_BUDGET: usize = WAYS * (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE);

fn protected_policy(keyspace: u8) -> ReadCachePolicy {
    ReadCachePolicy {
        protected_keyspace: Some(keyspace),
        ..ReadCachePolicy::default()
    }
}

/// One logical write: its cache key and the value bytes it published.
///
/// Keeps the shape of the former reference-returning helper so the policy
/// tests below exercise admission, CLOCK and sharding unchanged; only the
/// key changed from a physical reference to a logical identity.
fn record(version: u64, value: &[u8]) -> (CacheKey, Bytes) {
    (
        CacheKey {
            identity: MutationVersion {
                batch: BatchVersion {
                    epoch: 1,
                    sequence: version,
                },
                operation_index: 0,
            },
            keyspace: 0,
        },
        Bytes::copy_from_slice(value),
    )
}

fn record_for_keyspace(version: u64, value: &[u8], keyspace: u8) -> (CacheKey, Bytes) {
    let (mut key, value) = record(version, value);
    key.keyspace = keyspace;
    (key, value)
}

#[test]
fn insertion_bypasses_after_bounded_clock_work() {
    let budget = 1024 * 1024;
    let mut cache = CacheTable::new(budget);
    for version in 0..4096 {
        let (reference, mutation) = record(version, &[9; 1024]);
        assert!(cache.get(&reference).is_none()); // Demand fill, not one-pass writes.
        assert!(cache.insert_verified(reference, &mutation));
        cache.get(&reference).unwrap();
        if cache.stats().entries >= 128 {
            break;
        }
    }
    assert!(cache.stats().entries >= 128);
    let before = cache.stats();
    let payload = budget - before.metadata_bytes - ENTRY_BYTES - 8 - 2 * BYTE_OWNER_BYTES;
    let (reference, mutation) = record(10000, &vec![7; payload]);
    assert!(cache.get(&reference).is_none());
    assert!(!cache.insert_verified(reference, &mutation));
    let after = cache.stats();
    assert_eq!(
        after.bypass_eviction_limit,
        before.bypass_eviction_limit + 1
    );
    assert!(after.evictions - before.evictions <= MAX_EVICTION_STEPS as u64 + 1);
    assert!(after.retained_bytes <= budget);
    assert!(cache.get(&reference).is_none());
}

#[test]
fn large_budget_metadata_is_predictable_without_allocating() {
    let budget = 4usize * 1024 * 1024 * 1024;
    let metadata = ReadCache::metadata_bytes(budget);
    assert!(metadata > ReadCache::metadata_bytes(256 * 1024 * 1024));
    assert!(metadata < budget);
    assert!(ReadCache::metadata_bytes(usize::MAX) < usize::MAX);
}

#[test]
fn disabled_and_too_small_budgets_allocate_nothing() {
    let (reference, mutation) = record(1, b"value");
    for budget in [0, 1, SINGLE_SET_BUDGET - 1] {
        let mut cache = CacheTable::new(budget);
        assert!(cache.slots.is_empty());
        assert!(!cache.insert_verified(reference, &mutation));
        assert!(cache.get(&reference).is_none());
        assert_eq!(cache.stats().retained_bytes, 0);
        assert_eq!(cache.stats().misses, 1);
        let protected_cache = CacheTable::new_with_policy(budget, protected_policy(0));
        assert!(protected_cache.slots.is_empty());
        assert_eq!(protected_cache.stats().retained_bytes, 0);
    }
}

#[test]
fn exact_reference_separates_current_and_checkpoint_versions() {
    let mut cache = CacheTable::new(64 * 1024);
    let (old_ref, old) = record(1, b"old");
    let (new_ref, new) = record(2, b"new");
    assert!(cache.insert_verified(old_ref, &old));
    assert!(cache.get(&new_ref).is_none());
    assert!(cache.insert_verified(new_ref, &new));
    assert_eq!(cache.get(&new_ref), Some(new));
    assert_eq!(cache.get(&old_ref), Some(old));
    // An identity is (batch version, operation index). A different version,
    // a different operation index or a different keyspace is a different
    // entry; there is no physical address or digest to vary any more.
    let (other_operation, _) = record(1, b"other");
    let other_operation = CacheKey {
        identity: MutationVersion {
            operation_index: 1,
            ..other_operation.identity
        },
        ..other_operation
    };
    assert!(cache.get(&other_operation).is_none());
    assert!(
        cache
            .get(&CacheKey {
                keyspace: 1,
                ..old_ref
            })
            .is_none()
    );
}

#[test]
fn lookup_checks_full_identity_after_fingerprint_collision() {
    let mut cache = CacheTable::new(64 * 1024);
    let (reference, mutation) = record(1, b"value");
    assert!(cache.insert_verified(reference, &mutation));
    let changed_generation = CacheKey {
        identity: MutationVersion {
            operation_index: 1,
            ..reference.identity
        },
        ..reference
    };
    let original = cache.slots.iter().position(Option::is_some).unwrap();
    let target = cache.set_start_for_key(&changed_generation);
    cache.slots.swap(original, target);
    cache.tags.swap(original, target);
    // Force a tag collision. The different full identity must still miss.
    cache.tags[target] = cache.fingerprint(&changed_generation);
    assert_eq!(cache.get(&changed_generation), None);
    cache.slots.swap(original, target);
    cache.tags.swap(original, target);
    cache.tags[original] = cache.fingerprint(&reference);
    assert_eq!(
        cache.get(&reference),
        Some(Bytes::copy_from_slice(b"value"))
    );
    assert_eq!(cache.stats().hits, 1);
    assert_eq!(cache.stats().misses, 1);
}

#[test]
fn duplicate_fill_does_not_reallocate_or_change_accounting() {
    let mut cache = CacheTable::new(16 * 1024);
    let (reference, mutation) = record(1, b"value");
    assert!(cache.insert_verified(reference, &mutation));
    let before = cache.stats();
    let stored = cache.get(&reference).unwrap();
    assert!(cache.insert_verified(reference, &mutation));
    let after = cache.stats();
    assert_eq!(before.retained_bytes, after.retained_bytes);
    assert_eq!(after.inserts, 1);
    assert_eq!(after.entries, 1);
    assert_eq!(cache.get(&reference).unwrap().as_ptr(), stored.as_ptr());
}

#[test]
fn every_fill_is_detached_from_large_allocations() {
    let owner = Bytes::from(vec![42; 2 * 1024 * 1024]);
    let (reference, _) = record(1, &[42; 16]);
    let large = owner.slice(8..24);
    let mut cache = CacheTable::new(16 * 1024);
    assert!(cache.insert_verified(reference, &large));
    let value = cache.get(&reference).unwrap();
    assert_ne!(value.as_ptr(), large.as_ptr());
    assert_eq!(value.downcast_ref::<Box<[u8]>>().unwrap().len(), 16);
    assert_eq!(
        cache.stats().retained_bytes - cache.stats().metadata_bytes,
        16 + BYTE_OWNER_BYTES + ENTRY_BYTES
    );
}

#[test]
fn policy_oversized_demand_preserves_resident_entries() {
    let budget = 8 * 1024;
    let cache = ReadCache::new_with_policy(budget, protected_policy(0));
    let resident: Vec<_> = (0..WAYS as u64)
        .map(|version| record_for_keyspace(version, &[7; 512], 1))
        .collect();
    for (reference, mutation) in &resident {
        assert!(cache.insert_verified(*reference, mutation));
    }
    let regular_limit = {
        let table = cache.shards[0].table.lock();
        budget - table.metadata_bytes - table.protected_reserve_bytes
    };
    let (reference, mutation) = record_for_keyspace(100, &vec![5; regular_limit + 1], 1);
    let evictions = cache.stats().evictions;
    for _ in 0..2 {
        let prepared = cache.prepare(reference.identity, reference.keyspace, &mutation);
        assert!(!cache.insert_prepared(prepared));
    }
    assert_eq!(cache.stats().evictions, evictions);
    for (reference, mutation) in resident {
        assert_eq!(
            cache.get(reference.identity, reference.keyspace),
            Some(mutation)
        );
    }
}

#[test]
fn oversized_fills_leave_existing_data_untouched() {
    let mut cache = CacheTable::new(16 * 1024);
    let (reference, mutation) = record(1, b"value");
    assert!(cache.insert_verified(reference, &mutation));
    let (large_ref, large) = record(2, &[7; 16 * 1024]);
    assert!(!cache.insert_verified(large_ref, &large));
    assert_eq!(cache.get(&reference), Some(mutation));
    assert_eq!(cache.stats().entries, 1);
    assert_eq!(cache.stats().rejected_oversize, 1);
}

#[test]
fn sustained_pressure_bounds_payload_plus_all_reserved_metadata() {
    let mut cache = CacheTable::new(16 * 1024);
    let mut held = Vec::new();
    for i in 1..200 {
        let bytes = vec![i as u8; 256 + (i as usize % 16) * 64];
        let (reference, mutation) = record(i, &bytes);
        assert!(cache.get(&reference).is_none());
        let before = cache.stats();
        if cache.insert_verified(reference, &mutation) {
            assert_eq!(cache.get(&reference), Some(mutation));
            if held.len() < 4 {
                held.push(cache.get(&reference).unwrap());
            }
        } else {
            // More slots can require more than the bounded 64 CLOCK steps.
            let after = cache.stats();
            assert_eq!(after.inserts, before.inserts);
            assert_eq!(
                after.bypass_admission + after.bypass_eviction_limit,
                before.bypass_admission + before.bypass_eviction_limit + 1
            );
            assert!(cache.get(&reference).is_none());
        }
        let stats = cache.stats();
        assert!(stats.retained_bytes <= stats.budget_bytes);
        assert_eq!(stats.metadata_bytes, cache.slots.len() * FIXED_SLOT_BYTES);
        assert_eq!(stats.entries, cache.slots.iter().flatten().count());
        assert_eq!(
            stats.retained_bytes,
            stats.metadata_bytes
                + cache
                    .slots
                    .iter()
                    .flatten()
                    .map(|entry| entry.charge())
                    .sum::<usize>()
        );
    }
    assert!(cache.stats().evictions > 0);
    assert_eq!(held.len(), 4); // Application-owned clones survive safe eviction.
}

#[test]
fn empty_value_is_retained_and_absence_is_never_filled() {
    let mut cache = CacheTable::new(16 * 1024);
    let (put_ref, put) = record(1, b"");
    assert!(cache.insert_verified(put_ref, &put));
    assert_eq!(cache.get(&put_ref), Some(put));
    // A deleted member never reaches the cache: the read path returns
    // not-found without an entry, so absence cannot shadow a later write.
    let (deleted_ref, _) = record(2, b"");
    assert!(cache.get(&deleted_ref).is_none());
}

#[test]
fn local_clock_protects_reused_entries_before_single_use_collisions() {
    let mut cache = CacheTable::new(32 * 1024);
    let mut records = Vec::new();
    let target_set = cache.set_start_for_key(&record(1, b"value").0);
    for i in 1..10_000 {
        let pair = record(i, b"value");
        if cache.set_start_for_key(&pair.0) == target_set {
            records.push(pair);
            if records.len() == WAYS + 1 {
                break;
            }
        }
    }
    assert_eq!(records.len(), WAYS + 1);
    for (reference, mutation) in &records[..WAYS] {
        assert!(cache.insert_verified(*reference, mutation));
    }
    assert!(cache.get(&records[0].0).is_some());
    assert!(cache.insert_verified(records[WAYS].0, &records[WAYS].1));
    assert!(cache.get(&records[0].0).is_some());
    assert!(cache.get(&records[1].0).is_none());
    assert_eq!(cache.stats().entries, WAYS);
    assert_eq!(cache.stats().evictions, 1);
}

#[test]
fn write_churn_reuses_probation_without_displacing_read_hot_values() {
    let mut cache = CacheTable::new(SINGLE_SET_BUDGET);
    assert_eq!(cache.slots.len(), WAYS); // One deterministic collision set.
    let records: Vec<_> = (0..100).map(|version| record(version, &[7; 64])).collect();
    for (reference, mutation) in &records[..WAYS] {
        assert!(cache.insert_verified(*reference, mutation));
    }
    let allocations: Vec<_> = cache
        .slots
        .iter()
        .flatten()
        .map(|entry| std::ptr::from_ref(entry.as_ref()))
        .collect();
    for (reference, _) in &records[..WAYS - 1] {
        for _ in 0..3 {
            assert!(cache.get(reference).is_some());
        }
    }
    for (reference, mutation) in &records[WAYS..] {
        assert!(cache.insert_verified(*reference, mutation));
        assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
    }
    for (reference, mutation) in &records[..WAYS - 1] {
        assert_eq!(cache.get(reference), Some(mutation.clone()));
    }
    // New writes still get probationary space and immediate read-after-write.
    let (reference, mutation) = records.last().unwrap();
    assert_eq!(cache.get(reference), Some(mutation.clone()));
    assert_eq!(cache.history.len(), cache.slots.len());
    assert!(
        cache
            .slots
            .iter()
            .flatten()
            .all(|entry| allocations.contains(&std::ptr::from_ref(entry.as_ref())))
    );
}

#[test]
fn repeated_miss_earns_admission_but_one_touch_cannot_replace_a_hot_set() {
    let mut cache = CacheTable::new(SINGLE_SET_BUDGET);
    for version in 0..WAYS as u64 {
        let (reference, mutation) = record(version, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        assert!(cache.get(&reference).is_some());
    }
    let (reference, mutation) = record(100, b"new-value");
    let before = cache.stats();
    assert!(!cache.insert_verified(reference, &mutation));
    assert_eq!(cache.stats().bypass_admission, before.bypass_admission + 1);
    assert_eq!(cache.stats().evictions, before.evictions);
    assert!(cache.get(&reference).is_none());
    assert!(cache.insert_verified(reference, &mutation));
    assert_eq!(cache.get(&reference), Some(mutation));
}

#[test]
fn admission_preflight_skips_detachment_without_double_counting_cold_writes() {
    let budget = SINGLE_SET_BUDGET;
    let cache = ReadCache::new(budget);
    for version in 0..WAYS as u64 {
        let (reference, mutation) = record(version, b"value");
        let prepared = cache.prepare(reference.identity, reference.keyspace, &mutation);
        assert!(cache.insert_prepared(prepared));
    }
    // Passing both preparation and insertion did not promote any cold write.
    assert!(
        cache.shards[0]
            .table
            .lock()
            .slots
            .iter()
            .flatten()
            .all(|entry| entry.benefit == 0)
    );
    for version in 0..WAYS as u64 {
        let (reference, _) = record(version, b"value");
        assert!(cache.get(reference.identity, reference.keyspace).is_some());
    }
    let (reference, mutation) = record(100, b"new-value");
    let rejected = cache.prepare(reference.identity, reference.keyspace, &mutation);
    assert!(matches!(rejected.preparation, InsertPreparation::Admission));
    assert!(!cache.insert_prepared(rejected));
    let accepted = cache.prepare(reference.identity, reference.keyspace, &mutation);
    assert!(matches!(
        accepted.preparation,
        InsertPreparation::Prepared(_)
    ));
    assert!(cache.insert_prepared(accepted));
    assert_eq!(
        cache.get(reference.identity, reference.keyspace),
        Some(mutation)
    );
    assert_eq!(cache.stats().bypass_admission, 1);
}

#[test]
fn per_keyspace_counters_follow_lookup_and_evicted_entry_ownership() {
    let mut cache = CacheTable::new(8 * 1024);
    let (target, target_mutation) = record_for_keyspace(0, b"target", 0);
    let target_set = cache.set_start_for_key(&target);
    assert!(cache.get(&target).is_none());
    assert!(cache.insert_verified(target, &target_mutation));

    let mut inserted = Vec::new();
    for version in 1..10_000 {
        let pair = record_for_keyspace(version, b"value", 1);
        if cache.set_start_for_key(&pair.0) == target_set {
            assert!(cache.get(&pair.0).is_none());
            assert!(cache.insert_verified(pair.0, &pair.1));
            inserted.push(pair);
            if inserted.len() == WAYS - 1 {
                break;
            }
        }
    }
    assert_eq!(inserted.len(), WAYS - 1);
    assert!(cache.get(&inserted[0].0).is_some());

    let mut overflow = None;
    for version in 10_000..20_000 {
        let pair = record_for_keyspace(version, b"overflow", 1);
        if cache.set_start_for_key(&pair.0) == target_set {
            overflow = Some(pair);
            break;
        }
    }
    let (overflow_ref, overflow_mutation) = overflow.expect("collision candidate");
    // RejectFirst: a key seen once may not displace a resident that was
    // seen twice (every resident here was missed once and then inserted).
    // The second miss admits the candidate; the cold target then evicts.
    assert!(cache.get(&overflow_ref).is_none());
    assert!(cache.get(&overflow_ref).is_none());
    assert!(cache.insert_verified(overflow_ref, &overflow_mutation));

    let stats = cache.per_keyspace_stats();
    assert_eq!(
        stats[0],
        PerKeyspaceReadCacheStats {
            misses: 1,
            inserts: 1,
            evictions: 1,
            ..PerKeyspaceReadCacheStats::default()
        }
    );
    assert_eq!(stats[1].hits, 1);
    assert_eq!(stats[1].misses, (WAYS + 1) as u64);
    assert_eq!(stats[1].inserts, WAYS as u64);
    assert_eq!(stats[1].evictions, 0);
    assert_eq!(
        cache.stats().per_keyspace_counters_bytes,
        256 * size_of::<PerKeyspaceReadCacheStats>()
    );
}
#[test]
fn cached_fill_does_not_retain_a_custom_large_byte_owner() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Owner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl minibytes::BytesOwner for Owner {}
    impl Drop for Owner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let owner = Bytes::from_owner(Owner {
        bytes: vec![42; 1024 * 1024],
        dropped: dropped.clone(),
    });
    let (reference, _) = record(1, &[42; 16]);
    let payload = owner.slice(8..24);
    let mut cache = CacheTable::new(16 * 1024);
    assert!(cache.insert_verified(reference, &payload));
    drop(payload);
    drop(owner);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(cache.get(&reference).is_some());
}
#[test]
fn hits_clone_only_the_value_and_a_different_identity_misses() {
    let mut cache = CacheTable::new(16 * 1024);
    let (reference, mutation) = record(1, b"value");
    assert!(cache.insert_verified(reference, &mutation));
    let stored_value_ptr = cache.slots.iter().flatten().next().unwrap().value.as_ptr();
    let hit = cache.get(&reference).unwrap();
    assert_eq!(hit.as_ptr(), stored_value_ptr);
    assert!(
        cache
            .get(&CacheKey {
                keyspace: 1,
                ..reference
            })
            .is_none()
    );
    let (other_reference, _) = record(2, b"value");
    assert!(cache.get(&other_reference).is_none());
    assert_eq!(cache.stats().hits, 1);
    assert_eq!(cache.stats().misses, 2);
}

#[test]
fn regular_demand_can_fill_before_protected_writes() {
    let mut cache = CacheTable::new_with_policy(8 * 1024, protected_policy(0));
    let (reference, mutation) = record_for_keyspace(1, b"contents", 1);
    assert!(cache.insert_verified(reference, &mutation));
    assert_eq!(cache.get(&reference), Some(mutation));
    assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
}

#[test]
fn protected_summary_entries_survive_streaming_warm_and_demand_churn() {
    let (hot_reference, hot_mutation) = record_for_keyspace(0, b"summary", 0);
    let mut cache = CacheTable::new_with_policy(8 * 1024, protected_policy(0));
    let hot_set = cache.set_start_for_key(&hot_reference);
    assert!(cache.insert_verified(hot_reference, &hot_mutation));
    for _ in 0..3 {
        assert!(cache.get(&hot_reference).is_some());
    }

    let mut stream = Vec::new();
    for version in 1..10_000 {
        let pair = record_for_keyspace(version, &[7; 512], 1);
        if cache.set_start_for_key(&pair.0) == hot_set {
            assert!(cache.insert_verified(pair.0, &pair.1));
            stream.push(pair);
            if stream.len() == WAYS - 1 {
                break;
            }
        }
    }
    assert_eq!(stream.len(), WAYS - 1);

    for version in 10_000..20_000 {
        let (reference, mutation) = record_for_keyspace(version, &[9; 512], 1);
        if cache.set_start_for_key(&reference) == hot_set {
            cache.insert_verified(reference, &mutation);
        }
        assert_eq!(cache.get(&hot_reference), Some(hot_mutation.clone()));
        assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
    }
    let payload_budget = cache.stats().budget_bytes - cache.stats().metadata_bytes;
    assert!(cache.regular_retained_bytes <= payload_budget - cache.protected_reserve_bytes);
    assert!(cache.protected_retained_bytes > 0);
}
#[test]
fn sui_shape_skip_set_exempts_only_low_reuse_aliases() {
    let mut builder = KeyShapeBuilder::new();
    for name in [
        "checkpoint_content",
        "certified_checkpoints",
        "checkpoint_by_digest",
        "full_checkpoint_content_v2",
        "checkpoint_sequence_by_contents_digest",
        "watermarks",
        "ordinary_application_data",
    ] {
        builder.add_key_space(name, 1, 1, KeyType::uniform(1));
    }
    let shape = builder.build();
    let budget = 32 * 1024;
    let cache = ReadCache::new_with_policy(budget, ReadCachePolicy::for_sui_shape(&shape));

    for (version, keyspace) in shape.iter_ks().enumerate() {
        let (reference, mutation) =
            record_for_keyspace(version as u64 + 1, b"checkpoint-value", keyspace.id().0);
        let skip_warm = matches!(
            keyspace.name(),
            "checkpoint_by_digest" | "checkpoint_sequence_by_contents_digest" | "watermarks"
        );
        let prepared =
            cache.prepare_post_publish(reference.identity, reference.keyspace, &mutation);
        if skip_warm {
            assert!(!cache.insert_prepared(prepared));
            assert_eq!(cache.get(reference.identity, reference.keyspace), None);
            let demand_fill = cache.prepare(reference.identity, reference.keyspace, &mutation);
            assert!(cache.insert_prepared(demand_fill));
        } else {
            assert!(cache.insert_prepared(prepared));
        }
        assert_eq!(
            cache.get(reference.identity, reference.keyspace),
            Some(mutation)
        );
    }

    // Sui epoch/perpetual shapes lack the protected checkpoint family and
    // therefore keep ordinary read-after-write warming for alias-like names.
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("checkpoint_by_digest", 1, 1, KeyType::uniform(1));
    builder.add_key_space("watermarks", 1, 1, KeyType::uniform(1));
    let non_checkpoint_shape = builder.build();
    let non_checkpoint = ReadCache::new_with_policy(
        budget,
        ReadCachePolicy::for_sui_shape(&non_checkpoint_shape),
    );
    let (reference, mutation) = record_for_keyspace(20, b"contents", 0);
    let warmed =
        non_checkpoint.prepare_post_publish(reference.identity, reference.keyspace, &mutation);
    assert!(non_checkpoint.insert_prepared(warmed));
    assert_eq!(
        non_checkpoint.get(reference.identity, reference.keyspace),
        Some(mutation)
    );

    let generic = ReadCache::new(budget);
    let (reference, mutation) = record_for_keyspace(21, b"digest-alias", 2);
    let warmed = generic.prepare_post_publish(reference.identity, reference.keyspace, &mutation);
    assert!(generic.insert_prepared(warmed));
    assert_eq!(
        generic.get(reference.identity, reference.keyspace),
        Some(mutation)
    );
}

#[test]
fn regular_demand_reclaims_only_protected_capacity_above_the_floor() {
    let mut cache = CacheTable::new_with_policy(16 * 1024, protected_policy(0));
    let target_set = cache.set_start_for_key(&record_for_keyspace(0, b"x", 0).0);
    let mut protected_records = Vec::new();
    for version in 0..10_000 {
        let pair = record_for_keyspace(version, &[3; 2048], 0);
        if cache.set_start_for_key(&pair.0) == target_set {
            protected_records.push(pair);
            if protected_records.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(protected_records.len(), 3);
    for (reference, mutation) in &protected_records {
        assert!(cache.insert_verified(*reference, mutation));
        for _ in 0..3 {
            assert!(cache.get(reference).is_some());
        }
    }
    let protected_before = cache.protected_retained_bytes;
    assert!(protected_before > cache.protected_reserve_bytes);

    let mut reclaimed_borrowed_bytes = false;
    for version in 10_000..20_000 {
        let (reference, mutation) = record_for_keyspace(version, &[8; 2048], 1);
        assert!(cache.get(&reference).is_none());
        cache.insert_verified(reference, &mutation);
        if cache.protected_retained_bytes < protected_before {
            reclaimed_borrowed_bytes = true;
            break;
        }
    }
    assert!(reclaimed_borrowed_bytes);
    assert!(cache.protected_retained_bytes >= cache.protected_reserve_bytes);
    assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
}

/// Runs `work` on another thread and fails the test, instead of hanging,
/// when it does not finish: that means it blocked on a lock the caller holds.
fn finishes_within_deadline<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> R {
    let (done, result) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = done.send(work());
    });
    let value = result
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("operation blocked on a lock held by the test");
    worker.join().unwrap();
    value
}

#[test]
fn disabled_cache_takes_no_lock_and_counts_with_atomics() {
    let cache = Arc::new(ReadCache::new(0));
    assert!(!cache.is_enabled());
    assert_eq!(ReadCache::metadata_bytes(0), 0);
    // Every table lock is held by this thread. A disabled operation that
    // tried to lock any shard would block and fail the deadline.
    let guards: Vec<_> = cache
        .shards
        .iter()
        .map(|shard| shard.table.lock())
        .collect();
    let operations = 64u64;
    let worker_cache = Arc::clone(&cache);
    finishes_within_deadline(move || {
        for version in 0..operations {
            let (reference, mutation) = record(version, b"value");
            assert!(
                worker_cache
                    .get(reference.identity, reference.keyspace)
                    .is_none()
            );
            let prepared = worker_cache.prepare(reference.identity, reference.keyspace, &mutation);
            assert!(matches!(prepared.preparation, InsertPreparation::Disabled));
            assert!(!worker_cache.insert_prepared(prepared));
            let warmed = worker_cache.prepare_post_publish(
                reference.identity,
                reference.keyspace,
                &mutation,
            );
            assert!(!worker_cache.insert_prepared(warmed));
        }
    });
    drop(guards);
    for shard in cache.shards.iter() {
        assert_eq!(
            shard.table.stats().acquisitions,
            1,
            "only the test itself locked the shard"
        );
    }
    let stats = cache.stats();
    assert_eq!(stats.misses, operations);
    assert_eq!(stats.bypass_disabled, 2 * operations);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(
        cache.per_keyspace_stats()[0].misses,
        operations,
        "per-keyspace misses are summed across stripes"
    );
    // Disabled misses spread over several stripes, not one hot counter.
    let loaded = cache
        .shard_stats()
        .shards
        .iter()
        .filter(|shard| shard.misses != 0)
        .count();
    assert!(loaded > 1);
}

#[test]
fn shards_are_independent_and_statistics_sum_across_them() {
    let cache = Arc::new(ReadCache::new_with_shards(
        4 * 1024 * 1024,
        ReadCachePolicy::default(),
        4,
    ));
    assert!(cache.is_enabled());
    let mut by_shard: Vec<Vec<_>> = vec![Vec::new(); 4];
    for version in 0..200 {
        let pair = record(version, b"value");
        by_shard[cache.route(pair.0.identity)].push(pair);
    }
    assert!(by_shard.iter().all(|records| records.len() >= 20));

    // A thread holding shard 0's table mutex does not delay another shard.
    let held = cache.shards[0].table.lock();
    let worker_cache = Arc::clone(&cache);
    let others: Vec<_> = by_shard[1..].iter().flatten().take(30).cloned().collect();
    let expected = others.clone();
    finishes_within_deadline(move || {
        for (key, value) in &others {
            assert!(worker_cache.get(key.identity, key.keyspace).is_none());
            assert!(worker_cache.insert_verified(*key, value));
            assert_eq!(
                worker_cache.get(key.identity, key.keyspace).as_ref(),
                Some(value)
            );
        }
    });
    drop(held);

    for (key, value) in by_shard[0].iter().take(10) {
        assert!(cache.get(key.identity, key.keyspace).is_none());
        assert!(cache.insert_verified(*key, value));
    }
    let load = cache.shard_stats();
    assert_eq!(load.shards.len(), 4);
    assert_eq!(load.shards[0].entries, 10);
    assert_eq!(
        load.shards.iter().map(|shard| shard.entries).sum::<usize>(),
        40
    );
    let stats = cache.stats();
    assert_eq!(stats.entries, 40);
    assert_eq!(stats.inserts, 40);
    assert_eq!(stats.hits, 30);
    assert_eq!(stats.misses, 40);
    assert_eq!(
        stats.per_keyspace_counters_bytes,
        4 * 256 * size_of::<PerKeyspaceReadCacheStats>()
    );
    assert!(stats.retained_bytes <= stats.budget_bytes);
    for (reference, mutation) in expected {
        assert_eq!(
            cache.get(reference.identity, reference.keyspace),
            Some(mutation)
        );
    }
}

#[test]
fn clear_resets_entries_and_counters_but_keeps_the_policy() {
    let cache = ReadCache::new_with_shards(1024 * 1024, protected_policy(0), 2);
    for version in 0..20 {
        let (reference, mutation) = record(version, b"value");
        assert!(cache.insert_verified(reference, &mutation));
    }
    assert!(cache.stats().entries > 0);
    cache.clear();
    let stats = cache.stats();
    assert_eq!((stats.entries, stats.inserts, stats.hits), (0, 0, 0));
    assert_eq!(stats.retained_bytes, stats.metadata_bytes);
    let (key, _) = record(1, b"value");
    assert!(cache.get(key.identity, key.keyspace).is_none());
    let table = cache.shards[0].table.lock();
    assert_eq!(table.policy.protected_keyspace, Some(0));
    assert!(table.protected_reserve_bytes > 0);
}

#[test]
fn shard_count_follows_budget_and_small_budgets_stay_single_table() {
    // The pure routing rule: no allocation of multi-gigabyte tables here.
    assert_eq!(ReadCache::shard_count_for(64 * 1024), 1);
    assert_eq!(ReadCache::shard_count_for(MIN_SHARD_BYTES), 1);
    assert_eq!(ReadCache::shard_count_for(3 * MIN_SHARD_BYTES), 2);
    assert_eq!(ReadCache::shard_count_for(64 * MIN_SHARD_BYTES), MAX_SHARDS);
    assert_eq!(ReadCache::shard_count_for(1usize << 40), MAX_SHARDS);
    assert_eq!(ReadCache::shard_count_for(0), DISABLED_STRIPES);
    assert_eq!(ReadCache::new(64 * 1024).shards.len(), 1);
}

#[test]
fn block_backed_fills_never_pin_their_block_and_budget_bounds_retained_memory() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    // A read block returns to its pool only when every slice drops.
    const BLOCK: usize = 8 * 1024;
    static LIVE_BLOCKS: AtomicUsize = AtomicUsize::new(0);
    struct Block(Vec<u8>);
    impl AsRef<[u8]> for Block {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl minibytes::BytesOwner for Block {}
    impl Drop for Block {
        fn drop(&mut self) {
            LIVE_BLOCKS.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let budget = 64 * 1024;
    let cache = ReadCache::new(budget);
    for version in 0..2000u64 {
        LIVE_BLOCKS.fetch_add(1, Ordering::SeqCst);
        let block = Bytes::from_owner(Block(vec![9; BLOCK]));
        let (reference, _) = record(version, &[9; 1024]);
        let payload = block.slice(8..8 + 1024);
        cache.insert_verified(reference, &payload);
        drop(payload);
        drop(block);
        // No cached entry keeps the block alive.
        assert_eq!(LIVE_BLOCKS.load(Ordering::SeqCst), 0);
        let stats = cache.stats();
        assert!(stats.retained_bytes <= stats.budget_bytes);
    }
    let stats = cache.stats();
    assert!(stats.entries > 0);
    // Retained payload is exactly the charged exact-size copies, far below
    // `entries * BLOCK`.
    assert!(stats.retained_bytes - stats.metadata_bytes < stats.entries * 2 * 1024);
}
