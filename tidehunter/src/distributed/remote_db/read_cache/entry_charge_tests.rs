use super::*;

/// The 8 % budget rule: fixed slot metadata is held outside the byte budget
/// and may not exceed 8 % of metadata plus charged bytes, even for the
/// smallest record (`remote_db/tests.rs::explicit_cache_host_slice_*` asserts
/// `read_cache_metadata_bytes() * 100 <= read_cache_bytes * 8`). The rule is
/// also a `const _` assertion; here it is restated on the compiled layout, so
/// a changed field fails with a message instead of only at const evaluation.
#[test]
fn the_charge_floor_keeps_fixed_slot_metadata_within_the_8_percent_budget() {
    let fixed = size_of::<Option<Box<Entry>>>() + size_of::<u64>() + size_of::<AccessHistory>();
    assert_eq!(fixed, FIXED_SLOT_BYTES);
    let entry_bytes = size_of::<Entry>() + ENTRY_ALLOCATOR_BYTES;
    let computed = entry_bytes + 32 + 64 + 2 * (size_of::<Box<[u8]>>() + 2 * size_of::<usize>());
    assert_eq!(computed, COMPUTED_MIN_ENTRY_CHARGE);
    let budget_floor = (fixed * 92).div_ceil(8);
    assert_eq!(budget_floor, BUDGET_MIN_ENTRY_CHARGE);
    let floor = computed.max(budget_floor);
    assert_eq!(floor, MIN_ENTRY_CHARGE);
    // metadata / (metadata + data) <= 8 %, in integers.
    assert!(fixed * 100 <= (fixed + floor) * 8);
}

/// The floor changes the cache capacity of the replay shape (a 32-byte key,
/// a 512-byte value) by nothing, whether or not it binds numerically. The
/// slot count is `budget / (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE)`; the byte
/// ceiling admits `budget / charge` entries with `charge = payload +
/// ENTRY_BYTES`. While one slot costs no more than one replay-shape entry,
/// the byte ceiling is reached first and the floor lowers no hit rate for
/// that shape. Only records smaller than the floor (below about one
/// 64-byte value) hold fewer slots per byte of budget, which is the stated
/// cost of the 8 % rule.
#[test]
fn the_charge_floor_leaves_the_replay_shape_byte_limited() {
    let replay_shape_charge = 32 + 512 + 2 * BYTE_OWNER_BYTES + ENTRY_BYTES;
    assert!(
        FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE <= replay_shape_charge,
        "a slot ({}) costs more than a replay-shape entry ({replay_shape_charge}): the \
         floor now limits its capacity; measure the hit rate",
        FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE
    );
}

/// Literal pins of the sizes the charge model is computed from. The model
/// (`FIXED_SLOT_BYTES`, `BYTE_OWNER_BYTES`, the 8 % rule) is derived from
/// `size_of`, so a changed field silently changes capacity and the memory
/// the budget promises; a literal here turns that into a visible test edit.
#[test]
fn the_charge_model_sizes_are_pinned_to_literals() {
    assert_eq!(size_of::<Option<Box<Entry>>>(), 8, "one slot handle");
    assert_eq!(size_of::<AccessHistory>(), 16, "fingerprint plus count");
    assert_eq!(FIXED_SLOT_BYTES, 8 + 8 + 16, "handle + tag + history");
    assert_eq!(size_of::<Box<[u8]>>(), 16);
    assert_eq!(BYTE_OWNER_BYTES, 16 + 2 * 8, "box header + two counts");
    assert_eq!(WAYS, 4);
    #[cfg(target_pointer_width = "64")]
    {
        assert_eq!(size_of::<Entry>(), 72, "entry accounting layout");
        assert_eq!(MIN_ENTRY_CHARGE, 368, "small-record charge floor");
    }
}

/// A small-value working set that fits the slot ceiling with room to spare
/// (a quarter of the slots: with 4 ways a set overflows only at the tail of
/// the load distribution) must hit nearly always after one demand fill.
/// This is the shape the 8 % floor penalises, so it pins the stated cost of
/// the rule: the floor lowers the slot count, but a working set of a quarter
/// of what the budget advertises is still served.
#[test]
fn a_small_value_working_set_at_a_quarter_of_the_slots_hits_after_one_fill() {
    let key = |version: u64| CacheKey {
        identity: MutationVersion {
            batch: crate::distributed::types::BatchVersion {
                epoch: 1,
                sequence: version,
            },
            operation_index: 0,
        },
        keyspace: 0,
    };
    let budget = 1024 * 1024;
    let slots = CacheTable::slot_count(budget);
    let working_set = (slots / 4) as u64;
    assert!(working_set >= 256, "the check needs a meaningful set");
    let value = Bytes::copy_from_slice(&[7u8; 16]);
    let mut cache = CacheTable::new(budget);
    for version in 0..working_set {
        assert!(cache.get(&key(version)).is_none());
        assert!(cache.insert_verified(key(version), &value), "{version}");
    }
    let hits = (0..working_set)
        .filter(|&version| cache.get(&key(version)).is_some())
        .count() as u64;
    assert!(
        hits * 100 >= working_set * 95,
        "small-value hit rate {hits}/{working_set} ({slots} slots) is below 95 %"
    );
}
