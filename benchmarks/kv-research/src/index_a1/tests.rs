use super::*;
use crate::workload::value_bytes;

#[test]
fn touched_bitmap_round_trips_and_bounds_ids() {
    let bitmap = TouchedBitmap::new(130).unwrap();
    assert_eq!(bitmap.first(), None);
    for id in [129, 3, 64, 3] {
        bitmap.set(id).unwrap();
    }
    assert!(bitmap.set(130).is_err());
    assert_eq!(bitmap.count(), 3);
    assert_eq!(bitmap.first(), Some(3));
    assert!(bitmap.is_set(64).unwrap() && !bitmap.is_set(65).unwrap());
    let bytes = bitmap.to_bytes();
    assert_eq!(bytes.len(), 3 * 8);
    let restored = TouchedBitmap::from_bytes(130, &bytes).unwrap();
    assert_eq!(restored.to_bytes(), bytes);
    assert!(TouchedBitmap::from_bytes(131, &bytes[..16]).is_err());
    let mut outside = bytes;
    outside[16] |= 1 << 2; // ID 130 is outside a 130-key dataset.
    assert!(TouchedBitmap::from_bytes(130, &outside).is_err());
}

#[test]
fn fixture_run_counts_only_recorded_puts_and_selects_its_domain() {
    let run = FixtureRun::new(IndexFixtureStage::Overwrite, 10).unwrap();
    assert_eq!(run.value_domain(), ValueDomain::Timed);
    run.record_put(4).unwrap();
    run.record_put(4).unwrap();
    assert!(run.record_put(10).is_err());
    run.record_uncertain(7).unwrap();
    assert_eq!(run.successful_puts(), 2);
    assert_eq!(run.touched().count(), 1);
    assert!(run.uncertain().is_set(7).unwrap() && !run.touched().is_set(7).unwrap());
    assert_eq!(
        FixtureRun::new(IndexFixtureStage::Build, 10)
            .unwrap()
            .value_domain(),
        ValueDomain::Build
    );
    let pilot =
        FixtureRun::with_domain(IndexFixtureStage::Overwrite, ValueDomain::Pilot, 10).unwrap();
    assert_eq!(pilot.value_domain(), ValueDomain::Pilot);
    assert_eq!(
        IndexFixtureStage::parse("build"),
        Some(IndexFixtureStage::Build)
    );
    assert_eq!(IndexFixtureStage::parse("cold"), None);
}

#[test]
fn domain_values_differ_from_the_build_value_only_in_byte_zero_mark() {
    let namespace = [0xa1; 16];
    let build = value_bytes(&namespace, 7, 42, 1024, 1.0);
    assert_eq!(
        ValueDomain::Build.value(&namespace, 7, 42, 1024, 1.0)[..],
        build[..]
    );
    for (domain, mark) in [(ValueDomain::Pilot, 0x40), (ValueDomain::Timed, 0x80)] {
        let marked = domain.value(&namespace, 7, 42, 1024, 1.0);
        assert_eq!(build.len(), marked.len());
        assert_eq!(build[0] ^ marked[0], mark);
        assert_eq!(build[1..], marked[1..]);
    }
}

#[test]
fn bitmap_merge_unions_bits_of_the_same_dataset_only() {
    let left = TouchedBitmap::new(70).unwrap();
    let right = TouchedBitmap::new(70).unwrap();
    left.set(1).unwrap();
    right.set(69).unwrap();
    left.merge(&right).unwrap();
    assert!(left.is_set(1).unwrap() && left.is_set(69).unwrap());
    assert_eq!(left.count(), 2);
    assert!(left.merge(&TouchedBitmap::new(71).unwrap()).is_err());
}

#[test]
fn build_generator_writes_each_dataset_id_once_then_stops() {
    use crate::config::{KeyDistribution, OperationMix, PutKeyPolicy};
    use crate::workload::WorkloadGenerator;
    let shared = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mix = OperationMix {
        put: 100,
        ..OperationMix::default()
    };
    let mut clients: Vec<_> = (0..2)
        .map(|shard| {
            WorkloadGenerator::new(
                shard,
                5,
                mix.clone(),
                32,
                KeyDistribution::Uniform,
                PutKeyPolicy::New,
                std::sync::Arc::clone(&shared),
            )
            .with_build_ids()
        })
        .collect();
    let mut ids: Vec<u64> = (0..5)
        .map(|step| clients[step % 2].next_item().unwrap().keys[0])
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [0, 1, 2, 3, 4]);
    assert!(clients[0].next_item().is_err());
}
