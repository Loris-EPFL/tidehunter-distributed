//! Loading the adopted callback starts from the authority manifest.

use super::{CallbackStart, load, to_manifest};
use std::collections::BTreeMap;

const FILE: u64 = 1 << 20;

fn start(partition: u32, generation: u64, from: u64) -> CallbackStart {
    CallbackStart {
        partition,
        fencing_generation: generation,
        from,
    }
}

#[test]
fn a_stored_list_round_trips_through_the_manifest_form() {
    let starts = BTreeMap::from([((1, 2), FILE), ((1, 3), 2 * FILE), ((7, 3), 0)]);
    let loaded = load(
        &to_manifest(&starts),
        FILE,
        &[1, 7],
        &[vec![2, 3], vec![3]],
        4,
    )
    .unwrap();
    assert_eq!(loaded, starts);
}

#[test]
fn a_start_of_a_trimmed_generation_is_dropped_not_adopted() {
    // Generation 2 of owner 1 was trimmed after its last adoption.
    let entries = [start(1, 2, FILE), start(1, 3, FILE)];
    let loaded = load(&entries, FILE, &[1], &[vec![3]], 4).unwrap();
    assert_eq!(loaded, BTreeMap::from([((1, 3), FILE)]));
}

#[test]
fn a_start_of_an_unknown_owner_is_dropped() {
    let loaded = load(&[start(9, 3, FILE)], FILE, &[1], &[vec![3]], 4).unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn the_serving_generation_may_carry_a_start() {
    let loaded = load(&[start(1, 4, FILE)], FILE, &[1], &[vec![3]], 4).unwrap();
    assert_eq!(loaded, BTreeMap::from([((1, 4), FILE)]));
}

#[test]
fn an_unaligned_start_is_refused() {
    let error = load(&[start(1, 3, FILE + 1)], FILE, &[1], &[vec![3]], 4).unwrap_err();
    assert!(error.message().contains("file-aligned"), "{error}");
}

#[test]
fn an_unsorted_or_duplicated_list_is_refused() {
    let unsorted = [start(1, 3, FILE), start(1, 2, FILE)];
    assert!(load(&unsorted, FILE, &[1], &[vec![2, 3]], 4).is_err());
    let duplicated = [start(1, 3, FILE), start(1, 3, 2 * FILE)];
    assert!(load(&duplicated, FILE, &[1], &[vec![3]], 4).is_err());
}
