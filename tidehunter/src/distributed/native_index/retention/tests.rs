use super::*;
use crate::key_shape::KeyShapeBuilder;

#[test]
fn duplicate_pin_registrations_share_cut_entry_but_clones_share_one_quota_slot() {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("pins", 4, 1, KeyType::uniform(1));
    let index = NativeIndex::new(
        &builder.build(),
        [7; 16],
        2 * std::mem::size_of::<ReadPin>(),
    )
    .unwrap();
    let first = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
    let clone = first.clone();
    let duplicate = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
    assert_eq!(index.retention.registrations(), 2);
    let pins = index.retention.pin_counts();
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[&None], 2);
    assert!(matches!(
        index.pin_cut(IndexReadCut::BeforeAll),
        Err(NativeIndexError::Capacity)
    ));
    drop(first);
    assert_eq!(
        index.retention.registrations(),
        2,
        "a clone still owns the first registration"
    );
    assert!(matches!(
        index.pin_cut(IndexReadCut::BeforeAll),
        Err(NativeIndexError::Capacity)
    ));
    drop(clone);
    assert_eq!(index.retention.registrations(), 1);
    let replacement = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
    assert_eq!(
        index.retention.registrations(),
        2,
        "the released quota slot is reusable"
    );
    drop(replacement);
    drop(duplicate);
    assert_eq!(index.retention.registrations(), 0);
    assert!(index.retention.pin_counts().is_empty());
}

#[test]
fn pins_from_many_threads_spread_over_shards_and_release_everywhere() {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("pins", 4, 1, KeyType::uniform(1));
    let index = Arc::new(NativeIndex::new(&builder.build(), [7; 16], 1 << 20).unwrap());
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let index = Arc::clone(&index);
            std::thread::spawn(move || {
                let pins: Vec<_> = (0..50)
                    .map(|_| index.pin_cut(IndexReadCut::BeforeAll).unwrap())
                    .collect();
                pins.len()
            })
        })
        .collect();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), 50);
    }
    assert_eq!(index.retention.registrations(), 0);
    assert!(index.retention.pin_counts().is_empty());
}
