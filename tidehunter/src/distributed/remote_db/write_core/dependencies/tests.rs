use super::*;
use crate::key_shape::KeySpace;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

#[test]
fn dependency_registry_orders_same_key() {
    let registry = DependencyRegistry::new(1, RandomState::new());
    let next = AtomicU64::new(1);
    let keys = [
        (KeySpace(0), super::super::DependencyKey::from_slice(b"a")),
        (KeySpace(0), super::super::DependencyKey::from_slice(b"b")),
    ];
    let mut registrations = thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..1000)
                        .map(|_| {
                            registry.with_shards(registry.shard_indices(keys.iter()), |locked| {
                                // Key locks order this allocation and the
                                // registrations; AcqRel relays ring reuse.
                                let sequence = next.fetch_add(1, Ordering::AcqRel);
                                let mut predecessors = Vec::new();
                                locked.register(&keys, sequence, &mut predecessors, |_| false);
                                (sequence, predecessors)
                            })
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    registrations.sort_unstable_by_key(|entry| entry.0);
    for (offset, (sequence, predecessors)) in registrations.iter().enumerate() {
        assert_eq!(*sequence, offset as u64 + 1);
        if offset == 0 {
            assert!(predecessors.is_empty());
        } else {
            assert_eq!(predecessors, &[*sequence - 1]);
        }
    }
    registry.remove(&keys, 7999);
    registry.with_shards(registry.shard_indices(keys.iter()), |locked| {
        let mut predecessors = Vec::new();
        locked.register(&keys, 8001, &mut predecessors, |_| false);
        assert_eq!(predecessors, [8000]);
    });
    registry.remove(&keys, 8001);
    registry.with_shards(registry.shard_indices(keys.iter()), |locked| {
        let mut predecessors = Vec::new();
        locked.register(&keys, 8002, &mut predecessors, |_| false);
        assert!(predecessors.is_empty());
    });
}
