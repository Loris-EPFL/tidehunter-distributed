use super::*;
use std::collections::BTreeSet;

#[test]
fn concurrent_callers_share_one_logical_order() {
    let clock = LogicalClock::new_fenced_epoch(7);
    let versions = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let clock = &clock;
                scope.spawn(move || {
                    (0..1024)
                        .map(|_| clock.allocate().unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect::<BTreeSet<_>>()
    });
    assert_eq!(versions.len(), 4096);
    assert_eq!(
        versions.first(),
        Some(&BatchVersion {
            epoch: 7,
            sequence: 0
        })
    );
    assert_eq!(
        versions.last(),
        Some(&BatchVersion {
            epoch: 7,
            sequence: 4095
        })
    );
}

#[test]
fn exhausted_epoch_never_reuses_a_position() {
    let clock = LogicalClock {
        epoch: 9,
        next: AtomicU64::new(u64::MAX - 1),
    };
    assert_eq!(
        clock.allocate(),
        Some(BatchVersion {
            epoch: 9,
            sequence: u64::MAX - 1
        })
    );
    assert_eq!(clock.allocate(), None);
    assert_eq!(clock.allocate(), None);
}
