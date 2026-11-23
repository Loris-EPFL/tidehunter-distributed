use super::*;

fn entry(sequence: u64) -> InFlightEntry {
    InFlightEntry {
        version: BatchVersion { epoch: 1, sequence },
        seq: sequence,
    }
}

#[test]
fn one_generation_owns_an_operation_until_retirement() {
    let table = InFlight::new(2, RandomState::new());
    let operation = [3; 16];
    table.with_operation(operation, |entries| {
        entries.insert(operation, entry(0));
    });
    assert_eq!(table.lookup(operation), Some(entry(0)));
    // A later generation of the same id is not removed by an older one.
    table.retire(operation, 1);
    assert_eq!(table.lookup(operation), Some(entry(0)));
    table.retire(operation, 0);
    assert_eq!(table.lookup(operation), None);
    assert_eq!(table.len(), 0);
}

#[test]
fn locked_window_freezes_and_updates_every_operation() {
    let table = InFlight::new(2, RandomState::new());
    let operations = [[1; 16], [2; 16]];
    table.with_operations(&operations, |locked| {
        for (index, operation) in operations.iter().enumerate() {
            assert!(locked.get(*operation).is_none());
            locked.insert(*operation, entry(index as u64));
        }
    });
    let seen = table.with_operations(&operations, |locked| {
        operations
            .iter()
            .filter_map(|operation| locked.get(*operation))
            .count()
    });
    assert_eq!(seen, 2);
    assert_eq!(table.len(), 2);
}
