use super::*;

#[test]
fn busy_events_drop_oldest() {
    let mut ring = BusyEvents::default();
    for file in 0..66 {
        ring.record(BusyEvent {
            epoch: 7,
            file,
            cause: BusyCause::RecordPin,
        });
    }
    assert_eq!(ring.events.len(), 64);
    assert_eq!(ring.dropped, 2);
    assert_eq!(ring.events.front().unwrap().file, 2);
    assert_eq!(ring.events.back().unwrap().file, 65);
}
