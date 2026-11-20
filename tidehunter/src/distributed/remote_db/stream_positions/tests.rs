//! Stream positions: complete seeding, fail-closed admission and the bound.

use super::*;
use crate::distributed::remote_db::fencing_generations::generation_unused;

fn tail(partition: u32, generation: u64, durable_end: u64, complete: bool) -> OwnerTail {
    OwnerTail {
        partition,
        generation,
        durable_end,
        complete,
    }
}

fn position(partition: u32, generation: u64) -> StreamPosition {
    StreamPosition {
        partition,
        epoch: generation,
        replay_from: 0,
        last_offset: None,
        durable_end: 0,
        complete: true,
    }
}

#[test]
fn seeding_gives_every_owner_and_retained_generation_an_entry() {
    // Generation 2 is served and reported by both owners; generation 3 is an
    // aborted reservation only owner 0 ever fenced to.
    let positions = seed_positions(
        &[0, 1],
        &[(2, false), (3, true)],
        &[
            tail(0, 2, 8 << 20, true),
            tail(1, 2, 4 << 20, false),
            tail(0, 3, 0, true),
        ],
    )
    .expect("seed");
    assert_eq!(positions.len(), 4);
    assert_eq!(positions[&(0, 2)].durable_end, 8 << 20);
    assert!(positions[&(0, 2)].complete);
    // An owner still reporting an incomplete tail seeds an incomplete stream.
    assert_eq!(positions[&(1, 2)].durable_end, 4 << 20);
    assert!(!positions[&(1, 2)].complete);
    // Owner 1 never saw the aborted generation: recorded empty and complete,
    // not an absent entry.
    assert_eq!(positions[&(1, 3)].durable_end, 0);
    assert!(positions[&(1, 3)].complete);
    assert!(
        positions
            .iter()
            .all(|(key, stream)| *key == (stream.partition, stream.epoch))
    );
}

#[test]
fn a_served_generation_an_owner_did_not_report_fails_the_open() {
    let error = seed_positions(&[0, 1], &[(2, false)], &[tail(0, 2, 4096, true)])
        .expect_err("owner 1 gave no proof");
    assert!(
        error
            .to_string()
            .contains("owner 1 did not report retained generation 2"),
        "{error}"
    );
}

#[test]
fn the_seed_never_exceeds_the_stream_bound() {
    let retained: Vec<(u64, bool)> = (1..=(MAX_REGION_STREAMS as u64 + 1))
        .map(|generation| (generation, true))
        .collect();
    let error = seed_positions(&[0], &retained, &[]).expect_err("one stream too many");
    assert!(error.to_string().contains("stream bound"), "{error}");
    let at_bound = &retained[..MAX_REGION_STREAMS - 1];
    assert_eq!(
        seed_positions(&[0], at_bound, &[])
            .expect("at the bound")
            .len(),
        MAX_REGION_STREAMS - 1,
        "the serving generation's stream is the last one the region may hold"
    );
}

#[test]
fn a_seeded_aborted_generation_is_retirable_and_an_unseeded_one_is_not() {
    // The owner's ledger proves an aborted generation empty: trim-eligible.
    let seeded = seed_positions(&[0, 1], &[(3, true)], &[tail(0, 3, 0, true)]).expect("seed");
    assert!(generation_unused(
        &seeded,
        &BTreeMap::new(),
        &[],
        3,
        &[0, 1]
    ));
    // Without the seed the predicate has no proof (fail closed).
    assert!(!generation_unused(
        &BTreeMap::new(),
        &BTreeMap::new(),
        &[],
        3,
        &[0, 1]
    ));
}

#[test]
fn admission_refuses_a_stream_that_is_not_a_retained_fencing_generation() {
    let retained = vec![vec![1, 2], vec![2]];
    let indexes = HashMap::from([(10u32, 0usize), (11, 1)]);
    let positions = BTreeMap::new();
    admit_stream(&positions, &retained, &indexes, 10, 2).expect("retained");
    admit_stream(&positions, &retained, &indexes, 11, 2).expect("retained");
    // Segment generation 9 is nobody's fencing generation: no stale entry.
    let error = admit_stream(&positions, &retained, &indexes, 10, 9).expect_err("unretained");
    let text = error.to_string();
    assert!(
        text.contains("partition 10") && text.contains("segment generation 9"),
        "{text}"
    );
    // Generation 1 is retained on owner 10 only.
    assert!(admit_stream(&positions, &retained, &indexes, 11, 1).is_err());
    // An unknown owner partition.
    assert!(admit_stream(&positions, &retained, &indexes, 12, 2).is_err());
}

#[test]
fn admission_fails_closed_at_the_stream_bound() {
    let generations: Vec<u64> = (1..=(MAX_REGION_STREAMS as u64 + 1)).collect();
    let retained = vec![generations.clone()];
    let indexes = HashMap::from([(0u32, 0usize)]);
    let mut positions = BTreeMap::new();
    for &generation in &generations[..MAX_REGION_STREAMS] {
        positions.insert((0, generation), position(0, generation));
    }
    // An existing stream still publishes; a new one past the bound is refused.
    admit_stream(&positions, &retained, &indexes, 0, 1).expect("existing stream");
    let error = admit_stream(
        &positions,
        &retained,
        &indexes,
        0,
        MAX_REGION_STREAMS as u64 + 1,
    )
    .expect_err("new stream past the bound");
    assert!(error.to_string().contains("stream bound"), "{error}");
}

#[test]
fn recorded_extents_only_grow() {
    let mut positions = BTreeMap::new();
    record_extent(&mut positions, 0, 4, 8192, 4096);
    assert_eq!(positions[&(0, 4)].durable_end, 12288);
    // A lower extent (a relocated copy re-published out of order) never
    // lowers the end or the last offset.
    record_extent(&mut positions, 0, 4, 0, 4096);
    assert_eq!(positions[&(0, 4)].durable_end, 12288);
    assert_eq!(positions[&(0, 4)].last_offset, Some(8192));
    assert!(positions[&(0, 4)].complete);
}
