use super::PublicationWork;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

/// Interleaved pushes above the current pop, as dependents arrive while
/// their predecessor publishes, always pop in strictly increasing order.
#[test]
fn publication_work_pops_in_increasing_sequence_order() {
    let mut rng = StdRng::seed_from_u64(62);
    for _ in 0..256 {
        let mut work = PublicationWork::default();
        let mut expected: Vec<u64> = Vec::new();
        let mut next = 0u64;
        for _ in 0..rng.gen_range(1..32) {
            next += rng.gen_range(1..4);
            expected.push(next);
        }
        let (initial, mut later) = expected.split_at(rng.gen_range(1..=expected.len()));
        let mut shuffled = initial.to_vec();
        shuffled.shuffle(&mut rng);
        for &sequence in &shuffled {
            work.push(sequence);
        }
        let mut popped = Vec::with_capacity(expected.len());
        while let Some(sequence) = work.pop() {
            popped.push(sequence);
            let take = rng.gen_range(0..=later.len().min(3));
            let (dependents, rest) = later.split_at(take);
            for &dependent in dependents.iter().rev() {
                work.push(dependent);
            }
            later = rest;
        }
        // The heap drained before every later sequence was pushed.
        for &sequence in later {
            work.push(sequence);
        }
        while let Some(sequence) = work.pop() {
            popped.push(sequence);
        }
        assert_eq!(popped, expected);
    }
}
