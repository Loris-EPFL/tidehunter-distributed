//! Independent bounded model for M2's complete persistence groups.
//!
//! This is a design model, deliberately independent of `CompleteGroup`, CRC,
//! filesystem calls and production locks. Run before relaxing the storage rule:
//! `rustc --edition 2024 --test .../group_model.rs -o /tmp/group-model`.
//! Tidehunter §§3.1/3.3 and native `wal_replay.rs` require complete atomic batches
//! and recoverable prefixes. Raft Engine's `WriteBarrier` motivates amortizing a
//! barrier across callers, not its distinct on-disk recovery protocol:
//! https://github.com/tikv/raft-engine/blob/master/src/write_barrier.rs
//!
//! Two groups each contain two whole application batches. A group has four
//! abstract independently surviving regions: header, first batch, second batch,
//! completeness evidence. Recovery must validate all four before either member
//! is eligible. Checksums are assumed to detect torn data (not Byzantine faults).

use std::collections::{HashMap, VecDeque};

const COMPLETE: u8 = 15;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct State {
    validated: u8,
    emitted: [u8; 2],
    synced: u8,
    acknowledged: [u8; 2],
    reply_lost: u8,
    failed: bool,
    crashed: bool,
    recovered: Option<[u8; 2]>,
    skip_marker: bool,
    false_abort: bool,
    duplicate_identity: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    None,
    PipelineBeforeBarrier,
    ContinueAfterIoError,
    RecoverInnerBatch,
    StopAtMissingSkip,
    AcknowledgeBeforeBarrier,
    TimeoutMeansAbsent,
    RetryAsAnotherRecord,
}

#[derive(Debug, Default)]
struct Coverage {
    states: usize,
    transitions: usize,
    recovered: usize,
    torn_headers: usize,
    torn_bodies: usize,
    later_group: usize,
    unknown_survived: usize,
    lost_replies: usize,
}

fn invariant(s: State) -> Result<(), &'static str> {
    for group in 0..2 {
        if s.emitted[group] != 0 && s.validated & (1 << group) == 0 {
            return Err("group bytes emitted before all member validation");
        }
        if s.acknowledged[group] != 0 && s.synced & (1 << group) == 0 {
            return Err("durable success without a complete group persistence barrier");
        }
        if let Some(recovered) = s.recovered {
            let expected = if s.emitted[group] == COMPLETE { 3 } else { 0 };
            if recovered[group] != expected {
                return Err("recovery differs from complete surviving group inventory");
            }
            if s.acknowledged[group] & !recovered[group] != 0 {
                return Err("acknowledged member missing after crash");
            }
        }
    }
    if s.false_abort {
        return Err("uncertain I/O or lost reply incorrectly classified as absent");
    }
    if s.duplicate_identity {
        return Err("retry created another authoritative identity occurrence");
    }
    Ok(())
}

fn successors(s: State, new_fragment: bool, fault: Fault) -> Vec<(String, State)> {
    let mut out = Vec::new();
    let mut push = |label: String, next| out.push((label, next));
    if s.crashed {
        if s.recovered.is_none() {
            let mut next = s;
            let mut result = [0; 2];
            for (group, recovered) in result.iter_mut().enumerate() {
                if group == 1 {
                    if !new_fragment && s.emitted[0] != COMPLETE {
                        break;
                    }
                    if new_fragment && !s.skip_marker && fault == Fault::StopAtMissingSkip {
                        break;
                    }
                }
                if s.emitted[group] == COMPLETE {
                    *recovered = 3;
                } else if fault == Fault::RecoverInnerBatch && s.emitted[group] & 2 != 0 {
                    *recovered = 1;
                }
            }
            next.recovered = Some(result);
            push(
                "seal, enumerate registered fragment boundaries, recover".into(),
                next,
            );
        }
        return out;
    }
    for group in 0..2 {
        let bit = 1 << group;
        let can_start = group == 0
            || s.synced & 1 != 0
            || (fault == Fault::PipelineBeforeBarrier && s.emitted[0] == COMPLETE)
            || (fault == Fault::ContinueAfterIoError && s.failed && s.emitted[0] != 0);
        let can_write = !s.failed || fault == Fault::ContinueAfterIoError;
        if can_start && can_write && s.validated & bit == 0 {
            let mut next = s;
            next.validated |= bit;
            push(format!("validate both batches in group {group}"), next);
        }
        if can_write && s.validated & bit != 0 && s.emitted[group] != COMPLETE {
            let mut next = s;
            next.emitted[group] = (s.emitted[group] << 1) | 1;
            push(format!("emit next region of group {group}"), next);
        }
        if can_write && s.emitted[group] == COMPLETE && s.synced & bit == 0 {
            let mut next = s;
            next.synced |= bit;
            push(
                format!("successful group {group} file and directory persistence"),
                next,
            );
        }
        if s.emitted[group] != 0 && !s.failed {
            let mut next = s;
            next.failed = true;
            push(
                format!("uncertain write/sync error for group {group}; stop writer"),
                next,
            );
        }
        for member in 0..2 {
            if s.acknowledged[group] & (1 << member) == 0
                && (s.synced & bit != 0
                    || (fault == Fault::AcknowledgeBeforeBarrier && s.emitted[group] == COMPLETE))
            {
                let mut next = s;
                next.acknowledged[group] |= 1 << member;
                push(format!("acknowledge group {group} member {member}"), next);
            }
        }
        if s.synced & bit != 0 && s.reply_lost & bit == 0 {
            let mut next = s;
            next.reply_lost |= bit;
            push(format!("lose response for persisted group {group}"), next);
        }
    }
    if !s.skip_marker && s.validated & 2 != 0 {
        let mut next = s;
        next.skip_marker = true;
        push("persist inter-fragment skip marker".into(), next);
    }
    if fault == Fault::TimeoutMeansAbsent && (s.reply_lost != 0 || s.failed) {
        let mut next = s;
        next.false_abort = true;
        push(
            "incorrectly return definite abort after unknown outcome".into(),
            next,
        );
    }
    if fault == Fault::RetryAsAnotherRecord && s.reply_lost != 0 {
        let mut next = s;
        next.duplicate_identity = true;
        push("incorrectly append lost-response retry again".into(), next);
    }
    // Power-loss cuts are independent subsets, not just a sequential prefix:
    // mmap copy order does not promise persistence ordering among disk regions.
    for first in 0..=COMPLETE {
        if first & !s.emitted[0] != 0 || (s.synced & 1 != 0 && first != COMPLETE) {
            continue;
        }
        for second in 0..=COMPLETE {
            if second & !s.emitted[1] != 0 || (s.synced & 2 != 0 && second != COMPLETE) {
                continue;
            }
            let mut next = s;
            next.crashed = true;
            next.emitted = [first, second];
            push(
                format!("crash with surviving regions {first:04b}/{second:04b}"),
                next,
            );
        }
    }
    out
}

fn explore(new_fragment: bool, fault: Fault) -> Result<Coverage, Vec<String>> {
    let initial = State::default();
    let mut seen = HashMap::from([(initial, 0usize)]);
    let mut states = vec![(initial, None::<(usize, String)>)];
    let mut queue = VecDeque::from([0]);
    let mut coverage = Coverage::default();
    while let Some(index) = queue.pop_front() {
        let s = states[index].0;
        coverage.states += 1;
        if let Some(recovered) = s.recovered {
            coverage.recovered += 1;
            coverage.torn_headers +=
                usize::from(s.emitted.iter().any(|bits| bits & 1 == 0 && *bits != 0));
            coverage.torn_bodies += usize::from(
                s.emitted
                    .iter()
                    .any(|bits| bits & 1 != 0 && *bits != COMPLETE),
            );
            coverage.later_group += usize::from(recovered[1] == 3);
            coverage.unknown_survived +=
                usize::from(s.failed && recovered.iter().any(|members| *members != 0));
            coverage.lost_replies += usize::from(s.reply_lost != 0);
        }
        for (label, next) in successors(s, new_fragment, fault) {
            coverage.transitions += 1;
            if let Err(error) = invariant(next) {
                let mut trace = vec![format!("REJECTED: {error}"), label];
                let mut cursor = index;
                while let Some((parent, label)) = &states[cursor].1 {
                    trace.push(label.clone());
                    cursor = *parent;
                }
                trace.reverse();
                return Err(trace);
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = seen.entry(next) {
                entry.insert(states.len());
                queue.push_back(states.len());
                states.push((next, Some((index, label))));
            }
        }
    }
    Ok(coverage)
}

#[test]
fn complete_groups_recover_all_authority_under_fault_cuts() {
    for new_fragment in [false, true] {
        let coverage = explore(new_fragment, Fault::None).unwrap();
        assert!(coverage.states > 100);
        assert!(coverage.torn_headers > 0 && coverage.torn_bodies > 0);
        assert!(coverage.later_group > 0 && coverage.unknown_survived > 0);
        assert!(coverage.lost_replies > 0);
        println!("group model new_fragment={new_fragment}: {coverage:?}");
    }
}

#[test]
fn group_model_rejects_unsafe_alternatives() {
    for (fault, new_fragment) in [
        (Fault::PipelineBeforeBarrier, false),
        (Fault::ContinueAfterIoError, false),
        (Fault::RecoverInnerBatch, false),
        (Fault::StopAtMissingSkip, true),
        (Fault::AcknowledgeBeforeBarrier, false),
        (Fault::TimeoutMeansAbsent, false),
        (Fault::RetryAsAnotherRecord, false),
    ] {
        let trace = explore(new_fragment, fault).expect_err("unsafe protocol was not rejected");
        println!("{fault:?}:\n  {}", trace.join("\n  "));
    }
}
