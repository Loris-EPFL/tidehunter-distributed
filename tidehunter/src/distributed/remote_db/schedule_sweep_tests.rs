//! Bounded seeded schedule sweep: concurrent writers and readers, a fresh live
//! block, a relocation pass and a periodic reopen, every schedule compared
//! against the sequential model of the same plan.
//!
//! Writers own disjoint key partitions, so the model is exact without a
//! linearizability search: after the join, every acknowledged batch has applied
//! its edits exactly once and no two writers touch one key. Readers run after
//! the writers join and compare their partition to the model. The fresh block
//! rewrites every key after the rounds, so the relocation filter stops at a
//! live value above all earlier data and the pass reclaims a real dead prefix.
//!
//! `TH_GATE_SCHEDULES` sets the schedule count (default 200, maximum 1000);
//! `TH_GATE_SEED` sets the base seed. A failure prints both and the schedule,
//! round, key and arm that differ.
#![cfg(feature = "test_methods")]

use super::native_parity_gates::{
    Bounds, Edit, Model, Phase, apply_phase, assert_same_reads, assert_same_rows, close_arm,
    expected_rows, filtered_shape, fixture, hex, keys, model_edit, native_config, open_hybrid,
    open_native, read_one, scan, sequential_apply, value,
};
use crate::RelocationGate;
use crate::distributed::sui::Db as SuiDb;
use crate::key_shape::KeySpace;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::sync::{Arc, Barrier};

const KS_NAME: &str = "root";
/// Concurrent writers per schedule; the fixture admits four-key batches.
const WRITERS: usize = 4;
const READERS: usize = 4;
const ROUNDS: usize = 3;
/// A reopen every ten schedules bounds the retained index generations while
/// still exercising the reopen path many times within one sweep.
const REOPEN_EVERY: usize = 10;
/// The hybrid pass runs every schedule; the native pass runs every fiftieth.
/// Native's single value WAL file is far larger than the fixture's owner files,
/// so its whole-file target stays at zero and a per-schedule pass would rescan
/// every earlier schedule's bytes (quadratic in the sweep). The native arm is
/// the state oracle here; its relocation decisions have their own gates.
const NATIVE_RELOCATE_EVERY: usize = 50;
const DEFAULT_SCHEDULES: usize = 200;
const MAX_SCHEDULES: usize = 1000;
/// Keys deleted from the tail of the fresh block, so absent keys stay covered.
const FRESH_DELETES: usize = 8;

fn schedule_count() -> usize {
    let Some(raw) = std::env::var_os("TH_GATE_SCHEDULES") else {
        return DEFAULT_SCHEDULES;
    };
    let parsed = raw
        .to_string_lossy()
        .parse::<usize>()
        .unwrap_or_else(|error| panic!("TH_GATE_SCHEDULES must be an integer: {error}"));
    assert!(parsed != 0, "TH_GATE_SCHEDULES must be positive");
    parsed.min(MAX_SCHEDULES)
}

fn base_seed() -> u64 {
    match std::env::var("TH_GATE_SEED") {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|error| panic!("TH_GATE_SEED must be an integer: {error}")),
        Err(_) => 0x5eed_2026_1117,
    }
}

fn owner(key: &[u8]) -> usize {
    usize::from(key[3]) % WRITERS
}

/// One batch plan per writer, seeded by the schedule and round. Writers own
/// `owner(key)`, so plans never collide.
fn write_plans(seed: u64, round: usize) -> Vec<Vec<Edit>> {
    (0..WRITERS)
        .map(|writer| {
            let mut rng = StdRng::seed_from_u64(seed ^ ((round as u64) << 32) ^ writer as u64);
            let mut owned: Vec<Vec<u8>> = keys()
                .into_iter()
                .filter(|key| owner(key) == writer)
                .collect();
            owned.shuffle(&mut rng);
            let size = 1 + rng.gen_range(0..WRITERS);
            owned.truncate(size);
            owned
                .into_iter()
                .enumerate()
                .map(|(member, key)| {
                    if rng.gen_ratio(1, 4) {
                        Edit::Delete(key)
                    } else {
                        let generated = value(seed, &key, ((round as u64) << 8) | member as u64);
                        Edit::Put(key, generated)
                    }
                })
                .collect()
        })
        .collect()
}

/// Rewrite every key at a fresh generation, then delete the tail keys. All
/// earlier data becomes dead, so the next pass reclaims a whole dead prefix.
fn fresh_phase(seed: u64, schedule: usize) -> Phase {
    let all = keys();
    let generation = 1000 + schedule as u64;
    let mut batches: Vec<Vec<Edit>> = all
        .chunks(4)
        .map(|chunk| {
            chunk
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, generation)))
                .collect()
        })
        .collect();
    for chunk in all[all.len() - FRESH_DELETES..].chunks(4) {
        batches.push(chunk.iter().map(|key| Edit::Delete(key.clone())).collect());
    }
    Phase {
        batches,
        ..Phase::default()
    }
}

fn describe_read(value: &Option<Vec<u8>>) -> String {
    match value {
        None => "absent".to_owned(),
        Some(bytes) => format!("{}B {:02x?}", bytes.len(), &bytes[..bytes.len().min(8)]),
    }
}

fn concurrent_writes(db: &Arc<SuiDb>, ks: KeySpace, plans: &[Vec<Edit>], arm: &str) {
    let barrier = Barrier::new(plans.len() + 1);
    std::thread::scope(|scope| {
        for plan in plans {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                let mut writes = db.write_batch();
                for edit in plan {
                    match edit {
                        Edit::Put(key, value) => writes.write(ks, key.clone(), value.clone()),
                        Edit::Delete(key) => writes.delete(ks, key.clone()),
                    }
                }
                writes.commit().unwrap_or_else(|error| {
                    panic!("{arm}: concurrent batch commit failed: {error:?}")
                });
            });
        }
        barrier.wait();
    });
}

fn concurrent_reads(
    db: &Arc<SuiDb>,
    ks: KeySpace,
    model: &Model,
    seed: u64,
    schedule: usize,
    round: usize,
) {
    let domain = keys();
    let barrier = Barrier::new(READERS + 1);
    std::thread::scope(|scope| {
        for reader in 0..READERS {
            let domain = &domain;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for (position, key) in domain.iter().enumerate() {
                    if position % READERS != reader {
                        continue;
                    }
                    let got = read_one(db, ks, key);
                    let want = model.get(key).cloned();
                    assert!(
                        got == want,
                        "schedule {schedule} round {round} reader {reader} seed {seed:#x}: \
                         key {} read {} expected {}",
                        hex(key),
                        describe_read(&got),
                        describe_read(&want)
                    );
                }
            });
        }
        barrier.wait();
    });
}

#[test]
fn concurrent_schedule_sweep_matches_the_sequential_model() {
    let seed = base_seed();
    let schedules = schedule_count();
    eprintln!("schedule sweep: seed={seed:#x} schedules={schedules}");
    let config = native_config();
    let live = Arc::new(Mutex::new(Model::new()));
    let shape = filtered_shape(Arc::clone(&live));
    let fixture = fixture(&config, shape.clone());
    let native_root = tempdir::TempDir::new("native-schedule-sweep").expect("native root");
    let mut native = open_native(native_root.path(), &config, &shape);
    let mut hybrid = open_hybrid(&fixture);
    let ks = native.ks(KS_NAME);
    let domain = keys();

    for schedule in 0..schedules {
        let schedule_seed = seed ^ (schedule as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        for round in 0..ROUNDS {
            let plans = write_plans(schedule_seed, round);
            concurrent_writes(&native, ks, &plans, "native");
            concurrent_writes(&hybrid, ks, &plans, "hybrid");
            {
                let mut model = live.lock();
                for plan in &plans {
                    for edit in plan {
                        model_edit(&mut model, edit);
                    }
                }
            }
            let expected = live.lock().clone();
            assert_same_reads(
                schedule_seed,
                &format!("schedule {schedule} round {round} after writes"),
                ks,
                &native,
                &hybrid,
                &domain,
                &expected,
            );
            concurrent_reads(&native, ks, &expected, schedule_seed, schedule, round);
            concurrent_reads(&hybrid, ks, &expected, schedule_seed, schedule, round);
        }

        let fresh = fresh_phase(schedule_seed, schedule);
        apply_phase(
            &native,
            ks,
            &fresh,
            &format!("schedule {schedule} native fresh"),
        );
        apply_phase(
            &hybrid,
            ks,
            &fresh,
            &format!("schedule {schedule} hybrid fresh"),
        );
        {
            let mut model = live.lock();
            sequential_apply(&mut model, &fresh);
        }

        if (schedule + 1) % NATIVE_RELOCATE_EVERY == 0 || schedule + 1 == schedules {
            apply_phase(
                &native,
                ks,
                &Phase::relocate(),
                &format!("schedule {schedule} native"),
            );
        }
        let hybrid_gate = apply_phase(
            &hybrid,
            ks,
            &Phase::relocate(),
            &format!("schedule {schedule} hybrid"),
        );
        assert!(
            matches!(hybrid_gate, Some(RelocationGate::DidWork)),
            "schedule {schedule} seed={schedule_seed:#x}: the hybrid relocation pass must \
             reclaim the dead prefix behind the fresh block, got {hybrid_gate:?}"
        );

        let expected = live.lock().clone();
        assert_same_reads(
            schedule_seed,
            &format!("schedule {schedule} after relocation"),
            ks,
            &native,
            &hybrid,
            &domain,
            &expected,
        );
        let bounds = Bounds::all();
        assert_same_rows(
            schedule_seed,
            &format!("schedule {schedule} scan"),
            &scan(&native, ks, &bounds),
            &scan(&hybrid, ks, &bounds),
            &expected_rows(&expected, &bounds),
        );

        if (schedule + 1) % REOPEN_EVERY == 0 || schedule + 1 == schedules {
            close_arm(native, "native before reopen");
            close_arm(hybrid, "hybrid before reopen");
            native = open_native(native_root.path(), &config, &shape);
            hybrid = open_hybrid(&fixture);
            assert_same_reads(
                schedule_seed,
                &format!("schedule {schedule} reopen"),
                ks,
                &native,
                &hybrid,
                &domain,
                &expected,
            );
            assert_same_rows(
                schedule_seed,
                &format!("schedule {schedule} reopen scan"),
                &scan(&native, ks, &bounds),
                &scan(&hybrid, ks, &bounds),
                &expected_rows(&expected, &bounds),
            );
        }
    }
    close_arm(native, "native final");
    close_arm(hybrid, "hybrid final");
}
