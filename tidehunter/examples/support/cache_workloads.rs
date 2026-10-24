//! Opt-in resident-set/scan and overlapping read/write workloads. No remote deployment.
use super::{
    Backend, Case, Output, PlannedBatch, Result, Sample, invalid, key, ns, reads, single, value,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::time::Instant;
use tidehunter::distributed::tcp::ResidentReadStats;
use tidehunter::key_shape::KeySpace;

#[allow(clippy::too_many_arguments)] // Explicit benchmark shape and correctness oracle.
pub(super) fn hot_scan(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    hot_keys: usize,
    reads_count: usize,
) -> Result<()> {
    output.read_path_evidence(case, backend, "cache_fill", "before")?;
    let mut samples = Vec::with_capacity(hot_keys);
    let wall = Instant::now();
    for (index, generation) in oracle.iter().take(hot_keys).enumerate() {
        let expected = generation.map(|generation| value(index, generation, case.value_bytes));
        let k = key(index);
        let start = Instant::now();
        let found = backend.get(ks, &k)?;
        let elapsed_ns = ns(start);
        if found != expected {
            return Err(invalid("cache fill value mismatch").into());
        }
        samples.push(Sample {
            elapsed_ns,
            operations: 1,
            bytes: case.value_bytes,
        });
    }
    output.phase(case, "cache_fill", ns(wall), &samples)?;
    output.read_path_evidence(case, backend, "cache_fill", "after")?;
    reads(
        backend,
        case,
        output,
        "cache_hot_before_scan",
        ks,
        &oracle[..hot_keys],
        reads_count,
        false,
        false,
    )?;
    output.read_path_evidence(case, backend, "cache_probe_scan", "before")?;
    let start = Instant::now();
    let found = backend.scan(ks)?;
    let elapsed = ns(start);
    let mut expected: Vec<_> = oracle
        .iter()
        .enumerate()
        .filter_map(|(index, generation)| {
            generation.map(|generation| (key(index), index, generation))
        })
        .collect();
    expected.sort_unstable_by_key(|row| row.0);
    if found.len() != expected.len()
        || found
            .iter()
            .zip(&expected)
            .any(|((k, v), (expected_key, index, generation))| {
                k.as_ref() != expected_key || v != &value(*index, *generation, case.value_bytes)
            })
    {
        return Err(invalid("cache probe scan changed ordered values").into());
    }
    single(
        output,
        case,
        "cache_probe_scan",
        elapsed,
        found.len(),
        found.len() * case.value_bytes,
    )?;
    // Release caller-held results before sampling retained cache/process state.
    drop(found);
    drop(expected);
    output.read_path_evidence(case, backend, "cache_probe_scan", "after")?;
    reads(
        backend,
        case,
        output,
        "cache_hot_after_scan",
        ks,
        &oracle[..hot_keys],
        reads_count,
        false,
        false,
    )
}

type LaneSnapshot = (u32, usize, ResidentReadStats);

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(super) struct RegisteredHotPolicy {
    pub enabled: bool,
    pub hot_keys: usize,
    pub scalar_reads: usize,
    pub group_width: usize,
    pub group_calls: usize,
    pub read_lanes: usize,
    pub max_warm_passes: usize,
    pub required_steady_passes: usize,
    pub scalar_order: &'static str,
    pub group_order: &'static str,
    pub preparation: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounded_hot_reads: Option<usize>,
}

pub(super) fn registered_hot_policy(
    reads: usize,
    bounded_hot_reads: Option<usize>,
    read_lanes: usize,
    enabled: bool,
) -> RegisteredHotPolicy {
    let scalar_reads = bounded_hot_reads.unwrap_or_else(|| reads.clamp(32, 512));
    RegisteredHotPolicy {
        enabled,
        hot_keys: 32,
        scalar_reads,
        group_width: 32,
        group_calls: scalar_reads.div_ceil(32),
        read_lanes,
        max_warm_passes: 2 * read_lanes + 1,
        required_steady_passes: read_lanes,
        scalar_order: "mix64-v1(sample)%32",
        group_order: "unique-prefix-keys-0-through-31",
        preparation: "post-load-before-other-reads;fixed-passes;exact-scalar-then-group-sequence",
        bounded_hot_reads,
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(super) struct BoundedHotPolicy {
    pub enabled: bool,
    hot_keys: usize,
    scalar_reads: usize,
    group_width: usize,
    group_calls: usize,
    warm_passes: usize,
    scalar_order: &'static str,
    group_order: &'static str,
    preparation: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bounded_hot_reads: Option<usize>,
}

impl RegisteredHotPolicy {
    pub(super) fn bounded(self) -> BoundedHotPolicy {
        BoundedHotPolicy {
            enabled: self.enabled,
            hot_keys: self.hot_keys,
            scalar_reads: self.scalar_reads,
            group_width: self.group_width,
            group_calls: self.group_calls,
            warm_passes: self.max_warm_passes,
            scalar_order: self.scalar_order,
            group_order: self.group_order,
            preparation: self.preparation,
            bounded_hot_reads: self.bounded_hot_reads,
        }
    }
}

fn bounded_measurement_counts(policy: RegisteredHotPolicy, grouped: bool) -> (usize, usize) {
    if grouped {
        (policy.group_calls, policy.group_calls * policy.group_width)
    } else {
        (policy.scalar_reads, policy.scalar_reads)
    }
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BoundedObservation<'a> {
    Preparation {
        completed_passes: usize,
        verified_elements: usize,
        setup_wall_ns: u64,
    },
    Measurement {
        measurement_phase: &'a str,
        samples: usize,
        verified_elements: usize,
        population: &'static str,
        physical_io_available: bool,
    },
}

fn bounded_record(
    output: &mut Output,
    case: &Case,
    phase: &str,
    policy: BoundedHotPolicy,
    observation: BoundedObservation<'_>,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Record<'a, 'b> {
        backend: &'a str,
        transport: &'a str,
        value_bytes: usize,
        batch_size: usize,
        window: usize,
        repetition: usize,
        phase: &'a str,
        policy: BoundedHotPolicy,
        scope: &'static str,
        observation: BoundedObservation<'b>,
    }
    serde_json::to_writer(
        &mut output.bounded_controls,
        &Record {
            backend: case.label(),
            transport: case.transport.label(),
            value_bytes: case.value_bytes,
            batch_size: case.batch_size,
            window: case.window,
            repetition: case.repetition,
            phase,
            policy,
            scope: "verified_API_values;RMA_counters_only_in_registered_controls",
            observation,
        },
    )?;
    writeln!(output.bounded_controls)?;
    output.bounded_controls.flush()?;
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
fn bounded_io(backend: &mut Backend) -> Result<Vec<(u32, tidehunter::read_probe::ReadEvidence)>> {
    match backend {
        Backend::Native { db, .. } => Ok(vec![(0, db.read_evidence_for_benchmark(false)?)]),
        Backend::Tcp(db) => Ok(db.read_evidence_for_benchmark(false)?),
        Backend::Sharded(_) => Err(invalid("bounded hot control requires native or TCP").into()),
    }
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
fn bounded_io_record(
    output: &mut Output,
    case: &Case,
    phase: &str,
    before: &[(u32, tidehunter::read_probe::ReadEvidence)],
    after: &[(u32, tidehunter::read_probe::ReadEvidence)],
) -> Result<()> {
    if before.len() != after.len() {
        return Err(invalid("bounded read evidence inventory changed").into());
    }
    for ((owner, first), (last_owner, last)) in before.iter().zip(after) {
        if owner != last_owner {
            return Err(invalid("bounded read evidence owner changed").into());
        }
        let delta = last.delta(*first)?;
        writeln!(
            output.read_io,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            case.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            phase,
            owner,
            last.pid,
            delta.read_bytes,
            delta.minor_faults,
            delta.major_faults,
            last.file_bytes,
            first.resident_bytes,
            last.resident_bytes
        )?;
    }
    output.read_io.flush()?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct RegisteredCounts {
    message_read_requests: u64,
    grant_acquisitions: u64,
    eligible_reads: u64,
    rma_posts: u64,
    rma_completions: u64,
    rma_hits: u64,
    rma_errors: u64,
    fallback_missing: u64,
    fallback_oversize: u64,
    fallback_capacity: u64,
    fallback_unsupported: u64,
    fallback_stale: u64,
}

impl RegisteredCounts {
    fn between(before: &ResidentReadStats, after: &ResidentReadStats) -> Result<Self> {
        macro_rules! delta {
            ($field:ident) => {
                after.$field.checked_sub(before.$field).ok_or_else(|| {
                    invalid(concat!("registered counter reset: ", stringify!($field)))
                })?
            };
        }
        Ok(Self {
            message_read_requests: delta!(message_read_requests),
            grant_acquisitions: delta!(grant_acquisitions),
            eligible_reads: delta!(eligible_reads),
            rma_posts: delta!(rma_posts),
            rma_completions: delta!(rma_completions),
            rma_hits: delta!(rma_hits),
            rma_errors: delta!(rma_errors),
            fallback_missing: delta!(fallback_missing),
            fallback_oversize: delta!(fallback_oversize),
            fallback_capacity: delta!(fallback_capacity),
            fallback_unsupported: delta!(fallback_unsupported),
            fallback_stale: delta!(fallback_stale),
        })
    }

    fn add(&mut self, other: Self) -> Result<()> {
        macro_rules! add {
            ($field:ident) => {
                self.$field = self
                    .$field
                    .checked_add(other.$field)
                    .ok_or_else(|| invalid("registered counter sum overflow"))?;
            };
        }
        add!(message_read_requests);
        add!(grant_acquisitions);
        add!(eligible_reads);
        add!(rma_posts);
        add!(rma_completions);
        add!(rma_hits);
        add!(rma_errors);
        add!(fallback_missing);
        add!(fallback_oversize);
        add!(fallback_capacity);
        add!(fallback_unsupported);
        add!(fallback_stale);
        Ok(())
    }

    fn only_hits(self, elements: usize, groups: Option<usize>) -> bool {
        self.eligible_reads > 0
            && self.eligible_reads == self.rma_hits
            && self.rma_hits <= self.rma_posts
            && groups.is_none_or(|groups| self.eligible_reads == groups as u64)
            && self.rma_posts == elements as u64
            && self.rma_completions == elements as u64
            && self.message_read_requests == 0
            && self.grant_acquisitions == 0
            && self.rma_errors == 0
            && self.fallback_missing == 0
            && self.fallback_oversize == 0
            && self.fallback_capacity == 0
            && self.fallback_unsupported == 0
            && self.fallback_stale == 0
    }
}

#[allow(clippy::type_complexity)] // Explicit bounded owner/lane coverage, not a runtime abstraction.
fn registered_population(
    before: &[LaneSnapshot],
    after: &[LaneSnapshot],
    mut coverage: Option<(&mut BTreeSet<u32>, &mut BTreeSet<(u32, usize)>)>,
) -> Result<RegisteredCounts> {
    if before.len() != after.len() {
        return Err(invalid("registered lane population changed").into());
    }
    let mut total = RegisteredCounts::default();
    for ((owner, lane, first), (last_owner, last_lane, last)) in before.iter().zip(after) {
        if (owner, lane) != (last_owner, last_lane) {
            return Err(invalid("registered owner/lane identity changed").into());
        }
        let delta = RegisteredCounts::between(first, last)?;
        if let Some((owners, lanes)) = coverage.as_mut() {
            if delta.message_read_requests != 0 || delta.rma_posts != 0 {
                owners.insert(*owner);
            }
            if delta.rma_hits != 0 {
                lanes.insert((*owner, *lane));
            }
        }
        total.add(delta)?;
    }
    Ok(total)
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RegisteredObservation<'a> {
    Preparation {
        policy: RegisteredHotPolicy,
        status: &'static str,
        setup_wall_ns: u64,
        steady_passes: usize,
        expected_active_lanes: &'a [(u32, usize)],
        covered_lanes: &'a BTreeSet<(u32, usize)>,
        lane_snapshots: &'a [Vec<LaneSnapshot>],
        pass_populations: &'a [RegisteredCounts],
    },
    Measurement {
        policy: RegisteredHotPolicy,
        population: &'static str,
        measurement_phase: &'a str,
        expected_elements: usize,
        expected_groups: Option<usize>,
        before: &'a [LaneSnapshot],
        after: &'a [LaneSnapshot],
        counters: RegisteredCounts,
    },
}

fn registered_record<'a>(
    output: &mut Output,
    case: &'a Case,
    phase: &'a str,
    observation: RegisteredObservation<'a>,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Record<'r> {
        backend: &'r str,
        transport: &'r str,
        value_bytes: usize,
        batch_size: usize,
        window: usize,
        repetition: usize,
        phase: &'r str,
        scope: &'static str,
        source_occupancy_available: bool,
        observation: RegisteredObservation<'r>,
    }
    serde_json::to_writer(
        &mut output.registered_controls,
        &Record {
            backend: case.label(),
            transport: case.transport.label(),
            value_bytes: case.value_bytes,
            batch_size: case.batch_size,
            window: case.window,
            repetition: case.repetition,
            phase,
            scope: "actual_read_lane_counters;eligible_and_hits_are_groups;posts_and_completions_are_elements",
            observation,
            source_occupancy_available: false,
        },
    )?;
    writeln!(output.registered_controls)?;
    output.registered_controls.flush()?;
    Ok(())
}

pub(super) fn bounded_hot(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    policy: RegisteredHotPolicy,
    registered: bool,
) -> Result<()> {
    if !policy.enabled || oracle.len() < policy.hot_keys {
        return Err(
            invalid("bounded hot control requires an enabled policy and 32 loaded keys").into(),
        );
    }
    let monitor = match backend {
        Backend::Tcp(db) if registered => Some(db.read_evidence_monitor()),
        Backend::Tcp(_) | Backend::Native { .. } => None,
        Backend::Sharded(_) => {
            return Err(invalid("bounded hot control requires native or TCP").into());
        }
    };
    let keys: Vec<_> = (0..policy.hot_keys)
        .map(|index| key(index).to_vec())
        .collect();
    let expected = oracle[..policy.hot_keys]
        .iter()
        .enumerate()
        .map(|(index, generation)| {
            generation
                .map(|generation| value(index, generation, case.value_bytes))
                .ok_or_else(|| invalid("registered hot set contains an absent key"))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let verify_group = |found: &[Option<super::Bytes>]| -> Result<()> {
        if found.len() != expected.len()
            || found
                .iter()
                .zip(&expected)
                .any(|(found, expected)| found.as_ref() != Some(expected))
        {
            return Err(invalid("registered grouped value verification failed").into());
        }
        Ok(())
    };
    output.read_path_evidence(case, backend, "bounded_hot_setup", "before")?;
    let mut snapshots = Vec::with_capacity(if monitor.is_some() {
        policy.max_warm_passes + 1
    } else {
        0
    });
    if let Some(monitor) = &monitor {
        snapshots.push(monitor.sample_resident_read_lanes()?);
    }
    let mut populations = Vec::with_capacity(if monitor.is_some() {
        policy.max_warm_passes
    } else {
        0
    });
    let mut active_owners = BTreeSet::new();
    let mut covered_lanes = BTreeSet::new();
    let mut steady_passes = 0;
    let warm_elements = policy.scalar_reads + policy.group_calls * policy.group_width;
    let started = Instant::now();
    for _ in 0..policy.max_warm_passes {
        for sample in 0..policy.scalar_reads {
            let index = super::mix(sample as u64) as usize % policy.hot_keys;
            if backend.get(ks, &keys[index])?.as_ref() != Some(&expected[index]) {
                return Err(invalid("registered scalar warmup value mismatch").into());
            }
        }
        for _ in 0..policy.group_calls {
            verify_group(&backend.get_many(ks, &keys)?)?;
        }
        if let Some(monitor) = &monitor {
            let after = monitor.sample_resident_read_lanes()?;
            let population = registered_population(
                snapshots.last().expect("initial snapshot"),
                &after,
                Some((&mut active_owners, &mut covered_lanes)),
            )?;
            snapshots.push(after);
            let all_lanes = !active_owners.is_empty()
                && active_owners.iter().all(|owner| {
                    snapshots[0]
                        .iter()
                        .filter(|(candidate, _, _)| candidate == owner)
                        .count()
                        == policy.read_lanes
                        && (0..policy.read_lanes)
                            .all(|lane| covered_lanes.contains(&(*owner, lane)))
                });
            let warm_groups = policy.scalar_reads + policy.group_calls * active_owners.len();
            steady_passes = if all_lanes && population.only_hits(warm_elements, Some(warm_groups)) {
                steady_passes + 1
            } else {
                0
            };
            populations.push(population);
        }
    }
    let setup_wall_ns = ns(started);
    let prepared = monitor.is_some() && steady_passes >= policy.required_steady_passes;
    output.read_path_evidence(case, backend, "bounded_hot_setup", "after")?;
    bounded_record(
        output,
        case,
        "bounded_hot_setup",
        policy.bounded(),
        BoundedObservation::Preparation {
            completed_passes: policy.max_warm_passes,
            verified_elements: policy.max_warm_passes * warm_elements,
            setup_wall_ns,
        },
    )?;
    single(
        output,
        case,
        "bounded_hot_setup",
        setup_wall_ns,
        policy.max_warm_passes * warm_elements,
        policy.max_warm_passes * warm_elements * case.value_bytes,
    )?;
    if monitor.is_some() {
        let expected_lanes: Vec<_> = snapshots[0]
            .iter()
            .filter(|(owner, _, _)| active_owners.contains(owner))
            .map(|(owner, lane, _)| (*owner, *lane))
            .collect();
        registered_record(
            output,
            case,
            "registered_hot_setup",
            RegisteredObservation::Preparation {
                policy,
                status: if prepared {
                    "ready"
                } else {
                    "mixed_or_invalid"
                },
                setup_wall_ns,
                steady_passes,
                expected_active_lanes: &expected_lanes,
                covered_lanes: &covered_lanes,
                lane_snapshots: &snapshots,
                pass_populations: &populations,
            },
        )?;
    }
    for grouped in [false, true] {
        let phase = if grouped {
            "cache_hot_get_many"
        } else {
            "cache_hot_get"
        };
        let before = monitor
            .as_ref()
            .map(|monitor| monitor.sample_resident_read_lanes())
            .transpose()?;
        output.read_path_evidence(case, backend, phase, "before")?;
        #[cfg(all(target_os = "linux", feature = "test-utils"))]
        let io_before = bounded_io(backend)?;
        let (sample_count, elements) = bounded_measurement_counts(policy, grouped);
        let mut samples = Vec::with_capacity(sample_count);
        let wall = Instant::now();
        if grouped {
            for _ in 0..policy.group_calls {
                let start = Instant::now();
                let found = backend.get_many(ks, &keys)?;
                let elapsed_ns = ns(start);
                verify_group(&found)?;
                samples.push(Sample {
                    elapsed_ns,
                    operations: policy.group_width,
                    bytes: policy.group_width * case.value_bytes,
                });
            }
        } else {
            for sample in 0..policy.scalar_reads {
                let index = super::mix(sample as u64) as usize % policy.hot_keys;
                let start = Instant::now();
                let found = backend.get(ks, &keys[index])?;
                let elapsed_ns = ns(start);
                if found.as_ref() != Some(&expected[index]) {
                    return Err(invalid("registered scalar measurement value mismatch").into());
                }
                samples.push(Sample {
                    elapsed_ns,
                    operations: 1,
                    bytes: case.value_bytes,
                });
            }
        }
        let elapsed = ns(wall);
        #[cfg(all(target_os = "linux", feature = "test-utils"))]
        let io_after = bounded_io(backend)?;
        let after = monitor
            .as_ref()
            .map(|monitor| monitor.sample_resident_read_lanes())
            .transpose()?;
        output.read_path_evidence(case, backend, phase, "after")?;
        let counts = match (&before, &after) {
            (Some(before), Some(after)) => Some(registered_population(before, after, None)?),
            _ => None,
        };
        let groups = Some(if grouped {
            policy.group_calls * active_owners.len()
        } else {
            policy.scalar_reads
        });
        let pure = prepared && counts.is_some_and(|counts| counts.only_hits(elements, groups));
        let measured_phase = if monitor.is_none() || pure {
            phase
        } else if grouped {
            "cache_hot_get_many_mixed"
        } else {
            "cache_hot_get_mixed"
        };
        #[cfg(all(target_os = "linux", feature = "test-utils"))]
        bounded_io_record(output, case, measured_phase, &io_before, &io_after)?;
        output.phase(case, measured_phase, elapsed, &samples)?;
        bounded_record(
            output,
            case,
            phase,
            policy.bounded(),
            BoundedObservation::Measurement {
                measurement_phase: measured_phase,
                samples: samples.len(),
                verified_elements: elements,
                population: if pure {
                    "registered_hit_only"
                } else if monitor.is_some() {
                    "mixed_or_invalid"
                } else if matches!(backend, Backend::Native { .. }) {
                    "native_API_hotset_unclassified"
                } else {
                    "remote_API_hotset_unclassified"
                },
                physical_io_available: cfg!(all(target_os = "linux", feature = "test-utils")),
            },
        )?;
        if let (Some(before), Some(after), Some(counts)) = (&before, &after, counts) {
            registered_record(
                output,
                case,
                phase,
                RegisteredObservation::Measurement {
                    policy,
                    population: if pure {
                        "registered_hit_only"
                    } else {
                        "mixed_or_invalid"
                    },
                    measurement_phase: measured_phase,
                    expected_elements: elements,
                    expected_groups: groups,
                    before,
                    after,
                    counters: counts,
                },
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One bounded offered-concurrency workload and its oracle.
pub(super) fn concurrent_mixed(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &mut [Option<u64>],
    operation: &mut u64,
    batches: &[PlannedBatch],
    read_window: usize,
    native_workers: &mut Option<super::native_reads::NativeReadWorkers>,
) -> Result<()> {
    let first_read = batches.len() * case.batch_size;
    if first_read >= oracle.len() {
        return Err(invalid("concurrent mixed needs disjoint stable read keys").into());
    }
    let read_count = match backend {
        Backend::Native { .. } => native_workers
            .as_ref()
            .ok_or_else(|| invalid("concurrent native reads need workers"))?
            .len(),
        Backend::Tcp(_) => read_window,
        Backend::Sharded(_) => {
            return Err(invalid("concurrent mixed needs native or TCP backend").into());
        }
    };
    let windows = batches.len().div_ceil(case.window);
    let counters = backend.counters()?;
    output.read_path_evidence(case, backend, "concurrent_mixed", "before")?;
    let mut read_samples = Vec::with_capacity(windows * read_count);
    let mut write_samples = Vec::with_capacity(windows);
    let mut timings = Vec::with_capacity(if matches!(backend, Backend::Tcp(_)) {
        windows
    } else {
        0
    });
    let mut pending = Vec::with_capacity(read_count);
    let mut started = Vec::with_capacity(read_count);
    let expected: Vec<_> = (0..read_count)
        .map(|slot| {
            let index = first_read + slot % (oracle.len() - first_read);
            (
                key(index),
                oracle[index].map(|generation| value(index, generation, case.value_bytes)),
            )
        })
        .collect();
    let wall = Instant::now();
    for window in batches.chunks(case.window) {
        // Storage reads execute concurrently with the write window, not on keys
        // modified by it. Retirement latency includes the caller's write work;
        // this is not a standalone read service-latency measurement.
        for (slot, (k, _)) in expected.iter().enumerate() {
            started.push(Instant::now());
            match backend {
                Backend::Tcp(db) => pending.push(db.submit_get(ks, k)?),
                Backend::Native { .. } => native_workers.as_mut().unwrap().submit(slot, ks, *k)?,
                Backend::Sharded(_) => unreachable!(),
            }
        }
        let ready = window
            .iter()
            .enumerate()
            .map(|(offset, batch)| (*operation + offset as u64, batch.mutations.clone()))
            .collect();
        let before = backend.authority_write_stats();
        let checkpoint_cursor = backend.checkpoint_cursor(case.diagnostic);
        let start = Instant::now();
        backend.submit_window(ready, case.preparation_chunk_batches)?;
        *operation += window.len() as u64;
        backend.drain()?;
        let elapsed_ns = ns(start);
        if let (Some(before), Some(after)) = (before, backend.authority_write_stats()) {
            timings.push((
                before,
                after,
                backend.checkpoint_snapshot(checkpoint_cursor),
            ));
        }
        write_samples.push(Sample {
            elapsed_ns,
            operations: window.len() * case.batch_size,
            bytes: window.len() * case.batch_size * case.value_bytes,
        });
        match backend {
            Backend::Tcp(db) => {
                for (slot, request) in pending.drain(..).enumerate() {
                    let found = db.finish_get(request)?;
                    let elapsed_ns = ns(started[slot]);
                    if found != expected[slot].1 {
                        return Err(invalid("concurrent remote read value mismatch").into());
                    }
                    read_samples.push(Sample {
                        elapsed_ns,
                        operations: 1,
                        bytes: case.value_bytes,
                    });
                }
            }
            Backend::Native { .. } => {
                for (slot, (_, expected_value)) in expected.iter().enumerate() {
                    let found = native_workers.as_mut().unwrap().finish(slot)?;
                    let elapsed_ns = ns(started[slot]);
                    if &found != expected_value {
                        return Err(invalid("concurrent native read value mismatch").into());
                    }
                    read_samples.push(Sample {
                        elapsed_ns,
                        operations: 1,
                        bytes: case.value_bytes,
                    });
                }
            }
            Backend::Sharded(_) => unreachable!(),
        }
        started.clear();
        for batch in window {
            for &(index, generation) in &batch.changes {
                oracle[index] = generation;
            }
        }
    }
    let elapsed = ns(wall);
    output.phase(case, "concurrent_mixed_get", elapsed, &read_samples)?;
    output.phase(
        case,
        "concurrent_mixed_write_window",
        elapsed,
        &write_samples,
    )?;
    for (index, (before, after, diagnostic)) in timings.into_iter().enumerate() {
        output.write_window(
            case,
            "concurrent_mixed_write_window",
            index,
            write_samples[index].elapsed_ns,
            before,
            after,
        )?;
        output.checkpoint_records(case, "concurrent_mixed_write_window", index, diagnostic)?;
    }
    output.write_windows.flush()?;
    output.checkpoint_diagnostics.flush()?;
    output.stages(case, "concurrent_mixed", counters, backend.counters()?)?;
    output.stages(
        case,
        "concurrent_mixed_configuration",
        BTreeMap::new(),
        BTreeMap::from([
            ("offered_read_window", read_count as u64),
            ("read_key_count", expected.len() as u64),
        ]),
    )?;
    output.read_path_evidence(case, backend, "concurrent_mixed", "after")
}

/// This tail phase uses the same adapter and shared measurement as
/// sharded_wal_adapter_bench, without replacing any raw-engine lifecycle phase.
pub(super) fn hit_only_adapter(
    parameters: &super::BackendParameters,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    operation: u64,
) -> Result<()> {
    use std::sync::Arc;
    use tidehunter::distributed::sui::Db;
    use tidehunter::metrics::Metrics;
    use tidehunter::minibytes::Bytes;

    let db = match parameters.kind {
        super::Kind::NativeAsync | super::Kind::NativeBarrier => {
            let db = Db::open_native(
                &parameters.path,
                parameters.shape.clone(),
                Arc::clone(&parameters.native),
                Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            )
            .map_err(super::native_error)?;
            db.set_native_durable(parameters.kind == super::Kind::NativeBarrier);
            db
        }
        super::Kind::Tcp => Db::open_remote(
            &parameters.path,
            parameters.shape.clone(),
            parameters
                .remote
                .clone()
                .ok_or_else(|| invalid("missing adapter remote config"))?,
        )
        .map_err(super::native_error)?,
        super::Kind::Sharded => {
            return Err(invalid("cache-contention adapter requires native or remote").into());
        }
    };
    let live: Vec<_> = oracle
        .iter()
        .enumerate()
        .filter_map(|(index, generation)| generation.map(|generation| (index, generation)))
        .collect();
    if live.len() < 9 {
        return Err(
            invalid("cache-contention needs eight hot keys and a disjoint writer key").into(),
        );
    }
    let hot: Vec<_> = live[..8]
        .iter()
        .map(|&(index, generation)| {
            (
                Bytes::copy_from_slice(&key(index)),
                value(index, generation, case.value_bytes),
            )
        })
        .collect();
    let writer_index = live.last().expect("at least nine live keys").0;
    let writer = (
        Bytes::copy_from_slice(&key(writer_index)),
        value(writer_index, operation, case.value_bytes),
    );
    for callers in [1, 8] {
        let observation = super::cache_contention::measure(&db, ks, &hot, &writer, callers)
            .map_err(std::io::Error::other)?;
        serde_json::to_writer(
            &mut output.cache_contention,
            &serde_json::json!({
                "backend": case.label(), "transport": case.transport.label(),
                "value_bytes": case.value_bytes, "batch_size": case.batch_size,
                "window": case.window, "repetition": case.repetition,
                "api": "distributed::sui::Db::get", "observation": observation,
            }),
        )?;
        writeln!(output.cache_contention)?;
    }
    output.cache_contention.flush()?;
    // Cover every original key/tombstone again, with the new writer value, not
    // merely the warmed subset. Durable commits above retain native barriers.
    for (index, generation) in oracle.iter().enumerate() {
        let expected = if index == writer_index {
            Some(writer.1.clone())
        } else {
            generation.map(|generation| value(index, generation, case.value_bytes))
        };
        if db.get(ks, &key(index)).map_err(super::native_error)? != expected {
            return Err(invalid("post-contention complete state audit differs").into());
        }
    }
    db.wait_for_background_threads_to_finish()
        .map_err(super::native_error)
}

#[cfg(test)]
mod registered_tests {
    use super::*;

    #[test]
    fn registered_group_hit_counts_one_group_and_32_completed_elements() {
        let hit = RegisteredCounts {
            eligible_reads: 1,
            rma_hits: 1,
            rma_posts: 32,
            rma_completions: 32,
            ..RegisteredCounts::default()
        };
        assert!(hit.only_hits(32, None));
        assert!(!hit.only_hits(32, Some(32)));
        assert!(!RegisteredCounts::default().only_hits(32, None));
        for mixed in [
            RegisteredCounts {
                message_read_requests: 1,
                ..hit
            },
            RegisteredCounts {
                grant_acquisitions: 1,
                ..hit
            },
            RegisteredCounts {
                rma_errors: 1,
                ..hit
            },
            RegisteredCounts {
                fallback_capacity: 1,
                ..hit
            },
            RegisteredCounts {
                rma_completions: 31,
                ..hit
            },
        ] {
            assert!(!mixed.only_hits(32, None));
        }
    }

    #[test]
    fn registered_aggregate_completions_do_not_invent_second_lane_coverage() {
        let before = vec![
            (0, 0, ResidentReadStats::default()),
            (0, 1, ResidentReadStats::default()),
        ];
        let after = vec![
            (
                0,
                0,
                ResidentReadStats {
                    eligible_reads: 1,
                    rma_hits: 1,
                    rma_posts: 32,
                    rma_completions: 32,
                    ..ResidentReadStats::default()
                },
            ),
            (0, 1, ResidentReadStats::default()),
        ];
        let mut owners = BTreeSet::new();
        let mut lanes = BTreeSet::new();
        let population =
            registered_population(&before, &after, Some((&mut owners, &mut lanes))).unwrap();
        assert!(population.only_hits(32, None));
        assert_eq!(owners, BTreeSet::from([0]));
        assert_eq!(lanes, BTreeSet::from([(0, 0)]));
        assert!(!lanes.contains(&(0, 1)));
        assert!(RegisteredCounts::between(&after[0].2, &before[0].2).is_err());
    }

    #[test]
    fn registered_policy_preserves_legacy_read_count_when_unset() {
        let policy = registered_hot_policy(8192, None, 4, true);
        assert_eq!(policy.hot_keys, 32);
        assert_eq!(policy.scalar_reads, 512);
        assert_eq!(policy.group_width, 32);
        assert_eq!(policy.group_calls, 16);
        assert_eq!(policy.max_warm_passes, 9);
        assert_eq!(policy.required_steady_passes, 4);
        assert_eq!(registered_hot_policy(1, None, 2, true).scalar_reads, 32);
        assert!(!registered_hot_policy(8192, None, 4, false).enabled);
        let json = serde_json::to_value(policy).unwrap();
        assert!(json.get("bounded_hot_reads").is_none());
    }

    #[test]
    fn explicit_bounded_policy_sets_scalar_and_complete_group_sample_counts() {
        let policy = registered_hot_policy(512, Some(32_768), 4, true);
        assert_eq!(policy.scalar_reads, 32_768);
        assert_eq!(policy.group_width, 32);
        assert_eq!(policy.group_calls, 1_024);
        assert_eq!(bounded_measurement_counts(policy, false), (32_768, 32_768));
        assert_eq!(bounded_measurement_counts(policy, true), (1_024, 32_768));
        assert_eq!(policy.bounded_hot_reads, Some(32_768));
        let json = serde_json::to_value(policy).unwrap();
        assert_eq!(json["bounded_hot_reads"], 32_768);
        assert_eq!(json["scalar_reads"], 32_768);
        assert_eq!(json["group_calls"], 1_024);
    }
}
