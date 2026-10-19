//! Opt-in resident-set/scan and overlapping read/write workloads. No remote deployment.
use super::{
    Backend, Case, Output, PlannedBatch, Result, Sample, invalid, key, ns, reads, single, value,
};
use std::collections::BTreeMap;
use std::io::Write;
use std::time::Instant;
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
        let start = Instant::now();
        backend.submit_window(ready, case.preparation_chunk_batches)?;
        *operation += window.len() as u64;
        backend.drain()?;
        let elapsed_ns = ns(start);
        if let (Some(before), Some(after)) = (before, backend.authority_write_stats()) {
            timings.push((before, after));
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
    for (index, (before, after)) in timings.into_iter().enumerate() {
        output.write_window(
            case,
            "concurrent_mixed_write_window",
            index,
            write_samples[index].elapsed_ns,
            before,
            after,
        )?;
    }
    output.write_windows.flush()?;
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
