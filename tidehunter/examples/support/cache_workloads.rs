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
