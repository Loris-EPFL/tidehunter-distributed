//! Cold-start read phases share the real Db/RemoteDatabase APIs. Preparation,
//! eviction and evidence RPCs are outside the measured API timers. An advisory
//! cache drop alone is never labelled a verified physical read.
use super::{
    Backend, Case, KeySpace, Output, Result, batched_reads, invalid, key, mix, native_error, ns,
    reads, value, write_exchange_profile, write_node_profiles,
};
use std::io::Write;
use std::time::Instant;
use tidehunter::read_probe::ReadEvidence;

fn evidence(backend: &mut Backend, evict: bool) -> Result<Vec<(u32, ReadEvidence)>> {
    match backend {
        Backend::Native { db, .. } => Ok(vec![(0, db.read_evidence_for_benchmark(evict)?)]),
        Backend::Tcp(db) => Ok(db.read_evidence_for_benchmark(evict)?),
        Backend::Sharded(_) => Err(invalid("cold-read campaign needs native-sync or TCP").into()),
    }
}

/// Each row is sampled in its actual process. This is deliberately separate
/// from WAL residency evidence: opened snapshots must not inspect/evict WALs.
pub(super) fn resources(backend: &mut Backend, output: &mut Output, stage: &str) -> Result<()> {
    let Some(writer) = &mut output.read_resources else {
        return Ok(());
    };
    let (role, storage, native_files) = match backend {
        Backend::Native { db, .. } => (
            "native",
            Vec::new(),
            Some(db.registered_value_wal_files_for_benchmark()),
        ),
        Backend::Tcp(db) => ("frontend", db.process_resources_for_benchmark()?, None),
        Backend::Sharded(_) => return Err(invalid("resource evidence needs native or TCP").into()),
    };
    let local = tidehunter::read_probe::ProcessResources::process()?;
    write!(
        writer,
        "{stage},{role},,{},{},{},{},{}",
        local.pid, local.rss_bytes, local.peak_rss_bytes, local.user_cpu_ns, local.system_cpu_ns
    )?;
    if let Some(count) = native_files {
        writeln!(writer, ",{count}")?;
    } else {
        writeln!(writer, ",")?;
    }
    for (partition, process) in storage {
        writeln!(
            writer,
            "{stage},storage,{partition},{},{},{},{},{},",
            process.pid,
            process.rss_bytes,
            process.peak_rss_bytes,
            process.user_cpu_ns,
            process.system_cpu_ns
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn record(
    output: &mut Output,
    case: &Case,
    phase: &str,
    before: &[(u32, ReadEvidence)],
    after: &[(u32, ReadEvidence)],
) -> Result<()> {
    if before.len() != after.len() {
        return Err(invalid("read evidence inventory changed").into());
    }
    let mut physical = 0u64;
    for ((partition, first), (last_partition, last)) in before.iter().zip(after) {
        if partition != last_partition {
            return Err(invalid("read evidence partition changed").into());
        }
        let delta = last.delta(*first)?;
        physical += delta.read_bytes;
        writeln!(
            output.read_io,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            case.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            phase,
            partition,
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
    // proc read_bytes is charged for page-fault and read(2) storage I/O. The
    // same-process native result and every remote node are collected. This is
    // interval evidence, not per-key attribution or physical-device saturation.
    if physical == 0 {
        return Err(invalid(
            "cold-read phase caused zero accounted physical reads; no cold result is valid",
        )
        .into());
    }
    Ok(())
}

// Keep phase preparation, value checks and evidence collection together so
// the timed interval and its cache/I/O boundaries can be reviewed in order.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(super) fn run(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    read_window: usize,
    native_workers: &mut Option<super::native_reads::NativeReadWorkers>,
    diagnostics: bool,
) -> Result<()> {
    backend.drain()?;
    for (phase, batched, node_phase) in [
        ("cache_evicted_get", false, "cache_evicted_get-diagnostics"),
        (
            "cache_evicted_get_many",
            true,
            "cache_evicted_get_many-diagnostics",
        ),
    ] {
        let before = evidence(backend, true)?;
        if diagnostics && let Backend::Tcp(db) = backend {
            db.connection_profiles(true, true)?;
        }
        if batched {
            batched_reads(backend, case, output, ks, oracle, count, phase)?;
        } else {
            reads(
                backend, case, output, phase, ks, oracle, count, false, false,
            )?;
        }
        if diagnostics && let Backend::Tcp(db) = backend {
            write_node_profiles(
                output,
                case,
                node_phase,
                db.connection_profiles(false, false)?,
            )?;
        }
        let after = evidence(backend, false)?;
        record(output, case, phase, &before, &after)?;
    }
    let before = evidence(backend, true)?;
    if diagnostics && let Backend::Tcp(db) = backend {
        db.connection_profiles(true, true)?;
    }
    super::windowed_reads(
        backend,
        case,
        output,
        ks,
        oracle,
        count,
        "cache_evicted_get_window",
        read_window,
        native_workers,
    )?;
    if diagnostics && let Backend::Tcp(db) = backend {
        write_node_profiles(
            output,
            case,
            "cache_evicted_get_window-diagnostics",
            db.connection_profiles(false, false)?,
        )?;
    }
    let after = evidence(backend, false)?;
    record(output, case, "cache_evicted_get_window", &before, &after)?;
    // Independent diagnostic pass, never substituted for ordinary get latency.
    let before = evidence(backend, true)?;
    let queries: Vec<_> = (0..count.min(256))
        .map(|n| {
            // This matches the other phases' pointer-width truncation on 32-bit hosts.
            #[allow(clippy::cast_possible_truncation)]
            let index = mix(n as u64) as usize % oracle.len();
            (
                index,
                oracle[index].map(|g| value(index, g, case.value_bytes)),
            )
        })
        .collect();
    if let Backend::Tcp(db) = backend {
        db.connection_profiles(true, true)?;
    }
    for (sample, (index, expected)) in queries.into_iter().enumerate() {
        if let Backend::Tcp(db) = backend {
            let start = Instant::now();
            let (found, p) = db.get_profiled(ks, &key(index))?;
            let whole = ns(start);
            if found != expected {
                return Err(invalid("cold profiled get value mismatch").into());
            }
            write_exchange_profile(output, case, "cache-evicted", sample, &p.tcp)?;
            writeln!(
                output.remote_read_profiles,
                "{},{},{},{},{},cache-evicted,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                case.label(),
                case.value_bytes,
                case.batch_size,
                case.window,
                case.repetition,
                sample,
                p.index_ns,
                p.dispatch_ns,
                p.wait_ns,
                p.actor_queue_ns,
                p.actor_service_ns,
                p.actor_return_ns,
                p.tcp.request_encode_ns,
                p.tcp.exchange_ns,
                p.tcp.response_decode_ns,
                p.tcp.record_verify_ns,
                p.tcp.server_handler_ns,
                p.tcp.server_inventory_wait_ns,
                p.tcp.server_authority_lookup_ns,
                p.tcp.server_record_read_ns,
                p.cache_hits,
                p.cache_misses,
                whole,
                p.inline_read_ns,
                p.tcp.server_response_recheck_wait_ns,
                p.tcp.server_response_recheck_ns,
                p.tcp.server_response_recheck_samples,
                p.tcp.server_response_recheck_deferrals
            )?;
        } else if let Backend::Native { db, .. } = backend {
            let start = Instant::now();
            let (found, p) = db
                .get_profiled_for_benchmark(ks, &key(index))
                .map_err(native_error)?;
            let whole = ns(start);
            if found != expected {
                return Err(invalid("cold native value mismatch").into());
            }
            writeln!(
                output.native_read_profiles,
                "{},{},{},{},{},cache-evicted,{},{},{},{},{}",
                case.label(),
                case.value_bytes,
                case.batch_size,
                case.window,
                case.repetition,
                sample,
                p.index_ns,
                p.wal_and_decode_ns,
                p.value_cache_hit,
                whole
            )?;
        }
    }
    if let Backend::Tcp(db) = backend {
        write_node_profiles(
            output,
            case,
            "cache-evicted-read-diagnostics",
            db.connection_profiles(false, false)?,
        )?;
    }
    let after = evidence(backend, false)?;
    record(output, case, "cache_evicted_diagnostics", &before, &after)?;
    Ok(())
}
