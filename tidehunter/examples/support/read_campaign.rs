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

type WriteProcess = (
    &'static str,
    Option<u32>,
    tidehunter::read_probe::ProcessResources,
);

/// Snapshot process-accounted I/O on the controller and every actual storage owner.
pub(super) fn write_evidence(backend: &mut Backend) -> Result<Vec<WriteProcess>> {
    let mut snapshots = Vec::new();
    match backend {
        Backend::Native { .. } => snapshots.push((
            "native",
            None,
            tidehunter::read_probe::ProcessResources::process()?,
        )),
        Backend::Tcp(db) => {
            snapshots.push((
                "controller",
                None,
                tidehunter::read_probe::ProcessResources::process()?,
            ));
            snapshots.extend(
                db.process_resources_for_benchmark()?
                    .into_iter()
                    .map(|(partition, resources)| ("storage", Some(partition), resources)),
            );
        }
        Backend::Sharded(_) => return Err(invalid("write I/O evidence needs native or TCP").into()),
    }
    Ok(snapshots)
}

pub(super) fn record_write_evidence(
    output: &mut Output,
    case: &Case,
    phase: &str,
    before: &[WriteProcess],
    after: &[WriteProcess],
) -> Result<()> {
    if before.len() != after.len() {
        return Err(invalid("write evidence process inventory changed").into());
    }
    for ((role, partition, first), (last_role, last_partition, last)) in before.iter().zip(after) {
        if role != last_role || partition != last_partition || first.pid != last.pid {
            return Err(invalid("write evidence process identity changed").into());
        }
        if let (Some(first_start), Some(last_start)) =
            (first.process_start_ticks, last.process_start_ticks)
            && first_start != last_start
        {
            return Err(invalid("write evidence process start identity changed").into());
        }
        let read_delta = first
            .accounted_process_read_bytes
            .zip(last.accounted_process_read_bytes)
            .and_then(|(first, last)| last.checked_sub(first));
        let write_delta = first
            .accounted_process_write_bytes
            .zip(last.accounted_process_write_bytes)
            .and_then(|(first, last)| last.checked_sub(first));
        writeln!(
            output.write_io,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            case.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            phase,
            role,
            partition.map_or_else(String::new, |partition| partition.to_string()),
            last.pid,
            last.process_start_ticks
                .map_or_else(String::new, |ticks| ticks.to_string()),
            first
                .accounted_process_read_bytes
                .map_or_else(String::new, |bytes| bytes.to_string()),
            last.accounted_process_read_bytes
                .map_or_else(String::new, |bytes| bytes.to_string()),
            read_delta.map_or_else(String::new, |bytes| bytes.to_string()),
            first
                .accounted_process_write_bytes
                .map_or_else(String::new, |bytes| bytes.to_string()),
            last.accounted_process_write_bytes
                .map_or_else(String::new, |bytes| bytes.to_string()),
            write_delta.map_or_else(String::new, |bytes| bytes.to_string()),
        )?;
    }
    output.write_io.flush()?;
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
    // proc read_bytes is accounted at process level; it is interval evidence,
    // not per-key attribution or physical-device saturation.
    if physical == 0 {
        return Err(invalid(
            "cold-read phase caused zero accounted physical reads; no cold result is valid",
        )
        .into());
    }
    Ok(())
}

fn cold_process_identity(first: &ReadEvidence, last: &ReadEvidence) -> bool {
    first.pid != 0
        && first.pid == last.pid
        && first.start_ticks > 0
        && first.start_ticks == last.start_ticks
        && first.process_boot_id.is_some()
        && first.process_boot_id == last.process_boot_id
}

// Keep each probe's preparation, one timed API call, validation, and evidence
// collection together so the clock boundaries remain explicit.
#[allow(clippy::too_many_lines)]
fn cold_single_probes_run(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    probe_count: usize,
) -> Result<()> {
    if oracle.is_empty() || probe_count == 0 {
        return Err(invalid("cold single probes require a nonempty loaded dataset").into());
    }
    let phase_wall = Instant::now();
    let mut samples = Vec::with_capacity(probe_count);
    let mut all_probes_qualified = true;
    for probe_index in 0..probe_count {
        let preparation = Instant::now();
        backend.drain()?;
        #[allow(clippy::cast_possible_truncation)]
        let key_index = mix(probe_index as u64) as usize % oracle.len();
        let expected_value = oracle[key_index]
            .map(|generation| value(key_index, generation, case.value_bytes))
            .ok_or_else(|| invalid("cold single probe selected an absent key"))?;
        let query_key = key(key_index);
        let expected_owner = match backend {
            Backend::Native { .. } => 0,
            Backend::Tcp(db) => db
                .partition_for_key_for_benchmark(ks, &query_key)?
                .ok_or_else(|| invalid("cold single probe has no authoritative owner"))?,
            Backend::Sharded(_) => {
                return Err(invalid("cold single probes require native or TCP").into());
            }
        };
        let before = evidence(backend, true)?;
        let preparation_wall_ns = ns(preparation);

        let started = Instant::now();
        let found = backend.get(ks, &query_key)?;
        let api_elapsed_ns = ns(started);
        let value_verified = found.as_ref() == Some(&expected_value);
        if !value_verified {
            return Err(invalid("cold single probe value verification failed").into());
        }
        let after = evidence(backend, false)?;
        if before.len() != after.len() {
            return Err(invalid("cold probe process inventory changed").into());
        }

        let mut observations = Vec::with_capacity(before.len());
        let mut expected_owner_delta = None;
        let mut positive_any_owner = false;
        let mut identity_qualified = true;
        for ((partition, first), (last_partition, last)) in before.iter().zip(&after) {
            if partition != last_partition {
                return Err(invalid("cold probe owner inventory changed").into());
            }
            let delta = last.delta(*first)?;
            identity_qualified &= cold_process_identity(first, last);
            positive_any_owner |= delta.read_bytes > 0;
            if *partition == expected_owner {
                expected_owner_delta = Some(delta.read_bytes);
            }
            observations.push(serde_json::json!({
                "partition": partition,
                "process_boot_id": last.process_boot_id,
                "pid": last.pid,
                "process_start_ticks": last.start_ticks,
                "accounted_process_read_bytes_before": first.read_bytes,
                "accounted_process_read_bytes_after": last.read_bytes,
                "accounted_process_read_bytes_delta": delta.read_bytes,
                "minor_fault_delta": delta.minor_faults,
                "major_fault_delta": delta.major_faults,
                "file_bytes": last.file_bytes,
                "resident_bytes_before": first.resident_bytes,
                "resident_bytes_after": last.resident_bytes,
            }));
        }
        let expected_owner_delta = expected_owner_delta
            .ok_or_else(|| invalid("cold probe expected owner is absent from process evidence"))?;
        let positive_expected_owner = expected_owner_delta > 0;
        all_probes_qualified &= positive_expected_owner && identity_qualified;
        serde_json::to_writer(
            &mut output.cold_probes,
            &serde_json::json!({
                "backend": case.label(),
                "transport": case.transport.label(),
                "value_bytes": case.value_bytes,
                "batch_size": case.batch_size,
                "window": case.window,
                "repetition": case.repetition,
                "phase": "physical_cold_get",
                "probe_index": probe_index,
                "requested_probes": probe_count,
                "key_index": key_index,
                "expected_owner": expected_owner,
                "api_elapsed_ns": api_elapsed_ns,
                "preparation_wall_ns": preparation_wall_ns,
                "value_verified": value_verified,
                "identity_qualified": identity_qualified,
                "expected_owner_accounted_process_read_bytes_delta": expected_owner_delta,
                "positive_expected_owner_accounted_process_read": positive_expected_owner,
                "positive_any_owner_accounted_process_read": positive_any_owner,
                "observations": observations,
            }),
        )?;
        writeln!(output.cold_probes)?;
        samples.push(super::Sample {
            elapsed_ns: api_elapsed_ns,
            operations: 1,
            bytes: case.value_bytes,
        });
    }
    output.cold_probes.flush()?;
    output.phase(case, "physical_cold_get", ns(phase_wall), &samples)?;
    if !all_probes_qualified {
        return Err(invalid(
            "physical_cold_get is unqualified: at least one probe lacks stable process identity or expected-owner physical reads",
        )
        .into());
    }
    Ok(())
}
fn cold_group_key_indices(start: usize, loaded_keys: usize) -> Result<Vec<usize>> {
    if loaded_keys < 32 {
        return Err(invalid("cold group probes require at least 32 loaded keys").into());
    }
    if start >= loaded_keys {
        return Err(invalid("cold group probe start is outside the loaded dataset").into());
    }
    Ok((0..32)
        .map(|member| (start + member) % loaded_keys)
        .collect())
}

// One complete group call is a single sample; keep every owner lookup,
// eviction, value check, and process snapshot outside its API-only timer.
#[allow(clippy::too_many_lines)]
fn cold_group_probe(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    probe_index: usize,
    probe_count: usize,
) -> Result<(super::Sample, bool)> {
    let preparation = Instant::now();
    backend.drain()?;
    #[allow(clippy::cast_possible_truncation)]
    let start_key_index = mix(probe_index as u64) as usize % oracle.len();
    let key_indices = cold_group_key_indices(start_key_index, oracle.len())?;
    let query_keys: Vec<_> = key_indices
        .iter()
        .map(|&index| key(index).to_vec())
        .collect();
    let mut member_owners = Vec::with_capacity(32);
    for query_key in &query_keys {
        let owner = match backend {
            Backend::Native { .. } => 0,
            Backend::Tcp(db) => db
                .partition_for_key_for_benchmark(ks, query_key)?
                .ok_or_else(|| invalid("cold group member has no authoritative owner"))?,
            Backend::Sharded(_) => {
                return Err(invalid("cold group probes require native or TCP").into());
            }
        };
        member_owners.push(owner);
    }
    let mut expected_owners = member_owners.clone();
    expected_owners.sort_unstable();
    expected_owners.dedup();
    let expected_values: Vec<_> = key_indices
        .iter()
        .map(|&index| {
            oracle[index]
                .map(|generation| Some(value(index, generation, case.value_bytes)))
                .ok_or_else(|| invalid("cold group probe selected an absent key"))
        })
        .collect::<std::io::Result<_>>()?;
    let before = evidence(backend, true)?;
    let preparation_wall_ns = ns(preparation);

    let api_started = Instant::now();
    let api_result = backend.get_many(ks, &query_keys);
    let api_elapsed_ns = ns(api_started);
    let (found, api_error) = match api_result {
        Ok(found) => (Some(found), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let returned_member_count = found.as_ref().map_or(0, Vec::len);
    let values_verified = found
        .as_ref()
        .is_some_and(|found| found == &expected_values);
    let after = evidence(backend, false)?;
    if before.len() != after.len() {
        return Err(invalid("cold group process inventory changed").into());
    }

    let mut observations = Vec::with_capacity(before.len());
    let mut observed_expected_owners = Vec::new();
    let mut expected_owner_reads_positive = true;
    let mut identity_qualified = true;
    for ((partition, first), (last_partition, last)) in before.iter().zip(&after) {
        if partition != last_partition {
            return Err(invalid("cold group owner inventory changed").into());
        }
        let delta = last.delta(*first)?;
        identity_qualified &= cold_process_identity(first, last);
        if expected_owners.contains(partition) {
            observed_expected_owners.push(*partition);
            expected_owner_reads_positive &= delta.read_bytes > 0;
        }
        observations.push(serde_json::json!({
            "partition": partition,
            "process_boot_id": last.process_boot_id,
            "pid": last.pid,
            "process_start_ticks": last.start_ticks,
            "accounted_process_read_bytes_before": first.read_bytes,
            "accounted_process_read_bytes_after": last.read_bytes,
            "accounted_process_read_bytes_delta": delta.read_bytes,
            "minor_fault_delta": delta.minor_faults,
            "major_fault_delta": delta.major_faults,
            "file_bytes": last.file_bytes,
            "resident_bytes_before": first.resident_bytes,
            "resident_bytes_after": last.resident_bytes,
        }));
    }
    let expected_owners_observed = expected_owners
        .iter()
        .all(|owner| observed_expected_owners.contains(owner));
    let probe_qualified = values_verified
        && expected_owners_observed
        && expected_owner_reads_positive
        && identity_qualified;
    serde_json::to_writer(
        &mut output.cold_group_probes,
        &serde_json::json!({
            "backend": case.label(),
            "transport": case.transport.label(),
            "value_bytes": case.value_bytes,
            "batch_size": case.batch_size,
            "window": case.window,
            "repetition": case.repetition,
            "phase": "physical_cold_get_many",
            "probe_index": probe_index,
            "requested_probes": probe_count,
            "start_key_index": start_key_index,
            "key_indices": key_indices,
            "member_owners": member_owners,
            "expected_owners": expected_owners,
            "member_count": 32,
            "returned_member_count": returned_member_count,
            "api_elapsed_ns": api_elapsed_ns,
            "preparation_wall_ns": preparation_wall_ns,
            "values_verified": values_verified,
            "api_error": api_error,
            "expected_owners_observed": expected_owners_observed,
            "expected_owner_reads_positive": expected_owner_reads_positive,
            "identity_qualified": identity_qualified,
            "probe_qualified": probe_qualified,
            "observations": observations,
        }),
    )?;
    writeln!(output.cold_group_probes)?;
    Ok((
        super::Sample {
            elapsed_ns: api_elapsed_ns,
            operations: 32,
            bytes: 32 * case.value_bytes,
        },
        probe_qualified,
    ))
}

#[allow(clippy::too_many_lines)]
fn cold_group_probes_run(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    probe_count: usize,
) -> Result<()> {
    if probe_count == 0 {
        return Err(invalid("cold group probe count must be nonzero").into());
    }
    if oracle.len() < 32 {
        return Err(invalid("cold group probes require at least 32 loaded keys").into());
    }
    let phase_wall = Instant::now();
    let mut samples = Vec::with_capacity(probe_count);
    let mut every_probe_qualified = true;
    for probe_index in 0..probe_count {
        let (sample, qualified) =
            cold_group_probe(backend, case, output, ks, oracle, probe_index, probe_count)?;
        samples.push(sample);
        every_probe_qualified &= qualified;
    }
    output.cold_group_probes.flush()?;
    output.phase(case, "physical_cold_get_many", ns(phase_wall), &samples)?;
    if !every_probe_qualified {
        return Err(invalid(
            "physical_cold_get_many is unqualified: at least one probe failed value, process-identity or expected-owner physical-read verification",
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
    cold_single_probes: usize,
    cold_group_probes: usize,
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
    if !diagnostics {
        // Clean cold intervals retain counter/residency/physical-IO evidence
        // without enabling detailed per-read or provider clocks.
        if cold_single_probes != 0 {
            cold_single_probes_run(backend, case, output, ks, oracle, cold_single_probes)?;
        }
        if cold_group_probes != 0 {
            cold_group_probes_run(backend, case, output, ks, oracle, cold_group_probes)?;
        }
        return Ok(());
    }
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
                "{},{},{},{},{},cache-evicted,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
                p.tcp.server_response_recheck_deferrals,
                p.tcp.requests,
                p.tcp.server_response_recheck_samples != 0,
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
    if cold_single_probes != 0 {
        cold_single_probes_run(backend, case, output, ks, oracle, cold_single_probes)?;
    }
    if cold_group_probes != 0 {
        cold_group_probes_run(backend, case, output, ks, oracle, cold_group_probes)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{ReadEvidence, cold_group_key_indices, cold_process_identity};

    #[test]
    fn cold_probe_identity_requires_known_boot_and_same_process_lifetime() {
        let first = ReadEvidence {
            pid: 17,
            start_ticks: 5,
            process_boot_id: Some([1; 16]),
            ..ReadEvidence::default()
        };
        assert!(cold_process_identity(&first, &first));
        for changed in [
            ReadEvidence { pid: 18, ..first },
            ReadEvidence {
                start_ticks: 6,
                ..first
            },
            ReadEvidence {
                process_boot_id: Some([2; 16]),
                ..first
            },
            ReadEvidence {
                process_boot_id: None,
                ..first
            },
        ] {
            assert!(!cold_process_identity(&first, &changed));
        }
        for unknown in [
            ReadEvidence { pid: 0, ..first },
            ReadEvidence {
                start_ticks: 0,
                ..first
            },
            ReadEvidence {
                process_boot_id: None,
                ..first
            },
        ] {
            assert!(!cold_process_identity(&unknown, &unknown));
        }
    }

    #[test]
    fn cold_group_indices_wrap_without_duplicates_and_reject_small_datasets() {
        let indices = cold_group_key_indices(31, 32).unwrap();
        assert_eq!(indices.len(), 32);
        assert_eq!(indices[0], 31);
        assert_eq!(indices[1], 0);
        assert_eq!(indices[31], 30);
        let mut sorted = indices.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 32);
        assert!(cold_group_key_indices(0, 31).is_err());
    }
}
