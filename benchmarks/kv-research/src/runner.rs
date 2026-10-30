use crate::backend::{BackendResult, BatchAtomicity, KvBackend};
use crate::config::{ExecutionMode, RunConfig};
use crate::duration_ns;
use crate::report::{Reporter, RunSummary, Sample};
use crate::workload::{
    OperationKind, WorkItem, WorkloadGenerator, fresh_namespace, key_bytes, value_bytes,
};
use blake2::{Blake2b512, Digest};
use std::borrow::Cow;
use std::future::Future;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tidehunter::minibytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout_at};

// One owner enforces qualification, phase order, shutdown and terminal artifact publication.
#[allow(clippy::too_many_lines)]
pub async fn run<B: KvBackend>(
    backend: B,
    config: RunConfig,
    output: &Path,
    registry: prometheus::Registry,
) -> BackendResult<()> {
    config.validate()?;
    let info = backend.info();
    if info.name.is_empty() {
        return Err(invalid("backend returned empty identity"));
    }
    if config.workload.mix.put_batch > 0 && info.batch_atomicity == BatchAtomicity::Unsupported {
        return Err(invalid(
            "put_batch requested but backend does not provide supported batch semantics",
        ));
    }
    let namespace = if let Some(encoded) = &config.key_namespace_hex {
        let mut namespace = [0; 16];
        for (byte, pair) in namespace.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        }
        namespace
    } else {
        fresh_namespace()?
    };
    // Qualification remains isolated even when an owned cloud dataset is reused.
    let validation_namespace = derive_namespace(&fresh_namespace()?, b"qualification");
    let config_bytes = serde_json::to_vec(&config)?;
    let config_hash = hex_digest(&config_bytes);
    let started_wall = unix_seconds();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(config.limits.deadline_seconds);
    let mut reporter = Reporter::create(output, registry, info.name.clone())?;
    reporter.set_phase("qualification");
    reporter.snapshot()?;

    let backend = Arc::new(backend);
    let mut failed = false;
    let mut fatal_reason = None;
    match qualify_on_worker(
        Arc::clone(&backend),
        validation_namespace,
        config.seed,
        deadline,
        config.execution.request_timeout_ms,
        config.limits.shutdown_grace_seconds,
        &mut reporter,
    )
    .await
    {
        Ok(()) => reporter.snapshot()?,
        Err(error) => {
            failed = true;
            fatal_reason = Some(format!("pre-workload qualification failed: {error}"));
        }
    }

    let mut generator = WorkloadGenerator::new(
        config.seed ^ 0x6b76_7265_7365_6172,
        config.dataset.keys,
        config.workload.mix.clone(),
        config.workload.batch_size,
        config.workload.distribution.clone(),
    );
    let require_present_reads =
        !config.qualification_only && config.phases.preload && config.workload.mix.delete == 0;
    if fatal_reason.is_none() && !config.qualification_only && config.phases.preload {
        reporter.set_phase("preload");
        match preload(
            Arc::clone(&backend),
            &config,
            &namespace,
            started,
            deadline,
            &mut reporter,
        )
        .await
        {
            Ok(()) => reporter.snapshot()?,
            Err(error) => {
                failed = true;
                fatal_reason = Some(format!("preload failed: {error}"));
            }
        }
    }
    for (phase, count) in [
        ("warmup", config.phases.warmup_operations),
        ("aging", config.phases.aging_operations),
    ] {
        if config.qualification_only || fatal_reason.is_some() || count == 0 {
            continue;
        }
        reporter.set_phase(phase);
        if let Err(error) = run_phase(
            Arc::clone(&backend),
            &config,
            &namespace,
            &mut generator,
            phase,
            count,
            started,
            deadline,
            require_present_reads,
            &mut reporter,
        )
        .await
        {
            failed = true;
            fatal_reason = Some(format!("{phase} phase stopped: {error}"));
        }
        reporter.snapshot()?;
    }
    let mut measurement_attempted = false;
    let mut profile_arm_error = None;
    let mut read_profile_arm_error = None;
    let mut read_profile_armed = false;
    let mut read_start = serde_json::Value::Null;
    let mut read_end = serde_json::Value::Null;
    let mut measurement_drained = false;
    if !config.qualification_only
        && fatal_reason.is_none()
        && config.phases.measurement_operations > 0
    {
        if let Some(gate) = &config.measurement_gate
            && let Err(error) = wait_for_measurement_gate(gate, deadline).await
        {
            failed = true;
            fatal_reason = Some(format!("measurement gate failed: {error}"));
            reporter.snapshot()?;
        }
        if fatal_reason.is_none()
            && config.diagnostics.write_profile_limit > 0
            && let Err(error) = backend.arm_write_profile(config.diagnostics.write_profile_limit)
        {
            failed = true;
            let reason = format!("measurement write profile arm failed: {error}");
            profile_arm_error = Some(reason.clone());
            fatal_reason = Some(reason);
            reporter.snapshot()?;
        }
        if fatal_reason.is_none() && config.diagnostics.read_profile_limit > 0 {
            match backend.arm_read_profile(config.diagnostics.read_profile_limit) {
                Ok(()) => read_profile_armed = true,
                Err(error) => {
                    failed = true;
                    let reason = format!("measurement read profile arm failed: {error}");
                    read_profile_arm_error = Some(reason.clone());
                    fatal_reason = Some(reason);
                    reporter.snapshot()?;
                }
            }
        }
        if fatal_reason.is_none() {
            measurement_attempted = true;
            reporter.set_phase("measurement");
            read_start = backend.read_observation();
            let phase_result = run_phase(
                Arc::clone(&backend),
                &config,
                &namespace,
                &mut generator,
                "measurement",
                config.phases.measurement_operations,
                started,
                deadline,
                require_present_reads,
                &mut reporter,
            )
            .await;
            measurement_drained = phase_result.is_ok();
            read_end = backend.read_observation();
            if let Err(error) = phase_result {
                failed = true;
                fatal_reason = Some(format!("measurement phase stopped: {error}"));
            }
            reporter.snapshot()?;
        }
    }
    let evidence = measurement_read_evidence(
        &read_start,
        &read_end,
        measurement_attempted,
        measurement_drained,
    );
    std::fs::write(
        output.join("measurement-read-evidence.json"),
        serde_json::to_vec_pretty(&evidence)?,
    )?;

    if measurement_attempted
        && let Some(gate) = &config.measurement_completion_gate
        && let Err(error) = wait_for_measurement_gate(gate, deadline).await
    {
        failed = true;
        fatal_reason = Some(fatal_reason.map_or_else(
            || format!("measurement completion gate failed: {error}"),
            |previous| format!("{previous}; measurement completion gate failed: {error}"),
        ));
        reporter.snapshot()?;
    }
    if config.diagnostics.write_profile_limit > 0 {
        let profile_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
        let export = export_write_diagnostics(
            backend.as_ref(),
            &config,
            output,
            measurement_attempted,
            profile_arm_error.as_deref(),
            profile_deadline,
        )
        .await;
        match export {
            Ok(Some(error)) => {
                failed = true;
                fatal_reason = Some(fatal_reason.map_or_else(
                    || format!("write diagnostics finalization failed: {error}"),
                    |previous| {
                        format!("{previous}; write diagnostics finalization failed: {error}")
                    },
                ));
            }
            Ok(None) => {}
            Err(error) => {
                failed = true;
                fatal_reason = Some(fatal_reason.map_or_else(
                    || format!("write diagnostics export failed: {error}"),
                    |previous| format!("{previous}; write diagnostics export failed: {error}"),
                ));
            }
        }
    }
    if config.diagnostics.read_profile_limit > 0 {
        let profile_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
        match export_read_diagnostics(
            backend.as_ref(),
            &config,
            output,
            measurement_attempted,
            measurement_drained,
            read_profile_armed,
            read_profile_arm_error.as_deref(),
            profile_deadline,
            &evidence,
        )
        .await
        {
            Ok(Some(error)) => {
                failed = true;
                fatal_reason = Some(fatal_reason.map_or_else(
                    || format!("read diagnostics finalization failed: {error}"),
                    |previous| format!("{previous}; read diagnostics finalization failed: {error}"),
                ));
            }
            Ok(None) => {}
            Err(error) => {
                failed = true;
                fatal_reason = Some(fatal_reason.map_or_else(
                    || format!("read diagnostics export failed: {error}"),
                    |previous| format!("{previous}; read diagnostics export failed: {error}"),
                ));
            }
        }
    }
    reporter.set_phase("qualification");
    if fatal_reason.is_none() && Instant::now() < deadline {
        if let Err(error) = qualify_on_worker(
            Arc::clone(&backend),
            derive_namespace(&namespace, b"qualification-post"),
            config.seed,
            deadline,
            config.execution.request_timeout_ms,
            config.limits.shutdown_grace_seconds,
            &mut reporter,
        )
        .await
        {
            failed = true;
            fatal_reason = Some(format!("post-workload qualification failed: {error}"));
        }
    } else if fatal_reason.is_none() {
        failed = true;
        fatal_reason =
            Some("global deadline expired before post-workload qualification".to_owned());
    }
    if let Err(error) = shutdown_on_worker(
        backend,
        deadline,
        config.limits.shutdown_grace_seconds,
        &mut reporter,
    )
    .await
    {
        failed = true;
        fatal_reason = Some(fatal_reason.map_or_else(
            || format!("backend shutdown failed: {error}"),
            |previous| format!("{previous}; backend shutdown failed: {error}"),
        ));
    }

    let finished_wall = unix_seconds();
    let totals = reporter.totals();
    if totals.3 > 0 || totals.4 > 0 {
        failed = true;
        let detail = format!(
            "workload recorded {} timeout/unknown and {} error/wrong-value outcomes",
            totals.3, totals.4
        );
        fatal_reason =
            Some(fatal_reason.map_or(detail.clone(), |previous| format!("{previous}; {detail}")));
    }
    let summary = RunSummary {
        status: if failed { "failed".to_owned() } else { "completed".to_owned() },
        started_unix_seconds: started_wall,
        finished_unix_seconds: finished_wall,
        backend: info,
        phase_counts: serde_json::json!({
            "offered": totals.0, "completed_terminal": totals.1, "succeeded": totals.6, "rejected": totals.2,
            "timeouts_or_unknown_mutations": totals.3, "errors_or_wrong_values": totals.4,
            "logical_keys_completed_terminal": totals.5, "logical_keys_offered": totals.7, "logical_keys_rejected": totals.8,
            "outstanding_at_finalization": totals.0.saturating_sub(totals.1).saturating_sub(totals.2),
        }),
        distributions: reporter.distribution_summaries(),
        limitations: vec![
            "Finite local workload phases are not evidence of sustainable steady-state capacity.".to_owned(),
            "Owner physical-device independence is not inferred from configured topology; see backend topology evidence.".to_owned(),
            "Measured API latency ends when response bytes are materialized; byte verification is outside the API timer.".to_owned(),
            "Batch latency is one complete backend API call; logical key counts are reported separately and no per-key latency is inferred.".to_owned(),
            "Concurrent workload traces do not assert a completion-order final-state oracle or linearizability.".to_owned(),
            "Synchronous adapters run on fixed dedicated workers; Tokio timeouts cannot preempt blocking calls, so overruns are retained and the process supervisor supplies the hard wall-clock bound.".to_owned(),
        ],
    };
    let manifest_extra = serde_json::json!({
        "config_hash_blake2b512": config_hash,
        "binary": binary_identity(),
        "config": config,
        "run_namespace_hex": hex(&namespace),
        "seed": config.seed,
        "source_revision": config.source_revision.as_deref().or(option_env!("GIT_COMMIT")),
        "hardware": hardware_identity(),
        "timing": {"clock": "std::time::Instant monotonic", "timer_start": "backend API dispatch after bounded admission", "timer_end": "response materialized"},
        "measurement_read_evidence_schema_version": 1,
        "request_timeout_ms": config.execution.request_timeout_ms,
        "data_policy": {"key_bytes": 32, "key_derivation": "BLAKE2(namespace,seed,logical_key_id), all bits hashed",
            "value_identity_header_bytes": 16, "value_entropy_fraction_of_remainder": config.dataset.entropy},
        "phase_semantics": {"preload": config.phases.preload, "warmup_operations": config.phases.warmup_operations,
            "warmup_key_sweep": config.phases.warmup_key_sweep,
            "aging_operations": config.phases.aging_operations, "measurement_operations": config.phases.measurement_operations,
            "measurement_key_sweep": config.phases.measurement_key_sweep,
            "sustainability_claim": false},
        "qualification_failure": fatal_reason,
    });
    reporter.finish(&summary, failed, &manifest_extra)?;
    if failed {
        return Err(invalid(
            &fatal_reason.unwrap_or_else(|| "run failed".to_owned()),
        ));
    }
    Ok(())
}

fn counter_delta(
    start: &serde_json::Value,
    end: &serde_json::Value,
    fields: &[&str],
) -> serde_json::Value {
    let mut delta = serde_json::Map::new();
    for field in fields {
        let Some((a, b)) = start
            .get(field)
            .and_then(serde_json::Value::as_u64)
            .zip(end.get(field).and_then(serde_json::Value::as_u64))
        else {
            return serde_json::json!({"available": false, "error": format!("missing counter {field}")});
        };
        let Some(value) = b.checked_sub(a) else {
            return serde_json::json!({"available": false, "error": format!("counter decreased: {field}")});
        };
        delta.insert((*field).to_owned(), value.into());
    }
    serde_json::json!({"available": true, "counters": delta})
}

fn partition_deltas(
    start: &serde_json::Value,
    end: &serde_json::Value,
    fields: &[&str],
) -> serde_json::Value {
    if start["available"] != true || end["available"] != true {
        return serde_json::json!({"available": false, "error": "partition sample unavailable at a boundary"});
    }
    let (Some(a), Some(b)) = (
        start
            .get("partitions")
            .and_then(serde_json::Value::as_object),
        end.get("partitions").and_then(serde_json::Value::as_object),
    ) else {
        return serde_json::json!({"available": false, "error": "partition snapshot unavailable"});
    };
    let mut partitions = serde_json::Map::new();
    for key in a.keys().chain(b.keys()) {
        if partitions.contains_key(key) {
            continue;
        }
        let value = match (a.get(key), b.get(key)) {
            (Some(first), Some(last)) => counter_delta(first, last, fields),
            _ => {
                serde_json::json!({"available": false, "error": "partition missing at one boundary"})
            }
        };
        partitions.insert(key.clone(), value);
    }
    serde_json::json!({"available": true, "partitions": partitions})
}

fn lane_deltas(
    start: &serde_json::Value,
    end: &serde_json::Value,
    fields: &[&str],
) -> serde_json::Value {
    if start["available"] != true || end["available"] != true {
        return serde_json::json!({"available": false, "error": "lane snapshots unavailable at a boundary"});
    }
    let (Some(first), Some(last)) = (
        start
            .get("partitions")
            .and_then(serde_json::Value::as_object),
        end.get("partitions").and_then(serde_json::Value::as_object),
    ) else {
        return serde_json::json!({"available": false, "error": "lane partitions unavailable"});
    };
    let mut partitions = serde_json::Map::new();
    for key in first.keys().chain(last.keys()) {
        if partitions.contains_key(key) {
            continue;
        }
        let delta = match (first.get(key), last.get(key)) {
            (Some(a), Some(b)) => {
                let snapshot = |row: &serde_json::Value| {
                    let lanes = row.get("lanes").and_then(serde_json::Value::as_object);
                    let counters: serde_json::Map<String, serde_json::Value> = lanes
                        .into_iter()
                        .flat_map(|lanes| lanes.iter())
                        .map(|(lane, value)| {
                            (
                                lane.clone(),
                                value.get("stats").cloned().unwrap_or_default(),
                            )
                        })
                        .collect();
                    serde_json::json!({"available": lanes.is_some(), "partitions": counters})
                };
                let delta = partition_deltas(&snapshot(a), &snapshot(b), fields);
                serde_json::json!({"available": delta["available"], "lanes": delta["partitions"], "error": delta.get("error")})
            }
            _ => {
                serde_json::json!({"available": false, "error": "partition missing at one boundary"})
            }
        };
        partitions.insert(key.clone(), delta);
    }
    serde_json::json!({"available": true, "partitions": partitions})
}

fn owner_response_deltas(start: &serde_json::Value, end: &serde_json::Value) -> serde_json::Value {
    if start["available"] != true || end["available"] != true {
        return serde_json::json!({"available": false, "error": "owner snapshots unavailable at a boundary"});
    }
    let (Some(first), Some(last)) = (
        start
            .get("partitions")
            .and_then(serde_json::Value::as_object),
        end.get("partitions").and_then(serde_json::Value::as_object),
    ) else {
        return serde_json::json!({"available": false, "error": "owner partitions unavailable"});
    };
    let mut partitions = serde_json::Map::new();
    for key in first.keys().chain(last.keys()) {
        if partitions.contains_key(key) {
            continue;
        }
        let mut stages = serde_json::Map::new();
        for (stage, parent, field) in [
            ("read_response_encode", "", "read_response_encode"),
            ("read_output_queue", "", "read_output_queue"),
            ("read_output_submit", "", "read_output_submit"),
            ("read_response_retire", "", "read_response_retire"),
            ("read_grant_lookup", "read_grant", "lookup"),
            ("read_grant_reservation", "read_grant", "reservation"),
            ("read_grant_envelope", "read_grant", "envelope"),
            ("read_grant_registration", "read_grant", "registration"),
            (
                "read_grant_recheck_publication",
                "read_grant",
                "recheck_publication",
            ),
        ] {
            let value = match (first.get(key), last.get(key)) {
                (Some(a), Some(b)) => {
                    let (a, b) = if parent.is_empty() {
                        (a, b)
                    } else {
                        (&a[parent], &b[parent])
                    };
                    counter_delta(&a[field], &b[field], &["ns", "count", "errors"])
                }
                _ => {
                    serde_json::json!({"available": false, "error": "partition missing at one boundary"})
                }
            };
            stages.insert(stage.to_owned(), value);
        }
        partitions.insert(key.clone(), serde_json::Value::Object(stages));
    }
    serde_json::json!({
        "available": true, "partitions": partitions,
        "population": "sampled owner grant attempts and response frames across node boundary snapshots; each stage has its own count, independent of completed coordinator flights",
        "max_semantics": "lifetime maxima in raw start/end snapshots cannot be differenced into measurement maxima"
    })
}

fn measurement_read_evidence(
    start: &serde_json::Value,
    end: &serde_json::Value,
    attempted: bool,
    drained: bool,
) -> serde_json::Value {
    const COORDINATOR: &[&str] = &[
        "read_rpc_count",
        "hits",
        "misses",
        "inserts",
        "evictions",
        "rejected_oversize",
        "rejected_mismatch",
        "bypass_disabled",
        "bypass_admission",
        "bypass_eviction_limit",
        "admitted_bytes",
        "served_bytes",
    ];
    const OWNER: &[&str] = &[
        "cache_hits",
        "cache_misses",
        "cache_inserts",
        "cache_evictions",
        "cache_oversized",
        "cache_bypass_disabled",
        "cache_bypass_eviction_limit",
        "cache_bypass_scan",
        "cache_bypass_generation",
        "cache_admitted_bytes",
        "cache_served_bytes",
        "wal_reads",
        "wal_read_bytes",
        "resident_read_attempts",
        "resident_read_successes",
        "resident_read_fallbacks",
        "resident_read_unsupported",
        "resident_read_partial",
        "resident_read_deferred",
        "async_read_submissions",
        "completed_reads",
        "errors",
        "cancellations",
        "buffer_reuses",
        "buffer_allocations",
    ];
    const RESIDENT: &[&str] = &[
        "message_read_requests",
        "grants_issued",
        "grant_candidates",
        "grant_unavailable",
        "grant_acquisitions",
        "rma_completions",
        "eligible_reads",
        "rma_posts",
        "rma_hits",
        "rma_errors",
        "fallback_missing",
        "fallback_oversize",
        "fallback_capacity",
        "fallback_unsupported",
        "fallback_stale",
    ];
    const OWNER_GRANTS: &[&str] = &[
        "attempts",
        "issued",
        "reused",
        "denied_request_limit",
        "denied_oversize",
        "denied_regions",
        "denied_bytes",
        "denied_admission",
        "denied_source_unavailable",
        "denied_owner_unavailable",
        "denied_provider",
    ];
    let deltas = if attempted && drained && start["available"] == true && end["available"] == true {
        serde_json::json!({
            "coordinator": counter_delta(&start["coordinator"], &end["coordinator"], COORDINATOR),
            "owner_executor": partition_deltas(&start["owner_executor"], &end["owner_executor"], OWNER),
            "client_resident": partition_deltas(&start["client_resident"], &end["client_resident"], RESIDENT),
            "client_resident_lanes": lane_deltas(&start["client_resident_lanes"], &end["client_resident_lanes"], RESIDENT),
            "owner_response": owner_response_deltas(&start["owner_grants"], &end["owner_grants"]),
            "owner_output_abandoned": partition_deltas(&start["owner_grants"], &end["owner_grants"], &["read_output_abandoned"]),
            "owner_grants": partition_deltas(&start["owner_grants"], &end["owner_grants"], OWNER_GRANTS)
        })
    } else {
        serde_json::json!({"available": false, "error": if !attempted { "measurement not attempted" }
            else if !drained { "measurement did not complete and drain successfully" }
            else { "backend observation unavailable at a boundary" }})
    };
    serde_json::json!({
        "schema_version": 1, "scope": "measurement_boundary",
        "start_boundary": "after warmup, aging and measurement gate; before measurement run_phase",
        "end_boundary": "after measurement run_phase and worker drain; before completion gate, profile finalization and postqualification",
        "measurement_attempted": attempted, "measurement_drained": drained,
        "snapshot_semantics": "sequential component and owner samples, not a globally atomic cut; raw snapshots include gauges and lifetime peaks",
        "limitations": "logical WAL reads and async submissions are not physical IOPS; RMA counters do not establish hardware native FI_READ or per-operation hit fractions",
        "start": start, "end": end, "deltas": deltas
    })
}

async fn preload<B: KvBackend>(
    backend: Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    origin: Instant,
    deadline: Instant,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let mut pool = WorkerPool::new(&backend, config, namespace, origin, false)?;
    let drain_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let mut expired = false;
    for id in 0..config.dataset.keys {
        if Instant::now() >= deadline {
            expired = true;
            break;
        }
        if pool.pending.len() >= config.execution.concurrency {
            match receive_with_snapshots(&mut pool, reporter, drain_deadline, &mut next_snapshot)
                .await
            {
                Ok(completion) => pool.record_completion(completion, reporter)?,
                Err(error) => {
                    let _ = pool.record_unknown_pending(reporter);
                    pool.shutdown(false);
                    return Err(error);
                }
            }
        }
        let work = WorkItem {
            operation: OperationKind::Put,
            keys: vec![id],
        };
        let scheduled = duration_ns(origin.elapsed());
        reporter.offered("preload", "put", 1);
        if let Err(work) = pool.try_dispatch(work, "preload", scheduled, deadline, false) {
            reporter.rejected(rejected_sample(
                "preload",
                &work,
                scheduled,
                origin,
                "worker_queue_full",
            ))?;
        }
        if Instant::now() >= next_snapshot {
            reporter.snapshot()?;
            next_snapshot = Instant::now() + Duration::from_secs(1);
        }
    }
    while !pool.pending.is_empty() {
        match receive_with_snapshots(&mut pool, reporter, drain_deadline, &mut next_snapshot).await
        {
            Ok(completion) => pool.record_completion(completion, reporter)?,
            Err(error) => {
                let _ = pool.record_unknown_pending(reporter);
                pool.shutdown(false);
                return Err(error);
            }
        }
    }
    pool.shutdown(true);
    if expired {
        return Err(invalid("global deadline expired before preload completed"));
    }
    Ok(())
}

async fn receive_with_snapshots(
    pool: &mut WorkerPool,
    reporter: &mut Reporter,
    deadline: Instant,
    next_snapshot: &mut Instant,
) -> BackendResult<WorkerCompletion> {
    loop {
        tokio::select! {
            result = timeout_at(TokioInstant::from_std(deadline), pool.receive()) => return result?,
            () = sleep_until(TokioInstant::from_std(*next_snapshot)) => {
                reporter.snapshot()?;
                *next_snapshot = Instant::now() + Duration::from_secs(1);
            }
        }
    }
}

// Keep the ordered behavioral checks and their cleanup together; no timed workload is mixed in.
#[allow(clippy::too_many_lines)]
async fn qualify<B: KvBackend>(
    backend: &B,
    namespace: &[u8; 16],
    seed: u64,
    deadline: Instant,
    request_timeout_ms: u64,
) -> BackendResult<()> {
    if Instant::now() >= deadline {
        return Err(invalid("deadline expired before qualification"));
    }
    let keys = (0..6)
        .map(|id| key_bytes(namespace, seed, id))
        .collect::<Vec<_>>();
    let first = Bytes::from(vec![0x31; 256]);
    let overwrite = Bytes::from(vec![0x72; 256]);
    let second = Bytes::from(vec![0xb4; 256]);
    call(
        deadline,
        request_timeout_ms,
        backend.put(keys[0].clone(), first.clone()),
        "qualification put",
    )
    .await?;
    check_one(
        deadline,
        request_timeout_ms,
        backend.get(keys[0].as_ref()),
        Some(&first),
        "qualification initial read",
    )
    .await?;
    call(
        deadline,
        request_timeout_ms,
        backend.put(keys[0].clone(), overwrite.clone()),
        "qualification overwrite",
    )
    .await?;
    call(
        deadline,
        request_timeout_ms,
        backend.put(keys[1].clone(), second.clone()),
        "qualification distinct scalar put",
    )
    .await?;
    check_one(
        deadline,
        request_timeout_ms,
        backend.get(keys[0].as_ref()),
        Some(&overwrite),
        "qualification overwritten read",
    )
    .await?;
    check_many(
        deadline,
        request_timeout_ms,
        backend.get_many(&[
            keys[1].clone(),
            keys[4].clone(),
            keys[0].clone(),
            keys[1].clone(),
        ]),
        &[Some(&second), None, Some(&overwrite), Some(&second)],
        "qualification scalar ordered get_many",
    )
    .await?;
    let info = backend.info();
    if info.batch_atomicity != BatchAtomicity::Unsupported {
        call(
            deadline,
            request_timeout_ms,
            backend.put_batch(&[
                (keys[2].clone(), first.clone()),
                (keys[3].clone(), second.clone()),
            ]),
            "qualification put_batch",
        )
        .await?;
        check_many(
            deadline,
            request_timeout_ms,
            backend.get_many(&[
                keys[3].clone(),
                keys[5].clone(),
                keys[2].clone(),
                keys[3].clone(),
            ]),
            &[Some(&second), None, Some(&first), Some(&second)],
            "qualification ordered batch read",
        )
        .await?;
    }
    for key in &keys[..4] {
        call(
            deadline,
            request_timeout_ms,
            backend.delete(key.clone()),
            "qualification delete",
        )
        .await?;
    }
    check_many(
        deadline,
        request_timeout_ms,
        backend.get_many(&[
            keys[3].clone(),
            keys[1].clone(),
            keys[0].clone(),
            keys[4].clone(),
        ]),
        &[None, None, None, None],
        "qualification delete verification",
    )
    .await?;
    Ok(())
}
async fn qualify_on_worker<B: KvBackend>(
    backend: Arc<B>,
    namespace: [u8; 16],
    seed: u64,
    deadline: Instant,
    request_timeout_ms: u64,
    shutdown_grace_seconds: u64,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();
    let worker = thread::Builder::new()
        .name("kv-research-qualification".to_owned())
        .spawn(move || {
            let result = handle.block_on(qualify(
                backend.as_ref(),
                &namespace,
                seed,
                deadline,
                request_timeout_ms,
            ));
            let _ = sender.send(result);
        })?;
    let shutdown_deadline = deadline + Duration::from_secs(shutdown_grace_seconds);
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let result = loop {
        tokio::select! {
            result = &mut receiver => break result.map_err(|_| invalid("qualification worker exited without a result"))?,
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            () = sleep_until(TokioInstant::from_std(shutdown_deadline)) => {
                break Err(invalid("qualification worker exceeded global deadline and shutdown grace"));
            }
        }
    };
    if result.is_ok() || Instant::now() < shutdown_deadline {
        let _ = worker.join();
    }
    result
}
async fn shutdown_on_worker<B: KvBackend>(
    backend: Arc<B>,
    deadline: Instant,
    shutdown_grace_seconds: u64,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let backend = Arc::try_unwrap(backend)
        .map_err(|_| invalid("backend still has outstanding worker references at shutdown"))?;
    let handle = tokio::runtime::Handle::current();
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let worker = thread::Builder::new()
        .name("kv-research-shutdown".to_owned())
        .spawn(move || {
            let _ = sender.send(handle.block_on(backend.shutdown()));
        })?;
    let shutdown_deadline = deadline + Duration::from_secs(shutdown_grace_seconds);
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let result = loop {
        tokio::select! {
            result = &mut receiver => break result.map_err(|_| invalid("backend shutdown worker exited without a result"))?,
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            () = sleep_until(TokioInstant::from_std(shutdown_deadline)) => {
                break Err(invalid("backend shutdown exceeded the global deadline and grace period"));
            }
        }
    };
    if Instant::now() < shutdown_deadline {
        let _ = worker.join();
    }
    result
}

async fn call<F: Future<Output = BackendResult<()>>>(
    deadline: Instant,
    timeout_ms: u64,
    future: F,
    context: &str,
) -> BackendResult<()> {
    let request_deadline = request_deadline(deadline, timeout_ms);
    timeout_at(TokioInstant::from_std(request_deadline), future)
        .await
        .map_err(|_| {
            invalid(&format!(
                "{context} timed out; mutation outcome may be unknown"
            ))
        })?
        .map_err(|error| invalid(&format!("{context}: {error}")))
}

async fn check_one<F: Future<Output = BackendResult<Option<Bytes>>>>(
    deadline: Instant,
    timeout_ms: u64,
    future: F,
    expected: Option<&Bytes>,
    context: &str,
) -> BackendResult<()> {
    let request_deadline = request_deadline(deadline, timeout_ms);
    let actual = timeout_at(TokioInstant::from_std(request_deadline), future)
        .await
        .map_err(|_| invalid(&format!("{context} timed out")))??;
    if actual.as_ref() != expected {
        return Err(invalid(&format!(
            "{context}: returned bytes differ from expected value"
        )));
    }
    Ok(())
}

async fn check_many<F: Future<Output = BackendResult<Vec<Option<Bytes>>>>>(
    deadline: Instant,
    timeout_ms: u64,
    future: F,
    expected: &[Option<&Bytes>],
    context: &str,
) -> BackendResult<()> {
    let request_deadline = request_deadline(deadline, timeout_ms);
    let actual = timeout_at(TokioInstant::from_std(request_deadline), future)
        .await
        .map_err(|_| invalid(&format!("{context} timed out")))??;
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual.as_ref() != *expected)
    {
        return Err(invalid(&format!(
            "{context}: result count, order, missing status, or bytes differ"
        )));
    }
    Ok(())
}

fn request_deadline(global: Instant, timeout_ms: u64) -> Instant {
    global.min(Instant::now() + Duration::from_millis(timeout_ms))
}

struct WorkerRequest {
    id: u64,
    work: WorkItem,
    phase: &'static str,
    profiled_write: bool,
    scheduled_ns: u64,
    origin: Instant,
    global_deadline: Instant,
    dispatch_timestamp: Arc<AtomicU64>,
}

struct WorkerCompletion {
    id: u64,
    worker_index: usize,
    sample: Sample,
}

struct PendingOperation {
    operation: OperationKind,
    logical_keys: u64,
    bytes: u64,
    phase: &'static str,
    scheduled_ns: u64,
    dispatch_timestamp: Arc<AtomicU64>,
}

struct WorkerPool {
    senders: Vec<std_mpsc::SyncSender<WorkerRequest>>,
    next_worker: usize,
    idle_workers: Vec<usize>,
    closed_loop: bool,
    completions: mpsc::Receiver<WorkerCompletion>,
    workers: Vec<JoinHandle<()>>,
    pending: std::collections::HashMap<u64, PendingOperation>,
    next_id: u64,
    origin: Instant,
    value_size: usize,
}

impl WorkerPool {
    fn new<B: KvBackend>(
        backend: &Arc<B>,
        config: &RunConfig,
        namespace: &[u8; 16],
        origin: Instant,
        require_present_reads: bool,
    ) -> BackendResult<Self> {
        let (completion_tx, completions) = mpsc::channel(config.execution.queue_capacity.max(1));
        let closed_loop = config.execution.mode == ExecutionMode::ClosedLoop;
        let mut idle_workers = Vec::new();
        if closed_loop {
            idle_workers.reserve(config.execution.concurrency);
            idle_workers.extend((0..config.execution.concurrency).rev());
        }
        let mut senders = Vec::with_capacity(config.execution.concurrency);
        let mut workers = Vec::with_capacity(config.execution.concurrency);
        let per_worker_capacity = config
            .execution
            .queue_capacity
            .div_ceil(config.execution.concurrency)
            .max(1);
        let config = Arc::new(config.clone());
        let namespace = *namespace;
        let handle = tokio::runtime::Handle::current();
        for worker_index in 0..config.execution.concurrency {
            let (sender, receiver) = std_mpsc::sync_channel::<WorkerRequest>(per_worker_capacity);
            let backend = Arc::clone(backend);
            let config = Arc::clone(&config);
            let tx = completion_tx.clone();
            let handle = handle.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("kv-research-worker-{worker_index}"))
                    .spawn(move || {
                        while let Ok(request) = receiver.recv() {
                            let WorkerRequest {
                                id,
                                work,
                                phase,
                                profiled_write,
                                scheduled_ns,
                                origin,
                                global_deadline,
                                dispatch_timestamp,
                            } = request;
                            let sample = handle.block_on(execute_item(
                                backend.as_ref(),
                                &config,
                                &namespace,
                                work,
                                profiled_write,
                                phase,
                                scheduled_ns,
                                origin,
                                global_deadline,
                                require_present_reads,
                                dispatch_timestamp,
                            ));
                            if tx
                                .blocking_send(WorkerCompletion {
                                    id,
                                    worker_index,
                                    sample,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    })?,
            );
            senders.push(sender);
        }
        drop(completion_tx);
        Ok(Self {
            senders,
            next_worker: 0,
            idle_workers,
            closed_loop,
            completions,
            workers,
            pending: std::collections::HashMap::default(),
            next_id: 0,
            origin,
            value_size: config.dataset.value_size,
        })
    }

    fn try_dispatch(
        &mut self,
        work: WorkItem,
        phase: &'static str,
        scheduled_ns: u64,
        deadline: Instant,
        profiled_write: bool,
    ) -> Result<(), WorkItem> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let dispatch_timestamp = Arc::new(AtomicU64::new(0));
        let operation = work.operation;
        let logical_keys = work.keys.len() as u64;
        let bytes = if matches!(operation, OperationKind::Put | OperationKind::PutBatch) {
            logical_keys.saturating_mul(self.value_size as u64)
        } else {
            0
        };
        let mut request = WorkerRequest {
            id,
            work,
            phase,
            profiled_write,
            scheduled_ns,
            origin: self.origin,
            global_deadline: deadline,
            dispatch_timestamp: Arc::clone(&dispatch_timestamp),
        };
        'dispatch: {
            if self.closed_loop {
                let Some(index) = self.idle_workers.pop() else {
                    return Err(request.work);
                };
                match self.senders[index].try_send(request) {
                    Ok(()) => break 'dispatch,
                    Err(std_mpsc::TrySendError::Full(returned)) => {
                        self.idle_workers.push(index);
                        return Err(returned.work);
                    }
                    Err(std_mpsc::TrySendError::Disconnected(returned)) => {
                        return Err(returned.work);
                    }
                }
            }
            for offset in 0..self.senders.len() {
                let index = (self.next_worker + offset) % self.senders.len();
                match self.senders[index].try_send(request) {
                    Ok(()) => {
                        self.next_worker = (index + 1) % self.senders.len();
                        break 'dispatch;
                    }
                    Err(std_mpsc::TrySendError::Full(returned)) => request = returned,
                    Err(std_mpsc::TrySendError::Disconnected(returned)) => {
                        return Err(returned.work);
                    }
                }
            }
            return Err(request.work);
        }
        self.pending.insert(
            id,
            PendingOperation {
                operation,
                logical_keys,
                bytes,
                phase,
                scheduled_ns,
                dispatch_timestamp,
            },
        );
        Ok(())
    }

    fn record_completion(
        &mut self,
        completion: WorkerCompletion,
        reporter: &mut Reporter,
    ) -> BackendResult<()> {
        self.pending.remove(&completion.id);
        if let Err(error) = reporter.record(completion.sample) {
            let _ = self.record_unknown_pending(reporter);
            self.shutdown(false);
            return Err(error);
        }
        if self.closed_loop {
            self.idle_workers.push(completion.worker_index);
        }
        Ok(())
    }

    async fn receive(&mut self) -> BackendResult<WorkerCompletion> {
        self.completions
            .recv()
            .await
            .ok_or_else(|| invalid("worker completion channel closed with requests outstanding"))
    }

    fn record_unknown_pending(&mut self, reporter: &mut Reporter) -> BackendResult<()> {
        let now = duration_ns(self.origin.elapsed());
        let mut first_error = None;
        for (_, pending) in self.pending.drain() {
            let operation = pending.operation;
            let dispatch_marker = pending.dispatch_timestamp.load(Ordering::Acquire);
            let dispatch_observed = dispatch_marker > 0;
            let dispatched = dispatch_marker.saturating_sub(1);
            let outcome = if dispatch_observed {
                timeout_outcome(operation)
            } else {
                "timeout"
            };
            let sample = Sample {
                phase: pending.phase,
                operation: operation.as_str(),
                outcome,
                scheduled_ns: pending.scheduled_ns,
                dispatched_ns: dispatched,
                dispatch_observed,
                completed_ns: now,
                api_ns: if dispatch_observed {
                    now.saturating_sub(dispatched)
                } else {
                    0
                },
                response_ns: now.saturating_sub(pending.scheduled_ns),
                scheduling_ns: if dispatch_observed {
                    dispatched.saturating_sub(pending.scheduled_ns)
                } else {
                    0
                },
                logical_keys: pending.logical_keys,
                bytes: pending.bytes,
                error_kind: Cow::Borrowed("external_shutdown_deadline_unknown_outcome"),
            };
            if let Err(error) = reporter.record(sample)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn shutdown(&mut self, join_workers: bool) {
        self.senders.clear();
        if join_workers {
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        } else {
            self.workers.clear();
        }
    }
}

async fn wait_for_measurement_gate(
    gate: &crate::config::MeasurementGateConfig,
    global_deadline: Instant,
) -> BackendResult<()> {
    let invalid =
        |message: &str| std::io::Error::new(std::io::ErrorKind::InvalidInput, message.to_owned());
    let normalize_marker = |marker: &Path| -> BackendResult<std::path::PathBuf> {
        let parent = marker
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let file_name = marker
            .file_name()
            .ok_or_else(|| invalid("measurement gate marker path must name a file"))?;
        if !parent.is_dir() {
            return Err(
                invalid("measurement gate marker parent directory must already exist").into(),
            );
        }
        Ok(std::fs::canonicalize(parent)?.join(file_name))
    };
    let ready_identity = normalize_marker(&gate.ready_file)?;
    let release_identity = normalize_marker(&gate.release_file)?;
    if ready_identity == release_identity {
        return Err(invalid("measurement gate marker paths must resolve to distinct files").into());
    }
    for marker in [&gate.ready_file, &gate.release_file] {
        if marker.try_exists()? {
            return Err(invalid("measurement gate markers must not already exist").into());
        }
    }
    if Instant::now() >= global_deadline {
        return Err(invalid("global deadline expired before measurement gate").into());
    }
    let mut ready = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&gate.ready_file)?;
    ready.write_all(b"ready\n")?;
    ready.sync_all()?;

    let gate_deadline =
        (Instant::now() + Duration::from_secs(gate.timeout_seconds)).min(global_deadline);
    loop {
        if Instant::now() >= gate_deadline {
            let message = if gate_deadline == global_deadline {
                "global deadline expired while waiting at measurement gate"
            } else {
                "measurement gate timed out waiting for release marker"
            };
            return Err(invalid(message).into());
        }
        if gate.release_file.try_exists()? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn export_read_diagnostics<B: KvBackend>(
    backend: &B,
    config: &RunConfig,
    output: &Path,
    measurement_attempted: bool,
    measurement_drained: bool,
    armed: bool,
    arm_error: Option<&str>,
    finish_deadline: Instant,
    read_evidence: &serde_json::Value,
) -> BackendResult<Option<String>> {
    let mut finish_error = arm_error.map(str::to_owned);
    let mut profile = if armed {
        match timeout_at(
            TokioInstant::from_std(finish_deadline),
            backend.finish_read_profile(),
        )
        .await
        {
            Ok(Ok(profile)) => Some(profile),
            Ok(Err(error)) => {
                finish_error = Some(error.to_string());
                None
            }
            Err(_) => {
                finish_error = Some("read profile finalization exceeded shutdown grace".to_owned());
                None
            }
        }
    } else {
        None
    };
    if finish_error.is_none() {
        finish_error = profile
            .as_ref()
            .and_then(|value| value.get("finalization_error"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
    }
    if armed && !measurement_drained {
        let incomplete = "measurement did not complete and drain successfully";
        finish_error = Some(match finish_error {
            Some(error) => format!("{error}; {incomplete}"),
            None => incomplete.to_owned(),
        });
    }
    let runtime_enabled = profile
        .as_ref()
        .and_then(|value| value.get("runtime_enabled"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if armed && profile.is_some() {
        let runtime_limit = profile
            .as_ref()
            .and_then(|value| value.get("runtime_limit"))
            .and_then(serde_json::Value::as_u64);
        if !runtime_enabled || runtime_limit != Some(config.diagnostics.read_profile_limit as u64) {
            let error = format!(
                "read profile activation mismatch: enabled={runtime_enabled}, observed limit={runtime_limit:?}, requested limit={}",
                config.diagnostics.read_profile_limit
            );
            finish_error = Some(match finish_error {
                Some(previous) => format!("{previous}; {error}"),
                None => error,
            });
        }
        let selected = profile
            .as_ref()
            .and_then(|value| value.get("selected_operations"))
            .and_then(serde_json::Value::as_u64);
        let rpcs = read_evidence
            .pointer("/deltas/coordinator/counters/read_rpc_count")
            .and_then(serde_json::Value::as_u64);
        if selected == Some(0) && rpcs.is_some_and(|count| count > 0) {
            let error = format!(
                "read profile selected no requests despite {} observed remote read RPCs",
                rpcs.unwrap_or(0)
            );
            finish_error = Some(match finish_error {
                Some(previous) => format!("{previous}; {error}"),
                None => error,
            });
        }
    }
    if let Some(object) = profile.as_mut().and_then(serde_json::Value::as_object_mut) {
        if object
            .get("selected_operations")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
            && let Some(hits) = read_evidence
                .pointer("/deltas/coordinator/counters/hits")
                .and_then(serde_json::Value::as_u64)
            && let Some(rpcs) = read_evidence
                .pointer("/deltas/coordinator/counters/read_rpc_count")
                .and_then(serde_json::Value::as_u64)
            && let Some(coverage) = object
                .get_mut("coverage")
                .and_then(serde_json::Value::as_object_mut)
        {
            coverage.insert("reason".to_owned(), serde_json::json!(format!(
                "no remote read requests selected; measurement observed {hits} coordinator cache hits and {rpcs} remote read RPCs"
            )));
        }
        let resolution_valid = armed
            && measurement_attempted
            && measurement_drained
            && runtime_enabled
            && object
                .get("runtime_limit")
                .and_then(serde_json::Value::as_u64)
                == Some(config.diagnostics.read_profile_limit as u64);
        object.insert(
            "coordinator_resolution_valid".to_owned(),
            resolution_valid.into(),
        );
        object.insert("read_evidence".to_owned(), read_evidence.clone());
    }
    let field = |name: &str| profile.as_ref().and_then(|value| value.get(name));
    let evidence = serde_json::json!({
        "schema_version": 1,
        "backend": backend.info().name,
        "phase": "measurement",
        "phase_attempted": measurement_attempted,
        "runtime_enabled": runtime_enabled,
        "runtime_observation_mode": field("runtime_observation_mode").and_then(serde_json::Value::as_str).unwrap_or("not_armed"),
        "selection": {
            "requested_limit": config.diagnostics.read_profile_limit,
            "selected_operations": field("selected_operations").and_then(serde_json::Value::as_u64).unwrap_or(0),
            "completed_operations": field("completed_operations").and_then(serde_json::Value::as_u64).unwrap_or(0),
            "selection_policy": field("selection_policy").and_then(serde_json::Value::as_str).unwrap_or("not armed"),
            "phase_exclusions": ["qualification", "preload", "warmup", "aging", "post-measurement qualification"]
        },
        "clock_scope": field("clock_scope").cloned().unwrap_or_else(|| serde_json::json!("no engine stages captured")),
        "overlap_semantics": field("overlap_semantics").cloned().unwrap_or_else(|| serde_json::json!("stage clocks overlap and are not additive")),
        "unsupported_stages": field("unsupported_stages").cloned().unwrap_or_else(|| serde_json::json!(["measurement_read_stage_profile"])),
        "profile": profile,
        "error": finish_error.clone()
    });
    std::fs::write(
        output.join("read-diagnostics.json"),
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    Ok(finish_error)
}

async fn export_write_diagnostics<B: KvBackend>(
    backend: &B,
    config: &RunConfig,
    output: &Path,
    measurement_attempted: bool,
    arm_error: Option<&str>,
    finish_deadline: Instant,
) -> BackendResult<Option<String>> {
    let mut finish_error = arm_error.map(str::to_owned);
    let profile = if finish_error.is_none() && measurement_attempted {
        match timeout_at(
            TokioInstant::from_std(finish_deadline),
            backend.finish_write_profile(),
        )
        .await
        {
            Ok(Ok(profile)) => Some(profile),
            Ok(Err(error)) => {
                finish_error = Some(error.to_string());
                None
            }
            Err(_) => {
                finish_error =
                    Some("write profile finalization exceeded shutdown grace".to_owned());
                None
            }
        }
    } else {
        None
    };
    if finish_error.is_none() {
        finish_error = profile
            .as_ref()
            .and_then(|profile| profile.get("finalization_error"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
    }
    let observed_samples = profile
        .as_ref()
        .and_then(|profile| {
            profile
                .get("sampled_requests")
                .or_else(|| profile.get("selected_operations"))
        })
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let runtime_mode = profile
        .as_ref()
        .and_then(|profile| profile.get("runtime_observation_mode"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("not_armed");
    let evidence = serde_json::json!({
        "schema_version": 1,
        "backend": backend.info().name,
        "phase": "measurement",
        "phase_attempted": measurement_attempted,
        "runtime_enabled": observed_samples > 0,
        "runtime_observation_mode": runtime_mode,
        "selection": {
            "requested_limit": config.diagnostics.write_profile_limit,
            "observed_samples": observed_samples,
            "selection_policy": profile.as_ref()
                .and_then(|value| value.get("selection_policy"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("first N accepted scalar put/delete requests in measurement; batch writes are not selected"),
            "phase_exclusions": ["qualification", "preload", "warmup", "aging", "post-measurement qualification"]
        },
        "clock_scope": profile.as_ref()
            .and_then(|value| value.get("clock_scope"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!("no engine stages captured")),
        "overlap_semantics": profile.as_ref()
            .and_then(|value| value.get("overlap_semantics"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!("stage sums are independent clocks and are not additive latency fractions")),
        "unsupported_stages": profile.as_ref()
            .and_then(|value| value.get("unsupported_stages"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!(["measurement_write_stage_profile"])),
        "profile": profile,
        "error": finish_error.clone()
    });
    std::fs::write(
        output.join("write-diagnostics.json"),
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    Ok(finish_error)
}
fn is_profile_candidate(
    phase: &str,
    operation: OperationKind,
    selected: usize,
    limit: usize,
) -> bool {
    phase == "measurement"
        && matches!(operation, OperationKind::Put | OperationKind::Delete)
        && selected < limit
}
// Keep phase state, independent clocks and reporter borrowing explicit at this private boundary.
#[allow(clippy::too_many_arguments)]
// Keep open/closed-loop admission beside their common completion/drain accounting.
#[allow(clippy::too_many_lines)]
// Phase counts are admitted below 100 million, exactly representable in f64.
#[allow(clippy::cast_precision_loss)]
// Match makes the received-event and disconnected-channel paths explicit in select branches.
#[allow(clippy::single_match_else)]
async fn run_phase<B: KvBackend>(
    backend: Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    generator: &mut WorkloadGenerator,
    phase: &'static str,
    count: u64,
    origin: Instant,
    deadline: Instant,
    require_present_reads: bool,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let mut pool = WorkerPool::new(&backend, config, namespace, origin, require_present_reads)?;
    let phase_start = Instant::now();
    let mut scheduled_index = 0_u64;
    let mut phase_expired = false;
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let mut profiled_selected = 0usize;
    let sweep_keys = (phase == "warmup" && config.phases.warmup_key_sweep)
        || (phase == "measurement" && config.phases.measurement_key_sweep);
    let mut sweep_cursor = 0;
    match config.execution.mode {
        ExecutionMode::ClosedLoop => {
            while scheduled_index < count {
                if Instant::now() >= deadline {
                    phase_expired = true;
                    break;
                }
                if pool.pending.len() >= config.execution.concurrency {
                    tokio::select! {
                        () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                            reporter.snapshot()?;
                            next_snapshot = Instant::now() + Duration::from_secs(1);
                        }
                        completion = pool.completions.recv() => {
                            match completion {
                                Some(completion) => {
                                    if let Err(error) = pool.record_completion(completion, reporter) {
                                        let _ = pool.record_unknown_pending(reporter);
                                        pool.shutdown(false);
                                        return Err(error);
                                    }
                                }
                                None => {
                                    let _ = pool.record_unknown_pending(reporter);
                                    pool.shutdown(false);
                                    return Err(invalid("worker completion channel closed"));
                                }
                            }
                        }
                    }
                    continue;
                }
                let work = if sweep_keys {
                    generator.next_sweep_item(&mut sweep_cursor)?
                } else {
                    generator.next_item()?
                };
                let scheduled = duration_ns(origin.elapsed());
                reporter.offered(phase, work.operation.as_str(), work.keys.len() as u64);
                if Instant::now() >= next_snapshot {
                    reporter.snapshot()?;
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                }
                let profiled_write = is_profile_candidate(
                    phase,
                    work.operation,
                    profiled_selected,
                    config.diagnostics.write_profile_limit,
                );
                match pool.try_dispatch(work, phase, scheduled, deadline, profiled_write) {
                    Ok(()) => {
                        if profiled_write {
                            profiled_selected += 1;
                        }
                    }
                    Err(work) => reporter.rejected(rejected_sample(
                        phase,
                        &work,
                        scheduled,
                        origin,
                        "worker_queue_full",
                    ))?,
                }
                scheduled_index += 1;
            }
        }
        ExecutionMode::OpenLoop => {
            let rate = config
                .execution
                .offered_ops_per_second
                .ok_or_else(|| invalid("open-loop rate missing"))?;
            while scheduled_index < count {
                if Instant::now() >= deadline {
                    phase_expired = true;
                    break;
                }
                let due = phase_start + Duration::from_secs_f64(scheduled_index as f64 / rate);
                if due >= deadline {
                    phase_expired = true;
                    break;
                }
                tokio::select! {
                    () = sleep_until(TokioInstant::from_std(due)) => {},
                    () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                        reporter.snapshot()?;
                        next_snapshot = Instant::now() + Duration::from_secs(1);
                        continue;
                    }
                    completion = pool.completions.recv(), if !pool.pending.is_empty() => {
                        match completion {
                            Some(completion) => {
                                if let Err(error) = pool.record_completion(completion, reporter) {
                                    let _ = pool.record_unknown_pending(reporter);
                                    pool.shutdown(false);
                                    return Err(error);
                                }
                            }
                            None => {
                                let _ = pool.record_unknown_pending(reporter);
                                pool.shutdown(false);
                                return Err(invalid("worker completion channel closed"));
                            }
                        }
                        // Completion is not a new arrival; wait again for this same due time.
                        continue;
                    }
                }
                let work = if sweep_keys {
                    generator.next_sweep_item(&mut sweep_cursor)?
                } else {
                    generator.next_item()?
                };
                let scheduled = duration_ns(due.saturating_duration_since(origin));
                reporter.offered(phase, work.operation.as_str(), work.keys.len() as u64);
                let profiled_write = is_profile_candidate(
                    phase,
                    work.operation,
                    profiled_selected,
                    config.diagnostics.write_profile_limit,
                );
                let dispatched = if pool.pending.len() >= config.execution.queue_capacity {
                    Err(work)
                } else {
                    pool.try_dispatch(work, phase, scheduled, deadline, profiled_write)
                };
                match dispatched {
                    Ok(()) => {
                        if profiled_write {
                            profiled_selected += 1;
                        }
                    }
                    Err(work) => reporter.rejected(rejected_sample(
                        phase,
                        &work,
                        scheduled,
                        origin,
                        "admission_capacity",
                    ))?,
                }
                if Instant::now() >= next_snapshot {
                    reporter.snapshot()?;
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                }
                scheduled_index += 1;
            }
        }
    }
    let drain_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
    reporter.set_phase("drain");
    while !pool.pending.is_empty() {
        tokio::select! {
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            result = timeout_at(TokioInstant::from_std(drain_deadline), pool.receive()) => {
                match result {
                    Ok(Ok(completion)) => pool.record_completion(completion, reporter)?,
                    Ok(Err(error)) => {
                        let _ = pool.record_unknown_pending(reporter);
                        pool.shutdown(false);
                        return Err(error);
                    }
                    Err(_) => {
                        let _ = pool.record_unknown_pending(reporter);
                        pool.shutdown(false);
                        return Err(invalid("bounded worker shutdown grace expired; unresolved mutations retained as unknown outcomes"));
                    }
                }
            }
        }
    }
    pool.shutdown(true);
    if phase_expired {
        return Err(invalid(
            "global deadline expired before all phase arrivals were scheduled",
        ));
    }
    Ok(())
}

fn rejected_sample(
    phase: &'static str,
    work: &WorkItem,
    scheduled: u64,
    origin: Instant,
    reason: &'static str,
) -> Sample {
    let completed = duration_ns(origin.elapsed());
    Sample {
        phase,
        operation: work.operation.as_str(),
        outcome: "rejected",
        scheduled_ns: scheduled,
        dispatched_ns: 0,
        dispatch_observed: false,
        completed_ns: completed,
        api_ns: 0,
        response_ns: completed.saturating_sub(scheduled),
        scheduling_ns: 0,
        logical_keys: work.keys.len() as u64,
        bytes: 0,
        error_kind: Cow::Borrowed(reason),
    }
}

// Dispatch publication, API timing and the oracle need distinct inputs across the await boundary.
#[allow(clippy::too_many_arguments)]
// Keep one matched API boundary and its post-clock oracle for all operation kinds.
#[allow(clippy::too_many_lines)]
// This response type belongs only to the local API/oracle boundary, not the adapter interface.
#[allow(clippy::items_after_statements)]
async fn execute_item<B: KvBackend>(
    backend: &B,
    config: &RunConfig,
    namespace: &[u8; 16],
    work: WorkItem,
    profiled_write: bool,
    phase: &'static str,
    scheduled: u64,
    origin: Instant,
    deadline: Instant,
    require_present_reads: bool,
    dispatch_timestamp: Arc<AtomicU64>,
) -> Sample {
    let operation = work.operation;
    let op = operation.as_str();
    let key_bytes_vec = work
        .keys
        .iter()
        .map(|id| key_bytes(namespace, config.seed, *id))
        .collect::<Vec<_>>();
    let entries = if matches!(
        operation,
        OperationKind::Read | OperationKind::Put | OperationKind::GetMany | OperationKind::PutBatch
    ) {
        work.keys
            .iter()
            .zip(&key_bytes_vec)
            .map(|(id, key)| {
                (
                    key.clone(),
                    value_bytes(
                        namespace,
                        config.seed,
                        *id,
                        config.dataset.value_size,
                        config.dataset.entropy,
                    ),
                )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let key_count = work.keys.len() as u64;
    let bytes = match operation {
        OperationKind::Put | OperationKind::PutBatch => {
            (config.dataset.value_size * work.keys.len()) as u64
        }
        _ => 0,
    };
    let request_deadline = deadline.min(
        origin
            + Duration::from_nanos(scheduled)
                .saturating_add(Duration::from_millis(config.execution.request_timeout_ms)),
    );
    if Instant::now() >= request_deadline {
        return make_sample(
            phase,
            op,
            "timeout",
            scheduled,
            0,
            false,
            origin,
            0,
            key_count,
            bytes,
            Cow::Borrowed("request_timeout_before_backend_dispatch"),
        );
    }
    let api_start = Instant::now();
    let dispatched = duration_ns(api_start.duration_since(origin));
    dispatch_timestamp.store(dispatched.saturating_add(1), Ordering::Release);
    enum BackendResponse {
        Read(Option<Bytes>),
        Many(Vec<Option<Bytes>>),
        Mutation(u64),
    }
    let result = timeout_at(TokioInstant::from_std(request_deadline), async {
        match operation {
            OperationKind::Read | OperationKind::MissingRead => backend
                .get(key_bytes_vec[0].as_ref())
                .await
                .map(BackendResponse::Read),
            OperationKind::Put if profiled_write => backend
                .profiled_put(key_bytes_vec[0].clone(), entries[0].1.clone())
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            OperationKind::Put => backend
                .put(key_bytes_vec[0].clone(), entries[0].1.clone())
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            OperationKind::Delete if profiled_write => backend
                .profiled_delete(key_bytes_vec[0].clone())
                .await
                .map(|()| BackendResponse::Mutation(0)),
            OperationKind::Delete => backend
                .delete(key_bytes_vec[0].clone())
                .await
                .map(|()| BackendResponse::Mutation(0)),
            OperationKind::GetMany => backend
                .get_many(&key_bytes_vec)
                .await
                .map(BackendResponse::Many),
            OperationKind::PutBatch => backend
                .put_batch(&entries)
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
        }
    })
    .await;
    let api_end = Instant::now();
    let completed_ns = duration_ns(api_end.duration_since(origin));
    let api_ns = completed_ns.saturating_sub(dispatched);
    let timeout_ns = config
        .execution
        .request_timeout_ms
        .saturating_mul(1_000_000);
    match result {
        Ok(Ok(response)) => {
            let (returned_bytes, outcome, error_kind) = match response {
                BackendResponse::Read(value) => {
                    let absent = value.is_none();
                    let valid = operation == OperationKind::MissingRead
                        || match value.as_ref() {
                            Some(actual) => actual.as_ref() == entries[0].1.as_ref(),
                            None => !require_present_reads,
                        };
                    let returned_bytes = value.as_ref().map_or(0, |v| v.len() as u64);
                    let outcome = if operation == OperationKind::MissingRead {
                        if absent {
                            "success"
                        } else {
                            "unexpected_present"
                        }
                    } else if valid {
                        "success"
                    } else {
                        "wrong_value"
                    };
                    let error = if outcome == "success" {
                        String::new()
                    } else if operation == OperationKind::MissingRead {
                        "missing-key read returned a value".to_owned()
                    } else if value.is_none() && require_present_reads {
                        "read returned no value for a preloaded key".to_owned()
                    } else {
                        "read bytes differ from deterministic payload".to_owned()
                    };
                    (returned_bytes, outcome, error)
                }
                BackendResponse::Many(values) => {
                    let valid_count = values.len() == work.keys.len();
                    let valid_values = values.iter().zip(&entries).all(|(actual, expected)| {
                        match actual.as_ref() {
                            Some(value) => value.as_ref() == expected.1.as_ref(),
                            None => !require_present_reads,
                        }
                    });
                    let valid = valid_count && valid_values;
                    (
                        values
                            .iter()
                            .flatten()
                            .map(|value| value.len() as u64)
                            .sum(),
                        if valid { "success" } else { "wrong_value" },
                        if valid {
                            String::new()
                        } else {
                            "get_many result count, presence, or ordered value bytes differ"
                                .to_owned()
                        },
                    )
                }
                BackendResponse::Mutation(returned_bytes) => {
                    (returned_bytes, "success", String::new())
                }
            };
            let note = if api_ns > timeout_ns {
                format!("{error_kind}; completed_after_request_timeout")
            } else {
                error_kind
            };
            make_sample_at(
                phase,
                op,
                outcome,
                scheduled,
                dispatched,
                true,
                completed_ns,
                api_ns,
                key_count,
                returned_bytes.max(bytes),
                Cow::Owned(note),
            )
        }
        Ok(Err(error)) => {
            let timed_out = is_timed_out(error.as_ref());
            let transport_failure = crate::backend::is_backend_transport_error(error.as_ref());
            let is_mutation = matches!(
                operation,
                OperationKind::Put | OperationKind::Delete | OperationKind::PutBatch
            );
            let outcome = if is_mutation && (timed_out || transport_failure) {
                "unknown_mutation"
            } else if timed_out {
                "timeout"
            } else {
                "error"
            };
            let note = if timed_out {
                format!("backend_io_timeout: {error}")
            } else if is_mutation && transport_failure {
                format!("backend_transport_failure; mutation outcome unknown: {error}")
            } else if api_ns > timeout_ns {
                format!("{error}; completed_after_request_timeout")
            } else {
                error.to_string()
            };
            make_sample_at(
                phase,
                op,
                outcome,
                scheduled,
                dispatched,
                true,
                completed_ns,
                api_ns,
                key_count,
                bytes,
                Cow::Owned(note),
            )
        }
        Err(_) => make_sample_at(
            phase,
            op,
            timeout_outcome(operation),
            scheduled,
            dispatched,
            true,
            completed_ns,
            api_ns,
            key_count,
            bytes,
            Cow::Borrowed("request_deadline_timeout"),
        ),
    }
}

fn timeout_outcome(operation: OperationKind) -> &'static str {
    if matches!(
        operation,
        OperationKind::Put | OperationKind::Delete | OperationKind::PutBatch
    ) {
        "unknown_mutation"
    } else {
        "timeout"
    }
}

fn is_timed_out(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
        {
            return true;
        }
        current = source.source();
    }
    false
}

// Raw event fields stay explicit so this helper owns the single completion-clock observation.
#[allow(clippy::too_many_arguments)]
fn make_sample(
    phase: &'static str,
    operation: &'static str,
    outcome: &'static str,
    scheduled: u64,
    dispatched: u64,
    dispatch_observed: bool,
    origin: Instant,
    api_ns: u64,
    logical_keys: u64,
    bytes: u64,
    error_kind: Cow<'static, str>,
) -> Sample {
    let completed_ns = duration_ns(origin.elapsed());
    make_sample_at(
        phase,
        operation,
        outcome,
        scheduled,
        dispatched,
        dispatch_observed,
        completed_ns,
        api_ns,
        logical_keys,
        bytes,
        error_kind,
    )
}

// Centralize clock decomposition for the complete raw event, including undispatched outcomes.
#[allow(clippy::too_many_arguments)]
fn make_sample_at(
    phase: &'static str,
    operation: &'static str,
    outcome: &'static str,
    scheduled: u64,
    dispatched: u64,
    dispatch_observed: bool,
    completed_ns: u64,
    api_ns: u64,
    logical_keys: u64,
    bytes: u64,
    error_kind: Cow<'static, str>,
) -> Sample {
    let error_kind = if error_kind.len() <= 256 {
        error_kind
    } else {
        Cow::Owned(error_kind.chars().take(256).collect())
    };
    Sample {
        phase,
        operation,
        outcome,
        scheduled_ns: scheduled,
        dispatched_ns: dispatched,
        dispatch_observed,
        completed_ns,
        api_ns,
        response_ns: completed_ns.saturating_sub(scheduled),
        scheduling_ns: if dispatch_observed {
            dispatched.saturating_sub(scheduled)
        } else {
            0
        },
        logical_keys,
        bytes,
        error_kind,
    }
}

fn derive_namespace(namespace: &[u8; 16], purpose: &[u8]) -> [u8; 16] {
    let mut hasher = Blake2b512::new();
    hasher.update(namespace);
    hasher.update(purpose);
    let digest = hasher.finalize();
    std::array::from_fn(|index| digest[index])
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}
fn hex_digest(bytes: &[u8]) -> String {
    let digest = Blake2b512::digest(bytes);
    hex(&digest)
}

// One reusable 64 KiB setup buffer fits the non-recursive 2 MiB worker stack; avoid a heap copy.
#[allow(clippy::large_stack_arrays)]
fn binary_identity() -> serde_json::Value {
    let result = std::env::current_exe()
        .and_then(|path| std::fs::File::open(&path).map(|file| (path, file)));
    match result {
        Ok((path, mut file)) => {
            let mut hasher = Blake2b512::new();
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => {
                        return serde_json::json!({"path": path, "blake2b512": hex(&hasher.finalize())});
                    }
                    Ok(read) => hasher.update(&buffer[..read]),
                    Err(error) => {
                        return serde_json::json!({"path": path, "error": error.to_string()});
                    }
                }
            }
        }
        Err(error) => serde_json::json!({"error": error.to_string()}),
    }
}
fn unix_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}
fn hardware_identity() -> serde_json::Value {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("model name"))
                .map(str::to_owned)
        });
    let memory = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("MemTotal:"))
                .map(str::to_owned)
        });
    let hostname = std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|value| value.trim().to_owned());
    serde_json::json!({"os": std::env::consts::OS, "architecture": std::env::consts::ARCH, "cpu": cpu, "memory": memory, "hostname": hostname})
}
fn invalid(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_owned()).into()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BackendInfo;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    #[test]
    fn deltas_reject_resets_and_keep_partition_identity() {
        let start = serde_json::json!({"available": true, "partitions": {"3": {"reads": 10}, "8": {"reads": 2}}});
        let end = serde_json::json!({"available": true, "partitions": {"3": {"reads": 12}, "9": {"reads": 4}}});
        let delta = partition_deltas(&start, &end, &["reads"]);
        assert_eq!(delta["partitions"]["3"]["counters"]["reads"], 2);
        assert_eq!(delta["partitions"]["8"]["available"], false);
        assert_eq!(delta["partitions"]["9"]["available"], false);
        assert_eq!(
            counter_delta(
                &serde_json::json!({"reads": 4}),
                &serde_json::json!({"reads": 1}),
                &["reads"]
            )["available"],
            false
        );
    }

    #[test]
    fn lane_deltas_preserve_missing_and_reset_lanes() {
        let start = serde_json::json!({"available": true, "partitions": {"0": {"lanes": {
            "0": {"session_present": true, "stats": {"hits": 3}},
            "1": {"session_present": null, "stats": {"hits": 5}},
            "2": {"session_present": true, "stats": {"hits": 1}}
        }}}});
        let end = serde_json::json!({"available": true, "partitions": {"0": {"lanes": {
            "0": {"session_present": true, "stats": {"hits": 7}},
            "1": {"session_present": null, "stats": {"hits": 1}}
        }}}});
        let delta = lane_deltas(&start, &end, &["hits"]);
        assert_eq!(
            delta["partitions"]["0"]["lanes"]["0"]["counters"]["hits"],
            4
        );
        assert_eq!(delta["partitions"]["0"]["lanes"]["1"]["available"], false);
        assert_eq!(delta["partitions"]["0"]["lanes"]["2"]["available"], false);
    }

    #[test]
    fn owner_boundary_stages_keep_errors_missing_and_lifetime_max_separate() {
        let start = serde_json::json!({"available": true, "partitions": {"0": {
            "read_grant": {"registration": {"ns": 10, "count": 1, "errors": 0, "max_ns": 10}}
        }}});
        let end = serde_json::json!({"available": true, "partitions": {"0": {
            "read_grant": {"registration": {"ns": 30, "count": 2, "errors": 1, "max_ns": 20}}
        }}});
        let delta = owner_response_deltas(&start, &end);
        let registration = &delta["partitions"]["0"]["read_grant_registration"];
        assert_eq!(registration["counters"]["ns"], 20);
        assert_eq!(registration["counters"]["count"], 1);
        assert_eq!(registration["counters"]["errors"], 1);
        assert!(registration["counters"].get("max_ns").is_none());
        assert_eq!(
            delta["partitions"]["0"]["read_response_encode"]["available"],
            false
        );
        assert_eq!(
            owner_response_deltas(&start, &serde_json::json!({"available": false}))["available"],
            false
        );
    }

    #[tokio::test]
    async fn read_evidence_excludes_warmup_and_postqualification() {
        let directory = result_directory("read-boundary");
        let output = directory.join("result");
        let mut config = read_test_config();
        config.phases.preload = true;
        config.phases.warmup_operations = 3;
        config.phases.measurement_operations = 2;
        run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap();
        let evidence: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(evidence["deltas"]["coordinator"]["counters"]["hits"], 2);
        assert!(evidence["start"]["coordinator"]["hits"].as_u64().unwrap() >= 3);
        assert!(!output.join("read-diagnostics.json").exists());
        assert_eq!(evidence["deltas"]["owner_executor"]["available"], false);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn read_profile_is_confined_to_measurement_and_exports_empty_coverage() {
        let directory = result_directory("read-profile-boundary");
        let output = directory.join("result");
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut backend = MemoryBackend::new(false, false);
        backend.profile_events = Some(Arc::clone(&events));
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".to_owned();
        config.phases.warmup_operations = 3;
        config.phases.measurement_operations = 2;
        config.diagnostics.read_profile_limit = 4;
        run(backend, config, &output, prometheus::Registry::new())
            .await
            .unwrap();
        let events = events.lock().unwrap();
        let arm = events.iter().position(|event| *event == "arm").unwrap();
        let finish = events.iter().position(|event| *event == "finish").unwrap();
        assert!(events[..arm].contains(&"read"));
        assert_eq!(
            events[arm + 1..finish]
                .iter()
                .filter(|event| **event == "read")
                .count(),
            2
        );
        assert!(events[finish + 1..].contains(&"read"));
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("read-diagnostics.json")).unwrap())
                .unwrap();
        assert_eq!(evidence["runtime_enabled"], true);
        assert_eq!(evidence["selection"]["requested_limit"], 4);
        assert_eq!(evidence["selection"]["selected_operations"], 0);
        assert_eq!(
            evidence["profile"]["owners"]["7"]["stages"]["server_async"]["available"],
            false
        );
        assert_eq!(
            evidence["profile"]["owners"]["7"]["counters"]["read_profile_server_async_count"],
            0
        );
        assert!(evidence["error"].is_null());
        assert_eq!(
            evidence["profile"]["read_evidence"]["measurement_attempted"],
            true
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn read_profile_exports_arm_failure_and_incomplete_measurement() {
        let directory = result_directory("read-profile-failure");
        std::fs::create_dir_all(&directory).unwrap();
        let mut config = read_test_config();
        config.diagnostics.read_profile_limit = 1;
        let mut backend = MemoryBackend::new(false, false);
        let read_evidence = serde_json::json!({});
        let deadline = Instant::now() + Duration::from_secs(10);
        let error = export_read_diagnostics(
            &backend,
            &config,
            &directory,
            false,
            false,
            false,
            Some("arm failed"),
            deadline,
            &read_evidence,
        )
        .await
        .unwrap();
        assert_eq!(error.as_deref(), Some("arm failed"));
        let artifact: serde_json::Value = serde_json::from_slice(
            &std::fs::read(directory.join("read-diagnostics.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(artifact["runtime_enabled"], false);
        assert!(artifact["profile"].is_null());
        let events = Arc::new(Mutex::new(Vec::new()));
        backend.profile_events = Some(Arc::clone(&events));
        backend.arm_read_profile(1).unwrap();
        let error = export_read_diagnostics(
            &backend,
            &config,
            &directory,
            true,
            false,
            true,
            None,
            deadline,
            &read_evidence,
        )
        .await
        .unwrap();
        assert!(error.unwrap().contains("did not complete and drain"));
        assert_eq!(events.lock().unwrap().as_slice(), ["arm", "finish"]);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn read_profile_rejects_unobserved_activation_and_missing_rpc_samples() {
        let directory = result_directory("read-profile-missing-coverage");
        let mut config = read_test_config();
        config.diagnostics.read_profile_limit = 2;
        let mut backend = MemoryBackend::new(false, false);
        backend.profile_events = Some(Arc::new(Mutex::new(Vec::new())));
        backend.arm_read_profile(1).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let evidence = serde_json::json!({"deltas": {"coordinator": {"counters": {
            "hits": 0, "read_rpc_count": 3
        }}}});
        let error = export_read_diagnostics(
            &backend, &config, &directory, true, true, true, None, deadline, &evidence,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(error.contains("activation mismatch"));
        assert!(error.contains("selected no requests"));
        let artifact: serde_json::Value = serde_json::from_slice(
            &std::fs::read(directory.join("read-diagnostics.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(artifact["runtime_enabled"], true);
        assert_eq!(artifact["profile"]["runtime_limit"], 1);
        assert!(
            artifact["error"]
                .as_str()
                .unwrap()
                .contains("selected no requests")
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    struct MemoryBackend {
        data: Mutex<HashMap<Vec<u8>, Bytes>>,
        corrupt_reads: bool,
        reverse_groups: bool,
        omit_put_number: Option<usize>,
        put_count: AtomicUsize,
        read_count: AtomicUsize,
        profile_events: Option<Arc<Mutex<Vec<&'static str>>>>,
        profile_limit: AtomicUsize,
    }

    impl MemoryBackend {
        fn new(corrupt_reads: bool, reverse_groups: bool) -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
                corrupt_reads,
                reverse_groups,
                omit_put_number: None,
                put_count: AtomicUsize::new(0),
                read_count: AtomicUsize::new(0),
                profile_limit: AtomicUsize::new(0),
                profile_events: None,
            }
        }

        fn omitting_put(number: usize) -> Self {
            Self {
                omit_put_number: Some(number),
                ..Self::new(false, false)
            }
        }

        fn should_omit_put(&self) -> bool {
            self.put_count.fetch_add(1, Ordering::Relaxed)
                == self.omit_put_number.unwrap_or(usize::MAX)
        }

        fn actual_value(&self, value: Bytes) -> Bytes {
            if self.corrupt_reads {
                Bytes::from(vec![0xff; value.len()])
            } else {
                value
            }
        }
    }

    impl KvBackend for MemoryBackend {
        fn info(&self) -> BackendInfo {
            BackendInfo {
                name: "memory-test".to_owned(),
                server_version: "test".to_owned(),
                client_version: "test".to_owned(),
                acknowledgement: "test acknowledgement".to_owned(),
                consistency: "test sequential".to_owned(),
                replication_factor: 1,
                measurement_boundary: "in process".to_owned(),
                get_many_strategy: "ordered".to_owned(),
                batch_atomicity: BatchAtomicity::SerializableTransaction,
                topology: serde_json::json!({"owners": 1}),
                configuration: serde_json::json!({}),
            }
        }
        fn read_observation(&self) -> serde_json::Value {
            let reads = self.read_count.load(Ordering::Relaxed);
            serde_json::json!({"available": true, "coordinator": {
                "read_rpc_count": 0, "hits": reads, "misses": 0, "inserts": 0,
                "evictions": 0, "rejected_oversize": 0, "rejected_mismatch": 0,
                "bypass_disabled": 0, "bypass_admission": 0, "bypass_eviction_limit": 0,
                "admitted_bytes": 0, "served_bytes": 0},
                "owner_executor": {"available": false, "error": "test unavailable"},
                "client_resident": {"available": false, "error": "test unavailable"}})
        }
        fn arm_read_profile(&self, limit: usize) -> BackendResult<()> {
            let events = self
                .profile_events
                .as_ref()
                .ok_or_else(|| invalid("read profile unsupported"))?;
            assert!(limit > 0);
            self.profile_limit.store(limit, Ordering::Relaxed);
            events.lock().unwrap().push("arm");
            Ok(())
        }

        async fn finish_read_profile(&self) -> BackendResult<serde_json::Value> {
            let events = self
                .profile_events
                .as_ref()
                .ok_or_else(|| invalid("read profile unsupported"))?;
            events.lock().unwrap().push("finish");
            Ok(serde_json::json!({
                "runtime_enabled": true,
                "runtime_limit": self.profile_limit.swap(0, Ordering::Relaxed),
                "runtime_observation_mode": "test",
                "selected_operations": 0,
                "completed_operations": 0,
                "stages": {},
                "owners": {"7": {
                    "stages": {"server_async": {"available": false, "sum_ns": null, "count": 0, "max_ns": null, "unavailable_reason": "no profiled owner responses for this partition"}},
                    "counters": {"read_profile_server_async_count": 0}
                }},
                "coverage": {"reason": "no remote reads in test backend"}
            }))
        }

        async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
            if let Some(events) = &self.profile_events {
                events.lock().unwrap().push("read");
            }
            self.read_count.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?
                .get(key)
                .cloned()
                .map(|value| self.actual_value(value)))
        }

        async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
            if !self.should_omit_put() {
                self.data
                    .lock()
                    .map_err(|_| invalid("poisoned test store"))?
                    .insert(key.as_ref().to_vec(), value);
            }
            Ok(())
        }

        async fn delete(&self, key: Bytes) -> BackendResult<()> {
            self.data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?
                .remove(key.as_ref());
            Ok(())
        }

        async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
            self.read_count.fetch_add(1, Ordering::Relaxed);
            let data = self
                .data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?;
            let mut values = keys
                .iter()
                .map(|key| {
                    data.get(key.as_ref())
                        .cloned()
                        .map(|value| self.actual_value(value))
                })
                .collect::<Vec<_>>();
            if self.reverse_groups {
                values.reverse();
            }
            Ok(values)
        }

        async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
            let mut data = self
                .data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?;
            for (key, value) in entries {
                if !self.should_omit_put() {
                    data.insert(key.as_ref().to_vec(), value.clone());
                }
            }
            Ok(())
        }
    }
    struct GatedBackend {
        calls: AtomicUsize,
        slow_started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        slow_release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }

    #[test]
    fn unsupported_peer_observation_is_explicitly_unavailable() {
        let peer = GatedBackend {
            calls: AtomicUsize::new(0),
            slow_started: Mutex::new(None),
            slow_release: Mutex::new(None),
        };
        let snapshot = peer.read_observation();
        let evidence = measurement_read_evidence(&snapshot, &snapshot, true, true);
        assert_eq!(evidence["deltas"]["available"], false);
        assert!(snapshot["error"].as_str().is_some());
    }

    impl KvBackend for GatedBackend {
        fn info(&self) -> BackendInfo {
            MemoryBackend::new(false, false).info()
        }

        async fn get(&self, _key: &[u8]) -> BackendResult<Option<Bytes>> {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                if let Some(started) = self.slow_started.lock().unwrap().take() {
                    let _ = started.send(());
                }
                let release = { self.slow_release.lock().unwrap().take().unwrap() };
                let _ = release.await;
            }
            Ok(Some(Bytes::from_static(b"value")))
        }

        async fn put(&self, _key: Bytes, _value: Bytes) -> BackendResult<()> {
            Ok(())
        }

        async fn delete(&self, _key: Bytes) -> BackendResult<()> {
            Ok(())
        }

        async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
            Ok(vec![None; keys.len()])
        }

        async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
            Ok(())
        }
    }

    fn read_test_config() -> RunConfig {
        let mut config: RunConfig = serde_json::from_str(include_str!("../example.json")).unwrap();
        config.dataset.keys = 1;
        config.dataset.value_size = 256;
        config.workload.batch_size = 2;
        config.workload.mix = crate::config::OperationMix {
            read: 1,
            ..crate::config::OperationMix::default()
        };
        config.phases.warmup_operations = 0;
        config.phases.aging_operations = 0;
        config.phases.measurement_operations = 1;
        config.execution.request_timeout_ms = 1000;
        config.limits.deadline_seconds = 30;
        config
    }

    fn measurement_outcomes(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path.join("samples.csv"))
            .unwrap()
            .lines()
            .filter(|line| line.starts_with("measurement,"))
            .map(|line| line.split(',').nth(2).unwrap().to_owned())
            .collect()
    }

    fn result_directory(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "kv-research-{name}-{}-{}",
            std::process::id(),
            hex(&fresh_namespace().unwrap())
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_worker_starts_replacement_while_first_request_is_gated() {
        let directory = result_directory("idle-worker-progress");
        let output = directory.join("result");
        let mut config: RunConfig = serde_json::from_str(include_str!("../example.json")).unwrap();
        config.execution.concurrency = 2;
        config.execution.queue_capacity = 2;
        config.execution.request_timeout_ms = 30_000;
        let (slow_started_tx, slow_started_rx) = tokio::sync::oneshot::channel();
        let (slow_release_tx, slow_release_rx) = tokio::sync::oneshot::channel();
        let backend = Arc::new(GatedBackend {
            calls: AtomicUsize::new(0),
            slow_started: Mutex::new(Some(slow_started_tx)),
            slow_release: Mutex::new(Some(slow_release_rx)),
        });
        let mut reporter = Reporter::create(
            &output,
            prometheus::Registry::new(),
            "gated-test".to_owned(),
        )
        .unwrap();
        let origin = Instant::now();
        let deadline = origin + Duration::from_secs(30);
        let mut pool = WorkerPool::new(&backend, &config, &[0; 16], origin, false).unwrap();
        let read = |id| WorkItem {
            operation: OperationKind::Read,
            keys: vec![id],
        };

        pool.try_dispatch(read(0), "measurement", 0, deadline, false)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), slow_started_rx)
            .await
            .unwrap()
            .unwrap();
        pool.try_dispatch(read(1), "measurement", 0, deadline, false)
            .unwrap();
        let fast = tokio::time::timeout(Duration::from_secs(5), pool.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fast.id, 1);
        pool.record_completion(fast, &mut reporter).unwrap();

        pool.try_dispatch(read(2), "measurement", 0, deadline, false)
            .unwrap();
        let replacement = tokio::time::timeout(Duration::from_secs(5), pool.receive()).await;
        let (replacement, completed_while_slow_pending) = match replacement {
            Ok(Ok(completion)) => {
                let completion_id = completion.id;
                (
                    Some(completion),
                    completion_id == 2 && pool.pending.contains_key(&0),
                )
            }
            Ok(Err(_)) | Err(_) => (None, false),
        };
        let _ = slow_release_tx.send(());
        let slow = pool.receive().await.unwrap();
        let slow_id = slow.id;
        pool.record_completion(slow, &mut reporter).unwrap();
        let replacement_id = if let Some(completion) = replacement {
            let id = completion.id;
            pool.record_completion(completion, &mut reporter).unwrap();
            id
        } else {
            let completion = pool.receive().await.unwrap();
            let id = completion.id;
            pool.record_completion(completion, &mut reporter).unwrap();
            id
        };
        pool.shutdown(true);
        drop(pool);
        drop(reporter);
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(completed_while_slow_pending);
        assert_eq!(slow_id, 0);
        assert_eq!(replacement_id, 2);
    }

    #[tokio::test]
    async fn full_preload_missing_scalar_read_is_wrong_value() {
        let directory = result_directory("full-preload-missing-read");
        let mut config = read_test_config();
        config.phases.preload = true;
        let output = directory.join("result");
        let result = run(
            MemoryBackend::omitting_put(5),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let outcomes = measurement_outcomes(&output);
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(result.is_err());
        assert_eq!(outcomes, ["wrong_value"]);
    }

    #[tokio::test]
    async fn full_preload_missing_get_many_value_is_wrong_value() {
        let directory = result_directory("full-preload-missing-many");
        let mut config = read_test_config();
        config.dataset.keys = 2;
        config.workload.mix = crate::config::OperationMix {
            get_many: 1,
            ..crate::config::OperationMix::default()
        };
        config.seed = (0..10_000)
            .find(|seed| {
                let mut generator = WorkloadGenerator::new(
                    *seed ^ 0x6b76_7265_7365_6172,
                    config.dataset.keys,
                    config.workload.mix.clone(),
                    config.workload.batch_size,
                    config.workload.distribution.clone(),
                );
                let keys = generator.next_item().unwrap().keys;
                keys.contains(&0) && keys.contains(&1)
            })
            .expect("small deterministic seed search finds both dataset keys");
        let output = directory.join("result");
        let result = run(
            MemoryBackend::omitting_put(5),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let outcomes = measurement_outcomes(&output);
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(result.is_err());
        assert_eq!(outcomes, ["wrong_value"]);
    }

    #[tokio::test]
    async fn partial_preload_missing_scalar_read_remains_success() {
        let directory = result_directory("partial-preload-missing-read");
        let mut config = read_test_config();
        config.phases.preload = false;
        let output = directory.join("result");
        let result = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let outcomes = measurement_outcomes(&output);
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(result.is_ok());
        assert_eq!(outcomes, ["success"]);
    }

    #[tokio::test]
    async fn aging_delete_keeps_later_missing_read_valid() {
        let directory = result_directory("aging-delete-missing-read");
        let mut config = read_test_config();
        config.workload.mix = crate::config::OperationMix {
            read: 1,
            delete: 1,
            ..crate::config::OperationMix::default()
        };
        config.phases.aging_operations = 1;
        config.phases.measurement_operations = 1;
        config.seed = (0..10_000)
            .find(|seed| {
                let mut generator = WorkloadGenerator::new(
                    *seed ^ 0x6b76_7265_7365_6172,
                    config.dataset.keys,
                    config.workload.mix.clone(),
                    config.workload.batch_size,
                    config.workload.distribution.clone(),
                );
                generator.next_item().unwrap().operation == OperationKind::Delete
                    && generator.next_item().unwrap().operation == OperationKind::Read
            })
            .expect("small deterministic seed search finds delete then read");
        let output = directory.join("result");
        let result = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let outcomes = measurement_outcomes(&output);
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(result.is_ok());
        assert_eq!(outcomes, ["success"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_loop_completions_do_not_release_future_arrivals() {
        let directory = std::env::temp_dir().join(format!(
            "kv-research-arrivals-{}-{}",
            std::process::id(),
            hex(&fresh_namespace().unwrap())
        ));
        std::fs::create_dir(&directory).unwrap();
        let output = directory.join("result");
        let mut config: RunConfig = serde_json::from_str(include_str!("../example.json")).unwrap();
        config.dataset.keys = 8;
        config.dataset.value_size = 256;
        config.workload.batch_size = 2;
        config.workload.mix = crate::config::OperationMix {
            read: 1,
            ..crate::config::OperationMix::default()
        };
        config.execution.mode = ExecutionMode::OpenLoop;
        config.execution.concurrency = 1;
        config.execution.queue_capacity = 2;
        config.execution.offered_ops_per_second = Some(100.0);
        config.execution.request_timeout_ms = 1000;
        config.phases.warmup_operations = 0;
        config.phases.aging_operations = 0;
        config.phases.measurement_operations = 8;
        config.limits.deadline_seconds = 30;
        let result = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let raw = std::fs::read_to_string(output.join("samples.csv"));
        std::fs::remove_dir_all(&directory).unwrap();
        result.unwrap();
        let raw = raw.unwrap();
        let rows: Vec<_> = raw
            .lines()
            .filter(|line| line.starts_with("measurement,"))
            .collect();
        assert_eq!(
            rows.len(),
            8,
            "every declared arrival must have a terminal row"
        );
        for row in rows {
            let columns: Vec<_> = row.split(',').collect();
            let scheduled: u64 = columns[3].parse().unwrap();
            let dispatched: u64 = columns[4].parse().unwrap();
            let completed: u64 = columns[6].parse().unwrap();
            assert!(
                scheduled <= dispatched && dispatched <= completed,
                "early or inconsistent dispatch: {row}"
            );
        }
    }

    #[tokio::test]
    async fn qualification_rejects_wrong_returned_bytes() {
        let backend = MemoryBackend::new(true, false);
        assert!(
            qualify(
                &backend,
                &[3; 16],
                42,
                Instant::now() + Duration::from_secs(3),
                1000
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn qualification_rejects_reordered_get_many_results() {
        let backend = MemoryBackend::new(false, true);
        assert!(
            qualify(
                &backend,
                &[3; 16],
                42,
                Instant::now() + Duration::from_secs(3),
                1000
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn sample_latency_uses_shared_dispatch_and_completion_instants() {
        let sample = make_sample_at(
            "measurement",
            "read",
            "success",
            3,
            7,
            true,
            17,
            10,
            1,
            8,
            Cow::Borrowed(""),
        );
        assert_eq!(sample.scheduling_ns, 4);
        assert_eq!(sample.response_ns, 14);
    }

    #[test]
    fn backend_timed_out_errors_are_timeout_outcomes() {
        let error = std::io::Error::new(std::io::ErrorKind::TimedOut, "backend deadline");
        assert!(is_timed_out(&error));
        assert_eq!(timeout_outcome(OperationKind::PutBatch), "unknown_mutation");
        assert_eq!(timeout_outcome(OperationKind::GetMany), "timeout");
    }
    #[test]
    fn rejected_arrivals_keep_nonzero_response_and_zero_dispatch() {
        let origin = Instant::now()
            .checked_sub(Duration::from_nanos(100))
            .unwrap();
        let work = WorkItem {
            operation: OperationKind::GetMany,
            keys: vec![1, 2, 3],
        };
        let sample = rejected_sample("measurement", &work, 1, origin, "capacity");
        assert_eq!(sample.dispatched_ns, 0);
        assert!(sample.response_ns >= 99);
        assert_eq!(sample.logical_keys, 3);
        assert_eq!(sample.outcome, "rejected");
    }
    #[tokio::test]
    async fn measurement_completion_gate_waits_for_release_with_process_live() {
        let directory = result_directory("measurement-completion-gate");
        let ready_file = directory.join("measurement.complete");
        let release_file = directory.join("measurement.observed");
        let gate = crate::config::MeasurementGateConfig {
            ready_file: ready_file.clone(),
            release_file: release_file.clone(),
            timeout_seconds: 2,
        };
        let controller = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), async {
                while !ready_file.exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                std::fs::write(release_file, b"observed\n").unwrap();
            })
            .await
            .unwrap();
        });
        wait_for_measurement_gate(&gate, Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        controller.await.unwrap();
        assert!(gate.ready_file.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn measurement_completion_gate_timeout_preserves_ready_marker() {
        let directory = result_directory("measurement-completion-timeout");
        let gate = crate::config::MeasurementGateConfig {
            ready_file: directory.join("measurement.complete"),
            release_file: directory.join("measurement.observed"),
            timeout_seconds: 1,
        };
        assert!(
            wait_for_measurement_gate(&gate, Instant::now() + Duration::from_secs(2))
                .await
                .is_err()
        );
        assert!(gate.ready_file.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn diagnostic_selection_is_measurement_only_and_bounded_to_scalar_mutations() {
        assert!(!is_profile_candidate("warmup", OperationKind::Put, 0, 4));
        assert!(!is_profile_candidate(
            "measurement",
            OperationKind::Read,
            0,
            4
        ));
        assert!(!is_profile_candidate(
            "measurement",
            OperationKind::PutBatch,
            0,
            4
        ));
        assert!(is_profile_candidate(
            "measurement",
            OperationKind::Put,
            0,
            4
        ));
        assert!(is_profile_candidate(
            "measurement",
            OperationKind::Delete,
            3,
            4
        ));
        assert!(!is_profile_candidate(
            "measurement",
            OperationKind::Put,
            4,
            4
        ));
    }
}
