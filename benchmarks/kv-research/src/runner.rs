use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend};
use crate::config::{DurabilityTier, ExecutionMode, RunConfig};
use crate::duration_ns;
use crate::report::{InterferenceResult, Reporter, RunSummary, Sample, Stream, StreamCounts};
use crate::workload::{
    OperationKind, WorkItem, WorkloadGenerator, fresh_namespace, key_bytes, value_bytes,
};
use blake2::{Blake2b512, Digest};
use std::borrow::Cow;
use std::future::Future;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering, fence};
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
    // A requested tier that the backend did not apply would silently corrupt a matched comparison.
    if let Some(tier) = config.durability_tier()?
        && info
            .configuration
            .get("durability_tier")
            .and_then(serde_json::Value::as_str)
            != Some(tier.as_str())
    {
        return Err(invalid(
            "backend receipt does not confirm the requested durability_tier",
        ));
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

    let mut generators = client_generators(&config);
    // Presence checks need a baseline dataset; without one every key may legitimately be absent.
    let oracle = Arc::new(KeyOracle::new(
        !config.qualification_only
            && (config.phases.preload || config.phases.verify_existing_dataset),
        config.dataset.keys,
        config.workload.mix.delete > 0
            || config.workload.mix.put > 0
            || config.workload.mix.put_batch > 0,
    )?);
    oracle.ensure_capacity(config.execution.concurrency)?;
    if fatal_reason.is_none()
        && !config.qualification_only
        && (config.phases.preload || config.phases.verify_existing_dataset)
    {
        reporter.set_phase(if config.phases.verify_existing_dataset {
            "verify_existing_dataset"
        } else {
            "preload"
        });
        match preload(
            &backend,
            &config,
            &namespace,
            started,
            deadline,
            &oracle,
            &mut reporter,
        )
        .await
        {
            Ok(()) => reporter.snapshot()?,
            Err(error) => {
                failed = true;
                fatal_reason = Some(format!("dataset preparation failed: {error}"));
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
            &backend,
            &config,
            &namespace,
            &mut generators,
            phase,
            count,
            started,
            deadline,
            &oracle,
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
    let mut timeline_arm_error = None;
    let mut timeline_armed = false;
    let mut read_profile_armed = false;
    let mut client_profile_armed = false;
    let mut client_profile_arm_error = None;
    let mut read_start = serde_json::Value::Null;
    let mut read_end = serde_json::Value::Null;
    let mut measurement_drained = false;
    let mut relocation_requested = false;
    if !config.qualification_only
        && fatal_reason.is_none()
        && config.phases.measurement_operations > 0
    {
        if config.phases.evict_mapped_wal_before_measurement {
            reporter.set_phase("mapped_wal_eviction");
            let worker = Arc::clone(&backend);
            let result =
                tokio::task::spawn_blocking(move || worker.evict_mapped_wal_before_measurement())
                    .await;
            let result = match result {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(error) => Err(format!("eviction worker failed: {error}")),
            };
            let eviction = match result {
                Ok(evidence) => serde_json::json!({
                    "requested":true, "available":true, "scope":"after warmup and aging; before measurement gate",
                    "semantics":"advisory initial mapped-WAL eviction; not proof of physical I/O or sustained coldness",
                    "evidence":evidence
                }),
                Err(error) => {
                    failed = true;
                    fatal_reason = Some(format!(
                        "pre-measurement mapped-WAL eviction failed: {error}"
                    ));
                    serde_json::json!({"requested":true,"available":false,"error":error})
                }
            };
            std::fs::write(
                output.join("pre-measurement-wal-eviction.json"),
                serde_json::to_vec_pretty(&eviction)?,
            )?;
            reporter.snapshot()?;
        }
        if fatal_reason.is_none()
            && let Some(gate) = &config.measurement_gate
            && let Err(error) = wait_for_measurement_gate(gate, deadline).await
        {
            failed = true;
            fatal_reason = Some(format!("measurement gate failed: {error}"));
            reporter.snapshot()?;
        }
        if fatal_reason.is_none()
            && let Some(Err(error)) = backend.check_background_health()
        {
            failed = true;
            fatal_reason = Some(format!("pre-measurement background health failed: {error}"));
        }
        if fatal_reason.is_none()
            && config.diagnostics.write_profile_limit > 0
            && let Err(error) = boundary_on_worker(&backend, move |backend| {
                backend.arm_write_profile(config.diagnostics.write_profile_limit)
            })
            .await
        {
            failed = true;
            let reason = format!("measurement write profile arm failed: {error}");
            profile_arm_error = Some(reason.clone());
            fatal_reason = Some(reason);
            reporter.snapshot()?;
        }
        if fatal_reason.is_none() && config.diagnostics.write_timeline_limit > 0 {
            match backend.arm_write_timeline(
                config.diagnostics.write_timeline_limit,
                config.diagnostics.write_timeline_sample_every,
            ) {
                Ok(()) => timeline_armed = true,
                Err(error) => {
                    failed = true;
                    let reason = format!("measurement write timeline arm failed: {error}");
                    timeline_arm_error = Some(reason.clone());
                    fatal_reason = Some(reason);
                }
            }
        }
        if fatal_reason.is_none() && config.diagnostics.valkey_profile_limit > 0 {
            match backend.arm_client_profile(config.diagnostics.valkey_profile_limit) {
                Ok(()) => client_profile_armed = true,
                Err(error) => {
                    failed = true;
                    let reason = format!("measurement client profile arm failed: {error}");
                    client_profile_arm_error = Some(reason.clone());
                    fatal_reason = Some(reason);
                }
            }
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
            match boundary_on_worker(&backend, KvBackend::read_observation).await {
                Ok(snapshot) => read_start = snapshot,
                Err(error) => {
                    failed = true;
                    fatal_reason = Some(format!("measurement start boundary failed: {error}"));
                }
            }
        }
        if fatal_reason.is_none() {
            measurement_attempted = true;
            backend.set_measurement_window(true);
            reporter.set_phase("measurement");
            let phase_result = if config.execution.mode == ExecutionMode::Interference {
                let (result, phase_result) = run_interference_phase(
                    &backend,
                    &config,
                    &namespace,
                    &mut generators[0],
                    started,
                    deadline,
                    &oracle,
                    &mut reporter,
                )
                .await;
                if let Err(error) = std::fs::write(
                    output.join("interference_result.json"),
                    serde_json::to_vec_pretty(&result)?,
                ) {
                    Err(error.into())
                } else {
                    phase_result
                }
            } else {
                run_phase(
                    &backend,
                    &config,
                    &namespace,
                    &mut generators,
                    "measurement",
                    config.phases.measurement_operations,
                    started,
                    deadline,
                    &oracle,
                    &mut reporter,
                )
                .await
            };
            // Close before the end boundary, also when the phase failed.
            backend.set_measurement_window(false);
            measurement_drained = phase_result.is_ok();
            if let Err(error) = phase_result {
                failed = true;
                fatal_reason = Some(format!("measurement phase stopped: {error}"));
            }
            match boundary_on_worker(&backend, KvBackend::read_observation).await {
                Ok(snapshot) => read_end = snapshot,
                Err(error) => {
                    measurement_drained = false;
                    failed = true;
                    let reason = format!("measurement end boundary failed: {error}");
                    fatal_reason = Some(fatal_reason.map_or_else(
                        || reason.clone(),
                        |previous| format!("{previous}; {reason}"),
                    ));
                }
            }
            reporter.snapshot()?;
            if config.phases.relocation {
                match backend.start_relocation() {
                    Ok(()) => relocation_requested = true,
                    Err(error) => {
                        failed = true;
                        let reason = format!("post-measurement relocation request failed: {error}");
                        fatal_reason = Some(fatal_reason.map_or_else(
                            || reason.clone(),
                            |previous| format!("{previous}; {reason}"),
                        ));
                    }
                }
            }
        }
    }
    if config.diagnostics.valkey_profile_limit > 0 {
        let result = if client_profile_armed {
            backend.finish_client_profile(
                deadline + Duration::from_secs(config.limits.shutdown_grace_seconds),
            )
        } else {
            Err(invalid(
                client_profile_arm_error
                    .as_deref()
                    .unwrap_or("client profile not armed for measurement"),
            ))
        };
        let (profile, error) = match result {
            Ok(profile) => (Some(profile), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let evidence = serde_json::json!({
            "schema_version": 1,
            "requested_limit": config.diagnostics.valkey_profile_limit,
            "measurement_attempted": measurement_attempted,
            "measurement_drained": measurement_drained,
            "available": profile.as_ref().is_some_and(|value| value["available"] == true),
            "error": error,
            "profile": profile,
            "interpretation": "Client-local owner-pipeline records join caller_sample_id; grouped calls may have several records. Poll wait is not wire time or OS wake-to-run latency."
        });
        std::fs::write(
            output.join("valkey-timings.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        if let Some(error) = evidence["error"].as_str() {
            failed = true;
            fatal_reason = Some(fatal_reason.map_or_else(
                || format!("client profile export failed: {error}"),
                |previous| format!("{previous}; client profile export failed: {error}"),
            ));
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

    let write_pre_gate = if config.diagnostics.write_profile_limit == 0 {
        None
    } else if measurement_attempted && profile_arm_error.is_none() {
        Some(
            match boundary_on_worker(&backend, KvBackend::snapshot_write_profile).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    if error.is::<crate::backend::LiveAdoptionBoundaryError>() {
                        failed = true;
                        let reason = format!("measurement write profile boundary failed: {error}");
                        fatal_reason = Some(fatal_reason.map_or_else(
                            || reason.clone(),
                            |previous| format!("{previous}; {reason}"),
                        ));
                    }
                    serde_json::json!({"available": false, "reason": error.to_string()})
                }
            },
        )
    } else {
        Some(
            serde_json::json!({"available": false, "reason": "write profile not armed for measurement"}),
        )
    };

    if measurement_attempted && let Some(Err(error)) = backend.check_background_health() {
        failed = true;
        let reason = format!("measurement-end background health failed: {error}");
        fatal_reason = Some(fatal_reason.map_or_else(
            || reason.clone(),
            |previous| format!("{previous}; {reason}"),
        ));
    }
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
    let mut write_wait_metrics_exported = false;
    if config.diagnostics.write_timeline_limit > 0 {
        let (timeline, mut error) = if timeline_armed {
            match finish_write_timeline_on_worker(
                Arc::clone(&backend),
                deadline + Duration::from_secs(config.limits.shutdown_grace_seconds),
                &mut reporter,
            )
            .await
            {
                Ok(value) => {
                    let error = write_timeline_owner_error(&value);
                    (Some(value), error)
                }
                Err(error) => (None, Some(error.to_string())),
            }
        } else {
            (
                None,
                Some(
                    timeline_arm_error
                        .unwrap_or_else(|| "timeline not armed for measurement".to_owned()),
                ),
            )
        };
        if let Some(metrics) = timeline
            .as_ref()
            .and_then(|value| value.get("write_wait_metrics"))
        {
            match export_write_wait_metrics(&config, output, metrics) {
                Ok(exported) => write_wait_metrics_exported = exported,
                Err(export_error) => {
                    let detail = format!("write wait metrics export failed: {export_error}");
                    error = Some(match error {
                        Some(previous) => format!("{previous}; {detail}"),
                        None => detail,
                    });
                }
            }
        }
        let evidence = serde_json::json!({
            "schema_version": 2,
            "requested_limit": config.diagnostics.write_timeline_limit,
            "measurement_attempted": measurement_attempted,
            "measurement_drained": measurement_drained,
            "available": timeline.as_ref().is_some_and(|value| value.get("deadline_exceeded").and_then(serde_json::Value::as_bool) != Some(true)),
            "status": if error.is_some() { "failed" } else { "complete" },
            "error": error,
            "timeline": timeline,
            "interpretation": "Coordinator events use a local monotonic origin; stage intervals can overlap and are not additive. Server clocks, if present, are a separate domain. Wire time is not isolated."
        });
        if let Err(error) = std::fs::write(
            output.join("write-timeline.json"),
            serde_json::to_vec_pretty(&evidence)?,
        ) {
            failed = true;
            fatal_reason = Some(fatal_reason.map_or_else(
                || format!("write timeline export failed: {error}"),
                |previous| format!("{previous}; write timeline export failed: {error}"),
            ));
        } else if let Some(error) = evidence["error"].as_str() {
            failed = true;
            fatal_reason = Some(fatal_reason.map_or_else(
                || format!("write timeline incomplete: {error}"),
                |previous| format!("{previous}; write timeline incomplete: {error}"),
            ));
        }
    }
    if config.diagnostics.write_profile_limit > 0 {
        let profile_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
        let export = export_write_diagnostics(
            backend.as_ref(),
            &config,
            output,
            measurement_attempted,
            measurement_drained,
            profile_arm_error.as_deref(),
            write_pre_gate,
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
    let client_pool_final = match boundary_on_worker(&backend, KvBackend::read_observation).await {
        Ok(snapshot) => snapshot
            .get("client_pool")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        Err(error) => {
            failed = true;
            fatal_reason.get_or_insert_with(|| format!("final read boundary failed: {error}"));
            serde_json::Value::Null
        }
    };
    if let Some(Err(error)) = backend.check_background_health() {
        failed = true;
        let reason = format!("final background health failed: {error}");
        fatal_reason = Some(fatal_reason.map_or_else(
            || reason.clone(),
            |previous| format!("{previous}; {reason}"),
        ));
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
        fatal_reason = Some(match fatal_reason {
            Some(previous) => format!("{previous}; {detail}"),
            None => detail,
        });
    }
    let durability = durability_receipt(&config, &info);
    let summary = RunSummary {
        status: if failed { "failed".to_owned() } else { "completed".to_owned() },
        started_unix_seconds: started_wall,
        finished_unix_seconds: finished_wall,
        durability,
        backend: info,
        execution: serde_json::json!({
            "effective_client_shards": config.execution.effective_client_shards()
        }),
        phase_counts: serde_json::json!({
            "offered": totals.0, "completed_terminal": totals.1, "succeeded": totals.6, "rejected": totals.2,
            "timeouts_or_unknown_mutations": totals.3, "errors_or_wrong_values": totals.4,
            "logical_keys_completed_terminal": totals.5, "logical_keys_offered": totals.7, "logical_keys_rejected": totals.8,
            "outstanding_at_finalization": totals.0.saturating_sub(totals.1).saturating_sub(totals.2),
            "realized_mix": reporter.realized_mix(),
        }),
        distributions: reporter.distribution_summaries(),
        limitations: vec![
            "Finite local workload phases are not evidence of sustainable steady-state capacity.".to_owned(),
            "Owner physical-device independence is not inferred from configured topology; see backend topology evidence.".to_owned(),
            "Measured API latency ends when response bytes are materialized; byte verification is outside the API timer. The released_ns column and manifest worker_release aggregate carry the worker-release time that includes oracle cost.".to_owned(),
            "Batch latency is one complete backend API call; logical key counts are reported separately and no per-key latency is inferred.".to_owned(),
            "Read oracle: a missing value is accepted only when a delete of that key completed without a later clean put, or a delete or put of that key was in flight during the read; present values are always compared byte for byte. This is not a linearizability check.".to_owned(),
            "Synchronous adapters run on fixed dedicated workers; Tokio timeouts cannot preempt blocking calls, so overruns are retained and the process supervisor supplies the hard wall-clock bound.".to_owned(),
        ],
    };
    let mut manifest_extra = serde_json::json!({
        "config_hash_blake2b512": config_hash,
        "binary": binary_identity(),
        "config": config,
        "run_namespace_hex": hex(&namespace),
        "seed": config.seed,
        "source_revision": config.source_revision.as_deref().or(option_env!("GIT_COMMIT")),
        "hardware": hardware_identity(),
        "timing": {"clock": "std::time::Instant monotonic", "timer_start": "backend API dispatch after bounded admission", "timer_end": "response materialized", "worker_release": "released_ns, after oracle verification; closed-loop cycle time is released_ns minus dispatched_ns"},
        "durability": summary.durability,
        "read_oracle": {"mode": if !config.qualification_only && (config.phases.preload || config.phases.verify_existing_dataset) { if config.workload.mix.delete > 0 || config.workload.mix.put > 0 || config.workload.mix.put_batch > 0 { "per_key_overlap_aware" } else { "strict_present_after_baseline" } } else { "disabled_no_baseline_dataset" }},
        "client_pool_final": client_pool_final,
        "request_timeout_ms": config.execution.request_timeout_ms,
        "data_policy": {"key_bytes": 32, "key_derivation": "BLAKE2(namespace,seed,logical_key_id), all bits hashed",
            "put_key_policy": config.workload.put_key_policy,
            "value_identity_header_bytes": 16, "value_entropy_fraction_of_remainder": config.dataset.entropy},
        "phase_semantics": {"preload": config.phases.preload, "warmup_operations": config.phases.warmup_operations,
            "warmup_key_sweep": config.phases.warmup_key_sweep,
            "aging_operations": config.phases.aging_operations, "measurement_operations": config.phases.measurement_operations,
            "measurement_key_sweep": config.phases.measurement_key_sweep,
            "relocation": config.phases.relocation,
            "relocation_requested": relocation_requested,
            "sustainability_claim": false},
        "qualification_failure": fatal_reason,
    });
    if config.interference.is_some() {
        manifest_extra["interference"] = serde_json::json!({
            "schema_version": 1, "path": "interference_result.json",
            "failed_semantics": "admitted calls without a terminal worker response at drain",
        });
    }
    if write_wait_metrics_exported {
        manifest_extra["write_wait_metrics"] = serde_json::json!({
            "schema_version": 1, "path": "write_wait_metrics.json",
        });
    }
    reporter.finish(&summary, failed, &manifest_extra)?;
    if failed {
        return Err(invalid(fatal_reason.as_deref().unwrap_or("run failed")));
    }
    Ok(())
}

/// Durability contract and matched comparison cohort, copied into summary and manifest.
/// Native page-cache includes Kvrocks non-sync WAL writes. Synchronous includes
/// native WAL barriers and Valkey AOF always. Valkey no-fsync and everysec use
/// their own os_buffered and periodic_sync cohorts, never native page-cache.
fn durability_receipt(config: &RunConfig, info: &BackendInfo) -> serde_json::Value {
    if let Ok(Some(tier)) = config.durability_tier() {
        return serde_json::json!({
            "system": "tidehunter", "tier": tier.as_str(),
            "comparison_cohort": tier.comparison_cohort(),
            "confirmed_by_backend_receipt": info.configuration.get("durability_tier"),
        });
    }
    let ack_contract = info.configuration.get("ack_contract");
    let relaxed = match (
        info.name.as_str(),
        ack_contract
            .and_then(serde_json::Value::as_str)
            .unwrap_or("synced"),
    ) {
        (name, "os_buffered") if name.starts_with("valkey") => {
            Some(("appendfsync_no", "os_buffered"))
        }
        (name, "periodic_sync") if name.starts_with("valkey") => {
            Some(("appendfsync_everysec", "periodic_sync"))
        }
        ("kvrocks", "page_cache") => {
            Some(("wal_sync_no", DurabilityTier::Native.comparison_cohort()))
        }
        _ => None,
    };
    if let Some((tier, cohort)) = relaxed {
        return serde_json::json!({
            "system": info.name, "tier": tier, "comparison_cohort": cohort,
            "confirmed_by_backend_receipt": ack_contract,
        });
    }
    let (tier, cohort) = if info.name.starts_with("valkey") {
        (
            "appendfsync_always",
            DurabilityTier::Synchronous.comparison_cohort(),
        )
    } else {
        match info
            .configuration
            .get("durability")
            .and_then(serde_json::Value::as_str)
        {
            Some("page_cache") => ("page_cache", DurabilityTier::Native.comparison_cohort()),
            Some("value_wal_barrier") => (
                "value_wal_barrier",
                DurabilityTier::Synchronous.comparison_cohort(),
            ),
            Some("value_wal_group_barrier") => (
                "value_wal_group_barrier",
                DurabilityTier::Synchronous.comparison_cohort(),
            ),
            _ => ("unclassified", "none"),
        }
    };
    serde_json::json!({"system": info.name, "tier": tier, "comparison_cohort": cohort})
}

/// Reads a monotonic counter at `field`, a dotted path for nested objects such as
/// `device_credit_lock.wait_ns`.
fn counter_at<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    field
        .split('.')
        .try_fold(value, |current, part| current.get(part))
}

fn counter_delta(
    start: &serde_json::Value,
    end: &serde_json::Value,
    fields: &[&str],
) -> serde_json::Value {
    let mut delta = serde_json::Map::new();
    for field in fields {
        let Some((a, b)) = counter_at(start, field)
            .and_then(serde_json::Value::as_u64)
            .zip(counter_at(end, field).and_then(serde_json::Value::as_u64))
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

fn nvmeof_counter_delta(
    start: &serde_json::Value,
    end: &serde_json::Value,
    fields: &[&str],
) -> serde_json::Value {
    for field in fields {
        if start.get(field).and_then(serde_json::Value::as_u64) == Some(u64::MAX)
            || end.get(field).and_then(serde_json::Value::as_u64) == Some(u64::MAX)
        {
            return serde_json::json!({"available": false, "error": format!("saturated counter: {field}")});
        }
    }
    counter_delta(start, end, fields)
}

/// Live NVMe-oF counters whose measurement-window delta is exported: every `sum`
/// field of `LiveNvmeCounts` plus the two evidence-drop counters of `LiveNvmeStats`.
/// The observer exporter admits exactly this set; both sides are pinned to
/// `experiments/kv-comparison/observability/live_nvmeof_counter_fields.json`.
const LIVE_NVMEOF_DELTA_FIELDS: &[&str] = &[
    "completed_read_ops",
    "completed_read_bytes",
    "completed_write_ops",
    "completed_write_bytes",
    "recovery_read_ops",
    "recovery_read_bytes",
    "metadata_requests",
    "namespace_connects",
    "unknown_writes",
    "errors",
    "queue_ns",
    "queue_samples",
    "submit_ns",
    "submit_samples",
    "cq_ns",
    "cq_samples",
    "control_ns",
    "control_samples",
    "validation_ns",
    "validation_samples",
    "publication_ns",
    "publication_samples",
    "namespace_setup_ns",
    "namespace_setup_samples",
    "read_io_ns",
    "read_io_samples",
    "read_controller_resumes",
    "route_evidence_dropped",
    "fence_evidence_dropped",
    "lane_inflight_samples",
    "lane_inflight_request_sum",
    "lane_inflight_op_sum",
    "delivery_ns",
    "delivery_samples",
    "controller_connect_ns",
    "controller_connect_samples",
    "controller_disconnect_ns",
    "controller_disconnect_samples",
    "grant_hits",
    "grant_misses",
    "grant_evictions",
    "copy_bytes_avoided",
    "leased_frame_batches",
    "write_submit_calls",
    "owner_drains",
    "owner_drain_batches",
    "owner_drain_stop_empty",
    "owner_drain_stop_allocated",
    "owner_drain_stop_fence",
    "owner_drain_stop_limit",
    "owner_drain_stop_credit",
    "leased_submit_ready_left",
    "leased_submit_ready_left_batches",
    "leased_submit_ready_left_full",
    "leased_submit_ready_left_full_batches",
    "leased_deferred_submits",
    "lease_exchanges",
    "leased_frames",
    "leased_eventfd_unavailable",
    "leased_worker_waits",
    "leased_worker_wait_timeouts",
    "leased_flushes",
    "leased_flush_ns",
    "leased_flush_samples",
    "leased_flush_covered_frames",
    "read_lane_pin_failures",
    "leased_bytes",
    "leased_skip_markers",
    "extents_leased",
    "adoption_async_exchanges",
    "adoption_sync_exchanges",
    "adoption_async_ns",
    "adoption_async_samples",
    "adoption_settle_ns",
    "adoption_settle_samples",
    "adoption_history_full_settles",
    "fua_writes",
    "leased_write_ns",
    "leased_write_samples",
    "extent_switch_ns",
    "extent_switch_samples",
    "lease_reserve_ns",
    "lease_namespace_sync_ns",
    "lease_publish_ns",
    "lease_profile_samples",
    "adopt_owner_ns",
    "adopt_close_scan_ns",
    "adopt_close_scan_bytes",
    "adopt_profile_samples",
    "extent_preleases",
    "extent_switch_waits",
    "leased_failed_writes",
    "leased_short_writes",
    "leased_timed_out_writes",
    "leased_full_writes",
    "failed_write_ops",
    "short_write_ops",
    "short_write_bytes",
    // W1 write resume: attempts past the decision checks, attempts that rewrote
    // at least one frame, the frames they rewrote, and refused attempts. All are
    // monotonic event counts (`fetch_add` in live_metrics.rs), not gauges.
    "resume_attempts",
    "resume_rewrites",
    "resume_frames",
    "resume_refused",
];
/// Lifetime maxima of `LiveNvmeCounts`: gauge peaks recorded at both boundaries, never deltas.
const LIVE_NVMEOF_PEAK_FIELDS: &[&str] = &[
    "lane_inflight_request_max",
    "lane_inflight_op_max",
    "max_leased_in_flight",
];

pub(crate) fn measurement_read_evidence(
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
        "bypass_disabled",
        "bypass_admission",
        "bypass_eviction_limit",
        "admitted_bytes",
        "served_bytes",
        "read_profile_live_inline",
        "write_authority_prepare_ns",
        "write_authority_admit_ns",
        "write_authority_windows",
        "write_authority_batches",
        "read_profile_live_inline_fallback_drain",
        "read_profile_live_inline_fallback_busy",
        "read_profile_live_inline_fallback_grant",
        "read_profile_live_inline_fallback_credit",
        "read_profile_live_inline_fallback_paused",
        "read_profile_live_inline_fallback_coalesced",
    ];
    let deltas = if attempted && drained && start["available"] == true && end["available"] == true {
        serde_json::json!({
            "coordinator": counter_delta(&start["coordinator"], &end["coordinator"], COORDINATOR)
        })
    } else {
        serde_json::json!({"available": false, "error": if !attempted { "measurement not attempted" }
            else if !drained { "measurement did not complete and drain successfully" }
            else { "backend observation unavailable at a boundary" }})
    };
    let live_nvmeof = if attempted && drained {
        nvmeof_counter_delta(
            &start["live_nvmeof"],
            &end["live_nvmeof"],
            LIVE_NVMEOF_DELTA_FIELDS,
        )
    } else {
        serde_json::json!({"available":false,"error":"measurement not attempted or not drained"})
    };
    let client_pool = if attempted && drained && start.get("client_pool").is_some() {
        counter_delta(
            &start["client_pool"],
            &end["client_pool"],
            &[
                "connections_poisoned",
                "connections_replaced",
                "replacement_attempts_failed",
                "timeouts_before_send",
            ],
        )
    } else {
        serde_json::json!({"available":false,"error":"backend exposes no client pool counters or measurement not drained"})
    };
    // Lifetime maxima are not monotonic counters: a delta would be meaningless, so both
    // boundary values are recorded and the maximum is not attributed to the measurement window.
    let live_peaks = LIVE_NVMEOF_PEAK_FIELDS
        .iter()
        .map(|field| {
            (
                (*field).to_owned(),
                serde_json::json!({
                    "at_start": start["live_nvmeof"].get(field),
                    "at_end": end["live_nvmeof"].get(field),
                    "scope": "lifetime maximum, not a measurement-window maximum",
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({
        "schema_version": 1, "scope": "measurement_boundary",
        "start_boundary": "after warmup, aging and measurement gate; before measurement run_phase",
        "end_boundary": "after measurement run_phase and worker drain; before completion gate, profile finalization and postqualification",
        "measurement_attempted": attempted, "measurement_drained": drained,
        "snapshot_semantics": "sequential component and owner samples, not a globally atomic cut; raw snapshots include gauges and lifetime peaks",
        "limitations": "logical WAL reads, NOWAIT successes and async submissions are not physical IOPS or per-operation NAND evidence",
        "start": start, "end": end, "deltas": deltas, "live_nvmeof":live_nvmeof,
        "live_nvmeof_peaks": live_peaks,
        "client_pool": client_pool
    })
}

async fn preload<B: KvBackend>(
    backend: &Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    origin: Instant,
    deadline: Instant,
    oracle: &Arc<KeyOracle>,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let verify_existing = config.phases.verify_existing_dataset;
    let phase = if verify_existing {
        "verify_existing_dataset"
    } else {
        "preload"
    };
    let mut preparation_failed = false;
    let mut pool = WorkerPool::new(backend, config, namespace, origin, oracle)?;
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
                Ok(completion) => {
                    preparation_failed |= completion.sample.outcome != "success";
                    pool.record_completion(completion, reporter).await?;
                }
                Err(error) => {
                    let _ = pool.record_unknown_pending(reporter).await;
                    pool.shutdown(false);
                    return Err(error);
                }
            }
        }
        let work = WorkItem {
            operation: if verify_existing {
                OperationKind::Read
            } else {
                OperationKind::Put
            },
            keys: vec![id],
        };
        let scheduled = duration_ns(origin.elapsed());
        reporter.offered(phase, work.operation.as_str(), 1);
        if let Err(work) = pool.try_dispatch(work, phase, scheduled, deadline, false, None) {
            preparation_failed = true;
            reporter
                .rejected(rejected_sample(
                    phase,
                    &work,
                    scheduled,
                    origin,
                    "worker_queue_full",
                ))
                .await?;
        }
        if Instant::now() >= next_snapshot {
            reporter.snapshot()?;
            next_snapshot = Instant::now() + Duration::from_secs(1);
        }
    }
    while !pool.pending.is_empty() {
        match receive_with_snapshots(&mut pool, reporter, drain_deadline, &mut next_snapshot).await
        {
            Ok(completion) => {
                preparation_failed |= completion.sample.outcome != "success";
                pool.record_completion(completion, reporter).await?;
            }
            Err(error) => {
                let _ = pool.record_unknown_pending(reporter).await;
                pool.shutdown(false);
                return Err(error);
            }
        }
    }
    pool.shutdown(true);
    if expired {
        return Err(invalid(
            "global deadline expired before dataset preparation completed",
        ));
    }
    if preparation_failed {
        return Err(invalid("dataset preparation did not succeed for every key"));
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
async fn boundary_on_worker<B: KvBackend, T: Send + 'static>(
    backend: &Arc<B>,
    capture: impl FnOnce(&B) -> BackendResult<T> + Send + 'static,
) -> BackendResult<T> {
    let worker = Arc::clone(backend);
    tokio::task::spawn_blocking(move || capture(&worker)).await?
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
async fn finish_write_timeline_on_worker<B: KvBackend>(
    backend: Arc<B>,
    deadline: Instant,
    reporter: &mut Reporter,
) -> BackendResult<serde_json::Value> {
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let worker = thread::Builder::new()
        .name("kv-research-timeline-finish".to_owned())
        .spawn(move || {
            let _ = sender.send(backend.finish_write_timeline(deadline));
        })?;
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    loop {
        tokio::select! {
            result = &mut receiver => {
                let _ = worker.join();
                return result.map_err(|_| invalid("write timeline finalizer exited without a result"))?;
            }
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            () = sleep_until(TokioInstant::from_std(deadline)) => {
                return Err(invalid("write timeline finalization exceeded global deadline and shutdown grace"));
            }
        }
    }
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
        .map_err(|_| invalid(&format!("{context} timed out")))?
        .map_err(|source| step_error(context, source))?;
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
        .map_err(|_| invalid(&format!("{context} timed out")))?
        .map_err(|source| step_error(context, source))?;
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

/// Names the failed qualification step while keeping the backend error as `source()`,
/// so cause-chain classification (for example `is_timed_out`) still reaches it.
#[derive(Debug)]
struct StepError {
    context: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for StepError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.context, self.source)
    }
}

impl std::error::Error for StepError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn step_error(
    context: &str,
    source: Box<dyn std::error::Error + Send + Sync>,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(StepError {
        context: context.to_owned(),
        source,
    })
}

pub(crate) fn request_deadline(global: Instant, timeout_ms: u64) -> Instant {
    global.min(Instant::now() + Duration::from_millis(timeout_ms))
}

struct WorkerRequest {
    id: u64,
    work: WorkItem,
    phase: &'static str,
    profiled_write: bool,
    caller_sample_id: Option<u64>,
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
    caller_sample_id: Option<u64>,
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
    id_stride: u64,
    origin: Instant,
    value_size: usize,
    stream: Option<Stream>,
}

impl WorkerPool {
    fn new<B: KvBackend>(
        backend: &Arc<B>,
        config: &RunConfig,
        namespace: &[u8; 16],
        origin: Instant,
        oracle: &Arc<KeyOracle>,
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
            let oracle = Arc::clone(oracle);
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
                                caller_sample_id,
                                scheduled_ns,
                                origin,
                                global_deadline,
                                dispatch_timestamp,
                            } = request;
                            let mut sample = handle.block_on(execute_item(
                                backend.as_ref(),
                                &config,
                                &namespace,
                                work,
                                profiled_write,
                                caller_sample_id,
                                phase,
                                scheduled_ns,
                                origin,
                                global_deadline,
                                &oracle,
                                dispatch_timestamp,
                            ));
                            sample.caller_sample_id = caller_sample_id;
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
            id_stride: 1,
            origin,
            value_size: config.dataset.value_size,
            stream: None,
        })
    }

    fn try_dispatch(
        &mut self,
        work: WorkItem,
        phase: &'static str,
        scheduled_ns: u64,
        deadline: Instant,
        profiled_write: bool,
        caller_sample_id: Option<u64>,
    ) -> Result<(), WorkItem> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(self.id_stride);
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
            caller_sample_id,
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
                caller_sample_id,
                dispatch_timestamp,
            },
        );
        Ok(())
    }

    async fn record_completion(
        &mut self,
        mut completion: WorkerCompletion,
        reporter: &mut Reporter,
    ) -> BackendResult<()> {
        self.pending.remove(&completion.id);
        completion.sample.stream = self.stream;
        if let Err(error) = reporter.record(completion.sample).await {
            if self.stream.is_none() {
                let _ = self.record_unknown_pending(reporter).await;
                self.shutdown(false);
            } else if self.closed_loop {
                // Telemetry failure must not detach interference workers. The caller
                // stops admission, drains every accepted call, then joins both pools.
                self.idle_workers.push(completion.worker_index);
            }
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

    async fn record_unknown_pending(&mut self, reporter: &mut Reporter) -> BackendResult<()> {
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
                stream: self.stream,
                phase: pending.phase,
                operation: operation.as_str(),
                outcome,
                caller_sample_id: pending.caller_sample_id,
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
                released_ns: now,
                logical_keys: pending.logical_keys,
                bytes: pending.bytes,
                error_kind: Cow::Borrowed("external_shutdown_deadline_unknown_outcome"),
            };
            if let Err(error) = reporter.record(sample).await
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

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // Startup can fail after worker creation but before any request is sent.
        // Those idle threads must be joined, including a failed client-thread spawn.
        self.shutdown(self.pending.is_empty());
    }
}

pub(crate) async fn wait_for_measurement_gate(
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

// Keep the measurement boundaries and selected diagnostic state explicit at export.
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
        "error": finish_error
    });
    std::fs::write(
        output.join("read-diagnostics.json"),
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    Ok(finish_error)
}

// Keep the measurement boundaries and selected diagnostic state explicit at export.
#[allow(clippy::too_many_arguments)]
async fn export_write_diagnostics<B: KvBackend>(
    backend: &B,
    config: &RunConfig,
    output: &Path,
    measurement_attempted: bool,
    measurement_drained: bool,
    arm_error: Option<&str>,
    pre_gate: Option<serde_json::Value>,
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
        "measurement_drained": measurement_drained,
        "runtime_enabled": observed_samples > 0,
        "pre_completion_gate": pre_gate,
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
        "error": finish_error
    });
    std::fs::write(
        output.join("write-diagnostics.json"),
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    Ok(finish_error)
}

fn write_timeline_owner_error(timeline: &serde_json::Value) -> Option<String> {
    if timeline
        .get("deadline_exceeded")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Some(
            "write timeline finalization exceeded shutdown grace; partial evidence retained"
                .to_owned(),
        );
    }
    let errors = timeline
        .get("owner_errors")
        .and_then(serde_json::Value::as_array);
    let expected = timeline
        .get("owner_expected_connections")
        .and_then(serde_json::Value::as_u64);
    let captured = timeline
        .get("owner_captured_connections")
        .and_then(serde_json::Value::as_u64);
    if errors.is_none() || expected.is_none() || captured.is_none() {
        return Some("owner timeline coverage or errors unavailable".to_owned());
    }
    if errors.is_some_and(|errors| !errors.is_empty())
        || matches!((expected, captured), (Some(a), Some(b)) if a != b)
    {
        Some(format!(
            "owner timeline capture incomplete: captured {captured:?} of {expected:?} connections; owner errors: {}",
            errors.map_or(0, Vec::len)
        ))
    } else {
        None
    }
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
fn is_timeline_candidate(
    phase: &str,
    operation: OperationKind,
    selected: usize,
    limit: usize,
) -> bool {
    phase == "measurement"
        && matches!(
            operation,
            OperationKind::Put | OperationKind::Delete | OperationKind::PutBatch
        )
        && selected < limit
}

fn measured_caller_id(
    config: &RunConfig,
    phase: &str,
    operation: OperationKind,
    timeline: bool,
    id: u64,
) -> Option<u64> {
    (timeline
        || (phase == "measurement"
            && (config.diagnostics.valkey_profile_limit > 0
                || (config.diagnostics.read_profile_limit > 0
                    && matches!(
                        operation,
                        OperationKind::Read | OperationKind::MissingRead | OperationKind::GetMany
                    )))))
    .then_some(id)
}

/// Allow 250 ms to offer in-window arrivals: over 60 times the observed 4 ms
/// stalls, while still failing a scheduler hundreds of milliseconds behind.
const INTERFERENCE_ARRIVAL_CATCH_UP: Duration = Duration::from_millis(250);

/// The scheduler owns both bounded pools. Read admission never waits for a put.
/// Each pool owns fixed worker queues and drains before the common end clock.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_interference_phase<B: KvBackend>(
    backend: &Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    generator: &mut WorkloadGenerator,
    origin: Instant,
    deadline: Instant,
    oracle: &Arc<KeyOracle>,
    reporter: &mut Reporter,
) -> (InterferenceResult, BackendResult<()>) {
    let mut result = InterferenceResult {
        schema_version: 1,
        case_id: config.case_id.clone().unwrap_or_default(),
        read: StreamCounts::default(),
        put: StreamCounts::default(),
        measurement_drained: false,
        read_offered_per_second: 0,
        measurement_elapsed_ns: 0,
        window_start_ns: 0,
        window_end_ns: 0,
    };
    let Some(settings) = &config.interference else {
        return (result, Err(invalid("interference settings missing")));
    };
    result.read_offered_per_second = settings.read_offered_per_second;
    let mut read_config = config.clone();
    read_config.execution.mode = ExecutionMode::OpenLoop;
    read_config.execution.queue_capacity = settings.read_queue_capacity;
    let mut reads = match WorkerPool::new(backend, &read_config, namespace, origin, oracle) {
        Ok(pool) => pool,
        Err(error) => return (result, Err(error)),
    };
    reads.stream = Some(Stream::Read);
    let mut put_config = config.clone();
    put_config.execution.mode = ExecutionMode::ClosedLoop;
    put_config.execution.concurrency = settings.put_concurrency;
    put_config.execution.queue_capacity = settings.put_concurrency;
    let mut puts = match WorkerPool::new(backend, &put_config, namespace, origin, oracle) {
        Ok(pool) => pool,
        Err(error) => {
            reads.shutdown(true);
            return (result, Err(error));
        }
    };
    puts.stream = Some(Stream::Put);
    let start = Instant::now();
    let admission_end = start + Duration::from_secs(settings.duration_seconds);
    result.window_start_ns = duration_ns(start.saturating_duration_since(origin));
    let planned_reads = settings.read_offered_per_second * settings.duration_seconds;
    let mut next_snapshot = start + Duration::from_secs(1);
    let mut profile_selected = 0;
    let mut timeline_selected = 0;
    let mut iterations_since_yield = 0;
    let scheduled = async {
        loop {
            if Instant::now() >= deadline {
                return Err(invalid("global deadline expired during interference"));
            }
            let now = Instant::now();
            if now >= admission_end + INTERFERENCE_ARRIVAL_CATCH_UP
                && result.read.offered < planned_reads
            {
                return Err(invalid(&format!(
                    "interference arrival scheduler missed the admission window: lateness_ns={}, offered={}, planned={planned_reads}",
                    duration_ns(now.saturating_duration_since(admission_end)),
                    result.read.offered,
                )));
            }
            // Ready completions and catch-up arrivals can otherwise avoid yielding.
            if iterations_since_yield == 256 {
                tokio::task::yield_now().await;
                iterations_since_yield = 0;
                continue;
            }
            iterations_since_yield += 1;
            // Integer arrival clocks preserve the offered rate without accumulating drift.
            let due = start + Duration::from_nanos(
                result.read.offered * 1_000_000_000 / settings.read_offered_per_second,
            );
            if result.read.offered < planned_reads && Instant::now() >= due {
                if result.read.offered + result.put.offered >= config.phases.measurement_operations {
                    return Err(invalid("interference operation safety bound reached"));
                }
                let work = generator.interference_item(None)?;
                let scheduled_ns = duration_ns(due.saturating_duration_since(origin));
                reporter.offered("measurement", "read", 1);
                result.read.offered += 1;
                let caller_id = measured_caller_id(config, "measurement", OperationKind::Read, false, reads.next_id);
                let admitted = if reads.pending.len() >= settings.read_queue_capacity {
                    Err(work)
                } else {
                    reads.try_dispatch(work, "measurement", scheduled_ns, deadline, false, caller_id)
                };
                match admitted {
                    Ok(()) => result.read.admitted += 1,
                    Err(work) => {
                        result.read.rejected += 1;
                        let mut sample = rejected_sample("measurement", &work, scheduled_ns, origin, "admission_capacity");
                        sample.stream = Some(Stream::Read);
                        reporter.rejected(sample).await?;
                    }
                }
                continue;
            }
            let now = Instant::now();
            if now < admission_end && puts.pending.len() < settings.put_concurrency {
                if result.read.offered + result.put.offered >= config.phases.measurement_operations {
                    return Err(invalid("interference operation safety bound reached"));
                }
                let work = generator.interference_item(Some(result.put.offered))?;
                let profile = is_profile_candidate("measurement", OperationKind::Put, profile_selected, config.diagnostics.write_profile_limit);
                let timeline = is_timeline_candidate("measurement", OperationKind::Put, timeline_selected,
                    config.diagnostics.write_timeline_limit.saturating_mul(config.diagnostics.write_timeline_sample_every));
                // Keep caller ids distinct across both pools.
                let caller_id = measured_caller_id(config, "measurement", OperationKind::Put, timeline, config.phases.measurement_operations + puts.next_id);
                reporter.offered("measurement", "put", 1);
                result.put.offered += 1;
                match puts.try_dispatch(work, "measurement", duration_ns(now.saturating_duration_since(origin)), deadline, profile, caller_id) {
                    Ok(()) => {
                        result.put.admitted += 1;
                        profile_selected += usize::from(profile);
                        timeline_selected += usize::from(timeline);
                    }
                    Err(work) => {
                        result.put.rejected += 1;
                        let mut sample = rejected_sample("measurement", &work, duration_ns(now.saturating_duration_since(origin)), origin, "worker_queue_full");
                        sample.stream = Some(Stream::Put);
                        reporter.rejected(sample).await?;
                    }
                }
                continue;
            }
            if now >= admission_end && result.read.offered == planned_reads {
                break;
            }
            let wake = if result.read.offered < planned_reads {
                due
            } else {
                admission_end
            };
            tokio::select! {
                biased;
                () = sleep_until(TokioInstant::from_std(wake.min(deadline))) => {}
                completion = reads.completions.recv(), if !reads.pending.is_empty() => {
                    let completion = completion.ok_or_else(|| invalid("read completion channel closed"))?;
                    observe_stream_completion(&mut result.read, &completion.sample);
                    reads.record_completion(completion, reporter).await?;
                }
                completion = puts.completions.recv(), if !puts.pending.is_empty() => {
                    let completion = completion.ok_or_else(|| invalid("put completion channel closed"))?;
                    observe_stream_completion(&mut result.put, &completion.sample);
                    puts.record_completion(completion, reporter).await?;
                }
                () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                    reporter.snapshot()?;
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                }
            }
        }
        Ok(())
    }.await;
    reporter.set_phase("drain");
    let drain_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
    let mut terminal = scheduled;
    for (pool, counts) in [(&mut reads, &mut result.read), (&mut puts, &mut result.put)] {
        while !pool.pending.is_empty() {
            match timeout_at(TokioInstant::from_std(drain_deadline), pool.receive()).await {
                Ok(Ok(completion)) => {
                    observe_stream_completion(counts, &completion.sample);
                    if let Err(error) = pool.record_completion(completion, reporter).await {
                        terminal = Err(error);
                    }
                }
                other => {
                    terminal = Err(match other {
                        Ok(Err(error)) => error,
                        _ => invalid("interference worker drain deadline expired"),
                    });
                    break;
                }
            }
        }
        counts.failed = counts.admitted.saturating_sub(counts.completed);
        if !pool.pending.is_empty() {
            let _ = pool.record_unknown_pending(reporter).await;
            pool.shutdown(false);
        } else {
            pool.shutdown(true);
        }
    }
    result.measurement_drained = result.read.failed == 0 && result.put.failed == 0;
    result.window_end_ns = duration_ns(origin.elapsed());
    result.measurement_elapsed_ns = result.window_end_ns.saturating_sub(result.window_start_ns);
    (result, terminal)
}

fn observe_stream_completion(counts: &mut StreamCounts, sample: &Sample) {
    counts.completed += 1;
    counts.successful += u64::from(sample.outcome == "success");
    if sample.dispatch_observed {
        counts.first_dispatch_ns = Some(
            counts
                .first_dispatch_ns
                .map_or(sample.dispatched_ns, |ns| ns.min(sample.dispatched_ns)),
        );
        counts.last_completion_ns = Some(
            counts
                .last_completion_ns
                .map_or(sample.completed_ns, |ns| ns.max(sample.completed_ns)),
        );
    }
}

fn export_write_wait_metrics(
    config: &RunConfig,
    output: &Path,
    metrics: &serde_json::Value,
) -> BackendResult<bool> {
    if config.diagnostics.write_timeline_limit == 0 {
        return Ok(false);
    }
    let Some(case_id) = config.case_id.as_deref() else {
        return Ok(false);
    };
    std::fs::write(
        output.join("write_wait_metrics.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "case_id": case_id,
            "enabled": metrics["enabled"],
            "saturated": metrics["saturated"],
            "complete": metrics["complete"],
            "windows": metrics["windows"],
        }))?,
    )?;
    Ok(true)
}

fn client_generators(config: &RunConfig) -> Vec<WorkloadGenerator> {
    // Reserve new-key IDs across every client and phase. This atomic allocates IDs
    // only; oracle tracking is separate. K=1 retains the original seed and stream.
    let next_put_index = Arc::new(AtomicU64::new(0));
    (0..config.execution.effective_client_shards())
        .map(|shard| {
            WorkloadGenerator::new(
                (config.seed ^ 0x6b76_7265_7365_6172)
                    .wrapping_add((shard as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)),
                config.dataset.keys,
                config.workload.mix.clone(),
                config.workload.batch_size,
                config.workload.distribution.clone(),
                config.workload.put_key_policy,
                Arc::clone(&next_put_index),
            )
        })
        .collect()
}

/// Divide a fixed budget without rounding up the aggregate or dropping a remainder.
fn client_budget(total: usize, clients: usize, shard: usize) -> usize {
    total / clients + usize::from(shard < total % clients)
}

struct ClientSchedule {
    epoch: Instant,
    shard: u64,
    clients: u64,
    sweep_cursor: Option<Arc<AtomicU64>>,
}

/// Each client owns its generator, worker pool and report accumulators. Ready/start
/// handshakes happen once per phase, never per operation. All clients drain and join
/// before the caller can pass a phase, telemetry or measurement-completion boundary.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_phase<B: KvBackend>(
    backend: &Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    generators: &mut Vec<WorkloadGenerator>,
    phase: &'static str,
    count: u64,
    origin: Instant,
    deadline: Instant,
    oracle: &Arc<KeyOracle>,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let clients = config.execution.effective_client_shards();
    if clients == 1 {
        let mut pool = WorkerPool::new(backend, config, namespace, origin, oracle)?;
        let schedule = ClientSchedule {
            epoch: Instant::now(),
            shard: 0,
            clients: 1,
            sweep_cursor: None,
        };
        return run_client_phase(
            config,
            &mut generators[0],
            phase,
            count,
            origin,
            deadline,
            reporter,
            &mut pool,
            &schedule,
        )
        .await;
    }
    let handle = tokio::runtime::Handle::current();
    // Owner: parent phase task. Capacity K: one readiness message per client, no
    // per-operation messages. Senders wait; all are dropped after phase startup.
    let (ready_tx, mut ready_rx) = mpsc::channel(clients);
    let sweep_cursor = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::with_capacity(clients);
    let mut first_error = None;
    for (shard, mut generator) in std::mem::take(generators).into_iter().enumerate() {
        let mut client_config = config.clone();
        client_config.execution.concurrency =
            client_budget(config.execution.concurrency, clients, shard);
        client_config.execution.queue_capacity =
            client_budget(config.execution.queue_capacity, clients, shard);
        client_config.diagnostics.write_profile_limit =
            client_budget(config.diagnostics.write_profile_limit, clients, shard);
        client_config.diagnostics.write_timeline_limit =
            client_budget(config.diagnostics.write_timeline_limit, clients, shard);
        let mut client_reporter = reporter.fork_client();
        let mut pool = match WorkerPool::new(backend, &client_config, namespace, origin, oracle) {
            Ok(pool) => pool,
            Err(error) => {
                first_error = Some(error);
                break;
            }
        };
        pool.next_id = shard as u64;
        pool.id_stride = clients as u64;
        // Each start channel holds exactly one phase epoch, then closes. Waiting
        // here occurs only on the dedicated client thread, never an executor thread.
        let (start_tx, start_rx) = std_mpsc::sync_channel(1);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let ready = ready_tx.clone();
        let handle = handle.clone();
        let sweep_cursor = Arc::clone(&sweep_cursor);
        let spawned = thread::Builder::new()
            .name(format!("kv-research-client-{shard}"))
            .spawn(move || {
                let result = if ready.blocking_send(()).is_err() {
                    pool.shutdown(true);
                    Err(invalid("client phase readiness channel closed"))
                } else if let Ok(epoch) = start_rx.recv() {
                    let schedule = ClientSchedule {
                        epoch,
                        shard: shard as u64,
                        clients: clients as u64,
                        sweep_cursor: Some(sweep_cursor),
                    };
                    handle.block_on(run_client_phase(
                        &client_config,
                        &mut generator,
                        phase,
                        count,
                        origin,
                        deadline,
                        &mut client_reporter,
                        &mut pool,
                        &schedule,
                    ))
                } else {
                    pool.shutdown(true);
                    Err(invalid("client phase startup cancelled"))
                };
                let _ = done_tx.send((generator, client_reporter, result));
            });
        match spawned {
            Ok(thread) => threads.push((thread, start_tx, done_rx)),
            Err(error) => {
                first_error = Some(error.into());
                break;
            }
        }
    }
    drop(ready_tx);
    if first_error.is_none() {
        for _ in 0..clients {
            if ready_rx.recv().await.is_none() {
                first_error = Some(invalid("client failed before phase startup"));
                break;
            }
        }
    }
    let epoch = Instant::now();
    for (_, start, _) in &threads {
        if first_error.is_none() && start.send(epoch).is_err() {
            first_error = Some(invalid("client stopped before phase startup"));
        }
    }
    // Dropping every remaining start sender releases clients even on partial startup.
    for (thread, start, mut done) in threads {
        drop(start);
        let result = loop {
            tokio::select! {
                result = &mut done => break result,
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    if let Err(error) = reporter.snapshot() {
                        first_error.get_or_insert(error);
                    }
                }
            }
        };
        if thread.join().is_err() {
            first_error.get_or_insert_with(|| invalid("client scheduling thread panicked"));
        }
        match result {
            Ok((generator, client_reporter, result)) => {
                generators.push(generator);
                reporter.merge_client(client_reporter);
                if let Err(error) = result {
                    first_error.get_or_insert(error);
                }
            }
            Err(_) => {
                first_error.get_or_insert_with(|| invalid("client result channel closed"));
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

// Keep phase state and reporter borrowing explicit at this private boundary.
#[allow(clippy::too_many_arguments)]
// Keep open/closed-loop admission beside their common completion/drain accounting.
#[allow(clippy::too_many_lines)]
// Phase counts are admitted below 100 million, exactly representable in f64.
#[allow(clippy::cast_precision_loss)]
// Match makes the received-event and disconnected-channel paths explicit in select branches.
#[allow(clippy::single_match_else)]
async fn run_client_phase(
    config: &RunConfig,
    generator: &mut WorkloadGenerator,
    phase: &'static str,
    count: u64,
    origin: Instant,
    deadline: Instant,
    reporter: &mut Reporter,
    pool: &mut WorkerPool,
    schedule: &ClientSchedule,
) -> BackendResult<()> {
    let mut scheduled_index = schedule.shard;
    let mut phase_expired = false;
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let mut profiled_selected = 0usize;
    let mut timeline_selected = 0usize;
    let sweep_keys = (phase == "warmup" && config.phases.warmup_key_sweep)
        || (phase == "measurement" && config.phases.measurement_key_sweep);
    let mut sweep_cursor = 0;
    match config.execution.mode {
        ExecutionMode::ClosedLoop | ExecutionMode::Interference => {
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
                                    if let Err(error) = pool.record_completion(completion, reporter).await {
                                        let _ = pool.record_unknown_pending(reporter).await;
                                        pool.shutdown(false);
                                        return Err(error);
                                    }
                                }
                                None => {
                                    let _ = pool.record_unknown_pending(reporter).await;
                                    pool.shutdown(false);
                                    return Err(invalid("worker completion channel closed"));
                                }
                            }
                        }
                    }
                    continue;
                }
                let work = if sweep_keys {
                    client_sweep_item(generator, &mut sweep_cursor, schedule)?
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
                let timeline = is_timeline_candidate(
                    phase,
                    work.operation,
                    timeline_selected,
                    config
                        .diagnostics
                        .write_timeline_limit
                        .saturating_mul(config.diagnostics.write_timeline_sample_every),
                );
                let caller_id =
                    measured_caller_id(config, phase, work.operation, timeline, pool.next_id);
                match pool.try_dispatch(work, phase, scheduled, deadline, profiled_write, caller_id)
                {
                    Ok(()) => {
                        if profiled_write {
                            profiled_selected += 1;
                        }
                        if timeline {
                            timeline_selected += 1;
                        }
                    }
                    Err(work) => {
                        reporter
                            .rejected(rejected_sample(
                                phase,
                                &work,
                                scheduled,
                                origin,
                                "worker_queue_full",
                            ))
                            .await?
                    }
                }
                scheduled_index += schedule.clients;
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
                let due = schedule.epoch + Duration::from_secs_f64(scheduled_index as f64 / rate);
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
                                if let Err(error) = pool.record_completion(completion, reporter).await {
                                    let _ = pool.record_unknown_pending(reporter).await;
                                    pool.shutdown(false);
                                    return Err(error);
                                }
                            }
                            None => {
                                let _ = pool.record_unknown_pending(reporter).await;
                                pool.shutdown(false);
                                return Err(invalid("worker completion channel closed"));
                            }
                        }
                        // Completion is not a new arrival; wait again for this same due time.
                        continue;
                    }
                }
                let work = if sweep_keys {
                    client_sweep_item(generator, &mut sweep_cursor, schedule)?
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
                let timeline = is_timeline_candidate(
                    phase,
                    work.operation,
                    timeline_selected,
                    config
                        .diagnostics
                        .write_timeline_limit
                        .saturating_mul(config.diagnostics.write_timeline_sample_every),
                );
                let caller_id =
                    measured_caller_id(config, phase, work.operation, timeline, pool.next_id);
                let dispatched = if pool.pending.len() >= config.execution.queue_capacity {
                    Err(work)
                } else {
                    pool.try_dispatch(work, phase, scheduled, deadline, profiled_write, caller_id)
                };
                match dispatched {
                    Ok(()) => {
                        if profiled_write {
                            profiled_selected += 1;
                        }
                        if timeline {
                            timeline_selected += 1;
                        }
                    }
                    Err(work) => {
                        reporter
                            .rejected(rejected_sample(
                                phase,
                                &work,
                                scheduled,
                                origin,
                                "admission_capacity",
                            ))
                            .await?
                    }
                }
                if Instant::now() >= next_snapshot {
                    reporter.snapshot()?;
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                }
                scheduled_index += schedule.clients;
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
                    Ok(Ok(completion)) => pool.record_completion(completion, reporter).await?,
                    Ok(Err(error)) => {
                        let _ = pool.record_unknown_pending(reporter).await;
                        pool.shutdown(false);
                        return Err(error);
                    }
                    Err(_) => {
                        let _ = pool.record_unknown_pending(reporter).await;
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

fn client_sweep_item(
    generator: &mut WorkloadGenerator,
    cursor: &mut u64,
    schedule: &ClientSchedule,
) -> BackendResult<WorkItem> {
    if let Some(cursor) = &schedule.sweep_cursor {
        generator.next_shared_sweep_item(cursor)
    } else {
        generator.next_sweep_item(cursor)
    }
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
        stream: None,
        phase,
        operation: work.operation.as_str(),
        outcome: "rejected",
        caller_sample_id: None,
        scheduled_ns: scheduled,
        dispatched_ns: 0,
        dispatch_observed: false,
        completed_ns: completed,
        api_ns: 0,
        response_ns: completed.saturating_sub(scheduled),
        scheduling_ns: 0,
        released_ns: completed,
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
    caller_sample_id: Option<u64>,
    phase: &'static str,
    scheduled: u64,
    origin: Instant,
    deadline: Instant,
    oracle: &KeyOracle,
    dispatch_timestamp: Arc<AtomicU64>,
) -> Sample {
    let operation = work.operation;
    let op = operation.as_str();
    let key_bytes_vec = work
        .keys
        .iter()
        .map(|id| key_bytes(namespace, config.seed, *id))
        .collect::<Vec<_>>();
    let make_value = |id: u64| {
        value_bytes(
            namespace,
            config.seed,
            id,
            config.dataset.value_size,
            config.dataset.entropy,
        )
    };
    let entries = if matches!(
        operation,
        OperationKind::Read | OperationKind::Put | OperationKind::GetMany | OperationKind::PutBatch
    ) {
        work.keys
            .iter()
            .zip(&key_bytes_vec)
            .map(|(id, key)| (key.clone(), make_value(*id)))
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
    // Oracle bookkeeping stays outside the API clock and inside the worker-release clock.
    let tracking = oracle.tracks_mutations();
    let read_marks: Vec<ReadMark> =
        if tracking && matches!(operation, OperationKind::Read | OperationKind::GetMany) {
            work.keys.iter().map(|key| oracle.snapshot(*key)).collect()
        } else {
            Vec::new()
        };
    // A key repeated inside one batch is one in-flight put, so a key's count never exceeds the
    // number of concurrently executing operations (see `KeyOracle::ensure_capacity`).
    let put_keys: Vec<u64> =
        if tracking && matches!(operation, OperationKind::Put | OperationKind::PutBatch) {
            let mut keys = work.keys.clone();
            keys.sort_unstable();
            keys.dedup();
            keys
        } else {
            Vec::new()
        };
    let put_tickets: Vec<PutTicket> = put_keys.iter().map(|key| oracle.begin_put(*key)).collect();
    if tracking && operation == OperationKind::Delete {
        oracle.begin_delete(work.keys[0]);
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
        match (operation, caller_sample_id) {
            (OperationKind::Read | OperationKind::MissingRead, Some(id)) => backend
                .traced_get(key_bytes_vec[0].as_ref(), id)
                .await
                .map(BackendResponse::Read),
            (OperationKind::Read | OperationKind::MissingRead, None) => backend
                .get(key_bytes_vec[0].as_ref())
                .await
                .map(BackendResponse::Read),
            (OperationKind::Put, Some(id)) => backend
                .traced_put(key_bytes_vec[0].clone(), entries[0].1.clone(), id)
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            (OperationKind::Delete, Some(id)) => backend
                .traced_delete(key_bytes_vec[0].clone(), id)
                .await
                .map(|()| BackendResponse::Mutation(0)),
            (OperationKind::PutBatch, Some(id)) => backend
                .traced_put_batch(&entries, id)
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            (OperationKind::Put, None) if profiled_write => backend
                .profiled_put(key_bytes_vec[0].clone(), entries[0].1.clone())
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            (OperationKind::Put, None) => backend
                .put(key_bytes_vec[0].clone(), entries[0].1.clone())
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            (OperationKind::Delete, None) if profiled_write => backend
                .profiled_delete(key_bytes_vec[0].clone())
                .await
                .map(|()| BackendResponse::Mutation(0)),
            (OperationKind::Delete, None) => backend
                .delete(key_bytes_vec[0].clone())
                .await
                .map(|()| BackendResponse::Mutation(0)),
            (OperationKind::GetMany, Some(id)) => backend
                .traced_get_many(&key_bytes_vec, id)
                .await
                .map(BackendResponse::Many),
            (OperationKind::GetMany, None) => backend
                .get_many(&key_bytes_vec)
                .await
                .map(BackendResponse::Many),
            (OperationKind::PutBatch, None) => backend
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
    if tracking {
        let acknowledged = matches!(result, Ok(Ok(_)));
        match operation {
            OperationKind::Delete => oracle.end_delete(work.keys[0], acknowledged),
            OperationKind::Put | OperationKind::PutBatch => {
                for (key, ticket) in put_keys.iter().zip(&put_tickets) {
                    oracle.end_put(*key, *ticket, acknowledged);
                }
            }
            _ => {}
        }
    }
    let mut sample = match result {
        Ok(Ok(response)) => {
            let (returned_bytes, outcome, error_kind) = match response {
                BackendResponse::Read(value) => {
                    let absent = value.is_none();
                    let valid = operation == OperationKind::MissingRead
                        || match value.as_ref() {
                            Some(actual) => actual.as_ref() == entries[0].1.as_ref(),
                            None => oracle.absence_allowed(
                                work.keys[0],
                                read_marks.first().copied().unwrap_or_default(),
                            ),
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
                    } else if value.is_none() {
                        "read returned no value and no completed or overlapping delete explains it"
                            .to_owned()
                    } else {
                        "read bytes differ from deterministic payload".to_owned()
                    };
                    (returned_bytes, outcome, error)
                }
                BackendResponse::Many(values) => {
                    let valid_count = values.len() == work.keys.len();
                    let valid_values = values.iter().zip(&entries).enumerate().all(
                        |(index, (actual, expected))| match actual.as_ref() {
                            Some(value) => value.as_ref() == expected.1.as_ref(),
                            None => oracle.absence_allowed(
                                work.keys[index],
                                read_marks.get(index).copied().unwrap_or_default(),
                            ),
                        },
                    );
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
    };
    sample.released_ns = duration_ns(origin.elapsed()).max(sample.completed_ns);
    sample
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
        stream: None,
        phase,
        operation,
        outcome,
        caller_sample_id: None,
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
        released_ns: completed_ns,
        logical_keys,
        bytes,
        error_kind,
    }
}

// Per-key state words, `delete` and `puts`. `delete`: [63] uncertain | [62] maybe_absent |
// [20..52) delete epoch | [0..20) deletes in flight. `puts`: [20..52) put epoch | [0..20) puts
// in flight. An in-flight field holds at most `queue_capacity` (<= 65536) and an epoch counts at
// most `max_operations` operations, so no field can carry into its neighbour.
const INFLIGHT_MASK: u64 = (1 << 20) - 1;
const EPOCH_SHIFT: u32 = 20;
const EPOCH_MASK: u64 = (1 << 32) - 1;
const EPOCH_UNIT: u64 = 1 << EPOCH_SHIFT;
const MAYBE_ABSENT: u64 = 1 << 62;
const UNCERTAIN: u64 = 1 << 63;
/// Failing to clear `MAYBE_ABSENT` is always safe (it only widens what a read may report).
const CLEAR_ATTEMPTS: usize = 16;

fn inflight(word: u64) -> u64 {
    word & INFLIGHT_MASK
}

fn epoch(word: u64) -> u64 {
    (word >> EPOCH_SHIFT) & EPOCH_MASK
}

/// Delete state observed when a put started; see `KeyOracle::end_put`.
#[derive(Clone, Copy, Default)]
struct PutTicket {
    epoch: u64,
    overlapped_delete: bool,
}

/// Both state words of one key, captured immediately before a read is dispatched.
#[derive(Clone, Copy, Default)]
struct ReadMark {
    delete: u64,
    puts: u64,
}

struct KeyState {
    delete: AtomicU64,
    puts: AtomicU64,
}

/// Concurrency-aware presence oracle for the deterministic dataset.
///
/// Every put of a logical key writes the same bytes, so presence is the only mutable fact.
/// Keys are shared by all workers, so each key owns two atomic words (share-nothing between
/// keys, no lock on any path). A key may be reported absent only when a mutation explains it:
///
/// * `MAYBE_ABSENT` is set when a delete starts and cleared only by an acknowledged put whose
///   whole interval overlapped no delete, so a completed delete explains absence until the key
///   is provably rewritten;
/// * a read that overlapped a delete or a put of the same key (in flight at either end, or one
///   that started during the read) may also see absence.
///
/// A delete with an unknown outcome (error, timeout) may still land later, so it sets the sticky
/// `UNCERTAIN` bit and the flag is never cleared for that key again. A put with an unknown outcome
/// never clears the flag. Present values are always compared exactly by the caller. This is not a
/// linearizability check.
///
/// All operations use `SeqCst`: the argument is about real-time order across threads and an
/// external store, per-key operations are rare against I/O cost, and a single total order keeps
/// the proof simple. The fences pin the reads of `before`/`after` around the backend call.
struct KeyOracle {
    /// `false`: no baseline dataset exists, so any key may legitimately be absent.
    checked: bool,
    /// Empty unless the mix contains a mutation; then one state per logical key.
    states: Box<[KeyState]>,
}

impl KeyOracle {
    fn new(checked: bool, keys: u64, tracks_mutations: bool) -> BackendResult<Self> {
        let len = if checked && tracks_mutations {
            usize::try_from(keys)?
        } else {
            0
        };
        Ok(Self {
            checked,
            states: (0..len)
                .map(|_| KeyState {
                    delete: AtomicU64::new(0),
                    puts: AtomicU64::new(0),
                })
                .collect(),
        })
    }

    fn tracks_mutations(&self) -> bool {
        !self.states.is_empty()
    }

    /// Each worker thread executes one operation at a time and an operation counts a key once, so
    /// the in-flight count of one key never exceeds the worker count. That must fit its field.
    fn ensure_capacity(&self, concurrent_operations: usize) -> BackendResult<()> {
        if u64::try_from(concurrent_operations)? >= INFLIGHT_MASK {
            return Err(invalid(
                "execution.concurrency exceeds the read oracle's per-key in-flight capacity",
            ));
        }
        Ok(())
    }

    fn state(&self, key: u64) -> Option<&KeyState> {
        usize::try_from(key)
            .ok()
            .and_then(|index| self.states.get(index))
    }

    fn begin_delete(&self, key: u64) {
        if let Some(state) = self.state(key) {
            // In-flight and epoch first: from here a concurrent put cannot clear the flag.
            state.delete.fetch_add(EPOCH_UNIT + 1, Ordering::SeqCst);
            state.delete.fetch_or(MAYBE_ABSENT, Ordering::SeqCst);
        }
    }

    fn end_delete(&self, key: u64, acknowledged: bool) {
        if let Some(state) = self.state(key) {
            if !acknowledged {
                // Set before the in-flight decrement so no window shows a finished, certain delete.
                state.delete.fetch_or(UNCERTAIN, Ordering::SeqCst);
            }
            state.delete.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn begin_put(&self, key: u64) -> PutTicket {
        self.state(key).map_or_else(PutTicket::default, |state| {
            // Publish the put before sampling deletes so a read sees it in flight.
            state.puts.fetch_add(EPOCH_UNIT + 1, Ordering::SeqCst);
            let word = state.delete.load(Ordering::SeqCst);
            PutTicket {
                epoch: epoch(word),
                overlapped_delete: inflight(word) != 0,
            }
        })
    }

    /// Every put leaves the in-flight set when it returns. An acknowledged put clears
    /// `MAYBE_ABSENT` only if no delete overlapped its interval: none in flight at its start,
    /// none started since, none in flight now, none uncertain. An overlapping delete may have
    /// taken effect after the put, so absence stays allowed.
    fn end_put(&self, key: u64, ticket: PutTicket, acknowledged: bool) {
        let Some(state) = self.state(key) else {
            return;
        };
        state.puts.fetch_sub(1, Ordering::SeqCst);
        if !acknowledged || ticket.overlapped_delete {
            return;
        }
        let mut word = state.delete.load(Ordering::SeqCst);
        for _ in 0..CLEAR_ATTEMPTS {
            if word & MAYBE_ABSENT == 0
                || word & UNCERTAIN != 0
                || inflight(word) != 0
                || epoch(word) != ticket.epoch
            {
                return;
            }
            match state.delete.compare_exchange_weak(
                word,
                word & !MAYBE_ABSENT,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(current) => word = current,
            }
        }
    }

    /// State to capture immediately before a read is dispatched; default when untracked.
    fn snapshot(&self, key: u64) -> ReadMark {
        self.state(key).map_or_else(ReadMark::default, |state| {
            fence(Ordering::SeqCst);
            ReadMark {
                delete: state.delete.load(Ordering::SeqCst),
                puts: state.puts.load(Ordering::SeqCst),
            }
        })
    }

    /// Whether a read that started at `before` and has returned may report `key` absent.
    fn absence_allowed(&self, key: u64, before: ReadMark) -> bool {
        if !self.checked {
            return true;
        }
        let Some(state) = self.state(key) else {
            // Baseline present and no mutation in the mix: absence is always a lost key.
            return false;
        };
        fence(Ordering::SeqCst);
        let delete = state.delete.load(Ordering::SeqCst);
        let puts = state.puts.load(Ordering::SeqCst);
        before.delete & MAYBE_ABSENT != 0
            || delete & MAYBE_ABSENT != 0
            || inflight(before.delete) != 0
            || inflight(delete) != 0
            || epoch(before.delete) != epoch(delete)
            || inflight(before.puts) != 0
            || inflight(puts) != 0
            || epoch(before.puts) != epoch(puts)
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
pub(crate) fn binary_identity() -> serde_json::Value {
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
mod tests;
