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

    // The scheduler feeds every worker and keeps this counter across all workload phases.
    let next_put_index = Arc::new(AtomicU64::new(0));
    let mut generator = WorkloadGenerator::new(
        config.seed ^ 0x6b76_7265_7365_6172,
        config.dataset.keys,
        config.workload.mix.clone(),
        config.workload.batch_size,
        config.workload.distribution.clone(),
        config.workload.put_key_policy,
        next_put_index,
    );
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
            &mut generator,
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
            match backend.arm_write_timeline(config.diagnostics.write_timeline_limit) {
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
            reporter.set_phase("measurement");
            let phase_result = if config.execution.mode == ExecutionMode::Interference {
                let (result, phase_result) = run_interference_phase(
                    &backend,
                    &config,
                    &namespace,
                    &mut generator,
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
                    &mut generator,
                    "measurement",
                    config.phases.measurement_operations,
                    started,
                    deadline,
                    &oracle,
                    &mut reporter,
                )
                .await
            };
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

fn nvmeof_reader_index(
    snapshot: &serde_json::Value,
) -> Option<std::collections::BTreeMap<String, &serde_json::Value>> {
    if snapshot["available"] != true {
        return None;
    }
    let mut readers = std::collections::BTreeMap::new();
    for reader in snapshot["readers"].as_array()? {
        let bytes = |name: &str| {
            let values = reader.get(name)?.as_array()?;
            (values.len() == 16 && values.iter().all(|v| v.as_u64().is_some_and(|n| n <= 255)))
                .then_some(values)
        };
        let database = bytes("database_uuid")?;
        let export = bytes("export_uuid")?;
        if export.iter().all(|v| v.as_u64() == Some(0)) {
            return None;
        }
        let epoch = reader.get("epoch")?.as_u64().filter(|epoch| *epoch > 0)?;
        let partition = u32::try_from(reader.get("partition")?.as_u64()?).ok()?;
        let identity = serde_json::json!([database, epoch, partition, export]).to_string();
        if readers.insert(identity, reader).is_some() {
            return None;
        }
    }
    (!readers.is_empty()).then_some(readers)
}

fn nvmeof_deltas(start: &serde_json::Value, end: &serde_json::Value) -> serde_json::Value {
    const COUNTERS: &[&str] = &[
        "logical_requests",
        "logical_members",
        "eligible_hits",
        "ineligible_fallbacks",
        "aligned_requested_bytes",
        "aligned_read_bytes",
        "io_submissions",
        "io_completions",
        "io_errors",
        "io_timeouts",
        "admitted_sqes",
        "merged_members",
        "pool_hits",
        "pool_misses",
        "pool_recycled",
        "pool_discarded",
    ];
    const STAGES: &[&str] = &[
        "alignment",
        "admission",
        "queue",
        "submit_to_cq",
        "extraction",
        "verification",
        "lock_wait",
        "lock_hold",
    ];
    let (Some(a), Some(b)) = (nvmeof_reader_index(start), nvmeof_reader_index(end)) else {
        return serde_json::json!({"available":false,"error":"NVMe-oF reader identity or boundary unavailable"});
    };
    if a.keys().ne(b.keys()) {
        return serde_json::json!({"available":false,"error":"NVMe-oF reader set changed during measurement"});
    }
    let mut readers = serde_json::Map::new();
    let mut available = true;
    for (identity, first) in a {
        let last = b[&identity];
        let counters = nvmeof_counter_delta(first, last, COUNTERS);
        let mut reader_available =
            counters["available"] == true && first.get("transport") == last.get("transport");
        let mut stages = serde_json::Map::new();
        for stage in STAGES {
            let mut delta = nvmeof_counter_delta(&first[stage], &last[stage], &["count", "sum_ns"]);
            let max = first[stage]
                .get("max_ns")
                .and_then(serde_json::Value::as_u64)
                .zip(
                    last[stage]
                        .get("max_ns")
                        .and_then(serde_json::Value::as_u64),
                );
            if max.is_none_or(|(a, b)| b < a || b == u64::MAX) {
                delta = serde_json::json!({"available":false,"error":"missing, decreased or saturated lifetime maximum"});
            }
            let values = delta.get("counters");
            let count = values
                .and_then(|v| v.get("count"))
                .and_then(serde_json::Value::as_u64);
            let sum = values
                .and_then(|v| v.get("sum_ns"))
                .and_then(serde_json::Value::as_u64);
            if count == Some(0) && sum.is_some_and(|n| n != 0) {
                delta = serde_json::json!({"available":false,"error":"nonzero duration without observations"});
            }
            let mean = if delta["available"] == true {
                count
                    .zip(sum)
                    .and_then(|(count, sum)| (count != 0).then_some(sum as f64 / count as f64))
            } else {
                None
            };
            reader_available &= delta["available"] == true;
            stages.insert(
                (*stage).to_owned(),
                serde_json::json!({
                    "delta": delta,
                    "mean_ns": mean,
                    "measurement_max_ns": null,
                    "lifetime_max_ns_at_end": max.map(|(_, last)| last)
                }),
            );
        }
        available &= reader_available;
        readers.insert(
            identity,
            serde_json::json!({
                "available": reader_available, "identity": {
                    "database_uuid":first["database_uuid"], "epoch":first["epoch"],
                    "partition":first["partition"], "export_uuid":first["export_uuid"],
                    "transport":first["transport"]
                }, "counters":counters,"stages":stages
            }),
        );
    }
    serde_json::json!({
        "available":available,"active_readers":readers.len(),"readers":readers,
        "timing_scope":"submit_to_cq includes host kernel, transport, target and storage; overlapping physical reads cannot be added into API latency",
        "cold_storage_limit":"O_DIRECT bypasses initiator page cache; buffered targets may serve Linux page-cache hits; device activity does not prove NAND access",
        "max_semantics":"lifetime maxima are not measurement-window maxima or percentile distributions"
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
        "read_profile_live_inline",
        "read_profile_live_inline_fallback_drain",
        "read_profile_live_inline_fallback_busy",
        "read_profile_live_inline_fallback_grant",
        "read_profile_live_inline_fallback_credit",
        "read_profile_live_inline_fallback_paused",
        "read_profile_live_inline_fallback_coalesced",
    ];
    const OWNER: &[&str] = &[
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
        "idle_buffer_evictions",
        "device_credit_lock.acquisitions",
        "device_credit_lock.contended",
        "device_credit_lock.wait_ns",
        "device_credit_lock.hold_ns",
        "pool_registry_lock.acquisitions",
        "pool_registry_lock.contended",
        "pool_registry_lock.wait_ns",
        "pool_registry_lock.hold_ns",
        "transport.send_size_le_4k",
        "transport.send_size_le_16k",
        "transport.send_size_le_64k",
        "transport.send_size_gt_64k",
        "transport.send_post_eagain",
        "transport.send_post_retries",
        "transport.cq_idle_waits",
    ];
    let deltas = if attempted && drained && start["available"] == true && end["available"] == true {
        serde_json::json!({
            "coordinator": counter_delta(&start["coordinator"], &end["coordinator"], COORDINATOR),
            "owner_executor": partition_deltas(&start["owner_executor"], &end["owner_executor"], OWNER)
        })
    } else {
        serde_json::json!({"available": false, "error": if !attempted { "measurement not attempted" }
            else if !drained { "measurement did not complete and drain successfully" }
            else { "backend observation unavailable at a boundary" }})
    };
    let nvmeof = if attempted && drained {
        nvmeof_deltas(&start["nvmeof"], &end["nvmeof"])
    } else {
        serde_json::json!({"available":false,"error":"measurement not attempted or not drained"})
    };
    let live_nvmeof = if attempted && drained {
        nvmeof_counter_delta(
            &start["live_nvmeof"],
            &end["live_nvmeof"],
            &[
                "completed_read_ops",
                "completed_read_bytes",
                "completed_write_ops",
                "completed_write_bytes",
                "recovery_read_ops",
                "recovery_read_bytes",
                "metadata_requests",
                "namespace_connects",
                "payload_rpc_requests",
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
                "allocation_ns",
                "allocation_samples",
                "publication_ns",
                "publication_samples",
                "namespace_setup_ns",
                "namespace_setup_samples",
                "read_io_ns",
                "read_io_samples",
                "persistence_ns",
                "persistence_samples",
                "owner_completion_ns",
                "owner_completion_samples",
                "wal_persistence_ns",
                "wal_persistence_samples",
                "wal_namespace_ns",
                "wal_namespace_samples",
                "frontier_file_ns",
                "frontier_file_samples",
                "frontier_namespace_ns",
                "frontier_namespace_samples",
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
                "reserve_exchanges",
                "complete_exchanges",
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
            ],
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
    let live_peaks = [
        "lane_inflight_request_max",
        "lane_inflight_op_max",
        "max_leased_in_flight",
    ]
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
        "start": start, "end": end, "deltas": deltas, "nvmeof":nvmeof, "live_nvmeof":live_nvmeof,
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
                let timeline = is_timeline_candidate("measurement", OperationKind::Put, timeline_selected, config.diagnostics.write_timeline_limit);
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

// Keep phase state, independent clocks and reporter borrowing explicit at this private boundary.
#[allow(clippy::too_many_arguments)]
// Keep open/closed-loop admission beside their common completion/drain accounting.
#[allow(clippy::too_many_lines)]
// Phase counts are admitted below 100 million, exactly representable in f64.
#[allow(clippy::cast_precision_loss)]
// Match makes the received-event and disconnected-channel paths explicit in select branches.
#[allow(clippy::single_match_else)]
async fn run_phase<B: KvBackend>(
    backend: &Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    generator: &mut WorkloadGenerator,
    phase: &'static str,
    count: u64,
    origin: Instant,
    deadline: Instant,
    oracle: &Arc<KeyOracle>,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let mut pool = WorkerPool::new(backend, config, namespace, origin, oracle)?;
    let phase_start = Instant::now();
    let mut scheduled_index = 0_u64;
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
                let timeline = is_timeline_candidate(
                    phase,
                    work.operation,
                    timeline_selected,
                    config.diagnostics.write_timeline_limit,
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
                let timeline = is_timeline_candidate(
                    phase,
                    work.operation,
                    timeline_selected,
                    config.diagnostics.write_timeline_limit,
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
    #[tokio::test]
    async fn native_group_barrier_receipt_has_synchronous_cohort() {
        let directory = result_directory("native-group-barrier-receipt");
        let mut config = read_test_config();
        config.backend.kind = "native".to_owned();
        config.backend.options = serde_json::json!({
            "path": directory.join("data"),
            "durability": "value_wal_group_barrier",
            "native_wal_geometry": "small",
        });
        let backend =
            crate::backends::native::open(&config.backend.options, &prometheus::Registry::new())
                .await
                .unwrap();
        let receipt = durability_receipt(&config, &backend.info());
        assert_eq!(receipt["system"], "native");
        assert_eq!(receipt["tier"], "value_wal_group_barrier");
        assert_eq!(
            receipt["comparison_cohort"],
            DurabilityTier::Synchronous.comparison_cohort()
        );
        backend.shutdown().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn external_acknowledgement_receipts_preserve_defaults_and_classify_relaxed_contracts() {
        let mut config = read_test_config();
        for name in ["valkey_tcp", "valkey_rdma", "kvrocks", "garnet"] {
            config.backend.kind = name.into();
            let mut info = BackendInfo {
                name: name.into(),
                server_version: String::new(),
                client_version: String::new(),
                acknowledgement: String::new(),
                consistency: String::new(),
                replication_factor: 1,
                measurement_boundary: String::new(),
                get_many_strategy: String::new(),
                batch_atomicity: crate::backend::BatchAtomicity::Unsupported,
                topology: serde_json::json!({}),
                configuration: serde_json::json!({}),
            };
            let expected = if name.starts_with("valkey") {
                serde_json::json!({"system":name,"tier":"appendfsync_always",
                    "comparison_cohort":DurabilityTier::Synchronous.comparison_cohort()})
            } else {
                serde_json::json!({"system":name,"tier":"unclassified","comparison_cohort":"none"})
            };
            assert_eq!(durability_receipt(&config, &info), expected);
            info.configuration["ack_contract"] = serde_json::json!("synced");
            assert_eq!(durability_receipt(&config, &info), expected);
            let cases = if name.starts_with("valkey") {
                vec![
                    ("os_buffered", "appendfsync_no", "os_buffered"),
                    ("periodic_sync", "appendfsync_everysec", "periodic_sync"),
                ]
            } else if name == "kvrocks" {
                vec![(
                    "page_cache",
                    "wal_sync_no",
                    DurabilityTier::Native.comparison_cohort(),
                )]
            } else {
                Vec::new()
            };
            for (contract, tier, cohort) in cases {
                info.configuration["ack_contract"] = serde_json::json!(contract);
                assert_eq!(
                    durability_receipt(&config, &info),
                    serde_json::json!({
                        "system":name,"tier":tier,"comparison_cohort":cohort,
                        "confirmed_by_backend_receipt":contract,
                    })
                );
            }
        }
    }

    #[tokio::test]
    async fn new_put_keys_reach_all_workers_across_phases_and_manifest() {
        for batch in [false, true] {
            let directory = result_directory(if batch { "new-batch" } else { "new-put" });
            let output = directory.join("result");
            let mut config = read_test_config();
            let namespace = [3; 16];
            config.key_namespace_hex = Some(hex(&namespace));
            config.workload.put_key_policy = crate::config::PutKeyPolicy::New;
            config.workload.mix = crate::config::OperationMix {
                put: u32::from(!batch),
                put_batch: u32::from(batch),
                ..crate::config::OperationMix::default()
            };
            config.execution.concurrency = 4;
            config.execution.queue_capacity = 4;
            config.phases.preload = true;
            config.phases.warmup_operations = 4;
            config.phases.aging_operations = 4;
            config.phases.measurement_operations = 8;
            let expected_keys = config.dataset.keys + 16 * if batch { 2 } else { 1 };
            let backend = MemoryBackend::new(false, false);
            let data = Arc::clone(&backend.data);
            run(
                backend,
                config.clone(),
                &output,
                prometheus::Registry::new(),
            )
            .await
            .unwrap();
            {
                let data = data.lock().unwrap();
                assert_eq!(u64::try_from(data.len()).unwrap(), expected_keys);
                for id in 0..expected_keys {
                    assert_eq!(
                        data.get(key_bytes(&namespace, config.seed, id).as_ref()),
                        Some(&value_bytes(
                            &namespace,
                            config.seed,
                            id,
                            config.dataset.value_size,
                            config.dataset.entropy,
                        ))
                    );
                }
            }
            let manifest: serde_json::Value =
                serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap())
                    .unwrap();
            assert_eq!(
                manifest["run"]["config"]["workload"]["put_key_policy"],
                "new"
            );
            assert_eq!(manifest["run"]["data_policy"]["put_key_policy"], "new");
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn nested_lock_counters_are_differenced_and_a_missing_path_is_unavailable() {
        let at =
            |wait: u64| serde_json::json!({"device_credit_lock": {"wait_ns": wait, "hold_ns": 1}});
        let delta = counter_delta(&at(10), &at(25), &["device_credit_lock.wait_ns"]);
        assert_eq!(delta["counters"]["device_credit_lock.wait_ns"], 15);
        let missing = counter_delta(&at(10), &at(25), &["pool_registry_lock.wait_ns"]);
        assert_eq!(missing["available"], false);
        let reset = counter_delta(&at(25), &at(10), &["device_credit_lock.wait_ns"]);
        assert_eq!(reset["available"], false);
    }

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

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    #[tokio::test]
    async fn mapped_wal_eviction_precedes_measurement_and_failure_blocks_gate() {
        for supported in [true, false] {
            let directory = result_directory(if supported { "evict-ok" } else { "evict-fail" });
            let output = directory.join("result");
            let mut config = read_test_config();
            config.backend.kind = "native".into();
            config.phases.warmup_operations = 3;
            config.phases.measurement_operations = 2;
            config.phases.evict_mapped_wal_before_measurement = true;
            if !supported {
                config.measurement_gate = Some(crate::config::MeasurementGateConfig {
                    ready_file: directory.join("ready"),
                    release_file: directory.join("release"),
                    timeout_seconds: 1,
                });
            }
            let mut backend = MemoryBackend::new(false, false);
            backend.eviction_supported = supported;
            let result = run(backend, config, &output, prometheus::Registry::new()).await;
            let eviction: serde_json::Value = serde_json::from_slice(
                &std::fs::read(output.join("pre-measurement-wal-eviction.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(eviction["available"], supported);
            if supported {
                result.unwrap();
                let evidence: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    eviction["evidence"]["reads_before"],
                    evidence["start"]["coordinator"]["hits"]
                );
                assert_eq!(measurement_outcomes(&output).len(), 2);
            } else {
                assert!(result.is_err());
                assert!(!directory.join("ready").exists());
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
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

    #[tokio::test]
    async fn failed_read_boundaries_fail_case_and_preserve_terminal_evidence() {
        for boundary in [0, 1] {
            let directory = result_directory(&format!("read-boundary-error-{boundary}"));
            let output = directory.join("result");
            let mut backend = MemoryBackend::new(false, false);
            backend.observation_failure_at = Some(boundary);
            let mut config = read_test_config();
            config.phases.preload = true;
            config.phases.measurement_operations = 2;
            let error = run(backend, config, &output, prometheus::Registry::new())
                .await
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains(if boundary == 0 {
                "measurement start boundary failed"
            } else {
                "measurement end boundary failed"
            }));
            assert!(error.contains("adoption boundary did not quiesce"));
            let evidence: serde_json::Value = serde_json::from_slice(
                &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(evidence["measurement_attempted"], boundary == 1);
            assert_eq!(evidence["measurement_drained"], false);
            assert!(output.join("manifest.json").exists());
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    struct MemoryBackend {
        data: Arc<Mutex<HashMap<Vec<u8>, Bytes>>>,
        corrupt_reads: bool,
        reverse_groups: bool,
        omit_put_number: Option<usize>,
        put_count: AtomicUsize,
        read_count: AtomicUsize,
        profile_events: Option<Arc<Mutex<Vec<&'static str>>>>,
        profile_limit: AtomicUsize,
        write_gate_counter: Option<Arc<AtomicUsize>>,
        write_armed: AtomicUsize,
        write_snapshot_calls: Arc<AtomicUsize>,
        write_finish_calls: Arc<AtomicUsize>,
        write_snapshot_error: bool,
        write_snapshot_boundary_error: bool,
        observation_failure_at: Option<usize>,
        observation_calls: AtomicUsize,
        eviction_supported: bool,
    }

    impl MemoryBackend {
        fn new(corrupt_reads: bool, reverse_groups: bool) -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
                corrupt_reads,
                reverse_groups,
                omit_put_number: None,
                put_count: AtomicUsize::new(0),
                read_count: AtomicUsize::new(0),
                profile_limit: AtomicUsize::new(0),
                profile_events: None,
                eviction_supported: false,
                write_gate_counter: None,
                write_armed: AtomicUsize::new(0),
                write_snapshot_calls: Arc::new(AtomicUsize::new(0)),
                write_finish_calls: Arc::new(AtomicUsize::new(0)),
                write_snapshot_error: false,
                write_snapshot_boundary_error: false,
                observation_failure_at: None,
                observation_calls: AtomicUsize::new(0),
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
                // Stands in for the Tidehunter adapter in tests that select that backend kind.
                configuration: serde_json::json!({"durability_tier": "native"}),
            }
        }
        fn read_observation(&self) -> BackendResult<serde_json::Value> {
            let boundary = self.observation_calls.fetch_add(1, Ordering::Relaxed);
            if self.observation_failure_at == Some(boundary) {
                return Err(invalid("leased adoption boundary did not quiesce"));
            }
            let reads = self.read_count.load(Ordering::Relaxed);
            Ok(serde_json::json!({"available": true, "coordinator": {
                "read_rpc_count": 0, "hits": reads, "misses": 0, "inserts": 0,
                "evictions": 0, "rejected_oversize": 0, "rejected_mismatch": 0,
                "bypass_disabled": 0, "bypass_admission": 0, "bypass_eviction_limit": 0,
                "admitted_bytes": 0, "served_bytes": 0,
                "read_profile_live_inline": 0,
                "read_profile_live_inline_fallback_drain": 0,
                "read_profile_live_inline_fallback_busy": 0,
                "read_profile_live_inline_fallback_grant": 0,
                "read_profile_live_inline_fallback_credit": 0,
                "read_profile_live_inline_fallback_paused": 0,
                "read_profile_live_inline_fallback_coalesced": 0},
                "owner_executor": {"available": false, "error": "test unavailable"},
                "client_resident": {"available": false, "error": "test unavailable"}}))
        }
        fn arm_write_profile(&self, _limit: usize) -> BackendResult<()> {
            if self.write_gate_counter.is_none() {
                return Err(invalid("write profile unsupported"));
            }
            self.write_armed.store(1, Ordering::Relaxed);
            Ok(())
        }

        fn snapshot_write_profile(&self) -> BackendResult<serde_json::Value> {
            self.write_snapshot_calls.fetch_add(1, Ordering::Relaxed);
            if self.write_armed.load(Ordering::Relaxed) != 1 {
                return Err(invalid("write profile is not armed"));
            }
            if self.write_snapshot_error {
                if self.write_snapshot_boundary_error {
                    return Err(Box::new(crate::backend::LiveAdoptionBoundaryError(
                        "snapshot unavailable",
                    )));
                }
                return Err(invalid("snapshot unavailable"));
            }
            let counter = self
                .write_gate_counter
                .as_ref()
                .ok_or_else(|| invalid("write profile unsupported"))?;
            Ok(serde_json::json!({"available": true, "value": counter.load(Ordering::Relaxed)}))
        }

        async fn finish_write_profile(&self) -> BackendResult<serde_json::Value> {
            self.write_finish_calls.fetch_add(1, Ordering::Relaxed);
            if self.write_armed.swap(0, Ordering::Relaxed) != 1 {
                return Err(invalid("write profile was disabled before finish"));
            }
            let counter = self
                .write_gate_counter
                .as_ref()
                .ok_or_else(|| invalid("write profile unsupported"))?;
            Ok(
                serde_json::json!({"selected_operations": 1, "post_gate_value": counter.load(Ordering::Relaxed)}),
            )
        }

        fn evict_mapped_wal_before_measurement(&self) -> BackendResult<serde_json::Value> {
            if !self.eviction_supported {
                return Err(invalid("eviction unsupported"));
            }
            Ok(serde_json::json!({"reads_before":self.read_count.load(Ordering::Relaxed)}))
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
    #[derive(Default)]
    struct TimelineTestState {
        armed: bool,
        arms: usize,
        finishes: usize,
        finish_gate: Option<std_mpsc::Receiver<()>>,
        finish_started: Option<std_mpsc::Sender<()>>,
        mutation_gate: Option<std_mpsc::Receiver<()>>,
        partial_owner_export: bool,
        write_wait_metrics: Option<serde_json::Value>,
        calls: Vec<(&'static str, Option<u64>, bool)>,
    }

    struct TimelineTestBackend {
        inner: MemoryBackend,
        state: Arc<Mutex<TimelineTestState>>,
    }

    impl TimelineTestBackend {
        fn record(&self, operation: &'static str, id: Option<u64>) {
            let mut state = self.state.lock().unwrap();
            let armed = state.armed;
            state.calls.push((operation, id, armed));
        }
    }

    impl KvBackend for TimelineTestBackend {
        fn info(&self) -> BackendInfo {
            self.inner.info()
        }

        fn arm_write_timeline(&self, limit: usize) -> BackendResult<()> {
            assert!(limit > 0);
            let mut state = self.state.lock().unwrap();
            assert!(!state.armed);
            state.armed = true;
            state.arms += 1;
            Ok(())
        }

        fn finish_write_timeline(&self, _deadline: Instant) -> BackendResult<serde_json::Value> {
            let (gate, started) = {
                let mut state = self.state.lock().unwrap();
                (state.finish_gate.take(), state.finish_started.take())
            };
            if let Some(started) = started {
                let _ = started.send(());
            }
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
            let mut state = self.state.lock().unwrap();
            assert!(state.armed);
            state.armed = false;
            state.finishes += 1;
            let records: Vec<_> = state
                .calls
                .iter()
                .filter_map(|(operation, id, _)| {
                    id.map(|id| serde_json::json!({"caller_sample_id": id, "operation": operation}))
                })
                .collect();
            let errors = if state.partial_owner_export {
                serde_json::json!([{"owner": 2, "phase": "fetch", "message": "test fetch failure", "truncated": false}])
            } else {
                serde_json::json!([])
            };
            let mut value = serde_json::json!({
                "records": records,
                "owner_records": [{"owner": 1, "envelope_id": 10}],
                "owner_errors": errors,
                "owner_expected_connections": 2,
                "owner_captured_connections": if state.partial_owner_export { 1 } else { 2 }
            });
            if let Some(metrics) = &state.write_wait_metrics {
                value["write_wait_metrics"] = metrics.clone();
            }
            Ok(value)
        }

        async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
            self.inner.get_many(keys).await
        }

        async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
            self.record("put", None);
            self.inner.put(key, value).await
        }

        async fn delete(&self, key: Bytes) -> BackendResult<()> {
            self.record("delete", None);
            self.inner.delete(key).await
        }

        async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
            self.record("put_batch", None);
            self.inner.put_batch(entries).await
        }

        async fn traced_put(&self, key: Bytes, value: Bytes, id: u64) -> BackendResult<()> {
            self.record("put", Some(id));
            let gate = self.state.lock().unwrap().mutation_gate.take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
            self.inner.put(key, value).await
        }

        async fn traced_delete(&self, key: Bytes, id: u64) -> BackendResult<()> {
            self.record("delete", Some(id));
            self.inner.delete(key).await
        }

        async fn traced_put_batch(&self, entries: &[(Bytes, Bytes)], id: u64) -> BackendResult<()> {
            self.record("put_batch", Some(id));
            self.inner.put_batch(entries).await
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
        let snapshot = peer.read_observation().unwrap();
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

    struct ActiveCall(Arc<AtomicUsize>);
    impl Drop for ActiveCall {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    struct InterferenceBackend {
        inner: Arc<MemoryBackend>,
        read_delay: Duration,
        put_delay: Duration,
        active: Arc<AtomicUsize>,
    }

    impl InterferenceBackend {
        async fn delay(&self, duration: Duration) -> ActiveCall {
            self.active.fetch_add(1, Ordering::Relaxed);
            let active = ActiveCall(Arc::clone(&self.active));
            tokio::time::sleep(duration).await;
            active
        }
    }

    impl KvBackend for InterferenceBackend {
        fn info(&self) -> BackendInfo {
            self.inner.info()
        }
        async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
            let _active = self.delay(self.read_delay).await;
            self.inner.get(key).await
        }
        async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
            let _active = self.delay(self.put_delay).await;
            self.inner.put(key, value).await
        }
        async fn delete(&self, key: Bytes) -> BackendResult<()> {
            self.inner.delete(key).await
        }
        async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
            self.inner.get_many(keys).await
        }
        async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
            self.inner.put_batch(entries).await
        }
    }

    fn interference_test_config() -> RunConfig {
        let mut c = read_test_config();
        c.case_id = Some("dual-stream-test".into());
        c.key_namespace_hex = Some("01010101010101010101010101010101".into());
        c.execution.mode = ExecutionMode::Interference;
        c.execution.concurrency = 1;
        c.execution.queue_capacity = 3;
        c.phases.measurement_operations = 10_000;
        c.interference = Some(crate::config::InterferenceConfig {
            read_offered_per_second: 100,
            read_queue_capacity: 2,
            put_concurrency: 2,
            duration_seconds: 1,
            key_policy: crate::config::InterferenceKeyPolicy::DisjointReadPut,
        });
        c
    }

    #[tokio::test]
    async fn interference_catches_up_late_reads_but_bounds_scheduler_lateness_and_puts() {
        for lateness in [
            Duration::from_millis(4),
            INTERFERENCE_ARRIVAL_CATCH_UP + Duration::from_millis(10),
        ] {
            let directory = result_directory("interference-arrival-tail");
            let output = directory.join("result");
            let mut config = interference_test_config();
            let settings = config.interference.as_mut().unwrap();
            settings.read_offered_per_second = 1000;
            settings.read_queue_capacity = 1000;
            let backend = Arc::new(MemoryBackend::new(false, false));
            let mut reporter =
                Reporter::create(&output, prometheus::Registry::new(), "arrival-tail".into())
                    .unwrap();
            let mut generator = WorkloadGenerator::new(
                1,
                1,
                config.workload.mix.clone(),
                2,
                crate::config::KeyDistribution::Uniform,
                config.workload.put_key_policy,
                Arc::new(AtomicU64::new(0)),
            );
            let origin = Instant::now();
            let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
            let mut phase = Box::pin(run_interference_phase(
                &backend,
                &config,
                &[1; 16],
                &mut generator,
                origin,
                origin + Duration::from_secs(5),
                &oracle,
                &mut reporter,
            ));
            // The phase uses std::Instant, so pause polling, not the Tokio clock.
            // Workers may finish, but the scheduler cannot offer another arrival.
            std::future::poll_fn(|cx| {
                let data = backend.data.lock().unwrap();
                let polled = phase.as_mut().poll(cx);
                drop(data);
                assert!(polled.is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            sleep_until(TokioInstant::from_std(
                Instant::now() + Duration::from_secs(1) + lateness,
            ))
            .await;
            let (result, status) = phase.await;
            if lateness < INTERFERENCE_ARRIVAL_CATCH_UP {
                status.unwrap();
                assert_eq!(result.read.offered, 1000);
                assert_eq!(result.read.rejected, 0);
            } else {
                let error = status.unwrap_err().to_string();
                assert!(
                    error
                        .starts_with("interference arrival scheduler missed the admission window:")
                );
                assert!(error.contains("lateness_ns="));
                assert!(error.contains(&format!("offered={}, planned=1000", result.read.offered)));
                assert!(result.read.offered < 1000);
            }
            assert!(result.measurement_drained);
            assert_eq!(result.read.admitted, result.read.completed);
            assert_eq!(result.put.admitted, result.put.completed);
            assert_eq!(result.put.offered, 2);
            let summary = RunSummary {
                status: "completed".into(),
                durability: serde_json::json!({}),
                started_unix_seconds: 0.0,
                finished_unix_seconds: 0.0,
                backend: backend.info(),
                phase_counts: serde_json::json!({}),
                distributions: reporter.distribution_summaries(),
                limitations: Vec::new(),
            };
            reporter
                .finish(&summary, false, &serde_json::json!({}))
                .unwrap();
            let admission_end_ns = result.window_start_ns + 1_000_000_000;
            let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
            let mut last_read = None;
            for row in samples.lines().skip(1) {
                let fields: Vec<_> = row.split(',').collect();
                let scheduled: u64 = fields[3].parse().unwrap();
                assert!(scheduled < admission_end_ns);
                if fields[15] == "read" {
                    last_read = Some((scheduled, fields[9].parse::<u64>().unwrap()));
                } else {
                    assert_eq!(fields[15], "put");
                }
            }
            if lateness < INTERFERENCE_ARRIVAL_CATCH_UP {
                let (scheduled, scheduling_ns) = last_read.unwrap();
                assert_eq!(scheduled, admission_end_ns - 1_000_000);
                assert!(scheduling_ns >= duration_ns(lateness) + 1_000_000);
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interference_keeps_read_arrivals_independent_of_slow_puts_and_drains() {
        for read_delay in [Duration::ZERO, Duration::from_millis(40)] {
            let directory = result_directory("interference-streams");
            let output = directory.join("result");
            let config = interference_test_config();
            let inner = Arc::new(MemoryBackend::new(false, false));
            let active = Arc::new(AtomicUsize::new(0));
            let backend = InterferenceBackend {
                inner: Arc::clone(&inner),
                read_delay,
                put_delay: Duration::from_millis(30),
                active: Arc::clone(&active),
            };
            run(
                backend,
                config.clone(),
                &output,
                prometheus::Registry::new(),
            )
            .await
            .unwrap();
            assert_eq!(active.load(Ordering::Relaxed), 0);
            let result: serde_json::Value = serde_json::from_slice(
                &std::fs::read(output.join("interference_result.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(result["measurement_drained"], true);
            assert_eq!(result["read"]["offered"], 100);
            assert_eq!(result["read_offered_per_second"], 100);
            let start = result["window_start_ns"].as_u64().unwrap();
            let end = result["window_end_ns"].as_u64().unwrap();
            assert_eq!(result["measurement_elapsed_ns"], end - start);
            for stream in ["read", "put"] {
                let s = &result[stream];
                assert_eq!(
                    s["offered"].as_u64().unwrap(),
                    s["admitted"].as_u64().unwrap() + s["rejected"].as_u64().unwrap()
                );
                assert_eq!(s["admitted"], s["completed"]);
                assert_eq!(s["successful"], s["completed"]);
                assert_eq!(s["failed"], 0);
                assert!(s["first_dispatch_ns"].as_u64().unwrap() >= start);
                assert!(s["last_completion_ns"].as_u64().unwrap() <= end);
            }
            assert_eq!(result["put"]["rejected"], 0);
            if !read_delay.is_zero() {
                assert!(result["read"]["rejected"].as_u64().unwrap() > 0);
            }
            let overlap_start = result["read"]["first_dispatch_ns"]
                .as_u64()
                .unwrap()
                .max(result["put"]["first_dispatch_ns"].as_u64().unwrap());
            let overlap_end = result["read"]["last_completion_ns"]
                .as_u64()
                .unwrap()
                .min(result["put"]["last_completion_ns"].as_u64().unwrap());
            assert!(overlap_end - overlap_start > 800_000_000);
            let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
            let mut read_arrivals = Vec::new();
            let mut stream_rows = [0_u64; 2];
            for line in samples
                .lines()
                .skip(1)
                .filter(|line| line.starts_with("measurement,"))
            {
                let fields: Vec<_> = line.split(',').collect();
                let scheduled: u64 = fields[3].parse().unwrap();
                let completed: u64 = fields[6].parse().unwrap();
                let response: u64 = fields[8].parse().unwrap();
                assert_eq!(response, completed - scheduled);
                if fields[15] == "read" {
                    read_arrivals.push(scheduled);
                    stream_rows[0] += 1;
                } else {
                    assert_eq!(fields[15], "put");
                    stream_rows[1] += 1;
                }
                assert_eq!(fields[1], fields[15]);
                if fields[5] == "true" {
                    let dispatch: u64 = fields[4].parse().unwrap();
                    assert_eq!(fields[7].parse::<u64>().unwrap(), completed - dispatch);
                    assert_eq!(fields[9].parse::<u64>().unwrap(), dispatch - scheduled);
                }
            }
            read_arrivals.sort_unstable();
            assert_eq!(
                read_arrivals,
                (0..100)
                    .map(|index| start + index * 10_000_000)
                    .collect::<Vec<_>>()
            );
            assert_eq!(stream_rows[0], 100);
            assert_eq!(stream_rows[1], result["put"]["offered"].as_u64().unwrap());
            // The fake backend acknowledges only after storing each new value.
            let data = inner.data.lock().unwrap();
            for id in 0..result["put"]["successful"].as_u64().unwrap() + 1 {
                assert_eq!(
                    data.get(key_bytes(&[1; 16], config.seed, id).as_ref())
                        .unwrap(),
                    &value_bytes(
                        &[1; 16],
                        config.seed,
                        id,
                        config.dataset.value_size,
                        config.dataset.entropy
                    )
                );
            }
            drop(data);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_sample_writer_fails_interference_without_delaying_admission_or_drain() {
        let directory = result_directory("interference-stalled-writer");
        let output = directory.join("result");
        let mut config = interference_test_config();
        config.interference.as_mut().unwrap().duration_seconds = 20;
        let backend = Arc::new(InterferenceBackend {
            inner: Arc::new(MemoryBackend::new(false, false)),
            read_delay: Duration::from_millis(20),
            put_delay: Duration::from_millis(30),
            active: Arc::new(AtomicUsize::new(0)),
        });
        let mut reporter = Reporter::create(
            &output,
            prometheus::Registry::new(),
            "stalled-writer".into(),
        )
        .unwrap();
        // Keep the bounded receiver alive but never consume a row.
        let stalled_writer = reporter.stall_sample_writer_for_test();
        let mut generator = WorkloadGenerator::new(
            1,
            1,
            config.workload.mix.clone(),
            2,
            crate::config::KeyDistribution::Uniform,
            config.workload.put_key_policy,
            Arc::new(AtomicU64::new(0)),
        );
        let origin = Instant::now();
        let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
        let (result, status) = tokio::time::timeout(
            Duration::from_secs(2),
            run_interference_phase(
                &backend,
                &config,
                &[1; 16],
                &mut generator,
                origin,
                origin + Duration::from_secs(5),
                &oracle,
                &mut reporter,
            ),
        )
        .await
        .expect("telemetry saturation must not await the stalled consumer");
        assert!(
            status
                .unwrap_err()
                .to_string()
                .contains("interference telemetry admission failed")
        );
        assert!(result.read.offered < 2000);
        assert!(result.measurement_drained);
        assert_eq!(result.read.admitted, result.read.completed);
        assert_eq!(result.put.admitted, result.put.completed);
        assert_eq!(backend.active.load(Ordering::Relaxed), 0);
        assert_eq!(Arc::strong_count(&backend), 1);
        let summary = RunSummary {
            status: "failed".into(),
            durability: serde_json::json!({}),
            started_unix_seconds: 0.0,
            finished_unix_seconds: 0.0,
            backend: backend.info(),
            phase_counts: serde_json::json!({}),
            distributions: reporter.distribution_summaries(),
            limitations: Vec::new(),
        };
        assert!(
            reporter
                .finish(
                    &summary,
                    true,
                    &serde_json::json!({"interference":{"schema_version":1}})
                )
                .is_err()
        );
        drop(stalled_writer);
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["status"], "failed");
        assert_eq!(manifest["raw_samples_complete"], false);
        assert_eq!(manifest["sample_writer_backpressure_events"], 0);
        assert!(
            manifest["sample_writer_admission"]["submit_errors"]
                .as_u64()
                .unwrap()
                > 0
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interference_timeouts_cancel_async_calls_and_leave_no_pending_work() {
        for (timeout_ms, deadline_ms) in [(10, 5000), (1000, 50)] {
            let directory = result_directory("interference-timeout");
            let mut config = interference_test_config();
            config.execution.request_timeout_ms = timeout_ms;
            let backend = Arc::new(InterferenceBackend {
                inner: Arc::new(MemoryBackend::new(false, false)),
                read_delay: Duration::from_secs(60),
                put_delay: Duration::from_secs(60),
                active: Arc::new(AtomicUsize::new(0)),
            });
            let mut reporter = Reporter::create(
                &directory.join("result"),
                prometheus::Registry::new(),
                "timeout-test".into(),
            )
            .unwrap();
            let mut generator = WorkloadGenerator::new(
                1,
                1,
                config.workload.mix.clone(),
                2,
                crate::config::KeyDistribution::Uniform,
                config.workload.put_key_policy,
                Arc::new(AtomicU64::new(0)),
            );
            let origin = Instant::now();
            let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
            let (result, status) = run_interference_phase(
                &backend,
                &config,
                &[1; 16],
                &mut generator,
                origin,
                origin + Duration::from_millis(deadline_ms),
                &oracle,
                &mut reporter,
            )
            .await;
            assert_eq!(status.is_ok(), deadline_ms == 5000);
            assert!(result.measurement_drained);
            if deadline_ms == 5000 {
                assert_eq!(result.read.offered, 100);
            } else {
                assert!(result.read.offered < 100);
            }
            assert_eq!(result.read.admitted, result.read.completed);
            assert_eq!(result.put.admitted, result.put.completed);
            assert_eq!(result.read.successful + result.put.successful, 0);
            assert_eq!(backend.active.load(Ordering::Relaxed), 0);
            assert!(Arc::strong_count(&backend) == 1);
            let summary = RunSummary {
                status: "failed".into(),
                durability: serde_json::json!({}),
                started_unix_seconds: 0.0,
                finished_unix_seconds: 0.0,
                backend: backend.info(),
                phase_counts: serde_json::json!({}),
                distributions: reporter.distribution_summaries(),
                limitations: Vec::new(),
            };
            reporter
                .finish(&summary, true, &serde_json::json!({}))
                .unwrap();
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interference_native_barrier_puts_survive_local_reopen() {
        let directory = result_directory("interference-native-barrier");
        let output = directory.join("result");
        let mut config = interference_test_config();
        config.phases.measurement_operations = 1_000_000;
        config.limits.max_operations = 1_000_001;
        config.backend.options = serde_json::json!({
            "path":directory.join("data"), "key_count":1_000_001,
            "durability":"value_wal_barrier", "native_wal_geometry":"small",
        });
        let backend =
            crate::backends::native::open(&config.backend.options, &prometheus::Registry::new())
                .await
                .unwrap();
        run(
            backend,
            config.clone(),
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap();
        let result: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("interference_result.json")).unwrap(),
        )
        .unwrap();
        let puts = result["put"]["successful"].as_u64().unwrap();
        assert!(puts > 0);
        assert_eq!(result["put"]["admitted"], puts);
        let reopened =
            crate::backends::native::open(&config.backend.options, &prometheus::Registry::new())
                .await
                .unwrap();
        for id in 0..=puts {
            assert_eq!(
                reopened
                    .get(key_bytes(&[1; 16], config.seed, id).as_ref())
                    .await
                    .unwrap()
                    .unwrap(),
                value_bytes(
                    &[1; 16],
                    config.seed,
                    id,
                    config.dataset.value_size,
                    config.dataset.entropy
                )
            );
        }
        reopened.shutdown().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn interference_safety_ceiling_fails_closed_and_still_drains_both_streams() {
        let directory = result_directory("interference-ceiling");
        let output = directory.join("result");
        let mut config = interference_test_config();
        config.phases.measurement_operations = 101;
        let error = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("operation safety bound"));
        let result: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("interference_result.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(result["measurement_drained"], true);
        for stream in ["read", "put"] {
            assert_eq!(result[stream]["admitted"], result[stream]["completed"]);
            assert_eq!(result[stream]["failed"], 0);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn write_wait_export_preserves_exact_finish_values_and_legacy_gating() {
        let metrics = serde_json::json!({
            "enabled": true, "saturated": false, "complete": true,
            "windows": (["owner_enqueue_to_dequeue", "admitted_to_append_enqueue", "harvest_to_durable"].map(|wait| serde_json::json!({
                "wait":wait, "observations": 5, "depth_sum": 9, "depth_max": 3, "wake_attempts": 4, "wake_coalesced": 2,
            }))),
        });
        for focused in [false, true] {
            let directory = result_directory("write-wait-export");
            let output = directory.join("result");
            let mut config = read_test_config();
            config.backend.kind = "tidehunter".into();
            config.diagnostics.write_timeline_limit = 2;
            config.case_id = focused.then(|| "g9-test".into());
            let state = Arc::new(Mutex::new(TimelineTestState {
                write_wait_metrics: Some(metrics.clone()),
                ..TimelineTestState::default()
            }));
            let backend = TimelineTestBackend {
                inner: MemoryBackend::new(false, false),
                state: Arc::clone(&state),
            };
            run(backend, config, &output, prometheus::Registry::new())
                .await
                .unwrap();
            assert_eq!(state.lock().unwrap().finishes, 1);
            let path = output.join("write_wait_metrics.json");
            assert_eq!(path.exists(), focused);
            if focused {
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                assert_eq!(value.as_object().unwrap().len(), 6);
                assert_eq!(value["schema_version"], 1);
                assert_eq!(value["case_id"], "g9-test");
                assert_eq!(value["enabled"], metrics["enabled"]);
                assert_eq!(value["saturated"], metrics["saturated"]);
                assert_eq!(value["complete"], metrics["complete"]);
                assert_eq!(value["windows"], metrics["windows"]);
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
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
        let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
        let mut pool = WorkerPool::new(&backend, &config, &[0; 16], origin, &oracle).unwrap();
        let read = |id| WorkItem {
            operation: OperationKind::Read,
            keys: vec![id],
        };

        pool.try_dispatch(read(0), "measurement", 0, deadline, false, None)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), slow_started_rx)
            .await
            .unwrap()
            .unwrap();
        pool.try_dispatch(read(1), "measurement", 0, deadline, false, None)
            .unwrap();
        let fast = tokio::time::timeout(Duration::from_secs(5), pool.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fast.id, 1);
        pool.record_completion(fast, &mut reporter).await.unwrap();

        pool.try_dispatch(read(2), "measurement", 0, deadline, false, None)
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
        pool.record_completion(slow, &mut reporter).await.unwrap();
        let replacement_id = if let Some(completion) = replacement {
            let id = completion.id;
            pool.record_completion(completion, &mut reporter)
                .await
                .unwrap();
            id
        } else {
            let completion = pool.receive().await.unwrap();
            let id = completion.id;
            pool.record_completion(completion, &mut reporter)
                .await
                .unwrap();
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
                    config.workload.put_key_policy,
                    Arc::new(AtomicU64::new(0)),
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
                    config.workload.put_key_policy,
                    Arc::new(AtomicU64::new(0)),
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

    #[tokio::test]
    async fn mixed_delete_run_fails_when_a_never_deleted_key_is_lost() {
        let directory = result_directory("mixed-delete-lost-key");
        let mut config = read_test_config();
        config.dataset.keys = 2;
        config.execution.concurrency = 1;
        config.execution.queue_capacity = 1;
        config.workload.mix = crate::config::OperationMix {
            read: 1,
            delete: 1,
            ..crate::config::OperationMix::default()
        };
        // The only measured operation reads key 0, which the backend lost and nothing deleted.
        config.seed = (0..10_000)
            .find(|seed| {
                let mut generator = WorkloadGenerator::new(
                    *seed ^ 0x6b76_7265_7365_6172,
                    config.dataset.keys,
                    config.workload.mix.clone(),
                    config.workload.batch_size,
                    config.workload.distribution.clone(),
                    config.workload.put_key_policy,
                    Arc::new(AtomicU64::new(0)),
                );
                let item = generator.next_item().unwrap();
                item.operation == OperationKind::Read && item.keys == [0]
            })
            .expect("small deterministic seed search finds a read of key 0");
        let output = directory.join("result");
        // Qualification issues five puts, so put number 5 is the first preload put (key 0).
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
    async fn samples_carry_a_worker_release_clock_after_the_api_boundary() {
        let directory = result_directory("worker-release-clock");
        let output = directory.join("result");
        let result = run(
            MemoryBackend::new(false, false),
            read_test_config(),
            &output,
            prometheus::Registry::new(),
        )
        .await;
        assert!(result.is_ok());
        let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
        let mut lines = samples.lines();
        let columns: Vec<_> = lines.next().unwrap().split(',').collect();
        let completed = columns.iter().position(|c| *c == "completed_ns").unwrap();
        let released = columns.iter().position(|c| *c == "released_ns").unwrap();
        let mut rows = 0_u64;
        for line in lines {
            let fields: Vec<_> = line.split(',').collect();
            let completed: u64 = fields[completed].parse().unwrap();
            let released: u64 = fields[released].parse().unwrap();
            assert!(released >= completed, "{line}");
            rows += 1;
        }
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(rows > 0);
        assert_eq!(
            manifest["worker_release"]["samples"],
            manifest["completed_operations"]
        );
        assert!(manifest["sample_writer_admission"]["stall_events"].is_u64());
    }

    #[test]
    fn completed_delete_explains_absence_until_a_clean_put_restores_the_key() {
        let oracle = KeyOracle::new(true, 2, true).unwrap();
        assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
        oracle.begin_delete(0);
        oracle.end_delete(0, true);
        assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
        assert!(!oracle.absence_allowed(1, oracle.snapshot(1)));
        let ticket = oracle.begin_put(0);
        oracle.end_put(0, ticket, true);
        assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
    }

    #[test]
    fn put_overlapping_a_delete_cannot_restore_the_guarantee() {
        let oracle = KeyOracle::new(true, 1, true).unwrap();
        // Delete in flight when the put starts: the delete may take effect after the put.
        oracle.begin_delete(0);
        let ticket = oracle.begin_put(0);
        oracle.end_delete(0, true);
        oracle.end_put(0, ticket, true);
        assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
        // Delete starting during a later clean put.
        let ticket = oracle.begin_put(0);
        assert!(!ticket.overlapped_delete);
        oracle.begin_delete(0);
        oracle.end_put(0, ticket, true);
        oracle.end_delete(0, true);
        assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
        // An unacknowledged put never restores presence.
        let ticket = oracle.begin_put(0);
        oracle.end_put(0, ticket, false);
        assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
    }

    #[test]
    fn read_spanning_a_delete_cycle_may_see_absence() {
        let oracle = KeyOracle::new(true, 1, true).unwrap();
        let before = oracle.snapshot(0);
        // Delete and a clean rewrite both finish inside the read interval.
        oracle.begin_delete(0);
        oracle.end_delete(0, true);
        let ticket = oracle.begin_put(0);
        oracle.end_put(0, ticket, true);
        assert!(oracle.absence_allowed(0, before));
        assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
        // A delete still in flight at the end of the read also explains absence.
        let before = oracle.snapshot(0);
        oracle.begin_delete(0);
        assert!(oracle.absence_allowed(0, before));
    }

    #[test]
    fn delete_with_unknown_outcome_keeps_absence_allowed_forever() {
        let oracle = KeyOracle::new(true, 1, true).unwrap();
        oracle.begin_delete(0);
        oracle.end_delete(0, false);
        let ticket = oracle.begin_put(0);
        oracle.end_put(0, ticket, true);
        assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
    }

    #[test]
    fn oracle_without_baseline_accepts_absence_and_without_deletes_rejects_it() {
        let unchecked = KeyOracle::new(false, 4, true).unwrap();
        assert!(unchecked.absence_allowed(3, unchecked.snapshot(3)));
        let strict = KeyOracle::new(true, 4, false).unwrap();
        assert!(!strict.tracks_mutations());
        assert!(!strict.absence_allowed(3, strict.snapshot(3)));
    }

    #[test]
    fn read_overlapping_a_put_may_see_absence_but_not_before_or_after() {
        let oracle = KeyOracle::new(true, 1, true).unwrap();
        // Put in flight when the read starts.
        let ticket = oracle.begin_put(0);
        let before = oracle.snapshot(0);
        oracle.end_put(0, ticket, true);
        assert!(oracle.absence_allowed(0, before));
        // Put starts and ends inside the read interval.
        let before = oracle.snapshot(0);
        let ticket = oracle.begin_put(0);
        oracle.end_put(0, ticket, true);
        assert!(oracle.absence_allowed(0, before));
        // Put still in flight when the read returns.
        let before = oracle.snapshot(0);
        let ticket = oracle.begin_put(0);
        assert!(oracle.absence_allowed(0, before));
        // A put that finished before the read began explains nothing.
        oracle.end_put(0, ticket, false);
        assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
    }

    /// Reads return nothing (a lost key); the put blocks until released.
    struct OverlapBackend {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }

    impl OverlapBackend {
        async fn gate(&self) {
            let started = self.started.lock().unwrap().take();
            if let Some(started) = started {
                let _ = started.send(());
            }
            let release = self.release.lock().unwrap().take();
            if let Some(release) = release {
                let _ = release.await;
            }
        }
    }
    impl KvBackend for OverlapBackend {
        fn info(&self) -> BackendInfo {
            MemoryBackend::new(false, false).info()
        }
        async fn get(&self, _key: &[u8]) -> BackendResult<Option<Bytes>> {
            Ok(None)
        }
        async fn put(&self, _key: Bytes, _value: Bytes) -> BackendResult<()> {
            self.gate().await;
            Ok(())
        }
        async fn delete(&self, _key: Bytes) -> BackendResult<()> {
            Ok(())
        }
        async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
            Ok(vec![None; keys.len()])
        }
        async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
            self.gate().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn gated_put_explains_an_absent_read_only_while_it_is_in_flight() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let backend = OverlapBackend {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(Some(release_rx)),
        };
        let config = read_test_config();
        let oracle = KeyOracle::new(true, 1, true).unwrap();
        let origin = Instant::now();
        let deadline = origin + Duration::from_secs(30);
        let run = |operation| {
            execute_item(
                &backend,
                &config,
                &[0; 16],
                WorkItem {
                    operation,
                    keys: vec![0],
                },
                false,
                None,
                "measurement",
                0,
                origin,
                deadline,
                &oracle,
                Arc::new(AtomicU64::new(0)),
            )
        };
        let (put, during) = tokio::join!(run(OperationKind::Put), async {
            started_rx.await.unwrap();
            let sample = run(OperationKind::Read).await;
            release_tx.send(()).unwrap();
            sample
        });
        let after = run(OperationKind::Read).await;
        assert_eq!(put.outcome, "success");
        assert_eq!(
            during.outcome, "success",
            "absence during the put is explained"
        );
        assert_eq!(
            after.outcome, "wrong_value",
            "the same lost key after the put is not"
        );
    }

    #[tokio::test]
    async fn a_batch_repeating_one_key_is_one_in_flight_put() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let backend = OverlapBackend {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(Some(release_rx)),
        };
        let config = read_test_config();
        let oracle = KeyOracle::new(true, 1, true).unwrap();
        let origin = Instant::now();
        // Far more members than workers, all naming key 0: counted per member this would be 2048.
        let members = 2048;
        let put = execute_item(
            &backend,
            &config,
            &[0; 16],
            WorkItem {
                operation: OperationKind::PutBatch,
                keys: vec![0; members],
            },
            false,
            None,
            "measurement",
            0,
            origin,
            origin + Duration::from_secs(60),
            &oracle,
            Arc::new(AtomicU64::new(0)),
        );
        let observe = async {
            started_rx.await.unwrap();
            let during = oracle.snapshot(0);
            release_tx.send(()).unwrap();
            during
        };
        let (_, during) = tokio::join!(put, observe);
        assert_eq!(inflight(during.puts), 1);
        assert_eq!(epoch(during.puts), 1);
        assert_eq!(oracle.snapshot(0).puts & INFLIGHT_MASK, 0);
        assert!(oracle.ensure_capacity(4096).is_ok());
        assert!(
            oracle
                .ensure_capacity(usize::try_from(INFLIGHT_MASK).unwrap())
                .is_err()
        );
    }

    #[tokio::test]
    async fn run_refuses_a_tier_the_backend_receipt_does_not_confirm() {
        let directory = result_directory("tier-unconfirmed");
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".to_owned();
        config.backend.options = serde_json::json!({"durability_tier": "synchronous"});
        let output = directory.join("result");
        // The test backend reports the native tier, so a synchronous request must not run.
        let result = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let created = output.exists();
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(result.is_err());
        assert!(!created, "no artifact may describe an unconfirmed tier");
    }

    #[tokio::test]
    async fn confirmed_tier_and_cohort_are_recorded_in_summary_and_manifest() {
        let directory = result_directory("tier-confirmed");
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".to_owned();
        let output = directory.join("result");
        let result = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let read = |name: &str| -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(output.join(name)).unwrap()).unwrap()
        };
        let (summary, manifest) = (read("summary.json"), read("manifest.json"));
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(result.is_ok());
        for durability in [&summary["durability"], &manifest["durability"]] {
            assert_eq!(durability["tier"], "native");
            assert_eq!(durability["comparison_cohort"], "native_pagecache");
        }
        assert_eq!(manifest["run"]["durability"], summary["durability"]);
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
    async fn write_snapshot_precedes_completion_gate_without_finishing_profile() {
        let directory = result_directory("write-profile-gate");
        let output = directory.join("result");
        let ready = directory.join("complete");
        let release = directory.join("observed");
        let counter = Arc::new(AtomicUsize::new(7));
        let mut backend = MemoryBackend::new(false, false);
        backend.write_gate_counter = Some(Arc::clone(&counter));
        let snapshot_calls = Arc::clone(&backend.write_snapshot_calls);
        let finish_calls = Arc::clone(&backend.write_finish_calls);
        let gate_snapshot_calls = Arc::clone(&snapshot_calls);
        let gate_finish_calls = Arc::clone(&finish_calls);
        let mut config = read_test_config();
        config.diagnostics.write_profile_limit = 1;
        config.measurement_completion_gate = Some(crate::config::MeasurementGateConfig {
            ready_file: ready.clone(),
            release_file: release.clone(),
            timeout_seconds: 2,
        });
        let controller = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert_eq!(gate_snapshot_calls.load(Ordering::Relaxed), 1);
                assert_eq!(gate_finish_calls.load(Ordering::Relaxed), 0);
                counter.store(19, Ordering::Relaxed);
                std::fs::write(release, b"observed").unwrap();
            })
            .await
            .unwrap();
        });
        run(backend, config, &output, prometheus::Registry::new())
            .await
            .unwrap();
        controller.await.unwrap();
        assert_eq!(snapshot_calls.load(Ordering::Relaxed), 1);
        assert_eq!(finish_calls.load(Ordering::Relaxed), 1);
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("write-diagnostics.json")).unwrap())
                .unwrap();
        assert_eq!(evidence["pre_completion_gate"]["value"], 7);
        assert_eq!(evidence["profile"]["post_gate_value"], 19);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn disabled_write_profile_never_snapshots_or_finishes() {
        let directory = result_directory("write-profile-disabled");
        let output = directory.join("result");
        let backend = MemoryBackend::new(false, false);
        let snapshots = Arc::clone(&backend.write_snapshot_calls);
        let finishes = Arc::clone(&backend.write_finish_calls);
        run(
            backend,
            read_test_config(),
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap();
        assert_eq!(snapshots.load(Ordering::Relaxed), 0);
        assert_eq!(finishes.load(Ordering::Relaxed), 0);
        assert!(!output.join("write-diagnostics.json").exists());
        assert!(!output.join("write-timeline.json").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn timeline_dispatch_and_csv_join_are_bounded_and_disabled_cleanly() {
        for operation in ["put", "delete", "put_batch"] {
            for limit in [0, 3] {
                let directory = result_directory("timeline-end-to-end");
                let output = directory.join("result");
                let state = Arc::new(Mutex::new(TimelineTestState::default()));
                let backend = TimelineTestBackend {
                    inner: MemoryBackend::new(false, false),
                    state: Arc::clone(&state),
                };
                let mut config = read_test_config();
                config.backend.kind = "tidehunter".to_owned();
                config.dataset.keys = 8;
                config.execution.concurrency = 1;
                config.phases.warmup_operations = 2;
                config.phases.measurement_operations = 6;
                config.diagnostics.write_timeline_limit = limit;
                config.workload.mix = crate::config::OperationMix::default();
                match operation {
                    "put" => config.workload.mix.put = 1,
                    "delete" => config.workload.mix.delete = 1,
                    "put_batch" => config.workload.mix.put_batch = 1,
                    _ => unreachable!(),
                }
                run(backend, config, &output, prometheus::Registry::new())
                    .await
                    .unwrap();
                let csv = std::fs::read_to_string(output.join("samples.csv")).unwrap();
                let mut rows = csv.lines();
                let header: Vec<_> = rows.next().unwrap().split(',').collect();
                let id_column = header
                    .iter()
                    .position(|name| *name == "caller_sample_id")
                    .unwrap();
                let mut raw_ids = Vec::new();
                let mut warmup_count = 0;
                let mut measurement_count = 0;
                for row in rows {
                    let fields: Vec<_> = row.split(',').collect();
                    if fields[0] == "warmup" {
                        warmup_count += 1;
                        assert_eq!(fields[id_column], "");
                    } else if fields[0] == "measurement" {
                        measurement_count += 1;
                        if !fields[id_column].is_empty() {
                            assert_eq!(fields[1], operation);
                            raw_ids.push(fields[id_column].parse::<u64>().unwrap());
                        }
                    }
                }
                assert_eq!(warmup_count, 2);
                assert_eq!(measurement_count, 6);
                raw_ids.sort_unstable();
                assert_eq!(raw_ids, (0..limit as u64).collect::<Vec<_>>());
                let state = state.lock().unwrap();
                assert_eq!(state.arms, usize::from(limit > 0));
                assert_eq!(state.finishes, usize::from(limit > 0));
                assert!(
                    state
                        .calls
                        .iter()
                        .filter(|(_, id, _)| id.is_some())
                        .all(|(name, _, armed)| *name == operation && *armed)
                );
                if limit > 0 {
                    assert_eq!(
                        state
                            .calls
                            .iter()
                            .filter(|(name, id, armed)| *name == operation
                                && id.is_none()
                                && *armed)
                            .count(),
                        3
                    );
                    assert!(
                        state
                            .calls
                            .iter()
                            .filter(|(name, id, armed)| *name == operation
                                && id.is_none()
                                && !*armed)
                            .count()
                            >= 2
                    );
                    let evidence: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(output.join("write-timeline.json")).unwrap(),
                    )
                    .unwrap();
                    let mut sidecar_ids: Vec<_> = evidence["timeline"]["records"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|record| record["caller_sample_id"].as_u64().unwrap())
                        .collect();
                    sidecar_ids.sort_unstable();
                    assert_eq!(sidecar_ids, raw_ids);
                } else {
                    assert!(state.calls.iter().all(|(_, id, _)| id.is_none()));
                    assert!(
                        state
                            .calls
                            .iter()
                            .filter(|(name, _, _)| *name == operation)
                            .count()
                            >= 8
                    );
                    assert!(!output.join("write-timeline.json").exists());
                }
                drop(state);
                std::fs::remove_dir_all(directory).unwrap();
            }
        }
    }

    #[test]
    fn timeline_owner_status_detects_errors_and_coverage_independently() {
        let mut value = serde_json::json!({
            "owner_errors": [],
            "owner_expected_connections": 2,
            "owner_captured_connections": 2
        });
        assert!(write_timeline_owner_error(&value).is_none());
        value["deadline_exceeded"] = serde_json::json!(true);
        assert!(
            write_timeline_owner_error(&value)
                .unwrap()
                .contains("partial evidence")
        );
        value["deadline_exceeded"] = serde_json::json!(false);
        value["owner_captured_connections"] = serde_json::json!(1);
        assert!(write_timeline_owner_error(&value).is_some());
        value["owner_captured_connections"] = serde_json::json!(2);
        value["owner_errors"] = serde_json::json!([{"phase": "disarm"}]);
        assert!(write_timeline_owner_error(&value).is_some());
        value.as_object_mut().unwrap().remove("owner_errors");
        assert!(write_timeline_owner_error(&value).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_drain_and_stalled_finalizer_still_publish_terminal_artifacts() {
        let directory = result_directory("timeline-stalled-finalizer");
        let output = directory.join("result");
        let (mutation_release, mutation_gate) = std_mpsc::channel();
        let (finish_release, finish_gate) = std_mpsc::channel();
        let (finish_started, started) = std_mpsc::channel();
        let state = Arc::new(Mutex::new(TimelineTestState {
            mutation_gate: Some(mutation_gate),
            finish_gate: Some(finish_gate),
            finish_started: Some(finish_started),
            ..TimelineTestState::default()
        }));
        let backend = TimelineTestBackend {
            inner: MemoryBackend::new(false, false),
            state,
        };
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".to_owned();
        config.diagnostics.write_timeline_limit = 1;
        config.workload.mix = crate::config::OperationMix {
            put: 1,
            ..crate::config::OperationMix::default()
        };
        config.limits.deadline_seconds = 1;
        config.limits.shutdown_grace_seconds = 1;
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            run(backend, config, &output, prometheus::Registry::new()),
        )
        .await;
        assert!(outcome.unwrap().is_err());
        assert!(output.join("samples.csv").is_file());
        assert!(output.join("summary.json").is_file());
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("write-timeline.json")).unwrap())
                .unwrap();
        assert_eq!(evidence["status"], "failed");
        assert_eq!(evidence["available"], false);
        assert!(
            evidence["error"]
                .as_str()
                .unwrap()
                .contains("shutdown grace")
        );
        started.recv_timeout(Duration::from_secs(1)).unwrap();
        let _ = mutation_release.send(());
        let _ = finish_release.send(());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn partial_owner_timeline_export_fails_run_but_retains_records() {
        let directory = result_directory("timeline-partial-owner");
        let output = directory.join("result");
        let state = Arc::new(Mutex::new(TimelineTestState {
            partial_owner_export: true,
            ..TimelineTestState::default()
        }));
        let backend = TimelineTestBackend {
            inner: MemoryBackend::new(false, false),
            state: Arc::clone(&state),
        };
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".to_owned();
        config.diagnostics.write_timeline_limit = 1;
        config.workload.mix = crate::config::OperationMix {
            put: 1,
            ..crate::config::OperationMix::default()
        };
        assert!(
            run(backend, config, &output, prometheus::Registry::new())
                .await
                .is_err()
        );
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("write-timeline.json")).unwrap())
                .unwrap();
        assert_eq!(evidence["available"], true);
        assert_eq!(evidence["status"], "failed");
        assert!(
            evidence["error"]
                .as_str()
                .unwrap()
                .contains("owner timeline capture incomplete")
        );
        assert_eq!(
            evidence["timeline"]["owner_errors"][0]["message"],
            "test fetch failure"
        );
        assert_eq!(evidence["timeline"]["owner_records"][0]["envelope_id"], 10);
        assert_eq!(evidence["timeline"]["records"][0]["caller_sample_id"], 0);
        assert_eq!(state.lock().unwrap().finishes, 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn write_timeline_arm_error_exports_unavailable_evidence() {
        let directory = result_directory("timeline-arm-error");
        let output = directory.join("result");
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".to_owned();
        config.diagnostics.write_timeline_limit = 1;
        assert!(
            run(
                MemoryBackend::new(false, false),
                config,
                &output,
                prometheus::Registry::new()
            )
            .await
            .is_err()
        );
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("write-timeline.json")).unwrap())
                .unwrap();
        assert_eq!(evidence["available"], false);
        assert!(evidence["error"].as_str().unwrap().contains("timeline"));
        assert!(evidence["timeline"].is_null());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn write_snapshot_error_is_unavailable_but_profile_still_finishes() {
        for adoption_boundary in [false, true] {
            let directory = result_directory(&format!("write-snapshot-error-{adoption_boundary}"));
            let output = directory.join("result");
            let mut backend = MemoryBackend::new(false, false);
            backend.write_gate_counter = Some(Arc::new(AtomicUsize::new(7)));
            backend.write_snapshot_error = true;
            backend.write_snapshot_boundary_error = adoption_boundary;
            let snapshots = Arc::clone(&backend.write_snapshot_calls);
            let finishes = Arc::clone(&backend.write_finish_calls);
            let mut config = read_test_config();
            config.diagnostics.write_profile_limit = 1;
            let result = run(backend, config, &output, prometheus::Registry::new()).await;
            if adoption_boundary {
                assert!(
                    result
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("measurement write profile boundary failed")
                );
            } else {
                result.unwrap();
            }
            let evidence: serde_json::Value = serde_json::from_slice(
                &std::fs::read(output.join("write-diagnostics.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(snapshots.load(Ordering::Relaxed), 1);
            assert_eq!(finishes.load(Ordering::Relaxed), 1);
            assert_eq!(evidence["pre_completion_gate"]["available"], false);
            assert!(
                evidence["pre_completion_gate"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains("snapshot unavailable")
            );
            assert_eq!(evidence["profile"]["post_gate_value"], 7);
            std::fs::remove_dir_all(directory).unwrap();
        }
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
    fn timeline_selection_is_measurement_only_and_includes_batches() {
        for op in [
            OperationKind::Put,
            OperationKind::Delete,
            OperationKind::PutBatch,
        ] {
            assert!(!is_timeline_candidate("warmup", op, 0, 1));
            assert!(is_timeline_candidate("measurement", op, 0, 1));
            assert!(!is_timeline_candidate("measurement", op, 1, 1));
            assert!(!is_timeline_candidate("measurement", op, 0, 0));
        }
        assert!(!is_timeline_candidate(
            "measurement",
            OperationKind::Read,
            0,
            1
        ));
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
    #[tokio::test]
    async fn existing_dataset_checks_all_values_before_measurement_without_overwriting() {
        for scenario in ["present", "missing", "corrupt"] {
            let directory = result_directory(&format!("existing-{scenario}"));
            let mut config = read_test_config();
            let namespace = [9; 16];
            config.key_namespace_hex = Some(hex(&namespace));
            config.dataset.keys = 2;
            config.phases.preload = false;
            config.phases.verify_existing_dataset = true;
            let backend = MemoryBackend::new(false, false);
            for id in 0..2 {
                if scenario == "missing" && id == 1 {
                    continue;
                }
                let mut value = value_bytes(
                    &namespace,
                    config.seed,
                    id,
                    config.dataset.value_size,
                    config.dataset.entropy,
                )
                .to_vec();
                if scenario == "corrupt" && id == 1 {
                    value[32] ^= 1;
                }
                backend.data.lock().unwrap().insert(
                    key_bytes(&namespace, config.seed, id).to_vec(),
                    value.into(),
                );
            }
            let output = directory.join("result");
            let result = run(backend, config, &output, prometheus::Registry::new()).await;
            assert_eq!(result.is_ok(), scenario == "present");
            if scenario != "present" {
                assert!(measurement_outcomes(&output).is_empty());
                let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
                assert!(
                    samples
                        .lines()
                        .any(|line| line.starts_with("verify_existing_dataset,read,wrong_value,"))
                );
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn live_interval_requires_complete_monotone_route_counters() {
        let mut stats = serde_json::json!({
            "completed_read_ops": 2, "completed_read_bytes": 4096,
            "completed_write_ops": 3, "completed_write_bytes": 8192,
            "recovery_read_ops": 1, "recovery_read_bytes": 4096,
            "metadata_requests": 4, "namespace_connects": 1,
            "payload_rpc_requests": 0, "unknown_writes": 0, "errors": 0,
            "queue_ns": 0, "queue_samples": 0, "submit_ns": 0, "submit_samples": 0,
            "cq_ns": 0, "cq_samples": 0, "control_ns": 0, "control_samples": 0,
            "validation_ns": 0, "validation_samples": 0, "persistence_ns": 0,
            "allocation_ns": 0, "allocation_samples": 0, "publication_ns": 0,
            "publication_samples": 0, "namespace_setup_ns": 0, "namespace_setup_samples": 0,
            "read_io_ns": 0, "read_io_samples": 0,
            "persistence_samples": 0, "route_evidence_dropped": 0, "fence_evidence_dropped": 0
        });
        // Counters added with the multi-flight lanes; every one must be present to difference.
        for name in [
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
            "reserve_exchanges",
            "complete_exchanges",
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
            "adoption_history_full_settles",
            "fua_writes",
            "leased_failed_writes",
            "leased_short_writes",
            "leased_timed_out_writes",
            "leased_full_writes",
            "failed_write_ops",
            "short_write_ops",
            "short_write_bytes",
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
        ] {
            stats[name] = serde_json::json!(0);
        }
        for stage in [
            "owner_completion",
            "wal_persistence",
            "wal_namespace",
            "frontier_file",
            "frontier_namespace",
            "adoption_async",
            "adoption_settle",
            "leased_write",
            "extent_switch",
        ] {
            stats[format!("{stage}_ns")] = serde_json::json!(10);
            stats[format!("{stage}_samples")] = serde_json::json!(1);
        }
        let start = serde_json::json!({"available":true, "live_nvmeof": stats});
        let mut end = start.clone();
        end["live_nvmeof"]["completed_write_ops"] = serde_json::json!(5);
        let evidence = measurement_read_evidence(&start, &end, true, true);
        assert_eq!(evidence["live_nvmeof"]["available"], true);
        assert_eq!(
            evidence["live_nvmeof"]["counters"]["completed_write_ops"],
            2
        );
        for name in [
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
        ] {
            let mut measured = end.clone();
            measured["live_nvmeof"][name] = serde_json::json!(7);
            let delta = measurement_read_evidence(&start, &measured, true, true);
            assert_eq!(delta["live_nvmeof"]["counters"][name], 7, "{name}");
            measured["live_nvmeof"]
                .as_object_mut()
                .unwrap()
                .remove(name);
            assert_eq!(
                measurement_read_evidence(&start, &measured, true, true)["live_nvmeof"]["available"],
                false,
                "{name}",
            );
        }
        for stage in [
            "owner_completion",
            "wal_persistence",
            "wal_namespace",
            "frontier_file",
            "frontier_namespace",
        ] {
            let mut measured = end.clone();
            measured["live_nvmeof"][format!("{stage}_ns")] = serde_json::json!(40);
            measured["live_nvmeof"][format!("{stage}_samples")] = serde_json::json!(3);
            let delta = measurement_read_evidence(&start, &measured, true, true);
            assert_eq!(delta["live_nvmeof"]["counters"][format!("{stage}_ns")], 30);
            assert_eq!(
                delta["live_nvmeof"]["counters"][format!("{stage}_samples")],
                2
            );
            measured["live_nvmeof"]
                .as_object_mut()
                .unwrap()
                .remove(&format!("{stage}_samples"));
            assert_eq!(
                measurement_read_evidence(&start, &measured, true, true)["live_nvmeof"]["available"],
                false
            );
            measured["live_nvmeof"][format!("{stage}_samples")] = serde_json::json!(0);
            assert_eq!(
                measurement_read_evidence(&start, &measured, true, true)["live_nvmeof"]["available"],
                false
            );
        }
        end["live_nvmeof"]
            .as_object_mut()
            .unwrap()
            .remove("payload_rpc_requests");
        assert_eq!(
            measurement_read_evidence(&start, &end, true, true)["live_nvmeof"]["available"],
            false
        );
        end["live_nvmeof"]["payload_rpc_requests"] = serde_json::json!(0);
        end["live_nvmeof"]["completed_read_ops"] = serde_json::json!(1);
        assert_eq!(
            measurement_read_evidence(&start, &end, true, true)["live_nvmeof"]["available"],
            false
        );
    }

    #[test]
    fn nvmeof_boundaries_reject_identity_changes_resets_and_missing_samples() {
        let mut reader = serde_json::json!({
            "database_uuid":vec![1;16],"epoch":1,"partition":2,"export_uuid":vec![3;16],
            "logical_requests":0,"logical_members":0,"eligible_hits":0,
            "ineligible_fallbacks":0,"aligned_requested_bytes":0,"aligned_read_bytes":0,
            "io_submissions":0,"io_completions":0,"io_errors":0,"io_timeouts":0,
            "admitted_sqes":0,"merged_members":0,"pool_hits":0,"pool_misses":0,
            "pool_recycled":0,"pool_discarded":0
        });
        for stage in [
            "alignment",
            "admission",
            "queue",
            "submit_to_cq",
            "extraction",
            "verification",
            "lock_wait",
            "lock_hold",
        ] {
            reader[stage] = serde_json::json!({"count":1,"sum_ns":10,"max_ns":10});
        }
        // A large pre-window verification population must not enter measured means.
        reader["submit_to_cq"] = serde_json::json!({"count":65_536,"sum_ns":1_000_000,"max_ns":10});
        let start = serde_json::json!({"available":true,"readers":[reader]});
        let mut end = start.clone();
        end["readers"][0]["io_completions"] = serde_json::json!(2);
        end["readers"][0]["submit_to_cq"] =
            serde_json::json!({"count":65_538,"sum_ns":1_000_030,"max_ns":20});
        let delta = nvmeof_deltas(&start, &end);
        let row = delta["readers"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        assert_eq!(row["counters"]["counters"]["io_completions"], 2);
        assert_eq!(
            row["stages"]["submit_to_cq"]["delta"]["counters"]["sum_ns"],
            30
        );
        assert_eq!(row["stages"]["submit_to_cq"]["lifetime_max_ns_at_end"], 20);
        assert_eq!(row["stages"]["submit_to_cq"]["mean_ns"], 15.0);
        assert!(row["stages"]["submit_to_cq"]["measurement_max_ns"].is_null());
        assert!(row["stages"]["queue"]["mean_ns"].is_null());
        end["readers"][0]["submit_to_cq"]["count"] = serde_json::json!(0);
        let delta = nvmeof_deltas(&start, &end);
        let row = delta["readers"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        assert_eq!(row["stages"]["submit_to_cq"]["delta"]["available"], false);
        assert_eq!(delta["available"], false);
        end["readers"][0]["export_uuid"] = serde_json::json!(vec![4; 16]);
        assert_eq!(nvmeof_deltas(&start, &end)["available"], false);
        assert_eq!(
            nvmeof_deltas(&start, &serde_json::json!({}))["available"],
            false
        );
        let duplicate = serde_json::json!({"available":true,"readers":[start["readers"][0],start["readers"][0]]});
        assert_eq!(nvmeof_deltas(&duplicate, &duplicate)["available"], false);
        let mut missing = start.clone();
        missing["readers"][0]["alignment"]
            .as_object_mut()
            .unwrap()
            .remove("sum_ns");
        assert_eq!(nvmeof_deltas(&start, &missing)["available"], false);
        let mut saturated = start.clone();
        saturated["readers"][0]["io_completions"] = serde_json::json!(u64::MAX);
        assert_eq!(nvmeof_deltas(&start, &saturated)["available"], false);
        let mut invalid_identity = start.clone();
        invalid_identity["readers"][0]["database_uuid"] = serde_json::json!([1]);
        assert_eq!(nvmeof_deltas(&start, &invalid_identity)["available"], false);
    }

    fn read_boundary(
        owner_executor: &serde_json::Value,
        nvmeof: &serde_json::Value,
    ) -> serde_json::Value {
        let coordinator: serde_json::Map<String, serde_json::Value> = [
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
            "read_profile_live_inline",
            "read_profile_live_inline_fallback_drain",
            "read_profile_live_inline_fallback_busy",
            "read_profile_live_inline_fallback_grant",
            "read_profile_live_inline_fallback_credit",
            "read_profile_live_inline_fallback_paused",
            "read_profile_live_inline_fallback_coalesced",
        ]
        .into_iter()
        .map(|name| (name.to_owned(), serde_json::json!(0)))
        .collect();
        serde_json::json!({
            "available": true, "coordinator": coordinator,
            "owner_executor": owner_executor, "nvmeof": nvmeof
        })
    }

    #[test]
    fn live_inline_boundaries_require_complete_monotone_route_counts() {
        let start = read_boundary(&serde_json::Value::Null, &serde_json::Value::Null);
        let mut end = start.clone();
        end["coordinator"]["read_profile_live_inline"] = serde_json::json!(7);
        let measured = measurement_read_evidence(&start, &end, true, true);
        assert_eq!(
            measured["deltas"]["coordinator"]["counters"]["read_profile_live_inline"],
            7
        );
        let reset = measurement_read_evidence(&end, &start, true, true);
        assert_eq!(reset["deltas"]["coordinator"]["available"], false);
        end["coordinator"]
            .as_object_mut()
            .unwrap()
            .remove("read_profile_live_inline_fallback_grant");
        let missing = measurement_read_evidence(&start, &end, true, true);
        assert_eq!(missing["deltas"]["coordinator"]["available"], false);
    }

    #[test]
    fn evidence_carries_owner_wide_transport_send_counters() {
        let owner = |sizes: [u64; 4], eagain: u64, retries: u64, waits: u64| {
            let mut stats =
                serde_json::to_value(tidehunter::distributed::tcp::ReadExecutorStats::default())
                    .unwrap();
            stats["transport"] = serde_json::json!({
                "send_size_le_4k": sizes[0], "send_size_le_16k": sizes[1],
                "send_size_le_64k": sizes[2], "send_size_gt_64k": sizes[3],
                "send_post_eagain": eagain, "send_post_retries": retries,
                "cq_idle_waits": waits,
            });
            serde_json::json!({"available": true, "partitions": {"0": stats}})
        };
        let start = read_boundary(&owner([100, 20, 3, 0], 5, 4, 9), &serde_json::Value::Null);
        let end = read_boundary(&owner([160, 70, 9, 1], 8, 6, 19), &serde_json::Value::Null);
        let evidence = measurement_read_evidence(&start, &end, true, true);
        let counters = &evidence["deltas"]["owner_executor"]["partitions"]["0"]["counters"];
        for (name, expected) in [
            ("transport.send_size_le_4k", 60),
            ("transport.send_size_le_16k", 50),
            ("transport.send_size_le_64k", 6),
            ("transport.send_size_gt_64k", 1),
            ("transport.send_post_eagain", 3),
            ("transport.send_post_retries", 2),
            ("transport.cq_idle_waits", 10),
        ] {
            assert_eq!(counters[name], expected, "{name}");
        }
        // A reset owner counter must make the sample unavailable, never wrap.
        let reset = measurement_read_evidence(&end, &start, true, true);
        assert_eq!(
            reset["deltas"]["owner_executor"]["partitions"]["0"]["available"],
            false
        );
    }

    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    #[test]
    fn evidence_carries_sealed_reader_lock_admission_and_pool_aggregates() {
        use tidehunter::distributed::nvmeof::{NvmeofStats, StageStats};
        let mut reader = NvmeofStats {
            database_uuid: [1; 16],
            epoch: 1,
            partition: 2,
            export_uuid: [3; 16],
            ..NvmeofStats::default()
        };
        reader.lock_wait = StageStats {
            count: 1,
            sum_ns: 100,
            max_ns: 100,
        };
        reader.lock_hold = StageStats {
            count: 4,
            sum_ns: 400,
            max_ns: 150,
        };
        reader.admitted_sqes = 10;
        reader.merged_members = 2;
        reader.pool_hits = 7;
        reader.pool_misses = 3;
        reader.pool_recycled = 6;
        reader.pool_discarded = 1;
        let snapshot =
            |reader: &NvmeofStats| serde_json::json!({"available": true, "readers": [reader]});
        let start = read_boundary(&serde_json::Value::Null, &snapshot(&reader));
        reader.lock_wait = StageStats {
            count: 4,
            sum_ns: 700,
            max_ns: 300,
        };
        reader.lock_hold = StageStats {
            count: 12,
            sum_ns: 1_600,
            max_ns: 150,
        };
        reader.admitted_sqes = 40;
        reader.merged_members = 12;
        reader.pool_hits = 27;
        reader.pool_misses = 5;
        reader.pool_recycled = 26;
        reader.pool_discarded = 4;
        let end = read_boundary(&serde_json::Value::Null, &snapshot(&reader));
        let evidence = measurement_read_evidence(&start, &end, true, true);
        assert_eq!(evidence["nvmeof"]["available"], true);
        let row = evidence["nvmeof"]["readers"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        for (name, expected) in [
            ("admitted_sqes", 30),
            ("merged_members", 10),
            ("pool_hits", 20),
            ("pool_misses", 2),
            ("pool_recycled", 20),
            ("pool_discarded", 3),
        ] {
            assert_eq!(row["counters"]["counters"][name], expected, "{name}");
        }
        for (stage, count, sum, mean) in
            [("lock_wait", 3, 600, 200.0), ("lock_hold", 8, 1_200, 150.0)]
        {
            let delta = &row["stages"][stage]["delta"]["counters"];
            assert_eq!(
                (delta["count"].as_u64(), delta["sum_ns"].as_u64()),
                (Some(count), Some(sum)),
                "{stage}"
            );
            assert_eq!(row["stages"][stage]["mean_ns"], mean, "{stage}");
        }
        assert_eq!(row["stages"]["lock_wait"]["lifetime_max_ns_at_end"], 300);
        assert_eq!(row["available"], true);
    }
}
