//! Cold reopen, owner residency, and full verification for a retained Q1 case.

use super::super::cold::{Residency, evict_tree, page_size};
use super::super::{DatasetIdentity, RowCheck, iterated_rows, verify_rows};
use super::input::StageMode;
use super::{
    Context, EVIDENCE, OVERWRITE_STOP, OWNER_OBSERVE, OWNERS, Owners, PILOT_TOUCHED,
    PILOT_UNCERTAIN, Phase, PhaseReport, Status, TIMED_TOUCHED, TIMED_UNCERTAIN, check_database,
    coordinator_config, evidence, file_sha256, finish, frontier_json, keyspace, open_coordinator,
    owner_databases, predecessors, spec_after_build,
};
use crate::backend::BackendResult;
use crate::index_a1::{TouchedBitmap, ValueDomain};
use crate::workload::key_bytes;
use serde_json::{Value, json};
use std::fs;
use std::time::Instant;
use tidehunter::distributed::remote_db::diagnostics_clock_ns;
use tidehunter::distributed::tcp::host::identity::database_hex;

/// Owner page residency on the owner guests: local proof measures the owner
/// roots here; fleet reads each owner's `owner-observe` receipt.
fn owner_residency(context: &Context, database: &[u8; 16]) -> BackendResult<(Residency, Value)> {
    let mut total = Residency::default();
    let mut evidence = Vec::new();
    match context.stage.mode {
        StageMode::LocalFixtureProof => {
            let page = page_size()?;
            for owner in &context.stage.owners {
                let mut residency = Residency::default();
                evict_tree(&owner.root, page, 0, &mut residency)?;
                evidence.push(
                    json!({"partition": owner.partition, "residency": residency.json(),
                    "measured": "this process, owner stopped"}),
                );
                add(&mut total, &residency);
            }
        }
        StageMode::Fleet => {
            for owner in &context.stage.owners {
                let path = context
                    .stage
                    .case_root
                    .join(OWNERS)
                    .join(format!("p{}.observe.json", owner.partition));
                let receipt = evidence::read_json(&path)?;
                let terminal = &receipt["terminal"];
                if receipt["phase"] != Phase::OwnerObserve.name()
                    || receipt["status"] != "complete"
                    || receipt["input_sha256"] != context.stage_sha256.as_str()
                    || terminal["partition"] != owner.partition
                    || terminal["evicted"] != true
                    || !terminal["databases"]
                        .as_array()
                        .is_some_and(|ids| ids.iter().any(|id| *id == database_hex(database)))
                {
                    return Err(format!(
                        "owner {} observe receipt is not an evicted observation of this stage and database",
                        owner.partition
                    )
                    .into());
                }
                let residency = &terminal["residency"];
                let field = |name: &str| {
                    residency[name].as_u64().ok_or_else(|| {
                        format!(
                            "owner {} observe receipt lacks residency.{name}",
                            owner.partition
                        )
                    })
                };
                let observed = Residency {
                    files: field("files")?,
                    bytes: field("bytes")?,
                    allocated_bytes: field("allocated_bytes")?,
                    resident_bytes: field("resident_bytes")?,
                    selected_pages: field("selected_pages")?,
                    resident_pages: field("resident_pages")?,
                };
                let (bytes, sha) = file_sha256(&path)?;
                evidence.push(json!({"partition": owner.partition, "residency": residency,
                    "receipt": path, "receipt_bytes": bytes, "receipt_sha256": sha}));
                add(&mut total, &observed);
            }
        }
    }
    Ok((total, Value::Array(evidence)))
}

fn add(total: &mut Residency, part: &Residency) {
    total.files += part.files;
    total.bytes += part.bytes;
    total.allocated_bytes += part.allocated_bytes;
    total.resident_bytes += part.resident_bytes;
    total.selected_pages += part.selected_pages;
    total.resident_pages += part.resident_pages;
}

/// The selection is a superset of the replay pages (whole trees), so a
/// fraction over it can hide a fully resident replay set behind cold
/// unrelated pages. The superset qualifies only with a nonempty selection
/// and zero resident pages; otherwise the pages are `null` (unqualified).
pub(super) fn superset_cold(residency: &Residency) -> bool {
    residency.selected_pages > 0 && residency.resident_pages == 0
}

pub(super) fn pages(residency: &Residency) -> Value {
    if superset_cold(residency) {
        json!({"selected": residency.selected_pages, "resident": 0})
    } else {
        Value::Null
    }
}

/// The expected final payload of `id` and the other acceptable payloads.
/// Timed acknowledgements win, then pilot, then the build value; a domain
/// with an uncertain (dispatched, unacknowledged) put admits its value
/// beside the prior one.
pub(super) fn expected_domains(
    id: u64,
    timed: &TouchedBitmap,
    timed_uncertain: &TouchedBitmap,
    pilot: &TouchedBitmap,
    pilot_uncertain: &TouchedBitmap,
) -> BackendResult<(ValueDomain, Vec<ValueDomain>)> {
    if timed.is_set(id)? {
        return Ok((ValueDomain::Timed, Vec::new()));
    }
    let (prior, mut alternates) = if pilot.is_set(id)? {
        (ValueDomain::Pilot, Vec::new())
    } else if pilot_uncertain.is_set(id)? {
        (ValueDomain::Build, vec![ValueDomain::Pilot])
    } else {
        (ValueDomain::Build, Vec::new())
    };
    if timed_uncertain.is_set(id)? {
        alternates.push(ValueDomain::Timed);
    }
    Ok((prior, alternates))
}

/// The four phase bitmaps of the case.
pub(super) struct Domains {
    pub(super) timed: TouchedBitmap,
    pub(super) timed_uncertain: TouchedBitmap,
    pub(super) pilot: TouchedBitmap,
    pub(super) pilot_uncertain: TouchedBitmap,
}

impl Domains {
    fn load(context: &Context) -> BackendResult<Self> {
        Ok(Self {
            timed: context.bitmap(TIMED_TOUCHED)?,
            timed_uncertain: context.bitmap(TIMED_UNCERTAIN)?,
            pilot: context.bitmap(PILOT_TOUCHED)?,
            pilot_uncertain: context.bitmap(PILOT_UNCERTAIN)?,
        })
    }

    pub(super) fn check(
        &self,
        dataset: &DatasetIdentity,
        id: u64,
        value: Option<&[u8]>,
    ) -> BackendResult<RowCheck> {
        let (primary, alternates) = expected_domains(
            id,
            &self.timed,
            &self.timed_uncertain,
            &self.pilot,
            &self.pilot_uncertain,
        )?;
        let Some(value) = value else {
            return Ok(RowCheck::Missing);
        };
        if value == dataset.value(primary, id)?.as_ref() {
            return Ok(RowCheck::Correct);
        }
        for domain in alternates {
            if value == dataset.value(domain, id)?.as_ref() {
                return Ok(RowCheck::Ambiguous);
            }
        }
        Ok(RowCheck::Wrong)
    }
}

/// Nonces of the owner start receipts an earlier receipt recorded.
fn recorded_nonces(receipt: &Value) -> Vec<String> {
    receipt["owners"]
        .as_array()
        .map(|owners| {
            owners
                .iter()
                .filter_map(|owner| owner["start_receipt"]["launch_nonce"].as_str())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

#[allow(clippy::too_many_lines)] // One ordered evidence sequence; splitting hides the timing order.
pub(super) fn cold_reopen(context: &Context) -> BackendResult<PhaseReport> {
    let stage = &context.stage;
    let predecessors = predecessors(context)?;
    let database = context.database()?;
    let (overwrite, _) = context.receipt(Phase::Overwrite)?;
    let stop = evidence::read_json(&context.registered(OVERWRITE_STOP))?;
    let domains = Domains::load(context)?;
    let touched_key = domains
        .timed
        .first()
        .ok_or("no timed acknowledgement: nothing to read cold")?;
    if stop["touched_key"].as_u64() != Some(touched_key) {
        return Err("stop record names another first timed key".into());
    }
    let spec = spec_after_build(context)?;
    // Residency on the correct guests, before any fresh process starts.
    let page = page_size()?;
    let mut coordinator = Residency::default();
    let eviction_started = Instant::now();
    evict_tree(&stage.coordinator_path(), page, 0, &mut coordinator)?;
    let (owners_residency, owner_residency_evidence) = owner_residency(context, &database)?;
    let eviction_ns = crate::duration_ns(eviction_started.elapsed());
    // Whole-tree rule: the nominated value block is provably nonresident
    // only when no owner page stayed resident.
    let first_block_resident = owners_residency.resident_pages > 0;
    let owners = Owners::start(context, spec.as_ref(), "cold")?;
    let fresh_owner_processes = {
        let before = recorded_nonces(&overwrite);
        let now = recorded_nonces(&json!({"owners": owners.evidence}));
        before.len() == 2 && now.len() == 2 && now.iter().all(|nonce| !before.contains(nonce))
    };
    let owner_evidence = owners.evidence.clone();
    let config = coordinator_config(context, spec.as_ref())?;
    let keyspace = keyspace()?;
    let namespace = stage.dataset_identity().namespace()?;
    // Stopwatch: coordinator open after owners are ready, to the first
    // correct read; no warmup or verification before that read.
    let open_started = Instant::now();
    let open_start_ns = diagnostics_clock_ns();
    let opened = open_coordinator(context, config);
    let db = match opened {
        Ok(db) => db,
        Err(error) => {
            let _ = owners.stop();
            return Err(error);
        }
    };
    let open_ns = crate::duration_ns(open_started.elapsed());
    let recovery = db.remote_recovery_stats();
    let recovered = db.index_checkpoint_snapshot();
    let frontier = db.published_frontier().ok().flatten();
    let key = key_bytes(&namespace, stage.dataset.seed, touched_key);
    let read_started = Instant::now();
    let first = db.get(keyspace, &key);
    let first_read_ns = crate::duration_ns(read_started.elapsed());
    let open_to_read_ns = crate::duration_ns(open_started.elapsed());
    let dataset = stage.dataset_identity();
    let first_correct = match &first {
        Ok(value) => domains.check(&dataset, touched_key, value.as_deref())? == RowCheck::Correct,
        Err(_) => false,
    };
    let same_database = check_database(&db, Some(database)).is_ok();
    let tail_matches = recovery.as_ref().is_some_and(|stats| {
        stats.checkpoint_loaded
            && stats.cut_discarded_batches == 0
            && stop["suffix_batches"].as_u64() == Some(stats.tail_batches)
            && stop["suffix_bytes"].as_u64() == Some(stats.tail_bytes)
    });
    let frontier_matches =
        frontier_json(frontier) == stop["frontier"] && !stop["frontier"].is_null();
    let generation_advanced = recovered
        .as_ref()
        .zip(stop["fencing_generation"].as_u64())
        .is_some_and(|(snapshot, stopped)| snapshot.fencing_generation > stopped);
    // Orderly close after the timed read; `verify` reopens.
    let close = db
        .wait_for_background_threads_to_finish()
        .map_err(|error| format!("{error:?}"));
    let owner_stop = owners.stop().map_err(|error| error.to_string());
    let cold = superset_cold(&coordinator) && superset_cold(&owners_residency);
    let complete = first_correct
        && same_database
        && tail_matches
        && frontier_matches
        && generation_advanced
        && fresh_owner_processes
        && close.is_ok()
        && owner_stop.is_ok();
    Ok(PhaseReport {
        status: if complete { Status::Complete } else { Status::Failed },
        error: (!complete).then(|| {
            format!(
                "cold-reopen: first_correct={first_correct}, same_database={same_database}, tail_matches={tail_matches}, frontier_matches={frontier_matches}, generation_advanced={generation_advanced}, fresh_owner_processes={fresh_owner_processes}, close={close:?}, owner_stop={owner_stop:?}"
            )
        }),
        database: Some(database),
        owners: owner_evidence,
        predecessors,
        terminal: json!({
            "database_id": database_hex(&database),
            "frontier": frontier_json(frontier),
            "stop_frontier": stop["frontier"],
            "frontier_matches": frontier_matches,
            "suffix_batches": recovery.as_ref().map(|stats| stats.tail_batches),
            "suffix_bytes": recovery.as_ref().map(|stats| stats.tail_bytes),
            "stop_suffix_batches": stop["suffix_batches"],
            "stop_suffix_bytes": stop["suffix_bytes"],
            "tail_matches": tail_matches,
            "touched_key": touched_key,
            "open_to_read_ns": open_to_read_ns,
            "open_ns": open_ns,
            "first_read_ns": first_read_ns,
            "open_start_ns": open_start_ns,
            "first_correct": first_correct,
            "first_read_error": first.as_ref().err().map(|error| format!("{error:?}")),
            "discarded": recovery.as_ref().map(|stats| stats.cut_discarded_batches),
            "index_pages": pages(&coordinator),
            "wal_pages": pages(&owners_residency),
            "page_selection": "every page of the coordinator tree and of both owner host roots (a superset of the replay and first-value pages); qualified only with a nonempty selection and zero resident pages, else null",
            "cold_unqualified_reason": (!cold).then_some("a resident page remained in the measured superset, or the selection was empty; the replay pages themselves were not measured"),
            "first_block_resident": first_block_resident,
            "first_block_rule": "nonresident only when no owner page stayed resident after whole-tree eviction",
            "cold": cold,
            "warmup": false,
            "fresh_owner_processes": fresh_owner_processes,
            "same_database": same_database,
            "fencing_generation_advanced": generation_advanced,
            "recovered_fencing_generation": recovered.as_ref().map(|snapshot| snapshot.fencing_generation),
            "recovery": recovery.as_ref().map(crate::index_checkpoint::recovery_json),
            "recovered_snapshot": recovered,
            "residency": {
                "coordinator": coordinator.json(),
                "owners": owners_residency.json(),
                "owner_observations": owner_residency_evidence,
                "eviction_ns": eviction_ns,
            },
            "close_error": close.err(),
            "owner_stop_error": owner_stop.err(),
            "rows": Value::Null,
            "rows_source": "verify-receipt.json (every row is verified by the verify phase)",
        }),
        artifacts: Vec::new(),
        output_files: Vec::new(),
    })
}

/// Owner shutdown and target inventory receipts of the case: the local
/// fixture owners' start/shutdown records, or the fleet owner files the
/// controller published under `<case_root>/owners/`.
fn owner_inventory(context: &Context, database: &[u8; 16]) -> BackendResult<Value> {
    let root = match context.stage.mode {
        StageMode::LocalFixtureProof => context.stage.case_root.join(EVIDENCE),
        StageMode::Fleet => context.stage.case_root.join(OWNERS),
    };
    let mut files = Vec::new();
    let mut foreign = Vec::new();
    let mut pending = vec![root];
    while let Some(directory) = pending.pop() {
        let mut entries: Vec<_> = fs::read_dir(&directory)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                pending.push(path);
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if std::path::Path::new(&name)
                .extension()
                .is_none_or(|extension| extension != "json")
                || name.ends_with(".launch.json")
            {
                continue;
            }
            let (bytes, sha) = file_sha256(&path)?;
            let document = evidence::read_json(&path).unwrap_or(Value::Null);
            let targets: Vec<String> = document["targets"]
                .as_array()
                .map(|targets| {
                    targets
                        .iter()
                        .filter_map(|target| match &target["database"] {
                            Value::String(hex) => Some(hex.clone()),
                            Value::Array(_) => evidence_database(&target["database"]),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default();
            foreign.extend(
                targets
                    .iter()
                    .filter(|hex| **hex != database_hex(database))
                    .cloned(),
            );
            files.push(json!({
                "file": path, "bytes": bytes, "sha256": sha,
                "scope": document["scope"], "complete": document["complete"],
                "schema_version": document["schema_version"],
                "target_databases": targets,
            }));
        }
    }
    Ok(json!({"files": files, "foreign_target_databases": foreign}))
}

fn evidence_database(value: &Value) -> Option<String> {
    let bytes: Vec<u8> = value
        .as_array()?
        .iter()
        .map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
        .collect::<Option<_>>()?;
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    Some(database_hex(&bytes))
}

pub(super) fn verify(context: &Context) -> BackendResult<PhaseReport> {
    let stage = &context.stage;
    let predecessors = predecessors(context)?;
    let database = context.database()?;
    let domains = Domains::load(context)?;
    let spec = spec_after_build(context)?;
    let owners = Owners::start(context, spec.as_ref(), "verify")?;
    let owner_evidence = owners.evidence.clone();
    let config = coordinator_config(context, spec.as_ref())?;
    let db = match open_coordinator(context, config) {
        Ok(db) => db,
        Err(error) => {
            let _ = owners.stop();
            return Err(error);
        }
    };
    let keyspace = keyspace()?;
    let dataset = stage.dataset_identity();
    let namespace = dataset.namespace()?;
    let checked = check_database(&db, Some(database)).and_then(|_| {
        let (tally, rows) = verify_rows(&db, keyspace, &namespace, &dataset, |id, value| {
            domains.check(&dataset, id, value)
        })?;
        Ok((tally, rows, iterated_rows(&db, keyspace)?))
    });
    let close = db
        .wait_for_background_threads_to_finish()
        .map_err(|error| format!("{error:?}"));
    let owner_stop = owners.stop().map_err(|error| error.to_string());
    let (tally, rows, iterated) = checked?;
    let inventory = owner_inventory(context, &database)?;
    let owner_databases = owner_databases(context)?;
    let extra = iterated.saturating_sub(stage.index.keys);
    let foreign = inventory["foreign_target_databases"]
        .as_array()
        .is_some_and(|foreign| !foreign.is_empty());
    let complete = tally.mismatched() == 0
        && iterated == stage.index.keys
        && !foreign
        && close.is_ok()
        && owner_stop.is_ok();
    Ok(PhaseReport {
        status: if complete { Status::Complete } else { Status::Failed },
        error: (!complete).then(|| {
            format!(
                "verify: missing={}, wrong={}, iterated={iterated}, foreign_targets={foreign}, close={close:?}, owner_stop={owner_stop:?}",
                tally.missing, tally.wrong
            )
        }),
        database: Some(database),
        owners: owner_evidence,
        predecessors,
        terminal: json!({
            "database_id": database_hex(&database),
            "rows": tally.correct + tally.ambiguous,
            "correct": tally.correct,
            "ambiguous_accepted": tally.ambiguous,
            "missing": tally.missing,
            "wrong": tally.wrong,
            "iterated_rows": iterated,
            "extra_rows": extra,
            "row_evidence": rows,
            "domains": {
                "timed": domains.timed.count(),
                "timed_uncertain": domains.timed_uncertain.count(),
                "pilot": domains.pilot.count(),
                "pilot_uncertain": domains.pilot_uncertain.count(),
                "rule": "timed acknowledgement, else pilot acknowledgement, else build value; an uncertain put admits its domain value beside the prior one (ambiguous_accepted)",
            },
            "owner_inventory": inventory,
            "owner_databases": owner_databases,
            "close_error": close.err(),
            "owner_stop_error": owner_stop.err(),
        }),
        artifacts: Vec::new(),
        output_files: Vec::new(),
    })
}

/// Owner guest: allocation, database inventory, eviction and residency of
/// the owner host root of `partition`. Run with the owner process stopped
/// (the controller supervises it); the receipt is copied to the coordinator
/// guest as `<case_root>/owners/p<N>.observe.json`.
pub(super) fn owner_observe(context: &Context, partition: u32) -> BackendResult<()> {
    let owner = context
        .stage
        .owners
        .iter()
        .find(|owner| owner.partition == partition)
        .ok_or("--partition names no stage owner")?;
    let observed = (|| -> BackendResult<Value> {
        let allocation = evidence::tree_allocation(&owner.root)?;
        let databases = evidence::host_databases(&owner.root)?;
        let page = page_size()?;
        let mut residency = Residency::default();
        evict_tree(&owner.root, page, 0, &mut residency)?;
        Ok(json!({
            "partition": partition,
            "root": owner.root,
            "root_fingerprint": context.stage.index.owner_roots.get(usize::try_from(partition)?),
            "allocation": allocation,
            "allocated_bytes": allocation.allocated_bytes,
            "databases": databases,
            "evicted": true,
            "residency": residency.json(),
        }))
    })();
    let complete = observed.is_ok();
    let report = match observed {
        Ok(terminal) => PhaseReport {
            status: Status::Complete,
            error: None,
            database: None,
            owners: Value::Null,
            predecessors: Vec::new(),
            terminal,
            artifacts: Vec::new(),
            output_files: Vec::new(),
        },
        Err(error) => PhaseReport::failed(error.to_string()),
    };
    finish(context, report)?;
    if complete {
        Ok(())
    } else {
        Err(format!(
            "owner-observe failed; see {}",
            context.output.join(OWNER_OBSERVE).display()
        )
        .into())
    }
}
