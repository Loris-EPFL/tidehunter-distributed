//! Bounded end-to-end qualification against already running owners.
use super::*;
use crate::workload::{key_bytes, value_bytes};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    #[serde(default)]
    remote_config: Option<RemoteDatabaseConfig>,
    coordinator_path: PathBuf,
    namespace: [u8; 16],
    expected_route: String,
    durability_tier: String,
    #[serde(default = "default_target_implementation")]
    target_implementation: String,
    value_bytes: usize,
}

fn default_target_implementation() -> String {
    "kernel".to_owned()
}
// This is diagnostic state, never a successful qualification receipt. Updating
// it is best-effort so an evidence I/O error cannot replace the actual failure.
struct QualificationProgress {
    directory: PathBuf,
    file_name: &'static str,
    events: Vec<serde_json::Value>,
}

impl QualificationProgress {
    fn mark(&mut self, phase: &'static str, db: Option<&sui::Db>) {
        let snapshot = db
            .and_then(|db| db.live_nvmeof_stats())
            .and_then(|stats| serde_json::to_value(stats).ok());
        self.mark_value(phase, snapshot);
    }

    fn mark_value(&mut self, phase: &'static str, snapshot: Option<serde_json::Value>) {
        self.events.push(serde_json::json!({
            "phase": phase,
            "live_nvmeof": snapshot,
        }));
        let document = serde_json::json!({
            "schema_version": 1,
            "kind": "live_nvmeof_qualification_progress",
            "status": "partial_not_qualification_proof",
            "events": self.events,
        });
        let result = serde_json::to_vec_pretty(&document)
            .map_err(std::io::Error::other)
            .and_then(|bytes| {
                fs::write(
                    self.directory.join(format!(".{}.tmp", self.file_name)),
                    bytes,
                )
            })
            .and_then(|()| {
                fs::rename(
                    self.directory.join(format!(".{}.tmp", self.file_name)),
                    self.directory.join(self.file_name),
                )
            });
        if let Err(error) = result {
            eprintln!("qualification progress at {phase}: {error}");
        }
    }
}

fn check(condition: bool, message: &'static str) -> BackendResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn stats(db: &sui::Db) -> BackendResult<serde_json::Value> {
    let sample = db
        .live_nvmeof_stats()
        .ok_or("live NVMe-oF stats unavailable")?;
    Ok(serde_json::to_value(sample)?)
}

fn count(sample: &serde_json::Value, field: &str) -> BackendResult<u64> {
    sample
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("live NVMe-oF counter missing: {field}").into())
}

fn delta(start: &serde_json::Value, end: &serde_json::Value, field: &str) -> BackendResult<u64> {
    count(end, field)?
        .checked_sub(count(start, field)?)
        .ok_or_else(|| format!("live NVMe-oF counter decreased: {field}").into())
}

// A single 64 KiB sentinel/filler or tombstone frame must fit this charge.
// Check the observed completion too, so an unexpected frame geometry fails
// rather than silently weakening the qualification's byte bound.
fn transition_write(
    db: &sui::Db,
    start: &serde_json::Value,
    last: &mut serde_json::Value,
    deadline: Instant,
    acknowledged: &mut u64,
    operation: impl FnOnce() -> BackendResult<()>,
) -> BackendResult<()> {
    const BYTE_LIMIT: u64 = 2 * 1024 * 1024 * 1024;
    const FRAME_CHARGE: u64 = 128 * 1024;
    let used = delta(start, last, "completed_write_bytes")?;
    check(
        Instant::now() < deadline
            && *acknowledged < 65536
            && used
                .checked_add(FRAME_CHARGE)
                .is_some_and(|n| n <= BYTE_LIMIT),
        "member transition exceeded its admission bounds",
    )?;
    operation()?;
    *acknowledged += 1;
    let next = stats(db)?;
    check(
        Instant::now() < deadline
            && delta(last, &next, "completed_write_bytes")? <= FRAME_CHARGE
            && delta(start, &next, "completed_write_bytes")? <= BYTE_LIMIT,
        "member transition exceeded its completion bounds",
    )?;
    *last = next;
    Ok(())
}

fn verify_routes(sample: &serde_json::Value, expected: &str, target: &str) -> BackendResult<()> {
    let routes = sample
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .ok_or("observed live namespace routes unavailable")?;
    check(
        count(sample, "route_evidence_dropped")? == 0,
        "live namespace evidence was dropped",
    )?;
    check(!routes.is_empty(), "no observed live namespace route")?;
    for route in routes {
        check(
            route.get("transport").and_then(serde_json::Value::as_str) == Some(expected),
            "observed live namespace transport differs from requested qualification",
        )?;
        let identity = route
            .get("namespace_identity")
            .and_then(serde_json::Value::as_object)
            .ok_or("observed live namespace identity unavailable")?;
        check(
            route
                .get("target_implementation")
                .and_then(serde_json::Value::as_str)
                == Some(target)
                && identity
                    .get("target_implementation")
                    .and_then(serde_json::Value::as_str)
                    == Some(target),
            "opened namespace target implementation differs from requested qualification",
        )?;
        if target == "spdk" {
            let spdk = route
                .get("spdk")
                .and_then(serde_json::Value::as_object)
                .ok_or("opened SPDK route lacks authenticated owner binding")?;
            check(
                spdk.get("rpc_socket")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|s| Path::new(s).is_absolute())
                    && spdk
                        .get("image_path")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|s| Path::new(s).is_absolute())
                    && spdk
                        .get("pid")
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|n| n > 0)
                    && spdk
                        .get("process_start_ticks")
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|n| n > 0)
                    && spdk
                        .get("bdev_name")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|s| !s.is_empty()),
                "incomplete authenticated SPDK owner binding",
            )?;
            if let Some(writer) = route.get("writer").and_then(serde_json::Value::as_object) {
                let file = writer
                    .get("file_identity")
                    .and_then(serde_json::Value::as_object)
                    .ok_or("SPDK writer lacks authenticated WAL file identity")?;
                check(
                    file.get("path") == spdk.get("image_path")
                        && file
                            .get("device")
                            .and_then(serde_json::Value::as_u64)
                            .is_some()
                        && file
                            .get("inode")
                            .and_then(serde_json::Value::as_u64)
                            .is_some_and(|n| n > 0)
                        && file.get("bytes") == identity.get("bytes")
                        && file.get("block_size") == identity.get("block_size"),
                    "SPDK writer binding does not match authenticated WAL file",
                )?;
            }
        }
        check(
            identity
                .get("namespace_path")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| s.starts_with("/dev/"))
                && identity
                    .get("nqn")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|s| s.starts_with("nqn."))
                && identity
                    .get("nsid")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|n| n > 0)
                && identity
                    .get("bytes")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|n| n > 0)
                && identity
                    .get("controller_instance")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && identity
                    .get("block_size")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|n| n >= 512 && n.is_power_of_two())
                && identity
                    .get("uuid")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|id| {
                        id.len() == 16
                            && id.iter().all(|b| b.as_u64().is_some_and(|v| v <= 255))
                            && id.iter().any(|b| b.as_u64().is_some_and(|v| v != 0))
                    })
                && route
                    .get("address")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|s| s.parse::<std::net::IpAddr>().is_ok())
                && route
                    .get("service")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|n| n > 0),
            "incomplete observed live namespace identity",
        )?;
    }
    Ok(())
}
fn verify_fences(
    sample: &serde_json::Value,
    partitions: &[u32],
    target: &str,
) -> BackendResult<()> {
    check(
        count(sample, "fence_evidence_dropped")? == 0,
        "live fence evidence was dropped",
    )?;
    let fences = sample
        .get("fences")
        .and_then(serde_json::Value::as_array)
        .ok_or("live fence evidence unavailable")?;
    for partition in partitions {
        check(
            fences.iter().any(|f| {
                f.get("partition").and_then(serde_json::Value::as_u64)
                    == Some(u64::from(*partition))
                    && f.get("authority_epoch")
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|epoch| epoch >= 2)
                    && f["target"]["fixture_only"] == false
                    && f["target"]["target_implementation"].as_str() == Some(target)
                    && f["target"]["disable_drain_successes"]
                        .as_u64()
                        .is_some_and(|n| n > 0)
                    && f["target"]["errors"].as_u64() == Some(0)
            }),
            "missing observed target namespace fence/drain",
        )?;
    }
    Ok(())
}
fn shutdown_targets(
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    target: &str,
) -> BackendResult<Vec<serde_json::Value>> {
    use tidehunter::distributed::storage::{PartitionConfig, StaticInventory};
    use tidehunter::distributed::tcp::Client;
    let mut receipts = Vec::with_capacity(config.endpoints.len());
    for endpoint in &config.endpoints {
        let storage = &config.storage;
        let mut tls = config.tls.clone();
        tls.server_name.clone_from(&endpoint.server_name);
        let partition = PartitionConfig {
            inventory: StaticInventory {
                database: storage.database,
                epoch: 1,
                segment_generation: 1,
                partitions: storage.partitions.clone(),
            },
            partition: endpoint.partition,
            batch_limits: storage.batch_limits,
            group_limits: storage.group_limits,
            fragment_bytes: storage.fragment_bytes,
            wal_file_bytes: storage.wal_file_bytes,
            max_batches: storage.max_batches_per_partition,
            max_reference_metadata_bytes: storage.max_reference_metadata_bytes,
        };
        let mut client =
            Client::connect(&endpoint.address, &tls, config.timeout, partition, shape)?;
        let status = client.status()?;
        check(status.live_nvmeof, "owner does not report live target")?;
        let epoch = status
            .epochs
            .last()
            .ok_or("owner has no authority epoch")?
            .epoch;
        if status.active_epoch == Some(epoch) {
            client.seal(epoch)?;
        } else {
            check(
                status.active_epoch.is_none() && status.epochs.last().is_some_and(|e| e.sealed),
                "owner authority not in expected latest epoch",
            )?;
        }
        let stats = client.shutdown_live_target(epoch)?;
        check(
            !stats.fixture_only
                && stats.owned_namespace_count == 0
                && stats.errors == 0
                && matches!(
                    (stats.target_implementation, target),
                    (
                        tidehunter::distributed::nvmeof::LiveTargetImplementation::Kernel,
                        "kernel"
                    ) | (
                        tidehunter::distributed::nvmeof::LiveTargetImplementation::Spdk,
                        "spdk"
                    )
                ),
            "owner target shutdown did not prove matching namespace cleanup",
        )?;
        receipts.push(serde_json::json!({
            "partition": endpoint.partition, "endpoint": endpoint.address,
            "database": storage.database, "epoch": epoch, "stats": stats
        }));
    }
    Ok(receipts)
}

fn read_expected(
    db: &sui::Db,
    ks: tidehunter::key_shape::KeySpace,
    key: &Bytes,
    expected: Option<&Bytes>,
) -> BackendResult<()> {
    let actual = db.get(ks, key).map_err(native_error)?;
    check(
        actual.as_ref() == expected,
        "live qualification value mismatch",
    )
}

fn read_expected_live(
    db: &sui::Db,
    ks: tidehunter::key_shape::KeySpace,
    key: &Bytes,
    expected: &Bytes,
) -> BackendResult<()> {
    let before = stats(db)?;
    read_expected(db, ks, key, Some(expected))?;
    let after = stats(db)?;
    check(
        delta(&before, &after, "completed_read_ops")? > 0
            && delta(&before, &after, "completed_read_bytes")? > 0,
        "member sentinel read did not complete live namespace I/O",
    )
}
fn run_local_file(input: &Input, output: &Path) -> BackendResult<()> {
    use std::os::unix::fs::FileExt;
    use tidehunter::distributed::{
        codec::{BatchLimits, CompleteBatch, Mutation},
        group::{CompleteGroup, GroupLimits},
        nvmeof::live_storage::{LiveGroupDescriptor, LiveReserveResult},
        storage::{PartitionConfig, PartitionLog, StaticInventory},
        types::{BatchId, BatchVersion, RequestAuthority},
    };
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let ks = shape.iter_ks().next().ok_or("missing keyspace")?.id();
    let config = PartitionConfig {
        inventory: StaticInventory {
            database: input.namespace,
            epoch: 1,
            segment_generation: 1,
            partitions: vec![0],
        },
        partition: 0,
        batch_limits: BatchLimits {
            max_operations: 4,
            max_encoded_bytes: 512 * 1024,
        },
        group_limits: GroupLimits {
            max_batches: 4,
            max_encoded_bytes: 1024 * 1024 - 8,
        },
        fragment_bytes: 1024 * 1024,
        wal_file_bytes: 2 * 1024 * 1024,
        max_batches: 64,
        max_reference_metadata_bytes: 1024 * 1024,
    };
    let metrics =
        || tidehunter::metrics::Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let log =
        PartitionLog::create_live(&input.coordinator_path, config.clone(), &shape, metrics())?;
    let authority = RequestAuthority { epoch: 1 };
    let mut receipts = Vec::new();
    let mut batches = Vec::new();
    for n in 0..3u64 {
        let key = key_bytes(&input.namespace, 17, n);
        let value = value_bytes(&input.namespace, 29, n, input.value_bytes, 1.0);
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&(n + 1).to_be_bytes());
        let batch = CompleteBatch::new(
            &shape,
            BatchId {
                database: input.namespace,
                operation,
            },
            BatchVersion {
                epoch: 1,
                sequence: n + 1,
            },
            vec![
                Mutation::Put {
                    keyspace: ks,
                    key,
                    value,
                },
                Mutation::Delete {
                    keyspace: ks,
                    key: key_bytes(&input.namespace, 31, n),
                },
            ],
            config.batch_limits,
        )?;
        let group = CompleteGroup::new_live(
            &shape,
            vec![batch.clone()],
            config.batch_limits,
            config.group_limits,
        )?;
        let (descriptor, frame) = LiveGroupDescriptor::from_group(&group)?;
        let reservation = match log.reserve_live_group(authority, descriptor.clone())? {
            LiveReserveResult::Reserved(reservation) => reservation,
            LiveReserveResult::Existing(_) => {
                return Err("fresh local group unexpectedly existed".into());
            }
        };
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&reservation.segment.path)?;
        file.write_all_at(&frame, reservation.frame_offset % reservation.segment.bytes)?;
        let appended = log.complete_live_group(authority, reservation.token)?;
        check(appended.len() == 1, "local group receipt count mismatch")?;
        log.persist_many(&appended)?;
        check(
            log.complete_live_group(authority, reservation.token)? == appended,
            "local finalize retry mismatch",
        )?;
        check(
            matches!(log.reserve_live_group(authority, descriptor)?, LiveReserveResult::Existing(found) if found == appended),
            "local reservation retry mismatch",
        )?;
        check(
            log.read_batch(appended[0])?.encoded() == batch.encoded(),
            "local persisted batch mismatch",
        )?;
        receipts.push(appended[0]);
        batches.push(batch);
    }
    drop(log);
    // No target or initiator namespace was created by this positional-file path.
    let recovered =
        PartitionLog::recover_live_sealed(&input.coordinator_path, &config, metrics(), || Ok(()))?;
    for (receipt, batch) in receipts.iter().zip(&batches) {
        check(
            recovered.read_batch(*receipt)?.encoded() == batch.encoded(),
            "local recovered batch mismatch",
        )?;
    }
    drop(recovered);
    let proof = serde_json::json!({
        "schema_version": 1, "kind": "live_nvmeof_file_component_qualification",
        "durability_tier": input.durability_tier,
        "scope": "local positional file protocol; not kernel NVMe-oF, RDMA, or hardware performance",
        "frame_count": receipts.len(), "mixed_put_delete": true,
        "owner_validate_persist_retry_recovery": true
    });
    fs::write(
        output.join("qualification.json"),
        serde_json::to_vec_pretty(&proof)?,
    )?;
    Ok(())
}
fn read_input(input_path: &Path) -> BackendResult<(Input, Vec<u8>)> {
    use std::io::Read;
    const MAX_INPUT_BYTES: usize = 1024 * 1024;
    let mut bytes = Vec::new();
    fs::File::open(input_path)?
        .take((MAX_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    check(
        bytes.len() <= MAX_INPUT_BYTES,
        "qualification input exceeds 1 MiB",
    )?;
    Ok((serde_json::from_slice(&bytes)?, bytes))
}
fn validate_tier(input: &Input) -> BackendResult<()> {
    check(
        matches!(input.durability_tier.as_str(), "native" | "synchronous"),
        "durability_tier must be native or synchronous",
    )
}

pub(super) fn run_abrupt_child(input_path: &Path, ready: &Path) -> BackendResult<()> {
    use std::io::Write;
    let (input, _) = read_input(input_path)?;
    validate_tier(&input)?;
    let remote = input
        .remote_config
        .ok_or("child requires remote configuration")?;
    check(
        matches!(input.expected_route.as_str(), "tcp" | "rdma") && remote.live_nvme.is_some(),
        "child requires a live Linux initiator route",
    )?;
    check(
        matches!(input.target_implementation.as_str(), "kernel" | "spdk"),
        "target_implementation must be kernel or spdk",
    )?;
    let mut progress = QualificationProgress {
        directory: ready.parent().ok_or("missing ready parent")?.to_path_buf(),
        file_name: "qualification-child-progress.json",
        events: Vec::new(),
    };
    progress.mark("child_open", None);
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let db = sui::Db::open_remote(&input.coordinator_path, shape, remote).map_err(native_error)?;
    progress.mark("child_open_complete", Some(&db));
    let ks = db.ks("root");
    let start = stats(&db)?;
    check(
        count(&start, "payload_rpc_requests")? == 0
            && count(&start, "errors")? == 0
            && count(&start, "unknown_writes")? == 0,
        "abrupt child opened with payload RPC or live I/O errors",
    )?;
    progress.mark("child_mixed_write", Some(&db));
    let mut batch = db.write_batch();
    batch.delete(ks, key_bytes(&input.namespace, 17, 3));
    batch.write(
        ks,
        key_bytes(&input.namespace, 17, 5),
        value_bytes(&input.namespace, 29, 5, input.value_bytes, 1.0),
    );
    batch.commit().map_err(native_error)?;
    progress.mark("child_mixed_write_complete", Some(&db));
    let end = stats(&db)?;
    check(
        delta(&start, &end, "completed_write_ops")? > 0
            && delta(&start, &end, "completed_write_bytes")? > 0,
        "abrupt child did not complete a live payload write",
    )?;
    check(
        delta(&start, &end, "payload_rpc_requests")? == 0
            && delta(&start, &end, "errors")? == 0
            && delta(&start, &end, "unknown_writes")? == 0,
        "abrupt child used payload RPC or encountered an unknown write",
    )?;
    verify_routes(&end, &input.expected_route, &input.target_implementation)?;
    progress.mark("child_ready_write", Some(&db));
    let evidence = serde_json::json!({
        "schema_version": 1, "kind": "acknowledged_unrooted_mixed_batch",
        "exit_model": "process_exit_without_db_drop_or_checkpoint",
        "start": start, "end": end
    });
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(ready)?;
    file.write_all(&serde_json::to_vec_pretty(&evidence)?)?;
    file.sync_all()?;
    fs::File::open(ready.parent().ok_or("missing ready parent")?)?.sync_all()?;
    progress.mark("child_ready_synced", Some(&db));
    // Deliberately skip Rust destructors: a graceful Db shutdown checkpoints the tail.
    std::process::exit(0)
}

/// Sentinels retained for the graceful and abrupt recovery checks.
struct MemberTransition {
    before_key: Bytes,
    before_value: Bytes,
    after_key: Bytes,
    after_value: Bytes,
    delete_key: Bytes,
    evidence: serde_json::Value,
}

fn qualify_member_transition(
    db: &std::sync::Arc<sui::Db>,
    ks: tidehunter::key_shape::KeySpace,
    remote: &RemoteDatabaseConfig,
    input: &Input,
    early_key: &Bytes,
    early_value: &Bytes,
) -> BackendResult<MemberTransition> {
    check(
        remote.storage.wal_file_bytes > 0
            && remote.storage.wal_file_bytes <= 1024 * 1024 * 1024
            && remote.storage.fragment_bytes >= 128 * 1024,
        "member transition qualification geometry is outside its bounded fixture",
    )?;
    let transition_start = stats(db)?;
    verify_routes(
        &transition_start,
        &input.expected_route,
        &input.target_implementation,
    )?;
    let initial_routes = transition_start["routes"]
        .as_array()
        .ok_or("member transition routes unavailable")?;
    let initial_writer = initial_routes
        .iter()
        .find(|route| !route["writer"].is_null())
        .ok_or("initial writable member evidence unavailable")?;
    let initial_epoch = initial_writer["writer"]["epoch"]
        .as_u64()
        .ok_or("initial writable epoch unavailable")?;
    let initial_file = initial_writer["writer"]["file_index"]
        .as_u64()
        .ok_or("initial writable file index unavailable")?;
    let initial_uuid = &initial_writer["namespace_identity"]["uuid"];
    check(
        initial_epoch == 1 && initial_file == 0,
        "qualification did not start in the first member",
    )?;
    let before_key = key_bytes(&input.namespace, 17, 6);
    let filler_key = key_bytes(&input.namespace, 17, 7);
    let delete_key = key_bytes(&input.namespace, 17, 8);
    let after_key = key_bytes(&input.namespace, 17, 9);
    let before_value = value_bytes(&input.namespace, 29, 6, 65536, 1.0);
    let filler_value = value_bytes(&input.namespace, 29, 7, 65536, 1.0);
    let delete_value = value_bytes(&input.namespace, 29, 8, 65536, 1.0);
    let after_value = value_bytes(&input.namespace, 29, 9, 65536, 1.0);
    check(
        before_value != filler_value
            && before_value != delete_value
            && before_value != after_value
            && filler_value != delete_value
            && filler_value != after_value
            && delete_value != after_value,
        "member sentinel payloads are not distinct",
    )?;
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut writes_acknowledged = 0u64;
    let mut transition_end = transition_start.clone();
    transition_write(
        db,
        &transition_start,
        &mut transition_end,
        deadline,
        &mut writes_acknowledged,
        || {
            db.insert(ks, before_key.clone(), before_value.clone())
                .map_err(native_error)
        },
    )?;
    transition_write(
        db,
        &transition_start,
        &mut transition_end,
        deadline,
        &mut writes_acknowledged,
        || {
            db.insert(ks, delete_key.clone(), delete_value)
                .map_err(native_error)
        },
    )?;
    loop {
        transition_write(
            db,
            &transition_start,
            &mut transition_end,
            deadline,
            &mut writes_acknowledged,
            || {
                db.insert(ks, filler_key.clone(), filler_value.clone())
                    .map_err(native_error)
            },
        )?;
        if !writes_acknowledged.is_multiple_of(32) {
            continue;
        }
        let sample = &transition_end;
        let routes = sample["routes"]
            .as_array()
            .ok_or("member transition routes unavailable")?;
        if delta(&transition_start, sample, "completed_write_bytes")?
            > remote.storage.wal_file_bytes
            && routes.iter().any(|route| {
                route["writer"]["epoch"].as_u64() == Some(initial_epoch)
                    && route["writer"]["file_index"]
                        .as_u64()
                        .is_some_and(|file| file > initial_file)
                    && &route["namespace_identity"]["uuid"] != initial_uuid
            })
        {
            verify_routes(sample, &input.expected_route, &input.target_implementation)?;
            break;
        }
    }
    transition_write(
        db,
        &transition_start,
        &mut transition_end,
        deadline,
        &mut writes_acknowledged,
        || {
            db.insert(ks, after_key.clone(), after_value.clone())
                .map_err(native_error)
        },
    )?;
    transition_write(
        db,
        &transition_start,
        &mut transition_end,
        deadline,
        &mut writes_acknowledged,
        || db.remove(ks, delete_key.clone()).map_err(native_error),
    )?;
    read_expected(db, ks, &delete_key, None)?;
    read_expected_live(db, ks, &before_key, &before_value)?;
    read_expected_live(db, ks, &after_key, &after_value)?;
    read_expected(db, ks, early_key, Some(early_value))?;
    check(
        Instant::now() < deadline,
        "member transition reads exceeded time bound",
    )?;
    let transition_checked = stats(db)?;
    check(
        delta(
            &transition_start,
            &transition_checked,
            "payload_rpc_requests",
        )? == 0
            && delta(&transition_start, &transition_checked, "errors")? == 0
            && delta(&transition_start, &transition_checked, "unknown_writes")? == 0,
        "member transition used payload RPC or encountered an unknown write",
    )?;
    let member_transition = serde_json::json!({
        "wal_file_bytes": remote.storage.wal_file_bytes,
        "completed_write_bytes": delta(&transition_start, &transition_end, "completed_write_bytes")?,
        "completed_write_ops": delta(&transition_start, &transition_end, "completed_write_ops")?,
        "writes_acknowledged": writes_acknowledged,
        "writer_routes": transition_end["routes"].as_array()
            .ok_or("member transition routes unavailable")?
            .iter().filter(|route| route["writer"]["epoch"].as_u64() == Some(initial_epoch))
            .collect::<Vec<_>>(),
        "before_key_read": true,
        "after_key_read": true,
        "early_key_read": true,
        "post_transition_delete": true
    });
    Ok(MemberTransition {
        before_key,
        before_value,
        after_key,
        after_value,
        delete_key,
        evidence: member_transition,
    })
}

fn qualify_abrupt_child(
    input: &Input,
    input_bytes: &[u8],
    output: &Path,
    progress: &mut QualificationProgress,
) -> BackendResult<serde_json::Value> {
    let frozen_input = output.join("abrupt-child-input.json");
    let child_ready = output.join("abrupt-child-ready.json");
    fs::write(&frozen_input, input_bytes)?;
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .arg("--experimental-live-nvmeof-qualification-child")
        .arg(&frozen_input)
        .arg(&child_ready)
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(300);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err("abrupt qualification child exceeded its 300-second deadline".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    check(status.success(), "abrupt qualification child failed")?;
    let child_evidence: serde_json::Value = serde_json::from_slice(&fs::read(&child_ready)?)?;
    progress.mark_value("abrupt_child_complete", child_evidence.get("end").cloned());
    check(
        child_evidence["kind"] == "acknowledged_unrooted_mixed_batch"
            && child_evidence["exit_model"] == "process_exit_without_db_drop_or_checkpoint",
        "invalid abrupt child evidence",
    )?;
    check(
        delta(
            &child_evidence["start"],
            &child_evidence["end"],
            "completed_write_ops",
        )? > 0
            && delta(
                &child_evidence["start"],
                &child_evidence["end"],
                "completed_write_bytes",
            )? > 0
            && delta(
                &child_evidence["start"],
                &child_evidence["end"],
                "payload_rpc_requests",
            )? == 0,
        "abrupt child evidence did not prove live payload write",
    )?;
    verify_routes(
        &child_evidence["end"],
        &input.expected_route,
        &input.target_implementation,
    )?;
    Ok(child_evidence)
}

pub(super) fn run(input_path: &Path, output: &Path) -> BackendResult<()> {
    let (input, input_bytes) = read_input(input_path)?;
    validate_tier(&input)?;
    check(
        input.coordinator_path.is_absolute() && !input.coordinator_path.exists(),
        "coordinator_path must be an absolute fresh path",
    )?;
    check(
        input.namespace != [0; 16] && (1..=65536).contains(&input.value_bytes),
        "invalid bounded qualification data",
    )?;
    check(
        matches!(input.target_implementation.as_str(), "kernel" | "spdk"),
        "target_implementation must be kernel or spdk",
    )?;
    if input.expected_route == "local_file" {
        check(
            input.remote_config.is_none(),
            "local file qualification must not configure a remote backend",
        )?;
        check(
            input.target_implementation == "kernel",
            "local file qualification cannot qualify an SPDK target",
        )?;
        fs::create_dir(output)?;
        return run_local_file(&input, output);
    }
    check(
        matches!(input.expected_route.as_str(), "tcp" | "rdma"),
        "live Linux initiator qualification requires tcp or rdma",
    )?;
    let remote = input
        .remote_config
        .as_ref()
        .ok_or("remote_config required for live qualification")?;
    let live = remote
        .live_nvme
        .as_ref()
        .ok_or("live_nvme configuration required")?;
    check(
        (1..=60_000).contains(&live.connect_timeout_ms)
            && (1..=60_000).contains(&live.io_timeout_ms)
            && !remote.timeout.is_zero()
            && remote.timeout <= Duration::from_secs(60),
        "qualification timeouts must be positive and at most 60 seconds",
    )?;
    check(
        remote.read_cache_bytes > 0,
        "qualification requires enabled coordinator cache",
    )?;
    check(
        remote.endpoints.len() == 1
            && remote.storage.partitions.len() == 1
            && remote.storage.max_epochs >= 4
            && remote.replay_limits.max_batches >= 8
            && remote.replay_limits.max_encoded_bytes >= 4 * (input.value_bytes + 4096),
        "two-host qualification requires one owner, four epochs and a replay window above the child batch",
    )?;
    let partitions = &remote.storage.partitions;
    fs::create_dir(output)?;
    let mut progress = QualificationProgress {
        directory: output.to_path_buf(),
        file_name: "qualification-progress.json",
        events: Vec::new(),
    };
    progress.mark("initial_open", None);
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    // Writes may warm the coordinator cache. Disable it for the initial
    // boundary sentinels; the original configuration tests cache hits on reopen.
    let mut initial_remote = remote.clone();
    initial_remote.read_cache_bytes = 0;
    let db = sui::Db::open_remote(&input.coordinator_path, shape.clone(), initial_remote)
        .map_err(native_error)?;
    progress.mark("initial_open_complete", Some(&db));
    let ks = db.ks("root");
    let keys: Vec<_> = (0..6).map(|i| key_bytes(&input.namespace, 17, i)).collect();
    let values: Vec<_> = (0..6)
        .map(|i| value_bytes(&input.namespace, 29, i, input.value_bytes, 1.0))
        .collect();
    let start = stats(&db)?;
    check(
        count(&start, "payload_rpc_requests")? == 0
            && count(&start, "errors")? == 0
            && count(&start, "unknown_writes")? == 0,
        "initial open used payload RPC or encountered live I/O errors",
    )?;
    progress.mark("scalar_write_read", Some(&db));
    db.insert(ks, keys[0].clone(), values[0].clone())
        .map_err(native_error)?;
    read_expected(&db, ks, &keys[0], Some(&values[0]))?;
    db.insert(ks, keys[1].clone(), values[1].clone())
        .map_err(native_error)?;
    db.remove(ks, keys[1].clone()).map_err(native_error)?;
    read_expected(&db, ks, &keys[1], None)?;
    progress.mark("scalar_write_read_complete", Some(&db));
    progress.mark("batch_write", Some(&db));
    let mut batch = db.write_batch();
    batch.write(ks, keys[1].clone(), values[1].clone());
    batch.write(ks, keys[2].clone(), values[2].clone());
    batch.write(ks, keys[3].clone(), values[3].clone());
    batch.commit().map_err(native_error)?;
    progress.mark("batch_write_complete", Some(&db));
    progress.mark("mixed_write", Some(&db));
    let checkpoint = db.checkpoint();
    let mut mixed = db.write_batch();
    mixed.delete(ks, keys[1].clone());
    mixed.write(ks, keys[2].clone(), values[4].clone());
    mixed.write(ks, keys[4].clone(), values[4].clone());
    mixed.commit().map_err(native_error)?;
    progress.mark("mixed_write_complete", Some(&db));
    progress.mark("ordered_reads_and_iteration", Some(&db));
    let expected = [
        Some(&values[4]),
        None,
        Some(&values[0]),
        None,
        Some(&values[3]),
        Some(&values[4]),
        Some(&values[4]),
        None,
    ];
    let order = [2, 1, 0, 5, 3, 4, 2, 5];
    let query: Vec<_> = order.iter().map(|&i| keys[i].clone()).collect();
    let observed = db.multi_get(ks, &query);
    check(
        observed.len() == expected.len(),
        "ordered multi_get length mismatch",
    )?;
    for (actual, expected) in observed.into_iter().zip(expected) {
        check(
            actual.map_err(native_error)?.as_ref() == expected,
            "ordered multi_get mismatch",
        )?;
    }
    check(
        db.exists(ks, &keys[2]).map_err(native_error)?
            && !db.exists(ks, &keys[1]).map_err(native_error)?,
        "exists mismatch",
    )?;
    let old: BTreeMap<_, _> = checkpoint
        .iterator(ks)
        .map(|row| row.map_err(native_error))
        .collect::<Result<_, _>>()?;
    check(
        old.get(&keys[1]) == Some(&values[1]) && old.get(&keys[2]) == Some(&values[2]),
        "checkpoint did not retain pre-batch values",
    )?;
    let current: BTreeMap<_, _> = db
        .iterator(ks)
        .map(|row| row.map_err(native_error))
        .collect::<Result<_, _>>()?;
    check(
        current.len() == 4
            && current.get(&keys[2]) == Some(&values[4])
            && !current.contains_key(&keys[1]),
        "iteration mismatch",
    )?;
    progress.mark("ordered_reads_and_iteration_complete", Some(&db));
    // Keep this in one authority epoch: a second verified writer namespace
    // then demonstrates a file transition, not an epoch-recovery export.
    progress.mark("member_transition", Some(&db));
    let MemberTransition {
        before_key,
        before_value,
        after_key,
        after_value,
        delete_key,
        evidence: member_transition,
    } = qualify_member_transition(&db, ks, remote, &input, &keys[0], &values[0])?;
    progress.mark("member_transition_complete", Some(&db));
    progress.mark("flush", Some(&db));
    db.flush().map_err(native_error)?;
    let before_close = stats(&db)?;
    check(
        delta(&start, &before_close, "completed_write_ops")? > 0
            && delta(&start, &before_close, "completed_write_bytes")? > 0,
        "no completed live payload writes",
    )?;
    check(
        delta(&start, &before_close, "payload_rpc_requests")? == 0,
        "payload RPC used in live qualification",
    )?;
    check(
        delta(&start, &before_close, "errors")? == 0
            && delta(&start, &before_close, "unknown_writes")? == 0,
        "live I/O errors or unknown writes",
    )?;
    verify_routes(
        &before_close,
        &input.expected_route,
        &input.target_implementation,
    )?;
    progress.mark("before_graceful_close", Some(&db));
    db.wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    progress.mark("graceful_reopen", None);
    let reopened = sui::Db::open_remote(&input.coordinator_path, shape.clone(), remote.clone())
        .map_err(native_error)?;
    let ks = reopened.ks("root");
    let recovery = stats(&reopened)?;
    progress.mark("graceful_reopen_complete", Some(&reopened));
    verify_fences(&recovery, partitions, &input.target_implementation)?;
    check(
        count(&recovery, "payload_rpc_requests")? == 0 && count(&recovery, "errors")? == 0,
        "recovery used RPC payload or failed",
    )?;
    progress.mark("cache_miss_read", Some(&reopened));
    let cache_before = reopened
        .remote_counters()
        .ok_or("coordinator counters unavailable")?
        .0;
    read_expected(&reopened, ks, &keys[2], Some(&values[4]))?;
    let after_miss = stats(&reopened)?;
    let cache_miss = reopened
        .remote_counters()
        .ok_or("coordinator counters unavailable")?
        .0;
    check(
        cache_miss.misses > cache_before.misses
            && delta(&recovery, &after_miss, "completed_read_ops")? > 0
            && delta(&recovery, &after_miss, "completed_read_bytes")? > 0,
        "cache miss did not complete live namespace read",
    )?;
    progress.mark("cache_hit_read", Some(&reopened));
    read_expected(&reopened, ks, &keys[2], Some(&values[4]))?;
    let cache_hit = reopened
        .remote_counters()
        .ok_or("coordinator counters unavailable")?
        .0;
    let after_hit = stats(&reopened)?;
    check(
        cache_hit.hits > cache_miss.hits
            && delta(&after_miss, &after_hit, "completed_read_ops")? == 0,
        "repeat read did not use coordinator cache",
    )?;
    read_expected_live(&reopened, ks, &before_key, &before_value)?;
    read_expected_live(&reopened, ks, &after_key, &after_value)?;
    read_expected(&reopened, ks, &delete_key, None)?;
    read_expected(&reopened, ks, &keys[0], Some(&values[0]))?;
    read_expected(&reopened, ks, &keys[1], None)?;
    let after_reopen_reads = stats(&reopened)?;
    verify_routes(
        &after_reopen_reads,
        &input.expected_route,
        &input.target_implementation,
    )?;
    check(
        delta(&recovery, &after_reopen_reads, "payload_rpc_requests")? == 0
            && delta(&recovery, &after_reopen_reads, "errors")? == 0,
        "reopened live path used RPC payload or failed",
    )?;
    progress.mark("cache_checks_complete", Some(&reopened));
    progress.mark("before_second_graceful_close", Some(&reopened));
    reopened
        .wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    progress.mark("abrupt_child_start", None);
    let child_evidence = qualify_abrupt_child(&input, &input_bytes, output, &mut progress)?;
    progress.mark("abrupt_recovery_open", None);
    let crash_reopened =
        sui::Db::open_remote(&input.coordinator_path, shape.clone(), remote.clone())
            .map_err(native_error)?;
    let crash_recovery = stats(&crash_reopened)?;
    progress.mark("abrupt_recovery_open_complete", Some(&crash_reopened));
    verify_fences(&crash_recovery, partitions, &input.target_implementation)?;
    check(
        count(&crash_recovery, "recovery_read_ops")? > 0
            && count(&crash_recovery, "recovery_read_bytes")? > 0
            && count(&crash_recovery, "payload_rpc_requests")? == 0
            && count(&crash_recovery, "errors")? == 0,
        "abrupt reopen did not prove live payload recovery",
    )?;
    verify_routes(
        &crash_recovery,
        &input.expected_route,
        &input.target_implementation,
    )?;
    let crash_ks = crash_reopened.ks("root");
    read_expected(&crash_reopened, crash_ks, &keys[3], None)?;
    read_expected(&crash_reopened, crash_ks, &keys[5], Some(&values[5]))?;
    read_expected(&crash_reopened, crash_ks, &keys[2], Some(&values[4]))?;
    read_expected_live(&crash_reopened, crash_ks, &before_key, &before_value)?;
    read_expected_live(&crash_reopened, crash_ks, &after_key, &after_value)?;
    read_expected(&crash_reopened, crash_ks, &delete_key, None)?;
    read_expected(&crash_reopened, crash_ks, &keys[0], Some(&values[0]))?;
    let after_crash_reads = stats(&crash_reopened)?;
    verify_routes(
        &after_crash_reads,
        &input.expected_route,
        &input.target_implementation,
    )?;
    check(
        count(&after_crash_reads, "payload_rpc_requests")? == 0
            && count(&after_crash_reads, "errors")? == 0,
        "crash-reopened reads used RPC payload or failed",
    )?;
    progress.mark("abrupt_recovery_checks_complete", Some(&crash_reopened));
    progress.mark("before_final_close", Some(&crash_reopened));
    crash_reopened
        .wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    progress.mark("target_shutdown", None);
    let target_shutdown = shutdown_targets(remote, &shape, &input.target_implementation)?;
    progress.mark("target_shutdown_complete", None);
    let scope = if input.target_implementation == "spdk" {
        "verified_spdk_namespace_not_physical_media_performance"
    } else {
        "verified_kernel_namespace_not_physical_media_performance"
    };
    let proof = serde_json::json!({"schema_version": 1, "kind": "live_nvmeof_qualification", "expected_route": input.expected_route, "target_implementation": input.target_implementation, "durability_tier": input.durability_tier, "scope": scope, "start": start, "before_close": before_close, "member_transition": member_transition, "recovery": recovery, "after_miss": after_miss, "after_hit": after_hit, "after_reopen_reads": after_reopen_reads, "abrupt_child": child_evidence, "abrupt_recovery": crash_recovery, "after_crash_reads": after_crash_reads, "target_shutdown": target_shutdown, "checks": {"scalar_write_delete":true,"mixed_batch":true,"ordered_batch_missing_exists":true,"checkpoint_iteration":true,"flush_reopen":true,"abrupt_tail_recovery":true,"fence_drain":true,"cache_miss_hit":true,"payload_rpc_fallback":false}});
    fs::write(
        output.join("qualification.json"),
        serde_json::to_vec_pretty(&proof)?,
    )?;
    progress.mark("qualification_proof_written", None);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_file_qualification_persists_and_recovers_real_frames() {
        let root = std::env::temp_dir().join(format!(
            "live-file-qualification-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        fs::write(
            &input,
            serde_json::to_vec(&serde_json::json!({
                "coordinator_path": root.join("stream"),
                "namespace": [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
                "expected_route": "local_file",
                "durability_tier": "synchronous",
                "value_bytes": 256
            }))
            .unwrap(),
        )
        .unwrap();
        let output = root.join("result");
        run(&input, &output).unwrap();
        let proof: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join("qualification.json")).unwrap()).unwrap();
        assert_eq!(proof["kind"], "live_nvmeof_file_component_qualification");
        assert_eq!(proof["frame_count"], 3);
        assert_eq!(proof["owner_validate_persist_retry_recovery"], true);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn qualification_requires_explicit_valid_tier() {
        let base = serde_json::json!({
            "coordinator_path": "/unused",
            "namespace": vec![1; 16],
            "expected_route": "local_file",
            "value_bytes": 256
        });
        assert!(serde_json::from_value::<Input>(base.clone()).is_err());
        for tier in ["native", "synchronous", "invalid"] {
            let mut value = base.clone();
            value["durability_tier"] = serde_json::json!(tier);
            let input: Input = serde_json::from_value(value).unwrap();
            assert_eq!(validate_tier(&input).is_ok(), tier != "invalid");
        }
    }

    #[test]
    fn qualification_rejects_fixture_fences_and_unverified_routes() {
        let mut sample = serde_json::json!({
            "route_evidence_dropped": 0, "fence_evidence_dropped": 0,
            "routes": [{
                "transport":"rdma", "address":"10.0.0.2", "service":4420,
                "target_implementation":"kernel",
                "namespace_identity":{"namespace_path":"/dev/nvme0n1", "target_implementation":"kernel",
                    "nqn":"nqn.2026-01.test:wal", "nsid":1, "bytes":4096,
                    "controller_instance":0, "block_size":4096, "uuid":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}
            }],
            "fences":[{"partition":0,"authority_epoch":2,
                "target":{"fixture_only":false,"disable_drain_successes":1,"errors":0,"target_implementation":"kernel"}}]
        });
        assert!(verify_routes(&sample, "rdma", "kernel").is_ok());
        assert!(verify_routes(&sample, "tcp", "kernel").is_err());
        assert!(verify_routes(&sample, "rdma", "spdk").is_err());
        assert!(verify_fences(&sample, &[0], "kernel").is_ok());
        assert!(verify_fences(&sample, &[0], "spdk").is_err());
        sample["routes"][0]["target_implementation"] = serde_json::json!("spdk");
        assert!(verify_routes(&sample, "rdma", "spdk").is_err());
        sample["routes"][0]["namespace_identity"]["target_implementation"] =
            serde_json::json!("spdk");
        assert!(verify_routes(&sample, "rdma", "spdk").is_err());
        sample["routes"][0]["spdk"] = serde_json::json!({
            "rpc_socket": "/run/spdk.sock", "pid": 42, "process_start_ticks": 10,
            "bdev_name": "wal", "image_path": "/wal/member"
        });
        sample["routes"][0]["writer"] = serde_json::json!({
            "epoch": 1, "file_index": 0,
            "file_identity": {"path": "/wal/member", "device": 1, "inode": 2, "bytes": 4096, "block_size": 4096}
        });
        assert!(verify_routes(&sample, "rdma", "spdk").is_ok());
        sample["fences"][0]["target"]["target_implementation"] = serde_json::json!("spdk");
        assert!(verify_fences(&sample, &[0], "spdk").is_ok());
        sample["fences"][0]["target"]["fixture_only"] = serde_json::json!(true);
        assert!(verify_fences(&sample, &[0], "spdk").is_err());
        sample["fences"][0]["target"]["fixture_only"] = serde_json::json!(false);
        sample["fences"][0]["target"]["disable_drain_successes"] = serde_json::json!(0);
        assert!(verify_fences(&sample, &[0], "spdk").is_err());
        sample["route_evidence_dropped"] = serde_json::json!(1);
        assert!(verify_routes(&sample, "rdma", "spdk").is_err());
    }
}
