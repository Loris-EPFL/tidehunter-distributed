//! Bounded end-to-end qualification against already running owners.
use super::*;
use crate::workload::{key_bytes, value_bytes};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::time::Instant;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    #[serde(default)]
    remote_config: Option<RemoteDatabaseConfig>,
    coordinator_path: PathBuf,
    namespace: [u8; 16],
    expected_route: String,
    durability_tier: String,
    value_bytes: usize,
}
// This is diagnostic state, never a successful qualification receipt. Updating
// it is best-effort so an evidence I/O error cannot replace the actual failure.
struct QualificationProgress {
    directory: PathBuf,
    file_name: &'static str,
    started: Instant,
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
        self.record(phase, snapshot, serde_json::Value::Null);
    }

    /// An event inside a phase; `detail` locates it, for example a write count.
    fn mark_detail(&mut self, phase: &'static str, db: &sui::Db, detail: serde_json::Value) {
        let snapshot = db
            .live_nvmeof_stats()
            .and_then(|stats| serde_json::to_value(stats).ok());
        self.record(phase, snapshot, detail);
    }

    fn record(
        &mut self,
        phase: &'static str,
        snapshot: Option<serde_json::Value>,
        detail: serde_json::Value,
    ) {
        trace_phase(phase);
        self.events.push(serde_json::json!({
            "phase": phase,
            "elapsed_ms": u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "detail": detail,
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

/// Put a client phase on the kernel clock when the CI job armed the nvmet
/// trace. Best-effort and absent on local, non-root runs.
fn trace_phase(phase: &str) {
    if let Ok(mut marker) = std::fs::OpenOptions::new()
        .append(true)
        .open("/sys/kernel/tracing/trace_marker")
    {
        let _ = writeln!(marker, "{phase}");
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

/// The controller instance and pinned sysfs inode that served each subsystem's
/// observed namespace route, keyed by subsystem NQN.
fn controller_identities(
    sample: &serde_json::Value,
) -> BackendResult<BTreeMap<String, (u64, u64)>> {
    let routes = sample
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .ok_or("observed live namespace routes unavailable")?;
    let mut identities = BTreeMap::new();
    for route in routes {
        let identity = route
            .get("namespace_identity")
            .and_then(serde_json::Value::as_object)
            .ok_or("observed live namespace identity unavailable")?;
        let nqn = identity
            .get("nqn")
            .and_then(serde_json::Value::as_str)
            .ok_or("observed live namespace NQN unavailable")?
            .to_owned();
        let instance = identity
            .get("controller_instance")
            .and_then(serde_json::Value::as_u64)
            .ok_or("observed live controller instance unavailable")?;
        let inode = identity
            .get("controller")
            .and_then(serde_json::Value::as_object)
            .and_then(|controller| controller.get("sysfs_inode"))
            .and_then(serde_json::Value::as_u64)
            .ok_or("observed live controller inode unavailable")?;
        identities.insert(nqn, (instance, inode));
    }
    Ok(identities)
}

/// Controller identity evidence for the abrupt-exit adoption window. The check
/// scopes kernel-log churn to the child's own subsystems and initiator, so an
/// owner commissioning controller for the same subsystem can neither mask nor
/// fake a host controller recreate.
struct AdoptionWindow {
    nqns: BTreeSet<String>,
    host_nqns: BTreeSet<String>,
    /// `addr <ip>:<port>` as the host connect line prints it.
    services: BTreeSet<String>,
    /// Host controller names, e.g. `nvme3`.
    instances: BTreeSet<String>,
}

impl AdoptionWindow {
    fn from_child(sample: &serde_json::Value) -> BackendResult<Self> {
        let routes = sample["routes"]
            .as_array()
            .ok_or("abrupt child routes unavailable")?;
        check(!routes.is_empty(), "abrupt child observed no controllers")?;
        let mut window = Self {
            nqns: BTreeSet::new(),
            host_nqns: BTreeSet::new(),
            services: BTreeSet::new(),
            instances: BTreeSet::new(),
        };
        for route in routes {
            let identity = route["namespace_identity"]
                .as_object()
                .ok_or("abrupt child namespace identity unavailable")?;
            let nqn = identity["nqn"]
                .as_str()
                .ok_or("abrupt child subsystem NQN unavailable")?;
            let instance = identity["controller_instance"]
                .as_u64()
                .ok_or("abrupt child controller instance unavailable")?;
            let address = route["address"]
                .as_str()
                .ok_or("abrupt child controller address unavailable")?;
            let service = route["service"]
                .as_u64()
                .ok_or("abrupt child controller service unavailable")?;
            window.nqns.insert(nqn.to_owned());
            window.instances.insert(format!("nvme{instance}"));
            window.services.insert(format!("addr {address}:{service}"));
            let host_path = format!("/sys/class/nvme/nvme{instance}/hostnqn");
            match std::fs::read_to_string(&host_path) {
                Ok(host_nqn) => {
                    window.host_nqns.insert(host_nqn.trim().to_owned());
                }
                // A route whose controller was already retired is not an
                // adoption candidate; the secondary identity check covers it.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("read {host_path}: {error}").into()),
            }
        }
        Ok(window)
    }

    /// A host or target line that creates or removes one of the child's own
    /// public controllers. Commissioning controllers use a different host NQN,
    /// service and host instance, so they are not counted.
    fn is_churn(&self, line: &str) -> bool {
        if !self.nqns.iter().any(|nqn| line.contains(nqn)) {
            return false;
        }
        if line.contains("Created nvm controller") {
            return self.host_nqns.iter().any(|host| line.contains(host));
        }
        if line.contains("new ctrl:") {
            return self.services.iter().any(|service| line.contains(service));
        }
        if line.contains("Removing ctrl") || line.contains("Removing controller") {
            return self
                .instances
                .iter()
                .any(|instance| line.contains(&format!("nvme {instance}:")));
        }
        false
    }
}

/// The kernel log, from `dmesg` or `journalctl -k`. The CI kernel job runs as
/// root; a non-root local live run reports the missing evidence instead of
/// silently passing.
fn kernel_log() -> Option<String> {
    for (program, args) in [
        ("dmesg", Vec::new()),
        ("journalctl", vec!["-k", "--no-pager", "-o", "cat"]),
    ] {
        if let Ok(output) = std::process::Command::new(program).args(args).output()
            && output.status.success()
        {
            return Some(String::from_utf8_lossy(&output.stdout).into_owned());
        }
    }
    None
}

fn controller_churn(window: &AdoptionWindow) -> BackendResult<usize> {
    let log = kernel_log().ok_or(
        "kernel log unavailable for the abrupt-adoption proof; run the live qualification as root",
    )?;
    Ok(log.lines().filter(|line| window.is_churn(line)).count())
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

fn verify_routes(sample: &serde_json::Value, expected: &str) -> BackendResult<()> {
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
                    .get("controller")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|controller| {
                        ["queue_count", "sqsize", "sysfs_device", "sysfs_inode"]
                            .iter()
                            .all(|key| {
                                controller
                                    .get(*key)
                                    .and_then(serde_json::Value::as_u64)
                                    .is_some_and(|n| n > 0)
                            })
                    })
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
fn verify_fences(sample: &serde_json::Value, partitions: &[u32]) -> BackendResult<()> {
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
                    && f["target"]["fence_count"].as_u64().is_some_and(|n| n > 0)
                    && f["target"]["write_protect_successes"]
                        .as_u64()
                        .is_some_and(|n| n > 0)
                    && f["target"]["errors"].as_u64() == Some(0)
            }),
            "missing observed target namespace fence/write-protect",
        )?;
    }
    Ok(())
}
fn shutdown_targets(
    config: &RemoteDatabaseConfig,
    database: [u8; 16],
    shape: &KeyShape,
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
                database,
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
        let epoch = status.highest_fenced_generation;
        check(epoch != 0, "owner has no authority epoch")?;
        if status.active_epoch == Some(epoch) {
            client.seal(epoch)?;
        } else {
            check(
                status.active_epoch.is_none(),
                "owner authority not in expected latest epoch",
            )?;
        }
        let stats = client.shutdown_live_target(epoch)?;
        check(
            !stats.fixture_only && stats.owned_namespace_count == 0 && stats.errors == 0,
            "owner target shutdown did not prove matching namespace cleanup",
        )?;
        receipts.push(serde_json::json!({
            "partition": endpoint.partition, "endpoint": endpoint.address,
            "database": database, "epoch": epoch, "stats": stats
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
        nvmeof::live_storage::LiveGroupDescriptor,
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
        let (_, frame) = LiveGroupDescriptor::from_group(&group)?;
        let (extent, _) = log.lease_live_extent(authority)?;
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&extent.segment.path)?;
        file.write_all_at(&frame, extent.start % extent.segment.bytes)?;
        let through = extent.start + frame.len() as u64;
        // Close the extent behind a skip marker unless the frame fills it.
        if through < extent.end {
            let mut skip = [0; 4096];
            skip[..8].copy_from_slice(tidehunter::crc::CrcFrame::skip_marker().as_ref());
            file.write_all_at(&skip, through % extent.segment.bytes)?;
        }
        let (adoption, _) = log.adopt_live_extent(authority, extent.token, through, true)?;
        check(
            adoption.closed && adoption.receipt_count == 1,
            "local leased frame adoption mismatch",
        )?;
        let appended = log.receipt_page(log.receipt_count() - 1, 1)?;
        log.persist_many(&appended)?;
        check(
            log.adopt_live_extent(authority, extent.token, through, true)?
                .0
                == adoption,
            "local adoption retry mismatch",
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
        "scope": "local positional file protocol on the leased route; not kernel NVMe-oF, RDMA, or hardware performance",
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
        matches!(input.expected_route.as_str(), "tcp" | "rdma"),
        "child requires a live Linux initiator route",
    )?;
    let mut progress = QualificationProgress {
        directory: ready.parent().ok_or("missing ready parent")?.to_path_buf(),
        file_name: "qualification-child-progress.json",
        started: Instant::now(),
        events: Vec::new(),
    };
    progress.mark("child_open", None);
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let db = sui::Db::open_remote(&input.coordinator_path, shape, remote).map_err(native_error)?;
    progress.mark("child_open_complete", Some(&db));
    let ks = db.ks("root");
    let start = stats(&db)?;
    check(
        count(&start, "errors")? == 0 && count(&start, "unknown_writes")? == 0,
        "abrupt child opened with live I/O errors or unknown writes",
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
        delta(&start, &end, "errors")? == 0 && delta(&start, &end, "unknown_writes")? == 0,
        "abrupt child encountered live I/O errors or unknown writes",
    )?;
    verify_routes(&end, &input.expected_route)?;
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

/// The initial database's writer routes, one per partition, as observed.
fn writer_routes(sample: &serde_json::Value) -> BackendResult<Vec<serde_json::Value>> {
    Ok(sample["routes"]
        .as_array()
        .ok_or("live namespace routes unavailable")?
        .iter()
        .filter(|route| !route["writer"].is_null())
        .cloned()
        .collect())
}

/// Effective initiator identity of `database` on every partition, as the
/// owner host derives it (`tcp::host::identity`).
fn effective_initiators(
    remote: &RemoteDatabaseConfig,
    database: [u8; 16],
) -> BackendResult<Vec<serde_json::Value>> {
    use tidehunter::distributed::tcp::host::identity;
    let live = &remote.live_nvme;
    remote
        .storage
        .partitions
        .iter()
        .map(|&partition| {
            let configured = live
                .owner_identities
                .get(&partition)
                .map_or(live.host_id, |owner| owner.host_id);
            Ok(serde_json::json!({
                "partition": partition,
                "database": identity::database_hex(&database),
                "host_nqn": identity::effective_host_nqn(&database, partition),
                "host_id": identity::effective_host_id(&configured, &database, partition),
            }))
        })
        .collect()
}

/// Drop the qualified database through the owner host and reopen the same
/// path: the host reuses the freed target slot (same port and service) for a
/// new identity whose subsystem and effective initiator NQNs differ, and new
/// live I/O completes on it. With one host slot (`max_databases = 1`) the
/// reprovision succeeds only after the drop has released the slot.
fn qualify_target_reuse(
    input: &Input,
    remote: &RemoteDatabaseConfig,
    shape: &KeyShape,
    dropped: [u8; 16],
    dropped_sample: &serde_json::Value,
    progress: &mut QualificationProgress,
) -> BackendResult<(serde_json::Value, [u8; 16])> {
    use tidehunter::distributed::tcp::host::identity;
    let before = writer_routes(dropped_sample)?;
    check(!before.is_empty(), "no writer route before the drop")?;
    progress.mark("target_reuse_drop", None);
    sui::Db::drop_db(&input.coordinator_path)?;
    check(
        !input.coordinator_path.exists(),
        "dropped coordinator directory remains",
    )?;
    progress.mark("target_reuse_reprovision", None);
    // Fresh reads must reach the reused namespace, not the coordinator cache.
    let mut fresh_remote = remote.clone();
    fresh_remote.read_cache_bytes = 0;
    let db = sui::Db::open_remote(&input.coordinator_path, shape.clone(), fresh_remote)
        .map_err(native_error)?;
    let database = db
        .remote_database_id()
        .ok_or("reprovisioned database is not remote")?;
    check(
        database != dropped,
        "reprovision reused the dropped identity",
    )?;
    progress.mark("target_reuse_io", Some(&db));
    let ks = db.ks("root");
    let start = stats(&db)?;
    let key = key_bytes(&input.namespace, 23, 1);
    let value = value_bytes(&input.namespace, 31, 1, 65536, 1.0);
    db.insert(ks, key.clone(), value.clone())
        .map_err(native_error)?;
    read_expected_live(&db, ks, &key, &value)?;
    let end = stats(&db)?;
    check(
        delta(&start, &end, "completed_write_ops")? > 0
            && delta(&start, &end, "errors")? == 0
            && delta(&start, &end, "unknown_writes")? == 0,
        "reused target did not complete live I/O",
    )?;
    verify_routes(&end, &input.expected_route)?;
    let after = writer_routes(&end)?;
    check(!after.is_empty(), "no writer route after the reprovision")?;
    // One owner (checked by `run`): every route of either database names the
    // same target slot. The subsystem and the effective initiator are scoped
    // to each database, so they differ.
    let partition = *remote
        .storage
        .partitions
        .first()
        .ok_or("qualification has no partition")?;
    let scope = |database: &[u8; 16]| format!("db-{}-p", identity::database_hex(database));
    let scoped = |routes: &[serde_json::Value], database: &[u8; 16]| {
        routes.iter().all(|route| {
            route["namespace_identity"]["nqn"]
                .as_str()
                .is_some_and(|nqn| nqn.contains(&scope(database)))
                && route["host_nqn"].as_str()
                    == Some(identity::effective_host_nqn(database, partition).as_str())
        })
    };
    for (old, new) in before
        .iter()
        .flat_map(|old| after.iter().map(move |new| (old, new)))
    {
        check(
            new["address"] == old["address"]
                && new["service"] == old["service"]
                && new["transport"] == old["transport"],
            "reprovisioned database did not reuse the dropped target slot",
        )?;
    }
    check(
        scoped(&before, &dropped) && scoped(&after, &database),
        "target subsystem or initiator is not scoped to its database",
    )?;
    progress.mark("target_reuse_close", Some(&db));
    db.wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    Ok((
        serde_json::json!({
            "dropped_database": identity::database_hex(&dropped),
            "database": identity::database_hex(&database),
            "dropped_initiators": effective_initiators(remote, dropped)?,
            "initiators": effective_initiators(remote, database)?,
            "routes_before_drop": before,
            "routes_after_reprovision": after,
            "start": start,
            "end": end,
        }),
        database,
    ))
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
    progress: &mut QualificationProgress,
) -> BackendResult<MemberTransition> {
    // Bounded in-phase evidence: a stall stays within eight writes of an event.
    const MILESTONE_WRITES: u64 = 8;
    const MAX_MILESTONES: u64 = 64;
    check(
        remote.storage.wal_file_bytes > 0
            && remote.storage.wal_file_bytes <= 1024 * 1024 * 1024
            && remote.storage.fragment_bytes >= 128 * 1024,
        "member transition qualification geometry is outside its bounded fixture",
    )?;
    let transition_start = stats(db)?;
    verify_routes(&transition_start, &input.expected_route)?;
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
        if writes_acknowledged.is_multiple_of(MILESTONE_WRITES)
            && writes_acknowledged / MILESTONE_WRITES <= MAX_MILESTONES
        {
            progress.mark_detail(
                "member_transition_writes",
                db,
                serde_json::json!({ "writes_acknowledged": writes_acknowledged }),
            );
        }
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
            verify_routes(sample, &input.expected_route)?;
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
    progress.mark_detail(
        "member_transition_reads",
        db,
        serde_json::json!({ "writes_acknowledged": writes_acknowledged }),
    );
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
        delta(&transition_start, &transition_checked, "errors")? == 0
            && delta(&transition_start, &transition_checked, "unknown_writes")? == 0,
        "member transition encountered live I/O errors or unknown writes",
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
            )? > 0,
        "abrupt child evidence did not prove live payload write",
    )?;
    verify_routes(&child_evidence["end"], &input.expected_route)?;
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
    if input.expected_route == "local_file" {
        check(
            input.remote_config.is_none(),
            "local file qualification must not configure a remote backend",
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
    let live = &remote.live_nvme;
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
        remote.endpoints.len() == 1 && remote.storage.partitions.len() == 1,
        "two-host qualification requires one owner",
    )?;
    let partitions = &remote.storage.partitions;
    fs::create_dir(output)?;
    let mut progress = QualificationProgress {
        directory: output.to_path_buf(),
        file_name: "qualification-progress.json",
        started: Instant::now(),
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
        count(&start, "errors")? == 0 && count(&start, "unknown_writes")? == 0,
        "initial open encountered live I/O errors or unknown writes",
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
    } = qualify_member_transition(&db, ks, remote, &input, &keys[0], &values[0], &mut progress)
        .inspect_err(|error| {
            // Record before the database drop, which can block after a failed write.
            eprintln!("member transition failed: {error}");
            progress.mark_detail(
                "member_transition_failed",
                &db,
                serde_json::json!({ "error": error.to_string() }),
            );
        })?;
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
        delta(&start, &before_close, "errors")? == 0
            && delta(&start, &before_close, "unknown_writes")? == 0,
        "live I/O errors or unknown writes",
    )?;
    verify_routes(&before_close, &input.expected_route)?;
    progress.mark("before_graceful_close", Some(&db));
    db.wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    progress.mark("graceful_reopen", None);
    let reopened = sui::Db::open_remote(&input.coordinator_path, shape.clone(), remote.clone())
        .map_err(native_error)?;
    let ks = reopened.ks("root");
    let recovery = stats(&reopened)?;
    progress.mark("graceful_reopen_complete", Some(&reopened));
    verify_fences(&recovery, partitions)?;
    check(count(&recovery, "errors")? == 0, "recovery failed")?;
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
    verify_routes(&after_reopen_reads, &input.expected_route)?;
    check(
        delta(&recovery, &after_reopen_reads, "errors")? == 0,
        "reopened live reads failed",
    )?;
    progress.mark("cache_checks_complete", Some(&reopened));
    progress.mark("before_second_graceful_close", Some(&reopened));
    reopened
        .wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    progress.mark("abrupt_child_start", None);
    let child_evidence = qualify_abrupt_child(&input, &input_bytes, output, &mut progress)?;
    let child_controllers = controller_identities(&child_evidence["end"])?;
    check(
        !child_controllers.is_empty(),
        "abrupt child observed no controllers",
    )?;
    // The kernel log window between the child exit and the first replay read
    // must not create or remove a controller of the child's own initiator. This
    // is the primary, non-recyclable evidence; the instance/inode comparison
    // below is secondary because both allocators can reuse their values.
    let window = AdoptionWindow::from_child(&child_evidence["end"])?;
    let churn_after_child = controller_churn(&window)?;
    progress.mark("abrupt_recovery_open", None);
    let crash_reopened =
        sui::Db::open_remote(&input.coordinator_path, shape.clone(), remote.clone())
            .map_err(native_error)?;
    let churn_after_open = controller_churn(&window)?;
    let crash_recovery = stats(&crash_reopened)?;
    progress.mark("abrupt_recovery_open_complete", Some(&crash_reopened));
    let recovery_controllers = controller_identities(&crash_recovery)?;
    check(
        !recovery_controllers.is_empty(),
        "abrupt reopen observed no controllers",
    )?;
    check(
        churn_after_open == churn_after_child,
        "abrupt reopen created or removed a child controller; the orphan was not adopted",
    )?;
    let mut created_at_reopen = 0;
    let mut adopted = 0;
    for (nqn, identity) in &recovery_controllers {
        match child_controllers.get(nqn) {
            Some(child) if child == identity => adopted += 1,
            _ => created_at_reopen += 1,
        }
    }
    check(
        created_at_reopen == 0 && adopted > 0,
        "abrupt reopen did not reuse the child's controller instance and sysfs inode",
    )?;
    verify_fences(&crash_recovery, partitions)?;
    check(
        count(&crash_recovery, "recovery_read_ops")? > 0
            && count(&crash_recovery, "recovery_read_bytes")? > 0
            && count(&crash_recovery, "errors")? == 0,
        "abrupt reopen did not prove live payload recovery",
    )?;
    verify_routes(&crash_recovery, &input.expected_route)?;
    let crash_ks = crash_reopened.ks("root");
    read_expected(&crash_reopened, crash_ks, &keys[3], None)?;
    read_expected(&crash_reopened, crash_ks, &keys[5], Some(&values[5]))?;
    read_expected(&crash_reopened, crash_ks, &keys[2], Some(&values[4]))?;
    read_expected_live(&crash_reopened, crash_ks, &before_key, &before_value)?;
    read_expected_live(&crash_reopened, crash_ks, &after_key, &after_value)?;
    read_expected(&crash_reopened, crash_ks, &delete_key, None)?;
    read_expected(&crash_reopened, crash_ks, &keys[0], Some(&values[0]))?;
    // Reads never open a leased writer queue, so one live write after the
    // abrupt reopen installs the writer route that the drop check compares.
    let recovered_key = key_bytes(&input.namespace, 23, 2);
    let recovered_value = value_bytes(&input.namespace, 31, 2, 65536, 1.0);
    crash_reopened
        .insert(crash_ks, recovered_key, recovered_value)
        .map_err(native_error)?;
    let after_crash_reads = stats(&crash_reopened)?;
    verify_routes(&after_crash_reads, &input.expected_route)?;
    check(
        count(&after_crash_reads, "errors")? == 0,
        "crash-reopened live reads failed",
    )?;
    progress.mark("abrupt_recovery_checks_complete", Some(&crash_reopened));
    progress.mark("before_final_close", Some(&crash_reopened));
    let first_database = crash_reopened
        .remote_database_id()
        .ok_or("qualified database is not remote")?;
    crash_reopened
        .wait_for_background_threads_to_finish()
        .map_err(native_error)?;
    let (target_reuse, database) = qualify_target_reuse(
        &input,
        remote,
        &shape,
        first_database,
        &after_crash_reads,
        &mut progress,
    )?;
    progress.mark("target_shutdown", None);
    let target_shutdown = shutdown_targets(remote, database, &shape)?;
    progress.mark("target_shutdown_complete", None);
    let scope = "verified_kernel_namespace_not_physical_media_performance";
    let proof = serde_json::json!({
        "schema_version": 2, "kind": "live_nvmeof_qualification",
        "expected_route": input.expected_route, "durability_tier": input.durability_tier,
        "scope": scope, "start": start, "before_close": before_close,
        "member_transition": member_transition, "recovery": recovery,
        "after_miss": after_miss, "after_hit": after_hit, "after_reopen_reads": after_reopen_reads,
        "abrupt_child": child_evidence, "abrupt_recovery": crash_recovery,
        "abrupt_adoption": {
            "child_controllers": child_controllers,
            "recovery_controllers": recovery_controllers,
            "adopted": adopted,
            "created_at_reopen": created_at_reopen,
            "kernel_churn": {
                "after_child": churn_after_child,
                "after_open": churn_after_open,
            },
        },
        "after_crash_reads": after_crash_reads, "target_reuse": target_reuse,
        "target_shutdown": target_shutdown,
        "checks": {"scalar_write_delete":true,"mixed_batch":true,"ordered_batch_missing_exists":true,"checkpoint_iteration":true,"flush_reopen":true,"abrupt_tail_recovery":true,"fence_write_protect":true,"cache_miss_hit":true,"target_reuse":true,"abrupt_controller_adoption":true}
    });
    fs::write(
        output.join("qualification.json"),
        serde_json::to_vec_pretty(&proof)?,
    )?;
    progress.mark("qualification_proof_written", None);
    Ok(())
}

#[cfg(test)]
#[path = "live_qualification_tests.rs"]
mod tests;
