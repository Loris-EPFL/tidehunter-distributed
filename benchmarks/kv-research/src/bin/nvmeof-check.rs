//! Real-process Linux namespace qualification; this is not a performance benchmark.
#![cfg(target_os = "linux")]

#[path = "../../../../tidehunter/examples/support/tcp_cluster.rs"]
mod tcp_cluster;

use blake2::{Blake2b512, Digest};
use kv_research::workload::{key_bytes, value_bytes};
use prometheus::{Encoder, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tidehunter::distributed::codec::{BatchLimits, Mutation};
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::nvmeof::{
    ExportConfig, ExportManifest, NamespaceBinding, NvmeofReaderConfig, NvmeofTransport,
};
use tidehunter::distributed::remote_db::RemoteDatabase;
use tidehunter::distributed::storage::{PartitionLog, StaticInventory};
use tidehunter::distributed::sui;
use tidehunter::distributed::types::{BatchId, ValueRecordReference};
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::metrics::Metrics;

const MIB: usize = 1024 * 1024;
const SEED: u64 = 0x4e564d454f46;
type Result<T, E = Box<dyn Error>> = std::result::Result<T, E>;
#[derive(Serialize)]
struct ExportReady {
    partition: u32,
    epoch: u64,
    source_file_index: u64,
    manifest_path: PathBuf,
    image_path: PathBuf,
    export_uuid: [u8; 16],
    nqn: String,
    nsid: u32,
    image_len: u64,
    block_size: u32,
}
#[derive(Serialize)]
struct Ready {
    schema_version: u32,
    namespace: String,
    seed: u64,
    keys: u64,
    value_bytes: usize,
    exports: Vec<ExportReady>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bindings {
    bindings: Vec<Binding>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    export_uuid: [u8; 16],
    namespace_path: PathBuf,
    transport: String,
}
#[derive(Serialize)]
struct Sample {
    kind: &'static str,
    ids: Vec<u64>,
    elapsed_ns: u64,
}

fn fail(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&Registry::new(), false)
}
fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}
fn qualification_batch_ids(keys: u64) -> Vec<u64> {
    (0..32)
        .map(|slot| match slot {
            0 => 0,
            1 => keys - 1,
            _ if slot % 3 == 0 => keys - 1,
            _ => (slot * 7) % keys,
        })
        .collect()
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn publish_ready(root: &Path, ready: &Ready) -> Result<()> {
    let temporary = root.join("ready.pending");
    write_new(&temporary, &serde_json::to_vec_pretty(ready)?)?;
    fs::rename(temporary, root.join("ready.json"))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}
fn namespace(input: &str) -> Result<[u8; 16]> {
    if input.len() != 32
        || !input
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(fail("namespace must be exactly 32 lowercase hexadecimal digits").into());
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&input[index * 2..index * 2 + 2], 16)?;
    }
    Ok(bytes)
}
fn export_uuid(namespace: &[u8; 16], partition: u32, file: u64) -> [u8; 16] {
    let mut hash = Blake2b512::new();
    hash.update(b"tidehunter-nvmeof-qualification-export-v1");
    hash.update(namespace);
    hash.update(partition.to_be_bytes());
    hash.update(file.to_be_bytes());
    let digest = hash.finalize();
    let mut uuid = [0; 16];
    uuid.copy_from_slice(&digest[..16]);
    uuid[6] = (uuid[6] & 0x0f) | 0x40;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    uuid
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn operation(inventory: &StaticInventory, partition: u32, id: u64) -> Result<[u8; 16]> {
    for nonce in 0..1024u64 {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&id.to_be_bytes());
        operation[8..].copy_from_slice(&nonce.to_be_bytes());
        if inventory.partition_for(BatchId {
            database: inventory.database,
            operation,
        })? == partition
        {
            return Ok(operation);
        }
    }
    Err(fail("unable to route deterministic operation to requested partition").into())
}
fn wait_bindings(root: &Path) -> Result<Bindings> {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        match fs::read(root.join("bindings.json")) {
            Ok(bytes) => return Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return Err(fail("timed out waiting for bindings.json").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
fn main() -> Result<()> {
    if tcp_cluster::maybe_run_storage_node()? {
        return Ok(());
    }
    let mut args = std::env::args().skip(1);
    let Some(root) = args.next() else {
        return Err(fail(
            "usage: nvmeof-check NEW_DIRECTORY NAMESPACE_HEX [KEYS=64] [VALUE_BYTES=256]",
        )
        .into());
    };
    if root == "--help" {
        println!("nvmeof-check NEW_DIRECTORY NAMESPACE_HEX [KEYS=64] [VALUE_BYTES=256]");
        return Ok(());
    }
    let namespace_hex = args.next().ok_or_else(|| fail("missing namespace"))?;
    let namespace = namespace(&namespace_hex)?;
    let keys: u64 = args.next().map_or(Ok(64), |s| s.parse())?;
    let value_size: usize = args.next().map_or(Ok(256), |s| s.parse())?;
    if args.next().is_some()
        || !(2..=1024).contains(&keys)
        || !matches!(value_size, 64 | 256 | 1024 | 8192 | 65_536)
    {
        return Err(fail("keys must be 2..=1024; value bytes must be 64, 256, 1024, 8192, or 65536; no extra arguments").into());
    }
    let root = PathBuf::from(root);
    fs::create_dir(&root)?;
    let root = fs::canonicalize(root)?;
    let data = root.join("data");
    let evidence = root.join("owners");
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let ks = shape
        .iter_ks()
        .next()
        .ok_or_else(|| fail("empty key shape"))?
        .id();
    let storage = LocalInventoryConfig {
        database: namespace,
        partitions: vec![0, 1],
        batch_limits: BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 320 * 1024,
        },
        group_limits: GroupLimits {
            max_batches: 32,
            max_encoded_bytes: MIB - 8,
        },
        fragment_bytes: MIB as u64,
        wal_file_bytes: (128 * MIB) as u64,
        max_batches_per_partition: 1024,
        max_reference_metadata_bytes: 32 * MIB,
        max_epochs: 8,
    };
    let mut cluster = tcp_cluster::LoopbackCluster::start(
        &data,
        &evidence,
        &shape,
        storage.clone(),
        2048,
        32 * MIB,
        None,
    )?;
    cluster.config.read_cache_bytes = 32 * MIB;
    let coordinator = root.join("coordinator");
    let mut db = RemoteDatabase::open(&coordinator, &shape, cluster.config.clone(), metrics())?;
    let source_epoch = db.epoch();
    let inventory = StaticInventory {
        database: namespace,
        epoch: source_epoch,
        segment_generation: source_epoch,
        partitions: storage.partitions.clone(),
    };
    for id in 0..keys {
        let partition = (id % 2) as u32;
        db.write_batch(
            operation(&inventory, partition, id)?,
            vec![Mutation::Put {
                keyspace: ks,
                key: key_bytes(&namespace, SEED, id),
                value: value_bytes(&namespace, SEED, id, value_size, 1.0),
            }],
        )?;
    }
    // Shutdown commits a checkpoint and explicitly seals every source partition.
    db.shutdown()?;
    drop(db);
    cluster.shutdown()?;

    let expected: HashMap<Vec<u8>, u64> = (0..keys)
        .map(|id| (key_bytes(&namespace, SEED, id).to_vec(), id))
        .collect();
    let mut found = HashSet::new();
    let mut exports = Vec::<(ExportManifest, PathBuf)>::new();
    let export_root = root.join("exports");
    fs::create_dir(&export_root)?;
    for partition in 0..2u32 {
        let mut template = cluster.template(partition)?;
        template.inventory.epoch = source_epoch;
        template.inventory.segment_generation = source_epoch;
        let log_path = data.join(format!(
            "node-{partition}/storage/epoch_{source_epoch:016x}"
        ));
        let log = Arc::new(PartitionLog::recover_sealed(
            &log_path,
            &template,
            metrics(),
        )?);
        let mut groups: BTreeMap<u64, Vec<ValueRecordReference>> = BTreeMap::new();
        let mut cursor = 0;
        loop {
            let page = log.receipt_page(cursor, 128)?;
            if page.is_empty() {
                break;
            }
            cursor += page.len();
            for receipt in page {
                let batch = log.read_batch(receipt)?;
                for (index, mutation) in batch.mutations().iter().enumerate() {
                    let Mutation::Put { key, value, .. } = mutation else {
                        return Err(fail("unexpected non-put in sealed dataset").into());
                    };
                    let id = *expected
                        .get(&key[..])
                        .ok_or_else(|| fail("unexpected key in sealed dataset"))?;
                    if value[..] != value_bytes(&namespace, SEED, id, value_size, 1.0)[..]
                        || !found.insert(id)
                    {
                        return Err(fail("sealed dataset value mismatch or duplicate key").into());
                    }
                    let reference = log.record_reference(receipt, index as u32)?;
                    groups
                        .entry(reference.batch_address.offset / storage.wal_file_bytes)
                        .or_default()
                        .push(reference);
                }
            }
        }
        if groups.is_empty() {
            return Err(fail("a requested owner has no exported records").into());
        }
        for (file, references) in groups {
            let uuid = export_uuid(&namespace, partition, file);
            let directory = export_root.join(format!("partition-{partition}-file-{file}"));
            let config = ExportConfig {
                export_uuid: uuid,
                nqn: format!("nqn.2026-01.local.tidehunter:{}", hex(&uuid)),
                nsid: 1,
                block_size: 4096,
                max_catalog_bytes: 16 * MIB,
            };
            let manifest = log.export_sealed_readset(&directory, &references, &config)?;
            exports.push((manifest, directory));
        }
        drop(log);
    }
    if found.len() != keys as usize {
        return Err(fail("sealed exports do not cover every generated key").into());
    }
    for partition in 0..2 {
        cluster.restart_partition(partition)?;
    }
    let sui = sui::Db::open_remote(&coordinator, shape, cluster.config.clone())
        .map_err(|e| fail(format!("{e:?}")))?;
    let ready = Ready {
        schema_version: 1,
        namespace: namespace_hex.clone(),
        seed: SEED,
        keys,
        value_bytes: value_size,
        exports: exports
            .iter()
            .map(|(m, directory)| ExportReady {
                partition: m.partition,
                epoch: m.epoch,
                source_file_index: m.source_file_index,
                manifest_path: directory.join("manifest.json"),
                image_path: directory.join("image.wal"),
                export_uuid: m.export_uuid,
                nqn: m.nqn.clone(),
                nsid: m.nsid,
                image_len: m.image_len,
                block_size: m.block_size,
            })
            .collect(),
    };
    publish_ready(&root, &ready)?;
    let bindings = wait_bindings(&root)?;
    if bindings.bindings.len() != exports.len() {
        return Err(fail("binding count differs from exports").into());
    }
    let mut binding_map = HashMap::new();
    for binding in bindings.bindings {
        if binding_map.insert(binding.export_uuid, binding).is_some() {
            return Err(fail("duplicate namespace binding").into());
        }
    }
    let mut reader_configs = Vec::with_capacity(exports.len());
    for (manifest, _) in exports {
        let binding = binding_map
            .remove(&manifest.export_uuid)
            .ok_or_else(|| fail("missing namespace binding"))?;
        let transport = match binding.transport.as_str() {
            "tcp" => NvmeofTransport::Tcp,
            "rdma" => NvmeofTransport::Rdma,
            _ => return Err(fail("transport must be tcp or rdma").into()),
        };
        let config = NvmeofReaderConfig {
            binding: NamespaceBinding {
                namespace_path: binding.namespace_path,
                transport,
            },
            manifest,
            max_inflight_requests: 32,
            max_inflight_members: 128,
            max_inflight_bytes: 16 * MIB,
            max_catalog_bytes: 16 * MIB,
        };
        sui.install_nvmeof_reader(config.clone())
            .map_err(|e| fail(format!("{e:?}")))?;
        reader_configs.push(config);
    }
    if !binding_map.is_empty() {
        return Err(fail("unexpected namespace binding").into());
    }
    let registry = Registry::new();
    sui.register_nvmeof_metrics(&registry)
        .map_err(|e| fail(format!("{e:?}")))?;
    let mut samples = Vec::with_capacity(keys as usize + 1);
    let key = key_bytes(&namespace, SEED, 0);
    let expected = value_bytes(&namespace, SEED, 0, value_size, 1.0);
    let cache_before = sui
        .remote_counters()
        .ok_or_else(|| fail("missing coordinator counters"))?
        .0;
    let first_before: u64 = sui.nvmeof_stats().iter().map(|s| s.eligible_hits).sum();
    if sui
        .get(ks, &key)
        .map_err(|e| fail(format!("{e:?}")))?
        .as_ref()
        != Some(&expected)
    {
        return Err(fail("first scalar value mismatch").into());
    }
    let cache_after = sui
        .remote_counters()
        .ok_or_else(|| fail("missing coordinator counters"))?
        .0;
    let first_after: u64 = sui.nvmeof_stats().iter().map(|s| s.eligible_hits).sum();
    if cache_after.misses <= cache_before.misses || first_after <= first_before {
        return Err(fail("first read did not exercise coordinator miss and NVMe reader").into());
    }

    // Key zero is cached; the remaining distinct keys have not been read.
    // Duplicates exercise ordering and coalescing across both cached and NVMe members.
    let ids = qualification_batch_ids(keys);
    let batch: Vec<_> = ids
        .iter()
        .map(|&id| key_bytes(&namespace, SEED, id))
        .collect();
    let batch_cache_before = sui
        .remote_counters()
        .ok_or_else(|| fail("missing coordinator counters"))?
        .0;
    let batch_before = sui.nvmeof_stats();
    let start = Instant::now();
    let values = sui.multi_get(ks, &batch);
    let elapsed = elapsed_ns(start);
    if values.len() != ids.len() {
        return Err(fail("batch result count mismatch").into());
    }
    for (result, &id) in values.into_iter().zip(&ids) {
        if result.map_err(|e| fail(format!("{e:?}")))?.as_ref()
            != Some(&value_bytes(&namespace, SEED, id, value_size, 1.0))
        {
            return Err(fail(format!("batch value mismatch for {id}")).into());
        }
    }
    let batch_after = sui.nvmeof_stats();
    let batch_cache_after = sui
        .remote_counters()
        .ok_or_else(|| fail("missing coordinator counters"))?
        .0;
    let totals = |stats: &[tidehunter::distributed::nvmeof::NvmeofStats]| {
        stats
            .iter()
            .fold((0u64, 0u64, 0u64), |(eligible, submitted, completed), s| {
                (
                    eligible + s.eligible_hits,
                    submitted + s.io_submissions,
                    completed + s.io_completions,
                )
            })
    };
    let before = totals(&batch_before);
    let after = totals(&batch_after);
    if after.0 <= before.0
        || after.1 <= before.1
        || after.2 <= before.2
        || batch_cache_after.hits <= batch_cache_before.hits
        || batch_cache_after.misses <= batch_cache_before.misses
    {
        return Err(fail("mixed batch did not exercise cache and completed NVMe I/O").into());
    }
    let repeated = sui.multi_get(ks, &batch);
    if repeated.len() != ids.len() {
        return Err(fail("cached batch result count mismatch").into());
    }
    for (result, &id) in repeated.into_iter().zip(&ids) {
        if result.map_err(|e| fail(format!("{e:?}")))?.as_ref()
            != Some(&value_bytes(&namespace, SEED, id, value_size, 1.0))
        {
            return Err(fail(format!("cached batch value mismatch for {id}")).into());
        }
    }
    let repeat_cache = sui
        .remote_counters()
        .ok_or_else(|| fail("missing coordinator counters"))?
        .0;
    let repeat_nvme = totals(&sui.nvmeof_stats());
    if repeat_cache.hits <= batch_cache_after.hits || repeat_nvme != after {
        return Err(fail("repeated batch did not stay in coordinator cache").into());
    }
    let coordinator_evidence = serde_json::json!({
        "first_read_cache_misses": cache_after.misses - cache_before.misses,
        "first_read_nvme_eligible_members": first_after - first_before,
        "batch_operations": 1,
        "batch_members": ids.len(),
        "batch_cache_hits": batch_cache_after.hits - batch_cache_before.hits,
        "batch_cache_misses": batch_cache_after.misses - batch_cache_before.misses,
        "batch_nvme_eligible_members": after.0 - before.0,
        "batch_nvme_submissions": after.1 - before.1,
        "batch_nvme_completions": after.2 - before.2,
        "repeat_batch_operations": 1,
        "repeat_batch_members": ids.len(),
        "repeat_batch_cache_hits": repeat_cache.hits - batch_cache_after.hits,
        "repeat_batch_nvme_eligible_members": repeat_nvme.0 - after.0
    });
    samples.push(Sample {
        kind: "batch_with_duplicates",
        ids,
        elapsed_ns: elapsed,
    });
    for id in 0..keys {
        let key = key_bytes(&namespace, SEED, id);
        let expected = value_bytes(&namespace, SEED, id, value_size, 1.0);
        let start = Instant::now();
        let value = sui.get(ks, &key).map_err(|e| fail(format!("{e:?}")))?;
        let elapsed = elapsed_ns(start);
        if value.as_ref() != Some(&expected) {
            return Err(fail(format!("scalar value mismatch for {id}")).into());
        }
        samples.push(Sample {
            kind: "scalar",
            ids: vec![id],
            elapsed_ns: elapsed,
        });
    }
    let stats = sui.nvmeof_stats();
    if stats.is_empty() || stats.iter().any(|s| s.io_completions == 0) {
        return Err(fail("at least one exported reader did not complete namespace IO").into());
    }
    write_new(
        &root.join("samples.json"),
        &serde_json::to_vec_pretty(&samples)?,
    )?;
    write_new(
        &root.join("coordinator-cache-evidence.json"),
        &serde_json::to_vec_pretty(&coordinator_evidence)?,
    )?;
    write_new(
        &root.join("nvmeof-stats.json"),
        &serde_json::to_vec_pretty(&stats)?,
    )?;
    let mut text = Vec::new();
    TextEncoder::new().encode(&registry.gather(), &mut text)?;
    write_new(&root.join("metrics.prom"), &text)?;
    sui.clear_nvmeof_reader()
        .map_err(|e| fail(format!("{e:?}")))?;
    sui.wait_for_background_threads_to_finish()
        .map_err(|e| fail(format!("{e:?}")))?;
    // The last owner lease release disables its target. All initiator FDs
    // above are closed before management disconnects old controllers and
    // establishes fresh protected namespace bindings.
    write_new(
        &root.join("rebind-ready.json"),
        &serde_json::to_vec_pretty(&ready)?,
    )?;
    std::fs::File::open(&root)?.sync_all()?;
    let rebind_deadline = Instant::now() + Duration::from_secs(180);
    let rebound = loop {
        match fs::read(root.join("rebind-done.json")) {
            Ok(bytes) => break serde_json::from_slice::<Bindings>(&bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= rebind_deadline {
            return Err(fail("timed out waiting for rebind-done.json").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    if rebound.bindings.len() != reader_configs.len() {
        return Err(fail("rebound namespace count differs from exports").into());
    }
    let mut rebound_by_uuid = HashMap::new();
    for binding in rebound.bindings {
        if rebound_by_uuid
            .insert(binding.export_uuid, binding)
            .is_some()
        {
            return Err(fail("duplicate rebound namespace binding").into());
        }
    }
    for reader in &mut reader_configs {
        let binding = rebound_by_uuid
            .remove(&reader.manifest.export_uuid)
            .ok_or_else(|| fail("missing rebound namespace binding"))?;
        let transport = match binding.transport.as_str() {
            "tcp" => NvmeofTransport::Tcp,
            "rdma" => NvmeofTransport::Rdma,
            _ => return Err(fail("rebound transport must be tcp or rdma").into()),
        };
        if transport != reader.binding.transport {
            return Err(fail("rebound transport differs from initial binding").into());
        }
        reader.binding.namespace_path = binding.namespace_path;
    }
    if !rebound_by_uuid.is_empty() {
        return Err(fail("unexpected rebound namespace binding").into());
    }
    // Reopen the same committed authority through the generic adapter. Its
    // mandatory verification phase compares every value before measurement.
    let owner_configs: Vec<_> = (0..2)
        .map(|partition| {
            serde_json::json!({
                "partition": partition,
                "read_workers": 2,
                "wal_member_read": "async",
                "read_resident_first": false,
                "read_memory_allowance_bytes": 512 * MIB,
                "read_max_inflight_requests": 32,
                "read_max_inflight_bytes": 64 * MIB,
                "read_max_device_reads": 128
            })
        })
        .collect();
    let config_json = serde_json::json!({
        "schema_version": 1,
        "backend": { "kind": "tidehunter", "options": {
            "transport": { "kind": "tcp" },
            "path": &root,
            "evidence": root.join("runner-evidence"),
            "owners": 2,
            "key_count": keys,
            "max_batches_per_partition": storage.max_batches_per_partition,
            "coordinator_read_cache_bytes": 0,
            "external_remote_config": &cluster.config,
            "external_owner_read_config": owner_configs,
            "nvmeof_readers": reader_configs
        }},
        "seed": SEED,
        "namespace": "nvmeof-loopback-qualification",
        "key_namespace_hex": namespace_hex,
        "dataset": { "keys": keys, "value_size": value_size, "entropy": 1.0 },
        "workload": { "distribution": "uniform", "batch_size": 32,
            "mix": { "read": 1, "get_many": 1, "put": 0, "delete": 0,
                "missing_read": 0, "put_batch": 0 } },
        "execution": { "mode": "closed_loop", "concurrency": 1,
            "queue_capacity": 8, "request_timeout_ms": 30000 },
        "phases": { "preload": false, "verify_existing_dataset": true,
            "warmup_operations": 0, "aging_operations": 0, "measurement_operations": keys },
        "limits": { "max_keys": 1024, "max_bytes": 64 * MIB,
            "max_operations": 2 * keys, "deadline_seconds": 300, "shutdown_grace_seconds": 30 }
    });
    write_new(
        &root.join("runner-config.json"),
        &serde_json::to_vec_pretty(&config_json)?,
    )?;
    let run_config: kv_research::config::RunConfig = serde_json::from_value(config_json)?;
    run_config
        .validate()
        .map_err(|error| fail(error.to_string()))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime
        .block_on(async {
            let registry = Registry::new();
            let backend =
                kv_research::backends::tidehunter::open(&run_config.backend.options, &registry)
                    .await?;
            kv_research::runner::run(backend, run_config, &root.join("runner"), registry).await
        })
        .map_err(|error| fail(error.to_string()))?;
    cluster.shutdown()?;
    println!("qualification complete: {}", root.display());
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::qualification_batch_ids;

    #[test]
    fn qualification_batch_has_cached_unread_and_duplicate_members() {
        for keys in 2..=1024 {
            let ids = qualification_batch_ids(keys);
            assert_eq!(ids.len(), 32);
            assert_eq!(ids[0], 0);
            assert_eq!(ids[1], keys - 1);
            assert!(ids.iter().all(|&id| id < keys));
            assert!(ids.iter().filter(|&&id| id == keys - 1).count() > 1);
        }
    }
}
