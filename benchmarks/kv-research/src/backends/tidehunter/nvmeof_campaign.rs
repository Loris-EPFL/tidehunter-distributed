//! External dataset preparation and offline, owner-locked sealed WAL export.
use super::Options;
use crate::{
    backend::BackendResult,
    config::RunConfig,
    workload::{key_bytes, value_bytes},
};
use blake2::{Blake2b512, Digest};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};
use tidehunter::{
    distributed::{
        codec::{BatchLimits, CodecError, CompleteBatch, Mutation},
        nvmeof::ExportConfig,
        remote_db::RemoteDatabase,
        tcp::{NodeConfig, SealedOwnerExport},
        types::{BatchId, BatchVersion, ValueRecordReference},
    },
    key_shape::{KeyShape, KeyType},
    metrics::Metrics,
};

const MAX_KEYS: u64 = 65_536;
const PAGE: usize = 128;
const PRELOAD_RECIPE: &str = "kv-research-native-sealed-batch-v2";
const MAX_PRELOAD_MEMBERS: usize = 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepared {
    schema_version: u32,
    source_epoch: u64,
    namespace: [u8; 16],
    seed: u64,
    keys: u64,
    value_bytes: usize,
    entropy: f64,
    database: [u8; 16],
    partitions: Vec<u32>,
    preload_batch_max_members: usize,
    preload_recipe: String,
}

#[derive(Deserialize)]
struct Launch {
    node: NodeConfig,
}

// The engine conservatively charges 4096 bytes per reference for plans,
// catalog, deduplication and serialized manifest before allocating them.
fn export_catalog_budget(entries: usize, nqn: &str) -> BackendResult<usize> {
    if entries == 0 || entries > MAX_KEYS as usize || nqn.len() > 223 {
        return Err("export catalog exceeds dataset or NQN bounds".into());
    }
    entries
        .checked_mul(4096)
        .and_then(|n| n.checked_add(nqn.len()))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| "export catalog budget overflow".into())
}
fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&Registry::new(), false)
}

fn write_new(path: &Path, bytes: &[u8]) -> BackendResult<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn namespace(value: &str) -> BackendResult<[u8; 16]> {
    if value.len() != 32 || !value.is_ascii() {
        return Err("key_namespace_hex must be 32 ASCII hex characters".into());
    }
    let mut result = [0; 16];
    for (i, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)?;
    }
    Ok(result)
}

fn check_prepared(p: &Prepared) -> BackendResult<()> {
    if p.schema_version != 1
        || p.source_epoch == 0
        || !(1..=MAX_KEYS).contains(&p.keys)
        || !matches!(p.value_bytes, 64 | 256 | 1024 | 8192 | 65_536)
        || !p.entropy.is_finite()
        || !(0.0..=1.0).contains(&p.entropy)
        || p.partitions.is_empty()
        || p.partitions.len() > 1024
        || !(1..=MAX_PRELOAD_MEMBERS).contains(&p.preload_batch_max_members)
        || p.preload_recipe != PRELOAD_RECIPE
        || p.partitions.iter().copied().collect::<HashSet<_>>().len() != p.partitions.len()
    {
        return Err("invalid prepared dataset metadata".into());
    }
    Ok(())
}

// All generated keys are 32 bytes and all generated values have the same
// configured size. Probe the real native encoder once before writing any data;
// the database validates each actual batch again on submission.
fn preload_batch_limit(
    shape: &KeyShape,
    p: &Prepared,
    limits: BatchLimits,
) -> BackendResult<usize> {
    let ks = shape.iter_ks().next().ok_or("empty key shape")?.id();
    let sample = Mutation::Put {
        keyspace: ks,
        key: key_bytes(&p.namespace, p.seed, 0),
        value: value_bytes(&p.namespace, p.seed, 0, p.value_bytes, p.entropy),
    };
    let max = MAX_PRELOAD_MEMBERS.min(limits.max_operations as usize);
    for count in (1..=max).rev() {
        let mutations = vec![sample.clone(); count];
        match CompleteBatch::new(
            shape,
            BatchId {
                database: p.database,
                operation: [1; 16],
            },
            BatchVersion {
                epoch: 1,
                sequence: 0,
            },
            mutations,
            limits,
        ) {
            Ok(_) => return Ok(count),
            Err(CodecError::TooLarge) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err("native batch limits cannot hold one prepared value".into())
}

pub(super) fn prepare(input: &Path, output: &Path) -> BackendResult<()> {
    let run: RunConfig = serde_json::from_slice(&fs::read(input)?)?;
    run.validate()?;
    if run.backend.kind != "tidehunter" {
        return Err("external preparation requires the tidehunter backend".into());
    }
    let ns = namespace(
        run.key_namespace_hex
            .as_deref()
            .ok_or("explicit key_namespace_hex required")?,
    )?;
    let options: Options = serde_json::from_value(run.backend.options.clone())?;
    let config = options
        .external_remote_config
        .ok_or("external_remote_config required")?;
    let coordinator = options.path.join("coordinator");
    if coordinator.exists() {
        return Err("preparation requires a fresh coordinator directory".into());
    }
    let mut prepared = Prepared {
        schema_version: 1,
        source_epoch: 1,
        namespace: ns,
        seed: run.seed,
        keys: run.dataset.keys,
        value_bytes: run.dataset.value_size,
        entropy: run.dataset.entropy,
        database: config.storage.database,
        partitions: config.storage.partitions.clone(),
        preload_batch_max_members: 1,
        preload_recipe: PRELOAD_RECIPE.to_owned(),
    };
    check_prepared(&prepared)?;
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let ks = shape.iter_ks().next().ok_or("empty key shape")?.id();
    prepared.preload_batch_max_members =
        preload_batch_limit(&shape, &prepared, config.storage.batch_limits)?;
    fs::create_dir_all(&options.path)?;
    let mut db = RemoteDatabase::open(&coordinator, &shape, config, metrics())?;
    prepared.source_epoch = db.epoch();
    let mut start = 0;
    while start < prepared.keys {
        let end = prepared
            .keys
            .min(start + prepared.preload_batch_max_members as u64);
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&start.to_be_bytes());
        operation[8..].copy_from_slice(b"nvme-pr2");
        let mutations = (start..end)
            .map(|id| Mutation::Put {
                keyspace: ks,
                key: key_bytes(&ns, prepared.seed, id),
                value: value_bytes(
                    &ns,
                    prepared.seed,
                    id,
                    prepared.value_bytes,
                    prepared.entropy,
                ),
            })
            .collect();
        db.write_batch(operation, mutations)?;
        start = end;
    }
    db.shutdown()?;
    drop(db);
    write_new(output, &serde_json::to_vec_pretty(&prepared)?)
}

// All ancestors must be trusted root-owned directories, including the output parent.
fn trusted_directory(path: &Path) -> BackendResult<PathBuf> {
    let path = path.canonicalize()?;
    for component in path.ancestors() {
        let metadata = fs::symlink_metadata(component)?;
        if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
            return Err(format!("untrusted export ancestor: {}", component.display()).into());
        }
    }
    Ok(path)
}

fn export_uuid(p: &Prepared, partition: u32, file: u64) -> [u8; 16] {
    let mut h = Blake2b512::new();
    h.update(b"kv-research-nvmeof-export-v1");
    h.update(p.database);
    h.update(p.namespace);
    h.update(p.source_epoch.to_be_bytes());
    h.update(partition.to_be_bytes());
    h.update(file.to_be_bytes());
    let digest = h.finalize();
    let mut uuid = [0; 16];
    uuid.copy_from_slice(&digest[..16]);
    uuid[6] = (uuid[6] & 15) | 0x40;
    uuid[8] = (uuid[8] & 63) | 0x80;
    uuid
}

pub(super) fn export(launch: &Path, prepared_path: &Path, output: &Path) -> BackendResult<()> {
    let launch: Launch = serde_yaml::from_slice(&fs::read(launch)?)?;
    let p: Prepared = serde_json::from_slice(&fs::read(prepared_path)?)?;
    check_prepared(&p)?;
    let mut node = launch.node;
    let partition = node.partition.partition;
    if node.partition.inventory.database != p.database
        || node.partition.inventory.partitions != p.partitions
        || !p.partitions.contains(&partition)
    {
        return Err("owner inventory differs from prepared dataset".into());
    }
    node.root = trusted_directory(&node.root)?;
    let parent = trusted_directory(output.parent().ok_or("export output needs a parent")?)?;
    let name = output
        .file_name()
        .ok_or("export output needs a directory name")?;
    let output = parent.join(name);
    let owner = SealedOwnerExport::open(&node, p.source_epoch, metrics())?;
    fs::create_dir(&output)?;
    fs::set_permissions(&output, fs::Permissions::from_mode(0o700))?;
    let log = owner.log();
    let shape: KeyShape = serde_yaml::from_str(&node.shape_yaml)?;
    let ks = shape.iter_ks().next().ok_or("empty key shape")?.id();
    let expected: HashMap<_, _> = (0..p.keys)
        .map(|id| (key_bytes(&p.namespace, p.seed, id).to_vec(), id))
        .collect();
    if expected.len() != p.keys as usize {
        return Err("generated duplicate keys".into());
    }
    let mut seen = HashSet::new();
    let mut groups: BTreeMap<u64, Vec<ValueRecordReference>> = BTreeMap::new();
    let mut cursor = 0usize;
    loop {
        let page = log.receipt_page(cursor, PAGE)?;
        if page.is_empty() {
            break;
        }
        cursor = cursor
            .checked_add(page.len())
            .ok_or("receipt count overflow")?;
        if cursor > p.keys as usize {
            return Err("too many receipts in sealed dataset".into());
        }
        for receipt in page {
            let batch = log.read_batch(receipt)?;
            if batch.mutations().is_empty() || batch.mutations().len() > p.preload_batch_max_members
            {
                return Err("sealed receipt exceeds prepared batch bound".into());
            }
            for (index, mutation) in batch.mutations().iter().enumerate() {
                let Mutation::Put {
                    keyspace,
                    key,
                    value,
                } = mutation
                else {
                    return Err("non-put in sealed dataset".into());
                };
                if *keyspace != ks {
                    return Err("unexpected keyspace in sealed dataset".into());
                }
                let id = *expected
                    .get(&key[..])
                    .ok_or("unexpected key in sealed dataset")?;
                if !seen.insert(id)
                    || value[..]
                        != value_bytes(&p.namespace, p.seed, id, p.value_bytes, p.entropy)[..]
                {
                    return Err("duplicate key or value mismatch in sealed dataset".into());
                }
                let reference = log.record_reference(receipt, u32::try_from(index)?)?;
                let file = reference.batch_address.offset / node.partition.wal_file_bytes;
                let group = groups.entry(file).or_default();
                if group.len() >= p.keys as usize {
                    return Err("catalog bound exceeded".into());
                }
                group.push(reference);
            }
        }
    }
    let mut exports = Vec::new();
    for (file, references) in groups {
        let uuid = export_uuid(&p, partition, file);
        let hex: String = uuid.iter().map(|b| format!("{b:02x}")).collect();
        let dir = output.join(format!("partition-{partition}-file-{file}"));
        let nqn = format!("nqn.2026-01.local.tidehunter:{hex}");
        let max_catalog_bytes = export_catalog_budget(references.len(), &nqn)?;
        let config = ExportConfig {
            export_uuid: uuid,
            nqn,
            nsid: 1,
            block_size: 4096,
            max_catalog_bytes,
        };
        let m = owner.export_sealed_readset(&dir, &references, &config)?;
        exports.push(serde_json::json!({"partition":m.partition,"epoch":m.epoch,"source_file_index":m.source_file_index,"manifest_path":dir.join("manifest.json"),"image_path":dir.join("image.wal"),"export_uuid":m.export_uuid,"nqn":m.nqn,"nsid":m.nsid,"image_len":m.image_len,"block_size":m.block_size,"members":m.catalog.len()}));
    }
    let ready = serde_json::json!({"schema_version":1,"namespace":p.namespace,"seed":p.seed,"keys":p.keys,"value_bytes":p.value_bytes,"entropy":p.entropy,"source_epoch":p.source_epoch,"partition":partition,"members":seen.len(),"preload_batch_max_members":p.preload_batch_max_members,"preload_recipe":p.preload_recipe,"exports":exports});
    write_new(
        &output.join("ready.json"),
        &serde_json::to_vec_pretty(&ready)?,
    )?;
    fs::File::open(&output)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_requires_exact_hex() {
        assert!(namespace("00112233445566778899aabbccddeeff").is_ok());
        assert!(namespace("00").is_err());
        assert!(namespace("zz112233445566778899aabbccddeeff").is_err());
    }

    #[test]
    fn export_budget_admits_large_file_without_unbounded_allocation() {
        let nqn = "nqn.2026-01.local.tidehunter:0123456789abcdef";
        assert_eq!(
            export_catalog_budget(13_000, nqn).unwrap(),
            13_000 * 4096 + nqn.len() + 4096
        );
        assert!(export_catalog_budget(65_536, nqn).is_ok());
        assert!(export_catalog_budget(65_537, nqn).is_err());
        assert!(export_catalog_budget(0, nqn).is_err());
        assert!(export_catalog_budget(1, &"x".repeat(224)).is_err());
    }

    #[test]
    fn prepared_bounds_and_export_identity_are_explicit() {
        let mut p = Prepared {
            schema_version: 1,
            source_epoch: 7,
            namespace: [3; 16],
            seed: 9,
            keys: 4096,
            value_bytes: 65_536,
            entropy: 1.0,
            database: [4; 16],
            partitions: vec![0, 1],
            preload_batch_max_members: 32,
            preload_recipe: PRELOAD_RECIPE.to_owned(),
        };
        assert!(check_prepared(&p).is_ok());
        let first = export_uuid(&p, 0, 1);
        assert_ne!(first, export_uuid(&p, 1, 1));
        assert_ne!(first, export_uuid(&p, 0, 2));
        p.source_epoch += 1;
        assert_ne!(first, export_uuid(&p, 0, 1));
        p.keys = MAX_KEYS + 1;
        assert!(check_prepared(&p).is_err());
        p.keys = 1;
        p.partitions.push(1);
        assert!(check_prepared(&p).is_err());
    }

    #[test]
    fn preload_batch_limit_obeys_actual_native_encoding() {
        let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
        let prepared = Prepared {
            schema_version: 1,
            source_epoch: 1,
            namespace: [3; 16],
            seed: 9,
            keys: 65_536,
            value_bytes: 65_536,
            entropy: 1.0,
            database: [4; 16],
            partitions: vec![0, 1],
            preload_batch_max_members: 32,
            preload_recipe: PRELOAD_RECIPE.to_owned(),
        };
        let limits = BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 3 * 1024 * 1024,
        };
        assert_eq!(preload_batch_limit(&shape, &prepared, limits).unwrap(), 32);
        assert_eq!(
            preload_batch_limit(
                &shape,
                &prepared,
                BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 320 * 1024,
                }
            )
            .unwrap(),
            4
        );
        assert_eq!(
            preload_batch_limit(
                &shape,
                &prepared,
                BatchLimits {
                    max_operations: 3,
                    ..limits
                }
            )
            .unwrap(),
            3
        );
        assert!(
            preload_batch_limit(
                &shape,
                &prepared,
                BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 1024,
                }
            )
            .is_err()
        );
    }
}
