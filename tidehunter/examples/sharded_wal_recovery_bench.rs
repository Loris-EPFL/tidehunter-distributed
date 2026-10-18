//! Process-level checkpoint recovery measurement for externally owned shards.
//! Use each build's sharded_wal_bench to prepare remote.yaml and node launch files.
//! Seed and reopen share that directory, but require different fresh output dirs.
//! Enable --cfg checkpoint_recovery_stats when the selected frozen revision has
//! recovery counters; pass --check-cfg 'cfg(checkpoint_recovery_stats)' to the target.
#![allow(
    unexpected_cfgs,
    reason = "benchmark-only cfg passed to cargo rustc, not engine dependencies"
)]

#[cfg(all(feature = "experimental-fabric", checkpoint_recovery_stats))]
#[path = "support/fabric_profile.rs"]
mod fabric_profile;

use blake2::{Blake2b512, Digest};
use serde::Deserialize;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tidehunter::distributed::codec::Mutation;
use tidehunter::distributed::remote_db::{RemoteDatabase, RemoteDatabaseConfig};
use tidehunter::distributed::types::BatchVersion;
use tidehunter::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const BATCH_SIZE: usize = 32;
const AUTHORITY_MANIFEST: &str = "tcp-database-authority-v1";

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

struct Args {
    action: String,
    remote_config: PathBuf,
    output: PathBuf,
    value_bytes: usize,
    checkpoint_batches: usize,
    tail_batches: usize,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut options = std::collections::BTreeMap::new();
        let mut arguments = std::env::args().skip(1);
        while let Some(name) = arguments.next() {
            if !matches!(
                name.as_str(),
                "--action"
                    | "--remote-config"
                    | "--output"
                    | "--value-bytes"
                    | "--checkpoint-batches"
                    | "--tail-batches"
            ) {
                return Err(invalid(format!("unknown option {name}")).into());
            }
            let value = arguments
                .next()
                .ok_or_else(|| invalid(format!("missing value for {name}")))?;
            if options.insert(name.clone(), value).is_some() {
                return Err(invalid(format!("duplicate option {name}")).into());
            }
        }
        let mut required = |name: &str| {
            options
                .remove(name)
                .ok_or_else(|| invalid(format!("missing {name}")))
        };
        let args = Self {
            action: required("--action")?,
            remote_config: required("--remote-config")?.into(),
            output: required("--output")?.into(),
            value_bytes: required("--value-bytes")?.parse()?,
            checkpoint_batches: required("--checkpoint-batches")?.parse()?,
            tail_batches: required("--tail-batches")?.parse()?,
        };
        // These are the campaign dimensions, not an unbounded workload generator.
        // The entire tail stays below the current engine's automatic checkpoint cut.
        if !matches!(args.action.as_str(), "seed" | "reopen")
            || args.value_bytes != 8192
            || args.checkpoint_batches != 64
            || !matches!(args.tail_batches, 0 | 128)
        {
            return Err(invalid("expected action seed|reopen, value-bytes 8192, checkpoint-batches 64, tail-batches 0|128").into());
        }
        Ok(args)
    }

    fn key_count(&self) -> usize {
        self.checkpoint_batches * BATCH_SIZE
    }
}

#[derive(Deserialize)]
struct NodeLaunch {
    node: NodeShape,
}

#[derive(Deserialize)]
struct NodeShape {
    shape_yaml: String,
}

struct Launch {
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    keyspace: KeySpace,
    authority: PathBuf,
    receipt: PathBuf,
    config_digest: String,
    shape_digest: String,
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Blake2b512::digest(bytes))
}

impl Launch {
    fn read(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path)?;
        let root = path
            .parent()
            .ok_or_else(|| invalid("remote config has no parent"))?;
        let bytes = fs::read(&path)?;
        let config: RemoteDatabaseConfig = serde_yaml::from_slice(&bytes)?;
        if config.storage.partitions.len() != 5 {
            return Err(invalid("recovery campaign requires five prepared partitions").into());
        }
        // Use the actual launch shape, and reject anything the existing benchmark
        // key generator cannot represent. Never open with an assumed key length.
        let generated = KeyShape::new_single_config(
            8,
            16,
            KeyType::uniform(64),
            KeySpaceConfig::new().with_value_cache_size(0),
        );
        let expected = serde_yaml::to_value(&generated)?;
        let mut shape = None;
        for partition in &config.storage.partitions {
            let node: NodeLaunch = serde_yaml::from_slice(&fs::read(
                root.join(format!("node-{partition}/launch.yaml")),
            )?)?;
            let actual: KeyShape = serde_yaml::from_str(&node.node.shape_yaml)?;
            if serde_yaml::to_value(&actual)? != expected {
                return Err(invalid(format!(
                    "partition {partition} does not use the generated benchmark KeyShape"
                ))
                .into());
            }
            shape = Some(actual);
        }
        let shape = shape.ok_or_else(|| invalid("missing prepared key shape"))?;
        let keyspace = shape
            .iter_ks()
            .next()
            .ok_or_else(|| invalid("empty key shape"))?
            .id();
        let shape_digest = digest(serde_yaml::to_string(&shape)?.as_bytes());
        Ok(Self {
            config,
            shape,
            keyspace,
            authority: root.join("recovery-authority"),
            receipt: root.join("recovery-seed.json"),
            config_digest: digest(&bytes),
            shape_digest,
        })
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn key(index: usize) -> [u8; 8] {
    mix(index as u64).to_be_bytes()
}

fn value(index: usize, generation: u64, length: usize) -> Bytes {
    let seed = mix(index as u64 ^ generation.rotate_left(23));
    let mut bytes = vec![0; length];
    for (word, chunk) in bytes.chunks_mut(8).enumerate() {
        let pattern = mix(seed.wrapping_add(word as u64)).to_le_bytes();
        chunk.copy_from_slice(&pattern[..chunk.len()]);
    }
    bytes.into()
}

fn operation(batch: usize) -> [u8; 16] {
    let mut id = [0; 16];
    id[..8].copy_from_slice(&(batch as u64).to_be_bytes());
    id[8..].copy_from_slice(b"recovery");
    id
}

fn changes(args: &Args, batch: usize) -> impl Iterator<Item = (usize, Option<u64>)> + '_ {
    (0..BATCH_SIZE).map(move |slot| {
        if batch < args.checkpoint_batches {
            (batch * BATCH_SIZE + slot, Some(1))
        } else {
            let tail = batch - args.checkpoint_batches;
            let index = (tail * BATCH_SIZE + slot) % args.key_count();
            // Each pass both overwrites and deletes; the next pass changes which
            // keys are tombstones, exercising delete/reinsert as well as overwrite.
            let generation = 2 + (tail / args.checkpoint_batches) as u64;
            (
                index,
                if (index + generation as usize).is_multiple_of(4) {
                    None
                } else {
                    Some(generation)
                },
            )
        }
    })
}

fn mutations(args: &Args, ks: KeySpace, batch: usize) -> Vec<Mutation> {
    changes(args, batch)
        .map(|(index, generation)| match generation {
            Some(generation) => Mutation::Put {
                keyspace: ks,
                key: key(index).to_vec().into(),
                value: value(index, generation, args.value_bytes),
            },
            None => Mutation::Delete {
                keyspace: ks,
                key: key(index).to_vec().into(),
            },
        })
        .collect()
}

fn verify(db: &mut RemoteDatabase, args: &Args, ks: KeySpace) -> Result<()> {
    let mut expected = vec![Some(1); args.key_count()];
    for batch in args.checkpoint_batches..args.checkpoint_batches + args.tail_batches {
        for (index, generation) in changes(args, batch) {
            expected[index] = generation;
        }
    }
    for (index, generation) in expected.iter().enumerate() {
        let wanted = generation.map(|generation| value(index, generation, args.value_bytes));
        if db.get(ks, &key(index))? != wanted {
            return Err(
                invalid(format!("recovered value/tombstone mismatch at key {index}")).into(),
            );
        }
    }
    // A complete ordered scan additionally detects unexpected keys. Scalar gets
    // above check the tombstones that correctly do not appear in this scan.
    let mut live: Vec<_> = expected
        .iter()
        .enumerate()
        .filter_map(|(index, generation)| {
            generation.map(|generation| (key(index), index, generation))
        })
        .collect();
    live.sort_unstable_by_key(|entry| entry.0);
    let scanned = db.scan(ks)?;
    if scanned.len() != live.len() {
        return Err(invalid("recovered scan has unexpected live-key count").into());
    }
    for ((actual_key, actual_value), (key, index, generation)) in scanned.iter().zip(live) {
        if actual_key.as_ref() != key || *actual_value != value(index, generation, args.value_bytes)
        {
            return Err(invalid(format!("recovered scan mismatch at key {index}")).into());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct SeedReceipt {
    schema: u32,
    value_bytes: usize,
    batch_size: usize,
    checkpoint_batches: usize,
    tail_batches: usize,
    key_count: usize,
    config_digest: String,
    shape_digest: String,
    versions: Vec<BatchVersion>,
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn durable_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    sync_parent(path)
}

fn seed_receipt(args: &Args, launch: &Launch, versions: &[BatchVersion]) -> Result<()> {
    let mut json = Vec::new();
    write!(
        json,
        "{{\"schema\":1,\"value_bytes\":{},\"batch_size\":32,\"checkpoint_batches\":{},\"tail_batches\":{},\"key_count\":{},\"config_digest\":\"{}\",\"shape_digest\":\"{}\",\"versions\":[",
        args.value_bytes,
        args.checkpoint_batches,
        args.tail_batches,
        args.key_count(),
        launch.config_digest,
        launch.shape_digest
    )?;
    for (index, version) in versions.iter().enumerate() {
        if index != 0 {
            write!(json, ",")?;
        }
        write!(
            json,
            "{{\"epoch\":{},\"sequence\":{}}}",
            version.epoch, version.sequence
        )?;
    }
    writeln!(json, "]}}")?;
    durable_file(&launch.receipt, &json)
}

fn read_receipt(args: &Args, launch: &Launch) -> Result<SeedReceipt> {
    let receipt: SeedReceipt = serde_yaml::from_slice(&fs::read(&launch.receipt)?)?;
    if receipt.schema != 1
        || receipt.value_bytes != args.value_bytes
        || receipt.batch_size != BATCH_SIZE
        || receipt.checkpoint_batches != args.checkpoint_batches
        || receipt.tail_batches != args.tail_batches
        || receipt.key_count != args.key_count()
        || receipt.config_digest != launch.config_digest
        || receipt.shape_digest != launch.shape_digest
        || receipt.versions.len() != args.checkpoint_batches + args.tail_batches
        || receipt
            .versions
            .iter()
            .enumerate()
            .any(|(index, version)| version.epoch != 1 || version.sequence != index as u64)
    {
        return Err(invalid("seed receipt does not match this exact fresh recovery case").into());
    }
    // RemoteDatabase::open may initialize an absent authority manifest. Never
    // permit that path during reopen, even if a stale seed receipt is present.
    let manifest = fs::metadata(launch.authority.join(AUTHORITY_MANIFEST))?;
    if !manifest.is_file() || manifest.len() == 0 {
        return Err(invalid("missing or empty seeded authority manifest").into());
    }
    Ok(receipt)
}

struct Evidence {
    open_ns: u64,
    checkpoint_create_ns: u64,
    tail_append_ns: u64,
    verification_ns: u64,
    verified_keys: usize,
    exact_retry_verified: bool,
    abrupt_exit: bool,
}

fn evidence(args: &Args, db: &RemoteDatabase, record: &Evidence) -> Result<()> {
    let mut json = Vec::new();
    write!(
        json,
        "{{\"schema\":1,\"action\":\"{}\",\"value_bytes\":{},\"batch_size\":32,\"checkpoint_batches\":{},\"tail_batches\":{},\"key_count\":{},\"open_ns\":{},\"checkpoint_create_ns\":{},\"tail_append_ns\":{},\"verification_ns\":{},\"verified_keys\":{},\"exact_retry_verified\":{},\"abrupt_exit\":{}",
        args.action,
        args.value_bytes,
        args.checkpoint_batches,
        args.tail_batches,
        args.key_count(),
        record.open_ns,
        record.checkpoint_create_ns,
        record.tail_append_ns,
        record.verification_ns,
        record.verified_keys,
        record.exact_retry_verified,
        record.abrupt_exit
    )?;
    #[cfg(checkpoint_recovery_stats)]
    {
        let stats = db.recovery_stats();
        write!(
            json,
            ",\"recovery_stats\":{{\"checkpoint_loaded\":{},\"checkpoint_load_ns\":{},\"proof_validation_ns\":{},\"tail_replay_ns\":{},\"checkpoint_commit_ns\":{},\"connection_setup_ns\":{},\"fencing_ns\":{},\"tail_batches\":{},\"tail_bytes\":{},\"tls_config_ns\":{},\"read_lane_setup_ns\":{},\"connect_worker_ns\":{},\"status_worker_ns\":{},\"tls_worker_ns\":{},\"fabric_worker_ns\":",
            stats.checkpoint_loaded,
            stats.checkpoint_load_ns,
            stats.proof_validation_ns,
            stats.tail_replay_ns,
            stats.checkpoint_commit_ns,
            stats.connection_setup_ns,
            stats.fencing_ns,
            stats.tail_batches,
            stats.tail_bytes,
            stats.tls_config_ns,
            stats.read_lane_setup_ns,
            stats.connect_worker_ns,
            stats.status_worker_ns,
            stats.tls_worker_ns
        )?;
        if let Some(elapsed) = stats.fabric_worker_ns {
            write!(json, "{elapsed}")?;
        } else {
            write!(json, "null")?;
        }
        write!(
            json,
            ",\"read_lane_wait_ns\":{},\"created_connections\":{},\"reused_connections\":{},\"transport_checkout_ns\":{}",
            stats.read_lane_wait_ns,
            stats.created_connections,
            stats.reused_connections,
            stats.transport_checkout_ns,
        )?;
        #[cfg(feature = "experimental-fabric")]
        if let Some(profile) = stats.fabric_open_worker {
            for (name, value) in fabric_profile::counters(profile) {
                write!(json, ",\"{name}\":{value}")?;
            }
        }
        write!(json, "}}")?;
    }
    #[cfg(not(checkpoint_recovery_stats))]
    let _ = db;
    writeln!(json, "}}")?;
    durable_file(&args.output.join("recovery.json"), &json)?;
    durable_file(&args.output.join("COMPLETE"), b"complete\n")
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let launch = Launch::read(&args.remote_config)?;
    let receipt = if args.action == "reopen" {
        Some(read_receipt(&args, &launch)?)
    } else {
        for path in [&launch.authority, &launch.receipt] {
            match fs::symlink_metadata(path) {
                Ok(_) => {
                    return Err(invalid(format!("seed refuses existing {}", path.display())).into());
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
        None
    };
    fs::create_dir(&args.output)?;
    sync_parent(&args.output)?;
    let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let config = launch.config.clone();
    let started = Instant::now();
    let mut db = RemoteDatabase::open(&launch.authority, &launch.shape, config, metrics)?;
    let mut record = Evidence {
        open_ns: ns(started),
        checkpoint_create_ns: 0,
        tail_append_ns: 0,
        verification_ns: 0,
        verified_keys: 0,
        exact_retry_verified: false,
        abrupt_exit: false,
    };
    if let Some(receipt) = receipt {
        #[cfg(checkpoint_recovery_stats)]
        {
            let stats = db.recovery_stats();
            if !stats.checkpoint_loaded
                || stats.tail_batches != args.tail_batches as u64
                || (args.tail_batches == 0) != (stats.tail_bytes == 0)
            {
                return Err(invalid(
                    "reopen did not adopt the committed checkpoint and exact requested tail",
                )
                .into());
            }
        }
        let started = Instant::now();
        verify(&mut db, &args, launch.keyspace)?;
        // Retry every original checkpoint AND tail operation, using the actual
        // durably recorded identities, then ensure none resurrects old contents.
        for (batch, expected) in receipt.versions.iter().enumerate() {
            let actual =
                db.write_batch(operation(batch), mutations(&args, launch.keyspace, batch))?;
            if actual != *expected {
                return Err(
                    invalid(format!("retry changed original identity for batch {batch}")).into(),
                );
            }
        }
        verify(&mut db, &args, launch.keyspace)?;
        record.verification_ns = ns(started);
        record.verified_keys = args.key_count();
        record.exact_retry_verified = true;
        evidence(&args, &db, &record)?;
        db.shutdown()?;
        Ok(())
    } else {
        if !db.scan(launch.keyspace)?.is_empty() {
            return Err(invalid("seed requires empty external shard storage").into());
        }
        let mut versions = Vec::with_capacity(args.checkpoint_batches + args.tail_batches);
        for batch in 0..args.checkpoint_batches {
            versions.push(db.submit(operation(batch), mutations(&args, launch.keyspace, batch))?);
            if (batch + 1).is_multiple_of(BATCH_SIZE) {
                db.drain()?;
            }
        }
        db.drain()?;
        let started = Instant::now();
        db.flush_index()?;
        record.checkpoint_create_ns = ns(started);
        let started = Instant::now();
        for batch in args.checkpoint_batches..args.checkpoint_batches + args.tail_batches {
            versions.push(db.submit(operation(batch), mutations(&args, launch.keyspace, batch))?);
            if (batch + 1).is_multiple_of(BATCH_SIZE) {
                db.drain()?;
            }
        }
        db.drain()?;
        record.tail_append_ns = ns(started);
        if versions
            .iter()
            .enumerate()
            .any(|(index, version)| version.epoch != 1 || version.sequence != index as u64)
        {
            return Err(
                invalid("seed did not receive fresh consecutive original identities").into(),
            );
        }
        seed_receipt(&args, &launch, &versions)?;
        record.abrupt_exit = true;
        evidence(&args, &db, &record)?;
        // The controller owns all five shard processes. Neither RemoteDatabase
        // shutdown nor Drop runs here; notably no shutdown checkpoint is created.
        std::process::exit(0)
    }
}
