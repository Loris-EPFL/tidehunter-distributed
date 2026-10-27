//! Local Sui adapter comparison: real storage processes, one shared SSD.
//! Latencies exclude value verification; throughput includes verification and
//! thread rendezvous. Warm reads do not claim physical NVMe reads. All writes
//! use the value-WAL durability barrier on both backends.

#[path = "support/cache_contention.rs"]
mod cache_contention;
#[path = "support/tcp_cluster.rs"]
mod tcp_cluster;
#[path = "support/transport_config.rs"]
mod transport_config;

use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;
use tidehunter::config::Config;
use tidehunter::distributed::codec::BatchLimits;
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::storage::WalMemberReadPolicy;
use tidehunter::distributed::sui::Db;
use tidehunter::distributed::tcp::{PollingPolicy, Transport};
use tidehunter::key_shape::{KeyShape, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

type Result<T> = tcp_cluster::Result<T>;
const MIB: usize = 1024 * 1024;
const KEYS: usize = 512;
const CACHE_BYTES: usize = 16 * MIB;

fn adapter_error(error: tidehunter::db::DbError) -> String {
    format!("Sui adapter: {error:?}")
}

#[derive(Serialize)]
struct Row {
    operation: &'static str,
    workers: usize,
    keys_per_operation: usize,
    operations: usize,
    elapsed_ns: u128,
    p50_ns: u64,
    p99_ns: Option<u64>,
    mean_ns: u64,
    before: BTreeMap<&'static str, u64>,
    after: BTreeMap<&'static str, u64>,
    api_samples_ns: Vec<u64>,
}

fn counters(db: &Db) -> BTreeMap<&'static str, u64> {
    let mut values = BTreeMap::new();
    db.visit_timing_metrics(|name, value| {
        values.insert(name, value);
    });
    if let Some((cache, _, rpcs)) = db.remote_counters() {
        values.insert("read_rpcs", rpcs);
        values.insert("cache_hits", cache.hits);
        values.insert("cache_misses", cache.misses);
    }
    values
}

#[allow(clippy::too_many_arguments)] // Each field identifies the measured workload.
fn measure<T: Send>(
    db: &Db,
    operation: &'static str,
    workers: usize,
    keys_per_operation: usize,
    repetitions: usize,
    run: impl Fn(usize, usize) -> std::result::Result<T, String> + Sync,
    validate: impl Fn(T) -> bool + Sync,
) -> Result<Row> {
    let before = counters(db);
    let barrier = Barrier::new(workers + 1);
    let (mut samples, elapsed_ns) = std::thread::scope(|scope| {
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let (barrier, run, validate) = (&barrier, &run, &validate);
            tasks.push(scope.spawn(move || {
                let mut samples = Vec::with_capacity(repetitions);
                barrier.wait();
                for iteration in 0..repetitions {
                    let start = Instant::now();
                    let result = run(worker, iteration)?;
                    samples.push(start.elapsed().as_nanos() as u64);
                    if !validate(result) {
                        return Err("value verification failed".to_owned());
                    }
                }
                Ok::<_, String>(samples)
            }));
        }
        let start = Instant::now();
        barrier.wait();
        let mut samples = Vec::with_capacity(workers * repetitions);
        for task in tasks {
            samples.extend(task.join().map_err(|_| "measurement worker panicked")??);
        }
        Ok::<_, String>((samples, start.elapsed().as_nanos()))
    })?;
    let raw_samples = samples.clone();
    samples.sort_unstable();
    let operations = samples.len();
    let mean_ns =
        (samples.iter().map(|&v| u128::from(v)).sum::<u128>() / operations as u128) as u64;
    Ok(Row {
        operation,
        workers,
        keys_per_operation,
        operations,
        elapsed_ns,
        p50_ns: samples[(operations - 1) / 2],
        p99_ns: (operations >= 100).then(|| samples[(operations * 99).div_ceil(100) - 1]),
        mean_ns,
        before,
        after: counters(db),
        api_samples_ns: raw_samples,
    })
}

fn key(id: usize) -> Bytes {
    Bytes::copy_from_slice(
        &(id as u64)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .to_be_bytes(),
    )
}

fn run_case(
    output: &Path,
    backend: &str,
    value_bytes: usize,
    profile: bool,
    transport: &Transport,
) -> Result<()> {
    fs::create_dir(output)?;
    let temporary = tempdir::TempDir::new_in(output, "owned-data")?;
    let database = temporary.path().join("database");
    fs::create_dir(&database)?;
    let cache_bytes = if backend == "tcp_no_coordinator_cache" {
        0
    } else {
        CACHE_BYTES
    };
    let shape = KeyShape::new_single_config(
        8,
        16,
        KeyType::uniform(64),
        KeySpaceConfig::new().with_value_cache_size(cache_bytes / (16 * 64 * value_bytes)),
    );
    let cluster;
    let db = if backend == "native" {
        cluster = None;
        let mut config = Config::small();
        config.wal_file_size = (128 * MIB) as u64;
        config.max_maps = 15;
        config.max_index_maps = Some(3);
        config.max_dirty_keys = 1024;
        config.snapshot_written_bytes = u64::MAX / 2;
        config.snapshot_unload_threshold = u64::MAX / 2;
        config.metrics_enabled = false;
        let db = Db::open_native(
            &database,
            shape,
            Arc::new(config),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .map_err(adapter_error)?;
        db.set_native_durable(true);
        db
    } else {
        let storage = LocalInventoryConfig {
            database: [83; 16],
            partitions: vec![0, 1, 2, 3, 4],
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
            max_batches_per_partition: 4096,
            max_reference_metadata_bytes: 64 * MIB,
            max_epochs: 4,
        };
        cluster = Some(tcp_cluster::LoopbackCluster::start_with_transport(
            &temporary.path().join("cluster"),
            &output.join("nodes"),
            &shape,
            storage,
            4096,
            128 * MIB,
            None,
            transport.clone(),
            2,
            WalMemberReadPolicy::Async,
            0,
            128 * MIB,
            PollingPolicy::Adaptive,
            true,
            tidehunter::distributed::tcp::ResidentReadConfig::default(),
        )?);
        let mut config = cluster
            .as_ref()
            .expect("cluster was created")
            .config
            .clone();
        config.read_cache_bytes = cache_bytes;
        config.write_profile_limit = if profile { 128 } else { 0 };
        Db::open_remote(&database, shape, config).map_err(adapter_error)?
    };
    db.set_read_profile_limit(if profile { 4096 } else { 0 });
    db.set_native_write_profile_limit(if profile { 640 } else { 0 });
    let ks = db.shape().iter_ks().next().expect("single keyspace").id();
    let keys: Vec<_> = (0..KEYS).map(key).collect();
    let missing = key(KEYS);
    let value = Bytes::from(vec![0x5a; value_bytes]);
    for chunk in keys.chunks(32) {
        let mut batch = db.write_batch();
        for key in chunk {
            batch.write(ks, key.clone(), value.clone());
        }
        batch.commit().map_err(adapter_error)?;
    }
    // Establish warm page/index/cache state before either backend is measured.
    for key in &keys {
        if db.get(ks, key).map_err(adapter_error)?.as_deref() != Some(value.as_ref()) {
            return Err(io::Error::other("warmup value mismatch").into());
        }
    }
    let mut rows = Vec::new();
    rows.push(measure(
        &db,
        "empty_multi_get",
        1,
        0,
        1024,
        |_, _| Ok(db.multi_get(ks, &[])),
        |values| values.is_empty(),
    )?);
    rows.push(measure(
        &db,
        "absent_get",
        1,
        1,
        1024,
        |_, _| db.get(ks, &missing).map_err(adapter_error),
        |value| value.is_none(),
    )?);
    for workers in [1, 8] {
        rows.push(measure(
            &db,
            "warm_get",
            workers,
            1,
            1024 / workers,
            |worker, iteration| {
                db.get(ks, &keys[(worker + iteration * workers) % KEYS])
                    .map_err(adapter_error)
            },
            |actual| actual.as_deref() == Some(value.as_ref()),
        )?);
    }
    let bulk: Vec<Vec<u8>> = keys[..32].iter().map(|key| key.to_vec()).collect();
    rows.push(measure(
        &db,
        "warm_multi_get",
        1,
        32,
        128,
        |_, _| Ok(db.multi_get(ks, &bulk)),
        |values| {
            values.len() == bulk.len()
                && values
                    .into_iter()
                    .all(|actual| matches!(&actual, Ok(Some(bytes)) if bytes == &value))
        },
    )?);
    let mut ordered_keys = keys.clone();
    ordered_keys.sort();
    rows.push(measure(
        &db,
        "warm_iterator",
        1,
        KEYS,
        16,
        |_, _| {
            db.iterator(ks)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(adapter_error)
        },
        |records| {
            records.len() == KEYS
                && records
                    .iter()
                    .zip(&ordered_keys)
                    .all(|((key, actual), expected)| key == expected && actual == &value)
        },
    )?);
    let mut expected = vec![value.clone(); KEYS];
    for batch_size in [1, 32] {
        for workers in [1, 8] {
            let values: Vec<Vec<Bytes>> = (0..workers)
                .map(|worker| {
                    (0..32)
                        .map(|iteration| {
                            let mut bytes = vec![0x5a; value_bytes];
                            let identity = ((batch_size as u64) << 32)
                                | ((workers as u64) << 16)
                                | ((worker as u64) << 8)
                                | iteration as u64;
                            bytes[..8].copy_from_slice(&identity.to_be_bytes());
                            Bytes::from(bytes)
                        })
                        .collect()
                })
                .collect();
            for (worker, values) in values.iter().enumerate() {
                for (iteration, value) in values.iter().enumerate() {
                    for member in 0..batch_size {
                        expected[(worker * 32 + member + iteration * 256) % KEYS] = value.clone();
                    }
                }
            }
            rows.push(measure(
                &db,
                "durable_write",
                workers,
                batch_size,
                32,
                |worker, iteration| {
                    let mut batch = db.write_batch();
                    for member in 0..batch_size {
                        let index = (worker * 32 + member + iteration * 256) % KEYS;
                        batch.write(ks, keys[index].clone(), values[worker][iteration].clone());
                    }
                    batch.commit().map_err(adapter_error)
                },
                |()| true,
            )?);
        }
    }
    if cache_bytes != 0 {
        let hot: Vec<_> = keys[..8]
            .iter()
            .zip(&expected[..8])
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let writer = (keys[KEYS - 1].clone(), expected[KEYS - 1].clone());
        let observations = [1, 8]
            .into_iter()
            .map(|callers| cache_contention::measure(&db, ks, &hot, &writer, callers))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        serde_yaml::to_writer(
            File::create(output.join("cache-contention.yaml"))?,
            &observations,
        )?;
    }
    for (key, expected) in keys.iter().zip(&expected) {
        if db.get(ks, key).map_err(adapter_error)?.as_deref() != Some(expected.as_ref()) {
            return Err(io::Error::other("final value mismatch").into());
        }
    }
    db.wait_for_background_threads_to_finish()
        .map_err(adapter_error)?;
    drop(cluster);
    temporary.close()?;
    serde_yaml::to_writer(File::create(output.join("measurements.yaml"))?, &rows)?;
    println!(
        "{backend} value_bytes={value_bytes}: verified {} measurement rows; owned nodes stopped",
        rows.len()
    );
    Ok(())
}

fn main() -> Result<()> {
    if tcp_cluster::maybe_run_storage_node()? {
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    let output = args.next().ok_or(
        "usage: sharded_wal_adapter_bench NEW_OUTPUT_DIRECTORY [--profile] [--transport-config JSON_FILE]",
    )?;
    let mut profile = false;
    let mut transport = Transport::Tcp;
    while let Some(flag) = args.next() {
        if flag == "--profile" {
            if profile {
                return Err("duplicate --profile".into());
            }
            profile = true;
        } else if flag == "--transport-config" {
            let path = args.next().ok_or("missing --transport-config JSON_FILE")?;
            transport = transport_config::load(Path::new(&path))?;
        } else {
            return Err("unknown adapter benchmark argument".into());
        }
    }
    transport.validate()?;
    let output = Path::new(&output);
    fs::create_dir(output)?;
    for value_bytes in [256, 8192] {
        for backend in [
            "native",
            "tcp_no_coordinator_cache",
            "tcp_coordinator_cache",
        ] {
            run_case(
                &output.join(format!("{backend}-{value_bytes}")),
                backend,
                value_bytes,
                profile,
                &transport,
            )?;
        }
    }
    Ok(())
}
