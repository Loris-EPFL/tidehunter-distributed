//! Persistent authenticated RAM-response floor, not a storage benchmark.
//! No WAL/index work runs between the measured request and response. TLS,
//! framing, reactor scheduling and client payload validation remain measured.

#[path = "support/tcp_cluster.rs"]
mod tcp_cluster;

use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tidehunter::distributed::codec::BatchLimits;
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::storage::{PartitionConfig, StaticInventory, WalMemberReadPolicy};
use tidehunter::distributed::tcp::{Client, PollingPolicy, Transport};
use tidehunter::key_shape::{KeyShape, KeyType};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const MIB: usize = 1024 * 1024;

struct Args {
    output: PathBuf,
    remote: Option<PathBuf>,
    transport: Transport,
    polling: PollingPolicy,
    samples: usize,
    warmup: usize,
    repetitions: usize,
    shards: usize,
    values: Vec<usize>,
    idle_us: u64,
    profile: bool,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self {
            output: PathBuf::from("logs/transport-floor"),
            remote: None,
            transport: Transport::Tcp,
            polling: PollingPolicy::Adaptive,
            samples: 1000,
            warmup: 64,
            repetitions: 3,
            shards: 5,
            values: vec![1024, 8192],
            idle_us: 0,
            profile: false,
        };
        let mut input = std::env::args().skip(1);
        while let Some(flag) = input.next() {
            if flag == "--help" {
                println!(
                    "sharded_wal_transport_bench --output NEW_DIR [--remote-config YAML] [--transport tcp|fabric-tcp|efa|efa-direct] [--polling adaptive|busy] [--samples 100..100000] [--warmup 1..10000] [--repetitions 1..10] [--shards 1..5] [--values 1024,8192] [--idle-us 0..10000] [--profile true|false]"
                );
                println!(
                    "Local nodes are owned and stopped by this process. External nodes are operator-owned; their authenticated polling policy must match. Timed API includes full deterministic payload validation. Idle injection and result writing are outside each sample timer. Profiling adds clocks; compare unprofiled runs separately."
                );
                std::process::exit(0);
            }
            let value = input.next().ok_or_else(|| invalid("missing flag value"))?;
            match flag.as_str() {
                "--output" => args.output = value.into(),
                "--remote-config" => args.remote = Some(value.into()),
                "--transport" => {
                    args.transport = match value.as_str() {
                        "tcp" => Transport::Tcp,
                        "fabric-tcp" => Transport::FabricTcp,
                        "efa" => Transport::Efa,
                        "efa-direct" => Transport::EfaDirect,
                        _ => return Err(invalid("unknown transport").into()),
                    };
                }
                "--polling" => {
                    args.polling = match value.as_str() {
                        "adaptive" => PollingPolicy::Adaptive,
                        "busy" => PollingPolicy::Busy,
                        _ => return Err(invalid("unknown polling policy").into()),
                    };
                }
                "--samples" => args.samples = value.parse()?,
                "--warmup" => args.warmup = value.parse()?,
                "--repetitions" => args.repetitions = value.parse()?,
                "--shards" => args.shards = value.parse()?,
                "--values" => {
                    args.values = value
                        .split(',')
                        .map(str::parse)
                        .collect::<std::result::Result<_, _>>()?;
                }
                "--idle-us" => args.idle_us = value.parse()?,
                "--profile" => args.profile = value.parse()?,
                _ => return Err(invalid("unknown flag").into()),
            }
        }
        if !(100..=100_000).contains(&args.samples)
            || !(1..=10_000).contains(&args.warmup)
            || !(1..=10).contains(&args.repetitions)
            || !(1..=5).contains(&args.shards)
            || !(1..=8).contains(&args.values.len())
            || args
                .values
                .iter()
                .any(|bytes| !(1..=65_536).contains(bytes))
            || args.idle_us > 10_000
        {
            return Err(invalid("benchmark bounds exceeded").into());
        }
        args.transport.validate()?;
        Ok(args)
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn storage(shards: usize) -> LocalInventoryConfig {
    LocalInventoryConfig {
        database: [84; 16],
        partitions: (0..shards as u32).collect(),
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
        max_batches_per_partition: 256,
        max_reference_metadata_bytes: 16 * MIB,
        max_epochs: 8,
    }
}

fn connect(config: &RemoteDatabaseConfig, shape: &KeyShape) -> Result<Vec<Client>> {
    let mut clients = Vec::with_capacity(config.endpoints.len());
    for endpoint in &config.endpoints {
        let mut tls = config.tls.clone();
        tls.server_name.clone_from(&endpoint.server_name);
        let storage = &config.storage;
        clients.push(Client::connect(
            &endpoint.address,
            &tls,
            config.timeout,
            PartitionConfig {
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
            },
            shape,
        )?);
    }
    Ok(clients)
}

fn run(args: &Args, clients: &mut [Client]) -> Result<()> {
    let mut samples = BufWriter::new(File::create(args.output.join("samples.csv"))?);
    let mut summary = BufWriter::new(File::create(args.output.join("summary.csv"))?);
    writeln!(
        samples,
        "value_bytes,repetition,sample,partition_index,api_ns"
    )?;
    writeln!(
        summary,
        "transport,polling,instrumented,shards,value_bytes,repetition,idle_us,samples,wall_ns,p50_ns,p95_ns,p99_ns,max_ns,phase_ops_per_sec"
    )?;
    for client in clients.iter() {
        if client.polling_policy() != args.polling {
            return Err(invalid(
                "authenticated server polling policy differs from requested policy",
            )
            .into());
        }
    }
    for repetition in 0..args.repetitions {
        for &value_bytes in &args.values {
            for client in clients.iter_mut() {
                client.prepare_ram_probe(value_bytes)?;
                for _ in 0..args.warmup {
                    client.read_ram_probe()?;
                }
            }
            let before = clients
                .iter_mut()
                .map(Client::read_io_stats)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for client in clients.iter_mut() {
                client.connection_profile(args.profile, true)?;
            }
            let mut elapsed = Vec::with_capacity(args.samples);
            let wall = Instant::now();
            for sample in 0..args.samples {
                if args.idle_us != 0 {
                    std::thread::sleep(Duration::from_micros(args.idle_us));
                }
                let partition = sample % clients.len();
                let client = &mut clients[partition];
                let start = Instant::now();
                let value = client.read_ram_probe()?;
                elapsed.push(start.elapsed().as_nanos());
                // read_ram_probe validates length and every byte before returning.
                std::hint::black_box(value);
            }
            let wall_ns = wall.elapsed().as_nanos();
            let profiles = clients
                .iter_mut()
                .map(|client| client.connection_profile(false, false))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let after = clients
                .iter_mut()
                .map(Client::read_io_stats)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for (before, after) in before.iter().zip(&after) {
                if before.wal_reads != after.wal_reads
                    || before.wal_read_bytes != after.wal_read_bytes
                {
                    return Err(
                        io::Error::other("RAM response unexpectedly accessed WAL records").into(),
                    );
                }
            }
            fs::write(
                args.output
                    .join(format!("evidence-v{value_bytes}-r{repetition}.yaml")),
                serde_yaml::to_string(&(before, after, profiles))?,
            )?;
            for (sample, ns) in elapsed.iter().enumerate() {
                writeln!(
                    samples,
                    "{value_bytes},{repetition},{sample},{},{ns}",
                    sample % clients.len()
                )?;
            }
            elapsed.sort_unstable();
            let quantile = |percent: usize| elapsed[(elapsed.len() * percent).div_ceil(100) - 1];
            writeln!(
                summary,
                "{},{:?},{},{},{value_bytes},{repetition},{},{},{wall_ns},{},{},{},{},{}",
                args.transport.label(),
                args.polling,
                args.profile,
                clients.len(),
                args.idle_us,
                args.samples,
                quantile(50),
                quantile(95),
                quantile(99),
                elapsed[elapsed.len() - 1],
                args.samples as f64 * 1e9 / wall_ns.max(1) as f64,
            )?;
            samples.flush()?;
            summary.flush()?;
            println!(
                "bytes={value_bytes} repetition={repetition} p50_ns={} p99_ns={} wal_record_reads=0",
                quantile(50),
                quantile(99)
            );
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    if tcp_cluster::maybe_run_storage_node()? {
        return Ok(());
    }
    let args = Args::parse()?;
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(&args.output)?;
    let temporary = tempdir::TempDir::new_in(&args.output, "owned-nodes")?;
    let mut cluster = None;
    let shape;
    let config = if let Some(path) = &args.remote {
        let config: RemoteDatabaseConfig = serde_yaml::from_slice(&fs::read(path)?)?;
        if config.tls.transport != args.transport || config.endpoints.len() != args.shards {
            return Err(invalid(
                "remote configuration differs from explicit transport/shard count",
            )
            .into());
        }
        // The ordinary external benchmark publishes its exact shape beside remote.yaml.
        shape = serde_yaml::from_slice(&fs::read(path.with_file_name("shape.yaml"))?)?;
        config
    } else {
        shape = KeyShape::new_single(8, 16, KeyType::uniform(64));
        let owned = tcp_cluster::LoopbackCluster::start_with_transport(
            &temporary.path().join("nodes"),
            &args.output.join("nodes"),
            &shape,
            storage(args.shards),
            256,
            64 * MIB,
            None,
            args.transport,
            2,
            WalMemberReadPolicy::Async,
            0,
            128 * MIB,
            args.polling,
            false,
        )?;
        let config = owned.config.clone();
        cluster = Some(owned);
        config
    };
    let mut metadata = File::create(args.output.join("metadata.txt"))?;
    writeln!(
        metadata,
        "transport={}\npolling={:?}\ninstrumented={}\ntopology={}\nshards={}\ntimed_scope=authenticated_exchange_decode_and_full_payload_validation\nwall_scope=measured_requests_and_offered_idle_including_scheduler_delay\nwal_io=checked_zero_member_reads\nphysical_network_latency=not_isolated\nexternal_cleanup=operator_owned\nargv={:?}",
        args.transport.label(),
        args.polling,
        args.profile,
        if args.remote.is_some() {
            "external_nodes"
        } else {
            "loopback_shared_host"
        },
        args.shards,
        std::env::args().collect::<Vec<_>>()
    )?;
    let mut clients = connect(&config, &shape)?;
    let result = run(&args, &mut clients);
    drop(clients);
    if let Some(cluster) = &mut cluster {
        cluster.shutdown()?;
    }
    result?;
    fs::write(
        args.output.join("COMPLETE"),
        "all RAM replies validated; WAL member read counters unchanged; owned nodes stopped\n",
    )?;
    Ok(())
}
