//! Real-process mTLS/TCP correctness checks. No AWS and no throughput claim.
//! Run with experimental-distributed,test-utils for deterministic response loss.

#[path = "support/tcp_cluster.rs"]
mod tcp_cluster;

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::remote_db::{RemoteDatabase, RemoteDatabaseConfig};
use tidehunter::distributed::storage::StaticInventory;
use tidehunter::distributed::tcp::{Client, Transport};
use tidehunter::distributed::types::{BatchId, BatchVersion};
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const MIB: usize = 1024 * 1024;
const BATCHES: u64 = 32;

fn error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
fn operation(n: u64) -> [u8; 16] {
    let mut id = [0; 16];
    id[..8].copy_from_slice(&n.to_be_bytes());
    id
}
fn key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}
fn value(n: u64, generation: u64) -> Bytes {
    (0..1024)
        .map(|i| ((i as u64 * 73 + n * 19 + generation * 101) % 251) as u8)
        .collect::<Vec<_>>()
        .into()
}
fn put(ks: KeySpace, n: u64, generation: u64) -> Mutation {
    Mutation::Put {
        keyspace: ks,
        key: key(n).into(),
        value: value(n, generation),
    }
}
fn initial_batch(ks: KeySpace, batch: u64) -> Vec<Mutation> {
    vec![
        put(ks, batch * 2, 1),
        put(ks, batch * 2 + 1, 1),
        put(ks, batch * 2, 2),
    ]
}
fn expected(n: u64) -> Option<Bytes> {
    match n {
        0 => Some(value(n, 3)),
        1 => None,
        n if n < BATCHES * 2 => Some(value(n, if n % 2 == 0 { 2 } else { 1 })),
        64 => Some(value(n, 4)),
        _ => None,
    }
}
fn storage() -> LocalInventoryConfig {
    LocalInventoryConfig {
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
        max_batches_per_partition: 256,
        max_reference_metadata_bytes: 16 * MIB,
        max_epochs: 8,
    }
}
#[derive(Serialize, Deserialize)]
struct CoordinatorLaunch {
    path: PathBuf,
    shape: KeyShape,
    remote: RemoteDatabaseConfig,
}

fn native_apply(db: &Arc<Db>, mutations: Vec<Mutation>) -> Result<()> {
    let mut batch = db.write_batch();
    for mutation in mutations {
        match mutation {
            Mutation::Put {
                keyspace,
                key,
                value,
            } => batch.write(keyspace, key, value),
            Mutation::Delete { keyspace, key } => batch.delete(keyspace, key),
        }
    }
    batch
        .commit()
        .map_err(|e| error(format!("native batch failed: {e:?}")))?;
    Ok(())
}

fn coordinator_crash(config: &Path) -> Result<()> {
    let launch: CoordinatorLaunch = serde_yaml::from_slice(&fs::read(config)?)?;
    let ks = launch.shape.iter_ks().next().unwrap().id();
    let mut remote = RemoteDatabase::open(&launch.path, &launch.shape, launch.remote, metrics())?;
    let native_path = launch.path.with_extension("native-oracle");
    fs::create_dir(&native_path)?;
    let mut config = Config::small();
    config.frag_size = MIB as u64;
    config.wal_file_size = (128 * MIB) as u64;
    config.metrics_enabled = false;
    let native = Db::open(
        &native_path,
        launch.shape.clone(),
        Arc::new(config),
        metrics(),
    )
    .map_err(|e| error(format!("native open failed: {e:?}")))?;
    for batch in 0..BATCHES {
        remote.submit(operation(batch), initial_batch(ks, batch))?;
        native_apply(&native, initial_batch(ks, batch))?;
        if batch % 8 == 7 {
            remote.drain()?;
        }
    }
    remote.drain()?;
    let checkpoint = remote.checkpoint()?;
    // Persist this cut, then leave the overwrite and delete outside it when the
    // process exits. Reopen must adopt the image and replay exactly those tails.
    remote.flush_index()?;
    let native_checkpoint = native.checkpoint();
    remote.write_batch(operation(100), vec![put(ks, 0, 3)])?;
    native_apply(&native, vec![put(ks, 0, 3)])?;
    if remote.get_at(ks, &key(0), &checkpoint)?
        != native_checkpoint
            .get(ks, &key(0))
            .map_err(|e| error(format!("native checkpoint: {e:?}")))?
    {
        return Err(error("checkpoint differential mismatch").into());
    }
    drop(native_checkpoint);
    let delete = Mutation::Delete {
        keyspace: ks,
        key: key(1).into(),
    };
    remote.write_batch(operation(101), vec![delete.clone()])?;
    native_apply(&native, vec![delete])?;
    for n in 0..BATCHES * 2 {
        let native_value = native
            .get(ks, &key(n))
            .map_err(|e| error(format!("native get: {e:?}")))?;
        if remote.get(ks, &key(n))? != native_value || native_value != expected(n) {
            return Err(error("full-byte native differential mismatch").into());
        }
    }
    let native_scan = native
        .iterator(ks)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| error(format!("native scan: {e:?}")))?;
    if remote.scan(ks)? != native_scan {
        return Err(error("ordered scan differential mismatch").into());
    }
    let keys: Vec<_> = (0..BATCHES * 2).map(key).collect();
    if remote.get_many(ks, &keys)? != (0..BATCHES * 2).map(expected).collect::<Vec<_>>() {
        return Err(error("multi-key differential mismatch").into());
    }
    native.wait_for_background_threads_to_finish();
    println!(
        "coordinator_acknowledged_batches=34 operations=98 native_differential=verified checkpoint=verified scan=verified get_many=verified abrupt_exit_next=true"
    );
    // Intentional process death after acknowledged writes. No RemoteDatabase
    // shutdown or destructor runs; storage processes belong to the controller.
    std::process::exit(17)
}

struct OwnedChild(Option<Child>);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn crash_coordinator(
    config: &CoordinatorLaunch,
    config_path: &Path,
    evidence: &Path,
) -> Result<()> {
    fs::write(config_path, serde_yaml::to_string(config)?)?;
    let file = |name| {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(evidence.join(name))
    };
    let child = Command::new(std::env::current_exe()?)
        .arg("--coordinator-crash")
        .arg(config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(file("coordinator.stdout.log")?))
        .stderr(Stdio::from(file("coordinator.stderr.log")?))
        .spawn()?;
    let mut child = OwnedChild(Some(child));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.0.as_mut().unwrap().try_wait()? {
            child.0.take();
            if status.code() != Some(17) {
                return Err(error(format!(
                    "coordinator did not reach its intentional crash: {status}"
                ))
                .into());
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(error("coordinator correctness phase timed out").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn audit(db: &mut RemoteDatabase, ks: KeySpace, include_fresh: bool) -> Result<()> {
    let count = if include_fresh { 65 } else { 64 };
    let keys: Vec<_> = (0..count).map(key).collect();
    let wanted: Vec<_> = (0..count).map(expected).collect();
    if db.get_many(ks, &keys)? != wanted {
        return Err(error("recovered values mismatch").into());
    }
    for n in 0..count {
        if db.get(ks, &key(n))? != expected(n) || db.exists(ks, &key(n))? != expected(n).is_some() {
            return Err(error("recovered scalar/exists mismatch").into());
        }
    }
    let expected_scan: Vec<_> = (0..count)
        .filter_map(|n| expected(n).map(|v| (Bytes::from(key(n)), v)))
        .collect();
    if db.scan(ks)? != expected_scan {
        return Err(error("recovered scan mismatch").into());
    }
    Ok(())
}

fn routed_operation(
    storage: &LocalInventoryConfig,
    partition: u32,
    first: u64,
) -> Result<[u8; 16]> {
    let inventory = StaticInventory {
        database: storage.database,
        epoch: 1,
        segment_generation: 1,
        partitions: storage.partitions.clone(),
    };
    for number in first..first + 10000 {
        let op = operation(number);
        if inventory.partition_for(BatchId {
            database: storage.database,
            operation: op,
        })? == partition
        {
            return Ok(op);
        }
    }
    Err(error("could not find a bounded operation routed to target partition").into())
}

fn main() -> Result<()> {
    if tcp_cluster::maybe_run_storage_node()? {
        return Ok(());
    }
    let mut args = std::env::args().skip(1);
    let mut first = args.next();
    let transport = if first.as_deref() == Some("--transport") {
        let selected = match args.next().as_deref() {
            Some("tcp") => Transport::Tcp,
            Some("fabric-tcp") => Transport::FabricTcp,
            _ => return Err(error("local check transport must be tcp or fabric-tcp").into()),
        };
        first = args.next();
        selected
    } else {
        Transport::Tcp
    };
    transport.validate()?;
    if first.as_deref() == Some("--coordinator-crash") {
        return coordinator_crash(Path::new(
            &args
                .next()
                .ok_or_else(|| error("missing coordinator configuration"))?,
        ));
    }
    if first.as_deref() == Some("--help") {
        println!(
            "sharded_wal_cluster_check --output NEW_DIRECTORY (requires experimental-distributed,test-utils)"
        );
        return Ok(());
    }
    let output = match first.as_deref() {
        None => PathBuf::from(format!(
            "logs/local-tcp-check-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
        )),
        Some("--output") => PathBuf::from(args.next().ok_or_else(|| error("missing output path"))?),
        _ => return Err(error("unknown correctness driver argument").into()),
    };
    if args.next().is_some() {
        return Err(error("unexpected correctness driver arguments").into());
    }
    if !cfg!(feature = "test-utils") {
        return Err(error(
            "build with test-utils for the required deterministic reply-loss scenario",
        )
        .into());
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(&output)?;
    let temporary = tempdir::TempDir::new_in(&output, "owned-tcp-data")?;
    let shape = KeyShape::new_single(8, 16, KeyType::uniform(64));
    let ks = shape.iter_ks().next().unwrap().id();
    let mut cluster = tcp_cluster::LoopbackCluster::start_with_transport(
        &temporary.path().join("healthy"),
        &output.join("healthy"),
        &shape,
        storage(),
        256,
        64 * MIB,
        None,
        transport,
        2,
        tidehunter::distributed::storage::WalMemberReadPolicy::Async,
        0,
        512 * MIB,
        tidehunter::distributed::tcp::PollingPolicy::Adaptive,
        false,
    )?;
    let path = temporary.path().join("authority");
    let config = CoordinatorLaunch {
        path: path.clone(),
        shape: shape.clone(),
        remote: cluster.config.clone(),
    };
    crash_coordinator(&config, &temporary.path().join("coordinator.yaml"), &output)?;
    println!("separate_coordinator_abrupt_exit=verified");
    let mut db = RemoteDatabase::open(&path, &shape, cluster.config.clone(), metrics())?;
    let recovery = db.recovery_stats();
    if !recovery.checkpoint_loaded || recovery.tail_batches != 2 || recovery.tail_bytes == 0 {
        return Err(
            error("reopen did not adopt the checkpoint and replay both tail batches").into(),
        );
    }
    println!(
        "checkpoint_reopen=verified tail_batches={} tail_bytes={}",
        recovery.tail_batches, recovery.tail_bytes
    );
    audit(&mut db, ks, false)?;
    let old = db.write_batch(operation(0), initial_batch(ks, 0))?;
    if old
        != (BatchVersion {
            epoch: 1,
            sequence: 0,
        })
        || db.get(ks, &key(0))? != expected(0)
    {
        return Err(error("old retry changed identity or overwrote newer value").into());
    }
    db.write_batch(operation(102), vec![put(ks, 64, 4)])?;
    audit(&mut db, ks, true)?;
    db.shutdown()?;
    drop(db);
    println!("full_inventory_reopen=verified old_retry=verified new_epoch_write=verified");

    // Stop a real reconfiguration after its durable Preparing manifest and one
    // successful remote fence. This is an authorized interrupted epoch, unlike
    // forging an external epoch that never appeared in the authority inventory.
    #[cfg(feature = "test-utils")]
    if RemoteDatabase::open_fail_after_fences(&path, &shape, cluster.config.clone(), metrics(), 1)
        .is_ok()
    {
        return Err(error("partial fencing injection did not interrupt activation").into());
    }
    // A missing registered process must block resuming this authority.
    cluster.kill_partition(4)?;
    if RemoteDatabase::open(&path, &shape, cluster.config.clone(), metrics()).is_ok() {
        return Err(error("incomplete inventory opened with a missing node").into());
    }
    cluster.restart_partition(4)?;
    let mut db = RemoteDatabase::open(&path, &shape, cluster.config.clone(), metrics())?;
    audit(&mut db, ks, true)?;
    db.shutdown()?;
    drop(db);
    println!(
        "partial_fence_resume=verified missing_node_blocks_open=verified node_process_restart=verified"
    );

    let endpoint = &cluster.config.endpoints[0];
    let mut wrong_name = cluster.config.tls.clone();
    wrong_name.server_name = "unrelated.invalid".to_owned();
    if Client::connect(
        &endpoint.address,
        &wrong_name,
        Duration::from_secs(2),
        cluster.template(0)?,
        &shape,
    )
    .is_ok()
    {
        return Err(error("wrong server identity accepted").into());
    }
    let mut untrusted = cluster.untrusted_client_tls(&temporary.path().join("foreign-tls"))?;
    untrusted.server_name = endpoint.server_name.clone();
    if Client::connect(
        &endpoint.address,
        &untrusted,
        Duration::from_secs(2),
        cluster.template(0)?,
        &shape,
    )
    .is_ok()
    {
        return Err(error("untrusted client identity accepted").into());
    }
    println!("wrong_server_name=verified untrusted_client_ca=verified");
    cluster.shutdown()?;

    let mut lost = tcp_cluster::LoopbackCluster::start_with_transport(
        &temporary.path().join("lost-response"),
        &output.join("lost-response"),
        &shape,
        storage(),
        256,
        64 * MIB,
        Some(0),
        transport,
        2,
        tidehunter::distributed::storage::WalMemberReadPolicy::Async,
        0,
        512 * MIB,
        tidehunter::distributed::tcp::PollingPolicy::Adaptive,
        false,
    )?;
    let mut client = lost.client(0, &shape)?;
    client.fence_to(1)?;
    let op = routed_operation(&lost.config.storage, 0, 2000)?;
    let batch = CompleteBatch::new(
        &shape,
        BatchId {
            database: lost.config.storage.database,
            operation: op,
        },
        BatchVersion {
            epoch: 1,
            sequence: 0,
        },
        initial_batch(ks, 0),
        lost.config.storage.batch_limits,
    )?;
    if client.append_many(1, std::slice::from_ref(&batch)).is_ok() {
        return Err(error("injected persisted reply was not lost").into());
    }
    drop(client);
    let mut client = lost.client(0, &shape)?;
    let receipt = client
        .append_many(1, std::slice::from_ref(&batch))?
        .remove(0);
    let statistics = client.stats()?;
    if statistics.appended_groups != 1 || statistics.registered_records != 3 {
        return Err(error("lost-response retry appended duplicate authority").into());
    }
    let reference = batch.record_reference(receipt.appended().address(), 2)?;
    if client.read_record(1, &reference)? != put(ks, 0, 2) {
        return Err(error("direct checked record read mismatch").into());
    }
    let mut forged = reference;
    forged.descriptor.checksum.0 ^= 1;
    if client.read_record(1, &forged).is_ok() {
        return Err(error("forged record membership accepted").into());
    }
    client.seal(1)?;
    let recovered = client.recovery_page(1, 0, 128)?;
    if recovered.batches.len() != 1
        || recovered.batches[0].0.id() != batch.id()
        || recovered.next.is_some()
    {
        return Err(error("lost-response recovery inventory mismatch").into());
    }
    client.fence_to(2)?;
    let stale_op = routed_operation(&lost.config.storage, 0, 3000)?;
    let stale = CompleteBatch::new(
        &shape,
        BatchId {
            database: lost.config.storage.database,
            operation: stale_op,
        },
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        vec![put(ks, 99, 1)],
        lost.config.storage.batch_limits,
    )?;
    if client.append_many(1, &[stale]).is_ok() {
        return Err(error("stale epoch write accepted").into());
    }
    client.seal(2)?;
    if !client.recovery_page(2, 0, 128)?.batches.is_empty() {
        return Err(error("stale mutation appeared in new epoch").into());
    }
    drop(client);
    lost.shutdown()?;
    println!(
        "durable_reply_loss=verified exact_retry=verified scalar_membership=verified sealed_recovery=verified stale_epoch=verified"
    );
    fs::write(
        output.join("COMPLETE"),
        "real five-node TCP processes; native differential; coordinator crash/reopen; old retry; stale epoch; partial fence/resume; node crash/restart; mTLS negative cases; durable reply loss; forged reference rejected\n",
    )?;
    println!(
        "local_transport_correctness=verified results={} topology=one_shared_ssd rdma=false aws=false transport={}",
        output.display(),
        transport.label()
    );
    Ok(())
}
