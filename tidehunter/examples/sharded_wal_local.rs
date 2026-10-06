//! Exercises two native WAL partitions on local temporary storage. This is a
//! correctness example, not a native Db/Sui benchmark or a network deployment.
use std::sync::Arc;
use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use tidehunter::distributed::sequence::LogicalClock;
use tidehunter::distributed::storage::{
    PartitionConfig, PartitionLog, StaticInventory, recover_inventory,
};
use tidehunter::distributed::types::{BatchId, RequestAuthority};
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::metrics::Metrics;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempdir::TempDir::new("tidehunter-sharded-wal")?;
    let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
    let keyspace = shape.iter_ks().next().unwrap().id();
    let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let inventory = StaticInventory {
        database: [1; 16],
        epoch: 1,
        segment_generation: 1,
        partitions: vec![0, 1],
    };
    let clock = LogicalClock::new_fenced_epoch(1);
    let configs: Vec<_> = inventory
        .partitions
        .iter()
        .map(|partition| PartitionConfig {
            inventory: inventory.clone(),
            partition: *partition,
            batch_limits: BatchLimits {
                max_operations: 100,
                max_encoded_bytes: 60 * 1024,
            },
            fragment_bytes: 64 * 1024,
            wal_file_bytes: 128 * 1024,
            max_batches: 32,
        })
        .collect();
    let entries: Vec<_> = configs
        .iter()
        .map(|config| {
            (
                temporary
                    .path()
                    .join(format!("partition-{}", config.partition)),
                config.clone(),
            )
        })
        .collect();
    // Register every member before admitting the first database batch.
    let logs: Vec<_> = entries
        .iter()
        .map(|(path, config)| {
            PartitionLog::create(path, config.clone(), &shape, metrics.clone()).map(Arc::new)
        })
        .collect::<Result<_, _>>()?;
    let mut expected = Vec::new();
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let mut workers = Vec::new();
        for log in &logs {
            let shape = &shape;
            let clock = &clock;
            workers.push(scope.spawn(move || {
                let mut written = Vec::new();
                for n in 0..16u64 {
                    let mut operation = [0; 16];
                    operation[..8].copy_from_slice(
                        &(u64::from(log.config().partition) * 1000 + n).to_be_bytes(),
                    );
                    let mut id = BatchId {
                        database: log.config().inventory.database,
                        operation,
                    };
                    while log.config().inventory.partition_for(id)? != log.config().partition {
                        id.operation[15] = id.operation[15]
                            .checked_add(1)
                            .expect("two partitions are reachable");
                    }
                    let batch = CompleteBatch::new(
                        shape,
                        id,
                        clock.allocate().unwrap(),
                        vec![
                            Mutation::Put {
                                keyspace,
                                key: n.to_be_bytes().to_vec().into(),
                                value: vec![n as u8; 8192].into(),
                            },
                            Mutation::Delete {
                                keyspace,
                                key: (n + 100).to_be_bytes().to_vec().into(),
                            },
                        ],
                        log.config().batch_limits,
                    )?;
                    let appended =
                        log.append_complete_batch(RequestAuthority { epoch: 1 }, &batch)?;
                    log.persist_through(appended)?;
                    assert_eq!(log.read_batch(appended)?.digest(), batch.digest());
                    written.push(appended);
                }
                log.seal_epoch(RequestAuthority { epoch: 1 })?;
                Ok::<_, tidehunter::distributed::storage::StorageError>(written)
            }));
        }
        for worker in workers {
            expected.extend(worker.join().expect("local worker panicked")?);
        }
        Ok(())
    })?;
    drop(logs);
    let recovered = recover_inventory(&inventory, &entries, metrics)?;
    let mut count = 0;
    for log in recovered {
        for receipt in log.recovered_batches() {
            assert!(expected.contains(&receipt));
            assert_eq!(log.read_batch(receipt)?.mutations().len(), 2);
            count += 1;
        }
    }
    assert_eq!(count, expected.len());
    println!(
        "backend=experimental-native-partition-log partitions=2 complete_batches={count} recovery=verified"
    );
    println!(
        "Local shared-SSD correctness exercise; native Db/Sui, TCP and RDMA are not exercised."
    );
    Ok(())
}
