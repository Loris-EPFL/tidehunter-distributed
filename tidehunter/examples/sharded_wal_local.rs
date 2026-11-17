//! Exercises the public local database API on temporary shared-SSD storage.
//! This is a correctness example, not a native Db/Sui or network benchmark.
use std::sync::Arc;
use std::time::Duration;
use tidehunter::distributed::codec::{BatchLimits, Mutation};
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_db::{LocalDatabase, LocalDatabaseConfig};
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::storage::StaticInventory;
use tidehunter::distributed::types::BatchId;
use tidehunter::distributed::worker::WorkerConfig;
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
use tidehunter::metrics::Metrics;

fn verify(
    database: &mut LocalDatabase,
    keyspace: KeySpace,
) -> Result<(), Box<dyn std::error::Error>> {
    for n in 0..16u64 {
        let expected = vec![(n * 5 + 4) as u8; 8192];
        assert_eq!(
            database.get(keyspace, &n.to_be_bytes())?.as_deref(),
            Some(expected.as_slice())
        );
        assert!(database.get(keyspace, &(n + 100).to_be_bytes())?.is_none());
    }
    assert_eq!(database.scan(keyspace)?.len(), 16);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempdir::TempDir::new("tidehunter-sharded-wal")?;
    let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
    let keyspace = shape
        .iter_ks()
        .next()
        .ok_or("example shape has no keyspace")?
        .id();
    let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let inventory = StaticInventory {
        database: [1; 16],
        epoch: 1,
        segment_generation: 1,
        partitions: vec![0, 1, 2, 3, 4],
    };
    let config = LocalDatabaseConfig {
        storage: LocalInventoryConfig {
            database: inventory.database,
            partitions: inventory.partitions.clone(),
            batch_limits: BatchLimits {
                max_operations: 100,
                max_encoded_bytes: 60 * 1024,
            },
            group_limits: GroupLimits {
                max_batches: 32,
                max_encoded_bytes: 64 * 1024 - 8,
            },
            fragment_bytes: 64 * 1024,
            wal_file_bytes: 128 * 1024,
            max_batches_per_partition: 32,
            max_reference_metadata_bytes: 1024 * 1024,
            max_epochs: 4,
        },
        worker: WorkerConfig {
            max_pending_requests: 32,
            max_pending_bytes: 512 * 1024,
            max_group_wait: Duration::from_micros(100),
        },
        max_admitted_batches: 80,
        max_index_bytes: 8 * 1024 * 1024,
    };
    let mut database = LocalDatabase::open(
        temporary.path(),
        &shape,
        config.clone(),
        Arc::clone(&metrics),
    )?;
    let mut admitted = Vec::with_capacity(80);
    for n in 0..16u64 {
        for &partition in &inventory.partitions {
            let mut operation = [0; 16];
            operation[..8].copy_from_slice(&(u64::from(partition) * 1000 + n).to_be_bytes());
            let mut id = BatchId {
                database: inventory.database,
                operation,
            };
            while inventory.partition_for(id)? != partition {
                id.operation[15] = id.operation[15]
                    .checked_add(1)
                    .ok_or("owner routing search exhausted")?;
            }
            let mutations = vec![
                Mutation::Put {
                    keyspace,
                    key: n.to_be_bytes().to_vec().into(),
                    value: vec![(n * 5 + u64::from(partition)) as u8; 8192].into(),
                },
                Mutation::Delete {
                    keyspace,
                    key: (n + 100).to_be_bytes().to_vec().into(),
                },
            ];
            let version = database.submit(id.operation, mutations.clone())?;
            admitted.push((id.operation, mutations, version));
        }
    }
    database.drain()?;
    assert_eq!(database.stats().published_batches, 80);
    verify(&mut database, keyspace)?;
    let checkpoint = database.checkpoint()?;
    for n in 0..16u64 {
        let expected = vec![(n * 5 + 4) as u8; 8192];
        assert_eq!(
            database
                .get_at(keyspace, &n.to_be_bytes(), &checkpoint)?
                .as_deref(),
            Some(expected.as_slice())
        );
    }
    let storage = database.storage_stats();
    drop(checkpoint);
    database.shutdown()?;
    drop(database);
    let mut recovered = LocalDatabase::open(temporary.path(), &shape, config, metrics)?;
    verify(&mut recovered, keyspace)?;
    // Retried identities retain their original versions and must not overwrite
    // newer published values after the public database recovery path.
    for (operation, mutations, version) in admitted {
        assert_eq!(recovered.write_batch(operation, mutations)?, version);
    }
    verify(&mut recovered, keyspace)?;
    println!(
        "backend={} partitions=5 complete_batches=80 groups={} explicit_group_persistence_calls={} published_batches=80 mutations=160 checkpoint=verified sealed_recovery=verified retries=verified",
        recovered.backend_id(),
        storage.appended_groups,
        storage.persistence_barriers,
    );
    recovered.shutdown()?;
    println!(
        "Local shared-SSD correctness exercise; native Db/Sui, TCP and RDMA are not exercised."
    );
    Ok(())
}
