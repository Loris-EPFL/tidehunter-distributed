//! Exercises native WAL partition workers on temporary local storage. This is a
//! correctness example, not a native Db/Sui benchmark or a network deployment.
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tidehunter::CellId;
use tidehunter::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use tidehunter::distributed::completion::{CompletionEvent, CompletionInbox};
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::native_publication::{NativePendingSink, PreparedNativePublication};
use tidehunter::distributed::publication::{PublicationError, PublicationTracker};
use tidehunter::distributed::sequence::LogicalClock;
use tidehunter::distributed::storage::{
    PartitionConfig, PartitionLog, StaticInventory, recover_inventory,
};
use tidehunter::distributed::types::{BatchId, RequestAuthority, ValueRecordReference};
use tidehunter::distributed::worker::{GroupCommitWorker, WorkerConfig};
use tidehunter::index::pending_table::PendingTable;
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
use tidehunter::metrics::Metrics;

// Actual native pending tables, with an example-only owner supplying cell access.
// This is deliberately not a replacement for native LargeTable promotion,
// persistent index levels, caches, checkpoints or a serving Db.
#[derive(Default)]
struct PendingCells(HashMap<(KeySpace, CellId), PendingTable<ValueRecordReference>>);

impl NativePendingSink for PendingCells {
    type Error = Infallible;

    fn with_pending_table(
        &mut self,
        keyspace: KeySpace,
        cell: &CellId,
        apply: impl FnOnce(&mut PendingTable<ValueRecordReference>),
    ) -> Result<(), Self::Error> {
        apply(self.0.entry((keyspace, cell.clone())).or_default());
        Ok(())
    }
}

fn collect_completions(
    inbox: &mut CompletionInbox,
    publication: &mut PublicationTracker,
) -> Result<(), PublicationError> {
    for event in inbox.poll(inbox.retained_batches()) {
        match event {
            CompletionEvent::Unknown { id, .. } => {
                publication.mark_unknown(id)?;
            }
            CompletionEvent::Durable(receipt) => {
                publication.record_durable(receipt)?;
            }
        }
    }
    Ok(())
}

fn verify_pending_references(
    cells: &mut PendingCells,
    logs: &[Arc<PartitionLog>],
    batches: &[(usize, CompleteBatch)],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut seen = std::collections::HashSet::new();
    for ((keyspace, cell), table) in &mut cells.0 {
        let (changes, removed) = table.take_committed();
        assert_eq!(changes.len(), removed);
        for change in changes {
            let reference = change.value;
            let (partition_index, batch) = &batches[reference.batch_version.sequence as usize];
            assert_eq!(reference.batch_id, batch.id());
            assert_eq!(reference.batch_digest, batch.digest());
            let expected = &batch.mutations()[reference.descriptor.operation_index as usize];
            assert_eq!(*keyspace, expected.keyspace());
            // This example's fixed keys are not reduced; bridge unit tests cover
            // reduced/prefixed routing and full-key cache metadata, while the
            // codec regression separately covers hashed-key routing.
            assert_eq!(&change.key, expected.key());
            assert_eq!(change.is_modified, matches!(expected, Mutation::Put { .. }));
            assert_eq!(logs[*partition_index].read_record(&reference)?, *expected);
            assert!(seen.insert(reference.mutation_version()));
            assert_eq!(*cell, CellId::Integer(0));
        }
    }
    let expected_count: usize = batches.iter().map(|(_, b)| b.mutations().len()).sum();
    assert_eq!(seen.len(), expected_count);
    Ok(seen.len())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempdir::TempDir::new("tidehunter-sharded-wal")?;
    let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
    let keyspace = shape.iter_ks().next().unwrap().id();
    let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let inventory = StaticInventory {
        database: [1; 16],
        epoch: 1,
        segment_generation: 1,
        partitions: vec![0, 1, 2, 3, 4],
    };
    let clock = LogicalClock::new_fenced_epoch(1);
    // Fixed bounded campaign: both histories have room for all 80 admissions
    // before any logical version is allocated. A serving authority must reserve
    // all admission resources before allocating each version.
    let mut publication = PublicationTracker::new(inventory.database, 1, 80)?;
    let mut completions = CompletionInbox::new(inventory.database, 1, 80)?;
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
            group_limits: GroupLimits {
                max_batches: 32,
                max_encoded_bytes: 64 * 1024 - 8,
            },
            fragment_bytes: 64 * 1024,
            wal_file_bytes: 128 * 1024,
            max_batches: 32,
            max_reference_metadata_bytes: 1024 * 1024,
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
    let mut workers: Vec<_> = logs
        .iter()
        .map(|log| {
            GroupCommitWorker::start(
                log.clone(),
                &shape,
                WorkerConfig {
                    max_pending_requests: 32,
                    max_pending_bytes: 512 * 1024,
                    max_group_wait: Duration::from_micros(100),
                },
            )
        })
        .collect::<Result<_, _>>()?;
    let mut pending = Vec::new();
    let mut batches = Vec::new();
    for n in 0..16u64 {
        for (index, (log, worker)) in logs.iter().zip(&workers).enumerate() {
            let mut operation = [0; 16];
            operation[..8]
                .copy_from_slice(&(u64::from(log.config().partition) * 1000 + n).to_be_bytes());
            let mut id = BatchId {
                database: inventory.database,
                operation,
            };
            while inventory.partition_for(id)? != log.config().partition {
                id.operation[15] = id.operation[15]
                    .checked_add(1)
                    .expect("five partitions are reachable");
            }
            let version = clock.allocate().unwrap();
            let batch = CompleteBatch::new(
                &shape,
                id,
                version,
                vec![
                    Mutation::Put {
                        keyspace,
                        key: n.to_be_bytes().to_vec().into(),
                        value: vec![version.sequence as u8; 8192].into(),
                    },
                    Mutation::Delete {
                        keyspace,
                        key: (n + 100).to_be_bytes().to_vec().into(),
                    },
                ],
                log.config().batch_limits,
            )?;
            publication.admit(&batch)?;
            let ticket = completions.register(&batch)?;
            publication.mark_in_flight(batch.id())?;
            batches.push((index, batch.clone()));
            pending.push((ticket, worker.try_submit(batch)?));
        }
    }
    // Delay sequence zero's response and report uncertainty while later
    // requests complete in reverse order. Unknown must not become a hole.
    let (first_ticket, first_pending) = pending.remove(0);
    first_ticket.report_unknown();
    for (ticket, pending) in pending.into_iter().rev() {
        ticket.complete(pending.wait()?)?;
    }
    collect_completions(&mut completions, &mut publication)?;
    assert_eq!(publication.durable_prefix().next_sequence(), 0);
    let blocked = publication.publish_ready::<Infallible>(|_| {
        panic!("a missing sequence must block later publication")
    })?;
    assert_eq!(blocked.callbacks_completed, 0);
    first_ticket.complete(first_pending.wait()?)?;
    collect_completions(&mut completions, &mut publication)?;
    assert_eq!(completions.outstanding_batches(), 0);
    assert_eq!(publication.durable_prefix().next_sequence(), 80);

    let mut cells = PendingCells::default();
    let mut expected = Vec::new();
    let published = publication.publish_ready(|durable| {
        let receipt = durable.appended();
        let (index, batch) = &batches[receipt.version().sequence as usize];
        PreparedNativePublication::prepare(&logs[*index], batch, durable)?.publish(&mut cells)?;
        expected.push(receipt);
        Ok::<_, tidehunter::distributed::native_publication::NativePublicationError>(())
    })?;
    assert_eq!(published.callbacks_completed, 80);
    assert_eq!(published.published.next_sequence(), 80);
    let scalar_reads = verify_pending_references(&mut cells, &logs, &batches)?;
    for worker in &mut workers {
        worker.shutdown()?;
    }
    drop(workers);
    let mut groups = 0;
    let mut barriers = 0;
    for log in &logs {
        groups += log.stats().appended_groups;
        barriers += log.stats().persistence_barriers;
        log.seal_epoch(RequestAuthority { epoch: 1 })?;
    }
    drop(logs);
    let recovered: Vec<_> = recover_inventory(&inventory, &entries, metrics)?
        .into_iter()
        .map(Arc::new)
        .collect();
    let mut count = 0;
    let mut replay_order = Vec::new();
    for (index, log) in recovered.iter().enumerate() {
        for start in (0..log.receipt_count()).step_by(256) {
            for receipt in log.receipt_page(start, 256)? {
                assert!(expected.contains(&receipt));
                let batch = log.read_batch(receipt)?;
                assert_eq!(batch.mutations().len(), 2);
                for op in 0..2 {
                    let reference = log.record_reference(receipt, op)?;
                    assert_eq!(log.read_record(&reference)?, batch.mutations()[op as usize]);
                }
                replay_order.push((index, receipt));
                count += 1;
            }
        }
    }
    assert_eq!(count, expected.len());
    // Replay the sealed, complete inventory in logical order through the same
    // native pending bridge, retaining references to the recovered local WALs.
    // This is pending-stage replay, not native persistent-index recovery.
    // Derive replay order solely from recovered metadata; original requests are
    // only the independent correctness oracle used by verification below.
    replay_order.sort_by_key(|(_, receipt)| receipt.version());
    let mut replayed_cells = PendingCells::default();
    for (index, receipt) in replay_order {
        let log = &recovered[index];
        assert_eq!(log.config().partition, receipt.address().partition);
        let batch = log.read_batch(receipt)?;
        let durable = log.persist_through(receipt)?;
        PreparedNativePublication::prepare(log, &batch, durable)?.publish(&mut replayed_cells)?;
    }
    let replay_reads = verify_pending_references(&mut replayed_cells, &recovered, &batches)?;
    let scalar_reads = scalar_reads + count * 2 + replay_reads;
    println!(
        "backend=experimental-native-pending-publication partitions=5 complete_batches={count} groups={groups} explicit_group_persistence_calls={barriers} published_batches=80 native_pending_operations=160 scalar_reads={scalar_reads} unknown_prefix=verified sealed_pending_replay=verified"
    );
    println!(
        "Local shared-SSD correctness exercise; native Db/Sui, TCP and RDMA are not exercised."
    );
    Ok(())
}
