//! Test-only correlation for the real owner queue and real transport receipts.

use super::completion::AppendCompletion;
use crate::distributed::codec::CompleteBatch;
use crate::distributed::storage::{DurableBatch, LocalDbError};
use crate::distributed::types::BatchVersion;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, mpsc};

type Pending = mpsc::Receiver<Result<Vec<DurableBatch>, LocalDbError>>;
type Registered = (Arc<Mutex<ReplyGroup>>, usize);

struct ReplyGroup {
    version: BatchVersion,
    remaining: usize,
    receipts: Vec<Option<DurableBatch>>,
    reply: Option<mpsc::SyncSender<Result<Vec<DurableBatch>, LocalDbError>>>,
}

#[derive(Default)]
struct TestRegistry {
    closed: bool,
    pending: HashMap<u64, Registered>,
}

pub(crate) struct TestCompletion {
    capacity: usize,
    registry: Mutex<TestRegistry>,
}

impl TestCompletion {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            registry: Mutex::new(TestRegistry::default()),
        })
    }

    /// Each admitted batch occupies one bounded registry slot. Each logical
    /// test enqueue owns a one-result channel; a dropped receiver loses only
    /// its observation, not the real queue entry or transport operation.
    pub(crate) fn register(&self, batches: &[CompleteBatch]) -> Result<Pending, LocalDbError> {
        let first = batches
            .first()
            .ok_or_else(|| LocalDbError::new("empty test enqueue"))?;
        let mut registry = self.registry.lock();
        if registry.closed {
            return Err(LocalDbError::new("test worker completion route is closed"));
        }
        let pending = &mut registry.pending;
        if pending
            .len()
            .checked_add(batches.len())
            .is_none_or(|count| count > self.capacity)
        {
            return Err(LocalDbError::new("test owner queue capacity reached"));
        }
        for (index, batch) in batches.iter().enumerate() {
            let sequence = batch.version().sequence;
            if pending.contains_key(&sequence)
                || batches[..index]
                    .iter()
                    .any(|batch| batch.version().sequence == sequence)
            {
                return Err(LocalDbError::new("test enqueue reused a pending version"));
            }
        }
        let (reply, receiver) = mpsc::sync_channel(1);
        let group = Arc::new(Mutex::new(ReplyGroup {
            version: first.version(),
            remaining: batches.len(),
            receipts: vec![None; batches.len()],
            reply: Some(reply),
        }));
        for (index, batch) in batches.iter().enumerate() {
            pending.insert(batch.version().sequence, (Arc::clone(&group), index));
        }
        Ok(receiver)
    }
}

impl AppendCompletion for TestCompletion {
    fn durable(&self, _owner: u32, batches: Vec<(u64, DurableBatch)>) {
        for (sequence, receipt) in batches {
            let registered = {
                let mut registry = self.registry.lock();
                if registry.closed {
                    // Fail-stop already delivered unknown to every observer.
                    // A later real durable receipt cannot turn it into success.
                    return;
                }
                registry
                    .pending
                    .remove(&sequence)
                    .expect("real receipt belongs to an enqueued test generation")
            };
            let (group, index) = registered;
            let mut group = group.lock();
            assert!(group.receipts[index].replace(receipt).is_none());
            group.remaining -= 1;
            if group.remaining == 0 {
                let receipts = std::mem::take(&mut group.receipts)
                    .into_iter()
                    .map(|receipt| receipt.expect("all real receipts arrived"))
                    .collect();
                if let Some(reply) = group.reply.take() {
                    let _ = reply.send(Ok(receipts));
                }
            }
        }
    }

    fn failed(&self, _owner: u32, _sequences: &[u64], error: LocalDbError) {
        let pending = {
            let mut registry = self.registry.lock();
            registry.closed = true;
            std::mem::take(&mut registry.pending)
        };
        for (_, (group, _)) in pending {
            let mut group = group.lock();
            if let Some(reply) = group.reply.take() {
                let _ = reply.send(Err(LocalDbError::unknown(group.version, &error)));
            }
        }
    }
}
