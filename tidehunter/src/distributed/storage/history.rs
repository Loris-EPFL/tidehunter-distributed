//! Bounded in-memory window of recent owner receipts.
//!
//! WAL replay is the only history (Tidehunter §§3.3–3.4: snapshot position plus
//! replay of the suffix). This window serves retry and conflict lookups of the
//! most recent batches; it is never persisted and holds no values.
use super::*;
use crate::distributed::reference_directory::RegisteredBatch;

pub const WAL_REPLAY_WINDOW_BATCHES: usize = 4096;

struct MemoryEntry {
    receipt: AppendedBatch,
    group_end: u64,
}

// Lock order: state, authority, memory history. No I/O under the memory-history lock.
struct MemoryHistory {
    entries: std::collections::VecDeque<MemoryEntry>,
    ids: std::collections::HashMap<u128, usize>,
    versions: std::collections::HashMap<u128, usize>,
    base: usize,
}

impl MemoryHistory {
    fn new() -> Self {
        Self {
            entries: std::collections::VecDeque::with_capacity(WAL_REPLAY_WINDOW_BATCHES),
            ids: std::collections::HashMap::with_capacity(WAL_REPLAY_WINDOW_BATCHES),
            versions: std::collections::HashMap::with_capacity(WAL_REPLAY_WINDOW_BATCHES),
            base: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct HistoryRoot {
    pub(crate) count: usize,
    pub(crate) end: u64,
    pub(crate) file_count: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct HistoryRecord {
    receipt: AppendedBatch,
    pub(crate) group_end: u64,
}
impl HistoryRecord {
    pub(crate) fn receipt(&self) -> AppendedBatch {
        self.receipt
    }
}

/// Frozen small root plus the shared window.
#[derive(Clone)]
pub(crate) struct HistoryReader {
    root: HistoryRoot,
    memory: Arc<parking_lot::RwLock<MemoryHistory>>,
}

/// Sole append capability, serialized between the state owner and lifecycle owner.
pub(crate) struct History {
    reader: HistoryReader,
}

impl History {
    pub(crate) fn new() -> Self {
        Self {
            reader: HistoryReader {
                root: HistoryRoot {
                    count: 0,
                    end: 0,
                    file_count: 1,
                },
                memory: Arc::new(parking_lot::RwLock::new(MemoryHistory::new())),
            },
        }
    }

    pub(crate) fn reader(&self) -> HistoryReader {
        self.reader.clone()
    }

    pub(crate) fn append(
        &mut self,
        batches: &[Arc<RegisteredBatch>],
        file_count: u64,
    ) -> Result<(), StorageError> {
        let mut memory = self.reader.memory.write();
        for batch in batches {
            let receipt = batch.receipt();
            if memory
                .ids
                .contains_key(&u128::from_be_bytes(receipt.id().operation))
                || memory
                    .versions
                    .contains_key(&u128::from(receipt.version().sequence))
            {
                return Err(StorageError::Invalid("duplicate history radix authority"));
            }
        }
        let mut root = self.reader.root;
        for batch in batches {
            let receipt = batch.receipt();
            if memory.entries.len() == WAL_REPLAY_WINDOW_BATCHES
                && let Some(oldest) = memory.entries.pop_front()
            {
                memory
                    .ids
                    .remove(&u128::from_be_bytes(oldest.receipt.id().operation));
                memory
                    .versions
                    .remove(&u128::from(oldest.receipt.version().sequence));
                memory.base += 1;
            }
            memory
                .ids
                .insert(u128::from_be_bytes(receipt.id().operation), root.count);
            memory
                .versions
                .insert(u128::from(receipt.version().sequence), root.count);
            memory.entries.push_back(MemoryEntry {
                receipt,
                group_end: batch.group_end(),
            });
            root.count += 1;
            root.end = batch.group_end();
        }
        root.file_count = root.file_count.max(file_count);
        self.reader.root = root;
        Ok(())
    }
}

impl HistoryReader {
    pub(crate) fn base(&self) -> usize {
        self.memory.read().base
    }

    pub(crate) fn root(&self) -> &HistoryRoot {
        &self.root
    }

    pub(crate) fn at(&self, ordinal: usize) -> Result<HistoryRecord, StorageError> {
        let memory = self.memory.read();
        let index = ordinal
            .checked_sub(memory.base)
            .ok_or(StorageError::Invalid(
                "receipt ordinal outside the replay window",
            ))?;
        let entry = memory
            .entries
            .get(index)
            .filter(|_| ordinal < self.root.count)
            .ok_or(StorageError::Invalid("missing receipt ordinal authority"))?;
        Ok(HistoryRecord {
            receipt: entry.receipt,
            group_end: entry.group_end,
        })
    }

    pub(crate) fn lookup(&self, id: BatchId) -> Result<Option<HistoryRecord>, StorageError> {
        self.lookup_key(u128::from_be_bytes(id.operation), false, |receipt| {
            receipt.id() == id
        })
    }

    pub(crate) fn lookup_version(
        &self,
        version: BatchVersion,
    ) -> Result<Option<HistoryRecord>, StorageError> {
        self.lookup_key(u128::from(version.sequence), true, |receipt| {
            receipt.version() == version
        })
    }

    fn lookup_key(
        &self,
        key: u128,
        version: bool,
        matches: impl Fn(AppendedBatch) -> bool,
    ) -> Result<Option<HistoryRecord>, StorageError> {
        let memory = self.memory.read();
        let index = if version {
            &memory.versions
        } else {
            &memory.ids
        };
        let Some(&ordinal) = index.get(&key) else {
            return Ok(None);
        };
        if ordinal >= self.root.count {
            return Ok(None);
        }
        let Some(entry) = ordinal
            .checked_sub(memory.base)
            .and_then(|index| memory.entries.get(index))
        else {
            return Ok(None);
        };
        if !matches(entry.receipt) {
            return Err(StorageError::Invalid(
                "receipt lookup differs from rooted key",
            ));
        }
        Ok(Some(HistoryRecord {
            receipt: entry.receipt,
            group_end: entry.group_end,
        }))
    }
}
