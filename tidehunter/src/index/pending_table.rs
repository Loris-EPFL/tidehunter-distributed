use crate::WalPosition;
use minibytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// PendingTable stores key-value pairs that can be committed at once via Transaction.
///
/// `insert` and `remove` install values associated with a shared `Transaction`.
/// `Transaction::commit` makes those entries eligible for promotion together.
///
/// The same transaction may stage multiple updates to the same or different keys.
/// Transaction can be used across different PendingTables.
///
/// If transaction is dropped without call to Transaction::commit, pending updates are discarded.
/// After commit, `take_committed` returns the corresponding values. Table access
/// still requires exclusive ownership or the existing native cell synchronization;
/// the transaction flag does not make this collection a concurrent map.
///
/// `V` separates publication from the representation of an index reference. Native
/// callers retain `WalPosition` by default; the experimental backend can stage
/// logical references through the same transaction machinery. This table does not
/// compare versions, establish durability or implement a persistent index.
pub struct PendingTable<V = WalPosition> {
    data: HashMap<Bytes, Vec<PendingUpdate<V>>>,
    len: usize,
}

#[derive(Default)]
pub struct Transaction {
    status: TransactionStatus,
    committed: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CommittedChange<V = WalPosition> {
    pub key: Bytes,
    /// True if change was modification, false if change was deletion
    pub is_modified: bool,
    pub value: V,
    /// `(full_key, value)` for LRU cache updates. None for deletions or if LRU is not configured.
    pub lru_update: Option<(Bytes, Bytes)>,
}

#[derive(Clone)]
struct PendingUpdateInner<V> {
    update: V,
    lru_update: Option<(Bytes, Bytes)>,
}

#[derive(Default, Clone)]
struct TransactionStatus {
    status: Arc<AtomicUsize>,
}

const TRANSACTION_STATUS_PENDING: usize = 0;
const TRANSACTION_STATUS_COMMITTED: usize = 1;
const TRANSACTION_STATUS_REVERTED: usize = 2;

#[derive(Clone)]
struct PendingUpdate<V> {
    transaction_status: TransactionStatus,
    is_modified: bool,
    inner: PendingUpdateInner<V>,
}

// Deriving Default would unnecessarily require V: Default. A pending table has
// no values until the caller supplies one, including logical references that
// deliberately have no sentinel/default identity.
impl<V> Default for PendingTable<V> {
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            len: 0,
        }
    }
}

impl<V: Copy> PendingTable<V> {
    pub fn insert(
        &mut self,
        key: Bytes,
        position: V,
        lru_update: Option<(Bytes, Bytes)>,
        transaction: &Transaction,
    ) {
        self.insert_pending(key, true, position, lru_update, transaction);
    }

    pub fn remove(&mut self, key: Bytes, position: V, transaction: &Transaction) {
        self.insert_pending(key, false, position, None, transaction);
    }

    fn insert_pending(
        &mut self,
        key: Bytes,
        is_modified: bool,
        position: V,
        lru_update: Option<(Bytes, Bytes)>,
        transaction: &Transaction,
    ) {
        self.len += 1;
        let update = PendingUpdate::new(
            transaction.status.clone(),
            is_modified,
            position,
            lru_update,
        );
        self.data.entry(key).or_default().push(update);
    }

    pub fn take_committed_for(&mut self, key: &Bytes) -> (Vec<CommittedChange<V>>, usize) {
        let Some(updates) = self.data.get_mut(key) else {
            return (vec![], 0);
        };
        let mut committed = Vec::with_capacity(updates.len());
        let removed = Self::do_take_committed(key, updates, &mut committed);
        self.len = self.len.checked_sub(removed).expect("len overflow");
        if updates.is_empty() {
            self.data.remove(key);
        }
        (committed, removed)
    }

    pub fn take_committed(&mut self) -> (Vec<CommittedChange<V>>, usize) {
        let mut committed = Vec::with_capacity(self.len);
        let mut total_removed = 0;
        self.data.retain(|key, updates| {
            let removed = Self::do_take_committed(key, updates, &mut committed);
            self.len = self.len.checked_sub(removed).expect("len overflow");
            total_removed += removed;
            !updates.is_empty()
        });
        (committed, total_removed)
    }

    /// Take committed updates for a given key, add them to given vector
    /// and return number of entries removed.
    /// Number of removed entries and number of added updates may differ because
    /// some transactions might be reverted.
    fn do_take_committed(
        key: &Bytes,
        updates: &mut Vec<PendingUpdate<V>>,
        committed: &mut Vec<CommittedChange<V>>,
    ) -> usize {
        let mut removed = 0;
        updates.retain(|update| {
            let status = update.transaction_status.status.load(Ordering::SeqCst);
            if status == TRANSACTION_STATUS_PENDING {
                return true;
            }
            if status == TRANSACTION_STATUS_COMMITTED {
                let committed_change = CommittedChange {
                    key: key.clone(),
                    is_modified: update.is_modified,
                    value: update.inner.update,
                    lru_update: update.inner.lru_update.clone(),
                };
                committed.push(committed_change);
            } else {
                assert_eq!(
                    status, TRANSACTION_STATUS_REVERTED,
                    "Expected reverted transaction"
                );
            }
            removed += 1;
            false
        });
        removed
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl Transaction {
    pub fn commit(mut self) {
        self.status
            .status
            .store(TRANSACTION_STATUS_COMMITTED, Ordering::SeqCst);
        self.committed = true;
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            self.status
                .status
                .store(TRANSACTION_STATUS_REVERTED, Ordering::SeqCst);
        }
    }
}

impl<V> PendingUpdate<V> {
    fn new(
        transaction_status: TransactionStatus,
        is_modified: bool,
        position: V,
        lru_update: Option<(Bytes, Bytes)>,
    ) -> Self {
        let inner = PendingUpdateInner {
            update: position,
            lru_update,
        };
        Self {
            transaction_status,
            is_modified,
            inner,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use rand::prelude::SliceRandom;
    use rand::{Rng, thread_rng};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn typed_references_share_native_transaction_without_default_or_order_bounds() {
        // Neither Default nor Ord is needed: version comparison belongs to the
        // receiving index, not to this atomic-publication staging mechanism.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        struct Reference {
            sequence: u64,
            operation: u32,
        }

        let mut logical = PendingTable::<Reference>::default();
        let mut native = PendingTable::<WalPosition>::default();
        let tx = Transaction::default();
        let first = Reference {
            sequence: 9,
            operation: 0,
        };
        let second = Reference {
            sequence: 9,
            operation: 1,
        };
        let third = Reference {
            sequence: 9,
            operation: 2,
        };
        logical.insert(b(1), first, Some((b(1), b(2))), &tx);
        logical.remove(b(1), second, &tx);
        logical.insert(b(1), third, None, &tx);
        native.insert(b(2), WalPosition::test_value(7), None, &tx);
        assert_eq!(logical.take_committed_for(&b(1)), (vec![], 0));
        assert_eq!(native.take_committed(), (vec![], 0));

        tx.commit();
        let (changes, removed) = logical.take_committed_for(&b(1));
        assert_eq!(removed, 3);
        assert_eq!(logical.len(), 0);
        assert_eq!(
            changes.iter().map(|c| c.value).collect::<Vec<_>>(),
            vec![first, second, third]
        );
        assert_eq!(
            changes.iter().map(|c| c.is_modified).collect::<Vec<_>>(),
            vec![true, false, true]
        );
        assert_eq!(changes[0].lru_update, Some((b(1), b(2))));
        assert_eq!(changes[1].lru_update, None);
        assert_eq!(
            native.take_committed().0[0].value,
            WalPosition::test_value(7)
        );

        // An abandoned transaction drops every reference, including a duplicate
        // key deletion, while keeping the same count semantics as native.
        let abandoned = Transaction::default();
        logical.insert(b(1), first, None, &abandoned);
        logical.remove(b(1), second, &abandoned);
        native.remove(b(2), WalPosition::test_value(8), &abandoned);
        drop(abandoned);
        assert_eq!(logical.take_committed(), (vec![], 2));
        assert_eq!(native.take_committed(), (vec![], 1));
        assert_eq!(logical.len(), 0);
    }

    #[test]
    fn pending_table_tests() {
        let mut table = PendingTable::default();
        let tx1 = Transaction::default();
        let tx2 = Transaction::default();

        table.insert(b(1), WalPosition::test_value(0), None, &tx1);
        table.insert(b(1), WalPosition::test_value(1), None, &tx2);
        table.insert(b(2), WalPosition::test_value(2), None, &tx2);
        let (committed, removed) = table.take_committed();
        assert_eq!(committed.len(), 0);
        assert_eq!(removed, 0);
        assert_eq!(table.len(), 3);
        drop(tx1); // drop tx1 without committing it
        // Nothing is committed but table len is reduced by 1 since tx1 update is discarded now
        let (committed, removed) = table.take_committed();
        assert_eq!(committed.len(), 0);
        assert_eq!(removed, 1);
        assert_eq!(table.len(), 2);

        tx2.commit(); // Commit tx2

        let (mut committed, removed) = table.take_committed();
        // 2 values are committed and table is now empty(no more pending update).
        assert_eq!(committed.len(), 2);
        assert_eq!(removed, 2);
        committed.sort_by_key(|c| c.key.clone());
        assert_eq!(table.len(), 0);
        assert_eq!(
            &committed[0],
            &CommittedChange {
                key: b(1),
                is_modified: true,
                value: WalPosition::test_value(1),
                lru_update: None,
            }
        );

        assert_eq!(
            &committed[1],
            &CommittedChange {
                key: b(2),
                is_modified: true,
                value: WalPosition::test_value(2),
                lru_update: None,
            }
        );

        // test take_committed_for
        let tx3 = Transaction::default();
        table.insert(b(3), WalPosition::test_value(3), None, &tx3);
        table.insert(b(4), WalPosition::test_value(4), None, &tx3);
        tx3.commit();
        let (committed, removed) = table.take_committed_for(&b(3));
        assert_eq!(committed.len(), 1);
        assert_eq!(removed, 1);
        assert_eq!(table.data.len(), 1);
        assert_eq!(table.len(), 1);
        assert_eq!(
            &committed[0],
            &CommittedChange {
                key: b(3),
                is_modified: true,
                value: WalPosition::test_value(3),
                lru_update: None,
            }
        );
        let (committed, removed) = table.take_committed();
        assert_eq!(committed.len(), 1);
        assert_eq!(removed, 1);
        assert_eq!(table.data.len(), 0);
        assert_eq!(table.len(), 0);
        assert_eq!(
            &committed[0],
            &CommittedChange {
                key: b(4),
                is_modified: true,
                value: WalPosition::test_value(4),
                lru_update: None,
            }
        );
    }

    #[test]
    fn pending_table_concurrent_test() {
        let num_tables = 32;
        let num_mutators = 16;
        let iterations = 5000;
        let tables: Vec<_> = (0..num_tables)
            .map(|_| Arc::new(Mutex::new(PendingTable::<WalPosition>::default())))
            .collect();
        let (senders, receivers): (Vec<_>, Vec<_>) =
            tables.iter().map(|_| mpsc::channel::<Vec<u64>>()).unzip();
        let threads: Vec<_> = tables
            .clone()
            .into_iter()
            .zip(receivers)
            .enumerate()
            .map(|(id, (table, receiver))| {
                thread::spawn(move || {
                    let mut committed = HashSet::new();
                    while let Ok(expected_to_commit) = receiver.recv() {
                        let (changes, _removed) = table.lock().take_committed();
                        for c in changes {
                            println!("[{id}] committed {}", c.value.offset());
                            committed.insert(c.value.offset());
                        }
                        for num in expected_to_commit.into_iter() {
                            if !committed.remove(&num) {
                                println!("[{id}] Expected {num} to be committed but it is not");
                                std::process::exit(1);
                            };
                        }
                    }
                    if !committed.is_empty() {
                        panic!(
                            "[{id}] something is committed but should not be: {:?}",
                            committed
                        );
                    }
                })
            })
            .collect();
        let max_transactions = 32;
        let senders = Arc::new(senders);
        let tables = Arc::new(tables);
        let mut mutator_threads = vec![];
        for _ in 0..num_mutators {
            let senders = Arc::clone(&senders);
            let tables = Arc::clone(&tables);
            let thread = thread::spawn(move || {
                let mut transactions: Vec<(Vec<u64>, Transaction)> = vec![];
                let mut rng = thread_rng();
                for _ in 0..iterations {
                    let (transaction_changes, transaction) = if rng.gen_bool(
                        (max_transactions - transactions.len()) as f64 / (max_transactions as f64),
                    ) {
                        println!("new txn(total {})", transactions.len());
                        transactions.push(Default::default());
                        transactions.last_mut().unwrap()
                    } else {
                        println!("existing txn");
                        transactions.choose_mut(&mut rng).unwrap()
                    };
                    let num: u64 = rng.r#gen();
                    transaction_changes.push(num);
                    let table = tables.get(num as usize % tables.len()).unwrap();
                    table.lock().insert(
                        Bytes::from(num.to_be_bytes()),
                        WalPosition::test_value(num),
                        None,
                        transaction,
                    );

                    let transaction_to_commit = rng.gen_range(0..(transactions.len() * 4));
                    if transaction_to_commit >= transactions.len() {
                        continue;
                    }
                    println!("Committing random txn");
                    let (nums_to_commit, transaction_to_commit) =
                        transactions.remove(transaction_to_commit);
                    let mut tables_to_commit: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
                    for num_to_commit in nums_to_commit.iter() {
                        tables_to_commit
                            .entry((*num_to_commit as usize) % tables.len())
                            .or_default()
                            .push(*num_to_commit);
                    }
                    println!("Committing");
                    transaction_to_commit.commit();
                    for (table_id, message) in tables_to_commit {
                        println!("Sending to {table_id}: {message:?}");
                        senders[table_id].send(message).unwrap();
                    }
                }
            });
            mutator_threads.push(thread);
        }
        for thread in mutator_threads {
            thread.join().unwrap();
        }
        drop(senders);
        println!("Waiting for threads");
        for thread in threads {
            thread.join().unwrap();
        }
    }

    fn b(v: u64) -> Bytes {
        v.to_be_bytes().to_vec().into()
    }
}
