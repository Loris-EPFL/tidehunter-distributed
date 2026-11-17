//! Per-cell recovery inputs of both history modes (spec §7.1, §8.1 steps
//! 4-6): the owner suffix cursors that `cell_recovery::replay_mixed_cuts`
//! merges, and the through-`F` retry authority it checks every suffix batch
//! against.
//!
//! | Mode | Suffix cursor | Retry authority through `F` |
//! |---|---|---|
//! | WAL replay | physical scan from the wrapper's fragment boundary, skipping complete batches through its through-`F` last offset, required to reach the owner-proved durable end | the wrapper's bounded recent window |
//! | Receipt history | receipt ordinals from the wrapper's through-`F` proof prefix to the fenced owner's retained end | the durable receipt history at the wrapper's root |
//!
//! A stream the wrapper does not list belongs to an owner epoch newer than
//! the wrapper and is read from its start. Every listed stream must still be
//! retained by the fenced owners. The cursors use the same owner pages and
//! corruption rules as exact replay (`fill_replay_page`): an owner that cannot
//! reach a proved durable end fails recovery, it is not a tolerated tail.
//!
//! Recovery is one thread with no serving handle: streams share their
//! owner's control client through a `RefCell`, borrowed only for one page.

use super::super::cell_recovery::{ReplayStream, StreamCursor};
use super::super::checkpoint_contract::{CellCheckpointStream, CheckpointImageV4};
use super::super::*;
use crate::distributed::native_index::PrefixCut;
use std::cell::RefCell;
use std::collections::BTreeSet;

/// Complete batches buffered per stream: exact replay's page bound. WAL
/// pages can return a whole owner group, so that mode is at least the group
/// batch limit.
pub(in crate::distributed::remote_db) fn suffix_page_limit(
    config: &RemoteDatabaseConfig,
) -> Result<usize, LocalDbError> {
    let limit = config.recovery_page_limit()?;
    Ok(
        if config.history_persistence == HistoryPersistence::WalReplay {
            limit.max(config.storage.group_limits.max_batches as usize)
        } else {
            limit
        },
    )
}

/// One retained owner stream positioned at its through-`F` cursor.
pub(in crate::distributed::remote_db) struct SuffixStream<'a, 'c> {
    client: &'a RefCell<&'c mut Client>,
    partition: u32,
    epoch: u64,
    authority_epoch: u64,
    /// The next page cursor; `None` once the stream is exhausted.
    next: Option<usize>,
    /// The cursor is not complete, so the stream is read.
    scanned: bool,
    page: ReplayPage,
}

impl SuffixStream<'_, '_> {
    /// `(partition, owner epoch)` of the stream.
    pub(in crate::distributed::remote_db) fn key(&self) -> (u32, u64) {
        (self.partition, self.epoch)
    }

    /// False for a stream whose cursor is complete: it is never read.
    pub(in crate::distributed::remote_db) fn scanned(&self) -> bool {
        self.scanned
    }

    /// WAL replay: the end of the last complete batch any page returned,
    /// skipped through-`F` batches included (0 if none). The next root's
    /// proved durable end can grow to it; the skip offset stays through `F`.
    pub(in crate::distributed::remote_db) fn replayed_end(&self) -> u64 {
        self.page.replayed_end
    }
}

impl ReplayStream for SuffixStream<'_, '_> {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn fill(
        &mut self,
        limit: usize,
        page: &mut VecDeque<(CompleteBatch, DurableBatch)>,
    ) -> Result<bool, LocalDbError> {
        // Each owner page strictly advances the cursor (`fill_replay_page`
        // rejects `next <= cursor`), so this loop ends at the stream end. A
        // WAL page can filter to nothing below the through-F skip offset.
        while let Some(cursor) = self.next {
            let mut client = self
                .client
                .try_borrow_mut()
                .map_err(|_| LocalDbError::new("recovery owner client is busy"))?;
            fill_replay_page(
                &mut client,
                &mut self.page,
                self.epoch,
                self.authority_epoch,
                cursor,
                limit,
            )?;
            drop(client);
            self.next = self.page.next;
            if !self.page.batches.is_empty() {
                page.extend(self.page.batches.drain(..));
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Open one cursor per retained `(partition, owner epoch)` stream of the
/// fenced owners (`histories[node]`, epochs below `authority_epoch`) from a
/// checked format-4 `wrapper` (spec §8.1 step 4), or from the start of every
/// stream when no per-cell root is adopted yet. `clients[node]` is the
/// fenced control client of `config.endpoints[node]`.
pub(in crate::distributed::remote_db) fn open_suffix_streams<'a, 'c>(
    clients: &'a [RefCell<&'c mut Client>],
    config: &RemoteDatabaseConfig,
    histories: &[Vec<u64>],
    authority_epoch: u64,
    wrapper: Option<&CheckpointImageV4>,
) -> Result<Vec<SuffixStream<'a, 'c>>, LocalDbError> {
    if clients.len() != config.endpoints.len() || histories.len() != config.endpoints.len() {
        return Err(LocalDbError::new("recovery owner inventory differs"));
    }
    let persistence = config.history_persistence;
    let mut listed = BTreeMap::new();
    for stream in wrapper.iter().flat_map(|wrapper| wrapper.streams.iter()) {
        listed.insert((stream.partition, stream.epoch), stream);
    }
    // Epochs above it can be absent from the wrapper; 0 without a root.
    let wrapper_epoch = wrapper.map_or(0, |wrapper| wrapper.authority_epoch);
    let mut retained = BTreeSet::new();
    let mut streams = Vec::new();
    for (node, epochs) in histories.iter().enumerate() {
        let partition = config.endpoints[node].partition;
        for &epoch in epochs {
            if epoch >= authority_epoch || !retained.insert((partition, epoch)) {
                return Err(LocalDbError::new("recovery retained stream inventory"));
            }
            let cursor = match listed.get(&(partition, epoch)) {
                Some(stream) => StreamCursor::from_wrapper(stream, persistence)?,
                // Every stream the wrapper's authority knew is listed; only
                // newer owner epochs can be absent, and they hold no batch at
                // or below `F`.
                None if epoch > wrapper_epoch => {
                    if persistence == HistoryPersistence::WalReplay {
                        StreamCursor::Wal {
                            from: 0,
                            skip_through: None,
                            durable_end: 0,
                        }
                    } else {
                        StreamCursor::Receipts { from: 0 }
                    }
                }
                None => {
                    return Err(LocalDbError::new(
                        "checkpoint wrapper misses a retained stream",
                    ));
                }
            };
            streams.push(open_stream(
                &clients[node],
                partition,
                epoch,
                authority_epoch,
                cursor,
                listed.get(&(partition, epoch)).copied(),
            )?);
        }
    }
    if listed.keys().any(|key| !retained.contains(key)) {
        return Err(LocalDbError::new(
            "checkpoint wrapper stream outside fenced inventory",
        ));
    }
    Ok(streams)
}

fn open_stream<'a, 'c>(
    client: &'a RefCell<&'c mut Client>,
    partition: u32,
    epoch: u64,
    authority_epoch: u64,
    cursor: StreamCursor,
    listed: Option<&CellCheckpointStream>,
) -> Result<SuffixStream<'a, 'c>, LocalDbError> {
    let mut stream = SuffixStream {
        client,
        partition,
        epoch,
        authority_epoch,
        next: None,
        scanned: cursor != StreamCursor::Complete,
        page: ReplayPage {
            batches: VecDeque::new(),
            next: None,
            last: None,
            position: None,
            replayed_end: 0,
            replay_last: None,
        },
    };
    match cursor {
        // The sealed history holds no batch above `F`.
        StreamCursor::Complete => {
            if let Some(prefix) = listed.and_then(|stream| stream.receipt_prefix.as_ref()) {
                // Receipt mode: the fenced owner must still retain exactly
                // the proved prefix, or `complete` hides suffix batches.
                let proof = fenced_proof(client, authority_epoch, epoch, prefix)?;
                if proof.retained != proof.prefix {
                    return Err(LocalDbError::new(
                        "complete checkpoint stream retains batches above F",
                    ));
                }
            }
        }
        StreamCursor::Wal {
            from,
            skip_through,
            durable_end,
        } => {
            stream.page.position = Some(StreamPosition {
                partition,
                epoch,
                replay_from: from,
                last_offset: skip_through,
                durable_end,
                complete: false,
            });
            stream.next = Some(usize::try_from(from).map_err(LocalDbError::component)?);
        }
        StreamCursor::Receipts { from } => {
            let retained = match listed.and_then(|stream| stream.receipt_prefix.as_ref()) {
                Some(prefix) => fenced_proof(client, authority_epoch, epoch, prefix)?.retained,
                None => {
                    client
                        .try_borrow_mut()
                        .map_err(|_| LocalDbError::new("recovery owner client is busy"))?
                        .checkpoint(authority_epoch, epoch, 0)
                        .map_err(LocalDbError::component)?
                        .retained
                }
            };
            // Exact replay's rule: a stream whose retained end is the proved
            // prefix has no suffix to read.
            stream.next = (from != retained).then_some(from);
        }
    }
    Ok(stream)
}

/// The fenced owner proves the wrapper's through-`F` receipt prefix again.
fn fenced_proof(
    client: &RefCell<&mut Client>,
    authority_epoch: u64,
    epoch: u64,
    prefix: &CheckpointStream,
) -> Result<CheckpointProof, LocalDbError> {
    let proof = client
        .try_borrow_mut()
        .map_err(|_| LocalDbError::new("recovery owner client is busy"))?
        .checkpoint(authority_epoch, epoch, prefix.proof.prefix)
        .map_err(LocalDbError::component)?;
    if proof.digest != prefix.proof.digest || proof.retained < proof.prefix {
        return Err(LocalDbError::new("fenced shard checkpoint proof differs"));
    }
    Ok(proof)
}

/// The through-`F` retry authority of a format-4 wrapper (spec §8.1 step 6,
/// §9.2): every suffix batch must be absent from it, and after recovery the
/// publisher keeps it as its retry state.
pub(in crate::distributed::remote_db) enum RetryAuthority {
    /// WAL replay: the bounded recent window ending at `F`.
    Recent {
        recent: VecDeque<RecentReceipt>,
        ids: HashMap<BatchId, (BatchVersion, crate::distributed::types::PayloadDigest)>,
    },
    /// Receipt history: the durable history at the wrapper's root, with the
    /// through-`F` prefix count and chain state of every listed stream.
    History(ReceiptHistory),
}

impl RetryAuthority {
    /// The authority of a database without an adopted root: no retry state
    /// below `F = BeforeAll` (receipt mode: the history with no root).
    pub(in crate::distributed::remote_db) fn empty(
        path: &Path,
        config: &RemoteDatabaseConfig,
    ) -> Result<Self, LocalDbError> {
        Ok(
            if config.history_persistence == HistoryPersistence::WalReplay {
                Self::Recent {
                    recent: VecDeque::with_capacity(WAL_REPLAY_WINDOW_BATCHES),
                    ids: HashMap::new(),
                }
            } else {
                Self::History(ReceiptHistory::open(path, config.storage.database)?)
            },
        )
    }

    /// Build and validate the authority of a checked `wrapper`. Receipt
    /// mode opens the history under `path` and checks every through-`F`
    /// prefix against the history, as the exact loader does.
    pub(in crate::distributed::remote_db) fn from_wrapper(
        path: &Path,
        config: &RemoteDatabaseConfig,
        wrapper: &CheckpointImageV4,
    ) -> Result<Self, LocalDbError> {
        let floor = wrapper.replay_floor;
        if config.history_persistence == HistoryPersistence::WalReplay {
            let mut recent = VecDeque::with_capacity(WAL_REPLAY_WINDOW_BATCHES);
            let mut ids = HashMap::with_capacity(wrapper.recent.len());
            for receipt in &wrapper.recent {
                if receipt.id.database != config.storage.database
                    || !floor.contains(receipt.version)
                    || recent
                        .back()
                        .is_some_and(|last: &RecentReceipt| last.version >= receipt.version)
                    || ids
                        .insert(receipt.id, (receipt.version, receipt.digest))
                        .is_some()
                    || recent.len() == WAL_REPLAY_WINDOW_BATCHES
                {
                    return Err(LocalDbError::new("invalid checkpoint retry identity"));
                }
                recent.push_back(*receipt);
            }
            return Ok(Self::Recent { recent, ids });
        }
        let mut history = ReceiptHistory::open(path, config.storage.database)?;
        history.root = wrapper.history_root;
        for stream in &wrapper.streams {
            let prefix = stream
                .receipt_prefix
                .as_ref()
                .ok_or_else(|| LocalDbError::new("receipt stream without prefix proof"))?;
            let proof = &prefix.proof;
            let key = (proof.partition, proof.epoch);
            if history.counts.insert(key, proof.prefix).is_some()
                || history.chains.insert(key, prefix.state).is_some()
            {
                return Err(LocalDbError::new("duplicate checkpoint receipt stream"));
            }
            if proof.database != config.storage.database
                || proof.epoch > wrapper.authority_epoch
                || proof.retained < proof.prefix
                || proof.digest != history.prefix(proof.partition, proof.epoch, proof.prefix)?
            {
                return Err(LocalDbError::new("invalid checkpoint receipt prefix"));
            }
            if let Some(last) = proof.prefix.checked_sub(1) {
                let receipt = history.receipt(proof.partition, proof.epoch, last)?;
                // The newest real batch at or below `F` can precede `F`:
                // a complete prefix can end in resolved aborted gaps.
                if receipt.address.segment_generation != proof.epoch
                    || receipt.address.len < 8
                    || receipt
                        .address
                        .offset
                        .checked_add(u64::from(receipt.address.len))
                        .is_none()
                    || !floor.contains(receipt.version)
                    || config.inventory(proof.epoch).partition_for(receipt.id).ok()
                        != Some(proof.partition)
                    || history.lookup(receipt.id)? != Some(receipt.authenticated())
                {
                    return Err(LocalDbError::new("invalid retained checkpoint receipt"));
                }
            }
        }
        if floor == PrefixCut::BeforeAll && history.counts.values().any(|count| *count != 0) {
            return Err(LocalDbError::new("receipt prefix before the first batch"));
        }
        Ok(Self::History(history))
    }

    /// True if the authority already holds `id` (a suffix batch with it is a
    /// duplicate authoritative identity).
    pub(in crate::distributed::remote_db) fn known(
        &self,
        id: BatchId,
    ) -> Result<bool, LocalDbError> {
        match self {
            Self::Recent { ids, .. } => Ok(ids.contains_key(&id)),
            Self::History(history) => Ok(history.lookup(id)?.is_some()),
        }
    }

    /// Publisher retry state after recovery: the receipt history (receipt
    /// mode) or the recent window (WAL replay). The through-`F` wrapper
    /// streams stay the stream baseline of the next per-cell root.
    pub(in crate::distributed::remote_db) fn into_publisher_parts(
        self,
    ) -> (Option<ReceiptHistory>, VecDeque<RecentReceipt>) {
        match self {
            Self::Recent { recent, .. } => (None, recent),
            Self::History(history) => (Some(history), VecDeque::new()),
        }
    }
}
