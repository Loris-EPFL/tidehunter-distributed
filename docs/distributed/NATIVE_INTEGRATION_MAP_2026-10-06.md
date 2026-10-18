# Native publication and recovery integration map

Date: 6 October 2026. M3/M4 implementation map, based on this fork's native
`ce37b18` lineage and existing cache-race correction. This is not a completion
claim. M1/M2 storage work proceeds independently; native `Db` integration must
meet the gates below before Sui selects it.

## Invariants to carry from the paper and code

The Tidehunter paper §§3.1–3.4 makes values live in the Value WAL, treats complete
batches atomically during recovery, derives the index from the log, and snapshots
the index together with a replay frontier. Section 4.4 makes relocation a means of
reclaiming space, rather than a second ordinary value-storage stage. The inspected
native code supplies additional mechanisms: pending transactions for atomic live
publication, checkpoint latches, per-cell index layers and cache refill tokens.

For the distributed mode:

1. A complete validated durable authoritative batch becomes irrevocable before
   local index publication. No second full-value database or per-batch authority
   decision WAL is introduced.
2. A logical mutation version orders writes. A physical address locates bytes.
   Comparing two different storage nodes' offsets is meaningless.
3. Durable receipt, resolved prefix and published prefix are different facts.
   A global counter supplies none of the latter by itself.
4. Replay enumerates every registered stream, seals/fences the old authority and
   orders eligible batches by their original logical versions before serving.
5. A checkpoint pins a logical publication frontier and every needed reference.
   Index unloading cannot discard unresolved/post-checkpoint history.
6. Relocation changes location without creating a newer user mutation. Remote
   storage reuse requires protected readers and generation checks.

Reference implementations inform distinct parts of this design:

- [CorfuDB multi-object commit](https://github.com/CorfuDB/CorfuDB/blob/master/runtime/src/main/java/org/corfudb/runtime/object/transactions/OptimisticTransactionalContext.java)
  groups mutations in one authority entry. This is the relevant analogy; its
  conflict manager and object runtime are not substitutes for Tidehunter's index.
- [FoundationDB commit path](https://github.com/apple/foundationdb/wiki/Transaction-Commit-Path)
  separates ordered durable log history from derived storage application.
  Tidehunter should retain its own long-term values-in-log representation rather
  than copying FDB's value duplication into a second storage engine.
- [TiKV scheduler](https://github.com/tikv/tikv/blob/49e7a1179d676b1bc51447f6b0ff8973e45bc85f/src/storage/txn/scheduler.rs)
  provides an implementation reference for bounded local scheduling, task state
  and admission. It does not imply one network request per internal worker task.

## Why replacing `Db.wal` is insufficient

`WalPosition { offset: u64, len: u32 }` is serialized in 12 bytes. In the local
engine its offset can simultaneously order a mutation and locate the frame,
because all mutations share one append allocation domain. The same type also
locates **index blobs**, which remain local in the proposed initial architecture.
Changing it globally would alter both index-value semantics and physical index
storage semantics.

Do not pack a partition number into the high bits of the offset: that would order
writes by node identity or wrap independent offsets into a false global history.
Do not make the current physical `LastProcessed` into the maximum observed remote
counter: gaps and incomplete batches would then be reported as published.

## Source audit by role

Paths refer to `tidehunter/src/` in this checkout. Function names are the stable
anchors; line numbers may move during parallel implementation.

| Source and mechanism | Meaning of position today | Required treatment |
| --- | --- | --- |
| `wal/position.rs`: `WalPosition`, `WalFileId`, `MapId`, `HasOffset` | Physical byte address and native serialization | Keep physical WAL addresses for node allocators and local index blobs. Define separate logical/frontier types. |
| `wal/allocator.rs`, `wal/mod.rs`: allocation, `multi_write` | Byte-range uniqueness, fragment capacity | Node-local; no global allocation RPC and no logical order inferred from fragment order across nodes. |
| `wal/tracker.rs`: guards, contiguous completion and latch | Native physical allocation completion | Preserve native backend. Experimental authority needs logical resolution/publication tracking with its own checkpoint latches. |
| `batch.rs`: `PendingOp` and `WriteBatch` | Validated user mutations plus one shared transaction | Reuse real native batching/keyspace reduction. Validate all mutations and reserve bounded admission before authoritative bytes may exist. |
| `db.rs`: `do_write_batch`, scalar write/delete | Native append returns positions; pending operations then share one commit flag | Distributed append returns durable logical/physical references; install all pending native index entries before the same atomic commit. Any post-durability failure becomes recovery-required/unknown, never a definite abort. |
| `index/pending_table.rs`: `PendingUpdateInner`, `CommittedChange`, `Transaction` | Pending values currently store physical `WalPosition` | Retain atomic status lifetime; parameterize or introduce typed index references. Validate/prepare every reference before committing visibility. |
| `index/index_table/mod.rs`: `IndexWalPosition`, `checked_insert`, data overlay | Offset orders versions; length packs entry kind; flat entries are 12-byte positions | Separate mutation ordering from location. Keep duplicate-key operation order. Version the experimental flat format; never decode it as legacy format. |
| `index/index_table/flat.rs` and `index/index_format.rs` | Fixed entry width, binary-search layout and optimistic lookup | All entry sizes/search offsets must use the selected format. A wider reference has real cache/I/O cost that needs measurement. |
| `large_table.rs`: `skip_stale_update`, `mark_dirty`, `promote_pending`, `get_at` | Conflict order and logical-like per-cell replay frontier expressed as native offsets | Compare explicit logical mutation versions. Dirty cell/replay position must describe logical history plus the necessary stream boundaries. |
| `large_table.rs`: `levels`, `SnapshotEntryData`, `Loader::index_reader` | Local physical index blob addresses | Keep independent from remote value references; don't add an unnecessary remote index hop. |
| `db.rs`: `read_record_check_key`, `resolve_record_frame` | Reads one local frame or compressed batch; empty-value fast path derives size from local frame layout | Resolve experimental record references through the storage backend. Never apply native frame-length or compressed-batch shortcuts to a remote descriptor. Preserve full-key comparison for reduced-key spaces. |
| `db.rs`: `read_record_for_indexed_key`, `iterators/db_iterator.rs` | Index order followed by native frame resolution | Reuse ordered native index traversal and resolve value records with bounded parallelism; preserve errors and direction/bounds. |
| `large_table.rs`: cache refill token/invalidation | Prevent delayed old reads from repopulating cache after mutation | Carry the token across a remote read; accept refill only if the same cell mutation generation remains current. |
| `checkpoint.rs`: `DbCheckpoint` and `WalTrackerLatch` | Captured processed offset pins overlay promotion/flush | Experimental checkpoints pin a logical frontier and storage retention set, not a node's latest byte offset. Preserve documented drop-cell limitations. |
| `flusher.rs`: `retain_processed` and `large_table.rs`: `retain_unprocessed` | Keeps later/in-flight versions in overlay while older state is flushed | Use the experimental logical cut consistently. A latest-per-key collapse before filtering loses checkpoint-visible history. |
| `wal_replay.rs`, `replay_buffer.rs` | Physical sequential scan; incomplete batch truncation; sorted per-cell application | Distributed recovery first validates all streams and authority eligibility, then sorts by logical mutation version. Reuse native index replay application only after reference/order adaptation. |
| `db.rs`: `rebuild_control_region_from`; `control.rs` | Local index snapshot plus one replay-from offset | Experimental snapshot needs explicit inventory identity, logical cut, local index files, reference metadata and replay/retention boundaries for each registered stream. Persist dependencies before manifest publication. |
| `state_snapshot.rs` | Legacy snapshot utility currently assumes a single `wal_000...` truncation file | Not a distributed snapshot implementation. Keep inaccessible for distributed restore until format/lifecycle is explicitly handled. |
| `relocation/mod.rs`, `relocation/updates.rs` | Physical cutoffs and last-processed conditional replacement | Native physical comparison is not a distributed mutation CAS. Compare expected logical version and original reference; persist replacement and recovery mapping before reclamation. |
| Keyspace destroy/drop-cell WAL entries | Schema/lifecycle operation ordering | Must become authority-eligible ordered control records or be explicitly unsupported before effects in experimental mode; current complete-batch put/delete codec does not cover them. |

## Recommended implementation boundary

The next native change should make the index value domain explicit while keeping
the existing native physical backend as a reference. Candidate types:

```text
PhysicalWalPosition                   // existing node/index-log address
LogicalMutationVersion                // epoch, sequence, operation index
IndexValueReference                   // version + local or distributed location
LogicalPublicationFrontier            // contiguous resolved/published cut
DistributedValueRecordReference        // generation, container/member/record identity
```

The API can keep `Db`, `WriteBatch`, `DbCheckpoint` and iterators stable, but open
must select an explicit native or experimental backend. Every read/write/recovery
method then dispatches according to that selection. An `open_distributed` path
must refuse the wrong on-disk identity instead of reusing native WAL recovery.

There are two credible compact-index implementations to evaluate:

1. **Direct typed reference in the native index.** Store logical version and
   physical record descriptor in versioned index entries. This is easiest to audit
   but widens entries and may reduce the optimistic-index/cache advantage.
2. **Explicit logical reference ID plus derived mapping.** Keep a compact native
   index entry referring to immutable authority-record identity. Resolve its
   physical descriptor through a bounded, recoverable reference map, rebuilt from
   authoritative streams and checkpointed with the native index. This adds an
   indirection and cannot leave an unbounded permanent in-memory map.

The second option must not hide a logical ID in a value called physical
`WalPosition`, silently narrow the epoch/order range, or persist a new frontend
reference decision for every batch. Reference-map snapshots may be asynchronous
because the authoritative batch log supplies recovery, provided its retention and
publication cut are proved together. Compaction/retirement of that mapping is a
separate correctness gate.

An implementation that instead writes user values into a second local `Db`, or
uses a new standalone BTreeMap key/value database, does not meet M3. It can be a
test oracle only. Likewise a local reference WAL requiring a second synchronous
barrier would need a measured design revision before being accepted as the new
performance architecture.

## Minimum honest serving path

Develop M3 and M4 together; do not expose an unrecoverable partial backend:

1. Add experimental index reference/order format, pending publication and logical
   frontier; native serialization remains unchanged.
2. Establish exclusive local authority with durably registered static inventory,
   new epoch and complete predecessor sealing. Keep RF1 missing-node recovery
   blocked. This is controlled assignment, not automatic distributed consensus.
3. Append each validated complete batch to its deterministic storage partition;
   await durability and install all native pending entries at that batch's logical
   turn. Commit the shared native transaction and advance published progress.
4. Serve scalar read/exists and ordered/checkpoint reads from the actual native
   index through independently verified direct record reads. Preserve same-key
   conflict ordering and stale-cache refill protection.
5. Reopen by sealing/enumerating the complete predecessor inventory and replaying
   authority records into the native index. Initially retaining all log data can
   be an explicitly bounded development mode; it is not sustained production GC.
6. Add snapshots naming every stream's replay/retention frontier and native index
   image; only then permit retention reductions.
7. Fail explicitly for unsupported schema maintenance, compression/relocation or
   snapshot operations. No feature may silently delegate mutations to a native
   local WAL and claim distributed service. Such a restricted mode is not yet a
   drop-in Sui backend.

## Acceptance histories

### Implemented boundary: logical publication tracking

`distributed/publication.rs` now implements a bounded, volatile, single-epoch
tracker that accepts validated complete records and storage-verified durable
receipts. Its durable and published prefixes are distinct types. It retains
out-of-order durable responses behind gaps, keeps unknown outcomes unresolved,
checks identity/version/digest reuse, and invokes callbacks only in contiguous
logical order. Callback error or unwinding panic marks publication recovery-required
before any further callback can run. Later durable responses remain retainable.

This does **not** establish authority fencing or implement the native callback.
Construction is only valid after complete inventory registration, exclusive
authority and predecessor recovery. There is no live hole-resolution API and no
reset that guesses whether an uncertain callback committed. The future `Db` must
gate normal/cache reads and writes on publication health after callback failure.
Native pending updates must all be installed before their shared transaction
commit; application acknowledgement follows successful tracker advancement.

Admission and sequence allocation must be serialized by the enclosing authority
so every admitted batch has a consecutive registered reservation before storage
effects. Storage execution/responses may then run concurrently. The tracker
retains published retry identities within its explicit lifetime capacity. Reaching
that bound rejects new identities before effects while preserving identical retry
lookup. A proved retry-retention floor or recovered epoch rotation is required
before claiming sustained service; an LRU eviction must never silently reexecute
an old identity. The tracker has no persistence and cannot be recreated in the
same unfenced epoch after losing its state.

The callback tests exercise gaps, out-of-order durability, timeouts, poisoned
publication, and identity validation. They do not substitute for the complete
native index/flush/recovery histories below.

Validation on 6 October: six publication tests passed with no failures or ignored
tests using `CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --features
experimental-distributed --lib distributed::publication::tests --
--test-threads=2`. The feature-enabled native library also passed `cargo check`.
These are local component results, not a Sui or cluster run.

- Native and experimental backends receive identical mixed-keyspace put/delete
  histories, including repeated keys inside one batch, and agree after reopen.
- Two partitions complete conflicting batches in reversed physical completion
  order; logical order still wins and the publication prefix does not skip gaps.
- Crash after each persistence/publication step replays every eligible complete
  batch, including a surviving unacknowledged batch, with no partial visibility.
- Delayed remote get followed by overwrite/delete cannot install a stale cache
  refill; checkpoint reads obey their captured logical cut.
- Native flush/unload/reload and checkpoint promotion are exercised, so an
  all-memory test oracle cannot mask an incorrect persisted index format.
- Partial inventory, partial fencing, conflicting duplicate identity/digest,
  incompatible schema and old sealed retries fail closed.
- Snapshot/reference-map cut tests preserve later durable batches after an
  unresolved gap and never advance retention from a maximum sequence alone.
- Backend identity is observable in the real Sui audit. A successful program
  launch or local native fallback is insufficient evidence of integration.

No timing parity follows from these correctness checks. One shared laptop SSD
cannot measure the cluster benefit of independent storage drives; operation
counts, barriers and bytes are useful local mechanisms to compare first.
