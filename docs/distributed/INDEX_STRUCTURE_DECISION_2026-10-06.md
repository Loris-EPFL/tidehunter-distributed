# Index structures for the sharded Value WAL

Date: 6 October 2026. Research and an implemented publication groundwork slice.
Native `Db` still uses its existing physical WAL index. This document does not
claim a distributed persisted index, complete backend or measured speedup.

Read with [the integration map](NATIVE_INTEGRATION_MAP_2026-10-06.md),
[the implementation record](IMPLEMENTATION_M1_M2_2026-10-06.md) and
[the next-step plan](NEXT_STEPS_2026-10-06.md). Sources were checked against this
checkout's `ce37b18` lineage, rather than the previous modified native checkout.

## Decision for this implementation slice

Keep the native cell index organization while separating its logical reference
type from physical WAL addresses. Reuse its atomic pending transaction machinery.
Do not select a wholesale LSM, ART or Bw-tree replacement without measuring the
actual remaining index cost. Their advantages address different workloads and
none supplies Tidehunter's publication/recovery contract automatically.

The immediate code changes are:

- `index/pending_table.rs`: `PendingTable<V = WalPosition>` and
  `CommittedChange<V = WalPosition>`. The values need only be `Copy`; they need
  neither a default/sentinel value nor an ordering implementation. `Transaction`
  retains its shared atomic commit status and stays independent of `V`.
- `distributed/index_reference.rs`: `LogicalValueId(MutationVersion)` and a
  strict 20-byte big-endian payload codec with an explicit format discriminator.
  It is a candidate compact native index identity, not a physical locator or a
  new persisted native index format. It does not narrow the epoch, sequence or
  operation-index ranges.

Existing native callers continue to use `WalPosition` and the same transaction
flag, table mutation, promotion and index serialization paths. The generic table
does not remove its caller's synchronization. A commit flag makes all staged
entries eligible together; it does not promise an atomic scan across separate
calls to several tables or turn a `HashMap` into a concurrent map.

## Native mechanisms that constrain the choice

The local [Tidehunter paper](../../../middleware/misc/tidehunter.pdf),
§§3.1–3.4 and §4.4, establishes the relevant contracts: values stay in the Value
WAL; complete batches recover together; the processed frontier excludes earlier
incomplete operations; snapshots preserve a replay cut; relocation cannot replace
a newer user mutation. Those properties matter more than a data structure's name.

The current source contains details beyond the paper's high-level description:

| Source | Existing mechanism | Consequence for distribution |
| --- | --- | --- |
| `index/index_table/mod.rs`, `DataOverlay` | `BTreeMap<Bytes, SmallVec<[IndexWalPosition; 1]>>`; one key with several ordered pending versions | Keep repeated-key history across checkpoint cuts; a latest-only map is insufficient. |
| `index/index_table/flat.rs` | Compact sorted **in-memory** flat buffer, binary search, fixed or variable key layout | It is not the persisted index format. Experimental widths affect lookup arithmetic and memory locality. |
| `index/{index_format,lookup_header,uniform_lookup}.rs` | Separate persisted index encodings and unloaded point/range lookup | Updating pending types alone cannot make disk indexes understand logical references. |
| `index/levels.rs`, `large_table.rs`, `flusher.rs` | Per-cell index levels, merge, tombstones, unloading, checkpoint filtering | This checkout already has an index-only LSM organization. Preserve tombstones until no deeper level can resurrect a key. |
| `index/pending_table.rs` | Per-key staged updates share one transaction flag | Reuse this mechanism for atomic batch eligibility; do not independently publish records as storage replies arrive. |
| `large_table.rs`, `checkpoint.rs` | Sharded synchronization, cache refill protection and snapshot latches | Network I/O must not extend a cell critical section; delayed remote reads still need cache invalidation protection. |
| `relocation/updates.rs` | Conditional native relocation using native processed positions | Distributed relocation needs logical identity **and** exact old binding checks; cross-node physical offsets cannot establish freshness. |

The paper emphasizes avoiding general key-LSM traversal. The inspected repository
has evolved a per-cell index-level policy. This observation does not imply that it
stores values in SSTables or that the paper describes this exact later code.

## Alternatives and their applicability

| Structure | Useful property | Cost or missing property here | Recommendation |
| --- | --- | --- | --- |
| Sharded LSM containing user values | Mature ordered writes/scans; partition-local parallel work | Ordinary compaction rewrites values; a separate WAL can add another value copy. Cross-shard batches still need a protocol. | Do not replace the permanent Value WAL as this optimization. Reconsider only if measured small-value/range workload wins outweigh the changed storage design. |
| Sharded/index-only LSM with values in the WAL | Sorted references can be compacted independently from large values | Read amplification and index maintenance remain; replication, snapshots and reference lifetimes still need explicit integration. | Applicable; start with the existing cell index levels and measure them. |
| Native compact sorted blocks + ordered overlay | Dense immutable data, range scans, unloaded lookup, existing checkpoint behavior | Wider distributed references reduce entries per cache line/block. Overlay insertion still allocates and uses short cell locks. | Preserve as the first implementation and performance comparator. |
| B-tree overlay | Ordered iteration and batched node locality; no new reclamation scheme under cell ownership | Allocation and comparisons may matter under hot cells; standard `BTreeMap` does not provide concurrent mutation itself. | Retain the current overlay until profiles identify it as material. |
| Adaptive radix tree (ART) | Ordered byte keys and prefix/range operations with adaptive node sizes | In-memory structure and node-reclamation costs; no automatic persisted layout, atomic batch, checkpoint or recovery semantics. Prefix benefits depend on real key distribution. | A plausible later overlay comparison, especially for skew/prefix workloads. |
| Bw-tree | CAS-installed deltas and mapping table support latch-free operations | Delta traversal/consolidation, epoch reclamation, physical page mapping and persisted-state integration. Lock-free updates alone do not supply an atomic whole scan. | A separate measured index experiment if lock contention remains dominant. |
| Concurrent skip list | Ordered, concurrent individual operations with an existing Rust implementation | More links/allocations and reclamation; individual atomic calls do not form an atomic multi-key batch or checkpoint. | Do not substitute it for the transaction protocol. Compare only against the mutable overlay under a measured contention problem. |
| Compact logical reference + derived directory | Index key payload can omit repeated physical metadata; relocation can preserve logical identity | Extra lookup, retained metadata, recovery dependency and snapshot/GC pinning. A permanently growing RAM map would defeat index unloading. | Implement typed identity groundwork now; bound and persist the directory before selecting a serving format. |
| Roaring bitmap | Compressed integer membership/intersection sets | Does not map keys to values, preserve payload identities, prove absent operations or encode an atomic transaction. Sparse/random wide IDs can be a poor fit. | Consider auxiliary live-segment or completion sets, never the primary value index or authority proof. |

Primary comparisons:

- [WiscKey, FAST 2016](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu)
  separates values into a log while keys remain in the LSM. That is relevant to
  index-only compaction and scan prefetching, rather than evidence that values
  should be rewritten into a sharded LSM.
- [RocksDB BlobDB design](https://github.com/facebook/rocksdb/wiki/BlobDB)
  keeps blob references in SST files and integrates blob reclamation with
  compaction. Its mature lifecycle is informative; importing it wholesale would
  change the current authoritative Value WAL and recovery boundary.
- [ART paper](https://db.in.tum.de/~leis/papers/ART.pdf) analyzes adaptive nodes,
  path compression and ordered byte representations. Its reported inner-node
  memory bounds exclude leaf overhead under specific tagged-pointer assumptions;
  they are not a prediction of memory use for our larger reference payloads.
- [Bw-tree paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf),
  §§III–IV, describes delta consolidation and epoch reclamation. It explicitly
  distinguishes atomic scan steps from an atomic entire scan. We cannot assume
  its latch freedom supplies Tidehunter checkpoint semantics.
- [Crossbeam SkipMap documentation](https://docs.rs/crossbeam-skiplist/latest/crossbeam_skiplist/)
  documents lock-free individual operations and epoch-based reclamation. It also
  warns that sequences of operations can interleave; the native transaction flag
  and version history would still be necessary.
- [Roaring Rust implementation](https://github.com/RoaringBitmap/roaring-rs) and
  [Roaring paper](https://arxiv.org/abs/1402.6407) concern compressed integer sets.
  Use them only where set operations are actually required. No Roaring dependency
  was added to the index slice.

## Reference size, ordering and equality

Native `WalPosition` has a 12-byte encoded payload: offset `u64`, length `u32`.
Its Rust layout can include alignment padding. The native flat representation
packs the entry kind into the upper length bits; it must not do the same to a
logical version whose full bit range is valid.

The new logical identity encodes `(epoch: u64, sequence: u64,
operation_index: u32)` in **20 bytes**. Its Rust layout is that of
`MutationVersion`, which can include trailing alignment. A future fixed-key
index entry would require the key, those 20 bytes and an explicit entry-kind
representation, plus any container overhead. Thus this is not a 12-byte drop-in
replacement. Persisted format identity, database identity, integrity and snapshot
cut belong to the containing index image and are not established by this codec.

A minimally specified direct reference already needs 44 packed bytes for the
same logical version plus partition `u32`, generation `u64`, record offset `u64`
and length `u32`. That is a lower bound, not a proposed safe wire format: it omits
authority membership, parent-batch identity and integrity descriptors. The
current `ValueRecordReference` carries those additional fields. Repeating all of
them per persistent key is expensive; keeping them in a directory moves that
cost and adds an indirection, rather than making it disappear.

For illustration, 100 million references at 20 versus 12 packed bytes consume
800 MB more before keys, entry kind, allocator, version history, directory and
cache overhead. This arithmetic is not a measured resident-memory estimate.
Evaluate **index plus reference directory together**, including cold misses and
checkpoint retention, before choosing the compact form for the serving backend.

Logical equality means the same original mutation within the same database.
Physical equality means the same complete address/generation/descriptor binding.
Neither equality proves durable publication. Lexicographic encoding agrees with
logical order, including repeated-key operations within one batch. A later
physical write or another partition's larger offset does not create a newer user
mutation. No implicit conversion to `WalPosition` is provided.

## Remaining correctness and integration gates

1. **Authoritative directory.** Resolve a logical identity through a complete,
   validated batch registration and its operation descriptor. Do not accept a
   client-supplied self-consistent descriptor as proof of batch membership. A
   bounded epoch map is adequate for development; sustained service requires a
   recoverable and evictable representation.
2. **Native publication.** Stage every operation in the actual native pending
   tables, including deletes and repeated keys, before committing their shared
   transaction. Publication errors after durable authority require recovery.
3. **Version-aware index layers.** Adapt overlay ordering, packed flat buffers,
   unloaded formats, stale-update checks, tombstones and iterators together.
   Logical identities do not implement this integration merely by existing.
4. **Checkpoint history.** Keep the pre-cut version when a key also has a later
   in-flight update. Filter at the logical cut before collapsing same-key history.
   Pin directory and physical records alongside the native index image.
5. **Relocation.** Preserve the logical identity; compare the expected old physical
   binding including generation before installing a replacement. Persist the
   replacement and recoverable mapping before reclaiming source bytes. A logical
   equality check alone cannot reject two competing relocations or protect an old
   reader. This slice deliberately has no relocation helper pretending otherwise.
6. **Format dispatch.** Legacy on-disk indexes remain legacy. The experimental
   container must reject unknown format/database identities and supply fallible
   bounds checking, rather than infer its type from a 20-byte reference length.
7. **Measured selection.** Compare CPU, bytes per key, directory misses, cell lock
   wait, index read amplification and checkpoint overhead using matched native
   and experimental paths. Single-SSD results establish these local mechanisms;
   they do not predict multi-NVMe cluster throughput.

The tests in this slice exercise typed transaction staging, duplicate-key order,
abort cleanup, codec bounds/version rejection and full-range logical ordering.
The enclosing integration tests must additionally connect actual storage receipts
and native pending publication. Neither kind replaces the future native
flush/unload/reopen/checkpoint tests required for a serving distributed `Db`.

Local verification on 6 October, with `CARGO_BUILD_JOBS=2`, `--offline`, package
`tidehunter`, `--features experimental-distributed --lib` and two test threads:

- `index::pending_table::tests`: **3 passed**, including the pre-existing native
  concurrency test and the new mixed-type transaction test.
- `distributed::index_reference::tests`: **2 passed**.
- `git diff --check` for this slice: passed.

These are small correctness tests. They produce no throughput comparison and
do not establish distributed flush, snapshot or Sui readiness.

## Follow-up: serving experimental reference index (6 October)

This follow-up supersedes the earlier statement that this slice only supplies a
pending-table bridge. The new `distributed::native_index::NativeIndex` now serves
point lookup and ordered scans for the experimental local database. It is a
separate, explicitly versioned backend. It does **not** replace or claim to finish
legacy `LargeTable`, `IndexFormat`, cache-generation, or `Db` integration.

The scoped design adjustment was chosen to avoid a risky rewrite of the mature
physical-position index while making the new path actually serve WAL-backed
reads. Native `IndexTable` and the new typed index now both call
`index::versioned_overlay` for ordered inline version vectors, unique sorted
insertion, processed-prefix selection, and compact-block binary search. Their
ordering predicates remain different: legacy physical order is unchanged, while
the experimental index uses `MutationVersion`. Both use the actual native
`PendingTable`/shared `Transaction`, and the new backend derives cell routing and
reduced keys from native `KeySpaceDesc`. This is shared native machinery with a
new experimental cell container, not integration into the legacy LargeTable.

### Serving and persistence rules

- The per-cell overlay stores reduced keys and typed `ValueRecordReference`
  metadata. Application values remain exclusively in their partition Value WAL.
  No value LSM or second value store was introduced.
- Promotion touches only cells staged since the preceding promotion. A shared
  native transaction commits all pending entries before the enclosing authority
  promotes them and exposes the new published prefix. A post-authority error
  still requires recovery; partial in-memory promotion is never a definite abort.
- Repeated-key mutations are ordered by `(epoch, sequence, operation_index)`.
  Equal logical identities are accepted only for exact reference replay.
  Relocation cannot silently replace a different physical binding through this
  rule. There is no relocation or GC implementation in this slice.
- `IndexReadCut::BeforeAll`, `At(version)` and `Latest` are distinct. The index
  retains all versions and tombstones, so a post-cut put/delete cannot erase a
  checkpoint's previous version. The enclosing database supplies an authoritative
  published cut; the index's greatest retained version is not a proof of a
  complete database prefix.
- Forward/reverse scans perform a heap-based merge with one candidate per native
  cell. They initialize cells once per page and advance only the selected cell.
  Heap work is `O(cells + visited keys × log active cells)`; finding the next
  entry inside a cell additionally searches its overlay/compact block. Cursors
  use reduced native keys. Hash-index keyspaces reject ordered scans, and callers
  perform required full-key checks against the resolved WAL record.
- `flush_and_unload` creates a new immutable CRC-framed, versioned, metadata-only
  image, syncs the file and directory, and retains its compact cell blocks through
  a read-only mapping. Post-cut versions stay in the live overlay. `reload`
  materializes compact metadata bytes, not application values. `open_snapshot`
  validates bounds, format, database/schema, cell routing, logical order and cut.
  Failed image writes may leave an unreferenced partial file; a durable manifest
  may name an image only after successful completion. Existing image files are
  never overwritten.
- Snapshot loading alone does not establish WAL membership or checkpoint
  authority. The enclosing lifecycle must verify its inventory and resolve
  records through the partition's immutable authoritative directory. All WAL
  epochs and retained index histories remain pinned in this bounded no-GC mode.

### Explicit costs and remaining work

The first image format uses **162 packed bytes per complete reference**, plus
keys, version counts, offsets and cell headers. It repeats batch/domain/integrity
metadata intentionally until a durable compact indirection design replaces it.
Admission charges `size_of::<ValueRecordReference>() + 32 + reduced_key_length`
for each retained mutation and includes outstanding reservations before durable
append. This is a conservative admission accounting unit, **not a hard RSS
measurement**: allocator/tree/mapping overhead must be measured separately. The
current bound is at most 512 MiB of charged metadata per index. Flush/unload
changes residency but does not reclaim quota or history.

The outstanding scale work is therefore concrete: checkpoint-root authority,
compact reference-directory integration, safe history retention and conditional
relocation, durable retention/GC, cache policy, and the eventual legacy backend
integration decision. The Tidehunter paper's keep-values-in-WAL mechanism and
complete-batch/derived-index rules remain the anchors; no claim of cluster
throughput follows from this local implementation.

Verification after the shared-native extraction and heap scan change:

- `distributed::native_index::tests`: **4 passed**. Real WAL receipts cover
  multicell/keyspace publication, reduced/prefixed/hash keys, duplicate replay,
  within-batch mutations, historical cuts, old-cut snapshots with newer overlays,
  unload/reload, malformed images, wrong domains/schema and capacity rejection.
  The scan regression uses 64 native cells, 384 real WAL mutations and seven-key
  pages, with both directions and mixed compact/overlay history.
- Default native `index::index_table`: **70 passed, 1 pre-existing ignored**.
- These are bounded correctness checks; the parent task's rebuilt local benchmark
  records the current performance evidence separately.
