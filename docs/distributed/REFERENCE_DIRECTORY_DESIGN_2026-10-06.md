# Immutable membership directory for distributed Value WAL reads

Date: 6 October 2026. Component design and implementation; this directory does
not implement native `Db` publication, TCP, replication, remote fencing or GC.

## Why this lookup exists

The current experimental storage path already reads one record directly from its
long-term native WAL location. Before reading, it checks that the supplied record
descriptor belongs to a complete authority batch accepted during append or sealed
recovery. A self-consistent inner digest is insufficient: a caller could invent a
different reference. The previous implementation checked that membership in
`State.records` while holding the partition's metadata mutex.

Tidehunter's paper §§3.1–3.2 keeps the value in its Value WAL and locates it through
an index; §3.3 separates recovery progress from allocation. The implementation
must retain those distinctions. This change moves immutable **physical membership
metadata** out of the mutable retry/admission map. It leaves value bytes in the
same WAL. It does not turn directory membership into a durable receipt or a
database-visible commit. See the [local paper](../../../middleware/misc/tidehunter.pdf),
[`codec.rs`](../../tidehunter/src/distributed/codec.rs),
[`storage.rs`](../../tidehunter/src/distributed/storage.rs), and
[`reference_directory.rs`](../../tidehunter/src/distributed/reference_directory.rs).

## Data-structure comparison

| Candidate | Fit for this particular directory | Decision |
| --- | --- | --- |
| Previous `Mutex<BTreeMap<BatchId, ...>>` | Exact lookup and simple lifetime, but reads share the append/admission lock | Replaced for exact retry IDs by writer-owned `HashMap`; scalar membership reads now use the separate immutable directory |
| `ArcSwap<BTreeMap<...>>` replaced per append | Readers can see an immutable snapshot, but ordinary whole-map copy-on-write copies accumulated history for each update | Reserve ArcSwap for small, infrequently replaced routing/epoch snapshots |
| Persistent copy-on-write tree | Can avoid whole-map cloning using path copies; needs another tree implementation, publication protocol and allocation accounting | Worth measuring when mutation/deletion of directory entries becomes necessary |
| DashMap | Shards locking across keys; it does not remove read locks, and a hot partition/key can still contend | Unnecessary for one metadata writer and immutable entries |
| Crossbeam SkipMap | Supports concurrent insert/remove and epoch-protected readers | More capability, pointers and reclamation machinery than this sole-writer/no-removal stage needs |
| Roaring bitmap | Compact set operations over integers | Cannot store identity, full physical address, digests or descriptors; may later index retired/live segment numbers as a secondary summary |
| LSM tree | Bounded RAM and persistent indexed history can serve arbitrary identities | A possible future retry/history backing store, with explicit reads/compaction and checkpoint semantics; not necessary for this bounded in-memory read directory |
| Dense vector keyed by logical sequence | Cheap indexing | A partition receives sparse logical versions from a shared allocator; allocating missing slots or treating them as holes is incorrect |
| Hash table keyed by physical offset | Expected constant lookup | A resizing concurrent table needs synchronization/reclamation or whole-table replacements; not selected without evidence that binary search dominates |
| Segmented append-only slots ordered by physical offset | Stable entries, lazy bounded allocation, binary search, sole-writer prefix publication | Selected for the current retained-epoch component |

Sources reviewed for these mechanisms:

- Rust's [`OnceLock`](https://doc.rust-lang.org/std/sync/struct.OnceLock.html)
  exposes immutable initialized values through nonblocking `get`. The code uses
  stable APIs available since Rust 1.70. Readers never call `wait` or initialization
  methods. The standard library's linked-list example illustrates append-only
  ownership; our bounded segmented layout avoids linear traversal.
- [`append-only-vec` source](https://raw.githubusercontent.com/droundy/append-only-vec/master/src/lib.rs)
  demonstrates stable segmented allocation and a visible completed prefix.
  Its multiwriter coordination is unnecessary here. This implementation uses
  safe standard-library cells and one non-clone writer capability; no source is
  copied and no dependency is added.
- [ArcSwap's performance documentation](https://docs.rs/arc-swap/latest/arc_swap/docs/performance/index.html)
  distinguishes borrowed loads, owned Arc loads and expensive replacement.
  ArcSwap makes pointer publication efficient; it cannot itself eliminate the
  application's cost of constructing a replacement map.
- [Crossbeam SkipMap documentation and source](https://docs.rs/crossbeam-skiplist/latest/crossbeam_skiplist/)
  explains concurrent ordered maps, epoch reclamation and retained entry handles.
  A lock-free map does not automatically provide atomic multi-entry publication.
- [DashMap source](https://docs.rs/dashmap/latest/src/dashmap/lib.rs.html)
  uses shards protected by read/write locks.
- [Roaring Rust bitmap source](https://raw.githubusercontent.com/RoaringBitmap/roaring-rs/master/roaring/src/bitmap/mod.rs)
  models compressed sets of integers, which is a different payload contract.
- [RocksDB leveled compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction)
  is useful when evaluating persisted history rather than assuming an LSM removes
  WAL authority ordering, atomic publication or complete-inventory recovery.

These are component comparisons, not a performance ranking measured on this
workload. No source establishes that the selected directory closes the cluster gap.

## Chosen layout and API

`ReferenceDirectory` fixes its partition, segment generation and maximum batch
count at construction. A small immutable top-level array contains write-once
segment pointers. Each lazily allocated segment contains at most 256 write-once
`RegisteredBatch` slots. The last segment is shortened to the configured limit.

Each registered batch owns only:

- original operation identity and logical batch version;
- complete-batch digest;
- physical batch address, including partition and segment generation;
- a boxed slice of record descriptors;
- the containing complete group's physical end.

There are no key/value payload copies in the directory. Existing slots do not move
when another segment is allocated. A read returns borrowed immutable metadata and
does not increment an Arc reference count for each lookup. The containing storage
handle keeps the directory alive for the read.

The one writer receives `ReferenceDirectoryWriter`, which is not cloneable.
Calling `publish_group` requires `&mut self`. Existing storage ownership serializes
that capability. After complete metadata validation and initialization of every
slot, the writer release-stores the new visible length once. Readers acquire-load
the length once and binary-search only that immutable prefix. Thus readers see
either none or all of a newly registered group; an in-progress writer does not
block existing reads.

Physical offsets within one retained stream increase even when logical versions
arrive out of order. Slots are dense by **registered batch count**, not physical
offset or logical sequence. Large fragment/file gaps therefore consume no empty
per-offset slots. At one million batches, binary search requires at most about 20
comparisons. This is an algorithmic bound, not measured CPU or disk latency.

`lookup(address)` compares the entire physical address after finding its offset.
`validate_reference(reference)` compares every identity, version, complete-batch
digest, address and record-descriptor field. A foreign generation, a shifted
offset, an interior record masquerading as a batch or a changed digest is rejected
before record I/O. The existing codec then checks the requested record's bytes.

The arbitrary `BatchId` retry map is now a `HashMap` separately owned by the write
path. The per-partition `BatchVersion` index remains a `BTreeMap`: those versions
are sparse and recovery needs their logical ordering.
Retry identities are not dense counters, and converting them to a probabilistic
bitmap would lose conflict detection. A serialized directory handle is unnecessary:
every current reference already contains the physical batch offset.

## Publication, memory and failure rules

1. Storage validates complete group authority before registering any metadata.
   Directory insertion is not a substitute for whole-group CRC, schema, identity,
   placement, epoch and digest validation.
2. Storage checks directory capacity **before WAL effects**. Both maximum retained
   batches and maximum charged reference-metadata bytes are explicit limits.
3. The byte charge covers the directory's Arc allocation, top-level cells, all
   allocated slot segments including currently unused reserved slots, and exact
   boxed descriptor lengths. It excludes allocator overhead, the writer object,
   temporary input and the separate storage retry/version maps. It is neither an
   RSS measurement nor a bound on the entire process. The new group's complete
   allocation charge is reserved before initialization, so interruption can
   overcharge its partially installed metadata but cannot hide it from the budget.
4. Capacity refusal preserves existing metadata and never forgets an old retry.
   A later application cannot reexecute an evicted identity because this directory
   has no eviction operation.
5. Metadata publication after a successful WAL append cannot be reported as a
   definite abort if it fails. Storage must poison the writer and recover the
   sealed complete inventory. A caught panic after slot initialization leaves
   those slots hidden and makes that writer capability unusable.
6. Directory publication certifies membership only. The batch can still be
   unpersisted and not yet visible through the native database index. Durability
   receipts and logical publication trackers remain distinct.
7. All registered entries and corresponding WAL files are retained. No reuse,
   relocation, GC, lease expiry or replica promotion is introduced. A later
   retention design must extend lifetime and recovery rules before removing
   directory entries or source WAL regions.

This is a mutex-free metadata **read path**. It does not claim that the full
distributed write, native WAL lookup, mmap allocation, filesystem, or RPC path is
lock-free. A disk read can block even when its directory lookup cannot.

## Validation

The focused tests exercise:

- absence before registration and exact membership after registration;
- forged identity, version, full-batch digest, generation, partition, offset,
  length, operation index and descriptor fields;
- 513 entries across segment boundaries with enormous sparse physical offsets,
  stable old references and bounded metadata allocation;
- an exact metadata budget and before-publication refusal of the next batch;
- readers making progress while a new complete group's publication is paused;
- no partially visible group and no reuse after an interrupted publication;
- malformed later group metadata leaving the whole new group invisible.

Run from the repository root after module/storage integration:

```sh
CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --features experimental-distributed --lib distributed::reference_directory::tests -- --test-threads=2
```

Focused component validation completed: **6 passed, 0 failed**. Storage integration
and its complete-suite evidence are recorded separately by the integrating task.

These are local mechanism/correctness tests on one shared SSD. They do not measure
independent NVMe throughput, network latency, RDMA, Sui parity, or end-to-end native
index integration.
