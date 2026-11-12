# Sharded coordinator index and read cache (2026-11-06)

Scope: `NativeIndex` (`distributed/native_index/`), the coordinator `ReadCache`
(`distributed/remote_db/read_cache.rs`) and the lock wrappers in
`distributed/shard_lock.rs`. This record replaces the former single
`Arc<RwLock<NativeIndex>>` and `Arc<Mutex<ReadCache>>`.

## Why

Tidehunter paper §2.3 step 6 and §4.1 partition the index into cells, group
cells in rows, and protect rows with sharded mutexes. Different key ranges then
never synchronize. The single index `RwLock` made every read take one guard and
every publication take the write guard. The single cache mutex was taken twice
per read, even with the cache disabled. See
`checkpoint-work/spdk-matched-storage-20261105T004442Z/analysis/code-review/ReviewCoordinatorPath/report.md`
finding 4.

## Model

* `N` shards, a power of two in `1..=4096`, default 64
  (`NativeIndex::with_shards`, `RemoteDatabaseConfig::index_shards`).
* A `(keyspace, cell)` pair routes to `hash(keyspace, cell) & (N-1)`. The hash
  is FNV-1a with a murmur3 finalizer over explicit bytes (`StableHasher`). It is
  never `RandomState`.
* The shard map is **in memory only**. No snapshot, manifest or wire format
  records `N` or the hash. Opening a snapshot places cells by hash
  (`open_snapshot_with_shards`). A test opens one snapshot with 1, 4, 16 and 256
  shards.
* A shard is `ShardRwLock<HashMap<(KeySpace, CellId), CellIndex>>`. A point
  lookup or staged/promoted entry locks one shard. No operation holds two shard
  locks.
* Read cuts, pins, sparse views, retention floors and checkpoint capture keep
  their former meaning.

## Atomic batch visibility

A reader never sees half of a batch because visibility is decided by a cut, not
by which shards a publisher has finished.

1. `IndexWriter::promote_committed` applies a round's entries shard by shard,
   in ascending shard index. Each entry carries its `MutationVersion`. An applied
   entry is invisible to every cut that does not contain that version.
2. After every entry of every batch in the round is applied, the publisher
   advances the **publication watermark**: `publish_latest` or
   `install_reserved_live_view`. This is one `ArcSwap` store.
3. A reader resolves its cut once from the watermark, or holds an explicit
   pinned cut, and filters every shard lookup by it.

The watermark store happens after all shard writes, so a reader holding a cut
sees every entry of every batch the cut contains. The cut excludes later and
unpublished batches (higher version, or listed in the sparse view's `excluded`
set). Partial application is observable only through a cut that no reader holds.

`IndexReadCut::At` is public, so the watermark alone does not stop a caller
from naming an applied-but-unpublished version. Every entry point (`get`,
scans, histories, `pin_cut`, `validate_cut`, capture) therefore also checks an
explicit `At` cut against the watermark and fails with
`NativeIndexError::UnpublishedCut` when the cut's batch exceeds the
watermark's newest batch or an unpublished (excluded) batch lies at or below
it. `Sparse` views are minted after their entries are applied; `BeforeAll` is
empty. Regression tests: the partial-apply test (explicit `At` rejected on
every entry point while a publisher is blocked mid-apply) and
`explicit_cuts_cannot_reach_an_unpublished_late_older_batch` (late older
batch applied but excluded).

`promote_committed` therefore does **not** change what `Latest` readers see.
Test `partially_applied_batch_is_invisible_at_the_watermark_until_publication`
blocks a publisher at the highest shard and shows the apply is partial while
every watermark read stays one consistent snapshot.

## Reader recipe (replaces holding a read guard)

```text
let cut = index.resolve_cut(cut);          // once; Latest -> watermark
index.get(ks, key, cut.clone())            // one shard read lock per key
index.pin_cut(cut)                         // on a miss; sharded pin registry
```

`get(Latest)` resolves the watermark inside the shard lock. Do not call
`get(Latest)` and then `pin_cut(Latest)` as separate cut resolutions.

## Locks and order

`writer` mutex, then at most one shard lock, then retention `cold` mutex, then
pin-shard mutexes (ascending, only for checkpoint floor selection while holding
`cold`). Readers take one shard read lock, or one pin-shard mutex, and never
`writer` or `cold`.

* `writer` (`NativeIndex::writer()` -> `IndexWriter`) serializes publication and
  capture/installation. One owner already did this. It never blocks readers.
* Floor and in-progress floor are an immutable `Bounds` behind `ArcSwap`.
  `validate` is lock-free.
* A pin validates and registers under its pin-shard mutex. Floor selection holds
  every pin-shard mutex while it computes the minimum pin and publishes the new
  bounds, so a pin and a floor change cannot race (see `retention.rs`).

## Read cache

`N` independent shards (one for budgets under 16 MiB, up to 64), each a CLOCK
table behind its own mutex with a private byte budget. Counters are per-shard
relaxed atomics outside the mutex, summed on read; statistics never lock. A
disabled cache has no table: lookups and insertions only bump striped atomics.
The largest cacheable record is the shard budget (at least 8 MiB when sharded).

## Observability

* `NativeIndex::shard_stats()`: per-shard cell count, lock acquisitions,
  contended acquisitions, wait ns, sampled hold ns; publication shards-touched
  histogram (bucket `b` counts publications that locked `2^(b-1) < n <= 2^b`
  shards).
* `ReadCache::shard_stats()`: per-shard entries, hits, misses, lock stats.
* Wait is measured only on the slow path. Hold is sampled (every write hold, one
  in 64 read holds).

## Residual serialization

* `writer` mutex: publishers and maintenance among themselves. One owner calls
  them. Not on any read path.
* One padded atomic per pin registration (quota). One relaxed RMW, no lock.
* `cold` mutex: sparse-view reservation and checkpoint floor selection. Per
  admission/publication on the owner, never on a read.
* Per-shard lock: a reader and a publication that touch the same shard wait for
  each other for one lookup or one cell apply.
* Cache hits mutate a CLOCK benefit bit, so a hit takes the shard mutex.
* `Arc` refcount of a sparse view when a reader clones the watermark cut.

## Tests

`native_index/tests/sharding.rs`, `shard_lock.rs` and `read_cache.rs` cover:
independent progress of readers and publishers on different shards, no reader
taking the writer mutex, atomic multi-shard visibility under a stress run and a
deterministic partial apply, capture results identical for 1, 2, 8 and 64
shards, any shard count opening any snapshot, and the disabled cache taking no
lock.
