# Native Tidehunter control

By default, the `native` backend opens one ordinary `tidehunter::db::Db` with one fixed-size keyspace. Each native instance uses the established adapter-benchmark shape of 16 mutexes and 64 cells per mutex (1,024 cells total); dataset key count does not expand the index geometry. The harness generates opaque 32-byte keys; Tidehunter's `KeyType::Uniform` maps the first four key bytes to cells and retains the complete key in the index/WAL. Each instance uses the native index and key-shape path. See the Tidehunter paper, §§3 and 6.1 (`../../../papers/parsed/Tidehunter-2026-v2.md`): the Large Table indexes values by WAL position, and the paper's baseline uses 32-byte keys.

## Separate acknowledgment modes

- `durability: "page_cache"` is the regular foreground `Db::insert`, `Db::remove`, or native `WriteBatch::commit` return. No additional `persist_value_wal_for_benchmark` call is made. This measures the ordinary foreground native path, including whatever buffering/page-cache work the engine performs; it does **not** claim a foreground fsync durability barrier.
- `durability: "value_wal_barrier"` performs that same mutation and then calls the existing `Db::persist_value_wal_for_benchmark` Value-WAL persistence barrier before returning. The barrier's time is part of the API operation. It synchronizes the registered Value-WAL files and their directory (including clean older files); it is not a full index snapshot, a transaction-isolation mechanism, or an operating-system/device power-loss certification.
- `durability: "value_wal_group_barrier"` performs the same mutation, then waits for a shared Value-WAL barrier that starts after the mutation returned and the caller entered the barrier. Concurrent callers to the same Db share runs; a caller that enters during a run waits for a later covering run. Each run synchronizes every registered Value-WAL file and its directory for that Db, as in `value_wal_barrier`. There is no timer or fixed batching delay. A failed sync fails all waiting callers and every later barrier call for that Db with the same error; it is not retried. A leader panic wakes waiters with the sticky error `group barrier run panicked` and continues unwinding.

The modes are separate option values and are reported independently in `BackendInfo`. A write batch uses Tidehunter's native batch implementation; its `BatchAtomicity::CrashRecovery` label means native WAL batch recovery behavior only. It does not mean that other readers cannot observe intermediate publication or that the batch has serializable isolation.

The grouped mode removes the unshared per-put barrier from the concurrent durable Native comparison. It changes only the adapter. It does not change engine code, native mutation or read paths, file formats, index publication, relocation, or the files and directory that the barrier syncs. Sharing is per Db instance, not between instances or processes. It belongs to the `synchronous` comparison tier; `page_cache` remains separate.

The caller's registry exports `kv_research_native_group_barrier_runs_total` (all attempted runs), `kv_research_native_group_barrier_callers_total` (callers covered by successful runs), and `kv_research_native_group_barrier_errors_total` (failed runs). The `kv_research_native_group_barrier_group_size` histogram records callers per successful run with buckets 1, 2, 3, 4, 6, 8, 12, 16, 24, 32 and 64, plus `+Inf`. Its sum equals successful callers covered; its count equals successful runs. These metrics are registered in every mode and stay zero outside the grouped mode. Counters cover the whole adapter lifetime, not only sampled operations. Tickets and counters use fixed-width integers; ticket exhaustion returns an error instead of wrapping.

Native gets return Tidehunter's `Bytes` directly. `get_many` performs ordered scalar gets and retains those returned buffers; it neither changes the keyspace API nor adds a copy of value payloads. With one instance, native engine metrics are registered in the caller's Prometheus `Registry` via `Metrics::new_in_enabled`, then included by the shared result exporter.

## Independent native instances

`native_shards` accepts integers from 1 through 16 and defaults to 1. With 1, the Db path is `options.path` itself. With N greater than 1, instance i opens at `options.path/shard-{i:02}`. Each instance has its own Db, keyspace, WAL files and group barrier. All instances use the same geometry, durability and `key_count` option. The 16-by-64 index shape is per instance: five instances have five independent indexes, not one shared coordinator index.

Changing N changes both paths and key routing. Use a fresh root for each shard-count cohort; this option does not migrate an existing dataset or reproduce an older root with another shard count.

The adapter routes each scalar key with unsigned FNV-1a 64-bit: start at `0xcbf29ce484222325`, then set `h = (h XOR byte) * 0x100000001b3 mod 2^64` for each key byte. The shard is `h mod N`. The distributed backend instead hashes a 16-byte operation ID and places a whole batch on one owner; overwrites can move a key to another owner. Distinct uniform keys have the same statistical load per instance, but static key routing does not reproduce distributed placement. For the harness keys with namespace `01010101010101010101010101010101`, seed 7 and logical IDs 0 through 4095, the five key-shard counts are exactly 819, 821, 816, 848 and 792.

This control separates the native WAL count from the SSD count only when controller evidence proves the SSD backing. Use instances on the same filesystem (checked by device number); the SSD backing needs controller device-admission evidence, which a qualification-only run does not record. The adapter records the directory device numbers at open and reports `same_filesystem=false` when they differ. Set the same total harness `execution.concurrency` as the distributed case: for the c16 comparison, five Db instances share 16 caller threads, not 16 threads each. `native_shards` does not create extra caller threads or assign a fixed writer count to each instance. The instance count and total writers can match the distributed setup; instantaneous writer placement does not. There is no data-path network, and native index, relocation and persistence calls remain different. This is not a replica of the distributed topology or proof of a five-SSD result.

`get`, `put` and `delete` route to one instance; `get_many` routes each key and preserves input order, duplicates and missing results. With N greater than 1, every `put_batch` call returns an unsupported error because cross-shard atomicity is not provided. Write-profile arm, scalar profiled writes and profile finalization also return unsupported errors; profiles remain single-instance. Each group barrier covers only its own Db. A poisoned group barrier does not poison another instance.

For N greater than 1, engine metrics use one private Prometheus registry per instance and are not exported. The four group-barrier metric families remain in the caller's registry, with constant label `shard` set to decimal values `0` through `N-1`. Sum only additive counters and histogram counts or sums across instances; a histogram bucket bound is not an exact maximum. With N equal to 1, group-barrier names and labels are unchanged and have no `shard` label. The topology receipt reports `native_shards`, `shard_paths` and `routing`; with multiple instances it adds observed `shard_filesystem_device_ids` and `same_filesystem`. Filesystem identity does not prove a common physical SSD. `backing_block_device` is `not verified by the adapter`; a campaign run with device admission records the SSD mounts. The configuration records the admitted instance count.

## Write diagnostic inventory

Write diagnostic inventory and sampled scalar stages require `native_shards=1`.

Native `BackendInfo.configuration` reports the base preset, selected `native_wal_geometry`, Value-WAL fragment and file sizes, map limit, effective relocation batch limit, dirty-key and snapshot thresholds, unload jitter, relocation reclaim limit, and direct-I/O setting. The base is `Config::default()`; its WAL geometry is 128 MiB fragments and 10 GiB files. With 16 maps, the configured fragment map window is 2 GiB, not an assertion of resident or allocated memory. This is not a claim that these settings were used by the paper.

When `diagnostics.write_profile_limit` is nonzero, the exported `write-diagnostics.json` includes the Value-WAL registry inventory before measurement dispatch and after measurement and the optional completion gate. Each inventory reports availability, file count and total logical file length, or an error rather than zero on failure. File lengths can include preallocated space; they are not dirty bytes, physical allocation, bytes synchronized, or device I/O. Registry and file lengths are sampled sequentially, and background maintenance can continue between boundaries.

For selected scalar operations with `value_wal_barrier`, the profile also reports the minimum and maximum registered-file count from the same registry snapshots used by those sampled barriers. These are limited to the selected operations, not a whole-case time series. Existing file and namespace call counts and aggregate clocks remain scoped to those selected operations. No additional inventory or profiling locks are added to unprofiled writes.

With `value_wal_group_barrier`, selected scalar writes still report `wal_append_ns` and `index_apply_ns`. The shared barrier is not profiled per operation. `value_wal_persistence.status` is `grouped`; `group_barrier` reports whole-case `runs`, `callers_covered`, exact `max_group_size`, and `errors`. File and namespace calls cannot be attributed to individual callers. Arming a write profile does not reset these counters. This mode does not report a per-operation persistence clock.

The optional native backend setting `native_wal_geometry` selects `"native_default"` (the default: 128 MiB fragments and 10 GiB files), `"small"` (1 MiB fragments and 4 MiB files), `"large_files"` (1 MiB fragments and 1 GiB files), or `"owner_matched"` (the distributed research owner's 4 MiB fragments and 1 GiB files). The normalized admitted native options include the selected name. `native_default` leaves `Config::default()` geometry unchanged; the others override only fragment and file sizes. The selected durability remains unchanged. The benchmark open checks persisted config sizes while holding the database lock, before opening either WAL, and rejects mismatches and nonempty paths lacking a config file. Ordinary `Db::open` behavior is unchanged. The config file is create-once, not a migration witness: use a fresh path for each cohort rather than assuming a matching old file proves its full geometry history.

These fields affect both value and index WALs. `large_files` changes only the file size relative to `small` geometry; `owner_matched` also changes fragment-derived limits. The distributed owner differs in topology, batching, and persistence behavior. Keep all intervention cohorts separate from historical and native-default results; a latency change alone does not identify its cause.

Historical cloud comparison cases used `native_default` with the unshared
per-call Value-WAL barrier. New concurrent durable comparisons can select
`value_wal_group_barrier`; keep the acknowledgement mode explicit. The separate
`native_async_write` pure-write cohort uses `page_cache` and the same workload as
primary, not the same durability boundary. The local performance campaign also
selects `native_default` and keeps
its existing `page_cache` boundary; local qualification explicitly selects
`small` geometry. Standalone configurations also default to `native_default`.
The earlier benchmark used the full `Config::small()` preset, including different
maintenance thresholds and relocation policy. The current `small` selects only
the same WAL geometry and is not a reproduction of the old preset: reproducing
that preset requires the frozen historical binary. Do not alter old data or
reinterpret the historical ABBA result. Intermediate `owner_matched` results are
also separate. Geometry alone does not match hardware,
background maintenance, workload history, or acknowledgment.

## Limits of the comparison

This is a one-process reference. A one-SSD comparison requires controller evidence of the SSD backing; the adapter does not establish independent-NVMe scaling or the backing block device. Native Tidehunter has WAL relocation and index-maintenance behavior; do not compare its reclaim policy to the distributed Value-WAL prototype as if their storage lifetimes were identical. The distributed owner backend uses bounded retained history and does not reclaim physical Value-WAL space. The paper's fill/cooldown/measurement experiment uses a 1-TB fill and ten-minute phases on a 6.4-TB NVMe host; the small local harness deliberately is not that experiment and must not be represented as a reproduction of its performance results.

Primary mechanism references: `tidehunter/src/db.rs` (`Db::insert`, `Db::remove`, `Db::get`, `persist_value_wal_for_benchmark`) and `tidehunter/src/batch.rs` (`WriteBatch::commit`); Tidehunter paper §§3 and 6.1, especially the 32-byte key baseline and fill/cooldown/measurement methodology in §6.1.
