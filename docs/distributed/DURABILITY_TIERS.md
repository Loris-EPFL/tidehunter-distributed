# Distributed write-durability tiers

Code: `tidehunter/src/distributed/durability/` (`mod.rs`, `frontier.rs`, `native.rs`, `synchronous.rs`, `tests.rs`).
No wire format and no disk format changes. `ReceiptDurability` is an in-memory label.

## Decision

Distributed Tidehunter offers two write-acknowledgement tiers. `native`, the contract of native Tidehunter (page-cache acknowledgement), is the default everywhere: in the engine (`DurabilityConfig::default`), in the kv-research adapter, and in the comparison campaign (`cloud_campaign.DEFAULT_DURABILITY_TIER`, used by a job or spec that names no tier). `synchronous` is the stricter option and stays available, unchanged, when selected explicitly; reviewed campaign specs generated with the earlier synchronous matrix default name it explicitly. The crash-loss difference: after an owner kernel crash or power loss, `native` can lose acknowledged batches above the owner's synced frontier, while `synchronous` loses none (see the failure matrix). Both tiers use one frontier tracker and one syncer per owner WAL. They differ only in the acknowledgement condition.

| | `native` (default) | `synchronous` |
|---|---|---|
| Config label (`DurabilityTier::as_str`) | `native` | `synchronous` |
| Receipt label (`ReceiptDurability::as_str`) | `appended` | `synced` |
| Acknowledgement condition | owner **appended** frontier covers the batch | owner **synced** frontier covers the batch |
| Waits for fsync | no | yes (group commit) |
| Background fsync | yes, native cadence: when an owner WAL fragment is finalized (appended frontier crosses a fragment boundary), no age bound by default; at once on demand | same syncer; every acknowledgement is an on-demand trigger |
| Explicit flush | `native::flush`, `frontier::flush_all` | same; returns at once when nothing is unsynced |
| Matched benchmark cohort (`comparison_cohort`) | `native_pagecache` | `value_wal_barrier_and_valkey_appendfsync_always` |

Native source of the `native` contract: Tidehunter paper §3.1 and §5 (`papers/parsed/Tidehunter-2026-v2.md`). The writer allocates a WAL position, copies into the mmap WAL (page cache), updates the index, and completes without waiting for fsync. A tracker advances the highest WAL offset below which every entry is processed. A syncer batches fsyncs. Durability against an application crash is immediate. Durability against a kernel crash needs the fsync. Applications that need more call an explicit flush.

## Frontiers

Positions are owner-WAL byte offsets. A completed range is `[start, end)`; a frontier is an exclusive end.

* **Appended frontier** (`Frontier::appended`): the highest offset below which every position is completely written. Writers complete in any order (`Frontier::complete(start, end, batches)`); a completion that does not start at the frontier waits in a sorted, bounded buffer until the gap before it closes. Callers that already serialize appends use `publish_appended(end, batches)`. This is the paper tracker.
* **Synced frontier** (`Frontier::synced`): the highest offset below which every position is durable. Only the syncer advances it, after its fsync callback returned `Ok`.
* **Syncer** (`Syncer::spawn`): one thread per owner WAL. It captures the whole appended frontier, calls `sync(end)` once without any lock, then publishes the synced frontier and wakes the waiters. Every batch that completed during the fsync is covered by the next call. Group commit needs no per-request queue.
* **Cadence** (`SyncPolicy`, default `SyncPolicy::native(fragment_bytes)`): the syncer runs when the appended frontier is past a fragment boundary above the synced frontier, as native Tidehunter syncs a WAL map when it is finalized (paper §3.1, no other trigger). `fragment_bytes` is the owner storage `fragment_bytes`; pass it to `DurabilityConfig::frontier_config(start, fragment_bytes)`. The syncer also runs at once when any waiter exists. `max_unsynced_age_ms: Option<u64>` (config key `max_unsynced_age_ms`, default `None`) is an opt-in extra sync trigger that is stronger than native, not a guaranteed bound (see the failure matrix); it is not part of the native-matched contract, so a run that sets it is not a native-matched run and must say so.
* **Waiters** block on condition variables: `wait_appended` (Native acknowledgement), `wait_synced` (Synchronous acknowledgement; raises demand so the syncer starts at once), `request_sync` + `wait_flush` (flush).
* **Flush**: `native::flush(frontier, deadline)` takes the appended frontier at the call, requests a sync of it, and waits until the synced frontier reaches it. It therefore covers everything acknowledged before the call.
* **`flush_all(owners, deadline)`**: starts a flush on every owner (`FlushTarget::begin_flush`), then waits for all of them against one deadline (`finish_flush`). The fsyncs run in parallel on the owner syncers. The report has one result per owner; one failing owner does not hide the others. The coordinator implements `FlushTarget` for a remote owner client (begin = send the request, finish = await the reply).

## Recovery invariant

> The coordinator stable checkpoint frontier and any persisted index image may reference only batches inside each owner's **synced** frontier.

Reason. A checkpoint lets recovery skip WAL replay below it. If a checkpoint named an appended-but-unsynced batch and the owner then lost that batch in a kernel crash, the index would point at bytes that do not exist. This is the `AcknowledgeBeforeBarrier` fault of `group_model.rs`, moved from the acknowledgement to the checkpoint. The rule holds for both tiers: a Native acknowledgement may be ahead of the synced frontier, a checkpoint may not.

**Capture rule (required because the default cadence has no age bound, so the checkpoint lag is unbounded).** A checkpoint or index-image capture must call `request_sync` (`FlushTarget::begin_flush`) on every owner it references, wait for the tickets (`finish_flush`, or `flush_all`), and cut at the synced frontiers that result. It must never cut at a synced frontier read earlier without asking. Cutting at `synced()` alone is safe but may be arbitrarily stale.

How code enforces it. `Frontier::synced()` is a lock-free `Acquire` read of an owner-local offset stored with `Release` after the fsync returned. Offsets from different owners are not comparable; their minimum is not a logical checkpoint cut. The coordinator captures its stable logical cut, groups its receipts by `(partition, epoch)`, and requests owner checkpoint proofs before persisting the index image. Each owner flushes before issuing its proof. The coordinator checks each proof's prefix count, retained count and digest against the captured receipt history. Tests: `seeded_crash_model_checkpoints_reference_only_synced_positions` (300 seeds, device persists before the frontier publishes, crash discards unsynced bytes; a control that cuts at the appended frontier must violate the rule, so the check is not vacuous), `concurrent_writers_keep_acknowledgements_and_checkpoints_behind_the_device`, and `native_checkpoint_proof_is_issued_only_after_its_prefix_is_synced`.

## Failure matrix

`Appended` = in the owner page cache with every earlier batch. `Synced` = covered by a completed fsync. Rows for the owner assume the owner WAL is a local file; the NVMe-oF row covers the live path where the owner writes the WAL through an NVMe-oF initiator.

| Failure | `native` | `synchronous` |
|---|---|---|
| Owner process crash (kernel up) | Every acknowledged batch survives in the page cache and is recovered; the frontiers restart at the recovered end. Same as native Tidehunter. Local WAL: recovery replays the tail above the last durable root. Live NVMe-oF WAL (`storage.rs`): recovery scans strictly below the persisted live frontier root as before, and beyond it adopts complete valid frames and stops quietly at the first missing or invalid frame, then installs a new root before the history root advances. The Native live acknowledgement requires the contiguous appended frontier, so no acknowledged frame lies beyond a hole. The live root is written only by the syncer barrier. Tests: `storage/tests/live_native.rs`. | Same, and every acknowledged batch is also below the root. |
| Owner kernel crash or power loss | Batches above the synced frontier can be lost, although they were acknowledged. Window: everything above the synced frontier. The fragment trigger and the opt-in age trigger decide when the syncer *starts* an fsync; neither is a hard bound on the loss window. The syncer cannot interrupt an fsync in flight, so scheduling delay, I/O delay, or a stalled fsync lets acknowledged writes accumulate past one fragment or past the age, and no backpressure limits that. In normal operation the unsynced suffix is about one fragment, as in native Tidehunter, which has the same property. Checkpoints never reference them (invariant), so recovery is consistent, not complete. | No acknowledged batch is lost. |
| Coordinator crash | Owner WALs hold every acknowledged batch. Recovery loads the last checkpoint (synced batches only) and replays the owner tails. A kernel crash of an owner at the same time loses its unsynced tail as above. | Same, without that loss. |
| NVMe-oF target crash or target host kernel crash (live WAL path) | The initiator write completion and the target page cache (`buffered_io=1`) are not durability; batches not yet covered by the owner's persist barrier can be lost. Reads of the sealed and live route fail until the target restarts and the owner fences and recovers. [INFERENCE: loss window equals the owner's unsynced window; it depends on the barrier reaching the target backing file, see `experiments/nvmeof/LIVE_WAL.md`.] | A receipt exists only after the barrier that LIVE_WAL.md requires, so acknowledged batches survive. |
| fsync error | Sticky. The frontier fails; waiters get `FrontierError::Failed`; completions are refused. The owner must reopen and recover. A retry is never issued, because a failed fsync can drop dirty pages and a later success would not prove durability. | Same; no receipt for data that may be lost. |
| Owner crash with RF1 | Not tolerated for data on a lost disk. RF1 is one durable owner, not failover. | Same. |

Neither tier provides cross-batch transactions or failover.

## Counters (`Frontier::stats`)

Lag between appended and synced (unbounded by design under the native cadence; watch it): `lag_bytes`, `lag_batches`, `lag_age` now, and `peak_lag_*` at the start of any fsync. Syncer: `fsync_calls`, `fsync_failures`, `fsync_bytes`, `fsync_batches`, `max_batches_per_fsync`, `batches_per_fsync()`. Waits that blocked, with blocked time: `appended_waits`, `synced_waits`, `flush_requests`, `flush_waits`, `flush_wait_ns`, `flush_wait_max_ns`. Completion buffer: `pending_completions`, `peak_pending_completions`, `completion_full_waits`. `lag_age` of the oldest unsynced position overestimates the age of positions appended during an fsync (they count from the fsync start). Use `peak_lag_*` and `lag_*` to see whether the triggers keep up; nothing in this module limits lag.

## Bounds and queues

| Item | Owner | Capacity | Full policy | Shutdown |
|---|---|---|---|---|
| Out-of-order completion buffer | `Frontier` | `max_pending_completions` (default 4096, 24 bytes each) | wait; the completion at the frontier never needs a slot, so the lowest unfinished writer always progresses | `stop`/`fail` wake blocked writers with an error |
| Waiters | condition variables | one per caller thread | not queued per request | `stop` releases waiters that cannot be satisfied (`Stopped`) |
| Syncer | one thread per owner | one fsync in flight | later completions join the next fsync | `Syncer::shutdown`/drop: stop, one final fsync of the appended frontier, join |

Locking. One `parking_lot::Mutex` per frontier (one per owner WAL, so the lock is sharded by owner), no I/O and no allocation inside it, and it is the only lock in the module. The fsync callback runs with it released. The frontiers are also mirrored in two atomics for lock-free reads.

## Residual serialization

* One mutex per owner is taken by each completion, wait, and syncer capture. The critical sections are a few loads and stores. With the owner append already serialized under the partition state lock, this adds no new contention domain. [INFERENCE: not measured; no benchmark was run.]
* One syncer thread per owner WAL: at most one fsync in flight per owner. A Synchronous acknowledgement that arrives during an fsync waits for that fsync and for the next one (about two fsync times when no fsync stalls; a stalled fsync delays every waiter behind it without limit). More fsync parallelism per owner needs a different WAL layout, not this module.
* A Synchronous acknowledgement waits for the contiguous predecessors to complete first (the frontier is contiguous). A Native acknowledgement waits for the same predecessors, but not for any fsync.
* Different owners sync in parallel; `flush_all` takes the slowest owner, not the sum.

## Benchmark pairing

Compare each tier only with its cohort. Never rank across cohorts.

* `native` with the native `page_cache` cohort (`native_pagecache`; the `native_async_write` pure-write cohort uses the same `page_cache` boundary). Distributed `native` still adds the owner hop, so it is not a no-network equivalent.
* `synchronous` with native `value_wal_barrier` and Valkey `appendfsync always`.
* State the tier next to every number. The harness refuses a run whose backend receipt `configuration.durability_tier` differs from the requested tier.
