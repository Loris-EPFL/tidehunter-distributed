# Distributed write-durability tiers

Code: `tidehunter/src/distributed/durability/` (`mod.rs`, `frontier.rs`, `native.rs`, `synchronous.rs`, `tests.rs`) and, for durable-prefix recovery, `tidehunter/src/distributed/remote_db/durable_cut.rs`.
`ReceiptDurability` is an in-memory label. Durable-prefix recovery adds one coordinator file (`durable-cut.v1`, see [Native recovery](#native-recovery-durable-prefix)) and the owner `DurableProgress` request (protocol 27).

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

## Native recovery: durable prefix

Config: `RemoteDatabaseConfig::durable_cut` (`DurableCutConfig`; absent means the default). `recovery`: `durable_prefix` (default) or `per_owner` (legacy). `cut`: `extend` (default) or `strict`. `lag_ms` (default 1000) and `lag_bytes` (default 64 MiB) set the nudge.

Problem. One native WAL loses at most a suffix after a power loss (paper §3.1). With one WAL per owner, each owner can lose a different suffix: per-owner recovery can keep `{x=2}` (version 1, owner B, synced) and lose `{x=1, y=1}` (version 0, owner A, unsynced). The recovered state is then not a state the database ever had.

Contract (`durable_prefix`). Recovery keeps an exact prefix of the coordinator version order. It contains every batch at or below the persisted cut and every batch covered by an explicit flush (`flush_durable`) that returned. A power loss of any owner loses at most a suffix of that order. Batches above the cut can be lost even if they were acknowledged and survived on their owner; this is the Native acknowledgement contract (page cache, not fsync).

Mechanism.

* **Cut.** For the serving epoch, `cut = min(published floor, synced end of every owner that holds a receipted batch not covered by its last report)`. The published floor is the exclusive contiguous publication frontier; aborted versions below it count as covered. An owner whose receipted batches are all covered by its last report is idle and does not pin the cut. The minimum is O(owners) and runs at report cadence on the actor thread, not on the write path. The write path stores one atomic per receipt group and one per publication call.
* **Progress.** One poller thread per owner (`value-wal-durable-{partition}`) uses its own plain connection (counted in `NON_READ_CONNECTIONS`). While its owner holds uncovered receipts, it long-polls `DurableProgress` (wait `min(lag_ms / 4, 5 s)`); the owner replies after each durable advance or at the wait. An idle owner is not polled. `synced` in the reply covers every batch of the stream at or below it (data, namespace and live root synced).
* **Nudge.** The actor (`value-wal-durable-cut`) keeps bounded samples of the published floor. When the oldest floor above the cut is `lag_ms` old, or `lag_bytes` were appended above the cut, it asks every lagging owner (uncovered receipts, synced end below the floor) to sync at its next poll (`sync = true`; the owner self-adopts a leased extent first). The nudge is a sync trigger, not a hard bound: the poll in flight (up to `lag_ms / 4`) and the fsync add to the lag.
* **Record.** `durable-cut.v1` in the authority directory: CRC-framed YAML (`format: 1`, `database`, `live: {epoch, next}`, `owners: [{partition, boot_id, synced_next}]`, `truncations: [{epoch, next}]`), installed by temp file, fsync, rename, directory fsync. `live` names the tracked epoch and its persisted cut; only the actor rewrites it while the database serves, and only when the cut moved. `owners` holds one continuity mark per owner of the tracked epoch: the owner's kernel boot id at the epoch's fence and its highest reported synced end. Marks are written with the cut, so they can lag the reports; a lagging mark only lowers the continuity bound. A report with another boot id zeroes the mark's boot id. `flush_durable` sends the flushed prefix to the actor and returns after the record covers it.
* **Owner continuity.** The owner reads `/proc/sys/kernel/random/boot_id` once at open and returns it in `NodeStatus::boot_id` and in every `DurableProgressReport`. `NodeStatus::recovery_clean` is false when a WAL recovery that this owner process ran (at open or at a live fence) ended early at a lost, torn or invalid frame and a later fragment still starts with a valid frame: a hole below the recovered tail. A damaged or unwritten frame at the very tail is the normal crash tail and keeps it clean. Frames after a defect inside the same fragment cannot be located without scanning user bytes for magic, so only later fragments are probed. An owner is continuous when its fence `boot_id` equals its mark and `recovery_clean` is true: it kept its page cache, so it lost nothing it appended. The continuity bound is the minimum recorded `synced_next` over the owners that are not continuous (unbounded when all are; 0 when an owner has no mark).
* **Recovery.** For the previously tracked epoch, the bound starts at `max(live.next, checkpoint cut + 1)`. `strict` keeps only batches below it. `extend` also keeps the run of surviving sequences from the bound. A missing sequence below the continuity bound was never written (an aborted version, or one that never reached a continuous owner): the run skips it. A missing sequence at or above the bound may have been lost by an owner that is not continuous: the run stops there. Fencing synced every surviving batch, so the run is durable and the result is still an exact prefix. Recovery appends `{epoch, next}` to `truncations` and persists the record before the new epoch activates, so a discarded batch never returns. Every open honours every recorded truncation, in any mode. Each tracked epoch has at most one entry, so the list is bounded by `max_epochs`. The new tracked epoch starts with one mark per fenced owner (its fence boot id, `synced_next = 0`).
* **Scope.** An epoch is tracked only when `recovery = durable_prefix`, the history persistence is `wal_replay`, and every owner reports the `native` tier at fence time. Synchronous owners are per-batch durable and are never truncated. The on-disk receipt history proves complete old-epoch prefixes, which a truncated owner stream cannot, so `on_disk` history keeps per-owner recovery. A `per_owner` open keeps the tracked epoch whole and clears `live`. Without a record and without tracking, recovery is exactly the per-owner recovery.
* **Stats.** `RemoteDatabase::durable_cut_stats()`: `enabled`, `persisted_next`, `reports`, `nudges`, `persists`, `persist_failures`, `poll_failures`, `certifications`. `RecoveryStats`: `cut_discarded_batches`, `cut_extended_batches`, `cut_skipped_versions`, `cut_continuity_bound`, `recovered_cut`. A failed record write is counted and retried at the next evaluation; the record then lags, which only makes recovery keep less.
* **Shutdown.** The service stops after the shutdown checkpoint. That checkpoint's owner syncs end every parked poll; without it a parked poll returns within `lag_ms / 4` (at most 5 s).

## Failure matrix

`Appended` = in the owner page cache with every earlier batch. `Synced` = covered by a completed fsync. Rows for the owner assume the owner WAL is a local file; the NVMe-oF row covers the live path where the owner writes the WAL through an NVMe-oF initiator.

| Failure | `native` | `synchronous` |
|---|---|---|
| Owner process crash (kernel up) | Every acknowledged batch survives in the page cache and is recovered; the frontiers restart at the recovered end. Same as native Tidehunter. Local WAL: recovery replays the tail above the last durable root. Live NVMe-oF WAL (`storage.rs`): recovery scans strictly below the persisted live frontier root as before, and beyond it adopts complete valid frames and stops quietly at the first missing or invalid frame, then installs a new root before the history root advances. The Native live acknowledgement requires the contiguous appended frontier, so no acknowledged frame lies beyond a hole. The live root is written only by the syncer barrier. Tests: `storage/tests/live_native.rs`. | Same, and every acknowledged batch is also below the root. |
| Owner kernel crash or power loss | Batches above the synced frontier can be lost, although they were acknowledged. Window: everything above the synced frontier. The fragment trigger and the opt-in age trigger decide when the syncer *starts* an fsync; neither is a hard bound on the loss window. The syncer cannot interrupt an fsync in flight, so scheduling delay, I/O delay, or a stalled fsync lets acknowledged writes accumulate past one fragment or past the age, and no backpressure limits that. In normal operation the unsynced suffix is about one fragment, as in native Tidehunter, which has the same property. Checkpoints never reference them (invariant), so recovery is consistent, not complete. With `durable_prefix` (default) the loss is a suffix of the coordinator version order: batches of other owners above the first lost version are discarded too. With `per_owner` each owner loses its own suffix. | No acknowledged batch is lost. |
| Coordinator crash | Owner WALs hold every acknowledged batch. Recovery loads the last checkpoint (synced batches only) and replays the owner tails. `durable_prefix` + `extend` (default): when every owner kept its boot and recovered without a hole, every surviving batch is kept and a missing (aborted or unwritten) version is skipped. Otherwise a missing version at or above the continuity bound stops the run, and an acknowledged batch above it is discarded unless the persisted cut or a flush covered it. `strict` keeps only the persisted cut. `per_owner` keeps every surviving batch. A kernel crash of an owner at the same time loses its unsynced tail as above. | Same as `per_owner`, without that loss: never truncated at the cut. |
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
| Durable-cut inbox (reports, flush certifications, stop) | actor `value-wal-durable-cut` | owners + 8 messages; at most `owners + 8` drained per evaluation | senders wait; no sender holds a lock | sent `Stop` after every poller joined; exits on it or on disconnect |
| Durable-cut pollers | one thread per owner | one long poll in flight per owner | not queued | stop channel (`try_send`), join; a parked poll returns at the next owner sync or within `lag_ms / 4` (at most 5 s) |
| Lag samples | actor | 256 | the newest sample absorbs new floors and keeps its time (age only overestimated) | dropped with the actor |

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
