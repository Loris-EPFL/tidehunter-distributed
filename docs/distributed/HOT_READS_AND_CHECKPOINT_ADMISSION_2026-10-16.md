# Hot reads and checkpoint admission

Date: 16 October 2026; amended 18 October 2026.

## Decision

The 16 October change keeps complete values on the existing owner-local Value WALs. Improve bounded coordinator residency, reuse decoded Sui summaries, and remove avoidable foreground checkpoint I/O. That cache change does not alter wire formats, durable acknowledgement, consecutive publication, replication, consensus, or WAL ownership. The separate 17 October write/transport implementation below preserves these storage guarantees but adds an explicitly selected EFA-direct packet format. AWS campaign approval and execution remain separate.

The reference hardware campaign is `th-8e4b69e7c9d2b99f`, source archive `8f07aa9c52ea2586e6736e8961eae5cb6637d847d99975078a6505b5bf2494a5`. Its coordinator-cache-hit p50 was 0.710 microseconds; remote warm reads and durable-write tails are separate populations. The full evidence is in `../../../checkpoint-work/2026-10-16-targeted-metadata-residency/REPORT.md`.

**16 October local cache outcome (corrected source `6d4f…6682`):** decoded sequence reuse is observed (1,733 TCP hits / 1,777 backend gets, versus first-run 6 / 3,510), and ordinary content warming is restored. Corrected TCP replay took 15.752764 s versus paired native 18.245472 s, but was slower than the original 13.862170-s TCP baseline; these sequential shared-SSD controls do not establish cluster performance or a causal speedup. Raw KV remote-miss and bulk/durability-tail gaps remain. The source history and all three controls are retained below rather than treating the failed first cache policy as a successful result.

**Subsequent cloud evidence:** the [17 October completed cloud campaign report](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/REPORT.md) covers two corrected-source six-host fleets with verified hardware EFA, complete KV/Sui populations, audits, costs and verified resource closure. It confirms cache reuse but **not overall native parity**: clean Sui remains 1.58–1.94× paired native and matched old-source elapsed is modestly slower. Those cloud results are separate from every local timing below.

## Implementation boundaries

### Distributed cache

`src/distributed/remote_db/read_cache.rs` and its read/publication callers own derivative, exact-reference copies. Avoid repeated reference hashing and unnecessary key-owner clones on point hits. Protect useful small checkpoint metadata from low-reuse write warming and bulk values without increasing the configured total byte budget. Use existing keyspace names/identities, not guessed numeric IDs. Generic deployments retain explicit/default policy semantics; Sui-specific policy is selected in `src/distributed/sui.rs`.

Existing second chance, bounded admission history, verified post-publication warming, single-flight and scan no-fill are already implemented. Do not add duplicate versions of them. No new dependency or unsafe cache implementation is needed.

### Sui decoded summaries

Start with the existing `sui-core` checkpoint store. Preserve public owned-checkpoint APIs, the stored checkpoint format, and the generic `DBMap` type. Separate demand-reused sequence summaries from the producer's eight-entry recent-digest/watermark window so unrelated inserts cannot continually evict execution's summaries. Bound retained canonical bytes and refill-token metadata, not residency by an arbitrary independent row count; do not size a cache to hold the entire fixture. Avoid holding a summary-read lock over durable I/O.

The cache must distinguish sequence-table and digest-table authority. Protect in-flight fills against same-key overwrite, reset, pruning and failed/ambiguous writes. An unrelated append must not invalidate every useful resident summary. Do not weaken the existing watermark/mutation serialization merely by releasing its mutex around writes. Shared-object interfaces are justified only if required by measured clone cost; do not change `VerifiedCheckpoint` globally.

### Checkpoint I/O

`src/distributed/remote_db/checkpoint_lifecycle.rs` already has a background worker and starts at half the tail bound. The avoidable candidate is synchronous staging-intent persistence before that worker starts. If the manifest-writer inventory proves exclusivity, move that persistence into the existing worker, before any checkpoint/history/index output can be written. Preserve old-root recovery, intent-before-output ordering, adoption ordering, bounded admission and worker joining before releasing the directory lock. Do not increase replay limits or remove the publication gate to hide stalls.

## Sources and exact applicable mechanisms

| Source | Mechanism and application | Boundary |
|---|---|---|
| [Tidehunter v2](https://arxiv.org/abs/2602.01873v2), sections 3.1–3.4 and 4.1–4.4 | Local resident-value fast path; immutable WAL identity; independent index/background work; recovery and relocation frontiers | A cached result is not a durability acknowledgement or a new authority |
| [FASTER](https://doi.org/10.1145/3183713.3196898), Appendix D, and [F2 v3](https://arxiv.org/abs/2305.01516v3), section 7 | Bounded derivative copies of read-hot records backed by log identity | Mutable latest-key invalidation cannot replace captured-cut/version checks |
| [MICA](https://www.usenix.org/system/files/conference/nsdi14/nsdi14-paper-lim.pdf), sections 4.1 and 4.3 | Reduce shared hit bookkeeping and unnecessary per-request work | Its lossy cache is not a replacement for our authoritative index or durability protocol |
| [RocksDB HyperClockCache](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/cache.h) and [CacheLib pools](https://cachelib.org/docs/Cache_Library_User_Guides/Partition_cache_into_pools/) | Bounded eviction work and workload protection inside one existing byte cap | No blind C++ cache import or additional per-CF capacity; a protected reserve can leave capacity unused |
| [S3-FIFO](https://jasony.me/publication/sosp23-s3fifo.pdf), sections 4.1–4.3 | Distinguish probationary streaming entries from demand-reused entries | Existing CLOCK/reject-first work is not missing; no assumption its published speedup transfers to Sui |
| [ReFlex](https://people.ucsc.edu/~hlitz/papers/reflex.pdf), sections 3–4 | Keep demand execution free of avoidable blocking control work; distinguish queueing from storage service | Do not remove persistence, authentication or record verification to improve a timing |

## Implemented source map

Paths below are relative to their named repository, not to the old middleware. This table describes the 16 October corrected cache source `6d4f6b04…6682`, not the later write/transport changes. The historical first implementation (`7cbc40be…b290`) and diagnostic (`9c1f4f2d…0fcd`) are labeled separately below; their old policies and timings must not be attributed to the corrected build.

| Repository and source | Exact change | Problem and source connection |
|---|---|---|
| Tidehunter fork: `tidehunter/src/distributed/remote_db/read_cache.rs` | `get_point` validates the complete immutable reference, keyspace and requested key, then clones only the value owner. Preparation carries one reference fingerprint into insertion. | Remove repeated hashing and key-owner clones from resident scalar reads; Tidehunter resident reads and MICA's reduction of shared fast-path work. |
| Tidehunter fork: `tidehunter/src/distributed/remote_db/{pending_read.rs,pending_read/concurrent.rs}` | Scalar read callers use the value-only result, with their existing authority checks before returning. | A faster derivative hit must retain the same publication-cut and failure checks; Tidehunter/FASTER/F2 log identity is not replaced by a latest-key-only cache. |
| Tidehunter fork: `tidehunter/src/distributed/remote_db.rs`, `tidehunter/src/distributed/sui.rs`, `tidehunter/src/distributed/remote_db/read_cache.rs` | `configure_sui_read_cache` constructs private `ReadCachePolicy::for_sui_shape` from live, non-destroyed keyspace names and applies `ReadCache::configure_policy` before concurrent serving. Clearing the cache preserves policy. | Protect small checkpoint metadata without hard-coding keyspace IDs or imposing Sui policy on generic databases. |
| Tidehunter fork: `tidehunter/src/distributed/remote_db/read_cache.rs` | One quarter of the existing payload allowance is reserved for certified summaries. Protected entries may borrow the remainder; ordinary demand can reclaim borrowed space above the floor. When the certified table exists, post-publication warming skips only `checkpoint_by_digest`, `checkpoint_sequence_by_contents_digest` and `watermarks`; content/full-v2/ordinary warming remains enabled, and all keyspaces retain demand-fill eligibility. | CacheLib pool isolation and S3-FIFO-inspired admission separation, narrowed after the first blanket policy harmed content reuse. This is a bounded adaptation, not an implementation of either project's cache. Generic databases retain ordinary warming. |
| Tidehunter fork: `tidehunter/src/distributed/remote_db/checkpoint_lifecycle.rs` | The existing background worker persists the staging manifest before it writes checkpoint outputs. Owner-thread adoption and shutdown joining remain in place. | Remove a manifest sync from checkpoint admission, not from durability; Tidehunter background index work and ReFlex's separation of control work from demand execution. |
| Sui: `crates/sui-core/src/checkpoints/control_cache.rs` | Independent demand-read LRU for owned decoded sequence summaries, bounded by 1 MiB of canonical serialized bytes with no separate row-count ceiling. Keep the producer's eight-entry digest/control window separate; at most 512 identity tokens fence delayed fills. | Repeated typed reads need not re-enter storage and decode BCS after unrelated producer appends. Byte-driven eviction avoids the diagnosed arbitrary-row-cap churn; serialized-byte accounting is not an RSS limit. |
| Sui: `crates/sui-core/src/checkpoints/mod.rs` | Sequence lookups use that demand cache. Same-key mutation invalidates before and after the durable write; unrelated keys keep their tokens. Parallel readers of one key share a token. Reset, epoch changes and failed/ambiguous writes clear affected cache authority. | Prevent stale refill and producer-driven eviction without changing checkpoint ownership, serialized data or verification. Digest-only pruning must not resurrect the digest table through a sequence-table hit. |
| Sui: `crates/sui-core/src/checkpoints/checkpoint_executor/metrics.rs` | Clarify the existing mutation-blocked refill counter's help text. | It now counts same-key mutation-blocked refills, not inability to acquire the old control mutex. Historical values must not be compared as lock-contention measurements. |
| Sui: `crates/typed-store/src/{tidehunter_util.rs,rocks/mod.rs}` | Borrow the serialized stored-key suffix for scalar reads and existence checks. Keep explicit ownership at batch, write and iterator-bound boundaries. | Remove a second key allocation/copy while preserving typed-store serialization and prefix reconstruction. This applies to both native and distributed Tidehunter adapters, not only the distributed comparison. |
| Tests: Tidehunter `remote_db/{read_cache.rs,lifecycle_tests.rs}`; Sui `checkpoints/mod.rs`, `typed-store/src/rocks/tests.rs` | Exercise admission before protected entries exist, protected borrowing/reclaim, exact-reference isolation, intent failure/recovery, delayed refill/overwrite/pruning, reset/failure and real prefixed-key reads/batches/bounds. | Consumer-visible boundaries, not source-text or enum-variant assertions. |

The Sui sequence path is `CheckpointStore::get_checkpoint_by_sequence_number` → `get_checkpoint_by_sequence_number_with_loader` → `SequenceReadCache::{get,begin_refill,canonical_bytes,cache_refill}`. `validate_control_cache_read` runs before lookup and again before a cached return, or after the backend load. `SequenceReadCache::{begin_mutation,finish_mutation,invalidate_sequence,clear}` invalidate both the row and its refill identity. `CheckpointStore::update_pruning_epoch` clears sequence residency when relocation eligibility changes. Token identity is compared with `Arc::ptr_eq`; eviction of a token only suppresses a delayed refill, not the underlying read. Sharing a token is not single-flight storage I/O: parallel misses can still load independently.

`transform_th_key` in typed-store now returns a borrowed slice of the already serialized key. The scalar adapter still performs its original serialization and prefix semantics; this removes the subsequent suffix copy, not every key allocation. `RemoteDatabase::{start_checkpoint,start_checkpoint_with_floor,progress_checkpoint,commit_checkpoint}` and `BackgroundCheckpoint::drop` delimit worker launch, adoption and joining. `ReadCache::{prepare_verified,prepare_post_publish,preflight,configure_policy}` and `ReadCachePolicy::{for_sui_shape,skips_post_publish_warming}` delimit verified demand admission, policy-selected warming and the protected floor.

The protected quarter is calculated **after fixed cache metadata is subtracted**, not as one quarter of the entire configured budget. Ordinary entries cannot occupy that reserved payload allowance even while no protected entry exists. Protected entries may borrow the ordinary allowance, subject to ordinary demand reclaiming borrowed bytes without crossing the floor. Thus policy can intentionally leave payload capacity unused or reject an ordinary value that would fit the total cache. Set associativity and bounded eviction work can also reject a candidate despite aggregate free space. This is workload protection, not a promise of full utilization or universally higher hit rate.

The decoded sequence cache's 1-MiB charge is canonical BCS row size, not decoded heap ownership, allocator overhead, LRU/token metadata, returned clones, transient backend results or process RSS. Positive canonical row sizes bound resident row count through that byte allowance; there is no independent row-count ceiling. The 512-token cap separately bounds refill metadata and does not pin cached summaries. The existing eight-entry producer cache retains its own canonical-byte allowance: separating sequence residency adds a separately bounded decoded cache; it does not make the two caches share one 1-MiB process-memory cap.

### Locks and correctness boundaries

- No mutex or lock was removed from inherited single-instance Tidehunter. No unsafe code or dependency was added.
- The distributed cache still uses its existing short mutex. Hash preparation avoids repeated work; post-publication policy and admission share one lock acquisition. No cache or index guard crosses a remote read.
- Sui's existing control mutex still serializes durable control mutations. The new sequence-read mutex does not cover typed-store I/O, canonical-size calculation or preparation of the refill clone. Owned API results still require cloning; this change does not introduce a shared-summary API.
- A cache entry is derivative state. Complete WAL reference, record identity, captured-cut and fail-stop checks remain authoritative.
- A failed staging-intent write must fence the owner and authorize no checkpoint outputs. The prior root and retained tail remain the recovery source.
- Native and distributed read/write acknowledgement, encryption, record verification, publication, replication and consensus semantics are unchanged. This change adds neither a global WAL nor direct remote-memory reads.
- The new lock is the independent Sui sequence-cache mutex. Distributed pending reads additionally use fixed per-slot `FlightSignal` wait mutexes/condition variables for completion retirement; these do not participate in resident cache hits. Existing distributed cache, index and Sui durable-mutation locks remain. No inherited native lock was removed.
- Cryptographic verification, peer authentication and encryption are unchanged. As before, a verified resident copy can delay discovery of subsequent on-disk corruption; this is not identical corruption-detection timing to a fresh WAL read. Complete-batch atomic publication and durable acknowledgement remain authoritative even when cache warming or checkpoint staging runs in the background.

## Verification contract

Record a fresh pre-change native/distributed replay from immutable source-bound binaries, then use the same fixture and cache budgets after rebuilding. Exercise bounded cache pressure, overwrite/delete/captured cuts, same-key delayed fills, reset/pruning, unknown writes, checkpoint interruption/recovery and successful real Sui state equality.

Local runs use five loopback owners on one shared NVMe SSD. Record owned-process swap and host memory pressure. Local ratios and software fabric cannot prove independent-drive scaling or hardware EFA latency. Keep raw hit/miss timing, typed Sui operations, clean/profiled populations, and complete durable-window tails separate. A speedup is accepted only with unchanged state audits and safety checks.

## Integration corrections

The full engine regression run exposed a synchronous scan failure under a one-member pending-byte allowance. A notifier's temporary completion owner could retain the last credit after the caller consumed a result. The next wave then saw transient backpressure with no request to finish.

`tidehunter/src/distributed/remote_db/pending_read.rs` now keeps result credit on the live `ReadFlight`, gates polling and blocking reads on a fixed per-slot `FlightSignal`, and publishes readiness only after notifier-owned payloads and credits retire. A slot is reusable only when it is ready and no live flight owns it. Readiness publication also advances the admission generation, preventing a waiter from sleeping after the last credit wake but before slot availability. Signal storage and completion metadata are reserved from the existing pending-byte allowance. No per-request notifier allocation or blanket wait on an externally held read handle was added.

This follows the existing native fill-cell and [CacheLib fill-context](https://github.com/facebook/CacheLib/blob/a24370092f0bd6fa7145b2f01c3e50e63edd6a6e/cachelib/allocator/nvmcache/NvmCache.h#L767-L848) ownership model, adapted to immutable WAL references and separately bounded request/result lifetimes. The per-slot wait mutex serves blocked completion waiters; cache hits do not enter it.

Review also found that a regular candidate larger than its policy allowance could age and evict useful entries even though it could never fit. Its regression observed four evictions before the fix. Cache preflight now rejects this candidate before hashing history, detaching byte owners or evicting entries. The existing total-budget check and protected-floor checks remain in place.

The new native prefixed-key regression uses the existing forward iterator's exclusive upper bound. No iterator-bound contract was changed. Old assertions about the former cache-counter default and an unused reserve in a disabled cache were removed rather than pinned to the new implementation.

The native constructor-recovery fixture also assumed that dropping the last Sui store synchronously drained Tidehunter workers. During local validation, immediate reopen hit the existing five-second open-lock limit. The fixture now drops the store and calls the existing `Database::wait_for_tidehunter_background_threads` API before reopening. Both constructor paths pass with this explicit lifetime boundary. No retry timeout, native shutdown implementation or production acknowledgement was changed.

## Historical first-implementation correctness checks (`7cbc`)

For the first implementation, the parent integration run reported 773 engine tests passed and 13 ignored (artifact `2775`), and strict engine Clippy `--lib --tests -D warnings` passed. Typed-store reported 34 passed, including the explicit native prefix/bounds/batch regression (artifact `2784`); the decoded-cache internal tests reported two passed. These historical integration observations are distinct from the corrected `6d4f` rerun below (773 engine, 17 checkpoint-store and two internal tests), not commands rerun for this documentation.

The retained [Sui checkpoint-store log](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/validation-sui-core.log) records **16 passed, zero failed**, including both-constructor recovery, delayed overwrite/pruned-digest refill, key-scoped tokens, producer-insert survival, byte eviction/oversize rejection and failure/reset boundaries. The unchanged `HashSet` import warning in `sui-execution/latest/sui-adapter/src/temporary_store.rs` is not a regression from this work; its baseline byte hash is `2662d8dcecc712ea799a4fb9fac36d678fbb2961ddea2e05351612bce4cc9853`. No full-Sui warning-free lint result is claimed. Skills files were absent, so no stale skill file was updated.

The retained [validation summary](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/validation-summary.json) records the feature/profile scopes and changed-file format checks. The engine test run preceded an equivalent nested-if-to-let-chain Clippy correction; it is not represented as a second complete engine test run after that rewrite. Not every typed-store test uses the native backend.

## Local comparison provenance

All current local evidence lives under `checkpoint-work/2026-10-16-cache-checkpoint-parity/`. The [baseline input receipt](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/baseline-inputs.json) binds the immutable pre-change build, cache shapes and successful reopen/state audits. Its successful replay is `baseline-sui-tcp-r1/`; the earlier `baseline-sui-tcp/` is a failed setup caused by the inherited libfabric search path and produced no distributed timing. The historical cloud/software-fabric report above remains historical evidence, not the current local control.

| Source-bound build identity | Pre-change | First measured implementation (`7cbc`) |
|---|---|---|
| Source archive SHA256 | `8f07aa9c52ea2586e6736e8961eae5cb6637d847d99975078a6505b5bf2494a5` | `7cbc40be577c09f427f8218640fb377c22de6e307d7551a8a61f90d8e546b290` |
| Build manifest SHA256 | `83df752d6d3cf249ba99d9206050972ebbe97e755ad60d5bf036483bf20b457a` | `f3f9dfb776d57a54f28b133d9702a318793e82e7a868a9fd2dfedbe0193b1a73` |

First-implementation [build-manifest.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/build-final/build-manifest.json) and [build-receipt.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/build-final/build-receipt.json) bind all six `7cbc` executables; its bundle SHA256 is `eee5452bb7588f495b17167a61172a7f6fbb9b0000d546e35af4b753275ed9d3`. The directory name `build-final` is historical; the current corrected build is `build-corrected`, identified below. Partial earlier build directories are not measurement sources. Documentation changes after source freeze do not change measured-code identity.

### Historical first-implementation KV before/after (`7cbc`)

Full populations, distributions, ratios, stage spans, cache/WAL counter deltas, physical-I/O/resource observations, hashes and raw-artifact paths are retained in [kv-comparison.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/kv-comparison.json) and [kv-comparison.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/kv-comparison.csv). Raw final cases are `after-{cache,bulk}-{native,tcp}/` under the work directory; the matching `baseline-*` cases and `baseline-{kv,stage}-analysis.json` are retained there too. All four final cases exited zero, recorded `COMPLETE`, and verified reopened keys (320 in the cache case and 102,400 in bulk).

| Population and sample unit | Before native | Before TCP | After native | After TCP |
|---|---:|---:|---:|---:|
| Cache-enabled point get, mean µs/operation; 4,096 samples/backend | 0.340177 | 0.550999 | 0.619424 | 0.801898 |
| Cache-enabled point get, p50 µs/operation | 0.156 | 0.511 | 0.317 | 0.768 |
| Cache-disabled bulk point get, mean µs/operation; 8,192 samples/backend | 2.554404 | 48.706 | 3.783513 | 93.584800 |
| Cache-disabled bulk point get, p50 µs/operation | 2.514 | 46.694 | 3.795 | 86.114 |
| Cache-disabled bulk point get, p99 µs/operation | 3.895 | 122.382 | 5.717 | 238.269 |
| Bulk load durable window, mean ms/complete window; 100 windows/backend | 9.514641 | 27.502764 | 14.775824 | 47.833213 |
| Bulk load TCP durable-window p99, ms; 100 windows | — | 135.900 | — | 314.095 |

Cache-enabled TCP scalar reads had 4,096 frontend hits, zero misses and zero owner WAL reads. Final TCP/native scalar-hit mean ratio was 1.295×: **this is local coordinator residency, not remote value retrieval**, and neither backend showed an absolute scalar-cache speedup over its saved control.

Cache-disabled bulk TCP scalar reads had 8,192 frontend misses, 8,192 owner WAL reads (67,502,080 logical WAL bytes) and 8,192 resident-read successes, with zero asynchronous submissions. These are remote application reads serviced from resident WAL pages, not 8,192 measured physical-NVMe misses. Their final mean was 24.735× native. Final per-key window means were 3.521761 µs native and 857.830529 µs TCP (243.580×), a separate admission-to-retirement population. **The remote miss/bulk gap is not closed.**

Four foreground-checkpoint load windows remained, as in the baseline. Their cumulative foreground-checkpoint span increased from 446.595 to 1,035.212 ms, and the complete-window tail also regressed. The background staging change preserves ordering but this run does not demonstrate lower checkpoint tails. Nested/overlapping checkpoint, dispatch and durability spans must not be added as wall-time percentages.

Every advisory-evicted cold phase reported 220,819,456 physical-read bytes native and 94,613,504 TCP. This establishes physical file-read activity in those intervals, not uniformly cold values or device service latency. Raw KV resource CSVs did not contain `VmSwap`: swap is **unmeasured**, not zero. Summed per-process RSS maxima are not a simultaneous host peak.

These are sequential, single-repetition controls on one shared SSD with five loopback owners, using matching campaign settings (8-KiB values; 8-MiB cache-enabled budget; bulk caches disabled; bulk 32-batch × 32-mutation windows and Sui-style WAL geometry). Native controls regressed too. The evidence supports neither a causal source-speedup claim nor a universal native-parity claim. No AWS, independent-drive scaling, hardware EFA or one-sided RDMA was measured.

### Historical first-implementation real Sui replay (`7cbc`)

Both final pairs replayed the same **1,776 real private-chain checkpoints (0–1775)**, terminal digest `EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`, full source-range SHA256 `896408bc8581e07473e11db805a69295286df17e05cfb90ed0952c45a9286f9f`. Full Tidehunter/RocksDB audits, table counts, digest and source range matched the baseline. This is actual Sui execution, not synthetic KV, but not mainnet or capacity-scaling evidence.

| Sequential replay pair | Native startup-inclusive s | Distributed startup-inclusive s | Distributed/native | Native completed-window s | Distributed completed-window s |
|---|---:|---:|---:|---:|---:|
| Pre-change TCP | 13.533814 | 13.862170 | 1.0243× | 12.740274 | 12.832594 |
| Final TCP | 21.002374 | 21.373966 | 1.0177× | 19.646518 | 19.543880 |
| Final software fabric/TCP | 27.090235 | 73.183551 | 2.7015× | 25.734552 | 61.704961 |

Startup-inclusive elapsed includes startup and terminal watermark observation, not process lifetime. Completed windows exclude startup and use 250-ms watermark sampling; neither is per-checkpoint latency. Final TCP was close to its paired native control, but native and TCP were respectively **1.552× and 1.542× slower** than their saved pre-change controls. A slightly closer ratio is not evidence of absolute replay speedup. The software-fabric native control also differed materially; do not attribute its distributed difference solely to transport.

| TCP checkpoint population | Before | After |
|---|---:|---:|
| Certified-keyspace frontend hits / misses | 2,151 / 1,392 | 3,472 / 37 |
| Certified typed-store backend gets / mean µs | 3,544 / 65.805529 | 3,510 / 53.699240 |
| Decoded sequence hits / backend gets | 4 / 3,544 | 6 / 3,510 |
| Aggregate checkpoint owner WAL reads / logical bytes | 3,365 / 13,215,162 | 3,626 / 19,547,097 |

Selective warming/protection substantially reduced certified-keyspace frontend misses in this run, but it did **not** eliminate backend wrapper calls, improve aggregate checkpoint owner WAL traffic, or produce substantial decoded-cache reuse. Final native decoded counts were four hits and 3,538 backend gets; final software-fabric distributed counts were three hits and 3,519 backend gets. The independent decoded cache fixes the observed eviction/refill correctness mechanisms; this fixture does not demonstrate that a 256-entry cache is sufficient to bridge execution's reuse distance.

The typed-store backend histogram **excludes decoded sequence-cache hits**. Its mean is not total checkpoint lookup latency or decoded-hit latency. Quantiles in the retained wrapper comparisons are histogram bucket intervals, not raw quantiles. No separate decoded-hit API latency distribution was measured. Final same-key mutation-blocked refill counts were zero, but historical busy-bypass counters measured different global-lock behavior; those zeroes do not establish elimination of all contention or signature-verification work.

Sampled owned TCP remote RSS peaked at 1,742,168,064 bytes and sampled `VmSwap` at zero; the host recorded four swap-in pages. Native TCP recorded 39 host swap-in pages. Software-fabric remote RSS peaked at **11,996,831,744 bytes** and sampled owned swap at **7,300,284,416 bytes**; its host interval recorded 167,030 swap-in and 917,526 swap-out pages. The software-fabric timing is therefore **not an isolated transport comparison**. Owned maxima cover replay/storage processes, not producer/auditor processes; host counters include unrelated processes.

Both final transports used 8 MiB frontend cache per DB and 16 MiB owner cache per DB partition, with all profiling limits zero. These are not equal-total-memory native/distributed budgets, and decoded Sui residency is separately bounded. Disabled diagnostic spans are unavailable, not zero.

Configuration applicability must use executable defaults and the effective run configuration, not nearby prose. In `sui/crates/sui-config/src/node.rs`, `default_checkpoint_execution_max_concurrency` returns **4**; the field comment claiming a default of 200 is stale. In `sui/crates/sui-config/src/p2p.rs`, `checkpoint_content_download_concurrency` defaults to 400 content tasks and `checkpoint_content_download_tx_concurrency` to 50,000 transactions. These are distinct pipeline limits, not interchangeable cache-residency requirements or evidence that a specific cache capacity is correct. The unrelated stale config comment was not changed in this work.

Full machine-readable evidence:

- [sui-comparison.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/sui-comparison.json): full receipts, source/build/fixture binding, audits, all metric populations, resources, comparisons and limits.
- [sui-comparison.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/sui-comparison.csv): six baseline/final mode summaries.
- [sui-comparison-wrapper-latencies.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/sui-comparison-wrapper-latencies.csv): 306 typed-wrapper populations.
- [sui-comparison-adapter-means.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/sui-comparison-adapter-means.csv): 1,404 stage populations, including unavailable/empty cases.
- Raw `after-sui-tcp/` and `after-sui-fabric/` receipts, summaries and evidence remain under `checkpoint-work/2026-10-16-cache-checkpoint-parity/`, alongside the successful `baseline-sui-tcp-r1/` and `baseline-analysis.json`.

The retained cleanup observation found no final-build processes and all 30 deployment ports available. No AWS resources were launched, no hardware EFA measurements were taken, and no commit or push was made in this local work.

## Accepted post-measurement policy correction

The first measured policy suppressed post-publication warming for every keyspace except `certified_checkpoints`. That also suppressed useful Sui content warming: retained TCP counts fell from baseline `checkpoint_content` 583 hits/1,194 misses and `full_checkpoint_content_v2` 599/1,176 to first-run 2/1,776 and 0/1,775. Protecting certified metadata did not justify treating every other table as low-reuse traffic.

The accepted live-source correction in `tidehunter/src/distributed/remote_db/read_cache.rs` introduces private, copyable `ReadCachePolicy` with an optional protected keyspace and a fixed `[u64; 4]` post-publication skip mask. `ReadCachePolicy::for_sui_shape` resolves live IDs once by name. Only when `certified_checkpoints` exists does it skip warming for `checkpoint_by_digest`, `checkpoint_sequence_by_contents_digest` and `watermarks`. It restores ordinary warming for `checkpoint_content`, `full_checkpoint_content_v2` and other tables. Verified demand fills remain eligible for **all** keyspaces; the protected quarter of the existing payload allowance is unchanged, and cache clearing preserves policy. Generic, epoch and perpetual shapes without the certified table retain their ordinary policy.

This is a logical admission-policy correction to the overbroad streaming-traffic classification, not a measured speedup or a claim that three tables are universally low-reuse. It narrows the CacheLib/S3-FIFO-inspired isolation mechanism to the actual Sui control tables rather than applying it by association to all non-certified data. **The `7cbc40be…b290` build and every timing above precede this correction.** Any subsequent results require their own source/build receipts; neither frozen snapshots nor earlier evidence were rewritten.

The live cache header also no longer claims the system has no GC: current-cut/pin-based whole-file retirement already exists. This comment correction adds no new retirement or durability mechanism.

## Diagnostic root cause and implemented byte-bound correction

The first-run decoded-cache result was not accepted as completion of the performance objective. A separate, fully audited diagnostic replay traced cache events **only before foreground metric capture**; producer/auditor traffic and later verification RPC events were excluded. [diagnostic-sui-summary.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/diagnostic-sui-summary.json) and [diagnostic-sui-analysis.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/diagnostic-sui-analysis.json) retain counter reconciliation, caller categories, sequence histories and eviction examples. Diagnostic source SHA256 is `9c1f4f2df42a7ed16f7a7dd724bb754721ab87b8f9117b038964c16154920fcd`; its INFO logging perturbs scheduling, so **none of its elapsed times is a clean performance measurement**.

The trace identifies two traversals: the shared `ReadStore` forwarding path (`sui-core/src/storage.rs:98–105`, with state-sync content traversal in `sui-network/src/state_sync/mod.rs:1734–1750`) and executor synced-watermark fallback (`sui-core/src/checkpoints/checkpoint_executor/mod.rs:319–335` through `notify_read_synced_checkpoint` to checkpoint-store sequence lookup). The forwarding site alone is not a unique outer caller or a captured backtrace.

| Diagnostic cache fact | Native | TCP |
|---|---:|---:|
| Executor fallback rereads missing after prior entry eviction | 1,762 / 1,762 | 1,765 / 1,765 |
| Stable resident high-water rows / canonical bytes | 256 / 64,576 | 256 / 64,576 |
| Entry-count evictions | 3,282 | 3,285 |
| Byte-pressure evictions | 0 | 0 |
| Median first-traversal sequence lead at executor lookup | 391 | 410 |

The arbitrary row cap evicted summaries well below the existing 1-MiB byte allowance before the executor revisited them. No same-sequence overlapping backend misses, stale-token rejection or mutation-blocked refill explained these executor misses. This supports a capacity-policy correction, **not** adding single-flight, changing pipeline concurrency or increasing the byte budget.

The implemented correction removes the 256-row limit from `SequenceReadCache`, retains the existing **1-MiB canonical-byte limit** and 512 key-scoped refill tokens, and drives LRU eviction by retained canonical bytes. `LruCache::unbounded` means no separate entry-count cap; it does not mean unbounded retained row bytes. Serialized checkpoint summaries contain mandatory integer fields and have positive canonical size, so the unchanged byte allowance also bounds resident row count. The eviction pass uses a snapshot of the current row count as its finite loop bound. This is neither a fixture-sized 5,000-row cache nor a coupling to full-content retention/state-sync defaults.

There is a direct native precedent in `tidehunter/src/decompress_cache.rs:56–64`: variable-sized bodies are bounded by bytes rather than entry count. The caches still have different mechanisms and accounting; the Sui canonical-byte allowance remains **not an RSS limit**, and this change does not modify native engine locks or decompression policy. Temporary logging/caller instrumentation was removed; the clean corrective replay below verifies its targeted events are absent.

The clean corrective source was frozen separately in `checkpoint-work/2026-10-16-cache-checkpoint-parity/build-corrected/`. Its [plan.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/build-corrected/plan.json) records source archive SHA256 `6d4f6b04fda3ada8e6f7bc6dd67cb53c21c990d22c5e30286e2eb9f93da46682` and source-manifest SHA256 `3419133eac97766c4dac75ba29fe49f1a80e3b5916d3795a484de1656e0b36f2`. It includes byte-bound sequence residency and the selective three-table warming mask. The completed six-Cargo-executable [build manifest](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/build-corrected/build-manifest.json) has SHA256 `569ec67cde2aa3fc045d4b64d5ee7877213198424d9f9214181282478477474b`; [build-receipt.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/build-corrected/build-receipt.json) binds bundle SHA256 `42890f8ed3f8c9ab879f01a6ee361b6c0f69192260677f1c276123484d933454`. Build completion alone is not a performance result; the earlier measured and diagnostic identities remain unchanged. Subsequent documentation-only edits do not alter the compiled source captured by this freeze.

Corrective validation reran on the current source: [validation-corrected-engine-r1.log](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/validation-corrected-engine-r1.log) records **773 passed, zero failed, 13 ignored** (137.87 s). The parent also reported successful release engine library/tests Clippy with `-D warnings` (artifact `2824`). [validation-corrected-sui-r1.log](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/validation-corrected-sui-r1.log) records **17 passed, zero failed** (51.65 s), including `sequence_read_cache_reuses_summaries_within_byte_budget` and the overwrite, pruning, failure, reset and explicit-drain recovery cases. The unchanged Sui `HashSet` warning remains; this is not full-Sui warning-free lint. These checks do not substitute for clean corrected replay measurements.

The [corrected validation summary](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/validation-corrected-summary.json) is now **complete for local implementation, fresh source-bound builds, matched KV and actual private-chain Sui replay**. It additionally records two decoded-cache internal tests passed using the exact current-source test executable and distinguishes interrupted earlier attempts from passes. Its 194-test offline cloud-runner check (three optional skips) is tooling validation, not cloud deployment or performance evidence, and does not cover the separately changed outer campaign planner. Corrected build and runtime receipts are linked separately here; cloud admission/results remain separate.

## Corrected-source local KV results

The completed [corrected-kv-comparison.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-kv-comparison.json) and [corrected-kv-comparison.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-kv-comparison.csv) retain all four corrected cases against **both** the original `8f07` baseline and first `7cbc` implementation. The CSV has 102 phase-comparison rows; JSON additionally retains stage/window/path-counter comparisons, raw artifact hashes, matching campaign settings and source/executable receipts. All four corrected cases exited zero with `COMPLETE` and reopened-key verification (320/102,400 keys, including fresh/reused TCP transport). These runs use the corrected `6d4f…6682` build, not either earlier binary set.

| Corrected population | Native | TCP | Sample unit |
|---|---:|---:|---|
| Cache scalar mean / p50 / p99, µs | 0.379654 / 0.176 / 2.057 | 0.647797 / 0.611 / 1.130 | 4,096 operations/backend |
| Cache-disabled bulk scalar mean / p50 / p99, µs | 2.741808 / 2.699 / 4.846 | 82.869679 / 59.296 / 801.821 | 8,192 operations/backend |
| Bulk durable-window mean / p50 / p99, ms | 17.064868 / 16.535318 / 24.197456 | 34.515994 / 23.790224 / 192.987868 | 100 complete windows/backend |
| Advisory-evicted scalar mean / p50 / p99, µs | 88.931707 / 86.199 / 221.024 | 166.737832 / 119.732 / 958.754 | 8,192 operations/backend |

TCP cache scalar reads remain entirely frontend-local (4,096 hits, zero misses/WAL reads). Corrected mean TCP/native is 1.706×; neither that ratio nor raw API timing establishes Sui decoded-hit latency. Cache-disabled bulk scalar reads remain 8,192 frontend misses and owner WAL reads, 67,502,080 logical WAL bytes, and 8,192 resident-read successes with zero asynchronous submissions: the corrected mean gap is **30.224×**, and the scalar p99 is worse than either earlier run. Advisory-evicted phases retain physical-read evidence of 220,819,456 native / 94,613,504 TCP bytes per phase, not proof of uniformly cold NVMe accesses.

TCP complete-window mean/p99 of 34.515994/192.987868 ms improved over first-implementation 47.833213/314.094794 ms but remain worse than original-baseline 27.502764/135.899698 ms. Six windows contained foreground checkpoint work versus four in each earlier run; cumulative foreground spans were 892.105072 ms versus 446.595418 ms originally and 1,035.212221 ms first-run. These nested spans are not additive wall fractions. Corrective Sui-specific policy/dataflow changes do not alter this generic KV workload, and native controls also differ materially: this single-repetition evidence is not a causal corrective-speedup estimate.

No KV `VmSwap` samples were available, so swap is unmeasured. These remain sequential five-owner loopback/shared-SSD observations; they establish neither independent-drive/cloud scaling nor hardware-EFA performance. Cleanup receipts record no retained temporary DB directories or matching corrected-build KV processes. The remote scalar, bulk throughput and durability-tail gaps remain unresolved.

## Corrected-source clean real Sui replay

The `6d4f…6682` native/TCP pair completed with all Tidehunter and RocksDB full-state audits equal, matching checkpoint digest, all-row table counts and the same full 1,776-checkpoint source range as both earlier controls. The frozen runner verified all six executable bindings. No temporary targeted diagnostic events appeared in the clean native/remote logs. No corrected software-fabric replay was run; the older swap-confounded fabric row above is neither corrected-source nor hardware-EFA evidence.

| Clean TCP population | Original `8f07` | First `7cbc` | Corrected `6d4f` |
|---|---:|---:|---:|
| Native / TCP startup-inclusive s | 13.533814 / 13.862170 | 21.002374 / 21.373966 | 18.245472 / 15.752764 |
| Native / TCP completed-window s | 12.740274 / 12.832594 | 19.646518 / 19.543880 | 16.660984 / 14.460243 |
| TCP/native startup-inclusive ratio | 1.0243× | 1.0177× | 0.8634× |
| Native decoded sequence hits / backend gets | 4 / 3,533 | 4 / 3,538 | 1,765 / 1,777 |
| TCP decoded sequence hits / backend gets | 4 / 3,544 | 6 / 3,510 | 1,733 / 1,777 |
| TCP `checkpoint_content` frontend hits / misses | 583 / 1,194 | 2 / 1,776 | 479 / 1,298 |
| TCP `full_checkpoint_content_v2` frontend hits / misses | 599 / 1,176 | 0 / 1,775 | 469 / 1,306 |
| TCP owner WAL reads, **all namespaces** | 3,690 | 3,986 | 3,003 |
| TCP logical owner WAL bytes, **all namespaces** | 13,732,100 | 19,675,438 | 14,417,306 |

The corrected decoded cache now serves the second traversal rather than evicting summaries solely at 256 rows. Restored content warming recovers substantial hits lost by the first blanket policy, but remains below original content hit counts. All-namespace owner WAL reads fall below both controls, while logical bytes remain **above the original baseline**. These all-namespace totals deliberately differ from the narrower checkpoint-DB-only counters in the first-run table; the retained comparisons contain both, not inconsistent denominators.

Certified-checkpoint backend wrapper calls fall to 1,777 per backend. Their corrected mean is 33.945162 µs native / 32.008573 µs TCP, but decoded hits bypass that histogram: neither this mean nor its bucket-bound quantiles measure total lookup or cache-hit latency. Clean sequence eviction and resident-byte counters are unavailable, **not zero**; the diagnostic eviction proof is a separate instrumented population. Same-key refill-bypass counts are zero, with the previously noted historical counter-semantic boundary.

Sampled corrected owned RSS peaks were 1,028,788,224 bytes native and 1,733,427,200 bytes TCP; sampled owned swap was zero. Host intervals recorded two/four swap-in pages respectively and zero swap-out pages. These samples exclude producer/auditor RSS/swap and do not prove whole-host isolation. The outer runner's whole wall time was 52.94 s with 149.05 aggregate CPU seconds; it is not replay foreground time.

This is one sequential matched pair on a shared SSD with unchanged 8-MiB frontend / 16-MiB per-owner-partition cache settings and all diagnostic profiling limits zero. Observed reuse and warming counters support the intended mechanism, but timing variability across native controls prevents attributing the full elapsed difference to either correction. Corrected TCP is faster than its paired native in this run and the failed first TCP run, yet **slower than the original TCP baseline**. No claim of universal native parity, independent-drive scaling, mainnet capacity, multi-host behavior or hardware-EFA latency follows.

Complete machine-readable evidence under `checkpoint-work/2026-10-16-cache-checkpoint-parity/`:

- [corrected-sui-comparison.json](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-sui-comparison.json): six clean modes, full source/build/fixture binding, state audits, diagnostics separation, all metric populations, resources and limits.
- [corrected-sui-comparison.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-sui-comparison.csv): original, first and corrected native/TCP summaries.
- [corrected-sui-comparison-wrapper-latencies.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-sui-comparison-wrapper-latencies.csv), [adapter-means.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-sui-comparison-adapter-means.csv) and [keyspace-cache.csv](../../../checkpoint-work/2026-10-16-cache-checkpoint-parity/corrected-sui-comparison-keyspace-cache.csv): retained wrapper, stage and all observed keyspace populations, including unavailable/empty values.
- Raw `corrected-sui-tcp/` proof/receipts/summary and `corrected-sui-runner.time`; original comparison files were not overwritten.

The corrected replay cleanup observation found no owned executables and all 15 deployment ports available. Local verification is complete for these retained cases; cloud validation is a separate campaign and cannot be inferred from them.

## 17 October write and direct-transport implementation

This follow-on keeps owner-local authoritative WALs and the existing coordinator
publication/recovery model. It does not replace them with a global physical log,
remove the one-pending-physical-group gate, add replicas or introduce one-sided
WAL reads. Native upstream locks remain intact. No Sui Rust source changes were
needed for this follow-on; the earlier Sui cache and typed-store changes above
remain in place. Sui's `Cargo.lock` was refreshed for the reused optional
`crc32fast` dependency edge; its SHA-256 matches the frozen build lock
`38a72903c16a81f9491dc1e2707016998f7a4a2f01f3564529a39980295b9ecb`.

### Exact mechanism, source and problem map

| Source in `tidehunter-sharded-wal` | Implemented change | Source mechanism and boundary |
|---|---|---|
| `tidehunter/src/distributed/remote_db.rs::progress_pipeline` | Validate completed receipts, dispatch eligible already-staged successor groups, then publish the completed consecutive prefix | [Tidehunter v2 §§3.1/3.4/4.4](https://arxiv.org/abs/2602.01873v2), [Autonomous Commit §§3.1–3.5](https://doi.org/10.1145/3725328), and [LeanStore recovery](https://github.com/leanstore/leanstore/tree/latency-j/src/recovery): overlap independent durable work and index work without delaying a lone request to form a bigger batch. Existing admission, group bounds, one in-flight append per owner and durable-before-publication checks stay authoritative. |
| `tidehunter/src/wal/mod.rs::persist_frame_file` | Use `File::sync_data()` for the exact distributed group file | [TiKV Raft Engine's Unix log barrier](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/env/log_fd/unix.rs) and [Linux fdatasync](https://www.man7.org/linux/man-pages/man2/fsync.2.html): persist data and retrieval-required size/allocation metadata, not unrelated timestamps. Separate directory/name and witness barriers remain unchanged. Native `Db` fsync/all-registered-file paths are unchanged. |
| `tidehunter/src/distributed/tcp/{reactor.rs,reactor_output_tests.rs}` | Retain initialized input-buffer high water, return logical frame length separately, decode/account only that logical slice | [eRPC's bounded reusable buffers](https://www.usenix.org/system/files/nsdi19-kalia.pdf) and native Tidehunter's reuse of storage/read buffers: avoid zero-filling the same retained capacity for every frame. The existing 1-MiB recycle cap, frame validation, TLS and CQ ownership remain. No unsafe code was needed for this buffer change. |
| `tidehunter-fabric/src/endpoint/{efa_direct.rs,resources/efa_direct.rs}`, `endpoint.rs`, `resources.rs` | Separate named direct fabric, MTU fragmentation, DATA ordering, cumulative peer credit, stable generated context2, in-place registered TX framing and scoped RX decoding | [Pinned libfabric 2.6 EFA documentation](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md) and its [provider implementation](https://github.com/ofiwg/libfabric/tree/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src). Direct is not regular EFA with a renamed provider, not arbitrary-size SAS messaging, and not one-sided RDMA/NVMe-oF. |
| `tidehunter/src/distributed/tcp/fabric.rs`, `examples/sharded_wal_{bench,transport_bench}.rs`, `remote_db.rs::backend_id` | Explicit `efa-direct` enum/CLI/backend labels and constructor; TCP default and normal EFA retained | Prevent accidental fallback or mixing direct/software/regular-EFA results. The new enum variant is appended rather than changing old serialized discriminants. |
| `experiments/sui-checkpoint/{deployment.py,prepare_external.py,cloud_node.py,analyze.py}`, `experiments/aws-sharded-wal/{checkpoint_matrix.py,checkpoint_kv.py,sui_checkpoint.py,guest.py}` | Optional distinct direct cells, exact provider/fabric preflight binding, hardware traffic proof, KV/RAM/Sui collectors and pairwise analysis | Measure actual backend populations with per-cell native controls; retain source/workload/cache/profile bindings and original TCP/EFA defaults. Software success is never hardware qualification. |

These are mechanism adaptations, not copies of another engine's recovery or
consensus protocol. [BtrLog §5](https://arxiv.org/html/2606.27051v2#S5) and its
[prototype](https://github.com/maxi-k/btrlog/blob/master/prototype/src/server/wal/blockdevice.rs)
support explicit bounded I/O ownership, but raw-device `O_DIRECT`/`O_DSYNC` and
PLP assumptions were **not** imported into mmap/filesystem storage. Likewise,
Autonomous Commit's fsync-free raw-device result is not permission to remove
filesystem barriers here. [Lotus §§6–7](https://arxiv.org/abs/2607.26227v2)
motivates separating ready-work batching from added formation delay; it is not
an EFA or crash-durability proof.

### Safety and locks

- A dispatched successor may become durable while an earlier group waits for
  publication. It cannot become visible or return an ordered acknowledgement
  past that unpublished prefix. Failure still fences the owner and leaves exact
  durable groups available for bounded recovery/idempotent retry.
- `sync_data` is a durability operation, not asynchronous acknowledgement. The
  exact file, its newly created namespace entry when required, and existing
  durable witness must satisfy their barriers before a storage receipt.
- No native mutex was removed. Direct protocol state has one exclusive endpoint
  owner and needs no extra mutex. Existing coordinator/cache/index/worker locks
  were not replaced by an unproved lock-free structure.
- Direct uses 32 logical DATA credits, 33 fixed reassembly slots and physical
  three-DATA/one-CREDIT TX capacity. Physical CQ lifetime, peer ciphertext
  consumption and durable receipt remain independent. EAGAIN accepts no bytes
  or ordinal/fence advance; stale cumulative credit is harmless, future credit
  is not. TLS and exact WAL/member verification remain enabled.
- The direct codec reuses already-vetted `crc32fast` (engine lock 1.4.2) through
  one optional native dependency edge. There is no new registry package, hand
  CRC/SIMD loop or C ABI. New unsafe buffer/context accesses state their
  exclusive-slot, completion and successful-close invariants.
- The complete direct contract, opt-in timing scopes and pinned source-to-code
  map are in [fabric/DESIGN.md](../../tidehunter-fabric/DESIGN.md#separate-efa-direct-packet-transport-17-october).

### Completed local correctness checks

The parent ran 775 engine unit cases successfully (13 existing ignored cases),
including namespace/witness failures, complete-group crash cuts, exact durable
receipts, successor dispatch during blocked publication and recovery after
publication failure. The corrected native-enabled fabric run passed all 44
cases, explicitly including its real `tcp;ofi_rxm` million-byte bidirectional
partial-stream exercise. Seven existing software-provider integration cases
passed. Strict release Clippy `--lib --tests -D warnings` passed for engine and
fabric; TCP-only `experimental-distributed` checked without the fabric feature.
Python runner suites passed 197/200 with three skips and 80/80 respectively.

The software direct test initially closed one endpoint after only local CQ
completion; retaining both endpoints until both consumers finished fixed that
test's connection-reset failure. Production flush was not changed to pretend
that local CQ completion proves remote consumption. Redundant ordinal-getter
assertions were removed in favor of real direct-state EAGAIN/CQ/credit fixtures.

Hardware discovery on this laptop returned `fi_getinfo: -61 (No data available)`
for exact `efa-direct` selection. Positive hardware negotiation, RNR/reordering,
device errors, TLS over real EFA and performance remain hardware-only checks.
No AWS operations or launches were performed for this implementation. Existing
skill files were absent; no installed project skill was changed or made stale.


### Rebuilt binaries and measured local outcome

All six release executables were rebuilt from frozen archive
`e022c91a74f88e30e00ec60567dc57ef0d46263955f466d55656818157f71704`.
Build-manifest SHA-256:
`b067c6040c20a67e12fcb86668f4d6ba9e76316ae1f1b268329d614cec4ade2b`;
bundle SHA-256:
`d7eed88f5429a047a762d662095cc189cad78af4897f2dabd8f4394a63b32b1f`.
The storage executable is
`01e3dc9cbb3462f427627a8c868a48ba93f0cd63756ca459db68b4771f452fa2`.
Both Sui binaries, both auditors and transport-floor also have fresh successful
Cargo invocation/hash bindings. No previous executable was imported as a new
candidate. Source-manifest comparison confirms the only Sui file difference
from baseline is its lockfile; recorded compiler/build environments match.

The raw KV control used immutable `6d4f…6682` binaries before rebuilding.
Candidate native then TCP used identical flags: Sui WAL geometry, 8-KiB values,
32-key batches, 32-batch windows, 3,200 load batches, 8,192 read keys, two
preparation workers, 32-batch preparation chunks, pipelining/resident-first on,
all explicit value caches off, read window 32, advisory cold passes and resource
evidence enabled. Five loopback owners share one laptop SSD. Each of the four
cases passed complete reopening verification of all 102,400 loaded keys.

| p50 population, microseconds | Baseline native | Baseline TCP | Candidate native | Candidate TCP |
|---|---:|---:|---:|---:|
| Complete durable load window, 1,024 mutations | 11,083.425 | 25,310.652 | 10,878.349 | 25,184.970 |
| Warm scalar value read | 2.857 | 68.016 | 2.561 | 62.516 |
| Complete warm 32-key get-many | 84.120 | 708.882 | 86.618 | 648.561 |
| Warm windowed read, per admission/retirement | 2.791 | 926.383 | 2.596 | 592.971 |
| Advisory cold scalar read | 52.065 | 127.167 | 48.653 | 124.610 |
| Complete advisory cold 32-key get-many | 1,947.204 | 935.757 | 1,860.502 | 2,324.809 |
| Cold windowed read, per admission/retirement | 48.731 | 753.772 | 46.937 | 1,715.048 |
| Whole engine reopen | 9,023.152 | 91,057.175 | 8,043.747 | 85,074.187 |

This is **not native parity** or a uniform improvement. TCP warm scalar and
get-many p50 fell about 8%; native scalar p50 also fell, so the normalized scalar
gap remained about 24×. The durable-window ratio remained about 2.3×. Cold
batched/windowed TCP p50 regressed about 148%/128%. Charged physical bytes were
nearly equal: 94,613,504 baseline versus 94,621,696 candidate per cold pass.
These bytes do not identify the cause of the concurrency regression. Do not
explain it away as increased read amplification or claim the new transport fixed
it. Native get-many/window uses its serial default, not an optimal parallel-native
control. Window admission latency is not complete-window service time.

Separate scalar diagnostic populations improved: cold whole-call mean
199.731 → 152.615 µs with server record-read mean 120.627 → 117.457 µs; reopened
cache-cleared whole-call mean 45.290 → 39.368 µs. These 256-sample populations
do not explain the unprofiled cold get-many regression, and their nested spans
must not be added to infer a network budget. Full mean/p50/p95/p99/max/rate
comparisons for all 50 before/after backend-phase rows are retained, not only
the favorable rows above.

Candidate sampled owned swap was zero for native and five-owner TCP; host
swap-out deltas were zero. Sampled owned RSS peaks were 1,977,237,504 and
1,901,875,200 bytes respectively. The candidate had an external 0.5-second
memory observer, absent from the baseline. One outer repetition, time/order,
filesystem placement and host activity prevent a causal speedup claim.

### Real Sui validation and fresh previous-build control

The rebuilt native/TCP pair executed the same actual private Sui chain:
checkpoints 0..1775, full source-range SHA
`896408bc8581e07473e11db805a69295286df17e05cfb90ed0952c45a9286f9f`,
and final digest
`EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`.
All Tidehunter namespaces and RocksDB state audits matched. Cache budgets
remained 8 MiB per coordinator DB and 16 MiB per owner partition; read, write
and native-write diagnostic limits were zero.

| Clean local Sui population | Fresh old-source control `6d4f` | Candidate `e022` |
|---|---:|---:|
| Native startup-inclusive seconds | 24.224471 | 18.659016 |
| TCP startup-inclusive seconds | 42.299686 | 27.376437 |
| TCP/native startup-inclusive ratio | 1.7462× | 1.4672× |
| Native sampled completed-window seconds | 23.155525 | 17.584212 |
| TCP sampled completed-window seconds | 40.491288 | 25.785573 |
| Native / TCP decoded sequence hits | 1,762 / 1,765 | 1,771 / 1,751 |
| Native / TCP backend sequence gets | 1,777 / 1,777 | 1,777 / 1,777 |

The candidate ran before the fresh old-source control. The historical `6d4f`
pair above was 18.245/15.753 seconds, much faster than this fresh old-source
control. Native also varies materially; the fresh control's native interval had
3,201 host swap-in pages versus 11 in the candidate. Owned sampled swap and
host swap-out remained zero. Thus the latest candidate is faster than this fresh
old control but slower than the historical TCP run: neither universal regression
nor causal improvement is established. Cache reuse and full-state equality are
established. This fixture is not mainnet or an independent-drive capacity test.

Candidate native/TCP sampled owned RSS peaks were 1,019,469,824 /
1,730,756,608 bytes. Source-bound archives, all raw metrics, adapter counters,
histograms and resource samples remain available for inspection. Execution-start
spacing and 250-ms completed-window observations are not per-checkpoint service
latency. Profiling-disabled spans are unavailable, not zero.

The rebuilt actual `--transport efa-direct` command failed with explicit
`efa-direct hardware unavailable: ... No data available (61)`, exit 1 and no
COMPLETE marker. It did not emit a software/TCP result under a hardware label.
Source-bound external preparation also generated all three TCP/EFA/EFA-direct
configurations from the newly exported three-namespace schema, with five hosts
and 15 correctly labeled storage processes each. These validation-only roots,
private keys and duplicate archives were removed; no host was launched.

### Retained evidence and hardware boundary

Under [`checkpoint-work/2026-10-17-efa-direct-local/`](../../../checkpoint-work/2026-10-17-efa-direct-local/):

- `build/{plan.json,source-manifest.json,build-manifest.json,build-receipt.json,binaries/}`:
  exact six-executable provenance and portability.
- `rust-validation.json`, `python-validation.json`, `hardware-availability.json`,
  `efa-direct-unavailable-receipt.json`: exercised checks and explicit limits.
- `baseline-{native,tcp}/`, `candidate-{native,tcp}/`, candidate memory receipts,
  `kv-comparison.{json,csv}`: every selected phase, diagnostic profile and cold I/O
  proof. Original results are retained.
- `sui-tcp/`, `control-sui-tcp/`, their verified analysis JSON files and
  `sui-comparison.json`: complete replay archives, equal audits, captured metrics
  and workload/source identities.
- `external-preparation-validation.json`, `local-cleanup-receipt.json`:
  source-bound three-transport configuration proof and cleanup. No owned benchmark
  executable remained; all 15 deployment ports were available. A cache artifact
  regenerated by the parent's module import was restored byte-for-byte from the
  immutable source archive; post-run source inventory verification passed.

Local implementation and correctness validation are complete. The performance
goal remains unmet. Only approved hardware measurements can determine whether
direct avoids enough regular-EFA overhead; the present evidence cannot promise
few-microsecond remote reads, independent-NVMe scaling or native parity. Retain
TCP and ordinary EFA as measured controls, keep clean and diagnostic populations
separate, and inspect cold concurrent reads rather than hiding their regression.
No AWS launch, upload, commit or push was performed.


## 18 October hardware qualification and direct discovery repair

The authorized native/TCP/EFA/EFA-direct campaign first launched six
`g6.8xlarge` hosts in `us-east-1a`, cluster placement, as
`th-2e14bc3d9fcbf762`, using the rebuilt `e022…` inputs. The first real Sui cell
was deliberately EFA-direct. Exact provider discovery succeeded (`efa`,
`efa-direct`, provider 206.0, RDM), but the remote Sui process failed in
`Endpoint::open_efa_direct` with `fi_getinfo -61`. The matrix stopped before all
later cells or KV/RAM cases. This failed qualification provides **no distributed
performance result**; installer-localhost hardware bytes are not Sui traffic.

Pinned libfabric 2.6 source tracing found the cause: for FI_MSG, direct memory
registration requires `FI_MR_LOCAL | FI_MR_ALLOCATED`. The former LOCAL-only hint
was rejected before info alteration. `tidehunter-fabric/src/endpoint/resources.rs`
now requests ALLOCATED only for direct; existing stable, initialized buffer
ownership already satisfies it. Normal EFA/software negotiation, API versions,
validation, wire protocol, TLS and durability remain unchanged. See the
[exact provider source and invariant proof](../../tidehunter-fabric/DESIGN.md#hardware-discovery-correction-18-october).
This was the locally proven request fix; hardware data transfer and performance
were not yet qualified at that point.

After correction, all 44 fabric unit cases including its software stream exercise,
seven ordinary software-provider integration cases and strict engine/fabric
Clippy passed. The corrected six-executable build and same-build local replay
described below were then required; the old failed inputs remain immutable.

All 67 first-run S3 objects were recovered and content-hash verified. The exact
six instances, six ENIs, six root volumes, bucket, placement group, security group,
six roles and six profiles were independently verified absent/terminated;
23-region ownership scans found no residuals. Evidence is under
[`checkpoint-work/2026-10-18-efa-direct-hardware/th-2e14bc3d9fcbf762/`](../../../checkpoint-work/2026-10-18-efa-direct-hardware/th-2e14bc3d9fcbf762/),
including `cleanup-verified.json`, raw API observations and the failed controller
archive. Its actual dynamic linkage used `/opt/amazon/efa/lib/libfabric.so.1`;
logs show installer 1.50.0 and libfabric 2.6.0amzn1.0, not a substituted TCP library.
The pricing-based known compute/root/IPv4 upper estimate was about $2.40,
excluding unknown transfer/storage-request/billing adjustments; it is not an invoice.

## 18 October completed hardware comparison

### Inputs, coverage and closure

The corrected immutable source archive is
`70454d0113ff49df6abfedd38865f3d7f0adc215c5afa6ece1a0431b08a921aa`;
the build manifest is
`10923917786e2b598c03fa970801300e80826290d73edcaac3b5d88cf2642593`.
All six release executables were rebuilt before upload. Their actual hashes,
source bundles and workload bundles passed admission on both campaigns.
The frozen runtime sources matched 3,986 current source/manifests/lock files
and one source link. A fresh local native/TCP Sui replay completed with full
state equality before launch; its timings are not substituted for cloud results.

Both completed campaigns used six `g6.8xlarge` hosts in `us-east-1a`, cluster
placement: one controller and five storage hosts, with local instance NVMe.
Controller `findmnt`/model evidence confirms instance storage; owner bootstrap
receipts record the selected NVMe device, but owner-specific model/findmnt
snapshots were not retained. Replication factor was one, not a replicated
production deployment. TCP, ordinary EFA and the separate EFA-direct message
path each had their own native controls. EFA/direct required the exact hardware
provider/fabric and positive send **and** receive counter deltas on all six hosts.
These host counters are not wire latency or exclusive process attribution.

| Completed campaign | Sui pairs | KV pairs / backend executions | Clean / diagnostic KV pairs | RAM processes | Verified S3 objects |
|---|---:|---:|---:|---:|---:|
| A2 `th-7ffe878d1f51852d`, targeted | 12 | 33 / 66 | 30 / 3 | 6 | 1,098 |
| B2 `th-1819bd4b6691868e`, full | 3 | 51 / 102 | 39 / 12 | 0 | 933 |

The Sui workload executes a **private generated Sui chain, not mainnet**:
1,776 checkpoints through tip 1775, with 19,708 transactions/effects per replay
and the exact certified digest in the receipts. All 30 replays completed.
Every paired audit matched all rows and multiplicities in 57 selected Tidehunter
tables and 44 retained RocksDB column families. The KV value sizes are
**256 B and 8 KiB**, with B1/W1 and B32/W32 shapes plus the authority-32 case.
“Full” does not mean MiB-sized individual values or a production-size working set.

All result objects were retrieved and hash-verified before deleting their S3
buckets. Each fleet terminated before the next launched. Independent exact-ID
queries verified all 18 instances, including the failed qualification, terminated;
run-owned volumes, ENIs, buckets, security/placement groups and IAM resources were
removed. The final three-RunId query at **10:47:16 UTC** returned no instances
in pending/running/stopping/stopped/shutting-down states. Other users were not
included in the mutation set.

The AWS agent tried MCP first. Documented MCP read timeouts required the existing
project SDK adapter under the same profile/account. No new AWS client or SSH-based
retrieval path was substituted. The operations ledger gives a compute-only
estimate of $8.17–$12.17 including the failed qualification; this is not a bill
and excludes unknown ancillary charges, discounts and billing lag.

### Sui replay: paired results, not raw-KV timings

B2 clean results, coordinator cache 8 MiB and owner cache zero. Elapsed includes
process startup; RPC-index catchup is reported separately and excluded from it.

| Transport | Paired native seconds | Distributed seconds | Distributed / native |
|---|---:|---:|---:|
| TCP | 6.2417 | 10.3620 | 1.6601 |
| EFA | 6.2642 | 9.3832 | 1.4979 |
| EFA-direct | 6.2849 | 9.7498 | 1.5513 |

A2 keeps all four repetitions per transport separately: r1/r4 clean owner-cache
zero, r2 clean owner-cache 16 MiB, and r3 owner-cache 16 MiB with bounded read/write
diagnostics. Its clean paired elapsed ratios were TCP 1.593–1.665, EFA 1.457–1.537
and direct 1.482–1.504. These ranges are not pooled latency populations.

Actual B2 typed-store getter **means in microseconds**, native → distributed:

| Table | TCP | EFA | EFA-direct |
|---|---:|---:|---:|
| `certified_checkpoints` | 18.548 → 24.785 | 19.363 → 21.286 | 18.604 → 20.522 |
| `checkpoint_content` | 9.072 → 78.678 | 9.159 → 42.492 | 9.328 → 41.365 |
| `full_checkpoint_content_v2` | 28.542 → 106.111 | 28.681 → 65.382 | 28.784 → 65.119 |

Do not compare these native certified-checkpoint means with the roughly
one-microsecond raw KV getter. They are different APIs and work populations.
All certified-get p50 buckets are `(10,50] us`; these bounds cannot establish exact
p50 parity. Native certified-get p99 is `(10,50] us`, versus TCP `(200,300] us`
and EFA/direct `(50,100] us`. Backend histograms omit decoded execution-cache hits;
they do not measure the latency of those cache hits.

B2 wrapper write-batch means, native → distributed, microseconds:

| Namespace | TCP | EFA | EFA-direct |
|---|---:|---:|---:|
| Checkpoints | 455.807 → 707.793 | 460.486 → 525.574 | 468.616 → 712.911 |
| Epoch | 431.648 → 876.199 | 432.009 → 730.711 | 435.535 → 722.752 |
| Perpetual | 642.298 → 1197.301 | 640.734 → 999.953 | 641.906 → 1010.948 |

Adapter commit counts/means, wrapper batches, native barriers and owner
containing-file persistence are distinct populations. Preserve their counts.
Internal certificate-execution means were also near their native controls:
TCP 318.113 → 346.798 us, EFA 322.387 → 316.123 us, direct 321.430 → 327.353 us.
This internal timer is not end-to-end transaction durability.

### Raw KV latency, cache hits and cold-start boundaries

B2 clean p50, microseconds. The native column retains the **TCP / EFA / direct**
paired controls; it is not a common pooled native measurement.

| Population | Paired native p50s | TCP | EFA | EFA-direct |
|---|---:|---:|---:|---:|
| 256 B scalar, caches disabled | 0.370 / 0.360 / 0.370 | 141.334 | 44.931 | 45.332 |
| 8 KiB scalar, caches disabled | 1.240 / 1.220 / 1.270 | 161.995 | 61.142 | 62.631 |
| 8 KiB complete 32-key get-many, B1 case | 41.071 / 41.151 / 79.682 | 614.296 | 486.443 | 503.814 |
| 8 KiB windowed get, per operation, B1 case | 1.110 / 1.110 / 1.130 | 754.560 | 628.047 | 659.118 |
| Cold-start 8 KiB scalar, B32 case | 114.633 / 114.853 / 113.233 | 318.439 | 180.055 | 184.295 |
| Cold-start 8 KiB complete get-many, B32 case | 2971.561 / 2889.728 / 2943.669 | 830.623 | 641.888 | 699.379 |

The clean 8 KiB scalar remote/own-native ratios are **130.64 / 50.12 / 49.32**.
Its p99 values are TCP 238.367 us, EFA 75.552 us and direct 76.232 us.
Matched A2 scalar results were close, but are retained as a separate campaign.
Windowed admission-to-retirement latency is not a complete-window duration.
The native grouped/windowed control is serial; cold grouped speedups do not
prove a capacity advantage over an optimized parallel native control.

With an **8 MiB frontend cache**, B2 scalar cells recorded 4,096 hits and no misses.
Their p50/p99 were TCP 0.550/1.030 us, EFA 0.560/1.040 us and direct
0.550/0.990 us. This proves a sub-microsecond raw frontend-cache path, not a
sub-microsecond remote fetch. The much larger B32 working sets did not all fit:
the 8 KiB cached B32 scalar phases still had 3,770–3,792 misses out of 4,096.
Missing-key and existence checks are cheap coordinator metadata operations and
are exported separately from value fetches.

Cold phases start after eviction, not after an eviction before every request.
The B2 8 KiB B32 cold phase began with zero observed resident WAL bytes and
recorded 41.387 MiB native physical reads versus 35.605 MiB aggregate remote
physical reads. Repeated accesses can warm the phase. In particular, small B1
cold-start p50s near one microsecond are **not cold-device service latency**.
All Sui owner process physical-read deltas were zero despite logical WAL reads.
Those Sui replays exercise buffered/page-cache access, not cold NVMe fetches.

### Write windows, tails, maintenance and reopen

B2 bulk load **means in milliseconds**, 10 windows per population:

| Shape | Native controls TCP / EFA / direct | TCP | EFA | EFA-direct |
|---|---:|---:|---:|---:|
| 256 B B32/W32 | 3.102 / 3.480 / 3.568 | 4.111 | 3.772 | 4.092 |
| 8 KiB B32/W32 | 21.747 / 21.603 / 21.635 | 22.787 | 21.411 | 24.505 |

Corresponding 8 KiB remote phase rates were 44,781 / 47,681 / 41,679 logical
operations/s. B2 has too few load windows for p95/p99. A2 deliberately retained
100 windows: TCP-paired native p50/p99 21.597/22.752 ms, TCP 23.239/192.435 ms,
EFA 21.996/165.062 ms and direct 23.308/176.869 ms. Near-native median load
windows do **not** imply native tail latency. Do not pool the two load sizes.

B2 clean 8 KiB scalar-case lifecycle p50, milliseconds; each is one sample.

| Operation | Native controls TCP / EFA / direct | TCP | EFA | EFA-direct |
|---|---:|---:|---:|---:|
| Open | 7.114 / 6.777 / 7.232 | 14.866 | 59.425 | 47.453 |
| Full scan | 0.538 / 0.566 / 0.576 | 6.474 | 5.394 | 5.728 |
| Flush index | 3.170 / 3.380 / 3.341 | 12.659 | 12.955 | 13.154 |
| Shutdown | 1.075 / 1.381 / 1.278 | 12.678 | 11.699 | 11.951 |
| Fresh reopen | 2.897 / 3.508 / 2.826 | 14.175 | 54.046 | 40.789 |
| Reused-transport reopen | unavailable | 9.867 | 8.878 | 9.107 |

Fresh reopen creates 15 connections; reused reopen creates none. Connection-setup
counters are not an additive decomposition of these elapsed times. Checkpoint
capture/intervening write/read, scalar and bulk mixed writes/deletes, post-flush
and reopened reads, scan and audit distributions remain in the full API CSVs.
Two-sample mixed/delete windows have no valid p95/p99; 64-sample scalar windows
have no valid p99. Missing tails are empty/null with reasons, never zero.

### Observed bottlenecks and decisions

1. **Remote value delivery remains expensive without disk.** A2 clean RAM-only
   8 KiB full-payload responder p50s across its three separate repetitions were
   TCP 133.788–135.498 us, EFA 53.693–53.983 us and direct 55.653–55.813 us.
   All RAM WAL-delta checks passed. This is a full RPC/software-stack control,
   not pure wire RTT. Replacing the disk with NVMe-oF cannot remove the cost
   observed when the responder does not access the WAL at all.
2. **Routing/index lookup is not the measured scalar-read budget.** Separate
   A2 out-of-main-timer warm 8 KiB profiles measured client index means
   0.464/0.432/0.400 us, exchange means 159.352/60.213/62.672 us, and nested
   server-handler means 13.925/4.149/4.745 us for TCP/EFA/direct. No one-way
   wire, scheduler, TLS or completion sub-budget is inferred from the residual.
   Nested means/quantiles must not be added or subtracted into a wire budget.
3. **Persistence remains substantial for writes.** A2 direct checkpoint/epoch/
   perpetual append-persist means were 366.444/335.487/468.668 us, with containing
   WAL-file persistence 361.165/330.318/463.326 us, n=640 each. Client exchange
   populations have different counts. Native barriers cover all registered files;
   they are not equivalent per-file populations. Improve grouping/overlap while
   retaining the acknowledgement guarantee, not by dropping persistence.
4. **Direct is not a general EFA speedup.** B2 direct/EFA p50 ratios were
   1.024 scalar, 1.036 grouped, 1.049 windowed and 1.164 bulk load for 8 KiB.
   Its Sui total was 3.91% higher; logged startup was also higher and execution
   start spans were close. Sequential cells do not prove a causal transport
   difference. Keep ordinary EFA as the performance control.
5. **The EFA CPU cost is large.** B2 Sui owner CPU totals over enclosing sample
   intervals were TCP 26.04 s, EFA 456.62 s and direct 470.51 s. These are not
   API-exclusive CPU or a CPU-stack attribution. Remote frontend RSS was
   738.64/1517.61/950.55 MiB; sums of owner RSS maxima are not simultaneous fleet
   peaks. EFA/direct exchange efficiency needs work despite lower point latency.
6. **Caches help only their observed populations.** A2 Sui owner-cache hits
   remained zero even with 16 MiB configured; coordinator/decoded-cache counters
   are separate. Raw frontend hits can match the desired latency order, but
   there is no evidence that all production values fit or that owner-cache hits
   remove a remote round trip.

No global-WAL rewrite, storage-protocol replacement or native-parity claim follows
from these runs. The next measured work is the RAM RPC path and durable-write
grouping/tails. A genuinely cold production-size Sui working set is still a
different experiment from this generated, page-cache-resident chain.

### Complete data and measurement limits

At the project root, `checkpoint-work/2026-10-18-efa-direct-hardware/` retains:

- `analysis-a2-kv/` and `analysis-b2-kv/`: all API populations/quantiles/counts,
  paired native/transport contrasts, raw-window distributions, cache and cold
  proofs, stage/node profiles, RAM controls, IO/CPU/RSS/swap observations and
  availability/error classifications. Their `artifact_index.json` binds outputs.
- `analysis-a2-sui/` and `analysis-b2-sui/`: source-bound core exports plus
  interpretation CSVs for replay/startup/catchup, namespace adapters, getters,
  writes, histogram bounds, bounded profiles, pipeline/cache/resource/device
  observations and every hardware proof. Each interpretation manifest binds the
  unchanged core export manifest.
- Each RunId directory: admitted READY/config/source/build identities, every
  retrieved object, retrieval manifest/receipt, controller/node archives and
  independent closure evidence. `final-active-owned-inventory.json` and
  `final-campaign-ops-ledger.json` record final operations state.

Across the completed KV campaigns there are 3,924 API populations and 11,611,236
raw samples including RAM. No cache/profile/campaign/native-control populations
are pooled. Use the explicit `transport` and `raw_backend` fields: the harness's
historical `backend=tcp` category also denotes remote EFA adapters; hardware
identity is checked independently and is not inferred from that enum name.

Important limits and defects retained in the data:

- Histogram quantiles are bucket intervals, not exact sample quantiles. No
  checkpoint completion/service tail was captured; execution-start spacing is
  not a replacement. Some checkpoint histograms are empty.
- Decoded execution-cache-hit latency and frontend CPU were not captured in the
  Sui profiles. Unavailable/disabled spans are not zero-cost work.
- The native Sui execution-driver dispatch-queue gauge was negative in captured
  data; it is not interpreted as physical queue occupancy.
- Retained plaintext/TLS error counters include `WouldBlock`; cleanup HTTP
  absence responses also appear in generic counter exports. They are not failed
  RPC counts or benchmark correctness failures.
- Owner status/host intervals reported zero swap where captured. Missing
  frontend VmSwap in KV resource CSVs remains unavailable, not inferred zero.
- Normal-stop/reopen and full-state audits passed. No injected device loss,
  Byzantine behavior, mainnet scale or replicated availability was tested.

Offline export fixes accepted signed ancillary histogram bounds, included
checkpoint intervening-write raw spans, and retained source symlink metadata
without following it. None relaxes source, audit, hardware or closure guards.
The MR discovery change removed no mutex and changed no native-engine locking,
TLS, validation or durability guarantee. No installed project skill files were
made stale; the fabric design and this report contain the changed requirements.


