# Local metadata-residency and tail-latency implementation plan

## 1. Decision and scope

Written before implementation on `perf/spill-residency-batch-cache`, based on local HEAD `44a7effd2a4dea5589f9015b7c29dca39cd5d9b4` and measured cluster source `d34022d18aed2e1b7b029d595f43992868453b28`. The preceding audit verified equal contents for all 497 frozen source files before later documentation-only updates. Existing modified documents and unrelated untracked files are preserved. No commit or push is part of this task.

Implement two evidence-backed changes first:

1. Preserve eligible, already-validated immutable authority descriptors across successful history spill adoption, within the existing metadata budget.
2. Resolve coordinator batch-cache lookups in bounded windows rather than taking the same mutex once per member.

Keep the architecture: coordinator key index and visibility, whole atomic batch on one owner, owner-local Value WAL, existing durable receipts and sparse/dependency-respecting publication. No new global WAL, global sequencer, distributed LSM, replication protocol or transport rewrite. The old middleware remains archived and unused.

**Local work only. No AWS/GCP allocation, CI campaign dispatch or remote device changes.** This laptop has one shared SSD and no validated hardware RDMA. Local acceptance means correct behavior, bounded resource/lifetime management and matched local-relative measurements. Only a later user-authorized cluster comparison can establish distributed performance.

## 2. Measured evidence and priority order

Source: [completed campaign report](reports/2026-10-30-kv-comparison-41109588049/SUMMARY.md), [all metrics](reports/2026-10-30-kv-comparison-41109588049/METRICS.md), and [timing/source audit](reports/2026-10-30-kv-comparison-41109588049/STAGE_AUDIT.md). Run `41109588049` completed 196 cases on six c6id.32xlarge hosts in eu-west-1c with cluster placement; all resources were then removed. These are synthetic KV cases, not new Sui checkpoint results.

| Priority | Observed problem | Evidence | Action and scope |
|---|---|---|---|
| P0 | Incorrect attribution can motivate the wrong rewrite | Older G6/us-east-1 differs from current hardware/region; primary vs warm controls also differ in keys, warmup, lanes and resident-first | Preserve frozen cases/raw data, scopes and population labels; no causal whole-system claim |
| P1 | Useful authority metadata is discarded during spill | Primary EFA-native/TCP measured 1,937/1,936 history-load attempts while caches were well below 8,192-entry capacity | Retain eligible immutable descriptor Arcs after durable adoption; no extra value cache or foreground persistence |
| P2 | Bulk coordinator hits repeatedly contend on one cache mutex | Sampled 32-key cache-hit population: 8,192 acquisitions, 79,041,663 ns accumulated wait, mean 9.649 µs/acquisition | At most 32 consecutive slots per cache-lock window; release before routing/allocation/network |
| P3 | Improvements need isolated, reproducible local qualification | Warm-WAL case0157 reached zero loads and planning mean0.876 µs; old case had2,547 loads/22.738 µs, but other end-to-end settings/hardware confound | Same-binary-feature before/after local probes, fixed keys/settings, count/lifetime invariants, C1/C16/batch32 controls |
| P4 | Remaining remote/cold/write costs are not fully decomposed | Qualified registered C1 medians47–52 µs; FI_READ46.496 µs includes progress. Cold planning includes worker queue/load/handoff. Ready-group/caller distributions are missing | Instrument or isolate the missing boundary before changing workers, CQ scheduling or group commit; separate future step |
| P5 | No valid pure cold-NVMe API distribution yet | Eight distributed cold cases have6,998 actual block requests/window but wider windows; native nominal cold has1/0 matched reads | Fix measurement window/request/file attribution before a separate NVMe-oF comparison |

Numbers are single-repetition descriptive observations. API p50/p95/p99/p99.9 remain nearest-rank with sample-count warnings. Stage means use their own count/sum, may overlap, and are not additive latency fractions. A history lookup can hit Linux page cache; a WAL read does not prove physical NVMe I/O.

## 3. Papers, maintained code and applicability

QMD collection used: `tidehunter-papers`. Complete relevant text sections, not search snippets, are the mechanism references. Embedded base64 figures can truncate some QMD sections; truncated write/principle/tail sections are not relied on for unseen claims.

| Reference and inspected source | Adopt | Do not infer or copy |
|---|---|---|
| [Tidehunter v2](https://arxiv.org/html/2602.01873v2), QMD `tidehunter-2026-v2.md` §§3.2,3.4,4.2–4.4; local `papers/Tidehunter-2026-v2.pdf` | Direct index→Value-WAL positioning; distinguish value cache, index residency, OS cache and device reads; persist/capture/adopt while retaining later updates; retain read references through relocation/GC | Native does not always keep flushed indexes resident. §4.3 can unload after unmerge. Do not change WAL format, replay, relocation or ordering to make a cache faster |
| [KVell, SOSP2019](https://doi.org/10.1145/3341301.3359628), QMD `kvell-2019.md` §§5.3–5.4 | Predictable bounded cache work; avoid resizing/maintenance stalls; amortize work over bounded batches | KVell uses a B-tree for cache indexing and no commit log. It is not evidence to replace our preallocated cache or eliminate Tidehunter's WAL |
| [eRPC, NSDI2019](https://www.usenix.org/conference/nsdi19/presentation/kalia), [arXiv1806.00680v2](https://arxiv.org/abs/1806.00680v2), full processed LaTeX fetched through arXiv MCP; QMD `erpc-2019.md` §§3.1–3.3,4.2.2,7.2 | Bound short-path work; do not hold dispatch/progress behind long maintenance; retain buffers through actual completion | CX3/CX5 RAM-RPC results are not EFA/TLS/cold-WAL targets. Do not import unsignaled-completion assumptions or spin policy without measured need |
| [Valkey9.0.7 AOF lifecycle](https://github.com/valkey-io/valkey/blob/9.0.7/src/aof.c), `backgroundRewriteDoneHandler` | Preserve old authority until replacement persistence/adoption succeeds; keep maintenance separate from foreground acknowledgments | Valkey's RAM dataset and rewrite format are not our WAL-as-values storage. No source code is copied |
| [TiKV Raft Engine](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/purge.rs), purge/barrier/live-reference checks | Reclamation respects live references and durable ordering | No new consensus log, unsafe barrier or equivalent-guarantee claim |
| [SPDK target guide](https://spdk.io/doc/nvmf.html), [current stable v26.09 release](https://github.com/spdk/spdk/releases/tag/v26.09), [Linux NVMe/RDMA](https://github.com/torvalds/linux/blob/v6.12/drivers/nvme/host/rdma.c), [FIEMAP rules](https://docs.kernel.org/filesystems/fiemap.html) | Separate storage experiment with verified provider, ownership, read-only and mapping gates | Hardware RMA support is not stock NVMe/RDMA compatibility; file offsets are not safe raw-device LBAs |

Native-code counterpart: `tidehunter/src/large_table.rs` around `2535–2625` retains loaded state only under specific policies; normal asynchronous flush completion can unload. The adapted principle is **budgeted useful residency independent of persistence**, not universal retention.

## 4. Codebase map and repository impact

Line ranges below refer to the pre-edit source; symbols remain the stable mapping.

| File / symbols | Current behavior | Planned modification |
|---|---|---|
| `tidehunter/src/distributed/reference_directory.rs`: `RegisteredBatch`, `MetadataCredit`, `AuthorityCache`, `lookup`, `start_background`, `persist_pending` | Core credit follows an immutable batch Arc; cold lookup cache includes entry overhead in its reservation; adoption drains pending descriptors | Separate cache-entry overhead ownership from descriptor core, carry eligible existing Arcs into bounded LRU after successful adoption |
| `tidehunter/src/distributed/storage/tests/history.rs` | Durable-root, replay, pressure, held-pin and corruption fixtures | Add post-spill first-touch/budget cases; keep deliberately cold tests cold via explicit eviction |
| `tidehunter/src/distributed/remote_db/pending_read/concurrent.rs`: `get_many_cut` | Index read guard protects cut/ref resolution; cache mutex acquired per live key | Consecutive windows of at most32 slots; use existing values array; no cache guard during route/group allocation or transport |
| `tidehunter/src/distributed/remote_db/read_cache.rs` | Existing preallocated set-associative cache, bounded history/admission and full-reference checking | Reuse unchanged; no new tree/hash/cache implementation |
| Existing `remote_db/pending_read`, `remote_db/concurrent` test modules | Mixed results, duplicate keys, snapshot cuts, grouped owners and read credits | Add behavior-focused mixed/duplicate boundary coverage beyond32 and stale/cut/pressure checks where missing |
| `tidehunter/src/distributed/read_profile*`, coordinator `ResolutionStage` consumers | Each stage carries its own count/sum/max | Preserve schema; document cache-lock-wait count as acquisitions/windows, not keys; measure total wait per selected API as well as per acquisition |
| `benchmarks/kv-research/src/main.rs`, `backends/tidehunter*`, existing comparison helpers | Real five-owner local TCP and software-provider paths, raw API samples and boundary evidence | Use existing CLI/orchestration and unchanged sample/percentile rules; no cloud workflow or default primary-policy change in this delivery |
| `tcp/reactor.rs`, `tcp/read_executor.rs`, `storage.rs`, `wal/mod.rs` | Resident planning, cold worker, async value I/O, fencing, DMA and file pins | Read for invariants; no speculative transport or device changes in P1/P2 |

**Repository changes:** only the distributed Tidehunter fork's internal cache/adoption/read-resolution code, its tests and documentation. No Sui edits; no middleware edits; no native on-disk format or API cutover. No new dependency, unsafe block or lock-free abstraction is required. No native mutex is removed. Heavy Sui changes are not necessary for these two bottlenecks.

## 5. P1: retain authority metadata after spill

### 5.1 Correct sequence

```mermaid
flowchart LR
  A[Capture pending immutable Arcs] --> B[Persist history and root]
  B --> C[Validate and atomically adopt root]
  C --> D[Release authority guard]
  D --> E[Transfer eligible Arcs in 32-entry cache windows]
  E --> F[Bounded lookup residency]
  F -->|Normal eviction| G[Immutable history lookup]
```

1. Keep history persistence, durable-frontier selection, failure poisoning and prefix identity checks unchanged.
2. Adopt the durable root and remove the exact captured pending prefix under the existing authority guard. Retain the already prepaid captured Arc vector; do not decode or copy descriptors.
3. Release authority before warming the cache. Consume captured Arcs in windows of at most32 entries under the existing cache mutex, with at most1,024 captured members in a background rewrite. No I/O, network, sleep or per-entry descriptor/Arc allocation under these guards. Existing LRU-node allocation is allowed and charged; it is not misreported as zero allocation.
4. Use the same retention helper for successful synchronous `persist_pending` adoption, including seal and already replayed recovery suffix. Do not read or prewarm existing immutable history during open.
5. Duplicate address: keep the existing cached value and release the redundant captured owner/credit. A concurrent reader can load during the adoption-to-warm interval; this is safe and must not be treated as an invariant failure.
6. At full entry capacity, normal bounded LRU eviction may free one entry; reserve only the additional entry overhead. If credit cannot fit, skip optional retention, promptly release that captured Arc, and continue. Persistence/admission must not fail merely because optional cache retention failed.
7. `wait_background` reports success only after that worker has finished retention and normal cleanup. Readers during warming may take the cold fallback; cache warmth is not part of publication correctness.

### 5.2 Accounting and locking

- `RegisteredBatch` owns only its descriptor/core `MetadataCredit`; its Arc and held read/CQ owners keep that credit alive.
- A cached entry additionally owns separate `CACHE_ENTRY_BYTES` credit, released on eviction even if another reader retains the descriptor Arc.
- Cold load reserves descriptor/core and, when possible, cache-entry overhead before allocation, splitting ownership without double counting. Duplicate cold/carry-over races release unused reservations.
- Existing cache trim policy can still track core+entry bytes; actual hard-budget use comes from the independently owned credits.
- Preserve history→authority→cache order where existing code nests them; retention itself holds only the cache guard after authority release. No cache-held history I/O or join.
- Captured descriptors remain charged while awaiting retention. Tight-budget concurrent cold reads can encounter existing Capacity/backpressure semantics; do not wait for those readers or create a lock cycle. All warming is bounded and terminates.

### 5.3 Acceptance

After successful spill completion and with sufficient budget, first access to captured records returns exact values/references with no history-load increment. Eviction and reopen remain valid cold fallbacks. Small budgets never exceed credits; eviction releases entry overhead while held reader/CQ pins retain core credit; drop releases all credits. Failed durable-root installation/prefix mismatch does not publish optional residency as authority. Forged refs, stale generation, failure/reopen, snapshots and retirement retain existing outcomes. Permanent tests cover these behaviors, not arbitrary cache defaults or implementation-wiring counts.

## 6. P2: bounded coordinator batch-cache access

1. Preserve validation of the captured cut and per-key reference resolution under the existing index read guard.
2. Traverse references in original slot order, in consecutive chunks no larger than32. Index misses and per-key errors need no cache access. Do not take one mutex for the complete256-key maximum.
3. For each chunk containing live references, acquire the cache once and clone cached `Bytes` into the existing result slots. Keep exact-reference validation, bounded admission/history and duplicate-key touch order in `ReadCache::get`.
4. Release the cache before routing misses, growing owner groups, pin preparation, waiting, copying response payloads or transport. Retain index→cache lock order and release index before remote work.
5. Resolve route errors in slot order after unlocking. Do not add an all-hit route pre-scan solely to preserve incidental LRU touches on a fatal invalid-index route; that adds avoidable hot-path work. Returned error precedence and per-key values remain unchanged.
6. Preserve disabled-cache hit/miss accounting through the existing bounded `get` calls rather than add a new configuration flag or bypass counters. Leave scalar `get` unchanged.
7. `CacheLockWait.count` becomes actual lock acquisitions, while `CacheLookup.count` remains actual member lookups. Existing count/sum/max schema remains; consumers must not normalize both by key count. Validate this through runtime benchmark evidence, not permanent tests that pin the implementation's exact acquisition count.

Acceptance: mixed hits/misses/errors, duplicates across chunk boundaries, >32 and up to256 slots, snapshots/stale generations, values, owner grouping, cache accounting, output ordering and pending pins remain correct. No guard survives into network/DMA waits. C1 control must remain bounded; batch hold/wait tradeoffs are measured rather than assumed.

## 7. Local qualification and comparison protocol

### 7.1 Before editing and after integration

- Freeze the existing all-adapter release and its SHA; record source baseline, hardware, compiler, feature set and environment. Rebuild the same all-adapter release after changes. Use five local owner processes, not ten.
- Reuse the current CLI external-owner preparation and fresh case paths. Fixed logical namespace, seed, key/value sequence, cache budgets, warmup, lanes, owner workers, resident-first, operation counts and instrumentation must match before/after; only paths, ports and code identity differ.
- Preserve native and TCP fallback behavior. Run software-provider functional read/fence cases separately; never label them EFA hardware latency.
- Record MemAvailable/swap and `pswpin/pswpout` before/after. No global cache drop or disk/device ownership change.

### 7.2 Focused cases

| Local case | Purpose | Required observations |
|---|---|---|
| Five-owner scalar first-touch, short nonsweeping warmup, explicit value caches off | P1 adoption-to-read residency | Successful spills/adoption, authority cache start/end loads/entries/credits, exact values, API/raw timings; baseline must actually demonstrate cold loads |
| Same settings with resident-first held fixed | Separate metadata fix from value-I/O policy | Do not silently switch the legacy primary path, lane count or full sweep to obtain a gain |
| Warm coordinator-hit1KiB scalar, C1 and C16 | Unchanged scalar fast path and tail guard | Qualified cache hits/misses and scheduling/API/response distributions |
| Coordinator-hit8KiB×32, C16, full fixed sweep | P2 contended batch lookup | Exact values, no unintended remote misses, selected API/member/acquisition counts, total/cache hold-wait evidence and raw timings |
| TCP >32 mixed/duplicate/snapshot behavior and software transport fixture | Chunk/lifetime correctness | Per-key results/cuts, owner grouping, capacity/fallback and clean owner exits |

Read cases retain8,192 measured calls; large batch32 retains2,048 calls/65,536 logical keys. Single independent run per case by default, no run-to-run bars or significance claim. Preserve raw API/scheduling/response samples, p50/p95/p99/p99.9, mean, population standard deviation, maximum, successful/offered/rejected counts and observed throughput exposure. Missing is not zero; do not pool baseline/profiled or different paths.

Before any uncertain/regression rerun, use a practical local investigation threshold of >10% median or >25% p99 change under the same case, with unchanged counter qualification. These are investigation triggers, not statistical tests or cluster success criteria. If rerunning a comparison, rerun both versions/settings, not only the preferred result. Single-run tails remain descriptive.

Run format, strict Clippy, distributed library and fabric/related tests once after integration; exercise the real CLI path, not just unit tests. Retain meaningful regression tests for accounting, transition and snapshot bugs. Remove temporary database trees only after owned processes have exited and raw evidence is saved. Do not remove previous user data or old reports.

## 8. Separate NVMe-oF experiment: decision and gates

**Investigate separately; do not implement or deploy it in this delivery.** It targets a potential cold-storage path, not coordinator cache locking, metadata first-touch loads or registered-RAM completion cost.

### 8.1 Transport and provider distinctions

- NVMe/TCP is a storage protocol over IP/TCP, not EFA SRD. A file/image-backed disposable target is the portable functional control.
- Stock Linux NVMe/RDMA uses `rdma_cm` and RC queue pairs (`IB_QPT_RC`); the inspected SPDK target uses connection management, registered buffers and CQ state. IB/RoCE compatibility requires actual provider/QP/CM validation.
- [AWS EFA](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html) uses SRD/libfabric. FI_READ/FI_WRITE availability does not establish stock NVMe/RDMA support. Treat compatibility as unverified until an exact supported implementation and actual connection/transfer test prove it. Do not force an emulated path and call it hardware RDMA.
- Current [SPDKv26.09](https://github.com/spdk/spdk/releases/tag/v26.09) adds RDMA initiator interrupt mode, explicit completion/teardown polling requirements, and NVMe KV namespace support. Its in-memory `bdev_kvmalloc` and passthrough KV commands do not implement Tidehunter's WAL/index/recovery semantics. They are not an automatic store replacement.

Inspected implementation anchors: [SPDK target QP setup](https://github.com/spdk/spdk/blob/v26.09/lib/nvmf/rdma.c#L1009-L1043), [CM listener](https://github.com/spdk/spdk/blob/v26.09/lib/nvmf/rdma.c#L2801-L2834), [CMA wrapper](https://github.com/spdk/spdk/blob/v26.09/lib/rdma_cm/rdma_cm_cma.c), [read-data RDMA WRITE](https://github.com/spdk/spdk/blob/v26.09/lib/nvmf/rdma.c#L1430-L1448), [request/transfer/completion state machine](https://github.com/spdk/spdk/blob/v26.09/lib/nvmf/rdma.c#L2168-L2265), and [RAM-only kvmalloc](https://github.com/spdk/spdk/blob/v26.09/module/bdev/kvmalloc/bdev_kvmalloc.c#L20-L60). These source paths establish protocol/lifetime requirements, not measured compatibility or performance on our laptop/AWS fleet.

### 8.2 What a remote storage read still does

With persistent connections established, an initiator sends a command capsule/LBA request; the target validates it and performs the bdev/NVMe read; data is transferred to registered initiator RAM; a completion capsule returns. SPDK's read path uses target-to-host RDMA WRITE for data, then completion SEND. These are work requests, not necessarily individual packets. It is one logical initiator↔target exchange plus target storage work, not a CPU RDMA instruction dereferencing NAND. No physical switch-hop count can be inferred from AWS cluster placement.

### 8.3 WAL safety and experiment isolation

`ValueRecordReference`/`PhysicalValueAddress` identify partition, segment generation and checked logical file/member offsets—not safe namespace LBAs. Extents can move, files can be retired/reused, and snapshot/cut/CQ owners can still hold them.

Never use FIEMAP to bypass a live mounted filesystem: [kernel guidance](https://docs.kernel.org/filesystems/fiemap.html) says direct block access from such mappings is undefined while mounted. Never bind a filesystem-owned NVMe PCI controller to SPDK/VFIO; other namespaces/mounts may be detached. Use a dedicated disposable device/namespace or independently constructed immutable image with exclusive ownership and verified read-only enforcement. A file backend that opens O_RDWR is not made read-only by intent.

Before integration require a sealed immutable segment manifest, logical-to-exported offset mapping, exact generation/authority/durable-frontier binding, bounds/member checksums, read/cut pins through completion, revocation/remote quiescence before reuse/GC, and stale/late-completion rejection. Keep active segments and unqualified mappings on the current file-aware owner RPC path. Preserve writer durability, recovery, publication and fallback.

### 8.4 Future experiment matrix and go/no-go

On the same verified hardware/device/CPU budget compare: local file/direct read; current file-aware RPC; NVMe/TCP; NVMe/RDMA only on proven IB/RoCE/provider support; registered-RAM RMA as a separate network/progress control. Fix source bytes, alignment, QD/C1/C16, data layout, security and polling/interrupt mode; count errors, CPU, block/NVMe completions, copies and bandwidth as well as latency.

First establish measurement-scoped actual misses and native actual-miss evidence. A direct-I/O request is not proof of NAND-media access if lower device caching is unknown. Export command/target queue, bdev submit→complete, transfer and initiator completion clocks with their own counts; do not add nested times. Define material effect threshold and matched repetitions before running. Include out-of-bounds, short/corrupt reads, stale generation, concurrent retirement, cancellation, disconnect and teardown fixtures. Stop if exclusive ownership, read-only enforcement or completion lifetime cannot be proved.

Pursue storage offload only if it materially improves correctly qualified end-to-end cold reads at acceptable CPU/security/lifecycle cost after P1/P2 and attribution fixes. A negative or inconclusive result is valid. No cited paper promises few-microsecond NAND reads.

## 9. Implementation and local evidence

P1/P2 and the local P3 qualification are implemented on `perf/spill-residency-batch-cache`. Two Rust implementer agents owned the independent code slices; a paper researcher checked QMD/arXiv and primary implementation sources. P4/P5 remain the explicitly scoped later attribution/hardware gates, not claimed completed features. No cloud resources were launched, no workflow was dispatched, and no commit/push was made.

### 9.1 Final code mapping

| File and stable symbol | Implemented behavior |
|---|---|
| `reference_directory.rs:124`, `CachedAuthority` | Separate cache-entry credit from the immutable descriptor Arc's core credit |
| `reference_directory.rs:610`, `retain_adopted` | Consume already captured Arcs in cache-lock windows of at most32 entries; skip optional retention when credit cannot fit; preserve a concurrent duplicate fill |
| `reference_directory.rs:635`, `lookup` | Reserve before decoding; split core/entry ownership; release unused duplicate reservations through normal ownership |
| `reference_directory.rs:1004`, `start_background`; `:1156`, `persist_pending` | Adopt the durable root first, release authority/history guards, then perform bounded warming; persistence and failure fencing stay authoritative |
| `remote_db/pending_read/concurrent.rs:93`, `get_many_cut`; window loop`:142` | One cache acquisition per eligible32-slot window; release before route/group growth; preserve scalar reads, index→cache order, cuts, pins, result ordering and remote grouping |
| `storage/tests/history.rs:477,509`, plus existing credit/recovery fixtures | Post-spill first touch without reload, bounded-pressure eviction, independent entry/core lifetimes, explicit cold eviction and failure/reopen checks |
| `remote_db/concurrent/tests.rs:625` |70-key mixed/duplicate/absent/malformed results across window boundaries and old/latest cuts |
| `local_db/tests/lifecycle.rs`, `bounded_windows_pins_compaction_retirement_and_exact_retries_survive_reopen` | Replace an obsolete immediate-eviction count assertion with the configured metadata-byte bound; retain retry/snapshot/GC behavior checks |
| `benchmarks/kv-research/ADAPTERS.md`, coordinator decomposition | Document acquisition versus member denominators and adoption-clock scope |

No native mutex, WAL format, write durability policy, publication rule, transport protocol or security mode was removed/changed. No dependency was added. No Sui or archived middleware modification was needed.

### 9.2 Correctness and executable validation

| Check | Observed result |
|---|---|
| `cargo +1.90 test --locked --offline -p tidehunter --features experimental-fabric,test-utils --lib` |867 passed,0 failed,45 ignored |
| `cargo +1.90 test --locked --offline -p kv-research --features fabric,tikv,cassandra,foundationdb,valkey-tcp --lib` |55 passed,0 failed |
| Explicit ignored software-provider cold-authority/hot-progress/fencing fixture |1 passed; software transport only, not hardware-RDMA evidence |
| Clippy for engine library/tests with`-D warnings` |Passed |
| `cargo +1.90 fmt --all -- --check` |Passed |
| Matching all-adapter release rebuild |Passed; SHA256`3f406bf95f8b95726559672a4779bebc42cd6498dff4b80a1893dd9dd8031b24` |
| Existing CLI, five fresh external owners per local case |16 cases,106,496 measured read calls;239,616 successful API calls including preload/warmup;0 wrong values/errors/rejections/timeouts |
| Owned process/data lifecycle |All80 owner processes exited cleanly; only fresh task-owned database/storage trees removed after evidence capture |

Initial integration exposed two new fixture receiver type errors, an old immediate-eviction assertion and a deliberately cold fixture racing background warming. These were fixed before the final checks. Cold fixtures now join the first spill before explicit eviction. The binary test target has0 tests; it is not counted as validation. Ignored hardware tests are not claimed passed.

### 9.3 Matched local observations

Raw configurations, CSV samples, boundaries, diagnostics, source hashes, frozen binaries, logs and full API/scheduling/response distributions are retained under workspace path`checkpoint-work/local-spill-batch-2026-10-30/`. See [full local measurements](../../../checkpoint-work/local-spill-batch-2026-10-30/MEASUREMENTS.md), [machine-readable comparison](../../../checkpoint-work/local-spill-batch-2026-10-30/local-comparison.json), and [verification](../../../checkpoint-work/local-spill-batch-2026-10-30/verification.json).

Baseline binary SHA256:`31b8e75d819f84af18bc3627aa824cf699d5fe9174b9b1f426a50e1cf343c24f`. Both versions used Rust1.90, the same all-adapter features, seed20261024, logical namespaces/keys, four read lanes, two owner workers, resident-first policy, budgets, warmup and per-case samples. Each original cell has one independent repetition. Profiled twins and unprofiled latency observations remain separate.

| Unprofiled local case | Before API p50 µs | After API p50 µs | Before API p99 µs | After API p99 µs |
|---|---|---|---|---|
| First-touch256B scalar,C16; explicit coordinator/owner value caches off |76.637|46.334|294.052|177.247|
| Coordinator-hit8KiB×32,C16 |37.239|30.875|269.257|112.610|
| Coordinator-hit1KiB scalar,C1 |0.743|0.592|1.391|1.416|
| Coordinator-hit1KiB scalar,C16 |0.893|1.127|3.591|2.288|

The first-touch interval's authority history-load attempts changed4,535→0. Its profiled twin changed4,579→0; sampled planning mean42.466→2.327µs, broad owner handler mean47.807→8.001µs. Both source versions recorded15 successful background spills in these first-touch owner lifetimes. Explicit value-cache hits were zero. All measured owner value reads completed through the resident NOWAIT WAL path, with zero async submissions. These are OS-resident WAL values, not qualified physical-NVMe misses.

The batch profile selected256 API calls/8,192 member lookups. Acquisitions changed8,192→256; accumulated lock wait63,342,350→8,328,132ns, or247.431→32.532µs per selected API. Per-acquisition means use different denominators. Selected-prefix clocks are not every call's latency, and nested owner/transport clocks must not be added.

Every coordinator-hit case was qualified by exact logical hit counts, zero cache misses and zero read RPCs; the batch interval has65,536 hits. No new native timing is presented here. Use the retained cloud campaign for previous native/store comparisons.

**Scalar uncertainty remains:** the predeclared triggers caused only matched scalar-control repeats, using both frozen versions and unchanged settings. C16 repeat p50 was1.018→1.116µs and p99 was2.172→4.203µs; the initial tail moved in the opposite direction. C1 median movement also reversed. The scalar implementation was unchanged, but that alone does not establish no regression. Do not claim a scalar speedup, tail non-regression, confidence interval or statistical insignificance from these observations. Full unpooled results are in [control-repeat comparison](../../../checkpoint-work/local-spill-batch-2026-10-30/control-repeat-comparison.json).

Read cases retain8,192 measured calls; batch32 retains2,048 calls. p99.9 has only8 or2 observations beyond its nearest-rank position. The full report preserves p50/p95/p99/p99.9, mean, population SD, maximum, throughput exposure and error/rejection counts; no zero substitutes or run-to-run error bars.

Host counters across the case windows recorded26 swap-in pages and0 swap-out pages; prior swap usage existed. Minimum boundary MemAvailable was20,552,676KiB. These are host-window observations, not proof of per-process peak memory or a generally swap-free machine. Previous benchmark datasets/reports were untouched.

### 9.4 What these results do and do not establish

The targeted residency and acquisition mechanisms work and improve the measured local first-touch/batch-tail cases. They do not establish cluster parity, hardware EFA performance, a pure network time, physical-NVMe latency or scalar tail stability.

Remaining qualification: matched native/TCP/EFA/EFA-direct hardware runs with frozen cases and explicit cache/path labels; P4 worker/flight/write-clock coverage; P5 measurement-scoped actual-miss/native attribution; and the isolated NVMe-oF matrix only after its safety/provider gates. Retained cache credits still participate in the existing budget-pressure spill heuristic, so high-occupancy/small-pending-tail settings also need qualification;15-spill local cases do not prove unchanged spill frequency for all workloads.

No benchmark-branch publication, AWS run or transport/storage rewrite is included in this local delivery. The project-referenced`kvstore-*` skill directory is absent, so no in-repo skill could be updated; the changed accounting/profiling rules are documented here and in`ADAPTERS.md`.

## 10. Valkey-informed follow-up, 3 November 2026

Implementation branch: `feat/valkey-inspired-wal-batching`. This is a local
follow-up to the completed GCP comparison, not a new cloud result. The coordinator
index, whole-batch owner placement, independent owner Value WALs, durable receipts
and snapshot/recovery model remain unchanged. No Sui repository or middleware
change is required.

### 10.1 Mechanisms and source mapping

| Measured or source-proven cost | Implemented change | Source |
|---|---|---|
| The writer actor could wait for checkpoint admission instead of returning already-durable replies | Keep one prepared FIFO head without allocating a logical version; progress persistence and deliver published replies while capacity clears | `remote_db/concurrent.rs`: `DeferredWrite`, `drive_owner`; `remote_db.rs`: `prepared_pressure`, `admit_prepared_with_progress` |
| A checkpoint starting halfway through the bounded tail had limited suffix headroom | Start background capture at one-quarter of the existing tail batch/byte limits; retain all hard bounds and index/identity pressure checks | `remote_db/checkpoint_lifecycle.rs`: `checkpoint_due` |
| Read-flight sharing scanned live flights and their members under one mutex | Fixed-capacity exact `(owner, full immutable reference)` hash index under the existing mutex; reclaim entries with retired slots and charge index storage at startup | `remote_db/pending_read.rs`: `FlightRegistry` |
| Warm owner values still entered asynchronous I/O completion handling in ordinary comparison cases | Use the existing bounded Linux NOWAIT resident-first path; retain partial/miss/unsupported/budget fallback to async reads | `backends/tidehunter.rs`: `Options`, `prepare_external_case_file`, `open_for`; `cloud_campaign.py`: standard cohorts; `cloud_controller.py`: `prepare_tide` |
| Adjacent members could need separate asynchronous operations and completion bookkeeping | Coalesce only ascending, gap-free, same-file/log authorized ranges, at most 32 input members and 64 KiB; scatter duplicates and original slots; use bounded stack scratch | `tcp/read_executor.rs`: `read_run_end`, submission and completion |
| Response data must outlive kernel completion and caller cancellation | Every scalar/resident/coalesced response owner retains complete file/log/authority plans through the final `Bytes` clone; late CQEs retain ownership | `ReadBuffer`; `RecordReadPlan` and `WalMemberReadPlan` clone shared pins, not value buffers |

Paths in the table are below `tidehunter/src/distributed/` unless explicitly
named otherwise; native `WalMemberReadPlan` is in `tidehunter/src/wal/mod.rs`.
The benchmark adapter is under `benchmarks/kv-research/src/backends/`, and cloud
planning is under `experiments/kv-comparison/`.

No native mutex is removed and no new global production mutex is introduced.
The flight registry reuses its existing mutex; the writer head is actor-owned;
owner reads reuse the existing admission/pool guards. Fixed registry storage
reduces the payload allowance available under small read budgets, and too-small
configurations now reject explicitly. Snapshot capture/install locking remains:
removing it without changing capture and read-lease contracts would be unsafe.

The earlier capture threshold can increase checkpoint I/O. Coalescing has no
benefit for nonadjacent ranges or differently ordered inputs; it does not sort
random requests or read unrequested gaps. These are bounded mechanisms, not a
promise that every workload improves.

### 10.2 What is adapted from Valkey

Pinned reference: Valkey 9.0.7,
`fe8d406c0b5a64ec897f6455e4de684629883764`.

- [`t_string.c`, GET/MGET](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/t_string.c#L282-L297):
  keep the resident command path short and amortize grouped work. Tidehunter
  already groups misses by owner; this patch optimizes execution inside that
  path instead of adding another routing layer.
- [`aof.c`, persistence](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/aof.c#L1034-L1040)
  and [`server.c`, reply ordering](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/server.c#L1894-L1917):
  preserve persistence-before-acknowledgment while separating maintenance and
  progress. Tidehunter already shares persistence prefixes; the patch does not
  remove fsync, add a second log, or invent group commit.
- [Native Tidehunter v2](https://arxiv.org/html/2602.01873v2), §§3–4:
  resolve the exact Value-WAL position, preserve whole-batch replay and pinned
  immutable storage, and distinguish Linux-resident data from device I/O.
  The new owner path adds no application value cache.
- [eRPC](https://www.usenix.org/conference/nsdi19/presentation/kalia):
  bound short-path work and keep progress from waiting behind maintenance.
  Its RAM-RPC latency is not a guarantee for this WAL-backed implementation.

No Valkey source is copied. Its RAM dictionary, AOF rewrite and RC stream are
not replacements for Tidehunter's WAL-as-values storage or reference validation.

### 10.3 Qualification boundaries

The benchmark already runs synchronous backend operations on dedicated OS
workers (`runner.rs`: `WorkerPool::new`). An intermediate extra CPU semaphore
and in-place blocking boundary increased native cache-hit overhead and was
removed before the final candidate. Its raw results are retained separately;
they are not pooled with the final measurements or used to claim a speedup.
The existing workload concurrency, operation counts, worker queues, durability
barriers, clocks and percentile definitions remain intact.

New diagnostic counters `write_concurrent_checkpoint_deferrals` and
`write_concurrent_checkpoint_wait_ns` are exported through both Sui metric
visitors, the measurement-window write profile and the Prometheus diagnostic
allowlist. They describe deferred admission, which can include other capacity
pressure; they are not pure checkpoint I/O time.

Normal cloud cohorts explicitly request resident-first, cold standard cohorts
explicitly disable it, and focused diagnostic controls retain their policies.
The adapter checks the actual owner policy against the requested one. Owner
logical-member counters and asynchronous operation counts are separate now that
one I/O can cover multiple members; neither proves a physical NVMe access.

Local evidence is retained under
`checkpoint-work/valkey-inspired-local-20261103/`: frozen old binary, new binary
identity, complete configs, raw samples, outcome counts, nearest-rank API,
scheduling and response distributions, and qualification logs. One shared
laptop SSD and loopback TCP do not reproduce five independent NVMe drives or
hardware RDMA. The completed GCP report remains the cluster comparison until a
new hardware campaign is authorized and completed.

### 10.4 Final local measurements and remaining work

One independent repetition per workload and binary. Five loopback TCP owners
share this laptop's SSD. Coordinator value cache is disabled; Linux page cache
is not cleared. The old owners force async reads; the final normal owners use
resident-first. These combined changes do not isolate an individual mechanism.
Batch times cover the complete 32-value call, not one value.

| C16 API, µs | Old p50 | Final p50 | Old p99 | Final p99 |
|---|---:|---:|---:|---:|
| Native, 1 KiB read | 1.388 | 1.498 | 2.416 | 4.269 |
| Native, 8 KiB read-32 | 92.881 | 102.065 | 197.002 | 208.074 |
| Native, 1 KiB durable write | 4463.903 | 4443.166 | 6545.708 | 6882.843 |
| Distributed TCP, 1 KiB read | 77.956 | 54.093 | 381.936 | 364.573 |
| Distributed TCP, 8 KiB read-32 | 2266.419 | 2574.643 | 5924.537 | 6525.542 |
| Distributed TCP, 1 KiB durable write | 2387.564 | 2437.439 | 10834.189 | 10211.082 |

The point-read median fell by 30.6% in this run. **The batch median did not
improve: it was 13.6% slower.** The write median was 2.1% slower; its tail moved
in the other direction. These results do not establish improved batch/write
performance or cloud parity. The next cluster comparison must retain point
controls and investigate batch coordination/completion and write-tail pressure,
not assume fewer owner I/O submissions solve the complete API path.

All 57,344 measured calls across old/final binaries succeeded. Per case:
8,192 scalar reads, 2,048 read-32 calls, or 4,096 writes, with unchanged preload
and 512 warm-up calls. Raw API/scheduling/response samples, nearest-rank
p50/p95/p99/p99.9, mean, population standard deviation, maximum, throughput and
zero observed rejection/error counts are in `final-matched-summary.json`.
p99.9 has fewer than ten expected observations above its quantile for these
sample counts. No confidence interval, significance claim or run-to-run error
bar is warranted.

The predeclared relative tail trigger flagged native point reads. The absolute
p99 difference is 1.853 µs; native execution code is unchanged apart from two
measurement-description strings. No native-tail non-regression claim is made.
An extra repetition cannot change the stated few-microsecond native versus
tens-of-microseconds remote and millisecond batch conclusions, so no repetition
was added merely to narrow that variation.

The final point-read interval recorded 8,161 NOWAIT successes and four async
submissions across 8,165 logical owner reads. The batch interval recorded
60,557 NOWAIT successes across 62,042 logical reads, with 1,485 async
submissions. Both had zero owner errors/cancellations. This proves the resident
path was exercised, not physical-NVMe or RDMA latency. Shared fills explain why
logical owner reads need not equal API member counts.

Separate diagnostic controls captured 320 sampled owner append requests
(64 per connection) and 256 completed physical read flights. Their new
deferred-admission counters and read decomposition passed the existing
diagnostic formatter and typed Prometheus parsing/cardinality validation.
The write control observed zero foreground checkpoints and zero admission
deferrals; the paused-checkpoint regressions separately exercise deferral and
failure. These diagnostic samples are not pooled with unprofiled API timings.
Host-boundary counters observed zero swap-in and zero swap-out pages during the
final six-case run; this is not a per-process peak-memory claim.

Qualification:

- Standard distributed engine: 862 passed, seven ignored.
- Distributed engine with fabric, NVMe-oF and test utilities: 878 passed,
  21 ignored; ignored cases do not establish hardware coverage.
- Final owner-executor check after replacing scatter heap scratch: 36 passed.
- All enabled comparison-adapter library tests: 70 passed.
- Comparison orchestration: 420 passed, four skipped; observability: 107 passed.
- Standard Clippy with `-D warnings` and formatting passed. The broader
  optional-surface pedantic invocation still reports inherited diagnostics
  (30 library / 34 library-test errors), including old Valkey FFI casts and
  unmodified control/runner code. It was not silenced; source provenance is
  retained in `lint-source-provenance.json`.
- Existing diagnostic families lack optional Prometheus HELP descriptions, so
  strict style lint is not clean. Production metric types and real diagnostic
  values passed parsing/cardinality validation; that is not a full style-lint
  or new live Grafana-dashboard claim.
- The current `ci_prepare.run_tcp_diagnostic` completed with five real external
  owners, admitted its integrity-bound proof and recorded five zero exit codes.
  No GitHub dispatch or cloud allocation was performed.

The exact final executable is retained as `bin/kv-research` with SHA-256
`f7d4c34d298af1dcdbe1c74663f3a4d12639cd79b591d25a7d25bf8d82e58248`.
Hardware TCP/verbs/EFA, independent-SSD scaling and the effect on the GCP Valkey
comparison remain unmeasured for this revision. No project skill files were
present to update. No commit or push was made.

### 10.5 Exact-input concurrent-write diagnosis

The C16 1 KiB write diagnostic for the retained binary is in
`checkpoint-work/valkey-local-comparison-20261103T140423Z/exact-input-diagnostics/write1k-c16/result/write-diagnostics.json`
in the parent workspace. It sampled 2,210 owner append requests covering 4,096
receipts: 2,180 file persistence calls and 30 persistence no-ops. Summed owner
file-persistence time was 2.602 seconds; these overlapping cumulative clocks
are not an end-to-end latency decomposition or evidence of physical device I/O.
The coordinator interval recorded 11 background checkpoint starts, 70.5 ms of
capture/install clock, and no checkpoint admission deferrals. This single
diagnostic does not establish whether a different checkpoint threshold improves
tail latency or justify changing the hard suffix bounds.

The storage persistence operation already captures the complete appended prefix
at its serialization boundary, not just the receipts in the invoking request.
Draining already queued requests into one call would not cover groups appended
after that capture; the 30 no-ops do not establish that such a drain would
materially reduce the observed file syncs. The five measured storage nodes are
independent processes, each with its own active log and persistence owner.
Adding parallel per-log flushers on one node is therefore not evidence-based for
this measured configuration. Neither change was made on the basis of this
diagnostic.

The pinned Valkey source writes the accumulated AOF buffer and, under
`appendfsync always`, synchronously calls `fdatasync` before serving pending
replies (`src/aof.c:1112–1124,1238–1257`, `src/server.c:1894–1917`).
Commands already processed in an event-loop turn can share that barrier; this
does not imply an intentional batching delay. Tidehunter's existing prefix
capture likewise shares a barrier among complete groups already appended,
while allowing later groups to append during an in-progress sync.

Matched-input C1 diagnostic twins are retained under
`checkpoint-work/valkey-local-comparison-20261103T140423Z/corrections/exact-input-diagnostics/`
as `write1k-c1-before` and `write1k-c1-new`. Each selected 4,096 append
requests and recorded 4,096 successful file persistence calls. The experimental
`70eb` TCP completion-parking candidate reduced server polling but its mean
file-sync elapsed time rose from approximately 325 to 964 µs, while profiled
API p50 rose from approximately 471 to 1,482 µs. Client socket read calls
rose from 2.95 million to 8.11 million. These are two single-run observations:
the cause of the slower sync is not isolated, and nested clocks are not additive.
The prototype was **rejected and removed**; its retained artifacts are not the
current implementation. Neither this result nor the source comparison establishes
a complete fix for concurrent write latency.

### 10.6 Owner-scoped coordinator read progress

The next local diagnostic found a separate client-side serialization point.
With the rejected `70eb` executable, 242 of the first 256 physical 8 KiB
read-32 flights ran through the single coordinator read actor. Mean actor
service was 1,447 µs per flight; owner handler mean was 30 µs per flight.
The independent 256-API resolution prefix recorded 2,536 µs mean batch wait,
1 µs post-read cache accounting, and 3.6 µs result materialization. These
independent populations and overlapping clocks are not an additive latency
decomposition. They rule out post-read cache accounting as the millisecond
cost in this diagnostic, not every possible cache-contention problem.

The read service now has one persistent progress actor per owner endpoint,
rather than one actor for all owners. Each actor advances only its owner's
lanes, so a held driver or expensive response on another owner cannot block
its progress loop. Global flight, request and retained-byte credits remain
shared. Each bounded command channel can hold `flight_slots + 1` commands;
fixed channel metadata therefore scales with the configured owner count and
is separate from the existing retained-read-byte allowance.

Grouped lane reservations identify the caller that drives a connection, not
exclusive request admission. Compatible requests may join that lane under the
existing credits. Scalar synchronous reservations remain exclusive. An owner
batch with no admitted reads releases its reservations before waiting for its
next byte-credit wave; otherwise foreign accepted work could hold the credits
while neither actor nor caller drives it. Shutdown drops all owner senders and
drains accepted work. Shared NVMe-oF reader shutdown also waits for each actor
to submit or fail its queued work before closing reader admission; terminal
completion and pinned-memory retirement remain required.

Code map:

- `tidehunter/src/distributed/remote_db/pending_read.rs`: owner actor channels,
  lane routing, progress ownership, finite credits and shutdown.
- `pending_read/tests.rs` and `pending_read/locality_tests.rs`: joined requests,
  byte-wave handback, cancellation, cross-owner isolation and shared-reader
  shutdown regressions.
- `remote_db/read_cache.rs` and `pending_read/concurrent.rs`: bounded
  disabled-cache accounting and selected batch-wait/fill/materialization clocks.
- `remote_db/coordinator_profile.rs` and
  `benchmarks/kv-research/src/backends/tidehunter.rs`: snapshot and JSON stages.
- `experiments/kv-comparison/observability/campaign_exporter.py`: exports the
  new clocks while retaining historical evidence without inventing missing
  zero-duration observations.

This follows exclusive progress ownership from eRPC and the already examined
Valkey event-loop design; it does not copy their transport or replace native
WAL recovery. Native Tidehunter, the wire protocol, owner Value-WAL durability,
batch boundaries and fencing are unchanged. No native storage mutex was
removed. This is not yet evidence of hardware RDMA or independent-NVMe speed.

### 10.7 Targeted completion wakes and current local qualification

Batch completion previously used the global admission condition variable.
Every physical reply could wake unrelated batches, although their own values
were not ready. `FlightSignal` now registers batch-local signals under its
existing wait lock. Each `ReadRequest` removes its subscriptions before its
flight references and credits are released. The common one-consumer case uses
inline `SmallVec` storage. Overflow capacity is charged and shrunk on removal;
retirement frees the old subscriber list. Poison and shutdown wake waiters but
do not mark unfinished reads ready. No new global mutex was added.

Retirement holds the flight lock before publishing readiness and through the
subscriber drain. This prevents the retiring generation from consuming a new
generation's subscribers. The tests cover this race, completion before
registration, unrelated-waiter isolation, canceled coalesced consumers,
poison/stop wakeups, byte-credit waves and exact result ordering.

A busy grouped lane also asks its caller to hand progress back to the owning
actor. The caller releases its actual reservation; the actor does not steal
the guard. Scalar synchronous reservations remain exclusive. Diagnostic
`grouped_inline` classes identify **admission origin**, not which thread
eventually completes the flight. Their elapsed clocks are not CPU time.

Current executable SHA-256:
`b1d766ea44265677a8dabeb1a48f541856163c8dd2d494913a64c3e78874ed31`.
Evidence is in the parent workspace under
`checkpoint-work/valkey-local-comparison-20261103T140423Z/targeted-wakes/`
and `targeted-wakes-write-controls/`. Both passes use this same executable.
`all-current-source-comparison.json` links the two passes; `all-metrics.csv`
contains every case's separate API, scheduling and response distributions,
p50/p95/p99/p99.9, mean, population standard deviation, maximum, throughput,
rejection counts and sample-count warnings.

One independent repetition per case; timings below are **µs per API call**,
not per member of a read-32 call:

| Case | Earlier Tidehunter p50 | Current Tidehunter p50 | Paired Valkey p50 | Current Tidehunter p99 | Paired Valkey p99 |
|---|---:|---:|---:|---:|---:|
| 1 KiB read, C1 | 15.813 | 14.641 | 37.558 | 56.728 | 171.920 |
| 1 KiB read, C16 | 55.260 | 53.119 | 48.977 | 239.827 | 217.184 |
| 1 KiB read-32, C16 | 2232.671 | 1110.595 | 313.069 | 3682.621 | 728.555 |
| 8 KiB read-32, C16 | 2323.854 | 1246.531 | 520.404 | 3617.636 | 1204.564 |
| 1 KiB durable write, C1 | 476.450 | 516.720 | 931.989 | 13180.491 | 1968.195 |
| 1 KiB durable write, C16 | 6843.308 | 7968.080 | 2396.493 | 34934.841 | 5470.015 |

The earlier column uses the frozen `f7d4c34d` executable. The targeted-wake
read experiment also passed its predeclared threshold against the intermediate
`9b309b29` owner-actor executable: batch medians fell by 22.4% and 23.6%,
without a 20% scalar-median regression. This is not a confidence or
statistical-significance claim. No populations or repetitions were pooled.
The p99.9 sample-count warnings still apply.

All 57,344 current-source measured calls returned correct results, with no
rejections or unknown mutations. Valkey used five real primaries, the pinned
9.0.7 server, and the paired libvalkey TCP client. Both stores acknowledged RF1
durable writes; Tidehunter used its Value-WAL barriers, Valkey AOF `always`.
Tidehunter used mutual TLS and Valkey plaintext TCP, so this is not a pure
transport-cost comparison. Tidehunter's coordinator cache was disabled; owner
reads used Linux's page cache, while Valkey values remained in its heap.
There was no application value cache on Tidehunter owners.

The batch-read gap remains about 2.4-3.5x. The write tail remains unresolved:
the intermediate owner-actor run had a 2.404 ms C16 write median, whereas the
current run had 7.968 ms. The recorded writer, checkpoint and persistence
source hashes did not change between those executables. That does not prove
the new reader has no indirect effect, nor isolate SSD or scheduler variation.
Do not claim write parity from the faster intermediate run.

A separate current-source write diagnostic sampled 2,206 append requests:
WAL file-persistence time averaged 1.145 ms per sampled append, append work
132 µs per sampled append, and client exchange 2.069 ms per connection request
(2,261 requests, including controls). It observed no foreground checkpoints
or checkpoint-admission deferrals. These nested, differently counted clocks
are not additive and do not establish isolated device latency.

Final read diagnostics put mean batch wait at 1.155 ms and 1.262 ms for the
1 KiB and 8 KiB cases. Post-read fill/accounting was below 1.1 µs per selected
API; result materialization was below 3.9 µs. Owner handler means were about
23 µs and 44 µs per sampled physical flight. Independent API and physical-flight
prefixes must not be subtracted as a measured network latency.

The host recorded zero swap-in and swap-out page deltas during these passes.
All local benchmark containers exited; no unrelated container was removed.
The engine suite passed 891 tests with 21 ignored, adapter tests passed 70,
and observability tests passed 107. Both scoped Clippy checks with
`-D warnings`, formatting, and the release build passed. Real diagnostic
export passed the retained Prometheus parser/cardinality check. Existing
missing HELP descriptions still fail optional full metrics style lint.

These results qualify the code locally, not hardware RDMA performance. The
next bounded GCP campaign must use rebuilt, source-bound artifacts and keep
Valkey RDMA and Tidehunter native verbs separate from these TCP results.
The 56-case matrix retains all workloads and counts with one repetition.
Its `cold_os_cache` action does not evict Valkey heap values; those rows are
not a matched physical-NVMe comparison. Independent owner SSDs are required
to resolve the remaining storage and tail-latency questions.
