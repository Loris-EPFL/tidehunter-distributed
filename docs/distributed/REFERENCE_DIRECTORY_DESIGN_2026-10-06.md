# Immutable membership directory for distributed Value WAL reads

Started 6 October 2026. Latest implementation update: 28 October 2026.
The original segmented-directory design below is historical, not the current layout.

## Dependency-aware sparse publication, 28 October 2026

The coordinator still assigns each complete atomic batch to one physical owner
by batch identity; later updates to a key can be on another owner. An owner
persists the canonical native Value WAL, including its integrity checks, and
performs the durability synchronization before issuing a durable receipt.
Native locks and durability are not removed or weakened. This remains RF1:
loss of the sole owner's durable data is not masked by replication.

Live publication is distinct from the stable contiguous durable/published
prefixes. A durable batch can publish and receive its reply beyond an earlier
gap when its outstanding predecessors have resolved. Dependencies use the
native index identity: keyspace plus reduced key, so collisions intentionally
conflict; repeated keys, deletes and transitive multi-key dependencies are not
independent. Whole batches publish atomically. This is not a claim of general
transactional serializability. No global unique-clock RPC or new owner-to-owner
traffic is introduced in the normal RF1 path.

The outstanding window and its retained credits are bounded; a persistent hole
eventually backpressures admission, not unbounded suffix growth. Unknown
outcomes are not timeouts-as-aborts: uncertainty or fencing failure gates
serving, including cache hits, until fenced recovery reconciles complete owner
inventory and retry identities. Recovery merges complete owner batches in
logical version order. Checkpoint images use the genuine stable prefix, and
suffix WAL history must remain available until safely covered; sparse live
visibility must not be mistaken for checkpoint coverage.

A frozen sparse index cut records an upper mutation and excluded unpublished
batches. It is index-scoped and pins the stable historical floor, including
anchors needed when a previously excluded older batch later publishes. Releasing
the view releases its retention charge. This preserves a fixed scan rather than
using an upper sequence alone, which would admit late publications.

Before allocating a sequence or issuing WAL I/O, admission sizes sparse-view
reservations for the actual retained admission window, not the configured
maximum. Under one bounded coordinator index write guard it bulk-preflights the
extra quota for the new token and all incomplete existing tokens, grows those
tokens for every possible new exclusion, and reserves the new token. No external
quota consumer can race the preflight and per-token conversion; concurrent view
drops only free capacity. The first or serial write needs zero view quota.
Allocated sparse live/frozen
view bytes plus outstanding view reservations have their own `max_index_bytes`
cap, separate from both the resident-index and frozen-capture allowances, each
also bounded by `max_index_bytes`. A held snapshot can therefore cause a new
admission to fail cleanly before remote writes, rather than poisoning an already
durable batch for lack of view capacity. Transient metadata statistics sum
capture, view and reservation bytes, so that statistic can reach twice the
configured per-category cap; these accounting bounds are not exact RSS.

Pending credit reserves four times the dependency-entry size per operation.
When publication returns credit, empty keyspace dependency tables are released;
nonempty tables shrink geometrically when capacity exceeds four times live keys.
Thus a stalled writer does not retain the historical table high-water mark.

The supplied separate runtime regression passed: batch A was staged but never
reached WAL, durable sparse batch B was acknowledged before abrupt poison/drop,
and recovery returned B's exact old receipt on retry while a retry of absent A
used the new epoch. Native locks and durability remain unchanged.

### Native Tidehunter invariants rechecked through QMD

The [Tidehunter v2 paper](https://arxiv.org/abs/2602.01873v2) was rechecked
through QMD document `#2a504a`,
`tidehunter-papers/tidehunter-2026-v2.md`: §§3.1–3.4, §§4.3–4.4 and §5.
The atomic-batch paragraph was read separately at lines 127–128 because an
embedded figure truncated the first section response. Native source checks
included `db.rs::do_write_batch`, `db.rs::write_batch_into_wal`,
`wal/mod.rs::WalWriter::multi_write`, `wal/tracker.rs` and `wal/syncer.rs`.
These mechanisms constrain the distributed adaptation; external KV designs do
not override them.

| Native mechanism | Distributed adaptation and preserved invariant |
|---|---|
| §2.3: append values directly to their long-term WAL location; rarely move values | `storage.rs` retains complete batches in owner Value WALs. `reference_directory.rs` and receipt-history maintenance manage authority metadata, not a second value store or LSM value compaction. A later version may use another owner; the index selects its exact physical reference. |
| §3.1: atomic position allocation, then concurrent write/index work | `sequence.rs::LogicalClock` assigns logical versions inside the coordinator process; each owner allocates its own physical WAL positions. No network-global fetch-add is introduced. `publish_completed` no longer makes disjoint live replies wait for an unrelated lower logical version. This adapts native independent completion, not its physical address namespace. |
| §3.1: one WAL allocation covers an atomic batch; replay reconstructs the whole batch | `codec.rs::CompleteBatch`, `group.rs` and `native_publication.rs` retain whole batches on one owner. Transport aggregation combines complete batches but does not split one across owners. The index write guard and frozen sparse cuts preserve the distributed live-view boundary; the paper's batch paragraph specifically establishes crash-recovery atomicity, not general distributed transactions. |
| §3.1 and §5: background synchronization, amortized work and fixed workers | `tcp/control.rs` separates append execution from persistence. Small staged tails coalesce under a two-group owner cap; already-large staged work retains bounded pipelining so append and persistence can overlap. The threshold and cap are distributed engineering choices, not numbers prescribed by the paper. `completed_nodes` is allocated once and reused; no batching timer, sleep or per-request thread is added. |
| §3.2: LRU hit, index lookup on miss, direct Value-WAL access; page cache can serve WAL reads | Existing coordinator and owner caches remain distinct from physical NVMe. `tcp/read_executor.rs` skips resident-prefix staging when all members hit the owner cache; `tcp/reactor.rs` copies references only for requested grant promotion. Exact reference, checksum, fencing and memory-lifetime checks remain. A logical WAL-read counter is not proof of device I/O or a cold read. |
| §§3.1, 3.3–3.4: the contiguous processed frontier protects snapshots and replay | `publication.rs` retains contiguous durable/published prefixes independently of sparse live publication. `checkpoint_lifecycle.rs` captures the stable cut, not the live upper bound; `native_index/retention.rs` pins frozen exclusions and historical anchors. Recovery fences all owners, seals old streams and merges their actual complete batches. A missing unacknowledged old-epoch sequence is not invented to recover an acknowledged later batch. |
| §4.3: persist a frozen index while new writes continue; retain the old index until replacement is complete | Background checkpoint/history capture keeps old immutable authority and its suffix until durable adoption. Snapshot pins and reservations prevent reclamation or allocation pressure from invalidating an already exposed view. This is metadata persistence, not foreground value rewriting. |
| §4.4: relocation reclaims space; it must not replace a newer value or delete a referenced file | Sparse visibility never advances the stable reclamation boundary. Existing logical-version checks, read/grant pins and covered-root rules remain mandatory. This change neither introduces a new distributed value relocator nor claims complete parity with every native relocation feature. WAL fragmentation is not repaired by adding LSM compaction. |
| §5: native `parking_lot` guards and fixed background workers coexist with lock-free components | No native Tidehunter mutex or WAL guard is removed. One authority owns mutable dependency/publication state; index and retention guards still protect atomic views and memory ownership. The system is not described as globally lock-free. |

**Durability difference retained deliberately:** native ordinary writes complete
at the OS page-cache tier and batch stable-storage synchronization in the
background (§3.1). This distributed adapter already waits for the selected
owner's WAL persistence barrier before its durable receipt. The current change
does not weaken that contract to improve a latency number. Write comparisons
must select the explicit native Value-WAL barrier tier or label the stronger
distributed acknowledgement; default-native and owner-synced writes are not
equal durability populations. RF1 still does not survive loss of the sole
owner's durable storage.

### Intermediary fragmentation and correction

The paired, instrumented 1 KiB scalar-write diagnostic twins are in
[`control-put1k-profiled`](../../checkpoint-work/dependency-publication-20261028T191354Z/control-put1k-profiled/result/write-diagnostics.json)
and [`current-put1k-profiled`](../../checkpoint-work/dependency-publication-20261028T191354Z/current-put1k-profiled/result/write-diagnostics.json).
They capture the intermediary implementation, before the dispatch correction:

| Measurement | Control | Sparse intermediary |
|---|---:|---:|
| Measured admitted/completed calls | 4096/4096 | 4096/4096 |
| Coordinator append requests | 1632 | 2778 |
| Durable-receipt publication-blocked batch time, cumulative ns | 1,935,431,294 | 27,617,798 |
| Preparation/admission sum, ns | 102,801,646 | 120,820,776 |
| Publication sum, ns | 16,918,496 | 25,079,233 |
| Owner sampled appended groups / receipts | 1280 / 3246 | 1280 / 1789 |
| Sampled receipts per group | 2.536 | 1.398 |

Coordinator counters are measurement-boundary cumulative deltas. Owner samples
are the first 256 append requests on each of five append-owning connections;
they are not a random sample of all requests. Profiled clocks are cumulative,
overlap and can be nested: neither they nor blocked batch time are additive API
latency components or elapsed wall time. These instrumented observations are
not pooled with clean timings. The increased request count and smaller sampled
groups show fragmentation alongside the reduced publication-blocked sum, not
proof that one counter alone caused the observed performance change.

Separately, the clean paired local scalar run regressed from control p50/p99
4.692185/23.442444 ms and 2906.6583 calls/s to sparse
5.332600/31.564759 ms and 2339.6741 calls/s. Earlier source-bound historical
results below are a different observation, not a matched substitute for this
pair. The local processes share one SSD; swapout increased in both paired runs,
so neither these timings nor the diagnostic twins establish independent-NVMe or
AWS performance.

The count-only two-group cap was an intermediary, not the final policy. Its
clean 1 KiB scalar run improved relative to the earlier clean control, but a
targeted 8 KiB batch-32 pair exposed a large regression. Each row below has
4096 successful measured calls; p50/p99 are API latencies from the raw samples,
not the summary's histogram upper bounds. Calls/s counts batch calls, not keys.

| Intermediary run | API p50 / p99, ms | Calls/s |
|---|---:|---:|
| [Scalar control](../../checkpoint-work/dependency-publication-20261028T191354Z/control-put1k/result/samples.csv) | 4.692185 / 23.442444 | 2906.6583 |
| [Scalar count-only cap](../../checkpoint-work/dependency-publication-20261028T191354Z/coalesced-put1k/result/samples.csv) | 2.377098 / 10.681259 | 5449.3645 |
| [Batch-32 count-only first run](../../checkpoint-work/dependency-publication-20261028T191354Z/coalesced-write8k-batch32/result/samples.csv) | 13.272565 / 103.284664 | 801.3851 |
| [Batch-32 targeted control](../../checkpoint-work/dependency-publication-20261028T191354Z/control-write8k-batch32/result/samples.csv) | 16.244865 / 74.894842 | 720.9751 |
| [Batch-32 targeted count-only repeat](../../checkpoint-work/dependency-publication-20261028T191354Z/repeat-coalesced-write8k-batch32/result/samples.csv) | 39.303955 / 226.997191 | 276.2087 |

The [coalesced resource summary](../../checkpoint-work/dependency-publication-20261028T191354Z/coalesced-resources.json)
reports system-wide swap-in/out deltas of 28/0 pages for scalar, 64/45,580
for the first batch write, and 51/46,322 for the batch read. The
[targeted batch resource summary](../../checkpoint-work/dependency-publication-20261028T191354Z/batch-recheck-resources.json)
reports 2,022/228,740 pages for control and 24,635/154,370 for the repeat;
their peak sampled aggregate owned-process RSS was 242,982,912 and 243,822,592
bytes respectively (about 243 MB). These swap counters are system-wide, not
attributed to our processes; they do not establish that swap caused the
regression or was its sole cause. The first batch result and targeted pair are
separate observations, not pooled final results.

The byte-aware correction applies `APPEND_COALESCE_MIN_BYTES = 32 * 1024` to
each owner's staged encoded bytes including the group header, using
checked/saturating arithmetic. Below that threshold, if two groups are already
outstanding, further whole batches remain in bounded staging and can coalesce;
at or above it, normal bounded dispatch permits the prior pipelining even with
two outstanding groups. Global pending batch/byte credits, per-partition
reservations and group frame limits continue to bound work; there is no hard
two-group bound for all payloads. The `ClientWorker` correlates multiple
outstanding appends, and an owner can append a successor while an earlier
captured prefix awaits persistence. Native §3.1/§5 overlap and fixed workers
constrain this adaptation; 32 KiB is an explicit byte-amortization engineering
policy, not a paper-derived or 8 KiB benchmark-specific constant. There is no
timer, sleep, added mutex, read-lane change or per-key placement change.

This is whole-batch append aggregation, not LSM compaction or weaker durability.
Native [`WalWriter::multi_write`](../../tidehunter/src/wal/mod.rs) shares WAL
allocation, and [`WalSyncer`](../../tidehunter/src/wal/syncer.rs) flushes
finalized maps in the background. The distributed
[`worker.rs`](../../tidehunter/src/distributed/worker.rs),
[`append_group`](../../tidehunter/src/distributed/storage.rs) and
[`tcp/control.rs`](../../tidehunter/src/distributed/tcp/control.rs) adapt
aggregation and append/persistence ownership; they are not unmodified native
code. The pinned Valkey `beforeSleep` flush-before-reply
and always-sync links in the historical source table below illustrate the
retained durability-before-reply boundary. The final local qualification below
records the observed improvement and regressions separately; it does not
establish an improvement across all metrics.

Implementation map (paths relative to the repository root):

| Source | Responsibility |
|---|---|
| `tidehunter/src/distributed/publication.rs::PublicationTracker` | Bounded admissions, individual publication state and contiguous durable/published prefixes; unknown-hole recovery requirement. |
| `tidehunter/src/distributed/remote_db.rs::admit_prepared` | Constructs deduplicated predecessor links through `pending_writers`, keyed by `operation.ks()` and `operation.reduced_key()`; chained batch dependencies preserve transitive ordering. |
| `tidehunter/src/distributed/remote_db.rs::publish_completed` | Predecessor checks, authenticated atomic publication, live/stable frontiers and diagnostic counters. `replay_history` merges recovered owner history. |
| `tidehunter/src/distributed/remote_db.rs::{published_cut,protected_published_cut}` | Captures the latest published cut; the protected variant captures and pins it under the index read lock. |
| `tidehunter/src/distributed/native_publication.rs::NativePublicationRouting` | Prepared native-index routing and authenticated publication of a canonical batch. |
| `tidehunter/src/distributed/native_index.rs::{IndexReadCut,SparseIndexView}` and `native_index/retention.rs` | Frozen exclusions and pinned historical retention floor. |
| `tidehunter/src/distributed/tcp/read_executor.rs` and `tcp/reactor.rs` | Cache-hit-only submit skips resident-prefix staging allocation; reference cloning is limited to requested resident-grant promotion. Integrity and lifetime checks remain in force. |
| `benchmarks/kv-research/src/runner.rs` and `backends/tidehunter.rs` | Measurement-boundary read evidence and adapter observations. |

Design inspiration is bounded, not wholesale protocol adoption. The original
Tidehunter paper §§3.1–3.2, snapshots and recovery describes append-oriented
values, indexed reads and recovery; its flushing and relocation discussion
preserves newer logical state. The pinned Valkey and Raft Engine sources in the
historical section below motivate durability-before-reply and bounded
maintenance ownership, not cross-shard atomicity. [BtrLog's paper](https://arxiv.org/html/2606.27051v2)
and [implementation](https://github.com/maxi-k/btrlog) inform bounded owners
and asynchronous persistence; its ordered LSN acknowledgements and compaction
are not this publication protocol. No new Sui repository Rust API change is
required; the Sui adapter changes are internal `Clone` changes.

Python comparison, observability and Sui harness suites passed 266, 77 and 166
tests respectively. These counts are not Rust results or performance measurements.

### Final local qualification and open performance gate

Branch: `perf/async-authority-history`. The final byte-aware source tree is
`bbb5c1a72b6999143534015e56b23865718482f8d61301ef3b5ce144c74b3202`;
the rebuilt `kv-research` executable SHA-256 is
`a8e8900922eb4058b26d6f36aa0663343bfa0e6f57ab35a11445fc110b257c03`.
The exact dirty sources, binary binding, configurations, raw API/response/
scheduling samples, phase-qualified read evidence, diagnostics, resource
observations and complete numeric analysis are retained under
[`checkpoint-work/dependency-publication-20261028T191354Z`](../../checkpoint-work/dependency-publication-20261028T191354Z).
`byte-aware-source/source.json`, `byte-aware-binary-provenance.json`,
`run-plan.json` and `analysis.json` identify the final source/configuration
population. Earlier attempts remain separate, not overwritten or pooled.

The final engine library suite passed **835 tests**, with 42 ignored by the
ordinary invocation; the benchmark library passed **36**. The explicitly
selected software-provider suite then passed **22** tests from the ignored
population. Final Rust formatting and all-target engine/benchmark Clippy with
`-D warnings` passed. Release builds produced `kv-research`,
`sharded_wal_bench` and `sharded_wal_transport_bench`. Python comparison,
observability and Sui harness tests passed 266, 77 and 166 respectively.
This includes actual five-owner TLS storage, not only mocked responses.
It does not include a fresh full Sui-node checkpoint replay or hardware EFA.

An earlier software-provider run, concurrent with the full suite and builds,
timed out at the final `fence_to(2)` of the destination-slot fallback test.
The isolated case and both subsequent serialized 22-case suites passed without
relaxing its timeout. Resource contention is a possible cause, not an
established diagnosis; the initial failure remains recorded in `validation.json`.

All clean rows below used five owner processes, concurrency 16, 4096 keys,
512 warm-up calls, 4096 measured successful calls, seed 1775, TLS/TCP and a
disabled coordinator cache. The references are the preceding buffered-history
implementation's retained local cases. Durability, value sizes, batch widths
and per-case counts were preserved. Each row is one observation per source;
the API columns describe complete calls, never latency divided by batch size.

| Case | Buffered p50 ms | Final p50 ms | Buffered p99 ms | Final p99 ms | Buffered calls/s | Final calls/s |
|---|---:|---:|---:|---:|---:|---:|
| 1 KiB scalar PUT | 2.204482 | 2.583089 | 8.078600 | 11.689929 | 6139.63 | 4925.12 |
| 8 KiB PUT batch32 | 15.300765 | 16.317629 | 57.795093 | 206.204249 | 883.43 | 457.72 |
| 8 KiB GET-many32 | 3.475356 | 3.462331 | 6.940451 | 4.414343 | 4101.29 | 4227.88 |

The intermediate scalar regression improved after coalescing, but the final
scalar result is not faster than the earlier historical control above. Read
tails also varied in the historical repetitions; their earlier 4.646634 ms
p99 is retained rather than claiming a robust large improvement. With only
4096 samples, about four observations lie beyond p99.9. No error bars,
confidence interval or significance claim is made. `analysis.json` retains
p50/p95/p99/p99.9, mean, population standard deviation, maximum, throughput,
outcomes and the separate scheduling/response populations.

The predeclared material-regression thresholds were 20% for p50 or throughput
and 50% for p99. They triggered matched scalar and large-batch checks on
**both** frozen implementations. A final reverse-order batch32 pair ran the
new implementation first and the control second, after removing only this
task's completed databases; each case again used fresh data and unchanged
operation counts. Neither case recorded system swap-out:

| Reverse-order 8 KiB PUT batch32 | Current | Buffered control |
|---|---:|---:|
| API p50 ms | 16.763658 | 13.715796 |
| API p99 ms | 246.731694 | 51.499386 |
| Calls/s | 441.05 | 957.35 |
| Sampled aggregate owned RSS bytes | 243892224 | 238170112 |
| Host `nvme1n1` written MiB, whole case | 2546.04 | 2456.43 |
| Host `nvme1n1` cumulative write ms, whole case | 93046 | 30899 |

Host counters include preload, warm-up, shutdown and unrelated activity.
They are not per-operation device attribution. The reverse-order result means
the regression must **not** be dismissed as only swap or execution order.

The final scalar diagnostic still removes much unnecessary publication
coupling: 2196 of 4096 batches published outside the prefix; four waited on
native-key dependencies. Cumulative durable-receipt publication-blocked time
was 61,009,976 ns, versus 1,935,431,294 ns in the earlier matched control
diagnostic. Append requests were 2212, between the intermediary's 2778 and
the control's 1632. These are cumulative profiled observations, not latency
percentiles or disjoint shares of elapsed time.

Large batches remain a different problem. The separate, identically sampled
large-write diagnostic twins recorded:

| Sampled or cumulative stage | Buffered control | Current |
|---|---:|---:|
| Owner appended groups sampled | 1280 | 1280 |
| Owner receipts in those groups | 2364 | 2029 |
| Owner WAL-file persistence sum, ns | 3247995163 | 16325991382 |
| Owner persistence-state lock wait sum, ns | 109860251 | 38762552 |
| Owner persistence validation sum, ns | 3419478 | 3477682 |
| Owner control queue sum, ns | 1339550857 | 13607293912 |
| Coordinator publication sum, ns | 390198221 | 546952786 |

These populations overlap/nest and must not be added into a latency pie.
The measured extra service time is concentrated in WAL-file synchronization
and upstream queueing, not increased persistence-state mutex wait or receipt
validation. SHA-256 checks establish that the buffered and current
`storage.rs`, `storage/history.rs`, `storage/history/index.rs`,
`reference_directory.rs`, `tcp/control.rs` and native `wal/mod.rs` sources are
identical. The new admission/publication schedule can still change I/O
concurrency and checkpoint interference; identical persistence code does not
prove identical execution cost or exonerate the architecture.

**The large-write tail remains an open measurement issue.** All five owners
and the authority use one physical benchmark volume here. Independent NVMe
devices can remove cross-owner disk contention, but that is not yet an observed
improvement. Adding global disk serialization to make this laptop case faster
would reintroduce the cluster coupling being removed.
No fsync, checksum, conflict, snapshot, fencing or memory-lifetime guarantee
was weakened to hide the result.

The final read-boundary sample recorded 131072 coordinator cache misses and
zero coordinator hits. Its five available owner-executor deltas total 114180
cache hits and 85 logical WAL reads/async submissions. Requested-key and
executor-member counts differ, so these totals must not be mixed into a hit
ratio. They show predominantly owner-RAM service, not a physical-NVMe read
latency benchmark. No hardware RMA was used by these TCP cases.

### Matched persistence-tail investigation, 2026-10-28

The reported 246.731694 ms was a p99, not one isolated maximum: that population
contained 240 calls at or above 100 ms. A fresh matched pair then ran the frozen
buffered control first and the frozen current executable second. Each again
completed 4096 successful measurement calls, with unchanged workload and fresh
owned databases. Neither recorded system swap-out.

| Fresh 8 KiB PUT batch32 case | API p50 ms | API p99 ms | Maximum ms | Calls/s | Calls at least 100 ms |
|---|---:|---:|---:|---:|---:|
| Buffered control | 15.191637 | 198.481659 | 246.228908 | 487.26 | 243 |
| Current | 14.088723 | 66.018367 | 84.081969 | 875.94 | 0 |

This reverses the earlier ordering. It neither proves a current-source speedup
nor supports calling the earlier tail a deterministic architectural regression.
No populations are pooled, and no run-to-run significance claim is made.

Separate intrusive `strace` twins followed all owner processes. In the current
case, a node-0 WAL `fdatasync` lasting 64.266 ms overlapped a proven mapping of
the same WAL's fragment (`msync`, 55.473 ms), a coordinator index-container
`fsync` (57.869 ms), and node-3 WAL synchronization (64.287 ms). Their completion
times clustered within about 0.14 ms. The control showed the same class of
overlap: node-4 WAL synchronization, that WAL's fragment, owner authority-history
and coordinator receipt-history barriers completed within about 0.082 ms.

The mapping and shared-address-space provenance were checked, not inferred from
an unmapped `msync` address. These observations support shared-host persistence
interference as a hypothesis. They do not distinguish physical-device service,
filesystem/journal or inode/writeback coordination, CPU descheduling and tracing
overhead. The coarse wall-clock anchor cannot identify which individual API
sample contains a particular syscall. Traced timings are not clean API results.

Local-only reproducibility evidence is retained under
`checkpoint-work/tail-investigation-20261028T202848Z/`: `plan.json`,
`clean-pair.json`, `trace-pair.json`, `syscall-analysis.json`, the separate raw
case results and per-thread traces. This directory is intentionally not shipped
in the Git source archive. Source and executable SHA-256 checks confirmed the
same frozen implementations; no runtime or durability change was made during
this investigation.

### Approved cloud validation scope

The implementation is locally correctness-qualified for a **diagnostic**
independent-NVMe campaign, not performance-approved or production-parity proven.
Freeze the candidate, rebuild the selected suite's release executable, and
require source-bound packaging and real-server qualification before allocation.

The user-selected campaign uses the existing `kv-comparison` mode on
`bench/aws-efa`, not the Sui suite. Its tracked matrix contains 258 cases across
native Tidehunter, distributed TCP, EFA, direct EFA, native EFA, TiKV, Cassandra,
FoundationDB and Valkey TCP. Preserve the workload counts and one independent
repetition. Keep native page-cache and value-WAL-barrier tiers, configured cache
cohorts, observed read sources and physical-I/O evidence separate. The common
matrix contains scalar writes and batch reads; it does not reproduce this
local atomic PUT-batch32 case, which Valkey TCP does not support.

Collect raw API, scheduling and response samples, engine write profiles,
measurement-boundary cache/WAL evidence, per-process resource counters, host
I/O and the Prometheus TSDB snapshot with Grafana configuration. Verify durable
S3 export before normal teardown, retain the unconditional deadline/watchdog
backstop, download and hash-check the complete evidence, then independently
verify every task-owned instance and temporary attached volume has been removed.
Keep the result objects under the configured retention policy; S3 retention
continues to have a storage cost and is not billed as running EC2 compute.

A separate future Sui campaign must also freeze the compatible Sui checkout
and fixture and rebuild its Sui/native/storage/transport executables. It must
not be presented as part of this synthetic comparison.

No Sui dependency pin or native mutex was changed. Only completed task-owned
WAL datasets were reclaimed; raw results and provenance remain. No project
skill files were present to make stale.

## Historical bounded background history, 28 October 2026

The following subsection records the earlier design and measurements before the
sparse-publication cutover above; its global ordering decision is historical.

## Bounded background history, 28 October 2026

Branch: `perf/async-authority-history`, based on
`6d216d88c49362fe5a90c8762b46f34e572f5ace`. This change is local; no AWS campaign,
commit, push, Sui dependency pin, native `Db` algorithm or wire-format change is
part of it.

### Problem and decision

The previous distributed append path persisted descriptor/receipt pages, four
copy-on-write Patricia indexes and a durable history root while holding partition
state ownership. One slow owner then held back unrelated durable batches at the
coordinator's contiguous publication prefix. These are separate costs: local
maintenance and amplification by global publication.

Keep the Value WAL and the publication contract. Move only authority-history
maintenance off the normal append path:

1. After the native WAL durability barrier, capture a complete durable pending
   prefix. Capture uses physical group ends; it does not root half a group.
2. At most one background history job runs per partition. Its captured Arcs stay
   in the charged pending window until adoption; new appends use remaining space.
3. Write and sync metadata without holding partition state or TCP inventory
   ownership. Readers retain the old immutable root plus the pending tail.
4. Install the durable root, then briefly acquire authority write ownership and
   drain only the captured prefix. Delete covered inventory witnesses afterward.
5. If the hard window is full, release state/inventory ownership before waiting.
   If pins still consume the byte budget, return capacity pressure rather than
   dropping credits or admitting unbounded work.

The first local implementation exposed a large-batch regression. Moving I/O to
another thread did not remove its CPU and shared-SSD cost. The final writer also
combines immutable 256-byte node slots in a fixed **8 KiB** buffer and reads its
own staged nodes from that buffer. It flushes on a full buffer and before node
sync/root installation in both append and retirement. Committed readers remain
disk-backed. Bounds, alignment, digests, old roots and physical addresses are
unchanged; this is not value compaction or a second value log.

### Source and code map

| Source mechanism | Adaptation and implementation |
| --- | --- |
| Native `db.rs::rebuild_control_region_from`: queue cell flushes without cell-lock I/O; barrier/sync; store control region before deleting files | Durable capture, off-lock history I/O and root-before-witness-removal. See `reference_directory.rs::start_background`, `storage.rs::persist_receipts`, and `storage/file_inventory.rs::remove_covered`. Value bytes stay in the native WAL. |
| [Valkey 9.0.7 `aof.c`, pinned fe8d406, incremental rotation/base rewrite](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/aof.c#L2399-L2533) and [durable manifest adoption](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/aof.c#L2649-L2751) | Retain an authoritative old base and active suffix while one maintenance owner works; adopt only after persistence. Our bounded Arc capture replaces fork/COW. No Valkey C code or RAM-resident database is copied. |
| [Valkey `beforeSleep` AOF-before-reply ordering](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/server.c#L1894-L1917) and [always-policy synchronization](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/src/aof.c#L1188-L1251) | Keep the existing owner WAL file/namespace synchronization before a durable receipt. Background metadata is not permission to acknowledge an unsynced value. Valkey can still pause; its absence of our history-spill path is not absence of maintenance. |
| [TiKV Raft Engine group write/sync, pinned 097c499](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs#L138-L235) and [serialized rewrite/purge](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/purge.rs#L80-L127) | Reuse batching and sole-maintenance-owner principles. `storage/history/index.rs::{put_node,node,flush_nodes}` combines slot I/O without changing persistent nodes. No Raft log, unsafe queue, value rewrite, replica or weaker sync mode is introduced. |

Runtime integration:

- `reference_directory.rs`: charged frozen prefix, one history job, root
  adoption, failure notification, joins and optional maintenance counters.
- `storage.rs::{append_group_impl,persist_receipts,wait_history}`: proactive
  scheduling, bounded admission retries and off-state-lock pressure waits.
- `storage.rs::{seal_epoch,retire_files}` and writer drop: synchronize history
  ownership before lifecycle operations; retain old read and file pins.
- `storage/history.rs::{append,retire}` and `storage/history/index.rs`: bounded
  node buffering, checked writer reads and flush-before-root durability.
- `tcp/control.rs::append_profiled`: the first admission attempt retains epoch
  fencing under inventory ownership; history waiting occurs after that guard
  is released.
- `tcp.rs::StorageNode::open` and `tcp/inventory.rs`: each new epoch inherits a
  weak grant-registry failure notifier. A background failure requests the
  existing poison notice/drain protocol without acquiring log/inventory locks.
  Terminal reader authority advances only after remote DMA is proven drained,
  not immediately at notification.
- `examples/support/tcp_cluster.rs::maybe_run_storage_node`: optional local
  per-epoch maintenance JSON at orderly stop, separate from wire responses.

No native engine lock was removed. The new history mutex serializes metadata
writers, not ordinary index/value processing. The job handle mutex bounds
ownership and joining; it is not held by the history worker during persistence.

### Bounds, compatibility and failures

The `1024` resident batch ceiling is a distributed metadata window, **not**
native Tidehunter's database capacity or total retained history. The lower
configured window still applies. Proactive count pressure starts at half the
window; byte pressure also schedules maintenance. Replay-span maintenance starts
at 32 MiB, before the unchanged 64 MiB unrooted-WAL limit. Maximum group size,
metadata bytes, read/grant pins, epochs and work queues retain their bounds.

Frozen metadata keeps its credits until its last owner drops. The fixed charge
includes both Arc windows, worker/notifier ownership and the inline node buffer.
This is a metadata budget, not a process-RSS or thread-stack bound. The 8 KiB
buffer increases the minimum valid metadata budget. An experimental config set
exactly to the old minimum can be rejected before create/recovery filesystem
mutation; the default 64 MiB budget and on-disk format do not change.

I/O, spawn, worker-panic and uncertain root-install failures poison storage.
Failure after rename does not prove that the old root remains on disk. Recovery
must accept the durable root plus valid suffix; it must not acknowledge uncertain
new state or discard a previous acknowledged value. Existing RMA holders are
not freed before their exact drain/quarantine rules permit it.

### Diagnostics and interpretation

`PartitionLog::set_history_maintenance_diagnostics(true)` enables cumulative
`HistoryMaintenanceStats`: started spills, captured batches, maxima, failures,
join/wait calls and time, history persistence time and adoption time.
No per-write timestamp is added when diagnostics are disabled.

For the real-process harness, set `TIDEHUNTER_HISTORY_DIAGNOSTICS=1` on a separate
diagnostic run. Each node writes `launch.history-maintenance.json` beside its
launch file. `epoch_stats` contains `(epoch, counters)` pairs. Scope is owner
handle lifetime through the post-serve/pre-drop snapshot, including preload and
warm-up; an in-flight job can still finish after that snapshot. Wait counts can
include joining an already finished job. Snapshots are not an atomic set.

Keep these clocks separate from measurement-boundary coordinator prefix waits
and sampled connection/storage profiles. Persistence and adoption clocks
overlap request processing; they cannot be added into API wall-time percentages.
The wire profile schema, normal matrix, within-case sample counts and AWS launch
behavior are unchanged. New local snapshots are not new cloud measurements.

### Publication decision

This change does **not** remove global publication ordering. `remote_db.rs`
places complete batches by identity, not by a stable keyspace owner; successive
versions of one key can live on different owners. Therefore per-owner frontiers
alone cannot replace the current prefix. A future dependency-aware publication
design must prove key ordering, retry identity, batch recovery, read cuts,
checkpoint recovery and cache/fence behavior separately. Native `get` and the
native checkpoint latch are useful distinct references, not proof that the
current prefix can simply be deleted.

### Local measurements and verification

`integrity.json` binds the raw files and executable, verifies 260 compiled inputs
against the frozen final source, and records the exact checks. Database cleanup
is recorded separately; it does not delete old campaigns or raw observations.

Artifacts: [`checkpoint-work/history-maintenance-20261028T163102Z`](../../checkpoint-work/history-maintenance-20261028T163102Z/).
`analysis.json` retains all 17 separate populations and exact API, response and
scheduling distributions: p50/p95/p99/p99.9, mean, population standard deviation,
maximum, throughput and outcomes. Original samples, configurations, source
snapshots and binaries remain available. `owner-maintenance-stats.json` preserves
the optional node snapshots independently of disposable database files.

Both releases use Rust 1.90, release mode and `kv-research --features valkey-tcp`.
The baseline is frozen Git revision `6d216d88`; the final Rust sources are bound
by `buffered-source/source.json` and binary SHA-256
`aa54dcc39b1a8e2aaef8e39760b77e793dc2d9b4d2be872a3fb4efe240bca233`.
Each case retains 4096 preload calls, 512 warm-up calls and 4096 measured calls,
concurrency 16, queue 128, seed 1775 and the same key namespace. No errors,
wrong values, rejected calls, unknown mutations or outstanding calls were
reported. Complete batch latency is not divided by 32.

One laptop hosts five owner processes on **one shared SSD**. Coordinator value
cache is disabled; the loopback harness retains its configured 8 MiB owner
cache. These are not cold-device or independent-NVMe results. Native uses its
explicit Value-WAL barrier, not ordinary page-cache acknowledgment or a 5 µs
read. Valkey uses five RF1 primaries with AOF always and rewrite fsync enabled;
its local 2-CPU/1536-MiB container limits differ from host Tidehunter processes.
The peer rows are qualified controls, not a resource-equivalent production ranking.

All latencies below are exact nearest-rank **API milliseconds** from raw samples.
`async only` is the retained first implementation, before bounded node buffering.

| 1 KiB scalar PUT, 4096 measured | p50 | p95 | p99 | p99.9 | Mean | Stddev | Maximum | Calls/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Old distributed TCP | 2.135 | 6.166 | 130.161 | 198.780 | 5.187 | 19.879 | 198.793 | 3036 |
| Async only | 2.288 | 6.428 | 29.459 | 54.462 | 3.171 | 4.555 | 54.476 | 4812 |
| Final buffered distributed TCP | 2.204 | 4.583 | 8.079 | 9.845 | 2.525 | 1.255 | 9.872 | 6140 |
| Native, explicit WAL barrier | 11.025 | 16.366 | 19.116 | 21.777 | 11.310 | 2.947 | 24.916 | 1401 |
| Valkey TCP, AOF always | 2.662 | 4.046 | 5.021 | 6.585 | 2.648 | 0.781 | 7.208 | 5923 |

Scalar write calls at or above 80 ms fell from 80/4096 to 0/4096.
Median scalar latency did not improve; the improvement is in the tail and
completed throughput. This does not establish native-equivalent cloud performance.

| 8 KiB atomic PUT-batch32, 4096 measured | p50 | p95 | p99 | p99.9 | Mean | Stddev | Maximum | Calls/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Old distributed TCP, initial | 15.158 | 45.440 | 130.499 | 139.758 | 20.060 | 17.469 | 148.645 | 778 |
| Async only, regression retained | 15.634 | 82.471 | 197.731 | 299.801 | 27.588 | 33.958 | 310.510 | 565 |
| Old distributed TCP, matched recheck | 17.853 | 126.629 | 214.474 | 307.857 | 37.767 | 43.142 | 307.882 | 417 |
| Final buffered TCP, matched recheck | 15.301 | 37.476 | 57.795 | 70.644 | 17.637 | 8.912 | 72.450 | 883 |

The async-only regression exceeded the declared 20% material-effect threshold.
The old and final implementations were both rechecked with frozen sources and
unchanged settings. Old-run throughput varied substantially: do not select its
slower repeat to claim a robust speedup factor. Final large-write tails improved
against both old observations; at-or-above-80-ms calls were 0/4096, versus
80/4096 initially and 486/4096 in the old recheck.

| 8 KiB GET-many32, 4096 measured | p50 | p95 | p99 | p99.9 | Mean | Stddev | Maximum | Calls/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Old distributed TCP, initial | 3.414 | 4.232 | 4.850 | 8.165 | 3.438 | 0.536 | 9.425 | 4206 |
| Async only | 3.461 | 4.223 | 4.678 | 8.590 | 3.499 | 0.484 | 9.154 | 4155 |
| Final buffered TCP, initial | 3.475 | 4.376 | 6.940 | 10.237 | 3.548 | 0.741 | 11.682 | 4103 |
| Old distributed TCP, matched recheck | 3.416 | 4.196 | 4.595 | 5.194 | 3.446 | 0.444 | 5.684 | 4199 |
| Final buffered TCP, matched recheck | 3.461 | 4.240 | 4.647 | 5.174 | 3.471 | 0.460 | 5.596 | 4175 |

The first final read p99 exceeded the threshold despite a small throughput
change, so both implementations were rechecked. The recheck does not reproduce
that large tail difference. Keep both observations; this is not evidence that
read performance improved, nor a statistical-significance result.

Separate 1 KiB scalar diagnostic cases preserve the same counts but enable
sampled profiles. Their clocks and overhead are not pooled with clean cases:

| Diagnostic implementation | Sampled foreground history time, ms | Sum of measurement-boundary durable-prefix wait, ms | Prefix-blocked batches |
| --- | ---: | ---: | ---: |
| Old | 722.149 | 14358.356 | 2126 |
| Async only | 0.000 | 3823.388 | 2137 |
| Final buffered | 0.000 | 1800.862 | 2039 |

Zero here is an observed enabled profile field, not a missing measurement.
Prefix wait is a sum across concurrent batches, **not elapsed wall time**.
The final owner-lifetime snapshots record 15 started spills, 7705 captured
batches, zero failures and 1.335 s summed persistence across all owners and phases.
This proves that maintenance still occurs; it does not give a measurement-only
percentage. Prefix coupling remains measurable after removing foreground spills.

These are development results with one initial repetition and selective,
separately retained rechecks. No populations are pooled, no run-to-run error bars
or confidence claims are made. A 4096-sample p99.9 has only about four observations
above it and remains a weak tail estimate. Hardware EFA/RDMA and real Sui
checkpoint replay were not run in this task.

Verification: **815 library tests passed, 42 ignored**; **53 focused storage tests
passed**; strict all-target Clippy and formatting passed. The explicitly run
software-provider FI_READ regression also passed. The latter creates a
real registered read, injects a background root failure, drains its live grant
and rejects subsequent reads. It is software-provider correctness evidence,
not hardware RDMA latency evidence.

The remainder records the original 6 October component design.


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
