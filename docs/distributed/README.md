# Sharded Value WAL implementation

Started 6 October 2026 in a separate checkout named `tidehunter-sharded-wal`.

**Cloud/local correctness automation:**
[Shared test commands, Codex Cloud setup and EFA hardware limits](CLOUD_AND_CI_TESTING.md).
The same checks serve local and GitHub runners. Software-provider/direct-packet
checks do not qualify EFA hardware or performance; AWS campaigns remain separate
and explicitly authorized. Existing root agent guidance is retained, with new
scoped rules under `.github/` and `tidehunter-fabric/`.

**Legacy middleware retirement:**
The active implementation and checkpoint tooling are maintained in this fork
and the compatible Sui checkout. The former repositories are preserved at
`project:archive/middleware-legacy/` and
`project:archive/middleware-fabric-candidate/`; they are historical inputs, not
build or runtime dependencies. Checkpoint RPC/configuration/audit helpers now
live in `experiments/sui-checkpoint/checkpoint_support.py`. The retained portable
fixture is at `project:checkpoint-fixtures/sui-genesis-1775/fixture/`.
Software libfabric uses its owned cache rather than an archived `target/`.
Earlier dated sections retain their historical paths and measurements.

**One-sided resident read implementation, 23 October 2026:**
[Exact implementation, source mappings, safety rules and local evidence](ONE_SIDED_WAL_IMPLEMENTATION_2026-10-23.md).
Eligible registered-memory hits now use actual `FI_READ`; first reads and
capacity misses retain ordinary RPC. The registered pool is bounded and immutable,
not an adaptive replacement for all owner-cache or physical WAL reads.
Local native/RPC/RMA controls and certified Sui equality audits passed.
EFA hardware, independent-NVMe scaling and native-speed parity remain unproven;
the software-provider batch32 result did not improve every measured metric.

**Remote-read decision and plan, 23 October 2026:**
[Exact RAM/WAL read paths, EFA/ConnectX/UDP comparison and selective NVMe-oF gates](REMOTE_READ_PATH_PLAN_2026-10-23.md).
The archive audit found that the historical 8 KiB/batch32 comparison used a
ten-times-smaller dataset; it does not isolate a source regression. The new plan
maps all five owners, separates page-cache service from device I/O, and retains
the current architecture while qualifying thinner messages and bounded one-sided
reads. File-backed read-only NVMe-oF is a conditional cold-path experiment, not
an implemented feature or a replacement for every coordinator-cache miss.
No runtime code or AWS resource changes accompany this plan.

**Native-style distributed fast path, 22 October 2026:**
[`perf/native-wal-fastpath`: exact mechanisms, source references and local evidence](../../tidehunter-fabric/DESIGN.md#native-style-distributed-wal-path-22-october).
The distributed member format now uses a versioned CRC32/IEEE over identity,
version, descriptor and native mutation bytes. Whole-batch BLAKE2b and native
recovery CRC remain. This is an experimental format cutover, not a native `Db`
format change: batch v3, group v2, index v4, history/checkpoint v2,
registration/seal v5 and wire/ALPN v9. Old experimental data is rejected.
Bounded synchronous resident reads avoid SQ/CQ work; incomplete or unavailable
NOWAIT reads retain their owned prefix and fall back to asynchronous I/O.
Append and persistence owners overlap bounded work, while durable publication
still advances only through a complete contiguous prefix. Native-message output
retains buffers and leases through actual send completion.
These mechanisms do not implement one-sided RMA or NVMe-oF. The separate
[registered-read proposal](ONE_SIDED_WAL_READS_2026-10-21.md) remains a design.
No native Tidehunter lock was removed, and this cutover changes no Sui Rust API.

**Local cache and checkpoint cutover, 16 October 2026:**
[Exact source map, paper mechanisms, Sui changes and correctness boundaries](HOT_READS_AND_CHECKPOINT_ADMISSION_2026-10-16.md).
Sequence-summary demand residency is separate from producer metadata; distributed
scalar hits avoid extra ownership work, and checkpoint staging persistence runs
on the existing worker. No native engine mutex was removed. That cutover used
protocol v8. The dated sections below retain the limits and measurements of their
own implementation stages, not necessarily those of the current source.

**Local concurrency cutover, 13 October 2026:**
[Concurrent authority, grouped reads, transport progress and lock inventory](CONCURRENT_AUTHORITY_AND_TRANSPORT_2026-10-13.md).
Committed reads no longer borrow the write owner. Bounded concurrent Sui writes
reach independent shard WALs; bulk misses share per-shard RPCs. Adaptive TCP
waits on readiness, while fabric keeps completion progress live. No native
engine lock was removed. These local changes are not yet hardware measurements.

**Sui integration, 12 October 2026:**
`distributed::sui::Db` is the explicit native/sharded adapter used by typed-store.
Build Sui with `USE_TIDEHUNTER=1` and `sui-node/sharded-wal` (or
`sui-node/sharded-wal-fabric`); the feature requires `SUI_SHARDED_WAL_CONFIG`.
The JSON file contains `root` and `databases`, mapping each exact canonical path
relative to `root` to a `RemoteDatabaseConfig`. Missing namespaces fail closed.
The feature-disabled native control uses the same engine source and rejects a
remote configuration. `TH_SYNC_WRITES=true` uses the real native WAL persistence
barrier after each foreground write/batch, without changing native Db defaults.

The adapter publishes complete durable batches and captures exact iterator cuts.
Scans use bounded 256-reference pages. Bulk gets retain one cut across bounded
256-key windows, group misses by shard, and preserve caller order and per-key
errors. Hash keyspaces support full reduced-order enumeration, not application-key
bounds. Verified caches remain authoritative only through exact index references
and current health checks. No serving index/cache guard spans a network wait.

`SUI_SHARDED_WAL_SCHEMA_OUTPUT` exports canonical `shape.yaml`, `shape.json` and
`manifest.json` under each namespace; native export also requires
`SUI_SHARDED_WAL_ROOT`. Prometheus `tidehunter_adapter{namespace,stat}` exposes
API/key counts, submitted inline/actor read groups, coordinator cache and authenticated
shard-cache/WAL-submission counters. Transport can split groups at negotiated limits;
sampled `read_profile_wire_requests` records the corresponding wire requests.
These are not physical NVMe I/O counts.
Diagnostic network waits use detached control handles, not the write owner.

The existing bounded retained-history/no-GC restrictions still apply. Schemas
are immutable on remote reopen. Cell pruning emits one bounded atomic tombstone
batch; oversized ranges fail before publication. Relocation applies native
per-cell compactor and relocation-filter decisions through durable tombstones,
without claiming WAL space reclamation. Coordinator-only destruction, secondary
mode, RocksDB range operations and directory-copy checkpoints return errors:
none can be represented safely by deleting/copying only the coordinator files.
See `experiments/sui-checkpoint` for real checkpoint replay and full-state audits.
Adapter semantic regressions are in `remote_db/tests.rs` and detached-completion
health regressions in `remote_db/pending_read/tests.rs`.

**Completed native/TCP/EFA Sui matrix, 13 October:**
[Matched transport results, timing attribution and verified closure](CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#20-completed-nativetcpefa-real-checkpoint-matrix-13-october-2026).
All four paired real-checkpoint runs passed full-state audits on six
`g6.8xlarge` hosts in cluster placement. EFA hardware activity was verified on
every host. The unprofiled remote/native ratios were 1.799× for TCP and 1.658×
for EFA. Those runs exposed owner-lock contention and an empty bulk-read slow
path; the later local cutover addresses them. All six instances and owned
auxiliary resources were removed before analysis. Hardware differs from the earlier run.

**Earlier native/TCP Sui validation, 13 October:**
[Real checkpoint results and verified cloud closure](CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#19-completed-real-sui-replay-and-cloud-closure-13-october-2026).
Fresh native/remote binaries passed the same 1,776-checkpoint private-chain
replay and complete state audit locally and on five separate NVMe storage hosts.
Cloud native/TCP elapsed was 8.784/11.787 s (1.342×); local was
16.975/33.220 s (1.957×). Durable batch-commit means were competitive, but several
checkpoint metadata gets still had large gaps. This was TCP, not EFA or mainnet.
All seven launched instances, including a replaced controller, and all owned
auxiliary resources were removed before performance analysis.

**Implementation and correctness boundaries, 12–13 October:**
[Real Sui integration and transport fixes](CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#18-real-sui-integration-and-transport-fixes-1213-october-2026).
The adapter, bounded checkpoint read batching, response-revalidation timings and
source-bound checkpoint runner are implemented. Static RF1, bounded retained
history, no physical WAL GC and no failover remain explicit limits.

**Earlier synthetic hardware evidence, 12 October:**
[Cluster-placement stage timing](CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#17-completed-cluster-placement-stage-timing-12-october-2026).
That native/TCP/EFA campaign is closed. It separates authenticated transport
controls, cache hits, durable write stages and attribution limits. Its workload
differs from the real-Sui checkpoint result above.

**Earlier hardware evidence, 10 October:**
[Native/TCP/EFA comparison and completed recovery follow-up](CHECKPOINT_ASYNC_READS_2026-10-09.md#10-october-2026-hardware-comparison).
The performance matrix and separate recovery follow-up completed; saved cleanup
audits cover both fleets.

**Earlier implementation, 9 October:**
[Checkpoint adoption and bounded asynchronous reads](CHECKPOINT_ASYNC_READS_2026-10-09.md).
This records the earlier protocol-v2 implementation, finite demand-filled shard
caches and checkpointed bounded-tail reopen. Current transport protocol is v7;
the older document remains a dated implementation and measurement record.

**Previous read implementation, 8 October:**
[Coordinator-owned bounded read progress](../../experiments/aws-sharded-wal/README.md#implemented-8-october-bounded-read-progress).
Split-phase reads keep independent hits responsive while misses run. Bounded
authenticated lanes, grouped-wave admission and a shared shard I/O pool preserve
published references, verification and fail-stop behavior. Mapped and positional
member reads are selectable. All 66 local TCP/native comparison cases and 22
smaller software-fabric/native cases passed value/reopen checks. Overlap reduces
phase time, not request tails; mapped remains the default. Software-fabric memory
reached 8.25 GiB, so those runs are correctness evidence only. They used no cloud hardware.

**Hardware measurement results, 8 October:**
[Matched native concurrency and completed EFA campaign](../../experiments/aws-sharded-wal/README.md#completed-campaign-8-october-2026).
All 236 API cases and 590 node archives passed on six `g6.16xlarge` hosts.
Native window reads use up to 32 workers sharing one Db; scalar controls stay
unchanged. EFA improves on TCP, but uncached scalar and concurrent reads remain
slower than native. Cold grouped reads can win. The current tested configuration
regresses in scalar/grouped reads and startup against the previous source.
The report separates cache hit rates, read policies, resource cost and hardware
scope. All run-owned AWS resources are removed; the independent audit passed.

**Architecture assessment, 8 October:**
[Compute-local caching, EFA and NVMe-oF](CACHE_RDMA_NVME_OF_DESIGN_2026-10-08.md).
Research only: maps measured bottlenecks to cache, request-progress and remote-block
designs, with source sections, correctness requirements and per-metric feasibility.
It distinguishes EFA from TLS, compares frontend value caching with compute-host
page caching, and keeps NVMe-oF as a candidate. The read work above is a later,
separate implementation; the assessment remains a research snapshot.

**Previous transport implementation, 7 October:**
[Authenticated EFA transport and validation](EFA_TRANSPORT_2026-10-07.md).
TCP remains the default. The optional Rust/libfabric message path keeps the same
TLS, authority checks, complete WAL batches, value verification and durable
receipts. Local checks and all 30 local benchmark cases pass without swap.
[The completed EFA cluster campaign](EFA_CLUSTER_RESULTS_2026-10-07.md) passed all
68 API cases and 170 node archives, with positive hardware traffic in every EFA
node case and zero swap. Warm 8 KiB scalar p50 improved from 753.709 µs TCP to
55.655 µs EFA; paired native was 3.038 µs. Batched write time reached 1.24× native,
but individual operations, batched reads and reopen remain slower. Results are
retrieved; all six instances and all run-owned resources are fully closed.
Estimated compute/IP/root-storage upper bound: $13.30.

**Previous TCP implementation and cloud evidence, 7 October:**
[Read timing, verified cold I/O, and remaining targets](READ_PATH_AND_COLD_IO_2026-10-07.md).
Local validation passed 534 library tests, the final eight TCP protocol tests,
strict Clippy and 33 deployment tests. All 64 resumed local cases passed; another
build contaminated the final local performance comparison. The isolated AWS run
`th-f73786bd42d638db` rebuilt both sources and passed all **32 API cases and 80 node
archives**, with zero swap and verified physical reads on every cold shard.
At 8 KiB, distributed/native p50 is **188.033/2.707 µs warm scalar**,
**317.683/126.927 µs cold scalar**, and **1.556/3.135 ms cold 32-key reads**.
Cold batched throughput is **2.011× native**; warm load still takes
**212.346/139.200 ms (1.525×)**. The buffer changes alone show no broad gain.
Warm reads are dominated by remote exchange; writes also have substantial
preparation, canonical decode and persistence costs. Full measurements are linked
in the report. All six instances terminated automatically; all run-owned resources
were removed and other users stayed untouched. Runtime estimate is $0.532 before
requests/transfer, within the $5 reserve. EFA was not implemented in that earlier run.

**Previous implementation and test evidence, 7 October:**
[Preparation overlap and independent-NVMe measurements](PREPARATION_OVERLAP_2026-10-07.md).
Bounded chunk preparation services earlier admitted shard writes while preserving
the same offered window, complete batches and durability barrier. The exact source
archives rebuilt locally and in AWS. All **111 cloud cases and 285 node archives**
passed integrity, source/binary, command-option and evidence checks, with zero swap.
The larger 8 KiB/B32/W32 load takes **206.507 ms TCP / 139.679 ms native (1.478×)**.
The immediately preceding source in the same campaign takes 208.220 ms (1.511×), so
the newest change alone establishes no broad gain. Across twelve warm shapes the
median new/previous elapsed ratio is 0.9997×, and the 1 KiB/B1/W32 case is 11.8%
slower. Uncached warm scalar reads and reopen still have large gaps; all node cases
show zero physical disk-read bytes. This is not cold-storage scaling or production
parity. All six instances are terminated; their EBS volumes and all run-owned
IAM/network/S3 resources are removed. The laptop reboot did not interrupt the run.

**Previous source-guided implementation and local test evidence:**
[Source-guided preparation, persistence and scan pipelines](SOURCE_GUIDED_PIPELINES_2026-10-06.md).
One agent per complete TiDB/TiKV, DiskANN and DistributedANN paper reviewed the
sources; the document maps exact sections, pinned source functions, local changes
and correctness tests. Bounded parallel preparation, exact grouped receipts,
file-scoped namespace-sync reuse and bounded two-page scans are implemented.
The integrated library passes **532 tests (seven ignored)**, with separate
process/TLS checks passing. The **252-case** local matrix and **nine-case** anomaly
confirmation pass all full-value/reopen audits with zero swap and all benchmark
storage processes exited. At 8 KiB/batch32/window32, the new combined design cuts
load **93.538→84.096 ms (10.1%)** and scan **22.714→15.367 ms (32.3%)** versus the
previous pipeline binary, but still takes **2.10× / 2.18× native**, respectively.
Across all twelve shapes the median load ratio is **0.997×**, so a general write
speedup is not established. Scalar reads and recovery still lag; regressions,
outliers and the bounded confirmation are retained in the
[complete measurements](SOURCE_GUIDED_MEASUREMENTS_2026-10-06.md).
No AWS resources or RDMA implementation were introduced; native `Db`, Sui and the
old middleware behavior remain unchanged by this slice.

**Previous local pipeline/cache implementation and evidence:**
[Bounded write pipeline and immutable-reference read cache](PIPELINE_AND_READ_CACHE_2026-10-06.md).
That local slice overlaps preparation with per-node persistence, wakes on any
node completion, handles lone writes directly, preserves ordered publication and
adds a bounded verified-reference cache. Native Tidehunter, old middleware and Sui
code are unchanged in this slice.

Final exact-source correctness passes **520 feature-library tests (seven ignored)**
and the separate-process crash/recovery campaign. All **234 final local cases**
and raw distributions are independently verified, with zero swap and all 780 node
processes gone. At 8 KiB/B32/W32, pipeline-only load improves **21.0%** versus its
disabled same-binary control; pipeline-plus-cache improves **17.9%** versus previous
TCP but still takes **2.23× native**. Resident hits avoid network exchange, using
an extra 8 MiB cache versus the native reference's disabled application cache.
Mostly-miss reads and some small-write shapes still lag, and outliers remain.
No universal/native-parity claim, AWS launch or RDMA implementation accompanies
this slice. The linked report makes final results prominent and preserves earlier
iterations separately.

**Previous completed independent-NVMe baseline:**
[Local optimization and cluster validation](OPTIMIZATION_AND_CLUSTER_VALIDATION_2026-10-06.md).
The corrected AWS retry validates 106 cases and 270 node archives on five independent
NVMe storage machines plus a coordinator. Median warm durable-load phase gap to
native improves 2.88→2.50×; warm scalar-read p50 gap improves 151→89×. Reads were
served without physical device-read deltas, so this establishes warm-path
improvements rather than cold-NVMe scaling or parity. Both runs are fully cleaned
up, including compute, EBS and run-owned IAM/network/S3 resources; other users'
instance states are unchanged.
The [first TCP readiness record](LOCAL_TCP_READINESS_2026-10-06.md) preserves its implementation and evidence.
The [local serving record](IMPLEMENTATION_LOCAL_DB_2026-10-06.md) preserves the preceding in-process implementation.
The [structure/publication record](IMPLEMENTATION_STRUCTURES_2026-10-06.md),
the [M1/M2 record](IMPLEMENTATION_M1_M2_2026-10-06.md) and
[first implementation record](IMPLEMENTATION_2026-10-06.md) preserve earlier evidence.
The dated milestones and test counts below are retained history; the current
record linked above controls completion and performance claims.

**Next work:**
[11 October implementation plan](CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md).
The [6 October coordinator-hierarchy plan](NEXT_STEPS_2026-10-06.md) is retained history.

Run the local five-partition correctness example from the repository root:

```sh
cargo run --offline -p tidehunter --features experimental-distributed --example sharded_wal_local
```

It uses temporary directories on the shared SSD and removes its temporary data.
The earlier structure-validation example verifies **80 complete batches, 160 native pending
operations and 480 scalar reads**, including exact completion tickets, unknown-gap
handling, ordered native pending publication and replay from the sealed recovered
inventory. Peak RSS was **9,036 KiB**, with no swap activity. The
[structure record](IMPLEMENTATION_STRUCTURES_2026-10-06.md) binds those logs to source
and binary hashes. This exercises pending-stage integration; it is not a serving
native `Db`, Sui workload or speed comparison.

A separate fresh [benchmark driver](LOCAL_BACKEND_BENCHMARK_2026-10-06.md) exercises
the new serving backend and ordinary native `Db`, with explicit synchronous and
asynchronous acknowledgement labels. See the [local comparison](LOCAL_BENCHMARK_2026-10-06.md)
for the run status and measurement limits.

## Source and branch

- Branch: `feat/sharded-value-wal`.
- Base: `ce37b18`, verified against both remote `origin/main` and `upstream/main`.
- Origin: `git@github.com:Loris-EPFL/tidehunter-distributed.git`.
- Upstream: `git@github.com:MystenLabs/tidehunter.git`.
- The earlier local `refactor/distributed-nvme` commits are not included wholesale.
  Required mechanisms are reviewed individually against this fresh base.

The architecture decision and correctness inventory copied here are dated research
records. Their original relative source links refer to the parent project layout;
use native paths in **this checkout** for implementation. The decision's source
review used the older local branch; line numbers and claims must be rechecked.

## Current contract

The October 6 [architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md)
selects one index/publication authority per logical database and complete atomic
batches placed in independently appendable Value WAL partitions. Global management
assigns epochs, inventory and budgets; node-local workers execute storage work.
The [correctness inventory](native_correctness_and_storage_options_2026-10-06.md)
remains a required reference. Earlier independent-owner middleware stays a baseline.

The opt-in `experimental-distributed` feature identifies the new contract explicitly.
The ordinary `Db` remains the default. `distributed::local_db::LocalDatabase` is a
separate **bounded local RF1 serving backend**, identified as
`experimental-native-cell-sharded-value-wal-local-rf1`. It serves complete batches,
gets, exists, ordered scans and checkpoint reads over five local partition workers
in the benchmark. Values live only in the selected partition's native Value WAL.

Its native-routed typed cell index shares native pending transactions and ordered
overlay/search primitives. It is an explicit experimental cell container and image
format; legacy `LargeTable` and its persisted index are not fully genericized.
Index flush/unload serves mapped reference images, while startup currently replays
**all retained registered WAL histories**. Images do not yet accelerate recovery.

Scalar reference membership uses an immutable segmented directory without the
partition metadata mutex. Metadata admission reserves outstanding work before
logical allocation. Dense publication slots, exact retry hash lookup and a bounded
mutex-free completion handoff preserve uncertainty and publish complete batches
in logical order. Local inventory and epoch transitions exclude competing local
owners. `distributed::remote_db::RemoteDatabase` now serves the same bounded
contract over persistent mutual TLS/TCP to five separate storage-node processes,
with complete inventory, epoch fencing, exact retries and direct reference reads.
Independent pending batches dispatch across nodes; multi-get and scan pages scatter
reads concurrently. Complete batches stay on one partition. This controlled static
RF1 implementation has no distributed authority lease, automatic HA, replication
or physical WAL reclamation. Sui selection was added on 12 October as described
above. The default native directory-lock teardown race was also corrected.

The **6 October** full feature library run passed **499 tests**, with six ignored;
two focused read-chunk regression tests passed after subsequent review fixes. The
default native suite passes **383 tests**, with six ignored. Separate-process
correctness validates abrupt coordinator exit, complete recovery, missing-node
rejection/restart, partial fencing, exact retry after a durably lost response,
stale epochs and mutual TLS rejection. See the [TCP evidence record](LOCAL_TCP_READINESS_2026-10-06.md).

A fresh **96-case** release campaign plus **24 supplemental window-32 cases** compares native asynchronous and native
file-barrier modes, the local backend and five-node loopback TCP. All audits passed;
sampled aggregate process RSS peaked at **338,352 KiB** with zero system swap delta
and all sampled processes exited. Local synchronous write-window ratios are
**1.33–2.63×** native; TCP is **1.71–4.82×**. Warm scalar TCP reads remain far slower
than native in-process reads. These results establish no speed parity, independent
NVMe scaling or Sui performance. No AWS resources were launched. The
[benchmark report](LOCAL_TCP_BENCHMARK_2026-10-06.md) and
[readiness gates](LOCAL_TCP_READINESS_2026-10-06.md) link raw timings,
source/binary hashes and the earlier historical 72-case local campaigns.

Experimental registration/seal metadata is now **version 3**, with the explicit
reference-metadata budget. Complete batch codec **version 2** and group codec
**version 1** remain unchanged; ordinary native formats are unchanged. Earlier
experimental registration versions are refused without an in-place migration.
Complete groups still fit one native fragment, with explicit size/count and
retained-history limits. Those development limits must not silently narrow the
native `Db`/Sui API.

See the [native integration map](NATIVE_INTEGRATION_MAP_2026-10-06.md) for the
actual index/checkpoint/replay boundary, and the
[Sui branch inspection](SUI_VALIDATION_BASE_2026-10-06.md) for the preserved matched
validation base and upstream compatibility target.

### Shared logical WAL position

The user asked whether a global WAL counter could preserve speed. Yes: within one
logical database, `(epoch, sequence)` is shared across all storage partitions.
`distributed/sequence.rs` allocates it using a local atomic counter at the single
active database authority. Storage nodes allocate their own physical byte ranges.
Allocation adds no standalone sequencer RPC. Unrelated logical databases need not
share the counter. This primitive does not itself establish exclusive authority.

A globally comparable number is not a completed/durable prefix. A stalled batch
can leave a gap while later batches finish on other nodes. Recovery must fence,
enumerate all registered streams and resolve those gaps. Several independent writer
authorities would require a further protocol, such as sequencer reservation ranges;
we do not implement one remote atomic operation per mutation. A wall clock or unique
ID generator alone cannot prove committed ordering or complete recovery.

`distributed/publication.rs` tracks separate durable and published prefixes,
retains unknown outcomes and future durable batches behind gaps, and invokes an
ordered atomic-publication callback. `CompletionInbox` transfers exact outcomes
to this owner without a global completion-map mutex. The prepared publication
bridge now feeds the bounded typed-cell serving index, with ordered recovery from
all retained local or remotely registered WAL epochs. Legacy `LargeTable` adaptation,
snapshot-based replay acceleration and distributed lease/consensus authority
establishment remain unfinished. Counters and
handoff structures alone do not prove complete database publication.

### Data-structure decisions

Use the access pattern to select the structure. Exact retry identities use hash
maps; dense authority sequences use vectors; sparse per-partition versions retain
ordered trees; immutable physical metadata uses segmented append-only slots.
Keep native per-cell index levels and compact sorted representations while
adapting references. Roaring can be reconsidered for measured sparse set work;
ART/Bw-tree or other overlays need a measured index bottleneck and the same batch,
snapshot and retention rules. See the [comparison and implementation summary](IMPLEMENTATION_STRUCTURES_2026-10-06.md)
and the [new serving integration](IMPLEMENTATION_LOCAL_DB_2026-10-06.md).

## Research anchors for every implementation slice

- Tidehunter paper, §§3.1, 3.3–3.4, 4.4: complete-batch recovery, completed frontiers,
  values remain in the WAL, and conditional relocation. Local reference:
  `../../../papers/Tidehunter-2026-v2.pdf` from this directory.
- Native `tidehunter/src/{batch,db,wal_replay}.rs`, `wal/`, `index/pending_table.rs`:
  validate against actual code, not the paper alone.
- [CORFU](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf):
  fence/seal before resolving missing positions; timeout is not proof of a hole.
- [CorfuDB multi-object append](https://github.com/CorfuDB/CorfuDB/blob/master/runtime/src/main/java/org/corfudb/runtime/object/transactions/OptimisticTransactionalContext.java):
  one authority entry for several object mutations; no claim that its whole design
  can be copied without Tidehunter-specific recovery and relocation proofs.
- [DynamoDB](https://www.usenix.org/system/files/atc22-elhemali.pdf) and
  [TiKV's scheduler](https://github.com/tikv/tikv/blob/49e7a1179d676b1bc51447f6b0ff8973e45bc85f/src/storage/txn/scheduler.rs):
  coarse control plane, local bounded admission and execution.

Keep operation identity, current request authority, logical version and physical
address distinct. CRC validity alone does not authorize a mutation. Validate before
emitting complete recoverable bytes. Fail closed on incomplete inventory. Do not
silently retry an unknown result with a new identity, overwrite sealed holes, or
delete source values merely because index application completed.

## Local validation boundary

This laptop has one shared SSD and no assumed RDMA device. Correctness tests can
exercise multiple partitions and failures locally; they cannot establish independent
NVMe speedups, RDMA performance, cluster durability or Sui workload parity. No AWS
resources are required for this stage.
