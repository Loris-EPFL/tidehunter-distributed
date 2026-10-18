# Cluster RAM caches and read-path implementation plan

Date: 11 October 2026. Last hardware update: 12 October 2026.
**P0–P7 mechanisms implemented; local and hardware measurements validated.**
Target checkout: `tidehunter-sharded-wal`. Section 17 records the latest completed
cluster-placement campaign and full closure. Earlier sections retain their dated
implementation and measurement history. General native-performance parity is
not established.

## 1. Decision and document authority

**Use the cluster's additional RAM deliberately.** Keep a bounded, verified hot-value
cache at the coordinator, a bounded encoded-record cache on each storage node, and
Linux page caching for buffered WAL files. Increase useful resident working-set
capacity across machines without copying the entire database into every tier.

A coordinator hit removes the shard fetch. A shard-cache or shard-page-cache hit
still requires the remote request. Aggregate cluster RAM is not one local-memory
pool available to the coordinator. Placement skew, duplicate copies, metadata,
and caller-held buffers reduce the unique dataset coverage of summed cache sizes.

Keep the current authoritative index and independent physical shard Value WALs.
Caching must not become a second durability protocol, change batch-ID placement,
weaken publication, or hide failures. Fix the EFA submission bottleneck and other
miss-path costs even when a large cache makes hot workloads look fast.

The evidence order is:

1. [12 October cluster-placement stage timing](#17-completed-cluster-placement-stage-timing-12-october-2026):
   current native/TCP/EFA measurements, detailed timing scopes, and complete closure.
2. [12 October spread-placement campaign](#15-completed-separate-nvme-tcpefa-campaign-12-october-2026):
   the preserved source-matched RAM controls for the latest observation.
3. [11 October hardware validation](#11-authorized-hardware-validation-11-october-2026),
   with separate source-bound core, capacity and recovery archives.
4. [10 October hardware results](CHECKPOINT_ASYNC_READS_2026-10-09.md#10-october-2026-hardware-comparison),
   [core comparison CSV](../../logs/checkpoint-async-aws-2026-10-10/run-final/analysis/comparison.csv),
   and [completed recovery comparison](../../logs/checkpoint-async-aws-2026-10-10/run-recovery/analysis/comparison.csv).
5. The 9 October implementation in that report, rechecked against the current
   source on 11 October. It already has multiplexed reads and both explicit caches.
6. This plan controls implementation and acceptance gates. Sections 10–16 retain
   their implementation and experiment records; section 2 retains October 10 data.
7. [8 October cache/RDMA/NVMe-oF assessment](CACHE_RDMA_NVME_OF_DESIGN_2026-10-08.md)
   remains research history. Its serial-per-connection source descriptions predate
   protocol v2. [6 October source-guided work](SOURCE_GUIDED_PIPELINES_2026-10-06.md)
   remains the record of implemented preparation, persistence and scan work.

The **10 October hardware campaign's** current engine source hash was
`84bf88b81410cbc82cfd59eb987b8012a887d9288f3b76e250b9678587efa7d0`.
Its paired baseline was the 8 October CURRENT engine, hash
`97f18475bb15b8ad7ae20ac799fd2a39d5399ccdbc7d3ce8c88d5950730ec5b1`.
The [completion record](../../logs/checkpoint-async-aws-2026-10-10/campaign-completion-summary.json)
selects 114 qualification/core/pressure identities from the first fleet and 40
recovery identities from the follow-up. The original full-fleet receipt remains
partial; the follow-up independently passed 44 records, including qualification.
Do not pool its five recovery repetitions with the first fleet's two early cases.

## 2. Measured gaps that this plan must address

Five separate shard hosts each had local NVMe; a sixth host ran the coordinator
and native control. There were five paired repetitions per exact configuration,
RF1, no replicas and no full Sui workload. Core application caches were disabled;
separate tiny-cache pressure cases tested finite residency and eviction. Initial
value datasets were about 10/80 MiB, not larger-than-RAM production datasets.

Native TCP/EFA columns below are separate matched native controls. Throughput is
in thousands of logical operations per second. These are medians, not guarantees.

| Metric / value size | Native TCP-paired | Distributed TCP | Native EFA-paired | Distributed EFA |
| --- | ---: | ---: | ---: | ---: |
| Durable load, 1 KiB, k mutations/s | 192.425 | 154.194 | 197.563 | 156.469 |
| Durable load, 8 KiB, k mutations/s | 44.766 | 42.859 | 45.292 | 43.146 |
| Worst sampled durable window, 1 KiB, ms | 5.764 | 10.504 | 5.600 | 9.192 |
| Worst sampled durable window, 8 KiB, ms | 23.938 | 36.460 | 24.098 | 35.833 |
| Warm scalar p50, 1 KiB, microseconds | 0.710 | 273.425 | 0.740 | 219.365 |
| Warm scalar p50, 8 KiB, microseconds | 3.110 | 311.916 | 2.960 | 251.925 |
| Warm window32, 1 KiB, k reads/s | 235.083 | 64.580 | 232.636 | 14.312 |
| Warm window32, 8 KiB, k reads/s | 200.063 | 39.401 | 197.546 | 13.989 |
| Evicted-pass scalar p50, 8 KiB, microseconds | 98.212 | 431.009 | 105.232 | 371.718 |
| Evicted-pass get_many p50, 8 KiB, ms | 2.107 | 1.027 | 2.299 | 0.940 |
| Evicted-pass window32, 8 KiB, k reads/s | 143.327 | 38.300 | 141.911 | 14.012 |
| Full scan, 1 KiB, ms | 13.262 | 43.264 | 13.497 | 40.584 |
| Full scan, 8 KiB, ms | 782.629 | 120.964 | 842.044 | 115.693 |
| Index-maintenance phase, 8 KiB, ms | 14.943 | 18.550 | 14.664 | 17.913 |
| Engine reopen, 8 KiB, ms | 2.581 | 68.780 | 2.496 | 379.120 |
| Timed shutdown, 8 KiB, ms | 1.899 | 18.462 | 1.791 | 95.224 |

### 2.1 Interpretation and measurement limits

- The native warm microsecond read is RAM-backed, not a fresh SSD fetch. Buffered
  `io_uring` also uses page caching; a WAL SQE is not evidence of physical I/O.
  Evicted passes verify physical reads but can warm while they run.
- Load uses 32 mutations/batch and 32 batches/window. There are ten window samples
  per run: the saved p99 is the maximum. The table reports the median of these
  maxima, not a production p99 or individual durable-commit percentile. B1/W1
  serial-write latency was not measured in this campaign.
- The 8 KiB write-window maxima worsened against the distributed baseline in all
  five pairs: about 38% TCP and 27% EFA by ratio of medians. Near-native aggregate
  throughput does not excuse this tail regression. The 1 KiB load still loses
  about one fifth of native throughput.
- EFA warm-window throughput fell about 68–69% against the distributed baseline,
  although its scalar and grouped reads improved. TCP warm-window throughput
  improved. The comparison also changed mapped/4 connections/1 control worker to
  Async/1 connection/2 control workers: it does not isolate a single cause.
- Both 8 KiB distributed scans became about 33% slower than their distributed
  baseline in every pair, despite beating native's actual serial iterator. The
  1 KiB scan is still about three times native. No independent cold-cache gate
  immediately precedes the scan. Do not average the opposing size-specific ratios.
- The mixed phase performs scalar reads, then a write window; it is not concurrent
  mixed traffic. Read/write summaries share a wall timer and have only two write
  windows. Current EFA 8 KiB mixed shared-wall rate fell about 9.5% in every pair
  while its write-window latency improved. Its scalar reads, not that shared
  throughput label, are the relevant remaining problem.
- Index maintenance and shutdown have different native/distributed obligations.
  Compare their actual service costs, but do not call them identical primitives.
  Single-sample scan/maintenance/lifecycle p99 fields are not tail distributions.
- Recovery's five-process restart passed exact values, tombstones, scans and retry
  identities. It is not a host reboot or cold OS cache. EFA zero-tail engine open
  improved to about 372–383 ms; tail128 open is about 538–559 ms. There is no matched
  native crash-recovery series. Controller restoration also includes orchestration
  and verification; do not compare it to native engine reopen.

### 2.2 Newly identified write-timing issue

`RemoteDatabase::admit_prepared` starts its `validation_ns` timer before checking
`checkpoint_admission_requires_flush`. That branch calls `drain` and
`commit_checkpoint` synchronously. `submit_many` timing can also include progress
and waits. **The existing validation counter is not pure validation/hash CPU.**

The 8 KiB load writes about 80 MiB of values and crosses the 64 MiB encoded-tail
limit; the 1 KiB load does not. A foreground automatic checkpoint is therefore a
concrete candidate for the larger write-window maximum. Its contribution has not
been separately timed in the saved data. Do not claim the entire regression is
proved to come from checkpointing or "fix" it by increasing the tail bound.

## 3. Target RAM hierarchy and memory policy

| Tier | Owner / content | Benefit | Required bound |
| --- | --- | --- | --- |
| Coordinator hot-value cache | One authority; verified immutable key/value members | Removes shard RPC for hits after authoritative index resolution | Coordinator cache budget, plus separate index and caller-result accounting |
| Shard hot-record cache | One node shared across all its connections/partitions; encoded members | Bypasses kernel read issue/completion for hits | One node-wide cache budget, not one budget per connection |
| Linux page cache | Each host; buffered WAL/index file pages | Avoids physical device reads for resident pages | Leave host headroom; include it in memory/resource evidence |
| Local NVMe | Owning shard's Value WAL | Capacity and durable record source | Existing complete-batch, persistence and recovery rules |

A coordinator cache is local only to the coordinator. An external Sui process on
another host still pays its coordinator RPC. The engine benchmark does not include
that hop. Keep the integration near the application where practical; do not add a
mutable client cache or a second authority to claim local-hit latency.

### 3.1 Exploit capacity, not just tiny correctness-test caches

Use existing per-tier byte controls to sweep capacities. First measure below the
current 256 MiB ceilings; then support multi-GiB budgets where the host and workload
justify them. Do not set a large default before measuring working-set reuse.

For each host require an explicit finite RAM envelope:

`cache-owned memory + index/reference metadata + pending/reply/provider ownership
+ application-held results + allocator/kernel allowance + page-cache/headroom reserve
<= host memory allowance`.

Cache byte gauges account requested owned allocations, not all RSS. OS pages and
shared mappings must not be blindly double-counted as anonymous heap, but duplicated
application and page-cache copies must not be advertised as unique cached data.
Use process/cgroup/host evidence to reconcile the different scopes.

- The coordinator retains the most useful hot values. Its capacity does not grow
  merely because another storage node has free RAM.
- Shards retain their own residual working sets after coordinator hits are removed.
  More shard hosts can increase resident dataset coverage without a central copy.
  Do not assume batch placement produces uniform access or equal cache hit rates.
- Reserve room for recovery, index work, dirty pages, transport pools, TLS and
  in-flight replies. Admission failure must bypass caching, not fail a valid read.
- Keep read throughput and latency under memory pressure observable. Do not accept
  swapping/OOM as an invisible way to satisfy a cache-budget configuration.
- One-pass scans must not fill the point-read hot tier. Keep demand-driven fills;
  no benchmark-oracle prefill or mandatory full-dataset warmup.
- Start with existing CLOCK admission. Add a bounded second-touch filter only if
  scan/uniform-read measurements justify its overhead. Do not add a new cache crate
  or cooperative cross-shard victim cache speculatively.

[INFERENCE] Coordinator-local hits can plausibly approach native warm-read latency;
remote resident hits need an efficient transport path; true cold reads still need
storage service. No universal 5-microsecond promise follows from larger caches.
For an illustrative 3-microsecond hit and 250-microsecond miss, a 5-microsecond mean
requires about 99.2% hits. This is arithmetic, not a measured result. Uniform reads
well beyond resident capacity cannot acquire locality from a replacement policy.

## 4. Codebase map and repository ownership

Paths below are relative to this fork; links resolve from this document.

| Location / symbols | Existing responsibility | Planned work |
| --- | --- | --- |
| [remote_db.rs](../../tidehunter/src/distributed/remote_db.rs): `RemoteDatabaseConfig`, `fetch`, `admit_prepared`, `commit_checkpoint`, `open_inner`, `shutdown` | Authority, cache config, ordered writes, checkpoint and lifecycle | Cache envelope; timing separation; cheaper bounded checkpoint/lifecycle work |
| [pending_read.rs](../../tidehunter/src/distributed/remote_db/pending_read.rs): `submit_get_cut_profiled`, `complete_get`, `ReadService::open/stop` | Published-reference resolution, immediate hit, bounded misses and one read owner | Preserve zero key/value-copy hit; per-open connection setup; exact tier timings |
| [read_cache.rs](../../tidehunter/src/distributed/remote_db/read_cache.rs): `ReadCache`, `ReadCacheStats` | Four-way CLOCK, verified references, exact retained allocation charges | Larger safe budgets, bounded eviction effort, honest retained/pinned statistics |
| [tcp.rs](../../tidehunter/src/distributed/tcp.rs): `NodeConfig`, `Request`, `Client::connect`, `TlsFiles::client_config` | Node settings, protocol, handshake/status, append handler | Budget validation, scan-admission hint if added, shared per-open TLS material, service timing |
| [reactor.rs](../../tidehunter/src/distributed/tcp/reactor.rs): `Session::progress`, `DeferredRead` | Framing, TLS, submission and response-credit retirement | Separate frame submission from completion retirement; propagate cache-use class |
| [read_executor.rs](../../tidehunter/src/distributed/tcp/read_executor.rs): `ReadAdmission`, `ReadExecutor::submit/poll_completed` | Node-wide limits/cache; async file ownership and result leases | Resident fast path, bounded cache admission; isolate cache work from credit accounting |
| [shard cache.rs](../../tidehunter/src/distributed/tcp/read_executor/cache.rs): `Cache` | Encoded immutable-member cache shared across sessions | Bounded eviction, no-fill scans; selective lock partitioning only with evidence |
| [fabric.rs](../../tidehunter/src/distributed/tcp/fabric.rs) and [endpoint.rs](../../tidehunter-fabric/src/endpoint.rs) | Authenticated bootstrap and bounded registered TX/RX slots | Keep existing provider/TCP fallback choice; add completion-fence interface |
| [endpoint/resources.rs](../../tidehunter-fabric/src/endpoint/resources.rs) | Provider selection, manual progress, CQ and slot ownership | Track submitted/completed send fences without freeing DMA-visible buffers |
| [read_pipeline.rs](../../tidehunter/src/distributed/remote_db/read_pipeline.rs) | Exact, captured-cut, two-page scan; no coordinator cache fill | Reuse a scan cursor; carry scan no-fill hint to shards |
| [native_index.rs](../../tidehunter/src/distributed/native_index.rs): `scan`, `encode_image`, `flush_and_unload`, `CellIndex::all_versions` | Reference index, merged iteration, checkpoint images | Avoid rebuilding scan heap each page and repeated full image/history expansion |
| [preparation.rs](../../tidehunter/src/distributed/remote_db/preparation.rs), [codec.rs](../../tidehunter/src/distributed/codec.rs), [native_publication.rs](../../tidehunter/src/distributed/native_publication.rs) | Canonical preparation, exact receipts, whole-batch index publication | Reuse validated local metadata; retain untrusted-byte verification and transaction rules |
| [storage.rs](../../tidehunter/src/distributed/storage.rs), [group.rs](../../tidehunter/src/distributed/group.rs), [inventory.rs](../../tidehunter/src/distributed/tcp/inventory.rs) | Complete Value-WAL groups, file/namespace durability, epoch fencing and proofs | Instrument service stages; preserve sole-pending-group and fence rules |
| [native decompress_cache.rs](../../tidehunter/src/decompress_cache.rs), [config.rs](../../tidehunter/src/config.rs), [db.rs](../../tidehunter/src/db.rs), [wal/mod.rs](../../tidehunter/src/wal/mod.rs) | Inherited native baseline and correctness reference | No behavior changes planned; use existing controls in benchmark adapters |
| [sharded_wal_bench.rs](../../tidehunter/examples/sharded_wal_bench.rs), [read_probe.rs](../../tidehunter/src/read_probe.rs), [recovery example](../../tidehunter/examples/sharded_wal_recovery_bench.rs) | Workloads, timing, value audits, physical-read and recovery evidence | Cache-capacity/tier cases, real percentile sample counts, checkpoint spans |
| [local-read-progress.py](../../experiments/local-read-progress.py) | Local bounded TCP/software-fabric cases | Local capacity/progress controls without claiming independent-NVMe performance |
| [checkpoint_async.py](../../experiments/aws-sharded-wal/checkpoint_async.py), [guest.py](../../experiments/aws-sharded-wal/guest.py), [analyze_cloud.py](../../experiments/aws-sharded-wal/analyze_cloud.py) | Campaign identities, execution and independent evidence validation | A distinctly named new cache/progress campaign; preserve old matrices and archives |
| [freeze_build.py](../../experiments/aws-sharded-wal/freeze_build.py), [prepare_bundle.py](../../experiments/aws-sharded-wal/prepare_bundle.py), [control.py](../../experiments/aws-sharded-wal/control.py) | Immutable source/binary/package provenance and owned-resource lifecycle | Reuse, not replace; any later authorized run must identify the new exact build |
| [cluster_ram.py](../../experiments/aws-sharded-wal/cluster_ram.py) | New `cluster-ram` identities, controls and evidence validation | Separate October 10/11 comparison; historical profiles unchanged |
| [local-cluster-ram.py](../../experiments/local-cluster-ram.py), [local-cluster-recovery.py](../../experiments/local-cluster-recovery.py) | Five-process comparison and abrupt-exit/restart campaigns | Frozen binary/source checks, deadlines, exact audits and owned-process cleanup |
| [local_compare.py](../../experiments/aws-sharded-wal/local_compare.py) | Resource collection outside API timers | Process-tree RSS, host swap, host/cgroup context with explicit shared scope |

**Repository impact:** the implementation belongs mainly in this fork's distributed
modules and `tidehunter-fabric`, with benchmark changes in the same fork. No change
to Mysten's inherited native algorithms, old middleware, or Sui is required for these
steps. A later Sui integration may need adapter/configuration changes for colocation
and workload evidence, not a Sui consensus/object-semantics rewrite. Any unavoidable
native helper change must be separately justified, feature-scoped and tested against
the unchanged native control. This is not a new global physical WAL or full-RAM index.

## 5. Correctness rules that every step must preserve

1. Resolve the latest/captured index cut before cache lookup. Key by the complete
   `ValueRecordReference`, never only user key, shard offset, or a TTL.
2. Coordinator fills follow successful authenticated membership/integrity checks.
   Shard hits still validate membership and are verified by the consuming client.
3. Cache contents are derivative. They cannot acknowledge durability, expose an
   uncommitted batch, resurrect a delete, advance publication, or establish a
   shard's present health. Observed fencing/integrity/transport failure remains fatal
   to that authority even if a usable cached copy exists.
4. Preserve generation changes on cache clear/reopen. Old in-flight fills cannot
   repopulate a cleared generation. Before adding GC, gate hits on reachability and
   retain segment/export lifetimes through readers. Current retained-history behavior
   is not an implemented reclamation protocol.
5. Keep cache, request, response, provider, kernel and caller ownership distinct.
   Bound each; do not release credits or buffers still reachable by a worker, TLS,
   the NIC or an io_uring operation. Cancellation suppresses delivery, not ownership.
6. Keep exact record lengths, immutable bytes, complete groups, file/namespace
   synchronization, exact retry identities and contiguous publication. Network/RDMA
   completion is not a durable receipt. Cache tuning does not change write policy.
7. Keep mTLS, peer/epoch binding, integrity verification and fail-closed recovery.
   An advisory admission hint cannot alter authorization or which value is returned.
8. Keep the 64 MiB/1,024-batch admitted recovery-tail bounds. Persist checkpoint
   components before manifest adoption; preserve interrupted staging/retirement
   recovery. All required shards must be fenced before serving.
9. Preserve native behavior and the existing batch-ID placement rule. A scalar miss
   follows one published owner; it must not broadcast across shards to find a cache hit.

## 6. Ordered implementation work

The following steps remain the design and acceptance contract. Their mechanisms
and local correctness gates are implemented; hardware performance gates remain
open. A cache-hit speedup cannot substitute for miss-path and durability evidence.

### P0 — Correct attribution and freeze the next comparison

**Files:** benchmark/probe/analyzer, `remote_db.rs`, `preparation.rs`, recovery example.

- Keep old archives immutable. Correct presentation to use transport plus backend;
  a saved `backend=tcp` field can still describe EFA. Keep exact size and shape.
- Add bounded per-window checkpoint count/span and non-overlapping parent spans
  for preparation/admission, checkpoint, storage exchange/wait and publication.
  Keep nested worker elapsed sums separately labelled; CPU requires CPU evidence.
- Record frame-gate stalls, output occupancy, CQ progress gaps and per-tier cache
  hits/misses, retained bytes, evictions/bypasses and request/reply retention. Sample
  expensive process/device data outside latency timers.
- Separate open's connect/status/TLS/fabric/fence/proof/tail intervals and shutdown's
  drain/checkpoint/seal/join intervals. Service-readiness polling is not WAL replay.

**Acceptance:** meaningful sample counts and paired comparisons; no additive chart
of overlapping clocks, no single-sample p99, no mixed shared-wall rate described as
standalone throughput. No fabricated new spans for the old results.

### P1 — Pipeline EFA output without weakening ownership

**Files:** `tcp/reactor.rs::Session::progress`, fabric endpoint/resources and existing
[reactor tests](../../tidehunter/src/distributed/tcp/reactor_tests.rs) /
[nonblocking fabric tests](../../tidehunter-fabric/src/endpoint/resources/nonblocking_tests.rs).

The pre-change source fed only `output.front_mut()` into TLS and popped it only after
TLS was empty and `endpoint.sends_pending()` was false. TCP lacked that provider
completion gate. This could serialize frames on one EFA connection; its fraction of
the historical hardware throughput loss is not yet isolated. The new source separates
submission from retirement as specified below.

- Separate the next frame eligible for submission from frames awaiting retirement.
- After a frame's plaintext is consumed and its ciphertext has been accepted by
  transport buffers, allow the next frame to proceed while retaining its credits.
- Track a send fence/ordinal covering that frame's accepted ciphertext. Handle
  out-of-order local CQEs with a contiguous completed-send frontier; retire a
  frame's ownership only when its fence is satisfied. Track failure/close explicitly.
- Keep TLS record order, bounded TX slots, output byte/count limits and TCP behavior.
  Local send fences prove buffer retirement only, never persistence or client receipt.
- Hold connection count, read policy and cache budgets fixed when isolating the
  change. Evaluate normal `efa` manual progress; do not silently select `efa-direct`.

**Acceptance:** another admissible frame progresses before an earlier local send
retires; out-of-order CQEs, stalled peers, partial TLS writes, cancellation and
shutdown never free live memory or refund live credits. Existing authenticated
request correlation/error tests pass. Local software fabric proves behavior only;
later hardware must recover EFA concurrency without degrading scalar/tail latency.

### P2 — Support useful coordinator and shard cache capacities safely

**Files:** both cache implementations/config owners; benchmark CLI and runner bounds.

- Retain byte-based finite budgets and `0=disabled`. Extend the old 256 MiB cache
  ceilings consistently across coordinator config, node validation, benchmark CLI,
  external YAML preparation and the native benchmark cache control. Do not change
  native engine defaults. Validate cache plus in-flight read budgets against an
  explicit per-process read-memory allowance with checked arithmetic. This slice is
  not a total-host cap: reserve index, provider, application and OS memory separately.
- Preserve fixed/predictable metadata allocation and cheap CLOCK hits. Report its
  startup memory cost before allocating a large table; charge metadata and payload.
  Admission on a miss must not scan a multi-GiB cache unboundedly: cap eviction
  effort per insertion and bypass caching when it cannot fit within that effort.
- Preserve the coordinator's single-owner, no-extra-cache-lock fast path and exact
  detached payload ownership. Do not retain an entire network batch to cache one value.
- The pre-change shard cache shared `ReadAdmission.state`'s mutex with credits/stats.
  Separate long eviction/allocation work from admission accounting before scaling
  it. Use bounded cache partitions only if contention is measured, with a total node
  budget and explicit skew behavior; never multiply capacity by sessions.
- Report caller/response-held clones separately. Eviction removes cache ownership,
  not necessarily the allocation's last owner. Budgeted admission failure is not an
  I/O error. Preserve clear-generation checks after any lock restructuring.

**Acceptance:** larger budgets are genuinely accepted through every config path;
zero, oversize, arithmetic overflow and constrained-host cases behave explicitly.
Pressure, retained callers, concurrent hits/clear, cancel/reopen and skew cannot
break bounds, version selection or failure propagation. Show hit-rate versus RAM
curves; do not accept a change solely because a toy dataset now fits entirely.

### P3 — Make shard residency useful and protect it from scans

**Files:** `read_executor.rs`, shard `cache.rs`, `read_pipeline.rs`, `Request` and
reactor read dispatch. Use [existing executor tests](../../tidehunter/src/distributed/tcp/read_executor/tests.rs).

- Keep immediate explicit-cache hits separate from file reads. Buffered cache misses
  still use Linux page cache; do not equate them to device misses or switch to direct
  I/O just to simplify counters. Retain bounded asynchronous cold I/O ownership.
- Measure forced `IOSQE_ASYNC` cost on page-resident misses before changing policy.
  Do not perform potentially faulting mapped/positional reads on the network progress
  owner merely to improve the warm case.
- Preserve coordinator scan no-fill, already implemented. Add an advisory
  point/scan admission class to the shard request path so scans can consult existing
  entries without filling the hot tier. This changes wire shape: update both peers,
  protocol/ALPN identity, fixtures and packaged binaries together; reject mixed
  versions rather than silently ignoring the field. No placement/version semantics change.
- Start with scan bypass and existing CLOCK. Duplicate-miss coalescing or second-touch
  admission is conditional on measured duplicate/read-reuse evidence. If added, its
  pending table and waiters must be bounded; never block the transport owner waiting
  for another reader's fill.

**Acceptance:** hot point reads remain resident during a scan; the scan returns the
same ordered values/cut; admission hints cannot bypass validation; miss paths still
issue real file/device work when appropriate. Separate coordinator-hit, shard-hit,
buffered-read and verified physical-I/O evidence without inventing per-request
physical-device attribution from aggregate counters.

### P4 — Reduce automatic-checkpoint write stalls and repeated image work

**Files:** `remote_db.rs::{admit_prepared,commit_checkpoint,checkpoint_file_digest}`,
`native_index.rs::{encode_image,flush_and_unload,CellIndex::all_versions}` and
[existing remote checkpoint tests](../../tidehunter/src/distributed/remote_db/tests.rs).

- First expose the foreground checkpoint span from P0. Keep admission blocked when
  admitting another batch would exceed the exact recovery-tail limit.
- Remove repeated expansion/encoding of immutable index history where the same
  captured cut and validation prove reuse safe. Compute the image digest during
  its existing encoding/write pass instead of rereading solely for hashing.
- Collect independent shard proofs with bounded fan-out, then validate the complete
  set before adoption. Stream/reuse bounded encoding buffers; do not build another
  whole-database value image or introduce an unbounded background queue.
- Background preparation is a later substep only after defining an immutable
  captured index cut and retained ownership. This plan does not assume the current
  mutable index can be snapshotted concurrently without additional work.

**Acceptance:** unchanged checkpoint is a no-op; every interruption point retains
recoverability; old snapshot reads and exact retries survive; all image/metadata
persistence precedes manifest adoption; tail bounds remain unchanged. Later paired
runs must reduce worst-window/checkpoint stalls without sacrificing durable load
throughput. Do not make the spike disappear by moving it outside the reported timer.

### P5 — Close small-write overhead and preserve exact publication

**Files:** `preparation.rs`, `codec.rs`, `remote_db.rs`, `native_publication.rs`,
`native_index.rs`, `tcp.rs` append handler, `storage.rs` and existing
[pipeline regressions](../../tidehunter/src/distributed/remote_db/pipeline_tests.rs).

- Carry already validated local size/routing/reservation metadata through prepared
  admission/publication rather than walking descriptors and allocating vectors again.
  Reuse bounded scratch where ownership allows. Preserve whole-window preflight.
- Keep mandatory decode/integrity checks at untrusted transport/storage boundaries.
  A locally prepared object is not permission to trust incoming bytes or a receipt.
- Instrument node inventory-lock residence and decode/group/append/persist separately.
  Narrow append ownership only with an explicit fence-versus-admitted-write invariant;
  no speculative lock removal based on a combined storage-wait counter.
- Reuse existing grouped persistence and file-scoped namespace-sync proof. Do not
  add a second WAL, weaken fsync, or permit multiple unpersisted groups without a
  separately justified correctness change.

**Acceptance:** late invalid input/conflict rejects before live logical allocation;
equal retries retain identity/version; whole batches become visible together and
only through the contiguous durable prefix; lost outcomes remain fail-closed. Later
1 KiB measurements must improve the current throughput deficit without an 8 KiB
throughput/tail regression. A lower `validation_ns` alone is not acceptance.

### P6 — Remove independent startup/shutdown serialization

**Files:** `RemoteDatabase::open_inner/load_checkpoint/replay_history/shutdown`,
`ReadService::open/stop`, `Client::connect`, `TlsFiles::client_config`, inventory fencing.

- Build/share TLS configuration and immutable shape once per open attempt, not
  indefinitely across opens or credential changes. Reuse the authenticated status
  already obtained by `connect` instead of an extra status round trip.
- Use bounded endpoint-parallel connection/status and read-lane creation. Durably
  reserve the new inactive epoch before fencing; overlap independent shard fences,
  but serve only after every required shard succeeds and recovery is validated.
- Overlap independent checkpoint-proof and replay-page fetches while keeping exact
  proof checks, bounded tail pages and the single ordered logical replay merge.
- On shutdown, issue independent seals before waiting for all results. Drain/join
  every owned worker and kernel/provider operation before releasing the authority
  lock, including partial construction and failure paths.

**Acceptance:** no usable DB escapes partial fencing, setup, proof or replay failure;
retry reserves a newer epoch where required; credentials/ALPN/server-name checks
remain intact; no worker survives lock release. Measure actual whole-open/close
reductions and their component spans, not excluded work or orchestration polling.

### P7 — Recover small-value scan efficiency without losing large-value gains

**Files:** `NativeIndex::scan`, `read_pipeline.rs::{next_scan_page,scan_prefetched}`
and [existing scan tests](../../tidehunter/src/distributed/remote_db/read_pipeline/tests.rs).

- Retain a scan-scoped cell/merge cursor instead of reconstructing cell heads and
  the merge heap for every 256-record page. Preserve the captured published cut.
- Keep the current two-page/4 MiB-page bounds and per-shard credit admission as the
  starting point. Fix repeated orchestration before increasing lookahead.
- Keep exact FIFO emission and value verification; no ANN pruning/approximation,
  missing-result tolerance or point-cache pollution. Return-container memory remains
  caller-owned and must be reported separately from bounded prefetch state.

**Acceptance:** exact ordered keys/values/tombstones across mapped bases and overlays,
oversized records, updates/deletes, checkpoints and fences. Report 1/8 KiB separately;
close the small-value deficit and recover the distributed large-value regression
without claiming a universal scan speedup from its native-relative advantage.

### Dependency and execution order

P0 comes first. P1 and P2 can be implemented independently after agreeing on resource
accounting; one integration owner owns shared reactor/cache interfaces. P3 follows
P2 and is evaluated with P1 held fixed. P4 and P5 share authority/admission code and
must not be edited concurrently without an explicit owner. P6 follows P1's endpoint
lifetime contract. P7 follows stabilized read progress and the scan admission class.
Each step requires local correctness and targeted runtime evidence before its result
is called implemented. Hardware claims wait for a separately approved campaign.

## 7. Source-to-problem-to-implementation map

QMD retrieval was attempted on 11 October but timed out. This plan therefore uses
local paper text and primary upstream documentation/source, not an unreturned QMD
answer. Living documentation was accessed on 11 October; pinned code identifies
its revision. Existing October 6 reviews remain attributed as earlier research.

| Source / exact part | Mechanism and local problem | Plan mapping / limit |
| --- | --- | --- |
| [Tidehunter v2](https://arxiv.org/abs/2602.01873v2), §§3.2, 4.1; [local PDF](../../../papers/Tidehunter-2026-v2.pdf), [text](../../../papers/parsed/Tidehunter-2026-v2.md) | Value-cache/index/WAL hierarchy and lazy index-cell residency | P2/P3: budget value and index memory separately. Keep this fork's authoritative reference lookup first; do not copy the paper's key-only LRU shortcut across distributed cuts. |
| [Native decompression-cache invariants](../../tidehunter/src/decompress_cache.rs#L1), reachability and CLOCK/single-flight comments | Immutable address lifetime, cheap hit metadata, one-touch eviction | Preserve reference reachability and bounded ownership. Do not copy blocking fill waits or transient budget overshoot into the transport path. Native code remains unchanged. |
| [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache), LRU capacity/strict limits, sharding, statistics and Simulated Cache | Cache capacity fragmentation, pinned entries and lock/admission costs | P2/P3: one ownership-domain budget, explicit retained/pinned statistics, capacity curves. Insertion failure must bypass the derivative cache, not fail a valid read. No imported numeric defaults or automatic global LRU replacement. |
| [RocksDB memory usage](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB), Block cache, Indexes/filter blocks, iterator-pinned blocks | Cache capacity is not total host memory; application and page caches interact | P2: full host envelope and duplicate-copy accounting. Do not assume RocksDB's compressed/decompressed tier behavior matches our encoded WAL members. |
| [RocksDB cache_key.h, a004c2d](https://github.com/facebook/rocksdb/blob/a004c2d850475150c3da293e6b812c8877a82d81/cache/cache_key.h), `OffsetableCacheKey` identity and offset | Bare offset/user-key reuse can select stale bytes | P2/P3 retain full `ValueRecordReference`, database/generation/version/member identity; no TTL-based freshness. |
| [KVell, SOSP 2019](https://raw.githubusercontent.com/BLepers/KVell/master/sosp19-final40.pdf), §§4.1, 4.3, 5.3–5.4, Algorithm 1; [local text](../../../papers/parsed/KVell-2019.md) | Immediate resident hits, bounded asynchronous I/O, predictable metadata allocation | P2/P3: separate ready hits from I/O, keep node-wide limits and bounded eviction. Do not copy in-place slabs, no-WAL durability, fixed wait-for-batch behavior or assume its direct-I/O results transfer to buffered io_uring. |
| [Redis client-side caching](https://redis.io/docs/latest/develop/reference/client-side-caching/), Avoiding race conditions, connection loss, What to cache | Mutable key caching needs invalidation ordering and connection-loss handling | Keep reference-keyed caching after index resolution. P3 admission favors reuse; do not add TTLs/broadcast tracking or another mutable client cache as a shortcut. |
| [Linux Page Cache](https://docs.kernel.org/mm/page_cache.html), opening description | Buffered reads/writes and mmap share page caching; direct I/O differs | P0/P3: SQEs are not disk IOPS, shard page-cache hits still pay RPC. Keep buffered WAL semantics. |
| [libfabric EFA 2.6 manual](https://github.com/ofiwg/libfabric/blob/v2.6.0/man/fi_efa.7.md), Progress, capabilities, completion events | Normal `efa` requires manual progress; `efa-direct` has different guarantees | P1: frame submission/CQ retirement and progress evidence. No claim that EFA is inherently slower, no unreviewed provider/security change. |
| [Pinned TiKV/Raft Engine source review](TIKV_SOURCE_REVIEW_2026-10-06.md#exact-source--local-problem--change), [Engine::write / barrier](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs), [TiKV async writer](https://github.com/tikv/tikv/blob/c61d92c26a4d96a575386f5e32179550556e2c29/components/raftstore/src/store/async_io/write.rs) | Prepare before grouped persistence; completion follows exact persisted progress | P4/P5 retain already implemented grouping, namespace proof and ordered publication. Do not add Raft replication or another physical WAL to this RF1 optimization. |
| [DiskANN/DistributedANN prior exact mapping](SOURCE_GUIDED_PIPELINES_2026-10-06.md#3-exact-source-to-code-map), bounded lookahead and independent work | Repeated scan orchestration and unnecessary serial dependencies | P7 preserves already implemented exact two-page scheduling; only cursor reuse is new. ANN approximation, pruning and source benchmark numbers are not applicable KV guarantees. |

## 8. Local validation and eventual hardware comparison

The completed local campaigns are recorded in section 10. They use five shard
processes, explicit read-memory allowances, physical-read evidence for evicted
passes, and swap/process-cleanup evidence. The laptop shares one SSD, CPU and RAM
pool. Local revision comparisons identify local behavior and regressions; **even
their relative ratios and rankings need not transfer to a cluster**. Software fabric
is not hardware EFA. Neither slower local results nor faster ones establish what
independent NVMe drives and NIC progress will do.

### 8.1 Required workload and cache controls

| Case family | Purpose |
| --- | --- |
| Caches disabled, warm OS pages | Expose transport/progress and buffered-read overhead without hiding it behind frontend hits |
| Coordinator-only, shard-only, combined caches | Attribute marginal capacity/latency benefit and duplicate memory cost |
| Hot set fitting coordinator cache | Measure real local-hit distribution, including authoritative index lookup |
| Hot set exceeding coordinator cache but fitting owner-shard residency | Measure the added cluster RAM's useful coverage, per-shard skew and remaining RPC cost |
| Uniform reads beyond explicit caches | Show eviction/admission overhead without manufacturing locality |
| Hot point reads plus one-pass scan | Protect hot residency; measure both scan completion and point-read tails |
| Dataset beyond actual host/cluster resident capacity, or explicitly evicted passes | Prove physical storage work; label short evicted passes separately from sustained larger-than-RAM behavior |
| Updates/deletes, captured reads, checkpoint/reopen, cancel/fence/failure | Preserve exact versions, integrity, bounded ownership and recovery |
| Durable writes below and across automatic-checkpoint thresholds | Isolate preparation, normal persistence and checkpoint tail costs |
| B1/W1, grouped writes/reads, windowed reads, sequential mixed and true concurrent mixed | Stop treating one favorable batching shape as all-operation parity |

Keep 1/8 KiB results separate; add other sizes only from a justified application
workload. Hold placement, durability, offered concurrency, read policy and memory
controls fixed for isolated comparisons. Warm caches through recorded requests,
not hidden data copies. Measure startup/fill transient separately from steady state.

Report p50/p95/p99 with enough independent operation samples, throughput, hit/miss
counts by tier, bytes served/admitted, evictions/bypasses, cache/metadata/pinned bytes,
per-host RSS/cgroup/page-cache evidence, CPU/context switches, physical-read deltas,
write-group/barrier counts and lifecycle stage times. Per-request instrumentation
must be bounded and its overhead separately checked. Missing timing is not zero.

Retain two distinct claims: engine/path efficiency under stated comparable memory
budgets, and total cluster capacity/performance from additional hosts. Report the
extra RAM/CPU resources used. An equal-total-memory native control is valid only if
it fits a real host; do not force it into swap and call that an engine comparison.

### 8.2 Provenance, acceptance and cloud boundary

- Freeze the exact candidate and Oct10 comparison source; preserve native behavior
  and old raw results. Build all changed binaries locally before any upload.
- Add a distinctly named campaign/profile rather than silently changing the
  recorded `checkpoint-async` matrix. Preserve command/config/source/binary hashes,
  compiler/features, host placement, effective memory settings and raw samples.
- Alternate matched repetitions. Report ratio-of-medians and paired changes with
  their labels; do not pool transport-paired native controls or opposing sizes.
- Cache-local near-native latency is a hypothesis to verify, not an acceptance
  waiver for slow misses. Every release decision must retain cache-zero progress,
  worst-window/checkpoint, recovery, scan and memory-pressure evidence.
- Run hardware validation only after local correctness and explicit approval.
  Use separate NVMe hosts and actual EFA evidence where needed. Preserve the existing
  immutable plan, deadline/export reserve, S3 result retrieval and owned-resource
  termination/audit workflow. Do not alter other users' resources.
- No full-Sui, replicated durability, failover, sustained capacity-scale or universal
  latency-parity claim follows from this RF1 engine campaign. Those remain separate
  validation scopes, not silently waived requirements.

## 9. Deferred architecture choices and completion status

A bounded registered shard-memory tier or compute-owned NVMe-oF namespaces can be
investigated later. NVMe-oF can place file caching on the compute host in a different
ownership architecture; it does not remove cold device service or automatically
supply record identity, fencing, reclamation and durability. Neither is a prerequisite
for fixing the measured EFA gate or using existing cache capacity.

Do not introduce global physical-WAL ordering, remote value search/broadcast,
mutable distributed-cache coherence, new placement, GC or replication under the
label of cache optimization. Reopen those designs only with an explicit requirement
and a separate correctness/performance case.

**Status:** the local implementation and verification are complete. Performance
parity, hardware-EFA improvement and independent-NVMe scaling are not established.
The next hardware campaign is prepared in code but has not been launched.

## 10. Implementation and local evidence, 11 October 2026

### 10.1 Completed mechanisms and exact boundaries

| Step | Implemented change | Preserved invariant / evidence |
| --- | --- | --- |
| P0 | Exclusive write-window parent spans; foreground-checkpoint subset; append-service, recovery and shutdown spans; transport/cache counters; named campaign and strict validators | Nested worker elapsed sums are not additive wall time or CPU. Unsupported p95/p99 cells are blank. Old results gain no invented timings. |
| P1 | Separate output submission/retirement cursors; accepted-ciphertext send fences; contiguous completion frontier over four fixed TX slots; 32 live output-frame bound | TLS order, output bytes and reply leases remain owned through CQ retirement. Cancellation/close do not fabricate completion; endpoint quarantine remains. Protocol and ALPN are now v3 on both peers. |
| P2 | Larger finite cache budgets; `read_memory_allowance_bytes`; preallocation metadata estimates; bounded CLOCK insertion; independent shard cache/admission locks | Cache plus configured in-flight read bytes must fit the allowance. Eviction examines at most 64 global steps plus one four-way set, then bypasses. Generation checks and full reference identity remain. |
| P3 | `ReadClass::Point` / `ScanNoFill` on the wire; scans can hit but do not populate either cache | Membership, epoch, integrity and captured-cut checks still apply. Buffered asynchronous file reads remain the default. No direct I/O, blocking network-owner read, or forced-async policy change. |
| P4 | Index digest computed during encoding/write; unchanged checkpoint no-op; reuse unchanged mapped cell bytes; bounded parallel proofs | Captured cuts preserve post-cut overlays. Manifest adoption still follows durable components/proofs. The 64 MiB / 1,024-batch tail bounds are unchanged. |
| P5 | Reuse validated local preparation/routing metadata through publication; measure inventory wait/residence and nested append work | Untrusted input and receipts are still verified. Whole-window preflight, grouped persistence and contiguous whole-batch publication remain. |
| P6 | Per-open TLS/config reuse; at most eight parallel setup/fence/proof/replay tasks; parallel independent seals; explicit worker joins | All required shards fence before serving. Ordered replay and bounded pages remain. Workers drain/join before the authority lock is released, including failures. |
| P7 | Persistent captured-cut scan cursor and merge heap across pages; one changed-cell history expansion per checkpoint | Exact forward/reverse order, exclusive bounds, tombstones and oversized records remain. Two-page / 4 MiB-page prefetch bounds are unchanged. |

The source-to-problem map in section 7 supplies the design references. These changes
borrow bounded ownership, cache admission, independent work and batching principles;
they do not import another system's durability, approximate queries or performance
numbers. No new Rust dependency, native engine algorithm, Sui or old middleware
change was required. Comparing frozen manifests found 32 changed Rust/Cargo inputs,
with no changed inherited `tidehunter/src/` file outside `distributed/`.

Two defects found during integration were fixed:

- Hash routing had omitted a separately owned reduced-key allocation from its
  reservation. The distributed layer now charges the derived bytes plus a
  conservative 64-byte owner allowance. A short-input hash-routing regression
  failed before the fix and passed afterward; the inherited hash is unchanged.
- An outer foreground-checkpoint clock could exceed its parent checkpoint clock
  by a few hundred nanoseconds. It now uses deltas of the same checkpoint counters.
  The regression checks exact agreement when the automatic checkpoint is the only
  checkpoint. The first local campaign stopped on this validator failure; it is
  not included in the completed comparison.

Memory counters need precise interpretation. Cache-owned requested allocations
include fixed metadata. Conservative response/read leases and returned scan logical
value bytes are reported separately; they are not unique physical memory and may
overlap shared backing allocations. Arbitrary caller-held `Bytes` clones are not
an exact live-allocation gauge. The allowance is a **read-memory slice**, not an
RSS limit or a substitute for a host/cgroup budget. Large-cache configuration was
exercised, not filled to multi-GiB resident capacity.

### 10.2 Frozen provenance and completed checks

All new evidence is under
[`logs/cluster-ram-2026-10-11/`](../../logs/cluster-ram-2026-10-11/).

| Build | Source-tree SHA-256 | Benchmark SHA-256 |
| --- | --- | --- |
| Preserved October 10 baseline | `84bf88b81410cbc82cfd59eb987b8012a887d9288f3b76e250b9678587efa7d0` | `081a77b68a54b3f165dbadaaade8d2a87d28398d5c6723eeab522a34436bfb4c` |
| Final October 11 candidate, `current-fixed-timing/` | `1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28` | `0a96d28ece36649fec0333ac3f427c3d4b3dab0c9f21fb430b420b5f1dc85d8e` |

The candidate uses rustc 1.90.0 and `experimental-fabric,test-utils`. The local
libfabric is 2.6.0, SHA-256
`3d3b4d47c1c9550c8d2c2b57d725799fb890ccc791398168af77f6dc2eb7b662`.
The benchmark, cluster-check and recovery release examples were built locally.
Recovery includes `--cfg checkpoint_recovery_stats`. Both peers must use the same
protocol-v3 candidate; old/new campaign cases use separate complete fleets.

Completed verification:

- 226 distributed tests passed. The three normally ignored release probes were
  then explicitly run and passed, including authenticated software-fabric recovery
  and out-of-order completion on one connection.
- 18 fabric tests passed; six software-provider tests and nine targeted nonblocking
  tests also passed. The final checkpoint-timing regression passed.
- Strict `cargo clippy ... -- -D warnings` passed for fabric and TCP-only builds;
  `cargo fmt --all -- --check` passed.
- The offline Python suite passed with two optional real-systemd tests skipped.
  Those two were subsequently enabled and passed against local disposable units.
- Real five-node TCP and software-fabric cluster checks passed: abrupt coordinator
  exit, checkpoint/tail recovery, node restart, exact retries, partial fencing,
  missing-node refusal, wrong/untrusted TLS identity, forged references and stale
  epochs. Values, tombstones and scans were checked against the native oracle.
- All 33 TCP and 20 software-fabric campaign cases passed the final evidence
  validator. Deliberately corrupted temporary outputs were rejected for invalid
  percentiles/counts, overlapping parent times, budget overflow and duplicate
  physical-I/O attribution.

| Evidence directory | Completed work |
| --- | --- |
| `tcp-verified/` | 33 baseline/current/native, cache-zero, capacity, pressure, concurrent-mixed, diagnostic and large-config cases |
| `fabric-verified/` | 20 baseline/current/native core/scalar/equal-cache/diagnostic cases using `tcp;ofi_rxm`, **not EFA** |
| `resident-policy/` | Six fixed-topology mapped/positional/async controls at 1/8 KiB |
| `recovery-tcp/`, `recovery-fabric/` | Four cases each: zero/128-batch tails, coordinator exit or full five-node SIGKILL/restart, exact 2,048-key audit and retry checks |
| `host-resource-pressure/` | Three pressure cases exercising host/cgroup snapshots outside API timers |
| `correctness-tcp/`, `correctness-fabric/` | Separate-process correctness and failure scenarios |

The completed TCP campaign observed zero host swap-in/out pages and a peak sampled
process-tree RSS of 904.543 MiB. Software fabric observed zero swap-in and **three
host swap-out pages** (12 KiB on this host), with a 3,783.082 MiB tree peak. Host
swap counters do not identify the responsible process; this is not evidence of
benchmark-induced swap thrashing. The later pressure/resource run observed no swap
activity, with a 92.699 MiB maximum tree peak.

The added host/cgroup snapshots were exercised by the last pressure run, not
retroactively collected for earlier cases. Their cgroup includes other desktop
members, so its file-cache/anonymous totals are not benchmark-only RAM. All recorded
owned PID lists were empty after cleanup; a final process check found no running
benchmark, recovery or cluster-check process. Disposable datasets were removed;
raw results and frozen binaries were retained.

### 10.3 Matched local TCP comparison

**One repetition per case. Not a confidence interval or a cluster forecast.**
Core rows use B32/W32, one read lane, two shard read workers, the unchanged async
read policy, zero explicit caches and five loopback shard processes. Native uses
its matched synchronous-durability control and configured native read workers.
Throughput below is thousands of logical operations per second. The
[complete CSV](../../logs/cluster-ram-2026-10-11/tcp-verified/comparison.csv)
also contains scalar, mixed, tail, diagnostic and resource metrics.

| Metric | Old 1 KiB | New 1 KiB | Native 1 KiB | Old 8 KiB | New 8 KiB | Native 8 KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Durable B32/W32 load, k ops/s | 197.017 | 154.699 | 252.075 | 39.468 | 39.623 | 98.575 |
| Worst sampled load window, ms | 7.407 | 10.367 | 6.159 | 39.666 | 38.705 | 12.597 |
| Durable B1/W1 load, k ops/s | 1.169 | 1.279 | 1.600 | 1.140 | 1.054 | 1.434 |
| Warm scalar p50, µs | 23.825 | 25.180 | 0.925 | 36.274 | 57.767 | 2.119 |
| Warm window-32 reads, k ops/s | 65.058 | 97.216 | 435.596 | 31.641 | 58.155 | 289.874 |
| Evicted-pass scalar p50, µs | 75.862 | 68.712 | 1.090 | 103.945 | 95.212 | 46.032 |
| Evicted-pass window reads, k ops/s | 141.878 | 70.968 | 176.112 | 59.788 | 60.888 | 82.151 |
| Exact full scan, k values/s | 339.126 | 357.621 | 920.724 | 69.499 | 62.457 | 19.144 |
| Open, ms | 90.982 | 40.088 | 4.525 | 83.384 | 31.363 | 7.707 |
| Reopen, ms | 76.179 | 31.444 | 1.753 | 74.434 | 34.446 | 1.323 |
| Shutdown, ms | 18.330 | 7.765 | 1.226 | 18.702 | 9.230 | 0.895 |

Startup and warm concurrent reads improved locally. Several scalar, write, scan and
evicted-pass measurements did not. These are observed regressions, not proof of
their cause or of a hardware regression. The evicted passes verified nonzero physical
reads, but warm pages can accumulate within a pass: the native 1 KiB median above
does **not** mean a 1 µs SSD access. They are not sustained larger-than-RAM tests.

### 10.4 What the RAM controls actually demonstrate

The 8 KiB capacity probe records 4,096 fill keys (32 MiB of values), then 1,024 hot
requests, a 10,240-key scan and another hot pass. Hot passes sample a subset; they
are not a complete uniform sweep of the fill set. Every checked scan admitted zero
bytes to both caches. The post-scan results were:

| Coordinator MiB | Each shard MiB | Hot p50, µs | Coordinator hit rate | Shard hit rate among coordinator misses | Shard WAL reads |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 16 | 34.295 | 0% | 100% | 0 |
| 128 | 0 | 0.848 | 100% | Not applicable | 0 |
| 2 | 0 | 44.830 | 5.957% | 0% | 963 |
| 8 | 0 | 1.132 | 75.781% | 0% | 248 |
| 8 | 16 | 0.942 | 74.414% | 100% | 0 |

This demonstrates the intended distinction: coordinator hits remove the shard
exchange; shard hits remove the WAL fetch but still pay that exchange. A hit-heavy
median says nothing about miss latency or p99. These unequal-capacity cases are
not equal-resource native/distributed comparisons.

Separate equal-nominal-8-MiB controls are in the CSV. For 8 KiB values, the current
native/distributed hot p50 values were 2.028/1.639 µs; their initial point-read p50
values were 2.334/56.982 µs. Native cache capacity counts nominal values, whereas
the distributed budget includes metadata, so even this is not exact equal RAM.
A 1 GiB coordinator / 512 MiB-per-shard configuration also ran with explicit 2 GiB
read allowances; it proves configuration/allocation acceptance, not full occupancy.

### 10.5 Timing attribution and software-fabric limits

The exclusive current TCP load-window totals were:

| Parent interval, summed over measured API windows | 1 KiB, ms | 8 KiB, ms |
| --- | ---: | ---: |
| API total | 65.536 | 257.906 |
| Preparation / admission | 16.748 | 97.995 |
| Storage exchange / wait | 36.437 | 129.126 |
| Publication | 11.536 | 17.088 |
| Dispatch | 0.489 | 1.257 |
| Checkpoint | 0 | 11.696 |

Storage wait is about 56% / 50% here. It is **not network time**: it includes remote
service, persistence, queueing and progress while the caller waits. Node inventory
residence nests decode/group/WAL/persist work. Worker elapsed sums can exceed parent
wall time because workers overlap. The 8 KiB foreground checkpoint is the same
11.696 ms checkpoint subset, not another additive interval.

Detailed exchange/CQ clocks have separate diagnostic cases; they are not silently
combined with normal cases. A single pair does not establish stable instrumentation
overhead. `cq_max_poll_gap_ns` measures gaps only while a local send is outstanding,
not receive-only starvation, completion latency or RTT. Aggregated peaks are maximum
individual-connection peaks, not simultaneous cluster totals.

The separate legacy `profile_tcp_primitives` utility prints zero percentile
placeholders for aggregate-only encode/decode rows. Those percentiles were not
measured and are excluded from this analysis; use their total/iteration counts
only. Its TLS-roundtrip rows do contain samples, but are not complete storage RPCs.

Software-fabric results are mixed too. They exercise the same ownership/protocol
path, but use a different provider and one host:

| Software-fabric metric | Old 1 KiB | New 1 KiB | Old 8 KiB | New 8 KiB |
| --- | ---: | ---: | ---: | ---: |
| Durable B32/W32 load, k ops/s | 150.330 | 182.360 | 37.044 | 36.166 |
| Warm scalar p50, µs | 35.513 | 28.075 | 42.549 | 60.785 |
| Warm window reads, k ops/s | 31.449 | 31.487 | 19.275 | 18.737 |
| Evicted window reads, k ops/s | 29.278 | 61.788 | 50.748 | 29.754 |
| Full scan, k values/s | 338.778 | 168.290 | 73.588 | 73.413 |
| Reopen, ms | 970.777 | 320.815 | 1,035.450 | 329.656 |

The complete software-fabric metrics and matched native controls are in its own
[CSV](../../logs/cluster-ram-2026-10-11/fabric-verified/comparison.csv).
Do not call these EFA measurements or use them to select a hardware provider.
The resident-read controls likewise compare whole mapped/positional/async policies,
not an isolated `IOSQE_ASYNC` flag cost; no policy was changed from these numbers.

Abrupt-exit recovery, zero/128-batch tails respectively:

| Recovery mode | TCP open, ms | Software-fabric open, ms |
| --- | ---: | ---: |
| Coordinator restart, zero tail | 39.615 | 342.953 |
| Coordinator restart, 128 batches | 91.566 | 394.810 |
| Five-node restart, zero tail | 30.422 | 279.557 |
| Five-node restart, 128 batches | 95.588 | 427.631 |

The TCP 128-batch replay spans were about 47 ms; software-fabric setup/read-lane
creation contributed about 250–282 ms before counting other open stages. These
are process-crash recovery measurements with warm host caches, not host-reboot,
device-loss, replica failover or multi-host network measurements.

### 10.6 Local approval boundary and remaining acceptance

Local implementation alone did not pass hardware performance acceptance.
Section 11 records the subsequent user-approved `cluster-ram` campaign. Read-miss,
scalar-write and universal-parity targets remain unmet.
The new profile preserves the October 10 comparison source and freshly freezes the
candidate, with separate TCP/EFA cases and one NVMe host per shard. Retain actual
source/binary/package identities. The guest builds each revision's own recovery
example rather than overlaying new recovery code onto the baseline.

The runner retains qualification, exact scope selection, stage/resource validation,
export reserve, deadlines and owned-resource cleanup. Host collection is included
in its bundle inputs. Section 11 supersedes the local-only deployment status.

Compare cache-zero scalar/window/tail throughput, checkpoint windows, scans and
recovery as well as capacity curves. Keep physical-I/O evidence and provider progress
counters. Require actual EFA qualification and isolate instrumentation overhead with
repeated matched controls. Separate-SSD results may reverse local rankings because
the laptop shares storage bandwidth, CPU scheduling and memory pressure.

Full Sui, replication, production-sized residency/placement skew and universal
native-latency parity remain outside this RF1 engine campaign. No installed
`kvstore` skill files were present to update; this plan, campaign README and fabric
design document now describe the changed contracts.

## 11. Authorized hardware validation, 11 October 2026

### 11.1 Scope, provenance and completeness

The user approved deployment after local completion. Three sequential fleets each
used six `g6.16xlarge` instances in `us-east-1a`, with spread placement: five shard
hosts using separate instance-store NVMe devices and one coordinator/native host.
Actual EFA qualification preceded every scope. This remains an RF1 engine campaign:
no replicas, full Sui workload, host-loss failover or NVMe-oF claim.

The candidate source was
`1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28`;
the October 10 baseline was
`84bf88b81410cbc82cfd59eb987b8012a887d9288f3b76e250b9678587efa7d0`.
The candidate was rebuilt locally; the baseline used its retained, hash-verified
local binary. Both revisions were independently rebuilt on EC2, including their
own recovery examples. The package SHA256 was
`026883af385b43626657d14d3299ba79d090770c30781ddadc025bb652ec5b02`.
Each receipt records the actual cloud binary hash; do not replace it with the
different local binary hash.

| Scope | Run ID | Validated result executions | Verified node archives |
| --- | --- | ---: | ---: |
| Core | `th-5754f49cdbdf4a6b` | 172 | 440 |
| Capacity | `th-dd834b22e5e45840` | 126 | 480 |
| Recovery | `th-ae280fbf19fb05fb` | 44 | 220 |

All 342 executions passed, covering all 334 unique full-profile identities.
Four qualification identities repeat per fleet; do not pool those repetitions.
All node evidence records zero swap-in and swap-out pages.
[Exact identity and source validation](../../logs/cluster-ram-aws-2026-10-11/measurements-validation.json).

Raw archives, extracted samples/timings/logs, source/build receipts and complete
analysis are retained locally:

- [Core analysis](../../logs/cluster-ram-aws-2026-10-11/run-core-validated/analysis/analysis.json)
  and [all core comparisons](../../logs/cluster-ram-aws-2026-10-11/run-core-validated/analysis/comparison.csv).
- [Capacity analysis](../../logs/cluster-ram-aws-2026-10-11/run-capacity-validated/analysis/analysis.json)
  and [all capacity comparisons](../../logs/cluster-ram-aws-2026-10-11/run-capacity-validated/analysis/comparison.csv).
- [Recovery analysis](../../logs/cluster-ram-aws-2026-10-11/run-recovery-validated/analysis/analysis.json)
  and [all recovery comparisons](../../logs/cluster-ram-aws-2026-10-11/run-recovery-validated/analysis/comparison.csv).

### 11.2 Native versus distributed, application caches disabled

Eight-KiB values; medians over five repetitions. Native controls are paired to
each transport, not pooled. Except for the explicitly scalar-write row, the shape
is B32/W32 with 8,192 initial values. Throughput counts logical mutations or reads.

| Metric | Native TCP-paired | TCP | Native EFA-paired | EFA |
| --- | ---: | ---: | ---: | ---: |
| Durable batched writes, k mutations/s | 45.022 | 40.297 | 44.899 | 41.131 |
| Scalar durable write p50, B1/W1, microseconds | 206.974 | 696.885 | 222.855 | 573.792 |
| Warm scalar read p50, microseconds | 3.000 | 358.007 | 2.941 | 291.726 |
| Warm window32 reads, k reads/s | 199.397 | 39.418 | 198.377 | 41.778 |
| Evicted-pass window32 reads, k reads/s | 102.433 | 39.109 | 101.829 | 41.378 |
| Full scan, ms | 869.982 | 121.073 | 881.058 | 113.169 |
| Engine reopen, ms | 2.663 | 24.796 | 2.777 | 96.040 |

The scan is not an independently cold or warm experiment: earlier phases affect
residency. Its distributed win does not isolate disk parallelism from batching
and aggregate cache effects. Evicted passes verify disk activity but warm during
execution; not every measured access is an NVMe fetch. B32/W32 load has only eight
window samples per repetition, so it does not establish a production p99.

Against the baseline **remeasured in this same campaign**, EFA warm-window
throughput increased from 23.58k to 79.27k reads/s at 1 KiB and from 23.08k to
41.78k at 8 KiB. These are not the differently shaped October 10 historical rates.
Eight-KiB batched writes fell from 41.879k to 40.297k mutations/s on TCP and from
43.190k to 41.131k on EFA. Do not describe all metrics as improved.

### 11.3 RAM cache result and remaining read-miss cost

With a 128 MiB frontend budget, the dedicated 4,096-key hot probes had **4,096 hits
and zero misses in every repetition**, before and after a full scan.
P50 was 0.88/0.79 microseconds before/after scan for TCP and 0.87/0.77 for EFA.
This proves the local hit path and scan no-fill behavior on the tested working set.
It is not an equal-budget native-cache comparison or larger-than-RAM scaling proof.

The initial point-get phase had 2,059 hits and 2,037 misses. Its microsecond p50
therefore hides a large slow-miss population; do not present it as all-hit latency.
With only a 16 MiB cache per shard and no frontend cache, before-scan hot-probe p50
was still 260.614 microseconds on TCP and 195.910 on EFA. Shard RAM does not remove
the remote exchange.

In the separate 8 KiB reopened/cache-cleared diagnostic, EFA mean whole-call time
was 272.403 microseconds, of which exchange time was 230.233; server handler time
was 31.834 and index lookup 1.086. TCP measured 351.096 whole, 309.381 exchange,
30.894 server handler and 0.893 index. These are nested durations, not additive
components or pure wire time. RPC/progress/scheduling remains material; the central
index lookup is not the main scalar-read cost.

### 11.4 Recovery and concurrent mixed work

The five-process restart checks validated values, tombstones, scans, checkpoint
adoption and retry identities. With zero replay tail, EFA engine reopen fell from
389.715 to 97.273 ms; with 128 tail batches it fell from 550.930 to 151.003 ms.
Surviving-shard reopen changed from 376.884 to 96.757 ms and from 532.886 to
150.613 ms respectively. These remain process-recovery measurements with warm host
caches, not host reboot, replica failover or a matched native crash-recovery series.

Controller-inclusive full-restart restoration was 2.717 s with no tail and 2.549 s
with tail128; it includes SSM, readiness and verification. It is not engine latency.
The concurrent mixed phase's shared-wall rates were about 3.2% above paired native
for TCP and 5.7% for EFA. Reads and writes share that denominator; this does not
establish independent read-rate or scalar-latency parity.

### 11.5 Operations and remaining acceptance

The four pre-launch runner fixes are recorded in the
[campaign README](../../experiments/aws-sharded-wal/README.md#authorized-deployment-preflight-11-october).
All 67 offline runner tests passed, including optional MCP and real-systemd checks.
The repaired profile analyzer also processed all 53 saved local cases before upload.
No Rust engine change was made during this deployment campaign.

Results were downloaded through S3 and checked against archive byte counts and
SHA256 receipts before bucket cleanup. The operator's SSM request was denied;
startup used the existing EC2 Instance Connect fallback with an MCP-authenticated
host key and temporary exact-/32 ingress. AWS operations used official AWS MCP.
Fixed guest termination deadlines were never extended.
Per-scope `cleanup-receipt.json` and the final independent AWS audit establish
resource closure separately from benchmark completeness.

Hardware evidence now supports the EFA concurrent-read and connection-setup gains,
bounded cache correctness and a fast frontend all-hit path. It does **not** pass
universal parity: uncached scalar reads, windowed reads and scalar writes still
lose to native, and some batched-write rates regressed. Keep these as open
performance work; do not weaken correctness, hide misses or substitute cache-hit
numbers for the remote path. No installed `kvstore` skills were present to update.

Final closure: all 18 launched instances were confirmed terminated. The independent
23-region audit found no active/stopped TideHunter fleet and no owned EBS volumes,
ENIs, security groups, placement groups or Elastic IPs. All run roles, instance
profiles and S3 buckets were absent, including the superseded unlaunched namespace.
The four unrelated instances remained running. See the
[completion receipt](../../logs/cluster-ram-aws-2026-10-11/campaign-completion-summary.json)
and [raw AWS audit](../../logs/cluster-ram-aws-2026-10-11/final-aws-audit.json).
The measured launch-to-shutdown compute upper bound is **$21.81**, excluding
ancillary charges and not an AWS invoice. Pending time is conservatively included;
[AWS stops instance-usage billing at shutting-down](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html#instance-billing-by-state).


## 12. Scalar latency and transport lifetime implementation

This change targets the remaining scalar-read, scalar-write and reopen costs in
§11. It does not replace the per-shard Value WAL, weaken the durability mode, add
NVMe-oF, or claim native latency for a remote cache miss. Native Tidehunter's
engine and Sui are unchanged; implementation is in this fork's distributed layer,
`tidehunter-fabric`, and the benchmark examples.

### 12.1 Exact source-to-change mapping

| Observed cost or hazard | Source mechanism | Implementation and retained contract |
| --- | --- | --- |
| EFA control-socket syscall on each nonblocking TLS receive attempt | Pinned [EFA provider manual](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md): manual CQ progress does not require TCP polling | `tcp/reactor.rs::Session::progress_fabric_data` performs one CQ sweep; fabric TLS adapters use `try_*_no_poll`, not socket data I/O. `ControlPeerHealth` checks the separate control socket every 1 ms of active progress, plus one bounded sweep/scheduler delay, and forces a check on reuse. EOF, unexpected control data and existing request/idle deadlines still fail closed. |
| Repeated CQ polls, scheduling and zero initialization in active fabric loops | [FaSST §4](https://www.usenix.org/system/files/conference/osdi16/osdi16-kalia.pdf), bounded progress; [eRPC §4.3](https://www.usenix.org/system/files/nsdi19-kalia.pdf), short polling paths | `tcp/reactor.rs::ActivePoll` and `remote_db/pending_read.rs::drive_reads` use bounded 64-spin bursts followed by yield for fabric only. TCP keeps its socket policy. `endpoint/resources.rs` reads only the initialized, count-validated CQ prefix. Exact completion ownership, poisoning and deadlines are unchanged. |
| EFA endpoint creation and provider packet-pool registration | [AWS libfabric 2.6.0amzn1.0](https://github.com/aws/libfabric/releases/tag/v2.6.0amzn1.0), pinned upstream `3cc1095272b07ebeb5cc8f44851af6aa3eb75e41`; [eRPC §§4.2–4.3](https://www.usenix.org/system/files/nsdi19-kalia.pdf), bounded owned resources | `tidehunter-fabric/src/endpoint/resources/efa.rs` requests and validates 128 TX/RX entries. The source-derived packet/headroom bound, supported parameters and version gate are in [fabric DESIGN.md](../../tidehunter-fabric/DESIGN.md#bounded-efa-queues-and-setup-measurement). This does not remove the provider's independent peer/reorder/CQ allocation floors. |
| Recreating authenticated transports for every logical DB reopen | [eRPC §§3.1/4.2.2](https://www.usenix.org/system/files/nsdi19-kalia.pdf), endpoint/session lifetime independent of requests | `remote_db/transports.rs::RemoteTransportPool` retains at most one clean database's bounded control/read connection bundle. `open_with_transport_pool` takes exclusive ownership, validates the same-byte credential/configuration/shape fingerprint, rechecks peer certificate/name/current validity and fresh authenticated status, then runs normal epoch reservation, fencing, checkpoint proof validation and replay. Only successful drained/checkpointed/sealed shutdown returns resources. |
| Serialized read-lane creation after recovery | Parallel preparation without early publication; [Tidehunter §§3.3–3.4](https://arxiv.org/abs/2602.01873v2) requires valid snapshot/replay state before serving | `remote_db/transports.rs::ReadSetup` overlaps bounded read-connection setup with control setup/recovery. The owner joins it on success and every error before releasing the authority lock. Read lanes cannot serve before activation. |
| Blocking scalar get makes an actor dispatch and return handoff | [ReFlex §§3.1/4.1](https://hlitz.github.io/papers/reflex.pdf), short exclusive-owner path with asynchronous device work; [eRPC §§3.1–3.2](https://www.usenix.org/system/files/nsdi19-kalia.pdf), short handlers versus offloaded long work | `pending_read.rs::read_inline` lets a blocking scalar caller drive an idle read lane without the frontend actor handoff. The same RPC, epoch checks, record validation and byte credits remain. `submit_get` is still deferred; active requests are never stolen. Disk misses still use the shard's kernel-asynchronous executor. |
| Per-read payload allocation on the shard | [ReFlex §4.1](https://hlitz.github.io/papers/reflex.pdf) and [eRPC §4.2.2](https://www.usenix.org/system/files/nsdi19-kalia.pdf), owned reusable buffers | `tcp/read_executor.rs::ReadBuffer` returns an exact-sized allocation only after an exact successful CQE and the final `Bytes` owner drops. A node-wide pool retains at most `min(max_device_reads,128)` buffers within the existing pending-byte allowance. Cache/response clones and cancelled submitted reads keep their ownership. The bounded device-credit table keeps capacity instead of reallocating after every last scalar completion. |
| Scalar durable writes must not block read/fabric progress | [ReFlex §3.1](https://hlitz.github.io/papers/reflex.pdf), two-stage execution; [Tidehunter §3.1](https://arxiv.org/abs/2602.01873v2), distinguish foreground visibility from persistence | The server retains one append-worker transfer for decode/group construction/WAL and namespace persistence, while removing a redundant `Arc<StorageNode>` clone. Existing owned request decoding already avoids a second payload/hash pass. EFA progress changes also apply to append RPCs. No artificial batching delay or early acknowledgement is introduced; the durable barrier and its latency remain. |

These are adaptations, not copied implementations or imported paper throughput
claims. The eRPC implementation reviewed is
[`de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d`](https://github.com/erpc-io/eRPC/tree/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d),
especially `src/rpc_impl/rpc_ev_loop.cc`, `rpc_resp.cc` and `src/util/huge_alloc.cc`.
The papers' RAM/RDMA experiments do not establish durable NVMe performance here.

### 12.2 Correctness and transport boundaries

- EFA data stays on libfabric FI_MSG/FI_EP_RDM. TCP remains the default fallback
  chosen at connection creation; uncertain EFA requests are never replayed on TCP.
  TLS/framing/epoch/durability semantics are deliberately shared, not a slower
  TCP data path hidden under the EFA name.
- Wire protocol/ALPN is now v4 because buffer/control diagnostic layouts changed.
  Both peers must be rebuilt together; older peers fail during TLS negotiation.
  Preserved baseline executables use their own matching baseline shard processes.
- Every provider domain/endpoint still has one mutable owner. Pool checkout moves
  ownership only after old workers join; it does not concurrently share a domain
  or reset request IDs, TLS sequence numbers or completion-fence ordinals.
- Reuse preserves no index/cache/authority state. A changed credential file at the
  same path invalidates the bundle. A changed shard epoch is discovered by fresh
  status, not stale connect-time inventory. Failed reuse returns an error, not an
  automatic retry that could hide an uncertain request.
  The shard also revalidates the authenticated client certificate on a subsequent
  status request with its original trust snapshot and the current time, using
  [rustls `ClientCertVerifier`](https://docs.rs/rustls/0.23.31/rustls/server/danger/trait.ClientCertVerifier.html).
  Client-side server-certificate checking alone would not reject an expired
  client credential. Reuse performs both sides of this check without bypassing TLS.
  Side-effect-free scalar read admission errors do not poison write authority.
- `RemoteDatabase::open` remains a fresh transport open. Callers opt into
  `open_with_transport_pool`; dropping/clearing the process-owned pool releases
  idle endpoints. Process death cannot preserve this pool.
- Independent shard WALs, whole-batch publication, contiguous completed prefix,
  exact immutable references, checksums, crash recovery and durable receipts
  remain required. A CQ completion is not a storage acknowledgement.
- EFA's queue bound is version/protocol/configuration-specific. Configure the
  provider before its first initialization. Environment validation cannot
  inspect private `libfabric.conf` overrides; do not deploy private host-protocol
  threshold overrides outside the documented supported configuration.

### 12.3 Measurement contract and next hardware checks

The main benchmark preserves `reopen` as a fresh-transport measurement and adds
`reopen_reused_transport` plus a complete post-reuse value/tombstone audit.
The process-recovery example always creates fresh transports. Compare like with
like; a reused live process is not faster crash recovery.

Recovery evidence now includes created/reused connection counts,
`transport_checkout_ns` (identity check and disposal of mismatched idle resources),
`read_lane_wait_ns` (critical-path join wait), and overlapping
`read_lane_setup_ns`. TLS parsing is timed separately. Nested worker sums are not
additive wall time.
Fabric evidence breaks down allocation, discovery, fabric/domain, CQ/AV,
endpoint, bind/enable, MR registration, address/peer setup and deferred first
progress, with negotiated queue geometry. Reused transports have no new endpoint
creation profile. Reuse status time includes certificate/control revalidation
and draining previously owned sends.

Scalar diagnostics add `inline_read_ns`; actor queue/return time is zero only
when that call actually used the inline path. Existing exchange/handler/record
timings remain nested. Node evidence adds reusable buffer allocation/reuse and
idle-memory counters; transport profiles include actual `control_peer_checks`.
The local campaign accepts explicitly pinned baseline source/binary hashes so
the October 11 binary can be compared without changing historical campaign pins.

Hardware acceptance still requires rebuilt, hash-bound binaries on EFA hosts:
qualify negotiated queues and full-size encrypted traffic, record startup stages
and RSS, compare cold versus reused reopen, check control-loss/deadline behavior,
and compare scalar/batched reads and durable writes with matched TCP/native
controls. Use separate shard NVMe devices, actual evicted I/O evidence and
CPU/progress counters. The one-SSD software-provider path cannot prove EFA
latency, queue geometry on a NIC, independent-disk scaling or production parity.

### 12.4 Local measurements and unresolved costs

Evidence root: [`logs/scalar-transport-2026-10-11`](../../logs/scalar-transport-2026-10-11).
No AWS resources were launched or changed for this implementation.
Five RF1 storage processes share this laptop's SSD, CPU and RAM; these are engine
measurements, not a Sui workload, independent-disk scaling or replicated durability.

Provenance:

- Candidate source: `0f9ddec71640f93b46f2810744455c33f8cbac47f727e66765f654e6a0ac7227`.
- Candidate executable: `453c3f42f696dfa0a45fe84fd93dfa1928f34733744eff077d1a3eb25eb16d5a`.
- Baseline: October 11 `current-fixed-timing`, source
  `1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28`,
  executable `0a96d28ece36649fec0333ac3f427c3d4b3dab0c9f21fb430b420b5f1dc85d8e`.
- The source manifests differ in 26 Rust/Cargo inputs. No native
  `tidehunter/src/` file outside `distributed/` differs from the baseline.
- Release binaries use `experimental-fabric,test-utils` and pinned libfabric
  2.6.0. The recovery example includes its target-only recovery-statistics cfg.

**Matched TCP results.** Medians across three paired repetitions, with build
and native/distributed order alternated. Explicit frontend/shard caches were
disabled. Scalar workloads contain 320 load keys; the batch-32/window-32 workload
contains 10,240. Each scalar read phase samples 1,024 calls, each scalar load
samples 320 writes, and each repetition has one cold reopen.
Rates below use the instrumented phase wall clock, not summed overlapping calls.

| Metric | Value size | Previous distributed | Current distributed | Current paired native |
| --- | --- | ---: | ---: | ---: |
| Scalar read p50, warm host cache | 1 KiB | 42.338 µs | 28.801 µs | 0.806 µs |
| Scalar read p50, warm host cache | 8 KiB | 64.379 µs | 47.994 µs | 1.608 µs |
| Durable scalar write p50 | 1 KiB | 787.652 µs | 788.055 µs | 741.949 µs |
| Durable scalar write p50 | 8 KiB | 928.620 µs | 797.459 µs | 721.305 µs |
| Warm reads, window 32, batch workload | 1 KiB | 86.3k/s | 73.1k/s | 520.5k/s |
| Warm reads, window 32, batch workload | 8 KiB | 39.1k/s | 40.2k/s | 397.1k/s |
| Evicted-pass reads, window 32 | 1 KiB | 25.7k/s | 66.4k/s | 112.6k/s |
| Evicted-pass reads, window 32 | 8 KiB | 23.1k/s | 40.5k/s | 43.4k/s |
| Load writes, batch 32/window 32 | 1 KiB | 126.5k values/s | 137.2k values/s | 207.5k values/s |
| Load writes, batch 32/window 32 | 8 KiB | 30.1k values/s | 31.1k values/s | 70.5k values/s |
| Cold reopen, batch workload | 1 KiB | 36.567 ms | 31.925 ms | 2.161 ms |
| Cold reopen, batch workload | 8 KiB | 35.098 ms | 29.764 ms | 1.782 ms |

The scalar read p50 improved in every paired repetition at both sizes.
Throughput is not a uniform win: some paired runs reverse direction, and the
scalar-load workload's subsequent 8 KiB windowed read rate fell from 41.2k/s to
25.1k/s. Do not hide this result behind the improved evicted-pass median.
Three repetitions on one shared device do not establish a cluster ranking or
isolate the cause of those throughput changes.

**Reuse is a separate result.** For the batch workload, TCP's reused reopen
medians were 61.830 ms at 1 KiB and 60.434 ms at 8 KiB, slower than its cold opens.
For 8 KiB, the connection-setup stage fell from 5.477 to 0.771 ms, but fencing
rose from 13.447 to 41.762 ms and checkpoint load from 5.714 to 6.686 ms.
These are stage medians, not an additive decomposition. Fencing includes RPCs
and durable node work; these clocks do not identify it as pure network time.
In the smaller scalar workload, reused opens were 21.178/19.616 ms at 1/8 KiB.
Keep the pool opt-in; it is not a proven general TCP reopen optimization.

**Software fabric is functional/relative evidence only.** Its 20-case campaign
used one repetition through `tcp;ofi_rxm`, not EFA. For the 8 KiB scalar workload,
read p50 changed from 77.919 to 63.065 µs and durable write p50 from 908.620 to
893.559 µs. In the batch workload, cold reopen changed from 350.716 to
258.811 ms; reused reopen took 57.155 ms. Cold/reused connection setup was
194.076/1.011 ms, with ten created versus ten reused connections.
The software provider selected its unchanged 2,048-entry default queues; it
does not test the EFA-specific 128-entry configuration.

Execution evidence:

- The 8 KiB TCP diagnostic measured mean whole-call 49.189 µs, index lookup
  0.707 µs and inline work 48.124 µs. Actor queue/service/return clocks were zero
  because the inline path ran. Exchange was 47.654 µs; server handler 10.535 µs.
  These nested spans cannot be summed or subtracted into an exact wire latency.
- The same diagnostic's first 1,024 reads allocated five reusable device buffers
  and reused them 1,019 times. Each later evicted read phase reused 1,024 buffers
  with no new allocations and recorded 11,722,752 physical read bytes.
  This establishes real I/O during the phases, not a disk miss on every call.
- In software fabric's load diagnostic, node 0 recorded 195,515 CQ polls but
  only 703 control-peer checks. Control socket checks no longer scale one-for-one
  with CQ polling. The explicit control-violation scenario still closed the session.
- Legacy per-request socket/header/body fields in `exchange_profiles.csv` are
  unpopulated by the multiplexed reactor in both builds. Their zeros are **not**
  measured zero-cost I/O. Complete exchange spans and aggregate node counters
  remain available; per-request socket attribution remains unavailable.
- Both main campaigns recorded zero swap-in/out pages and no remaining sampled
  owned processes. Peak sampled process-tree RSS was 279,500 KiB for TCP and
  3,839,408 KiB for software fabric. These are whole-process peaks, not cache size.
- The existing local harness stops nodes sequentially; TCP fleet teardown took
  about 26 seconds in both builds. This is separate from measured engine
  shutdown/reopen. Every child exited; no teardown timeout was suppressed.

**Cold process recovery.** Four cases per transport verified all 2,048 keys,
tombstones, checkpoint state and exact retry identity, with ten fresh connections
and no reused transports in every reopened process. One run per case:

| Restart scope | Replay-tail batches | TCP engine open | Software-fabric engine open |
| --- | ---: | ---: | ---: |
| Coordinator only | 0 | 36.886 ms | 295.181 ms |
| Coordinator only | 128 | 119.769 ms | 368.322 ms |
| All five storage processes | 0 | 29.064 ms | 255.848 ms |
| All five storage processes | 128 | 119.867 ms | 342.306 ms |

These are process restarts with host caches available, not host reboot or a
matched native crash-recovery comparison.

Verification:

- 626 Tidehunter and 25 fabric library tests passed; ten tests were ignored by
  that full invocation. Three ignored fabric scenarios were then run explicitly.
- Eight real software-provider integration tests and the three explicit
  authenticated recovery/out-of-order/control-violation scenarios passed.
- The five-process correctness executable passed on TCP and software fabric,
  including abrupt coordinator exit, partial fencing, node restart, wrong server
  name/untrusted client, lost durable reply, exact retry and forged membership.
- Clippy passed with `-D warnings` for library/tests/examples in fabric-enabled
  and TCP-only configurations. All-target Clippy is still blocked by the
  unchanged native `benches/large_table_benchmarks.rs:169`
  `manual_is_multiple_of` warning; its file hash matches the baseline.
- Offline runner suite completed 67 tests: 65 passed, two optional systemd checks skipped.
- Frozen benchmark, recovery and five-process correctness binaries rebuilt.
  All 72 comparison/diagnostic executions and eight recovery cases completed.
  Final process lookup found no benchmark executables still running.

The scalar and cold-open improvements do **not** close the native gap.
EFA hardware was unavailable (`fi_getinfo: -61`, no local RDMA device), so the
provider-specific queue configuration, EFA startup savings and independent-NVMe
performance remain hardware acceptance gates under §12.3. Native engine and Sui
code remain unchanged. No installed `kvstore` skill documents were present.

## 13. EFA/NVMe hardware results, 11 October 2026

**Outcome:** the remote scalar path and cold transport startup improved, but
native parity is not achieved for scalar reads, scalar durable writes, concurrent
reads or cold reopen. EFA now beats TCP on the measured steady-state read rates.
Its fresh endpoint setup remains slower. Batch durable writes are close to the
matched native control. No additional fleet was launched to repeat completed work.

### 13.1 Execution, provenance and complete teardown

Run `th-db8b2af08939b4cc` used six `g6.16xlarge` instances in `us-east-1a`:
one coordinator/native-control host and five separate storage hosts. Each used
its own EC2 instance-store NVMe device, not five processes sharing one drive.
The run used RF1 and authenticated TCP or EFA message transport. It did not test
one-sided remote WAL access, NVMe-oF, replication or end-to-end Sui.

The local benchmark and recovery executables were rebuilt before upload.
The cloud rebuilt both frozen source archives independently with Rust 1.90.0,
`experimental-fabric,test-utils`, and libfabric `2.6.0amzn1.0`.
Manifest, source, executable and node-start identities were checked. Native
engine source was unchanged between the compared builds.

| Identity | Baseline | Current |
| --- | --- | --- |
| Source tree SHA-256 | `1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28` | `2e694cb0be9fe0738c217b75afb9fe0ba5d2eef170573bd95f092fa40e93dc57` |
| Cloud benchmark SHA-256 | `0822b4299106ef1f7e5ff3e450359ab33d59cc9417338f2a900b90d7340adf91` | `1457305dcd473122ef91308d3da029dd96f15f542a39e9bfdd820182c553be51` |
| Cloud recovery SHA-256 | `dcf14757d3756b1a45845aa7b7b0cee8ab36563fab309746e605d664d1dc9106` | `947e5b268f219ec3718a41816e11ccad45388d5141aa497e2d46b1ae5b15ad58` |

Evidence root:
[`logs/scalar-transport-aws-2026-10-11/run-full/`](../../logs/scalar-transport-aws-2026-10-11/run-full/).
The original `controller.tar.gz` is 27,152,834 bytes, SHA-256
`de04c270ec724f4d03e10034804621eb432531946dd1ab8a2253715b4dcf4499`.
All **124 planned executions and 360 node archives** validate. The complete S3
prefix was also downloaded: 1,093 objects, 51,964,188 bytes. Every object length
was checked, and the downloaded controller archive's actual SHA-256 was checked.
`s3-preservation.json` records the inventory. `s3-export/` is private mode 0700:
it contains ephemeral node TLS/config packages and must not be published.

The original campaign receipt remains **`complete: false`**. All executions
finished before a final audit incorrectly required `transport` in
`service-restore.json`; the producer records restart timing there, not transport.
The analyzer now checks transport through the source-bound prepare command and
hardware/setup evidence instead. The fixture was corrected to match the actual
producer; the focused regression failed before the analyzer correction, and all
eight scalar-transport tests then passed. Reanalysis of the untouched archive
with `--profile scalar-transport --allow-partial` reports no invalid/missing case
or node evidence, successful qualification and a verified build pair. Only the
historical campaign receipt failure remains. No `COMPLETE` marker was fabricated.
This is post-hoc validated measurement evidence, not a claim that the original
controller audit succeeded.

All six instances entered termination at **18:59:07 UTC**, immediately after
export. MCP retrieved the result archive, then became unavailable with
`Unknown tool: aws___run_script`. After explicit fallback authorization, the
existing ownership-guarded cleanup ran through AWS CLI. Independent API checks
confirm no remaining run-owned EBS volumes, snapshots, Elastic IPs, network
interfaces, S3 bucket, security group, placement group, IAM roles or instance
profiles. The two protected instances in this region remained running and
unchanged. An earlier read-only 23-region check found no nonterminated instances
with our benchmark ownership tags. See `cleanup-receipt.json`,
`full-closure-verified.json` and `fallback-compute-closure.json`.

The observed on-demand price was $3.3968 per instance-hour. Launch-to-termination
accounting gives **$7.24 estimated EC2 compute** for this run; it includes pending
time and is not an invoice. It excludes already incurred EBS, public IPv4, S3 and
transfer costs. No resources from this run remain to accrue continuing charges.
Past usage can still appear on later bills; other users' resources were not
removed. See `cost-estimate.json`, the saved Pricing API response, and
[EC2 pricing](https://aws.amazon.com/ec2/pricing/on-demand/) /
[billing by instance state](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html#instance-billing-by-state).

### 13.2 Comparison method and limits

- Core cases use batch 32, write window 32, 10,240 loaded mutations and three
  repetitions. Scalar-write cases use batch/window 1, 320 mutations and three
  repetitions. Both use read concurrency 32 for their concurrent-read phases.
  These datasets differ; cross-family changes are not an isolated batching test.
- Explicit frontend and shard value caches are disabled (`f0-s0`) to expose the
  remote path. Linux page cache still operates. Warm native scalar reads are not
  raw NVMe reads. Cold phases evict before the phase, not before each operation;
  readahead and earlier reads can make later reads hit RAM.
- Both sides use `value_wal_file_and_directory_barrier`. Native controls are
  measured separately beside TCP and EFA; the tables retain both instead of
  pooling correlated native observations.
- Values below are medians of three run-level measurements. Percentiles are
  medians of within-run percentiles, not percentiles of pooled samples.
  Throughput is instrumented logical operations divided by whole phase time,
  not inverse p50 or a sustained production saturation result.
- A core write latency sample covers 32 batches × 32 mutations, not one key.
  Only ten such load samples and two mixed-write samples exist per core run;
  their p99 is unavailable. Get-many latency is per call of up to 32 keys.
- **Metadata defect:** `point_get_window` summaries label samples
  `complete_window`, while raw samples each contain one logical operation.
  Its latency is per read under concurrency, not per complete 32-read window.
  Original files are preserved; this report uses the raw sample semantics.
- Mixed read and write component rates share the whole mixed-phase timer.
  They are not independently timed rates. Scalar-write families do not include
  cache-evicted passes; those passes belong to the core family.
- These are small-data, RF1 engine/API measurements, not larger-than-RAM steady
  state, device saturation, failover durability or a full Sui workload. Three
  repetitions establish descriptive medians/ranges, not statistical significance.

Complete metrics, baseline changes, repetition ranges and source rows are in
`analysis/comparison.csv`, `analysis/transport-full-summary.json`,
`analysis/transport-primary-tables.txt` and `analysis/transport-latency-tables.txt`.

### 13.3 Read latency and throughput

Core scalar-read latency, microseconds:

| Value | Phase / percentile | Native, TCP control | TCP | Native, EFA control | EFA |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 KiB | Warm p50 | 0.840 | 255.496 | 0.850 | 204.893 |
| 1 KiB | Warm p99 | 1.590 | 409.588 | 1.640 | 360.317 |
| 8 KiB | Warm p50 | 2.870 | 297.716 | 3.020 | 226.614 |
| 8 KiB | Warm p99 | 4.410 | 471.760 | 4.580 | 386.688 |
| 1 KiB | Cache-evicted phase p50 | 0.820 | 328.467 | 0.760 | 270.946 |
| 1 KiB | Cache-evicted phase p99 | 224.704 | 528.021 | 207.755 | 471.920 |
| 8 KiB | Cache-evicted phase p50 | 98.092 | 438.559 | 100.362 | 356.898 |
| 8 KiB | Cache-evicted phase p99 | 185.514 | 625.543 | 188.134 | 527.751 |

Current warm scalar p50 improved from baseline by 18–19% on TCP and 20–23%
on EFA in the core cases. The native gap remains large. The 1 KiB cold-pass
native p50 is still a cache-hit statistic for much of that phase; nonzero
physical I/O does not turn every call into an NVMe miss.

Core read rates, thousands of keys per second:

| Value | Phase | Native, TCP control | TCP | Native, EFA control | EFA |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 KiB | Warm serial | 966.1 | 3.7 | 950.1 | 4.7 |
| 8 KiB | Warm serial | 285.7 | 3.3 | 272.1 | 4.2 |
| 1 KiB | Cold-pass serial | 147.8 | 3.0 | 155.3 | 3.7 |
| 8 KiB | Cold-pass serial | 12.3 | 2.3 | 11.9 | 2.8 |
| 1 KiB | Warm concurrent, 32 | 228.7 | 70.5 | 226.0 | 85.5 |
| 8 KiB | Warm concurrent, 32 | 200.1 | 39.0 | 201.8 | 49.3 |
| 1 KiB | Cold-pass concurrent, 32 | 193.8 | 66.8 | 189.4 | 79.0 |
| 8 KiB | Cold-pass concurrent, 32 | 125.2 | 38.1 | 124.5 | 49.1 |
| 1 KiB | Warm get-many, 32 keys | 1,155.7 | 67.0 | 1,147.7 | 80.8 |
| 8 KiB | Warm get-many, 32 keys | 260.7 | 37.7 | 252.8 | 45.4 |
| 1 KiB | Cold-pass get-many, 32 keys | 154.6 | 48.4 | 158.2 | 53.7 |
| 8 KiB | Cold-pass get-many, 32 keys | 12.3 | 29.5 | 10.7 | 34.2 |

EFA is faster than TCP in these steady-state read phases. Its warm concurrent
rate reaches only 24–38% of its matched native control. The 8 KiB cold get-many
case instead exceeds native: 2.40× on TCP and 3.18× on EFA. That result combines
five storage hosts with different mapped/native versus remote record-read paths;
it does not isolate a network or drive-count speedup.

### 13.4 Writes, mixed workloads and scans

| Value | Write metric | Native, TCP control | TCP | Native, EFA control | EFA |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 KiB | Core throughput, thousand mutations/s | 176.8 | 149.1 | 175.0 | 161.0 |
| 8 KiB | Core throughput, thousand mutations/s | 44.2 | 42.3 | 44.5 | 44.0 |
| 1 KiB | Core 1,024-mutation window p50, ms | 5.654 | 6.319 | 5.754 | 5.953 |
| 8 KiB | Core 1,024-mutation window p50, ms | 22.961 | 22.614 | 23.170 | 22.239 |
| 1 KiB | Single durable write p50, µs | 74.062 | 499.191 | 80.551 | 472.680 |
| 8 KiB | Single durable write p50, µs | 154.663 | 659.924 | 151.923 | 601.893 |
| 1 KiB | Single durable write p99, µs | 278.306 | 2,741.779 | 275.236 | 2,442.960 |
| 8 KiB | Single durable write p99, µs | 421.500 | 2,854.108 | 427.639 | 2,376.630 |
| 1 KiB | Single-write throughput, mutations/s | 10,498 | 1,806 | 9,690 | 1,906 |
| 8 KiB | Single-write throughput, mutations/s | 5,341 | 1,438 | 5,307 | 1,562 |

EFA batch write throughput reaches 92%/99% of matched native at 1/8 KiB; TCP
reaches 84%/96%. Baseline-to-current batch throughput improves 4–5% on TCP and
5–7% on EFA. Single durable writes remain much slower: EFA reaches only 20–29%
of native throughput. Batch throughput parity is not scalar latency parity.

Mixed component rates and scans, keys or mutations per second:

| Shape / value | Metric | Native, TCP control | TCP | Native, EFA control | EFA |
| --- | --- | ---: | ---: | ---: | ---: |
| Core / 1 KiB | Each mixed component | 157,509 | 4,004 | 156,869 | 4,783 |
| Core / 8 KiB | Each mixed component | 15,621 | 2,670 | 14,948 | 3,216 |
| Scalar / 1 KiB | Each mixed component | 7,630 | 1,033 | 6,985 | 1,240 |
| Scalar / 8 KiB | Each mixed component | 4,804 | 905 | 4,800 | 967 |
| Core / 1 KiB | Full scan | 628,005 | 216,996 | 636,329 | 249,650 |
| Core / 8 KiB | Full scan | 5,544 | 66,044 | 5,521 | 73,169 |
| Scalar / 1 KiB | Full scan | 661,143 | 103,135 | 668,184 | 105,118 |
| Scalar / 8 KiB | Full scan | 426,472 | 43,065 | 407,499 | 43,310 |

Mixed rates improved in all four shapes, but remain far below native. The large
8 KiB core scan advantage is not general: smaller scalar-family scans reach
only 10–16% of native. No phase-specific scan I/O breakdown establishes the cause.
Core and scalar scans return 8,192 and 256 keys respectively.

Regressions must remain visible:

- EFA 1 KiB scalar-family scan rate fell 11.7%; all three paired scan times rose.
- TCP 8 KiB scalar-family warm concurrent rate fell 2.3%, in every paired run.
- TCP 1 KiB core get-many rate fell 4.7%; paired signs were mixed.
- TCP 8 KiB core cold get-many p99 rose 27.2%, with substantial run variation;
  cold concurrent rate fell 2.9%.
- TCP 1 KiB scalar-family concurrent-read p99 rose 11.9% in the median, with
  all paired runs slower. EFA 1 KiB scalar-write throughput fell 0.8%.
- Native control rates also moved despite unchanged native source. Do not
  interpret every ratio-of-medians change as statistically established.

### 13.5 Shutdown, reopen and fresh-process recovery

Lifecycle times in milliseconds. Core rows use batch/window 32; scalar rows use
batch/window 1. Reused transport is a separate current-only same-process phase.

| Shape / value | Metric | Native, TCP control | TCP | Native, EFA control | EFA |
| --- | --- | ---: | ---: | ---: | ---: |
| Core / 1 KiB | Shutdown | 2.41 | 3.41 | 2.52 | 3.33 |
| Core / 8 KiB | Shutdown | 2.01 | 3.75 | 2.00 | 3.47 |
| Scalar / 1 KiB | Shutdown | 1.34 | 3.22 | 1.31 | 2.88 |
| Scalar / 8 KiB | Shutdown | 1.42 | 3.34 | 1.31 | 3.23 |
| Core / 1 KiB | Fresh-transport reopen | 3.21 | 17.44 | 3.30 | 43.48 |
| Core / 8 KiB | Fresh-transport reopen | 2.62 | 18.55 | 2.55 | 44.25 |
| Scalar / 1 KiB | Fresh-transport reopen | 2.37 | 13.81 | 2.44 | 40.18 |
| Scalar / 8 KiB | Fresh-transport reopen | 2.89 | 15.74 | 2.82 | 39.26 |
| Core / 1 KiB | Reused-transport reopen | — | 12.78 | — | 11.93 |
| Core / 8 KiB | Reused-transport reopen | — | 12.37 | — | 11.99 |
| Scalar / 1 KiB | Reused-transport reopen | — | 9.51 | — | 9.24 |
| Scalar / 8 KiB | Reused-transport reopen | — | 8.69 | — | 8.69 |

EFA shutdown fell from roughly 82–87 ms to roughly 3 ms. Its fresh-transport
reopen fell 54–57%, but is still 2.4–2.9× TCP. Reusing live transports removes
most of that EFA/TCP difference; it does not make crash recovery equally fast.
Fresh-transport reopen still occurs in a live process with warmed provider
discovery, unlike a genuinely new recovery process.

For core 8 KiB, stage medians in milliseconds:

| Stage | TCP baseline → current → reused | EFA baseline → current → reused |
| --- | ---: | ---: |
| Control connection setup | 7.318 → 7.028 → 0.998 | 42.512 → 32.790 → 1.074 |
| Read-lane worker setup | 6.484 → 6.866 → 1.121 | 39.956 → 34.408 → 1.169 |
| Checkpoint load | 4.018 → 3.882 → 4.450 | 4.141 → 4.035 → 3.860 |
| Fencing | 4.307 → 4.076 → 3.927 | 4.334 → 4.346 → 3.974 |

Read-lane setup overlaps control setup and recovery. Its later critical-path
join wait is only 0.51–0.70 µs in these current cases. Do not add worker durations
or stage medians to the wall time. The similar checkpoint/fence stages and the
large setup difference locate the residual EFA fresh-open penalty in transport
initialization, not a slower native recovery engine.

Current EFA requested and selected TX/RX queues are 128, MTU is 8,928 and
provider version is 13,500,416 (`206 << 16`, provider ABI 206.0). This is not
`FI_VERSION(2,6)`. A prelaunch provider-version check was corrected to match
libfabric's provider ABI encoding; hardware qualification confirms the intended
geometry actually ran. Endpoint worker sums expose bind/enable, peer setup and
first progress costs, but those overlapping sums are not serial open latency.

Fresh-process recovery uses 8 KiB values, 2,048 checkpointed keys and zero or 128
tail batches. One observation per build/case; no native fresh-process control
was included, so no native/distributed crash-recovery ratio is claimed.

| Transport | Restart scope | Tail batches | Baseline engine open, ms | Current engine open, ms | Current controller restore, s |
| --- | --- | ---: | ---: | ---: | ---: |
| TCP | Coordinator only | 0 | 23.466 | 18.096 | 1.163 |
| TCP | All storage processes | 0 | 23.023 | 16.354 | 2.578 |
| TCP | Coordinator only | 128 | 81.440 | 76.193 | 1.113 |
| TCP | All storage processes | 128 | 86.195 | 78.511 | 2.514 |
| EFA | Coordinator only | 0 | 101.437 | 44.768 | 1.063 |
| EFA | All storage processes | 0 | 98.041 | 45.079 | 2.459 |
| EFA | Coordinator only | 128 | 151.581 | 96.715 | 0.962 |
| EFA | All storage processes | 128 | 152.032 | 102.765 | 2.390 |

The checks validate values, tombstones, complete scans, checkpoint/tail adoption
and exact retry identities. Current seed/reopen records create ten connections
and reuse none. Baseline freshness is established by separate processes and
archived source, not by counters that baseline does not emit. Full restart
requires changed shard PIDs, preserved data directories and fresh readiness.
OS caches are not evicted for these process-recovery cases.

Controller restore includes process/shard orchestration, verification, evidence
work and shutdown; engine open excludes those costs. For current EFA full/tail128,
2,390.147 ms includes 1,378.100 ms node orchestration, 102.765 ms engine open,
783.732 ms verification and 125.550 ms remaining process/evidence/shutdown work.
This sequential timer partition is not a network-time measurement.

Replay itself did not improve in that case: baseline/current tail replay was
45.715/47.178 ms. The recovery win is primarily startup reduction, not a new
parallel replay speedup. Its exact raw sources are
`results/transport-recovery-full-t128-efa-{baseline,current}-r0-recovery/`.

### 13.6 Remaining bottlenecks and decisions

Focused diagnostics use a separate single repetition, 256 reads per state.
The following are **nested mean spans**, in microseconds, not additive budgets:

| 8 KiB diagnostic state | Transport | Whole call | Exchange | Server handler | Server record read |
| --- | --- | ---: | ---: | ---: | ---: |
| Cache-evicted | TCP | 459.80 | 458.42 | 179.10 | 176.42 |
| Cache-evicted | EFA | 391.81 | 390.50 | 172.83 | 171.48 |
| Reopened, explicit cache cleared | TCP | 237.03 | 235.87 | 29.47 | 27.74 |
| Reopened, explicit cache cleared | EFA | 196.63 | 195.50 | 23.46 | 22.74 |

1. **Warm remote scalar reads remain the largest latency gap.** Index lookup,
   encode and decode mean spans are sub-microsecond. The exchange occupies almost
   the whole caller interval. Scalar reads now execute inline: actor queue,
   service and return spans are zero, unlike the baseline actor path. Removing
   that handoff helped, but did not eliminate the remaining transport/service
   cost. Exchange includes remote service, scheduling and transport work; it
   must not be relabeled as pure wire latency.
2. **Actual storage work matters for cold reads.** Server record-read means are
   around 171–176 µs in the 8 KiB evicted diagnostic versus 23–28 µs in its
   reopened/cache-cleared state. Evicted diagnostic phases start with zero WAL
   residency and record positive physical reads on native and distributed paths.
   The node snapshots show zero swap traffic. These measurements do not establish
   NVMe saturation or an SSD miss on every operation.
3. **Single durable writes still pay the remote persistence path per operation.**
   The scalar load performs 320 append groups and 320 persistence calls for 320
   mutations. Its storage-wait span accounts for most phase time. Core loads
   instead produce 50 groups at 1 KiB or 121 at 8 KiB for 10,240 mutations.
   Batching amortizes work, which explains why high-throughput writes can approach
   native while single durable writes remain slow. Do not remove the durability
   barrier to improve the comparison.
4. **EFA cold initialization, not steady-state reads, is the remaining EFA/TCP
   regression of greatest interest.** Reuse is useful within a live process;
   fresh recovery cannot reuse a dead process's transports. Startup overlap has
   removed the read-lane setup from the critical path, but cold endpoint setup
   still dominates much of EFA open time.
5. **Attribution is incomplete.** Legacy socket/header/body/drain fields are
   unpopulated in both revisions. Their zeros are not zero-cost measurements.
   EFA traffic evidence includes overlapping NIC counter names, so its aggregate
   is an activity check, not exact unique wire bytes or network amplification.
   Whole-case disk counters include startup/recovery/idle intervals; they cannot
   establish per-operation drive utilization.

**Next engineering priorities, not changes made by this campaign:**

- Decompose the remaining remote exchange into reactor queue, node admission,
  worker queue, request/response progress and completion delivery. Keep clock
  domains explicit. Optimize a measured fixed cost rather than assume the wire
  explains the full interval.
- Preserve bounded caches and long-lived connections for locality/reuse when the
  workload permits them. A coordinator hit can avoid the remote exchange; a
  shard-side cache still requires it. This cache-disabled campaign does not
  measure the hit-rate/capacity curves or establish production cache sizing.
- Keep write batching/group commit for throughput without weakening the durable
  acknowledgment contract. Evaluate scalar durability and its p99 separately.
- Further EFA startup work must target the measured endpoint initialization
  stages. Compare fresh-process startup separately from warm-provider reopen.
- A global WAL or NVMe-oF redesign is **not justified by these timings alone**.
  It would not automatically remove the remote request/completion overhead that
  dominates warm scalar calls. No evidence here supports a promise of native
  RAM-hit latency for arbitrary remote cache misses.
- Correct the inherited read-window `sample_unit` label before the next build.
  Do not rewrite old raw evidence to make the label appear historically correct.

Hardware acceptance still excludes power loss/host reboot, replica failover,
EFA control-loss/deadline fault injection, exhaustive maximum-frame/four-slot
boundary qualification, true concurrent mixed workloads, and sustained
larger-than-RAM/cache-pressure scaling. The planned 124-case performance matrix
is complete; those are distinct unmeasured properties, not silent passing gates.
No installed `kvstore` skill documents were made stale. The benchmark README and
this dated plan now record the actual hardware outcome and remaining limits.

## 14. Local polling and resident-read follow-through, 12 October 2026

**Status: implemented and exercised locally; not hardware-EFA qualified.** No AWS
resources were launched for this work. Native engine and Sui code are unchanged.
The warm remote/native gap is not closed. The new policies are explicit options,
not unconditionally enabled performance claims.

### 14.1 Locks, setup, transports, and security

- A sleep-free established network progress path is possible without making the
  whole database formally lock-free. Native Tidehunter already uses mutexes and
  reader/writer locks: `db.rs`, `large_table.rs`, `wal/tracker.rs`, and WAL mapping
  and maintenance paths. A local ownership boundary is preferable to replacing
  every lock with a shared atomic structure without contention evidence.
- Endpoint creation, registration, TLS setup, and peer discovery are amortized
  across persistent sessions. They matter for startup, recovery, churn, and
  scaling; they do not explain steady-state warm-read latency by themselves.
  Section 13 separates fresh EFA reopen from reused-transport reopen.
- InfiniBand, RoCE, and iWARP are other RDMA transport families, not setup-free
  replacements. Queue pairs/endpoints, registrations, completion processing,
  resource limits, and ownership still need a design. See
  [Linux RDMA connection management](https://man7.org/linux/man-pages/man7/rdma_cm.7.html).
  The [2017 Mellanox comparison](https://network.nvidia.com/pdf/whitepapers/WP_RoCE_vs_iWARP.pdf)
  describes RoCE and iWARP protocol stacks; its vendor benchmarks and deployment
  claims are not evidence about current hardware or this implementation.
- Native `crc.rs` frames encoded WAL values with length and CRC; `wal/mod.rs`
  copies those frames into mapped WAL storage. That engine path does not encrypt
  values. The distributed layer uses rustls to protect a new host-to-host
  boundary, not to encrypt the local WAL. CRC is not peer authentication.
  “Same as native” does not define who may impersonate a remote storage peer.
  This change therefore retains TLS, identity, reference/generation validation,
  and record integrity checks. TLS has not been isolated as the dominant cost.
- AWS infrastructure encryption is separate from both engine CRC and
  application TLS. [EC2 data protection](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/data-protection.html)
  documents mandatory NVMe instance-store encryption and conditional automatic
  inter-instance network encryption. Those features do not replace application
  peer authorization or shard-epoch checks.

### 14.2 Implemented interfaces and source mapping

| Source file | Change and invariant |
|---|---|
| `tidehunter/src/distributed/tcp.rs`, `tcp/inventory.rs` | `PollingPolicy::{Adaptive, Busy}` in `TcpLimits`; authenticated status advertises the server policy. `NodeConfig.read_resident_first` is opt-in. Wire protocol/ALPN is v5; do not mix old v4 binaries with this build. |
| `tcp/reactor.rs`, `remote_db/pending_read.rs` | Busy established-session progress has no deliberate sleep or scheduler yield. Deadlines, bounded sweeps, shutdown, and peer health checks remain. Admission, storage, background workers, and the idle listener can still wait. |
| `tcp/diagnostics.rs` | Opt-in server sweep, sleep count/time, yield, spin, and first-observed-frame polling-gap counters. A polling gap is not a packet-arrival timestamp or pure network latency. |
| `tcp/read_executor.rs`, `tcp/read_executor/cache.rs` | Immediate owned cache hits before unused device/SQ admission. Optional `RWF_NOWAIT` attempt; at most one forced-async fallback, retaining the same resource credits and buffer. |
| `tcp/reactor_tests.rs`, `tcp/reactor_output_tests.rs`, `tcp/read_executor/tests.rs`, `tcp/tests.rs`, `remote_db/pipeline_tests.rs` | Payload/frame limits, session isolation, deadlines, cancellation, output ownership, software-fabric behavior, resident fallback and partial-read correctness. |
| `tidehunter/examples/sharded_wal_transport_bench.rs` | New persistent authenticated RAM-response floor, with deterministic full-payload validation inside each API timer and zero WAL-member-read assertions. Local nodes are owned and stopped; external nodes remain operator-owned. |
| `tidehunter/examples/sharded_wal_bench.rs` | `--tcp-polling adaptive\|busy`, `--tcp-resident-first true\|false`, resident/async read counters, and corrected per-operation concurrent-read sample labels. Historical raw files are unchanged. |
| `tidehunter/examples/support/tcp_cluster.rs`, `sharded_wal_cluster_check.rs` | Propagate policies into owned/prepared nodes. External preparation writes `shape.yaml` beside `remote.yaml`, so the floor program can use the actual prepared cluster. |
| `tidehunter/Cargo.toml` | Register the floor example behind existing experimental-distributed and test-utils features. No new dependency. |

Paths abbreviated as `tcp/...` and `remote_db/...` are under
`tidehunter/src/distributed/`. Test-only hooks are compile-gated rather than
retained as dead production code.

**Defaults remain Adaptive polling and forced-async file reads.** Busy consumes
CPU per open session, not merely per machine. The idle connection listener still
sleeps because removing its sleep would burn another core without improving an
established read. Normal fixed deadlines and error handling remain active.

The RAM probe is test-utils-only, per-session, bounded to 64 KiB and negotiated
frame capacity. It retains an immutable response owner; it does not allocate or
read a WAL value for each reply. This isolates the authenticated exchange,
framing, decode, and client payload validation from index and storage work.
It does not isolate TLS cipher time or the physical network.

### 14.3 Resident-first correctness and source rationale

[eRPC, NSDI 2019](https://www.usenix.org/system/files/nsdi19-kalia.pdf),
sections 3.1–3.2 and 4.2–4.3, motivates short owner-thread handlers, offloading long
work, and retaining buffers through completion. It does not justify performing a
potentially cold blocking WAL read on the network reactor or removing durability.

The [Linux `RWF_NOWAIT` contract](https://man7.org/linux/man-pages/man2/readv.2.html)
permits a positive partial prefix. The first local cold-read experiment exposed
that case being rejected as corruption. A regression failed before the fix and
passed after it. The implemented state transitions are:

1. A full resident result publishes the complete owned record.
2. `EAGAIN` or unsupported NOWAIT behavior retries once through forced async I/O.
3. A positive partial prefix remains initialized in the same buffer. Only the
   missing suffix is submitted asynchronously, at matching file/buffer offsets.
4. A zero read, real error, or incomplete final suffix remains an error. No partial
   record is published or cached.
5. Buffer, file/log owner, request generation, cancellation state, byte/device/SQ
   accounting, and final record validation remain live across the transition.

The [Linux 6.17 io_uring implementation](https://github.com/torvalds/linux/blob/v6.17/io_uring/rw.c)
can normalize unsupported attempts to `EAGAIN`. Thus zero
`resident_read_unsupported` events does not prove universal filesystem support.
`wal_reads` counts logical member reads; resident attempts and async submissions
count file-read submissions, not physical NVMe operations.

This removes a redundant prefix read on partial fallback and avoids cold blocking
work. It does not establish a universal latency improvement. Framing and verification
remain enabled: section 13's measured framing costs do not justify sacrificing
integrity, and no isolated cipher measurement here justifies removing TLS.

### 14.4 Local measurements and negative results

Evidence root:
[`logs/transport-floor-local-2026-10-12/`](../../logs/transport-floor-local-2026-10-12/).
Linux 6.17.9, Rust 1.90.0, five processes on one laptop and one SSD. RF1.
No local RDMA device is exposed. The fabric run uses libfabric 2.6.0
`tcp;ofi_rxm` on loopback, **not EFA hardware**.

RAM-response p50 below is the median of three repetition p50s. Continuous cases
use 1,000 requests per value/repetition without profiling. Idle cases use 256
requests, profiling, and an offered 2 ms delay between requests rotating across
five nodes; a given node therefore sees approximately 10 ms idle intervals.
Profiling and offered-load regimes must not be combined as a matched comparison.

| Floor case | Adaptive 1 KiB | Busy 1 KiB | Adaptive 8 KiB | Busy 8 KiB |
|---|---:|---:|---:|---:|
| Continuous TCP, unprofiled | 8.163 µs | 10.809 µs | 16.918 µs | 16.834 µs |
| Continuous software fabric, unprofiled | 10.080 µs | 9.845 µs | 19.923 µs | 19.308 µs |
| Idle TCP, profiled | 92.736 µs | 24.037 µs | 113.628 µs | 41.759 µs |

All floor cases performed zero WAL-member reads. Busy helped idle latency, not
continuous TCP latency. Across the profiled idle run, server counters recorded
145,587 Adaptive sleeps and 2,086,883 yields versus zero Busy sleeps/yields.
Whole process-tree CPU time, including setup and teardown, rose from 2.98 to
16.91 CPU-seconds; wall time was 3.84 versus 3.68 seconds. This is not a free fix.
`idle-polling-counters.json` preserves the summed evidence.

The real-WAL runs use 8 KiB values, 2,048 loaded keys, 512 reads per read phase,
three repetitions, read window 32, write batches 32/window 8, one read lane, and
disabled coordinator/shard caches. Native uses eight concurrent read workers.
Rows report median p50 or median phase throughput, not aggregate percentiles.

| Metric | Old native | Old TCP | Rebuilt native | New default TCP | Resident-first Adaptive | Resident-first Busy |
|---|---:|---:|---:|---:|---:|---:|
| Warm scalar p50 | 1.264 µs | 26.867 µs | 1.196 µs | 33.437 µs | 37.410 µs | 38.313 µs |
| Warm window-32 reads/s | 642,047 | 55,912 | 475,494 | 61,519 | 38,384 | 33,786 |
| Cache-evicted scalar p50 | 42.556 µs | 80.217 µs | 42.480 µs | 81.283 µs | 69.985 µs | 87.135 µs |
| Cache-evicted window-32 reads/s | 66,831 | 70,061 | 70,441 | 66,313 | 73,342 | 33,890 |
| Batched writes/s | 73,557 | 26,976 | 81,177 | 25,199 | 26,229 | 24,930 |
| Fresh reopen p50 | 1.038 ms | 25.244 ms | 0.886 ms | 23.611 ms | 23.399 ms | 26.202 ms |
| Reused-transport reopen p50 | — | 19.239 ms | — | 18.201 ms | 19.463 ms | 21.395 ms |

These observations do not support a general speedup claim. New-default warm
scalar latency was worse than the old binary, while concurrent read throughput
improved; native concurrent throughput also varied substantially. The runs were
not CPU-pinned or interleaved, so these differences do not establish causality.
Resident-first Adaptive helped this cold scalar pass but hurt warm reads and
concurrent warm throughput. Busy hurt the real-WAL workload on this shared host.
Neither experimental policy is made the default.

Old binaries are frozen under
`logs/scalar-transport-aws-2026-10-11/current-efa-verified/`; the old benchmark
SHA-256 is `7f692391ec6b6acbc5c12c31cb64878764c0ef96d98d82e1b0dfb6a67594db2c`.
`comparison.json` maps source run directories to the table. `wal-final-adaptive`
and `wal-final-busy` used resident-first before it became opt-in; they are not the
new default. `wal-default-control` exercises the actual final default.
`final-mode-binary-sha256.json` identifies the final executables. Earlier floor
receipts identify their earlier build; no measurements were relabeled.

For resident-first Adaptive, each warm read phase completed all 1,536 logical
reads with resident successes and zero forced-async submissions. The cold scalar
phase had 177 resident successes, 1,359 fallbacks, and 117 valid partial-prefix
fallbacks. A final explicit `--tcp-resident-first true` run verified 512 reopened
keys, 128/128 warm resident successes, and 118/128 cold fallbacks including 17
partial prefixes. A separate 16 MiB-per-shard cache control produced 512 warm
cache hits and zero WAL reads, but did not improve scalar latency. Avoiding disk
work is therefore not sufficient by itself on this topology.

All successful WAL runs verified every reopened key. Observed cluster shutdown
was approximately 20–26 seconds per case, also present with the old binary; the
cause was not isolated and is not charged to API latency. The failed initial
partial-read run and rejected invalid benchmark-option runs remain preserved,
not counted as completed measurements.

### 14.5 What remains hardware-only

The [pinned libfabric provider comparison](https://raw.githubusercontent.com/ofiwg/libfabric/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/docs/efa_fabric_comparison.md)
and [EFA provider manual](https://raw.githubusercontent.com/ofiwg/libfabric/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md)
explain why normal `efa` and `efa-direct` are different programming contracts.
Direct mode removes provider matching/protocol work but does not provide the
same message-ordering contract, has device-MTU message limits, and requires
different context/registration handling. Current ordered 64 KiB TLS chunks
cannot be moved to it by changing only a provider name.

The next justified hardware comparison is the same persistent RAM probe on
actual TCP and EFA, with dedicated CPU capacity, Adaptive/Busy, profiled/unprofiled,
continuous/idle, and both sizes. Then run real WAL reads with resident-first
off/on and prove cache, resident, and cold paths using the counters. Keep fresh
setup separate from stable sessions. This is an experiment definition, not an
AWS launch or a hardware-EFA performance claim.

Only if normal EFA's persistent authenticated RAM floor remains inadequate should
the next transport cutover target `efa-direct` or one-sided registered RAM.
One-sided reads require authorized ranges, generation and publication checks,
pinned lifetimes, revocation, and integrity; a remote SSD miss still needs a
storage service. NVMe-oF does not remove this warm RAM-response exchange.
The working authenticated TCP path remains available. No evidence from this
laptop establishes arbitrary remote reads at native RAM-hit latency.

### 14.6 Verification and closure

- Full library suite with experimental-fabric/test-utils: 636 passed, 11 ignored.
  The ignored software-fabric RAM-probe test was then explicitly run and passed.
- Strict Clippy passed for lib/examples with experimental-fabric/test-utils and
  for the production experimental-distributed library; formatting check passed.
- All release examples rebuilt. A stale compiled local fabric dependency was
  removed with package-scoped `cargo clean` and rebuilt; no source workaround.
  The recovery example here is the ordinary build, not a claim of rebuilding a
  separate `checkpoint_recovery_stats` instrumented variant.
- External preparation plus the separate floor executable was exercised against
  five locally owned nodes using prepared configs and authenticated Busy policy.
  All five exited successfully. Temporary configs and TLS material were removed.
- No benchmark storage-node process remained. Host swap use was zero before and
  after; swap-in and swap-out counter deltas were both zero. Receipts:
  `local-nodes-closed.json`, `external-cleanup.json`, and `host-after.json`.
- No power-loss, host-loss, physical RDMA, or separate-NVMe scaling claim is made.
  No installed `kvstore` skill documents were made stale.

## 15. Completed separate-NVMe TCP/EFA campaign, 12 October 2026

This section records the hardware experiment proposed in §14.5. It supersedes
that section's pending-hardware status, not its correctness requirements.
**All-metric parity was not reached. EFA is faster than TCP on the measured
warm reads. Resident-first removes a large server read-dispatch cost, but the
persistent authenticated RAM-response path still has a large latency floor.**
Busy polling is not a general write-performance improvement.

### 15.1 Execution, provenance, and closure

The completed run is `th-0c5bc7b99c80f62d`: six `c6id.32xlarge` hosts in
`us-east-1a`, spread placement, one coordinator/native host and five separate
storage hosts. Each used one local instance-store NVMe SSD. Both frozen code
revisions and their native controls were rebuilt and measured on this fleet.
G6 capacity requests in all five offered zones failed without creating instances;
the new C6id plan and bundle were not substituted into a failed run's identity.
Absolute times must not be compared directly with archived G6 measurements.

- **216/216 declared API cases and 820/820 node archives passed validation.**
  Hardware EFA was qualified; these are not software-fabric or TCP-emulated EFA
  results. All 164 prepared five-shard configurations were covered.
- Automatic termination was requested for all six instances, including the
  coordinator/launcher, at **13:38:07 UTC**; all were verified terminated at
  **13:42:36 UTC**. Independent cleanup verification at **13:58:50 UTC** found
  zero active run-owned instances, EBS volumes, ENIs, EIPs, buckets, roles,
  instance profiles, security groups, or placement groups across all six attempts.
  Protected unrelated instances remained unchanged.
- Provisioning, launch, monitoring, and result/build retrieval used the official
  AWS MCP proxy. After two proxy timeouts during final non-compute cleanup, the
  permitted local SDK fallback performed only cleanup and its absence audit.
- Launch-to-termination-request timing implies **$27.403264 compute**; the
  conservative bound through the termination observation is **$30.306027**.
  Both exclude root storage, public IPv4, S3, and transfer, and are not invoices.
  The launch plan reserved $42, with a one-hour expiry and ten-minute export
  reserve. Failed capacity requests created no compute instances.
- Local gates: 111 runner tests completed, two skipped, no failures; rebuilt
  native and TCP runs each verified 10,240 reopened keys. The RAM probe and
  checkpoint-tail recovery ran locally through the same validators before upload.
  This turn changed the Python campaign harness and documentation, not native
  Tidehunter, Sui, or the previously frozen distributed Rust candidate.

Source and executable identities:

| Artifact | SHA-256 |
|---|---|
| Baseline source | `2e694cb0be9fe0738c217b75afb9fe0ba5d2eef170573bd95f092fa40e93dc57` |
| Current source | `4facb657c3b84720991cd02f18a97b3a53934e3edd1c8b49a5c139616d582239` |
| Final uploaded bundle | `8851d77c2c8c58d9ff7a84aa51d04ffbeb391646a39dba88a0d83d42c31f1c15` |
| Cloud-built baseline main executable | `7fee606349225eda28fdcfdfb6c991ce2580e753bb55c7683a5172f18fec0cec` |
| Cloud-built current main executable | `0adf5519be66bc3a3f87b341b4a7590c19a87431976abe5f41fed24cba2cecd2` |
| Downloaded controller archive | `7e6387e2e1602dfd0328b542d5cd52675fefa296b98a7f7f45cbacdc6c589562` |

The controller archive is 62,279,810 bytes. Both actual cloud-built main
executables were also downloaded, together with their build/source receipts;
local-build binaries were not relabeled as cloud-build binaries. The full
auxiliary executable and libfabric identities remain in those receipts.
Evidence is local because the temporary S3 bucket has been deleted:

- [Complete raw results](../../logs/transport-floor-aws-2026-10-12/run-c6id-us-east-1a/extracted/results/)
- [All comparison fields and repetitions](../../logs/transport-floor-aws-2026-10-12/analysis-c6id/comparison.csv)
- [Compact comparison of all 27 API phases](../../logs/transport-floor-aws-2026-10-12/analysis-c6id/api-phase-comparison.csv)
- [Observed regression ledger](../../logs/transport-floor-aws-2026-10-12/analysis-c6id/regression-ledger.csv)
- [Full analysis, recovery, and diagnostic counters](../../logs/transport-floor-aws-2026-10-12/analysis-c6id/analysis.json)
- [820 node I/O/resource records](../../logs/transport-floor-aws-2026-10-12/analysis-c6id/node_io.csv)
- [Downloaded cloud executables and receipts](../../logs/transport-floor-aws-2026-10-12/cloud-built-c6id/)
- [Final independent closure receipt](../../logs/transport-floor-aws-2026-10-12/final-closure.json)
- [Compute estimate](../../logs/transport-floor-aws-2026-10-12/c6id-compute-cost.json)
- [Capacity-attempt ledger](../../logs/transport-floor-aws-2026-10-12/attempt-ledger.json)

### 15.2 What the measurements mean

Main results are medians of three run-level measurements, not pooled percentiles.
Diagnostic and fresh-process recovery shapes have one repetition. The core shape
loads 10,240 values with batch 32/window 32; the scalar-write shape loads 320 with
batch 1/window 1. Read phases perform 4,096 logical reads, with 32-key get-many
or window 32 where stated. Explicit value-cache hits were zero in the reported
WAL read families. Warm reads can still use the Linux page cache.

Native uses the matched synchronous Value-WAL file/directory durability barrier.
Native controls were actually measured alongside each transport's default cases.
Other current policies reference those exact controls. The two native columns
below are separate measured control medians, not two native network transports.
Native code is unchanged, but its measurements vary; there is no synthetic common
denominator. RF1 is used: this is not Sui, replicated durability, NVMe-oF,
one-sided RDMA, or a power-loss/host-loss qualification.

The next tables use **one fixed read-latency candidate: Busy + resident-first**,
for both sizes and transports. They do not select a different fastest policy for
each metric. This is opt-in; the default remains Adaptive + resident-first off.

| 1 KiB metric | Native, TCP control | TCP candidate | Native, EFA control | EFA candidate |
|---|---:|---:|---:|---:|
| Durable scalar write p50, µs | 48.128 | 1,195.012 | 71.096 | 906.040 |
| Batched durable writes, kvalues/s | 235.908 | 153.864 | 219.260 | 168.788 |
| Warm scalar read p50, µs | 0.947 | 293.281 | 0.940 | 71.248 |
| Warm scalar read p99, µs | 1.603 | 602.586 | 1.570 | 410.385 |
| Warm get-many 32, kreads/s | 1,120.081 | 43.346 | 1,130.365 | 72.273 |
| Warm window 32, kreads/s | 71.339 | 46.846 | 71.424 | 59.331 |
| Cache-evicted window 32, kreads/s | 65.009 | 35.753 | 63.658 | 47.911 |
| Full scan, ms | 14.349 | 29.281 | 13.858 | 24.648 |
| Fresh-transport engine reopen, ms | 2.180 | 21.874 | 2.284 | 53.877 |
| Reused-transport engine reopen, ms | — | 18.143 | — | 15.154 |

| 8 KiB metric | Native, TCP control | TCP candidate | Native, EFA control | EFA candidate |
|---|---:|---:|---:|---:|
| Durable scalar write p50, µs | 268.217 | 1,220.655 | 250.497 | 1,048.245 |
| Batched durable writes, kvalues/s | 73.953 | 43.068 | 71.559 | 45.865 |
| Warm scalar read p50, µs | 2.884 | 332.801 | 2.936 | 89.073 |
| Warm scalar read p99, µs | 3.983 | 621.157 | 4.148 | 420.768 |
| Warm get-many 32, kreads/s | 303.774 | 37.200 | 299.117 | 56.353 |
| Warm window 32, kreads/s | 68.431 | 43.745 | 70.210 | 50.120 |
| Cache-evicted window 32, kreads/s | 35.611 | 33.287 | 35.577 | 44.273 |
| Full scan, ms | 739.441 | 99.998 | 755.458 | 103.561 |
| Fresh-transport engine reopen, ms | 2.231 | 22.774 | 1.970 | 53.068 |
| Reused-transport engine reopen, ms | — | 16.194 | — | 15.346 |

The compact CSV also covers open, hot-key reads, misses, existence, mixed
read/write traffic, deletes, checkpoint-handle capture/get/intervening write,
index flush, post-flush reads, reopen audits, and shutdown. Rates count logical
values/reads, not batch calls. A core write latency sample covers up to 1,024
mutations; dividing it by 1,024 does not yield per-mutation latency. Mixed read
and write rates share the complete mixed-phase wall interval. Single-sample
maintenance operations do not have meaningful p99 values. Checkpoint capture
times a handle capture, not complete durable checkpoint creation.

### 15.3 The resident path works; Busy has substantial tradeoffs

Core warm scalar p50, µs:

| Transport/value | Baseline Adaptive/off | Current Adaptive/off | Current Adaptive/on | Current Busy/off | Current Busy/on |
|---|---:|---:|---:|---:|---:|
| TCP, 1 KiB | 479.551 | 465.292 | 352.868 | 407.352 | 293.281 |
| TCP, 8 KiB | 492.413 | 518.274 | 333.076 | 477.252 | 332.801 |
| EFA, 1 KiB | 233.891 | 233.328 | 98.994 | 188.921 | 71.248 |
| EFA, 8 KiB | 250.786 | 251.068 | 127.405 | 208.545 | 89.073 |

Busy/on lowers EFA p50 by 69.5%/64.5% versus the same-fleet baseline, and TCP by
38.8%/32.4%. It does not reach native: EFA remains about 76×/30× slower for this
core scalar-read shape. Ratios differ with working set; the smaller scalar-write
shape has still faster native reads and roughly 145×/91× EFA gaps.

All 16 resident-enabled warm scalar configurations, in all three repetitions,
recorded 4,096 NOWAIT attempts, 4,096 successes, zero fallbacks, and zero forced
async submissions. Resident-off warm phases submitted 4,096 async reads instead.
Thus the improvement does not depend on an explicit shard value-cache hit.
Cold scalar median counts were 2,095 successes/2,001 fallbacks for 1 KiB and
716/3,380 for 8 KiB. Partial-prefix reads were subsets of those fallbacks
(172/526); unsupported-operation counts were zero. These exercise the suffix-only
fallback correctness rule from §14, not a special all-resident path.

Keep the negative results:

- Busy/on EFA scalar-write p99 worsened 20.4–25.1% versus baseline; its scalar
  write throughput fell for both sizes. Its 8 KiB write p50 rose from 900.767 to
  1,048.245 µs. Busy is not a general durable-write optimization.
- Busy/on EFA 8 KiB warm-window p99 rose from 1,110.766 to 1,308.977 µs (+17.8%).
  Its 50.120 kreads/s is below current-default 51.278, although above baseline
  49.155. Faster scalar p50 did not imply better concurrent tails.
- Default EFA core load throughput changed −6.62%/−0.12%, and delete throughput
  −12.26%/−5.23%. Default TCP core load improved +4.42%/+2.52%, but its 1 KiB
  mixed-phase rate fell 6.48%.
- Default TCP had read regressions too: core 1 KiB scalar p99 +16.37% and get-many
  throughput −12.17%; core 8 KiB get-many p99 +10.68%, cold get-many p99 +16.24%,
  and cold get-many throughput −6.19%. Smaller-shape 8 KiB scalar p99 rose 11.74%.
- Flush, shutdown, checkpoint-intervening writes, and recovery setup did not
  improve uniformly. The linked ledger retains all 212 main-summary comparisons
  with >10% worse p50/p95/p99/wall time or >10% lower logical throughput, across
  all policies. These are observed comparisons, not 212 established code defects;
  small single-sample timers and variable controls need that distinction.

Busy consumed more CPU. In the diagnostic interval from loaded state through the
end of warm-window reads, all-six-process CPU seconds for resident-enabled EFA
rose Adaptive→Busy from 3.800→7.804 at 1 KiB and 4.113→9.203 at 8 KiB; TCP rose
6.257→18.212 and 6.687→17.395. This interval includes earlier scalar phases, not
only the window phase. It is not a per-read CPU/wait percentage.
No controller or node swap-in/out was observed; all 820 node records reported
zero VmSwap, and maximum node VmHWM was about 62.65 MiB.

### 15.4 The zero-WAL response floor is still too high

Established, authenticated RAM-response sessions, with full payload verification
inside API timing and **zero WAL reads**:

| Probe/transport/value | Adaptive p50/p99, µs | Busy p50/p99, µs |
|---|---:|---:|
| Continuous, TCP 1 KiB | 293.930 / 628.146 | 321.676 / 526.940 |
| Continuous, TCP 8 KiB | 327.415 / 638.396 | 350.060 / 564.546 |
| Continuous, EFA 1 KiB | 64.884 / 403.592 | 65.211 / 404.277 |
| Continuous, EFA 8 KiB | 84.398 / 410.590 | 84.716 / 410.404 |
| Idle 2 ms, TCP 1 KiB | 412.026 / 664.640 | 385.112 / 686.445 |
| Idle 2 ms, TCP 8 KiB | 472.687 / 732.325 | 451.892 / 733.380 |
| Idle 2 ms, EFA 1 KiB | 202.360 / 501.157 | 123.657 / 449.833 |
| Idle 2 ms, EFA 8 KiB | 259.665 / 557.394 | 181.490 / 511.794 |

Continuous probes are unprofiled, 1,000 samples per size/repetition. Idle probes
are profiled, 256 samples; idle delay is outside API latency but inside wall
throughput. Therefore continuous-to-idle differences cannot be attributed solely
to idle delay. Each probe warms and rotates five persistent sessions.
Busy recorded zero deliberate idle sleeps and scheduler yields in the measured
polling path; Adaptive had roughly 26,000–29,000 sleep calls in the corresponding
aggregate counters. This does not prove that the entire engine/provider/OS is
lock-free or never sleeps.

This is a floor of **our authenticated software path**, not bare EFA/RDMA wire
latency or a hardware lower bound. Busy does not improve the continuous EFA
floor; it helps after idle. Removing WAL work alone still leaves 65–85 µs p50,
which is far above native warm reads. Independent medians must not be subtracted
to manufacture exact WAL, wire, or scheduling costs.

### 15.5 Timing attribution: read dispatch improved, exchange dominates

Separate one-repetition warm diagnostics; all entries are **means in µs**, not
the unprofiled p50s above. A0 means Adaptive/resident-off; B1 means Busy/on.

| Transport/value/policy | Whole API | Client exchange | Node read handler | Client record verification |
|---|---:|---:|---:|---:|
| TCP 1 KiB A0 | 509.546 | 508.210 | 115.759 | 2.141 |
| TCP 1 KiB B1 | 341.647 | 339.825 | 3.380 | 1.361 |
| TCP 8 KiB A0 | 501.577 | 500.162 | 114.306 | 9.664 |
| TCP 8 KiB B1 | 314.910 | 312.170 | 4.294 | 7.893 |
| EFA 1 KiB A0 | 252.949 | 251.865 | 99.680 | 1.341 |
| EFA 1 KiB B1 | 135.841 | 133.363 | 2.785 | 1.313 |
| EFA 8 KiB A0 | 273.537 | 272.270 | 100.616 | 7.889 |
| EFA 8 KiB B1 | 153.109 | 150.250 | 3.396 | 7.856 |

The resident path removes roughly 100 µs of handler residence, including async
dispatch/waiting; this is not 100 µs of measured SSD device time. The exchange
span contains about 99% of API wall time, but includes transport, framing,
authentication, queues, remote service, scheduling, and verification. It is not
pure network propagation. These clocks are nested: do not add them together.
EFA queued-response-to-send-retirement means remain roughly 125–136 µs versus
5–8 µs for TCP, but retirement may occur after the client sees the response.
That counter alone cannot establish a slower EFA critical path.

Coordinator metadata-only paths remain sub-microsecond: candidate existence p50
is about 0.28–0.32 µs and missing-get about 0.39–0.46 µs. They do not explain the
hundreds-of-microseconds remote-value gap.

Source timing boundaries:

- [`native_reads.rs`](../../tidehunter/examples/support/native_reads.rs), lines
  32–99: native window dispatch, worker channels, and retirement.
- [`read_campaign.rs`](../../tidehunter/examples/support/read_campaign.rs),
  lines 115–143: cache-eviction/read phase boundaries.
- [`sharded_wal_transport_bench.rs`](../../tidehunter/examples/sharded_wal_transport_bench.rs),
  lines 192–234: idle placement and complete RAM-response validation.
- [`tcp.rs`](../../tidehunter/src/distributed/tcp.rs), lines 210–232 and
  1050–1066: read timing scopes.
- [`reactor.rs`](../../tidehunter/src/distributed/tcp/reactor.rs), lines 869–888,
  1539–1574, and 1619–1644: established polling and response lifetime.
- [`read_executor.rs`](../../tidehunter/src/distributed/tcp/read_executor.rs),
  lines 715–743: resident/fallback read handling.

### 15.6 Durable writes have more than a transport bottleneck

Current-default aggregate counters for the complete core load, milliseconds:

| Transport/value | Preparation/validation | Storage wait | Publication | Persistence calls |
|---|---:|---:|---:|---:|
| TCP 1 KiB | 18.871 | 30.937 | 11.331 | 50 |
| TCP 8 KiB | 83.286 | 112.075 | 20.299 | 121 |
| EFA 1 KiB | 21.466 | 27.365 | 12.180 | 50 |
| EFA 8 KiB | 84.661 | 102.985 | 18.417 | 121 |

These are stage counters with nested/parallel work, not additive phase slices.
For example, one current EFA 8 KiB run records preparation wall 82.901 ms but
parallel preparation-task sum 146.795 ms. Neither is pure network time.
The complete scalar load instead performs 320 persistence calls, with storage
wait 269–289 ms on EFA and 340–373 ms on TCP. Remote durable-completion wait is
the dominant scalar-write counter; faster warm reads do not remove that barrier.

Batching reduces persistence calls to 50/121, but leaves material frontend
preparation and publication work. Counts are unchanged versus the baseline for
each shape, so measured changes are not explained by fewer durability barriers.
The calls are distributed over five nodes, not globally serial disk barriers.
One 8 KiB load window also contains an 11.006 ms foreground checkpoint and takes
31.800 ms versus roughly 19–23 ms for typical other windows.
No per-write device-latency or IOPS attribution was measured; do not label the
entire storage-wait span as NVMe time.

Resident-first improves the whole mixed workload: Adaptive/on core mixed-phase
rates rise 23.5–26.3% on EFA and 34.6–36.9% on TCP versus baseline. The mixed
interval includes reads, so this is not evidence of an equally large isolated
durable-write improvement.

### 15.7 Reopen and recovery are distinct measurements

Fresh engine reopen clears the process-owned transport pool while shard processes
survive. Reused reopen creates a new engine handle but retains those sessions.
The fixed candidate's fresh EFA reopen is about 53 ms; reuse lowers it to about
15 ms, near TCP's reused 16–18 ms, but still above native's roughly 2 ms.
Fresh-session setup matters for restart and short-lived engines; it is not paid
on every read of a stable cluster. These are not full service-recovery times.

All 16 fresh-process recovery cases passed checkpoint/tail adoption, complete key
verification, exact retries of every original checkpoint/tail operation, unchanged
version checks, and a second complete verification. Tail 128 adopts exactly 128
batches and 25,372,672 encoded bytes; all shapes verify 2,048 keys. Seed exits
without Drop/shutdown or an extra shutdown checkpoint. Every recovery creates ten
fresh transports and reuses none.

Current recorded times, milliseconds; one repetition per shape:

| Shard-process state/transport/tail batches | Engine open | Tail replay counter | Whole verified service restoration |
|---|---:|---:|---:|
| Surviving / TCP / 0 | 21.219 | 0.356 | 2,115.482 |
| Surviving / TCP / 128 | 86.876 | 50.442 | 1,663.722 |
| Surviving / EFA / 0 | 52.931 | 0.292 | 1,061.563 |
| Surviving / EFA / 128 | 101.200 | 43.685 | 960.660 |
| Restarted / TCP / 0 | 20.527 | 0.220 | 3,602.073 |
| Restarted / TCP / 128 | 85.592 | 52.928 | 3,062.203 |
| Restarted / EFA / 0 | 52.612 | 0.300 | 2,462.931 |
| Restarted / EFA / 128 | 105.163 | 45.374 | 2,373.470 |

“Restarted” means all five shard processes, not EC2 hosts or OS cache eviction.
The timer includes about 1.4 seconds of restart orchestration plus complete
verification and durable-identity retries. The node readiness loop samples at
100 ms, so its approximately 60–70 versus 150–170 ms node times cannot identify a
2× engine startup regression. Tail shapes have different retry/verification work;
subtracting their whole-service times does not isolate replay.
No equivalent native full-process recovery control was run in this matrix.

Recovery speed did not improve uniformly. Full EFA/tail0 engine open rose
47.336→52.612 ms (+11.1%), with connection setup 36.404→40.112 ms; full
EFA/tail128 open rose 2.5%. Surviving TCP whole verified restoration rose about
5.0%/3.1% for tail0/128. Full TCP/tail128 connection setup rose
9.719→11.071 ms, and its read-lane setup 8.060→10.842 ms. The complete analysis
retains the remaining stage and node-local changes. These single observations
are not a recovery latency distribution, and overlapping setup stages do not sum
to engine-open wall time.

The relevant boundaries are
[`sharded_wal_bench.rs`](../../tidehunter/examples/sharded_wal_bench.rs),
lines 2208–2279 and 2405–2425;
[`sharded_wal_recovery_bench.rs`](../../tidehunter/examples/sharded_wal_recovery_bench.rs),
lines 480–510 and 544–550; and
[`guest.py`](../../experiments/aws-sharded-wal/guest.py),
lines 353–421 and 1045–1080.

### 15.8 Interpretation and next decision

1. **Keep the resident-first option; do not make Busy a universal default.**
   Its warm fallback removal is demonstrated, including real partial-read
   fallback correctness. Adaptive/on captures much of the gain with lower CPU
   cost and fewer durable-write tail regressions. Defaults were not changed by
   this experiment.
2. **The next warm-read target is the authenticated transport/software path.**
   Isolate provider progress/completion, byte-stream/TLS/framing, and queue
   handoffs against the existing persistent RAM probe before another WAL design
   change. An `efa-direct` or authorized registered-RAM design is a justified
   controlled experiment, not a demonstrated fix. Preserve §14's authorization,
   generation, lifetime, integrity, revocation, and fallback requirements.
   This campaign does not establish that 1–5 µs remote reads are achievable.
3. **Keep separate write work.** Scalar durability wait, preparation/validation,
   publication, and foreground checkpoint work remain. Transport-only changes
   cannot be credited with removing all these costs. Do not weaken the durability
   contract to make a throughput comparison look better.
4. **Reuse established connections for long-lived engines.** Fresh EFA setup
   remains slower than TCP, while measured tail replay is faster on EFA. Keep
   engine-open, setup, replay, verified restoration, and node orchestration clocks
   separate instead of attributing all reopen delay to EFA network latency.
5. **Do not infer NVMe scaling from warm parity or this small data set.**
   Native window-32 includes worker-channel dispatch and FIFO retirement: its
   roughly 71 kreads/s at 1 KiB contrasts with 1.12 million get-many reads/s and
   sub-microsecond scalar p50. Near-native window throughput is not native-engine
   speed parity. Cache-evicted passes start cold but warm as they run; current
   EFA 8 KiB/r0 did read 37,335,040 physical bytes per pass, but not every request
   was a physical disk read. The data set is not larger than aggregate RAM.
6. **Retain measured distributed wins without inventing their causes.**
   Candidate EFA 8 KiB cache-evicted window throughput exceeds its native control
   by about 24%; the measured 8 KiB scan is about 7.3× faster. The 1 KiB versions
   are slower. No scan-phase device attribution was captured, so the large scan
   win is not proof that all of it came from five NVMe drives.

NVMe-oF or a global WAL would not by itself remove a zero-WAL RAM-response floor.
A bounded coordinator value-cache hit can avoid the remote path, but this matrix
does not remeasure that separate path and cannot assume all data fits in cache.
The result supports targeted transport and write-path work, not a claim of
all-metric parity or a need to replace the local WAL/index correctness model.
No further cluster was launched after this completed campaign. No installed
`kvstore` skill documents were made stale.

## 16. Stage timing implemented; cluster placement run postponed, 12 October 2026

**Status: local preparation complete. AWS launch is not authorized until the user
approves it again. No EC2 instance was launched for this experiment.**

The user postponed launch during provisioning. The empty S3 bucket, security group,
two IAM roles, and two instance profiles created for `th-3e68e59eebdd110d` were
removed through the official AWS MCP. The placement group had not been created.
An independent MCP audit found no run-owned instances, EBS volumes, network
interfaces, security/placement groups, bucket, roles, or instance profiles.
All four protected unrelated instances retained their running state. An all-region
query also found no active `ManagedBy=sharded-wal-benchmark` instances.

The cancellation receipt is
[`CANCELLED-BEFORE-LAUNCH.json`](../../logs/stage-timing-aws-2026-10-12-152740/run-cluster-us-east-1a/CANCELLED-BEFORE-LAUNCH.json).
There are **no new cloud performance results**. Section 15 remains the latest
completed cloud campaign; do not label its spread-placement measurements as
cluster-placement results.

### 16.1 Measurement boundaries and code ownership

No native engine, native WAL implementation, or Sui code changed. The changes
are in the experimental distributed layer, fabric adapter, and benchmark runner.
Diagnostic messages require protocol 6. Baseline protocol-5 peers and current
protocol-6 peers must run as separate, source-matched configurations.

| Boundary | Producer / consumer | Measured scope |
|---|---|---|
| Native read and write | [`sharded_wal_bench.rs`](../../tidehunter/examples/sharded_wal_bench.rs), `profile_native_reads`, existing native write counters | Index versus WAL/decode; batch preparation, commit, and explicit value-WAL barrier. WAL/decode is not device-only time. |
| Coordinator read | [`remote_db.rs`](../../tidehunter/src/distributed/remote_db.rs), `profile_remote_reads` in the benchmark | Existing index, dispatch, wait, actor queue/service/return, inline read, exchange, decode, and record verification. These nested spans must not be added together. |
| Server control and inventory | [`tcp/reactor.rs`](../../tidehunter/src/distributed/tcp/reactor.rs), [`tcp.rs`](../../tidehunter/src/distributed/tcp.rs) | Admission-to-worker queue residence, worker execution, inventory-lock wait/residence, append decode/group construction, and storage calls. Queue residence is not network time. |
| Storage append and durability | [`storage.rs`](../../tidehunter/src/distributed/storage.rs), `StorageWriteProfile`, `append_group_profiled`, `persist_many_profiled` | Append state-lock wait, validation/preparation, inherited WAL append, receipt publication; persistence serialization/state-lock waits, validation, file barrier, required namespace barrier, durable publication. Failed calls, retries, and no-op barriers retain separate counts. |
| Framing and TLS | [`tcp/diagnostics.rs`](../../tidehunter/src/distributed/tcp/diagnostics.rs), [`tcp/reactor.rs`](../../tidehunter/src/distributed/tcp/reactor.rs) | `WorkProfile` elapsed sums, calls, bytes, and errors for encoding/decoding, plaintext I/O/buffer growth, TLS I/O, and record processing. Plaintext write includes encryption; TLS I/O encloses transport work. |
| Fabric provider | [`endpoint.rs`](../../tidehunter-fabric/src/endpoint.rs), [`endpoint/resources.rs`](../../tidehunter-fabric/src/endpoint/resources.rs) | Send/receive copies; post calls and accepted bytes/capacity; backpressure/errors; CQ call, error-read, and completion-dispatch time; all-poll gaps. Post time is not completion residence. Poll gaps can contain idle or off-CPU time. |
| Authenticated RAM floor | `Client::read_ram_probe` in [`tcp.rs`](../../tidehunter/src/distributed/tcp.rs) | Full deterministic payload verification, verification calls, and successfully verified bytes, outside the exchange span. This path must perform zero WAL-member reads. |

New stage clocks are opt-in. `ConnectionProfile.client_transport` is sampled
before the final profile-control request; the server snapshot includes its local
control boundary. They are not synchronized two-sided packet timestamps.
The ordinary latency cases remain separate from instrumented diagnostic cases.
The RAM matrix includes same-source instrumentation-off/on controls to measure
observer overhead.

Storage stage sums are disjoint subsets of their corresponding local totals.
Those totals still nest within server service time. Parallel shard sums are not
request wall time. File-barrier time includes inherited lookup, locking, and sync;
it is not an isolated NVMe measurement. No field establishes pure wire latency,
one-way network delay, or CPU time by subtracting timestamps from different hosts.

### 16.2 Added workload coverage

The main benchmark now records:

- Independent, bounded warm native and remote scalar profiles (`state=warm`).
  Remote sampling clears only the coordinator value cache, not the shard WAL
  page cache. These diagnostic samples are not the ordinary scalar p50.
- Warm windowed-read node profiles, in addition to existing scalar/batched phases.
- Separate node profiles for cache-evicted scalar, get-many, and windowed reads.
  The existing independent cold diagnostic pass and reopened-read profiles remain.
- Native/coordinator/shard process resource snapshots before and after warm scalar,
  windowed, and batched phases. CPU deltas include observation/control overhead;
  they are process intervals, not per-request CPU attribution.
- Timing-schema and overlap metadata. Cold phases still require nonzero observed
  physical-read evidence; advisory eviction alone does not qualify a cold result.

The cloud analyzer rejects missing fields, inconsistent call/outcome counts,
missing diagnostic phases, incorrect sample sequences, and RAM verification
byte/count mismatches. An idle profiled connection may legitimately have zero
work samples; zero samples are not reported as zero latency.

### 16.3 Local evidence and frozen artifacts

All evidence is under
[`logs/stage-timing-aws-2026-10-12-152740/`](../../logs/stage-timing-aws-2026-10-12-152740/).

- 254 distributed Rust tests passed; five separately enabled software-fabric
  tests passed; 29 native-provider tests passed.
- The Python runner suite ran 122 tests with two skips.
- Clippy passed with warnings denied for libraries, examples, and tests.
  The broader all-target check found an unchanged inherited
  `clippy::manual_is_multiple_of` warning at
  `tidehunter/benches/large_table_benchmarks.rs:169`; the same expression exists
  in the preserved baseline. It was not changed as part of instrumentation.
- Ten actual main-program runs passed: four native, four five-shard TCP, and
  two software-fabric runs, including scalar/batched shapes and cold-read evidence.
- Six external five-node RAM-floor runs passed: TCP Adaptive/Busy, profiling
  off/on, and software-fabric Busy off/on. Both payload sizes and three repetitions
  were exercised. All 30 owned external node processes exited with code zero.
- The software-fabric runs were checked as software-provider runs, not falsely
  admitted as hardware EFA. The self-spawned RAM run was not used as proof of the
  external-node contract; the six external-node runs exercised that contract.
- No frozen-binary benchmark processes remained. Observed swap-in and swap-out
  counters were both zero. This laptop still has one shared SSD and no hardware
  EFA qualification; these runs establish behavior and evidence coverage, not
  independent-NVMe speed or placement gains.

Frozen current source:
`0848d53851fba812f5f0d19b636b8ad4f2a032aa8e4217f065ed3cb35c2d548d`.
Main executable:
`611429861d3a24b0cb474e80bfd35c4ba529a86ca75082eacb3ec5c3f1da26e4`.
RAM-floor executable:
`4b79aa6276ae864f98b6350fced6897a4acb98107aa9b0d683c9a5b489abd14a`.
The locally rebuilt source-bound bundle is
[`payload/bundle.tar.gz`](../../logs/stage-timing-aws-2026-10-12-152740/payload/bundle.tar.gz),
SHA-256 `d5261d18cf67c5f7d015a6e08d7ecc7ef0665b2a71d286fceffd7b64ba012b6e`.
The upload phase was not reached.

### 16.4 Deferred cluster-placement experiment

[`stage_timing.py`](../../experiments/aws-sharded-wal/stage_timing.py) defines
68 outputs over 52 five-node lifecycles, requiring 260 node archives:
36 uninstrumented performance outputs, 12 detailed diagnostic outputs, four EFA
qualification outputs, and 16 persistent RAM-floor outputs. There are 96 RAM-floor
summary rows and 78,144 measured RAM requests. Recovery is deliberately not
rerun or cloud-built in this targeted timing profile.

The prepared topology is six `c6id.32xlarge` instances: one coordinator/native
control and five separate shard instances, each using its own local NVMe device.
Native, TCP, and EFA use the same durability contract. Both baseline and current
sources are rebuilt on the fleet and checked against the immutable local source
manifests and uploaded archives before running.

**Cluster placement is an AWS infrastructure choice, not a database or transport
protocol change.** AWS describes cluster placement as packing instances close
together within one AZ; spread placement separates underlying hardware to reduce
correlated failures. It does not turn five shard instances into five processes
on one instance. The runner changes EC2 placement selection and verifies the
actual group, strategy, AZ, instance type, ownership, and EFA interfaces.
See [AWS placement groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html).

The reviewed limit is 1,500 seconds from launch, with 300 seconds reserved for
export and the controller's existing 30-second guard. At the AWS Pricing API's
verified 12 October price of $6.4512 per instance-hour, six hosts for 25 minutes
bound compute at $16.128; the campaign reserve is $18, not a billing receipt.
See [EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/).
No compute was launched under this plan.

After new user approval:

1. Keep the cancelled run closed. Create a new run ID and immutable plan; recheck
   source/binary hashes, AWS access, capacity, pricing, remaining budget, and
   termination guards. Rebuild if any Rust/Cargo input has changed.
2. Run the `stage-timing` profile with the prepared TCP/native/EFA controls and
   zero-WAL RAM floor. Do not silently change the matrix to fit partial results.
3. Retrieve and hash-check all result/log archives, terminate the exact fleet,
   remove run-owned resources, and independently verify closure.
4. Analyze with `analyze_cloud.py --profile stage-timing --spread-results` pointing
   at the preserved previous campaign. Its same-source baseline RAM controls
   match payloads, polling, samples, warmups, repetitions, and five peers.
   The spread-versus-cluster comparison is observational across campaigns, not a
   randomized placement-only experiment. Report that limitation and profiling
   overhead before attributing any residual to the network.

No `efa-direct`, one-sided read, NVMe-oF, encryption-policy, or durability change
is included. Hardware attribution, placement benefit, and current cloud
native/TCP/EFA gaps remain unmeasured. No installed `kvstore` skills were made stale.

## 17. Completed cluster-placement stage timing, 12 October 2026

**Complete and closed.** Run `th-85e6e8a4dd7328b9` measured the full section 16
matrix: 68 API outputs, 52 five-shard lifecycles, and 260 node archives. Local
archive validation passed every expected identity, source binding, correctness
audit, hardware qualification, and required I/O/resource record. No failed or
partial case was substituted for a complete result.

### 17.1 Topology, provenance, and evidence

Six `c6id.32xlarge` instances ran in the exact owned **cluster placement group**
in `us-east-1b`: one coordinator/native control, five separate storage instances,
each using one verified instance-store NVMe device. This remains RF1, with no
replicas and no full Sui workload. Native and distributed acknowledged the
required Value-WAL durability barriers. Application value caches were disabled;
Linux page cache and the resident-first read policy remained in use.

The first allocation in `us-east-1a`, run `th-16570411429c9106`, failed for lack
of capacity. Reconciliation found zero instances, and all preparatory resources
were removed before creating the new immutable east-1b plan.

- Current Rust source tree:
  `0848d53851fba812f5f0d19b636b8ad4f2a032aa8e4217f065ed3cb35c2d548d`.
- Preserved baseline source:
  `4facb657c3b84720991cd02f18a97b3a53934e3edd1c8b49a5c139616d582239`.
- Rebuilt local bundle:
  `a06fa96b687f36bec44cf56dcb655158cf4d2da2a2657effc8922a67b597285a`,
  14,167,294 bytes. Its actual S3 checksum and length matched before launch.
- Cloud main executable, current:
  `b4815c7f8612a801b201ff625302679389cf8b12def020dabd22e9f3b44eb2c0`.
- Cloud main executable, baseline:
  `dc2ffe8df2566b352cb562c66bec7619a6bee9ce94f844dce1a54e8c88db15f8`.
- Retrieved results:
  `39a95915b6d13b0fde5c9ae005c842ca423672194592bd615aba4f12ae3502b6`,
  18,487,986 archive bytes, verified against the downloaded bytes.

Both revisions and their RAM-floor executables were rebuilt from frozen sources
in separate, new cloud target directories with Rust 1.90.0 and
`experimental-fabric,test-utils`. Hardware used libfabric `2.6.0amzn1.0`.
All four EFA qualification cases passed. Baseline protocol 5 and current protocol
6 never shared peers. No Rust, native-engine, Sui, or transport-policy change was
made during this run.

Evidence root:
[`logs/stage-timing-cluster-2026-10-12-192613/`](../../logs/stage-timing-cluster-2026-10-12-192613/).
Important artifacts:

- [`run-us-east-1b/receipt.json`](../../logs/stage-timing-cluster-2026-10-12-192613/run-us-east-1b/receipt.json):
  archive and cloud-build identities.
- [`run-us-east-1b/extracted/results/`](../../logs/stage-timing-cluster-2026-10-12-192613/run-us-east-1b/extracted/results/):
  raw samples, summaries, node archives, source archives, build logs, and receipts.
- [`analysis-cluster/analysis.json`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/analysis.json):
  complete offline validation and explicit stage availability.
- [`analysis-cluster/comparison.csv`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/comparison.csv):
  all 165,984 comparison/counter rows; keep family, shape, and diagnostic labels.
- [`analysis-cluster/api-metric-comparison.csv`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/api-metric-comparison.csv):
  all summary fields with measured native controls where available.
- [`analysis-cluster/headline-current.json`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/headline-current.json),
  [`read-stage-diagnostics.json`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/read-stage-diagnostics.json),
  and [`storage-stage-weighted.json`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/storage-stage-weighted.json):
  the tables below, with original units and denominators.

### 17.2 Current native, TCP, and EFA comparison

Medians of three repetitions of each exact shape. Scalar writes use B1/W1;
other rows use B32/W32. Read-window phases use 32 admitted reads and native's
32-worker adapter. Both transports reference the same actually measured native
shape/repetition, not extra native runs. Rates count logical mutations or values,
not RPCs. Fresh reopen clears the transport pool; reused reopen does not.

| Metric | Value | Native | TCP | EFA |
| --- | ---: | ---: | ---: | ---: |
| Durable scalar-write p50, µs | 1 KiB | 53.935 | 1,085.213 | 852.491 |
| Durable scalar-write p50, µs | 8 KiB | 173.336 | 1,094.828 | 944.163 |
| Durable batched load, k mutations/s | 1 KiB | 205.800 | 163.368 | 171.958 |
| Durable batched load, k mutations/s | 8 KiB | 73.321 | 43.527 | 45.219 |
| Warm single-read p50, µs | 1 KiB | 0.898 | 248.446 | 42.412 |
| Warm single-read p50, µs | 8 KiB | 2.535 | 266.733 | 57.338 |
| Warm single-read p99, µs | 1 KiB | 1.594 | 339.008 | 52.535 |
| Warm single-read p99, µs | 8 KiB | 3.806 | 390.716 | 69.264 |
| Warm window32, k reads/s | 1 KiB | 71.691 | 65.212 | 114.531 |
| Warm window32, k reads/s | 8 KiB | 63.363 | 50.076 | 55.039 |
| Warm get-many32, k values/s | 1 KiB | 1,112.677 | 69.107 | 176.523 |
| Warm get-many32, k values/s | 8 KiB | 317.290 | 42.808 | 58.751 |
| Cache-evicted window32, k reads/s | 1 KiB | 66.197 | 44.527 | 79.548 |
| Cache-evicted window32, k reads/s | 8 KiB | 36.315 | 38.417 | 52.922 |
| Full scan, ms | 1 KiB | 12.094 | 27.963 | 28.924 |
| Full scan, ms | 8 KiB | 1,941.600 | 104.901 | 106.437 |
| Fresh engine reopen, ms | 1 KiB | 1.997 | 20.523 | 46.484 |
| Fresh engine reopen, ms | 8 KiB | 1.864 | 20.673 | 48.493 |
| Transport-reused reopen, ms | 1 KiB | Not measured | 14.756 | 17.112 |
| Transport-reused reopen, ms | 8 KiB | Not measured | 17.473 | 14.288 |

EFA beats TCP on warm point reads and measured window throughput, but single-read
p50 remains 47.2× native for 1 KiB and 22.6× for 8 KiB. Batched durable EFA writes
reach 83.6% and 61.7% of native throughput. Scalar writes and get-many remain
substantially behind native. EFA fresh reopen remains slower than TCP; transport
reuse changes that ordering for 8 KiB, but is a different operation.

The window wins are for the measured adapters, not a native throughput ceiling:
native's serial 1 KiB point-get phase reaches 856.637k reads/s, much higher than
its acknowledged 32-worker window adapter. No all-metric parity claim follows.

The 8 KiB scan returns 8,192 ordered values, or 64 MiB of caller-held payload,
and verifies the complete result. Its advantage is repeatable, but is not a
controlled warm/cold scan or a transport-only comparison. Native uses a serial
iterator; distributed scan prefetches bounded shard pages. The preceding eviction
and partial-read phases affect cache state. The previous native scan was about
739–755 ms, while previous distributed scans were about 100–104 ms. Much of the
larger new ratio is a slower native denominator, not a new distributed scan gain.
No phase-specific scan I/O breakdown establishes its cause.

Cold-read records prove physical reads, not merely cache hits: for 8 KiB window
repetition 0, native records 43,376,640 bytes and the five EFA shards total
37,335,040 bytes, with zero WAL-resident bytes before admission. Cache warms
within each finite pass. Data is much smaller than RAM; this is not sustained
larger-than-RAM, five-disk saturation, or linear NVMe scaling evidence.
All recorded swap-in/out deltas were zero.

### 17.3 What cluster placement changed in the observations

These RAM probes execute the real authenticated transport path without a WAL
read or the database's coordinator index/read actor. Both sides use the **same
baseline source**; continuous sampling, payloads, five peers, warmups, and three
repetitions match. Busy-policy median p50:

| Transport / payload | Historical spread, µs | Cluster, µs | Cluster / spread |
| --- | ---: | ---: | ---: |
| TCP / 1 KiB | 321.676 | 246.536 | 0.766 |
| TCP / 8 KiB | 350.060 | 267.384 | 0.764 |
| EFA / 1 KiB | 65.211 | 35.864 | 0.550 |
| EFA / 8 KiB | 84.716 | 51.457 | 0.607 |

The cluster run has a much lower observed EFA transport-stack floor. **Do not
call these causal placement gains:** time, fleet, cloud build environment, and AZ
also differ; the historical run used `us-east-1a`. This is not physical wire RTT.
[`ram-floor-comparison.json`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/ram-floor-comparison.json)
also retains Adaptive, current/baseline, profiling, and offered-idle controls.

Current unprofiled EFA RAM p50 is 0.6–3.5% slower than same-fleet baseline; Adaptive
TCP is 10.8–11.6% slower. These observations must remain visible. Continuous EFA
profiling adds about 5–6% p50; Busy TCP's profiled median is slightly lower, which
does not establish negative instrumentation cost. Offered idle increases latency
even with Busy. Adaptive's requested 50 µs sleep measured about 105 µs residence
in the idle EFA control; eliminating that sleep does not eliminate the whole
idle penalty.

[`historical-api-comparison.csv`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/historical-api-comparison.csv)
retains 2,598 matched main summary metrics across campaigns.
[`historical-wall-regressions.json`](../../logs/stage-timing-cluster-2026-10-12-192613/analysis-cluster/historical-wall-regressions.json)
lists all 16 distributed phase-wall increases above 10%, including missing-key
phases, some scans, maintenance, and reused reopen. These are cross-campaign
observations, not isolated code regressions; short single-sample phases do not
establish tail behavior.

### 17.4 Where the measured time goes

Serial profiled reads on the B32/W32-prepared database; means of 256 samples.
These are separate diagnostic passes, not the performance-table percentiles.
All times below are µs and **nested**, not additive.

| Value / transport | Whole call | Exchange | Coordinator index | Record validation inside exchange | Shard handler inside exchange |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 KiB / TCP | 223.701 | 219.238 | 2.175 | 1.449 | 2.963 |
| 1 KiB / EFA | 44.205 | 41.490 | 0.342 | 1.396 | 2.704 |
| 8 KiB / TCP | 280.846 | 279.139 | 0.494 | 7.884 | 3.512 |
| 8 KiB / EFA | 59.054 | 56.156 | 0.444 | 7.825 | 3.328 |

The caller-owned scalar lane was used; zero actor queue/service fields mean the
actor path was not used. The evidence does not implicate that coordinator queue
as the warm scalar bottleneck. `exchange` includes authenticated transport,
polling, scheduling, server residence, and record validation. Subtracting handler
time does not reveal physical network time.

The 8 KiB profiled Busy RAM control records mean client exchange of 44.301 µs
on EFA and 240.415 µs on TCP, plus about 11.65 µs of deterministic benchmark
payload verification **after** exchange. That verifier is not the production
WAL-member verifier. Selected EFA registered-slot copies take 0.026/0.143 µs
per probe on the client and 0.177/0.023 µs on the server. Measured client
authentication/decrypt work is about 1.58 µs per probe. These scopes do not
support a claim that bulk memcpy or encryption explains the whole gap.

The same 3,000 EFA probes cause 348,279 client CQ polls and 2,113,677 server
polls. TCP causes 1,601,745 client and 7,562,988 server TLS-read attempts.
Many are unsuccessful nonblocking attempts. Server totals span five independently
progressing connections, idle time, and some profile-control traffic.
`WouldBlock` appears in diagnostic error counters; it is not a failed request.
[INFERENCE] Repeated polling/adapter work is a stronger optimization candidate
than the small measured copy costs, but empty polls can also be a consequence
of delayed completion. They do not prove the original cause of that delay.

Durable-write work is different. Weighted local means in the 8 KiB load
diagnostic, over 121 append/persist calls across five shards:

| Per recorded call, µs | TCP | EFA |
| --- | ---: | ---: |
| Append decode/validation stage | 1,116.036 | 1,013.445 |
| Group construction | 87.448 | 79.355 |
| Storage append, inclusive | 551.829 | 473.696 |
| WAL-file persistence barrier | 2,024.961 | 1,836.143 |
| Inventory-lock acquisition | 0.096 | 0.118 |
| Storage append state-lock acquisition | 0.090 | 0.092 |
| Persistence serialization-lock acquisition | 0.054 | 0.049 |

These local work/residence clocks overlap with coordinator wait spans and other
shards. They are not 121 globally serialized barriers. Native records 10
complete-window barriers for the same 10,240-mutation load; distributed uses
50 groups at 1 KiB and 121 at 8 KiB. Both wait for the required durable writes,
but grouping granularity differs. The measured write-side candidates are
barrier amortization and decode/validation work, not these sub-microsecond lock
acquisitions. Removing locks or changing transport alone does not remove them.

### 17.5 Timing correctness and remaining attribution limits

The frozen source is in the result archive. Relevant code:

- [`tcp/reactor.rs`](../../tidehunter/src/distributed/tcp/reactor.rs):
  read exchange/record validation, server admission, completed-response authority
  recheck, output queue, and local completion retirement.
- [`remote_db/pending_read.rs`](../../tidehunter/src/distributed/remote_db/pending_read.rs):
  caller-owned scalar lane and nonblocking lane admission, with actor fallback.
- [`tcp/fabric.rs`](../../tidehunter/src/distributed/tcp/fabric.rs) and
  [`endpoint/resources.rs`](../../tidehunter-fabric/src/endpoint/resources.rs):
  connection-level TLS/framing work, registered-slot copies, posts, and CQ calls.
- [`storage.rs`](../../tidehunter/src/distributed/storage.rs):
  append validation, file/namespace persistence, and durable-frontier publication.
- [`remote_db/read_pipeline.rs`](../../tidehunter/src/distributed/remote_db/read_pipeline.rs)
  and [`support/native_reads.rs`](../../tidehunter/examples/support/native_reads.rs):
  different scan/window adapters behind the bounded comparison.

Three offline reporting defects were corrected after retrieval:

1. The generic summary required an old/new ratio and therefore omitted current-only
   main cases while exposing qualification as the headline. Stage summaries now
   retain exact main shapes and exclude qualification from that headline.
2. Profile aggregation dropped batch/window and merged scalar and window-prepared
   diagnostics into 512-sample entries. They now remain separate 256-sample
   diagnostic shapes.
3. Legacy per-request send/header/body/socket fields and unmeasured RAM-handler
   fields were numeric zeros with positive generic row/request denominators.
   Derived stage means now mark these fields unavailable, not zero-cost work.
   Raw CSVs remain unchanged. Connection-wide I/O counters are still available.

Changes are limited to the offline analyzer, stage-summary helper, and regression
tests. The measured cloud executable and raw records were not changed.
The 125-test runner suite passed with two skips, and the corrected analyzer again
validated all 68 outputs and 260 archives against the real downloaded data.

Limits not resolved by those report fixes:

- The completed-response inventory/authority recheck can defer TLS enqueueing
  after the recorded shard-handler interval. It has no dedicated wait span;
  the short measured admission wait does not rule out this later wait.
- RAM-probe exchange starts after its request is queued and waits for local
  output retirement. WAL-read exchange includes request encoding and can
  complete before the request's local CQ completion. Their scopes differ.
- Client transport counters precede the profile-control exchange; some socket
  and server counters include that control traffic.
- Poll gaps are progress opportunities, not packet-arrival timestamps.
  No field measures one-way network latency, hardware RTT, or pure NVMe service.
- Window scheduling, finite cache state, per-process memory allowances, and
  caller-held scan values prevent an equal-total-resource or production-scaling
  claim. This campaign did not rerun process-crash recovery.

### 17.6 Next work supported by this run

1. Keep the direct scalar read path. Preserve authenticated completion, exact
   WAL-member verification, authority checks, and bounded ownership.
2. Investigate TCP nonblocking polling and EFA progress/completion residence with
   the zero-WAL controls. Add the missing response-revalidation wait attribution
   before labeling the unexplained remainder network or mutex time.
3. Reduce write decode/validation and durable-group/barrier overhead without
   weakening the acknowledgment boundary or globally serializing independent
   shard WALs.
4. Keep fresh setup and transport-reused reopen separate. Do not present reuse
   as process recovery or as an equivalent native reopen.
5. Evaluate any `efa-direct` or one-sided proposal against the measured
   authenticated transport floor. Neither was tested here. A remote-storage
   layer cannot be credited with removing a gap already present without WAL
   access. This run does not justify a global-WAL rewrite or claim all-metric
   native parity.

No further cluster was launched. No installed `kvstore` skill was made stale.

### 17.7 Complete closure and cost observation

Instances launched at 19:35:39–42 UTC. The controller exported the complete
archive/receipt and requested termination of all six at 19:51:49 UTC, before the
original expiry. The local downloaded receipt and bytes were independently
verified. All six were observed terminated, not merely stopped.

The cleanup receipt completed at 20:05:05 UTC. Independent AWS MCP verification
at 20:06:31 UTC found both attempts' buckets, roles, profiles, security groups,
placement groups, network interfaces, and EBS volumes absent. No pending,
running, stopping, stopped, or shutting-down benchmark instance remained across
23 enabled regions. All four protected unrelated instances retained their
running state.

- [`cleanup-receipt.json`](../../logs/stage-timing-cluster-2026-10-12-192613/run-us-east-1b/cleanup-receipt.json)
- [`final-closure-verification.json`](../../logs/stage-timing-cluster-2026-10-12-192613/final-closure-verification.json)
- [`compute-cost-observation.json`](../../logs/stage-timing-cluster-2026-10-12-192613/compute-cost-observation.json)

At the verified $6.4512 per host-hour rate, launch-to-termination-request compute
is estimated at **$10.42**. Extending every host's interval to the first locally
recorded terminated-state receipt gives a conservative **$12.39** observation
bound, below the $16.128 hard-TTL compute cap and $18 campaign reserve.
Including the prior conservative compute ledger gives $49.94. These are timing
estimates, not an AWS invoice, and exclude EBS, S3, IPv4, API, and tax charges.
All run-owned ongoing compute/storage resources are removed.

## 18. Real Sui integration and transport fixes, 12–13 October 2026

This section supersedes earlier statements that the fork has no Sui selection.
It does not replace the dated measurements above with unverified results.
The new checkpoint path uses this fork directly; it does not use the old
`middleware` storage facade.

### 18.1 Source map and implemented changes

| Problem | Implementation | Preserved boundary |
| --- | --- | --- |
| Sui could not select the new sharded Value WAL | [`distributed/sui.rs`](../../tidehunter/src/distributed/sui.rs), [`typed-store/tidehunter_util.rs`](../../../sui/crates/typed-store/src/tidehunter_util.rs), Sui feature/build configuration | Explicit native/remote selection; exact canonical namespace and schema; no silent native fallback |
| Existing bulk interfaces degraded to scalar storage reads | [`execution_cache.rs`](../../../sui/crates/sui-core/src/execution_cache.rs), [`writeback_cache.rs`](../../../sui/crates/sui-core/src/execution_cache/writeback_cache.rs) | Input order, duplicates, missing values, negative cache entries and errors remain observable |
| Checkpoint shared-version initialization repeated independent lookups | [`shared_object_version_manager.rs`](../../../sui/crates/sui-core/src/authority/shared_object_version_manager.rs), [`authority_per_epoch_store.rs`](../../../sui/crates/sui-core/src/authority/authority_per_epoch_store.rs), checkpoint executor | Batch only known, unexecuted, non-end-of-epoch work; preserve certified effects, cancellation and epoch barriers |
| Remote reads could retain coordinator ownership across waits | Adapter split submit/wait/finish with [`pending_read.rs`](../../tidehunter/src/distributed/remote_db/pending_read.rs) | Network wait runs outside the adapter mutex; completion still validates authority, exact reference and health |
| Newly ready transport work waited for another reactor iteration | [`tcp/reactor.rs`](../../tidehunter/src/distributed/tcp/reactor.rs), [`tcp/fabric.rs`](../../tidehunter/src/distributed/tcp/fabric.rs) | Same bounded progress sweep for resident reads; output-only progress for newly queued responses; no unbounded busy loop |
| Completed-response revalidation was absent from stage attribution | Reactor diagnostics, protocol-v7 profile, benchmark and cloud analyzers | Separate wait/work/deferral/error and queue/submit/retire spans; local CQ completion is not peer receipt |
| Batch decoding created an avoidable intermediate representation | `CompleteBatch::decode_record` in [`codec.rs`](../../tidehunter/src/distributed/codec.rs) | Decode into reserved output storage without removing record, member or authority validation |
| Native pruning callbacks had no correct remote equivalent | Adapter retained-history frontier and bounded durable tombstones | Earliest retained Stop decision, including superseded/deleted history; newer updates survive; full-key collision and reduced-cell bounds remain correct |
| Pruning batches could exceed encoded-byte or operation limits | Adapter filter/compactor tombstone admission | Enforce both limits before each flush, including large keys; whole-cell range deletion remains all-or-error before publication |
| Shutdown errors could be converted into panics or lost before restore rename | Adapter, typed-store, authority tables and Sui restore caller | Propagate operating errors; do not rename restored state after failed rebuild/shutdown |
| Synthetic KV results did not establish actual Sui behavior | [`experiments/sui-checkpoint`](../../experiments/sui-checkpoint/README.md) and [`sui_checkpoint.py`](../../experiments/aws-sharded-wal/sui_checkpoint.py) | Actual fullnode execution, certified tip, exact complete state audit, same-source binaries and bounded owned processes |

The relevant primary storage reference is
[Tidehunter 2026 v2](../../../papers/parsed/Tidehunter-2026-v2.md), especially
§3.1 (allocation, publication and durability), §3.2 (indexed WAL reads),
§3.3–3.4 (snapshot/replay boundaries), and §4.4 (relocation).
The implementation retains values in append-only shard WALs and direct indexed
references. It does not introduce an LSM value rewrite, a second consensus value
log, a global physical WAL counter, or NVMe-oF. The Sui bulk work follows the
existing Sui cache/transaction interfaces rather than adding speculative
fetch-and-discard prefetching.

### 18.2 Timing and correctness contract

- Prometheus distinguishes Sui API calls from key counts and actual remote
  read RPCs. Empty, singleton and multi-key cache requests remain separate.
- Adapter counters export read index/wait work, coordinator cache outcomes,
  write validation, storage wait and publication. Shard cache hits and WAL-read
  submissions are logical events, not physical NVMe reads.
- Full per-request transport diagnostics belong to the diagnostic benchmark;
  the Sui runner does not claim every production read carries that profile.
- No clock here measures one-way network latency or pure NVMe service time.
  Aggregated concurrent stage times are not additive wall-clock fractions.
- Matched replay uses a real native persistence barrier and durable remote
  complete-batch acknowledgment. It must not compare durable remote writes
  against native's default asynchronous persistence as if guarantees matched.
- State audit covers all selected Tidehunter namespaces and live tables,
  including zero-row tables, plus the remaining RocksDB databases. Missing
  namespaces, rows, errors or incomplete index catchup fail completion.

### 18.3 Remaining architectural limits

The Sui adapter's scalar remote reads currently use detached actor progress, not
the benchmark's caller-owned scalar lane. Its synchronous remote write still
holds the per-database coordinator mutex across append/persistence and publishes
under the adapter write gate. Reads arriving during that write can therefore
wait before admission. The existing independent pending-batch engine API does
not by itself prove Sui uses concurrent writes. Measure this path before claiming
that coordinator contention is gone or making a further split-phase-write change.

The coordinator cache is bounded and demand-filled after verified reads, not
write-through. Native and distributed cache layouts are recorded but do not
form an equal-total-RAM comparison. This remains static RF1 with bounded retained
history: no automatic failover, replication or physical WAL reclamation.
Remote schemas are immutable. Directory-copy checkpoints, secondary mode and
coordinator-only database deletion cannot safely represent distributed state
and return errors. A short checkpoint replay is not production retention proof.

### 18.4 Workload and validation gates

The retained signed private chain contains 1,776 checkpoints (`0..1775`) and
4,096 generated user transactions. Replaying it is real Sui fullnode execution,
not a synthetic KV trace; its transaction mix is nevertheless generated and
does not establish mainnet representativeness. The runner records that distinction.
It uses five physical storage processes per selected database locally, sharing
one SSD; cloud partitions place the corresponding processes on five separate
NVMe hosts.

The distributed library passed 658 tests with seven ignored. Its fabric-enabled
run passed 658 with ten ignored; a separate seven-case fabric-filtered run
included the ignored authenticated RPC/recovery and out-of-order tests.
All eight raw software-provider endpoint tests passed. Both distributed-only
and fabric-enabled library Clippy passed with warnings denied. Local fabric
checks use the retained libfabric 2.4 build, not EFA hardware.

The authentication/recovery test intermittently reported a truncated TLS session
in the full parallel suite with a 500 ms server bound, but passed in isolation and one
unchanged full-suite invocation. Its server and client now use the normal
10-second bound because it checks protocol semantics, not storage latency.
The full suite then passed. Production timeout behavior was not changed;
temporary diagnostics were removed. These observations do not establish a
production transport defect or a measured performance improvement.

The local checkpoint harness passed 41 tests; the cloud runner suite passed
146 with three skips. All six targeted Sui behavioral checks passed with
`--cfg fail_points`, including bulk ordering/misses, event call/key accounting,
effects-based shared-version initialization/error propagation, and congestion
cancellation. The first cancellation invocation omitted that cfg and did not
activate its injection. Enabling it exposed simulator-only hooks compiled on
native Linux; those hooks and imports are now gated to their actual targets.
The final same-build local and cloud paired replays passed; §19 records the
measurements, complete audits, source identity and verified cloud closure.

The Sui tool caller and optional fabric-enabled node both passed release checks.
Native/remote configuration helpers compile only for the applicable backend;
schema export and metric namespaces share the same root selection. These
checks still report inherited warnings in the Move runtime and latest Sui
adapter; they are not full-workspace warning-free validation.

The first full local replay completed native checkpoint 1775, then exposed a
deployment error: `max_admitted_batches=128` incorrectly used the pending-window
size as the retained epoch-history limit. Remote checkpoint synchronization
stopped with `TCP authority admission capacity reached`. Deployment now uses
the storage history bound (1,000,000 batches), while the pending window remains
128 batches / 128 MiB. History grows with actual admissions; it is not allocated
to that upper bound at startup. Retry identities are not discarded, and no
storage error is suppressed. This is finite campaign sizing, not production GC.

The deployment gate requires all five freshly built executables, archived
sources and hashes, successful same-build local replay, and equal complete
state audit. No cluster is admitted on unit tests alone. The candidate bounded
campaign is native/TCP on six `i4i.xlarge` hosts in a cluster placement group,
one local NVMe each. This size has no EFA; it cannot validate EFA hardware
behavior or support an RDMA performance claim.

Cloud order is mandatory: verify execution and evidence, export raw results
and logs to S3, terminate all six exact run-owned instances, retrieve and verify
the saved bytes, confirm every instance is terminated, then analyze performance.
The guest runs `analyze.py --verify-only`; it does not spend paid fleet time
deriving performance comparisons. Independent absolute shutdown timers remain
armed, and OS shutdown must terminate rather than stop. Failure exports and
termination are unconditional; unrelated instances and resources are excluded.

No installed `kvstore` skill was made stale; those skills are absent locally.

## 19. Completed real Sui replay and cloud closure, 13 October 2026

Run `th-17e297a37e3f5aac` is closed. Performance analysis began only after
retrieval, integrity checks and verified deletion of the run-owned cloud resources.
This section supersedes the pending replay gate in §18, not earlier synthetic
measurements or their different workload/durability assumptions.

### 19.1 Executed workload and source identity

All five executables were rebuilt before upload: `sui-node-native`,
`sui-node-remote`, `storage-node`, `state-audit` and `rocksdb-audit`. The accepted
same-build local replay preceded cloud admission. The cloud used those exact
hash-checked artifacts, not historical binaries from the old middleware.

| Artifact | SHA-256 |
| --- | --- |
| Build manifest | `a8c5165b2c29f52b1311a566f864ae558703dbcd349acd76742fd2d3991858fb` |
| Source archive | `7c67c43cb399a08e9f8b310bac1ccec5a0e74441ec07ed0e9b37d1c802382b67` |
| Build bundle | `9cdf49a1b4e3ebbc5def95137cfcdec7a5d6b720f86e0989929a378917a8f45c` |
| Local replay evidence | `398a3bbae12aac47448601eddda13eec04c75cbd5d3fc9720a42a180cfc8aa0a` |
| Retrieved cloud controller archive | `133052f5330d230a5911582048053fb2804f12bec6c9100ab9483cb0c118d3a8` |

The workload executed the signed private chain's full checkpoint prefix
`0..1775`, with 4,096 generated user transactions. Its completed certified tip
was `EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`; the ordered summary-range
SHA-256 was `896408bc8581e07473e11db805a69295286df17e05cfb90ed0952c45a9286f9f`.
This is actual Sui fullnode execution, but a generated-transfer private chain,
not a representative mainnet workload.

Three selected databases each had five storage processes: fifteen processes
on five separate `i4i.xlarge` storage hosts, plus a controller/producer host.
Each host had its own local NVMe. All hosts used `us-east-1b` and a **cluster**
placement group. This campaign used TCP only; this instance size has no EFA.
The coordinator cache budget was 8 MiB per selected database and the explicit
shard value-cache budget was zero. Linux page cache was not flushed.

Native and remote writes used matched synchronous durability. Native's actual
WAL persistence barrier was enabled; this is not a comparison against the
paper's normal asynchronous-persistence write path.

### 19.2 Correctness and elapsed results

Both modes passed the complete audit of all 57 selected Tidehunter tables:
167,858 rows, including coverage of zero-row tables, with matching row counts,
key/value byte totals and sorted length-delimited row hashes. The remaining
RocksDB `epochs`, `indexes` and `rpc_store` were audited too. Legacy `indexes`
needed the existing validated sequence/digest-bijection normalization: semantic
rows matched, but the complete RocksDB files/raw rows were not byte-identical.
The cloud receipt has `complete=true` and no failures.

| Environment | Native foreground elapsed | Distributed TCP foreground elapsed | Distributed / native |
| --- | ---: | ---: | ---: |
| Laptop, one shared SSD | 16.975 s | 33.220 s | 1.957× |
| AWS cluster placement, separate NVMe hosts | 8.784 s | 11.787 s | 1.342× |

Foreground elapsed includes measured-node startup. The distributed cloud replay
took 34.18% longer; parity was not reached. The local/cloud ratio difference
cannot be attributed only to SSD separation: CPU, kernel, network and cache
conditions differ too. These are single paired runs, not confidence intervals.

The sampled completion windows exclude part of startup, use 250 ms sampling,
and cover a completed-checkpoint delta of 1,775:

| Environment/mode | Sampled window | Sampled checkpoint rate |
| --- | ---: | ---: |
| Local native | 15.922 s | 111.48/s |
| Local TCP | 31.156 s | 56.97/s |
| Cloud native | 7.695 s | 230.66/s |
| Cloud TCP | 10.229 s | 173.53/s |

These rates are not per-checkpoint latency percentiles. Earlier private runs
with native 5.219 s / old facade 17.795 s changed source, durability and topology;
they are not a controlled before/after speedup for this implementation.

### 19.3 Actual Sui read and batch-write boundaries

The retained Prometheus histograms expose typed-store boundaries. The following
are arithmetic means (`sum/count`), **not p50**, pure network time or bare engine
lookup time. Despite their `rocksdb_` names, these wrapper timers also surround
the selected Tidehunter backend.

| Cloud get table | Native calls | TCP calls | Native mean | TCP mean |
| --- | ---: | ---: | ---: | ---: |
| `running_root_accumulators` | 3,551 | 3,551 | 8.24 µs | 329.68 µs |
| `certified_checkpoints` | 3,523 | 3,552 | 32.77 µs | 294.29 µs |
| `full_checkpoint_content_v2` | 3,551 | 3,551 | 38.26 µs | 264.14 µs |
| `checkpoint_content` | 3,552 | 3,552 | 10.76 µs | 237.52 µs |
| `state_hash_by_checkpoint` | 1,776 | 1,776 | 3.02 µs | 141.85 µs |
| `object_per_epoch_marker_table_v2` | 13,590 | 13,849 | 1.55 µs | 4.19 µs |

The whole-application 1.342× result therefore does **not** mean remote point
reads are close to native. Some important checkpoint metadata gets remain
approximately 40–47× slower at this wrapper boundary. Call-count differences
are retained, not normalized away.

Matched durable batch commits at `DBBatch::write_opt` tell a different story:

| Cloud database | Calls in each mode | Native mean | TCP mean |
| --- | ---: | ---: | ---: |
| Checkpoint | 3,551 | 639.33 µs | 575.55 µs |
| Epoch | 3,555 | 661.70 µs | 623.15 µs |
| Perpetual | 5,329 | 1,108.69 µs | 961.68 µs |

Those mean batch commits are not slower in the distributed run. This does not
cover every scalar put or prove faster tails. The implementation boundary is
[`DBBatch::write_opt`](../../../sui/crates/typed-store/src/rocks/mod.rs), which
times `write_opt_internal` and commit accounting. Do not replace this matched
boundary with native engine-only write timing that omits the outer barrier.

The original analyzer retained metrics by keyword and omitted epoch/perpetual
wrapper series without a checkpoint label. Raw evidence already contained them.
The post-run reporting fix retains `rocksdb_` and `db_op_` series too. Its
regression test and all 24 tests in `test_harness.py` passed; both sealed replay
archives were reanalyzed successfully. This reporting-only change did not alter
any execution binary, frozen evidence or measured run.

### 19.4 Waits, caching and physical-host evidence

| Distributed cloud counter | Cumulative value |
| --- | ---: |
| Published batches | 22,030 |
| Actual read RPCs / coordinator cache misses | 12,051 |
| Coordinator cache hits | 5,276 |
| Coordinator cache evictions | 3,503 |
| Read index work | 0.0592 s |
| Read wait/work | 2.8892 s |
| Write validation | 0.4552 s |
| Write storage wait | 9.5850 s |
| Write publication | 0.6719 s |
| Shard logical WAL-read bytes | 21,597,786 |
| Shard asynchronous disk-read submissions / read errors | 0 / 0 |

These spans overlap across databases and operations. They are **not** additive
fractions of the 11.787 s elapsed time. Storage wait includes durable RPC,
queueing and persistence; it is not a network-only measurement. Read wait is
`LocalDbStats.value_read_ns`, including the relevant read/scan paths, not
one-way transport latency. Index work excludes time waiting to acquire the
adapter owner: 0.0592 s does not establish that coordination contention is gone.

Checkpoint DB reads account for 6,121 RPCs, 1.7720 s of read wait and 3,494
evictions. That cache retained 8,383,733 bytes against an 8 MiB budget, including
1,677,312 bytes of metadata. The epoch DB had 1,779 RPCs / 0.7816 s read wait;
perpetual had 4,151 RPCs / 0.3356 s read wait. The cache remains demand-filled,
not write-through.

Sui's execution cache matters independently of the coordinator cache:
`executed_effects_digests` made 1,775 multi-key DB fallback calls in both modes;
the much larger empty-call counts are not reads or RPCs. Object-by-version bulk
fallbacks had zero keys in this workload. It is incorrect to multiply API-call
counts by a synthetic remote scalar latency.

All five storage hosts had healthy samples spanning the replay, three owned
namespace processes each, no dropped samples, no process swap and no host
swap-in/out. Each recorded roughly 0.58–0.59 GB of process-attributed physical
writes over its sampled lifetime. All recorded zero process-attributed physical
read bytes. Those intervals include startup and audit, not only foreground
replay. The controller also recorded no cloud swap. This is resident-read
evidence, not a cold-NVMe read-throughput measurement.

Sampled local RSS sums the Sui node and its local shard processes. Cloud summary
RSS covers the Sui node only; separate host resource records cover its shards.
Do not compare those two memory columns as equal-scope coordinator measurements.
The local host recorded 64 swap-in pages during native replay, zero swap-out;
the measured Sui node had zero final `VmSwap`. This does not establish that the
local workload itself swapped.

### 19.5 Next work justified by this evidence

1. **Reduce checkpoint metadata round trips.** [INFERENCE] A bounded cache
   admission path for freshly committed values can avoid first-read RPCs that
   a larger demand-only cache cannot. Admit only after exact durable publication;
   retain reference/authority/health checks, deletion invalidation and byte limits.
   Measure the tables in §19.3, not just synthetic warm hits.
2. **Measure owner wait separately, then split write admission/wait if material.**
   The current Sui write holds its per-DB mutex across durable storage completion.
   Detached reads release ownership during their own waits but can still queue
   behind writes. Any split must preserve whole-batch visibility, ordering,
   pruning frontiers, errors and bounded pending memory.
3. **Expose a suitable low-overhead read lane to the Sui adapter.** It currently
   uses actor progress, not the synthetic benchmark's caller-owned scalar lane.
   [INFERENCE] Integrating that path may reduce the uncached metadata-read cost;
   this campaign did not isolate actor queue time from wire time.
4. **Keep durable batching and expand workload coverage before another transport
   conclusion.** The observed batch-write means do not justify replacing the
   WAL architecture. A larger, longer, mainnet-like checkpoint mix and cache-miss
   workload are needed before claiming NVMe scaling or production parity.

No result here establishes that EFA/RDMA or NVMe-oF fixes the remaining gap.
Remote cache misses still need communication, while compute-local verified hits
can avoid it. The retained model remains per-shard Value WALs with local indexed
references and a coordinator publication/index authority per database, not a
cluster-wide physical WAL counter. Static RF1, finite retained history, no
physical WAL GC and no failover remain explicit limits.

### 19.6 Retrieval, termination and operational evidence

AWS MCP repeatedly returned `Unknown tool: aws___run_script`. The authorized
SDK fallback was used; this was not a successful MCP-controlled launch. The
parent IAM user also lacked SSM command permission. Its initial idle controller
was terminated and replaced by a boot-started controller using the existing
scoped instance role. Neither user permissions nor the absolute fleet deadline
were expanded. Independent shutdown/termination bounds remained armed.

The controller verified execution and exported the complete archive to S3
before triggering run-owned termination. Local retrieval verified the archive
hash and preserved **46 result/log objects**. All **seven** launched instances,
including the replaced controller, were confirmed terminated at
`2026-10-13T00:59:26.505379Z`. Cleanup was independently confirmed at
`2026-10-13T01:03:47.248836Z`: zero owned EBS volumes, security groups, placement
groups, IAM roles/profiles or S3 bucket remained. The 23-enabled-region check
found no non-terminated TideHunter instances; four unrelated instances remained
running and untouched. Analysis followed this closure verification.

Evidence:

- [Local paired replay summary](../../../checkpoint-work/2026-10-13-final/replay/summary.json).
- [Cloud paired replay summary](../../../checkpoint-work/2026-10-13-final/cloud-ready/th-17e297a37e3f5aac/results/replay/summary.json).
- [Derived timing and host analysis, including input hashes](../../../checkpoint-work/2026-10-13-final/cloud-ready/th-17e297a37e3f5aac/analysis-details.json).
- [Retrieved files and checksums](../../../checkpoint-work/2026-10-13-final/cloud-ready/th-17e297a37e3f5aac/retrieved-results.json).
- [Exact instance termination receipt](../../../checkpoint-work/2026-10-13-final/cloud-ready/th-17e297a37e3f5aac/termination-verified.json).
- [Final resource cleanup receipt](../../../checkpoint-work/2026-10-13-final/cloud-ready/th-17e297a37e3f5aac/cleanup-verified.json).

No installed `kvstore` skill was made stale; those skills are absent locally.


## 20. Completed native/TCP/EFA real checkpoint matrix, 13 October 2026

This supersedes the **missing EFA validation** in §19, not that section's
historical measurements. Run `th-35862b94cc9d0904` completed all four paired
replays and full-state audits. Its six instances and auxiliary resources were
removed before performance analysis. **EFA helped, but native parity was not
reached.** The measurements identify coordinator ownership as well as transport
cost; they do not support attributing the whole gap to the network.

### 20.1 Implemented benchmark and timing changes

| Location | Implemented change |
|---|---|
| `experiments/aws-sharded-wal/checkpoint_matrix.py` and `sui_checkpoint.py` | Matched native/TCP/EFA cells; alternating transport order; per-cell profile limits; exact placement checks; EFA hardware activity and provider qualification; fresh roots, process units and identifiers; release of one cell before the next; one absolute fleet deadline |
| `experiments/sui-checkpoint/cloud_node.py` | Per-host/per-namespace resources and lifecycle evidence; cell/transport and artifact identity binding |
| `experiments/sui-checkpoint/run.py`, `deployment.py`, `analyze.py` | Local TCP/software-fabric execution, explicit bounded read profiling, timing qualification and unavailable-versus-zero handling |
| `tidehunter/src/distributed/sui.rs` | Native durable-write gate timing; remote owner, write-owner, read-finish-owner, admission, submit, detached-wait and finish spans, with call/error counts |
| `tidehunter/src/distributed/remote_db.rs`, `remote_db/pending_read.rs` | Bounded first-N actor queue/service/return, protocol, verification, server and transport measurements, exposed through lock-free diagnostic snapshots |
| `tidehunter/src/distributed/tcp/reactor.rs` | Actual timed CQ-call counts; disabled diagnostic clocks outside sampled work |
| `../sui/crates/typed-store/src/tidehunter_util.rs` | Export the additional adapter fields through the existing Sui metrics surface |

These are measurement and execution changes. They do not replace the WAL,
weaken durable publication, add replication or implement the further owner-lock
optimizations identified below. Native core WAL behavior remains unchanged.

The fresh build included native Sui, fabric-enabled distributed Sui, storage,
the distributed state auditor and the RocksDB auditor. Source binding:

- Build manifest SHA-256:
  `6c77785e65e0d5bbe87b077112800132cd98b4e1c33ed05587e0d50e85182815`.
- Frozen source archive SHA-256:
  `f3da51677ddcb2f6d86691edd90d99ea730d35c9f4832eb7ea7bf0eda75e62f1`.
- Uploaded binary bundle SHA-256:
  `414896b9c8b5ca433cd5ed604d6bc38e09ee880e385e65d57ccb02aec0988890`.
- Protocol v7; fabric enabled. Local libfabric 2.6.0; cloud EFA installer 1.50.0
  supplied libfabric 2.6.0amzn1.0.

Local verification passed 281 distributed Rust tests, the explicitly exercised
ignored software-fabric cases, strict Clippy, 46 checkpoint Python tests and
158 cloud Python tests with three skips. Both local real-checkpoint transports
passed complete state audits after the fresh build. The local TCP elapsed pair
was 14.078/17.301 s; software-fabric was 14.104/22.056 s. **Host swap activity
occurred locally**, so those times are provisional correctness evidence, not a
hardware EFA prediction or proof of a local speed improvement.

### 20.2 Workload, topology and acceptance evidence

- Same frozen private generated-transfer chain: checkpoints **0..1775 inclusive**,
  tip `EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`.
- Real `sui-node` execution, not synthetic KV operations and not mainnet.
  The fixture records 4,096 generated user transactions; that is not a claim
  that its total system-plus-user transaction count is 4,096.
- Six `g6.8xlarge` hosts in **us-east-1b**, one **cluster placement group**:
  one controller/Sui host and five storage hosts. Each host used its own local
  NVMe; only one of its two local NVMe devices was used. The GPUs were unused.
- Three selected database namespaces, five shard processes per namespace:
  **15 storage processes on five physical storage machines**, not 15 machines.
- RF1, 8 MiB coordinator cache per DB, zero explicit shard cache.
  Native synchronous WAL persistence and remote durable publication were both
  enabled. Linux page cache was not flushed.
- Every cell used fresh databases and verified the same **57 Tidehunter tables,
  167,858 rows**, plus the RocksDB epoch/index/RPC-store state. The existing
  validated legacy-index sequence/digest normalization is not byte identity.
- Both EFA cells required the EFA driver/provider and positive hardware
  send/receive deltas on **all six hosts**. They did not silently use TCP or the
  software-fabric provider. These are host-wide cell counters, including
  startup/audit traffic, not per-RPC wire measurements.

### 20.3 End-to-end paired results

Each row runs native first, then its distributed partner. The second transport
pair has profiling enabled and is **not** an equal-condition second speed
repetition. Native-first is needed for the authoritative schema export.

| Cell | Read profile limit per DB | Native elapsed | Distributed elapsed | Distributed/native |
|---|---:|---:|---:|---:|
| `r01-tcp` | 0 | 6.261450 s | 11.266734 s | 1.7994× |
| `r01-efa` | 0 | 6.526115 s | 10.817430 s | 1.6576× |
| `r02-efa` | 4,096 | 6.239809 s | 10.696400 s | 1.7142× |
| `r02-tcp` | 4,096 | 6.519831 s | 11.297360 s | 1.7328× |

The unprofiled EFA replay took about **4.0% less time than TCP**. The diagnostic
pair has the same direction, but run order, native-control variation and the
250 ms completion-sampling interval prevent a precise general speedup claim.
These elapsed measurements include startup and completion observation; they are
not steady-state throughput or checkpoint service latency.

The earlier §19 run used `i4i.xlarge`, not this hardware. Its 1.342× native/TCP
ratio must not be compared with 1.799× here as an architecture-only regression.
The native control itself is substantially faster on this new fleet.

### 20.4 Application-visible storage latencies

Unprofiled Sui typed-store histogram **means**, computed as `sum / count`.
The historical `rocksdb_*` metric names also cover the selected Tidehunter
backend. Each native column is the control paired with that transport.

| Operation / table | Native, TCP pair | TCP | Native, EFA pair | EFA |
|---|---:|---:|---:|---:|
| Get `running_root_accumulators` | 6.779 µs | 194.744 µs | 6.754 µs | 142.945 µs |
| Get `state_hash_by_checkpoint` | 2.909 µs | 93.883 µs | 2.826 µs | 112.330 µs |
| Get `checkpoint_content` | 8.387 µs | 220.740 µs | 8.623 µs | 185.970 µs |
| Get `certified_checkpoints` | 23.605 µs | 258.508 µs | 23.438 µs | 201.399 µs |
| Get `full_checkpoint_content_v2` | 29.875 µs | 228.912 µs | 29.671 µs | 189.243 µs |
| Get `object_per_epoch_marker_table_v2` | 1.223 µs | 6.658 µs | 1.252 µs | 5.270 µs |
| Get `watermarks` | 8.523 µs | 7.968 µs | 8.150 µs | 3.070 µs |
| Multiget `objects` | 0.462 µs | 8.436 µs | 0.457 µs | 7.823 µs |
| Multiget `unchanged_loaded_runtime_objects` | 1.216 µs | 99.966 µs | 1.141 µs | 156.572 µs |
| Multiget `next_shared_object_versions_v2` | 3.168 µs | 168.232 µs | 3.186 µs | 164.487 µs |
| Batch commit `checkpoint` | 453.687 µs | 642.289 µs | 468.213 µs | 613.670 µs |
| Batch commit `epoch` | 439.108 µs | 646.097 µs | 439.975 µs | 579.946 µs |
| Batch commit `perpetual` | 639.460 µs | 996.953 µs | 637.852 µs | 963.919 µs |

Multiget means include **zero-key calls**; they are not per-object NVMe latency.
EFA is not faster for every table. A table's mean also includes local hits,
absences, coordinator waits and changes in concurrent request scheduling, so
slower `state_hash_by_checkpoint` does not contradict faster sampled EFA RPCs.

Do not label these means p50. Existing histograms only establish enclosing
buckets. For example, in the EFA pair:

| Get | Native p50 bucket | EFA p50 bucket | EFA p99 bucket |
|---|---:|---:|---:|
| `running_root_accumulators` | ≤10 µs | >100–200 µs | >500–1,000 µs |
| `checkpoint_content` | ≤10 µs | >50–100 µs | >1,000–2,000 µs |
| `state_hash_by_checkpoint` | ≤10 µs | ≤10 µs | >400–500 µs |

The last row shows why a slow mean does not establish an equally slow median.
The complete exported CSV contains **408 observed latency series** across all
four cells and both modes, including counts, means and p50/p95/p99 bucket bounds.

### 20.5 Where time accumulated

These are cumulative concurrent spans, not disjoint wall-time fractions.
`owner_wait` includes the separately shown write-owner/read-finish-owner
subsets. Detached read wait overlaps the actor work below. Do not sum the table.

| Remote stage, all namespaces | Unprofiled TCP | Unprofiled EFA |
|---|---:|---:|
| Database-owner acquisition wait | 9.626642 s | 9.062937 s |
| Write-gate wait | 1.959432 s | 1.862247 s |
| Write-owner acquisition wait, subset | 0.016307 s | 0.016901 s |
| Read-finish-owner wait, subset | 0.846887 s | 0.650753 s |
| Read submission | 0.144284 s | 0.137200 s |
| Detached read wait | 1.995196 s | 1.515061 s |
| Read completion work | 0.020428 s | 0.019164 s |
| Read index work | 0.054010 s | 0.053724 s |
| Write validation | 0.395553 s | 0.384345 s |
| Durable storage wait | 11.298722 s | 10.490682 s |
| Write publication | 0.608904 s | 0.602782 s |

The bounded diagnostic pass sampled 4,096 checkpoint, 1,779 epoch and 4,096
perpetual successful read RPCs per transport, with zero sampled errors.

| Namespace | TCP actor service mean | EFA actor service mean | TCP server handler mean | EFA server handler mean | EFA local record-read mean |
|---|---:|---:|---:|---:|---:|
| Checkpoints | 178.203 µs | 132.547 µs | 18.354 µs | 14.706 µs | 9.155 µs |
| Epoch | 174.730 µs | 135.400 µs | 13.460 µs | 12.371 µs | 10.372 µs |
| Perpetual | 73.832 µs | 42.195 µs | 5.148 µs | 4.347 µs | 3.789 µs |

For checkpoint RPCs, EFA additionally measured 18.795 µs actor queue and
16.694 µs actor return means. The exchange mean was 131.512 µs versus
177.187 µs for TCP. **Exchange is not pure network RTT**: framing, progress,
server scheduling and response handling remain inside that boundary. Server
clocks do not establish synchronized one-way network latency.

Profiles select the first N actor requests after admission. They exclude cache
hits, index absences and pre-admission backpressure; startup and scans are
included. Perpetual's 4,149 scan records and 4,151 total read RPCs mean its
profile is largely scan traffic, not representative cold object gets during
checkpoint execution. CQ/sweep measurements can include interleaved unprofiled
work; output queue/submit/retire means require their own sample denominators.

### 20.6 Specific remaining coordination defect and next fixes

The benchmark exposed an existing **empty bulk-read slow path**, not just
unavoidable remote latency:

1. The feature-enabled Sui bulk branch in
   `../sui/crates/typed-store/src/rocks/mod.rs:340–352` calls the distributed
   adapter even for an empty key vector. The native branch at `:354–362` maps
   over keys and performs no database get when that iterator is empty.
2. `tidehunter/src/distributed/sui.rs:361–377` increments call/key counters,
   acquires the remote owner to capture a cut, and constructs a 32-slot pending
   queue **before** discovering that no keys need work.
3. `Db::commit_remote` at `:482–497` retains the same owner across
   `db.write_batch`, including its durable storage wait. Cached-read health
   validation, existence checks and ordinary read admission also acquire it.

In the unprofiled perpetual namespace, `read_calls - read_keys` establishes a
lower bound of **133,225 empty adapter read calls for TCP** and **133,509 for
EFA**. This bound follows because every nonempty counted call contributes at
least one key; batching can only make the bound conservative. Native and remote
call counters are not equal-granularity workload counts because native multiget
dispatches per key. Perpetual owner wait alone accumulated 7.584 s for TCP and
7.332 s for EFA. The counters do **not** identify the exact fraction of that wait
caused by empty calls.

The following are evidence-based next changes, **not implemented or measured
improvements in this campaign**:

1. Add an empty-input fast path before remote ownership/queue setup in bulk
   read entry points. Preserve the defined empty-result/error contract and do
   not remove health validation from real value or cache-hit reads. Validate
   with the real checkpoint workload and inspect owner acquisitions, not just
   a synthetic zero-key microbenchmark.
2. Separate read-only health/index-cut access from the mutable write owner.
   [INFERENCE] The measured wait and lock scope justify this work. Preserve
   whole-batch visibility, publication health, authority fencing, exact
   references, pruning cuts and fail-stop behavior; simply removing the mutex
   is not a correct implementation.
3. After that split, reduce actor handoff/progress cost on genuine misses and
   evaluate post-publication cache admission for checkpoint metadata. Keep the
   new stages so a claimed improvement can be attributed.
4. Retain durable write batching. Any further split of write admission, durable
   wait and publication must preserve ordering and bounded pending memory.
   EFA did not remove storage completion or publication work.

This evidence does not justify a new global physical WAL, NVMe-oF, or weaker
durability as the first fix. It also does not promise a remotely fetched value
will match a compute-local RAM hit.

### 20.7 Resource evidence and limits

No cloud coordinator or storage host reported swap-in/out during the captured
intervals. All sampled foreground-bracket storage-process `read_bytes` deltas
were zero, and the Sui processes reported zero physical read bytes. Together
with zero async-read submissions, this is **not a cold-NVMe read benchmark**.
Logical WAL reads still occurred through resident mappings/page cache.

Across all 15 storage processes, the sample-bracketed foreground CPU totals
were 98.37 CPU-seconds for TCP and 86.97 for EFA. Their charged write-byte deltas
were 2,756,558,848 and 2,761,662,464 respectively. These process counters are
not device bandwidth or precise write amplification; sample brackets can
include boundary work, and the coordinator's writes are separate.

The workload fits ample cluster RAM and exercises a private transfer chain.
It does not prove large working-set behavior, sustained SSD throughput, HA,
failover, physical WAL GC, or production/mainnet parity. No new synthetic
warm/cold-capacity matrix is implied by these real-checkpoint results.

### 20.8 Verified retrieval and closure

AWS MCP worked for IAM operations and signed S3 input uploads. Its EC2 route
failed with `You must specify a region` despite explicit `region_name`, and
later MCP routes returned `Unknown tool`. The authorized boto3 fallback handled
regional EC2, retrieval and cleanup. Parent SSM permissions were not expanded;
the bounded controller used its scoped instance role.

All cells exported before termination was requested at approximately 08:15 UTC.
All **six** instance states were independently confirmed `terminated` at
**2026-10-13T08:22:22.416869Z**. Retrieval preserved **176 objects,
47,377,615 bytes**, with size/hash verification. The controller archive SHA-256
is `f87866aa1d36c5f5353bdaa810f16e74a615393da62aec25d45d617a1a0d2887`.
Offline matrix, source binding, placement, EFA and full-state verification all
passed before deleting the temporary bucket.

Final auxiliary cleanup was verified at **2026-10-13T08:36:32.783679Z**:
no run-owned EBS volumes or ENIs; bucket, security group, placement group, all
six IAM roles and all six instance profiles absent. Four protected unrelated
instances remained running. Shared VPC/subnet resources were not deleted.

The conservative launch-to-final-termination-observation compute bound is
**$2.847859**, using the live verified $2.0144/host-hour rate. This is not an
invoice; auxiliary charges and tax are excluded. The retained cumulative
compute bound is $53.134834, below the $60 experiment ceiling. See the saved
cost ledger and [EC2 on-demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/).

Evidence, relative to the workspace:

- [Source-bound build manifest](../../../checkpoint-work/2026-10-13-efa/build/build-manifest.json).
- [All four offline acceptance checks](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/matrix-verified.json).
- [Unprofiled TCP summary](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/paired-results/r01-tcp/summary.json).
- [Unprofiled EFA summary](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/paired-results/r01-efa/summary.json).
- [Profiled EFA summary](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/paired-results/r02-efa/summary.json).
- [Profiled TCP summary](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/paired-results/r02-tcp/summary.json).
- [All derived timings and resource analysis](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/analysis-details.json).
- [Every observed wrapper latency series](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/all-wrapper-latencies.csv).
- [Retrieved object hashes](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/retrieved-results.json).
- [Instance termination receipt](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/termination-verified.json).
- [Auxiliary cleanup receipt](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/cleanup-verified.json).
- [Conservative cost bound](../../../checkpoint-work/2026-10-13-efa/cloud-ready/th-35862b94cc9d0904/cost-bound.json).

No installed `kvstore` skill was made stale; those skills are absent locally.

