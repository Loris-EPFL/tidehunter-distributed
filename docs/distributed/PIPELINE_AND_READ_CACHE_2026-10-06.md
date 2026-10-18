# Bounded write pipeline and immutable-reference read cache

Date: 6 October 2026. **Implemented and locally validated: 520 feature-library
tests pass, seven ignored; the separate-process campaign passes; all 234 final
benchmark cases and raw distributions are independently verified. Larger write
windows and cached reads improve, but uncached reads and some small-write shapes
still lag. Native parity is not established. No new AWS resources were launched.**

This document records the next architecture slice after the completed
[local/AWS optimization campaign](OPTIMIZATION_AND_CLUSTER_VALIDATION_2026-10-06.md).
The earlier campaign remains the baseline; its measurements do not measure this
slice. No RDMA, NVMe-oF, replication, Sui integration or AWS launch is part of the
local implementation described here.

## 1. Evidence and scope

The previous independent-NVMe run contained 106 verified cases. For its larger
8 KiB/B32/W32 load, the distributed wall time was 254.29 ms, versus approximately
136 ms for matched native persistence. Coordinator preparation/admission recorded
135.17 ms, storage wait 101.99 ms and publication 12.05 ms. These historical stage
names are coarse: preparation is not just hashing, and storage wait is not just
network latency. The run's warm scalar diagnostics attributed 89–96% of elapsed
time to exchange, which includes TLS, sockets, server scheduling and processing.
All recorded node physical-read deltas were zero.

Two mechanisms follow from the source and evidence:

1. Overlap preparation with already-dispatched storage work. The previous remote
   path admitted a complete window, dispatched it, waited for every participating
   node and only then published. This serializes stages that need not all wait.
2. Serve repeated reads from a bounded cache of verified immutable records at the
   publication authority. Remote warm page-cache reads still pay a network exchange;
   a local cache can avoid that exchange when the exact referenced version is present.

Neither mechanism establishes native parity. In particular, a cold miss must still
fetch bytes from another machine. The local laptop has one SSD; local comparisons
test software mechanisms and relative regressions, not independent-NVMe scaling.

## 2. Tidehunter rules this slice preserves

Relevant paper anchors were re-read in the local
[Tidehunter PDF](../../../middleware/misc/tidehunter.pdf): §2.3, §§3.1–3.4 and §4.4.
The current checkout's native implementation is the source authority; the older
correctness inventory contains historical paths/revisions and must not substitute
for this source review.

| Rule | Native anchor | Consequence here |
| --- | --- | --- |
| Values normally remain in their first authoritative WAL location | Paper §2.3; `wal/mod.rs`; `db.rs::write_batch_into_wal` | The cache is disposable RAM, not another durable value log. The pipeline still appends each complete batch to one storage partition. |
| Concurrent work follows unique allocation; allocation alone is not completion | Paper §3.1; `wal/allocator.rs::allocate`; `wal/tracker.rs::WalTracker` | Logical version allocation, dispatch, durability and publication remain separate states. Physical offsets remain node-local. |
| A completion prefix cannot advance across an unfinished earlier allocation | Paper §§3.1/3.3; `wal/tracker.rs::WalTrackerState` | Later durable replies may be retained, but ordered publication waits for every preceding admitted batch. |
| Atomic batches stay complete through append and index publication | Paper §3.1; `db.rs::write_batch_into_wal`; `index/pending_table.rs` | No member is published before the complete receipt and transaction publication callback succeed. |
| Acknowledgement has an explicit durability policy | Paper §3.1 durability discussion; `db.rs::sync` and `wal/mod.rs` | Durable success still waits for native file/namespace persistence on the selected partition and successful publication. Asynchronous native acknowledgements remain a separate benchmark reference. |
| Cached reads preserve exact key and version semantics | Paper §3.2; `db.rs::exists`; native `LargeTable` cache paths | Resolve a published reference first, then cache by that complete reference; keep the full-key comparison for reduced-key collisions. |
| Recovery and retention cannot be inferred from the highest returned position | Paper §§3.3–3.4/4.4; `wal_replay.rs`; `wal/tracker.rs` | Timeout is an unknown outcome, and no live hole-skipping or WAL reclamation is introduced. |

Native cache behavior is an inspiration, not code to copy without examination.
The new distributed cache uses a reference after index resolution so an older
checkpoint version cannot be confused with the latest value for the same key.

## 3. Primary project designs checked

Sources were fetched on 6 October 2026. GitHub `main`/`master` links are moving
references; these are design inspirations rather than claims that local code is a
copy of their implementation or that their benchmark gains transfer here.

### RocksDB: overlapping write stages while preserving order

[Pipelined Write](https://github.com/facebook/rocksdb/wiki/Pipelined-Write) separates
WAL and memtable work for consecutive write groups, allowing the following group
to progress through its WAL stage while the preceding group finishes its memtable
stage. Current
[`WriteThread::ExitAsBatchGroupLeader`](https://github.com/facebook/rocksdb/blob/main/db/write_thread.cc)
explicitly preserves group order when handing writers to the memtable queue.

**Adopt:** independent stages may overlap, but transition ordering remains an
explicit invariant. **Do not adopt:** its LSM, additional WAL, spin timings or
thread counts. Our existing complete Value WAL is already the value authority.

### TiKV: asynchronous storage completion and bounded grouping

The current
[`raftstore/store/async_io/write.rs`](https://github.com/tikv/tikv/blob/master/components/raftstore/src/store/async_io/write.rs)
separates background persistence from the state-machine thread, collects bounded
write work and sends a `Persisted` notification containing a ready number after
storage work. Its worker can coalesce queued work and uses explicit limits and
wait policy to manage grouping.

**Adopt:** exact completion identity, bounded queues, storage completion distinct
from state-machine advancement, and measurement of group count. **Do not adopt:**
Raft, a second value log, or its adaptive delay constants. Our earlier AWS capacity
experiment already showed that larger groups alone can make this workload slower.

### FoundationDB: assigned versions versus committed visibility

The project-maintained
[transaction commit-path description](https://github.com/apple/foundationdb/wiki/Transaction-Commit-Path)
distinguishes assignment of a commit version, durable log acknowledgement and the
committed-version update used for subsequent reads. Its
[recovery design](https://github.com/apple/foundationdb/blob/main/design/recovery-internals.md)
also distinguishes durable versions from known committed versions and fences old
logs during recovery.

**Adopt:** a sequence number is not by itself evidence of durable or visible state.
**Do not adopt:** FoundationDB's transaction resolvers, separate transaction log
architecture or recovery quorum rules. The current backend has one statically
fenced RF1 publication authority and its own complete-inventory recovery contract.

### RocksDB: immutable cache identity and bounded residency

Current [`cache_key.h`](https://github.com/facebook/rocksdb/blob/main/cache/cache_key.h)
combines a file-specific identity with its record/block offset, avoiding ambiguity
from an offset alone. The
[block-cache design](https://github.com/facebook/rocksdb/wiki/Block-Cache)
describes capacity accounting, immutable cached blocks and CLOCK/LRU choices.

**Adopt:** cache immutable identity rather than mutable user-key meaning, track
both data and metadata, and use second-chance eviction. This fork uses a private
four-way fixed table, so its metadata allocation is explicit and its existing
single-owner authority needs no added cache mutex. It does not copy RocksDB's
cache implementation or claim HyperClockCache's concurrency properties.

## 4. Implemented write pipeline

```text
caller prepares/admit batch N+1
          │                     node A appends prior group
          ├── bounded dispatch ─node B appends prior group
          │                     node C appends prior group
          ▼                              │
consume available exact durable replies ◀┘
          │
          ▼
publish only contiguous durable batches
          │
          └── return admission credit for published batches
```

Preparation remains single-owner initially. This first change overlaps it with
storage work; it does not claim parallel CPU preparation. Adding parallel encoding
later must preserve deterministic allocation and bounded admission, and needs its
own evidence because preparation currently includes several different costs.

### Data and ownership

- Keep an immutable `CompleteBatch` in its `Submission` until publication. Exact
  retries and the publication bridge need it even after dispatch.
- Track dispatch progress independently from `PublicationTracker`'s published
  prefix. Dispatch is not permission to release memory or index reservations.
- Retain exact per-submission byte credit. Refund it only once, after publication
  has completed and retained input ownership has been released.
- The append worker must drop its cloned complete-batch vector before sending the
  completion reply. Otherwise a descheduled worker could retain old payload
  ownership after the authority has published, refunded its credit and admitted
  another batch. A reply contains only bounded receipt metadata.
- Bound queued plus in-flight plus durable-unpublished batches and bytes. The old
  expression `submissions.len() - next_dispatch` is no longer a valid total
  outstanding-work bound once dispatch can occur during submission.
- Keep retained-history/index capacity limits separate from transient work credit.
  Published retry identities still count against the configured retained bounds.
- Use the existing one-connection-per-node worker ownership. No global transport
  mutex, new coordinator WAL or per-operation sequencing RPC is required.

### Dispatch and completion

- Dispatch useful per-node groups while later batches are prepared. Keep batches
  whole and within the existing encoded group/frame limits.
- Avoid turning every tiny admission into a separate persistence call. Group count
  is a validation metric, not an assumption; explicit drain flushes all tails.
- Allow a fixed bounded number of commands per node, initially one active append
  with staged pending work. A busy node must not make unbounded queuing possible.
- Poll all available node replies, publish any complete prefix and refill idle
  node workers. Wait for any node completion, not one selected node's reply: even
  the earliest unpublished batch may belong to a stalled node while other nodes
  could process additional staged groups within the existing admission budget.
- Validate exact receipt identity/domain/digest and complete counts before giving
  the durable receipt to `PublicationTracker`.
- Run `publish_ready` after available completions, preserving the native pending
  transaction callback. A completion at sequence 9 cannot publish over sequence 8.
- `drain` captures the admitted frontier, flushes staged tails and waits until that
  complete prefix is published. Its success is the acknowledgement barrier used by
  `write_batch`, checkpoint creation, index flush and shutdown.

### Lone-write refinement after the first complete comparison

When there is exactly one unpublished admission and no active append, there is no
independent node work to overlap. The latest source calls the same protected
`Client::append_many` directly on the caller, then accepts its durable receipt and
publishes the same prefix. It avoids actor request/completion handoffs for this
case while preserving the TCP/TLS request, storage barriers and receipt checking.
The `direct_appends` counter makes use of this path visible.

`healthy` becomes false before issuing I/O and is restored only after successful
publication. An error retains admission identity and credit and requires recovery.
The staged batch remains owned until publication; this path creates no actor
clone. The disabled-pipeline control retains actor dispatch. The earlier complete
campaign exposed a small-window regression and motivated this targeted change;
the final matched measurements including this refinement are reported in §9.

### Completion-driven wakeup refinement

A channel of wake tokens, with capacity equal to the number of node workers, is
sufficient; the existing individual reply receivers remain the source of truth.
For each append, the worker first sends its exact result to that request's reply
channel, then uses a nonblocking `try_send(())` on the wake channel. The owner runs
the complete poll/publish/refill pass before each wait. A 100 ms wait timeout only
triggers another poll, allowing disconnected reply receivers to be detected even
if a panicking worker did not notify; it never turns an operation into an abort.

This does not lose completions: a successful wake send leaves a token after its
reply is available; a full wake channel already contains a token that will cause
another poll. A stale token can produce an extra pass but cannot create a receipt.
At most one append is in flight per node, and tokens are bounded by node count.
The worker must never block sending a wake token, or shutdown and refill could
depend on the owner consuming a redundant notification. The earliest version is
retained for unknown-outcome diagnostics, not used to choose which node may wake
the pipeline.

This refinement removes a scheduling dependency; ordered publication still waits
for the earliest missing durable batch. Refilling other nodes cannot release
credit for durable batches held behind that gap, so a prolonged stall eventually
reaches the existing bounded backpressure limit.

### API and failure contract

`submit` becomes permitted to start storage work. Its returned `BatchVersion`
means admission; it does not acknowledge durable application success. A request
may have storage effects before the caller invokes `drain`. Document this change
explicitly and retain the exact operation ID for retry after ambiguity.

Regular reads currently observe published state. They do not promise to include
undrained submissions; this slice should preserve that contract. Reads may see an
earlier successfully published prefix while later submitted batches are pending.
Checkpoint creation, index flush and shutdown retain their explicit drain barrier.

On a transport or receipt failure, a node may already have persisted some groups.
The safe first implementation poisons the authority and requires recovery, while
retaining admitted identities. It must not clear credit and continue, infer an
abort from timeout, or allocate a replacement identity. A failed or panicking
publication callback also keeps the authority unavailable to all reads, including
cache hits. Shutdown still joins workers before releasing the host authority lock;
pending append work cannot outlive that lock.

## 5. Implemented read cache

Keep a bounded volatile cache at the index/publication authority. Look up the
current or checkpoint index first. Use its complete `ValueRecordReference` as the
cache identity: database/epoch/batch identity, mutation version, physical address
and member descriptor must not collapse into a user key or a bare WAL offset.

- Cache only records obtained from a verified immutable batch/reference. Do not
  admit an unverified network response, failed read or speculative incomplete batch.
- A new published overwrite resolves to another reference; a tombstone resolves
  through the index. Old cached data therefore cannot answer the new reference.
- A checkpoint resolves its own historical reference and may reuse that exact
  cached record. No latest-value shortcut may bypass the checkpoint cut.
- Preserve full-key comparison for reduced-key collisions. Do not introduce
  negative caching in this slice.
- Detach retained key/value storage from large network/batch buffers, or charge
  the complete backing allocation. An apparent 1 KiB entry must not pin an
  unaccounted multi-MiB transport frame.
- Bound retained data and metadata, support a zero-capacity uncached mode and
  skip individually oversized entries. Eviction never changes durable state.
- Health/fencing checks run before cache lookup; cache hits cannot revive a
  failed or closed authority. Cache data need not survive recovery.

Read-through caching is a useful first boundary. Priming from already-published
write batches is optional and must be measured separately; otherwise a fully
resident benchmark can hide all network misses without making the miss path faster.

The implemented first slice uses read-through fill only, an 8 MiB default and zero
to disable. Scans may reuse hits but do not fill the cache, preventing a one-pass
scan from evicting the working set. The benchmark adds a separate repeated hot-read
pass and clears the cache before its unique-key remote diagnostic pass.

This adds RAM residency, not a guarantee that every workload fits in memory. The
budget must be explicit in comparisons, including any native value-cache budget.
The current benchmark's native reference configures `value_cache_size = 0`, while
the new cached TCP case adds an 8 MiB application cache. A cache-hot comparison is
therefore an explicit memory-policy tradeoff; it is not equal-memory proof of
native parity. Uncached TCP and pipeline-only controls remain required to expose
transport and scheduling performance independently of cache hits.

## 6. Code ownership and required changes

| Area | Files in this fork | Expected change |
| --- | --- | --- |
| Authority pipeline | `tidehunter/src/distributed/remote_db.rs` | Submission credit, staged/in-flight ownership, completion polling, ordered incremental publication, drain barrier |
| Correctness model | `distributed/publication.rs`, `native_publication.rs`, `native_index.rs` | Reuse existing separation and atomic callback; change only if a demonstrated integration need appears |
| Cache | `distributed/remote_db/read_cache.rs` and `remote_db.rs` integration | Bounded exact-reference cache; all read APIs retain key/checkpoint/health semantics |
| Timing | `distributed/local_db.rs` stats and benchmark support as needed | Pipeline dispatch/completion/credit and cache hit/miss/retained-byte evidence; ordinary clocks separate from detailed diagnostics |
| Regression coverage | `distributed/remote_db/tests.rs` and cache/pipeline unit tests | Deterministic ordering, ambiguity and memory-bound tests |
| Benchmark | `examples/sharded_wal_bench.rs` and support | Fresh binary/source identity, pipeline disabled/enabled and cache cold/warm/disabled cases |
| Remote attribution | `distributed/tcp.rs` and the separate profiled-read wire request | Extra diagnostic clocks for node handler, inventory lock wait, authority lookup and record read; ordinary read timing remains separate |
| Native Tidehunter | Existing native engine files | No planned alteration to native hashes, CRC, formats or WAL semantics |
| Old middleware / Sui | Other repositories | No change required for this local experimental-backend slice |

## 7. Local acceptance gates

1. Force a later node to finish first; no index visibility may pass an unresolved
   earlier batch. Then complete the earlier batch and verify exact ordered values.
2. Prove dispatch starts before explicit drain and can overlap later preparation.
   Measure full load wall time so moving work into `submit` cannot fake a speedup.
3. Exercise capacity while work is queued, in flight and durable behind a hole.
   Verify exact retries at capacity and changed-payload retry rejection.
4. Lose a reply after durable append; ensure all cached and uncached serving fails
   closed until recovery. Verify no dropped success, identity reuse or live skip.
5. Check complete batch visibility, repeated-key ordering, deletes, checkpoint
   reads, full-key collisions, scans, flush/unload and reopen audits.
6. Verify cache hits, misses, eviction, oversized-record bypass and the retained
   backing-buffer budget. Validate cold and warmed reads separately.
7. Verify shutdown with pending work and failures; no worker survives authority
   lock release. Run separate-process crash/recovery with actual TCP/TLS nodes.
8. Build release binaries and compare matched local baseline/candidate runs using
   five processes. Retain source/binary hashes, all timings, group counts, cache
   counters and memory/swap deltas. Small bounded data suffices for these gates.

The expected write effect is overlap of stages, so old additive attribution is
not directly comparable to summed concurrent worker times. Report wall time,
preparation CPU time, actual blocking wait and publication time with their nesting
and overlap stated. Successful pipelining must not mean weaker acknowledgement.

## 8. Latest source and correctness evidence

The latest executable includes bounded completion-driven pipelining, O(1) staged
readiness checks, direct lone writes and the verified-reference cache. Preparation
is sequential on the caller and overlaps dispatched storage work. Admission credit
includes the worker's second metadata vector and is held until publication; the
worker drops its value-owning batch vector before sending completion. Observed
uncached transport/authority errors invalidate cached serving as well.

| Artifact | SHA-256 |
| --- | --- |
| Latest source tree | `c56f5c929bee8feeef0def74c4c07650fba3c661d74c4604d5e3d4d273c38fbe` |
| Latest release executable | `0fc431a21430f22b8e05a7b1e0fe5b516a3c03aea45543b4853e2dfab2f9eb04` |

The [source/build manifest](../../logs/pipeline-cache-2026-10-06/candidate-direct/source-manifest.json)
binds the exact sources and executable. Against these sources:

- The full feature library suite passes **520 tests, seven ignored**:
  [library log](../../logs/pipeline-cache-2026-10-06/library-direct.log).
- The full separate-process crash/recovery/fencing/mTLS campaign passes:
  [completion record](../../logs/pipeline-cache-2026-10-06/process-direct/COMPLETE).
- All **234 matched cases and their raw distributions are independently verified**:
  [analysis completion](../../logs/pipeline-cache-2026-10-06/analysis-direct/COMPLETE).
  Final measurements are reported below.
- No new AWS, RDMA or NVMe-oF run is part of this slice.

### Preserved iteration evidence

Earlier `candidate-final` and `candidate-verified` builds and their measurements
remain unchanged. The latter includes the worker ownership fix but precedes direct
lone writes; its binary hash is
`67ba9f4cdf91246aaefffb7756e2ffbdd94530421b0f83f95fe46d6c47f23386`.
Its [519-test suite](../../logs/pipeline-cache-2026-10-06/library-verified.log),
[process campaign](../../logs/pipeline-cache-2026-10-06/process-verified/COMPLETE)
and [234-case analysis](../../logs/pipeline-cache-2026-10-06/analysis-verified/analysis.json)
are historical checkpoints, not substitutes for the latest build's gates.

An earlier run failed the unchanged native
`relocation::relocation_tests::test_watermark_highest_wal_position_tracking`
assertion (0 versus 1200):
[retained failure](../../logs/pipeline-cache-2026-10-06/library-test-final.log).
Its [focused recheck](../../logs/pipeline-cache-2026-10-06/native-watermark-recheck.log)
and successive full reruns passed. The isolated failure is not described as fixed
by this distributed change. Source review and exercised tests are not a formal
proof or evidence of full native/Sui production parity.

## 9. Final matched local measurements, including direct lone writes

**The final candidate improves larger write windows and resident read hits, but
uncached reads and some small-write shapes still lag.** All results in this section
come from `candidate-direct`, whose source/binary hashes are recorded in §8.
Earlier `comparison`, `comparison-final` and `comparison-verified` runs remain
immutable development history; they are not mixed with this final campaign.

The [completed comparison](../../logs/pipeline-cache-2026-10-06/comparison-direct/COMPLETE)
contains **234 cases**: 216 main cases and 18 separately labeled hot-read cases.
The main matrix uses 1/8 KiB values, batch sizes 1/32, windows 1/8/32 and three
repetitions with six configurations:

| Configuration | Executable | Write scheduling | Read cache |
| --- | --- | --- | --- |
| Previous TCP | Archived previous optimization candidate | Previous windowed path | None |
| Current TCP control | Final direct candidate | Pipeline disabled, actor dispatch | None |
| Pipeline TCP | Final direct candidate | Pipeline and direct lone writes enabled | None |
| Pipeline + cache TCP | Final direct candidate | Pipeline and direct lone writes enabled | 8 MiB |
| Previous native | Archived previous optimization candidate | Native | Native application value cache disabled |
| Current native | Final direct candidate | Native | Native application value cache disabled |

Each main case loads 128 batches, performs 32 mixed batches and 512 point-read
samples, along with the checkpoint, delete, scan, flush and reopen verification
phases. The separate hot trace repeats the point-read pass for 1/8 KiB/B32/W32 under
current TCP control, pipeline-plus-cache and current native. Its 18 cases are not
pooled with the ordinary point-read trace.

The [independent analysis](../../logs/pipeline-cache-2026-10-06/analysis-direct/analysis.json)
recomputed distributions and totals for **1,292,676 samples and 4,776 phase rows**.
Every case passed full-value and reopen-oracle checks. All **780 node process IDs**
were verified gone. System swap-in and swap-out deltas were zero. Peak sampled
aggregate process-tree RSS was **230,956 KiB (225.54 MiB)**; 50 ms sampling is not
an exact physical-memory peak, and shared pages may count once per process.

The raw samples, all phase timings and tails, resource observations and source
manifests are retained. See the [full comparison CSV](../../logs/pipeline-cache-2026-10-06/analysis-direct/comparison.csv),
[per-configuration medians](../../logs/pipeline-cache-2026-10-06/analysis-direct/medians.csv)
and [stage data](../../logs/pipeline-cache-2026-10-06/analysis-direct/stages.csv).

### Representative 8 KiB / batch 32 / window 32

Each value is the median of three per-run measurements. A point or multi-get p50
is the median of the three run p50s, not a pooled percentile. Lower is better.

| Metric | Previous TCP | Current TCP control | Pipeline only | Pipeline + cache | Current native |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load wall, ms | 109.867 | 108.083 | **85.358** | 90.247 | 40.555 |
| Ordinary point-read p50, µs | 25.002 | 24.513 | 24.025 | 31.722 | **1.878** |
| Multi-get p50, µs | 284.112 | 344.433 | 316.611 | **27.778** | 56.554 |
| Scan wall, ms | 20.224 | 22.112 | 20.604 | 21.596 | **7.125** |
| Reopen wall, ms | 274.265 | 258.867 | 265.380 | 260.234 | **1.444** |

Pipeline-only load is **21.0% faster than its disabled same-binary control**.
The combined case is **17.9% faster than archived previous TCP**, while remaining
approximately **2.23× current native**. The cache does not alter WAL persistence
and is not write-primed, so differences between the two pipeline write rows must
not be attributed to cache hits. Previous native load was 35.750 ms here, versus
40.555 ms for current native; controls must be considered alongside speed ratios.

Variation is material: combined load ranges **82.81–502.57 ms** across three runs,
including a large outlier. Pipeline-only ranges **81.95–90.16 ms**. Medians alone
do not establish tail stability or statistical significance.

| Large-window load stage | Control | Pipeline only | Pipeline + cache |
| --- | ---: | ---: | ---: |
| Preparation/admission | 47.110 ms | 53.325 ms | 52.511 ms |
| Blocking storage wait | 56.103 ms | 27.588 ms | 26.525 ms |
| Publication | 3.932 ms | 4.749 ms | 5.431 ms |
| Durable groups / persistence barriers | 48 / 48 | 48 / 48 | 48 / 48 |
| Actor append commands | 20 | 48 | 48 |
| Early dispatches | 0 | 32 | 34 |
| Completion consumptions before drain | 0 | 14 | 16 |
| Direct lone appends | 0 | 0 | 0 |

The old control can hand several native groups to one actor command, whereas the
pipeline refills one bounded group at a time; the command count is not a count of
extra physical persistence barriers. The unchanged 48 groups/barriers and reduced
blocking wait support real overlap. Preparation itself did not become faster.
Per-stage medians need not sum to the median phase wall time.

### Across the twelve main shapes

These are unweighted medians and ranges of per-shape ratios, using the median of
three repetitions for each numerator and denominator. They are descriptive.

| Metric: pipeline + cache / previous TCP | Median ratio | Shape range |
| --- | ---: | ---: |
| Load wall | 0.916× | 0.594–1.142× |
| Ordinary point-read p50 | 0.531× | 0.014–1.279× |
| Multi-get p50 | 0.082× | 0.017–0.144× |
| Scan wall | 0.589× | 0.063–1.281× |
| Reopen wall | 0.999× | 0.856–1.232× |

Pipeline-only/control load has median **0.911×**, range **0.790–1.117×**;
nine of twelve shapes improve. Combined/previous TCP load also improves in nine
of twelve shapes. The main point-read trace contains repeated keys: cached B1
cases have **75.39% hits**, while B32 cases have **6.45% hits**. The aggregate
point-read improvement therefore does not mean remote cache misses are twice as
fast. Multi-get runs after point reads and can reuse the populated cache.

### Lone-write refinement: mechanism verified, residual variability remains

The preceding pre-direct campaign had a 60.8% pipeline/control regression at
1 KiB/B1/W1 and motivated removal of the actor handoffs for one outstanding batch.
That older result remains in [analysis-verified](../../logs/pipeline-cache-2026-10-06/analysis-verified/analysis.json).
The latest matched campaign is:

| 1 KiB / batch 1 / window 1 | Previous TCP | Control | Pipeline only | Pipeline + cache | Native |
| --- | ---: | ---: | ---: | ---: | ---: |
| Load wall, ms | 112.279 | 113.465 | 126.695 | **98.319** | 70.491 |
| Direct lone appends | 0 | 0 | 128 | 128 | Not applicable |

All TCP modes still execute **128 complete groups and 128 persistence barriers**.
Every repetition of the two enabled modes uses 128 direct appends, proving the
handoffs were removed. Combined load is 12.4% faster than previous TCP, but
pipeline-only remains **11.7% slower than same-binary control**, with a wide
**91.42–527.60 ms** run range. At 1 KiB/B1/W8, combined load remains **14.2% slower**
than previous TCP. These regressions are not erased from the report.

Native load controls have median current/previous ratio **1.000×**, with range
**0.840–1.604×** across shapes. Host variance exists, but it is not proof that all
remaining regressions are noise. The new architecture has not demonstrated a
universal write improvement or stable tail latency.

### Repeated cache-hot reads and memory-policy qualification

All **3,072 requests** in the separately measured hot cached passes hit the cache.

| Hot scalar p50 | 1 KiB | 8 KiB |
| --- | ---: | ---: |
| Current TCP control, uncached | 17.360 µs | 24.494 µs |
| Pipeline + 8 MiB cache | **0.585 µs** | **0.695 µs** |
| Current native, application value cache disabled | 0.598 µs | 1.592 µs |

This demonstrates that a resident exact reference avoids remote exchange. It
uses extra application-cache RAM and does not prove equal-memory native parity.
The native reference has `value_cache_size = 0`; its OS page cache remains active.
The mostly-miss 8 KiB/B32/W32 ordinary point-read p50 actually regresses from
25.002 to 31.722 µs in the combined configuration. Cache misses are still remote,
and this campaign does not measure cold-device storage scaling.

## 10. Remaining measured costs and next decision

The latest candidate's separate cache-cleared, unique-key diagnostics contain
3,168 reads per value size and give the following stage means:

| Nested diagnostic mean | 1 KiB | 8 KiB |
| --- | ---: | ---: |
| Whole profiled call | 21.478 µs | 38.974 µs |
| Client exchange | 17.562 µs | 26.881 µs |
| Server handler, inside exchange | 1.017 µs | 1.971 µs |
| Server inventory-mutex wait, inside handler | 0.0337 µs | 0.0373 µs |
| Server authority lookup, inside handler | 0.1068 µs | 0.1148 µs |
| Server record read, inside handler | 0.7611 µs | 1.701 µs |
| Client record verification, outside exchange | 1.490 µs | 9.175 µs |

See [profile data](../../logs/pipeline-cache-2026-10-06/analysis-direct/profiles.json)
and [per-shape profiles](../../logs/pipeline-cache-2026-10-06/analysis-direct/profile_shapes.csv).
These nested rows must not be summed. Exchange includes TLS, sockets, scheduling
and network movement outside the timed handler; its remainder cannot be labeled
pure wire latency. The measurements do not identify inventory-mutex contention as
the principal warm-read cost.

The pipeline removes an avoidable scheduling dependency and direct lone writes
remove unnecessary actor handoffs. CPU preparation, remote cache misses and
small-window persistence variability still cost time. Next performance work should
separate preparation's encoding, hashing, validation and allocation costs, preserve
integrity checking, and investigate the remaining small-window/tail variation.
An independent-NVMe retest can establish whether overlap transfers to the cluster,
using these exact frozen sources and explicit equal or separately labeled cache
budgets. Cache misses and cold storage need their own measurements.

RDMA and NVMe-oF remain separate later experiments. Neither was required to
implement these architectural corrections, and this local evidence does not prove
that either transport would close the remaining gap.
