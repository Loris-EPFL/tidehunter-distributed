# Tidehunter correctness rules and distributed storage alternatives

Date: 6 October 2026. **Correctness inventory and alternatives review.**

Later on the same date: [worker hierarchy and distributed Value-WAL decision](distributed_tidehunter_architecture_decision_2026-10-06.md).
With substantial native changes now allowed, that document recommends investigating
a native distributed-storage fork with one active publication authority per DB.
This inventory's correctness rules and open source concerns still apply; no candidate
has established production readiness or performance parity.

This review covers all 14 pages of the local
[Tidehunter paper](../misc/tidehunter.pdf), including its evaluation and references,
and the native engine's allocation, batch publication, read, replay, index flush,
snapshot, relocation and reclamation paths. It is not an exhaustive audit or a
formal proof of every engine feature. External sources were checked on this date.
No implementation, benchmark, dependency installation or AWS launch accompanies it.

## 1. Document chronology and authority

Filename dates identify the design generation. Modification time alone does not:
the October 4 design was edited on October 6 to add a pointer. A recent report can
also describe an older executable. Use its recorded build identity and scope.

| Document | Relevance now |
| --- | --- |
| This review, October 6 | Current correctness inventory and comparison of independent WALs, shared logs, remote block storage and LSM alternatives. Supersedes categorical rejection or selection of these alternatives in earlier plans. |
| [Native parity plan, October 6](native_parity_design_plan_2026-10-06.md) | Detailed **candidate A** below: improve partitioned owners and possibly parallel durable staging. The staging protocol remains unimplemented and unproven. |
| [Operation timings, October 6](local_operation_timings_2026-10-06.md) | Latest timing reduction, from the **October 5** executable; not a new October 6 replay. Source of the local measurements below. |
| [Remaining work, October 5](local_remaining_work_2026-10-05.md) | Current implementation record: prepare coverage, restricted scalar overtaking and opt-in shared fabric progress. Read this before older implementation checklists. |
| [Performance design, October 4](distributed_performance_design_2026-10-04.md) | Historical rationale and source review. Its independent-WAL recommendation is reopened here. |
| [Cloud results, October 4](publication_pipeline_cloud_results_2026-10-04.md), [EFA follow-up](publication_pipeline_efa_queue_fix_results_2026-10-04.md) | Latest relevant independent-NVMe measurements; their binaries predate the October 5 local changes. Preserve their evidence and qualifications. |
| [Coverage implementation, October 4](local_coverage_progress_2026-10-04.md) | Historical status; October 5 implements some items marked unfinished here. |
| [September 30 plan](distributed_performance_plan_2026-09-30.md), [September 26 NVMe design](distributed_nvme_architecture_2026-09-26.md) | Earlier workload evidence and design history. Their timings, completion status and selected architecture are not the current baseline. |

The current native checkout is `0c9ec5b4266a716fbac14cb12da97744e9a5e08b`.
Its two project commits above `ce37b18` add explicit durable synchronization,
retained/prepared batches and reference publication, among other changes. These
extensions must not be attributed to the paper. The pre-extension checkout
already has the per-cell L0/L1 index described below. It is a reference checkout,
not a claim that every line matches the paper's evaluated revision.

Middleware HEAD is `1969983bdd1244ef940686da824f48c35442b72f`, with substantial
existing working-tree changes. HEAD alone does not identify the current code or
any benchmark binary. Sui HEAD is `c79040af6c06417245e24574246ada38a99d8b30`.

## 2. What the paper establishes

The paper's design separates values from index maintenance. Values are appended
to a Value WAL and normally remain there; the small index stores their positions.
Writers reserve space atomically and can fill disjoint reservations concurrently.
Index cells have separate synchronization. A native atomic batch can reserve one
contiguous WAL region even when it affects many cells or keyspaces (§§2–3).

Ordinary foreground writes do not wait for stable-storage synchronization. The
paper distinguishes application-process failure from kernel/power failure. Its
fast append results must not be used as a durable-acknowledgement baseline without
matching that policy. Our paired durable benchmark uses the checkout's added
sync API on both sides.

The paper does **not** establish distributed Tidehunter performance. Its Sui
deployment consists of validators using local engines, not one engine whose
internal cells span all validator machines. Its single-roundtrip persisted-index
lookup describes storage access, not an end-to-end network operation.

The evaluation also does not establish universal superiority over LSM engines:
its large-value workload favors Tidehunter, while the small-value experiments
include cases favoring LSM stores (§6). Sui control records and large objects
therefore deserve separate workload analysis.

## 3. Native correctness inventory

These are requirements for a replacement to preserve, with the implementation
qualifications stated explicitly. A shared WAL, independent WALs, RPC, RDMA and
NVMe-oF can each implement some of these rules; none supplies all of them alone.

### 3.1 Allocation, records and ordering

1. **Unique allocation.** Concurrent writers must not overlap live reservations.
   Alignment, fragment boundaries and record-size limits remain valid.
   [Allocator](../../tidehunter/tidehunter/src/wal/allocator.rs) and
   [WAL `multi_write`](../../tidehunter/tidehunter/src/wal/mod.rs).
2. **Recognizable complete records and batches.** Recovery validates framing and
   checksums and recognizes a complete batch before applying it. It must not expose
   the valid first half of a torn batch. CRC detects corruption; it is not
   authentication and does not repair arbitrary damaged storage.
   [Replay](../../tidehunter/tidehunter/src/wal_replay.rs).
3. **Same-key ordering.** A late index update from an earlier write must not replace
   a newer write or resurrect a deletion. Native code uses WAL offsets in
   `skip_stale_update`. If placement changes, a physical address and a logical
   mutation version may need separate representations. Lexicographically comparing
   independent `(shard, offset)` pairs does not establish correct write order.
   [Large Table](../../tidehunter/tidehunter/src/large_table.rs).
4. **Atomic publication.** Native batch operations share a transaction status;
   pending entries become eligible together when it commits. A distributed batch
   needs an equivalent logical outcome and a read protocol that respects it.
   This does not make arbitrary sequences of separate `get` calls one snapshot or
   promise SQL transaction isolation.
   [Batch implementation](../../tidehunter/tidehunter/src/db.rs),
   [pending table](../../tidehunter/tidehunter/src/index/pending_table.rs).

### 3.2 Completion, durability and recovery

5. **Completed prefix, not maximum offset.** `last_processed` advances through
   consecutively completed allocations. WAL guards cover the append/index work;
   a latch waits for the relevant prefix. A later completed reservation cannot
   hide an unfinished earlier one. This prefix is not itself a durability proof.
   [Tracker](../../tidehunter/tidehunter/src/wal/tracker.rs).
6. **Explicit acknowledgement policy.** For the current durable comparison, success
   requires sufficient stable evidence to recover the complete operation. A network
   receipt, RDMA completion or remote RAM copy does not establish this. Sync errors
   cannot produce durable success. The added `Db::sync` waits for a completed
   prefix, syncs its WAL storage and handles namespace persistence.
   [DB sync](../../tidehunter/tidehunter/src/db.rs),
   [WAL sync](../../tidehunter/tidehunter/src/wal/mod.rs).
7. **Recoverable metadata ordering.** Persist the index/value state required by a
   new control record before making that record the recovery authority. Old state
   remains available until the new recovery state is safe. Current snapshot code
   synchronizes index and value storage, stores the control region, then permits
   reclamation. [Control](../../tidehunter/tidehunter/src/control.rs),
   [`rebuild_control_region_from`](../../tidehunter/tidehunter/src/db.rs).
8. **Replay before serving.** Reconstruct from a valid control snapshot and the
   required WAL suffix before exposing the database. A distributed implementation
   must additionally reconcile ambiguous commits and fence stale writers. A timeout
   leaves the outcome unknown until recovery establishes it; it is not an abort.
   [Open/replay](../../tidehunter/tidehunter/src/db.rs),
   [exclusive native lock](../../tidehunter/tidehunter/src/lock.rs).
9. **Separate authority from derived state.** Our retained/prepared extension keeps
   prepared values hidden, retains their source fragments and later publishes
   references. Retirement must preserve both value bytes and the evidence of their
   committed outcome. Prepared/appended/applied/durable/retired are different states.
   [Retained log](../../tidehunter/tidehunter/src/retained_log.rs).

### 3.3 Reads, indexes, relocation and reclamation

10. **Correct key identity and freshness.** Reduced/hash index keys may require
    verification of the full key. Positive and negative caches must respect writes,
    deletes and publication. The suspected refill issue in §4 remains open.
    [Read API](../../tidehunter/tidehunter/src/db.rs).
11. **Newest index level wins.** In this checkout, per-cell L0/L1 merges must preserve
    newer entries and retain tombstones while deeper levels can contain old values.
    Flushes must keep concurrent/unprocessed updates in the overlay. A distributed
    LSM needs the same rules across its placement and version metadata.
    [Flusher](../../tidehunter/tidehunter/src/flusher.rs),
    [index levels](../../tidehunter/tidehunter/src/index/levels.rs).
12. **Relocation does not create a new user version.** Read a value from a defined
    view; install its relocated pointer only if doing so cannot overwrite a newer
    mutation. The code uses the same `effective_limit` for its index view and
    relocation update threshold. Relocated records also have special replay
    treatment. Copying an old value to a higher offset must not make it logically
    newer. [Relocation](../../tidehunter/tidehunter/src/relocation/mod.rs),
    [conditional updates](../../tidehunter/tidehunter/src/relocation/updates.rs),
    [replay](../../tidehunter/tidehunter/src/wal_replay.rs).
13. **Reclamation preserves every supported reference.** Index/control recovery
    positions, retained preparations and published value references constrain GC.
    Native readers holding a file/mapping reference have a lifetime mechanism;
    remote readers need an explicit equivalent before memory or addresses are
    reused. Merely appending a newer copy is not permission to erase the old one.
    [Watermark](../../tidehunter/tidehunter/src/relocation/watermark.rs),
    [mapper deletion](../../tidehunter/tidehunter/src/wal/mapper.rs),
    [retention bound](../../tidehunter/tidehunter/src/retained_log.rs).
14. **State snapshot scope precisely.** The current `Db::checkpoint` pins an index
    frontier; its documentation says it does not itself retain every WAL file and
    is intended for short-lived reads. Cell dropping has further restrictions.
    Do not advertise unlimited historical snapshots from that API. A vector of
    independently taken owner snapshots is also not automatically a coherent
    cross-owner batch cut. [Checkpoint API](../../tidehunter/tidehunter/src/db.rs),
    [iterator](../../tidehunter/tidehunter/src/iterators/db_iterator.rs).

The contract is scoped to each logical DB exposed to Sui. Native Tidehunter does
not establish Byzantine protection, always-available partitioned operation, or
survival of permanent loss of its only disk. Distributed failover adds requirements:
durable authority and data replication, fencing, and verified recovery frontiers.
Async replication can lag acknowledged primary writes. It must not be described
as synchronous quorum durability.

Transport changes must preserve the middleware's configured authentication and
confidentiality as well as data correctness. Native embedded-engine trust does
not authorize arbitrary remote memory access.

## 4. Native source concerns that require resolution

### Possible stale value-cache refill: source finding, not reproduced

In [`Db::get`](../../tidehunter/tidehunter/src/db.rs), the engine can obtain an old
WAL position, release the index lock, read the value, then call `update_lru`.
[`LargeTable::update_lru`](../../tidehunter/tidehunter/src/large_table.rs) inserts
the value without receiving or rechecking its original WAL position/version.
The normal read path consults that cache before the main index.

A concerning schedule is: reader obtains old position P; a scalar writer finishes
an overwrite Q and updates the cache; the old reader then refills the cache with
P's value; a later reader obtains the stale cached value. A delete has a similar
possible refill problem. The older overlapping reader may legally return P;
poisoning the cache for a later read is the concern.

This path exists before the two project extension commits. A deterministic
interleaving test and audit of the affected cache configurations are needed before
asserting strict read-after-completed-write behavior for those configurations.
No reproduction, fix or failure-rate claim is made here. Treat read freshness as
an intended requirement with this implementation question open.

### Configuration limits, not demonstrated failures in our benchmark

- The WAL-scanning relocation path rejects enabled batch compression in the
  inspected source. Compression and relocation configuration must be checked
  together; compression is not GC.
- `flusher.rs` explicitly rejects `RelocationUpdates` on internally split/sharded
  cells in that path. Do not assume every combination of index splitting and
  relocation is supported merely because each feature exists.
- The flusher also contains a TODO about applying compactor results to a loaded
  in-memory index. Its affected behavior needs a focused audit before depending on
  application pruning semantics in a new design. This review does not establish
  a reproduced data-loss defect from that TODO.

## 5. Does Tidehunter compact the WAL?

**Yes, it has live-value relocation and WAL-file reclamation.** It does not normally
rewrite every large value through repeated sorted levels. The paper's “minimal
relocation” objective is compatible with occasionally moving surviving values
out of old files to reclaim space (§4.4).

Distinguish three operations:

| Operation | What moves | Purpose |
| --- | --- | --- |
| Index flush/merge/compaction | Keys, tombstones and WAL pointers | Maintain efficient lookup and remove obsolete index state |
| Value-WAL relocation / segment cleaning | Still-needed values from reclaimable regions | Recover capacity while preserving references and ordering |
| Batch compression | Encoded bytes within a batch | Save bytes; it does not decide liveness or release old segments |

Reclamation can be delayed while space permits because it is not required to
keep values out of a growing tower of sorted tables. Finite capacity still requires
eventual progress. Copying live values consumes read/write bandwidth and can compete
with foreground work. The paper's measured relocation cost is workload-specific.

A **physically sharded, cleaned value log** is therefore a relevant candidate.
The hard part is deciding who can prove an old segment is unreachable. A proposed
safe sequence is: retain source → copy live values → persist destination → persist
new mappings and recovery authority → retire old reader capabilities and references
→ reclaim. This sequence is an obligation to prove, not an implemented protocol.

An index shard that lags, an unresolved transaction, a replica needing catch-up, or
a remote reader with an outstanding lease can keep a segment live. Blindly retaining
only the latest record per key can discard required tombstones, batch outcomes,
prepared authority or snapshot state. Grouping data by retention lifetime could
reduce copying, but Sui's actual pruning rules must permit the grouping.

## 6. Is a sharded LSM applicable?

**Yes. “Sharded LSM” leaves two separate choices: what is sorted, and what is
distributed.** The current native checkout already has a small L0/L1 LSM-style
**index per cell**, with values in the Value WAL. These cells are within one native
DB; they are not independently committing network machines.
[Native README](../../tidehunter/README.md),
[index representation](../../tidehunter/tidehunter/src/index/levels.rs).

| Candidate | Relation to Tidehunter | Potential benefit | Remaining issue |
| --- | --- | --- | --- |
| Tune existing per-cell LSM indexes | Already present in the reference checkout | Reduce index rewrite/read cost where those costs dominate | Does not remove distributed batch barriers |
| Shard an index over shared immutable value storage | Keeps key/value separation; changes placement and recovery | Independent index work, possibly smaller local indexes | Remote pointer lifetime, atomic publication, version order, GC and remote lookup cost |
| Full key/value LSM engine per machine | Replaces the native storage engine beneath the distributed API | May suit small values, ordered access or mature transaction facilities | Large values may be rewritten during compaction; cross-machine atomicity remains |
| LSM with separated blob/value files | Related to Tidehunter's goals, but not identical to WAL-as-values | Smaller sorted structures and reduced large-value compaction | Initial WAL/blob writes, GC, recovery and distributed commit still need evaluation |

WiscKey is an academic precedent for separating values from an LSM index.
RocksDB's integrated BlobDB implements a related separation and garbage collection.
Its blob builder writes values and encodes file/offset references. These are useful
sources for index/value lifetime handling; they do not establish a drop-in Tidehunter
replacement with identical write amplification or transaction semantics.
[WiscKey, FAST 2016](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu),
[integrated BlobDB design](https://github.com/facebook/rocksdb/wiki/BlobDB),
[blob builder](https://github.com/facebook/rocksdb/blob/main/db/blob/blob_file_builder.cc).

For this workload, replacing the index format is not yet justified by the measured
gap: coordinator barriers and publication dependencies are already substantial.
The paper's small-value results justify examining the control-data mix, rather
than assuming one engine wins for every table. Splitting control records and large
objects into different engines would still need atomicity for batches crossing
that boundary. It could add another coordination problem.

## 7. Storage architectures worth comparing

These are alternatives at different layers, not a commitment to build all of them.

### A. Independent native WAL and index per owner

Optimize the current commit/publication protocol while keeping storage local.
The [parallel-staging proposal](native_parity_design_plan_2026-10-06.md) belongs here.
It may overlap persistence and reduce barriers, subject to a recovery proof.
Local relocation and index/value reads remain comparatively contained. Cross-owner
batches still need a common outcome and coherent read visibility.

This has the smallest expected additional native-engine scope. It is not proven
to be the fastest architecture or to reach native parity.

### B. A plus one-sided reads of registered value windows

Expose bounded immutable, resident WAL/cache windows with authenticated descriptors,
versions and lifetime protection. A cache miss or unsupported device uses the
ordinary TCP/RPC storage path. A receiver cannot use ordinary RDMA to fetch arbitrary
SSD bytes: data must be accessible through the provider's registered-memory model,
or a target must submit disk I/O and return/stage it. Registering an entire growing
WAL is not a bounded-RAM design.

This could reduce read serving overhead. It does not by itself change write commit
authority, make a stale pointer current, or make memory persistence durable. Preserve
the complete frame/key/codec validation contract, and revoke/drain access before
reusing a registered region. Access keys alone are not a substitute for the required
confidentiality policy. This is a distinct data path from our measured EFA RPC stream.

### C. One logical commit log, physically sharded value storage

Make a complete application batch one authoritative log record, placed on one
selected storage partition where practical. Index shards become derived views of
committed records. The record can remain the value's long-term location.

**This could replace our current per-key-owner prepare/decision exchanges.** That
is a substantive reason to investigate the idea. It succeeds only if the durable
log record is sufficient authority and reads either consult a committed overlay or
wait for the relevant index to reach the requested visibility cut. Deferring index
work without such a read rule exposes incomplete batches or moves waiting into reads.

Open obligations include conflicting-key order, concurrent writers, missing allocated
positions, membership/fencing, bounded index lag, cross-index snapshots and GC of
records referenced by several indexes. If a single batch is striped across storage
partitions, recovery needs all its pieces plus an unambiguous commit predicate.
Physical striping alone does not make those pieces atomic.

CORFU separates position allocation from client-directed storage access and includes
recovery for unwritten positions. Scalog orders cuts from distributed logs, showing
that shared ordering need not require one payload server. Both provide useful
mechanisms; neither removes the need to define completion and visibility. Their
reported rates are not forecasts for durable Sui replay on AWS.
[CORFU, NSDI 2012](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf),
[Scalog, NSDI 2020](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf).

### D. One native index/commit domain over several remote NVMe devices

Keep one native DB process and expose remote drives as block storage, possibly
striped below a single-writer filesystem. The engine keeps its ordinary atomic
allocator and batch publication mechanism. This removes cross-key-owner commit
coordination because there is only one key-index authority.

This is a credible alternative if the aim is aggregated storage capacity/bandwidth
for one Sui process. It does not distribute the engine's CPU or index memory, and
network/storage latency now affects its formerly local I/O. The frontend NIC,
filesystem and index process may limit scaling. An unreplicated stripe increases
the set of devices whose loss can make the DB unrecoverable.

The block stack must preserve flush/FUA and error semantics across every relevant
device. Linux explicitly requires remapping drivers to propagate these guarantees.
Do not mount an ordinary single-writer filesystem concurrently on several writers.
This block-layer candidate may need little native source change; replacing
Tidehunter's mmap/position/recovery machinery with a custom remote-log backend is
a much larger, different change.
[Linux write-cache and flush rules](https://docs.kernel.org/block/writeback_cache_control.html).

### How the alternatives interact

A and C choose different commit authority. B chooses a read transport/lifetime
protocol and can accompany either. D chooses a remote block boundary and one engine
authority; it is not synonymous with C. The LSM alternatives in §6 choose an index
or engine organization and can combine with some of these placements. Compatibility
at the concept level does not establish implementation compatibility.

## 8. AWS: EFA, NVMe-oF, speed and hops

### 8.1 Compatibility that must be established before any experiment

AWS EFA supports OS-bypass traffic through libfabric. Hardware RDMA read/write
capabilities depend on the exact instance type; EFA support alone is insufficient.
For example, AWS lists the EFA-capable i3en sizes without hardware RDMA read/write.
EFA traffic cannot cross AZs or VPCs. Cluster placement can improve network locality,
but is not a promise of a fixed physical-switch hop count.
[AWS EFA capabilities](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html),
[placement groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html).

The libfabric EFA provider has a reliable-datagram protocol with manual progress;
its direct endpoint has different capabilities and responsibilities. Query emulated
read/write capabilities and record the installed provider version. Advertising
`FI_RMA` alone does not prove the operation uses hardware one-sided access.
[EFA provider](https://ofiwg.github.io/libfabric/main/man/fi_efa.7.html).

**Standard NVMe/RDMA must not be treated as a drop-in EFA option.** SPDK's RDMA
transport uses verbs and RDMA-CM. The inspected EFA verbs provider supports UD and
its driver-specific SRD QP path rather than a generic RC endpoint. The source-level
inference is that the ordinary NVMe/RDMA transport is not established compatible
with EFA by simply enabling an EC2 flag. A supported custom transport or another
verified device/stack would need to be identified.
[SPDK RDMA target](https://github.com/spdk/spdk/blob/master/lib/nvmf/rdma.c),
[EFA verbs implementation](https://github.com/linux-rdma/rdma-core/blob/master/providers/efa/verbs.c).

NVMe/TCP over the IP network is a separate candidate that does not require an EFA
verbs mapping. SPDK supports TCP and RDMA transports. Its RDMA path still involves
target storage I/O and host memory; it is not a NIC reading SSD flash directly.
This review has not validated an EC2 deployment of either storage target.
[SPDK NVMe-oF](https://spdk.io/doc/nvmf.html),
[target architecture](https://spdk.io/doc/nvmf_tgt_pg.html).

Instance-store data is lost on stop, hibernation or termination, and on relevant
hardware failures. A remote block protocol does not change that lifecycle. Async
copies do not guarantee that the latest acknowledged commit survives such loss.
[AWS instance-store persistence](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-store-lifetime.html).

### 8.2 What “global NVMe speed” can mean

Aggregate bandwidth/capacity across drives is a plausible goal. Equality with every
local operation's latency is a different claim, particularly for a RAM-cache hit.
For planning, use these qualitative bounds, not a performance prediction:

```text
cold remote read latency includes:
    network exchange + target queue/service + device read + protocol handling

useful throughput is bounded by:
    aggregate device capacity after read/write amplification,
    usable network bandwidth after transport/replication/GC traffic,
    client NIC bandwidth, and CPU/index/commit processing capacity
```

Work can overlap, so cumulative service times cannot be added to predict replay
time. Several SSDs cannot remove a serial visibility dependency or a single-client
NIC limit. Conversely, a poor warm, dependent replay result does not prove that
remote storage cannot scale a cold, parallel workload.

ReFlex demonstrates that a carefully designed TCP remote-flash path can approach
local-flash performance in its evaluated setting. Recent CETOFS work shows that
filesystem and concurrency overhead can remain material above NVMe/RDMA; ntprof
provides a way to investigate NVMe/TCP stages. These support measuring the whole
stack, not promising native parity from the fabric name.
[ReFlex, ASPLOS 2017](https://anakli.inf.ethz.ch/papers/reflex.pdf),
[CETOFS, FAST 2026](https://www.usenix.org/conference/fast26/presentation/jia),
[ntprof, NSDI 2025](https://www.usenix.org/conference/nsdi25/presentation/kang).

### 8.3 Logical network paths

Here a crossing means a one-way transfer between hosts. It is not a physical
switch count or an exact packet count. `C` is client, `I` index, `S` storage and
`Q` sequencing service. Security handshakes are assumed already established.

| Operation | Simplified path | Foreground network dependency |
| --- | --- | --- |
| Native local lookup | C only | Zero host crossings |
| Cached-route owner RPC, local index and WAL | C → S → C | One request/response exchange, two crossings |
| Cached-address one-sided resident-value read | C → S → C | One read exchange; avoids target application handling only when the value is ready and access is valid |
| Remote index lookup followed by remote value lookup | C → I → C → S → C | Two dependent exchanges, four crossings |
| Index server forwards to another value server | C → I → S → I → C | Four crossings with target/server processing in the chain |
| NVMe-oF read from one target | C ↔ S | One target distance; command, data and completion transfer details depend on transport; no claim of exactly one packet RTT |
| Remote sequencer then append, no cached allocation | C → Q → C → S → C | Two sequential exchanges in this simplified RF1 case; batched allocation can amortize one |
| Current multiowner batch | C ↔ owners for prepare, local decision persistence, then publication/application work | Fanout within a phase can run in parallel; required phases still depend on one another |

Our full-map coordinator is embedded with the client; its local durable journal
barrier is **not an extra proxy network hop**. A synchronous remote replica adds
a downstream acknowledgement dependency; async replication does not require that
foreground round but has a weaker failure guarantee. Flushes, cache misses, stale
descriptors, retries and metadata refresh can add exchanges to the simplified paths.

## 9. What the existing measurements constrain

The October 5 local run has five owners sharing one NVMe. Native durable batches
average about 1.13–1.17 ms; distributed batches about 4.80–6.96 ms. Coordinator
decision time averages 1.948 ms including a 1.920 ms journal barrier. Owners perform
90,933 WAL-file syncs versus 21,772 native syncs, before coordinator barriers.
Full-map routing averages 0.707 µs. These are inclusive means and counters, not
an additive causal breakdown. [Timing evidence](local_operation_timings_2026-10-06.md).

The October 4 cluster report records TCP replay at 11.501 s versus 5.399 s native
(2.130×), and EFA at 14.133 s (2.617×), on older code with independent owner NVMe.
The EFA result measures the authenticated RPC implementation over fabric; it does
not measure one-sided WAL reads or NVMe/RDMA. Repetition counts and missing
diagnostics limit conclusions. [Cloud evidence](publication_pipeline_cloud_results_2026-10-04.md).

Thus transport substitution alone has not been shown to fix the gap. A redesigned
shared-log commit domain might eliminate some *current* coordination stages; it
would replace their correctness functions and require new evidence. Neither the
current barriers nor the single-SSD results prove independent WALs are optimal.

No measured result here identifies LSM index compaction or cold value reads as the
dominant cause. Request traces, index/Value-WAL byte counters, cache-hit classes,
device activity and matched durability are needed to establish that claim.

## 10. Repository impact before choosing a design

| Candidate | Middleware work | Native Tidehunter impact | Sui impact |
| --- | --- | --- | --- |
| A: independent owners, improved commit/publication | Significant: reservations, authority, recovery, coverage, publication | Prefer existing retained/prepared/sync APIs; add a narrow primitive only if required by the proof | Preserve typed-store API; no assumed execution/consensus changes |
| B: registered value read windows | Significant: descriptors, cache residency, permissions, TCP fallback, read leases and expiry/revocation | Likely a narrowly scoped safe frame/lifetime API; raw offsets alone are inadequate | Ideally none; facade handles the transport |
| C: shared authoritative log and derived indexes | Major: log ordering/commit, recovery, index progress, placement and GC | Potentially major: location/version split, external value lookup, replay/index integration; quantify before deciding | Adapter may remain stable only if all existing batch/read/snapshot semantics can be preserved |
| D: remote block storage below one native DB | Significant storage deployment, lifecycle, durability and failure validation; less distributed key-owner logic | Potentially minimal engine changes if a compatible single-writer block/filesystem stack suffices; custom log integration is major | Potentially none beyond storage configuration |
| Full LSM shards or separate control engine | Major backend/integration and migration work | Engine replacement or a substantial architectural change | No assumed consensus change; API and cross-engine batch compatibility must be demonstrated |

Relevant source map:

- Native allocation and storage: [`wal/`](../../tidehunter/tidehunter/src/wal),
  [`db.rs`](../../tidehunter/tidehunter/src/db.rs),
  [`wal_replay.rs`](../../tidehunter/tidehunter/src/wal_replay.rs).
- Native index/GC: [`large_table.rs`](../../tidehunter/tidehunter/src/large_table.rs),
  [`index/`](../../tidehunter/tidehunter/src/index),
  [`flusher.rs`](../../tidehunter/tidehunter/src/flusher.rs),
  [`relocation/`](../../tidehunter/tidehunter/src/relocation).
- Existing extension: [`retained_log.rs`](../../tidehunter/tidehunter/src/retained_log.rs).
- Middleware coordination: [`client/storage/`](../crates/client/src/storage), especially
  `wave.rs`, `wave_scheduler.rs`, `manifest.rs`, `authority.rs`, `coverage.rs`.
- Owner durability: [`worker/storage/`](../crates/worker/src/storage), especially
  `native_journal.rs`, `group_commit.rs`, `wave.rs`, `wave_coverage.rs`.
- Read/publication boundary: [`facade/db/`](../crates/tidehunter-facade/src/db),
  including `deferred_publication.rs` and `point_cache.rs`.
- TCP/fabric protocol: [`transport/storage/`](../crates/transport/src/storage),
  [`rdma-fabric-sys/src/`](../crates/rdma-fabric-sys/src).
- Sui storage boundary: [`typed-store`](../../sui/crates/typed-store/src/rocks/mod.rs).
  Keep changes here minimal and necessary; no upstream push is part of this review.

## 11. Decision gates before implementation

1. Resolve the native cache concern and the exact supported batch/read/checkpoint
   contract. Keep intended properties separate from verified implementation behavior.
2. Extract actual batch sizes, key/value sizes, owner fanout, read-hit classes and
   conflict patterns from the retained workload/evidence. Do not infer cold-storage
   demand from warm replay or infer a safe table split from value size alone.
3. Compare A, C and D on paper: one durable write, one cross-index batch, a read
   immediately after acknowledgement, lost reply, writer crash, stale owner, and GC
   during reads. Include bounded retained state and recovery without a live client.
4. For a shared log, prove the complete-batch commit predicate and index visibility
   rule. For independent WALs, prove staging/decision recovery and retirement. For
   remote blocks, prove flush/error propagation and exclusive ownership.
5. Choose a candidate only after identifying what measured dependency it removes
   and what new cost it adds. Evaluate B separately if read-service overhead is
   material. Consider LSM changes if index/small-value costs support them.
6. Any later prototype first gets deterministic crash/concurrency/GC validation
   and matched local old/new measurements. Local one-SSD measurements cannot predict
   the independent-NVMe gain. A later approved cluster test must compare fresh,
   identified builds under equal durability, security, resources and workload, then
   retrieve evidence and terminate owned resources.

There is no evidence-backed promise of identical performance for every operation.
There is also no basis for declaring independent WALs the only viable design.
The fixed requirements are recoverable atomic outcomes, correct visibility/order,
honest durability acknowledgements and safe value lifetime. The placement, log,
index and transport choices remain open until they satisfy those requirements
and address the measured workload.
