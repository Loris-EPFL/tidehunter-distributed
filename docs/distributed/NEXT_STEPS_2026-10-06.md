# Next implementation plan: database authority and distributed Value WAL

Date: 6 October 2026. **Implementation plan with current progress; unfinished work
is identified below.**

## Current implementation update

The [pipeline and read-cache record](PIPELINE_AND_READ_CACHE_2026-10-06.md) is the
latest implemented local slice. Per-node staged/in-flight work uses bounded any-node
completion notifications, ordered durable-prefix publication and exact credit
release. Lone writes use the protected client directly. A bounded cache uses
verified immutable references after index resolution.

Final exact-source correctness passes **520 feature-library tests (seven ignored)**
and the separate-process crash/recovery campaign. The final matched campaign
independently verifies **234 cases**, with zero swap and all node processes gone.
At 8 KiB/B32/W32, pipeline-only load improves **21.0%** against its disabled
same-binary control; the combined mode improves **17.9%** against previous TCP and
remains **2.23× native**. Hot-cache reads avoid exchange using extra RAM, while
mostly-miss reads and some small-write shapes still lag. The report preserves
outliers and unequal cache policies. No AWS launch or RDMA path accompanies this
slice.

The previous [local/AWS optimization record](OPTIMIZATION_AND_CLUSTER_VALIDATION_2026-10-06.md)
completed 288 local cases and a corrected 106-case independent-NVMe retry. All
run-owned cloud resources from both attempts were removed. That campaign measured
warm cached-device reads and retained a substantial native gap; it does not measure
the newer pipeline/cache implementation.

Current priority is to separate preparation's encoding, hashing, validation and
allocation costs and investigate the unresolved small-window persistence
regressions. Preserve cache-disabled and pipeline-disabled controls and explicit
cache budgets in any independent-NVMe retest; the new local overlap has not yet
been measured there. Full Sui integration, distributed authority leases,
replication, snapshot-assisted replay and safe reclamation remain separate gates.

### Historical first TCP implementation status

The [TCP readiness and evidence record](LOCAL_TCP_READINESS_2026-10-06.md) is the
first TCP serving implementation status. The [local database record](IMPLEMENTATION_LOCAL_DB_2026-10-06.md),
[structure/publication record](IMPLEMENTATION_STRUCTURES_2026-10-06.md) and
[M1/M2 record](IMPLEMENTATION_M1_M2_2026-10-06.md) retain historical evidence.
That earlier full feature library suite passed **499 tests**, with six ignored;
two focused read-chunk regression tests passed after review fixes. The default
native library passes **383 tests**, with six ignored. Real subprocess tests pass
for complete batches/reads, abrupt coordinator exit, registered-inventory recovery,
old retries, missing/restarted nodes, partial fencing, durable reply loss, stale
epochs and mTLS rejection.

The fresh 96-case release campaign and 24 separate window-32 cases passed all
audits across four serving backends and three repetitions, with zero swap delta
and no surviving sampled processes. The [full report](LOCAL_TCP_BENCHMARK_2026-10-06.md)
keeps the new window-32 workload separate from matched historical comparisons.
Local durable load-window ratios are **1.33–2.63×** same-run native file-barrier;
TCP is **1.71–4.82×**. Warm scalar RPC reads still show a large gap. The previous
72-case campaigns stay unchanged and are compared as historical local baselines.
This is a functioning bounded static RF1 TCP engineering backend, not Sui, HA or
production parity. No AWS resources were launched.

| Milestone | Current boundary |
| --- | --- |
| M1 | Version-2 descriptors and individually checked reads use immutable segmented membership metadata without the partition metadata mutex. The one-fragment complete batch/group limit remains a compatibility gate. |
| M2 | Complete groups, recovery, bounded workers and strict owned-WAL joins serve the local database. Metadata admission reserves outstanding work. Retention/rotation and fuller metadata-I/O fault coverage remain gates. |
| M3 | The new local RF1 backend serves actual requests through native pending transactions and a native-routed typed cell index sharing native overlay/search primitives. Checkpoints, ordered scans and mapped reference images work. Legacy LargeTable/persisted-format genericization and Sui adaptation are unfinished. |
| M4 | Durable complete inventory, preparing/active epochs, exclusive local ownership, remote node fencing, sealed histories and streaming logical replay are integrated. Partial fencing and missing-node recovery fail closed. Cross-host authority consensus/lease and snapshot-based replay acceleration remain unfinished. |
| M5 | Persistent mutual TLS/TCP, bounded durable append and direct/scatter reference reads serve real separate processes. Deterministic reply loss/retry, stale epochs and authentication rejection pass locally. The prior optimized implementation completed independent-NVMe deployment; new pipeline/cache independent-NVMe validation and sustained network/slow-peer stress remain gates. |
| M6 | Replication, protected-reader relocation and reclamation remain unfinished. |
| M7 | Native differential tests and a 96-case native/local/TCP serving campaign pass. Existing Sui branch analysis remains the validation-base decision; no Sui executable selects this backend. |

Continue with the [local serving source map](IMPLEMENTATION_LOCAL_DB_2026-10-06.md),
[legacy native integration map](NATIVE_INTEGRATION_MAP_2026-10-06.md) and
[Sui validation base](SUI_VALIDATION_BASE_2026-10-06.md). The local backend retains
all versions and WAL history under explicit caps. Persisted reference images can
be used for mapped reads, but startup reconstructs the index from all retained WALs.

Experimental registration/seal metadata is **v3** because its configuration now
includes `max_reference_metadata_bytes`; complete batch codec remains **v2** and
group codec **v1**. Native database/index formats remain unchanged. Earlier
experimental registrations are refused without automatic migration.

## 1. Repository state and priority

The fork's remote `main` and MystenLabs `upstream/main` were fetched again and both
resolved to `ce37b18e6c2ffb8e46b7d8c1bf136c9366a7642d`. No upstream commits were
missing. The implementation branch is `feat/sharded-value-wal` in
`Loris-EPFL/tidehunter-distributed`, authenticated and pushed as `Loris-EPFL`.

Committed foundation:

- `63b6c4a`: reproduced and fixed delayed native value-cache refill after mutation.
- `d86414a`: experimental native WAL partitions, complete-batch codec, logical
  counter, recovery models, local example and design records.

The historical [implementation record](IMPLEMENTATION_2026-10-06.md) states what those
commits implement. Its 402 passing tests, six ignored upstream tests and local
example establish a correctness starting point. They establish no cluster speedup.
This plan turns that substrate into a native distributed database path. The
[architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md)
and [correctness inventory](native_correctness_and_storage_options_2026-10-06.md)
remain the design and semantic references; this document orders the next work.

## 2. The coordinator hierarchy we will implement

**One active database authority owns the shared logical WAL counter and native
index for each logical database. Storage nodes own physical allocation and local
I/O workers. A small cluster control plane owns placement and fenced epochs.**

For a deployment with one logical database, this looks like one database
coordinator feeding several storage instances. Several logical databases can have
different authorities, counters and resource budgets. Sui's existing separate
database boundaries do not become one cluster-wide transaction domain.

```mermaid
flowchart TB
    CP[Cluster control plane: membership, epochs, inventory, coarse budgets]
    SUI[Sui storage adapter] --> DB[Per-DB authority: shared logical counter, native index, publication]
    CP -. assignments and fencing .-> DB
    CP -. stream ownership and budgets .-> A[Storage node A service]
    CP -. stream ownership and budgets .-> B[Storage node B service]
    DB -->|complete batch / value read| A
    DB -->|complete batch / value read| B
    A --> AW[Local admission, append, persistence and read workers]
    B --> BW[Local admission, append, persistence and read workers]
    AW --> AN[Native WAL on node A NVMe]
    BW --> BN[Native WAL on node B NVMe]
```

The node service is an endpoint and resource owner. Its local workers run on that
machine and can use shared memory. It does not introduce another network hop
between receiving a request and issuing local disk I/O.

| Layer | Owns | Request-path behavior |
| --- | --- | --- |
| Cluster control | Membership, authoritative assignment/epoch, complete registered inventory, placement and maintenance budgets | Cached configuration; metadata updates on bootstrap/reconfiguration |
| Per-DB authority | Semantic validation, bounded admission, logical versions, index/caches, atomic publication, read resolution and recovery | Current single-owner mutable API; bounded windows dispatch across storage nodes |
| Storage node service | Authorized epoch checks, identity/digest validation, bounded queues, native log partitions and durability receipts | Direct requests from the authority; local scheduling |
| Local worker pools | Append/copy, persistence groups, reads, later replication and reclamation | Execute node-local work using bounded queues and reserved recovery capacity |

The authority can live in the same process as the Sui storage adapter, preserving
the existing in-process API boundary. Making it a separate service would add an
application-to-authority exchange and must be measured explicitly. Multiple
authorities can be colocated or deployed on different hosts later.

### Shared WalCounter

The current `LogicalClock` is a primitive for this design:

```text
BatchId               = stable database + operation identity
BatchVersion          = fenced epoch + logical sequence
MutationVersion       = BatchVersion + operation index
PhysicalValueAddress  = partition + generation + byte offset + length
```

All partitions of one database see the same logical version domain. The authority
allocates the sequence with a local atomic operation, then sends it with the batch.
Storage nodes allocate independent physical byte ranges. There is no extra
counter-allocation RPC on this initial path. The counter's relaxed atomics supply
uniqueness; durability and publication require their own synchronization.

There must be exactly one valid authority for an epoch. The standalone clock constructor
accepts a caller-supplied epoch and cannot establish that guarantee. The local
serving inventory now supplies exclusive local ownership and durable epoch
transitions before activation. Creating independent clocks with the same epoch
is still unsafe. Remote node fencing is implemented for controlled static ownership;
cross-host exclusive authority establishment remains a lease/consensus gate.

Track allocated, durable, resolved and published progress separately. If batch 10
is unresolved while 11 finishes on another node, the initial publication prefix
still waits at 10. Allocation throughput does not remove that wait. A new epoch
must fence old writers, enumerate the complete inventory and settle surviving
records before its counter becomes active. Timeouts leave unknown outcomes.

If one authority later limits index CPU, memory or NIC throughput, measure that
limit before introducing several ordering authorities or a remote sequencer.
Reservation ranges require gap recovery and stale-writer fencing. Timestamps or
unique-ID generators alone cannot replace the completed-prefix protocol.

## 3. Intended operation paths and costs

### Durable write

1. At the authority, validate the whole application batch and reserve admission
   credit. Establish stable identity, epoch and logical sequence.
2. Choose one registered storage partition for the entire batch. Initial routing
   is deterministic by identity; retries resolve to that same placement.
3. Send one `append_durable` request. The node validates, appends and persists the
   authoritative batch, optionally with other independent batches in a proven
   persistence group.
4. Return its durable physical references; the authority publishes all index
   mutations atomically when the logical publication rule permits it.
5. Acknowledge the application only after the selected durability and publication
   contract is satisfied. A persisted batch is irrevocable and replayable even if
   the authority dies before publication.

The intended RF1 path has one authority-to-storage request/response exchange and
one storage durability stage. `append_group` and `persist_through` are separate
Rust operations; the local `GroupCommitWorker` combines them and returns
per-batch durable receipts. The TCP coordinator groups pending batches per node;
the node performs native complete-group append/persistence before the same durable
response. Storage nodes own bounded connection/request service rather than a
second `GroupCommitWorker` queue. This bounded RF1 RPC path is implemented. A metadata or frontend decision barrier per
application batch would require revisiting the expected cost reduction.

### Read

The authority resolves a typed native-cell index reference and requests that
record directly from its storage node, with bounded member/integrity checks. The
current TCP value path has no native value-cache equivalent, so a present scalar
get requires one storage exchange even when remote pages are resident. A future
coherent value cache could serve suitable hits locally. Reading an
entire multi-value batch for every scalar get is an explicit failure of the intended
design. Iterators require bounded parallel fetch while preserving API ordering.

### Failure and maintenance

The control plane changes assignments and fences epochs; it does not schedule
individual memcpy, cache or fsync operations. Storage workers execute local work.
The database authority owns the logical publication/recovery view and approves
conditional reference replacement. Replica, snapshot and reader retention all
constrain reclamation. Those mechanisms must preserve the paper's values-in-WAL
property and cannot delete a batch merely because it has been indexed.

## 4. Implementation milestones and acceptance gates

M1–M5 supply the first bounded RF1 Sui validation path. The integration harness in
M7 can be prepared in parallel and used as soon as those gates pass; initial
mechanism measurements need not wait for asynchronous replication. M6 is required
for the corresponding replica guarantees and sustained reclamation workloads.

### M1 — format and directly readable values

**Current progress:** implemented for explicitly bounded version-2 batch records.
Exact metadata membership reads now use the immutable reference directory, with
an explicit admission budget. Native/Sui large-batch compatibility and sustained
metadata retention remain gates; see the latest implementation record.

**Files:** `distributed/{types,codec,storage,reference_directory}.rs`, `wal/mod.rs`,
codec/reader and immutable-directory tests.

- Define a versioned batch/container layout with independently verifiable value
  records, bounded descriptors and an authoritative completeness rule.
- Add a value reference including generation, batch membership, record bounds,
  logical version and integrity evidence. Resolve scalar reads without fetching
  or checksumming every sibling value on every read.
- Preserve native put/delete encoding and duplicate-key operation order where
  suitable. Keep parsing fallible and validate before recoverable authority exists.
- Specify handling of native-size atomic batches that exceed one fragment. Use a
  proved segmented representation or an explicit compatibility decision; silently
  narrowing the serving API to the prototype's limits is unacceptable.
- Version the experimental format; refuse incompatible old directories until an
  explicit migration/export procedure exists.

**Gate:** corruption, forged/out-of-bounds references, duplicate keys, mixed
keyspaces and all truncation boundaries fail safely. Scalar read bytes scale with
the requested record and bounded metadata. Payload values have one authoritative
long-term log representation.

### M2 — recoverable persistence groups and local storage scheduling

**Current progress:** complete groups, an independent fault model, actual storage
recovery, bounded local workers and owned-WAL strict joins are implemented. The
earlier one-pending-batch rule is replaced by one pending **group** per partition;
group members remain complete application batches. Exact retry hash lookup and
pre-effect reference-metadata budget checks are integrated. The full milestone
still needs retry-retention floors/rotation and remaining metadata-I/O failure gates.

**Files:** `distributed/storage.rs`, proposed `distributed/{group,admission}.rs`,
`distributed/model.rs`, storage fault-injection tests.

- Extend the model before permitting several unpersisted batches in one stream.
  Evaluate a bounded complete group container as the first candidate: every member
  stays a whole application batch and becomes authority-eligible only under the
  container's explicit completeness rule. This changes eligibility and needs a
  proof; merely adding a footer is insufficient.
- Prove recovery/discovery across torn group headers, missing skip markers and
  later durable groups. Never search arbitrary value bytes for apparent frames.
- Bound groups by bytes, records and waiting time; flush promptly for dependent
  workloads. Reserve queue credits for recovery/sealing/control operations.
- Amortize compatible persistence while retaining per-batch outcomes. Treat
  uncertain I/O effects as unknown; never resurrect a sealed identity.
- Harden shutdown ownership and worker joins before relying on directory locks
  across component replacement. Cover injected write, sync, rename and directory
  sync failures, including retry after a lost response.
- Bound retry history through explicit retention floors/expired outcomes before
  replacing today's fixed `max_batches` lifetime cap.

**Gate:** the extended model detects unsafe alternatives; physical fault-cut tests
agree. Groups reduce barriers per batch under concurrency without hiding drain work
or silently adding unbounded latency to sequential Sui requests. Keep one pending
complete group until a further recoverable-pipelining proof passes. The controlled
nine-batch worker test uses two explicit group persistence calls; this is an
operation-count result, not an elapsed-time speedup or Sui measurement.

### M3 — native index references and atomic publication

**Current progress:** the bounded `distributed/local_db.rs` backend now integrates
ordered publication, completion handoff and the prepared native transaction bridge
with actual reads and writes. `distributed/native_index.rs` adapts native routing,
pending tables and shared ordered overlay/search primitives into a typed cell
container with checkpoint cuts, heap-merged ordered scans and mapped reference
images. No separate value map serves requests. This is not a completed genericization
of legacy `Db`/`LargeTable`; its flusher/formats and the Sui adapter remain gates.
Existence checks retain native index-only behavior where full-key validation is
unnecessary. Outstanding index/reference metadata is reserved before logical
allocation. See the [local source/evidence record](IMPLEMENTATION_LOCAL_DB_2026-10-06.md).

**Files:** `db.rs`, `batch.rs`, `large_table.rs`, `index/{pending_table,index_table,
index_format}.rs`, `checkpoint.rs`, `iterators/`, proposed `distributed/authority.rs`.

- Introduce an explicit experimental DB mode that stores logical mutation versions
  separately from physical record references. Retain the local native mode.
- Audit every comparison or watermark currently derived from `WalPosition.offset`;
  classify it as physical allocation, logical ordering, replay, snapshot or GC.
- Connect complete-batch durable receipts to native pending transactions and
  atomic publication. Maintain a bounded resolved/publication prefix, with durable
  batches retained across gaps. Model handling of rejection before effects and
  authority loss after possible effects.
- Route scalar/batch writes, cache misses, exists, checkpoints and iterators
  through the new mode. Preserve the actual native API guarantees and known
  checkpoint lifetime limits; do not advertise stronger transaction semantics.
- Carry the cache-generation fix through the new read path. Settle the remaining
  optional compactor/relocation-filter cache-coherence audit before enabling those
  configurations on the distributed backend.

**Gate:** differential tests compare native and experimental results for identical
operation histories, including concurrent same-key changes and atomic batches.
Test failures after durability but before every publication step. Assert the
experimental backend is actually serving the request; no silent native fallback.

### M4 — startup, durable epochs, recovery and snapshots

**Current progress:** `local_inventory.rs` persists complete registered streams
and preparing/active epochs. Local file ownership excludes competing local
writers; strict worker shutdown releases the native WAL owners. Reopen seals and
replays all retained registered WAL histories in logical order, preserves old retry
identities and activates a new epoch. Missing streams/configuration fail closed.
The remote variant now durably fences every registered node and replays sealed
histories, including interrupted Preparing epochs. This controlled static protocol
has no distributed lease or consensus. Index images are derived mapped-read
representations; they are not yet replay checkpoints, and no retained history is
reclaimed. Cross-host authority exclusivity and image-based recovery remain gates.

**Files:** proposed `distributed/{inventory,recovery,epoch}.rs`, `wal_replay.rs`,
`control.rs`, `state_snapshot.rs`, `wal/tracker.rs`.

- Complete registration of every configured stream before enabling database
  writes. Persist schema, inventory identity and format with recoverable metadata.
- Establish exclusive authority and durable fencing at all registered nodes. Use
  controlled static assignment for initial tests; unattended failover requires an
  established metadata-consensus implementation and a separate integration gate.
- Freeze inventory, seal old streams, discover complete eligible records and
  resolve holes from terminal evidence. Preserve original identities/versions on
  retry under a new request epoch. An unavailable RF1 node may block recovery.
- Rebuild the native index in logical order across partitions. Persist snapshots
  with the logical cut and explicit replay/retention boundaries for every stream.
  A maximum offset is insufficient evidence for a complete snapshot.

**Gate:** repeated reopen/failover models, partial fencing, stale in-flight messages,
missing nodes, old-epoch retries and snapshot crashes never expose partial history.
M3 and M4 can be developed together. The bounded local serving mode now validates
publication and full-history recovery; the separate-process TCP driver additionally
validates missing-node blocking, partial fencing, stale epochs and durable lost
replies. General distributed authority ownership remains a separate gate.

### M5 — authenticated TCP storage nodes

**Current progress:** implemented in `distributed/{remote_db,tcp}.rs` and
`distributed/tcp/inventory.rs`, with the standalone node mode and process ownership
in `examples/support/tcp_cluster.rs`. Persistent mTLS sessions carry bounded,
versioned append/read/status/fence/seal/recovery operations. Complete pending
batches dispatch in parallel across nodes; scalar reads contact the exact owner,
and multi-get/scan pages scatter bounded fetches while preserving order. Real
subprocess fault tests and the release campaign pass. General cross-host authority
exclusivity, slow-peer saturation stress and independent-NVMe measurements remain
gates; [current evidence](LOCAL_TCP_READINESS_2026-10-06.md) states their limits.

**Files:** `distributed/{remote_db,tcp}.rs`, `distributed/tcp/inventory.rs`,
`examples/{sharded_wal_cluster_check,sharded_wal_bench}.rs`, `examples/support/`.

- Implement bounded, versioned messages for durable append, value reads, status,
  seal/fence and recovery enumeration. Preserve authentication and confidentiality
  through TLS; bind authority to the registered DB/epoch/peer.
- Use persistent connections, deadlines and identity-preserving retries. Separate
  bounded admission from worker execution so slow storage cannot exhaust unrelated
  database or recovery capacity. Async network tasks and bounded blocking disk
  workers should have explicit ownership and shutdown.
- Cache placement locally and send each batch directly to its storage node.
  Expose queue, service, persistence and transport timings as correlated spans.
- Keep TCP semantics transport-independent so an eventual RDMA path can share the
  same authority, durability and reference-lifetime rules.

**Gate:** multi-process loopback tests exercise real framing/TLS and failures rather
than in-process dispatch. Cover response loss, stale epochs, slow peers and bounded
memory. Separate append and persistence internally without adding two mandatory
network round trips to the durable-write API.

### M6 — replication, relocation and reclamation

**Files:** proposed `distributed/{replication,retention}.rs`, `relocation/`,
`file_reader.rs`, index/reference and snapshot code.

- Add asynchronous copies of authoritative records plus required manifests.
  Nodes can host primary streams and selected foreign replica streams. Track
  appended, durable, replicated, published and snapshot progress separately.
- State the acknowledged-loss window on primary disk loss. Promotion needs
  fencing and a recoverable state; async copying does not promise zero loss.
- Implement protected reader lifetimes and conditional relocation that preserves
  the original mutation version. Persist replacement values and recovery mappings
  before releasing source retention; satisfy reader, snapshot and replica floors.
- Preserve data while any required proof is missing. Add crash/reader/replica
  models before enabling deletion or generation reuse.

**Gate:** an acknowledged RF1 write survives its declared native-comparable fault
model; stronger modes are labeled separately. No stale reference reads reused
storage, and slow replicas do not create unbounded retention without backpressure.
RF1 performance diagnostics can precede replication, with that limitation explicit.

### M7 — Sui integration and measured comparison

**Current base decision:** preserve Sui `c79040af6c` for matched validation once
M3–M5 pass. Fetched upstream main `4630995ca0` is a separate compatibility target;
it adds workload/cache/pruning changes without a newer typed-store adapter. See
[the inspected commits and commands](SUI_VALIDATION_BASE_2026-10-06.md).

**Files:** middleware `crates/tidehunter-facade/`, Sui `crates/typed-store/` backend
hooks, existing `middleware/experiments/{sui,aws}/` audit/campaign tooling.

- Make the adapter select the new native mode with a visible backend identity.
  Keep Sui-facing interfaces stable and remove duplicate middleware ownership of
  publication only when the native authority actually supplies it.
- Start with local differential tests, then the real Sui workload using bounded
  datasets. Compare new local versus old local under equal acknowledgement modes.
  Track swap, memory, disk and remaining background drain explicitly.
- Record source and binary hashes, configuration, topology, complete raw results
  and traces. Native asynchronous acknowledgement and distributed durable
  acknowledgement must not be described as equivalent modes.
- Measure batches/operations, RPCs, payload/physical/network bytes, value-read
  amplification, barriers, queue delays, gap waits and publication CPU. Analyze
  dependent replay latency separately from saturated throughput and cold reads.
- Use independent-NVMe cluster measurements only when a remaining question needs
  that topology. Build verified fresh binaries, retrieve evidence and terminate
  all owned benchmark resources under the approved budget and authorization.

**Gate:** genuine Sui correctness audits pass and the new path is demonstrably in
the executable. Report ratios and uncertainty against matched baselines. Production
readiness additionally requires sustained retention/relocation and recovery tests.

## 5. Parallel work boundaries

After agreeing each shared interface, independent agents can own:

| Workstream | Initial task | Shared boundary |
| --- | --- | --- |
| Record layout/read path | M1 codec, record references and integrity tests | Frozen reference/layout contract before index and transport consumption |
| Protocol/storage | M2 models, groups, failure injection and bounded admission | Authority eligibility and persistence outcomes |
| Native integration | M3 offset-role audit, index/publication design and tests | Logical version and physical reference interfaces |
| Recovery/control | M4 epoch/inventory/snapshot design | Complete inventory, fencing and replay cut |

Three agents plus the integrating parent can run concurrently; overlapping native
files require explicit ownership. Each task should reopen the relevant design
section and native source when its assumptions change, and record the primary
project/paper mechanism it adapts. Integrate one tested boundary at a time.

## 6. Repository impact and architecture limits

| Repository | Expected changes |
| --- | --- |
| This native fork | Substantial: reference/index formats, replay/snapshots, publication, storage-node services, retention and replication |
| Middleware | Substantial simplification/adaptation; preserve useful transport, Sui audit and campaign tools while the old architecture remains a comparator |
| Sui | Aim for minimal backend selection/configuration/adapter changes; no current evidence requires changing Sui consensus or execution |

The principal performance hypothesis is fewer durable participants and decision
stages per multi-key batch while values remain on independent NVMe logs. The shared
counter is intended to be a small local cost. The authority still concentrates one
DB's index, publication work and frontend bandwidth; a low stalled sequence can
hold its publication prefix. These are measurable limits, not solved by naming the
counter global. Remote misses also retain network latency.

RDMA can later accelerate a supported transport or protected resident-value read.
Cold SSD data still needs an explicit storage I/O path. NVMe-oF is a separate block
storage comparison with its own durability/ownership requirements. Neither is
required for the first functioning TCP architecture, and neither replaces ordering,
fencing, recovery or publication. No AWS work is part of this local implementation
stage.

## 7. Research anchors

- **[Tidehunter paper, v2](https://arxiv.org/abs/2602.01873v2)**,
  §§3.1–3.4 and §4.4, and native source: permanent Value WAL,
  local atomic allocation, batch replay, completed frontiers, snapshots and
  conditional relocation. [Local workspace copy](../../../middleware/misc/tidehunter.pdf).
- [CORFU](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf):
  ordering separated from storage, sealing and hole resolution. Our initial
  counter is colocated with one native index authority; no performance figure is
  transferred from CORFU to this workload.
- [CorfuDB multi-object transactions](https://github.com/CorfuDB/CorfuDB/blob/master/runtime/src/main/java/org/corfudb/runtime/object/transactions/OptimisticTransactionalContext.java):
  one complete multi-object authority record as a design precedent.
- [DynamoDB, ATC 2022](https://www.usenix.org/system/files/atc22-elhemali.pdf) and
  [TiKV scheduler source](https://github.com/tikv/tikv/blob/49e7a1179d676b1bc51447f6b0ff8973e45bc85f/src/storage/txn/scheduler.rs):
  fleet-level control with local admission and execution; neither establishes our
  proposed distributed Value WAL semantics.
- [FoundationDB commit path](https://github.com/apple/foundationdb/wiki/Transaction-Commit-Path):
  distinguish durable log authority from subsequent application. Its historical
  architecture is an analogy, not a template to copy with a second permanent
  value store.

These sources motivate specific boundaries. The next milestones still need their
own correctness evidence and matched performance measurements.

## 8. Distributed-path concurrency and the lock-free question

The desired distributed fast path can avoid several shared mutexes. The current
prototype is **not lock-free**: `PartitionLog` has per-partition metadata and
persistence mutexes, and `GroupCommitWorker` uses a bounded queue protected by a
short mutex/condition variable. There is no single global mutex around all storage
partitions. The logical counter already uses an atomic operation locally.

The implemented persistence split releases the metadata lock before file I/O.
Reads and already durable retries proceed while a newer group is being persisted;
new appends still respect the one-unpersisted-group recovery rule. The concurrency
regression tests this dependency directly. Sealing is a maintenance operation and
can still hold metadata while shutting down its native workers.

Scalar membership validation now uses the immutable physical-reference directory,
so it also proceeds while append/admission owns the metadata mutex. The complete
record read can still wait inside native WAL/file operations; this removes a
specific metadata dependency rather than making I/O nonblocking. The authority's
new `CompletionInbox` uses per-request single-producer immutable result cells and
atomic unknown/ownership flags instead of a shared result-map mutex.

### Implemented refinements and remaining ownership work

| Distributed component | Preferred next refinement | Correctness/performance condition |
| --- | --- | --- |
| Per-partition append/retry state | One writer worker owns mutable state; callers submit to a bounded queue | Ownership removes shared mutation of this state, but the worker remains sequential and can wait for disk I/O. Preserve stable retry identity and group recovery. |
| Queue submission | Consider a bounded atomic MPSC queue if measured contention warrants it | Preserve request/byte credits through completion, shutdown draining and uncertain outcomes. An unbounded lock-free queue is not acceptable. |
| Scalar read membership/reference table | Implemented: immutable segmented directory, complete-group prefix publication and explicit metadata capacity | Exact identity/digest/generation/descriptor checks remain. No map clone per append or read mutex. Retaining all source WAL bytes remains necessary until reader/GC lifetimes are implemented. |
| Logical counter | Keep the current in-process atomic allocator | Atomic uniqueness is not durable/published progress or remote fencing. Avoid a per-operation sequencer RPC. |
| Authority publication | Implemented: dense state vector, exact identity hash map and a mutex-free completion inbox; native pending bridge with three passing focused tests | One owner performs bounded polls and ordered publication. Unknown outcomes retain their original identity and can later become durable. The typed native-cell serving sink is wired to both local and persistent TCP dispatch. The owner remains serial; concurrent storage completion does not make publication parallel. |
| Native index rows/cache maintenance | Keep short sharded synchronization initially | Replacing it needs proofs for duplicate-key ordering, whole-batch visibility, cache invalidation, snapshots and relocation. Measure lock wait before widening the rewrite. |

See the [structure implementation record](IMPLEMENTATION_STRUCTURES_2026-10-06.md)
for decisions at each layer and tests. Queue replacement, legacy LargeTable adaptation, snapshot-based replay and remote
authority service remain unfinished. Exclusive mutable
ownership alone does not establish a formally lock-free service. Persistence,
network availability and unresolved lower logical versions can still prevent
completion. Even a lock-free in-memory structure can contend on cache lines and
requires correct memory reclamation.

The acceptance target is independent partition progress, bounded admission,
short or absent read-side critical sections, and no shared request lock spanning
remote/disk I/O. The target is not a mutex count of zero. Benchmark queue delay,
lock wait/hold time, serialization/hash/copy time, persistence, RPC and publication
separately under matched durability before attributing the old speed gap to locks.
RDMA may reduce transport cost later; it does not remove these ordering rules.

### Native reference, checked against this checkout

Regular Tidehunter uses a hybrid:

- `tidehunter/src/wal/allocator.rs`: atomic CAS allocation.
- `tidehunter/src/large_table.rs` and `primitives/sharded_mutex.rs`: sharded row
  mutexes, including contention timing; prefix-cell metadata also uses an RwLock.
- `tidehunter/src/index/pending_table.rs`: shared atomic transaction commit status
  combined with synchronized table mutation, not a globally lock-free index.
- `tidehunter/src/db.rs`: mutex-protected control-region state.

Its existence demonstrates that mutex-free whole-engine code is not a prerequisite
for the native baseline's performance. It does not prove our added locks are cheap;
those require their own measurements after native integration.
