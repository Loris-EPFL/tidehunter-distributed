# Implementation plan: checkpoint admission and remote WAL fast paths

Baseline: completed 18 October 2026 native/TCP/EFA/EFA-direct campaign.
Status: **planned, not implemented**. This document changes no runtime behavior.
No new benchmark or AWS launch is part of this planning task.

This is the ordered implementation specification for the findings in
[the post-campaign investigation](POST_CAMPAIGN_PERFORMANCE_PLAN_2026-10-18.md).
The [original hardware report](HOT_READS_AND_CHECKPOINT_ADMISSION_2026-10-16.md#18-october-completed-hardware-comparison)
and its hash-bound artifacts remain unchanged. Predicted benefits below are
**[INFERENCE]** until the acceptance gates pass.

## 1. Decision and boundaries

Optimize the measured checkpoint and message paths before replacing the storage
architecture. Keep the Value WAL as the authoritative value storage, not a log
in front of a second value database. Do not build a global full-value WAL, sharded
value LSM, one-sided RDMA protocol or NVMe-oF service in this work.

The current code is not a cluster of complete independent native `Db` instances:

- `RemoteDatabase` owns the coordinator `NativeIndex`, `LogicalClock`, publication
  tracker, bounded admissions and checkpoint metadata.
- `StaticInventory::partition_for_validated(batch.id())` places whole batches.
  A batch's storage owner is not found by a network hash-table lookup on each get.
- Each storage `PartitionLog` owns a native `Wal`, descriptor/reference authority,
  durable frontier and local retention/history. Values stay on its disk.
- A coordinator cache miss obtains an exact reference from its local index, then
  reads that member from its owning shard. There is no required extra remote
  index-lookup hop before that value RPC.
- The logical clock is local to the authority owner. This plan does not introduce
  a network round trip for a cluster-global counter.

The baseline uses RF1. Preserve that explicit comparison mode; it does not prove
replicated availability, survival of lost instance-store data or AZ failure.

Existing scalar inline reads, singleflight, bounded multiplexing, registered DMA
buffers, vectored TLS, member-only verification/SIMD, background checkpoints,
incremental index-cell reuse and ready-writer grouping must be preserved. They
are not missing features to implement again.

### Repository change scope

All source paths below are relative to `tidehunter-sharded-wal/`, except explicit
`project:` paths, which start at the containing project root. Symbols are the
stable mapping; line numbers describe the inspected source, not a permanent API.

| Repository / area | Planned changes | Excluded changes |
|---|---|---|
| Fork: `tidehunter/src/distributed/` | Admission policy, checkpoint diagnostics/history IO, reactor scheduling, adapter barriers and optional process transport retention | No change to recovery authority, acknowledged durability or native hash algorithms |
| Fork: `tidehunter-fabric/` | Derived RX readiness, direct-credit accounting, sweep sequencing; separately qualified idle CQ waits | No handwritten provider ABI, raw verbs replacement or removal of TLS/CRC |
| Fork: `tidehunter/examples/` and `experiments/` | Explicit metric contracts, diagnostic exports, matched controls and one-fleet phases | No edits to historical campaign raw data/source snapshots |
| Original inherited engine: `tidehunter/src/{db.rs,wal/,large_table.rs}` | Reuse existing APIs; no runtime changes currently required | No mutex removal, value-format replacement or new value-log layer |
| `project:sui/` | No runtime change required for core fixes; audit the existing typed-store flush boundary | Do not silently weaken `Database::flush`; any later callsite change needs its own contract review |
| `project:archive/middleware-legacy/` | Archived historical baseline; no runtime changes | No dependency on or parallel implementation in archived code |

## 2. Evidence, populations and unresolved attribution

Use `project:checkpoint-work/2026-10-18-efa-direct-hardware/` as the retained
baseline. Its source archive SHA-256 is
`70454d0113ff49df6abfedd38865f3d7f0adc215c5afa6ece1a0431b08a921aa`.
A2 is the targeted campaign; B2 is the full campaign. Do not pool them.

| Issue / population | Observed evidence | Conclusion permitted by the evidence |
|---|---|---|
| A2 TCP 8 KiB B32/W32 load tail | Paired native p99 22.752 ms; remote 192.435 ms. The same slow remote window contains 179.650 ms foreground checkpoint time | Checkpoint/admission wait encloses most of this outlier; exact checkpoint substage remains unproved |
| Corresponding A2 EFA/direct slow windows | API 165.062/176.869 ms; foreground checkpoint 150.534/159.702 ms | Transport changes alone did not remove the checkpoint stall |
| B2 EFA warm 8 KiB scalar, explicit caches disabled | Native p50 1.220 us; remote 61.142 us | Remote software/message path dominates a warm comparison, not a measured cold-device service time |
| B2 EFA complete 32-key get-many | Native p50 41.151 us; remote 486.443 us | Grouped-read scheduling/transport remains important; this is complete-call, not per-key latency |
| B2 bounded frontend-cache scalar | Distributed p50 0.550–0.560 us, all 4,096 hits | The local hit fast path already meets the requested latency order; do not make it pay new locks or RPCs |
| A2 profiled direct Sui append-persist | Checkpoint/epoch/perpetual means 366.444/335.487/468.668 us; containing-file persistence 361.165/330.318/463.326 us | Typical durable-write cost is mostly file persistence, not a directory barrier or demonstrated lock contention |
| B2 Sui `checkpoint_content`, EFA | Native getter mean 9.159 us; remote 42.492 us | Application-level value retrieval remains slower; it is not the one-microsecond raw getter population |
| B2 EFA fresh/reused-transport reopen | 54.046/8.878 ms, one sample per lifecycle cell | Connection setup is avoidable for eligible same-process reuse; no p99 or repeated-reopen claim follows |

Raw correlated tail rows are `analysis-a2-kv/write_windows.csv:5103,7872,2333`.
The containing-file means are in
`analysis-a2-sui/interpretation-bounded-profiles.csv`. The original report links
the complete API, cache, profile, physical-IO and lifecycle populations.

Important limits:

- The B2 bulk population has ten windows, insufficient for p95/p99. Preserve A2's
  100-window case for tail work; more repetitions are needed for robust tails.
- The approximately 54-us validated RAM probe includes payload checking and local
  request retirement. It is not pure hardware RTT or an irreducible floor.
- Ordinary WAL-read completion need not wait for local request retirement after
  a valid reply. Do not add that wait while unifying measurements.
- Existing 34–38-us retirement spans include delayed completion observation.
  NIC generation versus owner observation, wire time and scheduler delay are not
  separately established.
- Sui owner physical-read deltas were zero. The generated private-chain replay
  does not establish mainnet-scale or cold-NVMe behavior.
- Native grouped cold reads are a serial control, not an optimized parallel
  single-device baseline. Do not claim horizontal scaling from that contrast alone.

## 3. Non-negotiable invariants

1. Complete application batches remain inside complete outer recovery frames.
   Inner member frames do not authorize replay on their own.
2. Keep one independent unpersisted group per current physical stream. A later
   acknowledged group cannot sit behind a possibly torn earlier group.
3. Exact containing-file and required namespace/witness persistence precede
   durable receipt publication. A successful enqueue or allocation is not durability.
4. Logical visibility and acknowledgment never pass the contiguous durable prefix.
   A timed-out/lost reply does not cancel accepted IO or prove an abort.
5. Checkpoint pages and history become durable before root adoption. Adoption
   precedes releasing covered metadata, pins or files. Old rooted pages are immutable.
6. Preserve fresh epoch reservation, full-node fencing, complete recovery inventory,
   read-authority rechecks, reader pins and relocation/retention bounds.
7. TLS sequence, CRC/digest validation, byte credits and DMA/reply ownership survive
   partial IO, EAGAIN, cancellation and completion reordering.
8. Keep hard bounds on every queue, buffer, registry, burst and progress loop.
   No unbounded cache growth or waiting to fill an otherwise ready batch.

These follow native WAL/snapshot principles [R1], with the fork's additional
multi-host ownership and explicit durable-acknowledgment protocol. Native paper
writes ordinarily finish in page cache; the durable benchmark deliberately adds
barriers to both backends. Do not drop those barriers to imitate paper throughput.

## 4. S0: correct measurement contracts and expose checkpoint causes

**Do first.** This is required to evaluate later changes, not a claim that
instrumentation itself makes the engine faster.

### Files and exact work

- `tidehunter/src/distributed/remote_db.rs::{AuthorityWriteStats,record_admission_span}`
  and `remote_db/checkpoint_lifecycle.rs`:
  - Retain the distinction between exclusive owner spans, background lifetime and
    enclosing foreground admission wait.
  - Add a bounded diagnostic checkpoint record keyed by authority epoch/generation.
    Record trigger/budget, captured cut/count/bytes and before/after reservations.
  - Pass worker timing/counters back with `CompletedCheckpoint`; the owner adds
    adoption and cleanup spans. The worker must not mutate live authority state.
- `remote_db/checkpoint_lifecycle.rs::start_checkpoint_with_floor`:
  separate staging-manifest persistence, history construction/flush/sync,
  node-prefix client wait, index preparation/persistence, checkpoint metadata
  installation, owner adoption and obsolete-file cleanup.
- `remote_db/receipt_history.rs`, `storage/history.rs`, `storage/history/index.rs`:
  count stream opens, previous-slot reads, node/receipt writes, bytes, buffer
  flushes and sync operations in the selected diagnostic capture.
- `tidehunter/examples/sharded_wal_bench.rs::{Output::write_window,main}`:
  correlate pressure waits with window sample and checkpoint generation, including
  captures started before the window. Replace emitted
  `foreground_checkpoint_is_subset=true` with the actual enclosing-span contract.
- `experiments/aws-sharded-wal/cluster_ram.py::read_metrics`:
  remove the false `foreground_checkpoint_ns <= checkpoint_ns` assertion.
  `foreground_checkpoint_ns` includes drain plus checkpoint wait; it can exceed
  `checkpoint_ns`. Preserve source, identity, cardinality and numeric validation.
  `scalar_transport.py::read_cloud_case` reaches this legacy validator.
- `experiments/sui-checkpoint/{run.py,analyze.py}` and
  `tidehunter/src/distributed/sui.rs::visit_timing_metrics`:
  export the selected new records with explicit sample count and availability.
  A missing span is unavailable, not zero.

The latest retained `analysis_tools/kv.py` does not call the false legacy nesting
validator. Do not invalidate or rewrite the latest raw exports because a legacy
consumer is wrong. Port maintained analysis changes into the fork, not a frozen
`checkpoint-work` source snapshot.

### Measurement design

- Use a diagnostic cursor before/after each timed write window to associate
  completed pressure records with the window; include waited-on generations.
- Use process-local monotonic elapsed times. Do not subtract clocks across hosts
  or unrelated quantiles. Client RPC wait contains server work; label nesting.
- Keep records in a preallocated bounded collection, with an explicit dropped
  record count. Export outside timed intervals. Do not lock/write a log on each
  scalar read, or add per-poll clocks when profiling is disabled.
- First reuse existing transport profile counters. New packet-type counters can
  be exported in node-local diagnostic files included in existing archives.
  This plan does not require extending the bincode wire-profile layout.
- Current wire `PROTOCOL` is 8. If implementation does extend serialized
  `TransportProgressStats`, perform one clean version/ALPN/schema cutover across
  every producer, consumer, fixture and build/runner guard. Never silently add
  bincode fields under protocol 8 or retain compatibility shims.
- Clean performance runs and bounded-profile attribution runs use separate fresh
  populations. Measure observer overhead; do not call profiled latency clean.

### Acceptance

A diagnostic slow window can be joined to its pressure reason, capture cut,
worker phases and owner adoption, with bounds/availability explicit. A valid
foreground span larger than the checkpoint span is accepted by maintained
analysis; malformed IDs, durations and sample counts remain rejected.
No permanent test should merely pin CSV wording or copy a mock's output.

## 5. C1: bounded, demand-aware admission checkpoint progress

**Primary target:** A2 bulk-write p99 and unnecessary maintenance.
**Mechanism:** covered-cut progress rather than unconditional catch-up, informed
by [R1] asynchronous snapshots and [R2] captured-tail commit coverage.

### Current call sites that must all migrate

- `remote_db.rs::admit_prepared`: tail/known-identity pressure near 1391; aggregate
  partition-window pressure near 1420; index reservation pressure near 1430.
- `remote_db/preparation.rs::submit_many_inner`: aggregate prepared-window index
  pressure near 234. Do not fix only the scalar admission path.
- `checkpoint_lifecycle.rs::force_admission_checkpoint`: currently
  `drain().and_then(commit_checkpoint)`.
- Leave `commit_checkpoint`, explicit `flush_index` and shutdown fully caught up.

### Proposed algorithm

Use a private, allocation-free pressure description carrying the incoming
reservation, not a cached Boolean. Distinguish tail bytes/batches, known identities,
partition descriptor capacity, and additional index bytes. A prepared-window
index request carries its deduplicated incremental reservation.

1. Validate that the indivisible incoming batch/window can fit a fresh legal
   reservation. Preserve metadata-size/pre-effect checks and original errors.
2. Re-evaluate the relevant live predicate. If it fits, return.
3. Drain the already admitted work, as the current safe path does. Recompute
   reservations from current state: publication alone can release partition and
   reserved-index credits. If it now fits, return without forcing a checkpoint.
4. If a checkpoint exists, wait/adopt that capture once. Re-evaluate; if enough
   capacity was released, return even if an acknowledged suffix remains.
5. If still blocked, capture the current drained prefix once and wait/adopt it.
   Re-evaluate. If a legal fresh state still cannot fit, return the appropriate
   capacity/error result; do not loop or raise limits until it fits.

The helper performs at most one explicit wait/adoption of a pre-existing capture
and one newly requested capture after drain. Existing progress inside drain stays
bounded by the existing admitted-work limits. No distributed effect/logical clock
allocation for the incoming batch occurs before its reservation succeeds.

Do not reuse `index_bytes -= previously_reserved` blindly after the refactor.
Recompute the prepared window's additional charge plus current reservations;
drain/adoption may change the latter. Keep retries/conflicts fully preflighted.

### State and locking

Reuse the authority's exclusive owner and existing immutable background capture.
No new mutex, global worker or atomic counter is required. Keep background trigger
thresholds and all hard limits unchanged for the first comparison. No credits are
refunded on worker completion alone: adoption is the authority transition.

### Behavioral acceptance

Extend `remote_db/lifecycle_tests.rs` and `pipeline_tests.rs`:

- A paused captured cut releases enough capacity; a new write completes after
  that adoption without waiting for a second suffix checkpoint.
- Group-window pressure resolved by durable publication does not require a
  persisted checkpoint merely to continue accepting legal work.
- Aggregate `submit_many` index reservations behave identically to single admits.
- If the first adopted cut is insufficient, the next full drained cut supplies
  capacity or the operation returns a bounded error; no spin or limit overrun.
- Explicit `flush_index` still covers both the captured cut and acknowledged
  suffix. Keep `unchanged_flush_waits_for_captured_cut_and_then_covers_its_acknowledged_suffix`.
- Preserve interrupted adoption, retry identity, missing/corrupt authority and
  later-durable/earlier-unpublished cases in the existing lifecycle/pipeline suites.

Use channel gates/state transitions rather than wall-clock performance assertions.
Remove obsolete incidental counter relationships that assume every pressure wait
creates a checkpoint; do not replace them with another implementation-count pin.

## 6. C2: bounded checkpoint-history IO, without a format rewrite

**Targets:** checkpoint worker service, metadata syscall/write amplification,
foreground tails, flush/close. Node-side spill work is conditional on S0 showing
it contributes; typical sampled Sui appends had zero history spill.
**References:** [R1] metadata/value separation; [R4] bounded syscall batching.

### C2a. Coordinator stream batching: smallest first change

`remote_db/checkpoint_lifecycle.rs` already groups/sorts receipts by stream.
Replace its per-receipt `ReceiptHistory::append` loop with one stream-oriented
writer in `remote_db/receipt_history.rs`:

1. Open that stream file once; read/check its previously rooted terminal slot once.
2. Build successive entries from the authenticated receipt sequence and existing
   prefix state. Keep ordinal, digest, canonical padding and byte layout unchanged.
3. Stage contiguous 256-byte receipt slots in one bounded reusable buffer; flush
   with `FileExt::write_all_at` when full or the stream ends.
4. Keep tree insertion semantics and explicit sync/adoption unchanged initially.
5. Never retain open handles for every lifetime stream; process one active stream
   at a time and close its handle after the explicit batch finishes.

This reduces opens/previous-slot reads immediately without claiming to solve all
checkpoint cost. Reuse existing `PrefixDigest`, serializers and checksum code.

### C2b. Buffered immutable-node writes, after C2a attribution

Targets: coordinator `ReceiptHistory::{write_node,read_node,insert}` and, if needed,
node `History::{append,index_insert}` / `storage/history/index.rs::put_node`.

- Use a writer-local contiguous pending node range, not a shared unbounded map.
  Start with a 16-KiB node buffer and 16-KiB receipt buffer per active writer;
  these are proposed fixed budgets, not measured optimal settings. Charge actual
  reserved capacity/overhead to metadata budgets. Reuse existing descriptor chunks.
- Allocate offsets in the same order and encode/hash the exact existing slots.
  A writer lookup into its pending range must read/validate those buffered bytes;
  it cannot issue a disk read for a node not yet written. After flush, ordinary
  validated cache/disk reads apply.
- The immutable `HistoryReader` and live old-root readers must not see the
  unpublished writer overlay. Node append needs a private writer-aware lookup,
  not a changed read-authority rule.
- Flush pending ranges explicitly before file sync; sync all affected data before
  root installation. No fallible flushing in `Drop` and no acknowledged deferred
  IO hidden behind a destructor.
- On error retain fail-stop/no-adoption behavior. Unrooted node tails remain
  unreachable; only the unadopted receipt ordinal can be reconstructed, as today.
- Keep the existing 64-entry validated coordinator node cache and incremental
  `.cells` snapshot reuse; do not add a lifetime-sized authority/value cache.

No new crate, shared generic storage backend, Patricia-to-LSM migration, changed
on-disk version or altered root digest is required by this first design.

### Behavioral acceptance

Extend existing `receipt_history.rs` tests
`immutable_roots_page_exact_outcomes_without_retaining_completed_maps` and
`retained_tree_pointer_never_turns_missing_or_corrupt_pages_into_absence`, plus
storage history/crash coverage:

- More nodes than cache/buffer capacity; exact lookup, retry, prefix and reopen.
- Frozen older root remains valid and cannot see newly appended identities.
- A corrupt/missing node is an error, not an absent key.
- Interrupt before data sync, before root adoption and after adoption/before cleanup.
- Old rooted data is unchanged after a failed/torn buffered suffix.
- Bound metadata residency across many captures and reopens.

Compare syscall/byte counters outside clean timing. Keep only changes that reduce
the intended construction/service work without transferring a larger cost to
adoption or recovery. Structural opportunity is not causal proof of the 150-ms tail.

## 7. T1: ready-aware TLS receive and bounded output bursts

**Targets:** remote scalar/grouped exchange, owner CPU and windowed throughput.
**References:** [R3] immediate owner-loop work and [R5] inline completed TLS input.

### T1a. Derived endpoint readiness and rustls state

Files: `tidehunter-fabric/src/endpoint.rs`, `endpoint/resources.rs`,
`endpoint/efa_direct.rs`, and `tidehunter/src/distributed/tcp/reactor.rs`.

Proposed narrow API: `Endpoint::receive_ready(&mut self) -> io::Result<bool>`.
It validates endpoint health but performs no CQ poll, copy or post.

- Regular fabric readiness is the next stream-order completed RX slot with unread
  bytes (`!rx_pending[rx_read] && rx_len[rx_read] > rx_offset`), not any later slot.
- Direct readiness is the next ordinal with unread payload. Reordered packets
  behind a gap are buffered, but not readable stream bytes.
- Cache `plaintext_pending` and `peer_closed` in `Session`, independent of profiling.
  Initialize from rustls state after bootstrap; after `process_new_packets`,
  replace from `IoState`, and decrement after successful plaintext reads [R10].
- Fabric calls `read_tls` only when rustls wants input and ordered ciphertext is
  available. Empty/TX-only/CREDIT-only progress does not imply readable ciphertext.
- `receive()` skips an empty plaintext read, including between header and body,
  but preserves partial-frame deadlines and close/truncation handling. Buffered
  plaintext can remain ready after an empty CQ poll or `wants_read()==false`.
- Keep TCP's existing nonblocking read behavior. Do not add a per-sweep socket
  peek/poll merely to avoid a TLS call.
- Update reuse/idle eligibility and the existing `send_tls_plaintext` test helper
  so unread/terminal rustls state is never ignored.

### T1b. Bounded immediate output

Keep one explicit CQ acquisition per active sweep and existing no-poll IO adapters.
Use one stack-owned `OutputBudget` shared by the initial and post-dispatch output
turn. Initial proposal: at most `PROGRESS_BYTES` (64 KiB) newly admitted plaintext,
eight TLS transport-write attempts, and four newly submitted frames per sweep.
These are fairness bounds, not tuned performance claims.

Within that budget: write ready plaintext, flush ciphertext with the existing
vectored adapter, update accepted/completed fences, advance to the next already
queued frame when its predecessor is fully transport-accepted. Stop at transport
WouldBlock, zero transport write, no useful progress or exhausted bounds. A
plaintext-writer WouldBlock may require a ciphertext flush; do not suppress it.

No wait-to-fill, extra CQ read, renewed operation deadline or early lease release.
The direct path has only three DATA TX slots: the burst cannot assume more slots
become available without the next explicit sweep. Input already drains up to
`SESSION_REQUESTS` frames; do not implement a redundant input-batching layer.

### Acceptance and existing suites

Extend `tcp/{reactor_output_tests.rs,reactor_tests.rs}` and fabric
`endpoint/resources/nonblocking_tests.rs`:

- Fragmented TLS/header/body input and multiple buffered frames complete correctly.
- EOF, close-notify, truncation, oversized frames and absolute timeouts stay terminal.
- TX-only sweeps do not erase previously unread receive data.
- Ready replies submit while earlier CQEs are delayed, without overtaking or freeing
  DMA/reply leases. Cold completion and shutdown remain serviceable under output.
- Shared sweep bounds hold across both output turns; no hidden second CQ consumer.

Use temporary diagnostic counters to evaluate empty calls/sweep efficiency;
permanent tests must establish message progress, ordering, boundaries and lifetime,
not merely assert that a mocked counter was forwarded.

## 8. T2: EFA-direct credit piggyback with guaranteed standalone progress

**Target:** redundant direct packets/completions and avoidable direct CPU overhead.
**Files:** `tidehunter-fabric/src/{endpoint.rs,endpoint/efa_direct.rs}` and reactor
sweep boundaries. **References:** [R3] credit work within an immediate owner loop;
[R7] the actual provider/completion contract.

### Selected state and sequence

Rename private `ControlWindow.last_sent_credit` to `advertised_credit` and define
credit debt as `receive.consumed() > advertised_credit`. No timer, new debt queue
or new mutex is needed. Successful DATA posting advances that frontier; an older
standalone CREDIT completion must never regress it. Keep standalone ordinals and
in-flight ownership separate from DATA ordinals.

A receive-only deferral is insufficient: current `direct_progress()` can post
CREDIT before the same sweep has produced its response. Split the reactor-facing
API into:

- `try_progress_for_sweep`: CQ dispatch/repost/control retirement, no early CREDIT;
- existing no-poll receive: consume bytes and leave computed debt;
- DATA output: on successful post only, advertise the latest consumed frontier;
- `finish_sweep_no_poll`: attempt standalone CREDIT for remaining debt, no poll/wait.

The server finishes after resident/cold completion harvesting, dispatch, response
recheck/queueing and the bounded output turn. The client finishes before every
successful correlated-progress return, including the return that completes a get.
Existing independently useful `try_progress`, `try_receive`, flush/blocking helpers
retain their immediate control-progress contract; do not impose an undocumented
begin/end-sweep obligation on their callers.

### Safety and acceptance

- Earn credit only on full ciphertext-packet consumption, not arrival/partial read.
- DATA/CREDIT EAGAIN changes no accepted bytes, ordinal, advertised credit or fence.
- Reserve the independent control slot. Output-budget exhaustion, full DATA slots,
  no-response traffic and large asymmetric streams must still return credit.
- Unposted debt and in-flight controls forbid idle parking. A failed post carries
  debt to a later explicit sweep, never a silent stall.
- All physical sends, including CREDIT, participate in contiguous fence retirement.
- Preserve CRC, monotonic credit validation, reorder bounds and TLS stream order.

Extend `endpoint/{efa_direct_tests.rs,resources/nonblocking_tests.rs}` and existing
software-provider transfer tests. Cover success/failed DATA piggyback, old CREDIT
completion after newer DATA, unidirectional transfers, partial consumption,
reordering, exhausted credits and shutdown. The large-stream model must actually
apply DATA-carried credit rather than encode zero credit on every DATA packet.

Export successful DATA/CREDIT posts and piggyback advances separately in local
diagnostics. A controlled receive/ready-response interval should need response
DATA but no redundant standalone CREDIT; a no-response interval must emit CREDIT.
Real 8-KiB fragmentation must be measured, not assumed from total send count.

## 9. T3: separately qualify idle CQ notifications

**Conditional, hardware-gated.** This primarily targets idle owner CPU. It can
worsen first-arrival latency; it is not an assumed scalar-read speedup.

Files: fabric `endpoint/resources.rs` and existing generated bindings; reactor
idle policy. Use [R7], not the stale comment that EFA supports no wait objects.

1. At setup qualify the deployed libfabric build/device for EFA and direct wait
   capability, including SHM restrictions. Only genuine unsupported-capability
   outcomes select polling; ordinary setup failures remain errors.
2. Prefer bounded provider-managed `fi_cq_sread` within the exclusive owner.
   A positive return already consumes CQEs: dispatch them through the same
   `Resources::complete` validation as ordinary reads.
3. An eligible idle wait replaces the next sweep's ordinary CQ acquisition; it
   must not become a second hidden CQ consumer. Do not assume raw FD poll/rearm
   is equivalent to the provider's regular/direct wait paths.
4. Initially permit waits only when fully drained: no output/TLS/terminal input,
   partial frame, outstanding request/worker/io_uring work, TX/reply fence,
   buffered out-of-order input, direct debt/control or receive-repost obligation.
5. Bound the wait by control-peer health, shutdown and earliest deadline. Preserve
   the current roughly 1-ms health-check bound; do not introduce a 10-ms sleep.
6. Preparation EAGAIN, timeout and intentional interruption resume owner progress;
   invalid/failed completions retain fatal behavior. Keep busy polling available.

Hardware acceptance must cover repeated arming/wake cycles and arrivals around
arming, both fabrics, peer close, shutdown, no lost/duplicate CQE, idle CPU, and
clean first-arrival p50/p99. Do not enable adaptive waiting by default from local
scripted tests or an endpoint-open-only qualification.

## 10. A1: separate barrier, read-cut, full checkpoint and reopen contracts

### A1a. Explicit durability barrier, without weakening flush

Current mappings:

- `RemoteDatabase::drain`: durable contiguous publication of prior submissions.
- `RemoteDatabase::checkpoint` / Sui `Db::checkpoint`: pinned read cut, not a
  persisted/adopted checkpoint.
- `RemoteDatabase::flush_index`: drain plus full checkpoint/adoption/cleanup.
- Sui adapter `Db::flush`: native Value-WAL barrier, remote full checkpoint.
- `project:sui/crates/typed-store/src/rocks/mod.rs::Database::flush`: native
  TideHunter returns `Ok(())`; remote calls adapter flush. This is another
  unequal-work boundary, not proof that the remote file barrier is slower.

Add `ConcurrentRemoteDatabase::durability_barrier` and adapter
`distributed::sui::Db::durability_barrier`. Its FIFO owner command establishes a
cut, stops later admissions, waits for prior durable contiguous publication, and
replies without explicitly requesting `commit_checkpoint`. Hold the existing
adapter exclusive write gate. Native uses its existing Value-WAL barrier under
that gate. Keep pending-control kind explicit, not several interacting Booleans.

Preserve existing flush/shutdown semantics. Full checkpoint reuses native
`force_rebuild_control_region` and remote `flush_index`; no second implementation.
Report `durability_barrier`, `checkpoint_capture`, `full_checkpoint`, fresh reopen
and reused-transport reopen separately. A no-op barrier after already durable
commits is not write-persistence service time; include write-plus-barrier cases.
Automatic pressure checkpoints can still occur during write windows.

Keep upstream typed-store behavior unchanged in this work. If later traces show
its remote flush is material, audit all callers before proposing a narrower
contract. Do not change that caller merely to improve a comparison.

Extend `remote_db/concurrent/tests.rs`, lifecycle and adapter coverage: blocked
earlier partitions, later-durable successors, concurrent writers, empty barriers,
owner failure, full checkpoint suffix coverage and recovery audits. Timing/key
renames require all maintained analysis consumers to migrate together, including
`analyze_cloud.py`, `cluster_ram.py`, `checkpoint_async.py`, `transport_floor.py`,
`scalar_transport.py` and their semantic tests. Do not alter frozen result labels.

### A1b. Same-process Sui transport reuse

Raw KV already uses `RemoteDatabase::open_with_transport_pool`; Sui
`Db::open_remote` currently chooses fresh open. Reuse the existing pool rather
than inventing another transport cache or TLS session implementation.

Selected design: a bounded process registry in the fork, selecting one existing
`Arc<RemoteTransportPool>` per canonical authority directory. A private open-mode
selection is resolved inside `open_inner` **after existing directory creation,
parent sync, directory lock and canonicalization**. This handles first-open paths
without duplicating directory durability or reacquiring the same lock.
Normal fresh and explicit-pool public APIs retain their existing contracts; only
the internal Sui entry point selects process-owned retention.

- Registry owner key is canonical authority path. Compatibility remains the
  existing `pool_key`: full config/shape/endpoint order and current credential bytes.
- One open/close-only `OnceLock<Mutex<...>>` is acceptable; it must never enter
  scalar/batch read/write paths. Do not add a single global one-slot pool.
- Initial proposed bounds: 16 namespace entries and 256 reserved connection slots,
  charged with checked `endpoints * (1 + read_lanes)`. These bound the optimization's
  registry-managed ownership, not all valid active DBs in a process.
- Evict only registry-idle entries; drop/close resources outside its lock. If the
  registry cannot admit an entry, choose normal non-retained open, not a DB error.
- Keep compatibility/reuse checks and exclusive connection ownership. `clear()`
  only empties idle connections; it does not revoke a checked-out DB or its return.
- Reuse only after successful drain/checkpoint/read-join/seal. Failures discard
  checked-out transports. Fresh status, new durable epoch, all-node fencing and
  recovery remain mandatory. Never rewind TLS or silently retry a failed checkout.

Extend `remote_db/transports/tests.rs` plus adapter lifecycle cases: multiple
namespaces, path aliases/same basename under different roots, credential rotation
at unchanged filenames, changed config/shape/endpoints, registry saturation,
failed shutdown, competing opens and old detached read handles.

This helps same-process close/reopen only. Sui replay and `state-audit` are
separate processes; no current result proves repeated in-process Sui reopen.
Measure that separately. Do not claim this registry eliminates fresh startup.

## 11. W1: only coalesce network appends when a real ready queue exists

**Conditional on queue evidence.** Coordinator grouping already exists and each
partition permits one append RPC in flight. A sequential producer or full group
has no extra server aggregation opportunity. First measure queued small compatible
RPCs, barriers per application batch and queue/service time.

If useful, extend `tidehunter/src/distributed/tcp/control.rs` using `worker.rs`
patterns and [R8] TiKV's immediate leader/ready-follower mechanism:

1. Take a bounded contiguous already-queued append prefix; singleton runs at once.
2. Never pass status/checkpoint/fence/seal/retirement controls or cross authority.
3. Bound exact group bytes, whole batches, descriptors and temporary memory using
   `CompleteGroup::encoded_member_len` and fresh reference-capacity checks.
4. Preflight requests independently. Keep retries, conflicts, cross-request
   duplicate identities/versions and malformed/mixed-old-new RPCs on their original
   path, so a bad later RPC cannot invalidate a valid earlier one.
5. Append one combined outer recovery group, persist it, then map exact ordered
   receipt slices to original RPC identities. Do not append separate unpersisted
   groups and share a later sync under the current recovery protocol.
6. Cancellation does not cancel accepted IO; unknown outcomes poison authority and
   stop queued emission. Shared diagnostic spans must be explicitly shared, not
   reported as additive exclusive per-RPC times.

For the first coalescing implementation, leave RPCs requesting the existing
per-RPC profile on the original path. Label that difference explicitly; such a
profile cannot explain the clean coalesced path. Measure coalesced IO with a
bounded node-local group trace identifying its covered RPCs, without adding
ambiguous shared spans to the old wire profile.

Use existing `tcp/control_tests.rs`, worker retry/failure cases, `group_model.rs`
and `storage/crash_tests.rs`. Validate FIFO controls, active credits, cancellation,
torn siblings and failed persistence. Do not add a batching timer or a second WAL.
If no useful queue exists, leave this path unchanged and report that result.

## 12. B1: one fleet with complete, isolated phases

This is runner work, not an engine optimization. No cluster is launched now.

### Maintained entry points

- `experiments/aws-sharded-wal/sui_checkpoint.py::SuiCampaign.{execute,run_matrix,export}`
- `checkpoint_kv.py::{run_suite,CheckpointKvCampaign}`
- `checkpoint_matrix.py::{matrix_plan,validate_matrix}`
- `guest.py::{Campaign.run_case,node_action}`
- `atomic_start.py::{BINDINGS,configuration}`

The old `project:checkpoint-work/.../campaign_ops.py::plan_experiment` admits one
scope. Its launch/retrieve/cleanup logic is historical evidence; implement the new
phase admission in maintained runner code and bind it into the next source bundle.

### Selected phase contract

An immutable bounded ordered plan contains phase ID, kind, exact case specification,
expected counts, work allowance and export allowance. For unchanged original
coverage use targeted Sui, targeted KV, full Sui, full KV: 15 Sui pairs, 84 KV pairs
(168 backend executions), six RAM processes. New candidate repetitions must be
explicit extra admitted cells, not silently substituted or omitted coverage.

Keep one fleet RunId/root/S3 prefix, deadline, outer export reserve and termination
`finally`. Do not call `SuiCampaign.execute()` twice on the same fleet.

- Prefix case/cell identity end-to-end with phase: directories, systemd units,
  deployment/config keys, node stop markers, result/proof identities and analyzers.
  Targeted/full currently collide in both `r01-*` Sui and `storage-current-*` KV IDs.
- Install/hash-verify shared binaries once. Re-entering current KV install would
  fail on its `exist_ok=False` directory; path renaming alone is insufficient.
- Require exact process release before next phase, including unknown starts and
  PID/starttime evidence, not just successful stop submission. Preserve Sui's
  existing release handshake and bring KV boundaries to the same standard.
- Fresh UUIDs/data directories and explicit cache state per phase. Reuse is a
  named same-process experiment, not accidental cross-phase transport state.
- Export/verify controller evidence per phase as well as current node-per-case
  exports. Preserve prior/partial exports if a later phase fails.
- Termination must still run when stop, export, benchmark or validation fails.
  Independent exact-owned-ID closure verification remains separate.
- Start another phase only if its admitted work plus export reserves fits the
  original absolute deadline. No silent timer extension or truncated matrix.
- Local download/analysis cannot keep hosts alive. Verify retrieval before deleting
  S3 results/storage/IAM/support resources; never touch another user's resources.

Extend `experiments/aws-sharded-wal/{test_checkpoint_matrix.py,
test_sui_checkpoint_supervisor.py,test_atomic_start.py}` and
`experiments/sui-checkpoint/{test_cloud_node.py,test_analysis.py}`. Cover ID/stop-
marker collisions, second-suite installation, failed release/export, unknown start,
fixed deadline and later-phase failure retaining earlier evidence.

## 13. Implementation ownership and ordering

1. **S0**: one integration owner defines trace/span/schema contracts and fixes the
   false timing assumption. Freeze those interfaces before independent edits.
2. In parallel after S0:
   - checkpoint owner: C1 then C2a; C2b after construction attribution;
   - fabric Rust owner: T1 and T2 together, because they share sweep boundaries;
   - adapter/benchmark owner: A1 and B1; no competing edit of frozen reports.
3. Integrate and run local behavioral checks once the slices settle. Do not run
   full suites/formatters repeatedly while agents edit shared files.
4. Keep T3 and W1 as separate evidence-gated decisions, not prerequisites claimed
   complete by unsupported local hardware or absent aggregation opportunity.
5. Update `tidehunter-fabric/DESIGN.md` and the maintained implementation report
   after actual behavior is verified. State any upstream-Sui change precisely.

Do not split T1 and T2 between uncoordinated owners editing `Session::progress`.
Do not remove native/fencing locks to meet a lock-free label. Fix the current
reader-authority try-lock wording when its implementation is touched: the
`readers.read()` guard is blocking, although it is not the measured RAM-path cause.

## 14. Validation gates and claim limits

### Local gate

Use existing Cargo features (`experimental-distributed`, `experimental-fabric`)
and suites, not new dependencies or a substitute synthetic-only proof. Relevant
future commands from the fork root include:

```sh
cargo test -p tidehunter --lib
cargo test -p tidehunter --features experimental-distributed --lib
cargo test -p tidehunter-fabric --features native --lib
cargo test -p tidehunter --features experimental-fabric --lib
cargo build --release -p tidehunter --features experimental-fabric --example sharded_wal_bench
```

These commands are planned, not run for this Markdown task. Hardware/software-
provider gated cases must be reported separately from executed cases. Run project
format/lint checks after integration; do not suppress failures or fake EFA locally.

Exercise the existing five-shard local harness and generated Sui checkpoint replay,
with exact-state/tombstone/retry/reopen audits, bounded-cache hit and miss cases,
100-window pressure case and partial/concurrent IO. Record CPU, RSS, VmSwap,
physical IO, dirty/writeback state and cache counters. Compare the same previous
local cases and source identities; the laptop's shared SSD is not five NVMe drives.
Use deterministic consumer-visible regressions for state transitions and errors;
use throwaway diagnostic scenarios for performance/call-count hypotheses.

### Next authorized hardware gate

Only after local correctness and new binary/source manifests are verified:

- Rebuild all six campaign executables, including native/remote Sui, from the
  current admitted source/dependency state. Record hashes before upload and verify
  hashes/executables again on every host. Never reuse an old READY as new code.
- One controller and five independent-NVMe storage hosts, cluster placement;
  preserve native/TCP/EFA/EFA-direct controls and per-cell hardware activity proofs.
- Preserve the full replay/KV coverage; use at least three clean paired repetitions
  for the critical scalar/grouped/tail comparisons and separate bounded diagnostic
  cases. Admit their total runtime/cost envelope before allocation.
- Measure RAM exchange, payload validation and local retirement separately; warm
  scalar/member reads, complete 32-key calls, windowed reads, bounded frontend hit/
  miss, eviction/physical-IO controls, 100-window checkpoint tails, full maintenance
  and fresh/same-process-reused reopen. Add a parallel native cold-read control
  before claiming an independent-disk scaling advantage.
- Capture actual Sui checkpoint-completion/service distributions and decoded-hit
  latency if instrumented; current execution-start spacing/getter histograms do
  not supply those metrics. Any required Sui instrumentation is a separate,
  documented small change, not inferred data.
- Export full-state audits, raw samples, counts, diagnostic availability, source/
  config/provider identities and closure receipts. Fully terminate/clean the owned
  fleet before offline analysis; retrieved results must survive resource cleanup.

### Success and stop rules

Compare each candidate with its matched baseline and paired native control,
`gap = distributed / paired_native`, under the same API, durability, cache state,
workload, hardware and profiling mode. Report counts/raw samples and variability;
a lower counter or a single lifecycle sample is not proof of a latency win.

Accept a change only with unchanged correctness/bounds and repeatable improvement
in its intended clean metric, without a repeated material regression in other
critical cases. If a result is noise-limited or missing, report it as inconclusive.
Publish p50/p95/p99 only where sample counts support them; histogram quantiles
remain bounds. Never add overlapping spans or subtract unpaired quantiles.

If C1 removes redundant captures but the tail remains, use S0's correlated phase
trace to choose the next operation. If T1/T2 reduce empty work/packets but clean
latency stays unchanged, do not promise that more polling tweaks close the gap:
obtain completion-generation/observation and scheduler evidence. A device/wire
clock is unavailable unless the deployed stack actually exposes it.

Sub-microsecond local hits are established. Native-RAM parity for every remote
fetch is **not established**. Later autonomous-commit/per-core IO experiments
[R6, R9] require a separately proved recovery/ack protocol if they change grouping
or persistence. NVMe-oF cannot fix coordinator checkpoint adoption; one-sided RDMA
requires new access, fencing, pinning and reclamation guarantees. Reconsider those
architectures only against the residual measured path, not as a presumed cure.

## 15. Exact source-to-fix references

These are mechanisms to adapt, not code to paste without license and contract
review. No new dependency is proposed. The previous QMD review used the actual
`tidehunter-papers` collection; current source/pinned-code inspection refines it.
This is not a claim that every paper in the corpus was read for this plan.

| ID | Exact primary source / section or implementation | Specific use here | Not transferred |
|---|---|---|---|
| R1 | [Tidehunter v2](https://arxiv.org/abs/2602.01873v2), §§2.3, 3.1–3.4, 4.3–4.4; [local parsed paper](../../../papers/parsed/Tidehunter-2026-v2.md) | C1 asynchronous covered snapshots; C2 metadata-only persistence; all WAL/pin/relocation rules | Ordinary page-cache completion is not a durable per-write ack; relocation is space reclamation, not LSM read compaction |
| R2 | [Garnet/Tsavorite `LogCommitPolicy`](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/LogCommitPolicy.cs); [commit implementation](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs#L3359-L3457) | C1 captured-tail coverage/redundant work suppression; A1 distinguish allocation from durable coverage | Rate-limit timers, failure-skipping, separate full-value operation WAL |
| R3 | [eRPC NSDI paper](https://www.usenix.org/system/files/nsdi19-kalia.pdf), §§3–5; [owner event loop](https://github.com/erpc-io/eRPC/blob/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d/src/rpc_impl/rpc_ev_loop.cc) | T1 bounded immediate useful bursts, T2 credit work and loop-end flushing | Published small-RPC latency as an EFA/TLS prediction, unsignaled-send lifetime shortcuts |
| R4 | [KVell SOSP paper](https://doi.org/10.1145/3341301.3359628), §§4–5.6 and §6.5.1; [author overview](https://github.com/BLepers/KVell/blob/master/OVERVIEW.md) | C2 bounded device/syscall batching principle | Prototype no-WAL slab recovery and ≤4-KiB item/atomic-write assumptions |
| R5 | [Garnet paper](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf), §§3.1–3.3, 5.1–5.2; [synchronous TLS input](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/common/Networking/NetworkHandler.cs#L568-L614); [buffered async continuation](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/common/Networking/NetworkHandler.cs#L668-L687) | T1 completed input processed inline; cached plaintext must progress without more ciphertext | An assertion that Garnet is universally lock-free or its eRPC prototype is the open-source default |
| R6 | [Autonomous Commit, SIGMOD 2025](https://dl.acm.org/doi/10.1145/3725328), §§3.1, 3.3, 3.5 | Later investigation only: separate small aligned durable flush from dependency-aware ack | Dropping current barriers, arbitrary splitting of atomic batches, unproved multi-stream recovery |
| R7 | [libfabric 2.6 EFA manual](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md); [`efa_cq_trywait`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_cq.c#L523-L560); [RDM bounded read/wait](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_cq.c#L1018-L1074) | T2 completion/credit obligations; T3 capability and arm/recheck/dispatch sequencing | Assumed device support, raw FD wait without rearm proof, second CQ consumer |
| R8 | [TiKV raft-engine write barrier](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/write_barrier.rs#L170-L225); [`Engine::write`](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs#L147-L235) | W1 immediate singleton and ready-prefix amortization, aggregate durability demand | Separate unpersisted frames before shared fsync under this fork's different recovery protocol |
| R9 | [BtrLog paper](https://arxiv.org/abs/2606.27051v1), §§3, 5; [Rust/TLA+ artifact](https://github.com/maxi-k/btrlog) | Later measured per-core IO/stream-ownership option; distinguish durable from committed frontiers | A production-library guarantee, forced global total log, or new duplicate value persistence |
| R10 | [rustls 0.23.31 `IoState`](https://docs.rs/rustls/0.23.31/rustls/struct.IoState.html) | T1 authoritative plaintext-byte and peer-close state | Treating `wants_read()` as plaintext readiness |

FASTER is historical context, not a newly selected backend: its
[official repository](https://github.com/microsoft/FASTER) directs active work to
Garnet/Tsavorite. Current [post-campaign findings](POST_CAMPAIGN_PERFORMANCE_PLAN_2026-10-18.md)
retain the broader source review, evidence limits and benchmark retention trace.
