# Implementation plan: checkpoint admission and remote WAL fast paths

Original specification baseline: completed 18 October 2026 native/TCP/EFA/EFA-direct campaign.
Status: **specification plus source implementation record**; see §16 for the
21 October integration, the newer 20 October hardware baseline and validation
ownership. The earlier proposed sections are not blanket completion claims.

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

## 16. Implementation and measurement record, 21 October 2026

This records source integration, parent-executed local gates (§16.6),
single-SSD diagnostic attribution (§16.7) and matched-feature local adapter
observations (§16.8), **not a new hardware result or general native parity**.
Protected publication, the final six-binary Sui replay and hardware validation
remain separate gates. No child task built, tested, formatted, dispatched a
workflow or launched AWS resources. The stable `2026-10-20` branch/worktree
labels do not redefine the observed UTC date.

### 16.1 Newest completed hardware baseline

The baseline is successful protected
[run 39378783532](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/39378783532),
not an unfinished campaign or rebuilt binary relabelled as that run. The retained
report is
`project:checkpoint-work/aws-ci-runs/39378783532-1/RESULTS-39378783532.md`.
It binds engine `b3ca2f03c5d7e3274f2aa22d7543b5b82bbefcee`, Sui
`3261f3be974c941584014438159110140a2885a7`, source archive
`49deb1e90c32ee5008467d576f15223251ace77f36cafcd284c92fbd9b2696c9`
and build manifest
`590fc096ba9eea2f331cdd62e7e6277abccdedda815e996cc714700d79acbdcd`.
The six `g6.8xlarge` hosts were in cluster placement in `us-east-1a`, with one
controller/native host and five distinct instance-NVMe storage hosts, RF1. All
owned instances were terminated; the historical report and raw files are unchanged.

The certified fixture is the **private chain**, genesis through checkpoint 1775,
digest `EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`; it is not mainnet.
All six retained Sui audits agreed on 57 tables / 167,858 rows across three
Tidehunter namespaces and the retained RocksDB groups. That campaign had six
Sui pairs and 51 KV specifications / 102 backend executions: 39 clean and 12
diagnostic KV pairs. It had only **one clean observation per critical cell**,
not three clean repeats. Diagnostic observations must not be pooled as repeats.

Relevant baseline limitations remain:

- Warm uncached remote scalar medians were about 45–169 microseconds across the
  listed transport/value cells; these were resident/pagecache WAL service, not
  measured NVMe latency. The report's process physical-read deltas for Sui were zero.
- Batch-one cache cells had 4096 frontend hits / zero misses. The 8-KiB,
  batch-32/window-32 cells labelled cache had thousands of misses; their medians
  are not hit-only latency measurements.
- A cold interval with positive charged read bytes proves interval IO, not that
  each returned value required device IO. `async_read_submissions` and io_uring
  SQEs alone do not establish physical device reads.
- Reopen/lifecycle cells with one sample do not support p95/p99. Sui histograms
  provide bucket bounds, not exact quantiles. Hardware/provider changes and a
  different Sui source cannot be treated as an isolated before/after experiment.

The untouched local engine baseline is
`/tmp/tidehunter-perf-baseline-2026-10-20-ksb28u4n`, from
`b7123d6653b2b690a72fe5db8fcbdfb0419784e2`; the compatible local Sui input is
`6d4b704bb6b9af6e3d836e08ac3aa2161d443b2d`. This is a **newly frozen local
comparison**, not the old hardware source. In particular the new Sui collector
cardinality fix was absent from run 39378783532.

### 16.2 Problem-to-symbol and source-to-fix mapping

| Problem / maintained code | Integrated mechanism and invariant | Mechanism source and boundary |
|---|---|---|
| Hashing a full 162-byte `ValueRecordReference` and updating admission history on every scalar hit; `remote_db/read_cache.rs::{fingerprint,get_point}` | Salted placement hashes the 16-byte batch operation ID plus the 4-byte member operation index: 20 input bytes rather than the intermediate digest-plus-index 36 bytes or original full-reference 162 bytes. The stored **complete** reference and exact key/keyspace still decide a hit. Successful scalar hits do not need admission-history lookup; misses still maintain bounded history. No key-first/index bypass, stale epoch/offset alias, new unbounded cache or native-hash replacement is introduced. The member digest includes batch identity/version; this does not claim identical payloads collided under the previous digest placement. | Repo-local work elimination, not a transplanted hash algorithm. QMD `#366e94`, Garnet §4.7 describes derivative hot-record residency; [pinned Tsavorite `ReadCache.cs`](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs), `FindInReadCache`, `ReadCacheNeedToWaitForEviction` and `ReadCacheEvict` provide the read/eviction consistency boundary, not this fork's compact fingerprint. |
| Unconditional scalar timing work; `remote_db/pending_read/concurrent.rs::resolve_concurrent_get` and Sui scalar stage | Diagnostic clocks are conditional on the explicit read-profile selection. `LocalDbStats::read_resolve_ns_available`, `scalar_read_ns_available` and `read_index_ns_available` distinguish unavailable clean spans from measured zero. API benchmark timers are a separate population. | eRPC QMD `#f39b71`, §§3.1–3.2 distinguishes event-loop and worker work; this change is removal of repository observer work, not an eRPC latency prediction. No native mutex is removed. |
| Unconditional checkpoint catch-up; `remote_db.rs::ensure_admission_pressure_inner`, `remote_db/preparation.rs::submit_many_inner` and checkpoint lifecycle | Re-evaluate the actual reservation predicate after drain/adoption and stop once the incoming indivisible batch/window fits. The existing capture and at most one new capture bound maintenance; explicit commit/flush/shutdown still fully catch up. Metadata adoption, contiguous publication, whole-batch placement and native WAL barriers remain authoritative. | Native Tidehunter §§3.1, 3.3–3.4 and 4.4; QMD Garnet §5.1 (`#366e94`); [pinned `LogCommitPolicy::AdmitCommit`](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/LogCommitPolicy.cs) specifies captured-tail coverage and suppression of redundant requests. No timer-driven commit, global physical log or Garnet recovery protocol is imported. |
| Empty TLS/fabric polling work; `Endpoint::receive_ready`, TCP `Session` input/progress | Readiness derives from ordered unread ciphertext/direct payload and rustls buffered plaintext/close state. Initial and post-dispatch output turns share bounded budgets. DMA, TLS record order and partial IO ownership remain unchanged. | QMD eRPC §§3.1–3.2, plus [pinned `Rpc::run_event_loop_do_one_st`](https://github.com/erpc-io/eRPC/blob/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d/src/rpc_impl/rpc_ev_loop.cc); [rustls 0.23.31 `IoState`](https://docs.rs/rustls/0.23.31/rustls/struct.IoState.html) explicitly exposes `plaintext_bytes_to_read` and `peer_has_closed`. `wants_read()` is not plaintext readiness. |
| Standalone direct CREDIT packets despite a ready response; direct sweep progress | Sweep-only deferral gives ready DATA the first opportunity to carry current credits; advertised credit advances only after a successful post. Loop-end fallback retains necessary control progress and standalone APIs remain eager. | QMD eRPC §§4.2.2, 4.3.1, 5.1 and its loop-end burst in the pinned implementation above; libfabric [pinned EFA CQ source](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_cq.c). This is two-sided authenticated message IO, not one-sided RDMA. T3 hardware CQ sleeping remains deferred. |
| Incorrect enclosing-span interpretation and missing checkpoint causes; `sharded_wal_bench`, `cluster_ram.py::read_metrics`, `checkpoint_kv.py::validate_checkpoint_records` | Remove the false foreground-subset assertion and metadata claim; exclusive parent sums still must fit API wall time. Diagnostic windows export cursors and serialized bounded records with explicit availability, count and dropped count. Captures begun before a window remain identifiable by epoch/generation/cut. Null phase duration means unavailable. | Native snapshot/adoption ordering above. S0 separates worker spans, owner adoption and enclosing drain/wait; no subtraction of unrelated percentiles or cross-host clocks. The false validator is fixed only in maintained code, not historical exports. |
| RAM control was built but full suite never ran it; `checkpoint_kv.py::plan`, `guest.py::Campaign.run_case`, `transport_floor.py::{subsuite_floor_spec,read_floor_metrics}` | The first clean 256-byte scalar case per transport now runs the existing standalone control before its external nodes are stopped. Clean and profiled outputs have distinct identities and independent repetition counts. Hashes, exact argv, every payload/repetition, unchanged WAL counters and COMPLETE are validated again by the closed-campaign collector. | eRPC QMD §6.1 separates a small-RPC control from application work. Our API includes TLS, exchange, payload validation and local retirement: it is **not pure hardware RTT** and does not require ordinary value reads to wait for local request retirement. |
| A cache-labelled population can actually miss, and sequential cache tests miss contention; existing adapter benchmark plus `support/cache_contention.rs` | Reuse `distributed::sui::Db` and the same shared helper in both existing drivers. The controller's existing `storage-node` **benchmark mode**, not its storage daemon role, opens the same coordinator path with the external configuration after the original raw-engine phases. One/eight caller populations must prove every expected value, exactly one hit per get, zero misses/RPCs, and actual read/write API overlap for every caller. All keys/tombstones are audited again afterwards. | Native §3.2 and Garnet §4.7 motivate explicit local-hit versus record-fetch populations; neither authorizes bypassing current health/index identity. Native hit-count instrumentation is unavailable, so native is a warmed API control, not a counter-proven hit claim. |

The cited QMD sections were read through `doc_toc`/`doc_read` in the actual
`tidehunter-papers` collection. Primary eRPC, Tsavorite and rustls sources above
were fetched independently; their symbols, not generic reputation, support the
mapping. No external source is evidence that this candidate is faster. Existing
inline reads, singleflight, multiplexing, whole-batch placement and native locks
are preserved rather than reimplemented.

The scalar-facade asymmetry is explicit in `distributed/sui.rs::Db::get`:
both backends increment wrapper API/key counts, but the native branch directly
calls the native `Db::get`, whereas the remote branch previously always entered
the scalar `Stage`. The remote branch now uses `Stage::run_if` and the same
`ConcurrentRemoteDatabase::read_profile_enabled` selection as resolution timing.
The RAII stage guard preserves call/error counts, including unwinding, while
omitting the clean scalar clock/ns accumulation. This does **not** mean all clean
timestamps disappeared: existing bulk, write and wait stage timers and the raw
`RemoteDatabase` benchmark stages are unchanged.
The two availability flags are explicitly zero for the native facade, even for
an empty observed population.

### 16.3 Measurement/output contracts

- `checkpoint_diagnostics.jsonl` is empty for clean/native populations. In a
  selected remote diagnostic population every write-window row has a matching
  JSONL phase/sample identity, `cursor_before`, snapshot `next_cursor`,
  `record_count`, `dropped_records` and serialized diagnostic records. The ring is
  bounded at 128.
  The drop count is global from cursor zero, not a window-local loss count:
  `dropped_records = max(0,next_cursor-128)`. The collector requires the exact
  contiguous retained suffix from `max(cursor_before,dropped_records)` through
  `next_cursor-1`, validates its bounded length before iteration, and rejects
  omitted records even when the stated record count agrees with the omission.
  Source `MutationVersion` cuts serialize as
  `{batch:{epoch,sequence},operation_index}`, not as a physical WAL address.
  Optional worker phases are null when unavailable. Serialization and ring
  snapshots are outside the individual timed write API.
  Completion records identify generation/cut and flat worker/history counters,
  with null pressure trigger/outcome/wait. Pressure records identify the
  admission reason/outcome and `checkpoint_generation` link, with null worker
  phase fields. Join those identities rather than charging the same checkpoint
  construction span once for every waiter.
- `cache_contention.jsonl` retains common-process monotonic nanosecond start/end
  intervals **per caller and per get**, and all synchronous durable-write API
  intervals. Each population has 1024 gets per caller and 32 writes to a disjoint
  existing key. The one/eight caller groups are separate observations. The
  collector independently recomputes overlap and rejects a caller with zero
  overlap. This proves overlapping API calls, not simultaneous device IO.
  Counter reads, value checks and output serialization are outside get timers.
- The adapter benchmark retains raw API samples; p99 is unavailable for fewer
  than 100 samples, including its 16-iteration iterator population. The shared
  contention helper introduces no additional engine hot-path timestamps.
- `auxiliary-measurements.json`, generated by the existing closed-campaign
  collector, preserves RAM control summaries/raw-evidence identity and cache
  observations. Read-service evidence keeps frontend/owner counters, owner
  resident/pagecache successes, async submissions and charged physical-read
  bytes separately. Missing charged-IO evidence is unavailable; positive bytes
  apply to an interval, not each sample. SQEs are never relabelled device reads.
  Owner counters are available only for the exact configured remote owner-ID
  population. Missing native/unrecorded owner counters remain null, not the
  zero produced by summing an empty collection; partial or wrong remote owner
  populations fail validation. A complete observed remote population may
  legitimately report zero resident successes or async submissions.
- Sui analysis masks `scalar_read_ns` / `read_index_ns` unless their matching
  availability flag is one. The current replay validator requires explicit
  flags matching the selected clean/diagnostic limit. Getter wrapper histogram
  and complete checkpoint replay/full-state audits remain distinct metrics.
  Checkpoint summary fields use `write_checkpoint_diagnostic_` plus the exact
  `CheckpointDiagnosticSummary` field name, alongside
  `write_checkpoint_diagnostics_available`. Phase means use each phase's own
  `_count`; zero samples is unavailable, not zero latency. The flat history
  stream/open/previous-slot/write/byte/flush/sync totals have a separate
  `history_io_checkpoint_count`. They describe logical construction/syscall
  work, not physical device IO. Clean unavailable history spans/totals are null
  in maintained analysis; count/cursor diagnostics stay explicit.
- Diagnostic-only fabric post evidence is the existing stderr line
  `TIDEHUNTER_FABRIC_LOCAL_DIAGNOSTICS ` followed by schema-1 JSON with exactly
  `schema`, `reason`, `window`, `data_posts`, `credit_posts`,
  `piggyback_advances`, `peer`. Reasons are `profile-reset`, `profile-disable`,
  `session-abort`, `session-drop`; window IDs are process-unique positive u64s,
  allocated only on diagnostic enable/reset. Peer address metadata is nullable.
  These are local endpoint progress-counter deltas, not protocol-8 fields,
  per-RPC packets, whole-payload packetization, RTT or latency measurements.
  All-zero windows are omitted; absence is not a zero measured window.
- `checkpoint_matrix.py::parse_fabric_local_diagnostics` bounds each log scan
  to 128 MiB, each line to 64 KiB and records to 4096. It rejects malformed or
  duplicate JSON keys/window IDs, truncation, unknown reasons, noninteger or
  overflowing u64 counters, all-zero records and oversized peer metadata.
  Current full KV admission requires finalized nonzero **EFA-direct diagnostic**
  client and explicit node-profile windows, including the diagnostic RAM
  control. Wholly clean and non-direct logs must not contain these records.
  The node log of the first clean scalar case also contains its later diagnostic
  RAM control, so its population is explicitly mixed rather than called clean.
  A clean cold case has another mixed population: the separate profiled
  `cache-evicted-read-diagnostics` certificate after its three ordinary cold
  API phases (`read_campaign.rs::run`). `checkpoint_kv.py::cold_probe_certificate`
  binds that certificate to the saved successful command, deployment, effective
  read lanes, exact owner/sample populations, primary read-path boundaries,
  auxiliary physical-IO interval, peer identities and successful post counters.
  The auxiliary pass has its own clocks; its samples are not primary clean API
  latency. Its subsequent remote reopen profiles are `reopened-cache-cleared`,
  not native `reopened-warm`. The auxiliary IO recorder does not emit logical
  read-path snapshots; missing counters remain unavailable. DATA plus CREDIT
  posts must equal post attempts minus WouldBlock and errors in the same
  finalized profile window. No timing, source, owner or completeness guard is
  replaced with a blanket permission for diagnostic records in clean logs.
- Exact existing captures: controller
  `results/kv/<case>-tcp-process.stderr.log`, RAM
  `results/kv/<case>-transport-floor[-profiled]-process.stderr.log`, and KV
  `results/kv/nodes/<case>/node-<partition>.tar.gz:node.stderr.log`.
  Sui client stderr is merged into
  `results/cells/<cell>/replay-logs/remote.log`; Sui node final archives retain
  `cloud-node-logs/namespace-XX.stderr.log`. Existing archives/export paths
  already retain these files. The closed collector emits
  `fabric-local-diagnostics.json` and includes KV evidence in
  `auxiliary-measurements.json`; maintained Sui analysis also parses finalized
  client/node windows. Sui absence remains explicitly unavailable where a
  process's SIGTERM may bypass Rust `Drop`; no complete-session or server-window
  coverage is invented. No extra runner, binary, wire field or clean log work
  was added.

### 16.4 Admitted bounded campaign and timing proposal

The approved configuration keeps all three transports, every current full KV
shape/family and the certified private-chain/full-state audit. Sui selects
three clean repetitions followed by one bounded diagnostic repetition per
transport: profile schedules `[0,0,0,128]`. Full KV uses three clean repetitions
plus the existing fresh diagnostic cases: **129 pairs / 258 backend executions**,
not an incomplete subset. Paired native/remote ordering and transport ordering
still counterbalance by repetition.

Only clean warm 8-KiB/batch-32/window-32 loads grow from 320 to 3200 batches,
providing 100 complete write-window samples per critical observation. The
existing value-history guard sees
`(3200 + 2*64) * 32 * 8192 = 872415232` bytes, **0.8125 GiB**, below its
unchanged 1-GiB limit. Cold footprints and other shapes are not enlarged.
The added RAM populations are six processes total: per transport clean
`2 values * 3 repetitions * 512 samples` and diagnostic
`2 values * 1 repetition * 256 samples`, with 64 warmups per owner/value.
Cached scalar specifications add 72 separate one/eight-caller observations
across values, repetitions, transports and paired backends, without adding a
seventh packaged executable.

Deterministic arithmetic from the retained run-39378783532 receipts:

| Input | Observed value / exact derivation |
|---|---|
| Controller work interval | `before.json` 07:17:47.013973Z to `after.json` 07:30:26.900039Z = 759.886066 s |
| KV interval including preparation/release gaps | Earliest retained prepare 07:24:08.778489Z to last process finish 07:30:24.937267Z = 376.158778 s |
| KV process totals | 102 processes, sum 161.6978230719988 s; clean 115.34213710799935 s, diagnostic 46.35568596399946 s; maximum 6.902220391 s |
| Largest old clean bulk warm pair | TCP native + remote process durations = 6.604594268 s (EFA 3.982578884 s; direct 3.933809208 s) |
| Existing Sui replay/audit processes | Six retained `finished_unix - started_unix` intervals, approximately 27.475–28.882 s; this excludes external lifecycle gaps |

**[INFERENCE], not a runtime guarantee:** a deliberately conservative proposal
uses twice the non-KV interval, three times the whole old KV interval, nine added
320-batch equivalents for each of the nine enlarged critical clean pairs, plus
90 s for cache controls and 60 s for RAM controls:

```text
2 * (759.886066 - 376.158778)
  + 3 * 376.158778
  + 9 * 9 * 6.604594268
  + 90 + 60
= 2580.903045708 seconds after controller start
```

The last two allowances are proposals, not observations. Earlier allocation to
controller-start overhead was approximately 253 s, leaving a narrow projected
margin under the **original 3000-second work cutoff**. The existing absolute
3600-second lifetime, 600-second export reserve, USD60 weekly cap, fresh budget
admission, IAM scope, unknown-allocation gates, strict completeness and automatic
outer export/termination remain unchanged. A missed deadline fails closed; it
does not authorize dropping a cell, extending the fleet lifetime or claiming
partial results complete.

This concrete single-full-matrix integration does not claim the older §12
four-phase targeted/full generalization (15 Sui / 84 KV pairs) was implemented.
That was a proposed runner architecture, not the coverage of the latest
successful run. Existing current full coverage is retained and explicitly
expanded here; historical phase/raw identities are not rewritten.

### 16.5 Parent-owned local commands and build entry points

The candidate is prepared from a **new clean detached worktree of the final
reviewed commit**, not the dirty original checkout. The user's untracked
`docs/distributed/SUPERVISOR_BRIEF_2026-10-20.md` stays unmodified and local in the
original checkout; no product exclusion policy is added. Parent-local admission
evidence must establish its absence before `prepare` and again from both frozen
source-manifest owners and the source archive inventory. Simply not staging an
unignored file would not protect a dirty-source snapshot.

From the project root, use an independent provider prefix already supported by
the maintained setup, **not an archived middleware dependency**:

```sh
ROOT="$PWD"
BASE=/tmp/tidehunter-perf-baseline-2026-10-20-ksb28u4n
export PKG_CONFIG_PATH="$HOME/.cache/tidehunter/libfabric-2.6.0/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export LD_LIBRARY_PATH="$HOME/.cache/tidehunter/libfabric-2.6.0/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
python3 "$BASE/experiments/sui-checkpoint/build.py" prepare \
  --sui "$ROOT/sui" --engine "$BASE" \
  --output "$ROOT/checkpoint-work/perf-final-baseline-build" --fabric --jobs 2
python3 "$BASE/experiments/sui-checkpoint/build.py" build \
  --output "$ROOT/checkpoint-work/perf-final-baseline-build" \
  --target "$ROOT/sui/target" --deadline 10800
python3 "$ROOT/checkpoint-work/perf-final-baseline-build/sources/tidehunter-sharded-wal/experiments/sui-checkpoint/run.py" \
  --build "$ROOT/checkpoint-work/perf-final-baseline-build" \
  --fixture "$ROOT/checkpoint-fixtures/sui-genesis-1775/fixture" \
  --output "$ROOT/checkpoint-work/perf-final-baseline-replay" --deadline 900 \
  --port 24000 --coordinator-cache-mib 8 --shard-cache-mib 0 \
  --read-profile-limit 0 --write-profile-limit 0 --native-write-profile-limit 0
```

Those output paths must be new; use a different fresh label if parent already
owns them. Candidate commands are the same `prepare`, `build` and frozen `run.py`
commands with `--engine "$CANDIDATE"`, where `CANDIDATE` is that new clean reviewed
detached worktree, and fresh `perf-final-candidate-build` /
`perf-final-candidate-replay` output names.
Repeat clean observations in fresh output roots; run bounded diagnostics in
separate roots with explicit limits. Run baseline/candidate replay serially:
the real fixture has an exclusive lock and both builds may reuse `sui/target`.

With the reviewed detached candidate path already assigned to `CANDIDATE`:

```sh
python3 "$CANDIDATE/experiments/sui-checkpoint/build.py" prepare \
  --sui "$ROOT/sui" --engine "$CANDIDATE" \
  --output "$ROOT/checkpoint-work/perf-final-candidate-build" --fabric --jobs 2
python3 "$CANDIDATE/experiments/sui-checkpoint/build.py" build \
  --output "$ROOT/checkpoint-work/perf-final-candidate-build" \
  --target "$ROOT/sui/target" --deadline 10800
python3 "$ROOT/checkpoint-work/perf-final-candidate-build/sources/tidehunter-sharded-wal/experiments/sui-checkpoint/run.py" \
  --build "$ROOT/checkpoint-work/perf-final-candidate-build" \
  --fixture "$ROOT/checkpoint-fixtures/sui-genesis-1775/fixture" \
  --output "$ROOT/checkpoint-work/perf-final-candidate-replay" --deadline 900 \
  --port 24000 --coordinator-cache-mib 8 --shard-cache-mib 0 \
  --read-profile-limit 0 --write-profile-limit 0 --native-write-profile-limit 0
```

`build.py` snapshots exact Sui/engine sources, resolves only owned copies,
verifies selected engine dependency metadata and takes executables only from
successful Cargo JSON artifacts. Its six packaged names/entry points are:

| Packaged name | Build entry point |
|---|---|
| `sui-node-native` | Sui `-p sui-node --features typed-store/tidehunter` |
| `sui-node-remote` | Sui `-p sui-node --features sui-node/sharded-wal-fabric` |
| `rocksdb-audit` | Sui `distributed_storage_local_audit` example with `typed-store/tidehunter,sui-node/testing` |
| `storage-node` | Engine `sharded_wal_bench` example, `experimental-distributed,test-utils,experimental-fabric` |
| `transport-floor` | Engine `sharded_wal_transport_bench`, same feature set |
| `state-audit` | `experiments/sui-checkpoint/audit`, feature `fabric` |

The existing independent libfabric prefix, Cargo registry/Git caches and
`sui/target` release cache can accelerate rebuilding, but none authorizes an old
executable or source archive to be labelled current. Execute replay using its
frozen runner path: maintained `run.py` deliberately rejects Python sources that
do not match the admitted source manifest. The standalone
`sharded_wal_adapter_bench NEW_OUTPUT_DIRECTORY [--profile]` remains the existing
local driver; its new contention rows are separate from the original API rows.

The existing engine/fabric and Python semantic suites cover exact repeated
coverage, real overlap/hit population, RAM interval IO, unavailable scalar/index
timings, complete checkpoint-record suffixes, configured-owner availability and
rejecting SQE-only physical-IO claims. Parent-executed local outcomes are
recorded below; this documentation edit executed no checks. A passing working
branch gate does not establish the source binding or replay outcome of a later
reviewed clean worktree and six-binary build.


### 16.6 Recorded parent-executed local gates and source limits

Authority:
`project:checkpoint-work/final-performance-2026-10-20/local-gates.json`,
observed **2026-10-21T11:19:33Z**. It records working branch
`perf/cache-fabric-checkpoint-2026-10-20`, engine base
`b7123d6653b2b690a72fe5db8fcbdfb0419784e2` and Sui revision
`6d4b704bb6b9af6e3d836e08ac3aa2161d443b2d`. These are parent-observed
local outcomes, not an immutable final candidate build-manifest identity.

| Recorded gate / population | Result |
|---|---|
| Native engine | 383 passed; 6 ignored |
| Experimental distributed engine | 785 passed; 7 ignored |
| Fabric library | 48 passed; 1 ignored |
| Engine with `experimental-fabric` | 785 passed; 13 ignored |
| Explicit software-provider cases | 14 executed; all 14 passed; hardware RDMA evidence explicitly false |
| AWS Python harness | Successful gate; 460 ran, including 3 skipped |
| Sui Python harness | Successful gate; 109 ran, 0 skipped |
| Clippy, native/distributed/fabric | Passed with `-D warnings` through the existing profiles |
| Format | Passed |
| Release benchmark drivers | Passed with `experimental-fabric,test-utils`; all three existing drivers |

The feature populations overlap; the table is not a sum of unique tests.
Ignored/skipped cases did not execute. Full local-gate evidence is
`artifact://5213`, Python harness evidence `artifact://5201`, and the
distributed result additionally names `artifact://5207` in the retained JSON.
There is no local EFA hardware: software-provider execution establishes only
the exercised correctness/progress behavior, not hardware EFA/Direct latency.
The laptop and local storage processes share one SSD; no independent-NVMe
scaling or cluster-placement conclusion follows.

The raw engine/Sui facade distinction in §16.2 still matters. New S0 worker,
adoption and cleanup phase clocks are gated diagnostic `Option` values; clean
execution does not populate those records. Existing aggregate checkpoint and
foreground wait timers remain measured. This is not a claim that every clean
clock, atomic counter or wrapper stage was removed.

The retained
`project:checkpoint-work/final-performance-2026-10-20/dependency-admission.json`
records **no added or removed package versions**. The existing locked
`serde_json` 1.0.140 (MIT OR Apache-2.0) gained a direct engine dependency edge.
`tidehunter/Cargo.toml` admits benchmark JSONL through a dev-dependency and
fabric stderr serialization through an optional dependency enabled only by
the existing `experimental-fabric` feature. The default native runtime edge
is unchanged. Rust's standard library has no JSON serializer; the existing
crate supplies correct escaping rather than a handwritten serializer. This
dependency admission is not a substitute for the final source/build manifests.

**Matched-feature adapter v2 is now recorded separately in §16.8.** The initial frozen
baseline adapter used distributed-only features and is retained as such, not
relabeled. Parent rebuilt the frozen clean `b7123d...` baseline with the same
`experimental-fabric,test-utils` flags as the candidate. The initial candidate
adapter ran the new contention helper before the ten original measurements;
parent moved it after all ten and made its expected hot/writer bytes follow
the final oracle. That ordering changes the state of later operations, so the
earlier candidate bulk/write timings must not be used for a controlled
comparison. The replacement matched-v2 observations below do not relabel those
initial runs; no cache, bulk or write speedup figures from v1 are asserted here.

As of this record there is **no final candidate six-binary Sui replay result
or new hardware result**. The local-gates JSON explicitly records
`hardware_campaign_dispatched=false`; the final source-bound build/replay and
protected publication were pending at its observation. These local checks do
not authorize substituting an old binary, an initial adapter run or the
20 October hardware source for the final reviewed candidate.

### 16.7 Actual checkpoint-pressure diagnostic and C2b decision

Authority:
`project:checkpoint-work/final-performance-2026-10-20/candidate-pressure-summary.json`.
It marks `validated_actual_producer_records=true`: **105 recorded windows,
35 distinct producer records**, comprising **28 checkpoint completions and
7 pressure records**. This is one local, shared-SSD diagnostic population,
not a clean before/after comparison, Sui replay result or cloud attribution.

The seven pressure outcomes were six `AfterExistingCapture` and one `AfterDrain`.
Pressure wait total was **242,297,671 ns** across seven records (mean
34.613953 ms); drain total was **116,342,426 ns** across seven records, and
existing-capture wait total **125,949,210 ns** across six. There were **zero
sampled new-capture waits**: their mean/max are null, not measured zero latency.
This shows relief after an existing capture or drain in this population, not
that no background/new checkpoints occurred—the 28 completion records remain
explicit. Seven pressure samples do not support p95/p99.

The actual completion-phase attribution below retains exact totals from the
JSON; displayed means are rounded to six decimal milliseconds:

| Diagnostic phase | Samples | Total ns | Mean ms |
|---|---:|---:|---:|
| Staging manifest | 28 | 52,367,200 | 1.870257 |
| Receipt-history construction | 28 | 54,461,520 | 1.945054 |
| Receipt-history sync | 28 | 238,771,829 | 8.527565 |
| Node-prefix client wait | 28 | 140,395,134 | 5.014112 |
| Index prepare | 28 | 29,153,834 | 1.041208 |
| Index persist | 28 | 1,561,482,744 | 55.767241 |
| Checkpoint metadata install | 28 | 46,849,548 | 1.673198 |
| Owner adoption | 28 | 87,121,686 | 3.111489 |
| Obsolete cleanup | 28 | 80,568,097 | 2.877432 |

Worker phases, owner stages and enclosing foreground waits must not be added
into wall-time shares: capture work may overlap foreground service, and a
waiter's enclosing interval is not another independent construction sample.
These are diagnostic elapsed spans, not CPU time, device service or network
RTT. There are fewer than 100 completion samples; no p99 is claimed.

Coordinator history construction performed the following **nonzero** work:

| Coordinator history counter | Recorded total |
|---|---:|
| Stream opens | 280 |
| Previous-slot reads | 135 |
| Receipt write API calls | 278 |
| Receipt write bytes | 762,624 |
| Buffer flushes | 278 |
| Merkle-node write API calls | **25,466** |
| Merkle-node write bytes | **6,519,296** |
| Sync operations | 196 |

These are coordinator receipt/Merkle-history API counters, not charged physical
device IO. Shard history spill/retention counters are a different population:
any zero shard-spill observation must **not** be reported as zero coordinator
Merkle writes. The actual coordinator node writes above are nonzero.

**C2a is instrumented and exercised here; C2b buffering is deliberately not
added in this cutover.** C2b was conditional on construction attribution.
Although the Merkle-node API count is material, the measured receipt-history
construction mean is 1.945054 ms, versus 55.767241 ms for index persistence;
receipt sync and node-prefix wait are also separately visible. Construction
was not the dominant measured completion phase on this shared SSD. Reducing a
write counter alone is therefore not evidence that buffering is the next
effective latency fix. Retain the current persistence/recovery ordering and
do not introduce buffering merely to claim C2b completion.
Existing index batching must not be mistaken for missing work:
`tidehunter/src/distributed/native_index/snapshot.rs::persist_with_hook`
already combines rewritten cells into **one `.cells` container**, using a
16-KiB `BufWriter` rather than per-cell seek/flush. When a new container is
needed, it has one `container.sync_all()` at the inspected line 194; the root
file has one `root_file.sync_all()` at line 258, followed by one parent-directory
`sync_all()` at line 262 before authority-manifest adoption. There is no
per-cell `fdatasync` to remove or new cell-packing mechanism to add as if absent.
The 55.767241-ms local index-persist mean is an aggregate encoding, hashing,
write, mapping and barrier span on the shared SSD; S0 does not isolate physical
NVMe service latency within it. Any further software remedy there requires
finer attribution and matched cluster evidence, not weaker persistence barriers.


This is a scoped evidence-gated decision, not proof buffering cannot help on
other hardware. Final source-bound replay and matched hardware observations
must determine whether the same attribution holds across independent NVMe
owners. No index-persistence, receipt durability or directory barrier was
removed in response to this diagnostic, and no new candidate speedup is
inferred from it.


### 16.8 Matched-feature local adapter v2 observations

Authority:
`project:checkpoint-work/final-performance-2026-10-20/matched-adapter-summary.json`,
read separately from the preliminary adapter runs. Parent rebuilt both
executables with **`experimental-fabric,test-utils`**: baseline from the clean
frozen `b7123d6653b2b690a72fe5db8fcbdfb0419784e2` source, candidate from the
working changes. Parent-recorded adapter executable SHA-256 identities are:

| Population | Executable SHA-256 |
|---|---|
| Matched baseline | `2a2d992f19e832a21de38de1392cf964fefa598e6afea25148bf711dd40adb7f` |
| Candidate v2 | `c97ec9fa999cffe44e2cf3502c274ae138fc2e617ea4ea404ffa4190ed04883e` |

Each side retains **180 original API measurement rows**, three repetitions
over the existing native, uncached-TCP and coordinator-cached backends and
both value sizes. Candidate v2 runs the independent contention helper only
**after all ten original measurements**, with expected values taken from the
final oracle. V1 candidate timings and the initial distributed-only baseline
remain preliminary/superseded for comparison; they are not substituted into
this matched-feature table.

All times below are **nanoseconds per complete API call**, reported as the
**median of three repeat p50s**, not a percentile of pooled samples. `warm_get`
is `distributed::sui::Db::get`; `warm_multi_get` is a complete 32-key
`distributed::sui::Db::multi_get`, not per-key latency. The native bulk adapter
uses its existing scalar-get loop, not a newly parallelized native control.

| Value bytes | Original API / callers | Baseline native | Candidate native | Baseline coordinator cache | Candidate coordinator cache |
|---:|---|---:|---:|---:|---:|
| 256 | Scalar get / 1 | 200 | 202 | 601 | 376 |
| 8192 | Scalar get / 1 | 178 | 208 | 672 | 442 |
| 256 | Scalar get / 8 | 400 | 407 | 2046 | 1014 |
| 8192 | Scalar get / 8 | 394 | 377 | 1865 | 1196 |
| 256 | Complete 32-key multi-get / 1 | 4021 | 3618 | 13717 | 9618 |
| 8192 | Complete 32-key multi-get / 1 | 3859 | 2424 | 13663 | 9170 |

The reduction records 1024 scalar API samples per candidate original row
(total across the offered callers) and 128 complete bulk calls per candidate
row. Its baseline `samples` fields are null, so that reduced file does not
supply a baseline sample-count field. The cached original rows record 1024
hits for scalar and 4096 key hits for bulk, with zero misses/read RPCs in
every repeat on both sides. Native cache-hit counts are not instrumented;
native is a warmed API control, not evidence of zero cache hits.

These observations show lower local cache-supported scalar p50s in the
one/eight-caller candidate runs, but not universal parity or transport gains.
Bulk absolute cached p50s also fell; **the 8-KiB native-relative bulk gap must
not be claimed improved**, because its native control shifted proportionally
more (3859 to 2424 ns) than the cached result (13663 to 9170 ns). Repeat-to-repeat
variation is visible in the retained JSON, particularly the native/bulk
controls. There was no CPU affinity/pinning, and these are sequential local
microbenchmarks sharing one SSD, not randomized causal isolation or EFA
hardware measurements merely because fabric code was compiled in.

Uncached TCP does not show a material scalar improvement: its one-caller
repeat-p50 medians were 12,223 to 12,384 ns for 256-byte values and 23,149 to
23,439 ns for 8-KiB values. Faster local cache-supported service therefore
must not be presented as lower mandatory remote-fetch or hardware transport
latency.

Candidate v2 separately retains **24 contention populations**:
three repetitions × two value sizes × one/eight callers × native/cached
backends. Each caller performed 1024 reads; every caller in every population
had strictly positive observed overlap with the disjoint writer API intervals.
The **12 remote cached populations** each record exactly
`1024 * callers` hits, zero misses and zero read RPCs. The 12 native controls
retain null hit/miss/RPC diagnostics rather than invented hit counts.
The final-oracle value checks remained preserved. These independent
contention observations are not additional repeats of the original warm-get
rows and have no baseline contention counterpart in this reduction.

This adapter evidence does not replace the pending final six-binary certified
private-chain replay/full-state audit or matched hardware suite. It establishes
only the exercised local API/cache/concurrency observations under the stated
source, feature, sample and scheduling limits.


### 16.9 Final local readiness and outer-artifact identity

The parent built all six release executables from a new, clean detached engine
checkout at `4c74bcc903d01e8177cf717270e0acc43bdf3706` and published Sui commit
`6d4b704bb6b9af6e3d836e08ac3aa2161d443b2d`. The build used the independent
libfabric 2.6.0 prefix, two build jobs and protocol 8. All six actual executable
file hashes match the successful-Cargo-artifact build manifest:

- Frozen source archive:
  `4c342dfd86efaac0b08cf4a1fe462bbe6f62894376c6fa5b2cc9ef375f230177`.
- Source manifest:
  `15d2632640028a69d658e998e8ba39e6259e6739da8ed1a6aa61e21f447a1aeb`.
- Build manifest:
  `a402a3d3419061d5f855928272c5f3fae11185bec244b0f06b0ddbd6bf1ea4e7`.

The user's untracked supervisor brief is absent from the detached checkout,
both frozen source-owner inventories and the source archive. It remains local;
no product ignore/exclusion rule was added. The later `01a4397` change is only
Python campaign identity admission and its tests, not Rust runtime code.
The protected cloud pipeline must still rebuild its exact final merged source;
these local executables are not an authorization to reuse an older cloud build.

Actual local certified-checkpoint validation completed:

- Three clean native/TCP pairs with all timing-profile limits zero.
- One separate native/TCP diagnostic pair with read/write/native-write limits
  128, kept out of clean performance comparisons.
- One full native/software-libfabric-TCP pair, with no EFA hardware claim.
- Every pair reached checkpoint 1775, retained the expected certified digest,
  and passed the complete state comparison: **57 tables / 167858 rows** on each
  side across checkpoints, epoch and perpetual databases, plus the maintained
  RocksDB/RPC-index checks.
- The frozen `transport-floor` executable also ran the real software-provider
  RAM control for both value sizes; it reported zero WAL-record reads. This
  checks the sixth packaged executable, not hardware latency.

Both real replay backends use synchronous durability. In particular, maintained
`run.py` sets `TH_SYNC_WRITES=true`; native batch commit calls
`native_barrier(true)`, while remote publication follows durable shard receipts.
No faster asynchronous-native control is substituted for this comparison.

The local clean elapsed observations below include startup. They are not
individual checkpoint service-time measurements:

| Repeat | Baseline native, s | Baseline TCP, s | Candidate native, s | Candidate TCP, s |
|---|---:|---:|---:|---:|
| 1 | 13.275098 | 10.242821 | 13.271030 | 10.385648 |
| 2 | 13.546169 | 11.341606 | 13.548124 | 11.084891 |
| 3 | 13.546933 | 12.202069 | 17.025516 | 16.866909 |

**Do not infer a controlled Sui speedup from these laptop replays.** Every
candidate interval had host swap activity, although the final Sui node and all
15 local partition-process `VmSwap` observations were zero. For example, the
third remote interval recorded 68 host swap-in pages and 60326 swap-out pages.
The baseline replay intervals did not have those host swap deltas. The
diagnostic native/remote elapsed values were 28.456091/10.867505 seconds; neither
that ratio nor the software-provider elapsed time establishes a performance
gain. There is one shared SSD, unpinned scheduling and different background
memory pressure. These runs prove local correctness and source compatibility;
independent-NVMe and EFA timing remain hardware questions.

Local evidence is retained under the project workspace's
`checkpoint-work/final-performance-2026-10-20/`:
`candidate-six-binary-proof.json`, `candidate-source-admission.json`,
`candidate-sui-summary.json`, `candidate-sui-swap.json`,
`candidate-sui-audit-coverage.json`, `candidate-sui-software-proof.json`, and the
complete per-run logs, receipts and evidence archives. These generated outputs
are not committed product files.

Review also checked the distinction between **outer campaign repetition** and
**internal benchmark repetition**. Each outer native/remote case starts a fresh
process with `--repetitions 1`. Rust therefore emits internal repetition zero,
even when the enclosing case directory is `...-r2-*`. Comparing that row with
outer repetition two would reject correct data.

The real defect was weaker outer identity binding. `guest.py` now shares
`benchmark_case_identity` with `checkpoint_kv.py`. The latter's
`completed_case_ids` requires each pair's exact planned outer case ID, workload
variant, build, native/local versus remote transport and distinctness. It runs
after execution and before auxiliary files are opened. Reusing the r0 pair as
r1/r2 or relabelling cached data as an uncached family is rejected. Internal
repetition zero remains unchanged.

The actual native and TCP producers were exercised in fresh outer-r2 output
roots. Both passed full reopen/value audits and emitted internal zero for the
one/eight-caller observations. TCP recorded exactly 1024/8192 hits, zero misses
and zero read RPCs, with positive actual writer overlap for every caller.
Relabelling their outer identity was rejected. This functional proof is
`outer-repetition-proof.json`; its timings are not performance evidence.
The semantic Python suites then passed **462 AWS cases, three skipped**, and
**109 Sui cases, none skipped**. The follow-up read-only review found no
full/targeted false rejection or weakened deadline, budget, export or cleanup
gate. Required GitHub checks also passed at `01a4397`; later source revisions
must satisfy the normal protected-branch checks again.

At the 21 October 12:14:16Z read-only MCP refresh, there were no exact-owned
instances in the five configured regions. The authoritative prior-day ledger
remained revision 26: USD60 weekly cap, USD14.481852185325 recorded charge,
USD45.518147814675 reported available and zero unresolved runs. Billed-to-date
was unavailable under the account's permissions. No paid fleet had been
launched at that readiness checkpoint. Cloud execution remains the normal
protected `bench/aws-efa` merge workflow, with automatic export/termination and
MCP verification afterward.


### 16.10 Closed hardware campaign and measured remaining gaps

**Coordinator cache hits improved and remain in the native sub-microsecond
range. Mandatory remote reads and complete Sui replay did not reach parity.**
The transport changes must not be described as a demonstrated reduction of
the scalar remote-fetch floor.

#### Actual deployment and source admission

- Protected push/run:
  [`39605310614`, attempt 1](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/39605310614).
  Engine: `3f71aceaf0e9c9ba28a8c9a661d729d1d635f909`; Sui:
  `6d4b704bb6b9af6e3d836e08ac3aa2161d443b2d`.
- Six On-Demand `g6.8xlarge`, `us-east-1a`, one **cluster** placement group:
  one controller/native control and five distinct storage hosts with their
  own local NVMe. The west-region placement attempts did not obtain capacity.
  This is RF1, not a replication/HA measurement.
- All six executables were rebuilt by the protected prepare job. The actual
  uploaded executable bytes, not filenames alone, match the controller
  manifest. Build manifest:
  `6507071578acd81d88359ee018dd209f3daba8a528a9b136a4caae6e9b3cdc94`;
  source archive:
  `3c99b00096a6e70f5b90509adee6d17aec27b78f94fb2578e19f316b6103f4c4`.
- The full KV plan executed **129 native/remote pairs / 258 backend executions**,
  including three clean repeats and separate diagnostic populations. All
  **6048 primary summary rows** were recomputed from **12,658,677 raw samples**:
  count, p50, p95, p99 and maximum all match.
- The Sui matrix executed **12 pairs**: three clean and one profiled repeat per
  TCP/EFA/EFA-direct transport. All replay archives and 60 node archives are
  retained and hash-bound. Each pair reached the certified private-chain
  checkpoint 1775 and passed exact comparison of **57 tables / 167858 rows**
  across the three migrated databases, plus the retained RocksDB/RPC-index
  audits. This is not mainnet or a live multi-validator campaign.
- The controller archive is 197267700 bytes, SHA-256
  `15474bf06cff4c2752b7e27ed5df6220b9ddfa0ab4dd24940ec59203899719e0`.

The original workflow is **failed**, not retroactively successful. Its final
collector incorrectly forbade the explicitly profiled cold verification pass
described in §16.3. No workload or audit failed in the retained case records.
The corrected collector revalidates the unchanged archive and writes a
**separate recovered measurement attestation**; original `complete=false`
receipts and their failure text remain intact. Missing/foreign case, owner,
profile or physical-IO evidence is not waived.

#### Clean KV API results

Values below are medians of three per-repeat p50s, in microseconds. Scalar
populations contain 4096 calls per repeat; `get_many` contains 128 complete
up-to-32-key calls per repeat. Native ranges span the three matched controls,
not error bars or one pooled native control.

| API population | Native matched control | TCP | EFA | EFA-direct |
|---|---:|---:|---:|---:|
| Coordinator-hit hot scalar, 256 B | 0.140 | 0.410 | 0.420 | 0.420 |
| Coordinator-hit hot scalar, 8 KiB | 0.190–0.200 | 0.460 | 0.460 | 0.460 |
| Cache-fitting 32-key `get_many`, 8 KiB | 11.901–12.100 | 11.300 | 11.621 | 11.421 |
| Mandatory remote warm scalar, 256 B | 0.340 | 135.376 | 45.062 | 45.002 |
| Mandatory remote warm scalar, 8 KiB | 1.180–1.191 | 166.026 | 62.113 | 63.032 |
| RAM-only full RPC control, 256 B | Not a local-DB control | 102.893 | 37.352 | 37.871 |
| RAM-only full RPC control, 8 KiB | Not a local-DB control | 130.096 | 53.702 | 55.152 |

The scalar hit rows have recorded hits and no owner record reads. Their value
cache is bounded at 8 MiB. A `cache` family name or `_hot` phase alone does
**not** prove that a larger workload fits: use the recorded hit/miss/RPC
population. The RAM control actually executed all five owners, with 512
requests per value size per repeat and zero WAL-record work. It includes
TLS, protocol, progress and full payload validation; it is **not pure wire,
device latency, or an immutable hardware lower bound**.

Against completed run `39378783532` on the same host type/AZ/placement policy,
the direct hot scalar p50 changed from 0.500 to 0.420 us for 256 B and from
0.570 to 0.460 us for 8 KiB. Its matched native-normalized ratios changed from
3.333 to 3.000 and 2.714 to 2.300. Cached 8-KiB `get_many` changed from
14.65 to 11.42 us. These are descriptive before/after results, not randomized
causal isolation. The old campaign had only one clean repeat.

Mandatory remote scalar service is essentially unchanged. The new 8-KiB
EFA/direct p50 remains about 52–53 times its own native control. Ordinary
remote warm reads have owner-resident-page service evidence; they must not
be called NVMe-fetch latency. The evicted phases charge positive process IO,
but still contain many resident reads: one 8-KiB scalar population has
3776 resident successes out of 4096 and 320 asynchronous submissions.
Positive interval IO does not make every sampled key physically cold.

The new hit-only contention control separately recorded 72 native/remote
observations. The remote subset has 165888 reads/hits, zero misses/RPCs and
135104 reads overlapping the paired durable-write API interval. Remote p99
rises from 1.410–1.840 us with one caller to 32.941–36.351 us with eight callers.
These are not additional repeats of the ordinary scalar rows or evidence
that remote transport caused a cache-only contention tail.

#### Durable writes and lifecycle operations

The long load uses 8-KiB values, 32-key atomic batches and window 32:
3200 batches, 102400 keys, **100 complete 1024-key windows per execution**.
The matched native controls remain separate in the raw table.

| Measurement | Native matched control | TCP | EFA | EFA-direct |
|---|---:|---:|---:|---:|
| Long durable load, keys/s | 49,315–51,080 | 36,924 | 37,428 | 36,371 |
| Long write-window p50, ms | 19.948–20.697 | 21.365 | 20.562 | 22.144 |
| Long write-window p99, ms | 20.966–21.396 | 85.848 | 89.122 | 93.569 |
| Single 256-B durable write p50, us | 57.143–57.793 | 283.530 | 136.476 | 134.395 |
| Single 256-B durable writes, keys/s | 14,149–14,320 | 3,271 | 5,318 | 5,417 |
| Large-state full scan, s | 0.373–0.376 | 1.477 | 1.362 | 1.430 |
| Large-state index flush, ms | 94.048–95.821 | 149.692 | 145.888 | 148.643 |
| Large-state fresh reopen, ms | 1.985–2.041 | 40.125 | 67.532 | 58.025 |
| Reopen with reused transport, ms | No native analogue | 35.884 | 33.699 | 34.350 |

The previous long-load control had only 320 batches and ten windows, with
one-tenth the state. Its write tail, full scan, flush and reopen values are
**not a controlled old/new performance comparison**. Lifecycle rows above
are medians of three single observations, not p99 distributions. The new
long-load p99 is supported by 100 windows in each repeat; do not replace it
with the old ten-window maximum.

The measured exclusive parent spans in the long remote write windows are
approximately 30–31% preparation/admission, 40–44% storage wait, 9% publication
and 16–18% checkpoint work. Storage wait includes the remote operation and
durability wait, not just network transit. The enclosing foreground-checkpoint
span overlaps drain/wait work; background worker spans are another population.
Do not add those spans again.

The retained diagnostic write windows contain 13 unique capture-completion
records, not all 18 session cursor advances. Five advances occur outside the
sampled windows. Across those 13 records the coordinator ReceiptHistory
exports 14784 successful immutable-node write calls / 3784704 bytes, with
nonnull own count 13. Mean receipt-history construction/append is 3.286 ms;
its subsequent sync is 1.382 ms, and later index persistence is 5.493 ms.
These are coordinator retry-authority Merkle/Patricia-tree writes, **not
storage-shard history spill counters**. Node writes lack an isolated timer.

#### Real Sui checkpoint comparison

Full replay elapsed includes startup. Each ratio below is the median of
three paired remote/native ratios, not a ratio obtained by pooling native
controls across transports.

| Transport | Native elapsed median, s | Distributed elapsed median, s | Paired slowdown median |
|---|---:|---:|---:|
| TCP | 6.258751 | 10.346682 | 1.650971 |
| EFA | 6.257413 | 9.431054 | 1.506390 |
| EFA-direct | 6.270346 | 9.259837 | 1.476767 |

The following are **medians of per-cell wrapper means**, in microseconds,
not p50s. The historic `rocksdb_*_latency_seconds` names are typed-store
wrapper metrics even when the backing database is Tidehunter.

| Wrapper operation | Calls/cell | Native matched mean range | TCP | EFA | EFA-direct |
|---|---:|---:|---:|---:|---:|
| `certified_checkpoints` get | 1777 | 19.158–20.083 | 25.482 | 21.791 | 21.447 |
| `next_shared_object_versions_v2` multiget | 1775 | 2.955–3.011 | 6.074 | 5.656 | 5.421 |
| Perpetual write-batch commit | 5329 | 589.928–601.163 | 1196.864 | 986.616 | 983.466 |

These wrapper populations can mix hits and misses and include work above
the raw KV cache. Histograms provide bucket bounds, not raw quantiles;
p99 is unavailable below 100 observations in a cell. Clean distributed
scalar/index substage clocks and unsupported native remote-engine fields
remain null, not zero-duration measurements.

Across the three diagnostic Sui cells there are 126 captured checkpoints,
zero recorded foreground-checkpoint events, zero pressure-wait samples and
zero ring evictions. The perpetual namespace has ten captures per transport:
index preparation means are 7.977–8.281 ms; **index persistence means are
117.448–119.899 ms**. This later `prepared.persist` span is distinct from
ReceiptHistory construction, immutable-node writes and `history.sync`
(`remote_db/checkpoint_lifecycle.rs`). The nine namespace/cell observations
report 722763 coordinator node writes / 185027328 bytes. Buffering those
nodes cannot be claimed to fix the separately measured index-persist span.

All 60 storage-process IO brackets show zero charged read bytes during Sui
replay, despite logical WAL reads. These are resident-data observations, not
tests of fully cold per-shard NVMe fetches. Server CPU estimates use
independently bracketing intervals, not synchronized cluster-wide utilization.

Absolute distributed Sui elapsed improved only about 0.6–0.9% from the prior
completed campaign; that campaign used another engine/Sui source pair and
logged 605 remote ERROR entries. The new pair logs zero. Native timing also
changed. **There is no substantial demonstrated end-to-end parity gain.**
The direct path is modestly faster than regular EFA for this full replay,
but not consistently faster for individual reads or write tails.

#### What the evidence supports next

1. Keep C0's bounded in-process hit path and its full reference/key, health,
   retention and publication checks. This follows the FASTER/Garnet locality
   mechanism in §16.1; it does not require caching the whole distributed DB.
   Same order as native is observed; exact equality is not.
2. Investigate the measured full RPC/software floor before adding NVMe-oF.
   A diagnostic 8-KiB direct population has a 64.811-us mean client exchange
   and 5.599-us mean server handler; these are nested spans, not an additive
   pure-network decomposition. The RAM control already excludes WAL work.
   eRPC's endpoint/progress design motivates this investigation, but its
   hardware results do not guarantee those numbers on this AWS fleet.
3. Separate encoding/hash/write/barrier costs within index persistence before
   selecting another checkpoint optimization. C2b concerns the earlier
   ReceiptHistory node append path. It is not a substitute for attribution
   of the dominant Sui persistence span. Do not remove a durability barrier.
4. Retain whole-batch placement on one owner, physical WAL authority, exact
   retry evidence and contiguous durable publication. No native mutex was
   removed, no checksum/fence/TLS guarantee was relaxed and no global physical
   WAL or RDMA-only correctness path was introduced.

T3 idle CQ notification/parking and C2b immutable-node buffering remain
conditional work, not implemented or benchmarked speedups in this revision.
The mapping of each implemented mechanism to its exact native/paper/project
source remains §16.1. The hardware observations narrow the next bottleneck;
they do not justify promising 5-us mandatory remote reads.

#### Retention, termination and cost

The automatic workflow closed its fleet after exporting evidence. An
independent final AWS MCP observation at **2026-10-21 15:48:22 UTC** found
the exact six run-owned instance IDs terminated. Exact-tag scans across the
five configured regions found no run-owned EBS volumes/snapshots, ENIs,
security groups or placement groups; all six per-run IAM roles and profiles
were absent. No manual cleanup was needed; other users were not selected.
The shared foundation and S3 evidence remain intentionally retained.

The retained generated report separates observed estimates from reservations
and projections. The following UTC periods contain estimated usage, not
invoices or the sum of historical reservation amounts:

| Scope | Estimated usage, USD |
|---|---:|
| This run / 21 October 2026 | 8.626242 |
| Week beginning 19 October 2026 | 23.108094 |
| October 2026 | 23.108094 |
| Reported branch lifetime, from 18 October 2026 | 23.108094 |

The final MCP refresh verified ledger revision 29, available weekly headroom
USD36.891906, weekly cap unchanged at USD60, no unresolved runs and no active
reservations. Billed-to-date is unavailable. The run's compute-related estimate
is USD8.528800; its settlement also includes S3. A later S3 estimate differs by
USD0.012773; no double charge or reconciliation was added.
An identical USD41.266222 worst-case reservation exceeds current headroom;
do not silently raise the policy to obtain another fleet.

Full raw data remain in the workflow artifacts and
`s3://tidehunter-ci-354775971805-us-east-1/th-77284b2a87c3fec5/results/`.
Workspace reductions are under `checkpoint-work/aws-ci-runs/39605310614-1/`:

- `uploaded-inputs-proof.json`, `recovered-measurement-attestation.json`,
  `recovered-auxiliary-measurements.json`;
- `analysis-kv/kv-all-metrics.csv` and `.json`: **2400 comparisons / all
  27 primary phases**, including native controls, raw-source links, counts,
  ranges, rate denominators and old/new comparability flags;
- `analysis-kv/` contention, RAM-floor, S0, lifecycle and CQ/post reductions;
- `analysis-sui/` all 12 cells, all wrapper/adapter metrics, exact table and
  node-archive bindings, resources, per-namespace counters and old/new tables;
- the final independent operations receipt under
  `checkpoint-work/final-performance-2026-10-20/`.

