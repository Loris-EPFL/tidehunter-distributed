# Post-campaign performance plan and benchmark retention

Date: 18 October 2026.

Status: research and authorized local artifact cleanup completed. No runtime code
change, build, benchmark, AWS action, commit or push in this task. Two independent
read-only agents investigated transport and durable-write/checkpoint paths.
Recommendations below are not implemented improvements or promised latencies.

Artifact paths beginning `checkpoint-work/` or `middleware/` are relative to the
project root. Source paths beginning `tidehunter/`, `tidehunter-fabric/` or
`experiments/` are relative to `tidehunter-sharded-wal/`. Short distributed-module
paths are relative to `tidehunter-sharded-wal/tidehunter/src/distributed/`.

## 1. One fleet is sufficient

The previous split was an orchestration choice, not a storage or EFA requirement.
`checkpoint-work/2026-10-18-efa-direct-hardware/campaign_ops.py:252-323` admits
one `kv_suite.scope` (`targeted` or `full`) with its own case count/Sui schedule.
`experiments/aws-sharded-wal/sui_checkpoint.py:1115-1149` runs that matrix and
suite, exports evidence, and unconditionally terminates the fleet in `finally`.
Calling the existing top-level supervisor twice on one live fleet would therefore
terminate it after the first scope; it is not a supported one-line configuration.
The first hardware discovery failure was a separate failed qualification, not a
third required benchmark scope.

For the next authorized campaign, use one controller and five separate storage
hosts for an ordered, source-bound sequence of experiment phases. Native, TCP,
EFA and EFA-direct already ran sequentially within each previous fleet. Combining
targeted and full coverage needs runner/configuration changes, not DB changes:

1. Admit all phase specifications and their exact case counts before allocation.
2. Give phases distinct IDs, output/S3 prefixes and native controls. Current
   repeated IDs such as `r01-efa-direct` must not overwrite an earlier phase.
3. Keep one fleet-level termination `finally` and one absolute deadline/export
   reserve. Do not reset the lifetime timer for another phase. Refuse another
   phase if the remaining work/export interval is insufficient.
4. Between phases stop/join old storage processes, drain or close owned endpoints,
   and use fresh database/WAL directories. Explicitly record fresh versus reused
   transport. Never rewind a live TLS sequence to simulate a new session.
5. Complete and verify each phase's S3 export before deleting its local working
   data. Continue the next phase while local S3 downloads run; do not keep paid
   hosts idle during a slow local download or offline analysis.
6. Reset counters and enforce the intended cache state outside the timed interval.
   An explicit-cache-disabled run can still have warm Linux pages. Use eviction
   and residency/physical-I/O checks for the designated cold-start cases.
7. Preserve per-phase native controls and separate clean/profiled populations.
   Qualify direct hardware first once; use a recorded balanced order for later
   comparable transport repetitions to reduce ordering effects.

This removes a second allocation/bootstrap/upload cycle and reduces hardware
variation. It does **not** halve the useful computation or make cache isolation
irrelevant. Two independently allocated fleets give fresh process/device state
but introduce different-host noise; a shared fleet requires explicit resets.
A longer combined run also needs its own bounded deadline. Do not infer a future
runtime guarantee from the last campaign's durations.

## 2. Authoritative retained evidence

Latest results remain under
[`checkpoint-work/2026-10-18-efa-direct-hardware/`](../../../checkpoint-work/2026-10-18-efa-direct-hardware/).
The [completed hardware report](HOT_READS_AND_CHECKPOINT_ADMISSION_2026-10-16.md#18-october-completed-hardware-comparison)
contains all measured populations, source identities and closure evidence.
Its original bytes and hash-bound receipts were not rewritten by this cleanup.

The retained campaign includes both successful scopes, the failed direct
qualification diagnostics, all analyses, original retrieved objects, source
snapshots, six rebuilt executables, and closure proofs. Required older paths
remain in place because immutable READY/verification records refer to them:

- `checkpoint-work/2026-10-17-efa-direct-local/build/`: original qualification build.
- Its `sui-tcp` audit, receipt, evidence and schema/measurement records, without
  disposable native/remote/producer databases or deployment WAL copies.
- `checkpoint-work/2026-10-15-lifecycle-parity-aws/fixture-input/`: exact fixture bundle.
- `middleware/results-local/mixed-role-benchmark-2026-09-23-uLYjew/fixture/`:
  reusable generated-chain fixture.

Other dated benchmark databases, raw result trees and obsolete copied builds were
removed from the selected benchmark roots, not from papers, Cargo targets or AWS.
The separate 12 GiB `tidehunter-sharded-wal/logs/` debug/source-reference tree was
left unchanged; this was not a recursive deletion of every dated project file.
A verified **724,024-byte** compressed historical trace
retains **227** shallow reports, receipts, comparisons and tool-source files:
`historical-benchmark-trace.tar.gz`. `retention-plan-2026-10-18.json` records its
members/hashes and removed paths; `retention-receipt-2026-10-18.json` records the
completed checks. Older raw-result links can no longer be used as live datasets;
consult the compressed trace for retained history.

All **2,098** retained AWS objects (2,031 successful-campaign objects plus 67
failed-qualification objects) were re-read and content-hash/size checked after
cleanup. Eleven distinct required input/verification files also matched their
original hashes. Available space increased from approximately 5.8 GiB to
**114.14 GiB**, approximately **108.3 GiB reclaimed**. Sizes are observed local
filesystem figures, not an AWS billing/storage statement.

## 3. What the worst tails actually contain

The latest full-suite bulk runs have only ten load windows, so they cannot supply
p95/p99. Use A2's 100-window population for tail work. Its raw records correlate
these spans inside the **same** slow windows:

| Transport / sample | Whole API ms | Storage-wait ms | Checkpoint ms | Foreground-checkpoint ms |
|---|---:|---:|---:|---:|
| TCP / 86 | 192.435 | 11.319 | 168.741 | 179.650 |
| EFA / 85 | 165.062 | 10.114 | 142.134 | 150.534 |
| EFA-direct / 86 | 176.869 | 14.017 | 149.927 | 159.702 |

Source: `analysis-a2-kv/write_windows.csv:5103,7872,2333`. These nested spans
must not be summed. They establish **foreground checkpoint/admission pressure**
as the enclosing cost in those windows. They do not identify one particular
checkpoint syscall, fsync or data structure as the exclusive cause.

Typical Sui persistence is a different population: the direct 640-sample
checkpoint/epoch/perpetual append-persist means are 366.444/335.487/468.668 us,
with containing-file persistence 361.165/330.318/463.326 us. Those profiles record
zero append-history-spill time and only five namespace/witness events per 640
file barriers. Removing a directory sync cannot eliminate the typical file-sync
cost. Nor do first-128-per-connection diagnostics explain a later bulk outlier.

### P1. Admission checkpoints should release enough capacity, not always catch up

Location: `tidehunter/src/distributed/remote_db/checkpoint_lifecycle.rs:55-60,388-399`
and admission conditions in `remote_db.rs:1391-1444`.

`force_admission_checkpoint` currently drains then calls `commit_checkpoint`.
The latter adopts an existing capture, then starts another capture when any tail
remains. Explicit checkpoint/close need full catch-up; an admission reservation
may only need the capacity freed by the first adopted capture.

Change the admission-only path to re-evaluate the actual requested reservation,
tail, index and known-identity budgets after progress/adoption; do another capture
only when the reservation still cannot fit. Preserve the full explicit-checkpoint
contract. This is a bounded-control-flow improvement, not unbounded RAM buffering.

Proof obligations: unchanged hard budgets; only adopted roots release covered
history/tail credits; no publication beyond contiguous durable receipts; pending
writes and reader retention retain their existing ownership. Validate interrupted
adoption, multiple pressured budgets and out-of-order durability against existing
pipeline/lifecycle models. Predicted p99 benefit is **[INFERENCE]**, not measured.

### P2. Batch checkpoint-history IO without replacing the index format

Locations: `remote_db/receipt_history.rs:175-190,359-415`,
`storage/history.rs:263-349`, `storage/history/index.rs:76-151`.

The coordinator opens a stream file and rereads its preceding receipt per append,
writes each 256-byte slot, and path-copies metadata nodes with individual
positional writes. Node history maintains four immutable indexes. Use bounded
per-capture file handles and contiguous page/node buffers; preserve root hashes
and the current on-disk format. A bounded cache for newly built pages can avoid
re-reading bytes just produced by the same capture.

This is a concrete syscall/write-amplification opportunity, **not proof that it
causes the full 150 ms pause**. Keep the already implemented incremental snapshot
cell reuse and shared buffered `.cells` container (`native_index/snapshot.rs`);
do not propose a second incremental checkpoint implementation.

Before changing this path, correlate one slow checkpoint generation with:
trigger/budgets, captured cut/count/bytes, history opens/reads/writes/CPU/sync,
node-prefix proof queue/service, index reused/rewritten cells, and separate
metadata/adoption/cleanup times. Add bounded clocks only to diagnostic runs.
Pages must be durable before root adoption; adopted pages remain immutable;
adoption must precede metadata/WAL reclamation.

## 4. Remote reads: optimize the live message path, not a presumed hardware floor

The B2 8 KiB bounded front-cache case already records 0.55 us distributed p50,
approximately 0.21 us native, with all 4,096 distributed accesses hitting. The
explicit-cache-disabled scalar case is different: 61.142 us EFA versus its paired
1.220 us native p50. “Cache disabled” does not mean cold NVMe: these runs can read
warm Linux pages, and the Sui owner profiles record zero physical read bytes.
The remaining remote-path gap is real, but it is not a measured cold-device gap.

The roughly 54 us RAM result is **not hardware RTT or an irreducible network
floor**. `tcp.rs:986-1013::read_ram_probe` checks every payload byte; the RAM
control exchange also waits for local request-output retirement. Normal WAL
read completion can return a validated response before that local request CQE.

The separate profiled 8 KiB partition-0 populations measured EFA exchange
48.602 us and payload verification 9.626 us; direct exchange 50.479 us and
verification 9.132 us. These means cannot be subtracted from an unrelated clean
p50. Completion-generation versus completion-observation time is not separated.
The reported 34–38 us retirement spans include delayed observation, not just NIC
service. Current evidence does not establish a sub-10-us authenticated RPC.

The separate warm-WAL read profile attributes approximately 7.8 us to record
verification **inside** exchange. `codec.rs::CompleteBatch::{decode_record,
decode_records}` already verifies only requested members, shares value buffers
and uses SIMD batching for grouped digests. Keep member/address/shape validation;
do not present all exchange time as wire time, or propose already implemented
zero-copy/SIMD work as a new fix.

Already present: idle scalar lane borrowing, exact-reference singleflight,
bounded multiplexing, same-sweep resident replies, cold IO off the owner,
pre-registered stable DMA slots, bulk-byte serialization, reusable buffers,
vectored ciphertext, and submission separated from CQ retirement. Scalar warm
profiles used the inline route with zero actor spans. There is no production
50–64-us sleep to delete: `endpoint.rs`'s sleep helper is diagnostic, not the
live `Session` path. Setup/registration is connection-time, not per scalar read.

### P3. Ready-aware receive and bounded useful-work bursts

Locations: `tidehunter/src/distributed/tcp/reactor.rs::Session::{progress,receive,progress_output}`,
`tidehunter-fabric/src/endpoint.rs::try_*`, and endpoint receive state.

Every empty fabric sweep can enter rustls `read_tls` and plaintext parsing.
In one sampled EFA partition-0 scope there were 5,320 client TLS-read calls for
52 RAM RPCs, with 5,164 `WouldBlock` outcomes; server sweeps were also mostly
empty. These scope totals include idle/interleaved work, not a per-request
critical-path decomposition.

Expose useful RX readiness separately from TX-only/generic CQ progress. Skip
TLS/plaintext reads only when neither unread endpoint ciphertext nor buffered
rustls plaintext exists. Continue manual provider progress and EAGAIN recovery.
Preserve partial TLS/frame state, absolute deadlines and exclusive endpoint
ownership. Drain a bounded burst of already-ready output frames/completed input
rather than advancing only one plaintext frame per sweep; never wait to fill a
batch or starve peers, deadlines or cold completions.

Targets: RPC CPU, grouped/windowed throughput and potentially scalar exchange.
Acceptance: fewer empty TLS/parser calls and sweeps/key, unchanged correctness and
packet accounting, then an actual reduction in clean exchange/CPU. A counter-only
reduction is not a measured latency win.

### P4. Suppress redundant direct CREDIT using already-ready DATA

Locations: endpoint `direct_receive`, `direct_send_vectored`,
`queue_direct_control`, and `ControlWindow` in the direct resource implementation.

DATA already carries cumulative credit, but current consumption emits standalone
CREDIT eagerly and successful DATA does not update the standalone advertised
credit state. In the sampled server scope direct had 106 send posts/212 CQ
completions versus EFA 53/106, over 52 RAM requests plus boundary control traffic.
Direct framing/CRC/reassembly copy costs also exceed EFA's observed staging cost.
Do not assume every 8 KiB value needs two DATA packets; those sampled counts are
consistent with response DATA plus CREDIT.

Give an already-ready DATA response a bounded same-sweep opportunity to carry
credit and record that credit only after a successful post. By sweep end send
standalone CREDIT if no DATA succeeds. No timer waiting for future traffic.
Keep a reserved control slot, full-packet-consumption credit, ordinal/order/CRC
checks and standalone progress for unidirectional or large-window transfers.
EAGAIN cannot consume bytes, credit or ordinal. Validate low credits, reordering,
large fragmented replies, no-reply flows, fence/close and failed-post retries.

Targets: direct send/CQE rate, CPU and possibly its small disadvantage to EFA.
This is not a claim that two fewer packets will remove tens of microseconds.

### P5. Capability-qualified idle CQ notifications

`Resources::open_endpoint` forces `FI_WAIT_NONE`. Pinned libfabric 2.6 documents
RDM `FI_WAIT_UNSPEC`/`FI_WAIT_FD` support for EFA and direct; actual support also
depends on the build/device, and FD waits have SHM restrictions. The old 2.4
“no wait objects” comment is not a sufficient description of the deployed 2.6 API.

First qualify the actual notification capability; then arm/recheck/wait only
when truly drained. Do not wait with TLS/output/control work, active reads or
pending completion ownership. Bound waits by liveness/shutdown/deadlines and keep
polling when unsupported. This targets idle CPU first; first-arrival p99 may
regress. It is not a blanket replacement of active progress with blocking waits.

Longer-term per-core ownership of multiple sessions is a separate design option
if connection-owner oversubscription is measured. Do not replace the current
scalar inline route with an unconditional cross-thread actor hop.

The current code is not globally lock-free: `pending_read` shares client/lane and
flight ownership, and `tcp.rs:574-589` uses a blocking reader-authority `RwLock`
despite nearby try-lock wording. Measured authority checks are small; RAM replies
bypass that storage lock. Its presence does not explain the RAM result, and
removing fencing synchronization without a new proof is not an optimization.

## 5. Flush, reopen and network-write grouping

### P6. Match benchmark contracts and reuse existing pools in Sui

`distributed/sui.rs:1041-1046` maps native flush to a Value-WAL barrier and remote
flush to `flush_index`: drain, history/prefix proofs, index/checkpoint persistence,
manifest adoption and cleanup. These are real API costs but **unequal work**.
Report a matched durability barrier and a matched full checkpoint separately.
Do not claim the existing flush ratio isolates transport or data-fsync overhead,
or remove required checkpoint work merely to improve a number.

`distributed/sui.rs:543-555` opens a fresh `RemoteDatabase`, while the raw-KV
benchmark already uses `open_with_transport_pool`. Wire the existing process-owned,
configuration/identity-bound pool into the adapter. Preserve clean drain/checkpoint/
seal before reuse; fresh status/certificate validation, durable epoch reservation,
all-node fencing and recovery remain mandatory. Failed shutdown discards transports.
This targets startup/reopen, not steady-state reads or fsync.

Network `tcp/control.rs` preserves each RPC group and performs a barrier per
complete group. Bounded merging of already-queued same-authority requests is a
conditional concurrency improvement, using existing `worker.rs` patterns. It
cannot help a single sequential caller or already-full groups without adding
latency. Do not add a batching timer. Never merge across fence/seal boundaries or
allow multiple independent unpersisted groups behind a potentially torn frame.

## 6. Which external designs apply

QMD status confirmed the actual collection `tidehunter-papers`, 106 documents.
Relevant sections were retrieved there; online primary code anchors were checked.
Some parsed sections contain embedded base64 figures: truncated figure-heavy text
was not treated as a complete implementation proof. Current code and complete
text sections support the recommendations above.

| Source | Mechanism to adapt | Boundary / why not a drop-in replacement |
|---|---|---|
| [Tidehunter v2, §§3–4](https://arxiv.org/abs/2602.01873v2) | WAL bytes as long-term values; positional reads; asynchronous index snapshot freeze/persist/adopt; safe relocation frontier | Paper ordinary page-cache completion differs from explicitly fsync-acknowledged benchmark writes. No second full-value log. |
| [eRPC, NSDI 2019](https://www.usenix.org/system/files/nsdi19-kalia.pdf) and [event loop source](https://github.com/erpc-io/eRPC/blob/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d/src/rpc_impl/rpc_ev_loop.cc) | Exclusive endpoint/session ownership, process ready packets inline, bounded immediate RX/TX batches, reusable registered buffers | Much already exists here. Its small-message ConnectX result is not an EFA/TLS/8-KiB promise. Do not copy unsafe unsignaled-send lifetime shortcuts. |
| [Garnet, PVLDB 2026 §§3,5](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf) and [maintained session source](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/common/Networking/NetworkHandler.cs#L535-L614) | Process completed TLS/input inline; per-session state over shared storage; nonblocking prefix-consistent snapshots | Not universally lock-free. Its eRPC variant was a prototype, not the open-source default. |
| [Garnet/Tsavorite commit policy](https://github.com/microsoft/garnet/blob/87775887548e660bd53f2e5fb92c66773540da0f/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/LogCommitPolicy.cs) | Captured-tail coverage, redundant commit suppression, bounded pending commits; wait for committed rather than allocated address | Supports the admission-coverage principle, not skipping this fork's root/namespace proofs. Maintained successor to [FASTER, now not actively maintained](https://github.com/microsoft/FASTER). |
| [TiKV raft-engine write barrier](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/write_barrier.rs#L170-L225) and [Engine::write](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs#L147-L235) | Ready writers share a barrier; singleton proceeds immediately; OR sync requirements; publish only after shared durability | Already partly implemented. Do not add a second Raft WAL or assume it solves checkpoint pauses. |
| [KVell, SOSP 2019](https://doi.org/10.1145/3341301.3359628) and [author overview](https://github.com/BLepers/KVell/blob/master/OVERVIEW.md) | Bounded device queues and batched IO/syscalls | Research prototype, different in-place/no-WAL recovery and atomic-write assumptions; official implementation's >4-KiB restriction conflicts with our 8-KiB workload. |
| [Autonomous Commit, SIGMOD 2025](https://dl.acm.org/doi/10.1145/3725328) | Small aligned per-worker durable flush units; separate durability from dependency-aware acknowledgment; bounded acknowledgment progress under low load | Potential later shard-WAL design experiment, not the first checkpoint fix. Its O_DIRECT/alignment and multi-stream dependency protocol do not match our mmap/whole-batch recovery contract automatically. |
| [BtrLog, PVLDB 2026](https://arxiv.org/abs/2606.27051v1) and [Rust prototype/TLA+ artifact](https://github.com/maxi-k/btrlog) | Single writer per stream, per-core cooperative IO, direct worker networking, blocking S3 work off the critical owner | Useful Rust implementation reference, not a production library promise. Its quorum/failure model and IO layout differ; prototype implements the failure-free critical path. It is not evidence we need one cluster-global log. |
| [Pinned libfabric EFA manual](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md) | Actual direct/regular progress, packet-size and CQ-notification contracts | Runtime qualification remains necessary. Direct removes provider protocol features which our ordered TLS stream must currently reconstruct. |

## 7. Scope, invariants and acceptance order

First implement P1/P2 checkpoint work and P3/P4 useful-progress/credit work as
independent slices. Qualify P5 idle notifications separately. P6 is a lower-risk
adapter/benchmark correction with separate reopen acceptance. This work is in the
new fork's `tidehunter/src/distributed`, `tidehunter-fabric` and experiment runner;
no immediate broad upstream-Sui or legacy-middleware rewrite is required. Optional
Sui decoded-hit timing would be a separately documented observability change.

Preserve throughout:

- complete application-batch/outer-CRC-frame atomic recovery;
- one recoverable unpersisted group per current stream unless a new protocol is proved;
- exact-file plus required namespace/witness persistence before durable receipts;
- logical publication/acknowledgment only through the contiguous durable prefix;
- immutable pages before adopted checkpoint root, and root before reclaim;
- epoch/identity checks, pre-response fencing, reader pins and relocation safety;
- TLS/CRC validation, bounded queues, memory/byte credits, deadlines and DMA lifetime.

Local work can validate these transitions and compare software regressions. The
laptop cannot qualify EFA notifications, NIC completion timing or independent-NVMe
scaling. The next authorized cloud campaign should be one resettable fleet:
RAM controls with separate exchange/validation/retirement spans; warm scalar and
32-key read cases; the same 100-window tail case with correlated checkpoint phases;
and unchanged Sui private-chain controls. A production-size/mainnet-state workload
is a separate necessary validation, not established by the retained generated chain.

Success is measured against the same conditions and paired native controls. Do
not pool profiles/caches/campaigns or turn bucket bounds into exact percentiles.
If cheap-work counters improve but clean latency/CPU do not, reject the claimed
performance win and attribute the remaining critical path before another redesign.

Current evidence supports sub-microsecond local cache hits. It does **not** prove
that all authenticated remote reads can reach native RAM speed, or that a global
WAL, sharded LSM, one-sided RDMA or NVMe-oF would remove the remaining cost. Keep
per-shard authoritative WALs and improve the measured paths before taking on a
new distributed-storage/recovery protocol.
