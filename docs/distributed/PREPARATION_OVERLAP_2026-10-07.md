# Preparation and storage overlap — 7 October 2026

Status: implementation, local checks and independent-NVMe benchmark complete.
All run-owned AWS resources are removed; closure evidence is recorded below. This record follows
[SOURCE_GUIDED_PIPELINES_2026-10-06.md](SOURCE_GUIDED_PIPELINES_2026-10-06.md).
Earlier measured results remain historical, not claims about this executable.

## Issue and change

Whole-window parallel preparation saved CPU wall time but delayed admission of
all writes until that window finished. The benchmark drained every window before
preparing the next, leaving storage workers idle during preparation. Earlier
local measurements showed preparation falling 58.316→26.045 ms while exposed
storage wait increased 29.211→49.738 ms at 8 KiB/B32/W32.

The implementation retains the existing outstanding-write window and final
barrier. It prepares up to 16 complete application batches at a time. While the
next chunk runs on the existing bounded Rayon pool, the authority services exact
completion notifications, publishes contiguous completed batches, and sends the
next full group to an available storage node. A 32-batch preparation option gives
a same-binary whole-window control. Native preparation and durability are unchanged.

This refines the proposed cross-window rolling pipeline: overlap happens **inside
the same offered window**, so the experiment does not silently double concurrency.
A benchmark window is not an atomic application batch. Each `submit_many` argument
still receives whole-argument preflight; actual application batches remain intact.

## Exact design anchors

| Source | Mechanism used | Changed code and correctness boundary |
| --- | --- | --- |
| Tidehunter paper §§3.1, 3.3–3.4; native WAL tracker, batch/replay and pending-index publication | Allocation differs from completed prefix; keep complete batches in the Value WAL; publish through contiguous completion | `remote_db/preparation.rs` allocates no new logical slots before complete chunk preflight. Existing ordered publication, exact receipts and uncertain-outcome rules stay authoritative. |
| [TiDB §4.1.1](https://www.vldb.org/pvldb/vol13/p3072-huang.pdf), independent persistence/replication/application stages | Overlap independent CPU preparation and storage stages while keeping reply dependencies | `remote_db/preparation.rs::submit_many` services old admitted work while scoped preparation executes. No TiDB transaction or replication protocol is copied. |
| [Raft Engine `Engine::write`](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs#L158), preparation before write barrier | Keep expensive canonical preparation outside ordered admission | Existing Rayon pool is shared through `Arc`; a scoped borrowed task reports through a bounded channel. Only full ready groups dispatch during preparation. |

See [the dedicated TiKV review](TIKV_SOURCE_REVIEW_2026-10-06.md) for exact pinned
source provenance. This slice changes distributed preparation and its benchmark;
it adds no native `Db`/WAL, Sui, old middleware, RDMA or replication behavior.

## Correctness and measurement

- Existing library suite: **533 passed, seven ignored**, including 26 remote tests.
- New causal test blocks the next preparation, then releases an earlier storage
  actor. All three earlier groups reach the storage node before preparation is
  released. A conflicting retry rejects the entire new chunk without allocating
  any new version; old values and exact retries survive reopen.
- Existing lost-response, stale-epoch, complete-group, ordered-publication,
  reference-validation and crash/recovery tests remain required.
- `prepare_progress_completions` and `prepare_progress_passes` identify progress
  during preparation. `prepare_wall_ns` and validation include this overlapping
  interval. Worker sums, progress clocks and validation are **nested**, not an
  additive decomposition of end-to-end wall time.
- Both candidate and archived baseline disable the optional value read cache in
  this campaign. All byte/checkpoint/scan/reopen audits remain enabled.
- Source/binary hashes and raw results live in `logs/overlap-2026-10-07/`.

## Cloud experiment and closure

Six identical i4i.xlarge instances in us-east-1a: one authority/native reference
and five storage nodes, each with one local NVMe. Two archived source trees rebuild
in independent empty Cargo targets on the controller. Each node verifies the
actual cloud-built executable hash; capability and generated-config checks precede
any measured case. Full artifacts transfer through S3, with local receipt/hash
verification before deleting temporary S3 data.

Planned cases: 96 warm API cases (12 shapes × two builds × native/TCP × two
repetitions), 12 larger cases (320 load/64 mixed batches, two builds × native/TCP
× three repetitions), and three current whole-window TCP controls: **111 cases**.
No larger group-capacity variant is needed: the previous cloud result was slower
with 4 MiB groups. Independent devices alone do not establish SSD saturation or
cold-read scaling; these retained-history workloads are still bounded/warm.

The fleet has a fixed one-hour absolute expiry and export reserve, plus normal
success/failure teardown. All instances, owned EBS volumes, network/IAM resources
and temporary bucket must be removed and other users' instances checked unchanged.
The $5 experiment reserve is an estimate allowance, not an AWS billing cap.

## Local results before AWS

The exact baseline/candidate source archives rebuilt in independent empty targets.
Four native/external-TCP cases then passed with ten storage processes exiting
cleanly. The separate crash/fencing/mTLS/lost-response campaign passed. All 29
deployment tests passed.

The final timing campaign contains 27 cases (three repeats × baseline/current
whole-window/current chunk16 × native sync/native async/TCP), 8 KiB/B32/W32.
All integrity/reopen audits passed, zero system swap deltas, no sampled processes
remained, maximum sampled process-tree RSS 213,768 KiB. Earlier local directories
are retained as integrity evidence only: builds overlapped their final cases.
`local-final/` ran after all builds and other correctness campaigns had finished.

| Median | Previous source | Current, chunk32 | Current, chunk16 |
| --- | ---: | ---: | ---: |
| TCP load phase ms | 97.714 | 97.087 | 100.603 |
| TCP / paired native load | 2.196× | 2.342× | 2.493× |
| Validation ms | 33.312 | 35.513 | 37.447 |
| Exposed drain wait ms | 54.742 | 57.517 | 46.278 |
| Publication ms | 6.775 | 6.888 | 5.751 |
| Complete-group file barriers | 48 | 48 | 48 |

The overlap mechanism reduces exposed drain wait but **does not establish a local
write speedup**. Native results vary between separately timed runs; report both
absolute TCP time and paired gap, not the gap alone. Chunk16 makes progress during
preparation (median one earlier group completion), without extra file barriers.
The cluster experiment is needed to test this mechanism on separate CPUs and
devices; a shared laptop result is not extrapolated into a cloud performance claim.

## Verified cluster results

Run `th-a0d9861f5f40e2b1`, us-east-1a, six i4i.xlarge machines, five independently
mounted EC2 NVMe storage devices plus the authority/native reference's NVMe.
RF1, persistent mutual TLS/TCP, no RDMA, optional value cache disabled for both
archived sources. This is the bounded new-fork benchmark, not a Sui/HA production
workload. The laptop reboot did not interrupt the persistent controller.

**111/111 cases** and **285/285 node archives** pass independent local checks.
The analyzer verifies raw sample counts/sums against every summary, operation
counts, full-value/reopen completion, source/build/config receipts and archive
SHA-256. All expected TCP commands explicitly disabled the value cache; current
commands selected chunk16 or the named chunk32 control. No cache hits or swap
were observed. The complete results and all regressions are retained.

### Larger durable write case: 8 KiB, batch32, window32, 320 load batches

Three repetitions per build/backend. Values below are medians, not confidence
intervals. Native performs a Value-WAL file/directory barrier per write window;
this is not ordinary native asynchronous acknowledgement.

| Metric | Native, current reference | Archived source-guided TCP | Current chunk16 TCP | Current / native |
| --- | ---: | ---: | ---: | ---: |
| Load phase, ms | 139.679 | 208.220 | 206.507 | 1.478× |
| Load throughput, logical ops/s | 73311.151 | 49178.715 | 49586.777 | 0.676× |
| Scalar get mean, µs | 2.935 | 281.305 | 249.910 | 85.145× |
| Scalar get p50, µs | 2.867 | 242.950 | 244.496 | 85.279× |
| Scalar get p99, µs | 3.868 | 413.440 | 354.368 | 91.615× |
| 32-key get mean per call, µs | 79.202 | 587.433 | 545.849 | 6.892× |
| 32-key get p99 per call, µs | 116.521 | 663.291 | 614.784 | 5.276× |
| Missing get mean, µs | 0.333 | 0.542 | 0.562 | 1.690× |
| Exists mean, µs | 0.250 | 0.514 | 0.546 | 2.185× |
| Missing exists mean, µs | 0.250 | 0.397 | 0.409 | 1.638× |
| Mixed phase, ms | 31.740 | 613.769 | 547.934 | 17.263× |
| Mixed write-window mean, ms | 12.725 | 20.480 | 20.906 | 1.643× |
| Mixed write-window observed p99, ms | 12.961 | 21.048 | 21.448 | 1.655× |
| Checkpoint capture, µs | 143.169 | 0.946 | 0.866 | 0.006× |
| Checkpoint read mean, µs | 6.996 | 258.356 | 263.898 | 37.721× |
| Delete phase, ms | 4.550 | 5.322 | 5.159 | 1.134× |
| Full scan, ms | 26.504 | 61.073 | 61.754 | 2.330× |
| Index flush, ms | 9.818 | 11.447 | 11.457 | 1.167× |
| Post-flush get mean, µs | 2.910 | 214.067 | 189.976 | 65.276× |
| Shutdown, ms | 0.857 | 8.044 | 7.316 | 8.540× |
| Reopen, ms | 1.530 | 624.702 | 621.677 | 406.204× |
| Reopen audit, ms | 55.102 | 2253.995 | 2177.525 | 39.518× |

The mixed read/write rows share one combined phase wall timer. Do not add them
or interpret the 17.26× mixed-phase ratio as pure write latency. The measured
write-window API p99 ratio is 1.65×, and warm scalar reads dominate much of the
mixed phase. Each larger load run has only ten timed write windows; low sample
counts do not establish production p99/p999 bounds.

**Latest change alone:** 208.220→206.507 ms (0.8% lower elapsed time) is too small
and variable to establish a broad performance gain. Baseline repetitions were
208.220/192.535/211.621 ms; candidate repetitions were 204.803/206.507/206.758 ms.
The same current executable with whole-window chunk32 took 218.583 ms median;
chunk16 was 5.5% faster than that control. Controls ran after the main matrix, so
order/noise remain possible confounders.

**Historical cluster comparison:** the 6 October older-source larger case took
254.287 ms TCP versus 135.999 ms native (1.870×). The current result is
206.507 versus 139.679 ms (1.478×). These use the same shape and instance type
but different hosts/times and several intervening changes. This supports a
cumulative improvement, not attribution of the entire gain to today's change.

### All twelve warm write shapes

Two repetitions per source/backend. Optional read cache remains disabled.

| Value bytes | Batch | Window | Previous TCP ms | Current TCP ms | Current elapsed change | Current / native |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1024 | 1 | 1 | 50.267 | 48.206 | -4.1% | 6.771× |
| 1024 | 1 | 8 | 10.130 | 9.239 | -8.8% | 3.219× |
| 1024 | 1 | 32 | 3.491 | 3.905 | +11.8% | 1.723× |
| 1024 | 32 | 1 | 103.953 | 103.863 | -0.1% | 3.390× |
| 1024 | 32 | 8 | 32.037 | 32.042 | +0.0% | 1.994× |
| 1024 | 32 | 32 | 18.962 | 19.640 | +3.6% | 1.434× |
| 8192 | 1 | 1 | 66.005 | 64.644 | -2.1% | 4.493× |
| 8192 | 1 | 8 | 14.019 | 14.071 | +0.4% | 3.081× |
| 8192 | 1 | 32 | 6.376 | 6.418 | +0.7% | 1.913× |
| 8192 | 32 | 1 | 272.945 | 270.967 | -0.7% | 3.150× |
| 8192 | 32 | 8 | 130.434 | 129.635 | -0.6% | 2.209× |
| 8192 | 32 | 32 | 86.401 | 87.541 | +1.3% | 1.567× |

Across these shapes, the median current/previous elapsed ratio is 0.9997×: no
general write speedup. Median gap to native is 2.645× versus 2.625× for the
archived source. The 1 KiB/B1/W32 case regresses 11.8%; it must not be hidden
behind the larger-case result.

### What the clocks establish

Larger-case TCP stage medians:

| Counter | Archived source | Current chunk16 | Current chunk32 control |
| --- | ---: | ---: | ---: |
| Preparation wall ms | 83.044 | 100.381 | 94.612 |
| Validation ms, includes preparation/progress | 84.447 | 102.102 | 95.981 |
| Exposed drain wait ms | 98.179 | 86.332 | 100.666 |
| Publication ms, may overlap preparation | 17.037 | 18.832 | 16.783 |
| Groups completed during preparation | not instrumented | 26 | 0 |
| Complete-group file barriers | 120 | 120 | 120 |
| Explicit directory barriers | 5 | 5 | 5 |
| Peak concurrent storage nodes | 5 | 5 | 5 |

Overlap works on five machines and reduces exposed waiting, but preparation CPU
and scheduling overhead absorb much of the benefit. These clocks overlap; do not
sum them as a disjoint budget. Native's larger-case commit, preparation and
Value-WAL barrier clocks were 128.610, 3.851 and 6.487 ms respectively, with ten
barriers. Native preparation is not equivalent to complete distributed encoding
and hashing. Aggregate shard barriers are not 120 globally serialized fsyncs.

Current larger-case diagnostic scalar reads average 254.132 µs end-to-end:

- TCP/TLS exchange: 243.814 µs, about 95.9% of the whole call.
- Client record verification: 8.002 µs.
- Authority index lookup: 0.805 µs.
- Server handler: 2.196 µs, nested inside exchange; server WAL-record read: 1.949 µs.
- Actor queue/service/return clocks: zero for this direct scalar path.

Exchange includes transport, TLS, scheduling and server work. This is not a
packet RTT measurement, and does not prove a mutex bottleneck. Source entry points
are `tcp.rs::Client::read_record_profiled`, `RemoteDatabase::get_profiled`, and
`StorageNode`'s profiled record handler. The physical-disk snapshots show zero
read bytes in **all 285** node cases and positive writes in all cases. These are
warm workloads; they do not validate cold-read scaling or NVMe saturation.

### Next work supported by the evidence

1. Profile preparation allocation/copy/hash work and pool scheduling before
   increasing worker counts or changing ordering. The current mechanism is
   correct but does not deliver a broad measured speedup. Keep its whole-window
   control and the same offered concurrency/durability policy.
2. Separate node-side append decode, group construction, WAL copy and barrier
   clocks. `tcp.rs` handles append and `persist_many` sequentially; the authority
   has one outstanding append group per endpoint. Their contribution cannot be
   separated from the existing exposed-wait timer. Retain complete-group proof
   before changing persistence batching; a larger group alone was slower in the
   preceding cloud test. The pinned TiKV stage-separation sources above motivate
   this investigation, not weakening durability.
3. Warm scalar reads need amortized exchanges or an eligible verified cache.
   There is no physical disk read for NVMe-oF to remove here. The current grouped
   API already reduces per-key exchange cost; network transport tuning needs a
   matched measurement of its own. No RDMA benefit is established by this run.
4. Recovery remains a separate architecture gap. `RemoteDatabase::open_inner`
   fences all nodes and replays retained histories. Durable index checkpoints
   with exact per-stream replay frontiers are needed before claiming native-like
   reopen time; complete inventory, retry identities and uncertain-outcome rules
   must be preserved.

These are remaining issues, not completed fixes or a guarantee of parity. The
run does not justify another paid cluster until a specific local change or
network experiment has a bounded hypothesis.

## Artifacts, closure and scope

- Full archive: `logs/overlap-2026-10-07/aws-run/controller.tar.gz`.
- Archive SHA-256: `87697b7eecc03fe5f9f2bf6a8036b63fc32b753759c478c5bed8af4edd036486`.
- Full comparison and evidence: `aws-run/extracted/results/analysis/` under that
  same log directory; includes all summary fields, stages, raw profiles and node
  I/O verification.
- Current source tree: `8b9364cb5fc97bb6c0314414295591b1a6ce8cf6ef6df78a8cb2be27202f8888`.
- Current cloud binary: `0994e3c58e4b3db1437762a93f010a028275455818e82398033ceb69d2ed451b`.
- Baseline source tree: `699b7f3f7cee3ba301f3ade86ea541baf4d846494942c896b4525d19fa28c04a`.
- Baseline cloud binary: `1935cbe5a1fbf6cb028d745db5a6f09d29e3a6540a1ccb1bd8a5f1fdd053b4e8`.
- Fresh AWS MCP evidence confirms all six instances terminated at
  07:55:49 UTC (09:55:49 Europe/Paris), zero owned EBS volumes and both unrelated
  instances still running. `closure-inspection.json` retains that evidence.
- Final cleanup receipt: `logs/overlap-2026-10-07/aws-run/cleanup-receipt.json`.
  Both instance profiles/roles, the security group, all temporary S3 objects and
  the bucket were deleted. Other users' states remain unchanged. No replacement
  fleet was launched after the laptop reboot.
- Estimated compute/root-EBS/public-IP cost: **$0.425**, from 4,352 aggregate
  instance-seconds to AWS's recorded termination request. Shutdown lag, S3
  storage/requests, transfer and taxes are excluded; this is not an invoice.
  The experiment stayed within its $5 reserve and the user's $60 budget.
- No commits or pushes were made in this task.

Known tooling issue corrected during analysis: the added campaign-profile argument
was shadowed by a CSV-path variable. It now has a distinct name. The final analyzer
actually checks the disabled-cache and chunk-selection command arguments; all
111 cases pass these checks. This did not affect benchmark execution or raw data.

Skill maintenance: this slice adds no dependencies or native engine format changes.
The AWS skills are not made stale. The newly supplied root AGENTS.md references
`skills/README.md` and `kvstore-*` skills, but those files were absent from this
workspace and the local skill paths searched after reboot. Their detailed
completion/lint procedures could not be applied; no Rust edits or new builds were
made after that instruction arrived. The verified gates above state what was
actually run before deployment; no new Clippy gate is claimed.
