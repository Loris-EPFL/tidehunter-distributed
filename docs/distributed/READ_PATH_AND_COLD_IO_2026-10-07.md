# Read transport and verified cold I/O — 7 October 2026

Status: local validation and the isolated AWS TCP campaign completed. All 32 cloud
API cases and 80 node archives passed validation. All six instances terminated,
and all run-owned EBS, S3, IAM and network resources were removed.
The preceding baseline is
[Preparation overlap](PREPARATION_OVERLAP_2026-10-07.md).

## Scope and evidence boundary

The preceding AWS campaign loaded only 80 MiB of values in its larger case.
All storage-device samples recorded zero read sectors. Its approximately 85×
scalar-read gap is a warm local-versus-remote API comparison, not a measurement
of cold NVMe scaling. Disabling the application cache did not disable Linux's
page cache. A maximum mapped-region allowance is not a page-cache memory limit.

This change improves the existing authenticated TCP path and adds a cache-evicted
read campaign. Advisory eviction is followed by residency and physical-I/O
evidence. It does not pretend that the bounded dataset exceeds system RAM or
establish sustained capacity scaling. A later cluster campaign must distinguish
these cold-start phases from a sustained working set larger than effective RAM.

## Source anchors and code map

| Mechanism / source | Implemented location | Correctness boundary |
| --- | --- | --- |
| Tidehunter paper §3.2, direct indexed access to Value WAL and optional value/OS caches | Native `db.rs::get`, `wal/mod.rs::read`; diagnostic helpers call the same lookup and record helpers | No entire-WAL scan is introduced; native production reads, CRC, cache refills and write acknowledgement remain unchanged |
| [eRPC, NSDI 2019](https://www.usenix.org/conference/nsdi19/presentation/kalia), amortize buffer allocation and keep transport costs explicit | `distributed/tcp.rs`: reusable frame buffers capped at 1 MiB retained per direction; identical bounded bincode/mTLS frames | No eRPC transport or performance claim is copied; no additional runtime dependency |
| Linux socket timeouts and absolute per-frame deadline | `tcp.rs::DeadlineIo` reuses equal millisecond timeout settings, records syscall time/counts only when requested | Timeout never rounds upward; the absolute deadline is checked before each socket operation; final sub-millisecond remainder is retained; slow-peer deadline test remains required |
| [Linux fadvise](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html), sync before advisory eviction; [madvise](https://man7.org/linux/man-pages/man2/madvise.2.html), shared mapping semantics | Linux `test-utils` hooks in `wal/{mod,mapper}.rs`, `read_probe.rs`, and authenticated node inventory requests | Sync and drop only this WAL's mapped-page references and file cache. No machine-wide cache drop, raw block export or file-content change |
| [Linux process I/O counters](https://docs.kernel.org/filesystems/proc.html), and `mincore` residency | `read_probe.rs`; `examples/support/read_campaign.rs` | PID/start-time identity must agree; counters cannot decrease; a cold phase with zero accounted physical reads fails |
| Explicit service-stage metrics, as in [TiKV server metrics](https://github.com/tikv/tikv/blob/master/src/server/metrics.rs) | `tcp/diagnostics.rs`, `tcp.rs`, `remote_db.rs`, benchmark CSV/YAML | Monotonic clocks stay on their own host. Nested or concurrent clocks are not added as disjoint cluster elapsed time |

Native `Db` gains only feature-gated diagnostic methods. Sui and the old middleware
are unchanged. A few existing Clippy diagnostics in the experimental implementation
are simplified without changing behavior so the strict check passes.

## Timing contract

Ordinary runs keep server diagnostic clocks disabled. `--transport-diagnostics
true` explicitly enables them in a separate campaign. Existing scalar profiled
reads remain separate samples outside ordinary throughput/latency phases.

| Output | Measured parts and interpretation |
| --- | --- |
| `summary.csv`, `samples.csv` | Ordinary scalar/batched read and durable-write API latency, distribution and phase wall time. Diagnostic campaigns are identified in `campaign.txt`. |
| `remote_read_profiles.csv` | Authority index, dispatch/wait, actor clocks, request encoding, exchange, response decode, record verification, server authority and bounded WAL read. Server clocks nest inside exchange. |
| `exchange_profiles.csv` | Request TLS send; response header wait (includes remote execution and TLS read-ahead); body read; socket read/write calls, bytes, blocked syscall time and timeout setup. Socket times nest inside exchange stages. |
| `node_profiles.yaml` | Per-node request receive/decode, handler, response encode/send, socket clocks. Append inventory wait, canonical decode, complete-group construction, native WAL append and persistence. Append clocks nest inside handler. The corresponding client encode/exchange/decode and socket clocks cover writes and reads. Measurement-control RPCs are excluded. |
| `native_read_profiles.csv` | Native index and native WAL/CRC/decode path plus whole-call time and explicit application-cache-hit flag. WAL/CRC/decode is a combined measurement, not pure device latency. |
| `read_io.csv` | Per-process physical read-byte and minor/major-fault deltas, file bytes and resident bytes before/after. Includes all five remote nodes, or the native process. |

Server request-receive time includes waiting for the next request. It is not an
execution cost or a wire RTT. Socket read time includes kernel waiting and host
scheduling. No synchronized cross-host clock subtraction or claim of separately
measured physical network transit is made. A successful socket write means local
kernel acceptance, not delivery or disk persistence.

Physical-I/O evidence covers each whole phase plus its boundary evidence calls.
It does not attribute each physical read to a key or prove every get missed the
cache. Repeated keys and readahead can warm pages during a cache-evicted phase.
Residency is a whole-file snapshot (including sparse/unwritten ranges), not a
per-value hit ratio. Mapped page faults and pread can differ in read amplification.

## Reproducible local and external-node invocation

Build and freeze all Rust/Cargo inputs and the executable hash:

```sh
python3 experiments/aws-sharded-wal/freeze_build.py \
  --output logs/read-path-current \
  --features experimental-distributed,test-utils
```

Use a fresh output directory for each backend/run. Ordinary TCP comparison:

```sh
logs/read-path-current/sharded_wal_bench --backend tcp \
  --values 8192 --batch-sizes 32 --windows 32 \
  --batches 320 --mixed 64 --reads 512 --repetitions 1 \
  --tcp-read-cache-bytes 0 --output logs/read-tcp-warm
```

Add `--cold-read-pass true` for verified cache-evicted phases. Run the same flags
with `--backend native-sync` for the native reference. Run a separate repetition
with `--transport-diagnostics true` for server stage accounting. The cold hooks
are available only with Linux and `test-utils`; unsupported builds reject the flag.

The same binary supports `--prepare-external-case` and `--remote-config` with
these flags. Node evidence and diagnostics travel over authenticated TCP. Every
cloud node must use the exact cloud-built binary with `test-utils` enabled for
cold phases. The cloud controller now selects the manifest build features and the
explicit `read-path` matrix, verified by local deployment tests. Transfer
results/logs through S3, verify full artifacts, then
remove only that run's resources, as in the previous campaign.

## RDMA boundary

This laptop has no `/sys/class/infiniband` device or `/dev/infiniband`, and no
libfabric runtime. This change implements TCP improvements; it does not implement
or claim to validate an RDMA backend. The earlier middleware's EFA RPC transport
is not a one-sided WAL-read implementation and is not silently imported here.

An RDMA implementation still needs a bounded registered-region lifetime protocol,
exact published-reference validation, equivalent confidentiality/authentication,
cold-value disk-I/O fallback and hardware capability checks. Network memory
completion must not replace the durable-write receipt. NVMe-oF is not introduced.

## Verification and measurements

### Resumed work and current deployment scope

The five final pending source files compiled. The full library passed 534 tests
(seven ignored), strict Clippy passed, and the changed files passed formatting.
The cold diagnostic sampler now uses the same explicit pointer-width hash
truncation as the ordinary sampler.

The first resumed campaign completed 32 local cases: 24 interleaved old/new
native/TCP cases, six cold cases, and two separate diagnostic cases. Values were
1 KiB/B1/W1 and 8 KiB/B32/W32, with 320 load batches, 64 mixed batches and 4,096
point reads. No application read cache was enabled. All correctness/reopen,
physical-I/O, zero-swap and process-exit checks passed. Detailed comparisons are
in `logs/read-path-2026-10-07/resumed/comparison.md` and the adjacent machine-readable
`measurement_summary.json`.

That campaign established no broad performance gain: large warm load wall time
was 208.430→233.646 ms TCP versus 107.800→108.410 ms native, while small scalar TCP
read p50 improved 18.301→15.564 µs. Its three cold repetitions had median per-run
p50s of 44.680/91.793 µs for native/TCP scalar reads and 1,286.458/1,513.694 µs for
32-key reads. Those are local shared-SSD observations, not cluster predictions.

Review then isolated the large connection-diagnostic payload behind a `Box` in
the response enum and borrowed diagnostic state when creating clocks. This avoids
enlarging every ordinary response with a diagnostic-only payload. Serialization
remains unchanged. The eight TCP authority/authentication/deadline tests and
strict Clippy passed again. The frozen final Rust source is:

- Source tree: `34fd7ff92870ec11a35f71693c84f38bda2f4be8c89d28b9fba112b46482937a`.
- Local binary: `d6baaae42562be5c10b5ffbad6e81ae2949da927dd96f38ea53db25ccb985182`.
- Manifest: `logs/read-path-2026-10-07/resumed/candidate-layout/source-manifest.json`.

All 28 final-source confirmation cases passed, with zero swap, positive cold I/O
on every node, and all processes exited. **A concurrent unrelated CUDA/C++ build
contaminated these performance measurements:** the unchanged native and TCP
baseline also slowed several-fold. The confirmation is correctness evidence;
it does not establish a performance effect of the response-layout change.
Keep `resumed/confirmation/` and `confirmation-host-contention.json` with the data.
Four further local cases exercised the explicit external-node configuration path,
including current cold/diagnostic flags, with ten storage processes all exiting.

The cloud runner now has an explicit `--campaign read-path` mode. It preserves the
historical default matrix, but selects 32 API cases/80 node archives for this run:
24 warm old/new comparisons, six current cold cases, and two diagnostics. Guest
builds use the frozen manifest feature set and reject a cold campaign without
`test-utils`. Result validation checks exact modes, read counts, all cold I/O
phases and all expected partitions. All 33 local deployment tests passed, including a regression check for a transport exception whose service response is absent.

Run identity: **`th-f73786bd42d638db`**, `us-east-1a`, six `i4i.xlarge` instances,
one controller/native machine plus five RF1 TCP nodes. Both archived sources will
be rebuilt into separate fresh targets on the controller, with binary hashes
verified on each shard. No RDMA is claimed. Fresh MCP pricing is $0.343 per
instance-hour, $0.08/GiB-month gp3, and $0.005/public-IPv4-hour. Six machines with
32 GiB roots cost approximately $2.109 at the one-hour deadline before requests
and transfer; $5 is reserved against the user's $60 experiment budget. This is an
estimate, not a billing cap. Cost and API evidence are in `resumed/cost-plan.json`
and `resumed/aws-preflight.json` / `aws-prices.json`.

### Verified AWS results

Run `th-f73786bd42d638db` completed all **32 API cases and 80 node archives**.
Independent analysis validates the complete matrix, raw sample/summary agreement,
all source/build receipts, each node archive, instance-store identity and zero swap.
The downloaded 5,868,311-byte archive has SHA-256
`131b80bd3cf08cd9210ccb9c8cb8fee9755959628ed7dc706503e425a89af68c`.
Both executables were rebuilt with Rust 1.90.0 and
`experimental-distributed,test-utils`, in distinct fresh targets:

| Build | Source tree SHA-256 | Cloud executable SHA-256 |
| --- | --- | --- |
| Previous preparation-overlap | `8b9364cb5fc97bb6c0314414295591b1a6ce8cf6ef6df78a8cb2be27202f8888` | `1e17fc448aace1b0f89e7b2a02845757cc0da604520b165efa30f2b1635d0f93` |
| Current | `34fd7ff92870ec11a35f71693c84f38bda2f4be8c89d28b9fba112b46482937a` | `dbc333001089a24d68eed886ddfb4242f040380a5d159fce8f9ade2d3f6cef3c` |

The following ordinary results are medians across three repetitions. Read times
are API p50. A get-many call returns 32 keys. Load time covers 10,240 mutations in
320 batches, with a window of 32 batches; both backends use the Value-WAL
file/directory durability barrier. Diagnostic clocks are disabled in these rows.

| 8 KiB workload | Native | Distributed TCP | TCP / native |
| --- | ---: | ---: | ---: |
| Warm scalar get | 2.707 µs | 188.033 µs | 69.462× |
| Warm 32-key get | 83.705 µs | 456.244 µs | 5.451× |
| Cache-evicted scalar get | 126.927 µs | 317.683 µs | 2.503× |
| Cache-evicted 32-key get | 3,134.549 µs | 1,555.932 µs | 0.496× |
| Warm load wall time | 139.200 ms | 212.346 ms | 1.525× |
| Warm full scan wall time | 27.013 ms | 64.173 ms | 2.376× |
| Reopen wall time | 1.678 ms | 613.001 ms | 365.309× |

Cold 32-key throughput is about 19,718 logical reads/s distributed versus
9,803 native: **2.011× higher**. Cold scalar p99 is 484.425 µs distributed versus
233.546 µs native; cold 32-key p99 is 2,216.192 versus 4,753.889 µs. Each batched
phase has only 128 calls per repetition, so these tails need more samples before
setting an SLO. Physical-I/O evidence confirms positive reads on every shard in
every cold phase, and zero whole-WAL residency immediately after eviction.
Native reads 41.387 MiB per 4,096-read cold phase versus 37.473 MiB across the TCP
nodes. Layout/readahead therefore differ; this is not equal physical disk work.
Only the 20 node-case snapshots that include cold phases have physical reads;
the other 60 remain page-cache workloads.

**The new TCP buffer/deadline changes did not close the warm gap.** In the same
cloud campaign, previous/current TCP load times are 204.815/212.346 ms, scalar
read p50s are 188.326/188.033 µs, and 32-key p50s are 471.613/456.244 µs. These are
+3.7%, −0.2%, and −3.3%, respectively. Large-load repetitions overlap; there is
no broad improvement. The preceding source's matched load gap is 1.457× and the
current gap is 1.525×. The earlier campaign's approximately 85× warm scalar gap
cannot be compared as a code effect with this run's 69×: the paired old binary
also measures about 71× here.

At 1 KiB/B1/W1, current load takes 95.883 ms TCP versus 20.223 ms native (4.741×),
and scalar p50 is 159.060 versus 0.509 µs (312.495×). Small warm operations expose
remote-call latency most strongly. The large warm mixed-phase wall time is
416.827 ms TCP versus 32.313 ms native (12.900×); this includes both reads and
writes and is not write-only performance.

#### Where time is spent

A separate diagnostic case records the following **mean** scalar times. They
must not be substituted for the ordinary p50s above.

| Component | Warm/reopened | Cache-evicted |
| --- | ---: | ---: |
| Native whole get | 3.496 µs | 138.926 µs |
| Native index | 1.216 µs | 0.796 µs |
| Native WAL + CRC/decode | 2.095 µs | 137.882 µs |
| Distributed whole get | 180.593 µs | 347.855 µs |
| Coordinator index | 0.844 µs | 0.959 µs |
| Client exchange, including server work | 170.487 µs | 337.481 µs |
| Response-header wait, nested in exchange | 166.773 µs | 333.434 µs |
| Server handler, nested in exchange | 2.451 µs | 146.517 µs |
| Server WAL read, nested in handler | 2.229 µs | 146.196 µs |
| Client record verification | 7.927 µs | 7.870 µs |

The warm exchange is about 94% of the whole call. Its residual after the server
handler is about 168 µs; the cold residual is about 191 µs. These residuals combine
network, TLS, socket handling and scheduling. **They are not isolated wire RTTs.**
The index and authority lookup are below 1 µs; server inventory wait is about
0.03 µs. This does not support changing the shard map or introducing a global WAL
to fix scalar latency. The direct scalar path has zero actor queue/service/return
clocks. Request send is 3.4–3.7 µs and timeout setup about 0.2 µs, so removing more
timeout setup alone cannot recover the remaining gap.

Writes have a different limit. The diagnostic load lasts 209.290 ms distributed
versus 142.160 ms native. Coordinator clocks record 98.832 ms validation/admission
(including 97.090 ms preparation), 89.351 ms waiting for storage completion, and
19.273 ms ordered publication. Preparation overlaps earlier remote work. Across
all five nodes and 120 append requests, handler time sums to 298.356 ms:
119.154 ms canonical batch decoding, 13.909 ms group construction, 65.235 ms WAL
append and 98.674 ms persistence. Inventory lock wait sums to only 0.012 ms.
These node sums overlap one another and the coordinator; do not add them to wall
time. Native records 130.982 ms batch commit, 3.547 ms batch preparation and
6.470 ms Value-WAL barriers. Distributed has 120 group persistence calls versus
10 native window barriers; the five-node calls are not globally serialized.

Exact paths behind these costs:

- `remote_db/preparation.rs::submit_many` prepares canonical batches and reserves
  reference/index capacity before assigning logical slots.
- `remote_db.rs::drain` waits for any shard completion; `publish_completed` keeps
  durable publication ordered and calls `PreparedNativePublication`.
- `tcp.rs::handle_profiled` handles `Request::Append`: `CompleteBatch::decode_owned`,
  `CompleteGroup::new`, `append_group`, then `persist_many`.
- `tcp.rs::request_inner` and `exchange` measure the protected request/response
  exchange; `read_records_inner` checks returned records through
  `CompleteBatch::decode_records`.
- `db.rs::get_profiled_for_benchmark` measures the native lookup/WAL path without
  substituting another storage implementation.

#### Consequences and remaining work

The separate NVMe devices can already help bounded parallel cold reads: the
32-key cluster call beats native. The earlier local cold ratio was 1.18× slower,
but that local source preceded the final response-layout change, and the exact
final local timings were contaminated by another build. Treat that contrast as
directional evidence, not a controlled hardware-only experiment.

The next measured targets are:

1. **Warm scalar reads:** reduce the protected remote-exchange latency, or let
   callers overlap independent reads. Larger batches already amortize it. A
   separate RDMA/EFA implementation could test this residual, but its achievable
   gain is unmeasured. It must preserve exact published-reference checks, epoch
   fencing, region lifetime, confidentiality/authentication and cold-read fallback.
   This run implements and measures TCP only.
2. **Writes:** profile and reduce coordinator canonical preparation and repeated
   node decoding/copying; evaluate persistence grouping separately. Keep retry
   identity validation, capacity reservation, durable receipts and ordered
   publication. Network improvements alone cannot remove these measured costs.
3. **Recovery:** the 365× reopen gap remains. This campaign measures its API time,
   but does not split recovery sub-stages. It is a separate unresolved target.
4. **Sustained storage scaling:** test a working set larger than effective RAM and
   controlled concurrency before claiming SSD saturation or production scaling.
   The current 96 MiB history bound plus explicit eviction is a cold-start test.

This is still a static RF1 experimental fork, not Sui integration, a replication
benchmark or a tested RDMA/NVMe-oF design. No change to native durability, integrity
checks or publication rules is justified by these timings.

#### Evidence, cost and cleanup

All evidence is under
[`resumed/aws-run`](../../logs/read-path-2026-10-07/resumed/aws-run/).
The archive is retained along with its receipt and source-bound cloud build hashes.
[All phase comparisons](../../logs/read-path-2026-10-07/resumed/aws-run/extracted/results/analysis/all_phases.md),
[all metrics CSV](../../logs/read-path-2026-10-07/resumed/aws-run/extracted/results/analysis/comparison.csv),
[validation report](../../logs/read-path-2026-10-07/resumed/aws-run/extracted/results/analysis/analysis.json),
and [detailed timing/I/O](../../logs/read-path-2026-10-07/resumed/aws-run/extracted/results/analysis/detailed_read_path.json)
retain the full measurements, not only favorable rows.

MCP created and launched the run. Recorded MCP timeouts and operator SSM denial
required the existing EC2 Instance Connect starter through an SDK fallback; no
operator IAM permission was expanded. All six guests proved their exact bundle,
NVMe and timer state. Temporary `/32` SSH ingress was revoked and ephemeral keys
removed. S3 progress, full retrieval and cleanup resumed through MCP after the
connection recovered. A nullable service-response error-reporting defect was fixed
in the operator only; it did not change the uploaded guest or benchmark source.

The controller exported and terminated all six guests at 14:55:16 UTC. Independent
MCP inspection confirms `terminated` for every exact ID. Launch-to-termination
runtime is 1.5125 aggregate instance-hours, estimated at **$0.532** for EC2, IPv4
and root EBS, excluding requests, transfer and termination-state latency. This is
an estimate, not an invoice; the reserved amount was $5. Cleanup completed through
MCP at 15:11:17 UTC: no owned EBS volumes remain, the run bucket and security group
were deleted, and both run roles/profiles and their policies were removed. The
[cleanup receipt](../../logs/read-path-2026-10-07/resumed/aws-run/cleanup-receipt.json)
confirms the two protected other-user instances remain running and unchanged.
The independent eight-query [final resource audit](../../logs/read-path-2026-10-07/resumed/aws-run/final-resource-audit.json)
also confirms terminated instances and absent volumes, security group, bucket,
roles and instance profiles.
No placement group or Elastic IP was created. No commit or push was made.

### Historical local cases before the pause

The following sections preserve the initial checks and stopping point from before
the resumption above. Their pending-work statements are historical.

Both cases completed their full-value and reopen audits. They used the preserved
`logs/read-path-2026-10-07/candidate/sharded_wal_bench` binary, whose exact source
and binary hashes are in the adjacent `source-manifest.json`. Do not substitute
the newer `candidate-final` binary when describing these measurements.

Configuration: 8 KiB values, 32 keys/batch, window 32, 128 load batches (32 MiB
of application values), 32 mixed batches, 128 reads, one repetition, application
read cache disabled, cold pass enabled, transport diagnostics enabled. Native
and TCP ran sequentially on the same laptop SSD. Other desktop work was active.
These are initial correctness/measurement checks, not a controlled performance
comparison with the preceding architecture. Only four get-many calls contribute
to each batched phase; do not treat their tails as stable estimates.

| Measured phase | Native | Five-shard TCP | Interpretation |
| --- | ---: | ---: | --- |
| Load phase wall time | 104.530 ms | 188.368 ms | Diagnostic clocks enabled for TCP; not a clean optimization comparison |
| Warm scalar get p50 | 6.419 µs | 127.929 µs | Warm local versus remote API cost |
| Warm get-many (32 keys) p50 | 164.108 µs | 1,074.029 µs | Four calls only |
| Cache-evicted scalar get p50 | 74.063 µs | 205.149 µs | Disk reads verified over the phase; some reads may still hit readahead/repeated pages |
| Cache-evicted get-many (32 keys) p50 | 2,450.241 µs | 1,419.264 µs | Parallel remote requests share one SSD; four calls only, no cluster claim |

Each of the native cold scalar, cold batched and independent cold diagnostic
phases recorded **5,115,904 physical read bytes**. Each corresponding TCP phase
recorded **2,183,168 bytes**, summed across five node processes. All five TCP
nodes recorded positive reads. Whole-WAL resident bytes immediately after
eviction were zero in both backends. Different layout/access/readahead behavior
means physical byte totals differ; equal application values do not establish
equal physical disk work.

The independent TCP cold diagnostic pass measured these **mean** times:

- Whole call: 251.000 µs.
- Authority index: 2.694 µs; dispatch: 0.343 µs; wait: 246.033 µs.
- Request encode: 1.344 µs; exchange: 217.761 µs; response decode: 1.021 µs;
  record verification: 24.530 µs.
- Nested server handler: 100.228 µs, including inventory wait 0.281 µs,
  authority lookup 0.758 µs, and record read/copy 98.917 µs.

These intervals overlap as documented above. In particular, server time is
inside exchange, and exchange plus record verification is inside client wait.
They cannot be summed into independent network and disk time. The reopened warm
diagnostic pass had 93.665 µs mean whole-call time, 71.511 µs exchange, 17.136 µs
verification and 5.936 µs nested server record read/copy. Separate warm/cold
passes have different cache histories and diagnostic costs.

Resources: native peak sampled RSS 75,432 KiB; TCP process-tree RSS 212,340 KiB.
Both had zero swap-in/out deltas and no remaining sampled processes. All temporary
database directories from these cases were removed by the harness. Raw outputs,
commands, timing samples, physical-I/O evidence and resource records remain under
`logs/read-path-2026-10-07/trial-*`.

### Code validation and exact stopping point

- The library suite passed 534 tests, with seven existing ignored tests, before
  the final diagnostic/Clippy cleanup.
- The subsequent TCP protocol/authentication/deadline suite passed all eight
  tests, including client/server diagnostic accounting and control-RPC exclusion.
- Strict Clippy (`-D warnings`) passed for the library and benchmark example.
  A separate pedantic review found 1,001 warnings across the existing codebase;
  it is not a pedantic-clean build. Several warnings in new helpers were then
  corrected; no broad inherited-code lint rewrite was attempted.
- `candidate-final` built successfully with both diagnostic features, including
  client write-exchange accounting. It has not been benchmarked.
- Final small edits after that build add missing error docs, checked timing/size
  conversions, restore the native diagnostic operation timer, and tidy helper
  imports/signatures. **These final source edits have not yet been compiled or
  tested.** The exact changed paths and complete source hashes are saved in
  `logs/read-path-2026-10-07/STOPPED_CHECKPOINT.json`.
- No commit or push was made.

### Historical remaining work at the pause, superseded above

1. Simplify/review the cold helper's pointer-width sample-index conversion; keep
   its key sequence identical to the other benchmark phases. Format and compile
   the final edits, then rerun the relevant checks. Do not treat the last built
   executable as matching the current checkout.
2. Freeze the exact resulting source and executable again. Preserve the existing
   pre-change baseline executable and its manifest.
3. When the user resumes benchmarking, run the planned interleaved baseline/new
   native/TCP comparison with diagnostics disabled. Then run separate diagnostics
   and repeated cold phases. This comparison has **not** run, so no speedup over
   the previous architecture is established by this slice.
4. Review all distributions, write/read stages, physical-I/O and swap evidence.
   Report both native-relative gaps and old/new changes; keep single-SSD limits
   explicit. The bounded cold-start test does not establish sustained capacity
   scaling or per-key physical misses.
5. Before any later approved AWS campaign, update the guest build feature set
   and controller matrix to include cold and diagnostic modes, verify artifact
   collection, and rebuild/bind every node to the exact source. No launch now.
6. RDMA remains unimplemented and unvalidated in this fork. Select and implement
   its security, region-lifetime and cold-read fallback protocol separately if
   requested; lack of laptop hardware blocks hardware measurements, not source
   design. Do not claim the TCP work is an RDMA implementation.

### Historical AWS resource check before the resumed campaign

The existing official AWS MCP adapter queried all non-terminated EC2 states in
all **23 enabled account regions**, with no pagination left and no failed region.
The check completed on 7 October 2026 at approximately 09:06 UTC. No instance
matched our current or historical Tidehunter benchmark tags/names. Four other-user
instances were running and were left untouched. Raw region results are in
`logs/read-path-2026-10-07/aws-all-regions.json`. This was a read-only EC2 check,
not a complete audit of every billable AWS service.

## Skill maintenance

The AGENTS.md-referenced `skills/` and `kvstore-*` skills are absent from this
checkout and the installed skill roots. No such file was edited. If restored,
the performance/testing/wire-protocol/build skills must describe the new explicit
diagnostics, Linux `test-utils` cold hooks, evidence checks and frozen feature set.
