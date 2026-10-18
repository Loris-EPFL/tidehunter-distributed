# EFA cluster results, 7 October 2026

## Verdict

The authenticated EFA message transport works on independent NVMe hosts. It greatly reduces the warm scalar exchange delay in this deployment. It does **not** establish native parity for individual reads, single-write barriers, batched reads, or reopen. Batched write wall time is much closer: 1.28× native at 1 KiB and 1.24× at 8 KiB. These are RF1 results with no application value cache; they do not include replication or Sui execution.

## Evidence and scope

- Run: `th-2d6015657fbdf380`, six `c6id.32xlarge` hosts in one us-east-1b cluster placement group. Five storage nodes and one native/client host; one dedicated instance-store NVMe per host.
- All **68 API cases**, full-value/reopen audits, and **170 node archives** verified locally. All **75 EFA node archives** show positive hardware traffic. No swap-in or swap-out was observed.
- Three warm repetitions per current transport and shape. Previous-source TCP has one repetition per shape. Cold TCP and EFA each have two repetitions. Separate diagnostics have one repetition per transport.
- All native comparisons use the synchronous Value-WAL file/directory barrier. Each transport has its own adjacent native control. Tables keep those controls separate.
- Cold phases require recorded physical reads. The bounded datasets are at most about 96 MiB of logical values; this is not sustained larger-than-RAM, device-saturation, or failure-tolerant production evidence.
- EFA is two-sided message RPC under the existing mTLS session. It is not one-sided WAL reads or NVMe-oF.
- The controller rebuilt both archived sources in separate new Cargo target directories, then distributed the rebuilt executables through S3. The locally uploaded executable was not substituted for a cloud rebuild.

## Matched warm results

Each number is a median across three runs. Read latency is p50 per API call; a batched read requests 32 keys. Write time covers the full load: 320 write batches, with the stated batch size and outstanding window. Native-T and Native-E are the controls paired with TCP and EFA.

| Value / batch / window | Metric | Native-T | TCP | Native-E | EFA | EFA / native | TCP / EFA speedup |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 KiB / 1 / 1 | Full load (ms) | 32.339 | 449.602 | 34.145 | 203.826 | 5.97× | 2.21× |
| 1 KiB / 1 / 1 | Scalar read (µs) | 0.497 | 681.160 | 0.495 | 39.306 | 79.41× | 17.33× |
| 1 KiB / 1 / 1 | 32-key read (µs) | 17.446 | 1,146.399 | 17.500 | 415.099 | 23.72× | 2.76× |
| 1 KiB / 32 / 32 | Full load (ms) | 49.295 | 74.888 | 45.051 | 57.690 | 1.28× | 1.30× |
| 1 KiB / 32 / 32 | Scalar read (µs) | 0.922 | 672.582 | 0.862 | 40.024 | 46.43× | 16.80× |
| 1 KiB / 32 / 32 | 32-key read (µs) | 25.866 | 1,131.057 | 24.869 | 433.784 | 17.44× | 2.61× |
| 8 KiB / 1 / 1 | Full load (ms) | 123.142 | 516.648 | 130.747 | 265.826 | 2.03× | 1.94× |
| 8 KiB / 1 / 1 | Scalar read (µs) | 0.972 | 743.628 | 0.993 | 54.203 | 54.59× | 13.72× |
| 8 KiB / 1 / 1 | 32-key read (µs) | 55.875 | 1,490.369 | 55.234 | 753.103 | 13.63× | 1.98× |
| 8 KiB / 32 / 32 | Full load (ms) | 145.154 | 228.886 | 160.613 | 199.302 | 1.24× | 1.15× |
| 8 KiB / 32 / 32 | Scalar read (µs) | 2.998 | 753.709 | 3.038 | 55.655 | 18.32× | 13.54× |
| 8 KiB / 32 / 32 | 32-key read (µs) | 84.082 | 1,463.059 | 84.349 | 756.263 | 8.97× | 1.93× |

### Read tails and measured phase throughput

P99 values are medians of each run’s p99, not a pooled percentile. Throughput uses the full instrumented phase wall clock and counts logical keys, including all 32 keys per batched call. This is the harness’s offered concurrency, not maximum achievable transport throughput.

| 8 KiB, B32/W32 | Native-T | TCP | Native-E | EFA |
|---|---:|---:|---:|---:|
| point_get p99 (µs) | 4.194 | 938.871 | 4.286 | 67.063 |
| point_get keys/s | 264,386.147 | 1,362.764 | 259,689.259 | 17,734.686 |
| point_get_many p99 (µs) | 110.001 | 1,820.757 | 112.846 | 1,045.044 |
| point_get_many keys/s | 315,192.186 | 21,211.906 | 314,025.060 | 42,854.022 |

## Cold physical-read results

Controls are separate because native cold scalar latency varied between the two repetitions. Do not use one transport’s native control as the other’s denominator. Each cold phase evicts before its first call; later repeated keys can be warm. The diagnostic pass is separate.

| 8 KiB, B32/W32 | Native-T | TCP | Native-E | EFA | EFA / paired native |
|---|---:|---:|---:|---:|---:|
| cache_evicted_get p50 (µs) | 210.168 | 1,152.817 | 92.380 | 393.447 | 4.26× |
| cache_evicted_get_many p50 (µs) | 2,447.870 | 4,406.889 | 2,417.822 | 3,669.591 | 1.52× |

This fleet did not reproduce the previous i4i cold-batch advantage: EFA cold 32-key latency is 1.52× its native control, while the previous i4i TCP run was about 2× faster than its native control. Storage response, host size, cache state, and transport differ between those campaigns. This result rules out assuming that five NVMe drives automatically deliver lower latency for every workload.

## Where time remains

### Warm scalar read, diagnostic means in µs

The reopened-value-cache-cleared pass below remains warm in the OS page cache. The node’s independent warm pass supports the same conclusion. Nested and overlapping clocks must not be added as CPU percentages.

| Stage | TCP | EFA |
|---|---:|---:|
| Whole scalar call | 669.466 | 57.619 |
| Index lookup | 3.348 | 0.480 |
| Remote exchange, including server and scheduling | 617.224 | 48.035 |
| Response-header wait, within exchange | 583.251 | 46.557 |
| Shard handler, within exchange | 16.433 | 1.696 |
| Shard record read, within handler | 14.598 | 1.481 |
| Inventory lock wait, within handler | 0.225 | 0.029 |
| Client record verification | 36.334 | 7.858 |
| Scalar actor queue/service/return | 0 / 0 / 0 | 0 / 0 / 0 |

The scalar path already bypasses the actor queue. The measured inventory lock is not its main bottleneck. EFA removes most of the previous exchange delay, but transfer/progress and verification still dominate relative to a roughly 3 µs native cached read.

The separate warm node pass reports EFA response-send time of 37.182 µs/request, including 34.172 µs in TX drain. Those completion waits overlap client receipt and other clocks; they are not an additive 34 µs optimization budget. The next transport experiment must distinguish response posting, remote delivery, and local CQ completion.

### Batched reads

EFA warm 32-key p50 is 756.263 µs versus 84.349 µs native. The diagnostic run has 640 shard RPCs for 128 calls, exactly five RPCs per call. Mean per-shard EFA exchange is 153.133 µs and handler time is 9.216 µs. Source inspection confirms that `fetch_uncached` dispatches every nonempty shard group before waiting; the implementation does not deliberately serialize the five exchanges.

The present aggregate clocks do not isolate the critical-path actor wakeup, return wakeup, maximum shard completion, and client verification for a whole batched call. Instrument those boundaries before assigning the remaining difference to a global lock or promising a particular saving.

Cold diagnostic mean is 447.893 µs for EFA versus 108.548 µs native. EFA exchange is 438.177 µs, with 360.675 µs in the shard handler and 359.820 µs in record access. Native WAL/decode is 107.547 µs. A lower network delay alone will not remove this storage-path difference. The shard handler currently reads the requested records in a loop. Bounded within-shard I/O overlap is worth testing, with exact membership, retention, ordering and value verification preserved.

### Writes

For the diagnostic 8 KiB/B32/W32 EFA load, coordinator admission/validation is 82.958 ms, including 81.776 ms preparation; storage wait is 95.251 ms; publication is 20.342 ms. These clocks overlap.

Across all five nodes, EFA handler work totals 323.659 ms: canonical decode 116.639 ms, group construction 10.879 ms, WAL work 56.532 ms, and persistence 137.799 ms. TCP has nearly the same totals. These are sums of parallel node work, not one serial elapsed time. There are 120 shard persistence calls for this load, versus ten native window barriers.

EFA improves the transport and overlap, but cannot remove canonical preparation, publication, or durability barriers. Single-write barriers still take 5.97× native at 1 KiB and 2.03× native at 8 KiB. Batched write parity is more plausible than universal single-operation parity.

### Reopen

EFA warm 8 KiB/B32/W32 reopen takes 721.244 ms versus 2.538 ms native; TCP takes 589.121 ms. EFA adds connection/provider setup while recovery/inventory work remains. This regression must be kept visible. It is outside steady-state get/put timing and needs its own connection-establishment/recovery measurements.

## CPU and comparison limits

For the fifteen node samples in the warm 8 KiB/B32/W32 runs, median process CPU consumption per case-second is 4.837 ticks/s for TCP and 49.912 ticks/s for EFA, about 10.3×. The per-node median totals are 34 user + 44 system ticks for TCP and 113 + 135 for EFA. These counters cover the entire case, including idle time and the native phase; they do not isolate per-operation CPU.

Node peak RSS in those samples is 38,528 KiB TCP and 72,732 KiB EFA. These are per-process lifetime peaks, not a simultaneous cluster peak. Controller CPU time was not captured by the guest process recorder.

The previous TCP source rebuilt on these same hosts also has roughly 749 µs warm 8 KiB scalar p50; current TCP is 754 µs. Thus the much slower TCP latency than the prior i4i result (~188 µs) is not introduced by this source revision. Different instance size/NIC/host scheduling and active polling are confounders across fleets. The observed 13.54× EFA/TCP scalar speedup is for this matched deployment, not a universal EFA or pure-wire speedup. CPU stage timings also differ under blocking TCP and active EFA; CPU idle/frequency effects are a hypothesis that needs explicit measurement.

## Next changes, in order

1. Keep TCP as the default and explicit fallback. Keep the working EFA path optional. Preserve mTLS, authority fencing, immutable membership, complete batches, CRC/digest checks and durable receipts.
2. Add end-to-end batched-read worker dispatch/start/finish/return clocks and response posting/delivery/completion clocks. Use fixed request sets and a controlled CPU placement/polling policy to distinguish scheduling from fabric cost.
3. Test bounded within-shard cold-read concurrency/coalescing around `tcp.rs::read_response` and `storage.rs::read_member_range`; preserve the native WAL’s lifetime and retention rules. One global WAL does not by itself remove the measured per-record read delay.
4. Reduce measured preparation, canonical decode and publication work without dropping validation; batch durability only under the current acknowledgement contract. Follow the existing pipeline, rather than treating parallel node sums as serial time.
5. Measure and reduce EFA connection setup and recovery costs independently from steady-state RPC.
6. Consider one-sided exported immutable values only after defining retention, fencing, bounds, memory registration and cold-miss fallback. This campaign does not establish that one-sided RDMA or NVMe-oF would close the remaining gap. Neither makes a remote uncached read identical to a local page-cache read.

The measured results support continuing transport, batching and storage-path work. They do not support a promise of native latency for every small individual operation. See [the implementation and primary-source map](EFA_TRANSPORT_2026-10-07.md) and [the bounded transport invariants](../../tidehunter-fabric/DESIGN.md).

## Provenance, reporting corrections, and cleanup

- Uploaded bundle SHA256: `8f3831cea8a24d765cc1a74da7380c5a3b182d6b6816061e807f576209aeace0`.
- Measured source SHA256: `f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398`.
- Measured cloud executable SHA256: `94e5548ee203e6e5d24d4292337c6bd426bb0f2e93d7c910c86884892f776689`.
- Measured cloud libfabric: `2.6.0amzn1.0`, SHA256 `f6c6ae870fd94bc9302f360b80251a9b692ea7270e1ebf1e4886905814e23d2b`.
- Retrieved archive: 12,880,921 bytes; SHA256 `4d18fd5c5d76fb28c965c696979fe8a7824798342d8cb0d1685200901414f047`.
- Raw CSVs are unchanged. The exact measured source mislabeled only EFA cold `read_io.csv` and cache-evicted `remote_read_profiles.csv` rows as TCP. The analyzer accepts this known label only for that source, while still requiring EFA command flags, correct main CSV labels, and positive device counters. Three affected cases then validate; no timing value was changed.
- The label writer was corrected to use `Case::label()`, rebuilt locally and checked with a complete software-fabric cold/diagnostic run. All five relevant CSVs now use the selected transport. This later reporting-only source was not run on the terminated cloud fleet. Its source SHA256 is `08ea20812d8becf37204c9c8510fe96115439f29286a8557dfc5fd21472229e7`; executable SHA256 is `754a3204606ae371632ec60d5958a399481180b3ffc284cd670678d1de069392`.
- The verbose-provider readiness parser and mixed TCP/EFA node CSV column union were also corrected. These are operator-side validation fixes.
- All six instances were independently observed terminated at 16:51:22 UTC (18:51:22 Europe/Paris). Estimated upper bound for compute, public IPv4 and root storage is **$13.30**, excluding small S3/transfer/request costs and taxes. This is an estimate, not an invoice.
- Final ownership-scoped cleanup succeeded: all owned EBS volumes, roles/profiles, security group, placement group and S3 bucket were removed. Both protected users’ instances remained running. See `aws-run-b/cleanup-receipt.json`. The estimated spend is below the $42 run reserve and $60 experiment allowance.

Artifacts under `logs/efa-transport-2026-10-07/`: `aws-run-b/controller.tar.gz`, `aws-run-b/receipt.json`, `aws-run-b/extracted/results/`, `aws-analysis/analysis.json`, `aws-analysis/comparison.csv`, `aws-analysis/node_io.csv`, `aws-b-termination-audit.json`, and `cost-estimate.json`, and `final-aws-audit.json` (zero active managed benchmark instances and zero owned volumes). Full CSVs include p50/p95/p99/max, throughput, samples, stage counters, physical I/O and diagnostics.

## All warm phase wall times

These tables retain all phases. Units are milliseconds for the entire phase, not per-key latency. Mixed-read and mixed-write rows share a mixed-phase timer; do not add them. One-sample maintenance/open/reopen phases do not establish tail distributions.

### 1 KiB, batch 1, window 1

| Phase | Native-T ms | TCP ms | Native-E ms | EFA ms |
|---|---:|---:|---:|---:|
| checkpoint_capture | 0.218 | 0.003 | 0.241 | 0.003 |
| checkpoint_get | 0.005 | 0.668 | 0.004 | 0.088 |
| checkpoint_intervening_write | 0.638 | 1.392 | 0.767 | 0.574 |
| delete_window | 20.823 | 86.640 | 19.852 | 37.196 |
| exists | 0.520 | 1.360 | 0.536 | 1.433 |
| exists_missing | 0.506 | 1.110 | 0.493 | 0.749 |
| full_scan | 0.347 | 2.529 | 0.323 | 1.136 |
| load_write_window | 32.339 | 449.602 | 34.145 | 203.826 |
| maintenance_flush_index | 3.177 | 1.216 | 3.236 | 1.356 |
| mixed_get | 20.666 | 133.709 | 14.740 | 41.238 |
| mixed_write_window | 20.666 | 133.709 | 14.740 | 41.238 |
| open | 15.846 | 68.681 | 14.916 | 253.226 |
| point_get | 2.928 | 2,698.020 | 2.884 | 161.399 |
| point_get_many | 2.890 | 150.454 | 2.869 | 55.173 |
| point_get_missing | 0.550 | 1.082 | 0.519 | 1.266 |
| post_flush_get | 3.300 | 2,166.409 | 3.326 | 128.989 |
| reopen | 1.971 | 83.095 | 1.868 | 247.344 |
| reopen_get_audit | 0.379 | 168.937 | 0.359 | 10.798 |
| shutdown | 2.201 | 19.235 | 2.477 | 54.701 |

### 1 KiB, batch 32, window 32

| Phase | Native-T ms | TCP ms | Native-E ms | EFA ms |
|---|---:|---:|---:|---:|
| checkpoint_capture | 0.335 | 0.001 | 0.321 | 0.001 |
| checkpoint_get | 0.005 | 0.686 | 0.006 | 0.056 |
| checkpoint_intervening_write | 0.727 | 1.331 | 0.599 | 0.638 |
| delete_window | 5.952 | 9.522 | 7.358 | 6.387 |
| exists | 0.959 | 1.359 | 0.942 | 1.366 |
| exists_missing | 0.926 | 1.134 | 0.928 | 1.127 |
| full_scan | 7.877 | 43.217 | 7.356 | 16.155 |
| load_write_window | 49.295 | 74.888 | 45.051 | 57.690 |
| maintenance_flush_index | 9.712 | 10.879 | 12.334 | 11.412 |
| mixed_get | 11.165 | 1,261.441 | 11.110 | 92.799 |
| mixed_write_window | 11.165 | 1,261.441 | 11.110 | 92.799 |
| open | 17.523 | 61.480 | 12.859 | 245.385 |
| point_get | 4.873 | 2,685.207 | 4.695 | 163.578 |
| point_get_many | 3.988 | 149.301 | 3.861 | 55.945 |
| point_get_missing | 0.933 | 1.561 | 0.942 | 1.454 |
| post_flush_get | 6.509 | 2,134.261 | 6.678 | 130.517 |
| reopen | 2.524 | 186.658 | 2.787 | 324.219 |
| reopen_get_audit | 22.011 | 5,101.371 | 22.140 | 330.300 |
| shutdown | 2.053 | 20.895 | 2.241 | 56.093 |

### 8 KiB, batch 1, window 1

| Phase | Native-T ms | TCP ms | Native-E ms | EFA ms |
|---|---:|---:|---:|---:|
| checkpoint_capture | 0.143 | 0.001 | 0.228 | 0.003 |
| checkpoint_get | 0.006 | 0.818 | 0.006 | 0.147 |
| checkpoint_intervening_write | 1.018 | 1.826 | 0.950 | 1.020 |
| delete_window | 18.979 | 95.329 | 8.814 | 49.771 |
| exists | 0.527 | 1.325 | 0.541 | 1.020 |
| exists_missing | 0.498 | 1.252 | 0.523 | 1.357 |
| full_scan | 0.535 | 4.315 | 0.504 | 2.767 |
| load_write_window | 123.142 | 516.648 | 130.747 | 265.826 |
| maintenance_flush_index | 3.398 | 1.060 | 3.450 | 1.156 |
| mixed_get | 21.458 | 153.662 | 24.850 | 60.115 |
| mixed_write_window | 21.458 | 153.662 | 24.850 | 60.115 |
| open | 16.259 | 64.911 | 16.699 | 247.835 |
| point_get | 7.127 | 2,974.699 | 7.218 | 225.588 |
| point_get_many | 10.541 | 196.476 | 10.420 | 96.176 |
| point_get_missing | 0.542 | 1.292 | 0.558 | 1.107 |
| post_flush_get | 6.227 | 2,389.253 | 6.213 | 182.908 |
| reopen | 2.017 | 109.586 | 2.540 | 259.071 |
| reopen_get_audit | 1.315 | 194.124 | 1.302 | 15.072 |
| shutdown | 2.904 | 23.949 | 2.626 | 53.811 |

### 8 KiB, batch 32, window 32

| Phase | Native-T ms | TCP ms | Native-E ms | EFA ms |
|---|---:|---:|---:|---:|
| checkpoint_capture | 0.198 | 0.001 | 0.352 | 0.001 |
| checkpoint_get | 0.009 | 0.766 | 0.009 | 0.153 |
| checkpoint_intervening_write | 0.763 | 1.785 | 0.754 | 1.018 |
| delete_window | 6.932 | 10.360 | 6.793 | 6.466 |
| exists | 1.068 | 1.703 | 1.086 | 1.463 |
| exists_missing | 1.032 | 1.572 | 1.035 | 1.119 |
| full_scan | 27.639 | 73.585 | 28.230 | 35.524 |
| load_write_window | 145.154 | 228.886 | 160.613 | 199.302 |
| maintenance_flush_index | 13.037 | 11.346 | 10.837 | 11.216 |
| mixed_get | 31.930 | 1,511.550 | 34.730 | 150.208 |
| mixed_write_window | 31.930 | 1,511.550 | 34.730 | 150.208 |
| open | 15.848 | 64.193 | 17.018 | 249.740 |
| point_get | 15.492 | 3,005.657 | 15.773 | 230.960 |
| point_get_many | 12.995 | 193.099 | 13.044 | 95.580 |
| point_get_missing | 1.101 | 1.618 | 1.127 | 1.322 |
| post_flush_get | 15.252 | 2,363.120 | 15.214 | 182.525 |
| reopen | 2.631 | 589.121 | 2.538 | 721.244 |
| reopen_get_audit | 53.851 | 5,781.009 | 53.959 | 475.959 |
| shutdown | 2.734 | 26.933 | 2.735 | 53.935 |

## Skill maintenance

The referenced kvstore skill files are absent. If restored, the build, wire-protocol, concurrency, FFI and performance skills need the new optional feature, transport, buffer-lifetime and measurement contracts described in these documents. No existing skill file was silently left stale.
