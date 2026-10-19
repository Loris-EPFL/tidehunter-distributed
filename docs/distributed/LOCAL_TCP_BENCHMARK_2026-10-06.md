# Native, local authority and five-process TCP benchmark

Date: 6 October 2026. **96 matched-matrix release cases plus 24 supplemental window-32 cases completed; all correctness/reopen audits passed.**
This is the current TCP serving campaign. The [preceding local report](LOCAL_BENCHMARK_2026-10-06.md)
remains the historical source-bound baseline. [TCP readiness](LOCAL_TCP_READINESS_2026-10-06.md)
describes the implementation, subprocess failure tests and deployment boundary.

## 1. Reproducibility and meaning

The exact release command was `sharded_wal_bench --backend all --repetitions 3`.
The matrix is two value sizes (1/8 KiB), two batch sizes (1/32 operations), two
pending windows (1/8), four backends and three repetitions: 96 verified cases.
Each load phase has 128 complete batches; mixed/delete phases have 32 each.
Each TCP case starts five separate storage processes plus the coordinator, with
persistent mutually authenticated TLS sessions. All processes use one shared SSD.

Evidence:

- [Source/Cargo and binary manifest](../../logs/local-tcp-2026-10-06/release-source-manifest.json),
  [fresh release build](../../logs/local-tcp-2026-10-06/build-release.log),
  [completion marker](../../logs/local-tcp-2026-10-06/release-campaign/COMPLETE).
- [Campaign configuration](../../logs/local-tcp-2026-10-06/release-campaign/campaign.txt),
  [raw API samples](../../logs/local-tcp-2026-10-06/release-campaign/samples.csv),
  [phase summaries](../../logs/local-tcp-2026-10-06/release-campaign/summary.csv),
  [stage counters](../../logs/local-tcp-2026-10-06/release-campaign/stages.csv),
  [separate diagnostic read profiles](../../logs/local-tcp-2026-10-06/release-campaign/read_profiles.csv),
  [matched local comparison](../../logs/local-tcp-2026-10-06/comparison.csv).
- [Campaign stdout](../../logs/local-tcp-2026-10-06/benchmark.stdout.log),
  [stderr](../../logs/local-tcp-2026-10-06/benchmark.stderr.log),
  [resource sampling](../../logs/local-tcp-2026-10-06/benchmark-resources.json).
  The result directory retains per-case storage-node logs, configuration and PID evidence.

The source is the dirty implementation tree on `feat/sharded-value-wal`, based on
HEAD `13a16ca8bd632494fc6368f56d60d0806be4a869`; the manifest hashes current source,
not merely HEAD. Release binary SHA-256:
`057df487a161d4f1c9bc0fb2cc63dfc641fa4afcfee66f5ec3ed0dfd107c9e63`.
It was built offline/locked with `experimental-distributed`, without `test-utils`.
Root verified that current source/binary still match after the run.

The entire campaign took **65.20 s**, including native modes, process/certificate
startup, audits and cleanup. Sampled aggregate process-tree RSS peaked at
**338,352 KiB** (about 330.4 MiB), with up to six sampled concurrent processes.
All 121 sampled PIDs exited; system `pswpin`/`pswpout` deltas were zero. Sampling
was every 50 ms, so this is not an exact peak and shared pages can be counted once
per process. Per-child VmHWM also survives in case evidence; summing those marks
would not give a simultaneous aggregate peak. Temporary database data is removed.

### Acknowledgement and timing rules

`native_async_reference` returns through native foreground acknowledgement and has
no added per-window persistence barrier. It is an intentionally different reference
contract. `native_value_wal_barrier` persists all registered native Value-WAL files
plus their directory. Local/TCP modes return after the owning complete native-WAL
group is persisted and published. Both synchronous rows include persistence, but
file coverage and grouping costs differ; this is not an identical fsync algorithm.

Every table uses **the median of the three per-case mean API durations**. Ratios
are the ratio of those medians for the same workload shape, not medians of paired
ratios. A write sample is one **complete window** (batch size × window operations),
not one mutation. A multi-get sample is one call of up to 32 keys. Throughput uses
the instrumented whole phase, including driver/audit overhead. Tiny one-call phases
and 16-sample multi-get phases cannot support robust tail-latency conclusions.

## 2. Durable load windows and previous local architecture

Durations below are milliseconds per window. Native async is shown for context;
only the native barrier column is the synchronous ratio denominator.

| Value | Batch | Window | Native async ms | Native barrier ms | Local ms | TCP ms | Local/native | TCP/native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 0.006 | 0.685 | 0.951 | 1.174 | 1.39× | 1.71× |
| 1 KiB | 1 | 8 | 0.057 | 0.693 | 1.525 | 1.828 | 2.20× | 2.64× |
| 1 KiB | 32 | 1 | 0.141 | 0.869 | 1.335 | 1.902 | 1.54× | 2.19× |
| 1 KiB | 32 | 8 | 1.129 | 2.615 | 3.482 | 5.678 | 1.33× | 2.17× |
| 8 KiB | 1 | 1 | 0.009 | 0.670 | 1.024 | 1.323 | 1.53× | 1.97× |
| 8 KiB | 1 | 8 | 0.065 | 0.828 | 1.757 | 2.657 | 2.12× | 3.21× |
| 8 KiB | 32 | 1 | 0.463 | 1.566 | 2.772 | 5.426 | 1.77× | 3.46× |
| 8 KiB | 32 | 8 | 3.767 | 5.361 | 14.095 | 25.853 | 2.63× | 4.82× |

Compared with the preceding optimized local campaign (same window/batch shapes),
several local write cases improved, especially one-operation batches at window 8.
Large batches are mixed; controls drifted between runs. These are cumulative
implementation comparisons, not isolated attribution to one optimization.

| Value | Batch | Window | Previous local/native | Current local/native | Current/previous local time | Current/previous native time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 1.67× | 1.39× | 1.089× | 1.311× |
| 1 KiB | 1 | 8 | 4.19× | 2.20× | 0.554× | 1.055× |
| 1 KiB | 32 | 1 | 1.57× | 1.54× | 1.004× | 1.029× |
| 1 KiB | 32 | 8 | 2.17× | 1.33× | 0.854× | 1.394× |
| 8 KiB | 1 | 1 | 2.09× | 1.53× | 0.786× | 1.075× |
| 8 KiB | 1 | 8 | 3.60× | 2.12× | 0.579× | 0.983× |
| 8 KiB | 32 | 1 | 2.43× | 1.77× | 0.899× | 1.236× |
| 8 KiB | 32 | 8 | 2.61× | 2.63× | 1.309× | 1.298× |

The 1 KiB and 8 KiB single-operation/window-8 local means decreased about **45%**
and **42%**, respectively. The 8 KiB/batch-32/window-8 local mean increased **31%**,
while the native control increased **30%**; its relative gap stayed about 2.61–2.63×.
This is evidence for a useful small-batch improvement, not uniform convergence.

### Grouping and placement are real limits

The 128 sequential benchmark operation IDs place totals `[24, 28, 26, 24, 26]`
across five partitions. Within individual window-8 chunks, however, two windows
touch only two nodes and fourteen touch only three. Total balance therefore does
not prove that each pending window uses all five drives.

The current small-batch window-8 load uses **46 durable groups**, down from **82**
in the preceding local campaign; native uses **16** barriers. For 8 KiB × 32-op
batches, the one-fragment/1 MiB group limit raises the current count to **64**
(previously 105). The larger encoded batches cannot all share the same group.
These counts explain part of the write cost, but partitions still contend for
one SSD and its finite bandwidth/queues. The separate window-32 probe below is not mixed into matched window-8
historical comparisons.

## 3. Scalar reads, multi-get and scans

These are warm small-data tests with prior writes/audits and resident page-cache
possibility. They do not measure SSD cold-read throughput or independent drive
scaling. Durations are microseconds per scalar get or whole 32-key call.

| Value | Batch | Window | Native scalar µs | Local scalar µs | TCP scalar µs | Native 32-key µs | Local 32-key µs | TCP 32-key µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 1.21 | 3.34 | 109.67 | 43.10 | 105.17 | 525.63 |
| 1 KiB | 1 | 8 | 0.90 | 3.64 | 101.20 | 25.44 | 118.64 | 426.21 |
| 1 KiB | 32 | 1 | 1.56 | 5.63 | 120.64 | 45.19 | 127.54 | 521.73 |
| 1 KiB | 32 | 8 | 1.58 | 3.61 | 112.06 | 41.84 | 103.02 | 346.31 |
| 8 KiB | 1 | 1 | 2.03 | 14.30 | 207.35 | 59.27 | 490.30 | 1322.55 |
| 8 KiB | 1 | 8 | 1.79 | 16.88 | 195.52 | 61.00 | 452.72 | 1293.25 |
| 8 KiB | 32 | 1 | 3.07 | 20.15 | 214.94 | 101.30 | 530.91 | 1160.26 |
| 8 KiB | 32 | 8 | 2.96 | 14.76 | 203.97 | 93.25 | 501.71 | 1337.41 |

Local scalar means remain **2.28–9.44×** native. TCP scalar means are
**101–215 µs**, versus native **0.90–3.07 µs**, or **68.83–112.56×**. An exact-node
RPC still has framing, request/service scheduling, TLS, validation and copies;
it has not acquired native's in-process value-read latency. This benchmark has no
packet-level decomposition proving which of those costs dominates.

The batched TCP 32-key call is **8.28–22.31×** native. It uses concurrent grouped
requests across endpoints and amortizes exchange overhead. This helps relative to
32 sequential remote requests, while the native/local comparison loops actual
scalar APIs. Its whole-call latency must not be described as the latency of one
independent scalar get. Full scan is **1.13–5.50×** native locally and
**1.92–7.98×** over TCP, depending on value size/history.

Median-per-run tail statistics below show the range across the eight shapes.
They retain API-call units; they are not a pooled latency distribution.

| Backend | Phase | p50 µs range | p95 µs range | p99 µs range |
| --- | --- | ---: | ---: | ---: |
| Native barrier | `load_write_window` | 642.81–5054.61 | 898.15–6829.69 | 1288.93–6829.69 |
| Native barrier | `point_get` | 0.79–3.05 | 1.34–3.99 | 1.80–7.94 |
| Native barrier | `point_get_many` | 22.69–99.95 | 33.31–137.00 | 33.31–137.00 |
| Local | `load_write_window` | 906.65–13853.53 | 1364.08–17115.83 | 1629.38–17115.83 |
| Local | `point_get` | 2.87–20.19 | 4.39–27.08 | 5.68–32.07 |
| Local | `point_get_many` | 94.58–509.22 | 121.30–668.59 | 121.30–668.59 |
| TCP | `load_write_window` | 1113.17–25221.56 | 1699.97–29983.95 | 1991.58–29983.95 |
| TCP | `point_get` | 90.02–192.75 | 200.27–419.37 | 276.68–553.15 |
| TCP | `point_get_many` | 324.46–1249.11 | 634.48–1979.05 | 634.48–1979.05 |

## 4. All shared phase metrics

Ranges span workload shapes; row minima/maxima need not belong to the same case.
Durations are mean API microseconds, with one native-barrier denominator per matched
shape. `open`, `reopen`, maintenance and shutdown cover different backend lifecycle
work, so their ratios are descriptive. In particular, the new backends currently
replay **all retained WAL histories**, whereas native can use its existing recovery
metadata; high reopen ratios are not an isolated network penalty.

| Phase | Native µs range | Local µs range | TCP µs range | Local/native range | TCP/native range |
| --- | ---: | ---: | ---: | ---: | ---: |
| `checkpoint_capture` | 31.51–191.14 | 0.95–2.57 | 0.51–1.14 | 0.01–0.04× | 0.00–0.02× |
| `checkpoint_get` | 5.32–11.10 | 6.06–37.71 | 97.35–247.94 | 0.86–4.20× | 13.88–40.60× |
| `checkpoint_intervening_write` | 630.40–801.15 | 652.45–1205.85 | 927.16–3688.72 | 0.87–1.72× | 1.24–4.74× |
| `delete_window` | 610.32–1670.24 | 931.30–2092.78 | 1062.39–5721.77 | 1.11–2.04× | 1.63–7.81× |
| `exists` | 0.13–0.39 | 0.29–0.47 | 0.35–0.76 | 1.10–3.26× | 1.54–4.18× |
| `exists_missing` | 0.11–0.36 | 0.21–0.44 | 0.24–0.66 | 0.98–2.82× | 1.23–2.90× |
| `full_scan` | 314.27–14895.66 | 450.75–61969.02 | 781.80–79207.80 | 1.13–5.50× | 1.92–7.98× |
| `load_write_window` | 670.31–5361.49 | 951.37–14094.53 | 1174.11–25853.46 | 1.33–2.63× | 1.71–4.82× |
| `maintenance_flush_index` | 2478.62–11318.54 | 1238.93–12531.87 | 1376.81–12709.05 | 0.41–1.28× | 0.49–1.34× |
| `mixed_get` | 1.26–7.51 | 3.51–26.15 | 89.96–346.19 | 1.67–5.39× | 22.90–105.61× |
| `mixed_write_window` | 665.25–5421.57 | 983.97–13045.49 | 1018.13–30093.04 | 1.31–2.41× | 1.36–5.55× |
| `open` | 3827.48–5827.02 | 26148.45–33369.53 | 77235.68–83558.81 | 4.82–6.97× | 14.02–21.83× |
| `point_get` | 0.90–3.07 | 3.34–20.15 | 101.20–214.94 | 2.28–9.44× | 68.83–112.56× |
| `point_get_many` | 25.44–101.30 | 103.02–530.91 | 346.31–1337.41 | 2.44–8.27× | 8.28–22.31× |
| `point_get_missing` | 0.17–0.38 | 0.20–0.54 | 0.28–0.78 | 0.89–1.76× | 1.18–2.35× |
| `post_flush_get` | 1.25–4.14 | 2.35–14.09 | 91.67–171.62 | 0.76–6.86× | 25.16–104.72× |
| `reopen` | 2650.31–3548.00 | 42325.30–377123.69 | 78239.66–619531.86 | 12.01–106.29× | 22.19–181.24× |
| `reopen_get_audit` | 1.75–4.23 | 3.18–12.51 | 67.46–233.68 | 1.22–3.73× | 25.19–79.65× |
| `shutdown` | 1305.94–1807.24 | 12394.11–13820.43 | 20858.47–26666.59 | 7.50–10.40× | 13.63–19.45× |

`exists`/missing-key lookups can be answered from the authority index when the key
shape does not require WAL full-key validation; they are not remote value reads.
Cheap checkpoint capture pins a logical cut; it is not a durable accelerated replay
snapshot and cannot be used to explain away expensive reopen. Remote storage-stats
RPCs are excluded from all API/phase timers.

## 5. Attribution that the recorded clocks support

The local-only diagnostic after reopened history inspected 2,112 live reads per
value size. Aggregate stage durations divided by aggregate whole-call duration are:

| Value | Index | Membership | WAL range | Verification/decode | Whole-call mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 12.75% | 3.31% | 20.91% | 55.60% | 4.02 µs |
| 8 KiB | 4.23% | 0.94% | 10.87% | 82.05% | 15.87 µs |

The unlisted remainder includes clocks/wrapping and other call overhead. This is a
separately instrumented pass after recovery, not the normal point-read sample set.
Verification/decode includes checks, parsing and digest work; it does not isolate
hashing. WAL-range time includes mapped-range access/copy work and cannot be called
physical SSD latency. Membership itself is a small fraction in this warm workload.
Changing data structures for membership is therefore a weaker near-term hypothesis
than safely avoiding redundant decode/copy/verification work while retaining exact
reference membership, record integrity and immutable ownership.

For a 128-batch window-8 load, median cumulative counters illustrate the CPU/persistence
balance. Times are milliseconds; independently medianed stages are not guaranteed
to sum to the median API total, and worker stages overlap across nodes.

| Counter | Local 1 KiB/B1 | Local 8 KiB/B32 | TCP 1 KiB/B1 | TCP 8 KiB/B32 |
| --- | ---: | ---: | ---: | ---: |
| `validation_ns` | 0.891 | 117.383 | 1.048 | 129.998 |
| `dispatch_ns` | 0.270 | 0.580 | 0.139 | 0.215 |
| `storage_wait_ns` | 22.278 | 99.709 | 27.361 | 274.040 |
| `publication_ns` | 0.545 | 8.366 | 0.537 | 8.729 |
| `worker_admission_validation_ns` | 0.017 | 0.043 | not collected | not collected |
| `worker_ownership_ns` | 0.087 | 0.114 | not collected | not collected |
| `worker_queue_lock_wait_ns` | 0.002 | 0.002 | not collected | not collected |
| `worker_queue_wait_sum_ns` | 3.658 | 83.836 | not collected | not collected |
| `worker_group_build_ns` | 0.200 | 8.688 | not collected | not collected |
| `worker_append_ns` | 1.526 | 47.758 | not collected | not collected |
| `worker_persist_ns` | 56.229 | 155.601 | not collected | not collected |

Queue lock wait here is only a few microseconds over 128 batches. Worker queue wait
is larger, but it sums per-request waiting and can overlap persist/append; replacing
a short queue mutex does not remove durable storage service. The large-batch
coordinator validation counter is substantial. The remote `storage_wait_ns` includes
node queue/service, wire transport and reply processing, so it cannot be labeled
pure network latency. The node-to-authority response for a complete durable batch
must remain the durable authority for its eventual publication.

## 6. What this result supports

The new code is ready for a **bounded, controlled RF1 engineering experiment** on
separate SSDs if the remaining question is persistence overlap/drive scaling. It
has functioning multi-process reads, writes and recovery; it is not a Sui integration
or a production cluster. More local profiling of present CPU costs remains useful.

Priority hypotheses grounded in this campaign:

1. Keep complete per-partition Value-WAL records and one ordered publication
   authority, while reducing redundant owned-buffer processing under the same
   membership/digest checks. Do not drop integrity to obtain an artificial speedup.
2. Supply enough concurrent complete batches to use all target nodes, with bounded
   bytes and group sizes. Routing balance over a full run is insufficient.
3. Separate RPC fixed cost from per-byte cost with correlated node/client spans;
   measure a batched workload as well as dependent scalar latency. A new transport
   cannot remove coordinator publication, validation or retention requirements.
4. Add a proved replay checkpoint/retention boundary before expecting native reopen
   time. Do not reclaim the source WAL merely because the live index is rebuilt.
5. Measure independent NVMe concurrency only on a fresh source-bound cluster; do not
   extrapolate warm loopback reads into cold-drive or RDMA speed claims.

No speed-parity claim follows from these results. Complete batches stay on one
node, so one dependent request cannot aggregate five drives. Several pending
independent batches and sufficiently large I/O-bound work can use several drives;
coordinator CPU/NIC limits and each RPC remain. Replication, garbage collection,
Sui selection and cross-host authority consensus are still separate work.

## 7. Supplement: all-five-node fanout at window 32

The same unchanged release binary then ran 24 additional verified cases: two value
sizes × batch size 32 × window 32 × four backends × three repetitions. This is a
new workload, not a matched comparison with the preceding local architecture.
The exact source-bound [placement analysis](../../logs/local-tcp-2026-10-06/placement-analysis.json)
shows that all four 32-batch load windows target all five nodes.

| Value | Operations/window | Native barrier ms | Local ms | TCP ms | Local/native | TCP/native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 1,024 | 6.038 | 9.584 | 13.042 | 1.587× | 2.160× |
| 8 KiB | 1,024 | 18.451 | 47.238 | 63.506 | 2.560× | 3.442× |

The four load samples per case are too few for robust tail estimates. All-five-node
fanout establishes that the workload supplies parallel requests; it does not prove
disk saturation or independent-device utilization on this shared-SSD laptop.

The [supplement results](../../logs/local-tcp-2026-10-06/window32-campaign/summary.csv),
[raw samples](../../logs/local-tcp-2026-10-06/window32-campaign/samples.csv),
[stage counters](../../logs/local-tcp-2026-10-06/window32-campaign/stages.csv),
[completion marker](../../logs/local-tcp-2026-10-06/window32-campaign/COMPLETE) and
[resources](../../logs/local-tcp-2026-10-06/window32-resources.json) are kept separately.
Elapsed time was **19.84 s**; sampled aggregate peak RSS was **275,720 KiB**.
All 31 sampled PIDs exited and system swap deltas were zero. There were therefore
**120 verified release cases** in total, with no cloud operations.
