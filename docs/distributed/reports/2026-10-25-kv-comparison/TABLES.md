# Complete numeric appendix — AWS KV comparison, 2026-10-25

Run `th-7dea3c080cb44d48`. All eight systems; every completed and missing case retained. See [research report](REPORT.md) for graphs and interpretation.

## Sources, units and aggregation

- [Original comparison.csv](all-results.csv): **all 1,084 original rows**, including each repetition, operation/outcome, case status, case rates, error text and API mean/min/stddev/p50/p95/p99/p999/max; response/scheduling p50/p95/p99/p999/max are also retained there.
- [Original comparison.json](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/comparison.json): all 684 planned cases, full-precision metrics, population/sample counts, configurations, acknowledgement and measurement boundaries, counters and evidence references.
- [Original comparison.md](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/comparison.md): original per-repetition report. [Original matrix.json](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/matrix.json): planned workload/configuration definitions. These four received files are linked in place and unmodified.
- All three requested data sources were read programmatically with Python standard-library JSON/CSV/statistics processing. All 684 case IDs agree between matrix, JSON and CSV; JSON job records equal matrix records. Every JSON measured operation/outcome is present in CSV. All 478 completed cases have `measurement_rows_complete=true`; none of the 206 failed/not-run cases has admitted metrics.
- **Matched aggregate N=2 means repetitions 0 and 1 only.** Each numeric aggregate is the median of the two corresponding per-repetition numbers: `median(x_r0,x_r1)=(x_r0+x_r1)/2`. In particular, an API-p50 cell is the median of two per-repetition p50s, **not a pooled-sample p50**; the same rule applies to p99, response/scheduling tails, rates and counts. No operations, sizes, backends or repetitions are pooled. Repetition-2 FoundationDB data is shown separately without aggregation.
- Latency units are µs (source ns / 1,000). p50/p99 refer to the specified **operation/outcome** population. Unless an outcome is explicitly named, latency is conditional on `success`. A `p50 / p99` cell contains those two separate medians. Display precision is three decimals for latency and two decimals for rates; source files retain full precision.
- `Successful calls/s`, `Logical keys/s`, `Value MiB/s` and `Unsuccessful calls` are the source case-level fields, aggregated independently. Count medians may be fractional; offered-load tables additionally show exact r0/r1 counts. Rates use each case’s `elapsed_seconds` / observed raw measurement span, not a pooled duration. MiB = 1,048,576 bytes.
- `read-many32` latency measures one complete ordered **32-key API call**, not per-key latency. Its calls/s and logical keys/s are distinct. Adapter strategy is part of the measured contract: FoundationDB performs 32 sequential native gets in one serializable transaction; native performs ordered scalar `Db::get` calls; TiKV uses native RawKV `batch_get`; Cassandra uses prepared-query fanout bounded at 16; Tidehunter distributed adapters use ordered grouped reads.
- Primary cohort is `primary_rf1`; native uses `value_wal_barrier`. RF1 does **not** establish equivalent HA, transaction, security or fsync/acknowledgement contracts across engines; the original JSON retains those definitions. Linux page caches remain warm in primary cases; native `page_cache` is an explicitly separate extra. Counter evidence is cumulative across preload/warmup/postworkload, not a per-measurement delta; no counter-derived rate is calculated here.
- `—` means absent/unavailable, never zero. The original quantiles are exact nearest-rank timed-sample quantiles. Source tail caveats mark p99 descriptive below 10,000 calls and p999 descriptive below 100,000 calls; raw population/sample counts remain available.

## Coverage and common complete repetitions

**478 completed / 3 failed / 203 not_run = 684 planned cases.** The run is incomplete (`complete=false`); a completed execution can still contain rejected arrivals.

| Backend | Planned | Completed | Failed | not_run | Primary r0 completed | Primary r1 completed | Primary r2 completed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| native | 105 | 70 | 0 | 35 | 24 | 24 | 0 |
| tidehunter_tcp | 87 | 58 | 0 | 29 | 24 | 24 | 0 |
| tidehunter_efa | 87 | 58 | 0 | 29 | 24 | 24 | 0 |
| tidehunter_efa_direct | 87 | 58 | 0 | 29 | 24 | 24 | 0 |
| tidehunter_efa_native | 93 | 62 | 0 | 31 | 24 | 24 | 0 |
| tikv | 75 | 50 | 0 | 25 | 24 | 24 | 0 |
| cassandra | 75 | 50 | 0 | 25 | 24 | 24 | 0 |
| foundationdb | 75 | 72 | 3 | 0 | 24 | 24 | 24 |

The complete common primary repetitions are **r0 and r1**: every one of the 8 backends completed the same 24 profile/size configurations in each repetition, with complete measurement rows. This is **192 matched configurations / 384 admitted case repetitions**. Each backend’s 24 configurations are 4 main profiles × 5 sizes, offered-1k/10k/50k at 1,024 B, and Zipf read at 1,024 B. FoundationDB has 24 additional completed primary r2 cases; all seven other backends’ primary r2 cases are `not_run`.

| Repeat | Planned | Completed | Failed | not_run |
| --- | --- | --- | --- | --- |
| 0 | 228 | 227 | 1 | 0 |
| 1 | 228 | 227 | 1 | 0 |
| 2 | 228 | 24 | 1 | 203 |

| Profile (all cohorts) | Planned value sizes B | Planned | Completed | Failed | not_run |
| --- | --- | --- | --- | --- | --- |
| read-concurrent | 64, 256, 1024, 8192, 65536 | 120 | 85 | 0 | 35 |
| write-concurrent | 64, 256, 1024, 8192, 65536 | 120 | 85 | 0 | 35 |
| read-many32 | 64, 256, 1024, 8192, 65536 | 135 | 95 | 0 | 40 |
| point-mixed | 64, 256, 1024, 8192, 65536 | 135 | 95 | 0 | 40 |
| offered-1000 | 1024 | 24 | 17 | 0 | 7 |
| offered-10000 | 1024 | 24 | 17 | 0 | 7 |
| offered-50000 | 1024 | 24 | 17 | 0 | 7 |
| zipf-read | 1024 | 24 | 17 | 0 | 7 |
| coordinator-cache | 256, 8192 | 24 | 16 | 0 | 8 |
| owner-cache | 256, 8192 | 24 | 16 | 0 | 8 |
| registered-ram | 256, 8192 | 6 | 4 | 0 | 2 |
| cold-os-large | 65536 | 24 | 14 | 3 | 7 |

| Cohort | Planned | Completed | Failed | not_run |
| --- | --- | --- | --- | --- |
| primary_rf1 | 576 | 408 | 0 | 168 |
| native_pagecache | 30 | 20 | 0 | 10 |
| coordinator_cache_configured | 24 | 16 | 0 | 8 |
| owner_cache_configured | 24 | 16 | 0 | 8 |
| registered_ram_configured | 6 | 4 | 0 | 2 |
| cold_os_cache | 24 | 14 | 3 | 7 |

### Failed cases (no admitted measurement metrics)

| Exact case ID | Recorded error | Metrics unavailable | Gate released | OS caches dropped |
| --- | --- | --- | --- | --- |
| `0202-foundationdb-65536-cold-os-large-cold_os_cache-r0` | RuntimeError: core exited before quiescent measurement release: {"MainPID": "0", "Result": "exit-code", "ExecMainStatus": "1", "ControlGroup": "", "MemoryPeak": "34439168", "CPUUsageNSec": "7434233000", "IOReadBytes": "0", "IOWriteBytes": "17666048", "CPUQuotaPerSecUSec": "3s", "MemoryMax": "6442450944", "LoadState": "loaded", "ActiveState": "failed", "SubState": "failed"} | `validated execution has no observed measurement calls` | false | false |
| `0372-foundationdb-65536-cold-os-large-cold_os_cache-r1` | RuntimeError: core exited before quiescent measurement release: {"MainPID": "0", "Result": "exit-code", "ExecMainStatus": "1", "ControlGroup": "", "MemoryPeak": "34426880", "CPUUsageNSec": "7569041000", "IOReadBytes": "0", "IOWriteBytes": "17690624", "CPUQuotaPerSecUSec": "3s", "MemoryMax": "6442450944", "LoadState": "loaded", "ActiveState": "failed", "SubState": "failed"} | `validated execution has no observed measurement calls` | false | false |
| `0480-foundationdb-65536-cold-os-large-cold_os_cache-r2` | DeadlineError: absolute deadline/export reserve reached | `failed before trustworthy raw artifact admission` | — | — |

The two r0/r1 FoundationDB cold cases failed during preload after approximately 600 s, before quiescent measurement release; [r0 client.log](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/cases/0202-foundationdb-65536-cold-os-large-cold_os_cache-r0/client.log) and [r1 client.log](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/cases/0372-foundationdb-65536-cold-os-large-cold_os_cache-r1/client.log) each record `global deadline expired before preload completed`, with 1,905 timeout/unknown and zero error/wrong-value preload outcomes. Those are preload failures, not admitted measurement-call counts. r2 reached the absolute-deadline/export-reserve condition. The intended cold dataset is 131,072 keys × 65,536 value B = 8 GiB of values. Failed/not-run cases are never represented as zero latency or zero throughput. Exact status and case-prefix coverage for every planned configuration appears in the final ledger.

FoundationDB control-service failure is established in all three cold attempts: [stop-3255 control export](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/deployments/foundationdb-stop-3255/host--1-export.json), [stop-5645 control export](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/deployments/foundationdb-stop-5645/host--1-export.json), and [stop-7360 control export](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/deployments/foundationdb-stop-7360/host--1-export.json) each record `ERROR: Out of memory`, `ExitCode=20`, `Running=false`, `Status=exited`, and `OOMKilled=false`. FinishedAt timestamps place these process exits approximately 5.5–7.1 s after the corresponding cold-preload client starts. The recorded sequence is early FoundationDB control-server OOM/service loss followed by client preload timeouts (r0/r1) or the global deadline (r2), not an observed 600 s healthy-engine ingest. `OOMKilled=false` does not erase the explicit process-level out-of-memory error. The exact internal allocation responsible is not established. This AWS run configured 2,048 MiB for the control service and 6,144 MiB for the client within an 8,192 MiB coordinator-side budget; its failed cold attempts do **not** establish inherent FoundationDB 8 GiB engine performance.

**Post-run local correction, excluded from every AWS metric table:** [separate local qualification](../../../../checkpoint-work/kv-comparison/fdbbulk-3eac9a194513/qualification.json) records a corrected 6,144 MiB control-service / 2,048 MiB client reservation (8,192 MiB total), control-process limit 4,608 MiB and cache 512 MiB. It records 131,072 successful preload operations totaling 8 GiB, 64 successful measurement reads, zero other outcomes, all six servers running after preload/reads, and owned servers/data subsequently removed. This is a local shared-SSD functional qualification, without OS-cache dropping and without a locally cgroup-capped client; it is **not an AWS rerun or replacement performance result**. All original AWS primary figures retain their pre-fix 2 GiB control / 6 GiB client label, and all received artifacts remain unchanged.

## Primary pure profiles

All tables in this section: `primary_rf1`, uniform keys, 4,096 configured keys, concurrency 16, matched r0/r1, **N=2 per row**. All five value sizes and all eight backends are included. Unsuccessful calls are median per-case counts.

### read-concurrent

100% point read; 8,192 measurement calls and 512 warmup calls per repetition.

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | native | 2 | 1.920 | 3.340 | 133,499.98 | 133,499.98 | 8.15 | 0.00 |
| 64 | tidehunter_tcp | 2 | 182.005 | 691.459 | 27,233.15 | 27,233.15 | 1.66 | 0.00 |
| 64 | tidehunter_efa | 2 | 93.238 | 323.766 | 56,461.06 | 56,461.06 | 3.45 | 0.00 |
| 64 | tidehunter_efa_direct | 2 | 93.412 | 341.099 | 55,578.13 | 55,578.13 | 3.39 | 0.00 |
| 64 | tidehunter_efa_native | 2 | 88.522 | 319.731 | 72,757.03 | 72,757.03 | 4.44 | 0.00 |
| 64 | tikv | 2 | 345.673 | 580.451 | 38,437.38 | 38,437.38 | 2.35 | 0.00 |
| 64 | cassandra | 2 | 243.939 | 359.688 | 55,385.83 | 55,385.83 | 3.38 | 0.00 |
| 64 | foundationdb | 2 | 580.106 | 824.966 | 24,913.37 | 24,913.37 | 1.52 | 0.00 |
| 256 | native | 2 | 1.740 | 3.240 | 134,280.09 | 134,280.09 | 32.78 | 0.00 |
| 256 | tidehunter_tcp | 2 | 199.156 | 743.852 | 27,432.48 | 27,432.48 | 6.70 | 0.00 |
| 256 | tidehunter_efa | 2 | 92.898 | 341.167 | 48,176.77 | 48,176.77 | 11.76 | 0.00 |
| 256 | tidehunter_efa_direct | 2 | 91.087 | 329.585 | 56,954.17 | 56,954.17 | 13.90 | 0.00 |
| 256 | tidehunter_efa_native | 2 | 85.812 | 340.112 | 49,922.75 | 49,922.75 | 12.19 | 0.00 |
| 256 | tikv | 2 | 365.334 | 657.736 | 34,706.42 | 34,706.42 | 8.47 | 0.00 |
| 256 | cassandra | 2 | 242.625 | 356.083 | 55,452.17 | 55,452.17 | 13.54 | 0.00 |
| 256 | foundationdb | 2 | 598.873 | 855.606 | 24,359.05 | 24,359.05 | 5.95 | 0.00 |
| 1024 | native | 2 | 1.950 | 3.351 | 142,952.50 | 142,952.50 | 139.60 | 0.00 |
| 1024 | tidehunter_tcp | 2 | 192.695 | 696.385 | 29,228.66 | 29,228.66 | 28.54 | 0.00 |
| 1024 | tidehunter_efa | 2 | 97.253 | 366.028 | 47,446.16 | 47,446.16 | 46.33 | 0.00 |
| 1024 | tidehunter_efa_direct | 2 | 95.938 | 359.087 | 49,607.81 | 49,607.81 | 48.45 | 0.00 |
| 1024 | tidehunter_efa_native | 2 | 88.612 | 320.101 | 64,919.32 | 64,919.32 | 63.40 | 0.00 |
| 1024 | tikv | 2 | 373.972 | 683.914 | 35,268.84 | 35,268.84 | 34.44 | 0.00 |
| 1024 | cassandra | 2 | 240.495 | 356.288 | 55,319.81 | 55,319.81 | 54.02 | 0.00 |
| 1024 | foundationdb | 2 | 594.067 | 832.206 | 24,462.11 | 24,462.11 | 23.89 | 0.00 |
| 8192 | native | 2 | 4.185 | 7.601 | 138,908.12 | 138,908.12 | 1,085.22 | 0.00 |
| 8192 | tidehunter_tcp | 2 | 222.382 | 885.075 | 21,357.72 | 21,357.72 | 166.86 | 0.00 |
| 8192 | tidehunter_efa | 2 | 111.124 | 405.537 | 50,192.72 | 50,192.72 | 392.13 | 0.00 |
| 8192 | tidehunter_efa_direct | 2 | 106.594 | 375.135 | 45,154.34 | 45,154.34 | 352.77 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 98.173 | 354.327 | 46,544.88 | 46,544.88 | 363.63 | 0.00 |
| 8192 | tikv | 2 | 378.605 | 649.541 | 34,310.62 | 34,310.62 | 268.05 | 0.00 |
| 8192 | cassandra | 2 | 331.567 | 505.466 | 40,816.77 | 40,816.77 | 318.88 | 0.00 |
| 8192 | foundationdb | 2 | 721.171 | 1,006.694 | 20,531.70 | 20,531.70 | 160.40 | 0.00 |
| 65536 | native | 2 | 15.117 | 34.163 | 86,314.97 | 86,314.97 | 5,394.69 | 0.00 |
| 65536 | tidehunter_tcp | 2 | 379.389 | 1,466.319 | 12,096.10 | 12,096.10 | 756.01 | 0.00 |
| 65536 | tidehunter_efa | 2 | 179.959 | 850.243 | 20,924.06 | 20,924.06 | 1,307.75 | 0.00 |
| 65536 | tidehunter_efa_direct | 2 | 231.839 | 1,065.169 | 17,984.94 | 17,984.94 | 1,124.06 | 0.00 |
| 65536 | tidehunter_efa_native | 2 | 206.692 | 1,032.736 | 18,906.20 | 18,906.20 | 1,181.64 | 0.00 |
| 65536 | tikv | 2 | 649.426 | 1,298.478 | 17,850.47 | 17,850.47 | 1,115.65 | 0.00 |
| 65536 | cassandra | 2 | 434.170 | 671.409 | 30,415.13 | 30,415.13 | 1,900.95 | 0.00 |
| 65536 | foundationdb | 2 | 1,037.822 | 2,458.707 | 13,111.30 | 13,111.30 | 819.46 | 0.00 |

### write-concurrent

100% point put; 4,096 measurement calls and 512 warmup calls per repetition.

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | native | 2 | 820.811 | 2,729.244 | 13,340.41 | 13,340.41 | 0.81 | 0.00 |
| 64 | tidehunter_tcp | 2 | 1,190.646 | 142,284.350 | 3,963.64 | 3,963.64 | 0.24 | 0.00 |
| 64 | tidehunter_efa | 2 | 886.613 | 172,484.584 | 3,529.84 | 3,529.84 | 0.22 | 0.00 |
| 64 | tidehunter_efa_direct | 2 | 875.344 | 164,117.810 | 3,934.46 | 3,934.46 | 0.24 | 0.00 |
| 64 | tidehunter_efa_native | 2 | 863.529 | 169,738.348 | 3,560.30 | 3,560.30 | 0.22 | 0.00 |
| 64 | tikv | 2 | 485.652 | 801.433 | 28,284.06 | 28,284.06 | 1.73 | 0.00 |
| 64 | cassandra | 2 | 356.829 | 777.962 | 35,425.97 | 35,425.97 | 2.16 | 0.00 |
| 64 | foundationdb | 2 | 965.653 | 1,581.057 | 15,169.27 | 15,169.27 | 0.93 | 0.00 |
| 256 | native | 2 | 699.491 | 2,954.239 | 14,364.94 | 14,364.94 | 3.51 | 0.00 |
| 256 | tidehunter_tcp | 2 | 1,206.722 | 141,610.755 | 4,273.97 | 4,273.97 | 1.04 | 0.00 |
| 256 | tidehunter_efa | 2 | 900.767 | 169,742.751 | 3,841.77 | 3,841.77 | 0.94 | 0.00 |
| 256 | tidehunter_efa_direct | 2 | 909.632 | 174,375.204 | 3,471.11 | 3,471.11 | 0.85 | 0.00 |
| 256 | tidehunter_efa_native | 2 | 889.065 | 166,032.603 | 3,873.31 | 3,873.31 | 0.95 | 0.00 |
| 256 | tikv | 2 | 499.725 | 851.117 | 27,001.22 | 27,001.22 | 6.59 | 0.00 |
| 256 | cassandra | 2 | 365.224 | 670.814 | 34,815.05 | 34,815.05 | 8.50 | 0.00 |
| 256 | foundationdb | 2 | 959.351 | 1,783.908 | 15,054.48 | 15,054.48 | 3.68 | 0.00 |
| 1024 | native | 2 | 850.668 | 3,640.407 | 11,627.07 | 11,627.07 | 11.35 | 0.00 |
| 1024 | tidehunter_tcp | 2 | 1,325.220 | 141,248.518 | 3,800.96 | 3,800.96 | 3.71 | 0.00 |
| 1024 | tidehunter_efa | 2 | 959.332 | 165,474.718 | 3,549.03 | 3,549.03 | 3.47 | 0.00 |
| 1024 | tidehunter_efa_direct | 2 | 971.532 | 166,977.465 | 3,535.86 | 3,535.86 | 3.45 | 0.00 |
| 1024 | tidehunter_efa_native | 2 | 961.921 | 171,267.880 | 3,526.38 | 3,526.38 | 3.44 | 0.00 |
| 1024 | tikv | 2 | 627.754 | 1,028.871 | 22,362.25 | 22,362.25 | 21.84 | 0.00 |
| 1024 | cassandra | 2 | 493.840 | 908.634 | 27,802.76 | 27,802.76 | 27.15 | 0.00 |
| 1024 | foundationdb | 2 | 967.750 | 1,424.490 | 15,274.84 | 15,274.84 | 14.92 | 0.00 |
| 8192 | native | 2 | 993.906 | 2,945.823 | 11,893.13 | 11,893.13 | 92.92 | 0.00 |
| 8192 | tidehunter_tcp | 2 | 1,860.630 | 141,738.883 | 3,638.83 | 3,638.83 | 28.43 | 0.00 |
| 8192 | tidehunter_efa | 2 | 1,417.698 | 168,105.646 | 3,101.68 | 3,101.68 | 24.23 | 0.00 |
| 8192 | tidehunter_efa_direct | 2 | 1,424.363 | 171,437.959 | 3,093.46 | 3,093.46 | 24.17 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 1,429.849 | 169,923.773 | 3,120.53 | 3,120.53 | 24.38 | 0.00 |
| 8192 | tikv | 2 | 761.632 | 1,417.750 | 17,446.75 | 17,446.75 | 136.30 | 0.00 |
| 8192 | cassandra | 2 | 695.334 | 1,106.791 | 19,334.50 | 19,334.50 | 151.05 | 0.00 |
| 8192 | foundationdb | 2 | 1,093.261 | 2,044.660 | 12,170.53 | 12,170.53 | 95.08 | 0.00 |
| 65536 | native | 2 | 3,317.349 | 7,017.793 | 4,052.11 | 4,052.11 | 253.26 | 0.00 |
| 65536 | tidehunter_tcp | 2 | 4,291.917 | 8,331.932 | 2,880.78 | 2,880.78 | 180.05 | 0.00 |
| 65536 | tidehunter_efa | 2 | 4,252.685 | 93,784.280 | 2,322.87 | 2,322.87 | 145.18 | 0.00 |
| 65536 | tidehunter_efa_direct | 2 | 4,679.786 | 75,794.966 | 2,209.90 | 2,209.90 | 138.12 | 0.00 |
| 65536 | tidehunter_efa_native | 2 | 4,826.514 | 23,055.635 | 2,268.00 | 2,268.00 | 141.75 | 0.00 |
| 65536 | tikv | 2 | 1,115.510 | 2,411.446 | 11,460.39 | 11,460.39 | 716.27 | 0.00 |
| 65536 | cassandra | 2 | 1,249.338 | 8,568.526 | 8,605.48 | 8,605.48 | 537.84 | 0.00 |
| 65536 | foundationdb | 2 | 3,296.096 | 5,194.909 | 4,625.77 | 4,625.77 | 289.11 | 0.00 |

### read-many32

100% get_many, 32 logical keys per API call; 2,048 measurement calls and 512 warmup calls per repetition.

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | native | 2 | 56.159 | 89.703 | 111,752.95 | 3,576,094.54 | 218.27 | 0.00 |
| 64 | tidehunter_tcp | 2 | 1,601.781 | 2,068.038 | 9,357.67 | 299,445.43 | 18.28 | 0.00 |
| 64 | tidehunter_efa | 2 | 1,425.396 | 5,826.810 | 9,201.75 | 294,456.11 | 17.97 | 0.00 |
| 64 | tidehunter_efa_direct | 2 | 1,324.528 | 6,376.302 | 9,312.26 | 297,992.20 | 18.19 | 0.00 |
| 64 | tidehunter_efa_native | 2 | 1,101.672 | 7,251.533 | 10,590.84 | 338,906.77 | 20.69 | 0.00 |
| 64 | tikv | 2 | 6,297.739 | 10,148.367 | 2,437.79 | 78,009.17 | 4.76 | 0.00 |
| 64 | cassandra | 2 | 2,265.204 | 19,428.537 | 5,796.56 | 185,490.03 | 11.32 | 0.00 |
| 64 | foundationdb | 2 | 9,584.072 | 11,296.919 | 1,626.14 | 52,036.60 | 3.18 | 0.00 |
| 256 | native | 2 | 52.949 | 82.091 | 114,910.80 | 3,677,145.62 | 897.74 | 0.00 |
| 256 | tidehunter_tcp | 2 | 1,761.749 | 2,300.802 | 8,325.75 | 266,423.88 | 65.04 | 0.00 |
| 256 | tidehunter_efa | 2 | 1,486.470 | 5,367.721 | 8,389.97 | 268,479.01 | 65.55 | 0.00 |
| 256 | tidehunter_efa_direct | 2 | 1,508.836 | 5,243.834 | 8,492.18 | 271,749.80 | 66.35 | 0.00 |
| 256 | tidehunter_efa_native | 2 | 1,298.088 | 7,061.472 | 9,204.59 | 294,546.96 | 71.91 | 0.00 |
| 256 | tikv | 2 | 6,971.760 | 10,586.263 | 2,195.78 | 70,264.92 | 17.15 | 0.00 |
| 256 | cassandra | 2 | 3,212.459 | 14,646.149 | 4,649.58 | 148,786.40 | 36.32 | 0.00 |
| 256 | foundationdb | 2 | 9,854.356 | 12,658.979 | 1,543.45 | 49,390.55 | 12.06 | 0.00 |
| 1024 | native | 2 | 58.409 | 131.245 | 114,638.41 | 3,668,429.06 | 3,582.45 | 0.00 |
| 1024 | tidehunter_tcp | 2 | 1,819.024 | 2,540.776 | 8,028.81 | 256,921.83 | 250.90 | 0.00 |
| 1024 | tidehunter_efa | 2 | 1,631.293 | 5,584.499 | 7,909.71 | 253,110.63 | 247.18 | 0.00 |
| 1024 | tidehunter_efa_direct | 2 | 1,596.200 | 5,639.293 | 7,728.00 | 247,296.00 | 241.50 | 0.00 |
| 1024 | tidehunter_efa_native | 2 | 1,483.523 | 5,877.030 | 8,204.16 | 262,533.20 | 256.38 | 0.00 |
| 1024 | tikv | 2 | 7,317.433 | 11,281.171 | 2,077.05 | 66,465.54 | 64.91 | 0.00 |
| 1024 | cassandra | 2 | 2,649.260 | 5,365.435 | 5,311.80 | 169,977.64 | 165.99 | 0.00 |
| 1024 | foundationdb | 2 | 10,079.563 | 11,412.493 | 1,543.46 | 49,390.66 | 48.23 | 0.00 |
| 8192 | native | 2 | 124.040 | 262.786 | 45,072.92 | 1,442,333.46 | 11,268.23 | 0.00 |
| 8192 | tidehunter_tcp | 2 | 4,327.551 | 5,746.065 | 3,393.46 | 108,590.57 | 848.36 | 0.00 |
| 8192 | tidehunter_efa | 2 | 3,786.900 | 5,329.494 | 3,801.41 | 121,645.04 | 950.35 | 0.00 |
| 8192 | tidehunter_efa_direct | 2 | 4,234.787 | 5,803.935 | 3,439.09 | 110,050.90 | 859.77 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 3,001.059 | 5,743.020 | 4,560.25 | 145,928.04 | 1,140.06 | 0.00 |
| 8192 | tikv | 2 | 7,922.897 | 12,456.171 | 1,875.85 | 60,027.06 | 468.96 | 0.00 |
| 8192 | cassandra | 2 | 4,511.232 | 23,954.277 | 2,185.57 | 69,938.21 | 546.39 | 0.00 |
| 8192 | foundationdb | 2 | 12,822.365 | 15,508.321 | 1,205.54 | 38,577.28 | 301.39 | 0.00 |
| 65536 | native | 2 | 1,942.886 | 75,917.655 | 887.53 | 28,401.12 | 1,775.07 | 0.00 |
| 65536 | tidehunter_tcp | 2 | 19,746.573 | 46,959.467 | 591.84 | 18,938.94 | 1,183.68 | 0.00 |
| 65536 | tidehunter_efa | 2 | 17,322.677 | 40,565.643 | 675.04 | 21,601.21 | 1,350.08 | 0.00 |
| 65536 | tidehunter_efa_direct | 2 | 19,458.578 | 48,455.908 | 593.72 | 18,999.09 | 1,187.44 | 0.00 |
| 65536 | tidehunter_efa_native | 2 | 12,705.239 | 30,821.969 | 909.32 | 29,098.33 | 1,818.65 | 0.00 |
| 65536 | tikv | 2 | 16,839.692 | 29,817.825 | 825.19 | 26,405.96 | 1,650.37 | 0.00 |
| 65536 | cassandra | 2 | 9,749.599 | 30,722.772 | 1,235.28 | 39,528.98 | 2,470.56 | 0.00 |
| 65536 | foundationdb | 2 | 24,920.352 | 49,710.932 | 548.86 | 17,563.51 | 1,097.72 | 0.00 |

## Point-mixed c1

Primary RF1, concurrency 1, uniform distribution, 4,096 keys, 8,192 measurement calls and 512 warmup calls per repetition. Configured mix: read 45%, put 45%, delete 5%, missing_read 5%. **N=2 per row and per operation-specific p50/p99 pair**. `missing_read/success` denotes the operation’s successful missing-key check. Case rates include the whole mixed workload; they are **not read-only throughput**.

| Value B | Backend | N | read p50/p99 µs | put p50/p99 µs | delete p50/p99 µs | missing_read p50/p99 µs | Mixed case calls/s | Mixed case logical keys/s | Mixed case value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | native | 2 | 2.345 / 7.696 | 177.329 / 416.933 | 175.199 / 414.663 | 1.135 / 3.476 | 6,395.64 | 6,395.64 | 0.34 | 0.00 |
| 64 | tidehunter_tcp | 2 | 238.634 / 408.042 | 546.697 / 741.673 | 542.892 / 709.245 | 1.125 / 4.796 | 1,938.83 | 1,938.83 | 0.10 | 0.00 |
| 64 | tidehunter_efa | 2 | 88.087 / 196.025 | 321.540 / 1,264.534 | 320.034 / 571.165 | 1.020 / 3.541 | 2,733.84 | 2,733.84 | 0.15 | 0.00 |
| 64 | tidehunter_efa_direct | 2 | 84.481 / 372.819 | 315.916 / 2,098.870 | 313.964 / 533.077 | 0.990 / 2.600 | 2,706.36 | 2,706.36 | 0.15 | 0.00 |
| 64 | tidehunter_efa_native | 2 | 83.261 / 1,279.108 | 319.815 / 1,358.702 | 318.255 / 1,889.184 | 1.071 / 4.450 | 2,661.25 | 2,661.25 | 0.14 | 0.00 |
| 64 | tikv | 2 | 414.127 / 618.974 | 764.340 / 1,155.916 | 772.666 / 1,172.298 | 414.848 / 627.635 | 1,547.25 | 1,547.25 | 0.08 | 0.00 |
| 64 | cassandra | 2 | 341.583 / 477.230 | 476.194 / 854.321 | 474.839 / 783.084 | 332.877 / 467.078 | 2,142.06 | 2,142.06 | 0.12 | 0.00 |
| 64 | foundationdb | 2 | 666.713 / 5,005.934 | 1,114.024 / 1,802.644 | 1,112.004 / 1,809.354 | 686.880 / 4,858.812 | 720.54 | 720.54 | 0.04 | 0.00 |
| 256 | native | 2 | 1.970 / 3.601 | 166.382 / 318.681 | 166.974 / 316.445 | 1.040 / 1.905 | 8,091.52 | 8,091.52 | 1.74 | 0.00 |
| 256 | tidehunter_tcp | 2 | 245.014 / 413.714 | 592.927 / 771.101 | 586.457 / 735.858 | 1.090 / 3.180 | 1,869.89 | 1,869.89 | 0.40 | 0.00 |
| 256 | tidehunter_efa | 2 | 88.248 / 682.474 | 356.073 / 2,568.934 | 353.948 / 1,784.062 | 1.035 / 3.070 | 2,549.31 | 2,549.31 | 0.55 | 0.00 |
| 256 | tidehunter_efa_direct | 2 | 89.486 / 1,564.806 | 372.469 / 1,658.753 | 368.154 / 510.090 | 1.111 / 3.561 | 2,462.04 | 2,462.04 | 0.53 | 0.00 |
| 256 | tidehunter_efa_native | 2 | 82.471 / 841.139 | 347.252 / 1,526.741 | 344.237 / 1,841.317 | 1.030 / 2.460 | 2,595.93 | 2,595.93 | 0.56 | 0.00 |
| 256 | tikv | 2 | 422.853 / 624.186 | 787.727 / 1,219.624 | 784.455 / 1,137.263 | 437.641 / 639.422 | 1,520.85 | 1,520.85 | 0.33 | 0.00 |
| 256 | cassandra | 2 | 336.306 / 471.327 | 452.716 / 884.520 | 447.795 / 652.242 | 329.066 / 468.462 | 2,187.22 | 2,187.22 | 0.47 | 0.00 |
| 256 | foundationdb | 2 | 725.835 / 5,153.627 | 1,002.261 / 1,583.109 | 1,002.567 / 1,558.675 | 709.703 / 4,946.227 | 719.87 | 719.87 | 0.16 | 0.00 |
| 1024 | native | 2 | 2.390 / 7.720 | 173.539 / 444.175 | 172.154 / 413.962 | 1.155 / 3.790 | 7,256.04 | 7,256.04 | 6.26 | 0.00 |
| 1024 | tidehunter_tcp | 2 | 245.029 / 417.369 | 602.803 / 1,004.361 | 584.822 / 783.703 | 1.230 / 7.210 | 1,811.66 | 1,811.66 | 1.56 | 0.00 |
| 1024 | tidehunter_efa | 2 | 96.193 / 488.220 | 368.044 / 2,005.950 | 355.564 / 1,113.387 | 1.210 / 4.295 | 2,473.01 | 2,473.01 | 2.13 | 0.00 |
| 1024 | tidehunter_efa_direct | 2 | 86.422 / 1,639.982 | 364.793 / 1,237.991 | 352.972 / 496.088 | 1.071 / 2.730 | 2,516.26 | 2,516.26 | 2.17 | 0.00 |
| 1024 | tidehunter_efa_native | 2 | 82.962 / 217.308 | 352.304 / 990.960 | 341.953 / 1,612.765 | 0.975 / 2.850 | 2,611.32 | 2,611.32 | 2.25 | 0.00 |
| 1024 | tikv | 2 | 412.378 / 623.564 | 833.000 / 1,307.604 | 777.390 / 1,187.823 | 419.493 / 632.885 | 1,442.63 | 1,442.63 | 1.24 | 0.00 |
| 1024 | cassandra | 2 | 351.893 / 494.713 | 503.745 / 959.584 | 458.287 / 812.745 | 341.942 / 500.024 | 1,986.13 | 1,986.13 | 1.71 | 0.00 |
| 1024 | foundationdb | 2 | 743.108 / 5,126.033 | 1,011.469 / 1,593.652 | 1,008.809 / 1,601.758 | 718.955 / 5,108.426 | 718.73 | 718.73 | 0.62 | 0.00 |
| 8192 | native | 2 | 4.505 / 13.491 | 197.690 / 496.920 | 179.635 / 380.940 | 1.131 / 3.005 | 6,530.59 | 6,530.59 | 45.07 | 0.00 |
| 8192 | tidehunter_tcp | 2 | 267.486 / 444.895 | 697.044 / 1,166.057 | 578.946 / 738.278 | 1.251 / 4.935 | 1,649.06 | 1,649.06 | 11.38 | 0.00 |
| 8192 | tidehunter_efa | 2 | 104.068 / 1,723.572 | 434.090 / 2,442.709 | 356.213 / 1,520.721 | 1.145 / 4.745 | 2,158.25 | 2,158.25 | 14.89 | 0.00 |
| 8192 | tidehunter_efa_direct | 2 | 100.297 / 1,210.734 | 421.332 / 2,571.290 | 344.716 / 1,372.157 | 0.985 / 5.666 | 2,242.54 | 2,242.54 | 15.48 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 90.267 / 877.910 | 417.043 / 2,364.724 | 347.418 / 541.078 | 0.974 / 6.886 | 2,274.93 | 2,274.93 | 15.70 | 0.00 |
| 8192 | tikv | 2 | 459.521 / 680.233 | 1,179.977 / 1,440.233 | 807.808 / 1,213.911 | 441.545 / 649.131 | 1,164.46 | 1,164.46 | 8.04 | 0.00 |
| 8192 | cassandra | 2 | 357.878 / 512.105 | 706.575 / 1,047.953 | 443.170 / 732.218 | 332.005 / 459.341 | 1,699.97 | 1,699.97 | 11.73 | 0.00 |
| 8192 | foundationdb | 2 | 711.101 / 5,107.695 | 1,071.030 / 1,740.024 | 1,038.803 / 1,673.747 | 696.545 / 5,016.302 | 716.55 | 716.55 | 4.94 | 0.00 |
| 65536 | native | 2 | 15.627 / 39.033 | 541.594 / 1,003.777 | 340.508 / 629.481 | 1.325 / 4.131 | 2,917.60 | 2,917.60 | 161.05 | 0.00 |
| 65536 | tidehunter_tcp | 2 | 401.207 / 616.139 | 1,421.263 / 1,916.082 | 600.831 / 753.312 | 1.415 / 3.651 | 998.32 | 998.32 | 55.11 | 0.00 |
| 65536 | tidehunter_efa | 2 | 165.899 / 1,556.504 | 1,010.325 / 4,607.467 | 366.325 / 1,800.665 | 1.180 / 3.295 | 1,338.02 | 1,338.02 | 73.87 | 0.00 |
| 65536 | tidehunter_efa_direct | 2 | 210.017 / 2,037.640 | 1,053.447 / 4,931.343 | 363.332 / 2,976.664 | 1.220 / 5.636 | 1,249.55 | 1,249.55 | 68.98 | 0.00 |
| 65536 | tidehunter_efa_native | 2 | 188.895 / 1,275.481 | 1,038.747 / 4,967.124 | 352.363 / 2,033.282 | 1.145 / 2.585 | 1,273.30 | 1,273.30 | 70.30 | 0.00 |
| 65536 | tikv | 2 | 584.321 / 935.284 | 1,470.432 / 1,899.174 | 801.789 / 1,155.696 | 443.655 / 648.581 | 934.86 | 934.86 | 51.62 | 0.00 |
| 65536 | cassandra | 2 | 465.302 / 708.520 | 1,063.484 / 1,477.467 | 490.729 / 866.178 | 344.117 / 499.590 | 1,186.50 | 1,186.50 | 65.50 | 0.00 |
| 65536 | foundationdb | 2 | 820.331 / 5,266.467 | 1,115.685 / 1,937.930 | 981.433 / 1,661.758 | 696.566 / 5,127.331 | 679.88 | 679.88 | 37.53 | 0.00 |

## Offered-rate profiles

Primary RF1, 1,024 B values, uniform distribution, 4,096 keys, concurrency 32, configured mix read 70% / put 30%, eight-second offered interval, 512 warmup calls. Offered 1,000 / 10,000 / 50,000 API calls/s corresponds to 8,000 / 80,000 / 400,000 planned arrivals per repetition. Rates below are delivered successful case-level goodput over each observed raw span; they include both operations, not read-only throughput. All observed unsuccessful calls in admitted metrics have outcome `rejected`; no other error outcome was recorded.

| Offered calls/s | Backend | N | Delivered successful calls/s | Logical keys/s | Value MiB/s | Successful calls r0/r1 | Unsuccessful calls median | Rejected/error calls r0/r1 | Elapsed s median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | native | 2 | 999.97 | 999.97 | 0.98 | 8,000 / 8,000 | 0.00 | 0 / 0 | 8.000258 |
| 1000 | tidehunter_tcp | 2 | 999.60 | 999.60 | 0.98 | 8,000 / 7,994 | 3.00 | 0 / 6 | 8.000205 |
| 1000 | tidehunter_efa | 2 | 994.90 | 994.90 | 0.97 | 7,963 / 7,955 | 41.00 | 37 / 45 | 7.999765 |
| 1000 | tidehunter_efa_direct | 2 | 994.53 | 994.53 | 0.97 | 7,952 / 7,960 | 44.00 | 48 / 40 | 7.999770 |
| 1000 | tidehunter_efa_native | 2 | 994.18 | 994.18 | 0.97 | 7,960 / 7,947 | 46.50 | 40 / 53 | 8.000092 |
| 1000 | tikv | 2 | 999.98 | 999.98 | 0.98 | 8,000 / 8,000 | 0.00 | 0 / 0 | 8.000143 |
| 1000 | cassandra | 2 | 999.94 | 999.94 | 0.98 | 8,000 / 8,000 | 0.00 | 0 / 0 | 8.000477 |
| 1000 | foundationdb | 2 | 999.88 | 999.88 | 0.98 | 8,000 / 8,000 | 0.00 | 0 / 0 | 8.000970 |
| 10000 | native | 2 | 9,999.36 | 9,999.36 | 9.77 | 80,000 / 80,000 | 0.00 | 0 / 0 | 8.000512 |
| 10000 | tidehunter_tcp | 2 | 7,136.90 | 7,136.90 | 6.97 | 56,986 / 57,227 | 22,893.50 | 23,014 / 22,773 | 8.001586 |
| 10000 | tidehunter_efa | 2 | 6,660.94 | 6,660.94 | 6.50 | 53,950 / 54,516 | 25,767.00 | 26,050 / 25,484 | 8.143023 |
| 10000 | tidehunter_efa_direct | 2 | 6,676.92 | 6,676.92 | 6.52 | 53,851 / 54,470 | 25,839.50 | 26,149 / 25,530 | 8.112139 |
| 10000 | tidehunter_efa_native | 2 | 6,731.20 | 6,731.20 | 6.57 | 53,800 / 53,926 | 26,137.00 | 26,200 / 26,074 | 8.001991 |
| 10000 | tikv | 2 | 9,997.44 | 9,997.44 | 9.76 | 80,000 / 80,000 | 0.00 | 0 / 0 | 8.002051 |
| 10000 | cassandra | 2 | 9,997.61 | 9,997.61 | 9.76 | 80,000 / 80,000 | 0.00 | 0 / 0 | 8.001916 |
| 10000 | foundationdb | 2 | 9,994.59 | 9,994.59 | 9.76 | 80,000 / 80,000 | 0.00 | 0 / 0 | 8.004333 |
| 50000 | native | 2 | 38,079.34 | 38,079.34 | 37.19 | 303,018 / 306,401 | 95,290.50 | 96,982 / 93,599 | 8.001962 |
| 50000 | tidehunter_tcp | 2 | 10,894.58 | 10,894.58 | 10.64 | 89,554 / 87,456 | 311,495.00 | 310,446 / 312,544 | 8.123914 |
| 50000 | tidehunter_efa | 2 | 10,702.79 | 10,702.79 | 10.45 | 86,887 / 88,685 | 312,214.00 | 313,113 / 311,315 | 8.204077 |
| 50000 | tidehunter_efa_direct | 2 | 11,063.35 | 11,063.35 | 10.80 | 88,791 / 90,865 | 310,172.00 | 311,209 / 309,135 | 8.119533 |
| 50000 | tidehunter_efa_native | 2 | 10,985.20 | 10,985.20 | 10.73 | 89,608 / 88,576 | 310,908.00 | 310,392 / 311,424 | 8.110302 |
| 50000 | tikv | 2 | 32,746.66 | 32,746.66 | 31.98 | 259,113 / 265,107 | 137,890.00 | 140,887 / 134,893 | 8.004174 |
| 50000 | cassandra | 2 | 46,854.12 | 46,854.12 | 45.76 | 387,508 / 362,425 | 25,033.50 | 12,492 / 37,575 | 8.002869 |
| 50000 | foundationdb | 2 | 28,941.57 | 28,941.57 | 28.26 | 231,943 / 231,736 | 168,160.50 | 168,057 / 168,264 | 8.010604 |

### Successful operation-specific API, response and scheduling tails

All cells are medians of the r0/r1 statistics for the named `success` population, **N=2**. Response and scheduling p99s are not an all-arrival or pooled quantile. Rejected populations are retained separately immediately below.

| Offered calls/s | Backend | Operation/outcome=success | N | API p50/p99 µs | Response p99 µs | Scheduling p99 µs | Population calls r0/r1 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | native | read | 2 | 6.521 / 25.692 | 1,665.910 | 1,657.988 | 5,572 / 5,548 |
| 1000 | native | put | 2 | 425.304 / 607.684 | 2,168.983 | 1,659.136 | 2,428 / 2,452 |
| 1000 | tidehunter_tcp | read | 2 | 272.697 / 491.748 | 85,345.019 | 84,986.966 | 5,572 / 5,545 |
| 1000 | tidehunter_tcp | put | 2 | 720.397 / 134,021.136 | 139,715.851 | 74,463.172 | 2,428 / 2,449 |
| 1000 | tidehunter_efa | read | 2 | 119.454 / 377.600 | 111,316.330 | 111,126.406 | 5,544 / 5,520 |
| 1000 | tidehunter_efa | put | 2 | 493.834 / 158,276.573 | 167,704.845 | 105,070.936 | 2,419 / 2,435 |
| 1000 | tidehunter_efa_direct | read | 2 | 111.554 / 435.318 | 116,097.256 | 113,436.942 | 5,534 / 5,523 |
| 1000 | tidehunter_efa_direct | put | 2 | 476.157 / 159,738.223 | 168,857.350 | 109,617.359 | 2,418 / 2,437 |
| 1000 | tidehunter_efa_native | read | 2 | 102.873 / 543.317 | 113,742.796 | 113,565.252 | 5,539 / 5,512 |
| 1000 | tidehunter_efa_native | put | 2 | 474.048 / 161,252.486 | 169,792.891 | 113,407.591 | 2,421 / 2,435 |
| 1000 | tikv | read | 2 | 460.871 / 652.775 | 2,272.724 | 1,712.851 | 5,572 / 5,548 |
| 1000 | tikv | put | 2 | 902.019 / 1,334.639 | 2,867.935 | 1,708.519 | 2,428 / 2,452 |
| 1000 | cassandra | read | 2 | 392.618 / 548.565 | 2,529.450 | 2,090.336 | 5,572 / 5,548 |
| 1000 | cassandra | put | 2 | 558.270 / 1,085.544 | 2,946.913 | 2,090.665 | 2,428 / 2,452 |
| 1000 | foundationdb | read | 2 | 603.563 / 4,884.995 | 6,023.622 | 1,553.443 | 5,572 / 5,548 |
| 1000 | foundationdb | put | 2 | 1,192.674 / 1,927.452 | 3,238.081 | 1,550.075 | 2,428 / 2,452 |
| 10000 | native | read | 2 | 4.991 / 19.651 | 1,997.135 | 1,989.878 | 55,803 / 55,963 |
| 10000 | native | put | 2 | 431.255 / 992.443 | 2,640.836 | 1,992.268 | 24,197 / 24,037 |
| 10000 | tidehunter_tcp | read | 2 | 250.745 / 734.817 | 168,553.411 | 168,194.158 | 39,720 / 39,943 |
| 10000 | tidehunter_tcp | put | 2 | 821.041 / 171,190.421 | 177,489.636 | 168,009.005 | 17,266 / 17,284 |
| 10000 | tidehunter_efa | read | 2 | 126.845 / 3,911.572 | 204,222.196 | 203,903.226 | 37,605 / 38,130 |
| 10000 | tidehunter_efa | put | 2 | 573.904 / 206,284.237 | 215,493.112 | 203,308.607 | 16,345 / 16,386 |
| 10000 | tidehunter_efa_direct | read | 2 | 118.919 / 3,989.717 | 202,770.178 | 202,539.322 | 37,473 / 38,085 |
| 10000 | tidehunter_efa_direct | put | 2 | 579.303 / 204,835.297 | 216,120.981 | 201,650.247 | 16,378 / 16,385 |
| 10000 | tidehunter_efa_native | read | 2 | 108.733 / 3,694.218 | 203,809.081 | 203,556.007 | 37,562 / 37,642 |
| 10000 | tidehunter_efa_native | put | 2 | 581.337 / 206,433.894 | 213,996.427 | 203,118.117 | 16,238 / 16,284 |
| 10000 | tikv | read | 2 | 427.894 / 907.936 | 2,534.877 | 1,976.564 | 55,803 / 55,963 |
| 10000 | tikv | put | 2 | 802.895 / 1,478.491 | 3,030.412 | 1,974.630 | 24,197 / 24,037 |
| 10000 | cassandra | read | 2 | 295.567 / 468.878 | 2,260.727 | 1,905.092 | 55,803 / 55,963 |
| 10000 | cassandra | put | 2 | 463.168 / 882.757 | 2,563.053 | 1,904.499 | 24,197 / 24,037 |
| 10000 | foundationdb | read | 2 | 693.115 / 4,451.796 | 5,703.904 | 2,330.299 | 55,803 / 55,963 |
| 10000 | foundationdb | put | 2 | 1,049.967 / 1,831.284 | 3,648.755 | 2,367.820 | 24,197 / 24,037 |
| 50000 | native | read | 2 | 3.365 / 14.271 | 11,978.095 | 11,974.113 | 212,076 / 214,513 |
| 50000 | native | put | 2 | 1,311.960 / 8,088.262 | 15,516.356 | 11,912.743 | 90,942 / 91,888 |
| 50000 | tidehunter_tcp | read | 2 | 223.238 / 1,048.647 | 179,146.158 | 178,696.709 | 62,862 / 61,280 |
| 50000 | tidehunter_tcp | put | 2 | 1,044.122 / 176,624.090 | 187,707.685 | 179,745.605 | 26,692 / 26,176 |
| 50000 | tidehunter_efa | read | 2 | 126.815 / 4,775.262 | 213,501.829 | 213,275.358 | 60,736 / 62,165 |
| 50000 | tidehunter_efa | put | 2 | 1,002.544 / 211,928.062 | 226,004.440 | 213,678.566 | 26,151 / 26,520 |
| 50000 | tidehunter_efa_direct | read | 2 | 122.309 / 1,144.822 | 215,411.748 | 215,056.075 | 62,088 / 63,899 |
| 50000 | tidehunter_efa_direct | put | 2 | 984.701 / 212,652.295 | 233,402.856 | 217,155.522 | 26,703 / 26,966 |
| 50000 | tidehunter_efa_native | read | 2 | 116.744 / 4,000.332 | 215,315.083 | 215,094.478 | 62,692 / 62,068 |
| 50000 | tidehunter_efa_native | put | 2 | 1,006.251 / 212,708.983 | 230,659.230 | 216,550.362 | 26,916 / 26,508 |
| 50000 | tikv | read | 2 | 803.487 / 1,584.462 | 11,915.689 | 10,733.644 | 181,230 / 185,859 |
| 50000 | tikv | put | 2 | 1,116.644 / 5,135.256 | 12,360.442 | 10,680.151 | 77,883 / 79,248 |
| 50000 | cassandra | read | 2 | 270.976 / 460.015 | 7,650.934 | 6,599.257 | 271,120 / 253,994 |
| 50000 | cassandra | put | 2 | 526.967 / 1,233.671 | 8,466.173 | 6,624.075 | 116,388 / 108,431 |
| 50000 | foundationdb | read | 2 | 866.828 / 2,194.093 | 8,591.371 | 7,407.008 | 162,261 / 162,424 |
| 50000 | foundationdb | put | 2 | 1,405.581 / 2,236.476 | 9,117.855 | 7,376.924 | 69,682 / 69,312 |

### Rejected-arrival populations (exact per-repetition counts and tails)

Only observed rejected populations appear. `—` means no rejected population in that repetition, not a zero-time rejection. r0/r1 tail pairs below are **unaggregated**; a matched median is shown only when both repetitions have that population (N=2). TCP at offered 1,000 has rejected populations only in r1, so no matched rejection-tail aggregate is supplied. Rejected calls have no timed API latency in the source. Every rejected scheduling population has sample_count=0 and null scheduling quantiles in the source; scheduling cells therefore remain unavailable even when the rejected population is matched.

| Offered calls/s | Backend | Operation/outcome=rejected | Count r0/r1 | Response p99 µs r0/r1 | Scheduling p99 µs r0/r1 | Matched N | Median response p99 µs | Median scheduling p99 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | tidehunter_tcp | read | — / 3 | — / 1,034.037 | — / — | unpaired | — | — |
| 1000 | tidehunter_tcp | put | — / 3 | — / 841.289 | — / — | unpaired | — | — |
| 1000 | tidehunter_efa | read | 28 / 28 | 1,621.952 / 1,279.907 | — / — | 2 | 1,450.929 | — |
| 1000 | tidehunter_efa | put | 9 / 17 | 1,372.190 / 1,321.641 | — / — | 2 | 1,346.916 | — |
| 1000 | tidehunter_efa_direct | read | 38 / 25 | 1,745.220 / 910.166 | — / — | 2 | 1,327.693 | — |
| 1000 | tidehunter_efa_direct | put | 10 / 15 | 1,698.139 / 1,087.114 | — / — | 2 | 1,392.626 | — |
| 1000 | tidehunter_efa_native | read | 33 / 36 | 1,743.116 / 1,624.537 | — / — | 2 | 1,683.827 | — |
| 1000 | tidehunter_efa_native | put | 7 / 17 | 1,712.883 / 1,655.353 | — / — | 2 | 1,684.118 | — |
| 10000 | tidehunter_tcp | read | 16,083 / 16,020 | 1,974.677 / 1,938.476 | — / — | 2 | 1,956.577 | — |
| 10000 | tidehunter_tcp | put | 6,931 / 6,753 | 1,965.624 / 1,927.258 | — / — | 2 | 1,946.441 | — |
| 10000 | tidehunter_efa | read | 18,198 / 17,833 | 1,913.028 / 1,947.957 | — / — | 2 | 1,930.493 | — |
| 10000 | tidehunter_efa | put | 7,852 / 7,651 | 1,911.133 / 1,946.261 | — / — | 2 | 1,928.697 | — |
| 10000 | tidehunter_efa_direct | read | 18,330 / 17,878 | 1,919.518 / 1,951.273 | — / — | 2 | 1,935.396 | — |
| 10000 | tidehunter_efa_direct | put | 7,819 / 7,652 | 1,921.887 / 1,943.464 | — / — | 2 | 1,932.675 | — |
| 10000 | tidehunter_efa_native | read | 18,241 / 18,321 | 1,937.629 / 1,946.843 | — / — | 2 | 1,942.236 | — |
| 10000 | tidehunter_efa_native | put | 7,959 / 7,753 | 1,933.929 / 1,953.978 | — / — | 2 | 1,943.954 | — |
| 50000 | native | read | 67,807 / 65,723 | 1,674.465 / 1,696.155 | — / — | 2 | 1,685.310 | — |
| 50000 | native | put | 29,175 / 27,876 | 1,684.027 / 1,677.923 | — / — | 2 | 1,680.975 | — |
| 50000 | tidehunter_tcp | read | 217,021 / 218,956 | 56,846.805 / 57,607.398 | — / — | 2 | 57,227.102 | — |
| 50000 | tidehunter_tcp | put | 93,425 / 93,588 | 56,696.836 / 57,684.104 | — / — | 2 | 57,190.470 | — |
| 50000 | tidehunter_efa | read | 219,147 / 218,071 | 39,812.967 / 39,431.026 | — / — | 2 | 39,621.996 | — |
| 50000 | tidehunter_efa | put | 93,966 / 93,244 | 39,939.000 / 39,392.171 | — / — | 2 | 39,665.586 | — |
| 50000 | tidehunter_efa_direct | read | 217,795 / 216,337 | 41,236.030 / 41,524.034 | — / — | 2 | 41,380.032 | — |
| 50000 | tidehunter_efa_direct | put | 93,414 / 92,798 | 41,386.136 / 41,608.021 | — / — | 2 | 41,497.079 | — |
| 50000 | tidehunter_efa_native | read | 217,191 / 218,168 | 38,720.170 / 39,447.101 | — / — | 2 | 39,083.636 | — |
| 50000 | tidehunter_efa_native | put | 93,201 / 93,256 | 39,051.307 / 39,580.667 | — / — | 2 | 39,315.987 | — |
| 50000 | tikv | read | 98,653 / 94,377 | 7,895.507 / 8,045.621 | — / — | 2 | 7,970.564 | — |
| 50000 | tikv | put | 42,234 / 40,516 | 7,805.608 / 8,041.152 | — / — | 2 | 7,923.380 | — |
| 50000 | cassandra | read | 8,763 / 26,242 | 1,965.059 / 1,922.205 | — / — | 2 | 1,943.632 | — |
| 50000 | cassandra | put | 3,729 / 11,333 | 1,960.949 / 1,927.013 | — / — | 2 | 1,943.981 | — |
| 50000 | foundationdb | read | 117,622 / 117,812 | 1,462.913 / 1,476.794 | — / — | 2 | 1,469.854 | — |
| 50000 | foundationdb | put | 50,435 / 50,452 | 1,468.116 / 1,475.697 | — / — | 2 | 1,471.907 | — |

## Zipf read

`zipf-read`, primary RF1, exponent 0.99, 1,024 B values, 4,096 configured keys, concurrency 16, 100% read, 8,192 measurement calls and 512 warmup calls per repetition. Matched r0/r1, **N=2**.

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1024 | native | 2 | 1.825 | 3.155 | 121,518.19 | 121,518.19 | 118.67 | 0.00 |
| 1024 | tidehunter_tcp | 2 | 177.663 | 641.365 | 30,734.51 | 30,734.51 | 30.01 | 0.00 |
| 1024 | tidehunter_efa | 2 | 87.337 | 315.665 | 59,558.72 | 59,558.72 | 58.16 | 0.00 |
| 1024 | tidehunter_efa_direct | 2 | 83.846 | 310.249 | 78,315.98 | 78,315.98 | 76.48 | 0.00 |
| 1024 | tidehunter_efa_native | 2 | 80.267 | 292.548 | 81,590.39 | 81,590.39 | 79.68 | 0.00 |
| 1024 | tikv | 2 | 395.362 | 700.471 | 33,810.06 | 33,810.06 | 33.02 | 0.00 |
| 1024 | cassandra | 2 | 335.528 | 634.196 | 37,519.53 | 37,519.53 | 36.64 | 0.00 |
| 1024 | foundationdb | 2 | 643.755 | 1,050.963 | 22,508.91 | 22,508.91 | 21.98 | 0.00 |

## Configured cache and registered-RAM extras

These are **configured** cohorts, not renamed primary results and not a claim that every request hit a cache or registered-RAM path. All use uniform 100% read, concurrency 1, 4,096 keys, 8,192 measurement calls and 1,024 warmup calls. Values 256 B and 8,192 B; matched r0/r1, **N=2**. Unlisted backend/profile/size combinations were not planned.

| Profile | Configured cohort | Planned backends | Value B | Coordinator cache bytes | Owner read-cache bytes |
| --- | --- | --- | --- | --- | --- |
| coordinator-cache | coordinator_cache_configured | tidehunter_tcp, tidehunter_efa, tidehunter_efa_direct, tidehunter_efa_native | 256, 8192 | 134217728 | 0 |
| owner-cache | owner_cache_configured | tidehunter_tcp, tidehunter_efa, tidehunter_efa_direct, tidehunter_efa_native | 256, 8192 | 0 | 134217728 |
| registered-ram | registered_ram_configured | tidehunter_efa_native | 256, 8192 | 0 | 0 |

Registered-RAM job configuration: per owner `source_bytes=16777216`, `source_regions=4096`, `destination_bytes=20971520`, `destination_slots=640`, `max_handles=4096`, `max_read_bytes=32768`, `require_device_read=true`. Source backend configuration also records `read_resident_first=true`. This section labels the configuration only; it does not convert cumulative counters to measurement-interval hit rates.

### coordinator-cache — configured

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 256 | tidehunter_tcp | 2 | 0.760 | 1.740 | 33,718.32 | 33,718.32 | 8.23 | 0.00 |
| 256 | tidehunter_efa | 2 | 0.725 | 1.730 | 33,491.70 | 33,491.70 | 8.18 | 0.00 |
| 256 | tidehunter_efa_direct | 2 | 0.735 | 1.720 | 33,150.26 | 33,150.26 | 8.09 | 0.00 |
| 256 | tidehunter_efa_native | 2 | 0.800 | 1.720 | 33,775.34 | 33,775.34 | 8.25 | 0.00 |
| 8192 | tidehunter_tcp | 2 | 0.825 | 1.821 | 30,618.02 | 30,618.02 | 239.20 | 0.00 |
| 8192 | tidehunter_efa | 2 | 0.945 | 1.790 | 29,722.45 | 29,722.45 | 232.21 | 0.00 |
| 8192 | tidehunter_efa_direct | 2 | 0.935 | 1.801 | 30,328.12 | 30,328.12 | 236.94 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 0.830 | 1.780 | 30,518.87 | 30,518.87 | 238.43 | 0.00 |

### owner-cache — configured

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 256 | tidehunter_tcp | 2 | 159.767 | 261.661 | 4,773.47 | 4,773.47 | 1.17 | 0.00 |
| 256 | tidehunter_efa | 2 | 51.389 | 88.011 | 11,058.15 | 11,058.15 | 2.70 | 0.00 |
| 256 | tidehunter_efa_direct | 2 | 49.859 | 85.547 | 11,396.67 | 11,396.67 | 2.78 | 0.00 |
| 256 | tidehunter_efa_native | 2 | 48.044 | 83.027 | 11,150.15 | 11,150.15 | 2.72 | 0.00 |
| 8192 | tidehunter_tcp | 2 | 180.029 | 315.455 | 4,014.01 | 4,014.01 | 31.36 | 0.00 |
| 8192 | tidehunter_efa | 2 | 61.550 | 107.909 | 9,031.48 | 9,031.48 | 70.56 | 0.00 |
| 8192 | tidehunter_efa_direct | 2 | 60.860 | 107.053 | 8,958.48 | 8,958.48 | 69.99 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 54.764 | 100.373 | 9,589.75 | 9,589.75 | 74.92 | 0.00 |

### registered-ram — configured

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 256 | tidehunter_efa_native | 2 | 51.949 | 418.518 | 7,218.28 | 7,218.28 | 1.76 | 0.00 |
| 8192 | tidehunter_efa_native | 2 | 58.164 | 431.953 | 6,519.79 | 6,519.79 | 50.94 | 0.00 |

## Cold-OS-cache cases

`cold-os-large` / `cold_os_cache`: uniform 100% read, 65,536 B values, 131,072 keys (8 GiB value payload), concurrency 16, zero warmup calls, 4,096 measurement calls. Seven backends completed matched r0/r1 (**N=2**); their evidence records `os_caches_dropped=true` and `engine_caches_evicted=false`. OS-cache dropping is **not isolated-NVMe measurement** and does not evict engine caches. **FoundationDB is unavailable**: r0/r1 failed before measurement gate release/cache drop; r2 failed at the global deadline. It has no comparable cold metrics.

**FoundationDB cold availability caveat:** the three linked control exports ([stop-3255](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/deployments/foundationdb-stop-3255/host--1-export.json), [stop-5645](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/deployments/foundationdb-stop-5645/host--1-export.json), [stop-7360](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/deployments/foundationdb-stop-7360/host--1-export.json)) establish early control-server OOM/service loss: each has `ERROR: Out of memory`, exit code 20, `Running=false`, and `OOMKilled=false`, approximately 5.5–7.1 s into its cold-preload attempt. Client preload timeouts/deadline followed; the precise internal allocation is not established. The original AWS configuration was 2 GiB control-service / 6 GiB client. Its cold entry remains unavailable, not an inherent engine-performance conclusion. Completed primary measurements above remain the actual pre-fix configured AWS results; the separately documented post-run local correction does not overwrite them.

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 65536 | native | 2 | 347.717 | 598.763 | 36,356.02 | 36,356.02 | 2,272.25 | 0.00 |
| 65536 | tidehunter_tcp | 2 | 1,332.012 | 53,858.026 | 3,746.32 | 3,746.32 | 234.14 | 0.00 |
| 65536 | tidehunter_efa | 2 | 1,092.761 | 49,425.346 | 4,236.97 | 4,236.97 | 264.81 | 0.00 |
| 65536 | tidehunter_efa_direct | 2 | 1,172.762 | 50,875.337 | 3,873.93 | 3,873.93 | 242.12 | 0.00 |
| 65536 | tidehunter_efa_native | 2 | 1,169.080 | 49,157.796 | 3,751.97 | 3,751.97 | 234.50 | 0.00 |
| 65536 | tikv | 2 | 800.638 | 1,917.182 | 15,498.16 | 15,498.16 | 968.63 | 0.00 |
| 65536 | cassandra | 2 | 1,034.552 | 1,535.642 | 14,255.40 | 14,255.40 | 890.96 | 0.00 |
| 65536 | foundationdb | unavailable | — | — | — | — | — | — |

## Native page-cache extra

Backend `native`, cohort `native_pagecache`, durability `page_cache`; separate from the primary `value_wal_barrier` cohort. All five sizes have matched r0/r1 (**N=2**); r2 was not run. Profile operation counts/concurrency are the same as the corresponding primary profiles. These tables are not merged into the RF1 primary tables.

### Native page-cache read-many32

| Value B | Backend | N repeats | API p50 µs | API p99 µs | Successful calls/s | Logical keys/s | Value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | native | 2 | 64.130 | 126.160 | 117,171.64 | 3,749,492.36 | 228.85 | 0.00 |
| 256 | native | 2 | 52.094 | 97.977 | 118,028.71 | 3,776,918.60 | 922.10 | 0.00 |
| 1024 | native | 2 | 65.450 | 111.703 | 112,137.78 | 3,588,408.95 | 3,504.31 | 0.00 |
| 8192 | native | 2 | 128.925 | 392.566 | 19,318.35 | 618,187.04 | 4,829.59 | 0.00 |
| 65536 | native | 2 | 1,896.311 | 75,614.109 | 907.58 | 29,042.52 | 1,815.16 | 0.00 |

### Native page-cache point-mixed c1

| Value B | Backend | N | read p50/p99 µs | put p50/p99 µs | delete p50/p99 µs | missing_read p50/p99 µs | Mixed case calls/s | Mixed case logical keys/s | Mixed case value MiB/s | Unsuccessful calls |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | native | 2 | 1.231 / 2.130 | 5.070 / 13.572 | 4.891 / 13.106 | 0.625 / 1.210 | 30,282.77 | 30,282.77 | 1.63 | 0.00 |
| 256 | native | 2 | 1.146 / 2.146 | 5.545 / 17.471 | 5.550 / 15.756 | 0.611 / 1.240 | 31,400.76 | 31,400.76 | 6.77 | 0.00 |
| 1024 | native | 2 | 1.465 / 4.450 | 5.266 / 26.692 | 4.845 / 12.951 | 0.685 / 1.190 | 28,556.24 | 28,556.24 | 24.63 | 0.00 |
| 8192 | native | 2 | 4.180 / 17.716 | 12.131 / 81.241 | 5.855 / 16.221 | 0.775 / 2.870 | 24,040.61 | 24,040.61 | 165.87 | 0.00 |
| 65536 | native | 2 | 14.575 / 49.874 | 72.611 / 716.556 | 5.731 / 20.527 | 1.060 / 4.285 | 8,834.82 | 8,834.82 | 487.71 | 0.00 |

## FoundationDB repetition 2 — separate raw results

The following **24 completed primary cases are repetition 2 only**, unaggregated (**N=1 per case**), and are excluded from every matched table above. They comprise the same 24 primary profile/size configurations. FoundationDB’s r2 cold case failed and has no metrics. All other backends’ r2 cases were not run. Case rates below are not per-operation throughput.

| Case prefix (r2) | Profile | Value B | N | Successful calls | Unsuccessful calls | Case successful calls/s | Case logical keys/s | Case value MiB/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0466 | read-concurrent | 64 | 1 | 8,192 | 0 | 24,411.70 | 24,411.70 | 1.49 |
| 0479 | read-concurrent | 256 | 1 | 8,192 | 0 | 23,865.23 | 23,865.23 | 5.83 |
| 0477 | read-concurrent | 1024 | 1 | 8,192 | 0 | 23,679.10 | 23,679.10 | 23.12 |
| 0457 | read-concurrent | 8192 | 1 | 8,192 | 0 | 18,779.62 | 18,779.62 | 146.72 |
| 0456 | read-concurrent | 65536 | 1 | 8,192 | 0 | 13,704.41 | 13,704.41 | 856.53 |
| 0471 | write-concurrent | 64 | 1 | 4,096 | 0 | 15,607.87 | 15,607.87 | 0.95 |
| 0461 | write-concurrent | 256 | 1 | 4,096 | 0 | 15,117.35 | 15,117.35 | 3.69 |
| 0467 | write-concurrent | 1024 | 1 | 4,096 | 0 | 15,607.02 | 15,607.02 | 15.24 |
| 0469 | write-concurrent | 8192 | 1 | 4,096 | 0 | 11,939.93 | 11,939.93 | 93.28 |
| 0473 | write-concurrent | 65536 | 1 | 4,096 | 0 | 4,719.86 | 4,719.86 | 294.99 |
| 0476 | read-many32 | 64 | 1 | 2,048 | 0 | 1,643.44 | 52,590.02 | 3.21 |
| 0478 | read-many32 | 256 | 1 | 2,048 | 0 | 1,655.88 | 52,988.30 | 12.94 |
| 0460 | read-many32 | 1024 | 1 | 2,048 | 0 | 1,578.64 | 50,516.62 | 49.33 |
| 0472 | read-many32 | 8192 | 1 | 2,048 | 0 | 1,294.77 | 41,432.49 | 323.69 |
| 0459 | read-many32 | 65536 | 1 | 2,048 | 0 | 641.95 | 20,542.28 | 1,283.89 |
| 0470 | point-mixed | 64 | 1 | 8,192 | 0 | 732.73 | 732.73 | 0.04 |
| 0462 | point-mixed | 256 | 1 | 8,192 | 0 | 734.20 | 734.20 | 0.16 |
| 0468 | point-mixed | 1024 | 1 | 8,192 | 0 | 733.51 | 733.51 | 0.63 |
| 0464 | point-mixed | 8192 | 1 | 8,192 | 0 | 721.62 | 721.62 | 4.96 |
| 0474 | point-mixed | 65536 | 1 | 8,192 | 0 | 671.03 | 671.03 | 36.89 |
| 0463 | offered-1000 | 1024 | 1 | 8,000 | 0 | 999.90 | 999.90 | 0.98 |
| 0458 | offered-10000 | 1024 | 1 | 80,000 | 0 | 9,993.50 | 9,993.50 | 9.76 |
| 0465 | offered-50000 | 1024 | 1 | 217,736 | 182,264 | 27,197.52 | 27,197.52 | 26.56 |
| 0475 | zipf-read | 1024 | 1 | 8,192 | 0 | 22,895.42 | 22,895.42 | 22.36 |

### Repetition-2 operation/outcome latency (unaggregated)

| Case prefix | Profile | Value B | Operation/outcome | N | Population calls | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0466 | read-concurrent | 64 | read/success | 1 | 8,192 | 599.008 | 833.685 | 889.612 | 187.075 |
| 0479 | read-concurrent | 256 | read/success | 1 | 8,192 | 613.630 | 859.300 | 908.635 | 197.686 |
| 0477 | read-concurrent | 1024 | read/success | 1 | 8,192 | 617.109 | 882.309 | 934.583 | 195.296 |
| 0457 | read-concurrent | 8192 | read/success | 1 | 8,192 | 799.334 | 1,208.736 | 1,226.837 | 38.023 |
| 0456 | read-concurrent | 65536 | read/success | 1 | 8,192 | 1,040.032 | 1,585.996 | 1,687.835 | 345.097 |
| 0471 | write-concurrent | 64 | put/success | 1 | 4,096 | 954.103 | 1,068.592 | 1,080.853 | 39.883 |
| 0461 | write-concurrent | 256 | put/success | 1 | 4,096 | 971.098 | 1,834.097 | 1,894.112 | 40.504 |
| 0467 | write-concurrent | 1024 | put/success | 1 | 4,096 | 948.093 | 1,359.251 | 1,382.862 | 37.892 |
| 0469 | write-concurrent | 8192 | put/success | 1 | 4,096 | 1,125.940 | 2,132.189 | 2,165.312 | 778.691 |
| 0473 | write-concurrent | 65536 | put/success | 1 | 4,096 | 3,185.276 | 5,207.771 | 6,444.757 | 2,779.274 |
| 0476 | read-many32 | 64 | get_many/success | 1 | 2,048 | 9,301.349 | 16,460.270 | 16,611.452 | 1,504.128 |
| 0478 | read-many32 | 256 | get_many/success | 1 | 2,048 | 9,324.480 | 15,613.314 | 15,793.489 | 1,227.950 |
| 0460 | read-many32 | 1024 | get_many/success | 1 | 2,048 | 9,865.800 | 11,441.176 | 11,964.069 | 1,452.957 |
| 0472 | read-many32 | 8192 | get_many/success | 1 | 2,048 | 11,926.341 | 14,004.081 | 14,700.934 | 1,786.307 |
| 0459 | read-many32 | 65536 | get_many/success | 1 | 2,048 | 21,642.112 | 30,473.489 | 45,787.205 | 24,068.894 |
| 0470 | point-mixed | 64 | delete/success | 1 | 427 | 1,015.399 | 1,557.641 | 1,599.089 | 30.383 |
| 0470 | point-mixed | 64 | missing_read/success | 1 | 394 | 613.375 | 4,969.691 | 4,981.302 | 36.113 |
| 0470 | point-mixed | 64 | put/success | 1 | 3,620 | 1,023.160 | 1,563.017 | 1,593.239 | 34.573 |
| 0470 | point-mixed | 64 | read/success | 1 | 3,751 | 648.841 | 4,998.013 | 5,007.944 | 33.503 |
| 0462 | point-mixed | 256 | delete/success | 1 | 427 | 1,100.640 | 1,769.113 | 1,795.475 | 30.533 |
| 0462 | point-mixed | 256 | missing_read/success | 1 | 394 | 566.705 | 4,952.630 | 4,967.111 | 36.743 |
| 0462 | point-mixed | 256 | put/success | 1 | 3,620 | 1,114.999 | 1,702.597 | 1,753.841 | 33.073 |
| 0462 | point-mixed | 256 | read/success | 1 | 3,751 | 583.437 | 4,938.229 | 4,952.019 | 32.692 |
| 0468 | point-mixed | 1024 | delete/success | 1 | 427 | 1,005.869 | 1,539.844 | 1,562.546 | 37.423 |
| 0468 | point-mixed | 1024 | missing_read/success | 1 | 394 | 628.520 | 5,111.562 | 5,126.223 | 41.294 |
| 0468 | point-mixed | 1024 | put/success | 1 | 3,620 | 1,008.260 | 1,564.067 | 1,599.121 | 39.233 |
| 0468 | point-mixed | 1024 | read/success | 1 | 3,751 | 650.005 | 5,128.604 | 5,141.304 | 40.823 |
| 0464 | point-mixed | 8192 | delete/success | 1 | 427 | 1,113.770 | 1,819.006 | 1,841.118 | 37.773 |
| 0464 | point-mixed | 8192 | missing_read/success | 1 | 394 | 560.565 | 4,817.168 | 4,829.838 | 35.353 |
| 0464 | point-mixed | 8192 | put/success | 1 | 3,620 | 1,148.682 | 1,839.288 | 1,875.331 | 45.773 |
| 0464 | point-mixed | 8192 | read/success | 1 | 3,751 | 640.281 | 4,976.730 | 4,996.402 | 49.464 |
| 0474 | point-mixed | 65536 | delete/success | 1 | 427 | 995.448 | 1,837.983 | 1,860.955 | 51.264 |
| 0474 | point-mixed | 65536 | missing_read/success | 1 | 394 | 658.931 | 5,316.855 | 5,338.767 | 41.693 |
| 0474 | point-mixed | 65536 | put/success | 1 | 3,620 | 1,143.089 | 1,947.012 | 2,033.339 | 112.470 |
| 0474 | point-mixed | 65536 | read/success | 1 | 3,751 | 793.272 | 5,149.932 | 5,194.776 | 112.768 |
| 0463 | offered-1000 | 1024 | put/success | 1 | 2,379 | 1,258.912 | 1,821.008 | 3,121.930 | 1,537.417 |
| 0463 | offered-1000 | 1024 | read/success | 1 | 5,621 | 577.166 | 4,773.137 | 5,927.902 | 1,538.551 |
| 0458 | offered-10000 | 1024 | put/success | 1 | 23,957 | 1,061.085 | 1,734.469 | 3,316.407 | 2,013.279 |
| 0458 | offered-10000 | 1024 | read/success | 1 | 56,043 | 582.167 | 2,189.286 | 3,506.592 | 2,017.221 |
| 0465 | offered-50000 | 1024 | put/rejected | 1 | 54,734 | — | — | 1,503.780 | — |
| 0465 | offered-50000 | 1024 | put/success | 1 | 65,420 | 1,680.295 | 2,382.202 | 10,171.349 | 8,263.400 |
| 0465 | offered-50000 | 1024 | read/rejected | 1 | 127,530 | — | — | 1,499.426 | — |
| 0465 | offered-50000 | 1024 | read/success | 1 | 152,316 | 850.828 | 3,159.594 | 9,555.136 | 8,287.250 |
| 0475 | zipf-read | 1024 | read/success | 1 | 8,192 | 626.929 | 969.336 | 1,151.821 | 371.069 |

## Complete planned-case coverage ledger

Every one of the **228 planned backend/cohort/profile/size configurations** appears below, with all three original case prefixes and statuses; this retains all **478 completed, 3 failed and 203 not_run** cases, including unmeasured repetition-2 configurations. `C` = completed with complete measurement rows; `F` = failed with no admitted metrics; `NR` = not_run. A prefix identifies the exact original case ID as `PREFIX-BACKEND-VALUEB-PROFILE-COHORT-rREPEAT`; those exact IDs and raw rows remain in the linked [CSV](all-results.csv), [JSON](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/comparison.json) and [original report](../../../../checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/comparison.md). This ledger does not assign performance values to F/NR cases.

| Backend | Cohort | Profile | Value B | r0 prefix/status | r1 prefix/status | r2 prefix/status |
| --- | --- | --- | --- | --- | --- | --- |
| native | primary_rf1 | read-concurrent | 64 | 0164 C | 0239 C | 0580 NR |
| tidehunter_tcp | primary_rf1 | read-concurrent | 64 | 0081 C | 0438 C | 0615 NR |
| tidehunter_efa | primary_rf1 | read-concurrent | 64 | 0052 C | 0384 C | 0491 NR |
| tidehunter_efa_direct | primary_rf1 | read-concurrent | 64 | 0110 C | 0330 C | 0520 NR |
| tidehunter_efa_native | primary_rf1 | read-concurrent | 64 | 0021 C | 0274 C | 0549 NR |
| tikv | primary_rf1 | read-concurrent | 64 | 0139 C | 0305 C | 0669 NR |
| cassandra | primary_rf1 | read-concurrent | 64 | 0224 C | 0413 C | 0644 NR |
| foundationdb | primary_rf1 | read-concurrent | 64 | 0199 C | 0359 C | 0466 C |
| native | primary_rf1 | read-concurrent | 256 | 0151 C | 0250 C | 0593 NR |
| tidehunter_tcp | primary_rf1 | read-concurrent | 256 | 0068 C | 0449 C | 0628 NR |
| tidehunter_efa | primary_rf1 | read-concurrent | 256 | 0039 C | 0395 C | 0504 NR |
| tidehunter_efa_direct | primary_rf1 | read-concurrent | 256 | 0097 C | 0341 C | 0533 NR |
| tidehunter_efa_native | primary_rf1 | read-concurrent | 256 | 0008 C | 0285 C | 0562 NR |
| tikv | primary_rf1 | read-concurrent | 256 | 0126 C | 0316 C | 0682 NR |
| cassandra | primary_rf1 | read-concurrent | 256 | 0211 C | 0424 C | 0657 NR |
| foundationdb | primary_rf1 | read-concurrent | 256 | 0186 C | 0370 C | 0479 C |
| native | primary_rf1 | read-concurrent | 1024 | 0156 C | 0233 C | 0591 NR |
| tidehunter_tcp | primary_rf1 | read-concurrent | 1024 | 0073 C | 0432 C | 0626 NR |
| tidehunter_efa | primary_rf1 | read-concurrent | 1024 | 0044 C | 0378 C | 0502 NR |
| tidehunter_efa_direct | primary_rf1 | read-concurrent | 1024 | 0102 C | 0324 C | 0531 NR |
| tidehunter_efa_native | primary_rf1 | read-concurrent | 1024 | 0013 C | 0268 C | 0560 NR |
| tikv | primary_rf1 | read-concurrent | 1024 | 0131 C | 0299 C | 0680 NR |
| cassandra | primary_rf1 | read-concurrent | 1024 | 0216 C | 0407 C | 0655 NR |
| foundationdb | primary_rf1 | read-concurrent | 1024 | 0191 C | 0353 C | 0477 C |
| native | primary_rf1 | read-concurrent | 8192 | 0153 C | 0229 C | 0571 NR |
| tidehunter_tcp | primary_rf1 | read-concurrent | 8192 | 0070 C | 0428 C | 0606 NR |
| tidehunter_efa | primary_rf1 | read-concurrent | 8192 | 0041 C | 0374 C | 0482 NR |
| tidehunter_efa_direct | primary_rf1 | read-concurrent | 8192 | 0099 C | 0320 C | 0511 NR |
| tidehunter_efa_native | primary_rf1 | read-concurrent | 8192 | 0010 C | 0264 C | 0540 NR |
| tikv | primary_rf1 | read-concurrent | 8192 | 0128 C | 0295 C | 0660 NR |
| cassandra | primary_rf1 | read-concurrent | 8192 | 0213 C | 0403 C | 0635 NR |
| foundationdb | primary_rf1 | read-concurrent | 8192 | 0188 C | 0349 C | 0457 C |
| native | primary_rf1 | read-concurrent | 65536 | 0146 C | 0230 C | 0570 NR |
| tidehunter_tcp | primary_rf1 | read-concurrent | 65536 | 0063 C | 0429 C | 0605 NR |
| tidehunter_efa | primary_rf1 | read-concurrent | 65536 | 0034 C | 0375 C | 0481 NR |
| tidehunter_efa_direct | primary_rf1 | read-concurrent | 65536 | 0092 C | 0321 C | 0510 NR |
| tidehunter_efa_native | primary_rf1 | read-concurrent | 65536 | 0003 C | 0265 C | 0539 NR |
| tikv | primary_rf1 | read-concurrent | 65536 | 0121 C | 0296 C | 0659 NR |
| cassandra | primary_rf1 | read-concurrent | 65536 | 0206 C | 0404 C | 0634 NR |
| foundationdb | primary_rf1 | read-concurrent | 65536 | 0181 C | 0350 C | 0456 C |
| native | primary_rf1 | write-concurrent | 64 | 0150 C | 0237 C | 0585 NR |
| tidehunter_tcp | primary_rf1 | write-concurrent | 64 | 0067 C | 0436 C | 0620 NR |
| tidehunter_efa | primary_rf1 | write-concurrent | 64 | 0038 C | 0382 C | 0496 NR |
| tidehunter_efa_direct | primary_rf1 | write-concurrent | 64 | 0096 C | 0328 C | 0525 NR |
| tidehunter_efa_native | primary_rf1 | write-concurrent | 64 | 0007 C | 0272 C | 0554 NR |
| tikv | primary_rf1 | write-concurrent | 64 | 0125 C | 0303 C | 0674 NR |
| cassandra | primary_rf1 | write-concurrent | 64 | 0210 C | 0411 C | 0649 NR |
| foundationdb | primary_rf1 | write-concurrent | 64 | 0185 C | 0357 C | 0471 C |
| native | primary_rf1 | write-concurrent | 256 | 0152 C | 0244 C | 0575 NR |
| tidehunter_tcp | primary_rf1 | write-concurrent | 256 | 0069 C | 0443 C | 0610 NR |
| tidehunter_efa | primary_rf1 | write-concurrent | 256 | 0040 C | 0389 C | 0486 NR |
| tidehunter_efa_direct | primary_rf1 | write-concurrent | 256 | 0098 C | 0335 C | 0515 NR |
| tidehunter_efa_native | primary_rf1 | write-concurrent | 256 | 0009 C | 0279 C | 0544 NR |
| tikv | primary_rf1 | write-concurrent | 256 | 0127 C | 0310 C | 0664 NR |
| cassandra | primary_rf1 | write-concurrent | 256 | 0212 C | 0418 C | 0639 NR |
| foundationdb | primary_rf1 | write-concurrent | 256 | 0187 C | 0364 C | 0461 C |
| native | primary_rf1 | write-concurrent | 1024 | 0154 C | 0249 C | 0581 NR |
| tidehunter_tcp | primary_rf1 | write-concurrent | 1024 | 0071 C | 0448 C | 0616 NR |
| tidehunter_efa | primary_rf1 | write-concurrent | 1024 | 0042 C | 0394 C | 0492 NR |
| tidehunter_efa_direct | primary_rf1 | write-concurrent | 1024 | 0100 C | 0340 C | 0521 NR |
| tidehunter_efa_native | primary_rf1 | write-concurrent | 1024 | 0011 C | 0284 C | 0550 NR |
| tikv | primary_rf1 | write-concurrent | 1024 | 0129 C | 0315 C | 0670 NR |
| cassandra | primary_rf1 | write-concurrent | 1024 | 0214 C | 0423 C | 0645 NR |
| foundationdb | primary_rf1 | write-concurrent | 1024 | 0189 C | 0369 C | 0467 C |
| native | primary_rf1 | write-concurrent | 8192 | 0162 C | 0246 C | 0583 NR |
| tidehunter_tcp | primary_rf1 | write-concurrent | 8192 | 0079 C | 0445 C | 0618 NR |
| tidehunter_efa | primary_rf1 | write-concurrent | 8192 | 0050 C | 0391 C | 0494 NR |
| tidehunter_efa_direct | primary_rf1 | write-concurrent | 8192 | 0108 C | 0337 C | 0523 NR |
| tidehunter_efa_native | primary_rf1 | write-concurrent | 8192 | 0019 C | 0281 C | 0552 NR |
| tikv | primary_rf1 | write-concurrent | 8192 | 0137 C | 0312 C | 0672 NR |
| cassandra | primary_rf1 | write-concurrent | 8192 | 0222 C | 0420 C | 0647 NR |
| foundationdb | primary_rf1 | write-concurrent | 8192 | 0197 C | 0366 C | 0469 C |
| native | primary_rf1 | write-concurrent | 65536 | 0148 C | 0245 C | 0587 NR |
| tidehunter_tcp | primary_rf1 | write-concurrent | 65536 | 0065 C | 0444 C | 0622 NR |
| tidehunter_efa | primary_rf1 | write-concurrent | 65536 | 0036 C | 0390 C | 0498 NR |
| tidehunter_efa_direct | primary_rf1 | write-concurrent | 65536 | 0094 C | 0336 C | 0527 NR |
| tidehunter_efa_native | primary_rf1 | write-concurrent | 65536 | 0005 C | 0280 C | 0556 NR |
| tikv | primary_rf1 | write-concurrent | 65536 | 0123 C | 0311 C | 0676 NR |
| cassandra | primary_rf1 | write-concurrent | 65536 | 0208 C | 0419 C | 0651 NR |
| foundationdb | primary_rf1 | write-concurrent | 65536 | 0183 C | 0365 C | 0473 C |
| native | primary_rf1 | read-many32 | 64 | 0165 C | 0236 C | 0590 NR |
| tidehunter_tcp | primary_rf1 | read-many32 | 64 | 0082 C | 0435 C | 0625 NR |
| tidehunter_efa | primary_rf1 | read-many32 | 64 | 0053 C | 0381 C | 0501 NR |
| tidehunter_efa_direct | primary_rf1 | read-many32 | 64 | 0111 C | 0327 C | 0530 NR |
| tidehunter_efa_native | primary_rf1 | read-many32 | 64 | 0022 C | 0271 C | 0559 NR |
| tikv | primary_rf1 | read-many32 | 64 | 0140 C | 0302 C | 0679 NR |
| cassandra | primary_rf1 | read-many32 | 64 | 0225 C | 0410 C | 0654 NR |
| foundationdb | primary_rf1 | read-many32 | 64 | 0200 C | 0356 C | 0476 C |
| native | primary_rf1 | read-many32 | 256 | 0145 C | 0243 C | 0592 NR |
| tidehunter_tcp | primary_rf1 | read-many32 | 256 | 0062 C | 0442 C | 0627 NR |
| tidehunter_efa | primary_rf1 | read-many32 | 256 | 0033 C | 0388 C | 0503 NR |
| tidehunter_efa_direct | primary_rf1 | read-many32 | 256 | 0091 C | 0334 C | 0532 NR |
| tidehunter_efa_native | primary_rf1 | read-many32 | 256 | 0002 C | 0278 C | 0561 NR |
| tikv | primary_rf1 | read-many32 | 256 | 0120 C | 0309 C | 0681 NR |
| cassandra | primary_rf1 | read-many32 | 256 | 0205 C | 0417 C | 0656 NR |
| foundationdb | primary_rf1 | read-many32 | 256 | 0180 C | 0363 C | 0478 C |
| native | primary_rf1 | read-many32 | 1024 | 0161 C | 0248 C | 0574 NR |
| tidehunter_tcp | primary_rf1 | read-many32 | 1024 | 0078 C | 0447 C | 0609 NR |
| tidehunter_efa | primary_rf1 | read-many32 | 1024 | 0049 C | 0393 C | 0485 NR |
| tidehunter_efa_direct | primary_rf1 | read-many32 | 1024 | 0107 C | 0339 C | 0514 NR |
| tidehunter_efa_native | primary_rf1 | read-many32 | 1024 | 0018 C | 0283 C | 0543 NR |
| tikv | primary_rf1 | read-many32 | 1024 | 0136 C | 0314 C | 0663 NR |
| cassandra | primary_rf1 | read-many32 | 1024 | 0221 C | 0422 C | 0638 NR |
| foundationdb | primary_rf1 | read-many32 | 1024 | 0196 C | 0368 C | 0460 C |
| native | primary_rf1 | read-many32 | 8192 | 0143 C | 0242 C | 0586 NR |
| tidehunter_tcp | primary_rf1 | read-many32 | 8192 | 0060 C | 0441 C | 0621 NR |
| tidehunter_efa | primary_rf1 | read-many32 | 8192 | 0031 C | 0387 C | 0497 NR |
| tidehunter_efa_direct | primary_rf1 | read-many32 | 8192 | 0089 C | 0333 C | 0526 NR |
| tidehunter_efa_native | primary_rf1 | read-many32 | 8192 | 0000 C | 0277 C | 0555 NR |
| tikv | primary_rf1 | read-many32 | 8192 | 0118 C | 0308 C | 0675 NR |
| cassandra | primary_rf1 | read-many32 | 8192 | 0203 C | 0416 C | 0650 NR |
| foundationdb | primary_rf1 | read-many32 | 8192 | 0178 C | 0362 C | 0472 C |
| native | primary_rf1 | read-many32 | 65536 | 0144 C | 0238 C | 0573 NR |
| tidehunter_tcp | primary_rf1 | read-many32 | 65536 | 0061 C | 0437 C | 0608 NR |
| tidehunter_efa | primary_rf1 | read-many32 | 65536 | 0032 C | 0383 C | 0484 NR |
| tidehunter_efa_direct | primary_rf1 | read-many32 | 65536 | 0090 C | 0329 C | 0513 NR |
| tidehunter_efa_native | primary_rf1 | read-many32 | 65536 | 0001 C | 0273 C | 0542 NR |
| tikv | primary_rf1 | read-many32 | 65536 | 0119 C | 0304 C | 0662 NR |
| cassandra | primary_rf1 | read-many32 | 65536 | 0204 C | 0412 C | 0637 NR |
| foundationdb | primary_rf1 | read-many32 | 65536 | 0179 C | 0358 C | 0459 C |
| native | primary_rf1 | point-mixed | 64 | 0147 C | 0234 C | 0584 NR |
| tidehunter_tcp | primary_rf1 | point-mixed | 64 | 0064 C | 0433 C | 0619 NR |
| tidehunter_efa | primary_rf1 | point-mixed | 64 | 0035 C | 0379 C | 0495 NR |
| tidehunter_efa_direct | primary_rf1 | point-mixed | 64 | 0093 C | 0325 C | 0524 NR |
| tidehunter_efa_native | primary_rf1 | point-mixed | 64 | 0004 C | 0269 C | 0553 NR |
| tikv | primary_rf1 | point-mixed | 64 | 0122 C | 0300 C | 0673 NR |
| cassandra | primary_rf1 | point-mixed | 64 | 0207 C | 0408 C | 0648 NR |
| foundationdb | primary_rf1 | point-mixed | 64 | 0182 C | 0354 C | 0470 C |
| native | primary_rf1 | point-mixed | 256 | 0160 C | 0241 C | 0576 NR |
| tidehunter_tcp | primary_rf1 | point-mixed | 256 | 0077 C | 0440 C | 0611 NR |
| tidehunter_efa | primary_rf1 | point-mixed | 256 | 0048 C | 0386 C | 0487 NR |
| tidehunter_efa_direct | primary_rf1 | point-mixed | 256 | 0106 C | 0332 C | 0516 NR |
| tidehunter_efa_native | primary_rf1 | point-mixed | 256 | 0017 C | 0276 C | 0545 NR |
| tikv | primary_rf1 | point-mixed | 256 | 0135 C | 0307 C | 0665 NR |
| cassandra | primary_rf1 | point-mixed | 256 | 0220 C | 0415 C | 0640 NR |
| foundationdb | primary_rf1 | point-mixed | 256 | 0195 C | 0361 C | 0462 C |
| native | primary_rf1 | point-mixed | 1024 | 0158 C | 0247 C | 0582 NR |
| tidehunter_tcp | primary_rf1 | point-mixed | 1024 | 0075 C | 0446 C | 0617 NR |
| tidehunter_efa | primary_rf1 | point-mixed | 1024 | 0046 C | 0392 C | 0493 NR |
| tidehunter_efa_direct | primary_rf1 | point-mixed | 1024 | 0104 C | 0338 C | 0522 NR |
| tidehunter_efa_native | primary_rf1 | point-mixed | 1024 | 0015 C | 0282 C | 0551 NR |
| tikv | primary_rf1 | point-mixed | 1024 | 0133 C | 0313 C | 0671 NR |
| cassandra | primary_rf1 | point-mixed | 1024 | 0218 C | 0421 C | 0646 NR |
| foundationdb | primary_rf1 | point-mixed | 1024 | 0193 C | 0367 C | 0468 C |
| native | primary_rf1 | point-mixed | 8192 | 0166 C | 0235 C | 0578 NR |
| tidehunter_tcp | primary_rf1 | point-mixed | 8192 | 0083 C | 0434 C | 0613 NR |
| tidehunter_efa | primary_rf1 | point-mixed | 8192 | 0054 C | 0380 C | 0489 NR |
| tidehunter_efa_direct | primary_rf1 | point-mixed | 8192 | 0112 C | 0326 C | 0518 NR |
| tidehunter_efa_native | primary_rf1 | point-mixed | 8192 | 0023 C | 0270 C | 0547 NR |
| tikv | primary_rf1 | point-mixed | 8192 | 0141 C | 0301 C | 0667 NR |
| cassandra | primary_rf1 | point-mixed | 8192 | 0226 C | 0409 C | 0642 NR |
| foundationdb | primary_rf1 | point-mixed | 8192 | 0201 C | 0355 C | 0464 C |
| native | primary_rf1 | point-mixed | 65536 | 0157 C | 0240 C | 0588 NR |
| tidehunter_tcp | primary_rf1 | point-mixed | 65536 | 0074 C | 0439 C | 0623 NR |
| tidehunter_efa | primary_rf1 | point-mixed | 65536 | 0045 C | 0385 C | 0499 NR |
| tidehunter_efa_direct | primary_rf1 | point-mixed | 65536 | 0103 C | 0331 C | 0528 NR |
| tidehunter_efa_native | primary_rf1 | point-mixed | 65536 | 0014 C | 0275 C | 0557 NR |
| tikv | primary_rf1 | point-mixed | 65536 | 0132 C | 0306 C | 0677 NR |
| cassandra | primary_rf1 | point-mixed | 65536 | 0217 C | 0414 C | 0652 NR |
| foundationdb | primary_rf1 | point-mixed | 65536 | 0192 C | 0360 C | 0474 C |
| native | primary_rf1 | offered-1000 | 1024 | 0149 C | 0232 C | 0577 NR |
| tidehunter_tcp | primary_rf1 | offered-1000 | 1024 | 0066 C | 0431 C | 0612 NR |
| tidehunter_efa | primary_rf1 | offered-1000 | 1024 | 0037 C | 0377 C | 0488 NR |
| tidehunter_efa_direct | primary_rf1 | offered-1000 | 1024 | 0095 C | 0323 C | 0517 NR |
| tidehunter_efa_native | primary_rf1 | offered-1000 | 1024 | 0006 C | 0267 C | 0546 NR |
| tikv | primary_rf1 | offered-1000 | 1024 | 0124 C | 0298 C | 0666 NR |
| cassandra | primary_rf1 | offered-1000 | 1024 | 0209 C | 0406 C | 0641 NR |
| foundationdb | primary_rf1 | offered-1000 | 1024 | 0184 C | 0352 C | 0463 C |
| native | primary_rf1 | offered-10000 | 1024 | 0155 C | 0251 C | 0572 NR |
| tidehunter_tcp | primary_rf1 | offered-10000 | 1024 | 0072 C | 0450 C | 0607 NR |
| tidehunter_efa | primary_rf1 | offered-10000 | 1024 | 0043 C | 0396 C | 0483 NR |
| tidehunter_efa_direct | primary_rf1 | offered-10000 | 1024 | 0101 C | 0342 C | 0512 NR |
| tidehunter_efa_native | primary_rf1 | offered-10000 | 1024 | 0012 C | 0286 C | 0541 NR |
| tikv | primary_rf1 | offered-10000 | 1024 | 0130 C | 0317 C | 0661 NR |
| cassandra | primary_rf1 | offered-10000 | 1024 | 0215 C | 0425 C | 0636 NR |
| foundationdb | primary_rf1 | offered-10000 | 1024 | 0190 C | 0371 C | 0458 C |
| native | primary_rf1 | offered-50000 | 1024 | 0159 C | 0231 C | 0579 NR |
| tidehunter_tcp | primary_rf1 | offered-50000 | 1024 | 0076 C | 0430 C | 0614 NR |
| tidehunter_efa | primary_rf1 | offered-50000 | 1024 | 0047 C | 0376 C | 0490 NR |
| tidehunter_efa_direct | primary_rf1 | offered-50000 | 1024 | 0105 C | 0322 C | 0519 NR |
| tidehunter_efa_native | primary_rf1 | offered-50000 | 1024 | 0016 C | 0266 C | 0548 NR |
| tikv | primary_rf1 | offered-50000 | 1024 | 0134 C | 0297 C | 0668 NR |
| cassandra | primary_rf1 | offered-50000 | 1024 | 0219 C | 0405 C | 0643 NR |
| foundationdb | primary_rf1 | offered-50000 | 1024 | 0194 C | 0351 C | 0465 C |
| native | primary_rf1 | zipf-read | 1024 | 0163 C | 0228 C | 0589 NR |
| tidehunter_tcp | primary_rf1 | zipf-read | 1024 | 0080 C | 0427 C | 0624 NR |
| tidehunter_efa | primary_rf1 | zipf-read | 1024 | 0051 C | 0373 C | 0500 NR |
| tidehunter_efa_direct | primary_rf1 | zipf-read | 1024 | 0109 C | 0319 C | 0529 NR |
| tidehunter_efa_native | primary_rf1 | zipf-read | 1024 | 0020 C | 0263 C | 0558 NR |
| tikv | primary_rf1 | zipf-read | 1024 | 0138 C | 0294 C | 0678 NR |
| cassandra | primary_rf1 | zipf-read | 1024 | 0223 C | 0402 C | 0653 NR |
| foundationdb | primary_rf1 | zipf-read | 1024 | 0198 C | 0348 C | 0475 C |
| native | native_pagecache | read-many32 | 64 | 0175 C | 0252 C | 0598 NR |
| native | native_pagecache | read-many32 | 256 | 0173 C | 0256 C | 0602 NR |
| native | native_pagecache | read-many32 | 1024 | 0167 C | 0260 C | 0595 NR |
| native | native_pagecache | read-many32 | 8192 | 0168 C | 0254 C | 0594 NR |
| native | native_pagecache | read-many32 | 65536 | 0172 C | 0257 C | 0596 NR |
| native | native_pagecache | point-mixed | 64 | 0174 C | 0253 C | 0599 NR |
| native | native_pagecache | point-mixed | 256 | 0171 C | 0261 C | 0603 NR |
| native | native_pagecache | point-mixed | 1024 | 0169 C | 0255 C | 0601 NR |
| native | native_pagecache | point-mixed | 8192 | 0176 C | 0258 C | 0600 NR |
| native | native_pagecache | point-mixed | 65536 | 0170 C | 0259 C | 0597 NR |
| tidehunter_tcp | coordinator_cache_configured | coordinator-cache | 256 | 0086 C | 0451 C | 0631 NR |
| tidehunter_efa | coordinator_cache_configured | coordinator-cache | 256 | 0057 C | 0397 C | 0507 NR |
| tidehunter_efa_direct | coordinator_cache_configured | coordinator-cache | 256 | 0115 C | 0343 C | 0536 NR |
| tidehunter_efa_native | coordinator_cache_configured | coordinator-cache | 256 | 0028 C | 0287 C | 0565 NR |
| tidehunter_tcp | coordinator_cache_configured | coordinator-cache | 8192 | 0085 C | 0452 C | 0629 NR |
| tidehunter_efa | coordinator_cache_configured | coordinator-cache | 8192 | 0056 C | 0398 C | 0505 NR |
| tidehunter_efa_direct | coordinator_cache_configured | coordinator-cache | 8192 | 0114 C | 0344 C | 0534 NR |
| tidehunter_efa_native | coordinator_cache_configured | coordinator-cache | 8192 | 0025 C | 0290 C | 0566 NR |
| tidehunter_tcp | owner_cache_configured | owner-cache | 256 | 0084 C | 0454 C | 0632 NR |
| tidehunter_efa | owner_cache_configured | owner-cache | 256 | 0055 C | 0400 C | 0508 NR |
| tidehunter_efa_direct | owner_cache_configured | owner-cache | 256 | 0113 C | 0346 C | 0537 NR |
| tidehunter_efa_native | owner_cache_configured | owner-cache | 256 | 0026 C | 0291 C | 0568 NR |
| tidehunter_tcp | owner_cache_configured | owner-cache | 8192 | 0087 C | 0453 C | 0630 NR |
| tidehunter_efa | owner_cache_configured | owner-cache | 8192 | 0058 C | 0399 C | 0506 NR |
| tidehunter_efa_direct | owner_cache_configured | owner-cache | 8192 | 0116 C | 0345 C | 0535 NR |
| tidehunter_efa_native | owner_cache_configured | owner-cache | 8192 | 0029 C | 0288 C | 0564 NR |
| tidehunter_efa_native | registered_ram_configured | registered-ram | 256 | 0027 C | 0289 C | 0563 NR |
| tidehunter_efa_native | registered_ram_configured | registered-ram | 8192 | 0024 C | 0292 C | 0567 NR |
| native | cold_os_cache | cold-os-large | 65536 | 0177 C | 0262 C | 0604 NR |
| tidehunter_tcp | cold_os_cache | cold-os-large | 65536 | 0088 C | 0455 C | 0633 NR |
| tidehunter_efa | cold_os_cache | cold-os-large | 65536 | 0059 C | 0401 C | 0509 NR |
| tidehunter_efa_direct | cold_os_cache | cold-os-large | 65536 | 0117 C | 0347 C | 0538 NR |
| tidehunter_efa_native | cold_os_cache | cold-os-large | 65536 | 0030 C | 0293 C | 0569 NR |
| tikv | cold_os_cache | cold-os-large | 65536 | 0142 C | 0318 C | 0683 NR |
| cassandra | cold_os_cache | cold-os-large | 65536 | 0227 C | 0426 C | 0658 NR |
| foundationdb | cold_os_cache | cold-os-large | 65536 | 0202 F | 0372 F | 0480 F |
