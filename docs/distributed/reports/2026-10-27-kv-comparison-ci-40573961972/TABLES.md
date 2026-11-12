# KV comparison: complete population tables

Run `th-6449c7068670440c`, source `87465ef68abb6fa9b7b9cb27224281a7c4994e6d`. One independent repetition: numbers are per-case, not repeat medians or confidence estimates. [Full original CSV](all-results.csv) retains every raw p50/p95/p99/p99.9, mean, stddev, maximum and minimum for API, response and scheduling, plus configurations, outcome counts and case-wide calls/s, logical keys/s and MiB/s; [machine analysis](metrics-analysis.json); [historical comparisons](historical-comparison.csv). Case IDs below identify `validated-results/cases/<case_id>/`; per-case receipt, result, configuration and counter evidence are there.

API latency is a whole call (including the entire ordered get-many32), **not per-key latency**. Case throughput includes all successful operations, not the population alone; logical-key throughput is shown separately. Response and scheduling clocks have distinct boundaries; rejected operations have no API/scheduling clock. `—` means unavailable, never zero. Sample count is population calls; nearest-rank quantiles are conditional on timed samples and small populations have unstable tails. The harness warning is **p99 descriptive below 10,000 calls; p99.9 descriptive below 100,000 calls**. In addition, for n<1,000, nearest-rank p99.9 can resolve to the maximum; p99 at n<100 similarly. Standard deviation is within-case, not run-to-run.

Synthetic KV only, not Sui replay. Native is an embedded lower bound, not a five-host scale-equivalent peer. Backends differ in acknowledgement, consistency, batching, security and storage; consult full CSV. Valkey is RF1 AOF `appendfsync always`; GET is RAM-resident, not a disk-GET, RDMA, Sui, cross-slot atomic batch or HA comparison. Monitoring was cohosted and its perturbation was not calibrated.

## Coverage and accounting

258/258 completed, 9 variants, one repeat, 460 population rows; all case measurement rows complete and planned calls equal observed. 5,735,488 observed calls = 4,141,321 successful + 1,594,167 rejected; no other outcome or case error recorded. Rejections occur in 14 cases and must not be dropped from offered-load interpretation.

| Backend | Cases |
|---|---:|
| cassandra | 25 |
| foundationdb | 25 |
| native | 36 |
| tidehunter_efa | 30 |
| tidehunter_efa_direct | 30 |
| tidehunter_efa_native | 32 |
| tidehunter_tcp | 30 |
| tikv | 25 |
| valkey_tcp | 25 |

| Cohort | Cases |
|---|---:|
| cold_os_cache | 9 |
| coordinator_cache_configured | 8 |
| native_pagecache | 10 |
| owner_cache_configured | 8 |
| primary_rf1 | 216 |
| registered_ram_configured | 2 |
| write_stage_diagnostics | 5 |

## Selected 256-byte comparisons

Read-concurrent success API p50: EFA-native 92.122 µs (0058), TCP 197.664 µs (0211), Valkey 211.415 µs (0033), TiKV 335.708 µs (0120), FoundationDB 595.573 µs (0008), Cassandra 244.936 µs (0241), native 1.820 µs (0175). These are case prefixes; full IDs and p99/goodput below. EFA-native therefore beats these distributed alternatives on this particular read metric, while remaining far above embedded native.

Write-concurrent at 256 bytes: EFA-native p50 913.831 µs, p99 142,638.683 µs and 4,848.924 calls/s (0059), versus Valkey 439.929/864.419 µs and 32,326.545 calls/s (0034); Cassandra 369.698/728.266 µs and 36,553.485 calls/s (0242); native 607.113/2,721.570 µs and 20,129.704 calls/s (0176). This is worse for EFA-native here, not evidence of a universal bottleneck or equivalent durability.

Read-many32 at 256 bytes: EFA-direct 1,299.009 µs p50, 11,699.812 calls/s and 374,393.990 logical keys/s (0139); Valkey 2,003.884 µs, 6,643.908 calls/s (0027); native 61.891 µs, 128,971.218 calls/s (0169). Whole-batch latency is not per key.

## Write-stage diagnostic evidence

Clean primary 1,024-byte write-concurrent put p99 values are 142,370.794 µs (EFA-native `0061-tidehunter_efa_native-1024-write-concurrent-primary_rf1-r0`), 142,792.856 µs (EFA `0093-tidehunter_efa-1024-write-concurrent-primary_rf1-r0`), 142,883.737 µs (EFA-direct `0148-tidehunter_efa_direct-1024-write-concurrent-primary_rf1-r0`) and 144,162.639 µs (TCP `0214-tidehunter_tcp-1024-write-concurrent-primary_rf1-r0`); each has 4,096 calls, below the harness p99 descriptive threshold. Similar tails across transports are an observation, not proof of a shared cause.

The separately profiled 1,024-byte cases below are **not** the clean primary cases. Their complete emitted counters and selection metadata are in `cases/<case_id>/result/write-diagnostics.json`; raw API populations remain in the CSV. Each distributed case captured five connections, selecting the first up-to-256 append requests per connection after measurement arm. Coordinator counters are measurement-boundary cumulative deltas, while append/queue/storage counters are sums over selected requests. Clocks overlap or nest, and are neither elapsed wall time nor additive.

| Diagnostic case ID | API p99 µs | Selected append requests | Coordinator blocked durable receipt batches | Blocked receipt sum ms | Owner append sum ms | Connection control-queue sum ms | Owner file-persist successes |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0080-tidehunter_efa_native-1024-write-concurrent-write_stage_diagnostics-r0 | 142,697.023 | 1,233 | 2,004 | 8,710.287 | 878.867 | 471.591 | 1,159 |
| 0110-tidehunter_efa-1024-write-concurrent-write_stage_diagnostics-r0 | 142,893.538 | 1,206 | 2,001 | 9,128.414 | 881.649 | 616.110 | 1,144 |
| 0165-tidehunter_efa_direct-1024-write-concurrent-write_stage_diagnostics-r0 | 142,594.306 | 1,214 | 2,021 | 8,724.194 | 874.108 | 468.651 | 1,145 |
| 0231-tidehunter_tcp-1024-write-concurrent-write_stage_diagnostics-r0 | 143,008.810 | 1,256 | 2,067 | 9,271.702 | 984.980 | 516.464 | 1,144 |

Each distributed diagnostic reports 4,096 durable batches and nonzero publication-blocked receipt, owner append and control-queue counters, plus successful file-persist calls; these establish that the instrumented activities occurred in those profiled cases, not which activity caused the clean primary p99. The sums are not individual-request distributions, and the emitted diagnostics explicitly mark per-operation write percentiles and physical-device I/O unsupported. Neither subtraction of unrelated quantiles nor assigning a residual to one-way network latency is valid.

Native diagnostic `0201-native-1024-write-concurrent-write_stage_diagnostics-r0` reports p99 3,406.105 µs for 4,096 calls. Its first 256 accepted scalar writes recorded cumulative WAL append 105.355 ms, index application 0.804 ms and 256 sampled Value-WAL barriers with 512 file successes (186.706 ms file-time sum), without reported errors. These are separate, potentially nested clocks; physical-device I/O and per-operation stage percentiles are unsupported. Native diagnostic profiling and distributed profiling have different boundaries and selection policies.

## Registered-RAM transport evidence

The five storage startup proofs `guest/startup/storage-*-*.json` advertise FI_MSG/FI_RMA/FI_READ/FI_REMOTE_READ through fi_getinfo, explicitly discovery only. Registered-RAM case configs request `require_device_read=true`; post-workload `cases/<case_id>/read-evidence/read-evidence.json` reports completed client-side RDMA reads and errors below, alongside substantial message fallback. Counters are cumulative adapter lifetime through post-workload qualification, not measurement-only; their counts cannot be assigned to the timed population. Configuration plus completion counters establish observed RMA-path operation, but fi_getinfo alone is not hardware qualification; emitted evidence does not independently establish the provider’s physical hardware implementation for each completion.

| Case ID | RMA posts | Completions | Errors | Missing fallback | Message read requests |
|---|---:|---:|---:|---:|---:|
| 0074-tidehunter_efa_native-8192-registered-ram-registered_ram_configured-r0 | 2448 | 2448 | 0 | 6778 | 6778 |
| 0077-tidehunter_efa_native-256-registered-ram-registered_ram_configured-r0 | 2480 | 2480 | 0 | 6746 | 6746 |

## Historical context

228 current cases each have two historical **job-field** matches; 30 have none (25 Valkey and 5 write-stage cases). This is not 228 fully configuration-matched cases. Across 456 pairs, comparison of the emitted job, dataset, execution, limits, phases, qualification, workload, client budget, backend contract/effective configuration, adapter configuration and seed found 110 pairs equal on these fields, 110 with a changed seed (1775 vs 1776) but equal on the other compared fields, and 236 with changed TideHunter adapter/transport configuration representation. For TideHunter, old backend-specific kinds and transport strings became a generic kind and structured transport options; effective transport labels also changed for EFA variants. Equivalence is not assumed: those pairs are explicitly classified rather than presented as verified configuration matches. Run-specific paths and IP addresses were normalized, not substantive values. TideHunter per-case storage-unit CPU/memory limits were compared; native and external-store per-case server-unit proofs were not emitted, so their server limits are not established by this pairwise check. Exact pair classifications, differences, seeds and evidence availability are in [metrics-analysis.json](metrics-analysis.json), and classifications accompany each CSV row.

Historical run `th-e4a783f03101a780` used us-east-1a and source `c61c9e295708e5b4354dae43538e900f3515df4d`, while current used eu-central-1c and a different source. Results are region- and implementation-confounded, and are descriptive single-repetition observations, not statistical significance or causal effects. The historical CSV keeps each old repetition separate, including missing populations. Materiality: latency ≥10% **and** ≥1 µs; throughput ≥10%; rejection ≥1 percentage point. The thresholds do not establish configuration equivalence or significance.

Example, 256-byte native read-concurrent (current `0175-native-256-read-concurrent-primary_rf1-r0`): calls/s 161,880.012 versus 131,910.452 for historical `0151-native-256-read-concurrent-primary_rf1-r0` (+22.7%, material), whose compared emitted settings and seed match. Versus historical `0250-native-256-read-concurrent-primary_rf1-r1`, calls/s were 102,302.034 (+58.2%, material) but the seed differs. In both cases native server-unit limits were not verified by the pairwise case-proof check; neither comparison isolates a cause.

## All case populations

Each section separates cohort, profile and value size; all backend and outcome rows are retained. Case-level calls/s and keys/s repeat for each population in the same case and must not be summed. Unsuccessful is the case count. The CSV retains unrounded numbers and all other quantiles and boundaries.

### cold_os_cache / cold-os-large / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0257-cassandra-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 1,084.885 | 2,979.467 | 3,127.531 | 80.302 | 11,029.381 | 11,029.381 | 0 |
| 0024-foundationdb-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 1,262.828 | 3,171.639 | 3,215.550 | 85.842 | 10,918.896 | 10,918.896 | 0 |
| 0202-native-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 284.446 | 582.133 | 627.024 | 77.121 | 44,596.499 | 44,596.499 | 0 |
| 0111-tidehunter_efa-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 1,150.685 | 6,610.485 | 6,834.899 | 61.232 | 9,711.247 | 9,711.247 | 0 |
| 0166-tidehunter_efa_direct-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 1,203.157 | 10,045.122 | 10,483.113 | 111.852 | 9,261.477 | 9,261.477 | 0 |
| 0081-tidehunter_efa_native-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 1,181.175 | 13,525.156 | 13,786.522 | 96.112 | 9,474.188 | 9,474.188 | 0 |
| 0232-tidehunter_tcp-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 1,311.580 | 14,723.608 | 14,755.049 | 93.462 | 8,484.596 | 8,484.596 | 0 |
| 0136-tikv-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 853.668 | 1,684.237 | 1,721.457 | 103.292 | 16,802.868 | 16,802.868 | 0 |
| 0049-valkey_tcp-65536-cold-os-large-cold_os_cache-r0 | read/success | 4096 | 332.147 | 600.253 | 650.094 | 134.353 | 39,160.643 | 39,160.643 | 0 |

### coordinator_cache_configured / coordinator-cache / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0108-tidehunter_efa-256-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 0.860 | 1.960 | 25.051 | 23.351 | 33,472.883 | 33,472.883 | 0 |
| 0163-tidehunter_efa_direct-256-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 0.730 | 1.891 | 24.661 | 23.471 | 34,138.187 | 34,138.187 | 0 |
| 0078-tidehunter_efa_native-256-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 0.830 | 1.940 | 24.890 | 23.561 | 33,471.016 | 33,471.016 | 0 |
| 0229-tidehunter_tcp-256-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 0.780 | 1.720 | 24.680 | 23.480 | 31,122.330 | 31,122.330 | 0 |

### coordinator_cache_configured / coordinator-cache / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0107-tidehunter_efa-8192-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 0.990 | 1.760 | 28.171 | 26.871 | 30,398.201 | 30,398.201 | 0 |
| 0162-tidehunter_efa_direct-8192-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 1.050 | 1.920 | 29.181 | 28.131 | 29,404.668 | 29,404.668 | 0 |
| 0075-tidehunter_efa_native-8192-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 1.120 | 1.890 | 27.801 | 26.401 | 30,167.763 | 30,167.763 | 0 |
| 0228-tidehunter_tcp-8192-coordinator-cache-coordinator_cache_configured-r0 | read/success | 8192 | 0.820 | 1.630 | 27.801 | 26.561 | 31,223.796 | 31,223.796 | 0 |

### native_pagecache / point-mixed / 64 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0198-native-64-point-mixed-native_pagecache-r0 | delete/success | 391 | 5.810 | 16.080 | 31.661 | 24.931 | 29,460.953 | 29,460.953 | 0 |
| 0198-native-64-point-mixed-native_pagecache-r0 | missing_read/success | 405 | 0.670 | 1.200 | 22.100 | 21.140 | 29,460.953 | 29,460.953 | 0 |
| 0198-native-64-point-mixed-native_pagecache-r0 | put/success | 3775 | 5.891 | 15.181 | 31.861 | 24.750 | 29,460.953 | 29,460.953 | 0 |
| 0198-native-64-point-mixed-native_pagecache-r0 | read/success | 3621 | 1.250 | 2.210 | 25.850 | 23.940 | 29,460.953 | 29,460.953 | 0 |

### native_pagecache / point-mixed / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0195-native-256-point-mixed-native_pagecache-r0 | delete/success | 391 | 7.570 | 17.620 | 37.770 | 27.170 | 26,236.016 | 26,236.016 | 0 |
| 0195-native-256-point-mixed-native_pagecache-r0 | missing_read/success | 405 | 1.030 | 1.900 | 21.461 | 20.431 | 26,236.016 | 26,236.016 | 0 |
| 0195-native-256-point-mixed-native_pagecache-r0 | put/success | 3775 | 7.450 | 21.800 | 41.121 | 29.181 | 26,236.016 | 26,236.016 | 0 |
| 0195-native-256-point-mixed-native_pagecache-r0 | read/success | 3621 | 1.880 | 3.320 | 31.320 | 28.950 | 26,236.016 | 26,236.016 | 0 |

### native_pagecache / point-mixed / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0193-native-1024-point-mixed-native_pagecache-r0 | delete/success | 391 | 5.780 | 11.940 | 29.781 | 22.030 | 28,982.895 | 28,982.895 | 0 |
| 0193-native-1024-point-mixed-native_pagecache-r0 | missing_read/success | 405 | 0.670 | 1.460 | 24.141 | 23.261 | 28,982.895 | 28,982.895 | 0 |
| 0193-native-1024-point-mixed-native_pagecache-r0 | put/success | 3775 | 6.090 | 24.381 | 37.661 | 22.961 | 28,982.895 | 28,982.895 | 0 |
| 0193-native-1024-point-mixed-native_pagecache-r0 | read/success | 3621 | 1.420 | 3.010 | 25.511 | 23.300 | 28,982.895 | 28,982.895 | 0 |

### native_pagecache / point-mixed / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0200-native-8192-point-mixed-native_pagecache-r0 | delete/success | 391 | 6.130 | 19.071 | 35.500 | 24.270 | 23,291.851 | 23,291.851 | 0 |
| 0200-native-8192-point-mixed-native_pagecache-r0 | missing_read/success | 405 | 0.770 | 2.000 | 23.910 | 19.920 | 23,291.851 | 23,291.851 | 0 |
| 0200-native-8192-point-mixed-native_pagecache-r0 | put/success | 3775 | 12.881 | 61.452 | 86.742 | 28.960 | 23,291.851 | 23,291.851 | 0 |
| 0200-native-8192-point-mixed-native_pagecache-r0 | read/success | 3621 | 4.010 | 16.920 | 41.671 | 28.601 | 23,291.851 | 23,291.851 | 0 |

### native_pagecache / point-mixed / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0194-native-65536-point-mixed-native_pagecache-r0 | delete/success | 391 | 5.540 | 18.911 | 41.811 | 25.520 | 8,571.079 | 8,571.079 | 0 |
| 0194-native-65536-point-mixed-native_pagecache-r0 | missing_read/success | 405 | 1.210 | 5.750 | 29.341 | 27.650 | 8,571.079 | 8,571.079 | 0 |
| 0194-native-65536-point-mixed-native_pagecache-r0 | put/success | 3775 | 73.002 | 880.549 | 939.561 | 89.172 | 8,571.079 | 8,571.079 | 0 |
| 0194-native-65536-point-mixed-native_pagecache-r0 | read/success | 3621 | 14.270 | 48.821 | 131.093 | 87.462 | 8,571.079 | 8,571.079 | 0 |

### native_pagecache / read-many32 / 64 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0199-native-64-read-many32-native_pagecache-r0 | get_many/success | 2048 | 51.811 | 78.512 | 128.103 | 56.161 | 119,330.240 | 3,818,567.692 | 0 |

### native_pagecache / read-many32 / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0197-native-256-read-many32-native_pagecache-r0 | get_many/success | 2048 | 40.431 | 79.241 | 142.123 | 58.241 | 124,866.605 | 3,995,731.367 | 0 |

### native_pagecache / read-many32 / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0191-native-1024-read-many32-native_pagecache-r0 | get_many/success | 2048 | 54.862 | 85.692 | 146.483 | 68.171 | 118,395.710 | 3,788,662.732 | 0 |

### native_pagecache / read-many32 / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0192-native-8192-read-many32-native_pagecache-r0 | get_many/success | 2048 | 121.982 | 293.047 | 506.942 | 213.965 | 51,925.772 | 1,661,624.695 | 0 |

### native_pagecache / read-many32 / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0196-native-65536-read-many32-native_pagecache-r0 | get_many/success | 2048 | 1,885.802 | 52,702.919 | 56,746.719 | 54,231.743 | 1,814.784 | 58,073.091 | 0 |

### owner_cache_configured / owner-cache / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0106-tidehunter_efa-256-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 51.811 | 86.822 | 102.362 | 26.750 | 10,908.167 | 10,908.167 | 0 |
| 0161-tidehunter_efa_direct-256-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 49.261 | 83.032 | 98.892 | 25.880 | 11,153.047 | 11,153.047 | 0 |
| 0076-tidehunter_efa_native-256-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 48.631 | 83.032 | 99.652 | 27.831 | 11,420.695 | 11,420.695 | 0 |
| 0227-tidehunter_tcp-256-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 160.514 | 262.846 | 282.166 | 34.581 | 4,634.492 | 4,634.492 | 0 |

### owner_cache_configured / owner-cache / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0109-tidehunter_efa-8192-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 61.801 | 108.922 | 126.513 | 25.810 | 9,662.057 | 9,662.057 | 0 |
| 0164-tidehunter_efa_direct-8192-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 61.391 | 106.772 | 125.783 | 30.030 | 9,205.269 | 9,205.269 | 0 |
| 0079-tidehunter_efa_native-8192-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 54.822 | 99.592 | 118.903 | 31.041 | 9,771.502 | 9,771.502 | 0 |
| 0230-tidehunter_tcp-8192-owner-cache-owner_cache_configured-r0 | read/success | 8192 | 181.534 | 322.597 | 343.787 | 33.521 | 4,021.484 | 4,021.484 | 0 |

### primary_rf1 / offered-1000 / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0239-cassandra-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 548.652 | 1,094.504 | 2,351.406 | 1,506.318 | 999.916 | 999.916 | 0 |
| 0239-cassandra-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 372.539 | 544.502 | 1,947.447 | 1,506.531 | 999.916 | 999.916 | 0 |
| 0006-foundationdb-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 1,253.997 | 2,194.088 | 3,479.582 | 1,759.855 | 999.855 | 999.855 | 0 |
| 0006-foundationdb-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 681.964 | 5,002.600 | 6,200.579 | 1,753.415 | 999.855 | 999.855 | 0 |
| 0173-native-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 385.919 | 526.531 | 2,109.887 | 1,684.814 | 999.989 | 999.989 | 0 |
| 0173-native-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 6.470 | 15.171 | 1,695.012 | 1,687.283 | 999.989 | 999.989 | 0 |
| 0088-tidehunter_efa-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 496.901 | 133,077.173 | 138,304.534 | 74,490.627 | 1,000.042 | 1,000.042 | 0 |
| 0088-tidehunter_efa-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 121.943 | 223.674 | 79,972.748 | 79,692.762 | 1,000.042 | 1,000.042 | 0 |
| 0143-tidehunter_efa_direct-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 473.051 | 133,969.726 | 138,773.396 | 76,165.082 | 999.845 | 999.845 | 1 |
| 0143-tidehunter_efa_direct-1024-offered-1000-primary_rf1-r0 | read/rejected | 1 | — | — | 1,350.254 | — | 999.845 | 999.845 | 1 |
| 0143-tidehunter_efa_direct-1024-offered-1000-primary_rf1-r0 | read/success | 5571 | 112.222 | 222.035 | 81,041.975 | 80,821.461 | 999.845 | 999.845 | 1 |
| 0056-tidehunter_efa_native-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 460.030 | 134,098.527 | 138,257.828 | 76,984.028 | 999.824 | 999.824 | 1 |
| 0056-tidehunter_efa_native-1024-offered-1000-primary_rf1-r0 | read/rejected | 1 | — | — | 1,006.725 | — | 999.824 | 999.824 | 1 |
| 0056-tidehunter_efa_native-1024-offered-1000-primary_rf1-r0 | read/success | 5571 | 100.582 | 198.795 | 80,173.259 | 80,021.076 | 999.824 | 999.824 | 1 |
| 0209-tidehunter_tcp-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 713.036 | 134,381.280 | 139,973.693 | 75,315.456 | 1,000.020 | 1,000.020 | 0 |
| 0209-tidehunter_tcp-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 274.976 | 494.102 | 79,900.229 | 79,294.156 | 1,000.020 | 1,000.020 | 0 |
| 0118-tikv-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 939.271 | 1,352.550 | 3,133.647 | 1,951.616 | 999.945 | 999.945 | 0 |
| 0118-tikv-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 509.751 | 655.324 | 2,500.908 | 1,945.781 | 999.945 | 999.945 | 0 |
| 0031-valkey_tcp-1024-offered-1000-primary_rf1-r0 | put/success | 2428 | 590.843 | 830.288 | 2,624.002 | 1,961.655 | 999.976 | 999.976 | 0 |
| 0031-valkey_tcp-1024-offered-1000-primary_rf1-r0 | read/success | 5572 | 287.816 | 523.242 | 2,270.255 | 1,964.676 | 999.976 | 999.976 | 0 |

### primary_rf1 / offered-10000 / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0245-cassandra-1024-offered-10000-primary_rf1-r0 | put/success | 24197 | 492.241 | 970.652 | 2,605.265 | 1,903.565 | 9,997.873 | 9,997.873 | 0 |
| 0245-cassandra-1024-offered-10000-primary_rf1-r0 | read/success | 55803 | 296.536 | 485.331 | 2,276.880 | 1,917.118 | 9,997.873 | 9,997.873 | 0 |
| 0012-foundationdb-1024-offered-10000-primary_rf1-r0 | put/success | 24197 | 942.051 | 1,661.386 | 3,407.335 | 2,158.544 | 9,996.163 | 9,996.163 | 0 |
| 0012-foundationdb-1024-offered-10000-primary_rf1-r0 | read/success | 55803 | 630.523 | 4,346.865 | 5,519.868 | 2,125.830 | 9,996.163 | 9,996.163 | 0 |
| 0179-native-1024-offered-10000-primary_rf1-r0 | put/success | 24197 | 369.698 | 882.560 | 2,503.127 | 1,962.301 | 9,998.542 | 9,998.542 | 0 |
| 0179-native-1024-offered-10000-primary_rf1-r0 | read/success | 55803 | 5.270 | 20.690 | 1,974.813 | 1,967.996 | 9,998.542 | 9,998.542 | 0 |
| 0094-tidehunter_efa-1024-offered-10000-primary_rf1-r0 | put/rejected | 6904 | — | — | 1,975.835 | — | 7,131.655 | 7,131.655 | 22937 |
| 0094-tidehunter_efa-1024-offered-10000-primary_rf1-r0 | put/success | 17293 | 561.383 | 172,127.807 | 179,209.933 | 168,081.332 | 7,131.655 | 7,131.655 | 22937 |
| 0094-tidehunter_efa-1024-offered-10000-primary_rf1-r0 | read/rejected | 16033 | — | — | 1,992.043 | — | 7,131.655 | 7,131.655 | 22937 |
| 0094-tidehunter_efa-1024-offered-10000-primary_rf1-r0 | read/success | 39770 | 127.863 | 459.090 | 169,699.725 | 169,470.008 | 7,131.655 | 7,131.655 | 22937 |
| 0149-tidehunter_efa_direct-1024-offered-10000-primary_rf1-r0 | put/rejected | 6356 | — | — | 1,938.650 | — | 7,350.444 | 7,350.444 | 21188 |
| 0149-tidehunter_efa_direct-1024-offered-10000-primary_rf1-r0 | put/success | 17841 | 566.222 | 169,018.894 | 177,873.684 | 167,455.963 | 7,350.444 | 7,350.444 | 21188 |
| 0149-tidehunter_efa_direct-1024-offered-10000-primary_rf1-r0 | read/rejected | 14832 | — | — | 1,939.250 | — | 7,350.444 | 7,350.444 | 21188 |
| 0149-tidehunter_efa_direct-1024-offered-10000-primary_rf1-r0 | read/success | 40971 | 119.182 | 380.339 | 167,404.031 | 167,216.907 | 7,350.444 | 7,350.444 | 21188 |
| 0062-tidehunter_efa_native-1024-offered-10000-primary_rf1-r0 | put/rejected | 6476 | — | — | 1,932.758 | — | 7,289.001 | 7,289.001 | 21682 |
| 0062-tidehunter_efa_native-1024-offered-10000-primary_rf1-r0 | put/success | 17721 | 549.263 | 173,816.993 | 179,813.390 | 169,506.097 | 7,289.001 | 7,289.001 | 21682 |
| 0062-tidehunter_efa_native-1024-offered-10000-primary_rf1-r0 | read/rejected | 15206 | — | — | 1,942.392 | — | 7,289.001 | 7,289.001 | 21682 |
| 0062-tidehunter_efa_native-1024-offered-10000-primary_rf1-r0 | read/success | 40597 | 106.563 | 370.788 | 170,063.305 | 169,905.423 | 7,289.001 | 7,289.001 | 21682 |
| 0215-tidehunter_tcp-1024-offered-10000-primary_rf1-r0 | put/rejected | 6904 | — | — | 1,887.283 | — | 7,130.762 | 7,130.762 | 22942 |
| 0215-tidehunter_tcp-1024-offered-10000-primary_rf1-r0 | put/success | 17293 | 800.588 | 172,749.796 | 179,565.989 | 167,729.628 | 7,130.762 | 7,130.762 | 22942 |
| 0215-tidehunter_tcp-1024-offered-10000-primary_rf1-r0 | read/rejected | 16038 | — | — | 1,888.492 | — | 7,130.762 | 7,130.762 | 22942 |
| 0215-tidehunter_tcp-1024-offered-10000-primary_rf1-r0 | read/success | 39765 | 247.665 | 707.346 | 170,892.441 | 170,501.728 | 7,130.762 | 7,130.762 | 22942 |
| 0124-tikv-1024-offered-10000-primary_rf1-r0 | put/success | 24197 | 786.618 | 1,463.562 | 3,017.732 | 1,982.299 | 9,998.516 | 9,998.516 | 0 |
| 0124-tikv-1024-offered-10000-primary_rf1-r0 | read/success | 55803 | 433.050 | 1,066.274 | 2,622.734 | 1,984.568 | 9,998.516 | 9,998.516 | 0 |
| 0037-valkey_tcp-1024-offered-10000-primary_rf1-r0 | put/success | 24197 | 614.243 | 1,097.485 | 2,647.575 | 1,960.596 | 9,998.236 | 9,998.236 | 0 |
| 0037-valkey_tcp-1024-offered-10000-primary_rf1-r0 | read/success | 55803 | 377.788 | 1,032.702 | 2,483.915 | 1,954.614 | 9,998.236 | 9,998.236 | 0 |

### primary_rf1 / offered-50000 / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0249-cassandra-1024-offered-50000-primary_rf1-r0 | put/rejected | 2 | — | — | 124.005 | — | 49,991.590 | 49,991.590 | 2 |
| 0249-cassandra-1024-offered-50000-primary_rf1-r0 | put/success | 120115 | 521.991 | 882.939 | 2,653.547 | 2,013.292 | 49,991.590 | 49,991.590 | 2 |
| 0249-cassandra-1024-offered-50000-primary_rf1-r0 | read/success | 279883 | 277.137 | 415.129 | 2,313.743 | 2,006.210 | 49,991.590 | 49,991.590 | 2 |
| 0016-foundationdb-1024-offered-50000-primary_rf1-r0 | put/rejected | 51116 | — | — | 1,457.818 | — | 28,680.850 | 28,680.850 | 170396 |
| 0016-foundationdb-1024-offered-50000-primary_rf1-r0 | put/success | 69001 | 1,370.680 | 2,209.878 | 9,174.853 | 7,513.364 | 28,680.850 | 28,680.850 | 170396 |
| 0016-foundationdb-1024-offered-50000-primary_rf1-r0 | read/rejected | 119280 | — | — | 1,454.612 | — | 28,680.850 | 28,680.850 | 170396 |
| 0016-foundationdb-1024-offered-50000-primary_rf1-r0 | read/success | 160603 | 868.449 | 2,776.171 | 8,737.362 | 7,542.717 | 28,680.850 | 28,680.850 | 170396 |
| 0183-native-1024-offered-50000-primary_rf1-r0 | put/rejected | 27680 | — | — | 1,717.641 | — | 38,430.862 | 38,430.862 | 92245 |
| 0183-native-1024-offered-50000-primary_rf1-r0 | put/success | 92437 | 970.551 | 8,673.002 | 16,888.369 | 12,898.532 | 38,430.862 | 38,430.862 | 92245 |
| 0183-native-1024-offered-50000-primary_rf1-r0 | read/rejected | 64565 | — | — | 1,717.220 | — | 38,430.862 | 38,430.862 | 92245 |
| 0183-native-1024-offered-50000-primary_rf1-r0 | read/success | 215318 | 3.140 | 12.140 | 12,865.590 | 12,862.160 | 38,430.862 | 38,430.862 | 92245 |
| 0098-tidehunter_efa-1024-offered-50000-primary_rf1-r0 | put/rejected | 83519 | — | — | 1,952.507 | — | 15,313.180 | 15,313.180 | 276790 |
| 0098-tidehunter_efa-1024-offered-50000-primary_rf1-r0 | put/success | 36598 | 965.352 | 180,952.831 | 195,298.391 | 181,805.534 | 15,313.180 | 15,313.180 | 276790 |
| 0098-tidehunter_efa-1024-offered-50000-primary_rf1-r0 | read/rejected | 193271 | — | — | 1,960.769 | — | 15,313.180 | 15,313.180 | 276790 |
| 0098-tidehunter_efa-1024-offered-50000-primary_rf1-r0 | read/success | 86612 | 124.653 | 492.231 | 182,580.077 | 182,409.053 | 15,313.180 | 15,313.180 | 276790 |
| 0153-tidehunter_efa_direct-1024-offered-50000-primary_rf1-r0 | put/rejected | 83400 | — | — | 1,938.627 | — | 14,703.220 | 14,703.220 | 278135 |
| 0153-tidehunter_efa_direct-1024-offered-50000-primary_rf1-r0 | put/success | 36717 | 966.271 | 182,842.418 | 196,133.331 | 184,080.179 | 14,703.220 | 14,703.220 | 278135 |
| 0153-tidehunter_efa_direct-1024-offered-50000-primary_rf1-r0 | read/rejected | 194735 | — | — | 1,937.722 | — | 14,703.220 | 14,703.220 | 278135 |
| 0153-tidehunter_efa_direct-1024-offered-50000-primary_rf1-r0 | read/success | 85148 | 121.213 | 475.101 | 184,597.771 | 184,405.843 | 14,703.220 | 14,703.220 | 278135 |
| 0066-tidehunter_efa_native-1024-offered-50000-primary_rf1-r0 | put/rejected | 83486 | — | — | 1,954.932 | — | 15,145.969 | 15,145.969 | 277964 |
| 0066-tidehunter_efa_native-1024-offered-50000-primary_rf1-r0 | put/success | 36631 | 988.962 | 182,140.630 | 196,026.408 | 183,158.598 | 15,145.969 | 15,145.969 | 277964 |
| 0066-tidehunter_efa_native-1024-offered-50000-primary_rf1-r0 | read/rejected | 194478 | — | — | 1,953.002 | — | 15,145.969 | 15,145.969 | 277964 |
| 0066-tidehunter_efa_native-1024-offered-50000-primary_rf1-r0 | read/success | 85405 | 114.502 | 449.990 | 183,908.097 | 183,719.879 | 15,145.969 | 15,145.969 | 277964 |
| 0219-tidehunter_tcp-1024-offered-50000-primary_rf1-r0 | put/rejected | 83525 | — | — | 10,998.415 | — | 14,809.389 | 14,809.389 | 277526 |
| 0219-tidehunter_tcp-1024-offered-50000-primary_rf1-r0 | put/success | 36592 | 999.822 | 182,146.227 | 195,786.310 | 183,670.214 | 14,809.389 | 14,809.389 | 277526 |
| 0219-tidehunter_tcp-1024-offered-50000-primary_rf1-r0 | read/rejected | 194001 | — | — | 10,926.845 | — | 14,809.389 | 14,809.389 | 277526 |
| 0219-tidehunter_tcp-1024-offered-50000-primary_rf1-r0 | read/success | 85882 | 220.355 | 848.759 | 183,761.417 | 183,333.966 | 14,809.389 | 14,809.389 | 277526 |
| 0128-tikv-1024-offered-50000-primary_rf1-r0 | put/rejected | 39651 | — | — | 807.628 | — | 33,434.346 | 33,434.346 | 132358 |
| 0128-tikv-1024-offered-50000-primary_rf1-r0 | put/success | 80466 | 1,153.265 | 1,860.591 | 6,714.996 | 5,431.286 | 33,434.346 | 33,434.346 | 132358 |
| 0128-tikv-1024-offered-50000-primary_rf1-r0 | read/rejected | 92707 | — | — | 805.072 | — | 33,434.346 | 33,434.346 | 132358 |
| 0128-tikv-1024-offered-50000-primary_rf1-r0 | read/success | 187176 | 844.729 | 1,478.963 | 6,391.435 | 5,429.183 | 33,434.346 | 33,434.346 | 132358 |
| 0041-valkey_tcp-1024-offered-50000-primary_rf1-r0 | put/success | 120117 | 486.881 | 913.431 | 2,580.177 | 1,970.130 | 49,983.971 | 49,983.971 | 0 |
| 0041-valkey_tcp-1024-offered-50000-primary_rf1-r0 | read/success | 279883 | 395.199 | 869.499 | 2,512.102 | 1,974.589 | 49,983.971 | 49,983.971 | 0 |

### primary_rf1 / point-mixed / 64 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0237-cassandra-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 473.950 | 776.427 | 787.847 | 50.531 | 2,105.881 | 2,105.881 | 0 |
| 0237-cassandra-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 321.557 | 454.050 | 483.810 | 50.391 | 2,105.881 | 2,105.881 | 0 |
| 0237-cassandra-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 481.660 | 915.410 | 946.641 | 55.751 | 2,105.881 | 2,105.881 | 0 |
| 0237-cassandra-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 333.907 | 473.261 | 506.981 | 52.001 | 2,105.881 | 2,105.881 | 0 |
| 0004-foundationdb-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 1,138.144 | 1,930.943 | 1,958.113 | 45.081 | 710.359 | 710.359 | 0 |
| 0004-foundationdb-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 681.185 | 4,795.935 | 4,810.636 | 43.001 | 710.359 | 710.359 | 0 |
| 0004-foundationdb-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,126.405 | 1,857.021 | 1,887.102 | 44.831 | 710.359 | 710.359 | 0 |
| 0004-foundationdb-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 732.466 | 4,987.479 | 5,013.650 | 40.930 | 710.359 | 710.359 | 0 |
| 0171-native-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 130.233 | 298.227 | 313.118 | 26.531 | 8,721.761 | 8,721.761 | 0 |
| 0171-native-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.160 | 3.150 | 30.140 | 26.540 | 8,721.761 | 8,721.761 | 0 |
| 0171-native-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 131.163 | 359.448 | 394.298 | 29.361 | 8,721.761 | 8,721.761 | 0 |
| 0171-native-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 2.220 | 6.620 | 35.341 | 29.290 | 8,721.761 | 8,721.761 | 0 |
| 0086-tidehunter_efa-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 320.007 | 489.771 | 510.911 | 34.051 | 3,034.127 | 3,034.127 | 0 |
| 0086-tidehunter_efa-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.160 | 8.440 | 30.450 | 28.520 | 3,034.127 | 3,034.127 | 0 |
| 0086-tidehunter_efa-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 320.817 | 472.531 | 499.641 | 29.770 | 3,034.127 | 3,034.127 | 0 |
| 0086-tidehunter_efa-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 91.782 | 167.764 | 184.935 | 31.291 | 3,034.127 | 3,034.127 | 0 |
| 0141-tidehunter_efa_direct-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 321.567 | 438.690 | 462.550 | 26.051 | 3,064.702 | 3,064.702 | 0 |
| 0141-tidehunter_efa_direct-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.040 | 7.290 | 28.931 | 26.801 | 3,064.702 | 3,064.702 | 0 |
| 0141-tidehunter_efa_direct-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 323.336 | 485.941 | 510.142 | 29.060 | 3,064.702 | 3,064.702 | 0 |
| 0141-tidehunter_efa_direct-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 83.492 | 157.764 | 175.664 | 29.281 | 3,064.702 | 3,064.702 | 0 |
| 0054-tidehunter_efa_native-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 297.907 | 383.649 | 392.769 | 13.330 | 3,197.532 | 3,197.532 | 0 |
| 0054-tidehunter_efa_native-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 0.940 | 6.250 | 18.891 | 14.081 | 3,197.532 | 3,197.532 | 0 |
| 0054-tidehunter_efa_native-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 301.227 | 387.829 | 398.639 | 22.981 | 3,197.532 | 3,197.532 | 0 |
| 0054-tidehunter_efa_native-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 78.231 | 154.644 | 166.983 | 22.120 | 3,197.532 | 3,197.532 | 0 |
| 0207-tidehunter_tcp-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 547.313 | 690.605 | 707.945 | 27.860 | 1,941.092 | 1,941.092 | 0 |
| 0207-tidehunter_tcp-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.110 | 9.901 | 32.730 | 26.461 | 1,941.092 | 1,941.092 | 0 |
| 0207-tidehunter_tcp-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 556.482 | 746.517 | 777.157 | 29.901 | 1,941.092 | 1,941.092 | 0 |
| 0207-tidehunter_tcp-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 247.665 | 424.350 | 437.569 | 30.530 | 1,941.092 | 1,941.092 | 0 |
| 0116-tikv-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 790.987 | 1,092.204 | 1,139.275 | 50.261 | 1,488.445 | 1,488.445 | 0 |
| 0116-tikv-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 424.280 | 652.735 | 675.355 | 34.460 | 1,488.445 | 1,488.445 | 0 |
| 0116-tikv-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 793.408 | 1,195.656 | 1,223.567 | 44.301 | 1,488.445 | 1,488.445 | 0 |
| 0116-tikv-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 424.220 | 630.334 | 658.095 | 48.601 | 1,488.445 | 1,488.445 | 0 |
| 0029-valkey_tcp-64-point-mixed-primary_rf1-r0 | delete/success | 391 | 518.562 | 737.186 | 759.217 | 50.801 | 2,264.514 | 2,264.514 | 0 |
| 0029-valkey_tcp-64-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 247.836 | 351.498 | 382.828 | 51.162 | 2,264.514 | 2,264.514 | 0 |
| 0029-valkey_tcp-64-point-mixed-primary_rf1-r0 | put/success | 3775 | 522.061 | 697.866 | 724.795 | 52.041 | 2,264.514 | 2,264.514 | 0 |
| 0029-valkey_tcp-64-point-mixed-primary_rf1-r0 | read/success | 3621 | 249.766 | 363.988 | 390.509 | 55.251 | 2,264.514 | 2,264.514 | 0 |

### primary_rf1 / point-mixed / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0250-cassandra-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 468.781 | 861.649 | 902.380 | 55.471 | 2,093.829 | 2,093.829 | 0 |
| 0250-cassandra-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 335.717 | 474.550 | 506.171 | 61.611 | 2,093.829 | 2,093.829 | 0 |
| 0250-cassandra-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 474.990 | 895.680 | 925.241 | 55.381 | 2,093.829 | 2,093.829 | 0 |
| 0250-cassandra-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 347.428 | 488.231 | 515.962 | 55.051 | 2,093.829 | 2,093.829 | 0 |
| 0017-foundationdb-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 1,030.053 | 1,616.765 | 1,644.546 | 53.181 | 703.236 | 703.236 | 0 |
| 0017-foundationdb-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 804.648 | 4,994.990 | 5,011.850 | 50.251 | 703.236 | 703.236 | 0 |
| 0017-foundationdb-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,017.712 | 1,672.337 | 1,697.487 | 50.151 | 703.236 | 703.236 | 0 |
| 0017-foundationdb-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 780.297 | 5,065.011 | 5,077.931 | 45.421 | 703.236 | 703.236 | 0 |
| 0184-native-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 125.083 | 286.516 | 298.956 | 25.951 | 10,277.600 | 10,277.600 | 0 |
| 0184-native-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.089 | 2.260 | 22.260 | 20.950 | 10,277.600 | 10,277.600 | 0 |
| 0184-native-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 124.252 | 287.996 | 299.527 | 22.350 | 10,277.600 | 10,277.600 | 0 |
| 0184-native-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 1.980 | 3.780 | 24.790 | 22.920 | 10,277.600 | 10,277.600 | 0 |
| 0099-tidehunter_efa-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 374.409 | 531.632 | 546.722 | 30.760 | 2,692.878 | 2,692.878 | 0 |
| 0099-tidehunter_efa-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.360 | 7.720 | 31.850 | 29.931 | 2,692.878 | 2,692.878 | 0 |
| 0099-tidehunter_efa-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 377.639 | 537.851 | 565.612 | 33.941 | 2,692.878 | 2,692.878 | 0 |
| 0099-tidehunter_efa-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 95.002 | 180.084 | 205.574 | 32.741 | 2,692.878 | 2,692.878 | 0 |
| 0154-tidehunter_efa_direct-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 350.528 | 453.581 | 548.162 | 24.201 | 2,932.085 | 2,932.085 | 0 |
| 0154-tidehunter_efa_direct-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 0.990 | 2.210 | 21.450 | 15.410 | 2,932.085 | 2,932.085 | 0 |
| 0154-tidehunter_efa_direct-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 353.798 | 480.171 | 503.021 | 24.930 | 2,932.085 | 2,932.085 | 0 |
| 0154-tidehunter_efa_direct-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 82.742 | 164.864 | 176.064 | 23.031 | 2,932.085 | 2,932.085 | 0 |
| 0067-tidehunter_efa_native-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 360.768 | 494.351 | 510.222 | 25.250 | 2,814.571 | 2,814.571 | 0 |
| 0067-tidehunter_efa_native-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.040 | 9.170 | 30.051 | 26.481 | 2,814.571 | 2,814.571 | 0 |
| 0067-tidehunter_efa_native-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 365.908 | 516.831 | 538.532 | 27.111 | 2,814.571 | 2,814.571 | 0 |
| 0067-tidehunter_efa_native-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 81.822 | 160.984 | 183.674 | 28.181 | 2,814.571 | 2,814.571 | 0 |
| 0220-tidehunter_tcp-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 587.363 | 719.607 | 737.207 | 28.980 | 1,882.641 | 1,882.641 | 0 |
| 0220-tidehunter_tcp-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.230 | 8.540 | 29.321 | 27.041 | 1,882.641 | 1,882.641 | 0 |
| 0220-tidehunter_tcp-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 594.763 | 738.006 | 758.146 | 28.081 | 1,882.641 | 1,882.641 | 0 |
| 0220-tidehunter_tcp-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 243.245 | 414.059 | 428.980 | 29.281 | 1,882.641 | 1,882.641 | 0 |
| 0129-tikv-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 769.887 | 1,183.325 | 1,205.126 | 30.421 | 1,547.606 | 1,547.606 | 0 |
| 0129-tikv-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 423.709 | 635.804 | 661.795 | 41.531 | 1,547.606 | 1,547.606 | 0 |
| 0129-tikv-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 784.707 | 1,183.355 | 1,214.796 | 39.091 | 1,547.606 | 1,547.606 | 0 |
| 0129-tikv-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 412.958 | 623.924 | 651.414 | 40.671 | 1,547.606 | 1,547.606 | 0 |
| 0042-valkey_tcp-256-point-mixed-primary_rf1-r0 | delete/success | 391 | 530.102 | 682.195 | 712.535 | 49.791 | 2,258.587 | 2,258.587 | 0 |
| 0042-valkey_tcp-256-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 244.826 | 355.368 | 394.639 | 48.261 | 2,258.587 | 2,258.587 | 0 |
| 0042-valkey_tcp-256-point-mixed-primary_rf1-r0 | put/success | 3775 | 528.801 | 710.215 | 739.846 | 52.981 | 2,258.587 | 2,258.587 | 0 |
| 0042-valkey_tcp-256-point-mixed-primary_rf1-r0 | read/success | 3621 | 244.595 | 361.828 | 387.728 | 52.701 | 2,258.587 | 2,258.587 | 0 |

### primary_rf1 / point-mixed / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0248-cassandra-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 438.599 | 683.045 | 711.486 | 54.351 | 2,063.572 | 2,063.572 | 0 |
| 0248-cassandra-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 331.417 | 483.251 | 508.191 | 39.181 | 2,063.572 | 2,063.572 | 0 |
| 0248-cassandra-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 499.271 | 942.891 | 977.062 | 51.501 | 2,063.572 | 2,063.572 | 0 |
| 0248-cassandra-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 335.788 | 488.451 | 519.762 | 52.361 | 2,063.572 | 2,063.572 | 0 |
| 0015-foundationdb-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 1,012.492 | 1,626.726 | 1,665.216 | 39.491 | 703.713 | 703.713 | 0 |
| 0015-foundationdb-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 791.107 | 5,284.106 | 5,300.456 | 36.061 | 703.713 | 703.713 | 0 |
| 0015-foundationdb-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,014.542 | 1,598.965 | 1,635.385 | 50.641 | 703.713 | 703.713 | 0 |
| 0015-foundationdb-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 769.457 | 5,198.123 | 5,216.624 | 44.031 | 703.713 | 703.713 | 0 |
| 0182-native-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 122.703 | 240.145 | 257.546 | 26.420 | 10,154.155 | 10,154.155 | 0 |
| 0182-native-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.140 | 2.260 | 20.990 | 20.130 | 10,154.155 | 10,154.155 | 0 |
| 0182-native-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 123.963 | 283.287 | 294.046 | 25.620 | 10,154.155 | 10,154.155 | 0 |
| 0182-native-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 2.220 | 4.090 | 27.761 | 24.771 | 10,154.155 | 10,154.155 | 0 |
| 0097-tidehunter_efa-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 365.508 | 560.762 | 584.343 | 50.331 | 2,685.499 | 2,685.499 | 0 |
| 0097-tidehunter_efa-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.320 | 5.870 | 35.421 | 31.451 | 2,685.499 | 2,685.499 | 0 |
| 0097-tidehunter_efa-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 382.898 | 660.184 | 675.665 | 35.301 | 2,685.499 | 2,685.499 | 0 |
| 0097-tidehunter_efa-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 97.412 | 177.783 | 213.645 | 37.161 | 2,685.499 | 2,685.499 | 0 |
| 0152-tidehunter_efa_direct-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 348.498 | 511.831 | 532.011 | 25.521 | 2,845.462 | 2,845.462 | 0 |
| 0152-tidehunter_efa_direct-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.130 | 7.820 | 29.781 | 27.440 | 2,845.462 | 2,845.462 | 0 |
| 0152-tidehunter_efa_direct-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 360.588 | 626.144 | 643.894 | 31.230 | 2,845.462 | 2,845.462 | 0 |
| 0152-tidehunter_efa_direct-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 91.102 | 165.313 | 188.985 | 30.871 | 2,845.462 | 2,845.462 | 0 |
| 0065-tidehunter_efa_native-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 361.018 | 537.411 | 549.501 | 31.411 | 2,772.484 | 2,772.484 | 0 |
| 0065-tidehunter_efa_native-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.140 | 4.249 | 33.711 | 31.861 | 2,772.484 | 2,772.484 | 0 |
| 0065-tidehunter_efa_native-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 370.768 | 642.185 | 654.555 | 33.311 | 2,772.484 | 2,772.484 | 0 |
| 0065-tidehunter_efa_native-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 86.421 | 161.123 | 184.164 | 32.361 | 2,772.484 | 2,772.484 | 0 |
| 0218-tidehunter_tcp-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 563.292 | 771.707 | 785.838 | 29.011 | 1,899.267 | 1,899.267 | 0 |
| 0218-tidehunter_tcp-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.250 | 3.870 | 28.120 | 25.880 | 1,899.267 | 1,899.267 | 0 |
| 0218-tidehunter_tcp-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 588.173 | 975.491 | 993.752 | 30.161 | 1,899.267 | 1,899.267 | 0 |
| 0218-tidehunter_tcp-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 232.376 | 416.419 | 430.809 | 30.640 | 1,899.267 | 1,899.267 | 0 |
| 0127-tikv-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 790.948 | 1,126.064 | 1,142.535 | 33.340 | 1,424.597 | 1,424.597 | 0 |
| 0127-tikv-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 425.359 | 639.784 | 671.515 | 35.570 | 1,424.597 | 1,424.597 | 0 |
| 0127-tikv-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 845.219 | 1,276.688 | 1,304.399 | 38.400 | 1,424.597 | 1,424.597 | 0 |
| 0127-tikv-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 410.539 | 626.503 | 658.564 | 37.491 | 1,424.597 | 1,424.597 | 0 |
| 0040-valkey_tcp-1024-point-mixed-primary_rf1-r0 | delete/success | 391 | 543.522 | 684.215 | 742.886 | 49.352 | 2,116.060 | 2,116.060 | 0 |
| 0040-valkey_tcp-1024-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 257.806 | 344.987 | 368.988 | 35.010 | 2,116.060 | 2,116.060 | 0 |
| 0040-valkey_tcp-1024-point-mixed-primary_rf1-r0 | put/success | 3775 | 563.612 | 753.226 | 789.097 | 59.871 | 2,116.060 | 2,116.060 | 0 |
| 0040-valkey_tcp-1024-point-mixed-primary_rf1-r0 | read/success | 3621 | 259.706 | 364.858 | 392.709 | 59.561 | 2,116.060 | 2,116.060 | 0 |

### primary_rf1 / point-mixed / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0256-cassandra-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 444.100 | 671.146 | 694.476 | 34.721 | 1,698.117 | 1,698.117 | 0 |
| 0256-cassandra-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 320.687 | 467.130 | 490.801 | 44.011 | 1,698.117 | 1,698.117 | 0 |
| 0256-cassandra-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 713.386 | 1,017.462 | 1,056.884 | 56.301 | 1,698.117 | 1,698.117 | 0 |
| 0256-cassandra-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 352.148 | 519.721 | 554.223 | 52.791 | 1,698.117 | 1,698.117 | 0 |
| 0023-foundationdb-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 1,007.352 | 1,610.015 | 1,674.106 | 50.271 | 705.844 | 705.844 | 0 |
| 0023-foundationdb-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 790.258 | 5,123.402 | 5,140.743 | 42.360 | 705.844 | 705.844 | 0 |
| 0023-foundationdb-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,017.632 | 1,680.097 | 1,721.467 | 54.211 | 705.844 | 705.844 | 0 |
| 0023-foundationdb-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 779.057 | 5,130.042 | 5,146.723 | 57.491 | 705.844 | 705.844 | 0 |
| 0190-native-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 134.463 | 360.318 | 385.549 | 30.170 | 7,091.691 | 7,091.691 | 0 |
| 0190-native-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.230 | 3.930 | 38.740 | 34.551 | 7,091.691 | 7,091.691 | 0 |
| 0190-native-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 151.784 | 427.369 | 457.700 | 38.421 | 7,091.691 | 7,091.691 | 0 |
| 0190-native-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 4.490 | 16.220 | 49.471 | 37.151 | 7,091.691 | 7,091.691 | 0 |
| 0105-tidehunter_efa-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 342.058 | 486.981 | 553.023 | 25.661 | 2,558.191 | 2,558.191 | 0 |
| 0105-tidehunter_efa-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.150 | 7.320 | 19.091 | 17.730 | 2,558.191 | 2,558.191 | 0 |
| 0105-tidehunter_efa-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 416.139 | 708.056 | 721.566 | 31.911 | 2,558.191 | 2,558.191 | 0 |
| 0105-tidehunter_efa-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 97.802 | 181.854 | 198.215 | 30.991 | 2,558.191 | 2,558.191 | 0 |
| 0160-tidehunter_efa_direct-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 347.148 | 492.751 | 531.022 | 30.171 | 2,494.125 | 2,494.125 | 0 |
| 0160-tidehunter_efa_direct-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.070 | 3.360 | 32.371 | 28.740 | 2,494.125 | 2,494.125 | 0 |
| 0160-tidehunter_efa_direct-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 426.069 | 746.337 | 783.667 | 39.730 | 2,494.125 | 2,494.125 | 0 |
| 0160-tidehunter_efa_direct-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 99.932 | 188.034 | 216.865 | 37.881 | 2,494.125 | 2,494.125 | 0 |
| 0073-tidehunter_efa_native-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 336.517 | 510.482 | 534.352 | 24.900 | 2,622.436 | 2,622.436 | 0 |
| 0073-tidehunter_efa_native-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.090 | 7.020 | 23.270 | 20.601 | 2,622.436 | 2,622.436 | 0 |
| 0073-tidehunter_efa_native-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 408.849 | 691.835 | 704.496 | 31.131 | 2,622.436 | 2,622.436 | 0 |
| 0073-tidehunter_efa_native-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 87.622 | 160.713 | 179.664 | 30.980 | 2,622.436 | 2,622.436 | 0 |
| 0226-tidehunter_tcp-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 568.482 | 707.046 | 727.056 | 30.781 | 1,663.940 | 1,663.940 | 0 |
| 0226-tidehunter_tcp-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.450 | 8.340 | 30.300 | 28.310 | 1,663.940 | 1,663.940 | 0 |
| 0226-tidehunter_tcp-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 690.115 | 1,141.315 | 1,158.915 | 36.400 | 1,663.940 | 1,663.940 | 0 |
| 0226-tidehunter_tcp-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 266.466 | 447.350 | 466.520 | 36.351 | 1,663.940 | 1,663.940 | 0 |
| 0135-tikv-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 800.168 | 1,196.526 | 1,211.396 | 39.610 | 1,195.866 | 1,195.866 | 0 |
| 0135-tikv-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 423.559 | 629.003 | 655.524 | 36.520 | 1,195.866 | 1,195.866 | 0 |
| 0135-tikv-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,158.615 | 1,421.121 | 1,461.712 | 57.371 | 1,195.866 | 1,195.866 | 0 |
| 0135-tikv-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 427.839 | 655.485 | 694.405 | 56.421 | 1,195.866 | 1,195.866 | 0 |
| 0048-valkey_tcp-8192-point-mixed-primary_rf1-r0 | delete/success | 391 | 529.852 | 721.296 | 742.476 | 55.441 | 1,910.749 | 1,910.749 | 0 |
| 0048-valkey_tcp-8192-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 243.805 | 338.087 | 370.778 | 53.191 | 1,910.749 | 1,910.749 | 0 |
| 0048-valkey_tcp-8192-point-mixed-primary_rf1-r0 | put/success | 3775 | 683.815 | 838.148 | 875.489 | 64.862 | 1,910.749 | 1,910.749 | 0 |
| 0048-valkey_tcp-8192-point-mixed-primary_rf1-r0 | read/success | 3621 | 267.786 | 383.449 | 419.949 | 66.081 | 1,910.749 | 1,910.749 | 0 |

### primary_rf1 / point-mixed / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0247-cassandra-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 484.501 | 786.238 | 811.638 | 39.561 | 1,202.286 | 1,202.286 | 0 |
| 0247-cassandra-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 355.757 | 499.081 | 535.482 | 55.411 | 1,202.286 | 1,202.286 | 0 |
| 0247-cassandra-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,022.953 | 1,410.182 | 1,486.984 | 124.963 | 1,202.286 | 1,202.286 | 0 |
| 0247-cassandra-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 473.680 | 691.256 | 780.118 | 123.743 | 1,202.286 | 1,202.286 | 0 |
| 0014-foundationdb-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 984.231 | 1,545.644 | 1,699.127 | 64.162 | 678.413 | 678.413 | 0 |
| 0014-foundationdb-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 753.036 | 5,139.902 | 5,154.213 | 35.181 | 678.413 | 678.413 | 0 |
| 0014-foundationdb-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,090.434 | 1,904.042 | 1,994.514 | 109.742 | 678.413 | 678.413 | 0 |
| 0014-foundationdb-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 832.478 | 5,281.766 | 5,320.676 | 109.062 | 678.413 | 678.413 | 0 |
| 0181-native-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 251.746 | 498.101 | 528.482 | 36.951 | 3,445.097 | 3,445.097 | 0 |
| 0181-native-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.580 | 3.180 | 36.311 | 33.711 | 3,445.097 | 3,445.097 | 0 |
| 0181-native-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 438.390 | 842.529 | 924.161 | 89.352 | 3,445.097 | 3,445.097 | 0 |
| 0181-native-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 15.481 | 36.591 | 124.542 | 90.132 | 3,445.097 | 3,445.097 | 0 |
| 0096-tidehunter_efa-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 360.098 | 434.129 | 451.470 | 23.500 | 1,471.143 | 1,471.143 | 0 |
| 0096-tidehunter_efa-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.230 | 3.200 | 26.671 | 23.360 | 1,471.143 | 1,471.143 | 0 |
| 0096-tidehunter_efa-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,016.762 | 1,276.277 | 1,321.009 | 75.892 | 1,471.143 | 1,471.143 | 0 |
| 0096-tidehunter_efa-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 155.753 | 241.805 | 305.666 | 77.051 | 1,471.143 | 1,471.143 | 0 |
| 0151-tidehunter_efa_direct-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 363.609 | 496.012 | 525.522 | 30.020 | 1,369.763 | 1,369.763 | 0 |
| 0151-tidehunter_efa_direct-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.320 | 8.340 | 29.901 | 27.511 | 1,369.763 | 1,369.763 | 0 |
| 0151-tidehunter_efa_direct-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,050.034 | 1,332.350 | 1,441.182 | 87.891 | 1,369.763 | 1,369.763 | 0 |
| 0151-tidehunter_efa_direct-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 209.235 | 293.757 | 367.878 | 88.052 | 1,369.763 | 1,369.763 | 0 |
| 0064-tidehunter_efa_native-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 353.517 | 507.631 | 528.312 | 26.180 | 1,397.641 | 1,397.641 | 0 |
| 0064-tidehunter_efa_native-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.220 | 3.470 | 28.021 | 25.680 | 1,397.641 | 1,397.641 | 0 |
| 0064-tidehunter_efa_native-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,048.883 | 1,292.489 | 1,349.130 | 80.932 | 1,397.641 | 1,397.641 | 0 |
| 0064-tidehunter_efa_native-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 191.074 | 270.726 | 326.277 | 79.592 | 1,397.641 | 1,397.641 | 0 |
| 0217-tidehunter_tcp-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 595.104 | 752.177 | 769.487 | 32.761 | 990.904 | 990.904 | 0 |
| 0217-tidehunter_tcp-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 1.530 | 3.240 | 32.341 | 29.821 | 990.904 | 990.904 | 0 |
| 0217-tidehunter_tcp-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,399.041 | 1,875.452 | 1,939.533 | 80.481 | 990.904 | 990.904 | 0 |
| 0217-tidehunter_tcp-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 412.880 | 627.444 | 672.035 | 81.921 | 990.904 | 990.904 | 0 |
| 0126-tikv-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 815.358 | 1,190.836 | 1,224.887 | 47.191 | 920.733 | 920.733 | 0 |
| 0126-tikv-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 452.920 | 695.875 | 724.296 | 45.311 | 920.733 | 920.733 | 0 |
| 0126-tikv-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 1,444.832 | 1,847.481 | 1,933.953 | 110.963 | 920.733 | 920.733 | 0 |
| 0126-tikv-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 617.293 | 974.501 | 1,060.003 | 110.072 | 920.733 | 920.733 | 0 |
| 0039-valkey_tcp-65536-point-mixed-primary_rf1-r0 | delete/success | 391 | 513.641 | 762.277 | 784.667 | 50.871 | 1,600.687 | 1,600.687 | 0 |
| 0039-valkey_tcp-65536-point-mixed-primary_rf1-r0 | missing_read/success | 405 | 206.214 | 334.377 | 361.848 | 32.410 | 1,600.687 | 1,600.687 | 0 |
| 0039-valkey_tcp-65536-point-mixed-primary_rf1-r0 | put/success | 3775 | 827.018 | 1,087.964 | 1,163.405 | 105.963 | 1,600.687 | 1,600.687 | 0 |
| 0039-valkey_tcp-65536-point-mixed-primary_rf1-r0 | read/success | 3621 | 290.616 | 498.881 | 580.553 | 106.062 | 1,600.687 | 1,600.687 | 0 |

### primary_rf1 / read-concurrent / 64 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0254-cassandra-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 233.385 | 349.328 | 366.118 | 34.810 | 58,746.554 | 58,746.554 | 0 |
| 0021-foundationdb-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 604.533 | 844.479 | 860.149 | 32.161 | 24,596.603 | 24,596.603 | 0 |
| 0188-native-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 1.890 | 3.110 | 25.940 | 23.470 | 133,445.126 | 133,445.126 | 0 |
| 0103-tidehunter_efa-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 102.452 | 354.318 | 370.318 | 29.311 | 87,409.077 | 87,409.077 | 0 |
| 0158-tidehunter_efa_direct-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 95.582 | 335.588 | 352.178 | 28.501 | 105,190.550 | 105,190.550 | 0 |
| 0071-tidehunter_efa_native-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 90.682 | 314.447 | 328.837 | 28.051 | 111,274.759 | 111,274.759 | 0 |
| 0224-tidehunter_tcp-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 188.304 | 710.926 | 724.626 | 36.071 | 53,630.294 | 53,630.294 | 0 |
| 0133-tikv-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 366.118 | 601.493 | 613.884 | 28.001 | 39,800.329 | 39,800.329 | 0 |
| 0046-valkey_tcp-64-read-concurrent-primary_rf1-r0 | read/success | 8192 | 197.094 | 328.257 | 344.878 | 31.090 | 64,689.927 | 64,689.927 | 0 |

### primary_rf1 / read-concurrent / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0241-cassandra-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 244.936 | 383.988 | 402.829 | 32.891 | 48,470.397 | 48,470.397 | 0 |
| 0008-foundationdb-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 595.573 | 867.449 | 881.779 | 27.990 | 24,752.079 | 24,752.079 | 0 |
| 0175-native-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 1.820 | 3.250 | 26.570 | 23.660 | 161,880.012 | 161,880.012 | 0 |
| 0090-tidehunter_efa-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 104.122 | 335.137 | 348.707 | 27.440 | 105,086.556 | 105,086.556 | 0 |
| 0145-tidehunter_efa_direct-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 96.882 | 341.738 | 357.278 | 28.811 | 103,263.733 | 103,263.733 | 0 |
| 0058-tidehunter_efa_native-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 92.122 | 319.827 | 331.668 | 28.571 | 107,847.720 | 107,847.720 | 0 |
| 0211-tidehunter_tcp-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 197.664 | 649.185 | 666.294 | 35.071 | 53,837.525 | 53,837.525 | 0 |
| 0120-tikv-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 335.708 | 619.463 | 633.834 | 32.031 | 41,807.093 | 41,807.093 | 0 |
| 0033-valkey_tcp-256-read-concurrent-primary_rf1-r0 | read/success | 8192 | 211.415 | 349.858 | 364.018 | 28.561 | 63,849.330 | 63,849.330 | 0 |

### primary_rf1 / read-concurrent / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0246-cassandra-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 240.565 | 372.599 | 391.159 | 36.811 | 48,867.619 | 48,867.619 | 0 |
| 0013-foundationdb-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 627.744 | 879.499 | 893.350 | 31.391 | 23,803.976 | 23,803.976 | 0 |
| 0180-native-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 1.920 | 3.811 | 27.781 | 25.130 | 135,066.805 | 135,066.805 | 0 |
| 0095-tidehunter_efa-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 97.943 | 354.527 | 370.788 | 29.460 | 94,740.062 | 94,740.062 | 0 |
| 0150-tidehunter_efa_direct-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 100.603 | 354.068 | 371.738 | 31.001 | 89,380.143 | 89,380.143 | 0 |
| 0063-tidehunter_efa_native-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 94.322 | 335.568 | 349.458 | 29.290 | 97,844.108 | 97,844.108 | 0 |
| 0216-tidehunter_tcp-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 199.905 | 751.526 | 774.987 | 37.211 | 55,548.209 | 55,548.209 | 0 |
| 0125-tikv-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 393.509 | 684.095 | 700.316 | 32.260 | 36,939.263 | 36,939.263 | 0 |
| 0038-valkey_tcp-1024-read-concurrent-primary_rf1-r0 | read/success | 8192 | 211.905 | 354.038 | 368.598 | 33.720 | 63,251.489 | 63,251.489 | 0 |

### primary_rf1 / read-concurrent / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0243-cassandra-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 271.695 | 416.229 | 434.640 | 37.881 | 51,281.806 | 51,281.806 | 0 |
| 0010-foundationdb-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 691.175 | 994.162 | 1,010.102 | 34.181 | 21,705.216 | 21,705.216 | 0 |
| 0177-native-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 3.740 | 8.390 | 34.770 | 29.551 | 113,170.261 | 113,170.261 | 0 |
| 0092-tidehunter_efa-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 120.433 | 415.749 | 444.639 | 38.401 | 72,261.518 | 72,261.518 | 0 |
| 0147-tidehunter_efa_direct-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 116.503 | 397.269 | 416.439 | 32.410 | 90,663.028 | 90,663.028 | 0 |
| 0060-tidehunter_efa_native-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 104.983 | 363.208 | 381.818 | 31.920 | 98,503.070 | 98,503.070 | 0 |
| 0213-tidehunter_tcp-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 217.604 | 776.447 | 798.757 | 35.691 | 48,686.142 | 48,686.142 | 0 |
| 0122-tikv-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 443.980 | 880.769 | 897.910 | 34.911 | 31,863.923 | 31,863.923 | 0 |
| 0035-valkey_tcp-8192-read-concurrent-primary_rf1-r0 | read/success | 8192 | 234.195 | 409.109 | 426.420 | 43.491 | 56,874.810 | 56,874.810 | 0 |

### primary_rf1 / read-concurrent / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0236-cassandra-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 387.238 | 609.794 | 664.145 | 109.733 | 33,589.902 | 33,589.902 | 0 |
| 0003-foundationdb-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 988.562 | 1,519.713 | 1,562.344 | 59.072 | 14,829.300 | 14,829.300 | 0 |
| 0170-native-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 14.940 | 32.451 | 76.542 | 55.021 | 158,616.431 | 158,616.431 | 0 |
| 0085-tidehunter_efa-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 211.714 | 880.339 | 935.311 | 57.741 | 40,607.549 | 40,607.549 | 0 |
| 0140-tidehunter_efa_direct-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 250.356 | 1,270.488 | 1,325.669 | 61.362 | 34,138.754 | 34,138.754 | 0 |
| 0053-tidehunter_efa_native-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 234.566 | 923.071 | 967.672 | 56.631 | 39,654.277 | 39,654.277 | 0 |
| 0206-tidehunter_tcp-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 378.968 | 1,391.911 | 1,454.302 | 136.723 | 27,149.969 | 27,149.969 | 0 |
| 0115-tikv-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 597.983 | 1,155.246 | 1,197.917 | 106.582 | 23,441.170 | 23,441.170 | 0 |
| 0028-valkey_tcp-65536-read-concurrent-primary_rf1-r0 | read/success | 8192 | 352.338 | 709.675 | 750.046 | 106.492 | 36,554.356 | 36,554.356 | 0 |

### primary_rf1 / read-many32 / 64 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0255-cassandra-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 2,540.077 | 3,482.207 | 3,516.198 | 60.741 | 5,895.898 | 188,668.741 | 0 |
| 0022-foundationdb-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 9,612.531 | 10,906.499 | 10,992.711 | 64.762 | 1,649.606 | 52,787.406 | 0 |
| 0189-native-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 54.211 | 90.582 | 143.673 | 57.012 | 123,390.910 | 3,948,509.118 | 0 |
| 0104-tidehunter_efa-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,264.128 | 1,602.515 | 1,658.166 | 112.132 | 12,021.732 | 384,695.417 | 0 |
| 0159-tidehunter_efa_direct-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,457.942 | 1,731.629 | 1,771.550 | 65.952 | 10,516.495 | 336,527.832 | 0 |
| 0072-tidehunter_efa_native-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,190.626 | 1,605.436 | 1,641.076 | 63.191 | 12,609.840 | 403,514.870 | 0 |
| 0225-tidehunter_tcp-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,522.715 | 2,084.728 | 2,127.959 | 79.852 | 9,669.990 | 309,439.664 | 0 |
| 0134-tikv-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 7,622.797 | 11,724.796 | 11,754.697 | 66.931 | 2,104.525 | 67,344.793 | 0 |
| 0047-valkey_tcp-64-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,726.288 | 5,309.346 | 5,348.467 | 100.372 | 7,886.193 | 252,358.179 | 0 |

### primary_rf1 / read-many32 / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0235-cassandra-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 3,235.072 | 3,876.276 | 3,912.287 | 62.052 | 5,164.683 | 165,269.871 | 0 |
| 0002-foundationdb-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 9,508.118 | 10,587.181 | 10,732.654 | 93.232 | 1,585.215 | 50,726.884 | 0 |
| 0169-native-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 61.891 | 108.002 | 162.053 | 67.432 | 128,971.218 | 4,127,078.968 | 0 |
| 0084-tidehunter_efa-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,495.302 | 1,726.518 | 1,764.418 | 63.632 | 10,132.078 | 324,226.487 | 0 |
| 0139-tidehunter_efa_direct-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,299.009 | 1,614.576 | 1,671.157 | 92.652 | 11,699.812 | 374,393.990 | 0 |
| 0052-tidehunter_efa_native-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,350.819 | 1,715.448 | 1,753.178 | 60.471 | 11,089.036 | 354,849.140 | 0 |
| 0205-tidehunter_tcp-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,835.691 | 2,323.772 | 2,363.932 | 82.502 | 8,294.695 | 265,430.225 | 0 |
| 0114-tikv-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 7,501.745 | 11,728.247 | 11,757.218 | 61.561 | 2,098.827 | 67,162.460 | 0 |
| 0027-valkey_tcp-256-read-many32-primary_rf1-r0 | get_many/success | 2048 | 2,003.884 | 6,831.480 | 6,866.030 | 92.032 | 6,643.908 | 212,605.069 | 0 |

### primary_rf1 / read-many32 / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0251-cassandra-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 2,585.007 | 3,111.329 | 3,156.900 | 78.632 | 5,795.402 | 185,452.873 | 0 |
| 0018-foundationdb-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 9,595.800 | 10,593.682 | 10,640.904 | 111.603 | 1,649.290 | 52,777.281 | 0 |
| 0185-native-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 55.141 | 87.522 | 157.503 | 71.331 | 112,268.798 | 3,592,601.530 | 0 |
| 0100-tidehunter_efa-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,792.869 | 2,548.926 | 2,608.018 | 85.962 | 8,614.826 | 275,674.440 | 0 |
| 0155-tidehunter_efa_direct-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,805.900 | 2,463.065 | 2,511.456 | 91.202 | 8,528.095 | 272,899.037 | 0 |
| 0068-tidehunter_efa_native-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,427.271 | 2,004.354 | 2,059.075 | 79.102 | 10,307.648 | 329,844.736 | 0 |
| 0221-tidehunter_tcp-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 2,114.258 | 2,855.164 | 2,902.475 | 101.812 | 7,222.483 | 231,119.472 | 0 |
| 0130-tikv-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 7,328.870 | 11,277.197 | 11,335.688 | 87.082 | 2,145.445 | 68,654.235 | 0 |
| 0043-valkey_tcp-1024-read-many32-primary_rf1-r0 | get_many/success | 2048 | 2,767.840 | 10,958.160 | 11,004.671 | 86.682 | 4,648.002 | 148,736.063 | 0 |

### primary_rf1 / read-many32 / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0233-cassandra-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 4,650.884 | 26,737.057 | 26,913.711 | 365.938 | 2,606.657 | 83,413.037 | 0 |
| 0000-foundationdb-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 12,898.063 | 16,435.860 | 16,616.174 | 300.327 | 1,195.074 | 38,242.374 | 0 |
| 0167-native-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 118.072 | 237.746 | 413.920 | 171.064 | 54,743.623 | 1,751,795.941 | 0 |
| 0082-tidehunter_efa-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 3,865.935 | 5,461.649 | 5,619.073 | 293.826 | 4,019.736 | 128,631.557 | 0 |
| 0137-tidehunter_efa_direct-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 4,403.986 | 6,433.610 | 6,578.554 | 296.827 | 3,560.611 | 113,939.559 | 0 |
| 0050-tidehunter_efa_native-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 3,094.918 | 4,435.337 | 4,546.019 | 194.434 | 4,979.693 | 159,350.170 | 0 |
| 0203-tidehunter_tcp-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 4,386.037 | 6,072.205 | 6,230.768 | 312.397 | 3,490.856 | 111,707.404 | 0 |
| 0112-tikv-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 9,049.388 | 13,521.977 | 13,638.959 | 222.445 | 1,730.029 | 55,360.941 | 0 |
| 0025-valkey_tcp-8192-read-many32-primary_rf1-r0 | get_many/success | 2048 | 3,724.952 | 13,130.339 | 13,259.571 | 267.826 | 3,471.968 | 111,102.991 | 0 |

### primary_rf1 / read-many32 / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0234-cassandra-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 9,771.748 | 49,230.119 | 50,067.198 | 1,171.846 | 1,333.392 | 42,668.545 | 0 |
| 0001-foundationdb-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 27,523.842 | 59,357.129 | 60,207.947 | 1,898.292 | 482.289 | 15,433.234 | 0 |
| 0168-native-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 1,494.923 | 53,085.526 | 55,966.089 | 53,305.231 | 2,015.219 | 64,487.004 | 0 |
| 0083-tidehunter_efa-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 20,721.943 | 55,917.833 | 56,748.851 | 1,358.490 | 668.089 | 21,378.848 | 0 |
| 0138-tidehunter_efa_direct-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 23,456.795 | 56,369.586 | 57,196.174 | 1,374.631 | 603.486 | 19,311.557 | 0 |
| 0051-tidehunter_efa_native-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 15,060.710 | 36,305.395 | 37,143.344 | 1,424.281 | 912.101 | 29,187.241 | 0 |
| 0204-tidehunter_tcp-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 24,247.480 | 60,796.793 | 61,727.134 | 1,357.490 | 578.513 | 18,512.428 | 0 |
| 0113-tikv-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 16,235.937 | 25,243.885 | 26,050.582 | 2,006.154 | 903.893 | 28,924.561 | 0 |
| 0026-valkey_tcp-65536-read-many32-primary_rf1-r0 | get_many/success | 2048 | 9,701.263 | 17,836.942 | 18,722.421 | 2,033.525 | 1,430.295 | 45,769.432 | 0 |

### primary_rf1 / write-concurrent / 64 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0240-cassandra-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 368.329 | 1,035.653 | 1,048.183 | 31.141 | 36,625.340 | 36,625.340 | 0 |
| 0007-foundationdb-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 956.931 | 1,071.833 | 1,086.773 | 35.531 | 15,610.394 | 15,610.394 | 0 |
| 0174-native-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 650.364 | 2,468.695 | 2,483.235 | 31.621 | 19,464.520 | 19,464.520 | 0 |
| 0089-tidehunter_efa-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 890.999 | 142,945.359 | 142,960.049 | 33.161 | 4,160.886 | 4,160.886 | 0 |
| 0144-tidehunter_efa_direct-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 857.219 | 142,957.173 | 142,968.443 | 34.351 | 4,202.094 | 4,202.094 | 0 |
| 0057-tidehunter_efa_native-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 815.118 | 144,661.255 | 144,676.596 | 33.610 | 4,130.204 | 4,130.204 | 0 |
| 0210-tidehunter_tcp-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,196.787 | 143,810.756 | 143,823.526 | 37.301 | 3,966.725 | 3,966.725 | 0 |
| 0119-tikv-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 477.690 | 791.237 | 802.928 | 36.490 | 30,285.696 | 30,285.696 | 0 |
| 0032-valkey_tcp-64-write-concurrent-primary_rf1-r0 | put/success | 4096 | 421.169 | 714.386 | 730.556 | 28.361 | 34,926.029 | 34,926.029 | 0 |

### primary_rf1 / write-concurrent / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0242-cassandra-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 369.698 | 728.266 | 740.517 | 34.721 | 36,553.485 | 36,553.485 | 0 |
| 0009-foundationdb-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 968.171 | 1,932.863 | 1,952.482 | 43.491 | 14,688.050 | 14,688.050 | 0 |
| 0176-native-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 607.113 | 2,721.570 | 2,746.721 | 34.371 | 20,129.704 | 20,129.704 | 0 |
| 0091-tidehunter_efa-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 951.110 | 142,877.817 | 142,888.867 | 34.070 | 4,042.329 | 4,042.329 | 0 |
| 0146-tidehunter_efa_direct-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 889.890 | 143,287.096 | 143,300.056 | 32.241 | 4,193.666 | 4,193.666 | 0 |
| 0059-tidehunter_efa_native-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 913.831 | 142,638.683 | 142,652.464 | 33.701 | 4,848.924 | 4,848.924 | 0 |
| 0212-tidehunter_tcp-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,199.377 | 143,645.357 | 143,672.987 | 36.071 | 3,910.214 | 3,910.214 | 0 |
| 0121-tikv-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 490.641 | 820.488 | 833.328 | 32.001 | 29,276.724 | 29,276.724 | 0 |
| 0034-valkey_tcp-256-write-concurrent-primary_rf1-r0 | put/success | 4096 | 439.929 | 864.419 | 881.080 | 33.371 | 32,326.545 | 32,326.545 | 0 |

### primary_rf1 / write-concurrent / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0244-cassandra-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 497.351 | 821.028 | 837.848 | 34.690 | 29,978.463 | 29,978.463 | 0 |
| 0011-foundationdb-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 959.091 | 1,475.652 | 1,546.844 | 40.491 | 15,519.654 | 15,519.654 | 0 |
| 0178-native-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 698.516 | 3,317.593 | 3,340.275 | 32.771 | 16,300.780 | 16,300.780 | 0 |
| 0093-tidehunter_efa-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 952.880 | 142,792.856 | 142,806.706 | 35.310 | 4,136.148 | 4,136.148 | 0 |
| 0148-tidehunter_efa_direct-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 928.450 | 142,883.737 | 142,898.658 | 37.320 | 4,774.349 | 4,774.349 | 0 |
| 0061-tidehunter_efa_native-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 942.281 | 142,370.794 | 142,393.545 | 35.370 | 4,161.031 | 4,161.031 | 0 |
| 0214-tidehunter_tcp-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,296.708 | 144,162.639 | 144,191.859 | 35.571 | 3,836.452 | 3,836.452 | 0 |
| 0123-tikv-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 607.464 | 1,270.868 | 1,305.239 | 42.071 | 5,583.151 | 5,583.151 | 0 |
| 0036-valkey_tcp-1024-write-concurrent-primary_rf1-r0 | put/success | 4096 | 471.171 | 792.188 | 810.278 | 35.411 | 31,481.485 | 31,481.485 | 0 |

### primary_rf1 / write-concurrent / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0252-cassandra-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 692.346 | 1,067.404 | 1,082.544 | 36.480 | 21,319.433 | 21,319.433 | 0 |
| 0019-foundationdb-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,007.092 | 2,227.939 | 2,247.219 | 51.401 | 12,312.799 | 12,312.799 | 0 |
| 0186-native-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 841.869 | 2,351.112 | 2,374.113 | 35.251 | 15,617.327 | 15,617.327 | 0 |
| 0101-tidehunter_efa-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,360.289 | 143,804.506 | 143,823.176 | 37.811 | 3,751.479 | 3,751.479 | 0 |
| 0156-tidehunter_efa_direct-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,495.063 | 142,725.219 | 142,742.609 | 49.101 | 3,668.886 | 3,668.886 | 0 |
| 0069-tidehunter_efa_native-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,452.921 | 143,093.731 | 143,114.481 | 44.981 | 3,673.538 | 3,673.538 | 0 |
| 0222-tidehunter_tcp-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,854.432 | 144,966.433 | 144,987.472 | 45.812 | 3,383.485 | 3,383.485 | 0 |
| 0131-tikv-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 768.987 | 1,403.440 | 1,429.941 | 38.952 | 18,756.549 | 18,756.549 | 0 |
| 0044-valkey_tcp-8192-write-concurrent-primary_rf1-r0 | put/success | 4096 | 628.704 | 1,168.356 | 1,190.566 | 41.850 | 23,493.928 | 23,493.928 | 0 |

### primary_rf1 / write-concurrent / 65536 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0238-cassandra-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,310.549 | 2,901.945 | 2,942.086 | 76.282 | 10,783.677 | 10,783.677 | 0 |
| 0005-foundationdb-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 3,021.616 | 5,341.857 | 5,386.708 | 97.753 | 4,823.407 | 4,823.407 | 0 |
| 0172-native-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 3,061.068 | 6,372.712 | 6,415.153 | 98.612 | 4,864.617 | 4,864.617 | 0 |
| 0087-tidehunter_efa-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 4,054.339 | 7,714.498 | 7,752.889 | 119.202 | 3,317.561 | 3,317.561 | 0 |
| 0142-tidehunter_efa_direct-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 4,471.588 | 140,361.819 | 140,402.940 | 119.313 | 2,527.103 | 2,527.103 | 0 |
| 0055-tidehunter_efa_native-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 4,398.286 | 9,319.684 | 9,396.326 | 123.632 | 2,811.335 | 2,811.335 | 0 |
| 0208-tidehunter_tcp-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 4,148.312 | 9,195.624 | 9,263.605 | 116.992 | 2,981.503 | 2,981.503 | 0 |
| 0117-tikv-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,150.745 | 2,471.925 | 2,513.865 | 70.862 | 12,408.473 | 12,408.473 | 0 |
| 0030-valkey_tcp-65536-write-concurrent-primary_rf1-r0 | put/success | 4096 | 1,021.153 | 4,981.469 | 5,012.620 | 75.181 | 11,538.043 | 11,538.043 | 0 |

### primary_rf1 / zipf-read / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0253-cassandra-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 239.495 | 356.038 | 371.778 | 35.791 | 57,265.627 | 57,265.627 | 0 |
| 0020-foundationdb-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 643.144 | 938.410 | 949.921 | 30.880 | 23,209.848 | 23,209.848 | 0 |
| 0187-native-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 1.800 | 3.460 | 25.461 | 22.211 | 135,704.635 | 135,704.635 | 0 |
| 0102-tidehunter_efa-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 88.682 | 320.917 | 336.137 | 28.721 | 106,777.380 | 106,777.380 | 0 |
| 0157-tidehunter_efa_direct-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 88.462 | 321.287 | 338.918 | 29.171 | 107,947.190 | 107,947.190 | 0 |
| 0070-tidehunter_efa_native-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 80.132 | 283.756 | 296.706 | 28.680 | 111,702.510 | 111,702.510 | 0 |
| 0223-tidehunter_tcp-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 182.015 | 588.533 | 612.234 | 38.131 | 56,201.806 | 56,201.806 | 0 |
| 0132-tikv-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 398.248 | 742.607 | 755.966 | 30.671 | 35,728.353 | 35,728.353 | 0 |
| 0045-valkey_tcp-1024-zipf-read-primary_rf1-r0 | read/success | 8192 | 204.935 | 333.087 | 346.328 | 31.991 | 65,159.880 | 65,159.880 | 0 |

### registered_ram_configured / registered-ram / 256 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0077-tidehunter_efa_native-256-registered-ram-registered_ram_configured-r0 | read/success | 8192 | 51.841 | 461.550 | 472.811 | 21.011 | 7,026.627 | 7,026.627 | 0 |

### registered_ram_configured / registered-ram / 8192 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0074-tidehunter_efa_native-8192-registered-ram-registered_ram_configured-r0 | read/success | 8192 | 59.301 | 463.940 | 483.951 | 27.751 | 6,156.734 | 6,156.734 | 0 |

### write_stage_diagnostics / write-concurrent / 1024 bytes

| Case ID | Operation/outcome | n | API p50 µs | API p99 µs | Response p99 µs | Scheduling p99 µs | Calls/s | Logical keys/s | Unsuccessful |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0201-native-1024-write-concurrent-write_stage_diagnostics-r0 | put/success | 4096 | 682.935 | 3,406.105 | 3,421.255 | 34.221 | 16,585.233 | 16,585.233 | 0 |
| 0110-tidehunter_efa-1024-write-concurrent-write_stage_diagnostics-r0 | put/success | 4096 | 985.042 | 142,893.538 | 142,906.498 | 35.010 | 4,151.740 | 4,151.740 | 0 |
| 0165-tidehunter_efa_direct-1024-write-concurrent-write_stage_diagnostics-r0 | put/success | 4096 | 971.241 | 142,594.306 | 142,614.527 | 43.072 | 4,126.349 | 4,126.349 | 0 |
| 0080-tidehunter_efa_native-1024-write-concurrent-write_stage_diagnostics-r0 | put/success | 4096 | 963.341 | 142,697.023 | 142,713.964 | 35.831 | 4,140.210 | 4,140.210 | 0 |
| 0231-tidehunter_tcp-1024-write-concurrent-write_stage_diagnostics-r0 | put/success | 4096 | 1,330.169 | 143,008.810 | 143,021.769 | 35.501 | 3,837.817 | 3,837.817 | 0 |

