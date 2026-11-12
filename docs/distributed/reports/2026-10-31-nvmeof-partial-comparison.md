# Partial replacement-fleet comparison: 1 KiB scalar first-touch reads

**Partial, not a successful full campaign.** Replacement `th-7b5bbd983fa7e383`: 20 of 125 cases admitted completed (0000–0019); 0020 core workload completed but its case failed cleanup. No EFA, 8 KiB, write controls, or remaining NVMe/TCP concurrency/batch/repeat matrix completed. One independent repetition; diagnostic is a separately instrumented case, not a replicate. Never pool with earlier fleet `th-79da9cae47c0914b`.

## Matched API results
All are sealed 1,024-byte, scalar, concurrency 1, first_touch, same logical dataset (`sealed-v2-seed1775-size1024-keys65536`); native local and TCP/NVMe-TCP distributed use distinct physical sources. Calls/s is successful calls divided by observed raw measurement span; scalar calls are keys. Statistics below are frozen analyzer output, µs, nearest-rank raw samples, conditional on success. Stddev is within-case, not between-run.

| Case | Path | N success / offered | rejected or other unsuccessful | calls/s | API p50 | p95 | p99 | p99.9 | mean | stddev | max |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0000 | native | 8192/8192 | 0 | 22624.06 | 3.049 | 92.282 | 101.492 | 123.211 | 18.464 | 33.031 | 1239.029 |
| 0008 | tidehunter_tcp | 8192/8192 | 0 | 5708.40 | 122.062 | 207.804 | 236.484 | 268.984 | 137.979 | 35.876 | 784.932 |
| 0009 diagnostic, profile 256 | tidehunter_tcp | 8192/8192 | 0 | 5314.24 | 130.382 | 218.474 | 247.074 | 340.365 | 143.015 | 44.279 | 2211.555 |
| 0018 | tidehunter_nvme_tcp | 8192/8192 | 0 | 4234.65 | 176.543 | 265.724 | 356.496 | 469.187 | 191.083 | 47.901 | 1203.379 |
| 0019 diagnostic, profile 256 | tidehunter_nvme_tcp | 8192/8192 | 0 | 3825.87 | 213.084 | 309.994 | 378.366 | 480.247 | 220.151 | 51.024 | 1013.116 |

Scheduling and end-to-end response are separate populations, not additional API time. µs, same 8,192 successful calls:

| Case | schedule mean | schedule p50 | schedule p99 | schedule max | response mean | response p50 | response p99 | response max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0000 | 11.222 | 9.740 | 22.610 | 90.391 | 29.686 | 15.930 | 114.682 | 1248.359 |
| 0008 | 16.291 | 15.551 | 28.790 | 548.448 | 154.270 | 138.422 | 252.315 | 809.763 |
| 0009 | 17.112 | 17.420 | 30.870 | 728.952 | 160.127 | 147.612 | 265.524 | 2235.305 |
| 0018 | 16.715 | 16.380 | 29.490 | 1042.637 | 207.798 | 193.803 | 375.365 | 1303.150 |
| 0019 | 18.968 | 15.010 | 31.171 | 840.533 | 239.119 | 233.044 | 400.767 | 1186.529 |

Raw `result/samples.csv` independently counted for each listed case: 8,192 measurement/success rows, zero other measurement outcomes, plus 65,536 separate verify_existing_dataset/success rows. Verification is not measurement. p99 is descriptive below 10,000 calls and p99.9 below 100,000; tails are not stable estimates. Failed/rejected arrivals remain denominator in analyzer; missing observations are not zero.

The baseline NVMe/TCP median 176.543 µs vs legacy TCP 122.062 µs (1.45×), and vs native 3.049 µs (57.9×); NVMe/TCP throughput 4,234.65 vs TCP 5,708.40 calls/s. Native p50 is not a physical cold-device latency: native and TCP perform mapped WAL eviction and OS drop before measurement, but subsequent buffered reads/readahead may warm cache. Different cache paths and instrumentation, one run, and C1 prevent a causal, general performance or significance claim.

In native case 0000 the measurement-boundary client cgroup recorded 8,916,992 read bytes across devices over 1.920 seconds; this accounting includes driver, oracle, gate overlap and background work and does not identify how many of the 8,192 measured API calls caused physical reads. The analyzer has no cold-device evidence for 0000 or 0008. TCP 0008 records 8,192 owner read RPCs, not physical I/O operations. Therefore eviction and OS drop prove preparation, not that each first-touch call or its 3.049 µs native median hit NAND; mapped-page cache warming and readahead are plausible, not independently proven per call.

## Measurement evidence, direct path and diagnostics
First-touch native/TCP receipt: mapped WAL eviction completed before gate and OS caches dropped. NVMe/TCP receipt: mapped eviction not requested, OS drop false, engine cache eviction false; target receipts report TCP transport, initiator `O_DIRECT required by reader`, target cache `direct` on five exports. This bypasses host/target page cache, not device cache. It does not prove a NAND miss. Coordinator/owner explicit caches and registered-resident RAM were configured at zero. New direct-path repeat reads are not remote Linux RAM-hit experiments.

- 0008: measurement coordinator cache hits 0, misses 8192, disabled bypass 8192, read RPC 8192; RMA hits/posts/completions 0; resident telemetry records 8192 message read requests.

- 0009: measurement coordinator cache hits 0, misses 8192, disabled bypass 8192, read RPC 8192; RMA hits/posts/completions 0; resident telemetry records 8192 message read requests.

- 0018: measurement coordinator cache hits 0, misses 8192, disabled bypass 8192, read RPC 0; resident/RMA counters are zero in available partition deltas. Direct qualification: {'eligible_hits': 8192, 'ineligible_fallbacks': 0, 'io_completions': 8192, 'io_errors': 0, 'io_submissions': 8192, 'io_timeouts': 0}; qualified True.

- 0019: measurement coordinator cache hits 0, misses 8192, disabled bypass 8192, read RPC 0; resident/RMA counters are zero in available partition deltas. Direct qualification: {'eligible_hits': 8192, 'ineligible_fallbacks': 0, 'io_completions': 8192, 'io_errors': 0, 'io_submissions': 8192, 'io_timeouts': 0}; qualified True.

Drained measurement-interval reader stages below aggregate sums and counts across all five readers (weighted mean = summed ns / summed stage count). Alignment has two observations per read; other stages here one. Stages can nest/overlap, use different populations, and must not be added. Submit-to-CQ includes initiator kernel, network, target and device: it is not pure wire or device latency. Lifetime maxima are not interval maxima.

| case | stage | observations | weighted mean µs |
|---|---|---:|---:|
| 0018 | admission | 8192 | 0.494 |
| 0018 | alignment | 16384 | 0.206 |
| 0018 | extraction | 8192 | 0.219 |
| 0018 | queue | 8192 | 0.774 |
| 0018 | submit_to_cq | 8192 | 156.969 |
| 0018 | verification | 8192 | 1.154 |
| 0019 | admission | 8192 | 0.846 |
| 0019 | alignment | 16384 | 0.404 |
| 0019 | extraction | 8192 | 0.231 |
| 0019 | queue | 8192 | 1.189 |
| 0019 | submit_to_cq | 8192 | 167.965 |
| 0019 | verification | 8192 | 1.096 |

Sampled diagnostic case 0009, aggregate coordinator exchange 144.368 µs, inline 145.039 µs, encode 0.929 µs, decode 0.745 µs, verification 0.925 µs; owner server handler 32.331 µs, record read 31.384 µs, executor residence 7.924 µs and async 26.742 µs. Each stage has its own 256 observations; these overlap and aggregate/partition views are not independent samples. Diagnostic case 0019 sampled coordinator nvme_flight mean 250.357 µs (256 observations). Its owner RPC stages are unavailable, not zero. Neither these means nor differences yield a pure wire/device decomposition.

**Target/block trace, case 0019:** The `0008` label and root are inherited shared-export identifiers, not evidence of another case. The receipt binds trace generation `c5b5066e1f235ac8` to 0019, and all five `bindings.json` export UUIDs match 0019's measurement readers. On each owner, trace start falls after that host's pre-gate snapshot and before its end snapshot; trace stop falls after the end snapshot. For example owner 0 monotonic times in seconds are before 527.612, trace start 529.318, end snapshot 533.497, trace stop 534.931. These are same-host clocks, not cross-host synchronized timestamps. Thus this is evidence of the 0019 interval, although trace boundaries also include time outside the exact workload and individual block requests cannot be attributed one-to-one to API calls.

Owner 0 trace reports 1,792 matched nvmet request pairs, mean 41.764 µs, and 1,800 matched block pairs, mean 32.770 µs, with zero reported drops or unmatched/ambiguous events. Owner 4 reports 1,760 nvmet pairs, mean 42.016 µs, and 1,783 block pairs, mean 34.081 µs, likewise no reported drops or unmatched/ambiguous pairs. Owner 1, 2, and 3 have respectively 364, 208, and 1,144 dropped/overrun events; owner 1 also has an unmatched nvmet completion, so their trace distributions are incomplete. Controller IDs can collide across subsystems, block pairs can alias, merge or split, and no cross-layer request correlation is claimed. These are target request and block event durations, not NAND or network latencies; device caches and trace interval contamination remain possible. Raw summaries and bindings are under the preserved S3 tree `results/cells/0008-tidehunter_tcp-1024-first_touch-scalar-c1-baseline/storage/{0..4}/nvmeof/trace-c5b5066e1f235ac8/`; analyzer cold-device evidence remains absent.

CPU intervals are whole-host aggregate CPU-seconds, not isolated request CPU and may cover post-workload drain; client process resource boundary is unavailable in both direct cases. For 0018 coordinator user/system/idle/iowait = 1.95/1.22/129.57/0.01 s, owner 0 = 0.44/0.25/131.35/0.02 s; 0019 coordinator = 1.58/1.28/181.95/0.06 s, owner 0 = 1.01/0.43/186.46/0.04 s. These do not establish a CPU bottleneck.

Interpretation: the 156.969 and 167.965 µs submit-to-CQ means are the largest observed direct-reader stages, consistent with substantial time in the composite I/O path, but cannot isolate network, target scheduling, storage, device caching or CPU. Legacy owner record-read samples are material but not additive to coordinator exchange. No conclusion on EFA, warm remote RAM, physical NAND or write regression is supported.

## Failure and provenance
Case 0020 repeat-read is exploratory failed-cleanup, not admitted success: controller reports `InfrastructureError` during parallel host-disconnect on missing `/sys/class/nvme/nvme3/subsysnqn`; ownership remained live or quarantined and controller stopped. Its completed workload does not cure failed case cleanup. Monitoring was incomplete.

Exact evidence root: `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/kv-comparison/cloud-nvmeof-th-7b5bbd983fa7e383/controller-extracted`. `comparison.json`, `comparison.md`, `comparison.csv` contain frozen results; each case has `cases/<case-id>/receipt.json`, `result/samples.csv`, `result/measurement-read-evidence.json`, `result/summary.json`, `measurement-before.json`, `measurement-end.json`, `nvme-targets.json` where applicable, and `host-cpu-interval.json`; diagnostic sampled stages are in comparison.md and read diagnostics. Case 0019 trace references and bindings are in comparison.json `nvmeof_trace_evidence`. Case names resolve from comparison.json. Analysis contract: `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/local-nvmeof-cloudprep-20261031T192908Z-ca100224/cloud-analysis-contract.json`.

Source SHA256 `084bc689912c21500c55e313fb8f9824e40b0345b91a4d9bcde6cd144c1b9f82`; binary `94fad46c7c6f66d7f2cb8f20558b85c212a2662f586664c73be56fe8fbb1e2ad`; bundle `20c4c8c6add9c673e0bbd5bc1f6728a60e2879837c13b04487e7dc2d53152504`; extracted controller archive `e75ed797e8dd4426353b1959185cf863d9454aaf7cee4d1327477f2e58e52611`. Branch `experiment/linux-nvmeof-wal-reads`, dirty snapshot based on `895a83d7625d48df208c6ede8d59f0a33feb40bd`. Local S3 manifest and receipt in parent directory; S3 prefix `s3://tidehunter-ci-354775971805-us-east-1/th-7b5bbd983fa7e383/`.

Closure: `local://nvmeof-campaign-replacement-final.md`, `local://nvmeof-campaign-final.md`, parent `final-instance-observation.json` and `cost-settlement.json`. Six instances per fleet were MCP-confirmed terminated before reboot; no fresh AWS verification available. Conservative estimates $11.53 replacement plus $10.65 prior = $22.18, not actual bill; includes assumed future S3/transfer allowances. Ledger revision 111; protected aggregate report revision 107 stale, only CI role may publish.

## Appendix: all admitted 1 KiB baseline shapes

Seventeen baseline cases, no diagnostic cases pooled; batch32 rates are API calls/s, not logical keys/s. Frozen analyzer percentiles and within-case standard deviations in µs. Raw CSV measurement row counts were independently counted for each row and agree with offered and success counts; all unsuccessful counts are zero. Per analyzer, p99 is descriptive below 10,000 observations and p99.9 below 100,000; every row is below both thresholds. One repetition only.

| Case (all baseline) | N success / offered | unsuccessful | calls/s | p50 | p95 | p99 | p99.9 | mean | stddev | max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0000-native-1024-first_touch-scalar-c1-baseline | 8192 | 0 | 22624.06 | 3.049 | 92.282 | 101.492 | 123.211 | 18.464 | 33.031 | 1239.029 |
| 0001-native-1024-repeat_read-scalar-c1-baseline | 8192 | 0 | 31555.99 | 2.850 | 3.770 | 4.660 | 15.780 | 2.842 | 1.012 | 40.011 |
| 0002-native-1024-first_touch-scalar-c16-baseline | 8192 | 0 | 106714.50 | 110.701 | 156.163 | 179.012 | 200.503 | 115.605 | 21.398 | 227.133 |
| 0003-native-1024-repeat_read-scalar-c16-baseline | 8192 | 0 | 136560.73 | 3.660 | 5.340 | 6.590 | 20.610 | 3.757 | 1.402 | 22.660 |
| 0004-native-1024-first_touch-batch32-c1-baseline | 2048 | 0 | 2596.10 | 342.695 | 430.547 | 865.284 | 1245.259 | 312.353 | 175.332 | 2702.252 |
| 0005-native-1024-repeat_read-batch32-c1-baseline | 2048 | 0 | 7909.60 | 60.881 | 77.122 | 88.111 | 109.781 | 62.741 | 9.584 | 373.376 |
| 0006-native-1024-first_touch-batch32-c16-baseline | 2048 | 0 | 17093.13 | 955.915 | 1100.407 | 1188.049 | 1517.604 | 866.620 | 260.321 | 1523.914 |
| 0007-native-1024-repeat_read-batch32-c16-baseline | 2048 | 0 | 85707.45 | 105.262 | 126.652 | 157.673 | 270.205 | 106.956 | 15.716 | 294.735 |
| 0008-tidehunter_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 0 | 5708.40 | 122.062 | 207.804 | 236.484 | 268.984 | 137.979 | 35.876 | 784.932 |
| 0010-tidehunter_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 0 | 4270.99 | 194.693 | 240.074 | 264.984 | 305.955 | 187.755 | 36.023 | 625.270 |
| 0012-tidehunter_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 0 | 43632.45 | 260.055 | 771.873 | 1144.938 | 1552.895 | 332.419 | 214.077 | 1947.711 |
| 0013-tidehunter_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 0 | 53994.33 | 151.713 | 257.494 | 363.936 | 44601.633 | 235.515 | 1774.866 | 45674.180 |
| 0014-tidehunter_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 0 | 1734.05 | 500.637 | 628.900 | 690.111 | 1063.767 | 505.990 | 82.252 | 1359.722 |
| 0015-tidehunter_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 0 | 2414.22 | 339.396 | 398.406 | 450.227 | 555.469 | 342.751 | 34.797 | 850.884 |
| 0016-tidehunter_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 0 | 13674.85 | 968.816 | 2126.894 | 3106.930 | 4262.698 | 1091.345 | 538.945 | 4778.416 |
| 0017-tidehunter_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 0 | 6137.17 | 2358.478 | 4705.395 | 5961.985 | 7104.753 | 2525.012 | 1147.428 | 7995.827 |
| 0018-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 0 | 4234.65 | 176.543 | 265.724 | 356.496 | 469.187 | 191.083 | 47.901 | 1203.379 |

## Local host incident and current access

A local pstore record reports a kernel panic on Linux 6.17.9 after the campaign; the responsible module or cause is unknown. It is not cloud performance evidence. AWS MCP is offline and the CLI session expired; no fresh cloud verification or AWS calls were performed.