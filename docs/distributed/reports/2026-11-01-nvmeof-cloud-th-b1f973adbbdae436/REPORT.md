# Linux NVMe/TCP versus owner-record TCP/EFA and native Tidehunter

## Result and status

**The full 125-case benchmark matrix completed, but the monitoring-completeness gate did not. Linux NVMe/TCP did not improve remote-read latency in this run. Keep it experimental, not the default.**

- Run `th-b1f973adbbdae436`, branch `experiment/linux-nvmeof-wal-reads`, explicit frozen dirty snapshot; no commit/push or GitHub workflow dispatch in this execution.
- 125/125 case receipts admitted raw artifacts: 675,840 offered API calls, 675,840 successful, 0 unsuccessful. The 100 read cases contain 585,728 successful calls and 2,998,272 logical keys; 25 unchanged-write-control cases contain 90,112 successful calls.
- 26/26 NVMe intervals qualified, five readers each: 786,207 direct I/O submissions and completions; 0 recorded I/O errors/timeouts, 0 ineligible fallbacks, 0 coordinator value-cache hits and 0 owner-record-read RPCs in those intervals. These are measurement deltas, excluding full-key verification.
- Original terminal receipt stays `status=incomplete`, `complete=false`, `monitoring_complete=false`: 26 NVMe coordinator resource intervals were unavailable. The independent frozen-source raw-case report is complete for 125/125 cases; it does not upgrade the original monitoring result.
- **All six EC2 instances terminated.** Run-owned volumes, ENIs, Elastic IPs, security group, cluster placement group, six roles/profiles and incomplete multipart uploads were verified absent. Other users' resources were not touched. Benchmark S3 objects remain chargeable storage.

This is a **synthetic sealed native-WAL KV experiment**, not Sui checkpoint replay and not a comparison with TiKV/Cassandra/FoundationDB/Valkey. Generic `current_backend_coverage=false` means not all nine generic suite backends were selected; all four requested paths are present. One independent repetition; no run-to-run error bars, confidence or significance claims.

## Read comparison

Each cell is **API p50 / p99, microseconds**. `batch32` is one 32-key API, not a scalar latency multiplied/divided by 32. All other sizes, C1/C16 shapes, diagnostic twins, p95/p99.9/mean/SD/max, scheduling/response clocks, calls/s, keys/s and failure counts are in [TABLES.md](TABLES.md) and [all-operation-timings.csv](all-operation-timings.csv).

| Value / tier / API / concurrency | Native | RPC TCP | EFA-native | Linux NVMe/TCP |
| --- | --- | --- | --- | --- |
| 1KiB / first_touch-scalar-c1 | 3.410 / 102.982 | 125.102 / 243.274 | 65.812 / 129.882 | 228.575 / 480.721 |
| 1KiB / repeat_read-scalar-c1 | 2.880 / 4.520 | 203.474 / 265.385 | 66.211 / 83.282 | 354.957 / 530.541 |
| 1KiB / first_touch-batch32-c16 | 927.958 / 2328.265 | 987.750 / 3181.912 | 964.439 / 3386.936 | 11109.598 / 82939.544 |
| 1KiB / repeat_read-batch32-c16 | 103.892 / 146.423 | 2356.946 / 5685.832 | 2062.510 / 5331.074 | 26435.290 / 65721.743 |
| 8KiB / first_touch-scalar-c1 | 41.490 / 115.903 | 146.082 / 289.716 | 105.322 / 161.733 | 250.605 / 423.988 |
| 8KiB / repeat_read-scalar-c1 | 4.210 / 6.460 | 212.824 / 292.226 | 72.701 / 88.941 | 331.008 / 472.750 |
| 8KiB / first_touch-batch32-c16 | 4876.414 / 6767.650 | 3868.725 / 5393.945 | 2765.964 / 4201.491 | 13634.585 / 76649.301 |
| 8KiB / repeat_read-batch32-c16 | 166.073 / 274.085 | 2525.038 / 7393.103 | 2196.866 / 5320.410 | 26835.386 / 67388.982 |

The observed 8 KiB C16 first-touch batch p50 is 13.635 ms for NVMe/TCP versus 3.869 ms RPC-TCP, 2.766 ms EFA-native and 4.876 ms native. The repeat-read NVMe/TCP batch p50 is 26.835 ms. These are measured results, not successful latency improvements. This experiment alone does not isolate whether a different RDMA-backed NVMe target would improve them.

### Fairness boundaries

- All explicit distributed coordinator/owner value-cache limits are 0, with corresponding interval hits/inserts 0. Registered-RAM source/destination capacities are 0. Native unavailable distributed-cache counters are **unavailable**, not fabricated zeros.
- Native/TCP/EFA first-touch cases evict mapped WALs and drop OS pagecache once before the gate. Their reads remain buffered and can share pages, readahead and warm pagecache during measurement. Repeat cases follow full-key verification without eviction. First-touch uses a distinct-key sweep, repeat uses randomized requests; access patterns differ too.
- NVMe/TCP uses initiator O_DIRECT and a direct file-backed target. Repeat direct reads are **not** remote RAM-cache hits. Direct I/O bypasses these pagecache paths but does not attest NAND access or bypass device caches.
- RPC-TCP and NVMe/TCP share the exact same prepared owner-WAL source block for each size. EFA-native and native have logically matched seed/key/value populations but separately prepared physical layouts. Native generic preload issues one Put per key; owner preload uses complete native batches of up to 32 members. Thus this is not pure transport isolation.
- RPC-TCP uses mutual TLS; EFA-native uses trusted-network native EFA without TLS. The NVMe adapter's generic `mutual_tls` field describes owner RPC control/fallback. **Its actual measured kernel NVMe/TCP read traffic is not TLS**, and used no record-read RPC fallback. Do not attribute the whole TCP/EFA difference to encryption.
- EFA-native here is the owner-record protocol, not an NVMe/RDMA transport. AWS EFA was provisioned and admitted separately. The new path is explicitly **Linux NVMe/TCP**, not EFA-backed kernel NVMe/RDMA.

## Where direct-read time goes

Means below are microseconds **within each named stage's own count population**. Counts/sums/per-owner values for all 26 NVMe cases are in [nvme-stage-measurements.json](nvme-stage-measurements.json); [READ_AUDIT.md](READ_AUDIT.md) independently checks them against the frozen evidence.

| Case | Admission | SQE queue | Submit→CQ observation | Extraction | Verification |
| --- | --- | --- | --- | --- | --- |
| 0000-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-baseline | 0.472 | 0.675 | 241.588 | 0.183 | 1.144 |
| 0002-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-baseline | 0.801 | 1.150 | 330.326 | 0.307 | 1.694 |
| 0038-tidehunter_nvme_tcp-8192-first_touch-scalar-c1-baseline | 0.750 | 1.148 | 199.014 | 0.350 | 2.207 |
| 0039-tidehunter_nvme_tcp-8192-repeat_read-scalar-c1-baseline | 0.727 | 1.078 | 278.113 | 0.230 | 2.325 |
| 0044-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-baseline | 13.649 | 411.215 | 1783.639 | 0.258 | 106.322 |
| 0046-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-baseline | 2.672 | 44.193 | 517.246 | 0.318 | 25.039 |

`submit_to_cq` dominates scalar measured stage costs. It includes initiator kernel, transport, target, backing storage and userspace completion observation/progress—not pure wire or NAND time. Scalar extraction/checksum costs are small in these observations; replacing the native checksum cannot remove the measured hundreds of microseconds.

The batch-C16 slowdown is not fully attributed. Admission/progress, concurrent SQEs, owner fanout and verification have different populations and overlap. Do not add these means into an API critical path or subtract percentiles to create a network estimate. First-touch batch 0044 has mean API 19.500 ms versus per-member submit→CQ 1.784 ms and queue 0.411 ms; this does not define a serial remainder.

The frozen reader aligns and allocates each independently addressed native member separately (`tidehunter/src/distributed/nvmeof/reader.rs:285–320,384–411`), then extracts the exact member (`:556–568`). It does **not** read the entire 32-value WAL frame, but it also does not coalesce adjacent aligned ranges in that loop. Completed aligned namespace bytes / returned value bytes for first-touch scalar are:

| Value size | Aligned direct-read amplification |
|---|---:|
|64B|65.5×|
|256B|17.125×|
|1KiB|5.03125×|
|8KiB|1.50408935546875×|
|64KiB|1.0625×|

These are namespace-byte counters, not NAND amplification. Same physical pages can be reread for adjacent members; kernel/device merging and cache effects are not isolated.

## Unchanged durable-write controls

Cells are **API p50 / p99, milliseconds**, all C16. There is **no NVMe-oF writer** in this experiment. The new read backend did not create a faster write path. Native controls include the explicit Value-WAL persistence barrier; distributed controls acknowledge the complete batch durably on one configured owner, RF1, no replication. Native persists currently registered Value-WAL files and their directory; owner layout/barrier mechanics differ. No serializable cross-shard transaction/failover claim is made.

| Value / API / concurrency | Native | RPC TCP | EFA-native |
| --- | --- | --- | --- |
| 64B / write-c16 | 0.430 / 1.791 | 0.648 / 18.589 | 0.578 / 19.728 |
| 256B / write-c16 | 0.609 / 2.923 | 0.729 / 17.806 | 0.651 / 19.187 |
| 1024B / write-c16 | 0.657 / 2.407 | 0.987 / 56.489 | 0.798 / 60.465 |
| 8192B / write-c16 | 0.784 / 2.071 | 0.988 / 54.692 | 0.958 / 55.258 |
| 65536B / write-c16 | 3.269 / 6.505 | 2.131 / 4.559 | 2.162 / 4.703 |
| 1024B / write-batch32-c16 | 2.032 / 4.644 | 3.169 / 100.583 | 2.710 / 108.033 |
| 8192B / write-batch32-c16 | 9.417 / 16.080 | 7.931 / 120.202 | 7.661 / 124.200 |

Some medians are close or better distributed, especially 64 KiB writes and 8 KiB batch writes. Tails are not at parity: 1 KiB/8 KiB scalar remote p99 is about 55–60 ms versus native about 2 ms; remote batch p99 is about 101–124 ms. These are strict durability-control results, not native's asynchronous acknowledgement tier.

Four separate diagnostic twins sample the first 256 append requests per owner connection. Their exact counts/sums are in [write-stage-measurements.json](write-stage-measurements.json). Example 1 KiB TCP: native WAL append 19.026 µs per 1,280 sampled append calls, append validation 105.020 µs per 1,280, file persistence 141.543 µs per 1,167 actual persistence calls, total persistence 156.590 µs per 1,280 attempts. EFA-native sampled exchange is 357.386 µs versus TCP 484.287 µs, each over 1,290 profiled client requests including their control scope. These are different/nested populations, not complete API latency decompositions.

Coordinator measurement counters still record seven foreground checkpoints: 381.557 ms total for 1 KiB TCP, 422.557 ms for EFA-native (about 54.5/60.4 ms per checkpoint). **[INFERENCE]** Their timescale is consistent with the observed scalar tails, but no per-operation correlation proves that each p99 sample waited for a checkpoint. Pure network tuning does not remove an observed foreground checkpoint; instrument correlation before attributing the tail or changing durability.

## Prometheus/Grafana and remaining evidence gaps

- Prometheus, Grafana and bounded exporters ran on the controller; host selectors admitted 750/750 host selections and all 125 case terminal/summary exports. Saved monitoring capture says `complete=true`, seen/expected 125/125, gaps empty. This is snapshot coverage, not full process-accounting coverage.
- The 11.3 MiB monitoring bundle contains TSDB snapshot, range-query evidence, dashboard/provisioning/source/image configuration. Its own 344-file manifest was rehashed with 0 mismatches. `observability/monitoring.tar.gz` is preserved for offline restore. No new visual Grafana inspection was performed during this analysis; Grafana is a view of the same data, not independent timing evidence.
- Observer services were capped but cohosted. `observer_overhead_calibrated=false`; no non-perturbing claim. Authoritative latency data are raw API/scheduling/response CSVs, not scrape-derived timing.
- **26 NVMe client resource intervals failed.** Retained same-unit raw `io.stat` has five `(unknown)` device rows at each boundary; the strict major:minor parser rejects them. Those raw and accounting snapshots were sampled separately. Kernel cause and device identities remain unproven; do not sum anonymous rows or call missing CPU/I/O zero. Other 99 case resource intervals succeeded.
- All four NVMe diagnostic target/block trace sets have loss/overrun counts: 0001=1,507; 0003=5,720; 0045=134,493; 0047=134,079. They cannot establish complete per-call device attribution. Trace workflow `complete=true` means capture/cleanup/upload, not lossless tracing. Detailed paired/unmatched counts and exact source-case/generation mapping remain in [READ_AUDIT.md](READ_AUDIT.md) and raw receipts.

### Post-campaign local collector repair, not included in these measurements

`resource_accounting.py` now retains bounded invalid-I/O diagnostics and independently qualifies CPU/memory only after the final cgroup-inode recheck. Ambiguous I/O stays unavailable and overall resource completeness stays false. Boot/inode/time/counter-reset and device-population checks remain strict. Exporter and query inventory accept only explicitly qualified partial rows: overall availability 0, core availability 1, I/O availability 0, valid CPU/memory gauges, no invented I/O totals. Legacy unavailable v5 rows remain unchanged; no retrospective recovery claim.

Checks after these Python-only changes: comparison tooling 388 tests, 384 passed/4 skipped; observability 95 tests passed. Actual read-only local cgroup capture/interval succeeded. A temporary producer-to-consumer replay using the retained `(unknown)` raw text emitted qualified partial CPU/memory and missing-I/O coverage, while the original v5 interval remained byte-identical/unavailable. That replay recreated an inode and is not recovered cloud CPU evidence. No further privileged laptop hotplug, cloud fleet, Rust/native-WAL change, commit or push was made for the repair.

## Decision and next work

1. Keep the owner-record TCP/EFA path as the normal path. Retain Linux NVMe/TCP as an optional sealed-WAL direct-storage experiment; this run does not justify promoting it.
2. Before another cold-storage experiment, evaluate page/range coalescing and per-owner batched direct submissions with bounded memory/credits and unchanged source leases/native CRCs. Measure batch admission/progress separately. These are proposed changes, **not implemented or measured here**.
3. Obtain lossless same-window target/block evidence before claiming a pure network/device split. Preserve direct versus buffered tiers; do not reinterpret the few-microsecond native buffered repeat reads as device service.
4. For write tails, first correlate checkpoints/admission/publication with individual operation IDs. Preserve native crash recovery, complete-batch allocation and RF1 persistence semantics; do not remove a prefix or weaken acknowledgement merely to reduce a percentile.
5. No additional paid campaign is required merely to change the monitoring status flag. Current numerical results are usable with the stated limits. A future hardware NVMe/RDMA or different target is a separate experiment, not an EFA feature implied by this run.

## Provenance, closure and costs

Frozen source SHA256 `c9d9378277e04a60cc393adcf89bf630d586c95db1a359dc7f940396e797c3a5`; binary SHA256 `42faf4edd8babe7d01916fd4c43993b7b3781bddbd2b49a69b6adb261fd3972b`; bundle SHA256 `97409bd518783ba3d6d1e3a17590ba1b632f70cb8656868d112a57b3848348bc`. All six source/binary/deadline/kernel/boot receipts were verified before activation. Rust 1.90 release, generic x86-64, all seven benchmark features built and exact same binary exercised locally before upload.

Six `g6.8xlarge` in `us-east-1a`, mandatory cluster placement `th-b1f973adbbdae436`, EFA NICs. All six hosts have distinct boot IDs and distinct Amazon EC2 NVMe Instance Storage serials mounted ext4/noatime. Kernel `7.0.0-1014-aws` is provenance, not a safety proof. The earlier laptop 6.17.9 kernel panic remains unexplained; no new laptop hotplug stress was run.

The controller archive 295,584,760 bytes SHA256 `080a364aeac00c3a1d0f9182f822864ff7ace19f1f23041a44931b3ca6673555` and inventory 1,417,032 bytes SHA256 `a0ee4404058cceec012ff3a85bda84ac905bfdc24a37069d542479400eed8e24` matched terminal receipts. All 8,966 inventory entries (4,112,382,670 uncompressed bytes) matched size/SHA256. All 3,060 supplemental result-cell/startup S3 objects (168,071,882 bytes) were downloaded with HTTP size/ETag checks and local SHA256 records; all 126 case/startup partitions were re-listed unchanged after termination. The run's 140 deployment packages remain in private S3, not counted as measurement results. Local manifests are `s3-download-manifest.json`, `controller-inventory.json` and `console-manifest.json`. Raw root in the parent workspace:

`checkpoint-work/kv-comparison/cloud-nvmeof-th-b1f973adbbdae436/`

Source-owned peer trace S3 keys and same-case/generation mappings remain in receipts. The original comparison files were briefly rewritten by an operator's report helper, then restored byte-for-byte from the receipt-verified immutable archive and rechecked against inventory; S3/archive were unaffected. The corrected independent report uses the frozen v5 module in a fresh process and a separate audit directory.

Normal EC2 termination stalled in `shutting-down`; after archives were durable/verified, the operator rechecked account/exact RunId ownership and used Force+SkipOsShutdown for those six IDs. MCP independently confirmed all six terminated, block mappings empty. Support-resource cleanup and ledger release were verified. Six available 65,535-byte EC2 console tails were saved/hash-checked; truncation and logs do not establish shutdown cause.

Latest conservative estimate **$19.45**; all four related attempts **$54.22**. These are ledger estimates, not AWS invoices. Current ledger revision 115 has 0 active reservations and $131.483140408785 available within the $400 rolling weekly limit; no further increase was needed for v5. Calendar aggregates use UTC settlement dates: all-time $357.725577301106, week starting 2026-10-26 $186.801092370604, November/day 2026-11-01 $19.45. Calendar week differs from the enforced rolling 7-day charge $268.516859591215. [COSTS.json](COSTS.json) records exact inputs/basis. The protected shared published aggregate is stale; no IAM bypass was attempted. Retained S3 objects may continue charging.
