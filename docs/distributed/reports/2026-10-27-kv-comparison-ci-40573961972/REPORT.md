# Distributed KV comparison: completed AWS campaign, 27 October 2026

**Valid full comparison; fleet closed. Coordinator-cache API latency is in native's microsecond range. Remote reads and write tails are not at native parity.** No replacement cluster or additional campaign was launched.

## Campaign and evidence

- [Workflow 40573961972](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/40573961972): success; one push-triggered campaign after rebuilt, source-bound, real software qualification.
- Source `87465ef68abb6fa9b7b9cb27224281a7c4994e6d`; run `th-6449c7068670440c`.
- Six `g6.8xlarge` On-Demand machines in Frankfurt (`eu-central-1c`), **cluster placement**: one controller and five owners with separate, uniquely identified local NVMe drives. US attempts yielded no fleet.
- **258/258 cases completed**, nine variants, all five value sizes (64, 256, 1,024, 8,192, 65,536 bytes), one independent repetition. No per-case operation/sample counts were reduced. All measurement rows were complete.
- **5,735,488 calls:** 4,141,321 successes and 1,594,167 recorded rejections in 14 offered-load cases; no other recorded outcome or case error. Completion is not a claim that every offered request succeeded.
- Raw results, source inputs and costs are local and hash-verified; all six EC2 hosts are terminated, and all 23 enabled-region residual checks found no run-owned billable resources. [Source/retrieval/closure evidence](EVIDENCE.md), [integrity](INTEGRITY.json), [closure](CLOSURE.json).

This is the **synthetic KV matrix, not Sui checkpoint replay**. It validates the portable transport/Valkey comparison state; it does not establish Sui performance, HA, failure recovery, or hardware InfiniBand/RoCE/Falcon behavior.

## Representative matched workload: 256-byte values

Separate pure-read and pure-write closed-loop cases, concurrency 16, RF1 profile. These are successful-operation **API** quantiles, not response latency and not per-key latency for a batch. Every row has 8,192 read calls or 4,096 write calls; all these cases have zero rejections. The complete five-size/profile/cohort/outcome results, API/response/scheduling quantiles, means, standard deviations, maxima, counts, calls/s, keys/s and MiB/s are in [TABLES.md](TABLES.md) and [all-results.csv](all-results.csv).

| Backend | Read p50  µs | Read p99  µs | Write p50  µs | Write p99  µs | Write calls/s |
|---|---:|---:|---:|---:|---:|
| Native (embedded) | 1.820 | 3.250 | 607.113 | 2,721.570 | 20,129.7 |
| TideHunter TCP | 197.664 | 649.185 | 1,199.377 | 143,645.357 | 3,910.2 |
| TideHunter EFA | 104.122 | 335.137 | 951.110 | 142,877.817 | 4,042.3 |
| TideHunter EFA direct | 96.882 | 341.738 | 889.890 | 143,287.096 | 4,193.7 |
| TideHunter EFA native | 92.122 | 319.827 | 913.831 | 142,638.683 | 4,848.9 |
| TiKV | 335.708 | 619.463 | 490.641 | 820.488 | 29,276.7 |
| Cassandra | 244.936 | 383.988 | 369.698 | 728.266 | 36,553.5 |
| FoundationDB | 595.573 | 867.449 | 968.171 | 1,932.863 | 14,688.0 |
| Valkey TCP | 211.415 | 349.858 | 439.929 | 864.419 | 32,326.5 |

These contracts are not identical. Native is an embedded lower bound, not a five-host scale-equivalent peer. Primary native writes use the Value-WAL barrier; native page-cache acknowledgement has a separate cohort. Distributed variants have different transport/security paths. TiKV/FoundationDB/Cassandra have their own consistency/transaction/logging behavior even at RF1. Valkey TCP uses AOF `appendfsync always` but serves GET from RAM, not an AOF-backed disk read. Results characterize these adapters and settings, not each product's universally best performance. FoundationDB get-many32 uses 32 sequential transaction gets; Valkey uses ordered bounded per-key routing rather than cross-slot MGET. Neither shortcut is hidden in a per-key latency calculation.

## What this run establishes

### Coordinator-cache API performance

The separately configured, concurrency 1 coordinator-cache cases have API p50 **0.73–1.12 µs** across256-byte and 8 KiB values and the four TideHunter transports. This is the desired microsecond order at the measured API boundary. It is a different cohort from the concurrency 16 primary table, so it is not a controlled claim that distributed cache beats native.

Response p99 remains approximately 25–29 µs in these cache cases, with scheduling p99 approximately 23–27 µs. The backend API is not responsible for all end-to-end latency. API, scheduling and response are separate populations/boundaries; their quantiles must not be subtracted as if they described the same operation.

### Remote reads and batching

At 256 bytes, EFA-native read API p50 is 92.122 µs versus 197.664 µs for TideHunter TCP and 211.415 µs for Valkey TCP. It beats the tested distributed alternatives on this particular p50, but is about 50.6× the embedded native 1.820 µs. There is no native-parity remote-read result.

At 256 bytes, EFA-direct get-many32 p50 is 1,299.009 µs and 11,699.8 successful calls/s, versus native 61.891 µs and 128,971.2 calls/s. A call contains 32 keys; the EFA-direct logical-key rate is 374,394.0 keys/s, not 11,699.8 keys/s. [The batch figure](plots/primary-read-many32.png) covers all five sizes and shows why one scalar-read number cannot summarize batching.

Registered-RAM-configured cases are **mixed paths**, not isolated hardware-only FI_READ latency: cumulative counters report 2,448/2,480 completed RMA reads, zero RMA errors, and 6,778/6,746 message fallbacks for 8 KiB/256-byte cases. Counters include qualification and other phases; they cannot be assigned to the timed population or used as a latency fraction. Startup FI_READ discovery and a requested device-read policy are not independent per-completion proof of physical implementation. See [registered-RAM evidence](TABLES.md#registered-ram-transport-evidence).

### Writes and offered load remain weak points

The primary 1 KiB TideHunter write p99 values are about 142–144 ms across all four transports. The separate diagnostics record durable-receipt publication blocking, owner append, control-queue and file-persist activity, but their overlapping cumulative sums do not locate the cause of the clean-primary p99. The current evidence does **not** prove that network time alone explains this tail.

EFA-direct is not uniformly better: at 64 KiB, its write p99 is 140,361.819 µs, versus 9,319.684 µs for EFA-native and 9,195.624 µs for TCP. This is a result of this single run, not a significance or causal claim.

Most rejections arise under offered 50,000 calls/s; the TideHunter variants also reject approximately 21–23 thousand calls in their 10,000/s offered-load cases. Valkey recorded no rejection; Cassandra recorded two at 50,000/s. Do not rank successful-operation latency without [offered-load accounting](plots/offered-load-accounting.png) and its accepted/rejected denominators.

### NVMe and telemetry

All five owners really had independent instance-store drives. Warm 1 KiB EFA-native/TCP read examples recorded zero physical read-sector deltas across the five storage devices; they were not NVMe-fetch measurements. Cold 65,536-byte TideHunter cases show nonzero physical-read counters on all five storage devices, whereas native's large read activity is on the controller device. OS-cache eviction did not evict application caches; Valkey GET remains RAM-backed. Device totals do not prove an individual GET hit NVMe. [Exact device/cgroup vectors](OBSERVABILITY.md#cold-65536-byte-cases-actual-boundary-read-counters) preserve those scopes.

Prometheus/Grafana recording criteria passed: 258 cases, 1,418 selected cgroup intervals, 13 targets up at verification, a retained TSDB snapshot, paged query evidence, dashboard/provisioning and image/source locks. Some backend-specific panels are empty/inapplicable, not zero. Monitoring was cohosted and not perturbation-calibrated; Grafana visual rendering and offline replay of this campaign were not exercised. In the matched 1 KiB example EFA-native storage consumed about 4 average CPU cores per owner versus much lower TCP storage CPU; this is a resource observation, not proof of the latency cause. [Full monitoring analysis](OBSERVABILITY.md).

## Historical comparison

The previous completed comparison used `us-east-1a`, the same instance type and host ceiling but different sources, and has matched historical repetitions 0/1. This run uses `eu-central-1c` and one repetition. There are 228 current cases with job-field matches and 30 without (Valkey plus write diagnostics); job-field matches do not establish full configuration equivalence. Seeds and transport representation/effective labels differ in many pairs. [Historical comparisons](historical-comparison.csv) retain those differences; [TABLES.md](TABLES.md#historical-context) explains them. No portable-refactor speedup/regression or statistical significance is isolated by these observations.

Quantiles are the existing raw nearest-rank definitions. The harness warnings remain: p99 is descriptive below 10,000 calls and p99.9 below 100,000. Single-repetition plots have **no run-to-run error bars**. Mean/stddev describe within-case observations, not independent-run uncertainty. No incompatible populations or percentiles were pooled; missing data were not replaced with zero.

## Costs and shutdown

All figures below are **observed-resource estimates**, not final AWS billing. The linked account cannot list Cost Allocation Tags, so billed values remain unavailable. [COSTS.json](COSTS.json) preserves the separate generated-report/ledger provenance and partial-coverage limits.

| Scope | Estimate USD |
|---|---:|
| This run / UTC day 2026-10-27 |23.6912|
| Calendar week 2026-10-26 through 2026-11-01 |23.6912|
| October / tracked branch lifetime since 2026-10-18 |194.6157|
| Rolling seven-day admission usage |180.1338|
| Remaining under the editable $220 weekly budget |39.8662|
| Active reservations |0|

The original $56.2396 reservation was a hold, not a charge; it is released. EC2/root-EBS/public-IPv4 estimate is $23.6342; partial observed S3 adds $0.0570. Retained-storage projection $0.00627 is separate from observed usage, and unknown or late charges are not fabricated. Results remain in private S3/GitHub with 30-day retention and are already copied locally. No compute cluster remains running and no further cluster will be started from this task.

## Figures

Thirteen current-data PNG/PDF pairs, with source points in [plot-data.csv](plots/plot-data.csv) and definitions/coverage in [plots.json](plots/plots.json). The primary read, write, batch and cache figures were visually inspected. These are actual cloud results, not rendering fixtures.

| Figure | PNG | PDF |
|---|---|---|
| coverage | [PNG](plots/coverage.png) | [PDF](plots/coverage.pdf) |
| primary-read-concurrent | [PNG](plots/primary-read-concurrent.png) | [PDF](plots/primary-read-concurrent.pdf) |
| primary-write-concurrent | [PNG](plots/primary-write-concurrent.png) | [PDF](plots/primary-write-concurrent.pdf) |
| primary-read-many32 | [PNG](plots/primary-read-many32.png) | [PDF](plots/primary-read-many32.pdf) |
| primary-point-mixed-latency | [PNG](plots/primary-point-mixed-latency.png) | [PDF](plots/primary-point-mixed-latency.pdf) |
| primary-point-mixed-goodput | [PNG](plots/primary-point-mixed-goodput.png) | [PDF](plots/primary-point-mixed-goodput.pdf) |
| offered-load-accounting | [PNG](plots/offered-load-accounting.png) | [PDF](plots/offered-load-accounting.pdf) |
| offered-load-tails | [PNG](plots/offered-load-tails.png) | [PDF](plots/offered-load-tails.pdf) |
| zipf-versus-uniform | [PNG](plots/zipf-versus-uniform.png) | [PDF](plots/zipf-versus-uniform.pdf) |
| cache-cohorts | [PNG](plots/cache-cohorts.png) | [PDF](plots/cache-cohorts.pdf) |
| cold-os-cache | [PNG](plots/cold-os-cache.png) | [PDF](plots/cold-os-cache.pdf) |
| native-pagecache-latency | [PNG](plots/native-pagecache-latency.png) | [PDF](plots/native-pagecache-latency.pdf) |
| native-pagecache-goodput | [PNG](plots/native-pagecache-goodput.png) | [PDF](plots/native-pagecache-goodput.pdf) |

## Files and limits

- [TABLES.md](TABLES.md): all 460 operation/outcome population rows; [raw original CSV](all-results.csv).
- [metrics-analysis.json](metrics-analysis.json): counts, diagnostics and matched-history analysis.
- [OBSERVABILITY.md](OBSERVABILITY.md) and [telemetry-summary.json](telemetry-summary.json): exact measured resource scope, monitoring coverage and gaps.
- [EVIDENCE.md](EVIDENCE.md), [INTEGRITY.json](INTEGRITY.json), [CLOSURE.json](CLOSURE.json), [COSTS.json](COSTS.json): provenance, complete retrieval, resource teardown and accounting.

No new benchmark, service, cluster or performance implementation was added while analyzing this frozen campaign. The false-positive watchdog active-IAM health alarms are documented in EVIDENCE; source/code was not changed during the run. Backend guarantees are not silently weakened to obtain these results. The next investigation must separate true remote-read path latency and write-stage tail distributions rather than claim that the current mixed counters prove a root cause.
