# Completed AWS comparison and remaining bottlenecks

## Status and provenance

- [GitHub Actions 41109588049](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/41109588049): **success**.
- Run: `th-38c2508272a57587`; measured source: `d34022d18aed2e1b7b029d595f43992868453b28`, branch `bench/aws-efa`.
- Six On-Demand `c6id.32xlarge` hosts in `eu-west-1c`, mandatory cluster placement, one controller and five independent-NVMe owners. The allocator reached the approved CPU-instance fallback after G6/G6e capacity or quota failures.
- **196/196 cases completed; 1,271,808 successful measured API calls; zero rejected calls and zero other operation errors.** These are synthetic KV comparisons, not a new real-Sui-checkpoint replay.
- All six instances terminated. Independent AWS MCP scans found no run-owned volumes, ENIs, EIPs, security groups, placement groups, IAM roles or instance profiles across the five allowed regions. Shared infrastructure and other users were not removed.
- Downloaded controller and monitoring inventories verified all 17,628 and 712 regular members, respectively. All 497 regular frozen-source files matched the locally validated source at verification time. A matching executable hash across the two new input builds is expected: the later changes were hardware/configuration, test-fixture and documentation changes, not a different runtime implementation.

Evidence root: `checkpoint-work/aws-ci-runs/2026-10-30-owner-authority-campaign/`. Its `ops-final-manifest.json`, `ops-closure-mcp.json`, input-verification files, sealed archives, GitHub campaign/cost artifacts and revision-104 ledger reports are retained.

## Reports

- [All 196 populations, canonical nine-backend comparisons and semantics](METRICS.md).
- [Complete metrics CSV](complete-metrics.csv) and [JSON](complete-metrics.json): API/response/scheduling p50, p95, p99, p99.9, mean, population standard deviation, maximum, counts and throughput.
- [Stage, cache/RMA/device qualification and code/source audit](STAGE_AUDIT.md), with [structured evidence](stage-audit.json).
- [Canonical p99 figure](plots/canonical-api-p99.png), [coverage](plots/coverage.png), and [plot omissions](plots/plots.json).
- [Actual restored Grafana cache-hit view](grafana-coordinator-cache-replay.png).

One independent repetition. No run-to-run error bars, confidence or significance claim. Tail-sample warnings remain. The older cloud used G6/us-east-1; changed hardware and region prevent attributing end-to-end differences solely to code. Profiled and unprofiled runs are different populations, not repetitions.

## What improved, and what did not

### Coordinator hits reach the native microsecond order

Qualified C16 1-KiB scalar cache-hit API medians were **0.919 µs TCP, 0.920 µs EFA and 0.968 µs EFA-native**. The native primary median was **1.350 µs**. This demonstrates the same median order, not identical paths or all-tail parity. For example, the EFA-native cache-hit p99.9 was about 900 µs, with only 8,192 samples; its p99 was 5.382 µs. Ten of the 32 configured coordinator-hit cases were mixed and must not be called pure cache hits.

### The intended warm authority-cache problem is removed

The distributed history cache now permits up to 8,192 entries, still subject to metadata-byte and descriptor credits. The warm executor also avoids a redundant device-count scan after all members resolve. No native locks, final authority checks, DMA drain rules or durability conditions were removed.

All 32 warm-WAL cases had **zero measurement-boundary authority-history loads**. For case `0157-tidehunter_efa_direct-256-wal_warm-profiled-r0`, the preceding cloud recorded 2,547 loads and a selected planning mean of 22.738 µs; this run recorded zero and 0.876 µs. The handler mean changed from 26.068 to 2.886 µs. This supports useful metadata residency, not a causal whole-system speed claim across different machines.

### Primary results do not isolate the new resident-first path

The configured primary EFA-native 1-KiB read median/p99 were **244.941/5,345.960 µs**; TCP was **1,872.878/8,246.021 µs**. Do not hide these slow results. They differ from the fully warmed diagnostic controls:

| Setting | Primary | Warm-WAL C16 control |
|---|---:|---:|
| Dataset keys | 4,096 | 8,192 |
| Warmup operations | 512 | 8,192 |
| Full warmup key sweep | No | Yes |
| Read lanes | 2 | 4 |
| Resident-first reads | Disabled | Enabled |
| Explicit coordinator/owner value caches | Both zero | Both zero |

Primary EFA-native/TCP performed **1,937/1,936** measured history-load attempts, while their history caches remained well below the 8,192-entry limit. Their value reads used the asynchronous path. The corresponding warm controls had zero history loads and successful NOWAIT reads, with API medians **50.546/782.537 µs**. Those differences cannot be assigned to metadata alone because several settings differ.

Source inspection identifies the next residency opportunity: successful background history adoption removes captured pending descriptors without transferring them into the existing bounded LRU. A first subsequent lookup reloads and decodes history even with free cache capacity. Evaluate carrying over existing validated immutable Arcs at adoption, retaining entry/byte credits, normal eviction, lock ordering, generation checks and read/CQ pins. This carry-over is **not implemented or benchmarked by this campaign**. File-backed history access is not necessarily a physical NVMe miss.

### Remote latency remains far above a few microseconds

The eight qualified registered handle-headroom controls each completed 8,192 hardware posts/completions with no fallback. Their C1 API medians were approximately **47–52 µs**. In the profiled 8-KiB reuse headroom case, accepted-post to locally observed FI_READ completion/progress averaged **46.496 µs** over 256 selected flights. That is not isolated wire or NIC service latency.

Ordinary C1 registered controls still had 46 missing-grant fallbacks each; C16 registered controls remained mixed. Keep these populations separate.

Warm 256-B EFA-direct selected exchange/handler/planning means were **49.422/2.886/0.876 µs**, each with 256 observations. TCP exchange/handler means were **508.521/22.215 µs**. The exchange remainder includes transport, scheduling, encoding, queueing and observation; it is not a pure-network measurement.

The **8-KiB×32 C16 issue remains**. All four unprofiled qualified owner-hit batch throughputs were lower than the prior cloud, although several medians improved and tails changed. Current EFA-native owner-hit p50/p99 were 949.433/11,671.873 µs. Different hardware prevents a causal code-regression claim. The removed executor scan is not demonstrated to have improved executor residence in isolation.

### Writes have a different measured cost profile

In configured C16 1-KiB primary writes, EFA-native API p50/p99 were **1,272.971/5,166.871 µs**, native **3,016.031/12,522.410 µs**, and Valkey **1,927.434/3,058.112 µs**. This is not a claim that distributed writes beat ordinary asynchronous native puts: this benchmark adds an explicit native durability barrier, and Valkey uses observed AOF-always with no replica acknowledgment.

In EFA-native write diagnostics, selected append validation averaged 54.471 µs/request and persistence 256.516 µs/call; file-persistence averaged 283.775 µs/file call. Owner control queue/execution averaged 307.333/337.826 µs over a different request population. Existing ready-group sync/publication mechanisms already share eligible work. Group-size distributions and receipt-to-caller timing are still missing; overlapping stage totals cannot be added into an API latency budget.

## Cold NVMe qualification is still incomplete

All eight distributed cold cases recorded real request-identity block activity: **6,998 matched requests each** over the five owner collector windows. Their block-interval means were approximately 246–283 µs. The windows include control/gate/background work and cannot attribute those requests to individual measured API calls. Therefore **none is a measurement-scoped pure-NVMe API population**.

Native nominal cold controls recorded only one/zero matched device reads. Their sub-microsecond medians are not demonstrated cold-NVMe latency. Do not compare them with remote cold-labelled API percentiles as if both represented disk misses. NVMe-oF cannot be justified from these measurements.

## Prometheus and Grafana: useful, with scope limits

The 23.9-MiB monitoring archive restored using cached digest-pinned images, without downloads or live scrape targets. Offline verification preserved original timestamps and checked **196 observed cases, 34 query panels, datasource health OK and zero capture/replay gaps**. The authenticated dashboard was opened and visually checked against a qualified cache-hit case. API medians and raw counts were visible, not just container health.

All **1,142** exported resource-boundary process rows were available. Twelve reported positive CPU throttling: five Cassandra rows in the large read-many primary, and seven Tidehunter client rows in specified remote/owner-hit control cases. These are not all primary cases and do not explain every slow read. The resource interval includes driver/control/background overlap; throttled time is not an exact per-API pause budget. Coarse rolling host/cgroup samples cannot locate a microsecond tail event. Raw API samples remain authoritative.

The dashboard has 35 panels, including its scope panel; the observability README count was corrected locally. The replay is stopped after validation. Reopen the retained archive with the maintained `campaign_monitor.py replay` command and a fresh state leaf; no AWS machines are required.

## Next bounded work

1. Preserve eligible immutable authority descriptors across successful spill adoption within the existing budget. Measure first-touch history-load removal with otherwise identical primary settings.
2. Add a matched optimized-primary variant varying resident-first policy and lanes independently. Keep the legacy primary cases for historical comparison; do not conflate full warmup with a code improvement.
3. Amortize bounded 32-key coordinator-cache access. One sampled cache-hit batch population recorded a 9.649-µs mean mutex wait per member acquisition, not per API call. Preserve LRU/accounting and lock order; measure C1 hold-time tradeoffs.
4. Separate authority-worker queue/load/return and flight/actor/caller-ready timing. Relate CQ gaps and response retirement to matched populations before changing transport. Use an isolated warmed RMA floor to separate provider/progress cost from KV overhead; FI_READ residence alone is not wire latency.
5. Fix cold-device measurement window/request/file attribution and native actual-miss evidence before claiming a cold-NVMe distribution. Keep existing unqualified observations rather than deleting them.
6. Record ready-group sizes/singletons, requests per sync and receipt→eligibility→publication→caller timing. Do not replace the WAL, weaken acknowledgments or add another coordination protocol without measured need.

No further cluster was launched for these proposed steps. Future uncertainty/regression reruns must preserve frozen matched settings across all backends involved, define a material effect threshold before rerunning and use matched historical variability where available.

## Cost and cleanup

Revision 104 contains partial observed-resource estimates, **not the final AWS invoice**. Billing-to-date was unavailable because the linked account could not list cost-allocation tags.

| Estimate or policy | USD |
|---|---:|
| This run | 67.7440 |
| UTC day, 2026-10-30 | 79.1481 |
| Calendar week starting 2026-10-26 | 118.5804 |
| Rolling seven days | 226.3795 |
| October / lifetime since 2026-10-18 | 289.5049 |
| Editable rolling budget limit | 400.0000 |
| Available budget after settlement | 173.6205 |

Periods overlap and must not be added. Unresolved runs: zero. Private S3 evidence and shared ancillary services can retain small charges; the run-owned compute and listed infrastructure residuals are absent. Full results and accounting artifacts are also retained on the successful GitHub run.
