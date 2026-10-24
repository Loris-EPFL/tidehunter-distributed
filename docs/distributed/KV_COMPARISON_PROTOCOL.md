# Distributed KV comparison protocol

This protocol separates comparative research from engine optimization. Local execution establishes API, lifecycle, accounting and artifact behavior. It does not establish independent-NVMe, EFA, scaling or high-availability performance. No AWS credentials or paid campaign are required by the local comparison workflow.

## Research questions

1. At a fixed offered rate and resource budget, how do completed throughput, response-time distributions and failure rates compare?
2. What do coordinator/index disaggregation and owner-local Value WALs cost relative to an embedded native Tidehunter reference?
3. When do the WAL/value layout and cache populations reduce host-device reads/writes, occupied space, CPU or network traffic?
4. How do value size, access skew, working-set size and overwrite/delete age change those results?
5. Which results depend on different acknowledgment, consistency, atomicity or failure contracts?

A result need not favor this implementation. Do not omit regressions, rejected arrivals, failed cells, unsupported capabilities or a stronger competitor guarantee.

## Comparison tiers and acknowledgment

The primary portable contract is an opaque binary key/value API: complete point GET, blind full-value PUT, DELETE, explicit missing GET and ordered GET-many. A batch call is not a transaction. Crash-recovery atomicity, concurrent visibility and serializable isolation are separate properties. Only request an atomic write-batch profile when the backend declares the required mechanism; do not replace it with non-atomic client requests.

Native Tidehunter ordinary writes return after mapped WAL/index work, not an fsync. Its explicit Value-WAL synchronization mode is a separate result. Current distributed Tidehunter uses an authoritative coordinator index and RF1 native-WAL owners; a complete batch is placed on one owner. Value-WAL file/directory synchronization is not proof of replicated HA, full coordinator/index transaction recovery or serializable cross-key transactions.

TiKV RawKV, Cassandra CQL and FoundationDB transactions expose different contracts. Record the effective server/client versions, replication, consistency, disk-acknowledgment policy, batch API, topology and relevant settings. A replica acknowledgment is not automatically an fsync, and QUORUM is not automatically linearizability. Experimental RF1 and normal replicated deployments belong to distinct cohorts. Unverified properties must be labeled unverified, not inferred from a requested configuration.

Use native Tidehunter and RocksDB/integrated BlobDB as embedded local references. Do not put their embedded latency in the same service-level ranking as a competitor's network client. An application-integrated comparison may colocate our coordinator with the application, but must charge its CPU/RAM and state the boundary. An external-client experiment must include each system's real front door; do not add an extra RPC only to one system. The archived middleware is not used.

## Workload and data contract

- Logical keys are fixed 32-byte binary identities. Hash the entire identity, including the namespace and deterministic seed, rather than appending a constant run prefix to native hash-space keys. A constant high-bit prefix can concentrate native index cells.
- Values have declared sizes and entropy. The core size cases are 256 B, 1 KiB and 8 KiB. Use complete-value replacement, not YCSB's default ten-field partial-update interpretation.
- Preserve the generator version, logical seed/trace identity, actual namespace and key encoding. Equivalent logical traces can use separate owned namespaces without overwriting another run's data.
- Uniform key-bit distribution and skewed access are different controls. State the Zipf exponent, key population, value size, hot-set behavior and every operation ratio.
- The Zipf rejection-inversion generator has a fixed 1024-attempt bound. Exhaustion fails the run; it never silently substitutes a uniform distribution.
- Preload, sequential qualification, warm-up, finite aging and measurement are separate phases. Retain counts and timing for all phases; no post-hoc phase selection.
- Include read-only, write-only, read-heavy and balanced mixes. Separate scalar GET, GET-many and supported atomic writes; complete-group latency is not a measured individual-key latency obtained by division.
- Specify limits before starting: keys, logical bytes, operations, outstanding work, queue capacity, operation/phase deadlines and shutdown grace. Capacity errors remain results, not a reason to bypass native retention limits.
- Use fresh, overwrite-aged and delete-aged datasets. An aged finite workload is not proof of steady state.
- Working-set experiments must account for all application, coordinator, owner, registered, database and OS caches. A dataset exceeding one configured cache is not necessarily larger than available RAM. Collect actual memory and I/O evidence.
- Leave normal compaction/relocation/GC active where implemented and record their configuration/activity. Current distributed Tidehunter has bounded retained history and **no physical Value-WAL reclamation**. It cannot support a claim of indefinitely sustainable steady-state capacity. Report occupied bytes and maintenance debt instead of hiding that limitation.

## Scheduling and timing

Open-loop arrivals are scheduled independently of completions. Admission must not wait on a busy semaphore and thereby silently turn into closed loop. Bound work; record a rejected arrival when admission is full. Closed-loop concurrency is a separate workload model.

Record scheduled, dispatched and terminal times against one process's monotonic origin. Report scheduling delay, dispatch-to-terminal API time and scheduled-arrival response time separately. A terminal event includes errors and timeouts, not only successful requests. A timed-out mutation can still commit: classify its result as unknown, not rolled back. Retain offered/completed/rejected/error/timeout counts and disclose retries and logical versus attempted operations.

For a dispatched call require `scheduled <= dispatched <= terminal`, `API = terminal - dispatched`, `response = terminal - scheduled` and `scheduling = dispatched - scheduled`. A completion received while waiting for the next due time must be recorded without releasing that future arrival. Offline admission rejects broken clocks; zero-clamping cannot make them valid. Non-dispatched rejections retain their real response time and an explicit absent-dispatch marker, not an invented API time.

Stop the API timer after materialized bytes are available, before the independent data oracle. Perform the same declared validation policy for all backends and report its client resource cost. Keep producer, driver, queueing, application and transport scopes explicit. Do not add component p99s, subtract independent medians or infer one-way microsecond network latency from unsynchronized clocks.

Do not assume instrumentation is free. Keep per-request disk I/O and blocking scrape work off the measured path. Calibrate observer-off/on runs; use heavy perf/eBPF traces only for separate diagnostic windows. Existing low-overhead native/path microbenchmarks remain useful references, not substitutes for loaded API comparisons.

## Correctness and unsupported features

Before and after measured work, use isolated validation keys to check exact values, overwrite visibility, deletion, missing reads and ordered group membership. Do not infer final state by replaying overlapping writes in completion order. Performance success is not proof of linearizability, distributed transaction isolation or HA. Concurrent/failure-model claims require recorded histories and suitable model checkers, such as Jepsen/Elle, in a separately identified correctness study.

Ordered global scans and transactional read-modify-write are not part of the initial portable subset. A Cassandra partition scan, a native reduced-order iterator, a RawKV batch and an FDB transaction are not silently interchangeable. Optional backends/capabilities must fail explicitly when requested but unavailable; zero executed tests or a transport fallback is not a pass.

## Telemetry and authoritative artifacts

Raw client samples and per-run distributions are the latency evidence. Prometheus provides correlated operational context; Grafana visualizes it. Dashboard histogram quantiles are estimates. Retain their resolution and sample counts. Checkpoint-completion histograms measure whole-checkpoint lifecycle, not per-key microsecond latency.

Use coarse atomic live snapshots, stable labels and an explicit active/completed/failed status. Record freshness, resets and process/run identity in artifacts. Never emit stale or unavailable counters as invented zero values. Labels must not contain keys, WAL positions or request IDs. Native/Sui metric families with mixed cumulative and current gauges require semantic queries; do not blindly apply rate() to all stat labels.

The local controller is a measured application/coordinator. A research collector must be off the measured host for cloud experiments, or an explicitly isolated alternative must be shown not to perturb the workload. Host exporters must identify the actual WAL device rather than mix NVMe data I/O with the root/build filesystem.

For each run retain configuration, seed/generator identity, backend guarantees/capabilities, source and executable identity, topology and effective settings, raw/interval latency data, outcomes, phase bounds, telemetry and failure diagnostics. Retain compact seeds/hashes and generated-input definitions rather than redundant database copies. Do not rewrite historical hash-bound artifacts. Local artifact checks do not imply fresh cloud data.

## Analysis and resource accounting

First show latency versus achieved throughput, plus offered load and failure populations. Report p50/p95/p99 and only supported higher tails with sample counts. Use independent repetitions and randomized/counterbalanced system order. Run-level uncertainty matters: operations in one process are not independent deployment repetitions. Never average worker/run p99s. Merge compatible raw observations only with declared weighting and also show between-run variation.

Choose warm-up, duration and steady-state criteria before seeing results. Use the local workflow for qualification and an affordable small pilot; expand selected representative cells instead of running every possible parameter product. Larger-than-RAM, storage aging and scaling experiments require suitable independent devices before drawing cluster conclusions.

Count every data, coordinator, control-plane, transaction/log and load-generator role. Report allocated CPU/RAM as well as consumed resources and all relevant cache layers. Use a matched TCP deployment for a portable comparison; EFA and registered-memory controls are separate transport ablations, not capabilities attributed to competitors without an implemented path.

Report host-device byte amplification, occupied/live logical space, network traffic, CPU/operation and resource/cost denominators. Host write bytes do not establish physical NAND write amplification. Include all replicas and show replication separately from storage-engine amplification. Monetary inputs must be explicit estimates/rates, not presented as billing API evidence.

Automatic comparison must detect changed workloads, seeds, measurement scopes, hardware/resource budgets, acknowledgment/consistency/replication and observation modes. A deliberately different-contract comparison must display the differences and must not claim same-guarantee superiority.

## Sui application evidence

Keep Sui replay separate from generic KV measurements. Existing certified private-chain replay exercises real Sui execution, not mainnet representativeness. The latest retained fixture measured no physical read bytes; it cannot establish general cold or exact-version-object-miss behavior. Record source range, state size, keyspace/access counts and physical-I/O coverage. Adding a generic TiKV/Cassandra/FDB adapter does not create a corresponding Sui storage integration.

## Source and implementation map

- `benchmarks/kv-research/`: shared application-level backend boundary, bounded workload runner, outcome/timing artifacts and native/distributed/competitor adapters.
- `experiments/kv-comparison/`: local owned comparator lifecycle, experiment configuration, statistical/comparability analysis and compact artifact handling.
- `experiments/kv-comparison/observability/`: Prometheus/Grafana/node-exporter provisioning and current-run metrics exposition.
- `.github/workflows/kv-comparison.yaml`: credential-free local adapter/instrumentation qualification, not paid cloud performance evidence.
- `benchmarks/benchmark/src/`: existing native Tidehunter/RocksDB baseline and Prometheus exporter; preserve as the embedded reference.
- `tidehunter/src/metrics.rs` and Sui typed-store adapter: existing engine/application telemetry. No native engine or Sui behavior change is required for this comparison framework.
- `experiments/aws-sharded-wal/` and `experiments/sui-checkpoint/`: preserved source-bound cloud/Sui campaigns. Their protected branch activation, exact-owned cleanup, budget ledger and deadline remain separate. The existing cloud harness's five-owner assumptions are not evidence of arbitrary-N support.

## Primary references and exact use

- [YCSB, SoCC 2010](https://doi.org/10.1145/1807128.1807152): common workload/binding approach, operation mixes and latency/throughput evaluation. Use declared opaque-value workloads rather than assuming its record-field model equals our API.
- [go-ycsb](https://github.com/pingcap/go-ycsb): generator/adapter reference only. Its current FoundationDB Update performs read/decode/merge/write and its client dependencies need release qualification; do not charge that avoidable RMW to a blind PUT experiment.
- [TiKV RawKV client](https://github.com/tikv/client-go/tree/master/rawkv) and [Titan](https://docs.pingcap.com/tidb/stable/titan-overview/): actual point/batch semantics and key/value separation. Current docs enable Titan by default for new clusters since v7.6; old experimental-only descriptions are not current configuration evidence.
- [FoundationDB architecture](https://apple.github.io/foundationdb/architecture.html): transactional boundaries, read versions, role accounting and fsync-backed transaction logging. Version-specific effective settings still require qualification.
- [Cassandra storage engine](https://cassandra.apache.org/doc/stable/cassandra/architecture/storage-engine.html): commit-log synchronization, memtables, SSTables and maintenance; consistency level alone does not establish disk acknowledgment or cross-key isolation.
- [Prometheus histograms](https://prometheus.io/docs/practices/histograms/) and [node_exporter](https://github.com/prometheus/node_exporter): histogram/quantile aggregation and host context, not a replacement for raw API observations.
- [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/): reproducible data sources/dashboards rather than manually configured research evidence.
- [HDR Histogram](https://github.com/HdrHistogram/HdrHistogram): bounded-resolution distribution methodology; retain units, sample counts and correction policy.
- [Jepsen](https://github.com/jepsen-io/jepsen) and [Elle](https://github.com/jepsen-io/elle): separate history/model-based correctness studies, not guarantees inferred from performance completion.
- Native Tidehunter paper (`project:papers/parsed/Tidehunter-2026-v2.md`), write/read design and evaluation: foreground versus explicit durability, WAL values/cache layering, native RocksDB/BlobDB controls and oversized-data methodology.

## Local verification, 2026-10-24

Implementation branch: `feat/distributed-kv-comparison`. No AWS resources were
launched. The inherited engine, Sui fork and paid AWS/Sui campaign sources were
not modified.

The rebuilt `kv-research` executable exercised all seven real adapters. The
distributed local layouts used five Tidehunter storage-owner processes, three
TiKV stores plus PD, three Cassandra nodes, and three FoundationDB processes
with `double` redundancy. These are logical local layouts on one shared SSD,
not independent physical failure domains.

- Seven 1 KiB mixed-workload cells completed with 512 measured calls each.
- Native, Tidehunter/TCP and BlobDB also completed 8 KiB, batch-32 cells with
  128 measured calls each, including atomic PUT batches and ordered GET-many.
- Final strict admission retained 10 distinct contracts and 3,968 measured
  calls: 3,945 successful and 23 bounded-admission rejections. No backend error,
  wrong-value or timeout population was present. Rejections remain in the
  denominator; they are not silently removed to improve percentiles.
- Every declared preload/warm-up/aging/measurement phase and each raw clock
  decomposition was checked before campaign success publication.
- Comparison Rust suite: 22 passed. Controller/analysis suites: 38 passed.
  Monitoring semantic suite: 12 passed.
- Inherited Tidehunter library suite, with `experimental-distributed,test-utils`:
  805 passed, 7 ignored. Ignored cases are not evidence of hardware coverage.
- Scoped new-code Clippy passed with `--no-deps`, all adapter targets,
  `clippy::pedantic` and `-D warnings`. Formatting was checked without
  reformatting the shared inherited TCP harness.
- Prometheus/Grafana verification executed all 21 provisioned panel query sets
  against the real datasource. Browser inspection separately confirmed populated
  panels, completed/data-available/final-snapshot values of 1, and drain phase 0.
- All owned database containers, data volumes and `kv-research` processes were
  closed. The installed loopback-only monitoring stack remains available.

Evidence is retained, but not committed as generated benchmark data:

```text
checkpoint-work/kv-comparison/local-complete-r2/
checkpoint-work/kv-comparison/local-large-r2/
checkpoint-work/kv-comparison/local-validation-final/comparison.json
checkpoint-work/kv-comparison/local-validation-final/comparison.csv
checkpoint-work/kv-comparison/local-validation-final/comparison.md
checkpoint-work/kv-comparison/monitoring-final-proof.json
```

Validation found and fixed three concrete problems before accepting these
artifacts: the initial FoundationDB cache exceeded its configured memory bound;
the pinned Cassandra script overwrote `JMX_PORT` and collided across local
nodes; and the new open-loop runner treated a completion as permission to release
a future arrival. The scheduler regression failed before the fix and passed
after it. Earlier development timing artifacts are rejected, not retroactively
rewritten or presented as valid comparisons.

These finite **debug-build functional runs are not publication performance
results**. Single repetitions do not produce confidence intervals. Cache
residency, larger-than-RAM/device access, matched durability/RF/resource budgets,
independent NVMe scaling, fault histories and RDMA remain separate experiments.
The new GitHub workflow was parsed and its eight shell sections syntax-checked;
it was not pushed or executed on GitHub in this local-only task.

The broader inherited-engine pedantic invocation reported 1,253 diagnostics,
including cast warnings. That pre-existing audit is not clean; it was not
silenced or mixed into this tooling change. The scoped new comparison code
passes its strict check. No matching project skill files were present to update;
the new procedures are documented here and in the comparison READMEs.
