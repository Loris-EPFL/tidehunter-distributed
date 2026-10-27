# Distributed KV comparison protocol

This protocol separates comparative research from engine optimization. Local execution establishes API, lifecycle, accounting and artifact behavior. It does not establish independent-NVMe, EFA, scaling or high-availability performance. No AWS credentials or paid campaign are required by the local comparison workflow.

## Transport configuration and evidence

The distributed transport is not a separate storage protocol: coordinator
index, owner WAL, read grants, existing locks, and durable append receipts
remain unchanged. The engine `Transport` defaults to TCP; an explicit OFI
configuration selects one `FabricBackend` (`efa`, `efa-direct`, `verbs`, or
`software-tcp`) and `FabricSecurity` (`mutual-tls` or `trusted`). Canonical
example:

```json
{"kind":"ofi","fabric":{"backend":"verbs","domain":"mlx5_0","source_address":"10.1.0.2"},"security":"trusted"}
```

`domain` and `source_address` are optional host-local selectors. Supply each
host's own selectors as needed; peers must select the same backend and
security, while selectors need not be identical. Source-IP constraints depend
on the provider: EFA/EFA-direct reject a source IP because `FI_ADDR_EFA` is
opaque; verbs accepts IPv4 only because pinned RxM lists `FI_SOCKADDR` and
`FI_SOCKADDR_IN`, even where core verbs supports IPv6; software-tcp accepts
IPv4/IPv6 when `fi_getinfo` returns a matching sockaddr. With no selector,
each provider uses its default. Metadata reports a selected address only when
it is a parseable IPv4/IPv6 sockaddr. An explicit source is checked against
the selected OFI address, not echoed from config. Unsupported bindings,
protocol mismatch, or unmet required device-read capability fail; transport
never silently downgrades to TCP.

EFA/EFA-direct and generic verbs are separate provider paths. IB, RoCE, and
Falcon use verbs/RxM when supported by the selected host's provider, but are
not interchangeable wire fabrics; EFA SRD is not verbs. A provider name or
requested setting is not hardware proof. Retain actual provider/setup metadata
and capability provenance. For EFA/EFA-direct, native-read proof requires
`FI_OPT_EFA_EMULATED_READ == false`. For verbs, it requires provider PCI
NIC/device metadata matched to canonical sysfs PCI BDF; software `rxe`,
`rdma_rxe`, and `siw` devices are excluded. Verbs proof does not establish
Falcon identity. Software OFI FI_READ validation proves only software
correctness. Do not claim hardware qualification or performance without
parent-reviewed hardware evidence. Transport does not alter storage locking
or WAL/durability semantics.

Benchmark backend kind is unified as `tidehunter`; case identities such as
`tidehunter_tcp`, `tidehunter_efa`, `tidehunter_efa_direct`, and
`tidehunter_efa_native` remain historical reporting labels, not configuration
aliases. Preserve provider/security distinctions in any new labels and never
compare trusted and mutual-TLS cohorts as security-equivalent.

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

### Measurement-boundary and sampled write diagnostics

Keep the ordinary unprofiled matrix intact. An explicit
`diagnostics.write_profile_limit` adds separate native/Tidehunter write-diagnostic
cohorts; it does not replace primary comparisons or make profiled API latency
overhead-free. The default has 253 ordinary and five diagnostic cells per
repetition. Zero disables the additions. Neither mode reduces any within-case
phase, operation/sample count, backend, transport, size or correctness check.

Capture owned process counters at the pre-measurement gate and again while the
completion gate holds the still-live client, before profile finalization,
post-workload qualification, drain and shutdown. Bind the original boundary
frames and derived `resource-interval.json` by hash. Require identical boot,
cgroup path/inode, positive host-local monotonic elapsed time and nondecreasing
counters. Missing/reset/changed identities are unavailable, not zero. Report
all-cgroup-device IO separately from mounted-data-NVMe host-device counters.
End memory and cgroup-lifetime peak are gauges, not per-case memory maxima.
The broader through-exit host window remains separate. None of these intervals
is pure API time or engine-only CPU.

Arm write profiling after warm-up, not at database open. Native benchmark-only
scalar mutation entry points time the existing WAL append and index application;
the existing Value-WAL persistence profiler supplies a separate barrier component.
Ordinary native algorithms, acknowledgment points and locks are unchanged.
Distributed owner/connection profiles have a per-connection append-request cap,
not a global logical-write cap. Their component request counts can differ:
the local qualification observed 1280 owner samples and 1299 client requests.
Use each component's own count; do not normalize every clock by owner samples.

The distributed `coordinator_interval` is a separate full measurement-boundary
delta population: admission/preparation, dispatch, append/group counts, storage
wait, publication, checkpoint and durable-prefix blocking. It is not restricted
to the owner prefix. Overlapping/nested clocks, idle-inclusive receive clocks and
different populations are not additive percentages or measured network-only time.
Per-stage totals do not provide per-operation stage percentiles.

Preserve raw query evidence as hash-addressed bounded pages, not a single
monolithic verification response. Replay the exact recorded `(start,end]`
boundaries, including sparse one-point series; reject changed samples, page
gaps, overlaps and warnings. Do not invent rate samples or repair legacy missing
query hashes. The actual host/cgroup scrape period is five seconds and terminal
benchmark gauges use one second. A one-point series cannot prove no throttling.
Labelled terminal tables and original raw API/scheduling/response samples remain
different evidence sources. Observer cohosting and profiling overhead require
separate calibration before any overhead-neutral research claim.


## Analysis and resource accounting

First show latency versus achieved throughput, plus offered load and failure populations. Report p50/p95/p99/p99.9, mean, population standard deviation and maximum with sample counts and tail warnings. Preserve every operation's raw API, scheduling and response timings. Operations in one process are not independent deployment repetitions. Keep each case's quantiles separate; label any descriptive median of compatible per-repetition quantiles explicitly rather than presenting it as a pooled percentile. Do not pool incompatible populations or zero-fill missing observations.

Choose warm-up, duration and steady-state criteria before seeing results. Use the local workflow for qualification and an affordable small pilot; expand selected representative cells instead of running every possible parameter product. Larger-than-RAM, storage aging and scaling experiments require suitable independent devices before drawing cluster conclusions.

### Development versus publication

Development defaults to one independent repetition of every case, with all
backends, transports, sizes, workloads, concurrency/load levels, cohorts, preload,
warm-up, correctness checks and within-case operation/sample counts unchanged.
Label the result single-repetition and show no run-to-run error bars, confidence
interval or significance claim. Publication and explicit reproducibility studies
keep multiple independent repetitions under a declared protocol; neither two nor
three runs alone justify describing variation as statistically insignificant.

Use existing matched historical repetitions to assess whether observed spread
can change the stated performance conclusion. Declare the materially important
effect threshold before a rerun. The current development-triage policy uses
10% relative latency with a 1 µs absolute floor, 10% throughput, and one percentage
point of rejection fraction. These are engineering thresholds, not proof of
equivalence. Freeze any different thresholds before measurement.

Do not add development repetitions when the observed spread cannot change that
conclusion, even if it is unfavorable. Repeat only relevant uncertain or regressed
workload configurations, using frozen sources and identical declared settings
across every backend involved in that comparison. Do not rerun only a preferred
backend. Changed resources, acknowledgment, workload or observation modes make
a regression comparison confounded. Do not modify an active campaign, discard
existing repetitions, or launch another full campaign just to obtain a missing
third repetition.

The [comparison README](../../experiments/kv-comparison/README.md#development-repetition-policy)
records planning/reporting behavior and the preserved historical variability
assessment. Missing observations remain missing; no follow-up runs are launched
automatically.

Count every data, coordinator, control-plane, transaction/log and load-generator role. Report allocated CPU/RAM as well as consumed resources and all relevant cache layers. Use a matched TCP deployment for a portable comparison; EFA and registered-memory controls are separate transport ablations, not capabilities attributed to competitors without an implemented path.

Report host-device byte amplification, occupied/live logical space, network traffic, CPU/operation and resource/cost denominators. Host write bytes do not establish physical NAND write amplification. Include all replicas and show replication separately from storage-engine amplification. Monetary inputs must be explicit estimates/rates, not presented as billing API evidence.

Automatic comparison must detect changed workloads, seeds, measurement scopes, hardware/resource budgets, acknowledgment/consistency/replication and observation modes. A deliberately different-contract comparison must display the differences and must not claim same-guarantee superiority.

### Valkey TCP memory-store cohort

`valkey_tcp` adds 25 common cells per repetition without removing any of the
previous 228 ordinary cells or the five separate native/Tidehunter diagnostics.
The current default is 258 cases; three repetitions give 774. Historical reports
must use their retained frozen job list, not be replanned as a nine-backend run.
Missing historical Valkey observations are absent, never zero.

The distributed layout is five Valkey 9.0.7 primaries, RF1 and zero replicas, using
the exact-pinned redis-rs 1.8.0 TCP client. Its MGET cluster routing restores
binary-key input order, missing results and duplicates across slots. Cross-shard
atomic PUT batches are unsupported. Record plain unauthenticated TCP, the actual
slot map, primary count, server/client versions, resource limits and live AOF
policy; do not infer equal guarantees from a shared KV API.

Valkey GET is a **RAM-resident store read**, not a Value-WAL/NVMe cache-miss read.
AOF `appendfsync=always` and `no-appendfsync-on-rewrite=no` are a separate
write/recovery contract. Keep `noeviction` and normal AOF rewrite policies,
retain actual persistence/maintenance state, and report capacity errors rather
than changing the policy to complete a run. A local five-process cluster still
has one physical host and shared SSD; it proves neither distributed scaling nor
InfiniBand performance.

### RDMA-enabled comparator qualification

Add an RDMA comparator only as a separately qualified transport/storage cohort,
not as an unsupported capability of the current TCP/transaction adapters.
The implemented Valkey comparator above remains TCP-only.
[Valkey's experimental RDMA transport](https://github.com/valkey-io/valkey/blob/9.0.7/README.md)
uses librdmacm/libibverbs and still executes ordinary commands on the server.
The inspected [RDMA implementation](https://github.com/valkey-io/valkey/blob/9.0.7/src/rdma.c)
uses connected verbs/registered transfer buffers; it is not a client directly
reading the database's WAL one-sided. Use the pinned
[9.0.7 security release](https://github.com/valkey-io/valkey/releases/tag/9.0.7),
not an unpatched or floating build.

First qualify standalone Valkey TCP versus RDMA on matched compatible hardware.
Its RDMA-CM/RC path is not established to work unchanged with
[EFA's Libfabric RDM provider](https://ofiwg.github.io/libfabric/main/man/fi_efa.7.html).
A ConnectX/RoCE/IB experiment is a different hardware/provider cohort; Tidehunter
also needs a qualified path on that hardware before transport attribution is fair.
Do not label standalone RDMA qualification as proven cluster-client, replication
or cluster-bus RDMA support.

[Valkey AOF policies](https://valkey.io/topics/persistence/) remain separate:
no AOF, `everysec`, and `appendfsync always` have different acknowledgment and
recovery contracts. AOF command logging/rewrite is not Tidehunter's owner-local
Value WAL, even with fsync-before-reply. Charge all client/server/control roles,
registered RAM, polling CPU, replicas and storage work. The separate
[`experiments/valkey-infiniband`](../../experiments/valkey-infiniband/README.md)
tool supplies offline planning/preflight and bounded functional qualification of
an existing operator-supplied InfiniBand endpoint, with mandatory RDMA commands
and no TCP fallback. It is not imported into the active suite or CI backend
matrix. Its existence and unit tests are not hardware validation or RDMA
performance evidence.

[HERD](https://doi.org/10.1145/2619239.2626299) and
[FUSEE](https://www.usenix.org/system/files/fast23-shen.pdf) are useful separate
remote-memory research references, not durable-NVMe-WAL baselines. HERD's reported
microsecond average is not a portable p99 target or evidence of SSD access.


## Sui application evidence

Keep Sui replay separate from generic KV measurements. Existing certified private-chain replay exercises real Sui execution, not mainnet representativeness. The latest retained fixture measured no physical read bytes; it cannot establish general cold or exact-version-object-miss behavior. Record source range, state size, keyspace/access counts and physical-I/O coverage. Adding a generic TiKV/Cassandra/FDB/Valkey adapter does not create a corresponding Sui storage integration.

## Source and implementation map

- `benchmarks/kv-research/`: shared application-level backend boundary, bounded workload runner, outcome/timing artifacts and native/distributed/competitor adapters.
- `experiments/kv-comparison/`: local owned comparator lifecycle, experiment configuration, statistical/comparability analysis and compact artifact handling.
- `benchmarks/kv-research/src/backends/valkey_tcp.rs`: optional TCP-only cluster client, live policy admission, native MGET splitting and typed transport-failure outcomes.
- `experiments/valkey-infiniband/`: deliberately separate future hardware qualification, excluded from active benchmark manifests/package sources.
- `experiments/kv-comparison/observability/`: Prometheus/Grafana/node-exporter provisioning and current-run metrics exposition.
- `experiments/kv-comparison/resource_accounting.py`: bounded owned-cgroup capture, identity/reset validation and reproducible terminal interval deltas.
- `experiments/kv-comparison/cloud_campaign.py` and `cloud_controller.py`: separate diagnostic planning, live completion gate, raw/profile admission and integrity bindings.
- `benchmarks/kv-research/src/runner.rs` and `backends/{native,tidehunter}.rs`: measurement-only profile arming, bounded selection, component scopes and final artifacts.
- `tidehunter/src/db.rs`: benchmark-only native scalar append/index clocks; `distributed/remote_db.rs` and `remote_db/concurrent.rs`: dynamic profile control and fresh coordinator snapshots.
- `.github/workflows/kv-comparison.yaml`: credential-free local adapter/instrumentation qualification, not paid cloud performance evidence.
- `benchmarks/benchmark/src/`: existing native Tidehunter/RocksDB baseline and Prometheus exporter; preserve as the embedded reference.
- `tidehunter/src/metrics.rs` and Sui typed-store adapter: existing engine/application telemetry. New benchmark-only profiling hooks do not change ordinary native algorithms, acknowledgment, locks or Sui behavior.
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
