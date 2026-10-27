# KV comparison observability

Two modes share the pinned Prometheus, Grafana, node_exporter and Python images:

- **Campaign mode:** `campaign_monitor.py` collects explicitly admitted fleet
  targets, preserves a real Prometheus snapshot, and restores it offline.
- **Single-run local mode:** `stack.py` observes one explicit result directory.
  Its commands and limitations are below. It does not contact AWS.

Neither mode changes Tidehunter, Sui, database durability or workload semantics.
These tools observe the generic KV comparison harness; adding this integration
does not automatically add monitoring to the separate Sui checkpoint runner.

## Live cloud campaigns

`cloud_controller.py` starts campaign monitoring on the admitted fleet. Bootstrap
caches the digest-pinned images before measured work; package preparation includes
the monitor, exporters, dashboard and pins in the source-bound closure.

The collector is deliberately **cohosted on the measured controller**, not on a
seventh paid instance. TSDB state resides on the root filesystem under
`/var/lib/tidehunter-observation/<run_id>`, not the benchmark data mount.
Per-container CPU, memory, process and log limits bound resource use. They do
**not** prove zero measurement interference. Independent observation or matched
observer-off/on calibration is needed before attributing small differences.

The six admitted hosts receive explicit private-IP targets; arbitrary service
discovery, public ingress and host-network scans are not used. Host exporters bind
only the admitted private address. Grafana and Prometheus bind controller loopback.
Default campaign ports:

| Endpoint | Port | Access | CPU cap | RAM cap |
|---|---:|---|---:|---:|
| Grafana | 13000 | controller loopback, authenticated | 0.5 | 768 MiB |
| Prometheus, including admin API | 19090 | controller loopback | 1 | 1 GiB |
| Case exporter | 19899 | controller loopback | 0.25 | 256 MiB |
| node_exporter, per host | 19100 | admitted private address | 0.1 | 128 MiB |
| Cgroup exporter, per host | 19898 | admitted private address | 0.1 | 128 MiB |

Use `results/observability/access.json` from the running campaign. It records the
actual coordinator instance ID, region, private SSM port-forwarding commands and
required permissions. Forward the loopback ports; do not expose the admin API or
Grafana on a public security-group rule. SSM access requires working agent,
network and operator/instance permissions; recording a command does not grant IAM
permissions. The login is `researcher`; the generated password stays in the
session's private state. It is not included in result archives.

Before a case, the controller atomically selects its identity and explicit owned
process cgroups. After raw-result validation and **outside API measurement**, it
publishes the terminal summary and waits for a fresh scrape of that same case.
This bounded observation barrier preserves cases shorter than the scrape interval.
It does not repeat the workload. A failed case publishes failure, not invented
successful requests or zero latency.

Before fleet teardown, the controller exports:

- `results/observability/monitoring.tar.gz` and its SHA-256 receipt;
- startup, case-observation, completeness/gap, query and cleanup receipts;
- exact target and image identities, dashboard/configuration, capture time range
  and a real TSDB snapshot **including its current head**.

These files join the existing raw-result archive and S3 retrieval path. Monitoring
failure remains explicit but cannot prevent raw-result export or the fleet's
unconditional termination path. Startup checks exact listen ports, container
ownership/running/image identity and Prometheus session identity; partial startup
rolls back only newly attempted owned containers.

This adds no managed-monitoring subscription or extra EC2 host. Collector CPU,
root-volume writes, larger result archives and S3 retention/transfer still cost
resources. Local qualification is not a cloud cost or overhead measurement.

## What the campaign dashboard measures

Dashboard UID: `kv-comparison-campaign`. It contains 28 panels. Select
**one run and one case**; do not pool different value sizes,
durability contracts, cache cohorts, repeats or backends.

- Exact terminal API/response/scheduling quantiles, sample counts, offered,
  successful and rejected calls, calls/s, logical keys/s and value MiB/s come from
  validated raw results. Batch latency remains the complete group latency.
  The clocks are distinct scopes, not additive engine stages.
- Live rates and histogram quantiles are coarse rolling observations. Histogram
  quantiles are bucket estimates, not the exact raw-sample terminal quantiles.
  A short population can have one scrape and no valid rate; no data is not zero.
  The **actual** generated campaign configuration scrapes host/cgroup targets at
  5 seconds and terminal benchmark gauges at 1 second. A one-point cgroup series
  cannot establish a rate or no throttling; neither max-minus-min nor zero-fill
  is used. Workloads are not prolonged to obtain more scrapes. These sampling
  choices and cohost observer overhead remain uncalibrated.
  [Prometheus `rate`](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate)
  handles counter resets and extrapolates; terminal counter deltas are not that
  estimator. `rate-regressions.yaml` exercises the real PromQL engine for sparse,
  absent, observed-zero and reset cases rather than emulating rate in Python.
- All six hosts expose CPU, memory, PSI, diskstats and network context. Explicit
  owned cgroups expose available CPU/throttling, current/peak memory, OOM and
  device-I/O counters. Shared-host statistics and rolling windows can include
  preload or teardown; they are not exact per-case physical-I/O attribution.
  The access receipt preserves observed data-device mappings; unknown mappings
  remain unknown.
- Logical native WAL bytes are not physical device bytes. Page-cache/WAL
  submissions do not establish an NVMe fetch.
- Available coordinator/owner/resident read-path counters are labeled
  **adapter-lifetime**. They are not measurement-only hit ratios or RDMA timings.
  Missing owner evidence, disabled profiling and unsupported cgroup fields stay
  absent or explicitly unavailable.
- Optional terminal `resource_evidence` is a separate, validated before/end
  accounting source, not a Prometheus live-rate substitute. Its host-local
  monotonic interval includes driver/oracle, gate/control-plane overlap and
  background work; it is **not** pure per-operation CPU or measurement-only stage
  time. The controller checks same boot/cgroup inode and nonnegative deltas.
  IO byte totals cover all owned-cgroup devices (raw frames retain devices).
  Memory current is an end-boundary gauge; memory peak is cgroup-lifetime, not a
  measurement-only maximum. Missing fields remain missing. Unavailable/replaced/
  reset rows export availability=0 but no numerical resource fields.

The bounded terminal object is schema version 1 with
`scope="measurement_boundary_interval"` and a `processes` list (at most 16 unique
`host_index,process_role` identities). Host index is an integer -1 through 4;
roles use the existing bounded metric token syntax. Each row requires boolean
`available`. Unavailable rows contain only identity, availability and a nonblank
`reason` of at most 512 characters. Available rows require finite positive
`elapsed_seconds`; optional `cpu_seconds,cpu_throttled_seconds` are finite
nonnegative seconds and `cpu_periods,cpu_throttled_periods,io_read_bytes,
io_write_bytes,memory_current_bytes,memory_peak_bytes` are nonnegative integers.
Throttled periods cannot exceed observed periods when both exist. Unknown
fields are rejected. The exported `kv_case_resource_interval_*` metrics are
**gauges**, labeled by `scope,resource_host_index,process_role`; `resource_host_index`
avoids colliding with the benchmark exporter's own scrape-target `host_index`.
Absent resource evidence emits no resource gauges, not available=0 or zero usage.

Optional `finish-case --write-diagnostics FILE` (or `finish_case(...,
write_diagnostics=...)`) publishes a real schema-1 `write-diagnostics.json`.
Accepted top-level fields are `schema_version,backend,phase,phase_attempted,
runtime_enabled,runtime_observation_mode,selection,clock_scope,overlap_semantics,
unsupported_stages,profile,error`. `phase` must be `measurement`; selection has
`requested_limit,observed_samples,selection_policy,phase_exclusions`. Limits are
64 KiB of compact JSON, 512 numeric fields, at most 512 keys per object,
six object/list nesting levels,
32 list items and 2,048 characters per metadata string. Requested limit must be
1 through 65,536. All numerical artifact fields must be nonnegative integers.
Stage trees are bounded to four nested levels; unknown scalar metadata inside
stages is rejected except `status/reason`. Unsupported stages may not claim
zero counters.

For `scope=sampled_prefix`, `kv_case_write_diagnostic_available` requires actual
runtime-enabled evidence, positive observed samples, an attempted measurement
and an error-free nonempty stage profile. `kv_case_write_diagnostic_operations`
preserves requested-limit and observed-sample populations without equating
their scopes.
`kv_case_write_diagnostic_stage_seconds` converts actual accumulated `_ns`
counters to seconds; `kv_case_write_diagnostic_stage_count` preserves observed
call/sample/request/error/success/connection and related operation counts.
Native `profile.value_wal_persistence` retains its separate component scope.
These are terminal **gauges** with `phase=measurement,scope=sampled_prefix`,
not live rates, per-operation percentiles or proven-representative sampling.
Nested/independent stage clocks overlap and are **not additive latency
fractions**, pure network time or zero-overhead instrumentation. Exact policy,
clock descriptions, unsupported stages and errors remain in the admitted
artifact. Absence remains No data; unavailable prefix profiles expose no
sampled-prefix stage gauges.
The page/hash capture includes every new diagnostic metric family.

The full current TCP profile has 268 stage fields plus the separate coordinator
window; the former 256-field limit rejected a valid real artifact. The 512-field
budget admits that profile without clipping and does not relax the 64-KiB artifact,
16-MiB query-page or total archive limits. Terminal publication validates the
complete metrics/resource/profile payload before replacing the active selection.
An invalid caller payload cannot poison a working scrape target.

Diagnostic panels are labelled tables, not dense grids of anonymous values.
The `metric` column gives count/availability/seconds semantics; `detail` retains
the original stage, counter or sample population. A `_ns` stage name is converted
to seconds only when `metric=stage_seconds`. CPU period counts remain explicit;
a throttled-period fraction is undefined for zero observed periods. Query validation
uses each panel's actual table/time-series format and preserves PromQL `$1`
capture replacements and regex end anchors during Grafana-variable expansion.


An independent optional `profile.coordinator_interval` carries
`{scope:"measurement_boundary_cumulative_delta",clock_scope:<nonblank string>,
stages:{<whitelisted counter>:<nonnegative integer delta>}}`.
It is a **full measurement-boundary cumulative-counter window**, not the bounded
owner/transport/native prefix. The Rust adapter validates monotonic before/end
counters; a missing/reset counter yields `profile.coordinator_interval_error`
and no interval object, not a substituted zero. Interval error and successful
interval evidence are mutually exclusive. The exporter rejects unknown gauge/
peak keys, wrong scope, malformed clocks and invalid counters. Accepted counter
keys are:

```
write_storage_wait_ns
write_publication_ns
write_dispatch_ns
write_preparation_admission_ns
write_checkpoint_ns
write_checkpoint_count
write_foreground_checkpoint_ns
write_foreground_checkpoint_count
write_background_checkpoint_ns
write_background_checkpoint_suffix_batches
write_durable_receipt_publication_blocked_ns
write_durable_receipt_publication_blocked_batches
write_append_requests
write_direct_appends
write_durable_batches
write_concurrent_admitted
write_concurrent_completed
```

These values use the same stage-seconds/count families but
`scope=measurement_boundary_cumulative_delta,component=coordinator_interval`.
Availability requires an attempted measurement and a valid interval;
**owner-prefix runtime/sample availability does not gate it**. A recorded
interval error exports availability=0 without coordinator stage values; absent
optional interval evidence exports no coordinator values. The dashboard labels
both populations explicitly. Do not add their overlapping stage totals,
normalize full-window coordinator counters by prefix sample counts, infer
per-operation percentiles, or turn the populations into latency fractions.

Normal runs remain unprofiled; diagnostic runs are a separate observation mode.
The dashboard still cannot supply unmeasured TLS/fsync/coordination breakdowns,
pure network RTT, hardware-RDMA latency, per-request NVMe latency, or
vendor-specific compaction/GC internals. Use raw CSV, diagnostic artifacts,
deployment receipts and guarantee metadata for the research comparison.

## Retain and replay offline

Do not copy an active TSDB directory or replace old sample timestamps with
current time. `export` uses the
[Prometheus snapshot API](https://prometheus.io/docs/prometheus/latest/querying/api/#snapshot)
with `skip_head=false`, and records the original capture bounds. This follows
the [storage backup guidance](https://prometheus.io/docs/prometheus/latest/storage/#backups).
The bounded archive includes a SHA-256 manifest, not Grafana's SQLite database,
admin password or cloud credentials.
Raw query evidence is retained in schema-2 bounded `(start,end]` pages (at most
300 seconds each, recursively split on oversized responses down to one second).
These are instant queries of **raw range vectors**, not stepped range queries:
every observed timestamp/value is retained, including a sole sample. Canonical
series ordering makes query hashes independent of response row order. Receipts
contain bounded page identities/hashes, never the combined raw payloads.
`evidence/query-evidence/<query_sha256>.json` files are individually limited to
16 MiB and separately bound by the archive manifest; archive/member limits are
unchanged. Query warnings fail evidence collection rather than accepting
truncated results. A still-oversized one-second response is an honest gap.
Replay re-executes exactly the captured page boundaries and compares hashes,
checks complete nonoverlapping metric/time coverage, and retains capture gaps.
Legacy archives use their original full-range query hashes; missing historical
hashes remain missing and prevent parity claims. No old archive is rewritten or
silently assigned new baseline hashes.
The paging interval follows Prometheus'
[left-open/right-closed range selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors);
adjacent pages do not duplicate boundary samples.

Replay requires the archived digest-pinned images already cached locally.
Campaign `start` and `replay` never pull implicitly. Online preparation can use
`campaign_monitor.py --spec SESSION.json pull`; the campaign bootstrap does this
before measurement. Retain the images as well as the result bundle if the
analysis machine will be fully disconnected.

Run from the checkout root. This retained local qualification archive is usable
without AWS or a running database:

```bash
OBS=experiments/kv-comparison/observability
ARCHIVE=checkpoint-work/kv-comparison/monitoring-online-offline-2026-10-25/th-monitor-20261025142334/monitoring.tar.gz
REPLAY_STATE="$(mktemp -d /tmp/tidehunter-monitor-replay.XXXXXX)/state"
python3 "$OBS/campaign_monitor.py" replay \
  --archive "$ARCHIVE" --state-dir "$REPLAY_STATE"
python3 "$OBS/campaign_monitor.py" --spec "$REPLAY_STATE/spec.json" access
cat "$REPLAY_STATE/grafana_admin_password"
python3 "$OBS/campaign_monitor.py" --spec "$REPLAY_STATE/spec.json" verify
# When analysis is finished:
python3 "$OBS/campaign_monitor.py" --spec "$REPLAY_STATE/spec.json" stop
```

For a retrieved cloud run, set `ARCHIVE` to that run's
`results/observability/monitoring.tar.gz`. Always use a fresh state leaf. Default
replay endpoints are Grafana `127.0.0.1:23000` and Prometheus `127.0.0.1:29090`;
`access` returns the URL with the archive's original absolute time range.
The restored Prometheus has **no live scrape jobs**. Replay checks original
query hashes, authenticates Grafana and executes all source queries through its
real datasource API. It never pretends old telemetry was collected now.

Live retention is 7 days / 2 GB of blocks; block-size retention is
[not a hard disk quota](https://prometheus.io/docs/prometheus/latest/storage/#operational-aspects).
The WAL/head can require more space. Startup requires 6 GiB free on the root
filesystem; export/extraction is bounded to 4 GiB and 100,000 files. Replay
adjusts time retention for the archive's age. Local analysis containers run until
explicitly stopped; an operation deadline is not a daemon shutdown scheduler.
Cloud fleet deadlines and termination remain the campaign orchestrator's job.

## Local qualification, 25 October 2026

Evidence: `checkpoint-work/kv-comparison/monitoring-online-offline-2026-10-25/`.
Two actual native populations were admitted and validated: an open-loop mixed
case with 3,000 offered calls, and a 32-call, 8-KiB read-many case whose measurement
lasted 4.46 ms. Both terminal observations reached Prometheus. All six logical
host targets were healthy, and an explicitly labeled qualification worker
produced observed cgroup CPU/throttle counters.

After snapshot/export and full live shutdown, a fresh replay preserved all nine
checked exact counts/quantiles, with zero live scrape targets. Live and offline
Grafana pages were opened and captured. Failed-case/no-fabricated-count behavior,
occupied-port rejection, real partial-start rollback and unrelated-session
preservation were exercised. The single-run stack also started from cached images,
passed its real datasource queries, stopped and restored its prior selection.

The host targets share one laptop kernel and SSD; the cgroup worker was not
claimed as benchmark-client CPU accounting. These are functional qualification
results, **not** new comparative performance results or EC2/EFA/independent-NVMe
validation. No AWS resources were launched for this implementation.

## Single-run local mode

This Linux-only stack observes one **explicit current result directory**. It does
not start a database or discover runs. Prometheus and Grafana run in local Docker;
the exporter uses only Python's standard library. Its dashboard UID remains
`kv-comparison-local`.

## Start with a real run

Run these commands from the `tidehunter-sharded-wal` checkout. Requirements:
Python 3.10+, native Linux Docker Engine, and Docker Compose v2+ with `up --wait`.
Use [Docker's official Engine installation instructions](https://docs.docker.com/engine/install/ubuntu/)
if Docker is absent. Do not pipe an unreviewed installation script into a root shell.
Docker access is root-equivalent; the services themselves get no Docker socket.
Docker Desktop/WSL host-network behavior is not qualified by this setup.

```bash
OBS=experiments/kv-comparison/observability
# Create only the parent. The harness owns and creates the fresh result leaf.
mkdir -p checkpoint-work/kv-comparison
RESULTS="$(pwd)/checkpoint-work/kv-comparison/native-$(date +%Y%m%d-%H%M%S)"
python3 "$OBS/stack.py" configure --results "$RESULTS"
python3 "$OBS/stack.py" start
python3 "$OBS/stack.py" password
# Login user: researcher. Password is generated locally, never a committed default.
cargo run --locked -p kv-research -- \
  --config benchmarks/kv-research/example.json --output "$RESULTS"
python3 "$OBS/stack.py" status
python3 "$OBS/stack.py" verify --output "$RESULTS/observability-query-check.json"
```

Start the observer **before** the harness to see live samples. Do not pre-create
the result leaf: the harness rejects reuse. `configure` does not create it. The
immediate parent must already exist; supply an absolute path with no `..` segments.
For an uncreated leaf, only its canonical parent is mounted read-only into the
exporter, which opens only that named child and two fixed basenames. Prefer a
**dedicated results parent**, not a directory containing unrelated private files.
For an already existing result directory, only that directory is mounted.
No parent/run scanning occurs. A symlink run leaf or snapshot file is rejected.

Endpoints (all bind **127.0.0.1 only**):

- Grafana: <http://127.0.0.1:3000/d/kv-comparison-local>
- Prometheus: <http://127.0.0.1:9090> (targets: `/targets`)
- Comparison exporter: <http://127.0.0.1:9899/metrics>
- node_exporter: <http://127.0.0.1:9100/metrics>

`start` pulls only pinned images. Use `start --offline` to require the cached
pins without registry access; a missing image fails before service startup.
Both modes record actual local image IDs/repository digests in
`.state/resolved-images.json`, recreate these four services with `--pull never`,
and wait at most 150 seconds for container health. Recreation is intentional:
atomic replacement of a bind-mounted config file requires remount/restart. Start
only outside a timed measurement; restarting observers during a run changes load.
If startup fails, diagnose with `status` and exact-project logs, then stop:

```bash
python3 "$OBS/stack.py" status
docker compose --project-name kv-comparison-observability \
  --project-directory "$OBS" -f "$OBS/compose.yaml" \
  -f "$OBS/.state/runtime.json" logs --tail 100
python3 "$OBS/stack.py" stop
```

`pull` downloads pinned images without launching services. `stop` removes only
this Compose project's containers/network resources and retains data volumes,
selection and password. It never stops databases or unrelated containers.
`purge --yes` also removes only this project's named Prometheus/Grafana volumes.
It retains the generated secret so normal restart/login remains consistent.
Do not delete the password while retaining Grafana's initialized data volume;
Grafana's admin password setting applies at first initialization. Do not run
`docker system prune`, delete global volumes, or use `--remove-orphans` for cleanup.

## Verify the real query path

After the actual harness run, `stack.py verify` checks more than container or
`/api/health` readiness. It reads the generated secret without printing it, uses
HTTP Basic authentication to read the provisioned Grafana datasource and dashboard,
checks datasource/plugin health, and executes **every sourced dashboard target**
through Grafana's real `POST /api/ds/query` path. Up to eight panel querysets run
concurrently, each with bounded targets, response size, time range and HTTP timeout.
Backend/phase variables are safely expanded to the explicit all-values regex `.*`;
the five-second scrape interval sets the twenty-second rate window. Unknown
variables or unexpected datasources fail rather than silently dropping a query.

```bash
python3 "$OBS/stack.py" verify \
  --output "$RESULTS/observability-query-check.json" --readiness-seconds 45
```

The verifier requires actual Prometheus `kv_observer_state`, comparison/exporter
availability, and `up{job="host"}=1`. For a selected final file, observed producer
status, snapshot timestamp, finality and actual data availability must match that
file. Extra instant queries through Grafana require real state, host-up,
availability and run-status samples, not invented rows. An explicitly cleared
selection or still-uncreated selected leaf can verify `no_run`/`waiting` with no
workload metrics. An existing empty, stale or invalid result directory fails.
A process-level failed final snapshot can pass query-path verification only when
the observed failure status matches: this does **not** turn a failed workload into
a successful qualification.

Readiness polling is bounded (default 45 seconds, configurable 1–120) and applies
only until the selected scrape state is observed. Actual query/plugin failures
are not retried or hidden. Every HTTP request has an eight-second timeout. All
HTTP responses and per-query errors/statuses are checked; successful NoData is
valid for absent optional native/Sui metrics and distributions/rates that lack
observations. A missing explicit per-query status is recorded as null, not
fabricated as a reported status. The actual Grafana observation anchors must have
numeric samples. Any query/plugin failure returns nonzero and appears in the
bounded JSON receipt. The receipt records query identities by a dashboard-query
hash, response statuses and frame/point counts, not passwords or raw metric rows.
The output parent directory must exist. Verification changes no services or
workload metrics and should run outside timed populations.

This is **query-path verification, not visual dashboard proof**. It does not
render browser panels or assert that labels, layout and axis choices are useful.
Integration must still inspect the actual provisioned dashboard in a browser.
The distinction matters: Grafana's HTTP health endpoint can remain healthy while
the datasource plugin cannot create threads or execute dashboard queries.

## Select targets explicitly

The four exact file-discovery JSON files are generated under `.state/targets/`.
There is no network scan or result glob. Prometheus reads file discovery every five
seconds; missing optional targets are absent, not reported healthy.

```bash
python3 "$OBS/stack.py" configure --results "$RESULTS" \
  --native-target http://127.0.0.1:9184/metrics \
  --sui-target http://127.0.0.1:9185/metrics
python3 "$OBS/stack.py" start
```

The optional examples are **not claims that those listeners exist**. Supply the
actual already-running native benchmark/Sui endpoint, or omit it. Each optional
argument can repeat up to 16 times. Only explicit HTTP IPv4 loopback/localhost
endpoints are accepted; no credentials, query strings, fragments, traversal or
external DNS names. `configure` replaces both optional target lists; omitting a
list clears it. Omitting `--results` keeps the current result selection.
`configure --clear-results` intentionally selects no run. The current exporter and
host targets remain explicit fixed loopback entries. Keep the exporter selected
on the current run, not a directory containing multiple runs.

## Honest run freshness and failure

The exporter serves `/metrics` and `/healthz` only (GET/HEAD). Other paths, queries
and request bodies are rejected. It has a bounded single-request server with
socket timeout, no request worker/thread pool, no directory listing, and no file
writes. Scrapes read at most 8 MiB from a regular file through `O_NOFOLLOW` and a
directory descriptor. FIFO/device/symlink files, invalid framing, inconsistent
in-place reads, duplicate/nonfinite contract scalars and future producer clocks
are invalid. The core's atomic rename protocol avoids partial snapshots.

The producer must emit exactly one scalar each:

- `kv_comparison_snapshot_timestamp_seconds`: Unix producer snapshot timestamp.
- `kv_comparison_run_status`: 0 running, 1 completed, 2 failed.

Native registry metrics are passed through unchanged; no conversion or synthetic
performance values occur. Precedence is **final `metrics.prom`**, then `live.prom`.
A malformed final snapshot never falls back to an apparently healthy live one.
Default live freshness threshold is 15 seconds (`configure --stale-seconds 15`,
allowed 2–3600); a live timestamp over five seconds in the future is invalid.

| `kv_observer_state{state=...} 1` | Meaning | Workload metrics |
|---|---|---|
| `no_run` | No directory selected | Absent |
| `waiting` | Selected leaf or both snapshots do not exist yet | Absent |
| `running` | Valid fresh live snapshot, producer status 0 | Original bytes |
| `completed` | Fresh live terminal or valid final status 1 | Original bytes |
| `failed` | Fresh live terminal or valid final status 2 | Original bytes, including failures |
| `stale` | Live producer timestamp exceeded freshness threshold | Absent |
| `invalid` | Unreadable/unsafe file or invalid contract | Absent |

`kv_observer_data_available` distinguishes actual samples from observer-only
health. Snapshot age is emitted only when known. Final snapshots never expire:
`kv_observer_snapshot_final=1` and increasing age mark retained historical data,
not a still-running producer. A crashed producer without a final snapshot becomes
stale; the exporter cannot infer its exit code. Per-operation errors/timeouts may
occur in a process that completes; do not equate process completion with all
operations succeeding. `/healthz` and Prometheus `up=1` mean the exporter/scrape
works, **not** that the selected workload is healthy. No absence becomes zero ops,
zero latency or success.

The exporter is a pass-through for trusted locally produced Prometheus exposition,
not a generic parser/validator for arbitrary hostile metric text. Prometheus also
bounds body size, samples and label lengths and rejects malformed exposition.
Do not select downloaded/untrusted run files without reviewing them.

## Metric and dashboard semantics

The dashboard keeps backend, operation, outcome and phase populations distinct.
No raw key, WAL position, run UUID or unbounded run ID is added as a label.

- `kv_comparison_api_latency_seconds`: dispatch through materialized response.
  `kv_comparison_response_latency_seconds`: scheduled arrival through completion.
  `kv_comparison_scheduling_delay_seconds`: scheduled arrival through dispatch.
  Histogram panels use **current-run cumulative bucket populations**, useful even
  for a completed run shorter than a scrape interval. They are approximate bucket
  quantiles; raw `samples.csv` and `summary.json` are authoritative. No averaged
  percentiles or success-only reinterpretation of a failure-inclusive denominator.
- `kv_comparison_{offered,completed,rejected,timeout,error}_total`: bounded counters.
  `rate()` panels show scraped interval throughput, not configured offered rate
  or exact whole-run achieved throughput. At least two scrapes are needed; a frozen
  final snapshot eventually produces zero interval rate, not zero run throughput.
  Short runs retain final counts/distributions but may have no live rate curve.
- `get_many` and `put_batch` are full-group API calls, not per-key latencies.
  `kv_comparison_keys_completed_total` shows key work separately. Never divide
  complete batch latency by 32 and call it scalar read latency.
- `kv_comparison_current_phase`: qualification=1, preload=2, warmup=3, aging=4,
  measurement=5, drain=6. Warm-up/preload/qualification are not timed measurement.
- Native names come directly from `tidehunter/src/metrics.rs`: `wal_written_bytes`
  is a **gauge**, `wal_written_bytes_type` and `read` are counters; WAL positions
  are gauges, not global distributed durability/transaction frontiers.
  `value_cache_size` counts retained key/full-key/value payload bytes, excluding
  allocator/RSS overhead; `decompress_cache_bytes` is a byte gauge. Native `db_op`
  and `lookup_mcs` histograms use **microseconds**, unlike generic second-based
  harness histograms. They are not combined in the latency panels.
- Optional Sui `tidehunter_adapter{namespace,stat}` is a **mixed gauge family**
  containing event totals, sizes, limits and timing sums. Its panel shows raw
  snapshots only. Never blindly `rate(tidehunter_adapter[...])`. Sui checkpoint
  completion, storage API time and generic KV response time are different scopes.
- Host CPU/memory/disk/network are whole-host observations, not isolated backend
  accounting. Device layers may double-count disk IO. Loopback traffic is visible.
  Cache/WAL submission counters do not prove physical NVMe IO.

Switching runs resets counters at the same bounded label identity. Use a narrow
single-run dashboard time range and retained manifests, not a pooled historical
quantile across different configurations. Do not join runs just because their
backend labels match. Compare results only after checking hardware, workload,
guarantees, cache state, phase and observation mode.

## Isolation, calibration and evidence limits

The services use Linux host networking to reach explicit native loopback metrics,
but every listener is loopback-only. All capabilities are dropped; no privilege
escalation, host PID namespace, Docker socket or host-root mount is provided.
Only node_exporter receives read-only `/proc` and `/sys`, with an explicit small
collector set (CPU, memory, diskstats, netdev, stat, loadavg, uname, vmstat and
pressure). Swap-in/out counters and pressure expose local memory interference;
they are not isolated per-backend accounting. Filesystem usage metrics are
intentionally excluded rather than mounting `/`. Prometheus has
7-day/2-GB retention, and every service has a memory/process/log bound. Grafana's
password is random, stored only under ignored mode-0700 `.state/`; its separately
mounted read-only secret file is readable by Grafana's non-root UID. No anonymous
Grafana access or signup is enabled. Local users who can reach loopback can still
read unauthenticated Prometheus/exporter metrics: this is not a multi-user secure
metrics deployment.

This single-run local deployment is **not an off-host research collector**.
Campaign mode above deliberately cohosts a capped collector on the controller;
its receipts and dashboard disclose that placement. A dedicated non-measured
observer remains preferable for final precision. Neither mode establishes
overhead neutrality without calibration.

Before interpreting performance, run matched **observer-off / observer-on**
calibration using the same source/config/seed, physical cache policy and workload,
with repeated alternating order. Off means stop this stack and omit optional
native scrapes; on means the full observer and browser polling run. Record both
modes in run metadata; do not quietly subtract collector overhead or claim it is
zero. The harness's once-per-second/phase snapshots still cost work when Docker is
off; a separate source-qualified no-snapshot mode would be a distinct experiment,
not assumed here. Do not stop/start Docker during a timed population. Prometheus
scrapes, Python reads, Grafana rendering, disk retention and host collectors share
CPU/cache/SSD/network with the measured code. One local SSD and loopback traffic
cannot establish independent-NVMe scaling, network/EFA latency, HA or native-speed
parity. Live dashboards are coarse snapshots, not correctness or linearizability
proofs. Read the run's raw samples, manifest and backend guarantee metadata.

## Pins, sources and semantic checks

`images.lock.json` records the immutable registry index digests also used directly
by Compose. Versions selected from primary publisher release data on 24 October
2026: Prometheus **3.13.5** (supported 3.13 LTS), Grafana OSS **13.2.4**,
node_exporter **1.12.1**, Python **3.13.17-slim-bookworm**. No `latest` deployment
reference or pip dependency is used. Runtime pull receipts preserve local image
IDs and resolved repository digests. Prometheus/node_exporter are Apache-2.0,
Grafana OSS is AGPL-3.0, and Python uses the PSF license; retain publisher license
and image provenance when redistributing images or modifying Grafana.

Primary references:

- [Prometheus supported release cycle](https://prometheus.io/docs/introduction/release-cycle/)
  and [3.13.5 release](https://github.com/prometheus/prometheus/releases/tag/v3.13.5).
- [Publisher node_exporter releases/download metadata](https://prometheus.io/download/)
  and [host/container collector deployment](https://github.com/prometheus/node_exporter#docker).
- [Grafana 13.2.4 release](https://github.com/grafana/grafana/releases/tag/v13.2.4),
  [supported upgrade policy](https://grafana.com/docs/grafana/latest/upgrade-guide/when-to-upgrade/),
  [provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).
- [Official Python image manifest source](https://github.com/docker-library/official-images/blob/master/library/python).
- [Prometheus histograms and quantile aggregation](https://prometheus.io/docs/practices/histograms/)
  and [file discovery semantics](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config).
- [Tidehunter paper](../../../../papers/parsed/Tidehunter-2026-v2.md), §§3,6:
  native read tiers, asynchronous WAL persistence, and matched workload methodology.
  [Fork invariants and local evidence boundary](../../../docs/distributed/ONE_SIDED_WAL_IMPLEMENTATION_2026-10-23.md).
  No WAL/cache mechanism is changed by this stack.
- [Grafana datasource health and query HTTP APIs](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/data_source/).

The integration owner should run the focused file/target semantic regressions:

```bash
python3 -m unittest discover -s experiments/kv-comparison/observability -p 'test_*.py'
promtool test rules experiments/kv-comparison/observability/rate-regressions.yaml
```

These checks cover stale/final/failure precedence, absent future result leaves,
exact byte preservation, symlink/FIFO/size boundaries, and explicit safe targets.
They do not pretend to prove container wiring. The current live/export/restore
qualification and its limits are recorded above. Requalify target health,
authenticated dashboard queries, failure states and cleanup when changing the
collector or image pins.
