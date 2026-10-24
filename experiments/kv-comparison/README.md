# Local distributed-KV comparison

This is a **local-only research harness**, not an AWS deployment. The application
boundary is the `kv-research` Rust binary and its real native/TCP, RocksDB/BlobDB,
TiKV RawKV, Cassandra CQL and FoundationDB adapters. The common contract and native
WAL restrictions are in [the comparison protocol](../../docs/distributed/KV_COMPARISON_PROTOCOL.md).
Run from the repository root. Python helpers use only the standard library.
Linux Docker Engine with Compose v2 is required; host networking and distinct
loopback addresses are deliberate. Docker Desktop is not supported by this layout.
No credential, OIDC role, cloud account, subscription or paid workflow is used.

## Scope and evidence limits

* `distributed` is the default: multiple actual database processes, explicitly
  recorded replication and ownership, **one physical machine and shared SSD**.
  Logical process zones/racks are not independent failure domains. This does not
  prove cloud latency, independent-NVMe scaling, HA or network-partition behavior.
* `qualification` is an explicitly named smaller API-functional layout: one PD +
  one TiKV (RF1), one Cassandra (RF1), or one FDB process (`single`). The PR workflow
  uses this layout and `qualification_only=true`; no performance claim is valid.
* Native Tidehunter default append acknowledgement is not replicated/disk-fsynced
  commit. Current distributed Tidehunter retains bounded history without physical
  WAL reclamation. Aging is a **finite operation count**, never a claim of sustained
  steady state. Do not merge durability/consistency, native cache-hit, network API,
  physical-I/O or differently instrumented populations.
* Common operations replace complete opaque values; this is not a stock YCSB
  ten-field partial-update test. Read-many latency is a complete ordered group,
  not latency per key. Unsupported atomic batches are errors, not emulated atomicity.

## Pinned releases and resource budgets

`local.py` pins immutable registry index digests in addition to readable release
versions. Startup retains platform-specific local image IDs and RepoDigests in
`images.json`; no floating `latest` tags are used. Release selection consulted
[TiKV 8.5.4 LTS release notes](https://docs.pingcap.com/tidb/stable/release-8.5.4/),
[Apache Cassandra latest GA 5.0.9](https://cassandra.apache.org/_/download), and
[FoundationDB releases](https://github.com/apple/foundationdb/releases): 7.3.77 is
non-prerelease; 8.0.0/7.4.x were marked prerelease when selected. Upgrade pins and
clients together, then rerun qualification; never silently substitute another image.

| Role | Distributed count | Hard per-process/container CPU / RAM cap | Effective policy |
|---|---:|---|---|
| TiKV PD controller | 1 | 1 CPU / 512 MiB | single PD controller, no PD HA claim |
| TiKV storage | 3 | 2 CPU / 1536 MiB | RF3, distinct logical `zone` labels; Titan on; 256 MiB shared block cache per store |
| Cassandra storage+coordinator | 3 | 2 CPU / 1536 MiB | SimpleStrategy RF3, durable_writes=true, dc1, distinct logical racks; 512 MiB JVM heap, 256 MiB direct-memory cap and two GC/application processors |
| FDB combined roles | 3 | 1 CPU / 768 MiB | double redundancy (RF2), three coordinators/zones; SSD engine; 512 MiB server memory and 128 MiB cache; actual recruited roles retained |
| Rust workload/client (including native/TCP coordinator and owner descendants) | 1 owned process group | 2-CPU inherited affinity / 2048 MiB aggregate sampled RSS budget | independent wall deadline; per-process RSS/CPU sampled every 250 ms, **not** a hard memory cgroup |
| Monitoring | optional separate stack | see observability helper/config | not silently enabled or charged to backend budgets |

The primary `tidehunter_tcp` cell has one client/coordinator plus **five actual
owner processes** (six total), not five independent full database indexes. They
inherit one shared two-CPU affinity mask and the aggregate sampled 2048 MiB RSS
budget; there is no per-owner hard memory cgroup in this local TCP fixture.
RF1 Value-WAL persistence, authenticated TLS and bounded retained history remain
different contracts from plaintext replicated comparators. Raw metadata retains
those distinctions; the report must not imply equivalent durability or security.

TiKV total hard server caps: 7 CPU / 5 GiB; Cassandra: 6 CPU / 4.5 GiB;
FDB: 3 CPU / 2.25 GiB. Add client and optional monitoring, the Docker daemon,
OS/page cache and image storage; CPU quotas are maxima, not dedicated reservations.
External qualification uses one data node with the same per-role caps; Tidehunter/TCP
still starts its five owned processes. Only one backend stack runs at a time.
Tiny API data are appropriate for hosted CI, not saturation.
The larger default working set is 65,536 keys, not a claim that it exceeds RAM.
For a genuine memory-pressure profile explicitly increase keys within all admitted
byte/disk bounds and report observed cache/RSS/I/O rather than label it cold by size.
At least 4 GiB free disk is required to start; Docker storage is shared and image
layers remain cached. No global Docker prune is performed. Results retain compact
inputs/seeds/source snapshots and raw metrics, not database-volume copies.
After the supervised process group exits, native/TCP/embedded cells remove only
their generated `run_dir/database` tree, retaining TCP process evidence and all
raw result/provenance artifacts. Deletion failure is a failed cleanup receipt,
not successful completion; no unrelated directory is removed.

## Prepare and build explicitly

```sh
python3 experiments/kv-comparison/local.py prepare-fdb --prefix "$HOME/.local/kv-fdb-7.3.77"
export FDB_CLIENT_LIB_DIR="$HOME/.local/kv-fdb-7.3.77/lib"
export FDB_CLIENT_LIB_PATH="$FDB_CLIENT_LIB_DIR"
export LD_LIBRARY_PATH="$FDB_CLIENT_LIB_DIR:${LD_LIBRARY_PATH:-}"
export LIBRARY_PATH="$FDB_CLIENT_LIB_DIR:${LIBRARY_PATH:-}"
cargo build --locked --release -p kv-research --features tikv,cassandra,foundationdb,rocksdb
python3 -m unittest discover -s experiments/kv-comparison -p 'test_*.py'
```

FDB's Rust binding selects API 730 and links the actual matching 7.3.77
`libfdb_c.so`. The helper copies it from the **digest-pinned** official image into
an explicit user-owned prefix, retaining image/version/API/canonical library path
and SHA256/Blake2b-512 in `client.json`. It does not install packages, change `/usr`, run a server, or use
sudo. An existing library is never overwritten; a requested FDB campaign without
that explicit prefix fails clearly. Native build dependencies must be prepared
explicitly: on Ubuntu, `sudo apt-get install build-essential pkg-config clang
libclang-dev cmake protobuf-compiler libssl-dev libfontconfig1-dev ca-certificates
openssl bzip2`. Protobuf/TiKV code generation needs `protoc`; FoundationDB bindings
need libclang; RocksDB additionally builds its native C++ source. CI installs and
retains the package/protoc/clang versions explicitly. Linking or
server-version/configuration failure is not an acceptable skip. Apache Cassandra,
TiKV and FDB are Apache-2.0 servers; relevant Rust dependencies are MIT/Apache-2.0.
No Scylla server image/free-tier license is substituted for Cassandra.

For FDB admission and the supervised runner, the controller replaces
`LD_LIBRARY_PATH` with **only** the admitted library directory and removes
`LD_PRELOAD`/`LD_AUDIT`; it never appends ambient loader search paths. It checks the
actual mapped native library path and Blake2b-512 reported by the adapter against
admitted client metadata before accepting success, and rechecks the file bytes.
Existing matching SHA-admitted client receipts are enriched in campaign artifacts
with their canonical path/Blake2b-512 without overwriting the installed library.

## Start, observe readiness, reuse, export, clean

```sh
python3 experiments/kv-comparison/local.py start --backend tikv --layout distributed --state /tmp/kv-tikv
python3 experiments/kv-comparison/local.py ready --state /tmp/kv-tikv --timeout 180
# Repeating start with the exact same backend/layout/state reuses ONLY a ready owned stack.
python3 experiments/kv-comparison/local.py export --state /tmp/kv-tikv --output /tmp/kv-tikv-proof
python3 experiments/kv-comparison/local.py stop --state /tmp/kv-tikv
```

Replace `tikv` with `cassandra` or `foundationdb` for individual server reruns.
Use fresh state paths for fresh deployments; an existing stopped state is not
silently repurposed. `stop` removes only that receipt's Compose project containers
and volumes after checking their owner label. It never kills processes by port,
removes arbitrary names, or prunes unowned data. Failure leaves diagnostics and
owned state available for explicit `export`/`stop`; the campaign always cleans in
`finally`. Export or stop failures mark the controller receipt failed, retain every
cleanup reason, and propagate failure even if the Rust workload completed.
Cleanup still attempts exact-owned stop when export fails. Ctrl-C is also a
failure, not a successful shortened benchmark.

Readiness polls actual protocol/administrative observations until a bounded
startup deadline; no blind startup sleeps are evidence:

* TiKV: PD `/pd/api/v1/stores`, `/regions`, `/config/replicate`, `/version`;
  every requested store Up, every region has requested peers and a leader with no
  down/pending peers, each store's HTTP `/config` retained. Ownership/region count
  is whatever PD actually reports, not an invented uniform three-way split.
  RawKV client `pd_endpoints` uses `127.0.0.1:2379`; the separate administrative
  `pd_api_endpoints` explicitly uses `http://127.0.0.1:2379`. Bare host:port is not
  an HTTP URL. Stores use 20160–20162 and status 20180–20182.
* Cassandra: sequential bootstrap, successful CQL `system.local` query on each
  address, then each node's `nodetool status` must report the full exact UN set.
  CQL version/DC/rack/host ID/tokens are retained. Addresses are
  `127.0.0.11`, `.12`, `.13`, all port 9042. The owned `kv_comparison` keyspace
  uses actual SimpleStrategy RF1/RF3 and durable_writes=true; adapter validation
  checks effective keyspace settings and records actual consistency. Token/range
  ownership is in nodetool output; equal token counts do not imply equal bytes.
  Local-only JMX ports 7199–7201 are set through the final
  `-Dcassandra.jmx.local.port` property: the pinned image's `cassandra-env.sh`
  resets `JMX_PORT`, so setting only that environment variable is insufficient.
* FDB: explicit `configure new single|double ssd ...` via matching `fdbcli`, then
  `status json` must show database availability, requested process count,
  redundancy, version and distinct zone localities. Full observed roles,
  configuration/regions and storage statistics remain in deployment proof.
  Ports 4500–4502; the generated cluster file is the actual client input.

These queries plus Rust's pre/post sequential overwrite/read/delete/missing,
full-payload and ordered-group checks are the actual-server validation path.
A running container or printed config alone is insufficient. Unknown/unsupported
configuration and unavailable requested endpoints are errors, never green skips.
The default loopback ports must be free; no unowned deployment is stopped for you.

## Campaigns, repeat identity and raw exports

```sh
# Default: 4 backends × 3 profiles × 3 offered rates × 5 repeats = 180 finite cells.
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/campaign.json \
  --binary target/release/kv-research --output /tmp/kv-campaign-new
# Exactly one backend, retaining the same declared seed/profile/rate/repeat definitions:
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/campaign.json \
  --binary target/release/kv-research --backend cassandra --output /tmp/kv-cassandra-rerun-new
# Cheap actual adapter qualification (same command used by CI, not timing evidence):
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/ci.json \
  --binary target/debug/kv-research --qualification --backend foundationdb --output /tmp/kv-fdb-api-new
# Emergency/repeated exact-owned cleanup from an explicit campaign result root:
python3 experiments/kv-comparison/cleanup.py --root /tmp/kv-campaign-new
```

The default primary matrix includes `tidehunter_tcp`, TiKV, Cassandra and
FoundationDB. Optional embedded controls stay in a **separate** copied manifest
and output directory: set `backends` to `["native","rocksdb","blobdb"]` and
`max_runs` to 135 for the same three-profile/three-rate/five-repeat design. Their
one-process API boundaries and acknowledgement policies are an embedded control
tier, never a pretend distributed deployment.

For TCP cells the controller calculates a finite per-partition retained-history
cap as preload keys plus all configured phase calls plus 1024 qualification
allowance, conservatively allowing all groups to concentrate on one owner.
Requirements above the supported 1,000,000 cap are rejected, never clamped.
The calculated cap and `physical_wal_gc=false` remain in effective metadata and
the controller receipt. This is not a preallocation or a promise of unbounded
steady state: independent metadata bounds can reject admission, and physical WAL
bytes are not reclaimed.

To reuse a separately started exact-owned stack, add `--reuse-state /tmp/kv-tikv`
and `--backend tikv`, with the manifest's same `distributed` layout. Every cell
still has a new isolated key namespace, but receipts explicitly label underlying
storage **reused/nonfresh**, regardless of the profile's logical aging label.
Reused data/background state is not pooled with fresh deployments. The harness
exports/rechecks the existing stack and deliberately leaves it running, including
on workload failure; terminate it with `local.py stop --state /tmp/kv-tikv`.
CI does not use reuse mode.

The campaign manifest is admitted before expansion: explicit supported backends,
unique profiles/rates, finite positive/reasonable integer bounds, repeats and
`max_runs`. `order.json` is the complete seed-randomized order; a backend rerun
filters that order, not an independently picked convenient order. Each cell uses
a fresh database, generated namespace, effective config and dataset seed; fresh
and aged identities cannot accidentally share data. All cells are sequential.
`base_config` reuses the common Rust example schema. Edit it/a copied base config
for 256 B/1 KiB/8 KiB values, mix, entropy, distribution, operation counts and
admission limits; workload and byte/deadline limits must remain matched. For more
value/mix/distribution cells use separately named manifests, not implicit mutation.
Aging is the declared finite overwrite/delete phase, not a duration-based
unbounded warm-up. Open-loop offered rate is independent of completion; report
rejections, unknown outcomes, timeouts and achieved throughput, not just successes.

`--qualification` explicitly disables preload/warm-up/aging/measurement, uses a
90-second run deadline, 10-second shutdown grace and 5-second request timeouts,
while retaining the real deterministic API qualification and final checks. Its
effective JSON and logs are retained; zero timed workload samples are expected
and deliberately rejected by performance analysis.

The supervising Python process starts a new owned process group, imposes a wall
limit independent of Tokio/native blocking calls, preserves logs/live output,
and terminates/reaps the exact child group on deadline/failure. Termination does
not roll back mutations. Server containers have separate hard cgroups. The receipt
records client/hardware/OS/affinity/block devices, source revision, binary/config
hashes (binary SHA256 **and** Blake2b-512), exact timestamps and failures.
Before any DB starts, Rust's JSON `--validate-config` output is retained in
`admission.log` and its actual serde `admitted_config` in `normalized-config.json`.
The supervised runner consumes that normalized file; the controller requires
Rust `manifest.run.config` to match it and `manifest.run.binary.blake2b512` to match
the admitted executable. The original requested `config.json` is also retained.
After runner exit, `receipt.artifacts_sha256` binds the exact final
`manifest.json`, `summary.json`, `samples.csv` and `metrics.prom` bytes before
success is accepted.
`source.zip` contains tracked source bytes
and new source/configuration under the explicitly enumerated framework paths,
including observability, with per-file SHA256. Ignored untracked files, `.state`, runtime
artifacts, secrets/credentials directories, symlinks and unrelated user-untracked
files are excluded. Source revision alone is not a dirty-tree identity. Server
export includes image IDs, actual hard cgroup limits, versions, topology and bounded logs.
Actual server command/entrypoint/environment are bound by a normalized SHA256
without exporting environment values; only generated hostname/cluster identities
are masked. Cache/heap changes therefore split cohorts. Mounted TiKV/PD config
bytes are also part of the admission boundary. Rust retains `manifest.json`,
`summary.json`, all bounded `samples.csv` rows, `live.prom` and `metrics.prom`.
Keep **entire** campaign directories, not screenshots or isolated summary numbers.
Resource caps are not cloud pricing, and no local estimate is cloud billing.

## Strict offline analysis

```sh
python3 experiments/kv-comparison/analyze.py /tmp/kv-campaign-new/0000-BACKEND-PROFILE-rRATE-n0 \
  /tmp/kv-campaign-new/OTHER_MATCHED_REPEAT --output /tmp/kv-pooled.json
```

Pass **only matched completed performance repeats**. Qualification/failed runs,
zero measurement samples, missing effective BackendInfo and different workload,
seed, phases, limits, versions, binary, hardware, budgets, acknowledgement,
consistency, RF, measurement/group/atomicity strategy or effective configuration
are rejected. The analyzer is intentionally conservative: analyze each backend
and contract separately; unlike-contract backend tables may be shown side by side
with their guarantees, but never pooled into a speedup or common percentile.
Dynamic effective configuration changes can require separate reports rather than
stripping inconvenient fields. Quantiles are nearest-rank over pooled raw samples,
**never an average of per-run p99**. Every operation/outcome remains separate;
rejected/timeout/unknown outcomes remain in the offered-call denominator. Missing
latencies are unavailable, not zero. Conditional successful latency alone is not
an overall response-tail claim; evaluate drop/timeout fraction together with tail.
No shared local timing proves linearizability, crash recovery or replicated HA.

### Research tables and uncertainty

Keep failures and qualification-only runs in the coverage report:

```sh
python3 experiments/kv-comparison/research_report.py /ABS/CAMPAIGN_A /ABS/CAMPAIGN_B \
  --output /tmp/kv-research-report-new
```

This writes `comparison.json`, `comparison.csv` and `comparison.md`. It groups
only identical contracts, retains failed/invalid cells, separates embedded
references from distributed application APIs and refuses copied run identities.
Different durability/RF/consistency/resource budgets remain separate rows, not
automatic speedups. Raw per-operation/outcome samples supply pooled quantiles.
Run-level bootstrap intervals require at least five independent repetitions;
conditional tails also require enough tail observations in **each** run.
A missing interval does not mean zero uncertainty.

For a **predeclared** open-loop response-p99 objective, add, for example,
`--p99-slo-us 1000 --minimum-success-fraction 0.999 --slo-operation read`.
The JSON retains every observed offered-rate point and rejects overloaded,
slow or undersampled points from the observed admissible maximum. It never
claims that a finite local rate sweep proves sustainable capacity, future-tail
bounds, HA, cold-device latency or competitiveness on an independent-NVMe cluster.
No objective is silently chosen when these options are absent.
`--allow-incomplete` permits exporting failure coverage with exit success; it
does not turn those cells into performance results.

## Monitoring interface

Monitoring remains independent; databases never start with monitoring implicitly.
The explicit integration is:

```sh
python3 experiments/kv-comparison/observability/stack.py configure --results /ABS/CURRENT/result
python3 experiments/kv-comparison/observability/stack.py start
# Or campaign.py ... --observe explicitly configures AND refreshes/starts monitoring before each cell.
python3 experiments/kv-comparison/observability/stack.py status
python3 experiments/kv-comparison/observability/stack.py stop
```

`--observe` refreshes the exact-owned observer mount before workload timing and
leaves monitoring running until explicit `stack.py stop`. Requested mode and the
actual known observer project's presence, image/resource identity and health are
captured before and after every run. Omitting `--observe` **does not prove that
monitoring is off**: an externally started known project is recorded as present;
Docker/inventory failure is unknown, and absence only attests that exact project,
not arbitrary host observers. The harness never stops a user's observer implicitly.
Different observation contracts are not pooled. Aggregate process RSS can
double-count shared resident pages; it is a conservative sampled admission bound,
not independently attributable physical RAM or device I/O.

Prometheus 127.0.0.1:9090, Grafana 127.0.0.1:3000, explicit-current exporter
127.0.0.1:9899 and node exporter 127.0.0.1:9100 are loopback-only. See
[observability instructions](observability/README.md) for credential/provisioning,
freshness and exact-owned cleanup. Dashboards are observation aids; raw sample
and source/config/deployment artifacts remain the evidence.

## CI boundary and primary design anchors

`.github/workflows/kv-comparison.yaml` is a cheap credential-free PR/manual job:
pinned existing action SHAs, Cargo dependency cache, explicit native dependencies,
`cargo test --locked -p kv-research --features tikv,cassandra,foundationdb`, fixed
job/step deadlines, sequential real native/Tidehunter-TCP/TiKV/Cassandra/FDB qualification under
`--observe`, and actual Prometheus/Grafana datasource plus concurrent provisioned
dashboard query verification after each backend (`stack.py verify --output JSON`).
Unconditional cleanup independently attempts both exact-owned database cleanup
and observer stop, preserving failure if either fails; artifacts survive failure.
Only result/provenance/query receipts are uploaded, never observer `.state` or
its generated secret. It has no push/benchmark-branch/paid trigger and
changes no existing AWS workflow. Manual dispatch selects an individual backend.
A failed requested dependency, server, readiness or adapter exits nonzero. A tiny
single-process test proves only the functions actually exercised, not distributed
latency, replication resilience or physical ownership scaling.

Design anchors: Tidehunter paper §§3/6 (`../papers/parsed/Tidehunter-2026-v2.md`
relative to the containing project) separates Value-WAL/index publication,
materialized-value reads and fill/measurement phases; native source remains
unchanged. [TiKV config template](https://github.com/tikv/tikv/blob/v8.5.4/etc/config-template.toml)
and [Titan](https://docs.pingcap.com/tidb/stable/titan-overview/) anchor server knobs;
[Cassandra architecture](https://cassandra.apache.org/doc/stable/cassandra/architecture/storage-engine.html)
and [official container entrypoint](https://github.com/docker-library/cassandra/blob/master/5.0/docker-entrypoint.sh)
anchor startup; [FDB container source](https://github.com/apple/foundationdb/blob/7.3.77/packaging/docker/Dockerfile)
and [architecture](https://apple.github.io/foundationdb/architecture.html) anchor
client library paths and coordinated roles. [YCSB](https://doi.org/10.1145/1807128.1807152)
is a workload vocabulary, not an excuse to compare unlike adapters or assume its
pacing corrects coordinated omission. The harness does not copy go-ycsb's older
client dependencies or FoundationDB read-merge-write semantics.
