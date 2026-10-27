# Distributed-KV comparison

`local.py` and `campaign.py` remain a **local-only research harness**. The separate
`cloud_layout.py` interface below prepares external servers on admitted physical
hosts; it does not launch AWS resources or orchestrate a network deployment.
The application boundary is the `kv-research` Rust binary and its real native/TCP, RocksDB/BlobDB,
TiKV RawKV, Cassandra CQL, FoundationDB and Valkey Cluster TCP adapters. The common contract and native
WAL restrictions are in [the comparison protocol](../../docs/distributed/KV_COMPARISON_PROTOCOL.md).
Transport selection is explicit and provider-neutral. Tidehunter runtime kind
is `tidehunter`; `Transport` defaults to TCP or accepts structured OFI
configuration. Historical case names remain report identities, not runtime
aliases. See [adapter transport mapping](../../benchmarks/kv-research/ADAPTERS.md#tidehunter-transport-adapters-and-external-owner-preparation)
and the [comparison protocol](../../docs/distributed/KV_COMPARISON_PROTOCOL.md#transport-configuration-and-evidence)
for canonical JSON, per-host selectors, security matching, and evidence rules.
Do not interpret a software-provider FI_READ control as hardware proof or claim
EFA/verbs qualification without actual provider metadata and measured hardware.

Run from the repository root. Python helpers use only the standard library.
Linux Docker Engine with Compose v2 is required; host networking and distinct
loopback addresses are deliberate. Docker Desktop is not supported by this layout.
The local helpers require no credential, OIDC role, cloud account, subscription or paid workflow.

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
non-prerelease; 8.0.0/7.4.x were marked prerelease when selected, and
[Valkey 9.0.7](https://github.com/valkey-io/valkey/releases/tag/9.0.7) is the pinned
security release for the TCP integration. Upgrade pins and clients together, then
rerun qualification; never silently substitute another image.

| Role | Distributed count | Hard per-process/container CPU / RAM cap | Effective policy |
|---|---:|---|---|
| TiKV PD controller | 1 | 1 CPU / 512 MiB | single PD controller, no PD HA claim |
| TiKV storage | 3 | 2 CPU / 1536 MiB | RF3, distinct logical `zone` labels; Titan on; 256 MiB shared block cache per store |
| Cassandra storage+coordinator | 3 | 2 CPU / 1536 MiB | SimpleStrategy RF3, durable_writes=true, dc1, distinct logical racks; 512 MiB JVM heap, 256 MiB direct-memory cap and two GC/application processors |
| FDB combined roles | 3 | 1 CPU / 768 MiB | double redundancy (RF2), three coordinators/zones; SSD engine; 512 MiB server memory and 128 MiB cache; actual recruited roles retained |
| Valkey Cluster TCP primaries | 5 | 2 CPU / 1536 MiB | RF1, no replicas; 1024 MiB maxmemory per primary, noeviction; AOF appendfsync=always, no-appendfsync-on-rewrite=no; RAM-resident GET |
| Rust workload/client (including native/TCP coordinator and owner descendants) | 1 owned process group | 2-CPU inherited affinity / 2048 MiB aggregate sampled RSS budget | independent wall deadline; per-process RSS/CPU sampled every 250 ms, **not** a hard memory cgroup |
| Monitoring | campaign collector cohosted on controller; separate local mode optional | see [monitoring guide](observability/README.md) for hard caps | explicit observer placement; not off-host or proven overhead-neutral |

The primary `tidehunter_tcp` cell has one client/coordinator plus **five actual
owner processes** (six total), not five independent full database indexes. They
inherit one shared two-CPU affinity mask and the aggregate sampled 2048 MiB RSS
budget; there is no per-owner hard memory cgroup in this local TCP fixture.
RF1 Value-WAL persistence, authenticated TLS and bounded retained history remain
different contracts from plaintext replicated comparators. Raw metadata retains
those distinctions; the report must not imply equivalent durability or security.

An explicitly selected local software-OFI profile can need more aggregate RAM:
RxM allocates provider buffers per endpoint, outside the application slot
limits. The task-owned six-process transport qualification used 4,096 MiB,
sampled 2,281,836,544 aggregate RSS bytes and observed no swap-in/out increments.
This is not a larger default CI budget, a cloud per-host budget change, or a
software/hardware performance result. See the
[pinned source and local evidence](../../tidehunter-fabric/DESIGN.md#local-qualification-27-october-2026).

TiKV total hard server caps: 7 CPU / 5 GiB; Cassandra: 6 CPU / 4.5 GiB;
FDB: 3 CPU / 2.25 GiB; Valkey: 10 CPU / 7.5 GiB.
Add client and optional monitoring, the Docker daemon,
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
cargo build --locked --release -p kv-research --features tikv,cassandra,foundationdb,rocksdb,valkey-tcp
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

## Five-host external deployment contract

`cloud_layout.py` is a standard-library, **node-local** executable suitable for
SSM. It contains no AWS API access, SSH, remote Docker or network orchestration.
The caller admits/stages the fleet, starts nodes in order, collects receipts and
sets the overall experiment deadline. Requirements on every node are Linux Docker
Engine host networking, Python 3 with adjacent `local.py`, and util-linux
`findmnt`/`lsblk`. Administrative clients come from the same digest-pinned images
as the servers, not packages downloaded at runtime.

The fleet JSON uses `schema_version: 1`, a safe `run_id`, `region`,
`data_root: "/mnt/tidehunter-kv-comparison/<run_id>"`, `replication_factor: 1`,
`coordinator: {"instance_id": "...", "address": "10.x.x.x"}`, and exactly five
ordered `nodes: [{"index": 0, "instance_id": "...", "address": "10.x.x.x"}, ...]`.
All six addresses and machine IDs must be distinct; physical addresses must be
RFC1918 IPv4. Declare `server_cpus`, `server_memory_mib`, `client_cpus` and
`client_memory_mib` (minimum 2 CPU/2048 MiB storage, 2 CPU/4096 MiB coordinator).
No credentials belong in this file. Each storage server receives the identical
declared hard Docker CPU/memory cap, including Cassandra's embedded coordination.
PD reserves 1 CPU/1024 MiB of the coordinator budget; FDB control reserves
1 CPU/2048 MiB. Plans explicitly subtract these from the remaining workload
allowance, rather than hiding controller cost. Cassandra coordinator CQL tools
are short-lived, 1 CPU/512 MiB and must not overlap a measured workload.
These external client/server connections are plaintext and unauthenticated;
admit only an isolated private fleet. RF1 provides no failover/HA, and the
different transaction and acknowledgement semantics must remain labeled.

Pure APIs are `build_plan(fleet, backend)`, `node_config(fleet, backend, index)`
and `backend_options(fleet, backend, root)`. `index=-1` is coordinator, `0..4` are
storage hosts; `root` is the **coordinator backend directory** containing
`fdb.cluster`, not the fleet data root. Plans contain exact Docker argv, generated
config file bytes, data binds, labels, budgets and Rust adapter options. TiKV
options distinguish RawKV PD endpoints from HTTP PD administrative URLs;
Cassandra supplies all five CQL endpoints; FDB names its actual cluster file.
Each node also includes fully specified lifecycle `actions` argv using inline
`--fleet-json` (an alternative to `--fleet FILE`), the emitting helper's Python
and script paths, and bounded probe settings. Generate those actions from the
staged helper path on the target host. `bootstrap-probe` deliberately omits the
full-ring flag; normal `probe` actions initialize on coordinator and require
cluster coverage on storage.
Startup resolves the exact digest locally first and pulls only if that immutable
image is absent; a cached, digest-admitted image does not require a registry
round trip on every restart.

Execute these commands on the corresponding host, never treating a printed plan
as readiness:

```sh
python3 experiments/kv-comparison/cloud_layout.py plan --fleet /run/fleet.json --backend tikv
# Every host, using its own --node-index (-1 for coordinator):
python3 experiments/kv-comparison/cloud_layout.py prepare --fleet /run/fleet.json --backend tikv --node-index 0
python3 experiments/kv-comparison/cloud_layout.py start --fleet /run/fleet.json --backend tikv --node-index 0
# After coordinator and all five stores have started, on coordinator:
python3 experiments/kv-comparison/cloud_layout.py probe --fleet /run/fleet.json --backend tikv --node-index -1 --initialize --timeout 300
# Every host, retaining node-local protocol, image, process/cgroup and disk proof:
python3 experiments/kv-comparison/cloud_layout.py probe --fleet /run/fleet.json --backend tikv --node-index 0 --cluster --timeout 120
python3 experiments/kv-comparison/cloud_layout.py export --fleet /run/fleet.json --backend tikv --node-index 0
python3 experiments/kv-comparison/cloud_layout.py stop --fleet /run/fleet.json --backend tikv --node-index 0
```

Preparation requires the address actually bind on that host and `findmnt` plus
`lsblk` identify a writable **instance-store NVMe**, not EBS merely named NVMe.
The admitted physical form is a direct NVMe filesystem, not a root/overlay or
unexamined RAID mount. Receipts retain host boot ID/hostname plus device
serial/model; the fleet controller must compare **distinct boot IDs across all
six hosts** and distinct storage-device serials before claiming independent
physical placement. Declared instance IDs alone are not that proof.
Every persistent server data/commitlog/TLog directory is an explicit bind below
`data_root/<backend>/coordinator|node-N/data`. Symlink/noncanonical paths and
nonempty unowned directories are rejected. Generated run/fleet ownership receipts
must match exactly. Stop checks the complete owner/run/backend/node label set
and exact container name before removing only those containers; it never prunes
Docker, removes foreign containers/volumes, or deletes retained data. Export is
`<node-directory>/export/evidence.json`; gather it **before** stopping. Stop
retains database data. Fresh backend blocks require the separate explicit
`reset-data` action on every host **after all six hosts have stopped**:

```sh
python3 experiments/kv-comparison/cloud_layout.py reset-data --fleet /run/fleet.json --backend tikv --node-index 0
```

Reset refuses even stopped owner-labeled containers, data borrowed by any other
local container, symlink roots/markers, and nested mountpoints. It empties only
the exact admitted `data/` bind, never follows contained symlinks, invalidates
`initialization.json` before mutation, and retains configuration and exports.
The bounded maintenance tool uses the digest-pinned Cassandra image's `find`,
network disabled, 1 CPU/256 MiB, and only that data directory writable; database
files owned by container root/Cassandra need no host `sudo` workaround.
`reset.json` is `resetting` until actual emptiness is checked, then `reset`;
a partial deletion is not a fresh-deployment receipt. The caller must enforce
fleet-wide stop because this node-local helper cannot inspect other machines.
Restart and initialize again, then preload the same declared physical keyspace;
do not accumulate unrelated namespaces and label reused physical data fresh.

Readiness has bounded command/HTTP calls and a bounded polling deadline (1–900s).
Success is JSON `status: "ready"` with `proof.scope` (`node` versus `cluster`);
deadline or admission errors return nonzero with JSON `status: "failed"`.
Initialization and partial split/scatter attempts are preserved separately:

* **TiKV 8.5.4:** one PD on coordinator, five private-address stores, RF1 with
  one whole-keyspace `pd/default` RF1 voter placement rule, validated from actual
  `/config/rules`. Placement rules **must be enabled**: pinned PD
  [scatter `allowLeader`](https://github.com/tikv/pd/blob/v8.5.4/pkg/schedule/scatter/region_scatterer.go)
  needs a real rule fit, even when the ordinary RF scheduler can operate without
  rules. `/config/replicate` encodes these booleans as JSON strings.
  Coordinator `probe --initialize` submits only missing members of 19 evenly
  spaced hexadecimal RawKV split boundaries, then calls `/regions/scatter`.
  Admission uses observed boundaries/ownership, not a response percentage that
  becomes zero on an already completed retry. Region merging is disabled.
  Final admission requires at least 20 healthy single-peer regions, every exact
  store Up, and all five hosts represented among actual leaders/peers.
  The actual store config must show `raft-kv`, API1, explicit Raft Engine/data
  paths, `store-io-pool-size=1`, and `max-apply-unpersisted-log-limit=0`.
  There is **no `raftstore.sync-log` option in 8.5.4**. The pinned
  [write worker](https://github.com/tikv/tikv/blob/v8.5.4/components/raftstore/src/store/async_io/write.rs)
  synchronizes Raft writes before persistence notifications; disabling
  unpersisted application prevents the default 1024-entry early-apply window
  ([peer configuration](https://github.com/tikv/tikv/blob/v8.5.4/components/raftstore/src/store/peer.rs)).
  This is runtime configuration plus version-pinned source evidence, **not**
  a physical power-loss test or observation of every fsync. It does not claim
  identical live bytes or performance from a five-process count. Endpoints
  and hex encoding follow the
  [PD v8.5.4 API](https://github.com/tikv/pd/blob/v8.5.4/server/api/region.go).
* **Cassandra 5.0.9:** start storage nodes **sequentially**, checking each with
  `probe` (without `--cluster`) before starting the next. Each has one explicitly
  evenly spaced Murmur3 token, a private listen/broadcast/RPC address and a
  distinct rack. Coordinator `probe --initialize` creates the RF1 SimpleStrategy,
  durable-writes keyspace and validates CQL peers/schema. Then **every storage
  host must pass `probe --cluster`**, requiring its nodetool ring to contain
  exactly the five requested `UN` nodes, and comparing effective YAML with the
  running `system_views.settings` commitlog policy and persistent data paths.
  Actual racks, tokens and distinct CQL host IDs are checked, not just advertised.
  Coordinator-only CQL proof uses scope
  `cluster_metadata_requires_all_storage_cluster_receipts`, not full ring/ack
  admission. `commitlog_sync=batch`, not periodic, is
  required: the pinned
  [5.0.9 configuration](https://github.com/apache/cassandra/blob/cassandra-5.0.9/conf/cassandra.yaml)
  specifies that batch/group wait for commitlog flush before acknowledgement.
  [SettingsTable](https://github.com/apache/cassandra/blob/cassandra-5.0.9/src/java/org/apache/cassandra/db/virtual/SettingsTable.java)
  reads the running raw configuration. JMX is local with node-index-specific
  ports 7199–7203 for loopback qualification, not a remotely exposed admin API.
* **FoundationDB 7.3.77:** five storage-class processes with distinct machine/zone
  IDs and one transaction-class coordinator process, all explicitly addressed.
  Coordinator initialization uses `configure new single ssd logs=1
  commit_proxies=1 grv_proxies=1 resolvers=1`, as supported by the
  [7.3.77 configuration parser](https://github.com/apple/foundationdb/blob/7.3.77/fdbclient/ManagementAPI.actor.cpp).
  Storage roles are recruited by FDB, not invented from `--class`: live
  `status json` must show availability, full replication, single/RF1, all six
  correct version/address/locality identities, actual storage roles on **all
  five storage hosts**, plus actual log/proxy/resolver/master/controller roles.
  Full status is freshly fetched on **each of the six node probes** and retained,
  including placement and statistics; five empty processes without recruited
  storage roles fail admission. `prepare` and every `start` create the admitted
  `/data/logs` directory: the pinned server opens trace files but does not create
  its `--logdir`. Process-reported trace/file creation errors fail admission.

For actual local protocol qualification, set `qualification_only: true`, use six
distinct loopback IPs (for example `127.0.0.20` through `.25`) and unique
pseudo-machine IDs, and optionally use
`/tmp/tidehunter-kv-comparison/<run_id>` as data root. Execute every node-index
command on the same Linux Docker host. This bypasses physical NVMe admission;
all actual database/topology/replication/ack checks remain. TiKV additionally
uses an explicitly nonphysical 8 GiB store-capacity limit and PD's supported
`region-score-formula-version="v1"` **only in qualification**. The default v2
score has a 50 GiB absolute low-space term
([pinned implementation](https://github.com/tikv/pd/blob/v8.5.4/pkg/core/store.go));
on an 8 GiB loopback fixture tiny free-byte differences otherwise evacuate an
entire owner after scatter. Physical plans keep v2 and do not set a capacity
override. Actual schedule configuration is retained and admitted.
FoundationDB qualification explicitly passes
`--knob-min_available_space_ratio 0.01` to all six servers. **Physical plans pass
no knob override and retain the pinned 0.05 default.** The absolute 100,000,000
byte minimum and 0.01 safety-buffer ratio are not overridden
([7.3.77 ServerKnobs](https://github.com/apple/foundationdb/blob/7.3.77/fdbclient/ServerKnobs.cpp)).
This is only for a finite tiny functional fixture on a shared host, not a
relaxed cloud admission policy. The actual local diagnosis found approximately
3.6–3.9% free space on the shared filesystem: Ratekeeper trace events reported
`TPSLimit=0`, `Reason=7`, which the
[pinned Ratekeeper source](https://github.com/apple/foundationdb/blob/7.3.77/fdbserver/Ratekeeper.actor.cpp)
names `storage_server_min_free_space_ratio`. That blocked ordinary transactions
and later global status queries; increasing status timeouts would not fix it.
The observed coordinator cgroup had zero CPU-throttled periods, so the declared
CPU split remains unchanged. Each FDB proof now records the inspected actual
server argv and local `statvfs` total/available bytes and ratio, alongside the
declared ratio/default-source basis. Unexpected argv/knob changes fail admission.
Plans/receipts mark qualification/nonphysical explicitly. These results cannot
establish EC2/network latency, independent-NVMe scaling or cloud performance.

Actual local control-plane qualification completed two fresh five-TiKV/one-PD
bootstraps separated by guarded data reset: each coordinator proof reported
24 healthy RF1 regions with leaders/peers on all five stores; the first also
retained all six node-local probes. An attempted reset while those containers
existed was rejected. Owned exports, reset receipts and the second bootstrap
proof are under ignored
`checkpoint-work/kv-comparison/qual-placement-durable-b0414d3b/`; final cleanup
reported no errors and stopped every created container. These are actual
topology/persistence-policy checks, not workload throughput or durability
fault-injection results. Actual FDB qualification under
`checkpoint-work/kv-comparison/qual-fdb-fixed-ce6803a29d/` retained six freshly
fetched full-status receipts with all five recruited storage roles, the actual
qualification knob argv and approximately 3.6% available filesystem space.
A subsequent fresh coordinator status was available and healthy. Its bounded
256-byte Rust mixed-workload case completed 208 offered phase operations with
zero errors or unknown mutations. These are functional observations, not
cloud-performance results; the raw workload artifacts must be consulted rather
than treating deployment readiness as proof that every workload passed.
Focused offline regressions remain for the integrating agent to run:
`python3 -m unittest discover -s experiments/kv-comparison -p 'test_cloud_layout.py'`.


## Campaigns, repeat identity and raw exports

```sh
# Default: 5 backends × 3 profiles × 3 offered rates × 5 repeats = 225 finite cells.
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/campaign.json \
  --binary target/release/kv-research --output /tmp/kv-campaign-new
# Exactly one backend, retaining the same declared seed/profile/rate/repeat definitions:
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/campaign.json \
  --binary target/release/kv-research --backends cassandra --output /tmp/kv-cassandra-rerun-new
# Explicit local subset: comma-separated backend IDs; unsupported, empty, or
# duplicate selections fail before deployment/resource acquisition.
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/campaign.json \
  --backends tidehunter_tcp,tikv --binary target/release/kv-research --output /tmp/kv-selected-new
# API qualification also accepts an explicit backend subset:
python3 experiments/kv-comparison/campaign.py --manifest experiments/kv-comparison/ci.json \
  --backends native,tidehunter_tcp --binary target/debug/kv-research --qualification --output /tmp/kv-api-selected-new
# Emergency/repeated exact-owned cleanup from an explicit campaign result root:
python3 experiments/kv-comparison/cleanup.py --root /tmp/kv-campaign-new
```
An explicit `--backends` subset must be admitted by the selected tracked or
custom manifest; the option cannot add an ID the manifest does not name. For
manual non-default Tidehunter transports, put the matching central case ID in
a custom manifest and select that same ID. For example, a verbs config belongs
to `tidehunter_verbs`, not `tidehunter_tcp`:

```sh
python3 experiments/kv-comparison/campaign.py --manifest /path/to/custom-manifest.json \
  --backends tidehunter_verbs --transport-config transport-verbs.json \
  --binary target/release/kv-research --output /tmp/kv-verbs-new
```

`--transport-config` requires exactly one selected Tidehunter case and must
match its provider/security; it changes only host-local selectors, not the
case identity, report label, or transport cohort.


### Valkey TCP and the separate future InfiniBand path

`valkey_tcp` is a real five-primary Valkey Cluster comparator in `distributed`
layouts, not a standalone server relabeled distributed. The maintained
redis-rs 1.8.0 client caches the cluster slot map and handles MOVED/ASK responses.
One MGET uses its native cross-slot split/ordered reassembly, including duplicate
and missing keys. No adapter-wide mutex serializes operations. The Rust feature
is `valkey-tcp`; `put_batch` is unsupported, not emulated with separate SETs.

The digest-pinned image is
`valkey/valkey:9.0.7@sha256:7e1deda01b64ece3167d370ab5be3452d29734feec22cbcbc69b3a64f6859d6d`.
Admission records actual server/configuration/persistence evidence and requires
RF1, no replicas, five connected primaries and unique coverage of all 16,384
slots. Lifecycle readiness compares every primary's slot view and cluster links.
Only private, isolated, unauthenticated TCP endpoints are supported.

```sh
# Standalone real-API qualification; helper owns startup/export/cleanup:
python3 experiments/kv-comparison/campaign.py \
  --manifest experiments/kv-comparison/ci.json --backend valkey_tcp \
  --binary target/release/kv-research --qualification --output /tmp/kv-valkey-api-new
# The normal campaign.json distributed layout uses all five primaries:
python3 experiments/kv-comparison/campaign.py \
  --manifest experiments/kv-comparison/campaign.json --backend valkey_tcp \
  --binary target/release/kv-research --output /tmp/kv-valkey-distributed-new
```

GET reads the in-memory Valkey database; AOF is not a cold-read/NVMe tier.
Writes use AOF `appendfsync=always`, including during rewrite; normal maintenance
is not disabled. A memory-capacity failure under `noeviction` is a real failure,
not a reason to silently evict or change persistence. Do not claim equivalent
durability/security to other cohorts from common API shapes alone.

[`../valkey-infiniband`](../valkey-infiniband/README.md) contains only independent
offline planning, preflight and bounded functional qualification against an
operator-supplied future InfiniBand endpoint. It is excluded from active
manifests, package source bundles and CI backend choices. There is no Valkey
RDMA suite backend, EFA substitution or TCP fallback. Hardware qualification and
a matched TCP/RDMA comparison must precede any future suite integration.


The default primary matrix includes `tidehunter_tcp`, TiKV, Cassandra,
FoundationDB and Valkey Cluster TCP. Optional embedded controls stay in a **separate** copied manifest
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
`cargo test --locked -p kv-research --features tikv,cassandra,foundationdb,fabric,valkey-tcp`, fixed
job/step deadlines, sequential real native/Tidehunter-TCP/TiKV/Cassandra/FDB/Valkey qualification under
`--observe`, and actual Prometheus/Grafana datasource plus concurrent provisioned
dashboard query verification after each backend (`stack.py verify --output JSON`).
Unconditional cleanup independently attempts both exact-owned database cleanup
and observer stop, preserving failure if either fails; artifacts survive failure.
Only result/provenance/query receipts are uploaded, never observer `.state` or
its generated secret. It has no push/benchmark-branch/paid trigger. The
workflow-dispatch `backends` input accepts a comma-separated supported subset;
blank uses the ordered backend list in tracked `ci.json`. Pull requests use
that same tracked list. Invalid, duplicate, or empty list entries fail before
resource acquisition. A failed requested dependency, server, readiness or
adapter exits nonzero. A tiny single-process test proves only the functions
actually exercised, not distributed latency, replication resilience or
physical ownership scaling.

The local job installs pinned boto3/PyYAML in its own virtual environment for the
offline lifecycle tests; having the SDK installed does not grant AWS credentials.
Local and cloud TiKV launches explicitly allow 1,048,576 open descriptors instead
of inheriting Docker's host-dependent limit. The first PR runner inherited only
65,536 and TiKV rejected startup before workload execution. This follows
[TiKV's deployment prerequisite](https://docs.pingcap.com/tidb-in-kubernetes/dev/deploy-failures/#ulimit-is-not-large-enough);
the harness does not reduce RocksDB's file budget to conceal a startup failure.

Current normal-run validation also requires present values when preload covers
the dataset and no configured operation can delete a key. Missing ordinary reads
or missing members of read-many groups then fail instead of inflating successful
goodput. Delete-capable and unpreloaded workloads retain valid absence semantics.
Historical exports made before this oracle fix remain unchanged; their success
counts alone do not certify that every expected value was present.
Current closed-loop dispatch also replenishes an idle worker rather than placing
a replacement behind a busy worker's private queue. Older concurrent closed-loop
throughput and response times can include that scheduling bias. This does not
explain single-outstanding-request latency or separate Sui checkpoint results.
The open-loop arrival schedule, admission bound and rejection semantics remain
unchanged.
TiKV repetition matching uses the adapter's `tikv_raw` identity. It retains
configured placement/version while excluding volatile store IDs and heartbeat
observations; the original deployment evidence remains unchanged.

Credential-free CI qualification allows 15 minutes for native prerequisite
downloads within an 85-minute job limit. A slow package mirror previously exhausted
the five-minute setup step before compilation. This does not extend the paid
fleet's three-hour lifetime or enable AWS access during qualification.

Plots retain validated partial failures as diagnostics and coverage, never as
successful performance points. Cold-read availability is derived from the current
export, not historical FoundationDB failures. Primary curves require common
completed repeats across all requested systems; the separately labeled cold
cohort explicitly lists excluded systems and still requires matched repeats.

Release preparation probes `rustc` and records `cargo` from the extracted source
directory, just like compilation. This makes its admitted `rust-toolchain.toml`
select Rust 1.90 instead of a hosted runner's unrelated default toolchain.

EFA startup admission follows the Rust endpoint hints, not the transport label:

| Path | Provider | Fabric | Requested capabilities | Requested mode |
| --- | --- | --- | --- | --- |
| Ordinary EFA messages | `efa` | `efa` | `FI_MSG\|FI_DIRECTED_RECV` | `FI_CONTEXT` |
| Direct messages, including EFA-native without resident reads | `efa` | `efa-direct` | `FI_MSG` | `FI_CONTEXT2` |
| Registered-RAM reads | `efa` | `efa-direct` | `FI_MSG\|FI_RMA\|FI_READ\|FI_REMOTE_READ` | `FI_CONTEXT2\|FI_RX_CQ_DATA` |

`fi_info -p` selects a **provider**; `-f` selects a **fabric**. Passing
`efa-direct` to `-p` rejected the first fleet before any measurement.
The [pinned CLI implementation](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/util/info.c),
[provider rules](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md)
and [Rust hints](../../tidehunter-fabric/src/endpoint/resources.rs#L540)
define these selectors and masks. The pinned direct-RMA provider requires
hardware read **and write** support to advertise RMA.
[AWS lists both capabilities for all four permitted machine types](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html#efa-instance-types);
this establishes type eligibility, not actual device or endpoint admission.
Native-read, registration and geometry checks remain mandatory. No TCP fallback
is introduced. Each host retains a source/binary/host-bound
`fabric-admission.json`, including passed observations before a later failure;
discovery alone does not certify endpoint operation or performance.

`campaign-validation.json` reports benchmark validity separately from resource
closure. A startup export without `matrix.json` remains incomplete, with its
original controller error and unknown completed-case count; it does not acquire
invented metrics. `cleanup-validation.json` reports verified resource closure
independently. A successful cleanup is not a successful benchmark; unsafe or
tampered export evidence is still rejected.

Atomic allocation attaches the six distinct instance profiles after launch.
The KV installer now mirrors the retained
[Sui atomic bootstrap's post-profile agent restart](../aws-sharded-wal/atomic_start.py#L285):
after its private bundle download and runtime admission prove profile access,
it restarts the already-running SSM Agent before publishing readiness.
[AWS documents role discovery or agent restart after role attachment](https://repost.aws/knowledge-center/assign-iam-role-ec2-instance).
`ssm-agent-profile-refresh.json` records this process activation, **not** successful
managed-node registration. The controller must still obtain real command
acceptance within its existing bound; neither timeouts nor IAM policies are
relaxed. The real registration path cannot be validated on a non-EC2 laptop.

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

## Source-bound cloud packages

`cloud_package.py` performs **local packaging only**; it never calls AWS or
launches a fleet. The release target is Ubuntu24.04 x86_64, Rust1.90.0, with
`tikv,cassandra,foundationdb,fabric` in one executable. RocksDB is not needed for the
cloud matrix. `fabric` enables the real engine fabric feature, including direct
and device-RMA code; it does not install or qualify EFA hardware.

The protected AWS workflow selects **one** suite through
`../aws-sharded-wal/suite.json` (default `kv-comparison`), with an explicit manual
override for the original `sui-transport` suite. It does not run both campaigns.
Its KV compiler job is credential-free, `contents: read` only, and never checks
out Sui or downloads a checkpoint fixture. The pinned Rust toolchain, locked
Cargo sources, checksum-pinned libfabric 2.6.0 and digest/hash-pinned FDB client
feed the maintained `cloud_package` release build. All five features
`tikv,cassandra,foundationdb,fabric,valkey-tcp` remain mandatory.

The protected cloud workflow accepts a `backends` comma-separated
`workflow_dispatch` override. Blank leaves the tracked
`experiments/kv-comparison/cloud-campaign.json` backend list in effect;
push/default runs therefore retain all nine backend/transport IDs and the
existing 258 one-repetition cells. A nonempty value selects an explicit
supported subset (for example `tidehunter_tcp,tidehunter_efa`); invalid,
duplicate, or empty elements are rejected before cloud resource allocation.
The selected IDs and expanded jobs are frozen into campaign artifacts.


`ci_prepare.py build --engine PATH --output PATH [--deadline 10800 --jobs 2]`
requires the clean exact `GITHUB_SHA` checkout on the trusted protected branch.
It freezes, builds and seals the existing package, then runs that actual release
against native, five-owner loopback Tidehunter TCP, and real local
TiKV/Cassandra/FDB qualification servers. Complete same-binary receipts, raw
result hashes and source identities are required before readiness. These
software API checks are not EFA hardware, throughput or physical-HA evidence.
The existing observability/harness and software-provider tests run before the
release preparation; actual EFA hardware admission remains on the fleet.

`ci_prepare.py package --inputs PATH --output ARCHIVE` transports only
`READY.json`, the sealed `bundle.tar.gz`, and bounded ordinary qualification
evidence. `restore --archive ARCHIVE --sha256 HASH --output PATH` validates the
outer SHA, safe paths, unique bounded regular files and modes before extraction.
It restores the existing runtime under `package/` and checks the frozen source
inventory against the current clean orchestration checkout **before credentials**.
`READY.json` binds `engine_sha=source_commit=GITHUB_SHA`, bundle/source-tree/source
archive/manifest/binary identities and the complete qualification inventory.
No expanded compiler output, database or fixture crosses the job boundary;
runtime executable modes and monitoring source are retained.

The operator consumes
`ci_campaign.py run --config ci-config.json --inputs PREPARED_DIR --output PATH
--budget-file ../aws-sharded-wal/budget.json`; cleanup uses the same config/output/
budget. It reuses the existing protected environment, exact-owned cleanup,
independent watchdog and shared ledger rather than creating another cost
authority. Local workflow dispatch never obtains fleet credentials.

The admitted full matrix can emit 21,520,896 CSV rows across preload, warmup and
measurement. `ci-config.json` sets a separate 32-GiB raw export bound and reserves
the same conservative retained-result allowance for S3; it does not assume a
compression ratio. Monitoring archives retain their own 4-GiB bound. The shared
budget guard still filters unaffordable regional candidates before reserving.
Per-role CPU/RAM limits come from the CI config and remain in deployment receipts;
do not attribute differences from an older campaign to code alone if these limits
or cache/workload contracts differ.

Freeze **after all source changes land**. Every output directory must be new:

```sh
PACK="$PWD/checkpoint-work/kv-comparison/cloud-package"
FDB="$PWD/checkpoint-work/kv-comparison/fdb-client-7.3.77"
FABRIC="$HOME/.cache/tidehunter/libfabric-2.6.0"
mkdir -p "$PACK"
# If absent, prepare the native client using the existing digest-pinned image:
# python3 experiments/kv-comparison/local.py prepare-fdb --prefix "$FDB"
python3 -B experiments/kv-comparison/cloud_package.py snapshot \
  --output "$PACK/snapshot-v1"
python3 -B experiments/kv-comparison/cloud_package.py build \
  --snapshot "$PACK/snapshot-v1" --output "$PACK/build-v1" \
  --fdb-prefix "$FDB" --fabric-prefix "$FABRIC" --timeout 5400
python3 -B experiments/kv-comparison/cloud_package.py stage \
  --snapshot "$PACK/snapshot-v1" --build "$PACK/build-v1" \
  --fdb-prefix "$FDB" --output "$PACK/stage-v1"
python3 -B experiments/kv-comparison/cloud_package.py verify \
  --directory "$PACK/stage-v1"
```

The actual Cargo command uses `--release --frozen --target
x86_64-unknown-linux-gnu -p kv-research --no-default-features --features
tikv,cassandra,foundationdb,fabric,valkey-tcp`, two build jobs, generic `target-cpu=x86-64`,
generic C/C++ flags, and source-path remapping. Inherited Rust native-CPU flags
are discarded. It builds the **extracted frozen bytes**, not the mutable checkout,
and admits the source again after compilation. `--frozen` will fail rather than
update Cargo.lock or download missing dependencies; populate the ordinary local
Cargo cache from the locked workspace beforehand if needed.

### Ubuntu24 container build when the host ABI is newer

The inspected development host had glibc2.39, equal to Ubuntu24's baseline;
that observation does **not** admit an old executable. Every produced executable
and mapped native dependency is examined with `readelf` and `ldd`. A host newer
than glibc2.39 is rejected *before* compilation; individual newer
GLIBC/GLIBCXX/CXXABI/OpenSSL requirements, unknown SONAMEs and absolute rpaths
are also rejected rather than uploaded blindly.

`Dockerfile.cloud-build` supplies a real local Ubuntu24 build environment:
amd64 base manifest
`sha256:a2d43bffc46670d7108bb3c0468c67c5557c4c92e83e1e34cbff1322ad1b94dc`,
signed Ubuntu package snapshot `20251001T000000Z`, and the existing
checksum-pinned AWS EFA1.50.0 installer. Setup commands have finite deadlines.
It does not fetch a Rust toolchain or floating libfabric. Mount the existing
Rust1.90.0 toolchain and Cargo registry cache read-only, then perform the actual
release build without networking:

```sh
# Use this INSTEAD OF the host "build" command above, before staging.
# PACK/FDB variables and snapshot-v1 are from the previous block.
timeout --kill-after=30 2400 docker build --platform linux/amd64 \
  -f experiments/kv-comparison/Dockerfile.cloud-build \
  -t tidehunter-kv-cloud-builder:local experiments/kv-comparison
BUILDER="$(docker image inspect tidehunter-kv-cloud-builder:local --format '{{.Id}}')"
RUSTC="$(rustup which --toolchain 1.90 rustc)"
RUST_TOOLCHAIN="$(dirname "$(dirname "$RUSTC")")"
timeout --kill-after=30 5700 docker run --rm --platform linux/amd64 \
  --network none --cpus 2 --memory 8g --pids-limit 1024 \
  --user "$(id -u):$(id -g)" --tmpfs /cargo:rw,mode=1777 \
  --mount "type=bind,src=$RUST_TOOLCHAIN,dst=/rust,readonly" \
  --mount "type=bind,src=$HOME/.cargo/registry,dst=/cargo/registry,readonly" \
  --mount "type=bind,src=$PWD/experiments/kv-comparison,dst=/package/experiments/kv-comparison,readonly" \
  --mount "type=bind,src=$PACK,dst=/work" \
  --mount "type=bind,src=$FDB,dst=/fdb,readonly" \
  --env "CLOUD_PACKAGE_BUILDER_IMAGE=$BUILDER" \
  "$BUILDER" --snapshot /work/snapshot-v1 --output /work/build-v1 \
  --fdb-prefix /fdb --fabric-prefix /opt/amazon/efa --timeout 5400
```

The builder image ID and source-bound recipe hash are retained in `build.json`.
The recipe/commands are implementation, **not an observed successful build**:
run them and retain their actual receipts before declaring any artifact ready.
Missing cached crates, Docker availability, installer/setup failures and actual
provider/ABI admission failures remain hard failures, not synthetic readiness.

### Immutable interfaces and deployment admission

All manifests use `schema_version:1`:

* `source.json`: base `revision`, actual `branch`, truthful `dirty`, `sha256`
  (relative source path → SHA256), `tree_sha256` (canonical inventory hash), and
  `snapshot_sha256` (exact deterministic `source.zip` SHA256). Tracked dirty
  files and new comparison-framework files are included. Workspace manifests,
  Cargo.lock, native engine/fabric/examples/support sources and controller
  helpers are required. Unrelated untracked notes, databases/checkpoints,
  compiler caches, keys/credentials and symlinks are excluded. Suspected private
  credential bytes in otherwise permitted source files fail admission.
* `build.json`: frozen source/archive identities, exact feature/CPU/target and
  Cargo command, Rust/Cargo versions, executable SHA256/Blake2b-512, FDB identity,
  build-time libfabric identity, `ldd` mapped library hashes and ELF version
  requirements. The build log is hashed. A receipt is written only after success.
* `manifest.json`: source identities, executable `binary_sha256` and
  `binary_blake2b512`, `features`, `runtime_efa`, fixed relative `paths` and
  `files` (relative path → `{bytes,sha256,blake2b512}`).
* `bundle.json`: caller-admitted outer archive and manifest identities, each
  `{bytes,sha256,blake2b512}`. It is written only after the actual archive passes
  admission and the checkout still matches the frozen source inventory.

The tar contains `source.zip`, `source.json`, extracted `source/`, `build.json`,
`build.log`, `manifest.json`, `runtime/bin/kv-research`,
`runtime/lib/libfdb_c.so`, and `runtime/fdb-client.json`. Binary/native libraries
are **not** in the source ZIP. Source is bounded to256MiB/20,000 files,16MiB per
file; the entire expanded runtime tar is bounded to512MiB. Archives have sorted
ordinary-file members, normalized timestamps/owners and no link/traversal paths.
No database, checkpoint, operator credential or unrelated untracked note is
published. A changed checkout requires a new snapshot **and** rebuild.

Stage the bundle at an immutable directory on each node. Keep fleet/controller
config, bootstrap receipts, result directories and Python bytecode **outside**
that directory. Execute Python helpers with `-B`/`PYTHONDONTWRITEBYTECODE=1`.
The controller receives `repository_root=<stage>/source`,
`binary=<stage>/runtime/bin/kv-research`,
`fdb_library_dir=<stage>/runtime/lib`, and `source_sha256` meaning the exact
`source.zip` SHA256, not merely a git revision.

Before extraction, the operator/bootstrap must admit the outer SHA from the
locally retained `bundle.json` (never derive the trusted hash from the download):

```sh
python3 -B experiments/kv-comparison/cloud_package.py verify \
  --archive "$PACK/stage-v1/bundle.tar.gz" --sha256 "$ADMITTED_BUNDLE_SHA256" \
  --extract-to "$PACK/admitted-v1"
# On the real EFA guest after bootstrap, using its installed Python helper:
python3 -B "$STAGE/source/experiments/kv-comparison/cloud_package.py" verify \
  --directory "$STAGE" --runtime
```

`verify --runtime` verifies actual loader resolution to the staged native FDB
client and `/opt/amazon/efa/lib`, mapped library ABIs, installed fabric version
and the FDB client's own version API. Do not set `LD_PRELOAD`/`LD_AUDIT`.
The getter requires successful `fdb_select_api_version_impl(730,730)` first;
calling it before API selection can crash natively. Both local `fdb_identity`
(build/stage admission) and guest runtime admission use the same isolated Python
child, with explicit ctypes signatures, checked API-selection status, a
null/version check, faulthandler diagnostics and a10-second deadline. They never
initialize/reselect process-global FDB API state in the long-lived parent and
never start FDB networking. The actual returned `client_version` is retained in
the FDB receipt and compared again on the guest, so local admission exercises
native initialization before any fleet launch.
Hardware `fi_info`/endpoint/RMA admission and real workload checks remain the
controller's responsibility: a local software provider is **not** EFA evidence.

The FDB7.3.77/API730 client from the pinned image was inspected directly:
SHA256 `617572467897d0360f76ffc33f24b58090ee691e3273750d2ff334cc71aae3fa`,
Blake2b-512
`00724ddd562fe794722eefb0acd3fb8ccfcd001500a5c8912aa34ba1b0853350a9da34f47268d4bf514285fa21088c809aa8c2fcf965e253f39498b38deee638`.
Its required GLIBC symbols stop at2.17. Its dynamic dependencies are
glibc/pthread/rt/dl/math, liblzma5 and zlib1, **not** dynamic libstdc++ or OpenSSL;
the final executable's separate dependencies still require their own admission.
Package validation checks both native-client hashes rather than trusting only
`client.json` or a library filename.

The pinned bootstrap EFA installer is1.50.0 with SHA256
`fa6dff8593d866866c13cb4640d9059835cd4efa427971f100ab40c97bef2841`.
[AWS's release table](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-changelog.html)
explicitly maps1.50.0 to libfabric2.6.0amzn1.0/rdma-core64.0.
Do **not** update to current1.51.0/libfabric2.7.0amzn2.0: the engine's strict
provider206.0 negotiation has not admitted that newer provider.
The engine's actual checked contract is AWS libfabric2.6.0amzn1.0,
provider206.0: ordinary EFA protocol4, direct-EFA protocol1, with exact progress/
mode/queue/MTU checks in the engine. The locally cached upstream2.6.0 may compile
the matching ABI, but is deliberately **not** packaged as the AWS runtime.
Guests require the pinned AWS provider, `FI_EFA_USE_DEVICE_RDMA=1`, real device
RMA capabilities and no TCP fallback. No successful native/EFA build or
hardware qualification is claimed from packaging source alone.

Focused offline tests for the integrating agent:
`python3 -B -m unittest discover -s experiments/kv-comparison -p test_cloud_package.py`.
The native regression uses the actual SHA-pinned7.3.77 library from
`checkpoint-work/kv-comparison/fdb-client-7.3.77/lib` or `FDB_CLIENT_LIB_DIR`,
including repeated/concurrent admissions and the local identity receipt.
That class explicitly skips if the real dependency is absent; it does not
substitute a mocked version. Child failure/diagnostic containment remains
covered without the native dependency.


## Development repetition policy

Future development campaigns use **one independent repetition**, not fewer
measurements within each case. Adding Valkey TCP preserves all previous 228
ordinary cells and adds its 25 common cells: **253 unprofiled cases**. The default
also adds five separately labelled `write_stage_diagnostics` cases, one 1-KiB
concurrent-write case for native and each Tidehunter transport, giving **258
cases**, not extra independent repetitions.
`diagnostics.write_profile_limit=0` disables these additions and restores 253.
With `repeats=3`, those plans contain 774 and 759 cases, respectively.
Publication and explicit reproducibility checks keep multiple independent
repetitions; two or three runs alone do not justify a confidence or significance claim.

The one-repetition default leaves all nine backends/transports, five sizes,
workloads, concurrency and offered-load levels, cache/registered-RAM/cold-storage
cohorts, preload, warm-up, correctness checks and per-case operation/sample counts
unchanged. The 10,800-second hard deadline and 600-second export reserve also stay
unchanged. Every operation retains its raw API, scheduling and response clocks.
Per-case reports retain nearest-rank p50/p95/p99/p99.9, mean, population standard
deviation, maximum, throughput and rejection counts. Means and standard deviations
for all three clocks are computed after the measurement; undispatched rejections
have response timing but no invented API/scheduling timing. Tail sample-count
warnings remain.

### Single-run figures

The plotter examines **all planned cases**, including failures and `not_run`
entries. An explicitly one-repetition plan can use its single common completed
repetition across the compared cells. Its titles, captions, manifest and CSV say
one independent repetition and no run-to-run variability estimate; it draws no
run-to-run error bars. Within-case latency standard deviation is not a
between-run error bar.

A historical or publication plan with multiple repetitions still requires at
least two shared completed repetitions for a matched multi-run figure. Missing
repetitions do not silently turn that plan into a single-run result. Incompatible
populations remain separate; missing/failed observations are never zero-filled.
Original historical repetitions and their hash-bound artifacts are not changed.

### Selective follow-up decisions

Use existing matched repetitions before buying another run. The
[retained variability assessment](../../docs/distributed/reports/2026-10-25-kv-comparison-ci-40226403293/variability-summary.json)
analyzes the two completed histories separately, without pooling their changed
CPU/memory or harness settings. Its
[policy](../../docs/distributed/reports/2026-10-25-kv-comparison-ci-40226403293/variability-policy.json)
declares these development-triage thresholds before any follow-up measurement:

- API latency: a 10% relative effect and at least a 1 µs absolute effect.
- Achieved throughput: a 10% relative effect.
- Rejection fraction: a one-percentage-point absolute effect.

These are engineering decision thresholds, not statistical confidence or
equivalence bounds. A different question can use different thresholds, but record
the metric, direction and threshold **before** its rerun. Do not change a threshold
after seeing a favorable result.

For each stated comparison, check whether the observed historical spread can
change the material conclusion. If every observed baseline/candidate combination
gives the same conclusion, do not add development repetitions merely to confirm
it, including an unfavorable conclusion. Observed overlap or a small difference
is not a claim of statistical insignificance.

Repeat only report-relevant uncertain or regressed **workload configurations**,
not the full matrix to fill a missing third repetition. A follow-up needs a
reviewed, frozen comparison plan: source/binary identities, server image versions,
hardware/resource budgets, workload/seed and all case settings, guarantee/security
contract, metrics/thresholds, requested repetition count and the **complete set of
backends involved in that comparison**. Run each involved backend under that
plan, not just the preferred backend. Reject a changed-setting comparison as
confounded rather than calling it a reproducible regression. No follow-up is
automatically scheduled by the variability report.

The 253-case unprofiled development matrix is not narrowed by this policy.
Diagnostic cases are additions, not replacements or rows pooled with unprofiled
results. Targeted follow-ups require their own explicit reviewed selection;
publication and reproducibility checks keep multiple independent repetitions.

## Manual source-bound cloud comparison

The manual cloud path is separate from the existing local/CI campaign:

| File | Responsibility |
|---|---|
| `cloud-campaign.json` | One development repetition, five value sizes, nine backends: 253 ordinary plus five diagnostic cases by default. Three repetitions give 774 cases, or 759 with diagnostics disabled. The 10,800-second absolute bound and 600-second export reserve are unchanged. |
| `cloud_campaign.py` | Identical common profile order per repetition, typed configurations and JSON/CSV/Markdown raw-sample reports. |
| `cloud_package.py` | Dirty-source snapshot, frozen generic-x86 release build, exact hashes and immutable runtime archive. |
| `cloud_bootstrap.py` | Creation-only termination timer before installation, pinned EFA/runtime admission, ready markers and automatic controller startup. |
| `cloud_controller.py` | Guest-side bounded SSM/S3 orchestration, physical admission, case gates, exports and termination in `finally`. It does not allocate EC2. |
| `cloud_layout.py` | Exact-owned node-local preparation, live readiness, export, stop and explicit data reset. |
| `cloud_mcp_budget.py` | Existing conditional-write budget ledger through MCP; no alternate ledger or local AWS credential fallback. |

Allocate one controller/client plus five independent instance-store hosts, in
one AZ and a **cluster** placement group. Operator provisioning, observation and
cleanup use AWS MCP. The guest controller uses scoped instance credentials for
SSM/S3 and exact-fleet termination; the human identity need not have SSM.

Each storage role needs `s3:GetObject` as well as `s3:PutObject` on its **own**
`<run>/results/cells/*/storage/<index>/*` objects. Receipt publication performs
`PutObject` followed by `HeadObject`; AWS authorizes the latter with
[`s3:GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).
An uploaded receipt alone does not prove that the SSM command succeeded.
Keep input reads and that node's deployment-capsule reads separately scoped;
do not grant bucket listing, cross-node reads or cross-run access. Before
allocation, simulate the actual role policies **and permissions boundary** for
own-receipt read/write allowance and cross-node/cross-run denial. With multiple
S3 resources, inspect IAM's concrete `ResourceSpecificResults`, not the grouped
`EvalResourceName`/`EvalDecision`.

Exercise a real five-owner local TCP client using the generated cloud options
before sealing a release. `--validate-config` admits the outer typed schema;
it does not open the distributed authority. In particular, the authority's
`max_admitted_batches` limit is **1,000,000**, not 1,048,576. External preparation
rejects values above that engine bound before emitting launch files. Host
startup and stop use bounded five-worker barriers outside measurement; the
client starts only after all five startup proofs, and teardown joins all five
operations before the next case.

Do not launch a bootstrap-only fleet: user data must contain the admitted bundle
and automatic controller capsule before allocation. The exact
`<run>/inputs/controller.json` object is an **activation envelope**, not the raw
controller config. Its pending form has exactly `schema_version=1`,
`state="pending"`, `run_id` and the admitted source ZIP's `source_sha256`.
The ready form has the same header, `state="ready"` and one additional `config`
field containing the existing 14-key raw controller config.

The operator uses AWS MCP for this creation/publication sequence:

1. **Before allocating any EC2**, build `cloud_bootstrap.activation_record(plan)`
   and serialize it to UTF-8 JSON. Precreate the activation object using S3
   `PutObject` with `IfNoneMatch="*"`, `ExpectedBucketOwner`, AES256 SSE and the
   exact serialized body's SHA256 in object metadata/checksum. Verify the full
   downloaded bytes, length, SHA256 and SSE; retain the returned ETag and
   VersionId with that version's verified SHA256. A conditional-write conflict
   is a precondition failure to reconcile, not permission to overwrite a run.
2. Launch only after this pending object and the immutable source/runtime bundle
   are admitted. Bootstrap may read the existing pending state normally while
   the six hosts install. Both envelope states must match the exact plan run
   and source; unknown fields, versions or states fail closed.
3. After **all six guests are ready** and MCP has bound their exact IDs,
   profiles/policies and private addresses, validate the raw physical config
   with `cloud_controller.py validate --config CONFIG.json`. Build
   `cloud_bootstrap.activation_record(plan, config)` and replace pending with
   ready using `PutObject` **`IfMatch=<the admitted pending ETag>`**, the same
   expected owner/SSE, and the ready body's SHA256 metadata/checksum. Verify the
   full ready bytes, SSE, new ETag/VersionId and that version's SHA256. Never
   substitute an unconditional overwrite if the CAS fails.

`cloud_bootstrap.activation_config(plan, value)` returns `None` only for a valid
pending envelope. For ready it validates and returns the raw config, binding its
run/source/binary, account, bucket, region, absolute deadline and package paths to
the admitted plan. Bootstrap unwraps ready into its local raw `controller.json`,
checks the actual coordinator assignment and starts the existing controller.
Local physical-config validation still consumes the raw 14-key config, never an
envelope. There is no legacy raw-S3-config compatibility path.

This gate deliberately requires no `s3:ListBucket` permission. With least-privilege
object-only access, S3 can return **403 AccessDenied for a missing key**, not
`NoSuchKey`; treating that as normal waiting would also hide real authorization
failures. Only explicit valid pending means wait. Missing objects, 403/NoSuchKey,
invalid records and stale bindings are real precondition failures and fail fast.
The existing 900-poll bound and 600-second export reserve still apply; pending
does not extend the absolute lifetime.
The relative guest TTL is fixed by the admitted plan, at most 10,800 seconds;
the immutable absolute deadline may be earlier.
Never recompute TTL as “remaining time minus a margin” while
keeping the old absolute deadline: that can make the guest's pending-time bound
unreachable. The renderer rejects expired or unreachable lifetime contracts
locally, before allocation. Preserve the exact admitted plan/user-data bytes in
the RunInstances request.
Independent watchdog and instance-shutdown guards do not replace final exact-ID verification.

Primary operations are full-value point reads/writes/deletes/misses, concurrent
reads/writes, ordered read-many groups of 32 and an offered-rate sweep. Values
are 64 B, 256 B, 1 KiB, 8 KiB and 64 KiB. Extra cohorts cover Zipf reads,
coordinator/owner caches, trusted native-EFA registered RAM and large-data
OS-cache eviction. EFA, EFA-direct with TLS, and trusted EFA-native without TLS
are separate backends; TCP fallback never counts as EFA. Unsupported cross-store
atomic batches are excluded from the common comparison.

The pre-measurement gate follows completed preload/warm-up. The new completion
gate holds the still-live client after measured calls, before profile finalization,
post-workload qualification, drain and shutdown. The controller captures
`measurement-before.json` and `measurement-end.json` and binds a reproducible
`resource-interval.json` to those original frames. Same-boot/cgroup-inode checks,
host-local monotonic elapsed time and nondecreasing counters are required.
The legacy through-process-exit snapshot remains a separate, broader window.
These windows include driver/oracle/background and gate/control overlap; neither
is pure engine CPU or an API latency decomposition.
Optional OS-cache eviction occurs while the pre-measurement gate holds work.
Engine caches remain: cold-OS is **not** isolated NVMe latency.
Passive owner/RMA samples still retain generation, age, availability and their
lifetime-counter scope. Registered-RAM configuration alone does not prove a hit.

Backends use the same explicit generator namespace and 32-byte logical keys per
campaign/repetition, identical primary order and full per-cell preload. FDB's
reserved-domain encoding adds one physical byte; metadata retains that difference.
This bounds logical dataset
growth. Each external backend block resets between repetitions, only after all
hosts export and stop. External services remain running within a block;
native/Tidehunter cells open fresh engine state. Preload restores logical
values, not identical physical history, compaction or engine-cache state.
These are finite API comparisons, not steady-state or equal-HA claims.

Native `value_wal_barrier` is primary; `page_cache` controls stay separate. RF1
does not equalize transaction semantics, FDB's log/storage architecture or
encrypted/plaintext transport. Report these contracts with the figures.
CPU/memory caps include control roles; quotas are not dedicated cores. Local
qualification shares one SSD and cannot predict cloud network/disk scaling.

FoundationDB reserves **6 GiB for transaction/control roles and 2 GiB for its
client** within the same 8-GiB coordinator-host allowance; CPU reservations
remain 1 + 3. Plans with less than 8 GiB on that host are rejected. The control
process uses `--memory 4608MiB --cache-memory 512MiB` inside its 6-GiB Docker cap.
The previous 2-GiB control container / 1536-MiB process limit exited with
`ERROR: Out of memory` during the cloud run's 8-GiB preloads. In FoundationDB,
[`memory` is a self-termination limit, not an allocation target](https://apple.github.io/foundationdb/configuration.html#fdbserver-section).
Do not interpret the ensuing client timeouts as cold-read latency or inherent
database ingest speed. Export live server state/logs, not only client deadlines.

The corrected split completed a local **131,072 × 64-KiB preload plus 64 reads**
with zero other outcomes and all six servers still running; all owned fixture
services and data were then removed. Evidence:
`checkpoint-work/kv-comparison/fdbbulk-3eac9a194513/qualification.json`.
This verifies the bounded scenario on one shared SSD, not general FoundationDB
memory sizing or cloud performance. Run `th-7dea3c080cb44d48` retains its original
2-GiB-control / 6-GiB-client figures; this post-run fix has **not** been benchmarked
on AWS. Never rewrite those received results as if they used the corrected split.


Failed/partial cases retain actual offered calls, errors, unknown mutations,
planned-versus-observed coverage and trustworthy raw quantiles. Unvalidated
artifacts never become performance rows. Reports include API/response/scheduling
quantiles, calls/s, logical keys/s and value MiB/s; batch latency is per complete
group, not divided by 32. Tails remain descriptive below their sample thresholds.
Preserve raw rows plus deployment, source, executable, configuration, runtime
and cleanup receipts.

The sole editable budget is `../aws-sharded-wal/budget.json`. Reserve conservative
cost through the existing S3 CAS ledger before allocation; settle usage after
exact-owned cleanup. Do not manually edit generated aggregate reports.
Estimates must identify regional prices, observed instance/volume/IP lifetimes,
storage/retrieval exclusions and billing reconciliation availability.
An estimate is not an AWS final bill.

### Live monitoring and retained offline analysis

The manual cloud controller now starts digest-pinned Prometheus/Grafana and
explicit per-host node/cgroup exporters. The collector is capped and cohosted on
the controller; TSDB writes use its root filesystem, not benchmark NVMe.
Case identity is atomic, and a bounded post-measurement observation barrier
captures even sub-scrape-duration cases without changing workload/API timing.

`results/observability/access.json` records actual private SSM access commands.
The normal retrieved result archive contains
`results/observability/monitoring.tar.gz`, its hash receipt, the original timeline
and completeness/gap evidence. Offline replay starts no database or live scraper
and needs only cached pinned images. Missing telemetry is not backfilled with
current timestamps or zero-valued performance.

See the [monitoring guide](observability/README.md) for commands, exact-versus-live
latency semantics, retention bounds, cleanup and local qualification evidence.
No EC2 run or observer-overhead calibration was performed for this integration.
The historical campaign/report below did not capture these new TSDB artifacts;
its existing host samples and raw results remain the evidence for that run.

## Retained AWS report and offline figures, 25 October 2026

[Research report](../../docs/distributed/reports/2026-10-25-kv-comparison/REPORT.md)
contains 13 comparison figures in PNG and vector PDF, the full numeric appendix,
an untouched copy of the original CSV, and source/artifact explanations.
The measured fleet is closed. This is synthetic KV evidence, not Sui replay,
isolated NVMe/RDMA latency, equal-HA semantics or a complete 684-case campaign.

With Matplotlib installed in the analysis environment (rendered with 3.11.2):

```bash
python3 experiments/kv-comparison/plot_cloud_comparison.py \
  --input checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/comparison.json \
  --output docs/distributed/reports/2026-10-25-kv-comparison/figures
```

The command performs **offline postprocessing only**. It never launches EC2,
database services or a new benchmark. `figures/plots.json` binds the input hash
and each plotted observation; `plot-data.csv` retains per-repeat values and
observed ranges. Primary comparisons use common completed r0/r1 only; extra
FDB r2 results remain separate in the numeric appendix. Missing/failed points
are never zero-filled and two repetitions do not yield a confidence claim.
The report explicitly records that distributed write profiling was disabled;
API/response/scheduling clocks are not a measured engine-stage breakdown.


## Local diagnostic qualification, 26 October 2026

No AWS operations, cluster launch, commit or push were performed for this change.
The release `kv-research` binary was rebuilt with `tikv,cassandra,foundationdb,efa`.
Native and five-owner TCP each completed one functional run on this shared SSD:

| Backend | Preload rows | Warm-up rows | Measurement rows | Observed stage samples |
|---|---:|---:|---:|---|
| Native | 4096 | 512 | 4096 | 256 scalar mutations |
| Tidehunter TCP | 4096 | 512 | 4096 | 1280 append requests, capped at 256 per owner connection |

Both real completion gates were observed before profile finalization. All measured
calls succeeded; all five TCP owner profiles were captured. The independent
coordinator interval recorded 4096 admitted/completed/durable logical batches.
Append RPCs and logical batches are different counts because groups can coalesce.
Do not treat the owner cap as a global sample limit or divide every component
clock by its owner-sample count.

Actual Prometheus capture/export and fresh offline replay both verified the
original query pages and all 27 executable Grafana panels. Offline replay had
zero live scrape targets. The authenticated browser also verified the labelled
write-stage table; dense, unreadable stat grids were replaced by scoped tables.
All owned local observers and generated database trees were then removed.

Evidence is under
`checkpoint-work/kv-comparison/diagnostic-coverage-20261026T204922Z/`:
`cases/*/local-proof.json`, raw `result/` artifacts, boundary frames,
`monitoring.tar.gz`, online/offline verification and cleanup receipts.
The real kernel-counter probe used an owned **observer-exporter cgroup**, not an
isolated benchmark-client cgroup. Raw client process-group samples remain separate.
This qualifies counter parsing, gate lifetime and rendering, not cloud CPU attribution,
EFA/RDMA performance, independent-NVMe scaling or observer-neutral latency.

Validation: `kv-research` 38 tests passed; Tidehunter 806 passed and 41 ignored
with four harness threads; both packages passed formatting and all-target Clippy
with `-D warnings`. Python suites: 221 comparison tests, 77 observability tests,
and 613 shared AWS-harness tests run with four skips. Pinned Prometheus `promtool`
rate/reset/absence fixtures passed. Two test-only deadline assumptions were fixed:
the five-node fixture has a bounded 30-second liveness guard, and the fenced-reply
test starts its synthetic deadline after fencing setup. Production deadlines did
not change.

Real-artifact qualification also fixed rejection of the full 268-field TCP stage
profile under the former 256-field schema, malformed terminal publication, and
mistaking PromQL `$1` capture replacements for unresolved Grafana variables.
The diagnostic schema remains bounded to 64 KiB and 512 numeric fields; the
16-MiB per-query-page and global archive limits were not raised.

## Local Valkey TCP qualification, 27 October 2026

The digest-pinned Valkey 9.0.7 services and rebuilt redis-rs 1.8.0 adapter were
exercised locally. The five-primary fixture admitted all 16,384 slots, zero
replicas and bidirectional cluster links. All five primaries held actual
benchmark keys after the mixed case. Standalone API qualification also passed,
including full binary values, replacement writes, deletes and ordered MGET
with duplicate and missing keys.

| Five-primary functional case | Value bytes | Measured API calls | Successful raw rows, including preload/warm-up/aging |
|---|---:|---:|---:|
| Mixed read/write/delete/missing-read/MGET | 1024 | 512 | 864 |
| Cross-slot MGET, 32 keys | 8192 | 128 | 416 |
| Cross-slot MGET, 32 keys | 65536 | 128 | 288 |

These are bounded **functional qualification** cases, not a local/cloud
performance ranking or a replacement for the unchanged research per-case
counts. Raw rows retain API, scheduling and response clocks. Prometheus and
authenticated Grafana provisioned-dashboard queries passed for the actual
Valkey results; this was API/query verification, not visual verification.

The node-local deployment interface was also exercised using six logical
loopback roles on this laptop: client-only coordinator plus five primaries,
bootstrap/full-cluster probes, actual Rust API qualification, export, stop and
data reset. All six roles completed cleanup. This does not establish six
physical hosts, independent NVMe devices or cloud performance.

Checks passed: 38 `kv-research` tests; 806 Tidehunter tests with 41 ignored;
comparison/observability/InfiniBand Python suites with 239/77/13 tests; shared
campaign harness with 613 tests and four skips. Native-only CLI and all active
adapter builds passed; all-target Clippy used `-D warnings`. The native-only
binary rejected Valkey without its feature, and the enabled binary rejected
atomic `put_batch` before connecting. The local workflow YAML and ten shell
blocks were syntax-checked, not dispatched.

Compact source/config/image/runtime/timing/monitoring and cleanup receipts are
under `checkpoint-work/kv-comparison/valkey-tcp-20261027T115459Z/`.
Exact-owner checks found no remaining task containers, observer containers or
local database volumes; node-local data directories were reset. Cached images
and pre-existing observer data volumes were not globally pruned.

The separate InfiniBand tool completed offline `plan`; `preflight` correctly
returned missing requirements. No RDMA endpoint, EFA substitution, provider
provisioning, AWS launch, Git push or paid workflow was used.

## Cloud launch preflight, 27 October 2026

Terminal export receipt schema 2 separates the bulk file inventory into
`<run>/results/controller-inventory.json`. The small control receipt contains
its exact run-owned key, length and SHA-256. Both inventory and raw archive
must pass S3 `HeadObject` verification before the launcher accepts export.
Downloaded inventory bytes are checked before any archive extraction; each
extracted file still requires its recorded length and hash. The existing
4-MiB launcher control-JSON bound is unchanged. Bulk JSON uses the existing
32-MiB guest metadata bound; raw file/count/total export bounds remain separate.
The raw archive is uploaded and verified before publishing the bulk inventory.
Inventory publication failure remains a failed export, but cannot prevent the
raw archive upload or exact-fleet termination.

The retained failed-run inventory contains 29,827 files and serializes to
5,320,840 bytes. Local execution of the actual publication/control-reader
functions preserved every entry while reducing the terminal control object
to 767 bytes. This is a local serialization/admission check using in-memory
S3, not a new cloud result or a change to that historical campaign.

The GitHub post-campaign cleanup/retrieval step has a 45-minute timeout rather
than 15 minutes. This permits full-prefix evidence reconciliation after fleet
termination; the EC2 TTL stays 10,800 seconds, its 600-second export reserve
and independent watchdog are unchanged. The editable weekly limit remains
solely in `experiments/aws-sharded-wal/budget.json`; aggregate accounting stays
in the conditional-write S3 ledger.

Both the local GitHub KV qualification workflow and the paid workflow's
credential-free KV preparation job install `matplotlib==3.11.2` before running
the full comparison harness. Renderer tests exercise real headless figures,
including the rule that a single repetition has no run-to-run error bars;
they are not skipped when the library is missing. The exact CI Python pins
were exercised in a fresh isolated Python 3.12 environment before publication.

An additional real five-primary Valkey 9.0.7 preflight passed all slot-view,
RF1/AOF/noeviction checks and mixed workloads at 64 B, 256 B, 1 KiB, 8 KiB and
64 KiB. Each case completed all 376 operations without wrong values, rejected
operations, timeouts or unknown mutations. Ordered duplicate/missing
`get_many` qualification also passed. Both owned clusters and their volumes
were closed. Evidence is under
`checkpoint-work/valkey-readiness-20261027/`; these cases are functional checks,
not substitutes for the unchanged cloud matrix or proof of Valkey RDMA,
Sui integration, cold-storage GET or cross-slot atomic writes.

