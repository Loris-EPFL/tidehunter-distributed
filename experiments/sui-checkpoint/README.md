# Real Sui checkpoint replay on the sharded Value WAL

This runner executes **actual `sui-node` fullnode checkpoint execution** against a
frozen private producer, first with NativeDb and then with this fork's RF1
sharded-WAL adapter. It does not generate a KV trace, replay a synthetic storage
workload, or use the older middleware facade. Existing RocksDB tables remain
RocksDB. Every selected Tidehunter DB must export the same namespace/key shape
and use the remote backend in the distributed run; a missing mapping or a native
fallback fails the run.

## Portable transport selection

The Sui replay's shared WAL/index and read-grant protocol is transport
independent. TCP remains the default; opt-in OFI uses the same bounded engine
transport and does not change storage locks, WAL acknowledgement, or
durability. Configure a structured transport on each applicable endpoint:

```json
{"kind":"ofi","fabric":{"backend":"verbs","domain":"mlx5_0","source_address":"10.1.0.2"},"security":"trusted"}
```

The available provider profiles are `efa`, `efa-direct`, `verbs`, and
`software-tcp`. Domain/source selectors are host-local and may differ between
peers, which must agree on backend and security. Source-IP support depends on
the provider: EFA/EFA-direct reject source IPs, verbs accepts IPv4 only because
pinned RxM lists `FI_SOCKADDR`/`FI_SOCKADDR_IN` (despite core verbs IPv6
support), and software-tcp accepts IPv4/IPv6 when `fi_getinfo` selects a match.
No source selector means provider-default selection. Metadata reports a
selected IPv4/IPv6 source only when parseable; explicit selection is checked
against the returned address. Generic verbs supports provider-selected
IB/RoCE/Falcon devices; these are not wire-interchangeable, and EFA SRD is
separate. Explicit selection fails closed and does not downgrade to TCP.

Resident FI_READ is a separate explicit choice. Setting
`require_device_read: true` requires native reads plus provider-specific
device evidence. For verbs this includes PCI metadata matched to canonical
sysfs PCI BDF and rejects `rxe`, `rdma_rxe`, and `siw`; it does not establish
Falcon identity. Successful setup alone is not proof. Software-provider FI_READ
tests demonstrate software-path correctness only, not EFA/IB/RoCE/Falcon
hardware operation or performance.
Hardware qualification remains pending until actual device evidence and
parent-reviewed measurements are available. See the [fabric design](../../tidehunter-fabric/DESIGN.md#portable-transport-boundary-27-october-2026)
for implementation symbols and primary OFI, Falcon, and native-paper sources.

This structured-transport cutover uses wire protocol 11. The build plan records
it; external deployment and replay results copy the frozen build manifest's
protocol rather than inventing a new value. Rebuild all participants and
regenerate configurations together. Old string-transport deployments cannot
mix with these endpoints. Local transport checks and their hardware limits are
recorded in the [27 October qualification](../../tidehunter-fabric/DESIGN.md#local-qualification-27-october-2026);
no fresh full Sui replay is claimed by that check.

## 23 October local resident-read and comparison controls

The active KV planner now preserves **both** 8-KiB/batch32 working sets:
`load320` (10,240 keys, 80 MiB payload) and `load3200` (102,400 keys,
800 MiB payload). They are distinct case namespaces, never a matched
source-regression denominator. The targeted scope adds the smaller historical
control and a 256-byte/batch32 control; the full paid matrix is not enlarged.
Historical evidence and generated reports remain unchanged.

`aws-sharded-wal/checkpoint_kv.py` exposes offline-only commands:

```sh
python3 experiments/aws-sharded-wal/checkpoint_kv.py plan --config CONTROLLER_CONFIG.json
python3 experiments/aws-sharded-wal/checkpoint_kv.py identity RESULTS/CASE-tcp --config CONTROLLER_CONFIG.json --spec CASE_SPEC.json --mode tcp > CASE.identity.json
python3 experiments/aws-sharded-wal/checkpoint_kv.py compare OLD.identity.json NEW.identity.json --compared implementation
python3 experiments/aws-sharded-wal/checkpoint_kv.py compare TCP.identity.json FABRIC.identity.json --compared implementation transport
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence RESULTS/CASE-tcp --mode tcp
```

`plan` prints the same prepare/native/remote argv used by the existing
source-bound guest pipeline, but marks them planned, not observed. `identity`
requires successful exact command/binary receipts, source-archive provenance,
the observed load/read summary, query algorithm/order/seed, layout, preparation,
budgets and polling policy, compilation features, OS/architecture, admitted
region/AZ/instance/cluster profile, and observed coordinator CPU/NUMA allowed
masks. Allowed masks are not proof of per-thread pinned cores. Existing closed
campaign placement/provider verification remains necessary; the offline command
does not make a fresh hardware query. Missing older metadata remains unqualified.
`compare` exits 2 and forbids ratios for unqualified or mismatched controls.
Implementation/source digests are retained as provenance, not required equal
when implementation is the named A/B axis. Transport changes must also be
named explicitly; they do not silently relax workload/cache/policy matching.

Clean runs never enable detailed scalar/reopen/cold diagnostic clocks.
Clean advisory-eviction evidence uses the original counter/residency/physical-I/O
brackets; the obsolete separately profiled cold-certificate helper and its
tests are removed. Historical raw evidence and generated reports are preserved.
A fresh diagnostic twin replaces its scalar phase with profiled calls using the
same complete random oracle, key order, cardinality and initial prepared state;
it no longer samples a warmed sequential 256-key prefix. Diagnostic rows remain
separate from clean latency. Grouped reads expose widths 4 and 32 independently.
The targeted shard-cache-on cases explicitly fill a 256-key hot set with
coordinator caching disabled. `cache_hot_before_scan` is hit-only only when its
actual owner/frontend counter deltas prove it; enlarged uniform reads are not
assumed resident or cache hits.

Local software-provider controls use canonical transport selection, for
example `{"kind":"ofi","fabric":{"backend":"software-tcp"},"security":"trusted"}`.
Registered reads remain explicitly bounded by `resident_reads` and disabled
with all-zero limits. Configure both owners and client before launch;
existing external configurations reject CLI server-control overrides.
`resident_reads.jsonl` retains actual grant acquisition, posts, successful CQ
completions, verified hits, errors/fallback reasons and occupancy snapshots
at each read boundary, with coordinator-local/per-owner-connection scope.
It does not claim to sample remote source occupancy.

For raw FI_READ measurement, provide a `FabricConfig` JSON and explicit device
read policy:

```sh
sharded_wal_transport_bench --rma-probe --role server --listen 127.0.0.1:PORT --fabric-config fabric.json --require-device-read false
sharded_wal_transport_bench --rma-probe --role client --connect 127.0.0.1:PORT --fabric-config fabric.json --require-device-read false
```

The probe requires both `--fabric-config` and `--require-device-read`; set the
latter to `true` to require proven native device reads, not merely request
them. Output includes observed provider setup metadata and capability evidence.
A successful software-provider FI_READ comparison qualifies software
correctness only. Configure the same provider/security on both peers, with
host-specific `domain` and `source_address` selectors as needed. Hardware
qualification remains pending until parent-reviewed results from actual
devices.

For an explicit trusted-native resident experiment, `kv_suite.resident_reads`
accepts the complete limits object, without adding cases to either matrix:

```json
{
  "source_bytes": 4194304,
  "source_regions": 512,
  "destination_bytes": 20971520,
  "destination_slots": 640,
  "max_read_bytes": 32768,
  "max_handles": 4096,
  "require_device_read": false
}
```

This opt-in applies only to explicitly trusted-native cases. Existing
historical TCP/EFA/EFA-direct/EFA-native matrix identities remain unchanged.
Prepared owner and client YAML carry the exact limits, the case name binds
their digest, and cold cases deliberately omit them. Hardware qualification
must set `require_device_read: true` and retain actual provider/device evidence;
software-provider success is not hardware proof.

After the integrated source-bound build, run these paired loopback controls
with the **new bundle's** executable, not an older `target/release` binary.
Both use the same 32-key control before the legacy full-oracle workloads:

```sh
BUILD_ROOT=/path/to/new/source-bound/build
"$BUILD_ROOT/binaries/storage-node" --output logs/resident-native --backend native-sync --transport fabric-tcp-native --values 8192 --batch-sizes 32 --windows 1 --batches 320 --mixed 64 --reads 512 --read-window 32 --tcp-read-lanes 4 --native-read-cache-bytes 0 --tcp-read-cache-bytes 0 --tcp-shard-cache-bytes 0 --hot-read-pass true --read-resource-evidence true --bounded-hot-pass true
"$BUILD_ROOT/binaries/storage-node" --output logs/resident-local --backend tcp --transport fabric-tcp-native --values 8192 --batch-sizes 32 --windows 1 --batches 320 --mixed 64 --reads 512 --read-window 32 --tcp-read-lanes 4 --native-read-cache-bytes 0 --tcp-read-cache-bytes 0 --tcp-shard-cache-bytes 0 --hot-read-pass true --read-resource-evidence true --bounded-hot-pass true --rma-source-bytes 4194304 --rma-source-regions 512 --rma-destination-bytes 20971520 --rma-destination-slots 640 --rma-max-read-bytes 32768 --rma-max-handles 4096 --rma-require-device-read false
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence logs/resident-native --mode native-sync
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence logs/resident-local --mode tcp
```

`--bounded-hot-pass` is off by default and automatic when RMA is enabled.
Native receives no RMA arguments and invents no RMA counters. Inspect
`bounded_hot.service_qualified` on both sides and
`resident_reads.registered_controls.qualified` on the remote side. Native
qualification requires actual zero accounted reads/major faults with observed
WAL residency and its value cache disabled. Software fabric proves correctness,
not EFA latency. Replace `--batches 320` with `3200` on **both** sides for the
larger working set; never combine the two denominators.

Source-bound pipeline identities support an explicit bounded-only comparison:
`compare NATIVE.identity.json REMOTE.identity.json --population bounded-hot
--compared implementation transport`. The ordinary matcher remains strict.
Standalone CLI population evidence does not invent missing campaign provenance.

The destination budget is a coordinator-wide total partitioned across read
lanes, not multiplied per connection. At 32-KiB maximum reads, 640 slots and
20 MiB permit a width-32 result group on each of up to 20 lanes. Smaller totals
remain legitimate pressure controls but can force grouped-read RPC fallback;
they must not be presented as a supported resident grouped-hit population.
The 4,096-handle total also leaves room for session-scoped references across
read lanes; a fitting value set alone does not prove that the handle cache fits.
The control uses exactly the first 32 loaded keys, independent of the
320/3200-batch oracle, immediately after load and before other read populations.
The deterministic scalar sequence is `mix64(sample) % 32`, with
`min(512, max(32, reads))` calls; each grouped call uses all 32 distinct keys,
with `ceil(scalar_calls / 32)` calls. Native, RPC and RMA run the same
`2 * read_lanes + 1` complete scalar-then-group warm passes. RMA additionally
requires `read_lanes` consecutive fully registered-hit passes after every lane
of each active hot-set owner has verified hits; it does not stop preparation
early. `bounded_hot_setup` is the common CSV setup phase.
`bounded_hot_controls.jsonl` retains the shared policy and verified populations;
`registered_hot_setup` remains the RMA-specific preparation record inside
`registered_controls.jsonl`. Acquisition and setup time are not hit latency.

`registered_controls.jsonl` retains raw before/after lane snapshots, every
preparation pass, the key/order/cardinality policy and both measured populations.
For a registered experiment, only fully verified, prepared populations with
zero Read/ReadProfiled messages, new grant acquisitions, fallbacks or errors
may retain `cache_hot_get` / `cache_hot_get_many` rather than `*_mixed` labels.
Accepted FI_READ posts and successful completions must equal **every** intended returned element;
eligible reads and verified hits count complete owner groups, not elements.
A 32-member group can therefore have one group hit and 32 posts/completions.
Group hits must equal grouped calls times the actual hot-set owner count;
scalar hits must equal scalar calls. Positive partial populations cannot pass.
The consumer recomputes lane coverage and deltas, checks per-owner aggregates,
and binds the CSV sample/operation counts. Mixed or invalid populations use
`*_mixed` timing labels and cannot qualify the registered probe, even if some
RMA hits occurred. Successful RPC fallback with all-zero RMA counters is not
qualification. Inspect `resident_reads.registered_controls.qualified` in the
`evidence` output; ordinary process success is insufficient.

The original full-oracle `point_get` / `point_get_hot` and grouped populations
retain their original counts/order. They are **not** relabeled registered-hit-only
because the small control qualified. No server-handler clocks apply to a
registered hit, and neither software-provider results nor these API timings
establish hardware-only or physical-NVMe latency. Active RMA identities require
the observed preparation policy and qualified raw controls. Older binaries
without that policy remain unqualified unless their observed, source-bound
all-zero resident configuration proves this preparation did not run; that
inactive state is matched without inventing unused historical hot-set metadata.

Use fresh output roots for each native/TCP/software-fabric/cache-off/cache-on
control. Keep the same value shape, cardinality, read count/order and
preparation controls for intentional comparisons; label the transport/RMA axis.
The example requires Linux `test-utils` and `experimental-fabric`; use the same
libfabric ABI as the build. Detailed clocks remain off unless explicitly enabled.

`read_path_evidence.csv` additionally records nullable authority-cache counters:
resident hits/misses, contention, history loads, entries/capacity and charged
table bytes. Each owner retains its before/after observations; absent older
fields or unavailable samples remain null. Authority-history loads can be
served by page cache and never establish physical NVMe I/O.

For a local replay select the canonical engine transport with `--transport-config JSON_FILE`.
TCP is the default. Local hardware OFI is rejected; local runs permit TCP or
the software provider for correctness testing only. The transport configuration
must be present in a fabric-enabled build. Use external deployment configuration
for hardware providers; software-fabric results are not EFA or verbs hardware
latency.

Standalone `sharded_wal_bench` uses the same structured `Transport` file:

```sh
sharded_wal_bench --backend tcp --transport-config transport.json
```

Example benchmarks and `kv-research` have different selection surfaces:
`kv-research` uses backend kind `tidehunter` and the
`backend.options.transport` object in its run JSON.

Sui summaries additionally retain `shard_read_by_owner`, parsed from
`tidehunter_shard_read{namespace,partition,stat}`. Missing families on older
binaries or failed `shard_sample_available` observations are unavailable,
never zero. A foreground-only snapshot is cumulative, not an invented delta;
the strict before/after consumer preserves each owner and rejects resets or
population drift. Application-cache hits, resident/page-cache service, async
submissions and positive process physical-I/O brackets remain distinct.
Advisory eviction and async submission alone never prove physical NVMe reads,
and process-I/O brackets never establish one device read per key.

Parent integration checks (not run by the implementation worker):

```sh
(cd experiments/aws-sharded-wal && python3 -m unittest test_matched_reads test_cloud_profile_populations test_checkpoint_matrix test_local_safety)
(cd experiments/sui-checkpoint && python3 -m unittest test_shard_read_evidence test_analysis)
```

Run the existing Rust example builds/checks and bounded native/TCP/software-
fabric controls after integration. Require actual nonempty owner populations,
separate registered acquisition/hit/fallback populations, successful value and
reopen audits, and counter-qualified cache/physical-I/O classifications.
The existing deadline, export reserve, process ownership and unconditional
cleanup/termination semantics are unchanged.


## 16 October targeted metadata and residency experiment

The integrated build now includes six source-bound executables: native and
remote Sui nodes, the storage benchmark/server, both state auditors, and
`transport-floor` (`sharded_wal_transport_bench`). All six are rebuilt by Cargo
from the same frozen sources; old five-executable bundles are not admitted.

`CheckpointStore::get_checkpoint_by_sequence_number` uses a separate demand-read
cache bounded by 1 MiB of canonical serialized bytes, without a separate row cap.
Native and remote Sui use the same policy; the producer's eight-entry digest/control
window is separate. Hits retain storage failure fencing and same-key mutations
invalidate delayed fills. The exact Sui and engine changes are mapped in
[the local cache and checkpoint cutover](../../docs/distributed/HOT_READS_AND_CHECKPOINT_ADMISSION_2026-10-16.md).
Typed-store `certified_checkpoints` histograms observe backend calls only, not
sequence-cache hits. Fewer backend observations are not end-to-end sequence-lookup
latency or an unchanged request population.

The cloud supervisor accepts optional `coordinator_cache_mib_by_repetition` and
`shard_cache_mib_by_repetition` arrays, with one integer 0..128 per repetition.
Each effective budget is bound into the cell receipt, generated deployment and
replay evidence. An omitted schedule uses the corresponding global budget.
Keep clean repetitions separate from timing-profiled repetitions; compare cache
variants within the same source, hardware and transport.
The coordinator budget applies per logical database, shared across its CFs.
The shard budget applies per database partition, not per storage host. In the
three-DB/five-partition campaign, coordinator 8 MiB plus shard 16 MiB means
24 MiB coordinator + 240 MiB storage value-cache capacity, excluding metadata,
execution caches, in-flight buffers and OS page cache. It is not an RSS cap.

Use `kv_suite: {"scope": "targeted", "repetitions": 1, "diagnostics": true}`
for ten clean paired cases per requested transport: 256-byte and 8-KiB scalar reads
with coordinator cache off and shard cache off/on, an 8-KiB coordinator-hit
control, bulk preparation chunks of 16 versus 32, a bulk shard-cache pressure
case, a cache-evicted bulk case, and a 32-batch preparation control with Sui-style
WAL geometry. Bulk cases use 3,200 batches (100 full durable windows) and 8,192
read keys, exceeding the five 8-MiB shard caches' aggregate payload allowance.
The Sui-style control uses 16-MiB fragments, 256-MiB files and a 12-MiB group-byte
limit, instead of 1-MiB fragments, 128-MiB files and a 1-MiB-minus-eight-byte group
limit. It retains the raw benchmark's 32-batch group limit, not real Sui's 64;
it is not a complete Sui configuration. The benchmark explicitly bounds load
batches at 4,096 and checked value history at 1 GiB; defaults remain unchanged.
Native durability windows still synchronize all registered Value WAL files,
whereas owners synchronize the exact containing group file. Resource rows record
the native registered-file count outside timed operations; larger histories can
therefore expose extra conservative native barrier work. Do not call these
identical persistence implementations.
With diagnostics enabled, one additional fresh 8-KiB scalar pair per transport
collects stage clocks. It must not be pooled with the ten clean pairs. The first
256-byte clean case also runs the rebuilt authenticated
RAM-response executable with 256-byte/8-KiB payloads and no WAL member reads
during that measurement. Clean and profiled RAM processes are separate. This
floor has no invented native-storage comparator and is not physical wire RTT.
The default `full` scope retains the historical KV matrix.

Cache hits are not durability acknowledgements. No experiment may remove exact
reference checks, client integrity verification, valid-cut checks, durable-before-
publication barriers, or buffer/file lifetime pins. Shard residency avoids file
submission only after authoritative read planning; it is not one-sided RDMA.
RF1 remains unable to survive loss of an owner NVMe device.

The **completed 16 October hardware campaign** and its correctness/security
assessment are recorded in
[the targeted experiment report](../../../checkpoint-work/2026-10-16-targeted-metadata-residency/REPORT.md#completed-hardware-campaign).
It completed eight clean paired Sui cells and 22 paired KV cases, with diagnostic
cases separate from clean latency populations. The six `g6.8xlarge` instances
are terminated; the campaign bucket, volumes and temporary AWS resources are
removed. Each storage instance used its own instance-store mount, but shared
tenancy and cluster placement do not prove dedicated physical-host separation.
Keep these results separate from the older campaigns below.

The completed 13 October native/TCP/EFA matrix is recorded in
[the measured results and closure report](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#20-completed-nativetcpefa-real-checkpoint-matrix-13-october-2026).
All four paired real-checkpoint runs passed full-state audits; both EFA cells
proved hardware traffic on all six hosts. This is a private generated-transfer
chain, not mainnet or a cold-NVMe capacity workload. All instances and temporary
AWS resources are closed. The earlier TCP-only result remains in §19.
Only a run with `receipt.json.complete=true`, a verified evidence archive, and
a complete equal `experiment/audit.json` qualifies. No script in this directory
calls AWS, launches instances, commits, pushes, or deletes old results.

The **15 October local implementation and measurements** are in
[the current source map, correctness rules and qualified results](../../docs/distributed/CONCURRENT_AUTHORITY_AND_TRANSPORT_2026-10-13.md#15-october-local-lifecycle-and-serving-path-implementation).
They cover pooled immutable index containers, background checkpointing, pageable
retry authority, pinned whole-file retirement, single-flight and actual Sui page
prefetch. All five updated local native/TCP/software-fabric pairs passed state
audits. Software fabric is not hardware EFA; local ratios do not establish a
cluster speedup. No AWS run is authorized by these local scripts.

## Available real fixture and historical artifacts

The retained private frozen chain has a portable active copy at:

```
checkpoint-fixtures/sui-genesis-1775/fixture
```

Observed allocated size: **368 MiB**. It contains `producer`, `producer.yaml`,
`native.yaml`, `distributed.yaml`, and `experiment/results.json`. Genesis is
embedded in each YAML's `genesis.genesis`, not a missing external blob. The
producer DB is `producer/full_node_db/8a110acee50f`.
The measured native/distributed leaf identities are respectively
`b7ac33ddb105` and `94970e38de08`; do not substitute the producer's leaf in an
external remote configuration.

Its recorded successful range is **0..1775 inclusive**, 1,776 checkpoints, with
4,096 user transactions. The frozen tip is
`EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`; the complete ordered checkpoint
summary digest-list SHA-256 is
`896408bc8581e07473e11db805a69295286df17e05cfb90ed0952c45a9286f9f`.
The runner rechecks every certified summary through RPC before and after replay,
and the actual full replay plus audit, rather than summary availability alone,
proves the fixture is usable.

Reuse the existing `sui/target` as Cargo's build cache. Historical middleware
binaries and evidence remain in `archive/middleware-legacy/results-local/`.
They are not current candidate executables: the old distributed facade is never
accepted by this runner as the new backend. No archived source, tool or binary
is needed for a new build.

For a portable controller upload, `fixture.py --fixture ORIGINAL --output NEW`
writes `fixture.tar.gz` plus its SHA-256 receipt. It includes the complete
producer, embedded-genesis configs and range evidence, rebases DB paths to
relative paths, and hashes every input. Extract into an owned directory before
running; both original absolute-path and exported relative-path fixtures work.
The runner hashes/checks the complete producer copy before opening it.

RPC/configuration helpers and strict local RocksDB audits are maintained in
`checkpoint_support.py`. The engine source snapshot binds their contents;
there is no external helper-directory argument or import from middleware.

## Freeze and build

Requires Linux, Python 3 with PyYAML, OpenSSL, Rust/Cargo and the Sui build
prerequisites; fabric builds additionally require the existing libfabric
installation. Paths below are examples from the repository root. Use fresh
output directories outside either source checkout and retain failed outputs.

```sh
python3 tidehunter-sharded-wal/experiments/sui-checkpoint/build.py prepare \
  --sui sui --engine tidehunter-sharded-wal \
  --output /absolute/owned/checkpoint-build --jobs 2

python3 tidehunter-sharded-wal/experiments/sui-checkpoint/build.py build \
  --output /absolute/owned/checkpoint-build \
  --target /absolute/repository/sui/target --deadline 10800
```

Add `--fabric` to `prepare` for `sharded-wal-fabric`/`experimental-fabric` builds.
Preparation snapshots the exact tracked and nonignored untracked working files,
including dirty edits and tracked deletions. It refuses escaping symlinks and
source mutation during copying. Cargo metadata resolves the new local dependency
in **owned copies**, retaining existing pins where possible. Resolved lockfiles
are archived before the build. Upstream checkouts are not modified.

Builds run serially with bounded jobs/deadlines, `USE_TIDEHUNTER=1`, `--locked`,
and `GIT_REVISION` set to the frozen Sui revision (Git metadata is not copied).
The native Sui node and RocksDB auditor both enable `typed-store/tidehunter`;
the remote node enables `sui-node/sharded-wal` (or `-fabric`). The storage
executable is the existing
`sharded_wal_bench` executable's real `--experimental-storage-node` mode; no
benchmark workload runs in that mode. The new standalone `state-audit` uses the
same adapter and the same engine sources. The existing strict
`distributed_storage_local_audit` example is rebuilt from the same Sui sources.

`source-manifest.json`, `sources.tar.gz`, the exact commands/log hashes,
`build-manifest.json`, resolved lockfiles and executable hashes bind every
result to its real inputs. The build refuses source changes before/after Cargo.
There is no “attach a new manifest to an old binary” command. Cargo may reuse its
valid target cache, but each accepted executable comes from that invocation's
`compiler-artifact.executable` JSON message, never an assumed `target/release`
path. The manifest records each invocation's workspace-specific `cargo -vV`,
explicitly selected `rustc -vV`, `GIT_REVISION`, artifact path and JSON/log hashes.
Frozen Python imports disable bytecode writes so they cannot mutate the source
inventory. `build-bundle.tar.gz` includes source archive, binaries and build
evidence; `build-receipt.json` binds the bundle.
`portability.json` records `readelf --version-info` and `ldd` for each executable.
Before uploading to an existing Ubuntu/apt guest, compare its glibc/libstdc++ and
libfabric against those requirements; do not assume AL2023 or portability from
the laptop. Dynamic system libraries are not silently substituted or bundled.

## Paired local replay

Run the **frozen** runner (its own source hashes must match the build manifest):

```sh
python3 /absolute/owned/checkpoint-build/sources/tidehunter-sharded-wal/experiments/sui-checkpoint/run.py \
  --build /absolute/owned/checkpoint-build \
  --fixture /absolute/repository/checkpoint-fixtures/sui-genesis-1775/fixture \
  --output /absolute/owned/checkpoint-run \
  --deadline 900 --port 24000 --coordinator-cache-mib 8 --shard-cache-mib 0

python3 /absolute/owned/checkpoint-build/sources/tidehunter-sharded-wal/experiments/sui-checkpoint/analyze.py \
  /absolute/owned/checkpoint-run
```

A fixture lock prevents concurrent use of its private ports. The producer opens
an owned **copy** of the saved producer DB; neither replay writes the original
producer. Both measured DB roots are new. Producer and measured configs are
state-sync-only fullnodes bounded at tip+1, so they stay alive for final metrics
at the frozen tip. Their certified completed tip must equal the producer.
The producer alone receives `source_environment(prior)`: its original fragment
size and durability policy, with historical defaults of 32 MiB and asynchronous
writes. Measured durable settings must never reinterpret the producer's WAL.
The native control runs first because it exports the authoritative complete
namespace/key-shape inventory. This fixed order is recorded, not randomized.

For every native-exported database the runner allocates a fresh UUID and starts
five independent physical shard processes, each with its own WAL root, bounded
read workers and port. TLS modes also assign each node its own authenticated
identity; explicit trusted-native modes use no certificate files. There is one local
coordinator publication/index authority per DB, not a global WAL or NVMe-oF
layer. Loopback is **five physical shards per selected DB**, not five processes
shared across unrelated DB identities. It is one laptop SSD/RAM pool, not a
multi-NVMe or RDMA performance claim.

Use `--local-transport fabric-tcp` (TLS) or `--local-transport fabric-tcp-native`
(explicit trusted, no TLS) with a `--fabric` build for a complete paired replay
through libfabric's loopback software provider. TCP/TLS remains the default.
Both software-fabric modes reject non-loopback hosts and external guest roots;
they validate correctness, not EFA hardware or RDMA speed.
The runtime must find the same libfabric ABI used for the build.

`TH_SYNC_WRITES=true` requests a real durable barrier for each native write/batch;
remote whole-batch publication follows durable shard acknowledgments. Native
fragment size is 8 MiB, mapped windows three (perpetual multiplies that in Sui),
index maps three. Shard fragments are 16 MiB to contain the bounded 12 MiB group
and framing; maximum encoded batch is 8 MiB. Remote pending memory is 128 MiB,
including recovery admission for one bounded batch plus metadata per shard.
The pending window is 128 batches. Completed node metadata now spills from a
resident window capped at 1,024 batches into authenticated, pageable history.
The coordinator checkpoints before its 1,024-batch / 64-MiB hard recovery-tail
bound; it does not retain one RAM receipt per lifetime write. Exact retry
identities remain on disk after acknowledgement and permitted payload retirement.
Total historical metadata disk space and retained epoch count are not unbounded
production-retention guarantees. These layout differences are
recorded, not disguised as equal policies. `TOKIO_WORKER_THREADS=4` bounds Sui's
runtime workers. Coordinator/shard derivative caches are explicit 0..128 MiB
per DB/physical shard. Native defaults are bounded to one mutex, 128 dirty keys
and 1,000 value-cache entries per configured keyspace and exported in KeyShape.
For diagnostics, add `--read-profile-limit 4096 --write-profile-limit 128
--native-write-profile-limit 640`. The remote write limit applies to each owning
append connection; the native limit selects foreground durability barriers per DB.
Use a separate fresh run with all three limits zero for clean comparisons.
Do not pool runs with different read, remote-write or native-write budgets.
OS page cache is neither flushed nor claimed cold.
Analysis retains all `rocksdb_` wrapper and `db_op_` engine series, including
epoch/perpetual labels without the word `checkpoint`. Wrapper means use
histogram sum/count and are not p50 or disjoint wall-clock stages. The local RSS
summary includes local shard processes; externally hosted shard RSS is separate.

Node startup has a 30-second deadline, RPC startup a five-minute bound,
foreground replay defaults to 900 seconds, committed RPC-index catchup is
limited to 120 seconds and each audit command to 600 seconds. At most 16 DBs
are admitted, hence at most 80 shard processes. The runner samples only owned
PIDs and stops only its own process groups (TERM, then bounded KILL), including
audit command descendants on timeout or signal. Existing external servers are
never killed by the local runner. Interruptions preserve outputs and emit
failure evidence; they never become completed measurements. Fresh roots are
mandatory, never automatically removed or resumed.
Owned subprocess `poll()`/`wait()` use Linux `waitid(WNOWAIT)`, including legacy
RPC-readiness observations. The leader remains unreaped until the last group
signal, reserving its PID/PGID so cleanup cannot hit a recycled identity.
Cargo and successful audit commands also clean remaining descendants before
reaping; an unexpectedly externally reaped leader fails ownership checks rather
than authorizing a signal to an unowned process group.

## Namespace and audit contract

The remote node receives a JSON envelope with an absolute `root` and a
`databases` map from exact relative namespaces to complete
`RemoteDatabaseConfig` values. `deployment.py` writes the full engine serde
structure from the native-exported `shape.json`; no hand-written partial
configuration is needed. Names are **exact paths relative to root**, never
basename guesses. `SUI_SHARDED_WAL_SCHEMA_OUTPUT` exports
`<namespace>/{shape.json,shape.yaml,manifest.json}`. `shape.yaml` is the engine's
canonical serde-YAML-0.9 string, preserved byte-for-byte in node configuration
(Python YAML reconstruction would lose enum tags/identity). Native export uses
`SUI_SHARDED_WAL_ROOT`; remote selection uses `SUI_SHARDED_WAL_CONFIG`.
Feature-disabled native nodes reject remote configs, and sharded-feature nodes
require one. Every exported remote manifest must say `sharded-wal`.

After foreground completion, all ten RPC index pipelines must commit at the
frozen tip. Only then does the measured Sui node stop. The full audit reopens
NativeDb using its **persisted config** and reopens the remote authority while
its shards remain live; this exercises fencing/recovery and authenticated
exact-version value reads. Every live keyspace and every row is visited, with
counts, key/value byte totals and SHA-256 over sorted length-delimited row
SHA-256s, retaining multiplicity. Errors are fatal, never missing values.
This includes checkpoint, epoch, perpetual, object, marker and shared-version
tables, including zero-row tables. Full per-table counts appear in the summary.
It is a non-adversarial exact-state comparison, not a Byzantine proof.

All local RocksDB databases beneath `live` are separately inventoried. The
strict existing audit uses `AbsoluteConsistency` WAL recovery, requires the
committee `epochs` store and identical coverage, and compares every raw or
validated semantic row. Only legacy indexes permit the pre-existing validated
sequence/digest bijection normalization. No table is silently left unaudited.
No old middleware distribution/replication/drain code is used.

The adapter accepts an immutable declared schema, complete durable batches and
captured-cut iterator pages (256 references, including full hash-key enumeration).
Point and multi-get waits do not hold the coordinator mutex. Pruning durably
publishes tombstones, preserves historical Stop whole batches and Keep anchors,
and installs a pin-clamped supported-history floor. After that root is durable,
it can reclaim wholly unreachable files from **sealed epochs**. Active-epoch
files, live-value relocation and general epoch-directory destruction are not
implemented. Old retry outcomes survive payload retirement; unsupported old raw
cuts fail explicitly. A whole cell-range removal remains one atomic batch;
exceeding its batch limits fails before writes. Dropping only a coordinator DB,
Rocks range compaction/range tombstones, directory-copy checkpoints and secondary
mode remain Unsupported/errors. No HA, failover or unbounded-epoch claim is made.

## Five existing storage hosts / controller integration

After a source-identical local run has exported native shapes, prepare a fresh
external namespace (no hosts are launched):

```sh
python3 /absolute/owned/checkpoint-build/sources/tidehunter-sharded-wal/experiments/sui-checkpoint/prepare_external.py \
  --build /absolute/owned/checkpoint-build \
  --schema /absolute/owned/checkpoint-run/experiment/native-schema \
  --output /absolute/owned/external-config \
  --db-root /absolute/owned/cloud-run/remote/full_node_db/94970e38de08 \
  --guest-root /absolute/owned/unique-storage-run \
  --hosts 10.0.1.10,10.0.1.11,10.0.1.12,10.0.1.13,10.0.1.14 \
  --transport efa --coordinator-cache-mib 8 --shard-cache-mib 0
```

`external-manifest.json` contains every launch argv, ready/stop path, binary,
source/config hash and per-host payload hash. The existing parent controller
creates the fresh root on each of the **five distinct hosts**, distributes only
that host's archive, starts the listed bounded process groups, waits for its
ready files and retains PID ownership. TLS modes distribute only that host's leaf
key and public CA, never the coordinator identity or CA signing key; trusted-native
payloads contain no certificates. The controller then invokes
the paired runner with `--external-config .../config.json`,
`--external-resources .../resources.json`, and
`--external-stage-dir .../stages` (all three are required together).
Endpoint identity/config/cache/source bindings must match the exact manifest.
External transport supports `tcp`, `efa`, `efa-direct` and explicit trusted
`efa-native`. Both software-provider modes are rejected because the native
engine deliberately pins them to loopback.

The runner atomically publishes `stage.json` and durable `<stage>.json` markers
for `remote-starting` before remote execution, `remote-finished` after its node
stops, and `resources-required` only after full state audits and source checks.
Each contains `stage`, `unix_ns`, `config_sha256`, `build_manifest_sha256` and
`checkpoint_digest`. Shards must remain alive through `resources-required`.
The controller then stops only its owned processes, publishes final resources,
and atomically writes `stages/resources-ready` JSON containing the same three
bindings and a `unix_ns` no earlier than `resources-required`. The runner waits
at most 120 seconds for this marker before reading resources.

The resource JSON contains those same three bindings and `hosts`, keyed by the
exact five IPs. Each host binds the storage binary, guest root and exact command
inventory to owned PID/process-group/start-time identities; final `complete`,
`cleanup_complete`, `all_namespaces_healthy` must be true and `failure` absent.
Nonempty healthy `samples` must bracket remote execution with all namespace
PIDs covered, numeric `VmRSS`, `VmHWM`, `VmSwap` and all seven `/proc/PID/io`
counters; unavailable values and dropped samples fail qualification. Exited
samples after shutdown are not misrepresented as live zero-valued observations.
`swap_before/after` require numeric `pswpin/pswpout`; `diskstats_before/after`
must contain real nonempty numeric device rows. Missing evidence, placeholders
or binding mismatches fail completion. This is a controller contract, not AWS
permission or a renewed cloud budget.

The AWS supervisor in `../aws-sharded-wal/sui_checkpoint.py` validates full
state and evidence integrity with `analyze.py --verify-only`; it does not derive
performance comparisons on paid instances. It exports the raw result archives
and receipts to S3, then requests termination of the exact six owned instances.
The original absolute shutdown timers remain a separate backstop. Retrieve and
verify the exported files, confirm all run-owned instances are **terminated**,
and only then run the performance analyzer locally. Teardown never waits for
performance analysis.

The bootstrap also allows `g5.8xlarge` as an EFA-capable capacity alternative:
it requires exactly one blank instance-storage NVMe device, not the two devices
expected on `g6.8xlarge`. All six hosts in a comparison must use the same type;
hardware EFA and dependency admission still apply. Recheck regional capacity,
quota, price and the absolute lifetime budget before launch. A G5/G6 change
prevents attributing a cross-run speed difference solely to code; use each
cell's matched native control and disclose the hardware change.

### Atomic six-host startup (no human SSM call)

Use `../aws-sharded-wal/control.py:user_data(plan)` with
`plan["atomic_controller"] = True`. The common user data embeds the tracked
`atomic_start.py`; it does not depend on a deployment-directory autostart file.
It arms the existing shutdown backstop before installing anything, and retains
the bootstrap deadline derived from **EC2 pendingTime + ttl_seconds**, never
startup time. Other campaigns keep their existing bootstrap behavior.

The bootstrap source, dispatcher source and immutable plan share one
zlib-compressed, Base85-encoded capsule. `user_data()` returns an ordinary ASCII
shell script and rejects a payload larger than EC2's 16 KiB limit. Pass this
string directly as `RunInstances.UserData`: boto3 performs API base64 encoding.
Do not base64-encode it again. The size check uses the real artifact hashes;
repetitive placeholder hashes are not a representative deployment payload.

The launcher must perform the following in **one autonomous MCP script**, not
manual turns after `RunInstances`:

1. Launch exactly six same-type hosts together using the common storage-0 profile,
   `Role=bootstrap`, IMDSv2 required, and `InstanceMetadataTags=enabled`. Use the
   immutable `atomic_client_token` (default `<run-id>-atomic`) on this request.
2. Assign all six exact profiles and existing scoped instance policies, then set
   final role tags `storage-0` through `storage-4` and `controller`.
3. Fill the exact controller and storage IDs/private IPs into the existing
   supervisor config, publish its bytes to
   `<prefix>/inputs/controller-config.json`, and set **only the controller's**
   `ControllerConfigSha256` tag to SHA-256 of those exact bytes.

The bootstrap archive remains `<prefix>/input/bundle.tar.gz`, with
`bundle_sha256` and `bundle_bytes` pinned in the launch plan. Its files live under
`runner/`, including `atomic_start.py` and the unchanged `sui_checkpoint.py`.
The launch plan also supplies `account`, `region`, `run_id`, `bucket`, `prefix`,
`ttl_seconds`, `instance_type`, `placement_group`, `az`,
`export_reserve_seconds`, `protected_instances`, and `atomic_bindings`.
The last is a separate dictionary containing the frozen supervisor config's
`build_bundle`, `fixture_bundle`, `verification_bundle`,
`build_manifest_sha256`, `source_archive_sha256`, `source_manifest_sha256`,
`binaries`, `transports`, `repetitions`, `read_profile_limits`, `write_profile_limits`,
`native_write_profile_limits`, `checkpoint_digest`, and `checkpoint_limit`.
Do not add deployment options to
the benchmark configuration or rewrite its instance identities on the guest.

Every deployed `runner/*.py` must match its entry in the build's frozen
`source-manifest.json`. Freeze and rebuild after changing a startup helper,
then rerun the same-build local checkpoint and recreate its verification bundle.
Rebuilding from an older source archive does not include new lifecycle files.
Before provisioning, exercise the real `SuiCampaign.inputs()` method with the
exact deployment archives. A startup simulation that replaces the controller
subprocess does not exercise this check. On 14 October, this gate rejected an
older build paired with the new `atomic_start.py`; the source-consistent build
passed it and the full local state audit. This local gate does not establish
live IAM/SSM readiness or hardware EFA operation.

Each guest waits for the final Role tag, matching IMDS credential-role name
`<run-id>-<Role>`, and matching instance-profile ARN **before fetching any startup
credential material or constructing a fresh SDK session**. STS then verifies the
exact account, assumed role and EC2 instance session. Credential expiry must
cover the original bootstrap deadline. The managed-node agent is restarted
after profile convergence; no shutdown timer is restarted. Only then can the
guest verify/extract the bootstrap archive and publish `bundle-ready` plus its
role/instance/deadline-bound JSON proof. Storage never reads controller config
or starts a controller.

The controller verifies the config hash, immutable source/measurement bindings,
its own IMDS identity, and exact owned six-host inventory. It uses **its own
instance role** for bounded storage SSM readiness and admission commands. These
execute each storage guest's real `verify-ready` action, rechecking final
role/profile, bundle identity, active expiry timer and original deadline.
The unchanged supervisor subsequently performs its full hardware, placement,
filesystem, input, audit, export and teardown gates. Admission waits share at
most 600 seconds and finish before the original deadline minus the export
reserve and 180 seconds; handoff still requires the export reserve plus 600
seconds of campaign time and 30 seconds of final shutdown margin.

Controller status and logs are retained under `<prefix>/results/startup.json`,
`atomic-start.log` and `controller-autostart.log`. Storage status/logs use the
already permitted partition scope
`<prefix>/results/cells/startup/storage/<partition>/`. Before role credentials
are safe to use, errors remain in the guest's `atomic-start.json`,
`atomic-start.log` and cloud-init output; no foreign/bootstrap credentials are
borrowed to publish them. On failure the guest requests immediate self-poweroff
only after rechecking live metadata ownership; an untrusted bootstrap record or
unavailable ownership proof leaves the independently armed timer as the backstop.
Controller teardown discovers targets independently of even hash-valid config,
then rechecks exact account, run tags, launch client token, type, placement and
protected-instance exclusions before termination. Export failures cannot bypass
that request. A malformed config cannot nominate termination targets. The
original six terminate-on-shutdown timers remain the backstop; the parent must
still verify termination and remove the owned auxiliary resources.

Exercise the production `Guest.execute()` entrypoint locally for five storage
guests plus the controller, using fake IMDS/STS/S3/EC2/SSM services:

```sh
cd tidehunter-sharded-wal
PYTHONPATH=experiments/aws-sharded-wal python3 -m unittest -v test_atomic_start
```

The suite executes real role waits, archive extraction, config admission,
storage readiness commands, status/log exports and cleanup paths; it also
checks timeout, foreign identity, malformed/hash-mismatched config, stale ready
proof, unknown SSM send outcome and export/handoff failures. Only OS/AWS service
boundaries and the benchmark subprocess are faked. This is a startup exercise,
not new benchmark evidence. The live guest command is
`python3 /root/sharded-wal-atomic-start.py start --plan /mnt/tidehunter/<run-id>/atomic-plan.json`;
user data calls it automatically. Live profile/tag propagation, managed-node
registration and instance-role policy effectiveness still require cloud
confirmation; no human `ssm:DescribeInstanceInformation` permission is needed.

### Native/TCP/EFA matrix

The cloud supervisor accepts `transports: ["tcp", "efa"]`, `repetitions: 4`,
`read_profile_limits: [0, 4096, 4096, 0]`,
`write_profile_limits: [0, 128, 128, 0]`, and
`native_write_profile_limits: [0, 640, 640, 0]`. Each repetition's limits apply
equally to its two remote transports. Order alternates TCP/EFA and EFA/TCP;
**each cell includes its own native control**, which remains first to export the
authoritative shape. The first and last repetitions measure speed without
opt-in RPC profiling; the middle two collect bounded diagnostic spans.
Each transport therefore has two clean and two diagnostic observations with
both transport orders represented. Report their observed difference, not an
exact causal overhead estimate from so few samples. Always-on adapter timing
and existing scrape diagnostics remain enabled; profiling is not free.

Every cell gets new namespace UUIDs, coordinator/storage roots, systemd units,
config hashes, resource receipts and S3 prefixes. All cells use the same source,
six executable hashes, frozen fixture, per-cell cache budgets and durability settings.
The previous cell's exact units and PID/start-time identities must be stopped
before the next cell. The original fleet deadline is never extended.

Admission verifies six same-type hosts in the named run-owned **cluster**
placement group and exact availability zone. EFA cells require attached EFA
interfaces, the exact hardware `efa` `FI_EP_RDM` provider, and positive
`send_bytes` **and** `recv_bytes` deltas from sysfs devices bound to the `efa`
driver on the controller and all five storage hosts. Raw before/after snapshots
are retained. These counters establish host-wide hardware activity, not pure
wire latency or per-process traffic attribution. Software or TCP fallback
cannot qualify as EFA.

The bootstrap records verbose `fi_info -v` output (`prov_name: efa`); replay
preflight records summary output (`provider: efa`). The shared admission check
accepts both formats but requires every reported provider to be exactly `efa`
and the endpoint type to include `FI_EP_RDM`. This does not replace the six-host
hardware traffic proof or permit software/layered fallback.

#### Explicit EFA-direct comparison

The optional `transports: ["tcp", "efa", "efa-direct"]` matrix alternates the listed
order and its reverse between repetitions. The two-transport default is unchanged.
`prepare_external.py --transport efa-direct` generates separately labeled hardware
deployments. Local `run.py` remains TCP or software-fabric only: it never presents
loopback as EFA-direct. Rebuild all six executables with `--fabric` before use.

`efa-direct` selects fabric **`efa-direct` under provider `efa`**, not provider
`efa-direct` and not the ordinary EFA message protocol. The new path has its own
MTU fragmentation, bounded ordering and peer-consumption credits beneath the
existing authenticated TLS stream. See the
[transport contract](../../tidehunter-fabric/DESIGN.md). It is a two-sided message
transport, not one-sided WAL access or NVMe-oF, and has no production fallback.

`efa-native` uses the same direct message adapter **without software TLS or
application peer authentication**. It is not one-sided RMA or NVMe-oF.
Its `ca`, `cert` and `key` paths must be empty; preparation generates and exports
no certificates for these cells. Network isolation and trusted members are
required. Default TCP and both older EFA modes retain TLS.

The protected full CI configuration now requests
`transports: ["tcp", "efa", "efa-direct", "efa-native"]`, with a separate native
control in every cell. The manual schema's two-transport default is unchanged.
Before paid allocation, CI must rebuild and complete a same-source real-checkpoint
`fabric-tcp-native` local replay and full-state audit. This prerequisite is wired
but is not a claim that a new Sui replay or hardware run occurred locally.

See the [implemented native message contract](../../tidehunter-fabric/DESIGN.md#trusted-native-message-mode-21-october)
and [one-sided WAL design](../../docs/distributed/ONE_SIDED_WAL_READS_2026-10-21.md).

New cell payloads bind `fabric_preflight_transport` in expected evidence. Their
preflight runs `fi_info -p efa -f <fabric> -t FI_EP_RDM`; `efa-native` selects
fabric `efa-direct`, not a fabric named `efa-native`. Every provider/fabric/endpoint
must match. Both direct modes always require exact proof and positive device
traffic on all six hosts. Historical ordinary-EFA receipts are not rewritten.

Analysis emits all available matched pairs among TCP, EFA, EFA-direct and
EFA-native using generic reference/candidate transport fields. Each pair requires matching
source, workload, budgets, profile limits and repetition; every remote cell has
its own native control. Software protocol checks do not establish hardware
availability, EFA-direct latency, or improvement over regular EFA.


The controller role needs `ec2:DescribePlacementGroups` in addition to the
existing exact-run EC2/SSM permissions. Storage partition `i` may read only
`<prefix>/deploy/cells/*/storage-i.tar.gz` and read/write its own
`<prefix>/results/cells/*/storage/i/*` output objects, plus the shared bootstrap
input. It must not receive other partitions' private keys.

The controller archive retains `results/cells/<cell-id>/` with each paired
replay, full audit, cell receipt, resource evidence, raw logs and node archives.
EFA cells also retain provider output and raw counter proofs. Top-level
`campaign-receipt.json` lists every requested cell; `placement-proof.json` binds
the raw placement inventory to the source. Missing cells or failed proofs make
the whole matrix incomplete, while completed and partial cell evidence is still
exported before the exact-owned fleet termination attempt.

## Metrics and interpretation

* `*-foreground.prom` stops at the completed watermark, before RPC-index catchup,
  audit and shutdown. Full final scrapes are separate.
* Completion samples are 250 ms observations, not checkpoint p99/service time.
  Existing Sui stage histograms and execution logs are retained. Execution-start
  spacing is not completion latency; nested spans must not be added as disjoint.
* `execution_cache_db_read_calls{request_type,shape}` counts AuthorityStore
  fallback **API calls** (`scalar`, `empty`, `singleton`, `multi`). Existing
  `execution_cache_requests` counts **keys**. Neither is an RPC count.
  `object_by_version` includes get/exists; cache hits suppress downstream work.
* New execution-cache behavior batches exact-version ObjectStore reads,
  checkpoint unchanged-runtime references and replay shared-version
  initialization through the existing manager. First-use locks and ordering
  remain; receiving/dynamic-child checks are unchanged. No exact-version
  readiness prefetch is claimed: readiness returns no payload and successful DB
  reads do not populate that cache.
* Adapter cache gauges and engine counters are recorded by name, never inferred
  from requests. Missing instrumentation is unavailable, not a fabricated zero.
  Native value/index/page-cache state differs from bounded remote cache tiers.
  `tidehunter_adapter{namespace,stat}` separates calls/keys/submitted read RPCs
  from coordinator cache and authenticated cumulative shard cache/WAL-read
  samples. These diagnostic samples add control RPC work during scrapes.
  `shard_sample_available=0` means unavailable; diagnostics mask stale values to
  null, and qualification fails. WAL bytes and async submissions are not physical NVMe reads.
* Every foreground sample records owned-PID RSS/high-water/swap and physical
  `/proc/PID/io` plus host swap/diskstats/meminfo. Host counters include desktop
  noise; OS read/write charges differ from logical WAL bytes and mapped reads.
* Transport protocol is **9**, incompatible with prior endpoints. The current
  experimental member format is batch v3 with CRC32/IEEE; whole-batch BLAKE2b,
  sealed-file retirement and separate inventory-witness counters remain.
  Response recheck wait/work/deferrals and output queue/submit/retire spans are monotonic
  local residence, may overlap existing timing and are not one-way network,
  hardware RTT or pure NVMe latency.
* Always-on adapter stages expose cumulative `<stage>_ns`, `_calls` and
  `_errors`: `write_gate_wait`, `scalar_read`, `bulk_read`, `write_commit`,
  `read_submit`, `read_admission_wait`, `read_detached_wait` and `read_finish`.
  The old owner-wait fields were removed with the adapter owner mutex.
  Ordinary remote writers hold a shared maintenance gate; maintenance takes it
  exclusively. Native retains the exclusive durability gate.
  `remote_timing_available` distinguishes unsupported remote stages.
  Scalar/bulk/commit totals overlap nested work, not extra elapsed intervals.
  Bulk totals count bounded windows; API counts remain `read_calls/read_keys`.
  Bulk misses use shared per-shard RPCs at one captured cut. Per-key validation
  errors remain per-key; an outer window error is a `bulk_read_errors` event.
  Detached record/scan errors surface at finish, not at the wait boundary.
* `--read-profile-limit N` records `SUI_TH_READ_PROFILE_LIMIT=N` explicitly.
  The runner bounds it to 0..65536; zero disables extra RPC/profile clocks.
  Each DB selects its first N admitted inline/actor requests, including enqueue
  failures and startup/scans. Cache hits, missing keys and admission backpressure
  do not consume the budget. This is a bounded diagnostic population, not random
  workload sampling or checkpoint-only data.
* `--write-profile-limit N` configures bounded first-append diagnostics on the
  actual owning append connections. Zero disables the extra profile clocks and
  control exchanges. Collection uses the existing connection-profile protocol;
  a new diagnostic connection would not observe the append owner's profile.
  Local snapshots feed metrics; scrapes do not issue new write-profile RPCs.
  Captured connection/request counts identify the observed population. An
  unfinished or unavailable sample is not a zero-time append or fsync.
  Profile control exchanges are measurement overhead, so retain clean runs too.
* `--native-write-profile-limit N` selects the first N native foreground
  synchronous barriers per DB; zero adds no profile clocks. Maintenance/audit
  barriers are excluded. File-sync and directory-sync times use their own actual
  attempt counts, including failures; they are not divided by remote Append count.
  Native sync currently covers all registered WAL files, whereas a remote group
  sync covers its exact WAL file. These are different populations and work.
* `storage_append_history` is amortized completed-authority spill time per append,
  not per metadata I/O. Inventory-witness file/namespace times have separate
  call denominators: a new file pays the extra witness barrier once, not on each
  subsequent group. Never add these nested spans into a wall-time speedup.
* `write_storage_wait_ns` measures the write owner's completion-notification
  waiting, not storage persistence. Append, file/namespace synchronization,
  checkpoint work and durable-but-unpublished waiting use separate counters.
  These nested/concurrent sums must not be added into a wall-time decomposition.
* `read_profile_selected/completed/errors/returned` bind completed read groups and
  actor-only returns. `read_profile_inline_completed/inline_ns` separate caller-owned
  scalar progress; `read_profile_actor_completed` is the actor population.
  Do not divide actor queue/service timing by combined inline/actor completions.
  Successful-group count is completed minus errors; actor return uses returned
  count. Server coverage uses `server_response_recheck_samples`. Dropped replies
  need not have return samples. Server/codec spans nest within exchange/service.
  All sums can overlap concurrent requests; do not add them to elapsed replay.
  `read_rpcs` counts submitted groups; negotiated limits can split one group into
  several wire requests. `read_profile_wire_requests` counts those wire requests
  in successful sampled profiles, not in unsampled traffic.
* `write_durable_batches` distinguishes storage progress from acknowledged
  `write_concurrent_completed`. Admission, pending/queued/byte peaks and
  `write_peak_inflight_nodes` expose whether application writes actually overlap.
  These peaks are cumulative, and separately scraped counters are not atomic
  snapshots. The local implementation and remaining locks are documented in
  [the concurrency cutover](../../docs/distributed/CONCURRENT_AUTHORITY_AND_TRANSPORT_2026-10-13.md).
* `read_profile_transport_*` covers connection progress sweeps with sampled
  requests queued/active. It can include interleaved unprofiled work. Queue,
  submit and retire spans have separate sample counts because frames can cross
  the sample boundary. CQ clocks/counts describe local progress calls, not
  per-RPC network RTT. Sampling turns these clocks off when sampled work ends.
  The local monitor adds no diagnostic RPC or scrape-time owner/connection lock.
  Availability flags distinguish absent server/progress data and TCP's absent
  fabric CQ data; the analyzer masks unavailable timings instead of reporting
  zero latency. Qualification requires successful profile/server/transport
  coverage when enabled and actual timed CQ calls for fabric transports.

`experiment/results.json`, every raw log/metric/sample/schema/audit,
`evidence.tar.gz` and its receipt are retained. Bulk DBs/WALs and private TLS
material are left in the owned run directory but excluded from the public
measurement evidence archive. Build/source archives are bound by their hashes;
retain them alongside evidence. The analyzer reads results, audit and Prometheus
bytes directly from the receipt-hashed archive, not unchecked unpacked copies.
It rejects duplicate/unsafe members, mismatched archived source/build provenance,
incomplete runs, missing full audits or unequal checkpoint digests.

Parent verification after integration: run `python3 -m unittest discover -s
.../experiments/sui-checkpoint -p 'test_*.py'`, build both Sui features and the
standalone audit, then execute the real paired local fixture and analyze its
receipt. Unit tests alone do not validate real Sui execution.

## Local lifecycle and operation checks

From `tidehunter-sharded-wal/`, use fresh absolute output directories:

```sh
cargo run --offline --release -p tidehunter \
  --features experimental-distributed,test-utils \
  --example sharded_wal_lifecycle_check -- /absolute/new/lifecycle-output

cargo run --offline --release -p tidehunter \
  --features experimental-distributed,test-utils \
  --example sharded_wal_adapter_bench -- /absolute/new/adapter-output
```

The lifecycle command owns five TCP children, verifies history beyond resident
budgets, protected cuts, Sui pruning, root-before-unlink and exact retries after
payload retirement, then stops children and removes its temporary data. Its
native oracle is explicit synchronous Put/Delete/reopen, not a claim that native
WalBased relocation has identical immediate mixed-filter cleanup semantics.

The adapter command records all sixty read/write/iterator rows with profiling
off by default; append `--profile` only for a separate diagnostic run. It uses
256-byte and 8-KiB values, one/eight callers and bounded caches. These isolated
hot-set timings complement actual Sui replay; they do not replace it or establish
cold NVMe performance.

The successful evidence is retained under the outer project's
`checkpoint-work/2026-10-15-local-lifecycle-parity/`. `local-comparison.json`
contains all accepted real-Sui pairs, native/remote counters, stage denominators,
wrapper histogram bounds, memory/swap and all adapter rows. `validation.json`
records executed checks; `cleanup-receipt.json` records only this task's removed
bulk data. Existing prior-session results and the original fixture were retained.

## Combined real-Sui and cold-WAL cloud campaign

The existing cloud supervisor accepts this additional immutable configuration:

```json
"kv_suite": {"repetitions": 2, "diagnostics": true}
```

`aws-sharded-wal/checkpoint_kv.py` runs after the complete real-Sui matrix and
before its existing unconditional export/termination boundary. It uses the
same source-bound `storage-node` executable, controller and five separate NVMe
hosts; it does not invoke the older two-source rebuild or a second fleet.
`atomic_start.py` binds the options before controller handoff.

The fixed matrix covers 256-byte/8-KiB values, scalar/32-key shapes, warm
uncached, bounded coordinator-cache and verified cache-evicted value reads on
TCP/EFA. Every case has a native control. Clean repetitions and sampled
diagnostics remain separate. A 32-batch resident-authority-window stress case
does not claim that all descriptor entries or metadata file pages are cold.
Cold WAL phases fail if their measured processes account for zero physical
read bytes; this is phase evidence, not proof of one device read per key.

KV results include raw sample/summary CSVs, read IO/residency, process resource
snapshots, independent diagnostics, node archives and receipts under the
controller archive's `results/kv/`. Every EFA case requires positive, source/case-
bound hardware send/receive evidence on all six hosts. A failed or missing KV
case invalidates combined campaign completion but cannot bypass fleet teardown.

The controller role must include SSM discovery, exact-five-host SendCommand and
GetCommandInvocation; AmazonSSMManagedInstanceCore alone does not grant these
caller APIs. The exact owned S3 prefix additionally needs `case-artifacts/*`,
`results/nodes/*` and `inputs/kv-storage-node` for this subsuite. Existing
absolute bootstrap expiry and shutdown-to-terminate settings remain unchanged.

Atomic bootstrap archives must place every member under `runner/`, not
`root/runner/`. The controller configuration must set `campaign` to
`sui-checkpoint` and `run_root` to `/mnt/tidehunter/<run_id>`. Validate the
extracted archive with its frozen `atomic_start.validate_supervisor` before
publishing the generated controller configuration or its hash tag.

Release the controller hash only after all five current storage instances publish
`stage=bundle-ready` under `results/cells/startup/storage/<partition>/startup.json`.
Match each marker's instance, role, run and deadline; an old instance's marker is
not readiness. SSM `Online` alone does not prove that the runner is installed.
The bundle-ready marker follows the storage agent restart, so controller probes
cannot race extraction or that restart. Keep the original absolute expiry armed.

