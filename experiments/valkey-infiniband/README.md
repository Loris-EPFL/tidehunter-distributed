# Valkey over InfiniBand: preparation and bounded functional qualification

This directory is **not a benchmark backend**. It is independent of
`experiments/kv-comparison`, its Rust adapters, matrices, workflows and TCP
lifecycle. No RDMA backend exists in that suite. Nothing here provisions a
provider, downloads sources, installs packages/drivers, starts a server/container,
or dispatches a workflow. Only the explicit `qualify` action connects to an
already running, operator-supplied RDMA endpoint. **There is no TCP fallback.**

The supported release for this preparation is **Valkey 9.0.7**, official source
commit **`fe8d406c0b5a64ec897f6455e4de684629883764`**. RDMA is upstream's
**experimental, Linux-only** transport, not a promised production interface.
This directory's existence does not establish that compatible hardware or an
RDMA-capable binary is installed locally. Hardware execution must be reported
from actual qualification evidence, never from the offline plan or these unit
tests.

## 1. Requirements on both existing hosts

- Linux with a real, compatible InfiniBand verbs device and an ACTIVE InfiniBand
  port; accessible `/dev/infiniband/rdma_cm` and the device's `uverbs` character
  node. Kernel/provider, firmware, userspace `libibverbs` and `librdmacm`, fabric
  subnet manager, memory-registration/memlock limits, routing and permissions
  must already be configured by the operator. This program changes none of them.
- A provider supporting **reliable-connected (`IBV_QPT_RC`) QPs** and RDMA
  **WRITE_WITH_IMM**. Valkey uses **RDMA-CM (`RDMA_PS_TCP`)**, not libfabric/SRD.
  `RDMA_PS_TCP` names a connection-oriented RDMA-CM port space; it does not mean
  this tool can quietly use a TCP socket instead of RDMA.
- **IP over InfiniBand (IPoIB)** running on both nodes, with an assigned numeric
  endpoint IP and a route through the intended IPoIB interface. RDMA-CM's IP/GID
  resolution requires this for the InfiniBand path. Merely having an RDMA NIC,
  MPI working, or `ibv_devinfo` installed is not sufficient.
- For virtualized hosts, an SR-IOV InfiniBand exposure supporting ordinary verbs,
  RDMA-CM and IPoIB. Microsoft's existing SR-IOV HPC hosts are one documented
  example; its older non-SR-IOV classes do not provide the same IPoIB capability.
  Provider/model names alone never qualify a host. This is not provisioning advice.
- **EFA is not compatible with this path.** The EFA provider's ordinary QP API
  rejects non-UD QPs; its extended driver QP is SRD, not Valkey's RC QP. An EFA
  instance, `fi_info` result, or Tidehunter's existing EFA/libfabric transport
  therefore cannot be substituted. Software RXE/SIW and Ethernet/RoCE are also
  outside this intentionally InfiniBand-only cohort.
- Existing `valkey-cli` and server binaries from the pinned official Valkey
  source, with actual RDMA support, plus local read-only `ibv_devinfo`, `rdma`
  and `ip` diagnostic tools. Qualification rejects a client without `--rdma`.

### Official distributions versus actual RDMA-enabled builds

Use an existing officially sourced RDMA-capable prebuilt distribution **if one
is available and its build support is verified**. Do not infer RDMA support from
an official image name, generic binary archive, version string, or the existence
of the RDMA source file. The official release-automation Linux archive recipe
inspected below enables TLS but does not enable `BUILD_RDMA`; generic archives
must therefore be treated as unqualified, not advertised as known RDMA prebuilts.
This directory does not claim that an official RDMA-enabled prebuilt is currently
available, and does not download or build a replacement during plan/preflight.

An operator may instead supply an **already built/installed build of the exact
pinned official source**. For future manual preparation only, upstream's Makefile
recipe is:

```sh
# On a separately prepared host, in a fresh, verified checkout at the pinned
# commit, with librdmacm and libibverbs headers/libraries already installed:
make BUILD_RDMA=yes
# Inspect the resulting official client, not a different valkey-cli in PATH:
./src/valkey-cli --version
./src/valkey-cli --help
sha256sum ./src/valkey-cli ./src/valkey-server
```

These commands are instructions for an operator, **not commands run by the
qualifier**. Obtain/verify the source and dependencies separately; do not reuse
an old build with cached incompatible flags. `BUILD_RDMA=yes` builds the bundled
libvalkey RDMA support as well as server support. Upstream also documents
`BUILD_RDMA=module`; a module deployment additionally needs its matching
`valkey-rdma.so`, loader dependencies and separately retained module hash.
Prefer the built-in configuration for this small qualification to minimize
provenance ambiguity. No custom FFI or unofficial RDMA shim is provided here.

## 2. Prepare the server manually, separately

The endpoint must already be running on a dedicated, isolated, authorized test
fabric before `qualify` is invoked. Review upstream RDMA configuration and the
actual installed server before starting it yourself. A built-in-RDMA test
configuration has these relevant settings (replace the address and existing
storage directory):

```conf
# Example for a PRIVATE, fabric-isolated, dedicated test endpoint only.
# This qualifier has no password/TLS support: do not expose this to public or
# untrusted networks. protected-mode no deliberately removes that protection.
protected-mode no
port 0
rdma-bind 172.16.0.10
rdma-port 16379
cluster-enabled no
dir /existing/dedicated/valkey-data
appendonly yes
appendfsync always
no-appendfsync-on-rewrite no
maxmemory-policy noeviction
```

For a module build the operator also supplies the matching `loadmodule` setting.
The qualifier never changes `CONFIG`, ACLs, persistence policy or listeners and
never starts/stops this server. `port 0` is a useful additional assurance that
there is no ordinary TCP listener to mistake for RDMA. Use an isolated,
unauthenticated test endpoint whose existing ACL permits `INFO`, `CONFIG GET`,
`GET`, `SET`, `DEL` and `EVAL`; the script does not accept credentials or URIs.
Authentication/cloud credential environment variables are not passed to child
commands. Do not add secrets to executable paths or the provenance file.

On the server host, separately record the installed server's SHA-256 and
`valkey-server --version` output and compare them to your verified build. On the
client host, create a small provenance JSON with **exactly** these five fields,
using the real lowercase SHA-256 digests, not the illustrative strings:

```json
{
  "release": "9.0.7",
  "source_commit": "fe8d406c0b5a64ec897f6455e4de684629883764",
  "source_url": "https://github.com/valkey-io/valkey/tree/fe8d406c0b5a64ec897f6455e4de684629883764",
  "cli_sha256": "replace-with-the-64-hex-digit-installed-client-sha256",
  "server_sha256": "replace-with-the-64-hex-digit-server-host-binary-sha256"
}
```

The program checks the client digest against the actual installed file and
requires the exact pinned source identity. **The provenance file is an operator
assertion, not an upstream signature or independent supply-chain attestation.**
The remote server file hash is explicitly marked unobserved locally. Actual
server release/build identity comes from `INFO server` over the RDMA connection;
actual transport support additionally requires completed RDMA commands.

## 3. Local plan and preflight: no RDMA endpoint is contacted

Run from the repository root, using Python 3's standard library only:

```sh
python3 experiments/valkey-infiniband/qualify.py --help
python3 experiments/valkey-infiniband/qualify.py plan --output /tmp/valkey-ib-plan.json
python3 experiments/valkey-infiniband/qualify.py preflight --output /tmp/valkey-ib-preflight.json
```

Use a new output filename each time. Parent directories must already exist.
Without hardware/client/options, `plan` still exits **0** with `status=plan_only`
and an explicit `missing` list: it is a successful inventory, **not successful
qualification**. `preflight` exits **2** for missing requirements, including an
unavailable kernel device, unsupported client build, absent diagnostics or
missing endpoint/provenance inputs. `--help` is non-mutating.

For a fully specified local preflight, on the existing client host:

```sh
python3 experiments/valkey-infiniband/qualify.py preflight \
  --cli /existing/valkey/src/valkey-cli \
  --device mlx5_0 --interface ib0 --host 172.16.0.10 --port 16379 \
  --provenance /existing/evidence/valkey-build.json \
  --output /tmp/valkey-ib-specified-preflight.json
```

`plan` reads bounded local files/sysfs and hashes available binaries but starts
**no subprocesses**. It explicitly lists version/build/route observations not
made. `preflight` additionally invokes only `valkey-cli --version`,
`valkey-cli --help`, `ibv_devinfo -d DEVICE`, `rdma link show` and
`ip -4/-6 -j route get NUMERIC_IP`. Route lookup is a kernel query, not an
endpoint connection. It reports inventory, interface/device sysfs mapping and
whether the kernel route selects the requested IPoIB interface. All missing
requirements prevent the qualification action from contacting the endpoint.

A ready preflight has `status=local_preflight_ready_not_transport_qualified`:
that still proves neither a usable remote listener nor a successful RC QP nor
the physical path used by a connection. Run preflight independently on both
hosts for local inventory, using the opposite endpoint address where applicable.

## 4. Explicit bounded RDMA functional qualification

Only when local requirements and the preexisting remote service are ready:

```sh
python3 experiments/valkey-infiniband/qualify.py qualify \
  --cli /existing/valkey/src/valkey-cli \
  --device mlx5_0 --interface ib0 --host 172.16.0.10 --port 16379 \
  --provenance /existing/evidence/valkey-build.json \
  --command-timeout 5 --output /tmp/valkey-ib-functional.json
```

The explicit `--port` is mandatory for qualification; there is no assumed TCP
port or hostname/URI interpretation. The tool first repeats local preflight.
Then, using **only `valkey-cli --rdma`** with no cluster redirect/retry flags, it:

1. Observes actual `valkey_version=9.0.7`, nonempty build ID and standalone mode
   through `INFO server`. Observes `CONFIG GET` for the specific listener and
   persistence settings; rejects an absent/mismatched RDMA listener or cluster.
2. Observes a bounded allowlist of `INFO persistence` settings/state. This is a
   settings snapshot, not a crash-recovery or NVMe qualification.
3. Generates exactly one key in
   `tidehunter:valkey-ib-qualification:<random-uuid>:value`. Requires JSON nil for
   its initial missing observation, and `SET NX PX 120000` → exactly `OK`.
4. Reads and compares **all bytes exactly**, including every byte value, NUL,
   CR and LF; performs `SET XX PX 120000` overwrite and exact binary GET again.
   The official CLI's quoted input and raw output with an empty response
   delimiter preserve binary values. No textual `strip()` is applied to GET.
5. Requires `DEL` → integer `1`, missing `GET` → JSON null, and another `DEL` →
   integer `0`. Empty strings, newline changes and boolean/integer confusion do
   not pass as missing or exact responses.
6. In a `finally` path after any attempted SET, atomically deletes only that
   generated key **if its current value matches one of this run's test values**,
   then requires a missing GET. A colliding/replaced foreign value is not
   unconditionally removed. No scan, wildcard delete, `FLUSHDB` or `FLUSHALL`
   is used. The short TTL is a backstop, not a claim that network-error cleanup
   was observed successful.

A failed/timed-out operation fails the run: mutations are **never blindly
retried**. A failed SET can have an uncertain outcome; even a subsequent missing
GET cannot rule out a late SET, so evidence marks cleanup uncertain rather than
claiming it was proved clean. PX limits the lifetime after any late application.
If cleanup or its final missing observation fails, the run exits **2** with the
exact generated key and cleanup evidence retained. Never manually flush the
endpoint to clean up a qualification. An interrupted run attempts the same
bounded cleanup; SIGKILL/power loss cannot guarantee cleanup, hence the TTL.

Success exits **0** with
`status=rdma_functional_qualified_not_hardware_attested`. A failed local admission
or functional check never becomes a success and never triggers TCP fallback.

### Evidence and bounds

- A new, mode-0600 evidence file is reserved **before** contacting the endpoint;
  existing files are never overwritten. JSON is also printed to stdout. Use
  stdout only on trusted terminals/logs: it contains topology/build metadata and
  the generated nonsecret key/values, not passwords.
- At most 32 ordinary command invocations, plus 4 reserved cleanup invocations;
  each gets a 1–10 second wall deadline (default 5), and stdout and stderr are
  each capped at 64 KiB per command. There is no retry loop, background process,
  server lifecycle or cloud operation. Maximum command-wall budget is therefore
  360 seconds at the largest allowed timeout; normal qualification has fewer
  than 32 invocations. Local hashing is streaming and limited to 128 MiB per
  binary; provenance JSON is limited to 16 KiB.
- Retained evidence includes exact argv, exit/failure reason, bounded output
  and output hashes, actual client version/binary hash, declared source/server
  provenance, device/interface/route observations, actual server identity,
  listener/persistence snapshots, exact-value hashes and cleanup observations.
- No CLI functional-response durations or percentiles are reported. Wall
  timestamps/deadlines are operational evidence only. These fresh CLI processes
  include setup/connection work and are **not KV microsecond benchmark latency**.

### Requested device is not proved device use

`--device` selects **inventory**, not a device-binding argument to upstream
`valkey-cli`. IPoIB route and sysfs mapping are useful consistency checks but are
not hardware-path attestation. `ibv_devinfo` proves only an observed inventory;
it does not prove an RC QP was created by this client or used that device.
The fields `device_use_proven` and `hardware_path_attested` deliberately remain
**false even after functional success**. On a multi-NIC/virtualized host, obtain
separate operator-reviewed live QP/process/device correlation and isolated
counter/trace evidence before attributing traffic to a particular physical HCA.
Do not relabel this JSON as that evidence.

## 5. What a future performance comparison must control

- Use **the same existing hosts** for RDMA and ordinary Valkey TCP controls:
  matching source/build/compiler, topology, cores/NUMA/pinning, memory limits,
  values, client scheduling/concurrency, server IO-thread settings and
  persistence/maintenance state. Record whether TCP uses IPoIB or a different
  management Ethernet path. Do not compare different VM/NIC/CPU/storage classes
  and call the difference a transport improvement. TCP control execution belongs
  to the existing TCP suite, not to this tool, and is not run automatically.
- `appendonly yes`, `appendfsync always`,
  `no-appendfsync-on-rewrite no` and `maxmemory-policy noeviction` describe the
  intended durable-write control. Valkey's `always` policy can group writes
  before one fsync and before replies. Record effective settings, maintenance,
  persistence health, RF/topology and filesystem/device facts. This qualifier
  records whether these settings are observed but does **not** require them to
  pass transport-only functional qualification, and never certifies durability.
- **GET reads Valkey values from RAM.** An NVMe-backed AOF does not turn GET into
  an NVMe value-WAL/cold-read workload. Compare a hot/cache-resident Tidehunter
  cohort separately from native NVMe retrieval; preserve this distinction in
  claims and graphs. Functional success cannot establish a 5 µs latency floor.
- Tidehunter must first have an actual **compatible InfiniBand transport** and
  its own end-to-end qualification on these hosts: a supported RC/verbs or
  otherwise explicitly matched provider path, exact values, error/cancellation
  semantics, progress/threading, memory registration, durability and recovery
  contracts. Current EFA/SRD results do not supply this. Do not add an RDMA
  alias/backend/matrix row, substitute TCP, or publish a Valkey-vs-Tidehunter RDMA
  result before both real transports and their data-path semantics are proved.

## Primary-source mapping

| Claim / operator decision | Primary source |
| --- | --- |
| Pin 9.0.7 and exact source; security release | [Official 9.0.7 release](https://github.com/valkey-io/valkey/releases/tag/9.0.7), [pinned commit](https://github.com/valkey-io/valkey/commit/fe8d406c0b5a64ec897f6455e4de684629883764) |
| Linux-only experimental RDMA; Makefile flags; RDMA bind/port and module configuration | [Valkey 9.0.7 README](https://github.com/valkey-io/valkey/blob/9.0.7/README.md#running-valkey-with-rdma) |
| Actual RC QP, `rdma_create_qp`, `RDMA_PS_TCP`, WRITE_WITH_IMM | [Valkey 9.0.7 `src/rdma.c`](https://github.com/valkey-io/valkey/blob/9.0.7/src/rdma.c) |
| Official CLI `--rdma` is conditional on `USE_RDMA`; JSON, binary quoted input, raw delimiter and error flags | [Valkey 9.0.7 `src/valkey-cli.c`](https://github.com/valkey-io/valkey/blob/9.0.7/src/valkey-cli.c), [bundled libvalkey](https://github.com/valkey-io/valkey/blob/9.0.7/deps/libvalkey/README.md), [RDMA client implementation](https://github.com/valkey-io/valkey/blob/9.0.7/deps/libvalkey/src/rdma.c) |
| Generic official archive is not evidence of an RDMA build: inspected build recipe enables TLS without `BUILD_RDMA` | [Pinned official release-automation archive workflow](https://github.com/valkey-io/valkey-release-automation/blob/e7d73f057f70307577aa66b515ddb2fe6fd49718/.github/workflows/call-build-linux-archives.yml), [official release downloads](https://valkey.io/download/) |
| RDMA-CM IP resolution selects a local device using routes; InfiniBand needs IPoIB on both nodes | [rdma-core `rdma_resolve_addr(3)`](https://man7.org/linux/man-pages/man3/rdma_resolve_addr.3.html) |
| EFA ordinary-QP rejection of non-UD, distinct SRD driver QP | [rdma-core EFA provider `verbs.c`](https://github.com/linux-rdma/rdma-core/blob/master/providers/efa/verbs.c), functions `efa_create_qp`, `efa_create_qp_ex`, `efadv_create_driver_qp` |
| SR-IOV/IPoIB distinction and fabric constraints on existing Microsoft HPC hosts | [Microsoft InfiniBand setup reference](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/high-performance-compute/setup-infiniband) |
| AOF policy, group fsync before replies, dataset reconstruction versus RAM reads | [Official Valkey persistence documentation](https://valkey.io/topics/persistence/) |

## Offline behavioral checks for integration owner

```sh
python3 -m unittest discover -s experiments/valkey-infiniband -p 'test_*.py'
```

These tests cover exact/missing observations, binary mismatch, cleanup ownership,
uncertain mutations, provenance/endpoint validation and honest missing inventory.
They do not start a subprocess service, emulate an actual RDMA transport for
benchmarking, or count as hardware qualification. The implementation agent did
not execute builds, tests, formatters, services or hardware qualification.
