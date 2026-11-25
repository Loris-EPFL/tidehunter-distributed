# Local, Codex Cloud and GitHub test environments

This guide separates software correctness from real verbs-hardware validation.
The shared entry points do not launch cloud resources, use cloud credentials,
run the large benchmark campaign or change production transport selection.

## What Codex Cloud can test

| Environment / suite | What it exercises | What it does not establish |
|---|---|---|
| Native and distributed Rust tests | WAL/recovery/publication rules, TCP/mTLS, cache and adapter behavior | Independent-device scaling or datacenter latency |
| Fabric native unit tests | Actual packet/window/CQ state fixtures, CRC/order/credits, EAGAIN and registered-slot ownership | A real verbs NIC or provider data path |
| Libfabric `tcp;ofi_rxm` loopback | Real software-provider calls, stream transfer, partial buffers and completion progress | RDMA or verbs hardware traffic |
| GCP campaign (`experiments/gcp-rdma`) | Distributed-KV comparison — native, TCP, portable verbs, Valkey TCP/RDMA, Garnet and Kvrocks — on separate hosts, with activity and state proofs | Anything not explicitly measured in that campaign; Sui replay; cluster latency |
| Offline Python harness tests | Source/config identity, result validation, supervisor lifecycle and cleanup decisions without cloud calls | Actual allocation, device access or cluster shutdown |


No verbs device is assumed in a Codex Cloud container or standard GitHub runner.
A Cloud task can orchestrate an explicitly authorized external hardware campaign,
but the verbs peers must execute on the suitable hosts. An Internet/VPN connection
does not turn the Cloud container into a verbs peer.

Do not run every ignored test in Cloud. Hardware absence must not be hidden by a
software/TCP fallback or reported as a passing hardware test.

## Kernel NVMe/TCP recovery in GitHub Actions

The kernel job uses one binary and a real loopback NVMe/TCP target. Its gating
single-owner qualification runs the same four-epoch sequence as the cloud
qualifier: a coordinator child exits without dropping the database or
checkpointing, then the parent reopens it. It covers WAL-replay history, each
with native and synchronous durability. The
four-owner admission and second-open proof is gating. The `--wal-prewrite`
pass is also gating.
CI uses 8 MiB WAL members with 4 MiB fragments to keep the real member and
fragment crossings small. Cloud qualification keeps its 1 GiB members.

The `kernel-nvme-tcp-*` artifact retains per-history timings, cleanup evidence,
and the Rust qualifier's phase, child-ready receipt, and log on failure. These
are functional checks, not RDMA, independent media, or performance evidence.
Cloud qualification runs through [`experiments/gcp-rdma`](../../experiments/gcp-rdma/) (see also [GCP RDMA minimum access](GCP_RDMA_MINIMUM_ACCESS.md)).

## Shared commands

Run these from the checkout root. The Python runner itself resolves the repository
root, so an absolute path to it also works from another current directory.

```sh
bash .github/scripts/setup.sh core
python3 .github/scripts/checks.py native
python3 .github/scripts/checks.py distributed
python3 .github/scripts/checks.py harness
```

Prepare native fabric build dependencies before requesting fabric profiles:

```sh
bash .github/scripts/setup.sh fabric
python3 .github/scripts/checks.py fabric
python3 .github/scripts/checks.py software-fabric
```

Profiles:

| Profile | Requested coverage |
|---|---|
| `format` | Repository Rust formatting check |
| `lint` | Strict default/distributed engine lint plus the feature-disabled fabric crate |
| `native` | Native/default Rust tests |
| `distributed` | Experimental distributed engine tests with TCP and existing local fixtures |
| `fabric` | Strict native-fabric/engine-fabric lint, fabric/engine unit tests and default missing-provider rejection integration |
| `software-fabric` | Explicit selected ignored software-provider stream cases (`SOFTWARE_STREAM_TESTS`); not the real-verbs test |
| `harness` | Existing offline kv-comparison and GCP harness/analysis unit suites |
| `all` | The shared check set, including the explicitly requested software-provider coverage |

Preparation is explicit. Ordinary checks must not install packages, rewrite a
lockfile or silently download/build a missing provider. A requested profile fails
when its dependencies, selected tests or software provider are unavailable.
A reduced environment may intentionally request a smaller profile set, but must
report exactly that coverage rather than claim `all` passed.

Hosted runs split a profile with `--shards N --index I`. Each shard lists the
live test names of every cargo test target, runs every Nth name with `--exact`
and fails when it runs fewer tests than it selected. Local runs omit both flags
and keep the whole profile. A shard with no assigned name runs nothing.

Python 3.11 or newer is required. Setup prepares PyYAML and boto3 for the exact
interpreter used by the runner. `TIDEHUNTER_SETUP_SKIP_APT=1` is only for a machine
with the required native/Python dependencies already available; it is not a
dependency bootstrap mode. An existing isolated Python environment is suitable.
Setup reuses `clang` and `cmake` executables already on `PATH`, including
hosted-runner installations outside apt. It reuses a readable C `libclang` shared
library in `LIBCLANG_PATH` or the standard LLVM/multiarch library directories;
the C++-only `libclang-cpp` library does not qualify. Missing tools are still
installed. This avoids downloading a second CMake or the static Clang SDK just
because its apt metapackage is absent. The 300-second apt-update and 600-second
apt-install limits remain unchanged.

Concurrency fixtures use explicit gates and observable storage, publication and
recovery state. Per-stage diagnostic completion counters are not scheduling
barriers; a fast preparation may finish before a durable reply arrives.

The optional MCP-SDK test and opt-in real-systemd exercise can be skipped by their
existing test decorators; their skipped counts are not executed coverage. The
offline profile rejects `TH_TEST_SYSTEMD=1` rather than touching a real service.

Cargo checks use `--locked --offline`. Dependency fetching and toolchain preparation
belong to `setup.sh`, not a timed test or hidden fallback. The software installer
verifies the official [libfabric 2.6.0 release](https://github.com/ofiwg/libfabric/releases/tag/v2.6.0)
archive against its published SHA-512 and installs only TCP/RXM into an owned
prefix.
An unrelated `LD_LIBRARY_PATH` is retained without preventing prepared-provider
discovery; explicit incompatible/missing prefixes still fail.

Formatting and strict linting can reveal unrelated pre-existing work in a dirty
checkout. Do not format or relax inherited runtime code merely to hide a failure;
identify and report it, and keep the scope of any fix explicit.

### Preparing an independent local libfabric

Prepare the software provider in its owned cache, outside every source checkout:

```sh
bash .github/scripts/setup.sh fabric
python3 .github/scripts/checks.py software-fabric
```

The default prefix is
`${XDG_CACHE_HOME:-$HOME/.cache}/tidehunter/libfabric-2.6.0`.
An explicit `TIDEHUNTER_LIBFABRIC_PREFIX` still selects another prepared prefix.
No middleware checkout or archived build output is searched. Do not export the
software-provider selection into a cloud verbs benchmark shell or replace its pinned
hardware installation. Software setup does not install or qualify a verbs NIC driver.

## Codex Cloud setup

1. Push the intended source/tests/workflow commits to the fork. Cloud cannot
   validate uncommitted laptop files or implicitly read sibling repositories.
2. Connect/select `Loris-EPFL/tidehunter-distributed` and the intended branch/commit.
3. Create a Cloud environment and configure its install step to run
   `bash .github/scripts/setup.sh fabric` from the engine checkout root.
4. Permit dependency downloads during preparation. Publish the tested environment.
   In the current Cloud UI, use the prepared Install script; the legacy GitHub
   integration environment calls this a setup script.
5. Start a fresh task against the intended commit and run the desired shared
   profiles. Loopback software-provider/TCP cases require permission to create
   local sockets. If that is prohibited, report the requested suite failure;
   do not call skipped integration work a pass.
6. Keep cloud credentials and paid-cluster permission out of this correctness
   environment. Native Codex GitHub code review is enabled separately in account/
   repository settings; adding scripts or `AGENTS.md` does not enable it.

References: [current Cloud environments](https://learn.chatgpt.com/docs/environments/cloud-environments),
[legacy integration environments](https://learn.chatgpt.com/docs/environments/cloud-environment),
[Codex GitHub review setup](https://learn.chatgpt.com/docs/third-party/github).

### Copy-paste settings for the existing two-repository Cloud environment

These blocks retain the existing native-tools virtual environment, Clang 19
headers and both Rust toolchains. They assume the checkouts are
`/workspace/tidehunter-distributed` and `/workspace/sui-tidehunter-distributed`.
Push the shared `.github/scripts/setup.sh` and `.github/scripts/checks.py` files
to the engine branch selected in Cloud before using this install script.
Do not replace the Clang bootstrap with the generic Ubuntu apt setup.

#### Frontend-owned environment variables

Set these values in the Cloud environment UI, with Environment ownership.
They are the single source of truth: the install script and generated `env.sh`
must not redefine them. Enter literal values without shell quotes or `export`.

| Variable | Value |
|---|---|
| `CARGO_HOME` | `/workspace/.cargo` |
| `RUSTUP_HOME` | `/workspace/.rustup` |
| `CARGO_BUILD_JOBS` | `4` |
| `CARGO_PROFILE_DEV_DEBUG` | `0` |
| `CARGO_PROFILE_TEST_DEBUG` | `0` |
| `RUST_TEST_THREADS` | `2` |
| `LIBCLANG_PATH` | `/usr/lib/llvm-19/lib` |
| `PROTOC` | `/workspace/cloud-setup/native-tools/bin/protoc` |
| `BINDGEN_EXTRA_CLANG_ARGS` | `-resource-dir=/workspace/cloud-setup/sysroot/usr/lib/llvm-19/lib/clang/19` |
| `TIDEHUNTER_LIBFABRIC_PREFIX` | `/workspace/cloud-setup/libfabric-2.6.0` |

The complete `PROTOC` and `BINDGEN_EXTRA_CLANG_ARGS` values above must be present,
even when the UI truncates their display. Keep `sh.rustup.rs` allowed for a fresh
Rust bootstrap. Changing only the generated file would be undone by the next
installation; replace the saved Install script with the block below, prepare and
republish the environment, then start a new task.

#### Install script

Copy the entire block into the Cloud **Install script** field. It prepares
dependencies and runs readiness checks during environment preparation, not on
every task startup. The Sui command remains a native storage baseline.

```bash
#!/usr/bin/env bash
set -euo pipefail

: "${CARGO_HOME:?Set CARGO_HOME in the Cloud UI}"
: "${RUSTUP_HOME:?Set RUSTUP_HOME in the Cloud UI}"
: "${TIDEHUNTER_LIBFABRIC_PREFIX:?Set TIDEHUNTER_LIBFABRIC_PREFIX in the Cloud UI}"
export PATH="$CARGO_HOME/bin:$PATH"

mkdir -p /workspace/cloud-setup

if ! command -v rustup >/dev/null 2>&1; then
  curl --fail --location https://sh.rustup.rs -o /tmp/cloud-rustup-init.sh
  sh /tmp/cloud-rustup-init.sh -y --no-modify-path --profile minimal --default-toolchain none
fi

rustup toolchain install 1.90 1.96.1 --profile minimal

if [ ! -x /workspace/cloud-setup/native-tools/bin/python ]; then
  python3 -m venv /workspace/cloud-setup/native-tools
fi

/workspace/cloud-setup/native-tools/bin/pip install --disable-pip-version-check \
  cmake==3.31.6 ninja==1.11.1.4 protoc-wheel-0==29.3 \
  PyYAML==6.0.2 boto3==1.35.99

mkdir -p \
  /workspace/cloud-setup/apt/lists/partial \
  /workspace/cloud-setup/apt/cache/archives/partial \
  /workspace/cloud-setup/sysroot

printf '%s\n' \
  'deb [signed-by=/usr/share/keyrings/debian-archive-keyring.gpg] https://deb.debian.org/debian trixie main' \
  > /workspace/cloud-setup/apt/sources.list

apt-get \
  -o Dir::State::lists=/workspace/cloud-setup/apt/lists \
  -o Dir::Cache=/workspace/cloud-setup/apt/cache \
  -o Dir::Etc::sourcelist=/workspace/cloud-setup/apt/sources.list \
  -o Dir::Etc::sourceparts=- update

(
  cd /workspace/cloud-setup/apt
  apt-get \
    -o Dir::State::lists=/workspace/cloud-setup/apt/lists \
    -o Dir::Etc::sourcelist=/workspace/cloud-setup/apt/sources.list \
    -o Dir::Etc::sourceparts=- download libclang-common-19-dev
)

for package in /workspace/cloud-setup/apt/libclang-common-19-dev_*.deb; do
  dpkg-deb -x "$package" /workspace/cloud-setup/sysroot
done

cat > /workspace/cloud-setup/env.sh <<'ENV'
: "${CARGO_HOME:?Set CARGO_HOME in the Cloud UI}"
: "${RUSTUP_HOME:?Set RUSTUP_HOME in the Cloud UI}"
: "${TIDEHUNTER_LIBFABRIC_PREFIX:?Set TIDEHUNTER_LIBFABRIC_PREFIX in the Cloud UI}"
export PATH="$CARGO_HOME/bin:/workspace/cloud-setup/native-tools/bin:$PATH"
export PKG_CONFIG_PATH="$TIDEHUNTER_LIBFABRIC_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export LD_LIBRARY_PATH="$TIDEHUNTER_LIBFABRIC_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
ENV

. /workspace/cloud-setup/env.sh

command -v gcc g++ make pkg-config cmake ninja protoc
[ -r "$LIBCLANG_PATH/libclang-19.so.1" ]

cd /workspace/tidehunter-distributed

# Reuse the native prerequisites above. This adds rustfmt/clippy,
# fetches locked engine dependencies and builds software libfabric.
TIDEHUNTER_SETUP_SKIP_APT=1 bash .github/scripts/setup.sh fabric

# Engine readiness includes distributed and software-provider coverage.
python3 .github/scripts/checks.py all

cd /workspace/sui-tidehunter-distributed

# Native Sui/Tidehunter baseline, not distributed checkpoint replay.
USE_TIDEHUNTER=1 SUI_SKIP_SIMTESTS=1 \
  cargo test --locked -p typed-store --features tidehunter --lib
```

The exact interpreter used by the shared runner must be Python 3.11 or newer.
The prepared libfabric contains TCP/RXM software providers, not verbs hardware. Do not
export this software library selection into a cloud hardware benchmark shell.
The install script does not launch cloud resources, start a validator or alter project
manifests/lockfiles. A missing native prerequisite must be reported rather than
hidden by skipping a requested check.

#### Start skill

Copy the entire block into the Cloud **Start skill** field:

```text
Use the existing checkouts at /workspace/tidehunter-distributed and
/workspace/sui-tidehunter-distributed. Tasks are already isolated.
Do not create a Git worktree unless explicitly requested.

Source /workspace/cloud-setup/env.sh in every shell before invoking
Rust, Python checks or native build tools. Each repository's
rust-toolchain.toml selects its Rust version. Dependencies and build
outputs are retained. No persistent service is required.
Cloud frontend variables own build settings and tool paths. The sourced
env.sh derives search paths without overriding those frontend values.

During environment preparation, do not modify tracked source files,
manifests or lockfiles. During coding tasks, make the requested
changes and follow the applicable AGENTS.md instructions.

For Tidehunter, run the shared check runner from its checkout root:
  python3 .github/scripts/checks.py PROFILE

Profiles are format, lint, native, distributed, fabric,
software-fabric, harness and all. Run the profiles relevant to the
change. Run all before claiming full engine readiness. These checks
use locked, offline Cargo dependencies. Do not run every ignored test:
the software-fabric profile selects the supported software cases.

For the native Sui storage baseline, run from the Sui checkout:
  USE_TIDEHUNTER=1 SUI_SKIP_SIMTESTS=1 cargo test --locked \
    -p typed-store --features tidehunter --lib

Allow at least 10 minutes for Sui builds/tests. Both the feature and
USE_TIDEHUNTER are required. Inspect the effective Tidehunter
dependency before claiming sibling-fork integration. Native
typed-store results do not prove distributed Sui checkpoint behavior.
Do not silently replace a Git dependency with a sibling path.
Distributed Sui validation requires the compatible committed branch,
matching dependency layout, sharded-wal feature and shard configuration.

Use the environment Install script if dependencies need refreshing.
Do not alter manifests or lockfiles merely to make preparation pass.
Do not rerun the full installation at every task startup.

Cloud software-provider tests do not prove verbs/RDMA hardware behavior
or cluster performance. Use the GCP campaign pipeline
(`experiments/gcp-rdma`) for those measurements, subject to user authorization.
Do not launch GCP
resources as part of environment setup. Full validator, frontend
and live-network deployment are outside this library workflow.

Report actual test counts, failures, ignored tests and skipped checks.
Do not treat skipped hardware tests as passed validation.
```

#### Sui dependency and publication checks

The Cloud checkout may differ from the laptop checkout. Inspect its effective
dependency before claiming distributed integration:

```bash
. /workspace/cloud-setup/env.sh
cd /workspace/sui-tidehunter-distributed
cargo tree --locked --offline \
  -p typed-store --features tidehunter --depth 1
```

The current Sui `typed-store` manifest pins `Loris-EPFL/tidehunter-distributed`
at `6dffd324294670ac2e4cb5403ca8c774ef960423`; it does not use a sibling path.
The native `tidehunter` feature alone does not select remote shard storage.
Remote storage needs `sharded-wal` and its exact namespace configuration.
The checkpoint builder stages an explicit local-engine override in owned source
copies and records the resolved revisions. Do not alter the Cloud installation's
manifests or lockfiles merely to make a checkout layout fit.

#### Distributed Sui cache-metrics cardinality

In `sui/crates/typed-store/src/tidehunter_util.rs`, `AdapterCollector` registers
gauges only for declared canonical keyspaces, while the cache snapshot contains
all 256 possible keyspace IDs. Collection bounds the borrowed counter slice to
the registered gauge count before `zip_debug_eq`; unused IDs are not an error.
This preserves the gauge labels, counter values and equality check. It adds no
lock or allocation and changes no storage, durability or cache policy.

Local validation passed 34 Tidehunter-enabled `typed-store` library tests.
A temporary exercise ran the production loop with 0, 1, 57 and 256 registered
keyspaces over two successive snapshots and checked all exported series/values.
The exercise source was removed. These checks are not a new checkpoint replay
or RDMA validation; the existing AWS campaign used the pre-fix Sui revision.

Save and republish the environment, then start a new task. Existing tasks retain
their old environment. The copied install script requires validation in that
Cloud environment: local shell-syntax checks and earlier component results below
are not proof that this complete Cloud installation has succeeded.

## GitHub Actions

The maintained workflows use the same commands, not a second test implementation.
CI correctness jobs use ordinary Linux runners, bounded execution and read-only
repository permissions. They do not receive AWS credentials or execute privileged
untrusted-PR checkouts. Fabric preparation supplies the optional native library;
software-provider integration is named separately from hardware validation.

The existing large native benchmark workflow is a manual operation, separate from
required PR CI. Configure the `benchmarks` GitHub environment with the required
reviewer/branch protections supported by the repository's GitHub plan before
allowing a sensitive self-hosted runner. Committing `environment: benchmarks`
does not create those approval rules. Do not attach a general-purpose persistent
runner to untrusted pull requests.

Benchmarks use run-owned data directories and retain result artifacts; they do
not delete a shared `/opt/sui/db` dataset. A cancelled correctness job is safe to
replace with a newer revision; cancellation is not a cluster termination policy.

The dispatch choice `random_access` was removed because the inherited workflow
never executed it; its unchanged standalone program hardcodes a shared path and
generates 2 TB. The supported manual choices are `all`, `read_window`, `index`
and `benchmark`. Read-window now executes its existing bounded in-memory program.
The large index/general datasets remain manual-only and require adequate space.

References: [GitHub self-hosted runner security](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners)
and [environment approval controls](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments).

## Preserve local and cloud validation

The historical Sui replay campaign record is
[`experiments/sui-checkpoint/README.md`](../../experiments/sui-checkpoint/README.md)
(archived material; only the README remains). The cloud campaign lives in
[`experiments/gcp-rdma`](../../experiments/gcp-rdma/)
([minimum access](GCP_RDMA_MINIMUM_ACCESS.md)), activated only through the
protected `bench/gcp-rdma` branch with `GCP_BENCH_ENABLED=true`. Local replay
and the cloud campaign need no archived middleware repository. Historical
bundles retain their original source layouts.

For the next explicitly authorized hardware campaign:

- Rebuild and hash-bind the current engine, native/remote Sui and audit/transport
  binaries. Verify their source/config/provider identities on the target hosts.
- Use separate storage hosts/NVMe devices and the actual verbs provider.
  Preserve positive per-host hardware activity proof and complete replay/state
  audits. A software-provider result cannot replace either proof.
- Keep native controls, durability guarantees, cache modes and clean/profiled
  populations matched. The shared laptop SSD and Cloud container timings are
  not independent-NVMe or hardware-RDMA performance measurements.
- Preserve the campaign's independent deadline, complete/partial export and
  exact-owned-resource termination. Secure results before normal teardown;
  failures must not leave paid hosts waiting for an offline analysis.
- Verify retrieval before removing result storage/support resources. Do not modify
  another user's resources or historical hash-bound benchmark evidence.

The correctness profiles above do not launch cloud resources or change the
runtime transport, wire protocol or WAL format. Scoped guidance is in
`.github/AGENTS.md`; the pre-existing root guidance is retained.

## Historical record: local verification of CI setup before middleware retirement

The two sections below are pre-refactor validation records. The AWS EFA
benchmark workflow and `experiments/aws-sharded-wal` they name are removed
designs (tag `archive/multi-design-*`); the counts remain as history.

## Local verification of CI setup before middleware retirement

The setup and shared profiles were exercised on the shared-SSD laptop, with
Rust 1.90, two Cargo build jobs and two Rust test threads. A fresh owned prefix
was built from the checksum-verified upstream libfabric release; it contained
TCP/RXM only, not EFA. Existing system libraries and the old local prefix were
not replaced. Python SDK dependencies used an isolated environment; the apt
installation branch was not executed on the laptop.

| Executed suite | Result |
|---|---|
| Formatting and default/distributed/fabric strict Clippy | Passed |
| Native engine unit suite | 383 passed, 6 ignored; existing doctest ignored |
| Distributed engine unit suite | 775 passed, 7 ignored; existing doctest ignored |
| Fabric crate unit suite | 43 passed, 1 ignored |
| Engine with fabric features | 775 passed, 13 ignored |
| Default fabric integration | Missing-provider rejection passed; 8 explicitly ignored cases |
| Explicit software-provider/direct/TLS cases | 14 selected cases passed |
| Offline AWS harness | 197 passed, 3 skipped: two opt-in systemd cases and one optional MCP-SDK case |
| Sui harness/analysis | 80 passed |
| Existing concurrency program, reduced local workload | 2,048 operations, 25 restarts, 9 control-region rebuilds; both keyspace audits passed |

Counts are per feature/suite invocation, not a sum of unique tests. The reduced
concurrency run does not claim the full default CI stress workload, relocation
coverage or distributed performance. No observed swap was present in the before/
after host snapshots; those snapshots are not a continuous memory trace.

Failure-path checks also rejected an invalid requested prefix, zero selected
tests, real-systemd opt-in in the offline profile, and real-EFA discovery against
the software-only installation. Ambient unrelated library paths were retained
while resolving the prepared libfabric. The actual benchmark preparation/cleanup
shell steps preserved result files and unrelated data, and did not execute an
input containing shell substitution text.

Expanded lint coverage exposed an inherited benchmark parity-check lint; it now
uses `is_multiple_of(2)` with unchanged behavior. The upstream libfabric 2.6.0
configure script printed a `unary operator expected` warning in its disabled-verbs
probe; configuration, build and the exercised software suites succeeded. That
upstream warning remains visible rather than being suppressed.

Both pre-existing root `AGENTS.md` hashes were unchanged. The workflow YAML and
shell/runner syntax and admission conditions were checked locally; `actionlint`
was not installed. No live GitHub job, Codex Cloud task, large native benchmark,
real Sui replay or AWS hardware campaign was launched in this validation. Publish
the current source and environment before claiming a successful Cloud/Actions run.
No installed project skill files were available to update; this guide and the
scoped agent guidance describe the new build/test requirements.

## Middleware retirement verification

The active checkpoint harness now owns `checkpoint_support.py`; preparation,
build admission, local replay and AWS source admission require it as an ordinary
hash-bound engine source. Only the Sui and active-engine source trees are admitted.
The `--legacy-tools` option, external helper inventory and middleware imports are
removed. Historical bundles are unchanged and retain their matching frozen runners;
they must not be relabeled or passed off as new self-contained builds.

The legacy checkout was renamed to `project:archive/middleware-legacy/`, preserving
HEAD `1969983bdd1244ef940686da824f48c35442b72f`, its branch, complete dirty state
and all 443 tracked/nonignored source checksums. The candidate directory moved to
`project:archive/middleware-fabric-candidate/`. Both retained their directory inode
identities in same-filesystem renames; no source, Git history or benchmark data was
deleted. The fork's root guidance changed only its paper location to `../papers/`.

The active portable fixture is
`project:checkpoint-fixtures/sui-genesis-1775/fixture/`. Its export archive hash
and all 1,305 input-file checksums were verified. All three configurations load
through the extracted helper with relative DB paths, embedded genesis and the
retained 0..1775 metadata. The original fixture stays unchanged in the archive.

| Executed retirement check | Result |
|---|---|
| Offline AWS harness | 199 passed, 3 skipped: two opt-in systemd cases and one optional MCP-SDK case |
| Sui checkpoint/support/analysis harness | 102 passed |
| Software-provider/direct/TLS cases | 14 selected cases passed against independent libfabric 2.6.0 |
| Actual preparation CLI with temporary minimal workspaces | All three offline Cargo metadata invocations completed; only Sui/engine sources archived; changed frozen helper rejected |

Software libfabric was prepared under
`${XDG_CACHE_HOME:-$HOME/.cache}/tidehunter/libfabric-2.6.0`, outside the archived
repositories. The checks ran after both old top-level directories were absent.
No permanent probe scripts or temporary workspace copies remain. This verifies
tooling independence, not a rebuilt full Sui node, real checkpoint replay,
hardware EFA performance or a live AWS campaign. No commit or push was performed.

## Removed: AWS benchmark branch

The protected `bench/aws-efa` branch, its workflow, budget ledger and
one-time account setup described the removed AWS EFA campaign. They are
archived at tag `archive/multi-design-*`. The current cloud campaign is GCP
only: [`experiments/gcp-rdma`](../../experiments/gcp-rdma/) and
[GCP RDMA minimum access](GCP_RDMA_MINIMUM_ACCESS.md).
