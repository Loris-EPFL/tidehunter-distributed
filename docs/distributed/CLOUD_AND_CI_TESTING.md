# Local, Codex Cloud and GitHub test environments

This guide separates software correctness from real EFA hardware validation.
The shared entry points do not launch AWS, use AWS credentials, run the large
benchmark campaign, change production transport selection or replace the existing
local/AWS experiment scripts.

## What Codex Cloud can test

| Environment / suite | What it exercises | What it does not establish |
|---|---|---|
| Native and distributed Rust tests | WAL/recovery/publication rules, TCP/mTLS, cache and adapter behavior | Independent-device scaling or datacenter latency |
| Fabric native unit tests | Actual packet/window/CQ state fixtures, CRC/order/credits, EAGAIN and registered-slot ownership | A real EFA NIC or provider data path |
| Libfabric `tcp;ofi_rxm` loopback | Real software-provider calls, stream transfer, partial buffers and completion progress | RDMA or EFA traffic |
| Direct packet adapter over software | The direct protocol implementation over software-provider IO, including large bidirectional streams | EFA-direct hardware, one-sided reads or NVMe-oF |
| Offline Python harness tests | Source/config identity, result validation, supervisor lifecycle and cleanup decisions without AWS calls | Actual allocation, device access or cluster shutdown |
| Existing multi-host AWS campaign | Native/TCP/EFA/direct and Sui replay on separate hosts, with hardware-activity and state proofs | Anything not explicitly measured in that campaign |

No EFA device is assumed in a Codex Cloud container or standard GitHub runner.
A Cloud task can orchestrate an explicitly authorized external hardware campaign,
but the EFA peers must execute on the suitable hosts. An Internet/VPN connection
does not turn the Cloud container into an EFA peer.

The existing ignored `efa_direct_opens_only_the_negotiated_hardware_fabric` test
is a hardware discovery/guard check, not a full data-path or latency proof.
Do not run every ignored test in Cloud. Hardware absence must not be hidden by a
software/TCP fallback or reported as a passing hardware test.

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
| `software-fabric` | Explicit selected ignored software-provider, direct-adapter and software-fabric reactor cases; not the real-EFA test |
| `harness` | Existing offline AWS-runner and Sui harness/analysis unit suites |
| `all` | The shared check set, including the explicitly requested software-provider coverage |

Preparation is explicit. Ordinary checks must not install packages, rewrite a
lockfile or silently download/build a missing provider. A requested profile fails
when its dependencies, selected tests or software provider are unavailable.
A reduced environment may intentionally request a smaller profile set, but must
report exactly that coverage rather than claim `all` passed.

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

The optional MCP-SDK test and opt-in real-systemd exercise can be skipped by their
existing test decorators; their skipped counts are not executed coverage. The
offline profile rejects `TH_TEST_SYSTEMD=1` rather than touching a real service.

Cargo checks use `--locked --offline`. Dependency fetching and toolchain preparation
belong to `setup.sh`, not a timed test or hidden fallback. The software installer
verifies the official [libfabric 2.6.0 release](https://github.com/ofiwg/libfabric/releases/tag/v2.6.0)
archive against its published SHA-512 and installs only TCP/RXM into an owned
prefix. This differs from the separately pinned AWS `2.6.0amzn1.0` hardware build.
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
software-provider selection into an AWS EFA benchmark shell or replace its pinned
hardware installation. Software setup does not install or qualify an EFA driver.

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
6. Keep AWS credentials and paid-cluster permission out of this correctness
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
The prepared libfabric contains TCP/RXM software providers, not EFA. Do not
export this software library selection into an AWS hardware benchmark shell.
The install script does not launch AWS, start a validator or alter project
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

Cloud software-provider tests do not prove EFA/RDMA hardware behavior
or cluster performance. Use the existing local/AWS campaign pipeline
for those measurements, subject to user authorization. Do not launch
AWS resources as part of environment setup. Full validator, frontend
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

## Preserve local and AWS validation

Existing local/Sui/AWS entry points remain in
[`experiments/sui-checkpoint`](../../experiments/sui-checkpoint/README.md) and
[`experiments/aws-sharded-wal`](../../experiments/aws-sharded-wal/README.md).
A real Sui build also needs the compatible Sui checkout and a certified checkpoint
fixture. RPC, configuration and strict RocksDB audit helpers are maintained in
`experiments/sui-checkpoint/checkpoint_support.py` and are bound by the engine's
frozen source inventory. Neither local nor AWS replay needs the archived
middleware repository. Historical bundles retain their original source layouts.

For the next explicitly authorized hardware campaign:

- Rebuild and hash-bind the current engine, native/remote Sui and audit/transport
  binaries. Verify their source/config/provider identities on the target hosts.
- Use separate storage hosts/NVMe devices and the actual EFA/direct provider.
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

The correctness profiles above do not launch EC2 or change the runtime transport,
wire protocol or WAL format. The separately activated paid-branch workflow below
adds orchestration, not a one-sided remote WAL or NVMe-oF backend. Scoped guidance
is in `.github/AGENTS.md`; the pre-existing root guidance is retained.

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

## Protected AWS benchmark branch

### Publication and activation

The protected execution branch is `bench/aws-efa`. A push to that branch requests
one complete paid campaign **only after** `AWS_BENCH_ENABLED=true` is set.
The workflow is disarmed otherwise. Other branches retain cheap correctness CI;
they cannot start this paid workflow. Develop and review on feature branches,
then merge the reviewed engine state into the benchmark branch.

The implementation does not itself commit, push, deploy the shared foundation,
create GitHub environment restrictions or enable paid runs. These are separate
activation steps. Codex Cloud environment variables are not GitHub Actions
variables and are not copied automatically.
Publish every useful source file from both forks before a campaign. Develop and
publish incremental commits on a work branch, then merge through the protected
benchmark branch's correctness checks. Use `Loris-EPFL` for commit and push
identity. Generated caches, credentials and local build databases are not source.
For interactive AWS operations prefer the connected AWS MCP. If it is unavailable,
use a local CLI/SDK fallback only when the user has authorized it and the caller's
identity is verified in the expected account. Never bypass an IAM denial or
extract another job's credentials. Without valid interactive credentials, report
that limitation; retain the campaign's CI/EC2 cleanup guards and their receipts.
The campaign's own SDK calls inside GitHub/EC2 are unchanged.

| Maintained file | Responsibility |
|---|---|
| `.github/workflows/aws-efa-benchmarks.yaml` | Exact branch/repository admission, isolated jobs, fresh build, local proof, campaign, cleanup, artifacts and cost summary |
| `experiments/aws-sharded-wal/ci-config.json` | Approved regions/types, mandatory campaign matrix, fixed lifetime and frozen Sui fixture |
| `ci_prepare.py` in that directory | Clean committed source admission, six-binary rebuild, same-build local audit, portable hash-bound cross-job package |
| `ci_campaign.py` | Live discovery/pricing, budget admission, durable launch intents, six-instance allocation, complete collection and exact-owned closure |
| `atomic_start.py` | Automatic guest/controller startup, immutable handoff, final identity checks and local deadline |
| `ci_watchdog.py` | Independent recurring expiry cleanup, including failed partial provisioning and abandoned guest identities |
| `ci_budget.py` | Conditional-write shared ledger, rolling weekly admission, delayed AWS billing reconciliation and lifetime reporting |
| `ci_infrastructure.py`, `ci-foundation.guard` | Local IAM baseline generation, scoped foundation rendering and deployment checks; no deployment side effects |
| Existing `experiments/sui-checkpoint` and `checkpoint_kv.py` | Real checkpoint replay, state audits, matched native controls and KV measurements |

No middleware checkout is needed. This setup adds no engine or Sui runtime
change. It builds the selected committed engine and compatible reviewed Sui
revision using the existing source-freezing harness.
The Sui checkout is explicitly `Loris-EPFL/sui-tidehunter-distributed` at the
full commit in `AWS_BENCH_SUI_SHA`, never a moving branch. Its published engine
dependency uses an immutable fork Git revision. Only the owned frozen Sui copy
receives a Cargo source patch selecting the campaign's staged engine. Preparation
verifies resolved package paths/features and hashes the transformed manifests and
lockfiles; the original checkouts stay unchanged. Cross-job restoration verifies
and expands the hash-bound source archive before strict inventory admission.

### Manual retries keep producer and execution identities separate

The successful `prepare` job exports both `package_artifact_name` and
`package_sha256`. It uploads exactly that name, computed from its own run ID
and producer attempt. `benchmark` downloads that exact output from the same
workflow run; it does not compute a name from its own incremented attempt.
For example, if prepare succeeds on attempt 1 and benchmark fails, a benchmark
retry on attempt 2 still reads `campaign-inputs-RUN_ID-1`. If prepare itself
is rerun successfully on attempt 2, its consumers instead use the new
`campaign-inputs-RUN_ID-2` and its matching SHA-256.

This reuses immutable build inputs, not prior campaign results. Restoration
still checks the producer's package digest and frozen-source inventory before
fleet credentials. Campaign admission still verifies the current helper
sources, six binary/build receipts, fixture and same-build local full-state
audit before allocation. No cross-run/repository artifact selector, moving
source ref, fallback artifact search or evidence relabeling is supported.
The consumer's `GITHUB_RUN_ATTEMPT` remains unchanged: it derives a fresh
campaign identity, budget admission, absolute lifetime and owned evidence.
Prior unknown EC2 allocations remain fail-closed; retry does not authorize
abandoning them or reset the ledger or the USD 60 budget.

Only retry benchmark work after separate paid authorization and after the
previous attempt's allocation/cleanup state is known. Re-running failed jobs
may include `benchmark`; it is not a free retry mode. A completed campaign
needs cost-only reporting, not another benchmark. The input artifact expires
after two days; an expired/missing artifact or missing/mismatched digest must
fail, not select another run's package.

[GitHub reruns](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs)
retain the original `GITHUB_SHA` and `GITHUB_REF`. Therefore a rerun of an old
workflow does **not** acquire this workflow fix or a newly committed Sui fix.
Changing `AWS_BENCH_SUI_SHA` does not replace a skipped successful prepare
job's package. A changed engine/Sui source pair needs a new prepare/local
audit in a separately authorized new campaign. Do not repack old build proof
under the new source pin.

### Non-benchmark publication and source-pin updates

Publish reviewed source commits as `Loris-EPFL` to the engine work branch
and Sui's `fix/sui-cache-metrics-cardinality`, not to `bench/aws-efa`. One small
commit per affected repository is sufficient; this path needs no PR or
benchmark-branch merge. Preserve Sui's counter-prefix bound in
`crates/typed-store/src/tidehunter_util.rs`: the collector must zip only
`counters[..gauges.len()]`, not all fixed-capacity counter slots.

After those commits are published, from the engine checkout:

```bash
gh auth switch --user Loris-EPFL
gh api user --jq .login
ENGINE_BRANCH=fix/ci-benchmark-retries-2026-10-20
ENGINE_SHA=$(git rev-parse HEAD)
SUI_SHA=$(git -C ../sui rev-parse HEAD)
gh api "repos/Loris-EPFL/tidehunter-distributed/commits/$ENGINE_SHA" --jq .sha
gh api "repos/Loris-EPFL/sui-tidehunter-distributed/commits/$SUI_SHA" --jq .sha
# Correctness only; requires rust-tests.yaml to exist on the default branch.
gh workflow run rust-tests.yaml --repo Loris-EPFL/tidehunter-distributed \
  --ref "$ENGINE_BRANCH" -f tidehunter_repo_ref="$ENGINE_SHA"
```

The engine correctness workflow accepts this exact checkout override, has
read-only repository permission and no AWS credentials or EC2 launch path.
A feature-branch push alone does not select it: its push filters are `main`
and `feat/sharded-value-wal`; manual dispatch is the existing alternative.
Inspect actual job conclusions; dispatch acceptance is not a passing check.
Sui's fork-only `.github/workflows/typed-store-tidehunter.yml` runs on pushes
to `fix/sui-cache-metrics-cardinality` and supports manual dispatch. It uses
`ubuntu-24.04`, repository-selected Rust, pinned CMake/protobuf tools, two build
jobs, no debug symbols and read-only contents permission, without AWS/OIDC or
custom secrets. It checks typed-store formatting and runs
`USE_TIDEHUNTER=1 SUI_SKIP_SIMTESTS=1 cargo test --locked -p typed-store --features tidehunter --lib`
against the unchanged immutable engine dependency. It is a scoped native-adapter
correctness check, not a new full Sui/checkpoint/fabric build.
Do not dispatch Sui's broad upstream `rust.yml`: it fans out to unavailable
`ubuntu-ghcloud`/`windows-ghcloud` runners. Publication of the focused workflow
does not make those upstream jobs portable hosted CI.

Updating the paid source pin is a **separate, authorized step**, not part of
the non-benchmark publication/check commands above:

1. Record the full published Sui commit SHA and its reviewed engine dependency.
   Keep the compatibility table below truthful; an unpublished local fix is
   not the existing published pin.
2. After approval, set and read back the exact repository Actions variable:
   `gh variable set AWS_BENCH_SUI_SHA --repo Loris-EPFL/tidehunter-distributed --body "$SUI_SHA"`,
   then `gh variable get AWS_BENCH_SUI_SHA --repo Loris-EPFL/tidehunter-distributed`.
3. Only after separate campaign authorization, publish the reviewed engine
   state to the protected benchmark branch and require a new successful
   prepare/build/local audit for that exact engine SHA and Sui pin.

Do not change `AWS_BENCH_ENABLED`, dispatch the AWS campaign or merge/push
the benchmark branch merely to validate these publication changes. The
current publication request does not authorize EC2.

### Campaign and hardware contract

One fleet runs all transports sequentially: one controller/native host and five
storage hosts, each with its own local NVMe. All six have the same instance type,
AZ and **cluster** placement group. There is no spread-placement or shared-host
fallback. One `RunInstances` request has `MinCount=MaxCount=6`.

Candidates are `g6.8xlarge`, `g6e.8xlarge`, `g6.16xlarge` and the CPU-only
`c6id.32xlarge` fallback in `us-west-2`, `us-east-1`, `us-east-2`, `eu-west-1`
and `eu-central-1`. Oregon wins equal-price regional ties; Frankfurt remains a
fallback. Discovery verifies current EFA, x86-64, exact per-type SSD
inventory, AMI, subnet and routing capabilities, then prices compatible
candidates. It prefers the cheapest compatible candidate; a larger type is a
capacity fallback, not the default. Capacity and quota are not guaranteed.
The bootstrap and discovery share `bootstrap.INSTANCE_STORE_DISKS`: G6 requires
two SSDs and C6id requires four. Bootstrap validates the complete blank inventory
but mounts only one instance-store SSD per host, not a shared network disk or
RAID. Keep the launcher's generated exact-type IAM condition synchronized with
`ci-config.json` before publishing a new candidate.
Discovery queries approved instance types individually. A missing public Ubuntu
SSM parameter, deregistered AMI, unsupported exact instance type or absent usable
subnet/AZ offering records a regional/type rejection and permits other candidates.
Authorization failures (including `OptInRequired`), generic parameter errors,
`InvalidAMIID.NotFound`, timeouts, malformed responses and invalid pricing fail
closed. Allocation still advances only after a definitive capacity/quota
rejection; an ambiguous launch never licenses another fleet.

The default matrix contains:

- Native controls paired with TCP, EFA and EFA-direct Sui replay.
- One clean repetition and one separately profiled repetition per transport.
  Read/write/native-write profile limits are `[0, 128]`, not clocks enabled in
  every measured operation.
- The full paired KV suite: small/large values, scalar/batched operations,
  warm, explicit-cache and cold modes, authority pressure, transport floors and
  separate diagnostic cases. Raw timings, counters and state proofs are retained.
- The exact private-chain Sui fixture, checkpoints `0..1775`, tip digest
  `EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`, archive SHA-256
  `7eb43b71435547939760598e541d13b9a77f722c5a9d0f8c8ee5bd583b751f15`.

This is real Sui execution against a generated private chain, **not mainnet**
or proof of a production-sized working set. Hardware comparisons require
per-host EFA/direct activity and full native/distributed state audits.
Unsupported EFA must fail, never silently become TCP.

### Build and credential isolation

Before EC2 allocation, the preparation VM checks out the exact engine commit and
a full 40-character Sui commit, runs correctness checks, rebuilds all six binaries,
and completes the same-build five-process local TCP replay and state audit.
Local timings do not establish independent-NVMe or EFA performance.

External namespace preparation, like the replay entry point, disables Python
bytecode writes before importing helpers from the frozen source tree. It must
leave that tree unchanged even when `PYTHONDONTWRITEBYTECODE` is absent on a
guest. Otherwise importing `build.py` creates an extra `__pycache__` file and
the subsequent replay correctly rejects the changed inventory. The regression
executes the real preparation CLI with the CI bytecode settings removed and
checks the complete file inventory and all bound hashes. The source-integrity
validator remains unchanged; generated caches are not exempted or deleted later.

The build job uses the `rdma-bench-inputs` environment and an input-only OIDC
role that can retrieve the exact fixture. Its OIDC subject is **not trusted by
the fleet role**. Merely putting compilation on another VM would be insufficient
if both jobs could request tokens accepted by the fleet role.

A fresh orchestration VM retrieves this run's SHA-bound package and restores
executable modes/source links before obtaining fleet credentials. It runs no
Cargo build scripts. The controller and five storage hosts receive six distinct
per-run profiles under a fixed permissions boundary. EC2 starts without any
instance profile; final narrowed identities are attached only after the six
instance IDs and exact policies are known. Guest writes cannot modify another
run's inputs, results, budget ledger or deployment configuration.

The default build runner is `ubuntu-24.04`. Only a disposable GitHub-hosted
build VM removes unused Android/.NET/Haskell SDK directories. This step never
runs on the laptop or a self-hosted runner. A 45-GiB free-space admission check
is not a guarantee that every future Sui build fits. A larger trusted build
runner can be selected through `AWS_BENCH_BUILD_RUNNER`; it does not replace
the external EFA fleet.

### Lifetime and closure

The fixed campaign lifetime is 3,600 seconds, with the final 600 seconds reserved
for export. It is not reset by retries, controller handoff or a new cleanup
process. Builds happen before this clock starts.

Cleanup reconciles or requests termination for every exact owned host before
observing the fleet in one shared 600-second, 120-round window. A slow first host
does not delay termination requests to its peers or leave an earlier per-host
timeout after all hosts reach `terminated`. IAM removal still requires explicit
terminal proof for the entire owned fleet. The separate GitHub cleanup step has
15 minutes for this bounded observation and post-compute resource reconciliation;
it does not extend the EC2 lease, controller cutoff or export reserve.
The GitHub orchestration job has a separate 180-minute ceiling and explicit
preparation, campaign, cleanup and artifact-transfer step limits. Their combined
limits leave runner/post-action headroom, and fleet credentials cover the
AWS-bearing cleanup window. This job ceiling is not an EC2 lease extension.

Three independent paths enforce closure:

1. Parent `finally` and GitHub `always()` cleanup collect evidence and reconcile
   the exact run.
2. Each guest arms an absolute shutdown deadline with EC2
   instance-initiated shutdown configured to **terminate**.
3. An independently scheduled Lambda checks expiry every minute, even if
   GitHub, bootstrap, controller or S3 journal access fails.

Launch intent and candidate-specific client token are journalled before the
request. An unknown launch outcome is reconciled, not followed by a blind
second allocation. Changing AMI/subnet/type/AZ creates a distinct candidate
token; retrying that candidate keeps its token and original expiry.

S3 requests have at most three SDK attempts; Lambda readiness retains twelve,
and EC2 allocation retains one. Journal body reads restart at most twice after
a streaming timeout or truncation, always closing the previous body and keeping
the same size bound. A lost conditional-write response is accepted only when a
fresh read returns the exact submitted bytes; its ETag binds the next write.
Foreign, different, unreadable or unauthorized state still fails closed. The
budget ledger keeps its existing operation-bound reconciliation, and transport
errors with a null SDK response do not suppress independent cleanup. None of
these recovery paths renews the absolute fleet expiry.

Normal completion checks the complete S3 export before termination. Owned compute
is reconciled before local downloads, archive analysis or billing can delay the
runner. Cleanup covers instances, run-owned EBS/ENIs, security/placement groups
and temporary IAM identities. Known instance IDs require explicit terminal proof;
`NotFound`, missing pages or API failure are not proof. Foreign resources are untouched.

At failure or expiry, cost safety takes priority: export available partial logs,
then terminate rather than keep hosts running for analysis. Results may therefore
be incomplete after a failed campaign, and the job must not report a successful
comparison. No client can guarantee an instantaneous hard billing cutoff during
a simultaneous AWS control-plane and guest failure; unresolved closure stays
visible and conservatively reserved.

Raw evidence is retained in the private S3 bucket and GitHub artifact for 30 days.
The fixture and active budget ledger remain. Shared S3/Lambda/EventBridge/logging
resources are deliberately retained; no EC2 fleet remains idle. Retained storage,
IPv4 while allocated, transfer and shared control-plane services can incur
charges even though terminated instances no longer incur compute charges.

### Weekly budget and reporting

`experiments/aws-sharded-wal/budget.json` is the single editable budget policy:

```json
{"weekly_budget_usd": "60"}
```

This sets the **rolling seven-day shared branch limit**, not a per-run cap.
There is no competing GitHub budget variable or direct dollar-value CLI override.
Fresh admission retains only live-priced candidates whose all-in upper quote fits
the authoritative ledger's current availability. Each quote includes the six-host
fleet, root EBS, public IPv4, reconciliation allowance and bounded S3 overhead,
with separate upward decimal rounding. The selected full candidate objects and
overhead are journalled before the unchanged conditional reservation. A budget
loss between selection and reservation still denies allocation; it does not
silently select another set. Restarts reuse the persisted selection, including a
crash after reservation, rather than filtering against their own active hold.
This narrows unaffordable fallbacks only: region/type permissions, six hosts, the
full Sui/KV matrix and original lifetime remain unchanged. Missing pricing or an
absent, corrupt or unreadable ledger fails closed before paid allocation.

The ledger uses conditional S3 writes and account/repository/branch identity.
Unknown cleanup keeps the conservative maximum of reservation, observed usage
and billed amount. Verified cleanup releases the active reservation; recent
closed runs retain the maximum of observed usage and billed cost, with late
billing adjustments accounted separately. Reports distinguish historical
reservations, active holds, observed estimates and conservative budget charges.
Changing this file does not reset history or erase reservations.

The separate CI-generated `aggregate.json` is published to the branch's
`ci-control/reports/` S3 key and downloaded as an accounting artifact. It is not
a tracked configuration file or a manually maintained running counter. Only
accounting code writes it; the bucket policy denies report-content writes from
other principals, including ordinary human and guest sessions. Versioning and
conditional writes preserve update history. AWS administrators can still change
the bucket policy; this is not immutability against an administrator. Manual
initialization/reconciliation may record a publication warning; CI publishes the
authoritative report using its permitted accounting identity.

Publication reads the current report and authoritative per-run ledger, replaces
each run's contribution by run ID and derives **total, UTC calendar-month,
Monday-start calendar-week and daily** aggregates with explicit period bounds.
Retries, cleanup and delayed billing refreshes do not add the same run twice.
Changing the budget policy cannot lower or reset historical spend.

Reports separate usage estimates, AWS billed-to-date amounts, reservations and
remaining admission capacity. Estimates and billed amounts are not added
together. Billing can lag; inactive tags, incomplete coverage and unallocatable
costs are limitations, not final zero-dollar runs. Tracked lifetime begins at
ledger initialization. The calendar reporting week is distinct from the rolling
seven-day admission window.
Estimates are assigned to the UTC settlement day; billed costs use AWS's daily
records. GitHub runner/artifact charges, if any, are outside this AWS report.

AWS billing reconciliation is optional. If cost-allocation tags or billing access
are unavailable, campaign admission still uses durable reservations and observed
runtime estimates; generated reports mark AWS-billed totals unavailable, not zero.
There is no requirement to contact an external administrator before benchmarking.

Activating `Repository`, `BenchmarkBranch` and `RunId` can improve billed-cost
attribution if management-account access becomes available. Shared foundation
resources use `RunId=shared-infrastructure`. The current account is a member of
management account `574888829509`; member IAM permissions cannot activate the
management account's cost-allocation tags. Do not grant blanket administrator
permissions to work around that service boundary. See
[AWS cost-allocation authority](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/cost-alloc-tags.html).

### One-time account and GitHub setup

Keep `AWS_BENCH_ENABLED` unset or `false` until all steps below are complete.
The foundation creates no EC2 instances, but changes IAM and creates retained
support resources. Review its generated policies/template before an authorized
administrator deploys it. Do not upload static AWS access keys to GitHub.

From the engine root, generate and validate the local foundation:

```bash
python3 experiments/aws-sharded-wal/ci_infrastructure.py generate \
  --account 354775971805 --region us-east-1 \
  --bucket tidehunter-ci-354775971805-us-east-1 \
  --output ../checkpoint-work/aws-ci-foundation --profile tidehunter
python3 experiments/aws-sharded-wal/ci_infrastructure.py render \
  --account 354775971805 --region us-east-1 \
  --bucket tidehunter-ci-354775971805-us-east-1 \
  --output ../checkpoint-work/aws-ci-foundation
cfn-lint ../checkpoint-work/aws-ci-foundation/foundation.json
cfn-guard validate --rules experiments/aws-sharded-wal/ci-foundation.guard \
  --data ../checkpoint-work/aws-ci-foundation/foundation.json
```

The generator invokes pinned `iam-policy-autopilot==0.3.0`, retains its raw
baselines and source hashes, and separately scopes ownership/resources.
It reads the real AWS-managed SSM policy; the boundary narrows parameter and
document reads to AWS-owned public resources. No policy is uploaded by these
commands. Use a fresh output directory after source changes. Confirm whether the
account already has a GitHub OIDC provider before deployment; reuse/import it
instead of attempting to create a duplicate.

After policy review and administrator approval, deploy the foundation with
`CAPABILITY_NAMED_IAM`, inspect its outputs and verify the enabled watchdog's
actual health/target/permission checks. Upload the exact frozen fixture to
`fixtures/sui-genesis-1775/fixture.tar.gz` with SSE-S3 and the expected bucket
owner. Initialize the durable ledger once. For a local `aws login` profile,
the pinned SDK needs environment credentials; export them only inside a short
subshell, without printing or storing their values. GitHub OIDC jobs already
receive supported environment credentials and do not run this login/export step.

```bash
(
  eval "$(aws configure export-credentials --profile tidehunter --format env)"
  python3 experiments/aws-sharded-wal/ci_budget.py initialize \
    --bucket tidehunter-ci-354775971805-us-east-1 --account 354775971805 \
    --region us-east-1 --budget-file experiments/aws-sharded-wal/budget.json
)
```

Create **both** GitHub environments, `rdma-bench` and `rdma-bench-inputs`, and
restrict deployment branches to exactly `bench/aws-efa`. Require pull requests
and correctness checks on that branch; block force pushes and deletion.
Environment names in YAML do not configure these restrictions. A required
environment reviewer can add approval per run;
omit that approval gate only if reviewed merges should run fully automatically.
Keep the benchmark branch and its scripts trusted.

Import [`.github/rulesets/aws-efa-benchmarks.json`](../../.github/rulesets/aws-efa-benchmarks.json)
through **Settings → Rules → Rulesets → New ruleset → Import a ruleset**.
It requires pull requests, current GitHub-Actions correctness checks and resolved
review threads, and blocks branch deletion/force pushes with no bypass actors.
It does not require the post-merge paid campaign as a pre-merge check.
The default human-approval count is zero because the verified collaborator list
contains only the owner; set it to one when a second reviewer is available.
Publish the matching correctness workflow before expecting all seven checks.
The Sui source fork needs ordinary source-branch protection, not these AWS
environments, OIDC roles or AWS credentials.

Set these **repository Actions variables**, not Codex Cloud values:

| Variable | Source |
|---|---|
| `AWS_BENCH_ROLE_ARN` | Foundation `RoleArn` output |
| `AWS_BENCH_INPUT_ROLE_ARN` | Foundation `InputRoleArn` output |
| `AWS_BENCH_BUCKET`, `AWS_BENCH_BUCKET_REGION` | Foundation bucket outputs |
| `AWS_BENCH_GUEST_BOUNDARY_ARN` | Foundation `GuestBoundaryArn` output |
| `AWS_BENCH_WATCHDOG_FUNCTION`, `AWS_BENCH_WATCHDOG_RULE` | Foundation watchdog outputs |
| `AWS_BENCH_SUI_SHA` | Reviewed compatible Sui fork full commit SHA |
| `AWS_BENCH_BUILD_RUNNER` | Optional trusted build-runner label; default `ubuntu-24.04` |
| `AWS_BENCH_ENABLED` | Set `true` only after setup/verification |

For a private Sui fork, supply a narrowly scoped `SUI_CHECKOUT_TOKEN`; the
default GitHub token can check out a public fork. These are not AWS credentials.
The AWS trust subjects pin this fork's immutable repository/owner IDs and the
specific environment. The input environment is never accepted by the launcher.
The live repository OIDC configuration reports `use_default=true`,
`use_immutable_subject=true` and prefix
`repo:Loris-EPFL@91122257/tidehunter-distributed@1378911199`. Both deployed trust
policies match that prefix plus their distinct environment. No custom subject
template or long-lived AWS key is needed. Both source forks are currently public,
so `SUI_CHECKOUT_TOKEN` is not needed either.

Automatic pushes work from the benchmark branch. GitHub's `workflow_dispatch`
UI requires the workflow to exist on the default branch as well; if publishing
that identical guarded file there, dispatch with ref `bench/aws-efa`. Its
repository/ref guards still forbid paid runs from `main`.

Cost-only refresh and explicit recovery do not require another experiment:

```bash
# Trusted GitHub workflow; does not allocate EC2.
gh workflow run aws-efa-benchmarks.yaml \
  --repo Loris-EPFL/tidehunter-distributed --ref bench/aws-efa \
  -f operation=cost-report
# On the trusted GitHub runner, using the original run ID and current OIDC role:
python3 experiments/aws-sharded-wal/ci_campaign.py cleanup \
  --config experiments/aws-sharded-wal/ci-config.json \
  --run-id th-ORIGINAL --output /owned/recovery-output \
  --budget-file experiments/aws-sharded-wal/budget.json
```

The recovery example requires the real original ID and readable authoritative
journal. It never allocates a replacement fleet or invents cleanup ownership.
Do not rerun an entire benchmark merely to refresh delayed billing.

References:
[GitHub OIDC with AWS](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws),
[environment deployment protection](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments),
[manual workflow dispatch](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow),
[IAM Policy Autopilot](https://github.com/awslabs/iam-policy-autopilot),
[AWS EFA](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html),
[cluster placement](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-strategies.html),
[cost-allocation tag activation](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/activating-tags.html).

### Published compatibility pins

| Input | Published source |
|---|---|
| Sui fork | `Loris-EPFL/sui-tidehunter-distributed`, branch `refactor/distributed-nvme`, commit `3261f3be974c941584014438159110140a2885a7` |
| Sui's engine dependency | `Loris-EPFL/tidehunter-distributed`, commit `6dffd324294670ac2e4cb5403ca8c774ef960423` |
| Repository Actions pin | `AWS_BENCH_SUI_SHA=3261f3be974c941584014438159110140a2885a7` |

The published Sui manifest, remote branch, author/committer associations and
Actions variable were verified through GitHub as `Loris-EPFL`. The campaign
engine is the exact protected-branch workflow commit, not a separately moving
engine branch. The frozen-copy Cargo patch described above binds Sui to that
selected engine; the portable Sui checkout retains its immutable runtime pin.

Checkout/cache references use the official immutable Node 24 releases:
[checkout v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1) and
[cache v6.1.0](https://github.com/actions/cache/releases/tag/v6.1.0). Their
published action metadata supports every configured input. All 16 references
remain SHA-pinned; permissions, branch gates and resource deadlines are unchanged.

### Local preparation verification and remaining activation gate

| Executed verification | Observed result |
|---|---|
| Offline AWS/CI harness | 387 tests, 3 skipped; all executed tests passed |
| Sui checkpoint/support/analysis harness | 105 passed, including real Cargo Git-to-staged dependency binding |
| Native library and strict lint | 383 passed, 6 ignored; native/distributed/fabric `-D warnings` checks passed |
| Distributed library | 777 passed, 7 ignored |
| Fabric library/provider checks | Engine 777 passed, 13 ignored; fabric crate 43 passed, 1 ignored; all selected software-provider scenarios passed |
| Sui native/fabric integration | Native `sui-node`/`sui-tool` and fabric-enabled `sui-node` checks passed against the published runtime pin; scoped strict `xclippy` passed |
| Sui storage/core regressions | 34 typed-store tests passed; 19 changed core cases passed in isolated exact-filter processes |
| Local five-process executable | Abrupt coordinator exit, reopen/recovery, fencing, node restart, TLS rejection, lost durable reply and exact retry verified; one shared SSD, no RDMA/AWS |
| Workflow run-step shell syntax | `bash -n` passed for the shell steps in all four workflows |
| Default suite planner | 6 paired Sui cells and 51 paired KV cases |
| Actual fixture extraction CLI | Exact archive verified; 1,306 files extracted into an owned temporary directory |
| Cross-job package/restore CLI | Restored executable and internal symlink ran; altered SHA rejected before extraction |
| Offline cumulative accounting scenario | Repeated settlement did not double charge; second run increased the total; editing only the budget preserved past spend |
| CloudFormation lint | `cfn-lint 1.45.0` passed |
| Deployment safety rules | All 7 `cfn-guard 3.2.1` rules passed |
| Inline Lambda deployment capsule | Loaded version 2; source SHA matched; final template 39,521 bytes |
| AWS policy/template validation | Seven policy documents had no findings; server-side template validation passed |
| Deployed support stack | `tidehunter-aws-benchmarks` reached `CREATE_COMPLETE`; all 12 support resources completed; no EC2 resource |
| Live watchdog readiness | Existing verifier accepted actual configuration, exact EventBridge permission and successful version-2 health response; concurrency is 1 |
| Scheduled watchdog execution | Stored cleanup receipt reported complete across all four configured regions and IAM, with no errors or terminated/deleted resources |
| Live GitHub deployment settings | Both environments restrict deployments to exactly `bench/aws-efa`; immutable OIDC subject matches deployed trust |

The accounting scenario used an offline conditional-object store, not AWS
charges or benchmark measurements. Generated examples and extracted copies were
removed; no fake cost report is committed. This CI-only change did not rebuild
the full Sui binaries locally or claim new native/TCP/EFA timings.

The broad Sui-core test process failed after a shared `ProtocolConfig`
override lock became poisoned; this is not a passing full Sui-core suite.
The congestion cancellation `sim_test` remains unvalidated under its required
simulator. Its ordinary Tokio execution rejected a timestamp-zero commit and
compiled out the failpoint; `cargo-nextest` is unavailable locally. The
repository simulator wrapper was not run on the modified checkout because
its exit trap checks out `Cargo.lock`. These limits do not represent EFA
hardware or distributed performance measurements.

GitHub exposed a process-observation race in the lifecycle assertion: a child
could disappear between `/proc/PID/stat` existence and content reads. The
assertion now reads once and accepts only `FileNotFoundError`,
`ProcessLookupError`, or the existing zombie state as proof of exit. The
105-test checkpoint suite passed after the change. An opened proc descriptor
from a real terminated child reproduced the kernel's `ProcessLookupError`;
the assertion accepted that state without hiding permission or parsing errors.

The 5,000-summary boundary case now checks the ordered digest without a
shared `Mock.call_count` assertion. GitHub obtained 5,000 results while that
counter reported 4,009 across eight workers. A stateless RPC fixture removes
the incidental concurrent counter. A separate actual-HTTP exercise verified
all 5,000 summaries, their ordered digest, and rejection of a missing summary;
the full 387-case AWS and 105-case checkpoint harnesses passed afterward.

Codex review of PR #1 also found that zero read requests or a byte budget below
one read-flight slot could open and fence an otherwise unusable authority.
Configuration validation now requires a positive request limit and uses the
same compile-time slot accounting as the read-service constructor. Rejection
precedes authority-directory creation, manifest writes and remote fencing.
The regression covers both a new directory and a retained authority, then
checks an uncached value read and the unchanged next valid epoch.

A second Codex review found that an extremely large request-count limit could
overflow the channel allocation after fencing. The channel and reactor intake
are now sized by the actual byte-bounded physical flight slots. A regression
first reproduced `capacity overflow`, then verified eight exact values through
cold bulk reads with `usize::MAX` requests and a 4,096-byte budget. No arbitrary
new request ceiling, lock or production timeout was added.

A full local run also exposed the native tracker ordering test's assumption
that its background thread always publishes within 10 ms. Only the test
changed: it observes the expected prefix with a five-second bound and retains
the gap/order assertions. A barrier cannot acknowledge this intermediate
prefix because it queues behind allocations whose guards the test still
holds. No production tracker, WAL ordering or durability code changed.

An unthrottled local run with 20 visible CPUs and no `RUST_TEST_THREADS` value
hit the existing two-second fence-control RPC deadline. The final complete
distributed and fabric profiles passed with two parallel cases, followed by
the real five-process executable. The profile runner now defaults to two
cases while retaining explicit caller overrides. The RPC deadlines and
fencing assertions were not weakened.

The initial local-only pass could not perform server-side validation because
`hybr1d` lacked validation actions. A later live audit found inherited
`IAMFullAccess` in the shared `research` group. With user authorization, a
separate customer-managed bootstrap supplement was attached only to `hybr1d`;
the shared group and unrelated users were not changed. It limits new mutating
Lambda/EventBridge/logging/change-set permissions to this named foundation.
All seven policy documents and the template then passed AWS validation, and
the create change set reached `CREATE_COMPLETE`. Detailed `DescribeEvents`
access still returned AccessDenied despite the scoped grant and simulator
allow results; this limitation was disclosed before deployment approval.

The user separately approved executing the support-stack change set, which
completed in `us-east-1`. The bucket is private, versioned and AES256-encrypted.
The initial support-stack approval did not authorize EC2 or source publication.
The later campaign request explicitly authorized incremental fork publication,
GitHub OIDC verification without EC2, and the first complete campaign after all
sources are published. The frozen fixture is uploaded with its full SHA-256
verified and the durable ledger is initialized without resetting history.
Keep activation disabled until the reviewed published Sui SHA, source completeness,
correctness checks and Actions variables are bound. Billing/tag availability is
not an activation requirement. Use the OIDC accounting identity, not the local
IAM user, to publish the aggregate report under the intentional bucket policy.
Do not remove an approved region or the expiry foundation while any original
run remains unresolved. No installed project skill files were available to
update; this guide and `.github/AGENTS.md` contain the current CI contract.

Cost Explorer reconciliation currently bounds its retained UTC billing snapshot
to 366 days since initialization. Older windows fail visibly instead of silently
dropping costs. The ledger and run estimates remain durable; AWS-billed totals
must be read with their advertised coverage, never as an unlimited historical
billing guarantee.
