# Bounded local native / sharded-WAL benchmark

Date: 6 October 2026. Harness:
[`sharded_wal_bench.rs`](../../tidehunter/examples/sharded_wal_bench.rs).
This document specifies the measurement contract. Actual measured results belong
in [LOCAL_BENCHMARK](LOCAL_BENCHMARK_2026-10-06.md) after the integrating run.

Historical scope: the bounds and small-WAL layout below describe the 6 October
campaign. The 16 October harness keeps those defaults, but admits explicit cases
up to 4,096 load batches and 1 GiB checked value history, and adds a Sui-style
WAL-geometry control. See the [current experiment contract](../../experiments/sui-checkpoint/README.md#16-october-targeted-metadata-and-residency-experiment).
Do not apply these historical bounds to the newer targeted campaign.

## What is compared

| CSV backend | Serving code | Write acknowledgement measured |
| --- | --- | --- |
| `native_value_wal_barrier` | Ordinary native `Db`, `WriteBatch`, native cell/index read path | All submitted native batch commits plus a Value WAL file/directory barrier for each window |
| `experimental_local_authority` | New `LocalDatabase`, native pending transaction and shared native overlay/search primitives, five independent Value WAL partitions | All admitted batches in the window are persisted by their partition workers and published to the experimental native cell index |
| `native_async_reference` | Ordinary native `Db` with the same workload/configuration | Foreground native commits return at the existing page-cache/index boundary; this is a different durability mode |

The exact experimental source backend identity is
`experimental-native-cell-sharded-value-wal-local-rf1`. The implementation serves
actual put/delete/get/exists/scan/checkpoint requests using WAL value references;
there is no benchmark `BTreeMap` holding application values. The driver maintains
only deterministic expected generations to audit returned bytes.

The paper's §3.1 explicitly distinguishes foreground page-cache completion from
fsync-based durability. Consequently a ratio against `native_async_reference`
cannot be described as the cost of distribution at equal durability.
[Local Tidehunter paper](../../../middleware/misc/tidehunter.pdf).

The native comparison helper
`Db::persist_value_wal_for_benchmark`, gated by `experimental-distributed`,
synchronizes **all registered native Value WAL files and their directory** after
completed foreground writes. It does not change default native acknowledgements.
The experimental storage barrier synchronizes the containing group file and its
directory. These are conservative local file-persistence comparisons, not a claim
of identical implementation or replication guarantees.

The workload bounds native value history below 96 MiB and uses a 128 MiB native
Value WAL file with 1 MiB fragments. This keeps normal write barriers inside one
native value file, including the mapped lookahead allowance. Index files are
separate. Lifting this bound requires recording the native registered-file count;
otherwise all-files synchronization could unfairly add work to the native case.

## Workload and resource bounds

Defaults:

- Five local partition workers, each using a 1 MiB native WAL fragment and three
  retained Value WAL mappings. The native comparator gets 15 mappings, matching
  the aggregate 15 MiB mapped-value allowance rather than giving each backend
  only three maps. Index configurations still differ and are identified below.
- Values: 1 KiB and 8 KiB. Batches: one and 32 mutations. Admission windows: one
  and eight complete batches. Three backend modes produce 24 cases per repetition.
- 128 initial batches, 32 mixed read/update batches, 32 delete batches, 512 point
  reads for each read/existence phase, and one write across a checkpoint.
- Fixed 8-byte keys are spread by a deterministic bijection over the integer IDs.
  Values contain deterministic nonconstant data derived from key and generation.
  Every backend receives the same logical operation history.
- Value caches and compression are disabled. The native shape has 16 mutex shards
  and 64 cells per mutex. The experimental index uses the same native key routing.
- Temporary database directories are created under the result directory, then
  removed after each case. This avoids assuming `/tmp` is backed by an SSD.
- Hard bounds reject value history above 96 MiB per case, more than 512 load
  batches, more mixed batches than load batches, windows above 32, and point-read
  counts above 8,192. Only the defined 1/8 KiB and 1/32 batch choices are accepted.
- Partition reference metadata has a 16 MiB limit per partition; experimental
  native index metadata has a 128 MiB limit. These are logical charges, not a
  process RSS guarantee. Timed RSS and system swap deltas must accompany a run.

With default input sizes, a largest case writes roughly 40 MiB of value history.
Cases run sequentially and do not accumulate database data. CSV results and logs
remain. Three repetitions rotate backend order to expose order/cache effects.
This remains a small local campaign, not a sustained production-sized workload.

## Phases and correctness checks

| Phase | Operation and verification |
| --- | --- |
| Open | Native open or experimental complete inventory/epoch startup |
| `load_write_window` | Whole batch writes, acknowledgement at the selected window boundary |
| Point gets / missing gets | Verify full returned bytes or absence |
| Exists / missing exists | Verify only membership; reported payload byte count is zero |
| Mixed get / write window | Read each batch's keys before updating them; equal logical read and mutation counts |
| Checkpoint capture / intervening write / checkpoint get | Verify the earlier value through the captured checkpoint after a later write |
| `delete_window` | Delete known keys through whole batches |
| Full scan | Verify sorted keys and complete values against the remaining expected state |
| Maintenance flush | Native control-region rebuild versus experimental derived index image/unload; these perform different maintenance work |
| Post-flush gets | Verify values after the index maintenance path; OS page cache is not cleared |
| Shutdown / reopen | Include explicit drain/close and actual local startup |
| Reopen get audit | Verify every originally inserted key, including deleted keys, after reopening |

The experimental `flush_index` already flushes and unloads; the driver must not
call its alias `unload_index` immediately afterward and accidentally benchmark a
second copy of the same snapshot. Experimental startup currently reconstructs its
index from all retained authority WAL epochs. Native startup uses its own native
control region and replay cut. Reopen ratios therefore include different recovery
work and do not isolate only an index implementation's speed.

## Timing and output contract

The harness saves:

- `samples.csv`: every measured API or complete-window duration, logical operation
  count and application value bytes processed. These bytes are not physical disk
  traffic or network bytes.
- `summary.csv`: sample counts, measured API nanoseconds, mean/p50/p95/p99/max,
  phase wall time and instrumented phase operations per second. Quantiles use the
  nearest-rank convention. Small sample counts constrain tail interpretation.
- `stages.csv`: per-phase deltas of the backend's counters/timing accumulators.
- `campaign.txt`: selected dimensions and explicit topology/durability labels.
- `COMPLETE`: created only after every selected case and reopen audit succeeds.
  A directory without this marker is an incomplete campaign.

For window one, write-window latency is one batch's end-to-end completion time.
For a larger window it is the time to submit and finish **the whole window**.
The harness has no timestamp for each independently completed request and does
not fabricate per-batch latency quantiles from the window duration. Comparing
different window sizes requires considering both throughput and window latency.

Values and expected point/mixed-read payloads are prepared outside their measured
API calls; the main point/mixed query preparation is also outside the phase timer.
API latencies exclude byte validation performed after the call. Instrumented phase
throughput includes loop/timer, handle-cloning and verification overhead, and the
reopen audit includes expected-payload construction. It is not an uninstrumented
maximum-throughput estimate. `measured_api_ns` makes that overhead visible.
Mixed read and write rows share one phase-wall denominator, so their operation
rates are components of the same mixed workload, not two independent campaigns.

Native write stages separately measure batch preparation, batch commit and the
explicit Value WAL barrier. Experimental counters separate validation, dispatch,
caller storage wait, publication, read resolution and value read. These are
instrumented code regions: **storage wait is not measured fsync service time**.
It can include queueing and concurrent worker progress. The benchmark neither
estimates network latency (there is no network path) nor infers device latency
from an aggregate gap. Group/persistence counters exclude bootstrap, sealing and
native background sync activity.

## Commands and evidence

Build locally, using existing dependencies:

```sh
CARGO_BUILD_JOBS=2 cargo build --offline --release -p tidehunter --features experimental-distributed --example sharded_wal_bench
```

Small integration smoke before collecting performance evidence:

```sh
target/release/examples/sharded_wal_bench --output logs/local-smoke-UNIQUE --batches 8 --mixed 2 --reads 16 --values 1024 --batch-sizes 1 --windows 1,8
```

Default matrix with three repetitions:

```sh
target/release/examples/sharded_wal_bench --output logs/local-comparison-UNIQUE --repetitions 3
```

Output directories must be new. Wrap the run with resource measurement and record
`pswpin`/`pswpout` before and after. Retain the build output, command line, stdout,
stderr, source/Cargo hashes, binary hash, hardware/mount information and all three
CSVs. The head commit alone is insufficient while implementation changes are
uncommitted. Do not mark the example or campaign passed from `cargo check` alone.

## What this campaign cannot establish

All five partitions use the laptop's same NVMe device. CPU, page cache and device
queues are shared. This can reveal hashing/copying, directory/index, queueing,
publication and fsync-amortization costs locally; it does not model five independent
NVMe bandwidth budgets, inter-instance RTT, EFA/RDMA, replication or Sui execution.

The experimental backend shares the native pending transaction and ordered
overlay/search mechanisms, with its own typed cell container and explicit snapshot
format. It does not silently replace the ordinary native `LargeTable` or claim
identical index memory density. It retains all versions and WAL epochs under
explicit bounds. Native index cache/maintenance/recovery behavior differs.

An old middleware benchmark is a separate baseline. Ratios against that history
require matching its workload, durability mode, topology and code identity; this
new harness does not rerun or relabel that old architecture. No AWS resources,
cloud upload or cluster launch are required for this campaign.
