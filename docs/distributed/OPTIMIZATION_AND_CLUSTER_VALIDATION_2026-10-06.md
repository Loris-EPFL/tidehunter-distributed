# Local optimization and independent-NVMe cluster validation

Date: 6 October 2026. **Local comparison and the corrected AWS retry completed. All 106 cloud cases and 270 node archives validated. Both runs are fully cleaned up: instances terminated, owned EBS volumes absent and run-owned IAM/network/S3 resources removed. Performance improved, but native parity was not reached.**

This is the latest record. The [first TCP implementation/campaign](LOCAL_TCP_BENCHMARK_2026-10-06.md)
and [its readiness boundary](LOCAL_TCP_READINESS_2026-10-06.md) remain historical.
No earlier binary is silently replaced in those reports.

## What changed and which rules stay required

The architecture remains one index/publication authority and complete application
batches placed on independent native Value WAL partitions. This slice optimizes
actual measured code, while retaining the Tidehunter paper §§3.1–3.4 and §4.4
requirements: complete recoverable batches, values remain authoritative in the WAL,
separate allocated/durable/published progress, exact retry identity, full registered
recovery inventory and no unsafe reclamation.

| Change | Source | Preserved boundary |
| --- | --- | --- |
| Grouped member hashing with existing `blake2b_simd::many::update_many` | `distributed/codec.rs` | Identical BLAKE2b-256 output/domain; complete-batch digest and every member check stay enabled; ordinary native CRC32 and optional key hashing unchanged |
| Bulk byte-buffer serde instead of per-byte sequence handling, shared outgoing payload ownership | `distributed/tcp/payload.rs`, `tcp.rs` | Bincode fixed-integer byte compatibility, bounded frames/counts, immutable owned receive buffers, mTLS and exact reference verification |
| Direct dependent scalar read on the existing node connection | `distributed/remote_db.rs` | One protected per-connection guard; no scheduler handoffs when there is no fanout; durability/fencing/membership unchanged |
| Existing actor workers retained for parallel multi-node reads | `distributed/remote_db.rs` | Bounded scatter and original result ordering; this is not a fully lock-free transport |
| Separate TCP diagnostic clocks and schema-exact external preparation | `examples/sharded_wal_bench.rs`, `examples/support/tcp_cluster.rs` | Main operation timing stays separate from extra-clock profiles; no remote reset or cloud action occurs during preparation |

The Serde byte-buffer specialization follows its [primary serialization guide](https://serde.rs/impl-serialize.html#other-special-cases).
The SIMD path reuses the existing library rather than changing hashes or weakening
integrity. Local compiler/CPU support selects available parallel hashing lanes.
No ordinary native value format, value CRC or native key hash changes in this slice.

The isolated codec probe is supporting evidence, not the full-database result:
seven paired-round median ratios for 32-member grouped versus scalar verification
were 0.704 at 1 KiB and 0.559 at 8 KiB. Absolute timing drift was substantial; final
code also adds a scalar shortcut for one member. See the
[codec evidence directory](../../logs/optimization-2026-10-06/codec/).

## Local correctness and provenance

- Final feature suite: **504 passed, seven ignored**; separate-process fault/recovery
  campaign passed after the direct-read change. No additional ignored test is
  described as passed.
- Twelve no-cloud deployment safety tests cover exact instance/run/account scope,
  refusal to format nonblank/EBS/ambiguous drives, archive path/link/size guards,
  foreign-tag termination rejection, bounded transient export retries,
  permanent-denial fail-fast, actual export read-back verification and an exact
  six-file capacity transform that validates every original field before writing.
  The transform also passed against six actual frozen-generator YAML files:
  [configuration smoke](../../logs/optimization-2026-10-06/capacity-config-smoke.json).
  The three additional regression gates exercise normalized-mtime source archives,
  isolated build target paths, and rejection of identical executables or missing
  candidate CLI capabilities before any binary is distributed. Cargo itself is
  simulated in those focused tests; the real two-archive rebuild is a separate gate.
- The prep-only schema was consumed successfully by both the archived baseline and
  candidate with five real loopback processes and full reopen audits:
  [external-preparation smoke](../../logs/optimization-2026-10-06/deployment-prep-smoke/COMPLETE).
- [Baseline source/build manifest](../../logs/optimization-2026-10-06/baseline/source-manifest.json)
  and [candidate source/build manifest](../../logs/optimization-2026-10-06/current/source-manifest.json)
  bind all Rust/Cargo files, `.cargo/config.toml`, toolchain and executable.

| Build | Release SHA-256 |
| --- | --- |
| Archived TCP baseline | `057df487a161d4f1c9bc0fb2cc63dfc641fa4afcfee66f5ec3ed0dfd107c9e63` |
| Frozen optimized candidate | `706878580cd086535747e8010afcfe317fd54f107f048393bd2c0b78c7cd34f8` |

The [final comparison](../../logs/optimization-2026-10-06/final-comparison/) runs
1/8 KiB × batch size 1/32 × windows 1/8/32 × four backends × two binaries × three
repetitions: **288 verified cases**. Runs alternate baseline/candidate order.
All six baseline/candidate campaign invocations exited successfully, with zero system swap delta
and no surviving sampled child PID. Peak sampled aggregate RSS across runs was
**296,008 KiB**; 50 ms sampling does not prove an exact peak and shared pages may
be counted once per process. See each `*.resources.json` and the
[completion marker](../../logs/optimization-2026-10-06/final-comparison/COMPLETE).

Native async acknowledgement remains a separate reference mode. The following
ratios use the native **file-and-directory persistence barrier** row. Native and
sharded persistence stages differ in grouping/file coverage, as previously documented.

## Matched results

The tables use the median of three per-run **mean API durations** for each shape,
then ratios of those medians. The old/new comparison is within the newly interleaved
campaign; it is not a comparison against different historical machine conditions.
A write sample covers one whole pending window, and a multi-get sample covers up
to 32 keys. Lower duration ratios are better.

| Phase | Local new/old range | TCP new/old range | Current local/native range | Current TCP/native range |
| --- | ---: | ---: | ---: | ---: |
| `load_write_window` | 0.85–1.12× | 0.74–1.05× | 1.21–2.60× | 1.64–3.05× |
| `point_get` | 0.79–1.72× | 0.23–0.50× | 2.17–8.51× | 13.27–63.45× |
| `point_get_many` | 0.73–1.16× | 0.47–1.04× | 2.16–9.61× | 7.28–23.24× |
| `point_get_missing` | 0.74–1.52× | 0.81–1.48× | 0.86–1.74× | 1.02–2.56× |
| `exists` | 0.62–1.56× | 0.70–1.75× | 1.50–3.14× | 2.45–3.50× |
| `exists_missing` | 0.85–1.22× | 0.68–1.44× | 1.11–1.94× | 1.51–2.77× |
| `mixed_get` | 0.62–1.45× | 0.17–0.66× | 1.63–9.99× | 12.04–57.44× |
| `mixed_write_window` | 0.76–1.15× | 0.63–1.18× | 1.34–2.24× | 1.36–3.12× |
| `checkpoint_capture` | 0.58–1.36× | 0.50–1.43× | 0.00–0.05× | 0.00–0.03× |
| `checkpoint_intervening_write` | 0.80–1.26× | 0.77–2.06× | 0.89–1.67× | 0.95–2.08× |
| `checkpoint_get` | 0.47–1.39× | 0.34–1.78× | 0.68–4.49× | 8.72–26.56× |
| `delete_window` | 0.77–1.12× | 0.74–1.40× | 1.05–2.22× | 1.35–2.89× |
| `full_scan` | 0.72–1.40× | 0.27–1.05× | 1.12–5.93× | 1.78–6.31× |
| `maintenance_flush_index` | 0.66–1.08× | 0.85–1.23× | 0.54–1.02× | 0.53–1.31× |
| `post_flush_get` | 0.47–1.69× | 0.18–0.63× | 0.92–6.45× | 8.66–44.67× |
| `shutdown` | 0.74–1.20× | 0.84–1.14× | 5.98–17.67× | 9.66–36.83× |
| `reopen` | 0.81–1.27× | 0.71–1.14× | 20.82–108.00× | 41.97–176.59× |
| `reopen_get_audit` | 0.86–1.20× | 0.17–0.51× | 0.81–4.17× | 9.54–33.47× |

TCP scalar reads improved in all twelve shapes: **50–77% lower mean latency**.
The current means are **23–48 µs**, while they were **71–117 µs** in this same
interleaved baseline. The remaining gap to warm in-process native reads is still
**13.27–63.45×**. This is a substantial transport/software improvement with no
claim of native latency parity.

| Value | Batch | Window | Baseline TCP scalar µs | Candidate TCP scalar µs | Candidate/baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 79.00 | 29.21 | 0.370× |
| 1 KiB | 1 | 8 | 70.92 | 27.52 | 0.388× |
| 1 KiB | 1 | 32 | 91.69 | 24.69 | 0.269× |
| 1 KiB | 32 | 1 | 72.72 | 36.01 | 0.495× |
| 1 KiB | 32 | 8 | 71.94 | 24.94 | 0.347× |
| 1 KiB | 32 | 32 | 81.11 | 23.15 | 0.285× |
| 8 KiB | 1 | 1 | 81.74 | 31.16 | 0.381× |
| 8 KiB | 1 | 8 | 117.47 | 27.15 | 0.231× |
| 8 KiB | 1 | 32 | 92.10 | 34.27 | 0.372× |
| 8 KiB | 32 | 1 | 87.51 | 35.26 | 0.403× |
| 8 KiB | 32 | 8 | 112.87 | 25.68 | 0.228× |
| 8 KiB | 32 | 32 | 106.14 | 47.63 | 0.449× |

TCP durable load windows improved by up to 26%, with one end of the range about
5% slower; current ratios are **1.64–3.05×** synchronous native. Local writes are
mixed (**0.85–1.12×** their same-run baseline), confirming that this slice primarily
improves distributed payload/transport work. Local warm scalar reads remain mixed;
we do not attribute the full cumulative result to SIMD alone. Unchanged native
controls also vary with scheduling and cache conditions.

Reopen remains slow: the new backends replay all retained registered WAL histories
and perform their epoch/inventory protocol. They do different work from native
recovery, so a high reopen ratio cannot be assigned entirely to network overhead.
Cheap logical checkpoint capture is not an accelerated durable replay snapshot.

## Current scalar-read timing attribution

The separate candidate diagnostic pass contains 3,168 live reads per value size
after recovery. These are aggregate stage means, with extra clocks enabled;
they are not the ordinary warm-point-read distribution above.

| Stage | 1 KiB mean µs | 8 KiB mean µs |
| --- | ---: | ---: |
| Index | 0.492 | 0.558 |
| Dispatch / per-connection guard acquisition | 0.059 | 0.063 |
| Complete direct-client wait | 20.328 | 33.895 |
| Request encode, nested inside wait | 0.151 | 0.148 |
| TCP/TLS exchange, nested inside wait | 18.341 | 25.023 |
| Response decode, nested inside wait | 0.136 | 0.213 |
| Record verification, nested inside wait | 1.387 | 8.205 |
| Whole profiled call | 21.088 | 34.721 |

The actor queue/service/return fields are zero for the new direct scalar path.
**These rows overlap and must not be summed.** Exchange includes TLS/socket I/O,
scheduling and node queue/service work; it is not a measured wire-only RTT.
Exchange occupies about 87%/72% of the whole profiled call for 1/8 KiB; record
verification occupies about 7%/24%. This narrows the next attribution question
without justifying removal of integrity checks or a claim that RDMA fixes it.
Per-run raw `remote_read_profiles.csv`, `read_profiles.csv`, `samples.csv`,
`summary.csv` and `stages.csv` are retained for every candidate run.

## Separate configuration experiment: persistence group capacity

The [six-case capacity experiment](../../logs/optimization-2026-10-06/group-capacity/analysis.json)
uses the same frozen candidate with five separate local storage processes:
8 KiB/B32/W32, 128 load batches, 32 mixed batches and 512 point reads. Three pairs
run 1 MiB then 4 MiB fragments, with group caps of fragment size minus eight bytes;
no source change or rebuild occurs. This is separate from the matched matrix.

| Load-phase measure | 1 MiB | 4 MiB |
| --- | ---: | ---: |
| Total durable groups / persistence calls | 48 | 20 |
| Median complete load wall time | 153.32 ms | 109.58 ms |
| Median coordinator storage wait | 94.89 ms | 50.79 ms |
| Median validation time | 47.94 ms | 48.19 ms |
| Aggregate Value-WAL mapping allowance | 15 MiB | 60 MiB |
| Maximum sampled aggregate RSS | 204,952 KiB | 302,272 KiB |

The group count provides direct evidence that the smaller cap forces more
persistence groups for this workload. Twenty groups aggregate five nodes and
four windows; they do not mean twenty globally serial disk barriers. Median load
time falls 28.5%, but paired gains range from 2.3% to 48.0%, with pronounced initial
run drift. This is evidence for the mechanism, not a stable 29% speedup claim.
Validation stays near 48 ms and becomes a larger part of the remaining time.

Other phases are mixed: multi-get, delete, index flush and reopen medians regress.
The increased mapping allowance can also change page-cache and prefault behavior.
Native was not rerun for this experiment, so it provides no native-parity claim.
All six cases pass full-byte/checkpoint/scan/reopen audits, all thirty node processes
exit, and all six runs have zero swap delta. Partitions still contend for one SSD
and its finite bandwidth and queues.

## Independent-NVMe experiment — first attempt failed before measurements

The [dedicated new-fork deployment](../../experiments/aws-sharded-wal/README.md)
uses six identical machines: coordinator/native reference plus five RF1 storage
nodes, each with its own NVMe, in one AZ. The intended procedure rebuilds both
source trees on the controller with the pinned toolchain and `--locked`, then
verifies their cloud-built hashes on every node. The first attempt exposed a
shared-build-directory defect, detailed below; it produced no performance data.

The matrix compares native synchronous and TCP modes for each build, alternating
order, with 1/8 KiB, batches 1/32 and windows 1/8/32. A separate 320-load-batch,
64-mixed-batch, 8 KiB/B32/W32 supplement stresses persistence barriers while staying
within the current 96 MiB history bound. It remains a warm/modest-storage experiment,
not a cold-SSD saturation or Sui benchmark. Window 32 supplies all-five-node fanout;
independent disk statistics must establish actual device use.

The matched matrix retains its original 1 MiB capacity. Two additional current-only
TCP 8 KiB/B32/W32 cases test 4 MiB capacity with 128 load/32 mixed batches in separate
`capacity4m` namespaces, preserving the disclosed 60 MiB mapping allowance. These
are configuration rows with no new native reference. The default campaign contains
96 warm, eight larger barrier-heavy and two capacity cases: **106 API cases**.

Each case uses an explicit fresh namespace. The controller verifies S3 upload and
actual read-back before building, exports live status outside measured phases,
retains full/partial raw results plus verified node logs, and retries transient final
export failures only inside its fixed reserve. Exact tags/IDs constrain termination;
all guests have independent absolute shutdown timers. Parent AWS MCP orchestration
must retrieve and verify the archives and independently confirm full closure,
including owned EBS volumes and run-scoped resources, leaving other users untouched.
The total user budget remains $60; fresh preflight and the run ledger govern spend.

The first run was `th-3040dc77cebd86a8`; its [immutable plan](../../logs/optimization-2026-10-06/aws-run-final/plan.json)
binds the source bundle SHA-256 `1d29a085df43aaee295000ee117abaefd32a20b4609d76f1beb477ad5aedb6c9`.
The package is about 5.8 MB. AWS operations use the official MCP `run_script` and
presigned-URL tools through the cached proxy. The dedicated local bridge adapts
its exception import to the installed MCP SDK; the older middleware is unchanged.
The cached IAM Policy Autopilot supplied a baseline; deployed permissions are
narrowed to this run. Optional AWS Access Analyzer validation was denied by the
account policy, so no successful server-side policy-validation claim is made.
The local deployment boundary suite passed all 20 tests before provisioning.

The [run state](../../logs/optimization-2026-10-06/aws-run-final/state.json) and
[startup receipt](../../logs/optimization-2026-10-06/aws-ssh-start.json)
record five storage instances launched at **12:26:52 UTC** and the controller at
**12:27:09 UTC**, all `i4i.xlarge`. At **12:36:04 UTC**, the startup receipt verified
all six exact instance/account/run identities, bundle hashes, dedicated instance
NVMe mounts and active expiry timers, then recorded persistent controller startup.
The controller deadline remains **13:26:22 UTC**; startup did not extend it.

The operator's SSM inventory/SendCommand calls were denied despite working EC2
access. A temporary exact laptop IPv4 `/32` SSH ingress and EC2 Instance Connect
provided a single controller starter, with ED25519 host keys authenticated through
EC2 console output. The controller's instance role performed the five-node SSM
readiness check; the coordinator was checked locally. Benchmark transport remains
private mTLS/TCP and result retrieval remains through S3. This fallback changes
the startup control path, not the frozen benchmark sources or binaries. The lesson
for the next launch is to verify operator SSM permissions before starting paid
instances, independently of the instance-role policies.
The [revocation receipt](../../logs/optimization-2026-10-06/aws-ssh-close.json)
confirms that the temporary port-22 ingress rule was removed after startup.

### Failure, evidence retrieval and closure

The controller stopped at its first `--prepare-external-case` call, before any
measured case. The [prep stderr](../../logs/optimization-2026-10-06/aws-run-final/extracted/results/warm-baseline-v1024-b1-w1-r0-prepare.stderr.log)
reports an unknown flag. Both source archives and their hashes were correct, but
both cloud build receipts contain the **same** executable SHA-256:
`86b67289b00db939ad2c35f9c7823d4afaab3fcabfc556dd4037d6bf341a9339`.
The [current Cargo log](../../logs/optimization-2026-10-06/aws-run-final/extracted/results/build-current.stderr.log)
finishes in 0.08 seconds. The shared `CARGO_TARGET_DIR` and normalized archive
timestamps allowed Cargo to reuse the baseline executable for the current source
tree. This validates the user's concern about accidentally running old benchmark
code; checking only archived source hashes did not prevent it.

The [failure receipt](../../logs/optimization-2026-10-06/aws-run-final/receipt.json)
records **zero completed cases**, `complete=false`, and an 11,409-byte exported
archive. The archive was retrieved and its actual bytes verified against SHA-256
`ffad75210fd54c2f2902c41dd894b5e8cbd02bb70b5a3a52f18b9140b7dd8c21`.
The automatic cleanup requested termination at **12:37:48 UTC**. Independent
[AWS MCP verification](../../logs/optimization-2026-10-06/aws-end-state.stdout.log)
confirms all six owned instances terminated, zero owned EBS volumes, and both
protected other-user instances still running. Ancillary run-resource cleanup is
tracked separately; no cloud speed comparison is valid from this attempt.

The deployment fix uses a fresh target directory per source tree, verifies the
pair has distinct executable hashes, checks actual CLI capabilities, and executes
the full candidate prep-only command before uploading either executable to the
nodes. It retains the generated configuration hashes in a build-pair receipt.
Rust sources and the validated local comparison binaries have not changed.

### Retry gate and fresh immutable run plan

The [real archive-build gate](../../logs/optimization-2026-10-06/local-cloud-build-gate/COMPLETE)
passed using the exact baseline/current archives with their normalized timestamps,
two empty isolated target directories and real Cargo builds. Only cloud transport
and installation were replaced with local stubs. The real CLI capability checks,
exact external-preparation invocation and all six configuration hashes passed.
The [build-pair receipt](../../logs/optimization-2026-10-06/local-cloud-build-gate/results/cloud-build-pair-verified.json)
records distinct local gate binaries:

| Locally rebuilt archived source | SHA-256 |
| --- | --- |
| Baseline | `b20d92c27465c900ce780d54741027aeee11ae190ed9a2c967115efdca6a38d9` |
| Current | `00dc7b6281a310a0f66d10f1e81aeb5d1e3d2a0aa79a556fbccecce4f17c029b` |

These are validation executables from the fresh build paths, not substituted
hashes for the earlier measured local campaign. The subsequent
[process smoke](../../logs/optimization-2026-10-06/local-cloud-build-gate/process-smoke/COMPLETE)
ran both rebuilt versions in native synchronous and TCP modes: all four cases
passed full integrity/reopen checks, and all ten storage processes exited zero.
The [27 deployment tests](../../logs/optimization-2026-10-06/deployment-retry-tests.log)
also pass: 12 guest and 15 control tests, including the scoped Instance Connect
starter and resumable SSH-rule cleanup. No cloud measurements are inferred from
these local correctness checks.

The [new immutable plan](../../logs/optimization-2026-10-06/aws-run-retry/plan.json)
uses run **`th-68df1dd98b2d60c1`** and bundle SHA-256
`92972488309e69977635a43997b47be12ed3337743c3dc4411ad2a7c6e7d15c0`
(5,777,111 bytes). It retains six `i4i.xlarge` instances, one AZ, a one-hour maximum
guest lifetime and ten-minute export reserve. Its recorded hourly estimate is
$2.109 for the planned fleet/root-volume/public-IP assumptions, with a $5 reserve;
this is an estimate within the overall $60 budget, not an invoice or spending cap.
This retry subsequently completed, as recorded below. The first failed attempt's
artifacts and cleanup evidence remain separately preserved.

## Completed AWS retry: measured results

The [validated analysis](../../logs/optimization-2026-10-06/aws-run-retry/analysis/analysis.json)
accepts **106/106 API cases and 270/270 node archives**, with no missing, invalid or
unexpected cases. These comprise 96 warm old/new/native/TCP cases, eight larger
barrier-heavy cases and two separate capacity cases. Both source trees were rebuilt
on the controller in distinct empty targets with Rust 1.90.0 and `--locked`; the
distinct-binary, CLI and six-configuration preparation gates all passed.
An [independent audit](../../logs/optimization-2026-10-06/aws-run-retry/analysis/independent-audit.json)
also verified the downloaded archive, every archived source member against the
frozen manifests (173 baseline/176 current files), and recomputed quantiles, means,
maxima and throughput from **643,116 raw samples across 2,014 phase rows**.

| Cloud build | Verified executable SHA-256 |
| --- | --- |
| Baseline | `a2dfa85e54393d0f5b9866be3c3cb2e54fdea50f0a898d7ea408bb86b62a0b15` |
| Current | `a4e16e5c46c192fe08241b7f09c35ab0cced6ecb389d1e04752f545d40f32e67` |

The source manifests still identify the same baseline and candidate as the local
comparison. The six machines are identical `i4i.xlarge` instances in one AZ; native
runs on the coordinator's own instance NVMe, and each of the five storage processes
owns a separate instance NVMe. The controller and all storage nodes are RF1.

### All warm phases, using matched synchronous native controls

Each exact shape has two repetitions. First take the median of the two per-run
metrics, compute each shape's old/new and distributed/native ratios, then take the
unweighted median across the twelve shapes. These are **not pooled percentiles**.
Two repetitions provide descriptive evidence, not a statistical-significance claim.
The table explicitly separates API **p50** from whole-phase wall time; the earlier
local detailed table used **mean** API durations. The underlying
[comparison CSV](../../logs/optimization-2026-10-06/aws-run-retry/analysis/comparison.csv)
retains every shape, mean, p50/p95/p99, wall time, throughput and stage counter.

| Phase | TCP/native p50: old → new | New/old p50 | TCP/native phase wall: old → new | New/old phase wall |
| --- | ---: | ---: | ---: | ---: |
| `open` | 10.60 → 10.07× | 0.991× | 10.60 → 10.07× | 0.991× |
| `load_write_window` | 2.97 → 2.61× | 0.857× | 2.88 → 2.50× | 0.876× |
| `point_get` | 151.33 → 89.30× | 0.617× | 92.36 → 57.41× | 0.628× |
| `point_get_many` | 12.04 → 7.49× | 0.678× | 9.33 → 6.20× | 0.700× |
| `point_get_missing` | 1.45 → 1.35× | 1.011× | 1.43 → 1.29× | 0.955× |
| `exists` | 2.11 → 2.16× | 0.987× | 1.86 → 1.91× | 1.014× |
| `exists_missing` | 1.88 → 1.78× | 0.981× | 1.73 → 1.63× | 0.981× |
| `mixed_get` | 169.39 → 97.05× | 0.611× | 10.25 → 6.72× | 0.658× |
| `mixed_write_window` | 2.64 → 2.11× | 0.837× | 10.25 → 6.72× | 0.658× |
| `checkpoint_capture` | 0.01 → 0.01× | 0.871× | 0.01 → 0.01× | 0.871× |
| `checkpoint_intervening_write` | 2.37 → 2.04× | 0.940× | 2.37 → 2.03× | 0.940× |
| `checkpoint_get` | 39.56 → 29.38× | 0.697× | 39.56 → 29.38× | 0.697× |
| `delete_window` | 1.93 → 2.00× | 0.926× | 1.87 → 1.89× | 0.941× |
| `full_scan` | 5.89 → 3.65× | 0.679× | 5.89 → 3.65× | 0.679× |
| `maintenance_flush_index` | 0.62 → 0.60× | 0.997× | 0.62 → 0.60× | 0.997× |
| `post_flush_get` | 92.93 → 57.26× | 0.630× | 69.04 → 42.65× | 0.620× |
| `shutdown` | 10.73 → 10.48× | 0.954× | 10.73 → 10.48× | 0.954× |
| `reopen` | 38.51 → 32.28× | 0.839× | 38.51 → 32.28× | 0.839× |
| `reopen_get_audit` | 68.91 → 43.05× | 0.623× | 37.19 → 22.65× | 0.633× |

Write p50 is one whole pending window, not one mutation. Multi-get p50 covers up
to 32 keys. The two mixed rows share one complete mixed-workload wall timer; their
wall improvement cannot be assigned separately to reads or writes. Single-sample
checkpoint/maintenance/open/scan phases do not establish meaningful p99 tails.
Cheap logical checkpoint capture is not an equivalent durable recovery snapshot,
and distributed reopen still replays all retained histories. Native control drift
and independent medians explain why the ratio of displayed aggregate gaps need
not equal the aggregate new/old ratio.

Warm scalar p50 falls **38.3%**, from **104–154 µs** across shapes to **70–92 µs**.
The corresponding mean API ranges are **111–154 µs** and **72–94 µs**; the median
mean-latency improvement is 37.2%, and its new/native gap is 84.38× rather than the
p50 gap of 89.30×. Scalar p99 improves 31.6% in the aggregate, remaining 99–143 µs.
Native warm in-process reads remain far faster. Warm load wall time improves in
all twelve shapes by 5.5–32.4%, with a median improvement of **12.4%** and a remaining
median synchronous-native gap of **2.50×**.

### Larger workload and the capacity result

The larger supplement uses 8 KiB/B32/W32 with 320 load and 64 mixed batches. Median
complete load wall time falls **367.95 → 254.29 ms** (30.9% lower), improving the
matched native wall gap **2.68 → 1.87×**. The write-window p50 gap is **2.63 → 1.83×**.
Full scan falls **149.33 → 80.10 ms**, and reopen **818.27 → 534.03 ms**; the latter
still performs substantially more retained-history work than native recovery.

The separate 4 MiB configuration does **not** reproduce the local write improvement:
with 128 load batches, groups/persistence calls fall **48 → 20**, but load wall time
increases **106.98 → 115.59 ms** (**8.0% slower**). Storage wait rises **43.80 → 48.73 ms**,
while validation is nearly unchanged (**55.67 → 56.07 ms**). Its full scan improves
6.8%; reopen is 1.4% slower. Two repetitions and the changed 15 → 60 MiB mapping
allowance do not support making 4 MiB the default from this result. Fewer persistence
calls alone are not proof of faster execution on independent disks.

### Did the local improvements transfer?

This comparison uses the same metric and the same twelve warm shapes in each
environment. Local ratios use three repetitions; cloud ratios use two. Each is
paired against its own environment's baseline, with no raw laptop/cloud speed claim.

| Metric | Local new/old | Cloud new/old |
| --- | ---: | ---: |
| Scalar read p50 | 0.369× | 0.617× |
| Up-to-32-key multi-get p50 | 0.571× | 0.678× |
| Durable load phase wall | 0.958× | 0.876× |
| Full scan phase wall | 0.800× | 0.679× |
| Reopen phase wall | 0.902× | 0.839× |

The optimizations transfer, but their effect is workload dependent. Cloud write
and scan gains are larger; scalar-read gains are smaller because an actual remote
exchange remains. The scalar p50 gap to native improves **64.68 → 25.37× locally**
and **151.33 → 89.30× in the cluster**. Additional NVMe devices do not eliminate the
warm scalar network dependency. The opposite capacity result is a concrete warning
against extrapolating a shared-SSD laptop's batching gains into the cluster.

### Remaining time and storage evidence

For the larger load supplement, median coordinator stages change as follows:

| Stage over 320 load batches | Baseline | Current |
| --- | ---: | ---: |
| Validation/encoding | 176.80 ms | 135.17 ms |
| Storage wait, including remote service and persistence | 175.45 ms | 101.99 ms |
| Ordered publication | 12.83 ms | 12.05 ms |
| Aggregate persistence calls across nodes | 120 | 120 |

The largest remaining measured coordinator cost is validation/encoding for this
shape; remote storage wait is also material. These clocks are instrumentation
boundaries, not a claim that every source of CPU work or network delay is isolated.
The coordinator remains a single index/publication authority.

The separate after-reopen scalar diagnostic has 2,112 reads per value size for
warm current cases. It reports these **mean** durations, not main-pass p50:

| Diagnostic stage | 1 KiB | 8 KiB |
| --- | ---: | ---: |
| Index | 0.408 µs | 0.527 µs |
| Dispatch/connection acquisition | 0.062 µs | 0.068 µs |
| TCP/TLS exchange | 69.085 µs | 80.506 µs |
| Record integrity verification | 1.367 µs | 7.919 µs |
| Whole profiled call | 71.939 µs | 90.118 µs |

Exchange accounts for about **96%/89%** of whole profiled time for 1/8 KiB. It
includes network, TLS, socket scheduling, node queue and node service; it is not
wire-only RTT. The nested fields must not be summed as disjoint costs. Scalar
actor queue/service/return clocks are zero because the direct path avoids those
handoffs. More precise node-side attribution is required before assigning this
entire remainder to TCP or predicting an RDMA benefit.

The [node I/O evidence](../../logs/optimization-2026-10-06/aws-run-retry/analysis/node_io.csv)
contains 270 valid whole-case snapshots from the verified instance-store disks:

- **Zero physical read bytes in all 270 snapshots.** These were warm page-cache
  workloads; this run cannot establish cold-NVMe read scaling or disk saturation.
- Every node-case interval records physical writes, **3,211,264–32,337,920 bytes**.
  Independent disks were genuinely used for writes. Intervals include node startup,
  replay, shutdown and idle native phases, so these are not per-API I/O measurements.
- Node and controller swap deltas are zero. Controller whole-campaign disk deltas
  are zero read bytes and 2,275,377,152 written bytes across 448.96 seconds, including
  both source builds, transfers and all benchmarks; they cannot isolate native I/O.
- Controller process RSS sampling and node lifetime high-water marks are retained.
  They do not provide a simultaneous six-machine memory peak; short native cases
  can be missed by sampling. Per-operation CPU usage, device latency and IOPS were
  not captured, so those bottlenecks cannot be quantified from these logs.

This evidence supports keeping the measured software optimizations. It does not
support native performance parity, cold-storage scaling, saturation of five drives,
or a conclusion about RDMA/NVMe-oF. This remains static RF1 TCP engineering evidence,
without a Sui adapter, replicas, high availability or WAL reclamation.

### Retrieved evidence, termination and cost estimate

The [complete receipt](../../logs/optimization-2026-10-06/aws-run-retry/receipt.json)
binds the retrieved **5,281,344-byte** archive to SHA-256
`0baafb8b3c86e4785f33b753f2a630f886026d49f02df9b648775a8d6dfe3126`.
Both length and actual downloaded bytes were verified before analysis. Raw CSVs,
build/CLI/preparation logs, case configurations, process evidence and node archives
are retained under the retry's extracted results directory.

The retry's termination requests were recorded at **13:15:46 UTC**. Independent
[AWS MCP verification](../../logs/optimization-2026-10-06/aws-retry-end-state.stdout.log)
subsequently confirms all six instances terminated, zero owned EBS volumes and both
protected other-user instances still running. The 13:15:46 timestamp and the failed
attempt's 12:37:48 timestamp are termination-request times from the state-transition
reason, not independently measured times when termination finished.
The [first attempt's cleanup receipt](../../logs/optimization-2026-10-06/aws-run-final/cleanup-receipt.json)
records completed cleanup at 13:00:12 UTC with protected instance states unchanged.
The [retry cleanup receipt](../../logs/optimization-2026-10-06/aws-run-retry/cleanup-receipt.json)
records success at **13:29:22 UTC**. Both runs' journals record successful deletion
of their IAM roles/profiles, security group, placement group and temporary S3 bucket
after local archive verification. No run-owned paid fleet or EBS volume remains.

One read-only [AWS CLI S3 progress retrieval](../../logs/optimization-2026-10-06/aws-retry-cli-progress.stdout.log)
was used after MCP transport errors; provisioning, termination and cleanup retained
the scoped MCP control path. Progress is mutable, so a HEAD followed by GET could
observe two different object versions and falsely reject the newer body length.
The operator progress reader now uses one bounded GET. Immutable result archives
still require their recorded byte length and actual SHA-256 verification. The final
[deployment regression suite](../../logs/optimization-2026-10-06/deployment-complete-tests.log)
passes **29 tests: 12 guest and 17 control**, including this progress-read regression.

The [cost ledger](../../logs/optimization-2026-10-06/cost-plan.json) estimates
**$0.8023 total compute, root EBS and public IPv4 for both attempts**, using launch
to termination-request timestamps. It excludes shutdown lag, S3 requests/storage,
transfer, taxes and other overhead; it is not an AWS bill or final invoiced amount.
