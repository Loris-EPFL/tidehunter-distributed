## Protected AWS suite selection

The single paid workflow is `.github/workflows/aws-efa-benchmarks.yaml`.
On a protected `bench/aws-efa` push, `suite.json` selects exactly one suite:
`kv-comparison` (the committed default) or `sui-transport` (the preserved Sui
preparation and campaign). Manual dispatch accepts `suite=configured` or one
explicit suite. Unknown selections and non-manual overrides fail closed.
There is no second paid push workflow and no combined/double-fleet selection.

Selection and both build paths require the exact trusted repository/branch.
`AWS_BENCH_ENABLED=true` and account-side protected-environment approvals remain
required; committing a selector does not activate AWS. KV preparation has only
`contents: read`, no OIDC issuance, no Sui checkout token, and no fixture access.
Its source-bound all-adapter/EFA release and real software API qualification
finish before artifact restoration and fleet-role credentials on another VM.
The Sui preparation path retains its separate input-only identity.

```sh
gh workflow run aws-efa-benchmarks.yaml \
  --repo Loris-EPFL/tidehunter-distributed --ref bench/aws-efa \
  -f operation=benchmark -f suite=configured
# To explicitly run the original Sui suite instead:
# -f operation=benchmark -f suite=sui-transport
```

Both suites share non-cancelling workflow concurrency, `rdma-bench` approvals,
`budget.json`, the S3 CAS accounting ledger, the independent expiry watchdog,
artifact export and exact-owned cleanup. `operation=cost-report` skips all
preparation and allocation and preserves the existing billing/reconciliation
interface. KV uses `experiments/kv-comparison/ci-config.json` with a 10,800-second
absolute fleet lifetime and its export reserve; the Sui config remains separate.
The orchestration step/job allow 240/360 minutes for fleet work, retrieval and
cleanup; fleet-role maximum session duration must admit the requested four-hour
session. The unconditional invoked-campaign cleanup path has a 45-minute bound
and obtains a fresh one-hour OIDC session before reconciliation, rather than
relying on the original session.
These are orchestration bounds, not permission to reset guest expiry.

## Raw provider admission and current campaign authority

The software-fabric CI profile builds the real raw-read executable, runs both
256-byte and 8-KiB two-process software `FI_READ` populations, and passes the
actual producer reports through campaign admission and failure-only evidence
checks. Software success is not evidence of hardware RDMA or cloud performance.
The consumer reads requested device-read policy from `capabilities`, binds its
provider/configuration summaries to the actual root metadata, and keeps native,
emulated and unknown read modes distinct. No legacy root provider/domain fields
or invented hardware capability flags are accepted as substitutes.

Command reconstruction is filesystem-free: offline consumers can validate the
original remote absolute arguments without creating paths on the local machine.
Only the owning launch path writes its canonical fabric configuration before
starting the process. Both peers must complete the exact drain protocol.

Current benchmark artifacts use Rust `Transport::label()` names such as
`ofi-efa-direct-trusted`, not their independent research selection `efa-native`.
`experiments/portable_transport.py::metadata_transport_label` maps the expected
current identities in `transport_floor.py`, `cluster_ram.py`,
`scalar_transport.py` and `checkpoint_kv.py`. Research case IDs and grouping
labels remain unchanged. Explicit frozen `build=baseline` WAL artifacts retain
their old labels; current-only inline floor specs now state `build=current`.

Read-I/O snapshots can contain `authority: null`, as documented by
`ReadExecutorStats::authority`. This means unavailable authority-cache counters,
not zero. Counter projection retains the raw null, omits unavailable numeric
observations, and rejects malformed non-object authority sections.

A local five-process software-provider exercise admitted the actual external
floor metadata and both 100-sample payloads, rejected a mismatched security
identity, and admitted real WAL bounded controls, checkpoint windows and
one/eight-caller cache-hit contention. All five owned processes exited normally.
Evidence is under
`checkpoint-work/portable-cloud-launch-20261027/runtime-label-admission-191419Z/`.
This validates producer/consumer compatibility, not physical RDMA or cloud speed.

Sui CI preparation writes an owned trusted software-provider transport JSON
and passes `--transport-config` to the current replay runner. Admission requires
the result's canonical transport to match that request, the native/remote
database tables and RocksDB semantic audits to agree, and the original hashed
evidence and completion scrapes. The build protocol must match its frozen build
plan rather than the obsolete fixed version 9.

Maintained local read-progress, cluster-RAM, cluster-recovery, pipeline-cache
and source-guided drivers now generate canonical transport files for current
binaries. Explicit frozen-baseline runs retain their old CLI. A bounded real
read-progress run completed its four native/TCP warm/cache controls with no
remaining sampled child processes; evidence is under
`checkpoint-work/portable-cloud-launch-20261027/local-driver-check-193416Z/`.
This checks local integration, not cloud performance or a new full Sui replay.

For the selected KV comparison, current configuration files are authoritative:
`experiments/kv-comparison/ci-config.json` sets the three-hour fleet lifetime,
and `budget.json` sets the rolling weekly cap. Dated campaign notes below retain
historical deadlines, budgets and results; they do not override these policies.

Both suites' `ci-config.json` files restrict automatic hardware selection to
`g6.8xlarge` and `g6.16xlarge`, the two cheapest of the four integrated profiles
in the 28 October 2026 AWS Pricing observations. `g6e.8xlarge` and
`c6id.32xlarge` remain recognized by the tooling but are not automatic fallbacks.
Discovery still sorts compatible candidates across regions by live host cost.
Cluster placement, exact local-SSD inventory, native-read runtime qualification,
budget admission and absolute expiry remain required. If neither selected type
can be allocated within those limits, the campaign fails closed rather than
selecting a larger or more expensive type.


## 24 October bounded SSM metadata and complete-matrix retry

Run `40063575350` passed all 12 real Sui pairs, both native hardware `FI_READ`
payload probes and eight normal KV pairs. Its first cold case did not launch:
the verbose case identity exceeded SSM `SendCommand.Comment`'s 100-character
limit. Only that display label now uses a fixed-size case digest; the full case,
payload, command, namespace and artifact identities remain unchanged. An offline
probe exercised the actual production builder and SDK serialization for all
186 start/stop requests: 60 old labels exceeded the API bound; repaired labels
are at most 42 characters, with no AWS request sent.

The active full-matrix lifetime is **5,700 seconds (95 minutes)**: 85 minutes of
work and the unchanged 600-second export reserve. The validator accepts integer
lifetimes from 3,600 through the existing 7,200-second ceiling. No continuation,
deadline extension, budget override or case/sample reduction is permitted.
All **93 KV pairs / 186 processes, 12 Sui pairs / 24 replays**, two raw read sizes,
32,768 hot reads and 1,024 independently evidenced scalar plus complete32 cold
probes remain. Region/type fallback, cluster placement and the $100 weekly
`budget.json` authority are unchanged.

Duration admission uses actual retained clocks, not summed API time: the earlier
172-pair campaign took 64m45s; the latest eight pairs took 275.307s including
their enclosing suite work. The previously unmeasured cold-probe workload was
then run locally at its full requested count on the admitted current binary:
native 3.97s; five local TCP owners 40.68s, including 25.618s local shutdown.
The two physical-cold phases took 3.096s native and 6.626s TCP; every one of the
1,024 scalar and 1,024 complete32 probes qualified in each mode. This is a
shared-SSD duration/coverage calibration, not an AWS performance prediction.
The 95-minute admission is a forecast with leeway, not a completion guarantee.

The latest failed fleet is closed, all 747 raw objects are locally SHA/size
verified, and its reservation was released. Estimated branch usage is
$76.571582257221, leaving $23.428417742779 under the unchanged current cap.
Retained US pricing projects a roughly $22.81 maximum 95-minute reservation;
the next run must still reprice and pass the live affordability filter before
allocating. Final billing remains unavailable. No Rust/Sui code or pin changes
are part of this orchestration repair.

## 23 October actual checkpoint-completion admission

The same-build local replay must now retain a positive, finite
`checkpoint_completion_latency` histogram for **both native and remote** before
preparation can emit `READY.json` or enable the paid campaign. Validation reads
the exact foreground Prometheus scrapes inside the receipt-hashed replay archive;
loose files, execution-start spacing and the old unobserved
`checkpoint_exec_latency` metric cannot substitute for this evidence.

The timer spans synced-checkpoint admission before the pipeline wait through
queueing/execution, storage commit and publication after the highest-executed
watermark update. It is not certification-arrival latency or raw device service.
The actual observed count is retained independently for each backend: the final
watermark scrape can race the final histogram observation, so no exact1775-count
requirement or fabricated missing observation is introduced.

Derived observations, source-scrape hashes and cumulative-bucket quantile bounds
are saved in the copied verification receipt and `READY.json`. The original
replay receipt and evidence archive remain unchanged. Closed-campaign analysis
also emits `checkpoint-completion-latencies.csv`; bounds are not interpolated raw
quantiles, and fewer than100 observations do not establish p99. Missing, zero,
nonfinite or incomplete histograms fail the local readiness gate.

## 23 October focused hardware populations (current campaign policy)

The current `ci-config.json` selects `kv_suite.scope=hardware-populations`,
not the historical full Cartesian matrix described below. One existing
`bench/aws-efa` fleet is used, with the active absolute lifetime stated above.
No continuation, deadline reset, additional fleet or budget override is implied.
`budget.json` remains the sole $100/week authority.

The deterministic plan is **93 paired KV cases / 186 benchmark processes**:
88 clean pairs (two repetitions, reversed transport order) and five fresh
clock-diagnostic pairs. Each remote case retains its own actual native reference.
All TCP/EFA/EFA-direct/efa-native RPC controls retain coordinator8MiB hits,
coordinator0+encoded-owner8MiB hits, coordinator0+owner0 page-cache controls,
and scalar256B/8KiB positive-physical-I/O controls. Page-cache controls additionally
retain 8KiB/batch32 loads **320 and 3200 as distinct identities** on every transport.
The efa-native registered arm adds all four matched shapes without replacing
any efa-native RMA-OFF baseline. Registered RAM is a separately allocated,
immutable grant population, not the encoded owner cache or disk service.

Both native and remote commands bind `--bounded-hot-reads 32768`, independent
of ordinary `--reads`: 32 hot keys, 32,768 scalar samples, **1,024 complete
batch32 API calls**, and identical fixed `2*read_lanes+1` warm passes. Native
commands never receive RMA configuration. The explicit count participates in
case names and exact serialized policy matching; absent counts preserve legacy
512/16 sampling. Native/cache service stays unclassified without actual evidence.
RPC labels require measured frontend/owner counters and physical-I/O brackets;
budgets, advisory eviction and async submissions are not tier proof.

Cold cases additionally bind `--cold-single-probes 1024` on both backends:
**32,768 single probes across the complete campaign**. Each actual API call has
its own preceding eviction and process/residency bracket. Only an entire
1024-probe population with positive accounted physical reads on the **expected
owner** for every probe, with stable PID/start-ticks/boot identity, may qualify
as physical-cold API latency; preserve every failed/unqualified sample.
Preparation is outside per-API latency but inside the phase wall interval.
Neither its wall rate nor bytes/keys division is NVMe device IOPS/latency.
The older aggregate-positive eviction intervals remain separate controls.

The same cold cases also bind independent `--cold-group-probes 1024`: a separate
`physical_cold_get_many` phase and `cold_group_probes.jsonl` preserve **32,768
complete32-member API calls /1,048,576 verified logical members** campaign-wide.
Probe `i` starts at `mix64(i)%loaded_keys` and selects32 consecutive distinct
present keys with wrap. Authoritative member owners, eviction and before
evidence are prepared outside the one `get_many` timer; every returned value and
member count/order is verified. Every expected owner must show positive process
read bytes for every probe, with stable boot/PID/start identity. Missing owners,
zero/unknown IO and incomplete verification leave the **whole unfiltered**
population unqualified. A physical-IO interval on each owner does not establish
that all32 individual members required device access. Report complete-call
latency, never division by32 as per-member device latency. Native's actual
scalar-loop `get_many` implementation remains unchanged. Scalar probes and legacy
aggregate eviction controls remain independent. Both new probe populations require
the producer's `identity_qualified:true` before/after proof plus positive start
ticks and stable non-null boot/PID identity; a valid after-snapshot alone cannot
establish process continuity. Positive-IO booleans remain honest even when identity
is unavailable, but the whole population then remains unqualified.

Every case still performs real WAL load/mixed/delete writes, checkpoint
capture/intervening writes, flush/barriers, shutdown/reopen and value audits.
`auxiliary-measurements.json` retains complete read/write timing populations,
`write_io.csv` process/PID/start identities and interval byte evidence, rather than
inventing RAM-only durable writes or per-key physical I/O.

Sui retains the identical certified private checkpoint1775 fixture in **12 paired
cells / 24 replays**: ten clean cells across coordinator0/8MiB schedules, plus
two separate efa-native OFF/ON diagnostic cells. Owner cache8MiB is explicit.
Only the distinct `*-efa-native-rma` cells receive the complete shared resident
profile with `require_device_read:true`; ordinary transports and the native
reference remain RMA-OFF. Source/config/namespace bindings and all original
poison, grant, byte-owner, exact-stop, export and termination safeguards remain.
The raw 256B/8KiB FI_READ floor requires actual endpoint native/non-emulated
capability and terminal-drain proof, not merely `fi_info` or instance-type labels.
Natural Sui replay remains explicitly mixed/unqualified for pure registered hits:
actual per-owner FI_READ and wire-Read counters are retained separately. Bounded
RPC profile clocks never become target-handler/FI_READ component timings.

Offline command plans (use the final source-bound guest config for resolved paths):

```bash
python3 experiments/aws-sharded-wal/checkpoint_kv.py plan --config CONTROLLER_CONFIG
python3 experiments/aws-sharded-wal/checkpoint_kv.py plan-budget --config experiments/aws-sharded-wal/ci-config.json --kv-pair-seconds 30
python3 experiments/aws-sharded-wal/checkpoint_kv.py compare OFF_IDENTITY ON_IDENTITY --population registered-arm
python3 experiments/aws-sharded-wal/checkpoint_kv.py compare NATIVE_COLD_IDENTITY REMOTE_COLD_IDENTITY --population physical-cold
python3 experiments/aws-sharded-wal/checkpoint_kv.py compare NATIVE_GROUP_IDENTITY REMOTE_GROUP_IDENTITY --population physical-cold-group
python3 experiments/aws-sharded-wal/checkpoint_kv.py cold-evidence COLD_DIRECTORY --config CONTROLLER_CONFIG --spec COLD_SPEC --mode tcp
```

`plan-budget` is an explicitly **unqualified sensitivity estimate**, not a runtime
guarantee. With assumed600s setup,390s raw-floor reserve and155s per Sui cell
(the observed local swapped native+remote replay), 6,600s available work leaves
3,750s for93 KV pairs: mean40.32s/pair is the break-even bound. Assuming30s/pair
gives5,640s work plus600s export; the extra scalar **and grouped** per-probe
eviction work is included in that assumed mean, **not yet measured**.
Historical run39877444431's observed
campaign step was5,827.296s, but its old sample counts/source/cells are not a
prediction for this campaign. Sui's per-replay hang cap is300s; summing hang
caps is neither a runtime forecast nor permission to extend the absolute TTL.
Do not weaken the existing90s API admission guard or600s export reserve to force
completion. If measured current-source timings do not fit, resolve the lifecycle
with the parent before dispatch rather than dropping populations or reporting
a partial campaign complete.

## 23 October Sui node-supervisor deadline correction

Run `39864855396` rebuilt all six binaries and passed the same-build local Sui
audit. It obtained six `g6.8xlarge` hosts in Frankfurt with cluster placement,
but the first Sui cell stopped before replay: all five storage samplers rejected
their configuration and exited before readiness. No performance comparison was
produced. The exported supervisor log reports configuration rejection; local
reproduction identifies the remaining `cloud_node.py` one-hour ceiling against
the campaign's two-hour absolute deadline.

The Sui node supervisor now accepts at most 7,200 seconds, matching the outer
launch chain. It still uses the supplied absolute UTC deadline and its original
monotonic bound; the controller's export reserve, namespace ownership, sample
limit, failure cleanup and process-group reaping are unchanged. A lifetime
greater than two hours remains invalid.

Local validation runs the production supervisor CLI with a 110-minute deadline
and three isolated executable child fixtures: readiness and resource sampling
must succeed, the deadline must remain unchanged, and controller stop must reap
all children. The lifecycle regression also exercises the two-hour boundary.
This is supervisor validation, not RDMA hardware or Tidehunter performance proof.

## 22 October complete-matrix deadline and timing admission

Run `39830242085` completed all 16 Sui cells, 49 of 172 paired KV
specifications, and eight RAM controls. The original fixed work deadline then
rejected the next backend. Its failed receipt remains failed. All six hosts
and their temporary resources were independently verified terminated.

The next campaign uses **one workflow, one six-host fleet and one complete
matrix**. There is no continuation, cross-run completion or second benchmark
workflow. The configured absolute lifetime is **7,200 seconds**, including
capacity fallback and bootstrap; **600 seconds** remain reserved for export.
Completion still triggers immediate export and exact-owned termination.
Reentry, reboot and candidate fallback cannot reset that deadline.

The matrix retains 16 paired Sui cells, 156 clean KV specifications, 16
dedicated KV diagnostic specifications, and eight separate clean/profiled RAM
controls. The existing diagnostics now run before the clean KV specifications;
the clean relative order, workloads and per-case isolation are unchanged.
This prioritizes the previously missing component clocks, not extra workloads.
The Sui fixture is the frozen certified private chain through checkpoint 1775,
not a mainnet or larger-than-RAM result.

Dedicated diagnostics must retain the native and remote read-profile CSVs and
all applicable node-profile phases. Admission checks case identity, all five
owner records, warm/cold/reopen populations, append/persist and read timings,
EFA CQ evidence and native-message zero-TLS work. Every remote diagnostic
sample must have exactly one cache miss, including the cold pass, which runs
before mixed writes and deletions. Bounded profiled reads can select only some
batch owners: inactive owners remain explicit, while every active owner's
required counters must be positive. After draining, each owner's client/server
request counts must match exactly. The full load must exercise all five owners.
Another owner's clocks cannot conceal a missing active-owner profile.
`COMPLETE` alone is insufficient.
Per-request send/header/body and compatibility inventory-wait clocks that the
multiplexed protocol does not measure are unavailable, not observed zero
latency. Nested clocks are not an additive network-time decomposition.

Local execution exercised native and five-node TCP diagnostic paths, verified
all reopened values, and admitted their actual phase profiles. These nodes
shared one SSD; this proves the evidence path, not EFA hardware or performance.
The complete cloud run still requires six newly built, source-bound binaries
and a successful same-build Sui audit before allocation.

`budget.json` is the sole editable limit; the approved weekly limit is **$100**.
Live fleet estimates cover the full configured lifetime. The additional
reconciliation interval is charged at the hourly rate, not multiplied by the
longer lifetime again. Reservations remain holds, not booked expenses.
Generated daily/weekly/monthly/total reports remain separate from that file.
The launcher role/session ceiling is **10,800 seconds** so credentials cover the
two-hour fleet and cleanup; input/watchdog role ceilings are unchanged.
The campaign step is bounded at 150 minutes inside a 225-minute job, with
unconditional cleanup and artifact retention. This does not extend guest TTLs.
Earlier dated sections below retain their historical settings and results.

## 22 October read-only Pricing throttling

Run `39814797884` rebuilt all six executables and passed its same-build Sui
checkpoint audit. Its first benchmark attempt then failed in `GetProducts` with
`ThrottlingException` before reservation or fleet allocation. Independent AWS MCP
checks found no resources in that run's exact ownership scope.

The Pricing client now uses standard SDK backoff with **six total attempts per
request**. Authorization errors and exhausted throttling still fail closed; no
missing, stale, ambiguous or zero price is substituted. EC2 mutation requests
retain one total attempt. Lambda readiness remains at 12, and S3 remains at three.
Neither the fleet lifetime nor the export reserve changes.

Eleven real-botocore offline fault cases passed, including Pricing recovery with
retained cost evidence, bounded exhaustion, permission rejection, and the existing
EC2 ambiguous-response non-replay checks. These cases execute the SDK serializers,
parsers and retry handlers over a scripted HTTP transport; they do not call AWS.

## 19 October protected CI admission and reconciled costs

Five protected CI attempts rebuilt all six binaries and completed the native and
five-shard loopback replay through checkpoint 1775 with equal store audits.
None allocated a fleet: the first used an invalid IPv4 price filter; the second
exceeded EC2's 16 KiB startup-script limit; the third hit a Lambda health-probe
throttle. The fourth received definitive capacity/quota rejections for all 32
GPU-fleet attempts. The fifth rejected 17 candidates, then lost an S3 journal
response before reaching the CPU fallback; the retained error does not identify
the HTTP method. Cleanup receipts and independent exact-run AWS audits confirmed
no remaining compute, EBS, network, or guest IAM resources.

`ci_campaign.py` selects AmazonVPC prices by `group=VPCPublicIPv4Address`,
then retains only the `PublicIPv4:InUseAddress` usage-type suffix. The queried
IPv4 products have no `productFamily` field. Missing or ambiguous paid rates
still fail closed. Official Node 24 action pins support `allowed-account-ids`.

The Lambda readiness client uses standard SDK retries, limited to **12 total
attempts**, for configuration/policy reads and its read-only health probe. S3 uses
**three total SDK attempts per request**. EC2 and the remaining clients retain
one total attempt; allocation requests are not silently replayed. Exhausted
throttling, authorization errors, function errors, or a mismatched runtime
attestation still block allocation.
The watchdog's reserved concurrency, minute schedule and fleet deadline remain
unchanged. AWS documents [Invoke throttling](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html);
the captured minute-level metrics do not establish the exact throttle cause.

Journal streaming reads permit at most three full-body attempts, close every
response and retain the existing byte bound. After an ambiguous conditional PUT
or a conditional conflict, only reading back the identical submitted bytes
confirms success; that response's opaque ETag becomes the next CAS condition.
A different or unreadable object still fails closed. SDK transport exceptions
with `response=None` retain their original meaning in the launcher, budget,
bootstrap and watchdog instead of being replaced by an attribute error.
Local HTTP fault injection exercised both a committed PUT with a lost response
and an interrupted GET body. This does not extend the original fleet deadline
or establish cloud performance.

The atomic startup capsule uses Python's standard-library LZMA and Base85.
Replaying the actual failed launch metadata reduced the script from 16,871
to 15,989 bytes; the generated decoder restored the exact contract and sources.
The 16,384-byte guard, shutdown-first ordering, source checks, ownership fences,
six-host cluster placement, region fallback, and deadline remain unchanged.
This is local CI-readiness evidence, not a cloud performance result.

The approved hardware list now includes the CPU-only `c6id.32xlarge` fallback.
Discovery and bootstrap share one exact per-type SSD-count plan: G6 has two
drives, C6id has four, and bootstrap uses only one drive per host. A mismatched
inventory still blocks paid admission. The generated foundation adds only this
exact type to the launcher's allowlist; apply its reviewed change set before
merging the fallback configuration. Region fallback, price ordering, the $60
weekly limit and the full Sui/KV suite remain unchanged. Catalog support does
not establish allocation capacity or EFA/direct runtime qualification.
Oregon (`us-west-2`) is the first equal-price regional choice; Virginia, Ohio,
Ireland and Frankfurt remain fallbacks. The generated launcher, guest boundary
and independent watchdog must all include the same five regions before the next
protected-branch campaign. A region's advertised offerings are not a capacity
reservation.

The five-node prefix regression waits for the existing post-snapshot owner event
before checking completed-wait metrics. Commit acknowledgements precede that
diagnostic snapshot. Publication, visibility, recovery, and timing assertions
remain unchanged; no production lock or write-path ordering changed.

`budget.json` remains the only editable weekly-limit file; the latest dated
campaign settings above state its approved limit. Prelaunch reservations prevent
concurrent overspending but are not expenses.
Verified cleanup releases the unused active hold immediately. Unresolved or
ambiguous resource ownership keeps its hold. Original reservations and corrected
receipts remain in the versioned ledger; no counter or history is reset.
Active holds appear in the current UTC day, week, and month, exactly once in the
lifetime total. Original admission reservations keep their historical dates.

Settlement separates priced observed resource usage from future retained-storage
projections and unobserved components. Fixed request-count, transfer-multiplicity,
and future-retention allowances remain admission forecasts, not booked usage.
Unknown billing is unavailable/null, not zero. Late higher AWS bills remain
accounted without adding the same usage estimate and bill twice.

A closed legacy estimate can be corrected through the protected workflow:

```sh
gh workflow run aws-efa-benchmarks.yaml \
  --repo Loris-EPFL/tidehunter-distributed --ref bench/aws-efa \
  -f operation=cost-report -f reconcile_run_id="$RUN_ID" \
  -f reconcile_previous_digest="$PREVIOUS_SETTLEMENT_DIGEST"
```

Use the prior settlement digest from the authoritative ledger. Reconciliation
requires the configured account, owned immutable journal, completed cleanup, and
matching prior digest before any cleanup mutation. It rejects live instances and
unrelated stale requests. Corrections retain bounded append-only history and
use conditional writes; exact replay does not charge the run again.
The generated total/monthly/weekly/daily report is derived separately from that
ledger and is not an editable spend counter.

## 12 October cluster-placement stage timing: completed and closed

Run `th-85e6e8a4dd7328b9` completed in `us-east-1b`: **68/68 API outputs and
260/260 node archives validated**. Six `c6id.32xlarge` instances used the verified
cluster placement group: one coordinator/native control and five separate
own-NVMe shard hosts. The controller requested termination after export at
19:51:49 UTC. Independent AWS MCP verification at 20:06:31 UTC confirmed all six
terminated and no remaining run-owned EBS, network, placement, S3, role, or
instance-profile resources.
No active benchmark instances remained in the 23 enabled regions; four protected
unrelated instances were still running.

[Results, timing scopes, limitations, source bindings, and closure](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#17-completed-cluster-placement-stage-timing-12-october-2026)
are the latest hardware evidence. The earlier `us-east-1a` allocation failed with
`InsufficientInstanceCapacity`, created no instances, and was fully cleaned up.
The previously cancelled run in section 16 remains closed.

Evidence:
[`logs/stage-timing-cluster-2026-10-12-192613/`](../../logs/stage-timing-cluster-2026-10-12-192613/).
The downloaded result archive is SHA-256
`39a95915b6d13b0fde5c9ae005c842ca423672194592bd615aba4f12ae3502b6`.
The source-matched local bundle was rebuilt and checksum-verified in S3; both
revisions and their RAM-floor executables were then rebuilt from isolated
source/target directories on the fleet. Protocol-v5 and v6 peers never mixed.
The original 25-minute expiry and five-minute export reserve were not extended.
This RF1 engine campaign did not rerun process recovery or a full Sui workload.

The mounted MCP tool route was unavailable. Operator AWS APIs used the existing
`mcp_adapter.py` and cached official AWS MCP proxy, not a local AWS SDK or CLI
fallback. MCP-authorized, host-key-pinned EIC/SSH started the existing controller
because operator SSM access was denied. The guest controller used its scoped
instance role for shard jobs, S3 export, and automatic fleet termination.

Offline analysis:

```sh
python3 experiments/aws-sharded-wal/analyze_cloud.py RESULTS_DIRECTORY \
  --profile stage-timing --spread-results SAVED_TRANSPORT_FLOOR_RESULTS_DIRECTORY \
  --output ANALYSIS_DIRECTORY
```

The offline report now retains current-only main cases, keeps scalar/window
diagnostics separate, and marks unsupported timing fields unavailable rather
than zero cost. All 68 outputs and 260 archives passed the corrected analyzer;
125 runner tests ran with two skips. These report-only changes did not alter the
measured executables or raw archives.

Same-source historical RAM controls improved, particularly on EFA. The old
spread run used `us-east-1a`; this cluster run used `us-east-1b` at a different
time. This is observational, not causal placement isolation or wire latency.
Warm single-read parity remains unmet. No further cluster was launched.

## 12 October polling, resident-read, and RAM-response cloud campaign

The `transport-floor` profile is the preceding completed cloud campaign. Earlier
dated sections below describe their archived sources, protocols, and measurements.

**Completed on 12 October:** all 216 API cases and 820 node archives validated.
Both revisions were rebuilt locally and on the C6id fleet. Results and both
cloud-built main executables are stored locally. All six instances are terminated;
all run-owned storage, S3, network, and IAM resources are removed. Protected
unrelated instances were unchanged.
[Measured results, tradeoffs, provenance, and closure](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#15-completed-separate-nvme-tcpefa-campaign-12-october-2026)
include the complete 27-phase comparison and the regression ledger. Warm-read
latency improved with the opt-in resident path, but all-metric parity was not
reached; Busy worsened several durable-write tails. Compute is estimated at
$27.40, with a conservative observation bound of $30.31, excluding other charges.

It compares the previous scalar-transport candidate against the rebuilt
protocol-v5 candidate from section 14 of the
[implementation plan](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#14-local-polling-and-resident-read-follow-through-12-october-2026).
No native engine or Sui changes are included in this candidate.

| Full campaign component | Completed API cases required |
|---|---:|
| Hardware-EFA qualification, both builds and value sizes | 4 |
| Native/TCP/EFA scalar, batched, read, scan, reopen, and diagnostic cases | 188 |
| Persistent authenticated RAM-response processes | 8 |
| Surviving-shard and full-node process recovery | 16 |
| Total | 216 |

There are 164 separately prepared five-shard configurations and 820 required node
archives. Each floor process reuses its nodes for both 1 KiB/8 KiB payloads and
three repetitions, retaining all 48 floor measurements without redundant SSM
start/stop cycles. Policies cover Adaptive/Busy polling crossed with
resident-first off/on. Baseline and current-default native controls are measured
for matching workload/transport/repetition; other current policies reference
those exact measured native controls, not copied or synthesized artifacts.

The campaign keeps the existing RF1 topology: six identical EFA/NVMe hosts in one
verified `us-east-1` availability zone, spread placement to keep hosts separate,
one coordinator/native control and five storage hosts each using one own
instance-store NVMe device. The reviewed choices are `g6.16xlarge` and the
capacity fallback `c6id.32xlarge`. The selected type and zone are immutable in the
plan and guest configuration and must match every actual instance during
admission and archive validation. Earlier profiles retain their historical gates.
Both revisions and native controls run on the same selected fleet; a C6id result
is not an absolute-speed comparison against an archived G6 result.
Native and distributed use the same durable acknowledgment contract. This is
an engine benchmark, not a Sui workload or replicated durability qualification.
The 3,600-second launch-derived expiry includes a 600-second export reserve;
successful or failed completion exports evidence and terminates the exact fleet.

The 12 October G6 attempts in `us-east-1a`, `1b`, `1c`, `1d`, and `1f` returned
`InsufficientInstanceCapacity`; each reconciliation found zero instances.
An alternative needs a new run ID and immutable plan, never an edited failed-run
plan. Cleanup receipts and the attempt ledger remain beside each run's evidence.

AWS MCP hardware and pricing queries verified the C6id fallback: 128 vCPUs,
256 GiB RAM, four 1,900 GB instance-store SSDs (only one used), one EFA interface,
50 Gb/s networking, and spread placement support. Its Linux/shared On-Demand
price is $6.4512 per hour in `us-east-1`: at most $38.7072 compute for six hosts
within the one-hour limit, below the $42 campaign reserve. The source-bound
benchmark matrix, read policies, durability, and evidence requirements do not
change with this capacity fallback.

`freeze_build.py` now rebuilds and binds the main benchmark, RAM-floor executable,
and `checkpoint_recovery_stats` recovery executable. `prepare_bundle.py` verifies
and packages those auxiliary hashes as well as source archives. Guests rebuild
from separate create-only targets; current floor and per-revision recovery
drivers are source-bound. Mixed v4/v5 peers are not permitted.

The full source/binary pins and exact plan are in `transport_floor.py`. The
prepared artifacts and local gates are under
[`logs/transport-floor-aws-2026-10-12/`](../../logs/transport-floor-aws-2026-10-12/):
111 runner tests completed with two skips; actual rebuilt native and distributed
runs each verified 10,240 reopened keys; the external-config RAM floor and
instrumented checkpoint-tail seed/reopen passed the same metric validators.
The local shared SSD and loopback results do not qualify hardware EFA.

The mounted MCP route returned `Unknown tool`; the cached official MCP proxy
through `mcp_adapter.py` executed the preflight, provisioning, launch, monitoring,
and result/build retrieval. Operator SSM inventory and quota reads remain denied.
The scoped EIC starter uses the controller's instance role for SSM, pins its host
key, and revokes temporary exact-IPv4 SSH ingress. After two MCP proxy timeouts
during final non-compute cleanup, the permitted local SDK fallback performed only
cleanup and the independent all-attempts absence audit. No AWS CLI was used.

Before any repeat, create a new source-bound bundle and new run directory:

```sh
python3 experiments/aws-sharded-wal/control.py init \
  --run-dir NEW_RUN_DIRECTORY --bundle NEW_PAYLOAD_DIRECTORY/bundle.tar.gz \
  --campaign transport-floor --checkpoint-scope full \
  --efa-instance-type VERIFIED_HARDWARE --ttl-seconds 3600 \
  --az VERIFIED_ZONE --subnet VERIFIED_SUBNET
```

Use the existing provision/upload/launch/readiness/start/retrieve/terminate/cleanup
phases. Reconcile unknown outcomes rather than starting a replacement fleet.
The analyzer requires PyYAML; guest bootstrap installs `python3-yaml`.

```sh
python3 experiments/aws-sharded-wal/analyze_cloud.py RESULTS_DIRECTORY \
  --profile transport-floor --output ANALYSIS_DIRECTORY
```

The validator checks every declared API case, node archive, process hash, prepared
policy/shape, RAM-floor sample and counter, resident/async read evidence, and
recovery audit. It does not invent a `transport` field in `service-restore.json`.
Keep partial evidence distinct from a completed campaign.

## 11 October scalar latency and transport lifetime

[Implementation, source mapping and correctness contract](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#12-scalar-latency-and-transport-lifetime-implementation).

The new local candidate is
`logs/scalar-transport-2026-10-11/current/sharded_wal_bench`.
Its manifest binds the binary to every Rust/Cargo input and the pinned libfabric
runtime. The October 11 RAM candidate is now the comparison baseline, not the
candidate for these additional changes. No new AWS campaign was launched here.

Blocking scalar reads can drive an idle read lane directly; split-phase reads
keep their asynchronous API. Fabric uses its own CQ/progress path, not TCP data
I/O. Shared TLS/framing, epochs, membership checks and durable receipts remain.
`TcpReadProfile` is a historical type name. Aggregate node `socket` counters on
fabric describe its TLS/libfabric adapter, not TCP data syscalls. The legacy
per-request `exchange_profiles.csv` socket/header/body fields are unpopulated
by the multiplexed reactor in both the baseline and candidate; their zeros
mean unavailable detail, not zero transport work. Use complete exchange/server
spans and aggregate node counters instead.

Cold `reopen` still creates fresh authenticated transports.
`reopen_reused_transport` is a separate, fully audited, same-process reopen using
an explicitly owned transport pool. It is not crash-recovery latency. Reuse does
not retain index/cache/authority state; failed shutdown cannot return connections.
Current diagnostic peers require protocol/ALPN v4 and must be rebuilt together.

Rebuild with the pinned libfabric 2.6.0amzn1.0 installation on the build/runtime
search paths, then compare explicitly against the preserved October 11 binary:

```sh
python3 experiments/aws-sharded-wal/freeze_build.py \
  --output logs/scalar-transport-2026-10-11/current \
  --features experimental-fabric,test-utils
python3 experiments/local-cluster-ram.py \
  --candidate logs/scalar-transport-2026-10-11/current/sharded_wal_bench \
  --candidate-manifest logs/scalar-transport-2026-10-11/current/source-manifest.json \
  --baseline logs/cluster-ram-2026-10-11/current-fixed-timing/sharded_wal_bench \
  --baseline-manifest logs/cluster-ram-2026-10-11/current-fixed-timing/source-manifest.json \
  --baseline-source-sha256 1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28 \
  --baseline-binary-sha256 0a96d28ece36649fec0333ac3f427c3d4b3dab0c9f21fb430b420b5f1dc85d8e \
  --output logs/scalar-transport-2026-10-11/tcp \
  --suite core --transport tcp --values 1024,8192 --repetitions 3 --reads 1024
```

Output directories must be new; do not overwrite prior evidence. Local
`fabric-tcp` exercises the fabric API through `tcp;ofi_rxm`, not an EFA NIC.
Endpoint queue geometry and hardware gains require a separate EFA qualification.

Completed local evidence: 52 matched TCP/native executions, 20 software-fabric/
native executions, and eight cold process-recovery cases. TCP used three paired
repetitions; software fabric used one. All owned processes exited; neither main
campaign recorded swap-in or swap-out. Results, hashes and verification receipts
are under `logs/scalar-transport-2026-10-11/`; the
[measured results and limitations](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#124-local-measurements-and-unresolved-costs)
include regressions as well as gains. Do not equate these results with EFA hardware.

The `scalar-transport` cloud profile compares the preserved October 11 RAM
candidate against a freshly rebuilt scalar/transport candidate. It does not
reuse the October 10 baseline of `cluster-ram`. Baseline and current recovery
drivers come from their own hash-bound source archives; no current API overlay
is applied to the baseline.

`full` selects four EFA qualification cases, 104 native/distributed API executions
with three paired core repetitions and separate diagnostics, and 16 fresh-process
TCP/EFA recovery cases. Cold, pooled same-process and fresh-process recovery
results remain separate. Exact preparation/coordinator/node executable hashes,
source file sets, provider geometry and complete case inventories gate success.
The pinned package version is `2.6.0amzn1.0`; its raw provider ABI version is
`206 << 16`, not the public API encoding of `2.6`.

The deployment uses six `g6.16xlarge` hosts in `us-east-1a`: one coordinator/native
host and five RF1 storage hosts with separate local NVMe. The fixed one-hour
launch-derived lifetime includes a ten-minute export reserve. The controller
exports complete or partial evidence to its private run-owned S3 prefix and
terminates the exact tagged fleet. The operator verifies local archive hashes,
termination and owned-resource deletion independently. Unknown MCP/SSH outcomes
must be reconciled, never treated as permission to start another fleet.

Current campaign preparation is under `logs/scalar-transport-aws-2026-10-11/`.
Its `current-efa-verified` source hash is
`2e694cb0be9fe0738c217b75afb9fe0ba5d2eef170573bd95f092fa40e93dc57`;
the rebuilt benchmark hash is
`7f692391ec6b6acbc5c12c31cb64878764c0ef96d98d82e1b0dfb6a67594db2c`.
The previous local evidence above predates only the EFA provider-version gate
correction. This corrected build passed six additional native/TCP local
executions, with no swap traffic or remaining owned processes, plus 25 fabric
unit tests. The runner suite completed 84 tests: 82 passed, two optional systemd
checks skipped. Neither the local runs nor these checks establish hardware EFA
performance; that remains the cloud qualification gate.

## 11 October cluster RAM and read progress

[Implementation, source map and local evidence](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#10-implementation-and-local-evidence-11-october-2026).
[Authorized hardware results](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#11-authorized-hardware-validation-11-october-2026).
The laptop has five storage processes but only one shared SSD, CPU and RAM pool;
**local ratios and rankings need not transfer to independent NVMe hosts**.
Local `fabric-tcp` is software `tcp;ofi_rxm`; the cloud campaign qualified actual EFA.

The named `cluster-ram` profile compares the frozen October 10 current build with
the new candidate. It does not change historical `checkpoint-async` identities.
It adds cache-capacity curves, scan no-fill, bounded concurrent mixed work, honest
percentile sample counts, exclusive write-window/checkpoint clocks, lifecycle and
transport progress counters. Current peers require **protocol/ALPN v4** for the
expanded buffer/control diagnostics; rebuild coordinator and nodes together.
Preserved v3 baselines run with their own v3 nodes, never mixed with current peers.

### Cache and timing controls

- `--read-memory-allowance-bytes` bounds coordinator cache plus pending-read budgets.
  `--shard-read-memory-allowance-bytes` bounds each node's cache plus read credits.
  Both default to 512 MiB. These are read-memory slices, not total-RSS limits.
  YAML uses `read_memory_allowance_bytes`; external node YAML owns its server settings.
- Larger cache capacities are accepted only within the allowance. Fixed metadata
  is charged; CLOCK admission has bounded effort and bypasses rather than failing
  a valid read. Reserve separate index, application, provider and OS memory.
- Scan requests may use existing cache entries but never fill either tier.
  Reference identity, captured cuts, integrity, epoch checks and recovery are unchanged.
- `--cache-probe-keys` records fill/hot/scan/hot phases.
  `--concurrent-mixed true` overlaps bounded reads and writes on disjoint keys;
  pending-read retirement latency includes time spent doing caller write work.
- Checkpoint time is an exclusive write-window parent. Foreground-checkpoint time
  is its subset, not an extra additive span. Append worker intervals are nested;
  storage wait is not pure network time. Missing old spans are not zero.
- p95 requires at least 20 samples, p99 at least 100. Unsupported cells are blank.
  Use phase/API totals for grouped throughput, not unsupported grouped percentiles.
- CQ gap clocks are opt-in and cover intervals with outstanding local sends,
  not network RTT or receive-only starvation. Connection peaks are not host totals.
- Cache allocation, conservative reply credits and caller-returned logical values
  have separate scopes. They are not an exact unique-byte gauge for arbitrary clones.
  Host/cgroup samples are outside API timers; the collector's inherited cgroup can
  include unrelated members and must not be labelled benchmark-only.

### Completed local campaign and reproduction

The accepted candidate is
`logs/cluster-ram-2026-10-11/current-fixed-timing/sharded_wal_bench`, not the earlier
`current/` binary. Its source/binary SHA-256 values and release build settings are
in `source-manifest.json` and the implementation document. The benchmark,
cluster-check and stats-enabled recovery examples were built locally.

From the repository root, a **new** local output directory can be populated with:

```sh
python3 experiments/local-cluster-ram.py \
  --candidate logs/cluster-ram-2026-10-11/current-fixed-timing/sharded_wal_bench \
  --candidate-manifest logs/cluster-ram-2026-10-11/current-fixed-timing/source-manifest.json \
  --baseline logs/checkpoint-async-aws-2026-10-10/current-recovery/sharded_wal_bench \
  --baseline-manifest logs/checkpoint-async-aws-2026-10-10/current-recovery/source-manifest.json \
  --output logs/cluster-ram-local-repeat \
  --suite all --transport tcp --values 1024,8192 --repetitions 1 --reads 1024
```

`--suite core --transport fabric-tcp` selects the software-fabric core control.
Use the matching libfabric library in `LD_LIBRARY_PATH`. Local software checks
use `${XDG_CACHE_HOME:-$HOME/.cache}/tidehunter/libfabric-2.6.0/lib`;
AWS hardware runs use their separately pinned provider installation. No archived
middleware build output is required. Both binaries and exact baseline
identity are checked before execution. Cases have deadlines, value/physical-read/
timing checks and sampled owned-process cleanup. Do not rerun a completed campaign
merely to collect another output directory.

For abrupt-exit/restart verification, `experiments/local-cluster-recovery.py` takes
`--benchmark`, `--manifest`, `--recovery`, `--output` and `--transport tcp|fabric-tcp`.
Use `target/release/examples/sharded_wal_recovery_bench`, built from the same source
with `--cfg checkpoint_recovery_stats`. The driver checks all frozen source inputs,
records both executable hashes, owns five storage processes, audits zero/128-batch
recovery and exact retries, reaps nodes and removes disposable datasets.
`experiments/local-read-progress.py --suite resident` provides fixed-topology
mapped/positional/async controls; it is not an isolated measurement of one SQE flag.

Completed output is under `logs/cluster-ram-2026-10-11/`: 33 `tcp-verified/` cases,
20 `fabric-verified/` cases, six `resident-policy/` cases, four cases per recovery
transport, separate correctness programs and three `host-resource-pressure/` cases.
The latter exercised the added host/cgroup collection. Preserve the raw CSV/YAML,
commands, logs and source manifests, not only a rendered summary.

### Cloud profile

After explicit approval, the control profile is `--campaign cluster-ram` with
`--checkpoint-scope full|core|capacity|recovery`. Scopes retain qualification;
core and capacity are not accepted as historical checkpoint-async scopes.
The analyzer selects `--profile cluster-ram` and validates the recorded scope,
transport, memory settings, source/binary identity, node topology and raw evidence.
Each revision builds its own recovery example and archives its hash; new recovery
source is not overlaid onto the baseline.

The bundle now includes `cluster_ram.py` and the resource collector `local_compare.py`.
Prepare a fresh bundle from the then-current source before any upload. Keep separate
NVMe shard hosts, matched native controls, actual EFA qualification, diagnostic-off
controls and repeated measurements. Existing deadlines, export reserve, S3 evidence
and owned-resource termination remain required. The authorized October 11 run
exercised upload, cloud rebuild, S3 export and local checksum/evidence validation.
Passing those checks does not mean the read-miss or native-latency gaps are closed.

### Authorized deployment preflight, 11 October

The user subsequently approved the hardware campaign. Execute separate core
(3,600-second TTL), capacity (3,600 seconds) and recovery (2,400 seconds) fleets
sequentially, closing each before the next. Their union retains all 334 full-scope
identities; each fleet repeats its own four qualification identities. Do not pool
those duplicate qualification samples or extend a running fleet's deadline.

Pre-launch review found and fixed four runner defects without changing the Rust
engine or rebuilt benchmark identity:

- `cluster_ram.py` now supplies measured profile medians/means as well as sums/counts
  to the existing cloud summarizer. The former missing median caused `StatisticsError`
  on real saved diagnostic output; all 53 local cases now pass through that analysis.
- `control.py` distinguishes default public all-protocol egress from EFA self-SG
  egress, so resumed provisioning preserves its original mutation intent.
- `mcp_adapter.py` returns a verified task result even on its final allowed poll.
- `guest.py` encloses evidence/archive preparation in the termination `finally`.
  Final ownership is still verified through EC2, but does not require a writable
  audit disk before requesting termination.

All 67 offline tests passed with the optional MCP and real-systemd checks enabled.
Repackage the runner after these fixes; the initial preflight package is superseded.
Retain per-scope immutable plans and final receipts under
`logs/cluster-ram-aws-2026-10-11/`. Compute and resource closure must be verified,
not inferred from benchmark completion or a successful termination request.

### Validated hardware measurements

All three scopes passed the strict analyzer: core 172 cases/440 node archives,
capacity 126/480, recovery 44/220. This is 342 executions, 334 unique identities
and 1,140 verified node archives, with qualification repeated per fleet.
Each fleet used five separate NVMe shard hosts and a sixth native/coordinator host.
Each revision was rebuilt on EC2 from its own frozen source and recovery example.

The candidate source is `1aff4260271bb116477a2ebda6ee2a761961266d7d0fce500bd61a6e10653a28`;
the paired baseline is `84bf88b81410cbc82cfd59eb987b8012a887d9288f3b76e250b9678587efa7d0`.
The package SHA256 is `026883af385b43626657d14d3299ba79d090770c30781ddadc025bb652ec5b02`.
All raw archives, extracted CSV/JSON/logs, build receipts and analysis are in
`logs/cluster-ram-aws-2026-10-11/run-{core,capacity,recovery}-validated/`.
`measurements-validation.json` verifies the exact union; cleanup receipts and the
final AWS audit independently establish resource closure.

The new EFA concurrent-read path and recovery improve substantially. Frontend
all-hit probes reach sub-microsecond medians, but remote misses and scalar writes
remain far from native. Do not report the initial cache-enabled point-get p50 as
all-hit latency: that phase has 2,059 hits and 2,037 misses. The dedicated 128 MiB
hot probes have 4,096 hits and no misses in every repetition before and after scan.

## Historical implementation: 9 October checkpoint and asynchronous reads

[October 9 implementation, source provenance and benchmark controls](../../docs/distributed/CHECKPOINT_ASYNC_READS_2026-10-09.md).
The coordinator now multiplexes authenticated reads on each persistent connection;
nodes default to Linux `async` WAL reads. `mapped` and `positional` remain explicit
diagnostic controls. The new shard cache is disabled by default, finite when
enabled and populated only by demand reads. Checkpoint adoption fences all nodes,
validates exact receipt prefixes and replays the bounded uncovered tail.

The `checkpoint-async` campaign compares the preserved 8 October build with this
implementation. Select `--efa-instance-type g6.16xlarge --az us-east-1a`; the guest
and evidence validator require this matched topology. The planner rejects other
hardware or zones before creating a run plan, not after a paid fleet launch.

Use `--checkpoint-scope recovery --ttl-seconds 2400` only for a recovery follow-up.
It retains fresh four-case EFA qualification and all forty paired recovery cases,
but does not repeat completed performance/cache-pressure cases. The scope is
immutable in the plan, propagated to guests and validated from the result archive.
Keep separate fleet receipts; a complete recovery scope does not relabel an
earlier partial full campaign as complete.

The recovery example requires `experimental-distributed`, like the main
benchmark. Cargo skips it when checking native/default-feature targets.

Build coordinator and nodes from the same protocol-v2 source. Do not mix this
binary with a protocol-v1 node. Use `--tcp-wal-member-read async` and
`--tcp-shard-cache-bytes BYTES` when preparing an external case. Remote YAML owns
server settings after preparation; the benchmark rejects conflicting overrides.
Keep all caches disabled for cold passes. The local `pressure` suite verifies
frontend-only, shard-only and combined budgets smaller than the working set.

The 8 October sections and cloud matrices below describe their recorded sources,
settings and results. Their explicit mapped/positional cases remain controls;
they are not measurements of the new asynchronous implementation.

## Historical implementation: 8 October bounded read progress

**The coordinator owns cache misses.** It resolves the key at the published cut,
captures the immutable record reference, asks the owning shard for the value,
verifies the reply, and fills its exact-reference cache before returning it.
The new split-phase API changes scheduling, not ownership or value placement.

### API and limits

`RemoteDatabase::submit_get` returns an owned `PendingGet` without retaining a
coordinator borrow. `poll_get` returns `None` while pending, `Some(None)` for a
completed absence, or `Some(Some(value))` for a completed value. `finish_get`
waits for one handle. A completed handle cannot be consumed twice.
The existing `get` and `get_at` remain blocking convenience calls.

- An independent cache hit can complete while a miss waits. Hit handles do not
  allocate or copy key/value data. Misses use separate persistent authenticated
  read lanes, selected by outstanding work, instead of blocking write connections.
- Submission captures the read version. An intervening overwrite or delete does
  not change that handle's reference. Verification and observed authority failure
  checks still precede return/cache fill.
- Request and byte credits include queued, active and completed-but-unconsumed
  miss work. `read_backpressured` identifies temporary admission pressure; finish
  or drop earlier handles before retrying. An individually oversized request is
  not temporary pressure.
- Dropping a handle cancels delivery, not active transport ownership. Workers keep
  their credits and buffers until completion. A late observed failure still
  poisons the authority. Shutdown joins the lanes before releasing authority.
- `get_many` retains shard grouping. It and ordered scan split work into fitting
  waves when count/byte budgets are small. They drain their own work to progress;
  caller-owned outstanding handles can cause nonpoisoning backpressure. Values
  already returned to a caller-sized collection are outside pending-read credits.
  Scan lookahead cannot reserve all credits ahead of an incompletely admitted page.
- Queue time counts against the absolute deadline. No retry, rerouting or
  transport fallback was added. No coordinator cache/state mutex spans a remote
  wait; the authority's directory lock still remains held for its lifetime.

| Configuration | Default | Bound / meaning |
|---|---|---|
| `RemoteDatabaseConfig.read_lanes` | 2 per shard | 1–8 persistent read connections |
| `max_pending_reads` | 256 | 1–4,096 admitted miss requests, across all lanes |
| `max_pending_read_bytes` | 64 MiB | 1 byte–256 MiB conservative retained-work budget |
| `NodeConfig.read_workers` | 2 | 1–8 shared server I/O workers; 1 is the serial control |
| `NodeConfig.wal_member_read` | `mapped` | `mapped` or buffered `positional` member reads |

These fields have serde defaults for existing YAML. The server validates the
entire reference set and response bound before I/O, reads members in its bounded
shared Rayon pool without retaining the inventory lock, preserves response order,
and rechecks the epoch after I/O. Existing connection/frame limits also bound
queued server work.

`positional` uses member-range `read_exact_at`, not `O_DIRECT` or io_uring. `mapped`
uses retained mappings where available and member-sized mappings for sealed
history. Native whole-frame reads are unchanged. Mapped remains the default:
the measurements below do not support a universal positional-I/O replacement.

The benchmark exposes `--read-window` (1–256, default 1), `--tcp-read-lanes`,
`--tcp-max-pending-reads`, `--tcp-max-pending-read-bytes`,
`--tcp-server-read-workers` and `--tcp-wal-member-read`.
Server controls apply to local startup and `--prepare-external-case`; they are
rejected with `--remote-config`, where already-running nodes own those settings.

### Local mapped/positional and concurrency comparison

Run from the fork root, with the release binary's libfabric runtime available:

```sh
python3 experiments/local-read-progress.py \
  --candidate target/release/examples/sharded_wal_bench \
  --output logs/read-progress-local
```

The default local-only plan rotates 11 configurations across three repetitions
and 1/8 KiB values: **66 cases**, B32/W32 writes, 320 load batches, 64 mixed
batches and 4,096 point reads per phase. `--suite cold|warm` selects a subset;
`--transport fabric-tcp` exercises the software provider, not hardware EFA.
Every case requires full-value/reopen completion. Every cold phase requires
positive physical-read evidence and the complete per-node evidence inventory.
The runner records binary identity, commands, resource samples and a comparison
CSV, with a 600-second process deadline per case.

`point_get_window` and `cache_evicted_get_window` exercise separate scalar
submission/completion, not `get_many`. Each latency sample spans one request's
submission through retirement, including queue delay. The driver polls new
handles immediately and drains older pending handles when the window is full
or admission is blocked. Native executes its scalar API serially; this is **not
a matched-concurrency native comparison**. Use phase wall time for throughput;
do not divide window latency percentiles by window size. Grouped-read samples
still cover the complete grouped call.

Measured cold-start window phase, median of three repetitions. Wall time covers
4,096 reads; p99 is the median of the three per-request p99 values.

| Path: lanes / server workers / read window | 1 KiB wall ms | 8 KiB wall ms | 1 KiB p99 ms | 8 KiB p99 ms |
|---|---:|---:|---:|---:|
| Native serial API | 19.19 | 204.51 | 0.117 | 0.147 |
| TCP mapped: 1 / 1 / 1 | 528.34 | 886.44 | 0.441 | 0.652 |
| TCP mapped: 4 / 1 / 32 | 57.87 | 124.31 | 1.910 | 3.470 |
| TCP mapped: 4 / 4 / 32 | 74.42 | 135.93 | 2.080 | 4.107 |
| TCP positional: 1 / 1 / 1 | 576.95 | 968.95 | 0.434 | 0.644 |
| TCP positional: 4 / 1 / 32 | 75.26 | 201.94 | 1.871 | 2.632 |
| TCP positional: 4 / 4 / 32 | 66.37 | 96.74 | 3.680 | 1.872 |

Bounded overlap reduces phase time, but larger windows increase request tails.
Four server workers did not improve mapped reads on this shared SSD. At 8 KiB,
positional/four-worker reads used 35.61 MiB of physical reads versus 86.90 MiB for
mapped/four-worker reads. This is a cold-start workload: the page cache warms
within each phase, and the data fits RAM. It is not cold I/O for every request,
NVMe saturation, independent disks, hardware EFA or native-parity evidence.

The warm 8 MiB frontend-cache control also had misses. At 1 KiB, its window p50
was 1.855 µs but p99 was 1.779 ms. At 8 KiB, p50 was 617.513 µs and p99 was
1.415 ms. Cache capacity/conflicts and pending misses still matter; a hit median
does not establish miss or tail performance.

All 66 TCP/native cases passed, with peak sampled process-tree RSS 327.13 MiB,
zero system-wide swap activity during their intervals, and no sampled child
process left behind. Another 22 smaller software-fabric/native cases passed
value/reopen and cold-I/O checks. **Software-fabric memory remains expensive:**
peak sampled tree RSS was 8,446.77 MiB and one case overlapped six system-wide
swap-out pages. Those cases are correctness evidence only, not performance
evidence. Each read lane has a separate authenticated connection/fabric endpoint;
the pending-byte budget does not bound provider allocations, caches or total RSS.
No hardware-EFA memory conclusion follows from this software-provider result.

### Verification and paper basis

- 547 library tests passed; eight remained ignored in the full run. The explicit
  ignored software-fabric authentication/lost-response/recovery test also passed.
  Regressions cover blocked-miss hit progress, same-shard overlap, captured read
  versions, cancellation, retained credits, deadlines, fencing and response order.
- Four synchronous low-budget regressions failed before the fix and passed after
  grouped-wave admission. A real 8 KiB TCP case also passed all read/reopen checks
  with one pending request, a 32 KiB pending-byte budget and read window 32.
- Separate-process TCP and software-fabric drivers passed coordinator death,
  complete recovery, missing-node refusal, partial fencing, node restart, exact
  retry, stale epoch and mTLS negative cases.
- The actual CLI rejected 13 invalid/misapplied read-control combinations before
  creating output. External generation retained the requested frontend limits
  and positional/four-worker policy in all five node configurations.
- 37 deployment regression tests passed. The updated analyzer still accepts the
  archived 68-case/170-node EFA campaign. Release examples, changed-file formatting
  and strict library/test/example Clippy passed with and without fabric.
  All-target Clippy remains blocked by unrelated `manual_is_multiple_of` in
  `tidehunter/benches/large_table_benchmarks.rs:169`. Previously reported unrelated
  whole-tree formatting issues in `remote_db/read_cache.rs` and
  `remote_db/tests.rs` were not changed.

A second, parallel research agent used QMD to search and read primary-paper
sections. The implementation applies these mechanisms, not their performance
numbers or weaker/different durability contracts:

- [Tidehunter §3.2](https://arxiv.org/abs/2602.01873v2): resolve a miss, retrieve the
  WAL value, then fill the cache. Its standalone flow supports the ordering;
  assigning that responsibility to this fork's coordinator is a design inference.
- [eRPC §§3.1–3.2, 4.2.2–4.3.1](https://www.usenix.org/system/files/nsdi19-kalia.pdf):
  keep long work off short dispatch paths; retain transport ownership and bound
  outstanding work. QMD `#f39b71`, lines 93–110, 183–194 and 205–212.
- [ReFlex §§3.1, 4.1](https://anakli.inf.ethz.ch/papers/reflex.pdf): separate storage
  submission from completion so other requests progress during flash access.
  QMD `#f809a2`, lines 65–94 and 162–185.
- [KVell §§5.4–5.5](https://raw.githubusercontent.com/BLepers/KVell/master/sosp19-final40.pdf):
  return cached data immediately and bound asynchronous disk work. It uses Linux
  AIO, not io_uring; its mmap comparison does not establish a universal winner.
  QMD `#2d5a45`, lines 218–236 and 280–294.
- [DBMS io_uring study §3.3](https://arxiv.org/pdf/2512.04859): changing the I/O API
  without independent requests does not create overlap. QMD `#a8daf4`,
  lines 125–158.

Evidence: [commands, raw samples, summaries and verification receipts](../../logs/read-progress-2026-10-08-g4yezp82/).
Measured release SHA-256:
`127367c18e51c834c514746d60435d68c34e6a9f267419080952f23fc02f4398`.
Placement, WAL durability, complete-batch visibility, authentication and full
value verification are unchanged. This local slice remains bounded RF1, without
automatic multi-host authority leases or Sui integration. It did not launch a
cloud run; the later hardware results are below. The referenced project skill
files remain absent; no existing skill file was made stale.

## Hardware campaign: bounded read progress

Select `control.py init --campaign efa-progress` for the matched hardware
comparison. Use the preserved successful 7 October EFA source as baseline
(`f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398`).
Freeze the candidate with `experimental-fabric,test-utils`. The controller
rebuilds both source archives independently; uploaded executables are provenance,
not substitutes for the cloud builds.

`--efa-instance-type` selects hardware only when creating a new immutable plan.
The default is `c6id.32xlarge`. The alternative is `g6.16xlarge`, with no GPU use.
Both support [hardware RDMA](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html).
G6 uses [spread placement](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-strategies.html)
to keep all six instances on distinct hardware. C6id uses cluster placement.
Every compared build runs on the same selected type and placement strategy;
results from different fleets are not pooled.

| Hardware | vCPUs/host | RAM/host | Network | Local SSDs/host | Placement | Compute USD/host-hour |
|---|---:|---:|---:|---|---|---:|
| `c6id.32xlarge` | 128 | 256 GiB | 50 Gbit/s | 4 × 1,900 GB | Cluster | 6.4512 |
| `g6.16xlarge` | 64 | 256 GiB | 25 Gbit/s | 2 × 940 GB | Spread | 3.3968 |

The AWS API hardware descriptions and Linux/shared On-Demand prices were checked
on 8 October in `us-east-1`. Each guest uses exactly one blank instance SSD,
not RAID. Root volumes, public IPv4, S3 and transfer costs are additional.
`logs/efa-progress-2026-10-08/g6-rdma-preflight.json` preserves the G6 API responses.
Operator MCP requests allow 30 seconds to connect, 90 seconds to read and
95 seconds per tool call, with a 150-second outer bound. This accommodates the
observed MCP handshake/tool timeouts. It does not extend any EC2 deadline.
After an interrupted mutation, reconcile the exact run scope before retrying.

| Stage | Native/distributed pairs | API cases | Node archives |
|---|---:|---:|---:|
| EFA qualification, 1/2/4 read lanes and 1/8 KiB values | 6 | 12 | 30 |
| Previous/current TCP/EFA, current cache off/on, three repetitions | 72 | 144 | 360 |
| Mapped/positional and lane/worker/window controls, three repetitions | 36 | 72 | 180 |
| Separate TCP/EFA diagnostics | 4 | 8 | 20 |
| Total | 118 | 236 | 590 |

Qualification uses cache0, 64 load batches, 16 mixed batches, 256 reads and
B32/W32. Main cases use 320 load batches, 64 mixed batches and 4,096 reads.
The core matrix includes B1/W1 and B32/W32. Cold controls use
`(read_lanes, server_read_workers, read_window)` of `(1,1,1)`, `(4,1,32)` and
`(4,4,32)` for each member-read policy. Case and native/distributed order rotate.
Equivalent native controls retain separate identities and raw measurements.

`--native-read-workers N` accepts 1–32, default 1. Values above 1 use caller-owned
workers sharing one native Db and cache. At most `min(N, read_window)` requests
remain unretired. The current hardware control uses N=32; scalar `get` and
`get_many` remain unchanged. Window samples include admission-to-retirement
queue delay. Worker startup and teardown are outside phase timers. The preserved
baseline has no concurrent-window phase; the analyzer does not invent one.

`--read-resource-evidence true` requires Linux and `test-utils`. Outside timed
phases it records process RSS, OS-reported high-water RSS and process-wide CPU time
in `read_resources.csv`, at `opened`, `loaded`, `point_get_window` and, when
enabled, `cache_evicted_get_window`. Storage snapshots use an authenticated,
epoch-checked diagnostic request. They do not allocate extra read lanes.
The loaded-to-window CPU interval also includes preceding scalar phases; it is
not window-only CPU.

Linux [`VmRSS` and `VmHWM` are approximate](https://man7.org/linux/man-pages/man5/proc_pid_status.5.html).
The first hardware run exposed decreasing `VmHWM` after WAL page eviction.
Raw CSV values remain unchanged. Analysis retains each process's largest
observed peak, still checks every sample against the RAM bound, and still rejects
decreasing CPU counters. A sum of observed process peaks is neither a simultaneous
cluster peak nor a continuous measurement.

Broader cases cannot start before the archived qualification evidence passes.
The gate requires exact six-host ownership, five storage partitions, real EFA
device traffic, full-value/reopen completion, every expected process/stage/PID,
zero observed guest swap and every reported process RSS/peak at most 80% of host RAM.
Every cold phase needs positive physical reads for each participating process
and at most 10% resident file bytes before reads. This is a cold-start workload:
pages can warm during the phase, and the bounded dataset fits RAM.

Use `analyze_cloud.py RESULTS --profile efa-progress`. It repeats qualification
checks and rejects cache, source, policy, command, resource or artifact mismatches.
Server worker/member settings must be applied during external configuration
preparation, not to a client connected to already-running nodes.
The six identical EFA machines have an immutable launch-time expiry. Select the
shorter reviewed lifetime only when creating a new plan; it cannot extend an
existing run.

| Lifetime option | Maximum lifetime | Export reserve | EFA budget reservation |
|---|---:|---:|---:|
| Default `--ttl-seconds 3600` | 60 minutes | 10 minutes | $42 |
| Bounded retry `--ttl-seconds 3000` | 50 minutes | 10 minutes | $35 |

This compares one native host with a coordinator plus five RF1 storage hosts,
not equal aggregate hardware or cost. EFA remains two-sided authenticated
message transport, not one-sided GET.

### Completed campaign: 8 October 2026

Run `th-7190585f753496d8` completed **236 API cases and 590 verified node
archives**, with no missing cases, evidence mismatches or observed swap.
All 400 EFA node cases passed the hardware-traffic gate. Qualification passed
before the main matrix started. Full values, reopen, cold physical I/O and
per-process resource evidence passed the final local analysis.

The full comparison used six `g6.16xlarge` hosts in `us-east-1a`, with spread
placement and one local NVMe device per host. GPUs were unused. Native used one
host; distributed used a coordinator and five RF1 storage hosts. Both source
archives were rebuilt independently on the controller with Rust 1.90 and
libfabric `2.6.0amzn1.0`.

The earlier C6id run stopped after qualification because the analyzer incorrectly
required monotonic `VmHWM`. Its unchanged archive passed after the gauge fix.
It is not pooled with G6. Subsequent C6id requests in all five supported zones,
and the first G6 zone request, failed capacity allocation without creating
instances.

#### Scope and provenance

Unless stated otherwise, tables use B32/W32, cache disabled, 4,096 point reads,
and medians of three rotated repetitions. B32 loads 320 batches of 32 values;
B1 loads 320 single-value batches. A reported p99 is the median of the three
per-run p99 values, not a pooled percentile or a significance test.

Native window reads use 32 workers sharing one Db. Their queue/dispatch cost
is included. These numbers are not the maximum attainable native throughput:
direct scalar calls avoid that worker pool. Distributed window reads use four
lanes per shard; the warm control uses one server I/O worker. `get_many` latency
covers the whole 32-key call, not one key.

| Build | Source tree SHA-256 | Cloud executable SHA-256 |
|---|---|---|
| Previous | `f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398` | `f40174506ab50c1012f457ac978662e8d58ef606a8c19a0df4a42cbd32f1adb8` |
| Current | `97f18475bb15b8ad7ae20ac799fd2a39d5399ccdbc7d3ce8c88d5950730ec5b1` | `e4a9b6d1aaa9e87ba929d75574a9a7007e0144229cf22fc7425e2b4da8f506ec` |

The verified result archive is 50,911,177 bytes, SHA-256
`c550a1f329963dbaa882c7cca78d414997c4cd01c2ef9e89034082f1483b0b15`.
It contains 6,364 entries, expanding to 926,318,106 bytes.
[Raw archive, receipts and validated analysis](../../logs/efa-progress-2026-10-08/aws-run-g6-a/)
are retained locally. `analysis/comparison.csv` preserves every shape and
separate matched native control; `analysis/analysis.json` records all gates,
profiles and observed phase-wall regressions.

#### Single host versus distributed reads

| Warm metric | Value | Matched native | Distributed EFA |
|---|---:|---:|---:|
| Scalar p50, µs | 1 KiB | 0.811 | 289.057 |
| Scalar p50, µs | 8 KiB | 2.980 | 325.328 |
| Window32 throughput, thousand reads/s | 1 KiB | 229.285 | 68.092 |
| Window32 throughput, thousand reads/s | 8 KiB | 204.996 | 62.388 |
| Window32 p99, µs | 1 KiB | 192.095 | 760.738 |
| Window32 p99, µs | 8 KiB | 234.585 | 849.131 |
| 32-key `get_many` p50, µs | 1 KiB | 21.780 | 540.473 |
| 32-key `get_many` p50, µs | 8 KiB | 103.032 | 739.838 |

EFA is faster than TCP for these uncached reads, but does not reach native
latency or concurrent throughput. TCP window throughput was 46.683/42.409
thousand reads/s for 1/8 KiB; its separate native controls were 226.448/200.745.
The same EFA comparison is 1.46/1.47 times TCP throughput.

Cold-start 8 KiB controls show why scalar, grouped and concurrent APIs must
remain separate:

| Member policy | Lanes / server workers / window | Native thousand reads/s | EFA thousand reads/s | Native window p99, µs | EFA window p99, µs |
|---|---|---:|---:|---:|---:|
| Mapped | 1 / 1 / 1 | 7.815 | 2.637 | 274.866 | 747.938 |
| Mapped | 4 / 1 / 32 | 102.992 | 37.051 | 608.965 | 2573.800 |
| Mapped | 4 / 4 / 32 | 100.679 | 45.782 | 670.946 | 1427.484 |
| Positional | 1 / 1 / 1 | 7.926 | 2.470 | 277.926 | 556.733 |
| Positional | 4 / 1 / 32 | 101.690 | 34.840 | 641.975 | 1617.328 |
| Positional | 4 / 4 / 32 | 100.812 | 46.479 | 636.985 | 1426.714 |

Four server workers improve the 8 KiB cold window throughput over one worker
by 24% with mapped reads and 33% with positional reads. Changing both lane count
and window size is not an isolated lane-count experiment. The 1 KiB mapped
window gains no throughput from four workers: 61.814 versus 61.267 thousand
reads/s. These results do not justify changing the mapped default.

With positional reads and four workers, cold 8 KiB `get_many` p50 is
**0.874 ms EFA versus 3.035 ms native**. This grouped-call advantage does not
extend to the concurrent scalar control above. That window records median
physical reads of 35.605 MiB across the shards versus 41.387 MiB native.
Each required reader passed the positive-I/O gate. This is an initially evicted,
RAM-sized dataset that warms during a phase, not sustained cold NVMe saturation.

#### Cache controls

Both configured caches have an 8 MiB nominal budget, with the different
accounting policies described below. The measured hit fraction matters more
than the configured number.

| Value | EFA window hits / requests | EFA window, cache off, thousand reads/s | EFA window, cache on, thousand reads/s | Repeated scalar native p50, µs | Repeated scalar EFA p50, µs |
|---|---|---:|---:|---:|---:|
| 1 KiB | 3,861 / 4,096 (94.26%) | 68.092 | 833.129 | 0.190 | 1.500 |
| 8 KiB | 342 / 4,096 (8.35%) | 62.388 | 67.828 | 3.430 | 323.448 |

The 1 KiB gain is a hit-heavy workload result, not a transport speedup.
The cache-enabled native window controls are 251.414/203.454 thousand reads/s
for 1/8 KiB, including worker-pool dispatch. They must not be used to claim that
a coordinator hit is intrinsically faster than a direct native hit.
At 8 KiB, the low frontend hit fraction leaves the miss path dominant.

#### Writes, startup and regressions

| Value | Write batch / window | Native EFA-paired load, ms | EFA load, ms | Native TCP-paired load, ms | TCP load, ms |
|---|---|---:|---:|---:|---:|
| 1 KiB | 1 / 1 | 35.173 | 137.757 | 37.187 | 200.078 |
| 8 KiB | 1 / 1 | 77.839 | 172.008 | 76.265 | 255.776 |
| 1 KiB | 32 / 32 | 54.285 | 61.038 | 56.926 | 65.351 |
| 8 KiB | 32 / 32 | 229.002 | 238.460 | 229.151 | 234.528 |

These are whole load-stage times with the same durability barrier, not individual
mutation latency. Six-host distributed B32/W32 loading approaches one-host native
elapsed time, but not equal-cost performance.

The current tested read configuration regresses against the previous source
and its legacy read configuration on this same fleet:

| Value | Transport | Scalar p50 current / previous | `get_many` p50 current / previous | Reopen elapsed current / previous |
|---|---|---:|---:|---:|
| 1 KiB | TCP | 1.502 | 1.515 | 1.668 |
| 1 KiB | EFA | 1.713 | 1.731 | 3.509 |
| 8 KiB | TCP | 1.550 | 1.309 | 1.166 |
| 8 KiB | EFA | 1.829 | 1.739 | 2.018 |

The previous source has no concurrent-window API; no previous/current window
speedup is claimed. The table does not isolate individual code changes from
the changed lane/worker configuration. Current EFA 8 KiB reopen takes
1,359.888 ms, versus 2.424 ms native and 673.893 ms previous EFA.
Open and shutdown also regress: their observed B32/W32 wall-time ratios are
2.946–4.143 and 4.044–4.196, respectively. The complete observed-over-10% list
also includes checkpoint reads/capture, scans, mixed stages, post-flush reads,
reopen audits, some missing-key reads, maintenance and EFA loading. These are
observations from three repetitions, not independent causal or significance
claims. There is no broad performance win over the previous source.

#### Memory, CPU and next measured target

The small 8 KiB qualification cases isolate lane count at window32 and one
server worker. Each row is one qualification pair, not a main-matrix median.

| Read lanes / shard | Native opened RSS, MiB | Aggregate distributed opened RSS, MiB | Distributed loaded-to-window CPU, s |
|---|---:|---:|---:|
| 1 | 15.676 | 680.391 | 1.310 |
| 2 | 15.688 | 985.219 | 1.728 |
| 4 | 15.660 | 1594.812 | 2.798 |

The CPU interval includes preceding scalar phases; it is not window-only CPU
or utilization. Aggregate RSS sums separate process snapshots. All process
samples passed the host-RAM bound; all 590 node snapshots showed zero swap.

In the separate 8 KiB EFA diagnostic, the warm/reopened call averages 332.423 µs:
253.398 µs is in exchange, 56.059 µs in the nested server record-read span,
27.385/25.883 µs in actor queue/return, and 18.164 µs in record verification.
The cold diagnostic averages 574.659 µs, including 488.707 µs exchange and
296.483 µs in the nested server read span. Nested spans must not be added.
Each diagnostic state has 256 samples. Exchange includes transport, scheduling
and server work; this does not identify wire latency or disk service time alone.

**Next:** explain and reduce miss-path queue/worker handoff and connection
startup costs locally, preserving bounded progress and correctness. Retest a
changed candidate with this hardware matrix before changing defaults. Size the
coordinator cache from the working set and observed hit fraction. Do not add
more lanes, adopt positional I/O universally, or start an NVMe-oF rewrite on the
basis of these results. EFA here is two-sided authenticated messaging, not
one-sided RDMA GET or NVMe-oF.

#### Cleanup and cost

The six main-run instances self-terminated at **16:28:12 UTC**, after export.
The archive was retrieved and verified before the result bucket was deleted.
Scoped cleanup finished at **16:54:25 UTC**. An independent AWS MCP audit at
**17:02:06 UTC** covered all eight run namespaces, including capacity failures:
zero nonterminated experiment instances, EBS volumes, network interfaces,
security groups or placement groups, and no remaining run-owned roles,
instance profiles or S3 buckets. All 40 exact-name IAM/S3 absence checks passed.
The two pre-existing instances remained running and were not targeted.

[Shutdown transitions, cost calculation and independent audit](../../logs/efa-progress-2026-10-08/)
are retained alongside each run's cleanup receipt. Compute estimates from
launch to shutdown are **$6.48** for C6id qualification and **$11.96** for the
full G6 campaign. Including public IPv4 and 32 GiB gp3 modeled conservatively
through cleanup gives **$18.52** for these runs. With the earlier recorded fork
estimate, the tracked total is **$33.57** against the $60 allowance.
These are runtime estimates, not an invoice; S3, request, transfer, tax and
credit adjustments are excluded. Pending launch time may overstate compute.
EC2 compute billing stops when an instance enters
[`shutting-down`](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html).

No experiment cluster or run-owned cloud storage remains. Project skills
referenced by `AGENTS.md` are absent in this checkout; no existing skill file
was made stale.

## Historical prepared campaign: 8 October cache controls

`control.py init --campaign efa-cache` retains the earlier cache-only plan.
It was locally verified but was not launched as a cloud campaign.
It retains the 68-case EFA comparison and adds 48 cache-enabled API cases:
**116 API cases and 290 node archives** in total. Cached cases use current source,
both TCP and EFA, 1/8 KiB values, B1/W1 and B32/W32, and three repetitions.
Each case retains 320 load batches, 64 mixed batches and 4,096 point reads.

| Current warm case | Frontend cache | Native value cache | Repeated-read phases |
|---|---|---|---|
| `storage` / `storage-efa` | Disabled | Disabled | Enabled |
| `cache` / `cache-efa` | 8 MiB | 8 MiB nominal values | Enabled |

The native control now accepts `--native-read-cache-bytes`. Its budget is divided
across 1,024 per-cell LRUs: eight entries per cell at 1 KiB or one at 8 KiB for
the 8 MiB setting. This excludes native key/metadata overhead. The frontend's
existing cache includes retained bytes and cache-table metadata in its bound.
These are explicit cache policies, **not an equal-total-RAM comparison**.
Keep process RSS and hit/miss/eviction evidence with any performance claim.

Cached cases and current uncached warm controls use `--hot-read-pass true`.
All current `efa-cache` cases now also use `--read-window 32`; the analyzer
requires their window phase and validates cold window I/O evidence.
The `point_get_hot` and `point_get_many_hot` labels mean repeated reads, not a
promise of all hits. The working set and per-cell conflicts can exceed the cache.
Frontend stage counters report actual hits; native profiles retain their
`value_cache_hit` observations. Cold and separate diagnostic cases remain
cache-disabled. Cold reads reject either application cache being enabled.

This enables the existing **coordinator-side** immutable-reference cache; it does
not add shard caches or change placement. A cached value needs no shard RPC.
A scalar miss contacts the shard in its published reference. Multi-key calls
group references by shard and dispatch independent groups. Each complete atomic
write batch still belongs to one shard, while a write window can use many shards.
The current shard reader accesses `PartitionLog` WAL records, not native
`Db::get` and its value LRU; its OS page cache remains active.
The bounded miss-progress and within-shard I/O work is now implemented and
measured in the section above. It does not assume workload-specific placement.

Freeze the changed example with
`--features experimental-distributed,experimental-fabric,test-utils` before
packaging a fresh source-bound bundle. `efa-cache` retains the EFA campaign's
six `c6id.32xlarge` hosts and existing ownership, deadline, export and cleanup
guards. It does not change the default historical `overlap` selector or authorize
a launch. Analyze retrieved results with
`analyze_cloud.py RESULTS --profile efa-cache`. Cache comparisons use current
uncached controls and the matching cache-enabled native reference, not an old
native build with caching disabled.

Earlier cache-control verification on 8 October:

- 37 deployment regression tests passed, including invalid cache/cold campaign
  rejection and matched-native cache comparison ratios.
- 24 real API cases passed full-value and reopen audits: native, TCP and software
  libfabric/TCP, with both cache policies and all four shapes. All enabled cases
  recorded cache hits; disabled cases recorded none. This is correctness
  evidence on one host, not hardware EFA or independent-NVMe performance.
- The actual CLI rejected undersized/oversized native budgets and cached cold
  reads before creating output/database state. External configuration generation
  retained the 8 MiB frontend budget and all five endpoints.
- The independent campaign generator and analyzer agree on 116 cases and 290
  node archives. The updated analyzer still validates all 68 API cases and 170
  node archives from the completed 7 October EFA run.
- The earlier release build and feature-set Clippy passed. Current verification
  and the remaining unrelated lint/formatting findings are listed above.

Evidence: [local verification artifacts](../../logs/cache-controls-2026-10-08-034v45qk/).
No new native-parity result is claimed. The referenced project skill files remain
absent; no existing skill file was made stale.

## Historical campaign: 7 October TCP read transport and cold I/O

Select `control.py init --campaign read-path` for this historical comparison. It compares
the preserved preparation-overlap build with the new TCP buffer/deadline build.
It runs **32 API cases** and collects **80 node archives**: 24 warm comparisons
(three repetitions, two workload shapes, two builds, native/TCP), six current
cache-evicted cases and two separate current diagnostic cases. Each case has
320 load batches, 64 mixed batches and 4,096 point reads. Application read caching
is disabled. Shapes are 1 KiB/B1/W1 and 8 KiB/B32/W32; cold cases use the latter.

Freeze the current binary with `--features experimental-distributed,test-utils`.
The guest rebuilds each archive with its manifest's feature set in a separate
fresh target directory. It rejects a current cold campaign without `test-utils`.
Both native and TCP cold cases use `--cold-read-pass true`; separate diagnostic
cases also use `--transport-diagnostics true`. The external-node path supports
the same flags. `read_io.csv` records physical reads and residency; the extra
profile files record client, node and native stage times. These clocks can nest
or overlap; they are not an additive cluster wall-time budget.

Validate retrieved results with `analyze_cloud.py RESULTS --profile read-path`.
Source hashes, binary receipts, exact case/node counts, NVMe identity, physical
I/O, swap and correctness checks remain required. This is a bounded cold-start
campaign, not a sustained dataset-larger-than-RAM or RDMA benchmark.
See [Read path and cold I/O](../../docs/distributed/READ_PATH_AND_COLD_IO_2026-10-07.md)
for that campaign's measurements and evidence. Its 33 local deployment tests passed.

## Historical campaign: 7 October preparation overlap

The default `overlap` operator configuration compares the frozen 6 October source-guided
build to the new preparation-overlap source. The workload has **111 API cases**:
96 warm, 12 larger barrier-heavy cases (three repetitions per build/mode), and
three current whole-window `--tcp-preparation-chunk-batches 32` TCP controls.
Main current cases use chunks of 16; both builds use `--tcp-read-cache-bytes 0`.
The earlier 106-case matrix below is retained as historical protocol evidence.
Use `analyze_cloud.py RESULTS --profile overlap` for that matrix (285 node
archives), or the default `--profile original` for the October 6 archives.

The exact implementation, local gates, limitations and run evidence are in
[Preparation overlap](../../docs/distributed/PREPARATION_OVERLAP_2026-10-07.md).

# New-fork sharded-WAL cloud campaign

This directory belongs to `tidehunter-sharded-wal`. It never selects the sibling
middleware's old benchmark binaries. The controller compares two archived source
trees on six identical EC2 machines: one coordinator/native baseline and five RF1
storage nodes, each with its own NVMe. Data uses mTLS over TCP or the explicitly
selected EFA message transport. No replication or Sui adapter is claimed.

## Operator-owned control plane

The operator runs `control.py` through the connected AWS MCP for current account,
price, quota, AMI, subnet and instance checks. Its explicit phases create the
run-owned IAM/network/storage resources, launch EC2 instances, dispatch SSM,
transfer artifacts through S3 and verify cleanup. `prepare_bundle.py` remains
local-only; `bootstrap.py` and `guest.py` execute on the already launched guests.
Guest SSM/S3/termination SDK calls use a scoped instance role, never laptop tokens.
Manual fallback follows a recorded MCP error and retains the same exact run scope,
deadline, artifact checks and cleanup requirements.
The AWS compute, storage and Python SDK skills guided these boundaries.

For the default TCP campaign, confirm six `i4i.xlarge` machines in one AZ, an Ubuntu 24.04 x86
image, a 32 GiB encrypted root volume per machine with `DeleteOnTermination=true`,
IMDSv2 required, and `InstanceInitiatedShutdownBehavior=terminate`. Use a fresh
run-specific security group allowing port 7700 only from itself and HTTPS egress.
Auto-assigned public IPv4 permits SSM/S3 outbound access without NAT gateways. The
normal SSM path needs no public inbound port. A documented exceptional SSH starter
may temporarily allow port 22 from the operator's exact IPv4 `/32`; it must use
authenticated host keys and a run-specific ephemeral client key, and the ingress
must be revoked after startup. Do not change other users' resources.

Every instance must carry **all** tags:

```text
Project=TideHunter
ManagedBy=sharded-wal-benchmark
RunId=th-<exact 16 lowercase hex characters>
```

The controller checks the six exact IDs, owner account, tags and hardware before
terminating. Guest absolute shutdown timers and operator cleanup are independent
safeguards. Neither an estimate nor a timer is an AWS-enforced dollar spending cap.
Use freshly retrieved pricing and retain a total-experiment ledger below the
user's $60 allowance; the initial six-machine one-hour experiment is deliberately
smaller. Initialization records the reviewed estimate; compare it with fresh AWS
pricing before launch. It is not a live billing total.

## Files and preparation

- `prepare_bundle.py`: local-only validation/package of source and binary hashes.
- `control.py`: operator-side MCP orchestration, immutable plan and mutation
  journal, provisioning, launch, guest readiness, start, retrieval and cleanup.
- `mcp_adapter.py`: bridge to the installed AWS MCP; this is separate from guest
  instance-role SDK calls.
- `bootstrap.py`: guest-only account/type guard, immutable launch-time expiry,
  and mounting exactly one blank device whose model identifies EC2 instance store.
  EBS, existing filesystems/partitions, ambiguous drives and mounted devices fail.
- `guest.py`: source rebuild, exact artifact verification, SSM node lifecycle,
  fresh case namespaces, comparison, full/partial S3 export and scoped termination.
- `test_local_safety.py`: no-cloud checks for those destructive boundaries.
- `test_control.py`: fake-provider tests for control-plane scope, launch
  idempotency, readiness, start and cleanup gates.

## Operator permission preflight and documented fallback

Check the operator's **actual** permission boundary before paid launch, separately
from the instance-role policies. EC2 access and successful AWS authentication do
not imply `ssm:DescribeInstanceInformation`, `ssm:SendCommand` or invocation-read
permission. IAM policy validation, when unavailable to this account, is not proof
that these calls work.

The 6 October run encountered explicit denials for the operator's SSM inventory
and command APIs after EC2 launch. EC2 Instance Connect was available. The recovery
path used a temporary exact laptop IPv4 `/32` SSH rule, an ephemeral client key,
and ED25519 host keys obtained from authenticated EC2 console output. One SSH
starter asks the controller's scoped instance role to prove readiness of all five
storage nodes through SSM, verifies the controller locally, and then starts the
supervisor under persistent systemd ownership. This is a starter fallback;
benchmark traffic remains private mTLS/TCP, and result/progress retrieval remains
through S3. It does not relax the six-node readiness or original expiry checks.
An interrupted starter has an unknown outcome: inspect its receipts/service state
before any retry. Record successful startup and revoke the temporary SSH ingress;
never infer build or benchmark completion from the SSH exit status alone.

This fallback is now an explicit operator command:

```sh
python3 experiments/aws-sharded-wal/control.py ssh-readiness-start \
  --run-dir RUN_DIRECTORY --ssh-cidr OPERATOR_PUBLIC_IPV4/32
```

Use the actual run directory and exact current public `/32`. The helper verifies
the authenticated host key, records the single-start receipt and revokes its
temporary rule. If the revocation response is lost, resume that exact cleanup with
`control.py ssh-close --run-dir RUN_DIRECTORY`. Ordinary `progress`, `retrieve`,
`terminate` and `cleanup` phases continue through MCP. Do not interpret an unknown
starter outcome as permission to request a second controller.

After all local code/tests and release builds pass, run:

```sh
python3 experiments/aws-sharded-wal/prepare_bundle.py \
  --current-binary target/release/examples/sharded_wal_bench \
  --current-manifest logs/optimization-2026-10-06/current/source-manifest.json \
  --baseline-binary logs/optimization-2026-10-06/baseline/sharded_wal_bench \
  --baseline-manifest logs/optimization-2026-10-06/baseline/source-manifest.json \
  --baseline-source logs/optimization-2026-10-06/baseline/source.tar.gz \
  --output logs/optimization-2026-10-06/NEW-PAYLOAD-DIRECTORY
```

The current manifest is tied to the frozen final release build. The helper
rejects any source/binary mismatch and refuses an
existing output directory. Upload `bundle.tar.gz` using an MCP-issued presigned
URL; preserve `upload-manifest.json`. Bootstrap downloads must verify its SHA-256
before extracting, and must refuse symlinks/path traversal. On each host the result
must be `/mnt/tidehunter/<run-id>/bundle`. Do not substitute an old payload merely
because its filename is the same.

## Guest bootstrap and fixed lifetime

Arm a minimal shutdown fallback in user data **before any network install**, then
run `bootstrap.py --account ACCOUNT --run-id RUN --ttl-seconds 3600` as root.
The permanent systemd timer derives expiry from EC2 `pendingTime`, so boot/setup
and reboots never extend it. A 600-second export reserve is the default.
Do not repeat bootstrap to reset the deadline.

Ubuntu guest prerequisites: Python 3, `python3-boto3`, OpenSSL, GNU build tools and
CA certificates. Install official Rust 1.90.0 on the controller only. Install no
operator AWS profiles. The supervisor builds both source trees on the controller
with `cargo +1.90.0 build --release --locked -p tidehunter --features
FEATURES_FROM_MANIFEST --example sharded_wal_bench`, using **separate create-only
Cargo target directories for baseline and current**. Only downloaded dependency
sources may share a cache. It verifies every archived source hash and unchanged
lockfile, saves each resulting executable separately, rejects identical binaries
for these distinct comparison sources, checks required CLI capabilities and runs
the exact candidate external-preparation check before distributing
those **cloud-built** executables through S3 and SSM. Nodes verify each SHA-256.
The local binaries remain provenance/debug artifacts and do not serve cloud cases.

The first 6 October cloud attempt exposed a real build-provenance defect: normalized
archive timestamps combined with a shared Cargo target reused the baseline output
for the current tree. The current command finished in 0.08 seconds and its binary
rejected the new prep-only flag. Zero measured cases ran; failure evidence was
exported and all six instances terminated. The isolated targets and pre-distribution
binary/capability/preparation gates above address that failure; source hashes alone
are insufficient evidence of a correct rebuilt executable.

The fixed build path was then exercised locally against **both exact normalized
source archives**, with independent empty targets and real Cargo invocations.
Distinct executables and the exact preparation command passed, followed by four
native/TCP correctness cases and ten storage-process clean exits. Evidence is in
[`local-cloud-build-gate`](../../logs/optimization-2026-10-06/local-cloud-build-gate/COMPLETE)
and its [`process-smoke`](../../logs/optimization-2026-10-06/local-cloud-build-gate/process-smoke/COMPLETE).
Before the retry plan, all 27 deployment tests passed. After the mutable-progress
read fix, the final suite passes **29 tests: 12 guest and 17 control**.

Run the controller as a persistent systemd service with `RuntimeMaxSec` no later
than the immutable guest deadline. A short-lived SSM shell must not own a background
node directly: the node deliberately uses a parent-death signal. The runner uses
systemd ownership for each node and a bounded stop timeout.

Full-node recovery sends `SIGKILL` to the exact case/partition unit's entire
control group; it does not perform a graceful stop. The transient services use
[`systemd-run --collect`](https://www.freedesktop.org/software/systemd/man/latest/systemd-run.html#-G),
which unloads failed units too. A subsequent `systemctl stop` can therefore race
with collection and fail with “Unit … not loaded” after the crash succeeded.
The restart path instead waits up to 20 seconds for **both** the recorded old PID
to disappear from `/proc` and a successful exact-name `systemctl list-units`
query to show no remaining unit. Deactivation and collection must finish before
the unit name is reused; command/bus errors are not treated as proof of exit.
It then removes stale readiness and retains the fresh live PID, readiness,
executable-hash and unchanged absolute-deadline checks. Existing node data is
never extracted over or initialized during restart.

The opt-in local regressions exercise real systemd units and real process
death, including an already-collected unit, an explicitly gated `ExecStopPost`
transition, bounded waiting, same-name restart and an unrelated surviving process.
From the repository root, with `boto3` available to the selected Python interpreter:

```sh
env TH_TEST_SYSTEMD=1 PYTHONPATH="$PWD/experiments/aws-sharded-wal" \
  python3 -m unittest -v test_checkpoint_async.SystemdRestartTests
```

Root uses the system manager; an unprivileged caller uses the real user manager.
The test adapter only selects the manager, not command responses or process
behavior. The selected manager must permit transient services. Without the
opt-in environment variable these tests are skipped. They use unique
`th-restart-test-*` units with a 30-second maximum lifetime and explicit teardown,
make no AWS calls, and establish only local orchestration behavior—not storage
recovery correctness or a cloud/EFA measurement.

## Controller configuration

Create a mode-0600 JSON file with current IDs/addresses from the actual launch:

```json
{
  "run_id": "th-0123456789abcdef",
  "account": "354775971805",
  "region": "us-east-1",
  "bucket": "FRESH-PRIVATE-RUN-BUCKET",
  "s3_prefix": "th-0123456789abcdef",
  "run_root": "/mnt/tidehunter/th-0123456789abcdef",
  "coordinator_instance_id": "i-00000000000000001",
  "nodes": [
    {"partition": 0, "instance_id": "i-00000000000000002", "private_ip": "172.31.32.10"},
    {"partition": 1, "instance_id": "i-00000000000000003", "private_ip": "172.31.32.11"},
    {"partition": 2, "instance_id": "i-00000000000000004", "private_ip": "172.31.32.12"},
    {"partition": 3, "instance_id": "i-00000000000000005", "private_ip": "172.31.32.13"},
    {"partition": 4, "instance_id": "i-00000000000000006", "private_ip": "172.31.32.14"}
  ],
  "deadline_utc": "REPLACE-WITH-EARLIEST-ACTUAL-GUEST-EXPIRY",
  "export_reserve_seconds": 600,
  "repetitions": 2,
  "storage_repetitions": 2,
  "capacity_repetitions": 2,
  "terminate_on_finish": true
}
```

These instance IDs/IPs are placeholders, never launch targets. The deadline must
be the earliest actual bootstrap expiry, not now plus another hour. Execute:

```sh
python3 /mnt/tidehunter/RUN/bundle/runner/guest.py controller --config /mnt/tidehunter/RUN/controller.json
```

Controller IAM needs scoped S3 read/write of this exact run prefix, SSM SendCommand
to the five storage IDs using AWS-RunShellScript, GetCommandInvocation, and EC2
DescribeInstances plus TerminateInstances restricted to all three ownership tags.
Storage profiles need scoped S3 bundle/cloud-binary/case-config GET, result PUT,
and SSM managed-node permissions. Result/command destinations remain private and
encrypted; avoid logging credential contents. Use existing CloudTrail/API audit
and S3 access/CloudWatch monitoring where configured; enable the appropriate data
events/access logging for the run if stronger audit coverage is required.

## Case isolation and benchmark matrix

The candidate binary has a **prep-only** mode using the runtime's actual serde
schema and native `KeyShape`. It generates a fresh private test CA, leaf identities,
five node launch files and one remote configuration, with zero network calls:

```sh
sharded_wal_bench --prepare-external-case NEW_DIR \
  --guest-case-root /mnt/tidehunter/RUN/cases/CASE \
  --remote-endpoints IP0:7700,IP1:7700,IP2:7700,IP3:7700,IP4:7700 \
  --batches 128 --mixed 32
```

Only each node's leaf private key/certificate and the public CA reach that node;
the CA signing key and client key stay on the coordinator. Certificates are
short-lived. Paths and public configuration may enter evidence; private key bytes
never enter result archives.

Each old/new/build/shape/repetition gets a fresh explicit namespace. A restarted
node in the same case uses its original history; a new case never implicitly
resets an existing database. The archived baseline can consume prepared configs
without implementing the prep-only command. The separate `remote_read_profiles.csv` diagnostic runs after the timed reopen
audit. It records index, dispatch, wait, optional actor handoff and nested TCP
encode/exchange/decode/verification clocks. Nested durations overlap and cannot be
summed as a disjoint wall-time budget. Direct scalar reads have zero actor queue,
service and return fields; multi-endpoint fanout still uses actors.

Node start actions are deterministic;
a lost SSM reply is an unknown outcome and does not start another benchmark.

The default matrix is:

- Warm: 1/8 KiB values × 1/32 operations per batch × windows 1/8/32 × two builds ×
  native synchronous/TCP × two repetitions = **96 API cases**.
- Larger barrier-heavy supplement: 8 KiB × batch 32 × window 32, 320 load batches,
  64 mixed batches, two builds × native/TCP × two repetitions = **8 API cases**.
- Capacity supplement: current TCP only, 8 KiB × batch 32 × window 32, 128 load
  batches and 32 mixed batches × two repetitions = **2 API cases**. Its separate
  `capacity4m` namespace uses 4 MiB fragments and a group cap of 4 MiB minus eight
  bytes. This changes the aggregate Value-WAL mapping allowance from 15 to 60 MiB;
  it has no new native reference row. Set `capacity_repetitions` to 0 to omit it
  (accepted range 0–2). The default total is **106 API cases**.
- All preserve full-byte/checkpoint/scan/reopen audits. Build and native/TCP order
  alternates by repetition; every native result uses the same instance hardware
  as a storage node and its own local NVMe. No middleware binary participates.

The supplement approaches the existing 96 MiB per-case history bound. It is not a
cold-storage or SSD-saturation experiment on a 32 GiB machine. All five nodes receive
work at window 32, but only measurement of actual disk counters/queues can establish
utilization. More extensive retention/cold datasets require another prepared gate.

## Results and complete closure

S3 keys beneath the exact run prefix:

- `results/controller.tar.gz`: full or partial coordinator/build/phase evidence.
- `results/receipt.json`: completeness, archive length/SHA-256 and cloud build hashes.
- `results/progress.json`: initial/build/per-case status, updated outside measured
  phases; a missed advisory update is not evidence that a benchmark failed.
- `results/nodes/<case>/node-N.tar.gz` plus `.receipt.json`: node stdout/stderr,
  launch paths/configuration, process memory and before/after disk/vm counters.
- `cloud-bin/`: exact executables built in the cluster.
- `case-artifacts/`: ephemeral encrypted TLS/config packages; do not publish them.

Progress is a mutable snapshot: read it with one bounded GET, without comparing its
length to an earlier HEAD that may describe another version. Immutable result
archives still require their receipt's exact length and actual SHA-256. During the
6 October retry, one read-only AWS CLI S3 progress read followed recorded MCP errors;
the result archive and cleanup retained their normal verification boundaries.

A mandatory upload and byte-for-byte S3 read-back probe passes before cloud builds
or measured work. All archive uploads get SHA-256 metadata and verified HEAD
length/metadata. The controller also downloads and verifies each node archive and
embeds those artifacts in its final archive for one reliable operator retrieval. The
operator then retrieves through MCP-issued HTTPS URLs and checks actual downloaded
bytes against receipts; HEAD metadata alone is not local retrieval verification.
Partial results must remain labeled partial. Final transient transport/server export failures retry only until the immutable
deadline minus 30 seconds; permanent permission failures fail immediately. The
export reserve bounds retries;
when the deadline arrives, cleanup wins over keeping a paid fleet alive.

After final export, the controller terminates exactly its six verified tagged
instances. Hard guest timers remain armed even if export/termination API calls fail.
The operator must independently verify terminated state and deletion of all owned
root EBS volumes, then remove only run-owned IAM/profile/security/placement resources.
Retain verified local evidence before deleting temporary S3 objects. Also check
for multipart leftovers. Confirm the other users' instances remain unchanged.

### Completed scalar-transport hardware campaign, 11 October 2026

Run `th-db8b2af08939b4cc` executed all 124 planned cases on six separate
EFA/NVMe hosts. The corrected offline audit validates all cases and all 360 node
archives. Current and baseline cloud benchmark binaries were retrieved and their
actual SHA-256 values match the build receipts. All 1,093 objects from the run's
S3 prefix are preserved locally before bucket deletion.

The original final audit failed because it required `service-restore.transport`,
which the timing receipt does not emit. Transport remains checked against the
source-bound prepare command and hardware/setup evidence. The original archive
and `complete: false` receipts are unchanged; do not manufacture a campaign
`COMPLETE` marker. Reproduce the post-hoc analysis with:

```sh
python3 experiments/aws-sharded-wal/analyze_cloud.py \
  logs/scalar-transport-aws-2026-10-11/run-full/results \
  --profile scalar-transport \
  --output logs/scalar-transport-aws-2026-10-11/run-full/analysis \
  --allow-partial
```

The sole remaining campaign-level failure is the preserved historical audit
receipt, not a missing execution. See
[`posthoc-validation.json`](../../logs/scalar-transport-aws-2026-10-11/run-full/posthoc-validation.json).
MCP failed after result retrieval; explicitly authorized CLI cleanup then removed
all remaining owned resources. Independent absence checks are in
[`full-closure-verified.json`](../../logs/scalar-transport-aws-2026-10-11/run-full/full-closure-verified.json).
The local `s3-export/` directory contains ephemeral TLS/config artifacts, is
restricted to mode 0700, and must not be published.

The [hardware report](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#13-efanvme-hardware-results-11-october-2026)
contains matched native/TCP/EFA results, source identities, startup/recovery
breakdowns, regressions, cost, and unmeasured acceptance properties. Raw concurrent
read samples each represent one read although the inherited summary
`sample_unit` says `complete_window`; interpret those percentiles per read, not
per complete concurrency window. Preserve original measurement files.

## Persistent authenticated transport floor, 12 October 2026

The new `sharded_wal_transport_bench` separates an established authenticated
request/response from WAL and index work. It validates every payload byte inside
the timed API and asserts zero WAL-member reads. It is not a physical network
latency measurement or a substitute for the real-WAL benchmark.

Build both executables from the same source and features. Use the libfabric
environment described above for fabric builds; TCP-only builds can use
`experimental-distributed,test-utils` instead.

```sh
cargo +1.90.0 build --release -p tidehunter \
  --features experimental-fabric,test-utils \
  --example sharded_wal_bench --example sharded_wal_transport_bench

target/release/examples/sharded_wal_transport_bench \
  --output logs/floor-tcp-adaptive --transport tcp --polling adaptive \
  --shards 5 --values 1024,8192 --samples 1000 --repetitions 3

target/release/examples/sharded_wal_transport_bench \
  --output logs/floor-tcp-busy-idle --transport tcp --polling busy \
  --shards 5 --values 1024,8192 --samples 256 --repetitions 3 \
  --idle-us 2000 --profile true
```

Use new output directories. Compare matched profiling and offered-load settings.
`--transport fabric-tcp` is software-provider validation, not EFA evidence.
`--transport efa` requires actual supported hardware and a compatible provider.
No command here provisions AWS resources.

For an existing prepared cluster, pass `--remote-config /path/to/remote.yaml`.
External preparation now also creates its sibling `shape.yaml`. The requested
transport, shard count, and polling policy must match the prepared nodes;
polling is checked against authenticated node status. External node shutdown
remains the operator's responsibility. Local spawned nodes are stopped on exit.

The real-WAL benchmark and external preparation accept:

- `--tcp-polling adaptive|busy`: established-session progress policy. Adaptive
  remains the default. Busy removes deliberate hot-path sleeps/yields but consumes
  CPU per session; it does not make storage or the complete engine lock-free.
- `--tcp-resident-first true|false`: attempt nonblocking page-cache reads before
  async fallback. Default false. Valid only for the async read policy; explicit
  shard-cache hits remain immediate in either mode. External nodes must receive
  this setting when prepared, not only when the client starts.

Keep both caches disabled for the cold read pass. Resident attempts, successes,
fallbacks, partial prefixes, and async submissions are in `read_path_evidence.csv`.
These are not physical disk-operation counts. Floor evidence YAML contains,
in order, before-read stats, after-read stats, and connection profiles.
Polling counters describe server progress; polling gaps are not wire latency.

Protocol/ALPN is now v5. Rebuild and deploy the same binary generation to every
participant; do not attach a v5 client to an archived v4 campaign.
See the [local report and source mapping](../../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#14-local-polling-and-resident-read-follow-through-12-october-2026)
for measured benefits, regressions, CPU cost, correctness checks, and the limits
of the single-SSD/software-fabric results.
