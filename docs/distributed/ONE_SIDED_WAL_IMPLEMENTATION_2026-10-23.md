# One-sided resident Value-WAL reads: implementation and evidence guide

23 October 2026. Branch: `perf/one-sided-wal-reads-2026-10-23`. Base commit: `405f3e3bc7304e089713f1e8b04e07aee8e92451`.

**Status: hardware run `40063575350` remains partial; preparation retry `40073005718` failed before EC2 allocation.** The hardware run completed all 12 Sui pairs, both native hardware `FI_READ` payloads and eight KV pairs before an SSM metadata limit. All 747 raw objects are locally SHA/size-verified and its fleet is closed. The bounded-label repair and fixed 95-minute full-matrix reservation are published. The latest preparation exposed another intermittent software-fabric startup deadline; the locally verified observer correction below preserves process-failure evidence instead of masking it with a procfs permission error. The complete cloud KV matrix and native parity remain unproven. No native mutex was removed.

This is a source/status guide, not a replacement for [D1](ONE_SIDED_WAL_READS_2026-10-21.md), the [23 October remote-read plan](REMOTE_READ_PATH_PLAN_2026-10-23.md), or preserved historical results. Paths and symbols below identify the working tree. There are deliberately no fabricated GitHub permalinks to unpushed changes. After the parent's production/benchmark freeze signal, selected inspected line ranges were added below; these are local snapshot coordinates, not immutable committed permalinks. `project:` denotes the parent of this engine checkout.

## 24 October: failed hardware admission and repair

The first campaign allocated exactly six `g6.8xlarge` hosts in `us-east-1a`, one controller plus five owners, under cluster placement group `th-60ac6254cafaf05a`. It exported failure diagnostics and requested unconditional teardown before any workload cell. Independent AWS MCP checks at `2026-10-24T00:02:32Z` confirmed all six instances terminated and their EBS volumes, EFA interfaces, security group and placement group absent. This was an orchestration/source-admission defect, not a Tidehunter speed result.

The actual runner correctly contained `runner/resident_config.py`, SHA256 `9b80d3b460f7d64ce1f2a990148085974358ec37fe07bb8f62c76fffabd84c08`, identical to frozen `experiments/resident_config.py`. The old controller incorrectly looked for `experiments/aws-sharded-wal/resident_config.py`. `sui_checkpoint.py::validate_runner_helpers` now shares the explicit canonical source association, exact Python-helper inventory, ordinary-file and SHA checks with controller admission and `ci_campaign.py::admit_inputs`. The latter extracts and validates the **actual assembled runner archive before writing `input-admission.json` or allocating EC2**; binding checks remain enabled.

Retained evidence is `project:checkpoint-work/aws-ci-runs/40035176278-1/`: verified preparation metadata, original failed runner, failure exports, and accounting artifacts. `failure/helper-admission-proof.json` records admission of that real runner against its frozen manifest and rejection after changing the shared helper. Full repaired gates ran 601 AWS harness cases with four optional/environment skips and passed 137 Sui analysis cases. These are correctness checks, not hardware performance.

The retry prefers `us-east-1`, then `us-east-2`, `us-west-2`, `eu-central-1`, and `eu-west-1`: the first fleet succeeded in Virginia after four Oregon capacity rejections, and the results bucket is in Virginia. All regional fallbacks remain. Cluster placement, machine candidates, full matrix, 7,200-second deadline, 600-second export reserve and weekly budget are unchanged. The repair changes Python orchestration only; the Sui source and engine library pin remain unchanged.


## 24 October: complete per-cell storage dependencies

Run `40044046628`, AWS prefix `th-619c43918a0f3d42`, used the corrected engine `c303b79f5e6a7dd3eaaac2731c9c1c7f70172a62` and passed global runner admission. All five per-cell storage commands then executed and failed with response code 1: `storage/runner/guest.py:27` could not import `resident_config`. Per-cell preparation had independently copied only the AWS subdirectory, omitted the shared helper and included test modules. This was not an SSM authorization, network-latency or RDMA result.

`sui_checkpoint.py::{runner_helper_origins,assemble_runner,validate_runner_entrypoint}` now owns one canonical, source-bound runtime closure. Both the global CI archive and every per-cell storage payload use the same assembler. It validates source files, builds the complete non-test helper set, then validates installed hashes/inventory. Per-cell preparation also binds its source manifest and `cloud_node.py` before upload. `ci_campaign.py::admit_inputs` extracts the actual archive and runs `sui_checkpoint.py node --help` with Python `-E`, cleared `PYTHONPATH`/`PYTHONHOME`, neutral working directory and disabled metadata lookup; `input-admission.json` retains that proof before any fleet allocation.

The actual failed storage archive, SHA256 `22de1e96e8767556f56406c6b96ba34102d33179458c57c5b618a3d25ab2da42`, reproduced the import failure offline. Reassembly from its real, hash-verified frozen source bytes produced 25 runtime helpers; both actual packaged worker entrypoints loaded successfully without repository imports. Removing the shared helper then failed admission. Receipts: `project:checkpoint-work/aws-ci-runs/40044046628-1/failure/packaged-storage-cli-{before,after}.json`. The repaired AWS harness ran 607 cases with four optional/environment skips; Sui analysis passed 137.

Independent MCP closure confirmed all six instances terminated and every exact EBS volume, EFA interface, security group, placement group, IAM role and profile absent. All 27 raw objects were downloaded and SHA/size-verified locally; retained failure evidence is under that run directory. Ledger revision 46 settled a partial estimate of `$1.942976652964`, released the full reservation and left `$33.086289310709` available under the unchanged weekly budget. Billing remains unavailable. Existing admission filtering excludes unaffordable regional candidates; an affordable US fleet remains eligible. No Sui or Rust engine code, workload case, lifetime or budget change is part of this packaging repair.

## 24 October: completed Sui matrix and isolated raw-probe ports

Engine `be8cd05eac12a3c469319979cf0d70b0dd462391` and Sui `bc2fcffd8c5f94af843f6000136b1287f8d6fd5c` ran on six `g6.8xlarge` hosts in `us-east-1a`, cluster placement group `th-28349ced82717701`. Preparation rebuilt all six executables and passed the real local replay/full-state gate before allocation. One preceding GitHub VM had failed remote startup with a deadline and a thread-stall warning; an unchanged-byte retry passed without relaxed timeouts or checks. That is an intermittent observed startup failure, not an established memory-exhaustion cause.

All 12 reported Sui transport/cache pairs completed and their full state audits matched. All 24 foreground histograms contain 1,776 `checkpoint_completion_latency` observations; these are hardware-run histograms, not the package's earlier loopback histograms. The 256-byte raw probe completed 64 warmups and 1,000 measured native-provider `FI_READ`s, with accepted-read count 1,064 and complete drain. Provider post/completion p50 was `42.841 µs`; API plus validation p50 was `43.931 µs`. This instrumented provider/API interval is not pure wire RTT or a claim of few-microsecond EFA performance.

The next raw case failed before its server launched: retained SSM invocation `65bfd6f5-1dae-4d65-8bc1-e8c2d272b5e7` exited 1 in `raw_rma_floor.py::guest_action` at `reservation.bind`, with `OSError: [Errno 98] Address already in use`. Both payloads had selected port 17700. Thus this is an independent-case TCP lifecycle error, not an 8 KiB RDMA failure or KV performance result. `raw_rma_floor.py::owned_port` now binds the payload to a separate bounded, non-Sui/non-KV listener port; controller intents and guest validation use the same mapping. There is no port-reuse override, skipped raw case, weakened validation, new security-group ingress or runtime retry.

The boundary regression failed before the fix and passed after it. A real local TCP active-close exercise left 17700 in `TIME_WAIT` and reproduced errno 98 on reuse; the repaired 8 KiB case bound 27700 and transferred all 8,192 bytes correctly while the previous port remained unavailable. That proves the changed bootstrap path, not local RDMA hardware. The full corrected AWS harness ran 608 cases with four optional/environment skips; Sui analysis passed 137. Evidence: `project:checkpoint-work/aws-ci-runs/40051920946-2/failure/raw-rma-8192-start-cause.json` and `control-port-isolation-local.json`.

Independent AWS MCP at `04:29:41Z` confirmed all six instances terminated, all six EBS roots and EFA ENIs absent, the exact security/placement groups absent, and all six IAM roles/profiles absent. No manual termination was needed. The preserved controller archive is 126,353,032 bytes, locally SHA256-verified as `483f91b799aabb7ef06dd16cce6b773d414cd3920731443d6578456bb9195a58`; its original data and the privately retained input package remain under that run directory. Ledger revision 49 settled a **partial estimate**, `$4.335143879751`, released the whole reservation and reports total estimated settled usage `$71.248854569042`; current-policy weekly headroom is `$28.751145430958` under the unchanged `$100` policy. Billing is unavailable; future retained-storage projection is not included as realized usage. The repair changes Python orchestration only, with no Rust/Sui pin, workload, deadline, budget or placement change.

## 24 October: bounded SSM labels and measured cold-probe duration

Run `40063575350`, prefix `th-5a5f08b542737b69`, used engine `780f72e2003879c410202ac0aedb2084f0385b12` and the unchanged Sui/library pins. The raw 256-byte **and** 8 KiB native `FI_READ` probes now completed, so the distinct-port repair worked on hardware. All 12 Sui pairs completed and eight ordinary KV pairs produced 16 process results. The next cold start failed before execution: AWS rejected `SendCommand.Comment` because the verbose case label exceeded 100 characters. This is not a failed 8 KiB RDMA read or a cold-data measurement.

`guest.py::Campaign.ssm_nodes` now uses the full run/action plus a short case digest in this display-only field. Full case identities remain in the command payload, namespaces and receipts; unknown-ack handling and exact-unit cleanup are unchanged. The actual production method and botocore serializer were exercised for all 93 canonical cases and both actions, stopping before the AWS/network boundary. Sixty original labels were invalid, maximum 116 characters; all 186 repaired labels were at most 42 characters and retained their complete case identity in the payload. Proof: `project:checkpoint-work/aws-ci-runs/40063575350-1/failure/ssm-comment-all-planned-cases-local.json`.

Before shortening the fleet reservation, the admitted binary `fd379a657d558d579eedb2e18dd4b396a675e053975c3376c8f8e74d6404058e` ran the actual full-count 8 KiB cold case locally: native 3.97 seconds; five TCP owners sharing the laptop SSD 40.68 seconds, including 25.618 seconds local cluster shutdown. Both modes produced **1,024 qualified positive-expected-owner scalar probes and 1,024 qualified complete32 probes**. The two cold phases took 3.096 seconds native and 6.626 seconds TCP; the existing report consumer read both full results. This is duration/coverage evidence only, not a remote-EFA or independent-NVMe prediction. Receipt: `failure/cold-duration-and-coverage-local.json` under that run directory.

The active `ci-config.json` now gives the unchanged full matrix **5,700 seconds (95 minutes)**, retaining the 600-second export reserve, all 93 KV pairs/186 processes, 12 Sui pairs/24 replays, read counts, cold probes, placement, transports and regional fallbacks. Integer lifetimes remain bounded between one and two hours; expiry stays fixed at original launch. Historical full-campaign wall time was 64m45s for 172 KV pairs; the current eight-pair envelope was 275.307s. This supports a forecast with leeway, not a guarantee. At retained US prices the maximum reservation is about `$22.81`, below current-policy headroom `$23.428417742779`; live pricing/affordability must still admit the next run. The `$100` budget is unchanged.

All 747 objects, 602,287,771 bytes, were independently SHA/size-verified locally. The controller archive is 134,230,732 bytes, SHA256 `4063d6c268fdb31f8ac16b20d78e67b23f93eb0bdcac6f165c4ecaea60e09761`. MCP confirms all six instances terminated, the exact security/placement groups and six IAM role/profile pairs absent, and no RunId-tagged volumes or ENIs remaining. Individual volume/ENI IDs were not retained, so per-ID absence proof is unavailable; CloudTrail lookup is denied. Their launch settings require deletion on termination. This evidence limit is recorded in `failure/aws-mcp-closure.json`, not hidden. Settlement is a partial estimate `$5.322727688179`, total estimated branch usage `$76.571582257221`, with no outstanding holds; billing remains unavailable. No Rust/Sui code, pin or security property changes are part of this metadata/lifetime repair.

## 24 October: preserve the actual preallocation process failure

Run `40073005718` rebuilt from protected engine commit `0007a5c9fb605a29d54403571b1a474b228bb5a8`, but failed the real software-fabric Sui replay before AWS allocation. `remote.log` records an epoch-store open, remote-operation deadlines, connection-replacement warnings, a 50,303-ms thread-stall report and the typed-store open panic. The Python observer then raised `PermissionError` reading `/proc/<owned-pid>/io`, obscuring the child failure in the top-level receipt. These logs do not identify which startup sub-operation stopped progressing and do not establish OOM, a provider defect or a Rust locking defect. The retained failure is under `project:checkpoint-work/aws-ci-runs/40073005718-1/`.

`experiments/sui-checkpoint/run.py::sample` now requires the existing nonreaping `build.OwnedProcess`. Missing counters are accepted only after confirmed process exit, or a fresh `CoreDumping: 1` observation for the permission transition; a live unexplained permission/missing-proc failure still raises. Unavailable samples contain no invented IO/RSS values. `replay` retains the failed resource row, real exit status or core-dump state, the child-log location, and the last successfully collected foreground metrics. The workflow's failed-build artifact now retains existing `*.jsonl` and `*.prom` diagnostics beside logs and receipts, not database contents.

Four deterministic lifecycle regressions passed; the full offline suites passed 608 AWS harness cases with four explicit optional/environment skips and all 141 Sui harness cases. An actual owned Linux child made itself nondumpable: its live `/proc/io` permission failure remained an error; after SIGTERM, sampling recorded exit code `-15` without IO/RSS substitutes, and group cleanup reaped the reserved PID. Proof: `project:checkpoint-work/software-startup-repro-2026-10-24/observer-process-proof.json`. This is process-lifecycle correctness evidence, not a startup-liveness repair or performance measurement.

No Rust behavior, mutex, provider selection, replay deadline, retry policy, matrix population or AWS termination bound changes in this correction. A fresh source-bound build and complete local replay must still pass before the next fleet may launch. Only ignored `archive/middleware-legacy/target/debug` and `release` compiler outputs were removed after local disk exhaustion; archived sources, SDK toolchains and retained benchmark evidence were preserved.


## Current hardware-campaign preparation

Five source-review findings were corrected before hardware admission:

| Defect | Correction and source | Required evidence |
|---|---|---|
| EFA registration hints could reject the provider's basic MR map before native-read validation | `tidehunter-fabric/src/endpoint/resources.rs`, `Info::hints` and selected-mode validation advertise/accept `FI_MR_LOCAL`, `FI_MR_PROV_KEY`, `FI_MR_VIRT_ADDR`, and `FI_MR_ALLOCATED` for EFA RMA | The actual selected endpoint must report non-emulated device reads; software-provider success is not this proof |
| A message-only fallback could still request or promote resident grants | `tcp/reactor.rs` gates client acquisition and server promotion on negotiated `rma_capabilities`, not configured capacity alone | Regression uses a real committed record, current authority, original owner, and actual message-only endpoint; ordinary RPC must preserve the value |
| Dropping the registry/node could release original WAL authority while an unresolved MR remained quarantined | `SourceRegion::retained_owner` retains the same admitted original-owner `Arc` beside the MR/source bytes; failed MR close retains its byte charge too | After all unrelated original owners are dropped, uncertain teardown must block same-root reopen; exact successful drain/retirement must permit it |
| Profiled resident-grant RPC replies omitted response-recheck observations, making positive server spans appear unavailable and fail diagnostic qualification | `tcp/reactor.rs::flush_responses` updates both `RecordsProfiled` and `ResidentRecordsProfiled`; actual one-sided hits still report no target-handler timing | Real software-provider durable acquisition regression failed with zero recheck samples versus one request before the fix, then passed; reused `FI_READ` has no read RPC or server spans |
| Inline one-sided reads incremented the exported logical read-RPC count despite sending no Read message | `remote_db/pending_read.rs::ReadService::read_inline` uses the same actual `message_read_requests` delta check as reactor dispatch, after either profiled or ordinary client completion | Real remote-database acquisition counts one RPC; warmed ordinary/profiled `FI_READ` hits leave it unchanged. The regression observed an incorrect two versus one before the fix and passed after it |

The explicit software-fabric profile selects the new message-only fallback, strict device-read rejection, and original-owner quarantine/drain regressions. Selection is not an execution result; current verification results must be recorded separately from the historical ledger below.

The proposed `hardware-populations` campaign retains 93 KV pairs and 12 certified-Sui cells on one cluster-placement fleet. Native, TCP, EFA, EFA-direct, and trusted-native EFA controls remain distinct; only explicitly eligible cells enable registered reads. Both 320- and 3,200-batch 8-KiB controls remain. `--bounded-hot-reads 32768` requests 32,768 scalar calls and 1,024 complete 32-member calls per bounded population, replacing neither historical identities nor whole-workload phases.

Physical-I/O controls add independent scalar and complete-group probes: `--cold-single-probes 1024` and `--cold-group-probes 1024`. Each probe drains/evicts and captures process evidence outside its API timer, verifies the full result, and retains all samples. Every requested owner must account positive read bytes in every qualifying probe. `cold_probes.jsonl` and `cold_group_probes.jsonl` retain owner boot/PID/start identity, counters, residency, and preparation time; `physical_cold_get` and `physical_cold_get_many` report API latency separately from preparation. These are process-accounted physical-read probes, not raw NVMe service time, per-member device attribution, or saturation claims. Failed probes must not be filtered into an apparently pure population.

`write_io.csv` independently brackets real load/mixed/delete/flush phases on the native process or controller and every storage process. Process-accounted writes do not establish completed-device durability; append/persist semantics remain those of the actual WAL APIs. Coordinator hits, encoded owner-cache hits, registered-copy hits, page-cache controls, physical-I/O probes, and naturally mixed Sui replay are not interchangeable performance populations.

### Current local readiness evidence

The new release receipt is `project:tidehunter-sharded-wal/checkpoint-work/hardware-populations-2026-10-23-local-v1/receipt.json`. Its benchmark SHA256 is `516eea3d140b73965d23d9e5ff8b322acd620b483ece620ce58a703fe6e775a3`. All nine actual-CLI cases completed full-value/reopen audits and strict offline evidence checks: native/RPC/registered-RMA 8-KiB batch32 controls, coordinator/owner cache-hit controls, and native/TCP cold probes for 256-byte and 8-KiB values. Each bounded control has 1,024 scalar and 32 complete-group samples. Each of four cold cases retains all 32 scalar and 32 complete-group probes; every expected owner has positive accounted physical reads with known stable process identity. Real write-resource rows cover the native process or controller plus all five storage owners. The throwaway driver was removed; exact commands, logs, receipts and evidence remain.

The rebuilt raw software-floor receipt is under `raw-software/` in the same root: both 256-byte and 8-KiB payloads completed 1,000 clean plus 64 warmup `FI_READ`s, full payload validation and exact drain. Actual transfers include the 24-byte probe header. Its executable SHA256 is `9b203d8983560f34acaccbef8d83996c5de5dd0a99bf8dd9b607959a2faef550`; `hardware=false` is explicit.

Pre-profile-fix full gates include native 385 passed/6 ignored, TCP-only distributed 803 passed/7 ignored, fabric-enabled engine 804 passed/38 ignored, native fabric primitive 49 passed/2 ignored, and all 42 explicitly selected software-fabric cases. Final preparation gates ran 596 AWS harness cases with four explicit optional/environment skips and passed all 137 Sui Python cases. A separate actual raw-evidence mutation run passed all 14 cases. Sui typed-store passed all 34 native tests with the published dependency; its sharded/fabric surface and full `sui-core` also type-checked. The additional profiled-grant regression and strict all-target fabric Clippy passed after the response-recheck correction. These suites overlap; their counts must not be summed into an independent-test total. The earlier ledger below remains historical.

These runs validate functionality on one shared laptop SSD, not cloud latency, physical-device service time or independent-drive scaling. Local commands ran alongside correctness gates, so their timing files are not a controlled performance comparison. Current certified-Sui source binding and replay must still pass the pre-EC2 six-binary GitHub preparation gate.

The Sui adapter adds per-owner cache/WAL/authority/initiator metrics in `crates/typed-store/src/tidehunter_util.rs`; if either owner or resident sampling fails, it omits the combined owner family rather than emit stale fields. This changes observation, not Sui execution/consensus or the WAL format. The portable Sui manifest/lock must pin the published engine revision before updating `AWS_BENCH_SUI_SHA`.

Sui's former `checkpoint_exec_latency` was registered but had no observations anywhere in the current executor; the historical campaign's zero counts cannot yield completion p50/p95/p99. The replacement `checkpoint_completion_latency` is captured in `sui-core/src/checkpoints/checkpoint_executor/mod.rs` when the synced checkpoint enters the bounded executor, before pipeline-handle waiting, and observed after the final highest-executed publication stage. `metrics.rs` names the scope explicitly: admission through queueing, execution, storage commit and publication, not certificate network arrival or start spacing. Only tasks that reach that completion point contribute. It adds no per-key clocks, changes no execution/durability decision, and retains no old-name alias. The pre-EC2 real-checkpoint proof must contain positive valid native and remote completion histograms; their quantiles are bucket bounds, not exact raw-sample percentiles.

The local native and RMA diagnostic commands also completed full-value/reopen audits. `diagnostic-validation.json` in the same engine proof root records 128 native and 128 remote scalar profile rows with nonzero component clocks; 22 remote rows completed with zero Read RPCs and correctly marked server clocks unavailable. These instrumented mixed rows are not substituted for the clean bounded populations or for cloud timing.

The post-review profiled-grant correction was rebuilt and exercised separately in `rma-grant-profile-fixed-8192-b32/`: executable SHA256 `55538fe60ab3791c4a89e4cca3247170f46d48283e1a30c33a770ee6115dbeff`, full-value/reopen audit complete. `grant-profile-regression.json` records strict `checkpoint_kv.validate_diagnostic_evidence` acceptance for all five owners: 128 scalar profiles, 105 actual RPC rows with available server timing and 23 one-sided rows without a Read RPC or target-handler spans. The earlier saved diagnostic correctly fails that strict consumer with positive spans marked unavailable. `transport_floor.py::flatten_counters` also validates the exact root/client `server_handler_available` and `server_handler_not_applicable` boolean metadata paths without treating them as numeric counters; raw flags remain unchanged, including mixed aggregates where both are true. Unknown boolean counters and malformed known flags still fail. This is actual software-provider integration proof, not hardware timing.

The inline-counter correction also passed a new actual release CLI and the same strict consumer: `inline-rpc-regression.json`, executable SHA256 `c83ed7abc3c9fb8346559315590894e5af8666b3318feb5515722960e64ef4f9`, full-value/reopen audit complete, 128 profiles with 106 actual RPC/server-timing rows and 22 no-Read-RPC rows. These retained invocation counts depend on the observed mixed service path; they are not latency or hit-rate claims. Both profiled and ordinary inline public reads are covered by the real-provider counter regression.

Pinned-provider source proof is separate from runtime proof: [`efa_ep_getopt`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_ep.c#L63-L72) returns false for the direct endpoint's emulated-read/write options. The application still checks the actual selected endpoint and its negotiated RMA capabilities; a source-code constant or instance-type name alone does not qualify a campaign host.



## 1. Problem, and what this implementation does not change

A coordinator already knows which committed Value-WAL member a key names. A warm distributed RPC still pays request dispatch, owner-side authority/planning, reply framing, transport progress, and client validation even when the member's bytes are in RAM. This work adds a reusable, bounded grant to **an immutable registered RAM copy** so a later eligible read can use an actual `fi_read` without a per-hit RPC or target application handler.

The coordinator index remains authoritative for key/version selection. Each owner remains `PartitionLog` over the native `Wal`, not another complete native `Db`/`LargeTable` index. The owner WAL and its complete durable groups remain recovery authority. A registered copy, rkey, member CRC, read-cut pin, or successful CQE is not a replacement for that authority.

The optimization does not implement NVMe-oF, an EFA disk protocol, hardware page-fault service, arbitrary remote file reads, TeRM's Mellanox ODP modifications, new replication, or a different WAL/version/commit format. `FI_READ` addresses registered memory, not `PhysicalValueAddress.offset` on disk. RF1 instance-store loss remains data loss. It does not promise instantaneous detection of a remote fault or fence.

### Native mechanism → distributed mechanism

| Problem | Native Tidehunter mechanism retained | Distributed implementation / source |
|---|---|---|
| Resolve a key at a consistent version | In-memory/versioned index and publication order; native `LargeTable` lookup/value cache | Coordinator `NativeIndex`; `distributed/native_index/retention.rs`: `IndexReadPin`, `pin_cut`, `validate_cut`; `distributed/remote_db/pending_read.rs`: preparation/completion and point checks |
| Recoverable value identity | Native WAL frame CRC and complete append/recovery groups | `distributed/reference_directory.rs`: `RegisteredBatch`; `distributed/storage.rs`: durable-reference validation and `RecordReadPlan`; `distributed/types.rs`: `ValueRecordReference` |
| Avoid expensive read work for hot values | Native value LRU, mapped WAL reads, kernel page cache | Separate bounded decoded coordinator `ReadCache`, encoded owner `Cache`, kernel NOWAIT path, and registered resident copy; these populations must not be merged |
| Read one member rather than siblings | Native `Wal::read` still verifies its complete frame; the fork's independently verified member path has a separate contract | `wal/mod.rs`: `checked_member_range`, `plan_record_payload_range`, `read_record_payload_range`; `distributed/codec.rs`: `CompleteBatch::decode_record`, `decode_records`, member checksum/layout validation |
| Preserve read/file ownership | Native `Arc<File>`/mapping owners and short guards | `RecordReadPlan::resident_owner` retains the **original** `RegisteredBatch` metadata credit, native `Arc<File>`, and `Arc<PartitionLog>` through grant lifetime/quarantine |
| Reduce warm remote dispatch | No network in a native local read | `tcp/reactor.rs`: `resident_grants_for_read`, `Client::{submit_read,prepare_rma_read,progress_rma_read}`; `tidehunter-fabric`: owned registrations, actual `try_post_read` and non-polling `take_read_completion` |
| Keep cold work off the network owner | Existing worker/read scheduling and owned I/O buffers | `tcp/read_executor.rs`: `ReadExecutor::submit_planned`, `ReadLease`; bounded planning workers and ordinary RPC fallback |
| Fence/poison/retire safely | Existing reader authority, publication and file retirement | Additional session grant revocation plus service-wide admission guard; these do not remove native mutexes or weaken native CRC/durability |

### Prior locally validated source navigation

The following coordinates identify the earlier locally validated producer snapshot. The current prelaunch corrections above are newer than the immutable v2 archive and can shift these ranges. Symbols are the primary lookup for current code; the historical verification ledger must not be retroactively attributed to the new campaign build.

| Mechanism | Inspected local source ranges |
|---|---|
| Full grant and remotely transferable descriptor | `tidehunter/src/distributed/tcp/resident.rs:115–136` (`RemoteRegion`, `ResidentGrant`); `:155` (`encode_envelope`), `:195` (`checked_member`) |
| Exact original authority/file/log ownership | `tidehunter/src/distributed/storage.rs:564–590` (`ResidentReadOwner`, `RecordReadPlan::resident_owner`) |
| Owned group state and ordinary admission | `tidehunter/src/distributed/tcp/reactor.rs:1367–1394`, `:1787–1843` (`PendingRead`, `PendingRmaRead`, `submit_read`) |
| Nonblocking preparation/progress and checked zero-copy decode | `tidehunter/src/distributed/tcp/reactor.rs:2356`, `:2424`, `:2517–2540` (`prepare_rma_read`, `progress_rma_read`, direct `decode_record`) |
| Two-phase terminal authority | `tidehunter/src/distributed/tcp/control.rs:526–540` (`poison_readers`, `finish_poison_if_quiescent`) |
| Service-wide admission guard and release entry | `tidehunter/src/distributed/remote_db/pending_read.rs:460–463`, `:1050–1053` (`AdmissionPause`, `pause_and_release_resident_grants`) |
| Real physical partition labels, aggregate and lanes | `tidehunter/src/distributed/remote_db.rs:91–110` (`ReadIoMonitor` resident samplers) |
| Stable-context O(1) completion routing | `tidehunter-fabric/src/endpoint/resources.rs:432–473` (`RmaState::complete`, generation validation) |
| Actual `fi_read`, local descriptor and rotating slot cursor | `tidehunter-fabric/src/endpoint/resources.rs:1313–1369`; public `tidehunter-fabric/src/endpoint.rs:527–536` (`take_read_completion`, no CQ poll) |
| Native safety and retained async retry state | `tidehunter/src/file_reader.rs:20–23`, `:77–80`; `tidehunter/src/distributed/tcp/read_executor.rs:327–330`, `:397–400`, `:644–647` |
| Shared bounded control and I/O observation | `tidehunter/examples/support/cache_workloads.rs:116–119`, `:224–227`, `:233–236`, `:446–449` |
| Bounded-only matching and source-qualified service consumer | `experiments/aws-sharded-wal/checkpoint_kv.py:369–412`, `:449–570` (`match_bounded_hot`, `bounded_control_evidence`) |
| Shared explicit cloud Sui policy | `experiments/aws-sharded-wal/checkpoint_matrix.py:109–112`; `sui_checkpoint.py:141–144` (`cell_resident_reads`, `validate_prepared_cell`) |
| Later root analyzer-only Sui metric correction | `experiments/sui-checkpoint/analyze.py:132–149` (`shard_read_stats`); `test_shard_read_evidence.py:70–78` (optional `db` label and duplicate-owner collision regression); **not in immutable v2 archive** |


## 2. Five distinct memory paths, and physical I/O

Read order is logically: capture the authoritative reference/cut; answer absence/tombstone if selected; try an eligible coordinator cache hit; try the session's registered descriptor; otherwise perform the ordinary owner RPC. Owner RPC authorizes and pins its read plan before consulting value bytes.

| Tier | Contents / lifetime | What a hit avoids; what a miss does **not** prove |
|---|---|---|
| Coordinator bounded `ReadCache` | Verified decoded member/key/value, keyed by complete reference; fixed bounded policy, short cache/index guards | Hit avoids remote value retrieval. Zero budget disables this cache only, not Sui caches, the index, owner caches or page cache. Source: `remote_db/read_cache.rs`, `ReadCache`, `ReadCachePolicy`. |
| Owner encoded-member cache | Immutable encoded member under `ReadAdmission`/fixed four-way CLOCK `Cache`; lookup follows authority planning | Hit avoids member file I/O but still uses RPC. Miss does not prove disk service. Source: `tcp/read_executor/cache.rs`, `ReadExecutor::submit_planned`. |
| Kernel page cache / mapping | File-backed RAM; mmap/population is not permanent page pinning | NOWAIT exact success is resident RAM service. Buffered `io_uring`, `pread`, and mmap can also hit RAM. Async submission is not physical IOPS. |
| Bounded registered resident copy | Separately allocated immutable envelope/member, MR, source slot, exact grant, original authority/file/log owners | Eligible hit uses actual `FI_READ` into an owned registered destination. No handler/RPC per hit. It costs real additional RAM/registration capacity and is not a registered disk WAL. |
| Verified cold physical-I/O interval | Same real native/remote API plus process/residency evidence around the timed phase | Only measured accounted physical reads establish that physical I/O occurred. It does not establish per-key device latency, all-cold service, NVMe saturation, or independent-device scaling. |

Sui execution/object/control caches are a separate upstream tier and can bypass this adapter. The coordinator's versioned reference index and owner's complete-batch authority cache are metadata, not value-residency evidence. TX/RX registered message buffers and idle allocation pools are not cached WAL values.

`ReadExecutor`'s resident-first **file** probe is distinct from registered RMA: a sweep allows four `preadv2(RWF_NOWAIT)` calls, 64 KiB requested bytes, and a 16 KiB maximum member. Exact success publishes initialized bytes. A short result retains its initialized prefix and asynchronously reads only the suffix. `EAGAIN`/`EOPNOTSUPP` use the full async fallback; other errors propagate. Ineligible/exhausted probes go async. `WalMemberReadPolicy::Async` is buffered file I/O; mapped and positional are distinct synchronous policies, not device-direct or one-sided paths.

For cold evidence, `examples/support/read_campaign.rs` (`evidence`, `record`, `run`) collects the actual native process or exact remote owner set, PID, physical `read_bytes`, faults and WAL residency before/after. The cold phase rejects a zero accounted physical-read delta. Advisory eviction alone is not proof. `experiments/aws-sharded-wal/checkpoint_kv.py::classify_read_population` preserves the distinction between observed interval I/O and a physical-NVMe performance claim. Laptop processes can exercise this machinery but cannot prove five independent NVMe devices scale.

## 3. WAL authority, publication, and result acceptance

A safe hit requires **all** of the following; a short disk offset and rkey are insufficient:

1. The coordinator selects an exact `ValueRecordReference` at a pinned `IndexReadCut`. Preserve the database/partition, original batch/version, batch digest, physical address/generation and member descriptor/checksum. Current request epoch is distinct from the original record epoch and MR generation.
2. `RegisteredBatch` proves complete-group membership. `PartitionLog::validate_record_authority`, `validate_durable_record_reference`, `try_plan_record_read`/`plan_record_read`, and `persist_receipts` enforce the release-published durable group frontier; mere reference-directory presence before persistence is insufficient. Coordinator publication still advances only across its complete contiguous durable prefix.
3. The plan retains original native file, log and authority owners. `ResidentReadOwner` is derived from that plan **before registration**; a fresh lookup or `Arc<PartitionLog>` alone would lose the original file/metadata-credit guarantee.
4. Capacity for source bytes/regions and pending grant ownership is reserved before promotion. Build the complete immutable envelope and member, register the finalized allocation, establish the selected peer endpoint, and recheck reader authority under grant admission before publishing a descriptor. A partially initialized source must never be exposed.
5. `ResidentGrant` contains full reference, current request epoch/reader generation, owner boot, session, grant identity/generation, source region identity/generation, remote address/key and bounds. `encode_envelope` repeats the identity in a versioned header protected by CRC. There is no mutable remote poison byte or unproved seqlock.
6. The initiator reserves destination/operation capacity, posts the checked complete envelope, and does not inspect destination bytes before terminal completion. `checked_member` verifies header checksum, exact identity and lengths; each `CompleteBatch::decode_record` verifies its member's layout/CRC. The grouped convenience `decode_records` applies that same checked decoder to each member. Existing point-read full-key, scan reduced-key, tombstone, count, duplicate and caller-order semantics remain above transport.
7. Result acceptance remains tied to the captured logical pin and local authority/session health. An envelope checksum detects accidental corruption; it neither authenticates a malicious peer nor grants WAL authority. Wrong identity, corruption and authority failure are errors, not hidden by a successful retry/RPC.

The native complete-frame read still uses `CrcFrame` in `Wal::read`. The distributed bounded member path deliberately avoids hashing/copying siblings only after complete-group membership is known and the independently checksummed member will be verified. It is crate-private/opt-in, not a weakening of ordinary native `Db` reads.

### Acquisition is not part of a hit

`resident_grants_for_read` opportunistically attaches grants to successful normal replies; the first read can remain RPC. `Client::validate_resident_grants` reconciles every returned descriptor against exactly what its pending wire chunk reserved. One acquisition-bearing RPC per lane is admitted at a time (`resident_acquisition_inflight`/`PendingRead.grant_limit`); concurrent ordinary reads can proceed with allowance zero. A returned server-owned grant must not be silently dropped when a client handle table fills.

First-read RPC is intentionally not eliminated: acquisition happens on an ordinary read reply, and not every newly written or RAM-resident WAL member is prepublished as an RMA address. Source-region metadata allowance is partitioned **per endpoint/session** by `ResidentReadConfig::source_per_connection` (`resident.rs:62–78`), while source bytes remain node-wide. Promotion declines new members when that endpoint's table, request grant allowance or size limit is exhausted (`reactor.rs:3774–3778`); immutable granted entries are not adaptively replaced as an LRU. Thus free-looking RAM or kernel residency does not imply a reusable descriptor. This explains a real eligibility limit, not a claim that the feature already fixes checkpoint latency.

`Client::submit_read` chooses the resident path during ordinary admission and queues an owned nonblocking group in `PendingRead::rma` (`PendingRmaRead` at this source snapshot); scalar `read_records_correlated` uses this same submit/progress path rather than a private blocking RMA loop. `remote_db/pending_read.rs::drive_reads` admits work across owner lanes and performs bounded progress sweeps without waiting for a group to finish on one lane before other lanes can advance. Eligibility is for the complete current reference group: missing/stale handle, unsupported provider, oversize envelope, insufficient destination slots or ordinary source/destination pressure returns to **the same reference and cut's ordinary RPC** before accepting DMA. The first implementation does not split a member into fragments or treat a mixed group as a pure hit. A provider error after acceptance must retain/drain or quarantine the accepted operations; it is not ordinary capacity fallback.

`Client::prepare_rma_read` captures exact grants and reserves the group shape; `progress_rma_read` advances posts/completions and returns control while work remains. The endpoint/CQ is progressed once per bounded sweep, then `Endpoint::take_read_completion` harvests each already-progressed operation **without polling the CQ again per member**. The owned group retains operations, complete destination owners, next-post/accepted/completed counts, error state, original references, deadline and reply charge across sweeps. Successful envelope checks produce a zero-copy `Bytes::slice` of the member; decode and key/authority acceptance still run before delivery. This removes serialized wait/scan/copy overhead, not the correctness checks.

After the envelope checks, `progress_rma_read` decodes each zero-copy member directly with `CompleteBatch::decode_record` into the already-reserved `pending.values`. The former `decode_records` convenience path only looped over this same decoder; bypassing its temporary record/mutation vectors and associated `Bytes` clones removes allocations/owner churn, **not CRC, identity, key or authority checks**. No partial group result escapes on a member failure.

## 4. Source-grant lifecycle and failure semantics

### Healthy cooperative lifetime

The implementation uses bounded independently registered immutable member allocations, not D1's optional future multi-member append region allocator. `ResidentReadConfig` bounds source bytes/regions, destination bytes/slots, maximum read bytes and handles. `source_per_connection` bounds fixed endpoint metadata; node-wide `GrantRegistry` prevents source-byte multiplication across connections. `destination_per_connection` partitions client allowance across admitted lanes. Fabric process quotas additionally bound allocations/registrations. Envelopes and duplicated encoded copies cost memory; cache eviction cannot free a still-granted allocation.

The registered tier is a set of **immutable session grants**, not an adaptive, freely evictable LRU. The ordinary owner encoded-member cache retains its separate bounded CLOCK/cache policy for RPC fallback. Published registered bytes cannot be replaced on a recency decision; capacity pressure falls back until cooperative release/reclamation is safe. A 4 MiB registered-source budget bounds the charged resident allocations, **not every WAL file byte retained by the grants**: original native file owners are pinned at file granularity until release/quarantine resolution, so a few small resident members can retain larger WAL files. Account that retention separately; source RAM occupancy alone is not a file-GC bound. A lost holder remains bounded fail-closed/quarantined; inability to prove quiescence can require operational recovery/restart rather than a successful maintenance transition.

State and ownership are explicit: pending reservation → registered unpublished source → published grant → cooperative revoke → exact peer/session terminal drain → MR retirement → source/credit release. `GrantRegistry::{try_reserve,shrink_reservation,publish,release}` retain original owners. Unpublished retirement is unsafe and requires proof that no equivalent address/key/length was exposed. Published retirement requires `PeerDrainToken::after_ack`, whose unsafe contract requires exact peer/session/grant drain, not an idle guess.

`Client::revoke_resident_grants` stops use and releases the exact generation set only after client work is drained. `revoke_server_grants` validates that set, retires source registrations and then sends `GrantsRevoked`. A valid release can reach the server before its prior response TX CQE: `serve_session` defers revocation under the existing deadline until earlier response owners retire, instead of falsely treating the client's cooperative drain as a protocol violation. A send-completion prefix is not a remote-read drain certificate.

### Authority barriers, including spontaneous poison

No target handler executes for a successful one-sided hit. A remote crash, poison or new epoch cannot be detected instantaneously by reading a cached boolean or header generation. Safety instead requires an authority transition barrier:

- Normal maintenance uses `ReadService::pause_and_release_resident_grants` and its `AdmissionPause`. Stop new read admission across **all** lanes, wait for previously accepted lane work under a deadline, release grants, and retain the guard across the actual authority/retirement operation. `remote_db/checkpoint_lifecycle.rs` holds this guard for retirement. An instantaneous retained-request count is not a barrier. Native index/cache guards are not held over waits.
- Resident-disabled operation returns no extra guard, preserving existing ordinary retirement behavior. A healthy pre-operation drain timeout can reopen admission while existing owners remain retained; an uncertain grant-release or post-pause authority/retirement error poisons the service and must not resume admission as if maintenance succeeded.
- Spontaneous poison is two-phase. `GrantRegistry::request_poison` closes new grant/write/read admission without claiming issued grants were already revoked. The server sends bounded unsolicited `Response::ResidentPoison` using reserved response ID zero and exact owner-boot/session/generations. The client stops new cached acceptance, drains accepted reads and prior replies, and cooperatively releases grants. Idle read lanes use owner-affine `Client::progress_resident_control`, not a fresh per-hit status RPC.
- `ControlContext::finish_poison_if_quiescent` invalidates reader authority/publishes terminal health only after pending/live holders have quiesced. Old immutable durably certified reads can remain authorized during this pre-terminal interval. Unknown/lost holders prevent falsely successful transition; the operational outcome is bounded fail-stop/recovery, not a fabricated drain.

The above source contract now has the parent's explicit local software-fabric regression proof described below, including poison/release/admission/fallback and accepted-post deadline behavior. This is not a proof of EFA failed-device quiescence or of every possible CQ/revocation interleaving; coverage limits remain explicit.

### Deadline, cancellation, teardown and quarantine

`Client::progress_correlated` checks each owned pending read's deadline; `progress_rma_read` also rejects verification that exceeds the captured deadline. Expiry returns a bounded error and fail-stops the endpoint; it does not wait synchronously forever for a missing CQE. `ReadOperation::drop` cancels user interest only. The destination buffer, stable CQ context, registration and credits remain provider-owned until terminal CQ retirement, or conservatively quarantined on uncertainty.

A returned `ReadBuffer`/`minibytes::Bytes` keeps its DMA slot unavailable while **any consumer** retains it, even after CQ completion. A successful CQE is necessary to expose bytes; partial or errored destinations never become values. Context class and operation generation distinguish READ from SEND/RECV and late completions. `WouldBlock` means no operation accepted and no ownership transfer.

Connection loss, a user timeout, an empty CQ, local endpoint close, source MR close, rkey/generation change and elapsed lease duration are **not** proof that old remote DMA can no longer touch memory. Uncertain source/destination/resources and their permits remain quarantined within hard budgets; at capacity, refuse new grants/RMA or require restart rather than reclaim by clock. Do not mutate a registered source concurrently to signal poison. The pinned provider's `efa_mr_close` frees its C wrapper even when deregistration returns an error, so a close error is not a retryable native pointer.

No authoritative failed-endpoint EFA DMA-revocation guarantee was established by the inspected sources. This branch therefore relies on cooperative exact terminal-CQ drains for reuse and conservative failure quarantine, not an invented hardware teardown proof. Reconnect/restart uses fresh boot/session identities; remote pointers/keys are not checkpoint state. Logical WAL file retirement still needs its existing durable/reachability/cut rules plus grant exclusion. Retiring a resident copy is not permission to delete the WAL.

## 5. Async `ReadLease` partial progress and original native safety repair

`tcp/read_executor.rs` uses `ReadLease` to reserve reply ownership before cloning cache bytes or retaining partial resident reads. When device/SQ/request admission returns `SubmitOutcome::Full`, `PreparedRead` remains inside that lease: completed members, initialized NOWAIT prefixes, buffers, generation, class, counters and an exact plan signature survive retry. A retry validates the original reference/offset/length/device signature, does not repeat successful NOWAIT work, and submits only the needed suffix. The preclaimed lease transfers to the accepted request/reply exactly once; dropping it cancels unpublished progress and returns owned buffers/credits. `Full` is not permission to free accepted I/O or double-charge retained cache clones. The actual file/authority plans remain pinned through asynchronous completion and response retirement.

The native `FileReader` repair is separate from RMA. The old helpers built an initialized `Vec<u8>` view over uninitialized storage and treated a 4096-aligned allocation as a normal byte vector, risking a deallocation `Layout` mismatch (alignment 1 versus 4096). The helpers `io_buffer`/`io_buffer_bytes` are removed and their caller in `wal/mod.rs` migrated to `FileReader::read_exact_at`.

`file_reader.rs::AlignedBuffer` owns the exact allocation `Layout`, zero-initializes before exposing the initialized slice required by `FileExt`, and deallocates with that same layout. Direct-I/O ranges use checked arithmetic/alignment; interrupted reads retry; short reads accumulate via aligned overlapping scratch reads without exposing uninitialized tail bytes; EOF before the requested subrange is an error. `Bytes::from_owner` retains the aligned owner and returns the requested immutable slice. Buffered reads use initialized storage. Native WAL/frame CRC behavior is preserved. No native mutex removal or general lock-free claim is part of this change; existing short mutex/RwLock ownership remains.

Source navigation: `file_reader.rs::{FileReader::read_exact_at,AlignedBuffer,align_range,read_direct_at}`; `minibytes` shared-owner support; `wal/mod.rs::Wal::read`; executor tests including `full_retry_retains_partial_prefix_and_completed_resident_member` and `preclaimed_planning_credit_survives_full_and_transfers_only_on_admission`. These test names identify intended regressions, not proof they were executed on final source.

## 6. Fabric capability, permissions, geometry and trust boundary

Source: `tidehunter-fabric/src/endpoint.rs::{RmaConfig,RmaCapabilities,RmaAddressMode,RmaReadMode,RemoteReadRegion,RegisteredReadRegion,ReadOperation,ReadBuffer,Endpoint}` and `endpoint/resources.rs::{Info::hints,Resources::open_mode,RmaState}`. Official generated `ofi-libfabric-sys` bindings are used; no handwritten provider ABI or new transport framework is implied.

- RMA is explicit and negotiated on the selected provider/domain/endpoint while message-only opens remain available. Request/validate `FI_RMA|FI_READ|FI_REMOTE_READ`, RDM type, supported MR modes, context modes, actual IOV/queue geometry and checked operation bounds. `Endpoint::open_efa_direct_rma` does not silently select ordinary EFA or software.
- `FI_OPT_EFA_EMULATED_READ` is queried on the actual EFA endpoint at `FI_OPT_ENDPOINT`; only a successful false result qualifies native device-assisted reads. `require_device_read` fails closed on non-EFA, unproven or emulated selections. An instance type or `fi_info` capability advertisement alone is not this runtime proof. `g6.8xlarge` support is platform feasibility, not evidence from a host used here.
- Selected limits and `FI_OPT_MAX_RMA_SIZE` (when available) bound transfers separately from the approximately 8 KiB **message MTU**. The full envelope, not merely user payload length, must fit. Do not hard-code a purported universal device RMA maximum. The source has no initial multi-fragment member protocol.
- Export the provider-returned MR key, region identity/generation and checked remote range. With `FI_MR_VIRT_ADDR`, use the remote virtual base plus checked offset; otherwise use MR-relative offsets. The local destination's `fi_mr_desc` is an opaque process-local descriptor, supplied for `FI_MR_LOCAL`, never sent as a pointer. A disk WAL offset is neither address mode.
- Source registrations request remote-read permissions only; no remote write/atomics are required or enabled for this feature. Destinations are locally registered as required. `FI_MR_ALLOCATED` requires physically backed stable allocated pages; source backing mappings do not change while registered. Both peers install endpoint addresses before use; EFA-direct requires the target AV to contain the initiator.
- EFA-direct does not provide message/RMA ordering, `FI_FENCE`, target `FI_RMA_EVENT`, or `fi_cancel`. The implementation cannot infer read completion from packet ordering, target activity or send prefixes; the serialized endpoint owner progresses and routes CQ events explicitly, including while otherwise idle.
- RMA destination slots live in a fixed boxed array, so slot/context addresses do not move. READ CQ routing checks the context's base/stride/alignment/index geometry in constant time rather than linearly scanning every outstanding slot. A destination-pool cursor rotates admission searches rather than restarting every post at slot zero; this is a bounded pool search, not a claim that every capacity query is O(1). Neither optimization permits reuse while a provider operation or returned `Bytes` owner remains live.

**Security is not silently upgraded.** Resident grants are enabled only by explicit trusted-native fabric configuration. The native path is not mTLS/TLS record encryption and should not be described as authenticated/encrypted merely because the ordinary TCP control path supports mTLS. Raw probe bootstrap is explicitly trusted-test plaintext TCP. Isolate hosts/network and trust processes; MR read-only permissions and CRC are not Byzantine protection, cryptographic authentication, or a per-record sandbox. Do not enable a broader MR or weaken ordinary secure RPC to make a benchmark fast. Unsupported/disabled cases retain the existing RPC mode.

## 7. Measurement populations and historical controls

### Preserve the 320 versus 3,200 controls

The historical 8 KiB/batch32 comparison changed load cardinality:

| Preserved population from the dated plan | Load batches × members | Initial keys / payload | Scalar p50 reported there |
|---|---|---|---|
| Older complete run `39378783532` | 320 × 32 | 10,240 / 80 MiB | 60.924 µs |
| Recent partial `39830242085` | 3,200 × 32 | 102,400 / 800 MiB | 113.233 µs |
| Current complete first clean repeat `39877444431` | 3,200 × 32 | 102,400 / 800 MiB | 111.206 µs |
| Current smaller cold-family fixture, **pre-eviction warm** point reads | 320 × 32 | 10,240 / 80 MiB | 55.052 µs |

These are historical observations, not new measurements in this work. Original command/evidence locations remain in the [remote-read plan §3](REMOTE_READ_PATH_PLAN_2026-10-23.md#3-correcting-the-apparent-8-kib-regression), including `project:checkpoint-work/aws-ci-runs/39378783532-1/` and `project:checkpoint-work/native-wal-fastpath/aws-39877444431-analysis/`. Do not edit/replace the historical evidence or silently normalize these as identical workloads. The same-current-binary small-fixture result demonstrates workload sensitivity, **not** that cardinality alone caused the entire regression. Region, hardware size, source/protocol/lock identities, repetition count, phase order, key sequence and owner distribution are also confounded.

`8 KiB, batch32` describes value size and write/load batch shape. A subsequent scalar `get` fetches **one** member, not all 32. A full `get_many` of 32 keys is a different API operation, may fan out across owners, and must report request/group count versus member/value count separately. A warmed first-256-key diagnostic is not the random whole-oracle clean scalar population. Keep detailed clocks/diagnostics in separate repetitions; never substitute their prefix timings for clean whole-workload latency.

The often cited native ≈5 µs is a warm local RAM/cache/page-cache observation, **not physical-NVMe latency**. Even a perfect remote RAM transport still has fabric/validation/progress costs. Software loopback may prove control and `FI_READ` functionality; it predicts neither EFA latency nor independent-storage scaling.

### Required matched case identity

`checkpoint_kv.py` preserves legacy 320/3,200 cases and distinct coordinator-cache, owner-cache-on/off, cold, transport and registered controls. A valid comparison records load/read counts and dataset bytes; operation/key sequence and hotset; actual owner/lane distribution; shape/layout/preparation chunk; cache budgets and occupancy; worker/admission/polling policy; registration source/destination/slot/handle limits; actual provider/domain/read mode; source/archive/binary identities; and whether diagnostics were enabled.

Use `resident_reads.jsonl` for RMA/grant counters and ordinary read-path evidence for RPC/file tiers. RMA hits have no server-handler timing; represent it as not applicable rather than zero-duration RPC work. Coordinator-local connection gauges do not establish remote source occupancy. `resident_read_evidence`, `registered_control_evidence` and `require_registered_probe` reject missing/mixed/invalid populations rather than labeling every cache-on read an RMA hit. Bounded registered-hot controls and uniform legacy workloads are intentionally separate populations; source setup/acquisition time must remain visible outside the hit timer, not disappear under an unqualified warm-up label.

The shared matched-hotset implementation is `examples/support/cache_workloads.rs::{bounded_hot,registered_hot_policy,registered_population}`, called immediately post-load by `sharded_wal_bench`. `--bounded-hot-pass true` enables it for native/RPC controls (default false); RMA automatically implies it. All participating backends select exactly the first 32 loaded keys. Scalar calls use `mix64(sample) % 32` and `min(512, max(32, reads))` samples; grouped calls use all 32 **distinct** keys with `ceil(scalar_calls / 32)` calls. Untimed setup performs exactly `2 * target_read_lanes + 1` complete scalar-then-group warm passes for native/TCP/RMA alike, **without early exit**. The RMA-specific readiness check still requires all lanes of each active hot-set owner to have verified hits and the final `read_lanes` consecutive passes to be fully hit-only. `bounded_hot_setup` reports the generic setup cost; `registered_hot_setup` retains the additional RMA lane/acquisition proof.

`bounded_hot_policy` metadata and `bounded_hot_controls.jsonl` bind generic preparation, samples and verified elements for every backend. Native/RPC rows do **not** fabricate RMA/message counters or imply residency merely from a `cache_hot_*` timing label: their populations are `native_API_hotset_unclassified` / `remote_API_hotset_unclassified` until actual path evidence qualifies them. Linux `test-utils` collects real native/owner residency and process physical-I/O brackets in `read_io.csv` around both measured phases, outside per-call timers; unavailable observations stay unknown. The planner enables the paired native bounded control only for explicit RMA-profile cases, without extra matrix cases or legacy default changes. `bounded_control_evidence` admits native hot service only with native cache zero, actual zero accounted physical-I/O delta and positive before/after file residency; this is interval qualification, not proof of a particular CPU cache or zero fault potential. Remote registered service requires the actual strict FI_READ/lane proof. No native-versus-RMA ratio is qualified until both service populations are verified.

For **registered RMA rows**, `registered_controls.jsonl` retains raw lane snapshots, setup passes and complete policy. Pure labels `cache_hot_get` / `cache_hot_get_many` require posts = successful completions = all intended returned elements, eligible groups = verified group hits > 0, and zero Read/ReadProfiled messages, new acquisitions, fallbacks or errors. An owner group of 32 members can be one hit and 32 posts/completions. Mixed/unprepared RMA populations use `*_mixed` labels; actual lane/per-owner aggregates and CSV cardinality must agree. The generic native/RPC use of a `cache_hot_*` label is separately unclassified and is never evidence of this RMA gate. Qualification of the bounded32 control never relabels unchanged full-oracle `point_get` / `point_get_hot` as pure registered hits.

Counter semantics are separate: legacy `read_rpcs` / `RemoteDatabase::read_rpc_count` counts admitted RPC groups and excludes resident RMA groups; it is not a member/packet count. The initiator's actual wire Read/ReadProfiled count comes from `Client::message_read_requests`, incremented in `queue_read_chunk`, including additional chunks when one logical group needs more than one message. Use that exact initiator counter for zero-RPC hit qualification rather than inferring zero messages from the legacy group count. RMA `eligible_reads`/`rma_hits` count complete groups, while `rma_posts`/`rma_completions` count elements.

`ReadEvidenceMonitor::{sample_resident_reads,sample_resident_read_lanes}` in `distributed/remote_db.rs` maps internal client-vector indices through `self.clients` to actual physical partition IDs before exporting counters. This explicit benchmark API was named `ReadIoMonitor` in the earlier source-navigation snapshot. Operational collection now uses the separate inert `OperationalReadIoMonitor` described in §11. Vector index zero must not become partition zero when the actual owner is partition 2.

## 8. Reproduction entry points and policy

The following are **interfaces/recipes**, not claims that these exact commands have passed on final source. Run from the engine checkout with the existing source-bound build/environment; no download, install or cloud launch is required by this document.

### Raw provider read floor (separate from WAL/RPC)

`examples/sharded_wal_transport_bench.rs` dispatches `--rma-probe` to `examples/support/rma_probe.rs`. In **each** terminal set `BUILD_ROOT=../checkpoint-work/resident-rma-2026-10-23-local-v1/build-v2` (or another verified new source-bound bundle root) and retain its compatible libfabric environment. Start these in separate terminals, using a fresh output path/port:

```bash
"$BUILD_ROOT/binaries/transport-floor" --rma-probe \
  --role server --listen 127.0.0.1:48091 --provider 'tcp;ofi_rxm' --domain lo \
  --software-tcp --bytes 256 --samples 1000 --warmup 64 --deadline-ms 30000 \
  --polling busy --output target/rma-probe/server-256.json
```

```bash
"$BUILD_ROOT/binaries/transport-floor" --rma-probe \
  --role client --connect 127.0.0.1:48091 --provider 'tcp;ofi_rxm' --domain lo \
  --software-tcp --bytes 256 --samples 1000 --warmup 64 --deadline-ms 30000 \
  --polling busy --output target/rma-probe/client-256.json
```

Repeat with `--bytes 8192`, distinct output files and a fresh port. The provider must already be present and matching library paths configured; no installation is implied. Busy polling actually spins (no fixed sleep); reports retain the actual `Cpus_allowed_list`, not an invented CPU pin. Per-operation completion and payload-validation timings, capability/read mode and trusted-test TCP control scope distinguish this from an empty RPC or WAL measurement. The client drains terminal completions and releases result owners before sending the exact source-grant drain acknowledgment. Missing acknowledgment preserves/quarantines the source; no timer creates a `PeerDrainToken`.

A future real-hardware invocation substitutes the actual host addresses, `--provider efa --domain <selected-hardware-domain> --efa-direct`, **without** `--software-tcp`. It must qualify native/non-emulated capability and preserve source/host/provider artifacts. That command is not a claim of local EFA support or an authorization to launch resources. The raw floor does not include key lookup, WAL authority, member decode or Sui work.

The source-bound wrapper `experiments/aws-sharded-wal/raw_rma_floor.py::local_software` runs and validates both payload sizes with actual processes and preserves command/binary receipts, cleanup, raw reports and separate warmup/clean completion and API-validation CDFs:

```bash
python3 experiments/aws-sharded-wal/raw_rma_floor.py local-software --binary "$FLOOR_BINARY" --output /tmp/raw-rma-software-UNIQUE --samples 1000 --warmup 64 --deadline-ms 30000
```

`FLOOR_BINARY` must name the matching source-bound transport-floor executable; use a fresh output directory. Exercised debug and release receipts are recorded in the verification section below. The raw protocol adds a 24-byte identity header, so 256/8,192-byte **payloads** transfer 280/8,216 bytes per `FI_READ`. `--detailed-steps` retains setup separately; `validate_reports` rejects wrong capability, counts, identity, sample validation or drain.

For a future separately authorized fleet run, `run_registered_floor` is wired after suite installation only when nonzero `kv_suite.resident_reads` and `efa-native` are selected, before normal KV cases. It reuses the admitted transport-floor artifact on controller plus lowest-partition storage, discovers each actual EFA-direct domain, binds source/binary/host/command identities, and checks actual client/target reports. The reserved port is disjoint from normal KV/Sui ports; occupied bind fails rather than stealing a listener. Exact-owned readiness does not consume the probe's single TCP accept. Existing work deadlines/export reserve, scoped transfers, namespace ownership/tombstones, exact unit/process cleanup and archived/hash-validated client/target logs remain required. No new IAM permission/ingress widening or fleet provisioning is part of this hook. Its presence in source is not proof that it ran or passed.

The campaign hook additionally bounds a wedged executable with an earlier external `run_recorded` deadline: at most 35 seconds, ending at least 90 seconds before the supervisor's existing work deadline so `finally` retains exact-stop/export time. Initial pair admission requires more than 390 seconds remaining (two 180-second case reserves plus 30 seconds). These checks do not extend fleet TTL or export reserve. They are source-described lifecycle safeguards, not exercised cloud evidence.

### Matched resident-WAL control and offline consumers

For reproduction use the **new source-bound bundle's** executable (Linux `test-utils` and `experimental-fabric`), not an older root `target/release` binary. These settings match the completed release load320 case; change both sides together for load3200:

```bash
BUILD_ROOT=/path/to/new/source-bound/build
"$BUILD_ROOT/binaries/storage-node" --output logs/resident-native --backend native-sync --transport fabric-tcp-native --values 8192 --batch-sizes 32 --windows 1 --batches 320 --mixed 64 --reads 512 --read-window 32 --tcp-read-lanes 4 --native-read-cache-bytes 0 --tcp-read-cache-bytes 0 --tcp-shard-cache-bytes 0 --hot-read-pass true --read-resource-evidence true --bounded-hot-pass true
"$BUILD_ROOT/binaries/storage-node" --output logs/resident-local --backend tcp --transport fabric-tcp-native --values 8192 --batch-sizes 32 --windows 1 --batches 320 --mixed 64 --reads 512 --read-window 32 --tcp-read-lanes 4 --native-read-cache-bytes 0 --tcp-read-cache-bytes 0 --tcp-shard-cache-bytes 0 --hot-read-pass true --read-resource-evidence true --bounded-hot-pass true --rma-source-bytes 4194304 --rma-source-regions 512 --rma-destination-bytes 20971520 --rma-destination-slots 640 --rma-max-read-bytes 32768 --rma-max-handles 4096 --rma-require-device-read false
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence logs/resident-native --mode native-sync
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence logs/resident-local --mode tcp
```

At 32 KiB per destination, 20 MiB/640 slots can accommodate a width-32 result on each of up to 20 lanes; smaller totals are legitimate capacity-pressure controls but can force RPC. Handle capacity must also cover session-specific references across lanes. Native correctly rejects nonzero RMA configuration: it receives **no** `--rma-*` flags and invents no RMA counters. Native value cache, coordinator cache and shard cache are all explicitly zero here, while kernel RAM service remains measurable separately.

Use fresh output roots for each native/TCP/software-fabric/cache-off/cache-on control. For the 3,200-batch fixture, change `--batches` to `3200` on **both** sides; never mix its denominator with 320. Common CSV setup is `bounded_hot_setup`; the RMA-specific `registered_hot_setup` remains inside `registered_controls.jsonl`. Inspect both `bounded_hot.service_qualified` fields and the remote `resident_reads.registered_controls.qualified`. Standalone population evidence does not invent missing full campaign/source/host provenance or authorize ratios beyond the explicit matcher.

The offline `checkpoint_kv.py plan --config <controller.json>` prints planned native/remote argv without provisioning. `identity <results> --config <controller.json> --spec <case.json> --mode tcp` requires observed command/binary receipts, source/archive provenance, actual load/read summary, policy and placement/runtime metadata. `compare <old.identity.json> <new.identity.json> --compared implementation` permits source differences only as the named A/B axis; add `transport` only for an intended transport comparison. Unqualified/mismatched controls exit 2 and must not produce ratios. The original evidence is retained, including unavailable older metadata; no new metadata is invented for old campaigns.

The default full-workload matcher stays strict and **does not** silently permit native-versus-remote legacy-oracle ratios. A separately explicit bounded32 comparison is:

```bash
python3 experiments/aws-sharded-wal/checkpoint_kv.py compare native.identity.json remote.identity.json --population bounded-hot --compared implementation transport
```

`match_bounded_hot` requires both complete source-bound identities and actual qualified `bounded_hot` service evidence for scalar and unique32 grouped phases. It matches the loaded 320/3,200 context, exact bounded policy, preparation, cache budgets, coordinator allowance and non-topology runtime/CPU environment; differing diagnostic clocks need an explicit axis. Native has no real remote lane/owner/registration pools, so those **actual role-specific** budgets/topologies remain named implementation/transport provenance, not invented equal controls. Same-role bounded comparisons still use the exact full-policy matcher. Its permitted ratio scope is only `cache_hot_get` and `cache_hot_get_many`, never the legacy whole oracle. The `evidence` command exposes the separate `bounded_hot` qualification.

### Resident policy and Sui

`experiments/resident_config.py::resident_read_config` requires a complete bounded object, not a loose enable flag. The canonical keys are:

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

This is an example software-correctness budget, not a validated tuning recommendation. Limits are process-accounted; multiple Sui namespaces must pass `validate_destination_total`, and engine lane splitting can reduce per-lane capacity further. Zero/default disables resident reads. Hardware qualification requires `require_device_read: true`, supported trusted-native transport and actual capability proof; software fabric cannot qualify that flag.

`experiments/sui-checkpoint/run.py --resident-reads <policy.json>` reads and validates this file. `deployment.py::prepare` supplies the same policy to storage nodes and each remote database's `tls.resident_reads` transport configuration, rejects non-trusted transport, and enforces aggregate namespace destination bounds. External deployment policy must exactly match. The replay's `experiment/results.json` records `resident_reads_by_namespace` from actual config. A policy in JSON proves requested/recorded configuration, not RMA hits, replay equivalence, hardware qualification or performance. Real Sui checkpoint fixture/build/config identities and audited state equality remain required. Do not replace real checkpoint replay with a synthetic KV case.

Cloud Sui uses that same canonical opt-in profile, not a second unbound policy: `experiments/aws-sharded-wal/checkpoint_matrix.py::cell_resident_reads` selects `kv_suite.resident_reads` only for `efa-native` Sui cells. Enabled cells carry exact `resident_reads` metadata; missing/all-zero profiles preserve historical cell shape/default RPC policy, and TCP/EFA/EFA-direct cells stay disabled. `sui_checkpoint.py` writes an owned-cell `resident-reads.json`, passes actual `--resident-reads` to `prepare_external.py`, and preserves policy in results. Admission checks every coordinator database's `tls.resident_reads` and external manifest against the canonical cell profile, rejecting drift/fallback. `prepare_external.py` supplies the validated policy to owners and coordinator through `deployment.prepare`. Default policy/CI configuration is unchanged. This cloud wiring has no new AWS execution proof; the separately exercised **local** certified Sui replay is recorded below.

## 9. Verification ledger and remaining gates

Observed results and producer/analysis identities are retained in `project:checkpoint-work/resident-rma-2026-10-23-local-v1/verification-final.json`. The earlier ledger is a preserved checkpoint in time, not the final status.

| Scope | Actual supplied status | Meaning / limit |
|---|---|---|
| Fabric primitive and ownership | Actual software-`FI_READ` tests passed, including 32 retained destination slots collected in reverse order; strict fabric all-target clippy passed | Software ownership/completion proof only. The rebuilt raw CLI receipt and integrated resident coverage are recorded separately below; none proves EFA hardware. |
| Native main library | 385 passed, 6 ignored reported by parent | Native component result at its tested source; not an integration pass for later grant/barrier corrections. |
| `minibytes` | 11 tests reported passed | Shared-owner component proof, not device teardown proof. |
| Sui Python harness | Latest parent-reported **127 passed** (`bg_78`), including optional Sui `db` label/collision regression | Supersedes 126 before the real metric-label correction. This root analysis-only fix is newer than immutable v2 source archive; no Rust producer change. |
| Full AWS Python suite | Final offline rerun **554 tests run, suite passed, 4 skipped** (`bg_83`), using the command below | Supersedes 546/539-test snapshots and earlier fixture failures; orchestration/source-admission proof only, with no live AWS operations. |
| Main experimental Rust library / strict lint | Full library run **799 passed, 0 failed, 35 ignored**, inspected `artifact://6735`; strict engine clippy `--lib --tests` passed (`bg_55`), then final strict all-target clippy passed (`bg_66`), per parent | Use 799, not the tool aggregate 800 that double-counted a one-test child. The explicit ignored software profile below supplies separate runtime coverage; completed bundle/source identities are recorded below. |
| Grant authority / service-wide barrier resident tests | Current production `.github/scripts/checks.py software-fabric` profile passed per parent (`bg_63`, 120.43 s): all 9 stream tests, 1 direct-packet adapter test and 28 engine tests, **38 total**, including all 15 resident tests | Selector counts were checked and exact ignored tests actually ran. Covers native/TLS controls, real member/group order, fallback/corruption/epoch, physical partition-2 labels, poison/releases/acquisition/shutdown and resident-disabled retirement. Software correctness only; hardware tests not run. |
| Deadline with accepted DMA still pending | `expired_posted_rma_read_fail_stops_with_grant_memory_quarantined` actually passed in the current software-fabric profile | Bounded API return with expected fabric teardown error 16/quarantine retention; no unknown-DMA free. Does not establish that a real failed EFA device was revoked. |
| Revoke before outstanding response TX CQE | Existing regression does not prove a CQE was outstanding at release time | Coverage gap; no evidence of deterministic scheduling of this race. |
| EFA hardware floor, native-speed parity, independent-NVMe scaling | **Not proven** by any local result here | Local functional/replay/timing observations cannot establish hardware quiescence, ≈5 µs remote device reads, native parity or five independent NVMe scaling. |
| Raw CLI software runtime | Both earlier debug and **new bundle release** transport-floor cases completed for 256-byte/8-KiB payloads, 1,000 clean + 64 warmup reads each; release receipt binds the bundle executable and exact drain | Real emulated-provider functionality and scoped local timing, not pure-wire RTT, EFA hardware or whole-DB latency. |
| Native Sui typed-store | Parent reported 34 passed, 0 ignored, using pinned older engine plus feature-gated exporter | Native adapter/exporter evidence only; not a new-engine resident replay or real-checkpoint performance result. |
| Frozen bounded-control source and Rust policy tests | Benchmark/native bounded32 source finished/frozen; parent reported 3 `registered_tests` passed (`bg_68`) | Source/policy regression proof, not matched runtime CSVs or native speed. |
| Six-binary source-bound release build | **Completed successfully**, parent `bg_71`, 1,943.50 s; inspected `build-v2/build-manifest.json` binds all six release binaries, exact Cargo/compiler/environment/log receipts and frozen source archive/manifests | Compilation/source-binding proof, not replay or performance. Initial pre-build oversize failure was corrected solely by anchored `/checkpoint-work/` in existing `.gitignore`; `build.py`, tracked-source binding and size cap unchanged, failed output/evidence preserved. |
| New real-Sui registered replay | **Passed** (`bg_75`), certified private frozen-producer checkpoints 0–1775, both tips/digest equal; exact three selected TidehunterDB and three unchanged RocksDB audits all equal; root corrected analyzer verified archive/source/build/audits (`bg_79`) | Real local compatibility/correctness proof, mixed RPC/RMA. Remote swapping and 15 owner processes on one SSD prevent reliable cluster/native-speed conclusions. |
| Matched bounded32 actual API debug run | Rebuilt current debug `sharded_wal_bench`; native-sync and five-process software-fabric-native RMA both completed 8-KiB/batch32/window1/load320, 10,240-key full reopen audits; real evidence consumers qualified both bounded service populations | Functional local/debug proof, not optimized latency, cluster parity, independent NVMe or release/3,200/cold/Sui performance. Exact evidence is below. |
| Default TLS-TCP/native cold and owner-cache debug controls | Parent reported successful 8-KiB/batch32/load320 cold controls (`bg_73`, 70.34 s) and TLS-TCP coordinator-cache-off / 8-MiB owner-cache control (`bg_74`, 64.75 s), with all 10,240 reopen values verified for each | Actual process-I/O and owner-cache populations are detailed below; local/debug functionality, not per-key NVMe latency or cluster scaling. |
| Release load3200 native/RMA API pair | **Passed** (`bg_76`), 8-KiB/batch32/window1, 102,400 values checked after reopen; both bounded service gates and remote strict registered-control gate qualified | Pure measured bounded32 phases only; unchanged whole-oracle workloads and Sui replay are not relabeled hit-only. |
| Release load320 and separate batch1/window8 five-owner pair | Both pairs completed/reopened values verified (`bg_80`); both native/RMA bounded qualification flags true, RMA strict registered-control qualification true | Five-owner RMA phases actually cover owners 0–4; grouped 16 calls span 80 owner groups / 512 FI_READ operations. No hardware/native-speed implication. |
| Ordinary non-RMA software-fabric RPC release control | Batch32/window1/load320 completed with all 10,240 reopen values verified; exact bounded workload qualifies, but service gate stays false/unclassified | Actual zero physical-read deltas and resident WAL pages do not justify a registered-hit or pure RPC-cache classification. Scalar/group results are reported separately below. |

The exercised local software/release gates are closed by actual runtime and audit evidence, not test discovery alone. Remaining limitations are hardware EFA qualification and failed-device revocation proof, trustworthy independent-host/NVMe performance, native-speed evidence, the explicitly unproven TX-CQE race interleaving, and rebuilding/repackaging current analyzer sources before any later cloud use. Cooperative release tests do not establish malicious-peer security or all failed-device teardown behavior. Nothing here authorizes AWS allocation or promotes debug/loopback measurements to hardware guarantees.

Parent-exercised verification entry points (latest scope/counts are in the table; these commands are not rerun by the document worker):

```bash
cargo test --locked --offline -p tidehunter --features experimental-distributed,test-utils,experimental-fabric --lib
cargo clippy --locked --offline -p tidehunter --features experimental-distributed,test-utils,experimental-fabric --lib --tests -- -D warnings
python3 -m unittest discover -s experiments/aws-sharded-wal -p 'test_*.py'
python3 -m unittest discover -s experiments/sui-checkpoint -p 'test_*.py'
```

The older `verification-before-benchmark-build.json` preserves these command forms and earlier checkpoint-in-time outcomes; do not mistake its then-pending notes or older 546/126 harness counts for the final 554/127 runs. Strict final all-target lint/format were also reported passed by the parent. In the Sui checkout, the separately exercised native typed-store command was `USE_TIDEHUNTER=1 SUI_SKIP_SIMTESTS=1 cargo test --locked --offline -p typed-store --features tidehunter --lib` (34 passed), using its earlier pinned engine as explicitly noted above.

The exercised software-profile entry point was:

```bash
python3 .github/scripts/checks.py software-fabric
```

Its source validates the exact expected selectors before running each selected ignored case with `--exact`, `--nocapture`, and `--test-threads=1`; it fails if a selector silently discovers zero/different tests. The cold-history fixture uses a real one-batch history window, native persistence, cache eviction and `pause_next_authority_load`; the old `read_hook` was not on the active async path and is not cited as proof. The accepted-post deadline fixture establishes epoch 1 before append. These fixture repairs make the tests exercise their intended paths without weakening production authority checks. Existing compatible libfabric discovery/runtime environment is required; the profile does not install dependencies or prove EFA hardware.

### Completed six-binary release/source binding

The successful new owned build is `project:checkpoint-work/resident-rma-2026-10-23-local-v1/build-v2/`. Its inspected `build-manifest.json` records release profile, `fabric: true`, schema 1 and **packaging protocol 9** (not a claim about engine wire-protocol version). `commands[]` binds each successful invocation's cwd, complete Cargo argv, toolchain/compiler versions, artifact path, hash, timestamps and logs; `environment` and `resolved_locks` bind the runtime/build inputs. Native and remote Sui builds used Rust 1.96.1; engine examples/audit use the recorded Rust 1.90 toolchain.

| Frozen identity | SHA256 |
|---|---|
| `sources.tar.gz` | `8744c4e26bac11911ad0178f746f4a45fc04f94d13b55c49db1bca5db5047565` |
| `source-manifest.json` | `355f9684b0214ad935560cfaff4d9a53bd73b428402cea991817a8020603afbb` |
| `sui-node-native` | `a22745f55314f4a7e2e96b42a2031346734c68cce2e4ceb82dfac67572d4b227` |
| `sui-node-remote` | `07a6b073f84080372b807fdad4e1506ebc969030499b3bff054f3969d009b256` |
| `storage-node` | `75dcca9523ae1d4253cbd81be5616656e0ab009fec8708bc9cb780407716198d` |
| `transport-floor` | `7a4c63001fb58303516c8410831ec44f843c2bf3355d68bedfb220c4a119be42` |
| `state-audit` | `123bd04e9c313f7775c563ae025f824dac80946dc35d5d6042559398bba1d446` |
| `rocksdb-audit` | `40e4be7a59f8cbb39e2732d7a62c869db988ca0519a2fff4b1ee4e9e6f418693` |

The manifest's observed environment uses existing libfabric 2.6.0 discovery: `LD_LIBRARY_PATH=/home/hybr1d/.cache/tidehunter/libfabric-2.6.0/lib` and `PKG_CONFIG_PATH=/home/hybr1d/.cache/tidehunter/libfabric-2.6.0/lib/pkgconfig`, offline Cargo, two build jobs and Sui revision `6d4b704bb6b9af6e3d836e08ac3aa2161d443b2d`. These are build settings, not instructions to install a replacement provider or claim hardware. The Rust producers remain unchanged after the freeze. **One later root Python analyzer correction is not present in this immutable archive**: the current root consumer accepts Sui's optional constant `db` metric label while retaining numeric and duplicate-owner checks. It consumed/verified the older archive to emit the final summary; do not claim v2 contains that fix or is the final future cloud-upload bundle. CI must rebuild/package current sources for later cloud use.

### Completed real certified Sui replay and analysis provenance

The actual replay used `project:checkpoint-fixtures/sui-genesis-1775/fixture`, the new six-binary build, software-native fabric, port 24000, coordinator cache zero, owner cache 8 MiB and `project:checkpoint-work/resident-rma-2026-10-23-local-v1/profile-software.json`. This is a certified **private frozen-producer fixture, not mainnet**. Output is `project:checkpoint-work/resident-rma-2026-10-23-local-v1/sui-replay-registered/`.

Both native and remote completed checkpoint tip **1775** with digest `EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`. The verified range contains 1,776 summaries (range SHA256 `896408bc8581e07473e11db805a69295286df17e05cfb90ed0952c45a9286f9f`). `experiment/audit.json` has `all_equal: true`, with three equal selected TidehunterDB database audits and three equal unchanged RocksDB audits. The completed `receipt.json` binds build manifest SHA256 `79b5e8af177311d52f3966add0468ea82ad777026f732a8213766aeb0d4e62ce`, source archive SHA above, and evidence archive SHA256 `bcc16d229fafd4d86528f211c54d676ced21a1bdac91f42f59d4849995f082ee`. `experiment/results.json` records the actual identical resident policy for `live/checkpoints`, `live/store/epoch_0`, `live/store/perpetual`; all detailed read/write profiling limits are zero.

Reproduction uses the recorded settings and a **fresh** output path:

```bash
python3 experiments/sui-checkpoint/run.py \
  --build ../checkpoint-work/resident-rma-2026-10-23-local-v1/build-v2 \
  --fixture ../checkpoint-fixtures/sui-genesis-1775/fixture \
  --output ../checkpoint-work/resident-rma-2026-10-23-local-v1/sui-replay-registered-REPEAT \
  --local-transport fabric-tcp-native \
  --resident-reads ../checkpoint-work/resident-rma-2026-10-23-local-v1/profile-software.json \
  --coordinator-cache-mib 0 --shard-cache-mib 8 --port 24000 --deadline 900
```

The corrected current root `experiments/sui-checkpoint/analyze.py` verified the immutable evidence/source/build/digest/audits and emitted `summary.json`. Its `shard_read_stats` (`:132–149` at correction) now permits only the optional constant Sui `db` label in addition to namespace/partition/stat; it still rejects malformed numeric identities, negative/noninteger counters and duplicate owner/stat identities, including collisions across different `db` labels. Regression `test_sui_database_label_preserves_owner_identity_and_rejects_collisions` covers the discovered bug. The current analyzer SHA256 is `f529105707a94988b2e5179ff66d0bd37aaeaf69384d38c8e74591761eb3a88e`, recorded separately from the v2 producer archive in `verification-final.json`.

#### Mixed RMA/RPC foreground counters—not a hit-rate denominator

The inspected summary retains **cumulative owned-process observations at the foreground boundary**, not invented before/after deltas (`deltas_available=false`). Summed within each real namespace:

| Namespace | FI_READ posts / terminal completions | Verified owner-group hits | Actual Read/ReadProfiled wire requests | Grant acquisitions | Authority-history loads | RMA errors |
|---|---:|---:|---:|---:|---:|---:|
| `live/checkpoints` | 2 / 2 | 2 | 5,390 | 320 | 428 | 0 |
| `live/store/epoch_0` | 4,246 / 4,246 | 2,476 | 1,781 | 320 | 0 | 0 |
| `live/store/perpetual` | 1 / 1 | 1 | 50 | 2 | 0 | 0 |
| Total | **4,249 / 4,249** | **2,479** | **7,221** | **642** | **428** | **0** |

These units differ: elements/operations, owner groups and wire chunks are not interchangeable API-read counts. Do not derive a universal hit fraction from these totals. In the checkpoint namespace there were only **2 RMA operations against 5,390 actual wire Read/ReadProfiled requests**. Most value retrieval there remained ordinary RPC fallback; the replay is **not pure RMA**. Acquisition-on-read, bounded per-session source-region allowance and nonadaptive immutable grants mean RAM-resident new WAL members are not all prepublished addresses. Successful replay demonstrates real grant/RMA use alongside preserved fallback, not elimination of first-read RPC or a solved checkpoint-latency problem.

#### Observed local timing and why it is not cluster performance

| Scope | Native | Remote | Interpretation |
|---|---:|---:|---|
| Foreground process interval, including startup | 50.825168595 s | 81.007381233 s | Observed remote/native **1.593843827×**; this is not native parity. |
| Sampled checkpoint completion window | 49.764983855 s | 72.476793219 s | 250-ms observation resolution; **not individual checkpoint latency**. |
| Sampled aggregate peak RSS | 1,021,112,320 B | 12,702,277,632 B | Different local process populations; remote has 15 owners across three DBs. |
| Host swap delta, pages in / out | 3 / 0 | 108,067 / 650,707 | Remote process `VmSwap` was also positive; material local memory pressure. |
| Final process `VmSwap` | 0 B | Coordinator 1,707,597,824 B + owners 2,878,930,944 B | Actual swapped process memory, separate from host cumulative swap-page deltas. |

All fifteen storage owners shared one SSD and host memory; remote swapping, startup and this topology make the observed ratio unsuitable as a cluster prediction, uncontended-network comparison or independent-NVMe scaling proof. Keep these measurements visible with their limitations rather than suppressing them or promising ≈5 µs/native speed.

### Completed release bounded-control proof

The actual release load3200 pair is `kv-release-native-3200/` and `kv-release-rma-3200/` under the proof root, with derived `release-native-3200-evidence.json` / `release-rma-3200-evidence.json`. Both completion markers pass and the parent reports **all 102,400 reopened values verified**. Both `bounded_hot.qualified` and `service_qualified` are true; remote `registered_controls.qualified` is true. Nine fixed setup passes prepare the same first32 scalar/group sequence.

Measured remote scalar: 512 eligible groups, 512 posts/completions/hits. Measured unique32 grouped phase: 16 calls, 512 verified elements/posts/completions and 16 group hits. Both phases have zero actual wire read messages, new acquisitions, errors and every fallback reason. Native service qualifies as zero-accounted-I/O buffered hotset with residency; RMA service is strictly registered-hit-only. The hotset's actual registered lane coverage is owner 1/lane0–3, not an assertion that all owners handled this prefix.

Separate release load320 native/RMA and batch1/window8/load320 five-owner pairs completed and passed reopen/value verification. Their directories are `kv-release-{native,rma}-320/` and `kv-release-{native,rma}-five-owner/`; exact consumer reports are `release-{native,rma}-{320,3200,five-owner}-evidence.json`. **Every native/RMA pair has `bounded_hot.qualified=true` and `service_qualified=true`; every RMA control also has strict `registered_controls.qualified=true`.** In the five-owner RMA case, scalar and grouped phases genuinely cover owners `[0,1,2,3,4]`: scalar has 512 posts/completions and 512 completed groups; grouped has 512 posts/completions over **80** complete owner groups (16 API calls × 5 owners). Both measured phases have zero actual wire read messages, new grant acquisitions, fallback reasons and errors. Bounded32 qualification does not relabel the whole oracle or Sui replay.

#### Measured release API timings, including the RPC control

`release-timing-evidence.json` retains the exact rows, bound to the new bundle's `storage-node` SHA256 `75dcca9523ae1d4253cbd81be5616656e0ab009fec8708bc9cb780407716198d`. The table reports single-run p50 **microseconds**; scalar has 512 samples, while grouped has only 16 samples and measures the **entire 32-key `get_many` call**, not one member:

| Loaded context / local software path | Scalar `get` p50 (µs) | Whole unique32 `get_many` p50 (µs) |
|---|---:|---:|
| batch32/window1/load320 native, qualified resident file service | 0.984 | 36.320 |
| Same context RMA, qualified registered-hit service | 18.643 | 494.926 |
| Same context ordinary non-RMA software-fabric RPC | 42.758 | 463.631 |
| batch32/window1/load3200 native, qualified resident file service | 1.011 | 34.958 |
| Same enlarged context RMA, qualified registered-hit service | 22.858 | 661.478 |

The RPC control's `release-rpc-320-evidence.json` qualifies the fixed bounded **workload**, not its service type: `service_qualified=false`/unclassified, no RMA claim. Actual owner intervals show zero accounted physical reads with all WAL pages resident; do not turn that into fabricated exact cache-tier counters. In this one local software sample RMA scalar p50 is lower than RPC, but whole-group p50 is about **6.75% higher**, so it does not improve every metric. An 8-KiB × 32 RMA group posts 32 FI_READ operations, while ordinary RPC can batch a single owner's reply. Hardware/provider selection needs real matched hardware data rather than assumptions about an emulated provider.

These 320/3,200 rows are same-candidate source-bound **load controls**, not a historical implementation-regression comparison. One run per case and 16 grouped samples do not establish tails, significance, scaling or transferable EFA latency; no p95/p99 claim is made. Native remains substantially faster here; the result does not establish parity.

### Exercised paired native and resident-WAL debug API proof

Parent run `bg_70` completed successfully in 104.73 seconds with rebuilt root `target/debug/examples/sharded_wal_bench`. Both controls used 8,192-byte values, batch32, write window1, 320 load batches (10,240 keys / 80 MiB initial payload), 512 scalar samples, four target read lanes, zero native/coordinator/shard value caches, resource evidence and the shared bounded-hot pass. Both `kv-debug-native-320/COMPLETE` and `kv-debug-rma-320/COMPLETE` state that all selected cases and reopen audits passed; the parent reports each full reopen checked all 10,240 values.

Artifacts under `project:checkpoint-work/resident-rma-2026-10-23-local-v1/`:

- `kv-debug-native-320/` and `kv-debug-rma-320/`: actual CSV/JSON/control metadata and completion markers.
- `debug-native-evidence.json`: `bounded_hot.qualified=true`, `service_qualified=true`; both measured phases classify as `native_buffered_hotset_zero_accounted_IO_with_residency`.
- `debug-rma-evidence.json`: both bounded qualification flags true and `resident_reads.registered_controls.qualified=true`; both measured phases classify as `registered_hit_only`.

Both policies record exactly nine completed warm passes and 9,216 verified setup elements. Each measured scalar phase has 512 valid calls/elements; each grouped phase has 16 valid unique32 calls / 512 elements. Actual remote scalar deltas are 512 eligible groups, 512 posts, 512 completions and 512 verified group hits; actual grouped deltas are 16 eligible groups, 512 posts, 512 completions and 16 verified group hits. Both have **zero** message reads, new acquisitions, errors and every fallback reason. The bounded hotset in this run was served by owner 1; all four of its lanes were covered. This does not claim that all five owners served hotset RMA hits.

The native measured intervals have zero accounted physical reads and positive unchanged file residency; remote intervals likewise have zero accounted physical reads for the exact five-owner inventory. These are RAM-service/registered-copy functional populations, **not physical-NVMe throughput**. No debug timing ratio or percentile is promoted as hardware performance. The later completed release and real-Sui results above supersede this earlier debug-only handoff; the separate debug cold/owner-cache proofs follow.

The parent used the real offline consumer entry points (without replacing preserved derived evidence):

```bash
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence ../checkpoint-work/resident-rma-2026-10-23-local-v1/kv-debug-native-320 --mode native-sync
python3 experiments/aws-sharded-wal/checkpoint_kv.py evidence ../checkpoint-work/resident-rma-2026-10-23-local-v1/kv-debug-rma-320 --mode tcp
```

### Exercised cold-I/O and ordinary owner-cache debug controls

The same proof root additionally contains `debug-native-cold-evidence.json`, `debug-tcp-cold-evidence.json` and `debug-tcp-owner-cache-evidence.json`, produced by the actual offline consumers after successful parent runs. These are different service populations, not aliases for registered hits.

| Actual debug population | Observed evidence | Valid conclusion / limit |
|---|---|---|
| Native-sync cold scalar, grouped and windowed APIs | Each of the three `cache_evicted_get`, `cache_evicted_get_many`, `cache_evicted_get_window` intervals charged **9,908,224 physical-read bytes**, with WAL residency zero before | Physical process-interval I/O occurred; no per-key device latency, all-cold guarantee or NVMe saturation claim. |
| Default mTLS TCP cold APIs, five owners | Each of the same three intervals charged **6,086,656 bytes summed across the exact five owners**, with zero prior WAL residency | Ordinary RPC/file fallback really performs I/O. It is not a five-independent-NVMe scaling result. |
| Default mTLS TCP with 8-MiB owner encoded cache, coordinator cache zero, 256-key cache probe | Both `cache_hot_before_scan` and `cache_hot_after_scan`: **512 owner-cache hits, zero owner WAL reads, zero coordinator hits, 512 coordinator misses**; actual read-service classification `shard_cache_hit_only` | Ordinary RPC is served from the owner CLOCK cache, including after the scan. It is not RMA and is not a coordinator-cache hit. |
| Same owner-cache run's bounded32 scalar/grouped phases | Generic `bounded_hot.qualified=true`, zero accounted physical I/O, actual read-service `shard_cache_hit_only`; `bounded_hot.service_qualified=false` | The generic workload/sample proof passes, but the explicit bounded native-versus-RMA comparator cannot admit this RPC-cache service: its service gate deliberately recognizes native RAM and proven RMA, not ordinary owner-cache RPC. No registered-hit claim. |

Native cold evidence correctly leaves the generic owner-counter classifier as `unqualified_missing_counters`: native has no remote Node counters to fabricate. That **does not erase** the separate actual positive `read_bytes` and residency brackets. TCP cold authority counters report zero history loads at capacity 1,024, demonstrating that metadata authority and value-file I/O are separate in these runs; authority cache capacity/history loads are not themselves disk-service evidence. No latency improvement magnitude is inferred from these cache observations.

Together the paired resident runs, raw probes, cold brackets and owner-cache controls exercise distinct native/kernel RAM, registered resident copy, ordinary owner encoded-cache RPC and physical-I/O fallback paths. These are local software-provider observations; debug and release timings remain separate, with full-value/reopen correctness checked. No new AWS resources, EFA hardware result, global WAL, remote atomic protocol or NVMe-oF implementation is implied.

### Exercised raw software command and artifact identity

The parent reported rebuilding `target/debug/examples/sharded_wal_transport_bench` and successfully executing:

```bash
python3 experiments/aws-sharded-wal/raw_rma_floor.py local-software \
  --binary target/debug/examples/sharded_wal_transport_bench \
  --output /home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/resident-rma-2026-10-23-local-v1/raw-software-debug-v2 \
  --samples 1000 --warmup 64 --deadline-ms 30000
```

Evidence root: `project:checkpoint-work/resident-rma-2026-10-23-local-v1/raw-software-debug-v2/`. Its `receipt.json` has `complete: true`, `hardware: false`, binary SHA256 `accf37b0db04e38fb285eb54d186bf09ff80dad1b7c05eced60895bcbc330082`, both payloads, actual 280/8,216-byte transfers and `drain_complete: true`. Each `256/` and `8192/` case retains `client.json`, `server.json`, `processes.json` and stdout/stderr logs. The process receipts bind the actual debug executable/argv, two zero exit codes and complete cleanup. Client reports identify `tcp;ofi_rxm`, domain `lo`, offset address mode, **emulated** reads, `busy_spin`, 64 completed warmup plus 1,000 completed clean samples, and actual allowed CPUs `0-19`. The stored CDFs are debug software completion/API-validation populations; no hardware-performance percentile is promoted here. The parent's retained libfabric runtime environment is also required when reproducing the binary; no environment values were supplied to invent in this command.

The new **release** wrapper run also completed at `project:checkpoint-work/resident-rma-2026-10-23-local-v1/raw-software-release/receipt.json`: `complete=true`, `hardware=false`, both source/payload validations and `drain_complete=true`, with the exact bundle transport-floor SHA256 `7a4c63001fb58303516c8410831ec44f843c2bf3355d68bedfb220c4a119be42`. Both payloads used 1,000 clean + 64 warmup samples. Median FI_READ **completion** was 3,390.5 ns / 4,544 ns for 256/8,192-byte payloads; median client **API including deterministic pattern validation** was 5,576.5 ns / 12,946 ns. The actual transfers remain 280/8,216 bytes with the identity header. These are different timing scopes, not pure-wire RTT or complete DB latency. Do not subtract medians to fabricate stage costs or compare either directly with a native key lookup.

That execution caught and corrected a real pre-peer `NotConnected` bug: raw source reservation/registration now follows bootstrap `set_peer`, and provider CQ progress does not run before the peer is installed. This is a functional runtime correction, not evidence of instant failure detection or a new teardown guarantee.

## 10. Research/source mechanism map

The verified research handoff was `history://WalReadReferences:134-175`; primary pages were checked where needed. QMD collection identity is `tidehunter-papers`; `papers/parsed` contains local PDF-derived Markdown, not a collection name. The guide adopts mechanisms, **not** unverified literal code copying or portable paper latency targets.

| Primary reference | Mechanism borrowed / branch mapping | What is not adopted or demonstrated |
|---|---|---|
| [Tidehunter upstream](https://github.com/MystenLabs/tidehunter), local `project:papers/parsed/Tidehunter-2026-v2.md`, §§3.1–3.4, 4.1–4.4 | Index-to-Value-WAL resolution; complete append/durable/recovery authority; local cache/mapping mechanisms. Preserve in `storage.rs`, `reference_directory.rs`, `native_index/retention.rs`, `wal/mod.rs`. | Warm local memory access is not physical NVMe latency. A resident copy is not an independent authoritative database or replica. |
| [TeRM, FAST ’24](https://www.usenix.org/conference/fast24/presentation/yang-zhe); pinned [`mr.cc`](https://github.com/thustorage/TeRM/blob/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/libterm/ibverbs-pdp/mr.cc), [`rpc.cc`](https://github.com/thustorage/TeRM/blob/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/libterm/ibverbs-pdp/rpc.cc) | Explicit hot resident one-sided / nonresident software-service split; branch `Client::{submit_read,prepare_rma_read,progress_rma_read}` plus ordinary worker/RPC fallback. TeRM source checks residency and uses registered bounce memory with buffered/direct file service for misses. | Modified Mellanox ODP/page tables, magic-page scheme and driver/interposition are not EFA mechanisms. ConnectX-5/100-Gb/s InfiniBand/Optane results are not EFA or all-cold NVMe targets. |
| [eRPC, NSDI ’19](https://www.usenix.org/conference/nsdi19/presentation/kalia); [author repository](https://github.com/erpc-io/eRPC) | Exclusive progress/buffer ownership, short dispatch handlers, moving long file work off the network owner; `tcp/reactor.rs`, `read_executor.rs`, CQ-owned fabric buffers. Failure/retransmission needs DMA drain before reuse. | eRPC is not imported as middleware and its DPDK/InfiniBand transport is not EFA. Its tiny-RPC RAM measurements do not establish Sui/WAL/`fi_read` latency. |
| [FUSEE, FAST ’23](https://www.usenix.org/conference/fast23/presentation/shen); [author-linked implementation](https://github.com/dmemsys/FUSEE) | Conceptual remote-address/handle reuse with stale-reference validation; branch full-reference/session-scoped handles and immutable envelope validation. | FUSEE's distributed mutable index, replication, atomics and memory-store failure protocol are not this coordinator-authoritative disk-WAL architecture. Its RNIC testbed is not EFA. The earlier handoff's `/presentation/lu` URL names Perseus; the corrected FUSEE primary is `/presentation/shen`. |
| [CETOFS, FAST ’26](https://www.usenix.org/conference/fast26/presentation/jia) | Distinguish userspace network data path from target software translation, permission/concurrency checks and actual storage I/O. Branch cold RPC retains owner-side file service. | Not an RNIC fetching arbitrary file/NVMe offsets. The cited ≈19 µs 4 KiB Optane result includes network/device/software components; it is not a resident ≈5 µs RAM comparison or proof of EFA disk fetch. |
| Libfabric 2.6 [`fi_efa`](https://ofiwg.github.io/libfabric/v2.6.0/man/fi_efa.7.html), [`fi_rma`](https://ofiwg.github.io/libfabric/v2.6.0/man/fi_rma.3.html), [`fi_mr`](https://ofiwg.github.io/libfabric/v2.6.0/man/fi_mr.3.html), [`fi_cq`](https://ofiwg.github.io/libfabric/v2.6.0/man/fi_cq.3.html), [`fi_close`](https://ofiwg.github.io/libfabric/v2.6.0/man/fi_close.3.html) | Actual asynchronous memory-read contract, local/remote descriptors and permissions, capabilities, CQ ownership and teardown obligations. Maps to `tidehunter-fabric` only. | API support is not native-device qualification or a general failed-endpoint DMA-quiescence guarantee. |
| Pinned libfabric `3cc1095272b07ebeb5cc8f44851af6aa3eb75e41`: [`efa_fabric_comparison.md`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/docs/efa_fabric_comparison.md), [`efa_rma.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_rma.c), [`efa_mr.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_mr.c) | Evidence for direct-provider caps/modes, target AV insertion, native read posting, local descriptors, and the MR-close wrapper caveat. | A local close result, timeout or generation increment is not proven revocation of all already-posted remote DMA. This source pin is a research identity; runtime library/build identity still has to be recorded independently. |

The [AWS supported-instance table](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html#efa-instance-types) supplies instance-type capability context only. It is not an AWS operation or proof of an attached device selected by a particular experiment.

## 11. Local CI readiness repairs

The earlier cloud preparation failed before a complete campaign. Run
`40078567891` was cancelled before its benchmark job allocated EC2. Its closure
receipt is under
`project:checkpoint-work/aws-ci-runs/40078567891-1/failure/aws-mcp-cancelled-closure.json`.
There is no new cloud performance comparison from that cancelled run.

The local repair branch is `fix/software-fabric-local-liveness-2026-10-24`.
These changes fix readiness and observation, not the physical EFA latency floor.

| Failure or risk | Mechanism and code |
|---|---|
| A delayed diagnostic poisons a connection needed for a real append. | `remote_db.rs` separates explicit `ReadEvidenceMonitor` from inert `OperationalReadIoMonitor`. Its DB-owned sampler remains dormant until a monitor is requested: no thread, diagnostic connection, or extra strong/weak `ReadService` reference exists before activation. The lifecycle mutex serializes activation and stop; `ConcurrentRemoteDatabase` holds only a weak lifecycle reference. Active sampling uses independent, RMA-disabled clients. `tcp.rs` and `tcp/reactor.rs` accept a version-checked operational first request without the serving `Status` barrier. That session cannot issue data requests or upgrade through `Status`; every reply binds the configured partition identity. |
| Software-provider receive pools exceed the small CI host's practical memory budget. | `experiments/resident_config.py::software_fabric_environment` fixes `FI_OFI_RXM_MSG_RX_SIZE=128` for explicit software runs and rejects conflicting inherited values. Deployment, the raw software CLI and the software CI profile use this policy. The pinned RxM implementation registers this setting in `rxm_init.c:250–258` and uses it for the shared receive queue at `664–666`. Hardware configuration is not changed. |
| Idle manual-progress sessions compete with Sui cleanup on two CPUs. | Software `tcp;ofi_rxm` message-only endpoints qualify `FI_WAIT_UNSPEC`; `Endpoint::try_progress_for_sweep` can replace its single CQ acquisition with provider-managed `fi_cq_sread`. A wait requires drained message state, all receives posted, no TX, RMA or direct state, and at least one millisecond remaining. The requested wait is at most one millisecond. The reactor arms it only for an established, drained adaptive server session. Active clients and hardware EFA/direct keep their existing progress policy. |
| A collector retains a DB or waits on its serving connections. | `distributed/sui.rs` supplies try-only local metric reads. Sui `typed-store/src/tidehunter_util.rs` reads owner snapshots directly from inert state and uses a separate Weak-DB publisher for local adapter gauges. `Database` owns and stops that publisher before storage teardown or exclusive DB recovery. The collector takes no serving client mutex and performs no RPC. |
| Concurrent local publication and collection produce partial or misleading metric families. | Sui serializes publication and collection with one metrics-only mutex. DB reads remain try-only. `tidehunter_operational_read_io` contains exactly six owner-status fields; seven aggregate owner counters stay in `tidehunter_adapter` and follow owner availability. Each successful owner vector must contain all 23 base counters and every availability-gated authority/initiator counter. Sampling age starts before the first owner RPC, not when the last reply is published. Failed, stale or stopped samples cannot qualify. These locks are outside the read/write fast path. |
| A new opcode differs between the storage executable and Sui's feature set. | Production operational variants precede the `cfg(test-utils)` tails in both request and response enums. The transport wire version is `10`; experiment packaging protocol remains `9`. These are separate version domains. |
| A final scrape contains counters sampled before checkpoint completion. | `run.py::wait_for_final_metrics` takes a baseline after index catchup and certified checkpoint verification. Each required live namespace must advance its local stream and, for remote mode, its owner stream by at least two generations with valid availability and age. This wait is outside foreground timing and inside the original remaining replay deadline. `analyze.py` validates that cut and keeps historical evidence separate. |

The diagnostic request reports temporary lock contention as unavailable. It does
not imply a broken transport. The sampler retains that diagnostic connection on
`Ok(None)` and replaces it only after an actual request failure. It must not
recreate an EFA endpoint for every contended snapshot.

No original Tidehunter data-path lock was removed. WAL allocation, durable
receipts, contiguous publication, read authority, registered-buffer ownership
and quarantine rules are unchanged. A CQ completion still does not prove
durability or arbitrary remote-DMA quiescence. A bounded provider timeout is not
a hard scheduler-latency guarantee.

### Observed local evidence

- The original delayed-diagnostic regression failed at a real append with
  “connection requires replacement; request outcome may be unknown.”
  `diagnostic-interference-red.json` records the test executable identity under
  `project:checkpoint-work/software-startup-repro-2026-10-24/`.
- The repaired regression requires sampling attempts while inventory is held,
  a successful real append/read after release, then a complete later owner
  sample accounting for that WAL read. It passed after the protocol repair.
  Separate role tests passed on TCP, TLS software fabric and native software
  fabric; they reject wrong owner identity, data admission and a `Status`
  upgrade on a diagnostic session.
- In the old-binary constrained replay, the original ten-second shutdown grace
  failed. The captured stopped instruction was in Sui's upstream Moka hash-map
  destructor. Moving only the owned storage threads off Sui's two CPUs then
  allowed natural exit `0` after `0.951978` seconds; all selected Tidehunter and
  unchanged RocksDB row audits matched. This is a diagnostic CPU-contention
  experiment, **not** a passed original-deadline admission run. Records are in
  `replay-two-cpu-storage-release/experiment/`.
- The real software-provider transfer tests exercise idle arming, byte delivery,
  bidirectional progress, pending-send and buffered-receive exclusions, and
  registered-read exclusion. The software path is not hardware RDMA evidence.

The functional bundle with source SHA-256
`753ba725f05eff6ac9466e70645f352ddfec05ab1a9c3f6573d325f7e99c1f66`
is retained as `rebuilt-current/build-functional-validated/`. Its three completed
replays use the same frozen, certified private Sui chain: checkpoints `0..1775`,
1,776 verified summaries, digest
`EhUPyfwLGDEKH5Ssg3nFbVAHwi4XA87xNKvnFEo3ZJb6`. This is not mainnet data.
Both native and distributed nodes exited `0`. All three Tidehunter namespace
audits and the unchanged `epochs`, `indexes` and `rpc_store` RocksDB audits
matched. Each sealed archive passed `analyze.py --verify-only`, including
post-certified two-generation local/owner cuts.

| Completed local exercise | Evidence directory under `software-startup-repro-2026-10-24/` | Scope |
|---|---|---|
| Ordinary CPU affinity | `replay-rebuilt-normal/` | Native and TLS software-fabric replay on the available 20 CPUs. |
| Original constrained topology | `replay-rebuilt-two-cpu/` | All children on CPUs `0,1`; original 900-second replay and ten-second shutdown grace, no tracing, no CPU release and no longer deadline. All 177 complete remote resource samples report zero owned-process swap; peak summed RSS is 7,965,220,864 bytes, not unique physical memory. |
| Registered-read integration | `replay-rebuilt-registered-software/` | Coordinator cache off, owner cache 8 MiB, explicit software provider. Final cumulative counters show 710 actual FI_READ posts/completions/hits and 8,985 message reads across 15 owners. This is a mixed natural replay, not a pure RMA-hit benchmark. |
| Raw registered-read API | `rebuilt-current/raw-fi-read-software/` | Both 256-byte and 8,192-byte cases complete 64 warmup and 1,000 clean reads with exact drain and process cleanup. Provider is explicitly `tcp;ofi_rxm`, read mode `emulated`; no hardware claim. |

Strict engine profiles completed with 385 native tests, 805 distributed tests,
806 fabric-enabled engine tests, 49 native-fabric unit tests and all 49 explicitly
selected software-provider cases passing. Existing ignored cases remain visible
in `rebuilt-current/local-gates/7268`. The corrected inherited-offline harness
passes 608 AWS tests (four existing skips) and 164 Sui harness tests, recorded in
`local-gates/7283`. Native Sui storage passes 35 library tests (`local-gates/7285`).
Fresh local `file://` Git fixtures alone permit their initial Cargo checkout;
production Cargo checks remain locked and offline.

The final lint-clean source archive is
`0c1411cafa2cc04174e11342f18af035e5136f38b1abe963b43437ad5f068de1`.
All six executables were rebuilt from that snapshot after the Sui control-flow
simplifications; native Sui's 35 library tests and strict library Clippy passed.
`replay-final-two-cpu/` then passed the unchanged constrained replay, and
`replay-final-registered-software-resume/` passed the normal-affinity registered
replay. Both use the final source hash, native/distributed exit `0`, complete
Tidehunter/RocksDB equality and valid post-certified freshness cuts. Both sealed
archives pass strict verification. These are the final runtime gates; do not
substitute the earlier functional source hash for them.

One intervening registered replay was cancelled for a mistyped extra environment
variable and is explicitly marked inadmissible. Its immediate replacement hit a
local `Address already in use` startup error. No architecture change or timeout
increase was made for that port collision; after listeners cleared, the final
unchanged binaries passed. Failure logs remain separate from successful evidence.

GitHub OIDC was exercised separately by cost-report-only run
[`40092596198`](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/40092596198).
Its prepare and benchmark jobs were skipped; it could not allocate EC2.
The downloaded accounting artifacts agree on ledger revision 56, no active
reservations, estimated usage `$76.571582257221` and estimated available budget
`$23.428417742779` under the unchanged `$100` weekly limit. Cost Explorer remains
unavailable, not zero. Before the subsequent user-requested reload, MCP execution
returned `Unknown tool: aws___run_script` and the named local login had expired.
OIDC success alone is not an EC2 inventory or a hardware benchmark result.


### Remaining publication gates

The final local runtime gates above are complete. Before a paid campaign,
publish the compatible engine and Sui repair commits, update the immutable Sui
pin, and pass the protected review/checks before merging to `bench/aws-efa`.
Do not run the older benchmark branch as a substitute for testing these repairs.
The final local bundle lives at `rebuilt-current/build/`; its six successful
Cargo invocations and executable hashes are in `build-manifest.json`.

The earlier one-NVMe and software-provider timings remain local observations.
Neither these repairs nor passing local tests demonstrate independent-NVMe
scaling, hardware EFA/direct latency, or a new microsecond remote-read result.

