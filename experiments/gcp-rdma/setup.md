# GCP benchmark activation

The workflow is disarmed unless the `gcp-rdma-bench` environment variable
`GCP_BENCH_ENABLED` is exactly `true`. This repository does not create or grant
any of the infrastructure below. When explicitly enabled and approved, a push to
`bench/gcp-rdma` builds and qualifies the selected campaign. The current default
is the standard distributed-KV comparison, including native, TCP, portable verbs,
Valkey TCP/RDMA, Garnet and Kvrocks; focused NVMe experiments remain explicit.
A manual dispatch can choose `authenticate`, `preflight`, or `launch`.
`authenticate` only requests a short-lived token. `preflight` can run while paid
activation remains off: it rebuilds, qualifies locally, and checks cloud
prerequisites without reserving budget or creating resources. Every pull request
targeting `bench/gcp-rdma` runs the complete offline GCP safety suite without a
protected environment, OIDC permission, or cloud credentials. Cancellation is
not proof of fleet cleanup; absolute server-side DELETE expiry is mandatory.

Correctness jobs check out the immutable event SHA unless a manual
`tidehunter_repo_ref` override is supplied. Queued PR jobs must not depend on
`refs/pull/N/merge`, which can disappear when the pull request is merged.

## Repository administration

1. Create the GitHub environment **gcp-rdma-bench** under Settings →
   Environments. Under deployment branches, select **Selected branches and tags**
   and add `bench/gcp-rdma`. For the selected solo-maintainer setup, do not require
   another human reviewer or environment approval; the branch restriction and
   explicit disabled activation remain mandatory. Add reviewer protection later
   if another authorized reviewer is available. See [deployment environments](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments).
2. Publish these changes on a topic branch and open a pull request targeting
   `bench/gcp-rdma`. Keep paid activation off. Wait for the **GCP benchmark safety**
   check, then import `experiments/gcp-rdma/ruleset.json` under Settings → Rules →
   Rulesets before merging. The template requires that exact check from GitHub
   Actions integration **15368**, with an up-to-date base, a pull request, resolved
   discussions, no force push/deletion, and no bypass actors. It needs zero human
   approvals so the sole maintainer can merge. The template is not evidence that
   the live rule was changed; automatic Codex review is not a required check.
   See [rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository).
3. In that environment's Variables tab set:

   | Variable | Meaning |
   | --- | --- |
   | `GCP_BENCH_ENABLED` | Keep unset or `false` until every protection and cloud prerequisite is reviewed; set exactly `true` only to activate. |
   | `GCP_BENCH_WIF_PROVIDER` | Full provider resource name `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID`. |
   | `GCP_BENCH_SERVICE_ACCOUNT` | Orchestrator service account email granted the permissions below; it must match the WIF impersonation binding. |
   | `GCP_BENCH_IMAGE` | Exact reviewed custom image path. Use this together with the qualification variable while leaving the repository image fields blank. |
   | `GCP_BENCH_IMAGE_QUALIFICATION` | JSON object with `bucket`, `object`, decimal-string `generation`, and SHA-256 `sha256` for the reviewed receipt. Neither field may be supplied alone. |
   | `GCP_BENCH_RUNNER` | Optional **repository** variable (Settings → Secrets and variables → Actions → Variables), not an environment variable; runner selection occurs before environment variables are available. Default `ubuntu-24.04`. Select a trusted x86-64 Ubuntu 24.04 runner with adequate disk, memory and time; do not use a shared untrusted runner. |
   | `GCP_BENCH_FIXTURE_KEY` | Required for `sui` or `both`: object name of the reviewed fixture in the configured bucket. |
   | `GCP_BENCH_FIXTURE_GENERATION` | Required for `sui` or `both`: exact decimal GCS generation, never `latest`. |

   The accepted fixture is the real genesis-through-1775 portable archive with
   SHA-256 `7eb43b71435547939760598e541d13b9a77f722c5a9d0f8c8ee5bd583b751f15`;
   the workflow validates the bytes and complete fixture inventory. The Sui
   checkout is the reviewed `Loris-EPFL/sui-tidehunter-distributed` commit
   `ef6701911cc07d9102f55e52f5a0632e5ddb0a86`, not a moving branch. The
   repository is public, and checkout does not persist credentials. Never use a
   Google service-account key.
4. Update and review `config.json` and `budget.json` on the protected branch.
   The protected paid launcher currently admits exactly six `z4m-highmem-16`
   hosts in `us-central1`, with compact placement distance at most 3. Other
   shapes and regions require a separately reviewed price and policy change;
   the generic runtime's H4D support is not permission to launch it from CI.
   Keep the exact guest image empty until its drivers, kernel, complete binary
   ABI, tools and cached images are qualified. Blank means launch is blocked.
   Set the editable weekly reservation limit in `budget.json`; each CI run reserves $90 for at most
   four hours. Six hosts at the reviewed $3.287109973 hourly rate cost at most
   $78.90 rounded up for compute and mandatory Local SSD, leaving $11.10 for
   expected ancillary charges. This is not a project-wide spending cap.

## Current US launch preflight

The checked-in target is now `z4m-highmem-16` in `us-central1-a`, using ordinary
subnet `projects/mysten-research/regions/us-central1/subnetworks/default`, Falcon
subnet `falcon-rdma-us-central1-a`, and profile `us-central1-a-vpc-falcon`.
The current account can create/delete VMs and use both US subnets. The ordinary
subnet has Private Google Access enabled; the Falcon profile correctly blocks it.
These permission checks do not establish the future GitHub identity's access.

The administrator supplied the workflow identities and bucket on 2026-11-02.
The protected environment now contains:

| Setting | Value |
| --- | --- |
| `GCP_BENCH_ENABLED` | `false` |
| `GCP_BENCH_WIF_PROVIDER` | `projects/514867804006/locations/global/workloadIdentityPools/github-pool/providers/tidehunter-bench` |
| `GCP_BENCH_SERVICE_ACCOUNT` | `gh-bench-orchestrator@mysten-research.iam.gserviceaccount.com` |
| Guest account in `config.json` | `bench-guest@mysten-research.iam.gserviceaccount.com` |
| Artifact bucket in `config.json` | `mysten-research-bench-artifacts` |

The administrator restricts GitHub federation to this repository, branch and
environment. The launcher and recovery use `bench-` plus 16 hex digits; each
VM and its explicitly named boot disk start with that identity. Admission
requires the exact guest account. Our maximum absolute DELETE lifetime is
four hours for GitHub or six hours for the reviewed manual fleet. The
administrator's independent six-hour `bench-*` reaper remains a backstop;
it can race the manual deadline rather than leaving additional slack.
Workflow concurrency still limits that workflow/ref. Bucket admission uses
`lifecycle/active-runs.json`, a generation-CAS registry of per-run leases keyed
by `run_id`. `config.max_concurrent_runs` defaults to 3. Every lease reserves
its whole fleet before uploads or allocation; expired leases are never stolen.
The legacy `lifecycle/active-run.json` counts as one active run and is not
rewritten or deleted when another run starts. Only its own verified cleanup
can release it. These controls are not project-wide billing limits.

Local `loris.tran@epfl.ch` cannot list objects directly, but MCP object listing
with short-lived guest-account impersonation succeeded. This establishes the
tested local impersonation/storage path, not GitHub OIDC or orchestrator
launch permissions. Conditional resource-name grants may not be reflected in
project-wide permission tests; report an actual preflight failure rather than
removing admission or granting broader access.

The local workflow now offers dispatch operation `authenticate`. It only
requests a WIF access token and does not print it. It requires no paid
activation, image, budget reservation or build and cannot allocate a fleet.
Publish the reviewed workflow before dispatching it; neither its GitHub token
issuance nor the full CI campaign has been exercised yet. Keep activation off.

The image remains a genuine blocker for the full matrix. The manual Rocky 8
guest has glibc 2.28 and no `io_uring`; it is not a substitute for a qualified
image compatible with the Ubuntu 24 sealed binaries and asynchronous WAL
path. Rocky 10 optimized is only a candidate custom-image base, with driver
installation and hardware qualification still required. Do not populate the
image field merely because a public catalog image is READY.

Local verification counts below refer to their dated source snapshots, not
this preparation pass. Six-host quota/capacity, the prepared image, and the
GitHub principal's actual launch rights remain untested.

The published [run 41365518136](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/41365518136)
was green because activation was disabled: authentication, compilation and
allocation were skipped. It is not a completed benchmark campaign.

`tidehunter_verbs_native` selects trusted/no-TLS verbs messaging, not one-sided
FI_READ. The standard campaign does not build or invoke the existing raw FI_READ
probe. Valkey's selected RDMA arm has real RDMA PING/SET/GET/DEL admission.
Hardware evidence must distinguish these paths; neither a backend label nor a
local TCP qualification establishes an executed Falcon transfer.

#### Two-host live NVMe/RDMA counter admission

Two Falcon qualification pairs connected using RC send/read and loaded the Linux
NVMe/RDMA modules, but neither admitted the fleet. The first collector encountered
`EINVAL` on an unmapped `gid_attrs/ndevs` slot; only that slot is skipped, and
the routed interface must still map to exactly one ACTIVE port. The second
encountered missing `counters/port_xmit_data`. The classic InfiniBand PMA
`counters` directory is not universal: Linux creates it only when the provider
supplies `process_mad` ([sysfs implementation](https://github.com/torvalds/linux/blob/v7.0/drivers/infiniband/core/sysfs.c)).
Missing PMA does not itself establish an absent or broken RDMA route.

For the exact mapped port, the collector discovers the fixed pairs independently:
PMA `counters/port_xmit_data` and `port_rcv_data` (units of four octets);
IRDMA `hw_counters/ip4OutOctets` and `ip4InOctets` (octets); and
`hw_counters/OutRdmaSends` and `InRdmaSends` (operations). The latter names are
Linux IRDMA driver candidates, not a claim that the deployed Google provider
exposes or advances them ([driver source](https://github.com/torvalds/linux/blob/v7.0/drivers/infiniband/hw/irdma/verbs.c);
[Google IRDMA documentation](https://docs.cloud.google.com/compute/docs/networking/using-irdma)).
Both missing attributes mean an unavailable family; a partial pair, malformed
value, access error, changed port identity or counter reset is a failure.
The route snapshot also records a bounded raw listing of the port's `counters`
and `hw_counters` filenames for provider ABI discovery. Unknown names in that
listing are diagnostics only: their presence or change is not counter evidence
and cannot authorize admission.
All supported pairs retain their paths and units. Full admission requires at
least one *same* family advancing in both directions on both hosts, in addition
to the actual kernel namespace, route, operation completions, data integrity,
recovery, cache, fencing, shutdown and no-payload-RPC checks. Shared port
counters are independent route evidence, not attribution to a particular QP.
If counters are unavailable, bounded kernel/progress diagnostics may still be
captured, explicitly partial and non-admitted; they cannot authorize the fleet.
Optional partial exports are admitted conservatively as whole operations only
when enough time remains for the trusted GCS/local-token 30-second and POST
30-second caps, plus a margin and the final-status reserve. The reserve is
checked before each optional export; it is not an absolute HTTP wall-clock
clamp. The VM DELETE deadline and systemd maximum runtime remain independent
hard limits.

### Bounded lifecycle and recovery

The launcher persists each create intent and request ID before calling Compute.
Ambiguous requests reuse that ID; they do not allocate a second fleet. Budget
reservation follows acquisition of the active-run lease. Expiration never
authorizes taking another run's lease.

The normal path collects generation- and SHA-bound exports, then attempts cleanup
of every recorded VM, boot disk and placement policy. It releases the lease only
after proving their absence. Cleanup checks numeric IDs and source/run ownership;
it never deletes a resource merely because its name starts with `bench-`.
Boot disks are fixed at 80 GiB; larger source images are refused. Each create
intent includes a fresh ownership token in the disk description. Recovery can
adopt an orphan's numeric ID only when that exact token, run and source match.
An unmatched orphan is an error, not permission to delete by name.
Raw observations are not truncated to fit collection limits: over-limit exports
fail explicitly, retain their partial inventory, and still trigger cleanup.

The independent Actions cleanup job uses `recover-cleanup --terminal-workflow`
after its campaign job is terminal. Recovery validates the workflow branch, job,
run/attempt-derived identity, lease and durable state before reconciling ambiguous
creates. Ordinary recovery refuses an unexpired run. If both runners disappear,
Compute's absolute workflow-specific DELETE deadline remains active; the administrator's
six-hour reaper is a further backstop. Boot disks use `autoDelete`.

After the fleet campaign controller returns, its coordinator publishes a terminal
record only after the campaign response and the exports for every started suite
have generation and SHA-256 bindings in GCS. The record identifies each owner
and its required suite exports, or says no owner artifacts were required because
no suite started. KV requires a complete monitoring export; a started but failed
Sui suite lacks complete owner-export proof and therefore defers poweroff. Each
owner checks its own provenance and the bound objects using its metadata
service-account credentials. The coordinator also checks the bound objects. It waits ten
minutes from terminal publication and, if no operator takeover object exists,
requests `systemctl poweroff`. A valid takeover defers shutdown to 60 minutes
after terminal publication; an explicit `extend_until_utc` can extend that cap.
The runtime accepts `poweroff_grace_seconds` and `poweroff_hard_cap_seconds` in
the launch configuration (defaults 600 and 3600; cap at most 21600 seconds).
Active requests, missing or changed exports, invalid acknowledgement data, API
errors and ambiguous controller termination prevent guest poweroff. Decisions
are written as small per-guest objects under `runs/RUN/poweroff/decisions/`.
The manual operator acknowledges takeover only after verifying terminal exports.
Compute's absolute DELETE deadline and operator deletion/absence verification
remain required; powering off does not delete a VM or its boot disk. This
reduces idle compute cost when the workstation loses credentials after a run;
it does not replace the deadline for failures before durable export.

The pre-suite headroom probe creates 45 writable disposable sparse-file
namespaces on each of five owners, with unique NQNs and host ACLs. The coordinator
requests 1x32 queues, checks sysfs 2/31, and samples MR/QP/CQ/PD every 15
connections, up to 225 or 120 seconds in the connect loop. Fewer than 135 aborts;
a failed connection at or above 135 is reported. Unknown or failed cleanup
aborts. Target setup, sampling and retirement add overhead beyond the 120-second
connect loop. This probe does not validate WAL data.

Guest workers inherit unlimited MEMLOCK for RDMA, stop at the absolute deadline,
and bound both concurrent and total mailbox requests. Per-key SQLite transactions
retain attempts across restarts without rewriting the entire request history.
Scratch formatting requires matching Compute/metadata inventory, documented
Google local-NVMe aliases, a distinct boot device, a recognized local-NVMe model,
and no mount, partition or filesystem signature. This is not a
bytewise scan proving that a 14 TB device contains only zeros. The actual image's
Google by-id aliases must pass qualification before activation.

### Prepared image admission

`config.json` deliberately retains `"image": ""` and `"image_qualification": null`.
A public image's READY state does not establish supported IRDMA, io_uring, ABI or
benchmark readiness. The reviewed custom image must pass this separate process:

1. `gcp_image_prepare.py --mode hpc-rocky9` supports the exact provider-built
   `hpc-rocky-linux-9-v20260930` image, numeric ID `3102614992461399694`.
   Explicit flags authorize signed repository access, pinned container pulls,
   loaded provider weak-update module admission, and io_uring enablement.
   Requested package NEVRAs, official signing keys, libfabric 2.6.0 source and
   PyYAML wheel are pinned; kernel and provider-driver upgrades are excluded.
   The original Rocky 10 offline-RPM mode remains separate. Its CIQ EL10 metadata
   endpoint was inaccessible, so it is not the selected campaign base.
   Reproduction uses the captured custom image and installed package inventory;
   this is not a promise of byte-identical reconstruction from future repositories.
2. Capture the prepared custom image. On two distinct hosts booted from that
   exact captured image, run `gcp_image_qualify.py` under the guest identity with
   unlimited MEMLOCK, the sealed guest-compatible runtime package, and
   separately controlled physical peer servers. It checks the actual syscall
   and ELF linkage, cached containers, local SSD identity, physical RDMA transfers
   and Valkey RDMA operations. This task did not create those hosts.
3. Upload both diagnostic JSON files. Use `gcp_image_receipt.py` to create the
   receipt from their exact bucket/object/generation/SHA references. Review and
   upload that receipt, then set the exact custom image path and
   `image_qualification` reference (`bucket`, `object`, `generation`, `sha256`).
   Admission verifies its image numeric ID, recipe hashes and reciprocal evidence.

The receipt is review-bound evidence, not a hardware attestation. Qualification
needs read access to both exact instances and their boot disks; a missing
`compute.disks.get` grant must be resolved rather than bypassed. Use each tool's
`--help` and its strict manifest schema; do not substitute invented evidence.

The baseline runtime hash records what was actually exercised on that image; it
is not an equality requirement for every future source revision. Pinning a new
receipt changes the source snapshot, so whole-manifest equality would invalidate
itself. Before allocation, the current sealed KV and selected Sui binaries'
SONAME requirements and four version families (GLIBC, GLIBCXX, CXXABI, OPENSSL)
must fit the reciprocal guest evidence. Sui requirements are recomputed from
its six hash-bound executable artifacts after transfer. This is a static ABI
envelope, not complete symbol attestation or proof that new code already ran
on RDMA hardware. Current-package linkage is checked again on each guest.

Private campaign guests have no ordinary registry internet. They inspect cached
monitoring images without pulling; a missing image aborts bootstrap. The same
sealed source/runtime is checked again on each campaign guest. The AWS path keeps
its existing image-pull behavior.

The workflow freezes the protected environment's image and immutable receipt
reference into a deployment configuration carried in the SHA-sealed transfer
archive. Supplying image pins in both repository and environment is rejected.
Image approval therefore does not require changing the source snapshot that
produced the baseline binaries. Compute shape, regions, placement and budget
remain source-controlled; image variables cannot override them.

### Guest-compatible release builds

The selected HPC guest has glibc 2.34. A locally rebuilt all-adapter executable
required GLIBC 2.38/2.39 and is explicitly not a deployable artifact for that
guest. `Dockerfile.build` pins an official Rocky 9 amd64 base; `prepare.py
--build-container` builds Rust, libfabric and optional Valkey artifacts inside
that environment, then runs the existing functional qualification on the host.
The release receipt records the actual builder image ID and recipe hash. The
GCP build does not change the AWS builder.
Local functional qualification covers eight API cases, paired five-primary
libvalkey TCP operations, and the five-owner TCP diagnostic. Receipts bind the
actual source, builder and binary; an older receipt does not qualify a later
commit. These checks are not cloud performance or hardware RDMA qualification.
The Valkey CLI helper records stderr separately in `client-stderr.log`, so
loader warnings do not corrupt protocol replies.

Guest Python is `/opt/tidehunter-python/bin/python3`; the operating system's
Python is unchanged. Bootstrap and mailbox commands use that same interpreter
and an explicit PATH selecting the prepared `/usr/local/bin/fi_info`, not the
provider image's older libfabric tools.
Image admission now requires two fresh, source-bound qualification diagnostics
that record this exact interpreter, functional PyYAML 6.0.2 and a hard NOFILE
limit of at least 524288; older diagnostics lacking those fields do not qualify.
Before allocation, preflight validates the sealed observer renderer and validator
with the actual run ID and deadline and representative private addresses. These
addresses are not fleet evidence: the allocated fleet's actual rendered config
and observer readiness are separately checked before workloads. Each guest also
proves the benchmark transient unit actually runs the admitted Python with both
NOFILE limits at 524288; its process and systemd properties are retained in
bootstrap evidence. A low inherited soft limit is not itself proof of the unit's
effective limit.

The standard comparison retains its 273 cases. It covers all selected engines,
but has no dedicated common atomic batch-write workload: several adapters do
not implement the same semantics. An independent-write pipeline must not be
reported as an atomic Tidehunter batch.

### Initial HPC image bake and remaining cloud prerequisite

The bounded pilot `bench-img-3012611b9ce24218` produced custom image
`projects/mysten-research/global/images/bench-img-3012611b9ce24218-image`,
numeric ID `1937849582956621267`. Its software checks established Python
3.11.13/PyYAML 6.0.2, libfabric 2.6.0, Docker/Compose, the pinned image cache,
and an actual fio io_uring read after explicitly enabling io_uring. The provider
kernel and loaded CIQ weak-update driver were preserved.

The executed bake/replay preparation script SHA-256 was
`7e886d60aaa73a1a1c7d46d26df814b1c6f0439971cf494e5b69cdf5cddbb14b`.
Its exact source and results are retained under
`gs://mysten-research-bench-artifacts/runs/bench-img-3012611b9ce24218/artifacts/`.
Later source edits are not claimed as executed on this pilot. The captured
image is **not hardware-qualified**: reciprocal transfers, Valkey RDMA and the
guest-compatible sealed benchmark remain to be exercised on two captured-image
hosts. No full KV campaign results were produced by this bake.

The pilot VM, boot disk and placement policy were deleted and their absence
verified; the shared lease was released. The custom image is intentionally
retained and has storage charges. The ledger reserves $4 for this bounded pilot,
not its measured invoice.

Read-only Service Usage inspection established that
`cloudresourcemanager.googleapis.com` is DISABLED in project `514867804006`.
The guest's project permission test returned `SERVICE_DISABLED`, although its
actual Compute reads and GCS access worked. The connected user cannot enable
the API. An administrator must enable it before the current branch preflight
can pass. Do not remove that check or launch a fleet that is known to fail it.

The separate manual campaign path does not claim that the disabled guest CRM
project-level permission test passed. It tests the caller's management and
service-account delegation permissions, then checks guest access to the exact
new policy, instances, and boot disks before releasing the startup gates. It
retains the automated GitHub path's existing preflight unchanged.

The Python entrypoint is `await manual_campaign.run(config, ..., execute=...)`;
`execute` supplies the MCP gcloud callback. The inputs include the locally
admitted sealed package and revision, the exact reviewed specification and
preparation wheels, the pinned base image path and numeric ID, a candidate RDMA
device, an output directory, and an explicit non-compute cost quote. Recovery
uses `await manual_campaign.recover_cleanup(config, run_id=..., state_path=...,
execute=...)` with the persisted parent identity. It does not require a GitHub
terminal workflow. Each run keeps its own lease throughout preparation,
physical qualification and benchmarking; failure does not start a replacement.
The CAS admission sums all active reservations (including the legacy run)
against `config.active_run_limit_cents` (default 14500 cents). The existing
weekly ledger is a separate guard; cleaning a run releases active exposure,
not its weekly reservation.

The reviewed spec can set `reservation_hours` to an integer from 2 to 6
(default 6). It scales the conservative compute quote and per-run ceiling,
the workload hard deadline, parent and child expiry, and VM DELETE/self-stop
deadline together. The sealed spec and GO must bind this window. A shorter
window also reduces the permitted non-compute surcharge; quote it again.
Existing six-hour specs and their rounding are unchanged.
The exact `nvmeof_p13_write_engine` review pins 5.5 hours.
`nvmeof_p16_redrain_ab` pins 4.5 hours and one compact `z4m-highmem-16` fleet
(one coordinator, two owners). Its reviewed order runs 18 clean 512 B leased-put
cases in two reversed passes, then four c64 attribution cases. Keep all arms on
the same fleet. No `arms` filter applies. The archive budget includes the
attribution timelines; the scorecard excludes them. The owner start/stop proofs
record device counters before WAL creation and after WAL deletion.
Fractional windows are not accepted without an explicit reviewed value.

Sealing from a separate worktree is supported. Set every copied seal stage's
`REPO` (and make-go's `repo`) to that worktree, and `BASE` to a fresh attempt
directory. The GO's `repo` must select that same worktree for launch/recovery
imports. `prepare.ROOT` is derived from the imported module's `__file__`; the
source gate compares that checkout's HEAD, branch, dirty state and inventory
with `snapshot/source.json`. It does not require the main checkout or branch.
Seal only after committing the intended source; do not reuse an old snapshot.
After sealing, freeze that checkout until its fleet's cleanup is verified.
Do not edit, switch branches, merge, or leave untracked test/build artifacts
there while source gates can still run. Use another worktree for further work.
The controller spec omits launch-only `reservation_hours` but retains the
effective `hard_deadline_seconds`.

Admission queries regional quotas through `gcloud compute regions describe`
and CAS-checks all active plus new vCPU and local-SSD reservations. It adds
observed usage conservatively, because uploaded-but-unallocated fleets cannot
be assumed to appear in usage yet. This may refuse capacity that is already
counted twice; it never assumes an overlap. Missing/ambiguous quota metrics
fail closed. Z4M local SSDs are 14000 GiB each; the quota is
`LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY`, not the deprecated aggregate SSD metric.

Package, wheel, fixture and verification uploads share the machine-local
`/tmp/tidehunter-package-upload.lock` flock across processes and worktrees.
The lock wait is bounded to one hour; it is released on transfer failure or
process exit. Never delete that inode while uploads can run. An already-running
old launcher does not take this lock: wait for its upload to finish before
starting the first new-code upload.

Read-only Compute descriptions and lists retry actual callback timeouts at most
three times with identical arguments and the original phase/cleanup deadline.
Invalid responses, permission errors and identity mismatches are not retried.
A timed-out recorded delete intent is observed for absence, not submitted again;
unresolved observations never become successful cleanup.
Lifecycle state writes retry rate limits and transient server errors with bounded
backoff and the same generation precondition. A failed intent save does not
authorize a delete; recovery must reconcile any uncertain committed write.

The manual launcher waits on the exact coordinator's request-bound mailbox
response, not a possibly provisional controller receipt, without repeatedly
downloading the result prefix. The worker publishes that response after the
coordinator exits; complete and incomplete terminal exports must pin the source-
and binary-bound final receipt and the GCS generations, sizes and SHA-256
metadata of its controller archive and inventory. Failed or unknown outcomes
remain failures; already published partial objects are retained.
If delegated mailbox reads encounter a token timeout or a transport-only GET
failure, the launcher continues observing the same immutable request until the
original deadline. It does not resubmit the command or change credentials.
Certificate, authentication, authorization, integrity and source failures remain
errors. This retry does not establish whether the workload itself succeeded.

When that terminal export is validated, the launcher reconciles the exact-owned
compute, disks and policy and releases the lease only after absence is
verified, before downloading qualification and result objects locally. If the
terminal outcome or durable export is missing, provisional or corrupt, it
instead attempts the prior best-effort partial collection before cleanup;
collection failure never skips cleanup. Cleanup dispatches up to six
independent VM records concurrently under one shared 600-second budget; an
acknowledgement or lost response is not proof of deletion. Name-based deletion
still has the documented describe/delete TOCTOU window. Local downloads are
generation- and SHA-256-checked, and collection errors preserve any original
campaign or cleanup errors. A failed download does not erase the durable GCS
objects; retain them and the state/inventory for recovery.

The historical `standard-with-nvmeof` plan contains 409 cases and uses NVMe-oF
only for sealed reads. The distinct `standard-with-live-nvmeof` plan adds 32
live NVMe/RDMA cases and 20 matched atomic batch-write cases (five per native,
TCP, verbs-native and live NVMe backend), for 461 cases with the reviewed
backend and diagnostic selection. Its 29 primary NVMe cases match TCP's workload
mix, sizes, concurrency, warmup and operation counts; additional cache, cold and
diagnostic cases are separate. External stores without atomic batches do not
join that batch-write cohort. The 26 sealed NVMe read controls remain distinct
and do not demonstrate live writes. All cases are planned for one six-host
fleet, one development repetition and five value sizes. The reviewed manual
fleet has a six-hour deadline, with export and teardown reserves kept intact.
Historical matched partial timings plus phase allowances imply roughly four hours
and 32 minutes, leaving insufficiently robust slack under the former five-hour
limit. Deadline exhaustion still exports partial results, never false completion.
Dropping an OS cache is not proof of a NAND read. Keep raw results, Prometheus
and Grafana artifacts, and do not pool sealed and live populations.

Live mode requires a newly source-frozen package and a successful reciprocal
physical live NVMe write, read, fence and recovery qualification with route and
counter evidence on the first two members of the allocated fleet before suite
admission. An older sealed-read qualification does not satisfy this requirement.
If the live qualification implementation or its evidence is unavailable, admission
fails closed; no RPC fallback is a valid NVMe result.

One placement policy and exactly six VMs use the pinned base image. Each VM has
the same absolute 21600-second DELETE expiry, covering preparation, pair
qualification, suite and export; preparation and pair waits are additionally
bounded to 900 and 1200 seconds respectively. The suite reserves 900 seconds
for controller export. No transition resets the lifetime. The $3.287109973
per-host-hour quote gives a rounded-up $118.34 compute estimate for six six-hour
VMs. The $145 aggregate ceiling leaves at most $26.66 for the caller's explicit
non-compute quote covering disks, storage, requests and transfers. These are
estimates, not a billing guarantee or a project-wide limit; an incomplete
campaign exports partial results and is not silently relaunched.

The reviewed focused `parity_compact` and `remote_rw_compact` selections derive
a two-VM fleet (one coordinator and one owner) from their pinned specs; `fair_five_owner` and
the existing selections retain six VMs. Sui and CI remain five-owner only.
The same six-hour expiry applies. [INFERENCE] At the reviewed per-host price,
the compact compute reservation is $39.45 and the proportional aggregate
ceiling is $48.34, leaving at most $8.89 for the explicit non-compute quote.
These are estimates, not actual spend or a billing guarantee.

Run 10's reviewed `fabric_ab_compact` selection contains 80 focused jobs on
the compact two-VM layout: one coordinator and one owner. It has no
five-owner mode.

Every admitted guest prepares the pinned runtime in place; its individual evidence
binds the preparation manifest, wheel, source and guest identity. The first two
also execute reciprocal physical qualification and clean up its services,
controllers and owned data before suite admission. The image receipt identifies
the actual base image and the observed post-preparation runtime on these guests;
it does not claim the newly installed software was baked into the base image.
For focused specifications, the pair receipt lists the exact runtime, ABI,
NVMe, io_uring, libfabric, reciprocal RDMA and monitoring proofs. This profile
uses only the fabric, nvmeof and test-utils build features. It does not stage
FoundationDB or Valkey runtimes and does not inspect external-store images.
The original full-matrix profile retains its Valkey qualification.
After publishing the suite runtime, the controller waits at most 180 seconds
for all six actual workers to finish package, fleet and local-storage admission.
Each immutable readiness record binds the source, manifest, VM name, numeric ID
and original deadline. A startup error, missing readiness or replaced identity
blocks campaign submission and enters owned-resource cleanup.
The historical image qualification lacks direct `nvmet` and `nvme_fabrics`
evidence and has older recipe hashes, so it is not admitted for this campaign.
Local package preparation itself does not allocate any cloud VM.

The reviewed matrix retains the existing cases and four separately executed
Valkey reproduction controls, and adds the standard profiles for TiKV,
Cassandra, FoundationDB, Kvrocks and Garnet. `native_durability_cohort` is
explicitly enabled only in this reviewed specification: its TideHunter twins
use native page-cache acknowledgement with background fragment fsync, while
the original synchronous owner-barrier cases remain unchanged. Sealed twins
use separate physical source identities but the same logical data and are
read-only. Do not compare native-tier writes to Valkey's synchronous AOF
writes; check acknowledgement and cache contracts for every comparison.

The expanded campaign has no measured full-fleet runtime. The earlier 411-case
run took approximately 3.79 hours; five additional stores and the native-tier
twins add substantial unmeasured startup and case time. It is not established
that the complete matrix fits the six-hour deadline or the $145 ceiling including
non-compute costs. The existing deadline, export reserve and ceiling remain
hard bounds; partial results are not a complete comparison.

Generate a fresh risk-first permutation for the exact reviewed matrix (the
output must not already exist), then validate it before local qualification:

```sh
python3 experiments/gcp-rdma/risk_order.py /absolute/path/risk-first-order.json --generate
python3 experiments/gcp-rdma/risk_order.py /absolute/path/risk-first-order.json
```
The order is matrix-hash-bound and keeps each sealed physical source and
durability tier contiguous, without changing its internal read order.

Print the current focused matrix counts and SHA-256 values with one command:

```sh
python3 -c 'import sys,json,hashlib; sys.path.insert(0,"experiments/kv-comparison"); import cloud_campaign as c; [(print(s,len(j),hashlib.sha256(json.dumps(j,sort_keys=True,separators=(",",":")).encode()).hexdigest())) for s in c.FOCUSED_SELECTIONS for j in [c.make_focused_matrix(s)]]'
```

### Leased A/B compact campaign (run 11, 11 November 2026)

This manual-only selection uses two hosts, 72 cases, kernel NVMe/RDMA and
no SPDK. It reserves $48.34, including $39.45 rounded compute for six hours.
The fleet's first two hosts qualify both native and synchronous live tiers
before the suite. The immutable image receipt binds those same host identities.
The 21600-second expiry and 900-second export reserve remain in force.

Before seeking launch approval, verify the exact image name and numeric ID,
its focused qualification record and live proofs, quota, compact placement
policy, and remaining weekly capacity in the shared cloud budget ledger.
Generate the exact spec and order offline:

```sh
python3 -c 'import json,sys; sys.path.insert(0,"experiments/gcp-rdma"); import manual_campaign as m; json.dump(m.REVIEWED_SPECS["leased_ab_compact"],open(sys.argv[1],"x"),sort_keys=True,indent=2)' /absolute/path/leased-spec.json
python3 experiments/gcp-rdma/risk_order.py /absolute/path/leased-order.json --selection leased_ab_compact --generate
python3 experiments/gcp-rdma/risk_order.py /absolute/path/leased-order.json --selection leased_ab_compact
```

Seal the focused package using the preparation command below with this spec,
review its receipt and observer proof, then obtain explicit user approval.
Only the manual launcher may allocate this compact fleet; CI rejects it.

### NVMe-oF data path compact campaigns (`nvmeof_path_core`, `nvmeof_path_compact`)

These manual-only selections use the same two hosts, the `native` backend,
owner RPC over trusted verbs, and the live kernel NVMe/RDMA route with
permanent per-owner export. They have no SPDK target. `nvmeof_path_compact` is
the full selection (157 cases). `nvmeof_path_core` is its launched subset
(66 cases, same settings; run 12). The reservation, the pair qualification of
both live tiers, the expiry and the export reserve are the same as for the
leased A/B campaign. In the full selection the first 12 cases are canaries and
the seventh stops the run when its cold read is not served from the device.
In `nvmeof_path_core` three cases run first: the native 1 KiB put, the 64 KiB
cold live read that stops the run when it is not device-served, and the
native cold `get_many`; the other ten canaries follow.

Generate the exact spec, the order and the settings table offline (replace
`SELECTION` with one of the two names). The settings table is part of the
launch request:

```sh
python3 -c 'import json,sys; sys.path.insert(0,"experiments/gcp-rdma"); import manual_campaign as m; json.dump(m.REVIEWED_SPECS[sys.argv[2]],open(sys.argv[1],"x"),sort_keys=True,indent=2)' /absolute/path/nvmeof-path-spec.json SELECTION
python3 experiments/gcp-rdma/risk_order.py /absolute/path/nvmeof-path-order.json --selection SELECTION --generate
python3 experiments/gcp-rdma/risk_order.py /absolute/path/nvmeof-path-order.json --selection SELECTION
python3 -c 'import json,sys; sys.path.insert(0,"experiments/kv-comparison"); import cloud_campaign as c; json.dump(c.make_focused_matrix(sys.argv[2]),open(sys.argv[1],"x"),sort_keys=True,separators=(",",":"))' /absolute/path/nvmeof-path-matrix.json SELECTION
python3 experiments/kv-comparison/nvmeof_path.py --settings-only --matrix /absolute/path/nvmeof-path-matrix.json --output /absolute/path/nvmeof-path-settings
```

Seal and approval follow the leased A/B rules above. If the many-files canary
(G18X) fails because a namespace does not appear on the coordinator, set
`live_export_mode: "per_file"` in each live job, generate the matrix again and
seal again. A new launch needs a new approval.

`nvmeof_path_fix` (37 cases, run 13) is the confirmation selection for the `wal_replay` owner history,
lease-ahead and polled writes. It uses the same hosts, reservation, expiry and commands (`SELECTION` =
`nvmeof_path_fix`). Its first 8 cases are canaries; the fourth stops the run when its cold read is not
device-served.

### Sui replay over NVMe-oF on the three-host fleet (`manual_sui.py`, manual only)

`experiments/gcp-rdma/manual_sui.py` is the Sui launcher for one coordinator and
two owners. It prepares the external deployment with the frozen
`prepare_external.py`, installs each owner archive in the same absolute guest
root on its host, starts both owner process groups through `gcloud compute ssh`,
runs the frozen `run.py` with `--external-config`, `--external-resources` and
`--external-stage-dir`, answers `resources-required` with sampled per-PID
`/proc` evidence, and stops only the groups it started. The stage markers and
the resource contract are the same ones the external flow already documents.

Prerequisites: a frozen build (`build.py prepare` and `build.py build`), a
native schema exported by a source-identical local run
(`<output>/experiment/native-schema`), the fixture, three hosts in one zone, and
the guest RDMA/kernel admission below. The run output, external config
directory and stage directory must be fresh; the launcher refuses an existing
stage marker and finishes with `launcher-receipt.json`.

```sh
python3 experiments/gcp-rdma/manual_sui.py \
  --build /absolute/owned/checkpoint-build \
  --fixture /absolute/checkpoint-fixtures/sui-genesis-1775/fixture \
  --schema /absolute/local-run/experiment/native-schema \
  --output /absolute/cloud-run \
  --guest-root /mnt/tidehunter/sui-run-<run-id> \
  --coordinator H0 --owners H1,H2 --zone us-central1-a \
  --transport efa --live-transport rdma --durability-tier native \
  --fragment-mib 8 --deadline 900 -- --driver-concurrency 4
```

Launcher options end at `--`; everything after it is passed to `run.py`
unchanged, including `--driver-concurrency`, `--pacing-ms`, `--checkpoints`,
`--value-cache-size`, `--default-value-cache-size`, `--max-dirty-keys`,
`--snapshot-written-bytes`, `--max-maps` and
`--index-blob-cache-min-bytes`. Pacing needs a
`SUI_CHECKPOINT_EXECUTION_PACING_MS` hook in the frozen Sui fork; without it
`run.py` refuses a paced cell instead of claiming one. A limited
`--checkpoints N` replay still verifies the complete frozen source and reuses
its digest at N for the paired native and remote replays.

Resume: pass a stable `--ledger /absolute/campaign-ledger.json` and `--resume`.
The launcher records the cell (`--cell-id`, default `sui-replay`) as running
before it prepares or installs anything, and marks it passed only after the
results, receipt and evidence files exist and their sizes and SHA-256 values are
verified. A passed cell whose build, fixture, schema or knob hash still matches
is skipped before any host is touched; a changed hash or a failed/interrupted
entry reruns in a fresh output. Use a new `--output` for a rerun and the same
ledger. `run.py` records the cell id in `results.json`.

Stage 3 uses `experiments/sui-checkpoint/stage3_campaign.py` with the same
campaign ledger and the same bucket prefix. It regenerates the 72 specs when
they are absent, archives any previous database and output under
`stage3/stale/`, and runs only planned cells. `--mode cluster` needs
`--bucket-prefix`, the two tier preparations and the owner hosts; phase A (the
arm-B synchronous probe) runs first, `--calibrate` runs the three phase-B cells
once and freezes the counts, and `--skip-probe` skips phase A explicitly.
`--mode rehearsal` runs the local fixture and needs `--allow-local-ledger`.
`--allow-local-ledger` is the only local-rehearsal escape; it never uploads.

```sh
python3 experiments/sui-checkpoint/stage3_campaign.py \
  --mode rehearsal --root /absolute/stage3-campaign \
  --binary /absolute/kv-research --allow-local-ledger \
  --only S3-A1-c16-native-cache0

python3 experiments/sui-checkpoint/stage3_campaign.py \
  --mode cluster --root /absolute/stage3root \
  --binary /absolute/build/binaries/storage-node \
  --external native=/absolute/external-native \
  --external synchronous=/absolute/external-synchronous \
  --external-database live/checkpoints \
  --guest-root /mnt/tidehunter/$CAMPAIGN --native-root /mnt/tidehunter/$CAMPAIGN \
  --owners H1,H2 --native-host H1 --zone us-central1-a \
  --bucket-prefix gs://mysten-research-bench-artifacts/$CAMPAIGN \
  --ledger /absolute/campaign-ledger.json --calibrate --resume
```

`--from <cell-id>` shifts the plan, `--continue-on-failure` keeps measuring
after a failed cell (the default stops), and every non-reference spec is
`-cache0`; the two `-cache1000` read-only C=16 cells are the labelled cache-on
reference pair. `backend.options.value_cache_size` carries the same count into
the distributed and native keyspace configuration.

The Sui campaign's three stages do not use `manual_campaign.py`. That module
remains the KV/NVMe-oF fleet orchestrator. Stage 1 and stage 2 are driven by
`manual_sui.py` on the three Sui hosts (fault and mechanism cells by
`fault_cells.py`), and stage 3 is driven by `stage3_campaign.py` on the same
hosts; every driver writes the one campaign ledger at
`<prefix>/campaign-ledger.json`.

The retired compact six-VM Sui spec (`sui_nvmeof_compact`,
`sui_nvmeof_harness_synchronous`) is not used by the campaign. The three-stage
campaign uses this launcher for stages 1 and 2 and the `stage3_specs.py` set for
stage 3.

### Three-host fleet lifecycle for the Sui campaign (`gcp_manual_compute.py`)

`gcp_manual_compute.py` owns the fleet that `manual_sui.py` assumes exists. It
creates one placement policy and two or three hosts, then verifies or deletes
exactly those resources. It never retries a create that has an unknown outcome.

The fleet run identity is derived from the campaign, never chosen:

```sh
python3 -c 'import sys; sys.path.insert(0,"experiments/gcp-rdma"); import gcp_manual_compute as m; print(m.fleet_run_id(sys.argv[1]))' campaign-hybrid-sui-2026-11-25
# bench-68bbf96a569f1513
```

Hosts are `<run_id>-0` (H0: coordinator, Sui node, producer, driver), `-1` and
`-2` (H1, H2: owners and NVMe-oF targets). Every host carries the labels
`campaign`, `bench-run` and `bench-source` and the boot disk carries a token in
its description, so a delete cannot remove a foreign resource.

The fixed body of each host, printed by `--dry-run`:

| Field | Value |
| --- | --- |
| Machine | `z4m-highmem-16` with its mandatory bundled Local SSD |
| Boot disk | one 80 GiB `hyperdisk-balanced`, `NVME`, `boot`, `autoDelete` |
| Network | `default` (GVNIC) and `falcon-rdma-us-central1-a` (IRDMA), no external address |
| Service account | `bench-guest@mysten-research.iam.gserviceaccount.com`, `cloud-platform` |
| Placement | `<run_id>-placement`, `COLLOCATED`, `maxDistance 3` |
| Lifetime | `terminationTime` = deadline, `instanceTerminationAction` = `DELETE`, no restart on failure |

Example shell setup for the campaign:

```sh
CAMPAIGN=campaign-hybrid-sui-2026-11-25
RUN=bench-68bbf96a569f1513
CAMPAIGN_ROOT=/absolute/checkpoint-work/cluster-campaign-2026-11-25
STATE=$CAMPAIGN_ROOT/fleet-state.json
IMAGE=projects/mysten-research/global/images/bench-img-3012611b9ce24218-image
BUNDLE=c7b69b8bd942a119bcced33e0e5dbc4222ee47650306f03471c8b42ad0230fa9
```

1. Print the request bodies. This step makes no call and writes no file.

```sh
python3 experiments/gcp-rdma/gcp_manual_compute.py create \
  --campaign $CAMPAIGN --state $STATE --hosts 3 --hours 5 \
  --source-sha256 $BUNDLE --image $IMAGE --dry-run
```

   The output holds `policy` (the placement body) and one insert body per host.
   The command exits without a state file, so the next step starts clean.

2. Create the policy and the three hosts. The command admits the image first
   and refuses to create a fleet when `config.json` has no current receipt
   (see "Image requalification" below).

```sh
python3 experiments/gcp-rdma/gcp_manual_compute.py create \
  --campaign $CAMPAIGN --state $STATE --hosts 3 --hours 5 \
  --source-sha256 $BUNDLE
```

   `--hours 1..6` sets the absolute deadline; `--deadline <UTC ISO 8601>`
   pins an exact time instead. A lost watcher cannot leak cost past that
   deadline, because the provider deletes the instance itself. The command
   prints the fleet with the internal and RDMA address of each host.

3. Verify the placement and the fleet identity. This step reads only.

```sh
python3 experiments/gcp-rdma/gcp_manual_compute.py verify --state $STATE
```

   It checks `COLLOCATED` with `maxDistance 3`, the DELETE expiry, both network
   interfaces, the boot disk identity and the guest-visible readback. A second
   `create` on the same state reuses verified hosts and refuses any other
   campaign, host count, source or deadline.

4. Stage the frozen build and the fixture in the bucket once, then pull them on
   H0. One workstation upload and one datacenter-speed download replace two
   slow transfers through the tunnel.

```sh
STAGE=gs://mysten-research-bench-artifacts/$CAMPAIGN/stage
gcloud storage cp --recursive /absolute/cluster-campaign-2026-11-25/build $STAGE/build \
  --impersonate-service-account=bench-guest@mysten-research.iam.gserviceaccount.com
gcloud storage cp --recursive /absolute/checkpoint-fixtures/sui-genesis-1775/fixture $STAGE/fixture \
  --impersonate-service-account=bench-guest@mysten-research.iam.gserviceaccount.com
gcloud compute ssh $RUN-0 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo mkdir -p /mnt/tidehunter/'$CAMPAIGN' && sudo chown $USER /mnt/tidehunter/'$CAMPAIGN' && \
             gcloud storage cp --recursive '$STAGE'/build /mnt/tidehunter/'$CAMPAIGN'/ && \
             gcloud storage cp --recursive '$STAGE'/fixture /mnt/tidehunter/'$CAMPAIGN'/'
```

   The build is 1.1 GB and the fixture is 752 MB. `manual_sui.py` starts
   `run.py` on the machine that runs the launcher and reads both paths there,
   so the copy is needed only when the replay is driven from H0, for example
   across a long cell: pass `--build /mnt/tidehunter/$CAMPAIGN/build` and
   `--fixture /mnt/tidehunter/$CAMPAIGN/fixture` from that session. The owner
   hosts need no copy; the launcher installs each owner archive in the guest
   root.

   The guests have no external address, so every session needs IAP TCP
   forwarding. Check the prerequisite before the paid create:

```sh
gcloud services list --enabled --format='value(config.name)' | grep iap
```

   An empty result blocks every SSH step, because `iap.googleapis.com` is
   disabled in this project. Enabling it, and holding
   `iap.tunnelInstances.accessViaIAP`, is a project change outside this
   launcher. The firewall rule `default-allow-ssh` already covers the IAP
   source range `35.235.240.0/20`.

5. Identify the bundled Local SSD and mount it at the guest root on all three
   hosts. Do not assume the alias from another image.

```sh
gcloud compute ssh $RUN-0 --zone us-central1-a --tunnel-through-iap \
  --command 'ls -l /dev/disk/by-id/google-local-ssd-* && lsblk -o NAME,SIZE,MODEL'
```

   The `z4m-highmem-16` shape has one 14,000 GiB Local SSD. Create the
   filesystem once, then mount it at `/mnt/tidehunter/$CAMPAIGN` on each host
   with the same `mkfs` and `mount` commands. A stopped host loses the Local SSD
   content, so never place the ledger or a receipt there.

6. Load the pinned `nvmet.ko` on all three guests, as root, before any owner
   starts. The command prints its proof; `already_loaded` is not a failure.

```sh
gcloud compute ssh $RUN-0 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo /opt/tidehunter-python/bin/python3 /mnt/tidehunter/'$CAMPAIGN'/pinned-artifacts/nvmet-fix/kernel_fix.py --prefix /mnt/tidehunter/'$CAMPAIGN'/pinned-artifacts/nvmet-fix'
```

   Repeat for `$RUN-1` and `$RUN-2`. The prefix holds exactly `nvmet.ko`,
   `abi-admission.json`, `source.json` and `partial-nowait-read.patch`, and the
   module SHA-256 prefix is `5ba7b96c3b232e7f`. S1-0 is the first case that
   exercises this path.

7. Run the cell from the workstation. The launcher owns the owner processes,
   the external config and the ledger entry.

```sh
python3 experiments/gcp-rdma/manual_sui.py \
  --build $CAMPAIGN_ROOT/build \
  --fixture /absolute/checkpoint-fixtures/sui-genesis-1775/fixture \
  --schema $CAMPAIGN_ROOT/schema-run/experiment/native-schema \
  --output $CAMPAIGN_ROOT/stage1/S1-0 \
  --guest-root /mnt/tidehunter/$CAMPAIGN \
  --coordinator $RUN-0 --owners $RUN-1,$RUN-2 --zone us-central1-a \
  --transport tcp --live-transport rdma --durability-tier native \
  --fragment-mib 1024 --coordinator-cache-mib 0 --shard-cache-mib 0 --deadline 900 \
  --bucket-prefix gs://mysten-research-bench-artifacts/$CAMPAIGN \
  --ledger $CAMPAIGN_ROOT/campaign-ledger.json --cell-id S1-0 --stage stage1 \
  -- --driver-concurrency 4 --disk-sample-seconds 10 --checkpoints 4
```

   Every knob is given once, to the launcher. Do not repeat `--owners`,
   `--durability-tier`, `--fragment-mib`, `--coordinator-cache-mib`,
   `--shard-cache-mib`, `--write-profile-limit` or `--resident-reads` after
   `--`; those forward to both `prepare_external.py` and `run.py`. The cell
   passes only after its upload is verified and the campaign ledger is written
   under the bucket prefix. Lagging S1-1/S1-8 cells add `--lifecycle-checks
   --tail-checkpoints 64` after `--`.

   The mechanism and fault cells use `experiments/gcp-rdma/fault_cells.py`
   with the same prefix, the same ledger and `--resume`. `--dry-run` prints
   one cell's plan without a host.

```sh
python3 experiments/gcp-rdma/fault_cells.py \
  --cell S1-2S-publication-native --build $CAMPAIGN_ROOT/build \
  --fixture /absolute/checkpoint-fixtures/sui-genesis-1775/fixture \
  --schema $CAMPAIGN_ROOT/schema-run/experiment/native-schema \
  --output $CAMPAIGN_ROOT/stage1/S1-2S-publication-native \
  --guest-root /mnt/tidehunter/$CAMPAIGN \
  --coordinator $RUN-0 --owners $RUN-1,$RUN-2 --zone us-central1-a \
  --transport tcp --live-transport rdma \
  --bucket-prefix gs://mysten-research-bench-artifacts/$CAMPAIGN \
  --ledger $CAMPAIGN_ROOT/campaign-ledger.json --resume
```

9. Resume from another machine. The bucket ledger is authoritative; download
   it before `--resume`, then merge happens again on every save.

```sh
gcloud storage cp gs://mysten-research-bench-artifacts/$CAMPAIGN/campaign-ledger.json \
  $CAMPAIGN_ROOT/campaign-ledger.json \
  --impersonate-service-account=bench-guest@mysten-research.iam.gserviceaccount.com
```

   The evidence layout is
   `<prefix>/<stage>/evidence/<cell>/attempt-<n>/<name>` for summaries and
   `<prefix>/<stage>/raw/<cell>/attempt-<n>/<name>` for samples. Apply the
   90-day lifecycle rule to `raw/`; keep `evidence/` and the ledger.

10. Delete the fleet. This step deletes the three hosts, their boot disks and
   the placement policy, then proves each absence.

```sh
python3 experiments/gcp-rdma/gcp_manual_compute.py delete --state $STATE
```

11. Verify deletion independently of the state file. All four commands must
   return no row.

```sh
gcloud compute instances list --filter="name~^$RUN-" --format='value(name)'
gcloud compute disks list --filter="name~^$RUN-" --format='value(name)'
gcloud beta compute resource-policies list --regions=us-central1 \
  --filter="name=$RUN-placement" --format='value(name)'
gcloud compute instances list --filter="labels.campaign=$CAMPAIGN" --format='value(name)'
```

   A `cleanup incomplete` error is not success. Keep the state file: the next
   `delete` resumes from the recorded intents and finishes the deletion.

### Image requalification for the campaign fleet

`config.json` pins `qualification_profile: "focused"`. The image
`bench-img-3012611b9ce24218-image` (numeric ID `1937849582956621267`) carries a
focused receipt whose recipe hashes are older than the current
`gcp_image_prepare.py`, `gcp_image_qualify.py` and `gcp_worker.py`, so
`gcp_image.admit_image` rejects it. Admission needs two fresh reciprocal
diagnostics from that exact image.

The cheapest correct path uses two hosts and no campaign workload:

1. Seal the focused package from this exact checkout. The guest computes the
   recipe hashes from the package copies of `gcp_image_prepare.py`,
   `gcp_image_qualify.py` and `gcp_worker.py`, so an older package fails
   admission again.
2. Create a two-host fleet for the pair, with a 30 minute deadline.
3. Run `gcp_image_qualify.py --qualification-profile focused` on both guests.
4. Upload the two diagnostics, mint the receipt, upload the receipt.
5. Write the receipt reference into `config.json` with `--config`.
6. Delete the two hosts.

```sh
python3 -c 'import json,sys; sys.path.insert(0,"experiments/gcp-rdma"); import manual_campaign as m; json.dump(m.REVIEWED_SPECS["nvmeof_path_compact"],open(sys.argv[1],"x"),sort_keys=True,indent=2)' /absolute/qualify/focused-spec.json
docker build -f experiments/gcp-rdma/Dockerfile.build -t tidehunter-gcp-build experiments/gcp-rdma
python3 experiments/gcp-rdma/prepare.py --engine "$PWD" --revision "$(git rev-parse HEAD)" \
  --output /absolute/qualify/package --spec /absolute/qualify/focused-spec.json \
  --deadline 14400 --build-container tidehunter-gcp-build
cp /absolute/qualify/focused-spec.json /absolute/qualify/package/spec.json
sha256sum /absolute/qualify/package/manifest.json
```

The last line prints the `--manifest-sha256` value the guest command needs: the
manifest identity is the SHA-256 of the manifest file itself.
Preparation is a local Cargo build; it allocates no cloud resource.

The pair fleet uses its own campaign id, so its host names cannot collide with
the campaign fleet. `--role qualification` admits a READY image without a
receipt, because the receipt is what this fleet produces; it is refused for a
campaign fleet. The deadline is the leak guard, not the expected run time.

```sh
PAIR_CAMPAIGN=$CAMPAIGN-qualify
PAIR=bench-$(python3 -c 'import hashlib,sys; print(hashlib.sha256(("tidehunter-campaign:" + sys.argv[1]).encode()).hexdigest()[:16])' $PAIR_CAMPAIGN)
python3 experiments/gcp-rdma/gcp_manual_compute.py create \
  --campaign $PAIR_CAMPAIGN --state /absolute/qualify/pair-state.json \
  --hosts 2 --hours 1 --role qualification --image $IMAGE --source-sha256 $BUNDLE
python3 experiments/gcp-rdma/gcp_manual_compute.py verify \
  --state /absolute/qualify/pair-state.json
```

Creation prints the two hosts with their internal and RDMA addresses; those are
the peer values below. Delete the pair with the same state file when the receipt
exists:

```sh
python3 experiments/gcp-rdma/gcp_manual_compute.py delete --state /absolute/qualify/pair-state.json
```

The guest command runs on each host with the other host as the peer. Replace
the peer address, the peer instance name, the RDMA device and the package
digest with the values from `verify` and the seal. Copy the sealed package to
both hosts first; it is about 106 MB.

```sh
gcloud compute ssh $PAIR-0 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo mkdir -p /mnt/tidehunter/qualify'
gcloud compute scp --recurse --zone us-central1-a --tunnel-through-iap \
  /absolute/qualify/package $PAIR-0:/tmp/qualify-package
gcloud compute ssh $PAIR-0 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo cp -a /tmp/qualify-package /mnt/tidehunter/qualify/package'
```

Repeat both commands for `$PAIR-1`. The reciprocal perftest servers must listen
on both hosts before either client starts; each host is a client to the other.

```sh
gcloud compute ssh $PAIR-0 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo nohup ib_read_lat -R -d <irdma device> -p 18515 -s 1024 -n 1000 >/tmp/ib-read.log 2>&1 &
             sudo nohup ib_send_lat -R -d <irdma device> -p 18516 -s 1024 -n 1000 >/tmp/ib-send.log 2>&1 &
             sleep 5'
gcloud compute ssh $PAIR-1 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo nohup ib_read_lat -R -d <irdma device> -p 18515 -s 1024 -n 1000 >/tmp/ib-read.log 2>&1 &
             sudo nohup ib_send_lat -R -d <irdma device> -p 18516 -s 1024 -n 1000 >/tmp/ib-send.log 2>&1 &
             sleep 5'
gcloud compute ssh $PAIR-0 --zone us-central1-a --tunnel-through-iap \
  --command 'sudo /opt/tidehunter-python/bin/python3 \
    /mnt/tidehunter/qualify/package/source/experiments/gcp-rdma/gcp_image_qualify.py \
    --package /mnt/tidehunter/qualify/package \
    --manifest-sha256 <package manifest sha256> \
    --peer <peer RDMA address> --peer-instance <peer name> \
    --rdma-device <irdma device> --qualification-profile focused \
    --output /mnt/tidehunter/qualify/diagnostic.json'
```

   Run the same client command on `$PAIR-1` with the addresses and the instance
   name swapped, then stop the four servers with
   `pkill -f 'ib_(read|send)_lat'`. Each diagnostic is about 200 KB and records
   the two perftest command lines, the kernel modules, the container digests,
   the io_uring read and the sealed runtime linkage.

Then, on the workstation. The receipt input names the exact image, the sealed
runtime manifest and the two uploaded evidence objects; the tool verifies every
file digest against its reference.

```json
{
  "image": {"path": "projects/mysten-research/global/images/bench-img-3012611b9ce24218-image",
            "id": "1937849582956621267"},
  "runtime_sha256": "<sha256 of the sealed manifest.json>",
  "service_account": "bench-guest@mysten-research.iam.gserviceaccount.com",
  "evidence": [
    {"file": "/absolute/qualify/diagnostic-0.json",
     "reference": {"bucket": "mysten-research-bench-artifacts",
                   "object": "runs/<pair run id>/qualification/0/diagnostic.json",
                   "generation": "<uploaded generation>", "sha256": "<file sha256>"}},
    {"file": "/absolute/qualify/diagnostic-1.json",
     "reference": {"bucket": "mysten-research-bench-artifacts",
                   "object": "runs/<pair run id>/qualification/1/diagnostic.json",
                   "generation": "<uploaded generation>", "sha256": "<file sha256>"}}
  ]
}
```

```sh
gcloud storage cp /absolute/qualify/diagnostic-0.json \
  gs://mysten-research-bench-artifacts/runs/$PAIR/qualification/0/diagnostic.json
gcloud storage cp /absolute/qualify/diagnostic-1.json \
  gs://mysten-research-bench-artifacts/runs/$PAIR/qualification/1/diagnostic.json
python3 experiments/gcp-rdma/gcp_image_receipt.py \
  --spec /absolute/qualify/receipt-input.json \
  --output /absolute/qualify/image-receipt.json --qualification-profile focused
gcloud storage cp /absolute/qualify/image-receipt.json \
  gs://mysten-research-bench-artifacts/runs/$PAIR/qualification/image-receipt.json
python3 experiments/gcp-rdma/gcp_image_receipt.py \
  --spec /absolute/qualify/receipt-input.json \
  --output /absolute/qualify/image-receipt.json --qualification-profile focused \
  --config experiments/gcp-rdma/config.json \
  --bucket mysten-research-bench-artifacts \
  --object runs/$PAIR/qualification/image-receipt.json \
  --generation <uploaded generation>
```

`--config` writes `image`, `image_qualification` and `qualification_profile`
from the receipt and refuses stale recipes, a foreign bucket, a different
image or a malformed generation. `$PAIR` is the two-host fleet run id. The
receipt binds the two guest instance IDs, so the pair must stay alive until the
receipt is written.

For an explicitly manual dirty checkout, keep preparation output and the
specification outside the checkout. Generate the exact reviewed specification,
then build the image and seal the package (replace both absolute paths):

```sh
python3 -c 'import json,sys; sys.path.insert(0,"experiments/gcp-rdma"); import manual_campaign; json.dump(manual_campaign.REVIEWED_SPEC,open(sys.argv[1],"x"),sort_keys=True,indent=2)' /absolute/path/manual-spec.json
docker build -f experiments/gcp-rdma/Dockerfile.build -t tidehunter-gcp-build experiments/gcp-rdma
python3 experiments/gcp-rdma/prepare.py --engine "$PWD" \
  --revision "$(git rev-parse HEAD)" --output /absolute/path/new-manual-package \
  --spec /absolute/path/manual-spec.json --deadline 14400 \
  --build-container tidehunter-gcp-build --manual-allow-dirty
cp /absolute/path/manual-spec.json /absolute/path/new-manual-package/spec.json
```
The preparation deadline bounds the local build and qualification. Its accepted
range is 1800–14400 seconds. The manual fleet expiry remains 21600 seconds.

An existing pinned build image and verified native dependencies can be reused.
For a direct `cloud_package.py build`, `--target-dir` selects an existing Cargo
cache directory. Cargo still builds the frozen sources with the required
features and flags; the cache does not replace source or binary qualification.


The opt-in is restricted to this exact manual spec. The package records dirty
status, revision and the complete frozen source inventory; preparation and
launch reject an altered checkout. The default and GitHub transfer path remain
clean-source only. Review the package receipt, frozen inventory, ABI and
qualification before use. The async `execute` callback accepts gcloud argument
lists without the `gcloud` executable and must invoke the GCloud MCP tool.
The callback must stop on cancellation or tolerate overlapping read-only
observations. An await timeout cannot forcibly stop a callback that suppresses
cancellation; its local clamp is cooperative, not a hard VM-lifetime guarantee.
The `wheels` entries are local `{path, sha256}` PyYAML 6.0.2 wheel descriptors;
the base image must be the reviewed project-local image path and numeric ID.
Permission, network, wheel, image and RDMA-device prerequisites must be checked
fresh. All-host runtime preparation, reciprocal physical RDMA qualification and
selected NVMe module/tool admission remain mandatory; these instructions do not
establish cloud hardware readiness.

All six VMs receive an initial startup gate at creation, rather than relying on
changing metadata to rerun startup. After exact guest resource and base-image
admission, an immutable per-host GCS descriptor binds the instance ID, source,
deadline and script checksum. Only after every guest has prepared, both physical
proofs pass, and qualification cleanup succeeds is the suite runtime published.
The same six identities run the suite. Evidence contains actual reciprocal
peer operations and generation-pinned hashes; image readiness alone is not
qualification.

Fresh preflight must establish required effective permissions; no IAM or API
change is made by this path. Current hosts' RDMA-CM device and peer visibility
remain unproven until a newly authorized physical qualification succeeds.

For manual artifact bytes, the MCP sandbox cannot access workstation files.
The manual transport uses the existing GCS JSON object client with a
SDK-delegated guest credential cached only in memory for at most 60 seconds
from refresh start. Concurrent transfers share one refresh. Cache hits, lock
waits and refreshes check the current store deadline, including its permitted
post-deletion extension. The SDK token's actual expiry is opaque; this cache
does not guarantee validity or retry HTTP authorization errors. Compute
lifecycle mutations remain on MCP, except the separately recorded exact-argv
fallback for the MCP's malformed JSON-description invocation.
Before an HTTP request, a token subprocess timeout is retried up to three
total attempts, each limited to 30 seconds or the remaining store deadline.
Timed-out process groups are killed and reaped; nonzero SDK exits report
only exit status, are not retried, and never expose token output or stderr.
No artifact mutation is replayed by this token retry. Transfer timeouts and
chunk checks are cooperative: DNS, response headers, and a stalled read can
exceed the local deadline.
They are not a substitute for each VM's verified platform DELETE expiry and
boot-disk autoDelete. Resource deletion is name-based; even after checking
the exact numeric ID and owner nonce there is a residual describe/delete race.
The ownership model assumes one owner per run and no malicious replacement.
VMs, boot disks and compact policies use unique run names; manual guests also
bind an owner nonce and numeric VM/disk IDs. Artifacts share the reviewed bucket
but use distinct run prefixes. The subnet, base image and service account are
shared read-only infrastructure, not resources that fleet cleanup deletes.
No fleet-specific firewall rules or network tags are created by this launcher.

The reviewed manual reservation is an estimate, not a project-wide billing
cap. Allocation requires current user authorization, local validation and a
fresh source-bound package; this document alone does not authorize a launch.

The 3 November manual-runner verification passed 189 GCP safety tests, 420
comparison tests (four additional tests skipped), and 107 observability tests.
Comparison checks used the existing workspace `.venv`, which contains the
required AWS SDK and plotting dependencies. The image regression exercised
lost stop/create replies, exact-owned reconciliation and subsequent deletion;
the historical orchestration fixture exercised the former separate bake, pair
and fleet transitions without cloud resources. Those checks do not qualify an image or demonstrate
hardware RDMA. The following older counts refer to an earlier source snapshot.

### Verification of this preparation pass

Local checks passed: **96 GCP**, **417 comparison** (four skipped), **631 AWS
orchestration** (four skipped), and **165 Sui harness** tests (one skipped).
Sixteen workflow shell blocks passed `bash -n`. The actual offline CLI planned
the unchanged **273-case**, ten-backend, one-repetition matrix. Duplicate backend
selection and the real unqualified-image configuration were rejected.

The workflow's shell gate was executed for disabled launch, disabled-but-read-only
preflight, and enabled launch preparation. A compiled C executable and shared
library exercised actual ELF SONAME/version extraction and compatibility
admission; an unsupported OPENSSL version was rejected. These local fixtures do
not establish physical RDMA. Two independent static re-reviews found no remaining
blocker after correcting delivery retries, scratch mapping, disk ownership, image
evidence parsing and the ABI boundary.

GCP MCP returned no `bench-*` instances, persistent disks or placement policies.
The live protected environment still has `GCP_BENCH_ENABLED=false`. No resources
were created, no activation was enabled, no source was committed/pushed, and the
new required-status ruleset was not applied live in this pass. Hardware-image
qualification and publication remain the next operational steps.


### Manual two-host diagnostic, 2026-11-02

An independently operated, two-host diagnostic used SSH/SCP and instance-scoped
SSH keys without guest service accounts or a GCS bucket. It is outside GitHub
Actions and is not the six-host campaign. It does not satisfy or replace the
workflow's WIF, service-account, bucket, budget, expiry, or result-admission
requirements above.

The Rocky Linux 8.10 HPC guest's 4.18 kernel returned `ENOSYS` for
`io_uring_setup`; the diagnostic therefore selects positional owner WAL reads,
not the asynchronous read path. The observed `irdma0` devices completed
cross-host RDMA-CM RC perftest READ and SEND checks. The corrected-source manual
diagnostic completed three 64-operation qualifications and fifteen
1,000-operation measured cells, with all observations successful and results
retrieved locally. There are two physical hosts and five logical owners on one
owner host: this is not a five-independent-owner comparison or a cold-NVMe
measurement. The separate Rust raw FI_READ probe's strict native-device admission
currently rejects the observed provider metadata because its driver is
reported as `NULL`; perftest READ success does not establish that this guarded
Rust probe succeeded. Do not weaken the guard or substitute a software provider.

Rocky OpenSSL 1.1's default request configuration added a CA basicConstraints
extension in addition to the fixture helper's explicit extension, producing a
duplicate rejected by rustls. The helper now supplies a fixture-owned minimal
OpenSSL request configuration for CA generation while retaining the explicit
critical constraints and key usage. This changes test certificate generation,
not production TLS validation. Guest build and certificate/handshake
qualification remain necessary for each new source snapshot.

The Netherlands audit below is historical and does not override the current
US target.


## Netherlands access audit, 2026-11-02

GCP MCP and local authentication were re-established for `loris.tran@epfl.ch`.
Only read-only discovery and `testIamPermissions` queries were performed.
`config.json` now names the existing Netherlands resources; the bucket, guest
service account, prepared image and budget deliberately remain unconfigured.
GitHub activation remains `false`. No VM or other cloud resource was created.

| Resource | Observed state |
| --- | --- |
| Zone | `europe-west4-b` |
| Ordinary subnet | `projects/mysten-research/regions/europe-west4/subnetworks/default`, `10.164.0.0/20` |
| RDMA network | `falcon-rdma` |
| RDMA subnet | `falcon-rdma-europe-west4`, `10.10.0.0/20` |
| Network profile | `projects/mysten-research/global/networkProfiles/europe-west4-b-vpc-falcon`, RDMA/FALCON, IRDMA |
| RDMA firewall | `falcon-rdma-allow-internal`, enabled ingress, all protocols from `10.10.0.0/20` |
| Machine type | `h4d-highmem-192-lssd`, 192 vCPUs, 1,488 GiB RAM |
| General regional CPU quota | 5,000; reported usage zero |
| H4D-specific quota | Not verified: `cloudquotas.quotas.get` denied; legacy Service Usage queries by H4D metric and display name returned no entries, not a zero limit |
| Ordinary subnet Private Google Access | **Disabled**, so the current private-only launcher must refuse allocation |
| Existing labelled benchmark fleet | No matching `bench-run` instances returned |

The Falcon network omits `mtu`; that is consistent with its documented
profile-controlled 8,896-byte default. Admission accepts omission only with the
configured zonal Falcon profile and rejects a conflicting explicit MTU.
Private Google Access belongs on the ordinary subnet, **not** the Falcon
subnet: the live Falcon profile explicitly blocks that feature.

The account can create, inspect and delete VMs and compact placement policies,
use both subnets, and act as the visible service accounts. It cannot change
Private Google Access, create buckets or WIF resources, modify project IAM, or
mint access tokens for either visible service account. These user permissions
do not establish the permissions of a future GitHub or guest identity.
Bucket listing and WIF pool listing were denied; absence was not established.
See `access-audit.json` for the queried permissions and exact observations.

### Remaining work with the project administrator

1. Enable Private Google Access on the ordinary subnet, or supply a separate
   already-enabled ordinary subnet in the same zone's region. Do not modify the
   Falcon subnet for this purpose.
2. Confirm at least 1,152 H4D vCPUs and 22,500 GiB of the applicable H4D local-SSD
   quota in `europe-west4`. Quota is not a reservation or a capacity guarantee.
   A quota viewer grant is sufficient for inspection; an SDK add-on is not the
   current blocker.
3. Supply the existing artifact bucket and guest service account, verify their
   scoped permissions below, and configure the GitHub WIF provider and
   orchestrator identity. The local account cannot grant these permissions.
4. Supply and qualify a prepared RDMA guest image with a compatible build
   environment. The public candidate
   `projects/cloud-hpc-image-public/global/images/hpc-rocky-linux-9-v20260930`
   was READY when queried, but is **not** admitted as an application-ready
   image. It does not establish Docker images, Python packages, exact libfabric
   2.6.0, NVMe target modules or application ABI compatibility.
5. Approve the complete cost quote and fill `budget.json`; then qualify exact
   release artifacts and run non-allocating preflight before enabling launch.

For the subnet change, the operator must follow these steps:

* **Step 1**: Syntax Validation via
  `gcloud help compute networks subnets update`.
* **Step 2**: Parameter Verification: use project `mysten-research`, region
  `europe-west4`, subnet `default`, and `--enable-private-ip-google-access`.
  The inspected command help offers neither `--dry-run` nor `--validate-only`.
* **Step 3**: Dry-Run Command Proposal: not available for this command. Inspect
  the current subnet first and obtain approval for changing the shared subnet.
* **Step 4**: Command Proposal & Authorization: an authorized administrator may
  run the following after explicit approval; it was **not executed** here.

```bash
gcloud compute networks subnets update default \
  --project=mysten-research --region=europe-west4 \
  --enable-private-ip-google-access --quiet
```

### Image and build compatibility

The KV preparation job now builds in the pinned Rocky 9 container, including
when its runner is Ubuntu 24.04. GCP admission additionally limits ELF
requirements to GLIBC 2.34 and GLIBCXX 3.4.29; the shared package check's GLIBC
2.39 ceiling alone is not sufficient. The runtime carries no replacement glibc
or dynamic loader. Selected Sui builds still need their own compatible build
and guest-loader qualification; the KV build does not establish Sui readiness.
Google's documented IRDMA images and driver requirements remain the admission
boundary. A public or captured image being READY is not application or physical
RDMA qualification.

Relevant source paths are `.github/workflows/gcp-rdma-benchmarks.yaml`,
`experiments/gcp-rdma/prepare.py`, `gcp_bootstrap.py`, and
`experiments/kv-comparison/cloud_package.py`.
References: [IRDMA OS and driver support](https://docs.cloud.google.com/compute/docs/networking/using-irdma),
[HPC images](https://docs.cloud.google.com/compute/docs/images/workload/hpc-vm-images),
[ACI images](https://docs.cloud.google.com/compute/docs/images/workload/aci-images),
and [OS networking support](https://docs.cloud.google.com/compute/docs/images/os-details#networking).

### Cost before activation

The official H4D price table was read with **Netherlands (`europe-west4`)**
selected, not its default Iowa region, on 2026-11-02.

| On-demand item | USD |
| --- | ---: |
| One `h4d-highmem-192-lssd` VM-hour, including its mandatory local SSD | 10.083789168 |
| Six-host fleet-hour | 60.502735008 |
| Three fleet-hours, compute/local SSD only | 181.508205024 |

Boot disks, network traffic, retained artifacts, taxes and account-specific
discounts are not included. This is a public list-price observation, not a
complete run upper bound or an actual bill. `budget.json` remains unset.
Source: [Google Compute-optimized pricing](https://cloud.google.com/products/compute/pricing/compute-optimized).

`z4m-highmem-16` was found in `us-central1-c` (16 vCPUs, 122 GiB RAM and its
documented 14,000 GiB local SSD). Reusing that smaller shape would require a
zone-compatible Falcon network/subnet and a separate quote and quota check.
No US fallback network or candidate was invented or created.

The official Iowa table lists `z4m-highmem-16` at $3.287109973 per VM-hour,
or $19.722659838 per six-host hour, including mandatory local SSD and excluding
the same ancillary charges. It is the lower-cost alternative worth requesting
if the administrator can add its required US Falcon network. It has different
CPU, SSD and network limits from H4D; results must retain the actual hardware
identity and must not be pooled as matched repetitions across machine types.
Source: [Google storage-optimized pricing](https://cloud.google.com/products/compute/pricing/storage-optimized).


## GCP administrator request

Ask an administrator to verify or provision, without exposing long-lived keys:

* A Workload Identity Pool and GitHub OIDC provider in project `mysten-research`.
  Map `google.subject=assertion.sub`, `attribute.repository=assertion.repository`,
  and `attribute.ref=assertion.ref`, and constrain provider admission to
  `assertion.repository == 'Loris-EPFL/tidehunter-distributed' && assertion.ref == 'refs/heads/bench/gcp-rdma'`.
  Prefer additionally mapping immutable repository and owner numeric IDs and
  restricting those verified IDs. GitHub OIDC subject/environment claims and
  the chosen mapping must match the actual protected job. Grant the constrained
  principal `roles/iam.workloadIdentityUser` on the orchestrator service account;
  do not grant it to the whole pool. See [Google deployment federation](https://cloud.google.com/iam/docs/workload-identity-federation-with-deployment-pipelines),
  [GitHub OIDC](https://docs.github.com/en/actions/concepts/security/openid-connect),
  and [Google IAM service-account permissions](https://cloud.google.com/iam/docs/service-account-permissions).
* Existing compatible ordinary and RDMA VPC/subnets, in the specified zone and
  network profile, with only the private connectivity required by the six-host
  campaign. The ordinary subnet must enable Private Google Access
  (`privateIpGoogleAccess=true`) because guests have no external IP. No public
  ingress or default-network assumption. See
  [Google Cloud RDMA](https://cloud.google.com/compute/docs/instances/configure-rdma).
* An existing private bucket with version/generation preconditions available and
  the reviewed real fixture object retained at the configured generation. Limit
  access to the campaign prefix and fixture as appropriate; retain campaign
  artifacts until collection is confirmed. See [Cloud Storage request preconditions](https://cloud.google.com/storage/docs/request-preconditions).
* APIs already enabled for Compute Engine, Cloud Storage, IAM Credentials and
  Security Token Service as applicable. The workflow does not enable services.
* A dedicated orchestrator identity with only the required scope: Compute
  `instances.create`, `instances.get`, `instances.delete`, `instances.setMetadata`,
  `instances.setLabels`, `instances.setServiceAccount`, `disks.create`,
  `disks.get`, `disks.delete`,
  `images.list`, `images.useReadOnly`, `subnetworks.use`, `machineTypes.get`, `networks.get`,
  `resourcePolicies.create`, `resourcePolicies.get`, `resourcePolicies.use`,
  `resourcePolicies.delete`, and zonal/regional `operations.get`;
  `iam.serviceAccounts.actAs` and `iam.serviceAccounts.getAccessToken` on the exact
  guest service account (for example, a narrowly scoped Service Account Token
  Creator grant for the latter); and bucket
  `storage.objects.get`, `storage.objects.list`, `storage.objects.create`,
  `storage.objects.delete` (needed for conditional ledger updates). Verify
  actual effective permissions and quotas with the administrator; do not grant
  project-wide owner or mutation of IAM. The guest identity needs
  `compute.instances.get` on all six fleet VMs for identity verification, plus
  narrowly scoped object access for mailboxes and artifacts; it does not need
  fleet mutation or IAM administration. Preflight uses a short-lived guest token
  to test project-level `compute.instances.get` and bucket-level object
  permissions before allocation; it does not change IAM. These checks do not
  guarantee later access to individual VMs under conditional policies or IAM
  propagation delays.
  See [Compute Engine IAM](https://cloud.google.com/compute/docs/access/iam)
  and [Cloud Storage IAM permissions](https://cloud.google.com/storage/docs/access-control/iam-permissions).

The reviewed guest image must already contain Python 3.11 or later, PyYAML, OpenSSL 3, curl,
`ldd`, systemd, Docker and the pinned campaign images, NVMe tools and applicable
kernel `nvmet`/`nvme-rdma` support, and libfabric 2.6.0 in standard dynamic
loader paths plus `fi_info` from the pinned project setup. Guest admission
requires exactly one linked libfabric and `fi_info --version` reporting 2.6.0.
Valkey RDMA additionally needs the matching RDMA-CM/verbs libraries, device
access, provider and memory-registration support. Preinstalled software
providers are not proof of physical RDMA or supported NVMe/RDMA operation.

The observed account can create/delete VMs and placement policies and act as the
visible service accounts, but pool/provider listing and project IAM inspection
were denied; this does not prove that no pool exists. It cannot create WIF pools,
service accounts, networks, subnets or buckets, change IAM, or enable APIs. Ask
an administrator rather than attempting an escalation. Visible service accounts
include `orchestrator@mysten-research.iam.gserviceaccount.com` and the default
compute identity; visibility is not proof either identity is suitable.

## Operations and evidence

Choose suite `kv`, `sui`, or `both`, explicit KV backend IDs, matrix mode, and
separate comma-separated Sui transports. Sui accepts `tcp`, `tidehunter_verbs`,
and `tidehunter_verbs_native`; its default is `tcp`. The default KV mode is now
`standard`, with these explicit backend IDs:

```text
native,tidehunter_tcp,tidehunter_verbs_native,tikv,cassandra,foundationdb,valkey_tcp,valkey_rdma,garnet,kvrocks
```

With one repetition this adds 25 cases each for Garnet and Kvrocks to the
previous 223-case plan. Their workloads match the Valkey arms: value sizes,
concurrency, offered loads, operation counts and raw API, scheduling and
response timings are unchanged. `tidehunter_verbs` is an additional
TLS-protected verbs option. EFA remains on AWS, not renamed as Falcon. No GCP
backend may silently fall back to TCP.
Sui verbs selections also require an active RDMA link bound to each admitted
IRDMA address on every host; worker startup checks this even for Sui-only runs.

`nvmeof-focused` requires `native`, `tidehunter_tcp` and at least one of
`tidehunter_nvme_rdma` or `tidehunter_nvme_tcp`; portable verbs can also be
selected. For the portable RDMA comparison include `tidehunter_verbs_native`
and `tidehunter_nvme_rdma`; NVMe/TCP is an optional additional arm. Standard
mode rejects NVMe backends and GCP never offers EFA. NVMe/RDMA reads the independently
copied sealed export image, not the original WAL, and must not be labelled as
native NVMe or as proven NAND reads. Sui has no Valkey reader. The Sui replay
harness has a live NVMe-oF cell (`tidehunter_nvme_rdma` in `sui_cells`, built
with `experiments/sui-checkpoint/build.py prepare --fabric --nvmeof`), but the
workflow and the manual launcher do not admit a Sui specification with that
cell yet; a launched Sui replay remains native versus owner RPC transports.
Before allocation the exact qualified image must show the selected initiator
and target transport modules on both qualification guests. The existing
cross-host RDMA-CM perftests demonstrate the underlying fabric only; they do
not prove that NVMe/RDMA discovery, connection or reads work. On every campaign
host, runtime admission loads the selected modules, requires `nvme-cli`, the
initiator character device and nvmet configfs, and binds RDMA to the active
admitted IRDMA NIC. The actual target/controller connection and namespace
admission must still succeed, and scoped teardown remains mandatory.
Both suites run sequentially on the same fleet; the launcher owns final
deletion. Preparation must qualify the software-backed adapters and exact Sui
checkpoint replay/audits before allocation; those checks cannot qualify GCP
RDMA hardware or cloud performance.

### Garnet and Kvrocks TCP controls

The `garnet` and `kvrocks` arms use five-primary RF1 clusters over TCP; neither
requests RDMA. They use the pinned [Garnet v2.2.5](https://github.com/microsoft/garnet/releases/tag/v2.2.5)
and [Apache Kvrocks v2.17.0](https://github.com/apache/kvrocks/releases/tag/v2.17.0)
implementations. The pinned images are
`ghcr.io/microsoft/garnet:2.2.5@sha256:ef84a729cc054b048d8ed17113475890dc6389a8ea8725c34771efe785841137`
and
`apache/kvrocks:2.17.0@sha256:a1e99b3653fb900d5a17b80a9487f74079edc6d5e9b70e3103c01cbfc0d5146a`.
The Kvrocks image metadata identifies rc2 despite its source revision matching
the official v2.17.0 tag.

Garnet enables its SSD hybrid-log tier and AOF with commit frequency zero and
commit-wait; its main log is limited to 25% of container memory, with a
64 MiB index cap, 128 MiB read cache and 64 MiB AOF buffer. Garnet explicitly
enables lookup compaction every 300 seconds, checkpoints every 600 seconds and
recovery that fails on error. Kvrocks enables the RocksDB WAL and synchronous
writes, with block cache limited to 25% of container memory, two 32 MiB write
buffers per column family and two background jobs. Both retain background
maintenance. OS cache eviction does not guarantee
device reads: storage-engine caches still apply. These are distinct engines and
cache/durability configurations, not Valkey identities.

Local software qualification cannot establish GCP cluster behavior, physical
RDMA or cloud performance. Successful local preparation is not a claim that
either industry store has been qualified on GCP hardware.


### Matched Valkey TCP and RDMA controls

Selecting `valkey_rdma` requires `valkey_tcp`. Preparation builds the exact
Valkey 9.0.7 commit `fe8d406c0b5a64ec897f6455e4de684629883764` with
`BUILD_RDMA=yes USE_SYSTEMD=no`, then rebuilds its bundled standalone client
with `USE_RDMA=1 static`. Disabling optional `sd_notify` integration avoids
host-dependent library autodetection; the services use systemd's `exec`
protocol. Both arms use the same server artifacts, four persistent libvalkey
connections per primary and the same bounded progress/pipeline implementation.
An ordinary TCP-only campaign keeps its existing redis-rs/image path.

The pinned upstream Cluster client hardcodes TCP. The RDMA adapter instead
connects to each explicit RDMA endpoint, maps `CLUSTER MYID` to the validated
`CLUSTER NODES`/`SLOTS` views, and fails on redirects or topology drift.
Cross-slot MGETs are pipelined per primary and dispatched concurrently across
the five primaries; replies retain input order, duplicates and missing values.
The RDMA arm uses RDMA-CM reliable-connected verbs for measured commands.
Cluster creation and bus traffic still use the ordinary TCP network.

Each primary has an exact-owned systemd service with CPU/memory/memlock limits
and executable, argument and cgroup checks. Process metrics feed the existing
Prometheus/Grafana campaign pipeline. Functional RDMA PING/SET/GET/DEL and
configuration/topology admission must pass before measurement. There is no
CLI subprocess per measured operation.
Native local exports use `services.json`, not Docker inspection files.
`experiments/kv-comparison/analyze.py` checks both runtime snapshots against
the owned launch, executable SHA, raw CPU/memory/swap cgroup files and both
memlock limits. It normalizes libvalkey's node IDs and heartbeat times only
after proving endpoint and complete slot ownership. TCP/RDMA and native/Docker
contracts remain distinct; older native exports missing raw proof are rejected.

RF1, AOF `appendfsync=always`, `no-appendfsync-on-rewrite=no` and `noeviction`
are required on both arms. Valkey GET remains a RAM lookup, not a read from
its AOF or from a cold NVMe value WAL. The comparator is experimental RDMA,
not a production or performance claim.

Implementation map:

| Path | Responsibility |
| --- | --- |
| `benchmarks/kv-research/src/backends/valkey_rdma.rs` | Matched libvalkey clients, slot-map admission, bounded concurrent pipelines |
| `benchmarks/kv-research/src/backends/valkey_rdma_shim.c` | Narrow ABI against pinned headers; upstream RDMA/RESP progress and deadlines |
| `experiments/kv-comparison/prepare_valkey_rdma.py` | Pinned source and reproducible server/client build |
| `experiments/kv-comparison/qualify_valkey_tcp.py` | Real five-primary local Cluster/API qualification before allocation |
| `experiments/kv-comparison/valkey_service.py` | Exact-owned service lifecycle and resource evidence |
| `experiments/kv-comparison/cloud_layout.py` | Separate control/RDMA addresses, matched deployment and hardware functional gate |
| `experiments/gcp-rdma/prepare.py` | Optional source preparation, sealed artifacts and same-binary local proof |
| `experiments/gcp-rdma/gcp_runtime.py` | Actual IRDMA NIC admission and per-host verbs bindings |

Sources: [pinned Valkey RDMA instructions](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/README.md#running-valkey-with-rdma),
[bundled client RDMA implementation](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/deps/libvalkey/src/rdma.c),
[Cluster client's TCP connections](https://github.com/valkey-io/valkey/blob/fe8d406c0b5a64ec897f6455e4de684629883764/deps/libvalkey/src/cluster.c#L1583-L1589).

After approval, first dispatch `preflight`, inspect its source, binary, proof,
budget, quota and capability receipts, and address any fail-closed result.
Only then dispatch `launch` or approve a branch push. Preparation and campaign
run as separate jobs; the campaign verifies the transferred source, binary and
qualification before allocation. An independent always-run cleanup job recovers
the durable GCS lifecycle state if the campaign runner fails. These jobs retain
source/binary identities, raw results, monitoring, cleanup and budget reports in
their Actions artifacts. Inspect cleanup and server-side expiry even after a
failure; do not assume an artifact upload or a GitHub timeout deleted VMs.
Independent expiration must remain in force if the runner disappears.

## Budget files and report meaning

`budget.json` is the editable admission policy. The generated accounting state
is separate: `gs://BUCKET/budget/ledger.json`, updated with generation CAS, and
`gs://BUCKET/budget/runs/RUN_ID.json`, an immutable per-run reservation. The
launcher exports `budget-report.json` with daily, ISO-weekly, monthly and lifetime
totals. Reusing a run ID does not increment the ledger twice.

These amounts book the complete quoted upper bound before allocation. They are
conservative reservations, not measured spend or Google invoices; they are not
automatically refunded when a run finishes early or fails. Do not describe
their totals as actual billed costs. Retained GCS artifacts can remain chargeable
after VMs are deleted. The configured $90 CI reservation and $145 six-host or
$48.34 compact manual reservation are conservative estimates for the restricted US Z4M policy, not
invoices or guarantees against unrelated project spending. The weekly limit
applies to the shared CI and manual ledger.

## Local qualification

For a clean committed checkout, prepare the KV runtime for the Rocky 9 guest
using a local Docker daemon and an unused output directory:

```sh
docker build -f experiments/gcp-rdma/Dockerfile.build -t tidehunter-gcp-build experiments/gcp-rdma
python3 experiments/gcp-rdma/prepare.py --engine "$PWD" \
  --revision "$(git rev-parse HEAD)" --output /absolute/path/to/new-output \
  --spec /absolute/path/to/campaign-spec.json --deadline 14400 \
  --build-container tidehunter-gcp-build
```

The builder resolves the local image to its immutable Docker image ID, records
that ID and the source-bound recipe hash, and compiles the pinned Valkey and
libfabric sources in Rocky 9. The output-owned fabric prefix is used for the
unchanged host-side software qualification; it is not evidence of hardware
RDMA. The checkout is mounted read-only, while output paths are mounted at the
same absolute paths. The binary, FDB client, fabric library and selected Valkey
executables must satisfy the Rocky 9 glibc/GLIBCXX limits. `--build-container`
and `--fabric-prefix` are mutually exclusive. The host still needs Docker and
the existing local qualification dependencies, including the pinned FDB
client preparation prerequisites. No cloud machine is used for this build.
The build container has an independent absolute deadline across toolchain
setup and compilation, with TERM followed by KILL after five seconds. Parent
cleanup checks its UUID-derived name and owner label before removing its exact
container ID. A Docker daemon error does not count as proof of absence.


The local integration run passed 47 GCP tests, 390 comparison tests with four
skips, 631 AWS orchestration tests with four skips, 165 Sui harness tests with one
skip, 97 observability tests, and 34 NVMe operator tests with three skips.
The mailbox test executed a real subprocess twice through the same durable
request gate and verified that its mutation happened once. The actual offline
campaign CLI emitted a six-host plan, and the selected default matrix contained
116 cases. Offline fixture prices and resource names were not deployment inputs.

The new REST client also called the real project and service-account permission
test endpoints using the connected user identity. `access-audit.json` records the
read-only account audit; it is not an assertion that the future GitHub or guest
identities have those grants. YAML parsing and `bash -n` passed; `actionlint` was
not installed. No GitHub campaign, release-package build, GCP VM, kernel RDMA
qualification or real GCP checkpoint replay was performed in this setup task.
The workflow must still rebuild and qualify its exact release artifacts before
any cloud allocation.

Runner-dependent paths are initialized inside a step through `GITHUB_ENV`.
GitHub does not permit the `runner` expression context in job-level `env`;
ordinary YAML parsing cannot detect that workflow validation error.
Compile TCP-only and NVMe-enabled configurations separately; an all-feature build
does not detect feature-specific dead code.
The software-fabric profile runs retained message-path integration cases and
transport-level RMA probes, not the deleted owner-value-cache grant cases. It
does not establish hardware RDMA or kernel NVMe/RDMA performance.

### H4D follow-up qualification, 2026-11-02

The updated local suites ran 50 GCP tests, 391 comparison tests (four skipped)
and 165 Sui harness tests (one skipped), with no failures. The actual CLI emitted
a six-host H4D Netherlands plan using disposable, explicitly offline identities
and prices. Matrix planning produced 116 cases with one independent repetition.
All disposable fixtures were removed. The real configuration refused admission
before cloud access because its artifact bucket is still unconfigured.

Independent review found that initial SSD observations were not retained in
exports. The fix persists them in controller startup receipts and guest
bootstrap proofs, and in the Sui results archive for Sui-only or combined runs.
Corrective review found no additional consumer-visible defect.

These checks qualify local orchestration behavior, not release-binary ABI,
physical H4D RDMA, peer NVMe/RDMA connections or cloud performance. No new
release package was built and no campaign was launched in this follow-up.

### Valkey RDMA integration: local evidence

The final Python checks ran 406 comparison tests (four skipped), 54 GCP tests
and 97 observability tests without failures. Rust transport checks passed 54
tests with the RDMA feature and 51 with TCP alone; strict all-target Clippy
passed with `-D warnings`. The pinned upstream Valkey server, CLI and standalone
RDMA client libraries built locally. A debug `kv-research` executable was rebuilt
with Rust 1.90 and both Valkey features; this is not a sealed cloud release.

The actual five-primary local Cluster qualification completed 64 measurement
operations: 26 puts, 17 reads, two missing reads and 19 get-many calls. The
manifest records complete raw samples, zero errors/wrong values, zero timeouts
or unknown mutations, and no qualification failure. Its helper waited for all
five owned child servers to stop. Temporary database data was then removed;
configs, logs, manifests, raw timings and receipts remain under
`checkpoint-work/valkey-rdma-qualification-20261102/cluster-qualification`.

An actual slow-reply TCP probe exercised the same native buffered-progress shim:
a 100 ms deadline expired after about 101 ms while only five of 100 response
body bytes had arrived. This checks timeout handling, not transport speed.
An actual `valkey_rdma` connection attempt on this host failed with
`RDMA: create event channel failed`; a listening TCP control observed no
fallback connection. Neither observation is RDMA hardware qualification.

The offline Valkey-integration workflow plan at that time contained 223 cases.
Its two 25-case Valkey populations match apart from backend/case identity.
Paired libvalkey TCP is a different client cohort from historical redis-rs TCP; do not pool them or
attribute their difference solely to the transport.

Inspection also found an existing GCP startup defect: external backend storage
proofs were checked using AWS model-name rules. Backend startup now selects the
actual provider; a regression check admits six distinct Google Local SSD proofs
without weakening the boot-ID/serial independence checks.

The existing GitHub activation variable was observed as `false` and was not
changed. No campaign, GCP VM, physical RDMA peer transfer, active native systemd
Valkey service or cloud release-package qualification ran in this integration.
The image/ABI, permissions/network, budget and publication prerequisites above
remain required before launch.

### Monitoring and startup regression qualification, 2026-11-03

The observer admits exact GCP `bench-<16 lowercase hex>` identities as well as
the existing AWS `th-*` identities. Startup validates the actual rendered
configuration before guest work. Host-exporter or Prometheus/Grafana readiness
failure stops the campaign before measurement; later observer failure stops
the next case, while raw-result export and exact-owned cleanup remain active.
The GCP safety workflow also runs controller and observer regression suites.

The real local qualification is
`experiments/kv-comparison/observability/qualify_gcp_identity.py`. It starts six
logical loopback hosts, verifies live targets and Grafana queries, captures
the session, verifies offline replay and stops its owned containers. Its
deliberately failed fixture tests observation, not benchmark performance.
The local run at `/tmp/tidehunter-gcp-observer-20261103-validation` passed with
no live or replay gaps; the retained evidence is
`checkpoint-work/2026-11-03-monitoring-hardening-validation.json` in the parent
workspace. This does not supply the missing monitoring data for the completed
GCP campaign or qualify current changes on GCP hardware.

Benchmark services explicitly set `LimitNOFILE=524288`. Guest admission uses
the prepared Python and checks effective limits in a real retained transient
unit before cases. It must not use `systemd-run --wait` with
`RemainAfterExit=yes`: an exited retained unit remains active. Admission instead
polls its bounded state, validates its owned output and stops that exact unit.

### Single-fleet qualification failure, 2026-11-05

The one authorized six-host GCP fleet completed software preparation but failed
pair qualification before physical qualification or any of the planned 411 API
cases and four native Valkey controls. Runtime linkage inspection incorrectly
applied the portable Ubuntu 24 ABI ceiling to the guest's host OpenSSL library.
The fleet, boot disks and placement policy were deleted; retained evidence and
the distinction from local SPDK functional proof are documented in
`checkpoint-work/spdk-matched-storage-20261105T004442Z/single-fleet-campaign-report.md`
in the parent workspace. This run provides no cloud performance conclusion.
The local correction retains strict portable-artifact checks while validating
host dependency version closure; compiled ELF regressions passed, as did the
pinned SPDK target and private libaio on a local OpenSSL 3.5.8 runtime. This is
not guest or physical RDMA qualification; no cloud relaunch occurred.

### Buffered Linux NVMe target repair, 2026-11-05

The repeated failure at sealed-read case 0034 was not a build or capacity
failure. The preserved cloud completion was owner 0, `-EREMOTEIO`, 8192 bytes
at offset 380928; its NVMe controller reported INTERNAL with DNR. This status
alone does not prove the target's internal cause. A separate local reproduction
traced `nvmet_file_submit_bvec` returning only 4096 bytes for an 8192-byte
`IOCB_NOWAIT` read. The target treated that positive short read as an I/O error.

`experiments/nvmeof/patches/linux-nvmet-file-short-nowait-read.patch` routes
only positive short nonblocking reads through the existing blocking workqueue.
The sealed image is immutable, so repeating the full read is safe. Writes,
flush/FUA, direct I/O, page-cache policy and SPDK behavior are unchanged.
This is not a retry of a reported I/O error.

For the exact CIQ `5.14.0-687.49.1+2.1.el9_8_ciq.x86_64` guest, prepare an
external prefix containing `nvmet.ko`, `abi-admission.json`, `source.json` and
`partial-nowait-read.patch`; supply `--nvmet-fix-prefix /absolute/prefix`
to `prepare.py` or `cloud_package.py stage`. No module binary belongs in Git.
The reviewed module, source RPM, devel RPM, patch and ABI identities are pinned
by `experiments/nvmeof/kernel_fix.py`. Another kernel requires a new build and
admission; this asset is not a generic kernel override.

All six owned guests must load and report the exact candidate before physical
qualification and worker readiness. Admission checks live sysfs build IDs and
source versions, not just `modinfo` of an unloaded file. The installer preserves
normal signature/Secure Boot/lockdown enforcement and never changes the Falcon
provider drivers or physical NVMe driver. A prohibited unsigned module stops
preparation; no forced loading or security-policy change is permitted.

The actual local buffered-kernel qualification completed 65,536 verification
reads and 2,048 batch-32/C16 measurements without error. All five targets and
controllers were removed and the original local module was restored. This is
functional evidence on a shared SSD, not cloud RDMA performance or physical
NAND proof. Its retained proof is under the parent workspace's
`checkpoint-work/spdk-matched-storage-20261105T004442Z/kernel-nvmet-partial-read-fix/qualification-20261105T193521Z-2512060/out`.

The failure-only initiator collector now retains bounded command stderr and
exit status, and uses explicit UTC journal bounds. An unavailable diagnostic
remains unavailable; it must not replace the original owner/completion error.

Result retrieval retains every object and its exact generation, byte count and
SHA-256. At most eight immutable objects transfer concurrently; mutable
startup/progress/receipt objects refresh serially. Aggregate limits reserve
in-flight bytes, and successful siblings remain in the local inventory when
another object fails. A verified deleted fleet may have up to two hours of
offline transfer grace. This cannot extend VM lifetime or allocate a fleet.

### Full observer-matrix admission, 2026-11-06

The next GCP attempt loaded the reviewed kernel module on all six hosts under
their existing security settings. The formerly failing cold batch-32/C16 case
completed 65,536 verification reads and all 2,048 measured calls without error.
The campaign completed 284 API cases and four native Valkey controls, then
stopped before case 284's core workload: the observer still allowed SPDK only
for sealed reads and rejected a valid live-SPDK job. Its complete partial
archive was retrieved and all six VMs, boot disks and placement were deleted.
This partial run is not a complete SPDK comparison or a transport failure.

Observer labels now distinguish `tidehunter_spdk_live_rdma` from sealed
`tidehunter_spdk_rdma`. Valid live SPDK remains restricted to the RDMA backend;
invalid route/backend/target combinations are still rejected. Existing live
SPDK read/write interval rendering retains unavailable-owner semantics.

`qualify_observer_matrix.py` checks every generated job through the actual
observer's selection validation and waiting-state metric renderer. Manual
campaign admission runs the script from the admitted frozen package before
cloud preflight or allocation, and compares its count and canonical matrix
digest with the executing checkout. It does not run benchmark workloads,
fabricate measurements or replace hardware qualification. Local qualification
of the unchanged 411-job matrix passed.

The original failure-only journal command used an unsupported `--kernel`
option. It now uses `journalctl -k`; the actual local UTC-bounded command
completed successfully. Diagnostic failures retain their bounded stderr and
exit status rather than becoming an unexplained `RuntimeError`.

### Bounded live NVMe initiator queues, 2026-11-06

The next attempt completed 347 API cases and four native Valkey controls before
case `0347-tidehunter_nvme_rdma-65536-cold-os-large-cold_os_cache-r0`
failed to connect a historical WAL namespace. The original owner error was
`/dev/nvme-fabrics: Cannot allocate memory`; the preserved kernel journal records
failure to initialize a 128-entry MR pool for I/O queue 14. All six VMs, disks
and placement were deleted after export. The terminal archive and its 12,374
inventory entries were retrieved and verified. This remains a partial campaign.

The initiator previously omitted queue controls and the guest created 16 I/O
queues of depth 128 per controller. The client had a 16-GiB memory limit, peaked
near 738 MiB and reported no memory-limit or OOM events. These observations do
not establish host-memory exhaustion. The five retained end-of-case controllers
were rollover writers; the empty begin snapshot does not show a prior-case leak.

`tidehunter/src/distributed/nvmeof/live_connect.rs` now requests
`nr_io_queues=1,queue_size=32` on the shared kernel initiator path used with both
kernel and SPDK targets. Before namespace binding it verifies the exact owned
controller's `queue_count=2` (including admin) and `sqsize=31` (zero-based).
Failed admission uses existing disconnect/quarantine handling. Pool capacity,
pinning, read/write/flush semantics and workload sizes are unchanged.

The exact CIQ non-PI allocation model is `32 + Q × D` ordinary pool MRs per
controller. The existing 135-entry bound therefore changes from 280,800 to 8,640
modeled MRs, plus control-plane and provider headroom. This is not a guarantee
of hardware capacity. The apparent 16K ceiling inferred from the failure is
unmeasured; PBLE allocation can fail below advertised MR limits. Likewise,
`max_cqe` is a per-CQ limit, not an aggregate reservation limit.

Physical guest qualification now records `ibv_devinfo -v` capabilities and
kernel-reported `rdma -j resource show` summaries. Live-case begin/end snapshots
record the same evidence outside API measurement, not in periodic sampling or
per operation. Missing counts stay unavailable, not zero. These boundaries
are not a continuous resource-peak measurement. Local Linux NVMe/TCP can
qualify effective queues and functional recovery; physical Falcon resource
headroom and performance still require the bounded, single-fleet cloud run.
