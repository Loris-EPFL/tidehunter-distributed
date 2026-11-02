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
   The editable weekly reservation is $100; each run reserves $90 for at most
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
four hours, below the administrator's independent six-hour `bench-*` reaper.
Workflow concurrency prevents overlapping runs from this workflow/ref. A second
generation-CAS lease at `lifecycle/active-run.json` serializes campaigns sharing
the artifact bucket, including other runners. Neither is a project-wide limit.

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
Compute's absolute four-hour DELETE deadline remains active; the administrator's
six-hour reaper is a further backstop. Boot disks use `autoDelete`.

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

`nvmeof-focused` remains a separate selectable experiment: it requires
`native`, `tidehunter_tcp`, and `tidehunter_nvme_tcp`, and additionally permits
`tidehunter_nvme_rdma`. It excludes unrelated KV backends rather than calling
Valkey's RAM reads cold NVMe reads. Sui has no NVMe-oF or Valkey reader; its real
checkpoint replay remains native versus the selected Tidehunter transports.
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
after VMs are deleted. The configured $90 reservation is a conservative estimate
for the restricted US Z4M policy, not an invoice or a guarantee against unrelated
project spending. The weekly limit applies to this CI ledger, not manual runs.

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
