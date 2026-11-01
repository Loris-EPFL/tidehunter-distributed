# GCP benchmark activation

The workflow is disarmed unless the `gcp-rdma-bench` environment variable
`GCP_BENCH_ENABLED` is exactly `true`. This repository does not create or grant
any of the infrastructure below. When explicitly enabled and approved, a push to
`bench/gcp-rdma` builds and qualifies, then launches the matched NVMe-focused
KV campaign with native, TCP, NVMe/TCP and NVMe/RDMA.
A manual dispatch can choose `launch` or `preflight`; `preflight` builds and
qualifies locally and checks cloud prerequisites without creating VMs. The pull request job uses no
cloud credentials and runs only offline input-admission checks. A workflow
cancellation is not fleet cleanup; do not cancel a paid run. Server-side absolute
DELETE expiry is a required separate safety gate.

## Repository administration

1. Create the GitHub environment **gcp-rdma-bench** under Settings →
   Environments. Under deployment branches, select **Selected branches and tags**
   and add `bench/gcp-rdma`. For the selected solo-maintainer setup, do not require
   another human reviewer or environment approval; the branch restriction and
   explicit disabled activation remain mandatory. Add reviewer protection later
   if another authorized reviewer is available. See [deployment environments](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments).
2. Import `experiments/gcp-rdma/ruleset.json` under Settings → Rules → Rulesets.
   It applies only to `refs/heads/bench/gcp-rdma`, requires a pull request and
   resolved review discussions, blocks deletion and force pushes, and has no
   bypass actors. It requires zero human approvals so the sole maintainer can
   merge; automatic Codex reviews are separate and are not asserted as a merge
   gate. See [rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository).
3. In that environment's Variables tab set:

   | Variable | Meaning |
   | --- | --- |
   | `GCP_BENCH_ENABLED` | Keep unset or `false` until every protection and cloud prerequisite is reviewed; set exactly `true` only to activate. |
   | `GCP_BENCH_WIF_PROVIDER` | Full provider resource name `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/POOL_ID/providers/PROVIDER_ID`. |
   | `GCP_BENCH_SERVICE_ACCOUNT` | Orchestrator service account email granted the permissions below; it must match the WIF impersonation binding. |
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
   Set project ID `mysten-research`, project number `514867804006`, the existing
   bucket and guest service account, machine and exact image (`projects/PROJECT/global/images/IMAGE`), and explicit candidate
   zone, ordinary subnet, RDMA subnet and network profile. Do not substitute a
   default VPC. Set `max_distance` no greater than 3. Obtain a current written
   upper-bound quote covering all six VMs, disks, networking, storage and other
   expected charges; set `run_upper_bound_cents`, `weekly_limit_cents`,
   `max_duration_seconds` and `quote_description`. Blank required project settings
   are unconfigured; they are not approval or prices. The selected shape, guest image, network profile,
   drivers and NVMe/RDMA capability must be supported in the selected zone;
   hardware capability and native-speed performance are not established here.
   Six `z4m-highmem-16` instances require 96 vCPU and 84,000 GiB of Titanium
   SSD quota, not the smaller two-node pilot quota.

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

The reviewed guest image must already contain Python 3, PyYAML, OpenSSL, curl,
`ldd`, systemd, Docker and the pinned campaign images, NVMe tools and applicable
kernel `nvmet`/`nvme-rdma` support, and libfabric 2.6.0 in standard dynamic
loader paths plus `fi_info` from the pinned project setup. Guest admission
requires exactly one linked libfabric and `fi_info --version` reporting 2.6.0.
Preinstalled software providers are not proof of RDMA
hardware or supported NVMe/RDMA operation.

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
and `tidehunter_verbs_native`; its default is `tcp`. `standard` supports
native, TideHunter TCP, TiKV, Cassandra, FoundationDB and Valkey TCP;
`nvmeof-focused` requires `native`, `tidehunter_tcp`, and
`tidehunter_nvme_tcp`, and additionally permits `tidehunter_nvme_rdma`.
Sui has no NVMe-oF reader. EFA and loopback-only fabric TCP are rejected for
remote Sui. Both suites use the same
fleet sequentially; the launcher owns final deletion. Preparation must qualify
the software-backed adapters and exact Sui checkpoint replay/audits before
allocation; those checks cannot qualify GCP RDMA hardware or cloud performance.

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
after VMs are deleted. No price or budget was invented for the unconfigured
project settings.

## Local qualification

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
