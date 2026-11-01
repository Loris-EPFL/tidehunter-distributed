# GCP RDMA pilot: minimum access

Project: `mysten-research`. User: `loris.tran@epfl.ch`.
Scope: permissions and prerequisites only; no resources launched or IAM changed.
Access status below comes from the preceding read-only audit, not a successful VM launch.

## Strictly needed for our launch-and-cleanup workflow

Permissions matter; the broad predefined roles are not themselves mandatory.

| Capability | Minimum relevant permissions | Audit status |
|---|---|---|
| Create VMs and boot disks; use an image | `compute.instances.create`, `compute.disks.create`, image-use permission for the chosen image | Granted; public-image access must still work for the selected image |
| Attach ordinary and Falcon subnets | `compute.subnetworks.use` on each selected subnet | Granted at project scope; verify the new Falcon subnet after creation |
| Bootstrap and label benchmark VMs | `compute.instances.setMetadata`, `compute.instances.setLabels` | Granted |
| Attach a VM service account | `compute.instances.setServiceAccount`, `iam.serviceAccounts.actAs` on that account | Granted for the default Compute service account; recheck if using a new account |
| Create/use/remove compact placement policy | `compute.resourcePolicies.create`, `.get`, `.use`, `.delete` | Granted |
| Monitor launch and terminate our resources | `compute.instances.get`, `.list`, `.delete`; relevant operation-read permissions; `compute.disks.get`, `.delete` | Granted |
| Assign external IPs, if used for management/downloads | `compute.subnetworks.useExternalIp` | Granted; unnecessary if private connectivity supplies the required access |

This list assumes no custom snapshots, templates, static IP reservations or network tags. Those choices can require extra permissions. Do not request Owner, Editor or full Compute Admin just for this pilot. See [Google's RDMA creation permissions](https://docs.cloud.google.com/compute/docs/instances/create-vm-with-rdma).

## Missing infrastructure, not necessarily missing user permissions

An administrator must create one Falcon VPC/subnet, because none existed and this user cannot create networks/subnets. Use profile `us-central1-a-vpc-falcon`, a non-conflicting IPv4 subnet in `us-central1`, and Google's required MTU. Reuse the ordinary VPC for management traffic.

Prefer administrator precreation over granting permanent Network Admin. Both VMs must share a zone and a compact placement policy with maximum distance 3 or lower. See [Falcon network setup](https://docs.cloud.google.com/vpc/docs/create-vpc-network-rdma).

Positive Z4M regional CPU and family-specific local-SSD quota must also be confirmed. Quota and physical capacity are separate from IAM; the audit did not confirm usable Z4M quota.

## Conditional and optional permissions

| Permission or role | Strictly needed? | Recommendation |
|---|---|---|
| `serviceusage.services.use` | **Conditional:** required when setting/using a quota project, including `x-goog-user-project`, `--billing-project`, or equivalent client configuration. Not an RDMA hardware requirement. | Explicit quota attribution failed without it; resource-scoped read calls succeeded without that header. VM launch was not tested. Grant it if our selected tooling uses quota attribution. |
| `roles/serviceusage.serviceUsageConsumer` | **No specific role is mandatory.** This role is a standard way to obtain the permission above; a custom role can also supply it. | Useful to avoid quota-attribution failures, but do not call it universally required for Compute/RDMA. |
| Bucket-scoped `roles/storage.objectUser` | Required **if we use GCS for source/result transfer**, not for RDMA itself. | Admin creates one dedicated bucket; grant to the operator and VM service account. Avoid project-wide Storage Admin. |
| `storage.buckets.get` | Conditional on artifact tools reading bucket metadata. | Add only on the artifacts bucket if needed. |
| `roles/iam.serviceAccountUser` | Required only if it supplies otherwise missing `actAs` permission. | Already unnecessary for the audited default account; scope to a new benchmark account if one is created. |
| API-enable, IAM-edit, quota-edit, bucket-create, network-admin roles | Not needed by the benchmark operator if an administrator prepares these resources. | Do not request them by default. |

Google documents the quota-project condition explicitly: [quota-project permissions](https://docs.cloud.google.com/docs/quotas/set-quota-project#permissions_required_to_set_and_use_the_quota_project). [Storage roles](https://docs.cloud.google.com/iam/docs/roles-permissions/storage) define the artifact-access options.

## Small initial allocation and safety gates

Start with **two `z4m-highmem-16` VMs in `us-central1-a`**: one coordinator/initiator and one storage shard. Each has 16 vCPUs, 122 GB RAM and a mandatory 14,000 GiB local SSD. Request quota for 32 vCPUs and both machines' SSDs—not a six-node fleet or GPU quota.

This is the smallest documented Z4M shape, **not a verified cheapest hourly quote**. Include mandatory SSD, boot disk and network charges before approval. See [Z4M specifications](https://docs.cloud.google.com/compute/docs/storage-optimized-machines#z4m_machine_types).

Before a full benchmark campaign: qualify the guest RDMA driver and Linux NVMe/RDMA path on these two nodes; cloud RDMA availability alone does not prove that path works. Configure a hard cleanup deadline, preserve results outside the VMs, terminate both VMs and remove chargeable temporary disks. No extra VM IAM role is needed for cleanup performed by the existing operator identity; a separate cleanup identity needs its own narrowly scoped delete access.
