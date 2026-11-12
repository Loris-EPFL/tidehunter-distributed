# Evidence and campaign closure

## Frozen campaign

- [GitHub workflow 40573961972](https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/40573961972), push-triggered once from protected `bench/aws-efa`, conclusion **success**.
- [PR 28](https://github.com/Loris-EPFL/tidehunter-distributed/pull/28) merged after all nine CI checks passed, including real software-backend qualification. No checks or review-thread protection was bypassed.
- Engine source commit: `87465ef68abb6fa9b7b9cb27224281a7c4994e6d`.
- Run: `th-6449c7068670440c`. One repetition, all **258/258** cases, all nine variants, full monitoring recorder criteria satisfied. This is the synthetic KV comparison, **not Sui checkpoint replay**.
- Build source SHA256: `142c70377c790cdc1954d613f3733d524329b07b9f911c5c7914856e1b77c287`; source-tree SHA256 `0ce65350c27c8b92d6e4591b9a3a4279723382a94c516a260ab521b5b16881f7`.
- Fresh campaign binary SHA256: `6efeeca83dff98939d9e175fa95c4ae152462d781e3e6704769dac8327757420`. Source-bound runner bundle SHA256: `ebc9fba2adc0b8674b9eaba00f8bcbd339c77d027901248a1317cccecb6a7d65`.
- The prepare job rebuilt all adapters/EFA from this merged commit and qualified the exact release against real Valkey, TiKV, TideHunter TCP, FoundationDB, native and Cassandra local servers before AWS credentials/allocation. This software qualification is not an EFA hardware performance result.

## Actual fleet

Six On-Demand `g6.8xlarge` hosts in `eu-central-1c`, **cluster** placement group `pg-08085a480a313e96d`: one controller and five storage hosts. All six EFA interfaces and root-volume DeleteOnTermination settings were confirmed through MCP. The five worker bootstrap receipts report unique `Amazon EC2 NVMe Instance Storage` device serials and ext4 at `/mnt/tidehunter-kv-comparison`; they are not five databases sharing one disk. US attempts obtained no fleet before this Frankfurt fallback. No replacement fleet or additional campaign was launched.

Launch: 2026-10-27 20:18:53 UTC. Absolute expiry: 23:12:55.171503 UTC; reserved export window began at 23:02:55.171503 UTC. Measurements finished at 21:43:07.772245 UTC, and closure completed before expiry. The observed-resource settlement covers about 5,609 seconds of instance lifetime per host; timestamps are not an inferred throughput forecast.

## Local raw evidence

Checkout-relative artifact root:

```text
checkpoint-work/kv-comparison/cloud-ops/th-6449c7068670440c/github/
  archives/                         original GitHub ZIP artifacts
  download-verification.json        published ZIP SHA256 matches
  extracted/aws-efa-40573961972-1/
    s3/th-6449c7068670440c/          retrieved original S3 objects
    validated-results/              admitted raw cases and regenerated report
    resource-journal.json
    retrieval-manifest.json
    campaign-validation.json
    cleanup-verification.json
    settlement-receipt.json
  extracted/aws-costs-40573961972-1/
    aggregate.json
    billing-refresh.json
  extracted/campaign-inputs-40573961972-1/campaign-inputs.tar.gz
```

[INTEGRITY.json](INTEGRITY.json) records independent local verification of all three GitHub artifact SHA256s, **5,865 S3 objects / 273,259,979 bytes**, and **17,885 controller archive files / 912,670,772 uncompressed bytes** against their inventories. The 204,421,114-byte original controller archive SHA256 is `33dcdb37734c2b77520d8766647695e0e6795c69e938fa85d404e85d996a33ca`; its inventory SHA256 is `f63ace789d2d479e70384772abc900ddd1e559f9b82731dc33d580e95e0694bc`. Multipart S3 ETags are not treated as MD5 hashes.

**Original versus derived:** CI re-extracts the archive and regenerates `validated-results/comparison.json`. Its sole difference from the immutable archived summary is removal of the top-level `monitoring_complete` field. All case data and every other key are identical. Every other admitted local controller file matches its original inventory. Monitoring completeness remains independently recorded in `campaign-validation.json`, `export-receipt.json` and the observability receipt. Do not compare the regenerated JSON byte hash with the immutable inventory and call it corrupted.

Private durable S3 prefix: `s3://tidehunter-ci-354775971805-us-east-1/th-6449c7068670440c/results/`. Its evidence lifecycle expires objects after 30 days; current GitHub raw/cost artifacts expire on 2026-11-26, while the input artifact expires on 2026-10-29. All were downloaded locally before those dates. [Population tables and CSV](TABLES.md) and [figures](REPORT.md#figures) derive from these retained case populations, not qualification fixtures or old runs.

## Closure and cost

[CLOSURE.json](CLOSURE.json) separates the parent's independent MCP confirmation of all six exact EC2 IDs as **terminated** from the operations agent's successful residual checks across all 23 enabled regions: no run-owned EBS volumes, ENIs, Elastic IP allocations, security groups, placement groups, or run-prefixed IAM roles/profiles/policies remained. Volume/ENI/EIP scans use the unique RunId alone, not only a stricter multi-tag conjunction. The workflow performed teardown automatically; neither parent nor operations agent needed a manual AWS mutation. Other users' resources were not targeted.

[COSTS.json](COSTS.json) derives the run and calendar-day/week/month/lifetime estimates from the generated, non-budget report at ledger revision 87. The only editable admission budget remains `experiments/aws-sharded-wal/budget.json`. The run estimate is **$23.691202753990**, including $23.634205336992 for EC2/gp3/auto-public-IPv4 and a partial observed S3 upper bound. No active reservation or unresolved run remains. Actual AWS billing is **unavailable**, not zero: the linked account is denied `ListCostAllocationTags`. Retained S3 results remain billable until expiry; the current storage projection is $0.006271439256, separate from observed usage, and unknown or late charges are not fabricated.

## Operational observations and limits

- Watchdog Lambda `IncompleteCleanup` alarms during the active run were caused by the safe `NonterminalComputeProtectsRun` IAM guard. Regional compute reaping precedes IAM cleanup; the alarm did not establish failed EC2 termination. No watchdog mutation or active-source change was made. This noisy health signal remains a maintenance issue.
- [OBSERVABILITY.md](OBSERVABILITY.md) distinguishes complete recorder criteria, point-in-time Prometheus/Grafana API readiness, missing/inapplicable panel data, and actual physical-device versus logical reads. No visual Grafana verification or offline replay of this campaign was exercised.
- Plotter execution produced 13 PNG/PDF figure pairs. Parent visually inspected primary read, write, get-many32 and configured-cache figures. Original evidence was not modified.
- One repetition gives no run-to-run uncertainty estimate. Historical region/source changes and adapter differences prevent a causal performance claim. Native is an embedded lower bound; RF1 does not equalize all consistency, durability, transaction or security contracts.
- Final local harness checks before publication: 624 AWS tests run (3 optional skips), 166 Sui harness tests and 263 KV comparison tests. These are distinct from the actual cloud matrix. No new full Sui replay, physical InfiniBand/RoCE/Falcon test or Valkey RDMA run is claimed.
