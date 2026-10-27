# Evidence and limits — GitHub campaign 40226403293

## Identity and integrity

- GitHub run: <https://github.com/Loris-EPFL/tidehunter-distributed/actions/runs/40226403293>, attempt 1, protected branch `bench/aws-efa`.
- RunId: `th-e4a783f03101a780`; selected suite `kv-comparison`.
- Frozen Git revision: `c61c9e295708e5b4354dae43538e900f3515df4d`.
- Source **archive** SHA-256: `6e555483edef15acba4f02ecb05209003fbc9987115744c9f57a1f07e08ed9da`. This is not a Git commit ID.
- Measured binary BLAKE2b-512: `c148b6b33a1354bff4c9e70cd543413d11d12535ee8ba59421df121a6f1a71fa5ce06885159ab9e8b5f008dcba18d5a44e7ad764e8417f19cc082badb5672fc1`.
- Raw controller archive: 387,160,001 bytes; SHA-256 `bc4472c99850f774bf4420b86b3ddc32d0033354b68ff9f79c93750cdb7bbc83`.
- Numeric appendix SHA-256: `4895479e00ace34e3d15fb1870c05951eb407dd89b5859cd373f99a9a1612bc2`; 1,104 data rows. The report copy preserves the original CSV bytes.
- Raw files: **29,827** files verified against receipt path, size and individual SHA-256, with unsafe paths/duplicate/unreceipted members rejected by the maintained KV validator.

Retained local raw archive and receipt:
`checkpoint-work/kv-comparison/cloud-ops/th-e4a783f03101a780/received/`.

Full validation output:
`checkpoint-work/kv-comparison/cloud-ops/th-e4a783f03101a780/integrity-validation/`.
The validator returned **509 completed, 1 failed, 174 not run**. Benchmark and monitoring completion flags remain false. No failed/missing result was rewritten into a success.

## Fleet lifecycle

The fleet used six On-Demand `g6.8xlarge` instances, `us-east-1a`, cluster placement group `th-e4a783f03101a780`. Launch was 2026-10-25 19:42:17 UTC.

- Controller: `i-0a630f46f73684447`.
- Owners: `i-0c12a923f7a1476ef`, `i-083ef163fe7422095`, `i-096eedefbf6c01b9f`, `i-04bb45095b0c9252a`, `i-04a20f02cda4ab46a`.
- Work cutoff: 22:31:35.532052 UTC. Hard expiry: 22:41:35.532052 UTC. Export reserve: 600 seconds.
- Failure: 21:47:29–21:47:37 UTC, before that cutoff.
- S3 raw archive LastModified: 21:51:32 UTC.
- AWS state reason records user-initiated shutdown at 21:51:34 UTC. This timestamp is not an independent observation of terminal state.
- AWS MCP independently observed **all six terminated at 21:58:58.719 UTC**.
- At 21:59:14.966 UTC, exact-run EBS volumes/ENIs/security groups were absent, the placement group was unknown, and all six temporary IAM roles and profiles were absent.
- Expanded reconciliation at 22:01:13.976 UTC also found no run-tagged snapshots, EIPs or ASGs.
- After GitHub completed, final AWS MCP reconciliation at 23:07:23–23:07:57 UTC found no RunId-tagged instances or EBS volumes in all five allowed regions; run-owned network/control resources and all temporary IAM identities were absent. Exact-ID records had aged out; this does not replace the earlier explicit `terminated` state evidence.
- The operator did not manually send termination or launch replacement resources. The workflow/controller lifecycle performed shutdown.

See [MCP recovery evidence](../../../../checkpoint-work/kv-comparison/cloud-ops/th-e4a783f03101a780/reboot-recovery.json) and [final post-GitHub closure/accounting proof](../../../../checkpoint-work/kv-comparison/cloud-ops/th-e4a783f03101a780/final-accounting.json).
Private S3 artifacts remain retained. Closed EC2 does **not** mean zero remaining S3 storage/request charges.

## Why case 0509 stopped the campaign

`commands/007970.json` records `ConnectionClosedError` from `https://ssm.us-east-1.amazonaws.com/` while requesting a storage-2 snapshot with `drop=true`. It has `status=unknown_send_response` and no known command ID. This is the EC2 controller's connection to AWS SSM, not the laptop's Wi-Fi connection.

The client was stopped while no measurement was admitted. Its systemd log reports 1.0-GiB peak memory and 0-B peak swap; there is no admitted summary/samples/metrics for this case. Worker-stop receipts completed successfully. The exact S3 response key for request 007970 was later `NoSuchKey`; absence does not prove nonacceptance or that the cache drop did not execute.

A retry must preserve an operation identity and prevent duplicate destructive gate effects. Blindly retrying `SendCommand` would be unsafe here. The source currently fails closed, which preserved measurement integrity but aborted remaining cases.

## Prometheus and Grafana

- Cloud readiness evidence: Grafana **13.2.4**, database `ok`, dashboard UID `kv-comparison-campaign`; Prometheus **3.13.5**, explicit configuration/session identity retained.
- All **510 selected-case terminal scrape observations** were marked complete; selected and expected case identity sets match. This is terminal telemetry coverage, not 510 valid API measurements.
- Monitoring archive: 23,746,951 bytes; SHA-256 `e3611ff92f032e743ffa529f41adcac1445d3a89b09c6745f3a9843835d25c22`.
- Manifest SHA-256: `87a041e388c6137aacb218ab80ab1c807cda9897f1a826869215a02c2f3b602d`.
- TSDB snapshot declares complete, `skip_head=false`, four restored block files totaling **69,393,876 bytes**. It retains original timestamps, not resampled synthetic data.
- Full capture verification has a `ValueError` gap and **no recorded query hashes**. Actual offline replay reproduced `ValueError: JSON evidence exceeds size bound` against the same captured data and current 16-MiB JSON evidence bound.
- No capture flag or evidence was altered to force a passing replay. The failed replay's exact-owned containers were cleaned up; an actual `docker ps` check found none still running for this RunId.

Thus online service startup/terminal ingestion and snapshot retention are evidenced. **Full query verification, offline Grafana use and exact query-hash parity are not established.** The size-bound failure must be fixed locally before claiming the complete monitoring workflow is ready. No zero-overhead monitoring claim is made.

## Budget and settlement status

The approved editable weekly budget is **$200**, budget-file SHA-256 `f7bf931b70839ad5dd81332fe1d7dd8c0fe9c2d23bb0ebd6d804867beff99845`. The ledger's old top-level $60 snapshot is not the current authority.

The final generated report is **ledger revision 84**, as of **2026-10-25 23:02:09 UTC**. The current run settled at 22:43:04 UTC; `cleanup_verified=true`, `reservation_released=true`, active reservations **$0**, unresolved runs **0**.

| Scope | UTC period | Recorded usage estimate USD |
|---|---|---:|
| This campaign | `th-e4a783f03101a780` | 27.604082335488 |
| Branch total | Since 2026-10-18 | 170.924484930502 |
| Branch monthly | 2026-10-01 to 2026-11-01 | 170.924484930502 |
| Branch weekly | 2026-10-19 to 2026-10-26 | 170.924484930502 |
| Branch daily | 2026-10-25 to 2026-10-26 | 81.715767220611 |

Available weekly headroom is **$29.075515069498** under the $200 budget. The $56.238858715586 admission reservation was released; it is not the spent amount.

These are **partial-coverage resource-based estimates, not an AWS invoice**. Billing remains unavailable because the account cannot list cost-allocation tags. Future retained-current-bytes S3 storage is separately projected at **$0.010784488332** for this run and is excluded from recorded usage. Retained S3 data still incurs storage/request charges.

The [unchanged generated branch report](branch-cost-report.json) preserves all exact periods, coverage limits and run records; SHA-256 `760c5403c564cd03b701845534bd94b3f351ab6220fc88bad71ade6b1fc74348`. It came from GitHub artifact `aws-costs-40226403293-1`, not a manually incremented local counter.

Pricing-backed running estimates used a six-host compute + 32-GiB gp3 roots + in-use IPv4 rate of **$12.137441095896/hour**, excluding S3/ancillary charges. [EC2 pricing](https://aws.amazon.com/ec2/pricing/on-demand/) · [EBS pricing](https://aws.amazon.com/ebs/pricing/) · [IPv4 pricing](https://aws.amazon.com/vpc/pricing/) · [S3 pricing](https://aws.amazon.com/s3/pricing/).

GitHub concluded **failure**: the campaign step failed at 22:45:05 UTC; the separate post-run reconciliation step timed out after 15 minutes at 23:00:17 UTC. Raw-artifact retention succeeded and the accounting job succeeded. The resource closure and released reservation are independently evidenced; the timeout must not be described as a green cleanup step. The operator serially retrieves the full S3 prefix before settlement and repeats retrieval in reconciliation; this is a source-supported explanation for slow post-run processing, not a measured runner stack trace.

Retained GitHub artifacts include `aws-efa-40226403293-1` (849,738,360 bytes), `aws-costs-40226403293-1` (119,805 bytes), and the source-bound preparation package. The full locally verified archive is already retained independently.

## Reproduction and files

The figures were produced offline with the repository's maintained `plot_cloud_comparison.py` against the validated `comparison.json`. The output directory is outside immutable raw evidence. PNG and PDF figures were rendered; the batch32 and cache figures were visually inspected. Figure inputs, repeat identities, excluded observations and counts are retained in `plots.json` and `plot-data.csv`.

No cluster was relaunched after failure and no benchmark branch push was made. The current frozen cloud campaign was not modified. After result analysis, the user requested a future one-repetition development policy; those harness/reporting changes were implemented and validated locally, without changing these measurements or discarding existing repetitions. The report does not certify the full three-repetition campaign or a completed offline monitoring replay.
