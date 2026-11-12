# Superseded in part

The supervisor brief `docs/research/supervisor-brief-2026-11-10/main.pdf` replaces this brief's remote-NVMe interpretation and bottleneck ranking.

| Section or result in the 7 November brief | Status in the 10 November brief |
|---|---|
| Scoreboard, two 1 KiB comparison tables, other-stores figure/table | Carried forward unchanged, labelled "full-matrix fleet, 6 to 7 November, unchanged". |
| RDMA-against-TCP coherence figure and cache hit-ratio table | Carried forward unchanged, using the same CSV inputs and number language. |
| Remote NVMe reads | Replaced by focused run 7's device-qualified random reads and run 8's profiled components and floors. Ascending key-sweep times are not random-access SSD-served latency. |
| Bottlenecks and next work | Replaced by run 8's exact timeline intervals, separately sampled profiles, owner-stage extracts and the hardware-parity-first ranking. |
| Device-write per-operation latency interpretation | Withdrawn for native-versus-owner comparisons. Gate-window counters include background writeback. Compare the same in-process sync step instead. |
| Other figures and historical measurements | Retained here as historical evidence; not silently updated or transferred to a new fleet. |

The new brief compares sync-step means with means and shows owner p50/p99 separately. Native has no sync-step quantiles. It distinguishes owner append-persist software from the system call, the overwrite floor from WAL appends, and native mapped cold-read windows from exact-range owner reads. G13W append floors remain unrun and need launch authorization.

Evidence: run 7 fleet `bench-6e40ca83d74146cf` and run 8 fleet `bench-0a8a6af114a64287`, 9 November 2026. The new brief's `inputs.json`, `evidence.json` and `validation.json` record source hashes, case ids, values and checks. The seven CSV inputs of this older brief remain unchanged.
