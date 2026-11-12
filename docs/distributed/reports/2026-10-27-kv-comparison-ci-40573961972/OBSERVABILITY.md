# Observability: completed KV campaign

This is an analysis of the single synthetic KV campaign `th-6449c7068670440c`, source `87465ef68abb6fa9b7b9cb27224281a7c4994e6d`. It is not Sui checkpoint replay. The results are descriptive of one repetition, not estimates of run-to-run variability. [telemetry-summary.json](telemetry-summary.json) gives machine-readable provenance, selected observations, and explicit unavailable fields.

Throughout, **R** means the retained `checkpoint-work/kv-comparison/cloud-ops/th-6449c7068670440c/github/extracted/aws-efa-40573961972-1/validated-results` directory relative to the checkout root. `R/cases/<case-id>/` contains `resource-interval.json`, `measurement-before.json`, `measurement-end.json`, and `result/samples.csv`. **M** means a member of `R/observability/monitoring.tar.gz`. Raw evidence is preserved, not regenerated.

## What the export establishes

- `R/observability/receipt.json` reports complete; its 2,854 events include six monitor starts, 258 case selections, 1,548 host selections, 258 resource intervals, 258 terminal receipts, and six monitor stops. All events report complete. `M:evidence/capture.json` lists 258 expected and 258 seen case IDs, 258 terminal observations, no capture gaps, and `observer_overhead_calibrated=false`. This establishes the recorder's completeness criteria, **not that every conceivable backend metric was collected or that there were no transient scrape failures**.
- `M:config/prometheus.json` configures the benchmark exporter at 1 s and host and selected-cgroup exporters at 5 s. `M:evidence/verification.json` records 13 targets up at verification: one coordinator benchmark, six host and six cgroup targets, covering coordinator and five storage hosts. This is a point-in-time health observation. Per-case selections are explicit, not a blanket process-discovery guarantee.
- Capture records **459 paged query receipts** across 27 metric families, with 401 unique page files under `M:evidence/query-evidence/`; the page interval is `(start,end]`. The manifest enumerates 416 files with hashes. Query families cover terminal/availability, call throughput and API quantiles, read-path calls, selected cgroup CPU/throttling/I/O, interval CPU/I/O/memory, and write diagnostics. The separate snapshot includes head (`skip_head=false`), with four TSDB block files under `M:snapshot/`. The reported capture range is Unix 1793132516.184–1793137445.205; the snapshot block range starts at 1793132523.095 and ends exclusively at 1793137457.541. Thus the snapshot starts about 6.91 s after the declared capture range starts; do not assert sample coverage for that initial gap. Terminal-case evidence and retained paged queries are independently recorded.
- `M:evidence/ui-readiness.json` records Grafana 13.2.4 database `ok`, Prometheus 3.13.5, dashboard UID `kv-comparison-campaign`, and an initial successful but **empty** query. `M:evidence/verification.json` reports datasource health `OK`, 27 executable panels, a real datasource terminal anchor for case `0257`, and no verification gaps. Its `visual_verification=false`: this is API/query verification, not proof of visual rendering. Several panel queries have zero points: in particular 9 A/B/C, 23 A/B, 24 A/B/C, and 28 A. Do not interpret an empty backend-specific panel as zero activity. The terminal anchor and these panel checks use the selected final Cassandra case, not every metric for every backend.
- Monitor source hashes and pinned images are recorded in `M:evidence/capture.json`, with sources and image lock included under `M:source/`. Effective Prometheus configuration hash and session identity are in `M:evidence/ui-readiness.json`. Per-case receipts bind result artifacts and executable hashes. These pin the observer and workload evidence; the monitoring sources alone do not establish the complete benchmark source revision.

The case directories contain 25 FoundationDB, 25 Valkey TCP, 32 TideHunter EFA native, 30 TideHunter EFA, 25 TiKV, 30 TideHunter EFA direct, 36 native, 30 TideHunter TCP and 25 Cassandra cases. All 258 `resource-interval.json` files contain 1,418 selected role/host cgroup entries, all marked available. That is coverage of the **selected** cgroups; it is not a promise of comparable application, cache, per-process RSS or device metrics for every backend. For example, the Cassandra-anchored WAL panels above are empty. Some backend-specific metrics exist in individual `result/metrics.prom`, including native engine metrics, but a comparable block-cache hit-rate series across nine backends is not established. Missing is not zero.

## Matched resource observations

The following examples are the same `primary_rf1` 1,024-byte `read-concurrent`, repetition 0, not pooled across cases. Consult the individual `resource-interval.json` files for exact values and independently sampled elapsed times. Each row is a selected **client cgroup**, not a process-level CPU reading. CPU seconds divided by that cgroup's elapsed seconds estimates average cores over its boundary interval, not CPU per operation or instantaneous saturation. Peaks are cgroup lifetime high-water marks, **not interval RSS**. The workload ceilings were 8 CPUs / 16 GiB per host; the selected FoundationDB client budget (`cases/0013.../client-budget.json`) is 7 CPUs / 10 GiB, reflecting other colocated roles. Do not assume every role has the same allocation.

| Case prefix / variant | Client CPU s / elapsed s | Client throttled periods | Client cgroup lifetime memory peak MiB |
|---|---:|---:|---:|
| 0013 FoundationDB | 0.663 / 2.33 | 0 | 22.6 |
| 0038 Valkey TCP | 0.511 / 2.09 | 0 | 10.6 |
| 0063 TideHunter EFA native | 0.812 / 2.11 | 1 | 119.3 |
| 0095 TideHunter EFA | 0.841 / 2.07 | 1 | 393.0 |
| 0125 TiKV | 0.760 / 2.33 | 0 | 12.1 |
| 0150 TideHunter EFA direct | 0.845 / 2.14 | 1 | 119.2 |
| 0180 native | 0.180 / 2.10 | 0 | 23.6 |
| 0216 TideHunter TCP | 1.410 / 2.14 | 1 | 32.8 |
| 0246 Cassandra | 0.548 / 2.14 | 0 | 11.6 |

For `0063-tidehunter_efa_native-1024-read-concurrent-primary_rf1-r0`, storage-0 used 8.694 CPU seconds over 2.17 seconds, approximately 4.01 average cores; its selected cgroup had zero throttled periods during that interval. All five EFA-native storage cgroups recorded 8.495–8.694 CPU seconds. For matched `0216-tidehunter_tcp-1024-read-concurrent-primary_rf1-r0`, storage cgroups recorded 0.132–0.159 CPU seconds over approximately 2.05–2.18 seconds. This is an observed CPU-use contrast, consistent with different execution behavior, **not proof that EFA transport or polling caused a latency difference**. Different contracts and execution paths apply. The client in `0063` recorded 1 throttled period out of 14, totaling 0.005657 s; in `0216`, 1 of 14 totaling 0.0625 s. Throttling occurred, but these counters cannot quantify its effect on a particular request or prove that quota was the dominant bottleneck.

Raw successful measurement-read scheduling samples in `result/samples.csv`, nearest rank without histogram rounding, give the following. All have 8,192 samples per case; p99.9 is supported by only approximately eight observations above that rank, not an independent-run error bar. Scheduling is distinct from backend API and response latency.

| Case | p50 scheduling µs | p99 µs | p99.9 µs |
|---|---:|---:|---:|
| 0013 FoundationDB | 12.641 | 31.391 | 55.381 |
| 0063 EFA native | 13.600 | 29.290 | 65.121 |
| 0180 native | 10.140 | 25.130 | 55.971 |
| 0216 TCP | 14.740 | 37.211 | 119.613 |

These are per-call scheduling delays. Batch32 elsewhere reports complete-call latency, not per-key latency; call throughput and logical-key throughput must remain separate. Native is embedded, a lower-bound reference rather than a five-host scale-equivalent peer. Valkey TCP is RF1, AOF `appendfsync always`, RAM-resident GET, not a value-WAL or disk-GET, RDMA, Sui adapter, HA, or cross-slot atomic-batch benchmark. The other backends also have different durability, security and consistency contracts.

## Native application-level evidence

The retained terminal `R/cases/0180-native-1024-read-concurrent-primary_rf1-r0/result/metrics.prom` reports `lookup_result{ks="root",result="found",source="cache"}=8720` and the corresponding `not_found` value 12. It also reports 8,720 `read{kind="record",type="mapped"}` and 9,230,912 `read_bytes` for that label, versus zero record syscall reads; `wal_written_bytes=4,394,928`. These are cumulative terminal engine metrics, **not measurement-window deltas**, and include earlier phases. Mapped logical reads do not establish physical disk I/O or a value-cache hit rate.

The same export reports `memory_estimate{ks="root",kind="index_cache"}=1,572,864` bytes. The current checkout's `tidehunter/src/db.rs::report_memory_estimates` computes this gauge from configured key size, cell count and maximum dirty keys; it is a configuration-derived estimate, not measured RSS or cache occupancy. Current `large_table.rs` associates `source="cache"` with an index overlay lookup, distinct from its value LRU path, and `db.rs` sets `wal_written_bytes` from the WAL position offset. These implementation references are interpretation aids from the current checkout, not an independent verification of the campaign's exact source bytes. None of these observations constitutes a comparable nine-backend cache hit rate or a physical write-volume measurement.

## Disk, network, memory and interpretation

For `0038`, `0063`, and `0216`, storage-host `measurement-before.json` and `measurement-end.json` `diskstats` counters for `/dev/nvme1n1` show **zero read sectors on each of the five hosts** during the respective boundary intervals. Converting sectors at 512 bytes, `0038` storage-4 recorded 12,288 write bytes and the other four recorded zero; all five recorded zero writes for `0063` and `0216`. These are whole-device, whole-host deltas, not per-request or per-process attribution; boundary snapshots on separate hosts are not simultaneous. The same intervals' selected cgroup `io_read_bytes` are also zero, which is a different accounting scope. Application-level logical bytes, cgroup I/O and physical-device sectors cannot be equated. Case bootstrap device identities and mounts are retained in snapshots and `R/bootstrap-proofs.json`.

A dropped OS page cache does **not** prove every subsequent GET accessed physical NVMe: application/block caches can still satisfy requests, and Valkey GETs are RAM-backed. The zero-read examples are warm `primary_rf1` cases, not the `cold_os_cache` cohort; they cannot establish cold-read behavior for it. Page-cache operations are reported separately in the snapshots, including `application_caches_evicted`.

### Cold 65,536-byte cases: actual boundary read counters

For each full case ID below, source is `R/cases/<case-id>/measurement-before.json` and `measurement-end.json`, host `diskstats` for `/dev/nvme1n1`; subtraction is end minus before. Vectors are coordinator, storage-0 through storage-4, in that order. Linux diskstats read sectors are 512-byte units: the exact byte vectors are sector vectors multiplied by 512, also retained explicitly in `telemetry-summary.json`. The selected-role `resource-interval.json` `io_read_bytes` vectors are client then storage roles 0–4, except native has only the client. FoundationDB's separate coordinator `fdb-control` recorded 0 and TiKV's coordinator `pd` 159,744 bytes; these are not included in the client value.

| Case ID | Host device read sectors: coordinator, storage 0–4 | Selected cgroup read bytes: client, storage 0–4 |
|---|---|---|
| `0024-foundationdb-65536-cold-os-large-cold_os_cache-r0` | 2472, 1224, 1224, 1224, 1208, 1208 | 1081344, 0, 0, 4096, 0, 0 |
| `0049-valkey_tcp-65536-cold-os-large-cold_os_cache-r0` | 384, 1472, 1424, 1208, 1208, 1208 | 4096, 131072, 106496, 0, 0, 0 |
| `0081-tidehunter_efa_native-65536-cold-os-large-cold_os_cache-r0` | 19328, 140840, 134032, 148568, 143312, 142792 | 9797632, 71503872, 68018176, 75460608, 72769536, 72503296 |
| `0111-tidehunter_efa-65536-cold-os-large-cold_os_cache-r0` | 20816, 142504, 140432, 142168, 143480, 141440 | 10596352, 72355840, 71294976, 72183808, 72855552, 71811072 |
| `0136-tikv-65536-cold-os-large-cold_os_cache-r0` | 720, 98960, 106704, 943280, 118032, 822952 | 4096, 50049024, 54013952, 482349056, 59813888, 420732928 |
| `0166-tidehunter_efa_direct-65536-cold-os-large-cold_os_cache-r0` | 19064, 134488, 138424, 148920, 139416, 147912 | 9691136, 68251648, 70266880, 75640832, 70774784, 75124736 |
| `0202-native-65536-cold-os-large-cold_os_cache-r0` | 546048, 1216, 1216, 1216, 1216, 1216 | 279474176, unavailable, unavailable, unavailable, unavailable, unavailable |
| `0232-tidehunter_tcp-65536-cold-os-large-cold_os_cache-r0` | 19776, 138848, 141312, 143120, 140808, 144952 | 10027008, 70483968, 71745536, 72671232, 71483392, 73605120 |
| `0257-cassandra-65536-cold-os-large-cold_os_cache-r0` | 456, 174616, 186920, 304480, 314880, 301968 | 4096, 89096192, 95088640, 155275264, 160595968, 153985024 |

For example, native `0202`'s coordinator delta is 546,048 sectors = 279,576,576 device bytes; `0081` storage-0 has 140,840 sectors = 72,110,080 device bytes, with 71,503,872 selected storage-cgroup read bytes. `0202` has no storage-role cgroups: the five host counters still include host/background activity and must not be interpreted as native owner I/O. All nine before snapshots record `drop_os_caches=true` and `application_caches_evicted=false` on all six hosts. These host-wide device deltas and selected-cgroup accounting are independently sampled and have different scopes; neither attributes every 4 KiB request or individual GET to physical NVMe. OS cache drop did not evict application caches, and Valkey GET remains RAM-backed even when its host or server cgroup records some disk reads. Observer/background I/O may contribute; no causal ranking follows from these counters.

Host-level node-exporter panels include CPU, memory/pressure, diskstats, and network. In online verification the final Cassandra-anchored network receive and transmit queries each returned 192 points; the expressions use 20-second `rate(node_network_*_bytes_total[20s])`, non-loopback devices, and host labels. This demonstrates queryable host-network telemetry, **not per-case network bytes or isolated backend traffic**, especially for cases lasting only a few seconds. Pressure queries also returned points, but their host-wide 20-second windows and cohost activity make them unsuitable for an isolated case attribution without a time-aligned examination of the retained series. No extracted per-case network/pressure estimate is presented here. Likewise, cgroup `memory.current` and lifetime `memory.peak` include cgroup-accounted memory, not process RSS or a backend cache hit rate.

Prometheus, Grafana and exporters were cohosted with the workload. Their overhead was **not calibrated**; host CPU, pressure, disk and network include unrelated/background and observer activity. These observations identify possible investigative leads, not causal bottlenecks, and do not establish zero interference, steady state, hardware microsecond latency, or device durability.

## Offline analysis and replay

The archive already contains the original TSDB blocks, paged query responses, dashboard/provisioning configuration, source, image lock and original timestamps. Inspect the JSON members and per-case CSVs without running any service. For optional offline interactive replay on a suitably provisioned machine, the existing `experiments/kv-comparison/observability/README.md` documents the following procedure from checkout root. It starts local containers, not a new database or live scraper; pinned images must already be cached. Use a fresh state leaf and explicitly stop the owned replay when finished:

```bash
OBS=experiments/kv-comparison/observability
ARCHIVE=checkpoint-work/kv-comparison/cloud-ops/th-6449c7068670440c/github/extracted/aws-efa-40573961972-1/validated-results/observability/monitoring.tar.gz
REPLAY_STATE="$(mktemp -d /tmp/tidehunter-monitor-replay.XXXXXX)/state"
python3 "$OBS/campaign_monitor.py" replay --archive "$ARCHIVE" --state-dir "$REPLAY_STATE"
python3 "$OBS/campaign_monitor.py" --spec "$REPLAY_STATE/spec.json" access
python3 "$OBS/campaign_monitor.py" --spec "$REPLAY_STATE/spec.json" verify
python3 "$OBS/campaign_monitor.py" --spec "$REPLAY_STATE/spec.json" stop
```

Replay restores the original time range and no live scrape jobs; `access` supplies the local URL and time range. The documented command is a method, **not an assertion that this campaign's offline replay or visual inspection was exercised**. Preserve missing historical observations rather than backfilling zeros or current timestamps.
