# Original consolidated audit: contracts, stage evidence, and causal limits

Read-only findings from the completed audit; **no broad research rerun**.

**Notation:** repository-relative paths.  
`R` = `checkpoint-work/kv-comparison/cloud-ops/th-7dea3c080cb44d48/received/`  
`A` = sibling `ANALYSIS_TABLES.md`  
`B` = `benchmarks/kv-research/src/`  
`D` = `tidehunter/src/distributed/`

## Summary

- The artifacts support **adapter-strategy, acknowledgement, locality, admission/scheduling and long-tail observations**.
- They **do not support a measured routing/index/persistence/publication/TLS/provider/device-I/O latency decomposition**.
- Every **236 completed r0/r1 distributed case** with counter evidence has **`write_profile_limit=0`**.
- Distributed read evidence contains **lifetime counters and passive owner snapshots, not measured-stage latency histograms**.
- Native genuinely exports internal histograms, but they are **adapter-lifetime, not measurement-phase distributions**.
- Mechanisms visible in source—WAL persistence, ordered publication, checkpoint admission, transport progress—are **possible contributors**, not measured causes of the observed stalls.

## Frozen identity

From `A:12`, also `R/bootstrap-proofs.json[0].ready`:

- Base revision: `b70d83b7a02fc2de79ce1782898b68fb2535d924`, **plus recorded dirty source snapshot**.
- Source ZIP SHA-256: `ca25db97b64b9906100b0453ed3d383d293b6f2835a849ba0e6135b3d49ed713`.
- Executable SHA-256: `749372b1126b614bb27560649443b99483b5abea36848c1fc5e1711f53d02cef`.
- Current corrected `cloud_layout.py` memory settings **must not replace actual AWS allocation evidence**.

---

## 1. Actual versions and contracts: all eight systems

Representative manifest citations:

`R/cases/<case>/result/manifest.json#/backend_info/{server_version,client_version,acknowledgement,consistency,get_many_strategy,measurement_boundary,configuration}`

| System | Representative 8-KiB primary r0 many32 case | Actual version / client |
|---|---|---|
| Native | `0143-native-8192-read-many32-primary_rf1-r0` | Tidehunter `0.1.0`; adapter `0.1.0`; frozen source above |
| TCP | `0060-tidehunter_tcp-8192-read-many32-primary_rf1-r0` | Same Tidehunter/adapter |
| EFA | `0031-tidehunter_efa-8192-read-many32-primary_rf1-r0` | Same |
| EFA-direct | `0089-tidehunter_efa_direct-8192-read-many32-primary_rf1-r0` | Same |
| EFA-native | `0000-tidehunter_efa_native-8192-read-many32-primary_rf1-r0` | Same |
| TiKV | `0118-tikv-8192-read-many32-primary_rf1-r0` | **TiKV 8.5.4; PD v8.5.4; tikv-client 0.4.0** |
| Cassandra | `0203-cassandra-8192-read-many32-primary_rf1-r0` | **5.0.9; Scylla Rust driver 1.9.0** |
| FDB | `0178-foundationdb-8192-read-many32-primary_rf1-r0` | **7.3.77; Rust bindings 0.11.0; loaded C API maximum 730** |

The same contracts are copied into `R/comparison.json` under `.cases[].backend`. Backend ID is `.cases[].job.backend`; `.backend` is a contract object.

### Native

- Primary mutations perform native foreground mutation **then** `persist_value_wal_for_benchmark`.
- Barrier synchronizes **all currently registered Value-WAL files and their directory**, including clean older files. It is not an index snapshot/publication barrier or device power-loss certification.
- Scalar read: `Db::get`.
- Many32: **32 ordered scalar gets**, retaining returned `Bytes` without copying.
- Batch label: **crash recovery**, not serializable visibility/isolation.
- Security: in-process.
- Actual allocation: **4 CPU / 8 GiB**, engine and harness together.

Sources:

- `B/backends/native.rs:72-99,102-127,141-178`.
- `tidehunter/src/db.rs:455-482,1269-1280`.

### Four distributed Tidehunter adapters

Common contract:

- **RF1 complete Value-WAL batch persisted on configured owner; no replication**.
- Single coordinator `NativeIndex`; complete-batch owner allocation.
- Acknowledgement incorporates durable receipt and coordinator publication progression.
- Many32: **ordered partition-grouped reads**, one result per caller key.
- No serializable multi-key transaction/failover claim.
- Primary coordinator and owner explicit read caches are off; **Linux page-cache residency remains possible**.
- Allocation: coordinator/client **4 CPU / 8 GiB**, five owners each **4 CPU / 8 GiB**.
- Security: TCP, EFA and EFA-direct **mTLS**; EFA-native **trusted plaintext/no TLS**.

Sources:

- `B/backends/tidehunter.rs:565-608,743-807`.
- `D/sui.rs:1254-1290`.
- `D/remote_db/pending_read/concurrent.rs:89-169`.
- `D/remote_db/concurrent.rs:644-657`: replies after published prefix.
- `D/remote_db.rs:2587-2658`: ordered publication/index promotion.

Transport identities:

- `D/tcp/fabric.rs:219-224,267-280`: EFA/direct/native modes; direct/native open the direct endpoint.
- `R/bootstrap-proofs.json[0].fi_info`: actual EFA/direct provider admission, ABI **206.0**, `FI_PROTO_EFA`.
- `.ldd`: `/opt/amazon/efa/lib/libfabric.so.1`.
- Provider admission is **not evidence that ordinary primary EFA calls were one-sided RMA**.

### TiKV

- RawKV request success; server acknowledgement policy/device fsync **not independently verified by adapter**.
- Single-key semantics; batch commands non-atomic.
- Many32 uses **native RawKV `batch_get`**, restoring missing keys as `None` and caller order.
- Atomic `put_batch` unsupported.
- Plaintext client.
- Actual client **3 CPU / 7 GiB**, colocated PD **1 CPU / 1 GiB**; five TiKV servers each 4 CPU/8 GiB.
- Supplemental audit adds actual effective Titan/compression configuration; retain those facts alongside conservative manifest wording.

Sources: `B/backends/tikv.rs:198-216,227-279`.

### Cassandra

- QUORUM configured per prepared read/write/delete and logged batch, **RF1**.
- Observed `durable_writes=true`.
- Many32 = prepared per-key query fanout **bounded at 16**; no native CQL batch GET.
- Logged batch is not a serializable transaction.
- Plaintext, authentication disabled.
- Five 4-CPU/8-GiB servers; client 4 CPU/8 GiB.
- Retained deployment YAML explicitly configures **`commitlog_sync: batch`**; distinguish this from adapter wording “commitlog fsync policy not observed.”

Sources:

- `B/backends/cassandra.rs:356-455`.
- `R/deployments/cassandra-stop-3540/host-0-export.json#/evidence/config/files/cassandra.yaml`.

### FoundationDB

- Retryable **serializable transaction per API operation**.
- Many32 = **32 sequential `transaction.get(key, false).await` calls inside one transaction**.
- Mutations acknowledged through successful transaction commit; physical-device persistence not independently verified.
- Effective TLS/authentication unverified by adapter.
- Actual AWS client **3 CPU / 6 GiB**, colocated control **1 CPU / 2 GiB**.
- Control command line: **`--memory 1536MiB --cache-memory 512MiB`**.
- Five storage servers: each 4 CPU/8 GiB container, **`--memory 6144MiB --cache-memory 2048MiB`**.

Sources: `B/backends/foundationdb.rs:308-317,376-440`; `R/deployments/foundationdb-3010/proofs.json[].proof.runtime`.

**RF1 does not equalize durability, transaction, HA or security contracts.**

---

## 2. What stage-timing evidence actually exists

### Authoritative retained clocks

Each `result/samples.csv` contains:

`phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind`

Harness Prometheus families:

- `kv_comparison_api_latency_seconds`
- `kv_comparison_response_latency_seconds`
- `kv_comparison_scheduling_delay_seconds`

These are labeled by backend, operation, outcome, phase.

Boundary sources:

- `B/runner.rs:1116-1145`: keys and **full expected values** generated before API timing.
- `B/runner.rs:1172-1207`: API dispatch → materialized result.
- `B/runner.rs:1216-1269`: value verification after API clock stops.
- `B/report.rs:288-299`: three harness histograms.
- `experiments/kv-comparison/cloud_campaign.py:172-188`: throughput exposure is last completion minus first scheduled timestamp, not sum of API durations.

### Evidence matrix

| Stage/component | Actual artifact support | Attribution limit |
|---|---|---|
| API | Per-call samples and phase/outcome histograms | Inclusive adapter API, not isolated engine stages |
| Arrival scheduling | Per-call dispatch minus scheduled time | Includes expected-value generation and worker delay; **not isolated engine queueing** |
| Response | Scheduled arrival through materialized result | Successful population conditional on acceptance |
| Routing/index | Source path; coordinator cache hits/misses and RPC counts | No retained measurement-phase routing/index latency histogram |
| Owner queue/read execution | Lifetime executor counts, inflight peaks, WAL reads, async submissions | No measurement-only queue/service latency distribution |
| WAL persistence | Source persistence stages and optional profiler | **Disabled in campaign**; no measured persistence histogram |
| Publication/checkpoint | Source timings/diagnostic structures | Not retained as measured-phase stage samples |
| TLS/framing/provider | Source nested timing structures; actual provider admission | No isolated TLS/provider timing or percentage |
| Device I/O | Host diskstats; some client process/cgroup accounting | Broader interval; no per-call physical-I/O latency attribution |
| Native internal stages | Actual lifetime engine histograms | Not phase-separated; do not isolate added durability barrier |
| External engine stages | Startup/status/effective configuration and end-of-block exports | FDB status latency probes are not benchmark-call stage samples |

### Concrete disabled-profile evidence

All **236 completed r0/r1 distributed cases** with counter evidence:

`R/comparison.json#/cases/*/counter_evidence/observed/effective_configuration/client/write_profile_limit = 0`

Recursive counter-key inspection found no retained stage timing values beyond that configuration field. Filename searches found no profile/timing/histogram export.

Sources:

- `D/remote_db.rs:425-427,456-458`: zero disables bounded append profiling.
- `D/remote_db.rs:1559-1574`: profile limit passed to workers.
- `D/remote_db.rs:1603-1604`: checkpoint diagnostics enabled only when write profile limit is nonzero.
- `B/backends/tidehunter.rs:445-449`: distributed adapter accepts **unused `_registry`**.
- `D/remote_db/pending_read.rs:954-968`: read diagnostics start at default state.
- No adapter call enables/export-read-stage profiling.

**Do not claim engine stage timings were measured merely because profile structs exist.**

### Native histograms genuinely exist, but are cumulative

`R/cases/0154-native-1024-write-concurrent-primary_rf1-r0/result/metrics.prom` contains internal families including:

- `db_op`
- `lookup_mcs`
- `map_time_mcs`
- `large_table_contention`
- `snapshot_lock_time_mcs`
- other native counters/histograms.

Concrete example: `db_op_count{op="insert"} = 8710`, while **4096 puts** belong to the timed phase. The count includes preload, warmup and qualification. Native `db_op` timing also does not isolate the subsequent adapter-added persistence barrier.

Some histogram families have zero observations; their presence is not proof the stage ran.

### Counter scopes and semantics

`B/backends/tidehunter.rs:629-717` writes read evidence:

- `counter_scope`: **“cumulative from adapter open through post-workload qualification; owner IO fields come from the latest passive sample”**
- Coordinator: cache counters and `read_rpc_count`.
- Owner snapshot:
  - `available`, `resident_available`, `generation`, `failed_samples`,
  - `post_workload_sample`, `last_sample_age_ms`,
  - `owner_read_executor_stats`,
  - `resident_read_stats`.

Explicit semantics:

- **`wal_reads`** = logical owner Value-WAL member reads, **not device IOPS**.
- **`async_read_submissions`** = `io_uring` submissions, **not confirmed physical reads**.
- **`resident_read_successes`** = successful NOWAIT prefix reads.
- **`rma_hits`** = completed client-side RDMA reads.

`D/remote_db.rs:1124-1140` explicitly says actor/TCP stages are nested and must not be summed with dispatch/wait as a non-overlapping breakdown. This would remain relevant even if profiles had been enabled.

---

## 3. Write-tail evidence and defensible interpretation

At 1 KiB/c16, matched r0/r1:

| Backend | API p50 µs | API p99 µs | Successful calls/s |
|---|---:|---:|---:|
| TCP | 1325.220 | 141248.518 | 3800.96 |
| EFA | 959.332 | 165474.718 | 3549.03 |
| EFA-direct | 971.532 | 166977.465 | 3535.86 |
| EFA-native | 961.921 | 171267.880 | 3526.38 |

Concrete case `0011-tidehunter_efa_native-1024-write-concurrent-primary_rf1-r0`:

- Mean API **4467.657 µs**
- Median **979.087 µs**
- p99 **173836.764 µs**
- Maximum **176383.684 µs**
- Goodput **3529.35 calls/s**
- Measured API histogram: **80 of 4096 writes exceed 100 ms**, all ≤250 ms.

Citations: `R/comparison.json` selected case `.metrics`; case `result/metrics.prom:11-28`.

Source mechanisms inside the acknowledgement boundary:

- Actual file/namespace persistence: `D/storage.rs:1253-1304`.
- Receipt collection and grouping: `D/remote_db.rs:2447-2496`.
- Ordered publication/index promotion: `D/remote_db.rs:2587-2658`.
- Reply after published prefix: `D/remote_db/concurrent.rs:644-657`.
- Checkpoint hard-tail limits: **1024 batches / 64 MiB**, `D/remote_db.rs:381-382`.
- Proactive checkpoint due at half-tail: `D/remote_db/checkpoint_lifecycle.rs:40-49`.

**Recommended explanation**

> Distributed writes exhibit a small median but a pronounced long tail in both repetitions. Their acknowledgement boundary includes owner WAL persistence and coordinator publication, and ordered publication can hold later completions behind an earlier unresolved operation. **[INFERENCE]** Shared ordering and long in-flight calls can reduce delivered rate despite a low median. **Unknown:** no captured measurement-phase profile isolates persistence, checkpoint, publication, transport-progress or scheduling contributions.

Do **not** attribute the stalls to the 100-ms `recv_timeout`: it is notification-backed safety waiting, not proven stall causation (`D/remote_db/concurrent.rs:756-764`). Do not assert checkpointing caused the stalls merely from thresholds.

---

## 4. Other central observations and paragraph-ready mechanisms

### EFA/TCP reversals

- 1-KiB scalar read p50: TCP **192.695**, EFA **97.253**, EFA-native **88.612 µs**.
- 1-KiB many32 p99: TCP **2540.776**, EFA **5584.499**, direct **5639.293**, native **5877.030 µs**.
- 64-KiB scalar read p50: ordinary EFA **179.959**, EFA-native **206.692**, direct **231.839 µs**.

**Explain as workload/payload-dependent adapter+transport results; isolated reason unknown.** EFA-native vs direct is not a TLS-only experiment because native mode changes the data plane. Never subtract quantiles to estimate encryption cost.

### Offered50k

- c32, queue capacity128; 70% read/30% put; **400000 scheduled arrivals per repeat**.
- TCP delivered **10894.58 successful calls/s**, rejected **310446 / 312544** arrivals.
- Cassandra **46854.12 calls/s**, rejected **12492 / 37575**.
- EFA successful read API p99 **4775.262 µs**, response p99 **213501.829**, scheduling p99 **213275.358 µs**.

Sources: `A:355-362,396-411`; `B/runner.rs:968-1037`.

Accepted latency alone hides rejected service. Rejections have no timed API dispatch; never plot their encoded zeros as API latency.

### Coordinator/owner cache populations

All 16 coordinator-cache cases:

- **9232 lifetime coordinator hits, zero read RPCs**
- zero owner WAL reads
- ~0.7–0.95-µs medians reflect **local cache path**, not remote reads.

Owner-cache cases:

- **9226 RPCs**, zero coordinator hits
- representative r0 **5570 owner hits, 3658 misses/WAL reads**

Citations: cases `0025,0026,0028,0029`, `read-evidence/read-evidence.json`; `A:471-511`; `D/remote_db/pending_read/concurrent.rs:105-139,214-238`.

Do not turn lifetime counts into timed-phase hit fractions. Cache cohorts are c1, not primary c16 controls.

### Registered RAM population

Cases `0024,0027,0289,0292`:

- RMA hits: **2429 / 2440 / 2448 / 2467**
- Missing-grant fallbacks/message requests: **6797 / 6786 / 6778 / 6759**
- **Zero RMA errors**
- **2560 grant acquisitions** each.

Representative `0024`: **6799 successful NOWAIT reads, zero async submissions** on owner message-fallback path.

Citations: `.owner_io.{resident_read_stats,owner_read_executor_stats}`; `D/tcp/reactor.rs:2407-2474`; `D/tcp/read_executor.rs:805-894`.

`require_device_read=true` means **native RMA read capability**, not NVMe media access (`reactor.rs:2421-2425`). Mixed cohort latency cannot be reported as isolated one-sided RMA latency.

### OS-cold scope

- 8-GiB total values, zero warmup, 4096 reads.
- Successful OS cache-drop receipts; **engine caches not evicted**.
- Cold EFA-native r0 `0030`: storage-0 device completed-read counter **568 → 5716**.
- That difference spans gate-to-process-exit **including checks/drain/background and metadata work**. It does not establish per-call device I/O.

Sources: `0030/measurement-{before,after}.json#/hosts[].diskstats`; `cloud_controller.py:320-328,882-904`.

### FDB known preload OOM

All three cold attempts:

- `ERROR: Out of memory`
- `ExitCode=20`, `Running=false`, `Status=exited`
- `OOMKilled=false`
- ~5.5–7.1 s after cold-client startup.

AWS allocation: control **2 GiB**, fdbserver self-limit **1536 MiB**, client **6 GiB**. r0/r1 later record **1905 preload timeout/unknown outcomes**, no admitted measurement calls.

Citations: `R/deployments/foundationdb-stop-{3255,5645,7360}/host--1-export.json#/evidence/containers[]/{state,logs}`; case `0202/client.log:1-6`; `A:134-137`.

**Cause established:** process-level OOM/service loss during preload. **Unknown:** exact internal failing allocation. Local corrected6/2-GiB proof is not an AWS rerun or replacement performance result.

---

## 5. Reporting flaws / missing controls worth stating

1. **Different physical aging:** external service persists for backend/repeat block (`cloud_controller.py:1048-1058`); namespace repeat-scoped (`:852-854`); native/distributed Tidehunter opens case-scoped storage. Full preload matches logical contents, not compaction/cache/physical history.
2. **Native barrier asymmetry:** all registered WAL files+directory versus distributed exact-frame-file barrier. Whole difference is not “network overhead.”
3. **No measured-stage profiling:** source timing structures do not establish collected evidence.
4. **After-exit cgroup loss:** e.g. `0011/measurement-after.json#/client` has null cgroup/process I/O; no trustworthy interval CPU-throttling attribution.
5. **Normal reads allow `None`:** verification accepts absent values in mixed workloads (`runner.rs:1216-1227,1243-1250`). Not a linearizability/existence oracle. However all160 matched pure read/many32 cases’ reported value throughput agrees with successful keys × configured size: no aggregate missing-value byte shortfall there.
6. **N=2, sparse tails:** medians of per-repeat statistics, not pooled quantiles/CIs. Never include FDB r2 in matched comparisons.
7. **Do not claim native always best:** preserve 64-KiB many32 native p99/goodput reversal from supplement.
8. **Do not treat 8-GiB cold as exceeding aggregate engine RAM:** see supplemental exact resource/cache accounting.
9. **All comparison claims include adapter semantics/security/resource placement**, not isolated optimally tuned engines.