# th-38c2508272a57587 — read/write stage, source and residency audit

## Scope, exact-source binding and measurement rules

Completed GitHub **41109588049**, `bench/aws-efa` engine **d34022d18aed2e1b7b029d595f43992868453b28**. Results root **R** = `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/aws-ci-runs/2026-10-30-owner-authority-campaign/extracted-th-38c2508272a57587`; prior cloud root **B** = `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/aws-ci-runs/2026-10-30-read-tier-publication/controller-partial`. Source paths below are relative to `/home/hybr1d/Desktop/PDM/TideHunter/tidehunter-sharded-wal`.

**Provenance upgraded:** parent-saved `fresh-d340-input/independent-full-source-verification.json` identifies workflow 41109588049/source commit d340 and reports **497 regular frozen source files compared, zero mismatches** against the current checkout. Thus the native/distributed/config/runner source line mappings in this audit are fully bound to the frozen run content. Local HEAD remains `44a7effd2a4dea5589f9015b7c29dca39cd5d9b4`; d340 is absent from that local object database, but this does **not** limit the byte-matched source mappings. Earlier `independent-verification.json` binds clean source metadata, source SHA-256 `fc0ff1834f358501558e131548d88eff58eec6b2172f418fb129c6db4037245f`, binary SHA-256 `4251a024b5f3b0f40855af16287e67c8866470c629652be6042191411f1c7b49`. Parent-supplied sealed archive/inventory proof is reused, not recomputed.

Tables consume maintained reporter output (`comparison.json`), per-case `result/read-diagnostics.json`, `result/write-diagnostics.json`, normalized manifests and measurement boundary evidence; no replacement analysis/plot implementation was introduced. **196/196 cases completed**, all 196 have released/observed completion gates and attempted/drained read-evidence boundaries; `monitoring_complete=true`. Six c6id.32xlarge, eu-west-1c, mandatory cluster placement; controller and five distinct-local-NVMe owners. All nine backends appear. RF1 does not equate their acknowledgment, atomicity or recovery guarantees. Parent owns fleet/cost/monitor replay/final shared-report integration.

- **One independent repetition.** No run-to-run error bars, significance, confidence interval or repeatability claim. Before/after cloud hardware and region differ (prior G6/us-east-1): no causal total-speed attribution.
- API quantiles are nearest-rank `ceil(p*n)-1`, conditional on operation/outcome/timed population (`experiments/kv-comparison/analyze.py:18-22,25-89`). The 8,192-call reads, 2,048-call batches and 4,096-call writes carry descriptive p99 (<10,000) and p999 (<100,000) warnings.
- Stage means = sum/count for that stage's population. Clocks overlap/nest; do not sum means/percentiles or call exchange-minus-handler pure network/device time. Unavailable maxima/stages are **null**, not zero.
- Engine read boundaries: after warmup/aging/gate before measurement `run_phase`; end after measured work/worker drain, before completion gate/profile finalization/postqualification. Sequential component/owner snapshots are not globally atomic. Resource `measurement-before/end/after` windows are wider: quiescent gate release through process exit including checks/drain. Lifetime peaks/maxima cannot be differenced into measurement maxima.

## Principal findings

1. **Warm-WAL authority churn is gone in the measured boundaries:** all 32 warm-WAL cases have zero history-load delta; all 16 C1 cases qualify, totaling 131,072 message/WAL/NOWAIT-success members, no explicit value-cache hits, zero async submissions. This is metadata-residency evidence, not zero physical device activity.
2. **Primary C16 scalar reads still incur measured first-use authority loading:** EFA-native 0063 records 1,937 and TCP 0216 records 1,936 history-load attempts during measurement, despite capacity 8,192 and only roughly 440–455 cached entries per owner at end. Warm controls also change lanes, resident-first policy, key count and warmup; the total latency gap cannot be assigned to metadata alone.
3. **Spill adoption does not populate the authority LRU:** the persisted pending prefix is drained and captured Arcs dropped; a later uncached/nonpending read must regain residency via immutable history lookup. Bounded carry-over of validated descriptors is a concrete optimization to evaluate, not a new architecture or a license to weaken authority/durability.
4. **8 KiB ×32 C16 regressions/tails remain:** all 32 owner-hit cases qualify, but millisecond means and substantial tails persist. All four unprofiled owner-hit batch throughputs are lower than the prior cloud; hardware/region confound causality. The all-resolved fast return exists, but its isolated benefit is not established.
5. **Registered headroom changes purity:** eight headroom C1 cases each have 8,192 posts/completions and no fallback/message/WAL/errors. Ordinary C1 cases each retain 46 missing-grant message/WAL fallbacks; C16 registered cases remain mixed. FI_READ 46.5 µs is post→locally observed completion/progress, not isolated NIC/wire.
6. **Actual blocks, not WAL labels:** each distributed cold case has 6,998 matched device requests in wider windows, not 8,192 per-API device samples; all eight case-level cold qualifications fail measurement-window attribution. Native nominal cold controls record one/zero matched block reads.
7. **Write grouping/durability already exists:** validation, persistence, queues and local retirement are measured, but exact group distributions and publication→caller timing are absent. No transport/group-commit rewrite is justified by residuals.

## 1. Warm-WAL authority before/after

Same ID in B and R: `0157-tidehunter_efa_direct-256-wal_warm-profiled-r0`; `read/success`, 8,192 API samples; independently selected 256 remote flights/owner profiles.

| Observation | B: G6/us-east-1 | R: c6id/eu-west-1 |
|---|---:|---:|
| Per-owner authority capacity | 1,024 each | 8,192 each |
| Entries, start=end | 1,024 each | 1,535 / 1,535 / 1,534 / 1,534 / 1,534 |
| Per-owner history-load delta | 528 / 518 / 483 / 507 / 511 | 0 / 0 / 0 / 0 / 0 |
| Aggregate history-load / contention delta | 2,547 / 0 | 0 / 0 |
| Selected planning mean / max µs; n=256 | 22.7376 / 100.112 | 0.8763 / 10.054 |
| Owner handler mean µs; n=256 | 26.0684 | 2.8857 |
| Exchange mean µs; n=256 | 74.4930 | 49.4222 |
| Full API mean / p50 / p99 / max µs; n=8,192 | 67.4595 / 47.031 / 227.195 / 326.878 | 43.8826 / 43.223 / 103.960 / 173.790 |

R lifetime history-load counts are nonzero but **unchanged**: 1,535/1,535/1,534/1,534/1,534. Per-owner resident-hit deltas = 1,699/1,644/1,627/1,624/1,598 = 8,192. Coordinator misses=owner misses=WAL reads=NOWAIT successes=8,192; explicit value caches disabled. This demonstrates the intended residency outcome and lower observed planning, **not an isolated causal total-speed gain**. Source `reference_directory.rs:24-31,123-190,540-669`: max 1,024→8,192; capacity remains metadata-budget-derived, table slots/descriptors charged. No validation/value-cache contract was removed.

## 2. Primary C16 1 KiB reads: measured cold authority, not warmup speculation

Evidence: each `R/cases/<ID>/result/measurement-read-evidence.json` raw start/end; normalized config in `result/manifest.json → run.config`; effective client/owner configuration in `comparison.json`. All four are unprofiled C16 `read/success`, 8,192 API samples.

| Exact case ID | API p50 / mean / p99 µs | History-load delta | Logical WAL / async / NOWAIT-success delta |
|---|---:|---:|---:|
| `0063-tidehunter_efa_native-1024-read-concurrent-primary_rf1-r0` | 244.941 / 735.380 / 5,345.960 | **1,937** | 8,160 / 8,160 / 0 |
| `0180-tidehunter_efa_native-1024-wal_warm-read-tier-c16-scalar-baseline-r0` | 50.546 / 58.718 / 161.926 | **0** | 8,179 / 0 / 8,179 |
| `0216-tidehunter_tcp-1024-read-concurrent-primary_rf1-r0` | 1,872.878 / 2,255.506 / 8,246.021 | **1,936** | 8,172 / 8,172 / 0 |
| `0168-tidehunter_tcp-1024-wal_warm-read-tier-c16-scalar-baseline-r0` | 782.537 / 1,164.961 / 4,447.885 | **0** | 8,182 / 0 / 8,182 |

Primary raw start→end authority evidence (owners 0–4):

| Case | History-load deltas | Entries start→end | Aggregate resident misses / hits / contention deltas |
|---|---|---|---:|
| 0063 EFA-native | 383 / 398 / 387 / 377 / 392 | 66→449 / 57→455 / 54→441 / 62→439 / 63→455 | 1,941 / 6,219 / 11 |
| 0216 TCP | 379 / 391 / 383 / 395 / 388 | 74→453 / 53→444 / 59→442 / 55→450 / 62→450 | 1,938 / 6,234 / 18 |

Capacity is 8,192 and table_bytes=524,288 per owner at both boundaries, so this is **not the old 1,024-entry ceiling**. Primary coordinator hits=0; misses/RPC deltas 17,682/8,160 (0063), 13,749/8,172 (0216). Owner value-cache hits=0. Resident misses can include contention followed by a worker cache hit; history loads are separate attempted immutable-history lookups, not necessarily distinct batches/physical misses.

| Actual setting | Primary 0063/0216 | Warm-WAL 0180/0168 |
|---|---|---|
| Dataset keys | 4,096 | 8,192 |
| Warmup operations / sweep | 512 / false | 8,192 / true |
| Read lanes | **2** | **4** |
| Owner `read_resident_first` | **false** | **true** |
| Coordinator/owner value cache bytes | 0 / 0 | 0 / 0 |
| Owner metadata budget | 64 MiB | 64 MiB |
| Owner workers / max requests / max device reads | 4 / 32 / 128 | 4 / 32 / 128 |
| Retained history capacity, batches/partition | 16,384 | 24,576 |
| API calls / concurrency / read profile limit | 8,192 / 16 / 0 | 8,192 / 16 / 0 |

Both use closed-loop, queue128, 10-second timeout, no aging, no measurement key sweep, OS caches not dropped. EFA-native is trusted-network/no-TLS in both cohorts; TCP mutual TLS in both. Warm authority start=end entries are 1,538/1,532/1,538/1,538/1,540 (0180) and 1,539/1,539/1,535/1,537/1,540 (0168); history-load delta zero despite resident contention/misses. **Zero history loads does not mean zero worker handoff.**

Primary has no `read-diagnostics.json`; normalized read_profile_limit=0. We can demonstrate measurement-period metadata lookup and async value-read work, but cannot apportion p50/mean/tail differences among metadata, async submission/progress, lanes, warmup or interaction. No matched control varies only residency.

### Spill retention mechanism and precise recommendation

`reference_directory.rs` is byte-matched frozen d340 content:

1. `publish_group`, roughly `:875-947`, validates complete descriptors and retains `Arc<RegisteredBatch>` in pending, not in the history LRU.
2. `start_background :951-1018` captures the durable pending prefix and persists immutable history.
3. Successful adoption `:1044-1061` installs the root, drains pending and drops captured Arcs. **No LRU insertion occurs there.** Active read pins can keep Arcs alive but do not make them directory-searchable.
4. `lookup :605-669` checks cache and pending, then increments history_loads, finds history page, reserves descriptor credit, reads/decodes and caches the batch if eligible. `storage.rs:1576-1613` sends cold authority planning to a bounded worker.
5. `storage/history.rs:480-502,570-622` uses file-backed lookup and `read_exact_at`. Linux page cache may satisfy it: do not call every history-load attempt physical NVMe I/O.

**Recommended bounded optimization to evaluate:** at successful durable-root adoption, preserve eligible already validated immutable Arcs in the **existing** authority cache instead of requiring every uncached/nonpending spilled batch to reacquire residency by first-read history lookup. Preserve entry/byte limits, existing descriptor credits and additional cache-entry overhead, table accounting, lock ordering, active pin/CQ lifetime, generation/address checks, final authority recheck and durable-root/recovery/publication ordering. Reuse Arcs, not descriptor copying/re-decoding; allow eviction under pressure. Do not add a new foreground durability/admission dependency or retain all history unconditionally. Exact saved lookup count/latency is unmeasured; 1,937/1,936 are attempted lookups, not promised physical reads removed.

**Native analogy is qualified:** native `large_table.rs:2535-2625` can retain loaded state under non-unload/unloading-disabled policy, but ordinary async flush completion can explicitly unload processed data. The applicable principle is budgeted useful lookup residency separate from persistence, **not “native always retains every flushed index.”**

## 3. Qualified versus configured read populations

Maintained `comparison.json → cases[].read_tier_qualification`, not configured-tier labels:

| Cohort | Cases | Qualified | Boundary |
|---|---:|---:|---|
| coordinator_hit | 32 | 22 | Ten contain 1–11 coordinator misses/remote accesses |
| owner_hit | 32 | 32 | Zero owner misses/WAL/history-load deltas |
| wal_warm | 32 | 23 | All C1 qualify; eight C16 batch and one C16 scalar have async work |
| registered ordinary C1 steady/reuse | 8 | 0 | Each 8,146 posts/completions +46 missing-grant message/WAL fallbacks |
| registered C16 mixed acquisition/reuse | 8 | 0 | Mixed message/RMA, some acquisition/async work |
| registered handle-headroom C1 | 8 | 8 | Each 8,192 posts/completions; zero fallback/message/WAL/errors |
| remote | 32 | 0 | Configured controls, not pure-path claims |
| distributed cold_device | 8 | 0 | Wider collector windows, no measurement-scoped device attribution |
| native hot-WAL / cold-OS | 2 / 2 | 0 / 0 | Backend read observations/effective cache attestation unavailable |

### Exact counter examples (measurement deltas)

| Exact case ID | API calls / logical keys | Coord hit/miss | RPC | Owner hit/miss | WAL / async | Qualified |
|---|---:|---:|---:|---:|---:|---|
| `0043-tidehunter_tcp-8192-coordinator_hit-profiled-r0` | 8,192 / 8,192 | 8,192 / 0 | 0 | 0 / 0 | 0 / 0 | yes |
| `0053-tidehunter_efa_direct-256-coordinator_hit-profiled-r0` | 8,192 / 8,192 | 8,191 / 1 | 1 | 0 / 1 | 1 / 1 | no |
| `0075-tidehunter_efa_direct-8192-coordinator_hit-read-tier-c16-batch32-profiled-r0` | 2,048 / 65,536 | 65,536 / 0 | 0 | 0 / 0 | 0 / 0 | yes |
| `0115-tidehunter_efa_direct-8192-owner_hit-read-tier-c16-batch32-profiled-r0` | 2,048 / 65,536 | 0 / 65,536 | 10,227 | 64,629 / 0 | 0 / 0 | yes |
| `0178-tidehunter_efa_direct-1024-wal_warm-read-tier-c16-scalar-profiled-r0` | 8,192 / 8,192 | 0 / 9,818 | 8,180 | 0 / 8,180 | 8,180 / 0 | yes |
| `0179-tidehunter_efa_direct-8192-wal_warm-read-tier-c16-batch32-profiled-r0` | 2,048 / 65,536 | 0 / 65,536 | 10,230 | 0 / 64,472 | 64,472 / 244 | no |

All examples have zero authority-history delta. Admission rechecks can increase coordinator lookup attempts (`pending_read/concurrent.rs:50-71`); exact-reference joining/deduplication can reduce owner members (`pending_read.rs:2008-2048,2198-2202,2253-2255`). Grouped RPCs are not keys. These are supported mechanisms, not proof they explain every count difference; do not infer per-API hit fractions without corresponding counters.

### Registered posts, completions and fallbacks

| Exact case ID | Calls | Posts=completions | Message / missing / WAL / async | Acquisitions | Qualified |
|---|---:|---:|---:|---:|---|
| `0139-tidehunter_efa_native-8192-registered_reuse-profiled-r0` | 8,192 | 8,146 | 46 / 46 / 46 / 0 | 0 | no |
| `0139-tidehunter_efa_native-8192-registered_reuse-profiled-r0-handle-headroom` | 8,192 | 8,192 | 0 / 0 / 0 / 0 | 0 | yes |
| `0126-tidehunter_efa_native-1024-registered_steady_c16_mixed_acquisition_reuse-read-tier-c16-scalar-profiled-r0` | 8,192 | 2,076 | 6,104 / 5,988 / 6,104 / 0 | 66 | no |
| `0127-tidehunter_efa_native-8192-registered_steady_c16_mixed_acquisition_reuse-read-tier-c16-batch32-profiled-r0` | 2,048 | 18,744 | 10,150 / 45,220 / 45,892 / 151 | 0 | no |
| `0143-tidehunter_efa_native-8192-registered_reuse_c16_mixed_acquisition_reuse-read-tier-c16-batch32-profiled-r0` | 2,048 | 19,974 | 10,153 / 44,357 / 44,694 / 48 | 0 | no |

All 24 registered cases have zero RMA errors and capacity/oversize/stale/unsupported fallback deltas. Headroom requests max_handles=40,960 vs8,192; 8 KiB source_bytes=67,108,864, source_regions=8,192, destination_bytes=20,971,520, destination_slots=640, max_read_bytes=32,768, require_device_read=true unchanged. Preserve the suffix: not another repetition. Headroom 0139 API mean/p50/p99/max=51.8961/52.156/64.992/96.331 µs (8,192 reads); selected FI_READ mean **46.4955 µs**, max **62.090**, n=256. **This is accepted post→locally observed completion/progress residence, not isolated NIC service or pure wire latency.** Owner timing is not applicable (0 profiled responses/256 flights).

## 4. Supported dominant read stages and populations

Each row uses `R/cases/<ID>/result/read-diagnostics.json`; selected flights completed. Stage counts differ; “—” = unavailable max.

| Exact case ID | Stages | n | Mean µs | Max µs |
|---|---|---:|---:|---:|
| `0145-tidehunter_tcp-256-wal_warm-profiled-r0` | exchange / handler / planning / NOWAIT | 256 each | 508.521 / 22.215 / 4.492 / 6.425 | — / — / 57.511 / 63.737 |
| `0157-tidehunter_efa_direct-256-wal_warm-profiled-r0` | exchange / handler / planning / NOWAIT | 256 each | 49.422 / 2.886 / 0.876 / 0.780 | — / — / 10.054 / 2.931 |
| `0095-tidehunter_efa_direct-8192-owner_hit-profiled-r0` | exchange / handler / planning / executor | 256 each | 65.239 / 3.127 / 1.485 / 0.718 | — / — / 180.395 / 2.543 |
| `0107-tidehunter_tcp-8192-owner_hit-read-tier-c16-batch32-profiled-r0` | exchange / handler / planning / cache resolution | 256 each | 2,437.012 / 32.497 / 17.936 / 5.520 | — / — / 238.031 / 39.931 |
| `0111-tidehunter_efa-8192-owner_hit-read-tier-c16-batch32-profiled-r0` | exchange / handler / planning / cache resolution | 256 each | 510.653 / 17.938 / 8.450 / 7.344 | — / — / 155.325 / 70.107 |
| `0115-tidehunter_efa_direct-8192-owner_hit-read-tier-c16-batch32-profiled-r0` | exchange / handler / planning / cache resolution | 256 each | 1,419.180 / 19.154 / 8.423 / 8.309 | — / — / 234.021 / 69.359 |
| `0119-tidehunter_efa_native-8192-owner_hit-read-tier-c16-batch32-profiled-r0` | exchange / handler / planning / cache resolution | 256 each | 1,296.308 / 30.134 / 11.485 / 14.946 | — / — / 148.230 / 55.675 |
| `0179-tidehunter_efa_direct-8192-wal_warm-read-tier-c16-batch32-profiled-r0` | exchange / handler / planning | 256 each | 812.170 / 32.018 / 9.402 | — / — / 196.959 |
| same 0179 | NOWAIT / async | 1,603 / 26 | 1.915 / 88.827 | 13.829 / 196.781 |
| `0203-tidehunter_efa_direct-256-cold_device-profiled-r0` | exchange / handler / planning / async | 256 each | 1,329.648 / 1,277.995 / 1,153.892 / 121.214 | — / — / 3,399.468 / 222.472 |
| `0139-tidehunter_efa_native-8192-registered_reuse-profiled-r0-handle-headroom` | exchange / FI_READ | 256 each | 50.760 / 46.496 | — / 62.090 |

**Prefix populations:** coordinator selects first256 API resolutions independently; remote selects first256 newly admitted flights (not cache hits/index misses/coalesced followers). A C16 batch maps to multiple owner flights. These means are not whole-API means or tail decompositions. Actor queue/service: 0115 n=15 means454.660/1,023.604 µs, inline n=241 mean2,295.700; TCP0107 actor n=192 means191.665/2,799.180, inline n=64 mean1,503.992; 0119 actor n=8 means886.880/666.282, inline n=248 mean1,926.501. Do not average those populations indiscriminately.

Local transport output retirement means: 0111/0115/0119=38.964/135.518/75.010 µs, n=256 frames each. TCP0107 has290 output-frame observations and zero retirement sum: no separate retained-DMA interval, **not zero network cost**. Client provider CQ-call n=8,714/11,228/11,810, means0.3867/0.0979/0.0929 µs; not256 per-request RTTs. Owner response encode/queue/submit/retire boundary deltas have independent sampled-frame counts; lifetime maxima cannot be differenced.

Coordinator-only `0075-tidehunter_efa_direct-8192-coordinator_hit-read-tier-c16-batch32-profiled-r0`: zero remote flights,256 bulk resolutions/8,192 sampled cache hits. Cache-lock wait sum79,041,663 ns,n8,192,max349.193 µs (mean9.6486 µs/acquisition); lookup sum3,109,576 ns,n8,192,max16.178; index lookup sum4,000,776 ns,n8,192,max21.740. This motivates bounded batch cache-access investigation, not a per-call9.65 µs claim.

## 5. 8 KiB ×32 regressions and intended-stage distinction

All rows `get_many/success`, C16,2,048 API calls/65,536 logical keys; unprofiled owner-hit R cases fully qualify. B→R crosses hardware/region.

| Exact case ID | B→R calls/s | B→R p50 µs | B→R p99 µs | R mean / max µs |
|---|---:|---:|---:|---:|
| `0105-tidehunter_tcp-8192-owner_hit-read-tier-c16-batch32-baseline-r0` | 6,472.19→4,384.79 | 2,200.221→3,135.003 | 11,725.215→5,017.849 | 3,199.165 / 8,408.185 |
| `0109-tidehunter_efa-8192-owner_hit-read-tier-c16-batch32-baseline-r0` | 7,979.63→6,383.36 | 1,587.350→1,040.958 | 14,487.562→15,337.943 | 2,045.136 / 29,308.705 |
| `0113-tidehunter_efa_direct-8192-owner_hit-read-tier-c16-batch32-baseline-r0` | 7,550.61→5,709.92 | 1,652.070→1,224.536 | 12,896.191→15,364.231 | 2,327.629 / 27,239.041 |
| `0117-tidehunter_efa_native-8192-owner_hit-read-tier-c16-batch32-baseline-r0` | 9,059.77→6,865.05 | 1,412.863→949.433 | 3,901.554→11,671.873 | 1,871.124 / 21,428.748 |

Profiled counterparts0107/0111/0115/0119 (full IDs §4) R mean3,297.704/2,084.044/2,568.890/1,884.948 µs,p99 7,432.572/14,082.347/15,550.203/12,968.619. They are not repetitions of unprofiled cases.

Warm-WAL C16 batches remain **unqualified**: baseline0169/0173/0177/0181 async401/68/108/137; profiled0171/0175/0179/0183 async622/115/244/103. Authority-history loads remain zero; metadata warming did not make value paths pure NOWAIT. `0182-tidehunter_efa_native-1024-wal_warm-read-tier-c16-scalar-profiled-r0` also has4 async submissions.

`tcp/read_executor.rs:680-800,926-986` returns all-resolved records before device counting, preserving admission/reply lease/authority/file pins/recheck. But 0115 B→R selected executor mean **3.982→8.775 µs**, cache resolution3.216→8.309, planning**86.121→8.423**, handler91.826→19.154 (n256 each). Evidence supports lower observed planning/handler alongside worse other stages/total, **not an isolated scan-elimination speed gain**. Future matched all-hit comparison should isolate it.

Primary controls are separate, not tier-qualified: `0167-native-8192-read-many32-primary_rf1-r0` mean/p50/p99/max85.655/80.663/153.688/220.988 µs; `0050-tidehunter_efa_native-8192-read-many32-primary_rf1-r0`1,869.660/1,894.007/3,507.155/4,631.515. Native/distributed operations and cache populations differ; subtraction is not pure transport overhead.

## 6. Actual cold-storage activity, not logical labels

All eight distributed cold cases have coord/owner hits0, misses=WAL=async8,192, history loads4,860, and **6,998 actual matched block issue→completion requests** across five owners. Collectors qualify as request-identity traces, but cases do **not** qualify for measurement-scoped device attribution because windows include gate overlap/background/metadata. Weighted means = sum(request durations)/sum(counts), not pooled percentile estimates.

| Exact case ID | Actual request count | Weighted block mean µs | Max µs |
|---|---:|---:|---:|
| `0190-tidehunter_tcp-256-cold_device-baseline-r0` | 6,998 | 283.164 | 559.582 |
| `0191-tidehunter_tcp-256-cold_device-profiled-r0` | 6,998 | 272.791 | 579.547 |
| `0196-tidehunter_efa-256-cold_device-baseline-r0` | 6,998 | 273.564 | 867.534 |
| `0197-tidehunter_efa-256-cold_device-profiled-r0` | 6,998 | 259.634 | 1,012.341 |
| `0202-tidehunter_efa_direct-256-cold_device-baseline-r0` | 6,998 | 250.841 | 787.748 |
| `0203-tidehunter_efa_direct-256-cold_device-profiled-r0` | 6,998 | 245.965 | 524.311 |
| `0208-tidehunter_efa_native-256-cold_device-baseline-r0` | 6,998 | 257.276 | 542.739 |
| `0209-tidehunter_efa_native-256-cold_device-profiled-r0` | 6,998 | 250.843 | 520.652 |

0203 per-owner n1,446/1,407/1,398/1,340/1,407; p50 µs314.386/187.655/201.019/312.095/240.188. Its sampled planning mean1,153.892,max3,399.468,n256 dominates handler mean; async member mean121.214,max222.472,n256. Planning includes metadata/worker queue/handoff, not just range arithmetic. Async includes SQE preparation/retry/kernel/progress/CQE observation. Neither is NAND time and neither shares the6,998 device-request population.

Native `0184-native-256-native_hot_wal_lru0_pagecache-baseline-r0` and `0185-native-1024-native_hot_wal_lru0_pagecache-baseline-r0`: zero matched block reads, duration quantiles null. `0214-native-256-native_cold_os_lru0-baseline-r0`: one request262.209 µs, all trace-tail warnings. `0215-native-1024-native_cold_os_lru0-baseline-r0`: zero. Cold receipts say caches dropped, but backend cache/miss observations are unavailable; API p50s0.818/0.937 µs are **not demonstrated cold-NVMe latency**. Native `Wal::read` can use prefaulted mappings, checks whole-frame CRC and copies; distributed member-read validation is a different contract.

## 7. Write durability, publication, group-sync, caller and CQ evidence

All diagnostic rows: 1 KiB scalar `put/success`, C16,4,096 API calls. Native selects256 accepted scalar operations; distributed selects first256 append requests on five connections=1,280, **not1,280 logical writes**. Connection/control/frame populations differ. No per-write stage maxima/percentiles exported.

| Exact case ID | API mean / p50 / p99 / max µs;n4,096 | Interval append requests | Prefix receipts / persist calls / no-ops / file persists |
|---|---:|---:|---:|
| `0080-tidehunter_efa_native-1024-write-concurrent-write_stage_diagnostics-r0` | 1,495.077 / 1,367.802 / 4,774.404 / 5,780.597 | 2,066 | 2,394 / 1,280 / 205 / 1,075 |
| `0110-tidehunter_efa-1024-write-concurrent-write_stage_diagnostics-r0` | 1,304.159 / 1,168.316 / 4,693.036 / 5,743.900 | 2,304 | 2,166 / 1,280 / 210 / 1,070 |
| `0165-tidehunter_efa_direct-1024-write-concurrent-write_stage_diagnostics-r0` | 1,505.522 / 1,400.207 / 4,611.611 / 5,936.829 | 2,054 | 2,443 / 1,280 / 175 / 1,105 |
| `0231-tidehunter_tcp-1024-write-concurrent-write_stage_diagnostics-r0` | 2,096.216 / 1,939.514 / 5,313.681 / 6,840.824 | 2,233 | 2,201 / 1,280 / 276 / 1,004 |
| `0201-native-1024-write-concurrent-write_stage_diagnostics-r0` | 3,778.913 / 2,932.860 / 12,854.234 / 24,522.215 | n/a | 256 selected scalar operations/barriers |

Owner storage sums ms, order EFA-native/EFA/EFA-direct/TCP, append/persist n1,280 each: append total87.573/81.264/89.692/163.005; validation69.723/64.292/70.803/131.607; persist total328.340/303.127/326.855/314.574; WAL file-persist305.058/278.759/303.521/274.180 (file n in table). Validation and persistence are substantial measured work, not additive fractions of API/handler clocks. Selected namespace/inventory persistence count0 is not universal absence. Persist no-op means already covered by durable frontier, not unsynced reply.

0080 means: append68.4165 µs/request, validation54.4707 (n1,280); persist256.5158 µs/call(n1,280), file283.7748(n1,075). Connection/client requests1,300: exchange693.3588 µs; owner control queue/execution307.3334/337.8255 µs(n1,300). Client output-retire32.2490 µs(n1,310), owner31.1632(n1,315). Client CQ polls1,108,036, provider-call sum45.394 ms, **maximum poll gap12.839152 ms**; owner polls3,979,345,sum158.554 ms,max gap409.363 µs. These do not determine per-write caller cost or pure wire time.

0080 interval: admitted=completed=durable4,096; publication13.481175 ms,storage-wait332.190767,preparation/admission67.135901,dispatch3.655603;8 checkpoints,sum30.926732 ms,no foreground checkpoints. Out-of-durable-prefix receipt→publication blocked intervals n1,945,sum23.530636 ms, **dependency_wait_batches0**. TCP0231 blocked n1,969,sum25.983834 ms,dependency_wait_batches2. Blocked intervals overlap and are not all publication delay/predecessor stalls. Exact group sizes/singletons/queue residence and publication→caller-resume are absent.

Native0201 selected n256: WAL append519.864181 ms(mean2,030.7195 µs),index1.001195 ms(mean3.9109); separate256 barriers make512 successful file calls,sum456.692314 ms,256 namespace calls,sum47.627236 ms; no reported errors. Barrier covers all registered Value-WAL files/directory, possibly clean old files; not equivalent to one owner group sync or native default page-cache acknowledgment. Append/index are sequential; barrier components are nested, not extra API fractions.

## 8. Exact-source map and constrained next optimizations

All mappings below bind to frozen d340 through the497-file byte-match proof.

| Area | Source / invariant | Supported next step |
|---|---|---|
| Native/distributed read | `wal/mod.rs:379-467`;`storage.rs:1576-1638`: native full CRC/copy vs member bounds/authority/file/log pins | Preserve contracts and native reference; no parity target or validation removal |
| Authority residency/spill | `reference_directory.rs:24-31,123-190,605-669,875-1061` | Evaluate bounded post-adoption Arc carry-over with full credit accounting; measure adoption→first-read and worker savings |
| Cold planning | `tcp/reactor.rs:3208-3243,3821-3902` | Separate worker queue/load/return and contention; do not optimize “device arithmetic” from residence |
| All-resolved executor | `tcp/read_executor.rs:680-800,926-986`;consumer`reactor.rs:3894-3969` handles Ready/Pending/Full/Deferred | Already skips device scan after cache/NOWAIT resolution; isolate benefit on same hardware with all-hit qualification |
| Coordinator cache | `pending_read/concurrent.rs:89-201`: per-live-member mutex while index read guard held | Evaluate bounded32-key cache access/fewer acquisitions, preserve LRU/accounting/reference checks/lock order; measure C1 hold-time tradeoff |
| Flight scheduling | `pending_read.rs:1059-1213,1870-1945,2008-2048` | Match API/flight/member/actor-return/caller-ready populations; correlate C16 tails and CQ gaps before policy changes |
| Registered completion | `reactor.rs:1950-2005,2490-2905`: epoch grants,bounded destinations,mixed fallback order,verified completion | Keep headroom pure controls/mixed C16 separate; retain buffers/credits through actual DMA completion |
| Response path | `reactor.rs:4602-4933`;`tcp/resident.rs:331-340` | Attribute recheck/encode/queue/submit/local retirement and copies/TLS before scatter-gather or transport changes |
| Ready-group writes | `remote_db/concurrent.rs:677-809`;`remote_db.rs:2682-2816` | Already drains ready work/flushes tails without artificial wait; measure count/bytes/singletons/dispatch reason/requests-per-sync first |
| Durable publication | `tcp/control.rs:185-213,375-445,550-625`;`storage.rs:278-346,1250-1320`;`remote_db.rs:2648-2679,2843-3019` | Keep synced complete-batch receipt/dependency publication; measure receipt→eligible→publish→reply→caller stages |
| Native barrier | `db.rs:1368-1401`;`backends/native.rs:100-116,239-305` | Attribute clean-file/dirty-frontier work before considering optimization; never remove durability for speed |

## 9. Plan §17.8 and source/paper mechanisms

Reviewed `docs/distributed/REMOTE_READ_LATENCY_AND_TRANSPORT_PLAN_2026-10-29.md:998-1087`. Its pending/local-only language is historical: this campaign supplies counter evidence, not isolated executor-fast-return speed proof. Parent owns shared-plan updates.

- **Tidehunter QMD** `tidehunter-papers/tidehunter-2026-v2.md`, §§3.1/3.2/3.4/4.2, addresses109–138,143–146,159–168: relevant prose read; §3.1 trailing embedded image/base64 was truncated by retrieval limit. Direct index→Value-WAL/useful metadata residency supports avoiding unnecessary lookup/handoff; native page-cache/explicit-flush distinction prohibits guarantee equivalence. Native flush retention is policy-dependent as §2 states. Project anchor: `REFERENCE_DIRECTORY_DESIGN_2026-10-06.md` native invariant mapping.
- **Valkey** [beforeSleep](https://github.com/valkey-io/valkey/blob/a230da1a3da00747b423f5d9be3de3b0192692c3/src/server.c#L2064-L2087), [AOF flush](https://github.com/valkey-io/valkey/blob/a230da1a3da00747b423f5d9be3de3b0192692c3/src/aof.c#L1259-L1260), [always-sync/rewrite qualification](https://github.com/valkey-io/valkey/blob/a230da1a3da00747b423f5d9be3de3b0192692c3/src/aof.c#L1371-L1396): already-linked scheduling analogy—eligible buffered work shares sync before replies, with configuration exceptions; not equivalence to TideHunter publication.
- **TiKV Raft Engine** [Engine::write](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs#L139-L205), [WriteBarrier](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/write_barrier.rs#L151-L205): ready writers share sync, empty leader proceeds; do not copy unsafe barrier or equate log sync/replication/publication.
- **eRPC QMD** `tidehunter-papers/erpc-2019.md`, §§3.2/4.2/6.1,address105–110,169–198,292–297 read; retained [arXiv:1806.00680v2](https://arxiv.org/abs/1806.00680), [NSDI](https://www.usenix.org/conference/nsdi19/presentation/kalia). Inline short work, move long handlers off progress; keep queue/DMA ownership until retirement. Its2.3 µs is32-byte two-node CX5 RPC, not EFA/TLS/durable/cold-read target. Unsignaled completion techniques are not portable assumptions.

## Missing measurements and concrete next steps (not executed)

1. Same-hardware/source/data/security/durability C1/C16 profiled/unprofiled comparisons; independent repetition only in separately authorized future work. Keep8 KiB×32 regressions visible.
2. Post-spill retention-only comparison, spill/adoption→first-read correlation, entry/byte pressure and worker queue/load/return clocks. Primary has measured lookup counts but no selected stage timing; history loads are not unique batches or physical misses.
3. All-hit executor isolated stage benefit, retaining admission/fencing/pins/leases/final recheck.
4. Match API-resolution/flight/member/message/frame/actor/caller populations; export coalescing and admission recheck counters rather than infer differences.
5. Exact measurement-window block/request/file attribution plus effective native-cache/miss evidence. Current device counts cannot become per-API cold-device or NAND distributions.
6. Ready-group sizes/bytes/singletons and receipt→publication→caller clocks. Per-write stage maxima/percentiles/native put_batch append-index remain unavailable.
7. Attribute CQ gaps/local provider residence/response copies/TLS/registration before transport changes. No isolated wire/peer-arrival timestamps, multiplexed per-request send/header/body or pure device service time exists here. **FI_READ46.5 µs remains a post→observed-completion/progress span.**

Read-only investigation; no code changes, benchmark reruns, builds, tests, linters, formatters or AWS calls. Only these two local audit artifacts are written; raw data/receipts/source and shared reports remain untouched. Tracked diff was empty; no patch-introduced defect is asserted or engine correctness certification implied.
