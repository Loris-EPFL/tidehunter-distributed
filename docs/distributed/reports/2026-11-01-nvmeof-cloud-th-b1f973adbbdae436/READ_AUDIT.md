# Frozen v5 cloud sealed-read audit

## Scope, provenance and result

Read-only audit of **all 100 sealed-read rows**, cases 0000–0099, in the actual `th-b1f973adbbdae436` run. No AWS calls, source edits, benchmark executions, builds or privileged device operations were performed.

**Campaign is not complete:** `comparison.json` has `complete=false`, `monitoring_complete=false`, `current_backend_coverage=false`, `completed_cases=125`, `planned_cases=125`. All 100 read rows completed; that does not repair the monitoring gap. There is one independent repetition (`repeat=0`), not evidence for significance or error bars.

Paths used below:

- **E** = `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/kv-comparison/cloud-nvmeof-th-b1f973adbbdae436/controller-extracted`
- **S** = `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/local-nvmeof-cloudprep-20261031T192908Z-ca100224/package-v5/source`
- Controller package metadata: `E/../controller-config.json`; frozen package manifests: `S/../build.json`, `S/../manifest.json`, `S/../source.json`.
- Frozen source ZIP SHA256: `c9d9378277e04a60cc393adcf89bf630d586c95db1a359dc7f940396e797c3a5`; source tree SHA256 `198709edda6bd094d857457003dd0ecdc65d31def493d92d97b2d3c0a8a0fee3`.
- Measured binary SHA256: `42faf4edd8babe7d01916fd4c43993b7b3781bddbd2b49a69b6adb261fd3972b`; BLAKE2b512 `1b8117b2556b6c6f727c2e52b1544cf5eb917ff7177fc1f73a666707b9edf05036e4f9dc32b516969efa660fd2201620d675d3671f5175321721023deddef4a0`.
- Source revision metadata is `895a83d7625d48df208c6ede8d59f0a33feb40bd`, but the frozen snapshot hash, not revision alone, identifies measured uncommitted source. Current working-tree changes after v5 are not measured evidence.
- Controller archive identity supplied with the frozen evidence: 295,584,760 bytes, SHA256 `080a364aeac00c3a1d0f9182f822864ff7ace19f1f23041a44931b3ca6673555`; prior inventory verification covers 8,966 files / 4,112,382,670 uncompressed bytes. This audit did not repeat archive checksumming.

Evidence handling: parent reported that a separate Ops invocation of `cloud_campaign.report` transiently rewrote extracted comparison JSON/CSV/Markdown, then restored all three byte-for-byte from the verified archive. This auditor did not invoke that reporter and observed no numerical discrepancy. After the notice, reloaded the restored JSON: complete/monitoring false, 125 completed, 100 read rows, 585,728 read successes and 2,998,272 read keys, with 0000/0044/0099 anchors unchanged. Raw case files and the immutable archive were not changed by that reported incident.

### Independent checks and their scope

- Selected `.job.cohort == "sealed_read"`, using **`.job.backend`**, not the object-valued `.backend`. Found native 22, Tidehunter/TCP 26, EFA-native 26, NVMe/TCP 26: **88 baseline + 12 diagnostic = 100**.
- All 100 have `status=completed`, no case error, `measurement_rows_complete=true`, `offered_calls=success_calls=planned_measurement_calls=job.measurement_operations`, and `unsuccessful_calls=0`.
- There are **62 scalar populations × 8,192 calls and 38 get_many populations × 2,048 calls = 585,728 successful measurement API calls**, representing **2,998,272 logical keys**. A get_many call is the whole ordered 32-key group.
- Checked the raw receipt admission flag in all 100 `cases/00*/receipt.json`: all `raw_artifacts_validated=true`. No non-completed core manifest/summary/status or different binary BLAKE2b512 was found. The planned and validated phase-count dictionaries match in every receipt, including full-key verification and measurement separately. All 100 raw CSVs expose the required timing schema. Searches over all raw CSVs found no unsuccessful measurement rows or successful undispatched measurement rows. The 24 boundary scalar CSVs also matched an exact contiguous 8,192-success measurement tail.
- All 100 measurement boundaries report gate release and completion gate observed; all 100 read intervals say attempted and drained. Every NVMe interval and all five reader/counter/stage delta entries are available. Independently summed six NVMe qualification counter fields across readers and compared them to the qualification totals: **no mismatch**.
- All job coordinator/owner explicit-cache limits are zero. For all **78 distributed rows**, measurement deltas are available and have coordinator hits/inserts/admitted bytes zero, and every owner partition has cache hits/inserts/served bytes zero. Native does not expose the distributed interval counter interface; its unavailable counters must not be called zero.
- These latency tables independently select and exact-match the frozen raw-derived populations; means/quantiles are those admitted by the campaign analyzer, **not a second independent sort of every CSV**. Counts/admission, raw failure scans, grouping, NVMe counter totals, stage count/sum aggregation and amplification below were independently checked. The analyzer's nearest-rank definition is `sorted[ceil(q*n)-1]` (`S/experiments/kv-comparison/analyze.py:18–22`); raw clock validation is at lines 25–57 and phase-count admission at `campaign.py:86–110`.

## Exact matched API results

Each cell is **case-number: p50 / p99 / mean in microseconds; successful API calls/s**. p50/p99 retain exact nanosecond resolution after conversion; displayed means and throughput are rounded to 0.001. Full-precision numbers remain in `E/comparison.json` under each case's `metrics`. Throughput is from the observed raw measurement span, not `1 / API mean`; driver/oracle/scheduling time can matter.

Exact case ID and raw path reconstruction for every cell:

`<NNNN>-<backend>-<value_bytes>-<profile>`; backend strings are `native`, `tidehunter_efa_native`, `tidehunter_nvme_tcp`, `tidehunter_tcp` according to the column. The table tier/shape/concurrency plus `-baseline` or `-diagnostic` is the exact profile. Raw evidence is **`E/cases/<case_id>/result/samples.csv`**, `summary.json`, `manifest.json`, `measurement-read-evidence.json`, plus `E/cases/<case_id>/receipt.json`, `measurement-before.json`, `measurement-end.json` and `resource-interval.json`. Thus, for example, 0044 is exactly `0044-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-baseline`.

Every scalar row has n=8,192; every batch32 row has n=2,048. **All p99s are descriptive below 10,000 calls; all p999s are descriptive below 100,000 calls.** These are not tail-SLA estimates. Diagnostics are not pooled with baseline or used as independent repetitions. No native diagnostic twin exists; missing is not zero.

### 1 KiB baseline (65,536-key source)

| Tier / API shape / concurrency (baseline) | Native | EFA-native | NVMe/TCP | Tidehunter/TCP |
|---|---|---|---|---|
| first_touch-scalar-c1 | 0020: 3.410/102.982/24.492; 18494.815 | 0028: 65.812/129.882/74.657; 8985.501 | 0000: 228.575/480.721/276.097; 3165.958 | 0010: 125.102/243.274/141.583; 5375.080 |
| repeat_read-scalar-c1 | 0021: 2.880/4.520/2.862; 32921.257 | 0030: 66.211/83.282/66.775; 9978.000 | 0002: 354.957/530.541/388.324; 2303.588 | 0012: 203.474/265.385/198.944; 4018.117 |
| first_touch-scalar-c16 | 0022: 96.392/178.874/91.445; 114892.975 | 0032: 169.283/743.115/205.013; 67072.986 | 0004: 647.813/1866.945/722.475; 21135.539 | 0014: 281.485/1195.972/357.732; 40549.676 |
| repeat_read-scalar-c16 | 0023: 3.770/7.640/3.934; 122647.267 | 0033: 76.342/165.803/82.830; 134522.666 | 0005: 651.862/2053.040/749.775; 20366.543 | 0015: 159.734/383.178/275.071; 46424.870 |
| first_touch-batch32-c1 | 0024: 334.097/1132.592/375.113; 2185.998 | 0034: 463.619/532.681/463.100; 1898.401 | 0006: 1586.726/2235.107/1573.882; 601.145 | 0016: 557.840/754.385/558.148; 1531.501 |
| repeat_read-batch32-c1 | 0025: 58.941/81.282/60.317; 7737.379 | 0035: 161.593/193.644/163.838; 4047.970 | 0007: 918.418/1267.625/933.361; 987.424 | 0017: 358.967/488.340/364.206; 2232.119 |
| first_touch-batch32-c16 | 0026: 927.958/2328.265/980.036; 15223.107 | 0036: 964.439/3386.936/1143.288; 13067.706 | 0008: 11109.598/82939.544/17754.630; 894.695 | 0018: 987.750/3181.912/1131.251; 13159.200 |
| repeat_read-batch32-c16 | 0027: 103.892/146.423/107.396; 85703.917 | 0037: 2062.510/5331.074/2244.855; 6890.303 | 0009: 26435.290/65721.743/28543.735; 557.999 | 0019: 2356.946/5685.832/2528.511; 6116.939 |

### 8 KiB baseline (65,536-key source)

| Tier / API shape / concurrency (baseline) | Native | EFA-native | NVMe/TCP | Tidehunter/TCP |
|---|---|---|---|---|
| first_touch-scalar-c1 | 0058: 41.490/115.903/44.506; 12523.523 | 0066: 105.322/161.733/109.781; 6453.397 | 0038: 250.605/423.988/254.202; 3338.970 | 0048: 146.082/289.716/166.929; 4691.465 |
| repeat_read-scalar-c1 | 0059: 4.210/6.460/4.382; 27736.342 | 0067: 72.701/88.941/73.123; 9044.841 | 0039: 331.008/472.750/333.609; 2593.596 | 0049: 212.824/292.226/208.518; 3981.056 |
| first_touch-scalar-c16 | 0060: 303.686/897.868/351.964; 41695.551 | 0068: 260.105/1420.548/350.193; 41665.726 | 0040: 709.834/1870.226/778.312; 19618.063 | 0050: 339.087/1344.587/407.675; 36042.764 |
| repeat_read-scalar-c16 | 0061: 5.930/15.910/6.245; 119953.007 | 0069: 85.431/184.134/110.252; 104122.596 | 0041: 668.283/1998.729/756.672; 20150.487 | 0051: 177.644/450.949/241.938; 37743.309 |
| first_touch-batch32-c1 | 0062: 1264.595/6745.769/1867.931; 493.818 | 0070: 1033.380/1374.766/1096.397; 770.570 | 0042: 1905.097/2633.302/1894.351; 474.977 | 0052: 894.578/1338.537/900.606; 943.308 |
| repeat_read-batch32-c1 | 0063: 112.052/148.032/115.786; 3637.151 | 0071: 287.875/365.707/292.765; 2150.489 | 0043: 1052.321/1598.351/1080.391; 800.147 | 0053: 535.030/641.813/537.674; 1404.300 |
| first_touch-batch32-c16 | 0064: 4876.414/6767.650/4790.891; 3211.019 | 0072: 2765.964/4201.491/2833.740; 5280.068 | 0044: 13634.585/76649.301/19499.716; 810.391 | 0054: 3868.725/5393.945/3908.934; 3874.284 |
| repeat_read-batch32-c16 | 0065: 166.073/274.085/182.044; 45205.867 | 0074: 2196.866/5320.410/2355.640; 6317.396 | 0046: 26835.386/67388.982/29265.370; 542.185 | 0056: 2525.038/7393.103/2755.259; 5405.565 |

### Boundary sizes (8,192-key source; scalar c1 baseline)

| Value bytes / tier | Native | EFA-native | NVMe/TCP | Tidehunter/TCP |
|---|---|---|---|---|
| 64 / first_touch | 0080: 1.230/2.230/1.408; 34043.898 | 0082: 61.521/119.053/63.502; 10008.797 | 0076: 284.106/480.970/311.583; 2937.633 | 0078: 124.943/238.685/141.482; 5660.485 |
| 64 / repeat_read | 0081: 1.170/1.960/1.242; 31816.620 | 0083: 65.832/81.212/66.429; 10132.584 | 0077: 286.946/367.317/279.490; 3046.855 | 0079: 194.054/254.905/189.924; 4414.537 |
| 256 / first_touch | 0088: 0.980/1.840/1.133; 32304.866 | 0090: 60.821/116.652/64.806; 9997.876 | 0084: 302.296/519.320/337.279; 2598.587 | 0086: 155.153/232.935/151.974; 4989.720 |
| 256 / repeat_read | 0089: 0.980/1.740/1.038; 31883.341 | 0091: 65.872/80.501/66.119; 10313.351 | 0085: 290.636/411.128/288.361; 3002.237 | 0087: 196.574/252.815/192.918; 4153.049 |
| 65536 / first_touch | 0096: 94.392/354.017/129.250; 5235.060 | 0098: 255.665/498.000/289.638; 2800.643 | 0092: 427.668/747.995/449.293; 1863.178 | 0094: 250.384/701.394/285.673; 2764.042 |
| 65536 / repeat_read | 0097: 15.511/28.610/15.711; 14405.104 | 0099: 176.154/197.694/176.203; 4087.286 | 0093: 526.430/735.594/530.144; 1631.846 | 0095: 337.866/485.140/337.204; 2446.709 |

### Diagnostic twins, separate from baseline

Same units and n as their exact baseline partners above. Diagnostic instrumentation is `read_profile`, first 256 newly admitted physical flights; not a random full-run sample and not 256 API calls for every grouped workload. Backend stages have separate populations. NVMe diagnostic rows additionally enable target/block tracing.

| Value bytes / profile (diagnostic) | Native | EFA-native | NVMe/TCP | Tidehunter/TCP |
|---|---|---|---|---|
| 1024 / first_touch-scalar-c1 | — | 0029: 66.221/182.843/82.077; 8337.262 | 0001: 241.945/514.520/288.974; 3060.595 | 0011: 127.482/242.075/146.725; 5625.574 |
| 1024 / repeat_read-scalar-c1 | — | 0031: 67.461/160.163/70.777; 9083.402 | 0003: 321.736/531.850/370.210; 2484.845 | 0013: 198.384/254.475/193.738; 4100.200 |
| 8192 / first_touch-batch32-c16 | — | 0073: 2966.468/5592.808/3164.389; 4762.052 | 0045: 13271.349/78679.715/18867.039; 836.615 | 0055: 3835.745/5325.763/3913.240; 3882.459 |
| 8192 / repeat_read-batch32-c16 | — | 0075: 2154.072/9917.573/2451.393; 6078.412 | 0047: 27313.518/69559.884/29428.267; 539.174 | 0057: 2497.908/7289.771/2697.344; 5533.052 |

## Source, cache and security fairness

For each value size, all backends have exactly the same logical source ID, seed 1775, keyspace size and value size: `sealed-v2-seed1775-size<V>-keys<K>`. All jobs have warmup operations zero. Every run first verifies the full existing dataset, excluded from measurement. `first_touch` uses a deterministic distinct-key sweep; `repeat_read` uses uniform randomized requests over the preverified dataset. Thus the two tiers differ in access pattern as well as eviction; a first-touch versus repeat difference is not an isolated cache-effect experiment.

- Tidehunter/TCP and NVMe/TCP **share the exact owner source WAL block**: `E/datasets/sealed-v2-seed1775-size<V>-keys<K>-tcp/`. EFA-native uses its separately prepared `...-tidehunter_efa_native/` owner block. Native uses its separate `...-native/` local dataset. Equal logical values do not imply identical physical layout, devices or bytes read.
- Distributed source receipts identify `kv-research-native-sealed-batch-v2`, maximum 32 preload members; the owner V3 frame provides independently addressed/checksummed members. In contrast, frozen native dataset preparation config has batch_size 32 but its actual generic preload loop dispatches **one Put per key**, `S/benchmarks/kv-research/src/runner.rs:817–900`. Native source physical layout is not a controlled clone of the owner-batched layout. This is a comparison-boundary caveat, not evidence of a wrong value or an accidental 32-key read amplification.
- Native/TCP/EFA `first_touch` jobs request mapped-WAL eviction before gate and OS cache drop at gate; receipts report `completed_before_gate` and `os_caches_dropped=true`. They still use **buffered WAL**, can share pages/readahead and warm Linux pagecache during measurement. `engine_caches_evicted=null` for these eviction cases does not prove an engine cache flush; explicit distributed value-cache limits and measured hits are separately zero.
- Their `repeat_read` jobs do not evict/drop and follow full-key verification: OS-pagecache warmth is expected, not disallowed by explicit value-cache limits of zero.
- All NVMe jobs label `initiator_O_DIRECT_target_direct`, target cache `direct`, no OS drop or mapped-WAL eviction. Frozen `namespace.rs:153–162` opens the actual block namespace read-only with O_DIRECT; host receipts identify kernel TCP controllers and protected namespaces, and target setup receipts carry `target_cache=direct`. E.g. `E/cases/0000-.../nvme-host-0.json`, `nvme-targets.json`. This is **Linux NVMe/TCP**, not NVMe/RDMA over EFA. Repeated direct reads are not RAM-cache reads; O_DIRECT does not bypass device cache or establish NAND service time.

### Verified security scopes: do not label NVMe data mutual TLS

| Path | Actual measured metadata and implementation | Meaning |
|---|---|---|
| Native | transport `none`, security `in_process` | Local native API; no network data path |
| Tidehunter/TCP | transport `tcp`, security `mutual_tls`, read backend `owner_record_protocol` | Authenticated/encrypted owner-record RPC path |
| EFA-native | transport `ofi-efa-direct-trusted`, security `trusted_network_no_tls`, read backend `owner_record_protocol` | Separate trusted-network EFA owner-record control; no TLS |
| NVMe/TCP | read backend `linux_nvmeof`, namespace binding `tcp`; generic adapter still says `tcp` / `mutual_tls` | Generic TLS metadata describes the configured owner-RPC control/fallback capability, **not the kernel NVMe/TCP read data path** |

Across all 26 NVMe measured intervals, owner-read RPC delta=0 and ineligible fallback=0. Frozen `S/experiments/nvmeof/campaign_targets.py:161–162,201–202` configures kernel TCP/direct targets and connections. `manage.py:496–520` issues `nvme connect -t tcp ... -n <nqn> -l 0` with host NQN/host ID but **no TLS flags or TLS key setup**; target setup uses an allowed-host NQN list, not mutual TLS. The NVMe read data traffic in this run must not be represented as TLS-protected. **Security differences, buffered versus direct cache policies, and physical layout differences mean this is not pure protocol/transport isolation**, even when the logical workload is exactly matched. No equal-HA/transaction/failover guarantee is implied.

## All 26 NVMe measurement qualifications and bytes

For **every row in this table**: qualification true, no reasons, attempted/drained true, five active readers, reader/counter/stage delta availability true; coordinator explicit-cache hits=0, coordinator owner-read RPC delta=0, fallback=0, I/O errors=0, timeouts=0. Independently checked **eligible_hits = logical_members = io_submissions = io_completions** (column M), and **aligned_requested_bytes = aligned_read_bytes** (column B). These are drained measurement-only counters; full-key verification/lifetime totals were not used. `qualified` establishes this path was used, not a per-call NAND attestation.

| Case | Logical reader requests | M: eligible/member/submission/completion | B: requested/read bytes | B / returned value bytes |
|---|---:|---:|---:|---:|
| 0000 | 8192 | 8192 | 42205184 | 5.031250 |
| 0001 | 8192 | 8192 | 42205184 | 5.031250 |
| 0002 | 8192 | 8192 | 42033152 | 5.0107421875 |
| 0003 | 8192 | 8192 | 42033152 | 5.0107421875 |
| 0004 | 8192 | 8192 | 42205184 | 5.031250 |
| 0005 | 8191 | 8191 | 42024960 | 5.009765625 |
| 0006 | 2048 | 65536 | 337608704 | 5.03076171875 |
| 0007 | 10230 | 65520 | 336736256 | 5.01776123046875 |
| 0008 | 2048 | 65536 | 337608704 | 5.03076171875 |
| 0009 | 10230 | 65477 | 336515072 | 5.01446533203125 |
| 0038 | 8192 | 8192 | 100937728 | 1.50408935546875 |
| 0039 | 8192 | 8192 | 100978688 | 1.50469970703125 |
| 0040 | 8192 | 8192 | 100937728 | 1.50408935546875 |
| 0041 | 8192 | 8192 | 100978688 | 1.50469970703125 |
| 0042 | 2048 | 65536 | 807542784 | 1.5041656494140625 |
| 0043 | 10230 | 65520 | 807358464 | 1.5038223266601562 |
| 0044 | 2048 | 65536 | 807542784 | 1.5041656494140625 |
| 0045 | 2048 | 65536 | 807542784 | 1.5041656494140625 |
| 0046 | 10230 | 65470 | 806744064 | 1.5026779174804688 |
| 0047 | 10230 | 65469 | 806731776 | 1.502655029296875 |
| 0076 | 8192 | 8192 | 34340864 | 65.500000 |
| 0077 | 8192 | 8192 | 34312192 | 65.4453125 |
| 0084 | 8192 | 8192 | 35913728 | 17.125000 |
| 0085 | 8192 | 8192 | 35934208 | 17.134765625 |
| 0092 | 8192 | 8192 | 570425344 | 1.062500 |
| 0093 | 8192 | 8192 | 570425344 | 1.062500 |

Denominator in last column is `metrics.populations[0].logical_keys * job.value_size`, i.e. successful returned payload, not aligned member reads. Small member deficits (e.g. 0005: 8191 physical members for 8192 scalar successes; 0046: 65470 for 65536 returned keys) are compatible with source-supported duplicate/inflight singleflight coalescing, **not evidence of a lost API sample**. Frozen `pending_read.rs:2794–2816` deduplicates references and counts coalesced members. This audit does not claim a fully independently attributed per-call mapping for those deficits.

## NVMe stage count/sum weighted means, measurement only

Source: **`.measurement_read_evidence.observed.nvmeof.readers[].stages[stage].delta.counters`**, in each read case. Readers are keyed objects, not a JSON array. For each stage independently: `N = sum(reader.delta.count)`, `S = sum(reader.delta.sum_ns)`, **weighted mean = S / N**, not the unweighted average of reader means. Every cell below is **N / S(ns) → weighted mean(µs)**; exact counts/sums are retained so rounding is reversible. Lifetime maxima and full-key verification were not substituted; no per-stage percentile or measurement-window maximum is available.

| Case | Alignment | Admission | Queue | Submit→CQ observed | Extraction | Verification |
|---|---|---|---|---|---|---|
| 0000 | 16384/3177583→0.194 | 8192/3869370→0.472 | 8192/5525554→0.675 | 8192/1979086236→241.588 | 8192/1500265→0.183 | 8192/9368322→1.144 |
| 0001 | 16384/5619756→0.343 | 8192/5602942→0.684 | 8192/7627785→0.931 | 8192/1980838388→241.802 | 8192/1841257→0.225 | 8192/11078189→1.352 |
| 0002 | 16384/6773792→0.413 | 8192/6559815→0.801 | 8192/9417153→1.150 | 8192/2706033573→330.326 | 8192/2511805→0.307 | 8192/13880086→1.694 |
| 0003 | 16384/4004340→0.244 | 8192/4427403→0.540 | 8192/6277219→0.766 | 8192/2723978845→332.517 | 8192/1755380→0.214 | 8192/10072445→1.230 |
| 0004 | 28272/7610624→0.269 | 8192/5151591→0.629 | 8192/90150976→11.005 | 8192/3108040715→379.400 | 8192/2068512→0.253 | 8192/28513835→3.481 |
| 0005 | 31343/8395384→0.268 | 8191/5539995→0.676 | 8191/167292444→20.424 | 8191/2585123840→315.605 | 8191/2100200→0.256 | 8191/34768058→4.245 |
| 0006 | 327680/54067020→0.165 | 2048/20495360→10.008 | 65536/1160049211→17.701 | 65536/59011276167→900.441 | 65536/9778552→0.149 | 2048/31304423→15.285 |
| 0007 | 333916/48768922→0.146 | 10230/24357299→2.381 | 65520/1344362549→20.518 | 65520/23361778030→356.559 | 65520/11974848→0.183 | 10230/50165173→4.904 |
| 0008 | 767584/136620962→0.178 | 2048/25036058→12.225 | 65536/10660095185→162.660 | 65536/84959671634→1296.382 | 65536/15674447→0.239 | 2048/117740631→57.491 |
| 0009 | 1298013/228556783→0.176 | 10230/25553728→2.498 | 65477/2109872999→32.223 | 65477/29254259158→446.787 | 65477/18189675→0.278 | 10230/194504239→19.013 |
| 0038 | 16384/5748334→0.351 | 8192/6142374→0.750 | 8192/9401249→1.148 | 8192/1630326076→199.014 | 8192/2863888→0.350 | 8192/18080985→2.207 |
| 0039 | 16384/5897744→0.360 | 8192/5958342→0.727 | 8192/8834426→1.078 | 8192/2278299959→278.113 | 8192/1885951→0.230 | 8192/19049290→2.325 |
| 0040 | 27606/7653690→0.277 | 8192/5308776→0.648 | 8192/127558925→15.571 | 8192/3459334258→422.282 | 8192/2301555→0.281 | 8192/34797029→4.248 |
| 0041 | 28835/7596801→0.263 | 8192/5275763→0.644 | 8192/284933869→34.782 | 8192/2622406150→320.118 | 8192/2175432→0.266 | 8192/38548079→4.706 |
| 0042 | 327680/55946527→0.171 | 2048/22297095→10.887 | 65536/2362785234→36.053 | 65536/72983225870→1113.636 | 65536/10046297→0.153 | 2048/71483563→34.904 |
| 0043 | 330679/50853036→0.154 | 10230/26174032→2.559 | 65520/1767256257→26.973 | 65520/28195709258→430.337 | 65520/16221007→0.248 | 10230/116236447→11.362 |
| 0044 | 752064/139821370→0.186 | 2048/27952142→13.649 | 65536/26949396350→411.215 | 65536/116892584252→1783.639 | 65536/16926324→0.258 | 2048/217747897→106.322 |
| 0045 | 759456/137454404→0.181 | 2048/27112557→13.239 | 65536/22902235643→349.460 | 65536/117595202015→1794.360 | 65536/15595599→0.238 | 2048/200807592→98.051 |
| 0046 | 1279127/226476236→0.177 | 10230/27333287→2.672 | 65470/2893303069→44.193 | 65470/33864066547→517.246 | 65470/20828655→0.318 | 10230/256152162→25.039 |
| 0047 | 1276525/226174172→0.177 | 10230/27458418→2.684 | 65469/2766336264→42.254 | 65469/35245522726→538.354 | 65469/20236893→0.309 | 10230/264623840→25.867 |
| 0076 | 16384/3031477→0.185 | 8192/3556600→0.434 | 8192/5834992→0.712 | 8192/2278757465→278.169 | 8192/1510786→0.184 | 8192/9623048→1.175 |
| 0077 | 16384/3249397→0.198 | 8192/3848363→0.470 | 8192/6279108→0.766 | 8192/2000196515→244.165 | 8192/1415954→0.173 | 8192/9109735→1.112 |
| 0084 | 16384/6031180→0.368 | 8192/6383878→0.779 | 8192/8035989→0.981 | 8192/2343300904→286.047 | 8192/1601388→0.195 | 8192/12387595→1.512 |
| 0085 | 16384/4812222→0.294 | 8192/4855190→0.593 | 8192/7417383→0.905 | 8192/2009102657→245.252 | 8192/1543950→0.188 | 8192/9707966→1.185 |
| 0092 | 16384/7125535→0.435 | 8192/6904718→0.843 | 8192/10795489→1.318 | 8192/2999735449→366.179 | 8192/2379517→0.290 | 8192/65111940→7.948 |
| 0093 | 16384/6337985→0.387 | 8192/6435521→0.786 | 8192/10274252→1.254 | 8192/3719585769→454.051 | 8192/1493957→0.182 | 8192/62017955→7.571 |

Interpretation of counts: alignment includes charge/probe/retry observations (e.g. 0046 has 1,279,127, not 65,536). Admission and verification are per accepted reader request/flight; queue, submit→CQ and extraction are per physical member read. A 32-key group can have one same-owner request in the sweep, whereas random groups fan across owners (10,230 reader requests for 2,048 API calls). **Do not sum these means into an API critical path, divide API percentiles by 32, subtract percentiles, or treat overlapping SQEs as serial service.**

## Supported costs, frames and amplification

1. **The largest directly measured NVMe component is submission→CQ observation**, not checksum or extraction. Scalar baseline examples: 0000 241.588 µs, 0002 330.326 µs, 0038 199.014 µs, 0039 278.113 µs; extraction is 0.183–0.350 µs in those rows and verification 1.144–2.325 µs. At c16 the observed SQE queue component grows: 0040 15.571 µs; 0041 34.782 µs. These intervals include client-kernel/transport/target/storage plus userspace progress/CQE observation; they cannot isolate wire, target software or NAND.
2. **The c16 batch penalty is real in these observations but not fully attributed.** 0044 API mean 19,499.716 µs and p99 76,649.301 µs; per-member submit→CQ mean 1,783.639 µs, queue 411.215 µs, per-flight verification 106.322 µs. 0046 API mean 29,265.370 µs, p99 67,388.982 µs, while per-member submit→CQ mean is 517.246 µs. Different counts, concurrent SQEs, owner fanout and admission/progress scopes prohibit obtaining an unexplained residual by subtraction. There is no evidence here assigning the entire API regression to the device, link or one lock.
3. **NVMe directly reads each independently checksummed member, not a complete 32-value WAL frame.** Frozen `S/tidehunter/src/distributed/codec.rs:27–33,459–498,519–538` gives 8-byte member header + 4-byte native Put header + 32-byte key = **value + 44 bytes**. `reader.rs:285–320` aligns each member separately to 4 KiB. `reader.rs:384–411` issues one `opcode::Read` and aligned allocation per reference: no adjacent-range read coalescing in that loop. Buffer extraction retains the aligned allocation and slices the exact member; it does not copy sibling values (`reader.rs:556–568`). This is source-supported mechanism, not an optimization proposal or an allocation-time measurement.
4. **The measured amplification is substantial for small values.** First-touch scalar direct bytes/payload: 64 B **65.5×** (34,340,864 / 524,288), 256 B **17.125×** (35,913,728 / 2,097,152), 1 KiB **5.03125×** (42,205,184 / 8,388,608), 8 KiB **1.50408935546875×** (100,937,728 / 67,108,864), 64 KiB **1.0625×** (570,425,344 / 536,870,912). These are completed aligned namespace bytes, not measured NAND bytes. Alignment overlap can reread neighboring member/page bytes; kernel/device merging and device-cache effects are not individually measured.
5. **Owner TCP/EFA read-byte counters confirm member reads, not 32× sibling payloads.** Matching first-touch scalar rows 0010/0028: 8,192 WAL reads / **8,749,056 bytes** (=8192×1068); 0048/0066: **67,469,312 bytes** (=8192×8236); 0078/0082: **884,736 bytes**; 0086/0090: **2,457,600 bytes**; 0094/0098: **537,231,360 bytes**. Those are logical buffered WAL read bytes, not physical I/O; comparing them directly with NVMe aligned bytes does not prove the same backing-device amplification.
6. Completion consumption was traced in the **frozen** source, not inferred from the producer: `pending_read.rs:1123–1179` consumes every NVMe completion, checks member count, verifies/decodes each record and completes the request; `:1485–1501` invokes progress in the driver loop. No silent unmatched dispatch/drop was found in that path.
7. Supplemental diagnostic example (first 256 physical flights, **not full-population means**): 0011 TCP exchange 152.69409375 µs, inline 153.56594921875 µs, server async 32.3173046875 µs, server executor residence 9.56421484375 µs, server handler 39.12503515625 µs, request encode 1.15276953125 µs, response decode 0.8155625 µs, record verify 0.961375 µs. 0029 EFA exchange 95.489046875 µs, server async 42.981125 µs, executor residence 4.84460546875 µs, handler 44.74226171875 µs; transport CQ-call mean **0.0405790663 µs over 70,365 local calls**, not per API or wire latency. 0001 selected NVMe-flight mean **429.46179296875 µs over 256 flights**, deliberately different from the full-window 241.80156103515625 µs per-member submit→CQ mean. These overlapping/prefix-selected stages cannot be summed or used for cross-host timestamp subtraction. Raw: each diagnostic `receipt.json` → `read_diagnostics.observed.profile.stages` and its stage-specific count/sum.

## Remaining defects and limits

### Persisting evidence defects

- **Monitoring/client resource gap remains in all 26 NVMe rows.** Each `resource_evidence.complete=false`; the client entry is unavailable with `owned process counters unavailable at a boundary`. Storage process samples exist, but process CPU, memory and cgroup-I/O coverage for the whole NVMe comparison is not complete. `E/observability/receipt.json.complete=false` independently agrees. Do not claim campaign complete, zero client CPU/I/O, or equal per-path resource cost. The monitoring owner is responsible for any code fix; it cannot retrospectively make this measured run complete.
- **All four NVMe diagnostic trace sets have losses**, so target/block traces do not support complete per-call device attribution. Independently summed receipt values:

| Diagnostic case | overrun_or_dropped | paired block count | paired nvmet count | unmatched block completions | unmatched nvmet completions |
|---|---:|---:|---:|---:|---:|
| 0001 | 1507 | 7856 | 7815 | 0 | 1 |
| 0003 | 5720 | 6800 | 6764 | 1 | 3 |
| 0045 | 134493 | 31918 | 31816 | 178 | 199 |
| 0047 | 134079 | 31942 | 31885 | 102 | 127 |

`nvmeof_trace_evidence[].complete=true` is capture/cleanup/upload workflow completion, **not trace completeness**. Counted loss/overrun diagnostics must not be called missing API calls or a precisely correlated lost-request count. Raw S3 object keys are retained verbatim in each diagnostic receipt's `nvmeof_trace_evidence[].traces[].files[]`; they are not all copied into the controller extraction. Example 0001 owner 0: `th-b1f973adbbdae436/results/cells/0000-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-baseline/storage/0/nvmeof/trace-1e57635ee31cdb48/export-000/{trace.txt,summary.json,per_cpu_stats.json}`. Reused target cell ID 0000 belongs to the source/export lifecycle; the enclosing **diagnostic receipt ID** and trace generation identify the timed diagnostic. Do not relabel that trace as a baseline measurement.

### No additional demonstrated value-correctness defect

No wrong-value, unsuccessful API sample, binary/source admission failure, unexpected explicit-cache hit, NVMe fallback/error/timeout, interval reset/unavailable delta, or silently unconsumed NVMe completion was demonstrated in the audited frozen evidence. Native physical-layout and buffered/direct differences are substantive **fairness boundaries** to preserve, not grounds for pretending the paths were held identical. The 26 qualification successes do not prove every successful API call issued its own independent device operation, because singleflight/duplicate coalescing exists.

No sustainable-aging, Sui application/checkpoint, HA, transaction, NVMe/RDMA, network-only latency, NAND latency, independently repeated-trial or statistical-significance claim follows from this run. Writes were outside this read audit and remain the existing native/RF1 owner WAL path; there is no new NVMe writer in these results.
