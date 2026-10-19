# Local native / sharded-WAL benchmark results

Date: 6 October 2026. **Two completed release runs; local RF1 only.**

Two fresh 72-case campaigns are retained: sections 1–6 describe the first serving
baseline; section 7 records the compatible digest-provider/borrowed-key candidate.
**The follow-up has no demonstrated overall performance win.** Correctness passes,
but relative ratios are mixed and the unchanged native timings also drifted.

The new backend serves actual put/delete/get/exists/scan/checkpoint requests and
passes every reopen audit. It **does not yet match native performance** in this
small, warm local workload. This run establishes a reproducible baseline for the
new native-fork implementation; it cannot establish improvement over the earlier
middleware architecture or predict the independent-NVMe cluster ratio.

## 1. Exact evidence and comparison contract

- [Implementation/source map](IMPLEMENTATION_LOCAL_DB_2026-10-06.md) and
  [benchmark measurement contract](LOCAL_BACKEND_BENCHMARK_2026-10-06.md).
- [Raw per-call samples](../../logs/local-db-2026-10-06/release-campaign/samples.csv),
  [phase summaries](../../logs/local-db-2026-10-06/release-campaign/summary.csv),
  [stage counters](../../logs/local-db-2026-10-06/release-campaign/stages.csv),
  [derived comparison medians](../../logs/local-db-2026-10-06/release-campaign/phase-comparison-medians.csv).
- [Configuration](../../logs/local-db-2026-10-06/release-campaign/campaign.txt),
  [COMPLETE marker](../../logs/local-db-2026-10-06/release-campaign/COMPLETE),
  [stdout](../../logs/local-db-2026-10-06/benchmark-release.stdout.log),
  [resource log](../../logs/local-db-2026-10-06/benchmark-release.stderr.log),
  [build log](../../logs/local-db-2026-10-06/build-release-benchmark.log).
- [Source/binary/resource manifest](../../logs/local-db-2026-10-06/release-source-manifest.json):
  branch `feat/sharded-value-wal`, HEAD `13a16ca8bd632494fc6368f56d60d0806be4a869`
  **plus the recorded uncommitted source hashes**. All source hashes still matched
  immediately after this run. Later optimizations require another manifest.
  Binary SHA-256:
  `0873a2f6c3d28ff5f6811a73c3703fee43e6c2d45f4055a2ab151ff99f35ec5e`.

The release campaign completed at **2026-10-06 08:20:40 UTC**, exit status zero:
**72 cases = 3 repetitions × 8 workload shapes × 3 backend modes**. Each shape
uses 128 load batches, 32 mixed read/update batches, 32 delete batches, 512 point
queries per read/existence phase, a checkpoint mutation, a complete ordered scan,
index flush/unload, and a full reopen audit. There are 1,296 phase summary rows.

`native_value_wal_barrier` is ordinary native Db plus the feature-gated Value WAL
file/directory barrier at each window. `experimental_local_authority` is
`experimental-native-cell-sharded-value-wal-local-rf1`, with five storage workers,
one index/publication owner and native Value WAL files. `native_async_reference`
uses native foreground page-cache/index acknowledgement: its much smaller write
latency has a different durability contract. No replicas, Sui, TCP or RDMA ran.

Both persistence comparisons finish all writes in each window before returning.
Native synchronizes its registered Value WAL file(s); each experimental group
synchronizes its containing file and directory. Native history stays within the
configured 128 MiB Value WAL file. The differing barrier counts are measured work,
not silently normalized away. Native gets 15 one-MiB value mappings, matching the
five experimental partitions' aggregate 15 mappings. Value caches/compression are
disabled; both use the same native cell routing. Index formats/caches differ.

### Resources

All databases used the same mounted **CT1000P510SSD8 NVMe**, ext4 on
`/dev/nvme0n1p3`, with approximately 31 GiB RAM. Case data was created below the
result directory and removed after each case; it did not accumulate hundreds of
GB. The largest case contains approximately 40 MiB of value history.

The complete campaign took **12.96 seconds**, with **6.20 s user / 3.00 s system**
CPU and **130,524 KiB peak RSS** (about 127.5 MiB). System `pswpin` and `pswpout`
deltas were both **zero**. GNU time recorded zero major faults for this process,
120,679 voluntary and 6,790 involuntary context switches. Its file-system-output
counter was 3,625,056; that counter is retained without a speculative conversion
to physical bytes. System-wide major faults rose by 2,067 while the benchmark
process had zero; those are not evidence of benchmark swap or cold reads.
System disk counters are preserved, but are not isolated write amplification.

### Statistics

Each table uses the **median of the three repetition statistics for the same
case**. A latency ratio is the ratio of those medians: **above 1 means slower** for
the experimental backend. A throughput ratio above 1 means faster. These are small
samples and short runs, not confidence intervals. The comparison CSV preserves
all mean/p50/p95/p99/max, API/phase time, throughput, and repetition min/max means.

`KiB / batch / window` below means value size, mutations per batch, and admitted
batches per completed window. Window-eight latency is for **all eight batches**;
it is not a per-batch or per-mutation tail latency. With 128 load batches there
are 128 window-one samples or only 16 window-eight samples per repetition. The
mixed/delete window-eight rows have only four windows. Their p99 is effectively
a maximum and must not be presented as a stable production tail estimate.

## 2. Durable writes: still slower, with a visible grouping penalty

| KiB / batch / window | Native mean µs | New mean µs | New/native | p99 µs native → new | Instrumented mutation/s native → new |
| --- | --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 518.25 | 772.08 | 1.49× | 1,131.4 → 1,146.8 | 1,927 → 1,294 |
| 1 / 1 / 8 | 584.89 | 2,621.1 | 4.48× | 1,141.6 → 3,642.3 | 13,630 → 3,051 |
| 1 / 32 / 1 | 696.69 | 1,075.5 | 1.54× | 1,401.4 → 1,756.4 | 45,615 → 29,708 |
| 1 / 32 / 8 | 1,348.6 | 3,455.3 | 2.56× | 1,909.1 → 4,619.9 | 187,766 → 73,893 |
| 8 / 1 / 1 | 522.70 | 1,095.0 | 2.09× | 934.23 → 1,946.3 | 1,911 → 913 |
| 8 / 1 / 8 | 682.55 | 2,933.6 | 4.30× | 1,173.3 → 4,010.8 | 11,683 → 2,726 |
| 8 / 32 / 1 | 1,145.6 | 2,351.8 | 2.05× | 1,790.4 → 3,824.8 | 27,785 → 13,598 |
| 8 / 32 / 8 | 3,215.6 | 9,316.4 | 2.90× | 3,744.0 → 10,960.9 | 79,375 → 27,456 |

Window-one mean load latency is **1.49–2.09×** native; window-eight is
**2.56–4.48×** native. Increasing the window improves experimental throughput, but
the native log amortizes its barrier over all eight batches while the five logs
split each window across several independently persisted groups on one SSD.

| KiB / batch / window | Mixed write µs native → new | Ratio | Delete µs native → new | Ratio |
| --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 500.04 → 766.87 | 1.53× | 522.59 → 729.87 | 1.40× |
| 1 / 1 / 8 | 713.36 → 2,770.3 | 3.88× | 632.33 → 2,596.9 | 4.11× |
| 1 / 32 / 1 | 674.04 → 1,065.9 | 1.58× | 563.86 → 880.51 | 1.56× |
| 1 / 32 / 8 | 1,319.3 → 3,588.6 | 2.72× | 968.00 → 2,690.9 | 2.78× |
| 8 / 1 / 1 | 738.03 → 868.27 | 1.18× | 577.48 → 800.02 | 1.39× |
| 8 / 1 / 8 | 742.37 → 3,062.4 | 4.13× | 686.29 → 2,851.5 | 4.16× |
| 8 / 32 / 1 | 1,197.6 → 2,139.9 | 1.79× | 672.59 → 757.01 | 1.13× |
| 8 / 32 / 8 | 3,092.5 → 9,670.0 | 3.13× | 901.89 → 2,819.0 | 3.13× |

The mixed phase contains an equal number of logical gets and mutations. Its read
and write rows share one phase denominator; adding or comparing their rates as
independent campaigns would be incorrect. Single-key checkpoint-intervening
writes are reported in the complete phase comparison below.

### Which measured write stages dominate?

The following stages are median **total milliseconds for all 128 load batches**.
Each column is a separately aggregated counter; medians need not sum perfectly.
The last column is explicit persistence-call count, native → experimental.

| KiB / batch / window | New validation ms | New dispatch ms | New caller wait ms | New publication ms | Native barrier ms | Barrier calls |
| --- | --- | --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 1.21 | 0.34 | 95.81 | 1.29 | 64.33 | 128 → 128 |
| 1 / 1 / 8 | 1.31 | 0.15 | 39.93 | 0.32 | 8.68 | 16 → 82 |
| 1 / 32 / 1 | 12.08 | 0.42 | 118.37 | 6.49 | 82.36 | 128 → 128 |
| 1 / 32 / 8 | 11.10 | 0.26 | 39.81 | 3.92 | 14.42 | 16 → 82 |
| 8 / 1 / 1 | 5.38 | 0.68 | 127.35 | 1.29 | 64.68 | 128 → 128 |
| 8 / 1 / 8 | 3.22 | 0.13 | 42.39 | 0.39 | 9.88 | 16 → 82 |
| 8 / 32 / 1 | 69.89 | 0.49 | 224.82 | 6.04 | 121.73 | 128 → 128 |
| 8 / 32 / 8 | 68.23 | 0.29 | 75.99 | 4.24 | 26.49 | 16 → 99 |

The experiments directly establish several costs:

1. **Barrier/group fragmentation.** At window eight native performs 16 explicit
   barriers for 128 batches. The experimental backend performs a median 82, or 99
   for 8 KiB × 32 mutations. The five partitions share one device and one filesystem;
   these are not five independent durability engines. A larger cluster may execute
   the independent groups on separate NVMe devices, but this run does not quantify
   that gain. Group counts exclude bootstrap, sealing and native background syncs.
2. **Authority-side preparation at larger payloads.** For 8 KiB × 32/window eight,
   validation accounts for about **68.2 ms** of **149.1 ms** measured load API time
   (about 46%). It includes batch construction/encoding, digest generation,
   validation, routing and admission. The single authority performs it before
   dispatch. More storage disks cannot remove that CPU work.
3. **Publication is a smaller measured component.** For that same case publication
   is about **4.25 ms** and dispatch **0.29 ms**. This is not evidence that the shared
   logical counter or a publication mutex is the present dominant cost. The local
   backend has one mutable owner rather than many contending frontend callers.
4. **Caller wait is not device service time.** `storage_wait_ns` includes queueing,
   worker progress, group delay, persistence and completion waits. Workers overlap
   one another and some authority preparation. No counter here separately measures
   fsync duration, per-worker queue delay or network latency. Attributing the entire
   wait to the SSD or coordination would exceed the instrumentation.

## 3. Warm reads: the value path is the clearest local CPU target

| KiB / batch / window | Native mean µs | New mean µs | Ratio | p50 µs native → new | p95 µs native → new | p99 µs native → new |
| --- | --- | --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 0.517 | 2.781 | 5.38× | 0.427 → 2.633 | 0.864 → 3.726 | 1.138 → 8.715 |
| 1 / 1 / 8 | 0.470 | 1.810 | 3.85× | 0.409 → 1.663 | 0.757 → 2.559 | 1.068 → 2.920 |
| 1 / 32 / 1 | 0.803 | 2.145 | 2.67× | 0.783 → 2.059 | 1.121 → 2.436 | 1.350 → 3.652 |
| 1 / 32 / 8 | 0.901 | 2.505 | 2.78× | 0.874 → 2.298 | 1.238 → 3.164 | 1.493 → 6.658 |
| 8 / 1 / 1 | 1.056 | 8.284 | 7.84× | 0.817 → 7.977 | 1.999 → 10.35 | 2.479 → 12.96 |
| 8 / 1 / 8 | 0.955 | 8.838 | 9.25× | 0.810 → 8.640 | 1.538 → 11.36 | 1.751 → 14.34 |
| 8 / 32 / 1 | 2.020 | 9.634 | 4.77× | 1.978 → 9.493 | 2.690 → 10.57 | 7.137 → 13.84 |
| 8 / 32 / 8 | 1.884 | 9.673 | 5.13× | 1.873 → 9.538 | 2.461 → 10.50 | 3.491 → 14.61 |

Warm point-get mean latency is **2.67–9.25×** native. For 8 KiB, the measured new
index-resolution region is roughly **0.23–0.48 µs per get**, while value resolution
is about **8.0–9.1 µs**. This is a warm RAM/page-cache result: zero process major
faults, no cache drop, and a small dataset. It does not measure an NVMe miss.

The code gives a plausible, specific explanation to test:

- `local_db.rs::resolve` finds the epoch/partition and calls the storage reader.
- `storage.rs::read_record` proves exact immutable membership, reads only the
  requested member, and calls `CompleteBatch::decode_record`.
- `codec.rs::member_digest` computes **BLAKE2b-256 over that record and its identity,
  version and descriptor**. Native `crc.rs` uses `crc32fast` on its frame.
- Both mapped native and experimental readers copy the requested bytes into an
  owned buffer. A copied buffer therefore is not uniquely experimental overhead.

The combined value-read region contains membership, source selection, copying,
digest computation and decode. The data strongly identifies that region as a
local bottleneck; it does **not isolate hashing as the entire cost**. Digest and
membership checks must remain correct. A next optimization should first measure
these pieces, then consider a faster implementation of the same digest, fewer
encodings/copies or a safe verified resident-value representation. Replacing the
integrity contract requires an explicit format/security decision, not disabling
verification to improve a chart.

The instrumentation also differs: experimental reads collect extra `Instant`
stage measurements inside the API, while native reads have only the driver's
outer timing. That overhead matters for sub-microsecond misses/exists. Treat
those ratios as instrumented backend costs, not a proven lower bound on an
uninstrumented implementation.

## 4. Every measured phase, including maintenance and recovery

Each range below covers the eight shapes' **median mean API latency** in µs.
Ratio ranges use matched cases, rather than dividing unrelated range endpoints.
The derived comparison CSV contains all 144 case/phase rows, including every
backend's median p50/p95/p99/max and throughput and the repetition mean range.

| Phase | Native µs range | New µs range | Matched new/native ratio range |
| --- | --- | --- | --- |
| `open` | 2,338.3–2,688.7 | 22,135.2–24,268.2 | 8.629–9.934× |
| `load_write_window` | 518.25–3,215.6 | 772.08–9,316.4 | 1.490–4.481× |
| `point_get` | 0.470–2.020 | 1.810–9.673 | 2.670–9.252× |
| `point_get_missing` | 0.107–0.195 | 0.136–0.324 | 0.976–2.553× |
| `exists` | 0.071–0.144 | 0.207–0.328 | 2.064–3.546× |
| `exists_missing` | 0.061–0.148 | 0.143–0.244 | 1.547–2.431× |
| `mixed_get` | 0.613–4.636 | 1.740–15.27 | 2.028–6.465× |
| `mixed_write_window` | 500.04–3,092.5 | 766.87–9,670.0 | 1.176–4.125× |
| `checkpoint_capture` | 12.99–179.58 | 0.279–0.933 | 0.002–0.035× |
| `checkpoint_intervening_write` | 637.65–809.62 | 754.78–877.19 | 0.971–1.365× |
| `checkpoint_get` | 3.060–6.233 | 3.664–18.02 | 1.194–5.096× |
| `delete_window` | 522.59–968.00 | 729.87–2,851.5 | 1.126–4.155× |
| `full_scan` | 176.56–7,398.1 | 213.47–33,048.7 | 1.195–4.681× |
| `maintenance_flush_index` | 1,700.4–5,414.9 | 1,041.4–5,056.1 | 0.531–1.169× |
| `post_flush_get` | 0.591–2.457 | 1.329–7.376 | 1.210–8.706× |
| `shutdown` | 528.13–995.78 | 9,814.0–11,641.7 | 10.112–20.980× |
| `reopen` | 1,276.5–1,793.0 | 31,418.4–203,556.8 | 17.523–142.925× |
| `reopen_get_audit` | 0.844–2.263 | 1.437–8.029 | 1.014–5.702× |

Interpretation of the other phases:

- **Missing reads and existence** avoid value retrieval. The new index-only
  `exists` fast path works, including after the native routing decision. Their
  absolute costs are small, although instrumented ratios remain above native in
  most shapes. No WAL fetch is needed for full-key-indexed existence.
- **Scans** verify all live keys and values, so value verification cost accumulates.
  The typed index uses a heap merge across cells; the current measurements do not
  isolate merge cost from value fetching. The large cases return 3,072 live keys.
- **Checkpoint capture** is very cheap because it captures a published logical
  cut while every historical version remains retained. Native checkpoint capture
  has different bookkeeping. A lower number here does not establish a cheaper
  sustained checkpoint design: bounded retained history will eventually fill.
- **Maintenance** compares native control-region rebuild with a derived typed
  reference-image flush/unload. These are different recovery representations.
  Post-flush gets are verified but still have a warm OS page cache.
- **Shutdown** closes five independently owned worker/WAL resources plus inventory;
  **reopen** seals/enumerates/replays all retained WALs and creates a new epoch.
  New reopen is **31.4–203.6 ms**, versus native **1.28–1.79 ms**, yielding
  **17.5–142.9×** matched ratios. Native has a control-region replay cut; the new
  backend does not yet consume its images as startup checkpoints. This is a known
  recovery-work difference and a concrete next implementation target.
- **Reopen audit** verifies all original keys, including absence after deletion.
  Its API means are distinct from phase throughput, which also generates expected
  payload bytes and checks them. Every audit passed.

## 5. Native asynchronous reference: preserve the acknowledgement distinction

For completeness, native asynchronous load latency is included below. These are
window means in µs. The last column compares the new durable result to native's
page-cache acknowledgement and therefore **is not a same-durability gap**.

| KiB / batch / window | Native async µs | New durable µs | Different-contract ratio |
| --- | --- | --- | --- |
| 1 / 1 / 1 | 3.534 | 772.08 | 218.5× |
| 1 / 1 / 8 | 20.62 | 2,621.1 | 127.1× |
| 1 / 32 / 1 | 61.92 | 1,075.5 | 17.4× |
| 1 / 32 / 8 | 435.21 | 3,455.3 | 7.9× |
| 8 / 1 / 1 | 4.143 | 1,095.0 | 264.3× |
| 8 / 1 / 8 | 33.01 | 2,933.6 | 88.9× |
| 8 / 32 / 1 | 195.10 | 2,351.8 | 12.1× |
| 8 / 32 / 8 | 1,536.0 | 9,316.4 | 6.1× |

Native's default fast acknowledgement is central to the paper's design. Claiming
that the new backend matches that speed while waiting for file persistence would
conflate different guarantees. A future optional process-crash/page-cache mode
would need an explicit API contract, correct unknown-outcome/recovery behavior and
its own matched comparison. No such faster acknowledgement mode was fabricated
for this run.

## 6. What to optimize next, with the invariants retained

1. **Measure and reduce authority encoding/digest work.** Add narrow counters for
   per-member hashing, complete-batch hashing, buffer construction and admission
   scans. Preserve immutable batch identity, exact descriptors and the
   before-authority validation boundary. Prefer changing the implementation of a
   proven representation before changing what makes a record authoritative.
2. **Measure the warm value-read pieces.** Keep exact reference membership,
   generation/lifetime checks and independent record integrity. Benchmark a
   compatible optimized digest implementation and avoid unnecessary re-encoding;
   do not infer a need for remote storage from a local CPU verification cost.
3. **Improve grouping under the measured latency budget.** Examine per-partition
   queue/persistence service time and batch placement/group occupancy. Whole atomic
   batches must stay complete, retries must keep their original placement, and a
   lower logical gap must still block publication. Increasing wait time simply to
   lower fsync count needs latency measurements, not only throughput.
4. **Implement certified index-image recovery.** Reuse a persisted logical cut and
   explicit complete stream inventory to reduce replay. Validate crashes around
   image/manifest publication before allowing old history to be skipped. History
   reclamation is a later proof involving readers, snapshots and replicas.
5. **Then add and measure real TCP nodes.** The independent-NVMe test can answer
   whether concurrent per-node barriers reduce the local shared-device penalty.
   It will also add transport cost. The already visible authority CPU and warm
   integrity costs will remain unless optimized; faster RDMA cannot remove them.

These results support continuing focused local work before paying for a cluster.
They do not justify a promise of native-speed parity or a change to a second
value store/LSM tree. The existing values-in-WAL architecture now has an honest
serving baseline and measurable costs; network, replica, retention and Sui gates
remain explicit in [NEXT_STEPS](NEXT_STEPS_2026-10-06.md).

## 7. Follow-up candidate: compatible digest provider and borrowed keys

A second fresh release campaign completed at **08:36:23 UTC**, again with all
**72 cases and reopen audits passed**, source hashes unchanged during the run:

- [Candidate manifest](../../logs/local-db-2026-10-06/optimized-source-manifest.json),
  [raw samples](../../logs/local-db-2026-10-06/optimized-campaign/samples.csv),
  [phase summaries](../../logs/local-db-2026-10-06/optimized-campaign/summary.csv),
  [stages](../../logs/local-db-2026-10-06/optimized-campaign/stages.csv),
  [all-phase medians](../../logs/local-db-2026-10-06/optimized-campaign/phase-comparison-medians.csv),
  [before/after comparison](../../logs/local-db-2026-10-06/candidate-change-comparison.csv).
- [COMPLETE](../../logs/local-db-2026-10-06/optimized-campaign/COMPLETE),
  [stdout](../../logs/local-db-2026-10-06/benchmark-optimized.stdout.log),
  [resource log](../../logs/local-db-2026-10-06/benchmark-optimized.stderr.log),
  [fresh release build](../../logs/local-db-2026-10-06/build-release-benchmark-optimized.log)
  (16.14 s), and [final offline locked library/examples check](../../logs/local-db-2026-10-06/check-final-lib-examples.log).
- Binary SHA-256:
  `5c67d124222f4fe87dcee03b33cc10b829ba684a4d31f67b29bd619fd12468f3`.

The candidate contains **two changes**: experimental batch/member BLAKE2b-256
uses the cached `blake2b_simd` **1.0.3** provider, and experimental native cell
routing borrows fixed/variable/reduced keys through `Cow` where possible. The
provider offers runtime SIMD selection plus a portable fallback; that is a
library capability, not proof this campaign is faster.
[Primary provider implementation](https://github.com/oconnor663/blake2_simd).

Only the newly added experimental payload codec uses the new optional dependency.
Ordinary native `crc.rs` still uses CRC32, ordinary optional key hashing remains
unchanged, and the experimental index's schema hash still uses RustCrypto. The
complete batch/member formats and 256-bit digest outputs are unchanged. Two added
tests compare provider outputs across block/update boundaries and verify exact
v2 batch/member bytes against the previous implementation. The final feature
library passes **484 tests**, with six ignored, in **25.86 s**.
[Feature test log](../../logs/local-db-2026-10-06/test-feature-lib-optimized.log).

The whole run took **15.82 s**, **8.44 s user / 4.38 s system**, and **125,656 KiB
peak RSS** (about 122.7 MiB). Swap-in/out deltas and process major faults were zero.
The full campaign is longer than the baseline's 12.96 s; peak RSS is smaller but
these are whole-process observations, not per-backend memory attribution.

### Honest interpretation: no demonstrated overall speedup

All eight experimental load means increased in absolute time (**1.03–1.31×**
baseline). Unchanged native load means also moved (**1.008–1.392×**), and native
warm-get means rose in all eight shapes. This drift prevents confidently assigning
all between-run changes to the candidate. The within-campaign relative ratios
are mixed. The two changes also prevent a hash-only causal claim.

| KiB / batch / window | Candidate native → new µs | New/native baseline → candidate | New absolute candidate/baseline | Native absolute candidate/baseline |
| --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 522.60 → 873.53 | 1.49× → 1.67× | 1.13× | 1.01× |
| 1 / 1 / 8 | 657.22 → 2,754.1 | 4.48× → 4.19× | 1.05× | 1.12× |
| 1 / 32 / 1 | 844.58 → 1,329.7 | 1.54× → 1.57× | 1.24× | 1.21× |
| 1 / 32 / 8 | 1,876.9 → 4,077.3 | 2.56× → 2.17× | 1.18× | 1.39× |
| 8 / 1 / 1 | 623.80 → 1,303.1 | 2.09× → 2.09× | 1.19× | 1.19× |
| 8 / 1 / 8 | 842.69 → 3,033.7 | 4.30× → 3.60× | 1.03× | 1.23× |
| 8 / 32 / 1 | 1,267.1 → 3,083.2 | 2.05× → 2.43× | 1.31× | 1.11× |
| 8 / 32 / 8 | 4,131.6 → 10,765.0 | 2.90× → 2.61× | 1.16× | 1.28× |

Current window-one load ratios are **1.57–2.43×** native; window-eight ratios are
**2.17–4.19×**. Some ratios narrowed because native slowed more, which is not the
same observation as reducing the new backend's measured latency.

| KiB / batch / window | Candidate get µs native → new | New/native baseline → candidate | New absolute candidate/baseline | Native absolute candidate/baseline |
| --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 1.005 → 2.296 | 5.38× → 2.28× | 0.83× | 1.94× |
| 1 / 1 / 8 | 0.732 → 2.188 | 3.85× → 2.99× | 1.21× | 1.56× |
| 1 / 32 / 1 | 0.986 → 2.984 | 2.67× → 3.03× | 1.39× | 1.23× |
| 1 / 32 / 8 | 1.139 → 3.020 | 2.78× → 2.65× | 1.21× | 1.26× |
| 8 / 1 / 1 | 1.298 → 13.05 | 7.84× → 10.06× | 1.58× | 1.23× |
| 8 / 1 / 8 | 1.265 → 9.549 | 9.25× → 7.55× | 1.08× | 1.32× |
| 8 / 32 / 1 | 3.168 → 12.89 | 4.77× → 4.07× | 1.34× | 1.57× |
| 8 / 32 / 8 | 2.269 → 12.38 | 5.13× → 5.46× | 1.28× | 1.20× |

Current warm point-get ratios are **2.28–10.06×** native. Only the 1 KiB/single
mutation/window-one mean improved in absolute time; the other seven increased.
The general location of the cost is still the combined value-read path, but this
campaign does not establish that changing the digest provider improved it.

### Current all-phase comparison

Ranges retain the same aggregation and matched-case rules as section 4. Complete
p50/p95/p99/max and throughput values for all three backends are in the candidate
comparison CSV; native asynchronous writes still have a different acknowledgement
contract. A ratio is not a promise of repeatable speed at this small sample size.

| Phase | Candidate native µs | Candidate new µs | Current new/native ratio | New candidate/baseline ratio |
| --- | --- | --- | --- | --- |
| `open` | 3,018.1–4,107.7 | 24,954.7–27,967.6 | 6.242–8.504× | 1.068–1.249× |
| `load_write_window` | 522.60–4,131.6 | 873.53–10,765.0 | 1.574–4.191× | 1.034–1.311× |
| `point_get` | 0.732–3.168 | 2.188–13.05 | 2.285–10.056× | 0.826–1.576× |
| `point_get_missing` | 0.126–0.309 | 0.123–0.504 | 0.734–2.258× | 0.380–1.566× |
| `exists` | 0.093–0.210 | 0.193–0.466 | 1.687–2.571× | 0.902–1.423× |
| `exists_missing` | 0.062–0.270 | 0.103–0.373 | 0.746–3.255× | 0.616–1.809× |
| `mixed_get` | 0.782–7.511 | 3.022–21.86 | 2.680–4.835× | 1.003–1.737× |
| `mixed_write_window` | 507.96–4,298.0 | 889.75–11,209.0 | 1.479–4.009× | 0.952–1.328× |
| `checkpoint_capture` | 42.65–148.85 | 0.466–1.433 | 0.005–0.017× | 0.938–3.294× |
| `checkpoint_intervening_write` | 673.04–844.34 | 806.65–1,393.9 | 1.097–1.990× | 1.065–1.589× |
| `checkpoint_get` | 4.506–10.42 | 5.954–31.87 | 0.696–3.949× | 0.994–2.231× |
| `delete_window` | 524.95–1,348.0 | 843.72–3,305.1 | 1.370–4.580× | 0.892–1.336× |
| `full_scan` | 201.67–10,506.4 | 339.94–46,822.9 | 1.531–4.612× | 1.161–1.593× |
| `maintenance_flush_index` | 1,870.5–8,225.7 | 1,289.9–9,174.8 | 0.534–1.404× | 1.157–1.978× |
| `post_flush_get` | 0.834–3.127 | 1.829–10.96 | 0.972–8.474× | 0.926–1.709× |
| `shutdown` | 825.73–1,945.1 | 11,997.3–13,661.7 | 6.669–14.769× | 1.083–1.288× |
| `reopen` | 1,961.8–2,747.8 | 38,714.0–257,147.0 | 18.035–130.679× | 1.095–1.392× |
| `reopen_get_audit` | 1.004–3.238 | 2.246–11.21 | 1.028–4.125× | 1.225–1.704× |

### Current stage evidence and next gate

| KiB / batch / window | Validation ms | Dispatch ms | Caller wait ms | Publication ms | Persistence calls |
| --- | --- | --- | --- | --- | --- |
| 1 / 1 / 1 | 1.56 | 0.47 | 107.41 | 1.33 | 128 |
| 1 / 1 / 8 | 1.89 | 0.23 | 41.12 | 0.62 | 82 |
| 1 / 32 / 1 | 17.49 | 0.69 | 144.06 | 8.67 | 128 |
| 1 / 32 / 8 | 17.57 | 0.42 | 41.84 | 6.30 | 82 |
| 8 / 1 / 1 | 8.30 | 1.11 | 153.56 | 2.10 | 128 |
| 8 / 1 / 8 | 5.11 | 0.21 | 42.20 | 0.52 | 82 |
| 8 / 32 / 1 | 92.87 | 0.81 | 292.38 | 9.19 | 128 |
| 8 / 32 / 8 | 90.13 | 0.44 | 76.54 | 5.51 | 105 |

For the 8 KiB × 32/window-eight candidate, validation is **90.1 ms** and caller
wait **76.5 ms** across 128 batches. Publication is **5.51 ms**. A median **105**
partition-group persistence calls compare with native's **16** barriers. These
observations continue to support investigating encoding/integrity and grouping;
they do not identify a global counter or RPC bottleneck in a run without RPC.

The candidate is correctness-validated and format-compatible. **Its performance
benefit remains unproven.** Before treating it as an optimization, use a controlled
alternating comparison with enough repetition, an unchanged workload and separate
provider-only/routing-only attribution if needed. Avoid a claim of improvement
based solely on a ratio whose native denominator drifted. The next remote test
still requires TCP/fencing integration and cannot replace these local CPU checks.
No AWS resources were launched for either campaign.

## 8. Isolated digest-provider probe

To separate the provider question from database scheduling and borrowed-key
routing, the integrating task ran a small paired probe using the exact member
domain and identity/descriptor chunk pattern. It compared RustCrypto
`blake2` 0.10.6 with `blake2b_simd` 1.0.3, asserted equal digest outputs, warmed
both implementations, and alternated their order across nine paired repetitions.
Each provider hashed 32,768 records per repetition at each payload size. The
process was pinned to allowed CPU 0; AVX2 and SSE4.1 were detected.

[Probe source](../../logs/local-db-2026-10-06/hash-provider-probe.rs),
[54 raw rows](../../logs/local-db-2026-10-06/hash-provider-probe.csv),
[stderr](../../logs/local-db-2026-10-06/hash-provider-probe.stderr.log),
[source/binary manifest](../../logs/local-db-2026-10-06/hash-provider-probe-manifest.json).
Exit status was zero. This probe did not change the serving or benchmark sources.

| Payload | RustCrypto median ns/hash | Candidate median ns/hash | Median paired candidate/original ratio | Paired ratio range |
| --- | --- | --- | --- | --- |
| 32 B | 317.63 | 324.45 | 1.136× | 0.965–1.434× |
| 1 KiB | 1,384.44 | 1,349.90 | 0.866× | 0.704–1.103× |
| 8 KiB | 8,837.34 | 7,982.34 | 0.911× | 0.645–1.313× |

The provider medians are reported separately from the median **paired ratios**;
dividing the first two columns does not reproduce the fourth column. The smaller
records did not benefit in median paired timing. Larger records show modest
paired median reductions, but every ratio range crosses parity. CPU affinity
does not eliminate frequency, scheduling or thermal variation.

This is noisy evidence for a compatible implementation choice, not proof of an
end-to-end database speedup or a fix for the architectural gap. The provider stays
confined to the newly added distributed payload hashing, with its equality tests.
The completed database campaigns and remaining correctness/integration gates keep
their own conclusions above.

Final formatting, whitespace and offline locked library/examples checks passed.
Current source and release binary still match the candidate manifest. Both
campaigns, logs and the probe occupy approximately 77 MiB; temporary case database
directories were removed. No large retained benchmark dataset remains from these
two campaigns.
