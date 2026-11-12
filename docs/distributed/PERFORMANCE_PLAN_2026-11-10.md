# Distributed performance plan — 10 November 2026
## At a glance
Hardware parity remains unresolved. Run 12 completed its 66 cases on two VMs; the corrected local report admits every case, but sustained puts still have a 102 ms p99.9. The full result and historical comparisons are in `FOCUSED_CAMPAIGN_2026-11-07.md`, "Run 12 results". The original cloud receipt remains incomplete because of a standalone-floor report defect. Both VMs and the placement policy are deleted. No further run is authorized. G13W remains sealed but not launched.

Run 8, fleet bench-0a8a6af114a64287, 1 KiB c1. Cells show mean / p50 µs. Durable puts use owner synchronous or native barrier; page-cache puts use owner native tier or native page_cache. Reads use the stated cache state; distributed read copies are profiled. Repetitions and full contracts appear in the data tables.

| Route | Durable put | Page-cache put | Cache read | Cold read |
| --- | --- | --- | --- | --- |
| native | 32.7 / 27.3 | 2.3 / 2.2 | not measured | not measured / not measured |
| owner verbs | 126.6 / 121.5 | 78.2 / 74.4 | 38.1 / 37.5 | 109.3 / 116.0 |
| owner TCP | 130.8 / 128.5 | 85.5 / 81.5 | 46.8 / 46.5 | 115.8 / 122.5 |
| verbs mTLS | 125.3 / 122.8 | not measured | 38.4 / 37.4 | 109.8 / 116.0 |
| hybrid kernel | 125.1 / 119.3 | not measured | 42.6 / 42.1 | 112.7 / 120.4 |
| hybrid SPDK | 129.1 / 122.8 | not measured | not measured | 101.9 / 108.9 |

Run 7, fleet `bench-6e40ca83d74146cf`: 1 KiB c16 sustained puts, 200,000 puts per case, three repetitions. The tier states whether acknowledgement waits for the covering sync or only the page-cache append. The late rate is the mean of the last five complete one-second buckets per case, then the median across repetitions. The earlier values included a partial final bucket and were biased low. These finite-case rates are not sustained capacity.

| ID | Route | Tier | Reps | p99.9 µs median (min–max) | First second puts/s median (min–max) | Last five complete one-second buckets mean puts/s median (min–max) |
| --- | --- | --- | --- | --- | --- | --- |
| R9cdcd4f8 | owner TCP | native | 3 | 127784.9 (120054.0–127922.5) | 23060.0 (23035.0–23508.0) | 10397.8 (10380.0–10432.6) |
| Rcf24342a | owner TCP | synchronous | 3 | 116591.1 (115892.6–118610.1) | 24435.0 (24302.0–25085.0) | 10108.6 (9961.0–10123.4) |
| R73edea0a | owner verbs | native | 3 | 118609.3 (115816.5–122090.5) | 23181.0 (22634.0–24237.0) | 10382.4 (10314.2–10406.2) |
| R3cc10c42 | owner verbs | synchronous | 3 | 118578.1 (116099.4–126480.4) | 24988.0 (24819.0–25072.0) | 10587.4 (10318.0–10628.0) |

Ranked work after the gate: completion wake-up and network; owner persistence and hand-offs; size-dependent software; coordinator waits; put stall; batch writes; hybrid read residual. The ranked section gives measured accounting references, not predicted gains or proven end-to-end bounds [INFERENCE].

| Rank | Item | Measured basis | Accounting reference | Next step | Ask before |
| --- | --- | --- | --- | --- | --- |
| 0 | Hardware parity | native sync 27.3 µs; verbs 34.0 µs | Sync steps; SSD count and group-commit contracts differ | Defined: equal writers c1/c5/c16, adapter group barrier, five Native instances on one SSD, one owner and 64 KiB profiles; fair_native 168, parity 216 | Explicit launch authorization |
| 1 | Completion and network | read network 25.1 µs (66%); put 31.9 µs (24%) | Network component | Busy-poll A/B | Public configuration |
| 2 | Owner persist and hand-offs | persist 44.2 µs (33%) | Software and hand-off intervals | Same-call stage timers | Durability contract |
| 3 | Size-dependent software | 64 KiB coordinator 91.9 µs (20%); owner 127.5 µs (28%) | Coordinator and owner components | Time digest and owner stages | Wire or persisted format |
| 4 | c16 pipeline waits | waiting and coalescing 81.0 µs (19%) | Measured wait | Trace same-call pipeline stages | Durability contract |
| 5 | Put stall | timeline overwrite mean spacing 2062 puts; fit intercept 11116.4 µs | Observed stall | Correlate checkpoint phases | Persisted format |
| 6 | Batch writes | native p50 432.7 µs; owner verbs p50 1274.3 µs | Software and hand-offs | Time batch stages | Wire or persisted format |
| 7 | Hybrid read residual | resident residual 11.3 µs (27%) | Derived residual | Time dispatch and wake-up | Public queue geometry |

Hardware parity comes first; then rank work by the gap to the floor and its share of API mean. [INFERENCE] Workload note, without changing the ranking: sustained throughput falls from the first second to the last five complete one-second buckets in run 7.
## Decisions
Hardware parity comes first. The owner approved equal-writer Native and distributed arms at c1, c5 and c16: a grouped Native barrier in the benchmark adapter without an engine change, five Native instances in one process on one SSD (`native_shards`), one-owner distributed cases (`owner_count` 1), and 64 KiB write profiles. The cases are defined, not run: fair_native 168, parity 216 cases. Launch needs the owner's explicit authorization. Skills contain rules, not benchmark values. Do not add a `pread` path to native if it adds latency; handle cold-read pattern differences in the distributed stack and leave native unchanged. No experiment in this plan is authorized by its inclusion.
## Contracts and reading the results
| Contract | Acknowledgement |
|---|---|
| Topology | Native: one process, one SSD. Tidehunter: five owners. External stores: other external contributors record five configured or observed storage nodes; Cassandra's 3 November run records five contacts, not a verified five-node ring; five storage-host SSD mounts are attested only for the inspected 4 November and final deployments; FoundationDB also has a separate log/coordinator host; some 3 November FoundationDB and TiKV host identities name a different fleet than the report path, so the path does not establish the execution fleet. RF1 does not imply equal SSD count. |
| Native `value_wal_barrier` | The fork-only benchmark barrier calls `fsync` on every registered Value-WAL file and the directory after every put; callers do not share an application sync. |
| Native `value_wal_group_barrier` | One benchmark-adapter barrier covers callers that entered before the run started; failure is sticky. It does not change the engine. |
| Native `native_shards` | Five Native instances in one process route by key hash and share one SSD and one filesystem journal. This is not distributed operation-ID routing. |
| Native `page_cache` | Page-cache write returns; no per-put barrier. |
| Owner `synchronous` | A covering `fdatasync` completes before acknowledgement; batches appended during a sync are covered by the next sync. |
| Owner `native` | Page-cache acknowledgement. This tier name does not mean the native backend. |
| Paper write contract | Writes acknowledge at the page cache; finalized maps flush in the background. Explicit flush is available for stronger durability. The paper does not measure the fork-only per-put barrier. The measurements do not reproduce the paper; they do not contradict it. |

API time is completion minus dispatch for successful calls. Quantiles use exact nearest rank. `n` counts successful calls; c1 and c16 mean one and sixteen concurrent clients. A profiled copy adds profiling cost and has one repetition. For repeated rows, the mean and quantiles are medians of per-repetition values, not pooled calls. Do not pool repetitions or fleets or claim significance from these counts. Reads can have a fast mode, so mean can be below p50; put tails can put mean above p50.

Write timelines measure adjacent intervals on retained calls. Write and read profiles give sampled means. Native profiles write stages, not batch writes. A residual is a calculated difference, not a timed stage. Device counters cover a window, not individual calls. Floors are separate experiments. `n/c` means not computed, not zero. Native has no coordinator: its library work is WAL append plus index update. Read flights and API resolutions are sampled separately; do not subtract them. For WAL reads, network is exchange minus server handler, encode, decode and verify. Nested annotations are not additive.

Owner TCP uses mutual TLS. Owner verbs and hybrid use trusted transport; verbs mTLS isolates TLS. Hybrid writes use owner verbs. Hybrid buffered reads use a target page cache; kernel direct and SPDK use direct storage. Device reads per call help qualify a case but do not prove every call reached media.
## Run 8 values
Run 8 (`unified`), fleet `bench-0a8a6af114a64287`, completed 265 of 270 cases. Table notes state repetitions, contracts and case references. The case-ID appendix maps references to full IDs.
### Durable writes
Unprofiled; owner `synchronous`, native `value_wal_barrier`.

Run 8 also has one Native SSD and five owners. Native's per-put file-and-directory barrier differs from the owner's covering sync. [INFERENCE] The 64 KiB c16 Native barrier result is consistent with a serialized-service model, but its cause is not isolated and API calls can overlap.

For context, the run 8 unprofiled c1 1 KiB synchronous verbs put has p50 / mean 121.5 / 126.6 µs. The full-matrix c1 mixed-stream synchronous verbs put has 108.8 / 112.4 µs. Fleet, workload and repetition count differ; this is not a measured regression or profiling cost.

Across three fleets, Native per-put file-and-directory barrier c16 64 KiB put p50 was 2652.3 to 2935.8 µs; page-cache acknowledgement (not per-put durable) was 164.2 to 181.7 µs in two fleets. The main body of the sampled distribution shifts, not only a few tail calls; the page-cache jump is unexplained.

In one private checkpoint replay, 8 of 234,412 value-bearing successful puts and found reads were at least 64 KiB (0.0034%); this is not a mainnet rate. Many nonempty table medians are about 0.1 to 10 KiB, though smaller values occur; full-checkpoint contents had p50 8,885 and p95 11,466 bytes, approximately 8 to 12 KiB.

Run 8; fleet bench-0a8a6af114a64287. Counts are per repetition; timings are medians of repetition statistics, not pooled calls. IDs resolve in the case appendix. Each row is one population; p50 range spans repetitions.

| ID | Route | Shape | Size | c | Contract | Cache | n | Reps | Mean µs | p50 µs | p99 µs | p99.9 µs | p50 range µs | Reads/call | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R08b1e259 | hybrid SPDK | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 129.1 | 122.8 | 243.8 | 427.7 | 120.7 to 124.9 | not measured | qualified 2/2 |
| Rd6fbe26d | hybrid kernel DIRECT | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 125.1 | 119.3 | 228.5 | 365.5 | 119.1 to 119.5 | not measured | qualified 2/2 |
| R0f7a4fa4 | native | put | 1 KiB | 1 | value_wal_barrier; new; unprofiled | not measured | 4096 + 4096 | 2 | 31.3 | 27.2 | 84.1 | 99.3 | 26.8 to 27.6 | not measured | qualified 2/2 |
| Rc15b54b0 | native | put | 1 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 32.7 | 27.3 | 174.7 | 192.9 | 27.0 to 27.5 | not measured | qualified 2/2 |
| R27cd3fe5 | owner TCP | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 130.8 | 128.5 | 176.0 | 304.5 | 127.8 to 129.1 | not measured | qualified 2/2 |
| R08941181 | owner verbs | put | 1 KiB | 1 | synchronous; new; unprofiled | not measured | 4096 + 4096 | 2 | 124.6 | 119.4 | 229.7 | 379.7 | 116.8 to 122.0 | not measured | qualified 2/2 |
| R63036dd4 | owner verbs | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 126.6 | 121.5 | 221.4 | 440.2 | 120.8 to 122.2 | not measured | qualified 2/2 |
| Rc2292fda | verbs mTLS | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 125.3 | 122.8 | 174.7 | 345.0 | 122.5 to 123.0 | not measured | qualified 2/2 |
| Rffe00a2c | hybrid SPDK | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 333.4 | 335.0 | 605.8 | 830.9 | 332.8 to 337.2 | not measured | qualified 2/2 |
| Ra2ce76c9 | hybrid kernel DIRECT | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 336.1 | 330.2 | 631.6 | 751.3 | 324.5 to 336.0 | not measured | qualified 2/2 |
| R58ebf028 | native | put | 1 KiB | 16 | value_wal_barrier; new; unprofiled | not measured | 4096 + 4096 | 2 | 71.0 | 64.0 | 184.4 | 286.7 | 63.9 to 64.2 | not measured | qualified 2/2 |
| Rf1572aad | native | put | 1 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 70.5 | 64.1 | 169.5 | 279.6 | 63.2 to 65.1 | not measured | qualified 2/2 |
| R64d5dda3 | owner TCP | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 336.1 | 344.1 | 556.9 | 743.6 | 342.0 to 346.1 | not measured | qualified 2/2 |
| R8967c0cd | owner verbs | put | 1 KiB | 16 | synchronous; new; unprofiled | not measured | 4096 + 4096 | 2 | 356.5 | 353.7 | 627.3 | 807.6 | 348.1 to 359.2 | not measured | qualified 2/2 |
| Rb42eacfc | owner verbs | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 361.4 | 360.4 | 634.0 | 869.5 | 355.0 to 365.7 | not measured | qualified 2/2 |
| R5f6d119c | verbs mTLS | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 334.3 | 336.5 | 543.8 | 816.5 | 333.2 to 339.7 | not measured | qualified 2/2 |
| Ra466d20c | native | put | 8 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 43.1 | 38.9 | 96.6 | 113.8 | 38.6 to 39.1 | not measured | qualified 2/2 |
| Raf0d7639 | owner verbs | put | 8 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 229.1 | 226.8 | 291.2 | 566.7 | 220.8 to 232.7 | not measured | qualified 2/2 |
| Re10469a9 | native | put | 8 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 199.2 | 194.7 | 407.3 | 471.6 | 192.1 to 197.3 | not measured | qualified 2/2 |
| R7e542275 | owner verbs | put | 8 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 482.3 | 463.2 | 960.1 | 1256.0 | 459.1 to 467.3 | not measured | qualified 2/2 |
| R7f945823 | native | put | 64 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 161.1 | 157.2 | 211.8 | 226.4 | 157.1 to 157.3 | not measured | qualified 2/2 |
| R13eea891 | owner TCP | put | 64 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 495.6 | 490.3 | 591.1 | 760.3 | 488.7 to 491.9 | not measured | qualified 2/2 |
| R4afa3c44 | owner verbs | put | 64 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 445.8 | 441.4 | 537.4 | 735.0 | 438.6 to 444.2 | not measured | qualified 2/2 |
| R0a1f9571 | verbs mTLS | put | 64 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 471.9 | 466.6 | 572.3 | 701.0 | 463.5 to 469.7 | not measured | qualified 2/2 |
| R502435c7 | hybrid SPDK | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1774.0 | 1646.2 | 4382.9 | 4883.2 | 1619.7 to 1672.6 | not measured | qualified 2/2 |
| R9bbfa53e | hybrid kernel DIRECT | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1760.5 | 1642.5 | 3914.0 | 4591.8 | 1638.4 to 1646.6 | not measured | qualified 2/2 |
| R532834ed | native | put | 64 KiB | 16 | value_wal_barrier; new; unprofiled | not measured | 4096 + 4096 | 2 | 2675.4 | 2913.3 | 3421.6 | 3497.6 | 2890.8 to 2935.8 | not measured | qualified 2/2 |
| R7ebbb903 | native | put | 64 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 2686.8 | 2899.2 | 3400.0 | 3491.0 | 2867.4 to 2930.9 | not measured | qualified 2/2 |
| R9d90d5c8 | owner TCP | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1745.9 | 1666.0 | 3722.7 | 4962.6 | 1641.8 to 1690.1 | not measured | qualified 2/2 |
| Rbdb65963 | owner verbs | put | 64 KiB | 16 | synchronous; new; unprofiled | not measured | 4096 + 4096 | 2 | 1715.4 | 1634.3 | 3466.7 | 4164.1 | 1608.4 to 1660.2 | not measured | qualified 2/2 |
| R2ec0e6ef | owner verbs | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1707.6 | 1568.2 | 3593.4 | 5225.2 | 1564.1 to 1572.2 | not measured | qualified 2/2 |
| R75e726d0 | verbs mTLS | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1706.8 | 1622.8 | 3442.5 | 4281.2 | 1607.3 to 1638.4 | not measured | qualified 2/2 |
### Page-cache-acknowledged writes
Owner `native` tier and native `page_cache`; these are not durable-per-put comparisons.

Run 8; fleet bench-0a8a6af114a64287. Counts are per repetition; timings are medians of repetition statistics, not pooled calls. IDs resolve in the case appendix. Each row is one population; p50 range spans repetitions.

| ID | Route | Shape | Size | c | Contract | Cache | n | Reps | Mean µs | p50 µs | p99 µs | p99.9 µs | p50 range µs | Reads/call | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R5c5502ba | native | put | 1 KiB | 1 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 2.3 | 2.2 | 4.5 | 9.5 | 2.2 to 2.2 | not measured | qualified 2/2 |
| R361477d0 | owner TCP | put | 1 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 85.5 | 81.5 | 128.8 | 331.3 | 81.2 to 81.8 | not measured | qualified 2/2 |
| Rb14099c4 | owner verbs | put | 1 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 78.2 | 74.4 | 127.4 | 317.0 | 73.5 to 75.3 | not measured | qualified 2/2 |
| Ree7b782d | native | put | 1 KiB | 16 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 3.3 | 3.1 | 6.9 | 26.2 | 3.1 to 3.1 | not measured | qualified 2/2 |
| R2301b08a | owner TCP | put | 1 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 423.2 | 326.0 | 645.5 | 26944.0 | 315.3 to 336.8 | not measured | qualified 2/2 |
| R4533c702 | owner verbs | put | 1 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 514.9 | 324.4 | 733.4 | 49292.8 | 317.7 to 331.1 | not measured | qualified 2/2 |
| R292ec93e | native | put | 8 KiB | 1 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 3.4 | 3.2 | 5.8 | 12.3 | 3.2 to 3.3 | not measured | qualified 2/2 |
| Rac03e4ee | owner verbs | put | 8 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 104.8 | 100.7 | 152.6 | 356.2 | 100.3 to 101.1 | not measured | qualified 2/2 |
| R886717d2 | native | put | 8 KiB | 16 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 4.2 | 4.1 | 9.3 | 31.1 | 4.0 to 4.1 | not measured | qualified 2/2 |
| R70852372 | owner verbs | put | 8 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 607.4 | 463.7 | 984.9 | 37063.5 | 463.3 to 464.1 | not measured | qualified 2/2 |
| Rd4328490 | native | put | 64 KiB | 1 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 54.4 | 41.1 | 114.9 | 134.1 | 41.1 to 41.2 | not measured | qualified 2/2 |
| Rbf0c94da | owner TCP | put | 64 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 328.2 | 319.3 | 466.0 | 1198.7 | 319.1 to 319.6 | not measured | qualified 2/2 |
| R9a805863 | owner verbs | put | 64 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 284.0 | 275.2 | 396.8 | 1017.9 | 273.6 to 276.8 | not measured | qualified 2/2 |
| R0315682c | native | put | 64 KiB | 16 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 169.2 | 165.7 | 291.9 | 499.1 | 164.2 to 167.3 | not measured | qualified 2/2 |
| R1f5a546a | owner TCP | put | 64 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 1642.6 | 1567.7 | 3080.5 | 3900.6 | 1537.1 to 1598.2 | not measured | qualified 2/2 |
| Ra973e337 | owner verbs | put | 64 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 1608.7 | 1501.2 | 2991.9 | 4021.4 | 1470.8 to 1531.7 | not measured | qualified 2/2 |
### Batch writes and deletes
Available cases only. Missing routes are not zero; native batch decomposition is not available.

Run 8; fleet bench-0a8a6af114a64287. Counts are per repetition; timings are medians of repetition statistics, not pooled calls. IDs resolve in the case appendix. Each row is one population; p50 range spans repetitions.

| ID | Route | Shape | Size | c | Contract | Cache | n | Reps | Mean µs | p50 µs | p99 µs | p99.9 µs | p50 range µs | Reads/call | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R0daebf6d | native | delete | 1 KiB | 1 | page_cache; unprofiled | not measured | 4096 | 1 | 2.0 | 1.9 | 4.2 | 8.4 | not measured | not measured | qualified 1/1 |
| R92629de8 | native | delete | 1 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 | 1 | 30.8 | 27.5 | 81.9 | 90.5 | not measured | not measured | qualified 1/1 |
| R10f237bf | native | put_batch | 1 KiB | 1 | page_cache; unprofiled | not measured | 1024 | 1 | 38.7 | 37.9 | 50.6 | 53.4 | not measured | not measured | qualified 1/1 |
| R12e5f6b2 | native | put_batch | 1 KiB | 1 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 114.5 | 105.6 | 167.4 | 315.8 | not measured | not measured | qualified 1/1 |
| Rc0228526 | owner verbs | delete | 1 KiB | 1 | native; unprofiled | not measured | 4096 | 1 | 71.2 | 66.7 | 132.7 | 380.0 | not measured | not measured | qualified 1/1 |
| Rf6f6c07f | owner verbs | delete | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 | 1 | 122.4 | 114.2 | 287.2 | 426.8 | not measured | not measured | qualified 1/1 |
| Rf279f18f | owner verbs | put_batch | 1 KiB | 1 | native; unprofiled | not measured | 1024 | 1 | 284.7 | 273.5 | 402.4 | 564.0 | not measured | not measured | qualified 1/1 |
| R8a2dc242 | owner verbs | put_batch | 1 KiB | 1 | synchronous; unprofiled | not measured | 1024 | 1 | 413.2 | 407.3 | 495.0 | 703.7 | not measured | not measured | qualified 1/1 |
| R7a263dd8 | native | delete | 1 KiB | 16 | page_cache; unprofiled | not measured | 4096 | 1 | 3.0 | 2.8 | 5.8 | 23.4 | not measured | not measured | qualified 1/1 |
| R4697e418 | native | delete | 1 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 | 1 | 96.2 | 89.5 | 218.2 | 276.5 | not measured | not measured | qualified 1/1 |
| R166d37d0 | native | put_batch | 1 KiB | 16 | page_cache; unprofiled | not measured | 1024 | 1 | 182.1 | 178.2 | 388.6 | 455.6 | not measured | not measured | qualified 1/1 |
| R6ed9c0dc | native | put_batch | 1 KiB | 16 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 1107.4 | 1112.1 | 1305.8 | 1334.8 | not measured | not measured | qualified 1/1 |
| R3ec6d58f | owner verbs | delete | 1 KiB | 16 | native; unprofiled | not measured | 4096 | 1 | 519.4 | 274.1 | 581.9 | 61548.9 | not measured | not measured | qualified 1/1 |
| R35c69c2b | owner verbs | delete | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 | 1 | 289.4 | 292.6 | 530.6 | 750.3 | not measured | not measured | qualified 1/1 |
| R3067be9e | owner verbs | put_batch | 1 KiB | 16 | native; unprofiled | not measured | 1024 | 1 | 2208.3 | 2295.7 | 3148.4 | 3913.5 | not measured | not measured | qualified 1/1 |
| Ra39565f2 | owner verbs | put_batch | 1 KiB | 16 | synchronous; unprofiled | not measured | 1024 | 1 | 2453.2 | 2558.6 | 3887.2 | 4295.6 | not measured | not measured | qualified 1/1 |
| Rda9d8374 | native | put_batch | 8 KiB | 1 | page_cache; unprofiled | not measured | 1024 | 1 | 284.9 | 224.5 | 591.9 | 623.1 | not measured | not measured | qualified 1/1 |
| R49d0c784 | native | put_batch | 8 KiB | 1 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 449.8 | 432.7 | 614.8 | 632.5 | not measured | not measured | qualified 1/1 |
| R598ad5f1 | owner verbs | put_batch | 8 KiB | 1 | native; unprofiled | not measured | 1024 | 1 | 966.3 | 939.3 | 1345.6 | 2260.2 | not measured | not measured | qualified 1/1 |
| R71f25999 | owner verbs | put_batch | 8 KiB | 1 | synchronous; unprofiled | not measured | 1024 | 1 | 1318.4 | 1274.3 | 1651.9 | 1949.2 | not measured | not measured | qualified 1/1 |
| Rfaf05629 | native | put_batch | 8 KiB | 16 | page_cache; unprofiled | not measured | 1024 | 1 | 1091.0 | 876.7 | 2438.2 | 30149.3 | not measured | not measured | qualified 1/1 |
| Rd6321af3 | native | put_batch | 8 KiB | 16 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 10918.4 | 10865.2 | 13748.1 | 13789.8 | not measured | not measured | qualified 1/1 |
| R64025107 | owner verbs | put_batch | 8 KiB | 16 | native; unprofiled | not measured | 1024 | 1 | 7399.9 | 7024.1 | 12691.9 | 12739.5 | not measured | not measured | qualified 1/1 |
| R752a0ff1 | owner verbs | put_batch | 8 KiB | 16 | synchronous; unprofiled | not measured | 1024 | 1 | 7507.4 | 6478.5 | 15888.4 | 15965.3 | not measured | not measured | qualified 1/1 |
### Resident and cold reads
Memory means resident under the route contract. Cold is a requested state, qualified by counters and latency distribution. Native small cold reads do not establish cold latency. The 8 KiB case passes the device-count gate but has memory-like latency; 64 KiB meets case-level qualification. Window counters do not prove every call reached NAND. Native reads recent fragments through the memory map and older fragments with positional reads. Native cold components are not computed. Kernel direct counters include non-API reads; their device time is an upper bound or part of a combined device, fabric and stack interval, not an additive leaf.

Run 8; fleet bench-0a8a6af114a64287. Counts are per repetition; timings are medians of repetition statistics, not pooled calls. IDs resolve in the case appendix. Unprofiled distributed read copies were not run; see profiled decomposition. Each row is one population; p50 range spans repetitions.

| ID | Route | Shape | Size | c | Contract | Cache | n | Reps | Mean µs | p50 µs | p99 µs | p99.9 µs | p50 range µs | Reads/call | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Rc2cbb87b | native | read | 1 KiB | 1 | value_wal_barrier; unprofiled | cold | 2048 + 2048 | 2 | not measured | not measured | not measured | not measured | not measured | not measured | unqualified 2/2 |
| R565f3b20 | native | read | 1 KiB | 16 | value_wal_barrier; unprofiled | cold | 2048 | 1 | not measured | not measured | not measured | not measured | not measured | not measured | unqualified 1/1 |
| R436ed9cf | native | read | 8 KiB | 1 | value_wal_barrier; unprofiled | cold | 4096 + 4096 | 2 | 35.7 | 4.7 | 963.9 | 1009.3 | 4.7 to 4.8 | 1.03 | passes device-count gate; memory-like latency |
| R6bad39f9 | native | read | 8 KiB | 1 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 1.8 | 1.9 | 2.8 | 8.3 | 1.8 to 1.9 | not measured | qualified 2/2 |
| R65a80e76 | native | read | 8 KiB | 16 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 2.6 | 2.6 | 4.1 | 12.9 | 2.5 to 2.6 | not measured | qualified 2/2 |
| R78233185 | native | read | 64 KiB | 1 | value_wal_barrier; unprofiled | cold | 4096 + 4096 | 2 | 160.6 | 102.1 | 1004.9 | 1055.7 | 101.9 to 102.3 | 3.72 | qualified 2/2 |
| R09e69537 | native | read | 64 KiB | 1 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 10.6 | 11.0 | 16.0 | 19.9 | 11.0 to 11.1 | not measured | qualified 2/2 |
| Rf2eb0bc0 | native | read | 64 KiB | 16 | value_wal_barrier; unprofiled | cold | 4096 | 1 | 1034.0 | 345.9 | 9316.6 | 14959.2 | not measured | 3.76 | qualified 1/1 |
| R2eb25424 | native | read | 64 KiB | 16 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 15.3 | 15.4 | 25.4 | 34.9 | 15.3 to 15.4 | not measured | qualified 2/2 |
### Decomposition
Each table uses one case per route. Means and shares refer to that case's API mean. TOTAL is that API mean; API p50 is not a sum. Use the timeline copy when both copies exist. Do not average copies. Profile and unprofiled comparisons are context, not exact subtractions.

Run 8; fleet bench-0a8a6af114a64287; one profiled repetition per route. Cells show mean µs (share of API mean). A dash means not computed or not measured; rows with no computed values are omitted. Components can overlap. Owner synchronous waits for fdatasync; native barrier syncs WAL and directory. Read state is given in the title.
#### Durable put 1 KiB c1
| Component | Kind | owner verbs | owner TCP | verbs mTLS | hybrid kernel DIRECT | hybrid SPDK |
| --- | --- | --- | --- | --- | --- | --- |
| owner persist (file sync, includes software) | sampled mean | 44.2 (33%) | 38.3 (27%) | 38.3 (29%) | 38.8 (29%) | 38.5 (29%) |
| network (wire, both stacks, TLS, dispatch) | derived duration difference | 31.9 (24%) | 43.2 (30%) | 34.5 (26%) | 31.8 (24%) | 31.9 (24%) |
| client coordinator | aggregate of exact intervals | 20.1 (15%) | 23.1 (16%) | 21.6 (16%) | 22.5 (17%) | 22.0 (17%) |
| owner software | derived mean difference | 18.1 (14%) | 18.8 (13%) | 18.9 (14%) | 19.7 (15%) | 18.4 (14%) |
| durable wait and publication | aggregate of exact intervals | 7.5 (5.6%) | 8.0 (5.5%) | 7.3 (5.5%) | 7.8 (5.9%) | 7.8 (6.0%) |
| waiting and coalescing | aggregate of exact intervals | 6.0 (4.5%) | 6.5 (4.5%) | 6.2 (4.6%) | 6.3 (4.7%) | 6.6 (5.0%) |
| client queueing and wake-up | aggregate of exact intervals | 3.9 (2.9%) | 4.3 (3.0%) | 4.0 (3.0%) | 4.0 (3.0%) | 3.7 (2.8%) |
| residual | residual | 2.3 (1.7%) | 2.1 (1.5%) | 2.1 (1.6%) | 1.9 (1.4%) | 1.9 (1.4%) |
| TOTAL (API mean µs) | not measured | 134.1 | 144.3 | 132.9 | 132.8 | 130.8 |
| API p50 µs | not measured | 125.5 | 139.0 | 130.5 | 128.2 | 125.3 |
| ID | not measured | R40880520 | R119f59bc | R4ce90626 | Re7acf834 | R852b2f71 |
Not computed for all routes: client messaging (encode, decode, verify); device + fabric + stack (submit_to_cq, not separable); exchange (owner and network not separable).
#### Native barrier put 1 KiB c1
| Component | Kind | native |
| --- | --- | --- |
| Barrier file sync | sampled mean | 25.2 (80%) |
| Library work (WAL append plus index update) | sampled mean | 3.5 (11%) |
| residual | derived duration difference | 2.9 (9.3%) |
| TOTAL (API mean µs) | not measured | 31.6 |
| API p50 µs | not measured | 27.7 |
| ID | not measured | Ra056169f |
Not computed for all routes: client messaging (encode, decode, verify); client queueing and wake-up; device + fabric + stack (submit_to_cq, not separable); exchange (owner and network not separable); network (wire, both stacks, TLS, dispatch); owner persist (file sync, includes software); owner software; waiting and coalescing.
#### Durable put 64 KiB c1
| Component | Kind | owner verbs | owner TCP | native |
| --- | --- | --- | --- | --- |
| owner persist (file sync, includes software) | sampled mean | 170.6 (37%) | 163.4 (33%) | — |
| owner software | derived mean difference | 127.5 (28%) | 127.7 (26%) | — |
| client coordinator (native: WAL append and index update) | aggregate of exact intervals, sampled mean | 91.9 (20%) | 92.3 (18%) | 39.7 (25%) |
| network (wire, both stacks, TLS, dispatch) | derived duration difference | 48.4 (10%) | 92.7 (19%) | — |
| waiting and coalescing | aggregate of exact intervals | 10.0 (2.2%) | 9.6 (1.9%) | — |
| durable wait and publication (native: barrier file sync) | aggregate of exact intervals, sampled mean | 8.2 (1.8%) | 8.4 (1.7%) | 113.0 (71%) |
| client queueing and wake-up | aggregate of exact intervals | 4.6 (1.0%) | 4.4 (0.9%) | — |
| residual | derived duration difference, residual | 1.4 (0.3%) | 1.3 (0.3%) | 6.7 (4.2%) |
| TOTAL (API mean µs) | not measured | 462.5 | 499.9 | 159.5 |
| API p50 µs | not measured | 450.6 | 494.6 | 154.8 |
| ID | not measured | R6dbb1b55 | R0d1a0ee3 | Rc7e58ea0 |
Not computed for all routes: client messaging (encode, decode, verify); device + fabric + stack (submit_to_cq, not separable); exchange (owner and network not separable).
#### Durable put 1 KiB c16
| Component | Kind | owner verbs | owner TCP | native |
| --- | --- | --- | --- | --- |
| exchange (owner and network not separable) | aggregate of exact intervals | 108.1 (26%) | 128.4 (28%) | — |
| durable wait and publication (native: barrier file sync) | aggregate of exact intervals, sampled mean | 105.2 (25%) | 110.6 (24%) | 57.9 (80%) |
| waiting and coalescing | aggregate of exact intervals | 81.0 (19%) | 87.4 (19%) | — |
| client queueing and wake-up | aggregate of exact intervals | 70.0 (17%) | 75.2 (16%) | — |
| client coordinator (native: WAL append and index update) | aggregate of exact intervals, sampled mean | 50.7 (12%) | 54.4 (12%) | 5.4 (7.5%) |
| residual | derived duration difference, residual | 5.7 (1.4%) | 5.8 (1.3%) | 9.1 (13%) |
| TOTAL (API mean µs) | not measured | 420.9 | 461.9 | 72.4 |
| API p50 µs | not measured | 420.0 | 461.8 | 65.3 |
| ID | not measured | R1553ed29 | Rc9cfaa51 | R8d29f5b7 |
Not computed for all routes: client messaging (encode, decode, verify); device + fabric + stack (submit_to_cq, not separable); network (wire, both stacks, TLS, dispatch); owner persist (file sync, includes software); owner software.
#### Page-cache put 1 KiB c1
| Component | Kind | owner verbs | native |
| --- | --- | --- | --- |
| exchange (owner and network not separable) | aggregate of exact intervals | 44.1 (52%) | — |
| client coordinator (native: WAL append and index update) | aggregate of exact intervals, sampled mean | 22.2 (26%) | 1.7 (73%) |
| durable wait and publication (native: barrier file sync) | aggregate of exact intervals | 6.4 (7.6%) | — |
| waiting and coalescing | aggregate of exact intervals | 6.2 (7.4%) | — |
| client queueing and wake-up | aggregate of exact intervals | 3.2 (3.8%) | — |
| residual | derived duration difference, residual | 2.1 (2.5%) | 0.6 (27%) |
| TOTAL (API mean µs) | not measured | 84.2 | 2.3 |
| API p50 µs | not measured | 80.3 | 2.2 |
| ID | not measured | Radbfd0ca | R12350055 |
Not computed for all routes: client messaging (encode, decode, verify); device + fabric + stack (submit_to_cq, not separable); network (wire, both stacks, TLS, dispatch); owner persist (file sync, includes software); owner software.
#### Delete 1 KiB c1
| Component | Kind | owner verbs | owner TCP | native |
| --- | --- | --- | --- | --- |
| owner persist (file sync, includes software) | sampled mean | 37.7 (30%) | 36.7 (28%) | — |
| network (wire, both stacks, TLS, dispatch) | derived duration difference | 31.1 (25%) | 40.7 (31%) | — |
| client coordinator (native: WAL append and index update) | aggregate of exact intervals, sampled mean | 20.1 (16%) | 19.5 (15%) | 2.9 (9.7%) |
| owner software | derived mean difference | 15.7 (13%) | 16.0 (12%) | — |
| durable wait and publication (native: barrier file sync) | aggregate of exact intervals, sampled mean | 7.2 (5.8%) | 6.9 (5.2%) | 24.0 (81%) |
| waiting and coalescing | aggregate of exact intervals | 6.1 (4.9%) | 6.2 (4.7%) | — |
| client queueing and wake-up | aggregate of exact intervals | 4.2 (3.4%) | 3.7 (2.8%) | — |
| residual | derived duration difference, residual | 2.0 (1.6%) | 2.0 (1.5%) | 2.8 (9.5%) |
| TOTAL (API mean µs) | not measured | 124.1 | 131.8 | 29.8 |
| API p50 µs | not measured | 120.2 | 128.1 | 26.5 |
| ID | not measured | R2a0028ea | Re116d570 | R409e1b71 |
Not computed for all routes: client messaging (encode, decode, verify); device + fabric + stack (submit_to_cq, not separable); exchange (owner and network not separable).
#### Batch 32 × 8 KiB c1
| Component | Kind | owner verbs | owner TCP |
| --- | --- | --- | --- |
| owner software | derived mean difference | 460.6 (35%) | 462.1 (33%) |
| owner persist (file sync, includes software) | sampled mean | 357.2 (27%) | 324.4 (23%) |
| client coordinator | aggregate of exact intervals | 326.4 (25%) | 337.6 (24%) |
| network (wire, both stacks, TLS, dispatch) | derived duration difference | 91.6 (6.9%) | 182.0 (13%) |
| durable wait and publication | aggregate of exact intervals | 54.5 (4.1%) | 54.2 (3.9%) |
| waiting and coalescing | aggregate of exact intervals | 21.8 (1.7%) | 22.7 (1.6%) |
| client queueing and wake-up | aggregate of exact intervals | 5.0 (0.4%) | 5.3 (0.4%) |
| residual | residual | 2.3 (0.2%) | 3.0 (0.2%) |
| TOTAL (API mean µs) | not measured | 1319.5 | 1391.3 |
| API p50 µs | not measured | 1265.0 | 1372.4 |
| ID | not measured | R3e9799a7 | Rac31c3a7 |
Not computed for all routes: client messaging (encode, decode, verify); device + fabric + stack (submit_to_cq, not separable); exchange (owner and network not separable).
#### Resident read 1 KiB c1
| Component | Kind | owner verbs | owner TCP | verbs mTLS | hybrid kernel buffered |
| --- | --- | --- | --- | --- | --- |
| network (wire, both stacks, TLS, dispatch) | derived mean difference | 25.1 (66%) | 32.9 (70%) | 25.4 (66%) | 28.2 (66%) |
| owner software | sampled mean | 7.7 (20%) | 8.2 (18%) | 7.6 (20%) | — |
| residual | derived mean difference | 2.8 (7.5%) | 3.1 (6.7%) | 2.9 (7.7%) | 11.3 (27%) |
| client messaging (encode, decode, verify) | sampled mean | 1.4 (3.8%) | 1.4 (2.9%) | 1.3 (3.3%) | 1.0 (2.3%) |
| client coordinator | sampled mean | 0.7 (1.9%) | 0.8 (1.7%) | 0.8 (2.0%) | 0.7 (1.7%) |
| client queueing and wake-up | derived mean difference, sampled mean | 0.4 (1.1%) | 0.4 (0.8%) | 0.4 (1.0%) | 1.3 (3.0%) |
| waiting and coalescing | sampled mean | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) |
| TOTAL (API mean µs) | not measured | 38.1 | 46.8 | 38.4 | 42.6 |
| API p50 µs | not measured | 37.5 | 46.5 | 37.4 | 42.1 |
| ID | not measured | R035b1e82 | R1ec39309 | R79431f2a | R9c9bedb1 |
Not computed for all routes: device + fabric + stack (submit_to_cq, not separable); durable wait and publication; exchange (owner and network not separable); ssd device.
#### Resident read 64 KiB c1
| Component | Kind | owner verbs | owner TCP | hybrid kernel buffered |
| --- | --- | --- | --- | --- |
| network (wire, both stacks, TLS, dispatch) | derived mean difference | 41.3 (60%) | 82.6 (75%) | 47.7 (73%) |
| owner software | sampled mean | 17.3 (25%) | 17.9 (16%) | — |
| client messaging (encode, decode, verify) | sampled mean | 5.4 (7.9%) | 5.5 (5.0%) | 3.9 (5.9%) |
| residual | derived mean difference | 3.1 (4.5%) | 3.2 (2.9%) | 12.0 (18%) |
| client coordinator | sampled mean | 1.0 (1.4%) | 0.9 (0.9%) | 0.8 (1.3%) |
| client queueing and wake-up | derived mean difference, sampled mean | 0.5 (0.7%) | 0.4 (0.4%) | 1.3 (2.0%) |
| waiting and coalescing | sampled mean | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) |
| TOTAL (API mean µs) | not measured | 68.5 | 110.5 | 65.6 |
| API p50 µs | not measured | 68.0 | 109.2 | 65.0 |
| ID | not measured | R5cdd5f46 | R7e4bd14c | R121f66e4 |
Not computed for all routes: device + fabric + stack (submit_to_cq, not separable); durable wait and publication; exchange (owner and network not separable); ssd device.
#### Cold read 1 KiB c1
| Component | Kind | owner verbs | owner TCP | verbs mTLS | hybrid kernel DIRECT | hybrid SPDK |
| --- | --- | --- | --- | --- | --- | --- |
| ssd device | device counter | 64.9 (59%) | 64.5 (56%) | 64.5 (59%) | — | 63.4 (62%) |
| network (wire, both stacks, TLS, dispatch) | derived mean difference | 26.1 (24%) | 32.9 (28%) | 28.8 (26%) | — | 19.9 (19%) |
| owner software | derived mean difference | 11.6 (11%) | 12.2 (11%) | 10.4 (9.5%) | — | — |
| residual | derived mean difference | 3.4 (3.1%) | 3.3 (2.9%) | 3.3 (3.0%) | 15.7 (14%) | 14.7 (14%) |
| client messaging (encode, decode, verify) | sampled mean | 1.7 (1.5%) | 1.3 (1.1%) | 1.3 (1.2%) | 1.1 (1.0%) | 1.1 (1.1%) |
| client coordinator | sampled mean | 1.3 (1.2%) | 1.3 (1.1%) | 1.2 (1.1%) | 1.4 (1.2%) | 1.4 (1.4%) |
| client queueing and wake-up | derived mean difference, sampled mean | 0.4 (0.4%) | 0.4 (0.3%) | 0.4 (0.3%) | 1.6 (1.4%) | 1.5 (1.4%) |
| waiting and coalescing | sampled mean | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) |
| device + fabric + stack (submit_to_cq, not separable) | sampled mean | — | — | — | 92.9 (82%) | — |
| TOTAL (API mean µs) | not measured | 109.3 | 115.8 | 109.8 | 112.7 | 101.9 |
| API p50 µs | not measured | 116.0 | 122.5 | 116.0 | 120.4 | 108.9 |
| ID | not measured | Re3876655 | Rc87b6aea | R4b6b43bd | Ra8cafbba | R8c6b50b8 |
Not computed for all routes: durable wait and publication; exchange (owner and network not separable).
#### Cold read 64 KiB c1
| Component | Kind | owner verbs | owner TCP | hybrid kernel DIRECT | hybrid SPDK |
| --- | --- | --- | --- | --- | --- |
| ssd device | device counter | 86.7 (54%) | 86.2 (42%) | — | 92.3 (66%) |
| network (wire, both stacks, TLS, dispatch) | derived mean difference | 41.1 (26%) | 85.5 (41%) | — | 26.7 (19%) |
| owner software | derived mean difference | 21.2 (13%) | 23.0 (11%) | — | — |
| client messaging (encode, decode, verify) | sampled mean | 5.4 (3.4%) | 6.0 (2.9%) | 4.0 (2.7%) | 4.0 (2.9%) |
| residual | derived mean difference | 3.1 (2.0%) | 3.5 (1.7%) | 13.9 (9.4%) | 14.1 (10%) |
| client coordinator | sampled mean | 1.4 (0.9%) | 1.5 (0.7%) | 1.4 (0.9%) | 1.4 (1.0%) |
| client queueing and wake-up | derived mean difference, sampled mean | 0.5 (0.3%) | 0.5 (0.2%) | 1.4 (0.9%) | 1.4 (1.0%) |
| waiting and coalescing | sampled mean | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) | 0.0 (0.0%) |
| device + fabric + stack (submit_to_cq, not separable) | sampled mean | — | — | 127.2 (86%) | — |
| TOTAL (API mean µs) | not measured | 159.5 | 206.1 | 147.8 | 139.8 |
| API p50 µs | not measured | 161.4 | 207.1 | 147.3 | 140.6 |
| ID | not measured | R870d07f8 | R63bb4082 | R6d2332f6 | R9c0dae5c |
Not computed for all routes: durable wait and publication; exchange (owner and network not separable).
### Network and SSD floors
Floor tables give the median and range over repetitions. SSD reads use direct queue-depth-one I/O. SSD writes separate write and sync steps; the file was pre-written. Failed direct small-block cases are failures, not timings. These are run 8, fleet `bench-0a8a6af114a64287`; each row states its mode. In these inputs `ib_send_lat` and `ib_write_lat` record half round trip and are doubled for RTT; `ib_read_lat` records round trip. The [appendix lists each network floor repetition](PERFORMANCE_PLAN_2026-11-10-APPENDIX.md#network-floor-repetitions).

RTT p50 µs: median (min–max) across repetitions. fi_pingpong reports averages, not p50, and is omitted.

| Tool | Mode | Interface | 8 B | 1 KiB | 4 KiB | 64 KiB | ID |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ib_read_lat | event | rdma0 | 12.8 (12.3–12.9) | 13.0 (12.7–13.2) | 13.9 (13.5–14.1) | 20.0 (19.6–20.1) | Ra8a139b7 |
| ib_read_lat | polled | rdma0 | 7.0 (7.0–7.0) | 7.2 (7.2–7.2) | 8.1 (8.1–8.3) | 16.0 (16.0–16.0) | R853ff1b5 |
| ib_send_lat | event | rdma0 | 20.0 (19.5–20.5) | 21.9 (21.9–22.4) | 23.7 (23.7–24.2) | 34.4 (34.3–34.9) | Rb0bdeee1 |
| ib_send_lat | polled | rdma0 | 8.9 (8.9–9.0) | 10.8 (10.7–11.0) | 12.7 (12.6–12.7) | 23.4 (23.1–23.4) | Rd9fbc4cb |
| ib_write_lat | polled | rdma0 | 7.5 (7.5–7.5) | 9.4 (9.4–9.5) | 11.1 (11.1–11.1) | 23.1 (22.9–23.4) | R06a6387a |
| tcp_rtt | block | eth0 | 23.3 (22.5–25.2) | 27.0 (23.4–27.9) | 31.2 (30.5–34.7) | 60.6 (60.3–62.7) | Rb27b40da |
| tcp_rtt | block | rdma0 | 23.9 (23.8–24.0) | 25.4 (24.8–27.2) | 28.2 (27.1–29.2) | 61.8 (60.5–62.9) | R5bd21b57 |
| tcp_rtt | busy_poll | eth0 | 14.5 (13.8–14.9) | 16.8 (16.5–17.2) | 62.6 (62.4–64.0) | 60.4 (57.2–65.5) | R5fb21fa9 |
| tcp_rtt | busy_poll | rdma0 | 15.3 (15.0–16.0) | 16.9 (16.9–17.0) | 19.2 (19.0–19.3) | 47.9 (46.4–53.9) | Re0433543 |
| tcp_rtt | spin | eth0 | 22.4 (21.9–22.4) | 24.8 (24.3–25.0) | 35.4 (27.2–36.1) | 61.8 (61.3–62.4) | R607237d9 |
| tcp_rtt | spin | rdma0 | 23.0 (22.6–23.4) | 24.0 (23.9–24.5) | 26.4 (25.5–26.8) | 59.7 (58.6–63.0) | R18e6037a |

Each cell is mean / p50 / p99 µs, each median (min–max) across repetitions.

| Region | State | 4 KiB | 64 KiB | ID |
| --- | --- | --- | --- | --- |
| 2097152 KiB | aged | 70.7 (69.3–71.2) / 71.8 (71.6–72.0) / 84.5 (82.4–92.7) | 84.5 (84.3–84.6) / 85.4 (85.2–85.4) / 100.9 (98.8–101.9) | R7d78c3c3 |
| 2097152 KiB | fresh | 70.5 (70.4–70.7) / 72.0 (72.0–72.1) / 84.5 (83.5–84.5) | 84.8 (84.2–85.0) / 85.8 (85.1–85.8) / 101.9 (99.8–101.9) | R70bfe74b |
| 65536 KiB | aged | 61.0 (60.0–62.8) / 71.3 (71.2–71.4) / 83.5 (83.5–83.5) | 85.2 (84.7–85.7) / 85.4 (85.4–85.5) / 118.3 (117.2–130.6) | R36ffbce9 |
| 65536 KiB | fresh | 61.9 (60.1–62.9) / 71.4 (71.2–71.4) / 84.5 (83.5–84.5) | 85.6 (85.0–87.7) / 85.1 (85.0–85.6) / 117.2 (116.2–187.4) | Raca95b51 |

Each step shows mean / p50 / p99 µs, each median (min–max) across repetitions. Direct 1 KiB writes failed; they are not timings.

| Mode | Block | Region | Status | Reps | Write | Sync | Total | ID |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| buffered | 1 KiB | 524288 KiB | completed | 2 | 0.5 (0.5–0.5) / 0.3 (0.3–0.3) / 1.0 (1.0–1.1) | none | 0.5 (0.5–0.5) / 0.3 (0.3–0.3) / 1.0 (1.0–1.1) | R978d8580 |
| direct | 1 KiB | 524288 KiB | failed | 2 | not measured | not measured | not measured | R61acba84 |
| fdatasync | 1 KiB | 524288 KiB | completed | 2 | 0.8 (0.8–0.9) / 0.8 (0.7–0.8) / 1.5 (1.4–1.5) | 19.3 (19.3–19.4) / 19.3 (19.2–19.3) / 26.0 (25.9–26.0) | 20.2 (20.2–20.2) / 20.0 (20.0–20.1) / 26.9 (26.7–27.0) | R41ecb4e4 |
| buffered | 8 KiB | 524288 KiB | completed | 2 | 1.8 (1.8–1.8) / 1.7 (1.7–1.7) / 2.3 (2.3–2.3) | none | 1.8 (1.8–1.8) / 1.7 (1.7–1.7) / 2.3 (2.3–2.3) | R33dc2ab6 |
| direct | 8 KiB | 524288 KiB | completed | 2 | 20.8 (20.8–20.8) / 20.5 (20.4–20.5) / 29.0 (28.8–29.3) | none | 20.8 (20.8–20.8) / 20.5 (20.4–20.5) / 29.0 (28.8–29.3) | Reec31ff2 |
| fdatasync | 8 KiB | 524288 KiB | completed | 2 | 2.1 (2.0–2.1) / 2.0 (2.0–2.0) / 2.8 (2.7–2.9) | 21.4 (21.4–21.4) / 20.8 (20.8–20.9) / 27.2 (26.9–27.4) | 23.4 (23.4–23.4) / 22.9 (22.9–22.9) / 29.4 (29.0–29.7) | R33379631 |
| buffered | 64 KiB | 524288 KiB | completed | 2 | 12.9 (12.5–13.3) / 12.4 (12.1–12.6) / 20.4 (19.8–20.9) | none | 12.9 (12.5–13.3) / 12.4 (12.1–12.6) / 20.4 (19.8–20.9) | R1a070b2f |
| direct | 64 KiB | 524288 KiB | completed | 2 | 34.2 (33.5–34.8) / 32.4 (32.2–32.7) / 42.9 (42.7–43.1) | none | 34.2 (33.5–34.8) / 32.4 (32.2–32.7) / 42.9 (42.7–43.1) | R80ab5a8b |
| fdatasync | 64 KiB | 524288 KiB | completed | 2 | 13.7 (13.6–13.7) / 13.5 (13.5–13.5) / 19.5 (19.4–19.6) | 37.6 (37.5–37.8) / 37.0 (36.8–37.1) / 46.4 (46.2–46.6) | 51.3 (51.1–51.5) / 50.6 (50.4–50.8) / 60.4 (59.9–60.9) | R0d9ef188 |
### Sync steps and owner stages
Compare the same kind of in-process timed sync step, not identical calls: owner `fdatasync` of one file against native `fsync` of every registered file plus directory `fsync`. Native sync percentiles are not measured; compare means with means. The overwrite floor is a reference, not a WAL-append floor.

At 1 KiB c1, native file-plus-directory sync mean is 27.3 µs; owner verbs is 34.0 µs and owner TCP is 28.7 µs. At 64 KiB c1, native is 114.8 µs; verbs is 159.4 µs and TCP is 152.2 µs. These are separate cases, not paired calls.

| ID | route | size_bytes | concurrency | durability | step | n | calls | mean_us | p50_us | p99_us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Rc7e58ea0 | native | 65536 | 1 | value_wal_barrier: file fsync + directory fsync | directory fsync | 4096 | 4096 | 1.7 | not measured | not measured |
| Rc7e58ea0 | native | 65536 | 1 | value_wal_barrier: file fsync + directory fsync | file fsync | 4096 | 4096 | 113.0 | not measured | not measured |
| Ra056169f | native | 1024 | 1 | value_wal_barrier: file fsync + directory fsync | directory fsync | 4096 | 4096 | 2.2 | not measured | not measured |
| Ra056169f | native | 1024 | 1 | value_wal_barrier: file fsync + directory fsync | file fsync | 4096 | 4096 | 25.2 | not measured | not measured |
| R8d29f5b7 | native | 1024 | 16 | value_wal_barrier: file fsync + directory fsync | file fsync | 4096 | 4096 | 57.9 | not measured | not measured |
| R8d29f5b7 | native | 1024 | 16 | value_wal_barrier: file fsync + directory fsync | directory fsync | 4096 | 4096 | 6.9 | not measured | not measured |
| R3df860f7 | owner0 overwrite floor | 65536 | 1 | fdatasync; overwrites a pre-written file; not a WAL append reference | file fdatasync | 100000 | not measured | 37.8 | 37.1 | 46.6 |
| R0c540d09 | owner0 overwrite floor | 1024 | 1 | fdatasync; overwrites a pre-written file; not a WAL append reference | file fdatasync | 100000 | not measured | 19.4 | 19.2 | 26.0 |
| Rd7e6aeeb | owner0 overwrite floor | 8192 | 1 | fdatasync; overwrites a pre-written file; not a WAL append reference | file fdatasync | 100000 | not measured | 21.4 | 20.9 | 26.9 |
| R649e7f1f | owner0 overwrite floor | 8192 | 1 | fdatasync; overwrites a pre-written file; not a WAL append reference | file fdatasync | 100000 | not measured | 21.4 | 20.8 | 27.4 |
| R7a998b08 | owner0 overwrite floor | 1024 | 1 | fdatasync; overwrites a pre-written file; not a WAL append reference | file fdatasync | 100000 | not measured | 19.3 | 19.3 | 25.9 |
| R64240230 | owner0 overwrite floor | 65536 | 1 | fdatasync; overwrites a pre-written file; not a WAL append reference | file fdatasync | 100000 | not measured | 37.5 | 36.8 | 46.2 |
| R40880520 | owner verbs | 1024 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 34.0 | 27.1 | 184.0 |
| R40880520 | owner verbs | 1024 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 44.2 | 37.5 | 196.2 |
| Rc7e58ea0 | native | 65536 | 1 | value_wal_barrier: file fsync + directory fsync | file + directory fsync | 4096 | not measured | 114.8 | not measured | not measured |
| Rc9cfaa51 | owner TCP | 1024 | 16 | synchronous owner WAL fdatasync | file fdatasync | 1610 | not measured | 31.8 | 28.8 | 131.4 |
| Rc9cfaa51 | owner TCP | 1024 | 16 | synchronous owner WAL fdatasync | append persistence (inclusive) | 1610 | not measured | 42.0 | 38.5 | 148.8 |
| R4ce90626 | verbs mTLS | 1024 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 27.5 | 26.8 | 37.5 |
| R4ce90626 | verbs mTLS | 1024 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 38.3 | 37.8 | 51.9 |
| R1553ed29 | owner verbs | 1024 | 16 | synchronous owner WAL fdatasync | file fdatasync | 1515 | not measured | 31.5 | 28.5 | 150.8 |
| R1553ed29 | owner verbs | 1024 | 16 | synchronous owner WAL fdatasync | append persistence (inclusive) | 1517 | not measured | 40.5 | 36.9 | 163.6 |
| R119f59bc | owner TCP | 1024 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 28.7 | 26.2 | 56.9 |
| R119f59bc | owner TCP | 1024 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 38.3 | 36.2 | 84.8 |
| R6dbb1b55 | owner verbs | 65536 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 159.4 | 150.4 | 384.4 |
| R6dbb1b55 | owner verbs | 65536 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 170.6 | 161.9 | 396.5 |
| Ra056169f | native | 1024 | 1 | value_wal_barrier: file fsync + directory fsync | file + directory fsync | 4096 | not measured | 27.3 | not measured | not measured |
| Re7acf834 | hybrid kernel DIRECT | 1024 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 28.5 | 26.1 | 148.8 |
| Re7acf834 | hybrid kernel DIRECT | 1024 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 38.8 | 36.3 | 159.6 |
| R8d29f5b7 | native | 1024 | 16 | value_wal_barrier: file fsync + directory fsync | file + directory fsync | 4096 | not measured | 64.8 | not measured | not measured |
| R0d1a0ee3 | owner TCP | 65536 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 152.2 | 149.7 | 206.3 |
| R0d1a0ee3 | owner TCP | 65536 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 163.4 | 160.9 | 215.7 |
| R852b2f71 | hybrid SPDK | 1024 | 1 | synchronous owner WAL fdatasync | file fdatasync | 4096 | not measured | 29.0 | 26.9 | 42.7 |
| R852b2f71 | hybrid SPDK | 1024 | 1 | synchronous owner WAL fdatasync | append persistence (inclusive) | 4096 | not measured | 38.5 | 36.2 | 73.4 |

The owner-stage table uses retained owner envelopes and the owner clock for synchronous c1 puts. Differences are calculated per record; their p50 is the nearest-rank p50 of those differences. Control queue contains appender and flusher queues; WAL append wraps validation and descriptor preparation. Do not add nested stages. A naive remainder that subtracts all these stages is negative at small size; this is evidence of overlap, not a positive software component. The unassigned owner interval instead subtracts flusher queue, WAL append and append persistence from handler time per record. Do not subtract owner timestamps from coordinator timestamps.
Request decode occurs before owner dispatch; response encode follows handler completion. Neither belongs to the handler remainder. The write-profile copy and timeline are separate executions; do not use their stage sums as an attribution.

| ID | Route | Size | n | Stage | Mean µs | p50 µs |
| --- | --- | --- | --- | --- | --- | --- |
| R40880520 | owner verbs | 1 KiB | 4096 | handler_ns | 62.3 | 55.2 |
| R40880520 | owner verbs | 1 KiB | 4096 | control_queue_ns | 9.5 | 9.3 |
| R40880520 | owner verbs | 1 KiB | 4096 | appender_queue_ns | 4.6 | 4.8 |
| R40880520 | owner verbs | 1 KiB | 4096 | flusher_queue_ns | 4.6 | 4.5 |
| R40880520 | owner verbs | 1 KiB | 4096 | append_wal_ns | 8.7 | 8.3 |
| R40880520 | owner verbs | 1 KiB | 4096 | append_validation_ns | 1.7 | 1.6 |
| R40880520 | owner verbs | 1 KiB | 4096 | append_descriptor_preparation_ns | 0.4 | 0.4 |
| R40880520 | owner verbs | 1 KiB | 4096 | append_persist_ns | 44.2 | 37.5 |
| R40880520 | owner verbs | 1 KiB | 4096 | wal_file_persist_ns | 34.0 | 27.1 |
| R40880520 | owner verbs | 1 KiB | 4096 | unassigned owner time (append decode, group build, other handler work) | 4.8 | 4.8 |
| R40880520 | owner verbs | 1 KiB | 4096 | persist minus fdatasync | 10.2 | 10.1 |
| R119f59bc | owner TCP | 1 KiB | 4096 | handler_ns | 57.1 | 54.8 |
| R119f59bc | owner TCP | 1 KiB | 4096 | control_queue_ns | 10.0 | 9.8 |
| R119f59bc | owner TCP | 1 KiB | 4096 | appender_queue_ns | 5.0 | 5.1 |
| R119f59bc | owner TCP | 1 KiB | 4096 | flusher_queue_ns | 4.8 | 4.6 |
| R119f59bc | owner TCP | 1 KiB | 4096 | append_wal_ns | 9.0 | 8.5 |
| R119f59bc | owner TCP | 1 KiB | 4096 | append_validation_ns | 1.7 | 1.6 |
| R119f59bc | owner TCP | 1 KiB | 4096 | append_descriptor_preparation_ns | 0.4 | 0.4 |
| R119f59bc | owner TCP | 1 KiB | 4096 | append_persist_ns | 38.3 | 36.2 |
| R119f59bc | owner TCP | 1 KiB | 4096 | wal_file_persist_ns | 28.7 | 26.2 |
| R119f59bc | owner TCP | 1 KiB | 4096 | unassigned owner time (append decode, group build, other handler work) | 5.0 | 5.0 |
| R119f59bc | owner TCP | 1 KiB | 4096 | persist minus fdatasync | 9.6 | 9.3 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | handler_ns | 298.2 | 288.5 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | control_queue_ns | 11.4 | 11.1 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | appender_queue_ns | 5.6 | 5.7 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | flusher_queue_ns | 5.6 | 5.4 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | append_wal_ns | 50.4 | 48.6 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | append_validation_ns | 5.6 | 5.4 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | append_descriptor_preparation_ns | 4.3 | 4.1 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | append_persist_ns | 170.6 | 161.9 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | wal_file_persist_ns | 159.4 | 150.4 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | unassigned owner time (append decode, group build, other handler work) | 71.6 | 70.2 |
| R6dbb1b55 | owner verbs | 64 KiB | 4096 | persist minus fdatasync | 11.2 | 11.1 |

The separate write-profile copy uses a different execution. Its stage clocks overlap and nested components are not additive. Its storage-lock and publication timings are distinct from the timeline case above.

Run 8; fleet bench-0a8a6af114a64287; owner verbs synchronous 1 KiB c1; one separate profiled repetition, R0a5ba9da. Nested stages and populations overlap; do not add them.

| Stage | n | Mean µs |
| --- | --- | --- |
| Connection handler | 4096 | 58.6 |
| Control queue | 4116 | 10.7 |
| Request decode | 4096 | 0.7 |
| Append decode | 4096 | 2.5 |
| Group build | 4096 | 1.0 |
| Connection WAL append | 4096 | 8.6 |
| Storage append | 4096 | 8.5 |
| Connection persist | 4096 | 39.0 |
| Storage persist | 4096 | 29.0 |
| WAL file sync | 4096 | 28.1 |
| Append publication | 4096 | 2.3 |
| Persist publication | 4096 | 0.3 |
| Persist serialization lock wait | 4096 | 0.1 |
| Append state lock wait | 4096 | 0.1 |
| Persist state lock wait | 4096 | 0.2 |
| Append validation | 4096 | 1.7 |
| Persist validation | 4096 | 0.2 |
| Client request encode | 4096 | 0.5 |
| Client reply decode | 4096 | 0.5 |
### Put stall and sustained puts
Run 8 stall fits describe observations, not cause. Phase counts refer to the longest retained interval, not all stall time.

| case_id | diagnostic | key policy | completions | stalls | mean spacing puts | median spacing puts | fit intercept µs | fit slope µs/put | longest interval phase counts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R92c4c91e | timeline | overwrite | 200000 | 94 | 2062 | 2049 | 11116.4 | 0.57 | owner_enqueue_started -> owner_dequeued: 19, prepared -> admitted: 10 |
| R51feece2 | timeline | new | 200000 | 94 | 2070 | 2064 | 10667.8 | 0.96 | owner_enqueue_started -> owner_dequeued: 18, prepared -> admitted: 11 |
| R17a988c9 | profile | overwrite | 200000 | 95 | 2062 | 2058 | 18685.7 | 0.53 | not measured |
| R8bfe5b29 | profile | new | 200000 | 95 | 2070 | 2068 | 14019.5 | 0.95 | not measured |

The sustained results use run 7, fleet `bench-6e40ca83d74146cf`. Do not subtract them from run 8.

| ID | Route | Tier | Reps | p99.9 µs median (min–max) | First second puts/s median (min–max) | Last five complete one-second buckets mean puts/s median (min–max) |
| --- | --- | --- | --- | --- | --- | --- |
| R9cdcd4f8 | owner TCP | native | 3 | 127784.9 (120054.0–127922.5) | 23060.0 (23035.0–23508.0) | 10397.8 (10380.0–10432.6) |
| Rcf24342a | owner TCP | synchronous | 3 | 116591.1 (115892.6–118610.1) | 24435.0 (24302.0–25085.0) | 10108.6 (9961.0–10123.4) |
| R73edea0a | owner verbs | native | 3 | 118609.3 (115816.5–122090.5) | 23181.0 (22634.0–24237.0) | 10382.4 (10314.2–10406.2) |
| R3cc10c42 | owner verbs | synchronous | 3 | 118578.1 (116099.4–126480.4) | 24988.0 (24819.0–25072.0) | 10587.4 (10318.0–10628.0) |
## Measurement corrections
`/proc/diskstats` deltas cover the gate-to-gate window, longer than the case, and include background writeback. Do not derive native-versus-owner per-operation device latency from them. The fio floor overwrites a pre-written file; it does not measure WAL append. Native reads recent fragments through the memory map and older fragments with positional reads; owner reads exact ranges. [INFERENCE] Read-ahead of mapped faults or buffered positional reads can explain excess device bytes. Compare per-call latency, not device latency per operation. [INFERENCE] A mapping can remain resident after a cache drop; test a fresh process with no WAL mapping during the drop.

The event-driven write-latency failures reflect a tool limitation; the direct small-write failures reflect the SSD logical sector size. `failed_cases.csv` records case-level size 64 KiB for both G11W failures, while `REPORT-unified.md` identifies the helper block as 1 KiB. The case size is not the helper block size. Unqualified is not zero.

| ID | case_kind | repeat | error |
| --- | --- | --- | --- |
| R58624c3d | net_floor | 0 | FioFailure: FioFailure: network floor server exited before listener readiness |
| Radc0148c | net_floor | 1 | FioFailure: FioFailure: network floor server exited before listener readiness |
| R731c6dff | net_floor | 2 | FioFailure: FioFailure: network floor server exited before listener readiness |
| Refab106d | ssd_write_floor | 0 | FioFailure: write helper exit status 1; pwrite errno=22: Invalid argument  |
| Re141dcbd | ssd_write_floor | 1 | FioFailure: write helper exit status 1; pwrite errno=22: Invalid argument  |

| ID | route | size_bytes | concurrency | verdict | reason_codes |
| --- | --- | --- | --- | --- | --- |
| R565f3b20 | native | 1024 | 16 | unqualified | cold_read_ratio_below_threshold |
| R2237ccfa | native | 1024 | 1 | unqualified | cold_read_ratio_below_threshold |
| R360ccccc | native | 1024 | 1 | unqualified | cold_read_ratio_below_threshold |
## Run 7 values
Run 7, fleet `bench-6e40ca83d74146cf`, has three repetitions per case. Tables show median and spread, not a confidence interval. Owner reads use buffered I/O after eviction and cache drop; direct hybrid reads use kernel or SPDK targets. Random keys exercise the dataset. Device reads per call indicate qualification, not proof of a media hit on every call. Puts use the named tier. Tier medians are not paired calls, so their difference is not a measured per-call fsync cost.

| ID | Route | Size | c | Durability | Cache | Tier | api_p50_us median (min–max) | api_p99_us median (min–max) | read_ops_per_success median (min–max) | successful_calls_per_second median (min–max) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R2f8ceb1a | hybrid SPDK | 1 KiB | 1 | synchronous | drop_at_measurement_gate | first_touch | 110.8 (110.7–115.9) | 133.9 (130.7–174.1) | 1.18 (1.17–1.18) | 8539 (7861–8564) |
| Rec990c3c | hybrid SPDK | 1 KiB | 16 | synchronous | drop_at_measurement_gate | first_touch | 113.2 (112.6–113.4) | 235.9 (227.5–241.3) | 1.17 (1.17–1.17) | 124707 (122579–125006) |
| R8a82b2b2 | hybrid SPDK | 64 KiB | 1 | synchronous | drop_at_measurement_gate | first_touch | 141.9 (140.6–142.4) | 159.4 (157.4–160.0) | 1.09 (1.09–1.09) | 5297 (5292–5351) |
| Rf7bca305 | hybrid SPDK | 64 KiB | 16 | synchronous | drop_at_measurement_gate | first_touch | 153.5 (153.4–154.1) | 247.2 (238.0–268.8) | 1.09 (1.09–1.09) | 74063 (73298–74400) |
| Rc40c02ab | hybrid SPDK | 8 KiB | 1 | synchronous | drop_at_measurement_gate | first_touch | 124.4 (123.6–124.7) | 140.7 (140.4–141.3) | 1.08 (1.08–1.09) | 7222 (7145–7258) |
| Rab51db6a | hybrid kernel DIRECT | 1 KiB | 1 | synchronous | drop_at_measurement_gate | first_touch | 115.3 (114.9–115.5) | 139.9 (138.6–141.5) | 1.00 (1.00–1.00) | 8539 (8524–8578) |
| R48f1eb45 | hybrid kernel DIRECT | 1 KiB | 16 | synchronous | drop_at_measurement_gate | first_touch | 113.6 (111.6–118.0) | 259.2 (230.9–284.2) | 1.00 (1.00–1.00) | 122694 (122247–125132) |
| R1fe4f545 | hybrid kernel DIRECT | 64 KiB | 1 | synchronous | drop_at_measurement_gate | first_touch | 149.6 (149.6–151.8) | 172.3 (170.9–176.5) | 1.00 (1.00–1.00) | 5010 (4926–5035) |
| Red792580 | hybrid kernel DIRECT | 64 KiB | 16 | synchronous | drop_at_measurement_gate | first_touch | 159.1 (159.0–160.5) | 403.1 (355.8–425.8) | 1.00 (1.00–1.00) | 70726 (70602–71011) |
| R1a6b90f4 | hybrid kernel DIRECT | 8 KiB | 1 | synchronous | drop_at_measurement_gate | first_touch | 131.2 (131.2–131.5) | 149.8 (148.7–170.6) | 1.00 (1.00–1.00) | 6834 (6734–6835) |
| Radb79813 | owner TCP | 1 KiB | 1 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 122.9 (122.8–123.4) | 146.6 (144.0–179.7) | 0.95 (0.95–0.95) | 7848 (7601–7864) |
| Rb59ca589 | owner TCP | 1 KiB | 16 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 132.7 (132.3–133.6) | 357.4 (315.5–357.7) | 0.95 (0.95–0.95) | 89009 (87582–89635) |
| Rf18ada8a | owner TCP | 64 KiB | 1 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 211.0 (209.9–214.5) | 245.0 (241.4–252.1) | 1.00 (1.00–1.00) | 3912 (3825–3922) |
| R19c5b6d1 | owner TCP | 64 KiB | 16 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 249.5 (248.8–252.0) | 696.8 (652.0–925.7) | 1.00 (1.00–1.00) | 34706 (33064–45019) |
| R4f95aea4 | owner TCP | 8 KiB | 1 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 142.7 (141.3–143.1) | 164.4 (161.6–164.5) | 0.98 (0.97–0.98) | 6469 (6463–6509) |
| R9ad6c8ef | owner verbs | 1 KiB | 1 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 113.8 (111.5–113.8) | 137.5 (135.4–139.1) | 0.95 (0.95–0.95) | 8483 (8472–8636) |
| R74b6f05b | owner verbs | 1 KiB | 16 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 121.7 (118.5–126.5) | 313.9 (288.9–365.0) | 0.95 (0.95–0.95) | 94728 (91253–97992) |
| Rad712f55 | owner verbs | 64 KiB | 1 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 162.1 (161.2–165.2) | 190.0 (189.2–244.4) | 1.00 (1.00–1.00) | 4884 (4729–4905) |
| R097779f8 | owner verbs | 64 KiB | 16 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 184.1 (183.3–185.8) | 504.9 (499.9–547.9) | 1.00 (1.00–1.00) | 44210 (40128–45013) |
| R07667d72 | owner verbs | 8 KiB | 1 | synchronous | evict_mapped_wal_then_drop_at_measurement_gate | first_touch | 127.5 (126.1–128.0) | 148.6 (145.4–150.7) | 0.98 (0.98–0.98) | 7204 (7152–7246) |

| ID | Route | Size | c | Durability | Cache | Tier | api_p50_us median (min–max) | api_p999_us median (min–max) | api_p99_us median (min–max) | successful_calls_per_second median (min–max) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Rde60222f | owner TCP | 1 KiB | 1 | native | normal | not measured | 87.4 (87.3–88.5) | 363.4 (331.3–407.8) | 146.3 (136.9–147.7) | 9612 (9561–9705) |
| R74a9fc72 | owner TCP | 1 KiB | 1 | synchronous | normal | not measured | 135.7 (134.2–136.4) | 409.6 (311.6–452.7) | 203.1 (192.7–227.4) | 6642 (6537–6661) |
| R25b9a596 | owner TCP | 1 KiB | 16 | native | normal | not measured | 342.8 (329.6–344.0) | 48251.5 (43325.6–59222.1) | 727.0 (650.2–907.2) | 28658 (26936–28815) |
| R3531542c | owner TCP | 1 KiB | 16 | synchronous | normal | not measured | 368.2 (350.3–382.3) | 855.4 (756.4–880.7) | 587.0 (560.8–596.3) | 41184 (39041–42539) |
| R526ec4c4 | owner TCP | 64 KiB | 1 | native | normal | not measured | 348.6 (348.1–349.1) | 753.4 (677.1–860.7) | 494.2 (492.7–508.0) | 2507 (2494–2524) |
| R423d5b77 | owner TCP | 64 KiB | 1 | synchronous | normal | not measured | 523.0 (522.9–525.4) | 988.8 (963.2–1113.0) | 867.5 (671.2–905.9) | 1728 (1728–1757) |
| Rbac70f98 | owner TCP | 64 KiB | 16 | native | normal | not measured | 1698.7 (1649.0–1709.5) | 4655.5 (4131.2–4911.1) | 3465.9 (3434.4–3529.8) | 8624 (8478–8686) |
| R95ce919e | owner TCP | 64 KiB | 16 | synchronous | normal | not measured | 1759.3 (1750.6–1790.4) | 4258.4 (3918.8–5300.4) | 3792.5 (3421.5–4299.9) | 8316 (8105–8395) |
| Rbb3c7489 | owner verbs | 1 KiB | 1 | native | normal | not measured | 77.9 (76.3–80.2) | 353.5 (324.3–390.1) | 129.3 (125.3–134.3) | 10630 (10374–10845) |
| R2b7082a2 | owner verbs | 1 KiB | 1 | synchronous | normal | not measured | 127.4 (124.9–129.9) | 435.5 (418.7–456.1) | 196.9 (188.1–197.3) | 7033 (6895–7145) |
| Rae050ca3 | owner verbs | 1 KiB | 16 | native | normal | not measured | 332.7 (332.1–333.4) | 54569.5 (44297.8–57730.8) | 657.2 (649.2–873.7) | 27407 (26823–28057) |
| Rd608b458 | owner verbs | 1 KiB | 16 | synchronous | normal | not measured | 364.8 (347.7–367.2) | 767.6 (733.4–2255.1) | 554.9 (535.4–637.8) | 40994 (40547–43224) |
| R9fed41d3 | owner verbs | 64 KiB | 1 | native | normal | not measured | 295.0 (291.1–295.3) | 999.9 (918.5–1274.4) | 435.6 (416.2–438.7) | 2887 (2880–2929) |
| Rb522390b | owner verbs | 64 KiB | 1 | synchronous | normal | not measured | 468.4 (462.4–472.1) | 1075.8 (885.7–1113.9) | 775.1 (685.9–812.8) | 1932 (1909–1957) |
| Rf3321be1 | owner verbs | 64 KiB | 16 | native | normal | not measured | 1623.9 (1601.8–1639.0) | 4020.9 (3828.1–4645.3) | 3510.9 (3217.7–3846.9) | 8817 (8774–8872) |
| Rcc543507 | owner verbs | 64 KiB | 16 | synchronous | normal | not measured | 1749.9 (1680.0–1756.6) | 4196.5 (3908.1–4510.0) | 3573.1 (3315.1–3576.3) | 8397 (8304–8775) |

Tier p50 ratios are context across separate populations, not same-call fsync cost.

| ID | Route | Size | c | Reps | Synchronous / page-cache p50 ratio median (min–max) |
| --- | --- | --- | --- | --- | --- |
| Re6bb5a02 | owner TCP | 1 KiB | 1 | 3 | 1.54 (1.53–1.56) |
| R79ce08f6 | owner TCP | 1 KiB | 16 | 3 | 1.07 (1.02–1.16) |
| Rcef2ba8b | owner TCP | 64 KiB | 1 | 3 | 1.50 (1.50–1.51) |
| Rb6e0a29f | owner TCP | 64 KiB | 16 | 3 | 1.05 (1.02–1.07) |
| Rce5afd3a | owner verbs | 1 KiB | 1 | 3 | 1.62 (1.60–1.67) |
| R4e7ce30a | owner verbs | 1 KiB | 16 | 3 | 1.10 (1.04–1.10) |
| R3eca63c4 | owner verbs | 64 KiB | 1 | 3 | 1.59 (1.57–1.62) |
| R7b01961d | owner verbs | 64 KiB | 16 | 3 | 1.07 (1.03–1.09) |
## Cross-store reference
Native and Tidehunter use one run on full-matrix fleet `bench-0d99e8256fff4891`, not run 8. Native: one process, one SSD. Tidehunter: five owners. External stores: other external contributors record five configured or observed storage nodes; Cassandra's 3 November run records five contacts, not a verified five-node ring; five storage-host SSD mounts are attested only for the inspected 4 November and final deployments; FoundationDB also has a separate log/coordinator host; some 3 November FoundationDB and TiKV host identities name a different fleet than the report path, so the path does not establish the execution fleet. The external values are medians of per-run statistics over two to six completed GCP runs on different fleets, days and sometimes binaries; `median_runs` counts those runs. Native waits for its per-put file and directory barrier; Tidehunter synchronous waits for a covering owner sync; Valkey waits for AOF `appendfsync always`, Kvrocks for a synced RocksDB WAL write and Garnet for an AOF commit. Cassandra, FoundationDB and TiKV device persistence is not verified. These are not identical crash contracts. Valkey GET reads RAM; Tidehunter uses its owner read path. Native has no network hop, but that is not a latency lower bound.
The c1 read and put columns are the mixed point-operation profile, not standalone cases. The c16 columns use the named primary profiles. Cells show p50 / p99 in µs, success-only exact nearest-rank statistics; `runs` is `median_runs` for external stores. Whole-case rates include time outside API calls and are finite-case rates, not capacity; fast Native reads and some Valkey reads are load-generator bound.

| Store | Contract | point-mixed read c1 | point-mixed put c1 | read-concurrent c16 | write-concurrent c16 | read-many32 c16 |
| --- | --- | --- | --- | --- | --- | --- |
| cassandra | CQL write acknowledged at Quorum; durable_writes=true observed;<br>commitlog fsync policy and physical device persistence not verified | R4c5db0f2: 139.2 / 220.1; runs 4 | R4c5db0f2: 202.7 / 448.1; runs 4 | R667e1835: 103.6 / 184.1; runs 4 | R1da4ee60: 263.3 / 1361.5; runs 4 | Ra205001f: 2043.0 / 2445.6; runs 4 |
| foundationdb | successful FDB transaction commit using the observed cluster storage-<br>replication configuration; physical device fsync policy not<br>independently verified | R2255ba52: 479.9 / 4785.8; runs 2 | R2255ba52: 915.0 / 1070.4; runs 2 | R792ec997: 280.5 / 402.4; runs 2 | R29bb53bb: 985.8 / 1232.6; runs 2 | Rcbcbe610: 5202.7 / 6321.1; runs 2 |
| garnet | SET/DEL reply after AOF commit; CONFIG appendonly=yes, aof-commit-<br>freq=0, aof-commit-wait=yes, aof-null-device=no observed on every<br>primary | Re7dbaa50: 58.7 / 72.6; runs 4 | Re7dbaa50: 101.4 / 164.2; runs 4 | R48d0f867: 92.2 / 147.3; runs 4 | R02d93fcd: 144.5 / 66735.7; runs 4 | R85c88053: 2221.5 / 7474.7; runs 4 |
| kvrocks | SET/DEL reply with RocksDB WAL enabled and write_options.sync=yes<br>observed on every primary | R6941aa5d: 50.0 / 62.1; runs 2 | R6941aa5d: 135.3 / 165.8; runs 2 | R47632491: 94.4 / 141.1; runs 2 | R2cd4dfed: 262.0 / 863.8; runs 2 | R6a798c0a: 2061.0 / 7242.1; runs 2 |
| native | native write acknowledgement followed by explicit Value-WAL file<br>persistence barrier | Rcb575be2: 1.0 / 1.7; runs 1 | Rcb575be2: 26.3 / 86.0; runs 1 | R93c068f2: 1.4 / 2.2; runs 1 | R09333f62: 63.8 / 167.7; runs 1 | R2bd14b89: 55.3 / 113.3; runs 1 |
| th_kernel | RF1 complete Value-WAL batch persisted (covering fsync complete) on the<br>configured owner; no replication | R28d618d1: 52.2 / 160.6; runs 1 | R28d618d1: 447.7 / 1061.3; runs 1 | R176f9741: 55.9 / 242.2; runs 1 | R948f9096: 656.9 / 1206.4; runs 1 | R1ce5a7b9: 640.7 / 2846.1; runs 1 |
| th_spdk | RF1 complete Value-WAL batch persisted (covering fsync complete) on the<br>configured owner; no replication | Rb5f18cfd: 122.0 / 247.8; runs 1 | Rb5f18cfd: 485.5 / 1192.6; runs 1 | R08dd66c6: 71.7 / 156.4; runs 1 | Rda2b3087: 702.4 / 1247.7; runs 1 | Rd9dd0e0a: 641.8 / 2740.3; runs 1 |
| th_tcp | RF1 complete Value-WAL batch persisted (covering fsync complete) on the<br>configured owner; no replication | R46c72461: 41.5 / 58.7; runs 1 | R46c72461: 128.6 / 175.7; runs 1 | R4231e6e7: 60.1 / 209.0; runs 1 | R05b82bee: 322.2 / 506.5; runs 1 | R3ef5fa63: 840.1 / 2418.1; runs 1 |
| th_verbs | RF1 complete Value-WAL batch persisted (covering fsync complete) on the<br>configured owner; no replication | R8452f893: 32.0 / 49.4; runs 1 | R8452f893: 108.8 / 235.3; runs 1 | R3933c347: 45.0 / 175.9; runs 1 | R4d61ab72: 295.3 / 575.3; runs 1 | Ra6460d3c: 688.2 / 1722.6; runs 1 |
| tikv | RawKV request success; TiKV/PD acknowledgement policy is server-defined;<br>device fsync is not independently verified | R31b628ad: 91.0 / 121.7; runs 4 | R31b628ad: 169.1 / 280.3; runs 4 | R91593185: 213.6 / 362.0; runs 4 | Rab050378: 286.1 / 526.5; runs 4 | R4a9b876d: 3840.5 / 5871.4; runs 4 |
| valkey_rdma | SET success; appendonly=yes, appendfsync=always, no-appendfsync-on-<br>rewrite=no observed; RF1, no replica acknowledgement | Rd367c647: 43.2 / 55.3; runs 5 | Rd367c647: 126.8 / 169.5; runs 5 | R66b9e720: 43.1 / 92.5; runs 5 | Rbf24384c: 172.9 / 410.5; runs 5 | R20a0678a: 320.7 / 802.6; runs 5 |
| valkey_tcp | SET success; appendonly=yes, appendfsync=always, no-appendfsync-on-<br>rewrite=no observed; RF1, no replica acknowledgement | Rced4e202: 46.2 / 61.9; runs 6 | Rced4e202: 133.3 / 182.0; runs 6 | Rdf8bc338: 53.4 / 112.8; runs 6 | R1efddb98: 183.5 / 469.1; runs 6 | R4d2eaa40: 389.0 / 807.3; runs 6 |

64 KiB c16 pure puts, finite-case rates; per-node figures assume even spread and are not measured per-node rates. Native and owner are one full-matrix run; Valkey is a median over external runs. These different contracts and fleets are context, not matched rankings.

| Store | Acknowledgement | Storage nodes | 16/nodes (assumed) | Puts/s | aggregate puts/s / node count | Payload MiB/s/node | p50 µs | Mean µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| native | per-put file and directory barrier | 1 | 16.0 | 6102.2 | 6102.2 | 381.4 | 2713.7 | 2534.7 |
| th_verbs | synchronous covering sync | 5 | 3.2 | 9936.6 | 1987.3 | 124.2 | 1462.7 | 1558.8 |
| valkey_rdma | AOF always | 5 | 3.2 | 31902.4 | 6380.5 | 398.8 | 418.7 | 454.4 |

Native's c16 per-put barrier API mean and p50 are shown above. Its c1 mixed-stream barrier put mean and p50 are 156.3 and 154.1 µs. The pure c16 case and mixed c1 case are different workloads. [INFERENCE] The figures fit a serialized-service model, but API calls overlap and no trace isolates the cause. Run 8 also uses one Native SSD against five owners; its c16 barrier and covering-sync rows are not a matched contract comparison.

For c1 mixed 1 KiB puts, owner verbs synchronous p50 is 108.8 µs and Valkey RDMA AOF always is 126.8 µs. Their resident-oriented owner and RAM GET read p50 values are 32.0 and 43.2 µs. At c16, 1 KiB pure puts are 295.3 and 172.9 µs; 64 KiB pure puts are 1462.7 and 418.7 µs. At c1 mixed, 64 KiB puts are 397.0 and 250.7 µs. At c16, resident-oriented owner and RAM GET read p50 is 45.0 versus 43.1 µs at 1 KiB, and 83.1 versus 116.3 µs at 64 KiB. These are different fleets and read and durability contracts; c1 mixed and c16 pure cases are different workloads.
## Hardware parity gate
Three items remain open: owner versus native append sync; native mapped and positional reads versus owner exact-range reads; and host or device effects versus software-path effects. Neither the overwrite floor nor window-wide counters settle append parity. Parity also needs matched SSD count and a durable-contract definition; the approved fair comparison below tests these dimensions without changing the Native engine.

G13W tests append: 48 cases, four modes, three sizes, client and owner zero, and two repetitions. It compares overwrite with fresh sparse-file append using `pwrite` plus `fdatasync`, mmap store plus `fdatasync`, and mmap store plus `fsync`. Match size and durability call. The mmap floor omits native directory `fsync`; account for it separately.

| Result | Decision |
|---|---|
| Owner append floor near native sync time | [INFERENCE] The owner path adds the gap. |
| Owner append floor near owner sync time, client floor near native | [INFERENCE] VM or SSD state explains the gap. |
| Other | Mixed; no single cause is isolated. |

Estimated use is about 45 minutes for 6 VMs. Run 7 compute was about $3.29 per VM-hour: 13.17 VM-hours cost about $43.3. This launch is about $15 compute, excluding other charges. Reserve $145 per launch against the $290 weekly cap. If run 8 reserved $145, ISO week 46 holds this as the last free slot; launcher preflight must confirm. **Not launched; needs authorization.**
## Planned fair Native comparison (not run)
The owner approved the case design, not a launch. The Native and owner arms use the same writer counts where paired; remaining differences are stated below.

All cases use 1, 8 or 64 KiB values and 4,096 keys. The unprofiled cases
have two repetitions. Native modes are `page_cache`, per-put
`value_wal_barrier`, and `value_wal_group_barrier`.

| Group | Cases | Arm and writer counts |
|---|---:|---|
| G14N | 54 | One Native Db, three modes, c1, c5 and c16 |
| G14S | 36 | Five Native Db instances on one SSD, three modes, c5 and c16 |
| G14O1 | 36 | One active owner, verbs and TCP, synchronous, c1, c5 and c16 |
| G14O5 | 36 | Five active owners, verbs and TCP, synchronous, c1, c5 and c16 |
| G14P | 6 | 64 KiB profiles, one repetition: verbs five-owner c1 and c16, TCP five-owner c1, verbs one-owner c16, Native one-Db per-put barrier c1 and c16 |

Profiles have a 4,096-call limit. They are diagnostic and do not replace
unprofiled latency controls. Group-barrier counters report commands per sync;
the grouped barrier has no per-call sync-stage profile.

`fair_native` selects these groups (168 cases). `parity` adds the 48 G13W
append floors (216 cases). Both start with a grouped-barrier canary. Each
launch reserves the same $145 cap against the weekly budget. Run only with
the owner's explicit launch authorization.
### What each arm matches and what it does not
Compare only equal writer counts, sizes, cohorts and fleets. The same writer
count and key set do not make the remaining dimensions equal.

| Arm | Writers | Instances | SSDs | Network in data path | Durable mechanics | Routing |
|---|---|---|---|---|---|---|
| Native one Db | c1, c5, c16 | 1 Db | 1 | No | Per-put barrier, grouped barrier, or non-durable page-cache acknowledgement | Key hash |
| Native five Db | c5, c16 | 5 Db in one process | 1 shared | No | Per-put barrier, grouped barrier, or non-durable page-cache acknowledgement | Key hash |
| One owner | c1, c5, c16 | 1 active owner | 1 | Yes | Owner covering sync | Operation-ID hash |
| Five owners | c1, c5, c16 | 5 active owners | 5 | Yes | Owner covering sync | Operation-ID hash |

The Native barrier syncs registered Value-WAL files and the directory; the
owner covering sync uses a different call set. Grouped Native and owner sync
both share persistence work but do not have identical mechanics. Page-cache
acknowledgement is not a durable comparison. Distributed routing hashes the
16-byte operation ID, not the key; Native five-Db routing hashes the key.
Keep repetitions separate and label every unmatched dimension.

| Observation | Reading |
|---|---|
| Native grouped barrier versus one-owner synchronous at c16 | [INFERENCE] The difference includes network and owner software, as well as the unmatched sync and routing mechanics. It is not a pure network measurement. |
| Native per-put barrier versus grouped barrier at the same writer count | [INFERENCE] The difference helps assess sharing of syncs; the mechanics still differ. |
| Native one Db versus five Db on one SSD at the same writer count | [INFERENCE] The difference includes WAL count and key routing, with one shared SSD and filesystem. |
| One owner versus five owners at the same writer count | [INFERENCE] The difference includes SSD count and per-owner load, as well as routing and host effects. |

The five-SSD Native arm, with five runners in lockstep and one on each owner
VM, is designed but not built. It needs clock-skew evidence and a controller
case type; see the Native shards feasibility report. These cases do not prove
causation, equal hardware, or a general store ranking. One fleet and two
repetitions cannot establish fleet spread or statistical significance. A
write profile does not measure physical device time.
## Engine performance guard (measured, not validated)
The owner allows engine edits only if performance is intact. The fork already changes the default path: cache generation tokens and refill checks, a generic pending table and shared index search. Until 12 November 2026 the fork also zero-initialized each buffered file read. Since that date the buffered branch of `FileReader::read_exact_at` reads into uninitialized capacity through `pread`, as the original does, without the undefined behaviour of the original (`set_len` before the read). The direct-I/O branch keeps its zeroed aligned buffer; `direct_io` is off by default. The A/B below predates this change.

The local laptop A/B used three paper-benchmark binaries with default features: upstream `ce37b18` with its own lockfile, upstream with the fork lockfile, and the fork working tree. It ran write, get and mixed at 1,024 B and 64 B, two repetitions per arm. Writes acknowledge at the page cache without a per-put barrier; the value cache was off. Residency and device reads were not measured.

The table shows initial-run target throughput in operations/s: mean (min–max) across repetitions, not pooled samples. Every configuration is **undecided: invalid**: Firefox exceeded 30% of one core during every run, violating the interference rule. These observations do not establish unchanged performance.

| Mode | Bytes | Upstream own lock | Upstream fork lock | Fork | Verdict |
| --- | --- | --- | --- | --- | --- |
| write | 1024 | 478,500 (268,000–689,000) | 562,500 (551,000–574,000) | 535,500 (410,000–661,000) | undecided: invalid |
| get | 1024 | 4,150,000 (4,120,000–4,180,000) | 4,150,000 (4,140,000–4,160,000) | 4,155,000 (4,140,000–4,170,000) | undecided: invalid |
| mixed | 1024 | 807,000 (666,000–948,000) | 844,000 (628,000–1,060,000) | 1,220,000 (1,160,000–1,280,000) | undecided: invalid |
| write | 64 | 2,395,000 (2,380,000–2,410,000) | 1,930,000 (1,460,000–2,400,000) | 2,395,000 (2,390,000–2,400,000) | undecided: invalid |
| get | 64 | 5,120,000 (5,080,000–5,160,000) | 5,085,000 (5,040,000–5,130,000) | 5,075,000 (5,010,000–5,140,000) | undecided: invalid |
| mixed | 64 | 3,590,000 (3,560,000–3,620,000) | 3,570,000 (3,560,000–3,580,000) | 3,575,000 (3,560,000–3,590,000) | undecided: invalid |

[INFERENCE] The 64 B get and mixed, 64 B write, and 1 KiB get configurations are CPU-bound. Their descriptive arm means differ by less than about 2%, except the matched-lock 64 B write mean, which includes one low observation. Without that observation its remaining rate is close to the other arm means; the table retains it. The 1 KiB write varies by more than 2× within the upstream arm, and 1 KiB mixed also varies substantially. [INFERENCE] Page cache, writeback and `kswapd` may contribute; the cause is not established, and these runs cannot show a regression.

Rerun with the interfering process closed and three repetitions. Direct I/O and cache-on variants were not covered and need separate matched tests.
## Ranked focus
Shares refer to the same profiled case's API mean. Measured components are accounting references, not proven bounds on end-to-end improvement [INFERENCE]. Preserve the contracts and use matched fleets.
For every verification, pass only when the matched experiment confirms the target improvement without a correctness, durability or p99 regression.
### Gate zero: hardware parity
**Why:** The sync gap, read patterns, SSD count, durable-contract mechanics and host effects remain unresolved. **Reference:** [INFERENCE] Software effects cannot be assigned reliably until these effects are separated. **Do:** Run the defined parity and fair Native cases after explicit launch authorization, then investigate read-pattern parity. **Verify:** Compare matched append calls and repeat native cold reads in a fresh process. **Ask before:** Launching or changing the engine or durability contract.
### 1. Completion wake-up and network at c1
**Why:** Verbs resident-read network mean is 25.1 µs (66% of API mean); durable-put network is 31.9 µs (24%). The floor table compares polled and event-driven RDMA with blocking and busy-poll TCP. **Reference:** [INFERENCE] The measured network component is an accounting reference; wake-up alone is not measured.<br>
**Do:** Use the existing `PollingPolicy::{Adaptive, Busy}` in `tidehunter/src/distributed/tcp.rs`: adaptive is the default, parks drained TCP on readiness and can park idle fabric on a provider CQ wait object; otherwise it uses bounded spin/yield. Busy drives established sessions without explicit sleep/yield; it does not eliminate storage or admission waits. The Rust benchmark `tidehunter/examples/sharded_wal_bench.rs` already accepts `--tcp-polling adaptive|busy` for owned or prepared nodes. Extend the focused comparison controller to propagate the policy to actual owner launch configuration and case identity, and record authenticated node status; do not assume a requested flag changed an external node.
Add a `polling` job field and Python controller plumbing. The controller currently hard-codes `"polling": "adaptive"` in `experiments/kv-comparison/cloud_controller.py`. No Rust change is needed for this A/B.<br>
**Verify:** On one matched fleet, interleave three repetitions each of adaptive and busy for 1 KiB c1 cache-resident reads and synchronous durable puts, separately for verbs and TCP. Keep each route's security, workload, cache, CPU placement and durability fixed across its A/B; do not compare TCP mTLS to trusted verbs as a polling effect. Record per-repetition API mean, p50, p99, throughput, errors, client and owner process CPU, polling counters and actual policy. Report CPU cost and spread; distinguish completion-observation delay from wire time.
Pass when the network component moves toward the polled RDMA floor without a p99 regression. Report the CPU cost.<br>
**Ask before:** Launching or changing the public configuration surface.
Ask before making busy polling the default.
### 2. Owner persistence and flusher hand-offs at c1
**Why:** Verbs owner persist mean is 44.2 µs (33%). Durable wait and publication is 7.5 µs; waiting and coalescing 6.0 µs; client queueing 3.9 µs. The timeline table shows flusher queue wait. The separate profile table shows small storage-lock waits and publication and a gap between connection append persistence and storage persistence. [INFERENCE] The latter gap may include flusher hand-off, wake-up and durable-frontier waiting; it is not measured as a single stage. **Reference:** [INFERENCE] Measured intervals are accounting references, not proven bounds.<br>
**Do:** Instrument enqueue to flusher dequeue, sync start/end, durable-frontier advance, waiter notification and resume, and reply publication; retain batch/group size and queue depth. Correlate by owner-local request or batch identity and keep sync separate from scheduling, with nested intervals labelled.
In `tidehunter/src/distributed/tcp/control.rs`, split the connection `append_persist_ns` span around `flush_batch` wake-up and the durable-frontier wait. Test skipping a coalescing wait when nothing can coalesce; first check the existing singleton behavior.<br>
**Verify:** Repeat matched 1 KiB c1 synchronous puts with and without the candidate on one fleet, collecting owner hand-off distributions, API p50/p99 and CPU alongside sync timings. Confirm acknowledged batches survive recovery and publication ordering remains intact; do not subtract clocks across hosts.
Pass when the gap is measured and API p50 falls by the measured amount on a matched fleet.<br>
**Ask before:** Launching or changing durability or publication guarantees.
### 3. Software that grows with value size
**Why:** At 1 KiB, verbs client coordinator mean is 20.1 µs and owner software is 18.1 µs. At 64 KiB, they are 91.9 µs (20%) and 127.5 µs (28%). The owner-stage table shows growth in WAL append and unassigned owner time. `tidehunter/src/distributed/codec.rs`, `PayloadHasher` and `record_digest`, hashes the encoded batch payload with BLAKE2b, excluding the digest field. [INFERENCE] Decode and digest verification may contribute to the growth. The measured components are accounting references, not proven gain bounds.

[INFERENCE] Size-dependent software may contribute to the cross-store gap; the separate full-matrix 64 KiB c1 mixed put p50 is 397.0 µs for owner synchronous versus 250.7 µs for Valkey AOF always. Fleet and contracts differ, so these values do not measure that contribution.<br>
**Do:** Add 64 KiB write-profile copies for verbs and TCP, instrumenting coordinator encode and digest, owner decode, digest verification, group construction and copies/bytes; separate allocation from hashing and avoid adding nested spans. Compare 1 KiB and 64 KiB under the same contract before choosing a copy or allocation reduction.
Micro-benchmark `record_digest` on 64 KiB payloads on the fleet CPU. If hashing explains the growth, evaluate hashing once, a faster or parallel hash, or an existing CRC only where the integrity contract permits it.<br>
**Verify:** Collect clean and profiled matched cases on one fleet and compare stage counts, bytes and CPU plus API mean/p99; validate identical decoded values and corruption rejection. Profiled copies diagnose, not substitute for unprofiled latency.<br>
**Ask before:** Launching or changing digest, wire or persisted formats.
Ask before any option that changes the wire or persisted format.
### 4. Coordinator pipeline waits at c16
**Why:** Verbs exchange is 108.1 µs (26%); durable wait and publication 105.2 µs (25%); waiting and coalescing 81.0 µs (19%); client queueing and wake-up 70.0 µs (17%); coordinator 50.7 µs (12%). The cross-store table gives Valkey RDMA put p50 as context on another fleet and contract, not a subtraction. [INFERENCE] The measured intervals are accounting references, not proven end-to-end bounds.

[INFERENCE] Pipeline waits may contribute to the cross-store c16 gap. The separate full-matrix 1 KiB c16 pure put p50 is 295.3 µs for owner synchronous versus 172.9 µs for Valkey AOF always. Different fleets and contracts prevent attributing that gap to the run 8 waits.<br>
**Do:** Trace admission and preparation, staging/coalescing, owner dispatch and exchange, receipt, ordered publication and caller wake-up; retain queue depth, batch/group occupancy and per-stage identities. `remote_db/owner_dispatch.rs` already has early coalescing; measure its actual decisions before altering scheduling.
Read authority intake and the pipeline turn in `tidehunter/src/distributed/remote_db/concurrent.rs` against the timeline. Measure each queue depth, then fuse stage hand-offs one at a time on a matched fleet.<br>
**Verify:** Repeat c16 synchronous timeline and clean cases on one fleet; compare queue and stage distributions, throughput, API p99 and CPU, with valid complete batches and ordered publication. Diagnose head-of-line waits rather than adding overlapping spans.<br>
**Ask before:** Launching or changing durability or public configuration.
### 5. Put stall
**Why:** The stall table gives spacing and fitted duration intercept and slope for overwrite and new keys. The separate run 7 sustained table gives throughput for the first second and last five complete one-second buckets, and p99.9. [INFERENCE] Checkpoint work that scales with cumulative puts may contribute; `tidehunter/src/distributed/remote_db/checkpoint_lifecycle.rs` implements capture, persistence, adoption and cleanup, but does not identify the cause. The observed stall is an accounting reference, not a general speedup bound.<br>
**Do:** Retain existing checkpoint generation events and diagnostics for capture, receipt history, index persistence, root adoption, live installation and cleanup; add missing admission-block and queue timers, cumulative completed puts, keys and bytes at each generation. Correlate same-process phase boundaries with retained put stalls.<br>
**Verify:** Repeat sustained overwrite and new-key cases on a matched fleet, fit stall duration against cumulative puts and checkpoint size by repetition, and compare timing and slope with checkpoint phases and no-stall intervals. Correlation is not causation; only a matched candidate that changes the suspected phase and reduces stalls supports it. Preserve recoverability and p99.9.
Pass when the fitted slope falls and the first-second to last-five-complete-bucket throughput ratio approaches one.<br>
**Ask before:** Launching or changing persisted formats, checkpoint semantics or durability.
### 6. Batch writes
**Why:** The batch table compares native and verbs for 32 × 1 KiB and 32 × 8 KiB; the larger batch has a decomposition. [INFERENCE] Measured software and hand-offs are accounting references, not proven end-to-end gain bounds.<br>
**Do:** Instrument batch validation, encoding/digest, copies, dispatch, owner decode/group build, sync and publication with batch cardinality and bytes; distinguish per-batch fixed work from per-value work without weakening atomicity.<br>
**Verify:** On a matched fleet compare the existing batch32 sizes and scalar controls, reporting whole-batch latency and values/s as well as latency per value (whole-batch latency divided by completed values, not a value-level latency distribution). Retain per-repetition counts, p99 and correctness; native has no batch profiler.<br>
**Ask before:** Launching or changing wire, persisted format or atomic batch guarantees.
### 7. Hybrid client residual on reads
**Why:** The buffered hybrid resident-read residual is 11.3 µs (27% of API mean). It is derived, not a timed stage. [INFERENCE] It is an accounting reference, not a proven end-to-end gain bound.<br>
**Do:** Add sampled client-local timers from API dispatch through index lookup, read-lane admission and dispatch, NVMe-oF submission/completion, result validation and caller wake-up; identify nested spans and cache/source route. Existing `remote_db/pending_read.rs` drives read lanes and coalesced flights.<br>
**Verify:** Compare buffered and direct routes on one fleet with matched keys, sizes and concurrency, recording cache and I/O evidence, per-stage sample counts, API p99 and CPU. Attribute only non-overlapping measured spans; the residual remains derived until timed.<br>
**Ask before:** Launching or changing public queue geometry.
### Not worth work now
The SSD read component and matched floor are close in the decomposition and floor tables. Trusted verbs and verbs mTLS isolate TLS; their measured difference is small. [INFERENCE] These measured components or route differences are accounting references that make them lower priority; they do not prove an end-to-end bound or a general device or TLS ranking.
## Evidence gaps and next experiments
Add a native read profiler and measure batch reads. Fill missing TCP and hybrid batch and delete cases: about 50 cases, 2 repetitions, and 1.4 fleet hours. The controller `hybrid_put` kind needs a small extension. Profiled copies, batch and delete cases have one repetition; repeat before interpreting spread. Add a `sync` and dirty-page drain before the window, then retain gate-to-gate counter evidence. For native cold reads, drop caches while no process maps the WAL, then use a fresh process. No new run is authorized here.
The focused polling dimension needs Python job-field and controller plumbing.

The 64 KiB owner decode and group-build stages have not been measured by a write-profile copy. Add one verbs and one TCP copy to the next authorized run.

Polling A/B evidence is missing in this focused controller: the Rust policy and standalone benchmark option exist, and `experiments/aws-sharded-wal/transport_floor.py` has separate policy plumbing, but that does not prove the focused `experiments/kv-comparison/cloud_campaign.py` propagates and verifies it. Implement and retain launch/status evidence before interpreting the A/B; no result is implied.
## Leased extents (implemented 2026-11-11; run 11 on hardware)
The ranked items above keep the owner RPC on the put path. The opt-in `leased` write mode removes it. Design and contract: `experiments/nvmeof/LIVE_WAL.md`, section "Leased extents".

**What changed.** The owner leases the rest of a WAL fragment to the coordinator as one WAL allocation. The coordinator allocates positions in it, writes one CompleteGroup v3 frame per group through kernel NVMe-oF, and acknowledges on its own completion. The owner adopts frames asynchronously, persists the frontier root, fences and recovers. The disk format is unchanged. `reserved` stays the default. Run 11 ran `leased` on the 2-VM fleet (72 of 72 cases); it has no power-loss or kernel-crash qualification.

**Acknowledgement contract.** Native tier: page-cache write completion at the target (paper §3.1). Synchronous tier: completion of a write with `RWF_DSYNC` (FUA on a block namespace; data sync on the nvmet file backend). The owner frontier root follows asynchronously. This is not the same contract as the owner-RPC synchronous tier, which acknowledges after the owner root: compare the two only with this difference stated.

**Expected cost on the 2-VM fleet (written before run 11).** [INFERENCE] One NVMe-oF write replaces the owner RPC: the measured kernel NVMe/RDMA remote read adds about 40 µs over the local SSD read in this deployment, and the durable owner-RPC put costs 125–134 µs. The leased synchronous put removes the request and reply transport stages, the owner software stages and the owner waits from the acknowledgement path, and keeps one FUA write. Run 11 did not confirm this at 1 KiB: the FUA write alone takes 122.9 µs.

**Read path.** A scalar read of live data with a cached grant now runs on the caller thread (no actor and lane-worker hop). Aligned ranges that overlap or touch are merged into one read command (at most 32 members).

### Run 11 hardware results (11 November 2026)
Fleet `bench-775352a846e64517`: 2 `z4m-highmem-16` VMs, one coordinator and one owner, kernel NVMe-oF over RDMA with buffered file namespaces. 72 of 72 cases, no failed or rejected call. Two repetitions per cell; ranges are the two values. Full tables, contracts and gates: `FOCUSED_CAMPAIGN_2026-11-07.md`, "Run 11 results". Data: `checkpoint-work/focused-launch-20261111-leasedab2/analysis/output/` in the parent workspace.

| Put, p50 µs | Owner RPC | `leased` | `reserved` |
|---|---:|---:|---:|
| synchronous, 1 KiB, 1 writer | 115–119 | 153–154 | 426–428 |
| native, 1 KiB, 1 writer | 63.9–64.8 | 62.5–62.9 | 418–427 |
| synchronous, 64 KiB, 1 writer | 426–431 | 313 | not run |
| native, 64 KiB, 1 writer | 264–268 | 184–188 | not run |
| synchronous, 1 KiB, 16 writers | 482–500 | 486–501 | 746–771 |
| synchronous, 64 KiB, 16 writers | 4422–4621 | 2545–2596 | not run |

Synchronous owner RPC acknowledges after the owner root; synchronous `leased` acknowledges after the FUA write. These are different contracts.

- **1 KiB.** `leased` does not beat the owner RPC. It is equal in the native tier and 29 to 33% slower at p50 in the synchronous tier at one writer. The NVMe-oF write takes 30.2 µs with a page-cache acknowledgement and 122.9 µs with FUA (profiled copies, one repetition each). The owner RPC pays 51 to 55 µs for its sync (difference between its tier p50 values; separate cases).
- **64 KiB.** `leased` has a 27 to 44% lower p50 than the owner RPC in all four cells.
- **Against `reserved`.** `leased` is 2.8 times (synchronous) and 6.7 times (native) faster at 1 KiB, one writer.
- **Reads of unsealed data.** One reader: the inline read equals the owner RPC read at 1 KiB (30.6–31.0 against 30.3–30.6 µs) and is 11 to 13% faster at 64 KiB (49.9–51.0 against 57.2–57.4 µs). The inline route is 19 to 24% faster than the lane worker at one reader and gives no difference at 16 readers, where it admits 14 to 17% of the reads.
- **Regression item 6.** Owner verbs, native, 1 KiB, one writer: p99 107 to 108 µs (run-8 reference 127 µs, five owners).

### Local fixture results (workstation, not hardware)
**These numbers give an idea, not a cluster result.** The workstation has an NVMe SSD, so the software cost and the local write and sync cost are real. It cannot run NVMe-oF or RDMA: a regular file stands in for the namespace and loopback TCP for the control path. Use the numbers to see which software steps cost time and that each route works. Do not use them to predict the fabric part of cluster latency.

Harness: `tidehunter/src/distributed/remote_db/live_bench_tests.rs` (`#[ignore]`). Outputs: `checkpoint-work/p11-local-bench/run-819289` and `run-824525` in the parent workspace. The namespace is a regular file on ext4 on a consumer NVMe SSD. The control path is loopback TCP. No nvmet, no fabric and no hardware FUA are exercised. The numbers show software cost and control flow only.

That directory holds 68 invocations of the harness (full matrices, read-only runs and diagnostic runs on several code states of 11 November; most are partial or instrumented). The table below uses two: they ran on the code of the morning of 11 November with no other build or test process. The harness still flags most rows as interfered (1-minute load above 2). Each cell lists the p50 in µs of the four samples in run order (invocation 1 repetition 1 and 2, invocation 2 repetition 1 and 2). Samples are not pooled. n = 4096 puts for 1 KiB and 1024 for 64 KiB.

Two later pairs exist: one on the code of the afternoon (`run-2112995`, `run-2117848`) and one on the final code of the evening (`run-1566487`, `run-1571407`; zero failed or rejected calls). The 0.8 ms stall of the first pair did not appear in either. The final pair, p50 in µs, same layout:

| Tier | Value | Writers | Owner RPC (A) | Leased, O_DIRECT file (C) | Leased, buffered file (C-buf) |
|---|---|---:|---|---|---|
| native | 1 KiB | 1 | 35.8 / 37.1 / 36.5 / 41.0 | 26.6 / 25.6 / 26.9 / 26.2 | 23.9 / 24.2 / 25.0 / 22.7 |
| native | 1 KiB | 16 | 159 / 159 / 170 / 126 | 111 / 129 / 117 / 118 | 113 / 103 / 112 / 104 |
| native | 64 KiB | 1 | 194 / 200 / 194 / 201 | 115 / 115 / 119 / 120 | 93.4 / 96.4 / 94.1 / 91.8 |
| native | 64 KiB | 16 | 3058 / 3022 / 3034 / 2903 | 1567 / 1549 / 1610 / 1561 | 1367 / 1381 / 1408 / 1341 |
| synchronous | 1 KiB | 1 | 399 / 407 / 399 / 428 | 492 / 620 / 507 / 518 | 662 / 2226 / 654 / 658 |
| synchronous | 1 KiB | 16 | 611 / 645 / 2508 / 627 | 488 / 525 / 2622 / 590 | 1161 / 1079 / 1244 / 1098 |
| synchronous | 64 KiB | 1 | 666 / 657 / 669 / 655 | 714 / 589 / 576 / 562 | 2495 / 2706 / 731 / 787 |
| synchronous | 64 KiB | 16 | 3811 / 3758 / 3968 / 4982 | 3034 / 4383 / 5056 / 5797 | 2104 / 2366 / 9857 / 3508 |

Reading of the final pair:
- Native tier: the leased p50 is lower than the owner RPC p50 in four of four samples of every cell, on both files.
- Native tier, 1 KiB, one writer, mean: owner RPC 99.5 to 104 µs; leased buffered 48.8, 51.0, 53.1 and 129 µs (one of four higher); leased O_DIRECT 61.5, 262, 62.4 and 283 µs (two of four higher). Three to eight calls of at least 5 ms hold 43 to 88% of the summed API time of a leased case, and four such calls hold 58 to 61% of an owner RPC case: the reference-history spills (below) set the mean on both routes.
- Synchronous tier: the leased route is not faster here. At 1 KiB and one writer its p50 is higher than the owner RPC p50 in four of four samples on both files. The other cells are mixed. The local data sync dominates, and the two routes acknowledge under different contracts.
- Reads: inline p50 43.1, 40.2, 51.4 and 56.3 µs against 57.0, 57.7, 82.9 and 89.2 µs through the lane worker (lower in four of four). The lane-worker and batch read arms were noisier in the evening than in the morning; the morning harness logic, restored for a control, was equally noisy in the evening.

The first pair (code of the morning) follows.

| Tier | Value | Writers | Owner RPC (A) | Reserved (B) | Leased, O_DIRECT file (C) | Leased, buffered file (C-buf) |
|---|---|---:|---|---|---|---|
| native | 1 KiB | 1 | 41.7 / 40.6 / 36.7 / 37.0 | 2418 / 2598 / 2948 / 2166 | 27.3 / 26.3 / 26.7 / 26.9 | 25.2 / 21.6 / 23.4 / 23.4 |
| native | 1 KiB | 16 | 168 / 173 / 159 / 161 | 4114 / 4405 / 4103 / 13497 | 115 / 121 / 129 / 117 | 103 / 107 / 102 / 107 |
| native | 64 KiB | 1 | 206 / 212 / 279 / 190 | 7883 / 2765 / 2936 / 2841 | 128 / 129 / 129 / 132 | 94.4 / 96.7 / 92.8 / 100 |
| native | 64 KiB | 16 | 3574 / 2972 / 3667 / 3884 | 7982 / 14021 / 6740 / 12881 | 2506 / 3079 / 1538 / 2906 | 1390 / 1493 / 1453 / 1458 |
| synchronous | 1 KiB | 1 | 898 / 438 / 1511 / 1499 | 2171 / 2140 / 2182 / 2231 | 542 / 2241 / 519 / 548 | 678 / 765 / 668 / 688 |
| synchronous | 1 KiB | 16 | 691 / 730 / 2948 / 690 | 4353 / 14609 / 4093 / 4579 | 729 / 2913 / 543 / 506 | 12153 / 4281 / 1017 / 758 |
| synchronous | 64 KiB | 1 | 883 / 764 / 865 / 734 | 2718 / 7753 / 2939 / 2843 | 635 / 1184 / 624 / 1928 | 784 / 747 / 787 / 2427 |
| synchronous | 64 KiB | 16 | 4318 / 4470 / 4552 / 4819 | 7233 / 6990 / 8293 / 7626 | 3044 / 5024 / 4439 / 4319 | 2080 / 2309 / 2229 / 2341 |

Reading, with limits:
- **Native tier, matched page-cache contract (A against C-buf).** The leased p50 is lower in all four samples of every cell. The mean does not follow the p50: at 1 KiB and one writer the leased mean is 72.8 to 276 µs against 133 to 169 µs for the owner RPC, so one of four leased samples is worse.
- **Synchronous tier.** No consistent difference. The owner-RPC p50 itself spans 438 to 1511 µs at 1 KiB and one writer, and the leased route is slower in several samples (for example C-buf at 16 writers: 12153 µs). The local data sync dominates. The two routes also acknowledge under different contracts (see above).
- **Tail.** A stall of about 0.8 ms hits 0 to 3% of the 1 KiB operations and 4 to 32% of the 64 KiB operations in every arm, including the owner-RPC arm and the reads. p99 is therefore 0.8 to 1 ms in almost every row and the means are not comparable. The cause is not known. Cgroup CPU throttling is absent and the listed C-state exit latency (230 µs) does not match. The run-8 hardware reference shows no such stall on the owner route (owner verbs, native, 1 KiB, one writer: p99 127 µs), so the next hardware run must check that row for a regression.
- **Tail, follow-up (11 November, later the same day).** The stall is not a regression of the owner route and was not reproduced. Method: `kv-research`, one local TCP owner process, native tier, one caller, built from a clean export of `0d9bd94` and from the working tree, runs interleaved, order alternated (`checkpoint-work/p12-stall/`). 1 KiB put, 4 pairs, n = 8192: p50 46.3 to 52.1 µs (base) against 48.6 to 51.7 µs (current); p99 122 to 158 µs against 119 to 135 µs; calls above 400 µs 0.24 to 0.38% against 0.24 to 0.31%. 1 KiB read, 4 pairs: p50 12.5 to 13.5 µs against 12.4 to 13.4 µs (the read route was not verified from evidence). 64 KiB put, 16 pairs: the current p50 is higher in 8 pairs and lower in 8. The same harness binary that showed the stall was then run again three times (idle machine; directly after 75 s of load on all 20 CPUs; during a concurrent buffered file write): 0.17 to 0.22% of the 1 KiB puts and none of the inline reads exceeded 400 µs. Thermal throttling and dirty-page writeback are therefore not confirmed as causes. The cause of the stall in the two earlier invocations stays unknown; their means and p99 values stay unusable.
- **Owner RPC at 16 writers, native.** p99 is 81 to 139 ms and throughput is 7.0k to 10.1k puts/s, against 84k to 136k puts/s for the leased arms. The stalls of the owner arm cause most of this gap. Do not carry the ratio to hardware. The cause was found later: see "Reference-history spills" below.
- **Backpressure on the leased put path.** Each 1 KiB one-writer native case shows 2 to 5 synchronous adoptions on the put path, and one sample has a p99.9 of 38 ms. The first reading here ("the owner adopts more slowly than the coordinator writes") was wrong about the cause. The frame work is fast; the owner waits for its reference-history writer. See "Reference-history spills" below.
- **Reserved route.** Every reservation runs one owner barrier, also in the native tier (4096 barriers for 4096 puts). Its p50 is 2.1 to 2.9 ms here. This is existing behavior, not part of this change.
- **Copy removal.** The in-place frame encoding was not measured in a controlled before-and-after pair.

Reads, warm, 1 KiB, one reader, n = 8192 (512 for the batch), same four samples:

| Read route | p50 (µs) | Mean (µs) |
|---|---|---|
| Inline on the caller thread (R1) | 39.9 / 39.1 / 40.0 / 39.0 | 50.6 / 49.3 / 44.6 / 40.0 |
| Lane worker, inline disabled (R2) | 49.7 / 48.3 / 50.2 / 48.0 | 59.9 / 64.3 / 64.5 / 59.7 |
| Batch of 32 keys, per call (R3) | 199 / 196 / 196 / 191 | 209 / 212 / 223 / 196 |

The inline route is 8 to 11 µs lower at p50 in all four samples. The batch issues 1.0 read command per 32-key call (counter). No unmerged batch arm exists, so the latency effect of merging is not measured.

### Reference-history spills (found 11 November; existing owner mechanism)
Source: temporary stage timing on the workstation fixture, removed afterwards; evidence in `checkpoint-work/p12-adoption-cost/evidence.json`. Fixture results, not hardware results.

**Mechanism.** The owner keeps at most 1024 batches that its durable history root does not cover. When the window is full, one spill appends, for each batch, an 80-byte descriptor, a 256-byte receipt slot and copy-on-write nodes of four Patricia indexes (identity, version, address, ordinal). It then syncs the three files, writes and syncs a new root, renames it and syncs the directory. An owner RPC append and a leased adoption both wait for it.

**Cost on the workstation.** One 1024-batch spill took 74 to 85 ms. The index inserts took 65 to 75 ms of that; the syncs and the root took 5 to 8 ms. In the owner RPC arm (native, 1 KiB, one writer) four spills per 4096 puts took 53 to 60% of the summed API time. In the leased arms the same spills appear as three history-full settles per 4096 puts. The frame work of adoption itself took about 8 µs per 1 KiB frame, less than the coordinator's put period.

**Metadata volume.** At 1 KiB the history appends 6.9 to 9.2 KiB of index nodes per batch plus 336 bytes: 7.0 to 9.3 times the value bytes. At 64 KiB the ratio is 0.11 to 0.12. The index bytes per batch grow with the number of retained batches. No retention bound, checkpoint or value-file retirement prunes these files.

**Paper.** Tidehunter claims near 1x write amplification for large values (§1, contributions; §4.4). This history is an addition of this fork. At 1 KiB its owner-side metadata writes exceed the value writes by a factor of 7 to 9 on this fixture.

**Hardware (run 11).** The stall exists on hardware at 1 KiB. Owner RPC, native, one writer: the calls above 5 ms fall at puts 1016, 2040, 3064 and 4088 of the preload in both repetitions (48 to 62 ms each) and hold 34.5 to 35.4% of the summed API time of the measured puts. `leased` holds 39.6 to 39.8% in three calls of 58 to 64 ms. At 16 writers the share is 45.6 to 46.2% on both routes in the synchronous tier and 64.0 to 70.8% in the native tier; all three routes end between 13,300 and 17,100 puts per second. No call above 5 ms occurs at one writer in the synchronous tier or at 64 KiB at one writer. [INFERENCE] The owner was not traced; the period of 1,024 puts, the duration and its growth match the spill. On the `leased` route `adoption_history_full_settles` stays zero: a 4 MiB extent holds 1,024 frames of 4,096 bytes, so the extent switch comes first, and only 15.8 ms of each long call is attributed (extent switch stage).

**Changed.** The index insert kept the verified root-to-leaf path of its first walk instead of reading and verifying that prefix again. The appended bytes, their order and every sync are identical (a byte-for-byte test keeps the old algorithm as reference). Index time for one matched spill fell from 65–75 ms to 46–51 ms in three of three traced comparisons in each arm. One opportunistic check is lost: the old second read could detect a node that changed on disk between the two reads of one insert.

**Not changed (needs a decision).** Removing the stalls needs one of: building the index of a spill in bulk; starting the spill before the window is full; another index layout with pruning; a larger resident window. Each changes the appended format, the persistence schedule or an observable bound.

**Hardware experiments (items 1, 5 and 6 ran in run 11).**
1. Done in run 11. A/B at a matched contract: owner RPC, `reserved`, `leased`; 1 KiB and 64 KiB; concurrency 1 and 16; both tiers; selection `leased_ab_compact` with two repetitions (72 cases, 2 VMs). Two repetitions on one fleet give no confidence interval.
2. Power-loss and kernel-crash qualification of the FUA acknowledgement on the nvmet file backend. Until then the synchronous leased contract is a source-derived claim.
3. More than one frame in flight per stream (published WAL results favor 8–9 outstanding groups). The implementation keeps one.
4. NVMe poll queues with `IOPOLL`, registered buffers and files, and `DEFER_TASKRUN`. They change queue geometry: ask first.
5. Done in run 11 for the short cases: the stall appears on both routes at 1 KiB (see "Hardware (run 11)" above). A sustained case and an owner-side trace are missing.
6. Done in run 11: owner verbs, native, 1 KiB, one writer has a p99 of 107 to 108 µs.
7. Cause of the FUA cost on the buffered nvmet file backend (93 µs over the page-cache write at 1 KiB), and the direct-I/O target as an alternative. Not isolated.

**Campaign plumbing (done and run 11 November).** `cloud_campaign.py` has the selection `leased_ab_compact`; the controller prepares the live route in focused mode for it only; the evidence gates and `leased_ab.py` read the leased and inline counters.

## NVMe-oF data path with original defaults (run 12 on hardware, 12 November 2026)
Goal: puts and reads of the distributed store use NVMe-oF as the data path, with the defaults of the original engine, and are compared with the `native` backend of the same source tree. Selection, gates, settings and the full results: `FOCUSED_CAMPAIGN_2026-11-07.md`, "NVMe-oF data path" and "Run 12 results". Design text: `experiments/nvmeof/LIVE_WAL.md`. Fleet `bench-b2eec4ef24bc4f90` completed 66 cases and 453,440 successful measured workload calls. The archive is preserved and both VMs are deleted.

**Findings in the source that this work answers.**
- A live read of a WAL file that the lane had not opened needed an owner request (history lookup, export, check connect) and a controller connect. A lane kept at most eight files.
- The owner history spill stalls puts for 50 to 65 ms for each 1,024 batches (run 11).
- The research harness ran owners with 4 MiB fragments and 1 GiB WAL files; the original engine uses 128 MiB and 10 GiB.
- Mapped-WAL eviction rejected a live stream, so no live read case could be cold.
- The live controller had one fixed geometry (one I/O queue of depth 32, no poll queue).

**Changes.** Each added behaviour is off by default and is enabled per case.

| Change | Default | Run 12 value |
|---|---|---|
| Buffered file read without buffer zeroing (`file_reader.rs`) | on (parity with the original) | on |
| Owner WAL layout: 128 MiB fragments, 10 GiB files, 16 mapped fragments (`PartitionConfig::layout`); kv-research label `owner_wal_geometry` | `native_default` in the engine; older selections pass `research` | `native_default`; `sui_harness` in G18X |
| Mapped-WAL eviction accepts an idle live stream (no open extent, no pending reservation) | on | used by cold cases |
| Live controller queue geometry (`LiveQueueGeometry`) and a polled ring for the caller-inline read | 1 queue × 32, no poll queue | also 4 × 128 and 1 poll queue |
| Owner history persistence (`history_persistence`) | `on_disk` | `memory_only` in every remote case (needs `test-utils`) |
| Live export mode (`export_mode`) | `on_demand` | `per_owner` in every live case |

**Permanent export.** With `per_owner` the owner adds each WAL file of the active epoch as a namespace of one subsystem (1,024 files for each subsystem). The coordinator and the owner compute the subsystem name, the namespace id and the namespace UUID from database, partition, epoch and file index. A lane sends one `OpenLiveExports` request for each connection; after that a read is a local open of a block device and an NVMe-oF read. `per_file` is the fallback with one subsystem for each file. After an owner stop, a connection loss or a retirement, a read returns an error until the coordinator opens the database again.

**What `memory_only` removes from the result.** The owner writes no history file. It cannot restart with data, cannot retire a WAL file and cannot serve an owner RPC read of an old batch. Run 12 therefore has no owner RPC read arm and does not assess recovery or retries. A design draft for recovery without history files is in `NATIVE_STYLE_RECOVERY_DRAFT.md`.

**Verified on the cluster, not the workstation.** Permanent `per_owner` export, namespaces added to a connected subsystem (18 WAL files), poll queues, 4 × 128 queues, idle live-WAL eviction and device-qualified cold live reads all executed. All 34 live cases report zero measured controller connects, payload RPCs, live errors and unknown writes. The auxiliary controller inventory still uses old NQN discovery and does not independently archive all permanent controllers. Owner-crash, connection-loss and power-loss cases did not run.

**Result.** In short 1 KiB page-cache put cases the history stalls above 5 ms disappear, with zero history spills/waits in the two diagnostic copies. Compared with run 11, one-writer leased p50 falls from 62.5–62.9 to 54.9–56.6 µs; this remains far above same-fleet native at 1.95–2.03 µs. Warm remote 1 KiB read p50 is 28.8 µs, or 22.2 µs with polling; native is 1.02 µs. Only coordinator-cache hits approach native latency. Historical runs differ in fleet, binary, WAL geometry and history persistence; they are not an isolated optimization A/B.

**Unresolved regressions.** The 64 KiB concurrent put p99 is worse than run 11 on both routes. The 200,000-put leased case has p99.9 102.3 ms, maximum 143.6 ms, 1,504 calls above 5 ms and a rate that falls from 28,032 to 11,190 calls/s (last five complete seconds). History-full settles are zero and recorded extent/adoption settles do not explain these stalls. Wider queues improve some medians but worsen the warm 1 KiB p99; defaults are unchanged. Native cold 1 KiB and batch reads fail device qualification and must not be ranked as SSD reads. Native cold 64 KiB performs substantially more device I/O per call than the live route.

**Report correction.** The cloud exit failed only at final reporting: four standalone floor jobs have an owner host count but no running storage owner. The reporter incorrectly required owner durability receipts for them. A local report-only fix, covered by ten passing floor/report tests, now admits 66 of 66 cases. The original `incomplete` receipt and archive remain unchanged. `memory_only` still has no recovery with data; no production durability claim follows from these timings. Sui replay and its package seal are not complete. No additional cluster or run will start without a new instruction.

**Ask before:** a change of the default queue geometry, of the default export mode or of the durability contract.

## Recovery model and stall fixes (implemented 12 November 2026; run 13 on hardware)

Source of each problem: run 12 (`FOCUSED_CAMPAIGN_2026-11-07.md`, "Run 12 results"). Paper: A. Chursin et al., arXiv:2602.01873v2.

| Problem | Evidence | Paper | Fix |
|---|---|---|---|
| Owner history spill on the put path | 50 to 65 ms for each 1,024 batches (run 11) | 3.4: WAL is the only record | `history_persistence: wal_replay` (default): memory window of 4,096 batches, owner root file, restart by WAL scan |
| Checkpoint needs owner history and a leased settle | settle exchange before each proof | 3.3: snapshot stores positions, in the background | Image format 3 with one WAL position for each stream; `CheckpointPosition` needs no settle |
| Admission blocks at the replay limit | 4,096 batches or 256 MiB | no limit | No replay limit in `wal_replay`; recovery time grows with the suffix |
| Extent switch on the put path | 17.6 ms mean at 128 MiB (2 samples), p99 17.28 ms at 64 KiB with 16 writers | 3.1: buffer of pre-allocated maps | Lease-ahead: pre-leased next extent, close after the reply |
| Pauses without attribution | four of six pauses at 64 KiB; 1,504 calls above 5 ms in the sustained case | none | Owner lease and adoption profiles, switch stages in the write timeline, `stalls.csv` |
| Writer ring never polled | read I/O 26.9 µs interrupt, 20.0 µs polled | none | `polled_writes` (off by default) |

Not changed: batch preparation (12.4 µs) stays on the `wal-write-owner` thread, because the member checksum includes the batch version and that thread assigns versions in order.

Rule for the sustained case after run 13: if calls labelled `checkpoint:capture_start`, `checkpoint:freeze` or `checkpoint:live_install_start` hold more than 20% of the time of calls above 5 ms, the capture and install step size goes from 32 to 4. No other change without new evidence.

### Run 13 results (12 November 2026, one fleet, 37 of 37 cases)

Tables and evidence: `FOCUSED_CAMPAIGN_2026-11-07.md`, "Run 13 results".

| Problem | Result | Status |
|---|---|---|
| Owner history on the put path | Sustained 200,000 puts, same fleet: `wal_replay` 82,338 puts/s, p99.9 1.37 ms, 16 calls above 5 ms; `on_disk` 9,531 puts/s, p99.9 174.6 ms, 1,824 calls | Confirmed |
| Checkpoint settle | `CheckpointPosition` ran under leased writes in each case; 2 synchronous adoptions for each case against 99 with `on_disk` | Confirmed |
| Admission at the replay limit | No admission block seen in 200,000 puts. Recovery time against suffix length was not measured | Partly confirmed |
| Extent switch | 2 switches cost 0.09 ms with lease-ahead and 35 to 37 ms without it (same fleet). p99 at 64 KiB with 16 writers does not change (2 switches are below 1% of 4,096 calls); throughput +4% to +5% | Confirmed for the switch cost; no p99 effect in this workload |
| p99 of 17.28 ms at 64 KiB with 16 writers (run 12) | Came from `memory_only`: same-fleet arm 16.9 ms against 3.9 to 4.0 ms with `wal_replay` | Cause corrected |
| Pauses without attribution | One group remains: the first put after a close (31 to 33 ms). 16 calls overlap the position proof exchange in the 64 KiB timeline case | Open, see below |
| Polled writes | p50 at 1 KiB with 1 writer 39.6 µs against 46.6 to 46.9 µs. No effect shown at 64 KiB with 16 writers | Confirmed at 1 KiB only (one repetition) |

The step-size rule above did not apply: no slow call has one of the three labels.

Open after run 13:

1. First put after a close: initial lease and pre-lease cost 15 to 24 ms each (`reserve_external`). `wal_replay` adds one close before the first measured put (probable cause: a control command at the measurement gate; not confirmed). Candidate: no close for control commands that do not need it, or pre-lease at close.
2. The remote small put is 20 to 25 times the `native` p50 (46.6 µs against 1.9 µs) and the 64 KiB put with 16 writers is 11 times. `wal_replay` removed the stalls, not the per-call cost of the network and the device.
3. Recovery was executed (owner restart with data, coordinator reopen) but not timed. Crash tests on hardware were not done.
4. Sui replay over NVMe-oF has no hardware result. The one launch stopped before the suite at the guest library gate (`libstdc++.so.6` is not in the proof of a focused guest). See `FOCUSED_CAMPAIGN_2026-11-07.md`, "Sui replay over NVMe-oF".

## Regression reference
Compare each new run against this table and investigate a regression. Match fleet, route, workload, size, concurrency, cache and durability. Fleet-to-fleet spread is about 13%; do not subtract separate fleets.

Run 8; fleet bench-0a8a6af114a64287. Each populated cell gives repetitions and mean / p50 / p99 in µs. Row IDs follow the five value columns in order; — means no population. Durable puts use owner synchronous or native barrier; page-cache puts use owner native tier or native page_cache. Read state is in the column; profiled reads include profiling cost. Floor and sync cells state their own contract.

| Route | Size | c | Durable put | Page-cache put | Memory read | Cold read | Sync step | Row IDs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| hybrid SPDK | 1 KiB | 1 | 2 rep; 129.1 / 122.8 / 243.8 | not measured | not measured | 1 rep; 101.9 / 108.9 / 132.3 | 1 rep; file fdatasync; 29.0 / 26.9 / 42.7 | R08b1e259 / — / — / R8c6b50b8 / R852b2f71 |
| hybrid SPDK | 1 KiB | 16 | 2 rep; 333.4 / 335.0 / 605.8 | not measured | not measured | 1 rep; 117.0 / 114.9 / 317.8 | not measured | Rffe00a2c / — / — / Re69f96c6 / — |
| hybrid SPDK | 64 KiB | 1 | not measured | not measured | not measured | 1 rep; 139.8 / 140.6 / 157.7 | not measured | — / — / — / R9c0dae5c / — |
| hybrid kernel DIRECT | 1 KiB | 1 | 2 rep; 125.1 / 119.3 / 228.5 | not measured | not measured | 1 rep; 112.7 / 120.4 / 145.7 | 1 rep; file fdatasync; 28.5 / 26.1 / 148.8 | Rd6fbe26d / — / — / Ra8cafbba / Re7acf834 |
| hybrid kernel DIRECT | 1 KiB | 16 | 2 rep; 336.1 / 330.2 / 631.6 | not measured | not measured | 1 rep; 118.6 / 117.7 / 409.3 | not measured | Ra2ce76c9 / — / — / R957ce20c / — |
| hybrid kernel DIRECT | 64 KiB | 1 | not measured | not measured | not measured | 1 rep; 147.8 / 147.3 / 174.6 | not measured | — / — / — / R6d2332f6 / — |
| hybrid kernel buffered | 1 KiB | 1 | not measured | not measured | 1 rep; 42.6 / 42.1 / 52.0 | not measured | not measured | — / — / R9c9bedb1 / — / — |
| hybrid kernel buffered | 64 KiB | 1 | not measured | not measured | 1 rep; 65.6 / 65.0 / 77.1 | not measured | not measured | — / — / R121f66e4 / — / — |
| native | 1 KiB | 1 | 2 rep; 32.7 / 27.3 / 174.7 | 2 rep; 2.3 / 2.2 / 4.5 | not measured | 2 rep; not measured / not measured / not measured | 1 rep; file + directory fsync; 27.3 / not measured / not measured | Rc15b54b0 / R5c5502ba / — / Rc2cbb87b / Ra056169f |
| native | 1 KiB | 16 | 2 rep; 70.5 / 64.1 / 169.5 | 2 rep; 3.3 / 3.1 / 6.9 | not measured | 1 rep; not measured / not measured / not measured | 1 rep; file + directory fsync; 64.8 / not measured / not measured | Rf1572aad / Ree7b782d / — / R565f3b20 / R8d29f5b7 |
| native | 64 KiB | 1 | 2 rep; 161.1 / 157.2 / 211.8 | 2 rep; 54.4 / 41.1 / 114.9 | 2 rep; 10.6 / 11.0 / 16.0 | 2 rep; 160.6 / 102.1 / 1004.9 | 1 rep; file + directory fsync; 114.8 / not measured / not measured | R7f945823 / Rd4328490 / R09e69537 / R78233185 / Rc7e58ea0 |
| owner TCP | 1 KiB | 1 | 2 rep; 130.8 / 128.5 / 176.0 | 2 rep; 85.5 / 81.5 / 128.8 | 1 rep; 46.8 / 46.5 / 56.9 | 1 rep; 115.8 / 122.5 / 148.3 | 1 rep; file fdatasync; 28.7 / 26.2 / 56.9 | R27cd3fe5 / R361477d0 / R1ec39309 / Rc87b6aea / R119f59bc |
| owner TCP | 1 KiB | 16 | 2 rep; 336.1 / 344.1 / 556.9 | 2 rep; 423.2 / 326.0 / 645.5 | 1 rep; 63.6 / 58.8 / 138.5 | 1 rep; 152.4 / 142.4 / 476.8 | 1 rep; file fdatasync; 31.8 / 28.8 / 131.4 | R64d5dda3 / R2301b08a / Ref712b7f / R3b547898 / Rc9cfaa51 |
| owner TCP | 64 KiB | 1 | 2 rep; 495.6 / 490.3 / 591.1 | 2 rep; 328.2 / 319.3 / 466.0 | 1 rep; 110.5 / 109.2 / 128.2 | 1 rep; 206.1 / 207.1 / 237.9 | 1 rep; file fdatasync; 152.2 / 149.7 / 206.3 | R13eea891 / Rbf0c94da / R7e4bd14c / R63bb4082 / R0d1a0ee3 |
| owner verbs | 1 KiB | 1 | 2 rep; 126.6 / 121.5 / 221.4 | 2 rep; 78.2 / 74.4 / 127.4 | 1 rep; 38.1 / 37.5 / 49.1 | 1 rep; 109.3 / 116.0 / 142.0 | 1 rep; file fdatasync; 34.0 / 27.1 / 184.0 | R63036dd4 / Rb14099c4 / R035b1e82 / Re3876655 / R40880520 |
| owner verbs | 1 KiB | 16 | 2 rep; 361.4 / 360.4 / 634.0 | 2 rep; 514.9 / 324.4 / 733.4 | 1 rep; 46.9 / 44.0 / 114.7 | 1 rep; 131.7 / 124.8 / 385.8 | 1 rep; file fdatasync; 31.5 / 28.5 / 150.8 | Rb42eacfc / R4533c702 / R97e5b4fa / Rc11b3801 / R1553ed29 |
| owner verbs | 64 KiB | 1 | 2 rep; 445.8 / 441.4 / 537.4 | 2 rep; 284.0 / 275.2 / 396.8 | 1 rep; 68.5 / 68.0 / 80.8 | 1 rep; 159.5 / 161.4 / 193.2 | 1 rep; file fdatasync; 159.4 / 150.4 / 384.4 | R4afa3c44 / R9a805863 / R5cdd5f46 / R870d07f8 / R6dbb1b55 |
| verbs mTLS | 1 KiB | 1 | 2 rep; 125.3 / 122.8 / 174.7 | not measured | 1 rep; 38.4 / 37.4 / 49.6 | 1 rep; 109.8 / 116.0 / 139.5 | 1 rep; file fdatasync; 27.5 / 26.8 / 37.5 | Rc2292fda / — / R79431f2a / R4b6b43bd / R4ce90626 |
| verbs mTLS | 1 KiB | 16 | 2 rep; 334.3 / 336.5 / 543.8 | not measured | not measured | not measured | not measured | R5f6d119c / — / — / — / — |
| verbs mTLS | 64 KiB | 1 | 2 rep; 471.9 / 466.6 / 572.3 | not measured | not measured | not measured | not measured | R0a1f9571 / — / — / — / — |
| SSD direct QD1, fresh 2 GiB | 4 KiB | 1 | not measured | not measured | not measured | 3 rep; 70.5 / 72.0 / 84.5 | not measured | — / — / — / Ra26edc8a / — |
| SSD direct QD1, fresh 2 GiB | 64 KiB | 1 | not measured | not measured | not measured | 3 rep; 84.8 / 85.8 / 101.9 | not measured | — / — / — / Rc443c105 / — |
| ib_send_lat polled rdma0 | 1 KiB | 1 | not measured | not measured | 3 rep; RTT p50 10.8 µs | not measured | not measured | — / — / Rd9fbc4cb / — / — |
| ib_send_lat event rdma0 | 1 KiB | 1 | not measured | not measured | 3 rep; RTT p50 21.9 µs | not measured | not measured | — / — / Rb0bdeee1 / — / — |
| tcp_rtt block eth0 | 1 KiB | 1 | not measured | not measured | 3 rep; RTT p50 27.0 µs | not measured | not measured | — / — / Rb27b40da / — / — |
| tcp_rtt busy_poll eth0 | 1 KiB | 1 | not measured | not measured | 3 rep; RTT p50 16.8 µs | not measured | not measured | — / — / R5fb21fa9 / — / — |
| SSD write buffered | 1 KiB | 1 | not measured | 2 rep; 0.5 / 0.3 / 1.0 | not measured | not measured | not measured | — / R978d8580 / — / — / — |
| SSD write buffered | 64 KiB | 1 | not measured | 2 rep; 12.9 / 12.4 / 20.4 | not measured | not measured | not measured | — / R1a070b2f / — / — / — |
| SSD write fdatasync | 1 KiB | 1 | 2 rep; 20.2 / 20.0 / 26.9 | not measured | not measured | not measured | not measured | R41ecb4e4 / — / — / — / — |
| SSD write fdatasync | 64 KiB | 1 | 2 rep; 51.3 / 50.6 / 60.4 | not measured | not measured | not measured | not measured | R0d9ef188 / — / — / — / — |
| SSD write direct | 64 KiB | 1 | not measured | not measured | not measured | not measured | direct write, no sync; 2 rep; 34.2 / 32.4 / 42.9 | — / — / — / — / R80ab5a8b |
## Appendix and regeneration
The [generated appendix](PERFORMANCE_PLAN_2026-11-10-APPENDIX.md) contains the full operation table and maps every row ID in this plan to its case IDs.

Primary inputs are run 8 `analysis/REPORT-unified.md` and `analysis/output/`; run 7 `analysis-final-v2/output/`; full-matrix `docs/research/supervisor-brief-2026-11-10/populations.csv`; and `docs/distributed/FOCUSED_CAMPAIGN_2026-11-07.md`. `inputs.json` records input and rendered-output SHA-256 hashes. Missing inputs must stop regeneration, not become zero.

From the repository root:

```sh
python3 docs/distributed/performance-plan-2026-11-10/build_tables.py
python3 docs/distributed/performance-plan-2026-11-10/check_plan.py
```
