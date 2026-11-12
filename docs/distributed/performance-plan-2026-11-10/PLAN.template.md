# Distributed performance plan — {{const:date}}

## At a glance

Hardware parity is the gate. Do not optimize the other paths until the gate is resolved. Run {{const:run8}} is complete; G13W is sealed but **not launched**. Its launch needs the owner's authorization.

{{table:headline}}

Run {{const:run7}}, fleet `bench-6e40ca83d74146cf`: {{const:kib1}} {{const:c16}} sustained puts, 200,000 puts per case, {{const:reps3}} repetitions. The tier states whether acknowledgement waits for the covering sync or only the page-cache append. The late rate is the mean of the last five complete one-second buckets per case, then the median across repetitions. The earlier values included a partial final bucket and were biased low. These finite-case rates are not sustained capacity.

{{table:headline_throughput}}

Ranked work after the gate: completion wake-up and network; owner persistence and hand-offs; size-dependent software; coordinator waits; put stall; batch writes; hybrid read residual. The ranked section gives measured accounting references, not predicted gains or proven end-to-end bounds [INFERENCE].

{{table:ranked_basis}}

Hardware parity comes first; then rank work by the gap to the floor and its share of API mean. [INFERENCE] Workload note, without changing the ranking: sustained throughput falls from the {{const:firstsecond}} to the {{const:lastfiveseconds}} in run {{const:run7}}.

## Decisions

Hardware parity comes first. The owner approved equal-writer Native and distributed arms at c1, c5 and c16: a grouped Native barrier in the benchmark adapter without an engine change, five Native instances in one process on one SSD (`native_shards`), one-owner distributed cases (`owner_count` 1), and 64 KiB write profiles. The cases are defined, not run: {{table:fair_counts}} cases. Launch needs the owner's explicit authorization. Skills contain rules, not benchmark values. Do not add a `pread` path to native if it adds latency; handle cold-read pattern differences in the distributed stack and leave native unchanged. No experiment in this plan is authorized by its inclusion.

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

API time is completion minus dispatch for successful calls. Quantiles use exact nearest rank. `n` counts successful calls; {{const:c1}} and {{const:c16}} mean {{const:workers1}} and {{const:workers16}} concurrent clients. A profiled copy adds profiling cost and has {{const:profreps1}} repetition. For repeated rows, the mean and quantiles are medians of per-repetition values, not pooled calls. Do not pool repetitions or fleets or claim significance from these counts. Reads can have a fast mode, so mean can be below p50; put tails can put mean above p50.

Write timelines measure adjacent intervals on retained calls. Write and read profiles give sampled means. Native profiles write stages, not batch writes. A residual is a calculated difference, not a timed stage. Device counters cover a window, not individual calls. Floors are separate experiments. `n/c` means not computed, not zero. Native has no coordinator: its library work is WAL append plus index update. Read flights and API resolutions are sampled separately; do not subtract them. For WAL reads, network is exchange minus server handler, encode, decode and verify. Nested annotations are not additive.

Owner TCP uses mutual TLS. Owner verbs and hybrid use trusted transport; verbs mTLS isolates TLS. Hybrid writes use owner verbs. Hybrid buffered reads use a target page cache; kernel direct and SPDK use direct storage. Device reads per call help qualify a case but do not prove every call reached media.

## Run {{const:run8}} values

Run {{const:run8}} (`unified`), fleet `bench-0a8a6af114a64287`, completed {{const:completed8}} of {{const:total8}} cases. Table notes state repetitions, contracts and case references. The case-ID appendix maps references to full IDs.

### Durable writes

Unprofiled; owner `synchronous`, native `value_wal_barrier`.

Run {{const:run8}} also has one Native SSD and five owners. Native's per-put file-and-directory barrier differs from the owner's covering sync. [INFERENCE] The 64 KiB c16 Native barrier result is consistent with a serialized-service model, but its cause is not isolated and API calls can overlap.

For context, the run 8 unprofiled c1 1 KiB synchronous verbs put has p50 / mean {{csv:ops_summary.csv:121:p50_us:us}} / {{csv:ops_summary.csv:121:mean_us:us}} µs. The full-matrix c1 mixed-stream synchronous verbs put has {{csv:cross/populations.csv:431:api_p50_us:us}} / {{csv:cross/populations.csv:431:api_mean_us:us}} µs. Fleet, workload and repetition count differ; this is not a measured regression or profiling cost.

{{table:spike}}

{{table:sui}}

{{table:durable}}

### Page-cache-acknowledged writes

Owner `native` tier and native `page_cache`; these are not durable-per-put comparisons.

{{table:page_cache}}

### Batch writes and deletes

Available cases only. Missing routes are not zero; native batch decomposition is not available.

{{table:batch_delete}}

### Resident and cold reads

Memory means resident under the route contract. Cold is a requested state, qualified by counters and latency distribution. Native small cold reads do not establish cold latency. The 8 KiB case passes the device-count gate but has memory-like latency; 64 KiB meets case-level qualification. Window counters do not prove every call reached NAND. Native reads recent fragments through the memory map and older fragments with positional reads. Native cold components are not computed. Kernel direct counters include non-API reads; their device time is an upper bound or part of a combined device, fabric and stack interval, not an additive leaf.

{{table:reads}}

### Decomposition

Each table uses one case per route. Means and shares refer to that case's API mean. TOTAL is that API mean; API p50 is not a sum. Use the timeline copy when both copies exist. Do not average copies. Profile and unprofiled comparisons are context, not exact subtractions.

{{table:decompositions}}

### Network and SSD floors

Floor tables give the median and range over repetitions. SSD reads use direct queue-depth-one I/O. SSD writes separate write and sync steps; the file was pre-written. Failed direct small-block cases are failures, not timings. These are run {{const:run8}}, fleet `bench-0a8a6af114a64287`; each row states its mode. In these inputs `ib_send_lat` and `ib_write_lat` record half round trip and are doubled for RTT; `ib_read_lat` records round trip. The [appendix lists each network floor repetition](PERFORMANCE_PLAN_2026-11-10-APPENDIX.md#network-floor-repetitions).

{{table:network_floor}}

{{table:ssd_read_floor}}

{{table:ssd_write_floor}}

### Sync steps and owner stages

Compare the same kind of in-process timed sync step, not identical calls: owner `fdatasync` of one file against native `fsync` of every registered file plus directory `fsync`. Native sync percentiles are not measured; compare means with means. The overwrite floor is a reference, not a WAL-append floor.

At {{const:kib1}} {{const:c1}}, native file-plus-directory sync mean is {{csv:sync_step.csv:25:mean_us:us}} µs; owner verbs is {{csv:sync_step.csv:12:mean_us:us}} µs and owner TCP is {{csv:sync_step.csv:21:mean_us:us}} µs. At {{const:kib64}} {{const:c1}}, native is {{csv:sync_step.csv:14:mean_us:us}} µs; verbs is {{csv:sync_step.csv:23:mean_us:us}} µs and TCP is {{csv:sync_step.csv:29:mean_us:us}} µs. These are separate cases, not paired calls.

{{table:sync_step}}

The owner-stage table uses retained owner envelopes and the owner clock for synchronous {{const:c1}} puts. Differences are calculated per record; their p50 is the nearest-rank p50 of those differences. Control queue contains appender and flusher queues; WAL append wraps validation and descriptor preparation. Do not add nested stages. A naive remainder that subtracts all these stages is negative at small size; this is evidence of overlap, not a positive software component. The unassigned owner interval instead subtracts flusher queue, WAL append and append persistence from handler time per record. Do not subtract owner timestamps from coordinator timestamps.
Request decode occurs before owner dispatch; response encode follows handler completion. Neither belongs to the handler remainder. The write-profile copy and timeline are separate executions; do not use their stage sums as an attribution.

{{table:owner_stages}}

The separate write-profile copy uses a different execution. Its stage clocks overlap and nested components are not additive. Its storage-lock and publication timings are distinct from the timeline case above.

{{table:write_profile}}


### Put stall and sustained puts

Run {{const:run8}} stall fits describe observations, not cause. Phase counts refer to the longest retained interval, not all stall time.

{{table:stall}}

The sustained results use run {{const:run7}}, fleet `bench-6e40ca83d74146cf`. Do not subtract them from run {{const:run8}}.

{{table:run7_sustained}}

## Measurement corrections

`/proc/diskstats` deltas cover the gate-to-gate window, longer than the case, and include background writeback. Do not derive native-versus-owner per-operation device latency from them. The fio floor overwrites a pre-written file; it does not measure WAL append. Native reads recent fragments through the memory map and older fragments with positional reads; owner reads exact ranges. [INFERENCE] Read-ahead of mapped faults or buffered positional reads can explain excess device bytes. Compare per-call latency, not device latency per operation. [INFERENCE] A mapping can remain resident after a cache drop; test a fresh process with no WAL mapping during the drop.

The event-driven write-latency failures reflect a tool limitation; the direct small-write failures reflect the SSD logical sector size. `failed_cases.csv` records case-level size {{const:kib64}} for both G11W failures, while `REPORT-unified.md` identifies the helper block as {{const:kib1}}. The case size is not the helper block size. Unqualified is not zero.

{{table:failures}}

{{table:unqualified}}

## Run {{const:run7}} values

Run {{const:run7}}, fleet `bench-6e40ca83d74146cf`, has {{const:reps3}} repetitions per case. Tables show median and spread, not a confidence interval. Owner reads use buffered I/O after eviction and cache drop; direct hybrid reads use kernel or SPDK targets. Random keys exercise the dataset. Device reads per call indicate qualification, not proof of a media hit on every call. Puts use the named tier. Tier medians are not paired calls, so their difference is not a measured per-call fsync cost.

{{table:run7_reads}}

{{table:run7_puts}}

## Cross-store reference

Native and Tidehunter use one run on full-matrix fleet `bench-0d99e8256fff4891`, not run {{const:run8}}. Native: one process, one SSD. Tidehunter: five owners. External stores: other external contributors record five configured or observed storage nodes; Cassandra's 3 November run records five contacts, not a verified five-node ring; five storage-host SSD mounts are attested only for the inspected 4 November and final deployments; FoundationDB also has a separate log/coordinator host; some 3 November FoundationDB and TiKV host identities name a different fleet than the report path, so the path does not establish the execution fleet. The external values are medians of per-run statistics over two to six completed GCP runs on different fleets, days and sometimes binaries; `median_runs` counts those runs. Native waits for its per-put file and directory barrier; Tidehunter synchronous waits for a covering owner sync; Valkey waits for AOF `appendfsync always`, Kvrocks for a synced RocksDB WAL write and Garnet for an AOF commit. Cassandra, FoundationDB and TiKV device persistence is not verified. These are not identical crash contracts. Valkey GET reads RAM; Tidehunter uses its owner read path. Native has no network hop, but that is not a latency lower bound.
The c1 read and put columns are the mixed point-operation profile, not standalone cases. The c16 columns use the named primary profiles. Cells show p50 / p99 in µs, success-only exact nearest-rank statistics; `runs` is `median_runs` for external stores. Whole-case rates include time outside API calls and are finite-case rates, not capacity; fast Native reads and some Valkey reads are load-generator bound.

{{table:cross_store}}

64 KiB c16 pure puts, finite-case rates; per-node figures assume even spread and are not measured per-node rates. Native and owner are one full-matrix run; Valkey is a median over external runs. These different contracts and fleets are context, not matched rankings.

{{table:per_node}}

Native's c16 per-put barrier API mean and p50 are shown above. Its c1 mixed-stream barrier put mean and p50 are {{csv:cross/populations.csv:613:api_mean_us:us}} and {{csv:cross/populations.csv:613:api_p50_us:us}} µs. The pure c16 case and mixed c1 case are different workloads. [INFERENCE] The figures fit a serialized-service model, but API calls overlap and no trace isolates the cause. Run 8 also uses one Native SSD against five owners; its c16 barrier and covering-sync rows are not a matched contract comparison.

For c1 mixed 1 KiB puts, owner verbs synchronous p50 is {{csv:cross/populations.csv:431:api_p50_us:us}} µs and Valkey RDMA AOF always is {{csv:cross/populations.csv:297:api_p50_us:us}} µs. Their resident-oriented owner and RAM GET read p50 values are {{csv:cross/populations.csv:432:api_p50_us:us}} and {{csv:cross/populations.csv:298:api_p50_us:us}} µs. At c16, 1 KiB pure puts are {{csv:cross/populations.csv:449:api_p50_us:us}} and {{csv:cross/populations.csv:287:api_p50_us:us}} µs; 64 KiB pure puts are {{csv:cross/populations.csv:411:api_p50_us:us}} and {{csv:cross/populations.csv:280:api_p50_us:us}} µs. At c1 mixed, 64 KiB puts are {{csv:cross/populations.csv:409:api_p50_us:us}} and {{csv:cross/populations.csv:293:api_p50_us:us}} µs. At c16, resident-oriented owner and RAM GET read p50 is {{csv:cross/populations.csv:442:api_p50_us:us}} versus {{csv:cross/populations.csv:290:api_p50_us:us}} µs at 1 KiB, and {{csv:cross/populations.csv:403:api_p50_us:us}} versus {{csv:cross/populations.csv:275:api_p50_us:us}} µs at 64 KiB. These are different fleets and read and durability contracts; c1 mixed and c16 pure cases are different workloads.

## Hardware parity gate

Three items remain open: owner versus native append sync; native mapped and positional reads versus owner exact-range reads; and host or device effects versus software-path effects. Neither the overwrite floor nor window-wide counters settle append parity. Parity also needs matched SSD count and a durable-contract definition; the approved fair comparison below tests these dimensions without changing the Native engine.

G13W tests append: 48 cases, {{const:modes4}} modes, {{const:sizes3}} sizes, client and owner {{const:host0}}, and {{const:reps2}} repetitions. It compares overwrite with fresh sparse-file append using `pwrite` plus `fdatasync`, mmap store plus `fdatasync`, and mmap store plus `fsync`. Match size and durability call. The mmap floor omits native directory `fsync`; account for it separately.

| Result | Decision |
|---|---|
| Owner append floor near native sync time | [INFERENCE] The owner path adds the gap. |
| Owner append floor near owner sync time, client floor near native | [INFERENCE] VM or SSD state explains the gap. |
| Other | Mixed; no single cause is isolated. |

Estimated use is about {{const:duration45}} for {{const:vm6}}. Run {{const:run7}} compute was about {{const:vmrate}}: {{const:vmhours}} cost about {{const:vmcost}}. This launch is about {{const:launchcost}} compute, excluding other charges. Reserve {{const:reservation}} per launch against the {{const:weeklycap}} weekly cap. If run {{const:run8}} reserved {{const:reservation}}, ISO week {{const:week}} holds this as the last free slot; launcher preflight must confirm. **Not launched; needs authorization.**

## Planned fair Native comparison (not run)

The owner approved the case design, not a launch. The Native and owner arms use the same writer counts where paired; remaining differences are stated below.

{{table:fair_plan}}

## Engine performance guard (measured, not validated)

The owner allows engine edits only if performance is intact. The fork already changes the default path: cache generation tokens and refill checks, zero-initialized file reads, a generic pending table and shared index search.

The local laptop A/B used three paper-benchmark binaries with default features: upstream `ce37b18` with its own lockfile, upstream with the fork lockfile, and the fork working tree. It ran write, get and mixed at 1,024 B and 64 B, two repetitions per arm. Writes acknowledge at the page cache without a per-put barrier; the value cache was off. Residency and device reads were not measured.

The table shows initial-run target throughput in operations/s: mean (min–max) across repetitions, not pooled samples. Every configuration is **undecided: invalid**: Firefox exceeded 30% of one core during every run, violating the interference rule. These observations do not establish unchanged performance.

{{table:engine_guard}}

[INFERENCE] The 64 B get and mixed, 64 B write, and 1 KiB get configurations are CPU-bound. Their descriptive arm means differ by less than about 2%, except the matched-lock 64 B write mean, which includes one low observation. Without that observation its remaining rate is close to the other arm means; the table retains it. The 1 KiB write varies by more than 2× within the upstream arm, and 1 KiB mixed also varies substantially. [INFERENCE] Page cache, writeback and `kswapd` may contribute; the cause is not established, and these runs cannot show a regression.

Rerun with the interfering process closed and three repetitions. Direct I/O and cache-on variants were not covered and need separate matched tests.

## Ranked focus

Shares refer to the same profiled case's API mean. Measured components are accounting references, not proven bounds on end-to-end improvement [INFERENCE]. Preserve the contracts and use matched fleets.
For every verification, pass only when the matched experiment confirms the target improvement without a correctness, durability or p99 regression.


### Gate zero: hardware parity

**Why:** The sync gap, read patterns, SSD count, durable-contract mechanics and host effects remain unresolved. **Reference:** [INFERENCE] Software effects cannot be assigned reliably until these effects are separated. **Do:** Run the defined parity and fair Native cases after explicit launch authorization, then investigate read-pattern parity. **Verify:** Compare matched append calls and repeat native cold reads in a fresh process. **Ask before:** Launching or changing the engine or durability contract.

### 1. Completion wake-up and network at {{const:c1}}

**Why:** Verbs resident-read network mean is {{csv:component_summary.csv:209:mean_us:us}} µs ({{csv:component_summary.csv:209:share_of_api_mean:share}} of API mean); durable-put network is {{csv:component_summary.csv:1:mean_us:us}} µs ({{csv:component_summary.csv:1:share_of_api_mean:share}}). The floor table compares polled and event-driven RDMA with blocking and busy-poll TCP. **Reference:** [INFERENCE] The measured network component is an accounting reference; wake-up alone is not measured.

**Do:** Use the existing `PollingPolicy::{Adaptive, Busy}` in `tidehunter/src/distributed/tcp.rs`: adaptive is the default, parks drained TCP on readiness and can park idle fabric on a provider CQ wait object; otherwise it uses bounded spin/yield. Busy drives established sessions without explicit sleep/yield; it does not eliminate storage or admission waits. The Rust benchmark `tidehunter/examples/sharded_wal_bench.rs` already accepts `--tcp-polling adaptive|busy` for owned or prepared nodes. Extend the focused comparison controller to propagate the policy to actual owner launch configuration and case identity, and record authenticated node status; do not assume a requested flag changed an external node.
Add a `polling` job field and Python controller plumbing. The controller currently hard-codes `"polling": "adaptive"` in `experiments/kv-comparison/cloud_controller.py`. No Rust change is needed for this A/B.

**Verify:** On one matched fleet, interleave three repetitions each of adaptive and busy for {{const:kib1}} {{const:c1}} cache-resident reads and synchronous durable puts, separately for verbs and TCP. Keep each route's security, workload, cache, CPU placement and durability fixed across its A/B; do not compare TCP mTLS to trusted verbs as a polling effect. Record per-repetition API mean, p50, p99, throughput, errors, client and owner process CPU, polling counters and actual policy. Report CPU cost and spread; distinguish completion-observation delay from wire time.
Pass when the network component moves toward the polled RDMA floor without a p99 regression. Report the CPU cost.

**Ask before:** Launching or changing the public configuration surface.
Ask before making busy polling the default.

### 2. Owner persistence and flusher hand-offs at {{const:c1}}

**Why:** Verbs owner persist mean is {{csv:component_summary.csv:0:mean_us:us}} µs ({{csv:component_summary.csv:0:share_of_api_mean:share}}). Durable wait and publication is {{csv:component_summary.csv:4:mean_us:us}} µs; waiting and coalescing {{csv:component_summary.csv:5:mean_us:us}} µs; client queueing {{csv:component_summary.csv:6:mean_us:us}} µs. The timeline table shows flusher queue wait. The separate profile table shows small storage-lock waits and publication and a gap between connection append persistence and storage persistence. [INFERENCE] The latter gap may include flusher hand-off, wake-up and durable-frontier waiting; it is not measured as a single stage. **Reference:** [INFERENCE] Measured intervals are accounting references, not proven bounds.

**Do:** Instrument enqueue to flusher dequeue, sync start/end, durable-frontier advance, waiter notification and resume, and reply publication; retain batch/group size and queue depth. Correlate by owner-local request or batch identity and keep sync separate from scheduling, with nested intervals labelled.
In `tidehunter/src/distributed/tcp/control.rs`, split the connection `append_persist_ns` span around `flush_batch` wake-up and the durable-frontier wait. Test skipping a coalescing wait when nothing can coalesce; first check the existing singleton behavior.

**Verify:** Repeat matched {{const:kib1}} {{const:c1}} synchronous puts with and without the candidate on one fleet, collecting owner hand-off distributions, API p50/p99 and CPU alongside sync timings. Confirm acknowledged batches survive recovery and publication ordering remains intact; do not subtract clocks across hosts.
Pass when the gap is measured and API p50 falls by the measured amount on a matched fleet.

**Ask before:** Launching or changing durability or publication guarantees.

### 3. Software that grows with value size

**Why:** At {{const:kib1}}, verbs client coordinator mean is {{csv:component_summary.csv:2:mean_us:us}} µs and owner software is {{csv:component_summary.csv:3:mean_us:us}} µs. At {{const:kib64}}, they are {{csv:component_summary.csv:266:mean_us:us}} µs ({{csv:component_summary.csv:266:share_of_api_mean:share}}) and {{csv:component_summary.csv:265:mean_us:us}} µs ({{csv:component_summary.csv:265:share_of_api_mean:share}}). The owner-stage table shows growth in WAL append and unassigned owner time. `tidehunter/src/distributed/codec.rs`, `PayloadHasher` and `record_digest`, hashes the encoded batch payload with BLAKE2b, excluding the digest field. [INFERENCE] Decode and digest verification may contribute to the growth. The measured components are accounting references, not proven gain bounds.

[INFERENCE] Size-dependent software may contribute to the cross-store gap; the separate full-matrix 64 KiB c1 mixed put p50 is {{csv:cross/populations.csv:409:api_p50_us:us}} µs for owner synchronous versus {{csv:cross/populations.csv:293:api_p50_us:us}} µs for Valkey AOF always. Fleet and contracts differ, so these values do not measure that contribution.

**Do:** Add {{const:kib64}} write-profile copies for verbs and TCP, instrumenting coordinator encode and digest, owner decode, digest verification, group construction and copies/bytes; separate allocation from hashing and avoid adding nested spans. Compare {{const:kib1}} and {{const:kib64}} under the same contract before choosing a copy or allocation reduction.
Micro-benchmark `record_digest` on {{const:kib64}} payloads on the fleet CPU. If hashing explains the growth, evaluate hashing once, a faster or parallel hash, or an existing CRC only where the integrity contract permits it.

**Verify:** Collect clean and profiled matched cases on one fleet and compare stage counts, bytes and CPU plus API mean/p99; validate identical decoded values and corruption rejection. Profiled copies diagnose, not substitute for unprofiled latency.

**Ask before:** Launching or changing digest, wire or persisted formats.
Ask before any option that changes the wire or persisted format.

### 4. Coordinator pipeline waits at {{const:c16}}

**Why:** Verbs exchange is {{csv:component_summary.csv:220:mean_us:us}} µs ({{csv:component_summary.csv:220:share_of_api_mean:share}}); durable wait and publication {{csv:component_summary.csv:221:mean_us:us}} µs ({{csv:component_summary.csv:221:share_of_api_mean:share}}); waiting and coalescing {{csv:component_summary.csv:222:mean_us:us}} µs ({{csv:component_summary.csv:222:share_of_api_mean:share}}); client queueing and wake-up {{csv:component_summary.csv:223:mean_us:us}} µs ({{csv:component_summary.csv:223:share_of_api_mean:share}}); coordinator {{csv:component_summary.csv:224:mean_us:us}} µs ({{csv:component_summary.csv:224:share_of_api_mean:share}}). The cross-store table gives Valkey RDMA put p50 as context on another fleet and contract, not a subtraction. [INFERENCE] The measured intervals are accounting references, not proven end-to-end bounds.

[INFERENCE] Pipeline waits may contribute to the cross-store c16 gap. The separate full-matrix 1 KiB c16 pure put p50 is {{csv:cross/populations.csv:449:api_p50_us:us}} µs for owner synchronous versus {{csv:cross/populations.csv:287:api_p50_us:us}} µs for Valkey AOF always. Different fleets and contracts prevent attributing that gap to the run 8 waits.

**Do:** Trace admission and preparation, staging/coalescing, owner dispatch and exchange, receipt, ordered publication and caller wake-up; retain queue depth, batch/group occupancy and per-stage identities. `remote_db/owner_dispatch.rs` already has early coalescing; measure its actual decisions before altering scheduling.
Read authority intake and the pipeline turn in `tidehunter/src/distributed/remote_db/concurrent.rs` against the timeline. Measure each queue depth, then fuse stage hand-offs one at a time on a matched fleet.

**Verify:** Repeat {{const:c16}} synchronous timeline and clean cases on one fleet; compare queue and stage distributions, throughput, API p99 and CPU, with valid complete batches and ordered publication. Diagnose head-of-line waits rather than adding overlapping spans.

**Ask before:** Launching or changing durability or public configuration.

### 5. Put stall

**Why:** The stall table gives spacing and fitted duration intercept and slope for overwrite and new keys. The separate run {{const:run7}} sustained table gives throughput for the {{const:firstsecond}} and {{const:lastfiveseconds}}, and p99.9. [INFERENCE] Checkpoint work that scales with cumulative puts may contribute; `tidehunter/src/distributed/remote_db/checkpoint_lifecycle.rs` implements capture, persistence, adoption and cleanup, but does not identify the cause. The observed stall is an accounting reference, not a general speedup bound.

**Do:** Retain existing checkpoint generation events and diagnostics for capture, receipt history, index persistence, root adoption, live installation and cleanup; add missing admission-block and queue timers, cumulative completed puts, keys and bytes at each generation. Correlate same-process phase boundaries with retained put stalls.

**Verify:** Repeat sustained overwrite and new-key cases on a matched fleet, fit stall duration against cumulative puts and checkpoint size by repetition, and compare timing and slope with checkpoint phases and no-stall intervals. Correlation is not causation; only a matched candidate that changes the suspected phase and reduces stalls supports it. Preserve recoverability and p99.9.
Pass when the fitted slope falls and the first-second to last-five-complete-bucket throughput ratio approaches one.

**Ask before:** Launching or changing persisted formats, checkpoint semantics or durability.

### 6. Batch writes

**Why:** The batch table compares native and verbs for {{const:batch32}} × {{const:kib1}} and {{const:batch32}} × {{const:kib8}}; the larger batch has a decomposition. [INFERENCE] Measured software and hand-offs are accounting references, not proven end-to-end gain bounds.

**Do:** Instrument batch validation, encoding/digest, copies, dispatch, owner decode/group build, sync and publication with batch cardinality and bytes; distinguish per-batch fixed work from per-value work without weakening atomicity.

**Verify:** On a matched fleet compare the existing batch32 sizes and scalar controls, reporting whole-batch latency and values/s as well as latency per value (whole-batch latency divided by completed values, not a value-level latency distribution). Retain per-repetition counts, p99 and correctness; native has no batch profiler.

**Ask before:** Launching or changing wire, persisted format or atomic batch guarantees.

### 7. Hybrid client residual on reads

**Why:** The buffered hybrid resident-read residual is {{csv:component_summary.csv:67:mean_us:us}} µs ({{csv:component_summary.csv:67:share_of_api_mean:share}} of API mean). It is derived, not a timed stage. [INFERENCE] It is an accounting reference, not a proven end-to-end gain bound.

**Do:** Add sampled client-local timers from API dispatch through index lookup, read-lane admission and dispatch, NVMe-oF submission/completion, result validation and caller wake-up; identify nested spans and cache/source route. Existing `remote_db/pending_read.rs` drives read lanes and coalesced flights.

**Verify:** Compare buffered and direct routes on one fleet with matched keys, sizes and concurrency, recording cache and I/O evidence, per-stage sample counts, API p99 and CPU. Attribute only non-overlapping measured spans; the residual remains derived until timed.

**Ask before:** Launching or changing public queue geometry.

### Not worth work now

The SSD read component and matched floor are close in the decomposition and floor tables. Trusted verbs and verbs mTLS isolate TLS; their measured difference is small. [INFERENCE] These measured components or route differences are accounting references that make them lower priority; they do not prove an end-to-end bound or a general device or TLS ranking.

## Evidence gaps and next experiments

Add a native read profiler and measure batch reads. Fill missing TCP and hybrid batch and delete cases: about {{const:fillcases}}, {{const:fillreps}}, and {{const:fillhours}}. The controller `hybrid_put` kind needs a small extension. Profiled copies, batch and delete cases have {{const:profreps1}} repetition; repeat before interpreting spread. Add a `sync` and dirty-page drain before the window, then retain gate-to-gate counter evidence. For native cold reads, drop caches while no process maps the WAL, then use a fresh process. No new run is authorized here.
The focused polling dimension needs Python job-field and controller plumbing.

The {{const:kib64}} owner decode and group-build stages have not been measured by a write-profile copy. Add one verbs and one TCP copy to the next authorized run.

Polling A/B evidence is missing in this focused controller: the Rust policy and standalone benchmark option exist, and `experiments/aws-sharded-wal/transport_floor.py` has separate policy plumbing, but that does not prove the focused `experiments/kv-comparison/cloud_campaign.py` propagates and verifies it. Implement and retain launch/status evidence before interpreting the A/B; no result is implied.

## Regression reference

Compare each new run against this table and investigate a regression. Match fleet, route, workload, size, concurrency, cache and durability. Fleet-to-fleet spread is about {{const:percent13}}; do not subtract separate fleets.

{{table:reference}}

## Appendix and regeneration

The [generated appendix](PERFORMANCE_PLAN_2026-11-10-APPENDIX.md) contains the full operation table and maps every row ID in this plan to its case IDs.

Primary inputs are run {{const:run8}} `analysis/REPORT-unified.md` and `analysis/output/`; run {{const:run7}} `analysis-final-v2/output/`; full-matrix `docs/research/supervisor-brief-2026-11-10/populations.csv`; and `docs/distributed/FOCUSED_CAMPAIGN_2026-11-07.md`. `inputs.json` records input and rendered-output SHA-256 hashes. Missing inputs must stop regeneration, not become zero.

From the repository root:

```sh
python3 docs/distributed/performance-plan-2026-11-10/build_tables.py
python3 docs/distributed/performance-plan-2026-11-10/check_plan.py
```
