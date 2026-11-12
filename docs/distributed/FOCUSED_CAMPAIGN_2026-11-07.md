# Focused TideHunter campaign (7 November 2026)

Status: the matrix, the controller, the reducers and the benchmark modes are implemented and unit tested on a laptop. A local dry run of a representative subset on Soft-RoCE exercises the real controller (it is a functional check, not a hardware, SSD or performance result). RDMA, the real Local SSD, fio on a device and the block-trace file attribution need the fleet.

## Scope

The campaign runs the TideHunter family only: native, owner RPC over verbs, owner RPC over TCP, and the sealed NVMe-oF route over RDMA with a kernel or an SPDK target. It does not run Valkey or any other store, and the focused package contains no external-store runtime (cargo features exactly `fabric`, `nvmeof`, `test-utils`; pair qualification has no Valkey step). Reads follow this hierarchy: coordinator cache, then one NVMe-oF read to the owner host, then the target page cache (kernel target, `buffered_io=1`), then the SSD. Writes and data that is not in a sealed catalog use the owner RPC. The sealed-image route is this hybrid. See `LINUX_NVME_OF_READ_BACKEND_2026-10-31.md` for the target. Two cold controls bypass the target page cache: the SPDK target (`O_DIRECT`) and the kernel target with `buffered_io=0` (`ExportTarget::KernelDirect`, admitted only for sealed read-only images and only for the G1 cold read cases; every other kernel target stays buffered).

## Selections

`experiments/kv-comparison/cloud_campaign.py plan --mode focused --selection recommended|full|device|floors|stages|unified --output PLAN.json` prints the case counts and writes the jobs. `recommended` is the first 279 jobs. `full` adds 54 jobs and keeps the 279 unchanged. `device` has 120 jobs (group G10). `floors` has 69 network and SSD read floor jobs. `stages` has 183 operation and decomposition jobs; `unified` runs those followed by the unchanged 69 floors cases and 18 SSD write floors.

| Group | `case_kind` | Cases | Content |
|---|---|---:|---|
| G1 | `media_read` | 81 | Cold reads (owner verbs, owner TCP, owner verbs with prewarmed history and selective WAL eviction, hybrid kernel buffered, hybrid kernel direct, hybrid SPDK) and resident controls (owner verbs, owner TCP, hybrid kernel buffered), 1, 8 and 64 KiB, c1, 3 repetitions |
| G2 | `fio_floor` | 24 | Read-only fio, 4 and 64 KiB, depth 1 and 16, on an owner scratch file and on the sealed namespace |
| G3 | `block_trace` | 6 | Block trace of a cold owner read and of an owner put |
| G4 | `interference` | 12 | 1,000 scheduled reads per second with 16 synchronous put workers (owner verbs, owner TCP, hybrid kernel, hybrid SPDK); `measurement_operations` is a hard offered-operation ceiling of 400,000 per case |
| G5 | `sealed_knee` | 150 | First-touch and repeat reads, 1 KiB, c1, c2, c4, c8, c16, five routes |
| anchors | `native_anchor` | 6 | Native 1 KiB c16 read and put |
| G7 | `high_concurrency` | 48 | c64 and c256, reads on five routes, puts on three (full only) |
| G9 | `write_wait_diagnostic` | 6 | Owner verbs c16 puts with the write-wait counters on and off (full only) |
| G10R | `media_read` | 60 | Device-only selection: cold reads on spread datasets with random keys (owner verbs, owner TCP, hybrid kernel direct, hybrid SPDK), 1, 8 and 64 KiB at c1, 1 and 64 KiB at c16, 3 repetitions |
| G10W | `remote_put` | 60 | Device-only selection: plain closed-loop puts on owner verbs and owner TCP, tiers `synchronous` and `native`, 1 and 64 KiB, c1 and c16, plus one 200,000-put case per route and tier |
| G11N | `net_floor` | 45 | RDMA perftest, libfabric pingpong and TCP round trips across four sizes, three repetitions |
| G11S | `ssd_floor` | 24 | QD1 direct random reads at two block sizes and footprints, fresh and aged, three repetitions |
| G11W | `ssd_write_floor` | 18 | Buffered, fdatasync and direct sequential SSD writes, three block sizes, two repetitions (unified only) |
| G12N | `native_op`, `native_media_read` | 52 | Native writes, cache reads and qualified cold random reads |
| G12W | `remote_put` | 60 | Owner verbs, TCP and mutual-TLS verbs writes |
| G12H | `hybrid_put` | 12 | Hybrid kernel-direct and SPDK deployment writes |
| G12K | `native_op`, `remote_put` | 12 | Unique new-key native and owner verbs puts |
| G12D | Multiple | 43 | Write timeline, write profile and read profile copies, including c16 and mutual-TLS comparisons |
| G12S | `remote_put` | 4 | Sustained overwrite and new-key puts with diagnostics |

The planning estimate for `recommended` is 4.17 fleet hours and about $109 including the non-compute allowance (176 minutes of work). It is an estimate, not a bound. The launcher keeps its $145 ceiling and the six-hour fleet. The weekly budget in `experiments/gcp-rdma/budget.json` counts per ISO week (UTC); it was raised by one $145 quote for week 45.

## Stages and unified selections

`stages` measures native, owner and hybrid operation contracts and decomposes
representative calls. `unified` appends the identical floor cases, allowing
same-export wire and SSD bounds. G12N, G12W, G12H and G12K have two
repetitions except selected batch, delete and cold-read cases run once;
G12D and G12S run once, and the appended floors have three repetitions.
The first eleven cases are canaries: native durable put and cold read, verbs
put, batch and delete, kernel and SPDK hybrid puts, new-key verbs put, and one
case for each of write timeline, read profile and write profile. The unified
order is the eight flow canaries, all 43 G12D cases (including the three
diagnostic canaries), remaining repetition 0 groups, repetition 1 groups,
the unchanged floors block, then G11W with an fdatasync 1 KiB canary first.
The first work dropped if the fleet deadline nears is floor repetitions.

Hybrid puts preload through the owner route using a fresh namespace. The
sealed 1 KiB deployment remains background only: no reads are issued,
initiator read counters must be zero, and SPDK target reads must be zero.
The kernel arm relies on initiator counters. A failed hybrid measured run
without independently proven reader drain aborts the campaign and quarantines
its targets. An unqualified hybrid put or native cold read remains completed
with an unqualified verdict and is excluded from comparisons.

`put_key_policy` is a harness-only field: `overwrite` is the default; `new`
generates unique Sui-like inserts. Native cold preparation uses page-cache
durability at concurrency 16 with a 180-second cap. A timeout fails that case
after confirmed cleanup; measurement reopens with the planned durability.

The analysis tables include `ops_summary`, `apples_to_apples`,
`decomposition_write`, `decomposition_read`, `transport_share`, `stall`,
`component_summary`, `component_compare` and `ssd_write_floor`.
The component tables rank stages per action and compare routes; the write
floor table reports latency distributions by mode and size. Markdown files
are compact; CSV and `focused-summary.json` retain every field. Repetitions
and fleets are not pooled.

G12D adds c16 cache and disk read profiles, mutual-TLS verbs cache and disk
profiles and a put timeline, plus TCP 64 KiB put, 8 KiB batch and delete
timelines. Profiled reads do not exceed the 4,096-call profile limit.
Residual rows are computed only when the profile covers every measured read;
otherwise they state the missing coverage condition. Owner residuals split
client other and server software; hybrid and native cold residuals separate
their remaining software from measured stages and device time.

G12D does not profile `get_many`. Media qualification currently counts scalar
read API calls rather than logical keys, so it cannot qualify batched reads.
A follow-up needs per-key accounting and qualification before adding those
cases.

Write timeline owner records join by owner and envelope ID. At c1 each
envelope has one call; shared envelopes are excluded from the derived
exchange-minus-handler row. Native write profiles cover scalar puts and
deletes only; native batches have whole-operation measurements only. Native
cache reads have no stage split, and native `metrics.prom` is cumulative.

Exactness labels:

- `exact_interval`: one adjacent event pair of one retained call.
- `aggregate_of_exact_intervals`: mean and p50 over retained calls.
- `sampled_mean`: a profile sum with its own denominator.
- `device_counter`: a device-counter observation.
- `floor_derived`: a bound derived from the floor cases.
- `derived_duration_difference`: a difference of two durations of the same
  envelope or measured population, assuming the owner handler lies within the
  coordinator interval; never subtraction of timestamps across clocks.
- `derived_mean_difference`: a difference of means whose populations are not
  proven identical, shown only when counts match.
- `residual`: the remaining unattributed component.

Component accounting (`component_summary`, `component_compare`). The read
profile and the API samples cover the same reads only when the profile limit
(4096) is at least the number of measured reads; G12D read cases measure at
most 4096 reads for this reason. Owner reads: the WAL-read `exchange` spans
encoding through verified completion, so encode, decode, verification and the
server handler are nested inside it. Components: coordinator leaves; `client
queueing and wake-up` = `inline` - `exchange`; `client messaging` = encode +
decode + verify; `network` = exchange - server handler - encode - decode -
verify (wire, both stacks, TLS, dispatch, wake); `owner software` = server
handler minus the owner device time per read; `ssd device`; `residual` = API
mean - coordinator leaves - `inline`. They add up to the API mean. Hybrid
reads: coordinator leaves + reader stages (admission, alignment, queue,
`submit_to_cq` = device service + fabric and stack, extraction, verification) +
residual. Writes: the timeline intervals; the owner handler (persist, WAL
append, queues) lies inside the exchange interval, and the network part is the
exchange interval minus the handler for unshared envelopes. A negative derived
value is shown and flagged, never clamped. Batch reads (`get_many`) are not in
this run.

These are estimates, not a runtime guarantee. The earlier 245-minute suite
estimate plus 14.5 minutes for G12D additions and 7.5 minutes for G11W gives
about 267 minutes (4.45 hours). About 36 minutes of fleet overhead gives
5.05 hours, or about $100 compute for six VMs at $3.287 per VM-hour, excluding
other charges. The six-hour fleet, 900-second export reserve, 120-second
campaign margin and 600-second case deadline remain unchanged. To leave a full
600 seconds for the last case, start it no later than 5 h 33 min after the
fleet start (27 min before fleet expiry); the controller can start later with
a shortened remaining work budget. This estimate is not a bound. Slow
preparation, diagnostics, aged SSD cases and teardown can consume the margin.
Run 8 (9 Nov) launched `unified`. The suite took 2 h 16 min, about half the estimate above, because the average case took about 30 s. Results, the per-component decomposition and the ranking of components to improve are in `checkpoint-work/focused-launch-20261109-unified2/analysis/REPORT-unified.md`. A standalone `stages` or `floors` fleet has not been launched.

## Floors selection

The `floors` selection uses the existing six-VM fleet, with host 0 as client
and host 1 as server; hosts 2–5 remain idle. It runs no KV client or
TideHunter binary. Its first five cases are network and SSD canaries; the
remaining cases follow matrix order. Each case has a 600-second deadline.

G11N measures perftest polled and event-driven verbs, libfabric provider paths,
and TCP blocking, spinning and busy-poll receives. `fi_pingpong` has no warm-up
option and uses warmup 0. `ib_write_lat` and `fi_pingpong` are not covered by
image qualification; a missing binary fails only cases using that tool.
G11S measures 4 KiB and 64 KiB random reads on random-data 64 MiB and 2 GiB
scratch files on owner 0 (host index 1, the same Local SSD used by G2),
immediately and after 60 seconds, for 6 seconds per read run. Its per-I/O
latency log includes offsets to investigate the observed fast and slow modes.
Direct I/O does not prove NAND access.

The unified-only G11W writes a preheated 512 MiB scratch file sequentially
at queue depth one, for 6 seconds per case, at 1, 8 and 64 KiB. Buffered
writes acknowledge page-cache writes without sync; fdatasync writes call
`fdatasync()` after every write; direct writes use direct I/O. These measure
different acknowledgement contracts, not interchangeable device timings.

The case receipt binds `net_floor_result` or `ssd_floor_result`, the SSD
latency log, and `phase_timing` by SHA-256. Network outputs are bounded to
16 KiB per side and run; the SSD log is bounded to 12 MiB. Tool latency
semantics are recorded rather than assumed interchangeable.

## Evidence and qualification

Each focused case binds its artifacts in the case receipt as `receipt[stem] = {path, sha256}`. Every artifact has `schema_version` 1 and `case_id`. A missing requested artifact fails the case. An unavailable observation is recorded as `unqualified` with reason codes, never as zero.

- `phase_timing`: monotonic spans of dataset preload, export copy, target setup, backend deploy and target teardown, same host clock only.
- `media_evidence`: physical owner-SSD completed reads divided by successful measured reads. Cold needs at least 0.9. Memory needs at most 0.06. The window must be drained. This is a cohort proxy, not proof for each read. The warm-history arm is qualified by the report only when its cold ratio holds and its history-load delta is at most 0.10 of the matched normal-history arm (a reviewed value, not a measurement).
- `fio_result`: one fio job, `--readonly --direct=1 --rw=randread`, raw JSON kept and hashed, every number recomputed by the analyzer from the raw JSON. The scratch file has at least 2 GiB. The namespace location measures the buffered target path, not the SSD floor.
- `block_trace_evidence`: file attribution only with a stable extent and inode proof; otherwise `unknown`.
- `interference_result`: per stream offered, admitted, completed, successful, rejected and failed counts, plus the shared window and each stream's first dispatch and last completion. The route is proved by the effective configuration (`write_backend` is `owner_record_protocol`) and the NVMe counters; there is no independent append counter.
- `write_wait_metrics`: exact counters for three coordinator waits (owner enqueue to dequeue, admitted to append enqueue, harvest to durable): observations, depth sum and maximum, wake attempts and coalesced wakes, plus `saturated` and `complete`. They are armed only with the write-timeline diagnostics and cost one relaxed load per site when off.

An unsuccessful fio process or missing direct read-only fd observation fails the
case. The error retains the exit status, stderr tail and stdout prefix. A local
scratch failure is returned only after verified scratch cleanup. Uncertain
cleanup, unsafe access and deadlines remain fatal infrastructure or safety
failures. Failed cases are excluded from decision comparisons and make the
campaign incomplete.

## Analysis tool

Run `cloud_campaign.py report --root RESULTS` on a copy of the exported results,
then run `focused_analysis.py --export RESULTS --report RESULTS --output OUTPUT`.
The report must match the export matrix. The tool writes CSV and Markdown tables
and `focused-summary.json` with source case IDs. It uses admitted report
populations and evidence; it does not pool samples or re-admit raw artifacts.
Missing observations are not zero. Repetition spreads are descriptive, not
confidence intervals. Media qualification is a cohort proxy, not a per-read
device claim. Compare cache and durability contracts before interpreting ratios.

## Benchmark modes added

- `execution.mode = "interference"` with a top-level `interference` object (`read_offered_per_second`, `read_queue_capacity`, `put_concurrency`, `duration_seconds`, `key_policy`). The reader is open loop with a bounded queue; arrivals that find it full count as rejected. Puts are closed loop. Raw samples gain a `stream` column.
- `execution.queue_capacity` is explicit per case (at most 65,536) for c64 and c256. No default changed.
- An async timeout cannot preempt a blocking backend call; the process supervisor stays the last guard.

## Coordinator-cache accounting and trace harness

The coordinator cache no longer reserves a 1,024-byte payload floor per slot.
Each resident pays its key/value bytes, the existing nonempty byte-owner headers,
and `size_of::<Entry>() + 8` bytes. The 8-byte effective allocator charge models a
216-byte Entry in a 224-byte glibc chunk on this build; this is not an RSS bound.
Flat arrays contain one optional Box handle, one full-reference tag and one
access-history item per slot (32 bytes). For shard budget `b`, the slot count is
`4 * floor(b / (32 + 384) / 4)`, using a 32-byte key and 64-byte value as the
smallest realistic record. Fixed arrays use at most 7.70% of the budget.
All resident charges and fixed arrays fit within `read_cache_bytes`; its field,
default and host-allowance checks are unchanged. The protected quarter applies
after fixed arrays, and includes Entry charges. CLOCK still uses at most 64 steps.
Hits compare tags before dereferencing an Entry and still check full immutable
reference equality. Vacant slots allocate one Entry under the existing shard
lock; replacement reuses its Box. Payload detachment remains outside the lock.

`tidehunter/src/distributed/remote_db/read_cache/trace_eval.rs` replays ordered TSV
events on the real cache and LRU, CLOCK and window-TinyLFU controls. Controls keep
the baseline 240-byte per-live-entry metadata charge so comparisons use identical
traces and control capacities. `synthetic_mrc_sweep` and `file_trace_evaluation`
write `TH_TRACE_EVAL_OUT`; `TH_TRACE_EVAL_IN` names a captured TSV.
`TH_TRACE_EVAL_WARMUP`, `TH_TRACE_EVAL_EVALUATION` and `TH_TRACE_EVAL_BUDGET_MIB`
set windows and budgets. Slot-floor parameters and CSV columns are removed.
The Sui-output model fits observed quantiles and an assumed 64-byte lower endpoint;
it is not an empirical access trace. Results are in `fixes/cache-mrc-exact-accounting/`
beside `fixes/cache-mrc-synthetic/` in the 7 November campaign analysis.

## Deferred

- The direct kernel arm is validated on Soft-RoCE only; the real target host kernel and Local SSD behavior of `buffered_io=0` need the fleet.
- Owner history prewarm beyond the existing key sweep and mapped-WAL eviction.
- Block-trace file attribution on the fleet; fio on a real device or namespace; RDMA timing.
- A trace-replay workload (`sui_trace_replay`) is reserved in the job schema and rejected by the validator.

## Launch history (7 to 8 November 2026)

|Run|Stopped at|Cause|Fix|
|---|---|---|---|
|1 (7 Nov)|case 26 of 279|After the kernel direct 64 KiB case, the NVMe host disconnect read a sysfs attribute of a controller in teardown and got `ENODEV`. `manage.py` retried only `ENOENT`.|`controller_snapshot` and `namespace_candidates` retry `ENODEV` like `ENOENT` and still require two identical inventories. The same change is in the Rust live connect scans (`live_connect.rs`, ten sites, three left NotFound-only); the Rust change first shipped in the package of run 4.|
|2 (7 Nov)|case 27 of 279|The first fio case failed with exit status 1. `run_readonly_fio` dropped stderr and the host turned the RPC error into a campaign abort.|`FioFailure` carries a bounded stderr tail and stdout prefix; a fio failure with confirmed scratch cleanup fails one case. fio throughput uses `read.runtime` (ms), not the rounded `elapsed`.|
|2 cleanup|policy delete never sent|A GCS 429 on the lifecycle state object made `_save()` fail after `delete_requested` was set, so recovery only waited.|State saves retry HTTP 429 and 5xx with a bounded backoff. The compute and image delete paths restore the intent flag when the save fails. The run 4 cleanup passed without a stall.|
|3 (8 Nov)|before case 0|The headroom probe retired 225 test controllers with 8 parallel `nvme disconnect` calls; one call died with SIGSEGV (nvme-cli scanning sysfs while siblings tore controllers down).|`manage.delete_controller` writes `delete_controller` through one pinned sysfs directory handle after the identity checks; the probe uses it, and `manage.disconnect` retries once through it when the CLI fails and the controller remains.|
|4 (8 Nov)|case 39 of 279|All eight repetition 0 G2 cases failed with `fio: engine libaio not loadable` (the guest image has no `fio-engine-libaio`; the campaign continued, as designed). Then the third G4 case offered 19,999 of 20,000 planned reads: the last arrival is due 1 ms before the window end and the scheduler stalls about 1.8 ms every second. An NVMe case that fails mid-measurement cannot prove its reader drain, so the controller aborted.|The fio floor uses `--ioengine=io_uring` (built into fio, proven by the guest qualification). The interference scheduler catches up late arrivals for up to 250 ms after the window end (scheduled times and the put cutoff unchanged; lateness stays visible in `scheduling_ns`).|
|5 (8 Nov)|case 28 of 279|The first fio case completed (13.9k IOPS, p50 72 us). Its `case_finish` then waited for a final KV snapshot from the exporter; a fio case runs no KV client, so none appeared, monitoring became incomplete, and the controller refused the next case.|`campaign_monitor.finish_case` observes a completed standalone case (`STANDALONE_CASE_KINDS`, `fio_floor`) through its terminal record, like a failed case; a KV case still needs its final snapshot.|
|6 (8 Nov)|completed, 279 of 279|None. All gates passed, 0 failed cases, monitoring complete, fleet deleted by the launcher (`cleanup_verified`). The launcher then exited 1 on an SSL handshake timeout while downloading results; the controller archive was fetched from GCS and its sha256 matched the receipt.|Not applicable. Compute of the six attempts: about $121 ($72 for run 6).|
|7 (9 Nov, `device`)|completed, 120 of 120|None. All gates passed, 0 failed cases, 0 unqualified read cases, monitoring complete, fleet deleted by the launcher (`cleanup_verified`).|Not applicable. Compute about $43 (13.2 VM-hours); the seven attempts about $164.|
|8 (9 Nov, `unified`)|completed 265 of 270 (receipt `incomplete`)|Five one-case failures: three `ib_write_lat` event-mode network floors (the server exits before listener readiness) and two 1 KiB `O_DIRECT` write floors (`pwrite` errno 22, 4 KiB logical sectors). Three native cold 1 KiB cases are unqualified (0.28, 0.28 and 0.89 device reads per read); the 8 KiB native cold cases pass the ratio check but run mostly from memory (p50 4.8 µs). Suite 2 h 16 min (about 30 s per case), monitoring complete, fleet deleted by the launcher (`cleanup_verified`); the launcher then exited 1 because the campaign receipt is incomplete; the controller archive was fetched from GCS and its sha256 matched the receipt.|Not applicable. Compute at most about $49 (six VMs, 18:40 to 21:10 UTC; estimate); the eight attempts about $213.|

Known limits of the guest image: no `bpftrace` binary and no `trace_buf_size` boot parameter, so the six G3 block-trace cases record "unavailable". Cold 1 and 8 KiB reads on page-cached paths (owner OS, buffered kernel target) fail the 0.9 device-read test and are reported as unqualified; so are the nine warm-history cases (their history-load baseline is zero, so the 0.10 ratio is undefined). Four fleets (runs 1, 2, 4 and 6) gave median latencies that differ by up to about 13 percent on 26 matched G1 repetition 0 cases (median of per-case p50 ratios 0.89 to 1.07 between fleets). Inside run 6, three repetitions of one case differ in p50 by a median of 4 percent (maximum 8 percent). The G5 sweep is closed loop with 4,096 or 8,192 calls per case (spans 0.017 to 0.64 s), so its candidate knees are a screening result, not a capacity measurement. Evidence and tables: `checkpoint-work/focused-launch-20261107-r9/analysis/REPORT.md`.

Run 8 findings that change the reading of the tables: native 1 KiB cold reads are unqualified; 8 KiB passes the device-count gate but has memory-like latency, and 64 KiB meets case-level qualification. Window counters do not prove individual NAND access. At the measurement gate the client still held 1.1 GiB of mapped file pages after the cache drop; [INFERENCE] mapped pages can remain resident. Native reads recent fragments through the memory map and older fragments with positional reads; the measurement-window path split is unknown. The `O_DIRECT` 1 KiB write floor fails on 4 KiB logical sectors; `ib_write_lat` rejects event mode; hybrid kernel block-layer counters include about 365 non-API reads per case, so the kernel device time is an upper bound and the combined device, fabric and stack interval is reported; c16 read profiles cover about 20 percent of the reads (about five reads per flight), so their components stay unsplit.

Run 8 write-time correction (10 November 2026). Device counters from `diskstats` cover the whole gate-to-gate window (3 to 10 s), while a case runs 0.5 to 2 s. They include background writeback of dirty owner history pages (about 15 KiB dirtied per put). They are not valid for a native-versus-owner per-op comparison. Compare the same kind of in-process timed sync step, not the same call: the owner calls `fdatasync` on one file, while Native calls `fsync` on every registered Value-WAL file and on the directory. Native has only means for its sync step. Owner values are shown as mean and p50. At 1 KiB c1 the two are equal within the owner tail: owner TCP mean 28.7 µs (p50 26.2), owner verbs mean 34.0 µs (p50 27.1), native mean 27.3 µs. At 64 KiB c1 the owner step is slower: mean 152.2 µs (TCP) and 159.4 µs (verbs, p50 150.4) against a native mean of 114.8 µs (+37 and +45 µs by mean, uniform on all five owners, cause open). The fio `fdatasync` floor (19.3 µs at 1 KiB, 37.8 µs at 64 KiB for the sync step) overwrites a pre-written file and is not a WAL-append reference. See `analysis/output/sync_step.md` of run 8 and the append floor section below.

## Device-only selection (`device`, group G10, 9 November 2026)

Goal: the time of a remote shard action when the data comes from, or goes to, the NVMe SSD and not a cache, for owner verbs, owner TCP, hybrid kernel direct and hybrid SPDK.

**Why run 6 did not measure it for the owner designs.** The G1 owner cold arms read an ascending key sweep. The preload puts 32 consecutive keys into one batch, so consecutive keys sit next to each other in one owner WAL. The retained-WAL open path (`WalFiles::load_retained`) does not set `POSIX_FADV_RANDOM`, so kernel readahead is active and one device read serves many following reads. The measured reads per device read were 30, 14 and 1.8 at 1, 8 and 64 KiB (windows of about 33, 119 and 119 KiB). The mapped-WAL eviction evidence of the history arm shows `resident_bytes` 0, and that arm still read 0.03 device reads per call. [INFERENCE] This is a fit to the data. The device-only run tests it. The library is not changed.

**Method.**
- Random keys with replacement (`key_order: random_with_replacement`, so `measurement_key_sweep` is false) over a spread dataset of 65,536 keys at every size. Random access defeats readahead, and the key count keeps repeated pages rare. The sealed preparer admits at most 65,536 keys (`nvmeof_campaign.rs` `MAX_KEYS`), and a hybrid reader charges 4 KiB per catalog entry against a 448 MiB allowance, so 65,536 is the largest size that runs. With 4,096 random reads the expected share of reads that touch a new key is about 0.97 at 8 and 64 KiB. At 1 KiB about four values share a 4 KiB page, so 1 KiB cases measure 2,048 reads (about 0.94; 4,096 reads would give about 0.88, below the 0.9 cold test).
- Owner arms: the runner evicts the mapped WAL (`MADV_DONTNEED` and `POSIX_FADV_DONTNEED` on the whole WAL files), then the controller runs `sync` and `drop_caches`, then the gate is released. Hybrid direct arms keep the G1 gate (drop only): `O_DIRECT` at the target and the initiator, coordinator cache 0.
- The owner designs have no `O_DIRECT` mode: the owner reads through the page cache with buffered `io_uring`. Reads that miss the page cache are the SSD-served reads. The device counters qualify each case (at least 0.9 device reads per call). The qualification does not test `history_loads`; the analysis reports the change per case, and a non-zero change means authority history pages were read as well. A case that fails the cold test stays unqualified.
- Puts: tier `synchronous` waits for the covering fsync (the SSD path); tier `native` acknowledges after the page-cache append (the buffer). Hybrid designs write through the owner verbs protocol, so they have no separate put cases.

**Matrix** (120 cases, three repetitions of 40). G10R: 4 routes x (1, 8, 64 KiB at c1 and 1, 64 KiB at c16) = 20 per repetition. G10W: 2 routes x 2 tiers x (1, 64 KiB x c1, c16) = 16, plus 4 sustained cases (1 KiB, c16, 200,000 puts) = 20 per repetition. The first 7 cases are canaries, one of each new flow, and the execution order is the matrix order. The matrix digest is bound in `manual_campaign.REVIEWED_SPECS['device']`.

**Known limits.** Four thousand and ninety-six samples per read case (2,048 at 1 KiB) leave p99.9 descriptive. The 20 s window of run 6 (G4) showed a put stall regime shared by all designs (throughput 25k to 7.5k puts/s, about half of worker time in puts over 100 ms); the sustained cases test whether the `native` tier shows it too. No cause is assigned. An SSD can serve reads from its own buffers. Direct I/O does not attest NAND access.

**Results (run 7: fleet `bench-6e40ca83d74146cf`, 120 of 120 cases, three repetitions of every case).** Files: `checkpoint-work/focused-launch-20261109-g10b/analysis-final/` (`output/REPORT.md`, `coherency/coherency.md`, `summary.py`, `spdk_offset.py`). Cache contract: owner arms read through the page cache after the mapped-WAL eviction and `drop_caches`, with random keys over 65,536 keys, so 95 to 100 percent of the calls reached the SSD (0.95, 0.975 and 1.00 device reads per call at 1, 8 and 64 KiB). Hybrid arms read with `O_DIRECT` (1.00 device read per call on the kernel arm). Every read case qualified and no case changed `history_loads`. The tables show the median of three repetitions. For reads, the p50 range over repetitions is at most 8 percent (largest: owner verbs 1 KiB c16, 118 to 127 us). For puts it is at most 9 percent (largest: owner TCP `synchronous` 1 KiB c16, 350 to 382 us).

|SSD-served reads, p50 us|1 KiB c1|8 KiB c1|64 KiB c1|1 KiB c16|64 KiB c16|
|---|---:|---:|---:|---:|---:|
|owner verbs|114|128|162|122|184|
|owner TCP|123|143|211|133|250|
|hybrid kernel direct|115|131|150|114|159|
|hybrid SPDK|111|124|142|113|154|

Reads per second at c16 (1 KiB, 64 KiB): owner verbs 94.7k and 44.2k, owner TCP 89.0k and 34.7k, hybrid kernel direct 122.7k and 70.7k, hybrid SPDK 124.7k and 74.1k.

|Puts, p50 us (`synchronous` / `native`)|1 KiB c1|1 KiB c16|64 KiB c1|64 KiB c16|
|---|---:|---:|---:|---:|
|owner verbs|127 / 78|365 / 333|468 / 295|1750 / 1624|
|owner TCP|136 / 87|368 / 343|523 / 349|1759 / 1699|

- The four routes differ by 11 percent or less at 1 KiB c1 (111 to 123 us). At 64 KiB c1 the hybrid direct arms are 7 to 33 percent below the owner arms (SPDK 142, kernel 150, owner verbs 162, owner TCP 211 us).
- Coherency: a cold owner read equals the run 6 resident read plus the run 6 local fio floor within a ratio of 1.06 to 1.14. The TCP minus verbs gap is 9.1, 15.1 and 48.9 us at 1, 8 and 64 KiB, against 8.5, 10.3 and 41.1 us on resident data. The security classes differ: owner TCP runs with mutual TLS (`Transport::Tcp`, `mutual_tls`), owner verbs and both hybrid arms run the trusted verbs transport (`trusted_network_no_tls`), so the gap includes the TLS cost. The like-for-like partner of the trusted verbs transport, `tcp-trusted`, was not run. Kernel direct and SPDK differ by 4 to 6 percent. The median p50 ratio between repetitions is 1.016 (maximum 1.091). Twelve c16 cases have a Little's-law value of 10.4 to 13.9 against 16 workers; this is informational (client time outside the response clock).
- Run 6 read an ascending sweep. For the direct arms its p50 was 1.84 to 1.85 times lower at 1 KiB, 1.71 to 1.77 times lower at 8 KiB and 1.07 to 1.08 times lower at 64 KiB than the random-key value above. The run 6 hybrid direct latencies at 1 and 8 KiB (60 to 74 us) are not random-access SSD times. [INFERENCE] SSD-internal buffering of sequential reads; this is not measured.
- The SPDK arm has 1.08 to 1.18 kernel device reads per call. The SPDK bdev layer counts exactly 1.000 read per call with the same aligned size as the kernel arm (4.98, 12.04 and 68.0 KiB). The excess is a constant of about 345 to 370 kernel reads (about 38 MiB) per case, the same at 2,048 and 4,096 calls and at c1 and c16. The origin is not identified. It is not a per-call cost.
- At 1 KiB c1 the read latency is bimodal: a fast mode near 62 to 76 us and a slow mode near 110 to 135 us. The share of fast reads is 41 percent for kernel direct in all three repetitions, 5, 28 and 22 percent for SPDK, 16 to 17 percent for owner verbs (about 5 percent of those are page-cache hits) and 9 to 17 percent for owner TCP. The p50 sits at the edge of the slow mode, so the 1 KiB p50 values are fragile (kernel direct: mean 103.6 us, p50 115.3 us). At 8 and 64 KiB the fast share is at most 6 percent. The cause is not identified. [INFERENCE] SSD-internal read caching on the 70 MB footprint of the 1 KiB dataset. Per-component mean times for every read route are in `checkpoint-work/focused-launch-20261109-g10b/analysis-final/REPORT-run7.md`.
- Sustained puts (200,000, 1 KiB, c16): all four routes and tiers average 13.2 to 13.5k puts/s. Throughput falls from 22.6 to 25.1k puts/s in the first second to 10.0 to 10.6k puts/s late, p99.9 is 117 to 128 ms, and the whole pipeline stalls (no put completes for more than 5 ms) about every 2,060 puts on `synchronous` and every 1,560 to 1,900 puts on `native`. The stall length follows 17 to 21 ms (`synchronous`) or 3 to 7 ms (`native`) plus 0.53 to 0.65 us per cumulative put, the same on verbs and TCP. [INFERENCE] The growing part is coordinator or owner checkpoint work that depends on the number of puts, not on the transport or on fsync. No counter proves which phase grows.

## Sui KV capture (diagnostic)

`SUI_TH_KV_CAPTURE_DIR` names a fresh directory. The adapter reads it once when the first `distributed::sui::Db` opens. With the variable unset, each capture site costs one relaxed atomic load and one branch. Records go through one bounded channel (65,536 fixed-size records) with `try_send`; a writer thread owns all file I/O. Any dropped record makes the capture invalid. The controller must wait for `final.status` before it ends the node.

The hooks sit at the Sui table API (`sui.rs`), at the coordinator cache lookup and fill (`remote_db/read_cache.rs`) and at read route dispatch (`remote_db/pending_read.rs`, `pending_read/concurrent.rs`). Native and remote nodes enter the same API boundary. Only TideHunter-backed tables are visible. Native physical versions are not exposed. Checkpoint labels are temporal, because checkpoints execute concurrently.

Tools in `experiments/sui-checkpoint/`: `capture_run.py` wraps the frozen runner for the measured binaries, `trace_summary.py` and `trace_metrics.py` compute the workload summary and write one six-column evaluator TSV per logical DB for `trace_eval.rs`. The first private-fixture result is in `checkpoint-work/sui-kv-trace-20261107/REPORT.md` (4,096 user transactions, not mainnet; zero drops; capture-on wall time differed from capture-off by -17% to -38%, so the traces are workload evidence, not latency benchmarks).

## Mainnet checkpoint characterization

`experiments/sui-workload/` decodes public mainnet checkpoints (`checkpoints.mainnet.sui.io`, zstd protobuf `sui.rpc.v2.Checkpoint`, about 30 days of retention), recomputes every transaction digest and writes workload statistics and `workload-model.json` for a generator. The first result is in `checkpoint-work/sui-workload-20261107/primary/REPORT.md` (12 windows of 2,000 checkpoints). Checkpoint data is not a node KV trace: it shows transaction mix, object sizes and reconstructed input-object locality only.

## Append write floor (`append_floors`, G13W)

The G11W `fdatasync` floor overwrites a pre-written file. It is not a WAL
append reference: a WAL appends to fresh sparse pages. G13W compares that
overwrite floor with `pwrite` plus `fdatasync`, mmap store plus `fdatasync`,
and mmap store plus `fsync`. Each append case extends a sparse file and
writes each offset once. It has 512 warmup and 4,096 measured operations.

The 48 cases cover four modes, 1, 8 and 64 KiB blocks, client and owner 0,
and two repetitions. The 1 KiB client mmap case is the first canary.
Within each block and mode, host order is client, owner 0, owner 0, client.
The overwrite case uses its existing timed, pre-written-file contract.

| Comparison | Interpretation |
|---|---|
| Owner 0 append floor is near client native sync time | [INFERENCE] The owner path adds the gap. |
| Owner 0 append floor is near owner sync time and client floor is near native sync time | [INFERENCE] The VM or SSD difference explains the gap. |
| Other result | Mixed; the comparison does not isolate one cause. |

Compare matched sizes and durability calls. The mmap `fsync` floor does not
include the native directory `fsync`; account for that call separately.

## Fair native comparison and 64 KiB write profiles (planned, not run)

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

Run the standalone analysis on a finished result tree:
`python -B experiments/kv-comparison/fair_native.py --export EXPORT --report REPORT --output OUTPUT`.

## Compact fleet selections (planned, not run)

The six-host fleet has one coordinator and five owners. It matches the
five-node cross-store comparison; Tidehunter does not require five owners.
The compact layout has one coordinator and one owner.

`parity_compact` contains the 177 cases from `parity` that need at most one
owner host: the 48 append floors, Native and one-owner cases. The separate
`fair_five_owner` selection contains the other 39 cases and uses six hosts.
The selections are disjoint and preserve their `parity` order. They run on
different fleets, so their results do not establish same-fleet parity.

[INFERENCE] Run 8 took 136 minutes for 270 cases, about 30.2 seconds per
case. With 14 minutes of fleet overhead and $3.287109973 per VM-hour:

| Selection | Estimated elapsed hours | Estimated compute cost |
|---|---:|---:|
| `parity_compact`, 177 cases, 2 VMs | `(177 × 30.2 / 60 + 14) / 60 = 1.7182` | `1.7182 × 2 × $3.287109973 = $11.30` |
| `fair_five_owner`, 39 cases, 6 VMs | `(39 × 30.2 / 60 + 14) / 60 = 0.5605` | `0.5605 × 6 × $3.287109973 = $11.05` |
| `parity`, 216 cases, 6 VMs | `(216 × 30.2 / 60 + 14) / 60 = 2.0453` | `2.0453 × 6 × $3.287109973 = $40.34` |
| `fair_native`, 168 cases, 6 VMs | `(168 × 30.2 / 60 + 14) / 60 = 1.6427` | `1.6427 × 6 × $3.287109973 = $32.40` |

These are
planning estimates, not limits, and exclude other charges. The reservation
ceiling is 4,834 cents for compact versus 14,500 cents for six hosts.
Each launch still needs the owner's explicit authorization. No package has
been built for these selections.

## Compact remote reads and writes (`remote_rw_compact`, run 9)

Run 9 used the reordered 342-case matrix, SHA-256
`b2f2afc44afc1d066bedb27f819c5fa7789e000f96da0c7beb4c6f9359122620`.
The case IDs and jobs are the same as in the first draft order. Two
`z4m-highmem-16` VMs, one coordinator and one owner in `us-central1-a`, were
created on 10 November 2026 at 12:03:35 UTC and deleted at 14:10:20 UTC,
about 2 hours 7 minutes later. Of 342 planned cases, 339 completed and 3
failed. Estimated compute cost at list price is about $13.9, not an invoice.

- All three repetitions of the G11N `ib_write_lat` event-mode network floor
  failed with `network floor server exited before listener readiness`. The
  other 14 network-floor variants completed.
- Six Native cold 1 KiB cases did not qualify under the 0.9 device-reads-per-call
  gate: their ratios were 0.28, 0.28, 0.44, 0.43, 0.75 and 0.76. Native
  cold 8 and 64 KiB cases and all 36 owner cold cases qualified; the owner
  ratios were about 0.95 per call.
- Owner reads used one read lane at one and five readers, and four lanes at
  16 readers; Native used zero. The owner five-reader points are lane-limited.
- The first attempt aborted after about seven minutes and eight of nine append
  floors, at an estimated $0.8 compute cost. A descriptor closed and was reused
  during helper startup while the write-floor safety check inspected
  `/proc/<pid>/fd`. The fix uses bounded self-consistent descriptor inspection
  for both write floors and the read-only fio check. The revised order starts
  with a 46-case canary head to exercise each flow shape early.

Latency results are in the run 9 section of
`docs/research/supervisor-brief-2026-11-10/`.

## Fabric A/B and NVMe-oF reads (`fabric_ab_compact`, run 10)

Run 10 used the 80-case matrix, SHA-256
`8e12ba4352dc461680565b3bfaef5db705e702e03bce986323cc1e31ae9bde6e`. Two
`z4m-highmem-16` VMs, one coordinator and one owner in `us-central1-a` with
compact placement, were created on 10 November 2026 at about 21:56 UTC. The
last case finished at 23:07 UTC. The VMs were deleted by about 23:12 UTC, about
1 hour 15 minutes after creation, and the launcher verified that both VMs and
the placement policy were deleted. All 80
cases completed, and none had an unsuccessful call. All 40 cold cases
qualified under the device-reads-per-call gate (0.95 to 1.16 per call). The
estimated compute cost at list price is about $8.2, not an invoice.

The first attempt aborted after nine cases, about 15 minutes and an estimated
$1.6 of compute. After the SPDK canary, the sealed SPDK target metrics in
`observability/campaign_exporter.py` required five owner rows, and this fleet
has one. The exporter now accepts one or five contiguous owner rows. The
monitoring gate stayed fatal. No result of the first attempt is used.

Latency, CPU and NVMe-oF results are in the run 10 section of
`docs/research/supervisor-brief-2026-11-10/`. The analysis tool is
`experiments/kv-comparison/fabric_ab.py`.

The matrix design follows.

The 80 cases comprise two groups of 40. Each group has five arms across
1 and 64 KiB values, 1 and 16 readers, and two repetitions (0 and 1).

| Group | Arms | Workload |
|---|---|---|
| G16P | TCP mutual TLS adaptive; TCP trusted adaptive; TCP trusted busy; verbs trusted adaptive; verbs trusted busy | Warm repeat reads, memory expected, synchronous tier; one read lane at c1 and four at c16 |
| G16C | Owner TCP trusted; owner verbs trusted; NVMe-oF RDMA kernel direct; NVMe-oF RDMA SPDK; NVMe-oF TCP kernel direct | Cold first-touch reads, 65,536 keys, random with replacement; 2,048 operations at 1 KiB and 4,096 at 64 KiB |

The cold owner arms use adaptive polling and selective mapped-WAL eviction.
The NVMe-oF arms use the sealed-read route without mapped-WAL eviction; the
kernel targets use direct I/O. Every NVMe-oF arm fixes the owner control
channel to trusted verbs with adaptive polling, including NVMe-oF over TCP.
SPDK over TCP is not included.

Within each group, size, reader count and repetition define a matched cell.
The complete cell has all five arms with identical workload and dataset
geometry. G16P differs only in backend and owner polling. The allowed G16C
per-arm fields are `backend`, `owner_polling`, `nvmeof_route`,
`nvmeof_target`, `target_io`, `evict_mapped_wal_before_measurement`,
`media.wal_eviction`, `case_id`, `profile`, and the dataset fields
`source_id`, `directness`, `target_cache`, `mapped_wal_eviction`,
`cold_claim` and `page_cache_policy`. Both owner arms
use `evict_mapped_wal_then_drop_at_measurement_gate`; all NVMe-oF arms use
`drop_at_measurement_gate`. SPDK and kernel targets also differ in the
documented dataset `directness` and `target_cache`.
Compare only matched cells and retain each repetition separately.
TCP mutual TLS versus TCP trusted includes the security change;
TCP trusted versus trusted verbs matches the security class. Owner buffered
reads and direct NVMe-oF targets have different cache contracts. Device reads
per call qualify cold cases as a cohort proxy, not proof of individual NAND
access; report unqualified cases and their reasons.

The first 15 cases are repeat-0 canaries: the 1 KiB c1 case for each warm
arm, then each cold arm, followed by the 64 KiB c16 cases for the three
NVMe-oF arms and the two busy arms. Each canary appears once. The remaining
warm cases run first, then cold owner cases, then cold NVMe-oF cases; arm
order is forward for repeat 0 and reversed for repeat 1.

Busy means **owner spins (Busy policy)** in user space; the coordinator read
driver also spins. It is not kernel busy polling (`SO_BUSY_POLL`). One-sided
RDMA READ is not implemented and is not tested. CPU cores are computed from
per-process CPU seconds divided by elapsed seconds; hosts are sampled
separately. Missing counters are not recorded, not zero. One fleet and two
repetitions cannot establish fleet spread or statistical significance.

The pre-run planning estimate used the run 8 average of 30.2 seconds per case
and 14 minutes of fleet overhead, which gave about 0.90 hours and `$5.95`. The
actual elapsed time was about 1.25 hours (the suite phase ran from 21:58 to
23:07 UTC, about 52 seconds per case on average). The compact reservation
ceiling was 4,834 cents.

## Leased extents A/B (`leased_ab_compact`, run 11)

Run 11 ran this selection on 11 November 2026: fleet
`bench-775352a846e64517`, 72 of 72 cases completed, no failed or rejected
call. Results and launch history are at the end of this section. The
selection runs only what changed on 11 November 2026: the `leased`
live write mode, the caller-inline live read, and a regression check of the
unchanged owner RPC route. It has no external store and no Native arm.

Fleet: the compact layout of runs 9 and 10. Two `z4m-highmem-16` VMs in
`us-central1-a` with compact placement: one coordinator and one owner.

The matrix has 72 cases, SHA-256
`52029dc2047cecca4903b4240d7a84d8dcb402bfbfddcad827d605405f15c2dc`. The 13
earlier selections and their hashes are unchanged.

| Group | Arm | Cases |
|---|---|---:|
| G17W | Owner RPC put | 16 |
| G17W | `leased` put | 16, plus 8 write-profile copies |
| G17W | `reserved` put | 8, plus 2 write-profile copies |
| G17R | Live read, inline on | 8, plus 2 batch reads of 32 keys |
| G17R | Live read, inline off | 8 |
| G17R | Owner RPC read | 4 |

[INFERENCE] Cost estimate: run 10 used the same fleet for 80 cases and took
about 1 hour 15 minutes (about `$8.2` of compute at list price). In proportion
the 72 cases need about 1 hour 10 minutes and about `$7.4`. This selection adds
the two-host live pair qualification, which run 10 did not have; its bound is
20 minutes. The upper estimate is about 1 hour 30 minutes and `$10` of
compute. The reservation ceiling of the compact fleet stays 4,834 cents and
the VM expiry stays six hours. These are estimates, not an invoice.
Actual: the suite took 29 minutes 31 seconds (19:39:05 to 20:08:36 UTC), the
fleet existed for at most 41 minutes, and compute cost at most about `$4.50`
(2 hosts, 41 minutes, list price; estimate, not an invoice).

| Group | Arms | Workload |
|---|---|---|
| G17W | Owner RPC (trusted verbs, adaptive); live NVMe-oF RDMA `reserved`; live NVMe-oF RDMA `leased` | 100% put, 4,096 measured puts after 512 warm-up puts; 1 and 64 KiB; 1 and 16 writers; `synchronous` and `native` tiers; repetitions 0 and 1. The `reserved` arm has 1 KiB only. |
| G17R | Live NVMe-oF RDMA read with the inline route on; the same with the inline route off; owner RPC read (trusted verbs) | 100% read of unsealed data that the case preloads through the `leased` mode; warm (prewarm key sweep); 8,192 measured reads; 1 and 64 KiB; 1 and 16 readers; repetitions 0 and 1. The owner RPC read has one reader only. |

The live route uses the kernel NVMe-oF target with buffered file namespaces:
the target page cache is the shard cache. Each `leased` cell has one
write-profile copy (repetition 0). It carries the adoption stages and counters.

The G17R cases carry the cohort `primary_rf1`: the data is not sealed. The
two batch-read cases issue 100% `get_many` calls of 32 keys with the inline
route on; the inline route serves scalar reads only, so they measure the
merged lane read.

Contracts to state next to every result:

- Owner RPC `synchronous` acknowledges after the owner frontier root.
  `leased` `synchronous` acknowledges after the FUA write; the owner root
  follows asynchronously. These are different contracts. Do not rank them
  without that statement.
- Both `native` arms acknowledge at a page-cache write on the owner host.
- The live read arms read through the target page cache. The owner RPC read
  reads the owner WAL through its page cache. No case in this selection is a
  device read.
- The inline-on and inline-off read arms differ only in
  `LiveClientConfig.inline_reads`.

Evidence gates per case: a `leased` case must show zero reserve and complete
exchanges and at least one leased frame; in the `synchronous` tier the FUA
write count must equal the frame count plus the skip-marker count (an extent
switch writes one FUA marker block); an inline-off case must show zero inline
admissions. A violated gate fails the case.

Reading rule: each 4,096-put case contains about four owner reference-history
spills (one for each 1,024 batches) on either route. An owner RPC put waits
for the spill; a `leased` put waits in a history-full settle. A percentile can
miss these few long calls. Report the mean, the settle count and the settle
time of every `leased` case, and the calls above 5 ms of every owner RPC case.
See `PERFORMANCE_PLAN_2026-11-10.md`, "Reference-history spills".

Not in this selection: power-loss or kernel-crash qualification of the FUA
acknowledgement, more than one frame in flight, NVMe-oF over TCP, SPDK
targets, cold reads, external stores.

Analysis tool: `experiments/kv-comparison/leased_ab.py`.

Offline verification done: all Python suites, the matrix and observer
qualification of the reviewed specification, and the current completed-case
admission on four real cases of the standard live campaign of 4 November
(read, write, batch read, mixed). The three items that could not be verified
offline passed in run 11: the completed-case admission of G17 cases, the live
target on the compact fleet, and the pair qualification on the fleet's own two
hosts.

The package is built and sealed offline in the parent workspace under
`checkpoint-work/focused-launch-20261111-leasedab<N>/` (the highest `N` is
current): `stage1-build.py` freezes the source and builds in the container
without network; `stage2-package.py` stages the runtime with the nvmet
partial-read fix and without SPDK, runs the local qualification and the
observer admission, and rejects a changed specification. `make-go.py` writes
the launch record only from the owner's launch message. A source change after
sealing needs a new seal.

Launch prerequisites that need cloud access: the READY image and its
generation-pinned qualification receipt, quota and placement in
`us-central1-a`, and the remaining weekly capacity in the budget ledger
object. Steps: `experiments/gcp-rdma/setup.md`, section on
`leased_ab_compact`.

### Run 11 results

Inputs: `checkpoint-work/focused-launch-20261111-leasedab2/analysis/output/`
in the parent workspace (`leased_ab.md` and four CSV files). The controller
archive hash matches its receipt. Two repetitions per cell on one fleet: the
ranges below are the two repetition values, not a confidence interval.

Puts, p50 in µs (range over the two repetitions):

| Tier | Size | Writers | Owner RPC | `leased` | `reserved` |
|---|---|---:|---:|---:|---:|
| synchronous | 1 KiB | 1 | 115–119 | 153–154 | 426–428 |
| synchronous | 1 KiB | 16 | 482–500 | 486–501 | 746–771 |
| synchronous | 64 KiB | 1 | 426–431 | 313 | not run |
| synchronous | 64 KiB | 16 | 4422–4621 | 2545–2596 | not run |
| native | 1 KiB | 1 | 63.9–64.8 | 62.5–62.9 | 418–427 |
| native | 1 KiB | 16 | 334–335 | 323–326 | 753–757 |
| native | 64 KiB | 1 | 264–268 | 184–188 | not run |
| native | 64 KiB | 16 | 3485–3496 | 2163–2178 | not run |

The synchronous owner RPC and synchronous `leased` rows have different
acknowledgement contracts (see above).

- `leased` against `reserved` (the old live write): 2.8 times faster at
  1 KiB synchronous and 6.7 times at 1 KiB native, one writer.
- `leased` against the owner RPC at 1 KiB: equal in the native tier; 29 to 33%
  slower at p50 in the synchronous tier at one writer; equal at 16 writers.
- `leased` against the owner RPC at 64 KiB: 27 to 44% lower p50 in all four
  cells; 22 to 60% more puts per second.
- Stage means of the profiled copies (one repetition each): the NVMe-oF write
  from submission to completion takes 30.2 µs (native, 1 KiB), 122.9 µs
  (synchronous, 1 KiB), 73.7 µs (native, 64 KiB) and 199.5 µs (synchronous,
  64 KiB). The FUA write on the buffered nvmet file backend costs about 93 µs
  more than the page-cache write at 1 KiB. The owner RPC pays 51 to 55 µs for
  its covering sync (p50 difference between its tiers). The cause of the
  target-side cost is not isolated.
- Evidence gates: all 24 `leased` cases have zero reserve and complete
  exchanges and zero failed, short or timed-out writes; FUA writes equal
  frames plus skip markers in the synchronous tier and are zero in the native
  tier.

Reference-history stall (1 KiB only):

- Owner RPC, native, one writer: the calls above 5 ms fall at puts 1016,
  2040, 3064 and 4088 of the preload in both repetitions (48 to 62 ms each),
  and three more fall in the measured 4,096 puts (24 to 69 ms). They hold
  34.5 to 35.4% of the summed API time. The period equals the owner's
  1,024-batch history window.
- `leased`, native, one writer: three calls of 58 to 64 ms in the measured
  puts hold 39.6 to 39.8% of the time.
- 16 writers: the calls above 5 ms hold 45.6 to 46.2% of the time on both
  routes in the synchronous tier, and 70.7 to 70.8% (owner RPC) and 64.0 to
  64.8% (`leased`) in the native tier. All three routes end between 13,300
  and 17,100 puts per second.
- No call above 5 ms at one writer in the synchronous tier, and none at
  64 KiB at one writer in either tier.
- `adoption_history_full_settles` is zero in all 24 `leased` cases. With
  1 KiB values a 4 MiB extent holds 1,024 frames of 4,096 bytes, so the
  extent switch comes before the prefix history is full. The profiled copy
  shows three extent switches of 15.8 ms mean; the rest of each long call is
  not attributed to a stage. Use the columns `calls_ge_5ms` and
  `time_ge_5ms_share`, not the settle counter.
- The owner was not traced. [INFERENCE] The period, the duration and the
  growth with the number of puts match the reference-history spill measured
  on the workstation.

Reads of unsealed data, warm, p50 in µs:

| Size | Readers | Inline on | Inline off | Owner RPC |
|---|---:|---:|---:|---:|
| 1 KiB | 1 | 30.6–31.0 | 40.3 | 30.3–30.6 |
| 64 KiB | 1 | 49.9–51.0 | 61.4–65.8 | 57.2–57.4 |
| 1 KiB | 16 | 101.5–102.4 | 102.9–104.3 | not run |
| 64 KiB | 16 | 234.5–237.7 | 236.3–237.0 | not run |

- One reader: every measured read is one NVMe-oF read command (8,192 commands,
  zero owner reads); the command takes 28.7 µs from submission to completion
  at 1 KiB (one repetition).
- 16 readers: the inline route admits 1,169 to 1,375 of 8,192 reads; the
  others find the route busy and use the lane worker. No difference.
- Batch read of 32 keys of 1 KiB: 345 to 348 µs per call.

### Launch history (11 November 2026)

|Launch|Stopped at|Cause|Fix|
|---|---|---|---|
|`leasedab1`, fleet `bench-7a4ed25bf3d04b3d`|Pair qualification, after about 6 minutes; no case ran; about `$0.6` (estimate)|Cloud Storage closed one HTTPS connection during a status poll. `GoogleApi._request` retried a GET on `URLError` and `TimeoutError` only; `http.client.RemoteDisconnected` reaches the caller unwrapped.|`gcp_api.py` retries a GET on `ConnectionError` too; `manual_campaign.py` treats it like the other transport errors in the terminal wait. New seal `leasedab2`, same binary hash.|
|`leasedab2`, fleet `bench-775352a846e64517` (run 11)|Completed|||

Open: `ObjectStore.download` (the media read) has no retry for a transport
error. It was not the failing call and is not changed.

## NVMe-oF data path (`nvmeof_path_compact` and `nvmeof_path_core`, run 12)

Goal: measure NVMe-oF as the data path for puts and reads of the distributed
store, with the defaults of the original engine, against the `native` backend
of the same source tree. The `native` backend runs in the benchmark process on
the coordinator VM and uses the Local SSD of that VM. The owner VM has the
same machine type and the same SSD type; the two SSDs are different devices.

Rules of this selection:

- The reference arm is the `native` backend of this source tree with the
  original default configuration (128 MiB fragments, 10 GiB WAL files,
  16 mapped fragments, one shard, page-cache acknowledgement).
- Each job carries every setting explicitly. A setting that differs from the
  engine default is listed for each arm in the generated settings table
  (`nvmeof_path.py --settings-only`), column "differs from engine default".
- Remote results exclude the owner history: every remote case runs with
  `history_persistence: memory_only`. Such an owner has no restart with data,
  no WAL file retirement and no owner RPC read. Recovery and retries are not
  assessed.
- A case in which a cache holds the full data set is a cache case (G18M). It
  is not evidence for NVMe.
- A cold case below 0.9 device reads per read is reported with its ratio as
  "partly cached". It is not used as SSD evidence.

Fleet: the compact layout of runs 9 to 11 (two `z4m-highmem-16` VMs in
`us-central1-a`, compact placement, one coordinator and one owner).

The matrix has 157 cases, SHA-256
`8da5d9479b5fc470ff90c21211b3c3510a66d15bd3f03e520c9eefe38b7b2036`. The 14
earlier selections and their hashes are unchanged. Every read cell uses one
population of 65,536 keys; every put cell uses 4,096 keys, as in run 11.

| Group | Question | Cases |
|---|---|---:|
| G18F | What do the network and the SSD cost alone? `ib_send_lat` and `ib_read_lat`, polled and event (4); SSD read at queue depth 1, 4 KiB and 64 KiB (2); append and `fdatasync` write floors at 1,024 and 65,536 bytes (4) | 10 |
| G18N | What does the engine do alone on one VM of the fleet with original defaults? Puts at 1 and 64 KiB, 1 and 16 writers, `page_cache` and `value_wal_barrier` (16); reads at 1 and 64 KiB, 1 and 16 readers, memory and cold (16) | 32 |
| G18W | Do NVMe-oF writes reach the page-cache contract of the engine? Owner RPC and `leased` puts at 1 and 64 KiB, 1 and 16 writers, tiers `native` and `synchronous` (32); one write-profile copy, one write-timeline copy, one sustained case of 200,000 puts | 35 |
| G18R | Does a live NVMe-oF read from the SSD cost one device read? 65,536 keys, coordinator cache 0, eviction and `drop_caches`. One reader: inline, lane, inline polled. 16 readers: 4 lanes 1 × 32, 4 lanes 4 × 128, 8 lanes 4 × 128 | 24 |
| G18M | What does each cache add? Owner page cache warm (16); coordinator cache of 8 MiB with a Zipf 0.99 key choice over 65,536 keys (2) | 18 |
| G18B | Do batches of 32 operations keep the result? `put_batch` at 1 and 8 KiB on native, owner RPC and `leased` (24); `get_many` warm (4) and cold (2) | 30 |
| G18X | Does a read stay direct when the data is in more WAL files than a lane had grants? 64 KiB values, 65,536 keys, owner layout `sui_harness` (256 MiB files, about 17 files), cold | 8 |

The first 12 cases are canaries, one for each flow. The seventh canary, the
64 KiB cold inline read, carries `require_media_qualified`: the run stops when
this case is not served from the device. The eleventh canary (G18X, one
reader, 1 × 32) is the first case that adds namespaces to a connected
subsystem.

Settings that differ from the engine default, by arm type:

| Arm | Settings |
|---|---|
| `native` | none |
| Owner RPC put | `history_persistence=memory_only`, `coordinator_cache_bytes=0`, `read_lanes` 1 or 4 |
| Live NVMe-oF | the owner RPC settings, and `live_export_mode=per_owner`, `live_write_mode=leased`; polled arms `live_poll_queues=1`; wide arms `live_io_queues=4`, `live_io_queue_size=128`; lane arms `live_inline_reads=false` |
| G18M coordinator cache | `coordinator_cache_bytes` stays at the default of 8 MiB |
| G18X | also `owner_wal_geometry=sui_harness` |

New behaviour that this selection enables (each is off by default):

- `live_export_mode: per_owner`: the owner adds each WAL file of the active
  epoch as a namespace of one subsystem. A lane sends one request
  (`OpenLiveExports`) for each connection. After that a read needs no owner
  request: the coordinator computes the namespace from the WAL position.
- Queue geometry of the live controller (`live_io_queues`,
  `live_io_queue_size`, `live_poll_queues`) and a polled ring for the
  caller-inline read.
- `history_persistence: memory_only` (needs the `test-utils` feature).
- Mapped-WAL eviction on an idle live stream and device counters with
  sectors for reads and writes (media evidence `schema_version` 2).

Evidence gates per case, in addition to the run-11 gates for `leased` puts:

- The reported `live_export_mode` and queue geometry equal the job.
- A permanent-export case makes at most one controller connect in the
  measured interval. A read case sends at most `read_lanes + 1` owner
  requests in the measured interval.
- The owner receipt reports `history_persistence: memory_only`. The two
  diagnostic copies also need zero history waits.
- Polled cases report one poll queue from sysfs.

Analysis tool: `experiments/kv-comparison/nvmeof_path.py`. It keeps each case
and repetition, gives the ratio to the `native` reference of the same cell and
contract, and labels cache cases and partly cached cases.

Not possible on the workstation (no root, no `nvmet`): a real NVMe-oF
connection with poll queues or 4 × 128, a permanent export on the kernel
target, a namespace added to a connected subsystem, and cold live reads. The
canaries are the first execution of these paths. If the G18X canary fails
because a namespace does not appear or cannot be enabled, the fallback is
`live_export_mode: per_file` in each live job, a new matrix, a new seal and a
new launch message.

[INFERENCE] Cost estimate of the full selection: 157 cases, about 1 hour
35 minutes and about `$10` of compute at the run-11 rate. The reservation
ceiling of the compact fleet stays 4,834 cents and the VM expiry stays six
hours.

### Launched subset (`nvmeof_path_core`, 66 cases)

The owner authorized run 12 on 12 November 2026 with this rule: two VMs, the
most relevant cases, not the longest. Run 12 therefore uses the selection
`nvmeof_path_core`: 66 of the 157 cases. A core case equals its case of the
full selection in every setting; only the selection name and the case ID
differ. The full selection stays defined and is not launched.

Order: the first launch used the order of the full selection (the same 12
canaries first; matrix SHA-256
`eb9c8779392b6c6cffd183c6434cd86e11250450dbd1b4c1e29580af8b1a87a1`). For the
second launch the owner asked for the failed cases first. Three cases now run
first: the native 1 KiB put (failed in launch 1), the 64 KiB cold live read
with `require_media_qualified` (failed in launch 1), and the native cold
`get_many` (same class of defect, found by review). The other ten canaries
and the remaining cases follow in the old order. Case IDs do not change.
Matrix SHA-256 of this order: `f286ce6c7255d21f98c9cef8b334b3d6540b1cc84005bc265e0f39751225ff55`.

| Group | Kept | Cases |
|---|---|---:|
| G18F | `ib_read_lat` polled and event; SSD read at 4 KiB and 64 KiB | 4 |
| G18N | Puts: page cache at 1 and 64 KiB with 1 and 16 writers; barrier with one writer. Reads: cold with 1 and 16 readers; memory with one reader, and with 16 readers at 1 KiB. Second repetition: page-cache puts with one writer, cold reads with one reader | 17 |
| G18W | Owner RPC and `leased`: native tier at 1 and 64 KiB with 1 and 16 writers; synchronous tier with one writer. Second repetition: native tier with one writer. The write-profile copy, the write-timeline copy and the sustained case | 19 |
| G18R | One reader: inline at both sizes and both repetitions; lane and polled at 1 KiB. 16 readers: the three geometries at both sizes | 12 |
| G18M | One reader: inline and polled at both sizes. 16 readers at 1 KiB: 1 × 32 and 4 × 128. The coordinator-cache case | 7 |
| G18B | `put_batch` at 1 KiB, one writer, page-cache contract, on native, owner RPC and `leased`; cold `get_many` at 8 KiB on native and live | 5 |
| G18X | One reader with 1 × 32; 16 readers with 4 lanes and 4 × 128 | 2 |

Not in the subset: `ib_send_lat` (run 8 measured it on this machine type), the
SSD write floors, the synchronous tier with 16 writers, the second repetition
of most cells, the lane and polled cold arms at 64 KiB, batches with 16
writers, with 8 KiB values or with the durable contract, warm `get_many`,
64 KiB cache cells with 16 readers, and two of the four many-files arms.

[INFERENCE] Estimate for the subset: about 41 minutes of suite time, about 50
to 55 minutes of fleet time and about `$6` of compute at list price.

The package is sealed offline with the run-11 procedure in the parent
workspace under `checkpoint-work/focused-launch-20261112-nvmeofpath<N>/` (the
highest `N` is current): `stage1-build.py`, `stage2-package.py`, and
`make-go.py` only from the owner's launch message. The directory `settings/`
there holds the generated settings table (`nvmeof_path.md`, `settings.csv`)
of the sealed matrix; it is part of the launch request. The binary, source and
matrix hashes are in the output of `stage2-package.py`. A source change after
sealing needs a new seal. Launch steps: `experiments/gcp-rdma/setup.md`,
section on the NVMe-oF data path compact campaign.

Stage B (Sui checkpoint replay over NVMe-oF) is not complete. The replay
harness accepts the new contracts, owner settings and two-host cells; the
staged build resolves the NVMe-oF feature graph. The three Sui binaries have
not been built and sealed for this experiment, and the manual launcher does
not admit a Sui specification yet. The owner has now limited work to finishing
and analyzing this run. No further run or cluster is authorized.

### Launch history (12 November 2026)

|Launch|Stopped at|Cause|Fix|
|---|---|---|---|
|`nvmeofpath1`, fleet `bench-d571d0e6333f416d`|Case 7 of 66, after about 4 minutes of suite time; the fleet existed for about 11 minutes; about `$1.4` (estimate). The run stopped itself at the cold canary gate; the launcher deleted both VMs.|Two defects in the Python controller. (1) A `native` put case with `device_counters` asked the owner host for `cells/<case>/node-0`, which a native case does not have; the first canary failed in 3 seconds. (2) Device evidence was computed only for the case kinds `media_read` and `native_media_read`, not for `live_read`; the 64 KiB cold canary with `require_media_qualified` had no evidence and stopped the run.|The controller takes the device counters of a native case on the coordinator VM for the native database, and computes media evidence for a `live_read` case with a `media` object. The report admits that evidence. A review of the other G18 case kinds found one more defect of the same class before it ran: the native cold `get_many` case has no sealed data set, and its snapshot looked for one; it now uses the case database. Tests cover each selection condition. New seal `nvmeofpath2`.|
|`nvmeofpath2`, three starts at 02:23, 02:28 and 02:31 UTC (fleet ids `bench-6e9c9a9355bc4d73` and `bench-d80e83cf0602491e` were reserved; no VM and no placement policy existed)|Before fleet creation|The uplink of the workstation was degraded (a Wi-Fi fault; then 116 to 280 KB/s measured to Cloud Storage). Start 1: the local `gcloud auth print-access-token` call exceeded its 30 s limit. Starts 2 and 3: the upload of the 16 MiB package failed with `The write operation timed out`. `DelegatedArtifacts` capped every socket operation at 10 s, and an upload sends 1 MiB for each socket write, so an upload needed about 105 KiB/s. Starts 2 and 3 each wrote a 48.34 USD reservation into the budget ledger; no compute cost.|Streamed uploads use a socket bound of 300 s (`UPLOAD_SOCKET_SECONDS`); other requests keep 10 s. The local token call has a 120 s limit. New seal `nvmeofpath3`.|

Observations of the first launch (one repetition, one writer or reader; not a
result set, the run was incomplete):

| Case | p50 µs | p99 µs | Calls ≥ 5 ms | Owner requests in the measured interval |
|---|---:|---:|---:|---:|
| Owner RPC put, 1 KiB, native tier | 55.8 | 96.6 | 0 of 4,096 | not applicable |
| `leased` put, 1 KiB, native tier | 56.9 | 94.1 | 0 of 4,096 | 3,991 (asynchronous adoption) |
| Live read, warm, 1 KiB | 29.2 | 34.5 | 0 of 8,192 | 1 (`OpenLiveExports`) |
| Live read, cold, 1 KiB | 106.7 | 176.2 | 0 of 2,048 | 0 |
| Live read, cold, 64 KiB | 141.9 | 220.1 | 0 of 4,096 | 0 |
| Native read, memory, 1 KiB | 1.0 | 1.4 | 0 of 8,192 | not applicable |

- The pair qualification passed with the changed engine on the default
  `on_demand` route.
- Permanent `per_owner` export, eviction of a live WAL stream, 10 GiB WAL
  files with 128 MiB fragments and `memory_only` history ran on the hardware.
  No controller connect and no error occurred in a measured interval.
- The controller computed no device evidence for the cold cases (defect 2).
  The raw `/proc/diskstats` text in the before and end snapshots of the owner
  gives, for the SSD of the owner: 2,022 device reads for 2,048 reads at
  1 KiB (0.987 for each read, 4,096 bytes for each device read); 3,983 device
  reads for 4,096 reads at 64 KiB (0.972 for each read, about 69,600 bytes
  for each device read); 0 device reads for the 8,192 warm reads. A cold live
  read was one device read of the aligned record size.
- Device counters of the two put cases: about 0.94 and 1.03 GiB written in
  the snapshot window for 4 MiB of payload, on both routes. [INFERENCE] The
  data file system is made with default `mkfs.ext4`; its lazy initialization
  writes in the background. The window is not a write amplification measure.
- Not executed: poll queues, 4 × 128, `put_batch`, the many-files case, the
  floors.

### Run 12 results and comparison with the previous architecture

Fleet `bench-b2eec4ef24bc4f90`, parent `bench-589b6160b5f04a84`,
`nvmeofpath3`: all 66 case receipts completed. The 62 workload cases offered
453,440 measured API calls; all succeeded, with no rejected call or wrong
value. The other four cases are standalone floors. The last checkpoint was
03:19:35 UTC; the controller finished at 03:19:36. Both VMs, their boot disks
and the placement policy were confirmed absent at 03:23:23 UTC. The fleet
existed for at most 37 minutes: about `$4.05` of compute at list price, an
upper estimate, not an invoice. No further run was launched.

**Terminal report defect, not a workload failure.** The original cloud
receipt is `incomplete`, with `error: null` and `monitoring_complete: true`;
the guest and local launcher exited with an error. `report-error.json`
records `completed case lacks owner durability receipts`. The four standalone
floor jobs carry `owner_count: 1` to select their host, but start no storage
owner and correctly have no owner durability receipt. The final reporter
mistook this host setting for a requirement to run a storage owner.
`cloud_campaign.summarize_case` now excludes standalone floors from that
requirement; workload admission is unchanged. The existing real floor-receipt
test now covers these jobs. Ten focused report/floor tests passed. Local
strict regeneration admits **66 of 66** cases. The original receipt, failure
record and archive are preserved; this is not a successful cloud exit
retroactively assigned to the run.

**Reproducible inputs**, in the parent workspace:

- `checkpoint-work/focused-launch-20261112-nvmeofpath3/results-top/`;
  controller archive SHA-256
  `2311d46414661099b1245640c5ca395791dd7e42e49a2b4df7d500d15da96a53`,
  108,105,214 bytes, verified against the original receipt.
- Binary SHA-256
  `5433ccf1828b050d117904dc90169cbe180bbc07a33f0a3067d290fa52a9e1d2`;
  sealed source tree
  `1473ca501136562464acbe8c929bf20fac258c3438ed5fb05c543ab25bcc776e`;
  matrix `f286ce6c7255d21f98c9cef8b334b3d6540b1cc84005bc265e0f39751225ff55`.
  Only the local report admission changed after the run, not the tested binary.
- `analysis/output/nvmeof_path.md` and its five per-case CSV files;
  `latency-audit.csv` adds offered/successful counts, response and scheduling
  p99, and maximum API latency. `resources.csv` retains the 105 process-window
  records. `architecture-comparison.csv` retains 30 per-repetition historical
  comparisons and their contract differences. `evidence.json` records source
  hashes, counts, floor evidence, live counters and slow-call clusters.
- Regenerate the additional audit with
  `analysis/analyze-results.py /path/to/parent/workspace`; it uses the locally
  regenerated strict report and CSV files. `write-timeline-summary.json` is
  generated by `experiments/kv-comparison/analyze_write_timeline.py`.
- Previous inputs: run 11 `leasedab2/analysis/output/leased_{writes,reads}.csv`;
  run 10 `docs/research/supervisor-brief-2026-11-10/rwc10/data/cold_fabric.csv`;
  run 7 `run7-sustained-extract.csv` in the same supervisor brief.

**Comparison limits.** Runs 10, 11 and 12 used different fleets and binaries
on the same two-VM machine class. Run 11 used `on_disk` history, research WAL
geometry and `on_demand` export; run 12 uses `memory_only`, native WAL geometry
and `per_owner` export. Warm read populations grew from 4,096 to 65,536 keys.
Run-10 cold NVMe reads were sealed and direct; run-12 reads are live and
buffered, with device evidence. These are historical architecture comparisons,
not isolated optimization A/B tests. Small changes are not separated from
fleet spread. Ranges below retain the two repetitions; they are not confidence
intervals. Most new queue/cache cells have one repetition. Tail quantiles of
small populations are descriptive.

#### Puts: fewer small-value stalls, not native speed

Page-cache acknowledgement, RF1, one owner and one SSD; no SSD durability is
included. Each row uses 4,096 measured scalar puts per repetition.

| Route | Size | Writers | Run 11 p50 µs | Run 12 p50 µs | Run 11 p99 µs | Run 12 p99 µs |
|---|---|---:|---:|---:|---:|---:|
| Owner RPC | 1 KiB | 1 | 63.9–64.8 | 58.4–59.2 | 107.0–108.3 | 91.5–97.3 |
| Leased NVMe-oF | 1 KiB | 1 | 62.5–62.9 | 54.9–56.6 | 110.2–115.7 | 91.9–96.4 |
| Owner RPC | 1 KiB | 16 | 334.1–335.1 | 402.3 | 41,881–48,498 | 1,266.1 |
| Leased NVMe-oF | 1 KiB | 16 | 322.9–326.0 | 330.0 | 2,530–2,740 | 462.8 |
| Owner RPC | 64 KiB | 1 | 264.0–268.4 | 233.2–239.3 | 413.0–421.5 | 362.5–411.3 |
| Leased NVMe-oF | 64 KiB | 1 | 183.7–187.6 | 164.1–164.6 | 291.4–337.2 | 206.3–219.5 |
| Owner RPC | 64 KiB | 16 | 3,485–3,496 | 3,088.9 | 4,436–4,543 | 35,660.8 |
| Leased NVMe-oF | 64 KiB | 16 | 2,163–2,178 | 1,893.4 | 4,254–5,263 | 17,281.3 |

- The short 1 KiB page-cache cases now have **zero calls above 5 ms**, on
  both routes and at both caller counts. Run 11 had three such calls per
  one-writer case, 80 with 16 owner-RPC writers, and 32 with 16 leased writers.
  The two new history diagnostic copies report zero spills and zero waits.
  This removes the observed short-case history stall, not every long stall.
- The 16-writer owner-RPC p50 is worse, despite much better tails and rate.
  At 64 KiB the p99 regresses on both routes with 16 writers. With one writer,
  owner RPC has four calls above 5 ms in each repetition; leased has six.
  Leased p99.9 is 10.25–10.40 ms, versus 1.46–1.52 ms in run 11.
  These regressions remain unexplained by the collected stage evidence.
- Synchronous, one writer: owner RPC p50 is 98.4 µs at 1 KiB and 397.4 µs at
  64 KiB; leased is 143.2 and 287.1 µs. RPC acknowledges the synchronized owner
  root; leased acknowledges FUA data, with asynchronous root adoption. These
  are not identical recovery contracts. There are two leased calls above
  5 ms at 64 KiB. No crash/power-loss or `memory_only` recovery claim follows.
- Same-fleet in-process native page-cache p50 is 1.95–2.03 µs at 1 KiB and
  37.25–37.55 µs at 64 KiB, one writer. Leased does **not** reach native put
  latency. The old reserved live path was 418–427 µs at 1 KiB in run 11;
  most of that improvement had already occurred with leased mode in run 11.

Rates below include the load generator. Parentheses give mean API calls in
flight per run, not configured concurrency. Do not interpret them as capacity.

| Page-cache case | Run 11 calls/s (in flight) | Run 12 calls/s (in flight) |
|---|---:|---:|
| RPC 1 KiB, c1 | 8,349–8,636 (0.90–0.91) | 13,275–13,598 (0.86–0.87) |
| Leased 1 KiB, c1 | 7,951–8,011 (0.91) | 13,612–13,981 (0.86) |
| RPC 1 KiB, c16 | 13,309–13,407 (15.50–15.52) | 35,859 (14.68) |
| Leased 1 KiB, c16 | 16,026–16,852 (15.30–15.42) | 45,257 (14.45) |
| RPC 64 KiB, c16 | 4,373–4,441 (15.65–15.66) | 4,146 (15.71) |
| Leased 64 KiB, c16 | 5,592–5,982 (15.54–15.55) | 6,580 (15.54) |

The native small-put and memory-read rate rows have mean in-flight calls far
below half the configured concurrency. They are load-generator bound; their
rates do not establish native engine capacity.

#### Reads: polling helps; a remote cache is not local RAM

All warm target rows below have zero device reads. Coordinator cache is
disabled except in the separately named Zipf case.

| Live NVMe-oF case | n | p50 µs | p99 µs | p99.9 µs |
|---|---:|---:|---:|---:|
| Warm 1 KiB, c1, 1 × 32 | 8,192 | 28.8 | 34.7 | 39.1 |
| Warm 1 KiB, c1, poll queue | 8,192 | 22.2 | 27.2 | 31.4 |
| Warm 64 KiB, c1, 1 × 32 | 8,192 | 47.3 | 57.8 | 62.4 |
| Warm 64 KiB, c1, poll queue | 8,192 | 41.8 | 50.7 | 56.1 |
| Warm 1 KiB, c16, 4 lanes, 1 × 32 | 8,192 | 98.9 | 122.4 | 256.4 |
| Warm 1 KiB, c16, 4 lanes, 4 × 128 | 8,192 | 74.7 | 253.9 | 337.5 |
| Cold 1 KiB, c1, inline | 2,048 per rep | 104.6–105.4 | 121.3–154.1 | 398.4–486.6 |
| Cold 1 KiB, c1, lane | 2,048 | 117.8 | 133.3 | 402.5 |
| Cold 1 KiB, c1, poll queue | 2,048 | 95.7 | 156.9 | 499.9 |
| Cold 64 KiB, c1, inline | 4,096 per rep | 141.9–144.3 | 163.2–216.8 | 196.7–362.9 |
| Cold 64 KiB, c16, 4 lanes, 1 × 32 | 4,096 | 259.7 | 661.9 | 7,618.6 |
| Cold 64 KiB, c16, 4 lanes, 4 × 128 | 4,096 | 236.7 | 534.3 | 1,334.2 |
| Cold 64 KiB, c16, 8 lanes, 4 × 128 | 4,096 | 232.9 | 382.2 | 605.0 |

- Same-fleet polling lowers warm p50 by 23% at 1 KiB and 12% at 64 KiB.
  Cold 1 KiB p50 falls by 8.6%, but p99/p99.9 do not improve. A wide queue
  lowers warm c16 p50 by 24%, but doubles p99. Keep default geometry unchanged;
  this is not a universal queue-setting win.
- Run-11 warm inline p50 was 30.6–31.0 µs (1 KiB c1), 49.9–51.0 µs
  (64 KiB c1), and 101.5–102.4 µs (1 KiB c16). The new default-route
  differences are small, across different fleets and data sets.
- Same-fleet native warm p50 is 1.02 µs at 1 KiB and 11.49 µs at 64 KiB.
  Even the polled remote page-cache result is not a general 5 µs read.
- The 8 MiB coordinator-cache/Zipf case has 4,710 hits and 3,482 misses:
  57.5% hits. Its p50 is 1.24 µs, but mean is 14.31 µs and p99 is 37.29 µs.
  It is a mixed cache population, not 1 µs remote storage.

Cold comparison to run 10, historical context only:

| Size, callers | Run 10 sealed/direct p50 µs | Run 12 live/buffered default p50 µs |
|---|---:|---:|
| 1 KiB, c1 | 109.7–110.0 | 104.6–105.4 |
| 64 KiB, c1 | 140.4–141.3 | 141.9–144.3 |
| 1 KiB, c16 | 121.3–126.5 | 129.2 |
| 64 KiB, c16 | 236.1–236.2 | 259.7 |

There is no broad cold-latency improvement over that architecture. Wider
queues recover the 64 KiB concurrent result, on one repetition. The new
permanent export removes control traffic and file-count limits; it does not
remove SSD latency.

**Cold qualification and native read pattern.** All 15 cold live cases
(G18R, G18X and batch read) qualified. Scalar c1 reads use about 0.987–0.988
device reads per API read at 1 KiB, about 4 KiB per device read; at 64 KiB,
0.972–0.973 and about 69.7 KiB. The three native cold 1 KiB cases and native
cold batch case are unqualified (ratios 0.269–0.645 and 0.515); their small
p50 values are not SSD latency. Native cold 64 KiB qualifies, but uses about
3.72–3.77 device reads per call, about 118–120 KiB per device read. Its
one-reader p50 is 96.9–97.4 µs, mean 151.6–153.0 µs and p99 934.9–937.4 µs.
Live has higher p50 but lower mean and p99. Device populations differ; do not
subtract these times to infer network overhead or physical device time.

#### File count, batching, stages and resource cost

- G18X has **18 WAL files**, including frame/extent overhead, not 17.
  One-reader p50/p99 is 141.3/164.6 µs; 16-reader 4 × 128 is
  232.6/1,302.4 µs, p99.9 11.58 ms. Grant misses are 18 and 72, but owner
  metadata requests are only 1 and 4, and controller connects are zero in
  measurement. A grant miss is now a local namespace lookup, not a new
  controller. The many-file path ran; its concurrent tail is still worse.
- All 34 live cases have zero measured controller connects, payload RPCs,
  live errors and unknown writes. Read metadata requests are at most four,
  including the eight-lane case. All 43 remote owner receipts report
  `memory_only`. The existing gate validates leased/FUA and error counters.
- Batches of 32 puts, 1 KiB values, c1, page-cache acknowledgement:
  native p50/p99 34.6/44.3 µs; RPC 237.1/333.0 µs; leased 179.6/269.9 µs
  (1,024 API calls each). Cold `get_many` of 32 × 8 KiB, c1:
  live 434.9/649.9 µs, p99.9 2.53 ms, 256 calls; 0.941 device reads per
  successful key. Native 120.4/14,536.5 µs is partly cached, not a matched
  SSD reference.
- Hardware floors: QD1 direct SSD p50/p99 is 72.2/83.5 µs at 4 KiB
  (83,431 reads) and 84.5/100.9 µs at 64 KiB (69,460 reads).
  `ib_read_lat` round-trip p50/p99 is 7.41/8.08 µs at 1 KiB polled,
  12.31/13.97 µs in event mode; 5,000 iterations per size.
  This reads registered RAM, not a file or KV object. Floors are not
  additive components of the API distributions.
- The write-profile copy has 4,098 completed NVMe writes including markers,
  mean leased submission-to-completion 27.3 µs, and 4,012 asynchronous
  adoption samples averaging 68.7 µs. They overlap. The timeline copy joins
  all 4,096 calls without dropped calls, incomplete calls or owner errors:
  preparation p50/p99 9.80/25.13 µs, live I/O 28.88/36.67 µs,
  publication 3.70/8.78 µs. These are per-stage quantiles of an instrumented
  case, not additive quantiles or a decomposition of the unprofiled stalls.
- No cgroup CPU throttling was observed. Warm c1 client CPU in its gate
  interval rises from 0.075 to 0.115 cores with polling at 1 KiB, and from
  0.134 to 0.240 cores at 64 KiB. These averages include gate time; they are
  not per-call CPU or a promise that polling costs no core under load.
  Peak owner cgroup memory is about 840 MiB for the warm 1 KiB data set and
  4.94 GiB for 64 KiB; the peak includes preload/page cache, not just RSS.
  Client CPU evidence remains usable where cgroup I/O attribution is
  explicitly unavailable (34 process windows).
- Whole-device write windows reach about 256 times the payload in a short
  put case. The window includes unrelated/background writes and exceeds the
  API interval. This is not engine write amplification. [INFERENCE] Default
  ext4 lazy initialization remains a possible contributor; it was not isolated.

#### Important remaining defect: sustained stalls

`G18W-r0-396a77f1f3129f0d721c`, leased page-cache, 1 KiB, c16, 200,000
successful puts, one repetition:

- p50 **561.9 µs**, p99 **750.8 µs**, p99.9 **102.3 ms**, maximum **143.6 ms**.
- 1,504 calls above 5 ms hold **48.6% of summed API time**. They occur in
  94 consecutive raw-sample clusters of 16 calls each.
- Whole-case rate 14,812 calls/s (15.43 mean in flight), versus 45,257
  (14.45) in the short case. The 13 complete one-second completion buckets
  average 14,949 calls/s. The first is 28,032; the last five average
  **11,190 calls/s**. The partial final bucket is excluded.
- History-full settles are zero. The one measured extent switch totals only
  0.178 ms; 96 adoption settles total 5.93 ms. These do not explain the
  slow-call population. The sustained case has no same-call checkpoint
  profile; the cause is **not established**.
- Historical context: run 7's owner-verbs page-cache case, three
  repetitions, had p99.9 115.8–122.1 ms and late rates 10,314–10,406 puts/s.
  Different route, history, binary, fleet and geometry prevent causal
  attribution. The long-run stall pattern is not eliminated.

**Decision.** The hardware-only paths now execute: permanent export, connected
subsystem namespace addition, polled reads, wide queues, eviction and cold live
reads. Native latency parity, sustained-put stability and production recovery
are not established. The extra controller inventory still uses the old NQN
discovery convention, so it does not archive a complete independent inventory
of the new permanent controllers. No owner crash, connection-loss or
power-loss scenario ran. `memory_only` remains a benchmark-only mode, unlike
the recoverable WAL/snapshot model of paper sections 3.3–3.4. Sui checkpoint
replay is not part of these synthetic KV results. No default queue/export
setting changes and no further run are authorized by this report.


## Recovery and stall fixes (`nvmeof_path_fix`, run 13)

Goal: confirm on two VMs the changes made after run 12: owner history `wal_replay` (default; restart by WAL replay as in the Tidehunter paper, sections 3.3 and 3.4), lease-ahead for leased puts, polled writes, and the attribution of calls above 5 ms. Design text: `experiments/nvmeof/LIVE_WAL.md`.

The selection has 37 cases, one coordinator and one owner. Matrix SHA-256 `aa94a3842d196bd9cdb486b0c746e9939455b25d4d152e8127c1aba51f9ef606`. Each case is a copy of a case of an earlier selection with the overrides below. Every remote case uses `history_persistence: wal_replay` and every live case uses `live_lease_ahead: true`, `live_polled_writes: false`, unless the table says otherwise.

| Group | Cases | Content |
|---|---:|---|
| G19W | 21 | Leased puts, native tier: 64 KiB with 16 writers (2 repetitions; 2 more with lease-ahead off; 1 with `memory_only`), 1 KiB with 1 and 16 writers (2 repetitions each), 64 KiB with 1 writer. Leased put, synchronous tier, 1 KiB. Owner RPC puts: native 1 KiB (2 repetitions), native 64 KiB with 16 writers, synchronous 1 KiB. Sustained 200,000 puts with `wal_replay` and with `on_disk`. Write-timeline copies at 1 KiB with 1 writer and at 64 KiB with 16 writers. Polled writes at 1 KiB with 1 writer and at 64 KiB with 16 writers |
| G19R | 7 | Live cold reads: 1 KiB and 64 KiB with 1 reader, 1 KiB with 16 readers. Live warm reads with and without a poll queue. Owner RPC cold and warm reads at 1 KiB (first owner RPC reads with `wal_replay`: the owner decodes the reference from WAL bytes) |
| G19N | 8 | `native` reference on the coordinator VM: page-cache puts at 1 and 64 KiB with 1 and 16 writers; memory and cold reads at 1 KiB |
| G19X | 1 | Read over about 17 WAL files, 1 reader |

The first 8 cases are canaries: native put, leased 64 KiB put with 16 writers, owner RPC put, live cold 64 KiB read (the run stops when it is not device-served), owner RPC cold read, live warm read, polled write, sustained puts.

New evidence per case: owner lease and adoption profiles (`lease_reserve_ns`, `lease_namespace_sync_ns`, `lease_publish_ns`, `adopt_owner_ns`, `adopt_close_scan_ns`), `extent_preleases`, `extent_switch_waits`, and `stalls.csv` (each call above 5 ms labelled `extent_switch`, `checkpoint:<stage>` or `unattributed`; only for cases with a write timeline).

Not possible on the workstation: the publication of a pre-leased extent on the kernel target, polled writes on `nvme-rdma`, `ReplayLiveWal`, and `fdatasync` of a WAL file under concurrent NVMe-oF writes.

### Launch history (12 November 2026, run 13)

| Launch | Stopped at | Cause | Fix |
|---|---|---|---|
| Revision `4c7b0ff9`, fleet `bench-5fcb8a94c8184574` | Case 2 of 37 (canary rule), about 9 minutes, at most `$0.89` of compute (estimate) | Harness defect: `observability/campaign_exporter.py` rejected the 10 new live counters (`incomplete live NVMe-oF measurement counters`). The case had completed its 4,096 puts | Counters admitted. The two extent-switch stages admitted in `analyze.admit_write_timeline` (same class of defect, found by review). Commit `7b276de1` |
| Revision `7b276de1` | Not launched | Review before the launch: `kv-research export-nvmeof-dataset` and `nvmeof-check` listed a sealed owner log by receipt ordinal. `wal_replay` keeps no stable ordinal after an owner restart | Both list by WAL offset in `wal_replay`. Commit `3d5c0f79` |
| Revision `3d5c0f79`, binary `437c767d...`, fleet `bench-c1dce8770c684804`, 18:43:56 to 19:08:12 UTC | Completed: 37 of 37 cases, 551,552 measured calls, none unsuccessful. At most `$2.63` of compute (estimate) | — | — |

Instances, disks and the placement policy of each fleet were verified absent. Outputs: `checkpoint-work/focused-launch-20261112-nvmeoffix4/analysis/output/`.

### Run 13 results

One fleet, one or two repetitions for each cell. Run 12 used another fleet and binary: a comparison with run 12 is context, not a controlled test. The controlled comparisons are the arms inside run 13. All times are in µs unless a unit is given.

#### Sustained puts: the stalls of run 12 are gone with `wal_replay`

200,000 leased puts, 1 KiB, 16 writers, native tier.

| Arm | p50 | p99 | p99.9 | Puts/s | Calls above 5 ms | Synchronous adoptions |
|---|---:|---:|---:|---:|---:|---:|
| `wal_replay` (run 13) | 155.7 | 417.3 | 1,371.8 | 82,338 | 16 | 2 |
| `on_disk` (run 13, same fleet) | 517.4 | 704.5 | 174,632 | 9,531 | 1,824 | 99 |
| `memory_only` (run 12, other fleet) | 561.9 | 750.8 | 102,323 | 14,812 | 1,504 | not recorded |

With `on_disk` the owner spent 14.8 s in adoption exchanges (5,019 exchanges); with `wal_replay` 0.84 s (11,147 exchanges). The 16 slow calls of the `wal_replay` arm are one group at the first measured put (see "Remaining pause").

#### Leased 64 KiB puts with 16 writers

| Arm | Repetitions | p50 | p99 | p99.9 | Puts/s | Calls above 5 ms | Time of 2 extent switches |
|---|---:|---:|---:|---:|---:|---:|---:|
| `wal_replay`, lease-ahead on | 2 | 1,747 to 1,752 | 3,945 to 3,983 | 31,236 to 31,424 | 7,899 to 7,996 | 32 | 0.09 ms |
| `wal_replay`, lease-ahead off | 2 | 1,750 to 1,766 | 3,909 to 4,474 | 34,779 to 36,363 | 7,593 to 7,617 | 32 | 35.1 to 36.6 ms |
| `memory_only`, lease-ahead on | 1 | 1,889 | 16,933 | 31,313 | 6,825 | 80 | 0.09 ms |
| `wal_replay`, lease-ahead on, polled writes | 1 | 1,752 | 2,835 | 32,249 | 7,870 | 32 | 0.11 ms |
| `wal_replay`, lease-ahead on, write timeline | 1 | 1,788 | 2,883 | 30,381 | 7,884 | 32 | 0.10 ms |
| Owner RPC, `wal_replay` | 1 | 2,974 | 35,156 | 37,464 | 4,494 | 64 | — |
| `native`, page cache, coordinator VM | 2 | 155 to 161 | 258 to 308 | 377 to 469 | 55,009 to 55,958 | 0 | — |

- Lease-ahead removes the switch time from the put path (about 18 ms for each switch without it; `extent_switch_waits` is 0 in every case). Two switches touch 32 of 4,096 calls, less than 1%, so p99 does not change. Throughput is 4% to 5% higher and p99.9 is 3 to 5 ms lower.
- The p99 of 17.28 ms in run 12 came mainly from `memory_only`, not from the switch: the `memory_only` arm of run 13 repeats it (16.9 ms, 80 slow calls, 6 synchronous adoptions) with lease-ahead on.
- p99 with polled writes (2,835) is equal to p99 of the timeline copy without polled writes (2,883). No effect of polled writes at 64 KiB is shown.
- The leased route is 11 times the `native` p50 at this size and concurrency.

#### Small puts

| Cell (native tier, 1 KiB) | p50 | p99 | p99.9 | Puts/s | Run 12 p50 |
|---|---:|---:|---:|---:|---:|
| Leased, 1 writer (2 repetitions) | 46.6 to 46.9 | 82.2 to 85.0 | 380 to 575 | 14,870 to 15,192 | 54.9 to 56.6 |
| Leased, 1 writer, polled writes | 39.6 | 72.5 | 605 | 16,913 | no cell |
| Owner RPC, 1 writer (2 repetitions) | 50.0 to 50.1 | 87.6 to 90.3 | 480 to 696 | 15,998 to 16,043 | 58.4 to 59 |
| Leased, 16 writers, 4,096 puts (2 repetitions) | 126.7 to 129.2 | 596 to 692 | 32,984 to 33,909 | 52,880 to 53,013 | 330 |
| Leased, 1 writer, synchronous tier | 137.5 | 179.0 | 466 | 6,245 | 143.2 |
| Owner RPC, 1 writer, synchronous tier | 90.5 | 119.0 | 392 | 9,676 | 98.4 |
| `native`, page cache, 1 writer | 1.9 | 3.3 to 3.4 | 7.5 to 7.8 | 96,420 to 98,563 | 1.9 to 2.0 |

Polled writes lower the leased p50 by 15% at 1 writer (39.6 against 46.6 to 46.9, same fleet, one repetition). The leased route stays 20 to 25 times the `native` p50.

Regression: the short case with 16 writers has p99.9 of 33 ms (run 12: 1.6 ms) because of the pause below.

#### Remaining pause: first put after a close

Each leased put case of run 13 has one group of slow calls at the first measured put (31 to 33 ms; one call for each writer). Run 12 did not have it (first measured put 0.28 ms in the cell with 1 KiB and 1 writer). Evidence: `leased_skip_markers` is 2 in run 13 and 1 in run 12 for that cell; the owner profile shows 2 leases at the start (initial lease and pre-lease), 15 to 24 ms each (`lease_reserve_ns`). The reserve cost is not new (`reserve_external`; 17.6 ms for each lease in run 12). New is one more close of the extent before the first measured put. Probable cause (not confirmed): a control command that `wal_replay` sends at the measurement gate; each control command closes the leased extent (`remote_db.rs`, `Command::Control`). Candidate fix: do not close the extent for control commands that do not need it, or pre-lease at close.

`stalls.csv` (the two write-timeline cases): 64 KiB with 16 writers has 16 calls without label (the first group, 486 ms summed) and 16 calls that overlap `checkpoint:control_enqueue` (147 ms summed). 1 KiB with 1 writer has 2 calls without label. No call has the label `checkpoint:capture_start`, `checkpoint:freeze` or `checkpoint:live_install_start`, so the step-size rule of the plan does not apply. The sustained case has no write timeline; its 16 slow calls are the first group.

#### Reads

| Cell (1 reader unless stated) | p50 | p99 | p99.9 | Device reads for each read | Run 12 p50 |
|---|---:|---:|---:|---:|---:|
| Live cold, 1 KiB | 105.1 | 120.1 | 271 | 0.988 | 104.6 |
| Live cold, 64 KiB | 145.3 | 220.4 | 370 | 0.972 | 141.3 |
| Live cold, 1 KiB, 16 readers (99,850 reads/s) | 127.9 | 197.5 | 2,991 | 0.955 | 129.2 |
| Live cold, 64 KiB, 18 WAL files | 143.6 | 167.7 | 1,076 | 0.979 | 141.3 (1 file) |
| Live warm | 28.9 | 35.0 | 37.9 | 0 | 28 |
| Live warm, poll queue | 22.2 | 27.6 | 30.8 | 0 | 22.2 |
| Owner RPC cold, 1 KiB, sealed dataset | 103.3 | 156.3 | 205 | 0.948 | no cell |
| Owner RPC warm | 27.4 | 33.2 | 36.4 | — | no cell |
| `native` memory read | 1.2 | 1.7 | 7.2 | — | 1.0 |

Reads did not change against run 12, as expected: `wal_replay` changes owner history, not the read path. The owner RPC read with `wal_replay` (the owner decodes the reference from WAL bytes) has the same p50 as the live read.

#### First hardware execution, all passed

Publication of a pre-leased extent on the kernel target; lease-ahead over 18 WAL files (G19X); polled writes on `nvme-rdma`; `CheckpointPosition` under leased writes; owner RPC read from WAL bytes; sealed-dataset export by WAL offset; owner restart with data; coordinator reopen with `ReplayWal`.

Not assessed: owner or coordinator crash on hardware, power loss, connection loss, `ReplayLiveWal` after a coordinator loss with an open extent, more than one owner.
