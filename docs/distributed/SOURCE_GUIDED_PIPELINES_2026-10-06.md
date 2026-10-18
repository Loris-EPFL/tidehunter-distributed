# Source-guided preparation, persistence and scan pipelines

Date: 6 October 2026. This is the current implementation record for the next slice
after [the pipeline/cache campaign](PIPELINE_AND_READ_CACHE_2026-10-06.md).
**Implemented and locally validated.** The integrated library suite passes
**532 tests, seven ignored**, including 25 remote tests. The 252-case matched
campaign and separate nine-case anomaly confirmation pass all full-byte/reopen
audits, with zero swap. Larger-window preparation and multipage scans improve;
overall write improvement across all shapes is not established, and native parity
is not achieved. Measurements in §1 are the frozen previous implementation;
current results, including regressions, are in §7 and the
[complete measurements](SOURCE_GUIDED_MEASUREMENTS_2026-10-06.md).
No AWS resources, RDMA, NVMe-oF or new replication protocol are part of this slice.

## 1. Problem selected from measurements

Previous source tree:
`c56f5c929bee8feeef0def74c4c07650fba3c661d74c4604d5e3d4d273c38fbe`;
source/binary binding is in
[`candidate-direct/source-manifest.json`](../../logs/pipeline-cache-2026-10-06/candidate-direct/source-manifest.json).
The baseline is the current pipeline architecture, not an older middleware run.

For 8 KiB values / batch 32 / window 32, pipeline enabled and application value
cache disabled, the previous median distributed load was **85.358 ms**, versus
**40.555 ms native**. Preparation/admission recorded **53.325 ms**, blocking storage
wait **27.588 ms**, and publication **4.749 ms**. Independently calculated stage
medians do not sum exactly. Both old and pipelined runs issued **48 durable groups
and 48 file persistence barriers**; native used four explicit window barriers.

Previous separate cache-cleared 8 KiB scalar diagnostics measured **38.974 µs**
whole-call mean, **26.881 µs** exchange, **9.175 µs** member verification and
**1.971 µs** server handler, with the handler inside exchange. Inventory mutex wait
was only **0.0373 µs**. These are nested timings, not additive costs or a direct
measurement of network transit. A scan fetched one 256-reference page at a time.

The selected mechanisms address serial preparation, redundant namespace syncing
and known scan dependencies. They do not promise that a single dependent remote
read will become as fast as a native memory-resident read. More SSDs alone do not
remove any of those software dependencies.

## 2. One complete source review per independent agent

| Dedicated review | Entire primary reading | Exact source identity | Implementation ownership |
| --- | --- | --- | --- |
| [TiKV/TiDB](TIKV_SOURCE_REVIEW_2026-10-06.md) | TiDB paper pp3072–3084 and complete pertinent Raft Engine/TiKV source files | Raft Engine `097c499a19fbb38754c73aa2f31532329df7c0c6`; TiKV `c61d92c26a4d96a575386f5e32179550556e2c29` | Storage receipt/persistence amortization and recovery tests |
| [DiskANN](DISKANN_SOURCE_REVIEW_2026-10-06.md) | All 11 paper pages including algorithms, evaluation, figures and references, plus pertinent current Rust reader sources | Microsoft/DiskANN `bab8136b52ce47b484eb5b74fbb328e43b144061` | Bounded scan lookahead and exact-read tests |
| [DistributedANN](DISTRIBUTEDANN_SOURCE_REVIEW_2026-10-06.md) | All 8 paper pages, algorithms, figures, evaluation, limitations and references | PDF SHA-256 `345ef211697d3edc4d5cfafea2ecdf62025d3c07fee55489e489cd6120c6260a` | Design review, source mapping and matched benchmark wiring |

The parent implementation adds bounded parallel preparation and integrates the
three paths. Each review rechecks the relevant Tidehunter paper/native mechanisms.
“Complete documentation” here means the complete named paper plus the complete
implementation files needed for the adapted mechanism; it is not a claim to have
read every file or wiki page in TiKV or DiskANN.

## 3. Exact source-to-code map

| Source and exact location | Problem in this fork | Adaptation in local code | Preserved rule / verification |
| --- | --- | --- | --- |
| TiDB paper §4.1.1 p3075; Raft Engine pinned `src/engine.rs`, `Engine::write` and its `finish_populate` before `WriteBarrier` | Caller serializes complete-batch preparation before each next admission | `remote_db/preparation.rs`, `RemoteDatabase::submit_many`: bounded CPU preparation followed by deterministic admission | The original codec bytes/digests remain the authority. Invalid windows must not allocate a live gap; exact retries keep original identity. |
| DistributedANN §2.3 p3, Algorithm 1; §4.1 p5 head-index bottleneck | More storage nodes leave one coordinator doing all preparation | Bounded preparation pool with explicit worker count and byte/count budgets | This is a scheduling inference, not literal near-data scoring. Workers cannot publish, allocate a new authority epoch or return incomplete values. |
| Raft Engine pinned `Engine::write` and `src/write_barrier.rs`: prepare, grouped append, durability, completion | Revalidating/persisting every receipt separately can repeat lock and namespace work for one durable group | `storage.rs::PartitionLog::persist_many`/`persist_receipts`; TCP and local worker callers use exact grouped receipts | Every receipt is validated before persistence state advances. The same native WAL bytes and complete-group recovery remain. |
| TiKV pinned `async_io/write.rs`, `WriteTaskBatch::add_write_task` lines609–673 and `Worker::write_to_db` lines1109–1271 / persisted ready notification | Allocation, append completion and publication can be confused when adding concurrency | Existing `PublicationTracker`, exact durable receipts and `drain` remain the ordered authority | Later durable work cannot pass an earlier missing outcome; no acknowledgement is issued at mere preparation completion. |
| Raft Engine pinned `src/file_pipe_log/pipe.rs::rotate_imp` lines249–282 and `::sync` lines398–404: new-file namespace sync versus steady file sync | Reopening/syncing an already-persisted stream namespace for every group is redundant | `storage.rs::persist_receipts` tracks the exact `WalFileId` whose namespace sync succeeded; experimental-only helpers in `wal/mod.rs` separate file and namespace persistence | A new file or reopened process starts a new namespace-sync obligation. Failed sync does not mark it complete. Every group still performs file sync. |
| DiskANN §3.3 and §4.2; pinned Rust reader's bounded read batches | `RemoteDatabase::scan` waits for page N to finish before preparing/dispatching N+1 | `remote_db/read_pipeline.rs`, bounded one/two-page lookahead using existing node workers | Fixed published read cut, exact output order, no extra cache fill; missing/corrupt/stale-epoch replies fail the scan. |
| DistributedANN §2.4 pp3–4, Algorithm 2 | Coordinator serialization can make independently known requests dependent | Same bounded scan scheduling; coordinator retains only ordered assembly/verification duties | ANN early termination, pruning and partial result acceptance are explicitly not adopted. |
| Tidehunter §§3.1–3.4 and §4.4 | Optimization can break complete batches, address lifetimes or frontiers | Existing canonical codec, native Value WAL and publication/replay interfaces retained | Values stay in the WAL; logical order is not physical address or durability; current retained-history policy remains. |

Function names and tests in the implementation inventory below control the exact
mapping. The dedicated reviews pin source URLs and explain why each rejected
mechanism is inapplicable. Similarity of scheduling mechanisms is not evidence of
matching the source projects' benchmark results.

## 4. Architecture and code boundaries

```text
bounded caller window
        │
        ├── validate input/identities and reserve preparation capacity
        │
        ├── parallel independent canonical batch preparation
        │
        └── ordered admission and existing per-node dispatch
                           │
               independent complete Value WAL groups
                           │
               file durability + exact grouped receipts
                           │
               contiguous ordered publication
```

The global authority keeps its small ordering responsibility. CPU preparation and
independent node work can overlap or proceed concurrently without turning global
sequence allocation into a networked atomic operation. A single logical sequence
still does not establish durability or visible state.

### 4.1 Bounded parallel preparation

- API: `submit_many(&[([u8; 16], Vec<Mutation>)])` is a synchronous borrowed-window
  admission API. Caller input does not escape into a background queue.
- `preparation_workers` is bounded to 1–8; one worker is a serial control. Lone
  writes keep the existing scalar path to avoid unnecessary scheduling overhead.
- The complete codec remains unchanged. Independent canonical preparation can
  finish out of order; admission stays in deterministic original order.
- Count and byte limits cover the preparation window before constructing outputs,
  alongside the existing outstanding admission/index/reference budgets.
- Duplicate/retry operation IDs need original version/digest matching. No timeout
  or worker failure is permission to create a replacement logical identity.
- Returned versions mean admission. `drain` still establishes durable ordered
  visibility, and checkpoint/shutdown retain their barrier.

`prepare_wall_ns` measures the preparation call's elapsed preparation interval.
`prepare_task_ns` is the **sum of worker elapsed intervals**, including scheduling
effects; it is neither CPU time nor an additive wall-clock stage. The benchmark
labels it `prepare_task_elapsed_sum_ns` to avoid that mistake.

`validation_ns` includes window preflight and the complete preparation interval;
`prepare_wall_ns` is nested within it. The new scan's `value_read_ns` covers its
prefetch/assembly interval and includes later-page index resolution also counted
in `read_resolve_ns`. These counters cannot all be added to obtain total elapsed
time. The benchmark's `stages.csv` stores counter deltas; deltas of a high-water
counter such as `prepare_reserved_bytes_peak` represent increases in that maximum,
not the absolute peak for each later phase.

Preparing the whole borrowed window before its admission also delays that
window's first new storage dispatch. The previous scalar submit loop could overlap
later preparation with earlier dispatched groups. Therefore the new one-worker
control alone is insufficient: comparison against the frozen previous TCP binary
is essential to detect lost overlap or scheduling overhead.

### 4.2 Safe persistence optimization, not multigroup recovery redesign

The current framing permits only one unpersisted variable-length complete group
per stream. That gate is retained. An interrupted group may prevent replay from
finding subsequent groups reliably; ordinary files and CRCs do not prove that a
later group cannot survive after an earlier torn gap.

This slice amortizes receipt validation/locking and successful namespace syncing.
It **does not** claim fewer file durability barriers, a shared distributed WAL,
concurrent unpersisted groups, or crash-safe coalescing across such groups.
True multigroup persistence needs an explicit physical recovery format/protocol
and torn-gap fault tests. It remains a separate design gate despite TiKV's useful
grouping precedent.

### 4.3 Bounded scan lookahead

`scan_prefetch_pages` selects one page (control) or two pages. The scheduler can
submit the next known reference page before waiting for the current page, allowing
storage/network work to overlap with verified result assembly. It retains a fixed
published read cut and returns exact sorted values. Buffered pages are bounded;
it neither reads the whole database ahead nor fills the point-value cache.

This does not add `io_uring` to a warm mmap read, bypass TLS or integrity checks,
or allow partial scan success. The workload must contain more than one page for
prefetching to have an opportunity; single-page scans are a useful control.

### 4.4 Repository scope

| Repository / component | Change required here |
| --- | --- |
| `tidehunter-sharded-wal/tidehunter/src/distributed/` | Preparation pool, scan scheduler, receipt/persistence work and focused tests |
| `tidehunter-sharded-wal/tidehunter/examples/` | New controls, window API, metrics and exact process validation |
| `tidehunter-sharded-wal/experiments/local-source-guided.py` | Matched previous/current/serial/parallel/prefetch ablations, binaries pinned |
| Native Tidehunter engine / native WAL format | Experimental-only file/namespace persistence helper split in `wal/mod.rs`; ordinary native `Db` algorithm and native format remain unchanged |
| Earlier middleware repository | No change |
| Sui repository | No change; this slice does not claim Sui serving integration |

Preexisting dirty changes outside this slice remain in the worktree; they are not
evidence that this slice edited the native engine. Review current-slice snapshots
and final source manifests, not `git diff` against an old branch alone.

## 5. Deliberately unimplemented ideas

- **ANN approximation and partial failures:** incompatible with exact KV reads.
- **Move all integrity checks to nodes:** does not verify the returned bytes at
  the caller and would change the end-to-end integrity boundary.
- **Global physical WAL or global RDMA counter:** not needed for the selected
  preparation/scan costs; requires a separate protocol and recovery argument.
- **RDMA/NVMe-oF or io_uring:** no local hardware evidence that these are the next
  bottleneck, and none removes serial preparation or publication rules.
- **LSM for values:** does not remove exchange and would add a different value
  movement/compaction design to Tidehunter's direct Value WAL.
- **Recovery checkpoints:** useful for the large reopen gap but outside this
  source-guided scheduling slice; all retained histories still replay.

## 6. Validation and measurement protocol

Correctness gates include preparation ordering/retries/rejection/budgets, receipts
from wrong streams or groups, namespace-sync rollover/failure, stale authority,
corrupt scan records, bounded lookahead, full-byte comparison and process recovery.
The implementation inventory and final source-bound results are recorded below;
no test claim follows from a source analogy.

The runner `experiments/local-source-guided.py` uses a fresh frozen executable and
the previous `candidate-direct` executable. Every TCP case has application value
cache **disabled**. It compares:

1. Previous TCP (previous scalar-window preparation).
2. New serial preparation / one-page scan control.
3. Two preparation workers / one-page scan.
4. One preparation worker / two-page scan.
5. Combined two-worker/two-page design.
6. Previous and current native file-barrier controls.

Default: three rotated/reversed repetitions, 1/8 KiB values, batch 1/32 and windows
1/8/32: **252 cases**, with full-value and reopen checks. `--windows 1,32` yields
168 cases. Host-wide swap deltas, sampled process RSS and remaining process IDs
are retained. Results are specific to five node processes sharing one laptop SSD.

Read phases follow writes and correctness checks, without dropping the OS page
cache. The sampler records process RSS and system swap, not isolated physical
NVMe read/write counters. These are warm software-path comparisons; neither a
fast scan nor a read regression establishes cold-device IOPS scaling. A zero
application value cache is not a zero OS page cache.

```sh
python3 experiments/local-source-guided.py \
  --baseline logs/pipeline-cache-2026-10-06/candidate-direct/sharded_wal_bench \
  --candidate logs/source-guided-2026-10-06/candidate/sharded_wal_bench \
  --output logs/source-guided-2026-10-06/comparison
```

The runner never creates AWS resources. The same benchmark binary accepts runtime
or generated external configuration for a later explicitly managed cluster; that
is wiring, not evidence of independent-NVMe performance or deployment readiness.

### Mechanism-to-test inventory

| Adaptation | Exact test in this checkout | Evidence established |
| --- | --- | --- |
| Deterministic parallel batch preparation | `pipeline_tests.rs::parallel_preparation_preserves_order_retries_and_complete_recovery` | Logical sequence, repeated identity, exact bytes and reopen state remain correct. |
| No allocation gap on invalid preparation | `pipeline_tests.rs::parallel_preparation_rejects_late_conflicts_without_admitting_a_prefix` | A conflict later in the window cannot leave a newly admitted prefix or silently consumed version. |
| Bounded preparation and uncertain storage | `pipeline_tests.rs::preparation_budget_and_lost_reply_keep_the_existing_failure_contract` | Byte/count limits reject before effects; a lost durable reply retains the recovery/exact-retry contract. |
| TiKV-style grouped receipt handling | `storage/tests.rs::bulk_persistence_validates_every_receipt_before_io_and_keeps_exact_order` | All receipts, including foreign generations and duplicates, are checked before I/O; old receipts cannot promote new work. |
| Shared durability before any receipt is returned | `storage/tests.rs::bulk_receipts_are_not_returned_before_the_shared_barrier` | A causally held persistence gate prevents durable replies and subsequent append. |
| File identity scoped namespace proof | `storage/tests.rs::namespace_sync_is_amortized_only_for_the_exact_wal_file` | File rollover creates a new namespace obligation while each group retains its file barrier. |
| Namespace sync failure | `storage/tests.rs::failed_new_file_namespace_sync_never_acknowledges_or_advances_proof` | Failure after file sync remains unknown and cannot advance durable frontier/proof. |
| DiskANN-inspired bounded frontier | `read_pipeline/tests.rs::byte_budget_bounds_page_and_oversized_record_is_alone` | Nominal page bound, legal singleton and impossible transport record handling. |
| Exact ordering under concurrent read work | `read_pipeline/tests.rs::later_page_node_completes_while_first_node_is_explicitly_stalled` | Later-node progress can happen while an earlier node is blocked; output stays ordered. |
| Fixed published scan cut | `read_pipeline/tests.rs::multi_page_scan_matches_serial_cut_updates_deletes_and_reopen` | 320 exact keys across five mTLS nodes; updates, deletes, checkpoints, pending writes and reopen stay equivalent. |
| No ANN partial-result semantics | `read_pipeline/tests.rs::exact_ordered_row_rejects_mismatched_key_keyspace_and_tombstone` and `observed_scan_fence_failure_blocks_later_cached_reads` | Wrong rows are errors; an observed fence failure cannot be hidden by a later cache hit. |

Exact full-suite and process outcomes are recorded with final source hashes below.
These tests exercise software/file failure models; they are not physical power-cut
certification.

## 7. Final evidence

### 7.1 Source, build and correctness

| Identity | SHA-256 or value |
| --- | --- |
| Branch / HEAD | `feat/sharded-value-wal` / `13a16ca8bd632494fc6368f56d60d0806be4a869`; dirty source captured by manifest |
| Current source tree | `699b7f3f7cee3ba301f3ade86ea541baf4d846494942c896b4525d19fa28c04a` |
| Current benchmark executable | `184f2a747041aaea8ca6a1416f9b529d071aa098edf221fb4d6e12ad9ba1414c` |
| Previous pipeline executable | `0fc431a21430f22b8e05a7b1e0fe5b516a3c03aea45543b4853e2dfab2f9eb04` |
| Compiler | `rustc 1.90.0 (1159e78c4 2025-09-14)` |

The [source/build manifest](../../logs/source-guided-2026-10-06/candidate/source-manifest.json)
captures file hashes before and after an offline, locked release build. Both
campaigns use these exact frozen executables; analysis verifies the build/source
identity and executable hashes. No benchmark source was edited between campaigns.

- [Integrated library suite](../../logs/source-guided-2026-10-06/library.log):
  **532 passed, seven ignored**. The separate preparation/remote suite covers all
  25 remote tests, including the three new preparation tests.
- [Final focused TLS diagnostic test](../../logs/source-guided-2026-10-06/tcp-diagnostics-test.log):
  legacy and detailed statistics responses remain distinct and correctly decoded;
  the new namespace counter travels through the opt-in detailed response.
- [Separate-process correctness campaign](../../logs/source-guided-2026-10-06/process.log)
  and [completion receipt](../../logs/source-guided-2026-10-06/process/COMPLETE):
  crash/reopen, missing/restarted node, uncertain durable response, exact retry,
  fencing and TLS rejection pass.
- [Full matrix audit](../../logs/source-guided-2026-10-06/analysis/audit.json):
  **252 cases**, **1,335,456 raw samples**, **5,148 phase summaries** recomputed;
  all full-byte/reopen checks pass; **900 known storage PIDs exited**.
- Sampled peak aggregate process RSS was **238,148 KiB (232.57 MiB)**; system swap
  deltas were **zero**. RSS includes shared pages once per process and can miss
  peaks. These measurements describe this bounded workload, not a maximum-size
  production database.

### 7.2 Representative large-window result

8 KiB values / batch 32 / window 32, median of three runs. The previous binary is
rerun in the same interleaved campaign; its times differ from the historical §1
campaign. All TCP application value caches are disabled.

| Metric | Previous TCP | New combined TCP | Current native | New vs previous |
| --- | ---: | ---: | ---: | ---: |
| Entire load phase, ms | 93.538 | 84.096 | 40.032 | −10.1% |
| Mean API write window, ms | 23.338 | 20.975 | 9.975 | −10.1% |
| Ordinary point-read mean, µs | 33.418 | 35.529 | 1.940 | +6.3% |
| Ordinary point-read p50, µs | 26.525 | 25.904 | 1.926 | −2.3% |
| Multi-get p50, µs | 313.290 | 325.111 | 53.915 | +3.8% |
| Whole scan, ms | 22.714 | 15.367 | 7.037 | −32.3% |
| Reopen, ms | 243.350 | 260.725 | 1.775 | +7.1% |

Writes still take approximately **2.10× native** and the scan **2.18× native**.
Scalar read mean remains approximately **18.3× native**. The scalar read mean and
p50 move in opposite directions here, so reporting only the improved percentile
would hide variation. This slice does not optimize the scalar exchange itself.

See [all measured API phases and all twelve shapes](SOURCE_GUIDED_MEASUREMENTS_2026-10-06.md),
including checkpoint, exists, missing keys, updates, deletes, index flush and
recovery; and [raw medians/ranges](../../logs/source-guided-2026-10-06/analysis/medians.csv).
These phases exercise their documented interfaces, not equal production feature
coverage: the experimental checkpoint is a retained logical read cut and startup
still replays retained histories.

### 7.3 Which adapted mechanisms helped

| Large-window load stage/counter | Previous TCP | New serial control | New combined |
| --- | ---: | ---: | ---: |
| Preparation/admission (`validation_ns`), ms | 58.316 | 48.287 | 26.045 |
| Canonical preparation wall, ms, nested in preceding row | Not separately recorded | 47.903 | 25.659 |
| Worker elapsed sum, ms; not CPU time | Not separately recorded | 47.876 | 48.856 |
| Blocking storage wait, ms | 29.211 | 53.200 | 49.738 |
| Publication, ms | 5.228 | 4.943 | 5.110 |
| Complete groups / file barriers | 48 / 48 | 48 / 48 | 48 / 48 |
| Explicit namespace syncs | No separate counter | 5 | 5 |

The same-binary two-worker/one-page variant reduces load **107.686→89.035 ms**
versus one-worker/one-page control (**17.3%**). Preparation's wall reduction with
similar worker elapsed sum supports actual parallelism. It does not eliminate the
work, and precomputing the whole window loses some earlier preparation/persistence
overlap: storage wait rises. The archived previous-binary comparison is therefore
the decisive net result, not the larger same-binary ablation percentage alone.

The one-worker/two-page variant reduces scan **21.991→14.905 ms** versus the
one-worker/one-page control (**32.2%**). The causal test also demonstrates later
node progress behind a deliberately stalled earlier page. Extra read concurrency
is useful for this multipage scan; it does not remove scalar RPC latency.

The new namespace counter records five syncs for five current WAL files across
48 groups. The old source synchronized the namespace with each new group's file
barrier, but its executable did not expose a separate namespace counter. **No file
barrier reduction is measured or claimed.** Differences in load between variants
that change only scan policy are not attributed to prefetch, which runs later.

### 7.4 Full matrix and regression confirmation

Across the twelve exact shapes, new combined versus previous TCP:

| Metric | Shapes with lower median | Median of per-shape ratios | Worst measured ratio |
| --- | ---: | ---: | ---: |
| Whole load phase | 6 / 12 | 0.997× | 1.937× |
| Point-read p50 | 5 / 12 | 1.020× | 1.670× |
| Multi-get p50 | 6 / 12 | 1.009× | 2.757× |
| Whole scan | 10 / 12 | 0.774× | 2.332× |
| Reopen | 4 / 12 | 1.043× | 1.120× |

These are unweighted summaries of matched shape medians, not a pooled workload
or significance result. Overall write improvement is **not** established. The
all-shape table retains the original anomalous 8 KiB/batch32/window1 combined
load: **596.030 ms median**, range **279.962–1,316.446 ms**, versus **307.630 ms**
previous TCP. Every repetition used 128 direct appends, 128 file barriers and zero
parallel preparation calls. Most excess time occurred in storage wait; no policy
difference explains the earlier load from the later scan-prefetch setting.

A targeted nine-case confirmation used the **same executables**, shape and three
rotated repetitions; no code or setting was tuned after seeing the anomaly:

| 8 KiB / batch32 / window1 load | Median ms | Repetition range ms |
| --- | ---: | ---: |
| Previous TCP | 327.455 | 282.589–1,283.900 |
| New two-worker / one-page | 322.962 | 316.891–510.527 |
| New combined | 304.682 | 294.103–325.143 |

The severe candidate regression was **not reproduced**, while a large outlier
appeared in the previous binary. Its cause is not isolated. This does not prove
the original regression was harmless noise or justify removing it. A dedicated
host and longer distribution sampling are needed before tail-performance claims.

The [confirmation audit](../../logs/source-guided-2026-10-06/confirmation-analysis/audit.json)
verifies nine full-byte/reopen cases, 71,082 raw samples, 45 exited storage PIDs
and zero swap. Its results remain separate from the original 252-case matrix.

## 8. Remaining architecture decision

The source-inspired fixes are implemented and tested; native parity remains
unproven. The next measured write opportunity is to retain the successful
parallel preparation **while restoring overlap with storage**, without weakening
preflight, retry, memory or publication rules. True multigroup durability sharing
still needs the safe recovery-envelope design in the TiKV review. Scalar reads
retain TCP/TLS exchange and member-verification costs; scan prefetch does not
solve those. Reopen still needs a durable checkpoint plus suffix-replay design.

No cloud deployment follows automatically from this local completion. This record
does not establish independent-NVMe gains, RDMA necessity, Sui integration, HA or
production tail latency. The source/code map records what was safely adapted and
what remains a distinct design gate.

## 9. Clarification: the previous cluster gap versus the latest local gap

Added 6 October after rechecking the archived per-case CSVs. The approximately
1.9× cloud write result and the latest 2.10× laptop result are different
experiments. They cannot establish a regression between architectures.

All three rows below use 8 KiB values, batch 32, window 32 and a native reference
with an explicit persistence barrier. The table compares **whole load phase wall
time**, with each distributed result divided by native on the same environment.

| Environment and implementation | Load batches | Distributed ms | Native ms | Distributed/native |
| --- | ---: | ---: | ---: | ---: |
| Previous AWS code, larger supplement | 320 | 254.287 | 135.999 | 1.870× |
| Same previous AWS code, standard warm shape | 128 | 106.980 | 53.095 | 2.015× |
| Latest local source-guided code | 128 | 84.096 | 40.032 | 2.101× |

Cloud figures are medians of two runs; local figures are medians of three. The
AWS source predates both the pipeline/cache slice and this parallel-preparation /
scan-prefetch slice. **There is no cluster measurement of the current source.**
The former cloud code's median load gap across all twelve warm shapes was 2.50×;
1.87× was a particular larger workload, not the ratio for every metric or shape.

Cloud raw evidence is under
[`aws-run-retry/extracted/results`](../../logs/optimization-2026-10-06/aws-run-retry/extracted/results),
in `warm-current-v8192-b32-w32-r{0,1}-{tcp,native-sync}` and the corresponding
`storage-current-...` directories. See the
[historical cluster report](OPTIMIZATION_AND_CLUSTER_VALIDATION_2026-10-06.md#larger-workload-and-the-capacity-result)
and §7 above for current local provenance.

### 9.1 What independent NVMe devices can establish

The previous six-machine run already used five distinct storage NVMe devices and
a sixth for the coordinator/native reference. Rechecking all 270
[`node_io.csv`](../../logs/optimization-2026-10-06/aws-run-retry/analysis/node_io.csv)
snapshots confirms physical writes in every interval and **zero physical read
bytes**. The read workload was warm. Those whole-case counters prove independent
write-device use, not drive saturation or a per-operation I/O breakdown.

Independent disks can remove the laptop's shared-device contention and supply
more aggregate storage bandwidth/IOPS when enough independent requests reach
them. They cannot shorten serial coordinator encoding, global publication waits,
or a cache-hit request's network exchange. A global logical counter supplies
ordering, not storage bandwidth or a completion guarantee.

The earlier 320-batch cloud load recorded 135.17 ms preparation/validation,
101.99 ms storage wait and 12.05 ms publication. Storage wait includes transport,
server work and persistence; it is not 101.99 ms of measured NVMe time. This
supports investigating overlap and coordinator CPU before attributing the gap to
the disk layout. The current local slice confirms a tradeoff: preparation falls
58.316→26.045 ms while blocking storage wait rises 29.211→49.738 ms.

### 9.2 Next work, in order

1. **Preserve parallel preparation while restoring storage overlap.** Use a
   bounded rolling pipeline across windows: prepare the next window while an
   already-validated window is being persisted and completed work is published.
   `preparation.rs::submit_many` currently joins all preparation before new
   admission, and the benchmark drains each window before submitting the next.
   Retain the existing per-window preflight contract, exact retry identities,
   aggregate byte/count reservations and contiguous durable publication. Do not
   stream side effects from a window whose required preflight has not completed.
   Validate this locally with causal stall tests and matched old/new binaries.
   TiKV's independent processing stages and Raft Engine's preparation before
   persistence coordination are relevant precedents, not a ready-made protocol
   for this fork; see the dedicated TiKV review.
2. **Then measure scaling on independent disks.** Compare the archived cloud
   baseline and new source, rebuilt and hash-verified, against the same native
   durability policy. Include one versus five storage nodes, the same 128/320
   batch shapes, a concurrency sweep, warm reads and a separately verified
   cache-cold phase. Capture phase-aligned physical I/O, device latency/queue
   depth, coordinator/node CPU, ready/in-flight work and client latency. A longer
   bounded run is needed to distinguish steady throughput from startup and
   outliers. The current small retained-history bounds are not a saturation test.
3. **Choose the next optimization from those measurements.** If nodes wait for
   prepared work, improve admission/preparation scheduling. If small persistence
   barriers dominate, evaluate bounded group formation and the safe recovery
   envelope described in the TiKV review. Merely increasing the cap to 4 MiB
   already made the prior cloud write case 8% slower despite reducing barriers.
   If cold reads are device-limited, increase bounded independent read depth;
   if exchange dominates, investigate the measured transport/serialization path.
   Existing grouped reads and prefetch are starting mechanisms, not proof that
   either the network or storage device is saturated.

Matching or exceeding native **aggregate throughput** is a plausible objective
for parallel storage-heavy workloads, still unproven for this implementation.
An individual remote warm read retains an extra exchange versus a native
in-process hit. Caching can avoid that exchange for eligible hits, and batching
can amortize it across independent reads; additional SSDs cannot remove it.
RDMA may lower part of exchange cost, but neither it nor NVMe-oF fixes serial
preparation or establishes native latency parity. These are separate performance
questions and should have separate acceptance criteria. No new cloud run or code
change is part of this clarification.
