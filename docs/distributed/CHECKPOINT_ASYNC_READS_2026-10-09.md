# Checkpoint recovery and bounded asynchronous reads

9 October 2026. Implementation in `tidehunter-sharded-wal`. This replaces the
serial-per-connection read path described in the earlier research snapshot.
It does not implement replication, NVMe-oF, one-sided remote-memory reads or a
new placement policy. It retains mutual TLS and the existing durability contract.

## Read ownership and cache policy

The coordinator owns the key index. A point-read miss follows its published
`ValueRecordReference` to the named shard; there is no extra remote sequencer or
client-side shard-fetch hop. An absent key needs no shard read. A verified
frontend cache hit also needs no shard read. Multi-key reads group references by
shard and use bounded waves. The batch-ID placement rule is unchanged: neither
key proximity nor a favourable value layout is required.

Both caches are derivative, finite and populated only by demand reads:

- The existing frontend four-way CLOCK cache stores verified immutable members,
  keyed by the complete published reference, not just the application key.
- The new shard four-way CLOCK cache stores encoded members. It checks reference
  membership before a hit; the consuming client still verifies member integrity
  on both hits and misses. Cached bytes are not a new source of authority.
- Zero bytes disables either cache. Oversized entries bypass it. Pressure evicts
  entries; the fixed table, owned payload and owner metadata count toward its
  byte budget. Allocator rounding and caller-retained returned values are not
  cache-owned bytes.
- No load/recovery prefill, full-dataset shadow copy or admission policy based on
  knowing the future workload exists. A workload larger than both cache tiers
  must continue to read the shard WAL.
- Clearing the shard cache advances a generation so an older in-flight read
  cannot refill it. Fencing and late transport failures remain fail-stop events
  at the authority; a cache hit cannot hide an already observed failure.

The distributed node defaults to `wal_member_read: async` and a disabled shard
cache. The native/direct WAL policy default remains mapped. Explicit `mapped`
and `positional` distributed policies remain diagnostic controls; requesting a
nonzero shard cache with either is rejected rather than silently ignored.

## Request progress and resource bounds

Protocol and ALPN version 2 correlate authenticated request IDs with replies.
One connection can carry several outstanding reads and return an independent
ready result before a delayed result. Partial TLS records and application frames
retain parser state. Unknown/duplicate IDs, truncated frames and expired absolute
deadlines fail the connection. There is no protocol-v1 fallback or uncertain
request replay over TCP.

One coordinator read-owner thread drives all its persistent connections. The
`read_lanes` setting now selects connections per shard, not blocking actor
threads. Synchronous APIs use the same protocol and wait for their own result.
Dropped read handles retain their credits until completion/error; late failures
still poison the authority.

The coordinator yields between bounded active sweeps; when idle, it waits on
admission so a new scalar read wakes it immediately. Shard sessions keep active
I/O and a one-millisecond idle burst progressing, then back off when idle.
This removes periodic sleeps from accepted read progress. It trades CPU for
latency; the benchmark records process CPU and memory rather than treating
polling as free.

Server sessions use a lazily grown, bounded set of reusable workers. The accept
loop never joins a disconnected session thread. Integration found that
`JoinHandle::is_finished()` was insufficient to make that join nonblocking:
OS thread retirement after asynchronous I/O stalled acceptance and caused a
fresh TLS handshake to time out. Workers now survive session disconnects and
are joined only at service shutdown. The reconnect regression reads real WAL
members across 32 immediate authenticated reconnects.

Each active session drives network progress and its io_uring CQ without waiting
for disk. It uses `try_lock` for inventory checks and checks authority again
before response enqueueing. Resident hits bypass blocking workers. Explicit
mapped/positional reads and long control operations use the bounded control pool.

| Bound | Default | Scope |
| --- | ---: | --- |
| Frontend verified cache | 8 MiB | One authority; zero disables; maximum 256 MiB |
| Shard encoded-member cache | 0 | One node, shared across connections; maximum 256 MiB |
| `read_max_inflight_requests` | 32 | Node-wide, including retained responses; range 1–4096 |
| `read_max_inflight_bytes` | 64 MiB | Node-wide conservative request/read/reply credits; maximum 256 MiB |
| `read_max_device_reads` | 128 | Per-device node admission and per-connection ring capacity; range 1–4096 |
| Outstanding RPCs | 32 | Per authenticated session |
| Conservative session capacity | 256 MiB | Request/control/output ownership |
| TLS buffer | 256 KiB | Per session |
| Fabric application slots | 512 KiB | Four 64 KiB TX and four 64 KiB RX slots per endpoint |

Reusable empty owner maps, SQE scratch and kernel rings are separately bounded
by connection count and configured request/read maxima. They do not retain
values after completion and are not reported as cache bytes. The client also
negotiates member-count and byte bounds: a 256-member public aggregate call is
split when a peer permits fewer device reads or fewer retained bytes.

### Cold WAL I/O and lifetime

Linux `io-uring = 0.7.14` is an optional dependency of the distributed feature.
`Read` SQEs use `IOSQE_ASYNC`; there is no userspace blocking-pool fallback for
this policy. Unsupported kernels or denied ring setup produce an error.

A read plan pins the exact file and `PartitionLog`. Uninitialised buffers become
readable only after an exact-length CQE. Short reads and I/O errors are errors,
not missing keys. Cancellation suppresses successful delivery, not kernel
ownership. A cancelled read's late storage failure is still surfaced.

Device credits retire at CQ completion. Request/byte leases stay with the
response through TLS output and local provider-send retirement. Kernel pointers
never refer to request vectors or movable transport buffers. Transient
submission pressure is retried by the same nonblocking progress path. Shutdown
has a bounded drain; inability to retire live kernel pointers fail-stops the
process rather than freeing live I/O memory.

The libfabric wrapper supplies `try_send`, `try_send_vectored`, `try_receive`,
`try_progress` and `sends_pending`. `WouldBlock` does not poison an endpoint or
transfer ownership of unposted bytes. Posted slots remain immutable until their
matching CQEs. A local send completion is not a storage durability receipt.

## Durable checkpoint adoption and bounded replay

A committed checkpoint consists of an immutable native-index image, a bounded
binary receipt/deduplication image and an authority-manifest commit pointer.
It contains index and retry metadata, not a second copy of all values.

Installation ordering:

1. Drain admitted writes to a complete published prefix.
2. Durably reserve unique staging filenames before creating them.
3. Obtain exact retained-prefix proofs from the required shards and persist the
   immutable index and receipt images, including their digests.
4. Atomically persist the new authority-manifest root. This alone commits it.
5. Remove only the prior committed files named by the durable cleanup ledger,
   sync the directory, then durably clear the ledger.

A crash before step 4 retains the old committed root. A crash after it retains
the new root and resumable cleanup intent. Cleanup failures say that the new
root is committed and fail-stop the handle; they do not claim rollback. Unlink
never truncates or reuses a pinned immutable inode. Untracked files are not
scanned or deleted. This lifecycle retains the current pair plus at most one
staged or obsolete pair, excluding pre-existing untracked files and files still
pinned by a reader.

Reopen reserves a fresh epoch before remote fencing. It adopts a checkpoint only
after every required shard is fenced and schema, database, generation, cut,
image digests and exact receipt prefixes match. It restores original retry
receipts and replays only uncovered complete batches in logical order. A missing
shard or invalid checkpoint fails closed. Interrupted cleanup resumes only after
checkpoint and tail validation.

Automatic admission limits the uncovered tail to **1024 complete batches or
64 MiB of encoded batches**, counting pending/unacknowledged admissions too.
Crossing either bound drains and commits first. Bulk submission preflights the
whole window, including metadata capacity, before any member is admitted;
retries do not consume another identity reservation. Recovered tails are
checkpointed before further writes. `flush_index`, `unload_index` and shutdown
also commit, without rewriting an unchanged checkpoint. The receipt-image cap
is 128 MiB; the configured native-index metadata bound remains separate.

Important limits:

- Checkpoint creation is synchronous and scales with retained index/receipt
  metadata. This is not an incremental or fuzzy CPR/FASTER checkpoint.
- A first upgrade without a committed checkpoint can still require full replay.
- With surviving shard processes, reopen validates retained receipt metadata,
  not all historical value bytes again. Corruption introduced since their last
  validation is detected when the affected value is read or the shard restarts.
  Unread, overwritten or tombstone bytes are not guaranteed to be rescanned by
  coordinator-only reopen. Shard restart retains full WAL validation.
- No WAL reclamation, replication or multi-authority availability was added.
  The existing experimental inventory/index/epoch capacity limits still apply.

## Papers, inspected implementations and local adaptations

The paper sections were read through the QMD corpus. The linked upstream source
commits were inspected separately. These are adapted mechanisms, not copied
upstream code, dependencies on those systems, or claims to reproduce their
benchmark results.

| Source | Inspected upstream implementation | Adaptation in this repository |
| --- | --- | --- |
| [eRPC, NSDI 2019](https://www.usenix.org/system/files/nsdi19-kalia.pdf), §§3.1–3.2, 4.2.2, 4.3 | [eRPC `de83dab`](https://github.com/erpc-io/eRPC/tree/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d), `src/session.h`, `src/rpc_impl/rpc_ev_loop.cc`, `rpc_resp.cc` | Exclusive connection owners, correlated bounded in-flight RPCs, short resident handlers and separate long work in `tcp/reactor.rs`, `remote_db/pending_read.rs`; CQ-owned slots in `tidehunter-fabric`. Not eRPC's UDP transport or congestion-control implementation. |
| [KVell, SOSP 2019](https://dl.acm.org/doi/10.1145/3341301.3359628), §§5.4, 6.5.1 | [KVell `a6cbc0c`](https://github.com/BLepers/KVell/tree/a6cbc0cb186c7934dc6be785225d809f62730fea), `ioengine.c` and `slabworker.c` | Submit/poll disk progress, several outstanding reads and immediate resident hits in `tcp/read_executor.rs`; pinned WAL-member plans in `storage.rs` and `wal/mod.rs`. Uses io_uring rather than copying KVell's libaio wait-for-batch loop or placement scheme. |
| [Kreon](https://dl.acm.org/doi/10.1145/3418414), extended manuscript §3.4; [FASTER, SIGMOD 2018](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/), §§3.3, 6.5; [CPR, SIGMOD 2019](https://www.microsoft.com/en-us/research/wp-content/uploads/2019/01/cpr-sigmod19.pdf) | FASTER `332ea7d`: [checkpoint ordering](https://github.com/microsoft/FASTER/blob/332ea7da532a688acf648ee60c4f63a1caeaf1f7/cs/src/core/Index/Synchronization/HybridLogCheckpointTask.cs), [recovery](https://github.com/microsoft/FASTER/blob/332ea7da532a688acf648ee60c4f63a1caeaf1f7/cs/src/core/Index/Recovery/Recovery.cs) | Immutable image then committed root, recovery adoption and bounded tail in `remote_db.rs`, `native_index.rs`, `tcp/checkpoint.rs` and `tcp/inventory.rs`. Adapted to one quiescent publication authority, not a fuzzy multi-thread CPR implementation. |
| [SMART, OSDI 2023](https://www.usenix.org/system/files/osdi23-luo.pdf), §§4.2–4.3 | [SMART `802577d`, `RadixCache.h`](https://github.com/dmemsys/SMART/blob/802577d8a6b4f28851b504587ed214eca66f6d16/include/RadixCache.h) | Finite derivative-cache ownership and identity/version validation inform the cache constraints. The new shard cache reuses the existing local four-way CLOCK convention. No remote ART, RDMA delegation/coalescing or presumed optimal layout was introduced. |
| io_uring and libfabric lifetime contracts | [io-uring `ba91490`](https://github.com/tokio-rs/io-uring/tree/ba9149010ccda85bcdf28623d9268b10dd353a93), `src/lib.rs`, `src/squeue.rs`; [OFI messages](https://ofiwg.github.io/libfabric/main/man/fi_msg.3.html), [CQ contract](https://ofiwg.github.io/libfabric/main/man/fi_cq.3.html) | Exact SQE/CQE buffer and descriptor lifetimes; nonblocking provider ownership and bounded cancellation/drain. io-uring is MIT OR Apache-2.0 and pinned in `Cargo.lock`; no new FASTER/KVell/eRPC/SMART runtime dependency. |

HERD, Pilaf, LEED and remote-block designs remain alternatives discussed in the
[earlier research assessment](CACHE_RDMA_NVME_OF_DESIGN_2026-10-08.md), not features
implemented by this change.

## Benchmark evidence contract

`sharded_wal_bench` accepts `--tcp-wal-member-read async|mapped|positional` and
`--tcp-shard-cache-bytes`. `read_path_evidence.csv` records before/after counters
outside read timers: frontend plus every shard's cache budget, retained bytes,
hits, misses, evictions, WAL reads/bytes and admission peaks. `wal_reads` counts
admitted WAL operations, not physical device IOPS. Cold physical-read evidence
remains in `read_io.csv`.

`stages.csv` records checkpoint load, proof validation, fencing, connection
setup, tail replay and recheckpoint time, plus tail batch/byte counts. The main
open/reopen timer includes all this work. Per-request multiplexed socket spans
that cannot be attributed correctly are zero, not fabricated timings; connection
socket totals can include interleaved and control traffic.

`experiments/local-read-progress.py` includes cache-disabled Async cases and a
`pressure` suite with frontend-only, shard-only and combined caches. Pressure
requires a dataset larger than four times the sum of application-cache budgets,
at least 1024 reads, positive misses/evictions in enabled tiers and positive WAL
reads in scalar and windowed phases. It rejects missing evidence, over-budget
retention, exceeded node admission peaks and missing value/reopen audits. Cold
cases require disabled caches and positive physical-read bytes.

## Completed local verification

These results use one shared SSD and loopback processes, not independent storage
hosts or hardware RDMA. The final campaign used 1 KiB and 8 KiB values, 64 batches
of 32 initial writes, 1024 reads per phase and one repetition. Native window
reads use 32 workers; Async distributed reads use one connection per shard.
These short runs establish exercised behavior, not stable production rankings.

The 32-case TCP/native campaign and six-case software-fabric cache-pressure
campaign passed value/reopen audits and evidence qualification. Both used
benchmark SHA-256
`081a77b68a54b3f165dbadaaade8d2a87d28398d5c6723eeab522a34436bfb4c`.
All 18 cold TCP/native cases recorded positive physical reads in every reported
process/phase. No campaign case recorded swap or a remaining sampled process.
Peak sampled process-tree RSS was 0.122 GiB for TCP/native and 3.507 GiB for the
software-fabric pressure suite; software-provider memory is not an EFA estimate.

Selected **8 KiB, application-cache-disabled** results:

| Metric | Native | Distributed TCP, Async |
| --- | ---: | ---: |
| Warm scalar read p50 | 2.032 µs | 41.787 µs |
| Warm concurrent reads, window 32 | 464.2k reads/s | 28.8k reads/s |
| Cache-evicted concurrent reads, window 32 | 182.4k reads/s | 61.1k reads/s |
| Reopen, including connections and fencing | 1.264 ms | 160.143 ms |

Native parity is not demonstrated. During implementation, removing active
progress sleeps changed the local zero-cache scalar p50 from 332.369 µs to
41.787 µs, but the corresponding warm window throughput fell from 54.0k to
28.8k reads/s. The final cold-suite warm window measured 58.8k reads/s, which also
shows sensitivity to the surrounding run. These are single-run observations,
not a controlled old-source/new-source comparison or an improvement over the
previous AWS measurements. Concurrent throughput remains a concern.

The displayed distributed reopen loaded its checkpoint in 0.884 ms, validated
prefix proofs in 0.538 ms and replayed zero tail batches. Connection setup took
45.160 ms and fencing took 102.637 ms. Checkpoint adoption does not remove these
costs. Separate crash drivers on TCP and software fabric killed the coordinator
after a committed image plus two later batches; both adopted that image and
replayed exactly **two batches / 1310 encoded bytes**. Both also passed original
retry, new-epoch write, partial-fence resume, missing-node rejection, node restart,
TLS rejection, lost durable reply and stale-epoch checks.

Cache pressure did not turn the WAL into an unused backing store. In the 8 KiB
window phase, the 16 MiB initial dataset exceeded the largest combined cache
budget of 448 KiB:

| Frontend budget | Budget per shard | TCP WAL fetches / 1024 reads | Software-fabric WAL fetches / 1024 reads |
| --- | --- | ---: | ---: |
| 128 KiB | Disabled | 1018 | 1017 |
| Disabled | 64 KiB | 1005 | 1008 |
| 128 KiB | 64 KiB | 1008 | 1005 |

Every enabled tier recorded misses and evictions. Maximum observed retained
bytes were 124768 at the frontend and 60448 per shard, within the respective
131072/65536-byte budgets. All recorded admission peaks were within their bounds.
An additional zero-cache software-fabric cold case passed the same evidence
parser: each scalar, grouped and window phase performed 1024 WAL reads and
recorded 8646656 physical-read bytes. All 25 node PIDs recorded by the final
crash drivers and that cold case had exited after completion.

Regression/build checks passed:

- 585 engine library tests and one benchmark test; two normally ignored
  software-fabric protocol tests were also run explicitly. Seven other ignored
  library tests were not run.
- 21 fabric-crate tests, including the software-provider integration suite.
- 51 Python experiment/safety tests.
- Strict Clippy for the engine/fabric libraries, examples and tests, the default
  engine feature check, and the release examples. The final crash-driver change
  was separately linted, built and exercised on both transports.
- Malformed benchmark evidence exercises rejected header-only input and missing
  shard snapshots rather than accepting an empty comparison.

Receipts are under `logs/checkpoint-async-2026-10-09/`: `tcp-final`,
`fabric-pressure`, `fabric-cold`, `cluster-tcp-tail` and
`cluster-fabric-tcp-tail`. Each campaign includes `plan.json`, `comparison.csv`,
per-case counters and completion receipts. The standalone cold case and crash
drivers have their own benchmark/verification outputs rather than campaign
plans.

To reproduce the two campaigns from the workspace root, use the release example
built with `experimental-fabric,test-utils`, the same libfabric 2.6 library
environment, and fresh output paths:

```bash
python3 experiments/local-read-progress.py \
  --candidate target/release/examples/sharded_wal_bench \
  --output logs/local-tcp-repeat --suite all --transport tcp \
  --values 1024,8192 --batches 64 --reads 1024 --repetitions 1
python3 experiments/local-read-progress.py \
  --candidate target/release/examples/sharded_wal_bench \
  --output logs/local-fabric-repeat --suite pressure --transport fabric-tcp \
  --values 1024,8192 --batches 64 --reads 1024 --repetitions 1
```

## Hardware decision

This task stops after local verification. No cloud experiment is authorised by
this change. If a later campaign is authorised, measure this source on the same
controlled topology before changing hardware. Retain cache-zero and over-capacity
cases, matched native concurrency, identical value sizes, checkpoint/reopen scopes
and CPU/RSS/I/O evidence. Do not attribute software-provider results to EFA hardware.

A faster link cannot remove request scheduling, encryption, storage completion
or checkpoint work. [INFERENCE] Higher bandwidth is useful once measurements
show bandwidth or packet-rate saturation, not merely because the scalar gap is
large. For the quoted 8 KiB results, 62.4k reads/s represents about 4.09 Gbit/s of
value payload, versus a 25 Gbit/s advertised link; this arithmetic excludes
protocol traffic and is not an NIC-utilisation measurement.

AWS lists EFA on `i4i.32xlarge` at 75 Gbit/s and `i7ie.48xlarge` at 100 Gbit/s,
with local NVMe, in its [storage-optimised instance table](https://aws.amazon.com/ec2/instance-types/storage-optimized/).
These also change CPU, RAM, SSD count and cost, so they are not a speed-only
A/B comparison. Verify exact instance-size, region and [EFA capabilities](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html)
before choosing a fleet. No AWS resources were started for this implementation.

## 10 October 2026: hardware comparison

The user subsequently authorised the cloud comparison. Run
`th-0a6a4b67c15648f7` used six `g6.16xlarge` hosts in `us-east-1a`, spread
placement, EFA, and exactly one local instance-store NVMe per host. Five hosts
served shards; the sixth ran the coordinator and the paired native control.
Both builds were rebuilt from their frozen source on the controller.

- Baseline, the completed 8 October CURRENT engine:
  `97f18475bb15b8ad7ae20ac799fd2a39d5399ccdbc7d3ce8c88d5950730ec5b1`.
- Current engine:
  `84bf88b81410cbc82cfd59eb987b8012a887d9288f3b76e250b9678587efa7d0`.
- Uploaded bundle:
  `efcb130d6fd2e673b77f277bc7f7fad524aff66f3c2cf333e88bd5d3e4080663`.
- [Archived build receipts and raw results](../../logs/checkpoint-async-aws-2026-10-10/run-final/extracted/results/).
- [Complete comparison CSV](../../logs/checkpoint-async-aws-2026-10-10/run-final/analysis/comparison.csv)
  and [evidence validation](../../logs/checkpoint-async-aws-2026-10-10/run-final/analysis/analysis.json).

All 80 core API records, 30 cache-pressure records and four qualification records
passed. Each core/pressure shape has five repetitions with alternating build
order. The full campaign remains **partial**: its first full-shard restart failed
in benchmark orchestration, after two of forty recovery records completed.
The original validator reports 116/154 valid API records and 380/570 verified
node archives. The failed case's five partial node archives are retained too.

All six instances reached `terminated`; their six root volumes and network
interfaces were absent in the independent MCP audit. The other users' four
instances remained running. All 1,168 S3 objects were saved locally before
deleting the bucket; the local archive's actual bytes match its S3 receipt.
See [retrieval inventory](../../logs/checkpoint-async-aws-2026-10-10/run-final/all-s3-retrieval.json)
and [resource audit](../../logs/checkpoint-async-aws-2026-10-10/terminated-instance-volume-interface-audit.json).

### Measurement boundaries

This is an RF1 engine benchmark, not a full Sui workload, replicated durability
test, one-sided remote read, or sustained larger-than-RAM workload. The initial
datasets are approximately 10/80 MiB for 1/8 KiB values. The core disables both
application cache tiers; warm OS caching still matters. Cold passes evict before
each phase and verify physical reads, but later operations can find pages loaded
earlier in that phase.

Writes use 32 mutations per batch and 32 batches per window. Read windows use
32 outstanding reads; native uses 32 workers. Native controls remain paired to
their transport-labelled execution and must not be pooled into ten independent
repetitions. Baseline uses mapped reads, four connections per shard and one
control worker; current uses Async reads, one connection and two control workers.
The comparison measures the whole implementation/configuration, not an isolated
io_uring or transport change.

Every latency pair below is the median of five per-run p50/p99 values.
Write-window samples cover the entire 32-batch submission and drain, not a
single batch. There are ten write-window samples per run: its p99 is its maximum.
**Per-batch durable-commit percentiles and B1/W1 write latency were not measured
by this campaign.** Do not divide window percentiles by 32 or call the aggregate
`native_batch_commit_ns` counter a durability-latency distribution.

### Writes

`B → C` means baseline to current. Throughput counts logical mutations.

| Value / path | Load, k mutations/s B → C | Durable window p50/p99, ms B → C |
| --- | ---: | ---: |
| 1 KiB native, TCP-paired | 197.570 → 192.425 | 5.113/5.651 → 5.275/5.764 |
| 1 KiB TCP | 158.174 → 154.194 | 6.142/9.201 → 6.011/10.504 |
| 1 KiB native, EFA-paired | 198.279 → 197.563 | 5.041/5.592 → 4.927/5.600 |
| 1 KiB EFA | 162.770 → 156.469 | 5.796/10.432 → 6.019/9.192 |
| 8 KiB native, TCP-paired | 44.487 → 44.766 | 23.159/24.427 → 23.256/23.938 |
| 8 KiB TCP | 43.812 → 42.859 | 22.792/26.498 → 22.236/36.460 |
| 8 KiB native, EFA-paired | 47.353 → 45.292 | 21.473/23.690 → 22.450/24.098 |
| 8 KiB EFA | 41.300 → 43.146 | 24.050/28.211 → 22.004/35.833 |

Current load throughput is about 95% of matched native at 8 KiB and 79–80% at
1 KiB. This is not read parity or a statement about serial writes.

### Warm reads and engine reopen

Read latencies are microseconds; reopen is milliseconds. `get_many` latency is
per API returning up to 32 keys; its throughput counts keys. Window throughput
uses elapsed phase wall time, not summed individual latencies.

| Value / path | Scalar p50/p99 B → C | get_many p50/p99 B → C | get_many k keys/s B → C | Window k reads/s B → C | Reopen ms B → C |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 KiB native, TCP-paired | 0.691/1.460 → 0.710/1.490 | 21.280/33.941 → 21.541/33.150 | 1283.836 → 1278.396 | 228.127 → 235.083 | 3.178 → 3.269 |
| 1 KiB TCP | 402.509/704.325 → 273.425/619.194 | 838.748/1040.322 → 606.853/734.636 | 37.326 → 51.744 | 36.119 → 64.580 | 262.645 → 79.134 |
| 1 KiB native, EFA-paired | 0.711/1.540 → 0.740/1.510 | 21.861/34.501 → 21.220/33.571 | 1255.282 → 1280.053 | 226.477 → 232.636 | 2.922 → 2.979 |
| 1 KiB EFA | 235.295/513.132 → 219.365/550.492 | 659.275/843.568 → 528.461/628.233 | 47.579 → 59.297 | 46.569 → 14.312 | 949.928 → 371.510 |
| 8 KiB native, TCP-paired | 3.060/4.450 → 3.110/4.590 | 102.342/128.843 → 103.202/127.153 | 258.356 → 255.818 | 197.152 → 200.063 | 2.605 → 2.581 |
| 8 KiB TCP | 450.440/766.856 → 311.916/653.935 | 992.111/1417.210 → 739.756/1160.835 | 30.619 → 39.694 | 33.908 → 39.401 | 726.981 → 68.780 |
| 8 KiB native, EFA-paired | 3.150/4.500 → 2.960/4.490 | 102.942/129.122 → 103.112/132.403 | 257.014 → 255.982 | 199.695 → 197.546 | 2.543 → 2.496 |
| 8 KiB EFA | 278.176/548.452 → 251.925/574.262 | 797.728/1052.523 → 639.923/1060.571 | 37.692 → 45.937 | 43.766 → 13.989 | 1347.920 → 379.120 |

TCP scalar and grouped reads improve. EFA grouped throughput also improves, but
its concurrent-read throughput falls by about 68–69%. At 8 KiB, current warm
window throughput is only about 20% of native with TCP and 7% with EFA.
The revision is not a general performance improvement and does not close the
read gap. Reopen has one sample per run; these are medians, not reopen p99s.

### Cache-evicted reads

Latencies are microseconds; window throughput is k reads/s.

| Value / path | Scalar p50/p99 B → C | get_many p50/p99 B → C | Window p50/p99 B → C | Window k reads/s B → C |
| --- | ---: | ---: | ---: | ---: |
| 1 KiB native, TCP-paired | 0.710/230.004 → 0.720/227.795 | 23.720/4557.377 → 23.881/4492.928 | 137.992/555.672 → 131.383/548.192 | 203.921 → 212.075 |
| 1 KiB TCP | 409.089/724.116 → 324.977/766.477 | 843.238/2403.602 → 735.306/1097.223 | 886.690/1763.798 → 535.982/837.749 | 36.274 → 58.250 |
| 1 KiB native, EFA-paired | 0.690/237.655 → 0.730/226.795 | 23.650/4056.598 → 24.491/4124.639 | 137.783/558.072 → 132.873/552.642 | 205.793 → 211.732 |
| 1 KiB EFA | 240.805/518.011 → 276.806/694.105 | 653.564/1896.580 → 667.766/995.583 | 715.325/1442.311 → 2294.019/4027.569 | 45.631 → 14.253 |
| 8 KiB native, TCP-paired | 104.532/214.835 → 98.212/200.354 | 2357.010/4529.375 → 2107.106/4506.079 | 205.795/483.261 → 202.224/465.510 | 142.373 → 143.327 |
| 8 KiB TCP | 468.540/1076.053 → 431.009/838.048 | 1126.494/4249.143 → 1026.932/1459.501 | 1030.192/3138.899 → 827.828/1203.736 | 28.441 → 38.300 |
| 8 KiB native, EFA-paired | 99.313/204.214 → 105.232/208.165 | 2292.820/4716.609 → 2299.280/4836.456 | 202.955/445.299 → 204.304/455.220 | 143.770 → 141.911 |
| 8 KiB EFA | 293.177/808.828 → 371.718/768.557 | 878.239/3389.062 → 940.280/1384.411 | 808.648/2834.212 → 2322.781/4109.168 | 35.782 → 14.012 |

At 8 KiB, distributed grouped cold reads beat native serial `get_many` p50.
That is a specific benefit of batching/parallel shard work, not universal native
parity. Current TCP cold-window throughput improves 35%; EFA loses 61%.
The sub-microsecond native 1 KiB evicted-pass median does not measure a
sub-microsecond NVMe read: the finite pass becomes resident as it proceeds.

### What the timing and resource evidence establishes

- **Admission/preparation and storage-wait counters exceed publication at 8 KiB.**
  Current TCP accumulates 108.373 ms `validation_ns`, 116.243 ms storage wait and
  21.213 ms publication over 320 batches. EFA records 107.357, 112.266 and 24.946 ms.
  **11 October source clarification:** `validation_ns` includes synchronous drain
  and automatic checkpoint work at the admission boundary; it is not pure
  validation/hash CPU. The approximately 80 MiB load crosses the 64 MiB tail limit.
  Its checkpoint contribution to the worst write windows was not separately timed.
  These overlapping counters and independently computed medians are not an
  additive wall-time decomposition. Storage wait is not pure wire time. See the
  [implementation plan](CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md#22-newly-identified-write-timing-issue)
  for the attribution correction and bounded-checkpoint work; raw results are unchanged.
- **Read waiting remains large.** In 256 additional cold diagnostic reads per
  repetition, current 8 KiB TCP mean whole-call/exchange/server-record time is
  518.123/471.354/192.077 microseconds; EFA is
  449.454/409.393/185.400 microseconds. Actor queue means are 17.073/15.006
  microseconds. Exchange includes server work; server time includes ring
  admission, kernel residence, validation and response construction. The clocks
  overlap and describe a different sample stream from ordinary timed reads.
- **EFA window regression is not explained by device-credit exhaustion.**
  Representative 8 KiB r4 warm-window snapshots show only 3–5 peak inflight
  requests and 1–2 peak device reads per shard against limits of 32 and 128.
  Later grouped phases reach lifetime device-read peaks of 11–16. These are
  cumulative peaks, not per-phase IOPS. [INFERENCE] Progress/scheduling or
  connection policy deserves investigation before faster storage; this campaign
  does not isolate which changed component caused the regression.
- **Connection setup still dominates EFA reopen.** Current 8 KiB EFA r4 spends
  352.814 ms of 379.120 ms opening connections, then 18.786 ms fencing,
  4.019 ms loading its checkpoint and 1.746 ms validating proofs, with no tail.
  TCP r4 spends 58.469 ms of 84.370 ms on connection setup. Checkpoint reuse is
  effective, but it cannot remove connection cost.
- **Physical reads are real.** Current TCP and EFA 8 KiB r4 each record
  57,061,376 process physical-read bytes across shards in each evicted scalar,
  grouped and window pass, with zero initial WAL residency and positive reads
  on every shard. Native-EFA records 61,104,128 bytes per pass. All 380 verified
  node snapshots record zero swap-in/out. Whole-node counters include lifecycle
  work and cannot be used as API-specific device latency.
- **Small caches do not hide the WAL.** The 128 KiB frontend and 64 KiB/shard
  pressure cases have enabled-tier misses/evictions and about 8,170–8,184 WAL
  reads per 8,192-read hot scalar pass. Combined EFA r4 retains 124,768 frontend
  bytes and 60,448 bytes per shard, below the respective budgets. The tiny
  frontend cache adds roughly 8–12% grouped p50 cost here. This is not an
  all-hit workload or a small-cache native comparison.
- **Fewer threads do not prove less per-operation CPU.** Representative 8 KiB r4
  controller identities fall from 55 to 17. TCP sampled user/system seconds
  change from 3.75/2.78 to 6.99/6.48; EFA from 10.76/9.53 to 7.68/6.16.
  These are observed whole-process thread lifetimes, not phase CPU or a
  saturation measurement. EFA sampled controller RSS falls from about 937 to
  486 MiB at 8 KiB; this is not simultaneous cluster peak RSS.

Protocol-v2 per-request send/header/body timing zeros mean those intervals are
not attributable under multiplexing. They do not mean network time is zero.
No dedicated connection-diagnostic campaign cases were run. Actual EFA traffic
proves use of the provider, not one-sided WAL reads or an isolated wire RTT.

### Recovery orchestration defect and local repair

The first full-restart case successfully killed the five transient systemd
services. `systemd-run --collect` then allowed their failed units to disappear
before a checked `systemctl stop`; all five stop calls returned “Unit not loaded”.
The benchmark stopped before a full-restart reopen measurement. This is not
evidence of failed WAL recovery.

The runner now waits for both old PID disappearance and successful exact-name
unit-listing absence before reusing a unit name. It still uses abrupt SIGKILL,
checks command errors, preserves data, removes stale readiness, verifies the new
executable/PID and retains the original deadline. Real local systemd exercises
cover already-collected units, delayed deactivation, bounded waiting,
same-name restart and survival of an unrelated process. All 64 experiment tests
passed with those exercises enabled; no test units remained.

Before cloud launch, both engine builds also passed sixteen local recovery
scenarios: TCP/software-fabric, tail0/tail128 and surviving/full shard restart.
See [local recovery receipts](../../logs/checkpoint-async-aws-2026-10-10/local-recovery-verification.json).
These local observations do not substitute for EFA recovery measurements.

### Completed recovery follow-up

Run `th-5b6db97017c826a5` used the same six-host G6/EFA/NVMe topology and the
same two engine source trees. Only benchmark orchestration/scope changed.
Its immutable `recovery` scope retained fresh four-case hardware qualification
and all forty recovery cases, with a 40-minute hard lifetime and the unchanged
ten-minute export reserve. It did not repeat the completed core/pressure matrix.

The independent archive validator reports **complete=true, 44/44 API records
and 220/220 node archives**, with no missing/invalid cases. All five paired
repetitions passed for both builds, tails of zero/128 batches, and
surviving/full-restarted shards. Full restart means abrupt death and replacement
of all five shard processes while preserving their data, not machine/kernel
restart or OS-cache eviction. Value/tombstone/scan checks and exact retry versions
passed; current recovery loaded its checkpoint and replayed the exact bounded tail.

| Shard state / tail batches | Engine open, baseline ms | Engine open, current ms | Controller-inclusive restoration, baseline ms | Controller-inclusive restoration, current ms |
| --- | ---: | ---: | ---: | ---: |
| Surviving / 0 | 986.372 | 371.917 | 2528.101 | 1615.012 |
| Full process restart / 0 | 982.736 | 383.344 | 3988.831 | 3053.184 |
| Surviving / 128 | 1121.294 | 537.801 | 2425.422 | 1564.299 |
| Full process restart / 128 | 1130.520 | 558.752 | 3893.011 | 3030.017 |

These are medians of five repetitions. Restoration includes restart orchestration
and verification; it is not engine-open latency. The tail workloads also change
which values/tombstones verification reads, so compare builds within each row.
The original full-run receipt remains partial. Use its 114 qualification/core/
pressure records and this follow-up's forty recovery records: together they cover
all 154 planned case identities. Do not pool the original two early recovery
samples with the follow-up's five-repetition series.

- [Recovery raw results](../../logs/checkpoint-async-aws-2026-10-10/run-recovery/extracted/results/).
- [Recovery comparison CSV](../../logs/checkpoint-async-aws-2026-10-10/run-recovery/analysis/comparison.csv)
  and [independent validation](../../logs/checkpoint-async-aws-2026-10-10/run-recovery/analysis/analysis.json).
- [All 673 recovery S3 objects saved locally](../../logs/checkpoint-async-aws-2026-10-10/run-recovery/all-s3-retrieval.json).
- [Cross-fleet case provenance and cost estimate](../../logs/checkpoint-async-aws-2026-10-10/campaign-completion-summary.json).
- [All twelve instances terminated; no root volumes or interfaces; other users unchanged](../../logs/checkpoint-async-aws-2026-10-10/both-fleets-final-compute-audit.json).

The two fleets consumed an estimated $16.26 in EC2 compute, measured from launch
to the recorded shutdown transitions. Added to the earlier tracked estimate,
this is $49.83 before this run's small IP/root/S3 charges, taxes or credits.
This is an estimate, not an invoice.

No more experiments were launched after the user's stop instruction. The next
engineering work is to isolate the EFA concurrent-read regression while holding
read policy, connection count and workload fixed; measure progress and I/O
scheduling rather than treating exchange time as pure wire time. Write throughput
does not justify replacing shard-local Value WALs with a global physical WAL.
Connection setup remains a separate recovery cost. These are next investigations,
not implemented fixes or promises of native scalar-read parity.

