# M1/M2 local implementation and M3 publication groundwork

Date: 6 October 2026. **Bounded experimental components; native `Db`, Sui and
remote transport integration remain unfinished.**

This record supersedes the implementation status in the
[first-slice record](IMPLEMENTATION_2026-10-06.md). The
[next-step plan](NEXT_STEPS_2026-10-06.md),
[architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md)
and [native correctness inventory](native_correctness_and_storage_options_2026-10-06.md)
remain the design contract. Work stays in `tidehunter-sharded-wal` on
`feat/sharded-value-wal`, preserving the existing native, middleware and Sui
comparison checkouts. No AWS resources were launched.

## What changed

| Component | Implemented responsibility | Serving/integration boundary |
| --- | --- | --- |
| `distributed/{codec,types}.rs` | Version-2 complete batches with independently verified record descriptors, logical mutation identity, bounds and integrity checks | A record reference needs membership established by complete append/recovery; a valid inner hash alone cannot authorize a mutation. |
| `distributed/group.rs` | A complete outer authority group containing independently readable complete batch frames | Whole-group validation determines authority eligibility. Groups do not establish database publication. |
| `distributed/group_model.rs` | Independent bounded exploration of grouped persistence, torn regions, uncertain outcomes and next-fragment discovery | A bounded protocol model, not a filesystem/refinement proof. |
| `distributed/storage.rs` | Native WAL append/persist for whole groups, exact per-member receipts, direct record reads and full-inventory sealed recovery | One unpersisted group per partition. Local storage component with no network lease or automatic failover. |
| `distributed/worker.rs` | Bounded requests/bytes, local group formation, immediate isolated-caller service, durable per-batch results, drain/join and fail-closed uncertainty | No RPC or global per-worker dispatcher. A worker claim excludes another local scheduler, not a remote writer. |
| `distributed/publication.rs` | Distinct durable/published prefixes, exact identity admission, gap retention and ordered publication callbacks | The callback is not connected to the native index, checkpoints or `PendingTable::Transaction` yet. |
| `wal/mod.rs` | Bounded independently checked record-range reads using native mmap or file reads | Read lifetime is safe under the current no-GC/no-address-reuse restriction. |
| `thread_util.rs` | Actual owned-worker joins in tests and experimental builds | A stalled worker may block shutdown; directory ownership is retained. Default native production builds keep their prior timeout behavior. |
| `examples/sharded_wal_local.rs` | Five partition workers, complete batches, scalar reads, orderly shutdown, sealing and recovery | Shared-SSD correctness example, not native `Db`, Sui, TCP or RDMA. |

### Individual values remain directly readable

The earlier `read_batch` path decoded/verified the complete batch. M1 adds
`record_reference` and `read_record`: storage checks the exact retained authority
membership and descriptor, then reads and verifies only the selected record.
Values have one authoritative long-term representation in the native WAL group.

The scalar-read regression uses a 48-byte encoded record beside a 1 MiB sibling.
It exercises mmap reads and file reads after sealed recovery. After complete
authority validation, corrupting the sibling leaves the selected scalar readable,
while the sibling and whole-batch checks fail; corrupting the selected record makes
its own read fail. Forged generation and descriptor bounds are rejected. The
48-byte figure describes the requested logical record slice, **not** physical
SSD transfer granularity or a measured I/O latency.

References/descriptors are derived from a validated complete authority record.
Corruption in a sibling before recovery can still invalidate the outer group and
fail recovery. Independent scalar integrity does not weaken recovery's whole-group
authority requirement.

### One pending group replaces one pending batch

A persistence group is one native allocation containing complete application
batches. Every member must validate before effects. Recovery accepts members only
when the outer group and all member frames are complete and valid; a complete
inner batch inside a torn group is not independently authoritative.

The next group cannot be emitted before the previous group's successful file and
directory barrier. This prevents a later complete group from hiding behind an
earlier torn group within a fragment. Recovery still scans known registered
fragment boundaries, so a missing skip marker cannot hide a later-fragment group.
It never searches arbitrary value bytes for apparent frames. Uncertain append or
persistence errors poison the writer until sealed recovery.

The [group model](GROUP_MODEL_2026-10-06.md) was run before relaxing the old rule.
Its two layouts each explore 2,076 states and 3,889 transitions. It rejects seven
unsafe alternatives, including pipelining before a barrier, recovering an inner
batch from a torn group, timeout-to-hole conversion and retry duplication. Physical
tests then exercise complete/torn groups, truncated tails, missing skip markers,
lost responses, identity conflicts and incomplete inventories.

### Grouping has bounded local ownership

`GroupCommitWorker` counts active and queued requests against both request and
byte limits. Admission is nonblocking and rejects before effects when full.
Isolated callers start immediately; configured coalescing delay applies only to a
backlog with at least two requests, and is limited to 10 ms. Group count/bytes have
their own bounds. Dropping a response handle does not cancel an admitted write.

A controlled regression admits one batch, holds its worker while eight more
arrive, then verifies **nine batches use two explicit group persistence calls**.
The counter excludes creation/sealing I/O and native background sync activity.
This demonstrates amortization under that controlled backlog; it does not measure
a 4.5× speedup, request latency, total fsync count or real Sui behavior.

The worker distinguishes before-effects rejection from unknown outcomes. Injected
persistence failure and worker panic do not manufacture definite aborts for active
requests. Queued untouched requests can return before-effects errors. Shutdown
drains/joins before ownership is released. Seven worker tests cover grouping,
admission, duplicate retries, shutdown and uncertain/panicking execution.

Strict joining extends through the owned native WAL worker chain in experimental
builds. A blocked worker retains its directory lock instead of being detached
after a timeout. This does not establish a remote fencing lease, bounded device
shutdown or complete native `Db` background-worker lifecycle management.

### Persistence no longer holds the read metadata mutex

The partition now serializes explicit persistence/sealing with a separate lock.
It validates the exact pending group under the short metadata lock, releases that
lock for disk I/O, and advances only the captured group frontier after success.
Admission still cannot emit another group during this interval. Already durable
retries return without waiting behind a newer group's persistence.

A regression pauses the persistence stage and verifies that an existing scalar
read, an already durable retry, and a before-effects rejection of the next append
all complete before that stage resumes. The complete distributed suite passes
with this split. It proves removal of this critical-section dependency, not a
measured production speedup. Current queue/metadata locks still exist; see the
[next-step plan's concurrency section](NEXT_STEPS_2026-10-06.md#8-distributed-path-concurrency-and-the-lock-free-question).

### Logical publication is groundwork for M3

The new tracker retains admitted/in-flight/unknown/durable/published states with a
fixed identity count bound. Durable responses can arrive out of order but cannot
advance a contiguous durable prefix across a gap. Publication has a separate
prefix and advances only after an ordered callback succeeds. Callback failure or
unwinding panic requires authority recovery; future durable evidence is retained.

There is no live hole-resolution API, no timeout-based absence proof and no reset
that guesses whether an uncertain callback committed. Construction does not fence
anything. Native integration must establish authority, wire the actual atomic
pending transaction and gate cached/normal reads after an uncertain callback.
The [native source map](NATIVE_INTEGRATION_MAP_2026-10-06.md) records every relevant
physical-address, logical-order, checkpoint, replay and retention boundary.

## Compatibility and remaining limits

- **Experimental format version 2 is incompatible with version 1.** Registration
  and codec checks refuse old experimental directories. There is no in-place
  migration and the original native `Db` format is unchanged.
- A whole group, including each complete batch, must fit one native fragment;
  the native frame representation also has a limit below 1 GiB. No segmented
  oversized atomic-batch representation is implemented. These explicit component
  limits cannot silently become narrower native/Sui serving API limits.
- Each retained batch keeps identity/version and per-record descriptors in memory.
  Batch count, operations and bytes are bounded by configuration, but history is
  not evicted. There is no claim of constant memory for an arbitrarily long epoch.
  A proved retry-retention floor, persisted reference map or recovered epoch
  rotation is required before sustained service. An identity must never be
  silently forgotten and reexecuted.
- There is no value-log GC, address reuse, relocation, async replication or replica
  promotion. Current retention deliberately preserves every authority value.
- Fault tests cover the exercised group, persistence and lifecycle failures.
  They do not exhaust all rename/directory-sync failure points or emulate physical
  power loss and permanent disk loss.
- Actual native index reference/order separation, native transaction publication,
  checkpoint latches, replay, distributed snapshot manifests and health gates
  remain unfinished. Ordinary `Db` still uses its native local WAL path.
- Authenticated TCP storage services, transport retry/deadline behavior and remote
  fencing remain unfinished. No RDMA hardware/path was exercised.
- Sui is unchanged. The [branch inspection](SUI_VALIDATION_BASE_2026-10-06.md)
  recommends the existing `c79040af6c` branch for the first matched comparison;
  current upstream `4630995ca0` is a separate later compatibility target.

## Verified local results

| Validation | Result |
| --- | --- |
| Feature-enabled native library before the final persistence-lock split | **433 passed, 0 failed, 6 ignored** |
| Complete distributed suite after the persistence-lock split | **54 passed, 0 failed**, including the new concurrent read/persist test |
| Default native library | **380 passed, 0 failed, 6 ignored**; includes two added strict-shutdown tests |
| Feature-enabled native crate, all targets | `cargo check` passed |
| Group codec plus independent group model | Eight tests passed, included in the integrated suite |
| Publication component | Six tests passed, included in the integrated suite |
| Worker component | Seven tests passed, included in the integrated suite |
| Freshly built local example | **5 partitions, 80 complete batches, 320 scalar reads, sealed full-inventory recovery verified** |
| Example memory/swap | Peak RSS **7,644 KiB**; process swaps **0**; system `pswpin`/`pswpout` both stayed **0 → 0** |

The five-partition example observed 20 groups and 20 explicit group persistence
calls. Group composition depends on scheduling; 20 is an observation, not an
invariant. The debug correctness exercise took 0.57 s. This is not a storage speed
benchmark and cannot establish native/distributed performance parity.

All partitions use the laptop's **one shared SSD**. No Sui build/audit, cloud run,
independent-NVMe comparison, network transport or RDMA test is included. Six ignored
native tests are not counted as validation.

Commands, run from this repository root:

```sh
CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --features experimental-distributed --lib -- --test-threads=2
CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --lib -- --test-threads=2
CARGO_BUILD_JOBS=2 cargo check --offline -p tidehunter --features experimental-distributed --all-targets
CARGO_BUILD_JOBS=2 cargo build --offline -p tidehunter --features experimental-distributed --example sharded_wal_local
target/debug/examples/sharded_wal_local
```

Evidence is retained in ignored `logs/implementation-2026-10-06/`:

- `native-feature-m1-m2-tests.log`
- `distributed-after-io-lock-split.log`
- `storage-read-persistence-tests.log`
- `native-default-m1-m2-tests.log`
- `feature-all-targets-m1-m2.log`
- `grouped-local-build.log`
- `grouped-five-partition-local.log`, including RSS and swap counters
- `grouped-local-source-sha256.json`, binding the participating sources and built
  example binary; example binary SHA-256
  `bd805bb73cf0bf59885da1f6429efe0db483d1e6f504a6f36fcc6ee9eeceedc8`

## Next implementation boundary and source anchors

M3/M4 now require coordinated native index/reference format, atomic publication,
epoch recovery and snapshot work. The immediate goal is a recoverable explicit
native distributed `Db` mode, followed by authenticated TCP and the unchanged Sui
API. A toy in-memory KV oracle or silently reused native local WAL would not meet
that gate. Keep the remaining milestones in the [next-step plan](NEXT_STEPS_2026-10-06.md).

- Tidehunter paper §§3.1–3.4 and 4.4: values remain in the WAL, complete batch
  replay, separate completion/durability, checkpoints and conditional relocation.
- Native `wal/{allocator,mod,tracker}.rs`, `wal_replay.rs`, `batch.rs` and
  `index/pending_table.rs`: actual implementation mechanisms and integration
  constraints, rather than paper-only assumptions.
- [TiKV Raft Engine write barrier](https://github.com/tikv/raft-engine/blob/master/src/write_barrier.rs):
  grouping concurrent callers around persistence. The new implementation uses
  owned bounded jobs and a joined worker; it does not copy the intrusive list or
  assume the same recovery format.
- [CORFU](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf):
  sequence identity separated from physical storage and fencing before absent
  reservations can be finalized. Our local counter/tracker does not itself provide
  CORFU's fencing or cluster membership.

These mechanisms motivate the implementation. Performance hypotheses still need
matched local mechanism evidence and, later, independent-drive cluster results.
