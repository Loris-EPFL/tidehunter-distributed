# TiKV source review and exact persistence changes — 6 October 2026

This is the TiKV/TiDB agent's research and implementation record for
[source-guided pipelines](SOURCE_GUIDED_PIPELINES_2026-10-06.md). It supersedes no
earlier measurements. Tests and performance for the complete integrated change
belong in that main record.

## Reading scope and reproducible sources

Read the **entire 13-page paper**, including evaluations, related work and
references: *TiDB: A Raft-based HTAP Database*, PVLDB 13(12), pp. 3072–3084, 2020,
[publisher PDF](https://www.vldb.org/pvldb/vol13/p3072-huang.pdf).
Its SHA-256 is
`17cf841da3cb8068e4b9670bfaa9b35565e624bc5d0a23a6e759300be52741e8`.
The PDF and extracted full text are retained in
`logs/source-review-2026-10-06/tikv/`.

Read these **complete scoped source files**, including inline tests. This is not
a claim to have read all TiKV documentation or its full repository.

- Raft Engine revision `097c499a19fbb38754c73aa2f31532329df7c0c6`:
  [`src/engine.rs`](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/engine.rs)
  and [`src/write_barrier.rs`](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/write_barrier.rs).
- TiKV revision `c61d92c26a4d96a575386f5e32179550556e2c29`:
  [`components/raftstore/src/store/async_io/write.rs`](https://github.com/tikv/tikv/blob/c61d92c26a4d96a575386f5e32179550556e2c29/components/raftstore/src/store/async_io/write.rs).
- Additional targeted Raft Engine source: `file_pipe_log/pipe.rs`
  (`sync_dir`, `rotate_imp`, `append`, `sync`) and `log_file.rs`
  (`close`, `write`, `sync`), pinned to the same revision.

The paper's 2020 architecture and the current implementation are separate
evidence. In particular, the paper is not a description of every optimization in
the pinned 2026 source.

Local anchors rechecked: Tidehunter paper §§3.1, 3.3–3.4; native
`wal_replay.rs`, `WalTrackerState::add_processed`, experimental persistence
helpers in `wal/mod.rs`; complete distributed `group.rs`, `storage.rs`,
`worker.rs`, `group_model.rs`, and storage fault-cut tests.

## Exact source → local problem → change

| Source and exact mechanism | Our problem | Adaptation and local location | Status |
| --- | --- | --- | --- |
| TiDB §4.1.1, p. 3075: independent persistence, replication and application stages overlap, while replies retain their dependencies | Serial preparation at one authority limits the benefit of independent drives | Bounded preparation before ordered publication in `remote_db.rs` and `remote_db/preparation.rs`; parent agent owns implementation | Integrated separately; the paper motivates stage separation, not our identity/recovery protocol |
| Raft Engine `Engine::write`, lines 158–190: prepare before entering the write barrier; complete a writer group after shared sync | Worker repeatedly reacquires receipt state after syncing the same complete group | `PartitionLog::persist_many`, `persist_receipts`, `pending_receipts` in `storage.rs`; `worker.rs::process` calls once for the group | Implemented; amortizes receipt processing, **does not reduce complete-group file barriers** |
| Raft Engine `file_pipe_log::SinglePipe::rotate_imp`, lines 249–282, syncs a new file and directory; `SinglePipe::sync`, lines 398–404, syncs an existing file without another directory sync | Every distributed group opened and synchronized the unchanged WAL directory | `State::namespace_synced_file`, `PartitionLog::persist_receipts`; experimental-only `Wal::persist_frame_file` / `Wal::persist_namespace` | Implemented; exact file proof reused only after a successful first namespace sync |
| TiKV `WriteTaskBatch::add_write_task`, lines 609–673; `Worker::write_to_db`, lines 1109–1271: associate completion with exact peer/ready progress and notify after persistence | An allocated maximum is not evidence that prior batches are durable or published | Preserve exact receipts, explicit durable boundary and contiguous logical publication; no new global counter semantics | Existing correctness rule retained |

Source links for the namespace distinction:
[file creation and rotation](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/file_pipe_log/pipe.rs#L249),
[steady file synchronization](https://github.com/tikv/raft-engine/blob/097c499a19fbb38754c73aa2f31532329df7c0c6/src/file_pipe_log/pipe.rs#L398).

These are design adaptations, not copied source. Raft Engine's intrusive unsafe
writer list and panic-on-sync-failure policy were not copied. Our bounded owned
jobs and explicit unknown outcomes remain in use.

## Persistence behavior after this change

`persist_many(&[AppendedBatch])` returns matching `DurableBatch` objects in input
order. Duplicate retries remain duplicates in the response. Previously durable
receipts may accompany members of the sole pending group.

1. Validate **every** supplied receipt before starting I/O. A foreign generation,
   unknown identity or mismatched physical position rejects the call.
2. If all receipts are already durable, return without waiting behind another
   group's I/O. Old receipts never promote that later pending group.
3. Under the persistence mutex, recheck the exact pending group. Multiple pending
   group boundaries are an error, not a reason to sync an allocated maximum.
4. Synchronize the file containing that group on **every** persistence call.
5. If its exact file ID differs from this handle's acknowledged namespace proof,
   synchronize the directory. Record that proof only after successful completion.
6. Advance the exact durable group boundary, then return receipts. Ordered index
   publication remains the authority's responsibility.

The first group in each file still needs a directory barrier. We intentionally do
not initialize a speculative proof from mapper lookahead or the largest observed
filename. The current implementation never deletes/reuses active partition files;
introducing GC, file replacement or log reuse must revisit this proof. Restart
does not load the in-memory proof: recovery seals old streams, synchronizes their
surviving files and creates a new writer epoch separately.

`PartitionStats::persistence_barriers` still counts successful group file barriers.
The new `namespace_syncs` counts successful explicit directory barriers on first
use. Creation, recovery, native background syncs and sealing are excluded from
both counters where documented. These counters establish avoided work; they are
not latency estimates.

The only shared native file edited for this change is `wal/mod.rs`, inside
`#[cfg(feature = "experimental-distributed")]` helpers. The inherited `Db` write,
read, allocator, tracker, replay and flush behavior is unchanged. Sui and the old
middleware are unchanged.

## Correctness tests

New tests in `tidehunter/src/distributed/storage/tests.rs`:

- `bulk_persistence_validates_every_receipt_before_io_and_keeps_exact_order`:
  empty input, forged generation rejection before I/O, duplicate/reordered
  receipts, old-plus-pending receipts, no promotion by an old receipt, reopen.
- `namespace_sync_is_amortized_only_for_the_exact_wal_file`: twelve durable groups
  cross multiple files; every group keeps its file barrier, only first use adds a
  directory barrier; recovered receipts match exactly.
- `failed_new_file_namespace_sync_never_acknowledges_or_advances_proof`: inject
  failure **after file sync and before directory sync**, retain old proof and
  durable boundary, poison the writer, then resolve surviving identity on reopen.
- `bulk_receipts_are_not_returned_before_the_shared_barrier`: hold an explicit
  I/O gate; no durable response escapes and no later group is appended until
  release. This uses causal gates rather than a performance threshold.

Existing worker panic/uncertain-I/O tests, whole-group torn-write tests, and the
independent group model must continue to pass. These are process/file fault
simulations, not a physical power-cut certification.

Focused validation completed: **23 storage tests, 10 worker tests and both
independent group-model tests passed** with the experimental distributed feature,
offline locked dependencies. Logs are `storage-tests.log`, `worker-tests.log` and
`group-model-tests.log` in `logs/source-review-2026-10-06/tikv/`. The first compile
attempt occurred while other agents' modules were incomplete; the completed
integration compiled and these tests passed. Use the main record for the final
frozen-source full-suite and benchmark results.

## Why multiple unpersisted groups remain prohibited

Our variable-length native-framed stream is recovered by reading frames in order
inside each known fragment. Consider two unpersisted groups in one fragment:

```text
group A: torn length/header/body
group B: complete surviving bytes
```

A failed frame in A can hide the start of B. Scanning arbitrary payload bytes for
magic could mistake an embedded valid frame for authority. TiKV's grouping code
does not solve this recovery-format problem. The local independent `group_model`
already rejects this alternative. We therefore retain the one-pending-group gate
and fail-stop behavior after uncertain append/sync.

### Concrete safe cohort option — specified, not implemented

A versioned outer *cohort* frame can contain several existing complete groups:

```text
native outer CRC
cohort magic/version/count/bounded group offset table
  complete group A (with complete application batches)
  complete group B (with complete application batches)
```

All member schemas, identities, counts and offsets must be validated before native
allocation. Recovery authorizes member batches only if the **whole cohort** passes
outer CRC and complete nested validation. Inner complete groups alone are never
authority. One unpersisted cohort is allowed; all members share its durable end.
This changes the atomic recovery envelope rather than removing the pending gate.

It can fit the **existing 1 MiB fragment and unchanged three-map allowance** when
the aggregate nested groups plus cohort metadata and outer header fit 1 MiB.
Current maximum-size groups already occupy almost that entire fragment, so two
full groups cannot be combined. Useful operation needs smaller transfer-group
targets independent of the registered maximum, and bounded collection of those
groups into one cohort. A lone or full-size group keeps the ordinary format.

Required additional work: versioned registration/decoder acceptance, exact nested
member addresses, aggregate memory/admission limits, recovery corruption and
cross-fragment tests, and transport windowing so several groups reach a node
before the coordinator waits. The previous 4 MiB experiment changed more than
this design and cannot decide its outcome. Equally, this proposal has **no measured
speedup** and is not included in the completed code slice.

## Parts deliberately not transferred

TiDB §4.1.2's leader leases and follower read index require a replicated authority;
they are not permission to bypass our fencing or integrity validation. §4.2's
columnar learners and DeltaTree target HTAP, not Tidehunter's exact values-in-WAL
path. §5.1's timestamp oracle does not make allocation equivalent to commit.
TiKV's adaptive spin waits (`calculate_adaptive_wait_duration`) are workload
specific; adding them would need independent latency and CPU evidence.

The source-backed fixes remove concrete unnecessary work. They do not prove
native parity, eliminate the network exchange, or turn five processes on one SSD
into five independent NVMe devices.
