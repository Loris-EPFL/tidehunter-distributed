# Sharded Value WAL implementation

Started 6 October 2026 in a separate checkout named `tidehunter-sharded-wal`.

**Current implementation and test evidence:**
[Implementation record](IMPLEMENTATION_2026-10-06.md).

Run the local two-partition correctness example from the repository root:

```sh
cargo run --offline -p tidehunter --features experimental-distributed --example sharded_wal_local
```

It uses temporary directories on the shared SSD and removes them after verifying
32 complete batches, reads, seals and recovery. It is not a speed benchmark.

## Source and branch

- Branch: `feat/sharded-value-wal`.
- Base: `ce37b18`, verified against both remote `origin/main` and `upstream/main`.
- Origin: `git@github.com:Loris-EPFL/tidehunter-distributed.git`.
- Upstream: `git@github.com:MystenLabs/tidehunter.git`.
- The earlier local `refactor/distributed-nvme` commits are not included wholesale.
  Required mechanisms are reviewed individually against this fresh base.

The architecture decision and correctness inventory copied here are dated research
records. Their original relative source links refer to the parent project layout;
use native paths in **this checkout** for implementation. The decision's source
review used the older local branch; line numbers and claims must be rechecked.

## Current contract

The October 6 [architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md)
selects one index/publication authority per logical database and complete atomic
batches placed in independently appendable Value WAL partitions. Global management
assigns epochs, inventory and budgets; node-local workers execute storage work.
The [correctness inventory](native_correctness_and_storage_options_2026-10-06.md)
remains a required reference. Earlier independent-owner middleware stays a baseline.

The opt-in `experimental-distributed` feature identifies the new contract explicitly.
The legacy `Db` is still the serving backend. A partition service is a log component,
not a second key/value engine or proof that native index/replay integration is done.

### Shared logical WAL position

The user asked whether a global WAL counter could preserve speed. Yes: within one
logical database, `(epoch, sequence)` is shared across all storage partitions.
`distributed/sequence.rs` allocates it using a local atomic counter at the single
active database authority. Storage nodes allocate their own physical byte ranges.
Allocation adds no standalone sequencer RPC. Unrelated logical databases need not
share the counter. This primitive does not itself establish exclusive authority.

A globally comparable number is not a completed/durable prefix. A stalled batch
can leave a gap while later batches finish on other nodes. Recovery must fence,
enumerate all registered streams and resolve those gaps. Several independent writer
authorities would require a further protocol, such as sequencer reservation ranges;
we do not implement one remote atomic operation per mutation. A wall clock or unique
ID generator alone cannot prove committed ordering or complete recovery.

## Research anchors for every implementation slice

- Tidehunter paper, §§3.1, 3.3–3.4, 4.4: complete-batch recovery, completed frontiers,
  values remain in the WAL, and conditional relocation. Local reference:
  `../../../middleware/misc/tidehunter.pdf` from this directory.
- Native `tidehunter/src/{batch,db,wal_replay}.rs`, `wal/`, `index/pending_table.rs`:
  validate against actual code, not the paper alone.
- [CORFU](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf):
  fence/seal before resolving missing positions; timeout is not proof of a hole.
- [CorfuDB multi-object append](https://github.com/CorfuDB/CorfuDB/blob/master/runtime/src/main/java/org/corfudb/runtime/object/transactions/OptimisticTransactionalContext.java):
  one authority entry for several object mutations; no claim that its whole design
  can be copied without Tidehunter-specific recovery and relocation proofs.
- [DynamoDB](https://www.usenix.org/system/files/atc22-elhemali.pdf) and
  [TiKV's scheduler](https://github.com/tikv/tikv/blob/49e7a1179d676b1bc51447f6b0ff8973e45bc85f/src/storage/txn/scheduler.rs):
  coarse control plane, local bounded admission and execution.

Keep operation identity, current request authority, logical version and physical
address distinct. CRC validity alone does not authorize a mutation. Validate before
emitting complete recoverable bytes. Fail closed on incomplete inventory. Do not
silently retry an unknown result with a new identity, overwrite sealed holes, or
delete source values merely because index application completed.

## Local validation boundary

This laptop has one shared SSD and no assumed RDMA device. Correctness tests can
exercise multiple partitions and failures locally; they cannot establish independent
NVMe speedups, RDMA performance, cluster durability or Sui workload parity. No AWS
resources are required for this stage.
