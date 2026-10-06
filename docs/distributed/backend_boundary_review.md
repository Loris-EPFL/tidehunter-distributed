# Native integration boundary: first sharded Value WAL slice

Date: 6 October 2026. Base: `ce37b18`, freshly selected fork/upstream main.
This review applies the [architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md)
and [native correctness inventory](native_correctness_and_storage_options_2026-10-06.md).
It describes implementation boundaries, not a performance or production-readiness result.

## What can safely be reused

The native `tidehunter/src/wal/` module already provides atomic physical allocation,
CRC framing, mmap-backed append, direct record reads and replay iteration. A storage
partition can own that machinery without owning a second key/value index or copying
values into another authoritative KV database. The complete batch remains the
long-term value record. This preserves the paper's §§3.1–3.2 allocation and atomic
replay properties as a design objective.

The initial complete-record codec reuses the bytes emitted by native
`WalEntry::Record` and `WalEntry::Remove`, wrapped with stable identity, original
logical order and a digest. Its decoder is deliberately fallible: native
`WalEntry::from_bytes` assumes trusted bytes and can panic on short/invalid input.
Network or new experimental disk data must not enter that parser unchecked.
The experimental admission limits are explicit. They do not alter `Db`'s existing
batch limits or silently split one atomic batch into several authorities.

Canonical encoding binds database/operation identity, original batch version,
operation order, keyspace, keys and values. Current request authority is separate:
a retry after fencing retains its original digest and mutation version. A digest
is an integrity/identity mechanism; it does not authenticate a network peer.

## What cannot yet be swapped behind `Db`

Native `WalPosition { offset, len }` serves **two distinct roles**:

1. Physical record location in `wal/mod.rs`, `wal/layout.rs` and read paths.
2. User update ordering and processed/replay frontiers in the pending index,
   `large_table.rs`, `wal/tracker.rs`, `wal_replay.rs`, snapshots and relocation.

Offsets allocated independently by different partitions cannot satisfy role 2.
An encoding that packs partition IDs into the existing offset does not solve this:
it invents a physical ordering unrelated to original mutation order. A shared
per-database logical `(epoch, sequence)` is appropriate; a physical address retains
partition, generation, offset and length. One in-process authority can allocate
the logical sequence without an extra network round trip. Allocating a number
still proves neither persistence nor completed publication.

Consequently the first slice is a **native-owned storage substrate and executable
protocol model**, behind an experimental feature. It does not replace `Db` with a
parallel toy KV implementation and does not claim that Sui already uses a remote
Value WAL. The remaining integrated path must change these pieces together:

| Native component | Required adaptation before `Db` uses remote values |
| --- | --- |
| Batch construction and `db.rs` commit | Prevalidate all mutations, allocate stable batch identity/order, append one complete authority record, atomically publish its index entries |
| Index formats and pending table | Store or resolve physical address separately from logical mutation version; preserve same-key and duplicate-key order |
| Point reads, iterators and caches | Resolve records by physical address, verify identity/full keys and preserve native read freshness; use bounded remote requests |
| Replay/control/snapshot | Enumerate the entire durable static stream inventory, seal old authority, resolve holes, replay logical order and record per-stream recovery boundaries |
| Relocation and reclamation | Conditional address replacement without advancing user version; durable recovery mapping and remote reader lifetime before deletion |
| Replication | Copy complete authority records and required inventory; distinguish durable primary completion from asynchronous replica progress |

## Persistence boundary to audit

At the selected upstream base, `Wal::fsync()` synchronizes only
`current_file()`. The mapper may have created later files; synchronizing that file
alone is insufficient evidence for an earlier appended frame. The experimental
storage adapter must synchronize every file covered by the claimed prefix and
persist directory entries before returning a durable acknowledgement. Do not
import the old fork's retained-publication commits wholesale merely to obtain this.

A complete native CRC frame is necessary but insufficient authority evidence.
The registered inventory, stream epoch, validation and retry consistency must all
be checked before emit. A failed persistence response after bytes could exist is
an unknown outcome. Recovery may adopt an eligible complete frame even when the
original caller did not receive success. It must not append an old sealed hole as
if a new authority resurrected that original operation.

### First refinement deliberately restricts writeback concurrency

Serializing mmap copies alone does not serialize disk writeback. A later complete
batch can reach disk while an earlier frame is torn; a native iterator stopping at
the first invalid frame would then hide eligible authority. The first partition
implementation therefore admits only one batch awaiting explicit persistence at a
time. An existing retry is resolved in place; a new batch receives
`NeedsPersistence` before effects until the previous batch's barrier succeeds.
Different partitions can still progress independently. This is a correctness
foundation, **not the intended final batching/performance protocol**. Recoverable
group containers or durable allocation evidence need another explicit refinement
before permitting multiple unsynchronized frames per stream.

Fragment transitions require separate treatment: the next complete frame may
survive without the previous fragment's skip marker. Recovery inspects known
fragment boundaries independently, stopping within each fragment at its first
invalid frame. It does not search arbitrary value bytes for magic headers. The
one-outstanding-batch rule, immutable stream generations and absence of reclamation
are necessary assumptions for this initial recovery rule. File fault-cut tests
cover a missing skip marker, a truncated final frame, blocked later admission and
conflicting logical versions on different partitions. These tests do not simulate
every filesystem, controller or hardware power-failure behavior.

## Research anchors and boundaries

- Tidehunter paper §§3.1–3.4 and §4.4: atomic allocation, complete-batch recovery,
  processed-prefix tracking, snapshot/replay relationship, and conditional
  relocation. The paper distinguishes process-crash/page-cache completion from
  explicit stable-storage synchronization; experimental durable completion must
  name its stronger boundary.
- [CorfuDB stream append implementation](https://github.com/CorfuDB/CorfuDB/blob/master/runtime/src/main/java/org/corfudb/runtime/view/StreamsView.java):
  logical token/order and stream append are different stages, including epoch and
  conflict handling. The analogous complete multi-object authority record is a
  useful boundary, not a proof of Tidehunter recovery or GC.
- [FoundationDB commit-path documentation](https://github.com/apple/foundationdb/wiki/Transaction-Commit-Path):
  durable logging and storage application are distinct progress conditions. We
  borrow that separation, without copying values from the log into an additional
  permanent store. This wiki describes historical implementation details.

These mechanisms justify tests and explicit contracts. They provide no estimate
that one shared laptop SSD predicts independent cluster NVMe throughput. TCP,
authentication, RDMA and asynchronous replication remain separate integration gates.
