# Bounded protocol model: complete-batch Value WAL

Date: 6 October 2026. **Executable design model; not a proof of the storage implementation.**

This implements Gate 1 of the
[architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md#11-work-order-and-acceptance-gates).
The model is independent of production Rust types and methods, so an implementation
bug cannot automatically become its specification. It uses the standard library
only and is compiled as test code in `distributed::model`.

## Anchors and deliberate differences

- Tidehunter's paper, §§3.1–3.4 and §4.4, separates whole-batch recovery, completed
  publication prefixes and value relocation. The corresponding native code is
  [`wal_replay.rs`](../../tidehunter/src/wal_replay.rs),
  [`wal/tracker.rs`](../../tidehunter/src/wal/tracker.rs),
  [`index/pending_table.rs`](../../tidehunter/src/index/pending_table.rs) and
  [`relocation/updates.rs`](../../tidehunter/src/relocation/updates.rs).
  The original paper is available in the sibling middleware checkout at
  `../middleware/misc/tidehunter.pdf` (relative to this repository root).
  This model deliberately requires stable-storage evidence for **durable-mode**
  acknowledgement; the paper's ordinary page-cache acknowledgement is weaker.
- [CORFU, NSDI 2012, §§3.1–3.3](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf)
  motivates explicit epoch seals and hole handling. Our static RF1 inventory
  requires every registered stream; the paper's replicated placement protocol
  must not be reduced to "ask responsive nodes."
- [CorfuDB `StreamsView.append`](https://github.com/CorfuDB/CorfuDB/blob/master/runtime/src/main/java/org/corfudb/runtime/view/StreamsView.java#L128)
  checks size, obtains ordering tokens and handles overwritten/stale tokens.
  The project is a useful authority/reconfiguration reference, but we deliberately
  model the October 6 contract's immutable retry identity and original version.
  Copying a retry as a fresh logical mutation would violate that contract.

The native baseline is `ce37b18` in this fresh fork. The design documents describe
additional older-project native commits too; their added retained-log APIs are
not assumed to exist here.

## What is explored

[`tidehunter/src/distributed/model.rs`](../../tidehunter/src/distributed/model.rs)
uses breadth-first traversal with a visited-state set and predecessor links.
Every enabled transition from every reachable state is checked. Injected unsafe
transitions produce shortest counterexample traces.

### Complete-batch authority

Two reserved logical positions use two registered storage partitions. Each batch
updates two keys atomically. The traversal runs separately for conflicting and
disjoint keys. The inventory is already durably registered and fixed before the
initial state; dynamic registration is outside this bounded model.

Each request can validate, emit an incomplete frame, finish the whole frame,
persist, publish and acknowledge. Replies can be lost. One frontend failure can
occur at any point, with independent choices for losing unsynced complete frames
and making either storage partition unavailable. Durable media is not destroyed.
An unavailable partition may return. Old requests may still arrive before their
particular stream is fenced, including while another stream is already sealed.

Recovery freezes the inventory, seals every registered stream, persists surviving
complete frames, and replays complete records in logical order. It includes
unacknowledged records. A missing early reservation becomes a terminal hole only
after this evidence exists; a later durable batch is retained and published after
that hole. Snapshots preserve the exact index at their logical cut and the replay
boundary for both streams.

Safety checks cover validation before authoritative bytes, durable success,
immutable sealed inventories, terminal holes, atomic publication, original logical
order, snapshot/replay consistency and recovery completeness. The model does not
claim that two independent `get` calls constitute a transactional snapshot.

### Asynchronous replica and remote lifetime

A separate finite model explores one authoritative source, one relocation copy,
an overlapping newer user mutation, one protected reader, one retained snapshot
and one asynchronous replica. Payload copying, complete authority metadata and
replica persistence are different states. Copy completion, copy persistence,
conditional index installation and recovery-map persistence are also separate.

The source can retire only after its index and recovery references are replaced,
the older snapshot is released, the replica no longer needs the source, and read
capabilities are revoked and drained. Reuse changes the segment generation.
Relocation cannot replace a newer user mutation. States with an acknowledged
primary record missing on the replica are explicitly reachable: async replication
does not establish lossless promotion after permanent primary loss.

This is an obligation model for future GC/replication code, **not an implementation
of a remote pin service, asynchronous replication or RDMA memory registration**.

### Bounded admission

A third finite model permits two foreground credits and a separate recovery
progress slot. After failure, foreground credits can remain occupied until
recovery resolves their outcomes. Recovery remains schedulable with both credits
occupied. Sharing the exhausted foreground pool with recovery produces a detected
deadlock. This checks a capacity dependency; it is not a scheduler fairness proof.

## Results

Verified locally with Rust on 6 October 2026:

| Correct model | Reachable states | Checked transitions |
| --- | ---: | ---: |
| Two disjoint complete batches | 2,064 | 5,004 |
| Two conflicting complete batches | 2,064 | 5,004 |
| Reader/relocation/snapshot/async-replica retention | 530 | 1,730 |
| Bounded foreground/recovery admission | 10 | 13 |
| **Total** | **4,668** | **11,751** |

Each batch model reaches 144 recovered states, including 16 with a durable later
batch after an earlier hole, 68 with a lost reply to surviving authority, and
88 with a nonempty snapshot. It also checks 676 states with an unavailable
registered partition during frozen recovery. These categories overlap.

The retention model reaches 20 retired states, 10 reused-generation states and
306 states where the acknowledged source is ahead of its async replica. Eighteen
negative controls are rejected across the three models:

1. Completing a frame before validation.
2. Acknowledging before stable persistence.
3. Publishing across an unresolved reservation.
4. Publishing only one mutation of an atomic batch.
5. Treating a lost reply/timeout as a terminal hole.
6. Recovering from responsive streams only.
7. Using maximum observed sequence as a snapshot cut.
8. Reappending a sealed absent identity.
9. Retrying an old committed identity as a newer user mutation.
10. Deleting while an old reader is protected.
11. Deleting the target of persisted recovery metadata.
12. Deleting a value retained by an older snapshot.
13. Deleting data required for replica catch-up.
14. Installing stale relocation over a newer user mutation.
15. Treating replica payload bytes as complete commit authority.
16. Claiming lossless promotion from a lagging asynchronous replica.
17. Accepting an old address generation after segment reuse.
18. Making recovery depend on foreground credits that recovery must release.

Example counterexample to item 6:

```text
frontend fails; both registered partitions are unavailable
→ freeze inventory
→ incorrectly recover from the empty responsive subset
→ rejected: holes have no complete sealed inventory
```

Example counterexample to item 9:

```text
both conflicting batches become complete
→ frontend fails before publication
→ freeze and seal both streams, persisting surviving frames
→ recover in original logical order
→ incorrectly publish the first batch again under the new authority
→ rejected: visible index is not the original logical prefix
```

## Running the model

From this repository root:

```bash
cargo test -p tidehunter --features experimental-distributed distributed::model -- --nocapture
```

It can also run without building the native engine or obtaining dependencies:

```bash
rustc --edition 2024 --test tidehunter/src/distributed/model.rs -o /tmp/tidehunter-sharded-protocol-model
/tmp/tidehunter-sharded-protocol-model --nocapture
```

There are five tests, including the negative-control suites. Counters are printed
by `--nocapture`; tests require nonempty fault/recovery coverage rather than fixing
an exact state count that would prevent future model expansion.

## What this gate permits and what remains

The checked state space supports implementing an experimental, static registered
single-epoch storage backend. Its actual frame decoder, file ordering, filesystem
sync errors, native WAL recovery, directory locks, request admission and read
identity checks still need independent implementation tests.

Before automatic failover or GC can be enabled, extend this model or another
independently reviewed specification for repeated epochs, multiple active recovery
contenders, dynamic stream inventories, same-stream multi-record physical layouts,
multi-frame large batches, durable retry expiry, finite leases/revocation failures,
replica promotion and actual crash-atomic metadata writes. A native process lock is
not cross-host fencing. An unavailable or permanently lost RF1 stream cannot be
silently ignored. The current model does not prove liveness under permanently
unavailable participants, Byzantine behavior, cryptographic authentication, native
index implementation correctness or refinement of the complete distributed engine.

No speedup, cluster result, multi-NVMe behavior or RDMA support follows from these
state counts. Local development uses one shared SSD and the portable path.
