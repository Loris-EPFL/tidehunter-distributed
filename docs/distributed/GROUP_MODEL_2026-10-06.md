# Recoverable persistence groups: contract and bounded model

Date: 6 October 2026. M2 storage refinement; **no native index/Sui integration or
performance result is implied by this model**.

## Why a group needs its own recovery contract

The original experimental partition permits one application batch awaiting its
barrier. Simply allowing several separately framed pending batches is unsafe with
sequential native WAL recovery: a later complete batch can survive behind an
earlier torn frame. Mmap copy order does not establish stable-storage ordering.

The new candidate has one pending **group** per stream. A group contains one or
more whole application batches in one native WAL allocation. Every contained
batch becomes authority-eligible only when the complete outer CRC frame and every
member's schema, length, identity and digest validate. A complete inner batch in a
torn group is not independently eligible. Grouping changes the authority boundary
and cannot be enabled while still using the old independent-batch replay rule.

No second group is emitted before the first group has completed its successful
file and directory persistence barrier. Uncertain write or persistence failures
stop the writer until sealed recovery. Therefore an earlier torn group cannot
hide later acknowledged or authority-eligible data within the same fragment.
Recovery scans registered native fragment boundaries independently to discover a
next-fragment group even if the preceding skip marker is missing. It never scans
arbitrary value bytes for magic numbers.

## Source anchors

- Tidehunter paper §§3.1 and 3.3: complete atomic batch recovery and separate
  publication/durability frontiers. Native `wal/allocator.rs` and `wal/mod.rs`
  allocate an entire write region; `wal_replay.rs` discards interrupted native
  atomic batches. The local paper is `../middleware/misc/tidehunter.pdf` relative
  to this repository root.
- [TiKV Raft Engine's write barrier](https://github.com/tikv/raft-engine/blob/master/src/write_barrier.rs)
  and [engine write path](https://github.com/tikv/raft-engine/blob/master/src/engine.rs)
  group concurrent callers around a persistence stage. We borrow that scheduling
  idea; this group's authority/discovery contract is Tidehunter-specific and is
  not established by copying Raft Engine's format or unsafe linked queue.
- The latest [architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md)
  requires semantic validation before authoritative bytes, immutable retry
  identities, complete inventory and unknown outcomes after uncertain I/O.

## Executable model and evidence

[`group_model.rs`](../../tidehunter/src/distributed/group_model.rs) is independent
of production codec types, CRC calls, filesystem code and locks. Its two groups
each contain two complete application batches. Four abstract regions represent a
header, two member bodies and completeness evidence. A crash independently
chooses surviving regions among those emitted; a completed persistence barrier
requires all regions to survive. The group can share a fragment with the previous
group or begin at the next known fragment boundary.

The model was run successfully **before** implementing `group.rs`:

| Case | Reachable states | Transitions | Recovered states |
| --- | ---: | ---: | ---: |
| Same fragment | 2,076 | 3,889 | 817 |
| Next registered fragment | 2,076 | 3,889 | 817 |

Each case covers 238 torn-header states, 238 torn-body states, 288 states recovering
the later group, 393 uncertain-I/O states with surviving authority and 456
lost-response states. Coverage categories overlap. The model explicitly rejects:

1. Emitting a second group before the first persistence barrier.
2. Continuing to append after uncertain I/O failure.
3. Recovering an inner batch from a torn group.
4. Depending on the inter-fragment skip marker for discovery.
5. Reporting durable success before the persistence barrier.
6. Treating an uncertain error or lost response as definite absence.
7. Reappending a lost-response retry as another authoritative occurrence.

Example counterexample for the first unsafe alternative:

```text
emit both groups without persisting the first
→ crash leaves first group absent and second group complete
→ sequential same-fragment recovery stops at first group
→ complete second-group authority is lost
```

Run independently without Cargo or dependencies:

```sh
rustc --edition 2024 --test tidehunter/src/distributed/group_model.rs -o /tmp/tidehunter-group-model
/tmp/tidehunter-group-model --nocapture
```

This is bounded protocol exploration, not a refinement proof. Physical file fault
cuts, native framing, OS errors, directory lifecycle and scheduler progress require
separate implementation tests. It assumes torn/corrupt data is detected and does
not model checksum collisions, malicious writers, permanent disk loss, relocation
or automatic failover. The original broader recovery model remains required.

## Group format and storage contract

[`group.rs`](../../tidehunter/src/distributed/group.rs) uses a versioned 16-byte
header followed by native CRC-framed complete application batches. Each inner
frame starts at an 8-byte aligned position; padding must be zero. The whole group
is itself written as one outer native CRC frame. Inner frames permit existing
native random reads after trusted outer-group validation, without making their
existence independent recovery authority.

`GroupLimits` bounds encoded bytes and member count. The storage configuration
must additionally require the entire outer frame to fit one native fragment and
must validate all members' current authority, placement, retry identities and
versions before effects. `batch_offsets()` is relative to the group **payload**:
add the outer physical offset and 8-byte outer header to form an inner batch's
native physical address. The storage component must remember the outer group's
end for persistence accounting, not substitute one member's end.

This version does not segment oversized atomic batches. The limit is an explicit
experimental API boundary and cannot silently narrow native `Db`/Sui compatibility.
Returning an inner record reference also does not establish native index
publication, stable replication or a reader lifetime permitting garbage collection.

The grouping codec does not by itself schedule concurrent callers or delay a
singleton. A storage scheduler must impose bounded queue bytes/count, bounded
collection time, prompt sequential-request service and independent recovery
capacity. A durable response can cover all members with one persistence stage;
actual barrier reduction and latency require implementation measurements.

## Owned worker shutdown

With `experimental-distributed` enabled (and in unit tests), the existing WAL
thread join helper now waits for actual completion. Tracker, mapper and unlinker
destructors use this helper; the syncer already joins directly. Returning after a
warning timeout could otherwise release the partition's directory lock while an
old worker still touches files. Default native production builds retain their
existing timeout behavior.

The strict path deliberately has no bounded shutdown guarantee. A stalled worker
retains ownership. This change covers the partition's owned WAL chain, not every
native `Db` background worker or future remote lease. The 15 October storage
implementation now permits whole-unreferenced-file retirement from sealed epochs:
a durable independent receipt/descriptor root and pin-clamped index root precede
unlink; existing file/mapping owners survive completion and new read admission
cannot reacquire retired addresses. Active files and live-value relocation remain
out of scope. See the
[current lifecycle implementation and local evidence](CONCURRENT_AUTHORITY_AND_TRANSPORT_2026-10-13.md#15-october-local-lifecycle-and-serving-path-implementation).
Worker tests check lock retention until completion and panic propagation; they
do not simulate arbitrary device hangs.
