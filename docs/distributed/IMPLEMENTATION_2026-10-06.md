# Sharded Value WAL: first local implementation slice

Date: 6 October 2026. **Local experimental substrate, not a complete distributed database.**

This record distinguishes implemented code from the research proposal in the
[October 6 architecture decision](distributed_tidehunter_architecture_decision_2026-10-06.md).
The [correctness inventory](native_correctness_and_storage_options_2026-10-06.md),
[native boundary review](backend_boundary_review.md) and
[executable model](MODEL.md) remain required reading before the next slice.

## Checkout and base

- Explicit directory: `tidehunter-sharded-wal` beside the existing `tidehunter`,
  `middleware` and `sui` directories.
- Branch: `feat/sharded-value-wal`.
- Fresh baseline: `ce37b18`, verified against the existing fork's `origin/main`
  and MystenLabs `upstream/main` when this checkout was created.
- Origin: `git@github.com:Loris-EPFL/tidehunter-distributed.git`.
- Upstream: `git@github.com:MystenLabs/tidehunter.git`.
- The old local `refactor/distributed-nvme` branch and its retained/prepared batch
  extensions were not imported wholesale. The former middleware and Sui remain
  comparison/integration repositories.

The two copied research documents reviewed the older native branch. Their source
line numbers and descriptions of its added APIs must not be read as an inventory
of this fresh checkout. All implementation paths below refer to this repository.

## What exists now

The new API is opt-in through the `experimental-distributed` Cargo feature.
Native `Db` remains the serving engine. No Sui execution path selects this new
partition substrate yet.

| Source | Implemented responsibility | Explicit boundary |
| --- | --- | --- |
| `tidehunter/src/distributed/types.rs` | Separate stable `BatchId`, current `RequestAuthority`, logical batch/mutation versions, payload digest and physical partition/generation/address | Types do not establish a cluster lease or an authenticated peer |
| `tidehunter/src/distributed/sequence.rs` | One shared local atomic sequence within a caller-supplied fenced DB epoch; checked exhaustion | No remote sequencer, persistence, automatic restart or completed-prefix proof |
| `tidehunter/src/distributed/codec.rs` | Validated immutable complete-batch record using native put/delete encodings, canonical digest and bounded fallible decoding | Not the legacy native on-disk format; no nested control/relocation operations or silent atomic-batch splitting |
| `tidehunter/src/distributed/storage.rs` | Native WAL-backed partition append, receipt validation, explicit local persistence, reads, retry identity checks, sealed recovery and complete static inventory enumeration | Local component with no TCP server, native index publication, replication or GC |
| `tidehunter/src/distributed/model.rs` | Independent finite-state exploration and unsafe-protocol counterexamples | Design-level bounded model, not proof that implementation refines it |
| `tidehunter/src/distributed/storage/tests.rs` | Native multi-file log, retry, schema, identity/address, inventory and sealed-recovery tests | Functional tests on local storage |
| `tidehunter/src/distributed/storage/crash_tests.rs` | File fault cuts for missing skip marker, truncated final frame, pending durability and duplicate cross-partition version | Explicit file mutations after stopping writers; not physical power-loss testing |
| `tidehunter/src/wal/mod.rs` | Feature-gated synchronization of the exact containing file plus directory for ordinary persistence; all registered files for create/recovery/seal | Explicit local durability; not group commit |
| `tidehunter/examples/sharded_wal_local.rs` | Two concurrently used native partitions, one logical clock, complete-batch write/read/seal/full-inventory recovery example | Exercises the substrate, not the native DB index or Sui |
| Native cache read/refill paths and regression tests | Targeted stale-refill correction from the earlier correctness review | Does not complete every worker/checkpoint/relocation audit in Gate 0 |

The physical storage path reuses native Tidehunter allocation, mmap, CRC framing,
file readers and WAL workers. The complete batch is the value record; this slice
does not persist a second authoritative KV database or a frontend decision WAL.

```text
local DB authority's shared LogicalClock
    → immutable identity + logical version + validated complete batch
    → deterministic registered partition
    → native Value WAL append
    → explicit storage persistence receipt
    → [native index publication: next integration stage]
```

Different partition components can append concurrently. Each partition serializes
its local append/admission state. That is physical storage concurrency; the native
atomic index publication and logical completed prefix have not yet been connected.

`read_batch` currently reads and validates the whole complete-batch frame. Mapping
every future scalar `get` directly to that method would introduce whole-batch read
amplification. Native integration needs a defined subvalue addressing and integrity
rule, or justified bounded complete-frame caching. The single outer CRC currently
covers the entire batch; reading arbitrary slices alone cannot verify that CRC.
An authoritative container with independently checkable value slices is one option
to evaluate before calling the final read path equivalent to native WAL access.

## Correctness restrictions that currently shape performance

### One batch awaiting persistence per partition

Mmap copy order is not disk persistence order. If two unsynchronized frames are
allowed, a later complete frame can survive behind an earlier torn one, where a
native sequential replay would stop. This first refinement therefore returns
`NeedsPersistence` **before effects** for a new batch while an earlier one awaits
its explicit barrier. An identical retry resolves to its original receipt.

The rule permits a simpler recovery argument and is intentionally conservative.
It does not implement the intended optimized group commit. A group container or
equivalent durable allocation/record-discovery protocol must be specified and
tested before increasing per-partition unsynchronized concurrency. Benchmarking
this restriction as the final performance design would be misleading.

### Fragment recovery and sealed epochs

A complete frame can survive in the next native fragment while the preceding
fragment's skip marker is torn or absent. Recovery therefore scans **known fragment
boundaries** independently. It never searches arbitrary application payload bytes
for a matching magic value. Within each fragment it stops at the first invalid
frame; the one-outstanding-batch rule and fail-stop behavior after uncertain
append/persistence errors are necessary for this rule.

CRC validity is followed by complete-batch decoding, schema and limit validation,
database/partition/epoch checks, and identity/version uniqueness checks. A
CRC-valid but semantically invalid record fails recovery. A surviving eligible
unacknowledged complete frame is persisted and retained during recovery.

Reopening an existing epoch directory permanently seals it. Recovery never starts
an old writer again. Read-only lookup can resolve surviving identities; absent old
identities cannot be reappended through that sealed component. Creating a new epoch
and deciding its relationship to the old history remain authority/control-plane
responsibilities, not an implemented failover procedure.

### Inventory, durability and publication are different

Every partition stores its static full DB inventory, schema and configuration
durably before accepting a frame. The **database authority must wait until every
registered partition has completed bootstrap before admitting DB writes**.
Constructing one local `PartitionLog` cannot prove that all other components exist.

`recover_inventory` requires the entire configured set and rejects incompatible
schemas and duplicate logical versions. Failure can leave some reachable streams
already sealed; it cannot authorize serving from the responsive subset. A local
directory lock excludes local competing opens and does not fence a remote host.

An `AppendedBatch` is not durable evidence. A `DurableBatch` covers local storage
only. Neither proves replica durability, native index publication, a resolved
logical prefix, or survival of permanent loss of its only SSD. Ordered record
enumeration on a live log can include an unpersisted append; it is not a completed
database history. The current implementation retains all data and has no GC.

Ordinary persistence synchronizes the receipt's exact containing WAL file and the
directory. Create/recovery/seal use the broader all-registered-files barrier.
Native `Wal::fsync()` at the selected baseline synchronizes only the current file;
that is insufficient for a receipt in an older file after mapper lookahead. The
new exact-file operation avoids an increasing all-files foreground barrier. One
pending frame, file synchronization and directory synchronization remain a
conservative implementation, not the final group-commit optimization.

## Validation recorded for this slice

- Native test suite reported by the integration run: **378 passed, 6 upstream
  tests ignored**. Ignored tests are not counted as verification.
- Independent protocol model: **five tests passed**, covering **4,668 states**
  and **11,751 transitions**. All **18** unsafe variants were rejected.
  [Exact model scope and commands](MODEL.md#running-the-model).
- Combined feature-enabled native library suite: **402 passed, 0 failed,
  6 ignored**, including **24 new tests** and the four file fault-cut tests.
  Command: `CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --features experimental-distributed --lib -- --test-threads=2`.
  The reported run took 29.29 seconds; this is a test-suite duration, not a storage
  benchmark. Swap use was 0 bytes during this validation.
- The native crate passed `cargo check` for all targets. A broader workspace
  all-target check encountered a legacy third-party `minibytes` nightly benchmark
  dependency; **the entire workspace has not passed that check**. Native-crate
  success must not be presented as workspace-wide success.
- The public local example ran successfully with **two concurrent native partitions
  and 32 complete batches**, followed by readback, sealing, full inventory recovery
  and digest comparison. Both partitions used the laptop's same SSD.

Local verification logs are retained under
[`logs/implementation-2026-10-06/`](../../logs/implementation-2026-10-06/)
and excluded from Git. The directory contains the native tests, integrated
experimental tests, native check and local example records.

The native suite and deterministic refill tests address the reproduced cache
issue; they do not establish that every Gate 0 audit item is complete. Native
worker shutdown includes timeout-based joins. Checkpoint lifetime, optional
compression/relocation combinations and unsupported index splitting combinations
still need explicit acceptance decisions for the integrated distributed engine.

The laptop has one shared SSD and no assumed RDMA device. These are correctness
checks. There is no new cluster result, Sui workload speedup or demonstrated closure
of the earlier approximately 2× cluster gap in this record.

## Next implementation order

1. **Finish the storage refinement gates.** Keep malformed-file/configuration
   handling and recovery work bounded; preserve the fragment-discovery argument.
   Exercise I/O errors and uncertain outcomes as well as file fault cuts. Establish
   a recoverable group-commit format before weakening the pending-durability rule.
2. **Integrate native references and ordering together.** Native `WalPosition`
   currently supplies both a location and an ordering value. Adapt index entries,
   pending transactions, same-key/duplicate-key ordering, caches and readers to
   separate logical version from physical address. Define format compatibility and
   rollback; do not pack partition IDs into offsets and assume their ordering is
   meaningful. Resolve subvalue addressing/integrity so scalar reads do not
   repeatedly transfer or verify an entire multi-value batch.
3. **Connect native atomic publication and replay.** Durable complete batches
   must publish all their mutations atomically. Implement the resolved logical
   prefix, full-inventory recovery and snapshot cuts with per-stream boundaries.
   A stalled reservation needs terminal evidence; a timeout cannot fill it.
4. **Add the portable authenticated TCP service.** Reuse bounded framing and TLS
   work where suitable, with the same storage contracts. A durable append RPC can
   combine local append and persistence in one exchange while retaining their
   distinct typed completion states; the API decomposition does not require two
   mandatory network round trips. No plaintext transport should silently replace
   the existing middleware's security guarantees.
5. **Add asynchronous replication and retention under their own gates.** Replicate
   complete authority records and required manifests. Promotion must state its
   possible acknowledged-data loss. Remote reader capabilities, conditional
   relocation and durable recovery mapping must precede reclamation. Model
   coverage is not implementation of those mechanisms.
6. **Wire the native fork into the Sui adapter and paired benchmarks.** Preserve
   identical workload, acknowledgement policy, audits and source/binary identity.
   Measure stage times, barriers, bytes and waiting against both native and the
   previous middleware locally before requesting independent-NVMe evidence.
   Hardware RDMA and NVMe-oF remain optional later experiments.

The immediate deliverable is this correctness foundation in a separate fork.
Selecting the architecture for research did not demonstrate its final latency,
throughput, operational availability or memory scaling.

## Source inspirations

The [implementation README](README.md#research-anchors-for-every-implementation-slice)
links the local Tidehunter paper and source entry points. The paper's permanent
Value WAL and atomic batch replay guide this code; native offsets cannot simply
be stretched across independent machines. CORFU informs sealing and missing-slot
reasoning; CorfuDB informs the complete multi-object authority record; DynamoDB
and TiKV inform the division between coarse global control and local execution.
These are component-level inspirations. None proves this implementation correct,
novel or as fast as local Tidehunter.
