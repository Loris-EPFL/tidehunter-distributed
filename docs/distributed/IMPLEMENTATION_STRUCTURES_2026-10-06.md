# Distributed structure selection and native publication groundwork

Date: 6 October 2026. **Local implementation progress. The experimental components
still do not serve native `Db` or Sui, and no AWS work is part of this slice.**

This record supersedes the current-progress descriptions in
[IMPLEMENTATION_M1_M2](IMPLEMENTATION_M1_M2_2026-10-06.md). That record and
[the initial implementation record](IMPLEMENTATION_2026-10-06.md) retain their
historical test evidence. The [next-step plan](NEXT_STEPS_2026-10-06.md) remains
the implementation roadmap.

Three independent research/implementation tracks examined actual access patterns
and relevant primary designs:

- [Physical reference membership](REFERENCE_DIRECTORY_DESIGN_2026-10-06.md):
  immutable segmented metadata, publication, bounds, reclamation prerequisites,
  and comparisons with ArcSwap, concurrent maps, LSM and bitmaps.
- [Native index structures](INDEX_STRUCTURE_DECISION_2026-10-06.md): native cell
  levels, compact sorted buffers, version history, WiscKey/BlobDB, ART, Bw-tree,
  logical identity encoding and pending transactions.
- [Authority publication/completion structures](PUBLICATION_STRUCTURES_2026-10-06.md):
  dense state slots, exact retry identity maps, completion ownership, unknown
  outcomes, bounded polling and possible queue/bitmap refinements.

## Implemented changes and serving boundary

| Component | Current implementation | Remaining boundary |
| --- | --- | --- |
| `distributed/reference_directory.rs` | Immutable segmented batch metadata; scalar membership reads use nonblocking cells and a published prefix, without the partition metadata mutex | No reclamation, remote reader lease or persisted eviction protocol |
| `distributed/storage.rs` | Exact retry `HashMap`; sparse ordered version `BTreeMap`; immutable metadata integration in append/recovery; explicit pre-effect reference-metadata budget | Append/retry and persistence ownership still use synchronization; the full storage path is not lock-free |
| `distributed/publication.rs` | Dense `Vec<Entry>` for consecutive admitted sequences, exact operation-ID `HashMap`, separate durable/published frontiers | Unknown gaps still block the publication prefix; lifetime retention remains bounded rather than evictable |
| `distributed/completion.rs` | Bounded `CompletionInbox`, one producer ticket per request, immutable durable result in `OnceLock`, separate unknown flag, same-identity retry ownership and bounded rotating polls | No network reactor/wakeup integration; mutex-free handoff is not a lock-free database |
| `index/pending_table.rs` | `PendingTable<V = WalPosition>` and `CommittedChange<V = WalPosition>` reuse the native atomic transaction status for other reference types | Existing native cells, promotion and persistence still use their physical reference path |
| `distributed/index_reference.rs` | Explicit `LogicalValueId` encoding preserves all bits of epoch, sequence and operation index in 20 bytes | Candidate identity payload only; native persisted index format is unchanged |
| `distributed/native_publication.rs` | `PreparedNativePublication` source connects validated local storage batches/receipts to staged native pending tables through a sink and one native transaction | Three focused bridge tests pass; the sink is not a serving `LargeTable`/`Db` implementation |
| `distributed/codec.rs` | Routing validation rejects mutations whose keys cannot be assigned to a native cell before complete authority bytes are emitted | This preserves native routing preconditions; it does not supply distributed index promotion |

The former scalar metadata lookup shared the partition admission mutex. It now
validates exact complete-batch membership in stable read-only entries. A new group
is registered with one release-published prefix after every slot is initialized.
Readers never clone the complete history map or increment a per-entry Arc count.
Value bytes remain in their existing authoritative WAL records. Descriptor
validation remains mandatory before the bounded record read.

`max_reference_metadata_bytes` charges the directory root, reserved immutable
segments and descriptor slices before authority bytes are emitted. It excludes
allocator bookkeeping, temporary input, the separate retry/version maps and general
process RSS. Those maps still have a lifetime batch-count bound. Reaching either
limit rejects new writes before effects; it does not evict uncertain or old retry
identities. Sustained service still needs a proved retention/rotation protocol.

`CompletionInbox` carries outcomes to one authority owner without a global handoff
mutex. A timed-out or dropped producer leaves an unknown outcome, and a late
durable completion can still arrive. Reissuing a ticket requires the previous
producer to have relinquished its exact original binding. The owner polls only
outstanding slots with an explicit scan budget; finished history remains retained
without being scanned on every poll. This avoids a shared completion-map lock; it
does not eliminate network, persistence or logical-gap waits.

Review also tightened two authority boundaries. Native uniform-cell routing checks
the full cell-count product before authority eligibility, rejecting overflow or
an unrepresentable cell domain instead of discovering a routing panic after
durability. Direct partition append marks its writer poisoned before native WAL
allocation and clears that state only after successful metadata registration and
frontier updates. A caller that catches an allocation/copy/publication panic cannot
then append another group behind an uncertain tail. A direct-append failpoint test
checks later append, persistence and sealing refusal; worker-level panic handling
alone would not cover direct callers.

## Choose structures according to the layer

| Layer / information | Selected or preserved structure | Reason and reconsideration trigger |
| --- | --- | --- |
| Global control-plane membership/placement/epoch | Coherent immutable snapshot is the target; existing ArcSwap is a suitable candidate, not wired in this slice | Updates are infrequent and require a consistent epoch/map view. Do not copy the growing receipt history on each request. |
| Per-database logical sequence | Existing local atomic counter | Uniqueness is cheap; admission, fencing and completed frontiers still require explicit protocols. |
| Per-database admitted operation state | Dense vector plus exact randomized hash lookup | Admission is consecutive and retained; no sparse-sequence tree or readiness bitmap is needed to find the next complete state. |
| Per-request completion handoff | One producer capability, atomic unknown/ownership flags, immutable result cell | Independent workers report without mutating shared authority maps. Polling/wakeup scheduling is a separate concern. |
| Node-local identity retries | Exact `HashMap<BatchId, ...>` | No ordered ID query is needed. Full equality is checked even when hashes collide. |
| Node-local logical recovery order | `BTreeMap<BatchVersion, BatchId>` | A partition receives sparse versions; ordering/range traversal are useful. |
| Node-local physical read membership | Bounded segmented append-only directory ordered by physical offset | Stable entries with no removal, no whole-map copy and no read mutex. Sparse offsets consume no per-offset empty slots. |
| Native pending publication | Generic existing native pending tables plus shared transaction status | Reuses known complete-batch eligibility rather than inventing per-record publication. Caller ownership still protects table mutation. |
| Native mutable index overlay | Existing per-cell ordered overlay and short sharded synchronization | Preserves repeated-key history, tombstones, ordered iteration and checkpoint cuts. |
| Native compact/unloaded index | Existing compact sorted buffers and persisted cell levels | This checkout already has index-only LSM organization. Distribution requires new reference/format support, not automatically a different tree. |
| Future large persisted retry/reference history | Undecided; index-only LSM or another persistent directory remains plausible | Reconsider when history exceeds the bounded development epoch and its recovery/expiry rules are defined. Keep values in the authoritative WAL. |

This selection uses several structures because their keys and mutation patterns
differ. Replacing them all with a sharded LSM would introduce persistence and
compaction where only append-only metadata is needed. Replacing them all with
Roaring would lose identity and receipt payloads. Neither proposal automatically
settles whole-batch authority or cross-node publication.

### When to reconsider other structures

- **Roaring or dense bitmaps:** measure sparse replica coverage, live-segment
  intersections or ready-slot scans. Use a bitmap as a set summary only when it
  removes material work and cannot be mistaken for authoritative absence proof.
- **ART:** compare against the native overlay if real prefix/skew workloads expose
  tree-comparison or memory costs. Include full reference payload and version
  history rather than comparing empty tree nodes alone.
- **Bw-tree:** consider a separate overlay experiment if measured cell-lock
  contention dominates after I/O is outside those critical sections. Include delta
  consolidation, epoch reclamation and checkpoint/whole-batch semantics.
- **RCU/ArcSwap:** coherent routing updates already fit immutable replacement.
  A mutable or reclaimable value directory would need a reader-lifetime protocol;
  dropping a RAM entry does not authorize deleting remote WAL storage.
- **Concurrent skip list:** consider when ordered concurrent updates are actually
  needed. Individual atomic map operations do not provide the native transaction
  flag, a coherent snapshot or retention guarantees.
- **Persistent index-only LSM:** useful for metadata larger than RAM with an
  explicit rebuild/expiry protocol. Do not move large values out of their long-term
  WAL locations just to obtain an LSM label.

No Roaring, ART, Bw-tree, skip-list or queue dependency was added speculatively.
The structure changes use existing/native facilities and Rust standard-library
collections and synchronization.

## Format and compatibility

| Format | Current value | Compatibility |
| --- | --- | --- |
| Experimental partition registration/seal metadata | **3** | Explicit reference-metadata budget changes the registered configuration; previous v1/v2 directories are refused, without in-place migration |
| Complete experimental batch codec | **2** | Existing independently verified record descriptors remain unchanged |
| Complete group codec | **1** | Complete-group authority and one-pending-group recovery rule remain unchanged |
| Logical-value identity payload codec | **1**, 20 bytes | Separate candidate identity format; it does not redefine native index serialization |
| Ordinary native database/index format | Unchanged | Default native backend remains the comparison path |

The logical identity is wider than the native 12-byte encoded `WalPosition`.
Directory indirection can avoid repeating full physical descriptors in each index
entry, but adds its own memory and recovery costs. Evaluate index **plus directory**
and cold lookup behavior together before adopting a serving format.

## Correctness anchors

The [Tidehunter paper](../../../middleware/misc/tidehunter.pdf), §§3.1–3.4 and §4.4,
and native `batch.rs`, `wal_replay.rs`, `index/pending_table.rs`, `large_table.rs`,
checkpoint and relocation code remain the primary semantic anchors: retain values
in the WAL, recover whole atomic batches, preserve original logical order, keep
completed frontiers separate from allocation and preserve snapshot/reader history.

The detailed documents above link the primary implementations/papers behind each
comparison: Rust collections/OnceLock, append-only-vec, ArcSwap, Crossbeam,
WiscKey, RocksDB BlobDB, ART, Bw-tree and Roaring. Their mechanisms justify
specific candidate structures; none establishes performance parity for this fork.
The existing CORFU, DynamoDB and TiKV references still inform fenced inventory,
coarse global control and bounded node-local execution.

## Local evidence and next gates

Final results after the routing-bound and direct-append panic fixes:

| Scope | Result |
| --- | --- |
| Immutable reference directory | **6 passed**, including interrupted publication, exact metadata capacity and forged-reference rejection |
| Integrated storage tests | **19 passed**, including direct-append panic poisoning before another group can be emitted |
| Publication tracker | **9 passed**, including reverse completion behind an unknown gap |
| Completion inbox | **6 passed**, including producer loss, exact retry, bounded polling and completion/resume races |
| Generic native pending tables | **3 passed**, including existing native concurrency coverage |
| Logical identity codec/native pending use | **2 passed** |
| Prepared native publication bridge | **3 passed**, covering multiple cells/keyspaces, repeated keys, errors/panics and foreign durability receipts |
| Routing before authority eligibility | **2 passed**; unrouteable mutations and invalid uniform-cell domains are rejected before authoritative WAL append |
| Complete feature-enabled native library | **460 passed, 0 failed, 6 ignored**, 26.47 s; ignored tests are not counted as verification |
| Default native library | **381 passed, 0 failed, 6 ignored**, 35.01 s |
| Feature-enabled native crate, all targets | `cargo check` passed |
| Formatting and patch whitespace | `cargo fmt --all -- --check` and `git diff --check` passed |
| Freshly built updated example | **5 partitions, 80 complete batches, 160 native pending operations, 480 scalar reads**; unknown-prefix blocking and sealed-inventory pending replay verified |
| Example memory and swap | Peak RSS **9,036 KiB**; process swaps **0**; system `pswpin`/`pswpout` both stayed **0 → 0** |

Focused rows overlap with the complete-suite total and must not be added to it.
Suite duration is not a throughput measurement. Fresh verification logs are in
[`logs/implementation-structures-2026-10-06/`](../../logs/implementation-structures-2026-10-06/):
`test-feature-lib.log`, `test-native-lib.log` and `check-all-targets.log`. The default
native suite ran before the final two feature-only fixes; those fixes do not alter
the default native path. The final feature suite, all-target check, example build
and example run include both fixes.

Verification commands, from this repository root:

```sh
CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --features experimental-distributed --lib -- --test-threads=2
CARGO_BUILD_JOBS=2 cargo test --offline -p tidehunter --lib -- --test-threads=2
CARGO_BUILD_JOBS=2 cargo check --offline -p tidehunter --features experimental-distributed --all-targets
CARGO_BUILD_JOBS=2 cargo build --offline -p tidehunter --features experimental-distributed --example sharded_wal_local
target/debug/examples/sharded_wal_local
```

The updated five-partition example now exercises real composition of the new
components: 80 whole batches with 160 put/delete operations, exact completion
tickets, unknown sequence zero while later responses arrive in reverse order,
blocked publication until that gap resolves, then ordered publication through
the actual native pending transaction machinery. It validates each emitted pending
reference against its source WAL record. After sealing and full-inventory recovery,
it gathers receipts from the recovered inventory, sorts them in logical order,
replays them through the same pending bridge and checks those references again.
Original inputs are a verification oracle, not the source of the recovery inventory.
The freshly built example passed all these checks. The example-only cell owner
does not implement `LargeTable` promotion, persisted indexes, checkpoint restoration
or a serving native `Db`.

The debug correctness run took **0.30 s** and observed **79 groups / 79 explicit
group persistence calls**. Group composition depends on scheduling; 79 is an
observation, not a batching invariant. This small run is not a native/distributed
speed comparison or evidence of cluster throughput.

Fresh run evidence in the same ignored log directory:

- [`build-local-example.log`](../../logs/implementation-structures-2026-10-06/build-local-example.log)
- [`local-example.stdout.log`](../../logs/implementation-structures-2026-10-06/local-example.stdout.log), with explicit backend identity and counts
- [`local-example.stderr.log`](../../logs/implementation-structures-2026-10-06/local-example.stderr.log), with resource-use and exit-status evidence
- [`source-binary-manifest.json`](../../logs/implementation-structures-2026-10-06/source-binary-manifest.json), timestamp `2026-10-06T07:53:46Z`, source hashes including uncommitted changes above `13a16ca8bd632494fc6368f56d60d0806be4a869`

Final example binary SHA-256:
`c3cf08eae609d44cdde286dc5edd58d9e034ce6d0f0e1707d12615904c11996c`.
The head commit alone does not identify these uncommitted implementation changes;
the source/binary manifest binds the tested state.
Current Rust/Cargo source hashes and the example binary were checked against that
manifest after the final run. This slice changes no dependencies.

The immediate remaining work is coordinated M3/M4 integration: adapt native flat
and persisted index formats, version comparisons and checkpoint retention; connect
the real `LargeTable` publication sink and cache health gates; rebuild the native
index from complete fenced inventory; persist distributed snapshot cuts. Those
gates must pass before describing this fork as a serving distributed `Db`.
Authenticated TCP and explicit Sui backend selection follow. RDMA remains a later
transport experiment sharing the same authority and lifetime rules.

All evidence here is from bounded local correctness work. The laptop has one SSD
and no assumed RDMA hardware. No cluster speedup, native/Sui parity, or closure of
the prior cluster performance gap is established.
