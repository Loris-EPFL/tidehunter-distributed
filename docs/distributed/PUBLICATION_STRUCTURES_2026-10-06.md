# Publication, completion and authority data structures

Date: 6 October 2026. Scope: the bounded experimental database authority.
This supplements [NEXT_STEPS](NEXT_STEPS_2026-10-06.md); it does not claim that
ordinary `Db`, Sui, remote transport or failover now use the new components.

## Required properties and actual access patterns

The local [Tidehunter paper](../../../middleware/misc/tidehunter.pdf), §§3.1,
3.3–3.4, makes whole-batch replay and completed frontiers part of correctness.
Native `wal/tracker.rs::WalTrackerState::add_processed` retains out-of-order
physical allocations until their predecessor finishes. Native
`index/pending_table.rs::Transaction` separately commits pending index mutations.
An allocated version, copied bytes, durable receipt and successful publication
callback are four different events.

Our current `distributed/publication.rs` admits exactly sequences `[0, N)` in
one fenced epoch, with `N <= capacity <= 1,000,000`. It retains every identity
until the epoch is discarded through a separate recovery protocol. Receipts can
arrive in any order. Queries use either one exact operation ID or the next
sequence. There are no sequence range searches, no insertion into the middle,
no lawful live hole insertion and no lawful retained-identity eviction.

These constraints determine the following implementation choices.

## Implemented publication representation

| Responsibility | Previous representation | New representation | Reason |
| --- | --- | --- | --- |
| Admitted sequence → complete state/receipt | `BTreeMap<u64, Entry>` | `Vec<Entry>` indexed by dense sequence | Contiguous admission makes a search tree unnecessary. |
| Exact operation identity → sequence slot | `BTreeMap<BatchId, u64>` | `HashMap<[u8; 16], usize>` | Exact lookup only; one fixed database domain is checked before lookup. |
| Contiguous durable/published progress | Two scalar frontiers | Same two frontiers | The first unresolved batch must still block prefix progress. |
| Identity and receipt retention | Fixed lifetime count cap | Same cap; no eviction | Forgetting a published ID must not turn a retry into a new write. |

Rust's documented collection costs support constant-time vector indexing and
expected constant-time hash lookup, versus logarithmic tree lookup. Vectors have
amortized growth. These are operation-complexity facts, not a measured database
speedup. [Rust collection documentation](https://doc.rust-lang.org/std/collections/index.html).

The hash table uses the standard randomized hasher. It compares all 128 operation
bits; a hash match is not treated as identity equality. The tracker checks the
database separately, including `state`, `mark_in_flight` and `mark_unknown`, so
equal operation bytes from another database cannot alias. Randomization retains
the standard hash-collision defense for caller-supplied identities.
[Rust HashMap documentation](https://doc.rust-lang.org/std/collections/struct.HashMap.html).

Each entry now omits its redundant 32-byte `BatchId`; the identity table retains
the operation component, and the tracker retains the database. Receipts still
carry and validate their complete original ID, version and digest. A frontier
walk indexes each newly completed entry once, giving `O(N)` total forward
advances over an epoch. Each out-of-order receipt also does one check of the
current gap; repeated polling is not claimed to have zero cost.

Memory remains `O(N)` retained metadata. Vectors/hash tables allocate lazily;
requested sequence `u64::MAX` does not size either collection. Capacity is a
logical-entry bound, not an exact allocator/RSS byte budget. Allocation slack,
hash buckets and their control bytes remain. The old entry ID, sequence-map keys
and tree-node storage are removed, but the two implementations' total RSS is not
asserted from field sizes alone. This does not solve lifetime retry retention.

## Alternatives and where they may become appropriate

| Structure | Assessment for this authority | Reconsider when |
| --- | --- | --- |
| Dense bitmap | Represents only a Boolean fact. It cannot replace identity, digest, receipt, unknown state or callback health. A second bitmap would duplicate state here. | Sparse receipt payloads are moved elsewhere and profiling shows scans of readiness bits dominate. |
| Roaring bitmap | Useful compressed integer-set representation, with array/bitset/run containers. Our dense admitted slots already need full per-batch records and two frontiers; compression cannot remove those records. | Large sparse replica-coverage or snapshot-membership sets need intersections/differences, with measured density and update patterns. |
| `VecDeque` or fixed ring | Efficient front removal, but publication does not authorize forgetting retry identity. Ring overwrite would silently lose uncertainty or retry history. | An explicit durable retention floor and `Expired` retry contract permit prefix eviction. Slots then need generations to reject stale references. |
| Slab | Reusable integer slots help when entries are removed. Reuse is currently forbidden. Plain dense vector needs no free list. | A proved lifecycle permits arbitrary removal; use generational handles if tickets may outlive slots. |
| `BTreeMap` | Appropriate for ordered sparse keys/range queries. Native physical WAL offsets are variable-width and do need a different structure from our dense logical admission. | Sparse recovery inventory, predecessor/range queries or retained sequence ranges become common. |
| Persistent/sharded LSM | A possible future index for very large retained retry history. It adds persistence, read and compaction behavior that this volatile progress tracker does not need. | Durable retry history exceeds RAM and its rebuild/expiry protocol is defined. Keep values in their authoritative Value WAL; index metadata separately. |

The Roaring format uses different containers according to local density; its
benefit depends on the represented set. The original paper studies compressed
set operations. Neither source establishes an advantage for a dense vector of
rich per-operation states. This application's conclusion is our inference.
[Roaring format specification](https://github.com/RoaringBitmap/RoaringFormatSpec/),
[original Roaring paper](https://arxiv.org/abs/1402.6407).

`VecDeque` is a growable ring, and `slab` supplies reusable indexed storage;
neither supplies a safe distributed retention protocol.
[VecDeque documentation](https://doc.rust-lang.org/std/collections/struct.VecDeque.html),
[slab documentation](https://docs.rs/slab/latest/slab/).
RocksDB's sorted persistent storage and compaction machinery is useful context
for a future disk-backed metadata index. It does not remove ordering or unknown
outcomes from our protocol.
[RocksDB overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview).

## Mutex-free completion handoff implemented now

`distributed/completion.rs` introduces a bounded `CompletionInbox` owned by the
authority and one `CompletionTicket` for each dispatched logical batch:

```text
authority owner                         independent storage execution
  register immutable binding ───────────► unique producer ticket
  retain slot + retry identity              │
  poll outstanding slots ◄─────────────────┼─ unknown atomic flag
  feed PublicationTracker ◄───────────────┴─ OnceLock<DurableBatch>
  publish native transaction when ready
```

There is no mutex in this component. The owner alone changes registration,
retention and polling lists. `OnceLock::get` never blocks; it returns no value
while initialization is incomplete. Its `set` can generally block with competing
initializers, so this API prevents competing producer tickets rather than relying
on that behavior. It uses standard Rust synchronization with no new unsafe code
or dependency. [OnceLock contract](https://doc.rust-lang.org/std/sync/struct.OnceLock.html).

Important boundaries:

1. `register` accepts consecutive already-validated bindings and rejects before
   storage dispatch when full. The enclosing authority must reserve all relevant
   capacity before allocating a logical version. Rejected registration is never
   a hole permitting later sequences to pass.
2. The ticket's consuming `complete` validates the exact database, operation ID,
   original version and payload digest. The receipt becomes immutable.
3. `report_unknown` does not terminate the producer: a timeout can precede its
   late durable receipt. Drop/panic of an incomplete ticket also marks unknown.
4. `resume_ticket(sequence)` reissues only the original binding after the old
   producer drops. An acquire/release producer flag prevents simultaneous
   initialization; a second receipt check handles completion racing this handoff.
   An active timed-out producer cannot be replaced by a competing one.
5. The owner tracks outstanding slot indices in a separate `VecDeque`.
   `poll(max_scanned)` inspects at most that many distinct outstanding slots,
   rotates unresolved indices to the back and removes delivered durable
   slots from the polling list, retaining their history. It emits an unknown
   notification once and may emit a later durable notification. Empty completed
   history is not scanned on each poll.
6. Polling order is not publication order. `PublicationTracker` must retain
   future durable receipts behind a gap. The native transaction callback remains
   responsible for complete index visibility and fail-closed callback errors.
7. Polling needs an external scheduler; unbounded busy spinning is not the
   intended default. An eventual authority reactor should use bounded polls
   around transport completions or an explicit wakeup mechanism. There is no
   wakeup, remote epoch lease,
   automatic recovery or bounded completion time. Retained unknowns can exhaust
   admission intentionally. No timeout/drop is reported as a definite abort.

The mutable owner is serial by construction, though storage execution is
parallel. The component is a mutex-free handoff, not a proof that the whole
database is lock-free or that an owner paused by the scheduler cannot delay
publication. Callback panic/error still requires recovery; faster metadata does
not settle a missing storage outcome.

## Global authority queues and snapshots: integration requirements

The logical sequence allocator remains an inexpensive local atomic. With one
owner, version allocation and `PublicationTracker::admit` can occur in the same
owned operation; independent workers never call the counter before obtaining
admission. A later concurrent reservation scheme needs explicit abandoned-slot
recovery. Atomic fetch-add by itself does not supply that protocol.

For concurrent admission messages, a bounded queue is a reasonable next
candidate. Crossbeam `ArrayQueue` has preallocated stamped slots and acquire/
release value publication; overflow returns the message. It is not a substitute
for byte credits, terminal outcome retention, scheduling or shutdown. Its
overwrite operation must never be used for write/completion records.
[Crossbeam queue source](https://docs.rs/crossbeam-queue/latest/src/crossbeam_queue/array_queue.rs.html).
The original bounded MPMC algorithm explicitly distinguishes its atomic
implementation from formal lock freedom; stalled producer reservations can delay
other work. A mutex-free queue is not automatically a lock-free authority.
[Vyukov's algorithm](https://sites.google.com/site/1024cores/home/lock-free-algorithms/queues/bounded-mpmc-queue).

`crossbeam-queue` is absent from the inspected Cargo lockfile. Existing
`crossbeam-deque`, epoch and utilities are transitive and do not expose
`ArrayQueue`. This slice adds no queue dependency. Any later replacement of the
worker's bounded mutex/condition-variable queue must preserve active-plus-queued
byte/request credits, definitive before-effects rejection, drain/join, dropped
response semantics and recovery/control progress.

Routing and inventory are different: relatively rare complete immutable updates
fit the existing `arc-swap` dependency (locked at 1.7.1). Publish one internally
consistent map containing its epoch and endpoints, then use a single loaded view
for each request. Avoid rebuilding/copying the entire publication or receipt map
on every batch. Long-lived readers should use an owned `Arc`; retaining borrowed
guards across async yields can exhaust fast guard slots. ArcSwap documentation
also identifies writer and reference-count costs; their absence from a mutex
profile would not make them free.
[ArcSwap performance](https://docs.rs/arc-swap/latest/arc_swap/docs/performance/index.html),
[ArcSwap guard limitations](https://docs.rs/arc-swap/latest/arc_swap/docs/limitations/index.html).

## Local validation and remaining work

The publication suite passes nine tests: all six prior semantic tests plus
untrusted-large-sequence admission, database/full-ID separation, and 1,025
retained batches with reverse durable responses behind unknown sequence zero.
The last test resolves that gap, publishes each receipt once in logical order,
retains exact retries and rejects new identities at capacity.

The completion suite passes six tests covering binding rejection, unknown/late-durable handoff,
producer panic/drop, exact retry, concurrent completion/resume, bounded rotating
polls and before-effects registration limits. The completion/resume test races
64 independent handoffs using actual durable receipts. This is a regression
test, not an exhaustive concurrency proof.

These tests use small temporary partitions on the laptop's shared SSD. They
establish exercised correctness and reduced data-structure work. They do not
measure cluster latency, native/Sui throughput parity or RDMA performance.
Next serving gates remain native logical-reference integration, health-gated
reads, complete epoch recovery, snapshots, authenticated transport and an
explicitly selected Sui backend.
