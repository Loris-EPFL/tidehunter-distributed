# Local database authority over sharded native Value WALs

Date: 6 October 2026. Local implementation and validation record.
The serving suite and two release comparisons have completed. Compatible
digest-provider and borrowed-key routing changes pass correctness validation;
their combined end-to-end performance benefit is not established by these runs.

This extends [the structure/publication groundwork](IMPLEMENTATION_STRUCTURES_2026-10-06.md)
into an explicitly selected **bounded local RF1 serving backend**. Its source
identity is `experimental-native-cell-sharded-value-wal-local-rf1`.
It does not change the default native `Db` or integrate Sui or network transports.

## What now serves actual requests

`distributed/local_db.rs::LocalDatabase` owns one logical database's admission,
counter, completion tracking, native pending publication and typed cell index.
Applications can submit complete put/delete batches, drain acknowledged history,
read values, check existence, scan ordered keys, capture/read checkpoints, flush
and unload the index, shut down and reopen.

Value bytes remain in each selected partition's native Value WAL. An index entry
contains a typed logical version and validated physical record reference; there is
no second map or database holding copies of application values. A write returns a
logical admission token from `submit`; successful `drain`/`write_batch` establishes
the configured local storage durability plus ordered index publication. Admission
alone must not be returned to an application as durable success.

One mutable database owner schedules multiple independent partition workers.
Requests within an admission window can execute on those workers concurrently.
The owner does not turn its logical counter into a remote atomic operation.
Transport, actual independent disks and several independent database authorities
are outside this local implementation.

## Source map and reused native mechanisms

| Source | Responsibility | Native/design connection |
| --- | --- | --- |
| `distributed/local_db.rs` | Integrates admission, stable retries, complete local inventory, storage workers, durable completions, ordered publication and reads | Applies the existing architecture's single-authority/partitioned-values boundary |
| `distributed/local_inventory.rs` | Durable complete local inventory, preparing/active epoch state and retained sealed histories | Local file ownership and explicit epoch transitions implement a local lifecycle; they do not provide remote fencing |
| `distributed/native_index.rs` | Native-routed typed cells, retained logical versions/tombstones, point/ordered lookup, checkpoint cuts and persisted/mapped sorted metadata images | Shares native pending transactions and versioned overlay/search primitives; values stay in the native WAL |
| `distributed/codec.rs` | Whole-batch encoding plus independently verified member references | The new distributed payload hash uses `blake2b_simd` 1.0.3 with the same BLAKE2b-256 outputs; native WAL CRC32 and existing native key hashing are unchanged |
| `index/versioned_overlay.rs` | Reusable ordered version vectors and search helpers | Extracted/shared with the ordinary native index rather than introducing an unrelated latest-value map |
| `index/index_table/mod.rs` | Existing native index uses shared overlay primitives with its physical references | Default native interpretation and formats remain native |
| `lock.rs` | Close the owned lock descriptor while retaining the local registry guard, then unregister | Fixes a native teardown race in directory ownership; this affects default native and experimental local opens |
| `distributed/native_publication.rs` | Prepare exact durable membership, stage every operation and commit one native pending transaction | Preserves complete batch eligibility and repeated-key operation ordering |
| `distributed/{publication,completion}.rs` | Separate durable/published prefixes; immutable exact completion handoff with retained unknowns | Earlier structure slice integrated into real serving authority |
| `distributed/{storage,reference_directory,worker}.rs` | Native WAL groups, durability, exact immutable record membership and bounded workers | Earlier WAL substrate now backs actual index reads and recovery |
| `examples/sharded_wal_bench.rs` | Fresh ordinary-native versus experimental-local workload and audits | Explicit code/acknowledgement identities and no hidden legacy middleware fallback |

The experimental index is an adaptation of native cell routing, transactions and
ordered index mechanisms. It has its own cell container and explicit image format.
It is **not** a completed genericization of legacy `LargeTable`, its flusher and
all existing `IndexFormat` paths. Keeping that distinction visible is necessary
when interpreting memory, snapshot and performance comparisons.

The compatible follow-up borrows index routing keys where possible and changes
only the experimental codec's BLAKE2b implementation. The dependency is enabled by
the existing `experimental-distributed` backend feature, with no separate hashing
flag or change to ordinary native hashing. Digest equivalence and v2-format tests
preserve the original domain, identity, descriptor and output bytes. A faster
provider is a performance hypothesis until measured; the completed whole-backend
comparison did not establish an overall benefit.
A separate paired provider probe showed modest, variable median reductions for
larger payloads and no consistent win across sizes. It is recorded separately
from the end-to-end comparison in the [benchmark report](LOCAL_BENCHMARK_2026-10-06.md).

## Writes, unknown outcomes and visibility

1. Validate keyspace/routing, batch limits, retry identity and authority health.
   Reserve database and index metadata capacity before allocating new authority
   work; a normal before-effects refusal must not create an unresolved sequence.
2. Preserve the original operation ID and place the whole atomic batch in one
   registered partition. No transparent retry with a new identity is allowed.
3. The partition worker appends a complete authority group, persists its file and
   directory, then supplies a durable per-batch receipt through its completion
   ticket. Grouping follows the already modeled one-pending-group rule.
4. The authority records durable outcomes independently of their arrival order.
   It stages each ready batch in native pending tables and commits it atomically,
   promotes its typed versions and advances the published prefix.
5. Application-visible reads use published index state. Unknown storage or
   publication outcomes make the serving authority require recovery; a timeout
   cannot be converted into an absent mutation or skipped sequence.

The mutable owner is serial by construction. Immutable storage membership reads
and completion handoff avoid their previous shared mutexes, while node workers
and native internals retain other synchronization. This is not a claim of formal
lock freedom for the complete database.

Admission now reserves metadata for already outstanding batches as well as the
new request, both in the native index and the selected partition's reference
directory. Refusal occurs before allocating another logical version. Ignoring
outstanding reservations could otherwise accept a sequence that cannot fit when
its storage completion arrives.

Existence checks use the native index-only decision where the keyspace's indexing
scheme does not require validating the original key. Reduced/hash-indexed keys
retain the necessary WAL key check. Ordered scans merge cell streams with a heap;
the typed index does not rescan every cell to select every returned key.

## Local lifecycle and replay

The local inventory stores its full registered partition configuration and schema
before a new epoch can serve writes. Local directory ownership and strict worker
joins exclude competing local authorities and writers. Reopening seals old
registered streams, enumerates the complete history, and starts a fresh epoch
only after the local lifecycle permits it. Preparing epochs remain recorded so
interrupted setup cannot silently disappear from the inventory.

Startup reconstructs the typed native index by sorting eligible complete batches
from **all retained registered WAL epochs** in logical order. It preserves stable
identities so an old committed retry resolves to its original mutation, while a
new operation uses the new epoch. Missing registered streams, duplicate authority
history and incompatible schema/configuration fail closed.

There is no distributed lease, metadata consensus or network partition tolerance
in this local ownership protocol. The directory lock is not a substitute for
fencing a writer on another host. There is also no acknowledged-data survival of
permanent loss of an RF1 storage device.

## Checkpoints, flush and memory limits

Checkpoints capture a published logical cut. The index retains every version and
tombstone needed to answer that cut, including repeated updates to the same key.
The checkpoint is bound to its database domain. Read references still undergo
exact storage membership and per-record integrity checks before values are served.

`flush_index` persists a derived sorted reference image and unloads corresponding
cell overlay data to its mapped representation. It does not rewrite values into
another store. `unload_index` currently performs that same flush/unload operation;
calling both in sequence would write two images.

These images are not currently a startup acceleration path. Reopen still performs
full retained-WAL replay and reconstructs authority membership before serving.
An image alone cannot authorize dropping old WAL history or deciding an unknown
operation absent. Reusing images as recovery checkpoints requires a complete
manifest/replay-cut protocol and further validation.

Admission count, per-partition batch/descriptor metadata, total native index
metadata and retained epoch count are explicitly bounded. Exhaustion rejects new
work; the implementation does not silently discard old retry identities,
checkpoint history or uncertainty to make room. No GC, relocation, source-address
reuse, replica promotion or asynchronous replication is enabled yet.

## Research anchors and remaining scope

The [Tidehunter paper](../../../middleware/misc/tidehunter.pdf), §§3.1–3.4 and §4.4,
and native `batch.rs`, `wal_replay.rs`, pending tables, native index cells and
checkpoint code remain the semantic anchors. The selected design preserves the
long-term Value WAL representation and complete batch replay while separating
logical order from partition-local byte offsets.

The [index structure decision](INDEX_STRUCTURE_DECISION_2026-10-06.md),
[immutable membership decision](REFERENCE_DIRECTORY_DESIGN_2026-10-06.md) and
[publication structure decision](PUBLICATION_STRUCTURES_2026-10-06.md) link the
primary WiscKey/BlobDB, ART/Bw-tree, Rust/ArcSwap/Crossbeam and Roaring mechanisms
reviewed for these components. Their design choices are not a claim of measured
performance parity. Coarse control/local scheduling and fenced complete history
remain informed by the existing DynamoDB, TiKV and CORFU references.

Remaining work includes durable snapshot-based replay acceleration, sustained
retention/relocation and reader lifetimes, authenticated TCP storage services,
remote fencing and explicit Sui backend integration. RDMA can later use the same
authority/reference contract, but no RDMA or NVMe-oF path is exercised locally.
The old middleware architecture and default native backend remain comparators.

## Validation record

The local-database tests cover
native differential history, repeated keys, checkpoint reads, mapped-index reads,
reopen and stable old retries, before-effects rejection without a sequence hole,
unknown persistence gating/recovery, checkpoint database identity and outstanding
metadata reservations. The source-bound integrating evidence is:

| Gate | Result | Evidence |
| --- | --- | --- |
| Initial feature-enabled library | 481 passed, 0 failed, 6 ignored; 47.48 s | [Baseline log](../../logs/local-db-2026-10-06/test-feature-lib.log) |
| Final feature-enabled library | 484 passed, 0 failed, 6 ignored; 25.86 s; includes digest equivalence and many-cell scan regressions | [Final log](../../logs/local-db-2026-10-06/test-feature-lib-optimized.log) |
| Default native library | 383 passed, 0 failed, 6 ignored; 48.47 s | [Log](../../logs/local-db-2026-10-06/test-native-lib.log) |
| Local database focused suite | 5 passed | [Log](../../logs/local-db-2026-10-06/test-local-db.log) |
| Typed-index focused suite | 4 passed, including the added many-cell scan case | Separate focused run; subsequently included in the final 484-test suite |
| Native lock teardown coverage | 7 tests passed in native and feature configurations | Integrating task's focused runs, also included in the final native/feature suites |
| Formatting and whitespace | `cargo fmt --all -- --check` and `git diff --check` passed | Integrating task |
| Initial release build | Passed, 53.73 s | [Baseline build log](../../logs/local-db-2026-10-06/build-release-benchmark.log) |
| Final release build | Passed, 16.14 s; fresh binary recorded before candidate campaign | [Final build log](../../logs/local-db-2026-10-06/build-release-benchmark-optimized.log) |
| Final library/examples check | Offline, locked, feature-enabled `cargo check` passed | [Final check log](../../logs/local-db-2026-10-06/check-final-lib-examples.log) |
| Bounded release campaigns | 72 cases per candidate, 144 total, every reopen audit passed; both exit 0 | [Completed baseline, candidate and all metrics](LOCAL_BENCHMARK_2026-10-06.md) |

The baseline campaign reached 130,524 KiB peak RSS; the follow-up reached
125,656 KiB. Both had zero swap-in/out delta.
It is a real serving benchmark, but still a short warm local experiment on one
shared NVMe drive. The benchmark manifest binds the uncommitted Rust/Cargo hashes
and fresh binary to this run. The new path has measurable performance gaps;
correctness success is not evidence of native-speed parity.
Final Rust/Cargo source and binary hashes still match the candidate manifest.
Both campaigns, logs and the separate provider probe occupy about 77 MiB total;
temporary database data was removed after each case.

The [benchmark contract](LOCAL_BACKEND_BENCHMARK_2026-10-06.md) identifies exact
durability modes and measurement limits. The laptop has one shared SSD; local
relative results do not establish independent-NVMe cluster speed, RPC/EFA behavior,
Sui performance or closure of the historical cluster gap. No AWS resources are
needed or launched in this local stage.
