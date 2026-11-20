# Distributed Tidehunter architecture: leased NVMe-oF only

This is the single current description of the distributed design. All earlier
designs (owner-RPC payload path, Reserved writes, sealed images, SPDK,
`kernel_direct`, OnDisk/MemoryOnly receipt history, EFA/EFA-direct, per-cell
index) are removed; their documents survive at tag `archive/multi-design-*`.
Anchors are on tree `e6c532fd`.

## 1. Roles

| Role | Holds | Anchor |
|---|---|---|
| Coordinator | The RAM index and the Exact checkpoints | `tidehunter/src/distributed/native_index.rs`, `remote_db/checkpoint_lifecycle.rs` |
| Owner | One Value WAL per partition | `tidehunter/src/distributed/storage.rs:893` (`PartitionLog`) |

An owner process serves exactly one database: one partition, one shape, one
live-target subsystem. The owner host can serve several databases
(`tcp/host.rs:290` `OwnerHost`, `max_databases`, default 16), each under
`<root>/db-<32 hex>/` with its own NQN that names the database id.

Writes go as whole frames over a leased NVMe-oF kernel target
(`nvmeof/live_target.rs`: file-backed nvmet namespaces, `buffered_io=1`) with
`per_owner` live exports (`tcp/live_client.rs:768`). Control and metadata RPCs
run over TCP/mTLS or OFI verbs. History is WAL replay only.

## 2. Write path

Sequence: admission and grouping (`remote_db/write_core/{sequencer,owner_queue,write}.rs`)
→ lease of a live extent (`tcp/live_client.rs`) → whole-frame write over the
leased NVMe-oF extent (`write_core/worker.rs`) → owner page-cache publication
→ logical version (`write_core/publisher.rs`). A group is the completeness
boundary; one group holds whole batches with independent inner CRC frames.
Backpressure: bounded pending batches/bytes (`RemoteDatabaseConfig`:
`max_pending_batches`, `max_pending_bytes`, `owner_append_occupancy`); the
drain is the ack barrier. Foreground writes take the shared write gate only.
Pruning never takes it (section 8).

`RemoteDatabaseConfig` (`remote_db.rs:130`) is `deny_unknown_fields`; a key of
a removed mode (for example `history_persistence` or `checkpoint_mode`) fails
the open with a named legacy-mode error (`remote_db.rs:576`).

## 3. Acknowledgement tiers

Two tiers (`durability/{mod,native,synchronous}.rs`, `DURABILITY_TIERS.md`):

| | `native` (default) | `synchronous` |
|---|---|---|
| Ack when | owner **appended** frontier covers the batch | owner **synced** frontier covers the batch |
| Waits for fsync | no | yes (group commit) |

Sui maps `TH_SYNC_WRITES` to the tier. In the Sui fork
(`crates/typed-store/src/tidehunter_util.rs`, Sui tree `d2c2245f`; these anchors
are in the Sui repository, not the engine tree), the adapter parses it once in
`open` (`:91`) before any backend opens and passes it into `open_backend` (`:95`).

`check_leased_durability` (`:363-375`, called from `engine_config` at
`:346`) requires: unset or `false` → `live_nvme.leased_durability = native`;
`true` → `synchronous`. A mismatch panics:
`TH_SYNC_WRITES={sync_writes} requires leased_durability={required} for namespace {namespace}`.
No silent override in either direction. The engine rechecks each owner at
provision (`remote_db/authority_lifecycle.rs:200` installs
`live.leased_durability`) and at every leased grant
(`tcp/live_client.rs:1005-1010`, "leased durability mismatch: requested …,
owner …"). `set_native_durable` is native-only.

## 4. Read path

Coordinator value cache (`remote_db/read_cache.rs`) → on a miss, a bounded live
read lane (`remote_db/pending_read.rs`) obtains an authenticated owner mapping,
opens the verified namespace and issues aligned block reads through the
buffered live kernel target. The current extent and historical sealed WAL files
read the same way, through grants and pins with revoke/drain
(`nvmeof/live_namespace.rs`, `tcp/live_read.rs`). Flights are deduped
(`pending_read.rs` `FlightRegistry`); an inline path serves a scalar read on
the caller thread when its lane is idle and its grant is healthy and free
(`pending_read/live_inline.rs`). Scans use bounded page prefetch
(`read_pipeline.rs`, `scan_prefetch_pages`).

## 5. Recovery

WAL replay is the only history. Recovery validates everything inside the
persisted root, adopts complete valid frames beyond it across skip markers, and
stops at the first invalid frame — torn-tail acceptance, as native replay
(`storage/wal_replay.rs`, `remote_db/wal_replay_tests.rs`). The recent-receipt
window (`storage/history.rs`, 4096 batches) serves retries only; it is never
persisted.

Native-tier recovery is version B: `DurablePrefix` + `Extend`
(`remote_db/durable_cut.rs:617`, `durable-cut.v1` record). The cut is an exact
prefix of the coordinator version order; `extend` keeps the unbroken run of
surviving versions above it. Synchronous owners are per-batch durable and are
never truncated. The restart durability guard rewrites every validated frame
above the root before any new root is installed. Checkpoints load from the last
Exact root; the reference directory checks retained references and CRCs
(`reference_directory.rs`).

## 6. Control plane

Transports: `Tcp` (mTLS), `TcpTrusted` (loopback tests and local benchmarks)
and OFI verbs control (`remote_db/transports.rs:47` `connect`, `tcp.rs:10`).
Protocol 34 (`tcp.rs:134`). Every request/response variant has an explicit
documented wire tag, frozen by `tcp/wire_tag_tests.rs`. Protocol history: 29
removed the owner-RPC payload path; 30 added unlimited fencing generations
(L1); 31 R0 file inventory; 32 R1 retirement proofs; 33 R1 logical pages; 34
L2 provision/drop. Remaining classes: control, live lease/grant/pin, checkpoint
and lifecycle, durable progress, capacity inventory, relocation, host
provision/drop.

## 7. Lifecycle

- **Fencing generations (L1):** allocated from `next_generation` in the
  authority manifest, never reused, trimmed offline once retired and unpinned
  (`remote_db/fencing_generations.rs`, `tcp/inventory.rs`; no `max_epochs`
  limit).
- **Owner inventory:** partition high-water, generation seals and file
  inventory pages (`tcp/inventory.rs`).
- **Export revoke/drain and retirement:** a live file retires only when no
  lease, no retained reference and the allocator passed it
  (`storage/{file_seal,file_retirement,retired_files}.rs`).
- **Provision and drop (L2):** `ProvisionDatabase`/`DropDatabase` over
  authenticated control; the coordinator writes a durable drop intent, owners
  fence, revoke exports, drain readers, delete, then sync the directory
  (`tcp/host.rs`, `remote_db/authority_lifecycle.rs` and its `drop_tx`).

## 8. Capacity

Relocation runs native `WalBased` semantics asynchronously on one background
worker per database (`sui/relocation.rs:65` `RemoteRelocator`,
`sui.rs:1917` `Db::start_relocation`): it evaluates the filters on a pinned
cut, applies Remove decisions as index-only removals bound to the exact old
reference, persists them in the next Exact checkpoint, advances the floor, and
retires dead sealed files. It takes no exclusive write gate; foreground writes
keep the shared gate. Range drop is one singleton `DropCells` batch member
(native tag 6, `WAL_ENTRY_DROP_CELLS`, `tidehunter/src/db.rs:2244`) under the shared gate (`codec.rs:81`, `sui.rs:1994`
`drop_cells_in_range`); crash proofs are in
`remote_db/{drop_cells_tests,drop_cells_crash}.rs`. R0 shadow counters
(logical length, allocated bytes, retention blockers per file) never delete
(`remote_db/capacity_observer.rs:136`, `storage/capacity_inventory.rs`,
`storage/file_inventory.rs`).

## 9. Concurrency contracts

Lock-order chain on the owner: control gate → `LifecycleGate` → `NodeInventory`
→ `readers` (`tcp/control.rs:68`, `tcp/inventory.rs:117,224`, `tcp.rs:714`).
The lifecycle writer releases the inventory around every I/O step; no I/O
under the inventory lock. Read lanes: `driver` before `client`, callers
`try_lock` both (`remote_db/pending_read.rs:943`). Every channel names its
owner, capacity, full-queue policy and shutdown; see
`tcp/control.rs:27` (`MAX_COMMANDS`, `MAX_COMMAND_BYTES`),
`tcp/durable_progress.rs:22,27` (`HUB_COMMANDS`, `MAX_WAITERS`),
`remote_db/durable_cut.rs:605` (capacity `owners + 8`), `pending_read.rs:831`
(lane inbox bounded by flight credits). No unbounded channel exists. Findings
that need a design decision: `REFACTOR-FINDINGS.md`.

## 10. Non-goals / removed designs

Reserved writes, sealed images, SPDK, `kernel_direct`, OnDisk/MemoryOnly
receipt history, the owner-RPC payload path, EFA/EFA-direct, per-cell index:
all removed. See tag `archive/multi-design-*`. No re-implementation is planned.

## 11. Test surfaces

- Rootless leased ordinary-file fixture: `sui::Db::open_remote_with_local_live_fixture`
  (`distributed/sui.rs:689`). Sui side (Sui tree `d2c2245f`): per-namespace
  `local_live_fixture` flag (`tidehunter_util.rs:154-181`, stripped before
  engine parsing), gated
  on the typed-store `local-live-fixture` feature so the engine `test-utils`
  feature is required; loopback owners and ordinary-file live I/O, no sudo.
- Kernel NVMe/TCP loopback CI job: leased recovery on a real loopback target
  (`CLOUD_AND_CI_TESTING.md`).
- Real NVMe-oF/RDMA runs: CI kernel job and GCP only; no local sudo.

## 12. File map

`tidehunter/src/distributed/`:

- Coordinator: `remote_db.rs` + `remote_db/` (`write_core/` write path,
  `pending_read/` read lanes, `checkpoint_lifecycle.rs` Exact,
  `durable_cut.rs` version B, `fencing_generations.rs` L1,
  `authority_lifecycle/` L2 drop, `r1_events`/`checkpoint_attribution` R1,
  `capacity_observer.rs` R0, `read_cache/`, `read_pipeline/`).
- Sui adapter: `sui.rs` (open, drop, range drop, relocation entry;
  `sui/relocation.rs` R1 worker, `sui/drop_cells.rs`, `sui/index_checkpoint.rs`).
- Owners: `storage.rs` + `storage/` (WAL, replay, seal, retirement, capacity
  inventory), `tcp/` (`control.rs`, `inventory.rs`, `host.rs` L2 host,
  `live_client.rs` lease, `live_read.rs`, `durable_progress.rs`),
  `nvmeof/` (`live_target.rs` kernel target, `live_{connect,io,pool,namespace}.rs`).
- Shared: `codec.rs` (batch members incl. `DropCells`), `native_index.rs` +
  `native_index/` (Exact cells), `native_publication.rs`,
  `reference_directory.rs`, `types.rs`, `durability/`, `model.rs`,
  `group_model.rs`, `capacity/`.
- Fabric control crate: `tidehunter-fabric/` (OFI verbs endpoint).


