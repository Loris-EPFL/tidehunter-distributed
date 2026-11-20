# Distributed Tidehunter architecture: leased NVMe-oF only

This is the single current description of the distributed design. All earlier
designs (owner-RPC payload path, Reserved writes, sealed images, SPDK,
`kernel_direct`, OnDisk/MemoryOnly receipt history, EFA/EFA-direct, per-cell
index, the Exact coordinator index) are removed; the first group survives at
tag `archive/multi-design-*`. Sections 1, 4, 5, 6, 7, 8, 10 and 12 describe the
native-index hybrid and cite files, not line numbers; other anchors may lag.

## 1. Roles

| Role | Holds | Anchor |
|---|---|---|
| Coordinator | The native index stack: `LargeTable`, `IndexFlusher`, the local index WAL, the control region and snapshots, over version-carrying positions | `tidehunter/src/distributed/hybrid/` |
| Owner | One Value WAL per partition | `tidehunter/src/distributed/storage.rs` (`PartitionLog`) |

An owner process serves exactly one database: one partition, one shape, one
live-target subsystem. The owner host can serve several databases
(`tcp/host.rs` `OwnerHost`, `max_databases`, default 16), each under
`<root>/db-<32 hex>/` with its own NQN that names the database id.

Writes go as whole frames over a leased NVMe-oF kernel target
(`nvmeof/live_target.rs`: file-backed nvmet namespaces, `buffered_io=1`) with
`per_owner` live exports (`tcp/live_client.rs`). Control and metadata RPCs
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

`RemoteDatabaseConfig` (`remote_db.rs`) is `deny_unknown_fields`. A key of a
removed setting (for example `history_persistence`, `checkpoint_mode`,
`replay_limits`, `max_index_bytes`, `scan_prefetch_pages`, `index_shards` or
`compactor_cadence`) fails the parse. A manifest of a removed mode fails the
open with a named legacy-mode error (`unsupported legacy distributed mode
<name>: use archive/multi-design`). The authority manifest is format 10; the
Exact-era format 9 is refused by name before any fence. The index settings are
the `index` field (`HybridIndexConfig`); Sui fills it from its native
configuration (`RemoteDatabaseConfig::with_native_index`).

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

The coordinator resolves a key through the native index to a position. A
position is 48 bytes: batch version, operation index, member CRC-32 and the
physical address of the member frame (`hybrid/position.rs`). Order and equality
of positions use the logical part only; every compare-and-swap uses the
physical part. The value reader (`hybrid/read.rs`) checks the coordinator
value cache first (`remote_db/read_cache.rs`). On a miss it fetches exactly the
member frame through a bounded live read lane (`remote_db/pending_read.rs`): the
lane obtains an authenticated owner mapping, opens the verified namespace and
issues aligned block reads through the buffered live kernel target. The reader
checks the CRC-32, decodes the native entry and caches the value bytes. The
current extent and historical sealed WAL files read the same way, through grants
and pins with revoke/drain (`nvmeof/live_namespace.rs`, `tcp/live_read.rs`).
Grants are keyed by the position's address. A read that finds its file retired
re-resolves once through the index. A second failure returns the typed error
"value evicted during read". Flights are deduped (`pending_read.rs`
`FlightRegistry`); an inline path serves a scalar read on the caller thread
when its lane is idle and its grant is healthy and free
(`pending_read/live_inline.rs`). Scans are native live iterators over the index.

## 5. Recovery

Recovery is native recovery over a remote value WAL. The coordinator loads the
control region (V4; a version 3 or an unversioned region fails with a named
error). The region names, per owner stream, the byte offset where replay
starts, and per cell, the byte position up to which the cell is stored. The
coordinator then reads each owner stream (`hybrid/replay.rs`), merges the
streams in batch-version order and hands whole batches to the index. A batch at
or below a stored position applies again with no change (idempotent by
version). Replay has no length limit, as in native: the suffix is bounded only
by the snapshot cadence (`snapshot_written_bytes`, 8 GiB in Sui).

Native-tier recovery is version B: `DurablePrefix` + `Extend`
(`remote_db/durable_cut.rs`, `durable-cut.v1` record). The cut is an exact
prefix of the coordinator version order; `extend` keeps the unbroken run of
surviving versions above it. Replay applies a per-fencing-generation filter: it
drops the unacknowledged tail of an older generation and keeps draining.
Synchronous owners are per-batch durable and are never truncated. A stored
region names only positions that are owner-synced or aborted. Owner recovery
(`storage/wal_replay.rs`) validates everything inside the persisted root,
adopts complete valid frames beyond it across skip markers, and stops at the
first invalid frame, as native replay does. The recent-receipt window
(`storage/history.rs`) serves retries only and is never persisted.

| Owner event | Result | Operator action |
|---|---|---|
| A passed WAL file is not yet sealable | Not an error. The file waits for the next background seal kick. | None. |
| The `fdatasync` of a passed file, or the directory sync, fails | The owner stream is poisoned (`StorageError::Io`). No seal is committed and the root is unchanged. A retry could certify bytes that the kernel dropped. | Restart the owner. The open re-reads the previous root. A clean `seal_ready_files` then seals the file. A sync that keeps failing means a failing device: replace it. |

An owner seals a passed file only after an `fdatasync` of the file and a
directory sync. It then installs the seal and `synced_end >= end` in one root
write.

## 6. Control plane

Transports: `Tcp` (mTLS), `TcpTrusted` (loopback tests and local benchmarks)
and OFI verbs control (`remote_db/transports.rs` `connect`, `tcp.rs`).
Protocol 35 (`tcp.rs` `PROTOCOL`). Every request/response variant has an
explicit documented wire tag, frozen by `tcp/wire_tag_tests.rs`. Protocol
history: 29 removed the owner-RPC payload path; 30 added unlimited fencing
generations (L1); 31 file inventory; 32 retirement proofs; 33 logical relocation
pages; 34 L2 provision/drop; 35 keyed `ResolveLiveRecords` by the member
address and added `durable_end` to the inventory sweep. Remaining classes:
control, live lease/grant/pin, lifecycle, durable progress, file inventory,
relocation, host provision/drop.

`ResolveLiveRecords` is keyed by address alone. The owner checks that each
address names a range inside a live file of its own partition. The address has
a partition, a generation, an offset inside one file and a length of at least
the CRC header. The owner pins that range. It does not check member
boundaries. It does not check that the range holds the batch the caller
expects. The coordinator holds that expectation: it verifies the member CRC-32
from the
index position. A relocated position keeps its logical epoch but names the
generation that now holds its bytes, so the generation is never inferred from
the epoch. A retired range answers `NotFound`.

## 7. Lifecycle

- **Fencing generations (L1):** allocated from `next_generation` in the
  authority manifest and never reused. An older generation leaves the manifest
  once it is retired and unpinned (`remote_db/fencing_generations.rs`,
  `tcp/inventory.rs`; no `max_epochs` limit).
- **Owner inventory:** partition high-water, generation seals and file
  inventory pages (`tcp/inventory.rs`).
- **Export revoke/drain and retirement:** a live file retires only when no
  lease, no retained reference and the allocator passed it
  (`storage/{file_seal,file_retirement,retired_files}.rs`).
- **Provision and drop (L2):** `ProvisionDatabase`/`DropDatabase` over
  authenticated control; the coordinator writes a durable drop intent, owners
  fence, revoke exports, drain readers, delete, then sync the directory
  (`tcp/host.rs`, `remote_db/authority_lifecycle.rs` and its `drop_tx`).

### Stream bound and the residual case

The control region names one stream per owner and retained generation. The
bound is 4096 streams (`remote_db/stream_positions.rs`): `owners x (retained
generations + 1)` must not exceed it. The bound is a failure boundary, not a
quota. Sizes: at 2 owners, 2047 retained generations per owner; at 16 owners,
255.

The open runs in this order: seed from the control region, check the bound,
reserve the new generation, then trim what is eligible. The trim runs only at
open, after the generation is reserved (`fencing_generations` `maintain`, one
caller). No trim runs while the database is open.

- An open at bound−1 reserves its generation and trims what is eligible.
- A coordinator that finds the database at the bound refuses the open with a
  typed error. The error names the owner count, the retained count and the
  4096-stream bound. The manifest stays unchanged and no owner is fenced.
  There is no in-band recovery: a refused open runs no relocation and no trim.
  The open refuses even when some generations became eligible for trim after
  the last open, because the trim needs the new fenced generation.
- The warning is the gauge `retained_stream_pressure` (75 % of the bound), with
  `retained_streams`. At 75 % the open also queues a priority relocation, which
  retires old generations while the database still opens.
- Operator action: act on the gauge while the database still opens. If a
  database is found at the bound, the open stays refused until a separate
  offline fenced trim tool runs. That tool does not exist. It is built only if
  the gauge fires in a golden or qualification run. An online generation trim is
  a possible later design (`checkpoint-work/research/ONLINE-GENERATION-TRIM-2026-11-20.md`).

## 8. Capacity

Relocation runs native `WalBased` semantics asynchronously on one background
worker per database (`sui/relocation.rs` `RemoteRelocator`, `Db::start_relocation`
in `sui.rs`). It evaluates the Sui filters on the native index, copies each
surviving value to the current generation, waits until the owners made the
copies durable, re-points the index entry with a compare-and-swap on the exact
old physical position, stores a control region, and only then retires dead
sealed files. A pass that cannot prove a stored region at its cut retires
nothing. It takes no exclusive write gate; foreground writes keep the shared
gate. Range drop is one singleton `DropCells` batch member (native tag 6,
`WAL_ENTRY_DROP_CELLS`, `tidehunter/src/db.rs`) under the shared gate
(`codec.rs`, `sui.rs` `drop_cells_in_range`); crash proofs are in
`remote_db/{drop_cells_tests,drop_cells_crash}.rs`. The owner inventory
(`storage/file_inventory.rs`) reports logical length and allocated bytes per
file; it never deletes.

Gauges of the capacity path (exported by `sui.rs`, with `snapshot_latch_deferrals`):

- `retained_streams`: the number of owner streams in the table (section 7).
  `retained_stream_pressure`: 0 or 1. It is 1 from 75 % of the 4096-stream bound.
- `relocation_copy_deferrals`: passes that applied nothing because the owners
  had not made the copies durable in time. The relocator runs the pass again.
  It is a count, not a database failure.
- `retirement_latch_caps`: retirement runs that retired nothing because a held
  latch (a relocation pass's frozen cut, or the short NONE latch of a
  checkpoint pin) was below the stored cut. The check fails closed. A later run
  retries the files.

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
receipt history, the owner-RPC payload path, EFA/EFA-direct, the per-cell index
and the Exact coordinator index (RAM index, Exact checkpoints, retained views,
identity and index-byte admission, replay limits) are removed. See tag
`archive/multi-design-*` for the first group and the branch history before the
native-index hybrid for the Exact index. No re-implementation is planned.

Accepted weakenings against the Exact index (owner decision, 2026-11-20):

- The member CRC-32 replaces the reference digests as the value integrity check.
- Iterators are live; there is no retained view of an older cut for a scan.
- A retry of a batch that left the retry window applies again.
- After a restart, the retry window is empty until new batches fill it.

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

- Coordinator index: `hybrid/` (`position.rs` 48-byte version position,
  `loader.rs` cut fronts and allocation watermarks, `region.rs` control-region V4,
  `replay.rs` owner-stream merge and version B filter, `snapshot.rs` snapshot
  pass, `table.rs` the index stack, `read.rs` value reader, `config.rs`
  `HybridIndexConfig`). The native index code itself is in the engine root
  (`large_table.rs`, `flusher.rs`, `control.rs`, `wal_replay.rs`) and is
  generic over the position type.
- Coordinator: `remote_db.rs` + `remote_db/` (`write_core/` write path,
  `pending_read/` read lanes, `durable_cut.rs` version B,
  `fencing_generations.rs` L1, `stream_positions.rs` stream bound,
  `authority_lifecycle/` L2 drop, `relocation.rs` and `dead_wal_files.rs`
  relocation and file retirement, `r1_events.rs`, `read_cache.rs`).
- Sui adapter: `sui.rs` (open, drop, range drop, relocation entry;
  `sui/relocation.rs` relocation worker, `sui/drop_cells.rs`,
  `sui/coordinator_evidence.rs`).
- Owners: `storage.rs` + `storage/` (WAL, replay, seal, retirement, file
  inventory), `tcp/` (`control.rs`, `inventory.rs`, `host.rs` L2 host,
  `live_client.rs` lease, `live_read.rs`, `durable_progress.rs`),
  `nvmeof/` (`live_target.rs` kernel target, `live_{connect,io,pool,namespace}.rs`).
- Shared: `codec.rs` (batch members incl. `DropCells`), `native_publication.rs`,
  `reference_directory.rs`, `types.rs`, `durability/`, `model.rs`,
  `group_model.rs`.
- Fabric control crate: `tidehunter-fabric/` (OFI verbs endpoint).


