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
`max_pending_batches`, `max_pending_bytes`, `owner_append_occupancy`,
`coordinator_lease_ms`); the drain is the ack barrier. Foreground writes take the shared write gate only.
Pruning never takes it (section 8).

`RemoteDatabaseConfig` (`remote_db.rs`) is `deny_unknown_fields`. A key of a
removed setting (for example `history_persistence`, `checkpoint_mode`,
`replay_limits`, `max_index_bytes`, `scan_prefetch_pages`, `index_shards` or
`compactor_cadence`) fails the parse. A manifest of a removed mode fails the
open with a named legacy-mode error (`unsupported legacy distributed mode
<name>: use archive/multi-design`). The authority manifest is format 10. The
open refuses the formats 5 to 9 by name before any fence: format 5 is the
per-cell mode, and the formats 6 to 9 are the Exact index. Formats below 5 and
above 10 get the generic refusal with the format number. The open never
rewrites a refused manifest. A database that the archive branch created stays
on that branch. Open it there. The index settings are the `index` field
(`HybridIndexConfig`). Sui fills it from its native configuration
(`RemoteDatabaseConfig::with_native_index`).

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
value cache first (`remote_db/read_cache.rs`). On a miss it fetches the member
frame and nothing else. The fetch uses a bounded live read lane
(`remote_db/pending_read.rs`). The lane obtains an authenticated owner
mapping, opens the verified namespace and issues aligned block reads through
the buffered live kernel target. The reader checks the CRC-32, decodes the
native entry and caches the value bytes. The current extent and historical
sealed WAL files read the same way, through grants and pins with revoke/drain
(`nvmeof/live_namespace.rs`, `tcp/live_read.rs`).

The owner keys a grant by the position's address. A read that finds its file
retired re-resolves once through the index. A second failure returns the typed
error "value evicted during read". The registry (`pending_read.rs`
`FlightRegistry`) merges equal flights. An inline path serves a scalar read on
the caller thread when its lane is idle and its grant is healthy and free
(`pending_read/live_inline.rs`). Scans are native live iterators over the
index.

A key length or shape that the store cannot hold returns `Ok(None)`, as native
does: there is no read-side key-length rule, and the hybrid read path adds
none. Empty keys stay legal on hashed and variable-length keyspaces. The
parity row "zero-length and malformed-key reads" records the evidence.

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

The stored region names one row per serving stream and one row per retained
older generation (`hybrid/region.rs` `older_replay_from`). An older row is the
stream end as soon as the stored cut epoch is above that generation. Before
that, the row keeps its stored value and the counter
`older_rows_without_end` increments. The cut crosses the start of the next
generation only after the replay-dirty cells flush. That delay is the native
stale-cell lag (7.5 GiB in Sui).

Native-tier recovery is version B: `DurablePrefix` + `Extend`
(`remote_db/durable_cut.rs`, `durable-cut.v1` record). The cut is an exact
prefix of the coordinator version order; `extend` keeps the unbroken run of
surviving versions above it. Replay applies a per-fencing-generation filter: it
drops the unacknowledged tail of an older generation and keeps draining.
Synchronous owners are per-batch durable and are never truncated. A stored
region names only positions that are owner-synced or aborted. Owner recovery
(`storage/wal_replay.rs`) validates everything inside the persisted root. It
adopts complete valid frames beyond the root across skip markers. It stops at
the first invalid frame and discards the tail after it, as native WAL replay
does (owner decision, 2026-11-20). The recent-receipt window
(`storage/history.rs`) serves retries only and is never persisted. After a
restart the window is empty, exactly as on a single node. A retry of an
unknown version is then applied again as a new write (accepted, owner
decision, 2026-11-20).

| Owner event | Result | Operator action |
|---|---|---|
| A passed WAL file is not yet sealable | Not an error. The file waits for the next background seal kick. | None. |
| The `fdatasync` of a passed file, or the directory sync, fails | The owner stream is poisoned (`StorageError::Io`). No seal is committed and the root is unchanged. A retry could certify bytes that the kernel dropped. | Restart the owner. The open re-reads the previous root. A clean `seal_ready_files` then seals the file. A sync that keeps failing means a failing device: replace it. |
| The seal install fails, or a reopen finds that the sealed inventory no longer matches the surviving frames | The owner stream is poisoned (`StorageError::Io`). The next open reserves a new fencing generation. The files of the poisoned generation retire through the sealed-stream path (`storage/file_retirement.rs` `retire_sealed_stream_files`) after the recovery of the new generation covers them. | Restart the owner. Check the device before the new generation takes writes. |

An owner seals a passed file only after an `fdatasync` of the file and a
directory sync. It then installs the seal and `synced_end >= end` in one root
write.

## 6. Control plane

Transports: `Tcp` (mTLS), `TcpTrusted` (loopback tests and local benchmarks)
and OFI verbs control (`remote_db/transports.rs` `connect`, `tcp.rs`).
The protocol version is 37 (`PROTOCOL` in `tcp.rs`). Every request and
response variant has an explicit documented wire tag. `tcp/wire_tag_tests.rs`
freezes the tags.

Protocol history:

- 29: removed the owner-RPC payload path.
- 30: added unlimited fencing generations (L1).
- 31: file inventory.
- 32: retirement proofs.
- 33: logical relocation pages.
- 34: L2 provision and drop.
- 35: keyed `ResolveLiveRecords` by the member address. Added `durable_end` to
  the inventory sweep.
- 36: exports live as long as their files; removed the export pins.
- 37: added `RenewCoordinatorLease` and its granted-duration reply.

The remaining request classes are: control, live lease/grant/pin, lifecycle,
durable progress, file inventory, relocation and host provision/drop.

`ResolveLiveRecords` is keyed by address alone. The owner checks that each
address names a range inside a live file of its own partition. The address has
a partition, a generation, an offset inside one file and a length of at least
the CRC header. The owner pins that range. The owner does not check member
boundaries. It does not check that the range holds the batch the caller
expects. The coordinator holds that expectation. It verifies the member CRC-32
from the index position. A relocated position keeps its logical epoch. It
names the generation that now holds its bytes. The generation is never
inferred from the epoch. A retired range answers `NotFound`.

### Coordinator lease: one writer across machines

The distributed form of single Tidehunter's database lock. An owner grants a
time-bounded right to write the fenced authority epoch to one holder
(`RenewCoordinatorLease`, protocol 37; `tcp/coordinator_lease.rs`). The reactor
answers a renewal inline, ahead of the control queue, so an exclusive fence,
seal or trim cannot delay it. The coordinator holds one dedicated renewal
session per owner (`remote_db/coordinator_lease.rs`) and renews at a fifth of
the granted duration; every write worker checks its owner's validity stamp
before it submits a frame, so no lease work runs on the submission path. The
owner write-protects the epoch's exports once a grant passes its expiry plus a
one-second margin. A lapse on any owner is fail-stop for the database: later
writes fail and the application reopens. A failure of one owner therefore
stops the database, exactly as a disk failure stops single Tidehunter. There is
no degraded mode.

## 7. Lifecycle

- **Fencing generations (L1):** allocated from `next_generation` in the
  authority manifest and never reused. An older generation leaves the manifest
  once it is retired and unpinned (`remote_db/fencing_generations.rs`,
  `tcp/inventory.rs`; no `max_epochs` limit).
- **Owner inventory:** partition high-water, generation seals and file
  inventory pages (`tcp/inventory.rs`).
- **Export revoke/drain and retirement:** a live file retires only when it has
  no lease and no retained reference. The allocator must also be past the file
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

The open runs these steps in this order:

1. Check the bound (`check_stream_bound`). The check counts the retained
   generations of the manifest, not the seeded positions.
2. Reserve the new generation.
3. Run the fence sweep.
4. Seed the stream positions from the owner reports (`seed_positions`).
5. Trim the eligible generations.

The bound check runs before the fence sweep. The trim runs only at open, after
the reservation (`fencing_generations` `maintain`, one caller). No trim runs
while the database is open.

- An open at bound−1 reserves its generation and trims what is eligible.
- A coordinator that finds the database at the bound refuses the open with a
  typed error. The error names the owner count, the retained count and the
  4096-stream bound. The manifest stays unchanged and no owner is fenced.
  There is no in-band recovery: a refused open runs no relocation and no trim.
  The open refuses even when some generations became eligible for trim after
  the last open, because the trim needs the new fenced generation.
- The warning is the gauge `retained_stream_pressure` (75 % of the bound), with
  `retained_streams`. At 75 % the open also queues a priority relocation. That
  pass retires old generations while the database still opens.
- Operator action: watch the gauge while the database still opens. If a
  database is found at the bound, the open stays refused until a separate
  offline fenced trim tool runs. That tool does not exist. It is built only if
  the gauge fires in a golden or qualification run. An online generation trim
  is a possible later design. After a relocation pass, a command to the
  durable-cut actor forgets one retired generation. It runs only while the
  pressure gauge is set. It is not scheduled. It is not proven safe next to
  readers, grants and the relocation worker.
  The owner trim holds the exclusive control-queue slot for its whole run, so
  every other queued control waits behind it. A coordinator lease renewal is
  exempt: the reactor answers it inline, ahead of the queue (protocol 37). An
  online trigger must not be enabled until the deletion runs off the control
  queue. Today the trim runs only at open, before any serving session.

## 8. Capacity

Relocation uses the native `WalBased` strategy (`RelocationStrategy` in
`tidehunter/src/relocation/mod.rs`). It walks the WAL in order from the oldest
position. It runs asynchronously on one background worker per database
(`sui/relocation.rs` `RemoteRelocator`, `Db::start_relocation` in `sui.rs`).
A pass has six steps:

1. Scan each owner stream and stop at the first `StopRelocation` decision.
2. A filtered keyspace (Sui's shape) copies nothing and drops every entry
   below its terminal, as native's cutoff does; every other keyspace copies
   each live value below its own stream's target to the current generation.
3. Wait until the owners made the copies durable.
4. Re-point each copied entry with a compare-and-swap on the exact old
   physical position.
5. Store a control region.
6. Retire the dead sealed files.

A pass that cannot prove a stored region at its cut retires nothing. A pass
takes no exclusive write gate. Foreground writes keep the shared gate.

Range drop is one singleton `DropCells` batch member (native tag 6,
`WAL_ENTRY_DROP_CELLS`, `tidehunter/src/db.rs`) under the shared gate
(`codec.rs`, `sui.rs` `drop_cells_in_range`). Crash proofs are in
`remote_db/{drop_cells_tests,drop_cells_crash}.rs`. The owner inventory
(`storage/file_inventory.rs`) reports logical length and allocated bytes per
file. It never deletes.

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
- `retirement_busy_replies`: retirement requests the owner answered `Busy`
  because another lifecycle operation held the gate. The coordinator retries.
- `deferred_retirements` (`FileMaintenanceStats` in `tcp/inventory.rs`):
  retirement deferrals of the idle window. A file that became dead together
  with the last rolled file waits for the next retirement request after a lease
  adoption, or for the next open. The engine does not export this counter to
  the Sui metrics (open owner item).

These four counters mean "non-zero = defect or operator-visible state". The
first two use the dynamic `stat` label of the `tidehunter_adapter` metric in
the Sui adapter (`crates/typed-store/src/tidehunter_util.rs`, no registry
change). The last two are engine counters in `RecoveryStats`; only the
kv-research open record exports them.

- `retirement_missing_rows`: a retirement proof found fewer index rows than
  the files it covered. The retirement fails closed. Zero unless an invariant
  broke.
- `older_rows_without_end`: a region store carried an older-generation row
  whose owner end was unknown. Non-zero shows an owner stream without a
  reported end.
- `old_generation_replayed_from_zero`: the loaded control region named no row
  for a retained older generation, so the open replayed it from offset zero.
  Zero in steady state. Non-zero is legitimate only for a generation that
  opened and closed before any store, and is a defect otherwise.
- `stale_region_rows_skipped`: the loaded control region named a generation
  that this open does not retain (it was trimmed after the store). The open
  skips the row and the next store drops it. Non-zero is an operator-visible
  state after a trim.

`below_cut_skipped` is an engine counter in `RecoveryStats`, exported by
the same open record. It is information, not a defect: it counts batches below
the loaded control region's cut that the replay read again and did not apply,
because the stored index blobs already hold them. A batch is read again for one
of four reasons: the per-stream fragment floor, a retained generation with no
stored row that replays from offset 0, the dropped floor at the extent-queue
cap, or an older-generation row carried forward. Paper section 3.4: replay
starts at the snapshot's replay-from position. It is not part of `tail_batches`
or `tail_bytes`. Only batches at or above the cut reach the index.

### Index cell size and the index WAL fragment

- A hybrid index row is the key plus 48 bytes (the version-carrying position,
  `hybrid/position.rs`). A native index row is the key plus 12 bytes (the
  native WAL position).
- For 32-byte keys, one entry is 80 bytes against 44. For 8-byte keys, one
  entry is 56 bytes against 20.
- One L1 cell blob is one index WAL frame, so its size must stay below one
  index fragment. The fragment is the native `frag_size`. Sui sets it through
  its `thdb_config` (`crates/typed-store/src/tidehunter_util.rs`); the native
  `Config::default` uses 128 MiB (`tidehunter/src/config.rs`), and
  `HybridIndexConfig::default` uses 1 GiB.
- One fragment holds about 13.4 million entries at 32-byte keys, or 19.2
  million at 8-byte keys. The native index holds about 24.4 million or 53.7
  million. The hybrid cell thus holds about 55 % or 36 % of the native count.
- Sui takes the cell count of a keyspace from `default_mutex_count()`
  (`crates/typed-store/src/tidehunter_util.rs`; 1,024 release, 16 debug). A
  keyspace has `cells_per_mutex * num_mutexes` cells
  (`tidehunter/src/key_shape.rs`), and `default_cells_per_mutex()` is 1. One
  keyspace declares twice that count
  (`sui-core/src/authority/authority_store_tables.rs`,
  `default_mutex_count() * 2`), so it has 2,048 cells (32 in debug). At 709
  million rows, a cell of a 1,024-cell keyspace holds about 0.7 million
  entries, about 55 MB, or about 5 % of a 1 GiB fragment. A 32 MiB debug
  fragment holds about 419 thousand 32-byte-key entries per cell.
- Sharding is opt-in exactly as in native, through `index_auto_shard_threshold`
  (`TH_AUTOSHARD` in Sui, default false). A set threshold splits a cell into
  shards until each shard holds at most the threshold. With no threshold, a
  cell is not split, and the number of entries in one unsplit cell is then
  bounded by the fragment, not by a shard threshold. Use the setting for a
  keyspace with one cell, or with skewed keys.
- Native asserts beyond the fragment cap: the flusher stops with
  `Entry(n) is larger then frag_size(m)` (`wal/layout.rs`). No typed error is
  promised.
- The L0 blob is written whole (the native flusher non-promote branch, the
  same code in the hybrid). Its size is bounded by `l0_max_entries` (default
  `max_dirty_keys * 8`, so 8,192 rows) times the row size. With the 1 GiB
  production fragment this is far below a fragment: 8,192 rows of key plus 48
  bytes are under 1 MiB at 32-byte keys. A test with small fragments must set
  `l0_max_entries` as well as the shard threshold.
- The numbers for the exposed Sui keyspaces are not enumerated.

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
- Dispatch-instant observation is not available after the hybrid: the worker's
  position command, which carried that state, is gone, and `Command::Control`
  runs behind `client.wait_leased_control`. Tests use outcome-level proof
  instead. The parity row "dispatch-instant observation is not available after
  the hybrid" records it, including that the proof is code-level until the
  test runs on a compilable tree.
- `Client::checkpoint_position` (`tcp/checkpoint.rs:26`) is a retained
  test-observability path. It asks the owner for a synced position bound, and
  production code sends no such request after the hybrid. The wire variant
  `Request::CheckpointPosition` stays because the owner answers it.

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
  `authority_lifecycle/` L2 drop, `coordinator_lease.rs` the renewers,
  `relocation.rs` and `dead_wal_files.rs`
  relocation and file retirement, `r1_events.rs`, `read_cache.rs`).
- Sui adapter: `sui.rs` (open, drop, range drop, relocation entry;
  `sui/relocation.rs` relocation worker, `sui/drop_cells.rs`,
  `sui/coordinator_evidence.rs`).
- Owners: `storage.rs` + `storage/` (WAL, replay, seal, retirement, file
  inventory), `tcp/` (`control.rs`, `inventory.rs`, `host.rs` L2 host,
  `coordinator_lease.rs` the owner cell and lapse watchdog,
  `live_client.rs` lease, `live_read.rs`, `durable_progress.rs`),
  `nvmeof/` (`live_target.rs` kernel target, `live_{connect,io,pool,namespace}.rs`).
- Shared: `codec.rs` (batch members incl. `DropCells`), `native_publication.rs`,
  `reference_directory.rs`, `types.rs`, `durability/`, `model.rs`,
  `group_model.rs`.
- Fabric control crate: `tidehunter-fabric/` (OFI verbs endpoint).


