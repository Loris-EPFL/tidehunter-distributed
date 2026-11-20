# R6 refactor findings

Concurrency-audit findings from the R6 inventory §4 that need a main decision,
plus the confirmed no-change entries. Each entry names the site, the observed
behavior, the risk, and either the proposed fix or "no change". All anchors are
on engine tree `e6c532fd` (counter names as of that tree: `read_rpcs`; R6Prod's
rename to `live_read_submissions` lands after). The full table is
`checkpoint-work/research/R6-INVENTORY-2026-11-19.md` §4.

## Needs a main decision

### F1. Reference-directory `RwLock` on the point-read path

- Site: `tidehunter/src/distributed/reference_directory.rs:419`
  (`authority: RwLock<Authority>`), `:420` (`cache: Mutex<AuthorityCache>`).
- Behavior: every point read consults the read path through the shared
  `authority` RwLock. The critical section is short and no I/O runs under it,
  but the lock is one domain for all readers of the directory.
- Risk: contention only at high read concurrency; measured cost unknown.
- Proposed fix: none without numbers. Measure `read_rpcs` lane occupancy and
  `authority` wait counts in the local live bench first; shard by registered
  batch id only if the measured share exceeds the agreed threshold.

### F2. Per-lane `Client` mutex serializes live reads within one lane

- Site: `tidehunter/src/distributed/remote_db/pending_read.rs:943`
  (`client: Arc<Mutex<Client>>`) with `driver: Arc<Mutex<ReactorLane>>`
  (`:946`); the per-lane client type alias sits at `:696`
  (`UnreleasedLiveGrant`), and further client-holding sites are `:1721`,
  `:2884`.
- Behavior: within one read lane, reads that need the shared control client
  serialize on the `client` mutex; callers `try_lock` both `driver` and
  `client` and defer on failure.
- Risk: head-of-line delay inside a lane when many reads share one client
  session. Lanes are the intended parallelism unit, so cross-lane throughput
  is unaffected.
- Proposed fix: no design change now. The `driver`-before-`client` order is
  documented and callers never block on either lock. Revisit only if the
  `live_lane_full` and deferral counters show lane-internal serialization at
  the qualification workload.

### F3. Blocking `send` on the durable-cut actor inbox

- Site: `tidehunter/src/distributed/remote_db/durable_cut.rs:609`
  (`messages: mpsc::SyncSender<Message>`), capacity `owners + 8` (`:670`).
- Behavior: `Retire` and certification messages use a blocking `send`; no
  sender holds a lock, and the actor drains at most `owners + 8` messages per
  evaluation. Every poller sends at most one report per turn, so the bound
  holds for reports.
- Risk: if a stuck actor stops draining while more than `owners + 8` control
  messages queue, a sender blocks. Today only `Retire` and certify messages
  can exceed the report bound, and both originate from the single owner
  control worker, which cannot outrun itself.
- Proposed fix: none; the capacity comment (`durable_cut.rs:605`) documents
  owner and capacity. If a future producer outside the control worker is
  added, it must use `try_send` with an explicit drop policy. Decision:
  accept, or require that invariant as a test.

## Confirmed no change

### F4. Writer-side history leaf lock (L8b)

- Site: `reference_directory.rs:425` (`history: Arc<Mutex<History>>`).
- Behavior: writer-only leaf; not on the read path. Must not be taken while
  `authority` is write-locked by the same thread (lock-order note stands).
- No change.

### F5. Control flush channel (C1)

- Site: `tidehunter/src/distributed/tcp/control.rs:29-30` (`FLUSH_QUEUE =
  2 * MAX_COMMANDS + 16 = 144`), sender `Shared::flush`.
- Behavior: `flush.send` happens under the control queue lock; capacity
  exceeds one `Barrier` per control worker, so the send never blocks.
- No change; the capacity comment must stay beside `FLUSH_QUEUE`.

### F6. Reply channels of capacity one (C5)

- Site: `tidehunter/src/distributed/remote_db/durable_cut.rs:749`;
  `remote_db/capacity_observer.rs:274, 291`;
  `remote_db/checkpoint_lifecycle.rs:505` (one-reply control channels,
  `sync_channel(1)`).
- Behavior: exactly one reply per request; a late sender is dropped; callers
  use bounded `recv` (`control_until`).
- No change.

### F7. Cut-actor stop channel (C7)

- Site: `tidehunter/src/distributed/remote_db/durable_cut.rs:695`
  (`mpsc::sync_channel(1)`).
- Behavior: `DurableCut::stop` uses `try_send` and ignores a full channel;
  each poller is joined afterwards.
- No change.

### F8. Live drain reply channels (C9)

- Site: `tidehunter/src/distributed/remote_db/pending_read.rs:828`
  (`DrainLive(mpsc::SyncSender<...>)` in the read command enum), handled at
  `:1214` and sent with `try_send` at `:1669`; one reply slot per drain
  request.
- Behavior: one reply per drain request; `live_drains` counts accepted
  drains with release-ordered decrement; lane close ends them.
- No change.

### F9. `HubSender::advanced` drop-on-full (C2)

- Site: `tidehunter/src/distributed/tcp/durable_progress.rs:212`
  (`try_send(Command::Advanced)`), capacity `HUB_COMMANDS = 64` (`:27`),
  waiters bounded by `MAX_WAITERS = 16` (`:22`).
- Behavior: an `Advanced` notification is dropped when the hub inbox is
  full; this is correct because the hub re-checks frontiers after every
  drain. `Wait` falls back to `answer_now()` on full or disconnected
  (`:232`).
- No change; the drop-on-full rule must stay documented beside
  `HUB_COMMANDS`.

### F10. Live-lane failure attribution

- Site: `tidehunter/src/distributed/remote_db/pending_read/live_lane.rs:184,
  288, 322, 358` (`record_first_failure`).
- Behavior: the sites that the R6 inventory listed as poison-without-cause
  now record the first failure before they poison the lane.
- No change; keep the regression tests that assert the recorded cause.

## Deferred for numbers

### F11. Hot-path clones

- Sites: no per-line worklist is kept here; the measured files and the
  per-clone sites are in R6 inventory §3.4
  (`tcp/live_client.rs`, `remote_db/pending_read.rs`,
  `remote_db/write_core/publisher.rs`, `native_index.rs` — `.clone()` of
  `Vec`/`Bytes`/`String` on the put, get and replay paths).
- Behavior: clones exist where ownership forces a copy or where an `Arc`
  conversion was judged not yet worth it.
- Rule: any hot-path clone change needs before/after numbers from the local
  live bench, in a main-granted quiet window. Until then: no change.
