# Owner read admission without a node-wide mutex (6 November 2026)

Scope: `tidehunter/src/distributed/tcp/read_executor.rs`. The earlier review
(`ReviewFabricVerbs`, finding 6) found that every owner read passed through one
node-wide `ReadAdmission.state` mutex for reply admission, buffer reuse, stats and
device accounting. The review did not show that the lock was contended; it
asked for wait/hold evidence and for sharing nothing between sessions. This
change does both and keeps every budget the old lock enforced.

## Structure

| State | Before | After |
| --- | --- | --- |
| Reply request count | node mutex | `AtomicUsize`, bounded compare-exchange |
| Reserved reply bytes and idle buffer bytes | node mutex | one `AtomicU64` (reserved high 32 bits, idle low 32 bits), so reserve, evict and recycle each decide in one compare-exchange |
| Buffer reuse pool | one pool in the node mutex | one `SessionPool` per session; its mutex is a leaf and is used only by that session's executor and its outstanding response owners |
| Event counters (`wal_reads`, `completed_reads`, resident and async counts, errors, reuse) | node mutex | relaxed atomics in the session's pool; `stats()` sums live pools and a retired set that closed sessions fold into |
| Per-device in-flight read credit (`max_device_reads` per device, node-wide) | `HashMap` in the node mutex | 16 mutex-guarded shards keyed by device; one shard lock at a time, all-or-nothing with rollback |
| Diagnostic synchronous reads | node mutex | node-level relaxed atomics (they have no session) |

A resident read with a free byte allowance now takes no shared lock: one request
CAS, one byte CAS, a session-local buffer, session-local counters, and the
release atomics. An asynchronous read adds one device-shard lock for reserve and
one per completion.

## Invariants kept

- Reserved plus idle bytes never exceed `max_bytes`. A reservation that is only
  blocked by idle buffers evicts them from the session pools (`try_lock`, never
  waiting) before it is admitted; if the pools it could not lock hold the room it
  reports Full and the caller retries, as for any other admission failure.
- A reservation larger than `max_bytes` is `Capacity`; a full request or byte
  budget is `Ok(None)`; both are unchanged.
- Per-device credit is still node-wide, so two sessions on one device share its
  `max_device_reads`.
- Cancelled reads keep their buffers until the CQE; a buffer returns to the pool
  of the session that owns the operation.

## What changed observably

- Buffer reuse is per session. A buffer freed by one session is no longer reused
  by another. `buffer_allocations` can rise for workloads that spread reads over
  many short sessions.
- `ReadExecutorStats` gained `idle_buffer_evictions`, `device_credit_lock` and
  `pool_registry_lock` (`LockProfileStats`: acquisitions, contended, wait ns, hold
  ns). Wait time is measured only on acquisitions that found the lock held; hold
  time covers every acquisition (two clock reads, only on the asynchronous path
  and the cold registry). The wire protocol version is 21.
- `ReadExecutorStats.transport` (`OwnerTransportStats`) sums the always-on OFI
  send-path counters over every session of the owner, closed sessions included:
  `send_size_le_4k`, `send_size_le_16k`, `send_size_le_64k`, `send_size_gt_64k`,
  `send_post_eagain`, `send_post_retries`, `cq_idle_waits`. Each session keeps a
  private baseline and, at the end of every sweep (and before a diagnostic reset
  or session close), adds only the growth to one of 16 cache-line-aligned atomic
  shards. No lock is taken and a snapshot sums the shards. A session can trail
  its provider counters by one sweep; the total is zero for TCP sessions. The
  harness differences it per owner as `transport.<field>` in
  `measurement-read-evidence.json` (`deltas.owner_executor.partitions.<p>`).
- Snapshots are not transactional across sessions: a session closing during a
  snapshot can be counted in neither or both sets for that one sample.

## Residual serialization

- The pool registry mutex (session open/close, eviction under byte pressure,
  stats snapshots) is cold and reports its own contention.
- The device shards serialize only reads on the same device, which also share
  that device's credit by design.
- The owner's reactor thread per session is unchanged by this document.
