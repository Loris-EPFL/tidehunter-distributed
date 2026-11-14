# Several leased writes in flight per owner: research and assessment (14 November 2026)

Status: research note. No code is changed. The design decision belongs to the owner of the project.

## 1. Question

The Tidehunter paper (Chursin et al., arXiv:2602.01873v2) writes with many parallel writers. The distributed leased NVMe-oF route has one write in flight per owner. This note answers:

1. Do published systems run parallel writers over NVMe-oF or to a shared log on remote flash, and how?
2. Should each owner accept several leased writes in flight?
3. Does that need a distributed counter so that other owners see a coherent state?
4. What batching does NVMe-oF give, and which commands does it support?

## 2. Terms

| Term | Meaning in this repository |
|---|---|
| Coordinator | Process that orders writes, assigns logical versions, keeps the index and decides which owner gets a batch. In Sui it is the Sui node. |
| Owner | Process that owns one partition: one Value WAL on the SSD of its host. |
| Sui deployment | 3 databases (`checkpoints`, `epoch_0`, `perpetual`) × 5 physical shards = 15 owner processes on one owner VM, each with its own NVMe-oF controller (15 controllers in the kernel evidence). |
| KV benchmarks A4 to A7 | 1 database, 1 owner, 1 coordinator. |
| Leased extent | A range of an owner's WAL that the owner grants to one coordinator. Only that coordinator writes it. The owner adopts the written frames later, in order. |

## 3. What the paper does

Tidehunter paper, section 3.1 ("Write Flow"), section 2.3 step 6 and the abstract, item (a):

- The synchronous controller allocates a WAL position "through a fast atomic counter and after that, the clients run the rest of the write flow in parallel" (section 3.1).
- "The WAL remains shared (writes are serialized only at allocation)" (section 2.3, step 6).
- Each writer copies its entry into the memory-mapped WAL and updates the index. The copy goes to the page cache; a background syncer calls `fsync`. A mapper keeps "a buffer of pre-allocated maps so that writes never block waiting for map creation" (section 3.1).
- Completion is out of order. The asynchronous controller "updates the highest WAL offset for which all preceding entries have been processed" (section 3.1). Snapshots and recovery use that contiguous prefix (sections 3.3 and 3.4).
- Fixed background threads (tracker, mapper, syncer, flushers, relocator, snapshot), no thread per request (section 5).

## 4. What the distributed route does today (measured and read from code)

| Route | Concurrency per owner | Evidence |
|---|---|---|
| Leased NVMe-oF | 16 callers allocate and encode in parallel (8 preparation workers). One write worker forms a group of 5 to 11 puts and submits **one** NVMe-oF write; the next group waits for that completion. | Run A2 cycle trace (`PERFORMANCE_PLAN_2026-11-10.md`, "Run A2"): write worker busy almost all the time, one group in flight. |
| Owner RPC | Up to 8 groups in flight (`DEFAULT_OWNER_APPEND_OCCUPANCY = 8`, `remote_db/owner_dispatch.rs:19`); the owner appends in order. | Code. |
| Initiator I/O | `O_DIRECT` io_uring writes to the namespace device (`nvmeof/live_namespace.rs:174`); synced tier uses `RWF_DSYNC`, which the block layer sends as FUA (`nvmeof/live_io.rs:95,802`). | Code. |
| Queue geometry | 1 I/O queue, 32 deep, 0 poll queues (A6 settings). | `analysis/output/settings.csv` of Run A6. |

Rates in Run A6 (one fleet, one repetition), 64 KiB puts with 16 callers: leased synced 12,353 puts/s (about 770 MiB/s), leased page cache 20,488 puts/s (about 1.28 GiB/s), native page cache on the same VM class 37,154 puts/s (about 2.3 GiB/s). [INFERENCE] The SSD and page cache are not the limit; the single write in flight is.

**Deviation from the paper:** section 3.1, section 2.3 step 6 and abstract item (a). Leased writes are serialized per owner after allocation. This is a design choice (contiguous frame prefix for adoption, replies in prefix order, no hole after a failed write), not an NVMe-oF limit.

## 5. Related work

| Work | Year, venue | Mechanism | Relevance |
|---|---|---|---|
| [Rio: Order-Preserving and CPU-Efficient Remote Storage Access](https://arxiv.org/abs/2210.08934) | EuroSys 2023 | Linux NVMe over RDMA. Ordered writes execute out of order and asynchronously ("I/O pipeline"), complete in order; per-stream sequence numbers; requests of a stream use one NIC send queue to use RDMA RC in-order delivery; consecutive ordered requests are merged into one command; persistent ordering attributes for recovery. | Closest match. Shows that one ordered write at a time over NVMe-oF wastes the NIC and SSD: Rio reports two orders of magnitude more ordered-write throughput than Linux NVMe over RDMA with synchronous ordering. Its "complete in order" rule is the same as our prefix acknowledgement. |
| [CORFU: A Shared Log Design for Flash Clusters](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/balakrishnan) (local copy `tidehunter-papers/corfu-2012.md`) | NSDI 2012 | A sequencer hands out 64-bit log positions; clients write the flash units directly and in parallel; a hole (position taken, write never done) is filled with junk by a fast hole-filling primitive. | Our coordinator already plays the sequencer role inside a leased extent. Hole filling is the model for a failed write when several are in flight. |
| [PALF: Replicated Write-Ahead Logging for Distributed Databases](https://www.vldb.org/pvldb/vol17/p3745-xu.pdf) (local copy `tidehunter-papers/palf-2024.md`) | VLDB 2024 | Lock-free log sequencer (CAS on the LSN tail); threads fill a group buffer at their LSN offset concurrently; a sliding window tracks in-flight entries. | Same allocation model as the paper; the LSN is the buffer offset, so writers never overlap. |
| [Moving on From Group Commit: Autonomous Commit](https://doi.org/10.1145/3725328) (local copy `tidehunter-papers/autonomous-commit-2025.md`) | SIGMOD 2025 | Workers write small log batches in parallel instead of one group-commit thread; acknowledgement is parallelized. Reports that "the unithreaded design of group commit" causes most commit latency on NVMe SSDs. | Our single write worker is a group-commit thread per owner. The paper argues for several small parallel writes. |
| [NVMe ZNS Zone Append](https://zonedstorage.io/docs/introduction/zns) and [Bjørling, Zone Append, Vault 2020](https://www.usenix.org/conference/vault20/presentation/bjorling) | NVMe 2.0 | NVMe controllers may execute commands from their queues in any order, so writes to a sequential zone are limited to one outstanding per zone. Zone Append lets the device choose the offset and return it, so many appends can be outstanding. | Explains why "one write in flight" appears in log designs: it avoids reordering. We do not need it: our writes target fixed offsets in a regular namespace, so reordering is harmless. ZNS is not available on our route (section 7). |
| [WALTZ](https://www.vldb.org/pvldb/vol16/p2884-lee.pdf) | VLDB 2023 | Uses Zone Append for the RocksDB WAL on ZNS to cut tail latency. | Same conclusion: parallel appends lower tail latency compared with serialized appends. |
| [BtrLog](https://arxiv.org/abs/2606.27051) | VLDB 2026 | Single-writer cloud log service; quorum of SSD log nodes in one round trip; asynchronous archival. | Single writer per log, like our coordinator per extent; durable append latency is the design goal. |
| [Scalio](https://www.usenix.org/system/files/osdi25-sun.pdf) | OSDI 2025 | Key-value store on DPU-based JBOFs using NVMe-oF target offload. | [INFERENCE] Target-side processing cost is a design factor at high rates; relevant to queue count and target CPU. Only the abstract-level description was read. |

## 6. NVMe and NVMe-oF facts

### 6.1 Commands

NVM Command Set I/O commands ([NVM Command Set specification](https://nvmexpress.org/developers/nvme-command-set-specifications/); summary table in the [specification walkthrough](https://jiachanggit.github.io/nvme/admin-io-spec-walkthrough-en/), section 01): Read, Write, Compare, Verify, Copy, Dataset Management, Write Uncorrectable, Write Zeroes. Base specification: Flush. Write has the FUA bit (force unit access). Compare and Write can be fused (atomic compare-and-write up to ACWU). The Zoned Namespace command set adds Zone Append and zone management.

The Linux kernel target with a **file** backend (our route) implements only: Read, Write, Flush, Dataset Management, Write Zeroes ([`drivers/nvme/target/io-cmd-file.c`](https://github.com/torvalds/linux/blob/master/drivers/nvme/target/io-cmd-file.c), `nvmet_file_parse_io_cmd`). A Write with FUA becomes a file write with `IOCB_DSYNC` (same file, `NVME_RW_FUA` → `IOCB_DSYNC`). Flush becomes `vfs_fsync`. Zoned namespaces are exported only from a zoned **block device** backend ([`zns.c`](https://github.com/torvalds/linux/blob/master/drivers/nvme/target/zns.c), `nvmet_bdev_zns_enable`).

### 6.2 Ordering and atomicity

- NVMe does not order commands: the controller may execute commands from its submission queues in any order ([zonedstorage.io, "Zone Append"](https://zonedstorage.io/docs/introduction/zns)). Each command completes independently.
- A write is atomic only up to the atomic write unit (AWUN, and AWUPF for power fail); the minimum guarantee is one logical block ([NVM Command Set 1.2, atomicity figures](https://nvmexpress.org/wp-content/uploads/NVM-Express-NVM-Command-Set-Specification-Revision-1.2-2025.08.01-Ratified.pdf); [evanjones.ca, Durability: NVMe disks](https://evan-jones.appspot.com/durability-nvme.html)). A multi-block frame can tear on power loss; our frames carry CRC32 and a BLAKE2b digest, so a torn frame is detected.
- Rio, section 2.2: "Each layer of NVMe-oF is orderless"; Linux file systems obtain order by waiting for the previous write and a FLUSH ([Rio](https://arxiv.org/abs/2210.08934)).

### 6.3 Batching and concurrency

| Mechanism | What it gives | Where |
|---|---|---|
| Queue depth | Many commands outstanding on one queue pair; our queue accepts 32 today, we use 1. | `queue_size` connect option ([nvme-connect(1)](https://nvme-cli.readthedocs.io/en/latest/nvme-connect.html)). |
| Several I/O queues | Spread commands over target CPUs. Options `nr_io_queues`, `nr_write_queues` (separate queues for writes), `nr_poll_queues`. | [`drivers/nvme/host/fabrics.c`](https://github.com/torvalds/linux/blob/master/drivers/nvme/host/fabrics.c) option table; nvme-connect(1). |
| io_uring batch submission | Many SQEs submitted with one system call; completions reaped in batches. | [io_uring(7)](https://man7.org/linux/man-pages/man7/io_uring.7.html), [io_uring_submit(3)](https://man7.org/linux/man-pages/man3/io_uring_submit.3.html), [liburing wiki, Batching](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023). |
| Scatter-gather | One command writes one contiguous LBA range from several host buffers (vectored write), up to the maximum data transfer size. | Specification; Linux `writev`/`IORING_OP_WRITEV`. |
| Block-layer merging | Adjacent requests merge into one command; Rio shows merging consecutive ordered writes cuts initiator and target CPU. | Rio, section 3.2, lesson 3, and section 4.5. |
| In-capsule data | Small writes travel inside the command capsule (no RDMA READ round trip). nvmet-rdma default `PAGE_SIZE`, maximum `max(16 KiB, PAGE_SIZE)`, port attribute `param_inline_data_size`. | [`drivers/nvme/target/rdma.c`](https://github.com/torvalds/linux/blob/master/drivers/nvme/target/rdma.c) lines with `NVMET_RDMA_DEFAULT_INLINE_DATA_SIZE`; [`configfs.c`](https://github.com/torvalds/linux/blob/master/drivers/nvme/target/configfs.c). |
| No multi-range write | There is no command that writes several discontiguous ranges. Copy takes several source ranges but copies inside the device; Dataset Management takes ranges for hints and deallocation. | Specification. |

### 6.4 Target behavior that limits parallel writes on our route

- **Buffered file target path.** With `buffered_io=1`, nvmet first tries the write inline with `IOCB_NOWAIT`; if that cannot complete without blocking, it queues the request to the per-CPU workqueue `nvmet-buffered-io-wq` (`io-cmd-file.c`, `nvmet_file_submit_buffered_io`; `core.c`, `alloc_workqueue(..., WQ_MEM_RECLAIM | WQ_PERCPU, 0)`).
- **ext4 serializes buffered writes per file.** `ext4_buffered_write_iter` returns `-EOPNOTSUPP` for `IOCB_NOWAIT` and takes `inode_lock` for the copy; `generic_write_sync` (the `O_DSYNC`/FUA sync) runs after `inode_unlock` ([`fs/ext4/file.c`](https://github.com/torvalds/linux/blob/master/fs/ext4/file.c)). So on our owner (ext4), concurrent writes to one WAL file are copied into the page cache one at a time, and their syncs can overlap. [INFERENCE] The copy of a 1 MiB group is a few hundred µs at most, so several writes in flight still overlap network transfer, target queueing and the synced-tier sync, which are most of the measured write interval (Run A2: completion wait 279 µs page cache, 536 µs synced, of 355 and 627 µs). This must be measured.
- **Possible link to the crossing tail.** A FUA write becomes `IOCB_DSYNC` on ext4; a write into a freshly `fallocate`d (unwritten) range must convert extents and commit the journal. [INFERENCE] This is a candidate for the per-file stall; it is not proved (see `local://crossing-tail-diagnosis.md`).

## 7. Assessment

### 7.1 Should each owner accept several leased writes in flight?

**Yes, recommended**, for these reasons:

1. It restores the paper's write model (section 3.1): serialization only at allocation, parallel writes, a tracker for the contiguous prefix.
2. The measured limit is the single write in flight (section 4), not the SSD.
3. Rio shows the same gain on the same stack (Linux NVMe over RDMA), and Autonomous Commit shows that a single group-commit thread adds latency on NVMe SSDs.
4. NVMe-oF needs no change: the queue we open already accepts 32 commands.

### 7.2 Two variants

| Variant | Acknowledge a put when | Recovery and adoption | Risk |
|---|---|---|---|
| **A. Out-of-order execution, in-order acknowledgement** (Rio's rule) | Its own write and every earlier write of the extent have completed. | Unchanged: adoption and recovery already use the contiguous complete prefix. | Low. A slow write delays the replies of later writes (head-of-line), but never more than today, where later writes are not even submitted. Hole only on a failed write: fence the extent (existing failure path) or fill with a skip frame. |
| B. Out-of-order acknowledgement | Its own write has completed (synced tier: its own FUA write). | A hole can precede an acknowledged frame. Recovery and adoption must skip a hole and keep valid frames after it, or a crash loses an acknowledged put. | Higher: changes the recovery contract; needs CORFU-style hole filling and a proof for both tiers. |

Recommendation: implement variant A first. It keeps the acknowledgement contract and the recovery rules, and gives most of the throughput gain. Consider B only if measurements show head-of-line latency matters.

Elements of variant A (design note to write before code):

- Bounded in-flight window per owner (count and bytes), sized from queue depth and the bandwidth-delay product; full window = callers wait (existing backpressure).
- Positions allocated from the coordinator's existing per-extent cursor; groups stay; each group is one write at its own offset.
- A completion tracker per extent: completed set plus contiguous frontier; replies and adoption requests use the frontier.
- Extent switch: a group never spans extents; the next extent is used while earlier writes of the previous extent are still in flight; the closing adoption waits for the frontier.
- Failure: a failed or short write fences the extent (current rule); writes after it in the same extent are not acknowledged.
- Synced tier: every write keeps `RWF_DSYNC` (FUA), so each completion is individually durable; the frontier gives prefix durability.
- Queue geometry: start with the current 1 × 32 and measure; then test more I/O queues (or `nr_write_queues`) as separate arms.
- Crash consistency: no change in the on-disk format or recovery if A holds; tests must cover crash with several writes in flight, partial completion, failed middle write, extent switch with writes in flight, both tiers.

### 7.3 Is a distributed counter needed?

**No.**

- Inside a leased extent only one coordinator writes; the lease is exclusive and granted by the owner. Allocation is a local atomic counter in that coordinator, like the paper's counter (section 3.1) and CORFU's sequencer for a single sequencer node.
- Owners keep independent logs. Order across owners comes from the logical versions the coordinator assigns and from index publication after predecessor dependencies resolve. A physical WAL offset is never a global order. Several writes in flight per owner do not change this.
- If several coordinators wrote one owner in the future, each would hold its own extents; the lease grant is the only serialization point and it exists already.

### 7.4 Should we open one NVMe-oF path per writer?

**No.** A path is a controller and its connection, a kernel object with setup cost (milliseconds) that the project rules require to be reused. Parallelism comes from outstanding commands on the existing queue (depth 32 today) and, if needed, more I/O queues on the same controller. Rio dispatches each ordered stream to one NIC send queue to use RDMA in-order delivery; it does not open connections per request.

### 7.5 More owners

More owners (2 owner VMs, or 2 owner processes on one VM) is a separate lever: N logs, N writes in flight in total. It scales out but keeps the per-owner serialization. It is useful for the comparison with Valkey (5 primaries) and as a control, not as a replacement for 7.1.

## 8. Open items

1. Read the native engine's recovery for a hole (an allocated position whose copy never finished) to confirm what variant B would need; not needed for variant A.
2. Measure ext4 per-file write serialization on the owner with several writes in flight (one fleet arm with the window at 1, 4 and 8).
3. The crossing-tail trace (path B) stays independent of this change and should run first or in the same fleet.
4. ZNS Zone Append is not usable on the current route (file backend, and Google Cloud Local SSD is not exposed as ZNS [INFERENCE, not checked]).
