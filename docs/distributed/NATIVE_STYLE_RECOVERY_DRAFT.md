# Native-style recovery draft

Status: superseded on 13 November 2026 by the `wal_replay` history mode (default). The implemented design is in `experiments/nvmeof/LIVE_WAL.md`, "Owner history persistence". This text is kept as the gap analysis; its section 4 line numbers are older than the implementation.

## 1. Model

[INFERENCE] Proposed model, based on A. Chursin et al., *Tidehunter*, arXiv:2602.01873v2, sections 3.3 and 3.4: the owner WAL is the only record; the coordinator index is the only authority for a read; the coordinator flushes its index to local disk in the background; it stores one replay position per owner; recovery loads the index and replays the WAL suffix of each owner.

## 2. Owner without history files

[INFERENCE] Keep a window of 1,024 batches and a running prefix digest at the owner; drop older records. Precondition: the coordinator retries only batches that were not acknowledged. This precondition is not established by the current implementation.

The current resident limit is 1,024 batches (`tidehunter/src/distributed/reference_directory.rs:25`); history append advances a prefix digest (`tidehunter/src/distributed/storage/history.rs:273-287`). Coordinator admission checks `known`, pageable receipt history and active identities, returning the prior version for an exact caller retry before admitting new work (`tidehunter/src/distributed/remote_db.rs:2264-2291, 2650-2670`). A recovery test resubmits an acknowledged operation and expects its old version (`tidehunter/src/distributed/remote_db/lifecycle_tests.rs:568-570`). This is caller-to-coordinator deduplication, not evidence that the coordinator resends acknowledged batches to the owner.

The owner dispatch policy bounds already-admitted work (`tidehunter/src/distributed/remote_db/owner_dispatch.rs:1-13`). The leased client marks the writer failed on an adoption error and refuses work when poisoned (`tidehunter/src/distributed/tcp/live_client.rs:1179-1212`). A test checks that failed, short and timed-out leased writes are not retried (`tidehunter/src/distributed/tcp/live_client.rs:2522-2526`). [INFERENCE] These observations do not prove the proposed coordinator-to-owner retry precondition for every failure and reopen path; that precondition remains open. Coordinator-side exact deduplication of acknowledged caller retries must also be preserved or its contract explicitly changed.

## 3. Present state and gaps

### Enumerable WAL

**Present state.** Sealed recovery opens the reference history and inventory before scanning known fragments; frames do not cross fragment boundaries (`tidehunter/src/distributed/storage.rs:1061-1063, 1114-1124`). It decodes `CompleteGroup` and reconstructs batch receipts and descriptors from the frame (`tidehunter/src/distributed/storage.rs:1244-1255, 1301-1314`).

**Gap.** [INFERENCE] Define and validate enumeration of frame boundaries and batch identities from WAL bytes alone for both leased and owner-RPC frames, without history or its inventory as authority.

### Coordinator snapshot and positions

**Present state.** The native index can open a snapshot (`tidehunter/src/distributed/native_index.rs:847-871`). The local checkpoint persists an image, collects epoch and partition receipt counts and prefix digests, then installs a root (`tidehunter/src/distributed/local_db/checkpoint.rs:115-145`). Checkpoint proofs contain a prefix and digest (`tidehunter/src/distributed/tcp/checkpoint.rs:18-25`).

**Gap.** [INFERENCE] Atomically bind a coordinator index image to one physical replay position per owner, with the ordering and durability needed to load the image and replay every suffix safely. Receipt counts are not shown here to be physical WAL replay positions.

### NVMe-oF suffix replay

**Present state.** The live recovery reader asks the owner for a metadata page, then connects to the bindings in that page (`tidehunter/src/distributed/tcp/live_recovery.rs:16-25, 47-88`). It checks the CRC and exact length of an inner frame (`tidehunter/src/distributed/tcp/live_recovery.rs:284-290`).

**Gap.** [INFERENCE] Read and decode the suffix from each saved WAL position through NVMe-oF without requesting owner history metadata; validate boundaries, CRCs, identities and ordering before publishing index entries.

### Unadopted leased tail

**Present state.** Recovery treats a defective frame below the synced frontier as corruption and stops at the first invalid live frame beyond it; valid frames beyond the frontier are decoded (`tidehunter/src/distributed/storage.rs:1125-1130, 1204-1215, 1244-1255`). The client may have an outstanding adoption and marks the live writer failed if its result fails (`tidehunter/src/distributed/tcp/live_client.rs:1187-1212`).

**Gap.** [INFERENCE] Define which complete leased frames not adopted by the owner are replayed, how uncertain acknowledgements are resolved, and how incomplete or conflicting tails fail without returning wrong data.

### Retry rule

**Present state.** The coordinator admits an exact identity retry by returning the prior version and rejects a changed version or digest (`tidehunter/src/distributed/remote_db.rs:2650-2670`). The owner checks pending identities and versions and falls back to history (`tidehunter/src/distributed/reference_directory.rs:949-990, 995-1034`).

**Gap.** [INFERENCE] Use the recent memory window for exact owner retries and read the relevant WAL tail after a broken connection; define a bounded search and fail closed when the original outcome cannot be established. Establish the coordinator-to-owner retry precondition and preserve or explicitly change caller-to-coordinator deduplication before removing older authority.

### Retention watermark

**Present state.** `retire_files` requires healthy sealed storage, a complete durable receipt prefix and valid file offsets; it rejects live read pins before recording retirement and removing files (`tidehunter/src/distributed/storage.rs:3960-4015`).

**Gap.** [INFERENCE] Define a coordinator watermark that protects replay suffixes, all live index references, retained cuts and active readers before an owner may retire files. A replay position alone does not prove that earlier files contain no still-referenced values.

### Remove owner history and spill

**Present state.** The history root tracks counts, file inventory, prefix state and indexes; its reader owns nodes, receipts, descriptors and retirement files (`tidehunter/src/distributed/storage/history.rs:31-49, 85-93`). The directory starts background persistence and drains the pending prefix after history append (`tidehunter/src/distributed/reference_directory.rs:1317-1345, 1472-1483`).

**Gap.** [INFERENCE] Replace every dependency below before removing owner history files, spill and the window limit. Preserve the bounded recent retry window, but remove the current history-driven limit on accumulated batches.

## 4. History dependents

The first three columns reproduce the A4 inventory as supplied by the plan. [INFERENCE] The caller locations and full ranges in that inventory were not independently verified here; use the opened implementation ranges below as code evidence. The last column is proposed, not implemented. Each new-model entry is [INFERENCE].

| Dependent | Where | Needs from the history | Need in the new model |
|---|---|---|---|
| `lookup_conflict`, `lookup_id`, `lookup_version` | `reference_directory.rs:949–1035`; callers `adopt_live_frame` (`storage.rs:1947`), `append_group_impl` (2720), `reserve_live_group` (2309, 2323) | identity → receipt and version → receipt, on each put | [INFERENCE] Recent-window identity and version checks; define how older or uncertain retries are resolved without history. |
| `receipt_group_end` | `reference_directory.rs:1160–1177` | identity → receipt and group end | [INFERENCE] Retain group end in the recent window or reconstruct it from the WAL. |
| `receipt_page` | `reference_directory.rs:1036–1071`; callers `complete_live_group_impl` (`storage.rs:2521`), `PartitionLog::checkpoint_prefix` (3906), `tcp/control.rs:1885` | ordinal → receipt | [INFERENCE] Replace ordinal paging with WAL replay or a redesigned completion and checkpoint protocol. |
| `checkpoint_prefix` | `reference_directory.rs:1072–1113` | root prefix state; ordinal → prefix state for an older prefix | [INFERENCE] Keep the running digest and establish how older cuts are validated without historical prefix states. |
| `lookup` by address | `reference_directory.rs:882–948`; `load_authority` (`storage.rs:3381–3414`); callers: owner-RPC reads (`plan_record_read`, 3535), on-demand live grants (`resolve_live_record`, 3575), coordinator reopen (`resolve_live_batch`, 3588) | address → receipt and descriptors | [INFERENCE] Coordinator index supplies read references; replay reconstructs descriptors from WAL bytes. Owner-RPC reads and on-demand grants need a separate decision. |
| `is_retired`, `History::retire` | `history.rs:365`; `reference_directory.rs:1114, 1486` | WAL file retirement | [INFERENCE] Replace the retirement index with a safe coordinator watermark and a recoverable inventory rule. |
| Open and recovery | `History::open`, `recover_sealed_impl` (`storage.rs:1029–1347`) | durable root | [INFERENCE] Discover and validate retained WAL and replay from the coordinator snapshot positions without an owner history root. |
| Spill | `start_background` (`reference_directory.rs:1320–1454`), `persist_pending` (1472–1485) | `History::append` (`history.rs:273–363`) | [INFERENCE] Remove spill once all other consumers no longer require the history. |

Paths in this table are relative to `tidehunter/src/distributed/`; `history.rs` is `storage/history.rs`. The cited directory implementations were opened at `tidehunter/src/distributed/reference_directory.rs:882-920, 949-1115, 1160-1177, 1317-1345, 1472-1491`; history and recovery were opened at `tidehunter/src/distributed/storage/history.rs:273-290, 365-390` and `tidehunter/src/distributed/storage.rs:1029-1205, 1204-1320`.

## 5. Open questions

- [INFERENCE] How can readers other than the coordinator obtain valid references and read safely without owner history?
- [INFERENCE] How would a later asynchronous replica ship the same WAL and establish a safe replay and retention position?
- [INFERENCE] After retirement in permanent export mode, when and how can a new connection be established without violating the fail-closed rule?

## 6. Trade

[INFERENCE] Recovery time grows with the WAL written since the last snapshot; puts pay nothing for recovery metadata in this proposed model. This is a design objective, not a measured result.
