# Draft: one-sided registered resident WAL reads

21 October 2026. **Design only. No one-sided feature, correctness result, EFA
hardware proof, or measured speedup is claimed by this document.**

Updated 22 October: §2.1 now describes the implemented experimental member-CRC
cutover on `perf/native-wal-fastpath`. That two-sided change does not implement
the grant, registration or one-sided lifecycle proposed here.

## 1. Decision and scope

Add an optional read-only RMA fast path for **already resident, registered,
immutable encoded Value WAL members**. Keep the owning partition's native WAL
and complete-batch descriptor history authoritative. Resolve keys in the existing
coordinator index. Obtain a bounded resident handle through the existing message
service, then reuse that handle while its explicit grant remains valid. A warm
handle hit can issue one `fi_read`; a cold handle acquisition cannot honestly be
called a one-round-trip get.

The separately implemented trusted `efa-native` **two-sided message** mode is the
baseline, control channel, and cold/evicted fallback, not an implementation of
this RMA proposal. `Transport::EfaNative` opens libfabric's `efa-direct` fabric
without rustls records or cryptographic endpoint binding. `FabricTcpNative`
selects an explicitly named software-provider counterpart for local correctness,
never production EFA fallback or hardware performance evidence. See Stage A for
the implemented symbols and exercised scope. Existing `tcp`, `efa`, `efa-direct`
and `fabric-tcp` retain TLS. **Default TCP+TLS remains usable without libfabric
or EFA.** Production EFA selections fail if the requested hardware provider is
unavailable; no silent security-mode or provider downgrade is permitted.

Non-goals:

- No Byzantine/adversarial-peer model, BFT protocol, compulsory per-request
  signatures, or cryptographic binding of ordinary trusted cluster peers.
- No remote writes/atomics to WAL or publication state. A NIC completion never
  becomes a durable receipt. No new consensus or RF1 availability claim.
- No arbitrary remote NVMe access, NVMe-oF service, driver replacement, ODP port,
  remote hash-table traversal, middleware resurrection, or global physical WAL.
- No Sui API/storage-semantic change. Retain the existing `distributed::sui::Db`
  boundary, batch publication, snapshots, read cuts, and typed-store behavior.
- No optimization of a compute-local verified-value hit by adding an RMA, lock,
  handle lookup, or extra allocation to that hit.

## 2. Trust, corruption, and correctness are different contracts

Trusted mode assumes ordinary non-malicious processes in an isolated deployment.
Processes, hosts, links, and storage can fail; memory/data can be corrupted.
Deployment isolation is an operational prerequisite, not something a mode label
or a CRC enforces. An MR key is an access capability scoped by a fabric domain;
it is neither an authenticated peer identity nor application fencing authority.
Unkeyed BLAKE2b is also not authentication. Keep these distinctions visible in
configuration, errors, and measurements.

Three independent checks remain required:

1. **Authority and membership:** the storage owner matches the complete
   `ValueRecordReference` against a `RegisteredBatch` derived from validated
   whole-batch append/replay. A self-consistent member or checksum cannot prove
   that its batch completed, became durable, or is still retained.
2. **Logical correctness:** the coordinator selects the exact reference at its
   captured published cut, pins that cut, verifies the returned member, then
   applies the current full-key/keyspace/tombstone checks. An old immutable value
   can be correct for an old cut and incorrect for a later cut.
3. **Corruption detection:** outer native CRC protects complete recovery frames;
   independent member CRC32/IEEE protects scalar member bytes and identity
   metadata without reading siblings. Whole-batch BLAKE2b remains in the batch
   authority. Direct-message packet CRCs do not replace these storage checks.
   RMA completion alone cannot justify removing member verification.

### 2.1 Integrity format decision

**Current experimental format: native-style per-member CRC32/IEEE, batch v3.**
The checksum domain is `tidehunter-batch-member-v3\0`. Canonical coverage includes
the database/operation identity, original batch version, operation index, every
descriptor bound and kind/keyspace, and complete native entry/key/value bytes.
The member header is an eight-byte native length/checksum pair. The decoder
still validates exact descriptor layout, native mutation semantics and expected
key. Complete-batch membership and retention remain separate requirements.

This replaces the earlier independent BLAKE2b-256 member field, not whole-batch
`PayloadDigest`, native WAL recovery CRC or authority checks. It stores a 32-bit
check instead of a 256-bit digest; it provides neither authentication nor
cryptographic collision resistance. The contract is trusted deployment and
accidental-corruption detection, as for native CRC, not resistance to an adversary
choosing records. This is an explicit format decision, not a trusted-mode hash skip.

The clean cutover uses batch v3, group v2, index v4, history/local-checkpoint v2,
registration/seal v5 and wire/ALPN v9. Old experimental formats are rejected;
native `Db` storage is unchanged. Append, replay, mapped/positional/async reads,
RPC, caches and checkpoint readers use the new checked references. Corruption,
wrong identity/version/member/bounds/key, truncation, legacy-format rejection and
recovery regressions remain in the exercised suites. There is no RMA decoder yet:
a future RMA path must reuse this complete checked member, not only its value.
See the [implementation and exercised scope](../../tidehunter-fabric/DESIGN.md#native-style-distributed-wal-path-22-october).

## 3. Source-guided mechanisms

QMD collection: `tidehunter-papers`, not the local `papers/parsed` directory.
Research used QMD query, section TOCs, and section reads. Where embedded image
payloads exhausted a QMD section response, the corresponding local parsed paper
section was read in full with image payloads omitted, not treated as a complete
snippet. Sources below describe mechanisms, not transferable latency guarantees.

| Source and complete relevant sections | Mechanism to retain | Adaptation and limit in this engine |
|---|---|---|
| Tidehunter v2, §§2.3, 3.1–3.4, 4.1–4.4, 5; local `project:papers/parsed/Tidehunter-2026-v2.md:81-192` [P1] | Append values to their long-term WAL position; index resolves location; snapshots/root adoption govern recovery; relocation must not replace newer writes | Resident registered copies are derivative acceleration, not a second durable value store. Preserve independent partition WALs. Native paper's shared WAL is not a requirement for this distributed fork. Paper writes normally finish in page cache; the fork's explicit durable receipts remain stronger and unchanged. |
| TeRM, §§2.1–2.2, 3.1–3.2, 4.1–4.4, 5; QMD `#7ccb34` [P2] | Resident one-sided read, software RPC for nonresident data, bounded registered bounce buffers; client feedback is needed because the server CPU does not observe successful one-sided hits | Adopt the resident/RPC split, not its modified Mellanox ODP page tables. A stale residency hint must never authorize unsafe access. Use explicit pinned immutable grants; no magic-pattern inference for application values. |
| FaRM 2014, §§2, 3.1–3.6; QMD `#13daf0` [P3] | Cached region capabilities, address+size+incarnation fat pointers, epoch-based reclamation; mutable-object reads require a hardware-aware consistency protocol | Use full reference + region/slot generation + explicit grant and quiescence. Do not import FaRM's cache-line-version protocol, ascending-write assumptions, distributed transactions, DHT, or shared-memory API. Immutable members avoid those mutable-object requirements. |
| FaRM 2015, §§5–5.2, 5.4–5.5; QMD `#830bf2` [P4] | A remote CPU cannot check leases on an RDMA read; precise membership changes stop old issuers and reject old completions before new configuration work proceeds | Add a read-grant revocation barrier to current fencing, not a second consensus system. Its bounded-clock-drift/NVRAM/replication availability assumptions are not established by this RF1 fork. Timer expiry alone is not DMA quiescence. |
| HERD, §§2.3, 3–3.3, 4–4.3; QMD `#67e6a7` [P5] | Whole-operation round trips, request windows, endpoint fanout, and CPU/NIC work matter more than raw verb latency | A message baseline can beat multiple/larger one-sided reads. Existing local index resolution is useful: avoid adding a remote hash lookup. Do not copy UC/UD-specific posting or left-to-right DMA sentinel rules to EFA. |
| FaSST, §§2.2, 3–3.4.1, 4–4.4, 5.1, 7.1; QMD `#81ced9` [P6] | Opportunistic posting/completion batches, bounded receive/request windows, simple RPC fallback; evaluate read amplification and fanout | Compare full verified operations with matched limits and CPU. No wait-to-fill batching. The paper's small-object InfiniBand results and assumed/unimplemented recovery do not prove EFA throughput, reliability policy, or our recovery. |

Primary implementation cross-checks:

- TeRM tree `a94d44a59238d8d51266a7a2ea8bbf2daf3151d9`:
  `RemoteMrManager::rdma_should_via_rpc` and `RemoteMr::pull_page_bitmap` in
  [`libterm/ibverbs-pdp/mr.cc`][I1]; `RpcIo::do_io_by_psync`/`RpcIo::io` in
  [`rpc.cc`][I2] use registered bounce memory and real buffered/direct file I/O.
  Its [`driver/driver.patch`][I3] changes `umem_odp` and `mlx5` invalidation to
  map a reserved magic page. This is not an EFA userspace option.
- HERD tree `2fa86cb66c18adc22e3e8e6513cc3482f7c26242`:
  [`main.c`][I4] `run_server`, `process_pipeline`, and `post_recv` demonstrate
  request-region polling, response SEND, receive windows, and prefetch pipeline.
  Its circular cache eviction and polling sentinels are not WAL retention rules.
- FaSST tree `580abef01b48be4e1ab7f0d9ac95e554cccb4ce5`:
  [`rpc/rpc_datapath.cc`][I5] `Rpc::send_reqs`, `send_resps`, and `poll_comps`
  post already-ready bounded lists. These are source evidence for opportunistic
  batching, not EFA ABI or crash guarantees. No dependency is proposed.

## 4. Current engine map and preservation points

Paths are relative to this engine checkout. `project:` starts at its parent.
Source baseline supplied for this work is
`fe683e7fad3f517e2f02b7c5f66f37fcdce257e1`; line coordinates below identify the
inspected pre-native-session source and are navigation aids, not stable APIs.
Symbols are authoritative. Stage A records the companion native-session
implementation in the working branch and its evidence. The following RMA
symbols and protocol fields remain **proposals**, not claims that they exist.

| Existing file/symbol and inspected coordinates | Current contract / proposed connection |
|---|---|
| `tidehunter/src/distributed/types.rs:10-100`: `BatchId`, `BatchVersion`, `MutationVersion`, `PhysicalValueAddress`, `RecordDescriptor`, `ValueRecordReference` | Keep operation identity, logical order, current authority, physical generation and new MR generation distinct. A reference is not a durability certificate or lease. |
| `distributed/reference_directory.rs:27-106`: `RegisteredBatch::{validate_reference,reference,matches}`; `:137-175`: `MetadataBudget`/`MetadataCredit` | Exact full-reference authority and bounded retained descriptors. A grant must retain an authority owner/credit, not copy uncharged metadata into another registry. |
| `distributed/storage.rs:1264-1270`: `persist_receipts`; `:1372-1485`: `validate_record_authority`, `validate_durable_record_reference`, `try_plan_record_read`, `plan_record_read`, `RecordReadPlan` construction | Release-published durable group frontier precedes grant publication. Cold metadata/file work stays on bounded workers. A registered resident entry retains its exact native file/log and descriptor owners until retirement. |
| `distributed/storage.rs:1699-1745`: `PartitionLog::retire_files` | Whole-file deletion needs a complete rooted durable-prefix and reachability decision. Add grant exclusion/quiescence to this boundary; existing local read admission is not remote DMA exclusion. |
| `distributed/codec.rs`, `CompleteBatch::{decode_record,decode_records}`, `validate_reference_bounds`, `member_checksum` | Reuse versioned member CRC32/IEEE, exact layout, checked arithmetic and native mutation validation. Caller order remains unchanged. |
| `tidehunter/src/wal/mod.rs:382-421,423-548`: `Wal::read`, `checked_member_range`, `plan_record_payload_range`, `read_record_payload_range`; `tidehunter/src/crc.rs:14-68`: `CrcFrame` | Native `Db` reads retain CRC. Fork member reads require prior complete-group membership plus member integrity. Existing mapped reads copy the requested member; an mmap/page-cache hit is not a remotely registered resident allocation. |
| `distributed/native_index/retention.rs:6-33,69-80,106-158`: `IndexReadPin`, `validate_cut`, `pin_cut`, `capture_lease` | Keep logical cut pins through read/result ownership and failures. The new remote residency pin is additional and must participate in retention; it cannot replace this pin. |
| `distributed/tcp/inventory.rs:51-122,314-399`: `ReaderAuthority::{snapshot,snapshot_valid,invalidate}`, `NodeInventory::{fence_to,seal}` | Current request epoch and reader generation checks. RMA needs a barrier before invalidation/fence acknowledgment, since no target handler runs per read. Do not simply cache the current bool. |
| `distributed/tcp.rs:574-592,833-906`: `StorageNode::{reader_snapshot,reader_authority_valid,read_response}` | Existing reads check membership/durability and recheck reader generation after I/O. Grant issue must use the same checks; §7 defines the extra ongoing authorization needed for bypass. |
| `distributed/tcp/read_executor/cache.rs:1-3,13-55,56-128`: fixed four-way/CLOCK `Cache` | Cache is derivative, membership is checked before get, client verifies even a hit. Its exact-size CQ-retired buffer is not currently an MR. Reuse the bounded policy/accounting style, not an unbounded second cache. |
| `distributed/remote_db/pending_read.rs:1698-1788,1794-1848`: `prepare_get`, `point_value`, `check_handle`, `complete_get`; `:1633-1655`: `finish_record` | Resolve/capture pin under short index guard, then release it before network wait. Preserve full-key point checks, reduced-key scan checks, result count and tombstone rules, current authority ownership and health. Do not treat a reduced-key collision as a returned value. |
| `distributed/remote_db/read_pipeline.rs:113-183,335-374`: scan page preparation/completion | Preserve one captured cut, bounded reference pages, caller order/duplicates/per-key outcomes and no scan cache fill. Add RMA below this boundary, not in Sui. |
| `distributed/tcp/fabric.rs`: `Transport`, `bootstrap`; `distributed/tcp/reactor.rs`: `Session` framing/output budgets | Message transport is the integration base. The implemented trusted native session is mapped in Stage A. Grant/RMA capability must still be separately negotiated; no TLS ALPN claim for a plaintext native session. |
| `tidehunter-fabric/src/endpoint.rs:15-19,94-142,195-204,834-850`: `Endpoint`, `open_efa_direct`, `set_peer`, `Drop` | One serialized endpoint owner, stable contexts, four TX/RX slots, bounded process endpoint admission, fail-stop/quarantine on uncertain teardown. Add explicit RMA ownership, not borrowed caller buffers. |
| `tidehunter-fabric/src/endpoint/resources.rs:140-200,265-403`: `Info::hints`, `Resources::open_mode`; `:683-732,859-1007`: `post_send`, `poll`, `complete`, `close` | Current caps are messages, registrations only SEND/RECV, and context dispatch only TX/RX. RMA needs caps, remote-readable MR owners, a third completion class, error correlation and teardown proof. Existing completion-prefix fences describe sends, not remote reads. |
| `tidehunter-fabric/src/endpoint/resources/efa_direct.rs:12-40`: `validate`; `resources/efa.rs:24-85`: EFA geometry/environment validation | Negotiate and bound RMA sizes/IOVs/CQ capacity separately; the current message-MTU and packet-budget proof does not prove RMA geometry. |

## 5. What EFA and `fi_read` actually provide

Primary sources checked on 21 October 2026:

- [AWS EFA User Guide][A1] states RDMA read support for Nitro v4 and later and
  lists `g6.8xlarge` with RDMA read/write support. Earlier EFA-capable Nitro v3
  examples such as `c5n.18xlarge` are listed without RDMA read. Therefore
  “supports EFA messages” does **not** imply “supports device one-sided read.”
  An instance-family claim does not qualify the attached device, driver, or
  endpoint on a particular host. No AWS API was used for this research.
- The pinned libfabric commit
  `3cc1095272b07ebeb5cc8f44851af6aa3eb75e41` documents `FI_RMA`, `FI_READ` and
  `FI_REMOTE_READ` on `efa-direct` RDM, mandatory target-side AV insertion of the
  initiator, `FI_CONTEXT2`, local registrations, provider keys, allocated pages,
  virtual-address addressing, and no message ordering/`FI_FENCE`/`FI_RMA_EVENT` in
  the [fabric comparison][F1]. The [provider manual][F2] documents queryable RMA
  limits, device-RDMA/emulation options and unsupported direct `fi_cancel`.
- The pinned [`efa_rma_read`/`efa_rma_post_read`][F3] check `FI_RMA`, use remote
  address/key, require local descriptors, and post to the hardware read path.
  Internal assertions are not input validation for our Rust API. Query returned
  caps, IOV limits and maximum RMA transfer, then reject incompatible geometry
  before any post. Do not use the approximately 8-KiB **message** MTU as the RMA
  limit or assume an example approximately 1-GiB device maximum everywhere.
- [`fi_read`][F4] asynchronously copies a registered remote memory range to a
  local destination. The destination must not be touched until completion.
  There is no default target application completion for a read. Registration,
  address mode, and key scope follow [`fi_mr`][F5]. Completion/error processing
  follows [`fi_cq`][F6]. Provider auto-progress does not retire Rust ownership;
  the owner must still consume CQ events and make progress after EAGAIN.

**`fi_read` reads memory, not an arbitrary WAL offset on NVMe.** Registering a
file-backed mmap can fault/pin physical pages and charge real RAM; it does not
turn missing disk pages into hardware storage reads. This design instead
registers bounded allocated host memory populated from validated WAL reads.
Cold/evicted values go through normal message RPC and bounded file I/O. The
native WAL remains disk-backed and recoverable independent of the resident pool.

TeRM's ODP mapping, magic-page invalidation, `mlx5` driver patch, and libibverbs
interposition require Mellanox-specific mechanisms [P2, I1–I3]. They are not
available by requesting `FI_RMA` on EFA. No EFA ODP/TeRM compatibility is assumed.
The only adopted mechanism is the explicit hot-memory/slow-RPC separation.

## 6. Descriptor, registration, and publication

### 6.1 Proposed handle

Use a canonical, explicitly versioned wire descriptor, not a serialized Rust
pointer/struct. Bound its size and counts before allocation. It contains:

- Format/protocol version, integrity format, supported address mode, and
  negotiated maximum read size/fragment count.
- Database UUID, partition, current request-authority epoch, owner process boot
  incarnation, reader-authority generation and session incarnation.
- **Complete** expected `ValueRecordReference` (including original batch version,
  batch digest, physical segment generation, descriptor and member checksum).
- Region ID/generation, slot ID/generation, checked offset and encoded member
  length, immutable envelope length, and total registered region bounds.
- Remote RMA base/address and provider-issued key. Distinguish these from the
  local destination's opaque `fi_mr_desc`; that descriptor is never sent as a
  transferable pointer. For `FI_MR_VIRT_ADDR`, send virtual base plus checked
  offset; otherwise use checked zero-based MR offsets. Never substitute disk
  `PhysicalValueAddress.offset` for this address.
- Grant ID, grant generation, allowed peer/session, admission/renewal limits and
  expiration information as defined in §7. A client stores its own monotonic
  deadline conservatively; no absolute cross-host `Instant` is serialized.

The immutable registered envelope repeats format, owner/region/slot generation,
member length and full-reference identity, with a canonical header checksum.
Compare it with the expected descriptor after DMA and before decode. The
checksum detects corruption; it does not establish authorization. The envelope
and member stay unchanged from publication until all grants/DMA retire. The
mutable grant registry is **not** remotely read as an unproved seqlock.

### 6.2 Region/key scope and memory layout

Start with a small, configured number of preallocated registered regions per
owning endpoint/domain and database/partition/boot incarnation. Do not register
all WAL mappings or all process memory. An MR exposes only resident read data,
never mutable authority, secrets, unrelated databases, or write buffers.
Request `FI_REMOTE_READ` on the owner's MR and only required local access on
client destinations; do not grant `FI_REMOTE_WRITE`/atomics. If the provider
cannot enforce the selected read-only permissions, reject that mode.

A region key can cover multiple immutable members, so it is not a per-record
logical capability. Trusted issuers obey handle bounds and grants. Record-size
MRs give finer revocation at potentially high registration cost; region MRs are
the initial candidate, with all region grants drained before region reuse.
Provider keys may be reused, so correctness must not depend on rkey uniqueness.
Every reconnect/restart changes application incarnation and invalidates cached
handles. A new slot/region generation alone does not stop old DMA.

Choose an append-only allocation policy within each region: fill unpublished
slots, publish them once, and never overwrite published slots individually.
Seal and retire a region as a unit; use bounded size classes/oversize bypass to
limit fragmentation. Padding and unfilled/retiring regions still count against
the resident budget. This makes the first protocol boring and avoids mutable
object or per-cache-line RDMA ordering assumptions. More granular reuse is a
separate proven optimization, not an initial requirement.

### 6.3 Publication order

1. Capture current reader authority and exact reference. Validate bounds,
   partition/database/original epoch, complete `RegisteredBatch` membership,
   retained-file admission and `group_end <= durable_frontier`. Retain the
   authority/log/file owners. No logical publication can pass the coordinator's
   contiguous complete durable prefix.
2. Reserve all resident bytes, destination/registration capacity, metadata and
   grant credits before allocating/filling. If unavailable, serve ordinary RPC;
   do not evict a pinned owner or grow a side cache.
3. Read the encoded member through the existing bounded read worker/executor.
   Retire its file I/O before any provider access to the new slot. Fill the
   immutable envelope and full member. If local verification is omitted, client
   member verification remains mandatory; corruption cannot become a successful hit.
4. Complete MR registration/required setup and both peers' AV insertion.
   Establish CPU-to-device visibility with the audited provider/platform memory
   contract. Rust release/acquire synchronizes CPU threads; by itself it is not
   a proof of remote DMA order or cache coherence on every architecture.
5. Under the same admission synchronization used by fence/retirement, recheck
   authority and install the exact grant with all owner references/credits.
   Only then send the descriptor/ready acknowledgment. The immutable slot is
   never exposed in a partially filled state. A lost grant reply still owns a
   counted pin; retries use bounded request/grant identity rather than duplicating
   unlimited grants.

No remote descriptor table lookup is required in the first design. Opportunistic
bounded handles may be returned with a normal verified RPC response. In that
case descriptor setup cost must be recorded, not concealed in a warm-up label.

## 7. Read grants, fencing, and reclaiming memory

A **logical read-cut pin** preserves which committed version may be read. A
**residency grant** preserves authority to access an immutable registered
allocation and its lifetime. They are different. Capturing a cut does not pin an
MR, and an MR does not make a stale logical reference current.

### 7.1 Client read

After the existing local verified-cache miss and exact index resolution:

1. Retain `IndexReadPin` for the captured cut. Look up a bounded handle by the
   full reference and current endpoint/session/boot/authority generation.
2. Require a live local grant, unexpired operation deadline and healthy authority.
   Reserve read request/byte/CQ/destination credits. No guard spans a network wait.
   A missing/expired/unregistered/oversize handle goes straight to message RPC.
3. Post a read into exclusively provider-owned registered destination memory.
   Keep its context, MR, reference, grant, pin, and credits until the terminal
   completion or a proven endpoint teardown. A returned operation ID is local
   bookkeeping, not a remote durable acknowledgment.
4. After a successful CQE, compare envelope/incarnations/length/full reference,
   run the current member decoder/checksum, then the existing point/scan key and
   tombstone checks. Check the local grant/authority generation/health again at
   result acceptance. A complete wrong-generation read is not a cache hit.
5. Return immutable local bytes whose owner survives until the consumer drops
   them. Recycle the DMA destination only after both DMA and all local consumers
   retire. Release the logical pin at the existing result ownership boundary.
   Completion order never changes caller order or duplicate-key semantics.

If a member exceeds the negotiated RMA operation limit, the initial design uses
RPC. Later chunking must reserve all fragments, validate one immutable envelope
and complete full-member integrity, and handle partial failure without exposing
partial results. Do not assume adjacent `fi_read`s or RMA and messages are ordered.

### 7.2 Authority barrier, not a cached boolean

A pure read cannot observe an unannounced fence/poison at the target CPU in an
asynchronous system. Reading a generation word before/after the value does not
solve this race by itself. To preserve the current authority contract without a
validation RPC on every hit, **introduce an explicit read-grant barrier before a
fence/seal/retirement is acknowledged**:

1. Transition the owner to `Revoking`: stop new grants/renewals and relevant new
   read admissions. Existing grants define the still-authorized pre-fence read
   interval; do not publish the new authority as active yet.
2. Send revocations over the bounded control/message session. Each trusted client
   stops issuing with that grant, closes local acceptance for new/late results,
   drains all already posted RMA operations to terminal CQ results or closes its
   endpoint successfully, then acknowledges the exact grant generation. CQ drain
   includes cancelled/timed-out reads, not only successful user requests.
3. When every live holder is quiescent, invalidate grants and advance the existing
   reader generation/fence state. Only then acknowledge the fence and admit the
   new epoch. Reads admitted before the barrier may linearize before it; no
   old-grant result may be newly accepted after client revocation acknowledgment.
4. If any holder disappears, a timer is **suspicion**, not evidence that DMA has
   stopped. Revoke hardware access and obtain the provider/device-specific
   quiescence proof in §7.3, or fail closed. Do not acknowledge a safe completed
   fence based only on elapsed time or an empty target CQ.

This is a proposed extension to `NodeInventory::{fence_to,seal}` and grant
admission, not a feature already provided by `IndexReadPin` or `ReaderAuthority`.
The exact linearization contract must be reviewed with existing failure tests
before enabling reusable handles. If that contract cannot be established, use a
post-read validation RPC (and report its round trip), or retain message reads.
Do not silently weaken epoch/health checks to obtain a single-READ benchmark.

A bounded grant duration limits how long a healthy client may start reads and
bounds cached handles, but does not authorize owner reuse by clock alone. The
initial protocol needs no cross-host clock-synchrony assumption for safety:
normal renewal/revocation uses explicit acknowledgments and drains. A lease
scheme that reclaims after expiry would additionally need specified clock drift,
operation overlap bounds and hardware revocation; none are assumed here.

### 7.3 DMA quiescence and failure

Separate: **stop application admission**, **stop remote access**, **retire local
DMA**, and **release Rust memory**. None implies the others automatically.

- For cooperative eviction: remove region from handle publication, revoke every
  region grant, wait for exact requester drains/acks, deregister the MR, then
  release/recycle bytes and metadata credits. A checksum is not a replacement for
  this order; DMA reading freed Rust memory is invalid even if verification fails.
- For a lost peer/network partition: do not rely on TCP disconnect, absence of
  target read events (`FI_RMA_EVENT` is unsupported), or expiry of a logical pin.
  Close/revoke the affected endpoint/MRs using a reviewed EFA lifecycle. Prove
  that previously admitted remote operations cannot access reused pages after
  successful teardown. This is a hardware/provider gate, not established here.
- `fi_cancel` is not supported by `efa-direct` [F2]. A request timeout or dropped
  future ends the user's wait, not provider ownership. Keep posting/CQ progress
  and retirement alive independently of user-visible result delivery.
- If teardown fails or is uncertain, quarantine the entire affected registered
  allocation/contexts and its admission permits within a fixed process budget.
  Do not retry via an opaque pointer unless the provider contract says it is
  still live. At the quota, refuse new grants/RMA and require process restart;
  no unbounded leakage or fallback that frees live DMA memory. Ordinary message
  reads may continue only if their own authority and resource budgets are sound.
- Pinned source caveat: [`efa_mr_close`][F7] calls `efa_mr_dereg_impl` and frees its
  C wrapper even if deregistration reports an error; optional MR tracking warns
  about in-flight local operations rather than supplying a general remote drain.
  Thus the current generic send/receive teardown convention is **not sufficient
  evidence for remote-exposed-MR reuse**, and “close error means retryable FID”
  must not be assumed. Audit the exact library/driver error lifecycle first.

Owner crash/restart destroys the old residency service; clients invalidate all
old handles, fail old reads, reconnect using a fresh boot/session incarnation,
and acquire fresh grants only after normal recovery/fencing. Do not persist or
restore remote pointers/keys in checkpoints. A late old CQE cannot complete a
new request: keep stable operation contexts alive and do not reuse their IDs
until retirement; compare operation and session generations. Fresh incarnation
validation is necessary but cannot replace device teardown before address reuse.
RF1 loss of the owner's instance-store WAL remains data loss; RMA adds no replica.

### 7.4 Logical GC and relocation

Retiring a resident **copy** is not WAL deletion. A disk file can be retired only
under the existing durable recovery/reachability proof, retained cuts and local
pins **plus any remote grants that could still authorize it**. Initially retain
its authority/file/log owner for the full grant lifetime and veto retirement of
that region's named files until revocation/quiescence; do not let a copied value
outlive the authority needed to explain it. Do not scan an unbounded grant map
on the network owner: maintain bounded per-file/region pin counts.

For future relocation, publish the new physical reference only through normal
complete durable batch/index rules, with the native comparison against newer
logical writes preserved. The old reference remains valid only for its retained
cut/grant, until drained. A new MR address is not a new logical version. Never
retarget an old handle in place, recycle its slot early, or require a global
physical WAL to compare independent partition offsets. This proposal does not
add value relocation or physical GC beyond the fork's currently supported
sealed-file retirement boundaries.

## 8. Rust/libfabric/FFI changes required later

Keep unsafe code inside `tidehunter-fabric`, use the existing official generated
`ofi-libfabric-sys = 0.2.0`, and add no handwritten provider ABI or new runtime.
The package [wrapper header][F8] already declares `wrap_fi_read`, `wrap_fi_readv`,
`wrap_fi_readmsg`, `wrap_fi_mr_reg`, `wrap_fi_mr_key`, `wrap_fi_mr_desc`,
`wrap_fi_getopt` and `wrap_fi_cq_readerr`; the crate exposes the generated inline
aliases as the current source's `ffi::fi_*` calls. No claim of compiling the
proposed RMA calls is made here.

Required changes, as concrete responsibilities rather than code stubs:

1. **Capability discovery:** extend `Info::hints`/`Resources::open_mode` with an
   explicitly negotiated RMA profile (`FI_MSG | FI_RMA | FI_READ |
   FI_REMOTE_READ` as applicable to each role). Validate returned endpoint type,
   selected fabric, modes, caps, local/remote permissions, address mode,
   `tx_attr.rma_iov_limit`, message/RMA maxima, and completion/queue budgets.
   Device-path qualification checks provider device-read support rather than
   accepting an emulated read as hardware evidence. Preserve message-only opens.
2. **Owned registrations:** add separate stable owner/destination region owners
   retaining domain lifetime, allocation base/charged capacity, FID, local desc,
   exported key, region generation and pin counts. `FI_MR_LOCAL` means register
   local read destinations; `FI_REMOTE_READ` means register the owner's source.
   Registration failures consume no published grant. Domain/MR/endpoint lifetime
   must be statically or dynamically tied, not encoded as a free raw pointer.
3. **Posting:** add a bounded nonblocking read post with checked remote bounds,
   length/IOV limits, local destination capacity, grant state and a stable
   `fi_context2`. Success transfers exclusive destination ownership to the
   provider. `-FI_EAGAIN` transfers none: retain the admitted operation, progress
   once per bounded sweep, and retry without spinning/sleeping or double charge.
   Other post errors fail the operation/session according to known acceptance.
4. **Completion:** extend `Resources::complete` with a distinct read-context
   registry/slot class. Do not use the current send-prefix cursor as read
   completion or read MSG `len` padding as a guaranteed transfer length. Track
   expected length from the posted operation, recognize context/type exactly,
   and expose bytes only after a successful terminal CQE. Correlate
   `fi_cq_readerr` context/flags/error with its owning operation when available;
   uncorrelatable or duplicate/unknown CQEs poison the endpoint without releasing
   unrelated live owners. Keep success and error retirement exactly once.
5. **Progress/teardown:** reserve sufficient CQ/TX entries for all accepted RMA,
   message, receive, and control operations, plus reviewed provider reserve.
   Revisit the current `2 * SLOTS` CQ polling batch and creation geometry.
   Poll in fair bounded sweeps even when no message arrives. Endpoint success,
   MR deregistration, key invalidation and remote quiescence each need explicit
   state; uncertain teardown quarantines without reconstructing/freeing a Box.
6. **Engine integration:** grant request/reply/renew/revoke/drain use existing
   bounded framing/request IDs/control dispatch. Store handle cache and grants
   under the existing `ReadService`/storage-read ownership, not a second runtime
   or middleware. Wire fence/retention exclusions before enabling the fast path.
   Extend connection-pool identity with transport/security mode, RMA capability
   profile, boot/session/configuration generation; never reuse a TLS lane as
   native or a native lane with stale grants after reopen.

Read-only RMA does not need `fi_write`, remote atomics, `FI_FENCE`, remote
completion events, or one globally shared index MR. If an implementation proposes
one of these, review its new correctness requirement rather than adding it for
symmetry.

## 9. Bounded RAM and memory accounting

A hard resident-memory limit includes live, unpublished, sealed, revoking and
quarantined regions, not just a cache's logical value lengths. Independently
bound region count, registrations, grant count/per-peer grants, handle cache
entries, pending acquisitions/renewals/revocations, outstanding read operations,
read bytes, local destination/result owners, and control queue bytes. Every
progress and eviction pass has a fixed work bound and deadline.

Charge at admission:

- Allocated/pinned page capacity, size-class padding, immutable envelopes, idle
  region tails, and any duplicate WAL/cache/resident copy.
- Registration and provider bookkeeping using an audited conservative bound or a
  separately hard-bounded registration quota. Include memlock limits and provider
  MR-cache retention; RSS alone does not measure pinned memory.
- All retained descriptor arrays/`Arc` owner headers, grant and handle tables,
  per-file pin entries, operation contexts and CQ allocation; charge fixed table
  capacity even when empty.
- In-flight local destination capacity **and** completed bytes retained by the
  caller. A live result is not immediately reusable DMA scratch.
- Failure quarantine at its full charged size, with admission permits retained.

Use the existing fixed four-way/CLOCK convention for derivative lookup policy
where suitable. Do not mutate/evict live registered bytes just because an LRU
entry is removed. Prefer sharing one safely owned encoded allocation where the
native and fabric lifetimes permit it, but do not call a borrowed `Bytes` view
registered without an explicit stable-allocation owner. If two caches coexist,
use a shared allowance or debit both explicitly. Oversize/capacity/registration
misses use RPC. Never pin every on-disk member to improve a warm benchmark.

Because remote hits bypass the server CPU, server LRU recency cannot observe
them. Start with bounded grant renewals/reuse counts as a coarse retention hint;
optional sampled per-client counters must themselves be bounded and costed.
TeRM's periodic bitmap/hotspot pulls are research guidance, not required extra
telemetry or an unbounded key-index replica.

## 10. Misses, stale data, and failure outcomes

| Condition | Required behavior |
|---|---|
| Exact local verified-value hit | Existing local return, with existing authority health semantics; no RMA. |
| Handle absent, unknown residency, evicted region, expired grant, unsupported size, no registration credit | Existing message read at the same exact reference and captured cut. Optionally acquire a bounded handle with that response. No blind RDMA probe of an unpinned address. |
| Logical key updated/deleted after captured cut | Existing snapshot/read-cut semantics decide the reference. Handle lookup cannot choose the latest version; a handle for another full reference is a miss. |
| Revocation races a post | Serialize grant-state change and read admission. The winning post stays counted until terminal retirement; revoked generation cannot accept a new result. |
| Old address/key/slot/boot generation | Do not post if known stale. Reject mismatched completed envelopes. Reacquire over a healthy current message session; never repair by changing the expected reference to match bytes. |
| Timeout, lost control connection, owner crash, unknown CQ failure | Fail the current read/authority as existing failure semantics require; preserve provider-owned memory until CQ/teardown proof. Do not mask uncertainty with automatic transparent retry on TCP. |
| Member/header corruption or wrong full key/reference | Surface integrity/identity failure and poison as appropriate; do not disguise it as cache miss or silently overwrite evidence with a successful fallback. |
| Cold owner or missing metadata | Use bounded read worker, authoritative descriptor lookup and native WAL I/O. A missing authoritative file is an error, not a cache miss. |
| Capacity exhausted by revoking/quarantined owners | Refuse new RMA admission; continue RPC only within its independent healthy bounds, otherwise fail closed. No arbitrary sleeps, unlimited retry, or heap growth. |

## 11. Staged implementation and evidence gates

This is a proposal for subsequent work, not work performed by writing this file.
Do not enable a partial RMA path with validation or fencing disabled.

### Stage A: trusted two-sided baseline and hardware measurement

Implemented locally on `perf/efa-native-messages-2026-10-21`, based on
`fe683e7fad3f517e2f02b7c5f66f37fcdce257e1`. It is not pushed or hardware-qualified.
The baseline keeps the storage handler and every existing WAL authority check;
only the transport's software TLS layer is omitted in explicit trusted modes.

| Current implementation | Mechanism / exact source adaptation |
|---|---|
| `tidehunter/src/distributed/tcp/fabric.rs:215,267`, `Transport::{EfaNative,FabricTcpNative,is_native,open}` | Distinct application trust modes and explicit provider choice. `efa-native` means direct two-sided EFA messaging, not `fi_read`. |
| `tidehunter/src/distributed/tcp.rs:69,78,782,1092,1566`, mode preface, `StorageNode::connection`, `Client::connect_shared`, `TlsFiles::native_mode` | Bounded native-only mode/version setup, no TLS config/certificate reads in native mode, empty credential paths required. Legacy TLS handshake/ALPN is unchanged. |
| `tidehunter/src/distributed/tcp/reactor.rs:252,353,819`, `Session::{new_native,write_native,read_transport}` | Native frames directly use owned endpoint slots and bounded receive buffers. This applies eRPC-style exclusive ownership and removes TLS deframing/buffering; it does not import eRPC hardware timings. |
| Shared `Session::{finish_output,abort}`, `ReaderAuthority`, storage/codec | Keep send-fence retirement, bounded credits, quiescence/quarantine, epoch/membership/recheck, durable whole-batch publication and native CRC. Stage A originally used BLAKE2b members; §2.1 records the later CRC cutover. No original Tidehunter mutex is removed, and no new shared session lock is added. |
| `examples/support/tcp_cluster.rs`, WAL and transport-floor examples; `experiments/sui-checkpoint` runners | Certificate-free preparation and distinct result labels. Software-native is loopback-only. Sui Rust code and its API are unchanged. |
| `experiments/aws-sharded-wal/{checkpoint_matrix,ci_prepare,ci_campaign,sui_checkpoint,checkpoint_kv}.py` | Future CI full suite includes `efa-native`, exact `efa-direct` provider proof, all six host counters and native-zero-TLS diagnostics. A rebuilt same-source `fabric-tcp-native` real-Sui replay/full-state audit is required before allocation. |

Local evidence: strict Clippy passed in ordinary/distributed/fabric builds.
Distributed tests passed 785 with 7 ignored; the fabric build passed 785 with
19 ignored before explicit software selection. All 12 selected software-provider
engine cases then ran, including six native cases covering out-of-order reads,
bad references/epochs, TLS/native mismatch, corrupt replies, large/partial frames
and deadline/disconnect ownership. Native diagnostic windows had positive
frame/provider work and zero TLS read/write/process calls. Low-level provider
ownership, stream and direct-adapter cases also passed. The two offline Python
harnesses ran 488 tests (3 skips) and 111 tests.

The rebuilt release binaries completed 17 WAL runtime cases under
`project:checkpoint-work/efa-native-local-2026-10-21/`: eight software-native/native
warm controls, eight matched software-TLS/native controls and a profiled cold
software-native case. Value/reopen audits passed; the cold case observed physical
device reads on every shard and 100 transport windows with zero TLS calls.
The separate `ram-native-qualified` control completed 200 verified RAM replies
across two sizes with zero TLS work and no WAL-read increment. Trusted-native
scope labels and zero-TLS counters are checked separately from TLS results.
One shared SSD and loopback do not establish EFA latency or native parity. No
new Sui replay or AWS run is claimed. Hardware discovery returned `-61 (No data
available)`; the production `efa-native` selector failed without TCP fallback.
The [fabric contract](../../tidehunter-fabric/DESIGN.md#trusted-native-message-mode-21-october)
records the trust boundary and implementation. No RMA implementation is added.

### Stage B: specify and model the read-grant state machine

Finalize the grant/fence/retirement linearization points, canonical descriptor,
incarnation overflow policy (checked exhaustion, never wrap into a live identity),
byte/count limits and teardown error lifecycle. Model reordered/lost grant reply,
revoke and drain messages; client pause/crash/partition; owner crash/restart;
post versus revocation; delayed CQ error and old-session completion. Acceptance:
no grant from an unpublished/non-durable/foreign batch; no fence acknowledgment
or memory reuse while an old accepted operation can violate its boundary.
If proving forced remote quiescence is unavailable, restrict to cooperative
quiescence with fail-closed restart on failures, or keep the RPC path.

### Stage C: implement real software-provider RMA ownership

Implement the actual owned registration/post/CQ/error/teardown path in
`tidehunter-fabric`, using explicitly selected `tcp;ofi_rxm` capability discovery.
No fake RMA responses or tests of only a descriptor codec in place of real reads.
Exercise two processes, then several sessions with bounded quotas. Validate
successful reads and real provider failures; test harness injections separately
cover reorder/EAGAIN/teardown paths difficult to force deterministically.
Acceptance: every accepted operation has exactly one terminal owner, timed-out
reads retain memory, no aliases to DMA-owned buffers escape, and all measured
memory stays within the configured allowance including quarantine.

### Stage D: integrate storage/reference/retention and fallback

Grant only from validated durable authority; retain per-file logical/residency
pins; add fence barriers and generation invalidation; integrate under the existing
coordinator read service. Use unchanged native/Sui callers. Exercise complete
batch/replay, old cuts, overwrite/delete/repeated keys, cross-keyspace/full-key
collisions, checkpoints/reopen, file retirement, stale handles, forced owner
restart, cancellation and mixed hot/cold progress. Compare full state against the
native/message reference paths. No user-key or published-version shortcut.

### Stage E: qualify EFA hardware, then performance

Only a separately authorized hardware run can establish attached EFA device
read support, non-emulation, actual MR/key behavior, target AV prerequisite,
maximum geometry, DMA visibility/quiescence and sustained multi-host operation.
Save instance/device/driver/provider versions, selected caps/modes/limits,
registration/pinned-memory counts, read/CQ errors and exact binary/source identity.
Force loss/restart/retirement cases; a successful tiny remote RAM read is not
proof of WAL authority, crash consistency or safe key reuse.

Then compare, on the same isolated placement/independent-NVMe configuration:
native local, TCP+TLS, EFA/TLS, trusted two-sided `efa-native`, and trusted RMA.
Keep RF1, durable writes, member-integrity format, read cut, concurrency and memory
allowance matched. Include cache disabled, frontend-cache hit, owner resident
registered hit, handle miss, owner page-cache hit not registered, cold NVMe,
eviction churn, reconnect and mixed hot/cold populations. Run real Sui checkpoints
and full-state audits with unchanged Sui storage semantics, not only synthetic
GETs. Label software-provider runs as correctness only.

### Evidence possible locally versus hardware-only

| Local model / software provider can establish | Requires matching EFA hardware |
|---|---|
| Descriptor arithmetic/format, full-reference/key/digest verification, state-machine/failure interleavings, bounds, actual software `fi_read`/MR/CQ ownership, message fallback, recovery/state audits | Device-assisted non-emulated reads, EFA-direct caps/limits/AV behavior, driver/MR error behavior, device quiescence under remote failure, DMA visibility, NIC resources, inter-host latency/throughput/tails/CPU and independent-NVMe scaling |

The research author ran no builds, tests, runtime or AWS operations for the RMA
proposal. Stage A records the parent's separate two-sided implementation and
local validation. Stages B–E remain future RMA acceptance, not passed checks.

## 12. Performance hypotheses and unresolved choices

**[INFERENCE]** A valid cached handle may reduce server handler work, message
framing/reassembly/copies and software scheduling versus the trusted two-sided
baseline. It cannot remove the NIC round trip, PCIe/DMA, CQ observation, allocation
lifetime management or client member verification. A new handle may cost a control
RPC plus an RMA and lose to one ordinary value RPC. Large values may amortize
setup; churn and rare reuse may not. A compute-local hit remains a different and
usually cheaper operation. No 5-microsecond target or native-equivalent remote
latency is promised.

The retained investigation at
`project:checkpoint-work/remote-wal-read-investigation-2026-10-21/` separates
exchange from server record work and member verification. Exchange contains
verification and handler work; do not label it pure wire time. The RAM control's
extra request-retirement wait and payload validation do not establish a hard
latency floor. One-sided hypotheses must be judged against the **new trusted
message baseline**, not assigned all of a historical TLS/EFA gap as a benefit.

Measure whole verified get p50/p95/p99, complete get-many latency/throughput,
client/server CPU per operation, bytes/operation, handle hit/acquisition/renewal
cost, registration churn, CQ poll/dispatch cost, resident and pinned bytes,
revocation/fence/restart time, mixed-workload fairness and cold-device I/O. Report
population sizes and uncertainty; do not sum overlapping stage spans or divide a
complete batch latency into a fabricated scalar latency.

Decisions that must be closed before enabling reusable RMA handles:

1. **Fence semantics:** approve the explicit pre-fence grant interval/barrier and
   prove observed poison/epoch failure is not hidden. Otherwise retain post-read
   validation RPC or messages; no single-read marketing claim.
2. **Forced quiescence:** audit exact EFA/libfabric/driver teardown and failed-MR
   lifecycle, with hardware evidence. Cooperative drain is not a lost-peer proof.
3. **Region layout/revocation granularity:** choose bounded region size classes,
   MR/domain sharing scope and maximum grant duration/credits from workload and
   memory evidence. Region-wide pinning can waste RAM; per-member registration
   can waste latency and NIC resources. No unbounded dynamic compromise.
4. **Memory ownership:** decide whether existing exact-size read-cache buffers can
   safely share stable registered ownership or a debit-both copy is required.
   Define memlock/provider-cache bounds and completed-result charging.
5. **Integrity format:** reuse the current versioned member CRC and exact
   complete-batch authority in §2.1; do not substitute unchecked value bytes.
6. **RMA feature negotiation and batching:** use returned hardware geometry,
   explicit provider identity and bounded operation counts. Decide whether the
   first fast path is scalar-only internally or can serve existing grouped reads
   without reordering, extra round trips or fairness regressions.

The invariant is not “RDMA returned some plausible bytes.” It is: **the caller
receives the exact independently verified member selected by its published read
cut, while the matching complete durable batch, authority, retained storage,
registered allocation and all DMA owners remain valid through their required
boundaries.**

## References

[P1]: https://github.com/MystenLabs/tidehunter
[P2]: https://www.usenix.org/conference/fast24/presentation/yang-zhe
[P3]: https://www.usenix.org/conference/nsdi14/technical-sessions/dragojevi%C4%87
[P4]: https://www.microsoft.com/en-us/research/publication/no-compromises-distributed-transactions-with-consistency-availability-and-performance/
[P5]: https://dl.acm.org/doi/10.1145/2619239.2626299
[P6]: https://www.usenix.org/conference/osdi16/technical-sessions/presentation/kalia
[I1]: https://github.com/thustorage/TeRM/blob/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/libterm/ibverbs-pdp/mr.cc
[I2]: https://github.com/thustorage/TeRM/blob/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/libterm/ibverbs-pdp/rpc.cc
[I3]: https://github.com/thustorage/TeRM/blob/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/driver/driver.patch
[I4]: https://github.com/efficient/HERD/blob/2fa86cb66c18adc22e3e8e6513cc3482f7c26242/main.c
[I5]: https://github.com/efficient/fasst/blob/580abef01b48be4e1ab7f0d9ac95e554cccb4ce5/rpc/rpc_datapath.cc
[A1]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html
[F1]: https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/docs/efa_fabric_comparison.md
[F2]: https://ofiwg.github.io/libfabric/v2.6.0/man/fi_efa.7.html
[F3]: https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_rma.c
[F4]: https://ofiwg.github.io/libfabric/v2.6.0/man/fi_rma.3.html
[F5]: https://ofiwg.github.io/libfabric/v2.6.0/man/fi_mr.3.html
[F6]: https://ofiwg.github.io/libfabric/v2.6.0/man/fi_cq.3.html
[F7]: https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_mr.c
[F8]: https://docs.rs/crate/ofi-libfabric-sys/0.2.0/source/wrapper.h

- **[P1] Tidehunter v2.** Local requested paper:
  [`project:papers/parsed/Tidehunter-2026-v2.md`](../../../papers/parsed/Tidehunter-2026-v2.md),
  §§2.3, 3.1–3.4, 4.1–4.4, 5. The primary project repository above is not a claim
  that its current README is the exact local manuscript.
- **[P2] TeRM, FAST 2024.** QMD `#7ccb34`; local
  `project:papers/parsed/TeRM-2024.md:49-236`.
- **[P3] FaRM: Fast Remote Memory, NSDI 2014.** QMD `#13daf0`; local
  `project:papers/parsed/FaRM-2014.md:53-249`.
- **[P4] No Compromises, SOSP 2015.** QMD `#830bf2`; local
  `project:papers/parsed/FaRM-No-Compromises-2015.md:155-244`.
- **[P5] HERD: Using RDMA Efficiently for Key-Value Services, SIGCOMM 2014.**
  QMD `#67e6a7`; local `project:papers/parsed/HERD-2014.md:103-288`.
- **[P6] FaSST, OSDI 2016.** QMD `#81ced9`; local
  `project:papers/parsed/FaSST-2016.md:47-287,388-399`.
