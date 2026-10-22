# Bounded libfabric message transport

11 October 2026; amended 21 October 2026. This crate originally adapted the Rust
`Endpoint` and `Resources` implementation preserved at
`project:archive/middleware-legacy/crates/rdma-fabric-sys` in the parent project.
That path records provenance; the active crate does not build or import archived
code. It deliberately omits the original shared progress scheduler. Each TLS or
explicit trusted-native session owns one FI_EP_RDM endpoint and drives its completion queue. The coordinator's
single read owner services its connections; bounded reusable shard workers own
server sessions. There is no new Rust runtime or remote WAL registration.

## Dependencies and provenance

`ofi-libfabric-sys = 0.2.0` is the OpenFabrics libfabric project's generated Rust
ABI and official inline dispatch wrapper, licensed BSD-2-Clause OR GPL-2.0-only.
We use the BSD option. The package's Cargo metadata, README, build script and
unsafe wrapper boundary were reviewed. It needs the system headers, libfabric,
pkg-config and libclang; its vendored feature is not intended for downstream
crates. `libc` already exists in the workspace. Both dependencies are optional;
ordinary Tidehunter/TCP builds do not need libfabric.
The direct packet codec also reuses the engine's existing `crc32fast` package
(locked at 1.4.2), optional under `native`. This adds a dependency edge, not a new
registry package. CRC32/IEEE detects malformed packets. TLS provides peer
authentication and confidentiality only in the TLS transport modes.

Sources:
- [OFI Rust bindings](https://github.com/ofiwg/libfabric/tree/main/bindings/rust):
  generated provider ABI; no hand-maintained C structs or custom C source here.
- [EFA provider](https://ofiwg.github.io/libfabric/main/man/fi_efa.7.html):
  FI_EP_RDM/FI_MSG, FI_ORDER_SAS and manual progress requirements. Current main
  also documents wait objects on newer versions; this implementation chooses
  FI_WAIT_NONE and does not assume those newer capabilities.
- [Completion queues](https://ofiwg.github.io/libfabric/main/man/fi_cq.3.html):
  a validated CQ completion ends each buffer's provider ownership.
- [eRPC, NSDI 2019](https://www.usenix.org/conference/nsdi19/presentation/kalia):
  bounded preallocated buffers, batched completion handling and a short request
  path motivate direct ownership. This is not an eRPC implementation or a claim
  to reproduce its performance.

## Ordinary EFA/software-provider invariants

Four stable 64 KiB TX and four RX slots bound application buffers to 512 KiB per
endpoint. EFA requests 128 TX and 128 RX packet entries, not four: medium messages
must allocate every constituent packet before posting. Software providers retain
their default capacities. Only one thread operates an endpoint/domain at a time;
FI_THREAD_DOMAIN permits exclusive ownership transfers, never concurrent sharing.

TX slots are immutable until their exact context completes. RX bytes are read
only after completion; slots are reposted only after all bytes are consumed.
Delivery follows receive posting order, even if CQ completion order differs.
Blocking adapter waits have absolute deadlines and poll-count limits. The
`try_send`, `try_send_vectored`, `try_receive` and `try_progress` operations make
bounded attempts and return without waiting for completions. `WouldBlock` does
not poison the endpoint or consume unposted bytes. Protocol-v4 owners check
absolute request deadlines and control-socket liveness while progressing; active
polling and idle backoff CPU costs must remain visible in measurements.

Successful endpoint close precedes MR close and buffer release. Failed close
quarantines the complete allocation and its endpoint admission permit, bounded
by 1024 endpoints process-wide. No live DMA allocation is freed after a failed
close. A progress failure poisons the endpoint. No request is automatically
replayed on TCP after an uncertain failure.

Session output progress does not wait for the previous RPC's local CQ retirement
before submitting another admissible frame. The session tracks a submitted prefix and
an independent retirement cursor. Each fully accepted frame has a send fence;
its encoded capacity, output slot and shard reply lease remain owned until the
endpoint's contiguous completion frontier covers that fence.

The endpoint computes the frontier from the oldest outstanding ordinal in its
four fixed TX slots. Out-of-order completions and slot reuse need no growing gap
set. Cancellation/close never fabricate completion. The session admits at most
32 live output frames, applies retained-byte credits, and may hold one bounded
decoded request while waiting for reply credit. TCP retires on TLS ciphertext
acceptance; fabric retires on the corresponding provider completions.

Cheap counters record submission/retirement stalls, retained frames/bytes and
reply credits. Optional diagnostic clocks record CQ poll gaps while a local send
is outstanding; those gaps are not network RTT. Aggregated peaks are the largest
individual connection peak, not a simultaneous cluster-memory measurement.

Current envelope/TLS ALPN v8 retains `Point`/`ScanNoFill` admission hints and
bounded buffer/control-progress diagnostics. Hints never bypass membership or
client integrity checks. TLS modes retain their v8 wire handshake; only trusted
native modes add the explicit mode/version preface described below.
Local send completion is not remote application receipt or persistence. The durable storage receipt
remains the only write acknowledgement.

See the [11 October implementation plan and local evidence](../docs/distributed/CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md)
and [9 October provenance](../docs/distributed/CHECKPOINT_ASYNC_READS_2026-10-09.md).
Software-provider and five-process TCP/software-fabric checks exercise ownership,
recovery and failure paths. Real EFA performance still requires approved hardware
validation.

## Bounded EFA queues and setup measurement

The reviewed provider is [AWS libfabric 2.6.0amzn1.0](https://github.com/aws/libfabric/releases/tag/v2.6.0amzn1.0),
mapping to upstream commit `3cc1095272b07ebeb5cc8f44851af6aa3eb75e41`.
The bound is deliberately specific to libfabric 2.6.0, provider ABI version
206.0, EFA protocol 4, host memory, one authenticated peer, FI_MSG/FI_DIRECTED_RECV,
FI_ORDER_SAS, and messages of at most 65,536 bytes. Other EFA provider versions,
missing NIC geometry, MTUs below 4096, changed protocol IDs, or returned queues
other than exactly 128 fail before opening provider handles. There is no retry
with default queues, reduced ordering, or software fallback. Expanding the
version range requires reviewing the internal allocation/protocol paths; packet
pool geometry is not a libfabric API guarantee.

Source-to-bound mapping, all at that exact commit:

- [`efa_prov.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_prov.c)
  sets `fi_provider.version` to
  [`OFI_VERSION_DEF_PROV`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/include/ofi.h):
  `FI_VERSION(library_major * 100 + library_minor, revision * 10)`.
  Thus `fi_fabric_attr.prov_version` is `206 << 16` for this release, not the
  public API version `(2 << 16) | 6`. Pre-launch validation caught and corrected
  this distinction. The independent provider-version fixture failed before the
  correction and passed afterward; incompatible versions still fail closed.

- [`efa_user_info.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_user_info.c)
  calls `ofi_alter_info` after EFA capability selection.
  [`util_attr.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/util/src/util_attr.c)
  `fi_alter_tx_attr` / `fi_alter_rx_attr` copy nonzero hint sizes into the returned
  attributes. Exact returned-size checking therefore does not mistake a supported
  maximum for a negotiated value on this path. These same returned attributes
  are passed to `fi_domain` / `fi_endpoint`, not overwritten after validation.
- [`efa_base_ep.h`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_base_ep.h)
  `efa_base_ep_get_{tx,rx}_pool_size` take `min(device capacity, user size)`.
  [`efa_base_ep.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_base_ep.c)
  uses these for QP send/receive WR counts.
  [`efa_rdm_ep_fiops.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_ep_fiops.c)
  uses the same values as both chunk and maximum sizes of registered TX/RX
  packet pools; its operation-entry pool starts with `tx_size + rx_size = 256`
  entries. TX grows at endpoint creation; RX grows on the first CQ progress.
  [`efa_rdm_ep_utils.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_ep_utils.c)
  posts exactly the reduced RX pool and clamps the refill threshold to that
  pool's size, so larger configured thresholds still refill when it is empty.
- [`efa_prov_info.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_prov_info.c)
  exposes the device's actual `max_msg_sz` as `nic->link_attr->mtu`.
  [`efa_rdm_protocol.h`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_protocol.h)
  and [`efa_rdm_pkt_type.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_pkt_type.c)
  define a 24-byte medium MSG header plus at most the padded 40-byte raw-address
  header. The implementation conservatively reserves 128 bytes per packet.
- [`efa_rdm_ope.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_ope.c)
  `efa_rdm_ope_prepare_to_post_send` needs an entire medium-message batch at once
  or returns EAGAIN; its eight-byte host-memory balancing can add a final packet.
  Thus a safe per-chunk bound is `ceil(65536 / (4096 - 128)) + 1 = 18`.
  Four whole chunks require at most 72 entries per direction. A further 32
  entries (four per live TX/RX operation) reserve handshake/control/progress
  headroom, and `next_power_of_two(72 + 32) = 128`.
- [`efa_hmem.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_hmem.c)
  initializes host medium/read thresholds; [`efa_rdm_msg.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/rdm/efa_rdm_msg.c)
  selects eager/medium for these chunk sizes with supported thresholds. No
  long-CTS or RDMA-read context budget is silently assumed. RNR retries retain
  and reuse existing packet entries; exhausted posts still require bounded
  application retries plus CQ progress, not a promise of EAGAIN-free operation.

Configuration is process-scoped and must be established before libfabric first
initializes. Compatible environment overrides are accepted, not merely rejected
for being present: `FI_EFA_TX_SIZE` / `FI_EFA_RX_SIZE` allow 0 (provider default)
or at least 128; `FI_EFA_TX_QUEUE_SIZE` allows 0 or 128..INT_MAX;
`FI_EFA_INTERNAL_RX_REFILL_THRESHOLD` accepts any unsigned value because the
provider clamps it to the RX pool size (default 8);
`FI_EFA_INTER_MAX_MEDIUM_MESSAGE_SIZE` must be at least 65536;
`FI_EFA_INTER_MIN_READ_MESSAGE_SIZE` must exceed 65536;
`FI_EFA_EFA_CQ_READ_SIZE` must be positive. Missing values retain provider
defaults; incompatible or unparsable values fail explicitly, never get rewritten.
Non-EFA endpoints do not inherit these EFA policy restrictions.

This bounds the negotiated TX/RX queues and their fixed packet pools, **not all
provider memory**. In particular, the pinned endpoint pre-grows a 1024-peer
reorder-buffer pool with default 16,384 pointers per peer (128 MiB of pointer
storage on 64-bit hosts, before metadata/alignment). Its unexpected/OOO and
operation pools may grow, and its CQ implementation retains a provider/domain
floor. These allocations are independent of our four application slots; no
claim is made that queue hints remove them or that allocated virtual memory is
all resident. Endpoint admission and failed-close quarantine remain unchanged.

`Endpoint::open_profile()` exposes allocation, discovery, fabric/domain, CQ/AV,
endpoint, bind/enable, MR registration, address, peer setup, and first-CQ-read
nanoseconds, plus requested/selected TX/RX sizes, MTU and provider version.
Read the profile after authenticated bootstrap to include deferred RX allocation.
These are stage measurements, not network-latency or speedup claims.

Reactor owners call one `try_progress` per bounded sweep, then use
`try_send_vectored_no_poll` / `try_receive_no_poll` to avoid repolling the CQ for
each TLS read/write. These operations cannot release ownership without an
observed CQE and cannot progress by themselves; EAGAIN requires another sweep.
The original polling methods remain available. CQ batches use uninitialized
stack storage and inspect only the provider-written, count-validated prefix.
Read `op_context` for each CQE and `len` only after matching an owned receive;
never assume a whole `fi_cq_msg_entry` is initialized. The
[pinned CQ contract](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_cq.3.md)
defines `len` for receives, not sends. This avoids zeroing eight entries on every
empty poll. Error-queue handling, poisoning, exact-context retirement, receive
order and close quarantine do not change. Deterministic dispatch tests cover maximum chunks, no-repoll EAGAIN,
partial receive/repost ownership, error CQ reads and invalid counts; an opt-in
software-provider test transfers across ring wraps with exactly one explicit
CQ poll per sweep. No EFA hardware performance is inferred from these changes.

## Separate EFA-direct packet transport (17 October)

`Endpoint::open_efa_direct(domain)` selects **provider `efa`, fabric `efa-direct`**.
It does not call the ordinary EFA message path, and absence or incompatible
capabilities produces an error, not a TCP, software-provider or regular-EFA retry.
TCP remains the default distributed transport. TLS authentication, encryption,
protocol-v4 admission checks and durable storage receipts remain above this
ordered byte-stream adapter.

### Exact source boundaries

| Source | Responsibility | Problem addressed |
|---|---|---|
| `src/endpoint/resources/efa_direct.rs` | Validate pinned provider 206.0, EFA protocol 1, MTU-sized messages, `FI_CONTEXT2`, local MR and automatic data progress | Ordinary EFA protocol 4/SAS/medium-message assumptions do not apply to direct |
| `src/endpoint/resources.rs` | Select the exact fabric; stable generated `fi_context2` storage; registered TX/RX ownership and exact CQ fences | A direct packet must not outlive its MR or overwrite another in-flight physical slot |
| `src/endpoint/efa_direct.rs` | Versioned 28-byte headers, CRC32, DATA ordinals, bounded reassembly and cumulative consumption credit | Direct FI_MSG does not supply SAS stream ordering or arbitrary-size TLS messages |
| `src/endpoint.rs` | Fragment vectored TLS output into the actual free registered TX slot; decode a scoped completed RX packet once; drive reserved control progress | Avoid three payload staging copies, duplicate checksum passes and ACK starvation after EAGAIN |
| `src/endpoint/resources/nonblocking_tests.rs` | Actual direct-state API fixtures and explicit software-provider packet-adapter exercise | Test post failure, CQ versus peer credit, ACK flush and large partial streams without claiming hardware |

The normal four-application-slot/128-provider-entry geometry and regular EFA
protocol-4 validation above are unchanged. Direct requests four physical entries
and validates the returned direct geometry separately. Unsupported additional
context or registration modes fail before live provider operations. Generated
OFI Rust bindings remain the ABI; there is no new C source or handwritten context
layout.

### Ordering, credit and lifetime rules

- Three physical DATA TX slots and one reserved CREDIT TX slot retain registered
  bytes until their exact CQ completion. Physical lifetime is owned only by
  `Resources`, not by wire-ordinal modulo arithmetic.
- The independent logical window allows 32 DATA packets before peer-consumption
  credit. A fixed 33-slot heap-backed reorder store holds at most 31 packets
  ahead of a missing head. Receive slots are processed and reposted independently
  of application consumption, so later packets cannot occupy every hardware
  receive slot while preventing the missing head from arriving.
- Each packet is at most the actual negotiated MTU. Payload is at most MTU minus
  28 bytes. TLS records and application frames can span many packets/windows;
  partial reads preserve the stream. Reassembly payload storage is bounded by
  `33 * (MTU - 28)`, plus fixed slot metadata and the shared registered buffers.
  There is no packet-time heap allocation or additional endpoint mutex.
- DATA duplicates, malformed checksums/lengths, invalid/exhausted ordinals and
  out-of-window DATA fail closed. Stale/reordered cumulative CREDIT is an
  idempotent no-op; credit beyond successfully posted DATA is invalid.
- Local CQ completion, peer ciphertext consumption and durable storage receipt
  are three different events. Only the last acknowledges a durable write.
  A provider EAGAIN consumes no application bytes, wire ordinal or send fence.
- CREDIT spends no DATA credit, uses reserved physical capacity, coalesces while
  a prior control send is live and never generates an ACK loop. Receive-only
  progress retries a control post after EAGAIN. `flush` drives pending control
  even when no TX has yet been accepted; success still means local completion,
  not proof that the peer application consumed the stream.
- Successful endpoint close still precedes MR/buffer release. Failed close
  keeps the existing bounded quarantine. Direct does not depend on unsupported
  `fi_cancel` behavior.

TX framing copies payload once into its registered slot. RX validates the scoped
completed packet once, then copies only payload into reassembly before repost.
The final reassembly-to-TLS copy remains: this is not end-to-end zero-copy,
one-sided RDMA, direct WAL addressing or NVMe-oF.

The existing opt-in `send_copy_ns` scope includes direct framing and checksum;
`receive_copy_ns` includes direct packet validation and reassembly insertion.
Counts/bytes include control packets and transport headers. These differ from
ordinary EFA's simple completed-buffer copy scopes and must not be subtracted as
identical exclusive budgets. No extra clock is read when profiling is disabled;
CQ and enclosing TLS/RPC clocks retain their existing scopes.

### Pinned source basis and local proof

The reviewed implementation is upstream
[`3cc1095272b07ebeb5cc8f44851af6aa3eb75e41`](https://github.com/ofiwg/libfabric/tree/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src),
not libfabric main/2.7:

- [`fi_efa.7`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/man/fi_efa.7.md):
  named direct fabric, unordered/MTU-bounded messages, context2/local-MR/automatic
  progress, CQ progress after EAGAIN and unsupported cancellation.
- `efa_prov_info.c`, `efa_user_info.c`, `efa_msg.c`,
  `efa_data_path_direct_entry.h`, `efa_cq.c`, `efa_base_ep.c` and util
  `util_attr.c`: direct protocol identity, normalized capabilities, stable
  operation contexts, queue admission and QP-before-MR teardown.
- [eRPC](https://www.usenix.org/system/files/nsdi19-kalia.pdf): bounded reusable
  fast-path storage and explicit ownership. Its published latency is not a
  prediction for this TLS/libfabric implementation.

Local validation ran all 44 native-enabled fabric unit cases, including the
otherwise ignored software-provider adapter: both directions transferred
1,000,003/1,000,019 bytes with partial reads and exact content equality. Seven
existing software-provider integration cases also passed. The test initially
closed one endpoint when only its local CQ had retired; retaining both endpoints
until both consumers finish corrected that test lifetime error without weakening
the production completion contract.

The laptop's exact `fi_info -p efa -f efa-direct -t FI_EP_RDM` discovery returned
`-61 (No data available)`. These are software correctness results, **not hardware
EFA-direct measurements**. Real registration, delivery/reordering/RNR, device
faults, teardown, TLS on EFA and latency/throughput still require hardware.
Local receipts and later source-bound KV/Sui results are retained under
`checkpoint-work/2026-10-17-efa-direct-local/` at the project root. No AWS
operation or launch is part of this implementation.


## Hardware discovery correction (18 October)

The first hardware qualification, `th-2e14bc3d9fcbf762`, found the exact
`efa-direct` fabric with provider 206.0, but the application's `fi_getinfo`
returned `-FI_ENODATA`. This was a request mismatch, not evidence that the
instance lacked EFA-direct and not a latency result.

The pinned provider's
[`efa_prov_info.c`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/efa/src/efa_prov_info.c#L49)
declares MR requirements which
[`ofi_check_mr_mode`](https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/prov/util/src/util_attr.c#L545-L615)
normalizes for `FI_MSG` to **`FI_MR_LOCAL | FI_MR_ALLOCATED`**. Our former
LOCAL-only hint was rejected before `ofi_alter_info`, leaving no candidate.
The command-line query allowed those requirements and therefore succeeded.

`Info::hints` now requests both flags **only for direct**. The returned-MR
validator already accepts them. Buffers are heap allocated and zero-filled
before registration, retain stable addresses and stay live until endpoint/MR
close; failed close still quarantines them. This satisfies ALLOCATED rather than
merely ignoring the provider requirement.

Ordinary EFA/software hints, API versions 1.18/1.11, identity/protocol/context/
progress/MTU/queue checks, TLS, wire framing and write durability are unchanged.
Pinned source tracing did not identify API 1.18 as the rejection cause, so it
was not changed speculatively. No new lock, C ABI or fallback was added.

The direct implementer and independent reviewer traced this match failure.
Local native-enabled fabric cases and strict Clippy were rerun after the fix;
only hardware can prove that corrected discovery and subsequent data transfer
work. The failed source `e022…` and its complete partial evidence remain
immutable. All six failed-fleet instances and temporary AWS resources were
independently verified closed before a replacement campaign.

### Corrected hardware qualification

The fresh `70454d…921aa` build subsequently completed the authorized
`th-7ffe878d1f51852d` and `th-1819bd4b6691868e` campaigns: native controls,
TCP, ordinary EFA and EFA-direct, each on six `g6.8xlarge` hosts with cluster
placement and local instance NVMe. All 15 paired Sui cells passed complete state
audits, and all 84 paired KV cases passed their completion/reopen guards.
Required hardware cells had positive send and receive counter deltas on every
host and the exact selected fabric. The MR hint correction is therefore
hardware-qualified; it is not a software-provider substitution.

EFA-direct did not establish general latency parity or a consistent speedup over
ordinary EFA. RAM-only transport controls remained tens of microseconds.
Normal delivery/stop/reopen validation does not qualify injected device failures,
all reorder/RNR conditions or production-scale availability. The full measured
scope, timings, raw artifacts, independent closure proofs and remaining limits
are in the [18 October hardware comparison](../docs/distributed/HOT_READS_AND_CHECKPOINT_ADMISSION_2026-10-16.md#18-october-completed-hardware-comparison).


## Trusted native message mode (21 October)

`efa-native` uses the existing **provider `efa`, fabric `efa-direct`** adapter
without a rustls handshake, encryption records or application peer authentication.
It is a **two-sided message transport**, not `fi_read`, registered remote WAL
access or NVMe-oF. `fabric-tcp-native` selects `tcp;ofi_rxm` on loopback to run
the same no-TLS session code locally. Neither mode silently falls back to TCP
or to an encrypted transport. Default `tcp` and existing `efa`, `efa-direct`
and `fabric-tcp` keep their TLS security and handshake.

### Trust model

No Byzantine-fault protocol is added or needed. Removing TLS is nevertheless a
real removal of **application authentication and confidentiality**, not a claim
that authentication is BFT. Trusted modes require an isolated deployment with
trusted members and control access. Do not expose their listeners to untrusted
clients. CRC and unkeyed BLAKE2b detect corruption; they do not authenticate peers.

AWS documents automatic Nitro network encryption for eligible instance pairs,
including G6, under its
[same-Region and eligible VPC/path conditions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/data-protection.html#encryption-transit).
That hardware protection is conditional on deployment, does not identify an
application process, and is **not verified by the Rust mode selector**.

The existing `tls` configuration container remains for compatibility of callers.
For a trusted mode, `ca`, `cert` and `key` must be empty; omitted fields deserialize
as empty. Nonempty paths are rejected, not loaded or used as a fake identity.
`server_name` is informational in these modes. TLS modes still load and validate
real credentials. The complete serialized configuration, including transport,
remains in the connection-pool key; native's absent credential fingerprint is
not an authenticated identity.

### Implementation and preserved boundaries

Paths below are relative to the engine checkout; symbols identify this working
branch `perf/efa-native-messages-2026-10-21`.

| Code | Change and invariant |
|---|---|
| `tidehunter/src/distributed/tcp/fabric.rs:215`, `Transport::{EfaNative,FabricTcpNative,open}` | Explicit security/provider selection. `EfaNative` opens the real direct hardware fabric; the software counterpart is separately named. |
| `tidehunter/src/distributed/tcp.rs:69,78,782,1092`, mode preface and client/server setup | Only native setup uses the bounded eight-byte mode/version preface before address bootstrap. TLS first bytes/ALPN remain unchanged. TCP carries setup and control-liveness checks, not native payload frames. |
| `tidehunter/src/distributed/tcp.rs:1566`, `TlsFiles::native_mode` | Explicit empty-credential validation; native config construction does not read certificates or create rustls connections/verifiers. |
| `tidehunter/src/distributed/tcp/reactor.rs:252,353,819`, `Session::{new_native,write_native,read_transport}` | One owner and shared bounded framing. Native writes go to owned endpoint slots; receives fill the existing frame buffer without rustls buffering/record processing. |
| `Session::{progress_output,finish_output,abort}` | Existing frame/byte budgets, accepted-prefix/completion fences, reply credits and quarantine rules remain. A local CQE is still not a remote durable receipt. |
| `Client::revalidate_peer_at`, `Session` control-peer checks, `remote_db/transports.rs::pool_key` | TLS reuse still revalidates certificates. Native reuse has no certificate check but retains control health, fresh status, exact configuration, fencing and recovery. |
| `StorageNode` / `ReaderAuthority`, storage and codec | No change to full-reference membership, complete durable batch publication, generation recheck, retained cuts/files, member BLAKE2b or native WAL CRC. |
| `examples/support/tcp_cluster.rs`, both WAL benchmark examples | Certificate-free node/client preparation and distinct native-mode labels. No original native Tidehunter lock was removed; no new shared session lock was added. |
| `experiments/sui-checkpoint/{deployment,prepare_external,run,cloud_node,analyze}.py` | Native modes create no TLS files; software native is loopback-only. Hardware native remains a distinct complete, source-bound Sui cell and matched analysis population. No Sui Rust source/API change. |
| `experiments/aws-sharded-wal/{checkpoint_matrix,checkpoint_kv,sui_checkpoint,ci_campaign,ci_prepare}.py` | Future full CI covers TCP/EFA/EFA-direct/EFA-native. Native maps to `fi_info -p efa -f efa-direct`; all six hardware proofs and direct diagnostics remain mandatory. CI requires a rebuilt, audited real-checkpoint local `fabric-tcp-native` replay before allocation. |

This follows [eRPC's ownership/bounded-buffer mechanisms](https://www.usenix.org/system/files/nsdi19-kalia.pdf),
not its old small-object latency claims. The existing direct packet codec and
provider progress are reused. There is no new C ABI, crate, storage format, global
WAL, cache or automatic retry. The separate
[one-sided resident WAL design](../docs/distributed/ONE_SIDED_WAL_READS_2026-10-21.md)
maps TeRM/FaRM/HERD/FaSST mechanisms and the extra grant/MR/quiescence rules needed
before bypassing the storage handler.

### Exercised local evidence

Strict Clippy passed for ordinary, distributed and fabric-enabled targets.
Distributed tests: 785 passed, 7 ignored. Fabric-enabled engine tests: 785 passed,
19 ignored in the ordinary run; the software gate then explicitly ran all
12 selected engine tests, including six new native regressions. They cover
out-of-order reads, bad references/epochs, TLS/native mismatch, corrupt member
replies, partial/large frames, deadlines and retained credits after disconnect.
The large-frame test observes positive framing/provider work with **zero**
`tls_read`, `tls_write` and `tls_process` calls on both peers.

The fabric crate's 48 ordinary tests passed; its direct adapter and seven stream
tests also ran explicitly on the real software provider. Offline campaign and
Sui harness suites ran 488 tests (3 optional/systemd skips) and 111 tests.

The rebuilt release binaries completed **17 runtime cases** under
`project:checkpoint-work/efa-native-local-2026-10-21`: eight software-native/native
warm controls, eight matched software-TLS/native controls, and a profiled cold
software-native WAL case. Values were 1/8 KiB; application caches were disabled or
bounded to 8 MiB. Every case passed value/reopen audits.

The cold case recorded five distinct storage PIDs and positive physical reads on
every shard. Each of its four separately labeled cold passes recorded 770,048
physical read bytes across the shards. All 100 retained transport windows had
zero TLS read/write/process calls, with real framing/provider work. The five
retained node status snapshots had `VmSwap: 0 kB`; this is not a continuous
host-wide swap proof. Recorded cold nodes were no longer running after completion.

The rebuilt RAM-response control also completed 100 fully validated replies for
each of 256-byte and 8-KiB values on five software-native nodes. Its ten retained
peer-profile windows had zero TLS work and unchanged WAL-read counters. Native
metadata uses `trusted_native_exchange_decode_and_full_payload_validation`;
the offline reader rejects authenticated labels and nonzero or missing TLS
work counters for this mode. The control includes client validation, local send
retirement and software-provider scheduling; it is not a hardware latency floor.
The qualified run is `ram-native-qualified`; an earlier run with the old
authenticated scope label is excluded and discarded. All 60 recorded local node
processes, including that earlier run, were stopped at final inspection.

Observed `point_get_hot` p50, application value caches disabled:

| Value | Native local, µs | Software fabric + TLS, µs | Software fabric, no TLS, µs |
|---|---:|---:|---:|
| 1 KiB | 0.457 | 42.949 | 35.198 |
| 8 KiB | 1.190 | 45.935 | 39.803 |

This is **one sequential repetition with 64 scalar samples**, one shared SSD and
loopback. Native controls also vary between passes. These observations are not
a causal speedup estimate, tail-latency result, EFA prediction or native parity.
The 8 KiB coordinator-cache population observed 0.649 µs p50 with no TLS; cache
hits do not traverse either transport, so its difference from the TLS pass is
not a TLS benefit.

Local `fi_info` returned `-61 (No data available)` for EFA-direct. The current
production `efa-native` benchmark also exited 1 with **`efa-direct hardware
unavailable`**, without a completion marker or TCP fallback. The exact binary
SHA, source-file hashes, raw samples and proof are retained in `verification.json`,
`comparison-summary.json` and `hardware-native-rejection.json` under that local
evidence root. No new EFA hardware, Sui replay or one-sided result is claimed.
No AWS fleet was launched, no original native lock was removed, and nothing was pushed.

## Native-style distributed WAL path (22 October)

Implementation branch: `perf/native-wal-fastpath`, based on committed
`fe683e7fad3f517e2f02b7c5f66f37fcdce257e1` and the preceding native-message work.
The 21 October evidence above describes that earlier state, not this cutover.
This remains a two-sided service: the coordinator selects an exact reference,
the owning shard reads its local WAL, and the coordinator checks the returned
member. No one-sided `fi_read`, global physical WAL or NVMe-oF is implemented.
No Sui Rust source/API or original native `Db` format/lock changes are required.

### Mechanisms and source adaptations

Rust module paths below are relative to `tidehunter/src/`.

| Observed cost or constraint | Implemented mechanism and code map | Primary baseline and limit |
|---|---|---|
| Independent member hashing and oversized descriptors | `distributed/{types,codec,group,native_index}.rs`: `MemberChecksum`, `member_checksum`, checked member decoding and 134-byte index references. CRC covers the versioned domain, stable batch identity/version, descriptor and native mutation. | Native `crc.rs::CrcFrame`, `wal/mod.rs::Wal::read`, and the Tidehunter paper at `project:papers/parsed/Tidehunter-2026-v2.md`. Existing `crc32fast` supplies IEEE CRC32. Whole-batch BLAKE2b and outer recovery CRC remain; the 32-bit member check provides neither cryptographic authentication nor cryptographic collision resistance. |
| Resident reads paid SQ/CQ submission and retirement | `distributed/tcp/read_executor.rs`: `resident_preadv2`, `begin_sweep`, `submit_planned`. At most four synchronous NOWAIT calls, 64 KiB total and 16 KiB per member per owner sweep. Ready results avoid SQ/CQ; unsupported, partial, cold and deferred work retains ownership through asynchronous completion. | [Native WAL direct reads](../tidehunter/src/wal/mod.rs) and [TeRM MR/RPC code, pinned](https://github.com/thustorage/TeRM/tree/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/libterm). TeRM's custom mlx5 ODP is not ported to EFA. A page-cache hit is not proof of registered remote residency. |
| Native replies copied whole payloads and allocated metadata per member | `distributed/tcp/reactor.rs`: `Output::native_slices`, `Session::{queue_native_records,write_native}`. One metadata owner interleaves original member buffers; bounded stack IoSlices feed stable provider TX slots. Full wire bytes, metadata capacity and retained member owners are charged. | [eRPC ownership/progress implementation, pinned](https://github.com/erpc-io/eRPC/tree/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d), especially request/response/event-loop code. This removes one staging copy, not the required copy into registered TX storage or completion fences. |
| Per-node RPC/session gates serialized independent accepted appends | `distributed/remote_db.rs`: per-node FIFO flights and `ClientWorker`; `distributed/tcp.rs`: bounded async append request/completion state; `distributed/tcp/reactor.rs`: active append count and explicit non-append barrier. Existing one client thread is reused and its mutex is released before idle receive/yield. | [HERD request/response pipeline, pinned](https://github.com/efficient/HERD/tree/2fa86cb66c18adc22e3e8e6513cc3482f7c26242), `main.c::{process_pipeline,run_server,run_client}`: bounded outstanding work and owner processing. HERD's RAM store and RDMA-write/UD path are not copied or claimed as EFA/NVMe durability. |
| Disk persistence blocked later complete appends | `distributed/storage.rs::persist_receipts`, `distributed/worker.rs`, `distributed/tcp/control.rs`: bounded FIFO appender plus serial flusher. The flusher captures a contiguous appended frontier, releases append state during I/O, syncs every touched file and WAL namespace before the inventory witness, then publishes only the captured frontier. | Native `wal/mod.rs::{multi_write,persist_frame_file}` and completion tracking; [SpanDB write pipeline](https://github.com/SpanDB/SpanDB/blob/master/db/db_impl/db_impl_write.cc), [RocksDB WAL performance](https://github.com/facebook/rocksdb/wiki/WAL-Performance). No timed batching delay, device-atomic recovery assumption or second value log is added. |
| Cold file data must not stall the progress owner | Existing bounded planning/device owners remain. Synchronous NOWAIT is allowed only for already-authoritative plans and owned buffers; a positive short prefix is retained, not discarded or exposed as a full record. | [CETOFS, FAST 2026](https://www.usenix.org/system/files/fast26-jia.pdf), resident/cold path separation. No maintained public CETOFS implementation was established; no code or hardware timings are claimed to be imported. |

Source mechanisms are adapted to the existing Rust ownership and WAL contracts;
no external implementation was pasted in and no runtime crate was added.
Paper and implementation microsecond numbers are not predictions for EFA.

### Correctness and lock boundaries

The experimental clean cutover is batch v3 (`THBATCH3`), group v2 (`THGROUP2`),
index v4, history/local-checkpoint v2, registration/seal v5 and wire/ALPN v9.
The eight-byte member length/checksum header replaces the old 36-byte header.
Every producer, replay path and reader uses checked new references; old
experimental data/endpoints fail closed. Native `Db` remains unchanged.

Member checks do not establish complete-batch membership, durability or current
ownership. Exact batch registration, original logical version, physical
generation, read cut/key checks and authority recheck remain. An unknown or
blocked earlier write cannot be bypassed by a later durable receipt. A local
send CQE is not a durable acknowledgment.

No native Tidehunter mutex is removed. The distributed persistence mutex still
serializes actual persistence cohorts, but no append-state guard spans file I/O.
Queue mutexes and the existing client mutex remain bounded owner coordination;
this is not a claim that the whole distributed system is lock-free. Accepted
credits, file/log owners, immutable buffers and read leases survive through
their actual completion or safe endpoint quiescence/quarantine.

### Exercised local scope

Native library: 383 passed, 6 ignored. Distributed library: 791 passed, 7 ignored.
Fabric-enabled engine: 792 passed, 19 ignored. Fabric crate: 48 passed, 1 ignored.
Twenty selected software-provider tests ran explicitly; skipped hardware cases
are not counted as hardware proof. Strict Clippy and format checks passed.
AWS harness: 490 tests, 3 skips; Sui harness: 112 passed.

The rebuilt release CLI completed 16 warm cases, nine cold mapped/positional/
async cases, and three finite-cache pressure cases with full value/reopen
audits. The pressure working set exceeded combined application cache budgets
by more than four times. Evidence is retained at the project-level
`checkpoint-work/native-wal-fastpath/`; these are loopback processes on one SSD,
not inter-host EFA measurements.

Warm scalar figures below are the median of two per-run p50s, not a pooled
quantile or tail estimate. Frontend and shard application caches are disabled.

| Value | Earlier native, µs | Current native, µs | Earlier software-native remote, µs | Current software-native remote, µs |
|---|---:|---:|---:|---:|
| 1 KiB | 0.724 | 0.740 | 26.561 | 21.438 |
| 8 KiB | 1.499 | 1.433 | 37.021 | 27.398 |

This is not native parity or a causal hardware speedup. The 1 KiB all-hit
frontend-cache population instead rose from 0.514 to 0.687 µs in the same
two-run comparison; native cache hits also changed, from 0.351 to 0.412 µs.
Windowed writes varied in both native and remote runs. The 8 KiB cache-enabled
population had real misses and is not a hit-only measurement. Host swap
activity was recorded, so laptop results cannot establish cluster scaling.

The cache-disabled async cold scalar cases each recorded 64 NOWAIT fallbacks,
64 async submissions and 770,048 physical read bytes across the owned shards.
Their scalar p50s were 134.239/121.890 µs, versus native 201.838 µs in that
bounded run. Advisory eviction is not proof that every returned value required
physical I/O, and this is not a larger-than-RAM experiment.

The local miss cases also exposed about 25 seconds of five-node shutdown wall
time. A separate syscall trace located roughly five seconds per worker
**after `exit(0)`**, after userspace ring close/unmap and work drains completed.
The source review found no justified Rust/fabric workaround or established
kernel defect. Existing ten-second per-node shutdown bounds and fail-closed
timeout errors remain. This teardown observation is not charged to read latency
or claimed to reproduce on the AWS kernel.

The six rebuilt source-bound binaries then completed the real private-chain
replay through checkpoint 1775 (1,776 checkpoints), with protocol v9 and
`fabric-tcp-native`. Native and remote exited successfully; complete state and
RocksDB audits matched for `live/checkpoints`, `live/store/epoch_0` and
`live/store/perpetual`. All 17 recorded native/remote/shard PIDs were absent
after completion. The receipt and matching evidence archive are under
`checkpoint-work/native-wal-fastpath/sui-local-native/`.

The replay build manifest is
`8c28887cf4a1a14b55632bcbb2cbb223b3419998df5e10c6d535fc67a3f8ec7b`;
its source archive is
`e05ef782e55622953a96e0c83425f5690310ddab21ae30901871852dcd0dc819`.
All 320 current engine code/config inputs matched the frozen build. Its
standalone audit lockfile was intentionally generated only in the owned frozen
workspace, resolved to the selected engine/fabric paths and archived with its
hash. No working Sui manifest or lockfile was replaced.

This replay qualifies correctness, not isolated laptop speed. The remote final
process snapshot recorded 1,562,718,208 swap bytes, while native recorded zero;
the tests also shared this host. AWS has a separate hardware qualification and
source-bound rebuild gate. No new hardware result or few-microsecond remote-read
claim follows from local tests. Physical NVMe misses and remote-resident reads
are distinct target populations; an MR key or transport label cannot make cold
disk data resident.

