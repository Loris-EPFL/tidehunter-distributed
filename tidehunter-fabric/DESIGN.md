# Bounded libfabric message transport

11 October 2026; amended 18 October 2026. This crate originally adapted the Rust
`Endpoint` and `Resources` implementation preserved at
`project:archive/middleware-legacy/crates/rdma-fabric-sys` in the parent project.
That path records provenance; the active crate does not build or import archived
code. It deliberately omits the original shared progress scheduler. Each TLS
connection exclusively owns one FI_EP_RDM endpoint and drives its completion queue. The coordinator's
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
registry package. CRC32/IEEE detects malformed packets; TLS remains responsible
for cryptographic authentication and confidentiality.

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

TLS progress does not wait for the previous RPC's local CQ retirement before
submitting another admissible frame. The session tracks a submitted prefix and
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

Protocol/ALPN v4 retains authenticated `Point`/`ScanNoFill` admission hints and
extends bounded buffer/control-progress diagnostics. Hints never bypass membership
or client integrity checks. Both peers must use v4; older peers fail during TLS
negotiation rather than decoding changed diagnostic layouts.
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


