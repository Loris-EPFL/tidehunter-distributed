# Bounded libfabric message transport

11 October 2026; amended 21 October 2026. This crate originally adapted the Rust
`Endpoint` and `Resources` implementation preserved at
`project:archive/middleware-legacy/crates/rdma-fabric-sys` in the parent project.
That path records provenance; the active crate does not build or import archived
code. Each TLS or explicit trusted-native session owns one FI_EP_RDM endpoint
and drives its completion queue. The coordinator's single read owner services
its connections; bounded reusable shard workers own server sessions. The
endpoint is two-sided message transport only.

Removed by the NVMe-oF-only refactor (R4): the EFA and EFA-direct backends, the
EFA-direct packet protocol and the one-sided `FI_READ` API (`RmaConfig`,
registered read regions, `ReadBuffer`, `PeerDrainToken`). Their designs and
evidence remain in the `archive/multi-design` tag; the dated sections below
that described them were deleted, and the remaining dated sections are a
design log that may name since-removed engine files.

## Portable transport boundary (27 October 2026)

The distributed storage/WAL/index protocol is shared. Transport selection
changes only the bounded message endpoint; it does not create a
provider-specific WAL, index, lock, durability, or recovery implementation.

```text
                 shared Tidehunter WAL + index protocol
                                 |
                       common engine Transport
                         /               \
                       TCP               OFI
                 existing TLS       common libfabric Endpoint
                                      /            \
                              generic verbs     software tcp
                            IB / RoCE / Falcon  (correctness control)
```

`FabricBackend` explicitly selects `verbs` or `software-tcp`; provider mapping
is centralized: `verbs;ofi_rxm` and `tcp;ofi_rxm`, respectively. InfiniBand,
RoCE and Falcon use the common verbs/RxM implementation when the selected
host's libfabric exposes a compatible device, but are not interchangeable wire
technologies or a claim that devices on different networks can communicate.

Canonical engine configuration is structured:

```json
{"kind":"ofi","fabric":{"backend":"verbs","domain":"mlx5_0","source_address":"10.1.0.2"},"security":"trusted"}
```

`Transport` is `tcp` or `ofi`; OFI security is `mutual-tls` (default) or
`trusted`. `domain` and `source_address` are optional host-local selectors:
use the appropriate local domain/device and source IP on each host, while
selecting the same provider family and security mode on both peers. Source-IP
selectors are provider-constrained: verbs accepts
IPv4 only because the pinned RxM layer lists only `FI_SOCKADDR`/
`FI_SOCKADDR_IN` (regardless of core verbs IPv6 support); software-tcp accepts
IPv4 or IPv6 when `fi_getinfo` returns a matching sockaddr. Without a selector,
all providers use their default selection. Metadata records the selected source
address only when an IPv4/IPv6 sockaddr can be parsed. For an explicit source,
the `fi_getinfo` hint is exact-compared with the returned selection;
unsupported formats remain unobserved rather than being echoed from configuration.
Peers with different backend/security fail protocol matching. Fabric selection
never retries or silently downgrades to TCP. Default transport remains TCP;
configuration types do not require native libfabric linkage, which is opt-in.

`Endpoint::provider_info` reports `ProviderInfo` fields: selected
backend/provider/fabric/domain/source, observed device/driver/PCI bus address
when available, and capability evidence. Unknown or unobserved facts remain
absent or explicitly unverified. Software-provider tests qualify software
correctness only; they do not qualify IB, RoCE, or Falcon hardware or
performance.

The transport does not alter WAL append receipts, synchronization semantics,
index updates, grant/drain ownership, or the storage's existing locks. Local
send/CQ completion is not remote application receipt or persistence. The
durable storage receipt remains the write acknowledgement.

Opt-in `ProgressStats` also samples application slot state before each CQ poll:
`tx_all_slots_pending_polls`, `rx_no_posted_slots_polls`, and
`rx_unread_slots_polls`. Divide by the same endpoint's timed poll count, not
operations or elapsed time. These are poll-weighted samples, not durations.
An RX slot still marked pending can already have an unobserved completion, so
these counters do not measure hardware receive WQEs, RxM internal credits,
RNR retries, CQ occupancy, or time spent starved.
They flow through the existing sampled read and write profile counters and the
Sui adapter's `tidehunter_adapter` metrics; unsampled/TCP profiles omit them.
They are not retroactive evidence for older runs. Disabled diagnostics do not
sample. The serialized connection-profile layout changed, so transport protocol
and ALPN version 16 rejects older peers.

Implementation map:

- `tidehunter-fabric/src/config.rs`: `FabricBackend`, `FabricConfig`, selector validation.
- `tidehunter-fabric/src/endpoint.rs` and `endpoint/resources.rs`: endpoint setup, provider discovery, bounded message ownership.
- `tidehunter/src/distributed/tcp/fabric.rs`: engine `Transport`, `FabricSecurity`, protocol matching and endpoint bootstrap.
- `tidehunter/examples/support/tcp_cluster.rs` and `benchmarks/kv-research/src/backends/tidehunter.rs`: shared engine adapter and benchmark integration.

Primary references: [OFI verbs provider](https://ofiwg.github.io/libfabric/main/man/fi_verbs.7.html),
[OFI RxM provider](https://ofiwg.github.io/libfabric/main/man/fi_rxm.7.html),
[RDMA over Falcon specification, §§6.2–6.4](https://www.opencompute.org/documents/rdma-over-falcon-spec-v1-1-pdf),
and the local [Tidehunter 2026 paper, §3](../../papers/parsed/Tidehunter-2026-v2.md)
for WAL/index/concurrency semantics. The papers and provider documentation
inform design; they are not hardware-validation evidence.

### Local qualification, 27 October 2026

The final local release builds and strict all-target Clippy checks passed.
The fabric library passed 49 tests (2 hardware/opt-in tests ignored); the
fabric-enabled engine passed 807 (41 ignored), and the comparison crate
passed 40. Fourteen low-level software-provider tests and 21 application-level
software-provider tests also ran explicitly. Five-owner TCP, software OFI with
mutual TLS, and trusted software OFI completed the standalone crash/reopen,
retry, fencing and owner-restart checks.

The raw software FI_READ probe validated 64 warm-up and 1,000 clean reads with
different control/data interfaces and actual selected source addresses. It
reported **emulated**, not native, reads. Verbs and EFA-direct hardware
admission failed explicitly on this laptop, with no fallback or result claim.
No new EFA, InfiniBand, RoCE, Falcon or independent-NVMe performance is proved.
Sui preparation and artifact admission were checked locally; this refactor
does not claim a fresh full Sui checkpoint replay.

Native and five-owner software-OFI adapters passed real API qualification.
Separate tiny functional workload cases each recorded 384 raw timing rows,
with no failed/rejected operations or unknown mutations. These are functional
checks, not a fair performance comparison or a change to tracked case sizes.
Evidence is retained under
`checkpoint-work/portable-transports-20261027T145226Z/`.

Software OFI has a material provider-memory cost: pinned libfabric 2.6.0's
[RxM RX/TX pools](https://github.com/ofiwg/libfabric/blob/v2.6.0/prov/rxm/src/rxm_ep.c#L117-L149)
allocate 1,024-buffer chunks, with a
[16-KiB configured buffer size](https://github.com/ofiwg/libfabric/blob/v2.6.0/prov/rxm/src/rxm_init.c#L51-L55).
Application queue-size hints are not a hard cap on these pools. The local
six-process software qualification exceeded the default 2,048-MiB aggregate
allowance; a task-owned cohost profile explicitly allowed 4,096 MiB and sampled
a 2,281,836,544-byte aggregate peak, with zero swap-in/out counter increments.
The default CI/TCP budget and every cloud per-host budget remain unchanged.
The existing software MSG receive setting is recorded, not presented as a
hard provider-memory bound.


## Dependencies and provenance

`ofi-libfabric-sys = 0.2.0` is the OpenFabrics libfabric project's generated Rust
ABI and official inline dispatch wrapper, licensed BSD-2-Clause OR GPL-2.0-only.
We use the BSD option. The package's Cargo metadata, README, build script and
unsafe wrapper boundary were reviewed. It needs the system headers, libfabric,
pkg-config and libclang; its vendored feature is not intended for downstream
crates. `libc` already exists in the workspace. Both dependencies are optional;
ordinary Tidehunter/TCP builds do not need libfabric.

Sources:
- [OFI Rust bindings](https://github.com/ofiwg/libfabric/tree/main/bindings/rust):
  generated provider ABI; no hand-maintained C structs or custom C source here.
- [Completion queues](https://ofiwg.github.io/libfabric/main/man/fi_cq.3.html):
  a validated CQ completion ends each buffer's provider ownership.
- [eRPC, NSDI 2019](https://www.usenix.org/conference/nsdi19/presentation/kalia):
  bounded preallocated buffers, batched completion handling and a short request
  path motivate direct ownership. This is not an eRPC implementation or a claim
  to reproduce its performance.

## Message endpoint invariants

Four stable 64 KiB TX and four RX slots (the default `FabricConfig::slots`)
bound application buffers to 512 KiB per endpoint. Providers retain their
default packet capacities. Only one thread operates an endpoint/domain at a time;
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

See the archived 11 October implementation plan and local evidence and the
9 October provenance (tag `archive/multi-design-*`,
`docs/distributed/{CLUSTER_RAM_AND_READ_PATH_PLAN_2026-10-11.md,CHECKPOINT_ASYNC_READS_2026-10-09.md}`).
Software-provider and five-process TCP/software-fabric checks exercise ownership,
recovery and failure paths. Real verbs hardware performance still requires
approved hardware validation.

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
CQ poll per sweep. No hardware performance is inferred from these changes.

## Trusted native message mode (21 October)

Historical: the `efa-native` and `fabric-tcp-native` names below predate the
structured `Transport`. A trusted OFI session is now
`Transport::Ofi { security: Trusted }` over `verbs` or `software-tcp`; the
trust model below still applies to it.

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
WAL, cache or automatic retry. The separate one-sided resident WAL design
(`docs/distributed/ONE_SIDED_WAL_READS_2026-10-21.md`)
is archived at tag `archive/multi-design-*`; it
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

## Send retry, slot count, idle parking and provider facts (6 November 2026)

These changes follow the transport review of the 1 KiB/8 KiB verbs-versus-TCP
runs. They change no wire format between peers, no completion ownership rule
and no default geometry.

- **EAGAIN keeps the copy.** `Resources::send_copied` copies the caller's slices
  into a free registered TX slot once. If `fi_send` returns EAGAIN, the filled
  slot stays unposted and owns no ordinal, credit or provider resource, so it is
  an ordinary free slot (`free_send_slot`, fences and close are unchanged). The
  next send attempt compares the offered bytes with the kept copy (a read-only
  pass, not a copy) and, when length and content match, posts the same slot
  again. Different bytes are copied fresh. The counters `send_post_eagain` and
  `send_post_retries` (always on) count both; with diagnostics on, `send_copy_calls`
  shows one copy per accepted send. The identity check keeps the public
  contract that the endpoint never posts bytes the caller no longer offers.
- **Slot count.** `FabricConfig::slots` (`None` = 4) selects the number of 64 KiB
  TX and RX slots per endpoint, `1..=64` (`MAX_SLOTS`). The CQ depth, MR
  registration, completion-context arrays and ring arithmetic follow it. The
  default is unchanged.
- **Idle parking.** A server session opens its endpoint with
  `Endpoint::open_idle_parking`. Software RxM keeps its bounded `fi_cq_sread`.
  `verbs` additionally requests `FI_WAIT_FD` on the CQ, reads the descriptor with
  `fi_control(FI_GETWAIT)` and, only when the endpoint is quiescent (no send or
  receive slot in use, all receives posted), calls `fi_trywait` and, on success,
  `poll(2)` on the descriptor for at most 1 ms; then it reads the CQ without
  blocking. A nonzero `fi_trywait` means the provider has work to progress and the
  sweep polls normally. Outstanding requests, partial frames and unread slots
  never park. If the provider refuses the wait object (`ENOSYS`, `EOPNOTSUPP`,
  `EINVAL`) or returns no descriptor, the CQ is opened without one and the
  owner keeps its bounded spin/yield progress; the reason is recorded in
  `ProviderInfo::geometry.cq_wait`. Whether `verbs;ofi_rxm` grants `FI_WAIT_FD` is a
  provider property that only a real device can show: **unverified on hardware**.
  `cq_idle_waits` counts parked sweeps.
- **Provider facts at open.** `ProviderInfo::geometry` records the libfabric API
  version (`fi_version`), the version requested from `fi_getinfo`, the provider
  version, protocol, `tx_attr.inject_size`, TX/RX sizes, iov limits,
  `max_msg_size`, `mr_mode` (raw and decoded), data/control progress, threading,
  the slot count, the CQ wait outcome and every `FI_OFI_RXM_*` parameter libfabric
  reports for an RxM provider (configured override or `None`, the default parsed
  from its help text as `configured_or_default`, and the help text). These are
  configured/default evidence only. libfabric does not expose the values RxM
  finally uses, and `tx_inject_size` is not the eager threshold (64 on the
  software provider against a 16384 RxM buffer-size default), so the actual eager
  and SAR limits are **unverified**; the size buckets below are the way to look
  for them. The attributes recorded are negotiated capacities, not proof of a
  protocol choice.
  Per-endpoint `send_size_le_4k/16k/64k/gt_64k` bucket successful posts
  by length, so a size sweep can be matched to the thresholds above. The client
  side reads its facts through `Client::provider_info`; each accepted owner
  session writes one `TIDEHUNTER_FABRIC_PROVIDER_FACTS {json}` line to stderr (the
  existing local-observer line protocol, never a wire field), because
  `ConnectionProfile` is `Copy` and cannot carry the strings. The counters live
  in `TransportProgressStats` and are not yet in the Prometheus counter lists of
  `remote_db/pending_read.rs` and `sui.rs`.
- **TLS parity.** Mutual TLS runs against mutual TLS and trusted against trusted
  (`Transport::security_label`). This crate never carries TLS; it sits below the
  session that does.
