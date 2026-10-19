# Local TCP serving and cluster readiness

Date: 6 October 2026. **Separate-process correctness validation and 120 release cases passed.**

This extends the [bounded local database](IMPLEMENTATION_LOCAL_DB_2026-10-06.md)
to real, separately running storage-node processes. Earlier
[local campaigns](LOCAL_BENCHMARK_2026-10-06.md) remain source-bound evidence for
the in-process backend; their data and meaning are unchanged.

## Implemented boundary

One coordinator owns the logical sequence, native-routed typed cell index and
atomic publication. Five storage processes each own one partition's native Value
WAL and bounded connection/request service. Pending-batch grouping occurs in the
coordinator client worker, with complete native group append/persistence at the
node; nodes do not add another `GroupCommitWorker` queue. The coordinator reaches those processes over
persistent mutually authenticated TLS/TCP connections. The explicit backend is
`experimental-native-cell-sharded-value-wal-tcp-rf1`; there is no fallback to native
`Db` when a remote operation fails.

A complete application batch belongs to **one** storage partition. Several pending
batches can dispatch concurrently to different nodes; a window of one cannot
saturate five independent drives. A scalar get resolves its exact reference at the
coordinator and performs one request/response with that node. It does not scan all
WALs. Multi-get and scan pages scatter bounded requests across nodes concurrently
and restore the required result order. Coordinator validation, index work and
ordered publication still use one owner; node append/durability groups and logical
recovery merge retain serialization.

All five processes share this laptop's one NVMe device. This exercises framing,
serialization, scheduling, mTLS and separate process ownership. It does not
establish independent-NVMe saturation, RDMA speed, replication or Sui performance.
The authority directory has exclusive **local** ownership; trusted-node fencing is
not a distributed lease/consensus protocol excluding two independent coordinator
hosts. Deployment remains one controlled static authority and RF1 storage.

## Correctness anchors and source map

The required anchors remain the Tidehunter paper §§3.1–3.4 and §4.4, native
`batch.rs`, `wal_replay.rs`, pending transactions and the new complete-group model.
Complete application batches stay together. A logical sequence is not proof of
durability or publication. A timeout or lost reply leaves an unknown outcome.
Recovery must account for every registered stream, establish fencing and preserve
the original identity rather than allocate a fresh retry identity. CORFU's
seal-before-hole-resolution boundary and the bounded local execution discussed in
DynamoDB/TiKV remain the specific precedents in the
[architecture plan](NEXT_STEPS_2026-10-06.md#7-research-anchors).

| Source | Responsibility |
| --- | --- |
| `tidehunter/src/distributed/remote_db.rs` | Authority admission, per-node dispatch, ordered publication, direct reference reads, bounded scatter reads and full-history recovery |
| `tidehunter/src/distributed/tcp.rs` and `tcp/inventory.rs` | Versioned bounded messages, mutual TLS, negotiated read/frame limits, durable node epochs, append/read/seal/fence/recovery |
| `tidehunter/src/distributed/{codec,storage,worker,native_index}.rs` | Immutable owned wire decoding, complete native-WAL groups, local execution and native-routed typed reference index |
| `tidehunter/examples/sharded_wal_cluster_check.rs` | Five actual child storage processes, separate coordinator crash process and deterministic fault/recovery assertions |
| `tidehunter/examples/sharded_wal_bench.rs` | Native async, native file-barrier, local sharded and TCP sharded backends under the same bounded operation history and full-byte audits |
| `tidehunter/examples/support/tcp_cluster.rs` | Explicit child ownership, readiness/configuration evidence, same-port restart, resource records and cleanup |
| `tidehunter/examples/support/tls_material.rs` | One-day private test CA/client/server certificates generated with installed OpenSSL; no download |

Native `Db` retains its existing default WAL/index formats and CRC32. The compatible
BLAKE2b provider remains confined to new distributed payload integrity. Typed
native-cell images are derived index representations; startup still replays all
retained registered WAL histories. Values, retry records and reference metadata
remain retained under explicit limits. Reclamation is not enabled.

## Separate-process correctness evidence

Run from the repository root:

```sh
cargo run --offline --locked -p tidehunter --features experimental-distributed,test-utils --example sharded_wal_cluster_check -- --output logs/my-local-tcp-check
```

The output directory must be new. The successful debug run is
[`debug-check3.stdout.log`](../../logs/tcp-integration-2026-10-06/debug-check3.stdout.log),
with [completion marker](../../logs/tcp-integration-2026-10-06/debug-check3/COMPLETE)
and per-process logs/configuration in that directory. Its stderr is empty. This
run used real loopback sockets and a test-utils-only deterministic lost-response
hook; the release benchmark is built without `test-utils`.

| Scenario | Actual observation | Status |
| --- | --- | --- |
| Five real storage processes | Distinct child PIDs/listener addresses and configured partition ownership, then another fresh five-node set for reply-loss tests | Passed |
| Whole batches and reads | Real native `Db` differential audit: repeated-key order, deletes, ordered scan, old checkpoint values, full-byte scalar/multi-get values | Passed |
| Coordinator crash | Separate coordinator exits with status 17 without remote shutdown after 34 acknowledged batches/98 mutations; parent reopens complete registered inventory | Passed |
| Old retry and new epoch | Original operation returns its epoch-1/sequence-0 identity without overwriting later values; new-epoch write succeeds | Passed |
| Missing node and process restart | Killing a required node blocks authority open; restarting it on the same endpoint allows inventory recovery and full-byte audit | Passed |
| Partial fencing | Test hook durably records Preparing and fences exactly one node; normal reopen consumes a subsequent epoch after interrupted fencing | Passed |
| Reply loss after persistence | Injected drop occurs only after durable append; reconnect/exact retry resolves one group with three records, with no second group | Passed |
| Stale epoch | Fencing a newer epoch rejects old-epoch append; sealed recovery confirms the newer epoch has no fabricated batch | Passed |
| Authentication | Wrong server identity and a client certificate from an untrusted CA are refused | Passed |
| Reference membership | Exact scalar member read succeeds; a forged digest reference is refused | Passed |
| Process cleanup | Owned child handles stop/kill/reap on normal/error paths; Linux parent-death signal provides an additional cleanup boundary | Passed in exercised exits |

Two earlier harness attempts correctly failed: one invented an epoch outside the
authority's registered inventory; another requested recovery enumeration before
sealing the epoch. The harness was corrected to use the actual Preparing protocol
and seal-before-recovery rule. Neither production rejection was weakened.

The full feature library run passed **499 tests**, with six existing ignored tests:
[`test-feature.log`](../../logs/local-tcp-2026-10-06/test-feature.log).
Two focused read-chunk regression tests then passed after review fixes. The native
suite passed **383 tests**, with six ignored. A final offline/locked check of both
examples with `experimental-distributed,test-utils` passed. These are separate
pieces of evidence, not a fabricated fresh 501-test full-suite result. Protocol
unit tests cover additional bounded transport failures; the subprocess driver does
not claim to inject every malformed frame or arbitrary metadata-I/O failure.

## Benchmark and deployment configuration

Loopback benchmark cases create fresh storage processes and data directories for
each case. The coordinator reopens against the same case nodes for the recovery
audit. Logs and configuration evidence survive after temporary data is removed.
The harness generates ECDSA P-256 certificates with installed OpenSSL 3.0.13;
private keys stay in the temporary private harness directory and are not printed
or copied into result logs. TLS inputs are DER CA/leaf certificates and unencrypted
PKCS8 DER keys. Numeric endpoint addresses and verified DNS server names are
separate fields.

`--backend all` includes all four serving backends; `--backend local-all` selects
the earlier three in-process modes. TCP-only external operation accepts
`--remote-config CONFIG.yaml` and requires **one workload shape and one repetition
on a dedicated fresh node set**. Each node owns one statically configured
database/partition; silently reusing old history for an unrelated new case is
invalid. No external data is implicitly deleted or reset. Addresses, ports,
trusted CA, server identities, client credentials and bounded budgets are explicit
in `RemoteDatabaseConfig`, so the same interface can later target a cluster.
That interface does not constitute a cloud deployment or HA control plane.

The standard matrix has 1/8 KiB values, 1/32 operations per batch and windows 1/8.
Three repetitions across four backends produce **96 cases**. Each case uses 128
load batches, 32 mixed batches, 32 delete batches and 512 point-read requests.
All histories fit the bounded harness allocation; no hundreds-of-GB dataset is
needed for this initial serving comparison.

## Measurement contract

Existing scalar phases and acknowledgement labels stay separate. Native foreground
acknowledgement is a different contract from a durable TCP completion. The native
barrier row persists all registered native Value-WAL files plus directory metadata;
the sharded completion persists its complete group on the owning partition. They
are both synchronous persistence comparisons, not identical implementation costs.
Warm scalar RPC reads add a remote exchange to a native RAM/page-cache read; report
absolute latency as well as ratios.

A new `point_get_many` phase measures calls of up to 32 keys. Native and local
backends call their actual scalar read APIs within each call; remote calls group
requests by endpoint. All values and result positions are audited. Latency is for
the **whole multi-key call**, not a per-record tail latency. Scalar gets remain in
the report. A separate local-only `read_profiles.csv` diagnostic records index,
member lookup, WAL-range and verification/decode time after the main reopen audit;
its extra clocks do not contaminate normal timed read phases.

`stages.csv` additionally records worker admission, ownership, queue lock/wait,
group assembly, append and persistence counters. Worker execution overlaps, and
queue-wait sums accumulate across requests: their sum is not a serial wall-time
breakdown. Remote storage counters require real RPCs collected outside phase
timers. TCP startup/certificate work and process shutdown are separate phases.

Fresh source/Cargo and binary hashes, topology, operation/group counts, raw samples,
phase summaries, counters, child logs and COMPLETE markers accompany the release
campaign. Coordinator RSS alone does not measure the five storage processes'
combined peak. Per-child `/proc` VmHWM and VmRSS are captured before stopping; the
sum of individual high-water marks is an upper bound, not simultaneous process-tree
peak RSS. System swap deltas apply to the whole laptop and must retain that scope.

## Completed release measurement

The fresh release binary passed **96 cases** in the matched window-1/window-8
matrix and **24 separate supplemental window-32 cases**, all with full-byte/reopen
audits. The [full report](LOCAL_TCP_BENCHMARK_2026-10-06.md) records every shared
phase, latency quantiles, stage timing, read profiles, previous-local comparisons
and source/binary hashes. The main run sampled 338,352 KiB aggregate peak RSS; the
supplement sampled 275,720 KiB. Both had zero system swap delta, and every sampled
owned process exited.

Durable load-window ratios in the main matrix are **1.33–2.63×** native for local
and **1.71–4.82×** for TCP. Warm scalar TCP reads remain **101–215 µs**, or
**68.83–112.56×** native. The local warm read diagnostic attributes 56%/82% of
whole-call time to verification/decode for 1/8 KiB values, without isolating hashing.
There is no demonstrated speed parity.

Window-8 placement used only two or three nodes per window despite balanced total
placement. The separate window-32 workload reaches all five nodes per window, but
this remains evidence of request fanout rather than SSD saturation. The backend
is ready for a bounded static RF1 engineering experiment when independent disks
are needed to answer that question; it is not ready as a general HA/Sui deployment.

## Remaining gates

- Independent-NVMe experiments must measure sufficient parallel pending work,
  coordinator CPU/NIC limits, persistence overlap and cold read behavior. Loopback
  results cannot predict the amount of drive scaling.
- Authority establishment across machines needs a lease/consensus design before
  unattended competing coordinators or automatic failover are supported.
- RF1 cannot survive permanent loss of its sole value device. Async replication,
  protected-reader relocation, retry retention floors and reclamation remain open.
- Full native API/large-batch compatibility, legacy `LargeTable` integration and
  the real Sui adapter still need explicit validation.
- Startup replays all retained histories; snapshots do not yet accelerate replay.
- No AWS resources, EFA/RDMA experiments or cloud charges are created by this local
  validation. Cluster launch remains a separate authorized, bounded operation.
