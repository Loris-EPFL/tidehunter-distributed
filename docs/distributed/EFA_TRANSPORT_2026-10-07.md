# Authenticated EFA transport and parity limits

Status: local checks and the EFA cloud campaign are complete. All 68 API cases and 170 node archives verified. Results are stored locally; all six instances terminated and all run-owned resources were removed. See [the complete cluster results](EFA_CLUSTER_RESULTS_2026-10-07.md).

## Measured target

The preceding independent-NVMe TCP run reports an 8 KiB warm scalar diagnostic
mean of 180.593 us: exchange 170.487 us, server handler 2.451 us, coordinator index
0.844 us and client record verification 7.927 us. The exchange includes the
handler; these values are not additive CPU percentages or a measured wire RTT.
For large writes, preparation, canonical decode and persistence remain material.
See [the full baseline](READ_PATH_AND_COLD_IO_2026-10-07.md).

## Implementation map

| Source | Change and purpose |
|---|---|
| `tidehunter-fabric/src/endpoint.rs` | Exclusive Rust endpoint owner; four-slot pipelining, CQ progress, absolute deadlines and peer-exit cancellation. Removes the old middleware shared scheduler from this path. |
| `tidehunter-fabric/src/endpoint/resources.rs` | Reviewed existing libfabric ABI boundary, registered buffer lifetime, ordered delivery and safe close. |
| `tidehunter/src/distributed/tcp/fabric.rs` | Explicit TCP/EFA/software-provider choice, authenticated address exchange and transport negotiation. |
| `tidehunter/src/distributed/tcp.rs` | Keep the TLS session and identical requests, storage handlers, receipts and value checks over the selected stream. Progress TLS chunks without a drain per record; drain at RPC boundaries. |
| `tidehunter/src/distributed/tcp/diagnostics.rs` | Measure local fabric TX drains separately from writes and reads. All existing nested stage clocks remain. |
| `tidehunter/examples/sharded_wal_bench.rs` | `--transport tcp\|efa\|fabric-tcp`, distinct result labels, mismatch rejection and shared local/external configuration. |
| `tidehunter/examples/sharded_wal_cluster_check.rs` | Run crash/recovery, fencing and uncertain-ack checks over either local transport. |
| `experiments/aws-sharded-wal/` | Freeze current source, rebuild benchmark, record binary identity, export complete results through S3 and terminate only this campaign's resources. |

The native engine, Value WAL format, logical ordering, publication and persistence
rules do not change. Sui and the older middleware receive no changes.

## Correctness rules

Tidehunter paper sections 3.1/3.3 and the
[native correctness inventory](native_correctness_and_storage_options_2026-10-06.md)
remain the design anchor. Values retain their final local WAL location. An
allocated logical position is neither a durable nor published prefix. Complete
batches, authority epochs, exact retry identity, reference membership, CRC/digest
checks and durable receipts remain unchanged.

Mutual TLS verifies identities before any fabric address is accepted. ALPN binds
the requested transport. A bounded address exchange and ready marker finish over
TCP, then the same TLS record stream moves to FI_EP_RDM. TCP remains the default
and an explicitly selected fallback on a fabric-enabled listener. Required EFA
fails closed if unavailable; there is no silent software or TCP substitution.
After request submission, an error can mean an unknown outcome; never retry it
automatically using another transport.

This is two-sided EFA message RPC, not one-sided remote WAL access or NVMe-oF.
It retains encryption and server authority checks. No WAL mapping or registration
key is exposed to a peer. The FFI invariants and exact source inspirations are in
[the transport design](../../tidehunter-fabric/DESIGN.md).

## Performance hypothesis, not a promise

EFA can reduce kernel networking and wakeup costs. It cannot remove client
hash/verification, serialization, preparation or NVMe persistence. Idle active
polling costs CPU and must be included when interpreting results.

A serial remote warm get still includes a network exchange and checks that a
local page-cache get avoids. Matching native single-call latency for all sizes is
not established and must not be promised. Aggregate throughput and parallel cold
reads can benefit from independent drives; the previous cold 32-key run already
had 2.011x native throughput. If EFA reduces exchange enough to expose another
stage, optimize that measured stage next. One-sided memory access additionally
needs a reference/retention/lifetime protocol and a cold-storage path; NVMe-oF
alone does not provide those rules or eliminate coordination.

## Validation and cloud gates

The laptop uses one shared SSD and a software `tcp;ofi_rxm` provider. It can test
protocol semantics, fragmentation, deadlines and process failure, not RDMA speed.
AWS must use fresh source archives and newly rebuilt binaries, explicit EFA
interfaces, an exact EFA provider check, independent instance NVMe and identical
native/TCP/EFA API cases. Preserve warm/cold evidence, complete values, reopen,
all stage timing files, provider/runtime versions and source/binary hashes.

No data-path API success is inferred from provider completion. No timeout is
assumed to mean launch or write failure. Instances have a fixed termination timer;
results are uploaded before termination and independently verified locally.

The root AGENTS.md references `skills/README.md` and kvstore skills that are absent
from the project and installed roots. No existing skill file can be updated.
The new build feature, transport and diagnostics would require updates to the
build, wire-protocol, concurrency, FFI and performance skills if those files are
restored. This document records the current contract in their absence.

## Local evidence

- Full feature library: 534 passed, eight ignored (including the explicit
  software-provider integration test).
- Explicit TCP/software-fabric protocol run: all nine passed, including
  authentication, framing, diagnostics, uncertain durable replies and recovery.
- Native wrapper: unit tests and all four software-provider integration tests
  passed, including 2 MiB fragmented transfers and timeout/poison lifetimes.
- Separate-process software-fabric validation passed coordinator death, sealed
  recovery, missing-node refusal, node restart, partial fencing, exact retry,
  stale epoch and unauthorized peer cases.
- Strict Clippy passed for the wrapper, main library and both changed examples.
  A TCP-only build without the native feature also passed.
- 24 initial matched native/TCP/software-provider API cases and six final-build
  confirmation cases passed all value/reopen audits, with zero swap delta.
  Requiring `efa` on the laptop failed explicitly and produced no COMPLETE file.
- 36 local deployment tests passed, including the EFA interface, exact hardware,
  disk selection, cost/lifetime bound, source identity and uncertain launch gates.

The initial three-repetition local warm 8 KiB/B32/W32 medians were 107.101 ms
native load, 214.181 ms TCP and 295.531 ms software fabric. Warm scalar p50s were
2.469, 25.419 and 44.683 us. At 1 KiB/B1/W1 the scalar p50s were 0.493, 28.350 and
14.525 us. These mixed software-provider results do not predict EFA hardware
performance. All phases, p99s and process CPU costs are preserved under
`logs/efa-transport-2026-10-07/`, including `local-comparison.csv` and both local
receipt files. Final source differs only in imports/documentation and package
license metadata from the initial measured source; the final six cases verify
that exact rebuilt deployment binary.

Final deployment source: `f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398`.
Local executable: `ed4fb2896d932b1acbc031a93fcbf769378b0f1e1d37077a20a8981c467cbbbd`.
The local native runtime is libfabric 2.6.0; its binary hash and path are in the
frozen manifest. Cloud builds must record their own runtime and executable hashes.

## Bounded cloud plan

Run `th-1090e817f2952374` was rejected with `InsufficientInstanceCapacity` in
us-east-1a. A scoped MCP inventory confirmed zero instances and unchanged
protected instances. Its resources were fully removed; the cleanup receipt confirms zero owned volumes and unchanged protected instances. A separate immutable
run, `th-2d6015657fbdf380`, uses the verified us-east-1b subnet and the same bundle.

The plan uses six c6id.32xlarge hosts, cluster placement,
five RF1 storage nodes and one native/client controller. Each machine uses one
of its four blank NVMe drives. AWS MCP verified EFA support, instance-store
geometry, AZ offering, AMI and current Linux/shared price. The hourly fleet
estimate including public IPs and root storage is $38.7583; reserve $42, immutable
one-hour termination limit. No resource owned by other users is selected.

The fixed matrix has 68 API cases and 170 node archives: 1/8 KiB values, scalar
and 32-record write batches, three matched current TCP/EFA warm repetitions,
paired native references, the previous source, cold physical-I/O evidence and
separate diagnostics. All application value caches are disabled. Node EFA
provider identities and actual NIC counter deltas are required; setting a flag
alone is insufficient. This is a different instance size from the previous
TCP-only run, so compare transports and native on the same new hardware.

AWS MCP failed during provisioning after the bucket and security group were
created. The permitted CLI fallback reconciles only those exact run names/tags.
No failure or timeout authorizes duplicate launches. Result transfer remains S3.

The official EFA installer is 1.50.0, SHA256
`fa6dff8593d866866c13cb4640d9059835cd4efa427971f100ab40c97bef2841`, checked locally
against [AWS's checksum table](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-verify.html).
The installation uses the AMI's in-tree EFA kernel driver and the official
userspace stack. Mandatory provider initialization with device RDMA enabled must
succeed before any EFA result is accepted.

### Deployment findings

The second-zone launch succeeded: six c6id.32xlarge instances, five launched
at 16:30:44–45 UTC and the controller at 16:31:04 UTC. EFA reports
`libfabric: 2.6.0amzn1.0`. An initial readiness check incorrectly expected the
short `fi_info` field in verbose output. The actual field is `prov_name: efa`.
Both readiness and archive analysis now match that exact verbose field.
All 36 deployment tests still pass. This changes operator-side checks only;
the source archive, executable, and guest benchmark runner are unchanged.

The first SSH start failed before the controller start marker. An authenticated
follow-up read confirmed that the marker was absent. The failed intent and logs
were preserved before retrying the corrected readiness check. No uncertain
benchmark start was replayed.


## Completed cloud outcome

The EFA run passed the full matrix and hardware-counter checks. Warm 8 KiB scalar p50 is 55.655 µs EFA versus 753.709 µs matched TCP and 3.038 µs paired native. Batched writes take 1.24× paired native time. Universal native parity was not reached; small individual operations, batched reads, cold shard access and reopen remain material gaps. Full metrics, limits, source hashes, reporting fixes and next steps are in [the result report](EFA_CLUSTER_RESULTS_2026-10-07.md).

The post-run cold-diagnostic label correction was rebuilt and passed a complete local software-fabric cold/diagnostic case and strict Clippy. No cloud timings were changed, and no new fleet was launched for this reporting-only change.
