# Linux NVMe-oF sealed WAL experiment

For final-inode writes, mixed workloads and live recovery, use the separate
[live owner-WAL path](LIVE_WAL.md). The commands below remain sealed-image
controls; they are not the live write transport.

This is a trusted-root, opt-in experiment, not a fencing or Byzantine security boundary. The kernel target opens its file for read/write. Linux's temporary namespace write protection protects I/O commands **only after** the authorized initiator sets it; that target is initially published unprotected. Only a trusted host NQN on an isolated network may connect, and root on either machine can undo protection or modify the image. An NQN ACL is not authentication. Do not admit untrusted initiators. The reader must open the namespace `O_RDONLY | O_DIRECT` and independently verify identity, geometry and protection against the admitted manifest. A valid manifest alone is not database authority.

These commands manage kernel objects, not database authority. The Rust backend first pins every owner's epoch and validates/adopts each export through authenticated control RPCs. Fence, seal, retirement and terminal authority advance are refused while admissions remain. Clear/drain/close the Rust readers and release their tokens before disconnect/cleanup; an orphan admission quarantines recovery. The coordinator value cache remains supported. This backend does not claim automatic-failover availability parity.

The focused comparison admits native, TideHunter TCP RPC and at least one
NVMe-oF arm; portable verbs RPC and either NVMe/RDMA or NVMe/TCP can be selected
alongside them. TCP RPC and the NVMe readers share the TCP owner source block;
verbs uses a separate physical owner source because its endpoint transport is
different. They share a frozen logical dataset and namespace, not necessarily
the same WAL file or cache residency. NVMe-oF arms only read sealed exports;
write controls use the existing owner-WAL path, not NVMe-oF writes. First-touch
and repeat-read labels describe the requested cache procedure, not proof of
physical media reads. Coordinator value caching is disabled for these reads.

Prerequisites: Linux nvmet configfs mounted and accessible, preloaded `nvmet`, `nvmet_tcp` (or `nvmet_rdma`) on target; `nvme_fabrics`, `nvme_tcp` (or `nvme_rdma`) and `/dev/nvme-fabrics` on initiator; nvme-cli (tested syntax targets 2.8); privileges to configure nvmet and issue admin commands. RDMA needs an operational RDMA-CM reliable-connected provider and network: the existence of a device alone does not prove this, and EFA compatibility is not assumed. Set these up separately; these scripts never mount configfs, load modules, configure networking or launch cloud resources. Use distinct NQNs, port ID and private state directories per run; paths and names already in use are rejected. Both machines need the same admitted `manifest.json`; the target's manifest must reside beside its independently copied `image.wal`, whose file and ancestor directories must be root-owned and not group/world writable. The image must have one link and match manifest length. The image remains owned until namespace unpublication and all readers drain; these tools never delete it. Root-trusted provisioning must prevent replacement or modification throughout the experiment.

Example commands (replace all paths and addresses with your own, on the indicated machine):

```sh
# Both machines: non-mutating report; RDMA exits nonzero while its path is unverified.
# Repeat for --role initiator; tcp is the explicit local fallback.
python3 experiments/nvmeof/manage.py preflight --role target --transport rdma

# target: state path is new, its parent is a private root-owned directory
sudo python3 experiments/nvmeof/manage.py target-setup \
  --manifest /var/lib/tidehunter/export/manifest.json --state /var/lib/tidehunter/target-run \
  --hostnqn nqn.2026-01.example:host1 --address 192.0.2.10 --service 4420 --port-id 100
# initiator: copy the admitted manifest to a root-controlled path with the same basename
sudo python3 experiments/nvmeof/manage.py host-connect \
  --manifest /var/lib/tidehunter/manifest.json --state /var/lib/tidehunter/host-run \
  --hostnqn nqn.2026-01.example:host1 --address 192.0.2.10 --service 4420
# Only the JSON output with phase=protected authorizes considering its device for the reader.
# target: separately sample explicit underlying block device (not automatically discovered)
python3 experiments/nvmeof/collect.py --block-device nvme0n1 --seconds 30 --output /var/lib/tidehunter/target-interval.json
# Stop admission; wait for all reader I/O completions and close reader descriptors first.
sudo python3 experiments/nvmeof/manage.py host-disconnect --state /var/lib/tidehunter/host-run
# after every host has disconnected and drained, on target:
sudo python3 experiments/nvmeof/manage.py target-cleanup --state /var/lib/tidehunter/target-run --readers-drained
```

Choose `--transport rdma` on both ends only after provisioning a compatible RDMA provider; TCP is the default and is reported as TCP, not RDMA. Preflight reports missing prerequisites and active RDMA ports but cannot establish an RDMA-CM route or RC capability to the selected peer. Its `local_prerequisites_ready` permits an explicit setup/connect attempt; `ready` remains false and the command exits nonzero for RDMA until a path is proved. Discovery and the verified live controller are the actual connection gate; failure never falls back to TCP. The campaign helper accepts `--transport rdma` on target setup and carries that choice through its target receipt to host connect.

Every managed kernel target uses file-backed `buffered_io=1`, read back from configfs; owner lease admission independently checks the actual attribute. The initiator reader still uses `O_DIRECT`. The page cache belongs to the independently copied sealed export image, not the original WAL; warming one does not establish that the other is warm. Neither policy proves NAND access or a page-cache hit, and these management commands do not perform a machine-wide cache drop. The separate SPDK target policy is described below.

For kernel targets, the initiator performs discovery of the exact endpoint, makes a single new controller, sets namespace feature `0x84` to temporary protection (`-V 1`), reads it back and checks the identify namespace read-only bit, geometry and sysfs namespace UUID before returning the device. Controller loss timeout is zero. Temporary protection is not a durable promise across reset/reconnect or namespace re-creation: stop/drain readers immediately on any loss, reset or topology change, disconnect the recorded controller, and run a new `host-connect` with a new state directory before resuming. Never reuse an old device path or old success JSON. Protection is namespace-wide, not per-host; do not mix writable users. The reader must also fail closed on loss/change rather than assuming this helper monitors it.

Sealed initiator controllers (`manage.py host-connect`, `campaign_targets.py host-connect`) now request `nr_io_queues=4` and `queue_size=128` by default (`nvme connect -i 4 -Q 128`). After the controller appears, the manager reads sysfs `queue_count` and `sqsize` back and refuses admission, disconnecting the new controller, unless they equal `5` (four I/O queues plus the admin queue) and `127` (zero-based depth). The requested and observed values are recorded in the host state and campaign receipt as `queue_geometry`. Both options may be overridden together with `--nr-io-queues` and `--queue-size`; the readback rule applies to the override. This default changed: the 2026-11-06 run set no queue flags for sealed controllers, so sealed results from that run are not comparable with results from this default. The live initiator keeps its own `1 x 32` geometry (`queue_count=2`, `sqsize=31`), set in `live_connect.rs`.

The initiator independently checks the host block-device `BLKROGET`; if needed, privileged management sets `BLKROSET` and checks it again. This host guard does not replace target feature and identify verification. Multipath namespace heads are resolved through the verified subsystem and UUID; multiple controllers are rejected instead of changing global multipath settings.

Discovery and connect use an explicit private host ID derived from the supplied NQN. They do not reuse or modify `/etc/nvme` identity: Linux rejects pairing the existing machine host ID with a different experiment NQN.

The per-machine state records exact names and creation ownership. Teardown unlinks only recorded owned objects, never issues global disconnect, never removes the image or another subsystem, and refuses mismatched controller/link identity. Cleanup on setup errors is attempted immediately. A crash/power loss between a kernel operation and its state fsync can leave an unrecorded object or connection: do not reuse those names; inspect configfs/sysfs manually as trusted root and reconcile only demonstrably owned objects. If a disconnect or rollback fails, retain state, quiesce all readers and resolve it before reusing any resource. Do not run two managers for the same names. The `--readers-drained` flag is an operator attestation, not a remote drain protocol.

Collector output contains independent monotonic-time intervals for aggregate `/sys/class/block/<device>/stat` and `/proc/stat`; sectors are the kernel's 512-byte accounting sectors. It does not measure per-operation NAND, page-cache hit or miss, target stage or network latency. Unavailable cache counts and their denominator are null, never inferred from device totals. Its `trace.available=false` applies to this interval sampler only. Ingest this JSON as `nvmeof_target_evidence` in a terminal campaign record.

For diagnostic case boundaries, run `collect.py --case-image /path/image.wal --block-device nvme0n1 --output begin.json`, repeat to `end.json`, and use `collect.py --compare begin.json end.json --output delta.json`. The snapshot records the image device/inode, backing block identity and independent monotonic read bounds; the delta refuses changed or unverifiable image/device bindings and marks individual resets or missing fields unavailable. Device I/O remains shared-device evidence, not file-specific cache misses. `controller_snapshot("nvme0")` is also available to host collectors on initiators: `queue_count` includes the admin queue and `sqsize + 1` is the controller I/O queue size, not observed concurrent I/O. Attributes may be unavailable and multipath may not identify one controller. See Linux [NVMe sysfs](https://github.com/torvalds/linux/blob/master/drivers/nvme/host/sysfs.c), [NVMe RDMA queue setup](https://github.com/torvalds/linux/blob/master/drivers/nvme/host/rdma.c), and [block I/O accounting](https://docs.kernel.org/admin-guide/iostats.html).

Case deltas additionally require the same available host boot ID and positive block `diskseq` at both boundaries. A missing or changed identity makes the delta unavailable even if the snapshot's individual observations remain available; older snapshots without these identities cannot be compared.

The existing host `rdma_diagnostics.capture()` can be compared with `compare(begin, end)` per device/port. Standard PMA `port_xmit_wait` counts ticks without transmission due to insufficient credits **or** arbitration; it is not a pure credit-stall count. Provider-specific `hw_counters` retain raw names and values with unknown units and semantics, not guessed retry counts. Missing, reset, replaced-device and truncated samples are explicit; hardware counters may be cached by the driver. See the Linux [InfiniBand sysfs ABI](https://github.com/torvalds/linux/blob/master/Documentation/ABI/stable/sysfs-class-infiniband).

Live case host-local boundary snapshots begin before the client preload and end even after failure. They inspect only bounded WAL files below the validated owner root and match initiator controllers to the run's live host NQN, transport and target endpoints. Newly appearing or missing controllers and WAL files are not treated as zero at the other boundary. At most eight verified WAL files per host and boundary receive an optional two-second stock [`fincore`](https://man7.org/linux/man-pages/man1/fincore.1.html) probe; its before/after inode/device/size identity and unavailable/error state are retained. This is a boundary residency observation, not a per-I/O cache hit or miss, and depends on kernel/tool permissions. It never installs tools or changes cache policy. Diagnostic or cold-OS-cache live cases additionally attempt the existing bounded device-wide block tracer before preload and stop/export it in the failure path; these traces are not file-attributed I/O.

For target trace capture, provision tracefs with nvmet and block events enabled in the kernel, then run concurrently with the measurement on the target: `sudo python3 experiments/nvmeof/trace.py --seconds 30 --buffer-kb 256 --ctrl-id 1 --block-device nvme0n1 --output /var/lib/tidehunter/trace-run`. Replace controller ID with the actual target-side NVMe controller ID and use a new output directory. The tool creates its own randomly named tracefs instance, bounded per-CPU buffers and a 64 MiB raw output cap; it never alters global tracing. It records event formats, raw trace, per-CPU loss statistics and a summary. Ingest `summary.json` as `nvmeof_trace_evidence` in a terminal campaign record. nvmet `(controller ID, qid, command ID)` start/completion pairs measure target request lifetime; controller IDs can collide across subsystems, so isolate the target or treat this attribution as ambiguous. Block `(major,minor,sector,length)` issue/completion matching can alias, merge or split; it cannot prove association to a particular nvmet request. Unmatched/ambiguous pairs and buffer loss invalidate completeness. Raw traces support independent analysis; neither interval is pure NAND or network time. Absence of privileges or required events is a hard failure, not zero latency. The Grafana `KV NVMe-oF — qualified evidence` dashboard shows actual Rust counters and available target evidence. Run-state cache labels and collector qualification must accompany any published result.

SIGTERM runs scoped trace cleanup. After SIGKILL, once the capture process is gone, use `sudo python3 experiments/nvmeof/trace.py --recover /var/lib/tidehunter/trace-run`; recovery checks the saved tracefs instance identity before disabling or removing it. A crash between instance creation and identity persistence may leave a disabled instance requiring manual trusted-root inspection. State and output parent directories and their ancestors must be root-owned and not group/world writable.

Kernel reference points (Linux v6.17): [`target/configfs.c`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/nvme/target/configfs.c?h=v6.17) defines `device_uuid`, `buffered_io`, host ACL and port attributes; [`target/io-cmd-file.c`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/nvme/target/io-cmd-file.c?h=v6.17) opens file namespace O_RDWR and adds O_DIRECT unless buffered; [`target/admin-cmd.c`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/nvme/target/admin-cmd.c?h=v6.17) implements set/get write protect and identify RO; [`target/core.c`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/nvme/target/core.c?h=v6.17) permits only read and flush when `ns->readonly`, rejecting other I/O opcodes. These guarantees depend on the running kernel; no guessed configfs readonly attribute is used. nvme-cli command syntax: [set-feature](https://github.com/linux-nvme/nvme-cli/blob/v2.8/Documentation/nvme-set-feature.txt) and [get-feature](https://github.com/linux-nvme/nvme-cli/blob/v2.8/Documentation/nvme-get-feature.txt).

## SPDK direct-file target comparison

The sealed SPDK control changes the immutable-image target, not the Linux
initiator. It uses source-bound [SPDK 26.01](https://github.com/spdk/spdk/tree/2ef883ef96e79c3cc16da02f667a7a58c2453f2f)
with its [file AIO bdev](https://github.com/spdk/spdk/blob/2ef883ef96e79c3cc16da02f667a7a58c2453f2f/module/bdev/aio/bdev_aio.c)
verified `O_RDONLY | O_DIRECT`. The separate live arm uses the final native
owner WAL inode with verified `O_RDWR | O_DIRECT`; it is not a writable sealed
export. Neither arm grants SPDK a raw SSD. File I/O still uses the filesystem
and block stack.

`spdk_comparison: true` in a `standard-with-live-nvmeof` specification selects
separate sealed and live SPDK/RDMA comparison cases. Runtime remains
`tidehunter_nvme_rdma`; reports distinguish `tidehunter_spdk_rdma` (sealed)
from `tidehunter_spdk_live_rdma` (live). See [LIVE_WAL.md](LIVE_WAL.md) for
live reservation, durability, fencing and qualification requirements.

The kernel target is buffered; the SPDK file target requests direct data I/O.
A repeated SPDK read must not be labelled an OS-RAM-cache hit. First-touch
keys can still share pages, and direct I/O does not establish NAND access or
exclude an SSD controller cache. Only attributable measured device evidence
can support a physical-device claim. Kernel and SPDK targets use the same
export images; the owner-RPC source has the same logical values but can have a
different physical WAL layout.

### Local artifact preparation

`Dockerfile.spdk-build` retains the native compiler recipe. Resolve the parent
`tidehunter-gcp-build` image to an immutable image ID before building and retain
both IDs. Apply the reviewed patch to the pinned SPDK source before configuring
with the options admitted by `spdk_artifacts.CONFIGURE` and building before
any paid allocation. The following commands run from the SPDK source root;
`PATCH` is the absolute path to the repository patch:

```sh
git apply "$PATCH"
./configure --with-rdma=verbs --target-arch=nehalem \
  --disable-tests --disable-unit-tests --disable-examples \
  --without-vhost --without-virtio --without-vfio-user \
  --without-fsdev --without-nvme-cuse --without-uring
make clean
make -j4
```

Inside the native build environment, seal a new prefix with
`spdk_artifacts.py seal --source SOURCE --prefix PREFIX --builder-image IMAGE_ID
--library /lib64/libaio.so.1 --build-proof BUILD_PROOF.json --build-log BUILD.log`.
The retained build proof binds the clean/build commands, builder image,
source and configuration hashes, binary hash and build log. The sealer checks
pinned source and the exact admitted patch, clean submodules, configured features,
file identities and licenses. The prefix must not contain a copied
libibverbs/librdmacm or provider stack: the guest uses its installed
Falcon-compatible stack. The GCP preparer and runtime packager accept this
prefix with `--spdk-prefix`.

The admitted patch is
[`spdk-26.01-nvmf-remove-ns-status.patch`](patches/spdk-26.01-nvmf-remove-ns-status.patch)
(SHA-256 `ba21692d7ac70190a505a7b94dc749c8f544d6eef8632a38980f4acf5e987373`).
It makes pause/resume failures return RPC errors instead of acknowledging a
successful namespace removal. Preserve the clean rebuild record tying this
source to the binary hash: a source manifest alone does not prove how an
executable was built.

The package manifest records the executable's actual SHA-256 and exact build
provenance. A previously sealed read-only-only artifact is not admitted for
the live daemon. ELF/ABI admission alone is not physical RDMA qualification.
Do not combine `--no-huge` with an explicit `--iova-mode=va` for this
SPDK/DPDK build: no-huge already supplies that argument, and DPDK rejects the
duplicate before target startup.

### Protection and timing qualification

Stock SPDK's read-only AIO descriptor is a different protection mechanism
from Linux nvmet's temporary namespace write-protect feature `0x84`.
The existing kernel admission must not be weakened, emulated with ordinary
configfs files, or silently reused when that feature fails. SPDK requires
explicit owner-backed read-only/direct file, process and namespace mapping
proof, plus the initiator's checked identity, geometry and block read-only
guard. Its namespace must be drained before the export lifetime is released.

`ExportTarget` distinguishes `Kernel` from `Spdk(SpdkTarget)` throughout target
receipts, host connection, `NamespaceBinding`, authenticated owner registration
and the persisted export lease. Missing target fields in older kernel metadata
remain kernel bindings; a failed kernel check never selects SPDK. SPDK admission
checks the root-owned process start identity, executable, Unix RPC peer/socket,
actual `O_RDONLY | O_DIRECT` image descriptor, AIO read-only setting, namespace
UUID/NSID and bdev geometry. Its advertised WRITE capability is **not** evidence
of a writable backing descriptor.

Last-token release removes the exact SPDK namespace and checks its absence,
after the existing reader CQ-drain/FD-close protocol. It does not kill a daemon
shared by other exports. A real local root-process/RPC exercise admitted a valid
lease, revoked its namespace and released its pins; it rejected a replaced
process and a writable backing-file replacement. This is control-plane
qualification, separate from the NVMe/TCP data-transfer exercise below.

Each target receives a shared absolute per-case deadline, capped by fleet
deletion. Its systemd runtime budget covers setup, five host connections,
admission, the measured case and cleanup; it is recomputed immediately before
starting. An unacknowledged queued start is not certified stopped just because
one systemd lookup reports `not-found`; its ownership state remains quarantined.


The local NVMe/TCP component returned the complete 16-KiB input byte-for-byte
and exposed an actual read-only/direct target descriptor. A separate mutation
attempt produced failed remote NVMe WRITE completions. The stock `spdk_dd`
utility nevertheless exited zero and printed copied bytes; its exit status
alone is not a write-success or write-protection test. Preserve the actual
completion errors and compare the source contents.

`spdk_measurement.py` admits hash-bound start/end native `bdev_get_iostat`
snapshots for every active owner (five by default, or one for a compact fleet).
It rejects changed processes or binaries, counter resets, missing owners and writes.
Completion means are weighted by actual bdev I/O counts and exclude setup and
final verification.
Cumulative extrema cannot produce per-window p99. These are file-bdev
completion timings, not per-API, pure fabric or NAND timings.

The campaign exporter publishes the admitted read counts, bytes and mean
completion time under `kv_spdk_target_*`; the NVMe evidence dashboard has
separate SPDK panels. Baseline and instrumented populations remain separate.
No cluster performance is established by the local component qualification.

### Owned-WAL physical-I/O diagnostic

`experiments/kv-comparison/wal_extent_evidence.py` uses the Linux
[FIEMAP metadata API](https://docs.kernel.org/filesystems/fiemap.html), without
`FIEMAP_FLAG_SYNC` or raw-device reads. Directory-descriptor-relative opens
reject symlink traversal; nonblocking file opens reject FIFOs and other
non-regular replacements before querying extents. Device/disk sequence,
root/file identities and mappings must match at both boundaries.

Request-identity block traces also retain issue sectors and byte lengths.
Only requests fully inside unchanged, written, unshared WAL extents contribute
to `owned_wal_io`. Unknown, delayed, encoded, shared, inline and unwritten
extents do not qualify. Other device I/O and straddling requests remain
unattributed. Capture is diagnostic-only, capped at 32 MiB and 300,000 events;
lost/truncated or incomplete request evidence is not promoted to qualified I/O.

The exporter publishes `kv_case_owned_wal_*`; NVMe evidence panels 15–16 show
its availability, sample count, bytes and block issue-to-completion distribution.
These are **observed file-owned block requests**, not every API call, NAND
latency or a measured page-cache-miss fraction. The wider collector-window
guard against claiming a pure-cold API population remains in place. A local
FIEMAP syscall and confinement/FIFO regressions exercise collection mechanics;
physical remote-SSD latency still requires the cluster.

