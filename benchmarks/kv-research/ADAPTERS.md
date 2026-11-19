# External KV adapters: research, semantics, and local preparation

These adapters are standalone application-level KV comparators; they do not replace Tidehunter's native engine or alter its WAL/recovery model. The benchmark provides blind full-value `put`, `get`, `delete`, ordered `get_many`, and optional `put_batch`. The common generator supplies opaque logical binary keys; FoundationDB adds the documented user-domain encoding below, while the other adapters retain the raw bytes. The Tidehunter paper's §3.1 separates append/WAL durability from later work, §3.2 describes index-assisted reads, and §6.2 compares engine-specific operations and storage behavior. External APIs have different server-side paths and acknowledgement meanings; retain `BackendInfo` semantic/trust metadata rather than claim equivalent durability or transport. See [Tidehunter paper §§3, 6](../../../papers/parsed/Tidehunter-2026-v2.md) and the [one-sided WAL evidence guide](../../docs/distributed/ONE_SIDED_WAL_IMPLEMENTATION_2026-10-23.md).

For a normal run with a complete preload and no configured workload deletes, ordinary `get` and every `get_many` element must be present and match its deterministic full value; an absent result is a `wrong_value` outcome. Without preload, or when the workload can delete keys, absence remains valid. Explicit `missing_read` always expects absence. Ordered `get_many` checks preserve both result count and the value for each corresponding input key.

Closed-loop dispatch now sends work only to an idle worker and returns a worker to the available set when its completion is consumed. Earlier runner artifacts used round-robin private queues, which could place a request behind a busy worker while another worker was idle; closed-loop throughput and latency from that runner can therefore be confounded by head-of-line scheduling, especially for asymmetric request times. Open-loop arrival timing and its bounded per-worker queues are unchanged.

The runner writes `measurement-read-evidence.json` for the measurement boundary.
After warmup/aging and the measurement gate, it takes a quiescent start
observation; the end observation follows measured-request drain and precedes
postqualification and the completion gate. Failure to attempt or drain is
explicit. Tidehunter owner observations are fresh authenticated snapshots,
sampled sequentially across owners, not a globally atomic snapshot. Monotone
counter deltas are separate from nonmonotone gauges; unavailable or reset
counters are explicit, never imputed as zero. Logical WAL reads do not prove
physical NVMe reads. See the [comparison evidence notes](../../experiments/kv-comparison/README.md#measurement-read-evidence).




Security is a workload dimension, not an equivalence claim: the existing native TCP adapter uses mTLS, TiKV RawKV and Valkey TCP use plaintext, CQL reports either plaintext or CA-verified TLS, and FoundationDB reports negotiated transport/trust as unverified from its live status. Runs with different trust modes must remain explicitly labeled and are not security-matched comparisons. The adapters' `validate_options` entrypoints are pure JSON parsing/semantic validation, share their deny-unknown-fields parser with `open`, and do not inspect live endpoints or generated files. In particular, FDB preflight validates only that `cluster_file` is a nonempty UTF-8 path; its presence is checked only when opening the backend after lifecycle preparation.

## Workload key selection

`workload.mix` weights the six operations (`read`, `put`, `delete`,
`missing_read`, `get_many`, `put_batch`). `workload.distribution` selects
`"uniform"` or `{"zipf":{"exponent":1.2}}` within the prepared key range.
`workload.batch_size` sets the number of members in each grouped operation.

`workload.put_key_policy` accepts `"overwrite"` (the default when absent) or
`"new"`. Unknown values fail config admission. For example, a put-only workload is:

```json
{
  "distribution": "uniform",
  "mix": {"read": 0, "put": 1, "delete": 0, "missing_read": 0, "get_many": 0, "put_batch": 1},
  "batch_size": 32,
  "put_key_policy": "new"
}
```

`"overwrite"` keeps the existing seeded random key sequence. `"new"` assigns
each scalar put and each put-batch member a unique logical id `dataset.keys + n`.
One shared atomic counter starts at zero per run and continues across warm-up,
aging and measurement; all worker/backend requests use these ids through the
common `WorkItem` path. Overflow fails the run rather than wrapping. The physical
key still hashes the namespace, seed and logical id. Preload remains `0..dataset.keys`.
Rejected open-loop calls can leave gaps among stored ids; no id is reused.

`"new"` requires a nonzero `put` or `put_batch` weight and zero `read`,
`get_many`, `missing_read` and `delete` weights. It cannot be combined with a
measurement key sweep or `execution.mode="interference"`. The interference mode
keeps its own disjoint read/put id policy. Distribution does not select new put ids.
The manifest echoes the effective policy in `run.config.workload.put_key_policy`
and `run.data_policy.put_key_policy`, including the default `"overwrite"`.


## Selected Rust clients and dependency rationale

All clients are optional Cargo dependencies behind individual `tikv`, `cassandra`, `foundationdb`, `valkey-tcp`, `garnet`, and `kvrocks` features. A native-only invocation does not compile/link them. The manifest pins each database client to the reviewed release to limit API drift; the workspace lockfile records its dependency closure. Do not enable `--all-features` as a generic check: FoundationDB additionally needs a user-owned native client library.

| Adapter | Client and compatibility | MSRV / license | Why this client; boundary |
|---|---|---|---|
| TiKV RawKV | `tikv-client = 0.4.0`; RawClient docs target TiKV >=5.0 and specifically require PD endpoints. | Upstream toolchain is Rust 1.84.1; Apache-2.0. | Current upstream Rust client; `RawClient::batch_get` is a true native request, but omits missing keys and does not preserve order, so the adapter restores both. Rust implementation is used without application-written unsafe code. Reqwest (optional, rustls/json) is added only to query PD's observed HTTP configuration/store metadata; tonic 0.10 is an optional direct dependency used to recognize typed `DeadlineExceeded` status codes from the public TiKV error. |
| Cassandra / Scylla | `scylla = 1.9.0`, Scylla's current Rust CQL driver, explicitly documents Cassandra compatibility. `system.local.scylla_version`, when present, identifies Scylla; otherwise the adapter reports a Cassandra-compatible CQL server with vendor identity unverified. | Rust 1.88; MIT OR Apache-2.0. | The maintained async Rust driver provides prepared statements and CQL logged batches. CQL has no native ordered batch GET in this adapter; `get_many` is a bounded fanout of prepared per-key reads and is labeled as such. TLS uses the driver's Rustls 0.23 integration with an explicit CA PEM file and ring crypto provider; credentials may only be provided by environment-variable names and require TLS. Typed client and server read/write timeout errors are normalized without string matching. The application adapter contains no unsafe code. |
| FoundationDB | `foundationdb = 0.11.0`, `foundationdb-sys` C API wrapper; `fdb-7_3` API binding. | Rust 1.85.1; MIT OR Apache-2.0. | The Rust binding is maintained, asynchronous, and makes native retryable transactions available. Reads, deletes, and writes use `Database::run`; `put_batch` is one serializable transaction. The safe wrapper is built over FFI; the adapter's one `unsafe { foundationdb::boot() }` call follows the wrapper's documented safety contract by retaining and dropping the network handle and rejecting a second open in the same process. |
| Valkey TCP | `redis = 1.8.0`, default features disabled, `cluster-async` and `tokio-comp`; pinned Valkey server 9.0.7. | Rust 1.88; BSD-3-Clause. | Maintained async cluster client, rather than a second RESP/slot-map implementation. Cloneable multiplexed handles avoid a new adapter-wide mutex. Native MGET routing splits by slot and restores input indices, nils and duplicates. The reviewed client contains unsafe buffer-trait forwarding; this adapter adds no unsafe code. This is not a full transitive dependency audit. |
| Garnet / Apache Kvrocks | `redis = 1.8.0`, async cluster; Garnet 2.2.5, Kvrocks 2.17.0. | Rust 1.88; BSD-3-Clause client. | Distinct server identities and durability admission share the cluster client; native MGET uses the ordered cross-slot splitter, not an atomic batch. |

Manifest support dependencies are purpose-specific: `reqwest` for PD metadata over HTTP, `tonic` for typed TiKV gRPC status handling, `rustls` plus `rustls-pemfile` for explicit verified CQL TLS, and the RocksDB crate/features for the separate native-adapter owner. `foundationdb` disables default convenience recipes and uses embedded **headers only** (`embedded-fdb-include`) with `fdb-7_3`; no client shared library is embedded. Review each client's resolved dependency closure with its exact manifest/lockfile pin; do not accept unrelated version churn. Redis-rs 1.8.0 also declares a target-specific Tokio >=1.51 requirement, so its addition requires updating this workspace's older unified Tokio family.

## Adapter-specific contract and observation boundary

### Native Tidehunter

| Option | Default | Contract |
|---|---|---|
| `path` | required | One Db at this path with one instance; parent of `shard-00` through `shard-{N-1:02}` with multiple instances. |
| `key_count` | 1,048,576 | Positive dataset bound; the same bound is given to each Db. |
| `durability` | `page_cache` | `page_cache`, `value_wal_barrier` or `value_wal_group_barrier`; all instances use the same mode. |
| `native_wal_geometry` | `native_default` | `native_default`, `small`, `large_files` or `owner_matched`; the same geometry is used per instance. |
| `native_shards` | 1 | Integer 1 through 16; independent Db instances in one process, without extra caller threads; filesystem device numbers are checked at open; SSD backing needs controller device-admission evidence. |

Multiple native instances use FNV-1a over each key, not the distributed backend's
operation-ID placement. Match total caller concurrency explicitly; five instances
with 16 total callers are not five SSDs or a distributed placement replica.
Use the same filesystem (checked by device number); SSD backing needs controller device-admission evidence.
The receipt records device IDs at open and `same_filesystem` for multiple instances;
`backing_block_device` remains `not verified by the adapter`.
With `native_shards>1`, atomic `put_batch` and write profiling are unsupported.
Engine metrics stay private; group-barrier metrics carry a `shard` label. See
`NATIVE.md` for paths, receipts, routing and comparison limits.

### TiKV

- `tikv` options require `pd_endpoints` and `required_replication_factor`; PD client endpoints must be host:port or root plaintext `http://` endpoints without credentials, because RawClient TLS is not configured. Optional `pd_api_endpoints` must be absolute root http(s) URLs (or is derived from the validated PD endpoints) and `request_timeout_ms` defaults to 5000. Each option object denies unknown fields. The RawKV client receives PD endpoints, not TiKV store addresses.
- Before returning an adapter, the constructor reads PD `config/replicate` and `stores` HTTP metadata. It rejects a configured `max-replicas` that differs from the required value, and reports observed store versions. PD's `max-replicas` is a configured ceiling—not proof of every region's live peer count or placement-rule result. Actual region peer counts, store-side compaction settings, Titan/value-separation settings, and physical persistence configuration are not observed by this adapter and are explicitly marked unverified.
- `get_many` calls native `RawClient::batch_get`, indexes returned pairs by binary key, and maps against the original key slice. Missing keys become `None`; duplicate inputs still yield duplicate ordered outputs. Individual RawKV reads and writes are not transactional. `put_batch` is rejected because RawKV batch-put cannot establish the required atomic transaction guarantee; this adapter does not silently switch to TiKV's separate transactional API.
- RawKV client TLS is not configured; the adapter reports plaintext. Request success does not prove disk fsync. Typed tonic `DeadlineExceeded` is surfaced as `io::ErrorKind::TimedOut` with the original status retained as its source; ordinary errors remain unchanged. PD configuration and TiKV store versions are observed; effective write acknowledgement/disk behavior is server-defined and remains unverified.

### Cassandra / Scylla CQL

- Options require CQL `nodes`, existing `keyspace`, and `required_replication_factor`; `consistency` defaults to and only accepts `quorum` (the adapter requires `SimpleStrategy`). `get_many_concurrency` defaults to 16, and request timeout to 5000 ms. Optional credentials refer to environment variable **names** (`username_env`, `password_env`), never literal credential values. They require `tls_ca_file`; the driver validates the server certificate against that PEM CA, and there is no plaintext-auth fallback.
- The adapter reads `system.local`, `system_schema.keyspaces`, and the table's `system_schema.tables.compaction`. It requires existing `SimpleStrategy`, the requested replication factor, and `durable_writes=true`, and creates only `kv_research` if absent. It reports the observed RF and strategy, durable-writes setting, server product/version evidence, data centre, and compaction map. Token/rack placement is not established by that keyspace metadata and is marked unobserved. `commitlog_sync`/fsync are not queried: `durable_writes=true` alone is not proof of an fsync-before-ack policy. The adapter stores values as a regular CQL `blob` column; it does not claim a separate value log or infer a storage-engine value-separation setting from the blob type.
- `get_many` issues bounded independent prepared SELECT requests and restores source order; it is **not** a CQL multi-key read. The adapter identifies Scylla only from an observed `system.local.scylla_version` column; absence is reported as vendor-unverified CQL, not assumed Cassandra.
- `put_batch` is a CQL **logged batch**, with the selected consistency level and request timeout. It is reported as `CrashRecovery`, not serializable transaction: it does not promise isolation from concurrent readers/writers. Single writes are blind full-value INSERTs (no read/merge/update).
- Security metadata reflects actual client selection: TLS is configured with CA verification or explicitly `plaintext`; authentication is on/off without exposing secrets. A successful TLS-configured connection requires TLS negotiation, while no TLS context means plaintext. Typed client request timeouts and server `ReadTimeout`/`WriteTimeout` errors become `io::ErrorKind::TimedOut` with the original driver error as source.

### FoundationDB

- Options require an explicit `cluster_file` and `required_replication_factor`; `transaction_timeout_ms` defaults to 5000. There is no fallback to a default cluster file. The adapter sets a database transaction timeout to bound the C client retry behavior and also applies an API-side timeout; an interrupted mutation may have an unknown outcome, not rollback semantics.
- FoundationDB reserves keys starting with `0xff` for [internal system use](https://apple.github.io/foundationdb/developer-guide.html) and limits [physical keys to 10,000 bytes](https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values). Uniform random benchmark keys can hit that reserved domain. User operations prepend `min(first_byte_or_zero, 0xfe)` to the original key. This injective, order-preserving embedding keeps physical first bytes across `0x00..0xfe`, rather than concentrating the workload under one fixed prefix. The 32-byte logical keys become 33-byte physical keys; the maximum logical length is 9,999 bytes. All five user operations share the encoding; the explicit system-status query remains a system query. Metadata records the encoding and overhead. Keys/values are retained outside retry closures rather than copied on each transaction attempt.
- The adapter reads FoundationDB's machine-readable `\xff\xff/status/json` system key using `ReadSystemKeys`, requires `full_replication=true`, and verifies the requested RF from observed custom `storage_replicas` or the standard `redundancy_mode` (`single`/`double`/`triple`). It retains the status and configured storage/log replication policies; reported `storage_replicas` and standard mode are distinct evidence, not assumed from the launch option. Process versions and locality come from that same live status when present. The status schema is explicitly not a stable public API; unknown or missing required RF/full-replication fields fail closed.
- `get_many` performs sequential native key gets inside one serializable transaction and preserves exact order. `put_batch` sets all entries in a single retryable serializable transaction. `Database::run` may retry a transaction and warns that a result can be maybe-committed; the adapter performs no external side effect in its transaction closure. Typed FoundationDB `timed_out` (1004), `transaction_timed_out` (1031), and the adapter API's elapsed deadline are surfaced as `io::ErrorKind::TimedOut` with their original causes retained. Physical device persistence, negotiated transport security/authentication, storage-engine compaction, and internal value-separation implementation are not inferred from a successful commit or merely echoed cluster-file data; they remain unverified unless observed status proves them.
- Runtime client identity is read from the unique executable, non-deleted `libfdb_c.so*` mapping in `/proc/self/maps`, canonicalized and hashed as Blake2b-512; the loaded C API maximum version is queried from the FoundationDB API. These are emitted under `configuration.loaded_native_library` and are evidence from the running process, not the caller-supplied path or build-time link search.

### Garnet and Apache Kvrocks

`garnet` and `kvrocks` require their identically named Cargo features and exactly five distinct TCP endpoints, `cluster=true`, `required_replication_factor=1`, `required_primary_nodes=5`, and a positive `request_timeout_ms`. At open each endpoint must identify the pinned product/version, report a primary with no replicas and cluster mode, and present a healthy complete 16,384-slot cluster view. All five views must agree, and each endpoint must identify a distinct primary. The client routes binary keys with redis-rs cluster routing; GET, SET and DEL preserve complete byte strings. Ordered `get_many` uses native MGET through redis-rs's cross-slot splitter and ordered response combiner, preserving duplicates, empty values and missing entries. One backend timeout covers the complete MGET operation. Atomic `put_batch` is not supported. Timed-out or transport-failed mutations have unknown outcomes.

Each Garnet node must report `appendonly=yes`, `aof-commit-freq=0`, `aof-commit-wait=yes` and `aof-null-device=no` through CONFIG. The adapter records INFO store and LogDir, but **does not independently verify hybrid-log storage tiering**: Garnet's pinned CONFIG interface does not expose that toggle. A qualifying integrated run additionally requires a lifecycle receipt inspecting the live owned image, effective `--storage-tier`/`--logdir` arguments and storage mounts. Manual adapter admission alone is not full storage-tier qualification. Kvrocks requires `rocksdb.write_options.sync=yes`, `rocksdb.write_options.disable_wal=no`, and cluster mode; the mounted storage device is likewise a separate lifecycle fact. CONFIG snapshots identify cache and maintenance settings where available. Background maintenance remains enabled by lifecycle configuration. Neither OS page-cache eviction nor successful client reads prove cold-device I/O.

See the pinned [Garnet runtime CONFIG implementation](https://github.com/microsoft/garnet/blob/v2.2.5/libs/server/Config/RuntimeServerConfig.cs), [Garnet cluster documentation](https://microsoft.github.io/garnet/docs/cluster/overview), [Kvrocks 2.17.0 configuration](https://github.com/apache/kvrocks/blob/v2.17.0/kvrocks.conf) and [Kvrocks cluster documentation](https://kvrocks.apache.org/docs/cluster). INFO/configuration is sampled at admission, not per operation; configuration changes during a run are outside that snapshot.

### Valkey TCP

- `valkey_tcp` requires `nodes`, `cluster`, `required_replication_factor=1`, `required_primary_nodes`, and positive `request_timeout_ms`. Only unauthenticated host:port TCP endpoints are admitted. Standalone qualification uses one endpoint/primary; the distributed comparator requires exactly five distinct configured primaries and all 16,384 hash slots. Replica reads and replicas are not enabled.
- Admission queries every configured node's actual Valkey version, primary role, replica count, cluster mode, AOF configuration and persistence state. Cluster topology must have connected, non-migrating primaries, no failed/replica nodes, and unique complete slot ownership. The local/external lifecycle also compares every node's slot view and cluster links. These checks run outside measured operations.
- `get` uses GET; blind full-value `put` uses SET; `delete` uses DEL. `get_many` is one MGET through redis-rs's maintained cross-slot splitter and ordered response combiner, not 32 serial client GET calls. Full binary values, absent elements and duplicate input positions remain part of the common correctness check. Cross-shard atomic `put_batch` is unsupported and rejected during configuration admission; separate SETs are not relabeled an atomic batch.
- GET reads Valkey's RAM-resident database. AOF is persistence/recovery, **not a disk-miss GET tier**. The admitted policy is `appendonly=yes`, `appendfsync=always`, `no-appendfsync-on-rewrite=no`, and `maxmemory-policy=noeviction`; normal AOF rewrite/maintenance settings remain active and their actual state is retained. This RF1/fsync-before-reply, plaintext cohort is not equivalent to replicated transaction stores or Tidehunter's asynchronous acknowledgement modes.
- Cluster commands have a bounded two-retry client budget, separate from the outer API deadline. MOVED/ASK routing and transport retry are distinguished in metadata. Standalone multiplexed commands do not have that cluster retry policy. Timed-out or transport-failed mutations have an unknown outcome; disconnects are not falsely recorded as timeouts. No extra application-level command retry is added.
- Rust implementation: `src/backends/valkey_tcp.rs`; local/external lifecycle: `experiments/kv-comparison/{local,cloud_layout}.py`. The separate transport qualification is in [`experiments/valkey-infiniband`](../../experiments/valkey-infiniband/README.md).

### Valkey RDMA and matched libvalkey TCP control

`valkey_rdma` requires Cargo feature `valkey-rdma`, Linux, and the same five option names as the default TCP adapter. `nodes` must be distinct numeric IP:port RDMA listeners (bracket IPv6 addresses); timeout is 1–60000 ms. Standalone requires one node; Cluster requires five RF1 primaries. There is no TCP fallback. The transport uses the pinned upstream bundled libvalkey implementation, which requires RDMA-CM, RC QPs, verbs and WRITE_WITH_IMM; EFA/SRD is not a substitute. GCP IRDMA requires separate hardware qualification. Building or connecting does not attest the physical path.

For a matched-client TCP control, use backend `valkey_tcp`, add `"client":"libvalkey"` to its options, and enable both `valkey-tcp` and `valkey-rdma`. This explicitly selects ordinary TCP; the default TCP client remains redis-rs. Both matched controls use four persistent connections per node, per-connection serialization, Tokio's blocking pool, and identical static cluster routing. Cluster MGET groups by slot, appends groups to upstream libvalkey pipelines per primary, concurrently drains up to five primary pipelines, and restores input order, misses and duplicates. The shim uses upstream buffer and reply-reader APIs with a shrinking deadline, since `valkeyGetReply` renews the timeout for each read. A partially failed pipeline leaves its connection unusable rather than allowing unread responses to contaminate later commands; shutdown waits for outstanding blocking calls. This is not the redis-rs asynchronous progress model; retain the client and transport labels in comparisons.

Set `VALKEY_SOURCE_DIR` at Cargo build time to a clean checkout of Valkey 9.0.7 at `fe8d406c0b5a64ec897f6455e4de684629883764`. Install a C compiler, `ar`, and rdma-core development libraries (`librdmacm` and `libibverbs`). Build the server from the pinned source with `BUILD_RDMA=yes`. Separately, in a clean client checkout at the same commit, run `make -C "$VALKEY_SOURCE_DIR/deps/libvalkey" USE_RDMA=1 static`. The client checkout must contain `deps/libvalkey/lib/libvalkey.a` and `libvalkey_rdma.a`; the shim compiles against its `deps/libvalkey/include` headers. Do not reuse the server's libvalkey archive: the server build supplies its own SDS and dictionary implementations. The checkout commit and tracked-file cleanliness are checked at build; artifact provenance remains an operator assertion, not a cryptographic attestation. Native-only and default TCP builds need none of these libraries.

The matched libvalkey adapter also supports bounded, opt-in client call
diagnostics. Prepare a new pinned checkout with
`prepare_valkey_rdma.py --output PATH --cq-diagnostics` to compile the
measurement-only CQ hook into the standalone client; the server is built before
that hook is applied. The build checks the exact source transformation, rejects
other tracked changes and rejects a source/archive hook-symbol mismatch.
Keep the recorded source and archive SHA-256 values with results: the build
does not independently reproduce or attest the operator-built libraries.
Unmodified pinned client builds remain supported, with CQ data unavailable.

Selected calls record caller sample ID, operation, owner and connection,
mutex wait, blocking-pool enqueue-to-start and return-to-async-wake, command
encoding inside the pipeline (excluding initial argument-vector construction),
libvalkey transport plus parser, and materialization/decode. MGET
can produce one record per primary pipeline with the same caller ID; the
bounded limit counts records, not keys. Error and cancelled calls may have
partial fields; `pipeline_outcome` reports pipeline completion, not outer API
semantic validation. A snapshot explicitly marks unfinished workers. Zero is
a measured value, while null means absent. For hooked RDMA only, CQ records
count actual `ibv_poll_cq` invocations/completions, time those calls, measure
the `poll` wait on both CQ and CM descriptors, and measure userspace return
from a CQ-ready poll to the first CQ poll. The poll interval includes kernel
blocking and scheduling: OS wakeup-to-scheduling latency is unavailable
without kernel tracing. TCP has no RDMA CQ and reports it unavailable, not zero.
These nested intervals are not additive and do not isolate wire or server time.


The upstream Cluster client hardcodes TCP for node connections; it is not used for RDMA. Instead admission queries each explicit endpoint for `CLUSTER MYID`, verifies each node's `CLUSTER NODES` and `CLUSTER SLOTS` views agree, all 16,384 slots, healthy non-migrating distinct primaries, RF1 and durability settings. This maps node IDs to slots without assuming TCP-advertised and RDMA IPs or ports match. The fixed map is a static-topology cohort: MOVED/ASK and other server errors fail without retry, and topology changes that do not produce a redirect are not actively monitored. Cross-slot atomic batch writes are unsupported. Each transport error or timeout may leave a mutation's result unknown. GET remains RAM-resident; the AOF is a durability log, not an NVMe read tier.

## FoundationDB user-owned native client preparation

The `foundationdb` feature requires a compatible FoundationDB **7.3.x client library** at both link and runtime. This workspace must not install packages globally, modify `/usr/include`, copy libraries into system paths, bundle the library, or silently download it. Use the local lifecycle's owned 7.3.77 image/library preparation to copy `libfdb_c.so` into a user-controlled directory, and retain the image digest and copied library SHA/version as lifecycle evidence. The reviewed local image selected by the orchestration owner is `foundationdb/foundationdb:7.3.77`, digest `sha256:559b178a137bbe09272ad6e3752531aa04e2ddacab8ccfa4ffa86467db033bea`; this pin is for local preparation only, not proof of a server run.

For a user-owned prefix such as `$HOME/.local/opt/foundationdb-7.3`, place the library under `$HOME/.local/opt/foundationdb-7.3/lib`, set `FDB_CLIENT_LIB_PATH` to that directory while compiling the feature, and set `LD_LIBRARY_PATH` to the same directory only for the benchmark process. The crate feature fixes the C API header ABI at API 730 (`fdb-7_3`) and uses its own embedded matching headers; the actual `libfdb_c.so` must still report/build for the matching 7.3 ABI. The crate's build script also uses bindgen, so provide the required clang/libclang toolchain locally without installing it from this project. No global linker configuration is changed. Provide the explicit cluster file in adapter options. Missing header feature, library, incompatible ABI, file, or service is an error, never a skip or native fallback.

The pinned release/reference sources are [foundationdb-rs 0.11.0 prerequisites and API features](https://docs.rs/crate/foundationdb/0.11.0), [foundationdb-sys 0.11.0 linker build script](https://docs.rs/crate/foundationdb-sys/0.11.0/source/build.rs), [FoundationDB C API versions](https://docs.rs/foundationdb-sys/0.11.0/foundationdb_sys/constant.FDB_API_VERSION.html), and [FoundationDB machine-readable status](https://apple.github.io/foundationdb/mr-status.html). The C API build script supports `FDB_CLIENT_LIB_PATH`; no host-global install is required.

## Primary implementation references

- TiKV client-rust: [RawClient API](https://docs.rs/tikv-client/0.4.0/tikv_client/struct.RawClient.html), [0.4.0 source/release metadata](https://crates.io/crates/tikv-client/0.4.0), and [PD config HTTP API](https://docs.pingcap.com/tidb/stable/pd-control/). Raw `batch_get` explicitly omits non-existent keys and does not preserve input order; this implementation accounts for both. The existing go-ycsb integration is only a workload reference: its transactional/database semantics are not copied.
- Scylla Rust driver: [1.9.0 driver docs](https://docs.rs/scylla/1.9.0/scylla/), [prepared-statement consistency/timeout methods](https://docs.rs/scylla/1.9.0/scylla/statement/prepared/struct.PreparedStatement.html), [logged batch API](https://docs.rs/scylla/1.9.0/scylla/statement/batch/struct.Batch.html), and [1.9.0 source manifest/features](https://docs.rs/crate/scylla/1.9.0/source/Cargo.toml.orig). Cassandra's [storage-engine documentation](https://cassandra.apache.org/doc/5.0/cassandra/architecture/storage-engine.html) distinguishes commitlog acknowledgement from configured sync behavior and explains replay after restart; this is why `durable_writes` is not reported as verified disk fsync.
- FoundationDB: [Rust `Database::run` and timeout/retry semantics](https://docs.rs/foundationdb/0.11.0/foundationdb/struct.Database.html), [transaction methods](https://docs.rs/foundationdb/0.11.0/foundationdb/struct.Transaction.html), [status JSON field contract](https://apple.github.io/foundationdb/mr-status.html), and [architecture](https://apple.github.io/foundationdb/architecture.html). The adapter uses retryable native transactions, not go-ycsb's read/merge/write `Update` path.
- Valkey: [9.0.7 security release](https://github.com/valkey-io/valkey/releases/tag/9.0.7), [AOF/fsync policy](https://valkey.io/topics/persistence/), and [cluster specification](https://valkey.io/topics/cluster-spec/). Client: [redis-rs 1.8.0 release](https://github.com/redis-rs/redis-rs/releases/tag/redis-1.8.0), [crate/MSRV/license metadata](https://crates.io/api/v1/crates/redis/1.8.0), [MGET routing/response policies](https://docs.rs/redis/1.8.0/src/redis/cluster_handling/routing.rs.html), and [async request/retry implementation](https://docs.rs/redis/1.8.0/src/redis/cluster_handling/async_connection/request.rs.html).

## Evidence boundary

Build the optional OFI benchmark path with the Cargo feature `fabric` (engine
and example targets use `experimental-fabric`). Ordinary TCP/native builds do
not link libfabric. Feature selection does not install or qualify a provider;
runtime/provider admission remains explicit and fails rather than downgrading.

The constructors query live metadata. Actual-server qualification must retain the deployment receipt, executable/configuration identities and full-payload/ordered-group checks; compilation and mocked configuration tests are not live-server evidence. The separate five-host `cloud_layout.py` contract also verifies physical placement and effective acknowledgement controls. Local loopback qualification does not establish independent-NVMe or EFA performance. Cloud measurements require the source-bound release package and real hardware admission described in `experiments/kv-comparison/README.md`.

For `kv-research`, invoke a run with the ordinary `--config`/`--output`
interface; select backend kind `tidehunter` and provide `backend.options.transport`
as a structured engine `Transport` object. The example transport configuration
is:

```json
{"kind":"ofi","fabric":{"backend":"verbs","domain":"mlx5_0","source_address":"10.1.0.2"},"security":"trusted"}
```

Transport configuration for the Tidehunter example/benchmark harness uses
`--transport-config JSON_FILE`.

Historical `tidehunter_efa`, `tidehunter_efa_direct`, `tidehunter_efa_native`
and `tidehunter_efa_trusted` remain immutable case labels in existing result
artifacts only: the EFA and EFA-direct backends were removed and remain in the
`archive/multi-design` tag. The runtime backend kind is `tidehunter`, with
selection in `backend.options.transport`; do not introduce separate verbs
runtime kinds or reinterpret historical results.

## Tidehunter transport adapters and external owner preparation

The distributed adapter runtime uses one backend kind, `tidehunter`, one
engine `Transport`, and the shared `tidehunter/examples/support/tcp_cluster.rs`
harness. It preserves the same keyshape, owner protocol, WAL acknowledgement
and RF1 semantics across transports. Historical case identifiers remain
reporting labels, not transport/configuration aliases:

| Case ID (existing result labels stay historical; other IDs are explicit selections) | Structured engine selection | Security/data path |
|---|---|---|
| `tidehunter_tcp` | `{"kind":"tcp"}` | Existing TCP with mutual TLS; default and no native library linkage. |
| `tidehunter_verbs` / `tidehunter_verbs_native` | `{"kind":"ofi","fabric":{"backend":"verbs"},"security":"mutual-tls"}` / same with `trusted` | Generic verbs path; distinct trust labels. |
| `tidehunter_software_tcp` / `tidehunter_software_tcp_native` | `{"kind":"ofi","fabric":{"backend":"software-tcp"},"security":"mutual-tls"}` / same with `trusted` | Software OFI provider; correctness control, never hardware proof. |

The canonical `Transport` configuration is an object, not a backend string.
For a host-bound selection, add that host's optional `domain` and
`source_address` inside `fabric`; selectors can differ by host, but provider
and security must agree between peers. Source-IP support is provider-specific:
verbs accepts IPv4 only because the pinned RxM layer
lists `FI_SOCKADDR`/`FI_SOCKADDR_IN` (regardless of core verbs IPv6 support);
software-tcp accepts IPv4/IPv6 when the returned OFI address supports it.
No source selector means provider-default selection. Metadata reports the
selected parseable source; an explicit selector must match it.

```json
{"kind":"ofi","fabric":{"backend":"verbs","domain":"mlx5_0","source_address":"10.1.0.2"},"security":"trusted"}
```

The native feature is opt-in; configuration-only builds do not link libfabric.
An explicit OFI selection fails when the feature/provider/device is unavailable
and never falls back to TCP. Transport does not fork the WAL/index protocol or
change locks, RF1, durable receipts, or acknowledgement semantics. The engine
transport is `tidehunter/src/distributed/tcp/fabric.rs`; provider configuration
and endpoint setup are in `tidehunter-fabric/src/config.rs` and
`tidehunter-fabric/src/endpoint.rs`; adapter construction is in
`benchmarks/kv-research/src/backends/tidehunter.rs`.
Provider setup reports actual metadata; requested configuration alone is not
provider evidence, and a successful OFI open or provider label does not
establish a hardware data path. Software OFI correctness does not qualify
hardware or performance. Exercised local checks and explicit hardware limits
are recorded in the [27 October fabric qualification](../../tidehunter-fabric/DESIGN.md#local-qualification-27-october-2026).

Distributed adapters require `external_remote_config` with `live_nvme`, except
on Linux builds with `test-utils`: `tidehunter_tcp_trusted` can omit it to
launch supervised loopback OwnerHosts on ordinary files. Reopening reuses
the saved fixture endpoints and rejects changed fixture settings. This route
does not qualify kernel NVMe-oF or a fleet run.
The owner launch describes an `OwnerHost` and has no database ID, shape or WAL
geometry. The coordinator supplies those on first open. Subsequent opens of
the same authority recover its provisioned identity; dropping it removes
owner state before the path can provision a new identity.

The coordinator's `external_remote_config` serializes its engine `Transport`
under its `tls` settings; each owner's launch serializes its
own selected `Transport`. Prep input `owner_transports` supplies the per-owner
transport vector and its selectors for generated launch/config metadata; it is
not a field added to `RemoteDatabaseConfig`. Client and owner provider/security
must agree, while local selectors may differ. The adapter rejects a selected
OFI transport if native fabric support is unavailable.
The coordinator cache budget is configured by
`coordinator_read_cache_bytes`; when absent, the external adapter preserves the
budget in `external_remote_config.read_cache_bytes` (engine default 8 MiB).
Effective JSON retains transport, security, coordinator cache settings, owner
storage/read configuration, and placement evidence. External topology is not
asserted to use independent NVMe unless deployment evidence establishes it.

With feature `fabric`, effective JSON includes `fabric_provider_evidence` for
the existing owner-monitor connections. Provider identity, domain and fabric
are selected values; the open profile includes selected TX/RX capacities,
MTU and provider version when observed. Null means unavailable, including
unspecified zero queue hints and reused connections without an open profile.
These are OFI capacities, not hardware WQE or CQ occupancy. They cover monitor
connections only, not every lane. Sampled read and write profile counter maps
include the application-slot CQ-poll samples described in the
[fabric design](../../tidehunter-fabric/DESIGN.md); absent fields mean no timed
fabric CQ samples, not zero pressure. The existing Sui adapter Prometheus
collector exports these same visitor fields; the KV benchmark's `metrics.prom`
does not itself export this profile map.

Use the built binary's pure prep mode to generate engine-native external configs instead of reconstructing their YAML in deployment code:

```sh
kv-research --prepare-external-case input.json --output FRESH_DIR
```

`input.json` specifies an absolute `guest_root`, one, two, four, five, or sixteen
unique `addresses` as `IPv4:port`/`[IPv6]:port`, a structured engine `transport`,
optional `owner_transports`, and the required `live_nvmeof` settings below.
Set positive `max_batches_per_partition`, `max_admitted_batches` and
`max_index_bytes`, a nonnegative `coordinator_cache_bytes`, an
`owner_wal_member_read` policy (`async`, `mapped` or `positional`), and
`polling` (`adaptive` or `busy`). The default synthetic keyshape has 16 mutexes
× 64 cells. The helper writes `node-*/launch.yaml`, `remote.yaml`, `remote.json`,
`shape.yaml`, and generated TLS material when TLS is selected. Each launch
sets `host.wal_prewrite: false`; the cloud and local harnesses can set it from
`owner_wal_prewrite` before starting the owner. A pre-existing output directory
is rejected. The signing key stays in the generated
coordinator-side TLS tree; stage each node's launch config and required CA
certificate/server leaf certificate+key to that owner, never the CA signing
key. The coordinator uses `remote.json` credentials.

At shutdown, `read-evidence.json` records coordinator cache hits/misses/bytes and read RPC count separately from the most recent available owner read-executor counters. Owner `wal_reads` are logical member reads, not device IOPS; async submissions are not proof of physical NVMe activity. Evidence includes sample generation/age/availability and the effective read configuration. It adds no per-operation sampling calls; a passive bounded owner monitor samples outside the measured API calls.

### Linux NVMe-oF read selection

Linux builds with the `nvmeof` feature accept `nvmeof_readers` in an external
Tidehunter adapter configuration. Each reader binds the verified namespace to
its sealed export manifest; TCP and RDMA are distinct transport selections.
The [local management commands](../../experiments/nvmeof/README.md) describe
target setup, initiator connection and non-mutating preflight. RDMA requires a
compatible RDMA-CM path and is never replaced with TCP or EFA/SRD.
The target uses a verified buffered file and the initiator uses `O_DIRECT`;
neither establishes a page-cache hit or NAND access.

Set `coordinator_read_cache_bytes` to zero for remote-path controls or to an
adequate positive budget for coordinator-hit controls. The receipt names
the configured budget; measurement counters, not that setting, establish hits.
Target and reader transport evidence must match the selected transport.

To write a local plan without launching or configuring resources:

```sh
python3 experiments/kv-comparison/cloud_campaign.py plan \
  --spec experiments/kv-comparison/cloud-nvmeof-campaign.json \
  --mode nvmeof-focused \
  --backends native,tidehunter_tcp,tidehunter_nvme_tcp,tidehunter_nvme_rdma \
  --output /tmp/nvmeof-plan.json
```

The plan is not hardware qualification. AWS CI rejects NVMe/RDMA before
allocation; a compatible provider and verified path remain required elsewhere.

### Live NVMe-oF reads and writes

`standard-with-live-nvmeof` selects the final-owner-inode data path, including
scalar writes/deletes, atomic batches and mixed workloads. Historical sealed
controls carry the explicit `sealed_read` route and remain separate populations.
See [live WAL architecture and source map](../../experiments/nvmeof/LIVE_WAL.md).

External preparation accepts one, two, four, five, or sixteen owners. In
`live_nvmeof`, `port_id` and `service` are the public pool bases for owner 0.
`max_databases` is optional (default 1, valid range 1–16). With `M` slots per
owner, owner `p` gets `M` public slots starting at `base + 2*p*M`, followed
by `M` private commissioning slots. These ranges apply to both port IDs and
services. Launch files contain `host.live_target_pool`, not a per-database
target. The coordinator provisions database and effective target identities.
Selected RDMA requires the actual Linux NVMe/RDMA initiator; there is no TCP payload fallback.
Private loopback TCP commissioning is a target-control prerequisite, not the
selected payload route.

Run `kv-research --experimental-live-nvmeof-qualification INPUT.json --output FRESH_DIR`
before admitting a physical campaign. `expected_route=local_file` is explicitly
component-only. TCP/RDMA input requires already running owners and verified
Linux initiator namespaces. The optional `target_implementation` is `kernel`
by default or explicitly `spdk`: the latter requires the SPDK owner daemon and
verified actual SPDK binding, not merely the requested value. The native run
verifies live writes, mixed/delete batches, ordered and missing reads,
checkpoints/iteration, cache miss/hit, graceful reopen and separate abrupt-exit
tail recovery. Its final receipts require actual namespace-drain and
target-specific shutdown evidence. TCP qualification does not establish
physical RDMA performance or power-loss durability.

Qualification input requires `durability_tier` (`native` or `synchronous`).
The guest gate runs each selected target in both tiers, with separate receipts
and fresh owner launches. Admission checks the proof against the independently
collected owner process identity and effective durability receipt; a requested
tier alone is not evidence. `local_file` records its tier but does not qualify
an owner process or physical transport.

Live Prometheus collection is registered for live mode even without a sealed
reader. `tidehunter_nvmeof_live_*` and the typed quiescent boundary artifact
retain their own stage/sample populations. Submission-to-CQ is a composite
kernel/fabric/target/storage interval, not pure network time. A zero timing
sample count is unavailable. Owner completion and persistence stages are
observed only when the corresponding profiled completion reply is received.


## Read oracle, sample admission and worker-release clock

**Read oracle.** The generator derives each value from its logical key, so every put of a key
writes the same bytes and presence is the only mutable fact. With a baseline dataset
(`preload` or `verify_existing_dataset`) a read of a baseline key must return the exact
bytes unless a mutation explains absence. Without a delete, put or put_batch in the mix any
missing value is `wrong_value`. With mutations, each key owns two atomic words (no lock, no
shared state between keys): a delete sets `maybe_absent` when it starts; only an acknowledged
put whose whole interval overlapped no delete clears it; a delete with an unknown outcome
(error or timeout) sets a sticky `uncertain` bit that blocks clearing. Deletes and puts each
publish an in-flight count and a started-operation epoch. A read may return absence only if
`maybe_absent` was set before or after the call, or a delete or a put of that key was in flight
at either end of the call or started during it. A put that finished before the read began
explains nothing. Present values are always compared byte for byte. Without a baseline dataset
every key may be absent, as before. This is a concurrency-aware presence check, not a
linearizability proof; it costs one `SeqCst` load pair per read key and is outside the API
clock. Memory is 16 bytes per logical key, allocated only when the mix contains a mutation. A key
repeated inside one batch counts once per operation, so a key's in-flight count never exceeds
`execution.concurrency`; the run is refused if that cannot fit the 20-bit field.
The manifest records `run.read_oracle.mode` (`disabled_no_baseline_dataset`,
`strict_present_after_baseline`, `per_key_overlap_aware`).

**Sample admission.** The raw sample queue is a bounded `tokio::sync::mpsc` channel of
4096 messages. A full queue suspends only the scheduling task (`send().await`); no
runtime thread blocks and no sample is dropped. A stopped writer fails the run. The manifest
records `sample_writer_admission` with `stall_total_ns`, `stall_events` and `submit_errors`;
the older `sample_writer_backpressure_ns/_events` keys carry the same stall totals. The
writer thread flushes on a `Flush` message sent with each periodic snapshot and at least
once per second while rows flow.

**Worker-release clock.** `completed_ns` stays the API response boundary. The
`released_ns` column (last column of `samples.csv`; older files lack it) is taken after oracle
verification on the same origin clock, so the closed-loop cycle time of a worker is
`released_ns - dispatched_ns`. `summary.json` distributions gain a per-operation `oracle`
metric (`released_ns - completed_ns`, dispatched samples only) and the manifest
`worker_release` aggregate records sample count, sum and maximum oracle time.

**Valkey client pool repair.** A libvalkey connection is poisoned from the first appended
command byte until all replies are consumed. A request whose deadline expired before any
byte was written fails with a timeout and leaves the connection usable. A poisoned
connection, or one in sticky context error, is replaced with a fresh connection before its
next request (at most 2 attempts per request, each bounded by the connection timeout;
nothing is attempted after the request deadline). Counters `connections_poisoned`,
`connections_replaced`, `replacement_attempts_failed` and `timeouts_before_send` appear in
the measurement read evidence (`client_pool` delta) and in `manifest.run.client_pool_final`.
The redis-rs based `valkey_tcp` adapter has no pool or poison state: it clones one
multiplexed handle and a timed-out request only drops its own waiter.

**Transport receipts.** The Tidehunter adapter deserializes the engine `Transport` directly.
`transport.kind` is `tcp` (mutual TLS), `tcp-trusted` (no TLS) or `ofi`; an `ofi`
selector accepts `fabric.slots` (omitted means the engine default). The effective
configuration receipt carries `transport_config` (the full
serialized selector) and `transport_security`, so every transport knob of a run is recorded.

**Durability tier and comparison rule.** Distributed Tidehunter has two acknowledgement tiers,
selected by `backend.options.durability_tier` (default `native`; any other value is rejected
at config validation):

| Tier | Acknowledgement | Matched cohort (same run set only) |
|---|---|---|
| `native` | WAL page-cache write and publication; fsync in the background; explicit flush for stronger durability | native `durability=page_cache` only |
| `synchronous` | covering WAL barrier completed | native `durability=value_wal_barrier` or `durability=value_wal_group_barrier`, and Valkey `appendfsync always` |

Never mix tiers in one comparison: a `native` Tidehunter result is not compared with Valkey
`appendfsync always`, and a `synchronous` result is not compared with native `page_cache`.
The runner records the requested tier and its cohort in `summary.json` (`durability`) and
`manifest.json` (`durability` and `run.durability`, plus `backend_info.configuration`). The run fails
at startup unless the adapter's receipt (`configuration.durability_tier`) confirms the requested
tier, so a tier the engine did not apply cannot be reported. The tier is constant per run and is
therefore a file-level field, not a `samples.csv` column. Native and Valkey runs report their
fixed contract (`page_cache`, `value_wal_barrier`, `value_wal_group_barrier`, `appendfsync_always`) through the same
`durability` object.

Native `value_wal_group_barrier` removes the unshared per-put barrier from the concurrent
durable comparison. A shared run must start after the caller enters the post-mutation
barrier. It syncs the same registered Value-WAL files and directory. A failed sync fails
all waiters and later calls; there is no retry. This is an adapter change, not an engine
change. See `NATIVE.md` for group-size metrics and the limits of this comparison.

For external topology, the OwnerHost launch carries the tier and a fresh `launch_nonce`.
The owner publishes `<ready_file>.durability.json` before readiness. It names the
partition, bound address, process (`pid`, `boot_id`, `started_unix_ns`), launch
nonce and durability policy. It does not name a database: the coordinator
provisions the database and verifies its tier on open.
`external_owner_durability_receipts` must provide one startup receipt per
partition with the matching endpoint and tier. Replace the receipt whenever an
owner process restarts. A missing list, unreadable file or mismatch fails open.
The adapter records the tier and comparison cohort in `configuration.durability`.
The ordinary-file route instead keeps its fixture startup and shutdown records
under the case evidence directory; those records are not kernel qualification.

## Measurement gate for external cold-state capture

An optional `RunConfig.measurement_gate` pauses after preload, warm-up and aging have drained, immediately before the `measurement` phase begins. Its shape is:

```json
{"ready_file":"/absolute/existing-dir/ready","release_file":"/absolute/existing-dir/release","timeout_seconds":60}
```

Both marker paths must be distinct, absolute and absent when the run starts; their parent directories must already exist. The runner atomically creates the ready marker with create-new semantics, then waits at most `timeout_seconds` (maximum 120) and never beyond the run's global deadline. A controller may collect device counters and perform explicitly authorized cold-state preparation on owned hosts while the workload is paused, then create the release marker. Existing application-cache configuration and actual cache/RDMA evidence remain recorded separately; the gate does not claim that page-cache dropping removes all memory-resident data.

An optional `RunConfig.measurement_completion_gate` uses the same shape and marker rules. It pauses after measured operations and their normal result/oracle handling, but before post-workload qualification, backend drain or shutdown. The process and client cgroup remain alive while the controller captures end-of-measurement accounting. Timeout or marker errors fail the run and preserve its partial artifacts; cleanup continues after the gate returns.

## Measurement-phase write diagnostics

`RunConfig.diagnostics.write_profile_limit` defaults to `0`, which leaves the ordinary backend write paths unprofiled and emits no `write-diagnostics.json`. Values from `1` through `65_536` request diagnostic evidence; selection depends on the backend. The runner arms profiling only after warm-up/aging and the optional pre-measurement gate; it finalizes the profile only after the completion gate, before post-workload qualification. With `measurement_completion_gate`, this ensures the live client process remains available for end accounting before a remote profile finalizer performs its drain/control work.

Native scalar `put`/`delete` samples time the existing `WalWriter::write` append and `LargeTable` index application separately through benchmark-only entry points; those entry points preserve the native write algorithm and add clocks only on selected operations. A configured Value-WAL barrier has a separate existing file/namespace persistence profile; page-cache mode reports that barrier as not applicable. Native batch writes are explicitly excluded from native append/index stage sampling.

TCP/EFA adapters arm the existing append-owner connection profiler dynamically at the measurement boundary. Their cap is per append-owning connection; the artifact reports expected/captured connections, sampled requests, transport/client/server scopes and owner append/persistence scopes. A zero sample count or incomplete profile remains visibly unavailable. The legacy receive-frame clock is not collected under multiplexing; zero is not a measured receive cost. Client, transport and owner timers can overlap and are not additive shares of API latency. These clocked samples are bounded diagnostic evidence, not per-operation percentiles, physical device I/O, or a representative latency distribution. The artifact records unsupported stages and phase/selection policy explicitly.

For `tidehunter_live_nvmeof`, the nonzero limit enables an existing fixed-space
counter window; it does not select a first-N prefix. The arm snapshot is taken
after warmup and the end is frozen before the completion gate, excluding the
gate and finalizer. `stages.live_nvmeof` contains checked cumulative scalar
deltas, with completed NVMe writes as `selected_operations`. One I/O frame
need not equal one key or API call. Reads, metadata, errors and each elapsed
clock's sample count remain separate populations. Missing or reset counters,
unfrozen ends, errors and unknown writes fail the diagnostic; zero writes are
unavailable for admission. These clocks overlap and include scheduling, kernel,
fabric and target work where applicable; they are not device or wire latency
or stage percentiles. See [live timing interpretation](../../experiments/nvmeof/LIVE_WAL.md#timing-interpretation).

Distributed evidence also includes an optional `coordinator_interval` of whitelisted monotone coordinator counters, computed from a fresh snapshot immediately after profile arm through a snapshot **after the completion gate**, before profile finalization drains or sends control requests. Its legacy `measurement_boundary_cumulative_delta` scope name does not imply an exact measured-request window: background/checkpoint completions during gate observation can enter this delta. It is separate from sampled owner-prefix clocks and subtracts values already charged at arm time. Checkpoint phase sums/counts are charged when a checkpoint completes; they can include work begun before arm and exclude checkpoints still in flight at the end. Root adoption, live owner installation, worker cleanup and index preparation remain separate phase populations. These cumulative stage-clock deltas are not wall time or per-request durations; overlapping stages must not be added as disjoint fractions. Missing or decreased counters are reported as unavailable rather than zero-filled.

The checkpoint phase subset additionally requires `coordinator_checkpoint_diagnostics_available=true`. A missing/false flag suppresses those Prometheus clocks, even if stale numeric fields exist; a zero phase count does not mean zero latency. Dynamic profile arming now completes older checkpoint work before enabling/resetting its bounded diagnostic ring, outside measurement. This does not add collection to unprofiled primary cases.

Connection/transport counters and owner append samples are not interchangeable
populations. The local functional run recorded 1280 sampled append requests
(256 per five owner connections) but 1299 client requests. Use each component's
own count, not the owner sample count, when interpreting its accumulated clocks.
The 4096 logical measured writes are a third count; append grouping can coalesce
multiple logical batches into one RPC. None of these totals is a per-operation
stage latency percentile.

Cloud Markdown reports now show write-stage sums and their own denominators, including mixed append/checkpoint-control exchanges, owner file syncs and separately scoped coordinator pressure/checkpoint phases. Missing observations are not zero-filled. The adapter omits legacy unattributable receive/send/header/body clock fields from its exported stage samples and lists them as unsupported; the peer wire format is unchanged. The unconditional `cq_poll_gap_max_ns` includes idle periods, whereas `cq_max_poll_gap_ns` covers pending local TX only; neither measures outstanding-RPC response delay or physical wire RTT. The current first-N policy has no join to individual slow API rows. Diagnostic arming drains an older checkpoint before measurement, so these runs are not isolated measurements of timer overhead versus unprofiled cases.


## Request-linked write timelines

`diagnostics.write_timeline_limit` defaults to zero; `1..=65536` selects the first
admitted measurement-phase put, delete, or put_batch calls for a distributed
Tidehunter backend. It is independent of the aggregate write profiler and cannot
be combined with `write_profile_limit`: that profiler's cap changes admission.
Warmup and qualification are not traced. Selection does not stop or drain
measurement when the cap is reached. `samples.csv` includes an optional
`caller_sample_id`; an empty field, including in historical CSVs, means no ID.
The bounded `write-timeline.json` is exported after measurement and the optional
completion gate. It retains dropped/incomplete counters, errors and absent stages.
Finalization runs on a dedicated thread with the global deadline plus shutdown
grace passed to the engine. If it cannot finish in time, the run fails and the
timeline is marked unavailable; terminal benchmark artifacts are still published.
An outstanding finalizer retains its backend reference, so shutdown also reports
that ownership remains outstanding rather than claiming cleanup completed.

For a local distributed configuration with a fresh output directory, set
`"diagnostics":{"write_timeline_limit":4096,"write_profile_limit":0}` and run:

```sh
kv-research --config /path/to/local-distributed.json --output /path/to/fresh-result
python3 experiments/kv-comparison/analyze_write_timeline.py /path/to/fresh-result --output /path/to/timeline-analysis.json
```

The analysis joins on caller ID, summarizes available intervals and shows the
slowest calls with their raw events and errors. Coordinator timestamps share one
monotonic origin; stage intervals overlap and are not additive. `tx_queued`
marks local reactor queueing, not a NIC or kernel send; `completion_harvested`
marks worker observation, not the decode instant. Their interval includes remote
work and local processing and is not isolated wire latency. Server clocks, when
available, belong to another clock domain and cannot be subtracted from these
timestamps. Owner records join by owner and envelope ID and are summarized per
envelope, not per logical call. `session_admission_wait_ns` includes session
barrier and submit backpressure; `appender_queue_ns` runs from submit attempt to
control dequeue. `control_queue_ns` combines session, appender, flusher and
barrier waits; `wal_file_persist_ns` includes lookup and locking as well as
synchronization and is not a pure fsync syscall timer. An absent file-persistence
observation is not a zero-duration fsync. Instrumented runs are diagnostic, not
a replacement for profile-zero primary measurements; missing records or events
are unavailable, not zero.
The linked owner summary counts each `(owner, envelope_id)` once even when an
RPC covers multiple callers; the all-owner summary is a separate population.
Raw persistence capture frontiers and cumulative group counts are retained, but
their deltas are unavailable without independent proof that no untraced capture
on another connection intervened.
For live NVMe writes, `live_groups` joins sampled records by `BatchVersion` and
owner. It retains the reservation token when received, and the owner completion
RPC's validation duration and full persistence profile only when returned.
These are group observations, not per-batch costs: count each group once even
when it contains multiple sampled calls. `dropped_live_groups` counts groups
omitted at the group limit or the four-times-limit total membership budget;
missing completion stages or owner profile
remain absent on errors. Coordinator events separate group construction,
reserve RPC, NVMe submission through completion, completion RPC, and receipt
validation; namespace setup has separate events when a new queue is needed. They include local
scheduling and are not isolated network or media latency. The persistence
profile's call counts determine which of its duration fields were observed;
nested WAL and frontier durations must not be summed with their enclosing total.
On owner failure, the first recorded cause is retained in a bounded diagnostic
string and attached to later failed admissions and dropped write replies. An
uncertain reply still identifies that reply's own batch version, not the first
failed batch. This evidence does not identify the cause of earlier runs that
did not retain it.


Local and cloud campaign receipts bind this sidecar by its exact SHA-256, path
and compact coverage summary, without duplicating the event payload. A requested
completed capture requires matching case configuration, caller/raw joins and
complete owner coverage. Missing or tampered evidence fails admission; failed
partial captures remain partial. Generic result archives already retain JSON
sidecars. Existing matrix defaults/counts are unchanged: enable the limit in
both the selected job and its executed configuration, with the legacy write
profile limit zero. Protocol 19 requires rebuilding all clients and owners.

## Measurement-phase read diagnostics

`diagnostics.read_profile_limit` defaults to `0`: no read artifact or selected
stage clocks. Values `1..=65536` enable the finite distributed Tidehunter sampler.
Native and other stores reject a nonzero read limit before opening a database:
their complete API measurements are not a substitute for owner WAL timers.

For a distributed case, both profiles can be enabled:

```json
{"diagnostics":{"read_profile_limit":256,"write_profile_limit":256}}
```

Read profiling covers real scalar and grouped misses, including reads in mixed
workloads. It selects the first N remote flight admissions, not N logical keys
or N requests on every connection. Coordinator cache hits, local negative
lookups, and waiters reusing a flight do not create new sampled remote I/O.
The runner arms after warmup/aging and the release gate, snapshots counters
around the measured calls, then captures/disables profiling before subsequent
qualification. The optional completion gate remains outside the measurement.

`result/read-diagnostics.json` retains:

- Requested/armed status, selected/completed counts, scope, coverage and errors.
- `profile.stages`: each stage's own `available`, `sum_ns`, `count`, `max_ns`
  and `unavailable_reason`; absent maxima and unobserved durations are `null`.
- `profile.counters`: exact engine counts, including partial/deferred/error
  outcomes and failures whose owner profile was retained or unavailable.
- `profile.read_evidence`: separate measurement-boundary coordinator-cache,
  WAL, NOWAIT, async and NVMe-reader observations. These are not the sampled prefix.
- `profile.caller_correlation.trace`: a separate bounded, best-effort caller-ID
  sample. Calls retain logical-member offsets and owner-dispatch attempts, lane
  and reusable flight slots, request events, and, for live NVMe, SQE/CQE tokens.
  Pair SQE and CQE events by nonzero queue ID and token; slots can be reused.
  `members` and `coalesced_members` are logical API indices, not physical I/O
  counts. When `membership_classified` is true, `coalesced_members` identifies
  members joining pre-existing flights; duplicates within the same attempt can
  also share one physical reference. Do not count the remaining members as I/O.
  A coalesced follower does not inherit another call's device events. For live
  I/O, count observed completions using unique paired nonzero queue-ID/token
  events; sealed NVMe events describe logical requests, not individual SQEs. A
  conservative 4 MiB total charge, group/event caps, contention, cancellation
  and incomplete/error status are reported; missing observations are not zero.
  All timestamps are coordinator-local monotonic offsets from that call's start.
  SQE-to-CQE intervals include kernel and scheduling time, not pure wire or
  physical-media latency. Aggregate resolution and physical-flight populations
  remain independently selected and cannot be joined to these callers.

The correlation follows the existing `remote_db/pending_read.rs` flight,
`ReadRequest` and lane queues and `tcp/reactor.rs` `PendingRead` request map;
the underlying authority and validation paths are unchanged. Its owner-thread
locality is analogous to [eRPC's pinned event loop](https://github.com/erpc-io/eRPC/blob/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d/src/rpc_impl/rpc_ev_loop.cc)
and [request dispatch](https://github.com/erpc-io/eRPC/blob/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d/src/rpc_impl/rpc_req.cc);
this does not import eRPC's transport or hardware latency claims.

`profile.owners` maps actual partition IDs to their own server stages and
counters. It reuses the globally selected requests; it does not give each
owner a fresh sample budget. The monitor visits preallocated atomic collectors
sequentially, not as a globally atomic cut. After a drained phase, verify the
per-owner counts sum to their aggregate before attributing a slow shard.
Prometheus labels the aggregate as `partition="aggregate"` and owner rows with
their actual IDs. Do not pool these two views of the same work.

| Stage | Actual boundary and denominator |
|---|---|
| `server_planning` | Owner range/authority planning until plans are ready, including cold-planning wait; counted planned requests |
| `server_executor_residence` | First executor submission attempt through successful admission or synchronous completion; retains `Full` retry residence; counted admitted/completed requests |
| `server_nowait` | Actual bounded `preadv2(RWF_NOWAIT)` call elapsed time; counted syscall attempts, including fallback errors/partial results |
| `server_async` | Each member's SQE preparation through CQE observation; counted observed member completions, including retained failed replies |
| `server_response_ready` | Construction of owned response records; counted construction passes, not encoding or output |
| `server_handler` / `server_record_read` | Broader owner service scopes, excluding the final recheck; nested with the narrower WAL stages |
| `server_response_recheck_wait` / `server_response_recheck` | Ready-result wait and authority recheck; counted profiled replies |
| `actor_queue` / `actor_service` | Selected successful actor-origin requests; caller-owned admission origins are separate |
| `request_encode` / `exchange` / `response_decode` / `record_verify` | Actual message-path call counts; NVMe flights do not fabricate RPC encode/decode observations |
| `transport_output_queue` / `transport_output_submit` / `transport_output_retire` | Client connection progress during selected work, with each transition's frame count; may include interleaved work |
| `transport_cq_call` | Instrumented local provider CQ-call wall time and its timed-call count; not hardware transit |
| `inline` | Submission-to-completion elapsed time for scalar/grouped caller-owned admission origins; includes any later actor takeover, not exclusive caller CPU time |

For an available stage, `mean_ns = sum_ns / count`. A member-duration sum may
exceed the request's wall time because members run concurrently. Do not add
nested clocks, subtract unrelated percentiles, or subtract host timestamps.
Exact API/scheduling/response percentiles still come from every raw operation;
these stage aggregates do not supply stage p50/p99.

Async elapsed includes user-space submission preparation/retries, kernel work
and completion polling/scheduling. It does **not** prove an NVMe fetch or isolate
device service from the kernel page cache. NOWAIT unsupported/partial results
retain normal async fallback. RMA has no owner read handler for a direct hit;
sync diagnostic readers have no async-executor observation. Cache-only phases
remain valid, but owner WAL timing is unavailable instead of a fabricated zero.
Successful cancellations suppress replies as before; a disconnected request
cannot return an owner profile. Received profiled owner errors preserve their
timings without converting the error into success.

The normal unprofiled cohort is the performance baseline, not its diagnostic
twin. Pure wire and physical-media latency remain unsupported.

The cloud planner adds separately labelled representative read diagnostic twins,
preserving original cases and operation counts. Configuration, artifact hash
admission, `comparison.md` and the campaign Prometheus/Grafana exports retain
the new evidence. See [campaign settings](../../experiments/kv-comparison/README.md#read-stage-diagnostic-campaigns).


### Read controls

The current controls use coordinator cache hits or owner WAL reads. Owner WAL
reads may be served by the Linux page cache or storage; the counters do not
distinguish them.

Grouped point reads fan out before waiting, retire ready owners rather than
the first submitted owner, and may drive idle lanes in the caller. After 64
consecutive empty progress sweeps, they return ownership to the actor.
Accepted work and its buffer/byte credits stay in the shared driver through
handoff, cancellation and retirement. Detached scans remain actor-driven.
The Sui-compatible bulk API accepts borrowed `AsRef<[u8]>` keys; the benchmark
adapter no longer copies every key into a new `Vec<u8>`.


`phases.warmup_key_sweep=true` is an explicit, preloaded closed-loop read-only
control. It covers the unchanged logical working set during warm-up and
advances the usual random generator; measured requests still use the configured
random distribution. Validation rejects an incomplete sweep or mutating mix.
Existing cases default to false and retain their original warm-up.



### Coordinator, acquisition and output decomposition

The coordinator has an independent first-N API-resolution sampler. It includes
point/bulk cache hits, negative index results, remote misses and errors, unlike
the unchanged first-N *physical remote-flight* sampler. Its snapshot records
index-lock wait, member-only index lookup, cache-lock wait and cache lookup,
plus separate cut validation/capture, route, pin and batch-construction clocks.
Bulk samples also record admitted batch wait, post-read cache-fill/accounting,
and result materialization. These spans count only APIs that reach their end;
preparation outcome counters do not imply that a later remote read succeeded.
Older evidence without these three stages remains readable and does not emit
invented zero-duration observations for them.
Each has its own count, accumulated ns and maximum; bulk envelopes are not
pooled with member lookups. Logical member outcomes
and API resolution counts are distinct. `profile.coordinator_resolution_valid`
requires a correctly armed, attempted and drained interval; missing remote
flight evidence does not invalidate an otherwise valid cache-only resolution
sample. Nonzero rearm resets after drain; disarm preserves the snapshot.
The legacy combined resolve clock is not an index-only measurement.

Batch cache lookup holds the cache for at most 32 consecutive request slots.
Cache-lock-wait counts are actual acquisitions/windows; cache-lookup counts are
member lookups. Compare total lock wait per selected API as well as per
acquisition; do not divide both stages by the same member count or add overlapping
clocks into an end-to-end latency estimate.

Successful history adoption can retain immutable authority descriptors within
the existing metadata budget; persistence does not require immediate eviction.
Eviction releases cache-entry overhead independently of descriptor credits held
by readers. With `TIDEHUNTER_HISTORY_DIAGNOSTICS=1`, `adoption_ns` includes root
adoption and bounded post-adoption cache warming. It is not authority-lock hold
time and must not be added to overlapping request clocks.

`read_rpc_count` increments once for an admitted owner group that starts
message-read work. Coalesced consumers can share completed work, so logical
rounds do not imply a minimum dispatch count. The counter is not a packet
count and does not enumerate multiple internal messages for one group.
Use per-lane `message_read_requests` and RMA posts/completions for those
transport-operation populations.

Sampled owner grant setup now distinguishes lookup/reuse, reservation/admission,
envelope preparation, the actual `fi_mr_reg` call, and authority
recheck/publication. Each has its own count/sum/max/errors. Registration failure
is retained when observed; a provider call is not necessarily a fresh hardware
pin because providers can cache registrations. Client grant installation and
accepted FI_READ submission through locally observed completion are separate
stages. Neither timer is a pure network or disk-service measurement.

Owner response encoding, enqueue-to-first-write, local submission and local
retirement are sampled node-cumulative observations queried outside API timing.
They are not retroactively inserted into an already encoded reply and do not
require encoding the response twice. Abandoned output is counted separately
from retirement. Boundary counter deltas retain their populations; a cumulative
maximum is reported as a node-lifetime maximum, never subtracted.
Fatal connection failures can leave other in-flight profiles unavailable.
An NVMe flight has no owner record-read RPC handler; its reader stages and
errors are reported separately rather than filled with zero owner durations.
Historical grant/session records remain readable when present, but the current
producer no longer emits registered-value cache observations.

### First-access controls and cache policy

`phases.measurement_key_sweep=true` is opt-in. It requires a preloaded,
closed-loop, read-only workload with enough distinct keys for the complete
measured call count, including every full batch member. It does not wrap into
repeated keys. Default false preserves the ordinary random measurement sequence.
This deterministic first-touch sweep is labelled explicitly; it is not an IID
uniform-random population.

Focused campaign receipts name requested/effective coordinator cache budgets,
Linux page-cache policy, warm-up, sweep flags, concurrency and lane evidence.
Disabling the coordinator cache forces remote value service but does not disable
Linux page cache or metadata residency. Owner value caches and long-lived
registered-value grants are no longer runtime options. Transient transport
buffers remain. Pure-hit claims require observed paths, not a configured budget.

The opt-in `read-tier` campaign uses labelled remote, coordinator-cache,
warm-WAL, storage-observation and bounded-mixed controls. Baseline/profiled
twins are separate populations with the same input data. Backend-independent
`data_id` binds matching key/value bytes; `source_id` identifies each population.
Warm-up does not prove that every WAL read hit the Linux page cache.

Normal local Tidehunter owners and new standard cloud cohorts now request
`owner_read_resident_first=true`: a bounded NOWAIT read from the Linux page cache
can finish without an asynchronous value-read completion. Cold standard cohorts
request `false`; focused read-tier and sealed-storage controls retain their
explicit policies. Preparation and adapter options must agree with the actual
owner launch configurations. This does not add an owner application value cache.
Unsupported NOWAIT, missing pages, partial reads and exhausted sweep budgets
retain the bounded asynchronous fallback.

Adjacent or overlapping authorized ranges in the same pinned file/log can share
one asynchronous read, limited to 32 consecutive input slots and 64 KiB, without
sorting or reading gaps. Results retain original order and duplicate slots.
`async_read_submissions` and sampled owner async counts describe submitted I/O
operations; `completed_reads` and `wal_reads` describe logical members. A single
I/O can now cover multiple members. Neither population establishes physical
NVMe accesses, and their difference is not a cache-miss count.

The comparison runner already provides one dedicated OS worker per configured
concurrency slot. Native and Tidehunter adapters call their synchronous DB APIs
directly there. Do not insert another semaphore, `spawn_blocking` queue or
`block_in_place` layer merely because the outer trait uses `async`: it adds
overhead without freeing these already-dedicated workers.

### Cold-device evidence is not a per-API NAND timer

The controller can bracket a measured interval with an owned, bounded Linux
block collector. Where permitted, bpftrace/BTF tracks request identity internally,
including partial completions and requeues, without exporting kernel addresses.
Its issue-to-completion intervals are block/driver residence on the mapped
backing device, not application request latency or NAND service. Maps, perf
buffers, output and wall duration are bounded; loss, incomplete requests,
errors and unavailable permissions remain explicit.

Tracefs sector/length events are a counts-only fallback: boundary-straddling,
overlapping and split requests cannot establish request identity. Candidate
correlations are not promoted to proven device intervals. Device-wide activity
can include metadata, read-ahead and background work. OS-cache-drop receipts,
actual device activity and logical WAL reads support the cold *protocol* claim,
not that every API call missed every cache. Coordinator index temperature is
independent. Full API/scheduling/response distributions remain in `samples.csv`;
do not subtract cross-host timestamps, pool incompatible tiers, or turn nested
stage means into additive time shares.
