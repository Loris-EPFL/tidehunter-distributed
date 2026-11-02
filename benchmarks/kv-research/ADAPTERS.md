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
`--transport-config JSON_FILE`. The isolated raw FI_READ probe instead uses
`--fabric-config JSON_FILE` plus mandatory `--require-device-read true|false`;
these are separate interfaces and the probe is not a WAL/application benchmark.

Historical `tidehunter_tcp`, `tidehunter_efa`, `tidehunter_efa_direct`, and
`tidehunter_efa_native` remain immutable case labels in existing result
artifacts. The new runtime backend kind is `tidehunter`, with selection in
`backend.options.transport`; do not introduce separate EFA/verbs runtime kinds
or reinterpret historical results.

## Tidehunter transport adapters and external owner preparation

The distributed adapter runtime uses one backend kind, `tidehunter`, one
engine `Transport`, and the shared `tidehunter/examples/support/tcp_cluster.rs`
harness. It preserves the same keyshape, owner protocol, WAL acknowledgement
and RF1 semantics across transports. Historical case identifiers remain
reporting labels, not transport/configuration aliases:

| Case ID (existing result labels stay historical; other IDs are explicit selections) | Structured engine selection | Security/data path |
|---|---|---|
| `tidehunter_tcp` | `{"kind":"tcp"}` | Existing TCP with mutual TLS; default and no native library linkage. |
| `tidehunter_efa` | `{"kind":"ofi","fabric":{"backend":"efa"},"security":"mutual-tls"}` | EFA provider with mutual TLS. |
| `tidehunter_efa_direct` | `{"kind":"ofi","fabric":{"backend":"efa-direct"},"security":"mutual-tls"}` | EFA-direct with mutual TLS. |
| `tidehunter_efa_native` | `{"kind":"ofi","fabric":{"backend":"efa-direct"},"security":"trusted"}` | Trusted EFA-direct; separate unencrypted security cohort. |
| `tidehunter_efa_trusted` | `{"kind":"ofi","fabric":{"backend":"efa"},"security":"trusted"}` | Trusted ordinary EFA; separate unencrypted security cohort. |
| `tidehunter_verbs` / `tidehunter_verbs_native` | `{"kind":"ofi","fabric":{"backend":"verbs"},"security":"mutual-tls"}` / same with `trusted` | Generic verbs path; distinct trust labels. |
| `tidehunter_software_tcp` / `tidehunter_software_tcp_native` | `{"kind":"ofi","fabric":{"backend":"software-tcp"},"security":"mutual-tls"}` / same with `trusted` | Software OFI provider; correctness control, never hardware proof. |

The canonical `Transport` configuration is an object, not a backend string.
For a host-bound selection, add that host's optional `domain` and
`source_address` inside `fabric`; selectors can differ by host, but provider
and security must agree between peers. Source-IP support is provider-specific:
EFA/EFA-direct reject it; verbs accepts IPv4 only because the pinned RxM layer
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
provider evidence. `require_device_read: true` requires native FI_READ and
backend-specific evidence, not merely a successful OFI open or provider label.
For verbs, evidence includes provider PCI NIC/device metadata matched to
canonical sysfs PCI BDF and excludes software `rxe`, `rdma_rxe`, and `siw`;
this does not establish Falcon identity. EFA native mode requires an observed
non-emulated read capability. Software OFI correctness does not qualify
hardware or performance. Exercised local checks and explicit hardware limits
are recorded in the [27 October fabric qualification](../../tidehunter-fabric/DESIGN.md#local-qualification-27-october-2026).

Every distributed adapter accepts the existing `external_remote_config` object plus an `external_owner_read_config` array. The latter is required for external owners and must have exactly one entry for every configured partition:

```json
{
  "partition": 0,
  "read_workers": 2,
  "wal_member_read": "async",
  "read_resident_first": false,
  "read_memory_allowance_bytes": 536870912,
  "read_max_inflight_requests": 32,
  "read_max_inflight_bytes": 67108864,
  "read_max_device_reads": 128
}
```

The coordinator's `external_remote_config` serializes its engine `Transport`
under its `tls` settings; each owner's generated `NodeConfig` serializes its
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

Use the built binary's pure prep mode to generate engine-native external configs instead of reconstructing their YAML in deployment code:

```sh
kv-research --prepare-external-case input.json --output FRESH_DIR
```

`input.json` has the required fields: absolute `guest_root`; five unique
`addresses` as `IPv4:port`/`[IPv6]:port`; canonical `transport` as a structured
engine `Transport`; optional `owner_transports` for per-owner selectors;
positive `max_batches_per_partition`, `max_admitted_batches`, and
`max_index_bytes`; nonnegative `coordinator_cache_bytes`; positive
`owner_read_workers` and `owner_read_memory_allowance_bytes`;
`owner_wal_member_read` (`async`, `mapped`, or `positional`); optional
`owner_read_resident_first` enables Linux page-cache NOWAIT with async reads;
and `polling` (`adaptive` or `busy`). The helper fixes the keyshape at 16 mutexes
× 64 cells and writes `node-{0..4}/launch.yaml`, `remote.yaml`, `remote.json`,
`shape.yaml`, generated `tls/` material when TLS is selected, and
`external_owner_read_config.json` extracted from each actual launch file. Owner
limits remain as configured by the prep helper. A pre-existing output
directory is rejected. The signing key stays in the generated
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

## Measurement gate for external cold-state capture

An optional `RunConfig.measurement_gate` pauses after preload, warm-up and aging have drained, immediately before the `measurement` phase begins. Its shape is:

```json
{"ready_file":"/absolute/existing-dir/ready","release_file":"/absolute/existing-dir/release","timeout_seconds":60}
```

Both marker paths must be distinct, absolute and absent when the run starts; their parent directories must already exist. The runner atomically creates the ready marker with create-new semantics, then waits at most `timeout_seconds` (maximum 120) and never beyond the run's global deadline. A controller may collect device counters and perform explicitly authorized cold-state preparation on owned hosts while the workload is paused, then create the release marker. Existing application-cache configuration and actual cache/RDMA evidence remain recorded separately; the gate does not claim that page-cache dropping removes all memory-resident data.

An optional `RunConfig.measurement_completion_gate` uses the same shape and marker rules. It pauses after measured operations and their normal result/oracle handling, but before post-workload qualification, backend drain or shutdown. The process and client cgroup remain alive while the controller captures end-of-measurement accounting. Timeout or marker errors fail the run and preserve its partial artifacts; cleanup continues after the gate returns.

## Measurement-phase write diagnostics

`RunConfig.diagnostics.write_profile_limit` defaults to `0`, which leaves the ordinary backend write paths unprofiled and emits no `write-diagnostics.json`. Values from `1` through `65_536` request sampled evidence. The runner arms profiling only after warm-up/aging and the optional pre-measurement gate; it finalizes the profile only after the completion gate, before post-workload qualification. With `measurement_completion_gate`, this ensures the live client process remains available for end accounting before a remote profile finalizer performs its drain/control work.

Native scalar `put`/`delete` samples time the existing `WalWriter::write` append and `LargeTable` index application separately through benchmark-only entry points; those entry points preserve the native write algorithm and add clocks only on selected operations. A configured Value-WAL barrier has a separate existing file/namespace persistence profile; page-cache mode reports that barrier as not applicable. Native batch writes are explicitly excluded from native append/index stage sampling.

TCP/EFA adapters arm the existing append-owner connection profiler dynamically at the measurement boundary. Their cap is per append-owning connection; the artifact reports expected/captured connections, sampled requests, transport/client/server scopes and owner append/persistence scopes. A zero sample count or incomplete profile remains visibly unavailable. The legacy receive-frame clock is not collected under multiplexing; zero is not a measured receive cost. Client, transport and owner timers can overlap and are not additive shares of API latency. These clocked samples are bounded diagnostic evidence, not per-operation percentiles, physical device I/O, or a representative latency distribution. The artifact records unsupported stages and phase/selection policy explicitly.

Distributed evidence also includes an optional `coordinator_interval` of whitelisted monotone coordinator counters, computed from a fresh snapshot immediately after profile arm and a fresh snapshot after the last measured write reply, before profile finalization drains or sends control requests. It is separate from sampled owner-prefix clocks and excludes pre-measurement counters by subtraction. Its nanosecond stages are cumulative stage-clock deltas, not wall time; overlapping coordinator stages and checkpoint work must not be added as disjoint fractions. Missing or decreased counters are reported as unavailable rather than zero-filled.

Connection/transport counters and owner append samples are not interchangeable
populations. The local functional run recorded 1280 sampled append requests
(256 per five owner connections) but 1299 client requests. Use each component's
own count, not the owner sample count, when interpreting its accumulated clocks.
The 4096 logical measured writes are a third count; append grouping can coalesce
multiple logical batches into one RPC. None of these totals is a per-operation
stage latency percentile.

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
