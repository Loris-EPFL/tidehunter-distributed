# External KV adapters: research, semantics, and local preparation

These adapters are standalone application-level KV comparators; they do not replace Tidehunter's native engine or alter its WAL/recovery model. The benchmark provides blind full-value `put`, `get`, `delete`, ordered `get_many`, and optional `put_batch`. The common generator supplies opaque logical binary keys; FoundationDB adds the documented user-domain encoding below, while the other adapters retain the raw bytes. The Tidehunter paper's §3.1 separates append/WAL durability from later work, §3.2 describes index-assisted reads, and §6.2 compares engine-specific operations and storage behavior. External APIs have different server-side paths and acknowledgement meanings; retain `BackendInfo` semantic/trust metadata rather than claim equivalent durability or transport. See [Tidehunter paper §§3, 6](../../../papers/parsed/Tidehunter-2026-v2.md) and the [one-sided WAL evidence guide](../../docs/distributed/ONE_SIDED_WAL_IMPLEMENTATION_2026-10-23.md).

For a normal run with a complete preload and no configured workload deletes, ordinary `get` and every `get_many` element must be present and match its deterministic full value; an absent result is a `wrong_value` outcome. Without preload, or when the workload can delete keys, absence remains valid. Explicit `missing_read` always expects absence. Ordered `get_many` checks preserve both result count and the value for each corresponding input key.



Security is a workload dimension, not an equivalence claim: the existing native TCP adapter uses mTLS, TiKV RawKV currently uses plaintext, CQL reports either plaintext or CA-verified TLS, and FoundationDB reports negotiated transport/trust as unverified from its live status. Runs with different trust modes must remain explicitly labeled and are not security-matched comparisons. The three `validate_options` entrypoints are pure JSON parsing/semantic validation, share their deny-unknown-fields parser with `open`, and do not inspect live endpoints or generated files. In particular, FDB preflight validates only that `cluster_file` is a nonempty UTF-8 path; its presence is checked only when opening the backend after lifecycle preparation.

## Selected Rust clients and dependency rationale

All clients are optional Cargo dependencies behind individual `tikv`, `cassandra`, and `foundationdb` features. A native-only invocation does not compile/link them. The manifest pins each database client to the reviewed release to limit API drift; the integration owner resolves the workspace lockfile. Do not enable `--all-features` as a generic check: FoundationDB additionally needs a user-owned native client library.

| Adapter | Client and compatibility | MSRV / license | Why this client; boundary |
|---|---|---|---|
| TiKV RawKV | `tikv-client = 0.4.0`; RawClient docs target TiKV >=5.0 and specifically require PD endpoints. | Upstream toolchain is Rust 1.84.1; Apache-2.0. | Current upstream Rust client; `RawClient::batch_get` is a true native request, but omits missing keys and does not preserve order, so the adapter restores both. Rust implementation is used without application-written unsafe code. Reqwest (optional, rustls/json) is added only to query PD's observed HTTP configuration/store metadata; tonic 0.10 is an optional direct dependency used to recognize typed `DeadlineExceeded` status codes from the public TiKV error. |
| Cassandra / Scylla | `scylla = 1.9.0`, Scylla's current Rust CQL driver, explicitly documents Cassandra compatibility. `system.local.scylla_version`, when present, identifies Scylla; otherwise the adapter reports a Cassandra-compatible CQL server with vendor identity unverified. | Rust 1.88; MIT OR Apache-2.0. | The maintained async Rust driver provides prepared statements and CQL logged batches. CQL has no native ordered batch GET in this adapter; `get_many` is a bounded fanout of prepared per-key reads and is labeled as such. TLS uses the driver's Rustls 0.23 integration with an explicit CA PEM file and ring crypto provider; credentials may only be provided by environment-variable names and require TLS. Typed client and server read/write timeout errors are normalized without string matching. The application adapter contains no unsafe code. |
| FoundationDB | `foundationdb = 0.11.0`, `foundationdb-sys` C API wrapper; `fdb-7_3` API binding. | Rust 1.85.1; MIT OR Apache-2.0. | The Rust binding is maintained, asynchronous, and makes native retryable transactions available. Reads, deletes, and writes use `Database::run`; `put_batch` is one serializable transaction. The safe wrapper is built over FFI; the adapter's one `unsafe { foundationdb::boot() }` call follows the wrapper's documented safety contract by retaining and dropping the network handle and rejecting a second open in the same process. |

Manifest support dependencies are purpose-specific: `reqwest` for PD metadata over HTTP, `tonic` for typed TiKV gRPC status handling, `rustls` plus `rustls-pemfile` for explicit verified CQL TLS, and the RocksDB crate/features for the separate native-adapter owner. `foundationdb` disables default convenience recipes and uses embedded **headers only** (`embedded-fdb-include`) with `fdb-7_3`; no client shared library is embedded. The dependency lockfile is intentionally owned by the parent integration agent and must be reviewed/resolved there; expected large transitive/native dependency changes from each optional feature should be kept scoped to that feature.

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

## FoundationDB user-owned native client preparation

The `foundationdb` feature requires a compatible FoundationDB **7.3.x client library** at both link and runtime. This workspace must not install packages globally, modify `/usr/include`, copy libraries into system paths, bundle the library, or silently download it. Use the local lifecycle's owned 7.3.77 image/library preparation to copy `libfdb_c.so` into a user-controlled directory, and retain the image digest and copied library SHA/version as lifecycle evidence. The reviewed local image selected by the orchestration owner is `foundationdb/foundationdb:7.3.77`, digest `sha256:559b178a137bbe09272ad6e3752531aa04e2ddacab8ccfa4ffa86467db033bea`; this pin is for local preparation only, not proof of a server run.

For a user-owned prefix such as `$HOME/.local/opt/foundationdb-7.3`, place the library under `$HOME/.local/opt/foundationdb-7.3/lib`, set `FDB_CLIENT_LIB_PATH` to that directory while compiling the feature, and set `LD_LIBRARY_PATH` to the same directory only for the benchmark process. The crate feature fixes the C API header ABI at API 730 (`fdb-7_3`) and uses its own embedded matching headers; the actual `libfdb_c.so` must still report/build for the matching 7.3 ABI. The crate's build script also uses bindgen, so provide the required clang/libclang toolchain locally without installing it from this project. No global linker configuration is changed. Provide the explicit cluster file in adapter options. Missing header feature, library, incompatible ABI, file, or service is an error, never a skip or native fallback.

The pinned release/reference sources are [foundationdb-rs 0.11.0 prerequisites and API features](https://docs.rs/crate/foundationdb/0.11.0), [foundationdb-sys 0.11.0 linker build script](https://docs.rs/crate/foundationdb-sys/0.11.0/source/build.rs), [FoundationDB C API versions](https://docs.rs/foundationdb-sys/0.11.0/foundationdb_sys/constant.FDB_API_VERSION.html), and [FoundationDB machine-readable status](https://apple.github.io/foundationdb/mr-status.html). The C API build script supports `FDB_CLIENT_LIB_PATH`; no host-global install is required.

## Primary implementation references

- TiKV client-rust: [RawClient API](https://docs.rs/tikv-client/0.4.0/tikv_client/struct.RawClient.html), [0.4.0 source/release metadata](https://crates.io/crates/tikv-client/0.4.0), and [PD config HTTP API](https://docs.pingcap.com/tidb/stable/pd-control/). Raw `batch_get` explicitly omits non-existent keys and does not preserve input order; this implementation accounts for both. The existing go-ycsb integration is only a workload reference: its transactional/database semantics are not copied.
- Scylla Rust driver: [1.9.0 driver docs](https://docs.rs/scylla/1.9.0/scylla/), [prepared-statement consistency/timeout methods](https://docs.rs/scylla/1.9.0/scylla/statement/prepared/struct.PreparedStatement.html), [logged batch API](https://docs.rs/scylla/1.9.0/scylla/statement/batch/struct.Batch.html), and [1.9.0 source manifest/features](https://docs.rs/crate/scylla/1.9.0/source/Cargo.toml.orig). Cassandra's [storage-engine documentation](https://cassandra.apache.org/doc/5.0/cassandra/architecture/storage-engine.html) distinguishes commitlog acknowledgement from configured sync behavior and explains replay after restart; this is why `durable_writes` is not reported as verified disk fsync.
- FoundationDB: [Rust `Database::run` and timeout/retry semantics](https://docs.rs/foundationdb/0.11.0/foundationdb/struct.Database.html), [transaction methods](https://docs.rs/foundationdb/0.11.0/foundationdb/struct.Transaction.html), [status JSON field contract](https://apple.github.io/foundationdb/mr-status.html), and [architecture](https://apple.github.io/foundationdb/architecture.html). The adapter uses retryable native transactions, not go-ycsb's read/merge/write `Update` path.

## Evidence boundary

The constructors query live metadata. Actual-server qualification must retain the deployment receipt, executable/configuration identities and full-payload/ordered-group checks; compilation and mocked configuration tests are not live-server evidence. The separate five-host `cloud_layout.py` contract also verifies physical placement and effective acknowledgement controls. Local loopback qualification does not establish independent-NVMe or EFA performance. Cloud measurements require the source-bound release package and real hardware admission described in `experiments/kv-comparison/README.md`.

## Tidehunter transport adapters and external owner preparation

The distributed adapter kinds share the unchanged `tidehunter/examples/support/tcp_cluster.rs` engine harness and the same keyshape, owner protocol, WAL acknowledgement and RF1 semantics:

| Backend kind | Engine `Transport` value | Security/data path |
|---|---|---|
| `tidehunter_tcp` | `tcp` | Existing TCP with mutual TLS; default local path remains available without libfabric. |
| `tidehunter_efa` | `efa` | EFA provider with mutual TLS. |
| `tidehunter_efa_direct` | `efa-direct` | EFA-direct with mutual TLS. |
| `tidehunter_efa_native` | `efa-native` | Trusted-network EFA-direct without TLS; this is a separate unencrypted security cohort. |

Build one comparator binary with `--features "efa,rocksdb,tikv,cassandra,foundationdb"` as needed. The `efa` feature explicitly forwards `tidehunter/experimental-fabric`; without it, EFA configuration fails during config admission. `Transport::validate` rejects absent fabric support, and the real transport open rejects a missing EFA device/provider. These paths never relabel or fall back to TCP. The engine's reviewed hardware runtime is AWS libfabric `2.6.0amzn1.0` (provider ABI 206.0, EFA protocol 4); the feature does not install a provider. `efa-native` deliberately disables application TLS and must not be compared as security-equivalent to the three TLS transports.

Every distributed adapter accepts the existing `external_remote_config` object plus an `external_owner_read_config` array. The latter is required for external owners and must have exactly one entry for every configured partition:

```json
{
  "partition": 0,
  "read_workers": 2,
  "wal_member_read": "async",
  "read_resident_first": false,
  "read_cache_bytes": 8388608,
  "read_memory_allowance_bytes": 536870912,
  "read_max_inflight_requests": 32,
  "read_max_inflight_bytes": 67108864,
  "read_max_device_reads": 128,
  "resident_reads": {
    "source_bytes": 0,
    "source_regions": 0,
    "destination_bytes": 0,
    "destination_slots": 0,
    "max_read_bytes": 0,
    "max_handles": 0,
    "require_device_read": false
  }
}
```

This is copied from the actual serialized owner `NodeConfig`, not inferred from the coordinator's request. `external_remote_config.tls.transport` must exactly match the selected backend kind; TCP under an EFA label is rejected. The coordinator cache budget is configured by `coordinator_read_cache_bytes`; when absent, an external adapter preserves the budget in `external_remote_config.read_cache_bytes` (the engine default is 8 MiB). Effective JSON configuration retains the exact transport name, trust mode, coordinator cache settings, owner storage limits, owner read configuration and placement evidence. External topology is not asserted to use independent NVMe unless its deployment evidence establishes that fact.

Use the built binary's pure prep mode to generate engine-native external configs instead of reconstructing their YAML in deployment code:

```sh
kv-research --prepare-external-case input.json --output FRESH_DIR
```

`input.json` has exactly these required fields: absolute `guest_root`; five unique `addresses` as `IPv4:port`/`[IPv6]:port` strings; `transport` (`tcp`, `efa`, `efa-direct`, or `efa-native`); positive `max_batches_per_partition`, `max_admitted_batches` and `max_index_bytes`; nonnegative `coordinator_cache_bytes`; positive `owner_read_workers` and `owner_read_memory_allowance_bytes`; nonnegative `owner_read_cache_bytes`; `owner_wal_member_read` (`async`, `mapped`, or `positional`); `owner_read_resident_first`; `resident_reads` with the fields shown above; and `polling` (`adaptive` or `busy`). The helper fixes the comparison keyshape at 16 mutexes × 64 cells and writes `node-{0..4}/launch.yaml`, `remote.yaml`, `remote.json`, `shape.yaml`, generated `tls/` material when TLS is selected, plus `external_owner_read_config.json` extracted from each actual launch file. Owner limits are 32 operations/320 KiB encoded batch, 32 batches/1 MiB group, 1 MiB fragments, 1 GiB WAL files, 64 MiB reference metadata and four epochs. Owner read admission includes a 64 MiB in-flight allowance, 32 requests and 128 device reads. A pre-existing output directory is rejected. The signing key stays in the generated coordinator-side TLS tree; stage each node's launch config and required CA certificate/server leaf certificate+key, never the CA signing key, to that owner. The coordinator uses `remote.json` credentials.

At shutdown, `read-evidence.json` records coordinator cache hits/misses/bytes and read RPC count separately from the most recent available owner read-executor and resident/RDMA counters. Owner `wal_reads` are logical member reads, not device IOPS; async submissions are not proof of physical NVMe activity. Evidence includes sample generation/age/availability and the effective cache/read configuration. It adds no per-operation sampling calls; a passive bounded owner monitor samples outside the measured API calls.

## Measurement gate for external cold-state capture

An optional `RunConfig.measurement_gate` pauses after preload, warm-up and aging have drained, immediately before the `measurement` phase begins. Its shape is:

```json
{"ready_file":"/absolute/existing-dir/ready","release_file":"/absolute/existing-dir/release","timeout_seconds":60}
```

Both marker paths must be distinct, absolute and absent when the run starts; their parent directories must already exist. The runner atomically creates the ready marker with create-new semantics, then waits at most `timeout_seconds` (maximum 120) and never beyond the run's global deadline. A controller may collect device counters and perform explicitly authorized cold-state preparation on owned hosts while the workload is paused, then create the release marker. Existing application-cache configuration and actual cache/RDMA evidence remain recorded separately; the gate does not claim that page-cache dropping removes all memory-resident data.
