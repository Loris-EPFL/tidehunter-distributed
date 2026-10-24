# Native Tidehunter control

The `native` backend opens the ordinary `tidehunter::db::Db` with one fixed-size keyspace. Both native and TCP controls use the established adapter-benchmark shape of 16 mutexes and 64 cells per mutex (1,024 cells total); dataset key count does not expand the index geometry. The harness generates its opaque 32-byte keys; Tidehunter's `KeyType::Uniform` maps the first four key bytes to cells and retains the complete key in the index/WAL. This is the native index and key-shape path, not an independently indexed collection of WAL owners. See the Tidehunter paper, §§3 and 6.1 (`../../../papers/parsed/Tidehunter-2026-v2.md`): the Large Table indexes values by WAL position, and the paper's baseline uses 32-byte keys.

## Separate acknowledgment modes

- `durability: "page_cache"` is the regular foreground `Db::insert`, `Db::remove`, or native `WriteBatch::commit` return. No additional `persist_value_wal_for_benchmark` call is made. This measures the ordinary foreground native path, including whatever buffering/page-cache work the engine performs; it does **not** claim a foreground fsync durability barrier.
- `durability: "value_wal_barrier"` performs that same mutation and then calls the existing `Db::persist_value_wal_for_benchmark` Value-WAL persistence barrier before returning. The barrier's time is part of the API operation. It synchronizes the registered Value-WAL files and their directory (including clean older files); it is not a full index snapshot, a transaction-isolation mechanism, or an operating-system/device power-loss certification.

The modes are separate option values and are reported independently in `BackendInfo`. A write batch uses Tidehunter's native batch implementation; its `BatchAtomicity::CrashRecovery` label means native WAL batch recovery behavior only. It does not mean that other readers cannot observe intermediate publication or that the batch has serializable isolation.

Native gets return Tidehunter's `Bytes` directly. `get_many` performs ordered scalar gets and retains those returned buffers; it neither changes the keyspace API nor adds a copy of value payloads. Native metrics are registered in the caller's Prometheus `Registry` via `Metrics::new_in_enabled`, then included by the shared result exporter.

## Limits of the comparison

This is a one-process, one-SSD reference. It does not establish independent-NVMe scaling. Native Tidehunter has WAL relocation and index-maintenance behavior; do not compare its reclaim policy to the distributed Value-WAL prototype as if their storage lifetimes were identical. The distributed owner backend uses bounded retained history and does not reclaim physical Value-WAL space. The paper's fill/cooldown/measurement experiment uses a 1-TB fill and ten-minute phases on a 6.4-TB NVMe host; the small local harness deliberately is not that experiment and must not be represented as a reproduction of its performance results.

Primary mechanism references: `tidehunter/src/db.rs` (`Db::insert`, `Db::remove`, `Db::get`, `persist_value_wal_for_benchmark`) and `tidehunter/src/batch.rs` (`WriteBatch::commit`); Tidehunter paper §§3 and 6.1, especially the 32-byte key baseline and fill/cooldown/measurement methodology in §6.1.
