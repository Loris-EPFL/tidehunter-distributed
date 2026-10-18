# DiskANN source review and bounded WAL scan lookahead

Date: 6 October 2026. Scope: local experimental distributed Tidehunter reads.
The implementation described below is in this checkout; validation status is
recorded in the final source-guided campaign. No RDMA, NVMe-oF or AWS operation
is introduced by this change.

## 1. What was read

The assigned agent read **all 11 pages**, including the algorithms, evaluation,
figures and references, of Subramanya et al., *DiskANN: Fast Accurate Billion-point
Nearest Neighbor Search on a Single Node*, NeurIPS 2019:
[publisher PDF](https://papers.nips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf).
The PDF SHA-256 is
`15ee6136e41eb70d05692ca5f5af18b47fc63754c3a5004de686d8b1be2c6d2c`.

The current Microsoft repository was pinned at
[`bab8136b52ce47b484eb5b74fbb328e43b144061`](https://github.com/microsoft/DiskANN/tree/bab8136b52ce47b484eb5b74fbb328e43b144061).
These files, including their comments and tests, were read in full:

| File at that revision | Scope |
| --- | --- |
| `README.md` | 64 lines; provider architecture and current Rust/legacy C++ distinction |
| `diskann-disk/src/search/provider/aligned_file_reader/reader/linux.rs` | 374 lines; all implementation and completion tests |
| `diskann-disk/src/search/provider/cached_disk_vertex_provider.rs` | 161 lines; all cache filtering and result-slot mapping |
| `diskann-disk/src/search/provider/disk_vertex_provider.rs` | 488 lines; all disk-load/result-mapping code and tests |
| `diskann/src/graph/search/knn_search.rs` | 291 lines; all entrypoint, recorded search and parameter tests |
| `diskann/src/graph/search/mod.rs` | 131 lines; all search API documentation |

The connected `disk_provider.rs` was inspected specifically at lines 815–995
(`DiskAccessor::expand_beam`, `ensure_loaded` and adjacent traits), rather than
claiming the entire 3,131-line provider or every DiskANN wiki page was read.
The complete paper and this explicit source scope are the basis for the change.
Downloaded source evidence is retained under
`logs/source-review-2026-10-06/diskann/`.

## 2. Source mechanism → measured problem → implementation

The prior local report measured a 21.6 ms distributed scan versus 7.1 ms native
for 8 KiB values, batch/window 32. Scalar cache-cleared reads spent approximately
26.9 µs in exchange and 9.17 µs verifying records. Those scalar observations do
not quantify scan-stage attribution. The concrete source-level scan problem is
that each 256-reference page was fully fetched before the next page was resolved
or submitted, even though its references were independent.

| Exact source location | Mechanism relevant here | Change in our code | Limit of the transfer |
| --- | --- | --- | --- |
| Paper §3.3, pp. 6–7; §4.2, p. 8 | Small concurrent frontiers reduce sequential I/O rounds; excessive concurrency can hurt queues and bandwidth. | `remote_db/read_pipeline.rs::scan_prefetched` dispatches up to two bounded pages before waiting, allowing later-node work to proceed while an earlier page is assembled. | This is our adaptation, not a claim that DiskANN implements this exact two-page RPC scheduler. |
| [`LinuxAlignedFileReader::read`, lines 91–123](https://github.com/microsoft/DiskANN/blob/bab8136b52ce47b484eb5b74fbb328e43b144061/diskann-disk/src/search/provider/aligned_file_reader/reader/linux.rs#L91) and `many_batches_all_should_have_data`, lines 208–245 | Bound outstanding work and account for completion before exposing results. | `ScanPage::bounded` limits normal pages to 256 records and 4 MiB of conservative record/metadata charge. `finish_scan_page` checks every result count and emits pages in index order. | We reuse the existing authenticated TCP workers, not its `io_uring`, `O_DIRECT` or 512-byte alignment policy. |
| [`CachedDiskVertexProvider::load_vertices`, lines 79–101](https://github.com/microsoft/DiskANN/blob/bab8136b52ce47b484eb5b74fbb328e43b144061/diskann-disk/src/search/provider/cached_disk_vertex_provider.rs#L79) and `filter_cached_nodes`, lines 141–156 | Remove cache hits from storage work while retaining original result positions. | `dispatch_scan_page` consults our existing exact-reference cache, groups only misses by node and keeps per-response destination slots. | Our cache identity includes the immutable WAL reference, not a graph vertex ID; a scan does not fill/evict the point-read cache. |
| [`DiskVertexProvider::process_loaded_node`, lines 129–193](https://github.com/microsoft/DiskANN/blob/bab8136b52ce47b484eb5b74fbb328e43b144061/diskann-disk/src/search/provider/disk_vertex_provider.rs#L129) | Result location must remain tied to request identity despite I/O completion order. | `PendingScanPage` retains index keys and slot maps. `checked_row` verifies keyspace and the reduced full key against the exact index row before returning a value. | Every exact KV result is required; none can be omitted as an approximate candidate. |
| [`Knn::search`, lines 168–191](https://github.com/microsoft/DiskANN/blob/bab8136b52ce47b484eb5b74fbb328e43b144061/diskann/src/graph/search/knn_search.rs#L168) | Explicit search-width control makes concurrency a measurable policy. | `RemoteDatabaseConfig::scan_prefetch_pages` accepts 1 or 2, defaults to 2, and allows a matched same-binary serial control. | This is an exact scan policy, not a recall/latency knob. Both settings must return identical data. |

The paper's §3.4 cache idea supports keeping useful data in RAM, but the existing
verified-value cache predates this change. This slice **does not claim credit for
introducing that cache**. Paper §§2.1–2.4 and §3.1's graph construction, §3.2's
fixed-vector layout, and §3.5's approximate search refinement do not replace
Tidehunter's index, WAL format or exact key-value semantics.

## 3. Tidehunter correctness boundaries

The local Tidehunter paper §§3.1–3.4 and the native implementation were revisited:

| Native rule / source | Preservation here |
| --- | --- |
| Paper §3.2; `db.rs::get` / `read_record_check_key` resolve the index and then a particular WAL value. | Each queued read retains the full published `ValueRecordReference`; existing TCP membership, epoch and member-digest checks are unchanged. |
| Paper §§3.1/3.3; `db.rs::checkpoint` distinguishes an allocated position from a completed index frontier. | The scan captures `self.latest` once as `IndexReadCut::At`; it does not drain writes or advance publication to make a read succeed. |
| `distributed/native_index.rs::scan` returns reduced index keys and rejects hash-index ordered scans. | Cursors use reduced index keys; `checked_row` compares the returned full key after the same native reduction. Hash-index rejection remains before remote work. |
| Native WAL values remain authoritative; derivative RAM is disposable. | Cache hits can avoid I/O, but any observed scan transport/fencing/integrity error marks the authority unhealthy before later cache reads. Prefetch adds no durable copy. |
| Static RF1 retains registered generations; future GC must protect readers. | No retention or reclamation boundary changes. Before future WAL reclamation is enabled, this fixed read cut needs a corresponding reachability/retention guarantee. |

The node connection remains sequential and authenticated. There is no new routing
hop, protocol message or replacement checksum. Prefetch does not turn the mutable
database handle into a generally concurrent API, and does not add a native worker
inside the original single-instance engine.

## 4. Bounded memory and errors

Each normal page has at most 256 references and at most 4 MiB of nominal record,
index-key and slot charge; at most two such pages are dispatched. A legal record
larger than the page allowance is served alone, without another dispatched page.
An individually impossible transport record is rejected before dispatch. The
underlying frame limit and peer-negotiated limit still apply.

The charge is a scheduling allowance, not an exact resident-memory accounting or
an 8 MiB RSS promise. Existing TLS/socket/frame buffers, receive/decode scratch,
retained vector capacity (at most 256 rows per page), allocator rounding, the
authoritative index, a small
lookahead vector of up to 256 index references, and the caller's returned scan
collection are separate. The API already returns the full result `Vec`; converting
it to a streaming external interface would be a separate API change.

If a later page fails, the whole call returns an error rather than a partial scan.
Already queued immutable reads may finish after a failure; dropping their receivers
does not acknowledge or mutate any data. Subsequent cached reads are blocked by
the existing unhealthy-authority gate.

## 5. Implemented validation

Tests live in `tidehunter/src/distributed/remote_db/read_pipeline/tests.rs`:

1. `byte_budget_bounds_page_and_oversized_record_is_alone`: large-record metadata,
   boundary splitting, legal singleton handling and impossible-frame rejection.
2. `exact_ordered_row_rejects_mismatched_key_keyspace_and_tombstone`: wrong key,
   wrong keyspace and tombstone cannot become a live scan row.
3. `multi_page_scan_matches_serial_cut_updates_deletes_and_reopen`: 320 exact keys
   on five mTLS nodes; identical serial/prefetched results after updates/deletes,
   held checkpoint reads, an unpublished write, cache reuse without scan fill,
   index unload and authority reopen.
4. `later_page_node_completes_while_first_node_is_explicitly_stalled`: a held first
   node connection proves later-node progress is independent; final output order
   stays unchanged. The assertion uses a bounded progress timeout, not a speed
   threshold.
5. `observed_scan_fence_failure_blocks_later_cached_reads`: an uncached stale-epoch
   result closes the authority before a previously cached value can be returned.

All five new tests passed in the root agent's 22-test remote-database suite.
Final whole-library pass counts, source hashes and measured performance belong in
[the combined campaign record](SOURCE_GUIDED_PIPELINES_2026-10-06.md).

## 6. What this does not solve

This change removes an avoidable page-level scheduling dependency. It does not
remove a scalar remote read's exchange or verification cost; it does not prove
native parity or independent-NVMe scaling on a laptop with one SSD. Cold-device
queue depth, multiple requests per connection and streaming application consumers
remain separate measurements/design decisions. The DiskANN paper supplies a
bounded-concurrency mechanism, not a prediction of our resulting speedup.
