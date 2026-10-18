# DistributedANN: complete paper review and exact-KV applicability

Reviewed 6 October 2026 for the source-guided distributed pipeline slice. This is
the dedicated paper review; implementation status and measurements are in
[the source-to-code record](SOURCE_GUIDED_PIPELINES_2026-10-06.md).

## Source identity and reading scope

- Philip Adams et al., **DistributedANN: Efficient Scaling of a Single DiskANN
  Graph Across Thousands of Computers**, first Workshop on Vector Databases at
  ICML, 2025. [Microsoft-hosted full PDF](https://www.microsoft.com/en-us/research/wp-content/uploads/2025/07/DistributedANN_Adams_VecDBICML.pdf).
- Downloaded and read **all eight pages**, including algorithms, footnotes,
  evaluation, limitations, future directions, acknowledgments and references.
  The PDF metadata identifies creation on 29 June 2025.
- Saved review copy: `logs/source-guided-2026-10-06/references/DistributedANN_Adams_VecDBICML.pdf`.
  SHA-256: `345ef211697d3edc4d5cfafea2ecdf62025d3c07fee55489e489cd6120c6260a`.
- Text was extracted page by page; the two algorithms and their placement were
  also inspected on rendered page 3. Page numbers below are printed/PDF pages,
  both one-based. No public implementation of Bing's DistributedANN services is
  claimed to have been inspected. Microsoft/DiskANN is a separate code reference
  owned by the [DiskANN review](DISKANN_SOURCE_REVIEW_2026-10-06.md).

### Reading inventory

| Pages | Material checked | Relevance decision |
| --- | --- | --- |
| 1 | Abstract, motivation, introduction | Throughput scaling is a distinct goal from latency parity. |
| 2 | Figure 1; §§2.1–2.2 | Remote cache hits still need communication; graph-specific replication does not transfer directly. |
| 3 | §§2.2–2.4; Algorithms 1–2; footnotes 4–6 | Bounded independent computation around an orchestration layer is applicable as a scheduling principle. |
| 4 | End of §2.4; §3 graph construction; §4 setup | Partial ANN results and graph stitching do not establish exact KV correctness. |
| 5 | Table 1; §§4.1–4.2; Figure 4 | Better throughput can coexist with higher latency; resource budgets and failure semantics must be explicit. |
| 6 | Table 2; §§4.3–4.4; §§5–5.1 | GPU/index partitioning alternatives and networking suggestions are tradeoffs, not a prescribed KV architecture. |
| 7 | End of §5.1; acknowledgments; impact; references | Network topology matters; dense-network improvement is proposed future work. |
| 8 | Remaining references | No additional implementation or durability algorithm is supplied. |

## Mechanisms and what actually transfers

The paper distributes graph-search computation near its stored graph nodes,
retains a smaller in-memory head index, and orchestrates bounded search rounds.
Its exact mechanism is **not** an exact key-value store commit protocol. The
reported throughput improvement accompanies higher latency and greater storage
and network use (Table 1). Section 4.2 permits failed node reads to reduce recall;
that tradeoff is unavailable to our exact KV interface.

| Exact source location | Existing local problem | Safe adaptation / boundary |
| --- | --- | --- |
| §2.2, p3, “In-Memory Head index” | A storage-node page-cache hit still traverses TCP/TLS. | Our previously implemented reference-keyed cache at the authority can avoid an exchange. It is a disposable full-value cache, not an ANN head index; current misses still require the complete value. |
| §2.3, p3, Algorithm 1 | `RemoteDatabase::submit` previously prepared/hashed each complete batch on one caller thread. Preparation dominated the measured larger write shape. | Bounded independent preparation workers leave identity allocation, receipts and publication at one authority. This is our inference from the placement principle; the paper does not implement batch hashing or ordered WAL preparation. |
| §2.3, p3, uniform work discussion and footnote 4 | Arbitrarily large/unbounded preparation jobs can exhaust memory or monopolize workers. | Bound a submitted window by batches, encoded bytes, retained input, output and metadata. Dispatch bounded work; retain ownership accounting until the corresponding publication. Do not copy ANN batch-size constants. |
| §2.4, pp3–4, Algorithm 2 | Independent reads can be serialized by the coordinator even when their references are known. | Look ahead across a bounded number of scan pages while preserving the fixed published cut and ordered exact output; see the DiskANN implementation mapping. No ANN heap/pruning is introduced. |
| §4.1, p5, head-index CPU bottleneck | Distributing storage does not distribute a serial compute bottleneck automatically. | Measure coordinator preparation separately from node persistence and exchange. The present single authority remains responsible for one database; replicate/shard it only with a separate correctness protocol. |
| §5.1, pp6–7, networking future work | Lower network latency may help misses after software bottlenecks are reduced. | Retain TCP for this slice. The paper supplies no measured RDMA/NVMe-oF result or proof that either solves our preparation/persistence bottleneck. |

## Transfers explicitly rejected

- **Score-only responses:** Algorithm 1 returns candidate identifiers and scores.
  Exact `get` must return the entire value and verify its exact membership. Moving
  verification to the server cannot substitute for client validation of received
  bytes without a new integrity model.
- **Partial successful scans:** §4.2's graceful recall loss is acceptable to the
  evaluated ANN service. Our missing/timed-out/corrupt record is an error. No
  skipped output, false “not found,” publication hole or early credit refund is
  permitted.
- **Replica racing:** Hedging in §2.4 presupposes suitable replicas. This static
  RF1 backend has none; duplicate in-flight writes must not gain new identities.
- **Additional durable value copies:** §2.2 duplicates compressed neighbors to
  reduce ANN hops. Our values-in-WAL property needs no analogous extra value log.
- **Global WAL claim:** A graph distributed over a KV store says nothing about
  global append ordering, torn-write recovery, fencing, or the safety of a shared
  WalPosition. Those rules come from Tidehunter and our explicit protocol.

## Tidehunter correctness anchor

The current local paper is `../middleware/misc/tidehunter.pdf` (14 pages,
arXiv:2602.01873v2; SHA-256
`73957df11a17dc80b61eef8f18a4f8af988954c1772402553e595d2a6c5e4127`).
The following sections and actual code were re-read for this scheduling change:

| Rule | Paper | Source in this checkout | Consequence for parallel preparation |
| --- | --- | --- | --- |
| Allocation precedes independent work; highest allocated is not completed | §3.1 | `wal/tracker.rs::WalTrackerState::add_processed` and `distributed/sequence.rs` | Give every prepared batch one deterministic identity/version. A failed preparation must not silently consume a missing live sequence. |
| Complete batches replay and publish as a unit | §3.1 | `db.rs::write_batch_into_wal`, `index/pending_table.rs::Transaction::commit`, `distributed/publication.rs::PublicationTracker` | Publish only after exact complete durable receipts, in the contiguous admitted prefix. |
| Values are addressed from the index in the Value WAL | §3.2 | `distributed/codec.rs::CompleteBatch::decode_record`, `distributed/native_index.rs`, `distributed/remote_db.rs` | Keep physical node/stream addresses separate from logical sequence; preserve member and full-key checks. |
| Recovery frontiers are evidence, not inferred from a maximum | §§3.3–3.4 | `distributed/publication.rs`, inventory/replay modules | Timeouts remain unknown; do not continue past uncertain append/publication. |
| Relocation is space reclamation with safe frontiers | §4.4 | Native relocation/tracker, retained experimental streams | This slice adds no reclamation or reference lifetime relaxation. |

The native `Db` implementation and native formats are the control. Heavy changes
here stay in the experimental distributed layer; the paper's client parallelism
does not itself make the current mutable coordinator safe for concurrent mutation.

## Review of the chosen preparation design

The selected design uses a bounded batch window: validate and bound jobs, prepare
independent canonical batches concurrently, then admit them in deterministic
sequence order and dispatch through the existing storage pipeline. The exact
implementation and public API are recorded in the main source-to-code document.

Required checks before calling it safe:

1. Complete-batch encoding and digests remain byte-identical to the sequential
   codec, including deletes and repeated keys; no new durable format is needed.
2. Invalid input is rejected before logical allocation or storage effects. Worker
   failure cannot be converted into successful skipped work.
3. Retry identity resolves to the original version/digest; changed retries fail.
4. All accepted work, intermediate encoded output and worker-held owners are
   bounded. A cheap `Bytes` clone is not evidence that its backing allocation is
   small; detach or account retained owners deliberately.
5. Completion out of order cannot publish out of order. Memory credit returns
   only after the last retained operation owner is released and publication ends.
6. The entire window returning from admission still is not a durable
   acknowledgement. Existing `drain`, checkpoint and shutdown barriers remain.
7. Benchmark identical data, cache policy, persistence policy and admitted window;
   include a sequential preparation control and native control, keeping timing
   sums distinct from wall-clock stage durations.

## What this paper does not establish

It does not prove native Tidehunter latency parity, independent-NVMe speedup on a
single laptop SSD, an RDMA design, or correctness of our implementation. Local
tests can establish the exercised scheduling and recovery properties. Matched
measurements must establish whether parallel work pays for its overhead at each
batch/window size. Small windows may correctly use the scalar path.
