# Run 9 supervisor-brief edit plan — archival, integration completed

## Status, scope and source of truth

This is the retained **pre-result planning ledger**, not current campaign or document status. The authorized real-data integration is complete: the final brief is 47 pages, with acceptance and visual inspection recorded in `rwc/REAL-ACCEPTANCE.json`; exact rebuild commands are in `rwc/RUNBOOK.md`. All quoted “current/unpatched” sources, line numbers, future-tense instructions and placeholders below refer to the earlier planning snapshot. They are preserved as design history, not outstanding work. `rwc/main-tex.patch` now records the final main-only integration diff against `main.tex.pre-run9`; it is no longer the earlier pre-result draft and must not be reapplied to the already integrated source.

**Historical planning snapshot, not final status:** Main then reported the authorized second attempt at 137/342 cases. Final receipts now show all 342 terminal: 339 controller-completed and three failed G11N event-mode floors, with six analytically unqualified Native cold 1-KiB cases. The report retains `complete=false`. The preliminary observations in §4 remain quarantined history; only the selected final export supplies generated findings. Synthetic fixtures remain isolated and explicitly SYNTHETIC.

`{UPPER_CASE_BRACES}` below are explanatory placeholders, not literal final TeX. Use source-traced generated number macros and actual `\RWCFINDING{...}` expansions at integration. Do not leave a placeholder in the final PDF, invent an absent statistic, or insert a new raw RWC number key into the old root number scanner. No separate abstract/environment is present in `main.tex` or the six inspected `cross-store-*.tex` files; `main.tex:33–55` is the abstract-like scope/findings text to revise. Do not create a competing abstract or rename the brief.

## 1. Exact-source edit ledger

Entries distinguish stale statements from historically correct statements that need scope or a cross-reference. A new compact experiment does **not** invalidate measurements from runs 7/8 or the historical full-matrix fleet. Preserve their numbers, population IDs, qualification verdicts and contracts.

### 1. Opening scope; no separate abstract exists

**Source:** `main.tex:33–34`. **Action:** Augment scope; retain the existing no-Sui-replay statement.

Exact current text:

````text
\section{Scope and findings}
This brief reports synthetic key--value timings and separate Sui value-size observations, not Sui replay performance. Closed-loop concurrency $c$ is the number of workers that each wait for one call to finish before starting the next. We write c1 and c16 for $c=1$ and $c=16$. A route is the path between the caller and the stored value. An owner holds one shard's value write-ahead log (Value WAL). The coordinator holds the distributed index and publishes completed writes. The native backend runs in process; it has no coordinator or network hop.
````

Proposed replacement / insertion template:

````text
\section{Scope and findings}
This brief retains the historical synthetic key--value measurements and separate Sui value-size observations. Run 9 adds the two-VM \texttt{remote\_rw\_compact} campaign: {FINAL_EXPORT_STATUS}, with matched sizes, caller counts and repetitions for Native and one-owner TCP/verbs writes and cold/warm reads. This is not Sui checkpoint replay, five-owner scaling or a matched-durability ranking. {EXISTING_WORKLOAD_DEFINITIONS_UNCHANGED}
````

### 2. Fleet inventory and repetition footnote

**Source:** `main.tex:38–44`. **Action:** Add a distinct run-9 row; move the bottom rule after it. Do not change historical fleet identifiers or repetition counts.

Exact current text:

````text
Data & Fleet, dates and use & Repetitions\\\midrule
Cross-store & Native/TH: \texttt{bench-0d99e8256fff4891}, 6--7 November. External stores span GCP fleets, days and binaries. & TH/native: 1; others: \N{S:ext_runs_min}--\N{S:ext_runs_max}\\
Run 7, device & \texttt{bench-6e40ca83d74146cf}; 9 November. SSD-served reads and puts. & 3\\
Run 8, unified & \texttt{bench-0a8a6af114a64287}; 9 November. Stages, floors and native controls. & 1 or 2$^{a}$\\\bottomrule
\end{tabular}
\end{center}
$^{a}$ Profiled copies run once. Network and SSD read floors run three times; write floors run twice. Run 8 completed \N{Q:run-summary:0:completed|.0f} of \N{Q:run-summary:0:planned|.0f} cases; \N{Q:run-summary:0:failed|.0f} failed. \N{Q:run-summary:0:unqualified|.0f} native cold 1 KiB cases are unqualified. Run 7 completed all \N{Q:run-summary:1:completed|.0f} cases. Fleets, workloads and repetitions differ; the example in Section~\ref{sec:native-gap} shows why times must stay separate.
````

Proposed replacement / insertion template:

````text
{EXISTING_CROSS_STORE_RUN7_RUN8_ROWS_UNCHANGED}
Run 9, compact & \texttt{{FLEET_ID_FROM_STATE}}; {LAUNCH_DATE}. Two VMs; one coordinator and one remote shard-WAL owner. & Main grid: r0, r1; read/network floors: r0--r2; append floors: r0, r1; diagnostics: r0 only.\\\bottomrule
{EXISTING_RUN7_RUN8_FOOTNOTE_UNCHANGED}
Run 9 planned 342 cases; {COMPLETED}, {FAILED}, {NOT_RUN}, and {UNQUALIFIED_COLD} are final-export counts, not canary progress. Distinguish controller completion from analysis qualification. Parent/attempt/package identity is recorded in Provenance and limits.
````

### 3. Run-9 findings insertion

**Source:** `main.tex:54–55`. **Action:** Preserve the historical cross-store finding; append data-generated run-9 findings before the list closes.

Exact current text:

````text
\item \textbf{Cross-store results stay separate.} Retained c16 1 KiB puts: owner verbs synchronous p50 \WC{p50}{th_verbs}{1024}~\us; historical Valkey RDMA AOF-always p50 \WC{p50}{valkey_rdma}{1024}~\us. The contracts and fleets differ. New runs do not replace these values.
\end{enumerate}
````

Proposed replacement / insertion template:

````text
{EXISTING_CROSS_STORE_FINDING_UNCHANGED}
\item \textbf{Run 9: matched one-owner grid.} \RWCFINDING{parity}
\item \textbf{Run 9: writes and qualified reads.} \RWCFINDING{results}
\end{enumerate}
% Final expansion: {MATCHED_PAIR_COUNTS}; {WRITE_MEANS_R0_R1}; {QUALIFIED_COLD_MEANS_R0_R1}; {WARM_MEANS_R0_R1}; {NATIVE_UNQUALIFIED_CELL_LIST}. No preliminary canary numbers.
````

### 4. Method: add compact topology rather than rewriting six-VM history

**Source:** `main.tex:58–59`. **Action:** Append a run-9-specific topology/parity paragraph to Method. Method currently has no explicit six-VM topology sentence; the six-VM strings are historical macros, not stale run-9 settings.

Exact current text:

````text
\section{Method and contracts}\label{sec:method}
API time is completion minus dispatch. Response time is completion minus schedule time in the closed-loop harness; it includes scheduling and admission. Latency uses successful calls only. Rejected calls have no API latency. Quantiles use exact nearest rank within each operation and outcome. No samples or fleets are pooled. A median across runs is a median of per-run statistics, not a quantile of pooled calls. A range is minimum to maximum, not a confidence interval. One profiled repetition gives no confidence interval or significance claim.
````

Proposed replacement / insertion template:

````text
{EXISTING_API_CLOCK_AND_QUANTILE_DEFINITIONS_UNCHANGED}
\textbf{Run-9 compact topology.} One coordinator and one remote shard-WAL VM, both \texttt{z4m-highmem-16}. Native uses the coordinator SSD; owner TCP/verbs use the remote owner SSD. The coordinator cache is off. Match the workload size, reader/writer count and repetition; preserve the upstream parity gate. Native has no remote-read lanes; owners use one read lane at c1/c5 and four at c16. Thus configured readers match, but internal read-lane geometry does not. Native remains in process; RPC, security, index/routing and sync scope remain unmatched.
````

### 5. Method: grouped Native contract is missing from the global contract table

**Source:** `main.tex:64–67`. **Action:** Retain the existing per-put/page-cache/owner rows; add the implemented grouped-barrier row, without relabelling per-put barriers.

Exact current text:

````text
Native \texttt{value\_wal\_barrier} & Durable: \texttt{fsync} of the Value-WAL files and the directory after every put.\\
Native \texttt{page\_cache} & Returns after the page-cache write. Does not wait for fsync.\\
Owner \texttt{synchronous} & Returns after the covering Value-WAL \texttt{fdatasync}. Several puts can share it.\\
Owner \texttt{native} & Page-cache acknowledgement. This tier has no relation to the native backend.\\\bottomrule
````

Proposed replacement / insertion template:

````text
{EXISTING_CONTRACT_ROWS_UNCHANGED}
Native \texttt{value\_wal\_group\_barrier} & Waits for a covering grouped Native barrier; unlike the per-put barrier, callers can share it. The final report must name its file/directory scope from the packaged implementation; do not equate it to owner one-file fdatasync.\\
% Place before the table's bottom rule.
````

### 6. Read-section boundary: insert new run-9 body after all run-8 read material

**Source:** `main.tex:184–187`. **Action:** Insertion only. Keep historical read decomposition and Native qualification claims attached to run 8.

Exact current text:

````text
The owner device component is \N{C:ssd1v:device}~\us\ at 1 KiB and \N{C:ssd64v:device}~\us\ at 64 KiB, near the measured read floors. Native and owner device \emph{operations} differ in size and count; compare API calls, not device latency per operation. c16 read profiles cover only part of the reads and stay unsplit; the appendix gives their whole calls.

\clearpage
\section{Hardware parity}\label{sec:parity}
````

Proposed replacement / insertion template:

````text
{EXISTING_END_OF_RUN8_READS_UNCHANGED}
\input{rwc/section-rwc.tex}
\clearpage
\section{Hardware parity}\label{sec:parity}
````

### 7. Historical cold qualification needs an explicit run boundary

**Source:** `main.tex:180`. **Action:** Do not replace run-8 Native observations with run-9 canaries; add a cross-reference if needed.

Exact current text:

````text
\textbf{Native correction.} Native cold 1 KiB cases are unqualified. The 8 KiB cases pass the device-count gate but have mainly memory-like latency. The 64 KiB cases meet the case-level SSD-service qualification: about \N{D:ssd64n:device_reads_per_call|.2f} device reads per API call. Native reads recent fragments through the memory map and older fragments with positional reads. \inference\ Read-ahead of mapped faults or buffered positional reads can increase device transfer sizes. Window counters do not prove that every call reached NAND; the measurement-only mapped/positional split is not measured. The owner reads the exact value range.
````

Proposed replacement / insertion template:

````text
{EXISTING_RUN8_NATIVE_CORRECTION_UNCHANGED}
These are run-8 qualifications. Run-9 Native cold qualification is evaluated independently for every size, reader count and repetition; its final unqualified cells are {NATIVE_UNQUALIFIED_CELL_LIST_FROM_EXPORT}, with ratios/reasons in the run-9 qualification appendix.
````

### 8. Keep run-8 floor failures separate from run-9 failures

**Source:** `main.tex:198`. **Action:** Scope the existing failure paragraph explicitly to run 8 and append the distinct run-9 receipt reason.

Exact current text:

````text
Three \texttt{ib\_write\_lat} event-mode cases fail because that tool rejects the mode. Two 1 KiB direct-write cases fail with \texttt{EINVAL}: the SSD logical sector is 4 KiB. Neither failure removes a completed application case. The 1 KiB overwrite fdatasync mean is \N{Q:run8-ssd_write_floor:0:sync_mean_us}~\us\ for sync alone, versus \N{Q:run8-ssd_write_floor:0:mean_us}~\us\ for write plus sync. At 64 KiB they are \N{Q:run8-ssd_write_floor:6:sync_mean_us} and \N{Q:run8-ssd_write_floor:6:mean_us}~\us. This floor overwrites a pre-written file. It excludes fresh block allocation and journal work. It is not a WAL-append reference.
````

Proposed replacement / insertion template:

````text
In run 8, {EXISTING_FAILURE_AND_OVERWRITE_FLOOR_TEXT_UNCHANGED}. Run 9 independently records {FINAL_FAILED_EVENT_WRITE_CASE_IDS_AND_REPETITIONS}: \texttt{ib\_write\_lat}, event mode, with \texttt{network floor server exited before listener readiness}. These are failed tool/mode controls, not zero network latency or failed KV calls. Retain each failed receipt; do not replace the old run-8 failure description with the newer message.
````

### 9. Append-floor heading is stale

**Source:** `main.tex:211`. **Action:** Replace the not-run heading; completion is conditional on the retrieved export.

Exact current text:

````text
\subsection{Planned append floor: not run}
````

Proposed replacement / insertion template:

````text
\subsection{Append floors: run-9 evidence}
% If export is incomplete: "Append floors: launched, final results pending"; never say all 48 completed before checking receipts.
````

### 10. Append-floor not-run / authorization claim is stale

**Source:** `main.tex:212`. **Action:** Keep the reviewed design, but distinguish selection membership and actual completion.

Exact current text:

````text
Group G13W, selection \texttt{append\_floors}, has 48 planned cases: four modes, 1/8/64 KiB, client and owner 0, two repetitions. Modes are overwrite, append pwrite plus fdatasync, append mmap store plus fdatasync, and append mmap store plus fsync. Each append case writes each fresh sparse-file offset once (512 warmup, 4,096 measured operations). \textbf{It has not run and needs launch authorization.}
````

Proposed replacement / insertion template:

````text
Run 9 includes 48 G13W controls from the frozen \texttt{append\_floors} design: four modes, 1/8/64 KiB, client and owner 0, two repetitions. {EXISTING_MODE_AND_APPEND_GEOMETRY_DESCRIPTION_UNCHANGED} Final completion is {G13W_COMPLETED}/{G13W_PLANNED}; failed/not-run cases are {G13W_UNAVAILABLE_CASES}. The campaign is authorized and launched; standalone \texttt{append\_floors} is not thereby a separately completed campaign. See the run-9 floor appendix.
````

### 11. Append-floor inference table is too causal and still prospective

**Source:** `main.tex:215–218`. **Action:** Replace decision rules with qualified interpretation templates; do not turn independent floor comparisons into an overhead decomposition.

Exact current text:

````text
Planned comparison & Decision rule, not a result\\\midrule
Owner 0 append floor near client native sync time & \inference\ Owner-path software adds the gap.\\
Owner 0 append floor near owner sync; client floor near native sync & \inference\ VM or SSD differences explain the gap.\\
Other outcome & Mixed. The comparison does not isolate one cause.\\\bottomrule
````

Proposed replacement / insertion template:

````text
Observed control & Interpretation limit\\\midrule
{CLIENT_AND_OWNER_APPEND_MEANS_BY_SIZE_MODE_REPETITION} & [INFERENCE] Similar floors leave owner software as a candidate, not an identified residual.\\
{SAME_KIND_SYNC_CALL_COMPARISON} & [INFERENCE] Different floors can motivate a VM/SSD or allocation hypothesis, not prove its cause.\\
{FAILED_OR_MISSING_CONTROLS} & Not computed; retain status and reason. No subtraction of separately sampled means.\\
````

### 12. Run-7 all-qualified sentence is not a run-9 claim

**Source:** `main.tex:246`. **Action:** Qualify the first sentence explicitly; retain all run-7 evidence and the remaining paragraph.

Exact current text:

````text
All read cases qualify. Authority history loads stay unchanged, so this does not cover history-load misses. The SPDK kernel-counter excess is a per-case constant, not a per-call device cost; its bdev layer reports one aligned read per call. Its origin is open. Direct I/O does not attest NAND access.
````

Proposed replacement / insertion template:

````text
All run-7 read cases qualify. {REST_OF_EXISTING_RUN7_PARAGRAPH_UNCHANGED} Run-9 Native cold failures of qualification are listed separately; this historical sentence does not override them.
````

### 13. Carried-forward fleet introduction must also exclude run 9

**Source:** `main.tex:264`. **Action:** Retain six VMs and the historical fleet id, but remove ambiguity when a third later run is added.

Exact current text:

````text
\textbf{\carried.} Native and Tidehunter use fleet \texttt{bench-0d99e8256fff4891}, six \texttt{z4m-highmem-16} VMs in \texttt{us-central1-a}, RDMA and Local SSD. External-store statistics span other GCP fleets, days and binaries. These are not run-7 or run-8 measurements. Coordinator cache is off. API quantiles are success-only. Native waits for its own per-put Value-WAL file and directory fsync; owner synchronous waits for a covering fdatasync, which can serve several puts. Valkey uses AOF \texttt{appendfsync=always}. Similar durable intent does not prove equal persistence mechanics.
````

Proposed replacement / insertion template:

````text
{EXISTING_CARRIED_FORWARD_FLEET_AND_CONTRACT_TEXT_UNCHANGED}
Replace only "These are not run-7 or run-8 measurements." with "These are not run-7, run-8 or run-9 measurements." The historical full-matrix fleet still has six VMs; do not rewrite it as the compact fleet.
````

### 14. Historical unmatched comparisons remain separate from new matched workload grid

**Source:** `main.tex:285`. **Action:** Retain the historical workload explanation; add a narrow pointer rather than reusing old mixed c1 cases as run-9 partners.

Exact current text:

````text
\begin{table}[H]\centering\fitinput{tab-onekib-2.tex}
````

Proposed replacement / insertion template:

````text
{EXISTING_HISTORICAL_WORKLOAD_PARAGRAPH_UNCHANGED}
Run 9 instead uses the reviewed read-only/write-only compact grid at c1/c5/c16, matched within each temperature and repetition. Its workload pairing does not remove the remaining network, sync, index/routing, host-SSD and read-lane differences.
````

### 15. Next-work ranking must be conditional on completed controls

**Source:** `main.tex:316–317`. **Action:** Remove grouped-barrier not-decided language for the launched compact selection; do not imply the five-owner or Native c3/c4 controls ran.

Exact current text:

````text
The ranking is fixed below. Hardware parity is Gate 0, not a software optimization. Each expected effect is \inference. No predicted saving is a measured result. Maintain acknowledgement and validation checks in every A/B run.
Parity also requires equal SSD count and a matched acknowledgement policy. Native has one SSD against five owners. A Native c3/c4 control and a grouped Native barrier need owner approval (configuration and code change) and launch authorization; they are not decided work.
````

Proposed replacement / insertion template:

````text
The ranks below retain the run-8 hypotheses. Revisit their priority using the qualified run-9 controls {FINAL_STATUS_REFERENCE}; no new rank or predicted saving follows from canaries alone. Hardware parity remains a gate, not a software optimization.
The compact run measures one Native data SSD against one remote owner data SSD and includes the reviewed grouped Native barrier at c1/c5/c16. It does not equalize network or sync mechanics. Native c3/c4, five-owner scale-out and five-SSD Native remain unmeasured/unimplemented as applicable; do not mark them completed.
````

### 16. Next work rank 0: hardware-parity launch is underway, not future authorization

**Source:** `main.tex:322`. **Action:** Replace the prospective verify/decision cells after final floors are imported; retain run-8 numerical evidence as historical context.

Exact current text:

````text
0 & Hardware parity & Run 8 64 KiB c1 sync means: TCP \N{Y:t:65536:1:mean_us}, verbs \N{Y:v:65536:1:mean_us}, native file+directory \N{Y:n:65536:1:mean_us}~\us. Window counters are not a per-put comparison. Native cold reads \N{D:ssd64n:device_reads_per_call|.2f} device ops/call. & \inference\ Separate VM/SSD and append-allocation cost from owner software. & G13W append floors on client and owner 0. Use in-process sync steps. A pre-window sync/drain is future controller work. Keep Native's mapped and positional read paths unchanged; change read patterns only in distributed code. & Launch authorization. No native rewrite.\\[8pt]
````

Proposed replacement / insertion template:

````text
0 & Hardware parity & {RUN8_CONTEXT}; run-9 client/owner append and direct-read controls {PER_CASE_MEANS_AND_STATUSES}. & [INFERENCE] Separate hardware/allocation hypotheses from owner-path software; no floor residual. & Check size, host, QD, region/state and same-kind sync call; list {MISSING_OR_FAILED_CONTROLS}. & {EVIDENCE_READY_OR_UNRESOLVED}; no Native read-path rewrite.\\
````

### 17. Next work rank 1: network/cold/warm evidence is not a new stage timer

**Source:** `main.tex:323`. **Action:** Add run-9 same-fleet evidence and the failed event-write controls; keep the polling A/B as future work.

Exact current text:

````text
1 & Completion wake-up and network at c1 & Run 8 verbs memory read network \N{C:mem1v:network}~\us; durable put network \N{C:p1v:network}~\us. Compare RDMA polled/event and TCP block/busy-poll floors in Section~\ref{sec:parity}. & \inference\ Reduce idle completion and stack delay, at a possible CPU cost. & Busy-poll versus adaptive A/B, same fleet and contract; record p50, tails, throughput and CPU. & Permit a bounded polling experiment.\\[8pt]
````

Proposed replacement / insertion template:

````text
1 & Completion/wake-up at c1 & {RUN8_PROFILE_CONTEXT}; run-9 qualified cold and warm API populations {PER_REPETITION_REFERENCE}; separate successful network floors {FLOOR_REFERENCE}. & [INFERENCE] Completion/stack work is a candidate, not API minus Native or API minus floors. & Busy-poll/adaptive A/B with unchanged contracts and CPU/tails; exclude failed ib_write_lat event controls from latency statistics. & A/B remains unmeasured in run 9.\\
````

### 18. Next work ranks 2 and 3: distinguish actual diagnostic coverage

**Source:** `main.tex:324–325`. **Action:** Use the reviewed G14P diagnostic cases only if their final receipts completed. Do not claim run 9 supplies c1 TCP/verbs 64-KiB profiles or a new call-linked trace.

Exact current text:

````text
2 & Owner persist and flusher hand-offs at c1 & Run 8 verbs flusher queue mean \N{A:v1:flusher_queue_ns:mean_us}~\us; append-persist exceeds its own fdatasync by \N{A:v1:persist_software_ns:mean_us}~\us. Separate profile: locks+publication \N{P:locks_publication_sum_ns:mean_us|.2f}~\us. Coordinator waits \N{C:p1v:durable}/\N{C:p1v:coalescing}/\N{C:p1v:queue}~\us. & \inference\ Reduce flusher/frontier hand-off and wake-up, not locks. Keep durability. & Time append-persist and storage persist in the same population. Trace flusher and frontier wakes. Do not add nested queue counters. & No durability change.\\[8pt]
3 & Software that grows with value size & Run 8 verbs append-WAL mean \N{A:v1:append_wal_ns:mean_us} to \N{A:v64:append_wal_ns:mean_us}~\us. Unassigned owner mean at 64 KiB: \N{A:v64:handler_remainder_ns:mean_us|.2f}~\us. Coordinator \N{C:p1v:coordinator} to \N{C:p64v:coordinator}~\us. & \inference\ Decode and BLAKE2b verification can contribute. Full-payload BLAKE2b is a code fact, not measured attribution. & Add 64 KiB write-profile copies for verbs and TCP (two cases). Time decode, group build and record digest separately. & Ask before digest change: wire and persisted format.\\\bottomrule
````

Proposed replacement / insertion template:

````text
2 & Owner persist/hand-offs & {RUN8_LINKED_PROFILE_CONTEXT}; {COMPLETED_RUN9_DIAGNOSTICS_IF_ANY}. & [INFERENCE] Candidate hand-off work; retain covering sync. & Add same-population append/storage timers where missing. & Attribution remains open unless the required linked intervals are present.\\
3 & Size-dependent software & {RUN8_CONTEXT}; reviewed G14P covers Native 64 KiB c1/c16 and owner verbs 64 KiB c16, once each, with {FINAL_CASE_STATUSES}. & [INFERENCE] Decode/hash/group-build hypotheses, not isolated measured costs. & c1 TCP and verbs 64-KiB profiles/timers remain missing; do not substitute the c16 verbs profile. & No digest/format change inferred from throughput or independent floors.\\
````

### 19. Next work ranks 4--7 are not closed by the compact campaign

**Source:** `main.tex:334–337`. **Action:** Preserve the historical measurements. Append coverage qualifiers to the verify/decision cells, not new unmeasured results.

Exact current text:

````text
4 & Coordinator pipeline waits at c16 & Run 8 verbs timeline: queue \N{C:p16v:queue}, coalescing \N{C:p16v:coalescing}, durable wait \N{C:p16v:durable} of \N{D:p16v:mean_us}~\us\ TOTAL (\N{W:p16v|.1f} percent together). & \inference\ Reduce time between ready work and the next stage. & Trace waits with a tracing-off control at c1 and c16. Report shares of the same-case timeline, not a mixed profile. & Keep bounded queues and group commit.\\[8pt]
5 & Put stall & Run 8 overwrite/new timeline slope \N{Q:run8-stall-fits:0:slope_us_per_put|.3f}/\N{Q:run8-stall-fits:1:slope_us_per_put|.3f}~\us/put; mean spacing \N{Q:run8-stall-fits:0:spacing|.0f}/\N{Q:run8-stall-fits:1:spacing|.0f} puts; median spacing \N{Q:run8-stall-fits:0:median_spacing|.0f}/\N{Q:run8-stall-fits:1:median_spacing|.0f} puts. Run 7 sustained rates fall late. & \inference\ Bound checkpoint work that grows with cumulative puts. & Correlate checkpoint events, stall intervals and cumulative puts; repeat overwrite and new keys. & Identify phase before changing it.\\[8pt]
6 & Batch writes & Run 8 c1, 32$\times$1 KiB p50: native barrier \N{D:batch1n:p50_us}, verbs sync \N{D:batch1v:p50_us}~\us. At 32$\times$8 KiB: \N{D:batch8n:p50_us}/\N{D:ubatch8v:p50_us}~\us. & \inference\ Amortize software without changing the batch contract. & Compare same-shape native and verbs controls. Use batch owner-software and coordinator timers; add repeated controls. & Native batch profiler needed for attribution.\\[8pt]
7 & Hybrid client residual on reads & Run 8 kernel-buffered memory 1 KiB residual \N{C:mem1k:residual}~\us; kernel-direct cold residual \N{C:ssd1k:residual}~\us. & \inference\ Remove client work outside the reader leaves if linked tracing identifies it. & Add dispatch and completion timestamps to the same calls; verify the residual shrinks without moving cost outside the clock. & No storage-path rewrite yet.\\\bottomrule
````

Proposed replacement / insertion template:

````text
Rank 4: {EXISTING_RUN8_TIMELINE_ROW}; run 9 does not add the same-call c16 write timeline, so pipeline-wait attribution remains open.
Rank 5: {EXISTING_RUN7_RUN8_STALL_ROW}; short compact controls do not replace the 200,000-put checkpoint/stall investigation.
Rank 6: {EXISTING_BATCH_ROW}; run 9 has no batch-write/read grid or Native batch profiler.
Rank 7: {EXISTING_HYBRID_ROW}; run 9 excludes NVMe-oF reads and does not resolve hybrid residuals.
{UPDATED_PRIORITY_IF_FINAL_EVIDENCE_JUSTIFIES_IT}; otherwise retain the rank as a historical hypothesis.
````

### 20. Do not generalize the run-8 not-worth-work conclusion

**Source:** `main.tex:341`. **Action:** Scope the prioritization to run 8 and require the final run-9 hardware controls before extending it.

Exact current text:

````text
\textbf{Not worth work now.} SSD read device time is at the floor (run 8 verbs 1 KiB: \N{C:ssd1v:device}~\us). Verbs TLS is small in the unprofiled put controls: p50 \N{D:uput1m:p50_us}~\us\ versus \N{D:uput1v:p50_us}~\us\ for trusted verbs at 1 KiB c1 (two repetitions per population). This is descriptive, not a significance claim or isolated encryption timer.
````

Proposed replacement / insertion template:

````text
\textbf{Run-8 prioritization.} {EXISTING_RUN8_DEVICE_AND_TLS_OBSERVATIONS_UNCHANGED} Run-9 API/floor proximity is separate, descriptive evidence. It does not establish physical SSD service per call, isolated wire/encryption cost or an additive software remainder. {RUN9_CONTROL_GAPS_AND_QUALIFICATION_REFERENCE}.
````

### 21. Evidence-gaps paragraph has stale G13W and profile coverage

**Source:** `main.tex:343`. **Action:** Retain gaps explicitly belonging to runs 7/8; replace the document-wide G13W not-run sentence and annotate partial run-9 diagnostics.

Exact current text:

````text
\textbf{Evidence gaps in runs 7 and 8.} Native has no read profiler or batch-write profiler. Batch reads are absent from those runs, not from the full matrix. Unprofiled TCP/hybrid batch controls and hybrid/mTLS delete cases are absent. Profiled copies have one repetition. Distributed write-profile copies cover 1 KiB only; 64 KiB decode/group-build attribution needs two new cases. The 64 KiB sync gap, stall phase, hybrid residual and excess kernel device reads have no proven cause. No saturation or measured CPU cost supports a production claim. G13W has not run.
````

Proposed replacement / insertion template:

````text
{HISTORICAL_RUN7_RUN8_GAPS_UNCHANGED}
Run-9 G13W completion is {G13W_FINAL_STATUS}; its independent floors are in {RWC_FLOOR_APPENDIX_REF}. G14P supplies only {COMPLETED_DIAGNOSTIC_CASE_LIST}, not full c1 TCP/verbs 64-KiB profiling. Native read/batch decomposition, stall cause, five-owner scaling, NVMe-oF reads and production-capacity/CPU claims remain outside run-9 evidence.
````

### 22. Provenance pipeline and attempt identity are incomplete

**Source:** `main.tex:346–347`. **Action:** Add exact launched-package provenance and identify the excluded attempt; retain historical extractors and their evidence.

Exact current text:

````text
\section{Provenance and limits}
\textbf{Reproduction.} \path{build_run8_inputs.py} copies small CSV tables and derives the flat stall and sustained-rate extracts. It never reads the large decomposition export or summary JSON. \path{inputs.json} stores source and output SHA-256 hashes. \path{data.py} extends the original number language. \path{make_tables.py} resolves all \texttt{\textbackslash N\{key\}} values and stops on an unknown or ambiguous selection. \path{plot_final.py} makes vector PDFs. \path{evidence.json} records each point's case id, source values, run, fleet, contract and repetitions. The appendix lists every operation population of run 8, including diagnostic copies.
````

Proposed replacement / insertion template:

````text
\section{Provenance and limits}
Run 9 selection \texttt{remote\_rw\_compact}, 342 planned cases. Parent {PARENT_ID_FROM_STATE}; measurement fleet/attempt {MEASUREMENT_RUN_ID_FROM_STATE}; package SHA-256 {LAUNCHED_PACKAGE_SHA256}; source revision {PACKAGED_REVISION}, dirty {PACKAGED_DIRTY_BOOLEAN}; launch {RECEIPT_TIMESTAMP_WITH_ZONE}; coordinator VM {COORDINATOR_VM_NAME}; shard-WAL VM {OWNER_VM_NAME}. Record the exact state.json field names and receipt paths used.
Attempt \texttt{bench-f369f00616c841e4} stopped after approximately seven minutes at a controller descriptor-inspection race, subsequently fixed. No results from that attempt enter any final table, figure, ratio or repetition count.
{EXISTING_RUN7_RUN8_REPRODUCTION_PARAGRAPH_UNCHANGED}
The added analyzer/builder/plotter read only the final selected attempt's export/report. Hash the six analyzer CSVs, remote_rw.md, derived cold_qualification.csv, number definitions, generated tables and figures in inputs.json and the run-9 manifest. Preserve one case and repetition per plotted population.
````

### 23. Limits: each run no longer means five owners

**Source:** `main.tex:357`. **Action:** Restrict the existing topology sentence to historical runs; retain the old population caveats and add compact-specific limits.

Exact current text:

````text
\textbf{Limits.} Each run has one client and five owners of the same VM class and region; equal class is not hardware parity. The run-7 random dataset has 65,536 keys. It does not cover authority-history misses. Small SSD-read populations are bimodal. Their p50 can change while their means remain similar. Profiles perturb calls, and some exported leaves are derived means with only count equality, not a proved per-call join. Their additive arithmetic is bookkeeping, not an exact physical partition. Native whole-call means in the read figure aggregate two per-repetition means; they are not one profiled call population. Full case metadata stays in the copied tables.
````

Proposed replacement / insertion template:

````text
\textbf{Limits.} Runs 7 and 8 each have one client and five owners; run 9 has one coordinator and one remote shard-WAL owner, both \texttt{z4m-highmem-16}. Equal class is not hardware parity: Native uses the coordinator SSD, owner arms the owner SSD. {EXISTING_RUN7_RUN8_POPULATION_LIMITS_UNCHANGED} Run 9 is one fleet, r0/r1 main-grid cases, not two fleets; read/network floor r2 is not a third main-grid repetition. Its read-lane, security, sync/index/routing and cold/warm-geometry differences remain explicit.
````

### 24. Limits: no new infrastructure and append-floor authorization are stale

**Source:** `main.tex:359`. **Action:** Retain the no-overclaim list; replace the two now-obsolete launch statements.

Exact current text:

````text
No claim here establishes physical NAND service, isolated wire time, statistical significance, serializable transactions, permanent-loss tolerance, failover, or production capacity. No new infrastructure ran for this brief. The append floor still needs authorization. Native remains unchanged. Decisions and controls must preserve the stated durability and security contract.
````

Proposed replacement / insertion template:

````text
No claim here establishes physical NAND service per call, isolated wire time, statistical significance, serializable transactions, permanent-loss tolerance, failover or production capacity. Run-9 measurements were collected by the authorized two-VM campaign {FINAL_ATTEMPT_ID}; document generation launches no infrastructure. Append-floor outcomes are {FINAL_G13W_STATUS}, not presumed successful. Native remains unchanged; preserve durability and security contracts.
````

### 25. Separate run-9 appendix hook

**Source:** `main.tex:363–364`. **Action:** Do not splice run-9 rows into the generated run-8 appendix or change its row ids/aggregations.

Exact current text:

````text
\input{appendix-tables.tex}
\end{document}
````

Proposed replacement / insertion template:

````text
\input{appendix-tables.tex}
\input{rwc/appendix-rwc.tex} % Proposed future file: qualification and floor/status tables; not created/applied by this plan.
\end{document}
````

### 26. Fair-comparison heading: only the compact subset is launched

**Source:** `cross-store-fair-native.tex:2–3`. **Action:** Replace the blanket not-run heading. Preserve the planned five-Db/five-owner arms as unmeasured and all distinct acknowledgement contracts.

Exact current text:

````text
\subsubsection{Planned fair Native comparison (not run)}\label{sec:fair-native}
Compare equal total writer counts: \N{B:fair-native-plan:G14N:writer0|.0f}, \N{B:fair-native-plan:G14N:writer1|.0f} and \N{B:fair-native-plan:G14N:writer2|.0f} for one-Db Native and owner arms. The five-Db Native arm uses only its listed counts. Native modes are page-cache acknowledgement, per-put file/directory barrier and grouped barrier. Owner arms use TCP or verbs with synchronous covering persistence. Shared work does not make the durable mechanics equal; page-cache acknowledgement is not a per-put persistence guarantee.
````

Proposed replacement / insertion template:

````text
\subsubsection{Fair Native comparison: compact subset in run 9}\label{sec:fair-native}
The authorized run-9 selection measures the one-Db Native and one-owner TCP/verbs subset at equal total writers c1/c5/c16, with three Native acknowledgement controls. Final status: {G14N_G14O1_G14P_FINAL_CASE_STATUS}. The five-Db Native and five-owner rows below remain design inventory, not run-9 results. {EXISTING_CONTRACT_LIMITS_UNCHANGED}.
````

### 27. Fair-comparison design table must not imply all arms ran

**Source:** `cross-store-fair-native.tex:6`. **Action:** Keep the frozen design counts, identify selected versus absent groups, and link actual results rather than treating registry counts as completed cases.

Exact current text:

````text
\caption{Planned arms, not results. Counts and writers come from read-only calls to \texttt{cloud\_campaign.make\_focused\_matrix}. Instances, SSD count, network, durable mechanics and routing remain explicit comparison limits.}\label{tab:fair-native}
````

Proposed replacement / insertion template:

````text
\caption{Design inventory, not pooled results. Counts and writer grids come from the frozen matrix definitions. Run 9 includes reviewed G14N/G14O1 controls and three separate G14P diagnostics; final outcomes are {RUN9_RESULT_REFERENCE}. G14S and G14O5 were not run in this compact campaign. Instances, SSD location/count, network/security, sync mechanics and routing remain unmatched.}\label{tab:fair-native}
````

### 28. Fair-comparison authorization and selection membership

**Source:** `cross-store-fair-native.tex:9`. **Action:** Retain full frozen-selection sizes/digests; distinguish authorized remote_rw_compact from the larger selections.

Exact current text:

````text
\texttt{fair\_native} has \N{B:fair-native-plan:fair_native:cases|.0f} cases; \texttt{parity} has \N{B:fair-native-plan:parity:cases|.0f}, including \N{B:fair-native-plan:G13W:cases|.0f} G13W append floors. Frozen matrix selections are unchanged. A launch needs the owner's explicit authorization. A five-SSD Native arm is not implemented. Source: \texttt{FOCUSED\_CAMPAIGN\_2026-11-07.md}, fair-comparison section; generated counts and selection digests are in \texttt{fair-native-plan.csv}.
````

Proposed replacement / insertion template:

````text
{EXISTING_FAIR_NATIVE_AND_PARITY_DESIGN_COUNTS_UNCHANGED} The authorized \texttt{remote\_rw\_compact} selection contains 342 planned cases and reuses the reviewed one-owner/Native controls and G13W floors; it does not execute every \texttt{fair\_native} or \texttt{parity} case. Its actual completion is {FINAL_RUN9_COUNTS}. Frozen selection digests remain unchanged. A five-SSD Native arm remains unimplemented; new campaigns outside this authorization need separate approval. Source: {MATRIX_AND_FINAL_EXPORT_PROVENANCE}.
````

### 29. Five-owner reading guide is historical, not universal

**Source:** `cross-store-explanation.tex:2`. **Action:** Prefix the retained reading guide with its scope. Do not overwrite its correct historical topology.

Exact current text:

````text
\textbf{How to read these figures.} Native: one process, one SSD. Tidehunter: five owners. Other external contributors record five configured or observed storage nodes; Cassandra's 3 November run records five contacts, not a verified five-node ring. Five storage-host SSD mounts are attested only for the inspected 4 November and final deployments. FoundationDB also has a separate log/coordinator host. Equal RF1 is not equal SSD count. Native performs file and directory fsync after each put; owner covering fdatasync can serve multiple batches. Valkey waits for AOF fsync once per event-loop pass\footnote{Valkey 9.0.7, \texttt{src/server.c}: \texttt{beforeSleep} flushes AOF before pending client writes. See \url{https://github.com/valkey-io/valkey/blob/9.0.7/src/server.c} and \url{https://valkey.io/topics/persistence/}. Commands per sync were not measured in these runs.}; Kvrocks uses a synced RocksDB WAL; Garnet waits for AOF commit. Hollow write markers mean Cassandra, FoundationDB and TiKV device persistence is not verified. \runs. Hardware and configuration are not fully matched.
````

Proposed replacement / insertion template:

````text
\textbf{How to read the retained full-matrix figures.} {EXISTING_READING_GUIDE_TEXT_AFTER_HEADING_UNCHANGED} The compact one-owner run-9 controls are separate in Section~\ref{sec:rwc}; none of these historical five-owner/external points is replaced.
````

### 30. Native does not group callers only in the per-put-barrier arm

**Source:** `cross-store-explanation.tex:5`. **Action:** Keep the existing mechanism, but prevent it being read as a claim about the newly measured grouped Native arm.

Exact current text:

````text
\textbf{Topology and durable mechanics.} Native is one process on one SSD; the retained final cases of other systems use five storage nodes. Native's fork-only benchmark barrier calls \texttt{Db::insert}, then fsyncs every registered Value-WAL file and opens and fsyncs the directory after each put. It does not group scalar callers. Owner synchronous acknowledges after its covering file fdatasync; batches appended during one sync can share the next sync. A missing network hop does not make Native a latency lower bound under another contract or topology. These settings express durable intent, not proved crash or device-persistence equivalence.
````

Proposed replacement / insertion template:

````text
\textbf{Retained topology and per-put durable mechanics.} {EXISTING_PER_PUT_BARRIER_MECHANISM_UNCHANGED} The sentence "It does not group scalar callers" refers to that retained per-put barrier, not the run-9 \texttt{value\_wal\_group\_barrier} arm. {EXISTING_OWNER_MECHANICS_AND_LIMITS_UNCHANGED}.
````

### 31. Historical fleet/profile context should point to the new control, not absorb it

**Source:** `cross-store-explanation.tex:24`. **Action:** Retain all historical numbers; append only a scope link.

Exact current text:

````text
\textbf{Fleet and profile context.} Run-8 unprofiled c1 1 KiB synchronous verbs put p50/mean is \N{D:uput1v:p50_us}/\N{D:uput1v:mean_us}~\us; the full-matrix mixed point put is \N{p50:th_verbs:primary_rf1:point-mixed:put:1024|.1f}/\N{mean:th_verbs:primary_rf1:point-mixed:put:1024|.1f}~\us. They differ in fleet, workload and repetition count. This difference is not a measured regression or profiling cost. Run 8 also compares one Native SSD with five owners; its c16 64 KiB Native barrier is not an equal-device-count control.
````

Proposed replacement / insertion template:

````text
{EXISTING_RUN8_VERSUS_FULL_MATRIX_CONTEXT_UNCHANGED}
Run 9 adds a distinct one-owner workload-matched control on one new fleet. It does not retroactively equalize SSD count, workloads or profiles in these historical cases, and is not a measured before/after regression test.
````

### 32. Five old curve pages remain old data

**Source:** `cross-store-curves.tex:1–2`. **Action:** Add a single context sentence below the heading instead of editing every numerical caption or replacing the runs macro globally.

Exact current text:

````text
\clearpage
\subsection{Cross-store curves}\label{sec:curves}
````

Proposed replacement / insertion template:

````text
\clearpage
\subsection{Cross-store curves}\label{sec:curves}
These five curve figures retain the earlier full-matrix and historical external-store populations. They are not run-9 measurements; Section~\ref{sec:rwc} presents the compact matched-caller grid. Keep every original quantile, rate, cache caveat, five-owner label and \texttt{\string\runs} provenance unchanged.
````

### 33. Repeated-spike introduction must stay attached to pre-run-9 populations

**Source:** `cross-store-spike.tex:2`. **Action:** Qualify the heading; do not append compact cases to historical ranges without their own raw-sample audit.

Exact current text:

````text
\textbf{The 64 KiB points are reproducible.} The retained c16 cases reproduce the high Native latency, not a particular cause. Table~\ref{tab:spike} shows each population separately. Native per-put barrier p50 spans \N{B:spike-summary:B_minimum:value|,.1f}--\N{B:spike-summary:B_maximum:value|,.1f}~\us; Native page-cache p50 spans \N{B:spike-summary:P_minimum:value|.1f}--\N{B:spike-summary:P_maximum:value|.1f}~\us. These are observed ranges, not confidence intervals or sustained-capacity results.
````

Proposed replacement / insertion template:

````text
\textbf{The retained pre-run-9 64 KiB points are reproducible.} {EXISTING_REPRODUCIBILITY_PARAGRAPH_AFTER_HEADING_UNCHANGED} Run-9 per-repetition controls are reported separately; a different grouped-barrier or one-owner result neither changes these recorded ranges nor proves their cause.
````

### 34. Repeated-spike barrier policy is not the policy of every Native arm

**Source:** `cross-store-spike.tex:12`. **Action:** Scope the first sentence to retained per-put-barrier cases. Keep the open causal questions and source paths.

Exact current text:

````text
The barrier policy is known: one library barrier after every put, without an application-level covering group. \inference\ The serialized-service model is consistent with the earlier c1/c16 ratios, but kernel sync work can overlap. The page-cache jump from 8 to 64 KiB remains unexplained: \N{J:pagecache_jump} times the p50 for eight times the payload. Worker identity and per-put WAL offsets are absent from \path{samples.csv}; fragment-boundary clustering cannot be tested. The API clock includes engine frame encoding, CRC and WAL copying, but excludes harness value generation and oracle bookkeeping. Sources: \path{runner.rs:2655--2808}, \path{backends/native.rs:173--179,295--300}, \path{db.rs:579--594,2157--2167} and \path{crc.rs:17--29}; paths are relative to the benchmark or engine source directories.
````

Proposed replacement / insertion template:

````text
The retained Native per-put-barrier policy is one library barrier after every put, without an application-level covering group. {REST_OF_EXISTING_PARAGRAPH_UNCHANGED} Run-9 grouped Native acknowledgement is a distinct arm, not evidence that the old per-put calls shared barriers.
````

### 35. Paper comparison: distinguish retained five sizes from compact three sizes

**Source:** `cross-store-paper.tex:7`. **Action:** Scope the brief-side size description; do not change paper metadata or add paper latency claims.

Exact current text:

````text
Value sizes & Main evaluation: 64 B, 128 B and 1 KB. & Five sizes through 64 KiB. Only the overlapping sizes can supply context; workloads still differ.\\[4pt]
````

Proposed replacement / insertion template:

````text
Value sizes & Main evaluation: 64 B, 128 B and 1 KB. & Retained cross-store curves use five sizes through 64 KiB; run 9 uses 1/8/64 KiB. Only overlapping sizes supply context; workloads and contracts still differ.\\[4pt]
````

### 36. Paper comparison: new cold/warm evidence does not reproduce the paper

**Source:** `cross-store-paper.tex:15`. **Action:** Preserve the methodological conclusion; append a specific run-9 boundary.

Exact current text:

````text
The paper reports aggregate throughput, not per-call latency: the reciprocal of its aggregate rate is not an API mean or quantile. Our finite cases do not reproduce its dataset, duration, distribution or maintenance. The measurements do not reproduce the paper; they do not contradict it.
````

Proposed replacement / insertion template:

````text
{EXISTING_PAPER_NON_REPRODUCTION_PARAGRAPH_UNCHANGED} Run-9 device-qualified cold controls and prewarmed cache reads are separate workloads; neither establishes equivalence to the paper's uniform/skewed throughput phases. Do not substitute canary Native warm latency for the retained figure's quantile.
````

### 37. Existing run-8 appendix provenance remains authoritative

**Source:** `appendix-tables.tex:1–3`. **Action:** Retain this generated run-8 appendix; add no run-9 rows to it. New run-9 appendix follows in a separate file via main.tex.

Exact current text:

````text
\appendix
\clearpage\section{Durable scalar puts}
Run 8, focused unified fleet. Each row is one CSV population, not pooled calls. API statistics are in \us. With two repetitions each cell is the median of per-repetition statistics. $n$ lists successful calls in each repetition; $r$ counts repetitions. B: native barrier; P: native page cache; S: owner synchronous; N: owner page cache. T: timeline; W: write profile; R: read profile; --: no profile. Row ids map to the zero-based row of \path{run8-ops_summary.csv}.
````

Proposed replacement / insertion template:

````text
{EXISTING_APPENDIX_START_AND_RUN8_POPULATION_TEXT_UNCHANGED}
% Proposed separate run-9 appendix intro, after the full generated run-8 file:
\section{Run 9: device qualification and unavailable cases}\label{app:rwc-qualification}
One compact fleet, main-grid r0/r1 shown separately, no median or pooled quantile. All Native cold cells retain their device ratio/verdict. {QUALIFICATION_TABLE_INPUT}
````

### 38. Run-8 appendix five-owner caveats are not stale

**Source:** `appendix-tables.tex:40`. **Action:** Retain this exact statement and its duplicate at line 65, scoped by the existing Run-8 introductions. Do not change five owners to one.

Exact current text:

````text
Native barrier uses one process and one SSD against five owners. Its per-put file and directory fsync is not owner covering fdatasync. The c16 64 KiB result therefore is not a local-path latency lower bound.
````

Proposed replacement / insertion template:

````text
{EXISTING_RUN8_FIVE_OWNER_CAVEAT_UNCHANGED}
% In the NEW run-9 appendix only: Native and owner arms each have one data SSD, on different hosts. Network/security, sync scope, index/routing and read lanes are still unmatched.
````

### 39. Run-8 read appendix must not acquire run-9 qualification values

**Source:** `appendix-tables.tex:125–126`. **Action:** Preserve old row ids and median-of-per-repetition statistics; separate run-9 qualifications and repetitions.

Exact current text:

````text
\clearpage\section{Reads}
Run 8, focused unified fleet. Each row is one CSV population, not pooled calls. API statistics are in \us. With two repetitions each cell is the median of per-repetition statistics. $n$ lists successful calls in each repetition; $r$ counts repetitions. B: native barrier; P: native page cache; S: owner synchronous; N: owner page cache. T: timeline; W: write profile; R: read profile; --: no profile. Row ids map to the zero-based row of \path{run8-ops_summary.csv}.
````

Proposed replacement / insertion template:

````text
{EXISTING_RUN8_READ_APPENDIX_HEADING_AND_INTRO_UNCHANGED}
% Do not replace its existing rows with the new CSVs or describe run-9 r0/r1 as this appendix's medians. The new qualification table includes c1/c5/c16, all three sizes and all arms, with unqualified latency shown unavailable.
````

### 40. README opening, numbered sections and 40-page cap are stale

**Source:** `README.md:3`. **Action:** Replace page/section bookkeeping after final compilation. Avoid fixed section numbers where a source label/title suffices.

Exact current text:

````text
`main.pdf` is the 40-page supervisor brief. Section 8.1 presents topology, contracts, normalized rates, reproducible large-value cases, the separate laptop structure test and planned fair-comparison arms; 8.2 presents the five curves; 8.3 gives paper context; 8.4 reports Sui value sizes, not replay performance. Cross-store data are context, not a matched-fleet or matched-durability ranking. Native and Tidehunter use one full-matrix run; external stores use medians across historical GCP fleets, days and sometimes binaries. No cloud launch, dependency change or production-code change is part of this build. The authorized page limit for the laptop/fair-plan addition is 40.
````

Proposed replacement / insertion template:

````text
`main.pdf` is the {FINAL_PAGE_COUNT}-page supervisor brief (hard limit 48 pages). It retains the historical runs and adds “Two-VM remote reads and writes (run 9)” after the reads section. The compact campaign uses one coordinator and one shard-WAL VM; main-grid repetitions are r0/r1. Detailed qualification and floor/failure receipts are in the new appendix. Cross-store and Sui-size contexts are unchanged and are not run-9 comparisons or Sui replay results. Final measurements and launched-package provenance come only from the selected retrieved export.
````

### 41. README regeneration and legacy checker staging

**Source:** `README.md:9–18`. **Action:** Add the new analysis/build/plot/check sequence; preserve historical extractors and clearly scope the old hard-coded verifier.

Exact current text:

````text
```sh
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python -B build_run8_inputs.py
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python -B make_tables.py
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python -B plot_final.py
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python -B verify_brief.py
/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python -B verify_curves.py
```

The input builder reads run 8 `focused-launch-20261109-unified2/analysis/` and corrected run 7 `focused-launch-20261109-g10b/analysis-final-v2/output/`. It keeps the original run-7 report as historical prose, not the source of late rates. It calls `build_coherence_inputs.py` for curve contributors, topology and paper facts, then `build_size_spike_inputs.py` for the corrected remote Sui trace, retained raw put samples and protocol size limits. `inputs.json` records source and output SHA-256 hashes. None of those sources is changed.
````

Proposed replacement / insertion template:

````text
Historical inputs and plots: `build_run8_inputs.py`, `make_tables.py`, `plot_final.py` as before. Run historical 40-page acceptance before integrating run 9, and keep that record; `verify_brief.py` has hard-coded page/section/figure counts and is not the integrated 48-page checker.
From the retrieved, final selected attempt:
`remote_rw.py --export {EXPORT} --report {REPORT} --output rwc/run9-output`
`rwc/build_rwc_inputs.py --output rwc/run9-output --provenance rwc/run9-provenance.json --register`
`rwc/plot_rwc.py --output rwc/run9-output`
`latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex`
`rwc/verify_rwc.py --pdf`
Keep registration after any old builder that rewrites inputs.json. Integrate the updated document patch once only; use the revised appendix hooks and not merely the earlier main-only draft patch. Keep every new RWC number in generated nested sources/wrappers: the frozen root scanner does not understand raw new RWC keys in root main.tex. Exact commands and render checks: `rwc/RUNBOOK.md` (to be updated for the final body/appendix layout).
````

### 42. README file/evidence inventory is incomplete

**Source:** `README.md:26–43`. **Action:** Retain historical inventory rows and append the new run-9 sources/evidence and real PDF/render outputs.

Exact current text:

````text
| `main.tex`, `cross-store-explanation.tex`, `cross-store-curves.tex`, `cross-store-paper.tex` | Brief, topology/contracts and Native/Valkey explanation, five curve pages, and paper context. |
| `build_run8_inputs.py`, `build_coherence_inputs.py` | Copy focused data and extract same-run curve metrics, topology, storage-only counter spreads and paper facts. |
| `populations.csv`, `previous-populations.csv`, `device-reads.csv`, `external-spread.csv`, `write-timeline-spans.csv`, `readmany-trace.csv`, `stage-evidence.csv` | Unchanged carried-forward inputs. |
| `run8-*.csv`, `owner-stages.csv`, `owner-profile.csv`, `run-summary.csv` | Focused operation, component, floor, stall and owner-clock evidence. |
| `run7-*.csv` | Focused device and put evidence; corrected sustained summaries and extracts. |
| `curve-runs.csv` | Each run's API quantiles, mean, finite rate and mean in-flight calls, with case id, source and occupancy method. |
| `storage-topology.csv`, `historical-topology.csv`, `per-node-64k.csv` | Retained-final counts, all 675 external contributor node-count records, and normalized per-node context. Even spread assumed; not observed node-level metrics. |
| `device-write-spread.csv` | Only storage hosts 1–5; coordinator host 0 excluded. Gate-window counters, not call-linked rates. |
| `chart-anomalies.csv` | Garnet 64 B successful measurement puts with API time at least 10 ms; raw sample source, count and min/max stall times. |
| `paper-context.csv` | Throughput values extracted from actual parsed-paper text, with the matched source text. |
| `cross-store-spike.tex`, `cross-store-sizes.tex` | Repeated large-value cases and separate Sui size context; neither adds replay performance results. |
| `build_size_spike_inputs.py`, `sui-value-sizes.csv`, `size-summary.csv`, `spike-cases.csv`, `spike-summary.csv`, `size-context.csv` | Independent raw trace/sample extraction, per-population quantiles and source-linked protocol/paper configuration values. |
| `verify_size_spike.py` | Independent raw trace and raw sample checks, called by `verify_brief.py`; no generator imports. |
| `data.py`, `make_tables.py`, `numbers.tex`, `macros.tex`, `tab-*.tex`, `appendix-tables.tex` | Number language and generated tables. |
| `number-baseline.json` | Pre-audit numeric definitions. All existing keys are retained; only the twelve corrected late-rate values change. |
| `plot_final.py`, `plot_curves.py`, `figures/*.pdf`, `evidence.json` | Vector figures and every plotted point's values and provenance; external curve points retain all contributors. |
| `verify_brief.py`, `verify_curves.py`, `validation.json`, `main.txt`, `rendered/` | Independent checks, accepted PDF text, and changed-page renders at 110 dpi. |
| `main.pdf`, `main.log`, `build-output.txt` | Compiled brief and compiler evidence. |
````

Proposed replacement / insertion template:

````text
{EXISTING_FILE_INVENTORY_ROWS_UNCHANGED}
Add rows for: `rwc/section-rwc.tex`, proposed `rwc/appendix-rwc.tex`, `rwc/build_rwc_inputs.py`, `rwc/plot_rwc.py`, `rwc/verify_rwc.py`, `rwc/data/run9-output/*`, generated number/findings/provenance/hooks, six `tab-rwc-*.tex` tables, `figures/rwc-*.pdf`, `inputs-rwc.json`, `figure-evidence.json`, `validation-rwc.json`, run-9 launch receipts/provenance, final compiler log and rendered run-9 pages. Keep SYNTHETIC fixtures/evidence labelled and excluded from the real results; they are pipeline acceptance, not campaign data.
````

### 43. README verification scope must distinguish historical and integrated checks

**Source:** `README.md:45–47`. **Action:** Retain the historical checker details, but do not claim its hard-coded checks validate the new section.

Exact current text:

````text
`verify_curves.py` imports no data or plotting module. It checks all 1,235 curve values, all 810 per-run source populations, all 108 per-node numeric cells, explanation number keys, and storage-only counter spreads. External occupancy is computed inside each run before taking the median. The checker never multiplies separately sampled medians. It also checks paper source matches.

`verify_brief.py` checks source/output hashes, existing PDF tables, owner-clock extracts, corrected complete buckets, network half-round-trip conversion, all retained number definitions and unchanged older plotted values. It checks the final compiler log and printed figure fonts, then calls the independent curve checker. Only the twelve `Q:run7-sustained-extract:<0..11>:late_puts_s|.0f` keys may change. `validation.json` lists each old/new value and all added keys. Visual inspection is a separate human check, tied to the PDF hash; scripts do not claim to inspect pixels.
````

Proposed replacement / insertion template:

````text
{EXISTING_HISTORICAL_CHECKER_DESCRIPTION_UNCHANGED}
Run-9 independent verification recomputes the new number definitions and table cells from the six CSV families plus the source-checked local cold_qualification.csv, verifies hashes and per-marker case/repetition provenance, and checks the final PDF/log/page budget. Preserve the old 14-figure historical evidence separately from the six new figures; the integrated document has both, not a newly rewritten historical benchmark. Visual checks are tied to the final PDF SHA-256.
````

### 44. README fair-plan authorization sentence is incomplete

**Source:** `README.md:55`. **Action:** Keep frozen matrix/design provenance and explain the launched subset versus remaining unrun designs.

Exact current text:

````text
`fair-native-plan.csv` records group counts, writer counts and selection hashes from read-only `cloud_campaign.make_focused_matrix` calls. It also records the existing frozen selections. `cross-store-fair-native.tex` states the unmatched dimensions and explicit launch-authorization requirement. Matrix generation runs no benchmark and launches no infrastructure; no matrix source or frozen case is edited. `verify_size_spike.py` independently regenerates these counts and digests.
````

Proposed replacement / insertion template:

````text
`fair-native-plan.csv` remains the frozen design inventory and digest record. The authorized `remote_rw_compact` run 9 executes only its reviewed one-owner/Native subset, G13W floors, read controls and diagnostics; actual results/statuses come from the final export. Five-Db/five-owner design rows are not thereby run. Matrix generation itself launches nothing. `cross-store-fair-native.tex` links completed compact evidence while preserving unmatched dimensions and approval requirements for further campaigns.
````

### 45. README generic Native and five-owner contract summary needs arm/run scope

**Source:** `README.md:74–76`. **Action:** Retain historical mechanics; add grouped Native and compact topology without globally replacing the five-owner label.

Exact current text:

````text
Native is one process on one SSD, with its own per-put file and directory fsync in the barrier mode. Five-owner Tidehunter synchronous uses covering fdatasync; Valkey AOF-always can group clients in one event-loop pass. Kvrocks has synced WAL writes; Garnet has commit-before-reply settings. Cassandra, TiKV and FoundationDB device persistence is not verified. Similar durable intent is not proof of equal crash recovery or power-loss survival.

Reads use different cache paths: Valkey RAM GET; Native's resident-oriented workload; owner memory or buffered misses; kernel buffered or direct target; SPDK direct target. Native reads recent mapped fragments and older fragments by positional reads. Native 8 KiB requested-cold cases pass the device-count gate but have mainly memory-like latency; 64 KiB meets case-level qualification, not per-call NAND proof.
````

Proposed replacement / insertion template:

````text
Retained Native per-put barriers use one process/SSD and all-file/directory fsync without sharing. Run 9 separately includes Native page-cache and grouped-barrier controls and one-owner TCP/verbs covering fdatasync. The five-owner label still applies to the historical owner runs, not to run 9. Native uses the coordinator SSD, owner arms the remote owner SSD. Run-9 Native cold cells qualify only by their own receipts and device-read ratios; list {FINAL_NATIVE_UNQUALIFIED_CELLS}. Warm reads are prewarmed serving-node page cache, coordinator cache off. Native recent mapped/older positional paths and owner exact-range reads remain distinct; no per-call NAND proof follows.
````

### 46. README limits must not imply the new controls prove old causes

**Source:** `README.md:80`. **Action:** Preserve unexplained historical features; add one-fleet/no-decomposition limits.

Exact current text:

````text
Native's 64 KiB barrier fits a serialized-service model [INFERENCE], not proof of kernel serialization or queue depth one. API calls overlap. Its page-cache 64 KiB discontinuity remains unexplained. Valkey and owner actual commands per sync are not measured. Run-8 wait/software shares suggest candidates, not a measured explanation of a separately sampled cross-store gap. No fleet, repetition or outcome samples are pooled.
````

Proposed replacement / insertion template:

````text
{EXISTING_HISTORICAL_UNEXPLAINED_FEATURES_PARAGRAPH_UNCHANGED} Run-9 grouped controls and client/owner floors test additional hypotheses on one new fleet; they do not prove the earlier 64-KiB jump, quantify isolated network/software cost, establish fleet spread or close checkpoint-stall/profile gaps. Do not pool attempts, repetitions, outcomes or temperatures.
````

### 47. README document-build cloud boundary

**Source:** `README.md:96`. **Action:** Replace an ambiguous all-work-is-local reading with the distinction between the authorized data collection and offline document generation.

Exact current text:

````text
No new experiment, repository suite, smoke test or cloud operation is required by this document build. Native production code and other agents' files remain unchanged.
````

Proposed replacement / insertion template:

````text
Document generation and verification launch no infrastructure and run no production benchmark. Run-9 measurements come from the separately authorized two-VM campaign {FINAL_ATTEMPT_ID}; the aborted first attempt contributes no results. This document update does not change Native production code or other agents' implementation files. Final claims use the retrieved export, not preliminary canaries or synthetic fixtures.
````

### Reviewed text that must remain historical, not silently rewritten

| Source | Disposition |
|---|---|
| `main.tex:23–25` | Keep `\runs`, `\rEight`, `\rSeven` attached to their existing fleets. The two **six-VM** macros describe runs 7/8 and stay six VMs. Add a separate run-9 topology description/macro; do not globally replace six/five with two/one. |
| `main.tex:47–54, 89–184, 187–209, 224–260` | Preserve every existing run-7/run-8 latency, stage, floor and qualification value. Scope links above do not import run-9 values into the old columns. In particular, run-8 Native 1/8/64-KiB observations and run-7 “all qualify” are not run-9 qualification results. |
| `main.tex:142, 282–312` | Existing batch/delete and retained cross-store statements stay scoped to those runs. Do not claim run 9 added batch, hybrid or external-store measurements. Retain the legacy cache-hit estimate as labelled historical context, not a run-9 measured curve. |
| `cross-store-explanation.tex:8–22, 26` | Keep historical topology-normalized tables, Native page-cache jump, Valkey comparisons, profile hypotheses and unexplained features. “Owner page-cache write curves were not run on this fleet” remains true: run 9's owner writes are synchronous, not owner page-cache writes. Do not convert independent populations into before/after effects. |
| `cross-store-curves.tex:4–31` | Preserve all five figures/captions and numeric series; the one new scope sentence suffices. No rerun of Valkey/external stores happened in the compact campaign. Native resident-oriented mixed-stream quantiles are not the compact read partners. |
| `cross-store-spike.tex:5–18` | Preserve raw-sample-audited historical ranges and the laptop experiment. A local covered-ticket model is not the cloud engine's grouped barrier. Do not label the laptop measurements as run 9 or append compact controls to its ranges. |
| `cross-store-paper.tex:6, 8–13, 17` | Preserve paper metadata, paper claims, source paths and methodological limits. A new grouped-barrier measurement is not upstream paper validation, and 1-KiB cache-read canaries are not sustained paper throughput. Only the inventory-size scope and final non-reproduction pointer need additions. |
| `cross-store-sizes.tex:1–27` | **No scientific-result change.** All Sui trace distributions, protocol limits, exact-size counts and no-mainnet/no-cache-qualification caveats remain unchanged. Run 9 is synthetic KV calls, not replay, a new size capture or a Sui cache-hit measurement. This file has been reviewed, not omitted. |
| `appendix-tables.tex:3, 42, 67, 91, 126` | Preserve the run-8 “median of per-repetition statistics” descriptions for their old populations. The new run-9 appendix uses r0/r1 separately; do not rewrite the old generated appendix's aggregation or 128 operation rows to imitate the new convention. |
| `README.md:20, 49–53, 57–70, 78, 82, 84–94` | Keep copied-input provenance, raw-source limitations, historical curve changes, complete-bucket correction, paper limits and old correction ledger. Add a distinct run-9 evidence subsection, not another correction to unchanged older numbers. |

The generated `appendix-tables.tex` is produced by the existing builder. Prefer keeping it unchanged and adding the separate run-9 appendix; a manual modification to it would be overwritten. Any later change to a tracked builder requires Main's explicit scope/authorization, not this plan.

## 2. Figure/table placement and page budget

### Proposed final allocation: six main-body pages plus two new appendix pages

The earlier synthetic prototype compiled at **48 pages** with eight added main-body pages. The following rearrangement is a **proposal**, not an already-built result: retain all six figures and all six new tables, move only the detailed qualification/floor receipts into the appendix, and reserve the same eight-page increment. If the earlier sections retain their page sizes, the new body occupies pages 10–15, the old material shifts by six pages, and the two new appendix pages are 47–48. Actual page numbers are placeholders until the final real-data build; use labels/cross-references in prose.

| Slot | Main body / appendix | Required contents and inputs |
|---|---|---|
| Body page 1 | After the **entire** `sec:reads` section, before historical Hardware parity | Two-VM topology, contracts including read lanes, qualification rule and one-fleet limit; `figures/rwc-parity.pdf`; `rwc/generated/tab-rwc-parity.tex`. First two heat-table columns count pairs, last three **distinct cases**, not affected pairs. Main-grid failures/qualification are separate from floor failures. |
| Body page 2 | Matched writes | `figures/rwc-write-latency.pdf`; `tab-rwc-write.tex`. Panels have equal writers c1/c16, all sizes; separate r0/r1 p50 and p99. Page-cache acknowledgement is not durable; per-put/grouped/covering barriers remain distinct. |
| Body page 3 | Matched cold/warm read curves | `figures/rwc-read-latency.pdf`; `tab-rwc-read-cold.tex`. c1/c16 panels, exact Native unqualified cells left unavailable rather than plotted at their memory-speed latency. Point to the full c1/c5/c16 qualification appendix. |
| Body page 4 | Warm reads and descriptive cold/warm contrast | `tab-rwc-read-warm.tex`; `figures/rwc-cold-warm.pdf`. One-reader means only in the contrast; cold/warm dataset geometry differs. Native warm is not a coordinator-cache hit. |
| Body page 5 | Finite-case rates | `figures/rwc-throughput.pdf`, 1/64 KiB, c1/c5/c16; separate write/cold/warm panels. Keep the explicit **Native reads** legend handle, not the write-only per-put-barrier label. Preserve per-case occupancy, offered/unsuccessful calls and n in evidence. |
| Body page 6 | Independent floors and interpretation | `figures/rwc-floors.pdf`; same-kind-of-call discussion and limits; short failure/qualification callouts with appendix links. Show client/owner SSD floors and network controls **beside** owner whole-call means, no stacked decomposition or subtraction. Record failed event-write floors as unavailable, not zero. |
| New appendix page 1 | After the whole old generated appendix; proposed `rwc/appendix-rwc.tex`, `app:rwc-qualification` | Full `tab-rwc-cold-qualification.tex`: all sizes, arms, c1/c5/c16, r0/r1, ratio and status. State the exact final Native unqualified cell list and retain reason codes/case IDs in linked CSV/evidence. No hidden pooling, no replacing old run-8 read rows. |
| New appendix page 2 | `app:rwc-floors` | `tab-rwc-floors.tex` plus a compact failure-receipt table/list for the final failed G11N cases, deduplicated by **case ID** (the six-CSV network table repeats each case for multiple sizes). Include tool, mode, repetition, status, exact error and receipt path. Retain all other modes in the complete floors CSV. Add launched-package/VM provenance and the explicit excluded-first-attempt statement here if the main provenance paragraph would overflow. |

A separate failure-receipt list is required because the current representative floor table selects blocking TCP and polled send controls; it does **not** itself display the failing `ib_write_lat` event-mode controls. The current fixture's deliberately failed polled-send example is a rendering test, not the actual campaign failure pattern. Do not rename that synthetic example into a measured receipt.

Use the six real figure names above only after building from real inputs without `--synthetic`. Keep every synthetic figure's filename and watermark; do not just rename a SYNTHETIC PDF. Preserve readable printed fonts (at least the brief's 7-pt floor), units, per-panel contracts and distinct repetitions. No new longtable package or production-code change is needed for the proposal.

### If real values or the extra prose exceed 48 pages

Do not reduce the plot font, hide unqualified cases or discard old measurements. Proposed first compression targets, for Main's approval only:

1. `main.tex:211–221`: replace the now-duplicated prospective append-floor decision table with a short design/history paragraph and a reference to the new, fully populated floor appendix. This removes obsolete prospective wording, not measurements.
2. `cross-store-fair-native.tex:2–9`: condense the old planned-design explanation into a selected/unmeasured status inventory and link the new results. Retain the unmeasured G14S/G14O5 rows and all remaining contract differences.
3. `main.tex:346–359` and the run-9 appendix: use one authoritative provenance block plus a short main-body pointer instead of printing a long package hash/VM inventory twice. Keep both attempt identities and all receipt links.
4. Only if needed and separately approved, compress duplicated historical cross-store narrative or the existing Sui size-context layout, while keeping its measured tables, source links and no-replay caveats. Do not silently remove the old appendix or external populations.

Final acceptance still requires a real-data compile and rendered-page inspection at <=48 pages. This planning-only task performs no rebuild and asserts no new final page count.

## 3. Required caveat/provenance checklist and wording templates

### Fleet, matching, lanes and persistence

- **One final fleet, not fleet spread.** Main-grid r0 and r1 are independent cases on the same fleet, not independent fleet samples, confidence intervals or proof of significance. Never pool repetitions or import a result from the aborted attempt.
- **Repetition nuance:** main write/cold/warm grids use r0/r1. G11N network and G11S/G15F SSD-read floors use r0/r1/r2. G13W append floors use r0/r1, not r2. G14P has three separate once-only diagnostics. Thus “floors have three repetitions” is too broad; state the family-specific counts.
- **Same readers/writers is not identical internals.** Read-only matrix inspection confirmed Native `read_lanes=0`; both owner transports use `read_lanes=1` at c1/c5 and `read_lanes=4` at c16, in both G15C/G15M. c5 means five readers, not five lanes. List lane geometry among unmatched dimensions in the section and every generated comparison caption (`rwc_data.py`'s `UNMATCHED` text must be extended at implementation time).
- **Different SSDs/paths remain.** Native is in process on the coordinator SSD, while owner arms use the owner SSD plus coordinator/index and remote RPC/history/routing. Floors on both hosts characterize that hardware rather than making the hosts identical. No “same SSD” or automatically equal hardware claim follows from the same VM class.
- **Durability/security remain distinct.** Native page-cache acknowledgement is not durable; per-put file/directory fsync, grouped Native barrier and owner covering one-file fdatasync are different mechanisms. TCP mTLS versus trusted verbs is not transport-only. RF1 is not failover or permanent-loss tolerance.
- **Cold versus warm have different geometry.** Match Native/owner arms within a temperature; do not claim the cold and warm datasets/operation counts are identical. Prewarm requests a serving-node cache regime, not per-call proof that every read hit RAM. Coordinator cache remains off; retain device evidence when available rather than imputing zero.

### Cold qualification and exact unavailable cells

Final wording template:

> Cold eligibility requires the receipt verdict `qualified` **and** device reads per successful API call >=0.9 for that case. A cache drop, `first_touch` tier or low API latency is not device-service proof. Final unqualified Native cells are `{SIZE, READERS, REPETITION, CASE_ID, RATIO, REASON}`; all remaining Native cold cells have their own explicit verdict. Their latency is retained as an unavailable cold result, not ranked as a faster SSD read. The ratio is a case/window-level proxy, not evidence that every call reached NAND.

Main's preliminary canary information establishes only that observed **Native cold 1 KiB c1** canaries are unqualified (reported ratio about 0.28, p50 about 2 µs). The canary message does not supply a complete repetition-specific case list, and proves nothing about all c5/c16, 8-KiB or 64-KiB Native cells. Fill the exact final set from the export; do not pre-label every Native 1-KiB cell or invent qualification for unobserved cells. Native recent-fragment mapped reads and older positional reads remain code-path facts. Readahead as an explanation of the memory-like/cold-count mismatch must be qualified **[INFERENCE]** unless final call-linked evidence identifies it.

The analyzer's six flat CSVs omit the device ratios/reasons; the brief's source-checked `cold_qualification.csv` indexes the corresponding `remote_rw.md` table. Preserve that source connection and raw receipt links. Do not manufacture ratios from latency or treat absent counters as zero.

### Failed network controls and missing evidence

Final wording template:

> The final export contains `{FINAL_FAILED_CASE_LIST}`. The observed G11N `ib_write_lat` event-mode controls for r0/r1/r2 failed with `network floor server exited before listener readiness`; confirm all three exact case IDs and receipts in the final selected attempt. They have no valid latency statistic. This is a failed tool/mode control, not zero network latency or proof that KV writes/verbs failed. Other successful tool/mode controls remain distinct populations; do not substitute them as though the failed tool completed.

Main's snapshot reports three such floor failures and otherwise completed execution through case 137. Do not turn that snapshot into “339/342 completed,” “only three failures in the campaign,” or a terminal success claim. Preserve later failures/not-run cases if they appear. Do not copy run 8's `EINVAL` direct-write failures into run 9 merely because the old brief contains them. A diagnostic/process failure, a controller abort and an analyzed cold-qualification failure are separate status classes.

The compact run excludes five-owner scale-out, NVMe-oF/hybrid reads, Spot and other machine types. It supplies no new batch grid, Sui replay, full fleet-to-fleet spread, linked read decomposition, production saturation/CPU evidence or causal checkpoint-stall explanation. Standalone floors and successful API means are separate populations; the **[INFERENCE]** discussion names hypotheses only and computes no additive network/SSD/software residual.

### Aborted first attempt and final attempt identity

Mandatory final provenance template (fill from retrieved `state.json` and launch/package receipts, not the current checkout):

```text
Selection: remote_rw_compact; planned cases: 342.
Final parent: {RUN9_PARENT_ID_FROM_STATE}.
Final measurement fleet/attempt: {RUN9_MEASUREMENT_ID_FROM_STATE}.
State source and exact JSON field names: {STATE_PATH_AND_FIELDS}.
Package SHA-256: {LAUNCHED_PACKAGE_SHA256}.
Packaged revision: {PACKAGED_REVISION}; dirty: {PACKAGED_DIRTY_BOOLEAN}.
Launch timestamp (timezone included): {LAUNCH_TIMESTAMP}.
Coordinator VM: {COORDINATOR_VM_NAME}; shard-WAL VM: {OWNER_VM_NAME}.
Final export/report and hashes: {FINAL_EXPORT_AND_ANALYSIS_PROVENANCE}.
Excluded first attempt: bench-f369f00616c841e4, aborted after approximately
seven minutes at a controller descriptor-inspection race, subsequently fixed.
No results from that attempt are used in any final figure/table/ratio/repetition.
```

Main identifies the running second attempt's expected **parent** as `bench-bb40628be5664257` and expected **fleet** as `bench-41ecdf3f69fb47ec`. They are not interchangeable run IDs. Leave the two placeholders in this plan until checking the final `state.json` field meanings; record both rather than guessing which should populate the analysis `run_id`. If final state disagrees, resolve that discrepancy before using the export. The fixed controller can change the packaged source/hash/dirty state between attempts; never reuse first-attempt provenance or report the current local HEAD as the launched revision without its receipt.

## 4. Preliminary canaries: quarantined planning observations, not final results

The following observations are supplied by Main, not independently recomputed here, and are intentionally **not** proposed final number macros:

- Owner cold 1 KiB c1: TCP API mean about 108 µs, verbs about 99 µs; reported device-read ratio 0.948, qualified.
- Native requested-cold 1 KiB c1: unqualified at about 0.28 device reads/call, p50 about 2 µs. This is not a Native SSD latency result.
- Owner verbs warm 1 KiB c1 mean about 28 µs; Native warm about 0.9 µs.
- Three G11N `ib_write_lat`, event-mode controls failed with the listener-readiness error above; the campaign snapshot is incomplete.

These canaries motivate the final discussion structure; they do not identify per-repetition final values, give an isolated wire/software difference, license a Native-vs-owner SSD ranking or establish two-fleet reproducibility. The first attempt's measurements remain excluded even where they resemble the second attempt. Final plots/tables/text are generated only from the selected final export.

## 5. Owner terminology and handoff boundary

The owner calls this supervisor brief the “coordinator report.” Treat that only as task-routing vocabulary: keep the existing directory, `main.tex`, `main.pdf`, title and supervisor-brief naming. Add **no alias explanation or terminology note to the document itself**. There is no separate coordinator-report source to create or rename.

The planning-only handoff described here has since been completed under Main's explicit integration authorization. Real export/provenance, generated results, the six-body/two-appendix layout, independent checks and rendered-page review are recorded in `rwc/REAL-ACCEPTANCE.json`. No campaign result remains pending merely because an earlier template in this archival ledger uses a placeholder. See `rwc/RUNBOOK.md` for current commands and limitations; no first-attempt or synthetic result enters the final PDF.
