# Supervisor brief, 10 November 2026

`main.pdf` is the supervisor brief, including measured runs 9 and 10 (hard limit: 56 pages). Run 9 follows the run-8 reads; run 10 adds matched fabric/security/polling controls after run 9. Each compact fleet has one coordinator and one remote shard-WAL VM, both `z4m-highmem-16`; r0/r1 remain separate. Earlier fleets and Sui value sizes remain separate evidence, not replay performance. Current page count and PDF hash are in `rwc10/generated/validation-rwc10.json`, `rwc10/REAL-ACCEPTANCE10.json` and root `validation.json`; `rwc/REAL-ACCEPTANCE.json` preserves historical run-9 acceptance.

## Regenerate

From this directory:

```sh
PY=/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python
RUN=/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/focused-launch-20261110-rwcompact2
RUN10=/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/focused-launch-20261110-fabricab2
$PY -B build_run8_inputs.py
$PY -B make_tables.py
$PY -B plot_final.py
$PY -B rwc/build_rwc_inputs.py --output "$RUN/analysis2" \
  --export "$RUN/export" --provenance rwc/run9-provenance.json --register
$PY -B rwc/plot_rwc.py --output "$RUN/analysis2"
$PY -B ../../../experiments/kv-comparison/fabric_ab.py \
  --export "$RUN10/export" --report "$RUN10/export" --output rwc10/run10-output
$PY -B rwc10/build_rwc10_inputs.py --output rwc10/run10-output \
  --provenance rwc10/run10-provenance.json --export "$RUN10/export" --report "$RUN10/export" --register
$PY -B rwc10/plot_rwc10.py --output rwc10/run10-output
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
$PY -B rwc/verify_rwc.py --pdf
$PY -B rwc10/verify_rwc10.py --pdf
$PY -B verify_brief.py
$PY -B verify_curves.py
```

The input builder reads run 8 `focused-launch-20261109-unified2/analysis/` and corrected run 7 `focused-launch-20261109-g10b/analysis-final-v2/output/`. It keeps the original run-7 report as historical prose, not the source of late rates. It calls `build_coherence_inputs.py` for curve contributors, topology and paper facts, then `build_size_spike_inputs.py` for the corrected remote Sui trace, retained raw put samples and protocol size limits. `inputs.json` records source and output SHA-256 hashes. None of those sources is changed.

Run-9 inputs are the final second attempt's six CSVs and `remote_rw.md`; `rwc/run9-provenance.json` records the actual launched package, not the current checkout. The export contains all 342 terminal cases: 339 controller-completed and three failed event-mode `ib_write_lat` floors. Six completed Native cold 1-KiB cases are analytically unqualified, a different status from controller failure. The top-level report retains `complete=false` because of the floor failures. No result from the aborted first attempt is used.

`main.tex.pre-run9`, `rwc/validation-pre-run9.json` and `rwc/main-pre-run9.pdf` preserve historical acceptance. The integration patch was applied once after authorization; do not reapply it on rebuild. Synthetic fixtures remain under `rwc/`, excluded from the final manifest/PDF inputs. See `rwc/RUNBOOK.md` for exact commands and receipt identities.

Run 10 uses only the selected second `fabric_ab_compact` attempt: all 80 controller cases completed, all 40 cold cases qualified, controller exit 0 and cleanup verified. Final receipts, hashes, exact coordinator-creation timestamp, separately attributed monitor observations and list-price cost estimates are in `rwc10/run10-provenance.json`. The first attempt's nine completed cases are excluded from every final result. `main.tex.pre-run10` backs up the pre-integration source. The three run-10 patches were dry-run and applied once after Main's GO; do not regenerate/reapply them on normal rebuild. Register run 10 last because historical builders rewrite `inputs.json`. See `rwc10/RUNBOOK10.md`.

The seven versioned full-matrix CSV inputs are copied byte for byte from the 7 November brief. Do not run the old `build_inputs.py`: it needs the unavailable large export. Once built, the copied CSVs suffice for the number and plotting stages. Independent acceptance also reads retained originals to check curve contributors and topology; those raw-source checks need the original reports.

## Files and evidence

| Files | Use |
|---|---|
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
| `rwc/section-rwc.tex`, `rwc/appendix-rwc.tex` | Run-9 body and qualification/floor/provenance appendix. |
| `rwc/build_rwc_inputs.py`, `rwc/plot_rwc.py`, `rwc/rwc_data.py`, `rwc/verify_rwc.py` | Source-traced generation and independent acceptance. |
| `rwc/data/run9-output/*`, `rwc/generated/*`, `figures/rwc-*.pdf` | Real CSVs/report, source-checked ratio/case-status CSVs, six tables, number/finding macros, failure/provenance text, manifests, per-marker evidence and six new vector figures. |
| `rwc10/section-rwc10.tex`, `rwc10/{build_rwc10_inputs,plot_rwc10,verify_rwc10}.py` | Run-10 section, source-traced findings/tables/figures and independent verification. |
| `rwc10/run10-output/`, `rwc10/data/`, `rwc10/generated/`, `figures/rwc10-*.pdf` | Fresh analysis of the selected real export, copied CSVs/report, controller statuses, six tables, 390 number macros, five vector figures and per-value evidence. |

`verify_curves.py` imports no data or plotting module. It checks all 1,235 curve values, all 810 per-run source populations, all 108 per-node numeric cells, explanation number keys, and storage-only counter spreads. External occupancy is computed inside each run before taking the median. The checker never multiplies separately sampled medians. It also checks paper source matches.

`verify_brief.py` retains historical source/output hashes, PDF numeric claims, raw owner extracts, complete-bucket checks, network conversions and baseline numbers, then invokes curve and size/spike checks. The integrated cap is 56 pages; all 14 historical, six run-9 and five run-10 figures retain font checks. The two compact-campaign verifiers independently check their CSV-derived numbers, matched cells, statuses and per-value figure evidence; run 10 also checks independently selected namespace cohorts, range reductions, payload-rate conversions and same-window occupancy identities. Visual checks remain separate, tied to the final PDF hash.

The size extractor follows `experiments/sui-checkpoint/trace_summary.py` observation rules without importing it. It keeps found payload reads and successful put members separate, includes scan rows, and excludes duplicate scalar-write batch records. Missing/exists/delete operations supply no observed value length. It uses one corrected remote capture, including initialization, not mainnet. No cache or durability qualification follows. The coarse typical-size range rounds the six displayed payload tables' nonempty medians to the nearest decade; the full-content range rounds its read p50/p90 outwards to KiB. Exact lengths and threshold membership remain separate questions.

The spike extractor recomputes every displayed population from successful measurement put samples, including local N3 and queue controller archives. It keeps profiles, repetitions and manual exports separate; it does not treat copied manual baselines as independent fleets. The checker independently reads the raw trace and samples again, checks every CSV statistic and every added numeric key, and records its results under `validation.json.size_spike`. Figure 1 size labels were visually checked in the parsed paper's embedded image at line 23; other paper numbers and protocol limits have source line references in `size-context.csv`. Full regeneration and independent checks require these retained raw sources; number/table/plot generation can use the copied CSVs.

The laptop addition uses only the later IEEE-CRC pass in `local://report-spike.md`. `local-structure-test.csv` retains each run separately, its original report row, report line and run label. Raw scratch samples were deleted. The pipeline hashes the transcription and the report when available; verification checks the transcription, not nonexistent raw samples. It never combines the earlier CRC32C pass with these results. `tab-local-structure.tex` shows run 1; run 2 within each cell, with no pooling.

`fair-native-plan.csv` remains the frozen design-count/digest inventory. Run 9 measures only the authorized `remote_rw_compact` subset: one-Db Native, one-owner controls, G13W floors, reads and diagnostics. G14S/five-Db and G14O5/five-owner rows did not run. A five-SSD Native arm remains unimplemented. `cross-store-fair-native.tex` distinguishes the measured subset from remaining design; matrix generation and verification launch nothing.

## Figure changes

- `curves-write-c16.pdf`: Native is labelled one process/one SSD. A grey dotted page-cache-ack curve is added to the three latency panels, not the finite-rate panel. Cassandra, FoundationDB and TiKV use hollow markers. The fourth panel keeps finite-case successes/s.
- `curves-read-c16.pdf`: the fourth panel shows mean in-flight API calls out of the configured concurrency, computed per run and then medianed. Low occupancy makes the load-generator limit visible.
- `curves-read32-c16.pdf`: the same occupancy change counts API calls, not keys. All original quantiles remain.
- `curves-point-read-c1.pdf`: the fourth panel now shows read API mean, not the duplicated mixed-case rate.
- `curves-point-put-c1.pdf`: the fourth panel now shows put API mean. The grey page-cache curve and hollow unverified-persistence markers are added.
- All five: corrected per-series provenance, topology, calls per point/run, finite-exposure note, cache caveat and K/k/M notation. Write pages include the acknowledgement key. The twelve original series keep their order, markers and dash patterns. No missing owner page-cache write curves are invented. All labels remain at least 7 pt when printed.
- `scoreboard.pdf`, `others.pdf`, `coherence.pdf`: plotted values are unchanged. Captions and evidence metadata now distinguish Native/TH provenance from historical external medians. Valkey transport pairs are ratios of separate medians; pair-level same-fleet provenance is not established.
- Focused components, sync, stall and run-7 API figures keep their plotted values. Network table values were already converted correctly; only the send/write semantics wording changes.

The grey Native label says “no foreground fsync”; the c16 caption identifies its `native_async_write` cohort. Hollow write markers are 5.5 pt with a 1.2 pt edge. The grey line at 16 workers in c16 read panels is not a store series. Panel four is successful-call occupancy only in the two c16 read figures, and the selected operation's API mean in both c1 figures. The renderer checks that note text stays below all panel labels.

Other external contributors record five configured or observed storage nodes; Cassandra's 3 November run records five contacts, not a verified five-node ring. Five storage-host SSD mounts are attested only for the inspected 4 November and final deployments. FoundationDB has a separate log/coordinator host. Some 3 November FDB/TiKV embedded fleet identities differ from the path. Figure notes, the reading guide and table caption keep these limits explicit.

## Contracts and limits

Native is one process on one SSD, with its own per-put file and directory fsync in the barrier mode. Five-owner Tidehunter synchronous uses covering fdatasync; Valkey AOF-always can group clients in one event-loop pass. Kvrocks has synced WAL writes; Garnet has commit-before-reply settings. Cassandra, TiKV and FoundationDB device persistence is not verified. Similar durable intent is not proof of equal crash recovery or power-loss survival.

Reads use different cache paths: Valkey RAM GET; Native's resident-oriented workload; owner memory or buffered misses; kernel buffered or direct target; SPDK direct target. Native reads recent mapped fragments and older fragments by positional reads. Native 8 KiB requested-cold cases pass the device-count gate but have mainly memory-like latency; 64 KiB meets case-level qualification, not per-call NAND proof.

Run 9 uses one owner: Native is on the coordinator SSD, owners on the remote SSD. Native per-put and grouped barriers differ from owner covering sync; page-cache acknowledgement is non-durable. Owner reads use one lane at c1/c5 and four at c16, Native zero remote lanes. c5 owner cells are lane-limited, not transport limits or rankings. Native cold 1 KiB is unqualified at c1/c5/c16 in r0/r1; Native 8/64 KiB and all owner cold cells qualify by their own receipts/ratios. This is case-level evidence, not every-call NAND proof. Independent floors are not subtracted from API means.

Run-7 late rate means the mean of the last five **complete** one-second buckets, excluding the partial final bucket. Whole-case rates do not change. The earlier brief was biased low; the historical report's approximate late range was consistent with complete buckets. The decline does not identify a stall cause or prove capacity.

Native's 64 KiB barrier fits a serialized-service model [INFERENCE], not proof of kernel serialization or queue depth one. API calls overlap. Its page-cache 64 KiB discontinuity remains unexplained. Valkey and owner actual commands per sync are not measured. Run-8 wait/software shares suggest candidates, not a measured explanation of a separately sampled cross-store gap. No fleet, repetition or outcome samples are pooled.

Run-9 grouped controls and both-host floors test additional hypotheses on one fleet; they do not prove the earlier 64-KiB jump, isolate network/software cost, establish fleet spread or close checkpoint-stall/profile gaps. Attempts, repetitions, outcomes and temperatures are not pooled.

Run 10 separates mutual TLS from trusted TCP/verbs and Adaptive from Busy. Busy spins in user space on both owner and coordinator; it is not `SO_BUSY_POLL`. One fleet/two repetitions, whole-case ratios and cross-fleet run-9 floors do not identify wire/crypto intervals or the cause of the 64-KiB rate ceiling. Kernel direct and SPDK file-AIO NVMe-oF are namespace controls, not engine one-sided RDMA READ (`rma=None`, no registration keys). Their target work is outside the owner cgroup. Qualification does not prove physical NAND service per call.

The paper measures page-cache acknowledgement, different hardware/dataset/duration, and throughput rather than engine latency quantiles. It has no 8 or 64 KiB experiment. The measurements do not reproduce the paper; they do not contradict it.

### Corrections to the binding wording

The retained data require these explicit corrections, recorded in `validation.json`:

1. Native page-cache c16 occupancy is at most about 1.26 through 8 KiB, but about 8.40 at 64 KiB; it is not below 1.3 at every size. Its finite rate remains omitted as requested.
2. Storage write-count max/min is about 1.36 for Valkey and 1.07 for owner verbs, not a common “within about 25%” spread. The brief prints exact storage-only ranges instead.
3. The reciprocal Native c1 mean rounds to 6,396 puts/s, not 6,397.
4. “No overlap” is not true of API calls. Only a serialized-service model is supported as an inference.
5. Run-8 software shares do not prove that the SSD is not limiting the full-matrix owner path. The brief states a hypothesis, not that stronger conclusion.
6. The paper size summary applies to its main evaluation. The supplied Figure 1 facts include other small sizes; the brief does not claim those sizes are absent from the whole paper.
7. FoundationDB has five storage-role hosts plus a separate transaction/log host. The per-node table counts storage nodes, not total persistence devices.

Document generation launches no infrastructure or production benchmark. The selected second attempts supplied runs 9 and 10; their aborted first attempts supply no final measurements. This document work changes no production Rust code. Cleanup follows campaign receipts; costs are explicitly list-price estimates, not invoices.
