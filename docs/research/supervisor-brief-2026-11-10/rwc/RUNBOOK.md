# Run 9: final real-data supervisor brief

The launch seal was consumed and Main authorized integration into the real brief. `main.tex.pre-run9` is the pre-integration source; the main-only draft patch was applied once. Do not reapply it on regeneration. The integrated source also implements the decidable items in `EDIT-PLAN.md`, including six main-body pages and two new appendix pages, explicit read lanes, actual qualification/failure receipts, updated historical scope, next-work decisions and launched-package provenance. `EDIT-PLAN.md` is a retained pre-result plan, not current campaign status.

## Authoritative inputs

- Final export/report: `/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/focused-launch-20261110-rwcompact2/export/` (`matrix.json`, `comparison.json`).
- Final six CSVs plus report: the same parent directory's `analysis2/` (includes `read_lanes`, `native_shards` and durability fields).
- Matched Native report: `analysis-fair-native/`, same export.
- Provenance transcription: `rwc/run9-provenance.json`. Receipt paths in it are hashed, including final state, package receipt, campaign receipt and controller outcome. The state distinguishes parent `bench-bb40628be5664257` from fleet `bench-41ecdf3f69fb47ec`.

All 342 planned cases have terminal receipts: 339 controller-completed, three failed G11N event-mode `ib_write_lat` floors. The terminal report deliberately retains `complete=false` and campaign receipt exit 1; the controller/export error fields are null. Do not report a clean top-level success or hide the floor failures. Six controller-completed Native cold 1-KiB cases are analytically unqualified, not additional controller failures. No result from aborted fleet `bench-f369f00616c841e4` enters the data or repetitions.

Final package/source hashes, timestamps and VM names describe the launched dirty revision, not the current checkout. Cost estimates use the supplied 2.11 VM-fleet hours, two hosts and list price; they are explicitly [INFERENCE], not invoices. Cleanup is recorded as verified. No document-generation command launches infrastructure.

## Exact regeneration commands

Run from this brief directory:

```sh
PY=/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python
REPO=/home/hybr1d/Desktop/PDM/TideHunter/tidehunter-sharded-wal
RUN=/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/focused-launch-20261110-rwcompact2

# Retained historical inputs and figures; never run the unavailable-export build_inputs.py.
$PY -B build_run8_inputs.py
$PY -B make_tables.py
$PY -B plot_final.py

# Optional independent re-analysis: writes only inside this brief.
$PY -B "$REPO/experiments/kv-comparison/remote_rw.py" \
  --export "$RUN/export" --report "$RUN/export" --output rwc/run9-output

# Build from the authoritative analysis2 CSVs, then register AFTER the old input builder.
$PY -B rwc/build_rwc_inputs.py --output "$RUN/analysis2" \
  --export "$RUN/export" --provenance rwc/run9-provenance.json --register
$PY -B rwc/plot_rwc.py --output "$RUN/analysis2"
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
$PY -B rwc/verify_rwc.py --pdf
$PY -B verify_brief.py
$PY -B verify_curves.py
pdfinfo main.pdf
```

The LaTeX MCP can also compile the real project-scoped `main.tex`; `latexmk` provides the definitive multi-pass reference build. Its static validator does not follow all input-file labels, so “undefined reference” from that validator alone is not a compiler result. The final compiler log must have no unresolved references, missing characters, overfull/underfull boxes or LaTeX warnings.

`verify_brief.py` received the minimal integration accommodation: 48-page budget, heading-based lookup for the same historical numeric claims, acceptance of six added figure pages and current scope/status wording. It still independently checks the 14 historical figure data sets, 82 historical PDF claims, all retained number keys, raw owner extracts, sustained buckets and unchanged historical tables, and calls the curve/size/spike checks. Its diff is archived under `rwc/verify-brief-run9.patch`. `verify_curves.py` and `verify_size_spike.py` remain unchanged. The latter's checks are invoked by `verify_brief.py`; it has no standalone CLI.

## New outputs and their scientific contract

- `rwc/data/run9-output/`: six copied analyzer CSVs and `remote_rw.md`, plus `campaign_cases.csv` (independently checked against the complete raw comparison report) and `cold_qualification.csv` (device ratios/reasons indexed from the report, independently checked against it).
- `rwc/generated/tab-rwc-{parity,write,read-cold,read-warm,cold-qualification,floors}.tex`: six tables. Compact write/read tables retain all c1/c5/c16 cases, one real row per repetition with an explicit Rep. column (r0/r1), aligned Size/Arm labels and p50/p99 in each concurrency cell, never pooled.
- `rwc/generated/rwc-{numbers,findings,provenance,failures,figure-hooks}.tex`: source-traced values, fully resolved findings, exact launch metadata, deduplicated failed floor receipts and real figure paths.
- `rwc/generated/inputs-rwc.json`, `figure-evidence.json`, `validation-rwc.json`: source/output hashes, every plotted metric with its case/repetition/lanes/durability and independent verification results. Root `inputs.json` registers only real final inputs.
- `figures/rwc-{write-latency,read-latency,throughput,cold-warm,floors,parity}.pdf`: six new real-data vector figures.
- `rwc/section-rwc.tex`: six-page main-body section after run-8 reads. `rwc/appendix-rwc.tex`: separate qualification and floor/provenance appendix; the generated run-8 `appendix-tables.tex` is unchanged.

Native page-cache acknowledgement is not durable. Native per-put/grouped barriers and owner covering fdatasync are distinct; Native uses the coordinator SSD, owners the remote SSD. Both host floors are shown as independent controls, not components subtracted from API means. Standalone network tools do not measure full RPC/security processing.

Native has zero remote-read lanes (in-process); owners use one lane at c1/c5 and four at c16. The c5 owner read cells are lane-limited, not transport limits or a basis to rank transports. The throughput figure marks/shades them. Derived c5/c1 ratios are computed within the same arm, temperature, size and repetition, not from pooled samples or separately sampled medians.

Native cold 1 KiB is unqualified at all three reader counts in both repetitions. Native 8/64 KiB and all 36 owner cold cells qualify. The threshold is device reads per successful call >=0.9 AND a qualified receipt; this remains a case-level proxy, not per-call NAND proof. Raw unqualified latency is not plotted as cold SSD performance. Cold/warm geometry differs. One final fleet with r0/r1 does not establish fleet spread or significance. Read/network floors include r2; append floors do not. G14P has three once-only profiles, not extra unprofiled repetitions.

The three failed event-write floors retain their exact listener-readiness error, not zero latency or system-failure labels. No five-owner scaling, NVMe-oF reads, Spot/other VM classes, Sui replay, failover or production capacity claim is added.

## Visual inspection and the 48-page gate

```sh
mkdir -p rwc/rendered-real
pdftoppm -f 1 -l 2 -r 110 -png main.pdf rwc/rendered-real/opening
pdftoppm -f 10 -l 15 -r 110 -png main.pdf rwc/rendered-real/body
# Use actual appendix pages from pdfinfo / final heading positions, not stale planned page numbers.
pdftoppm -f 46 -l 47 -r 110 -png main.pdf rwc/rendered-real/appendix
pdftoppm -r 120 -png -singlefile figures/rwc-throughput.pdf rwc/rendered-real/rwc-throughput
```

Render each of the other five figure PDFs similarly. Open the PNGs and inspect at print-equivalent size: axes/units, legends, filled/open repetitions, c5 lane-limit labels, unqualified/missing markers, compact table alignment, captions, hashes and page breaks. Visual acceptance is tied to the final PDF SHA-256, not inherited from an older build. The final independent check refuses more than 48 pages. The obsolete prospective append-floor table was compressed to a measured-control summary; no historical measured table or population was removed.

## Preserved preparation evidence

`rwc/SYNTHETIC-export/`, `SYNTHETIC-edge-export/`, `data/SYNTHETIC-output/`, watermarked figures in `rwc/synthetic-figures/` and their old `ACCEPTANCE.json`/compiler log remain preparation fixtures, not results. Synthetic plotting/hooks also target `rwc/synthetic-figures/`, never the real `figures/` directory. They are excluded from the real input registry, figure hooks, final PDF and `main.fls` input list. Do not run a fixture builder into the final `rwc/generated/`; use a separately authorized isolated copy if fixture acceptance is needed again. The real handoff evidence is `rwc/generated/validation-rwc.json`, root `validation.json`, current compiler output, real renders and `rwc/REAL-ACCEPTANCE.json`.
