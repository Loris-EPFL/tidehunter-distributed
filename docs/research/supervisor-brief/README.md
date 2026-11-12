# Distributed Tidehunter: Architecture and Google Cloud Evaluation

`main.tex` is the current supervisor brief. It covers the coordinator and owner-WAL architecture and the completed synthetic Google Cloud comparison of 3 November 2026. The larger `../supervisor-report-2026-10-29/` is a separate historical report and is unchanged. The Mermaid source for the topology diagram is `architecture.mmd`; the read-path diagram is maintained in `main.tex`.

`validation.json` records the document's build and inspection status. Rebuild and inspect the PDF after source or figure changes and before distribution. Old AWS-only figure files are retained as historical assets but are not included in the revised document.

## Campaign evidence

- Run: `bench-b35a62e24b47faaf`; 273 unique completed cases, ten arms, one independent repetition. Six `z4m-highmem-16` VMs in `us-central1-a`, compact placement, Falcon/verbs RDMA. The workload is synthetic, not Sui checkpoint replay.
- Report and source data: `../../../../checkpoint-work/gcp-kv-comparison-2026-11-03-b35a62e24b47faaf/report.md`, `input/comparison.json`, `metrics-all-populations.csv`, and `figures/plots.json` within that campaign directory. The CSV has 1,446 rows, including all available API, scheduling and response statistics. The manifest identifies exact observations and case IDs for the campaign figures.
- `evidence.json` records source and binary digests, the CSV and generated figure hashes, all plotted case IDs and source values, and all available clock rows for the 1 KiB primary concurrent read, write and read-32 populations used in Table 1. Each figure plots the primary RF1 successful read or write population at concurrency 16 for all ten backends and five value sizes. The full campaign data and separate cache and offered-load cases remain in the campaign directory.
- Figure 3 is concurrent scalar writes; Figure 4 is concurrent single-value reads. Figure 5 reuses the write cases to show descriptive p99.9, maxima, finite measurement exposure and successful value-payload bandwidth. The evidence contains 150 figure-case records from 100 unique cases, not 150 independent runs.
- Value-size positions now use their actual logarithmic spacing. Rates still use the entire original raw measurement span. The native 64 KiB point and TiKV 1 KiB pause remain visible; no outliers were removed. The audit is `../BENCHMARK_ANOMALY_AUDIT_2026-11-03.md`. Its local intervention results are deliberately excluded from this brief.
- The original archive and cleanup evidence are under `../../../../checkpoint-work/gcp-manual-run-b35a62e24b47faaf/`; the final cloud export is `gs://mysten-research-bench-artifacts/bench-b35a62e24b47faaf/results/nofile-recovery-2/`.

## Rebuilding

From this directory, with the retained `checkpoint-work` campaign directory present:

```bash
../../../../.venv/bin/python plot_evidence.py
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
pdfinfo main.pdf
```

The Python script requires Matplotlib. It renders supervisor-specific figures from the original comparison JSON and regenerates the evidence record without changing raw data or frozen campaign figures. Compilation uses the included PDFs and does not require Mermaid or network access. After the build, check for LaTeX errors, undefined references, legibility, and a page count of at most ten, and update `validation.json` with the actual result.
