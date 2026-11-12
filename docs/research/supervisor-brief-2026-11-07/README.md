# Full-matrix campaign brief, 7 November 2026

`main.pdf` is the brief. `main.tex` is its source. It reports the 652-case Google Cloud matrix (fleet `bench-0d99e8256fff4891`): where Tidehunter is better and worse than Valkey and the other stores, how fast remote NVMe/RDMA reads are, the coherence of RDMA against TCP, the bottlenecks, and the next work.

## Regenerate

From the workspace root:

```sh
.venv/bin/python tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-07/build_inputs.py   # needs the retrieved export
.venv/bin/python tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-07/plot_final.py
.venv/bin/python tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-07/make_tables.py
cd tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-07
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

`build_inputs.py` reads the retrieved campaign export (12 GB, not in the repository) under `checkpoint-work/spdk-matched-storage-20261105T004442Z/gcp-full-matrix-campaign-20261106T213227Z-direct/`. Its outputs are kept here, so the last three steps run without the export.

## Files

| File | Content |
|---|---|
| `populations.csv`, `previous-populations.csv` | One row per case and operation population, this run and the previous campaign (this run from the strict `comparison.csv` that the fixed report code regenerates into `analysis/gcp-full-matrix-2026-11-07/report-fixed/`). In `populations.csv` every completed row of Valkey, TiKV, Cassandra, FoundationDB, Kvrocks and Garnet carries the median over all GCP runs that completed the population (`analysis/gcp-full-matrix-2026-11-07/fixes/external-store-medians.csv`; column `median_runs` is the number of runs). Column `raw_api_p50_us` keeps the final-run p50 of every row; the cross-campaign drift section uses it. |
| `external-spread.csv` | Fastest-to-slowest p50 ratio across the runs of the same external-store population (populations with at least two runs): median, 90th percentile and maximum per store and for all. |
| `device-reads.csv` | Block-device counters of the six hosts between the measurement gates (`/proc/diskstats`). The Local SSD is `/dev/nvme0n1` on every host; `build_inputs.py` checks this per case. |
| `write-timeline-spans.csv` | Contiguous intervals between consecutive events of write case 0491, owner-record means, and the authority-thread waits (`corr`: Pearson r of a wait against the number of competing commands; `bucket`: mean wait at 0 and 8 competing commands). |
| `readmany-trace.csv` | Where the time of a traced 32-key call goes (cases 0474, 0483, 0093, 0095). |
| `stage-evidence.csv` | Selected stage means from `bottlenecks/stage-breakdown.csv` of the analysis directory, and authority history loads of the cold 64 KiB cases (`result/measurement-read-evidence.json`). |
| `inputs.json` | Source and output SHA-256 hashes. |
| `data.py` | Loaders, matching rules and the number language. |
| `make_tables.py` | Writes `macros.tex`, `numbers.tex` and the `tab-*.tex` fragments. |
| `plot_final.py` | Writes `figures/*.pdf` and `evidence.json` (case ids and values of every plotted point). |

## Numbers

Every measured number in `main.tex` is a `\N{key}` or a macro such as `\RC{p50}{th_verbs}{1024}`. `make_tables.py` scans the TeX sources, evaluates each key against the CSV tables and writes `numbers.tex`. An unknown or ambiguous key stops the build. The key grammar is in the docstring of `data.py`. Hand-typed numbers are limited to values outside the CSV tables: the native `valkey-benchmark` controls, file and line references, and arithmetic on code constants.

## Reading rules

- API times are success-only exact nearest-rank quantiles. One repetition per Tidehunter and Native case. The values of the other stores are medians of per-run quantiles over all GCP runs that completed the same population (user decision: no re-run, no drift gate); the median of per-run quantiles is not a pooled quantile. The `-durability-native` twin of each sealed read case is a second execution, used only to estimate run noise.
- A matched pair has the same cohort, profile, value size, concurrency, operation, key count and offered rate.
- `device reads per call` is the sum of reads completed on the five owner hosts divided by successful calls (the coordinator's own SSD for Native). It shows whether a read reached an SSD, not which read caused it.
- The strict report admits all 548 completed cases. An earlier strict report rejected twelve live SPDK cases because a derived mean differed by 1e-14 us (Python 3.12 float summation); the check now accepts the plain left-to-right sum. No case is marked `*` any more; the marking code stays for future runs.
- Statements marked `[INFERENCE]` are reasoning, not measurements.
