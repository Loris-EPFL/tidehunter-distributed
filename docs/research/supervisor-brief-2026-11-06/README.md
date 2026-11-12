# Final-campaign supervisor brief

`main.pdf` is the supervisor brief. `main.tex` is its source; `plot_final.py` generates vector figures and `evidence.json`. The copied architecture diagram is from the previous supervisor brief; the earlier brief is unchanged.

From the workspace root, regenerate with:

```sh
.venv/bin/python tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-06/plot_final.py
.venv/bin/python tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-06/make_tables.py
cd tidehunter-sharded-wal/docs/research/supervisor-brief-2026-11-06
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

The final source is `checkpoint-work/spdk-matched-storage-20261105T004442Z/final-full-report/`: `all-populations.csv`, `input/comparison.json` and `longitudinal-347.csv`. The historical reference is `checkpoint-work/gcp-kv-comparison-2026-11-03-b35a62e24b47faaf/metrics-all-populations.csv`; it is a different run, binary and queue policy. The archived final controller receipt is `checkpoint-work/spdk-matched-storage-20261105T004442Z/gcp-queue-bounded-campaign-20261106T032027Z/final-retrieval/terminal-controller.tar.gz`, SHA-256 `6070cbc80a63109006cc33381dbd9d798ac0442940941f45b9851560fe7dc1a0`. Evidence records per-figure source rows, identifiers, counts and source and figure hashes. Percentiles are conditional and nearest-rank; no populations or repetitions are pooled. Each case has one repetition.

The 1 KiB table values are rounded for readability. `evidence.json` retains the exact source values. Valkey batch-write cases are absent, not zero. Configured cache cohorts are not evidence that every call was a cache hit, and cache-drop cohorts do not prove every call reached physical SSD. Valkey GET reads RAM. Successful-call throughput is over finite case exposure; rejected offered arrivals are separate. See `validation.json` for the checks actually performed.

The bounded queue policy is explicitly requested by the live NVMe-oF initiator; the sealed `nvme connect` call has no queue flags. Coherence and remote-read evidence reports are under the campaign directory's `analysis/coherence/` and `analysis/remote-nvme/`. First-touch is not proof of a per-call cold physical read.
