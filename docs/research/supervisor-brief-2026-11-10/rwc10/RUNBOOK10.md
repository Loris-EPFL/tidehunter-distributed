# Run 10: fabric A/B brief preparation

## Launch seal and scientific status

Preparation initially changed only new, untracked `rwc10/` files and the authorized scratch copy. After Main's final export GO, the three integration patches were dry-run and applied once to the real brief, with `main.tex.pre-run10` retained. The retained run-9 PDF checker also uses the integrated 56-page cap. The selected measured export is now integrated; no cloud command, production benchmark, Rust edit or commit was involved.

Attempt 1 (fleet `bench-9dc7f51e57e9449f`, parent `bench-3309de6a3c344e0f`) aborted after 9 of 80 cases: the monitoring exporter assumed five owner rows for sealed SPDK target metrics. The one-or-five-owner rule was fixed before attempt 2. None of attempt 1's measurements enters the final report; its layout-test artifacts remain EXCLUDED. The selected second attempt is fleet `bench-afdfe33817034a7c`, parent `bench-38b1466b6b0e4e1e`: 80 of 80 completed, all 40 cold cases qualified, controller exit 0 and cleanup verified. Final receipt-backed provenance supersedes the earlier launch notification in `run10-launched.json`; see `run10-provenance.json` and `REAL-ACCEPTANCE10.json`.

The fixture uses the real reviewed 80-job `cloud_campaign.make_focused_matrix('fabric_ab_compact')`, not a hand-recreated matrix. It contains invented successful-call latencies, client/owner cgroup CPU, manifests, CQ wait deltas and qualification receipts. Busy CPU is near one core on both hosts; Adaptive is about 0.05–0.15 cores. TCP CQ absence is NR, not zero. One SPDK cold 64-KiB/c16/r1 cell is deliberately unqualified, exercising absent latency/ratio markers while retaining its device count. This is a rendering/checker test, not a predicted result.

All fixture export/data/generated directories contain `SYNTHETIC`; every fixture figure has SYNTHETIC in its filename and a watermark. `synthetic-only.patch` also watermarks every page of the scratch PDF. Never apply that patch to a real results document. The ladder deliberately mixes **real run-9 network floor context** with synthetic run-10 engine values; its title/footer, caption and watermark disclose this. It never subtracts floors or claims matched fleets.

## Exact preparation commands

From the existing brief directory:

```sh
PY=/home/hybr1d/Desktop/PDM/TideHunter/.venv/bin/python
REPO=/home/hybr1d/Desktop/PDM/TideHunter/tidehunter-sharded-wal

$PY -B rwc10/make_synthetic_export.py
$PY -B "$REPO/experiments/kv-comparison/fabric_ab.py" \
  --export rwc10/SYNTHETIC-export --report rwc10/SYNTHETIC-export \
  --output rwc10/SYNTHETIC-output
$PY -B rwc10/build_rwc10_inputs.py --output rwc10/SYNTHETIC-output \
  --provenance rwc10/SYNTHETIC-export/SYNTHETIC-provenance.json
$PY -B rwc10/plot_rwc10.py --output rwc10/SYNTHETIC-output --synthetic
$PY -B rwc10/verify_rwc10.py --synthetic
$PY -B rwc10/prepare_scratch.py --output rwc10/SYNTHETIC-output \
  --provenance rwc10/SYNTHETIC-export/SYNTHETIC-provenance.json
```

`prepare_scratch.py` regenerates the three integration diffs against the current source, copies the brief into `/tmp/rwc10-build/`, applies those diffs plus the explicit synthetic flag/watermark, registers only in that copy and rebuilds its new inputs/figures. It refuses to replace an existing scratch directory unless it has this script's ownership marker. Its copies omit large prior render/fixture directories. The original `main.tex`, run-9 section, root `inputs.json`, PDF and tracked scripts stay untouched.

Build and accept **inside the scratch copy**:

```sh
cd /tmp/rwc10-build
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
$PY -B rwc10/verify_rwc10.py --synthetic --pdf
# The retained legacy checker assumes the real repository directory depth.
# Run it in the real brief after final-data integration, not this flat /tmp copy.
pdfinfo main.pdf
mkdir -p rwc10/SYNTHETIC-renders
pdftoppm -r 110 -png main.pdf rwc10/SYNTHETIC-renders/page
```

The project-scoped LaTeX MCP refused `/tmp/rwc10-build/main.tex` because its allowed base is `/home/hybr1d/Desktop/PDM/TideHunter`; the supported fallback is the documented `latexmk` build inside `/tmp`. Static validation alone is not compilation. Open the rendered new section and changed front/method/provenance pages. Check every full figure at print-equivalent width: axes/units, categorical labels, separate repetitions, security and policy names, core-role markers, unavailable cold points, device ratios and cross-fleet floor labels. Record the actual scratch PDF hash/page count and manual visual review in `rwc10/SYNTHETIC-ACCEPTANCE10.json`. The cap is **56**, not an assertion that every future dataset will fit without a layout review.

## Outputs

- `make_synthetic_export.py`: reviewed matrix fixture and clearly synthetic provenance.
- `rwc10_data.py`: shared presentation names, CSV lookup and declared run-9 floor selector.
- `build_rwc10_inputs.py`: six `tab-rwc10-*.tex` tables (arms, warm, pairs, CPU, cold, ladder); `rwc10-numbers.tex` using the existing `\N{...}` mechanism; resolved `rwc10-findings.tex`; provenance/hooks; source/output SHA-256 manifest.
  Raw provenance also hashes the selected `matrix.json`, raw `comparison.json`, present per-case receipts/manifests/CPU/CQ/media evidence and launched receipt sources. `campaign_cases.csv` retains controller status separately from analytical qualification. Real builds require explicit `--export` and `--report`; fixtures default to the provenance file's export directory. Cold findings enumerate qualified, unqualified, failed, not-run and any additional statuses, including explicit zero counts.
- `plot_rwc10.py`: five vector PDFs, `rwc10-{warm,gap,cpu,cold,ladder}.pdf`; every numeric marker/annotation has CSV family, row, case, repetition, field and factor in `figure-evidence.json`.
- `verify_rwc10.py`: no imports from the builders/plotter; recomputes every registered number, reconstructs table cells, checks all warm and cold pair ratios/CPU differences, all retained floor repetitions and each plotted value. Missing data is not zero. PDF mode checks the 56-page cap and compiler log; visual inspection stays separate.
- `section-rwc10.tex`: new section after run 9, including the run-9 TCP mTLS versus trusted-verbs correction, CPU cost, qualified NVMe-oF controls, inference-only interpretation and unimplemented engine READ/kernel-busy-poll limits.
- `main-tex-run10.patch`: preamble hooks, two scope finding macros, method/next-work/provenance updates and new section hook, against current `main.tex`.
- `run9-security-run10.patch`: one added run-9 sentence about its unmatched security classes.
- `verify-brief-run10.patch`: exact tracked-verifier diff for cap 56 and five additional figures; all 14 historical and six run-9 figure checks remain intact.
- `synthetic-only.patch`: scratch flag and watermark ONLY, never part of a real-data cutover.
- `prepare_patches.py`, `prepare_scratch.py`: reproducible unapplied-diff and isolated-copy workflow.
- `seal-baseline.json`: hashes of the existing tracked brief scripts and original main/run-9 sources, registry, README and PDF. `SYNTHETIC-ACCEPTANCE10.json` records their equality check.

## Quarantined partial-export layout test

Main supplied the aborted attempt's partial export solely to exercise missing-case handling. It contains 9 completed controller cases and 71 not-run cases; not-run cells are shown as **not run**, not zero or failed. Its tables and every figure filename/watermark say EXCLUDED. They are never registered in root `inputs.json`, never selected by the final section hooks, and omitted from scratch copies. This mode forbids `--register`; the known first-attempt fleet/export is rejected without the exclusion flag.

```sh
# cwd: original brief; these commands write only rwc10/EXCLUDED-*.
FIRST=/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/focused-launch-20261110-fabricab1/export
$PY -B rwc10/build_rwc10_inputs.py --output /tmp/f10-partial \
  --provenance rwc10/EXCLUDED-attempt1-provenance.json \
  --export "$FIRST" --report "$FIRST"
$PY -B rwc10/plot_rwc10.py --output /tmp/f10-partial --excluded-attempt
$PY -B rwc10/verify_rwc10.py --excluded-attempt
```

Expected layout-test output: six excluded tables and five excluded figures, with all available values independently recomputed. The original preparation certificate records 65 numbers and 63 plotted values before the final narrative extensions; it is historical, not a final-result certificate. Never copy excluded artifacts to the final generated/figure directories.

## When the real export arrives, after Main's go

This one-time integration has now been applied. On normal rebuild, follow the root README's regeneration sequence; do not rerun `prepare_patches.py` or reapply patches against the already integrated source.

Do not apply any patch or register new root inputs before the go message. Obtain the selected final attempt's actual provenance, not current local HEAD or first-attempt values. Supply `rwc10/run10-provenance.json` with:

- `synthetic: false`, `run_id`, `parent_run_id`;
- `package_sha256`, `source_revision`, Boolean `source_dirty`;
- timezone-qualified `launch_time`, ordered `vm_names` (coordinator, shard WAL);
- `receipt_sources`: paths to the exact state/package/campaign receipts used; optional extra metadata such as class/zone/matrix digest can be retained.

Use the selected second attempt's launched facts only after checking its final receipts; do not invent exact timestamps or substitute current local HEAD. Preserve terminal controller failures separately from analytical qualification, report actual receipt status even if the wrapper exits nonzero, and exclude aborted attempts. Then use the retrieved real paths (replace the two shell path variables with the actual retrieval locations):

```sh
# cwd: the original brief directory, ONLY after authorization
EXPORT=/absolute/path/to/retrieved/final/export
REPORT=/absolute/path/to/retrieved/final/report
$PY -B "$REPO/experiments/kv-comparison/fabric_ab.py" \
  --export "$EXPORT" --report "$REPORT" --output rwc10/run10-output
$PY -B rwc10/prepare_patches.py
patch --dry-run -p1 -i rwc10/main-tex-run10.patch
cp -n main.tex main.tex.pre-run10
patch --dry-run -p1 -i rwc10/run9-security-run10.patch
patch --dry-run -p1 -i rwc10/verify-brief-run10.patch
patch -p1 -i rwc10/main-tex-run10.patch
patch -p1 -i rwc10/run9-security-run10.patch
patch -p1 -i rwc10/verify-brief-run10.patch
# Do NOT apply synthetic-only.patch.
```

If earlier input builders need refreshing, run the established run-9 sequence first (`build_run8_inputs.py`, `make_tables.py`, `plot_final.py`, then the real run-9 builder/plotter with its existing provenance). They rewrite `inputs.json`; register run 10 **last**:

```sh
$PY -B rwc10/build_rwc10_inputs.py --output rwc10/run10-output \
  --provenance rwc10/run10-provenance.json --export "$EXPORT" --report "$REPORT" --register
$PY -B rwc10/plot_rwc10.py --output rwc10/run10-output
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
$PY -B rwc10/verify_rwc10.py --pdf
$PY -B verify_brief.py
$PY -B verify_curves.py
pdfinfo main.pdf
```

Real products use `rwc10/data/`, `rwc10/generated/` and the requested real `figures/rwc10-*.pdf`. Fixture products remain quarantined; the explicit real preamble has no synthetic fallback. Apply integration patches once only. Re-render and inspect the actual final section/pages, recompute acceptance against its PDF hash and reject >56 pages. If space is needed, first shorten duplicated *new run-10* captions/prose or move detailed run-10 tables to an appendix; do not remove historical measurements, pool repetitions or reduce printed figure fonts below the brief's 7-pt floor.

## Interpretation boundaries

Only selected attempt-two measurements enter the final run-10 section. The five-arm warm comparison separates configured security and polling, but whole-case ratios do not isolate crypto/wire intervals. Busy affects coordinator and owner in user space, not SO_BUSY_POLL. Engine endpoints use `rma=None` and export no keys: raw RDMA READ floors are not an implemented engine path. Cold RPC, kernel-direct and SPDK file-AIO paths remain distinct; every NVMe-oF control channel is trusted verbs Adaptive. One fleet/two repetitions, not pooled, do not establish five-owner scale-out, fleet spread, significance, physical NAND service per call, Spot behavior, failover, Sui replay or production capacity. Independent run-9 floors never yield a subtracted software remainder.

## Final review corrections

Main found a real defect missed by the initial visual acceptance: the cold figure's SPDK legend entry touched the top-left panel title. The cold plot now reserves a separate legend band (`top=.70`, `hspace=1.25`). All five figures also check rendered legend/title, x-label/title and footer/label bounding boxes before saving; these narrow checks supplement, not replace, full-resolution visual review. The generation/build/verification commands above are unchanged.

The front-page table now includes run 10. Findings 11 and 12 carry CSV-traced security and cold-path results; Finding 3 distinguishes the completed user-space Busy A/B from the unrun kernel socket variant. The findings list alone uses 9-pt type and 1-pt item spacing so all twelve findings remain on page 1; table width and footnote format are unchanged. Current visual evidence and the corrected PDF hash are recorded in `REAL-ACCEPTANCE10.json`.
