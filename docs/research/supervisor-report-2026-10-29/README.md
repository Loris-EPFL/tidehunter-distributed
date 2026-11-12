# Supervisor report

`main.tex` is the report entry point. It includes the observability narrative in `sections/`, generated tables in `tables/`, vector figures in `figures/eval/`, and `references.bib`. Data and generation provenance are retained under `data/` and with the figure/table generators. The report describes the evidence cut of 29 October 2026, not current deployment status.

`evidence-provenance.json` records source paths and SHA-256 hashes for the evidence cut, including measured-source and local-difference information. Large raw campaign artifacts remain external.

`validation.json` records the final PDF/source hashes, exhaustive numeric-table checks, rendered-page review scope, and the successful clean offline regeneration and source-bundle build.

From this directory, compile with a TeX installation that provides pdfLaTeX, BibTeX, TikZ, longtable, pdflscape and the packages named in `main.tex`:

```sh
pdflatex -interaction=nonstopmode -halt-on-error main.tex
bibtex main
pdflatex -interaction=nonstopmode -halt-on-error main.tex
pdflatex -interaction=nonstopmode -halt-on-error main.tex
```

Alternatively use `latexmk -pdf main.tex` where available. The appendices are intentionally long: they enumerate the retained case and population data. Raw evidence is preserved in the separately retained AWS run directory named in the report; do not interpret this source project as a substitute for the original raw artifacts.

The LaTeX MCP validator reports four `data-*` references as undefined because their labels are in included table fragments. The TeX compiler resolves these cross-file references after the required passes; inspect its log for actual unresolved references.
