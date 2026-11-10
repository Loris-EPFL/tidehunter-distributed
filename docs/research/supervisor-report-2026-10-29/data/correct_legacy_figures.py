#!/usr/bin/env python3
"""Rerender retained plotted observations with the documented Valkey caption correction.

Run from report root: python data/correct_legacy_figures.py
The source manifest remains unchanged as historical evidence. Curves and metrics are
unchanged; the correction follows measured comparison.json and valkey_tcp.rs.
"""
import json,sys
from pathlib import Path
root=Path.cwd(); sys.path.insert(0,str(root/'data')); import plot_cloud_comparison as plot
manifest=json.loads((root/'data/plots.json').read_text()); out=root/'figures/eval'
stale='Valkey get-many uses ordered bounded per-key GET routing, not cross-slot MGET; no atomic PUT-batch support.'
correct='Valkey get-many issues one MGET through redis-rs, which splits by slot and reassembles ordered results; no atomic PUT-batch support.'
for figure in manifest['figures']:
    figure['caption']=figure['caption'].replace(stale,correct)
plot.render(manifest['figures'],manifest['coverage'],out)
for path in out.glob('*.png'): path.unlink()
print('Rerendered legacy vector PDFs with corrected Valkey caption; source manifest unchanged.')
